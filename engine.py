"""
Netra-One Asynchronous Execution Engine
=======================================
Concurrent node execution with bounded asyncio queues, drop-oldest
backpressure, fault isolation, and graceful shutdown.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Coroutine, Optional

from pipeline_compiler import (
    CompiledPipeline,
    PipelineNode,
    PipelineEdge,
    PipelineCompiler,
    PipelineCompileError,
)

logger = logging.getLogger("netraone.engine")


# ---------------------------------------------------------------------------
# Data Structures
# ---------------------------------------------------------------------------

class NodeState(str, Enum):
    IDLE = "idle"
    RUNNING = "running"
    ERROR = "error"
    STOPPED = "stopped"


@dataclass
class NodeHealth:
    """Real-time telemetry for a single node."""
    node_id: str
    node_type: str
    state: str
    frames_processed: int = 0
    frames_dropped: int = 0
    error_count: int = 0
    last_error: Optional[str] = None
    avg_latency_ms: float = 0.0
    current_fps: float = 0.0
    queue_depth: int = 0
    queue_maxsize: int = 0
    _latency_window: list[float] = field(default_factory=list, repr=False)
    _fps_timestamps: list[float] = field(default_factory=list, repr=False)

    def record_latency(self, latency_ms: float) -> None:
        self._latency_window.append(latency_ms)
        if len(self._latency_window) > 100:
            self._latency_window.pop(0)
        self.avg_latency_ms = sum(self._latency_window) / len(self._latency_window)

    def record_frame(self) -> None:
        now = time.monotonic()
        self._fps_timestamps.append(now)
        # Keep only last second of timestamps
        cutoff = now - 1.0
        self._fps_timestamps = [t for t in self._fps_timestamps if t > cutoff]
        self.current_fps = len(self._fps_timestamps)
        self.frames_processed += 1

    def record_drop(self) -> None:
        self.frames_dropped += 1

    def record_error(self, error: str) -> None:
        self.error_count += 1
        self.last_error = error
        self.state = NodeState.ERROR.value

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "node_type": self.node_type,
            "state": self.state,
            "frames_processed": self.frames_processed,
            "frames_dropped": self.frames_dropped,
            "error_count": self.error_count,
            "last_error": self.last_error,
            "avg_latency_ms": round(self.avg_latency_ms, 2),
            "current_fps": round(self.current_fps, 1),
            "queue_depth": self.queue_depth,
            "queue_maxsize": self.queue_maxsize,
        }


@dataclass
class FramePacket:
    """A single frame flowing through the pipeline."""
    frame_id: str
    stream_id: str
    timestamp: float
    payload: Any = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class AlertPayload:
    """Final alert emitted by the sink node."""
    timestamp: float
    stream_id: str
    alert_type: str
    coordinates: list[list[int]]
    latency_ms: float
    frame_id: str = ""


# ---------------------------------------------------------------------------
# Backpressure Queue
# ---------------------------------------------------------------------------

class BackpressureQueue:
    """
    Bounded asyncio.Queue with drop-oldest policy.

    When the queue is full, the oldest item is evicted to make room
    for the newest — preventing memory exhaustion under downstream
    backpressure while keeping the freshest data.
    """

    def __init__(self, maxsize: int = 10, name: str = ""):
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
        self._name = name
        self._drops = 0

    @property
    def maxsize(self) -> int:
        return self._queue.maxsize

    @property
    def qsize(self) -> int:
        return self._queue.qsize()

    @property
    def drops(self) -> int:
        return self._drops

    async def put(self, item: Any) -> bool:
        """
        Put an item, dropping the oldest if full.
        Returns True if an item was dropped to make room.
        """
        dropped = False
        if self._queue.full():
            try:
                self._queue.get_nowait()
                self._drops += 1
                dropped = True
            except asyncio.QueueEmpty:
                pass
        await self._queue.put(item)
        return dropped

    async def get(self) -> Any:
        return await self._queue.get()

    def get_nowait(self) -> Any:
        return self._queue.get_nowait()

    def task_done(self) -> None:
        self._queue.task_done()


# ---------------------------------------------------------------------------
# Node Workers
# ---------------------------------------------------------------------------

class AsyncNodeWorker:
    """
    Wraps a pipeline node as an async task with input/output queues,
    health tracking, and fault isolation.
    """

    def __init__(
        self,
        node: PipelineNode,
        input_queue: Optional[BackpressureQueue],
        output_queues: list[BackpressureQueue],
        health: NodeHealth,
        process_fn: Callable[[Any], Coroutine[Any, Any, Any]],
    ):
        self.node = node
        self.input_queue = input_queue
        self.output_queues = output_queues
        self.health = health
        self._process_fn = process_fn
        self._task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self) -> None:
        self._running = True
        self.health.state = NodeState.RUNNING.value
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self.health.state = NodeState.STOPPED.value

    async def _run_loop(self) -> None:
        """Main processing loop with fault isolation."""
        while self._running:
            try:
                # Source nodes have no input queue — they generate data
                if self.input_queue is not None:
                    item = await self.input_queue.get()
                    self.input_queue.task_done()
                else:
                    item = await self._generate()

                start = time.monotonic()
                result = await self._process_fn(item)
                latency_ms = (time.monotonic() - start) * 1000

                self.health.record_frame()
                self.health.record_latency(latency_ms)
                self.health.queue_depth = (
                    self.input_queue.qsize if self.input_queue else 0
                )

                # Fan-out to all downstream queues
                if result is not None:
                    for q in self.output_queues:
                        dropped = await q.put(result)
                        if dropped:
                            self.health.record_drop()

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Fault isolation: log and continue, don't kill the pipeline
                error_msg = f"{type(exc).__name__}: {exc}"
                logger.error(
                    "Node '%s' error (isolated): %s", self.node.id, error_msg
                )
                self.health.record_error(error_msg)
                await asyncio.sleep(0.01)  # brief backoff

    async def _generate(self) -> Any:
        """Override for source nodes."""
        await asyncio.sleep(0)
        return None


# ---------------------------------------------------------------------------
# Concrete Node Processors
# ---------------------------------------------------------------------------

class StreamSourceProcessor(AsyncNodeWorker):
    """Generates synthetic frame packets at a target FPS."""

    def __init__(self, node: PipelineNode, output_queues: list[BackpressureQueue], health: NodeHealth):
        self._stream_id = node.params.get("stream_id", "cam_01")
        self._target_fps = node.params.get("target_fps", 30)
        self._interval = 1.0 / self._target_fps if self._target_fps > 0 else 0.033
        super().__init__(node, None, output_queues, health, self._process)

    async def _generate(self) -> FramePacket:
        await asyncio.sleep(self._interval)
        return FramePacket(
            frame_id=str(uuid.uuid4()),
            stream_id=self._stream_id,
            timestamp=time.time(),
            payload={"raw_data": f"frame_{uuid.uuid4().hex[:8]}"},
        )

    async def _process(self, item: Any) -> FramePacket:
        return item


class FrameTransformProcessor(AsyncNodeWorker):
    """Simulates frame preprocessing (resize + normalize)."""

    def __init__(self, node: PipelineNode, input_queue: BackpressureQueue, output_queues: list[BackpressureQueue], health: NodeHealth):
        super().__init__(node, input_queue, output_queues, health, self._process)

    async def _process(self, packet: FramePacket) -> FramePacket:
        # Simulate transform work
        await asyncio.sleep(0.001)
        packet.metadata["resized"] = self.node.params.get("resize", [640, 640])
        packet.metadata["normalized"] = self.node.params.get("normalize", True)
        return packet


class MockInferenceProcessor(AsyncNodeWorker):
    """Simulates neural inference with configurable latency."""

    def __init__(self, node: PipelineNode, input_queue: BackpressureQueue, output_queues: list[BackpressureQueue], health: NodeHealth):
        super().__init__(node, input_queue, output_queues, health, self._process)

    async def _process(self, packet: FramePacket) -> FramePacket:
        latency_s = self.node.params.get("simulated_latency_ms", 12) / 1000.0
        await asyncio.sleep(latency_s)
        packet.metadata["inference"] = {
            "detections": [
                {"class": "person", "confidence": 0.92, "bbox": [120, 80, 200, 300]},
                {"class": "vehicle", "confidence": 0.78, "bbox": [350, 150, 180, 120]},
            ],
            "batch_size": self.node.params.get("batch_size", 4),
        }
        return packet


class SpatialHeuristicProcessor(AsyncNodeWorker):
    """Geofencing filter — drops frames outside restricted zones."""

    def __init__(self, node: PipelineNode, input_queue: BackpressureQueue, output_queues: list[BackpressureQueue], health: NodeHealth):
        super().__init__(node, input_queue, output_queues, health, self._process)

    async def _process(self, packet: FramePacket) -> Optional[FramePacket]:
        await asyncio.sleep(0.002)
        zone = self.node.params.get("restricted_zone", [[100, 100], [500, 500]])
        detections = packet.metadata.get("inference", {}).get("detections", [])
        filtered = []
        for det in detections:
            bbox = det.get("bbox", [0, 0, 0, 0])
            cx = (bbox[0] + bbox[2]) // 2
            cy = (bbox[1] + bbox[3]) // 2
            if zone[0][0] <= cx <= zone[1][0] and zone[0][1] <= cy <= zone[1][1]:
                filtered.append(det)
        if not filtered:
            return None  # Frame filtered out — no alert
        packet.metadata["filtered_detections"] = filtered
        packet.metadata["geofence_zone"] = zone
        return packet


class SinkAlertProcessor(AsyncNodeWorker):
    """Terminal node — emits alert payloads."""

    def __init__(self, node: PipelineNode, input_queue: BackpressureQueue, output_queues: list[BackpressureQueue], health: NodeHealth, alert_callback: Optional[Callable[[AlertPayload], None]] = None):
        self._alert_callback = alert_callback
        super().__init__(node, input_queue, output_queues, health, self._process)

    async def _process(self, packet: FramePacket) -> AlertPayload:
        detections = packet.metadata.get("filtered_detections", [])
        zone = packet.metadata.get("geofence_zone", [[0, 0], [0, 0]])
        alert = AlertPayload(
            timestamp=time.time(),
            stream_id=packet.stream_id,
            alert_type="GEOFENCE_BREACH",
            coordinates=zone,
            latency_ms=(time.time() - packet.timestamp) * 1000,
            frame_id=packet.frame_id,
        )
        if self._alert_callback:
            self._alert_callback(alert)
        return alert


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class PipelineEngine:
    """
    Orchestrates concurrent execution of a compiled pipeline.
    """

    def __init__(self, compiled: CompiledPipeline, queue_maxsize: int = 10):
        self._compiled = compiled
        self._queue_maxsize = queue_maxsize
        self._workers: dict[str, AsyncNodeWorker] = {}
        self._queues: dict[str, BackpressureQueue] = {}
        self._health: dict[str, NodeHealth] = {}
        self._alert_callbacks: list[Callable[[AlertPayload], None]] = []
        self._running = False

    @property
    def health(self) -> dict[str, NodeHealth]:
        return self._health

    def on_alert(self, callback: Callable[[AlertPayload], None]) -> None:
        """Register a callback for alert payloads."""
        self._alert_callbacks.append(callback)

    async def start(self) -> None:
        """Build queues and start all node workers."""
        self._running = True

        # Create a queue per edge (from_node -> to_node)
        edge_queues: dict[tuple[str, str], BackpressureQueue] = {}
        for edge in self._compiled.edges:
            q = BackpressureQueue(maxsize=self._queue_maxsize, name=f"{edge.from_node}->{edge.to_node}")
            edge_queues[(edge.from_node, edge.to_node)] = q
            self._queues[f"{edge.from_node}->{edge.to_node}"] = q

        # Build workers
        node_map = {n.id: n for n in self._compiled.nodes}
        for node_id in self._compiled.topological_order:
            node = node_map[node_id]
            health = NodeHealth(
                node_id=node.id,
                node_type=node.type,
                state=NodeState.IDLE.value,
                queue_maxsize=self._queue_maxsize,
            )
            self._health[node_id] = health

            # Input queue: first incoming edge's queue
            input_q = None
            for edge in self._compiled.edges:
                if edge.to_node == node_id:
                    input_q = edge_queues[(edge.from_node, edge.to_node)]
                    break

            # Output queues: all outgoing edges
            output_qs = []
            for edge in self._compiled.edges:
                if edge.from_node == node_id:
                    output_qs.append(edge_queues[(edge.from_node, edge.to_node)])

            # Create the right processor
            worker = self._create_worker(node, input_q, output_qs, health)
            self._workers[node_id] = worker

        # Start all workers
        for worker in self._workers.values():
            await worker.start()

        logger.info(
            "Pipeline '%s' started with %d nodes",
            self._compiled.pipeline_id,
            len(self._workers),
        )

    async def stop(self) -> None:
        """Graceful shutdown — stop all workers."""
        self._running = False
        for worker in self._workers.values():
            await worker.stop()
        logger.info("Pipeline '%s' stopped", self._compiled.pipeline_id)

    def _create_worker(
        self,
        node: PipelineNode,
        input_q: Optional[BackpressureQueue],
        output_qs: list[BackpressureQueue],
        health: NodeHealth,
    ) -> AsyncNodeWorker:
        if node.type == "stream_source":
            return StreamSourceProcessor(node, output_qs, health)
        elif node.type == "frame_transform":
            return FrameTransformProcessor(node, input_q, output_qs, health)
        elif node.type == "mock_inference_node":
            return MockInferenceProcessor(node, input_q, output_qs, health)
        elif node.type == "spatial_heuristic":
            return SpatialHeuristicProcessor(node, input_q, output_qs, health)
        elif node.type == "sink_alert":
            async def alert_cb(alert: AlertPayload):
                for cb in self._alert_callbacks:
                    cb(alert)
            return SinkAlertProcessor(node, input_q, output_qs, health, alert_cb)
        else:
            raise ValueError(f"Unknown node type: {node.type}")

    def get_telemetry(self) -> dict[str, Any]:
        """Return real-time telemetry for all nodes."""
        return {
            "pipeline_id": self._compiled.pipeline_id,
            "running": self._running,
            "nodes": {nid: h.to_dict() for nid, h in self._health.items()},
        }


# ---------------------------------------------------------------------------
# Convenience
# ---------------------------------------------------------------------------

async def run_pipeline(
    definition: dict[str, Any],
    duration_s: float = 5.0,
    queue_maxsize: int = 10,
) -> PipelineEngine:
    """Compile and run a pipeline for a given duration."""
    compiler = PipelineCompiler()
    compiled = compiler.compile(definition)
    engine = PipelineEngine(compiled, queue_maxsize=queue_maxsize)
    await engine.start()
    await asyncio.sleep(duration_s)
    await engine.stop()
    return engine
