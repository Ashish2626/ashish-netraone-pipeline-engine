"""
Netra-One Real-Time Alert API & Telemetry Service
================================================
FastAPI service exposing:
  GET  /health           — per-node telemetry (FPS, queue depth, errors)
  POST /pipeline/compile — validate pipeline JSON/YAML, return topo order
  WS   /ws/alerts        — live alert broadcast
  POST /pipeline/run     — start a pipeline run (for load testing)
  GET  /pipeline/status  — current pipeline status
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from typing import Any, Optional

import yaml
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from pipeline_compiler import (
    PipelineCompiler,
    PipelineCompileError,
    CycleDetectedError,
    SchemaValidationError,
    NodeValidationError,
    DuplicateNodeError,
    UnknownNodeError,
)
from engine import PipelineEngine, AlertPayload

logger = logging.getLogger("netraone.server")

# ---------------------------------------------------------------------------
# Pydantic Models
# ---------------------------------------------------------------------------

class PipelineDefinition(BaseModel):
    pipeline_id: str = "unnamed_pipeline"
    nodes: list[dict[str, Any]] = Field(default_factory=list)
    edges: list[dict[str, str]] = Field(default_factory=list)


class CompileResponse(BaseModel):
    pipeline_id: str
    valid: bool
    topological_order: list[str]
    node_count: int
    edge_count: int
    error: Optional[str] = None


class HealthResponse(BaseModel):
    pipeline_id: str
    running: bool
    nodes: dict[str, Any]


class AlertBroadcast(BaseModel):
    timestamp: float
    stream_id: str
    alert_type: str
    coordinates: list[list[int]]
    latency_ms: float
    frame_id: str


# ---------------------------------------------------------------------------
# Global State
# ---------------------------------------------------------------------------

compiler = PipelineCompiler()
active_engine: Optional[PipelineEngine] = None
alert_websockets: list[WebSocket] = []


async def broadcast_alert(alert: AlertPayload) -> None:
    """Broadcast an alert to all connected WebSocket clients."""
    msg = AlertBroadcast(
        timestamp=alert.timestamp,
        stream_id=alert.stream_id,
        alert_type=alert.alert_type,
        coordinates=alert.coordinates,
        latency_ms=alert.latency_ms,
        frame_id=alert.frame_id,
    )
    disconnected = []
    for ws in alert_websockets:
        try:
            await ws.send_text(msg.model_dump_json())
        except Exception:
            disconnected.append(ws)
    for ws in disconnected:
        alert_websockets.remove(ws)


# ---------------------------------------------------------------------------
# FastAPI App
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage engine lifecycle."""
    global active_engine
    yield
    if active_engine:
        await active_engine.stop()
        active_engine = None


app = FastAPI(
    title="Netra-One Pipeline Engine",
    description="Real-time surveillance pipeline execution with telemetry and alert streaming",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health", response_model=HealthResponse)
async def get_health():
    """Real-time telemetry per node: throughput (FPS), queue depth, error count."""
    if active_engine is None:
        return HealthResponse(
            pipeline_id="none",
            running=False,
            nodes={},
        )
    return HealthResponse(**active_engine.get_telemetry())


@app.post("/pipeline/compile", response_model=CompileResponse)
async def compile_pipeline(definition: PipelineDefinition):
    """
    Accept pipeline JSON/YAML, validate graph syntax, return topological order.
    """
    try:
        raw = definition.model_dump()
        compiled = compiler.compile(raw)
        return CompileResponse(
            pipeline_id=compiled.pipeline_id,
            valid=True,
            topological_order=compiled.topological_order,
            node_count=len(compiled.nodes),
            edge_count=len(compiled.edges),
        )
    except CycleDetectedError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except SchemaValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except NodeValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except DuplicateNodeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except UnknownNodeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except PipelineCompileError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/pipeline/run")
async def run_pipeline(definition: PipelineDefinition, duration_s: float = 5.0):
    """Start a pipeline run for a given duration (for load testing)."""
    global active_engine
    if active_engine and active_engine._running:
        raise HTTPException(status_code=409, detail="Pipeline already running")

    try:
        raw = definition.model_dump()
        compiled = compiler.compile(raw)
    except PipelineCompileError as e:
        raise HTTPException(status_code=400, detail=str(e))

    engine = PipelineEngine(compiled, queue_maxsize=10)
    engine.on_alert(broadcast_alert)
    active_engine = engine
    await engine.start()

    # Schedule auto-stop
    async def auto_stop():
        await asyncio.sleep(duration_s)
        await engine.stop()

    asyncio.create_task(auto_stop())
    return {"status": "started", "pipeline_id": compiled.pipeline_id, "duration_s": duration_s}


@app.get("/pipeline/status")
async def pipeline_status():
    """Current pipeline status and telemetry."""
    if active_engine is None:
        return {"running": False, "pipeline_id": None}
    return active_engine.get_telemetry()


@app.websocket("/ws/alerts")
async def websocket_alerts(websocket: WebSocket):
    """WebSocket endpoint broadcasting live alert payloads."""
    await websocket.accept()
    alert_websockets.append(websocket)
    try:
        while True:
            # Keep connection alive; alerts are pushed via broadcast_alert
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        if websocket in alert_websockets:
            alert_websockets.remove(websocket)


# ---------------------------------------------------------------------------
# Load Generator
# ---------------------------------------------------------------------------

@app.post("/load-test")
async def load_test(num_frames: int = 1000, target_fps: int = 30):
    """
    Simulate N synthetic frame packets through the pipeline.
    Returns throughput metrics.
    """
    definition = {
        "pipeline_id": "load_test_pipeline",
        "nodes": [
            {"id": "src", "type": "stream_source", "params": {"stream_id": "load_gen", "target_fps": target_fps}},
            {"id": "preproc", "type": "frame_transform", "params": {"resize": [640, 640], "normalize": True}},
            {"id": "detector", "type": "mock_inference_node", "params": {"batch_size": 4, "simulated_latency_ms": 12}},
            {"id": "geofence", "type": "spatial_heuristic", "params": {"restricted_zone": [[100, 100], [500, 500]]}},
            {"id": "sink", "type": "sink_alert", "params": {}},
        ],
        "edges": [
            {"from": "src", "to": "preproc"},
            {"from": "preproc", "to": "detector"},
            {"from": "detector", "to": "geofence"},
            {"from": "geofence", "to": "sink"},
        ],
    }

    try:
        compiled = compiler.compile(definition)
    except PipelineCompileError as e:
        raise HTTPException(status_code=400, detail=str(e))

    engine = PipelineEngine(compiled, queue_maxsize=10)
    alerts_received: list[AlertPayload] = []
    engine.on_alert(lambda a: alerts_received.append(a))

    await engine.start()
    start_time = time.monotonic()

    # Wait until we've processed enough frames or timeout
    max_wait = 60.0
    while (time.monotonic() - start_time) < max_wait:
        src_health = engine.health.get("src")
        if src_health and src_health.frames_processed >= num_frames:
            break
        await asyncio.sleep(0.1)

    await engine.stop()
    elapsed = time.monotonic() - start_time

    telemetry = engine.get_telemetry()
    total_processed = sum(n["frames_processed"] for n in telemetry["nodes"].values())
    total_dropped = sum(n["frames_dropped"] for n in telemetry["nodes"].values())
    total_errors = sum(n["error_count"] for n in telemetry["nodes"].values())

    return {
        "frames_requested": num_frames,
        "frames_processed": total_processed,
        "frames_dropped": total_dropped,
        "errors": total_errors,
        "alerts_emitted": len(alerts_received),
        "elapsed_seconds": round(elapsed, 2),
        "throughput_fps": round(total_processed / elapsed, 1) if elapsed > 0 else 0,
        "node_telemetry": telemetry["nodes"],
    }


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
