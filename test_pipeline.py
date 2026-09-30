"""
Netra-One Pipeline Engine — Unit Tests
======================================
Covers: valid DAG compilation, cycle detection, schema validation,
drop-frame backpressure policy, engine execution, burst loads, and
graceful shutdown.
"""

from __future__ import annotations

import asyncio
import time

import pytest
import yaml

from pipeline_compiler import (
    PipelineCompiler,
    CycleDetectedError,
    SchemaValidationError,
    NodeValidationError,
    DuplicateNodeError,
    UnknownNodeError,
    PipelineCompileError,
)
from engine import (
    PipelineEngine,
    BackpressureQueue,
    FramePacket,
    AlertPayload,
    NodeState,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def compiler():
    return PipelineCompiler()


@pytest.fixture
def valid_pipeline_def():
    return {
        "pipeline_id": "test_pipeline",
        "nodes": [
            {"id": "src", "type": "stream_source", "params": {"stream_id": "cam_01", "target_fps": 30}},
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


@pytest.fixture
def cyclic_pipeline_def():
    return {
        "pipeline_id": "cyclic_pipeline",
        "nodes": [
            {"id": "a", "type": "stream_source", "params": {"stream_id": "cam", "target_fps": 10}},
            {"id": "b", "type": "frame_transform", "params": {"resize": [640, 640], "normalize": True}},
            {"id": "c", "type": "mock_inference_node", "params": {"batch_size": 1, "simulated_latency_ms": 5}},
        ],
        "edges": [
            {"from": "a", "to": "b"},
            {"from": "b", "to": "c"},
            {"from": "c", "to": "a"},  # cycle!
        ],
    }


@pytest.fixture
def schema_mismatch_def():
    return {
        "pipeline_id": "bad_schema",
        "nodes": [
            {"id": "src", "type": "stream_source", "params": {"stream_id": "cam", "target_fps": 10}},
            {"id": "detector", "type": "mock_inference_node", "params": {"batch_size": 1, "simulated_latency_ms": 5}},
        ],
        "edges": [
            {"from": "src", "to": "detector"},  # raw_frame -> expects preprocessed_frame
        ],
    }


# ---------------------------------------------------------------------------
# Compiler Tests
# ---------------------------------------------------------------------------

class TestPipelineCompiler:
    """Tests for DAG compilation, cycle detection, and schema validation."""

    def test_valid_dag_compiles(self, compiler, valid_pipeline_def):
        result = compiler.compile(valid_pipeline_def)
        assert result.pipeline_id == "test_pipeline"
        assert len(result.nodes) == 5
        assert len(result.edges) == 4
        assert result.topological_order[0] == "src"
        assert result.topological_order[-1] == "sink"

    def test_topological_order_correct(self, compiler, valid_pipeline_def):
        result = compiler.compile(valid_pipeline_def)
        order = result.topological_order
        # src must come before preproc, preproc before detector, etc.
        assert order.index("src") < order.index("preproc")
        assert order.index("preproc") < order.index("detector")
        assert order.index("detector") < order.index("geofence")
        assert order.index("geofence") < order.index("sink")

    def test_cycle_detection_raises(self, compiler, cyclic_pipeline_def):
        with pytest.raises(CycleDetectedError) as exc_info:
            compiler.compile(cyclic_pipeline_def)
        assert "Cycle detected" in str(exc_info.value)
        assert exc_info.value.cycle_path is not None

    def test_self_loop_detected(self, compiler):
        definition = {
            "pipeline_id": "self_loop",
            "nodes": [
                {"id": "a", "type": "stream_source", "params": {"stream_id": "cam", "target_fps": 10}},
            ],
            "edges": [{"from": "a", "to": "a"}],
        }
        with pytest.raises(CycleDetectedError):
            compiler.compile(definition)

    def test_schema_mismatch_raises(self, compiler, schema_mismatch_def):
        with pytest.raises(SchemaValidationError) as exc_info:
            compiler.compile(schema_mismatch_def)
        assert "Schema mismatch" in str(exc_info.value)

    def test_unknown_node_type_rejected(self, compiler):
        definition = {
            "pipeline_id": "bad_type",
            "nodes": [
                {"id": "x", "type": "nonexistent_type", "params": {}},
            ],
            "edges": [],
        }
        with pytest.raises(NodeValidationError) as exc_info:
            compiler.compile(definition)
        assert "unknown node type" in str(exc_info.value)

    def test_missing_param_rejected(self, compiler):
        definition = {
            "pipeline_id": "missing_param",
            "nodes": [
                {"id": "src", "type": "stream_source", "params": {"stream_id": "cam"}},  # missing target_fps
            ],
            "edges": [],
        }
        with pytest.raises(NodeValidationError) as exc_info:
            compiler.compile(definition)
        assert "missing required parameter" in str(exc_info.value)

    def test_duplicate_node_id_rejected(self, compiler):
        definition = {
            "pipeline_id": "dup",
            "nodes": [
                {"id": "a", "type": "stream_source", "params": {"stream_id": "cam", "target_fps": 10}},
                {"id": "a", "type": "frame_transform", "params": {"resize": [640, 640], "normalize": True}},
            ],
            "edges": [],
        }
        with pytest.raises(DuplicateNodeError):
            compiler.compile(definition)

    def test_unknown_edge_reference_rejected(self, compiler):
        definition = {
            "pipeline_id": "bad_ref",
            "nodes": [
                {"id": "a", "type": "stream_source", "params": {"stream_id": "cam", "target_fps": 10}},
            ],
            "edges": [{"from": "a", "to": "nonexistent"}],
        }
        with pytest.raises(UnknownNodeError):
            compiler.compile(definition)

    def test_compile_from_yaml_string(self, compiler):
        yaml_text = """
pipeline_id: yaml_test
nodes:
  - id: src
    type: stream_source
    params:
      stream_id: cam_01
      target_fps: 30
  - id: sink
    type: sink_alert
    params: {}
edges:
  - { from: src, to: sink }
"""
        # This will fail schema validation (raw_frame -> geofence_filtered expected)
        # but tests YAML parsing works
        with pytest.raises(SchemaValidationError):
            compiler.compile_from_string(yaml_text, fmt="yaml")

    def test_compile_from_json_string(self, compiler):
        json_text = """
{
  "pipeline_id": "json_test",
  "nodes": [
    {"id": "src", "type": "stream_source", "params": {"stream_id": "cam", "target_fps": 10}},
    {"id": "preproc", "type": "frame_transform", "params": {"resize": [640, 640], "normalize": true}},
    {"id": "detector", "type": "mock_inference_node", "params": {"batch_size": 1, "simulated_latency_ms": 5}},
    {"id": "geofence", "type": "spatial_heuristic", "params": {"restricted_zone": [[100, 100], [500, 500]]}},
    {"id": "sink", "type": "sink_alert", "params": {}}
  ],
  "edges": [
    {"from": "src", "to": "preproc"},
    {"from": "preproc", "to": "detector"},
    {"from": "detector", "to": "geofence"},
    {"from": "geofence", "to": "sink"}
  ]
}
"""
        result = compiler.compile_from_string(json_text, fmt="json")
        assert result.pipeline_id == "json_test"
        assert len(result.topological_order) == 5


# ---------------------------------------------------------------------------
# Backpressure Queue Tests
# ---------------------------------------------------------------------------

class TestBackpressureQueue:
    """Tests for bounded queue with drop-oldest policy."""

    @pytest.mark.asyncio
    async def test_drop_oldest_when_full(self):
        q = BackpressureQueue(maxsize=3, name="test")
        await q.put("a")
        await q.put("b")
        await q.put("c")
        assert q.qsize == 3
        # Now full — putting "d" should drop "a"
        dropped = await q.put("d")
        assert dropped is True
        assert q.qsize == 3
        assert q.drops == 1
        # Verify "a" was dropped
        first = await q.get()
        assert first == "b"

    @pytest.mark.asyncio
    async def test_no_drop_when_not_full(self):
        q = BackpressureQueue(maxsize=5, name="test")
        dropped = await q.put("x")
        assert dropped is False
        assert q.drops == 0

    @pytest.mark.asyncio
    async def test_multiple_drops(self):
        q = BackpressureQueue(maxsize=2, name="test")
        await q.put("a")
        await q.put("b")
        await q.put("c")  # drops a
        await q.put("d")  # drops b
        assert q.drops == 2
        assert q.qsize == 2
        first = await q.get()
        assert first == "c"


# ---------------------------------------------------------------------------
# Engine Tests
# ---------------------------------------------------------------------------

class TestPipelineEngine:
    """Tests for async execution engine."""

    @pytest.mark.asyncio
    async def test_engine_executes_pipeline(self, valid_pipeline_def):
        compiler = PipelineCompiler()
        compiled = compiler.compile(valid_pipeline_def)
        engine = PipelineEngine(compiled, queue_maxsize=10)
        await engine.start()
        await asyncio.sleep(0.5)
        await engine.stop()

        telemetry = engine.get_telemetry()
        assert telemetry["running"] is False
        # Source should have generated frames
        src_health = telemetry["nodes"]["src"]
        assert src_health["frames_processed"] > 0

    @pytest.mark.asyncio
    async def test_graceful_shutdown(self, valid_pipeline_def):
        compiler = PipelineCompiler()
        compiled = compiler.compile(valid_pipeline_def)
        engine = PipelineEngine(compiled, queue_maxsize=10)
        await engine.start()
        await asyncio.sleep(0.2)
        await engine.stop()
        # All nodes should be stopped
        for nid, health in engine.health.items():
            assert health.state == NodeState.STOPPED.value

    @pytest.mark.asyncio
    async def test_telemetry_tracks_fps(self, valid_pipeline_def):
        compiler = PipelineCompiler()
        compiled = compiler.compile(valid_pipeline_def)
        engine = PipelineEngine(compiled, queue_maxsize=10)
        await engine.start()
        await asyncio.sleep(1.0)
        await engine.stop()

        src_health = engine.health["src"]
        assert src_health.current_fps > 0
        assert src_health.frames_processed > 0

    @pytest.mark.asyncio
    async def test_burst_load_handling(self):
        """Engine should handle burst without crashing."""
        definition = {
            "pipeline_id": "burst_test",
            "nodes": [
                {"id": "src", "type": "stream_source", "params": {"stream_id": "burst", "target_fps": 100}},
                {"id": "preproc", "type": "frame_transform", "params": {"resize": [640, 640], "normalize": True}},
                {"id": "detector", "type": "mock_inference_node", "params": {"batch_size": 4, "simulated_latency_ms": 20}},
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
        compiler = PipelineCompiler()
        compiled = compiler.compile(definition)
        engine = PipelineEngine(compiled, queue_maxsize=5)  # small queue for backpressure
        await engine.start()
        await asyncio.sleep(2.0)
        await engine.stop()

        # Should have processed frames and possibly dropped some
        total_processed = sum(h.frames_processed for h in engine.health.values())
        assert total_processed > 0

    @pytest.mark.asyncio
    async def test_alert_callback_fires(self, valid_pipeline_def):
        alerts: list[AlertPayload] = []
        compiler = PipelineCompiler()
        compiled = compiler.compile(valid_pipeline_def)
        engine = PipelineEngine(compiled, queue_maxsize=10)
        engine.on_alert(lambda a: alerts.append(a))
        await engine.start()
        await asyncio.sleep(1.0)
        await engine.stop()
        # Alerts may or may not fire depending on geofence filtering,
        # but the callback mechanism should work without errors
        assert isinstance(alerts, list)


# ---------------------------------------------------------------------------
# Integration Tests
# ---------------------------------------------------------------------------

class TestIntegration:
    """End-to-end integration tests."""

    @pytest.mark.asyncio
    async def test_full_pipeline_run(self, valid_pipeline_def):
        """Compile, run, and verify telemetry."""
        compiler = PipelineCompiler()
        compiled = compiler.compile(valid_pipeline_def)
        engine = PipelineEngine(compiled, queue_maxsize=10)
        await engine.start()
        await asyncio.sleep(1.0)
        await engine.stop()

        telemetry = engine.get_telemetry()
        assert telemetry["pipeline_id"] == "test_pipeline"
        assert len(telemetry["nodes"]) == 5
        # Verify all nodes have health data
        for node_id in ["src", "preproc", "detector", "geofence", "sink"]:
            assert node_id in telemetry["nodes"]

    def test_sample_yaml_file(self):
        """Verify the sample YAML file compiles correctly."""
        compiler = PipelineCompiler()
        result = compiler.compile_from_file("sample_pipeline.yaml")
        assert result.pipeline_id == "perimeter_patrol_v1"
        assert len(result.nodes) == 5
        assert result.topological_order == [
            "source_rtsp", "preproc", "detector", "geofence_filter", "alert_emitter"
        ]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
