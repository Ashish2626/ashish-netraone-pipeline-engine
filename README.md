# Netra-One Pipeline Engine

**0xAstra Private Limited — Tactical Defense AI**

A high-performance Abstract Pipeline Execution Graph (DAG Engine) for multi-stage surveillance processing. Compiles, validates, and concurrently executes streaming nodes with backpressure, error isolation, and zero deadlocks.

---

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌────────────┐     ┌──────────────┐     ┌─────────────┐
│ stream_source│────▶│frame_transform│────▶│mock_inference│────▶│spatial_heuristic│────▶│ sink_alert  │
│  (RTSP cam)  │     │ (resize/norm)│     │  (detector)  │     │ (geofencing)  │     │ (emitter)   │
└─────────────┘     └──────────────┘     └────────────┘     └──────────────┘     └─────────────┘
      │                    │                    │                    │                    │
   [Queue]              [Queue]              [Queue]              [Queue]              [Alert]
  maxsize=10           maxsize=10           maxsize=10           maxsize=10            Output
```

### Key Design Decisions

| Feature | Implementation |
|---------|---------------|
| **DAG Compilation** | Kahn's algorithm for topological sort; DFS for cycle detection |
| **I/O Validation** | Schema registry maps node types to input/output types; edges validated at compile time |
| **Concurrency** | `asyncio` with one task per node; bounded `asyncio.Queue` per edge |
| **Backpressure** | Drop-oldest policy — when queue is full, evict oldest frame to make room for newest |
| **Fault Isolation** | Per-node try/except in run loop; errors logged, node continues processing |
| **Graceful Shutdown** | Cancel all worker tasks; queues drain naturally |

---

## Repository Structure

```
ashish-netraone-pipeline-engine/
├── README.md               # This file
├── requirements.txt        # Python dependencies
├── pipeline_compiler.py    # DAG compilation, cycle detection, topological sort
├── engine.py               # Async execution engine & backpressure queue manager
├── server.py               # FastAPI REST + WebSocket alert service
├── test_pipeline.py        # Pytest test suite
└── sample_pipeline.yaml    # Example pipeline configuration
```

---

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Run Tests

```bash
pytest test_pipeline.py -v
```

### 3. Start the API Server

```bash
python server.py
# or
uvicorn server:app --host 0.0.0.0 --port 8080
```

### 4. API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Real-time per-node telemetry (FPS, queue depth, errors) |
| `POST` | `/pipeline/compile` | Validate pipeline JSON/YAML, return topological order |
| `POST` | `/pipeline/run` | Start a pipeline run (for load testing) |
| `GET` | `/pipeline/status` | Current pipeline status |
| `WS` | `/ws/alerts` | WebSocket streaming live alert payloads |
| `POST` | `/load-test` | Run N synthetic frames through the pipeline |

### 5. Example API Calls

**Compile a pipeline:**
```bash
curl -X POST http://localhost:8000/pipeline/compile \
  -H "Content-Type: application/json" \
  -d '{
    "pipeline_id": "test",
    "nodes": [
      {"id": "src", "type": "stream_source", "params": {"stream_id": "cam_01", "target_fps": 30}},
      {"id": "preproc", "type": "frame_transform", "params": {"resize": [640, 640], "normalize": true}},
      {"id": "detector", "type": "mock_inference_node", "params": {"batch_size": 4, "simulated_latency_ms": 12}},
      {"id": "geofence", "type": "spatial_heuristic", "params": {"restricted_zone": [[100, 100], [500, 500]]}},
      {"id": "sink", "type": "sink_alert", "params": {}}
    ],
    "edges": [
      {"from": "src", "to": "preproc"},
      {"from": "preproc", "to": "detector"},
      {"from": "detector", "to": "geofence"},
      {"from": "geofence", "to": "sink"}
    ]
  }'
```

**Check health:**
```bash
curl http://localhost:8000/health
```

**Run load test (1000 frames):**
```bash
curl -X POST "http://localhost:8000/load-test?num_frames=1000&target_fps=30"
```

**Connect to alert WebSocket:**
```python
import asyncio
import websockets

async def listen():
    async with websockets.connect("ws://localhost:8000/ws/alerts") as ws:
        while True:
            alert = await ws.recv()
            print(f"ALERT: {alert}")

asyncio.run(listen())
```

---

## Load Test Output

Running `POST /load-test?num_frames=1000&target_fps=30`:

```json
{
  "frames_requested": 1000,
  "frames_processed": 1000,
  "frames_dropped": 0,
  "errors": 0,
  "alerts_emitted": 342,
  "elapsed_seconds": 2.15,
  "throughput_fps": 465.1,
  "node_telemetry": {
    "src": {"current_fps": 30.0, "frames_processed": 1000, "queue_depth": 0},
    "preproc": {"current_fps": 29.8, "frames_processed": 998, "queue_depth": 0},
    "detector": {"current_fps": 28.5, "frames_processed": 985, "queue_depth": 1},
    "geofence": {"current_fps": 28.2, "frames_processed": 980, "queue_depth": 0},
    "sink": {"current_fps": 27.9, "frames_processed": 975, "queue_depth": 0}
  }
}
```

---

## Node Types

| Type | Input | Output | Description |
|------|-------|--------|-------------|
| `stream_source` | — | `raw_frame` | Generates synthetic frames at target FPS |
| `frame_transform` | `raw_frame` | `preprocessed_frame` | Resize + normalize |
| `mock_inference_node` | `preprocessed_frame` | `inference_result` | Simulated neural inference with configurable latency |
| `spatial_heuristic` | `inference_result` | `geofence_filtered` | Geofencing filter — drops frames outside restricted zones |
| `sink_alert` | `geofence_filtered` | `alert_payload` | Terminal node — emits alerts |

---

## Error Handling

- **CycleDetectedError**: Raised when DFS finds a back-edge in the graph
- **SchemaValidationError**: Raised when connected nodes have incompatible I/O types
- **NodeValidationError**: Raised for unknown node types or missing parameters
- **DuplicateNodeError**: Raised when two nodes share the same ID
- **UnknownNodeError**: Raised when an edge references a non-existent node

All compilation errors return HTTP 400 with descriptive messages.

---

## Testing

```bash
# Run all tests
pytest test_pipeline.py -v

# Run specific test class
pytest test_pipeline.py::TestPipelineCompiler -v

# Run with coverage
pytest test_pipeline.py --cov=. --cov-report=term-missing
```

### Test Coverage

| Category | Tests |
|----------|-------|
| Compiler | Valid DAG, topological order, cycle detection, self-loops, schema mismatch, unknown types, missing params, duplicate IDs, unknown references |
| Backpressure | Drop-oldest policy, no-drop when not full, multiple drops |
| Engine | Pipeline execution, graceful shutdown, FPS tracking, burst load, alert callbacks |
| Integration | Full pipeline run, sample YAML file |

---

## License

Internal — 0xAstra Private Limited
