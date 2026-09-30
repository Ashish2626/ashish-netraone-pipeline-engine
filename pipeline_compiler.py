"""
Netra-One Pipeline Compiler
===========================
Declarative DAG compilation, cycle detection, topological sort,
and I/O schema compatibility validation for multi-stage surveillance pipelines.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

import yaml


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class PipelineCompileError(Exception):
    """Base exception for all pipeline compilation failures."""


class CycleDetectedError(PipelineCompileError):
    """Raised when the pipeline graph contains a dependency cycle."""

    def __init__(self, cycle_path: list[str]):
        self.cycle_path = cycle_path
        super().__init__(
            f"Cycle detected in pipeline graph: {' -> '.join(cycle_path)}"
        )


class SchemaValidationError(PipelineCompileError):
    """Raised when connected nodes have incompatible I/O schemas."""

    def __init__(self, from_node: str, to_node: str, reason: str):
        self.from_node = from_node
        self.to_node = to_node
        self.reason = reason
        super().__init__(
            f"Schema mismatch on edge '{from_node}' -> '{to_node}': {reason}"
        )


class NodeValidationError(PipelineCompileError):
    """Raised when a node definition is invalid (missing fields, unknown type, etc.)."""

    def __init__(self, node_id: str, reason: str):
        self.node_id = node_id
        self.reason = reason
        super().__init__(f"Invalid node '{node_id}': {reason}")


class DuplicateNodeError(PipelineCompileError):
    """Raised when two nodes share the same id."""

    def __init__(self, node_id: str):
        self.node_id = node_id
        super().__init__(f"Duplicate node id: '{node_id}'")


class UnknownNodeError(PipelineCompileError):
    """Raised when an edge references a node that does not exist."""

    def __init__(self, node_id: str):
        self.node_id = node_id
        super().__init__(f"Edge references unknown node: '{node_id}'")


# ---------------------------------------------------------------------------
# Node Type Registry & I/O Schemas
# ---------------------------------------------------------------------------

class IOType(str, Enum):
    """Well-known data types that flow through pipeline edges."""
    RAW_FRAME = "raw_frame"
    PREPROCESSED_FRAME = "preprocessed_frame"
    INFERENCE_RESULT = "inference_result"
    GEOFENCE_FILTERED = "geofence_filtered"
    ALERT_PAYLOAD = "alert_payload"
    ANY = "any"


# Each node type declares what it consumes (input) and produces (output).
NODE_SCHEMAS: dict[str, dict[str, Any]] = {
    "stream_source": {
        "input": IOType.ANY,          # sources accept no upstream input
        "output": IOType.RAW_FRAME,
        "params": {"stream_id": str, "target_fps": int},
    },
    "frame_transform": {
        "input": IOType.RAW_FRAME,
        "output": IOType.PREPROCESSED_FRAME,
        "params": {"resize": list, "normalize": bool},
    },
    "mock_inference_node": {
        "input": IOType.PREPROCESSED_FRAME,
        "output": IOType.INFERENCE_RESULT,
        "params": {"batch_size": int, "simulated_latency_ms": int},
    },
    "spatial_heuristic": {
        "input": IOType.INFERENCE_RESULT,
        "output": IOType.GEOFENCE_FILTERED,
        "params": {"restricted_zone": list},
    },
    "sink_alert": {
        "input": IOType.GEOFENCE_FILTERED,
        "output": IOType.ALERT_PAYLOAD,
        "params": {},
    },
}


# ---------------------------------------------------------------------------
# Data Classes
# ---------------------------------------------------------------------------

@dataclass
class PipelineNode:
    id: str
    type: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineEdge:
    from_node: str
    to_node: str


@dataclass
class CompiledPipeline:
    """Result of a successful compilation pass."""
    pipeline_id: str
    nodes: list[PipelineNode]
    edges: list[PipelineEdge]
    topological_order: list[str]
    adjacency: dict[str, list[str]]


# ---------------------------------------------------------------------------
# Compiler
# ---------------------------------------------------------------------------

class PipelineCompiler:
    """
    Compiles a declarative pipeline definition (dict) into a validated,
    topologically-sorted execution graph.
    """

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compile(self, definition: dict[str, Any]) -> CompiledPipeline:
        """
        Validate and compile a pipeline definition.

        Parameters
        ----------
        definition : dict
            Parsed JSON/YAML with keys: pipeline_id, nodes, edges.

        Returns
        -------
        CompiledPipeline

        Raises
        ------
        PipelineCompileError (or subclass) on any validation failure.
        """
        pipeline_id = definition.get("pipeline_id", "unnamed_pipeline")
        raw_nodes = definition.get("nodes", [])
        raw_edges = definition.get("edges", [])

        nodes = self._parse_nodes(raw_nodes)
        edges = self._parse_edges(raw_edges)

        self._check_duplicate_ids(nodes)
        self._validate_node_definitions(nodes)
        self._validate_edge_references(nodes, edges)
        self._validate_schema_compatibility(nodes, edges)
        self._detect_cycles(nodes, edges)

        topo_order = self._topological_sort(nodes, edges)
        adjacency = self._build_adjacency(nodes, edges)

        return CompiledPipeline(
            pipeline_id=pipeline_id,
            nodes=nodes,
            edges=edges,
            topological_order=topo_order,
            adjacency=adjacency,
        )

    def compile_from_file(self, path: str) -> CompiledPipeline:
        """Load a YAML or JSON file and compile it."""
        with open(path, "r", encoding="utf-8") as fh:
            if path.endswith((".yaml", ".yml")):
                definition = yaml.safe_load(fh)
            else:
                definition = json.load(fh)
        return self.compile(definition)

    def compile_from_string(self, text: str, fmt: str = "yaml") -> CompiledPipeline:
        """Compile from a raw string (YAML or JSON)."""
        if fmt == "yaml":
            definition = yaml.safe_load(text)
        else:
            definition = json.loads(text)
        return self.compile(definition)

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_nodes(raw_nodes: list[dict]) -> list[PipelineNode]:
        nodes = []
        for raw in raw_nodes:
            nodes.append(
                PipelineNode(
                    id=raw["id"],
                    type=raw["type"],
                    params=raw.get("params", {}),
                )
            )
        return nodes

    @staticmethod
    def _parse_edges(raw_edges: list[dict]) -> list[PipelineEdge]:
        return [
            PipelineEdge(from_node=e["from"], to_node=e["to"])
            for e in raw_edges
        ]

    # ------------------------------------------------------------------
    # Validation passes
    # ------------------------------------------------------------------

    @staticmethod
    def _check_duplicate_ids(nodes: list[PipelineNode]) -> None:
        seen: set[str] = set()
        for node in nodes:
            if node.id in seen:
                raise DuplicateNodeError(node.id)
            seen.add(node.id)

    def _validate_node_definitions(self, nodes: list[PipelineNode]) -> None:
        for node in nodes:
            if node.type not in NODE_SCHEMAS:
                raise NodeValidationError(
                    node.id,
                    f"unknown node type '{node.type}'. "
                    f"Valid types: {list(NODE_SCHEMAS.keys())}",
                )
            schema = NODE_SCHEMAS[node.type]
            expected_params = schema.get("params", {})
            for param_name, param_type in expected_params.items():
                if param_name not in node.params:
                    raise NodeValidationError(
                        node.id,
                        f"missing required parameter '{param_name}'",
                    )
                value = node.params[param_name]
                if not isinstance(value, param_type):
                    raise NodeValidationError(
                        node.id,
                        f"parameter '{param_name}' must be {param_type.__name__}, "
                        f"got {type(value).__name__}",
                    )

    def _validate_edge_references(
        self, nodes: list[PipelineNode], edges: list[PipelineEdge]
    ) -> None:
        node_ids = {n.id for n in nodes}
        for edge in edges:
            if edge.from_node not in node_ids:
                raise UnknownNodeError(edge.from_node)
            if edge.to_node not in node_ids:
                raise UnknownNodeError(edge.to_node)

    def _validate_schema_compatibility(
        self, nodes: list[PipelineNode], edges: list[PipelineEdge]
    ) -> None:
        node_map = {n.id: n for n in nodes}
        for edge in edges:
            producer = node_map[edge.from_node]
            consumer = node_map[edge.to_node]
            producer_output = NODE_SCHEMAS[producer.type]["output"]
            consumer_input = NODE_SCHEMAS[consumer.type]["input"]
            if producer_output != consumer_input and consumer_input != IOType.ANY:
                raise SchemaValidationError(
                    edge.from_node,
                    edge.to_node,
                    f"'{producer.type}' outputs {producer_output.value} but "
                    f"'{consumer.type}' expects {consumer_input.value}",
                )

    def _detect_cycles(
        self, nodes: list[PipelineNode], edges: list[PipelineEdge]
    ) -> None:
        """
        DFS-based cycle detection. Raises CycleDetectedError with the
        offending path if a back-edge is found.
        """
        adjacency: dict[str, list[str]] = {n.id: [] for n in nodes}
        for edge in edges:
            adjacency[edge.from_node].append(edge.to_node)

        WHITE, GRAY, BLACK = 0, 1, 2
        color: dict[str, int] = {n.id: WHITE for n in nodes}
        path: list[str] = []

        def dfs(node_id: str) -> Optional[list[str]]:
            color[node_id] = GRAY
            path.append(node_id)
            for neighbor in adjacency[node_id]:
                if color[neighbor] == GRAY:
                    # Found a back-edge — extract the cycle
                    cycle_start = path.index(neighbor)
                    return path[cycle_start:] + [neighbor]
                if color[neighbor] == WHITE:
                    result = dfs(neighbor)
                    if result is not None:
                        return result
            path.pop()
            color[node_id] = BLACK
            return None

        for node in nodes:
            if color[node.id] == WHITE:
                cycle = dfs(node.id)
                if cycle is not None:
                    raise CycleDetectedError(cycle)

    @staticmethod
    def _topological_sort(
        nodes: list[PipelineNode], edges: list[PipelineEdge]
    ) -> list[str]:
        """Kahn's algorithm — O(V + E)."""
        in_degree: dict[str, int] = {n.id: 0 for n in nodes}
        adjacency: dict[str, list[str]] = {n.id: [] for n in nodes}
        for edge in edges:
            adjacency[edge.from_node].append(edge.to_node)
            in_degree[edge.to_node] += 1

        queue = [nid for nid, deg in in_degree.items() if deg == 0]
        result: list[str] = []

        while queue:
            current = queue.pop(0)
            result.append(current)
            for neighbor in adjacency[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        return result

    @staticmethod
    def _build_adjacency(
        nodes: list[PipelineNode], edges: list[PipelineEdge]
    ) -> dict[str, list[str]]:
        adjacency: dict[str, list[str]] = {n.id: [] for n in nodes}
        for edge in edges:
            adjacency[edge.from_node].append(edge.to_node)
        return adjacency


# ---------------------------------------------------------------------------
# Convenience
# ---------------------------------------------------------------------------

def compile_pipeline(definition: dict[str, Any]) -> CompiledPipeline:
    """One-shot compile helper."""
    return PipelineCompiler().compile(definition)
