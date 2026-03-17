from typing import Any

import rustworkx as rx

from sqllineage.core.graph_operator import GraphOperator
from sqllineage.utils.constant import EdgeDirection
from sqllineage.utils.entities import EdgeTuple


class RustworkXGraphOperator(GraphOperator):
    """
    rustworkx based implementation of GraphOperator

    node added to rustworkx graph can only be accessed by index, and it doesn't distinguish nodes vs nodes attributes.
    For the implementation, each vertex T is represented as a dict with at least one key "vertex" to hold the actual
    vertex object, e.g., {vertex: T, prop1: val1, ...}, and we will maintain a mapping from vertex T to its node index
    in the graph

    edge added to rustworkx does not have the concept of label, so we will store label as part of edge data,
    e.g., {label: str, prop1: val1, ...}
    """

    def __init__(self, graph: rx.PyDiGraph[Any, Any] | None = None) -> None:
        if graph is None:
            self.graph = rx.PyDiGraph()
            self._vertex_to_index: dict[Any, int] = {}
        else:
            self.graph = graph
            # Rebuild vertex mapping from existing graph using actual node indices
            self._vertex_to_index = {}
            for node_idx in self.graph.node_indices():
                node_data = self.graph[node_idx]
                self._vertex_to_index[node_data["vertex"]] = node_idx

    def add_vertex_if_not_exist(self, vertex: Any, **props) -> None:
        if vertex in self._vertex_to_index:
            # Update existing node
            node_idx = self._vertex_to_index[vertex]
            node_data = self.graph[node_idx]
            node_data.update(props)
            self.graph[node_idx] = node_data
        else:
            # Add new node
            node_data = {"vertex": vertex, **props}
            node_idx = self.graph.add_node(node_data)
            self._vertex_to_index[vertex] = node_idx

    def retrieve_vertices_by_props(self, **props) -> list[Any]:
        vertices = []
        for node_idx in self.graph.node_indices():
            node_data = self.graph[node_idx]
            if all(node_data.get(prop) == val for prop, val in props.items()):
                vertices.append(node_data["vertex"])
        return vertices

    def retrieve_source_vertices(self) -> list[Any]:
        return [
            self.graph[node_idx]["vertex"]
            for node_idx in self.graph.node_indices()
            if len(self.graph.in_edges(node_idx)) == 0
            and len(self.graph.out_edges(node_idx)) > 0
        ]

    def retrieve_target_vertices(self) -> list[Any]:
        return [
            self.graph[node_idx]["vertex"]
            for node_idx in self.graph.node_indices()
            if len(self.graph.in_edges(node_idx)) > 0
            and len(self.graph.out_edges(node_idx)) == 0
        ]

    def retrieve_selfloop_vertices(self) -> list[Any]:
        return [
            self.graph[node_idx]["vertex"]
            for node_idx in self.graph.node_indices()
            if self.graph.has_edge(node_idx, node_idx)
        ]

    def update_vertices(self, *vertices: Any, **props) -> None:
        for vertex in vertices:
            if vertex in self._vertex_to_index:
                node_idx = self._vertex_to_index[vertex]
                node_data = self.graph[node_idx]
                node_data.update(props)
                self.graph[node_idx] = node_data

    def drop_vertices(self, *vertices: Any) -> None:
        indices_to_remove = []
        for vertex in vertices:
            # remove from mapping and collect indices to remove
            if (idx := self._vertex_to_index.pop(vertex, None)) is not None:
                indices_to_remove.append(idx)
        self.graph.remove_nodes_from(indices_to_remove)

    def add_edge_if_not_exist(
        self, src_vertex: Any, tgt_vertex: Any, label: str, **props
    ) -> None:
        if src_vertex is not None and tgt_vertex is not None:
            src_idx = self._vertex_to_index.get(src_vertex)
            tgt_idx = self._vertex_to_index.get(tgt_vertex)
            if src_idx is None:
                src_idx = self.graph.add_node({"vertex": src_vertex})
                self._vertex_to_index[src_vertex] = src_idx
            if tgt_idx is None:
                tgt_idx = self.graph.add_node({"vertex": tgt_vertex})
                self._vertex_to_index[tgt_vertex] = tgt_idx
            edge_data = {"label": label, **props}
            if not self.graph.has_edge(src_idx, tgt_idx):
                self.graph.add_edge(src_idx, tgt_idx, edge_data)

    def retrieve_edges_by_label(self, label: str) -> list[EdgeTuple]:
        edges = []
        for src_idx, tgt_idx in self.graph.edge_list():
            edge_data = self.graph.get_edge_data(src_idx, tgt_idx)
            if edge_data["label"] == label:
                attribute = edge_data.copy()
                attribute.pop("label")
                edges.append(
                    EdgeTuple(
                        source=self.graph[src_idx]["vertex"],
                        target=self.graph[tgt_idx]["vertex"],
                        label=label,
                        attributes=attribute,
                    )
                )
        return edges

    def retrieve_edges_by_vertex(
        self, vertex: Any, direction: str, label: str | None = None
    ) -> list[EdgeTuple]:
        edges: list[EdgeTuple] = []
        vertex_idx = self._vertex_to_index.get(vertex, -1)

        # rustworkx returns in_edges and out_edges in reverse insertion order
        # we want to return in insertion order to match the behavior of NetworkX
        if direction == EdgeDirection.IN:
            edge_list = reversed(self.graph.in_edges(vertex_idx))
        else:
            edge_list = reversed(self.graph.out_edges(vertex_idx))

        # Collect edges and sort by (source_idx, target_idx) for deterministic ordering
        for src_idx, tgt_idx, edge_data in edge_list:
            if label is None or edge_data.get("label") == label:
                src_node = self.graph[src_idx]
                tgt_node = self.graph[tgt_idx]
                src_vertex = (
                    src_node.get("vertex") if isinstance(src_node, dict) else src_node
                )
                tgt_vertex = (
                    tgt_node.get("vertex") if isinstance(tgt_node, dict) else tgt_node
                )
                label = edge_data["label"]
                attributes = edge_data.copy()
                attributes.pop("label")
                edges.append(
                    EdgeTuple(
                        source=src_vertex,
                        target=tgt_vertex,
                        label=label,
                        attributes=attributes,
                    )
                )

        return edges

    def drop_edge(self, src_vertex: Any, tgt_vertex: Any) -> None:
        src_idx = self._vertex_to_index.get(src_vertex)
        tgt_idx = self._vertex_to_index.get(tgt_vertex)
        if (
            src_idx is not None
            and tgt_idx is not None
            and self.graph.has_edge(src_idx, tgt_idx)
        ):
            self.graph.remove_edge(src_idx, tgt_idx)

    def get_sub_graph(self, *vertices: Any) -> "RustworkXGraphOperator":
        indices = [
            self._vertex_to_index[vertex]
            for vertex in vertices
            if vertex in self._vertex_to_index
        ]
        return RustworkXGraphOperator(self.graph.subgraph(indices))

    def merge(self, other: GraphOperator) -> None:
        if isinstance(other, RustworkXGraphOperator):
            # Create a mapping from other's indices to self's indices
            index_mapping = {}

            # Add all nodes from other graph
            for other_idx in other.graph.node_indices():
                node_data = other.graph[other_idx]
                vertex = node_data["vertex"]
                if vertex not in self._vertex_to_index:
                    new_idx = self.graph.add_node(node_data)
                    self._vertex_to_index[vertex] = new_idx
                    index_mapping[other_idx] = new_idx
                else:
                    self.graph[self._vertex_to_index[vertex]].update(node_data)
                    index_mapping[other_idx] = self._vertex_to_index[vertex]

            # Add all edges from other graph
            for src_idx, tgt_idx in other.graph.edge_list():
                edge_data = other.graph.get_edge_data(src_idx, tgt_idx)
                self_src_idx = index_mapping.get(src_idx, -1)
                self_tgt_idx = index_mapping.get(tgt_idx, -1)
                if not self.graph.has_edge(self_src_idx, self_tgt_idx):
                    self.graph.add_edge(self_src_idx, self_tgt_idx, edge_data)
        else:
            raise TypeError(
                "Expect other to be RustworkXGraphOperator, got " + str(type(other))
            )

    def list_lineage_paths(self, src_vertex: Any, tgt_vertex: Any) -> list[list[Any]]:
        result = []
        for path in rx.all_simple_paths(
            self.graph,
            self._vertex_to_index.get(src_vertex, -1),
            self._vertex_to_index.get(tgt_vertex, -1),
        ):
            path_vertices = []
            for idx in path:
                node_data = self.graph[idx]
                path_vertices.append(node_data["vertex"])
            result.append(path_vertices)
        return result

    def to_cytoscape(
        self, compound: bool = False
    ) -> list[dict[str, dict[str, object]]]:
        if compound:
            parents_dict = {}
            for node_idx in self.graph.node_indices():
                node_data = self.graph[node_idx]
                vertex = node_data.get("vertex")
                parent = getattr(vertex, "parent", None)
                if parent is not None:
                    parents_dict[parent] = {
                        "name": str(parent),
                        "type": type(parent).__name__,
                    }

            nodes = []
            for node_idx in self.graph.node_indices():
                vertex = self.graph[node_idx]["vertex"]
                parent = getattr(vertex, "parent", None)
                parent_candidates = getattr(vertex, "parent_candidates", [])
                nodes.append(
                    {
                        "data": {
                            "id": str(vertex),
                            "parent": (
                                parents_dict[parent]["name"]
                                if parent in parents_dict
                                else "<unknown>"
                            ),
                            "parent_candidates": [
                                {"name": str(p), "type": type(p).__name__}
                                for p in parent_candidates
                            ],
                            "type": type(vertex).__name__,
                        }
                    }
                )

            nodes += [
                {"data": {"id": attr["name"], "type": attr["type"]}}
                for _, attr in parents_dict.items()
            ]
        else:
            nodes = [
                {"data": {"id": str(self.graph[node_idx]["vertex"])}}
                for node_idx in self.graph.node_indices()
            ]

        edges: list[dict[str, dict[str, Any]]] = [
            {
                "data": {
                    "id": f"e{edge_id}",
                    "source": str(self.graph[src_idx]["vertex"]),
                    "target": str(self.graph[tgt_idx]["vertex"]),
                }
            }
            for edge_id, (src_idx, tgt_idx) in enumerate(self.graph.edge_list())
        ]

        return nodes + edges
