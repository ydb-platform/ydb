from typing import no_type_check

from collate_sqllineage.core.models import Column
from collate_sqllineage.utils.constant import EdgeType


def _handle_source_wildcard(source_parent, target_parent, parents_dict, graph):
    for k, v in parents_dict[source_parent]["columns"].items():
        target_column = parents_dict[target_parent]["columns"].get(k)

        if k != "*" and (target_column is None or not graph.has_edge(v, target_column)):
            if target_column is None:
                target_column = Column(k)
                target_column.parent = target_parent
            parents_dict[target_parent]["columns"][k] = target_column
            graph.add_edge(target_parent, target_column, type=EdgeType.HAS_COLUMN)
            graph.add_edge(v, target_column, type=EdgeType.LINEAGE)


def _handle_target_wildcard(source_parent, target_parent, parents_dict, graph):
    lineage_map = {}
    for k, v in graph.in_edges(data=True)._adjdict.items():
        lineage_map[k] = {i["type"] for i in v.values()}

    for k, v in parents_dict[target_parent]["columns"].items():
        source_column = parents_dict[source_parent]["columns"].get(k)
        if k != "*" and (source_column is None or not graph.has_edge(source_column, v)):
            if source_column is None:
                source_column = Column(k)
                source_column.parent = source_parent
            parents_dict[source_parent]["columns"][k] = source_column
            add_lineage = True
            if EdgeType.LINEAGE in lineage_map.get(v, []):
                add_lineage = False
            if add_lineage:
                graph.add_edge(source_parent, source_column, type=EdgeType.HAS_COLUMN)
                graph.add_edge(source_column, v, type=EdgeType.LINEAGE)


@no_type_check
def handle_wildcard(col_graph, graph):
    """
    compound nodes is used to group nodes together to their parent.
    See https://js.cytoscape.org/#notation/compound-nodes for reference.
    """
    parents_dict = {
        node.parent: {
            "name": str(node.parent) if node.parent is not None else "<unknown>",
            "type": (
                type(node.parent).__name__
                if node.parent is not None
                else "Table or SubQuery"
            ),
            "columns": {},
        }
        for node in col_graph.nodes
    }

    for node in col_graph.nodes:
        parents_dict[node.parent]["columns"][node.raw_name] = node

    edge_list = list(col_graph.edges)

    for edge in edge_list:
        if edge[0].raw_name == "*" and edge[1].raw_name == "*":
            source_parent = edge[0].parent
            target_parent = edge[1].parent
            _handle_source_wildcard(source_parent, target_parent, parents_dict, graph)
            _handle_target_wildcard(source_parent, target_parent, parents_dict, graph)
