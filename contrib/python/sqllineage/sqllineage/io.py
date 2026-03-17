from typing import Any

from sqllineage.core.graph_operator import GraphOperator


def to_cytoscape(go: GraphOperator, compound=False) -> list[dict[str, dict[str, Any]]]:
    return go.to_cytoscape(compound=compound)
