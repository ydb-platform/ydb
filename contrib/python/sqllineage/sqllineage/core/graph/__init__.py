import importlib
import logging
import typing

from sqllineage.config import SQLLineageConfig
from sqllineage.core.graph.networkx import NetworkXGraphOperator

logger = logging.getLogger(__name__)


def get_graph_operator_class() -> type:
    try:
        path = SQLLineageConfig.GRAPH_OPERATOR_CLASS.rsplit(".", 1)
        if len(path) == 1:
            raise ImportError
        module_path, class_name = path
        module = importlib.import_module(module_path)
        if not hasattr(module, class_name):
            raise ImportError
        return typing.cast(type, getattr(module, class_name))
    except ImportError:
        logger.warning(
            "Failed to import %s, using default NetworkXGraphOperator",
            SQLLineageConfig.GRAPH_OPERATOR_CLASS,
        )
        return NetworkXGraphOperator
