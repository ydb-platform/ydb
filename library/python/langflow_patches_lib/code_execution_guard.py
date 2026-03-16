import logging
import os
from typing import Type, Optional

from lfx.interface.components import component_cache

logger = logging.getLogger(__name__)

component_code_cache = {}


class CodeProtectionError(Exception):
    pass


def restore_templated_code(vertex_type: str) -> Optional[Type]:
    """Replace potentially custom code from vertex with cached code of template"""

    if vertex_type not in component_code_cache:
        for bundle, components in component_cache.all_types_dict.items():
            if vertex_type in components:
                try:
                    component_code_cache[vertex_type] = components[vertex_type]["template"]["code"]["value"]
                    break
                except KeyError:
                    logger.error(f"Component {vertex_type} found, but don't have template code at expected path")
                    raise CodeProtectionError(
                        f"Component {vertex_type} found in cache, but we can't get corresponding surce code.\n"
                        "See code_execution_guard.py or contact ABC:openai-experiments-platform"
                    )

    if vertex_type not in component_code_cache:
        logger.error(f"Component {vertex_type} not found in cache, cache state is {component_code_cache.keys()}")
        raise CodeProtectionError(
            f"Component {vertex_type} not found in cache. Seems like you try to use forbidden component."
        )
    return component_code_cache[vertex_type]


def is_custom_code_exec_enabled() -> bool:
    """
    Check if custom code execution is explicitly allowed.
    """
    return os.getenv("LANGFLOW_ALLOW_CUSTOM_CODE", "true").lower() == "true"
