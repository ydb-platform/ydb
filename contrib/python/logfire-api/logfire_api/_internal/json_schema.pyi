from .utils import JsonDict
from _typeshed import Incomplete
from functools import lru_cache
from typing import Any

__all__ = ['create_json_schema', 'attributes_json_schema_properties', 'attributes_json_schema', 'JsonSchemaProperties']

def create_json_schema(obj: Any, seen: set[int]) -> JsonDict:
    """Create a JSON Schema from the given object.

    Args:
        obj: The object to create the JSON Schema from.
        seen: A set of object IDs that have already been processed.

    Returns:
        The JSON Schema.
    """

JsonSchemaProperties: Incomplete

def attributes_json_schema(properties: JsonSchemaProperties) -> str: ...
def attributes_json_schema_properties(attributes: dict[str, Any]) -> JsonSchemaProperties: ...
