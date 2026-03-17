from .json_types import ArraySchema as ArraySchema, DataType as DataType, JSONSchema as JSONSchema
from .utils import safe_repr as safe_repr
from _typeshed import Incomplete
from typing import Any

class JsonArgsValueFormatter:
    """Format values recursively based on the information provided in value dict.

    When a custom format is identified, the `$__datatype__` key is always present.
    """
    def __init__(self, *, indent: int) -> None: ...
    def __call__(self, value: Any, *, schema: JSONSchema | None = None, indent_current: int = 0): ...

json_args_value_formatter: Incomplete
json_args_value_formatter_compact: Incomplete
