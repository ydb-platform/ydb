import ast
import executing
import types
from .ast_utils import CallNodeFinder as CallNodeFinder, get_node_source_text as get_node_source_text
from .scrubbing import BaseScrubber as BaseScrubber, MessageValueCleaner as MessageValueCleaner, NOOP_SCRUBBER as NOOP_SCRUBBER
from .stack_info import warn_at_user_stacklevel as warn_at_user_stacklevel
from .utils import log_internal_error as log_internal_error
from _typeshed import Incomplete
from collections.abc import Iterator
from functools import lru_cache
from string import Formatter
from types import CodeType as CodeType
from typing import Any, Literal
from typing_extensions import NotRequired, TypedDict

class LiteralChunk(TypedDict):
    t: Literal['lit']
    v: str

class ArgChunk(TypedDict):
    t: Literal['arg']
    v: str
    spec: NotRequired[str]

class ChunksFormatter(Formatter):
    def chunks(self, format_string: str, kwargs: dict[str, Any], *, scrubber: BaseScrubber, fstring_frame: types.FrameType | None = None) -> tuple[list[LiteralChunk | ArgChunk], dict[str, Any], str]: ...

chunks_formatter: Incomplete

def logfire_format(format_string: str, kwargs: dict[str, Any], scrubber: BaseScrubber) -> str: ...
def logfire_format_with_magic(format_string: str, kwargs: dict[str, Any], scrubber: BaseScrubber, fstring_frame: types.FrameType | None = None) -> tuple[str, dict[str, Any], str]: ...
@lru_cache
def compile_formatted_value(node: ast.FormattedValue, ex_source: executing.Source) -> tuple[str, CodeType, CodeType]:
    """Returns three things that can be expensive to compute.

    1. Source code corresponding to the node value (excluding the format spec).
    2. A compiled code object which can be evaluated to calculate the value.
    3. Another code object which formats the value.
    """

class KnownFormattingError(Exception):
    """An error raised when there's something wrong with a format string or the field values.

    In other words this should correspond to errors that would be raised when using `str.format`,
    and generally indicate a user error, most likely that they weren't trying to pass a template string at all.
    """
class FStringAwaitError(Exception):
    """An error raised when an await expression is found in an f-string.

    This is a specific case that can't be handled by f-string introspection and requires
    pre-evaluating the await expression before logging.
    """
class FormattingFailedWarning(UserWarning): ...

def warn_formatting(msg: str): ...
def warn_fstring_await(msg: str): ...

class FormattingCallNodeFinder(CallNodeFinder):
    """Finds the call node corresponding to a call like `logfire.span` or `logfire.info`."""
    def heuristic_main_nodes(self) -> Iterator[ast.AST]: ...
    def heuristic_call_node_filter(self, node: ast.Call) -> bool: ...
    def warn_inspect_arguments_middle(self): ...
