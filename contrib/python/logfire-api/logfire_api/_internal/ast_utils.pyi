import ast
import executing
import types
from .constants import ATTRIBUTES_MESSAGE_TEMPLATE_KEY as ATTRIBUTES_MESSAGE_TEMPLATE_KEY, ATTRIBUTES_SAMPLE_RATE_KEY as ATTRIBUTES_SAMPLE_RATE_KEY, ATTRIBUTES_TAGS_KEY as ATTRIBUTES_TAGS_KEY
from .stack_info import StackInfo as StackInfo, get_filepath_attribute as get_filepath_attribute
from .utils import uniquify_sequence as uniquify_sequence
from _typeshed import Incomplete
from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from opentelemetry.util import types as otel_types

@dataclass(frozen=True)
class LogfireArgs:
    """Values passed to `logfire.instrument` and/or values stored in a logfire instance as basic configuration.

    These determine the arguments passed to the method calls added by the AST transformer.
    """
    tags: tuple[str, ...]
    sample_rate: float | None
    msg_template: str | None = ...
    span_name: str | None = ...

@dataclass
class BaseTransformer(ast.NodeTransformer):
    """Helper for rewriting ASTs to wrap function bodies in `with {logfire_method_name}(...):`."""
    logfire_args: LogfireArgs
    logfire_method_name: str
    filename: str
    module_name: str
    qualname_stack: list[str] = ...
    def __post_init__(self) -> None: ...
    def visit_ClassDef(self, node: ast.ClassDef): ...
    def visit_FunctionDef(self, node: ast.FunctionDef | ast.AsyncFunctionDef): ...
    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef): ...
    def rewrite_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef, qualname: str) -> ast.AST: ...
    def logfire_method_call_node(self, node: ast.FunctionDef | ast.AsyncFunctionDef, qualname: str) -> ast.Call: ...
    def logfire_method_arg_values(self, qualname: str, lineno: int) -> tuple[str, dict[str, otel_types.AttributeValue]]: ...

class CallNodeFinder(ABC):
    """Base class for finding the `ast.Call` node corresponding to a function call in a given frame.

    Uses `executing`, then falls back to a heuristic if that fails.
    The heuristic is defined by subclasses which depends on what we're looking for,
    but in general `executing` is expected to work.
    Warns appropriately when things don't work.
    Only used when `inspect_arguments=True` in `logfire.configure()`.
    """
    frame: Incomplete
    ex: Incomplete
    source: Incomplete
    node: Incomplete
    def __init__(self, frame: types.FrameType) -> None: ...
    @abstractmethod
    def heuristic_main_nodes(self) -> Iterator[ast.AST]:
        """AST nodes (e.g. statements) to search for potential call nodes inside."""
    @abstractmethod
    def heuristic_call_node_filter(self, node: ast.Call) -> bool:
        """Condition that a potential call node must satisfy to be considered a match."""
    @abstractmethod
    def warn_inspect_arguments_middle(self) -> str:
        """Middle part of the warning message for `warn_inspect_arguments`.

        Should describe the consequences of the failure.
        """
    def warn_inspect_arguments(self, msg: str): ...
    def get_stacklevel(self): ...

class InspectArgumentsFailedWarning(Warning): ...

def get_node_source_text(node: ast.AST, ex_source: executing.Source):
    """Returns some Python source code representing `node`.

    Preferably the actual original code given by `ast.get_source_segment`,
    but falling back to `ast.unparse(node)` if the former is incorrect.
    This happens sometimes due to Python bugs (especially for older Python versions)
    in the source positions of AST nodes inside f-strings.
    """
