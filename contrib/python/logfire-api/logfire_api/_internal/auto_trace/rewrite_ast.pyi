import ast
from ..ast_utils import BaseTransformer as BaseTransformer, LogfireArgs as LogfireArgs
from ..main import Logfire as Logfire
from contextlib import AbstractContextManager as AbstractContextManager
from dataclasses import dataclass
from typing import Any, Callable, TypeVar

def compile_source(tree: ast.AST, filename: str, module_name: str, logfire_instance: Logfire, min_duration: int) -> Callable[[dict[str, Any]], None]:
    """Compile a modified AST of the module's source code in the module's namespace.

    Returns a function which accepts module globals and executes the compiled code.

    The modified AST wraps the body of every function definition in `with context_factories[index]():`.
    `context_factories` is added to the module's namespace as `logfire_<uuid>`.
    `index` is a different constant number for each function definition.
    `context_factories[index]` is one of these:
        - `partial(logfire_instance._fast_span, name, attributes)` where the name and attributes
            are constructed from `filename`, `module_name`, attributes of `logfire_instance`,
            and the qualified name and line number of the current function.
        - `MeasureTime`, a class that measures the time elapsed. If it exceeds `min_duration`,
            then `context_factories[index]` is replaced with the `partial` above.
    If `min_duration` is greater than 0, then `context_factories[index]` is initially `MeasureTime`.
    Otherwise, it's initially the `partial` above.
    """
def rewrite_ast(tree: ast.AST, filename: str, logfire_name: str, module_name: str, logfire_instance: Logfire, context_factories: list[Callable[[], AbstractContextManager[Any]]], min_duration: int) -> ast.AST: ...

@dataclass
class AutoTraceTransformer(BaseTransformer):
    """Trace all encountered functions except those explicitly marked with `@no_auto_trace`."""
    logfire_instance: Logfire
    context_factories: list[Callable[[], AbstractContextManager[Any]]]
    min_duration: int
    def check_no_auto_trace(self, node: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef) -> bool:
        """Return true if the node has a `@no_auto_trace` or `@logfire.no_auto_trace` decorator."""
    def visit_ClassDef(self, node: ast.ClassDef): ...
    def visit_FunctionDef(self, node: ast.FunctionDef | ast.AsyncFunctionDef): ...
    def rewrite_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef, qualname: str) -> ast.AST: ...
    def logfire_method_call_node(self, node: ast.FunctionDef | ast.AsyncFunctionDef, qualname: str) -> ast.Call: ...
T = TypeVar('T')

def no_auto_trace(x: T) -> T:
    """Decorator to prevent a function/class from being traced by `logfire.install_auto_tracing`.

    This is useful for small functions that are called very frequently and would generate too much noise.

    The decorator is detected at import time.
    Only `@no_auto_trace` or `@logfire.no_auto_trace` are supported.
    Renaming/aliasing either the function or module won't work.
    Neither will calling this indirectly via another function.

    Any decorated function, or any function defined anywhere inside a decorated function/class,
    will be completely ignored by `logfire.install_auto_tracing`.

    This decorator simply returns the argument unchanged, so there is zero runtime overhead.
    """
def has_yield(node: ast.AST): ...
