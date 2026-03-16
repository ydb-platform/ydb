from __future__ import annotations

import ast
import functools
import inspect
import sys
import types
import warnings
from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, cast

import executing
from opentelemetry.util import types as otel_types

from .constants import (
    ATTRIBUTES_MESSAGE_TEMPLATE_KEY,
    ATTRIBUTES_SAMPLE_RATE_KEY,
    ATTRIBUTES_TAGS_KEY,
)
from .stack_info import StackInfo, get_filepath_attribute
from .utils import uniquify_sequence


@dataclass(frozen=True)
class LogfireArgs:
    """Values passed to `logfire.instrument` and/or values stored in a logfire instance as basic configuration.

    These determine the arguments passed to the method calls added by the AST transformer.
    """

    tags: tuple[str, ...]
    sample_rate: float | None
    msg_template: str | None = None
    span_name: str | None = None


@dataclass
class BaseTransformer(ast.NodeTransformer):
    """Helper for rewriting ASTs to wrap function bodies in `with {logfire_method_name}(...):`."""

    logfire_args: LogfireArgs
    logfire_method_name: str
    filename: str
    module_name: str

    def __post_init__(self):
        # Names of functions and classes that we're currently inside,
        # so we can construct the qualified name of the current function.
        self.qualname_stack: list[str] = []

    def visit_ClassDef(self, node: ast.ClassDef):
        self.qualname_stack.append(node.name)
        # We need to call generic_visit here to modify any functions defined inside the class.
        node = cast(ast.ClassDef, self.generic_visit(node))
        self.qualname_stack.pop()
        return node

    def visit_FunctionDef(self, node: ast.FunctionDef | ast.AsyncFunctionDef):
        self.qualname_stack.append(node.name)
        qualname = '.'.join(self.qualname_stack)
        self.qualname_stack.append('<locals>')
        # We need to call generic_visit here to modify any classes/functions nested inside.
        self.generic_visit(node)
        self.qualname_stack.pop()  # <locals>
        self.qualname_stack.pop()  # node.name

        return self.rewrite_function(node, qualname)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef):
        return self.visit_FunctionDef(node)

    def rewrite_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef, qualname: str) -> ast.AST:
        # Replace the body of the function with:
        #     with <logfire_method_call_node>:
        #         <original body>
        body = node.body.copy()
        new_body: list[ast.stmt] = []
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            # If the first statement is just a string literal, it's a docstring.
            # Keep it as the first statement in the new body, not wrapped in a span,
            # so it's still recognized as a docstring.
            new_body.append(body.pop(0))

        # Ignore functions with a trivial/empty body:
        # - If `body` is empty, that means it originally was just a docstring that got popped above.
        # - If `body` is just a single `pass` statement
        # - If `body` is just a constant expression, particularly an ellipsis (`...`)
        if not body or (
            len(body) == 1
            and (
                isinstance(body[0], ast.Pass)
                or (isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant))
            )
        ):
            return node

        span = ast.With(
            items=[
                ast.withitem(
                    context_expr=self.logfire_method_call_node(node, qualname),
                )
            ],
            body=body,
            type_comment=node.type_comment,
        )
        new_body.append(span)

        kwargs: dict[str, Any] = {name: getattr(node, name, None) for name in node._fields}
        kwargs['body'] = new_body
        new_node = type(node)(**kwargs)
        return ast.fix_missing_locations(ast.copy_location(new_node, node))

    def logfire_method_call_node(self, node: ast.FunctionDef | ast.AsyncFunctionDef, qualname: str) -> ast.Call:
        raise NotImplementedError()

    def logfire_method_arg_values(self, qualname: str, lineno: int) -> tuple[str, dict[str, otel_types.AttributeValue]]:
        stack_info: StackInfo = {
            **get_filepath_attribute(self.filename),
            'code.lineno': lineno,
            'code.function': qualname,
        }
        attributes: dict[str, otel_types.AttributeValue] = {**stack_info}  # type: ignore

        logfire_args = self.logfire_args
        msg_template = logfire_args.msg_template or f'Calling {self.module_name}.{qualname}'
        attributes[ATTRIBUTES_MESSAGE_TEMPLATE_KEY] = msg_template

        span_name = logfire_args.span_name or msg_template

        if logfire_args.tags:
            attributes[ATTRIBUTES_TAGS_KEY] = uniquify_sequence(logfire_args.tags)

        sample_rate = logfire_args.sample_rate
        if sample_rate not in (None, 1):  # pragma: no cover
            attributes[ATTRIBUTES_SAMPLE_RATE_KEY] = sample_rate

        return span_name, attributes


class CallNodeFinder(ABC):
    """Base class for finding the `ast.Call` node corresponding to a function call in a given frame.

    Uses `executing`, then falls back to a heuristic if that fails.
    The heuristic is defined by subclasses which depends on what we're looking for,
    but in general `executing` is expected to work.
    Warns appropriately when things don't work.
    Only used when `inspect_arguments=True` in `logfire.configure()`.
    """

    def __init__(self, frame: types.FrameType):
        self.frame = frame
        # This is where the magic happens. It has caching.
        self.ex = executing.Source.executing(frame)
        self.source = self.ex.source
        self.node = self._get_call_node()

    def _get_call_node(self) -> ast.Call | None:
        if isinstance(self.ex.node, ast.Call):
            return self.ex.node

        # `executing` failed to find a node.
        # This shouldn't happen in most cases, but it's best not to rely on it always working.

        if not self.source.text:
            # This is a very likely cause.
            # There's nothing we could possibly do to make magic work here,
            # and it's a clear case where the user should turn the magic off.
            self.warn_inspect_arguments(
                'No source code available. '
                'This happens when running in an interactive shell, '
                'using exec(), or running .pyc files without the source .py files.',
            )
            return None

        msg = '`executing` failed to find a node.'
        if sys.version_info[:2] < (3, 11):  # pragma: no cover
            # inspect_arguments is only on by default for 3.11+ for this reason.
            # The AST modifications made by auto-tracing
            # mean that the bytecode doesn't match the source code seen by `executing`.
            # In 3.11+, a different algorithm is used by `executing` which can deal with this.
            msg += ' This may be caused by a combination of using Python < 3.11 and auto-tracing.'

        # Simple fallback heuristic: check if there's only one possible call node.
        call_nodes = [
            node
            for main_node in self.heuristic_main_nodes()
            for node in ast.walk(main_node)
            if isinstance(node, ast.Call)
            if self.heuristic_call_node_filter(node)
        ]
        if len(call_nodes) != 1:
            self.warn_inspect_arguments(msg)
            return None

        return call_nodes[0]

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

    def warn_inspect_arguments(self, msg: str):
        import logfire

        msg = (
            f'Failed to introspect calling code. Please report this issue to Logfire. '
            f'{self.warn_inspect_arguments_middle()} '
            f'Set inspect_arguments=False in logfire.configure() to suppress this warning. The problem was:\n'
            f'{msg}'
        )
        warnings.warn(msg, InspectArgumentsFailedWarning, stacklevel=self.get_stacklevel())
        logfire.warn(msg)

    def get_stacklevel(self):
        # Get a stacklevel which can be passed to warn_inspect_arguments
        # which points at the given frame, where the f-string was found.
        current_frame = inspect.currentframe()
        stacklevel = 0
        while current_frame:  # pragma: no branch
            if current_frame == self.frame:
                break
            stacklevel += 1
            current_frame = current_frame.f_back
        return stacklevel


class InspectArgumentsFailedWarning(Warning):
    pass


@functools.lru_cache(maxsize=1024)
def get_node_source_text(node: ast.AST, ex_source: executing.Source):
    """Returns some Python source code representing `node`.

    Preferably the actual original code given by `ast.get_source_segment`,
    but falling back to `ast.unparse(node)` if the former is incorrect.
    This happens sometimes due to Python bugs (especially for older Python versions)
    in the source positions of AST nodes inside f-strings.
    """
    source_unparsed = ast.unparse(node)
    source_segment = ast.get_source_segment(ex_source.text, node) or ''
    try:
        # Verify that the source segment is correct by checking that the AST is equivalent to what we have.
        source_segment_unparsed = ast.unparse(ast.parse(source_segment, mode='eval'))
    except Exception:  # probably SyntaxError, but ast.parse can raise other exceptions too
        source_segment_unparsed = ''
    return source_segment if source_unparsed == source_segment_unparsed else source_unparsed
