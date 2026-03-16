from __future__ import annotations

import ast
from collections.abc import Iterable

from tokenize_rt import Offset
from tokenize_rt import Token

from pyupgrade._ast_helpers import ast_to_offset
from pyupgrade._ast_helpers import is_name_attr
from pyupgrade._data import register
from pyupgrade._data import State
from pyupgrade._data import TokenFunc
from pyupgrade._token_helpers import find_closing_bracket
from pyupgrade._token_helpers import find_op
from pyupgrade._token_helpers import remove_brace


def _replace_unpack_with_star(i: int, tokens: list[Token]) -> None:
    start = find_op(tokens, i, '[')
    end = find_closing_bracket(tokens, start)

    remove_brace(tokens, end)
    # replace `Unpack` with `*`
    tokens[i:start + 1] = [tokens[i]._replace(name='OP', src='*')]


@register(ast.Subscript)
def visit_Subscript(
    state: State,
    node: ast.Subscript,
    parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    if state.settings.min_version < (3, 11):
        return

    if is_name_attr(node.value, state.from_imports, ('typing',), ('Unpack',)):
        if isinstance(parent, ast.Subscript):
            yield ast_to_offset(node.value), _replace_unpack_with_star


def _visit_func(
        state: State,
        node: ast.AsyncFunctionDef | ast.FunctionDef,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    if state.settings.min_version < (3, 11):
        return

    vararg = node.args.vararg
    if (
            vararg is not None and
            isinstance(vararg.annotation, ast.Subscript) and
            is_name_attr(
                vararg.annotation.value,
                state.from_imports,
                ('typing',), ('Unpack',),
            )
    ):
        yield ast_to_offset(vararg.annotation.value), _replace_unpack_with_star


@register(ast.AsyncFunctionDef)
def visit_AsyncFunctionDef(
        state: State,
        node: ast.AsyncFunctionDef,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    yield from _visit_func(state, node, parent)


@register(ast.FunctionDef)
def visit_FunctionDef(
        state: State,
        node: ast.FunctionDef,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    yield from _visit_func(state, node, parent)
