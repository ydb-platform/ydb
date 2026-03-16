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
from pyupgrade._token_helpers import find_op
from pyupgrade._token_helpers import parse_call_args


def _fix_typevar_default(i: int, tokens: list[Token]) -> None:
    j = find_op(tokens, i, '[')
    args, end = parse_call_args(tokens, j)
    # remove the trailing `None` arguments
    del tokens[args[0][1]:args[-1][1]]


def _should_rewrite(state: State) -> bool:
    return (
        state.settings.min_version >= (3, 13) or (
            not state.settings.keep_runtime_typing and
            state.in_annotation and
            'annotations' in state.from_imports['__future__']
        )
    )


def _is_none(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and node.value is None


@register(ast.Subscript)
def visit_Subscript(
        state: State,
        node: ast.Subscript,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    if not _should_rewrite(state):
        return

    if (
            is_name_attr(
                node.value,
                state.from_imports,
                ('collections.abc', 'typing', 'typing_extensions'),
                ('Generator',),
            ) and
            isinstance(node.slice, ast.Tuple) and
            len(node.slice.elts) == 3 and
            _is_none(node.slice.elts[1]) and
            _is_none(node.slice.elts[2])
    ):
        yield ast_to_offset(node), _fix_typevar_default
    elif (
            is_name_attr(
                node.value,
                state.from_imports,
                ('collections.abc', 'typing', 'typing_extensions'),
                ('AsyncGenerator',),
            ) and
            isinstance(node.slice, ast.Tuple) and
            len(node.slice.elts) == 2 and
            _is_none(node.slice.elts[1])
    ):
        yield ast_to_offset(node), _fix_typevar_default
