from __future__ import annotations

import ast
from collections.abc import Iterable

from tokenize_rt import Offset
from tokenize_rt import Token

from pyupgrade._ast_helpers import ast_to_offset
from pyupgrade._data import register
from pyupgrade._data import State
from pyupgrade._data import TokenFunc
from pyupgrade._token_helpers import find_op


def _replace_io_open(i: int, tokens: list[Token]) -> None:
    j = find_op(tokens, i, '(')
    tokens[i:j] = [tokens[i]._replace(name='NAME', src='open')]


@register(ast.Call)
def visit_Call(
        state: State,
        node: ast.Call,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    if (
            isinstance(node.func, ast.Attribute) and
            isinstance(node.func.value, ast.Name) and
            node.func.value.id == 'io' and
            node.func.attr == 'open'
    ):
        yield ast_to_offset(node.func), _replace_io_open
