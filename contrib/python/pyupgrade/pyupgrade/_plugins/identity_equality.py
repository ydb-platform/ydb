from __future__ import annotations

import ast
import functools
from collections.abc import Iterable

from tokenize_rt import Offset
from tokenize_rt import Token

from pyupgrade._ast_helpers import ast_to_offset
from pyupgrade._data import register
from pyupgrade._data import State
from pyupgrade._data import TokenFunc


def _fix_is_literal(
        i: int,
        tokens: list[Token],
        *,
        op: ast.Is | ast.IsNot,
) -> None:
    while tokens[i].src != 'is':
        i -= 1
    if isinstance(op, ast.Is):
        tokens[i] = tokens[i]._replace(src='==')
    else:
        tokens[i] = tokens[i]._replace(src='!=')
        # since we iterate backward, the empty tokens keep the same length
        i += 1
        while tokens[i].src != 'not':
            tokens[i] = Token('EMPTY', '')
            i += 1
        tokens[i] = Token('EMPTY', '')


def _is_literal(n: ast.AST) -> bool:
    return (
        isinstance(n, ast.Constant) and
        n.value not in {True, False} and
        isinstance(n.value, (str, bytes, int, float))
    )


@register(ast.Compare)
def visit_Compare(
        state: State,
        node: ast.Compare,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    left = node.left
    for op, right in zip(node.ops, node.comparators):
        if (
                isinstance(op, (ast.Is, ast.IsNot)) and
                (_is_literal(left) or _is_literal(right))
        ):
            func = functools.partial(_fix_is_literal, op=op)
            yield ast_to_offset(right), func
        left = right
