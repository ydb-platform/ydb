from __future__ import annotations

import ast
from collections.abc import Iterable

from tokenize_rt import Offset
from tokenize_rt import Token

from pyupgrade._ast_helpers import ast_to_offset
from pyupgrade._ast_helpers import is_async_listcomp
from pyupgrade._data import register
from pyupgrade._data import State
from pyupgrade._data import TokenFunc
from pyupgrade._token_helpers import find_closing_bracket


def _replace_list_comprehension(i: int, tokens: list[Token]) -> None:
    start = i
    end = find_closing_bracket(tokens, start)
    tokens[start] = tokens[start]._replace(src='(')
    tokens[end] = tokens[end]._replace(src=')')


@register(ast.Assign)
def visit_Assign(
        state: State,
        node: ast.Assign,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    if (
            len(node.targets) == 1 and
            isinstance(node.targets[0], ast.Tuple) and
            isinstance(node.value, ast.ListComp) and
            not is_async_listcomp(node.value)
    ):
        yield ast_to_offset(node.value), _replace_list_comprehension
