from __future__ import annotations

import ast
from collections.abc import Iterable

from tokenize_rt import Offset
from tokenize_rt import Token

from pyupgrade._ast_helpers import ast_to_offset
from pyupgrade._data import register
from pyupgrade._data import State
from pyupgrade._data import TokenFunc
from pyupgrade._token_helpers import find_end


def _remove_metaclass_type(i: int, tokens: list[Token]) -> None:
    j = find_end(tokens, i)
    del tokens[i:j]


@register(ast.Assign)
def visit_Assign(
        state: State,
        node: ast.Assign,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    if (
            len(node.targets) == 1 and
            isinstance(node.targets[0], ast.Name) and
            node.targets[0].col_offset == 0 and
            node.targets[0].id == '__metaclass__' and
            isinstance(node.value, ast.Name) and
            node.value.id == 'type'
    ):
        yield ast_to_offset(node), _remove_metaclass_type
