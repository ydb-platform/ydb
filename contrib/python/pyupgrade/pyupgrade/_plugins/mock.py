from __future__ import annotations

import ast
from collections.abc import Iterable

from tokenize_rt import Offset
from tokenize_rt import Token

from pyupgrade._ast_helpers import ast_to_offset
from pyupgrade._data import register
from pyupgrade._data import State
from pyupgrade._data import TokenFunc
from pyupgrade._token_helpers import find_name


def _fix_mock_mock(i: int, tokens: list[Token]) -> None:
    j = find_name(tokens, i + 1, 'mock')
    del tokens[i + 1:j + 1]


@register(ast.Attribute)
def visit_Attribute(
        state: State,
        node: ast.Attribute,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    if (
            not state.settings.keep_mock and
            isinstance(node.value, ast.Name) and
            node.value.id == 'mock' and
            node.attr == 'mock'
    ):
        yield ast_to_offset(node), _fix_mock_mock
