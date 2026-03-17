from __future__ import annotations

import ast
from collections.abc import Iterable

from tokenize_rt import Offset
from tokenize_rt import Token

from pyupgrade._ast_helpers import ast_to_offset
from pyupgrade._ast_helpers import has_starargs
from pyupgrade._data import register
from pyupgrade._data import State
from pyupgrade._data import TokenFunc
from pyupgrade._string_helpers import is_codec
from pyupgrade._token_helpers import find_call
from pyupgrade._token_helpers import find_closing_bracket


def _fix_default_encoding(i: int, tokens: list[Token]) -> None:
    i = find_call(tokens, i + 1)
    j = find_closing_bracket(tokens, i)
    del tokens[i + 1:j]


@register(ast.Call)
def visit_Call(
        state: State,
        node: ast.Call,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    if (
            isinstance(node.func, ast.Attribute) and (
                (
                    isinstance(node.func.value, ast.Constant) and
                    isinstance(node.func.value.value, str)
                ) or
                isinstance(node.func.value, ast.JoinedStr)
            ) and
            node.func.attr == 'encode' and
            not has_starargs(node) and
            len(node.args) == 1 and
            isinstance(node.args[0], ast.Constant) and
            isinstance(node.args[0].value, str) and
            is_codec(node.args[0].value, 'utf-8')
    ):
        yield ast_to_offset(node), _fix_default_encoding
