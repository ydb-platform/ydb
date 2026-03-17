from __future__ import annotations

import ast
from collections.abc import Iterable

from tokenize_rt import Offset
from tokenize_rt import rfind_string_parts
from tokenize_rt import Token

from pyupgrade._ast_helpers import ast_to_offset
from pyupgrade._data import register
from pyupgrade._data import State
from pyupgrade._data import TokenFunc
from pyupgrade._token_helpers import find_closing_bracket
from pyupgrade._token_helpers import find_op


def _fix(i: int, tokens: list[Token]) -> None:
    dot_pos = find_op(tokens, i, '.')
    open_pos = find_op(tokens, dot_pos, '(')
    close_pos = find_closing_bracket(tokens, open_pos)
    for string_idx in rfind_string_parts(tokens, dot_pos - 1):
        tok = tokens[string_idx]
        tokens[string_idx] = tok._replace(src=f'f{tok.src}')
    del tokens[dot_pos:close_pos + 1]


@register(ast.Call)
def visit_Call(
        state: State,
        node: ast.Call,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    if (
            state.settings.min_version >= (3, 6) and
            isinstance(node.func, ast.Attribute) and
            isinstance(node.func.value, ast.Constant) and
            isinstance(node.func.value.value, str) and
            node.func.attr == 'format' and
            len(node.args) == 0 and
            len(node.keywords) == 1 and
            node.keywords[0].arg is None and
            isinstance(node.keywords[0].value, ast.Call) and
            isinstance(node.keywords[0].value.func, ast.Name) and
            node.keywords[0].value.func.id == 'locals' and
            len(node.keywords[0].value.args) == 0 and
            len(node.keywords[0].value.keywords) == 0
    ):
        yield ast_to_offset(node), _fix
