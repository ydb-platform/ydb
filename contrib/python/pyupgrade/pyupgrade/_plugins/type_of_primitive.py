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
from pyupgrade._token_helpers import find_closing_bracket
from pyupgrade._token_helpers import find_op

_TYPES = {
    bool: 'bool',
    bytes: 'bytes',
    str: 'str',
    int: 'int',
    float: 'float',
    complex: 'complex',
}


def _rewrite_type_of_primitive(
        i: int,
        tokens: list[Token],
        *,
        src: str,
) -> None:
    open_paren = find_op(tokens, i + 1, '(')
    j = find_closing_bracket(tokens, open_paren)
    tokens[i] = tokens[i]._replace(src=src)
    del tokens[i + 1:j + 1]


@register(ast.Call)
def visit_Call(
        state: State,
        node: ast.Call,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    if (
            isinstance(node.func, ast.Name) and
            node.func.id == 'type' and
            len(node.args) == 1 and
            isinstance(node.args[0], ast.Constant) and
            node.args[0].value not in {Ellipsis, None}
    ):
        func = functools.partial(
            _rewrite_type_of_primitive,
            src=_TYPES[type(node.args[0].value)],
        )
        yield ast_to_offset(node), func
