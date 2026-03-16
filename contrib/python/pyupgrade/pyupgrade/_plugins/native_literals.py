from __future__ import annotations

import ast
import functools
from collections.abc import Iterable

from tokenize_rt import Offset
from tokenize_rt import Token

from pyupgrade._ast_helpers import ast_to_offset
from pyupgrade._ast_helpers import has_starargs
from pyupgrade._ast_helpers import is_name_attr
from pyupgrade._data import register
from pyupgrade._data import State
from pyupgrade._data import TokenFunc
from pyupgrade._token_helpers import find_op
from pyupgrade._token_helpers import parse_call_args
from pyupgrade._token_helpers import replace_call

SIX_NATIVE_STR = frozenset(('ensure_str', 'ensure_text', 'text_type'))


def _fix_literal(i: int, tokens: list[Token], *, empty: str) -> None:
    j = find_op(tokens, i, '(')
    func_args, end = parse_call_args(tokens, j)
    if any(tok.name == 'NL' for tok in tokens[i:end]):
        return
    if func_args:
        replace_call(tokens, i, end, func_args, '{args[0]}')
    else:
        tokens[i:end] = [tokens[i]._replace(name='STRING', src=empty)]


def is_a_native_literal_call(
        node: ast.Call,
        from_imports: dict[str, set[str]],
) -> bool:
    return (
        (
            is_name_attr(node.func, from_imports, ('six',), SIX_NATIVE_STR) or
            isinstance(node.func, ast.Name) and node.func.id == 'str'
        ) and
        not node.keywords and
        not has_starargs(node) and
        (
            len(node.args) == 0 or
            (
                len(node.args) == 1 and
                isinstance(node.args[0], ast.Constant) and
                isinstance(node.args[0].value, str)
            )
        )
    )


@register(ast.Call)
def visit_Call(
        state: State,
        node: ast.Call,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    if is_a_native_literal_call(node, state.from_imports):
        func = functools.partial(_fix_literal, empty="''")
        yield ast_to_offset(node), func
    elif (
            isinstance(node.func, ast.Name) and node.func.id == 'bytes' and
            not node.keywords and not has_starargs(node) and
            (
                len(node.args) == 0 or (
                    len(node.args) == 1 and
                    isinstance(node.args[0], ast.Constant) and
                    isinstance(node.args[0].value, bytes)
                )
            )
    ):
        func = functools.partial(_fix_literal, empty="b''")
        yield ast_to_offset(node), func
