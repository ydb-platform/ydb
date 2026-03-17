from __future__ import annotations

import ast
import functools
from collections.abc import Iterable

from tokenize_rt import Offset
from tokenize_rt import Token

from pyupgrade._ast_helpers import ast_to_offset
from pyupgrade._ast_helpers import is_name_attr
from pyupgrade._data import register
from pyupgrade._data import State
from pyupgrade._data import TokenFunc
from pyupgrade._token_helpers import find_and_replace_call
from pyupgrade._token_helpers import find_op


def _remove_call(i: int, tokens: list[Token]) -> None:
    i = find_op(tokens, i, '(')
    j = find_op(tokens, i, ')')
    del tokens[i:j + 1]


def _is_literal_kwarg(
        keyword: ast.keyword, name: str, value: bool | None,
) -> bool:
    return (
        keyword.arg == name and
        isinstance(keyword.value, ast.Constant) and
        keyword.value.value is value
    )


def _eligible(keywords: list[ast.keyword]) -> bool:
    if len(keywords) == 1:
        return _is_literal_kwarg(keywords[0], 'maxsize', None)
    elif len(keywords) == 2:
        return (
            (
                _is_literal_kwarg(keywords[0], 'maxsize', None) and
                _is_literal_kwarg(keywords[1], 'typed', False)
            ) or
            (
                _is_literal_kwarg(keywords[1], 'maxsize', None) and
                _is_literal_kwarg(keywords[0], 'typed', False)
            )
        )
    else:
        return False


@register(ast.Call)
def visit_Call(
        state: State,
        node: ast.Call,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    if (
            state.settings.min_version >= (3, 8) and
            not node.args and
            not node.keywords and
            is_name_attr(
                node.func,
                state.from_imports,
                ('functools',),
                ('lru_cache',),
            )
    ):
        yield ast_to_offset(node), _remove_call
    elif (
            state.settings.min_version >= (3, 9) and
            isinstance(node.func, ast.Attribute) and
            node.func.attr == 'lru_cache' and
            isinstance(node.func.value, ast.Name) and
            node.func.value.id == 'functools' and
            not node.args and
            _eligible(node.keywords)
    ):
        func = functools.partial(
            find_and_replace_call, template='functools.cache',
        )
        yield ast_to_offset(node), func
