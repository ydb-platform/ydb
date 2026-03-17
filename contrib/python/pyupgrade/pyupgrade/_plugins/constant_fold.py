from __future__ import annotations

import ast
from collections.abc import Iterable

from tokenize_rt import Offset

from pyupgrade._ast_helpers import ast_to_offset
from pyupgrade._ast_helpers import is_type_check
from pyupgrade._data import register
from pyupgrade._data import State
from pyupgrade._data import TokenFunc
from pyupgrade._token_helpers import constant_fold_tuple


def _to_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    elif isinstance(node, ast.Attribute):
        base = _to_name(node.value)
        if base is None:
            return None
        else:
            return f'{base}.{node.attr}'
    else:
        return None


def _can_constant_fold(node: ast.Tuple) -> bool:
    seen = set()
    for el in node.elts:
        name = _to_name(el)
        if name is not None:
            if name in seen:
                return True
            else:
                seen.add(name)
    else:
        return False


def _cbs(node: ast.AST | None) -> Iterable[tuple[Offset, TokenFunc]]:
    if isinstance(node, ast.Tuple) and _can_constant_fold(node):
        yield ast_to_offset(node), constant_fold_tuple


@register(ast.Call)
def visit_Call(
        state: State,
        node: ast.Call,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    if is_type_check(node):
        yield from _cbs(node.args[1])


@register(ast.Try)
def visit_Try(
        state: State,
        node: ast.Try,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    for handler in node.handlers:
        yield from _cbs(handler.type)
