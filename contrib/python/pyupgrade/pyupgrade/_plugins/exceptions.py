from __future__ import annotations

import ast
import functools
from collections.abc import Iterable
from typing import NamedTuple

from tokenize_rt import Offset
from tokenize_rt import Token

from pyupgrade._ast_helpers import ast_to_offset
from pyupgrade._data import register
from pyupgrade._data import State
from pyupgrade._data import TokenFunc
from pyupgrade._data import Version
from pyupgrade._token_helpers import constant_fold_tuple
from pyupgrade._token_helpers import find_op
from pyupgrade._token_helpers import parse_call_args
from pyupgrade._token_helpers import replace_name


class _Target(NamedTuple):
    target: str
    module: str | None
    name: str
    min_version: Version


_TARGETS = (
    _Target('OSError', 'mmap', 'error', (3,)),
    _Target('OSError', 'os', 'error', (3,)),
    _Target('OSError', 'select', 'error', (3,)),
    _Target('OSError', 'socket', 'error', (3,)),
    _Target('OSError', None, 'IOError', (3,)),
    _Target('OSError', None, 'EnvironmentError', (3,)),
    _Target('OSError', None, 'WindowsError', (3,)),
    _Target('TimeoutError', 'socket', 'timeout', (3, 10)),
    _Target('TimeoutError', 'asyncio', 'TimeoutError', (3, 11)),
)


def _fix_except(
        i: int,
        tokens: list[Token],
        *,
        at_idx: dict[int, _Target],
) -> None:
    start = find_op(tokens, i, '(')
    func_args, end = parse_call_args(tokens, start)

    for i, target in reversed(at_idx.items()):
        tokens[slice(*func_args[i])] = [Token('NAME', target.target)]

    constant_fold_tuple(start, tokens)


def _get_rewrite(
        node: ast.AST,
        state: State,
        targets: list[_Target],
) -> _Target | None:
    for target in targets:
        if (
                target.module is None and
                isinstance(node, ast.Name) and
                node.id == target.name
        ):
            return target
        elif (
                target.module is not None and
                isinstance(node, ast.Name) and
                node.id == target.name and
                node.id in state.from_imports[target.module]
        ):
            return target
        elif (
                target.module is not None and
                isinstance(node, ast.Attribute) and
                isinstance(node.value, ast.Name) and
                node.attr == target.name and
                node.value.id == target.module
        ):
            return target
    else:
        return None


def _alias_cbs(
        node: ast.expr,
        state: State,
        targets: list[_Target],
) -> Iterable[tuple[Offset, TokenFunc]]:
    target = _get_rewrite(node, state, targets)
    if target is not None:
        func = functools.partial(
            replace_name,
            name=target.name,
            new=target.target,
        )
        yield ast_to_offset(node), func


@register(ast.Raise)
def visit_Raise(
        state: State,
        node: ast.Raise,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    targets = [
        target for target in _TARGETS
        if state.settings.min_version >= target.min_version
    ]
    if node.exc is not None:
        yield from _alias_cbs(node.exc, state, targets)
        if isinstance(node.exc, ast.Call):
            yield from _alias_cbs(node.exc.func, state, targets)


@register(ast.Try)
def visit_Try(
        state: State,
        node: ast.Try,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    targets = [
        target for target in _TARGETS
        if state.settings.min_version >= target.min_version
    ]
    for handler in node.handlers:
        if isinstance(handler.type, ast.Tuple):
            at_idx = {}
            for i, elt in enumerate(handler.type.elts):
                target = _get_rewrite(elt, state, targets)
                if target is not None:
                    at_idx[i] = target

            if at_idx:
                func = functools.partial(_fix_except, at_idx=at_idx)
                yield ast_to_offset(handler.type), func
        elif handler.type is not None:
            yield from _alias_cbs(handler.type, state, targets)
