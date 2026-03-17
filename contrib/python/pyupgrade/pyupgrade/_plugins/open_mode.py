from __future__ import annotations

import ast
import functools
import itertools
from collections.abc import Iterable
from typing import NamedTuple

from tokenize_rt import Offset
from tokenize_rt import Token
from tokenize_rt import tokens_to_src

from pyupgrade._ast_helpers import ast_to_offset
from pyupgrade._ast_helpers import has_starargs
from pyupgrade._data import register
from pyupgrade._data import State
from pyupgrade._data import TokenFunc
from pyupgrade._token_helpers import delete_argument
from pyupgrade._token_helpers import find_op
from pyupgrade._token_helpers import parse_call_args


def _plus(args: tuple[str, ...]) -> tuple[str, ...]:
    return args + tuple(f'{arg}+' for arg in args)


def _permute(*args: str) -> tuple[str, ...]:
    return tuple(''.join(p) for s in args for p in itertools.permutations(s))


MODE_REMOVE = frozenset(_permute('U', 'r', 'rU', 'rt'))
MODE_REPLACE_R = frozenset(_permute('Ub'))
MODE_REMOVE_T = frozenset(_plus(_permute('at', 'rt', 'wt', 'xt')))
MODE_REMOVE_U = frozenset(_permute('rUb'))
MODE_REPLACE = MODE_REPLACE_R | MODE_REMOVE_T | MODE_REMOVE_U


class FunctionArg(NamedTuple):
    arg_idx: int
    value: ast.expr


def _fix_open_mode(i: int, tokens: list[Token], *, arg_idx: int) -> None:
    j = find_op(tokens, i, '(')
    func_args, end = parse_call_args(tokens, j)
    mode = tokens_to_src(tokens[slice(*func_args[arg_idx])])
    mode_stripped = mode.split('=')[-1]
    mode_stripped = ast.literal_eval(mode_stripped.strip())
    if mode_stripped in MODE_REMOVE:
        delete_argument(arg_idx, tokens, func_args)
    elif mode_stripped in MODE_REPLACE_R:
        new_mode = mode.replace('U', 'r')
        tokens[slice(*func_args[arg_idx])] = [Token('SRC', new_mode)]
    elif mode_stripped in MODE_REMOVE_T:
        new_mode = mode.replace('t', '')
        tokens[slice(*func_args[arg_idx])] = [Token('SRC', new_mode)]
    elif mode_stripped in MODE_REMOVE_U:
        new_mode = mode.replace('U', '')
        tokens[slice(*func_args[arg_idx])] = [Token('SRC', new_mode)]
    else:
        raise AssertionError(f'unreachable: {mode!r}')


@register(ast.Call)
def visit_Call(
        state: State,
        node: ast.Call,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    if (
            (
                (
                    isinstance(node.func, ast.Name) and
                    node.func.id == 'open'
                ) or (
                    isinstance(node.func, ast.Attribute) and
                    isinstance(node.func.value, ast.Name) and
                    node.func.value.id == 'io' and
                    node.func.attr == 'open'
                )
            ) and
            not has_starargs(node)
    ):
        if (
                len(node.args) >= 2 and
                isinstance(node.args[1], ast.Constant) and
                isinstance(node.args[1].value, str)
        ):
            if (
                node.args[1].value in MODE_REPLACE or
                (len(node.args) == 2 and node.args[1].value in MODE_REMOVE)
            ):
                func = functools.partial(_fix_open_mode, arg_idx=1)
                yield ast_to_offset(node), func
        elif node.keywords and (len(node.keywords) + len(node.args) > 1):
            mode = next(
                (
                    FunctionArg(n, keyword.value)
                    for n, keyword in enumerate(node.keywords)
                    if keyword.arg == 'mode'
                ),
                None,
            )
            if (
                mode is not None and
                isinstance(mode.value, ast.Constant) and
                isinstance(mode.value.value, str) and
                (
                    mode.value.value in MODE_REMOVE or
                    mode.value.value in MODE_REPLACE
                )
            ):
                func = functools.partial(
                    _fix_open_mode,
                    arg_idx=len(node.args) + mode.arg_idx,
                )
                yield ast_to_offset(node), func
