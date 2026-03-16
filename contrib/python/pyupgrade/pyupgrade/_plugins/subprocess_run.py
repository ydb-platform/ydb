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
from pyupgrade._token_helpers import delete_argument
from pyupgrade._token_helpers import find_op
from pyupgrade._token_helpers import parse_call_args
from pyupgrade._token_helpers import replace_argument


def _use_capture_output(
    i: int,
    tokens: list[Token],
    *,
    stdout_arg_idx: int,
    stderr_arg_idx: int,
) -> None:
    j = find_op(tokens, i, '(')
    func_args, _ = parse_call_args(tokens, j)
    if stdout_arg_idx < stderr_arg_idx:
        delete_argument(stderr_arg_idx, tokens, func_args)
        replace_argument(
            stdout_arg_idx,
            tokens,
            func_args,
            new='capture_output=True',
        )
    else:
        replace_argument(
            stdout_arg_idx,
            tokens,
            func_args,
            new='capture_output=True',
        )
        delete_argument(stderr_arg_idx, tokens, func_args)


def _replace_universal_newlines_with_text(
    i: int,
    tokens: list[Token],
    *,
    arg_idx: int,
) -> None:
    j = find_op(tokens, i, '(')
    func_args, _ = parse_call_args(tokens, j)
    for i in range(*func_args[arg_idx]):
        if tokens[i].src == 'universal_newlines':
            tokens[i] = tokens[i]._replace(src='text')
            break
    else:
        raise AssertionError('`universal_newlines` argument not found')


@register(ast.Call)
def visit_Call(
        state: State,
        node: ast.Call,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    if (
            state.settings.min_version >= (3, 7) and
            is_name_attr(
                node.func,
                state.from_imports,
                ('subprocess',),
                ('check_output', 'run'),
            )
    ):
        stdout_idx = None
        stderr_idx = None
        universal_newlines_idx = None
        skip_universal_newlines_rewrite = False
        for n, keyword in enumerate(node.keywords):
            if keyword.arg == 'stdout' and is_name_attr(
                keyword.value,
                state.from_imports,
                ('subprocess',),
                ('PIPE',),
            ):
                stdout_idx = n
            elif keyword.arg == 'stderr' and is_name_attr(
                keyword.value,
                state.from_imports,
                ('subprocess',),
                ('PIPE',),
            ):
                stderr_idx = n
            elif keyword.arg == 'universal_newlines':
                universal_newlines_idx = n
            elif keyword.arg == 'text' or keyword.arg is None:
                skip_universal_newlines_rewrite = True
        if (
                universal_newlines_idx is not None and
                not skip_universal_newlines_rewrite
        ):
            func = functools.partial(
                _replace_universal_newlines_with_text,
                arg_idx=len(node.args) + universal_newlines_idx,
            )
            yield ast_to_offset(node), func
        if stdout_idx is not None and stderr_idx is not None:
            func = functools.partial(
                _use_capture_output,
                stdout_arg_idx=len(node.args) + stdout_idx,
                stderr_arg_idx=len(node.args) + stderr_idx,
            )
            yield ast_to_offset(node), func
