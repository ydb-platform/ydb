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
from pyupgrade._token_helpers import find_op
from pyupgrade._token_helpers import parse_call_args


def _eligible_lambda_replacement(lambda_expr: ast.Lambda) -> str | None:
    if isinstance(lambda_expr.body, ast.Constant):
        if lambda_expr.body.value == 0:
            return type(lambda_expr.body.value).__name__
        elif lambda_expr.body.value == '':
            return 'str'
        else:
            return None
    elif isinstance(lambda_expr.body, ast.List) and not lambda_expr.body.elts:
        return 'list'
    elif isinstance(lambda_expr.body, ast.Tuple) and not lambda_expr.body.elts:
        return 'tuple'
    elif isinstance(lambda_expr.body, ast.Dict) and not lambda_expr.body.keys:
        return 'dict'
    elif (
            isinstance(lambda_expr.body, ast.Call) and
            isinstance(lambda_expr.body.func, ast.Name) and
            not lambda_expr.body.args and
            not lambda_expr.body.keywords and
            lambda_expr.body.func.id in {'dict', 'list', 'set', 'tuple'}
    ):
        return lambda_expr.body.func.id
    else:
        return None


def _fix_defaultdict_first_arg(
        i: int,
        tokens: list[Token],
        *,
        replacement: str,
) -> None:
    start = find_op(tokens, i, '(')
    func_args, end = parse_call_args(tokens, start)

    tokens[slice(*func_args[0])] = [Token('CODE', replacement)]


@register(ast.Call)
def visit_Call(
        state: State,
        node: ast.Call,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    if (
            is_name_attr(
                node.func,
                state.from_imports,
                ('collections',),
                ('defaultdict',),
            ) and
            node.args and
            isinstance(node.args[0], ast.Lambda)
    ):
        replacement = _eligible_lambda_replacement(node.args[0])
        if replacement is None:
            return

        func = functools.partial(
            _fix_defaultdict_first_arg,
            replacement=replacement,
        )
        yield ast_to_offset(node), func
