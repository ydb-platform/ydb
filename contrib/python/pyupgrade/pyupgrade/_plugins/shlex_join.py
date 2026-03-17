from __future__ import annotations

import ast
import functools
from collections.abc import Iterable

from tokenize_rt import NON_CODING_TOKENS
from tokenize_rt import Offset
from tokenize_rt import Token

from pyupgrade._ast_helpers import ast_to_offset
from pyupgrade._data import register
from pyupgrade._data import State
from pyupgrade._data import TokenFunc
from pyupgrade._token_helpers import find_name
from pyupgrade._token_helpers import find_op
from pyupgrade._token_helpers import victims


def _fix_shlex_join(i: int, tokens: list[Token], *, arg: ast.expr) -> None:
    j = find_op(tokens, i, '(')
    comp_victims = victims(tokens, j, arg, gen=True)
    k = find_name(tokens, comp_victims.arg_index, 'in') + 1
    while tokens[k].name in NON_CODING_TOKENS:
        k += 1
    tokens[comp_victims.ends[0]:comp_victims.ends[-1] + 1] = [Token('OP', ')')]
    tokens[i:k] = [Token('CODE', 'shlex.join'), Token('OP', '(')]


@register(ast.Call)
def visit_Call(
        state: State,
        node: ast.Call,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    if state.settings.min_version < (3, 8):
        return

    if (
            isinstance(node.func, ast.Attribute) and
            isinstance(node.func.value, ast.Constant) and
            node.func.value.value == ' ' and
            node.func.attr == 'join' and
            not node.keywords and
            len(node.args) == 1 and
            isinstance(node.args[0], (ast.ListComp, ast.GeneratorExp)) and
            isinstance(node.args[0].elt, ast.Call) and
            isinstance(node.args[0].elt.func, ast.Attribute) and
            isinstance(node.args[0].elt.func.value, ast.Name) and
            node.args[0].elt.func.value.id == 'shlex' and
            node.args[0].elt.func.attr == 'quote' and
            not node.args[0].elt.keywords and
            len(node.args[0].elt.args) == 1 and
            isinstance(node.args[0].elt.args[0], ast.Name) and
            len(node.args[0].generators) == 1 and
            isinstance(node.args[0].generators[0].target, ast.Name) and
            not node.args[0].generators[0].ifs and
            not node.args[0].generators[0].is_async and
            node.args[0].elt.args[0].id == node.args[0].generators[0].target.id
    ):
        func = functools.partial(_fix_shlex_join, arg=node.args[0])
        yield ast_to_offset(node), func
