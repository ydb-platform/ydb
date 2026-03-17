from __future__ import annotations

import ast
import functools
from collections.abc import Iterable

from tokenize_rt import Offset
from tokenize_rt import Token
from tokenize_rt import UNIMPORTANT_WS

from pyupgrade._ast_helpers import ast_to_offset
from pyupgrade._data import register
from pyupgrade._data import State
from pyupgrade._data import TokenFunc
from pyupgrade._token_helpers import immediately_paren
from pyupgrade._token_helpers import remove_brace
from pyupgrade._token_helpers import victims


def _fix_dict_comp(
        i: int,
        tokens: list[Token],
        arg: ast.ListComp | ast.GeneratorExp,
) -> None:
    if not immediately_paren('dict', tokens, i):
        return

    dict_victims = victims(tokens, i + 1, arg, gen=True)
    elt_victims = victims(tokens, dict_victims.arg_index, arg.elt, gen=True)

    del dict_victims.starts[0]
    end_index = dict_victims.ends.pop()

    tokens[end_index] = Token('OP', '}')
    for index in reversed(dict_victims.ends):
        remove_brace(tokens, index)
    # See #6, Fix SyntaxError from rewriting dict((a, b)for a, b in y)
    if tokens[elt_victims.ends[-1] + 1].src == 'for':
        tokens.insert(elt_victims.ends[-1] + 1, Token(UNIMPORTANT_WS, ' '))
    for index in reversed(elt_victims.ends):
        remove_brace(tokens, index)
    assert elt_victims.first_comma_index is not None
    tokens[elt_victims.first_comma_index] = Token('OP', ':')
    for index in reversed(dict_victims.starts + elt_victims.starts):
        remove_brace(tokens, index)
    tokens[i:i + 2] = [Token('OP', '{')]


@register(ast.Call)
def visit_Call(
        state: State,
        node: ast.Call,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    if (
            not isinstance(parent, ast.FormattedValue) and
            isinstance(node.func, ast.Name) and
            node.func.id == 'dict' and
            len(node.args) == 1 and
            not node.keywords and
            isinstance(node.args[0], (ast.ListComp, ast.GeneratorExp)) and
            isinstance(node.args[0].elt, (ast.Tuple, ast.List)) and
            len(node.args[0].elt.elts) == 2
    ):
        func = functools.partial(_fix_dict_comp, arg=node.args[0])
        yield ast_to_offset(node.func), func
