from __future__ import annotations

import ast
from collections.abc import Iterable

from tokenize_rt import NON_CODING_TOKENS
from tokenize_rt import Offset
from tokenize_rt import Token

from pyupgrade._ast_helpers import ast_to_offset
from pyupgrade._ast_helpers import has_starargs
from pyupgrade._ast_helpers import is_name_attr
from pyupgrade._data import register
from pyupgrade._data import State
from pyupgrade._data import TokenFunc
from pyupgrade._token_helpers import arg_str
from pyupgrade._token_helpers import find_block_start
from pyupgrade._token_helpers import find_op
from pyupgrade._token_helpers import parse_call_args
from pyupgrade._token_helpers import remove_decorator
from pyupgrade._token_helpers import replace_call


def _fix_add_metaclass(i: int, tokens: list[Token]) -> None:
    j = find_op(tokens, i, '(')
    func_args, end = parse_call_args(tokens, j)
    metaclass = f'metaclass={arg_str(tokens, *func_args[0])}'
    # insert `metaclass={args[0]}` into `class:`
    # search forward for the `class` token
    j = i + 1
    while not tokens[j].matches(name='NAME', src='class'):
        j += 1
    class_token = j
    # then search forward for a `:` token, not inside a brace
    j = find_block_start(tokens, j)
    last_paren = -1
    for k in range(class_token, j):
        if tokens[k].src == ')':
            last_paren = k

    if last_paren == -1:
        tokens.insert(j, Token('CODE', f'({metaclass})'))
    else:
        insert = last_paren - 1
        while tokens[insert].name in NON_CODING_TOKENS:
            insert -= 1
        if tokens[insert].src == '(':  # no bases
            src = metaclass
        elif tokens[insert].src != ',':
            src = f', {metaclass}'
        else:
            src = f' {metaclass},'
        tokens.insert(insert + 1, Token('CODE', src))
    remove_decorator(i, tokens)


def _fix_with_metaclass(i: int, tokens: list[Token]) -> None:
    j = find_op(tokens, i, '(')
    func_args, end = parse_call_args(tokens, j)
    if len(func_args) == 1:
        tmpl = 'metaclass={args[0]}'
    elif len(func_args) == 2:
        base = arg_str(tokens, *func_args[1])
        if base == 'object':
            tmpl = 'metaclass={args[0]}'
        else:
            tmpl = '{rest}, metaclass={args[0]}'
    else:
        tmpl = '{rest}, metaclass={args[0]}'
    replace_call(tokens, i, end, func_args, tmpl)


@register(ast.ClassDef)
def visit_ClassDef(
        state: State,
        node: ast.ClassDef,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    for decorator in node.decorator_list:
        if (
                isinstance(decorator, ast.Call) and
                is_name_attr(
                    decorator.func,
                    state.from_imports,
                    ('six',),
                    ('add_metaclass',),
                ) and
                not has_starargs(decorator)
        ):
            yield ast_to_offset(decorator), _fix_add_metaclass

    if (
            len(node.bases) == 1 and
            isinstance(node.bases[0], ast.Call) and
            is_name_attr(
                node.bases[0].func,
                state.from_imports,
                ('six',),
                ('with_metaclass',),
            ) and
            not has_starargs(node.bases[0])
    ):
        yield ast_to_offset(node.bases[0]), _fix_with_metaclass
