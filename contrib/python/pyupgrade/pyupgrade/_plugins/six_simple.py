from __future__ import annotations

import ast
import functools
from collections.abc import Iterable

from tokenize_rt import Offset

from pyupgrade._ast_helpers import ast_to_offset
from pyupgrade._ast_helpers import is_type_check
from pyupgrade._data import register
from pyupgrade._data import State
from pyupgrade._data import TokenFunc
from pyupgrade._plugins.imports import REMOVALS
from pyupgrade._plugins.native_literals import is_a_native_literal_call
from pyupgrade._token_helpers import replace_name

NAMES = {
    'text_type': 'str',
    'binary_type': 'bytes',
    'class_types': '(type,)',
    'string_types': '(str,)',
    'integer_types': '(int,)',
    'unichr': 'chr',
    'iterbytes': 'iter',
    'print_': 'print',
    'exec_': 'exec',
    'advance_iterator': 'next',
    'next': 'next',
    'callable': 'callable',
}
NAMES_MOVES = REMOVALS[(3,)]['six.moves']
NAMES_TYPE_CTX = {
    'class_types': 'type',
    'string_types': 'str',
    'integer_types': 'int',
}


@register(ast.Attribute)
def visit_Attribute(
        state: State,
        node: ast.Attribute,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    if (
            isinstance(node.value, ast.Name) and
            node.value.id == 'six' and
            node.attr in NAMES
    ):
        # these will be handled by the native literals plugin
        if (
                isinstance(parent, ast.Call) and
                is_a_native_literal_call(parent, state.from_imports)
        ):
            return

        if node.attr in NAMES_TYPE_CTX and is_type_check(parent):
            new = NAMES_TYPE_CTX[node.attr]
        else:
            new = NAMES[node.attr]

        func = functools.partial(replace_name, name=node.attr, new=new)
        yield ast_to_offset(node), func
    elif (
            isinstance(node.value, ast.Attribute) and
            isinstance(node.value.value, ast.Name) and
            node.value.value.id == 'six' and
            node.value.attr == 'moves' and
            node.attr == 'xrange'
    ):
        func = functools.partial(replace_name, name=node.attr, new='range')
        yield ast_to_offset(node), func
    elif (
            isinstance(node.value, ast.Attribute) and
            isinstance(node.value.value, ast.Name) and
            node.value.value.id == 'six' and
            node.value.attr == 'moves' and
            node.attr in NAMES_MOVES
    ):
        func = functools.partial(replace_name, name=node.attr, new=node.attr)
        yield ast_to_offset(node), func


@register(ast.Name)
def visit_Name(
        state: State,
        node: ast.Name,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    if (
            node.id in state.from_imports['six'] and
            node.id in NAMES
    ):
        # these will be handled by the native literals plugin
        if (
                isinstance(parent, ast.Call) and
                is_a_native_literal_call(parent, state.from_imports)
        ):
            return

        if node.id in NAMES_TYPE_CTX and is_type_check(parent):
            new = NAMES_TYPE_CTX[node.id]
        else:
            new = NAMES[node.id]

        func = functools.partial(replace_name, name=node.id, new=new)
        yield ast_to_offset(node), func
    elif (
            node.id in state.from_imports['six.moves'] and
            node.id in {'xrange', 'range'}
    ):
        func = functools.partial(replace_name, name=node.id, new='range')
        yield ast_to_offset(node), func
