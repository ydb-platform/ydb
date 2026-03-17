from __future__ import annotations

import ast
import functools
from collections.abc import Iterable

from tokenize_rt import Offset

from pyupgrade._ast_helpers import ast_to_offset
from pyupgrade._data import register
from pyupgrade._data import State
from pyupgrade._data import TokenFunc
from pyupgrade._token_helpers import replace_name


@register(ast.Attribute)
def visit_Attribute(
        state: State,
        node: ast.Attribute,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    if (
            isinstance(node.value, ast.Name) and
            node.value.id == 'typing' and
            node.attr == 'Text'
    ):
        func = functools.partial(replace_name, name=node.attr, new='str')
        yield ast_to_offset(node), func


@register(ast.Name)
def visit_Name(
        state: State,
        node: ast.Name,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    if node.id in state.from_imports['typing'] and node.id == 'Text':
        func = functools.partial(replace_name, name=node.id, new='str')
        yield ast_to_offset(node), func
