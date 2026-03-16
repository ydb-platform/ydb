from __future__ import annotations

import ast
import functools
from collections.abc import Iterable

from tokenize_rt import Offset

from pyupgrade._ast_helpers import ast_to_offset
from pyupgrade._data import register
from pyupgrade._data import State
from pyupgrade._data import TokenFunc
from pyupgrade._plugins.imports import REPLACE_EXACT
from pyupgrade._token_helpers import replace_name

COLLECTIONS_ABC_ATTRS = frozenset(
    attr for mod, attr in REPLACE_EXACT[(3,)] if mod == 'collections'
)


@register(ast.Attribute)
def visit_Attribute(
        state: State,
        node: ast.Attribute,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    if (
            isinstance(node.value, ast.Name) and
            node.value.id == 'collections' and
            node.attr in COLLECTIONS_ABC_ATTRS
    ):
        new_attr = f'collections.abc.{node.attr}'
        func = functools.partial(replace_name, name=node.attr, new=new_attr)
        yield ast_to_offset(node), func
