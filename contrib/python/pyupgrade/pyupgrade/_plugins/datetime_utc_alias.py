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
            state.settings.min_version >= (3, 11) and
            node.attr == 'utc' and
            isinstance(node.value, ast.Attribute) and
            node.value.attr == 'timezone' and
            isinstance(node.value.value, ast.Name) and
            node.value.value.id == 'datetime'
    ):
        func = functools.partial(
            replace_name,
            name='utc',
            new='datetime.UTC',
        )
        yield ast_to_offset(node), func
