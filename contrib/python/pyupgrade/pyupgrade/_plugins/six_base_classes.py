from __future__ import annotations

import ast
from collections.abc import Iterable

from tokenize_rt import Offset

from pyupgrade._ast_helpers import ast_to_offset
from pyupgrade._ast_helpers import is_name_attr
from pyupgrade._data import register
from pyupgrade._data import State
from pyupgrade._data import TokenFunc
from pyupgrade._token_helpers import remove_base_class


@register(ast.ClassDef)
def visit_ClassDef(
        state: State,
        node: ast.ClassDef,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    for base in node.bases:
        if is_name_attr(base, state.from_imports, ('six',), ('Iterator',)):
            yield ast_to_offset(base), remove_base_class
