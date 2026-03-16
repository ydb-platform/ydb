from __future__ import annotations

import ast
from collections.abc import Iterable

from tokenize_rt import Offset

from pyupgrade._ast_helpers import ast_to_offset
from pyupgrade._ast_helpers import is_name_attr
from pyupgrade._data import register
from pyupgrade._data import State
from pyupgrade._data import TokenFunc
from pyupgrade._token_helpers import remove_decorator


@register(ast.ClassDef)
def visit_ClassDef(
        state: State,
        node: ast.ClassDef,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    for decorator in node.decorator_list:
        if is_name_attr(
                decorator,
                state.from_imports,
                ('six',),
                ('python_2_unicode_compatible',),
        ):
            yield ast_to_offset(decorator), remove_decorator
