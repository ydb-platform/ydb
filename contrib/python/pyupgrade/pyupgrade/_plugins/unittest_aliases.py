from __future__ import annotations

import ast
import functools
from collections.abc import Iterable

from tokenize_rt import Offset

from pyupgrade._ast_helpers import ast_to_offset
from pyupgrade._ast_helpers import has_starargs
from pyupgrade._data import register
from pyupgrade._data import State
from pyupgrade._data import TokenFunc
from pyupgrade._token_helpers import replace_name

METHOD_MAPPING = {
    'assertEquals': 'assertEqual',
    'failUnlessEqual': 'assertEqual',
    'failIfEqual': 'assertNotEqual',
    'failUnless': 'assertTrue',
    'assert_': 'assertTrue',
    'failIf': 'assertFalse',
    'failUnlessRaises': 'assertRaises',
    'failUnlessAlmostEqual': 'assertAlmostEqual',
    'failIfAlmostEqual': 'assertNotAlmostEqual',
    'assertNotEquals': 'assertNotEqual',
    'assertAlmostEquals': 'assertAlmostEqual',
    'assertNotAlmostEquals': 'assertNotAlmostEqual',
    'assertRegexpMatches': 'assertRegex',
    'assertNotRegexpMatches': 'assertNotRegex',
    'assertRaisesRegexp': 'assertRaisesRegex',
}

FUNCTION_MAPPING = {
    'findTestCases': 'defaultTestLoader.loadTestsFromModule',
    'makeSuite': 'defaultTestLoader.loadTestsFromTestCase',
    'getTestCaseNames': 'defaultTestLoader.getTestCaseNames',
}


@register(ast.Call)
def visit_Call(
        state: State,
        node: ast.Call,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    if (
            isinstance(node.func, ast.Attribute) and
            isinstance(node.func.value, ast.Name) and
            node.func.value.id == 'self' and
            node.func.attr in METHOD_MAPPING
    ):
        func = functools.partial(
            replace_name,
            name=node.func.attr,
            new=f'self.{METHOD_MAPPING[node.func.attr]}',
        )
        yield ast_to_offset(node.func), func
    elif (
            isinstance(node.func, ast.Attribute) and
            isinstance(node.func.value, ast.Name) and
            node.func.value.id == 'unittest' and
            node.func.attr in FUNCTION_MAPPING and
            not has_starargs(node) and
            not node.keywords and
            len(node.args) == 1
    ):
        func = functools.partial(
            replace_name,
            name=node.func.attr,
            new=f'unittest.{FUNCTION_MAPPING[node.func.attr]}',
        )
        yield ast_to_offset(node.func), func
