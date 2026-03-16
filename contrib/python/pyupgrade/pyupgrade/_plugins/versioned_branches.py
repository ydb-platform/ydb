from __future__ import annotations

import ast
from collections.abc import Iterable
from typing import cast

from tokenize_rt import Offset
from tokenize_rt import Token

from pyupgrade._ast_helpers import ast_to_offset
from pyupgrade._ast_helpers import is_name_attr
from pyupgrade._data import register
from pyupgrade._data import State
from pyupgrade._data import TokenFunc
from pyupgrade._data import Version
from pyupgrade._token_helpers import Block


def _find_if_else_block(tokens: list[Token], i: int) -> tuple[Block, Block]:
    if_block = Block.find(tokens, i)
    i = if_block.end
    while tokens[i].src != 'else':
        i += 1
    else_block = Block.find(tokens, i, trim_end=True)
    return if_block, else_block


def _fix_py3_block(i: int, tokens: list[Token]) -> None:
    if tokens[i].src == 'if':
        if_block = Block.find(tokens, i)
        if_block.dedent(tokens)
        del tokens[if_block.start:if_block.block]
    else:
        if_block = Block.find(tokens, i)
        if_block.replace_condition(tokens, [Token('NAME', 'else')])


def _fix_py2_block(i: int, tokens: list[Token]) -> None:
    if tokens[i].src == 'if':
        if_block, else_block = _find_if_else_block(tokens, i)
        else_block.dedent(tokens)
        del tokens[if_block.start:else_block.block]
    else:
        if_block, else_block = _find_if_else_block(tokens, i)
        del tokens[if_block.start:else_block.start]


def _fix_remove_block(i: int, tokens: list[Token]) -> None:
    block = Block.find(tokens, i)
    del tokens[block.start:block.end]


def _fix_py2_convert_elif(i: int, tokens: list[Token]) -> None:
    if_block = Block.find(tokens, i)
    # wasn't actually followed by an `elif`
    if tokens[if_block.end].src != 'elif':
        return
    tokens[if_block.end] = Token('CODE', tokens[i].src)
    _fix_remove_block(i, tokens)


def _fix_py3_block_else(i: int, tokens: list[Token]) -> None:
    if tokens[i].src == 'if':
        if_block, else_block = _find_if_else_block(tokens, i)
        if_block.dedent(tokens)
        del tokens[if_block.end:else_block.end]
        del tokens[if_block.start:if_block.block]
    else:
        if_block, else_block = _find_if_else_block(tokens, i)
        del tokens[if_block.end:else_block.end]
        if_block.replace_condition(tokens, [Token('NAME', 'else')])


def _fix_py3_convert_elif(i: int, tokens: list[Token]) -> None:
    if_block = Block.find(tokens, i)
    # wasn't actually followed by an `elif`
    if tokens[if_block.end].src != 'elif':
        return
    tokens[if_block.end] = Token('CODE', tokens[i].src)
    if_block.dedent(tokens)
    del tokens[if_block.start:if_block.block]


def _eq(test: ast.Compare, n: int) -> bool:
    return _cmp(test, ast.Eq, n)


def _lt(test: ast.Compare, n: int) -> bool:
    return _cmp(test, ast.Lt, n)


def _gte(test: ast.Compare, n: int) -> bool:
    return _cmp(test, ast.GtE, n)


def _cmp(test: ast.Compare, op: type[ast.cmpop], n: int) -> bool:
    return (
        isinstance(test.ops[0], op) and
        isinstance(test.comparators[0], ast.Constant) and
        test.comparators[0].value == n
    )


def _compare_to_3(
    test: ast.Compare,
    op: type[ast.cmpop] | tuple[type[ast.cmpop], ...],
    minor: int = 0,
) -> bool:
    if not (
            isinstance(test.ops[0], op) and
            isinstance(test.comparators[0], ast.Tuple) and
            len(test.comparators[0].elts) >= 1 and
            all(
                isinstance(n, ast.Constant) and isinstance(n.value, int)
                for n in test.comparators[0].elts
            )
    ):
        return False

    # checked above but mypy needs help
    ast_elts = cast('list[ast.Constant]', test.comparators[0].elts)
    # padding a 0 for compatibility with (3,) used as a spec
    elts = tuple(e.value for e in ast_elts) + (0,)

    return elts[:2] == (3, minor) and all(n == 0 for n in elts[2:])


@register(ast.If)
def visit_If(
        state: State,
        node: ast.If,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:

    min_version: Version
    if state.settings.min_version == (3,):
        min_version = (3, 0)
    else:
        min_version = state.settings.min_version
    assert len(min_version) >= 2

    if (
            # if six.PY2:
            is_name_attr(
                node.test,
                state.from_imports,
                ('six',),
                ('PY2',),
            ) or
            # if not six.PY3:
            (
                isinstance(node.test, ast.UnaryOp) and
                isinstance(node.test.op, ast.Not) and
                is_name_attr(
                    node.test.operand,
                    state.from_imports,
                    ('six',),
                    ('PY3',),
                )
            ) or
            # sys.version_info == 2 or < (3,)
            # or < (3, n) or <= (3, n) (with n<m)
            (
                isinstance(node.test, ast.Compare) and
                is_name_attr(
                    node.test.left,
                    state.from_imports,
                    ('sys',),
                    ('version_info',),
                ) and
                len(node.test.ops) == 1 and (
                    _eq(node.test, 2) or
                    _compare_to_3(node.test, ast.Lt, min_version[1]) or
                    any(
                        _compare_to_3(node.test, (ast.Lt, ast.LtE), minor)
                        for minor in range(min_version[1])
                    )
                )
            ) or
            # sys.version_info[0] == 2 or < 3
            # sys.version_info.major == 2 or < 3
            (
                isinstance(node.test, ast.Compare) and
                (
                    (
                        isinstance(node.test.left, ast.Subscript) and
                        isinstance(node.test.left.slice, ast.Constant) and
                        node.test.left.slice.value == 0
                    ) or
                    (
                        isinstance(node.test.left, ast.Attribute) and
                        node.test.left.attr == 'major'
                    )
                ) and
                is_name_attr(
                    node.test.left.value,
                    state.from_imports,
                    ('sys',),
                    ('version_info',),
                ) and
                len(node.test.ops) == 1 and
                (
                    _eq(node.test, 2) or
                    _lt(node.test, 3)
                )
            )
    ):
        if len(node.orelse) == 1 and isinstance(node.orelse[0], ast.If):
            yield ast_to_offset(node), _fix_py2_convert_elif
        elif node.orelse:
            yield ast_to_offset(node), _fix_py2_block
        elif node.col_offset == 0:
            yield ast_to_offset(node), _fix_remove_block
    elif (
            # if six.PY3:
            is_name_attr(
                node.test,
                state.from_imports,
                ('six',),
                ('PY3',),
            ) or
            # if not six.PY2:
            (
                isinstance(node.test, ast.UnaryOp) and
                isinstance(node.test.op, ast.Not) and
                is_name_attr(
                    node.test.operand,
                    state.from_imports,
                    ('six',),
                    ('PY2',),
                )
            ) or
            # sys.version_info == 3 or >= (3,) or > (3,)
            # sys.version_info >= (3, n) (with n<=m)
            # or sys.version_info > (3, n) (with n<m)
            (
                isinstance(node.test, ast.Compare) and
                is_name_attr(
                    node.test.left,
                    state.from_imports,
                    ('sys',),
                    ('version_info',),
                ) and
                len(node.test.ops) == 1 and (
                    _eq(node.test, 3) or
                    _compare_to_3(node.test, (ast.Gt, ast.GtE)) or
                    _compare_to_3(node.test, ast.GtE, min_version[1]) or
                    any(
                        _compare_to_3(node.test, (ast.Gt, ast.GtE), minor)
                        for minor in range(min_version[1])
                    )
                )
            ) or
            # sys.version_info[0] == 3 or >= 3
            # sys.version_info.major == 3 or >= 3
            (
                isinstance(node.test, ast.Compare) and
                (
                    (
                        isinstance(node.test.left, ast.Subscript) and
                        isinstance(node.test.left.slice, ast.Constant) and
                        node.test.left.slice.value == 0
                    ) or
                    (
                        isinstance(node.test.left, ast.Attribute) and
                        node.test.left.attr == 'major'
                    )
                ) and
                is_name_attr(
                    node.test.left.value,
                    state.from_imports,
                    ('sys',),
                    ('version_info',),
                ) and
                len(node.test.ops) == 1 and
                (
                    _eq(node.test, 3) or
                    _gte(node.test, 3)
                )
            )
    ):
        if len(node.orelse) == 1 and isinstance(node.orelse[0], ast.If):
            yield ast_to_offset(node), _fix_py3_convert_elif
        elif node.orelse:
            yield ast_to_offset(node), _fix_py3_block_else
        else:
            yield ast_to_offset(node), _fix_py3_block
