from __future__ import annotations

import ast
import collections
import contextlib
import functools
from collections.abc import Generator
from collections.abc import Iterable
from typing import Any

from tokenize_rt import Offset
from tokenize_rt import Token
from tokenize_rt import tokens_to_src

from pyupgrade._ast_helpers import ast_to_offset
from pyupgrade._data import register
from pyupgrade._data import State
from pyupgrade._data import TokenFunc
from pyupgrade._token_helpers import Block
from pyupgrade._token_helpers import find_and_replace_call
from pyupgrade._token_helpers import find_block_start
from pyupgrade._token_helpers import find_name

FUNC_TYPES = (ast.Lambda, ast.FunctionDef, ast.AsyncFunctionDef)


def _fix_yield(i: int, tokens: list[Token]) -> None:
    in_token = find_name(tokens, i, 'in')
    colon = find_block_start(tokens, i)
    block = Block.find(tokens, i, trim_end=True)
    container = tokens_to_src(tokens[in_token + 1:colon]).strip()
    tokens[i:block.end] = [Token('CODE', f'yield from {container}\n')]


def _all_isinstance(
        vals: Iterable[Any],
        tp: type[Any] | tuple[type[Any], ...],
) -> bool:
    return all(isinstance(v, tp) for v in vals)


def _fields_same(n1: ast.AST, n2: ast.AST) -> bool:
    for (a1, v1), (a2, v2) in zip(ast.iter_fields(n1), ast.iter_fields(n2)):
        # ignore ast attributes, they'll be covered by walk
        if a1 != a2:
            return False
        elif _all_isinstance((v1, v2), ast.AST):
            continue
        elif _all_isinstance((v1, v2), (list, tuple)):
            if len(v1) != len(v2):
                return False
            # ignore sequences which are all-ast, they'll be covered by walk
            elif _all_isinstance(v1, ast.AST) and _all_isinstance(v2, ast.AST):
                continue
            elif v1 != v2:
                return False
        elif v1 != v2:
            return False
    return True


def _targets_same(target: ast.AST, yield_value: ast.AST) -> bool:
    for t1, t2 in zip(ast.walk(target), ast.walk(yield_value)):
        # ignore `ast.Load` / `ast.Store`
        if _all_isinstance((t1, t2), ast.expr_context):
            continue
        elif type(t1) is not type(t2):
            return False
        elif not _fields_same(t1, t2):
            return False
    else:
        return True


class Scope:
    def __init__(self, node: ast.AST) -> None:
        self.node = node

        self.reads: set[str] = set()
        self.writes: set[str] = set()

        self.yield_from_fors: set[Offset] = set()
        self.yield_from_names: dict[str, set[Offset]]
        self.yield_from_names = collections.defaultdict(set)


class Visitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self._scopes: list[Scope] = []
        self.super_offsets: set[Offset] = set()
        self.yield_offsets: set[Offset] = set()

    @contextlib.contextmanager
    def _scope(self, node: ast.AST) -> Generator[None]:
        self._scopes.append(Scope(node))
        try:
            yield
        finally:
            info = self._scopes.pop()
            # discard any that were referenced outside of the loop
            for name in info.reads:
                offsets = info.yield_from_names[name]
                info.yield_from_fors.difference_update(offsets)
            self.yield_offsets.update(info.yield_from_fors)
            if self._scopes:
                cell_reads = info.reads - info.writes
                self._scopes[-1].reads.update(cell_reads)

    def _visit_scope(self, node: ast.AST) -> None:
        with self._scope(node):
            self.generic_visit(node)

    visit_ClassDef = _visit_scope
    visit_Lambda = visit_FunctionDef = visit_AsyncFunctionDef = _visit_scope
    visit_ListComp = visit_SetComp = _visit_scope
    visit_DictComp = visit_GeneratorExp = _visit_scope

    def visit_Name(self, node: ast.Name) -> None:
        if self._scopes:
            if isinstance(node.ctx, ast.Load):
                self._scopes[-1].reads.add(node.id)
            elif isinstance(node.ctx, (ast.Store, ast.Del)):
                self._scopes[-1].writes.add(node.id)
            else:
                raise AssertionError(node)

        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        if (
                isinstance(node.func, ast.Name) and
                node.func.id == 'super' and
                len(node.args) == 2 and
                isinstance(node.args[1], ast.Name) and
                # there are at least two scopes
                len(self._scopes) >= 2 and
                # the last scope is a function where the first arg is arg2
                isinstance(self._scopes[-1].node, FUNC_TYPES) and
                self._scopes[-1].node.args.args and
                node.args[1].id == self._scopes[-1].node.args.args[0].arg
        ):
            args = node.args[0]
            scope = len(self._scopes) - 2
            current_scope = self._scopes[scope]
            # if in nested classes, all names in arg1 must match the scopes
            while (
                    isinstance(args, ast.Attribute) and
                    scope > 0 and
                    isinstance(current_scope.node, ast.ClassDef) and
                    args.attr == current_scope.node.name
            ):
                args = args.value
                scope -= 1
                current_scope = self._scopes[scope]
            # now check if it is outer most class and its name match
            if (
                    isinstance(args, ast.Name) and
                    isinstance(current_scope.node, ast.ClassDef) and
                    args.id == current_scope.node.name and
                    # an enclosing scope cannot be a class
                    (
                        scope == 0 or
                        not isinstance(
                            self._scopes[scope - 1].node,
                            ast.ClassDef,
                        )
                    )
            ):
                self.super_offsets.add(ast_to_offset(node))

        self.generic_visit(node)

    def visit_For(self, node: ast.For) -> None:
        if (
            len(self._scopes) >= 1 and
            not isinstance(self._scopes[-1].node, ast.AsyncFunctionDef) and
            len(node.body) == 1 and
            isinstance(node.body[0], ast.Expr) and
            isinstance(node.body[0].value, ast.Yield) and
            node.body[0].value.value is not None and
            _targets_same(node.target, node.body[0].value.value) and
            not node.orelse
        ):
            offset = ast_to_offset(node)
            func_info = self._scopes[-1]
            func_info.yield_from_fors.add(offset)
            for target_node in ast.walk(node.target):
                if (
                        isinstance(target_node, ast.Name) and
                        isinstance(target_node.ctx, ast.Store)
                ):
                    func_info.yield_from_names[target_node.id].add(offset)
            # manually visit, but with target+body as a separate scope
            self.visit(node.iter)
            with self._scope(node):
                self.visit(node.target)
                for stmt in node.body:
                    self.visit(stmt)
                assert not node.orelse
        else:
            self.generic_visit(node)


@register(ast.Module)
def visit_Module(
        state: State,
        node: ast.Module,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    visitor = Visitor()
    visitor.visit(node)

    super_func = functools.partial(find_and_replace_call, template='super()')
    for offset in visitor.super_offsets:
        yield offset, super_func

    for offset in visitor.yield_offsets:
        yield offset, _fix_yield
