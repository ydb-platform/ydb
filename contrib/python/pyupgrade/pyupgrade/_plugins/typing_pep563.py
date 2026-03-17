from __future__ import annotations

import ast
import functools
import sys
from collections.abc import Iterable
from collections.abc import Sequence

from tokenize_rt import NON_CODING_TOKENS
from tokenize_rt import Offset

from pyupgrade._ast_helpers import ast_to_offset
from pyupgrade._data import register
from pyupgrade._data import State
from pyupgrade._data import Token
from pyupgrade._data import TokenFunc


def _supported_version(state: State) -> bool:
    return (
        state.settings.min_version >= (3, 14) or
        'annotations' in state.from_imports['__future__']
    )


def _dequote(i: int, tokens: list[Token], *, new: str) -> None:
    end = i + 1
    for j in range(end, len(tokens)):
        if tokens[j].name == 'STRING':
            end = j + 1
        elif tokens[j].name not in NON_CODING_TOKENS:
            break
    else:
        raise AssertionError('past end?')
    tokens[i:end] = [tokens[i]._replace(src=new)]


def _get_name(node: ast.expr) -> str:
    if isinstance(node, ast.Name):
        return node.id
    elif isinstance(node, ast.Attribute):
        return node.attr
    else:
        raise AssertionError(f'expected Name or Attribute: {ast.dump(node)}')


def _get_keyword_value(
        keywords: list[ast.keyword],
        keyword: str,
) -> ast.expr | None:
    for kw in keywords:
        if kw.arg == keyword:
            return kw.value
    else:
        return None


def _process_call(node: ast.Call) -> Iterable[ast.AST]:
    name = _get_name(node.func)
    args = node.args
    keywords = node.keywords
    if name == 'TypedDict':
        if keywords:
            for keyword in keywords:
                yield keyword.value
        elif len(args) != 2:  # garbage
            pass
        elif isinstance(args[1], ast.Dict):
            yield from args[1].values
        else:
            raise AssertionError(f'expected ast.Dict: {ast.dump(args[1])}')
    elif name == 'NamedTuple':
        if len(args) == 2:
            fields: ast.expr | None = args[1]
        elif keywords:
            fields = _get_keyword_value(keywords, 'fields')
        else:  # garbage
            fields = None

        if isinstance(fields, ast.List):
            for elt in fields.elts:
                if isinstance(elt, ast.Tuple) and len(elt.elts) == 2:
                    yield elt.elts[1]
        elif fields is not None:
            raise AssertionError(f'expected ast.List: {ast.dump(fields)}')
    elif name in {
        'Arg',
        'DefaultArg',
        'NamedArg',
        'DefaultNamedArg',
        'VarArg',
        'KwArg',
    }:
        if args:
            yield args[0]
        else:
            keyword_value = _get_keyword_value(keywords, 'type')
            if keyword_value is not None:
                yield keyword_value


def _process_subscript(node: ast.Subscript) -> Iterable[ast.AST]:
    name = _get_name(node.value)
    if name == 'Annotated':
        if isinstance(node.slice, ast.Tuple) and node.slice.elts:
            yield node.slice.elts[0]
    elif name != 'Literal':
        yield node.slice


def _replace_string_literal(
        annotation: ast.expr,
) -> Iterable[tuple[Offset, TokenFunc]]:
    nodes: list[ast.AST] = [annotation]
    while nodes:
        node = nodes.pop()
        if isinstance(node, ast.Call):
            nodes.extend(_process_call(node))
        elif isinstance(node, ast.Subscript):
            nodes.extend(_process_subscript(node))
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            func = functools.partial(_dequote, new=node.value)
            yield ast_to_offset(node), func
        else:
            for name in node._fields:
                value = getattr(node, name)
                if isinstance(value, ast.AST):
                    nodes.append(value)
                elif isinstance(value, list):
                    nodes.extend(value)


def _process_args(
        args: Sequence[ast.arg | None],
) -> Iterable[tuple[Offset, TokenFunc]]:
    for arg in args:
        if arg is not None and arg.annotation is not None:
            yield from _replace_string_literal(arg.annotation)


def _visit_func(
        state: State,
        node: ast.AsyncFunctionDef | ast.FunctionDef,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    if not _supported_version(state):
        return

    yield from _process_args([node.args.vararg, node.args.kwarg])
    yield from _process_args(node.args.args)
    yield from _process_args(node.args.kwonlyargs)
    yield from _process_args(node.args.posonlyargs)
    if node.returns is not None:
        yield from _replace_string_literal(node.returns)


register(ast.AsyncFunctionDef)(_visit_func)
register(ast.FunctionDef)(_visit_func)


@register(ast.AnnAssign)
def visit_AnnAssign(
        state: State,
        node: ast.AnnAssign,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    if not _supported_version(state):
        return
    yield from _replace_string_literal(node.annotation)


if sys.version_info >= (3, 12):  # pragma: >=3.12 cover
    @register(ast.TypeVar)
    def visit_TypeVar(
            state: State,
            node: ast.TypeVar,
            parent: ast.AST,
    ) -> Iterable[tuple[Offset, TokenFunc]]:
        if node.bound is not None:
            yield from _replace_string_literal(node.bound)
