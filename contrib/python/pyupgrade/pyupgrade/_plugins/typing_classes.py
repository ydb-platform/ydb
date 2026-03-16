from __future__ import annotations

import ast
import functools
from collections.abc import Iterable

from tokenize_rt import Offset
from tokenize_rt import Token
from tokenize_rt import UNIMPORTANT_WS

from pyupgrade._ast_helpers import ast_to_offset
from pyupgrade._ast_helpers import has_starargs
from pyupgrade._ast_helpers import is_name_attr
from pyupgrade._data import register
from pyupgrade._data import State
from pyupgrade._data import TokenFunc
from pyupgrade._token_helpers import KEYWORDS


def _unparse(node: ast.expr) -> str:
    if isinstance(node, ast.Name):
        return node.id
    elif isinstance(node, ast.Attribute):
        return ''.join((_unparse(node.value), '.', node.attr))
    elif isinstance(node, ast.Subscript):
        if isinstance(node.slice, ast.Tuple):
            if len(node.slice.elts) == 1:
                slice_s = f'{_unparse(node.slice.elts[0])},'
            else:
                slice_s = ', '.join(_unparse(elt) for elt in node.slice.elts)
        else:
            slice_s = _unparse(node.slice)
        return f'{_unparse(node.value)}[{slice_s}]'
    elif (
            isinstance(node, ast.Constant) and
            isinstance(node.value, (str, bytes))
    ):
        return repr(node.value)
    elif isinstance(node, ast.Constant) and node.value is Ellipsis:
        return '...'
    elif isinstance(node, ast.List):
        return '[{}]'.format(', '.join(_unparse(elt) for elt in node.elts))
    elif isinstance(node, ast.Constant) and node.value in {True, False, None}:
        return repr(node.value)
    elif isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
        return f'{_unparse(node.left)} | {_unparse(node.right)}'
    else:
        raise NotImplementedError(ast.dump(node))


def _typed_class_replacement(
        tokens: list[Token],
        i: int,
        call: ast.Call,
        types: dict[str, ast.expr],
) -> tuple[int, str]:
    while i > 0 and tokens[i - 1].name == 'DEDENT':
        i -= 1
    if i > 0 and tokens[i - 1].name in {'INDENT', UNIMPORTANT_WS}:
        indent = f'{tokens[i - 1].src}{" " * 4}'
    else:
        indent = ' ' * 4

    # NT = NamedTuple("nt", [("a", int)])
    # ^i                                 ^end
    end = i + 1
    comments = []
    while end < len(tokens) and tokens[end].name != 'NEWLINE':
        token = tokens[end]
        if token.name == 'COMMENT':
            comments.append(token)
        end += 1

    parts = []
    for k, v in types.items():
        while comments and (v.lineno, v.col_offset) > comments[0].offset:
            comment = comments.pop(0)
            parts.append(f'{indent}{comment.src}')

        member = f'{indent}{k}: {_unparse(v)}'

        if comments and v.lineno == comments[0].line:
            comment = comments.pop(0)
            member += f'  {comment.src}'

        parts.append(member)

    parts.extend(f'{indent}{x.src}' for x in comments)

    attrs = '\n'.join(parts)
    return end, attrs


def _fix_named_tuple(i: int, tokens: list[Token], *, call: ast.Call) -> None:
    types = {
        tup.elts[0].value: tup.elts[1]
        for tup in call.args[1].elts  # type: ignore  # (checked below)
    }
    end, attrs = _typed_class_replacement(tokens, i, call, types)
    src = f'class {tokens[i].src}({_unparse(call.func)}):\n{attrs}'
    tokens[i:end] = [Token('CODE', src)]


def _fix_kw_typed_dict(i: int, tokens: list[Token], *, call: ast.Call) -> None:
    types = {
        arg.arg: arg.value
        for arg in call.keywords
        if arg.arg is not None
    }
    end, attrs = _typed_class_replacement(tokens, i, call, types)
    src = f'class {tokens[i].src}({_unparse(call.func)}):\n{attrs}'
    tokens[i:end] = [Token('CODE', src)]


def _fix_dict_typed_dict(
        i: int,
        tokens: list[Token],
        *,
        call: ast.Call,
) -> None:
    types = {
        k.value: v
        for k, v in zip(
            call.args[1].keys,  # type: ignore  # (checked below)
            call.args[1].values,  # type: ignore  # (checked below)
        )
    }
    if call.keywords:
        total = call.keywords[0].value.value  # type: ignore # (checked below)  # noqa: E501
        end, attrs = _typed_class_replacement(tokens, i, call, types)
        src = (
            f'class {tokens[i].src}('
            f'{_unparse(call.func)}, total={total}'
            f'):\n'
            f'{attrs}'
        )
        tokens[i:end] = [Token('CODE', src)]
    else:
        end, attrs = _typed_class_replacement(tokens, i, call, types)
        src = f'class {tokens[i].src}({_unparse(call.func)}):\n{attrs}'
        tokens[i:end] = [Token('CODE', src)]


@register(ast.Assign)
def visit_Assign(
        state: State,
        node: ast.Assign,
        parent: ast.AST,
) -> Iterable[tuple[Offset, TokenFunc]]:
    if state.settings.min_version < (3, 6):
        return

    if (
            # NT = ...("NT", ...)
            len(node.targets) == 1 and
            isinstance(node.targets[0], ast.Name) and
            isinstance(node.value, ast.Call) and
            len(node.value.args) >= 1 and
            isinstance(node.value.args[0], ast.Constant) and
            isinstance(node.value.args[0].value, str) and
            node.targets[0].id == node.value.args[0].value and
            not has_starargs(node.value)
    ):
        if (
                is_name_attr(
                    node.value.func,
                    state.from_imports,
                    ('typing',),
                    ('NamedTuple',),
                ) and
                len(node.value.args) == 2 and
                not node.value.keywords and
                isinstance(node.value.args[1], (ast.List, ast.Tuple)) and
                len(node.value.args[1].elts) > 0 and
                all(
                    isinstance(tup, ast.Tuple) and
                    len(tup.elts) == 2 and
                    isinstance(tup.elts[0], ast.Constant) and
                    isinstance(tup.elts[0].value, str) and
                    tup.elts[0].value.isidentifier() and
                    tup.elts[0].value not in KEYWORDS
                    for tup in node.value.args[1].elts
                )
        ):
            func = functools.partial(_fix_named_tuple, call=node.value)
            yield ast_to_offset(node), func
        elif (
                is_name_attr(
                    node.value.func,
                    state.from_imports,
                    ('typing', 'typing_extensions'),
                    ('TypedDict',),
                ) and
                len(node.value.args) == 1 and
                len(node.value.keywords) > 0 and
                not any(
                    keyword.arg == 'total'
                    for keyword in node.value.keywords
                )
        ):
            func = functools.partial(_fix_kw_typed_dict, call=node.value)
            yield ast_to_offset(node), func
        elif (
                is_name_attr(
                    node.value.func,
                    state.from_imports,
                    ('typing', 'typing_extensions'),
                    ('TypedDict',),
                ) and
                len(node.value.args) == 2 and
                (
                    not node.value.keywords or
                    (
                        len(node.value.keywords) == 1 and
                        node.value.keywords[0].arg == 'total' and
                        isinstance(node.value.keywords[0].value, ast.Constant)
                    )
                ) and
                isinstance(node.value.args[1], ast.Dict) and
                node.value.args[1].keys and
                all(
                    isinstance(k, ast.Constant) and
                    isinstance(k.value, str) and
                    k.value.isidentifier() and
                    k.value not in KEYWORDS
                    for k in node.value.args[1].keys
                )
        ):
            func = functools.partial(_fix_dict_typed_dict, call=node.value)
            yield ast_to_offset(node), func
