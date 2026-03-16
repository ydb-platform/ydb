from __future__ import annotations

import functools
import re
from collections import OrderedDict as odict
from collections.abc import Iterator
from typing import Any, TypedDict, cast

from annet.annlib import lib
from annet.vendors import tabparser


# =====
def _merge_trees(t1: odict[Any, Any], t2: odict[Any, Any]) -> odict[Any, Any]:
    if not t1:
        return t2
    if not t2:
        return t1
    ret = t1.copy()
    for k, v in t2.items():
        if k in ret:
            ret[k]["children"] = _merge_trees(ret[k]["children"], v["children"])
        else:
            ret[k] = v
    return ret


def _convert(tree: ParsedTree) -> odict[Any, Any]:
    ret: odict[Any, Any] = odict()
    for rule_id, attrs in tree:
        if rule_id not in ret:
            ret[rule_id] = attrs | {"children": odict()}
        ret[rule_id]["children"] = _merge_trees(ret[rule_id]["children"], _convert(attrs["children"]))
    return ret


def parse_text(text: str, params_scheme) -> odict[Any, Any]:
    ret = _parse_tree_with_params(tabparser.parse_to_tree_multi(text, _split_rows, ["#"]), params_scheme)
    return _convert(ret)


class _ParsedTreeNode(TypedDict):
    row: str
    type: str
    params: dict[str, Any]
    children: ParsedTree
    raw_rule: str
    context: Any


ParsedTree = list[tuple[str, _ParsedTreeNode]]


def parse_text_multi(text: str, params_scheme) -> ParsedTree:
    ret = _parse_tree_with_params(tabparser.parse_to_tree_multi(text, _split_rows, ["#"]), params_scheme)
    return ret


@functools.lru_cache()
def compile_row_regexp(row, flags=0):
    if "(?i)" in row:
        row = row.replace("(?i)", "")
        flags |= re.IGNORECASE

    if "*" in row:
        row = re.sub(r"\(([^\?])", r"(?:\1", row)  # Все дефолтные группы превратить в non-captured
        row = re.sub(r"\*/(\S+)/", r"(\1)", row)  # */{regex_one_word}/ -> ({regex_one_word})
        row = re.sub(r"(^|\s)\*", r"\1([^\\s]+)", row)

    # Заменяем <someting> на named-группы
    row = re.sub(r"<(\w+)>", r"(?P<\1>\\w+)", row)

    if row.endswith("~"):
        # We determine the most specific regex for the row at matching in match_row_to_acls
        row = row[:-1] + "(.+)"
    elif row.endswith("..."):
        row = row[:-3]
    elif "~/" in row:
        # ~/{regex}/ -> {regex}, () не нужны поскольку уже (?:) - non-captured
        row = re.sub(r"~/(((?!~/).)+)/", r"\1", row)
    else:
        row += r"(?:\s|$)"
    row = re.sub(r"\s+", r"\\s+", row)
    return re.compile("^" + row, flags=flags)


# =====
def _split_rows(text: str) -> Iterator[str]:
    for row in re.split(r"\n(?!\s*%(?!context))", text):
        yield row.replace("\n", " ")


def _parse_tree_with_params(raw_tree: tabparser.SimpleTree, scheme, context: dict | None = None) -> ParsedTree:
    tree: ParsedTree = []
    if context is None:
        context = {}
    for raw_rule, children in raw_tree:
        (row, params) = _parse_raw_rule(raw_rule, scheme)
        row_type = "normal"

        if row.startswith("!"):
            row = row[1:].strip()
            if len(row) == 0:
                continue
            row_type = "ignore"
        elif context_raw := params.get("context"):
            context = _parse_context(context, context_raw)
            continue
        tree.append(
            (
                raw_rule,
                {
                    "row": row,
                    "type": row_type,
                    "params": params,
                    "children": _parse_tree_with_params(children, scheme, cast(dict, context).copy()),
                    "raw_rule": raw_rule,
                    "context": cast(dict, context).copy(),
                },
            )
        )
    return tree


def _parse_raw_rule(raw_rule: str, scheme) -> tuple[str, dict[str, str]]:
    params: dict[str, str] = {}

    row, *params_raw = re.split(r"(?:^|\s)%(?=[a-zA-Z_]\w*)", raw_rule)
    for param in params_raw:
        name, _, value = param.partition("=")
        params[name.strip()] = value.strip() or "1"

    row = re.sub(r"\s+", " ", row.strip())
    params = _fill_and_validate(params, scheme, raw_rule)
    return row, params


def _fill_and_validate(params, scheme, raw_rule):
    return {
        key: (
            attrs["validator"](params[key])
            if key in params
            else (attrs["default"](raw_rule) if callable(attrs["default"]) else attrs["default"])
        )
        for (key, attrs) in scheme.items()
    }


def match_context(ifcontext, context):
    if not ifcontext:
        return True
    for ifcontext_value in ifcontext:
        name, value = ifcontext_value.split(":")
        if name in context:
            if context[name] == value:
                return True
    return False


def _parse_context(context, row):
    name, value = row.strip().split(":")
    return lib.merge_dicts(context, {name: value})
