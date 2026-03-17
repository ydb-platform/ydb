import functools
import re
from collections import OrderedDict as odict

from valkit.common import valid_bool, valid_string_list
from valkit.python import valid_object_path

from annet.annlib.rbparser import platform, syntax
from annet.vendors import registry_connector

from .common import import_rulebook_function


DEFAULT_PATCH_LOGIC = "common.default"
ORDERED_PATCH_LOGIC = "common.ordered"
REWRITE_PATCH_LOGIC = "common.rewrite"
REWRITE_DIFF_LOGIC = "common.rewrite_diff"
MULTILINE_DIFF_LOGIC = "common.multiline_diff"


# =====
@functools.lru_cache()
def compile_patching_text(text, vendor):
    return _compile_patching(
        tree=syntax.parse_text(
            text,
            params_scheme={
                "global": {
                    "validator": valid_bool,
                    "default": False,
                },
                "logic": {
                    "validator": valid_object_path,
                    "default": DEFAULT_PATCH_LOGIC,
                },
                "diff_logic": {
                    "validator": valid_object_path,
                    "default": registry_connector.get()[vendor].diff(False),
                },
                "comment": {
                    "validator": valid_string_list,
                    "default": [],
                },
                "multiline": {
                    "validator": valid_bool,
                    "default": False,
                },
                "ordered": {
                    "validator": valid_bool,
                    "default": False,
                },
                "context": {
                    "validator": str,
                    "default": None,
                },
                "rewrite": {
                    "validator": valid_bool,
                    "default": False,
                },
                "parent": {
                    "validator": valid_bool,
                    "default": False,
                },
                "force_commit": {
                    "validator": valid_bool,
                    "default": False,
                },
                "ignore_case": {
                    "validator": valid_bool,
                    "default": False,
                },
            },
        ),
        reverse_prefix=registry_connector.get()[vendor].reverse,
        vendor=vendor,
    )


# =====
def _compile_patching(tree, reverse_prefix, vendor):
    rules = {"local": odict(), "global": odict()}
    for raw_rule, attrs in tree.items():
        regexp = _attrs_to_regexp(attrs)
        attrs = _regexp_to_attrs(regexp, attrs)
        if attrs["type"] == "ignore":
            rule = {
                "type": attrs["type"],
                "attrs": {
                    "regexp": regexp,
                    "diff_logic": import_rulebook_function(attrs["params"]["diff_logic"]),
                    "parent": bool(attrs["children"]),
                    "context": attrs["context"],
                },
                "children": {"global": {}, "local": {}},
            }
        else:
            if attrs["params"]["ordered"]:
                attrs["params"]["diff_logic"] = registry_connector.get()[vendor].diff(True)
                attrs["params"]["logic"] = ORDERED_PATCH_LOGIC
            elif attrs["params"]["rewrite"]:
                attrs["params"]["diff_logic"] = REWRITE_DIFF_LOGIC
                attrs["params"]["logic"] = REWRITE_PATCH_LOGIC
            elif attrs["params"]["multiline"]:
                attrs["params"]["diff_logic"] = MULTILINE_DIFF_LOGIC
            rule = {
                "type": attrs["type"],
                "attrs": {
                    "logic": import_rulebook_function(attrs["params"]["logic"]),
                    "diff_logic": import_rulebook_function(attrs["params"]["diff_logic"]),
                    "regexp": regexp,
                    "reverse": _make_reverse(attrs["row"], reverse_prefix, flags=regexp.flags),
                    "comment": attrs["params"]["comment"],
                    "multiline": attrs["params"]["multiline"],
                    "parent": attrs["params"]["parent"] or bool(attrs["children"]),
                    "force_commit": attrs["params"]["force_commit"],
                    "ignore_case": attrs["params"]["ignore_case"],
                    "ordered": attrs["params"]["ordered"],
                    "context": attrs["context"],
                },
                "children": None,
            }
            if not attrs["params"]["global"]:
                rule["children"] = _compile_patching(attrs["children"], reverse_prefix, vendor)
        rules["global" if attrs["params"]["global"] else "local"][raw_rule] = rule
    return rules


@functools.lru_cache()
def _make_reverse(row, reverse_prefix, flags=0):
    if row.startswith(reverse_prefix + " "):
        row = row[len(reverse_prefix + " ") :]
    else:
        row = "%s %s" % (reverse_prefix, row)

    if row[-1] == "~":
        row = row[:-1] + "{}"

    row = re.sub(r"\*(/\S+/)?", "{}", row, flags=flags)
    row = re.sub(r"\s*~(/\S+/)?", "", row, flags=flags)
    return row


def _attrs_to_regexp(attrs):
    flags = 0
    ignore_case = attrs["params"]["ignore_case"]
    if ignore_case:
        flags |= re.IGNORECASE
    return syntax.compile_row_regexp(attrs["row"], flags=flags)


def _regexp_to_attrs(regexp, attrs):
    attrs["params"]["ignore_case"] = bool(regexp.flags & re.IGNORECASE)
    return attrs
