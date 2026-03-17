import functools
from collections import OrderedDict as odict
from collections import namedtuple

from valkit.common import valid_bool, valid_number, valid_string_list
from valkit.python import valid_object_path

from annet.annlib.rbparser import syntax
from annet.annlib.rbparser.deploying import compile_messages
from annet.vendors import registry_connector

from .common import import_rulebook_function


Answer = namedtuple("Answer", ("text", "send_nl"))
DEFAULT_TIMEOUT = 30
DEFAULT_SEND_NL = True
DEFAULT_APPLY_LOGIC = "common.apply"


# =====
@functools.lru_cache()
def compile_deploying_text(text, vendor):
    return _compile_deploying(
        tree=syntax.parse_text(
            text,
            params_scheme={
                "timeout": {
                    "validator": lambda arg: valid_number(arg, min=1, type=float),
                    "default": 30,
                },
                "send_nl": {
                    "validator": valid_bool,
                    "default": True,
                },
                "apply_logic": {
                    "validator": valid_object_path,
                    "default": DEFAULT_APPLY_LOGIC,
                },
                "ifcontext": {
                    "validator": valid_string_list,
                    "default": [],
                },
            },
        ),
        reverse_prefix=registry_connector.get()[vendor].reverse,
    )


# =====
def _compile_deploying(tree, reverse_prefix):
    deploying = odict()
    for rule_id, attrs in tree.items():
        if attrs["type"] == "normal" and not attrs["row"].startswith(("ignore:", "dialog:")):
            (ignore, dialogs) = compile_messages(attrs["children"])
            deploying[rule_id] = {
                "attrs": {
                    "regexp": syntax.compile_row_regexp(attrs["row"]),
                    "timeout": attrs["params"]["timeout"],
                    "apply_logic": import_rulebook_function(attrs["params"]["apply_logic"]),
                    "apply_logic_name": attrs["params"]["apply_logic"],
                    "ignore": ignore,
                    "dialogs": dialogs,
                    "ifcontext": attrs["params"]["ifcontext"],
                },
                "children": _compile_deploying(attrs["children"], reverse_prefix),
            }
    return deploying


def match_deploy_rule(rules, cmd_path, context):
    for depth, row in enumerate(cmd_path):
        for rule in rules.values():
            if rule["attrs"]["regexp"].match(row):
                ifcontext = rule["attrs"]["ifcontext"]
                if syntax.match_context(ifcontext, context):
                    if depth == len(cmd_path) - 1:
                        return rule
                    else:
                        rules = rule["children"]
                        if len(rules) == 0:
                            break
    # default match
    return {
        "attrs": {
            "regexp": syntax.compile_row_regexp("~"),
            "timeout": DEFAULT_TIMEOUT,
            "apply_logic": import_rulebook_function(DEFAULT_APPLY_LOGIC),
            "apply_logic_name": DEFAULT_APPLY_LOGIC,
            "ignore": [],
            "dialogs": odict(),
        },
        "children": odict(),
    }
