#
# Copyright (C) 2018 Satoru SATOH <ssato @ redhat.com>
# License: MIT
#
r"""Globals, functions common in some JSON backends:

Changelog:

.. versionadded:: 0.9.8
"""
import anyconfig.backend.base
import anyconfig.compat


JSON_LOAD_OPTS = ["cls", "object_hook", "parse_float", "parse_int",
                  "parse_constant", "object_pairs_hook"]

JSON_DUMP_OPTS = ["skipkeys", "ensure_ascii", "check_circular", "allow_nan",
                  "cls", "indent", "separators", "default", "sort_keys"]

JSON_DICT_OPTS = ["object_pairs_hook", "object_hook"]

# It seems that 'encoding' argument is not allowed in json.load[s] and
# json.dump[s] in JSON module in python 3.x.
if not anyconfig.compat.IS_PYTHON_3:
    JSON_LOAD_OPTS.append("encoding")
    JSON_DUMP_OPTS.append("encoding")


class Parser(anyconfig.backend.base.StringStreamFnParser):
    """
    Parser for JSON files.
    """
    _cid = "std.json"
    _type = "json"
    _extensions = ["json", "jsn", "js"]
    _ordered = True
    _allow_primitives = True

    # .. note:: These may be overwritten.
    _load_opts = JSON_LOAD_OPTS
    _dump_opts = JSON_DUMP_OPTS
    _dict_opts = JSON_DICT_OPTS

# vim:sw=4:ts=4:et:
