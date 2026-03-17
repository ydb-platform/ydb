#
# Copyright (C) 2011 - 2025 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""Provides constants for anyconfig.cli."""
STD_IN_OR_OUT = "-"

USAGE = """\
%(prog)s [Options...] CONF_PATH_OR_PATTERN_0 [CONF_PATH_OR_PATTERN_1 ..]

Examples:
  %(prog)s --list  # -> Supported config types: configobj, ini, json, ...
  # Merge and/or convert input config to output config [file]
  %(prog)s -I yaml -O yaml /etc/xyz/conf.d/a.conf
  %(prog)s -I yaml '/etc/xyz/conf.d/*.conf' -o xyz.conf --otype json
  %(prog)s '/etc/xyz/conf.d/*.json' -o xyz.yml \\
    --atype json -A '{"obsoletes": "syscnf", "conflicts": "syscnf-old"}'
  %(prog)s '/etc/xyz/conf.d/*.json' -o xyz.yml \\
    -A obsoletes:syscnf;conflicts:syscnf-old
  %(prog)s /etc/foo.json /etc/foo/conf.d/x.json /etc/foo/conf.d/y.json
  %(prog)s '/etc/foo.d/*.json' -M noreplace
  # Query/Get/set part of input config
  %(prog)s '/etc/foo.d/*.json' --query 'locs[?state == 'T'].name | sort(@)'
  %(prog)s '/etc/foo.d/*.json' --get a.b.c
  %(prog)s '/etc/foo.d/*.json' --set a.b.c=1
  # Validate with JSON schema or generate JSON schema:
  %(prog)s --validate -S foo.conf.schema.yml '/etc/foo.d/*.xml'
  %(prog)s --gen-schema '/etc/foo.d/*.xml' -o foo.conf.schema.yml"""

ATYPE_HELP_FMT = """\
Explicitly select type of argument to provide configs from %s.

If this option is not set, original parser is used: 'K:V' will become {K: V},
'K:V_0,V_1,..' will become {K: [V_0, V_1, ...]}, and 'K_0:V_0;K_1:V_1' will
become {K_0: V_0, K_1: V_1} (where the tyep of K is str, type of V is one of
Int, str, etc."""

QUERY_HELP = ("Query with JMESPath expression language. See "
              "http://jmespath.org for more about JMESPath expression. "
              "This option is not used with --get option at the same time. "
              "Please note that python module to support JMESPath "
              "expression (https://pypi.python.org/pypi/jmespath/) is "
              "required to use this option")
GET_HELP = ("Specify key path to get part of config, for example, "
            "'--get a.b.c' to config {'a': {'b': {'c': 0, 'd': 1}}} "
            "gives 0 and '--get a.b' to the same config gives "
            "{'c': 0, 'd': 1}. Path expression can be JSON Pointer "
            "expression (http://tools.ietf.org/html/rfc6901) such like "
            "'', '/a~1b', '/m~0n'. "
            "This option is not used with --query option at the same time. ")
SET_HELP = ("Specify key path to set (update) part of config, for "
            "example, '--set a.b.c=1' to a config {'a': {'b': {'c': 0, "
            "'d': 1}}} gives {'a': {'b': {'c': 1, 'd': 1}}}.")
