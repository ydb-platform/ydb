#
# Copyright (C) 2011 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""Argument parser."""
from __future__ import annotations

import argparse

from .. import api
from . import constants, utils


DEFAULTS = {
    "loglevel": 0, "list": False, "output": None, "itype": None, "otype": None,
    "atype": None, "merge": api.MS_DICTS, "ignore_missing": False,
    "template": False, "env": False, "schema": None, "validate": False,
    "gen_schema": False, "extra_opts": None,
}


def gen_type_help_txt(types: str, target: str = "Input") -> str:
    """Generate a type help txt."""
    return (
        f"Select type of {target} files from {types}"  # noqa: S608
        "[Automatically detected by those extension]"
    )


def make_parser(
    defaults: dict | None = None,
    prog: str | None = None,
) -> argparse.ArgumentParser:
    """Make an instance of argparse.ArgumentParser to parse arguments."""
    if defaults is None:
        defaults = DEFAULTS

    ctypes: list[str] = utils.list_parser_types()
    ctypes_s: str = ", ".join(ctypes)

    apsr = argparse.ArgumentParser(prog=prog, usage=constants.USAGE)
    apsr.set_defaults(**defaults)

    apsr.add_argument("inputs", type=str, nargs="*", help="Input files")
    apsr.add_argument(
        "--version", action="version",
        version=f"%%(prog)s {'.'.join(api.version())}",
    )

    apsr.add_argument("-o", "--output", help="Output file path")
    apsr.add_argument("-I", "--itype", choices=ctypes, metavar="ITYPE",
                      help=gen_type_help_txt(ctypes_s))
    apsr.add_argument("-O", "--otype", choices=ctypes, metavar="OTYPE",
                      help=gen_type_help_txt(ctypes_s, "Output"))

    mss = api.MERGE_STRATEGIES
    mss_s = ", ".join(mss)
    mt_help = (
        "Select strategy to merge multiple configs from "  # noqa: S608
        f"{mss_s} {defaults['merge']}"
    )
    apsr.add_argument("-M", "--merge", choices=mss, metavar="MERGE",
                      help=mt_help)

    apsr.add_argument("-A", "--args", help="Argument configs to override")
    apsr.add_argument("--atype", choices=ctypes, metavar="ATYPE",
                      help=constants.ATYPE_HELP_FMT % ctypes_s)

    lpog = apsr.add_argument_group("List specific options")
    lpog.add_argument("-L", "--list", action="store_true",
                      help="List supported config types")

    spog = apsr.add_argument_group("Schema specific options")
    spog.add_argument("--validate", action="store_true",
                      help="Only validate input files and do not output. "
                           "You must specify schema file with -S/--schema "
                           "option.")
    spog.add_argument("--gen-schema", action="store_true",
                      help="Generate JSON schema for givne config file[s] "
                           "and output it instead of (merged) configuration.")

    gspog = apsr.add_argument_group("Query/Get/set options")
    gspog.add_argument("-Q", "--query", help=constants.QUERY_HELP)
    gspog.add_argument("--get", help=constants.GET_HELP)
    gspog.add_argument("--set", help=constants.SET_HELP)

    cpog = apsr.add_argument_group("Common options")
    cpog.add_argument("-x", "--ignore-missing", action="store_true",
                      help="Ignore missing input files")
    cpog.add_argument("-T", "--template", action="store_true",
                      help="Enable template config support")
    cpog.add_argument("-E", "--env", action="store_true",
                      help="Load configuration defaults from "
                           "environment values")
    cpog.add_argument("-S", "--schema", help="Specify Schema file[s] path")
    cpog.add_argument("-e", "--extra-opts",
                      help="Extra options given to the API call, "
                           "--extra-options indent:2 (specify the "
                           "indent for pretty-printing of JSON outputs) "
                           "for example")
    cpog.add_argument("-v", "--verbose", action="count", dest="loglevel",
                      help="Verbose mode; -v or -vv (more verbose)")
    return apsr


def parse(
    argv: list[str],
    prog: str | None = None,
) -> tuple[argparse.ArgumentParser, argparse.Namespace]:
    """Parse given arguments ``argv`` and return it with the parser."""
    psr = make_parser(prog=prog)
    return (psr, psr.parse_args(argv))
