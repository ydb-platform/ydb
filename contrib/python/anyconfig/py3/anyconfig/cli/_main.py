#
# Copyright (C) 2011 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""CLI frontend module for anyconfig."""
from __future__ import annotations

import os
import sys
import typing
import warnings

from .. import api, parser
from . import (
    actions, constants, detectors, filters, parse_args, utils,
)

if typing.TYPE_CHECKING:
    import argparse


def try_special_command_if_no_inputs(args: argparse.Namespace) -> None:
    """Run one of some special commands do not require inputs argument."""
    if not args.list and not args.env:
        utils.exit_with_output("No inputs were given!", 1)

    if not args.output:
        args.output = sys.stdout

    if args.list:
        actions.show_parsers_and_exit()

    if args.env:
        args.otype = detectors.try_detecting_output_type(args)
        actions.try_output_result(os.environ.copy(), args)
        sys.exit(0)


def process_args_or_run_command(
    args: argparse.Namespace,
) -> argparse.Namespace:
    """Process ``args`` and/or run commands.

    Process ``args``, that is, validate and update it, and raise SystemExit if
    something goes wrong at once.
    """
    # Validate args:
    if args.inputs:
        if (not args.itype and len(args.inputs) == 1
                and args.inputs[0] == constants.STD_IN_OR_OUT):
            utils.exit_with_output(
                "No input type was given but required for the input '-'",
                1,
            )
    else:
        try_special_command_if_no_inputs(args)

    if args.validate and not args.schema:
        utils.exit_with_output(
            "--validate and --schema options must be used together",
            1,
        )

    # Update args:
    if args.loglevel:
        warnings.simplefilter("always")

    args.otype = detectors.try_detecting_output_type(args)

    if constants.STD_IN_OR_OUT in args.inputs:
        args.inputs = [
            sys.stdin if inp == constants.STD_IN_OR_OUT else inp
            for inp in args.inputs
        ]

    if not args.output or args.output == constants.STD_IN_OR_OUT:
        args.output = sys.stdout

    return args


def exit_if_not_mergeable(diff: api.InDataExT) -> None:
    """Check if ``diff`` is a dict like object can be merged."""
    if not detectors.is_dict_like(diff):
        msg_code = (f"Cannot be merged: {diff!r}", 1)
        utils.exit_with_output(*msg_code)


def try_validate(cnf: api.InDataExT, args: argparse.Namespace) -> None:
    """Try validate ``cnf`` with the schema loaded from ``args.schema``."""
    scm = api.load(args.schema)
    (res, errors) = api.validate(cnf, scm, ac_schema_errors=True)

    if res:
        msg_code = ("Validation succeeded", 0)
    else:
        msg_code = (
            "Validation failed:"
            f"{(os.linesep + '  ').join(errors)}",
            1,
        )

    utils.exit_with_output(*msg_code)


def main(argv: list[str] | None = None) -> None:
    """Provide the entrypoint to run the CLI.

    :param argv: Argument list to parse or None (sys.argv will be set).
    """
    (_psr, args) = parse_args.parse((argv or sys.argv)[1:])
    args = process_args_or_run_command(args)

    cnf = os.environ.copy() if args.env else {}

    if args.extra_opts:
        args.extra_opts = parser.parse(args.extra_opts)

    diff = utils.load_diff(args, args.extra_opts or {})

    if cnf:
        exit_if_not_mergeable(diff)
        api.merge(cnf, diff)  # type: ignore[arg-type]
    else:
        cnf = diff  # type: ignore[assignment]

    if args.args:
        diff = parser.parse(args.args)  # type: ignore[assignment]
        exit_if_not_mergeable(diff)
        api.merge(cnf, diff)  # type: ignore[arg-type]

    cnf = (
        api.gen_schema(cnf) if args.gen_schema  # type: ignore[assignment]
        else filters.do_filter(cnf, args)
    )

    if args.validate:
        try_validate(cnf, args)

    actions.try_output_result(cnf, args)
