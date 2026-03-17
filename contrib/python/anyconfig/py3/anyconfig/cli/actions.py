#
# Copyright (C) 2011 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""Actions for anyconfig.cli.*."""
import argparse

from .. import api
from . import utils


def show_parsers_and_exit() -> None:
    """Show list of info of parsers available."""
    utils.exit_with_output(utils.make_parsers_txt())


def try_output_result(
    cnf: api.InDataExT, args: argparse.Namespace,
) -> None:
    """Try to output result."""
    api.dump(
        cnf, args.output, args.otype, **(args.extra_opts or {}),
    )
