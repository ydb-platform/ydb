# -*- coding: utf-8 -*-
"""Command line tools used for debugging and testing.

This file is part of PyVISA.

:copyright: 2019-2024 by PyVISA Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

import argparse
import logging


def get_shell_parser() -> argparse.ArgumentParser:
    """Create argument parser for pyvisa-shell program."""

    description = """
    PyVISA interactive console for debugging and testing with PyVISA-compatible
    devices.
    """

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument(
        "--backend",
        "-b",
        dest="backend",
        action="store",
        default=None,
        help="backend to be used (default: ivi)",
    )
    parser.add_argument(
        "-v",
        dest="verbosity",
        action="count",
        default=0,
        help="verbosity; use up to 2 times for increased output",
    )

    return parser


def _create_backend_str(args: argparse.Namespace) -> str:
    """Create the backend string from the CLI arguments."""
    if not args.backend:
        return ""

    # User already entered things correctly like "@py" or "file.yaml@sim"
    if "@" in args.backend:
        return args.backend

    # Keep the current functionality
    return "@" + args.backend


def visa_shell() -> None:
    """Run the VISA shell CLI program."""
    from pyvisa import shell

    parser = get_shell_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=(logging.WARN, logging.INFO, logging.DEBUG)[min(2, args.verbosity)]
    )

    backend = _create_backend_str(args)
    shell.main(backend)


def get_info_parser() -> argparse.ArgumentParser:
    """Create argument parser for pyvisa-info program."""
    parser = argparse.ArgumentParser(description="PyVISA diagnostic information tool.")

    return parser


def visa_info() -> None:
    """Summarize the infos about PyVISA and VISA."""
    from pyvisa import util

    parser = get_info_parser()
    parser.parse_args()

    util.get_debug_info()
