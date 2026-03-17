#
# Copyright (C) 2011 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
# pylint: disable=broad-except
"""Filter functions for anyconfig.cli.*."""
from __future__ import annotations

import typing

from .. import api, parser
from . import utils

if typing.TYPE_CHECKING:
    import argparse
    from ..common import InDataExT


def do_filter(
    cnf: dict[str, typing.Any], args: argparse.Namespace,
) -> InDataExT:
    """Filter ``cnf`` by query/get/set and return filtered result."""
    if args.query:
        try:
            return api.try_query(cnf, args.query)
        except Exception as exc:  # noqa: BLE001
            utils.exit_with_output(f"Failed to query: exc={exc!s}", 1)

    if args.get:
        (cnf, err) = api.get(cnf, args.get)
        if cnf is None:
            utils.exit_with_output(f"Failed to get result: err={err!s}", 1)

        return cnf

    if args.set:
        (key, val) = args.set.split("=")
        api.set_(cnf, key, parser.parse(val))

    return cnf
