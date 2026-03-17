#
# Copyright (C) 2011 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""Initialize sys.std{out,err}."""
from __future__ import annotations

import io
import sys
import typing

from .. import ioinfo


def make() -> tuple[typing.IO, typing.IO] | None:
    """Initialize sys.std{out,err} and returns them."""
    encoding = ioinfo.get_encoding()

    # TODO(ssato): #188 What should be done for an error, "AttributeError:
    # '_io.StringIO' object has no attribute 'buffer'"?
    try:
        return (
            io.TextIOWrapper(sys.stdout.buffer, encoding=encoding),
            io.TextIOWrapper(sys.stderr.buffer, encoding=encoding),
        )
    except AttributeError:
        pass

    return None
