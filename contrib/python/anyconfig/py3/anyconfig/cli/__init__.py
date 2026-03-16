#
# Copyright (C) 2011 - 2021 Satoru SATOH <satoru.satoh@gmail.com>
# SPDX-License-Identifier: MIT
#
"""CLI frontend module for anyconfig."""
import sys

from ._main import main


__all__ = [
    "main",
]

if __name__ == "__main__":
    main(sys.argv)

# vim:sw=4:ts=4:et:
