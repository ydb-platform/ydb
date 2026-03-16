#
# Copyright (C) 2011 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
# Suppress import positions after some global variables are defined
# pylint: disable=wrong-import-position
"""Provide config parser objects aggregated."""
from __future__ import annotations

from ..backend import ParserClssT, PARSERS
from ..processors import Processors
from ..singleton import Singleton


class Parsers(Processors, Singleton):
    """Manager class for parsers."""

    _pgroup: str = "anyconfig_backends"

    def __init__(self, prcs: ParserClssT | None = None) -> None:
        """Initialize with PARSERS."""
        if prcs is None:
            prcs = PARSERS

        super().__init__(prcs)
