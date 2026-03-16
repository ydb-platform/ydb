#
# Copyright (C) 2021 - 2024 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""Backend basic classes, functions and constants."""
from __future__ import annotations

import typing

from .compat import BinaryFilesMixin
from .datatypes import (
    GenContainerT, OptionsT,
    InDataT, InDataExT, OutDataExT, IoiT,
    PathOrStrT,
)
from .dumpers import (
    ToStringDumperMixin, ToStreamDumperMixin, BinaryDumperMixin,
)
from .loaders import (
    LoaderMixin, FromStringLoaderMixin, FromStreamLoaderMixin,
    BinaryLoaderMixin,
)
from .utils import (
    ensure_outdir_exists, to_method,
)
from .parsers import (
    Parser,
    StringParser, StreamParser, StringStreamFnParser,
)


ParserT = typing.TypeVar("ParserT", bound=Parser)
ParsersT = list[ParserT]
ParserClssT = list[type[ParserT]]


__all__ = [
    "BinaryFilesMixin",
    "GenContainerT", "OptionsT",
    "InDataT", "InDataExT", "OutDataExT", "IoiT",
    "PathOrStrT",
    "ToStringDumperMixin", "ToStreamDumperMixin", "BinaryDumperMixin",
    "LoaderMixin",
    "FromStringLoaderMixin", "FromStreamLoaderMixin", "BinaryLoaderMixin",
    "ensure_outdir_exists", "to_method",
    "Parser",
    "StringParser", "StreamParser", "StringStreamFnParser",
    "ParserT", "ParsersT", "ParserClssT",
]
