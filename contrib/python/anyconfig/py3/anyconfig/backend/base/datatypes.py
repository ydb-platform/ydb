#
# Copyright (C) 2021 - 2025 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""Utility functions in anyconfig.backend.base."""
from __future__ import annotations

import collections.abc
import pathlib
import typing

from ...common import (
    InDataT, InDataExT,
)
from ...ioinfo import (
    IOInfo, PathOrIOInfoT,
)


OutDataExT = InDataExT

IoiT = IOInfo
MaybeFilePathT = typing.Optional[PathOrIOInfoT]

GenContainerT = collections.abc.Callable[..., InDataT]
OptionsT = dict[str, typing.Any]

PathOrStrT = typing.Union[str, pathlib.Path]
