#
# Copyright (C) 2021 - 2024 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
# pylint: disable=inherit-non-class,too-few-public-methods
"""anyconfig basic data types."""
from __future__ import annotations

import typing


InDataT = dict[str, typing.Any]

PrimitiveT = typing.Union[None, int, float, bool, str, InDataT]
InDataExT = typing.Union[PrimitiveT, InDataT]
