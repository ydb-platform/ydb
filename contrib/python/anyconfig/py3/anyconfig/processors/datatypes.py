#
# Copyright (C) 2018 - 2024 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""Common functions and variables."""
from __future__ import annotations

import typing

from ..models import processor


ProcT = typing.TypeVar("ProcT", bound=processor.Processor)
ProcsT = list[ProcT]
ProcClsT = type[ProcT]
ProcClssT = list[ProcClsT]

MaybeProcT = typing.Optional[typing.Union[str, ProcT, ProcClsT]]
