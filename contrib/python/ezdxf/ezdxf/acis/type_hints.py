# Copyright (c) 2024, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Sequence, Union
from typing_extensions import TypeAlias

EncodedData: TypeAlias = Union[str, Sequence[str], bytes, bytearray]
