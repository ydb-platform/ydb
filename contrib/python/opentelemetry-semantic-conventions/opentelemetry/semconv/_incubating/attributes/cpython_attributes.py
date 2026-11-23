# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Final

CPYTHON_GC_GENERATION: Final = "cpython.gc.generation"
"""
Value of the garbage collector collection generation.
"""


class CPythonGCGenerationValues(Enum):
    GENERATION_0 = 0
    """Generation 0."""
    GENERATION_1 = 1
    """Generation 1."""
    GENERATION_2 = 2
    """Generation 2."""
