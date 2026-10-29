# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Final

OPENTRACING_REF_TYPE: Final = "opentracing.ref_type"
"""
Parent-child Reference type.
Note: The causal relationship between a child Span and a parent Span.
"""


class OpentracingRefTypeValues(Enum):
    CHILD_OF = "child_of"
    """The parent Span depends on the child Span in some capacity."""
    FOLLOWS_FROM = "follows_from"
    """The parent Span doesn't depend in any way on the result of the child Span."""
