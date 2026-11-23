# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Final

from typing_extensions import deprecated

ERROR_MESSAGE: Final = "error.message"
"""
Deprecated: Use domain-specific error message attribute. For example, use `feature_flag.error.message` for feature flag errors.
"""

ERROR_TYPE: Final = "error.type"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.error_attributes.ERROR_TYPE`.
"""


@deprecated(
    "Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.error_attributes.ErrorTypeValues`."
)
class ErrorTypeValues(Enum):
    OTHER = "_OTHER"
    """Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.error_attributes.ErrorTypeValues.OTHER`."""
