# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from typing import Final

EXCEPTION_ESCAPED: Final = "exception.escaped"
"""
Deprecated: It's no longer recommended to record exceptions that are handled and do not escape the scope of a span.
"""

EXCEPTION_MESSAGE: Final = "exception.message"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.exception_attributes.EXCEPTION_MESSAGE`.
"""

EXCEPTION_STACKTRACE: Final = "exception.stacktrace"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.exception_attributes.EXCEPTION_STACKTRACE`.
"""

EXCEPTION_TYPE: Final = "exception.type"
"""
Deprecated in favor of stable :py:const:`opentelemetry.semconv.attributes.exception_attributes.EXCEPTION_TYPE`.
"""
