# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""
The OpenTelemetry logging API describes the classes used to generate logs and events.

The :class:`.LoggerProvider` provides users access to the :class:`.Logger`.

This module provides abstract (i.e. unimplemented) classes required for
logging, and a concrete no-op implementation :class:`.NoOpLogger` that allows applications
to use the API package alone without a supporting implementation.

To get a logger, you need to provide the package name from which you are
calling the logging APIs to OpenTelemetry by calling `LoggerProvider.get_logger`
with the calling module name and the version of your package.

The following code shows how to obtain a logger using the global :class:`.LoggerProvider`::

    from opentelemetry._logs import get_logger

    logger = get_logger("example-logger")

.. versionadded:: 1.15.0
"""

from opentelemetry._logs._internal import (
    Logger,
    LoggerProvider,
    LogRecord,
    NoOpLogger,
    NoOpLoggerProvider,
    get_logger,
    get_logger_provider,
    set_logger_provider,
)
from opentelemetry._logs.severity import SeverityNumber

__all__ = [
    "Logger",
    "LoggerProvider",
    "LogRecord",
    "NoOpLogger",
    "NoOpLoggerProvider",
    "get_logger",
    "get_logger_provider",
    "set_logger_provider",
    "SeverityNumber",
]
