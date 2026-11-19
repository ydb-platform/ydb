# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from logging import getLogger
from os import environ

_logger = getLogger(__name__)


def parse_boolean_environment_variable(
    environment_variable: str, default: bool = False
) -> bool:
    value = environ.get(environment_variable)
    if value is None:
        return default

    match value.strip().lower():
        case "true":
            return True
        case "false":
            return False
        case _:
            _logger.warning(
                "Invalid value for %s: %r. Expected 'true' or 'false'.",
                environment_variable,
                value,
            )
            return default
