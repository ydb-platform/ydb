from __future__ import annotations

import warnings
from typing import Literal

from opentelemetry.context import create_key

LOGFIRE_ATTRIBUTES_NAMESPACE = 'logfire'
"""Namespace within OTEL attributes used by logfire."""

LevelName = Literal['trace', 'debug', 'info', 'notice', 'warn', 'warning', 'error', 'fatal']
"""Level names for records."""

LEVEL_NUMBERS: dict[LevelName, int] = {
    'trace': 1,
    'debug': 5,
    'info': 9,
    'notice': 10,
    # warning is used by standard lib logging, has same meaning as "warn"
    'warning': 13,
    'warn': 13,
    'error': 17,
    'fatal': 21,
}

NUMBER_TO_LEVEL: dict[int, LevelName] = {v: k for k, v in LEVEL_NUMBERS.items()}

LOGGING_TO_OTEL_LEVEL_NUMBERS: dict[int, int] = {
    0: 9,  # logging.NOTSET: default to info
    1: 1,  # OTEL trace
    2: 1,
    3: 2,
    4: 2,
    5: 3,
    6: 3,
    7: 4,
    8: 4,
    9: 5,
    10: 5,  # debug
    11: 5,
    12: 5,
    13: 6,
    14: 6,
    15: 7,
    16: 7,
    17: 8,
    18: 8,
    19: 9,
    20: 9,  # info
    21: 9,
    22: 9,
    23: 10,  # notice
    24: 10,
    25: 11,  # 25 = success in loguru
    26: 11,
    27: 12,
    28: 12,
    29: 13,
    30: 13,  # warning
    31: 13,
    32: 13,
    33: 14,
    34: 14,
    35: 15,
    36: 15,
    37: 16,
    38: 16,
    39: 17,
    40: 17,  # error
    41: 17,
    42: 17,
    43: 18,
    44: 18,
    45: 19,
    46: 19,
    47: 20,
    48: 20,
    49: 21,
    50: 21,  # fatal/critical
}
"""Mapping from standard library logging level numbers to OTEL/logfire level numbers.
Based on feeling rather than hard maths."""

ATTRIBUTES_LOG_LEVEL_NAME_KEY = f'{LOGFIRE_ATTRIBUTES_NAMESPACE}.level_name'
"""Deprecated, use only ATTRIBUTES_LOG_LEVEL_NUM_KEY."""

ATTRIBUTES_LOG_LEVEL_NUM_KEY = f'{LOGFIRE_ATTRIBUTES_NAMESPACE}.level_num'
"""The key within OTEL attributes where logfire puts the log level number."""


# This is in this file to encourage using it instead of setting these attributes manually.
def log_level_attributes(level: LevelName | int) -> dict[str, int]:
    if isinstance(level, str):
        if level not in LEVEL_NUMBERS:
            warnings.warn(f'Invalid log level name: {level!r}')
            level = 'error'
        level = LEVEL_NUMBERS[level]

    return {
        ATTRIBUTES_LOG_LEVEL_NUM_KEY: level,
    }


SpanTypeType = Literal['log', 'pending_span', 'span']

ATTRIBUTES_SPAN_TYPE_KEY = f'{LOGFIRE_ATTRIBUTES_NAMESPACE}.span_type'
"""Used to differentiate logs, pending spans and regular spans. Absences should be interpreted as a real span."""

ATTRIBUTES_PENDING_SPAN_REAL_PARENT_KEY = f'{LOGFIRE_ATTRIBUTES_NAMESPACE}.pending_parent_id'
"""The real parent of a pending span, i.e. the parent of it's corresponding span and also it's grandparent"""

ATTRIBUTES_TAGS_KEY = f'{LOGFIRE_ATTRIBUTES_NAMESPACE}.tags'
"""The key within OTEL attributes where logfire puts tags."""

ATTRIBUTES_MESSAGE_TEMPLATE_KEY = f'{LOGFIRE_ATTRIBUTES_NAMESPACE}.msg_template'
"""The message template for a log."""

ATTRIBUTES_MESSAGE_KEY = f'{LOGFIRE_ATTRIBUTES_NAMESPACE}.msg'
"""The formatted message for a log."""

DISABLE_CONSOLE_KEY = f'{LOGFIRE_ATTRIBUTES_NAMESPACE}.disable_console_log'
"""special attribute to disable console logging, on a per span basis."""

ATTRIBUTES_JSON_SCHEMA_KEY = f'{LOGFIRE_ATTRIBUTES_NAMESPACE}.json_schema'
"""Key in OTEL attributes that collects the JSON schema."""

ATTRIBUTES_LOGGING_ARGS_KEY = f'{LOGFIRE_ATTRIBUTES_NAMESPACE}.logging_args'
"""Key in OTEL attributes that collects the arguments from standard library logging."""

ATTRIBUTES_LOGGING_NAME = f'{LOGFIRE_ATTRIBUTES_NAMESPACE}.logger_name'
"""Key in OTEL attributes that collects the standard library logger name."""

ATTRIBUTES_VALIDATION_ERROR_KEY = 'exception.logfire.data'
"""The key within OTEL attributes where logfire puts validation errors."""

ATTRIBUTES_SCRUBBED_KEY = f'{LOGFIRE_ATTRIBUTES_NAMESPACE}.scrubbed'
"""Key in OTEL attributes with metadata about parts of a span that have been scrubbed."""

RESOURCE_ATTRIBUTES_PACKAGE_VERSIONS = 'logfire.package_versions'
"""Versions of installed packages, serialized as list of json objects with keys 'name' and 'version'."""

RESOURCE_ATTRIBUTES_DEPLOYMENT_ENVIRONMENT_NAME = 'deployment.environment.name'
"""The name of the deployment environment e.g. production, staging, etc."""

RESOURCE_ATTRIBUTES_VCS_REPOSITORY_REF_REVISION = 'vcs.repository.ref.revision'
"""The revision of the VCS repository e.g. git commit hash, branch name, tag name, etc.

Check https://opentelemetry.io/docs/specs/semconv/attributes-registry/vcs/ for more information.
"""

RESOURCE_ATTRIBUTES_VCS_REPOSITORY_URL = 'vcs.repository.url.full'
"""The full URL of the VCS repository e.g. https://github.com/pydantic/logfire.

Check https://opentelemetry.io/docs/specs/semconv/attributes-registry/vcs/ for more information.
"""

RESOURCE_ATTRIBUTES_CODE_ROOT_PATH = 'logfire.code.root_path'
"""The root path of the current repository."""

RESOURCE_ATTRIBUTES_CODE_WORK_DIR = 'logfire.code.work_dir'
"""The working directory of the application."""

OTLP_MAX_INT_SIZE = 2**63 - 1
"""OTLP only supports signed 64-bit integers, larger integers get sent as strings."""

ATTRIBUTES_SAMPLE_RATE_KEY = 'logfire.sample_rate'
"""Key in attributes that indicates the sample rate for this span."""

CONTEXT_ATTRIBUTES_KEY = create_key('logfire.attributes')  # note this has a random suffix that OTEL adds
"""Key in the OTEL context that contains the logfire attributes."""

CONTEXT_SAMPLE_RATE_KEY = create_key('logfire.sample-rate')  # note this has a random suffix that OTEL adds
"""Key in the OTEL context that contains the current sample rate."""

MESSAGE_FORMATTED_VALUE_LENGTH_LIMIT = 128
"""Maximum number of characters for formatted values in a logfire message."""

ONE_SECOND_IN_NANOSECONDS = 1_000_000_000

ATTRIBUTES_EXCEPTION_FINGERPRINT_KEY = 'logfire.exception.fingerprint'
