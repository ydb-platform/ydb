# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

import enum


class SeverityNumber(enum.Enum):
    """Numerical value of severity.

    Smaller numerical values correspond to less severe events
    (such as debug events), larger numerical values correspond
    to more severe events (such as errors and critical events).

    See the `Log Data Model`_ spec for more info and how to map the
    severity from source format to OTLP Model.

    .. _Log Data Model: https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/logs/data-model.md#field-severitynumber
    """

    UNSPECIFIED = 0
    TRACE = 1
    TRACE2 = 2
    TRACE3 = 3
    TRACE4 = 4
    DEBUG = 5
    DEBUG2 = 6
    DEBUG3 = 7
    DEBUG4 = 8
    INFO = 9
    INFO2 = 10
    INFO3 = 11
    INFO4 = 12
    WARN = 13
    WARN2 = 14
    WARN3 = 15
    WARN4 = 16
    ERROR = 17
    ERROR2 = 18
    ERROR3 = 19
    ERROR4 = 20
    FATAL = 21
    FATAL2 = 22
    FATAL3 = 23
    FATAL4 = 24
