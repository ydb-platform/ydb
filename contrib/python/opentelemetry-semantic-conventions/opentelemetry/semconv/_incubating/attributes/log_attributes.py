# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Final

LOG_FILE_NAME: Final = "log.file.name"
"""
The basename of the file.
"""

LOG_FILE_NAME_RESOLVED: Final = "log.file.name_resolved"
"""
The basename of the file, with symlinks resolved.
"""

LOG_FILE_PATH: Final = "log.file.path"
"""
The full path to the file.
"""

LOG_FILE_PATH_RESOLVED: Final = "log.file.path_resolved"
"""
The full path to the file, with symlinks resolved.
"""

LOG_IOSTREAM: Final = "log.iostream"
"""
The stream associated with the log. See below for a list of well-known values.
"""

LOG_RECORD_ORIGINAL: Final = "log.record.original"
"""
The complete original Log Record.
Note: This value MAY be added when processing a Log Record which was originally transmitted as a string or equivalent data type AND the Body field of the Log Record does not contain the same value. (e.g. a syslog or a log record read from a file.).
"""

LOG_RECORD_UID: Final = "log.record.uid"
"""
A unique identifier for the Log Record.
Note: If an id is provided, other log records with the same id will be considered duplicates and can be removed safely. This means, that two distinguishable log records MUST have different values.
The id MAY be an [Universally Unique Lexicographically Sortable Identifier (ULID)](https://github.com/ulid/spec), but other identifiers (e.g. UUID) may be used as needed.
"""


class LogIostreamValues(Enum):
    STDOUT = "stdout"
    """Logs from stdout stream."""
    STDERR = "stderr"
    """Events from stderr stream."""
