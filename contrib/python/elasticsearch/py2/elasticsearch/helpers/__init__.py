#  Licensed to Elasticsearch B.V. under one or more contributor
#  license agreements. See the NOTICE file distributed with
#  this work for additional information regarding copyright
#  ownership. Elasticsearch B.V. licenses this file to you under
#  the Apache License, Version 2.0 (the "License"); you may
#  not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import sys

from .actions import (
    _chunk_actions,
    _process_bulk_chunk,
    bulk,
    expand_action,
    parallel_bulk,
    reindex,
    scan,
    streaming_bulk,
)
from .errors import BulkIndexError, ScanError

__all__ = [
    "BulkIndexError",
    "ScanError",
    "expand_action",
    "streaming_bulk",
    "bulk",
    "parallel_bulk",
    "scan",
    "reindex",
    "_chunk_actions",
    "_process_bulk_chunk",
]


try:
    # Asyncio only supported on Python 3.6+
    if sys.version_info < (3, 6):
        raise ImportError

    from .._async.helpers import (
        async_bulk,
        async_reindex,
        async_scan,
        async_streaming_bulk,
    )

    __all__ += ["async_scan", "async_bulk", "async_reindex", "async_streaming_bulk"]
except (ImportError, SyntaxError):
    pass
