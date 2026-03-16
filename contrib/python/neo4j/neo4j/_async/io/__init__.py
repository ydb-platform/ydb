# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
Low-level functionality required for speaking Bolt.

It is not intended to be used directly by driver users. Instead, the
``session`` module provides the main user-facing abstractions.
"""

__all__ = [
    "AcquisitionAuth",
    "AcquisitionDatabase",
    "AsyncBolt",
    "AsyncBoltPool",
    "AsyncNeo4jPool",
    "ConnectionErrorHandler",
    "acquisition_timeout_to_deadline",
]


# [bolt-version-bump] search tag when changing bolt version support
from . import (  # noqa - imports needed to register protocol handlers
    _bolt3,
    _bolt4,
    _bolt5,
    _bolt6,
)
from ._bolt import AsyncBolt
from ._common import ConnectionErrorHandler
from ._pool import (
    acquisition_timeout_to_deadline,
    AcquisitionAuth,
    AcquisitionDatabase,
    AsyncBoltPool,
    AsyncNeo4jPool,
)
