# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
#
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


from typing import Any

from .base import Connection

try:
    import queue
except ImportError:
    import Queue as queue  # type: ignore


class PoolingConnection(Connection):
    _free_connections: queue.Queue[Connection]

    """
    Base connection class for connections that use libraries without thread
    safety and no capacity for connection pooling. To use this just implement a
    ``_make_connection`` method that constructs a new connection and returns
    it.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._free_connections = queue.Queue()
        super().__init__(*args, **kwargs)

    def _make_connection(self) -> Connection:
        raise NotImplementedError

    def _get_connection(self) -> Connection:
        try:
            return self._free_connections.get_nowait()
        except queue.Empty:
            return self._make_connection()

    def _release_connection(self, con: Connection) -> None:
        self._free_connections.put(con)

    def close(self) -> None:
        """
        Explicitly close connection
        """
        pass
