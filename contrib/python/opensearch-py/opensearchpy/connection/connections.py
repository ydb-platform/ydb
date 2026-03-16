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

import opensearchpy
from opensearchpy.serializer import serializer


class Connections:
    """
    Class responsible for holding connections to different clusters. Used as a
    singleton in this module.
    """

    def __init__(self) -> None:
        self._kwargs: Any = {}
        self._conns: Any = {}

    def configure(self, **kwargs: Any) -> None:
        """
        Configure multiple connections at once, useful for passing in config
        dictionaries obtained from other sources, like Django's settings or a
        configuration management tool.

        Example::

            connections.configure(
                default={'hosts': 'localhost'},
                dev={'hosts': ['opensearchdev1.example.com:9200'], 'sniff_on_start': True},
            )

        Connections will only be constructed lazily when requested through
        ``get_connection``.
        """
        for k in list(self._conns):
            # try and preserve existing client to keep the persistent connections alive
            if k in self._kwargs and kwargs.get(k, None) == self._kwargs[k]:
                continue
            del self._conns[k]
        self._kwargs = kwargs

    def add_connection(self, alias: str, conn: Any) -> None:
        """
        Add a connection object, it will be passed through as-is.
        """
        self._conns[alias] = conn

    def remove_connection(self, alias: str) -> None:
        """
        Remove connection from the registry. Raises ``KeyError`` if connection
        wasn't found.
        """
        errors = 0
        for d in (self._conns, self._kwargs):
            try:
                del d[alias]
            except KeyError:
                errors += 1

        if errors == 2:
            raise KeyError(f"There is no connection with alias {alias!r}.")

    def create_connection(self, alias: str = "default", **kwargs: Any) -> Any:
        """
        Construct an instance of ``opensearchpy.OpenSearch`` and register
        it under given alias.
        """
        kwargs.setdefault("serializer", serializer)
        conn = self._conns[alias] = opensearchpy.OpenSearch(**kwargs)
        return conn

    def get_connection(self, alias: str = "default") -> Any:
        """
        Retrieve a connection, construct it if necessary (only configuration
        was passed to us). If a non-string alias has been passed through we
        assume it's already a client instance and will just return it as-is.

        Raises ``KeyError`` if no client (or its definition) is registered
        under the alias.
        """
        # do not check isinstance(OpenSearch) so that people can wrap their
        # clients
        if not isinstance(alias, str):
            return alias

        # connection already established
        try:
            return self._conns[alias]
        except KeyError:
            pass

        # if not, try to create it
        try:
            return self.create_connection(alias, **self._kwargs[alias])
        except KeyError:
            # no connection and no kwargs to set one up
            raise KeyError(f"There is no connection with alias {alias!r}.")


connections = Connections()
configure = connections.configure
add_connection = connections.add_connection
remove_connection = connections.remove_connection
create_connection = connections.create_connection
get_connection = connections.get_connection
