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

from typing import Type

from elasticsearch import AsyncElasticsearch

from .connections import Connections


class AsyncElasticsearchConnections(Connections[AsyncElasticsearch]):
    def __init__(
        self, *, elasticsearch_class: Type[AsyncElasticsearch] = AsyncElasticsearch
    ):
        super().__init__(elasticsearch_class=elasticsearch_class)


connections = AsyncElasticsearchConnections(elasticsearch_class=AsyncElasticsearch)
configure = connections.configure
add_connection = connections.add_connection
remove_connection = connections.remove_connection
create_connection = connections.create_connection
get_connection = connections.get_connection
