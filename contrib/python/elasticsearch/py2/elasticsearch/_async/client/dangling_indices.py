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

from .utils import SKIP_IN_PATH, NamespacedClient, _make_path, query_params


class DanglingIndicesClient(NamespacedClient):
    @query_params(
        "accept_data_loss",
        "master_timeout",
        "timeout",
        response_mimetypes=["application/json"],
    )
    async def delete_dangling_index(self, index_uuid, params=None, headers=None):
        """
        Deletes the specified dangling index

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/modules-gateway-dangling-indices.html>`_

        :arg index_uuid: The UUID of the dangling index
        :arg accept_data_loss: Must be set to true in order to delete
            the dangling index
        :arg master_timeout: Specify timeout for connection to master
        :arg timeout: Explicit operation timeout
        """
        if index_uuid in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index_uuid'.")

        return await self.transport.perform_request(
            "DELETE",
            _make_path("_dangling", index_uuid),
            params=params,
            headers=headers,
        )

    @query_params(
        "accept_data_loss",
        "master_timeout",
        "timeout",
        response_mimetypes=["application/json"],
    )
    async def import_dangling_index(self, index_uuid, params=None, headers=None):
        """
        Imports the specified dangling index

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/modules-gateway-dangling-indices.html>`_

        :arg index_uuid: The UUID of the dangling index
        :arg accept_data_loss: Must be set to true in order to import
            the dangling index
        :arg master_timeout: Specify timeout for connection to master
        :arg timeout: Explicit operation timeout
        """
        if index_uuid in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index_uuid'.")

        return await self.transport.perform_request(
            "POST", _make_path("_dangling", index_uuid), params=params, headers=headers
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    async def list_dangling_indices(self, params=None, headers=None):
        """
        Returns all dangling indices.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/modules-gateway-dangling-indices.html>`_
        """
        return await self.transport.perform_request(
            "GET", "/_dangling", params=params, headers=headers
        )
