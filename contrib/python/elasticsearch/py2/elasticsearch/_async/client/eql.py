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


class EqlClient(NamespacedClient):
    @query_params(
        "keep_alive",
        "keep_on_completion",
        "wait_for_completion_timeout",
        request_mimetypes=["application/json"],
        response_mimetypes=["application/json"],
    )
    async def search(self, index, body, params=None, headers=None):
        """
        Returns results matching a query expressed in Event Query Language (EQL)

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/eql-search-api.html>`_

        :arg index: The name of the index to scope the operation
        :arg body: Eql request body. Use the `query` to limit the query
            scope.
        :arg keep_alive: Update the time interval in which the results
            (partial or final) for this search will be available  Default: 5d
        :arg keep_on_completion: Control whether the response should be
            stored in the cluster if it completed within the provided
            [wait_for_completion] time (default: false)
        :arg wait_for_completion_timeout: Specify the time that the
            request should block waiting for the final response
        """
        for param in (index, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return await self.transport.perform_request(
            "POST",
            _make_path(index, "_eql", "search"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    async def delete(self, id, params=None, headers=None):
        """
        Deletes an async EQL search by ID. If the search is still running, the search
        request will be cancelled. Otherwise, the saved search results are deleted.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/eql-search-api.html>`_

        :arg id: The async search ID
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'id'.")

        return await self.transport.perform_request(
            "DELETE", _make_path("_eql", "search", id), params=params, headers=headers
        )

    @query_params(
        "keep_alive",
        "wait_for_completion_timeout",
        response_mimetypes=["application/json"],
    )
    async def get(self, id, params=None, headers=None):
        """
        Returns async results from previously executed Event Query Language (EQL)
        search

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/eql-search-api.html>`_

        :arg id: The async search ID
        :arg keep_alive: Update the time interval in which the results
            (partial or final) for this search will be available  Default: 5d
        :arg wait_for_completion_timeout: Specify the time that the
            request should block waiting for the final response
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'id'.")

        return await self.transport.perform_request(
            "GET", _make_path("_eql", "search", id), params=params, headers=headers
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    async def get_status(self, id, params=None, headers=None):
        """
        Returns the status of a previously submitted async or stored Event Query
        Language (EQL) search

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/eql-search-api.html>`_

        :arg id: The async search ID
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'id'.")

        return await self.transport.perform_request(
            "GET",
            _make_path("_eql", "search", "status", id),
            params=params,
            headers=headers,
        )
