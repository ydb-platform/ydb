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

from .utils import SKIP_IN_PATH, NamespacedClient, _bulk_body, _make_path, query_params


class FleetClient(NamespacedClient):
    @query_params(
        "checkpoints",
        "timeout",
        "wait_for_advance",
        "wait_for_index",
        request_mimetypes=["application/json"],
        response_mimetypes=["application/json"],
    )
    def global_checkpoints(self, index, params=None, headers=None):
        """
        Returns the current global checkpoints for an index. This API is design for
        internal use by the fleet server project.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/get-global-checkpoints.html>`_

        :arg index: The name of the index.
        :arg checkpoints: Comma separated list of checkpoints
        :arg timeout: Timeout to wait for global checkpoint to advance
            Default: 30s
        :arg wait_for_advance: Whether to wait for the global checkpoint
            to advance past the specified current checkpoints  Default: false
        :arg wait_for_index: Whether to wait for the target index to
            exist and all primary shards be active  Default: false
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        return self.transport.perform_request(
            "GET",
            _make_path(index, "_fleet", "global_checkpoints"),
            params=params,
            headers=headers,
        )

    @query_params(
        request_mimetypes=["application/x-ndjson"],
        response_mimetypes=["application/json"],
    )
    def msearch(self, body, index=None, params=None, headers=None):
        """
        Multi Search API where the search will only be executed after specified
        checkpoints are available due to a refresh. This API is designed for internal
        use by the fleet server project.

        .. warning::

            This API is **experimental** so may include breaking changes
            or be removed in a future version

        :arg body: The request definitions (metadata-fleet search
            request definition pairs), separated by newlines
        :arg index: The index name to use as the default
        """
        if body in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'body'.")

        body = _bulk_body(self.transport.serializer, body)
        return self.transport.perform_request(
            "POST",
            _make_path(index, "_fleet", "_fleet_msearch"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "allow_partial_search_results",
        "wait_for_checkpoints",
        "wait_for_checkpoints_timeout",
        request_mimetypes=["application/json"],
        response_mimetypes=["application/json"],
    )
    def search(self, index, body=None, params=None, headers=None):
        """
        Search API where the search will only be executed after specified checkpoints
        are available due to a refresh. This API is designed for internal use by the
        fleet server project.

        .. warning::

            This API is **experimental** so may include breaking changes
            or be removed in a future version

        :arg index: The index name to search.
        :arg body: The search definition using the Query DSL
        :arg allow_partial_search_results: Indicate if an error should
            be returned if there is a partial search failure or timeout  Default:
            True
        :arg wait_for_checkpoints: Comma separated list of checkpoints,
            one per shard
        :arg wait_for_checkpoints_timeout: Explicit wait_for_checkpoints
            timeout
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        return self.transport.perform_request(
            "POST",
            _make_path(index, "_fleet", "_fleet_search"),
            params=params,
            headers=headers,
            body=body,
        )
