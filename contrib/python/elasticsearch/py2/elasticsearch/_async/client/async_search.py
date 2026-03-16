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


class AsyncSearchClient(NamespacedClient):
    @query_params(
        response_mimetypes=["application/json"],
    )
    async def delete(self, id, params=None, headers=None):
        """
        Deletes an async search by ID. If the search is still running, the search
        request will be cancelled. Otherwise, the saved search results are deleted.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/async-search.html>`_

        :arg id: The async search ID
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'id'.")

        return await self.transport.perform_request(
            "DELETE", _make_path("_async_search", id), params=params, headers=headers
        )

    @query_params(
        "keep_alive",
        "typed_keys",
        "wait_for_completion_timeout",
        response_mimetypes=["application/json"],
    )
    async def get(self, id, params=None, headers=None):
        """
        Retrieves the results of a previously submitted async search request given its
        ID.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/async-search.html>`_

        :arg id: The async search ID
        :arg keep_alive: Specify the time interval in which the results
            (partial or final) for this search will be available
        :arg typed_keys: Specify whether aggregation and suggester names
            should be prefixed by their respective types in the response
        :arg wait_for_completion_timeout: Specify the time that the
            request should block waiting for the final response
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'id'.")

        return await self.transport.perform_request(
            "GET", _make_path("_async_search", id), params=params, headers=headers
        )

    @query_params(
        "_source",
        "_source_excludes",
        "_source_includes",
        "allow_no_indices",
        "allow_partial_search_results",
        "analyze_wildcard",
        "analyzer",
        "batched_reduce_size",
        "default_operator",
        "df",
        "docvalue_fields",
        "expand_wildcards",
        "explain",
        "from_",
        "ignore_throttled",
        "ignore_unavailable",
        "keep_alive",
        "keep_on_completion",
        "lenient",
        "max_concurrent_shard_requests",
        "preference",
        "q",
        "request_cache",
        "routing",
        "search_type",
        "seq_no_primary_term",
        "size",
        "sort",
        "stats",
        "stored_fields",
        "suggest_field",
        "suggest_mode",
        "suggest_size",
        "suggest_text",
        "terminate_after",
        "timeout",
        "track_scores",
        "track_total_hits",
        "typed_keys",
        "version",
        "wait_for_completion_timeout",
        request_mimetypes=["application/json"],
        response_mimetypes=["application/json"],
    )
    async def submit(self, body=None, index=None, params=None, headers=None):
        """
        Executes a search request asynchronously.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/async-search.html>`_

        :arg body: The search definition using the Query DSL
        :arg index: A comma-separated list of index names to search; use
            `_all` or empty string to perform the operation on all indices
        :arg _source: True or false to return the _source field or not,
            or a list of fields to return
        :arg _source_excludes: A list of fields to exclude from the
            returned _source field
        :arg _source_includes: A list of fields to extract and return
            from the _source field
        :arg allow_no_indices: Whether to ignore if a wildcard indices
            expression resolves into no concrete indices. (This includes `_all`
            string or when no indices have been specified)
        :arg allow_partial_search_results: Indicate if an error should
            be returned if there is a partial search failure or timeout  Default:
            True
        :arg analyze_wildcard: Specify whether wildcard and prefix
            queries should be analyzed (default: false)
        :arg analyzer: The analyzer to use for the query string
        :arg batched_reduce_size: The number of shard results that
            should be reduced at once on the coordinating node. This value should be
            used as the granularity at which progress results will be made
            available.  Default: 5
        :arg default_operator: The default operator for query string
            query (AND or OR)  Valid choices: AND, OR  Default: OR
        :arg df: The field to use as default where no field prefix is
            given in the query string
        :arg docvalue_fields: A comma-separated list of fields to return
            as the docvalue representation of a field for each hit
        :arg expand_wildcards: Whether to expand wildcard expression to
            concrete indices that are open, closed or both.  Valid choices: open,
            closed, hidden, none, all  Default: open
        :arg explain: Specify whether to return detailed information
            about score computation as part of a hit
        :arg from_: Starting offset (default: 0)
        :arg ignore_throttled: Whether specified concrete, expanded or
            aliased indices should be ignored when throttled
        :arg ignore_unavailable: Whether specified concrete indices
            should be ignored when unavailable (missing or closed)
        :arg keep_alive: Update the time interval in which the results
            (partial or final) for this search will be available  Default: 5d
        :arg keep_on_completion: Control whether the response should be
            stored in the cluster if it completed within the provided
            [wait_for_completion] time (default: false)
        :arg lenient: Specify whether format-based query failures (such
            as providing text to a numeric field) should be ignored
        :arg max_concurrent_shard_requests: The number of concurrent
            shard requests per node this search executes concurrently. This value
            should be used to limit the impact of the search on the cluster in order
            to limit the number of concurrent shard requests  Default: 5
        :arg preference: Specify the node or shard the operation should
            be performed on (default: random)
        :arg q: Query in the Lucene query string syntax
        :arg request_cache: Specify if request cache should be used for
            this request or not, defaults to true
        :arg routing: A comma-separated list of specific routing values
        :arg search_type: Search operation type  Valid choices:
            query_then_fetch, dfs_query_then_fetch
        :arg seq_no_primary_term: Specify whether to return sequence
            number and primary term of the last modification of each hit
        :arg size: Number of hits to return (default: 10)
        :arg sort: A comma-separated list of <field>:<direction> pairs
        :arg stats: Specific 'tag' of the request for logging and
            statistical purposes
        :arg stored_fields: A comma-separated list of stored fields to
            return as part of a hit
        :arg suggest_field: Specify which field to use for suggestions
        :arg suggest_mode: Specify suggest mode  Valid choices: missing,
            popular, always  Default: missing
        :arg suggest_size: How many suggestions to return in response
        :arg suggest_text: The source text for which the suggestions
            should be returned
        :arg terminate_after: The maximum number of documents to collect
            for each shard, upon reaching which the query execution will terminate
            early.
        :arg timeout: Explicit operation timeout
        :arg track_scores: Whether to calculate and return scores even
            if they are not used for sorting
        :arg track_total_hits: Indicate if the number of documents that
            match the query should be tracked
        :arg typed_keys: Specify whether aggregation and suggester names
            should be prefixed by their respective types in the response
        :arg version: Specify whether to return document version as part
            of a hit
        :arg wait_for_completion_timeout: Specify the time that the
            request should block waiting for the final response  Default: 1s
        """
        if "from_" in params:
            params["from"] = params.pop("from_")

        return await self.transport.perform_request(
            "POST",
            _make_path(index, "_async_search"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    async def status(self, id, params=None, headers=None):
        """
        Retrieves the status of a previously submitted async search request given its
        ID.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/async-search.html>`_

        :arg id: The async search ID
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'id'.")

        return await self.transport.perform_request(
            "GET",
            _make_path("_async_search", "status", id),
            params=params,
            headers=headers,
        )
