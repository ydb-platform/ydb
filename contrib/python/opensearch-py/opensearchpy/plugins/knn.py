# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

# ------------------------------------------------------------------------------------------
# THIS CODE IS AUTOMATICALLY GENERATED AND MANUAL EDITS WILL BE LOST
#
# To contribute, kindly make modifications in the opensearch-py client generator
# or in the OpenSearch API specification, and run `nox -rs generate`. See DEVELOPER_GUIDE.md
# and https://github.com/opensearch-project/opensearch-api-specification for details.
# -----------------------------------------------------------------------------------------+


from typing import Any

from ..client.utils import SKIP_IN_PATH, NamespacedClient, _make_path, query_params


class KnnClient(NamespacedClient):
    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def delete_model(
        self,
        *,
        model_id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Used to delete a particular model in the cluster.


        :arg model_id: The id of the model.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if model_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'model_id'.")

        return self.transport.perform_request(
            "DELETE",
            _make_path("_plugins", "_knn", "models", model_id),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def get_model(
        self,
        *,
        model_id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Used to retrieve information about models present in the cluster.


        :arg model_id: The id of the model.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if model_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'model_id'.")

        return self.transport.perform_request(
            "GET",
            _make_path("_plugins", "_knn", "models", model_id),
            params=params,
            headers=headers,
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
        "ccs_minimize_roundtrips",
        "default_operator",
        "df",
        "docvalue_fields",
        "error_trace",
        "expand_wildcards",
        "explain",
        "filter_path",
        "from_",
        "human",
        "ignore_throttled",
        "ignore_unavailable",
        "lenient",
        "max_concurrent_shard_requests",
        "pre_filter_shard_size",
        "preference",
        "pretty",
        "q",
        "request_cache",
        "rest_total_hits_as_int",
        "routing",
        "scroll",
        "search_type",
        "seq_no_primary_term",
        "size",
        "sort",
        "source",
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
    )
    def search_models(
        self,
        *,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Use an OpenSearch query to search for models in the index.


        :arg _source: Set to `true` or `false` to return the `_source`
            field or not, or a list of fields to return.
        :arg _source_excludes: List of fields to exclude from the
            returned `_source` field.
        :arg _source_includes: List of fields to extract and return from
            the `_source` field.
        :arg allow_no_indices: Whether to ignore if a wildcard indexes
            expression resolves into no concrete indexes. (This includes `_all`
            string or when no indexes have been specified).
        :arg allow_partial_search_results: Indicate if an error should
            be returned if there is a partial search failure or timeout. Default is
            True.
        :arg analyze_wildcard: Specify whether wildcard and prefix
            queries should be analyzed. Default is false.
        :arg analyzer: The analyzer to use for the query string.
        :arg batched_reduce_size: The number of shard results that
            should be reduced at once on the coordinating node. This value should be
            used as a protection mechanism to reduce the memory overhead per search
            request if the potential number of shards in the request can be large.
            Default is 512.
        :arg ccs_minimize_roundtrips: Indicates whether network round-
            trips should be minimized as part of cross-cluster search requests
            execution. Default is True.
        :arg default_operator: The default operator for query string
            query (AND or OR). Valid choices are and, or.
        :arg df: The field to use as default where no field prefix is
            given in the query string.
        :arg docvalue_fields: A comma-separated list of fields to return
            as the docvalue representation of a field for each hit.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Whether to expand wildcard expression to
            concrete indexes that are open, closed or both. Valid choices are all,
            closed, hidden, none, open.
        :arg explain: Specify whether to return detailed information
            about score computation as part of a hit.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg from_: Starting offset. Default is 0.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_throttled: Whether specified concrete, expanded or
            aliased indexes should be ignored when throttled.
        :arg ignore_unavailable: Whether specified concrete indexes
            should be ignored when unavailable (missing or closed).
        :arg lenient: Specify whether format-based query failures (such
            as providing text to a numeric field) should be ignored.
        :arg max_concurrent_shard_requests: The number of concurrent
            shard requests per node this search executes concurrently. This value
            should be used to limit the impact of the search on the cluster in order
            to limit the number of concurrent shard requests. Default is 5.
        :arg pre_filter_shard_size: Threshold that enforces a pre-filter
            round-trip to prefilter search shards based on query rewriting if the
            number of shards the search request expands to exceeds the threshold.
            This filter round-trip can limit the number of shards significantly if
            for instance a shard can not match any documents based on its rewrite
            method, that is if date filters are mandatory to match but the shard
            bounds and the query are disjoint.
        :arg preference: Specify the node or shard the operation should
            be performed on. Default is random.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg q: Query in the Lucene query string syntax.
        :arg request_cache: Specify if request cache should be used for
            this request or not, defaults to index level setting.
        :arg rest_total_hits_as_int: Indicates whether `hits.total`
            should be rendered as an integer or an object in the rest search
            response. Default is false.
        :arg routing: A comma-separated list of specific routing values.
        :arg scroll: Specify how long a consistent view of the index
            should be maintained for scrolled search.
        :arg search_type: Search operation type. Valid choices are
            dfs_query_then_fetch, query_then_fetch.
        :arg seq_no_primary_term: Specify whether to return sequence
            number and primary term of the last modification of each hit.
        :arg size: Number of hits to return. Default is 10.
        :arg sort: A comma-separated list of <field>:<direction> pairs.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg stats: Specific 'tag' of the request for logging and
            statistical purposes.
        :arg stored_fields: A comma-separated list of stored fields to
            return.
        :arg suggest_field: Specify which field to use for suggestions.
        :arg suggest_mode: Specify suggest mode. Valid choices are
            always, missing, popular.
        :arg suggest_size: How many suggestions to return in response.
        :arg suggest_text: The source text for which the suggestions
            should be returned.
        :arg terminate_after: The maximum number of documents to collect
            for each shard, upon reaching which the query execution will terminate
            early.
        :arg timeout: Operation timeout.
        :arg track_scores: Whether to calculate and return scores even
            if they are not used for sorting.
        :arg track_total_hits: Indicate if the number of documents that
            match the query should be tracked.
        :arg typed_keys: Specify whether aggregation and suggester names
            should be prefixed by their respective types in the response.
        :arg version: Whether to return document version as part of a
            hit.
        """
        # from is a reserved word so it cannot be used, use from_ instead
        if "from_" in params:
            params["from"] = params.pop("from_")

        return self.transport.perform_request(
            "POST",
            "/_plugins/_knn/models/_search",
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source", "timeout")
    def stats(
        self,
        *,
        node_id: Any = None,
        stat: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Provides information about the current status of the k-NN plugin.


        :arg node_id: A comma-separated list of node IDs or names to
            limit the returned information; use `_local` to return information from
            the node you're connecting to, leave empty to get information from all
            nodes.
        :arg stat: A comma-separated list of stats to retrieve; use
            `_all` or empty string to retrieve all stats.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: Operation timeout.
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_plugins", "_knn", node_id, "stats", stat),
            params=params,
            headers=headers,
        )

    @query_params(
        "error_trace", "filter_path", "human", "preference", "pretty", "source"
    )
    def train_model(
        self,
        *,
        body: Any = None,
        model_id: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Create and train a model that can be used for initializing k-NN native library
        indexes during indexing.


        :arg model_id: The id of the model.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg preference: Preferred node to execute training.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "POST",
            _make_path("_plugins", "_knn", "models", model_id, "_train"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def warmup(
        self,
        *,
        index: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Preloads native library files into memory, reducing initial search latency for
        specified indexes.


        :arg index: A comma-separated list of indexes; use `_all` or
            empty string to perform the operation on all indexes.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        return self.transport.perform_request(
            "GET",
            _make_path("_plugins", "_knn", "warmup", index),
            params=params,
            headers=headers,
        )
