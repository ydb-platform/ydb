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


class SearchRelevanceClient(NamespacedClient):
    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def delete_experiments(
        self,
        *,
        experiment_id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Deletes a specified experiment.


        :arg experiment_id: The experiment id
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
        if experiment_id in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for a required argument 'experiment_id'."
            )

        return await self.transport.perform_request(
            "DELETE",
            _make_path("_plugins", "_search_relevance", "experiments", experiment_id),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def delete_judgments(
        self,
        *,
        judgment_id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Deletes a specified judgment.


        :arg judgment_id: The judgment id
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
        if judgment_id in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for a required argument 'judgment_id'."
            )

        return await self.transport.perform_request(
            "DELETE",
            _make_path("_plugins", "_search_relevance", "judgments", judgment_id),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def delete_query_sets(
        self,
        *,
        query_set_id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Deletes a query set.


        :arg query_set_id: The query set id
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
        if query_set_id in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for a required argument 'query_set_id'."
            )

        return await self.transport.perform_request(
            "DELETE",
            _make_path("_plugins", "_search_relevance", "query_sets", query_set_id),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def delete_search_configurations(
        self,
        *,
        search_configuration_id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Deletes a specified search configuration.


        :arg search_configuration_id: The search configuration id
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
        if search_configuration_id in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for a required argument 'search_configuration_id'."
            )

        return await self.transport.perform_request(
            "DELETE",
            _make_path(
                "_plugins",
                "_search_relevance",
                "search_configurations",
                search_configuration_id,
            ),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def get_experiments(
        self,
        *,
        experiment_id: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Gets experiments.


        :arg experiment_id: The experiment id
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
        return await self.transport.perform_request(
            "GET",
            _make_path("_plugins", "_search_relevance", "experiments", experiment_id),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def get_judgments(
        self,
        *,
        judgment_id: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Gets judgments.


        :arg judgment_id: The judgment id
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
        return await self.transport.perform_request(
            "GET",
            _make_path("_plugins", "_search_relevance", "judgments", judgment_id),
            params=params,
            headers=headers,
        )

    @query_params(
        "error_trace",
        "filter_path",
        "flat_stat_paths",
        "human",
        "include_all_nodes",
        "include_individual_nodes",
        "include_info",
        "include_metadata",
        "pretty",
        "source",
    )
    async def get_node_stats(
        self,
        *,
        node_id: Any,
        stat: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Gets stats by node.


        :arg node_id: The node id
        :arg stat: The statistic to return
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg flat_stat_paths: Requests flattened stat paths as keys
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg include_all_nodes: Whether to include all nodes
        :arg include_individual_nodes: Whether to include individual
            nodes
        :arg include_info: Whether to include info
        :arg include_metadata: Whether to include metadata
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if node_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'node_id'.")

        return await self.transport.perform_request(
            "GET",
            _make_path("_plugins", "_search_relevance", node_id, "stats", stat),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def get_query_sets(
        self,
        *,
        query_set_id: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Lists the current query sets available.


        :arg query_set_id: The query set id
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
        return await self.transport.perform_request(
            "GET",
            _make_path("_plugins", "_search_relevance", "query_sets", query_set_id),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def get_search_configurations(
        self,
        *,
        search_configuration_id: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Gets the search configurations.


        :arg search_configuration_id: The search configuration id
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
        return await self.transport.perform_request(
            "GET",
            _make_path(
                "_plugins",
                "_search_relevance",
                "search_configurations",
                search_configuration_id,
            ),
            params=params,
            headers=headers,
        )

    @query_params(
        "error_trace",
        "filter_path",
        "flat_stat_paths",
        "human",
        "include_all_nodes",
        "include_individual_nodes",
        "include_info",
        "include_metadata",
        "pretty",
        "source",
    )
    async def get_stats(
        self,
        *,
        stat: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Gets stats.


        :arg stat: The statistic to return
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg flat_stat_paths: Requests flattened stat paths as keys
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg include_all_nodes: Whether to include all nodes
        :arg include_individual_nodes: Whether to include individual
            nodes
        :arg include_info: Whether to include info
        :arg include_metadata: Whether to include metadata
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return await self.transport.perform_request(
            "GET",
            _make_path("_plugins", "_search_relevance", "stats", stat),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def post_query_sets(
        self,
        *,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Creates a new query set by sampling queries from the user behavior data.


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
        return await self.transport.perform_request(
            "POST",
            "/_plugins/_search_relevance/query_sets",
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def put_experiments(
        self,
        *,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Creates an experiment.


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
        return await self.transport.perform_request(
            "PUT",
            "/_plugins/_search_relevance/experiments",
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def put_judgments(
        self,
        *,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Creates a judgment.


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
        return await self.transport.perform_request(
            "PUT",
            "/_plugins/_search_relevance/judgments",
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def put_query_sets(
        self,
        *,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Creates a new query set by uploading manually.


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
        return await self.transport.perform_request(
            "PUT",
            "/_plugins/_search_relevance/query_sets",
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def put_search_configurations(
        self,
        *,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Creates a search configuration.


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
        return await self.transport.perform_request(
            "PUT",
            "/_plugins/_search_relevance/search_configurations",
            params=params,
            headers=headers,
            body=body,
        )
