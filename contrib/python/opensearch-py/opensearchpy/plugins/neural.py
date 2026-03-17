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

from ..client.utils import NamespacedClient, _make_path, query_params


class NeuralClient(NamespacedClient):
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
    def stats(
        self,
        *,
        node_id: Any = None,
        stat: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Provides information about the current status of the neural-search plugin.


        :arg node_id: A comma-separated list of node IDs or names to
            limit the returned information; leave empty to get information from all
            nodes.
        :arg stat: A comma-separated list of stats to retrieve; use
            empty string to retrieve all stats.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg flat_stat_paths: Whether to return stats in the flat form,
            which can improve readability, especially for heavily nested stats. For
            example, the flat form of `"processors": { "ingest": {
            "text_embedding_executions": 20181212 } }` is
            `"processors.ingest.text_embedding_executions": "20181212"`. Default is
            false.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg include_all_nodes: When `true` includes aggregated
            statistics across all nodes in the `all_nodes` category. When `false`,
            excludes the `all_nodes` category from the response. Default is True.
        :arg include_individual_nodes: When `true` includes statistics
            for individual nodes in the `nodes` category. When `false`, excludes the
            `nodes` category from the response. Default is True.
        :arg include_info: When `true` includes cluster-wide information
            in the `info` category. When `false`, excludes the `info` category from
            the response. Default is True.
        :arg include_metadata: Whether to return stat metadata instead
            of the raw stat value, includes additional information about the stat.
            These can include things like type hints, time since last stats being
            recorded, or recent rolling interval values Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_plugins", "_neural", node_id, "stats", stat),
            params=params,
            headers=headers,
        )
