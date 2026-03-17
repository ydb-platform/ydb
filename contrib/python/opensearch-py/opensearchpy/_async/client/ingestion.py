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

from .utils import SKIP_IN_PATH, NamespacedClient, _make_path, query_params


class IngestionClient(NamespacedClient):
    @query_params(
        "error_trace",
        "filter_path",
        "human",
        "next_token",
        "pretty",
        "size",
        "source",
        "timeout",
    )
    async def get_state(
        self,
        *,
        index: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Use this API to retrieve the ingestion state for a given index.


        :arg index: Index for which ingestion state should be retrieved.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg next_token: Token to retrieve the next page of results.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg size: Number of results to return per page.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: Timeout for the request.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        return await self.transport.perform_request(
            "GET",
            _make_path(index, "ingestion", "_state"),
            params=params,
            headers=headers,
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "human",
        "pretty",
        "source",
        "timeout",
    )
    async def pause(
        self,
        *,
        index: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Use this API to pause ingestion for a given index.


        :arg index: Index for which ingestion should be paused.
        :arg cluster_manager_timeout: Time to wait for cluster manager
            connection.
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
        :arg timeout: Timeout for the request.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        return await self.transport.perform_request(
            "POST",
            _make_path(index, "ingestion", "_pause"),
            params=params,
            headers=headers,
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "human",
        "pretty",
        "source",
        "timeout",
    )
    async def resume(
        self,
        *,
        index: Any,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Use this API to resume ingestion for the given index.


        :arg index: Index for which ingestion should be resumed.
        :arg cluster_manager_timeout: Time to wait for cluster manager
            connection.
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
        :arg timeout: Timeout for the request.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        return await self.transport.perform_request(
            "POST",
            _make_path(index, "ingestion", "_resume"),
            params=params,
            headers=headers,
            body=body,
        )
