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


class AsynchronousSearchClient(NamespacedClient):
    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def delete(
        self,
        *,
        id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Deletes any responses from an asynchronous search.


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
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'id'.")

        return await self.transport.perform_request(
            "DELETE",
            _make_path("_plugins", "_asynchronous_search", id),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def get(
        self,
        *,
        id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Gets partial responses from an asynchronous search.


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
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'id'.")

        return await self.transport.perform_request(
            "GET",
            _make_path("_plugins", "_asynchronous_search", id),
            params=params,
            headers=headers,
        )

    @query_params(
        "error_trace",
        "filter_path",
        "human",
        "index",
        "keep_alive",
        "keep_on_completion",
        "pretty",
        "source",
        "wait_for_completion_timeout",
    )
    async def search(
        self,
        *,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Performs an asynchronous search.


        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg index: The name of the index to be searched. Can be an
            individual name, a comma-separated list of indexes, or a wildcard
            expression of index names.
        :arg keep_alive: The amount of time that the result is saved in
            the cluster. For example, `2d` means that the results are stored in the
            cluster for 48 hours. The saved search results are deleted after this
            period or if the search is canceled. Note that this includes the query
            execution time. If the query exceeds this amount of time, the process
            cancels this query automatically.
        :arg keep_on_completion: Whether to save the results in the
            cluster after the search is complete. You can examine the stored results
            at a later time.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg wait_for_completion_timeout: The amount of time to wait for
            the results. You can poll the remaining results based on an ID. The
            maximum value is 300 seconds. Default is `1s`.
        """
        return await self.transport.perform_request(
            "POST",
            "/_plugins/_asynchronous_search",
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def stats(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Monitors any asynchronous searches that are `running`, `completed`, or
        `persisted`.


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
            "/_plugins/_asynchronous_search/stats",
            params=params,
            headers=headers,
        )
