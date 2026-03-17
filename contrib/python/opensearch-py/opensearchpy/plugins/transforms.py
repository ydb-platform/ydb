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


class TransformsClient(NamespacedClient):
    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def delete(
        self,
        *,
        id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Delete an index transform.


        :arg id: Transform to delete
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

        return self.transport.perform_request(
            "DELETE",
            _make_path("_plugins", "_transform", id),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def explain(
        self,
        *,
        id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns the status and metadata of a transform job.


        :arg id: Transform to explain
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

        return self.transport.perform_request(
            "GET",
            _make_path("_plugins", "_transform", id, "_explain"),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def get(
        self,
        *,
        id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns the status and metadata of a transform job.


        :arg id: Transform to access
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

        return self.transport.perform_request(
            "GET",
            _make_path("_plugins", "_transform", id),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def preview(
        self,
        *,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns a preview of what a transformed index would look like.


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
        return self.transport.perform_request(
            "POST",
            "/_plugins/_transform/_preview",
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "error_trace",
        "filter_path",
        "human",
        "if_primary_term",
        "if_seq_no",
        "pretty",
        "source",
    )
    def put(
        self,
        *,
        id: Any,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Create an index transform, or update a transform if `if_seq_no` and
        `if_primary_term` are provided.


        :arg id: Transform to create/update
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg if_primary_term: Only perform the operation if the document
            has this primary term.
        :arg if_seq_no: Only perform the operation if the document has
            this sequence number.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'id'.")

        return self.transport.perform_request(
            "PUT",
            _make_path("_plugins", "_transform", id),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "error_trace",
        "filter_path",
        "from_",
        "human",
        "pretty",
        "search",
        "size",
        "sortDirection",
        "sortField",
        "source",
    )
    def search(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns the details of all transform jobs.


        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg from_: The starting transform to return. Default is `0`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg search: The search term to use to filter results.
        :arg size: Specifies the number of transforms to return. Default
            is `10`.
        :arg sortDirection: Specifies the direction to sort results in.
            Can be `ASC` or `DESC`. Default is `ASC`.
        :arg sortField: The field to sort results with.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        # from is a reserved word so it cannot be used, use from_ instead
        if "from_" in params:
            params["from"] = params.pop("from_")

        return self.transport.perform_request(
            "GET", "/_plugins/_transform", params=params, headers=headers
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def start(
        self,
        *,
        id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Start transform.


        :arg id: Transform to start
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

        return self.transport.perform_request(
            "POST",
            _make_path("_plugins", "_transform", id, "_start"),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def stop(
        self,
        *,
        id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Stop transform.


        :arg id: Transform to stop
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

        return self.transport.perform_request(
            "POST",
            _make_path("_plugins", "_transform", id, "_stop"),
            params=params,
            headers=headers,
        )
