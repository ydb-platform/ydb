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


class SmClient(NamespacedClient):
    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def create_policy(
        self,
        *,
        policy_name: Any,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Creates a snapshot management policy.


        :arg policy_name: The name of the snapshot management policy to
            create.
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
        if policy_name in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for a required argument 'policy_name'."
            )

        return self.transport.perform_request(
            "POST",
            _make_path("_plugins", "_sm", "policies", policy_name),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def delete_policy(
        self,
        *,
        policy_name: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Deletes a snapshot management policy.


        :arg policy_name: The name of the snapshot management policy to
            delete.
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
        if policy_name in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for a required argument 'policy_name'."
            )

        return self.transport.perform_request(
            "DELETE",
            _make_path("_plugins", "_sm", "policies", policy_name),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def explain_policy(
        self,
        *,
        policy_name: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Explains the state of the snapshot management policy.


        :arg policy_name: The name of the snapshot management policy to
            explain.
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
        if policy_name in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for a required argument 'policy_name'."
            )

        return self.transport.perform_request(
            "GET",
            _make_path("_plugins", "_sm", "policies", policy_name, "_explain"),
            params=params,
            headers=headers,
        )

    @query_params(
        "error_trace",
        "filter_path",
        "from_",
        "human",
        "pretty",
        "queryString",
        "size",
        "sortField",
        "sortOrder",
        "source",
    )
    def get_policies(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Retrieves all snapshot management policies with optional pagination and
        filtering.


        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg from_: The starting index from which to retrieve snapshot
            management policies. Default is 0.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg queryString: The query string to filter the returned
            snapshot management policies.
        :arg size: The number of snapshot management policies to return.
        :arg sortField: The name of the field to sort the snapshot
            management policies by.
        :arg sortOrder: The order to sort the snapshot management
            policies. Valid choices are asc, desc.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        # from is a reserved word so it cannot be used, use from_ instead
        if "from_" in params:
            params["from"] = params.pop("from_")

        return self.transport.perform_request(
            "GET", "/_plugins/_sm/policies", params=params, headers=headers
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def get_policy(
        self,
        *,
        policy_name: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Retrieves a specific snapshot management policy by name.


        :arg policy_name: The name of the snapshot management policy to
            retrieve.
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
        if policy_name in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for a required argument 'policy_name'."
            )

        return self.transport.perform_request(
            "GET",
            _make_path("_plugins", "_sm", "policies", policy_name),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def start_policy(
        self,
        *,
        policy_name: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Starts a snapshot management policy.


        :arg policy_name: The name of the snapshot management policy to
            start.
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
        if policy_name in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for a required argument 'policy_name'."
            )

        return self.transport.perform_request(
            "POST",
            _make_path("_plugins", "_sm", "policies", policy_name, "_start"),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def stop_policy(
        self,
        *,
        policy_name: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Stops a snapshot management policy.


        :arg policy_name: The name of the snapshot management policy to
            stop.
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
        if policy_name in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for a required argument 'policy_name'."
            )

        return self.transport.perform_request(
            "POST",
            _make_path("_plugins", "_sm", "policies", policy_name, "_stop"),
            params=params,
            headers=headers,
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
    def update_policy(
        self,
        *,
        policy_name: Any,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Updates an existing snapshot management policy. Requires `if_seq_no` and
        `if_primary_term`.


        :arg policy_name: The name of the snapshot management policy to
            update.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg if_primary_term: The primary term of the policy to update.
        :arg if_seq_no: The sequence number of the policy to update.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if policy_name in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for a required argument 'policy_name'."
            )

        return self.transport.perform_request(
            "PUT",
            _make_path("_plugins", "_sm", "policies", policy_name),
            params=params,
            headers=headers,
            body=body,
        )
