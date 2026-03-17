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


class FlowFrameworkClient(NamespacedClient):
    @query_params(
        "error_trace",
        "filter_path",
        "human",
        "pretty",
        "provision",
        "reprovision",
        "source",
        "update_fields",
        "use_case",
        "validation",
    )
    async def create(
        self,
        *,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Creates a new workflow template.


        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg provision: Whether to provision the workflow as part of the
            request. Default is false.
        :arg reprovision: Whether to reprovision an existing workflow.
            Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg update_fields: Whether to update only the fields included
            in the request body.. Default is false.
        :arg use_case: Specifies the workflow template to use.
        :arg validation: Specifies the validation type. Valid values are
            `all` (validate the template) and `none` (do not validate the template).
            Default is all.
        """
        return await self.transport.perform_request(
            "POST",
            "/_plugins/_flow_framework/workflow",
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "clear_status", "error_trace", "filter_path", "human", "pretty", "source"
    )
    async def delete(
        self,
        *,
        workflow_id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Deletes a workflow template.


        :arg workflow_id: The ID of the workflow.
        :arg clear_status: Whether to delete the workflow state without
            deprovisioning resources. OpenSearch deletes the workflow state only if
            the provisioning status is not `IN_PROGRESS`. . Default is false.
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
        if workflow_id in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for a required argument 'workflow_id'."
            )

        return await self.transport.perform_request(
            "DELETE",
            _make_path("_plugins", "_flow_framework", "workflow", workflow_id),
            params=params,
            headers=headers,
        )

    @query_params(
        "allow_delete", "error_trace", "filter_path", "human", "pretty", "source"
    )
    async def deprovision(
        self,
        *,
        workflow_id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Deprovision workflow's resources when you no longer need them.


        :arg workflow_id: The ID of the workflow.
        :arg allow_delete: Specifies whether to allow deletion of
            resources with potential data loss.
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
        if workflow_id in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for a required argument 'workflow_id'."
            )

        return await self.transport.perform_request(
            "POST",
            _make_path(
                "_plugins", "_flow_framework", "workflow", workflow_id, "_deprovision"
            ),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def get(
        self,
        *,
        workflow_id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Retrieves a workflow template.


        :arg workflow_id: The ID of the workflow.
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
        if workflow_id in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for a required argument 'workflow_id'."
            )

        return await self.transport.perform_request(
            "GET",
            _make_path("_plugins", "_flow_framework", "workflow", workflow_id),
            params=params,
            headers=headers,
        )

    @query_params("all", "error_trace", "filter_path", "human", "pretty", "source")
    async def get_status(
        self,
        *,
        workflow_id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Retrieves the current workflow provisioning status.


        :arg workflow_id: The ID of the workflow.
        :arg all: Whether to return all fields in the response. Default
            is false.
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
        if workflow_id in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for a required argument 'workflow_id'."
            )

        return await self.transport.perform_request(
            "GET",
            _make_path(
                "_plugins", "_flow_framework", "workflow", workflow_id, "_status"
            ),
            params=params,
            headers=headers,
        )

    @query_params(
        "error_trace", "filter_path", "human", "pretty", "source", "workflow_step"
    )
    async def get_steps(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Retrieves available workflow steps.


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
        :arg workflow_step: The name of the workflow step.
        """
        return await self.transport.perform_request(
            "GET",
            "/_plugins/_flow_framework/workflow/_steps",
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def provision(
        self,
        *,
        workflow_id: Any,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Provisioning a workflow. This API is also executed when the Create or Update
        Workflow API is called with the provision parameter set to true.


        :arg workflow_id: The ID of the workflow.
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
        if workflow_id in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for a required argument 'workflow_id'."
            )

        return await self.transport.perform_request(
            "POST",
            _make_path(
                "_plugins", "_flow_framework", "workflow", workflow_id, "_provision"
            ),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def search(
        self,
        *,
        body: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Search for workflows by using a query matching a field.


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
        if body in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'body'.")

        return await self.transport.perform_request(
            "POST",
            "/_plugins/_flow_framework/workflow/_search",
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    async def search_state(
        self,
        *,
        body: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Search for workflows by using a query matching a field.


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
        if body in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'body'.")

        return await self.transport.perform_request(
            "POST",
            "/_plugins/_flow_framework/workflow/state/_search",
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "error_trace",
        "filter_path",
        "human",
        "pretty",
        "provision",
        "reprovision",
        "source",
        "update_fields",
        "use_case",
        "validation",
    )
    async def update(
        self,
        *,
        workflow_id: Any,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Updates a workflow template that has not been provisioned.


        :arg workflow_id: The ID of the workflow.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg provision: Whether to provision the workflow as part of the
            request. Default is false.
        :arg reprovision: Whether to reprovision an existing workflow.
            Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg update_fields: Whether to update only the fields included
            in the request body.. Default is false.
        :arg use_case: Specifies the workflow template to use.
        :arg validation: Specifies the validation type. Valid values are
            `all` (validate the template) and `none` (do not validate the template).
            Default is all.
        """
        if workflow_id in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for a required argument 'workflow_id'."
            )

        return await self.transport.perform_request(
            "PUT",
            _make_path("_plugins", "_flow_framework", "workflow", workflow_id),
            params=params,
            headers=headers,
            body=body,
        )
