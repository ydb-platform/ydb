# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
#
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


# ------------------------------------------------------------------------------------------
# THIS CODE IS AUTOMATICALLY GENERATED AND MANUAL EDITS WILL BE LOST
#
# To contribute, kindly make modifications in the opensearch-py client generator
# or in the OpenSearch API specification, and run `nox -rs generate`. See DEVELOPER_GUIDE.md
# and https://github.com/opensearch-project/opensearch-api-specification for details.
# -----------------------------------------------------------------------------------------+


import warnings
from typing import Any

from .utils import SKIP_IN_PATH, NamespacedClient, _make_path, query_params


class TasksClient(NamespacedClient):
    @query_params(
        "actions",
        "detailed",
        "error_trace",
        "filter_path",
        "group_by",
        "human",
        "nodes",
        "parent_task_id",
        "pretty",
        "source",
        "timeout",
        "wait_for_completion",
    )
    def list(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns a list of tasks.


        :arg actions: A comma-separated list of actions that should be
            returned. Keep empty to return all.
        :arg detailed: When `true`, the response includes detailed
            information about shard recoveries. Default is false.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg group_by: Groups tasks by parent/child relationships or
            nodes. Valid choices are nodes, none, parents.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg nodes: A comma-separated list of node IDs or names used to
            limit the returned information. Use `_local` to return information from
            the node you're connecting to, specify the node name to get information
            from a specific node, or keep the parameter empty to get information
            from all nodes.
        :arg parent_task_id: Returns tasks with a specified parent task
            ID (`node_id:task_number`). Keep empty or set to -1 to return all.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: The amount of time to wait for a response.
        :arg wait_for_completion: Waits for the matching task to
            complete. When `true`, the request is blocked until the task has
            completed. Default is false.
        """
        return self.transport.perform_request(
            "GET", "/_tasks", params=params, headers=headers
        )

    @query_params(
        "actions",
        "error_trace",
        "filter_path",
        "human",
        "nodes",
        "parent_task_id",
        "pretty",
        "source",
        "wait_for_completion",
    )
    def cancel(
        self,
        *,
        task_id: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Cancels a task, if it can be cancelled through an API.


        :arg task_id: The task ID.
        :arg actions: A comma-separated list of actions that should be
            returned. Keep empty to return all.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg nodes: A comma-separated list of node IDs or names used to
            limit the returned information. Use `_local` to return information from
            the node you're connecting to, specify the node name to get information
            from a specific node, or keep the parameter empty to get information
            from all nodes.
        :arg parent_task_id: Returns tasks with a specified parent task
            ID (`node_id:task_number`). Keep empty or set to -1 to return all.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg wait_for_completion: Waits for the matching task to
            complete. When `true`, the request is blocked until the task has
            completed. Default is false.
        """
        return self.transport.perform_request(
            "POST",
            _make_path("_tasks", task_id, "_cancel"),
            params=params,
            headers=headers,
        )

    @query_params(
        "error_trace",
        "filter_path",
        "human",
        "pretty",
        "source",
        "timeout",
        "wait_for_completion",
    )
    def get(
        self,
        *,
        task_id: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns information about a task.


        :arg task_id: The task ID.
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
        :arg timeout: The amount of time to wait for a response.
        :arg wait_for_completion: Waits for the matching task to
            complete. When `true`, the request is blocked until the task has
            completed. Default is false.
        """
        if task_id in SKIP_IN_PATH:
            warnings.warn(
                "Calling client.tasks.get() without a task_id is deprecated "
                "and will be removed in a future version. Use client.tasks.list() instead.",
                category=DeprecationWarning,
                stacklevel=3,
            )

        return self.transport.perform_request(
            "GET", _make_path("_tasks", task_id), params=params, headers=headers
        )
