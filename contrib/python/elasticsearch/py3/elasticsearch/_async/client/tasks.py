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

import typing as t

from elastic_transport import ObjectApiResponse

from ._base import NamespacedClient
from .utils import (
    SKIP_IN_PATH,
    Stability,
    _quote,
    _rewrite_parameters,
    _stability_warning,
)


class TasksClient(NamespacedClient):

    @_rewrite_parameters()
    @_stability_warning(Stability.EXPERIMENTAL)
    async def cancel(
        self,
        *,
        task_id: t.Optional[str] = None,
        actions: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        nodes: t.Optional[t.Sequence[str]] = None,
        parent_task_id: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        wait_for_completion: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Cancel a task.</p>
          <p>WARNING: The task management API is new and should still be considered a beta feature.
          The API may change in ways that are not backwards compatible.</p>
          <p>A task may continue to run for some time after it has been cancelled because it may not be able to safely stop its current activity straight away.
          It is also possible that Elasticsearch must complete its work on other tasks before it can process the cancellation.
          The get task information API will continue to list these cancelled tasks until they complete.
          The cancelled flag in the response indicates that the cancellation command has been processed and the task will stop as soon as possible.</p>
          <p>To troubleshoot why a cancelled task does not complete promptly, use the get task information API with the <code>?detailed</code> parameter to identify the other tasks the system is running.
          You can also use the node hot threads API to obtain detailed information about the work the system is doing instead of completing the cancelled task.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-tasks>`_

        :param task_id: The task identifier.
        :param actions: A comma-separated list or wildcard expression of actions that
            is used to limit the request.
        :param nodes: A comma-separated list of node IDs or names that is used to limit
            the request.
        :param parent_task_id: A parent task ID that is used to limit the tasks.
        :param wait_for_completion: If true, the request blocks until all found tasks
            are complete.
        """
        __path_parts: t.Dict[str, str]
        if task_id not in SKIP_IN_PATH:
            __path_parts = {"task_id": _quote(task_id)}
            __path = f'/_tasks/{__path_parts["task_id"]}/_cancel'
        else:
            __path_parts = {}
            __path = "/_tasks/_cancel"
        __query: t.Dict[str, t.Any] = {}
        if actions is not None:
            __query["actions"] = actions
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if nodes is not None:
            __query["nodes"] = nodes
        if parent_task_id is not None:
            __query["parent_task_id"] = parent_task_id
        if pretty is not None:
            __query["pretty"] = pretty
        if wait_for_completion is not None:
            __query["wait_for_completion"] = wait_for_completion
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="tasks.cancel",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    @_stability_warning(Stability.EXPERIMENTAL)
    async def get(
        self,
        *,
        task_id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        wait_for_completion: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get task information.
          Get information about a task currently running in the cluster.</p>
          <p>WARNING: The task management API is new and should still be considered a beta feature.
          The API may change in ways that are not backwards compatible.</p>
          <p>If the task identifier is not found, a 404 response code indicates that there are no resources that match the request.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-tasks>`_

        :param task_id: The task identifier.
        :param timeout: The period to wait for a response. If no response is received
            before the timeout expires, the request fails and returns an error.
        :param wait_for_completion: If `true`, the request blocks until the task has
            completed.
        """
        if task_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'task_id'")
        __path_parts: t.Dict[str, str] = {"task_id": _quote(task_id)}
        __path = f'/_tasks/{__path_parts["task_id"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        if wait_for_completion is not None:
            __query["wait_for_completion"] = wait_for_completion
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="tasks.get",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    @_stability_warning(Stability.EXPERIMENTAL)
    async def list(
        self,
        *,
        actions: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        detailed: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        group_by: t.Optional[
            t.Union[str, t.Literal["nodes", "none", "parents"]]
        ] = None,
        human: t.Optional[bool] = None,
        nodes: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        parent_task_id: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        wait_for_completion: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get all tasks.
          Get information about the tasks currently running on one or more nodes in the cluster.</p>
          <p>WARNING: The task management API is new and should still be considered a beta feature.
          The API may change in ways that are not backwards compatible.</p>
          <p><strong>Identifying running tasks</strong></p>
          <p>The <code>X-Opaque-Id header</code>, when provided on the HTTP request header, is going to be returned as a header in the response as well as in the headers field for in the task information.
          This enables you to track certain calls or associate certain tasks with the client that started them.
          For example:</p>
          <pre><code>curl -i -H &quot;X-Opaque-Id: 123456&quot; &quot;http://localhost:9200/_tasks?group_by=parents&quot;
          </code></pre>
          <p>The API returns the following result:</p>
          <pre><code>HTTP/1.1 200 OK
          X-Opaque-Id: 123456
          content-type: application/json; charset=UTF-8
          content-length: 831

          {
            &quot;tasks&quot; : {
              &quot;u5lcZHqcQhu-rUoFaqDphA:45&quot; : {
                &quot;node&quot; : &quot;u5lcZHqcQhu-rUoFaqDphA&quot;,
                &quot;id&quot; : 45,
                &quot;type&quot; : &quot;transport&quot;,
                &quot;action&quot; : &quot;cluster:monitor/tasks/lists&quot;,
                &quot;start_time_in_millis&quot; : 1513823752749,
                &quot;running_time_in_nanos&quot; : 293139,
                &quot;cancellable&quot; : false,
                &quot;headers&quot; : {
                  &quot;X-Opaque-Id&quot; : &quot;123456&quot;
                },
                &quot;children&quot; : [
                  {
                    &quot;node&quot; : &quot;u5lcZHqcQhu-rUoFaqDphA&quot;,
                    &quot;id&quot; : 46,
                    &quot;type&quot; : &quot;direct&quot;,
                    &quot;action&quot; : &quot;cluster:monitor/tasks/lists[n]&quot;,
                    &quot;start_time_in_millis&quot; : 1513823752750,
                    &quot;running_time_in_nanos&quot; : 92133,
                    &quot;cancellable&quot; : false,
                    &quot;parent_task_id&quot; : &quot;u5lcZHqcQhu-rUoFaqDphA:45&quot;,
                    &quot;headers&quot; : {
                      &quot;X-Opaque-Id&quot; : &quot;123456&quot;
                    }
                  }
                ]
              }
            }
           }
          </code></pre>
          <p>In this example, <code>X-Opaque-Id: 123456</code> is the ID as a part of the response header.
          The <code>X-Opaque-Id</code> in the task <code>headers</code> is the ID for the task that was initiated by the REST request.
          The <code>X-Opaque-Id</code> in the children <code>headers</code> is the child task of the task that was initiated by the REST request.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-tasks>`_

        :param actions: A comma-separated list or wildcard expression of actions used
            to limit the request. For example, you can use `cluser:*` to retrieve all
            cluster-related tasks.
        :param detailed: If `true`, the response includes detailed information about
            the running tasks. This information is useful to distinguish tasks from each
            other but is more costly to run.
        :param group_by: A key that is used to group tasks in the response. The task
            lists can be grouped either by nodes or by parent tasks.
        :param nodes: A comma-separated list of node IDs or names that is used to limit
            the returned information.
        :param parent_task_id: A parent task identifier that is used to limit returned
            information. To return all tasks, omit this parameter or use a value of `-1`.
            If the parent task is not found, the API does not return a 404 response code.
        :param timeout: The period to wait for each node to respond. If a node does not
            respond before its timeout expires, the response does not include its information.
            However, timed out nodes are included in the `node_failures` property.
        :param wait_for_completion: If `true`, the request blocks until the operation
            is complete.
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_tasks"
        __query: t.Dict[str, t.Any] = {}
        if actions is not None:
            __query["actions"] = actions
        if detailed is not None:
            __query["detailed"] = detailed
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if group_by is not None:
            __query["group_by"] = group_by
        if human is not None:
            __query["human"] = human
        if nodes is not None:
            __query["nodes"] = nodes
        if parent_task_id is not None:
            __query["parent_task_id"] = parent_task_id
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        if wait_for_completion is not None:
            __query["wait_for_completion"] = wait_for_completion
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="tasks.list",
            path_parts=__path_parts,
        )
