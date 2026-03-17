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
from .utils import SKIP_IN_PATH, _quote, _rewrite_parameters


class WatcherClient(NamespacedClient):

    @_rewrite_parameters()
    async def ack_watch(
        self,
        *,
        watch_id: str,
        action_id: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Acknowledge a watch.
          Acknowledging a watch enables you to manually throttle the execution of the watch's actions.</p>
          <p>The acknowledgement state of an action is stored in the <code>status.actions.&lt;id&gt;.ack.state</code> structure.</p>
          <p>IMPORTANT: If the specified watch is currently being executed, this API will return an error
          The reason for this behavior is to prevent overwriting the watch status from a watch execution.</p>
          <p>Acknowledging an action throttles further executions of that action until its <code>ack.state</code> is reset to <code>awaits_successful_execution</code>.
          This happens when the condition of the watch is not met (the condition evaluates to false).
          To demonstrate how throttling works in practice and how it can be configured for individual actions within a watch, refer to External documentation.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-ack-watch>`_

        :param watch_id: The watch identifier.
        :param action_id: A comma-separated list of the action identifiers to acknowledge.
            If you omit this parameter, all of the actions of the watch are acknowledged.
        """
        if watch_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'watch_id'")
        __path_parts: t.Dict[str, str]
        if watch_id not in SKIP_IN_PATH and action_id not in SKIP_IN_PATH:
            __path_parts = {
                "watch_id": _quote(watch_id),
                "action_id": _quote(action_id),
            }
            __path = f'/_watcher/watch/{__path_parts["watch_id"]}/_ack/{__path_parts["action_id"]}'
        elif watch_id not in SKIP_IN_PATH:
            __path_parts = {"watch_id": _quote(watch_id)}
            __path = f'/_watcher/watch/{__path_parts["watch_id"]}/_ack'
        else:
            raise ValueError("Couldn't find a path for the given parameters")
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="watcher.ack_watch",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def activate_watch(
        self,
        *,
        watch_id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Activate a watch.
          A watch can be either active or inactive.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-activate-watch>`_

        :param watch_id: The watch identifier.
        """
        if watch_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'watch_id'")
        __path_parts: t.Dict[str, str] = {"watch_id": _quote(watch_id)}
        __path = f'/_watcher/watch/{__path_parts["watch_id"]}/_activate'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="watcher.activate_watch",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def deactivate_watch(
        self,
        *,
        watch_id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Deactivate a watch.
          A watch can be either active or inactive.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-deactivate-watch>`_

        :param watch_id: The watch identifier.
        """
        if watch_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'watch_id'")
        __path_parts: t.Dict[str, str] = {"watch_id": _quote(watch_id)}
        __path = f'/_watcher/watch/{__path_parts["watch_id"]}/_deactivate'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="watcher.deactivate_watch",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def delete_watch(
        self,
        *,
        id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Delete a watch.
          When the watch is removed, the document representing the watch in the <code>.watches</code> index is gone and it will never be run again.</p>
          <p>Deleting a watch does not delete any watch execution records related to this watch from the watch history.</p>
          <p>IMPORTANT: Deleting a watch must be done by using only this API.
          Do not delete the watch directly from the <code>.watches</code> index using the Elasticsearch delete document API
          When Elasticsearch security features are enabled, make sure no write privileges are granted to anyone for the <code>.watches</code> index.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-delete-watch>`_

        :param id: The watch identifier.
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        __path_parts: t.Dict[str, str] = {"id": _quote(id)}
        __path = f'/_watcher/watch/{__path_parts["id"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="watcher.delete_watch",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "action_modes",
            "alternative_input",
            "ignore_condition",
            "record_execution",
            "simulated_actions",
            "trigger_data",
            "watch",
        ),
    )
    async def execute_watch(
        self,
        *,
        id: t.Optional[str] = None,
        action_modes: t.Optional[
            t.Mapping[
                str,
                t.Union[
                    str,
                    t.Literal[
                        "execute", "force_execute", "force_simulate", "simulate", "skip"
                    ],
                ],
            ]
        ] = None,
        alternative_input: t.Optional[t.Mapping[str, t.Any]] = None,
        debug: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        ignore_condition: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        record_execution: t.Optional[bool] = None,
        simulated_actions: t.Optional[t.Mapping[str, t.Any]] = None,
        trigger_data: t.Optional[t.Mapping[str, t.Any]] = None,
        watch: t.Optional[t.Mapping[str, t.Any]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Run a watch.
          This API can be used to force execution of the watch outside of its triggering logic or to simulate the watch execution for debugging purposes.</p>
          <p>For testing and debugging purposes, you also have fine-grained control on how the watch runs.
          You can run the watch without running all of its actions or alternatively by simulating them.
          You can also force execution by ignoring the watch condition and control whether a watch record would be written to the watch history after it runs.</p>
          <p>You can use the run watch API to run watches that are not yet registered by specifying the watch definition inline.
          This serves as great tool for testing and debugging your watches prior to adding them to Watcher.</p>
          <p>When Elasticsearch security features are enabled on your cluster, watches are run with the privileges of the user that stored the watches.
          If your user is allowed to read index <code>a</code>, but not index <code>b</code>, then the exact same set of rules will apply during execution of a watch.</p>
          <p>When using the run watch API, the authorization data of the user that called the API will be used as a base, instead of the information who stored the watch.
          Refer to the external documentation for examples of watch execution requests, including existing, customized, and inline watches.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-execute-watch>`_

        :param id: The watch identifier.
        :param action_modes: Determines how to handle the watch actions as part of the
            watch execution.
        :param alternative_input: When present, the watch uses this object as a payload
            instead of executing its own input.
        :param debug: Defines whether the watch runs in debug mode.
        :param ignore_condition: When set to `true`, the watch execution uses the always
            condition. This can also be specified as an HTTP parameter.
        :param record_execution: When set to `true`, the watch record representing the
            watch execution result is persisted to the `.watcher-history` index for the
            current time. In addition, the status of the watch is updated, possibly throttling
            subsequent runs. This can also be specified as an HTTP parameter.
        :param simulated_actions:
        :param trigger_data: This structure is parsed as the data of the trigger event
            that will be used during the watch execution.
        :param watch: When present, this watch is used instead of the one specified in
            the request. This watch is not persisted to the index and `record_execution`
            cannot be set.
        """
        __path_parts: t.Dict[str, str]
        if id not in SKIP_IN_PATH:
            __path_parts = {"id": _quote(id)}
            __path = f'/_watcher/watch/{__path_parts["id"]}/_execute'
        else:
            __path_parts = {}
            __path = "/_watcher/watch/_execute"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if debug is not None:
            __query["debug"] = debug
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if action_modes is not None:
                __body["action_modes"] = action_modes
            if alternative_input is not None:
                __body["alternative_input"] = alternative_input
            if ignore_condition is not None:
                __body["ignore_condition"] = ignore_condition
            if record_execution is not None:
                __body["record_execution"] = record_execution
            if simulated_actions is not None:
                __body["simulated_actions"] = simulated_actions
            if trigger_data is not None:
                __body["trigger_data"] = trigger_data
            if watch is not None:
                __body["watch"] = watch
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="watcher.execute_watch",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def get_settings(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get Watcher index settings.
          Get settings for the Watcher internal index (<code>.watches</code>).
          Only a subset of settings are shown, for example <code>index.auto_expand_replicas</code> and <code>index.number_of_replicas</code>.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-get-settings>`_

        :param master_timeout: The period to wait for a connection to the master node.
            If no response is received before the timeout expires, the request fails
            and returns an error.
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_watcher/settings"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="watcher.get_settings",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def get_watch(
        self,
        *,
        id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get a watch.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-get-watch>`_

        :param id: The watch identifier.
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        __path_parts: t.Dict[str, str] = {"id": _quote(id)}
        __path = f'/_watcher/watch/{__path_parts["id"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="watcher.get_watch",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "actions",
            "condition",
            "input",
            "metadata",
            "throttle_period",
            "throttle_period_in_millis",
            "transform",
            "trigger",
        ),
    )
    async def put_watch(
        self,
        *,
        id: str,
        actions: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
        active: t.Optional[bool] = None,
        condition: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        if_primary_term: t.Optional[int] = None,
        if_seq_no: t.Optional[int] = None,
        input: t.Optional[t.Mapping[str, t.Any]] = None,
        metadata: t.Optional[t.Mapping[str, t.Any]] = None,
        pretty: t.Optional[bool] = None,
        throttle_period: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        throttle_period_in_millis: t.Optional[t.Any] = None,
        transform: t.Optional[t.Mapping[str, t.Any]] = None,
        trigger: t.Optional[t.Mapping[str, t.Any]] = None,
        version: t.Optional[int] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create or update a watch.
          When a watch is registered, a new document that represents the watch is added to the <code>.watches</code> index and its trigger is immediately registered with the relevant trigger engine.
          Typically for the <code>schedule</code> trigger, the scheduler is the trigger engine.</p>
          <p>IMPORTANT: You must use Kibana or this API to create a watch.
          Do not add a watch directly to the <code>.watches</code> index by using the Elasticsearch index API.
          If Elasticsearch security features are enabled, do not give users write privileges on the <code>.watches</code> index.</p>
          <p>When you add a watch you can also define its initial active state by setting the <em>active</em> parameter.</p>
          <p>When Elasticsearch security features are enabled, your watch can index or search only on indices for which the user that stored the watch has privileges.
          If the user is able to read index <code>a</code>, but not index <code>b</code>, the same will apply when the watch runs.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-put-watch>`_

        :param id: The identifier for the watch.
        :param actions: The list of actions that will be run if the condition matches.
        :param active: The initial state of the watch. The default value is `true`, which
            means the watch is active by default.
        :param condition: The condition that defines if the actions should be run.
        :param if_primary_term: only update the watch if the last operation that has
            changed the watch has the specified primary term
        :param if_seq_no: only update the watch if the last operation that has changed
            the watch has the specified sequence number
        :param input: The input that defines the input that loads the data for the watch.
        :param metadata: Metadata JSON that will be copied into the history entries.
        :param throttle_period: The minimum time between actions being run. The default
            is 5 seconds. This default can be changed in the config file with the setting
            `xpack.watcher.throttle.period.default_period`. If both this value and the
            `throttle_period_in_millis` parameter are specified, Watcher uses the last
            parameter included in the request.
        :param throttle_period_in_millis: Minimum time in milliseconds between actions
            being run. Defaults to 5000. If both this value and the throttle_period parameter
            are specified, Watcher uses the last parameter included in the request.
        :param transform: The transform that processes the watch payload to prepare it
            for the watch actions.
        :param trigger: The trigger that defines when the watch should run.
        :param version: Explicit version number for concurrency control
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        __path_parts: t.Dict[str, str] = {"id": _quote(id)}
        __path = f'/_watcher/watch/{__path_parts["id"]}'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if active is not None:
            __query["active"] = active
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if if_primary_term is not None:
            __query["if_primary_term"] = if_primary_term
        if if_seq_no is not None:
            __query["if_seq_no"] = if_seq_no
        if pretty is not None:
            __query["pretty"] = pretty
        if version is not None:
            __query["version"] = version
        if not __body:
            if actions is not None:
                __body["actions"] = actions
            if condition is not None:
                __body["condition"] = condition
            if input is not None:
                __body["input"] = input
            if metadata is not None:
                __body["metadata"] = metadata
            if throttle_period is not None:
                __body["throttle_period"] = throttle_period
            if throttle_period_in_millis is not None:
                __body["throttle_period_in_millis"] = throttle_period_in_millis
            if transform is not None:
                __body["transform"] = transform
            if trigger is not None:
                __body["trigger"] = trigger
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="watcher.put_watch",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("from_", "query", "search_after", "size", "sort"),
        parameter_aliases={"from": "from_"},
    )
    async def query_watches(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        from_: t.Optional[int] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        query: t.Optional[t.Mapping[str, t.Any]] = None,
        search_after: t.Optional[
            t.Sequence[t.Union[None, bool, float, int, str]]
        ] = None,
        size: t.Optional[int] = None,
        sort: t.Optional[
            t.Union[
                t.Sequence[t.Union[str, t.Mapping[str, t.Any]]],
                t.Union[str, t.Mapping[str, t.Any]],
            ]
        ] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Query watches.
          Get all registered watches in a paginated manner and optionally filter watches by a query.</p>
          <p>Note that only the <code>_id</code> and <code>metadata.*</code> fields are queryable or sortable.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-query-watches>`_

        :param from_: The offset from the first result to fetch. It must be non-negative.
        :param query: A query that filters the watches to be returned.
        :param search_after: Retrieve the next page of hits using a set of sort values
            from the previous page.
        :param size: The number of hits to return. It must be non-negative.
        :param sort: One or more fields used to sort the search results.
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_watcher/_query/watches"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        # The 'sort' parameter with a colon can't be encoded to the body.
        if sort is not None and (
            (isinstance(sort, str) and ":" in sort)
            or (
                isinstance(sort, (list, tuple))
                and all(isinstance(_x, str) for _x in sort)
                and any(":" in _x for _x in sort)
            )
        ):
            __query["sort"] = sort
            sort = None
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if from_ is not None:
                __body["from"] = from_
            if query is not None:
                __body["query"] = query
            if search_after is not None:
                __body["search_after"] = search_after
            if size is not None:
                __body["size"] = size
            if sort is not None:
                __body["sort"] = sort
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="watcher.query_watches",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def start(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Start the watch service.
          Start the Watcher service if it is not already running.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-start>`_

        :param master_timeout: Period to wait for a connection to the master node.
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_watcher/_start"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="watcher.start",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def stats(
        self,
        *,
        metric: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[
                        str,
                        t.Literal[
                            "_all",
                            "current_watches",
                            "pending_watches",
                            "queued_watches",
                        ],
                    ]
                ],
                t.Union[
                    str,
                    t.Literal[
                        "_all", "current_watches", "pending_watches", "queued_watches"
                    ],
                ],
            ]
        ] = None,
        emit_stacktraces: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get Watcher statistics.
          This API always returns basic metrics.
          You retrieve more metrics by using the metric parameter.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-stats>`_

        :param metric: Defines which additional metrics are included in the response.
        :param emit_stacktraces: Defines whether stack traces are generated for each
            watch that is running.
        """
        __path_parts: t.Dict[str, str]
        if metric not in SKIP_IN_PATH:
            __path_parts = {"metric": _quote(metric)}
            __path = f'/_watcher/stats/{__path_parts["metric"]}'
        else:
            __path_parts = {}
            __path = "/_watcher/stats"
        __query: t.Dict[str, t.Any] = {}
        if emit_stacktraces is not None:
            __query["emit_stacktraces"] = emit_stacktraces
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="watcher.stats",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def stop(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Stop the watch service.
          Stop the Watcher service if it is running.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-stop>`_

        :param master_timeout: The period to wait for the master node. If the master
            node is not available before the timeout expires, the request fails and returns
            an error. To indicate that the request should never timeout, set it to `-1`.
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_watcher/_stop"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="watcher.stop",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("index_auto_expand_replicas", "index_number_of_replicas"),
        parameter_aliases={
            "index.auto_expand_replicas": "index_auto_expand_replicas",
            "index.number_of_replicas": "index_number_of_replicas",
        },
    )
    async def update_settings(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        index_auto_expand_replicas: t.Optional[str] = None,
        index_number_of_replicas: t.Optional[int] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Update Watcher index settings.
          Update settings for the Watcher internal index (<code>.watches</code>).
          Only a subset of settings can be modified.
          This includes <code>index.auto_expand_replicas</code>, <code>index.number_of_replicas</code>, <code>index.routing.allocation.exclude.*</code>,
          <code>index.routing.allocation.include.*</code> and <code>index.routing.allocation.require.*</code>.
          Modification of <code>index.routing.allocation.include._tier_preference</code> is an exception and is not allowed as the
          Watcher shards must always be in the <code>data_content</code> tier.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-watcher-update-settings>`_

        :param index_auto_expand_replicas:
        :param index_number_of_replicas:
        :param master_timeout: The period to wait for a connection to the master node.
            If no response is received before the timeout expires, the request fails
            and returns an error.
        :param timeout: The period to wait for a response. If no response is received
            before the timeout expires, the request fails and returns an error.
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_watcher/settings"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if index_auto_expand_replicas is not None:
                __body["index.auto_expand_replicas"] = index_auto_expand_replicas
            if index_number_of_replicas is not None:
                __body["index.number_of_replicas"] = index_number_of_replicas
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="watcher.update_settings",
            path_parts=__path_parts,
        )
