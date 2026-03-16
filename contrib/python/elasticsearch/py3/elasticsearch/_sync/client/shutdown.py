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


class ShutdownClient(NamespacedClient):

    @_rewrite_parameters()
    def delete_node(
        self,
        *,
        node_id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        master_timeout: t.Optional[
            t.Union[str, t.Literal["d", "h", "m", "micros", "ms", "nanos", "s"]]
        ] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[
            t.Union[str, t.Literal["d", "h", "m", "micros", "ms", "nanos", "s"]]
        ] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Cancel node shutdown preparations.
          Remove a node from the shutdown list so it can resume normal operations.
          You must explicitly clear the shutdown request when a node rejoins the cluster or when a node has permanently left the cluster.
          Shutdown requests are never removed automatically by Elasticsearch.</p>
          <p>NOTE: This feature is designed for indirect use by Elastic Cloud, Elastic Cloud Enterprise, and Elastic Cloud on Kubernetes.
          Direct use is not supported.</p>
          <p>If the operator privileges feature is enabled, you must be an operator to use this API.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-shutdown-delete-node>`_

        :param node_id: The node id of node to be removed from the shutdown state
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        :param timeout: Period to wait for a response. If no response is received before
            the timeout expires, the request fails and returns an error.
        """
        if node_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'node_id'")
        __path_parts: t.Dict[str, str] = {"node_id": _quote(node_id)}
        __path = f'/_nodes/{__path_parts["node_id"]}/shutdown'
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
        if timeout is not None:
            __query["timeout"] = timeout
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="shutdown.delete_node",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def get_node(
        self,
        *,
        node_id: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        master_timeout: t.Optional[
            t.Union[str, t.Literal["d", "h", "m", "micros", "ms", "nanos", "s"]]
        ] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get the shutdown status.</p>
          <p>Get information about nodes that are ready to be shut down, have shut down preparations still in progress, or have stalled.
          The API returns status information for each part of the shut down process.</p>
          <p>NOTE: This feature is designed for indirect use by Elasticsearch Service, Elastic Cloud Enterprise, and Elastic Cloud on Kubernetes. Direct use is not supported.</p>
          <p>If the operator privileges feature is enabled, you must be an operator to use this API.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-shutdown-get-node>`_

        :param node_id: Which node for which to retrieve the shutdown status
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        """
        __path_parts: t.Dict[str, str]
        if node_id not in SKIP_IN_PATH:
            __path_parts = {"node_id": _quote(node_id)}
            __path = f'/_nodes/{__path_parts["node_id"]}/shutdown'
        else:
            __path_parts = {}
            __path = "/_nodes/shutdown"
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
        return self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="shutdown.get_node",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("reason", "type", "allocation_delay", "target_node_name"),
    )
    def put_node(
        self,
        *,
        node_id: str,
        reason: t.Optional[str] = None,
        type: t.Optional[
            t.Union[str, t.Literal["remove", "replace", "restart"]]
        ] = None,
        allocation_delay: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        master_timeout: t.Optional[
            t.Union[str, t.Literal["d", "h", "m", "micros", "ms", "nanos", "s"]]
        ] = None,
        pretty: t.Optional[bool] = None,
        target_node_name: t.Optional[str] = None,
        timeout: t.Optional[
            t.Union[str, t.Literal["d", "h", "m", "micros", "ms", "nanos", "s"]]
        ] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Prepare a node to be shut down.</p>
          <p>NOTE: This feature is designed for indirect use by Elastic Cloud, Elastic Cloud Enterprise, and Elastic Cloud on Kubernetes. Direct use is not supported.</p>
          <p>If you specify a node that is offline, it will be prepared for shut down when it rejoins the cluster.</p>
          <p>If the operator privileges feature is enabled, you must be an operator to use this API.</p>
          <p>The API migrates ongoing tasks and index shards to other nodes as needed to prepare a node to be restarted or shut down and removed from the cluster.
          This ensures that Elasticsearch can be stopped safely with minimal disruption to the cluster.</p>
          <p>You must specify the type of shutdown: <code>restart</code>, <code>remove</code>, or <code>replace</code>.
          If a node is already being prepared for shutdown, you can use this API to change the shutdown type.</p>
          <p>IMPORTANT: This API does NOT terminate the Elasticsearch process.
          Monitor the node shutdown status to determine when it is safe to stop Elasticsearch.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-shutdown-put-node>`_

        :param node_id: The node identifier. This parameter is not validated against
            the cluster's active nodes. This enables you to register a node for shut
            down while it is offline. No error is thrown if you specify an invalid node
            ID.
        :param reason: A human-readable reason that the node is being shut down. This
            field provides information for other cluster operators; it does not affect
            the shut down process.
        :param type: Valid values are restart, remove, or replace. Use restart when you
            need to temporarily shut down a node to perform an upgrade, make configuration
            changes, or perform other maintenance. Because the node is expected to rejoin
            the cluster, data is not migrated off of the node. Use remove when you need
            to permanently remove a node from the cluster. The node is not marked ready
            for shutdown until data is migrated off of the node Use replace to do a 1:1
            replacement of a node with another node. Certain allocation decisions will
            be ignored (such as disk watermarks) in the interest of true replacement
            of the source node with the target node. During a replace-type shutdown,
            rollover and index creation may result in unassigned shards, and shrink may
            fail until the replacement is complete.
        :param allocation_delay: Only valid if type is restart. Controls how long Elasticsearch
            will wait for the node to restart and join the cluster before reassigning
            its shards to other nodes. This works the same as delaying allocation with
            the index.unassigned.node_left.delayed_timeout setting. If you specify both
            a restart allocation delay and an index-level allocation delay, the longer
            of the two is used.
        :param master_timeout: The period to wait for a connection to the master node.
            If no response is received before the timeout expires, the request fails
            and returns an error.
        :param target_node_name: Only valid if type is replace. Specifies the name of
            the node that is replacing the node being shut down. Shards from the shut
            down node are only allowed to be allocated to the target node, and no other
            data will be allocated to the target node. During relocation of data certain
            allocation rules are ignored, such as disk watermarks or user attribute filtering
            rules.
        :param timeout: The period to wait for a response. If no response is received
            before the timeout expires, the request fails and returns an error.
        """
        if node_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'node_id'")
        if reason is None and body is None:
            raise ValueError("Empty value passed for parameter 'reason'")
        if type is None and body is None:
            raise ValueError("Empty value passed for parameter 'type'")
        __path_parts: t.Dict[str, str] = {"node_id": _quote(node_id)}
        __path = f'/_nodes/{__path_parts["node_id"]}/shutdown'
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
            if reason is not None:
                __body["reason"] = reason
            if type is not None:
                __body["type"] = type
            if allocation_delay is not None:
                __body["allocation_delay"] = allocation_delay
            if target_node_name is not None:
                __body["target_node_name"] = target_node_name
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="shutdown.put_node",
            path_parts=__path_parts,
        )
