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


class ConnectorClient(NamespacedClient):

    @_rewrite_parameters()
    @_stability_warning(Stability.EXPERIMENTAL)
    async def check_in(
        self,
        *,
        connector_id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Check in a connector.</p>
          <p>Update the <code>last_seen</code> field in the connector and set it to the current timestamp.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-check-in>`_

        :param connector_id: The unique identifier of the connector to be checked in
        """
        if connector_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'connector_id'")
        __path_parts: t.Dict[str, str] = {"connector_id": _quote(connector_id)}
        __path = f'/_connector/{__path_parts["connector_id"]}/_check_in'
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
            endpoint_id="connector.check_in",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    @_stability_warning(Stability.BETA)
    async def delete(
        self,
        *,
        connector_id: str,
        delete_sync_jobs: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        hard: t.Optional[bool] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Delete a connector.</p>
          <p>Removes a connector and associated sync jobs.
          This is a destructive action that is not recoverable.
          NOTE: This action doesn’t delete any API keys, ingest pipelines, or data indices associated with the connector.
          These need to be removed manually.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-delete>`_

        :param connector_id: The unique identifier of the connector to be deleted
        :param delete_sync_jobs: A flag indicating if associated sync jobs should be
            also removed. Defaults to false.
        :param hard: A flag indicating if the connector should be hard deleted.
        """
        if connector_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'connector_id'")
        __path_parts: t.Dict[str, str] = {"connector_id": _quote(connector_id)}
        __path = f'/_connector/{__path_parts["connector_id"]}'
        __query: t.Dict[str, t.Any] = {}
        if delete_sync_jobs is not None:
            __query["delete_sync_jobs"] = delete_sync_jobs
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if hard is not None:
            __query["hard"] = hard
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
            endpoint_id="connector.delete",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    @_stability_warning(Stability.BETA)
    async def get(
        self,
        *,
        connector_id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        include_deleted: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get a connector.</p>
          <p>Get the details about a connector.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-get>`_

        :param connector_id: The unique identifier of the connector
        :param include_deleted: A flag to indicate if the desired connector should be
            fetched, even if it was soft-deleted.
        """
        if connector_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'connector_id'")
        __path_parts: t.Dict[str, str] = {"connector_id": _quote(connector_id)}
        __path = f'/_connector/{__path_parts["connector_id"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if include_deleted is not None:
            __query["include_deleted"] = include_deleted
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="connector.get",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "last_access_control_sync_error",
            "last_access_control_sync_scheduled_at",
            "last_access_control_sync_status",
            "last_deleted_document_count",
            "last_incremental_sync_scheduled_at",
            "last_indexed_document_count",
            "last_seen",
            "last_sync_error",
            "last_sync_scheduled_at",
            "last_sync_status",
            "last_synced",
            "sync_cursor",
        ),
    )
    @_stability_warning(Stability.EXPERIMENTAL)
    async def last_sync(
        self,
        *,
        connector_id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        last_access_control_sync_error: t.Optional[str] = None,
        last_access_control_sync_scheduled_at: t.Optional[t.Union[str, t.Any]] = None,
        last_access_control_sync_status: t.Optional[
            t.Union[
                str,
                t.Literal[
                    "canceled",
                    "canceling",
                    "completed",
                    "error",
                    "in_progress",
                    "pending",
                    "suspended",
                ],
            ]
        ] = None,
        last_deleted_document_count: t.Optional[int] = None,
        last_incremental_sync_scheduled_at: t.Optional[t.Union[str, t.Any]] = None,
        last_indexed_document_count: t.Optional[int] = None,
        last_seen: t.Optional[t.Union[str, t.Any]] = None,
        last_sync_error: t.Optional[str] = None,
        last_sync_scheduled_at: t.Optional[t.Union[str, t.Any]] = None,
        last_sync_status: t.Optional[
            t.Union[
                str,
                t.Literal[
                    "canceled",
                    "canceling",
                    "completed",
                    "error",
                    "in_progress",
                    "pending",
                    "suspended",
                ],
            ]
        ] = None,
        last_synced: t.Optional[t.Union[str, t.Any]] = None,
        pretty: t.Optional[bool] = None,
        sync_cursor: t.Optional[t.Any] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Update the connector last sync stats.</p>
          <p>Update the fields related to the last sync of a connector.
          This action is used for analytics and monitoring.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-last-sync>`_

        :param connector_id: The unique identifier of the connector to be updated
        :param last_access_control_sync_error:
        :param last_access_control_sync_scheduled_at:
        :param last_access_control_sync_status:
        :param last_deleted_document_count:
        :param last_incremental_sync_scheduled_at:
        :param last_indexed_document_count:
        :param last_seen:
        :param last_sync_error:
        :param last_sync_scheduled_at:
        :param last_sync_status:
        :param last_synced:
        :param sync_cursor:
        """
        if connector_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'connector_id'")
        __path_parts: t.Dict[str, str] = {"connector_id": _quote(connector_id)}
        __path = f'/_connector/{__path_parts["connector_id"]}/_last_sync'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if last_access_control_sync_error is not None:
                __body["last_access_control_sync_error"] = (
                    last_access_control_sync_error
                )
            if last_access_control_sync_scheduled_at is not None:
                __body["last_access_control_sync_scheduled_at"] = (
                    last_access_control_sync_scheduled_at
                )
            if last_access_control_sync_status is not None:
                __body["last_access_control_sync_status"] = (
                    last_access_control_sync_status
                )
            if last_deleted_document_count is not None:
                __body["last_deleted_document_count"] = last_deleted_document_count
            if last_incremental_sync_scheduled_at is not None:
                __body["last_incremental_sync_scheduled_at"] = (
                    last_incremental_sync_scheduled_at
                )
            if last_indexed_document_count is not None:
                __body["last_indexed_document_count"] = last_indexed_document_count
            if last_seen is not None:
                __body["last_seen"] = last_seen
            if last_sync_error is not None:
                __body["last_sync_error"] = last_sync_error
            if last_sync_scheduled_at is not None:
                __body["last_sync_scheduled_at"] = last_sync_scheduled_at
            if last_sync_status is not None:
                __body["last_sync_status"] = last_sync_status
            if last_synced is not None:
                __body["last_synced"] = last_synced
            if sync_cursor is not None:
                __body["sync_cursor"] = sync_cursor
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="connector.last_sync",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        parameter_aliases={"from": "from_"},
    )
    @_stability_warning(Stability.BETA)
    async def list(
        self,
        *,
        connector_name: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        from_: t.Optional[int] = None,
        human: t.Optional[bool] = None,
        include_deleted: t.Optional[bool] = None,
        index_name: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        pretty: t.Optional[bool] = None,
        query: t.Optional[str] = None,
        service_type: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        size: t.Optional[int] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get all connectors.</p>
          <p>Get information about all connectors.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-list>`_

        :param connector_name: A comma-separated list of connector names to fetch connector
            documents for
        :param from_: Starting offset (default: 0)
        :param include_deleted: A flag to indicate if the desired connector should be
            fetched, even if it was soft-deleted.
        :param index_name: A comma-separated list of connector index names to fetch connector
            documents for
        :param query: A wildcard query string that filters connectors with matching name,
            description or index name
        :param service_type: A comma-separated list of connector service types to fetch
            connector documents for
        :param size: Specifies a max number of results to get
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_connector"
        __query: t.Dict[str, t.Any] = {}
        if connector_name is not None:
            __query["connector_name"] = connector_name
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if from_ is not None:
            __query["from"] = from_
        if human is not None:
            __query["human"] = human
        if include_deleted is not None:
            __query["include_deleted"] = include_deleted
        if index_name is not None:
            __query["index_name"] = index_name
        if pretty is not None:
            __query["pretty"] = pretty
        if query is not None:
            __query["query"] = query
        if service_type is not None:
            __query["service_type"] = service_type
        if size is not None:
            __query["size"] = size
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="connector.list",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "description",
            "index_name",
            "is_native",
            "language",
            "name",
            "service_type",
        ),
    )
    @_stability_warning(Stability.BETA)
    async def post(
        self,
        *,
        description: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        index_name: t.Optional[str] = None,
        is_native: t.Optional[bool] = None,
        language: t.Optional[str] = None,
        name: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        service_type: t.Optional[str] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create a connector.</p>
          <p>Connectors are Elasticsearch integrations that bring content from third-party data sources, which can be deployed on Elastic Cloud or hosted on your own infrastructure.
          Elastic managed connectors (Native connectors) are a managed service on Elastic Cloud.
          Self-managed connectors (Connector clients) are self-managed on your infrastructure.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-put>`_

        :param description:
        :param index_name:
        :param is_native:
        :param language:
        :param name:
        :param service_type:
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_connector"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if description is not None:
                __body["description"] = description
            if index_name is not None:
                __body["index_name"] = index_name
            if is_native is not None:
                __body["is_native"] = is_native
            if language is not None:
                __body["language"] = language
            if name is not None:
                __body["name"] = name
            if service_type is not None:
                __body["service_type"] = service_type
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
            endpoint_id="connector.post",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "description",
            "index_name",
            "is_native",
            "language",
            "name",
            "service_type",
        ),
    )
    @_stability_warning(Stability.BETA)
    async def put(
        self,
        *,
        connector_id: t.Optional[str] = None,
        description: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        index_name: t.Optional[str] = None,
        is_native: t.Optional[bool] = None,
        language: t.Optional[str] = None,
        name: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        service_type: t.Optional[str] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create or update a connector.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-put>`_

        :param connector_id: The unique identifier of the connector to be created or
            updated. ID is auto-generated if not provided.
        :param description:
        :param index_name:
        :param is_native:
        :param language:
        :param name:
        :param service_type:
        """
        __path_parts: t.Dict[str, str]
        if connector_id not in SKIP_IN_PATH:
            __path_parts = {"connector_id": _quote(connector_id)}
            __path = f'/_connector/{__path_parts["connector_id"]}'
        else:
            __path_parts = {}
            __path = "/_connector"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if description is not None:
                __body["description"] = description
            if index_name is not None:
                __body["index_name"] = index_name
            if is_native is not None:
                __body["is_native"] = is_native
            if language is not None:
                __body["language"] = language
            if name is not None:
                __body["name"] = name
            if service_type is not None:
                __body["service_type"] = service_type
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
            endpoint_id="connector.put",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    @_stability_warning(Stability.BETA)
    async def sync_job_cancel(
        self,
        *,
        connector_sync_job_id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Cancel a connector sync job.</p>
          <p>Cancel a connector sync job, which sets the status to cancelling and updates <code>cancellation_requested_at</code> to the current time.
          The connector service is then responsible for setting the status of connector sync jobs to cancelled.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-sync-job-cancel>`_

        :param connector_sync_job_id: The unique identifier of the connector sync job
        """
        if connector_sync_job_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'connector_sync_job_id'")
        __path_parts: t.Dict[str, str] = {
            "connector_sync_job_id": _quote(connector_sync_job_id)
        }
        __path = (
            f'/_connector/_sync_job/{__path_parts["connector_sync_job_id"]}/_cancel'
        )
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
            endpoint_id="connector.sync_job_cancel",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    @_stability_warning(Stability.EXPERIMENTAL)
    async def sync_job_check_in(
        self,
        *,
        connector_sync_job_id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Check in a connector sync job.
          Check in a connector sync job and set the <code>last_seen</code> field to the current time before updating it in the internal index.</p>
          <p>To sync data using self-managed connectors, you need to deploy the Elastic connector service on your own infrastructure.
          This service runs automatically on Elastic Cloud for Elastic managed connectors.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-sync-job-check-in>`_

        :param connector_sync_job_id: The unique identifier of the connector sync job
            to be checked in.
        """
        if connector_sync_job_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'connector_sync_job_id'")
        __path_parts: t.Dict[str, str] = {
            "connector_sync_job_id": _quote(connector_sync_job_id)
        }
        __path = (
            f'/_connector/_sync_job/{__path_parts["connector_sync_job_id"]}/_check_in'
        )
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
            endpoint_id="connector.sync_job_check_in",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("worker_hostname", "sync_cursor"),
    )
    @_stability_warning(Stability.EXPERIMENTAL)
    async def sync_job_claim(
        self,
        *,
        connector_sync_job_id: str,
        worker_hostname: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        sync_cursor: t.Optional[t.Any] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Claim a connector sync job.
          This action updates the job status to <code>in_progress</code> and sets the <code>last_seen</code> and <code>started_at</code> timestamps to the current time.
          Additionally, it can set the <code>sync_cursor</code> property for the sync job.</p>
          <p>This API is not intended for direct connector management by users.
          It supports the implementation of services that utilize the connector protocol to communicate with Elasticsearch.</p>
          <p>To sync data using self-managed connectors, you need to deploy the Elastic connector service on your own infrastructure.
          This service runs automatically on Elastic Cloud for Elastic managed connectors.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-sync-job-claim>`_

        :param connector_sync_job_id: The unique identifier of the connector sync job.
        :param worker_hostname: The host name of the current system that will run the
            job.
        :param sync_cursor: The cursor object from the last incremental sync job. This
            should reference the `sync_cursor` field in the connector state for which
            the job runs.
        """
        if connector_sync_job_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'connector_sync_job_id'")
        if worker_hostname is None and body is None:
            raise ValueError("Empty value passed for parameter 'worker_hostname'")
        __path_parts: t.Dict[str, str] = {
            "connector_sync_job_id": _quote(connector_sync_job_id)
        }
        __path = f'/_connector/_sync_job/{__path_parts["connector_sync_job_id"]}/_claim'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if worker_hostname is not None:
                __body["worker_hostname"] = worker_hostname
            if sync_cursor is not None:
                __body["sync_cursor"] = sync_cursor
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="connector.sync_job_claim",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    @_stability_warning(Stability.BETA)
    async def sync_job_delete(
        self,
        *,
        connector_sync_job_id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Delete a connector sync job.</p>
          <p>Remove a connector sync job and its associated data.
          This is a destructive action that is not recoverable.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-sync-job-delete>`_

        :param connector_sync_job_id: The unique identifier of the connector sync job
            to be deleted
        """
        if connector_sync_job_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'connector_sync_job_id'")
        __path_parts: t.Dict[str, str] = {
            "connector_sync_job_id": _quote(connector_sync_job_id)
        }
        __path = f'/_connector/_sync_job/{__path_parts["connector_sync_job_id"]}'
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
            endpoint_id="connector.sync_job_delete",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("error",),
    )
    @_stability_warning(Stability.EXPERIMENTAL)
    async def sync_job_error(
        self,
        *,
        connector_sync_job_id: str,
        error: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Set a connector sync job error.
          Set the <code>error</code> field for a connector sync job and set its <code>status</code> to <code>error</code>.</p>
          <p>To sync data using self-managed connectors, you need to deploy the Elastic connector service on your own infrastructure.
          This service runs automatically on Elastic Cloud for Elastic managed connectors.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-sync-job-error>`_

        :param connector_sync_job_id: The unique identifier for the connector sync job.
        :param error: The error for the connector sync job error field.
        """
        if connector_sync_job_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'connector_sync_job_id'")
        if error is None and body is None:
            raise ValueError("Empty value passed for parameter 'error'")
        __path_parts: t.Dict[str, str] = {
            "connector_sync_job_id": _quote(connector_sync_job_id)
        }
        __path = f'/_connector/_sync_job/{__path_parts["connector_sync_job_id"]}/_error'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if error is not None:
                __body["error"] = error
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="connector.sync_job_error",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    @_stability_warning(Stability.BETA)
    async def sync_job_get(
        self,
        *,
        connector_sync_job_id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get a connector sync job.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-sync-job-get>`_

        :param connector_sync_job_id: The unique identifier of the connector sync job
        """
        if connector_sync_job_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'connector_sync_job_id'")
        __path_parts: t.Dict[str, str] = {
            "connector_sync_job_id": _quote(connector_sync_job_id)
        }
        __path = f'/_connector/_sync_job/{__path_parts["connector_sync_job_id"]}'
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
            endpoint_id="connector.sync_job_get",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        parameter_aliases={"from": "from_"},
    )
    @_stability_warning(Stability.BETA)
    async def sync_job_list(
        self,
        *,
        connector_id: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        from_: t.Optional[int] = None,
        human: t.Optional[bool] = None,
        job_type: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["access_control", "full", "incremental"]]
                ],
                t.Union[str, t.Literal["access_control", "full", "incremental"]],
            ]
        ] = None,
        pretty: t.Optional[bool] = None,
        size: t.Optional[int] = None,
        status: t.Optional[
            t.Union[
                str,
                t.Literal[
                    "canceled",
                    "canceling",
                    "completed",
                    "error",
                    "in_progress",
                    "pending",
                    "suspended",
                ],
            ]
        ] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get all connector sync jobs.</p>
          <p>Get information about all stored connector sync jobs listed by their creation date in ascending order.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-sync-job-list>`_

        :param connector_id: A connector id to fetch connector sync jobs for
        :param from_: Starting offset (default: 0)
        :param job_type: A comma-separated list of job types to fetch the sync jobs for
        :param size: Specifies a max number of results to get
        :param status: A sync job status to fetch connector sync jobs for
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_connector/_sync_job"
        __query: t.Dict[str, t.Any] = {}
        if connector_id is not None:
            __query["connector_id"] = connector_id
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if from_ is not None:
            __query["from"] = from_
        if human is not None:
            __query["human"] = human
        if job_type is not None:
            __query["job_type"] = job_type
        if pretty is not None:
            __query["pretty"] = pretty
        if size is not None:
            __query["size"] = size
        if status is not None:
            __query["status"] = status
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="connector.sync_job_list",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("id", "job_type", "trigger_method"),
    )
    @_stability_warning(Stability.BETA)
    async def sync_job_post(
        self,
        *,
        id: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        job_type: t.Optional[
            t.Union[str, t.Literal["access_control", "full", "incremental"]]
        ] = None,
        pretty: t.Optional[bool] = None,
        trigger_method: t.Optional[
            t.Union[str, t.Literal["on_demand", "scheduled"]]
        ] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create a connector sync job.</p>
          <p>Create a connector sync job document in the internal index and initialize its counters and timestamps with default values.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-sync-job-post>`_

        :param id: The id of the associated connector
        :param job_type:
        :param trigger_method:
        """
        if id is None and body is None:
            raise ValueError("Empty value passed for parameter 'id'")
        __path_parts: t.Dict[str, str] = {}
        __path = "/_connector/_sync_job"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if id is not None:
                __body["id"] = id
            if job_type is not None:
                __body["job_type"] = job_type
            if trigger_method is not None:
                __body["trigger_method"] = trigger_method
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="connector.sync_job_post",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "deleted_document_count",
            "indexed_document_count",
            "indexed_document_volume",
            "last_seen",
            "metadata",
            "total_document_count",
        ),
    )
    @_stability_warning(Stability.EXPERIMENTAL)
    async def sync_job_update_stats(
        self,
        *,
        connector_sync_job_id: str,
        deleted_document_count: t.Optional[int] = None,
        indexed_document_count: t.Optional[int] = None,
        indexed_document_volume: t.Optional[int] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        last_seen: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        metadata: t.Optional[t.Mapping[str, t.Any]] = None,
        pretty: t.Optional[bool] = None,
        total_document_count: t.Optional[int] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Set the connector sync job stats.
          Stats include: <code>deleted_document_count</code>, <code>indexed_document_count</code>, <code>indexed_document_volume</code>, and <code>total_document_count</code>.
          You can also update <code>last_seen</code>.
          This API is mainly used by the connector service for updating sync job information.</p>
          <p>To sync data using self-managed connectors, you need to deploy the Elastic connector service on your own infrastructure.
          This service runs automatically on Elastic Cloud for Elastic managed connectors.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-sync-job-update-stats>`_

        :param connector_sync_job_id: The unique identifier of the connector sync job.
        :param deleted_document_count: The number of documents the sync job deleted.
        :param indexed_document_count: The number of documents the sync job indexed.
        :param indexed_document_volume: The total size of the data (in MiB) the sync
            job indexed.
        :param last_seen: The timestamp to use in the `last_seen` property for the connector
            sync job.
        :param metadata: The connector-specific metadata.
        :param total_document_count: The total number of documents in the target index
            after the sync job finished.
        """
        if connector_sync_job_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'connector_sync_job_id'")
        if deleted_document_count is None and body is None:
            raise ValueError(
                "Empty value passed for parameter 'deleted_document_count'"
            )
        if indexed_document_count is None and body is None:
            raise ValueError(
                "Empty value passed for parameter 'indexed_document_count'"
            )
        if indexed_document_volume is None and body is None:
            raise ValueError(
                "Empty value passed for parameter 'indexed_document_volume'"
            )
        __path_parts: t.Dict[str, str] = {
            "connector_sync_job_id": _quote(connector_sync_job_id)
        }
        __path = f'/_connector/_sync_job/{__path_parts["connector_sync_job_id"]}/_stats'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if deleted_document_count is not None:
                __body["deleted_document_count"] = deleted_document_count
            if indexed_document_count is not None:
                __body["indexed_document_count"] = indexed_document_count
            if indexed_document_volume is not None:
                __body["indexed_document_volume"] = indexed_document_volume
            if last_seen is not None:
                __body["last_seen"] = last_seen
            if metadata is not None:
                __body["metadata"] = metadata
            if total_document_count is not None:
                __body["total_document_count"] = total_document_count
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="connector.sync_job_update_stats",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    @_stability_warning(Stability.EXPERIMENTAL)
    async def update_active_filtering(
        self,
        *,
        connector_id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Activate the connector draft filter.</p>
          <p>Activates the valid draft filtering for a connector.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-update-filtering>`_

        :param connector_id: The unique identifier of the connector to be updated
        """
        if connector_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'connector_id'")
        __path_parts: t.Dict[str, str] = {"connector_id": _quote(connector_id)}
        __path = f'/_connector/{__path_parts["connector_id"]}/_filtering/_activate'
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
            endpoint_id="connector.update_active_filtering",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("api_key_id", "api_key_secret_id"),
    )
    @_stability_warning(Stability.BETA)
    async def update_api_key_id(
        self,
        *,
        connector_id: str,
        api_key_id: t.Optional[str] = None,
        api_key_secret_id: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Update the connector API key ID.</p>
          <p>Update the <code>api_key_id</code> and <code>api_key_secret_id</code> fields of a connector.
          You can specify the ID of the API key used for authorization and the ID of the connector secret where the API key is stored.
          The connector secret ID is required only for Elastic managed (native) connectors.
          Self-managed connectors (connector clients) do not use this field.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-update-api-key-id>`_

        :param connector_id: The unique identifier of the connector to be updated
        :param api_key_id:
        :param api_key_secret_id:
        """
        if connector_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'connector_id'")
        __path_parts: t.Dict[str, str] = {"connector_id": _quote(connector_id)}
        __path = f'/_connector/{__path_parts["connector_id"]}/_api_key_id'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if api_key_id is not None:
                __body["api_key_id"] = api_key_id
            if api_key_secret_id is not None:
                __body["api_key_secret_id"] = api_key_secret_id
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="connector.update_api_key_id",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("configuration", "values"),
    )
    @_stability_warning(Stability.BETA)
    async def update_configuration(
        self,
        *,
        connector_id: str,
        configuration: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        values: t.Optional[t.Mapping[str, t.Any]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Update the connector configuration.</p>
          <p>Update the configuration field in the connector document.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-update-configuration>`_

        :param connector_id: The unique identifier of the connector to be updated
        :param configuration:
        :param values:
        """
        if connector_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'connector_id'")
        __path_parts: t.Dict[str, str] = {"connector_id": _quote(connector_id)}
        __path = f'/_connector/{__path_parts["connector_id"]}/_configuration'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if configuration is not None:
                __body["configuration"] = configuration
            if values is not None:
                __body["values"] = values
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="connector.update_configuration",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("error",),
    )
    @_stability_warning(Stability.EXPERIMENTAL)
    async def update_error(
        self,
        *,
        connector_id: str,
        error: t.Optional[t.Union[None, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Update the connector error field.</p>
          <p>Set the error field for the connector.
          If the error provided in the request body is non-null, the connector’s status is updated to error.
          Otherwise, if the error is reset to null, the connector status is updated to connected.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-update-error>`_

        :param connector_id: The unique identifier of the connector to be updated
        :param error:
        """
        if connector_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'connector_id'")
        if error is None and body is None:
            raise ValueError("Empty value passed for parameter 'error'")
        __path_parts: t.Dict[str, str] = {"connector_id": _quote(connector_id)}
        __path = f'/_connector/{__path_parts["connector_id"]}/_error'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if error is not None:
                __body["error"] = error
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="connector.update_error",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("features",),
    )
    @_stability_warning(Stability.EXPERIMENTAL)
    async def update_features(
        self,
        *,
        connector_id: str,
        features: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Update the connector features.
          Update the connector features in the connector document.
          This API can be used to control the following aspects of a connector:</p>
          <ul>
          <li>document-level security</li>
          <li>incremental syncs</li>
          <li>advanced sync rules</li>
          <li>basic sync rules</li>
          </ul>
          <p>Normally, the running connector service automatically manages these features.
          However, you can use this API to override the default behavior.</p>
          <p>To sync data using self-managed connectors, you need to deploy the Elastic connector service on your own infrastructure.
          This service runs automatically on Elastic Cloud for Elastic managed connectors.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-update-features>`_

        :param connector_id: The unique identifier of the connector to be updated.
        :param features:
        """
        if connector_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'connector_id'")
        if features is None and body is None:
            raise ValueError("Empty value passed for parameter 'features'")
        __path_parts: t.Dict[str, str] = {"connector_id": _quote(connector_id)}
        __path = f'/_connector/{__path_parts["connector_id"]}/_features'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if features is not None:
                __body["features"] = features
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="connector.update_features",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("advanced_snippet", "filtering", "rules"),
    )
    @_stability_warning(Stability.BETA)
    async def update_filtering(
        self,
        *,
        connector_id: str,
        advanced_snippet: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        filtering: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        rules: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Update the connector filtering.</p>
          <p>Update the draft filtering configuration of a connector and marks the draft validation state as edited.
          The filtering draft is activated once validated by the running Elastic connector service.
          The filtering property is used to configure sync rules (both basic and advanced) for a connector.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-update-filtering>`_

        :param connector_id: The unique identifier of the connector to be updated
        :param advanced_snippet:
        :param filtering:
        :param rules:
        """
        if connector_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'connector_id'")
        __path_parts: t.Dict[str, str] = {"connector_id": _quote(connector_id)}
        __path = f'/_connector/{__path_parts["connector_id"]}/_filtering'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if advanced_snippet is not None:
                __body["advanced_snippet"] = advanced_snippet
            if filtering is not None:
                __body["filtering"] = filtering
            if rules is not None:
                __body["rules"] = rules
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="connector.update_filtering",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("validation",),
    )
    @_stability_warning(Stability.EXPERIMENTAL)
    async def update_filtering_validation(
        self,
        *,
        connector_id: str,
        validation: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Update the connector draft filtering validation.</p>
          <p>Update the draft filtering validation info for a connector.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-update-filtering-validation>`_

        :param connector_id: The unique identifier of the connector to be updated
        :param validation:
        """
        if connector_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'connector_id'")
        if validation is None and body is None:
            raise ValueError("Empty value passed for parameter 'validation'")
        __path_parts: t.Dict[str, str] = {"connector_id": _quote(connector_id)}
        __path = f'/_connector/{__path_parts["connector_id"]}/_filtering/_validation'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if validation is not None:
                __body["validation"] = validation
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="connector.update_filtering_validation",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("index_name",),
    )
    @_stability_warning(Stability.BETA)
    async def update_index_name(
        self,
        *,
        connector_id: str,
        index_name: t.Optional[t.Union[None, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Update the connector index name.</p>
          <p>Update the <code>index_name</code> field of a connector, specifying the index where the data ingested by the connector is stored.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-update-index-name>`_

        :param connector_id: The unique identifier of the connector to be updated
        :param index_name:
        """
        if connector_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'connector_id'")
        if index_name is None and body is None:
            raise ValueError("Empty value passed for parameter 'index_name'")
        __path_parts: t.Dict[str, str] = {"connector_id": _quote(connector_id)}
        __path = f'/_connector/{__path_parts["connector_id"]}/_index_name'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if index_name is not None:
                __body["index_name"] = index_name
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="connector.update_index_name",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("description", "name"),
    )
    @_stability_warning(Stability.BETA)
    async def update_name(
        self,
        *,
        connector_id: str,
        description: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        name: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Update the connector name and description.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-update-name>`_

        :param connector_id: The unique identifier of the connector to be updated
        :param description:
        :param name:
        """
        if connector_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'connector_id'")
        __path_parts: t.Dict[str, str] = {"connector_id": _quote(connector_id)}
        __path = f'/_connector/{__path_parts["connector_id"]}/_name'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if description is not None:
                __body["description"] = description
            if name is not None:
                __body["name"] = name
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="connector.update_name",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("is_native",),
    )
    @_stability_warning(Stability.BETA)
    async def update_native(
        self,
        *,
        connector_id: str,
        is_native: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Update the connector is_native flag.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-update-native>`_

        :param connector_id: The unique identifier of the connector to be updated
        :param is_native:
        """
        if connector_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'connector_id'")
        if is_native is None and body is None:
            raise ValueError("Empty value passed for parameter 'is_native'")
        __path_parts: t.Dict[str, str] = {"connector_id": _quote(connector_id)}
        __path = f'/_connector/{__path_parts["connector_id"]}/_native'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if is_native is not None:
                __body["is_native"] = is_native
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="connector.update_native",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("pipeline",),
    )
    @_stability_warning(Stability.BETA)
    async def update_pipeline(
        self,
        *,
        connector_id: str,
        pipeline: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Update the connector pipeline.</p>
          <p>When you create a new connector, the configuration of an ingest pipeline is populated with default settings.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-update-pipeline>`_

        :param connector_id: The unique identifier of the connector to be updated
        :param pipeline:
        """
        if connector_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'connector_id'")
        if pipeline is None and body is None:
            raise ValueError("Empty value passed for parameter 'pipeline'")
        __path_parts: t.Dict[str, str] = {"connector_id": _quote(connector_id)}
        __path = f'/_connector/{__path_parts["connector_id"]}/_pipeline'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if pipeline is not None:
                __body["pipeline"] = pipeline
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="connector.update_pipeline",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("scheduling",),
    )
    @_stability_warning(Stability.BETA)
    async def update_scheduling(
        self,
        *,
        connector_id: str,
        scheduling: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Update the connector scheduling.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-update-scheduling>`_

        :param connector_id: The unique identifier of the connector to be updated
        :param scheduling:
        """
        if connector_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'connector_id'")
        if scheduling is None and body is None:
            raise ValueError("Empty value passed for parameter 'scheduling'")
        __path_parts: t.Dict[str, str] = {"connector_id": _quote(connector_id)}
        __path = f'/_connector/{__path_parts["connector_id"]}/_scheduling'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if scheduling is not None:
                __body["scheduling"] = scheduling
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="connector.update_scheduling",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("service_type",),
    )
    @_stability_warning(Stability.BETA)
    async def update_service_type(
        self,
        *,
        connector_id: str,
        service_type: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Update the connector service type.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-update-service-type>`_

        :param connector_id: The unique identifier of the connector to be updated
        :param service_type:
        """
        if connector_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'connector_id'")
        if service_type is None and body is None:
            raise ValueError("Empty value passed for parameter 'service_type'")
        __path_parts: t.Dict[str, str] = {"connector_id": _quote(connector_id)}
        __path = f'/_connector/{__path_parts["connector_id"]}/_service_type'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if service_type is not None:
                __body["service_type"] = service_type
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="connector.update_service_type",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("status",),
    )
    @_stability_warning(Stability.EXPERIMENTAL)
    async def update_status(
        self,
        *,
        connector_id: str,
        status: t.Optional[
            t.Union[
                str,
                t.Literal[
                    "configured", "connected", "created", "error", "needs_configuration"
                ],
            ]
        ] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Update the connector status.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-connector-update-status>`_

        :param connector_id: The unique identifier of the connector to be updated
        :param status:
        """
        if connector_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'connector_id'")
        if status is None and body is None:
            raise ValueError("Empty value passed for parameter 'status'")
        __path_parts: t.Dict[str, str] = {"connector_id": _quote(connector_id)}
        __path = f'/_connector/{__path_parts["connector_id"]}/_status'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if status is not None:
                __body["status"] = status
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="connector.update_status",
            path_parts=__path_parts,
        )
