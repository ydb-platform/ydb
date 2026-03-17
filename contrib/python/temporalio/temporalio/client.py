"""Client for accessing Temporal."""

from __future__ import annotations

import abc
import asyncio
import copy
import dataclasses
import inspect
import json
import re
import uuid
import warnings
from abc import ABC, abstractmethod
from asyncio import Future
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum, IntEnum
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    FrozenSet,
    Generic,
    Iterable,
    Mapping,
    Optional,
    Sequence,
    Text,
    Tuple,
    Type,
    Union,
    cast,
    overload,
)

import google.protobuf.duration_pb2
import google.protobuf.json_format
import google.protobuf.timestamp_pb2
from google.protobuf.internal.containers import MessageMap
from typing_extensions import Concatenate, Required, TypedDict

import temporalio.api.common.v1
import temporalio.api.enums.v1
import temporalio.api.errordetails.v1
import temporalio.api.failure.v1
import temporalio.api.history.v1
import temporalio.api.schedule.v1
import temporalio.api.sdk.v1
import temporalio.api.taskqueue.v1
import temporalio.api.update.v1
import temporalio.api.workflow.v1
import temporalio.api.workflowservice.v1
import temporalio.common
import temporalio.converter
import temporalio.exceptions
import temporalio.nexus
import temporalio.runtime
import temporalio.service
import temporalio.workflow
from temporalio.activity import ActivityCancellationDetails
from temporalio.service import (
    HttpConnectProxyConfig,
    KeepAliveConfig,
    RetryConfig,
    RPCError,
    RPCStatusCode,
    TLSConfig,
)

from .common import HeaderCodecBehavior
from .types import (
    AnyType,
    LocalReturnType,
    MethodAsyncNoParam,
    MethodAsyncSingleParam,
    MethodSyncOrAsyncNoParam,
    MethodSyncOrAsyncSingleParam,
    MultiParamSpec,
    ParamType,
    ReturnType,
    SelfType,
)


class Client:
    """Client for accessing Temporal.

    Most users will use :py:meth:`connect` to create a client. The
    :py:attr:`service` property provides access to a raw gRPC client. To create
    another client, like for a different namespace, :py:func:`Client` may be
    directly instantiated with a :py:attr:`service` of another.

    Clients are not thread-safe and should only be used in the event loop they
    are first connected in. If a client needs to be used from another thread
    than where it was created, make sure the event loop where it was created is
    captured, and then call :py:func:`asyncio.run_coroutine_threadsafe` with the
    client call and that event loop.

    Clients do not work across forks since runtimes do not work across forks.
    """

    @staticmethod
    async def connect(
        target_host: str,
        *,
        namespace: str = "default",
        api_key: Optional[str] = None,
        data_converter: temporalio.converter.DataConverter = temporalio.converter.DataConverter.default,
        plugins: Sequence[Plugin] = [],
        interceptors: Sequence[Interceptor] = [],
        default_workflow_query_reject_condition: Optional[
            temporalio.common.QueryRejectCondition
        ] = None,
        tls: Union[bool, TLSConfig] = False,
        retry_config: Optional[RetryConfig] = None,
        keep_alive_config: Optional[KeepAliveConfig] = KeepAliveConfig.default,
        rpc_metadata: Mapping[str, str] = {},
        identity: Optional[str] = None,
        lazy: bool = False,
        runtime: Optional[temporalio.runtime.Runtime] = None,
        http_connect_proxy_config: Optional[HttpConnectProxyConfig] = None,
        header_codec_behavior: HeaderCodecBehavior = HeaderCodecBehavior.NO_CODEC,
    ) -> Client:
        """Connect to a Temporal server.

        Args:
            target_host: ``host:port`` for the Temporal server. For local
                development, this is often "localhost:7233".
            namespace: Namespace to use for client calls.
            api_key: API key for Temporal. This becomes the "Authorization"
                HTTP header with "Bearer " prepended. This is only set if RPC
                metadata doesn't already have an "authorization" key.
            data_converter: Data converter to use for all data conversions
                to/from payloads.
            plugins: Set of plugins that are chained together to allow
                intercepting and modifying client creation and service connection.
                The earlier plugins wrap the later ones.

                Any plugins that also implement
                :py:class:`temporalio.worker.Plugin` will be used as worker
                plugins too so they should not be given when creating a
                worker.
            interceptors: Set of interceptors that are chained together to allow
                intercepting of client calls. The earlier interceptors wrap the
                later ones.

                Any interceptors that also implement
                :py:class:`temporalio.worker.Interceptor` will be used as worker
                interceptors too so they should not be given when creating a
                worker.
            default_workflow_query_reject_condition: The default rejection
                condition for workflow queries if not set during query. See
                :py:meth:`WorkflowHandle.query` for details on the rejection
                condition.
            tls: If false, the default, do not use TLS. If true, use system
                default TLS configuration. If TLS configuration present, that
                TLS configuration will be used.
            retry_config: Retry configuration for direct service calls (when
                opted in) or all high-level calls made by this client (which all
                opt-in to retries by default). If unset, a default retry
                configuration is used.
            keep_alive_config: Keep-alive configuration for the client
                connection. Default is to check every 30s and kill the
                connection if a response doesn't come back in 15s. Can be set to
                ``None`` to disable.
            rpc_metadata: Headers to use for all calls to the server. Keys here
                can be overriden by per-call RPC metadata keys.
            identity: Identity for this client. If unset, a default is created
                based on the version of the SDK.
            lazy: If true, the client will not connect until the first call is
                attempted or a worker is created with it. Lazy clients cannot be
                used for workers.
            runtime: The runtime for this client, or the default if unset.
            http_connect_proxy_config: Configuration for HTTP CONNECT proxy.
            header_codec_behavior: Encoding behavior for headers sent by the client.
        """
        connect_config = temporalio.service.ConnectConfig(
            target_host=target_host,
            api_key=api_key,
            tls=tls,
            retry_config=retry_config,
            keep_alive_config=keep_alive_config,
            rpc_metadata=rpc_metadata,
            identity=identity or "",
            lazy=lazy,
            runtime=runtime,
            http_connect_proxy_config=http_connect_proxy_config,
        )

        root_plugin: Plugin = _RootPlugin()
        for plugin in reversed(plugins):
            plugin.init_client_plugin(root_plugin)
            root_plugin = plugin

        service_client = await root_plugin.connect_service_client(connect_config)

        return Client(
            service_client,
            namespace=namespace,
            data_converter=data_converter,
            interceptors=interceptors,
            default_workflow_query_reject_condition=default_workflow_query_reject_condition,
            header_codec_behavior=header_codec_behavior,
            plugins=plugins,
        )

    def __init__(
        self,
        service_client: temporalio.service.ServiceClient,
        *,
        namespace: str = "default",
        data_converter: temporalio.converter.DataConverter = temporalio.converter.DataConverter.default,
        plugins: Sequence[Plugin] = [],
        interceptors: Sequence[Interceptor] = [],
        default_workflow_query_reject_condition: Optional[
            temporalio.common.QueryRejectCondition
        ] = None,
        header_codec_behavior: HeaderCodecBehavior = HeaderCodecBehavior.NO_CODEC,
    ):
        """Create a Temporal client from a service client.

        See :py:meth:`connect` for details on the parameters.
        """
        # Store the config for tracking
        config = ClientConfig(
            service_client=service_client,
            namespace=namespace,
            data_converter=data_converter,
            interceptors=interceptors,
            default_workflow_query_reject_condition=default_workflow_query_reject_condition,
            header_codec_behavior=header_codec_behavior,
            plugins=plugins,
        )

        root_plugin: Plugin = _RootPlugin()
        for plugin in reversed(plugins):
            plugin.init_client_plugin(root_plugin)
            root_plugin = plugin

        self._init_from_config(root_plugin.configure_client(config))

    def _init_from_config(self, config: ClientConfig):
        self._config = config

        # Iterate over interceptors in reverse building the impl
        self._impl: OutboundInterceptor = _ClientImpl(self)
        for interceptor in reversed(list(self._config["interceptors"])):
            self._impl = interceptor.intercept_client(self._impl)

    def config(self) -> ClientConfig:
        """Config, as a dictionary, used to create this client.

        This makes a shallow copy of the config each call.
        """
        config = self._config.copy()
        config["interceptors"] = list(config["interceptors"])
        return config

    @property
    def service_client(self) -> temporalio.service.ServiceClient:
        """Raw gRPC service client."""
        return self._config["service_client"]

    @property
    def workflow_service(self) -> temporalio.service.WorkflowService:
        """Raw gRPC workflow service client."""
        return self._config["service_client"].workflow_service

    @property
    def operator_service(self) -> temporalio.service.OperatorService:
        """Raw gRPC operator service client."""
        return self._config["service_client"].operator_service

    @property
    def test_service(self) -> temporalio.service.TestService:
        """Raw gRPC test service client."""
        return self._config["service_client"].test_service

    @property
    def namespace(self) -> str:
        """Namespace used in calls by this client."""
        return self._config["namespace"]

    @property
    def identity(self) -> str:
        """Identity used in calls by this client."""
        return self._config["service_client"].config.identity

    @property
    def data_converter(self) -> temporalio.converter.DataConverter:
        """Data converter used by this client."""
        return self._config["data_converter"]

    @property
    def rpc_metadata(self) -> Mapping[str, str]:
        """Headers for every call made by this client.

        Do not use mutate this mapping. Rather, set this property with an
        entirely new mapping to change the headers.
        """
        return self.service_client.config.rpc_metadata

    @rpc_metadata.setter
    def rpc_metadata(self, value: Mapping[str, str]) -> None:
        """Update the headers for this client.

        Do not mutate this mapping after set. Rather, set an entirely new
        mapping if changes are needed.
        """
        # Update config and perform update
        self.service_client.config.rpc_metadata = value
        self.service_client.update_rpc_metadata(value)

    @property
    def api_key(self) -> Optional[str]:
        """API key for every call made by this client."""
        return self.service_client.config.api_key

    @api_key.setter
    def api_key(self, value: Optional[str]) -> None:
        """Update the API key for this client.

        This is only set if RPCmetadata doesn't already have an "authorization"
        key.
        """
        # Update config and perform update
        self.service_client.config.api_key = value
        self.service_client.update_api_key(value)

    # Overload for no-param workflow
    @overload
    async def start_workflow(
        self,
        workflow: MethodAsyncNoParam[SelfType, ReturnType],
        *,
        id: str,
        task_queue: str,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy = temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: Optional[temporalio.common.RetryPolicy] = None,
        cron_schedule: str = "",
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[
            Union[
                temporalio.common.TypedSearchAttributes,
                temporalio.common.SearchAttributes,
            ]
        ] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        start_delay: Optional[timedelta] = None,
        start_signal: Optional[str] = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
        request_eager_start: bool = False,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: Optional[temporalio.common.VersioningOverride] = None,
    ) -> WorkflowHandle[SelfType, ReturnType]: ...

    # Overload for single-param workflow
    @overload
    async def start_workflow(
        self,
        workflow: MethodAsyncSingleParam[SelfType, ParamType, ReturnType],
        arg: ParamType,
        *,
        id: str,
        task_queue: str,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy = temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: Optional[temporalio.common.RetryPolicy] = None,
        cron_schedule: str = "",
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[
            Union[
                temporalio.common.TypedSearchAttributes,
                temporalio.common.SearchAttributes,
            ]
        ] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        start_delay: Optional[timedelta] = None,
        start_signal: Optional[str] = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
        request_eager_start: bool = False,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: Optional[temporalio.common.VersioningOverride] = None,
    ) -> WorkflowHandle[SelfType, ReturnType]: ...

    # Overload for multi-param workflow
    @overload
    async def start_workflow(
        self,
        workflow: Callable[
            Concatenate[SelfType, MultiParamSpec], Awaitable[ReturnType]
        ],
        *,
        args: Sequence[Any],
        id: str,
        task_queue: str,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy = temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: Optional[temporalio.common.RetryPolicy] = None,
        cron_schedule: str = "",
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[
            Union[
                temporalio.common.TypedSearchAttributes,
                temporalio.common.SearchAttributes,
            ]
        ] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        start_delay: Optional[timedelta] = None,
        start_signal: Optional[str] = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
        request_eager_start: bool = False,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: Optional[temporalio.common.VersioningOverride] = None,
    ) -> WorkflowHandle[SelfType, ReturnType]: ...

    # Overload for string-name workflow
    @overload
    async def start_workflow(
        self,
        workflow: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: str,
        result_type: Optional[Type] = None,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy = temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: Optional[temporalio.common.RetryPolicy] = None,
        cron_schedule: str = "",
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[
            Union[
                temporalio.common.TypedSearchAttributes,
                temporalio.common.SearchAttributes,
            ]
        ] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        start_delay: Optional[timedelta] = None,
        start_signal: Optional[str] = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
        request_eager_start: bool = False,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: Optional[temporalio.common.VersioningOverride] = None,
    ) -> WorkflowHandle[Any, Any]: ...

    async def start_workflow(
        self,
        workflow: Union[str, Callable[..., Awaitable[Any]]],
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: str,
        result_type: Optional[Type] = None,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy = temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: Optional[temporalio.common.RetryPolicy] = None,
        cron_schedule: str = "",
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[
            Union[
                temporalio.common.TypedSearchAttributes,
                temporalio.common.SearchAttributes,
            ]
        ] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        start_delay: Optional[timedelta] = None,
        start_signal: Optional[str] = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
        request_eager_start: bool = False,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: Optional[temporalio.common.VersioningOverride] = None,
        # The following options should not be considered part of the public API. They
        # are deliberately not exposed in overloads, and are not subject to any
        # backwards compatibility guarantees.
        callbacks: Sequence[Callback] = [],
        workflow_event_links: Sequence[
            temporalio.api.common.v1.Link.WorkflowEvent
        ] = [],
        request_id: Optional[str] = None,
        stack_level: int = 2,
    ) -> WorkflowHandle[Any, Any]:
        """Start a workflow and return its handle.

        Args:
            workflow: String name or class method decorated with
                ``@workflow.run`` for the workflow to start.
            arg: Single argument to the workflow.
            args: Multiple arguments to the workflow. Cannot be set if arg is.
            id: Unique identifier for the workflow execution.
            task_queue: Task queue to run the workflow on.
            result_type: For string workflows, this can set the specific result
                type hint to deserialize into.
            execution_timeout: Total workflow execution timeout including
                retries and continue as new.
            run_timeout: Timeout of a single workflow run.
            task_timeout: Timeout of a single workflow task.
            id_conflict_policy: Behavior when a workflow is currently running with the same ID.
                Default is UNSPECIFIED, which effectively means fail the start attempt.
                Set to USE_EXISTING for idempotent deduplication on workflow ID.
                Cannot be set if ``id_reuse_policy`` is set to TERMINATE_IF_RUNNING.
            id_reuse_policy: Behavior when a closed workflow with the same ID exists.
                Default is ALLOW_DUPLICATE.
            retry_policy: Retry policy for the workflow.
            cron_schedule: See https://docs.temporal.io/docs/content/what-is-a-temporal-cron-job/
            memo: Memo for the workflow.
            search_attributes: Search attributes for the workflow. The
                dictionary form of this is deprecated, use
                :py:class:`temporalio.common.TypedSearchAttributes`.
            static_summary: A single-line fixed summary for this workflow execution that may appear
                in the UI/CLI. This can be in single-line Temporal markdown format.
            static_details: General fixed details for this workflow execution that may appear in
                UI/CLI. This can be in Temporal markdown format and can span multiple lines. This is
                a fixed value on the workflow that cannot be updated. For details that can be
                updated, use :py:meth:`temporalio.workflow.get_current_details` within the workflow.
            start_delay: Amount of time to wait before starting the workflow.
                This does not work with ``cron_schedule``.
            start_signal: If present, this signal is sent as signal-with-start
                instead of traditional workflow start.
            start_signal_args: Arguments for start_signal if start_signal
                present.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.
            request_eager_start: Potentially reduce the latency to start this workflow by
                encouraging the server to start it on a local worker running with
                this same client.
            priority: Priority of the workflow execution.
            versioning_override: Overrides the versioning behavior for this workflow.

        Returns:
            A workflow handle to the started workflow.

        Raises:
            temporalio.exceptions.WorkflowAlreadyStartedError: Workflow has
                already been started.
            RPCError: Workflow could not be started for some other reason.
        """
        temporalio.common._warn_on_deprecated_search_attributes(
            search_attributes, stack_level=stack_level
        )
        name, result_type_from_type_hint = (
            temporalio.workflow._Definition.get_name_and_result_type(workflow)
        )
        return await self._impl.start_workflow(
            StartWorkflowInput(
                workflow=name,
                args=temporalio.common._arg_or_args(arg, args),
                id=id,
                task_queue=task_queue,
                execution_timeout=execution_timeout,
                run_timeout=run_timeout,
                task_timeout=task_timeout,
                id_reuse_policy=id_reuse_policy,
                id_conflict_policy=id_conflict_policy,
                retry_policy=retry_policy,
                cron_schedule=cron_schedule,
                memo=memo,
                search_attributes=search_attributes,
                start_delay=start_delay,
                versioning_override=versioning_override,
                headers={},
                static_summary=static_summary,
                static_details=static_details,
                start_signal=start_signal,
                start_signal_args=start_signal_args,
                ret_type=result_type or result_type_from_type_hint,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
                request_eager_start=request_eager_start,
                priority=priority,
                callbacks=callbacks,
                workflow_event_links=workflow_event_links,
                request_id=request_id,
            )
        )

    # Overload for no-param workflow
    @overload
    async def execute_workflow(
        self,
        workflow: MethodAsyncNoParam[SelfType, ReturnType],
        *,
        id: str,
        task_queue: str,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy = temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: Optional[temporalio.common.RetryPolicy] = None,
        cron_schedule: str = "",
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[
            Union[
                temporalio.common.TypedSearchAttributes,
                temporalio.common.SearchAttributes,
            ]
        ] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        start_delay: Optional[timedelta] = None,
        start_signal: Optional[str] = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
        request_eager_start: bool = False,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: Optional[temporalio.common.VersioningOverride] = None,
    ) -> ReturnType: ...

    # Overload for single-param workflow
    @overload
    async def execute_workflow(
        self,
        workflow: MethodAsyncSingleParam[SelfType, ParamType, ReturnType],
        arg: ParamType,
        *,
        id: str,
        task_queue: str,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy = temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: Optional[temporalio.common.RetryPolicy] = None,
        cron_schedule: str = "",
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[
            Union[
                temporalio.common.TypedSearchAttributes,
                temporalio.common.SearchAttributes,
            ]
        ] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        start_delay: Optional[timedelta] = None,
        start_signal: Optional[str] = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
        request_eager_start: bool = False,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: Optional[temporalio.common.VersioningOverride] = None,
    ) -> ReturnType: ...

    # Overload for multi-param workflow
    @overload
    async def execute_workflow(
        self,
        workflow: Callable[
            Concatenate[SelfType, MultiParamSpec], Awaitable[ReturnType]
        ],
        *,
        args: Sequence[Any],
        id: str,
        task_queue: str,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy = temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: Optional[temporalio.common.RetryPolicy] = None,
        cron_schedule: str = "",
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[
            Union[
                temporalio.common.TypedSearchAttributes,
                temporalio.common.SearchAttributes,
            ]
        ] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        start_delay: Optional[timedelta] = None,
        start_signal: Optional[str] = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
        request_eager_start: bool = False,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: Optional[temporalio.common.VersioningOverride] = None,
    ) -> ReturnType: ...

    # Overload for string-name workflow
    @overload
    async def execute_workflow(
        self,
        workflow: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: str,
        result_type: Optional[Type] = None,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy = temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: Optional[temporalio.common.RetryPolicy] = None,
        cron_schedule: str = "",
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[
            Union[
                temporalio.common.TypedSearchAttributes,
                temporalio.common.SearchAttributes,
            ]
        ] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        start_delay: Optional[timedelta] = None,
        start_signal: Optional[str] = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
        request_eager_start: bool = False,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: Optional[temporalio.common.VersioningOverride] = None,
    ) -> Any: ...

    async def execute_workflow(
        self,
        workflow: Union[str, Callable[..., Awaitable[Any]]],
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: str,
        result_type: Optional[Type] = None,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy = temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: Optional[temporalio.common.RetryPolicy] = None,
        cron_schedule: str = "",
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[
            Union[
                temporalio.common.TypedSearchAttributes,
                temporalio.common.SearchAttributes,
            ]
        ] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        start_delay: Optional[timedelta] = None,
        start_signal: Optional[str] = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
        request_eager_start: bool = False,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: Optional[temporalio.common.VersioningOverride] = None,
    ) -> Any:
        """Start a workflow and wait for completion.

        This is a shortcut for :py:meth:`start_workflow` +
        :py:meth:`WorkflowHandle.result`.
        """
        return await (
            # We have to tell MyPy to ignore errors here because we want to call
            # the non-@overload form of this and MyPy does not support that
            await self.start_workflow(  # type: ignore
                workflow,  # type: ignore[arg-type]
                arg,
                args=args,
                task_queue=task_queue,
                result_type=result_type,
                id=id,
                execution_timeout=execution_timeout,
                run_timeout=run_timeout,
                task_timeout=task_timeout,
                id_reuse_policy=id_reuse_policy,
                id_conflict_policy=id_conflict_policy,
                retry_policy=retry_policy,
                cron_schedule=cron_schedule,
                memo=memo,
                search_attributes=search_attributes,
                static_summary=static_summary,
                static_details=static_details,
                start_delay=start_delay,
                start_signal=start_signal,
                start_signal_args=start_signal_args,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
                request_eager_start=request_eager_start,
                priority=priority,
                versioning_override=versioning_override,
                stack_level=3,
            )
        ).result()

    def get_workflow_handle(
        self,
        workflow_id: str,
        *,
        run_id: Optional[str] = None,
        first_execution_run_id: Optional[str] = None,
        result_type: Optional[Type] = None,
    ) -> WorkflowHandle[Any, Any]:
        """Get a workflow handle to an existing workflow by its ID.

        Args:
            workflow_id: Workflow ID to get a handle to.
            run_id: Run ID that will be used for all calls.
            first_execution_run_id: First execution run ID used for cancellation
                and termination.
            result_type: The result type to deserialize into if known.

        Returns:
            The workflow handle.
        """
        return WorkflowHandle(
            self,
            workflow_id,
            run_id=run_id,
            result_run_id=run_id,
            first_execution_run_id=first_execution_run_id,
            result_type=result_type,
        )

    def get_workflow_handle_for(
        self,
        workflow: Union[
            MethodAsyncNoParam[SelfType, ReturnType],
            MethodAsyncSingleParam[SelfType, Any, ReturnType],
        ],
        workflow_id: str,
        *,
        run_id: Optional[str] = None,
        first_execution_run_id: Optional[str] = None,
    ) -> WorkflowHandle[SelfType, ReturnType]:
        """Get a typed workflow handle to an existing workflow by its ID.

        This is the same as :py:meth:`get_workflow_handle` but typed.

        Args:
            workflow: The workflow run method to use for typing the handle.
            workflow_id: Workflow ID to get a handle to.
            run_id: Run ID that will be used for all calls.
            first_execution_run_id: First execution run ID used for cancellation
                and termination.

        Returns:
            The workflow handle.
        """
        defn = temporalio.workflow._Definition.must_from_run_fn(workflow)
        return self.get_workflow_handle(
            workflow_id,
            run_id=run_id,
            first_execution_run_id=first_execution_run_id,
            result_type=defn.ret_type,
        )

    # Overload for no-param update
    @overload
    async def execute_update_with_start_workflow(
        self,
        update: temporalio.workflow.UpdateMethodMultiParam[[SelfType], LocalReturnType],
        *,
        start_workflow_operation: WithStartWorkflowOperation[SelfType, Any],
        id: Optional[str] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> LocalReturnType: ...

    # Overload for single-param update
    @overload
    async def execute_update_with_start_workflow(
        self,
        update: temporalio.workflow.UpdateMethodMultiParam[
            [SelfType, ParamType], LocalReturnType
        ],
        arg: ParamType,
        *,
        start_workflow_operation: WithStartWorkflowOperation[SelfType, Any],
        id: Optional[str] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> LocalReturnType: ...

    # Overload for multi-param update
    @overload
    async def execute_update_with_start_workflow(
        self,
        update: temporalio.workflow.UpdateMethodMultiParam[
            MultiParamSpec, LocalReturnType
        ],
        *,
        args: MultiParamSpec.args,  # pyright: ignore
        start_workflow_operation: WithStartWorkflowOperation[SelfType, Any],
        id: Optional[str] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> LocalReturnType: ...

    # Overload for string-name update
    @overload
    async def execute_update_with_start_workflow(
        self,
        update: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        start_workflow_operation: WithStartWorkflowOperation[Any, Any],
        args: Sequence[Any] = [],
        id: Optional[str] = None,
        result_type: Optional[Type] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> Any: ...

    async def execute_update_with_start_workflow(
        self,
        update: Union[str, Callable],
        arg: Any = temporalio.common._arg_unset,
        *,
        start_workflow_operation: WithStartWorkflowOperation[Any, Any],
        args: Sequence[Any] = [],
        id: Optional[str] = None,
        result_type: Optional[Type] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> Any:
        """Send an update-with-start request and wait for the update to complete.

        A WorkflowIDConflictPolicy must be set in the start_workflow_operation. If the
        specified workflow execution is not running, a new workflow execution is started
        and the update is sent in the first workflow task. Alternatively if the specified
        workflow execution is running then, if the WorkflowIDConflictPolicy is
        USE_EXISTING, the update is issued against the specified workflow, and if the
        WorkflowIDConflictPolicy is FAIL, an error is returned. This call will block until
        the update has completed, and return the update result. Note that this means that
        the call will not return successfully until the update has been delivered to a
        worker.

        Args:
            update: Update function or name on the workflow. arg: Single argument to the
                update.
            args: Multiple arguments to the update. Cannot be set if arg is.
            start_workflow_operation: a WithStartWorkflowOperation definining the
                WorkflowIDConflictPolicy and how to start the workflow in the event that a
                workflow is started.
            id: ID of the update. If not set, the default is a new UUID.
            result_type: For string updates, this can set the specific result
                type hint to deserialize into.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.

        Raises:
            WorkflowUpdateFailedError: If the update failed.
            WorkflowUpdateRPCTimeoutOrCancelledError: This update call timed out
                or was cancelled. This doesn't mean the update itself was timed out or
                cancelled.

            RPCError: There was some issue starting the workflow or sending the update to
                the workflow.
        """
        handle = await self._start_update_with_start(
            update,
            arg,
            args=args,
            start_workflow_operation=start_workflow_operation,
            wait_for_stage=WorkflowUpdateStage.COMPLETED,
            id=id,
            result_type=result_type,
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
        )
        return await handle.result()

    # Overload for no-param start update
    @overload
    async def start_update_with_start_workflow(
        self,
        update: temporalio.workflow.UpdateMethodMultiParam[[SelfType], LocalReturnType],
        *,
        start_workflow_operation: WithStartWorkflowOperation[SelfType, Any],
        wait_for_stage: WorkflowUpdateStage,
        id: Optional[str] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> WorkflowUpdateHandle[LocalReturnType]: ...

    # Overload for single-param start update
    @overload
    async def start_update_with_start_workflow(
        self,
        update: temporalio.workflow.UpdateMethodMultiParam[
            [SelfType, ParamType], LocalReturnType
        ],
        arg: ParamType,
        *,
        start_workflow_operation: WithStartWorkflowOperation[SelfType, Any],
        wait_for_stage: WorkflowUpdateStage,
        id: Optional[str] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> WorkflowUpdateHandle[LocalReturnType]: ...

    # Overload for multi-param start update
    @overload
    async def start_update_with_start_workflow(
        self,
        update: temporalio.workflow.UpdateMethodMultiParam[
            MultiParamSpec, LocalReturnType
        ],
        *,
        args: MultiParamSpec.args,  # pyright: ignore
        start_workflow_operation: WithStartWorkflowOperation[SelfType, Any],
        wait_for_stage: WorkflowUpdateStage,
        id: Optional[str] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> WorkflowUpdateHandle[LocalReturnType]: ...

    # Overload for string-name start update
    @overload
    async def start_update_with_start_workflow(
        self,
        update: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        start_workflow_operation: WithStartWorkflowOperation[Any, Any],
        wait_for_stage: WorkflowUpdateStage,
        args: Sequence[Any] = [],
        id: Optional[str] = None,
        result_type: Optional[Type] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> WorkflowUpdateHandle[Any]: ...

    async def start_update_with_start_workflow(
        self,
        update: Union[str, Callable],
        arg: Any = temporalio.common._arg_unset,
        *,
        start_workflow_operation: WithStartWorkflowOperation[Any, Any],
        wait_for_stage: WorkflowUpdateStage,
        args: Sequence[Any] = [],
        id: Optional[str] = None,
        result_type: Optional[Type] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> WorkflowUpdateHandle[Any]:
        """Send an update-with-start request and wait for it to be accepted.

        A WorkflowIDConflictPolicy must be set in the start_workflow_operation. If the
        specified workflow execution is not running, a new workflow execution is started
        and the update is sent in the first workflow task. Alternatively if the specified
        workflow execution is running then, if the WorkflowIDConflictPolicy is
        USE_EXISTING, the update is issued against the specified workflow, and if the
        WorkflowIDConflictPolicy is FAIL, an error is returned. This call will block until
        the update has been accepted, and return a WorkflowUpdateHandle. Note that this
        means that the call will not return successfully until the update has been
        delivered to a worker.

        .. warning::
           This API is experimental

        Args:
            update: Update function or name on the workflow. arg: Single argument to the
                update.
            args: Multiple arguments to the update. Cannot be set if arg is.
            start_workflow_operation: a WithStartWorkflowOperation definining the
                WorkflowIDConflictPolicy and how to start the workflow in the event that a
                workflow is started.
            wait_for_stage: Required stage to wait until returning: either ACCEPTED or
                COMPLETED. ADMITTED is not currently supported. See
                https://docs.temporal.io/workflows#update for more details.
            id: ID of the update. If not set, the default is a new UUID.
            result_type: For string updates, this can set the specific result
                type hint to deserialize into.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.

        Raises:
            WorkflowUpdateFailedError: If the update failed.
            WorkflowUpdateRPCTimeoutOrCancelledError: This update call timed out
                or was cancelled. This doesn't mean the update itself was timed out or
                cancelled.

            RPCError: There was some issue starting the workflow or sending the update to
                the workflow.
        """
        return await self._start_update_with_start(
            update,
            arg,
            wait_for_stage=wait_for_stage,
            args=args,
            id=id,
            result_type=result_type,
            start_workflow_operation=start_workflow_operation,
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
        )

    async def _start_update_with_start(
        self,
        update: Union[str, Callable],
        arg: Any = temporalio.common._arg_unset,
        *,
        wait_for_stage: WorkflowUpdateStage,
        args: Sequence[Any] = [],
        id: Optional[str] = None,
        result_type: Optional[Type] = None,
        start_workflow_operation: WithStartWorkflowOperation[SelfType, ReturnType],
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> WorkflowUpdateHandle[Any]:
        if wait_for_stage == WorkflowUpdateStage.ADMITTED:
            raise ValueError("ADMITTED wait stage not supported")

        if start_workflow_operation._used:
            raise RuntimeError("WithStartWorkflowOperation cannot be reused")
        start_workflow_operation._used = True

        update_name, result_type_from_type_hint = (
            temporalio.workflow._UpdateDefinition.get_name_and_result_type(update)
        )

        update_input = UpdateWithStartUpdateWorkflowInput(
            update_id=id,
            update=update_name,
            args=temporalio.common._arg_or_args(arg, args),
            headers={},
            ret_type=result_type or result_type_from_type_hint,
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
            wait_for_stage=wait_for_stage,
        )

        def on_start(
            start_response: temporalio.api.workflowservice.v1.StartWorkflowExecutionResponse,
        ):
            start_workflow_operation._workflow_handle.set_result(
                WorkflowHandle(
                    self,
                    start_workflow_operation._start_workflow_input.id,
                    first_execution_run_id=start_response.run_id,
                    result_run_id=start_response.run_id,
                    result_type=start_workflow_operation._start_workflow_input.ret_type,
                )
            )

        def on_start_error(
            error: BaseException,
        ):
            start_workflow_operation._workflow_handle.set_exception(error)

        input = StartWorkflowUpdateWithStartInput(
            start_workflow_input=start_workflow_operation._start_workflow_input,
            update_workflow_input=update_input,
            _on_start=on_start,
            _on_start_error=on_start_error,
        )

        return await self._impl.start_update_with_start_workflow(input)

    def list_workflows(
        self,
        query: Optional[str] = None,
        *,
        limit: Optional[int] = None,
        page_size: int = 1000,
        next_page_token: Optional[bytes] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> WorkflowExecutionAsyncIterator:
        """List workflows.

        This does not make a request until the first iteration is attempted.
        Therefore any errors will not occur until then.

        Args:
            query: A Temporal visibility list filter. See Temporal documentation
                concerning visibility list filters including behavior when left
                unset.
            limit: Maximum number of workflows to return. If unset, all
                workflows are returned. Only applies if using the
                returned :py:class:`WorkflowExecutionAsyncIterator`.
                as an async iterator.
            page_size: Maximum number of results for each page.
            next_page_token: A previously obtained next page token if doing
                pagination. Usually not needed as the iterator automatically
                starts from the beginning.
            rpc_metadata: Headers used on each RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for each RPC call.

        Returns:
            An async iterator that can be used with ``async for``.
        """
        return self._impl.list_workflows(
            ListWorkflowsInput(
                query=query,
                page_size=page_size,
                next_page_token=next_page_token,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
                limit=limit,
            )
        )

    async def count_workflows(
        self,
        query: Optional[str] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> WorkflowExecutionCount:
        """Count workflows.

        Args:
            query: A Temporal visibility filter. See Temporal documentation
                concerning visibility list filters.
            rpc_metadata: Headers used on each RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for each RPC call.

        Returns:
            Count of workflows.
        """
        return await self._impl.count_workflows(
            CountWorkflowsInput(
                query=query, rpc_metadata=rpc_metadata, rpc_timeout=rpc_timeout
            )
        )

    @overload
    def get_async_activity_handle(
        self, *, workflow_id: str, run_id: Optional[str], activity_id: str
    ) -> AsyncActivityHandle:
        pass

    @overload
    def get_async_activity_handle(self, *, task_token: bytes) -> AsyncActivityHandle:
        pass

    def get_async_activity_handle(
        self,
        *,
        workflow_id: Optional[str] = None,
        run_id: Optional[str] = None,
        activity_id: Optional[str] = None,
        task_token: Optional[bytes] = None,
    ) -> AsyncActivityHandle:
        """Get an async activity handle.

        Either the workflow_id, run_id, and activity_id can be provided, or a
        singular task_token can be provided.

        Args:
            workflow_id: Workflow ID for the activity. Cannot be set if
                task_token is set.
            run_id: Run ID for the activity. Cannot be set if task_token is set.
            activity_id: ID for the activity. Cannot be set if task_token is
                set.
            task_token: Task token for the activity. Cannot be set if any of the
                id parameters are set.

        Returns:
            A handle that can be used for completion or heartbeat.
        """
        if task_token is not None:
            if workflow_id is not None or run_id is not None or activity_id is not None:
                raise ValueError("Task token cannot be present with other IDs")
            return AsyncActivityHandle(self, task_token)
        elif workflow_id is not None:
            if activity_id is None:
                raise ValueError(
                    "Workflow ID, run ID, and activity ID must all be given together"
                )
            return AsyncActivityHandle(
                self,
                AsyncActivityIDReference(
                    workflow_id=workflow_id, run_id=run_id, activity_id=activity_id
                ),
            )
        raise ValueError("Task token or workflow/run/activity ID must be present")

    async def create_schedule(
        self,
        id: str,
        schedule: Schedule,
        *,
        trigger_immediately: bool = False,
        backfill: Sequence[ScheduleBackfill] = [],
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[
            Union[
                temporalio.common.TypedSearchAttributes,
                temporalio.common.SearchAttributes,
            ]
        ] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> ScheduleHandle:
        """Create a schedule and return its handle.

        Args:
            id: Unique identifier of the schedule.
            schedule: Schedule to create.
            trigger_immediately: If true, trigger one action immediately when
                creating the schedule.
            backfill: Set of time periods to take actions on as if that time
                passed right now.
            memo: Memo for the schedule. Memo for a scheduled workflow is part
                of the schedule action.
            search_attributes: Search attributes for the schedule. Search
                attributes for a scheduled workflow are part of the scheduled
                action. The dictionary form of this is DEPRECATED, use
                :py:class:`temporalio.common.TypedSearchAttributes`.
            static_summary: A single-line fixed summary for this workflow execution that may appear
                in the UI/CLI. This can be in single-line Temporal markdown format.
            static_details: General fixed details for this workflow execution that may appear in
                UI/CLI. This can be in Temporal markdown format and can span multiple lines. This is
                a fixed value on the workflow that cannot be updated. For details that can be
                updated, use :py:meth:`temporalio.workflow.get_current_details` within the workflow.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.

        Returns:
            A handle to the created schedule.

        Raises:
            ScheduleAlreadyRunningError: If a schedule with this ID is already
                running.
        """
        temporalio.common._warn_on_deprecated_search_attributes(search_attributes)
        return await self._impl.create_schedule(
            CreateScheduleInput(
                id=id,
                schedule=schedule,
                trigger_immediately=trigger_immediately,
                backfill=backfill,
                memo=memo,
                search_attributes=search_attributes,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    def get_schedule_handle(self, id: str) -> ScheduleHandle:
        """Get a schedule handle for the given ID."""
        return ScheduleHandle(self, id)

    async def list_schedules(
        self,
        query: Optional[str] = None,
        *,
        page_size: int = 1000,
        next_page_token: Optional[bytes] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> ScheduleAsyncIterator:
        """List schedules.

        This does not make a request until the first iteration is attempted.
        Therefore any errors will not occur until then.

        Note, this list is eventually consistent. Therefore if a schedule is
        added or deleted, it may not be available in the list immediately.

        Args:
            page_size: Maximum number of results for each page.
            query: A Temporal visibility list filter. See Temporal documentation
                concerning visibility list filters including behavior when left
                unset.
            next_page_token: A previously obtained next page token if doing
                pagination. Usually not needed as the iterator automatically
                starts from the beginning.
            rpc_metadata: Headers used on each RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for each RPC call.

        Returns:
            An async iterator that can be used with ``async for``.
        """
        return self._impl.list_schedules(
            ListSchedulesInput(
                page_size=page_size,
                next_page_token=next_page_token,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
                query=query,
            )
        )

    async def update_worker_build_id_compatibility(
        self,
        task_queue: str,
        operation: BuildIdOp,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Used to add new Build IDs or otherwise update the relative compatibility of Build Ids as
        defined on a specific task queue for the Worker Versioning feature.

        For more on this feature, see https://docs.temporal.io/workers#worker-versioning

        .. warning::
           This API is experimental

        Args:
            task_queue: The task queue to target.
            operation: The operation to perform.
            rpc_metadata: Headers used on each RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for each RPC call.
        """
        return await self._impl.update_worker_build_id_compatibility(
            UpdateWorkerBuildIdCompatibilityInput(
                task_queue,
                operation,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    async def get_worker_build_id_compatibility(
        self,
        task_queue: str,
        max_sets: Optional[int] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> WorkerBuildIdVersionSets:
        """Get the Build ID compatibility sets for a specific task queue.

        For more on this feature, see https://docs.temporal.io/workers#worker-versioning

        .. warning::
           This API is experimental

        Args:
            task_queue: The task queue to target.
            max_sets: The maximum number of sets to return. If not specified, all sets will be
                returned.
            rpc_metadata: Headers used on each RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for each RPC call.
        """
        return await self._impl.get_worker_build_id_compatibility(
            GetWorkerBuildIdCompatibilityInput(
                task_queue,
                max_sets,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    async def get_worker_task_reachability(
        self,
        build_ids: Sequence[str],
        task_queues: Sequence[str] = [],
        reachability_type: Optional[TaskReachabilityType] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> WorkerTaskReachability:
        """Determine if some Build IDs for certain Task Queues could have tasks dispatched to them.

        For more on this feature, see https://docs.temporal.io/workers#worker-versioning

        .. warning::
           This API is experimental

        Args:
            build_ids: The Build IDs to query the reachability of. At least one must be specified.
            task_queues: Task Queues to restrict the query to. If not specified, all Task Queues
                will be searched. When requesting a large number of task queues or all task queues
                associated with the given Build IDs in a namespace, all Task Queues will be listed
                in the response but some of them may not contain reachability information due to a
                server enforced limit. When reaching the limit, task queues that reachability
                information could not be retrieved for will be marked with a ``NotFetched`` entry in
                {@link BuildIdReachability.taskQueueReachability}. The caller may issue another call
                to get the reachability for those task queues.
            reachability_type: The kind of reachability this request is concerned with.
            rpc_metadata: Headers used on each RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for each RPC call.
        """
        return await self._impl.get_worker_task_reachability(
            GetWorkerTaskReachabilityInput(
                build_ids,
                task_queues,
                reachability_type,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )


class ClientConfig(TypedDict, total=False):
    """TypedDict of config originally passed to :py:meth:`Client`."""

    service_client: Required[temporalio.service.ServiceClient]
    namespace: Required[str]
    data_converter: Required[temporalio.converter.DataConverter]
    interceptors: Required[Sequence[Interceptor]]
    default_workflow_query_reject_condition: Required[
        Optional[temporalio.common.QueryRejectCondition]
    ]
    header_codec_behavior: Required[HeaderCodecBehavior]
    plugins: Required[Sequence[Plugin]]


class WorkflowHistoryEventFilterType(IntEnum):
    """Type of history events to get for a workflow.

    See :py:class:`temporalio.api.enums.v1.HistoryEventFilterType`.
    """

    ALL_EVENT = int(
        temporalio.api.enums.v1.HistoryEventFilterType.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT
    )
    CLOSE_EVENT = int(
        temporalio.api.enums.v1.HistoryEventFilterType.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT
    )


class WorkflowHandle(Generic[SelfType, ReturnType]):
    """Handle for interacting with a workflow.

    This is usually created via :py:meth:`Client.get_workflow_handle` or
    returned from :py:meth:`Client.start_workflow`.
    """

    def __init__(
        self,
        client: Client,
        id: str,
        *,
        run_id: Optional[str] = None,
        result_run_id: Optional[str] = None,
        first_execution_run_id: Optional[str] = None,
        result_type: Optional[Type] = None,
        start_workflow_response: Optional[
            Union[
                temporalio.api.workflowservice.v1.StartWorkflowExecutionResponse,
                temporalio.api.workflowservice.v1.SignalWithStartWorkflowExecutionResponse,
            ]
        ] = None,
    ) -> None:
        """Create workflow handle."""
        self._client = client
        self._id = id
        self._run_id = run_id
        self._result_run_id = result_run_id
        self._first_execution_run_id = first_execution_run_id
        self._result_type = result_type
        self._start_workflow_response = start_workflow_response
        self.__temporal_eagerly_started = False

    @property
    def id(self) -> str:
        """ID for the workflow."""
        return self._id

    @property
    def run_id(self) -> Optional[str]:
        """If present, run ID used to ensure that requested operations apply
        to this exact run.

        This is only created via :py:meth:`Client.get_workflow_handle`.
        :py:meth:`Client.start_workflow` will not set this value.

        This cannot be mutated. If a different run ID is needed,
        :py:meth:`Client.get_workflow_handle` must be used instead.
        """
        return self._run_id

    @property
    def result_run_id(self) -> Optional[str]:
        """Run ID used for :py:meth:`result` calls if present to ensure result
        is for a workflow starting from this run.

        When this handle is created via :py:meth:`Client.get_workflow_handle`,
        this is the same as run_id. When this handle is created via
        :py:meth:`Client.start_workflow`, this value will be the resulting run
        ID.

        This cannot be mutated. If a different run ID is needed,
        :py:meth:`Client.get_workflow_handle` must be used instead.
        """
        return self._result_run_id

    @property
    def first_execution_run_id(self) -> Optional[str]:
        """Run ID used to ensure requested operations apply to a workflow ID
        started with this run ID.

        This can be set when using :py:meth:`Client.get_workflow_handle`. When
        :py:meth:`Client.start_workflow` is called without a start signal, this
        is set to the resulting run.

        This cannot be mutated. If a different first execution run ID is needed,
        :py:meth:`Client.get_workflow_handle` must be used instead.
        """
        return self._first_execution_run_id

    async def result(
        self,
        *,
        follow_runs: bool = True,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> ReturnType:
        """Wait for result of the workflow.

        This will use :py:attr:`result_run_id` if present to base the result on.
        To use another run ID, a new handle must be created via
        :py:meth:`Client.get_workflow_handle`.

        Args:
            follow_runs: If true (default), workflow runs will be continually
                fetched, until the most recent one is found. If false, the first
                result is used.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for each RPC call. Note,
                this is the timeout for each history RPC call not this overall
                function.

        Returns:
            Result of the workflow after being converted by the data converter.

        Raises:
            WorkflowFailureError: Workflow failed, was cancelled, was
                terminated, or timed out. Use the
                :py:attr:`WorkflowFailureError.cause` to see the underlying
                reason.
            Exception: Other possible failures during result fetching.
        """
        # We have to maintain our own run ID because it can change if we follow
        # executions
        hist_run_id = self._result_run_id
        while True:
            async for event in self._fetch_history_events_for_run(
                hist_run_id,
                wait_new_event=True,
                event_filter_type=WorkflowHistoryEventFilterType.CLOSE_EVENT,
                skip_archival=True,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            ):
                if event.HasField("workflow_execution_completed_event_attributes"):
                    complete_attr = event.workflow_execution_completed_event_attributes
                    # Follow execution
                    if follow_runs and complete_attr.new_execution_run_id:
                        hist_run_id = complete_attr.new_execution_run_id
                        break
                    # Ignoring anything after the first response like TypeScript
                    type_hints = [self._result_type] if self._result_type else None
                    results = await self._client.data_converter.decode_wrapper(
                        complete_attr.result,
                        type_hints,
                    )
                    if not results:
                        return cast(ReturnType, None)
                    elif len(results) > 1:
                        warnings.warn(f"Expected single result, got {len(results)}")
                    return cast(ReturnType, results[0])
                elif event.HasField("workflow_execution_failed_event_attributes"):
                    fail_attr = event.workflow_execution_failed_event_attributes
                    # Follow execution
                    if follow_runs and fail_attr.new_execution_run_id:
                        hist_run_id = fail_attr.new_execution_run_id
                        break
                    raise WorkflowFailureError(
                        cause=await self._client.data_converter.decode_failure(
                            fail_attr.failure
                        ),
                    )
                elif event.HasField("workflow_execution_canceled_event_attributes"):
                    cancel_attr = event.workflow_execution_canceled_event_attributes
                    raise WorkflowFailureError(
                        cause=temporalio.exceptions.CancelledError(
                            "Workflow cancelled",
                            *(
                                await self._client.data_converter.decode_wrapper(
                                    cancel_attr.details
                                )
                            ),
                        )
                    )
                elif event.HasField("workflow_execution_terminated_event_attributes"):
                    term_attr = event.workflow_execution_terminated_event_attributes
                    raise WorkflowFailureError(
                        cause=temporalio.exceptions.TerminatedError(
                            term_attr.reason or "Workflow terminated",
                            *(
                                await self._client.data_converter.decode_wrapper(
                                    term_attr.details
                                )
                            ),
                        ),
                    )
                elif event.HasField("workflow_execution_timed_out_event_attributes"):
                    time_attr = event.workflow_execution_timed_out_event_attributes
                    # Follow execution
                    if follow_runs and time_attr.new_execution_run_id:
                        hist_run_id = time_attr.new_execution_run_id
                        break
                    raise WorkflowFailureError(
                        cause=temporalio.exceptions.TimeoutError(
                            "Workflow timed out",
                            type=temporalio.exceptions.TimeoutType.START_TO_CLOSE,
                            last_heartbeat_details=[],
                        ),
                    )
                elif event.HasField(
                    "workflow_execution_continued_as_new_event_attributes"
                ):
                    cont_attr = (
                        event.workflow_execution_continued_as_new_event_attributes
                    )
                    if not cont_attr.new_execution_run_id:
                        raise RuntimeError(
                            "Unexpectedly missing new run ID from continue as new"
                        )
                    # Follow execution
                    if follow_runs:
                        hist_run_id = cont_attr.new_execution_run_id
                        break
                    raise WorkflowContinuedAsNewError(cont_attr.new_execution_run_id)
            # This is reached on break which means that there's a different run
            # ID if we're following. If there's not, it's an error because no
            # event was given (should never happen).
            if hist_run_id is None:
                raise RuntimeError("No completion event found")

    async def cancel(
        self,
        *,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Cancel the workflow.

        This will issue a cancellation for :py:attr:`run_id` if present. This
        call will make sure to use the run chain starting from
        :py:attr:`first_execution_run_id` if present. To create handles with
        these values, use :py:meth:`Client.get_workflow_handle`.

        .. warning::
            Handles created as a result of :py:meth:`Client.start_workflow` with
            a start signal will cancel the latest workflow with the same
            workflow ID even if it is unrelated to the started workflow.

        Args:
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.

        Raises:
            RPCError: Workflow could not be cancelled.
        """
        await self._client._impl.cancel_workflow(
            CancelWorkflowInput(
                id=self._id,
                run_id=self._run_id,
                first_execution_run_id=self._first_execution_run_id,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    async def describe(
        self,
        *,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> WorkflowExecutionDescription:
        """Get workflow details.

        This will get details for :py:attr:`run_id` if present. To use a
        different run ID, create a new handle with via
        :py:meth:`Client.get_workflow_handle`.

        .. warning::
            Handles created as a result of :py:meth:`Client.start_workflow` will
            describe the latest workflow with the same workflow ID even if it is
            unrelated to the started workflow.

        Args:
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.

        Returns:
            Workflow details.

        Raises:
            RPCError: Workflow details could not be fetched.
        """
        return await self._client._impl.describe_workflow(
            DescribeWorkflowInput(
                id=self._id,
                run_id=self._run_id,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    async def fetch_history(
        self,
        *,
        event_filter_type: WorkflowHistoryEventFilterType = WorkflowHistoryEventFilterType.ALL_EVENT,
        skip_archival: bool = False,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> WorkflowHistory:
        """Get workflow history.

        This is a shortcut for :py:meth:`fetch_history_events` that just fetches
        all events.
        """
        return WorkflowHistory(
            workflow_id=self.id,
            events=[
                v
                async for v in self.fetch_history_events(
                    event_filter_type=event_filter_type,
                    skip_archival=skip_archival,
                    rpc_metadata=rpc_metadata,
                    rpc_timeout=rpc_timeout,
                )
            ],
        )

    def fetch_history_events(
        self,
        *,
        page_size: Optional[int] = None,
        next_page_token: Optional[bytes] = None,
        wait_new_event: bool = False,
        event_filter_type: WorkflowHistoryEventFilterType = WorkflowHistoryEventFilterType.ALL_EVENT,
        skip_archival: bool = False,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> WorkflowHistoryEventAsyncIterator:
        """Get workflow history events as an async iterator.

        This does not make a request until the first iteration is attempted.
        Therefore any errors will not occur until then.

        Args:
            page_size: Maximum amount to fetch per request if any maximum.
            next_page_token: A specific page token to fetch.
            wait_new_event: Whether the event fetching request will wait for new
                events or just return right away.
            event_filter_type: Which events to obtain.
            skip_archival: Whether to skip archival.
            rpc_metadata: Headers used on each RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for each RPC call.

        Returns:
            An async iterator that doesn't begin fetching until iterated on.
        """
        return self._fetch_history_events_for_run(
            self._run_id,
            page_size=page_size,
            next_page_token=next_page_token,
            wait_new_event=wait_new_event,
            event_filter_type=event_filter_type,
            skip_archival=skip_archival,
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
        )

    def _fetch_history_events_for_run(
        self,
        run_id: Optional[str],
        *,
        page_size: Optional[int] = None,
        next_page_token: Optional[bytes] = None,
        wait_new_event: bool = False,
        event_filter_type: WorkflowHistoryEventFilterType = WorkflowHistoryEventFilterType.ALL_EVENT,
        skip_archival: bool = False,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> WorkflowHistoryEventAsyncIterator:
        return self._client._impl.fetch_workflow_history_events(
            FetchWorkflowHistoryEventsInput(
                id=self._id,
                run_id=run_id,
                page_size=page_size,
                next_page_token=next_page_token,
                wait_new_event=wait_new_event,
                event_filter_type=event_filter_type,
                skip_archival=skip_archival,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    # Overload for no-param query
    @overload
    async def query(
        self,
        query: MethodSyncOrAsyncNoParam[SelfType, LocalReturnType],
        *,
        reject_condition: Optional[temporalio.common.QueryRejectCondition] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> LocalReturnType: ...

    # Overload for single-param query
    @overload
    async def query(
        self,
        query: MethodSyncOrAsyncSingleParam[SelfType, ParamType, LocalReturnType],
        arg: ParamType,
        *,
        reject_condition: Optional[temporalio.common.QueryRejectCondition] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> LocalReturnType: ...

    # Overload for multi-param query
    @overload
    async def query(
        self,
        query: Callable[
            Concatenate[SelfType, MultiParamSpec],
            Union[Awaitable[LocalReturnType], LocalReturnType],
        ],
        *,
        args: Sequence[Any],
        reject_condition: Optional[temporalio.common.QueryRejectCondition] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> LocalReturnType: ...

    # Overload for string-name query
    @overload
    async def query(
        self,
        query: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        result_type: Optional[Type] = None,
        reject_condition: Optional[temporalio.common.QueryRejectCondition] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> Any: ...

    async def query(
        self,
        query: Union[str, Callable],
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        result_type: Optional[Type] = None,
        reject_condition: Optional[temporalio.common.QueryRejectCondition] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> Any:
        """Query the workflow.

        This will query for :py:attr:`run_id` if present. To use a different
        run ID, create a new handle with via
        :py:meth:`Client.get_workflow_handle`.

        .. warning::
            Handles created as a result of :py:meth:`Client.start_workflow` will
            query the latest workflow with the same workflow ID even if it is
            unrelated to the started workflow.

        Args:
            query: Query function or name on the workflow.
            arg: Single argument to the query.
            args: Multiple arguments to the query. Cannot be set if arg is.
            result_type: For string queries, this can set the specific result
                type hint to deserialize into.
            reject_condition: Condition for rejecting the query. If unset/None,
                defaults to the client's default (which is defaulted to None).
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.

        Returns:
            Result of the query.

        Raises:
            WorkflowQueryRejectedError: A query reject condition was satisfied.
            RPCError: Workflow details could not be fetched.
        """
        query_name: str
        ret_type = result_type
        if callable(query):
            defn = temporalio.workflow._QueryDefinition.from_fn(query)
            if not defn:
                raise RuntimeError(
                    f"Query definition not found on {query.__qualname__}, "
                    "is it decorated with @workflow.query?"
                )
            elif not defn.name:
                raise RuntimeError("Cannot invoke dynamic query definition")
            # TODO(cretz): Check count/type of args at runtime?
            query_name = defn.name
            ret_type = defn.ret_type
        else:
            query_name = str(query)

        return await self._client._impl.query_workflow(
            QueryWorkflowInput(
                id=self._id,
                run_id=self._run_id,
                query=query_name,
                args=temporalio.common._arg_or_args(arg, args),
                reject_condition=reject_condition
                or self._client._config["default_workflow_query_reject_condition"],
                headers={},
                ret_type=ret_type,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    # Overload for no-param signal
    @overload
    async def signal(
        self,
        signal: MethodSyncOrAsyncNoParam[SelfType, None],
        *,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None: ...

    # Overload for single-param signal
    @overload
    async def signal(
        self,
        signal: MethodSyncOrAsyncSingleParam[SelfType, ParamType, None],
        arg: ParamType,
        *,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None: ...

    # Overload for multi-param signal
    @overload
    async def signal(
        self,
        signal: Callable[
            Concatenate[SelfType, MultiParamSpec], Union[Awaitable[None], None]
        ],
        *,
        args: Sequence[Any],
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None: ...

    # Overload for string-name signal
    @overload
    async def signal(
        self,
        signal: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None: ...

    async def signal(
        self,
        signal: Union[str, Callable],
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Send a signal to the workflow.

        This will signal for :py:attr:`run_id` if present. To use a different
        run ID, create a new handle with via
        :py:meth:`Client.get_workflow_handle`.

        .. warning::
            Handles created as a result of :py:meth:`Client.start_workflow` will
            signal the latest workflow with the same workflow ID even if it is
            unrelated to the started workflow.

        Args:
            signal: Signal function or name on the workflow.
            arg: Single argument to the signal.
            args: Multiple arguments to the signal. Cannot be set if arg is.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.

        Raises:
            RPCError: Workflow could not be signalled.
        """
        await self._client._impl.signal_workflow(
            SignalWorkflowInput(
                id=self._id,
                run_id=self._run_id,
                signal=temporalio.workflow._SignalDefinition.must_name_from_fn_or_str(
                    signal
                ),
                args=temporalio.common._arg_or_args(arg, args),
                headers={},
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    async def terminate(
        self,
        *args: Any,
        reason: Optional[str] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Terminate the workflow.

        This will issue a termination for :py:attr:`run_id` if present. This
        call will make sure to use the run chain starting from
        :py:attr:`first_execution_run_id` if present. To create handles with
        these values, use :py:meth:`Client.get_workflow_handle`.

        .. warning::
            Handles created as a result of :py:meth:`Client.start_workflow` with
            a start signal will terminate the latest workflow with the same
            workflow ID even if it is unrelated to the started workflow.

        Args:
            args: Details to store on the termination.
            reason: Reason for the termination.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.

        Raises:
            RPCError: Workflow could not be terminated.
        """
        await self._client._impl.terminate_workflow(
            TerminateWorkflowInput(
                id=self._id,
                run_id=self._run_id,
                args=args,
                reason=reason,
                first_execution_run_id=self._first_execution_run_id,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    # Overload for no-param update
    @overload
    async def execute_update(
        self,
        update: temporalio.workflow.UpdateMethodMultiParam[[SelfType], LocalReturnType],
        *,
        id: Optional[str] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> LocalReturnType: ...

    # Overload for single-param update
    @overload
    async def execute_update(
        self,
        update: temporalio.workflow.UpdateMethodMultiParam[
            [SelfType, ParamType], LocalReturnType
        ],
        arg: ParamType,
        *,
        id: Optional[str] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> LocalReturnType: ...

    # Overload for multi-param update
    @overload
    async def execute_update(
        self,
        update: temporalio.workflow.UpdateMethodMultiParam[
            MultiParamSpec, LocalReturnType
        ],
        *,
        args: MultiParamSpec.args,  # pyright: ignore
        id: Optional[str] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> LocalReturnType: ...

    # Overload for string-name update
    @overload
    async def execute_update(
        self,
        update: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: Optional[str] = None,
        result_type: Optional[Type] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> Any: ...

    async def execute_update(
        self,
        update: Union[str, Callable],
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: Optional[str] = None,
        result_type: Optional[Type] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> Any:
        """Send an update request to the workflow and wait for it to complete.

        This will target the workflow with :py:attr:`run_id` if present. To use a
        different run ID, create a new handle with via :py:meth:`Client.get_workflow_handle`.

        Args:
            update: Update function or name on the workflow.
            arg: Single argument to the update.
            args: Multiple arguments to the update. Cannot be set if arg is.
            id: ID of the update. If not set, the default is a new UUID.
            result_type: For string updates, this can set the specific result
                type hint to deserialize into.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.

        Raises:
            WorkflowUpdateFailedError: If the update failed.
            WorkflowUpdateRPCTimeoutOrCancelledError: This update call timed out
                or was cancelled. This doesn't mean the update itself was timed
                out or cancelled.
            RPCError: There was some issue sending the update to the workflow.
        """
        handle = await self._start_update(
            update,
            arg,
            args=args,
            wait_for_stage=WorkflowUpdateStage.COMPLETED,
            id=id,
            result_type=result_type,
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
        )
        return await handle.result()

    # Overload for no-param start update
    @overload
    async def start_update(
        self,
        update: temporalio.workflow.UpdateMethodMultiParam[[SelfType], LocalReturnType],
        *,
        wait_for_stage: WorkflowUpdateStage,
        id: Optional[str] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> WorkflowUpdateHandle[LocalReturnType]: ...

    # Overload for single-param start update
    @overload
    async def start_update(
        self,
        update: temporalio.workflow.UpdateMethodMultiParam[
            [SelfType, ParamType], LocalReturnType
        ],
        arg: ParamType,
        *,
        wait_for_stage: WorkflowUpdateStage,
        id: Optional[str] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> WorkflowUpdateHandle[LocalReturnType]: ...

    # Overload for multi-param start update
    @overload
    async def start_update(
        self,
        update: temporalio.workflow.UpdateMethodMultiParam[
            MultiParamSpec, LocalReturnType
        ],
        *,
        args: MultiParamSpec.args,  # pyright: ignore
        wait_for_stage: WorkflowUpdateStage,
        id: Optional[str] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> WorkflowUpdateHandle[LocalReturnType]: ...

    # Overload for string-name start update
    @overload
    async def start_update(
        self,
        update: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        wait_for_stage: WorkflowUpdateStage,
        args: Sequence[Any] = [],
        id: Optional[str] = None,
        result_type: Optional[Type] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> WorkflowUpdateHandle[Any]: ...

    async def start_update(
        self,
        update: Union[str, Callable],
        arg: Any = temporalio.common._arg_unset,
        *,
        wait_for_stage: WorkflowUpdateStage,
        args: Sequence[Any] = [],
        id: Optional[str] = None,
        result_type: Optional[Type] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> WorkflowUpdateHandle[Any]:
        """Send an update request to the workflow and return a handle to it.

        This will target the workflow with :py:attr:`run_id` if present. To use a
        different run ID, create a new handle with via :py:meth:`Client.get_workflow_handle`.

        Args:
            update: Update function or name on the workflow. arg: Single argument to the
                update.
            wait_for_stage: Required stage to wait until returning: either ACCEPTED or
                COMPLETED. ADMITTED is not currently supported. See
                https://docs.temporal.io/workflows#update for more details.
            args: Multiple arguments to the update. Cannot be set if arg is.
            id: ID of the update. If not set, the default is a new UUID.
            result_type: For string updates, this can set the specific result
                type hint to deserialize into.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.

        Raises:
            WorkflowUpdateRPCTimeoutOrCancelledError: This update call timed out
                or was cancelled. This doesn't mean the update itself was timed out or
                cancelled.
            RPCError: There was some issue sending the update to the workflow.
        """
        return await self._start_update(
            update,
            arg,
            wait_for_stage=wait_for_stage,
            args=args,
            id=id,
            result_type=result_type,
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
        )

    async def _start_update(
        self,
        update: Union[str, Callable],
        arg: Any = temporalio.common._arg_unset,
        *,
        wait_for_stage: WorkflowUpdateStage,
        args: Sequence[Any] = [],
        id: Optional[str] = None,
        result_type: Optional[Type] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> WorkflowUpdateHandle[Any]:
        if wait_for_stage == WorkflowUpdateStage.ADMITTED:
            raise ValueError("ADMITTED wait stage not supported")

        update_name, result_type_from_type_hint = (
            temporalio.workflow._UpdateDefinition.get_name_and_result_type(update)
        )

        return await self._client._impl.start_workflow_update(
            StartWorkflowUpdateInput(
                id=self._id,
                run_id=self._run_id,
                first_execution_run_id=self.first_execution_run_id,
                update_id=id,
                update=update_name,
                args=temporalio.common._arg_or_args(arg, args),
                headers={},
                ret_type=result_type or result_type_from_type_hint,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
                wait_for_stage=wait_for_stage,
            )
        )

    def get_update_handle(
        self,
        id: str,
        *,
        workflow_run_id: Optional[str] = None,
        result_type: Optional[Type] = None,
    ) -> WorkflowUpdateHandle[Any]:
        """Get a handle for an update. The handle can be used to wait on the
        update result.

        Users may prefer the more typesafe :py:meth:`get_update_handle_for`
        which accepts an update definition.

        Args:
            id: Update ID to get a handle to.
            workflow_run_id: Run ID to tie the handle to. If this is not set,
                the :py:attr:`run_id` will be used.
            result_type: The result type to deserialize into if known.

        Returns:
            The update handle.
        """
        return WorkflowUpdateHandle(
            self._client,
            id,
            self._id,
            workflow_run_id=workflow_run_id or self._run_id,
            result_type=result_type,
        )

    def get_update_handle_for(
        self,
        update: temporalio.workflow.UpdateMethodMultiParam[Any, LocalReturnType],
        id: str,
        *,
        workflow_run_id: Optional[str] = None,
    ) -> WorkflowUpdateHandle[LocalReturnType]:
        """Get a typed handle for an update. The handle can be used to wait on
        the update result.

        This is the same as :py:meth:`get_update_handle` but typed.

        Args:
            update: The update method to use for typing the handle.
            id: Update ID to get a handle to.
            workflow_run_id: Run ID to tie the handle to. If this is not set,
                the :py:attr:`run_id` will be used.

        Returns:
            The update handle.
        """
        return self.get_update_handle(
            id, workflow_run_id=workflow_run_id, result_type=update._defn.ret_type
        )


class WithStartWorkflowOperation(Generic[SelfType, ReturnType]):
    """Defines a start-workflow operation used by update-with-start requests.

    Update-With-Start allows you to send an update to a workflow, while starting the
    workflow if necessary.
    """

    # Overload for no-param workflow, with_start
    @overload
    def __init__(
        self,
        workflow: MethodAsyncNoParam[SelfType, ReturnType],
        *,
        id: str,
        task_queue: str,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        retry_policy: Optional[temporalio.common.RetryPolicy] = None,
        cron_schedule: str = "",
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[
            Union[
                temporalio.common.TypedSearchAttributes,
                temporalio.common.SearchAttributes,
            ]
        ] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        start_delay: Optional[timedelta] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: Optional[temporalio.common.VersioningOverride] = None,
    ) -> None: ...

    # Overload for single-param workflow, with_start
    @overload
    def __init__(
        self,
        workflow: MethodAsyncSingleParam[SelfType, ParamType, ReturnType],
        arg: ParamType,
        *,
        id: str,
        task_queue: str,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        retry_policy: Optional[temporalio.common.RetryPolicy] = None,
        cron_schedule: str = "",
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[
            Union[
                temporalio.common.TypedSearchAttributes,
                temporalio.common.SearchAttributes,
            ]
        ] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        start_delay: Optional[timedelta] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: Optional[temporalio.common.VersioningOverride] = None,
    ) -> None: ...

    # Overload for multi-param workflow, with_start
    @overload
    def __init__(
        self,
        workflow: Callable[
            Concatenate[SelfType, MultiParamSpec], Awaitable[ReturnType]
        ],
        *,
        args: Sequence[Any],
        id: str,
        task_queue: str,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        retry_policy: Optional[temporalio.common.RetryPolicy] = None,
        cron_schedule: str = "",
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[
            Union[
                temporalio.common.TypedSearchAttributes,
                temporalio.common.SearchAttributes,
            ]
        ] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        start_delay: Optional[timedelta] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: Optional[temporalio.common.VersioningOverride] = None,
    ) -> None: ...

    # Overload for string-name workflow, with_start
    @overload
    def __init__(
        self,
        workflow: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: str,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy,
        result_type: Optional[Type] = None,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        retry_policy: Optional[temporalio.common.RetryPolicy] = None,
        cron_schedule: str = "",
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[
            Union[
                temporalio.common.TypedSearchAttributes,
                temporalio.common.SearchAttributes,
            ]
        ] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        start_delay: Optional[timedelta] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: Optional[temporalio.common.VersioningOverride] = None,
    ) -> None: ...

    def __init__(
        self,
        workflow: Union[str, Callable[..., Awaitable[Any]]],
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: str,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy,
        result_type: Optional[Type] = None,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        retry_policy: Optional[temporalio.common.RetryPolicy] = None,
        cron_schedule: str = "",
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[
            Union[
                temporalio.common.TypedSearchAttributes,
                temporalio.common.SearchAttributes,
            ]
        ] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        start_delay: Optional[timedelta] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: Optional[temporalio.common.VersioningOverride] = None,
        stack_level: int = 2,
    ) -> None:
        """Create a WithStartWorkflowOperation.

        See :py:meth:`temporalio.client.Client.start_workflow` for documentation of the
        arguments.
        """
        temporalio.common._warn_on_deprecated_search_attributes(
            search_attributes, stack_level=stack_level
        )
        name, result_type_from_run_fn = (
            temporalio.workflow._Definition.get_name_and_result_type(workflow)
        )
        if id_conflict_policy == temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED:
            raise ValueError("WorkflowIDConflictPolicy is required")

        self._start_workflow_input = UpdateWithStartStartWorkflowInput(
            workflow=name,
            args=temporalio.common._arg_or_args(arg, args),
            id=id,
            task_queue=task_queue,
            execution_timeout=execution_timeout,
            run_timeout=run_timeout,
            task_timeout=task_timeout,
            id_reuse_policy=id_reuse_policy,
            id_conflict_policy=id_conflict_policy,
            retry_policy=retry_policy,
            cron_schedule=cron_schedule,
            memo=memo,
            search_attributes=search_attributes,
            static_summary=static_summary,
            static_details=static_details,
            start_delay=start_delay,
            headers={},
            ret_type=result_type or result_type_from_run_fn,
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
            priority=priority,
            versioning_override=versioning_override,
        )
        self._workflow_handle: Future[WorkflowHandle[SelfType, ReturnType]] = Future()
        self._used = False

    async def workflow_handle(self) -> WorkflowHandle[SelfType, ReturnType]:
        """Wait until workflow is running and return a WorkflowHandle."""
        return await self._workflow_handle


@dataclass(frozen=True)
class AsyncActivityIDReference:
    """Reference to an async activity by its qualified ID."""

    workflow_id: str
    run_id: Optional[str]
    activity_id: str


class AsyncActivityHandle:
    """Handle representing an external activity for completion and heartbeat."""

    def __init__(
        self, client: Client, id_or_token: Union[AsyncActivityIDReference, bytes]
    ) -> None:
        """Create an async activity handle."""
        self._client = client
        self._id_or_token = id_or_token

    async def heartbeat(
        self,
        *details: Any,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Record a heartbeat for the activity.

        Args:
            details: Details of the heartbeat.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.
        """
        await self._client._impl.heartbeat_async_activity(
            HeartbeatAsyncActivityInput(
                id_or_token=self._id_or_token,
                details=details,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            ),
        )

    async def complete(
        self,
        result: Optional[Any] = temporalio.common._arg_unset,
        *,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Complete the activity.

        Args:
            result: Result of the activity if any.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.
        """
        await self._client._impl.complete_async_activity(
            CompleteAsyncActivityInput(
                id_or_token=self._id_or_token,
                result=result,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            ),
        )

    async def fail(
        self,
        error: Exception,
        *,
        last_heartbeat_details: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Fail the activity.

        Args:
            error: Error for the activity.
            last_heartbeat_details: Last heartbeat details for the activity.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.
        """
        await self._client._impl.fail_async_activity(
            FailAsyncActivityInput(
                id_or_token=self._id_or_token,
                error=error,
                last_heartbeat_details=last_heartbeat_details,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            ),
        )

    async def report_cancellation(
        self,
        *details: Any,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Report the activity as cancelled.

        Args:
            details: Cancellation details.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.
        """
        await self._client._impl.report_cancellation_async_activity(
            ReportCancellationAsyncActivityInput(
                id_or_token=self._id_or_token,
                details=details,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            ),
        )


@dataclass
class WorkflowExecution:
    """Info for a single workflow execution run."""

    close_time: Optional[datetime]
    """When the workflow was closed if closed."""

    data_converter: temporalio.converter.DataConverter
    """Data converter from when this description was created."""

    execution_time: Optional[datetime]
    """When this workflow run started or should start."""

    history_length: int
    """Number of events in the history."""

    id: str
    """ID for the workflow."""

    parent_id: Optional[str]
    """ID for the parent workflow if this was started as a child."""

    parent_run_id: Optional[str]
    """Run ID for the parent workflow if this was started as a child."""

    root_id: Optional[str]
    """ID for the root workflow."""

    root_run_id: Optional[str]
    """Run ID for the root workflow."""

    raw_info: temporalio.api.workflow.v1.WorkflowExecutionInfo
    """Underlying protobuf info."""

    run_id: str
    """Run ID for this workflow run."""

    search_attributes: temporalio.common.SearchAttributes
    """Current set of search attributes if any.

    .. deprecated::
        Use :py:attr:`typed_search_attributes` instead.
    """

    start_time: datetime
    """When the workflow was created."""

    status: Optional[WorkflowExecutionStatus]
    """Status for the workflow."""

    task_queue: str
    """Task queue for the workflow."""

    typed_search_attributes: temporalio.common.TypedSearchAttributes
    """Current set of search attributes if any."""

    workflow_type: str
    """Type name for the workflow."""

    @classmethod
    def _from_raw_info(
        cls,
        info: temporalio.api.workflow.v1.WorkflowExecutionInfo,
        converter: temporalio.converter.DataConverter,
        **additional_fields: Any,
    ) -> WorkflowExecution:
        return cls(
            close_time=info.close_time.ToDatetime().replace(tzinfo=timezone.utc)
            if info.HasField("close_time")
            else None,
            data_converter=converter,
            execution_time=info.execution_time.ToDatetime().replace(tzinfo=timezone.utc)
            if info.HasField("execution_time")
            else None,
            history_length=info.history_length,
            id=info.execution.workflow_id,
            parent_id=info.parent_execution.workflow_id
            if info.HasField("parent_execution")
            else None,
            parent_run_id=info.parent_execution.run_id
            if info.HasField("parent_execution")
            else None,
            root_id=info.root_execution.workflow_id
            if info.HasField("root_execution")
            else None,
            root_run_id=info.root_execution.run_id
            if info.HasField("root_execution")
            else None,
            raw_info=info,
            run_id=info.execution.run_id,
            search_attributes=temporalio.converter.decode_search_attributes(
                info.search_attributes
            ),
            start_time=info.start_time.ToDatetime().replace(tzinfo=timezone.utc),
            status=WorkflowExecutionStatus(info.status) if info.status else None,
            task_queue=info.task_queue,
            typed_search_attributes=temporalio.converter.decode_typed_search_attributes(
                info.search_attributes
            ),
            workflow_type=info.type.name,
            **additional_fields,
        )

    async def memo(self) -> Mapping[str, Any]:
        """Workflow's memo values, converted without type hints.

        Since type hints are not used, the default converted values will come
        back. For example, if the memo was originally created with a dataclass,
        the value will be a dict. To convert using proper type hints, use
        :py:meth:`memo_value`.

        Returns:
            Mapping of all memo keys and they values without type hints.
        """
        return {
            k: (await self.data_converter.decode([v]))[0]
            for k, v in self.raw_info.memo.fields.items()
        }

    @overload
    async def memo_value(
        self, key: str, default: Any = temporalio.common._arg_unset
    ) -> Any: ...

    @overload
    async def memo_value(
        self, key: str, *, type_hint: Type[ParamType]
    ) -> ParamType: ...

    @overload
    async def memo_value(
        self, key: str, default: AnyType, *, type_hint: Type[ParamType]
    ) -> Union[AnyType, ParamType]: ...

    async def memo_value(
        self,
        key: str,
        default: Any = temporalio.common._arg_unset,
        *,
        type_hint: Optional[Type] = None,
    ) -> Any:
        """Memo value for the given key, optional default, and optional type
        hint.

        Args:
            key: Key to get memo value for.
            default: Default to use if key is not present. If unset, a
                :py:class:`KeyError` is raised when the key does not exist.
            type_hint: Type hint to use when converting.

        Returns:
            Memo value, converted with the type hint if present.

        Raises:
            KeyError: Key not present and default not set.
        """
        payload = self.raw_info.memo.fields.get(key)
        if not payload:
            if default is temporalio.common._arg_unset:
                raise KeyError(f"Memo does not have a value for key {key}")
            return default
        return (
            await self.data_converter.decode(
                [payload], [type_hint] if type_hint else None
            )
        )[0]


@dataclass
class WorkflowExecutionDescription(WorkflowExecution):
    """Description for a single workflow execution run."""

    raw_description: temporalio.api.workflowservice.v1.DescribeWorkflowExecutionResponse
    """Underlying protobuf description."""

    _static_summary: Optional[str] = None
    _static_details: Optional[str] = None
    _metadata_decoded: bool = False

    async def static_summary(self) -> Optional[str]:
        """Gets the single-line fixed summary for this workflow execution that may appear in
        UI/CLI. This can be in single-line Temporal markdown format.
        """
        if not self._metadata_decoded:
            await self._decode_metadata()
        return self._static_summary

    async def static_details(self) -> Optional[str]:
        """Gets the general fixed details for this workflow execution that may appear in UI/CLI.
        This can be in Temporal markdown format and can span multiple lines.
        """
        if not self._metadata_decoded:
            await self._decode_metadata()
        return self._static_details

    async def _decode_metadata(self) -> None:
        """Internal method to decode metadata lazily."""
        self._static_summary, self._static_details = await _decode_user_metadata(
            self.data_converter, self.raw_description.execution_config.user_metadata
        )
        self._metadata_decoded = True

    @staticmethod
    async def _from_raw_description(
        description: temporalio.api.workflowservice.v1.DescribeWorkflowExecutionResponse,
        converter: temporalio.converter.DataConverter,
    ) -> WorkflowExecutionDescription:
        return WorkflowExecutionDescription._from_raw_info(  # type: ignore
            description.workflow_execution_info,
            converter,
            raw_description=description,
        )


class WorkflowExecutionStatus(IntEnum):
    """Status of a workflow execution.

    See :py:class:`temporalio.api.enums.v1.WorkflowExecutionStatus`.
    """

    RUNNING = int(
        temporalio.api.enums.v1.WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING
    )
    COMPLETED = int(
        temporalio.api.enums.v1.WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED
    )
    FAILED = int(
        temporalio.api.enums.v1.WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_FAILED
    )
    CANCELED = int(
        temporalio.api.enums.v1.WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_CANCELED
    )
    TERMINATED = int(
        temporalio.api.enums.v1.WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_TERMINATED
    )
    CONTINUED_AS_NEW = int(
        temporalio.api.enums.v1.WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW
    )
    TIMED_OUT = int(
        temporalio.api.enums.v1.WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_TIMED_OUT
    )


@dataclass
class WorkflowExecutionCount:
    """Representation of a count from a count workflows call."""

    count: int
    """Approximate number of workflows matching the original query.

    If the query had a group-by clause, this is simply the sum of all the counts
    in py:attr:`groups`.
    """

    groups: Sequence[WorkflowExecutionCountAggregationGroup]
    """Groups if the query had a group-by clause, or empty if not."""

    @staticmethod
    def _from_raw(
        raw: temporalio.api.workflowservice.v1.CountWorkflowExecutionsResponse,
    ) -> WorkflowExecutionCount:
        return WorkflowExecutionCount(
            count=raw.count,
            groups=[
                WorkflowExecutionCountAggregationGroup._from_raw(g) for g in raw.groups
            ],
        )


@dataclass
class WorkflowExecutionCountAggregationGroup:
    """Aggregation group if the workflow count query had a group-by clause."""

    count: int
    """Approximate number of workflows matching the original query for this
    group.
    """

    group_values: Sequence[temporalio.common.SearchAttributeValue]
    """Search attribute values for this group."""

    @staticmethod
    def _from_raw(
        raw: temporalio.api.workflowservice.v1.CountWorkflowExecutionsResponse.AggregationGroup,
    ) -> WorkflowExecutionCountAggregationGroup:
        return WorkflowExecutionCountAggregationGroup(
            count=raw.count,
            group_values=[
                temporalio.converter._decode_search_attribute_value(v)
                for v in raw.group_values
            ],
        )


class WorkflowExecutionAsyncIterator:
    """Asynchronous iterator for :py:class:`WorkflowExecution` values.

    Most users should use ``async for`` on this iterator and not call any of the
    methods within. To consume the workflows as histories, call
    :py:meth:`map_histories`.
    """

    def __init__(
        self,
        client: Client,
        input: ListWorkflowsInput,
    ) -> None:
        """Create an asynchronous iterator for the given input.

        Users should not create this directly, but rather use
        :py:meth:`Client.list_workflows`.
        """
        self._client = client
        self._input = input
        self._next_page_token = input.next_page_token
        self._current_page: Optional[Sequence[WorkflowExecution]] = None
        self._current_page_index = 0
        self._limit = input.limit
        self._yielded = 0

    @property
    def current_page_index(self) -> int:
        """Index of the entry in the current page that will be returned from
        the next :py:meth:`__anext__` call.
        """
        return self._current_page_index

    @property
    def current_page(self) -> Optional[Sequence[WorkflowExecution]]:
        """Current page, if it has been fetched yet."""
        return self._current_page

    @property
    def next_page_token(self) -> Optional[bytes]:
        """Token for the next page request if any."""
        return self._next_page_token

    async def fetch_next_page(self, *, page_size: Optional[int] = None) -> None:
        """Fetch the next page if any.

        Args:
            page_size: Override the page size this iterator was originally
                created with.
        """
        page_size = page_size or self._input.page_size
        if self._limit is not None and self._limit - self._yielded < page_size:
            page_size = self._limit - self._yielded

        resp = await self._client.workflow_service.list_workflow_executions(
            temporalio.api.workflowservice.v1.ListWorkflowExecutionsRequest(
                namespace=self._client.namespace,
                page_size=page_size,
                next_page_token=self._next_page_token or b"",
                query=self._input.query or "",
            ),
            retry=True,
            metadata=self._input.rpc_metadata,
            timeout=self._input.rpc_timeout,
        )
        self._current_page = [
            WorkflowExecution._from_raw_info(v, self._client.data_converter)
            for v in resp.executions
        ]
        self._current_page_index = 0
        self._next_page_token = resp.next_page_token or None

    def __aiter__(self) -> WorkflowExecutionAsyncIterator:
        """Return self as the iterator."""
        return self

    async def __anext__(self) -> WorkflowExecution:
        """Get the next execution on this iterator, fetching next page if
        necessary.
        """
        if self._limit is not None and self._yielded >= self._limit:
            raise StopAsyncIteration
        while True:
            # No page? fetch and continue
            if self._current_page is None:
                await self.fetch_next_page()
                continue
            # No more left in page?
            if self._current_page_index >= len(self._current_page):
                # If there is a next page token, try to get another page and try
                # again
                if self._next_page_token is not None:
                    await self.fetch_next_page()
                    continue
                # No more pages means we're done
                raise StopAsyncIteration
            # Get current, increment page index, and return
            ret = self._current_page[self._current_page_index]
            self._current_page_index += 1
            self._yielded += 1
            return ret

    async def map_histories(
        self,
        *,
        event_filter_type: WorkflowHistoryEventFilterType = WorkflowHistoryEventFilterType.ALL_EVENT,
        skip_archival: bool = False,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> AsyncIterator[WorkflowHistory]:
        """Create an async iterator consuming all workflows and calling
        :py:meth:`WorkflowHandle.fetch_history` on each one.

        This is just a shortcut for ``fetch_history``, see that method for
        parameter details.
        """
        async for v in self:
            yield await self._client.get_workflow_handle(
                v.id, run_id=v.run_id
            ).fetch_history(
                event_filter_type=event_filter_type,
                skip_archival=skip_archival,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )


@dataclass(frozen=True)
class WorkflowHistory:
    """A workflow's ID and immutable history."""

    workflow_id: str
    """ID of the workflow."""

    events: Sequence[temporalio.api.history.v1.HistoryEvent]
    """History events for the workflow."""

    @property
    def run_id(self) -> str:
        """Run ID extracted from the first event."""
        if not self.events:
            raise RuntimeError("No events")
        if not self.events[0].HasField("workflow_execution_started_event_attributes"):
            raise RuntimeError("First event is not workflow start")
        return self.events[
            0
        ].workflow_execution_started_event_attributes.original_execution_run_id

    @staticmethod
    def from_json(
        workflow_id: str, history: Union[str, Dict[str, Any]]
    ) -> WorkflowHistory:
        """Construct a WorkflowHistory from an ID and a json dump of history.

        This is built to work both with Temporal UI/tctl JSON as well as
        :py:meth:`to_json` even though they are slightly different.

        Args:
            workflow_id: The workflow's ID
            history: A string or parsed-to-dict representation of workflow
                history

        Returns:
            Workflow history
        """
        parsed = _history_from_json(history)
        return WorkflowHistory(workflow_id, parsed.events)

    def to_json(self) -> str:
        """Convert this history to JSON.

        Note, this does not include the workflow ID.
        """
        return google.protobuf.json_format.MessageToJson(
            temporalio.api.history.v1.History(events=self.events)
        )

    def to_json_dict(self) -> Dict[str, Any]:
        """Convert this history to JSON-compatible dict.

        Note, this does not include the workflow ID.
        """
        return google.protobuf.json_format.MessageToDict(
            temporalio.api.history.v1.History(events=self.events)
        )


@dataclass
class WorkflowHistoryEventAsyncIterator:
    """Asynchronous iterator for history events of a workflow.

    Most users should use ``async for`` on this iterator and not call any of the
    methods within.
    """

    def __init__(
        self,
        client: Client,
        input: FetchWorkflowHistoryEventsInput,
    ) -> None:
        """Create an asynchronous iterator for the given input.

        Users should not create this directly, but rather use
        :py:meth:`WorkflowHandle.fetch_history_events`.
        """
        self._client = client
        self._input = input
        self._next_page_token = input.next_page_token
        self._current_page: Optional[
            Sequence[temporalio.api.history.v1.HistoryEvent]
        ] = None
        self._current_page_index = 0

    @property
    def current_page_index(self) -> int:
        """Index of the entry in the current page that will be returned from
        the next :py:meth:`__anext__` call.
        """
        return self._current_page_index

    @property
    def current_page(
        self,
    ) -> Optional[Sequence[temporalio.api.history.v1.HistoryEvent]]:
        """Current page, if it has been fetched yet."""
        return self._current_page

    @property
    def next_page_token(self) -> Optional[bytes]:
        """Token for the next page request if any."""
        return self._next_page_token

    async def fetch_next_page(self, *, page_size: Optional[int] = None) -> None:
        """Fetch the next page if any.

        Args:
            page_size: Override the page size this iterator was originally
                created with.
        """
        resp = await self._client.workflow_service.get_workflow_execution_history(
            temporalio.api.workflowservice.v1.GetWorkflowExecutionHistoryRequest(
                namespace=self._client.namespace,
                execution=temporalio.api.common.v1.WorkflowExecution(
                    workflow_id=self._input.id,
                    run_id=self._input.run_id or "",
                ),
                maximum_page_size=self._input.page_size or 0,
                next_page_token=self._next_page_token or b"",
                wait_new_event=self._input.wait_new_event,
                history_event_filter_type=temporalio.api.enums.v1.HistoryEventFilterType.ValueType(
                    self._input.event_filter_type
                ),
                skip_archival=self._input.skip_archival,
            ),
            retry=True,
            metadata=self._input.rpc_metadata,
            timeout=self._input.rpc_timeout,
        )
        # We don't support raw history
        assert len(resp.raw_history) == 0
        self._current_page = list(resp.history.events)
        self._current_page_index = 0
        self._next_page_token = resp.next_page_token or None

    def __aiter__(self) -> WorkflowHistoryEventAsyncIterator:
        """Return self as the iterator."""
        return self

    async def __anext__(self) -> temporalio.api.history.v1.HistoryEvent:
        """Get the next execution on this iterator, fetching next page if
        necessary.
        """
        while True:
            # No page? fetch and continue
            if self._current_page is None:
                await self.fetch_next_page()
                continue
            # No more left in page?
            if self._current_page_index >= len(self._current_page):
                # If there is a next page token, try to get another page and try
                # again
                if self._next_page_token is not None:
                    await self.fetch_next_page()
                    continue
                # No more pages means we're done
                raise StopAsyncIteration
            # Increment page index and return
            ret = self._current_page[self._current_page_index]
            self._current_page_index += 1
            return ret


class ScheduleHandle:
    """Handle for interacting with a schedule.

    This is usually created via :py:meth:`Client.get_schedule_handle` or
    returned from :py:meth:`Client.create_schedule`.

    Attributes:
        id: ID of the schedule.
    """

    def __init__(self, client: Client, id: str) -> None:
        """Create schedule handle."""
        self._client = client
        self.id = id

    async def backfill(
        self,
        *backfill: ScheduleBackfill,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Backfill the schedule by going through the specified time periods as
        if they passed right now.

        Args:
            backfill: Backfill periods.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.
        """
        if not backfill:
            raise ValueError("At least one backfill required")
        await self._client._impl.backfill_schedule(
            BackfillScheduleInput(
                id=self.id,
                backfills=backfill,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            ),
        )

    async def delete(
        self,
        *,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Delete this schedule.

        Args:
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.
        """
        await self._client._impl.delete_schedule(
            DeleteScheduleInput(
                id=self.id,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            ),
        )

    async def describe(
        self,
        *,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> ScheduleDescription:
        """Fetch this schedule's description.

        Args:
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.
        """
        return await self._client._impl.describe_schedule(
            DescribeScheduleInput(
                id=self.id,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            ),
        )

    async def pause(
        self,
        *,
        note: Optional[str] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Pause the schedule and set a note.

        Args:
            note: Note to set on the schedule.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.
        """
        await self._client._impl.pause_schedule(
            PauseScheduleInput(
                id=self.id,
                note=note,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            ),
        )

    async def trigger(
        self,
        *,
        overlap: Optional[ScheduleOverlapPolicy] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Trigger an action on this schedule to happen immediately.

        Args:
            overlap: If set, overrides the schedule's overlap policy.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.
        """
        await self._client._impl.trigger_schedule(
            TriggerScheduleInput(
                id=self.id,
                overlap=overlap,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            ),
        )

    async def unpause(
        self,
        *,
        note: Optional[str] = None,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Unpause the schedule and set a note.

        Args:
            note: Note to set on the schedule.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.
        """
        await self._client._impl.unpause_schedule(
            UnpauseScheduleInput(
                id=self.id,
                note=note,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            ),
        )

    @overload
    async def update(
        self,
        updater: Callable[[ScheduleUpdateInput], Optional[ScheduleUpdate]],
        *,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None: ...

    @overload
    async def update(
        self,
        updater: Callable[[ScheduleUpdateInput], Awaitable[Optional[ScheduleUpdate]]],
        *,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None: ...

    async def update(
        self,
        updater: Callable[
            [ScheduleUpdateInput],
            Union[Optional[ScheduleUpdate], Awaitable[Optional[ScheduleUpdate]]],
        ],
        *,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Update a schedule using a callback to build the update from the
        description.

        The callback may be invoked multiple times in a conflict-resolution
        loop.

        Args:
            updater: Callback that returns the update. It accepts a
                :py:class:`ScheduleUpdateInput` and returns a
                :py:class:`ScheduleUpdate`. If None is returned or an error
                occurs, the update is not attempted. This may be called multiple
                times.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys. This is for every call made
                within.
            rpc_timeout: Optional RPC deadline to set for the RPC call. This is
                for each call made within, not overall.
        """
        await self._client._impl.update_schedule(
            UpdateScheduleInput(
                id=self.id,
                updater=updater,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            ),
        )


@dataclass
class ScheduleSpec:
    """Specification of the times scheduled actions may occur.

    The times are the union of :py:attr:`calendars`, :py:attr:`intervals`, and
    :py:attr:`cron_expressions` excluding anything in :py:attr:`skip`.
    """

    calendars: Sequence[ScheduleCalendarSpec] = dataclasses.field(default_factory=list)
    """Calendar-based specification of times."""

    intervals: Sequence[ScheduleIntervalSpec] = dataclasses.field(default_factory=list)
    """Interval-based specification of times."""

    cron_expressions: Sequence[str] = dataclasses.field(default_factory=list)
    """Cron-based specification of times.

    This is provided for easy migration from legacy string-based cron
    scheduling. New uses should use :py:attr:`calendars` instead. These
    expressions will be translated to calendar-based specifications on the
    server.
    """

    skip: Sequence[ScheduleCalendarSpec] = dataclasses.field(default_factory=list)
    """Set of matching calendar times that will be skipped."""

    start_at: Optional[datetime] = None
    """Time before which any matching times will be skipped."""

    end_at: Optional[datetime] = None
    """Time after which any matching times will be skipped."""

    jitter: Optional[timedelta] = None
    """Jitter to apply each action.

    An action's scheduled time will be incremented by a random value between 0
    and this value if present (but not past the next schedule).
    """

    time_zone_name: Optional[str] = None
    """IANA time zone name, for example ``US/Central``."""

    @staticmethod
    def _from_proto(spec: temporalio.api.schedule.v1.ScheduleSpec) -> ScheduleSpec:
        return ScheduleSpec(
            calendars=[
                ScheduleCalendarSpec._from_proto(c) for c in spec.structured_calendar
            ],
            intervals=[ScheduleIntervalSpec._from_proto(i) for i in spec.interval],
            cron_expressions=spec.cron_string,
            skip=[
                ScheduleCalendarSpec._from_proto(c)
                for c in spec.exclude_structured_calendar
            ],
            start_at=spec.start_time.ToDatetime().replace(tzinfo=timezone.utc)
            if spec.HasField("start_time")
            else None,
            end_at=spec.end_time.ToDatetime().replace(tzinfo=timezone.utc)
            if spec.HasField("end_time")
            else None,
            jitter=spec.jitter.ToTimedelta() if spec.HasField("jitter") else None,
            time_zone_name=spec.timezone_name or None,
        )

    def _to_proto(self) -> temporalio.api.schedule.v1.ScheduleSpec:
        start_time: Optional[google.protobuf.timestamp_pb2.Timestamp] = None
        if self.start_at:
            start_time = google.protobuf.timestamp_pb2.Timestamp()
            start_time.FromDatetime(self.start_at)
        end_time: Optional[google.protobuf.timestamp_pb2.Timestamp] = None
        if self.end_at:
            end_time = google.protobuf.timestamp_pb2.Timestamp()
            end_time.FromDatetime(self.end_at)
        jitter: Optional[google.protobuf.duration_pb2.Duration] = None
        if self.jitter:
            jitter = google.protobuf.duration_pb2.Duration()
            jitter.FromTimedelta(self.jitter)
        return temporalio.api.schedule.v1.ScheduleSpec(
            structured_calendar=[cal._to_proto() for cal in self.calendars],
            cron_string=self.cron_expressions,
            interval=[i._to_proto() for i in self.intervals],
            exclude_structured_calendar=[cal._to_proto() for cal in self.skip],
            start_time=start_time,
            end_time=end_time,
            jitter=jitter,
            timezone_name=self.time_zone_name or "",
        )


@dataclass(frozen=True)
class ScheduleRange:
    """Inclusive range for a schedule match value."""

    start: int
    """Inclusive start of the range."""

    end: int = 0
    """Inclusive end of the range.

    If unset or less than start, defaults to start.
    """

    step: int = 0
    """
    Step to take between each value.

    Unset or 0 defaults as 1.
    """

    def __post_init__(self):
        """Set field defaults."""
        # Class is frozen, so we must setattr bypassing dataclass setattr
        if self.end < self.start:
            object.__setattr__(self, "end", self.start)
        if self.step == 0:
            object.__setattr__(self, "step", 1)

    @staticmethod
    def _from_protos(
        ranges: Sequence[temporalio.api.schedule.v1.Range],
    ) -> Sequence[ScheduleRange]:
        return tuple(ScheduleRange._from_proto(r) for r in ranges)

    @staticmethod
    def _from_proto(range: temporalio.api.schedule.v1.Range) -> ScheduleRange:
        return ScheduleRange(start=range.start, end=range.end, step=range.step)

    @staticmethod
    def _to_protos(
        ranges: Sequence[ScheduleRange],
    ) -> Sequence[temporalio.api.schedule.v1.Range]:
        return tuple(r._to_proto() for r in ranges)

    def _to_proto(self) -> temporalio.api.schedule.v1.Range:
        return temporalio.api.schedule.v1.Range(
            start=self.start, end=self.end, step=self.step
        )


@dataclass
class ScheduleCalendarSpec:
    """Specification relative to calendar time when to run an action.

    A timestamp matches if at least one range of each field matches except for
    year. If year is missing, that means all years match. For all fields besides
    year, at least one range must be present to match anything.
    """

    second: Sequence[ScheduleRange] = (ScheduleRange(0),)
    """Second range to match, 0-59. Default matches 0."""

    minute: Sequence[ScheduleRange] = (ScheduleRange(0),)
    """Minute range to match, 0-59. Default matches 0."""

    hour: Sequence[ScheduleRange] = (ScheduleRange(0),)
    """Hour range to match, 0-23. Default matches 0."""

    day_of_month: Sequence[ScheduleRange] = (ScheduleRange(1, 31),)
    """Day of month range to match, 1-31. Default matches all days."""

    month: Sequence[ScheduleRange] = (ScheduleRange(1, 12),)
    """Month range to match, 1-12. Default matches all months."""

    year: Sequence[ScheduleRange] = ()
    """Optional year range to match. Default of empty matches all years."""

    day_of_week: Sequence[ScheduleRange] = (ScheduleRange(0, 6),)
    """Day of week range to match, 0-6, 0 is Sunday. Default matches all
    days."""

    comment: Optional[str] = None
    """Description of this schedule."""

    @staticmethod
    def _from_proto(
        spec: temporalio.api.schedule.v1.StructuredCalendarSpec,
    ) -> ScheduleCalendarSpec:
        return ScheduleCalendarSpec(
            second=ScheduleRange._from_protos(spec.second),
            minute=ScheduleRange._from_protos(spec.minute),
            hour=ScheduleRange._from_protos(spec.hour),
            day_of_month=ScheduleRange._from_protos(spec.day_of_month),
            month=ScheduleRange._from_protos(spec.month),
            year=ScheduleRange._from_protos(spec.year),
            day_of_week=ScheduleRange._from_protos(spec.day_of_week),
            comment=spec.comment or None,
        )

    def _to_proto(self) -> temporalio.api.schedule.v1.StructuredCalendarSpec:
        return temporalio.api.schedule.v1.StructuredCalendarSpec(
            second=ScheduleRange._to_protos(self.second),
            minute=ScheduleRange._to_protos(self.minute),
            hour=ScheduleRange._to_protos(self.hour),
            day_of_month=ScheduleRange._to_protos(self.day_of_month),
            month=ScheduleRange._to_protos(self.month),
            year=ScheduleRange._to_protos(self.year),
            day_of_week=ScheduleRange._to_protos(self.day_of_week),
            comment=self.comment or "",
        )


@dataclass
class ScheduleIntervalSpec:
    """Specification for scheduling on an interval.

    Matches times expressed as epoch + (n * every) + offset.
    """

    every: timedelta
    """Period to repeat the interval."""

    offset: Optional[timedelta] = None
    """Fixed offset added to each interval period."""

    @staticmethod
    def _from_proto(
        spec: temporalio.api.schedule.v1.IntervalSpec,
    ) -> ScheduleIntervalSpec:
        return ScheduleIntervalSpec(
            every=spec.interval.ToTimedelta(),
            offset=spec.phase.ToTimedelta() if spec.HasField("phase") else None,
        )

    def _to_proto(self) -> temporalio.api.schedule.v1.IntervalSpec:
        interval = google.protobuf.duration_pb2.Duration()
        interval.FromTimedelta(self.every)
        phase: Optional[google.protobuf.duration_pb2.Duration] = None
        if self.offset:
            phase = google.protobuf.duration_pb2.Duration()
            phase.FromTimedelta(self.offset)
        return temporalio.api.schedule.v1.IntervalSpec(interval=interval, phase=phase)


class ScheduleAction(ABC):
    """Base class for an action a schedule can take.

    See :py:class:`ScheduleActionStartWorkflow` for the most commonly used
    implementation.
    """

    @staticmethod
    def _from_proto(
        action: temporalio.api.schedule.v1.ScheduleAction,
    ) -> ScheduleAction:
        if action.HasField("start_workflow"):
            return ScheduleActionStartWorkflow._from_proto(action.start_workflow)
        else:
            raise ValueError(f"Unsupported action: {action.WhichOneof('action')}")

    @abstractmethod
    async def _to_proto(
        self, client: Client
    ) -> temporalio.api.schedule.v1.ScheduleAction: ...


@dataclass
class ScheduleActionStartWorkflow(ScheduleAction):
    """Schedule action to start a workflow."""

    workflow: str
    args: Union[Sequence[Any], Sequence[temporalio.api.common.v1.Payload]]
    id: str
    task_queue: str
    execution_timeout: Optional[timedelta]
    run_timeout: Optional[timedelta]
    task_timeout: Optional[timedelta]
    retry_policy: Optional[temporalio.common.RetryPolicy]
    memo: Optional[
        Union[Mapping[str, Any], Mapping[str, temporalio.api.common.v1.Payload]]
    ]
    typed_search_attributes: temporalio.common.TypedSearchAttributes
    untyped_search_attributes: temporalio.common.SearchAttributes
    """This is deprecated and is only present in case existing untyped
    attributes already exist for update. This should never be used when
    creating."""
    static_summary: Optional[Union[str, temporalio.api.common.v1.Payload]]
    static_details: Optional[Union[str, temporalio.api.common.v1.Payload]]
    priority: temporalio.common.Priority

    headers: Optional[Mapping[str, temporalio.api.common.v1.Payload]]
    """
    Headers may still be encoded by the payload codec if present.
    """
    _from_raw: bool = dataclasses.field(compare=False, init=False)

    @staticmethod
    def _from_proto(  # pyright: ignore
        info: temporalio.api.workflow.v1.NewWorkflowExecutionInfo,  # type: ignore[override]
    ) -> ScheduleActionStartWorkflow:
        return ScheduleActionStartWorkflow("<unset>", raw_info=info)

    # Overload for no-param workflow
    @overload
    def __init__(
        self,
        workflow: MethodAsyncNoParam[SelfType, ReturnType],
        *,
        id: str,
        task_queue: str,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        retry_policy: Optional[temporalio.common.RetryPolicy] = None,
        memo: Optional[Mapping[str, Any]] = None,
        typed_search_attributes: temporalio.common.TypedSearchAttributes = temporalio.common.TypedSearchAttributes.empty,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
    ) -> None: ...

    # Overload for single-param workflow
    @overload
    def __init__(
        self,
        workflow: MethodAsyncSingleParam[SelfType, ParamType, ReturnType],
        arg: ParamType,
        *,
        id: str,
        task_queue: str,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        retry_policy: Optional[temporalio.common.RetryPolicy] = None,
        memo: Optional[Mapping[str, Any]] = None,
        typed_search_attributes: temporalio.common.TypedSearchAttributes = temporalio.common.TypedSearchAttributes.empty,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
    ) -> None: ...

    # Overload for multi-param workflow
    @overload
    def __init__(
        self,
        workflow: Callable[
            Concatenate[SelfType, MultiParamSpec], Awaitable[ReturnType]
        ],
        *,
        args: Sequence[Any],
        id: str,
        task_queue: str,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        retry_policy: Optional[temporalio.common.RetryPolicy] = None,
        memo: Optional[Mapping[str, Any]] = None,
        typed_search_attributes: temporalio.common.TypedSearchAttributes = temporalio.common.TypedSearchAttributes.empty,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
    ) -> None: ...

    # Overload for string-name workflow
    @overload
    def __init__(
        self,
        workflow: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: str,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        retry_policy: Optional[temporalio.common.RetryPolicy] = None,
        memo: Optional[Mapping[str, Any]] = None,
        typed_search_attributes: temporalio.common.TypedSearchAttributes = temporalio.common.TypedSearchAttributes.empty,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
    ) -> None: ...

    # Overload for raw info
    @overload
    def __init__(
        self,
        workflow: str,
        *,
        raw_info: temporalio.api.workflow.v1.NewWorkflowExecutionInfo,
    ) -> None: ...

    def __init__(
        self,
        workflow: Union[str, Callable[..., Awaitable[Any]]],
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: Optional[str] = None,
        task_queue: Optional[str] = None,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        retry_policy: Optional[temporalio.common.RetryPolicy] = None,
        memo: Optional[Mapping[str, Any]] = None,
        typed_search_attributes: temporalio.common.TypedSearchAttributes = temporalio.common.TypedSearchAttributes.empty,
        untyped_search_attributes: temporalio.common.SearchAttributes = {},
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        headers: Optional[Mapping[str, temporalio.api.common.v1.Payload]] = None,
        raw_info: Optional[temporalio.api.workflow.v1.NewWorkflowExecutionInfo] = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
    ) -> None:
        """Create a start-workflow action.

        See :py:meth:`Client.start_workflow` for details on these parameter
        values.
        """
        super().__init__()
        if raw_info:
            self._from_raw = True
            # Ignore other fields
            self.workflow = raw_info.workflow_type.name
            self.args = raw_info.input.payloads if raw_info.input else []
            self.id = raw_info.workflow_id
            self.task_queue = raw_info.task_queue.name
            self.execution_timeout = (
                raw_info.workflow_execution_timeout.ToTimedelta()
                if raw_info.HasField("workflow_execution_timeout")
                else None
            )
            self.run_timeout = (
                raw_info.workflow_run_timeout.ToTimedelta()
                if raw_info.HasField("workflow_run_timeout")
                else None
            )
            self.task_timeout = (
                raw_info.workflow_task_timeout.ToTimedelta()
                if raw_info.HasField("workflow_task_timeout")
                else None
            )
            self.retry_policy = (
                temporalio.common.RetryPolicy.from_proto(raw_info.retry_policy)
                if raw_info.HasField("retry_policy")
                else None
            )
            self.memo = raw_info.memo.fields if raw_info.memo.fields else None
            self.typed_search_attributes = (
                temporalio.converter.decode_typed_search_attributes(
                    raw_info.search_attributes
                )
            )
            self.headers = raw_info.header.fields if raw_info.header.fields else None
            # Also set the untyped attributes as the set of attributes from
            # decode with the typed ones removed
            self.untyped_search_attributes = (
                temporalio.converter.decode_search_attributes(
                    raw_info.search_attributes
                )
            )
            for pair in self.typed_search_attributes:
                if pair.key.name in self.untyped_search_attributes:
                    # We know this is mutable here
                    del self.untyped_search_attributes[pair.key.name]  # type: ignore
            self.static_summary = (
                raw_info.user_metadata.summary
                if raw_info.HasField("user_metadata") and raw_info.user_metadata.summary
                else None
            )
            self.static_details = (
                raw_info.user_metadata.details
                if raw_info.HasField("user_metadata") and raw_info.user_metadata.details
                else None
            )
            self.priority = (
                temporalio.common.Priority._from_proto(raw_info.priority)
                if raw_info.HasField("priority") and raw_info.priority
                else temporalio.common.Priority.default
            )
        else:
            self._from_raw = False
            if not id:
                raise ValueError("ID required")
            if not task_queue:
                raise ValueError("Task queue required")
            # Use definition if callable
            if callable(workflow):
                defn = temporalio.workflow._Definition.must_from_run_fn(workflow)
                if not defn.name:
                    raise ValueError("Cannot schedule dynamic workflow explicitly")
                workflow = defn.name
            elif not isinstance(workflow, str):
                raise TypeError("Workflow must be a string or callable")
            self.workflow = workflow
            self.args = temporalio.common._arg_or_args(arg, args)
            self.id = id
            self.task_queue = task_queue
            self.execution_timeout = execution_timeout
            self.run_timeout = run_timeout
            self.task_timeout = task_timeout
            self.retry_policy = retry_policy
            self.memo = memo
            self.typed_search_attributes = typed_search_attributes
            self.untyped_search_attributes = untyped_search_attributes
            self.headers = headers  # encode here
            self.static_summary = static_summary
            self.static_details = static_details
            self.priority = priority

    async def _to_proto(
        self, client: Client
    ) -> temporalio.api.schedule.v1.ScheduleAction:
        execution_timeout: Optional[google.protobuf.duration_pb2.Duration] = None
        if self.execution_timeout:
            execution_timeout = google.protobuf.duration_pb2.Duration()
            execution_timeout.FromTimedelta(self.execution_timeout)
        run_timeout: Optional[google.protobuf.duration_pb2.Duration] = None
        if self.run_timeout:
            run_timeout = google.protobuf.duration_pb2.Duration()
            run_timeout.FromTimedelta(self.run_timeout)
        task_timeout: Optional[google.protobuf.duration_pb2.Duration] = None
        if self.task_timeout:
            task_timeout = google.protobuf.duration_pb2.Duration()
            task_timeout.FromTimedelta(self.task_timeout)
        retry_policy: Optional[temporalio.api.common.v1.RetryPolicy] = None
        if self.retry_policy:
            retry_policy = temporalio.api.common.v1.RetryPolicy()
            self.retry_policy.apply_to_proto(retry_policy)
        priority: Optional[temporalio.api.common.v1.Priority] = None
        if self.priority:
            priority = self.priority._to_proto()
        action = temporalio.api.schedule.v1.ScheduleAction(
            start_workflow=temporalio.api.workflow.v1.NewWorkflowExecutionInfo(
                workflow_id=self.id,
                workflow_type=temporalio.api.common.v1.WorkflowType(name=self.workflow),
                task_queue=temporalio.api.taskqueue.v1.TaskQueue(name=self.task_queue),
                input=None
                if not self.args
                else temporalio.api.common.v1.Payloads(
                    payloads=[
                        a
                        if isinstance(a, temporalio.api.common.v1.Payload)
                        else (await client.data_converter.encode([a]))[0]
                        for a in self.args
                    ]
                ),
                workflow_execution_timeout=execution_timeout,
                workflow_run_timeout=run_timeout,
                workflow_task_timeout=task_timeout,
                retry_policy=retry_policy,
                memo=None
                if not self.memo
                else temporalio.api.common.v1.Memo(
                    fields={
                        k: v
                        if isinstance(v, temporalio.api.common.v1.Payload)
                        else (await client.data_converter.encode([v]))[0]
                        for k, v in self.memo.items()
                    },
                ),
                user_metadata=await _encode_user_metadata(
                    client.data_converter, self.static_summary, self.static_details
                ),
                priority=priority,
            ),
        )
        # Add any untyped attributes that are not also in the typed set
        untyped_not_in_typed = {
            k: v
            for k, v in self.untyped_search_attributes.items()
            if k not in self.typed_search_attributes
        }
        if untyped_not_in_typed:
            temporalio.converter.encode_search_attributes(
                untyped_not_in_typed, action.start_workflow.search_attributes
            )
        # TODO (dan): confirm whether this be `is not None`
        if self.typed_search_attributes:
            temporalio.converter.encode_search_attributes(
                self.typed_search_attributes, action.start_workflow.search_attributes
            )
        if self.headers:
            await _apply_headers(
                self.headers,
                action.start_workflow.header.fields,
                client.config()["header_codec_behavior"] == HeaderCodecBehavior.CODEC
                and not self._from_raw,
                client.data_converter.payload_codec,
            )
        return action


class ScheduleOverlapPolicy(IntEnum):
    """Controls what happens when a workflow would be started by a schedule but
    one is already running.
    """

    SKIP = int(
        temporalio.api.enums.v1.ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_SKIP
    )
    """Don't start anything.

    When the workflow completes, the next scheduled event after that time will
    be considered.
    """

    BUFFER_ONE = int(
        temporalio.api.enums.v1.ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE
    )
    """Start the workflow again soon as the current one completes, but only
    buffer one start in this way.

    If another start is supposed to happen when the workflow is running, and one
    is already buffered, then only the first one will be started after the
    running workflow finishes.
    """

    BUFFER_ALL = int(
        temporalio.api.enums.v1.ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL
    )
    """Buffer up any number of starts to all happen sequentially, immediately
    after the running workflow completes."""

    CANCEL_OTHER = int(
        temporalio.api.enums.v1.ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER
    )
    """If there is another workflow running, cancel it, and start the new one
    after the old one completes cancellation."""

    TERMINATE_OTHER = int(
        temporalio.api.enums.v1.ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER
    )
    """If there is another workflow running, terminate it and start the new one
    immediately."""

    ALLOW_ALL = int(
        temporalio.api.enums.v1.ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
    )
    """Start any number of concurrent workflows.

    Note that with this policy, last completion result and last failure will not
    be available since workflows are not sequential."""


@dataclass
class ScheduleBackfill:
    """Time period and policy for actions taken as if the time passed right
    now.
    """

    start_at: datetime
    """Start of the range to evaluate the schedule in.

    This is exclusive
    """
    end_at: datetime
    overlap: Optional[ScheduleOverlapPolicy] = None

    def _to_proto(self) -> temporalio.api.schedule.v1.BackfillRequest:
        start_time = google.protobuf.timestamp_pb2.Timestamp()
        start_time.FromDatetime(self.start_at)
        end_time = google.protobuf.timestamp_pb2.Timestamp()
        end_time.FromDatetime(self.end_at)
        overlap_policy = temporalio.api.enums.v1.ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED
        if self.overlap:
            overlap_policy = temporalio.api.enums.v1.ScheduleOverlapPolicy.ValueType(
                self.overlap
            )
        return temporalio.api.schedule.v1.BackfillRequest(
            start_time=start_time,
            end_time=end_time,
            overlap_policy=overlap_policy,
        )


@dataclass
class SchedulePolicy:
    """Policies of a schedule."""

    overlap: ScheduleOverlapPolicy = dataclasses.field(
        default_factory=lambda: ScheduleOverlapPolicy.SKIP
    )
    """Controls what happens when an action is started while another is still
    running."""

    catchup_window: timedelta = timedelta(days=365)
    """After a Temporal server is unavailable, amount of time in the past to
    execute missed actions."""

    pause_on_failure: bool = False
    """Whether to pause the schedule if an action fails or times out.

    Note: For workflows, this only applies after all retries have been
    exhausted.
    """

    @staticmethod
    def _from_proto(pol: temporalio.api.schedule.v1.SchedulePolicies) -> SchedulePolicy:
        return SchedulePolicy(
            overlap=ScheduleOverlapPolicy(int(pol.overlap_policy)),
            catchup_window=pol.catchup_window.ToTimedelta(),
            pause_on_failure=pol.pause_on_failure,
        )

    def _to_proto(self) -> temporalio.api.schedule.v1.SchedulePolicies:
        catchup_window = google.protobuf.duration_pb2.Duration()
        catchup_window.FromTimedelta(self.catchup_window)
        return temporalio.api.schedule.v1.SchedulePolicies(
            overlap_policy=temporalio.api.enums.v1.ScheduleOverlapPolicy.ValueType(
                self.overlap
            ),
            catchup_window=catchup_window,
            pause_on_failure=self.pause_on_failure,
        )


@dataclass
class ScheduleState:
    """State of a schedule."""

    note: Optional[str] = None
    """Human readable message for the schedule.

    The system may overwrite this value on certain conditions like
    pause-on-failure.
    """

    paused: bool = False
    """Whether the schedule is paused."""

    # Cannot be set to True on create
    limited_actions: bool = False
    """
    If true, remaining actions will be decremented for each action taken.

    On schedule create, this must be set to true if :py:attr:`remaining_actions`
    is non-zero and left false if :py:attr:`remaining_actions` is zero.
    """

    remaining_actions: int = 0
    """Actions remaining on this schedule.

    Once this number hits 0, no further actions are scheduled automatically.
    """

    @staticmethod
    def _from_proto(state: temporalio.api.schedule.v1.ScheduleState) -> ScheduleState:
        return ScheduleState(
            note=state.notes or None,
            paused=state.paused,
            limited_actions=state.limited_actions,
            remaining_actions=state.remaining_actions,
        )

    def _to_proto(self) -> temporalio.api.schedule.v1.ScheduleState:
        return temporalio.api.schedule.v1.ScheduleState(
            notes=self.note or "",
            paused=self.paused,
            limited_actions=self.limited_actions,
            remaining_actions=self.remaining_actions,
        )


@dataclass
class Schedule:
    """A schedule for periodically running an action."""

    action: ScheduleAction
    """Action taken when scheduled."""

    spec: ScheduleSpec
    """When the action is taken."""

    policy: SchedulePolicy = dataclasses.field(default_factory=SchedulePolicy)
    """Schedule policies."""

    state: ScheduleState = dataclasses.field(default_factory=ScheduleState)
    """State of the schedule."""

    @staticmethod
    def _from_proto(sched: temporalio.api.schedule.v1.Schedule) -> Schedule:
        return Schedule(
            action=ScheduleAction._from_proto(sched.action),
            spec=ScheduleSpec._from_proto(sched.spec),
            policy=SchedulePolicy._from_proto(sched.policies),
            state=ScheduleState._from_proto(sched.state),
        )

    async def _to_proto(self, client: Client) -> temporalio.api.schedule.v1.Schedule:
        catchup_window = google.protobuf.duration_pb2.Duration()
        catchup_window.FromTimedelta(self.policy.catchup_window)
        return temporalio.api.schedule.v1.Schedule(
            spec=self.spec._to_proto(),
            action=await self.action._to_proto(client),
            policies=self.policy._to_proto(),
            state=self.state._to_proto(),
        )


@dataclass
class ScheduleDescription:
    """Description of a schedule."""

    id: str
    """ID of the schedule."""

    schedule: Schedule
    """Schedule details that can be mutated."""

    info: ScheduleInfo
    """Information about the schedule."""

    typed_search_attributes: temporalio.common.TypedSearchAttributes
    """Search attributes on the schedule."""

    search_attributes: temporalio.common.SearchAttributes
    """Search attributes on the schedule.

    .. deprecated::
        Use :py:attr:`typed_search_attributes` instead.
    """

    data_converter: temporalio.converter.DataConverter
    """Data converter used for memo decoding."""

    raw_description: temporalio.api.workflowservice.v1.DescribeScheduleResponse
    """Raw description of the schedule."""

    @staticmethod
    def _from_proto(
        id: str,
        desc: temporalio.api.workflowservice.v1.DescribeScheduleResponse,
        converter: temporalio.converter.DataConverter,
    ) -> ScheduleDescription:
        return ScheduleDescription(
            id=id,
            schedule=Schedule._from_proto(desc.schedule),
            info=ScheduleInfo._from_proto(desc.info),
            typed_search_attributes=temporalio.converter.decode_typed_search_attributes(
                desc.search_attributes
            ),
            search_attributes=temporalio.converter.decode_search_attributes(
                desc.search_attributes
            ),
            data_converter=converter,
            raw_description=desc,
        )

    async def memo(self) -> Mapping[str, Any]:
        """Schedule's memo values, converted without type hints.

        Since type hints are not used, the default converted values will come
        back. For example, if the memo was originally created with a dataclass,
        the value will be a dict. To convert using proper type hints, use
        :py:meth:`memo_value`.

        Returns:
            Mapping of all memo keys and they values without type hints.
        """
        return {
            k: (await self.data_converter.decode([v]))[0]
            for k, v in self.raw_description.memo.fields.items()
        }

    @overload
    async def memo_value(
        self, key: str, default: Any = temporalio.common._arg_unset
    ) -> Any: ...

    @overload
    async def memo_value(
        self, key: str, *, type_hint: Type[ParamType]
    ) -> ParamType: ...

    @overload
    async def memo_value(
        self, key: str, default: AnyType, *, type_hint: Type[ParamType]
    ) -> Union[AnyType, ParamType]: ...

    async def memo_value(
        self,
        key: str,
        default: Any = temporalio.common._arg_unset,
        *,
        type_hint: Optional[Type] = None,
    ) -> Any:
        """Memo value for the given key, optional default, and optional type
        hint.

        Args:
            key: Key to get memo value for.
            default: Default to use if key is not present. If unset, a
                :py:class:`KeyError` is raised when the key does not exist.
            type_hint: Type hint to use when converting.

        Returns:
            Memo value, converted with the type hint if present.

        Raises:
            KeyError: Key not present and default not set.
        """
        payload = self.raw_description.memo.fields.get(key)
        if not payload:
            if default is temporalio.common._arg_unset:
                raise KeyError(f"Memo does not have a value for key {key}")
            return default
        return (
            await self.data_converter.decode(
                [payload], [type_hint] if type_hint else None
            )
        )[0]


@dataclass
class ScheduleInfo:
    """Information about a schedule."""

    num_actions: int
    """Number of actions taken by this schedule."""

    num_actions_missed_catchup_window: int
    """Number of times an action was skipped due to missing the catchup
    window."""

    num_actions_skipped_overlap: int
    """Number of actions skipped due to overlap."""

    running_actions: Sequence[ScheduleActionExecution]
    """Currently running actions."""

    recent_actions: Sequence[ScheduleActionResult]
    """10 most recent actions, oldest first."""

    next_action_times: Sequence[datetime]
    """Next 10 scheduled action times."""

    created_at: datetime
    """When the schedule was created."""

    last_updated_at: Optional[datetime]
    """When the schedule was last updated."""

    @staticmethod
    def _from_proto(info: temporalio.api.schedule.v1.ScheduleInfo) -> ScheduleInfo:
        return ScheduleInfo(
            num_actions=info.action_count,
            num_actions_missed_catchup_window=info.missed_catchup_window,
            num_actions_skipped_overlap=info.overlap_skipped,
            running_actions=[
                ScheduleActionExecutionStartWorkflow._from_proto(r)
                for r in info.running_workflows
            ],
            recent_actions=[
                ScheduleActionResult._from_proto(r) for r in info.recent_actions
            ],
            next_action_times=[
                f.ToDatetime().replace(tzinfo=timezone.utc)
                for f in info.future_action_times
            ],
            created_at=info.create_time.ToDatetime().replace(tzinfo=timezone.utc),
            last_updated_at=info.update_time.ToDatetime().replace(tzinfo=timezone.utc)
            if info.HasField("update_time")
            else None,
        )


class ScheduleActionExecution(ABC):
    """Base class for an action execution."""

    pass


@dataclass
class ScheduleActionExecutionStartWorkflow(ScheduleActionExecution):
    """Execution of a scheduled workflow start."""

    workflow_id: str
    """Workflow ID."""

    first_execution_run_id: str
    """Workflow run ID."""

    @staticmethod
    def _from_proto(
        exec: temporalio.api.common.v1.WorkflowExecution,
    ) -> ScheduleActionExecutionStartWorkflow:
        return ScheduleActionExecutionStartWorkflow(
            workflow_id=exec.workflow_id,
            first_execution_run_id=exec.run_id,
        )


@dataclass
class ScheduleActionResult:
    """Information about when an action took place."""

    scheduled_at: datetime
    """Scheduled time of the action including jitter."""

    started_at: datetime
    """When the action actually started."""

    action: ScheduleActionExecution
    """Action that took place."""

    @staticmethod
    def _from_proto(
        res: temporalio.api.schedule.v1.ScheduleActionResult,
    ) -> ScheduleActionResult:
        return ScheduleActionResult(
            scheduled_at=res.schedule_time.ToDatetime().replace(tzinfo=timezone.utc),
            started_at=res.actual_time.ToDatetime().replace(tzinfo=timezone.utc),
            action=ScheduleActionExecutionStartWorkflow._from_proto(
                res.start_workflow_result
            ),
        )


@dataclass
class ScheduleUpdateInput:
    """Parameter for an update callback for :py:meth:`ScheduleHandle.update`."""

    description: ScheduleDescription
    """Current description of the schedule."""


@dataclass
class ScheduleUpdate:
    """Result of an update callback for :py:meth:`ScheduleHandle.update`."""

    schedule: Schedule
    """Schedule to update."""

    search_attributes: Optional[temporalio.common.TypedSearchAttributes] = None
    """Search attributes to update."""


@dataclass
class ScheduleListDescription:
    """Description of a listed schedule."""

    id: str
    """ID of the schedule."""

    schedule: Optional[ScheduleListSchedule]
    """Schedule details that can be mutated.

    This may not be present in older Temporal servers without advanced
    visibility.
    """

    info: Optional[ScheduleListInfo]
    """Information about the schedule.

    This may not be present in older Temporal servers without advanced
    visibility.
    """

    typed_search_attributes: temporalio.common.TypedSearchAttributes
    """Search attributes on the schedule."""

    search_attributes: temporalio.common.SearchAttributes
    """Search attributes on the schedule.

    .. deprecated::
        Use :py:attr:`typed_search_attributes` instead.
    """

    data_converter: temporalio.converter.DataConverter
    """Data converter used for memo decoding."""

    raw_entry: temporalio.api.schedule.v1.ScheduleListEntry
    """Raw description of the schedule."""

    @staticmethod
    def _from_proto(
        entry: temporalio.api.schedule.v1.ScheduleListEntry,
        converter: temporalio.converter.DataConverter,
    ) -> ScheduleListDescription:
        return ScheduleListDescription(
            id=entry.schedule_id,
            schedule=ScheduleListSchedule._from_proto(entry.info)
            if entry.HasField("info")
            else None,
            info=ScheduleListInfo._from_proto(entry.info)
            if entry.HasField("info")
            else None,
            typed_search_attributes=temporalio.converter.decode_typed_search_attributes(
                entry.search_attributes
            ),
            search_attributes=temporalio.converter.decode_search_attributes(
                entry.search_attributes
            ),
            data_converter=converter,
            raw_entry=entry,
        )

    async def memo(self) -> Mapping[str, Any]:
        """Schedule's memo values, converted without type hints.

        Since type hints are not used, the default converted values will come
        back. For example, if the memo was originally created with a dataclass,
        the value will be a dict. To convert using proper type hints, use
        :py:meth:`memo_value`.

        Returns:
            Mapping of all memo keys and they values without type hints.
        """
        return {
            k: (await self.data_converter.decode([v]))[0]
            for k, v in self.raw_entry.memo.fields.items()
        }

    @overload
    async def memo_value(
        self, key: str, default: Any = temporalio.common._arg_unset
    ) -> Any: ...

    @overload
    async def memo_value(
        self, key: str, *, type_hint: Type[ParamType]
    ) -> ParamType: ...

    @overload
    async def memo_value(
        self, key: str, default: AnyType, *, type_hint: Type[ParamType]
    ) -> Union[AnyType, ParamType]: ...

    async def memo_value(
        self,
        key: str,
        default: Any = temporalio.common._arg_unset,
        *,
        type_hint: Optional[Type] = None,
    ) -> Any:
        """Memo value for the given key, optional default, and optional type
        hint.

        Args:
            key: Key to get memo value for.
            default: Default to use if key is not present. If unset, a
                :py:class:`KeyError` is raised when the key does not exist.
            type_hint: Type hint to use when converting.

        Returns:
            Memo value, converted with the type hint if present.

        Raises:
            KeyError: Key not present and default not set.
        """
        payload = self.raw_entry.memo.fields.get(key)
        if not payload:
            if default is temporalio.common._arg_unset:
                raise KeyError(f"Memo does not have a value for key {key}")
            return default
        return (
            await self.data_converter.decode(
                [payload], [type_hint] if type_hint else None
            )
        )[0]


@dataclass
class ScheduleListSchedule:
    """Details for a listed schedule."""

    action: ScheduleListAction
    """Action taken when scheduled."""

    spec: ScheduleSpec
    """When the action is taken."""

    state: ScheduleListState
    """State of the schedule."""

    @staticmethod
    def _from_proto(
        info: temporalio.api.schedule.v1.ScheduleListInfo,
    ) -> ScheduleListSchedule:
        # Only start workflow supported for now
        if not info.HasField("workflow_type"):
            raise ValueError("Unknown action on schedule")
        return ScheduleListSchedule(
            action=ScheduleListActionStartWorkflow(workflow=info.workflow_type.name),
            spec=ScheduleSpec._from_proto(info.spec),
            state=ScheduleListState._from_proto(info),
        )


class ScheduleListAction(ABC):
    """Base class for an action a listed schedule can take."""

    pass


@dataclass
class ScheduleListActionStartWorkflow(ScheduleListAction):
    """Action to start a workflow on a listed schedule."""

    workflow: str
    """Workflow type name."""


@dataclass
class ScheduleListInfo:
    """Information about a listed schedule."""

    recent_actions: Sequence[ScheduleActionResult]
    """Most recent actions, oldest first.

    This may be a smaller amount than present on
    :py:attr:`ScheduleDescription.info`.
    """

    next_action_times: Sequence[datetime]
    """Next scheduled action times.

    This may be a smaller amount than present on
    :py:attr:`ScheduleDescription.info`.
    """

    @staticmethod
    def _from_proto(
        info: temporalio.api.schedule.v1.ScheduleListInfo,
    ) -> ScheduleListInfo:
        return ScheduleListInfo(
            recent_actions=[
                ScheduleActionResult._from_proto(r) for r in info.recent_actions
            ],
            next_action_times=[
                f.ToDatetime().replace(tzinfo=timezone.utc)
                for f in info.future_action_times
            ],
        )


@dataclass
class ScheduleListState:
    """State of a listed schedule."""

    note: Optional[str]
    """Human readable message for the schedule.

    The system may overwrite this value on certain conditions like
    pause-on-failure.
    """

    paused: bool
    """Whether the schedule is paused."""

    @staticmethod
    def _from_proto(
        info: temporalio.api.schedule.v1.ScheduleListInfo,
    ) -> ScheduleListState:
        return ScheduleListState(
            note=info.notes or None,
            paused=info.paused,
        )


class ScheduleAsyncIterator:
    """Asynchronous iterator for :py:class:`ScheduleListDescription` values.

    Most users should use ``async for`` on this iterator and not call any of the
    methods within.
    """

    def __init__(
        self,
        client: Client,
        input: ListSchedulesInput,
    ) -> None:
        """Create an asynchronous iterator for the given input.

        Users should not create this directly, but rather use
        :py:meth:`Client.list_schedules`.
        """
        self._client = client
        self._input = input
        self._next_page_token = input.next_page_token
        self._current_page: Optional[Sequence[ScheduleListDescription]] = None
        self._current_page_index = 0

    @property
    def current_page_index(self) -> int:
        """Index of the entry in the current page that will be returned from
        the next :py:meth:`__anext__` call.
        """
        return self._current_page_index

    @property
    def current_page(self) -> Optional[Sequence[ScheduleListDescription]]:
        """Current page, if it has been fetched yet."""
        return self._current_page

    @property
    def next_page_token(self) -> Optional[bytes]:
        """Token for the next page request if any."""
        return self._next_page_token

    async def fetch_next_page(self, *, page_size: Optional[int] = None) -> None:
        """Fetch the next page if any.

        Args:
            page_size: Override the page size this iterator was originally
                created with.
        """
        resp = await self._client.workflow_service.list_schedules(
            temporalio.api.workflowservice.v1.ListSchedulesRequest(
                namespace=self._client.namespace,
                maximum_page_size=page_size or self._input.page_size,
                next_page_token=self._next_page_token or b"",
                query=self._input.query or "",
            ),
            retry=True,
            metadata=self._input.rpc_metadata,
            timeout=self._input.rpc_timeout,
        )
        self._current_page = [
            ScheduleListDescription._from_proto(v, self._client.data_converter)
            for v in resp.schedules
        ]
        self._current_page_index = 0
        self._next_page_token = resp.next_page_token or None

    def __aiter__(self) -> ScheduleAsyncIterator:
        """Return self as the iterator."""
        return self

    async def __anext__(self) -> ScheduleListDescription:
        """Get the next execution on this iterator, fetching next page if
        necessary.
        """
        while True:
            # No page? fetch and continue
            if self._current_page is None:
                await self.fetch_next_page()
                continue
            # No more left in page?
            if self._current_page_index >= len(self._current_page):
                # If there is a next page token, try to get another page and try
                # again
                if self._next_page_token is not None:
                    await self.fetch_next_page()
                    continue
                # No more pages means we're done
                raise StopAsyncIteration
            # Get current, increment page index, and return
            ret = self._current_page[self._current_page_index]
            self._current_page_index += 1
            return ret


class WorkflowUpdateHandle(Generic[LocalReturnType]):
    """Handle for a workflow update execution request."""

    def __init__(
        self,
        client: Client,
        id: str,
        workflow_id: str,
        *,
        workflow_run_id: Optional[str] = None,
        result_type: Optional[Type] = None,
        known_outcome: Optional[temporalio.api.update.v1.Outcome] = None,
    ):
        """Create a workflow update handle.

        Users should not create this directly, but rather use
        :py:meth:`WorkflowHandle.start_update` or :py:meth:`WorkflowHandle.get_update_handle`.
        """
        self._client = client
        self._id = id
        self._workflow_id = workflow_id
        self._workflow_run_id = workflow_run_id
        self._result_type = result_type
        self._known_outcome = known_outcome

    @property
    def id(self) -> str:
        """ID of this Update request."""
        return self._id

    @property
    def workflow_id(self) -> str:
        """The ID of the Workflow targeted by this Update."""
        return self._workflow_id

    @property
    def workflow_run_id(self) -> Optional[str]:
        """If specified, the specific run of the Workflow targeted by this Update."""
        return self._workflow_run_id

    async def result(
        self,
        *,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> LocalReturnType:
        """Wait for and return the result of the update. The result may already be known in which case no network call
        is made. Otherwise the result will be polled for until it is returned.

        Args:
            rpc_metadata: Headers used on the RPC call. Keys here override client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for each RPC call. Note: this is the timeout for each
                RPC call while polling, not a timeout for the function as a whole. If an individual RPC times out,
                it will be retried until the result is available.

        Raises:
            WorkflowUpdateFailedError: If the update failed.
            WorkflowUpdateRPCTimeoutOrCancelledError: This update call timed out
                or was cancelled. This doesn't mean the update itself was timed
                out or cancelled.
            RPCError: Update result could not be fetched for some other reason.
        """
        # Poll until outcome reached
        await self._poll_until_outcome(
            rpc_metadata=rpc_metadata, rpc_timeout=rpc_timeout
        )

        # Convert outcome to failure or value
        assert self._known_outcome
        if self._known_outcome.HasField("failure"):
            raise WorkflowUpdateFailedError(
                await self._client.data_converter.decode_failure(
                    self._known_outcome.failure
                ),
            )
        if not self._known_outcome.success.payloads:
            return None  # type: ignore
        type_hints = [self._result_type] if self._result_type else None
        results = await self._client.data_converter.decode(
            self._known_outcome.success.payloads, type_hints
        )
        if not results:
            return None  # type: ignore
        elif len(results) > 1:
            warnings.warn(f"Expected single update result, got {len(results)}")
        return results[0]

    async def _poll_until_outcome(
        self,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        if self._known_outcome:
            return
        req = temporalio.api.workflowservice.v1.PollWorkflowExecutionUpdateRequest(
            namespace=self._client.namespace,
            update_ref=temporalio.api.update.v1.UpdateRef(
                workflow_execution=temporalio.api.common.v1.WorkflowExecution(
                    workflow_id=self.workflow_id,
                    run_id=self.workflow_run_id or "",
                ),
                update_id=self.id,
            ),
            identity=self._client.identity,
            wait_policy=temporalio.api.update.v1.WaitPolicy(
                lifecycle_stage=temporalio.api.enums.v1.UpdateWorkflowExecutionLifecycleStage.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED
            ),
        )

        # Continue polling as long as we have no outcome
        while True:
            try:
                res = (
                    await self._client.workflow_service.poll_workflow_execution_update(
                        req,
                        retry=True,
                        metadata=rpc_metadata,
                        timeout=rpc_timeout,
                    )
                )
                if res.HasField("outcome"):
                    self._known_outcome = res.outcome
                    return
            except RPCError as err:
                if (
                    err.status == RPCStatusCode.DEADLINE_EXCEEDED
                    or err.status == RPCStatusCode.CANCELLED
                ):
                    raise WorkflowUpdateRPCTimeoutOrCancelledError() from err
                else:
                    raise
            except asyncio.CancelledError as err:
                raise WorkflowUpdateRPCTimeoutOrCancelledError() from err


class WorkflowUpdateStage(IntEnum):
    """Stage to wait for workflow update to reach before returning from
    ``start_update``.
    """

    ADMITTED = int(
        temporalio.api.enums.v1.UpdateWorkflowExecutionLifecycleStage.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED
    )
    ACCEPTED = int(
        temporalio.api.enums.v1.UpdateWorkflowExecutionLifecycleStage.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED
    )
    COMPLETED = int(
        temporalio.api.enums.v1.UpdateWorkflowExecutionLifecycleStage.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED
    )


class WorkflowFailureError(temporalio.exceptions.TemporalError):
    """Error that occurs when a workflow is unsuccessful."""

    def __init__(self, *, cause: BaseException) -> None:
        """Create workflow failure error."""
        super().__init__("Workflow execution failed")
        self.__cause__ = cause

    @property
    def cause(self) -> BaseException:
        """Cause of the workflow failure."""
        assert self.__cause__
        return self.__cause__


class WorkflowContinuedAsNewError(temporalio.exceptions.TemporalError):
    """Error that occurs when a workflow was continued as new."""

    def __init__(self, new_execution_run_id: str) -> None:
        """Create workflow continue as new error."""
        super().__init__("Workflow continued as new")
        self._new_execution_run_id = new_execution_run_id

    @property
    def new_execution_run_id(self) -> str:
        """New execution run ID the workflow continued to"""
        return self._new_execution_run_id


class WorkflowQueryRejectedError(temporalio.exceptions.TemporalError):
    """Error that occurs when a query was rejected."""

    def __init__(self, status: Optional[WorkflowExecutionStatus]) -> None:
        """Create workflow query rejected error."""
        super().__init__(f"Query rejected, status: {status}")
        self._status = status

    @property
    def status(self) -> Optional[WorkflowExecutionStatus]:
        """Get workflow execution status causing rejection."""
        return self._status


class WorkflowQueryFailedError(temporalio.exceptions.TemporalError):
    """Error that occurs when a query fails."""

    def __init__(self, message: str) -> None:
        """Create workflow query failed error."""
        super().__init__(message)
        self._message = message

    @property
    def message(self) -> str:
        """Get query failed message."""
        return self._message


class WorkflowUpdateFailedError(temporalio.exceptions.TemporalError):
    """Error that occurs when an update fails."""

    def __init__(self, cause: BaseException) -> None:
        """Create workflow update failed error."""
        super().__init__("Workflow update failed")
        self.__cause__ = cause

    @property
    def cause(self) -> BaseException:
        """Cause of the update failure."""
        assert self.__cause__
        return self.__cause__


class RPCTimeoutOrCancelledError(temporalio.exceptions.TemporalError):
    """Error that occurs on some client calls that timeout or get cancelled."""

    pass


class WorkflowUpdateRPCTimeoutOrCancelledError(RPCTimeoutOrCancelledError):
    """Error that occurs when update RPC call times out or is cancelled.

    Note, this is not related to any general concept of timing out or cancelling
    a running update, this is only related to the client call itself.
    """

    def __init__(self) -> None:
        """Create workflow update timeout or cancelled error."""
        super().__init__("Timeout or cancellation waiting for update")


class AsyncActivityCancelledError(temporalio.exceptions.TemporalError):
    """Error that occurs when async activity attempted heartbeat but was cancelled."""

    def __init__(self, details: Optional[ActivityCancellationDetails] = None) -> None:
        """Create async activity cancelled error."""
        super().__init__("Activity cancelled")
        self.details = details


class ScheduleAlreadyRunningError(temporalio.exceptions.TemporalError):
    """Error when a schedule is already running."""

    def __init__(self) -> None:
        """Create schedule already running error."""
        super().__init__("Schedule already running")


@dataclass
class StartWorkflowInput:
    """Input for :py:meth:`OutboundInterceptor.start_workflow`."""

    workflow: str
    args: Sequence[Any]
    id: str
    task_queue: str
    execution_timeout: Optional[timedelta]
    run_timeout: Optional[timedelta]
    task_timeout: Optional[timedelta]
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy
    id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy
    retry_policy: Optional[temporalio.common.RetryPolicy]
    cron_schedule: str
    memo: Optional[Mapping[str, Any]]
    search_attributes: Optional[
        Union[
            temporalio.common.SearchAttributes, temporalio.common.TypedSearchAttributes
        ]
    ]
    start_delay: Optional[timedelta]
    headers: Mapping[str, temporalio.api.common.v1.Payload]
    start_signal: Optional[str]
    start_signal_args: Sequence[Any]
    static_summary: Optional[str]
    static_details: Optional[str]
    # Type may be absent
    ret_type: Optional[Type]
    rpc_metadata: Mapping[str, str]
    rpc_timeout: Optional[timedelta]
    request_eager_start: bool
    priority: temporalio.common.Priority
    # The following options are experimental and unstable.
    callbacks: Sequence[Callback]
    workflow_event_links: Sequence[temporalio.api.common.v1.Link.WorkflowEvent]
    request_id: Optional[str]
    versioning_override: Optional[temporalio.common.VersioningOverride] = None


@dataclass
class CancelWorkflowInput:
    """Input for :py:meth:`OutboundInterceptor.cancel_workflow`."""

    id: str
    run_id: Optional[str]
    first_execution_run_id: Optional[str]
    rpc_metadata: Mapping[str, str]
    rpc_timeout: Optional[timedelta]


@dataclass
class DescribeWorkflowInput:
    """Input for :py:meth:`OutboundInterceptor.describe_workflow`."""

    id: str
    run_id: Optional[str]
    rpc_metadata: Mapping[str, str]
    rpc_timeout: Optional[timedelta]


@dataclass
class FetchWorkflowHistoryEventsInput:
    """Input for :py:meth:`OutboundInterceptor.fetch_workflow_history_events`."""

    id: str
    run_id: Optional[str]
    page_size: Optional[int]
    next_page_token: Optional[bytes]
    wait_new_event: bool
    event_filter_type: WorkflowHistoryEventFilterType
    skip_archival: bool
    rpc_metadata: Mapping[str, str]
    rpc_timeout: Optional[timedelta]


@dataclass
class ListWorkflowsInput:
    """Input for :py:meth:`OutboundInterceptor.list_workflows`."""

    query: Optional[str]
    page_size: int
    next_page_token: Optional[bytes]
    rpc_metadata: Mapping[str, str]
    rpc_timeout: Optional[timedelta]
    limit: Optional[int]


@dataclass
class CountWorkflowsInput:
    """Input for :py:meth:`OutboundInterceptor.count_workflows`."""

    query: Optional[str]
    rpc_metadata: Mapping[str, str]
    rpc_timeout: Optional[timedelta]


@dataclass
class QueryWorkflowInput:
    """Input for :py:meth:`OutboundInterceptor.query_workflow`."""

    id: str
    run_id: Optional[str]
    query: str
    args: Sequence[Any]
    reject_condition: Optional[temporalio.common.QueryRejectCondition]
    headers: Mapping[str, temporalio.api.common.v1.Payload]
    # Type may be absent
    ret_type: Optional[Type]
    rpc_metadata: Mapping[str, str]
    rpc_timeout: Optional[timedelta]


@dataclass
class SignalWorkflowInput:
    """Input for :py:meth:`OutboundInterceptor.signal_workflow`."""

    id: str
    run_id: Optional[str]
    signal: str
    args: Sequence[Any]
    headers: Mapping[str, temporalio.api.common.v1.Payload]
    rpc_metadata: Mapping[str, str]
    rpc_timeout: Optional[timedelta]


@dataclass
class TerminateWorkflowInput:
    """Input for :py:meth:`OutboundInterceptor.terminate_workflow`."""

    id: str
    run_id: Optional[str]
    first_execution_run_id: Optional[str]
    args: Sequence[Any]
    reason: Optional[str]
    rpc_metadata: Mapping[str, str]
    rpc_timeout: Optional[timedelta]


@dataclass
class StartWorkflowUpdateInput:
    """Input for :py:meth:`OutboundInterceptor.start_workflow_update`."""

    id: str
    run_id: Optional[str]
    first_execution_run_id: Optional[str]
    update_id: Optional[str]
    update: str
    args: Sequence[Any]
    wait_for_stage: WorkflowUpdateStage
    headers: Mapping[str, temporalio.api.common.v1.Payload]
    ret_type: Optional[Type]
    rpc_metadata: Mapping[str, str]
    rpc_timeout: Optional[timedelta]


@dataclass
class UpdateWithStartUpdateWorkflowInput:
    """Update input for :py:meth:`OutboundInterceptor.start_update_with_start_workflow`."""

    update_id: Optional[str]
    update: str
    args: Sequence[Any]
    wait_for_stage: WorkflowUpdateStage
    headers: Mapping[str, temporalio.api.common.v1.Payload]
    ret_type: Optional[Type]
    rpc_metadata: Mapping[str, str]
    rpc_timeout: Optional[timedelta]


@dataclass
class UpdateWithStartStartWorkflowInput:
    """StartWorkflow input for :py:meth:`OutboundInterceptor.start_update_with_start_workflow`."""

    # Similar to StartWorkflowInput but without e.g. run_id, start_signal,
    # start_signal_args, request_eager_start.

    workflow: str
    args: Sequence[Any]
    id: str
    task_queue: str
    execution_timeout: Optional[timedelta]
    run_timeout: Optional[timedelta]
    task_timeout: Optional[timedelta]
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy
    id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy
    retry_policy: Optional[temporalio.common.RetryPolicy]
    cron_schedule: str
    memo: Optional[Mapping[str, Any]]
    search_attributes: Optional[
        Union[
            temporalio.common.SearchAttributes, temporalio.common.TypedSearchAttributes
        ]
    ]
    start_delay: Optional[timedelta]
    headers: Mapping[str, temporalio.api.common.v1.Payload]
    static_summary: Optional[str]
    static_details: Optional[str]
    # Type may be absent
    ret_type: Optional[Type]
    rpc_metadata: Mapping[str, str]
    rpc_timeout: Optional[timedelta]
    priority: temporalio.common.Priority
    versioning_override: Optional[temporalio.common.VersioningOverride] = None


@dataclass
class StartWorkflowUpdateWithStartInput:
    """Input for :py:meth:`OutboundInterceptor.start_update_with_start_workflow`."""

    start_workflow_input: UpdateWithStartStartWorkflowInput
    update_workflow_input: UpdateWithStartUpdateWorkflowInput
    _on_start: Callable[
        [temporalio.api.workflowservice.v1.StartWorkflowExecutionResponse], None
    ]
    _on_start_error: Callable[[BaseException], None]


@dataclass
class HeartbeatAsyncActivityInput:
    """Input for :py:meth:`OutboundInterceptor.heartbeat_async_activity`."""

    id_or_token: Union[AsyncActivityIDReference, bytes]
    details: Sequence[Any]
    rpc_metadata: Mapping[str, str]
    rpc_timeout: Optional[timedelta]


@dataclass
class CompleteAsyncActivityInput:
    """Input for :py:meth:`OutboundInterceptor.complete_async_activity`."""

    id_or_token: Union[AsyncActivityIDReference, bytes]
    result: Optional[Any]
    rpc_metadata: Mapping[str, str]
    rpc_timeout: Optional[timedelta]


@dataclass
class FailAsyncActivityInput:
    """Input for :py:meth:`OutboundInterceptor.fail_async_activity`."""

    id_or_token: Union[AsyncActivityIDReference, bytes]
    error: Exception
    last_heartbeat_details: Sequence[Any]
    rpc_metadata: Mapping[str, str]
    rpc_timeout: Optional[timedelta]


@dataclass
class ReportCancellationAsyncActivityInput:
    """Input for :py:meth:`OutboundInterceptor.report_cancellation_async_activity`."""

    id_or_token: Union[AsyncActivityIDReference, bytes]
    details: Sequence[Any]
    rpc_metadata: Mapping[str, str]
    rpc_timeout: Optional[timedelta]


@dataclass
class CreateScheduleInput:
    """Input for :py:meth:`OutboundInterceptor.create_schedule`."""

    id: str
    schedule: Schedule
    trigger_immediately: bool
    backfill: Sequence[ScheduleBackfill]
    memo: Optional[Mapping[str, Any]]
    search_attributes: Optional[
        Union[
            temporalio.common.SearchAttributes, temporalio.common.TypedSearchAttributes
        ]
    ]
    rpc_metadata: Mapping[str, str]
    rpc_timeout: Optional[timedelta]


@dataclass
class ListSchedulesInput:
    """Input for :py:meth:`OutboundInterceptor.list_schedules`."""

    page_size: int
    next_page_token: Optional[bytes]
    rpc_metadata: Mapping[str, str]
    rpc_timeout: Optional[timedelta]
    query: Optional[str] = None


@dataclass
class BackfillScheduleInput:
    """Input for :py:meth:`OutboundInterceptor.backfill_schedule`."""

    id: str
    backfills: Sequence[ScheduleBackfill]
    rpc_metadata: Mapping[str, str]
    rpc_timeout: Optional[timedelta]


@dataclass
class DeleteScheduleInput:
    """Input for :py:meth:`OutboundInterceptor.delete_schedule`."""

    id: str
    rpc_metadata: Mapping[str, str]
    rpc_timeout: Optional[timedelta]


@dataclass
class DescribeScheduleInput:
    """Input for :py:meth:`OutboundInterceptor.describe_schedule`."""

    id: str
    rpc_metadata: Mapping[str, str]
    rpc_timeout: Optional[timedelta]


@dataclass
class PauseScheduleInput:
    """Input for :py:meth:`OutboundInterceptor.pause_schedule`."""

    id: str
    note: Optional[str]
    rpc_metadata: Mapping[str, str]
    rpc_timeout: Optional[timedelta]


@dataclass
class TriggerScheduleInput:
    """Input for :py:meth:`OutboundInterceptor.trigger_schedule`."""

    id: str
    overlap: Optional[ScheduleOverlapPolicy]
    rpc_metadata: Mapping[str, str]
    rpc_timeout: Optional[timedelta]


@dataclass
class UnpauseScheduleInput:
    """Input for :py:meth:`OutboundInterceptor.unpause_schedule`."""

    id: str
    note: Optional[str]
    rpc_metadata: Mapping[str, str]
    rpc_timeout: Optional[timedelta]


@dataclass
class UpdateScheduleInput:
    """Input for :py:meth:`OutboundInterceptor.update_schedule`."""

    id: str
    updater: Callable[
        [ScheduleUpdateInput],
        Union[Optional[ScheduleUpdate], Awaitable[Optional[ScheduleUpdate]]],
    ]
    rpc_metadata: Mapping[str, str]
    rpc_timeout: Optional[timedelta]


@dataclass
class UpdateWorkerBuildIdCompatibilityInput:
    """Input for :py:meth:`OutboundInterceptor.update_worker_build_id_compatibility`."""

    task_queue: str
    operation: BuildIdOp
    rpc_metadata: Mapping[str, str]
    rpc_timeout: Optional[timedelta]


@dataclass
class GetWorkerBuildIdCompatibilityInput:
    """Input for :py:meth:`OutboundInterceptor.get_worker_build_id_compatibility`."""

    task_queue: str
    max_sets: Optional[int]
    rpc_metadata: Mapping[str, str]
    rpc_timeout: Optional[timedelta]


@dataclass
class GetWorkerTaskReachabilityInput:
    """Input for :py:meth:`OutboundInterceptor.get_worker_task_reachability`."""

    build_ids: Sequence[str]
    task_queues: Sequence[str]
    reachability: Optional[TaskReachabilityType]
    rpc_metadata: Mapping[str, str]
    rpc_timeout: Optional[timedelta]


@dataclass
class Interceptor:
    """Interceptor for clients.

    This should be extended by any client interceptors.
    """

    def intercept_client(self, next: OutboundInterceptor) -> OutboundInterceptor:
        """Method called for intercepting a client.

        Args:
            next: The underlying outbound interceptor this interceptor should
                delegate to.

        Returns:
            The new interceptor that will be called for each client call.
        """
        return next


class OutboundInterceptor:
    """OutboundInterceptor for intercepting client calls.

    This should be extended by any client outbound interceptors.
    """

    def __init__(self, next: OutboundInterceptor) -> None:
        """Create the outbound interceptor.

        Args:
            next: The next interceptor in the chain. The default implementation
                of all calls is to delegate to the next interceptor.
        """
        self.next = next

    ### Workflow calls

    async def start_workflow(
        self, input: StartWorkflowInput
    ) -> WorkflowHandle[Any, Any]:
        """Called for every :py:meth:`Client.start_workflow` call."""
        return await self.next.start_workflow(input)

    async def cancel_workflow(self, input: CancelWorkflowInput) -> None:
        """Called for every :py:meth:`WorkflowHandle.cancel` call."""
        await self.next.cancel_workflow(input)

    async def describe_workflow(
        self, input: DescribeWorkflowInput
    ) -> WorkflowExecutionDescription:
        """Called for every :py:meth:`WorkflowHandle.describe` call."""
        return await self.next.describe_workflow(input)

    def fetch_workflow_history_events(
        self, input: FetchWorkflowHistoryEventsInput
    ) -> WorkflowHistoryEventAsyncIterator:
        """Called for every :py:meth:`WorkflowHandle.fetch_history_events` call."""
        return self.next.fetch_workflow_history_events(input)

    def list_workflows(
        self, input: ListWorkflowsInput
    ) -> WorkflowExecutionAsyncIterator:
        """Called for every :py:meth:`Client.list_workflows` call."""
        return self.next.list_workflows(input)

    async def count_workflows(
        self, input: CountWorkflowsInput
    ) -> WorkflowExecutionCount:
        """Called for every :py:meth:`Client.count_workflows` call."""
        return await self.next.count_workflows(input)

    async def query_workflow(self, input: QueryWorkflowInput) -> Any:
        """Called for every :py:meth:`WorkflowHandle.query` call."""
        return await self.next.query_workflow(input)

    async def signal_workflow(self, input: SignalWorkflowInput) -> None:
        """Called for every :py:meth:`WorkflowHandle.signal` call."""
        await self.next.signal_workflow(input)

    async def terminate_workflow(self, input: TerminateWorkflowInput) -> None:
        """Called for every :py:meth:`WorkflowHandle.terminate` call."""
        await self.next.terminate_workflow(input)

    async def start_workflow_update(
        self, input: StartWorkflowUpdateInput
    ) -> WorkflowUpdateHandle[Any]:
        """Called for every :py:meth:`WorkflowHandle.start_update` and :py:meth:`WorkflowHandle.execute_update` call."""
        return await self.next.start_workflow_update(input)

    async def start_update_with_start_workflow(
        self, input: StartWorkflowUpdateWithStartInput
    ) -> WorkflowUpdateHandle[Any]:
        """Called for every :py:meth:`Client.start_update_with_start_workflow` and :py:meth:`Client.execute_update_with_start_workflow` call."""
        return await self.next.start_update_with_start_workflow(input)

    ### Async activity calls

    async def heartbeat_async_activity(
        self, input: HeartbeatAsyncActivityInput
    ) -> None:
        """Called for every :py:meth:`AsyncActivityHandle.heartbeat` call."""
        await self.next.heartbeat_async_activity(input)

    async def complete_async_activity(self, input: CompleteAsyncActivityInput) -> None:
        """Called for every :py:meth:`AsyncActivityHandle.complete` call."""
        await self.next.complete_async_activity(input)

    async def fail_async_activity(self, input: FailAsyncActivityInput) -> None:
        """Called for every :py:meth:`AsyncActivityHandle.fail` call."""
        await self.next.fail_async_activity(input)

    async def report_cancellation_async_activity(
        self, input: ReportCancellationAsyncActivityInput
    ) -> None:
        """Called for every :py:meth:`AsyncActivityHandle.report_cancellation` call."""
        await self.next.report_cancellation_async_activity(input)

    ### Schedule calls

    async def create_schedule(self, input: CreateScheduleInput) -> ScheduleHandle:
        """Called for every :py:meth:`Client.create_schedule` call."""
        return await self.next.create_schedule(input)

    def list_schedules(self, input: ListSchedulesInput) -> ScheduleAsyncIterator:
        """Called for every :py:meth:`Client.list_schedules` call."""
        return self.next.list_schedules(input)

    async def backfill_schedule(self, input: BackfillScheduleInput) -> None:
        """Called for every :py:meth:`ScheduleHandle.backfill` call."""
        await self.next.backfill_schedule(input)

    async def delete_schedule(self, input: DeleteScheduleInput) -> None:
        """Called for every :py:meth:`ScheduleHandle.delete` call."""
        await self.next.delete_schedule(input)

    async def describe_schedule(
        self, input: DescribeScheduleInput
    ) -> ScheduleDescription:
        """Called for every :py:meth:`ScheduleHandle.describe` call."""
        return await self.next.describe_schedule(input)

    async def pause_schedule(self, input: PauseScheduleInput) -> None:
        """Called for every :py:meth:`ScheduleHandle.pause` call."""
        await self.next.pause_schedule(input)

    async def trigger_schedule(self, input: TriggerScheduleInput) -> None:
        """Called for every :py:meth:`ScheduleHandle.trigger` call."""
        await self.next.trigger_schedule(input)

    async def unpause_schedule(self, input: UnpauseScheduleInput) -> None:
        """Called for every :py:meth:`ScheduleHandle.unpause` call."""
        await self.next.unpause_schedule(input)

    async def update_schedule(self, input: UpdateScheduleInput) -> None:
        """Called for every :py:meth:`ScheduleHandle.update` call."""
        await self.next.update_schedule(input)

    async def update_worker_build_id_compatibility(
        self, input: UpdateWorkerBuildIdCompatibilityInput
    ) -> None:
        """Called for every :py:meth:`Client.update_worker_build_id_compatibility` call."""
        await self.next.update_worker_build_id_compatibility(input)

    async def get_worker_build_id_compatibility(
        self, input: GetWorkerBuildIdCompatibilityInput
    ) -> WorkerBuildIdVersionSets:
        """Called for every :py:meth:`Client.get_worker_build_id_compatibility` call."""
        return await self.next.get_worker_build_id_compatibility(input)

    async def get_worker_task_reachability(
        self, input: GetWorkerTaskReachabilityInput
    ) -> WorkerTaskReachability:
        """Called for every :py:meth:`Client.get_worker_task_reachability` call."""
        return await self.next.get_worker_task_reachability(input)


class _ClientImpl(OutboundInterceptor):
    def __init__(self, client: Client) -> None:  # type: ignore
        # We are intentionally not calling the base class's __init__ here
        self._client = client

    ### Workflow calls

    async def start_workflow(
        self, input: StartWorkflowInput
    ) -> WorkflowHandle[Any, Any]:
        req: Union[
            temporalio.api.workflowservice.v1.StartWorkflowExecutionRequest,
            temporalio.api.workflowservice.v1.SignalWithStartWorkflowExecutionRequest,
        ]
        if input.start_signal is not None:
            req = await self._build_signal_with_start_workflow_execution_request(input)
        else:
            req = await self._build_start_workflow_execution_request(input)

        resp: Union[
            temporalio.api.workflowservice.v1.StartWorkflowExecutionResponse,
            temporalio.api.workflowservice.v1.SignalWithStartWorkflowExecutionResponse,
        ]
        first_execution_run_id = None
        eagerly_started = False
        try:
            if isinstance(
                req,
                temporalio.api.workflowservice.v1.SignalWithStartWorkflowExecutionRequest,
            ):
                resp = await self._client.workflow_service.signal_with_start_workflow_execution(
                    req,
                    retry=True,
                    metadata=input.rpc_metadata,
                    timeout=input.rpc_timeout,
                )
            else:
                resp = await self._client.workflow_service.start_workflow_execution(
                    req,
                    retry=True,
                    metadata=input.rpc_metadata,
                    timeout=input.rpc_timeout,
                )
                first_execution_run_id = resp.run_id
                eagerly_started = resp.HasField("eager_workflow_task")
        except RPCError as err:
            # If the status is ALREADY_EXISTS and the details can be extracted
            # as already started, use a different exception
            if err.status == RPCStatusCode.ALREADY_EXISTS and err.grpc_status.details:
                details = temporalio.api.errordetails.v1.WorkflowExecutionAlreadyStartedFailure()
                if err.grpc_status.details[0].Unpack(details):
                    raise temporalio.exceptions.WorkflowAlreadyStartedError(
                        input.id, input.workflow, run_id=details.run_id
                    )
            raise
        handle: WorkflowHandle[Any, Any] = WorkflowHandle(
            self._client,
            req.workflow_id,
            result_run_id=resp.run_id,
            first_execution_run_id=first_execution_run_id,
            result_type=input.ret_type,
            start_workflow_response=resp,
        )
        setattr(handle, "__temporal_eagerly_started", eagerly_started)
        return handle

    async def _build_start_workflow_execution_request(
        self, input: StartWorkflowInput
    ) -> temporalio.api.workflowservice.v1.StartWorkflowExecutionRequest:
        req = temporalio.api.workflowservice.v1.StartWorkflowExecutionRequest()
        await self._populate_start_workflow_execution_request(req, input)
        # _populate_start_workflow_execution_request is used for both StartWorkflowInput
        # and UpdateWithStartStartWorkflowInput. UpdateWithStartStartWorkflowInput does
        # not have the following two fields so they are handled here.
        req.request_eager_execution = input.request_eager_start
        if input.request_id:
            req.request_id = input.request_id

        links = [
            temporalio.api.common.v1.Link(workflow_event=link)
            for link in input.workflow_event_links
        ]
        req.completion_callbacks.extend(
            temporalio.api.common.v1.Callback(
                nexus=temporalio.api.common.v1.Callback.Nexus(
                    url=callback.url,
                    header=callback.headers,
                ),
                links=links,
            )
            for callback in input.callbacks
        )
        # Links are duplicated on request for compatibility with older server versions.
        req.links.extend(links)
        return req

    async def _build_signal_with_start_workflow_execution_request(
        self, input: StartWorkflowInput
    ) -> temporalio.api.workflowservice.v1.SignalWithStartWorkflowExecutionRequest:
        assert input.start_signal
        req = temporalio.api.workflowservice.v1.SignalWithStartWorkflowExecutionRequest(
            signal_name=input.start_signal
        )
        if input.start_signal_args:
            req.signal_input.payloads.extend(
                await self._client.data_converter.encode(input.start_signal_args)
            )
        await self._populate_start_workflow_execution_request(req, input)
        return req

    async def _build_update_with_start_start_workflow_execution_request(
        self, input: UpdateWithStartStartWorkflowInput
    ) -> temporalio.api.workflowservice.v1.StartWorkflowExecutionRequest:
        req = temporalio.api.workflowservice.v1.StartWorkflowExecutionRequest()
        await self._populate_start_workflow_execution_request(req, input)
        return req

    async def _populate_start_workflow_execution_request(
        self,
        req: Union[
            temporalio.api.workflowservice.v1.StartWorkflowExecutionRequest,
            temporalio.api.workflowservice.v1.SignalWithStartWorkflowExecutionRequest,
        ],
        input: Union[StartWorkflowInput, UpdateWithStartStartWorkflowInput],
    ) -> None:
        req.namespace = self._client.namespace
        req.workflow_id = input.id
        req.workflow_type.name = input.workflow
        req.task_queue.name = input.task_queue
        if input.args:
            req.input.payloads.extend(
                await self._client.data_converter.encode(input.args)
            )
        if input.execution_timeout is not None:
            req.workflow_execution_timeout.FromTimedelta(input.execution_timeout)
        if input.run_timeout is not None:
            req.workflow_run_timeout.FromTimedelta(input.run_timeout)
        if input.task_timeout is not None:
            req.workflow_task_timeout.FromTimedelta(input.task_timeout)
        req.identity = self._client.identity
        req.request_id = str(uuid.uuid4())
        req.workflow_id_reuse_policy = cast(
            "temporalio.api.enums.v1.WorkflowIdReusePolicy.ValueType",
            int(input.id_reuse_policy),
        )
        req.workflow_id_conflict_policy = cast(
            "temporalio.api.enums.v1.WorkflowIdConflictPolicy.ValueType",
            int(input.id_conflict_policy),
        )
        if input.retry_policy is not None:
            input.retry_policy.apply_to_proto(req.retry_policy)
        req.cron_schedule = input.cron_schedule
        if input.memo is not None:
            for k, v in input.memo.items():
                req.memo.fields[k].CopyFrom(
                    (await self._client.data_converter.encode([v]))[0]
                )
        if input.search_attributes is not None:
            temporalio.converter.encode_search_attributes(
                input.search_attributes, req.search_attributes
            )
        metadata = await _encode_user_metadata(
            self._client.data_converter, input.static_summary, input.static_details
        )
        if metadata is not None:
            req.user_metadata.CopyFrom(metadata)
        if input.start_delay is not None:
            req.workflow_start_delay.FromTimedelta(input.start_delay)
        if input.headers is not None:
            await self._apply_headers(input.headers, req.header.fields)
        if input.priority is not None:
            req.priority.CopyFrom(input.priority._to_proto())
        if input.versioning_override is not None:
            req.versioning_override.CopyFrom(input.versioning_override._to_proto())

    async def cancel_workflow(self, input: CancelWorkflowInput) -> None:
        await self._client.workflow_service.request_cancel_workflow_execution(
            temporalio.api.workflowservice.v1.RequestCancelWorkflowExecutionRequest(
                namespace=self._client.namespace,
                workflow_execution=temporalio.api.common.v1.WorkflowExecution(
                    workflow_id=input.id,
                    run_id=input.run_id or "",
                ),
                identity=self._client.identity,
                request_id=str(uuid.uuid4()),
                first_execution_run_id=input.first_execution_run_id or "",
            ),
            retry=True,
            metadata=input.rpc_metadata,
            timeout=input.rpc_timeout,
        )

    async def describe_workflow(
        self, input: DescribeWorkflowInput
    ) -> WorkflowExecutionDescription:
        return await WorkflowExecutionDescription._from_raw_description(
            await self._client.workflow_service.describe_workflow_execution(
                temporalio.api.workflowservice.v1.DescribeWorkflowExecutionRequest(
                    namespace=self._client.namespace,
                    execution=temporalio.api.common.v1.WorkflowExecution(
                        workflow_id=input.id,
                        run_id=input.run_id or "",
                    ),
                ),
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            ),
            self._client.data_converter,
        )

    def fetch_workflow_history_events(
        self, input: FetchWorkflowHistoryEventsInput
    ) -> WorkflowHistoryEventAsyncIterator:
        return WorkflowHistoryEventAsyncIterator(self._client, input)

    def list_workflows(
        self, input: ListWorkflowsInput
    ) -> WorkflowExecutionAsyncIterator:
        return WorkflowExecutionAsyncIterator(self._client, input)

    async def count_workflows(
        self, input: CountWorkflowsInput
    ) -> WorkflowExecutionCount:
        return WorkflowExecutionCount._from_raw(
            await self._client.workflow_service.count_workflow_executions(
                temporalio.api.workflowservice.v1.CountWorkflowExecutionsRequest(
                    namespace=self._client.namespace,
                    query=input.query or "",
                ),
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            )
        )

    async def query_workflow(self, input: QueryWorkflowInput) -> Any:
        req = temporalio.api.workflowservice.v1.QueryWorkflowRequest(
            namespace=self._client.namespace,
            execution=temporalio.api.common.v1.WorkflowExecution(
                workflow_id=input.id,
                run_id=input.run_id or "",
            ),
        )
        if input.reject_condition:
            req.query_reject_condition = cast(
                "temporalio.api.enums.v1.QueryRejectCondition.ValueType",
                int(input.reject_condition),
            )
        req.query.query_type = input.query
        if input.args:
            req.query.query_args.payloads.extend(
                await self._client.data_converter.encode(input.args)
            )
        if input.headers is not None:
            await self._apply_headers(input.headers, req.query.header.fields)
        try:
            resp = await self._client.workflow_service.query_workflow(
                req, retry=True, metadata=input.rpc_metadata, timeout=input.rpc_timeout
            )
        except RPCError as err:
            # If the status is INVALID_ARGUMENT, we can assume it's a query
            # failed error
            if err.status == RPCStatusCode.INVALID_ARGUMENT:
                raise WorkflowQueryFailedError(err.message)
            else:
                raise
        if resp.HasField("query_rejected"):
            raise WorkflowQueryRejectedError(
                WorkflowExecutionStatus(resp.query_rejected.status)
                if resp.query_rejected.status
                else None
            )
        if not resp.query_result.payloads:
            return None
        type_hints = [input.ret_type] if input.ret_type else None
        results = await self._client.data_converter.decode(
            resp.query_result.payloads, type_hints
        )
        if not results:
            return None
        elif len(results) > 1:
            warnings.warn(f"Expected single query result, got {len(results)}")
        return results[0]

    async def signal_workflow(self, input: SignalWorkflowInput) -> None:
        req = temporalio.api.workflowservice.v1.SignalWorkflowExecutionRequest(
            namespace=self._client.namespace,
            workflow_execution=temporalio.api.common.v1.WorkflowExecution(
                workflow_id=input.id,
                run_id=input.run_id or "",
            ),
            signal_name=input.signal,
            identity=self._client.identity,
            request_id=str(uuid.uuid4()),
        )
        if input.args:
            req.input.payloads.extend(
                await self._client.data_converter.encode(input.args)
            )
        if input.headers is not None:
            await self._apply_headers(input.headers, req.header.fields)
        await self._client.workflow_service.signal_workflow_execution(
            req, retry=True, metadata=input.rpc_metadata, timeout=input.rpc_timeout
        )

    async def terminate_workflow(self, input: TerminateWorkflowInput) -> None:
        req = temporalio.api.workflowservice.v1.TerminateWorkflowExecutionRequest(
            namespace=self._client.namespace,
            workflow_execution=temporalio.api.common.v1.WorkflowExecution(
                workflow_id=input.id,
                run_id=input.run_id or "",
            ),
            reason=input.reason or "",
            identity=self._client.identity,
            first_execution_run_id=input.first_execution_run_id or "",
        )
        if input.args:
            req.details.payloads.extend(
                await self._client.data_converter.encode(input.args)
            )
        await self._client.workflow_service.terminate_workflow_execution(
            req, retry=True, metadata=input.rpc_metadata, timeout=input.rpc_timeout
        )

    async def start_workflow_update(
        self, input: StartWorkflowUpdateInput
    ) -> WorkflowUpdateHandle[Any]:
        workflow_id = input.id
        req = await self._build_update_workflow_execution_request(input, workflow_id)

        # Repeatedly try to invoke UpdateWorkflowExecution until the update is durable.
        resp: temporalio.api.workflowservice.v1.UpdateWorkflowExecutionResponse
        while True:
            try:
                resp = await self._client.workflow_service.update_workflow_execution(
                    req,
                    retry=True,
                    metadata=input.rpc_metadata,
                    timeout=input.rpc_timeout,
                )
            except RPCError as err:
                if (
                    err.status == RPCStatusCode.DEADLINE_EXCEEDED
                    or err.status == RPCStatusCode.CANCELLED
                ):
                    raise WorkflowUpdateRPCTimeoutOrCancelledError() from err
                else:
                    raise
            except asyncio.CancelledError as err:
                raise WorkflowUpdateRPCTimeoutOrCancelledError() from err
            if (
                resp.stage
                >= temporalio.api.enums.v1.UpdateWorkflowExecutionLifecycleStage.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED
            ):
                break

        # Build the handle. If the user's wait stage is COMPLETED, make sure we
        # poll for result.
        handle: WorkflowUpdateHandle[Any] = WorkflowUpdateHandle(
            client=self._client,
            id=req.request.meta.update_id,
            workflow_id=workflow_id,
            workflow_run_id=resp.update_ref.workflow_execution.run_id,
            result_type=input.ret_type,
        )
        if resp.HasField("outcome"):
            handle._known_outcome = resp.outcome
        if input.wait_for_stage == WorkflowUpdateStage.COMPLETED:
            await handle._poll_until_outcome()
        return handle

    async def _build_update_workflow_execution_request(
        self,
        input: Union[StartWorkflowUpdateInput, UpdateWithStartUpdateWorkflowInput],
        workflow_id: str,
    ) -> temporalio.api.workflowservice.v1.UpdateWorkflowExecutionRequest:
        run_id, first_execution_run_id = (
            (
                input.run_id,
                input.first_execution_run_id,
            )
            if isinstance(input, StartWorkflowUpdateInput)
            else (None, None)
        )
        req = temporalio.api.workflowservice.v1.UpdateWorkflowExecutionRequest(
            namespace=self._client.namespace,
            workflow_execution=temporalio.api.common.v1.WorkflowExecution(
                workflow_id=workflow_id,
                run_id=run_id or "",
            ),
            first_execution_run_id=first_execution_run_id or "",
            request=temporalio.api.update.v1.Request(
                meta=temporalio.api.update.v1.Meta(
                    update_id=input.update_id or str(uuid.uuid4()),
                    identity=self._client.identity,
                ),
                input=temporalio.api.update.v1.Input(
                    name=input.update,
                ),
            ),
            wait_policy=temporalio.api.update.v1.WaitPolicy(
                lifecycle_stage=temporalio.api.enums.v1.UpdateWorkflowExecutionLifecycleStage.ValueType(
                    input.wait_for_stage
                )
            ),
        )
        if input.args:
            req.request.input.args.payloads.extend(
                await self._client.data_converter.encode(input.args)
            )
        if input.headers is not None:
            await self._apply_headers(input.headers, req.request.input.header.fields)
        return req

    async def start_update_with_start_workflow(
        self, input: StartWorkflowUpdateWithStartInput
    ) -> WorkflowUpdateHandle[Any]:
        seen_start = False

        def on_start(
            start_response: temporalio.api.workflowservice.v1.StartWorkflowExecutionResponse,
        ):
            nonlocal seen_start
            if not seen_start:
                input._on_start(start_response)
                seen_start = True

        err: Optional[BaseException] = None

        try:
            return await self._start_workflow_update_with_start(
                input.start_workflow_input, input.update_workflow_input, on_start
            )
        except asyncio.CancelledError as _err:
            err = _err
            raise WorkflowUpdateRPCTimeoutOrCancelledError() from err
        except RPCError as _err:
            err = _err
            if err.status in [
                RPCStatusCode.DEADLINE_EXCEEDED,
                RPCStatusCode.CANCELLED,
            ]:
                raise WorkflowUpdateRPCTimeoutOrCancelledError() from err
            else:
                multiop_failure = (
                    temporalio.api.errordetails.v1.MultiOperationExecutionFailure()
                )
                if err.grpc_status.details and err.grpc_status.details[0].Unpack(
                    multiop_failure
                ):
                    status = next(
                        (
                            st
                            for st in multiop_failure.statuses
                            if (
                                st.code != RPCStatusCode.OK
                                and not (
                                    st.details
                                    and st.details[0].Is(
                                        temporalio.api.failure.v1.MultiOperationExecutionAborted.DESCRIPTOR
                                    )
                                )
                            )
                        ),
                        None,
                    )
                    if status and status.code in list(RPCStatusCode):
                        if (
                            status.code == RPCStatusCode.ALREADY_EXISTS
                            and status.details
                        ):
                            details = temporalio.api.errordetails.v1.WorkflowExecutionAlreadyStartedFailure()
                            if status.details[0].Unpack(details):
                                err = temporalio.exceptions.WorkflowAlreadyStartedError(
                                    input.start_workflow_input.id,
                                    input.start_workflow_input.workflow,
                                    run_id=details.run_id,
                                )
                        else:
                            err = RPCError(
                                status.message,
                                RPCStatusCode(status.code),
                                err.raw_grpc_status,
                            )
                raise err
        finally:
            if err and not seen_start:
                input._on_start_error(err)

    async def _start_workflow_update_with_start(
        self,
        start_input: UpdateWithStartStartWorkflowInput,
        update_input: UpdateWithStartUpdateWorkflowInput,
        on_start: Callable[
            [temporalio.api.workflowservice.v1.StartWorkflowExecutionResponse], None
        ],
    ) -> WorkflowUpdateHandle[Any]:
        start_req = (
            await self._build_update_with_start_start_workflow_execution_request(
                start_input
            )
        )
        update_req = await self._build_update_workflow_execution_request(
            update_input, workflow_id=start_input.id
        )
        multiop_req = temporalio.api.workflowservice.v1.ExecuteMultiOperationRequest(
            namespace=self._client.namespace,
            operations=[
                temporalio.api.workflowservice.v1.ExecuteMultiOperationRequest.Operation(
                    start_workflow=start_req
                ),
                temporalio.api.workflowservice.v1.ExecuteMultiOperationRequest.Operation(
                    update_workflow=update_req
                ),
            ],
        )

        # Repeatedly try to invoke ExecuteMultiOperation until the update is durable
        while True:
            multiop_response = (
                await self._client.workflow_service.execute_multi_operation(multiop_req)
            )
            start_response = multiop_response.responses[0].start_workflow
            update_response = multiop_response.responses[1].update_workflow
            on_start(start_response)
            known_outcome = (
                update_response.outcome if update_response.HasField("outcome") else None
            )
            if (
                update_response.stage
                >= temporalio.api.enums.v1.UpdateWorkflowExecutionLifecycleStage.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED
            ):
                break

        handle: WorkflowUpdateHandle[Any] = WorkflowUpdateHandle(
            client=self._client,
            id=update_req.request.meta.update_id,
            workflow_id=start_input.id,
            workflow_run_id=start_response.run_id,
            known_outcome=known_outcome,
            result_type=update_input.ret_type,
        )
        if update_input.wait_for_stage == WorkflowUpdateStage.COMPLETED:
            await handle._poll_until_outcome()

        return handle

    ### Async activity calls

    async def heartbeat_async_activity(
        self, input: HeartbeatAsyncActivityInput
    ) -> None:
        details = (
            None
            if not input.details
            else await self._client.data_converter.encode_wrapper(input.details)
        )
        if isinstance(input.id_or_token, AsyncActivityIDReference):
            resp_by_id = await self._client.workflow_service.record_activity_task_heartbeat_by_id(
                temporalio.api.workflowservice.v1.RecordActivityTaskHeartbeatByIdRequest(
                    workflow_id=input.id_or_token.workflow_id,
                    run_id=input.id_or_token.run_id or "",
                    activity_id=input.id_or_token.activity_id,
                    namespace=self._client.namespace,
                    identity=self._client.identity,
                    details=details,
                ),
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            )
            if resp_by_id.cancel_requested or resp_by_id.activity_paused:
                raise AsyncActivityCancelledError(
                    details=ActivityCancellationDetails(
                        cancel_requested=resp_by_id.cancel_requested,
                        paused=resp_by_id.activity_paused,
                    )
                )

        else:
            resp = await self._client.workflow_service.record_activity_task_heartbeat(
                temporalio.api.workflowservice.v1.RecordActivityTaskHeartbeatRequest(
                    task_token=input.id_or_token,
                    namespace=self._client.namespace,
                    identity=self._client.identity,
                    details=details,
                ),
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            )
            if resp.cancel_requested or resp.activity_paused:
                raise AsyncActivityCancelledError(
                    details=ActivityCancellationDetails(
                        cancel_requested=resp.cancel_requested,
                        paused=resp.activity_paused,
                    )
                )

    async def complete_async_activity(self, input: CompleteAsyncActivityInput) -> None:
        result = (
            None
            if input.result is temporalio.common._arg_unset
            else await self._client.data_converter.encode_wrapper([input.result])
        )
        if isinstance(input.id_or_token, AsyncActivityIDReference):
            await self._client.workflow_service.respond_activity_task_completed_by_id(
                temporalio.api.workflowservice.v1.RespondActivityTaskCompletedByIdRequest(
                    workflow_id=input.id_or_token.workflow_id,
                    run_id=input.id_or_token.run_id or "",
                    activity_id=input.id_or_token.activity_id,
                    namespace=self._client.namespace,
                    identity=self._client.identity,
                    result=result,
                ),
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            )
        else:
            await self._client.workflow_service.respond_activity_task_completed(
                temporalio.api.workflowservice.v1.RespondActivityTaskCompletedRequest(
                    task_token=input.id_or_token,
                    namespace=self._client.namespace,
                    identity=self._client.identity,
                    result=result,
                ),
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            )

    async def fail_async_activity(self, input: FailAsyncActivityInput) -> None:
        failure = temporalio.api.failure.v1.Failure()
        await self._client.data_converter.encode_failure(input.error, failure)
        last_heartbeat_details = (
            None
            if not input.last_heartbeat_details
            else await self._client.data_converter.encode_wrapper(
                input.last_heartbeat_details
            )
        )
        if isinstance(input.id_or_token, AsyncActivityIDReference):
            await self._client.workflow_service.respond_activity_task_failed_by_id(
                temporalio.api.workflowservice.v1.RespondActivityTaskFailedByIdRequest(
                    workflow_id=input.id_or_token.workflow_id,
                    run_id=input.id_or_token.run_id or "",
                    activity_id=input.id_or_token.activity_id,
                    namespace=self._client.namespace,
                    identity=self._client.identity,
                    failure=failure,
                    last_heartbeat_details=last_heartbeat_details,
                ),
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            )
        else:
            await self._client.workflow_service.respond_activity_task_failed(
                temporalio.api.workflowservice.v1.RespondActivityTaskFailedRequest(
                    task_token=input.id_or_token,
                    namespace=self._client.namespace,
                    identity=self._client.identity,
                    failure=failure,
                    last_heartbeat_details=last_heartbeat_details,
                ),
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            )

    async def report_cancellation_async_activity(
        self, input: ReportCancellationAsyncActivityInput
    ) -> None:
        details = (
            None
            if not input.details
            else await self._client.data_converter.encode_wrapper(input.details)
        )
        if isinstance(input.id_or_token, AsyncActivityIDReference):
            await self._client.workflow_service.respond_activity_task_canceled_by_id(
                temporalio.api.workflowservice.v1.RespondActivityTaskCanceledByIdRequest(
                    workflow_id=input.id_or_token.workflow_id,
                    run_id=input.id_or_token.run_id or "",
                    activity_id=input.id_or_token.activity_id,
                    namespace=self._client.namespace,
                    identity=self._client.identity,
                    details=details,
                ),
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            )
        else:
            await self._client.workflow_service.respond_activity_task_canceled(
                temporalio.api.workflowservice.v1.RespondActivityTaskCanceledRequest(
                    task_token=input.id_or_token,
                    namespace=self._client.namespace,
                    identity=self._client.identity,
                    details=details,
                ),
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            )

    ### Schedule calls

    async def create_schedule(self, input: CreateScheduleInput) -> ScheduleHandle:
        # Limited actions must be false if remaining actions is 0 and must be
        # true if remaining actions is non-zero
        if (
            input.schedule.state.limited_actions
            and not input.schedule.state.remaining_actions
        ):
            raise ValueError(
                "Must set limited actions to false if there are no remaining actions set"
            )
        if (
            not input.schedule.state.limited_actions
            and input.schedule.state.remaining_actions
        ):
            raise ValueError(
                "Must set limited actions to true if there are remaining actions set"
            )

        initial_patch: Optional[temporalio.api.schedule.v1.SchedulePatch] = None
        if input.trigger_immediately or input.backfill:
            initial_patch = temporalio.api.schedule.v1.SchedulePatch(
                trigger_immediately=temporalio.api.schedule.v1.TriggerImmediatelyRequest(
                    overlap_policy=temporalio.api.enums.v1.ScheduleOverlapPolicy.ValueType(
                        input.schedule.policy.overlap
                    ),
                )
                if input.trigger_immediately
                else None,
                backfill_request=[b._to_proto() for b in input.backfill]
                if input.backfill
                else None,
            )
        try:
            request = temporalio.api.workflowservice.v1.CreateScheduleRequest(
                namespace=self._client.namespace,
                schedule_id=input.id,
                schedule=await input.schedule._to_proto(self._client),
                initial_patch=initial_patch,
                identity=self._client.identity,
                request_id=str(uuid.uuid4()),
                memo=None
                if not input.memo
                else temporalio.api.common.v1.Memo(
                    fields={
                        k: (await self._client.data_converter.encode([v]))[0]
                        for k, v in input.memo.items()
                    },
                ),
            )
            if input.search_attributes:
                temporalio.converter.encode_search_attributes(
                    input.search_attributes, request.search_attributes
                )
            await self._client.workflow_service.create_schedule(
                request,
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            )
        except RPCError as err:
            already_started = (
                err.status == RPCStatusCode.ALREADY_EXISTS
                and err.grpc_status.details
                and err.grpc_status.details[0].Is(
                    temporalio.api.errordetails.v1.WorkflowExecutionAlreadyStartedFailure.DESCRIPTOR
                )
            )
            if already_started:
                raise ScheduleAlreadyRunningError()
            raise
        return ScheduleHandle(self._client, input.id)

    def list_schedules(self, input: ListSchedulesInput) -> ScheduleAsyncIterator:
        return ScheduleAsyncIterator(self._client, input)

    async def backfill_schedule(self, input: BackfillScheduleInput) -> None:
        await self._client.workflow_service.patch_schedule(
            temporalio.api.workflowservice.v1.PatchScheduleRequest(
                namespace=self._client.namespace,
                schedule_id=input.id,
                patch=temporalio.api.schedule.v1.SchedulePatch(
                    backfill_request=[b._to_proto() for b in input.backfills],
                ),
                identity=self._client.identity,
                request_id=str(uuid.uuid4()),
            ),
            retry=True,
            metadata=input.rpc_metadata,
            timeout=input.rpc_timeout,
        )

    async def delete_schedule(self, input: DeleteScheduleInput) -> None:
        await self._client.workflow_service.delete_schedule(
            temporalio.api.workflowservice.v1.DeleteScheduleRequest(
                namespace=self._client.namespace,
                schedule_id=input.id,
                identity=self._client.identity,
            ),
            retry=True,
            metadata=input.rpc_metadata,
            timeout=input.rpc_timeout,
        )

    async def describe_schedule(
        self, input: DescribeScheduleInput
    ) -> ScheduleDescription:
        return ScheduleDescription._from_proto(
            input.id,
            await self._client.workflow_service.describe_schedule(
                temporalio.api.workflowservice.v1.DescribeScheduleRequest(
                    namespace=self._client.namespace,
                    schedule_id=input.id,
                ),
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            ),
            self._client.data_converter,
        )

    async def pause_schedule(self, input: PauseScheduleInput) -> None:
        await self._client.workflow_service.patch_schedule(
            temporalio.api.workflowservice.v1.PatchScheduleRequest(
                namespace=self._client.namespace,
                schedule_id=input.id,
                patch=temporalio.api.schedule.v1.SchedulePatch(
                    pause=input.note or "Paused via Python SDK",
                ),
                identity=self._client.identity,
                request_id=str(uuid.uuid4()),
            ),
            retry=True,
            metadata=input.rpc_metadata,
            timeout=input.rpc_timeout,
        )

    async def trigger_schedule(self, input: TriggerScheduleInput) -> None:
        overlap_policy = temporalio.api.enums.v1.ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED
        if input.overlap:
            overlap_policy = temporalio.api.enums.v1.ScheduleOverlapPolicy.ValueType(
                input.overlap
            )
        await self._client.workflow_service.patch_schedule(
            temporalio.api.workflowservice.v1.PatchScheduleRequest(
                namespace=self._client.namespace,
                schedule_id=input.id,
                patch=temporalio.api.schedule.v1.SchedulePatch(
                    trigger_immediately=temporalio.api.schedule.v1.TriggerImmediatelyRequest(
                        overlap_policy=overlap_policy,
                    ),
                ),
                identity=self._client.identity,
                request_id=str(uuid.uuid4()),
            ),
            retry=True,
            metadata=input.rpc_metadata,
            timeout=input.rpc_timeout,
        )

    async def unpause_schedule(self, input: UnpauseScheduleInput) -> None:
        await self._client.workflow_service.patch_schedule(
            temporalio.api.workflowservice.v1.PatchScheduleRequest(
                namespace=self._client.namespace,
                schedule_id=input.id,
                patch=temporalio.api.schedule.v1.SchedulePatch(
                    unpause=input.note or "Unpaused via Python SDK",
                ),
                identity=self._client.identity,
                request_id=str(uuid.uuid4()),
            ),
            retry=True,
            metadata=input.rpc_metadata,
            timeout=input.rpc_timeout,
        )

    async def update_schedule(self, input: UpdateScheduleInput) -> None:
        # TODO(cretz): This is supposed to be a retry-conflict loop, but we do
        # not yet have a way to know update failure is due to conflict token
        # mismatch
        update = input.updater(
            ScheduleUpdateInput(
                description=ScheduleDescription._from_proto(
                    input.id,
                    await self._client.workflow_service.describe_schedule(
                        temporalio.api.workflowservice.v1.DescribeScheduleRequest(
                            namespace=self._client.namespace,
                            schedule_id=input.id,
                        ),
                        retry=True,
                        metadata=input.rpc_metadata,
                        timeout=input.rpc_timeout,
                    ),
                    self._client.data_converter,
                )
            )
        )
        if inspect.iscoroutine(update):
            update = await update
        if not update:
            return
        assert isinstance(update, ScheduleUpdate)
        request = temporalio.api.workflowservice.v1.UpdateScheduleRequest(
            namespace=self._client.namespace,
            schedule_id=input.id,
            schedule=await update.schedule._to_proto(self._client),
            identity=self._client.identity,
            request_id=str(uuid.uuid4()),
        )
        if update.search_attributes is not None:
            request.search_attributes.indexed_fields.clear()  # Ensure that we at least create an empty map
            temporalio.converter.encode_search_attributes(
                update.search_attributes, request.search_attributes
            )
        await self._client.workflow_service.update_schedule(
            request,
            retry=True,
            metadata=input.rpc_metadata,
            timeout=input.rpc_timeout,
        )

    async def update_worker_build_id_compatibility(
        self, input: UpdateWorkerBuildIdCompatibilityInput
    ) -> None:
        req = input.operation._as_partial_proto()
        req.namespace = self._client.namespace
        req.task_queue = input.task_queue
        await self._client.workflow_service.update_worker_build_id_compatibility(
            req, retry=True, metadata=input.rpc_metadata, timeout=input.rpc_timeout
        )

    async def get_worker_build_id_compatibility(
        self, input: GetWorkerBuildIdCompatibilityInput
    ) -> WorkerBuildIdVersionSets:
        req = temporalio.api.workflowservice.v1.GetWorkerBuildIdCompatibilityRequest(
            namespace=self._client.namespace,
            task_queue=input.task_queue,
            max_sets=input.max_sets or 0,
        )
        resp = await self._client.workflow_service.get_worker_build_id_compatibility(
            req, retry=True, metadata=input.rpc_metadata, timeout=input.rpc_timeout
        )
        return WorkerBuildIdVersionSets._from_proto(resp)

    async def get_worker_task_reachability(
        self, input: GetWorkerTaskReachabilityInput
    ) -> WorkerTaskReachability:
        req = temporalio.api.workflowservice.v1.GetWorkerTaskReachabilityRequest(
            namespace=self._client.namespace,
            build_ids=input.build_ids,
            task_queues=input.task_queues,
            reachability=input.reachability._to_proto()
            if input.reachability
            else temporalio.api.enums.v1.TaskReachability.TASK_REACHABILITY_UNSPECIFIED,
        )
        resp = await self._client.workflow_service.get_worker_task_reachability(
            req, retry=True, metadata=input.rpc_metadata, timeout=input.rpc_timeout
        )
        return WorkerTaskReachability._from_proto(resp)

    async def _apply_headers(
        self,
        source: Optional[Mapping[str, temporalio.api.common.v1.Payload]],
        dest: MessageMap[Text, temporalio.api.common.v1.Payload],
    ) -> None:
        await _apply_headers(
            source,
            dest,
            self._client.config()["header_codec_behavior"] == HeaderCodecBehavior.CODEC,
            self._client.data_converter.payload_codec,
        )


async def _apply_headers(
    source: Optional[Mapping[str, temporalio.api.common.v1.Payload]],
    dest: MessageMap[Text, temporalio.api.common.v1.Payload],
    encode_headers: bool,
    codec: Optional[temporalio.converter.PayloadCodec],
) -> None:
    if source is None:
        return
    if encode_headers and codec is not None:
        for payload in source.values():
            new_payload = (await codec.encode([payload]))[0]
            payload.CopyFrom(new_payload)
    temporalio.common._apply_headers(source, dest)


def _history_from_json(
    history: Union[str, Dict[str, Any]],
) -> temporalio.api.history.v1.History:
    if isinstance(history, str):
        history = json.loads(history)
    else:
        # Copy the dict so we can mutate it
        history = copy.deepcopy(history)
    if not isinstance(history, dict):
        raise ValueError("JSON history not a dictionary")
    events = history.get("events")
    if not isinstance(events, Iterable):
        raise ValueError("History does not have iterable 'events'")
    for event in events:
        if not isinstance(event, dict):
            raise ValueError("Event not a dictionary")
        _fix_history_enum(
            "CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE",
            event,
            "requestCancelExternalWorkflowExecutionFailedEventAttributes",
            "cause",
        )
        _fix_history_enum("CONTINUE_AS_NEW_INITIATOR", event, "*", "initiator")
        _fix_history_enum("EVENT_TYPE", event, "eventType")
        _fix_history_enum(
            "PARENT_CLOSE_POLICY",
            event,
            "startChildWorkflowExecutionInitiatedEventAttributes",
            "parentClosePolicy",
        )
        _fix_history_enum("RETRY_STATE", event, "*", "retryState")
        _fix_history_enum(
            "SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE",
            event,
            "signalExternalWorkflowExecutionFailedEventAttributes",
            "cause",
        )
        _fix_history_enum(
            "START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE",
            event,
            "startChildWorkflowExecutionFailedEventAttributes",
            "cause",
        )
        _fix_history_enum("TASK_QUEUE_KIND", event, "*", "taskQueue", "kind")
        _fix_history_enum(
            "TIMEOUT_TYPE",
            event,
            "workflowTaskTimedOutEventAttributes",
            "timeoutType",
        )
        _fix_history_enum(
            "WORKFLOW_ID_REUSE_POLICY",
            event,
            "startChildWorkflowExecutionInitiatedEventAttributes",
            "workflowIdReusePolicy",
        )
        _fix_history_enum(
            "WORKFLOW_TASK_FAILED_CAUSE",
            event,
            "workflowTaskFailedEventAttributes",
            "cause",
        )
        _fix_history_failure(event, "*", "failure")
        _fix_history_failure(event, "activityTaskStartedEventAttributes", "lastFailure")
        _fix_history_failure(
            event, "workflowExecutionStartedEventAttributes", "continuedFailure"
        )
    return google.protobuf.json_format.ParseDict(
        history, temporalio.api.history.v1.History(), ignore_unknown_fields=True
    )


_pascal_case_match = re.compile("([A-Z]+)")


def _fix_history_failure(parent: Dict[str, Any], *attrs: str) -> None:
    _fix_history_enum(
        "TIMEOUT_TYPE", parent, *attrs, "timeoutFailureInfo", "timeoutType"
    )
    _fix_history_enum("RETRY_STATE", parent, *attrs, "*", "retryState")
    # Recurse into causes. First collect all failure parents.
    parents = [parent]
    for attr in attrs:
        new_parents = []
        for parent in parents:
            if attr == "*":
                for v in parent.values():
                    if isinstance(v, dict):
                        new_parents.append(v)
            else:
                child = parent.get(attr)
                if isinstance(child, dict):
                    new_parents.append(child)
        if not new_parents:
            return
        parents = new_parents
    # Fix each
    for parent in parents:
        _fix_history_failure(parent, "cause")


def _fix_history_enum(prefix: str, parent: Dict[str, Any], *attrs: str) -> None:
    # If the attr is "*", we need to handle all dict children
    if attrs[0] == "*":
        for child in parent.values():
            if isinstance(child, dict):
                _fix_history_enum(prefix, child, *attrs[1:])
    else:
        child = parent.get(attrs[0])
        if isinstance(child, str) and len(attrs) == 1:
            # We only fix it if it doesn't already have the prefix
            if not parent[attrs[0]].startswith(prefix):
                parent[attrs[0]] = (
                    prefix + _pascal_case_match.sub(r"_\1", child).upper()
                )
        elif isinstance(child, dict) and len(attrs) > 1:
            _fix_history_enum(prefix, child, *attrs[1:])
        elif isinstance(child, list) and len(attrs) > 1:
            for child_item in child:
                if isinstance(child_item, dict):
                    _fix_history_enum(prefix, child_item, *attrs[1:])


@dataclass(frozen=True)
class WorkerBuildIdVersionSets:
    """Represents the sets of compatible Build ID versions associated with some Task Queue, as
    fetched by :py:meth:`Client.get_worker_build_id_compatibility`.
    """

    version_sets: Sequence[BuildIdVersionSet]
    """All version sets that were fetched for this task queue."""

    def default_set(self) -> BuildIdVersionSet:
        """Returns the default version set for this task queue."""
        return self.version_sets[-1]

    def default_build_id(self) -> str:
        """Returns the default Build ID for this task queue."""
        return self.default_set().default()

    @staticmethod
    def _from_proto(
        resp: temporalio.api.workflowservice.v1.GetWorkerBuildIdCompatibilityResponse,
    ) -> WorkerBuildIdVersionSets:
        return WorkerBuildIdVersionSets(
            version_sets=[
                BuildIdVersionSet(mvs.build_ids) for mvs in resp.major_version_sets
            ]
        )


@dataclass(frozen=True)
class BuildIdVersionSet:
    """A set of Build IDs which are compatible with each other."""

    build_ids: Sequence[str]
    """All Build IDs contained in the set."""

    def default(self) -> str:
        """Returns the default Build ID for this set."""
        return self.build_ids[-1]


class BuildIdOp(ABC):
    """Base class for Build ID operations as used by
    :py:meth:`Client.update_worker_build_id_compatibility`.
    """

    @abstractmethod
    def _as_partial_proto(
        self,
    ) -> temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityRequest:
        """Returns a partial request with the operation populated. Caller must populate
        non-operation fields. This is done b/c there's no good way to assign a non-primitive message
        as the operation after initializing the request.
        """
        ...


@dataclass(frozen=True)
class BuildIdOpAddNewDefault(BuildIdOp):
    """Adds a new Build Id into a new set, which will be used as the default set for
    the queue. This means all new workflows will start on this Build Id.
    """

    build_id: str

    def _as_partial_proto(
        self,
    ) -> temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityRequest:
        return (
            temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityRequest(
                add_new_build_id_in_new_default_set=self.build_id
            )
        )


@dataclass(frozen=True)
class BuildIdOpAddNewCompatible(BuildIdOp):
    """Adds a new Build Id into an existing compatible set. The newly added ID becomes
    the default for that compatible set, and thus new workflow tasks for workflows which have been
    executing on workers in that set will now start on this new Build Id.
    """

    build_id: str
    """The Build Id to add to the compatible set."""

    existing_compatible_build_id: str
    """A Build Id which must already be defined on the task queue, and is used to find the
    compatible set to add the new id to.
    """

    promote_set: bool = False
    """If set to true, the targeted set will also be promoted to become the overall default set for
    the queue."""

    def _as_partial_proto(
        self,
    ) -> temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityRequest:
        return temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityRequest(
            add_new_compatible_build_id=temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityRequest.AddNewCompatibleVersion(
                new_build_id=self.build_id,
                existing_compatible_build_id=self.existing_compatible_build_id,
                make_set_default=self.promote_set,
            )
        )


@dataclass(frozen=True)
class BuildIdOpPromoteSetByBuildId(BuildIdOp):
    """Promotes a set of compatible Build Ids to become the current default set for the task queue.
    Any Build Id in the set may be used to target it.
    """

    build_id: str
    """A Build Id which must already be defined on the task queue, and is used to find the
    compatible set to promote."""

    def _as_partial_proto(
        self,
    ) -> temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityRequest:
        return (
            temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityRequest(
                promote_set_by_build_id=self.build_id
            )
        )


@dataclass(frozen=True)
class BuildIdOpPromoteBuildIdWithinSet(BuildIdOp):
    """Promotes a Build Id within an existing set to become the default ID for that set."""

    build_id: str

    def _as_partial_proto(
        self,
    ) -> temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityRequest:
        return (
            temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityRequest(
                promote_build_id_within_set=self.build_id
            )
        )


@dataclass(frozen=True)
class BuildIdOpMergeSets(BuildIdOp):
    """Merges two sets into one set, thus declaring all the Build Ids in both as compatible with one
    another. The default of the primary set is maintained as the merged set's overall default.
    """

    primary_build_id: str
    """A Build Id which and is used to find the primary set to be merged."""

    secondary_build_id: str
    """A Build Id which and is used to find the secondary set to be merged."""

    def _as_partial_proto(
        self,
    ) -> temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityRequest:
        return temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityRequest(
            merge_sets=temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityRequest.MergeSets(
                primary_set_build_id=self.primary_build_id,
                secondary_set_build_id=self.secondary_build_id,
            )
        )


@dataclass(frozen=True)
class WorkerTaskReachability:
    """Contains information about the reachability of some Build IDs"""

    build_id_reachability: Mapping[str, BuildIdReachability]
    """Maps Build IDs to information about their reachability"""

    @staticmethod
    def _from_proto(
        resp: temporalio.api.workflowservice.v1.GetWorkerTaskReachabilityResponse,
    ) -> WorkerTaskReachability:
        mapping = dict()
        for bid_reach in resp.build_id_reachability:
            tq_mapping = dict()
            unretrieved = set()
            for tq_reach in bid_reach.task_queue_reachability:
                if tq_reach.reachability == [
                    temporalio.api.enums.v1.TaskReachability.TASK_REACHABILITY_UNSPECIFIED
                ]:
                    unretrieved.add(tq_reach.task_queue)
                    continue
                tq_mapping[tq_reach.task_queue] = [
                    TaskReachabilityType._from_proto(r) for r in tq_reach.reachability
                ]

            mapping[bid_reach.build_id] = BuildIdReachability(
                task_queue_reachability=tq_mapping,
                unretrieved_task_queues=frozenset(unretrieved),
            )

        return WorkerTaskReachability(build_id_reachability=mapping)


@dataclass(frozen=True)
class BuildIdReachability:
    """Contains information about the reachability of a specific Build ID"""

    task_queue_reachability: Mapping[str, Sequence[TaskReachabilityType]]
    """Maps Task Queue names to the reachability status of the Build ID on that queue. If the value
    is an empty list, the Build ID is not reachable on that queue.
    """

    unretrieved_task_queues: FrozenSet[str]
    """If any Task Queues could not be retrieved because the server limits the number that can be
    queried at once, they will be listed here.
    """


class TaskReachabilityType(Enum):
    """Enumerates how a task might reach certain kinds of workflows"""

    NEW_WORKFLOWS = 1
    EXISTING_WORKFLOWS = 2
    OPEN_WORKFLOWS = 3
    CLOSED_WORKFLOWS = 4

    @staticmethod
    def _from_proto(
        reachability: temporalio.api.enums.v1.TaskReachability.ValueType,
    ) -> TaskReachabilityType:
        if (
            reachability
            == temporalio.api.enums.v1.TaskReachability.TASK_REACHABILITY_NEW_WORKFLOWS
        ):
            return TaskReachabilityType.NEW_WORKFLOWS
        elif (
            reachability
            == temporalio.api.enums.v1.TaskReachability.TASK_REACHABILITY_EXISTING_WORKFLOWS
        ):
            return TaskReachabilityType.EXISTING_WORKFLOWS
        elif (
            reachability
            == temporalio.api.enums.v1.TaskReachability.TASK_REACHABILITY_OPEN_WORKFLOWS
        ):
            return TaskReachabilityType.OPEN_WORKFLOWS
        elif (
            reachability
            == temporalio.api.enums.v1.TaskReachability.TASK_REACHABILITY_CLOSED_WORKFLOWS
        ):
            return TaskReachabilityType.CLOSED_WORKFLOWS
        else:
            raise ValueError(f"Cannot convert reachability type: {reachability}")

    def _to_proto(self) -> temporalio.api.enums.v1.TaskReachability.ValueType:
        if self == TaskReachabilityType.NEW_WORKFLOWS:
            return (
                temporalio.api.enums.v1.TaskReachability.TASK_REACHABILITY_NEW_WORKFLOWS
            )
        elif self == TaskReachabilityType.EXISTING_WORKFLOWS:
            return temporalio.api.enums.v1.TaskReachability.TASK_REACHABILITY_EXISTING_WORKFLOWS
        elif self == TaskReachabilityType.OPEN_WORKFLOWS:
            return temporalio.api.enums.v1.TaskReachability.TASK_REACHABILITY_OPEN_WORKFLOWS
        elif self == TaskReachabilityType.CLOSED_WORKFLOWS:
            return temporalio.api.enums.v1.TaskReachability.TASK_REACHABILITY_CLOSED_WORKFLOWS
        else:
            return (
                temporalio.api.enums.v1.TaskReachability.TASK_REACHABILITY_UNSPECIFIED
            )


class CloudOperationsClient:
    """Client for accessing Temporal Cloud Operations API.

    .. warning::
        This client and the API are experimental

    Most users will use :py:meth:`connect` to create a client. The
    :py:attr:`cloud_service` property provides access to a raw gRPC cloud
    service client.

    Clients are not thread-safe and should only be used in the event loop they
    are first connected in. If a client needs to be used from another thread
    than where it was created, make sure the event loop where it was created is
    captured, and then call :py:func:`asyncio.run_coroutine_threadsafe` with the
    client call and that event loop.

    Clients do not work across forks since runtimes do not work across forks.
    """

    @staticmethod
    async def connect(
        *,
        api_key: Optional[str] = None,
        version: Optional[str] = None,
        target_host: str = "saas-api.tmprl.cloud:443",
        tls: Union[bool, TLSConfig] = True,
        retry_config: Optional[RetryConfig] = None,
        keep_alive_config: Optional[KeepAliveConfig] = KeepAliveConfig.default,
        rpc_metadata: Mapping[str, str] = {},
        identity: Optional[str] = None,
        lazy: bool = False,
        runtime: Optional[temporalio.runtime.Runtime] = None,
        http_connect_proxy_config: Optional[HttpConnectProxyConfig] = None,
    ) -> CloudOperationsClient:
        """Connect to a Temporal Cloud Operations API.

        .. warning::
            This client and the API are experimental

        Args:
            api_key: API key for Temporal. This becomes the "Authorization"
                HTTP header with "Bearer " prepended. This is only set if RPC
                metadata doesn't already have an "authorization" key. This is
                essentially required for access to the cloud API.
            version: Version header for safer mutations. May or may not be
                required depending on cloud settings.
            target_host: ``host:port`` for the Temporal server. The default is
                to the common cloud endpoint.
            tls: If true, the default, use system default TLS configuration. If
                false, the default, do not use TLS. If TLS configuration
                present, that TLS configuration will be used. The default is
                usually required to access the API.
            retry_config: Retry configuration for direct service calls (when
                opted in) or all high-level calls made by this client (which all
                opt-in to retries by default). If unset, a default retry
                configuration is used.
            keep_alive_config: Keep-alive configuration for the client
                connection. Default is to check every 30s and kill the
                connection if a response doesn't come back in 15s. Can be set to
                ``None`` to disable.
            rpc_metadata: Headers to use for all calls to the server. Keys here
                can be overriden by per-call RPC metadata keys.
            identity: Identity for this client. If unset, a default is created
                based on the version of the SDK.
            lazy: If true, the client will not connect until the first call is
                attempted or a worker is created with it. Lazy clients cannot be
                used for workers.
            runtime: The runtime for this client, or the default if unset.
            http_connect_proxy_config: Configuration for HTTP CONNECT proxy.
        """
        # Add version if given
        if version:
            rpc_metadata = dict(rpc_metadata)
            rpc_metadata["temporal-cloud-api-version"] = version
        connect_config = temporalio.service.ConnectConfig(
            target_host=target_host,
            api_key=api_key,
            tls=tls,
            retry_config=retry_config,
            keep_alive_config=keep_alive_config,
            rpc_metadata=rpc_metadata,
            identity=identity or "",
            lazy=lazy,
            runtime=runtime,
            http_connect_proxy_config=http_connect_proxy_config,
        )
        return CloudOperationsClient(
            await temporalio.service.ServiceClient.connect(connect_config)
        )

    def __init__(
        self,
        service_client: temporalio.service.ServiceClient,
    ):
        """Create a Temporal Cloud Operations client from a service client.

        .. warning::
            This client and the API are experimental

        Args:
            service_client: Existing service client to use.
        """
        self._service_client = service_client

    @property
    def service_client(self) -> temporalio.service.ServiceClient:
        """Raw gRPC service client."""
        return self._service_client

    @property
    def cloud_service(self) -> temporalio.service.CloudService:
        """Raw gRPC cloud service client."""
        return self._service_client.cloud_service

    @property
    def identity(self) -> str:
        """Identity used in calls by this client."""
        return self._service_client.config.identity

    @property
    def rpc_metadata(self) -> Mapping[str, str]:
        """Headers for every call made by this client.

        Do not use mutate this mapping. Rather, set this property with an
        entirely new mapping to change the headers. This may include the
        ``temporal-cloud-api-version`` header if set.
        """
        return self.service_client.config.rpc_metadata

    @rpc_metadata.setter
    def rpc_metadata(self, value: Mapping[str, str]) -> None:
        """Update the headers for this client.

        Do not mutate this mapping after set. Rather, set an entirely new
        mapping if changes are needed. Currently this must be set with the
        ``temporal-cloud-api-version`` header if it is needed.
        """
        # Update config and perform update
        self.service_client.config.rpc_metadata = value
        self.service_client.update_rpc_metadata(value)

    @property
    def api_key(self) -> Optional[str]:
        """API key for every call made by this client."""
        return self.service_client.config.api_key

    @api_key.setter
    def api_key(self, value: Optional[str]) -> None:
        """Update the API key for this client.

        This is only set if RPCmetadata doesn't already have an "authorization"
        key.
        """
        # Update config and perform update
        self.service_client.config.api_key = value
        self.service_client.update_api_key(value)


# Intended to become a union of callback types
Callback = temporalio.nexus.NexusCallback


async def _encode_user_metadata(
    converter: temporalio.converter.DataConverter,
    summary: Optional[Union[str, temporalio.api.common.v1.Payload]],
    details: Optional[Union[str, temporalio.api.common.v1.Payload]],
) -> Optional[temporalio.api.sdk.v1.UserMetadata]:
    if summary is None and details is None:
        return None
    enc_summary = None
    enc_details = None
    if summary is not None:
        if isinstance(summary, str):
            enc_summary = (await converter.encode([summary]))[0]
        else:
            enc_summary = summary
    if details is not None:
        if isinstance(details, str):
            enc_details = (await converter.encode([details]))[0]
        else:
            enc_details = details
    return temporalio.api.sdk.v1.UserMetadata(summary=enc_summary, details=enc_details)


async def _decode_user_metadata(
    converter: temporalio.converter.DataConverter,
    metadata: Optional[temporalio.api.sdk.v1.UserMetadata],
) -> Tuple[Optional[str], Optional[str]]:
    """Returns (summary, details)"""
    if metadata is None:
        return None, None
    return (
        None
        if not metadata.HasField("summary")
        else (await converter.decode([metadata.summary]))[0],
        None
        if not metadata.HasField("details")
        else (await converter.decode([metadata.details]))[0],
    )


class Plugin(abc.ABC):
    """Base class for client plugins that can intercept and modify client behavior.

    Plugins allow customization of client creation and service connection processes
    through a chain of responsibility pattern. Each plugin can modify the client
    configuration or intercept service client connections.

    If the plugin is also a temporalio.worker.Plugin, it will additionally be propagated as a worker plugin.
    You should likley not also provide it to the worker as that will result in the plugin being applied twice.
    """

    def name(self) -> str:
        """Get the name of this plugin. Can be overridden if desired to provide a more appropriate name.

        Returns:
            The fully qualified name of the plugin class (module.classname).
        """
        return type(self).__module__ + "." + type(self).__qualname__

    @abstractmethod
    def init_client_plugin(self, next: Plugin) -> None:
        """Initialize this plugin in the plugin chain.

        This method sets up the chain of responsibility pattern by providing a reference
        to the next plugin in the chain. It is called during client creation to build
        the plugin chain. Note, this may be called twice in the case of :py:meth:`connect`.
        Implementations should store this reference and call the corresponding method
        of the next plugin on method calls.

        Args:
            next: The next plugin in the chain to delegate to.
        """

    @abstractmethod
    def configure_client(self, config: ClientConfig) -> ClientConfig:
        """Hook called when creating a client to allow modification of configuration.

        This method is called during client creation and allows plugins to modify
        the client configuration before the client is fully initialized. Plugins
        can add interceptors, modify connection parameters, or change other settings.

        Args:
            config: The client configuration dictionary to potentially modify.

        Returns:
            The modified client configuration.
        """

    @abstractmethod
    async def connect_service_client(
        self, config: temporalio.service.ConnectConfig
    ) -> temporalio.service.ServiceClient:
        """Hook called when connecting to the Temporal service.

        This method is called during service client connection and allows plugins
        to intercept or modify the connection process. Plugins can modify connection
        parameters, add authentication, or provide custom connection logic.

        Args:
            config: The service connection configuration.

        Returns:
            The connected service client.
        """


class _RootPlugin(Plugin):
    def init_client_plugin(self, next: Plugin) -> None:
        raise NotImplementedError()

    def configure_client(self, config: ClientConfig) -> ClientConfig:
        return config

    async def connect_service_client(
        self, config: temporalio.service.ConnectConfig
    ) -> temporalio.service.ServiceClient:
        return await temporalio.service.ServiceClient.connect(config)
