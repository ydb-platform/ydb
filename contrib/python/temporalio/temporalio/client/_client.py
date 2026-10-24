"""Client support for accessing Temporal."""

from __future__ import annotations

from collections.abc import (
    Awaitable,
    Callable,
    Mapping,
    Sequence,
)
from datetime import timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Concatenate,
    cast,
    overload,
)

import nexusrpc
from nexusrpc import OutputT
from typing_extensions import Required, Self, TypedDict

import temporalio.activity
import temporalio.api.workflowservice.v1
import temporalio.common
import temporalio.converter
import temporalio.runtime
import temporalio.service
import temporalio.workflow
from temporalio.service import (
    ConnectConfig,
    DnsLoadBalancingConfig,
    GrpcCompression,
    HttpConnectProxyConfig,
    KeepAliveConfig,
    PayloadLimitsConfig,
    RetryConfig,
    ServiceClient,
    TLSConfig,
)

from ..common import HeaderCodecBehavior
from ..types import (
    CallableAsyncNoParam,
    CallableAsyncSingleParam,
    CallableSyncNoParam,
    CallableSyncSingleParam,
    LocalReturnType,
    MethodAsyncNoParam,
    MethodAsyncSingleParam,
    MultiParamSpec,
    NexusServiceType,
    ParamType,
    ReturnType,
    SelfType,
)
from ._activity import (
    ActivityExecutionAsyncIterator,
    ActivityExecutionCount,
    ActivityHandle,
    AsyncActivityHandle,
    AsyncActivityIDReference,
)
from ._impl import _ClientImpl
from ._interceptor import (
    CountActivitiesInput,
    CountNexusOperationsInput,
    CountWorkflowsInput,
    CreateScheduleInput,
    GetWorkerBuildIdCompatibilityInput,
    GetWorkerTaskReachabilityInput,
    ListActivitiesInput,
    ListNexusOperationsInput,
    ListSchedulesInput,
    ListWorkflowsInput,
    OutboundInterceptor,
    StartActivityInput,
    StartWorkflowInput,
    StartWorkflowUpdateWithStartInput,
    UpdateWithStartUpdateWorkflowInput,
    UpdateWorkerBuildIdCompatibilityInput,
)
from ._nexus import (
    NexusClient,
    NexusOperationExecutionAsyncIterator,
    NexusOperationExecutionCount,
    NexusOperationHandle,
    _NexusClient,
)
from ._schedule import (
    Schedule,
    ScheduleAsyncIterator,
    ScheduleBackfill,
    ScheduleHandle,
)
from ._worker_versioning import (
    BuildIdOp,
    TaskReachabilityType,
    WorkerBuildIdVersionSets,
    WorkerTaskReachability,
)
from ._workflow import (
    WithStartWorkflowOperation,
    WorkflowExecutionAsyncIterator,
    WorkflowExecutionCount,
    WorkflowHandle,
    WorkflowUpdateHandle,
    WorkflowUpdateStage,
)

if TYPE_CHECKING:
    from ._interceptor import Interceptor
    from ._plugin import Plugin


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

    @classmethod
    async def connect(
        cls,
        target_host: str,
        *,
        namespace: str = "default",
        api_key: str | None = None,
        data_converter: temporalio.converter.DataConverter = temporalio.converter.DataConverter.default,
        plugins: Sequence[Plugin] = [],
        interceptors: Sequence[Interceptor] = [],
        default_workflow_query_reject_condition: None
        | (temporalio.common.QueryRejectCondition) = None,
        tls: bool | TLSConfig | None = None,
        retry_config: RetryConfig | None = None,
        keep_alive_config: KeepAliveConfig | None = KeepAliveConfig.default,
        rpc_metadata: Mapping[str, str | bytes] = {},
        identity: str | None = None,
        lazy: bool = False,
        runtime: temporalio.runtime.Runtime | None = None,
        http_connect_proxy_config: HttpConnectProxyConfig | None = None,
        dns_load_balancing_config: DnsLoadBalancingConfig | None = None,
        grpc_compression: GrpcCompression = GrpcCompression.GZIP,
        payload_limits: PayloadLimitsConfig = PayloadLimitsConfig(),
        header_codec_behavior: HeaderCodecBehavior = HeaderCodecBehavior.NO_CODEC,
    ) -> Self:
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
            tls: If ``None``, the default, TLS will be enabled automatically
                when ``api_key`` is provided, otherwise TLS is disabled. If
                ``False``, do not use TLS. If ``True``, use system default TLS
                configuration. If TLS configuration present, that TLS
                configuration will be used.
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
            dns_load_balancing_config: DNS load balancing configuration for the
                client connection. Default is to re-resolve DNS every 30s. Can
                be set to ``None`` to disable. Silently disabled when
                ``http_connect_proxy_config`` is set, since the two are mutually
                exclusive.
            grpc_compression: Transport-level gRPC compression for the client
                connection. Default is gzip. Set to
                :py:attr:`GrpcCompression.NONE` to disable compression.
            payload_limits: Warning thresholds for outbound payload/memo sizes. Over-threshold
                fields are logged but still sent. Set a threshold to 0 to disable it.
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
            dns_load_balancing_config=dns_load_balancing_config,
            grpc_compression=grpc_compression,
            payload_limits=payload_limits,
        )

        def make_lambda(
            plugin: Plugin, next: Callable[[ConnectConfig], Awaitable[ServiceClient]]
        ):
            return lambda config: plugin.connect_service_client(config, next)

        next_function = ServiceClient.connect
        for plugin in reversed(plugins):
            next_function = make_lambda(plugin, next_function)

        service_client = await next_function(connect_config)

        return cls(
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
        default_workflow_query_reject_condition: None
        | (temporalio.common.QueryRejectCondition) = None,
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
            plugins=plugins,
            interceptors=interceptors,
            default_workflow_query_reject_condition=default_workflow_query_reject_condition,
            header_codec_behavior=header_codec_behavior,
        )
        self._initial_config = config.copy()

        for plugin in plugins:
            config = plugin.configure_client(config)

        self._init_from_config(config)

    def _init_from_config(self, config: ClientConfig):
        self._config = config

        # Iterate over interceptors in reverse building the impl
        self._impl: OutboundInterceptor = _ClientImpl(self)
        for interceptor in reversed(list(self._config["interceptors"])):
            self._impl = interceptor.intercept_client(self._impl)

    def config(self, *, active_config: bool = False) -> ClientConfig:
        """Config, as a dictionary, used to create this client.

        Args:
            active_config: If true, return the modified configuration in use rather than the initial one
                provided to the client.

        This makes a shallow copy of the config each call.
        """
        config = self._config.copy() if active_config else self._initial_config.copy()
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
    def rpc_metadata(self) -> Mapping[str, str | bytes]:
        """Headers for every call made by this client.

        Do not use mutate this mapping. Rather, set this property with an
        entirely new mapping to change the headers.
        """
        return self.service_client.config.rpc_metadata

    @rpc_metadata.setter
    def rpc_metadata(self, value: Mapping[str, str | bytes]) -> None:
        """Update the headers for this client.

        Do not mutate this mapping after set. Rather, set an entirely new
        mapping if changes are needed.

        Raises:
            TypeError: the key/value pair is not a valid gRPC ASCII or binary metadata.
                All binary metadata must be supplied as bytes, and the key must end in '-bin'.

        .. warning::
            Attempting to set an invalid binary RPC metadata value may leave the client
            in an inconsistent state (as well as raise a :py:class:`TypeError`).
        """
        # Update config and perform update
        # This may raise if the metadata is invalid:
        self.service_client.update_rpc_metadata(value)
        self.service_client.config.rpc_metadata = value

    @property
    def api_key(self) -> str | None:
        """API key for every call made by this client."""
        return self.service_client.config.api_key

    @api_key.setter
    def api_key(self, value: str | None) -> None:
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
        execution_timeout: timedelta | None = None,
        run_timeout: timedelta | None = None,
        task_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy = temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        cron_schedule: str = "",
        memo: Mapping[str, Any] | None = None,
        search_attributes: None
        | (
            temporalio.common.TypedSearchAttributes | temporalio.common.SearchAttributes
        ) = None,
        static_summary: str | None = None,
        static_details: str | None = None,
        start_delay: timedelta | None = None,
        start_signal: str | None = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
        request_eager_start: bool = False,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: temporalio.common.VersioningOverride | None = None,
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
        execution_timeout: timedelta | None = None,
        run_timeout: timedelta | None = None,
        task_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy = temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        cron_schedule: str = "",
        memo: Mapping[str, Any] | None = None,
        search_attributes: None
        | (
            temporalio.common.TypedSearchAttributes | temporalio.common.SearchAttributes
        ) = None,
        static_summary: str | None = None,
        static_details: str | None = None,
        start_delay: timedelta | None = None,
        start_signal: str | None = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
        request_eager_start: bool = False,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: temporalio.common.VersioningOverride | None = None,
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
        execution_timeout: timedelta | None = None,
        run_timeout: timedelta | None = None,
        task_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy = temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        cron_schedule: str = "",
        memo: Mapping[str, Any] | None = None,
        search_attributes: None
        | (
            temporalio.common.TypedSearchAttributes | temporalio.common.SearchAttributes
        ) = None,
        static_summary: str | None = None,
        static_details: str | None = None,
        start_delay: timedelta | None = None,
        start_signal: str | None = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
        request_eager_start: bool = False,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: temporalio.common.VersioningOverride | None = None,
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
        result_type: type | None = None,
        execution_timeout: timedelta | None = None,
        run_timeout: timedelta | None = None,
        task_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy = temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        cron_schedule: str = "",
        memo: Mapping[str, Any] | None = None,
        search_attributes: None
        | (
            temporalio.common.TypedSearchAttributes | temporalio.common.SearchAttributes
        ) = None,
        static_summary: str | None = None,
        static_details: str | None = None,
        start_delay: timedelta | None = None,
        start_signal: str | None = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
        request_eager_start: bool = False,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: temporalio.common.VersioningOverride | None = None,
    ) -> WorkflowHandle[Any, Any]: ...

    async def start_workflow(
        self,
        workflow: str | Callable[..., Awaitable[Any]],
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: str,
        result_type: type | None = None,
        execution_timeout: timedelta | None = None,
        run_timeout: timedelta | None = None,
        task_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy = temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        cron_schedule: str = "",
        memo: Mapping[str, Any] | None = None,
        search_attributes: None
        | (
            temporalio.common.TypedSearchAttributes | temporalio.common.SearchAttributes
        ) = None,
        static_summary: str | None = None,
        static_details: str | None = None,
        start_delay: timedelta | None = None,
        start_signal: str | None = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
        request_eager_start: bool = False,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: temporalio.common.VersioningOverride | None = None,
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
        execution_timeout: timedelta | None = None,
        run_timeout: timedelta | None = None,
        task_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy = temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        cron_schedule: str = "",
        memo: Mapping[str, Any] | None = None,
        search_attributes: None
        | (
            temporalio.common.TypedSearchAttributes | temporalio.common.SearchAttributes
        ) = None,
        static_summary: str | None = None,
        static_details: str | None = None,
        start_delay: timedelta | None = None,
        start_signal: str | None = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
        request_eager_start: bool = False,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: temporalio.common.VersioningOverride | None = None,
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
        execution_timeout: timedelta | None = None,
        run_timeout: timedelta | None = None,
        task_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy = temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        cron_schedule: str = "",
        memo: Mapping[str, Any] | None = None,
        search_attributes: None
        | (
            temporalio.common.TypedSearchAttributes | temporalio.common.SearchAttributes
        ) = None,
        static_summary: str | None = None,
        static_details: str | None = None,
        start_delay: timedelta | None = None,
        start_signal: str | None = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
        request_eager_start: bool = False,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: temporalio.common.VersioningOverride | None = None,
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
        execution_timeout: timedelta | None = None,
        run_timeout: timedelta | None = None,
        task_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy = temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        cron_schedule: str = "",
        memo: Mapping[str, Any] | None = None,
        search_attributes: None
        | (
            temporalio.common.TypedSearchAttributes | temporalio.common.SearchAttributes
        ) = None,
        static_summary: str | None = None,
        static_details: str | None = None,
        start_delay: timedelta | None = None,
        start_signal: str | None = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
        request_eager_start: bool = False,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: temporalio.common.VersioningOverride | None = None,
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
        result_type: type | None = None,
        execution_timeout: timedelta | None = None,
        run_timeout: timedelta | None = None,
        task_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy = temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        cron_schedule: str = "",
        memo: Mapping[str, Any] | None = None,
        search_attributes: None
        | (
            temporalio.common.TypedSearchAttributes | temporalio.common.SearchAttributes
        ) = None,
        static_summary: str | None = None,
        static_details: str | None = None,
        start_delay: timedelta | None = None,
        start_signal: str | None = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
        request_eager_start: bool = False,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: temporalio.common.VersioningOverride | None = None,
    ) -> Any: ...

    async def execute_workflow(
        self,
        workflow: str | Callable[..., Awaitable[Any]],
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: str,
        result_type: type | None = None,
        execution_timeout: timedelta | None = None,
        run_timeout: timedelta | None = None,
        task_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy = temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        cron_schedule: str = "",
        memo: Mapping[str, Any] | None = None,
        search_attributes: None
        | (
            temporalio.common.TypedSearchAttributes | temporalio.common.SearchAttributes
        ) = None,
        static_summary: str | None = None,
        static_details: str | None = None,
        start_delay: timedelta | None = None,
        start_signal: str | None = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
        request_eager_start: bool = False,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: temporalio.common.VersioningOverride | None = None,
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
        run_id: str | None = None,
        first_execution_run_id: str | None = None,
        result_type: type | None = None,
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
        workflow: (
            MethodAsyncNoParam[SelfType, ReturnType]
            | MethodAsyncSingleParam[SelfType, Any, ReturnType]
        ),
        workflow_id: str,
        *,
        run_id: str | None = None,
        first_execution_run_id: str | None = None,
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
        id: str | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
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
        id: str | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> LocalReturnType: ...

    # Overload for multi-param update
    @overload
    async def execute_update_with_start_workflow(
        self,
        update: temporalio.workflow.UpdateMethodMultiParam[
            MultiParamSpec, LocalReturnType
        ],
        *,
        args: MultiParamSpec.args,  # type: ignore
        start_workflow_operation: WithStartWorkflowOperation[SelfType, Any],
        id: str | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
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
        id: str | None = None,
        result_type: type | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> Any: ...

    async def execute_update_with_start_workflow(
        self,
        update: str | Callable,
        arg: Any = temporalio.common._arg_unset,
        *,
        start_workflow_operation: WithStartWorkflowOperation[Any, Any],
        args: Sequence[Any] = [],
        id: str | None = None,
        result_type: type | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
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
        id: str | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
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
        id: str | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> WorkflowUpdateHandle[LocalReturnType]: ...

    # Overload for multi-param start update
    @overload
    async def start_update_with_start_workflow(
        self,
        update: temporalio.workflow.UpdateMethodMultiParam[
            MultiParamSpec, LocalReturnType
        ],
        *,
        args: MultiParamSpec.args,  # type: ignore
        start_workflow_operation: WithStartWorkflowOperation[SelfType, Any],
        wait_for_stage: WorkflowUpdateStage,
        id: str | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
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
        id: str | None = None,
        result_type: type | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> WorkflowUpdateHandle[Any]: ...

    async def start_update_with_start_workflow(
        self,
        update: str | Callable,
        arg: Any = temporalio.common._arg_unset,
        *,
        start_workflow_operation: WithStartWorkflowOperation[Any, Any],
        wait_for_stage: WorkflowUpdateStage,
        args: Sequence[Any] = [],
        id: str | None = None,
        result_type: type | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
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
        update: str | Callable,
        arg: Any = temporalio.common._arg_unset,
        *,
        wait_for_stage: WorkflowUpdateStage,
        args: Sequence[Any] = [],
        id: str | None = None,
        result_type: type | None = None,
        start_workflow_operation: WithStartWorkflowOperation[SelfType, ReturnType],
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
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
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
            _on_start=on_start,
            _on_start_error=on_start_error,
        )

        return await self._impl.start_update_with_start_workflow(input)

    def list_workflows(
        self,
        query: str | None = None,
        *,
        limit: int | None = None,
        page_size: int = 1000,
        next_page_token: bytes | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
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
        query: str | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
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

    # async no-param
    @overload
    async def start_activity(
        self,
        activity: CallableAsyncNoParam[ReturnType],
        *,
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ActivityHandle[ReturnType]: ...

    # sync no-param
    @overload
    async def start_activity(
        self,
        activity: CallableSyncNoParam[ReturnType],
        *,
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ActivityHandle[ReturnType]: ...

    # async single-param
    @overload
    async def start_activity(
        self,
        activity: CallableAsyncSingleParam[ParamType, ReturnType],
        arg: ParamType,
        *,
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ActivityHandle[ReturnType]: ...

    # sync single-param
    @overload
    async def start_activity(
        self,
        activity: CallableSyncSingleParam[ParamType, ReturnType],
        arg: ParamType,
        *,
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ActivityHandle[ReturnType]: ...

    # async multi-param
    @overload
    async def start_activity(
        self,
        activity: Callable[..., Awaitable[ReturnType]],
        *,
        args: Sequence[Any],
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ActivityHandle[ReturnType]: ...

    # sync multi-param
    @overload
    async def start_activity(
        self,
        activity: Callable[..., ReturnType],
        *,
        args: Sequence[Any],
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ActivityHandle[ReturnType]: ...

    # string name
    @overload
    async def start_activity(
        self,
        activity: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: str,
        result_type: type | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ActivityHandle[Any]: ...

    async def start_activity(
        self,
        activity: (
            str | Callable[..., Awaitable[ReturnType]] | Callable[..., ReturnType]
        ),
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: str,
        result_type: type | None = None,
        # Either schedule_to_close_timeout or start_to_close_timeout must be present
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ActivityHandle[ReturnType]:
        """Start an activity and return its handle.

        .. warning::
           This API is experimental.

        Args:
            activity: String name or callable activity function to execute.
            arg: Single argument to the activity.
            args: Multiple arguments to the activity. Cannot be set if arg is.
            id: Unique identifier for the activity. Required.
            task_queue: Task queue to send the activity to.
            result_type: For string name activities, optional type to deserialize result into.
            schedule_to_close_timeout: Total time allowed for the activity from schedule to completion.
            schedule_to_start_timeout: Time allowed for the activity to sit in the task queue.
            start_to_close_timeout: Time allowed for a single execution attempt.
            heartbeat_timeout: Time between heartbeats before the activity is considered failed.
            id_reuse_policy: How to handle reusing activity IDs from closed activities.
                Default is ALLOW_DUPLICATE.
            id_conflict_policy: How to handle activity ID conflicts with running activities.
                Default is FAIL.
            retry_policy: Retry policy for the activity.
            search_attributes: Search attributes for the activity.
            summary: A single-line fixed summary for this activity that may appear
                in the UI/CLI. This can be in single-line Temporal markdown format.
            priority: Priority of the activity execution.
            start_delay: Time to wait before dispatching the activity.
                This delay is not applied to retry attempts.
            rpc_metadata: Headers used on the RPC call.
            rpc_timeout: Optional RPC deadline to set for the RPC call.

        Returns:
            A handle to the started activity.
        """
        name, result_type_from_type_annotation = (
            temporalio.activity._Definition.get_name_and_result_type(activity)
        )
        return await self._impl.start_activity(
            StartActivityInput(
                activity_type=name,
                args=temporalio.common._arg_or_args(arg, args),
                id=id,
                task_queue=task_queue,
                result_type=result_type or result_type_from_type_annotation,
                schedule_to_close_timeout=schedule_to_close_timeout,
                schedule_to_start_timeout=schedule_to_start_timeout,
                start_to_close_timeout=start_to_close_timeout,
                heartbeat_timeout=heartbeat_timeout,
                id_reuse_policy=id_reuse_policy,
                id_conflict_policy=id_conflict_policy,
                retry_policy=retry_policy,
                search_attributes=search_attributes,
                summary=summary,
                start_delay=start_delay,
                headers={},
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
                priority=priority,
            )
        )

    # async no-param
    @overload
    async def execute_activity(
        self,
        activity: CallableAsyncNoParam[ReturnType],
        *,
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ReturnType: ...

    # sync no-param
    @overload
    async def execute_activity(
        self,
        activity: CallableSyncNoParam[ReturnType],
        *,
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ReturnType: ...

    # async single-param
    @overload
    async def execute_activity(
        self,
        activity: CallableAsyncSingleParam[ParamType, ReturnType],
        arg: ParamType,
        *,
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ReturnType: ...

    # sync single-param
    @overload
    async def execute_activity(
        self,
        activity: CallableSyncSingleParam[ParamType, ReturnType],
        arg: ParamType,
        *,
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ReturnType: ...

    # async multi-param
    @overload
    async def execute_activity(
        self,
        activity: Callable[..., Awaitable[ReturnType]],
        *,
        args: Sequence[Any],
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ReturnType: ...

    # sync multi-param
    @overload
    async def execute_activity(
        self,
        activity: Callable[..., ReturnType],
        *,
        args: Sequence[Any],
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ReturnType: ...

    # string name
    @overload
    async def execute_activity(
        self,
        activity: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: str,
        result_type: type | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> Any: ...

    async def execute_activity(
        self,
        activity: (
            str | Callable[..., Awaitable[ReturnType]] | Callable[..., ReturnType]
        ),
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: str,
        result_type: type | None = None,
        # Either schedule_to_close_timeout or start_to_close_timeout must be present
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ReturnType:
        """Start an activity, wait for it to complete, and return its result.

        .. warning::
           This API is experimental.

        This is a convenience method that combines :py:meth:`start_activity` and
        :py:meth:`ActivityHandle.result`.

        Returns:
            The result of the activity.

        Raises:
            ActivityFailureError: If the activity completed with a failure.
        """
        handle: ActivityHandle[ReturnType] = await self.start_activity(
            cast(Any, activity),
            arg,
            args=args,
            id=id,
            task_queue=task_queue,
            result_type=result_type,
            schedule_to_close_timeout=schedule_to_close_timeout,
            schedule_to_start_timeout=schedule_to_start_timeout,
            start_to_close_timeout=start_to_close_timeout,
            heartbeat_timeout=heartbeat_timeout,
            id_reuse_policy=id_reuse_policy,
            id_conflict_policy=id_conflict_policy,
            retry_policy=retry_policy,
            search_attributes=search_attributes,
            summary=summary,
            priority=priority,
            start_delay=start_delay,
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
        )
        return await handle.result()

    # async no-param
    @overload
    async def start_activity_class(
        self,
        activity: type[CallableAsyncNoParam[ReturnType]],
        *,
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ActivityHandle[ReturnType]: ...

    # sync no-param
    @overload
    async def start_activity_class(
        self,
        activity: type[CallableSyncNoParam[ReturnType]],
        *,
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ActivityHandle[ReturnType]: ...

    # async single-param
    @overload
    async def start_activity_class(
        self,
        activity: type[CallableAsyncSingleParam[ParamType, ReturnType]],
        arg: ParamType,
        *,
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ActivityHandle[ReturnType]: ...

    # sync single-param
    @overload
    async def start_activity_class(
        self,
        activity: type[CallableSyncSingleParam[ParamType, ReturnType]],
        arg: ParamType,
        *,
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ActivityHandle[ReturnType]: ...

    # async multi-param
    @overload
    async def start_activity_class(
        self,
        activity: type[Callable[..., Awaitable[ReturnType]]],  # type: ignore[reportInvalidTypeForm]
        *,
        args: Sequence[Any],
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ActivityHandle[ReturnType]: ...

    # sync multi-param
    @overload
    async def start_activity_class(
        self,
        activity: type[Callable[..., ReturnType]],  # type: ignore[reportInvalidTypeForm]
        *,
        args: Sequence[Any],
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ActivityHandle[ReturnType]: ...

    async def start_activity_class(
        self,
        activity: type[Callable],  # type: ignore[reportInvalidTypeForm]
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: str,
        result_type: type | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ActivityHandle[Any]:
        """Start an activity from a callable class.

        .. warning::
           This API is experimental.

        See :py:meth:`start_activity` for parameter and return details.
        """
        return await self.start_activity(
            cast(Any, activity),
            arg,
            args=args,
            id=id,
            task_queue=task_queue,
            result_type=result_type,
            schedule_to_close_timeout=schedule_to_close_timeout,
            schedule_to_start_timeout=schedule_to_start_timeout,
            start_to_close_timeout=start_to_close_timeout,
            heartbeat_timeout=heartbeat_timeout,
            id_reuse_policy=id_reuse_policy,
            id_conflict_policy=id_conflict_policy,
            retry_policy=retry_policy,
            search_attributes=search_attributes,
            summary=summary,
            priority=priority,
            start_delay=start_delay,
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
        )

    # async no-param
    @overload
    async def execute_activity_class(
        self,
        activity: type[CallableAsyncNoParam[ReturnType]],
        *,
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ReturnType: ...

    # sync no-param
    @overload
    async def execute_activity_class(
        self,
        activity: type[CallableSyncNoParam[ReturnType]],
        *,
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ReturnType: ...

    # async single-param
    @overload
    async def execute_activity_class(
        self,
        activity: type[CallableAsyncSingleParam[ParamType, ReturnType]],
        arg: ParamType,
        *,
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ReturnType: ...

    # sync single-param
    @overload
    async def execute_activity_class(
        self,
        activity: type[CallableSyncSingleParam[ParamType, ReturnType]],
        arg: ParamType,
        *,
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ReturnType: ...

    # async multi-param
    @overload
    async def execute_activity_class(
        self,
        activity: type[Callable[..., Awaitable[ReturnType]]],  # type: ignore[reportInvalidTypeForm]
        *,
        args: Sequence[Any],
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ReturnType: ...

    # sync multi-param
    @overload
    async def execute_activity_class(
        self,
        activity: type[Callable[..., ReturnType]],  # type: ignore[reportInvalidTypeForm]
        *,
        args: Sequence[Any],
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ReturnType: ...

    async def execute_activity_class(
        self,
        activity: type[Callable],  # type: ignore[reportInvalidTypeForm]
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: str,
        result_type: type | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> Any:
        """Start an activity from a callable class and wait for completion.

        .. warning::
           This API is experimental.

        This is a shortcut for ``await`` :py:meth:`start_activity_class`.
        """
        return await self.execute_activity(
            cast(Any, activity),
            arg,
            args=args,
            id=id,
            task_queue=task_queue,
            result_type=result_type,
            schedule_to_close_timeout=schedule_to_close_timeout,
            schedule_to_start_timeout=schedule_to_start_timeout,
            start_to_close_timeout=start_to_close_timeout,
            heartbeat_timeout=heartbeat_timeout,
            id_reuse_policy=id_reuse_policy,
            id_conflict_policy=id_conflict_policy,
            retry_policy=retry_policy,
            search_attributes=search_attributes,
            summary=summary,
            priority=priority,
            start_delay=start_delay,
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
        )

    # async no-param
    @overload
    async def start_activity_method(
        self,
        activity: MethodAsyncNoParam[SelfType, ReturnType],
        *,
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ActivityHandle[ReturnType]: ...

    # async single-param
    @overload
    async def start_activity_method(
        self,
        activity: MethodAsyncSingleParam[SelfType, ParamType, ReturnType],
        arg: ParamType,
        *,
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ActivityHandle[ReturnType]: ...

    # async multi-param
    @overload
    async def start_activity_method(
        self,
        activity: Callable[
            Concatenate[SelfType, MultiParamSpec], Awaitable[ReturnType]
        ],
        *,
        args: Sequence[Any],
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ActivityHandle[ReturnType]: ...

    # sync multi-param
    @overload
    async def start_activity_method(
        self,
        activity: Callable[Concatenate[SelfType, MultiParamSpec], ReturnType],
        *,
        args: Sequence[Any],
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ActivityHandle[ReturnType]: ...

    async def start_activity_method(
        self,
        activity: Callable,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: str,
        result_type: type | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ActivityHandle[Any]:
        """Start an activity from a method.

        .. warning::
           This API is experimental.

        See :py:meth:`start_activity` for parameter and return details.
        """
        return await self.start_activity(
            cast(Any, activity),
            arg,
            args=args,
            id=id,
            task_queue=task_queue,
            result_type=result_type,
            schedule_to_close_timeout=schedule_to_close_timeout,
            schedule_to_start_timeout=schedule_to_start_timeout,
            start_to_close_timeout=start_to_close_timeout,
            heartbeat_timeout=heartbeat_timeout,
            id_reuse_policy=id_reuse_policy,
            id_conflict_policy=id_conflict_policy,
            retry_policy=retry_policy,
            search_attributes=search_attributes,
            summary=summary,
            priority=priority,
            start_delay=start_delay,
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
        )

    # async no-param
    @overload
    async def execute_activity_method(
        self,
        activity: MethodAsyncNoParam[SelfType, ReturnType],
        *,
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ReturnType: ...

    # async single-param
    @overload
    async def execute_activity_method(
        self,
        activity: MethodAsyncSingleParam[SelfType, ParamType, ReturnType],
        arg: ParamType,
        *,
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ReturnType: ...

    # async multi-param
    @overload
    async def execute_activity_method(
        self,
        activity: Callable[
            Concatenate[SelfType, MultiParamSpec], Awaitable[ReturnType]
        ],
        *,
        args: Sequence[Any],
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ReturnType: ...

    # sync multi-param
    @overload
    async def execute_activity_method(
        self,
        activity: Callable[Concatenate[SelfType, MultiParamSpec], ReturnType],
        *,
        args: Sequence[Any],
        id: str,
        task_queue: str,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ReturnType: ...

    async def execute_activity_method(
        self,
        activity: Callable,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: str,
        result_type: type | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        start_delay: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> Any:
        """Start an activity from a method and wait for completion.

        .. warning::
           This API is experimental.

        This is a shortcut for ``await`` :py:meth:`start_activity_method`.
        """
        return await self.execute_activity(
            cast(Any, activity),
            arg,
            args=args,
            id=id,
            task_queue=task_queue,
            result_type=result_type,
            schedule_to_close_timeout=schedule_to_close_timeout,
            schedule_to_start_timeout=schedule_to_start_timeout,
            start_to_close_timeout=start_to_close_timeout,
            heartbeat_timeout=heartbeat_timeout,
            id_reuse_policy=id_reuse_policy,
            id_conflict_policy=id_conflict_policy,
            retry_policy=retry_policy,
            search_attributes=search_attributes,
            summary=summary,
            priority=priority,
            start_delay=start_delay,
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
        )

    def list_activities(
        self,
        query: str,
        *,
        limit: int | None = None,
        page_size: int = 1000,
        next_page_token: bytes | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ActivityExecutionAsyncIterator:
        """List activities not started by a workflow.

        .. warning::
           This API is experimental.

        This does not make a request until the first iteration is attempted.
        Therefore any errors will not occur until then.

        Args:
            query: A Temporal visibility list filter for activities. Required.
            limit: Maximum number of activities to return. If unset, all
                activities are returned. Only applies if using the
                returned :py:class:`ActivityExecutionAsyncIterator`
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
        return self._impl.list_activities(
            ListActivitiesInput(
                query=query,
                page_size=page_size,
                next_page_token=next_page_token,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
                limit=limit,
            )
        )

    async def count_activities(
        self,
        query: str | None = None,
        *,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ActivityExecutionCount:
        """Count activities not started by a workflow.

        .. warning::
           This API is experimental.

        Args:
            query: A Temporal visibility filter for activities.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.

        Returns:
            Count of activities.
        """
        return await self._impl.count_activities(
            CountActivitiesInput(
                query=query, rpc_metadata=rpc_metadata, rpc_timeout=rpc_timeout
            )
        )

    @overload
    def get_activity_handle(
        self,
        activity_id: str,
        *,
        run_id: str | None = None,
    ) -> ActivityHandle[Any]: ...

    @overload
    def get_activity_handle(
        self,
        activity_id: str,
        *,
        run_id: str | None = None,
        result_type: type[ReturnType],
    ) -> ActivityHandle[ReturnType]: ...

    def get_activity_handle(
        self,
        activity_id: str,
        *,
        run_id: str | None = None,
        result_type: type | None = None,
    ) -> ActivityHandle[Any]:
        """Get a handle to an existing activity, as the caller of that activity.

        The activity must not have been started by a workflow.

        .. warning::
           This API is experimental.

        To get a handle to an activity execution that you control for manual completion and
        heartbeating, see :py:meth:`Client.get_async_activity_handle`.

        Args:
            activity_id: The activity ID.
            run_id: The activity run ID. If not provided, targets the latest run.
            result_type: The result type to deserialize into.

        Returns:
            A handle to the activity.
        """
        return ActivityHandle(
            self,
            activity_id,
            run_id=run_id,
            result_type=result_type,
        )

    @overload
    def get_async_activity_handle(
        self, *, activity_id: str, run_id: str | None = None
    ) -> AsyncActivityHandle:
        pass

    @overload
    def get_async_activity_handle(
        self, *, workflow_id: str, run_id: str | None, activity_id: str
    ) -> AsyncActivityHandle:
        pass

    @overload
    def get_async_activity_handle(self, *, task_token: bytes) -> AsyncActivityHandle:
        pass

    def get_async_activity_handle(
        self,
        *,
        workflow_id: str | None = None,
        run_id: str | None = None,
        activity_id: str | None = None,
        task_token: bytes | None = None,
    ) -> AsyncActivityHandle:
        """Get a handle to an activity execution that you control, for manual
        completion and heartbeating.

        To get a handle to an activity execution as the caller of that activity,
        see :py:meth:`Client.get_activity_handle`.

        This function may be used to get a handle to an activity started by a
        client, or an activity started by a workflow.

        To get a handle to an activity started by a workflow, use one of the
        following two calls:
        - Supply ``workflow_id``, ``run_id``, and ``activity_id``
        - Supply the activity ``task_token`` alone

        To get a handle to an activity not started by a workflow, supply
        ``activity_id`` and ``run_id``

        Args:
            workflow_id: Workflow ID for the activity, or None if not a workflow
                activity. Cannot be set if task_token is set.
            run_id: Run ID for the activity or workflow. Cannot be set if
                task_token is set.
            activity_id: ID for the activity. Cannot be set if task_token is
                set.
            task_token: Task token for the activity. Cannot be set with other
                fields.

        Returns:
            A handle that can be used for completion or heartbeating.
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
        elif activity_id is not None:
            return AsyncActivityHandle(
                self,
                AsyncActivityIDReference(
                    activity_id=activity_id,
                    run_id=run_id,
                    workflow_id=None,
                ),
            )
        raise ValueError(
            "Require task token, or workflow_id & run_id & activity_id, or activity_id & run_id"
        )

    async def create_schedule(
        self,
        id: str,
        schedule: Schedule,
        *,
        trigger_immediately: bool = False,
        backfill: Sequence[ScheduleBackfill] = [],
        memo: Mapping[str, Any] | None = None,
        search_attributes: None
        | (
            temporalio.common.TypedSearchAttributes | temporalio.common.SearchAttributes
        ) = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
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
        query: str | None = None,
        *,
        page_size: int = 1000,
        next_page_token: bytes | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
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
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None:
        """Used to add new Build IDs or otherwise update the relative compatibility of Build Ids as
        defined on a specific task queue for the Worker Versioning feature.

        For more on this feature, see https://docs.temporal.io/workers#worker-versioning

        .. deprecated::
            Legacy API, see the docs above for new usage

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
        max_sets: int | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> WorkerBuildIdVersionSets:
        """Get the Build ID compatibility sets for a specific task queue.

        For more on this feature, see https://docs.temporal.io/workers#worker-versioning

        .. deprecated::
            Legacy API, see the docs above for new usage

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
        reachability_type: TaskReachabilityType | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> WorkerTaskReachability:
        """Determine if some Build IDs for certain Task Queues could have tasks dispatched to them.

        For more on this feature, see https://docs.temporal.io/workers#worker-versioning

        .. deprecated::
            Legacy API, see the docs above for new usage

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

    def create_nexus_client(
        self,
        service: type[NexusServiceType] | str,
        endpoint: str,
    ) -> NexusClient[NexusServiceType]:
        """Create a client for starting standalone Nexus operations.

        .. warning::
           This API is experimental and unstable.

        Args:
            service: The Nexus service type or service name string.
            endpoint: Endpoint name, resolved to a URL via the cluster's
                endpoint registry.

        Returns:
            A Nexus client for the given service and endpoint.
        """
        return _NexusClient(client=self, service=service, endpoint=endpoint)

    def list_nexus_operations(
        self,
        query: str,
        *,
        limit: int | None = None,
        page_size: int = 1000,
        next_page_token: bytes | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> NexusOperationExecutionAsyncIterator:
        """List standalone Nexus operations.

        .. warning::
           This API is experimental and unstable.

        This does not make a request until the first iteration is attempted.
        Therefore any errors will not occur until then.

        Args:
            query: A Temporal visibility list filter for nexus operations. Required.
            limit: Maximum number of operations to return. If unset, all
                operations are returned. Only applies if using the
                returned :py:class:`NexusOperationExecutionAsyncIterator`
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
        return self._impl.list_nexus_operations(
            ListNexusOperationsInput(
                query=query,
                page_size=page_size,
                next_page_token=next_page_token,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
                limit=limit,
            )
        )

    async def count_nexus_operations(
        self,
        query: str | None = None,
        *,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> NexusOperationExecutionCount:
        """Count standalone Nexus operations.

        .. warning::
           This API is experimental and unstable.

        Args:
            query: A Temporal visibility filter for nexus operations.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.

        Returns:
            Count of nexus operations.
        """
        return await self._impl.count_nexus_operations(
            CountNexusOperationsInput(
                query=query, rpc_metadata=rpc_metadata, rpc_timeout=rpc_timeout
            )
        )

    @overload
    def get_nexus_operation_handle(
        self,
        operation_id: str,
        *,
        run_id: str | None = None,
    ) -> NexusOperationHandle[Any]: ...

    @overload
    def get_nexus_operation_handle(
        self,
        operation_id: str,
        *,
        run_id: str | None = None,
        result_type: type[ReturnType],
    ) -> NexusOperationHandle[ReturnType]: ...

    @overload
    def get_nexus_operation_handle(
        self,
        operation_id: str,
        *,
        operation: nexusrpc.Operation[Any, OutputT],
        run_id: str | None = None,
    ) -> NexusOperationHandle[OutputT]: ...

    def get_nexus_operation_handle(
        self,
        operation_id: str,
        *,
        operation: nexusrpc.Operation[Any, Any] | None = None,
        run_id: str | None = None,
        result_type: type | None = None,
    ) -> NexusOperationHandle[Any]:
        """Get a handle to an existing standalone Nexus operation.

        .. warning::
           This API is experimental and unstable.

        Args:
            operation_id: The operation ID.
            operation: A ``nexusrpc.Operation`` from which the result type
                is extracted. If both ``operation`` and ``result_type`` are
                provided, the ``result_type`` takes precedence.
            run_id: The operation run ID. If not provided, targets the latest run.
            result_type: The result type to deserialize into.

        Returns:
            A handle to the operation.
        """
        result_type = result_type or (operation.output_type if operation else None)
        return NexusOperationHandle(
            self,
            operation_id,
            run_id=run_id,
            result_type=result_type,
        )


class ClientConnectConfig(TypedDict, total=False):
    """TypedDict of keyword arguments for :py:meth:`Client.connect`."""

    target_host: str
    namespace: str
    api_key: str | None
    data_converter: temporalio.converter.DataConverter
    plugins: Sequence[Plugin]
    interceptors: Sequence[Interceptor]
    default_workflow_query_reject_condition: (
        temporalio.common.QueryRejectCondition | None
    )
    tls: bool | TLSConfig | None
    retry_config: RetryConfig | None
    keep_alive_config: KeepAliveConfig | None
    rpc_metadata: Mapping[str, str | bytes]
    identity: str | None
    lazy: bool
    runtime: temporalio.runtime.Runtime | None
    http_connect_proxy_config: HttpConnectProxyConfig | None
    dns_load_balancing_config: DnsLoadBalancingConfig | None
    grpc_compression: GrpcCompression
    payload_limits: PayloadLimitsConfig
    header_codec_behavior: HeaderCodecBehavior


class ClientConfig(TypedDict, total=False):
    """TypedDict of config originally passed to :py:meth:`Client`."""

    service_client: Required[temporalio.service.ServiceClient]
    namespace: Required[str]
    data_converter: Required[temporalio.converter.DataConverter]
    plugins: Required[Sequence[Plugin]]
    interceptors: Required[Sequence[Interceptor]]
    default_workflow_query_reject_condition: Required[
        temporalio.common.QueryRejectCondition | None
    ]
    header_codec_behavior: Required[HeaderCodecBehavior]
