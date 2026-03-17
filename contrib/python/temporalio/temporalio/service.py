"""Underlying gRPC services."""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import timedelta
from enum import IntEnum
from typing import ClassVar, Generic, Mapping, Optional, Tuple, Type, TypeVar, Union

import google.protobuf.empty_pb2
import google.protobuf.message

import temporalio.api.cloud.cloudservice.v1
import temporalio.api.common.v1
import temporalio.api.operatorservice.v1
import temporalio.api.testservice.v1
import temporalio.api.workflowservice.v1
import temporalio.bridge.client
import temporalio.bridge.proto.health.v1
import temporalio.exceptions
import temporalio.runtime

__version__ = "1.16.0"

ServiceRequest = TypeVar("ServiceRequest", bound=google.protobuf.message.Message)
ServiceResponse = TypeVar("ServiceResponse", bound=google.protobuf.message.Message)

logger = logging.getLogger(__name__)

# Set to true to log all requests and responses
LOG_PROTOS = False


@dataclass
class TLSConfig:
    """TLS configuration for connecting to Temporal server."""

    server_root_ca_cert: Optional[bytes] = None
    """Root CA to validate the server certificate against."""

    domain: Optional[str] = None
    """TLS domain."""

    client_cert: Optional[bytes] = None
    """Client certificate for mTLS.

    This must be combined with :py:attr:`client_private_key`."""

    client_private_key: Optional[bytes] = None
    """Client private key for mTLS.

    This must be combined with :py:attr:`client_cert`."""

    def _to_bridge_config(self) -> temporalio.bridge.client.ClientTlsConfig:
        return temporalio.bridge.client.ClientTlsConfig(
            server_root_ca_cert=self.server_root_ca_cert,
            domain=self.domain,
            client_cert=self.client_cert,
            client_private_key=self.client_private_key,
        )


@dataclass
class RetryConfig:
    """Retry configuration for server calls."""

    initial_interval_millis: int = 100
    """Initial backoff interval."""
    randomization_factor: float = 0.2
    """Randomization jitter to add."""
    multiplier: float = 1.5
    """Backoff multiplier."""
    max_interval_millis: int = 5000
    """Maximum backoff interval."""
    max_elapsed_time_millis: Optional[int] = 10000
    """Maximum total time."""
    max_retries: int = 10
    """Maximum number of retries."""

    def _to_bridge_config(self) -> temporalio.bridge.client.ClientRetryConfig:
        return temporalio.bridge.client.ClientRetryConfig(
            initial_interval_millis=self.initial_interval_millis,
            randomization_factor=self.randomization_factor,
            multiplier=self.multiplier,
            max_interval_millis=self.max_interval_millis,
            max_elapsed_time_millis=self.max_elapsed_time_millis,
            max_retries=self.max_retries,
        )


@dataclass(frozen=True)
class KeepAliveConfig:
    """Keep-alive configuration for client connections."""

    interval_millis: int = 30000
    """Interval to send HTTP2 keep alive pings."""
    timeout_millis: int = 15000
    """Timeout that the keep alive must be responded to within or the connection
    will be closed."""
    default: ClassVar[KeepAliveConfig]
    """Default keep alive config."""

    def _to_bridge_config(self) -> temporalio.bridge.client.ClientKeepAliveConfig:
        return temporalio.bridge.client.ClientKeepAliveConfig(
            interval_millis=self.interval_millis,
            timeout_millis=self.timeout_millis,
        )


KeepAliveConfig.default = KeepAliveConfig()


@dataclass(frozen=True)
class HttpConnectProxyConfig:
    """Configuration for HTTP CONNECT proxy for client connections."""

    target_host: str
    """Target host:port for the HTTP CONNECT proxy."""
    basic_auth: Optional[Tuple[str, str]] = None
    """Basic auth for the HTTP CONNECT proxy if any as a user/pass tuple."""

    def _to_bridge_config(
        self,
    ) -> temporalio.bridge.client.ClientHttpConnectProxyConfig:
        return temporalio.bridge.client.ClientHttpConnectProxyConfig(
            target_host=self.target_host,
            basic_auth=self.basic_auth,
        )


@dataclass
class ConnectConfig:
    """Config for connecting to the server."""

    target_host: str
    api_key: Optional[str] = None
    tls: Union[bool, TLSConfig] = False
    retry_config: Optional[RetryConfig] = None
    keep_alive_config: Optional[KeepAliveConfig] = KeepAliveConfig.default
    rpc_metadata: Mapping[str, str] = field(default_factory=dict)
    identity: str = ""
    lazy: bool = False
    runtime: Optional[temporalio.runtime.Runtime] = None
    http_connect_proxy_config: Optional[HttpConnectProxyConfig] = None

    def __post_init__(self) -> None:
        """Set extra defaults on unset properties."""
        if not self.identity:
            self.identity = f"{os.getpid()}@{socket.gethostname()}"

    def _to_bridge_config(self) -> temporalio.bridge.client.ClientConfig:
        # Need to create the URL from the host:port. We allowed scheme in the
        # past so we'll leave it for only one more version with a warning.
        # Otherwise we'll prepend the scheme.
        target_url: str
        tls_config: Optional[temporalio.bridge.client.ClientTlsConfig]
        if "://" in self.target_host:
            warnings.warn(
                "Target host as URL with scheme no longer supported. This will be an error in future versions."
            )
            target_url = self.target_host
            tls_config = (
                self.tls._to_bridge_config()
                if isinstance(self.tls, TLSConfig)
                else None
            )
        elif isinstance(self.tls, TLSConfig):
            target_url = f"https://{self.target_host}"
            tls_config = self.tls._to_bridge_config()
        elif self.tls:
            target_url = f"https://{self.target_host}"
            tls_config = TLSConfig()._to_bridge_config()
        else:
            target_url = f"http://{self.target_host}"
            tls_config = None

        return temporalio.bridge.client.ClientConfig(
            target_url=target_url,
            api_key=self.api_key,
            tls_config=tls_config,
            retry_config=self.retry_config._to_bridge_config()
            if self.retry_config
            else None,
            keep_alive_config=self.keep_alive_config._to_bridge_config()
            if self.keep_alive_config
            else None,
            metadata=self.rpc_metadata,
            identity=self.identity,
            client_name="temporal-python",
            client_version=__version__,
            http_connect_proxy_config=self.http_connect_proxy_config._to_bridge_config()
            if self.http_connect_proxy_config
            else None,
        )


class ServiceClient(ABC):
    """Direct client to Temporal services."""

    @staticmethod
    async def connect(config: ConnectConfig) -> ServiceClient:
        """Connect directly to Temporal services."""
        return await _BridgeServiceClient.connect(config)

    def __init__(self, config: ConnectConfig) -> None:
        """Initialize the base service client."""
        super().__init__()
        self.config = config
        self.workflow_service = WorkflowService(self)
        self.operator_service = OperatorService(self)
        self.cloud_service = CloudService(self)
        self.test_service = TestService(self)
        self._check_health_call = self._new_call(
            "check",
            temporalio.bridge.proto.health.v1.HealthCheckRequest,
            temporalio.bridge.proto.health.v1.HealthCheckResponse,
            service="health",
        )

    async def check_health(
        self,
        *,
        service: str = "temporal.api.workflowservice.v1.WorkflowService",
        retry: bool = False,
        metadata: Mapping[str, str] = {},
        timeout: Optional[timedelta] = None,
    ) -> bool:
        """Check whether the WorkflowService is up.

        In addition to accepting which service to check health on, this accepts
        some of the same parameters as other RPC calls. See
        :py:meth:`ServiceCall.__call__`.

        Returns:
            True when available, false if the server is running but the service
            is unavailable (rare), or raises an error if server/service cannot
            be reached.
        """
        resp = await self._check_health_call(
            temporalio.bridge.proto.health.v1.HealthCheckRequest(service=service),
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )
        return (
            resp.status
            == temporalio.bridge.proto.health.v1.HealthCheckResponse.ServingStatus.SERVING
        )

    @property
    @abstractmethod
    def worker_service_client(self) -> _BridgeServiceClient:
        """Underlying service client."""
        raise NotImplementedError

    @abstractmethod
    def update_rpc_metadata(self, metadata: Mapping[str, str]) -> None:
        """Update service client's RPC metadata."""
        raise NotImplementedError

    @abstractmethod
    def update_api_key(self, api_key: Optional[str]) -> None:
        """Update service client's API key."""
        raise NotImplementedError

    @abstractmethod
    async def _rpc_call(
        self,
        rpc: str,
        req: google.protobuf.message.Message,
        resp_type: Type[ServiceResponse],
        *,
        service: str,
        retry: bool,
        metadata: Mapping[str, str],
        timeout: Optional[timedelta],
    ) -> ServiceResponse:
        raise NotImplementedError

    def _new_call(
        self,
        name: str,
        req_type: Type[ServiceRequest],
        resp_type: Type[ServiceResponse],
        *,
        service: str = "workflow",
    ) -> ServiceCall[ServiceRequest, ServiceResponse]:
        return ServiceCall(self, name, req_type, resp_type, service)


class WorkflowService:
    """Client to the Temporal server's workflow service."""

    def __init__(self, client: ServiceClient) -> None:
        """Initialize the workflow service."""
        wsv1 = temporalio.api.workflowservice.v1
        self.count_workflow_executions = client._new_call(
            "count_workflow_executions",
            wsv1.CountWorkflowExecutionsRequest,
            wsv1.CountWorkflowExecutionsResponse,
        )
        self.create_schedule = client._new_call(
            "create_schedule",
            wsv1.CreateScheduleRequest,
            wsv1.CreateScheduleResponse,
        )
        self.create_workflow_rule = client._new_call(
            "create_workflow_rule",
            wsv1.CreateWorkflowRuleRequest,
            wsv1.CreateWorkflowRuleRequest,
        )
        self.delete_schedule = client._new_call(
            "delete_schedule",
            wsv1.DeleteScheduleRequest,
            wsv1.DeleteScheduleResponse,
        )
        self.delete_worker_deployment = client._new_call(
            "delete_worker_deployment",
            wsv1.DeleteWorkerDeploymentRequest,
            wsv1.DeleteWorkerDeploymentResponse,
        )
        self.delete_worker_deployment_version = client._new_call(
            "delete_worker_deployment_version",
            wsv1.DeleteWorkerDeploymentVersionRequest,
            wsv1.DeleteWorkerDeploymentVersionResponse,
        )
        self.delete_workflow_execution = client._new_call(
            "delete_workflow_execution",
            wsv1.DeleteWorkflowExecutionRequest,
            wsv1.DeleteWorkflowExecutionResponse,
        )
        self.delete_workflow_rule = client._new_call(
            "delete_workflow_rule",
            wsv1.DeleteWorkflowRuleRequest,
            wsv1.DeleteWorkflowRuleResponse,
        )
        self.describe_batch_operation = client._new_call(
            "describe_batch_operation",
            wsv1.DescribeBatchOperationRequest,
            wsv1.DescribeBatchOperationResponse,
        )
        self.describe_deployment = client._new_call(
            "describe_deployment",
            wsv1.DescribeDeploymentRequest,
            wsv1.DescribeDeploymentResponse,
        )
        self.deprecate_namespace = client._new_call(
            "deprecate_namespace",
            wsv1.DeprecateNamespaceRequest,
            wsv1.DeprecateNamespaceResponse,
        )
        self.describe_namespace = client._new_call(
            "describe_namespace",
            wsv1.DescribeNamespaceRequest,
            wsv1.DescribeNamespaceResponse,
        )
        self.describe_schedule = client._new_call(
            "describe_schedule",
            wsv1.DescribeScheduleRequest,
            wsv1.DescribeScheduleResponse,
        )
        self.describe_task_queue = client._new_call(
            "describe_task_queue",
            wsv1.DescribeTaskQueueRequest,
            wsv1.DescribeTaskQueueResponse,
        )
        self.describe_worker_deployment = client._new_call(
            "describe_worker_deployment",
            wsv1.DescribeWorkerDeploymentRequest,
            wsv1.DescribeWorkerDeploymentResponse,
        )
        self.describe_worker_deployment_version = client._new_call(
            "describe_worker_deployment_version",
            wsv1.DescribeWorkerDeploymentVersionRequest,
            wsv1.DescribeWorkerDeploymentVersionResponse,
        )
        self.describe_workflow_execution = client._new_call(
            "describe_workflow_execution",
            wsv1.DescribeWorkflowExecutionRequest,
            wsv1.DescribeWorkflowExecutionResponse,
        )
        self.describe_workflow_rule = client._new_call(
            "describe_workflow_rule",
            wsv1.DescribeWorkflowRuleRequest,
            wsv1.DescribeWorkflowRuleResponse,
        )
        self.execute_multi_operation = client._new_call(
            "execute_multi_operation",
            wsv1.ExecuteMultiOperationRequest,
            wsv1.ExecuteMultiOperationResponse,
        )
        self.fetch_worker_config = client._new_call(
            "fetch_worker_config",
            wsv1.FetchWorkerConfigRequest,
            wsv1.FetchWorkerConfigResponse,
        )
        self.get_cluster_info = client._new_call(
            "get_cluster_info",
            wsv1.GetClusterInfoRequest,
            wsv1.GetClusterInfoResponse,
        )
        self.get_current_deployment = client._new_call(
            "get_current_deployment",
            wsv1.GetCurrentDeploymentRequest,
            wsv1.GetCurrentDeploymentResponse,
        )
        self.get_deployment_reachability = client._new_call(
            "get_deployment_reachability",
            wsv1.GetDeploymentReachabilityRequest,
            wsv1.GetDeploymentReachabilityResponse,
        )
        self.get_search_attributes = client._new_call(
            "get_search_attributes",
            wsv1.GetSearchAttributesRequest,
            wsv1.GetSearchAttributesResponse,
        )
        self.get_system_info = client._new_call(
            "get_system_info",
            wsv1.GetSystemInfoRequest,
            wsv1.GetSystemInfoResponse,
        )
        self.get_worker_build_id_compatibility = client._new_call(
            "get_worker_build_id_compatibility",
            wsv1.GetWorkerBuildIdCompatibilityRequest,
            wsv1.GetWorkerBuildIdCompatibilityResponse,
        )
        self.get_worker_task_reachability = client._new_call(
            "get_worker_task_reachability",
            wsv1.GetWorkerTaskReachabilityRequest,
            wsv1.GetWorkerTaskReachabilityResponse,
        )
        self.get_worker_versioning_rules = client._new_call(
            "get_worker_versioning_rules",
            wsv1.GetWorkerVersioningRulesRequest,
            wsv1.GetWorkerVersioningRulesResponse,
        )
        self.get_workflow_execution_history = client._new_call(
            "get_workflow_execution_history",
            wsv1.GetWorkflowExecutionHistoryRequest,
            wsv1.GetWorkflowExecutionHistoryResponse,
        )
        self.get_workflow_execution_history_reverse = client._new_call(
            "get_workflow_execution_history_reverse",
            wsv1.GetWorkflowExecutionHistoryReverseRequest,
            wsv1.GetWorkflowExecutionHistoryReverseResponse,
        )
        self.list_archived_workflow_executions = client._new_call(
            "list_archived_workflow_executions",
            wsv1.ListArchivedWorkflowExecutionsRequest,
            wsv1.ListArchivedWorkflowExecutionsResponse,
        )
        self.list_batch_operations = client._new_call(
            "list_batch_operations",
            wsv1.ListBatchOperationsRequest,
            wsv1.ListBatchOperationsResponse,
        )
        self.list_closed_workflow_executions = client._new_call(
            "list_closed_workflow_executions",
            wsv1.ListClosedWorkflowExecutionsRequest,
            wsv1.ListClosedWorkflowExecutionsResponse,
        )
        self.list_deployments = client._new_call(
            "list_deployments",
            wsv1.ListDeploymentsRequest,
            wsv1.ListDeploymentsResponse,
        )
        self.list_namespaces = client._new_call(
            "list_namespaces",
            wsv1.ListNamespacesRequest,
            wsv1.ListNamespacesResponse,
        )
        self.list_open_workflow_executions = client._new_call(
            "list_open_workflow_executions",
            wsv1.ListOpenWorkflowExecutionsRequest,
            wsv1.ListOpenWorkflowExecutionsResponse,
        )
        self.list_schedule_matching_times = client._new_call(
            "list_schedule_matching_times",
            wsv1.ListScheduleMatchingTimesRequest,
            wsv1.ListScheduleMatchingTimesResponse,
        )
        self.list_schedules = client._new_call(
            "list_schedules",
            wsv1.ListSchedulesRequest,
            wsv1.ListSchedulesResponse,
        )
        self.list_task_queue_partitions = client._new_call(
            "list_task_queue_partitions",
            wsv1.ListTaskQueuePartitionsRequest,
            wsv1.ListTaskQueuePartitionsResponse,
        )
        self.list_worker_deployments = client._new_call(
            "list_worker_deployments",
            wsv1.ListWorkerDeploymentsRequest,
            wsv1.ListWorkerDeploymentsResponse,
        )
        self.list_workflow_executions = client._new_call(
            "list_workflow_executions",
            wsv1.ListWorkflowExecutionsRequest,
            wsv1.ListWorkflowExecutionsResponse,
        )
        self.list_workers = client._new_call(
            "list_workers",
            wsv1.ListWorkersRequest,
            wsv1.ListWorkersResponse,
        )
        self.list_workflow_rules = client._new_call(
            "list_workflow_rules",
            wsv1.ListWorkflowRulesRequest,
            wsv1.ListWorkflowRulesResponse,
        )
        self.patch_schedule = client._new_call(
            "patch_schedule",
            wsv1.PatchScheduleRequest,
            wsv1.PatchScheduleResponse,
        )
        self.pause_activity = client._new_call(
            "pause_activity",
            wsv1.PauseActivityRequest,
            wsv1.PauseActivityResponse,
        )
        self.poll_activity_task_queue = client._new_call(
            "poll_activity_task_queue",
            wsv1.PollActivityTaskQueueRequest,
            wsv1.PollActivityTaskQueueResponse,
        )
        self.poll_nexus_task_queue = client._new_call(
            "poll_nexus_task_queue",
            wsv1.PollNexusTaskQueueRequest,
            wsv1.PollNexusTaskQueueResponse,
        )
        self.poll_workflow_execution_update = client._new_call(
            "poll_workflow_execution_update",
            wsv1.PollWorkflowExecutionUpdateRequest,
            wsv1.PollWorkflowExecutionUpdateResponse,
        )
        self.poll_workflow_task_queue = client._new_call(
            "poll_workflow_task_queue",
            wsv1.PollWorkflowTaskQueueRequest,
            wsv1.PollWorkflowTaskQueueResponse,
        )
        self.query_workflow = client._new_call(
            "query_workflow",
            wsv1.QueryWorkflowRequest,
            wsv1.QueryWorkflowResponse,
        )
        self.record_activity_task_heartbeat = client._new_call(
            "record_activity_task_heartbeat",
            wsv1.RecordActivityTaskHeartbeatRequest,
            wsv1.RecordActivityTaskHeartbeatResponse,
        )
        self.record_activity_task_heartbeat_by_id = client._new_call(
            "record_activity_task_heartbeat_by_id",
            wsv1.RecordActivityTaskHeartbeatByIdRequest,
            wsv1.RecordActivityTaskHeartbeatByIdResponse,
        )
        self.record_worker_heartbeat = client._new_call(
            "record_worker_heartbeat",
            wsv1.RecordWorkerHeartbeatRequest,
            wsv1.RecordWorkerHeartbeatResponse,
        )
        self.register_namespace = client._new_call(
            "register_namespace",
            wsv1.RegisterNamespaceRequest,
            wsv1.RegisterNamespaceResponse,
        )
        self.request_cancel_workflow_execution = client._new_call(
            "request_cancel_workflow_execution",
            wsv1.RequestCancelWorkflowExecutionRequest,
            wsv1.RequestCancelWorkflowExecutionResponse,
        )
        self.reset_activity = client._new_call(
            "reset_activity",
            wsv1.ResetActivityRequest,
            wsv1.ResetActivityResponse,
        )
        self.reset_sticky_task_queue = client._new_call(
            "reset_sticky_task_queue",
            wsv1.ResetStickyTaskQueueRequest,
            wsv1.ResetStickyTaskQueueResponse,
        )
        self.reset_workflow_execution = client._new_call(
            "reset_workflow_execution",
            wsv1.ResetWorkflowExecutionRequest,
            wsv1.ResetWorkflowExecutionResponse,
        )
        self.respond_activity_task_canceled = client._new_call(
            "respond_activity_task_canceled",
            wsv1.RespondActivityTaskCanceledRequest,
            wsv1.RespondActivityTaskCanceledResponse,
        )
        self.respond_activity_task_canceled_by_id = client._new_call(
            "respond_activity_task_canceled_by_id",
            wsv1.RespondActivityTaskCanceledByIdRequest,
            wsv1.RespondActivityTaskCanceledByIdResponse,
        )
        self.respond_activity_task_completed = client._new_call(
            "respond_activity_task_completed",
            wsv1.RespondActivityTaskCompletedRequest,
            wsv1.RespondActivityTaskCompletedResponse,
        )
        self.respond_activity_task_completed_by_id = client._new_call(
            "respond_activity_task_completed_by_id",
            wsv1.RespondActivityTaskCompletedByIdRequest,
            wsv1.RespondActivityTaskCompletedByIdResponse,
        )
        self.respond_activity_task_failed = client._new_call(
            "respond_activity_task_failed",
            wsv1.RespondActivityTaskFailedRequest,
            wsv1.RespondActivityTaskFailedResponse,
        )
        self.respond_activity_task_failed_by_id = client._new_call(
            "respond_activity_task_failed_by_id",
            wsv1.RespondActivityTaskFailedByIdRequest,
            wsv1.RespondActivityTaskFailedByIdResponse,
        )
        self.respond_nexus_task_completed = client._new_call(
            "respond_nexus_task_completed",
            wsv1.RespondNexusTaskCompletedRequest,
            wsv1.RespondNexusTaskCompletedResponse,
        )
        self.respond_nexus_task_failed = client._new_call(
            "respond_nexus_task_failed",
            wsv1.RespondNexusTaskFailedRequest,
            wsv1.RespondNexusTaskFailedResponse,
        )
        self.respond_query_task_completed = client._new_call(
            "respond_query_task_completed",
            wsv1.RespondQueryTaskCompletedRequest,
            wsv1.RespondQueryTaskCompletedResponse,
        )
        self.respond_workflow_task_completed = client._new_call(
            "respond_workflow_task_completed",
            wsv1.RespondWorkflowTaskCompletedRequest,
            wsv1.RespondWorkflowTaskCompletedResponse,
        )
        self.respond_workflow_task_failed = client._new_call(
            "respond_workflow_task_failed",
            wsv1.RespondWorkflowTaskFailedRequest,
            wsv1.RespondWorkflowTaskFailedResponse,
        )
        self.scan_workflow_executions = client._new_call(
            "scan_workflow_executions",
            wsv1.ScanWorkflowExecutionsRequest,
            wsv1.ScanWorkflowExecutionsResponse,
        )
        self.set_current_deployment = client._new_call(
            "set_current_deployment",
            wsv1.SetCurrentDeploymentRequest,
            wsv1.SetCurrentDeploymentResponse,
        )
        self.set_worker_deployment_current_version = client._new_call(
            "set_worker_deployment_current_version",
            wsv1.SetWorkerDeploymentCurrentVersionRequest,
            wsv1.SetWorkerDeploymentCurrentVersionResponse,
        )
        self.set_worker_deployment_ramping_version = client._new_call(
            "set_worker_deployment_ramping_version",
            wsv1.SetWorkerDeploymentRampingVersionRequest,
            wsv1.SetWorkerDeploymentRampingVersionResponse,
        )
        self.shutdown_worker = client._new_call(
            "shutdown_worker",
            wsv1.ShutdownWorkerRequest,
            wsv1.ShutdownWorkerResponse,
        )
        self.signal_with_start_workflow_execution = client._new_call(
            "signal_with_start_workflow_execution",
            wsv1.SignalWithStartWorkflowExecutionRequest,
            wsv1.SignalWithStartWorkflowExecutionResponse,
        )
        self.signal_workflow_execution = client._new_call(
            "signal_workflow_execution",
            wsv1.SignalWorkflowExecutionRequest,
            wsv1.SignalWorkflowExecutionResponse,
        )
        self.start_batch_operation = client._new_call(
            "start_batch_operation",
            wsv1.StartBatchOperationRequest,
            wsv1.StartBatchOperationResponse,
        )
        self.start_workflow_execution = client._new_call(
            "start_workflow_execution",
            wsv1.StartWorkflowExecutionRequest,
            wsv1.StartWorkflowExecutionResponse,
        )
        self.stop_batch_operation = client._new_call(
            "stop_batch_operation",
            wsv1.StopBatchOperationRequest,
            wsv1.StopBatchOperationResponse,
        )
        self.terminate_workflow_execution = client._new_call(
            "terminate_workflow_execution",
            wsv1.TerminateWorkflowExecutionRequest,
            wsv1.TerminateWorkflowExecutionResponse,
        )
        self.trigger_workflow_rule = client._new_call(
            "trigger_workflow_rule",
            wsv1.TriggerWorkflowRuleRequest,
            wsv1.TriggerWorkflowRuleResponse,
        )
        self.unpause_activity = client._new_call(
            "unpause_activity",
            wsv1.UnpauseActivityRequest,
            wsv1.UnpauseActivityResponse,
        )
        self.update_activity_options = client._new_call(
            "update_activity_options",
            wsv1.UpdateActivityOptionsRequest,
            wsv1.UpdateActivityOptionsResponse,
        )
        self.update_namespace = client._new_call(
            "update_namespace",
            wsv1.UpdateNamespaceRequest,
            wsv1.UpdateNamespaceResponse,
        )
        self.update_schedule = client._new_call(
            "update_schedule",
            wsv1.UpdateScheduleRequest,
            wsv1.UpdateScheduleResponse,
        )
        self.update_task_queue_config = client._new_call(
            "update_task_queue_config",
            wsv1.UpdateTaskQueueConfigRequest,
            wsv1.UpdateTaskQueueConfigResponse,
        )
        self.update_worker_config = client._new_call(
            "update_worker_config",
            wsv1.UpdateWorkerConfigRequest,
            wsv1.UpdateWorkerConfigResponse,
        )
        self.update_worker_deployment_version_metadata = client._new_call(
            "update_worker_deployment_version_metadata",
            wsv1.UpdateWorkerDeploymentVersionMetadataRequest,
            wsv1.UpdateWorkerDeploymentVersionMetadataResponse,
        )
        self.update_worker_build_id_compatibility = client._new_call(
            "update_worker_build_id_compatibility",
            wsv1.UpdateWorkerBuildIdCompatibilityRequest,
            wsv1.UpdateWorkerBuildIdCompatibilityResponse,
        )
        self.update_worker_versioning_rules = client._new_call(
            "update_worker_versioning_rules",
            wsv1.UpdateWorkerVersioningRulesRequest,
            wsv1.UpdateWorkerVersioningRulesResponse,
        )
        self.update_workflow_execution = client._new_call(
            "update_workflow_execution",
            wsv1.UpdateWorkflowExecutionRequest,
            wsv1.UpdateWorkflowExecutionResponse,
        )
        self.update_workflow_execution_options = client._new_call(
            "update_workflow_execution_options",
            wsv1.UpdateWorkflowExecutionOptionsRequest,
            wsv1.UpdateWorkflowExecutionOptionsResponse,
        )


class OperatorService:
    """Client to the Temporal server's operator service."""

    def __init__(self, client: ServiceClient) -> None:
        """Initialize the operator service."""
        osv1 = temporalio.api.operatorservice.v1
        self.add_or_update_remote_cluster = client._new_call(
            "add_or_update_remote_cluster",
            osv1.AddOrUpdateRemoteClusterRequest,
            osv1.AddOrUpdateRemoteClusterResponse,
            service="operator",
        )
        self.add_search_attributes = client._new_call(
            "add_search_attributes",
            osv1.AddSearchAttributesRequest,
            osv1.AddSearchAttributesResponse,
            service="operator",
        )
        self.create_nexus_endpoint = client._new_call(
            "create_nexus_endpoint",
            osv1.CreateNexusEndpointRequest,
            osv1.CreateNexusEndpointResponse,
            service="operator",
        )
        self.delete_nexus_endpoint = client._new_call(
            "delete_nexus_endpoint",
            osv1.DeleteNexusEndpointRequest,
            osv1.DeleteNexusEndpointResponse,
            service="operator",
        )
        self.delete_namespace = client._new_call(
            "delete_namespace",
            osv1.DeleteNamespaceRequest,
            osv1.DeleteNamespaceResponse,
            service="operator",
        )
        self.get_nexus_endpoint = client._new_call(
            "get_nexus_endpoint",
            osv1.GetNexusEndpointRequest,
            osv1.GetNexusEndpointResponse,
            service="operator",
        )
        self.list_clusters = client._new_call(
            "list_clusters",
            osv1.ListClustersRequest,
            osv1.ListClustersResponse,
            service="operator",
        )
        self.list_nexus_endpoints = client._new_call(
            "list_nexus_endpoints",
            osv1.ListNexusEndpointsRequest,
            osv1.ListNexusEndpointsResponse,
            service="operator",
        )
        self.list_search_attributes = client._new_call(
            "list_search_attributes",
            osv1.ListSearchAttributesRequest,
            osv1.ListSearchAttributesResponse,
            service="operator",
        )
        self.remove_remote_cluster = client._new_call(
            "remove_remote_cluster",
            osv1.RemoveRemoteClusterRequest,
            osv1.RemoveRemoteClusterResponse,
            service="operator",
        )
        self.remove_search_attributes = client._new_call(
            "remove_search_attributes",
            osv1.RemoveSearchAttributesRequest,
            osv1.RemoveSearchAttributesResponse,
            service="operator",
        )
        self.update_nexus_endpoint = client._new_call(
            "update_nexus_endpoint",
            osv1.UpdateNexusEndpointRequest,
            osv1.UpdateNexusEndpointResponse,
            service="operator",
        )


class CloudService:
    """Client to the Temporal server's cloud service."""

    def __init__(self, client: ServiceClient) -> None:
        """Initialize the cloud service."""
        clv1 = temporalio.api.cloud.cloudservice.v1
        self.add_namespace_region = client._new_call(
            "add_namespace_region",
            clv1.AddNamespaceRegionRequest,
            clv1.AddNamespaceRegionResponse,
            service="cloud",
        )
        self.add_user_group_member = client._new_call(
            "add_user_group_member",
            clv1.AddUserGroupMemberRequest,
            clv1.AddUserGroupMemberResponse,
            service="cloud",
        )
        self.create_api_key = client._new_call(
            "create_api_key",
            clv1.CreateApiKeyRequest,
            clv1.CreateApiKeyResponse,
            service="cloud",
        )
        self.create_connectivity_rule = client._new_call(
            "create_connectivity_rule",
            clv1.CreateConnectivityRuleRequest,
            clv1.CreateConnectivityRuleResponse,
            service="cloud",
        )
        self.create_namespace = client._new_call(
            "create_namespace",
            clv1.CreateNamespaceRequest,
            clv1.CreateNamespaceResponse,
            service="cloud",
        )
        self.create_namespace_export_sink = client._new_call(
            "create_namespace_export_sink",
            clv1.CreateNamespaceExportSinkRequest,
            clv1.CreateNamespaceExportSinkResponse,
            service="cloud",
        )
        self.create_nexus_endpoint = client._new_call(
            "create_nexus_endpoint",
            clv1.CreateNexusEndpointRequest,
            clv1.CreateNexusEndpointResponse,
            service="cloud",
        )
        self.create_service_account = client._new_call(
            "create_service_account",
            clv1.CreateServiceAccountRequest,
            clv1.CreateServiceAccountResponse,
            service="cloud",
        )
        self.create_user_group = client._new_call(
            "create_user_group",
            clv1.CreateUserGroupRequest,
            clv1.CreateUserGroupResponse,
            service="cloud",
        )
        self.create_user = client._new_call(
            "create_user",
            clv1.CreateUserRequest,
            clv1.CreateUserResponse,
            service="cloud",
        )
        self.delete_api_key = client._new_call(
            "delete_api_key",
            clv1.DeleteApiKeyRequest,
            clv1.DeleteApiKeyResponse,
            service="cloud",
        )
        self.delete_connectivity_rule = client._new_call(
            "delete_connectivity_rule",
            clv1.DeleteConnectivityRuleRequest,
            clv1.DeleteConnectivityRuleResponse,
            service="cloud",
        )
        self.delete_namespace = client._new_call(
            "delete_namespace",
            clv1.DeleteNamespaceRequest,
            clv1.DeleteNamespaceResponse,
            service="cloud",
        )
        self.delete_namespace_export_sink = client._new_call(
            "delete_namespace_export_sink",
            clv1.DeleteNamespaceExportSinkRequest,
            clv1.DeleteNamespaceExportSinkResponse,
            service="cloud",
        )
        self.delete_namespace_region = client._new_call(
            "delete_namespace_region",
            clv1.DeleteNamespaceRegionRequest,
            clv1.DeleteNamespaceRegionResponse,
            service="cloud",
        )
        self.delete_nexus_endpoint = client._new_call(
            "delete_nexus_endpoint",
            clv1.DeleteNexusEndpointRequest,
            clv1.DeleteNexusEndpointResponse,
            service="cloud",
        )
        self.delete_service_account = client._new_call(
            "delete_service_account",
            clv1.DeleteServiceAccountRequest,
            clv1.DeleteServiceAccountResponse,
            service="cloud",
        )
        self.delete_user_group = client._new_call(
            "delete_user_group",
            clv1.DeleteUserGroupRequest,
            clv1.DeleteUserGroupResponse,
            service="cloud",
        )
        self.delete_user = client._new_call(
            "delete_user",
            clv1.DeleteUserRequest,
            clv1.DeleteUserResponse,
            service="cloud",
        )
        self.failover_namespace_region = client._new_call(
            "failover_namespace_region",
            clv1.FailoverNamespaceRegionRequest,
            clv1.FailoverNamespaceRegionResponse,
            service="cloud",
        )
        self.get_account = client._new_call(
            "get_account",
            clv1.GetAccountRequest,
            clv1.GetAccountResponse,
            service="cloud",
        )
        self.get_api_key = client._new_call(
            "get_api_key",
            clv1.GetApiKeyRequest,
            clv1.GetApiKeyResponse,
            service="cloud",
        )
        self.get_api_keys = client._new_call(
            "get_api_keys",
            clv1.GetApiKeysRequest,
            clv1.GetApiKeysResponse,
            service="cloud",
        )
        self.get_async_operation = client._new_call(
            "get_async_operation",
            clv1.GetAsyncOperationRequest,
            clv1.GetAsyncOperationResponse,
            service="cloud",
        )
        self.get_connectivity_rule = client._new_call(
            "get_connectivity_rule",
            clv1.GetConnectivityRuleRequest,
            clv1.GetConnectivityRuleResponse,
            service="cloud",
        )
        self.get_connectivity_rules = client._new_call(
            "get_connectivity_rules",
            clv1.GetConnectivityRulesRequest,
            clv1.GetConnectivityRulesResponse,
            service="cloud",
        )
        self.get_namespace = client._new_call(
            "get_namespace",
            clv1.GetNamespaceRequest,
            clv1.GetNamespaceResponse,
            service="cloud",
        )
        self.get_namespaces = client._new_call(
            "get_namespaces",
            clv1.GetNamespacesRequest,
            clv1.GetNamespacesResponse,
            service="cloud",
        )
        self.get_namespace_export_sink = client._new_call(
            "get_namespace_export_sink",
            clv1.GetNamespaceExportSinkRequest,
            clv1.GetNamespaceExportSinkResponse,
            service="cloud",
        )
        self.get_namespace_export_sinks = client._new_call(
            "get_namespace_export_sinks",
            clv1.GetNamespaceExportSinksRequest,
            clv1.GetNamespaceExportSinksResponse,
            service="cloud",
        )
        self.get_nexus_endpoint = client._new_call(
            "get_nexus_endpoint",
            clv1.GetNexusEndpointRequest,
            clv1.GetNexusEndpointResponse,
            service="cloud",
        )
        self.get_nexus_endpoints = client._new_call(
            "get_nexus_endpoints",
            clv1.GetNexusEndpointsRequest,
            clv1.GetNexusEndpointsResponse,
            service="cloud",
        )
        self.get_region = client._new_call(
            "get_region",
            clv1.GetRegionRequest,
            clv1.GetRegionResponse,
            service="cloud",
        )
        self.get_regions = client._new_call(
            "get_regions",
            clv1.GetRegionsRequest,
            clv1.GetRegionsResponse,
            service="cloud",
        )
        self.get_service_account = client._new_call(
            "get_service_account",
            clv1.GetServiceAccountRequest,
            clv1.GetServiceAccountResponse,
            service="cloud",
        )
        self.get_service_accounts = client._new_call(
            "get_service_accounts",
            clv1.GetServiceAccountsRequest,
            clv1.GetServiceAccountsResponse,
            service="cloud",
        )
        self.get_usage = client._new_call(
            "get_usage",
            clv1.GetUsageRequest,
            clv1.GetUsageResponse,
            service="cloud",
        )
        self.get_user_group = client._new_call(
            "get_user_group",
            clv1.GetUserGroupRequest,
            clv1.GetUserGroupResponse,
            service="cloud",
        )
        self.get_user_group_members = client._new_call(
            "get_user_group_members",
            clv1.GetUserGroupMembersRequest,
            clv1.GetUserGroupMembersResponse,
            service="cloud",
        )
        self.get_user_groups = client._new_call(
            "get_user_groups",
            clv1.GetUserGroupsRequest,
            clv1.GetUserGroupsResponse,
            service="cloud",
        )
        self.get_user = client._new_call(
            "get_user",
            clv1.GetUserRequest,
            clv1.GetUserResponse,
            service="cloud",
        )
        self.get_users = client._new_call(
            "get_users",
            clv1.GetUsersRequest,
            clv1.GetUsersResponse,
            service="cloud",
        )
        self.remove_user_group_member = client._new_call(
            "remove_user_group_member",
            clv1.RemoveUserGroupMemberRequest,
            clv1.RemoveUserGroupMemberResponse,
            service="cloud",
        )
        self.rename_custom_search_attribute = client._new_call(
            "rename_custom_search_attribute",
            clv1.RenameCustomSearchAttributeRequest,
            clv1.RenameCustomSearchAttributeResponse,
            service="cloud",
        )
        self.set_user_group_namespace_access = client._new_call(
            "set_user_group_namespace_access",
            clv1.SetUserGroupNamespaceAccessRequest,
            clv1.SetUserGroupNamespaceAccessResponse,
            service="cloud",
        )
        self.set_user_namespace_access = client._new_call(
            "set_user_namespace_access",
            clv1.SetUserNamespaceAccessRequest,
            clv1.SetUserNamespaceAccessResponse,
            service="cloud",
        )
        self.update_account = client._new_call(
            "update_account",
            clv1.UpdateAccountRequest,
            clv1.UpdateAccountResponse,
            service="cloud",
        )
        self.update_api_key = client._new_call(
            "update_api_key",
            clv1.UpdateApiKeyRequest,
            clv1.UpdateApiKeyResponse,
            service="cloud",
        )
        self.update_namespace = client._new_call(
            "update_namespace",
            clv1.UpdateNamespaceRequest,
            clv1.UpdateNamespaceResponse,
            service="cloud",
        )
        self.update_namespace_export_sink = client._new_call(
            "update_namespace_export_sink",
            clv1.UpdateNamespaceExportSinkRequest,
            clv1.UpdateNamespaceExportSinkResponse,
            service="cloud",
        )
        self.update_namespace_tags = client._new_call(
            "update_namespace_tags",
            clv1.UpdateNamespaceTagsRequest,
            clv1.UpdateNamespaceTagsResponse,
            service="cloud",
        )
        self.update_nexus_endpoint = client._new_call(
            "update_nexus_endpoint",
            clv1.UpdateNexusEndpointRequest,
            clv1.UpdateNexusEndpointResponse,
            service="cloud",
        )
        self.update_service_account = client._new_call(
            "update_service_account",
            clv1.UpdateServiceAccountRequest,
            clv1.UpdateServiceAccountResponse,
            service="cloud",
        )
        self.update_user_group = client._new_call(
            "update_user_group",
            clv1.UpdateUserGroupRequest,
            clv1.UpdateUserGroupResponse,
            service="cloud",
        )
        self.update_user = client._new_call(
            "update_user",
            clv1.UpdateUserRequest,
            clv1.UpdateUserResponse,
            service="cloud",
        )
        self.validate_namespace_export_sink = client._new_call(
            "validate_namespace_export_sink",
            clv1.ValidateNamespaceExportSinkRequest,
            clv1.ValidateNamespaceExportSinkResponse,
            service="cloud",
        )


class TestService:
    """Client to the Temporal test server's test service."""

    def __init__(self, client: ServiceClient) -> None:
        """Initialize the test service."""
        tsv1 = temporalio.api.testservice.v1
        self.get_current_time = client._new_call(
            "get_current_time",
            google.protobuf.empty_pb2.Empty,
            tsv1.GetCurrentTimeResponse,
            service="test",
        )
        self.lock_time_skipping = client._new_call(
            "lock_time_skipping",
            tsv1.LockTimeSkippingRequest,
            tsv1.LockTimeSkippingResponse,
            service="test",
        )
        self.sleep_until = client._new_call(
            "sleep_until",
            tsv1.SleepUntilRequest,
            tsv1.SleepResponse,
            service="test",
        )
        self.sleep = client._new_call(
            "sleep",
            tsv1.SleepRequest,
            tsv1.SleepResponse,
            service="test",
        )
        self.unlock_time_skipping_with_sleep = client._new_call(
            "unlock_time_skipping_with_sleep",
            tsv1.SleepRequest,
            tsv1.SleepResponse,
            service="test",
        )
        self.unlock_time_skipping = client._new_call(
            "unlock_time_skipping",
            tsv1.UnlockTimeSkippingRequest,
            tsv1.UnlockTimeSkippingResponse,
            service="test",
        )


class ServiceCall(Generic[ServiceRequest, ServiceResponse]):
    """Callable RPC method for services."""

    def __init__(
        self,
        service_client: ServiceClient,
        name: str,
        req_type: Type[ServiceRequest],
        resp_type: Type[ServiceResponse],
        service: str,
    ) -> None:
        """Initialize the service call."""
        self.service_client = service_client
        self.name = name
        self.req_type = req_type
        self.resp_type = resp_type
        self.service = service

    async def __call__(
        self,
        req: ServiceRequest,
        *,
        retry: bool = False,
        metadata: Mapping[str, str] = {},
        timeout: Optional[timedelta] = None,
    ) -> ServiceResponse:
        """Invoke underlying client with the given request.

        Args:
            req: Request for the call.
            retry: If true, will use retry config to retry failed calls.
            metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            timeout: Optional RPC deadline to set for the RPC call.

        Returns:
            RPC response.

        Raises:
            RPCError: Any RPC error that occurs during the call.
        """
        return await self.service_client._rpc_call(
            self.name,
            req,
            self.resp_type,
            service=self.service,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )


class _BridgeServiceClient(ServiceClient):
    @staticmethod
    async def connect(config: ConnectConfig) -> _BridgeServiceClient:
        client = _BridgeServiceClient(config)
        # If not lazy, try to connect
        if not config.lazy:
            await client._connected_client()
        return client

    def __init__(self, config: ConnectConfig) -> None:
        super().__init__(config)
        self._bridge_config = config._to_bridge_config()
        self._bridge_client: Optional[temporalio.bridge.client.Client] = None
        self._bridge_client_connect_lock = asyncio.Lock()

    async def _connected_client(self) -> temporalio.bridge.client.Client:
        async with self._bridge_client_connect_lock:
            if not self._bridge_client:
                runtime = self.config.runtime or temporalio.runtime.Runtime.default()
                self._bridge_client = await temporalio.bridge.client.Client.connect(
                    runtime._core_runtime,
                    self._bridge_config,
                )
            return self._bridge_client

    @property
    def worker_service_client(self) -> _BridgeServiceClient:
        """Underlying service client."""
        return self

    def update_rpc_metadata(self, metadata: Mapping[str, str]) -> None:
        """Update Core client metadata."""
        # Mutate the bridge config and then only mutate the running client
        # metadata if already connected
        self._bridge_config.metadata = metadata
        if self._bridge_client:
            self._bridge_client.update_metadata(metadata)

    def update_api_key(self, api_key: Optional[str]) -> None:
        """Update Core client API key."""
        # Mutate the bridge config and then only mutate the running client
        # metadata if already connected
        self._bridge_config.api_key = api_key
        if self._bridge_client:
            self._bridge_client.update_api_key(api_key)

    async def _rpc_call(
        self,
        rpc: str,
        req: google.protobuf.message.Message,
        resp_type: Type[ServiceResponse],
        *,
        service: str,
        retry: bool,
        metadata: Mapping[str, str],
        timeout: Optional[timedelta],
    ) -> ServiceResponse:
        global LOG_PROTOS
        if LOG_PROTOS:
            logger.debug("Service %s request to %s: %s", service, rpc, req)
        try:
            client = await self._connected_client()
            resp = await client.call(
                service=service,
                rpc=rpc,
                req=req,
                resp_type=resp_type,
                retry=retry,
                metadata=metadata,
                timeout=timeout,
            )
            if LOG_PROTOS:
                logger.debug("Service %s response from %s: %s", service, rpc, resp)
            return resp
        except temporalio.bridge.client.RPCError as err:
            # Intentionally swallowing the cause instead of using "from"
            status, message, details = err.args
            raise RPCError(message, RPCStatusCode(status), details)


class RPCStatusCode(IntEnum):
    """Status code for :py:class:`RPCError`."""

    OK = 0
    CANCELLED = 1
    UNKNOWN = 2
    INVALID_ARGUMENT = 3
    DEADLINE_EXCEEDED = 4
    NOT_FOUND = 5
    ALREADY_EXISTS = 6
    PERMISSION_DENIED = 7
    RESOURCE_EXHAUSTED = 8
    FAILED_PRECONDITION = 9
    ABORTED = 10
    OUT_OF_RANGE = 11
    UNIMPLEMENTED = 12
    INTERNAL = 13
    UNAVAILABLE = 14
    DATA_LOSS = 15
    UNAUTHENTICATED = 16


class RPCError(temporalio.exceptions.TemporalError):
    """Error during RPC call."""

    def __init__(
        self, message: str, status: RPCStatusCode, raw_grpc_status: bytes
    ) -> None:
        """Initialize RPC error."""
        super().__init__(message)
        self._message = message
        self._status = status
        self._raw_grpc_status = raw_grpc_status
        self._grpc_status: Optional[temporalio.api.common.v1.GrpcStatus] = None

    @property
    def message(self) -> str:
        """Message for the error."""
        return self._message

    @property
    def status(self) -> RPCStatusCode:
        """Status code for the error."""
        return self._status

    @property
    def raw_grpc_status(self) -> bytes:
        """Raw gRPC status bytes."""
        return self._raw_grpc_status

    @property
    def grpc_status(self) -> temporalio.api.common.v1.GrpcStatus:
        """Status of the gRPC call with details."""
        if self._grpc_status is None:
            status = temporalio.api.common.v1.GrpcStatus()
            status.ParseFromString(self._raw_grpc_status)
            self._grpc_status = status
        return self._grpc_status
