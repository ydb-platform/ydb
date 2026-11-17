# Generated file. DO NOT EDIT
"""Generated RPC calls for Temporal services."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import timedelta
from typing import TYPE_CHECKING

import google.protobuf.empty_pb2

import temporalio.api.cloud.cloudservice.v1
import temporalio.api.operatorservice.v1
import temporalio.api.testservice.v1
import temporalio.api.workflowservice.v1
import temporalio.bridge.proto.health.v1

if TYPE_CHECKING:
    from temporalio.service import ServiceClient


class WorkflowService:
    """RPC calls for the WorkflowService."""

    def __init__(self, client: ServiceClient):
        """Initialize service with the provided ServiceClient."""
        self._client = client
        self._service = "workflow"

    async def count_activity_executions(
        self,
        req: temporalio.api.workflowservice.v1.CountActivityExecutionsRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.CountActivityExecutionsResponse:
        """Invokes the WorkflowService.count_activity_executions rpc method."""
        return await self._client._rpc_call(
            rpc="count_activity_executions",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.CountActivityExecutionsResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def count_nexus_operation_executions(
        self,
        req: temporalio.api.workflowservice.v1.CountNexusOperationExecutionsRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.CountNexusOperationExecutionsResponse:
        """Invokes the WorkflowService.count_nexus_operation_executions rpc method."""
        return await self._client._rpc_call(
            rpc="count_nexus_operation_executions",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.CountNexusOperationExecutionsResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def count_schedules(
        self,
        req: temporalio.api.workflowservice.v1.CountSchedulesRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.CountSchedulesResponse:
        """Invokes the WorkflowService.count_schedules rpc method."""
        return await self._client._rpc_call(
            rpc="count_schedules",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.CountSchedulesResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def count_workers(
        self,
        req: temporalio.api.workflowservice.v1.CountWorkersRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.CountWorkersResponse:
        """Invokes the WorkflowService.count_workers rpc method."""
        return await self._client._rpc_call(
            rpc="count_workers",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.CountWorkersResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def count_workflow_executions(
        self,
        req: temporalio.api.workflowservice.v1.CountWorkflowExecutionsRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.CountWorkflowExecutionsResponse:
        """Invokes the WorkflowService.count_workflow_executions rpc method."""
        return await self._client._rpc_call(
            rpc="count_workflow_executions",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.CountWorkflowExecutionsResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def create_schedule(
        self,
        req: temporalio.api.workflowservice.v1.CreateScheduleRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.CreateScheduleResponse:
        """Invokes the WorkflowService.create_schedule rpc method."""
        return await self._client._rpc_call(
            rpc="create_schedule",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.CreateScheduleResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def create_worker_deployment(
        self,
        req: temporalio.api.workflowservice.v1.CreateWorkerDeploymentRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.CreateWorkerDeploymentResponse:
        """Invokes the WorkflowService.create_worker_deployment rpc method."""
        return await self._client._rpc_call(
            rpc="create_worker_deployment",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.CreateWorkerDeploymentResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def create_worker_deployment_version(
        self,
        req: temporalio.api.workflowservice.v1.CreateWorkerDeploymentVersionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.CreateWorkerDeploymentVersionResponse:
        """Invokes the WorkflowService.create_worker_deployment_version rpc method."""
        return await self._client._rpc_call(
            rpc="create_worker_deployment_version",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.CreateWorkerDeploymentVersionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def create_workflow_rule(
        self,
        req: temporalio.api.workflowservice.v1.CreateWorkflowRuleRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.CreateWorkflowRuleResponse:
        """Invokes the WorkflowService.create_workflow_rule rpc method."""
        return await self._client._rpc_call(
            rpc="create_workflow_rule",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.CreateWorkflowRuleResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def delete_activity_execution(
        self,
        req: temporalio.api.workflowservice.v1.DeleteActivityExecutionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.DeleteActivityExecutionResponse:
        """Invokes the WorkflowService.delete_activity_execution rpc method."""
        return await self._client._rpc_call(
            rpc="delete_activity_execution",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.DeleteActivityExecutionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def delete_nexus_operation_execution(
        self,
        req: temporalio.api.workflowservice.v1.DeleteNexusOperationExecutionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.DeleteNexusOperationExecutionResponse:
        """Invokes the WorkflowService.delete_nexus_operation_execution rpc method."""
        return await self._client._rpc_call(
            rpc="delete_nexus_operation_execution",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.DeleteNexusOperationExecutionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def delete_schedule(
        self,
        req: temporalio.api.workflowservice.v1.DeleteScheduleRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.DeleteScheduleResponse:
        """Invokes the WorkflowService.delete_schedule rpc method."""
        return await self._client._rpc_call(
            rpc="delete_schedule",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.DeleteScheduleResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def delete_worker_deployment(
        self,
        req: temporalio.api.workflowservice.v1.DeleteWorkerDeploymentRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.DeleteWorkerDeploymentResponse:
        """Invokes the WorkflowService.delete_worker_deployment rpc method."""
        return await self._client._rpc_call(
            rpc="delete_worker_deployment",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.DeleteWorkerDeploymentResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def delete_worker_deployment_version(
        self,
        req: temporalio.api.workflowservice.v1.DeleteWorkerDeploymentVersionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.DeleteWorkerDeploymentVersionResponse:
        """Invokes the WorkflowService.delete_worker_deployment_version rpc method."""
        return await self._client._rpc_call(
            rpc="delete_worker_deployment_version",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.DeleteWorkerDeploymentVersionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def delete_workflow_execution(
        self,
        req: temporalio.api.workflowservice.v1.DeleteWorkflowExecutionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.DeleteWorkflowExecutionResponse:
        """Invokes the WorkflowService.delete_workflow_execution rpc method."""
        return await self._client._rpc_call(
            rpc="delete_workflow_execution",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.DeleteWorkflowExecutionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def delete_workflow_rule(
        self,
        req: temporalio.api.workflowservice.v1.DeleteWorkflowRuleRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.DeleteWorkflowRuleResponse:
        """Invokes the WorkflowService.delete_workflow_rule rpc method."""
        return await self._client._rpc_call(
            rpc="delete_workflow_rule",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.DeleteWorkflowRuleResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def deprecate_namespace(
        self,
        req: temporalio.api.workflowservice.v1.DeprecateNamespaceRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.DeprecateNamespaceResponse:
        """Invokes the WorkflowService.deprecate_namespace rpc method."""
        return await self._client._rpc_call(
            rpc="deprecate_namespace",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.DeprecateNamespaceResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def describe_activity_execution(
        self,
        req: temporalio.api.workflowservice.v1.DescribeActivityExecutionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.DescribeActivityExecutionResponse:
        """Invokes the WorkflowService.describe_activity_execution rpc method."""
        return await self._client._rpc_call(
            rpc="describe_activity_execution",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.DescribeActivityExecutionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def describe_batch_operation(
        self,
        req: temporalio.api.workflowservice.v1.DescribeBatchOperationRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.DescribeBatchOperationResponse:
        """Invokes the WorkflowService.describe_batch_operation rpc method."""
        return await self._client._rpc_call(
            rpc="describe_batch_operation",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.DescribeBatchOperationResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def describe_deployment(
        self,
        req: temporalio.api.workflowservice.v1.DescribeDeploymentRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.DescribeDeploymentResponse:
        """Invokes the WorkflowService.describe_deployment rpc method."""
        return await self._client._rpc_call(
            rpc="describe_deployment",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.DescribeDeploymentResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def describe_namespace(
        self,
        req: temporalio.api.workflowservice.v1.DescribeNamespaceRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.DescribeNamespaceResponse:
        """Invokes the WorkflowService.describe_namespace rpc method."""
        return await self._client._rpc_call(
            rpc="describe_namespace",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.DescribeNamespaceResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def describe_nexus_operation_execution(
        self,
        req: temporalio.api.workflowservice.v1.DescribeNexusOperationExecutionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.DescribeNexusOperationExecutionResponse:
        """Invokes the WorkflowService.describe_nexus_operation_execution rpc method."""
        return await self._client._rpc_call(
            rpc="describe_nexus_operation_execution",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.DescribeNexusOperationExecutionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def describe_schedule(
        self,
        req: temporalio.api.workflowservice.v1.DescribeScheduleRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.DescribeScheduleResponse:
        """Invokes the WorkflowService.describe_schedule rpc method."""
        return await self._client._rpc_call(
            rpc="describe_schedule",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.DescribeScheduleResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def describe_task_queue(
        self,
        req: temporalio.api.workflowservice.v1.DescribeTaskQueueRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.DescribeTaskQueueResponse:
        """Invokes the WorkflowService.describe_task_queue rpc method."""
        return await self._client._rpc_call(
            rpc="describe_task_queue",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.DescribeTaskQueueResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def describe_worker(
        self,
        req: temporalio.api.workflowservice.v1.DescribeWorkerRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.DescribeWorkerResponse:
        """Invokes the WorkflowService.describe_worker rpc method."""
        return await self._client._rpc_call(
            rpc="describe_worker",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.DescribeWorkerResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def describe_worker_deployment(
        self,
        req: temporalio.api.workflowservice.v1.DescribeWorkerDeploymentRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.DescribeWorkerDeploymentResponse:
        """Invokes the WorkflowService.describe_worker_deployment rpc method."""
        return await self._client._rpc_call(
            rpc="describe_worker_deployment",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.DescribeWorkerDeploymentResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def describe_worker_deployment_version(
        self,
        req: temporalio.api.workflowservice.v1.DescribeWorkerDeploymentVersionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.DescribeWorkerDeploymentVersionResponse:
        """Invokes the WorkflowService.describe_worker_deployment_version rpc method."""
        return await self._client._rpc_call(
            rpc="describe_worker_deployment_version",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.DescribeWorkerDeploymentVersionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def describe_workflow_execution(
        self,
        req: temporalio.api.workflowservice.v1.DescribeWorkflowExecutionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.DescribeWorkflowExecutionResponse:
        """Invokes the WorkflowService.describe_workflow_execution rpc method."""
        return await self._client._rpc_call(
            rpc="describe_workflow_execution",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.DescribeWorkflowExecutionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def describe_workflow_rule(
        self,
        req: temporalio.api.workflowservice.v1.DescribeWorkflowRuleRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.DescribeWorkflowRuleResponse:
        """Invokes the WorkflowService.describe_workflow_rule rpc method."""
        return await self._client._rpc_call(
            rpc="describe_workflow_rule",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.DescribeWorkflowRuleResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def execute_multi_operation(
        self,
        req: temporalio.api.workflowservice.v1.ExecuteMultiOperationRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.ExecuteMultiOperationResponse:
        """Invokes the WorkflowService.execute_multi_operation rpc method."""
        return await self._client._rpc_call(
            rpc="execute_multi_operation",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.ExecuteMultiOperationResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def fetch_worker_config(
        self,
        req: temporalio.api.workflowservice.v1.FetchWorkerConfigRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.FetchWorkerConfigResponse:
        """Invokes the WorkflowService.fetch_worker_config rpc method."""
        return await self._client._rpc_call(
            rpc="fetch_worker_config",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.FetchWorkerConfigResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_cluster_info(
        self,
        req: temporalio.api.workflowservice.v1.GetClusterInfoRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.GetClusterInfoResponse:
        """Invokes the WorkflowService.get_cluster_info rpc method."""
        return await self._client._rpc_call(
            rpc="get_cluster_info",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.GetClusterInfoResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_current_deployment(
        self,
        req: temporalio.api.workflowservice.v1.GetCurrentDeploymentRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.GetCurrentDeploymentResponse:
        """Invokes the WorkflowService.get_current_deployment rpc method."""
        return await self._client._rpc_call(
            rpc="get_current_deployment",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.GetCurrentDeploymentResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_deployment_reachability(
        self,
        req: temporalio.api.workflowservice.v1.GetDeploymentReachabilityRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.GetDeploymentReachabilityResponse:
        """Invokes the WorkflowService.get_deployment_reachability rpc method."""
        return await self._client._rpc_call(
            rpc="get_deployment_reachability",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.GetDeploymentReachabilityResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_search_attributes(
        self,
        req: temporalio.api.workflowservice.v1.GetSearchAttributesRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.GetSearchAttributesResponse:
        """Invokes the WorkflowService.get_search_attributes rpc method."""
        return await self._client._rpc_call(
            rpc="get_search_attributes",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.GetSearchAttributesResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_system_info(
        self,
        req: temporalio.api.workflowservice.v1.GetSystemInfoRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.GetSystemInfoResponse:
        """Invokes the WorkflowService.get_system_info rpc method."""
        return await self._client._rpc_call(
            rpc="get_system_info",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.GetSystemInfoResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_worker_build_id_compatibility(
        self,
        req: temporalio.api.workflowservice.v1.GetWorkerBuildIdCompatibilityRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.GetWorkerBuildIdCompatibilityResponse:
        """Invokes the WorkflowService.get_worker_build_id_compatibility rpc method."""
        return await self._client._rpc_call(
            rpc="get_worker_build_id_compatibility",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.GetWorkerBuildIdCompatibilityResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_worker_task_reachability(
        self,
        req: temporalio.api.workflowservice.v1.GetWorkerTaskReachabilityRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.GetWorkerTaskReachabilityResponse:
        """Invokes the WorkflowService.get_worker_task_reachability rpc method."""
        return await self._client._rpc_call(
            rpc="get_worker_task_reachability",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.GetWorkerTaskReachabilityResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_worker_versioning_rules(
        self,
        req: temporalio.api.workflowservice.v1.GetWorkerVersioningRulesRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.GetWorkerVersioningRulesResponse:
        """Invokes the WorkflowService.get_worker_versioning_rules rpc method."""
        return await self._client._rpc_call(
            rpc="get_worker_versioning_rules",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.GetWorkerVersioningRulesResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_workflow_execution_history(
        self,
        req: temporalio.api.workflowservice.v1.GetWorkflowExecutionHistoryRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.GetWorkflowExecutionHistoryResponse:
        """Invokes the WorkflowService.get_workflow_execution_history rpc method."""
        return await self._client._rpc_call(
            rpc="get_workflow_execution_history",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.GetWorkflowExecutionHistoryResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_workflow_execution_history_reverse(
        self,
        req: temporalio.api.workflowservice.v1.GetWorkflowExecutionHistoryReverseRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.GetWorkflowExecutionHistoryReverseResponse:
        """Invokes the WorkflowService.get_workflow_execution_history_reverse rpc method."""
        return await self._client._rpc_call(
            rpc="get_workflow_execution_history_reverse",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.GetWorkflowExecutionHistoryReverseResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def list_activity_executions(
        self,
        req: temporalio.api.workflowservice.v1.ListActivityExecutionsRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.ListActivityExecutionsResponse:
        """Invokes the WorkflowService.list_activity_executions rpc method."""
        return await self._client._rpc_call(
            rpc="list_activity_executions",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.ListActivityExecutionsResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def list_archived_workflow_executions(
        self,
        req: temporalio.api.workflowservice.v1.ListArchivedWorkflowExecutionsRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.ListArchivedWorkflowExecutionsResponse:
        """Invokes the WorkflowService.list_archived_workflow_executions rpc method."""
        return await self._client._rpc_call(
            rpc="list_archived_workflow_executions",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.ListArchivedWorkflowExecutionsResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def list_batch_operations(
        self,
        req: temporalio.api.workflowservice.v1.ListBatchOperationsRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.ListBatchOperationsResponse:
        """Invokes the WorkflowService.list_batch_operations rpc method."""
        return await self._client._rpc_call(
            rpc="list_batch_operations",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.ListBatchOperationsResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def list_closed_workflow_executions(
        self,
        req: temporalio.api.workflowservice.v1.ListClosedWorkflowExecutionsRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.ListClosedWorkflowExecutionsResponse:
        """Invokes the WorkflowService.list_closed_workflow_executions rpc method."""
        return await self._client._rpc_call(
            rpc="list_closed_workflow_executions",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.ListClosedWorkflowExecutionsResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def list_deployments(
        self,
        req: temporalio.api.workflowservice.v1.ListDeploymentsRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.ListDeploymentsResponse:
        """Invokes the WorkflowService.list_deployments rpc method."""
        return await self._client._rpc_call(
            rpc="list_deployments",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.ListDeploymentsResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def list_namespaces(
        self,
        req: temporalio.api.workflowservice.v1.ListNamespacesRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.ListNamespacesResponse:
        """Invokes the WorkflowService.list_namespaces rpc method."""
        return await self._client._rpc_call(
            rpc="list_namespaces",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.ListNamespacesResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def list_nexus_operation_executions(
        self,
        req: temporalio.api.workflowservice.v1.ListNexusOperationExecutionsRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.ListNexusOperationExecutionsResponse:
        """Invokes the WorkflowService.list_nexus_operation_executions rpc method."""
        return await self._client._rpc_call(
            rpc="list_nexus_operation_executions",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.ListNexusOperationExecutionsResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def list_open_workflow_executions(
        self,
        req: temporalio.api.workflowservice.v1.ListOpenWorkflowExecutionsRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.ListOpenWorkflowExecutionsResponse:
        """Invokes the WorkflowService.list_open_workflow_executions rpc method."""
        return await self._client._rpc_call(
            rpc="list_open_workflow_executions",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.ListOpenWorkflowExecutionsResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def list_schedule_matching_times(
        self,
        req: temporalio.api.workflowservice.v1.ListScheduleMatchingTimesRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.ListScheduleMatchingTimesResponse:
        """Invokes the WorkflowService.list_schedule_matching_times rpc method."""
        return await self._client._rpc_call(
            rpc="list_schedule_matching_times",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.ListScheduleMatchingTimesResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def list_schedules(
        self,
        req: temporalio.api.workflowservice.v1.ListSchedulesRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.ListSchedulesResponse:
        """Invokes the WorkflowService.list_schedules rpc method."""
        return await self._client._rpc_call(
            rpc="list_schedules",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.ListSchedulesResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def list_task_queue_partitions(
        self,
        req: temporalio.api.workflowservice.v1.ListTaskQueuePartitionsRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.ListTaskQueuePartitionsResponse:
        """Invokes the WorkflowService.list_task_queue_partitions rpc method."""
        return await self._client._rpc_call(
            rpc="list_task_queue_partitions",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.ListTaskQueuePartitionsResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def list_worker_deployments(
        self,
        req: temporalio.api.workflowservice.v1.ListWorkerDeploymentsRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.ListWorkerDeploymentsResponse:
        """Invokes the WorkflowService.list_worker_deployments rpc method."""
        return await self._client._rpc_call(
            rpc="list_worker_deployments",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.ListWorkerDeploymentsResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def list_workers(
        self,
        req: temporalio.api.workflowservice.v1.ListWorkersRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.ListWorkersResponse:
        """Invokes the WorkflowService.list_workers rpc method."""
        return await self._client._rpc_call(
            rpc="list_workers",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.ListWorkersResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def list_workflow_executions(
        self,
        req: temporalio.api.workflowservice.v1.ListWorkflowExecutionsRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.ListWorkflowExecutionsResponse:
        """Invokes the WorkflowService.list_workflow_executions rpc method."""
        return await self._client._rpc_call(
            rpc="list_workflow_executions",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.ListWorkflowExecutionsResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def list_workflow_rules(
        self,
        req: temporalio.api.workflowservice.v1.ListWorkflowRulesRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.ListWorkflowRulesResponse:
        """Invokes the WorkflowService.list_workflow_rules rpc method."""
        return await self._client._rpc_call(
            rpc="list_workflow_rules",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.ListWorkflowRulesResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def patch_schedule(
        self,
        req: temporalio.api.workflowservice.v1.PatchScheduleRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.PatchScheduleResponse:
        """Invokes the WorkflowService.patch_schedule rpc method."""
        return await self._client._rpc_call(
            rpc="patch_schedule",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.PatchScheduleResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def pause_activity(
        self,
        req: temporalio.api.workflowservice.v1.PauseActivityRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.PauseActivityResponse:
        """Invokes the WorkflowService.pause_activity rpc method."""
        return await self._client._rpc_call(
            rpc="pause_activity",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.PauseActivityResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def pause_activity_execution(
        self,
        req: temporalio.api.workflowservice.v1.PauseActivityExecutionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.PauseActivityExecutionResponse:
        """Invokes the WorkflowService.pause_activity_execution rpc method."""
        return await self._client._rpc_call(
            rpc="pause_activity_execution",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.PauseActivityExecutionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def pause_workflow_execution(
        self,
        req: temporalio.api.workflowservice.v1.PauseWorkflowExecutionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.PauseWorkflowExecutionResponse:
        """Invokes the WorkflowService.pause_workflow_execution rpc method."""
        return await self._client._rpc_call(
            rpc="pause_workflow_execution",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.PauseWorkflowExecutionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def poll_activity_execution(
        self,
        req: temporalio.api.workflowservice.v1.PollActivityExecutionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.PollActivityExecutionResponse:
        """Invokes the WorkflowService.poll_activity_execution rpc method."""
        return await self._client._rpc_call(
            rpc="poll_activity_execution",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.PollActivityExecutionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def poll_activity_task_queue(
        self,
        req: temporalio.api.workflowservice.v1.PollActivityTaskQueueRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.PollActivityTaskQueueResponse:
        """Invokes the WorkflowService.poll_activity_task_queue rpc method."""
        return await self._client._rpc_call(
            rpc="poll_activity_task_queue",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.PollActivityTaskQueueResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def poll_nexus_operation_execution(
        self,
        req: temporalio.api.workflowservice.v1.PollNexusOperationExecutionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.PollNexusOperationExecutionResponse:
        """Invokes the WorkflowService.poll_nexus_operation_execution rpc method."""
        return await self._client._rpc_call(
            rpc="poll_nexus_operation_execution",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.PollNexusOperationExecutionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def poll_nexus_task_queue(
        self,
        req: temporalio.api.workflowservice.v1.PollNexusTaskQueueRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.PollNexusTaskQueueResponse:
        """Invokes the WorkflowService.poll_nexus_task_queue rpc method."""
        return await self._client._rpc_call(
            rpc="poll_nexus_task_queue",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.PollNexusTaskQueueResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def poll_workflow_execution_time_skipping(
        self,
        req: temporalio.api.workflowservice.v1.PollWorkflowExecutionTimeSkippingRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.PollWorkflowExecutionTimeSkippingResponse:
        """Invokes the WorkflowService.poll_workflow_execution_time_skipping rpc method."""
        return await self._client._rpc_call(
            rpc="poll_workflow_execution_time_skipping",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.PollWorkflowExecutionTimeSkippingResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def poll_workflow_execution_update(
        self,
        req: temporalio.api.workflowservice.v1.PollWorkflowExecutionUpdateRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.PollWorkflowExecutionUpdateResponse:
        """Invokes the WorkflowService.poll_workflow_execution_update rpc method."""
        return await self._client._rpc_call(
            rpc="poll_workflow_execution_update",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.PollWorkflowExecutionUpdateResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def poll_workflow_task_queue(
        self,
        req: temporalio.api.workflowservice.v1.PollWorkflowTaskQueueRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.PollWorkflowTaskQueueResponse:
        """Invokes the WorkflowService.poll_workflow_task_queue rpc method."""
        return await self._client._rpc_call(
            rpc="poll_workflow_task_queue",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.PollWorkflowTaskQueueResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def query_workflow(
        self,
        req: temporalio.api.workflowservice.v1.QueryWorkflowRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.QueryWorkflowResponse:
        """Invokes the WorkflowService.query_workflow rpc method."""
        return await self._client._rpc_call(
            rpc="query_workflow",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.QueryWorkflowResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def record_activity_task_heartbeat(
        self,
        req: temporalio.api.workflowservice.v1.RecordActivityTaskHeartbeatRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.RecordActivityTaskHeartbeatResponse:
        """Invokes the WorkflowService.record_activity_task_heartbeat rpc method."""
        return await self._client._rpc_call(
            rpc="record_activity_task_heartbeat",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.RecordActivityTaskHeartbeatResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def record_activity_task_heartbeat_by_id(
        self,
        req: temporalio.api.workflowservice.v1.RecordActivityTaskHeartbeatByIdRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.RecordActivityTaskHeartbeatByIdResponse:
        """Invokes the WorkflowService.record_activity_task_heartbeat_by_id rpc method."""
        return await self._client._rpc_call(
            rpc="record_activity_task_heartbeat_by_id",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.RecordActivityTaskHeartbeatByIdResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def record_worker_heartbeat(
        self,
        req: temporalio.api.workflowservice.v1.RecordWorkerHeartbeatRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.RecordWorkerHeartbeatResponse:
        """Invokes the WorkflowService.record_worker_heartbeat rpc method."""
        return await self._client._rpc_call(
            rpc="record_worker_heartbeat",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.RecordWorkerHeartbeatResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def register_namespace(
        self,
        req: temporalio.api.workflowservice.v1.RegisterNamespaceRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.RegisterNamespaceResponse:
        """Invokes the WorkflowService.register_namespace rpc method."""
        return await self._client._rpc_call(
            rpc="register_namespace",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.RegisterNamespaceResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def request_cancel_activity_execution(
        self,
        req: temporalio.api.workflowservice.v1.RequestCancelActivityExecutionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.RequestCancelActivityExecutionResponse:
        """Invokes the WorkflowService.request_cancel_activity_execution rpc method."""
        return await self._client._rpc_call(
            rpc="request_cancel_activity_execution",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.RequestCancelActivityExecutionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def request_cancel_nexus_operation_execution(
        self,
        req: temporalio.api.workflowservice.v1.RequestCancelNexusOperationExecutionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.RequestCancelNexusOperationExecutionResponse:
        """Invokes the WorkflowService.request_cancel_nexus_operation_execution rpc method."""
        return await self._client._rpc_call(
            rpc="request_cancel_nexus_operation_execution",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.RequestCancelNexusOperationExecutionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def request_cancel_workflow_execution(
        self,
        req: temporalio.api.workflowservice.v1.RequestCancelWorkflowExecutionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.RequestCancelWorkflowExecutionResponse:
        """Invokes the WorkflowService.request_cancel_workflow_execution rpc method."""
        return await self._client._rpc_call(
            rpc="request_cancel_workflow_execution",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.RequestCancelWorkflowExecutionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def reset_activity(
        self,
        req: temporalio.api.workflowservice.v1.ResetActivityRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.ResetActivityResponse:
        """Invokes the WorkflowService.reset_activity rpc method."""
        return await self._client._rpc_call(
            rpc="reset_activity",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.ResetActivityResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def reset_activity_execution(
        self,
        req: temporalio.api.workflowservice.v1.ResetActivityExecutionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.ResetActivityExecutionResponse:
        """Invokes the WorkflowService.reset_activity_execution rpc method."""
        return await self._client._rpc_call(
            rpc="reset_activity_execution",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.ResetActivityExecutionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def reset_sticky_task_queue(
        self,
        req: temporalio.api.workflowservice.v1.ResetStickyTaskQueueRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.ResetStickyTaskQueueResponse:
        """Invokes the WorkflowService.reset_sticky_task_queue rpc method."""
        return await self._client._rpc_call(
            rpc="reset_sticky_task_queue",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.ResetStickyTaskQueueResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def reset_workflow_execution(
        self,
        req: temporalio.api.workflowservice.v1.ResetWorkflowExecutionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.ResetWorkflowExecutionResponse:
        """Invokes the WorkflowService.reset_workflow_execution rpc method."""
        return await self._client._rpc_call(
            rpc="reset_workflow_execution",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.ResetWorkflowExecutionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def respond_activity_task_canceled(
        self,
        req: temporalio.api.workflowservice.v1.RespondActivityTaskCanceledRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.RespondActivityTaskCanceledResponse:
        """Invokes the WorkflowService.respond_activity_task_canceled rpc method."""
        return await self._client._rpc_call(
            rpc="respond_activity_task_canceled",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.RespondActivityTaskCanceledResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def respond_activity_task_canceled_by_id(
        self,
        req: temporalio.api.workflowservice.v1.RespondActivityTaskCanceledByIdRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.RespondActivityTaskCanceledByIdResponse:
        """Invokes the WorkflowService.respond_activity_task_canceled_by_id rpc method."""
        return await self._client._rpc_call(
            rpc="respond_activity_task_canceled_by_id",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.RespondActivityTaskCanceledByIdResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def respond_activity_task_completed(
        self,
        req: temporalio.api.workflowservice.v1.RespondActivityTaskCompletedRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.RespondActivityTaskCompletedResponse:
        """Invokes the WorkflowService.respond_activity_task_completed rpc method."""
        return await self._client._rpc_call(
            rpc="respond_activity_task_completed",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.RespondActivityTaskCompletedResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def respond_activity_task_completed_by_id(
        self,
        req: temporalio.api.workflowservice.v1.RespondActivityTaskCompletedByIdRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.RespondActivityTaskCompletedByIdResponse:
        """Invokes the WorkflowService.respond_activity_task_completed_by_id rpc method."""
        return await self._client._rpc_call(
            rpc="respond_activity_task_completed_by_id",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.RespondActivityTaskCompletedByIdResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def respond_activity_task_failed(
        self,
        req: temporalio.api.workflowservice.v1.RespondActivityTaskFailedRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.RespondActivityTaskFailedResponse:
        """Invokes the WorkflowService.respond_activity_task_failed rpc method."""
        return await self._client._rpc_call(
            rpc="respond_activity_task_failed",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.RespondActivityTaskFailedResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def respond_activity_task_failed_by_id(
        self,
        req: temporalio.api.workflowservice.v1.RespondActivityTaskFailedByIdRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.RespondActivityTaskFailedByIdResponse:
        """Invokes the WorkflowService.respond_activity_task_failed_by_id rpc method."""
        return await self._client._rpc_call(
            rpc="respond_activity_task_failed_by_id",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.RespondActivityTaskFailedByIdResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def respond_nexus_task_completed(
        self,
        req: temporalio.api.workflowservice.v1.RespondNexusTaskCompletedRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.RespondNexusTaskCompletedResponse:
        """Invokes the WorkflowService.respond_nexus_task_completed rpc method."""
        return await self._client._rpc_call(
            rpc="respond_nexus_task_completed",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.RespondNexusTaskCompletedResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def respond_nexus_task_failed(
        self,
        req: temporalio.api.workflowservice.v1.RespondNexusTaskFailedRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.RespondNexusTaskFailedResponse:
        """Invokes the WorkflowService.respond_nexus_task_failed rpc method."""
        return await self._client._rpc_call(
            rpc="respond_nexus_task_failed",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.RespondNexusTaskFailedResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def respond_query_task_completed(
        self,
        req: temporalio.api.workflowservice.v1.RespondQueryTaskCompletedRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.RespondQueryTaskCompletedResponse:
        """Invokes the WorkflowService.respond_query_task_completed rpc method."""
        return await self._client._rpc_call(
            rpc="respond_query_task_completed",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.RespondQueryTaskCompletedResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def respond_workflow_task_completed(
        self,
        req: temporalio.api.workflowservice.v1.RespondWorkflowTaskCompletedRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.RespondWorkflowTaskCompletedResponse:
        """Invokes the WorkflowService.respond_workflow_task_completed rpc method."""
        return await self._client._rpc_call(
            rpc="respond_workflow_task_completed",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.RespondWorkflowTaskCompletedResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def respond_workflow_task_failed(
        self,
        req: temporalio.api.workflowservice.v1.RespondWorkflowTaskFailedRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.RespondWorkflowTaskFailedResponse:
        """Invokes the WorkflowService.respond_workflow_task_failed rpc method."""
        return await self._client._rpc_call(
            rpc="respond_workflow_task_failed",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.RespondWorkflowTaskFailedResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def scan_workflow_executions(
        self,
        req: temporalio.api.workflowservice.v1.ScanWorkflowExecutionsRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.ScanWorkflowExecutionsResponse:
        """Invokes the WorkflowService.scan_workflow_executions rpc method."""
        return await self._client._rpc_call(
            rpc="scan_workflow_executions",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.ScanWorkflowExecutionsResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def set_current_deployment(
        self,
        req: temporalio.api.workflowservice.v1.SetCurrentDeploymentRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.SetCurrentDeploymentResponse:
        """Invokes the WorkflowService.set_current_deployment rpc method."""
        return await self._client._rpc_call(
            rpc="set_current_deployment",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.SetCurrentDeploymentResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def set_worker_deployment_current_version(
        self,
        req: temporalio.api.workflowservice.v1.SetWorkerDeploymentCurrentVersionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.SetWorkerDeploymentCurrentVersionResponse:
        """Invokes the WorkflowService.set_worker_deployment_current_version rpc method."""
        return await self._client._rpc_call(
            rpc="set_worker_deployment_current_version",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.SetWorkerDeploymentCurrentVersionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def set_worker_deployment_manager(
        self,
        req: temporalio.api.workflowservice.v1.SetWorkerDeploymentManagerRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.SetWorkerDeploymentManagerResponse:
        """Invokes the WorkflowService.set_worker_deployment_manager rpc method."""
        return await self._client._rpc_call(
            rpc="set_worker_deployment_manager",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.SetWorkerDeploymentManagerResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def set_worker_deployment_ramping_version(
        self,
        req: temporalio.api.workflowservice.v1.SetWorkerDeploymentRampingVersionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.SetWorkerDeploymentRampingVersionResponse:
        """Invokes the WorkflowService.set_worker_deployment_ramping_version rpc method."""
        return await self._client._rpc_call(
            rpc="set_worker_deployment_ramping_version",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.SetWorkerDeploymentRampingVersionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def shutdown_worker(
        self,
        req: temporalio.api.workflowservice.v1.ShutdownWorkerRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.ShutdownWorkerResponse:
        """Invokes the WorkflowService.shutdown_worker rpc method."""
        return await self._client._rpc_call(
            rpc="shutdown_worker",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.ShutdownWorkerResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def signal_with_start_workflow_execution(
        self,
        req: temporalio.api.workflowservice.v1.SignalWithStartWorkflowExecutionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.SignalWithStartWorkflowExecutionResponse:
        """Invokes the WorkflowService.signal_with_start_workflow_execution rpc method."""
        return await self._client._rpc_call(
            rpc="signal_with_start_workflow_execution",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.SignalWithStartWorkflowExecutionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def signal_workflow_execution(
        self,
        req: temporalio.api.workflowservice.v1.SignalWorkflowExecutionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.SignalWorkflowExecutionResponse:
        """Invokes the WorkflowService.signal_workflow_execution rpc method."""
        return await self._client._rpc_call(
            rpc="signal_workflow_execution",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.SignalWorkflowExecutionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def start_activity_execution(
        self,
        req: temporalio.api.workflowservice.v1.StartActivityExecutionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.StartActivityExecutionResponse:
        """Invokes the WorkflowService.start_activity_execution rpc method."""
        return await self._client._rpc_call(
            rpc="start_activity_execution",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.StartActivityExecutionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def start_batch_operation(
        self,
        req: temporalio.api.workflowservice.v1.StartBatchOperationRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.StartBatchOperationResponse:
        """Invokes the WorkflowService.start_batch_operation rpc method."""
        return await self._client._rpc_call(
            rpc="start_batch_operation",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.StartBatchOperationResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def start_nexus_operation_execution(
        self,
        req: temporalio.api.workflowservice.v1.StartNexusOperationExecutionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.StartNexusOperationExecutionResponse:
        """Invokes the WorkflowService.start_nexus_operation_execution rpc method."""
        return await self._client._rpc_call(
            rpc="start_nexus_operation_execution",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.StartNexusOperationExecutionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def start_workflow_execution(
        self,
        req: temporalio.api.workflowservice.v1.StartWorkflowExecutionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.StartWorkflowExecutionResponse:
        """Invokes the WorkflowService.start_workflow_execution rpc method."""
        return await self._client._rpc_call(
            rpc="start_workflow_execution",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.StartWorkflowExecutionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def stop_batch_operation(
        self,
        req: temporalio.api.workflowservice.v1.StopBatchOperationRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.StopBatchOperationResponse:
        """Invokes the WorkflowService.stop_batch_operation rpc method."""
        return await self._client._rpc_call(
            rpc="stop_batch_operation",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.StopBatchOperationResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def terminate_activity_execution(
        self,
        req: temporalio.api.workflowservice.v1.TerminateActivityExecutionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.TerminateActivityExecutionResponse:
        """Invokes the WorkflowService.terminate_activity_execution rpc method."""
        return await self._client._rpc_call(
            rpc="terminate_activity_execution",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.TerminateActivityExecutionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def terminate_nexus_operation_execution(
        self,
        req: temporalio.api.workflowservice.v1.TerminateNexusOperationExecutionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.TerminateNexusOperationExecutionResponse:
        """Invokes the WorkflowService.terminate_nexus_operation_execution rpc method."""
        return await self._client._rpc_call(
            rpc="terminate_nexus_operation_execution",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.TerminateNexusOperationExecutionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def terminate_workflow_execution(
        self,
        req: temporalio.api.workflowservice.v1.TerminateWorkflowExecutionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.TerminateWorkflowExecutionResponse:
        """Invokes the WorkflowService.terminate_workflow_execution rpc method."""
        return await self._client._rpc_call(
            rpc="terminate_workflow_execution",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.TerminateWorkflowExecutionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def trigger_workflow_rule(
        self,
        req: temporalio.api.workflowservice.v1.TriggerWorkflowRuleRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.TriggerWorkflowRuleResponse:
        """Invokes the WorkflowService.trigger_workflow_rule rpc method."""
        return await self._client._rpc_call(
            rpc="trigger_workflow_rule",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.TriggerWorkflowRuleResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def unpause_activity(
        self,
        req: temporalio.api.workflowservice.v1.UnpauseActivityRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.UnpauseActivityResponse:
        """Invokes the WorkflowService.unpause_activity rpc method."""
        return await self._client._rpc_call(
            rpc="unpause_activity",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.UnpauseActivityResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def unpause_activity_execution(
        self,
        req: temporalio.api.workflowservice.v1.UnpauseActivityExecutionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.UnpauseActivityExecutionResponse:
        """Invokes the WorkflowService.unpause_activity_execution rpc method."""
        return await self._client._rpc_call(
            rpc="unpause_activity_execution",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.UnpauseActivityExecutionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def unpause_workflow_execution(
        self,
        req: temporalio.api.workflowservice.v1.UnpauseWorkflowExecutionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.UnpauseWorkflowExecutionResponse:
        """Invokes the WorkflowService.unpause_workflow_execution rpc method."""
        return await self._client._rpc_call(
            rpc="unpause_workflow_execution",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.UnpauseWorkflowExecutionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def update_activity_execution_options(
        self,
        req: temporalio.api.workflowservice.v1.UpdateActivityExecutionOptionsRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.UpdateActivityExecutionOptionsResponse:
        """Invokes the WorkflowService.update_activity_execution_options rpc method."""
        return await self._client._rpc_call(
            rpc="update_activity_execution_options",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.UpdateActivityExecutionOptionsResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def update_activity_options(
        self,
        req: temporalio.api.workflowservice.v1.UpdateActivityOptionsRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.UpdateActivityOptionsResponse:
        """Invokes the WorkflowService.update_activity_options rpc method."""
        return await self._client._rpc_call(
            rpc="update_activity_options",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.UpdateActivityOptionsResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def update_namespace(
        self,
        req: temporalio.api.workflowservice.v1.UpdateNamespaceRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.UpdateNamespaceResponse:
        """Invokes the WorkflowService.update_namespace rpc method."""
        return await self._client._rpc_call(
            rpc="update_namespace",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.UpdateNamespaceResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def update_schedule(
        self,
        req: temporalio.api.workflowservice.v1.UpdateScheduleRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.UpdateScheduleResponse:
        """Invokes the WorkflowService.update_schedule rpc method."""
        return await self._client._rpc_call(
            rpc="update_schedule",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.UpdateScheduleResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def update_task_queue_config(
        self,
        req: temporalio.api.workflowservice.v1.UpdateTaskQueueConfigRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.UpdateTaskQueueConfigResponse:
        """Invokes the WorkflowService.update_task_queue_config rpc method."""
        return await self._client._rpc_call(
            rpc="update_task_queue_config",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.UpdateTaskQueueConfigResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def update_worker_build_id_compatibility(
        self,
        req: temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityResponse:
        """Invokes the WorkflowService.update_worker_build_id_compatibility rpc method."""
        return await self._client._rpc_call(
            rpc="update_worker_build_id_compatibility",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.UpdateWorkerBuildIdCompatibilityResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def update_worker_config(
        self,
        req: temporalio.api.workflowservice.v1.UpdateWorkerConfigRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.UpdateWorkerConfigResponse:
        """Invokes the WorkflowService.update_worker_config rpc method."""
        return await self._client._rpc_call(
            rpc="update_worker_config",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.UpdateWorkerConfigResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def update_worker_deployment_version_compute_config(
        self,
        req: temporalio.api.workflowservice.v1.UpdateWorkerDeploymentVersionComputeConfigRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.UpdateWorkerDeploymentVersionComputeConfigResponse:
        """Invokes the WorkflowService.update_worker_deployment_version_compute_config rpc method."""
        return await self._client._rpc_call(
            rpc="update_worker_deployment_version_compute_config",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.UpdateWorkerDeploymentVersionComputeConfigResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def update_worker_deployment_version_metadata(
        self,
        req: temporalio.api.workflowservice.v1.UpdateWorkerDeploymentVersionMetadataRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> (
        temporalio.api.workflowservice.v1.UpdateWorkerDeploymentVersionMetadataResponse
    ):
        """Invokes the WorkflowService.update_worker_deployment_version_metadata rpc method."""
        return await self._client._rpc_call(
            rpc="update_worker_deployment_version_metadata",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.UpdateWorkerDeploymentVersionMetadataResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def update_worker_versioning_rules(
        self,
        req: temporalio.api.workflowservice.v1.UpdateWorkerVersioningRulesRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.UpdateWorkerVersioningRulesResponse:
        """Invokes the WorkflowService.update_worker_versioning_rules rpc method."""
        return await self._client._rpc_call(
            rpc="update_worker_versioning_rules",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.UpdateWorkerVersioningRulesResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def update_workflow_execution(
        self,
        req: temporalio.api.workflowservice.v1.UpdateWorkflowExecutionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.UpdateWorkflowExecutionResponse:
        """Invokes the WorkflowService.update_workflow_execution rpc method."""
        return await self._client._rpc_call(
            rpc="update_workflow_execution",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.UpdateWorkflowExecutionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def update_workflow_execution_options(
        self,
        req: temporalio.api.workflowservice.v1.UpdateWorkflowExecutionOptionsRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.UpdateWorkflowExecutionOptionsResponse:
        """Invokes the WorkflowService.update_workflow_execution_options rpc method."""
        return await self._client._rpc_call(
            rpc="update_workflow_execution_options",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.UpdateWorkflowExecutionOptionsResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def validate_worker_deployment_version_compute_config(
        self,
        req: temporalio.api.workflowservice.v1.ValidateWorkerDeploymentVersionComputeConfigRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.workflowservice.v1.ValidateWorkerDeploymentVersionComputeConfigResponse:
        """Invokes the WorkflowService.validate_worker_deployment_version_compute_config rpc method."""
        return await self._client._rpc_call(
            rpc="validate_worker_deployment_version_compute_config",
            req=req,
            service=self._service,
            resp_type=temporalio.api.workflowservice.v1.ValidateWorkerDeploymentVersionComputeConfigResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )


class OperatorService:
    """RPC calls for the OperatorService."""

    def __init__(self, client: ServiceClient):
        """Initialize service with the provided ServiceClient."""
        self._client = client
        self._service = "operator"

    async def add_or_update_remote_cluster(
        self,
        req: temporalio.api.operatorservice.v1.AddOrUpdateRemoteClusterRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.operatorservice.v1.AddOrUpdateRemoteClusterResponse:
        """Invokes the OperatorService.add_or_update_remote_cluster rpc method."""
        return await self._client._rpc_call(
            rpc="add_or_update_remote_cluster",
            req=req,
            service=self._service,
            resp_type=temporalio.api.operatorservice.v1.AddOrUpdateRemoteClusterResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def add_search_attributes(
        self,
        req: temporalio.api.operatorservice.v1.AddSearchAttributesRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.operatorservice.v1.AddSearchAttributesResponse:
        """Invokes the OperatorService.add_search_attributes rpc method."""
        return await self._client._rpc_call(
            rpc="add_search_attributes",
            req=req,
            service=self._service,
            resp_type=temporalio.api.operatorservice.v1.AddSearchAttributesResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def create_nexus_endpoint(
        self,
        req: temporalio.api.operatorservice.v1.CreateNexusEndpointRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.operatorservice.v1.CreateNexusEndpointResponse:
        """Invokes the OperatorService.create_nexus_endpoint rpc method."""
        return await self._client._rpc_call(
            rpc="create_nexus_endpoint",
            req=req,
            service=self._service,
            resp_type=temporalio.api.operatorservice.v1.CreateNexusEndpointResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def delete_namespace(
        self,
        req: temporalio.api.operatorservice.v1.DeleteNamespaceRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.operatorservice.v1.DeleteNamespaceResponse:
        """Invokes the OperatorService.delete_namespace rpc method."""
        return await self._client._rpc_call(
            rpc="delete_namespace",
            req=req,
            service=self._service,
            resp_type=temporalio.api.operatorservice.v1.DeleteNamespaceResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def delete_nexus_endpoint(
        self,
        req: temporalio.api.operatorservice.v1.DeleteNexusEndpointRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.operatorservice.v1.DeleteNexusEndpointResponse:
        """Invokes the OperatorService.delete_nexus_endpoint rpc method."""
        return await self._client._rpc_call(
            rpc="delete_nexus_endpoint",
            req=req,
            service=self._service,
            resp_type=temporalio.api.operatorservice.v1.DeleteNexusEndpointResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_nexus_endpoint(
        self,
        req: temporalio.api.operatorservice.v1.GetNexusEndpointRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.operatorservice.v1.GetNexusEndpointResponse:
        """Invokes the OperatorService.get_nexus_endpoint rpc method."""
        return await self._client._rpc_call(
            rpc="get_nexus_endpoint",
            req=req,
            service=self._service,
            resp_type=temporalio.api.operatorservice.v1.GetNexusEndpointResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def list_clusters(
        self,
        req: temporalio.api.operatorservice.v1.ListClustersRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.operatorservice.v1.ListClustersResponse:
        """Invokes the OperatorService.list_clusters rpc method."""
        return await self._client._rpc_call(
            rpc="list_clusters",
            req=req,
            service=self._service,
            resp_type=temporalio.api.operatorservice.v1.ListClustersResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def list_nexus_endpoints(
        self,
        req: temporalio.api.operatorservice.v1.ListNexusEndpointsRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.operatorservice.v1.ListNexusEndpointsResponse:
        """Invokes the OperatorService.list_nexus_endpoints rpc method."""
        return await self._client._rpc_call(
            rpc="list_nexus_endpoints",
            req=req,
            service=self._service,
            resp_type=temporalio.api.operatorservice.v1.ListNexusEndpointsResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def list_search_attributes(
        self,
        req: temporalio.api.operatorservice.v1.ListSearchAttributesRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.operatorservice.v1.ListSearchAttributesResponse:
        """Invokes the OperatorService.list_search_attributes rpc method."""
        return await self._client._rpc_call(
            rpc="list_search_attributes",
            req=req,
            service=self._service,
            resp_type=temporalio.api.operatorservice.v1.ListSearchAttributesResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def remove_remote_cluster(
        self,
        req: temporalio.api.operatorservice.v1.RemoveRemoteClusterRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.operatorservice.v1.RemoveRemoteClusterResponse:
        """Invokes the OperatorService.remove_remote_cluster rpc method."""
        return await self._client._rpc_call(
            rpc="remove_remote_cluster",
            req=req,
            service=self._service,
            resp_type=temporalio.api.operatorservice.v1.RemoveRemoteClusterResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def remove_search_attributes(
        self,
        req: temporalio.api.operatorservice.v1.RemoveSearchAttributesRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.operatorservice.v1.RemoveSearchAttributesResponse:
        """Invokes the OperatorService.remove_search_attributes rpc method."""
        return await self._client._rpc_call(
            rpc="remove_search_attributes",
            req=req,
            service=self._service,
            resp_type=temporalio.api.operatorservice.v1.RemoveSearchAttributesResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def update_nexus_endpoint(
        self,
        req: temporalio.api.operatorservice.v1.UpdateNexusEndpointRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.operatorservice.v1.UpdateNexusEndpointResponse:
        """Invokes the OperatorService.update_nexus_endpoint rpc method."""
        return await self._client._rpc_call(
            rpc="update_nexus_endpoint",
            req=req,
            service=self._service,
            resp_type=temporalio.api.operatorservice.v1.UpdateNexusEndpointResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )


class CloudService:
    """RPC calls for the CloudService."""

    def __init__(self, client: ServiceClient):
        """Initialize service with the provided ServiceClient."""
        self._client = client
        self._service = "cloud"

    async def add_namespace_region(
        self,
        req: temporalio.api.cloud.cloudservice.v1.AddNamespaceRegionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.AddNamespaceRegionResponse:
        """Invokes the CloudService.add_namespace_region rpc method."""
        return await self._client._rpc_call(
            rpc="add_namespace_region",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.AddNamespaceRegionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def add_user_group_member(
        self,
        req: temporalio.api.cloud.cloudservice.v1.AddUserGroupMemberRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.AddUserGroupMemberResponse:
        """Invokes the CloudService.add_user_group_member rpc method."""
        return await self._client._rpc_call(
            rpc="add_user_group_member",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.AddUserGroupMemberResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def create_account_audit_log_sink(
        self,
        req: temporalio.api.cloud.cloudservice.v1.CreateAccountAuditLogSinkRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.CreateAccountAuditLogSinkResponse:
        """Invokes the CloudService.create_account_audit_log_sink rpc method."""
        return await self._client._rpc_call(
            rpc="create_account_audit_log_sink",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.CreateAccountAuditLogSinkResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def create_api_key(
        self,
        req: temporalio.api.cloud.cloudservice.v1.CreateApiKeyRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.CreateApiKeyResponse:
        """Invokes the CloudService.create_api_key rpc method."""
        return await self._client._rpc_call(
            rpc="create_api_key",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.CreateApiKeyResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def create_billing_report(
        self,
        req: temporalio.api.cloud.cloudservice.v1.CreateBillingReportRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.CreateBillingReportResponse:
        """Invokes the CloudService.create_billing_report rpc method."""
        return await self._client._rpc_call(
            rpc="create_billing_report",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.CreateBillingReportResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def create_connectivity_rule(
        self,
        req: temporalio.api.cloud.cloudservice.v1.CreateConnectivityRuleRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.CreateConnectivityRuleResponse:
        """Invokes the CloudService.create_connectivity_rule rpc method."""
        return await self._client._rpc_call(
            rpc="create_connectivity_rule",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.CreateConnectivityRuleResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def create_custom_role(
        self,
        req: temporalio.api.cloud.cloudservice.v1.CreateCustomRoleRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.CreateCustomRoleResponse:
        """Invokes the CloudService.create_custom_role rpc method."""
        return await self._client._rpc_call(
            rpc="create_custom_role",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.CreateCustomRoleResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def create_namespace(
        self,
        req: temporalio.api.cloud.cloudservice.v1.CreateNamespaceRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.CreateNamespaceResponse:
        """Invokes the CloudService.create_namespace rpc method."""
        return await self._client._rpc_call(
            rpc="create_namespace",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.CreateNamespaceResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def create_namespace_export_sink(
        self,
        req: temporalio.api.cloud.cloudservice.v1.CreateNamespaceExportSinkRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.CreateNamespaceExportSinkResponse:
        """Invokes the CloudService.create_namespace_export_sink rpc method."""
        return await self._client._rpc_call(
            rpc="create_namespace_export_sink",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.CreateNamespaceExportSinkResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def create_nexus_endpoint(
        self,
        req: temporalio.api.cloud.cloudservice.v1.CreateNexusEndpointRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.CreateNexusEndpointResponse:
        """Invokes the CloudService.create_nexus_endpoint rpc method."""
        return await self._client._rpc_call(
            rpc="create_nexus_endpoint",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.CreateNexusEndpointResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def create_service_account(
        self,
        req: temporalio.api.cloud.cloudservice.v1.CreateServiceAccountRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.CreateServiceAccountResponse:
        """Invokes the CloudService.create_service_account rpc method."""
        return await self._client._rpc_call(
            rpc="create_service_account",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.CreateServiceAccountResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def create_user(
        self,
        req: temporalio.api.cloud.cloudservice.v1.CreateUserRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.CreateUserResponse:
        """Invokes the CloudService.create_user rpc method."""
        return await self._client._rpc_call(
            rpc="create_user",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.CreateUserResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def create_user_group(
        self,
        req: temporalio.api.cloud.cloudservice.v1.CreateUserGroupRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.CreateUserGroupResponse:
        """Invokes the CloudService.create_user_group rpc method."""
        return await self._client._rpc_call(
            rpc="create_user_group",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.CreateUserGroupResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def delete_account_audit_log_sink(
        self,
        req: temporalio.api.cloud.cloudservice.v1.DeleteAccountAuditLogSinkRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.DeleteAccountAuditLogSinkResponse:
        """Invokes the CloudService.delete_account_audit_log_sink rpc method."""
        return await self._client._rpc_call(
            rpc="delete_account_audit_log_sink",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.DeleteAccountAuditLogSinkResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def delete_api_key(
        self,
        req: temporalio.api.cloud.cloudservice.v1.DeleteApiKeyRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.DeleteApiKeyResponse:
        """Invokes the CloudService.delete_api_key rpc method."""
        return await self._client._rpc_call(
            rpc="delete_api_key",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.DeleteApiKeyResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def delete_connectivity_rule(
        self,
        req: temporalio.api.cloud.cloudservice.v1.DeleteConnectivityRuleRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.DeleteConnectivityRuleResponse:
        """Invokes the CloudService.delete_connectivity_rule rpc method."""
        return await self._client._rpc_call(
            rpc="delete_connectivity_rule",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.DeleteConnectivityRuleResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def delete_custom_role(
        self,
        req: temporalio.api.cloud.cloudservice.v1.DeleteCustomRoleRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.DeleteCustomRoleResponse:
        """Invokes the CloudService.delete_custom_role rpc method."""
        return await self._client._rpc_call(
            rpc="delete_custom_role",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.DeleteCustomRoleResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def delete_namespace(
        self,
        req: temporalio.api.cloud.cloudservice.v1.DeleteNamespaceRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.DeleteNamespaceResponse:
        """Invokes the CloudService.delete_namespace rpc method."""
        return await self._client._rpc_call(
            rpc="delete_namespace",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.DeleteNamespaceResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def delete_namespace_export_sink(
        self,
        req: temporalio.api.cloud.cloudservice.v1.DeleteNamespaceExportSinkRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.DeleteNamespaceExportSinkResponse:
        """Invokes the CloudService.delete_namespace_export_sink rpc method."""
        return await self._client._rpc_call(
            rpc="delete_namespace_export_sink",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.DeleteNamespaceExportSinkResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def delete_namespace_region(
        self,
        req: temporalio.api.cloud.cloudservice.v1.DeleteNamespaceRegionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.DeleteNamespaceRegionResponse:
        """Invokes the CloudService.delete_namespace_region rpc method."""
        return await self._client._rpc_call(
            rpc="delete_namespace_region",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.DeleteNamespaceRegionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def delete_nexus_endpoint(
        self,
        req: temporalio.api.cloud.cloudservice.v1.DeleteNexusEndpointRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.DeleteNexusEndpointResponse:
        """Invokes the CloudService.delete_nexus_endpoint rpc method."""
        return await self._client._rpc_call(
            rpc="delete_nexus_endpoint",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.DeleteNexusEndpointResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def delete_service_account(
        self,
        req: temporalio.api.cloud.cloudservice.v1.DeleteServiceAccountRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.DeleteServiceAccountResponse:
        """Invokes the CloudService.delete_service_account rpc method."""
        return await self._client._rpc_call(
            rpc="delete_service_account",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.DeleteServiceAccountResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def delete_user(
        self,
        req: temporalio.api.cloud.cloudservice.v1.DeleteUserRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.DeleteUserResponse:
        """Invokes the CloudService.delete_user rpc method."""
        return await self._client._rpc_call(
            rpc="delete_user",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.DeleteUserResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def delete_user_group(
        self,
        req: temporalio.api.cloud.cloudservice.v1.DeleteUserGroupRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.DeleteUserGroupResponse:
        """Invokes the CloudService.delete_user_group rpc method."""
        return await self._client._rpc_call(
            rpc="delete_user_group",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.DeleteUserGroupResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def failover_namespace_region(
        self,
        req: temporalio.api.cloud.cloudservice.v1.FailoverNamespaceRegionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.FailoverNamespaceRegionResponse:
        """Invokes the CloudService.failover_namespace_region rpc method."""
        return await self._client._rpc_call(
            rpc="failover_namespace_region",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.FailoverNamespaceRegionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_account(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetAccountRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetAccountResponse:
        """Invokes the CloudService.get_account rpc method."""
        return await self._client._rpc_call(
            rpc="get_account",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetAccountResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_account_audit_log_sink(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetAccountAuditLogSinkRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetAccountAuditLogSinkResponse:
        """Invokes the CloudService.get_account_audit_log_sink rpc method."""
        return await self._client._rpc_call(
            rpc="get_account_audit_log_sink",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetAccountAuditLogSinkResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_account_audit_log_sinks(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetAccountAuditLogSinksRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetAccountAuditLogSinksResponse:
        """Invokes the CloudService.get_account_audit_log_sinks rpc method."""
        return await self._client._rpc_call(
            rpc="get_account_audit_log_sinks",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetAccountAuditLogSinksResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_api_key(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetApiKeyRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetApiKeyResponse:
        """Invokes the CloudService.get_api_key rpc method."""
        return await self._client._rpc_call(
            rpc="get_api_key",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetApiKeyResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_api_keys(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetApiKeysRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetApiKeysResponse:
        """Invokes the CloudService.get_api_keys rpc method."""
        return await self._client._rpc_call(
            rpc="get_api_keys",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetApiKeysResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_async_operation(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetAsyncOperationRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetAsyncOperationResponse:
        """Invokes the CloudService.get_async_operation rpc method."""
        return await self._client._rpc_call(
            rpc="get_async_operation",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetAsyncOperationResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_audit_logs(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetAuditLogsRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetAuditLogsResponse:
        """Invokes the CloudService.get_audit_logs rpc method."""
        return await self._client._rpc_call(
            rpc="get_audit_logs",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetAuditLogsResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_billing_report(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetBillingReportRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetBillingReportResponse:
        """Invokes the CloudService.get_billing_report rpc method."""
        return await self._client._rpc_call(
            rpc="get_billing_report",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetBillingReportResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_connectivity_rule(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetConnectivityRuleRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetConnectivityRuleResponse:
        """Invokes the CloudService.get_connectivity_rule rpc method."""
        return await self._client._rpc_call(
            rpc="get_connectivity_rule",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetConnectivityRuleResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_connectivity_rules(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetConnectivityRulesRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetConnectivityRulesResponse:
        """Invokes the CloudService.get_connectivity_rules rpc method."""
        return await self._client._rpc_call(
            rpc="get_connectivity_rules",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetConnectivityRulesResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_current_identity(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetCurrentIdentityRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetCurrentIdentityResponse:
        """Invokes the CloudService.get_current_identity rpc method."""
        return await self._client._rpc_call(
            rpc="get_current_identity",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetCurrentIdentityResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_custom_role(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetCustomRoleRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetCustomRoleResponse:
        """Invokes the CloudService.get_custom_role rpc method."""
        return await self._client._rpc_call(
            rpc="get_custom_role",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetCustomRoleResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_custom_roles(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetCustomRolesRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetCustomRolesResponse:
        """Invokes the CloudService.get_custom_roles rpc method."""
        return await self._client._rpc_call(
            rpc="get_custom_roles",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetCustomRolesResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_namespace(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetNamespaceRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetNamespaceResponse:
        """Invokes the CloudService.get_namespace rpc method."""
        return await self._client._rpc_call(
            rpc="get_namespace",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetNamespaceResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_namespace_capacity_info(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetNamespaceCapacityInfoRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetNamespaceCapacityInfoResponse:
        """Invokes the CloudService.get_namespace_capacity_info rpc method."""
        return await self._client._rpc_call(
            rpc="get_namespace_capacity_info",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetNamespaceCapacityInfoResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_namespace_export_sink(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetNamespaceExportSinkRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetNamespaceExportSinkResponse:
        """Invokes the CloudService.get_namespace_export_sink rpc method."""
        return await self._client._rpc_call(
            rpc="get_namespace_export_sink",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetNamespaceExportSinkResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_namespace_export_sinks(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetNamespaceExportSinksRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetNamespaceExportSinksResponse:
        """Invokes the CloudService.get_namespace_export_sinks rpc method."""
        return await self._client._rpc_call(
            rpc="get_namespace_export_sinks",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetNamespaceExportSinksResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_namespaces(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetNamespacesRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetNamespacesResponse:
        """Invokes the CloudService.get_namespaces rpc method."""
        return await self._client._rpc_call(
            rpc="get_namespaces",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetNamespacesResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_nexus_endpoint(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetNexusEndpointRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetNexusEndpointResponse:
        """Invokes the CloudService.get_nexus_endpoint rpc method."""
        return await self._client._rpc_call(
            rpc="get_nexus_endpoint",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetNexusEndpointResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_nexus_endpoints(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetNexusEndpointsRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetNexusEndpointsResponse:
        """Invokes the CloudService.get_nexus_endpoints rpc method."""
        return await self._client._rpc_call(
            rpc="get_nexus_endpoints",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetNexusEndpointsResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_region(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetRegionRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetRegionResponse:
        """Invokes the CloudService.get_region rpc method."""
        return await self._client._rpc_call(
            rpc="get_region",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetRegionResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_regions(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetRegionsRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetRegionsResponse:
        """Invokes the CloudService.get_regions rpc method."""
        return await self._client._rpc_call(
            rpc="get_regions",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetRegionsResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_service_account(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetServiceAccountRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetServiceAccountResponse:
        """Invokes the CloudService.get_service_account rpc method."""
        return await self._client._rpc_call(
            rpc="get_service_account",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetServiceAccountResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_service_account_namespace_assignments(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetServiceAccountNamespaceAssignmentsRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetServiceAccountNamespaceAssignmentsResponse:
        """Invokes the CloudService.get_service_account_namespace_assignments rpc method."""
        return await self._client._rpc_call(
            rpc="get_service_account_namespace_assignments",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetServiceAccountNamespaceAssignmentsResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_service_accounts(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetServiceAccountsRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetServiceAccountsResponse:
        """Invokes the CloudService.get_service_accounts rpc method."""
        return await self._client._rpc_call(
            rpc="get_service_accounts",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetServiceAccountsResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_usage(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetUsageRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetUsageResponse:
        """Invokes the CloudService.get_usage rpc method."""
        return await self._client._rpc_call(
            rpc="get_usage",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetUsageResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_user(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetUserRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetUserResponse:
        """Invokes the CloudService.get_user rpc method."""
        return await self._client._rpc_call(
            rpc="get_user",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetUserResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_user_group(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetUserGroupRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetUserGroupResponse:
        """Invokes the CloudService.get_user_group rpc method."""
        return await self._client._rpc_call(
            rpc="get_user_group",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetUserGroupResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_user_group_members(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetUserGroupMembersRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetUserGroupMembersResponse:
        """Invokes the CloudService.get_user_group_members rpc method."""
        return await self._client._rpc_call(
            rpc="get_user_group_members",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetUserGroupMembersResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_user_group_namespace_assignments(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetUserGroupNamespaceAssignmentsRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetUserGroupNamespaceAssignmentsResponse:
        """Invokes the CloudService.get_user_group_namespace_assignments rpc method."""
        return await self._client._rpc_call(
            rpc="get_user_group_namespace_assignments",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetUserGroupNamespaceAssignmentsResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_user_groups(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetUserGroupsRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetUserGroupsResponse:
        """Invokes the CloudService.get_user_groups rpc method."""
        return await self._client._rpc_call(
            rpc="get_user_groups",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetUserGroupsResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_user_namespace_assignments(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetUserNamespaceAssignmentsRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetUserNamespaceAssignmentsResponse:
        """Invokes the CloudService.get_user_namespace_assignments rpc method."""
        return await self._client._rpc_call(
            rpc="get_user_namespace_assignments",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetUserNamespaceAssignmentsResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def get_users(
        self,
        req: temporalio.api.cloud.cloudservice.v1.GetUsersRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.GetUsersResponse:
        """Invokes the CloudService.get_users rpc method."""
        return await self._client._rpc_call(
            rpc="get_users",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.GetUsersResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def remove_user_group_member(
        self,
        req: temporalio.api.cloud.cloudservice.v1.RemoveUserGroupMemberRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.RemoveUserGroupMemberResponse:
        """Invokes the CloudService.remove_user_group_member rpc method."""
        return await self._client._rpc_call(
            rpc="remove_user_group_member",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.RemoveUserGroupMemberResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def rename_custom_search_attribute(
        self,
        req: temporalio.api.cloud.cloudservice.v1.RenameCustomSearchAttributeRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.RenameCustomSearchAttributeResponse:
        """Invokes the CloudService.rename_custom_search_attribute rpc method."""
        return await self._client._rpc_call(
            rpc="rename_custom_search_attribute",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.RenameCustomSearchAttributeResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def set_service_account_namespace_access(
        self,
        req: temporalio.api.cloud.cloudservice.v1.SetServiceAccountNamespaceAccessRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.SetServiceAccountNamespaceAccessResponse:
        """Invokes the CloudService.set_service_account_namespace_access rpc method."""
        return await self._client._rpc_call(
            rpc="set_service_account_namespace_access",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.SetServiceAccountNamespaceAccessResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def set_user_group_namespace_access(
        self,
        req: temporalio.api.cloud.cloudservice.v1.SetUserGroupNamespaceAccessRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.SetUserGroupNamespaceAccessResponse:
        """Invokes the CloudService.set_user_group_namespace_access rpc method."""
        return await self._client._rpc_call(
            rpc="set_user_group_namespace_access",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.SetUserGroupNamespaceAccessResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def set_user_namespace_access(
        self,
        req: temporalio.api.cloud.cloudservice.v1.SetUserNamespaceAccessRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.SetUserNamespaceAccessResponse:
        """Invokes the CloudService.set_user_namespace_access rpc method."""
        return await self._client._rpc_call(
            rpc="set_user_namespace_access",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.SetUserNamespaceAccessResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def update_account(
        self,
        req: temporalio.api.cloud.cloudservice.v1.UpdateAccountRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.UpdateAccountResponse:
        """Invokes the CloudService.update_account rpc method."""
        return await self._client._rpc_call(
            rpc="update_account",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.UpdateAccountResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def update_account_audit_log_sink(
        self,
        req: temporalio.api.cloud.cloudservice.v1.UpdateAccountAuditLogSinkRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.UpdateAccountAuditLogSinkResponse:
        """Invokes the CloudService.update_account_audit_log_sink rpc method."""
        return await self._client._rpc_call(
            rpc="update_account_audit_log_sink",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.UpdateAccountAuditLogSinkResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def update_api_key(
        self,
        req: temporalio.api.cloud.cloudservice.v1.UpdateApiKeyRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.UpdateApiKeyResponse:
        """Invokes the CloudService.update_api_key rpc method."""
        return await self._client._rpc_call(
            rpc="update_api_key",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.UpdateApiKeyResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def update_custom_role(
        self,
        req: temporalio.api.cloud.cloudservice.v1.UpdateCustomRoleRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.UpdateCustomRoleResponse:
        """Invokes the CloudService.update_custom_role rpc method."""
        return await self._client._rpc_call(
            rpc="update_custom_role",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.UpdateCustomRoleResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def update_namespace(
        self,
        req: temporalio.api.cloud.cloudservice.v1.UpdateNamespaceRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.UpdateNamespaceResponse:
        """Invokes the CloudService.update_namespace rpc method."""
        return await self._client._rpc_call(
            rpc="update_namespace",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.UpdateNamespaceResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def update_namespace_export_sink(
        self,
        req: temporalio.api.cloud.cloudservice.v1.UpdateNamespaceExportSinkRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.UpdateNamespaceExportSinkResponse:
        """Invokes the CloudService.update_namespace_export_sink rpc method."""
        return await self._client._rpc_call(
            rpc="update_namespace_export_sink",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.UpdateNamespaceExportSinkResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def update_namespace_tags(
        self,
        req: temporalio.api.cloud.cloudservice.v1.UpdateNamespaceTagsRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.UpdateNamespaceTagsResponse:
        """Invokes the CloudService.update_namespace_tags rpc method."""
        return await self._client._rpc_call(
            rpc="update_namespace_tags",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.UpdateNamespaceTagsResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def update_nexus_endpoint(
        self,
        req: temporalio.api.cloud.cloudservice.v1.UpdateNexusEndpointRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.UpdateNexusEndpointResponse:
        """Invokes the CloudService.update_nexus_endpoint rpc method."""
        return await self._client._rpc_call(
            rpc="update_nexus_endpoint",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.UpdateNexusEndpointResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def update_service_account(
        self,
        req: temporalio.api.cloud.cloudservice.v1.UpdateServiceAccountRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.UpdateServiceAccountResponse:
        """Invokes the CloudService.update_service_account rpc method."""
        return await self._client._rpc_call(
            rpc="update_service_account",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.UpdateServiceAccountResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def update_user(
        self,
        req: temporalio.api.cloud.cloudservice.v1.UpdateUserRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.UpdateUserResponse:
        """Invokes the CloudService.update_user rpc method."""
        return await self._client._rpc_call(
            rpc="update_user",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.UpdateUserResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def update_user_group(
        self,
        req: temporalio.api.cloud.cloudservice.v1.UpdateUserGroupRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.UpdateUserGroupResponse:
        """Invokes the CloudService.update_user_group rpc method."""
        return await self._client._rpc_call(
            rpc="update_user_group",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.UpdateUserGroupResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def validate_account_audit_log_sink(
        self,
        req: temporalio.api.cloud.cloudservice.v1.ValidateAccountAuditLogSinkRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.ValidateAccountAuditLogSinkResponse:
        """Invokes the CloudService.validate_account_audit_log_sink rpc method."""
        return await self._client._rpc_call(
            rpc="validate_account_audit_log_sink",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.ValidateAccountAuditLogSinkResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def validate_namespace_export_sink(
        self,
        req: temporalio.api.cloud.cloudservice.v1.ValidateNamespaceExportSinkRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.cloud.cloudservice.v1.ValidateNamespaceExportSinkResponse:
        """Invokes the CloudService.validate_namespace_export_sink rpc method."""
        return await self._client._rpc_call(
            rpc="validate_namespace_export_sink",
            req=req,
            service=self._service,
            resp_type=temporalio.api.cloud.cloudservice.v1.ValidateNamespaceExportSinkResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )


class TestService:
    """RPC calls for the TestService."""

    def __init__(self, client: ServiceClient):
        """Initialize service with the provided ServiceClient."""
        self._client = client
        self._service = "test"

    async def get_current_time(
        self,
        req: google.protobuf.empty_pb2.Empty,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.testservice.v1.GetCurrentTimeResponse:
        """Invokes the TestService.get_current_time rpc method."""
        return await self._client._rpc_call(
            rpc="get_current_time",
            req=req,
            service=self._service,
            resp_type=temporalio.api.testservice.v1.GetCurrentTimeResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def lock_time_skipping(
        self,
        req: temporalio.api.testservice.v1.LockTimeSkippingRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.testservice.v1.LockTimeSkippingResponse:
        """Invokes the TestService.lock_time_skipping rpc method."""
        return await self._client._rpc_call(
            rpc="lock_time_skipping",
            req=req,
            service=self._service,
            resp_type=temporalio.api.testservice.v1.LockTimeSkippingResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def sleep(
        self,
        req: temporalio.api.testservice.v1.SleepRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.testservice.v1.SleepResponse:
        """Invokes the TestService.sleep rpc method."""
        return await self._client._rpc_call(
            rpc="sleep",
            req=req,
            service=self._service,
            resp_type=temporalio.api.testservice.v1.SleepResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def sleep_until(
        self,
        req: temporalio.api.testservice.v1.SleepUntilRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.testservice.v1.SleepResponse:
        """Invokes the TestService.sleep_until rpc method."""
        return await self._client._rpc_call(
            rpc="sleep_until",
            req=req,
            service=self._service,
            resp_type=temporalio.api.testservice.v1.SleepResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def unlock_time_skipping(
        self,
        req: temporalio.api.testservice.v1.UnlockTimeSkippingRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.testservice.v1.UnlockTimeSkippingResponse:
        """Invokes the TestService.unlock_time_skipping rpc method."""
        return await self._client._rpc_call(
            rpc="unlock_time_skipping",
            req=req,
            service=self._service,
            resp_type=temporalio.api.testservice.v1.UnlockTimeSkippingResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

    async def unlock_time_skipping_with_sleep(
        self,
        req: temporalio.api.testservice.v1.SleepRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.api.testservice.v1.SleepResponse:
        """Invokes the TestService.unlock_time_skipping_with_sleep rpc method."""
        return await self._client._rpc_call(
            rpc="unlock_time_skipping_with_sleep",
            req=req,
            service=self._service,
            resp_type=temporalio.api.testservice.v1.SleepResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )


class HealthService:
    """RPC calls for the HealthService."""

    def __init__(self, client: ServiceClient):
        """Initialize service with the provided ServiceClient."""
        self._client = client
        self._service = "health"

    async def check(
        self,
        req: temporalio.bridge.proto.health.v1.HealthCheckRequest,
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> temporalio.bridge.proto.health.v1.HealthCheckResponse:
        """Invokes the HealthService.check rpc method."""
        return await self._client._rpc_call(
            rpc="check",
            req=req,
            service=self._service,
            resp_type=temporalio.bridge.proto.health.v1.HealthCheckResponse,
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )
