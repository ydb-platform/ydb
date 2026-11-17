// Generated file. DO NOT EDIT

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use super::{
    client::{rpc_req, rpc_resp, ClientRef, RpcCall},
    rpc_call,
};

#[pymethods]
impl ClientRef {
    fn call_workflow_service<'p>(
        &self,
        py: Python<'p>,
        call: RpcCall,
    ) -> PyResult<Bound<'p, PyAny>> {
        self.runtime.assert_same_process("use client")?;
        use temporalio_client::grpc::WorkflowService;
        let mut connection = self.connection.clone();
        self.runtime.future_into_py(py, async move {
            let bytes = match call.rpc.as_str() {
                "count_activity_executions" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        count_activity_executions
                    )
                }
                "count_nexus_operation_executions" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        count_nexus_operation_executions
                    )
                }
                "count_schedules" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        count_schedules
                    )
                }
                "count_workers" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        count_workers
                    )
                }
                "count_workflow_executions" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        count_workflow_executions
                    )
                }
                "create_schedule" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        create_schedule
                    )
                }
                "create_worker_deployment" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        create_worker_deployment
                    )
                }
                "create_worker_deployment_version" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        create_worker_deployment_version
                    )
                }
                "create_workflow_rule" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        create_workflow_rule
                    )
                }
                "delete_activity_execution" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        delete_activity_execution
                    )
                }
                "delete_nexus_operation_execution" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        delete_nexus_operation_execution
                    )
                }
                "delete_schedule" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        delete_schedule
                    )
                }
                "delete_worker_deployment" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        delete_worker_deployment
                    )
                }
                "delete_worker_deployment_version" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        delete_worker_deployment_version
                    )
                }
                "delete_workflow_execution" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        delete_workflow_execution
                    )
                }
                "delete_workflow_rule" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        delete_workflow_rule
                    )
                }
                "deprecate_namespace" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        deprecate_namespace
                    )
                }
                "describe_activity_execution" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        describe_activity_execution
                    )
                }
                "describe_batch_operation" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        describe_batch_operation
                    )
                }
                "describe_deployment" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        describe_deployment
                    )
                }
                "describe_namespace" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        describe_namespace
                    )
                }
                "describe_nexus_operation_execution" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        describe_nexus_operation_execution
                    )
                }
                "describe_schedule" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        describe_schedule
                    )
                }
                "describe_task_queue" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        describe_task_queue
                    )
                }
                "describe_worker" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        describe_worker
                    )
                }
                "describe_worker_deployment" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        describe_worker_deployment
                    )
                }
                "describe_worker_deployment_version" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        describe_worker_deployment_version
                    )
                }
                "describe_workflow_execution" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        describe_workflow_execution
                    )
                }
                "describe_workflow_rule" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        describe_workflow_rule
                    )
                }
                "execute_multi_operation" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        execute_multi_operation
                    )
                }
                "fetch_worker_config" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        fetch_worker_config
                    )
                }
                "get_cluster_info" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        get_cluster_info
                    )
                }
                "get_current_deployment" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        get_current_deployment
                    )
                }
                "get_deployment_reachability" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        get_deployment_reachability
                    )
                }
                "get_search_attributes" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        get_search_attributes
                    )
                }
                "get_system_info" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        get_system_info
                    )
                }
                "get_worker_build_id_compatibility" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        get_worker_build_id_compatibility
                    )
                }
                "get_worker_task_reachability" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        get_worker_task_reachability
                    )
                }
                "get_worker_versioning_rules" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        get_worker_versioning_rules
                    )
                }
                "get_workflow_execution_history" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        get_workflow_execution_history
                    )
                }
                "get_workflow_execution_history_reverse" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        get_workflow_execution_history_reverse
                    )
                }
                "list_activity_executions" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        list_activity_executions
                    )
                }
                "list_archived_workflow_executions" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        list_archived_workflow_executions
                    )
                }
                "list_batch_operations" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        list_batch_operations
                    )
                }
                "list_closed_workflow_executions" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        list_closed_workflow_executions
                    )
                }
                "list_deployments" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        list_deployments
                    )
                }
                "list_namespaces" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        list_namespaces
                    )
                }
                "list_nexus_operation_executions" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        list_nexus_operation_executions
                    )
                }
                "list_open_workflow_executions" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        list_open_workflow_executions
                    )
                }
                "list_schedule_matching_times" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        list_schedule_matching_times
                    )
                }
                "list_schedules" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        list_schedules
                    )
                }
                "list_task_queue_partitions" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        list_task_queue_partitions
                    )
                }
                "list_worker_deployments" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        list_worker_deployments
                    )
                }
                "list_workers" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        list_workers
                    )
                }
                "list_workflow_executions" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        list_workflow_executions
                    )
                }
                "list_workflow_rules" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        list_workflow_rules
                    )
                }
                "patch_schedule" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        patch_schedule
                    )
                }
                "pause_activity" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        pause_activity
                    )
                }
                "pause_activity_execution" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        pause_activity_execution
                    )
                }
                "pause_workflow_execution" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        pause_workflow_execution
                    )
                }
                "poll_activity_execution" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        poll_activity_execution
                    )
                }
                "poll_activity_task_queue" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        poll_activity_task_queue
                    )
                }
                "poll_nexus_operation_execution" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        poll_nexus_operation_execution
                    )
                }
                "poll_nexus_task_queue" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        poll_nexus_task_queue
                    )
                }
                "poll_workflow_execution_time_skipping" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        poll_workflow_execution_time_skipping
                    )
                }
                "poll_workflow_execution_update" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        poll_workflow_execution_update
                    )
                }
                "poll_workflow_task_queue" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        poll_workflow_task_queue
                    )
                }
                "query_workflow" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        query_workflow
                    )
                }
                "record_activity_task_heartbeat" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        record_activity_task_heartbeat
                    )
                }
                "record_activity_task_heartbeat_by_id" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        record_activity_task_heartbeat_by_id
                    )
                }
                "record_worker_heartbeat" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        record_worker_heartbeat
                    )
                }
                "register_namespace" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        register_namespace
                    )
                }
                "request_cancel_activity_execution" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        request_cancel_activity_execution
                    )
                }
                "request_cancel_nexus_operation_execution" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        request_cancel_nexus_operation_execution
                    )
                }
                "request_cancel_workflow_execution" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        request_cancel_workflow_execution
                    )
                }
                "reset_activity" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        reset_activity
                    )
                }
                "reset_activity_execution" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        reset_activity_execution
                    )
                }
                "reset_sticky_task_queue" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        reset_sticky_task_queue
                    )
                }
                "reset_workflow_execution" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        reset_workflow_execution
                    )
                }
                "respond_activity_task_canceled" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        respond_activity_task_canceled
                    )
                }
                "respond_activity_task_canceled_by_id" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        respond_activity_task_canceled_by_id
                    )
                }
                "respond_activity_task_completed" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        respond_activity_task_completed
                    )
                }
                "respond_activity_task_completed_by_id" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        respond_activity_task_completed_by_id
                    )
                }
                "respond_activity_task_failed" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        respond_activity_task_failed
                    )
                }
                "respond_activity_task_failed_by_id" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        respond_activity_task_failed_by_id
                    )
                }
                "respond_nexus_task_completed" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        respond_nexus_task_completed
                    )
                }
                "respond_nexus_task_failed" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        respond_nexus_task_failed
                    )
                }
                "respond_query_task_completed" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        respond_query_task_completed
                    )
                }
                "respond_workflow_task_completed" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        respond_workflow_task_completed
                    )
                }
                "respond_workflow_task_failed" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        respond_workflow_task_failed
                    )
                }
                "scan_workflow_executions" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        scan_workflow_executions
                    )
                }
                "set_current_deployment" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        set_current_deployment
                    )
                }
                "set_worker_deployment_current_version" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        set_worker_deployment_current_version
                    )
                }
                "set_worker_deployment_manager" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        set_worker_deployment_manager
                    )
                }
                "set_worker_deployment_ramping_version" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        set_worker_deployment_ramping_version
                    )
                }
                "shutdown_worker" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        shutdown_worker
                    )
                }
                "signal_with_start_workflow_execution" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        signal_with_start_workflow_execution
                    )
                }
                "signal_workflow_execution" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        signal_workflow_execution
                    )
                }
                "start_activity_execution" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        start_activity_execution
                    )
                }
                "start_batch_operation" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        start_batch_operation
                    )
                }
                "start_nexus_operation_execution" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        start_nexus_operation_execution
                    )
                }
                "start_workflow_execution" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        start_workflow_execution
                    )
                }
                "stop_batch_operation" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        stop_batch_operation
                    )
                }
                "terminate_activity_execution" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        terminate_activity_execution
                    )
                }
                "terminate_nexus_operation_execution" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        terminate_nexus_operation_execution
                    )
                }
                "terminate_workflow_execution" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        terminate_workflow_execution
                    )
                }
                "trigger_workflow_rule" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        trigger_workflow_rule
                    )
                }
                "unpause_activity" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        unpause_activity
                    )
                }
                "unpause_activity_execution" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        unpause_activity_execution
                    )
                }
                "unpause_workflow_execution" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        unpause_workflow_execution
                    )
                }
                "update_activity_execution_options" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        update_activity_execution_options
                    )
                }
                "update_activity_options" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        update_activity_options
                    )
                }
                "update_namespace" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        update_namespace
                    )
                }
                "update_schedule" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        update_schedule
                    )
                }
                "update_task_queue_config" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        update_task_queue_config
                    )
                }
                "update_worker_build_id_compatibility" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        update_worker_build_id_compatibility
                    )
                }
                "update_worker_config" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        update_worker_config
                    )
                }
                "update_worker_deployment_version_compute_config" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        update_worker_deployment_version_compute_config
                    )
                }
                "update_worker_deployment_version_metadata" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        update_worker_deployment_version_metadata
                    )
                }
                "update_worker_versioning_rules" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        update_worker_versioning_rules
                    )
                }
                "update_workflow_execution" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        update_workflow_execution
                    )
                }
                "update_workflow_execution_options" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        update_workflow_execution_options
                    )
                }
                "validate_worker_deployment_version_compute_config" => {
                    rpc_call!(
                        connection,
                        call,
                        WorkflowService,
                        workflow_service,
                        validate_worker_deployment_version_compute_config
                    )
                }
                _ => {
                    return Err(PyValueError::new_err(format!(
                        "Unknown RPC call {}",
                        call.rpc
                    )))
                }
            }?;
            Ok(bytes)
        })
    }

    fn call_operator_service<'p>(
        &self,
        py: Python<'p>,
        call: RpcCall,
    ) -> PyResult<Bound<'p, PyAny>> {
        self.runtime.assert_same_process("use client")?;
        use temporalio_client::grpc::OperatorService;
        let mut connection = self.connection.clone();
        self.runtime.future_into_py(py, async move {
            let bytes = match call.rpc.as_str() {
                "add_or_update_remote_cluster" => {
                    rpc_call!(
                        connection,
                        call,
                        OperatorService,
                        operator_service,
                        add_or_update_remote_cluster
                    )
                }
                "add_search_attributes" => {
                    rpc_call!(
                        connection,
                        call,
                        OperatorService,
                        operator_service,
                        add_search_attributes
                    )
                }
                "create_nexus_endpoint" => {
                    rpc_call!(
                        connection,
                        call,
                        OperatorService,
                        operator_service,
                        create_nexus_endpoint
                    )
                }
                "delete_namespace" => {
                    rpc_call!(
                        connection,
                        call,
                        OperatorService,
                        operator_service,
                        delete_namespace
                    )
                }
                "delete_nexus_endpoint" => {
                    rpc_call!(
                        connection,
                        call,
                        OperatorService,
                        operator_service,
                        delete_nexus_endpoint
                    )
                }
                "get_nexus_endpoint" => {
                    rpc_call!(
                        connection,
                        call,
                        OperatorService,
                        operator_service,
                        get_nexus_endpoint
                    )
                }
                "list_clusters" => {
                    rpc_call!(
                        connection,
                        call,
                        OperatorService,
                        operator_service,
                        list_clusters
                    )
                }
                "list_nexus_endpoints" => {
                    rpc_call!(
                        connection,
                        call,
                        OperatorService,
                        operator_service,
                        list_nexus_endpoints
                    )
                }
                "list_search_attributes" => {
                    rpc_call!(
                        connection,
                        call,
                        OperatorService,
                        operator_service,
                        list_search_attributes
                    )
                }
                "remove_remote_cluster" => {
                    rpc_call!(
                        connection,
                        call,
                        OperatorService,
                        operator_service,
                        remove_remote_cluster
                    )
                }
                "remove_search_attributes" => {
                    rpc_call!(
                        connection,
                        call,
                        OperatorService,
                        operator_service,
                        remove_search_attributes
                    )
                }
                "update_nexus_endpoint" => {
                    rpc_call!(
                        connection,
                        call,
                        OperatorService,
                        operator_service,
                        update_nexus_endpoint
                    )
                }
                _ => {
                    return Err(PyValueError::new_err(format!(
                        "Unknown RPC call {}",
                        call.rpc
                    )))
                }
            }?;
            Ok(bytes)
        })
    }

    fn call_cloud_service<'p>(&self, py: Python<'p>, call: RpcCall) -> PyResult<Bound<'p, PyAny>> {
        self.runtime.assert_same_process("use client")?;
        use temporalio_client::grpc::CloudService;
        let mut connection = self.connection.clone();
        self.runtime.future_into_py(py, async move {
            let bytes = match call.rpc.as_str() {
                "add_namespace_region" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        add_namespace_region
                    )
                }
                "add_user_group_member" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        add_user_group_member
                    )
                }
                "create_account_audit_log_sink" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        create_account_audit_log_sink
                    )
                }
                "create_api_key" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        create_api_key
                    )
                }
                "create_billing_report" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        create_billing_report
                    )
                }
                "create_connectivity_rule" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        create_connectivity_rule
                    )
                }
                "create_custom_role" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        create_custom_role
                    )
                }
                "create_namespace" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        create_namespace
                    )
                }
                "create_namespace_export_sink" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        create_namespace_export_sink
                    )
                }
                "create_nexus_endpoint" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        create_nexus_endpoint
                    )
                }
                "create_service_account" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        create_service_account
                    )
                }
                "create_user" => {
                    rpc_call!(connection, call, CloudService, cloud_service, create_user)
                }
                "create_user_group" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        create_user_group
                    )
                }
                "delete_account_audit_log_sink" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        delete_account_audit_log_sink
                    )
                }
                "delete_api_key" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        delete_api_key
                    )
                }
                "delete_connectivity_rule" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        delete_connectivity_rule
                    )
                }
                "delete_custom_role" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        delete_custom_role
                    )
                }
                "delete_namespace" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        delete_namespace
                    )
                }
                "delete_namespace_export_sink" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        delete_namespace_export_sink
                    )
                }
                "delete_namespace_region" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        delete_namespace_region
                    )
                }
                "delete_nexus_endpoint" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        delete_nexus_endpoint
                    )
                }
                "delete_service_account" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        delete_service_account
                    )
                }
                "delete_user" => {
                    rpc_call!(connection, call, CloudService, cloud_service, delete_user)
                }
                "delete_user_group" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        delete_user_group
                    )
                }
                "failover_namespace_region" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        failover_namespace_region
                    )
                }
                "get_account" => {
                    rpc_call!(connection, call, CloudService, cloud_service, get_account)
                }
                "get_account_audit_log_sink" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        get_account_audit_log_sink
                    )
                }
                "get_account_audit_log_sinks" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        get_account_audit_log_sinks
                    )
                }
                "get_api_key" => {
                    rpc_call!(connection, call, CloudService, cloud_service, get_api_key)
                }
                "get_api_keys" => {
                    rpc_call!(connection, call, CloudService, cloud_service, get_api_keys)
                }
                "get_async_operation" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        get_async_operation
                    )
                }
                "get_audit_logs" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        get_audit_logs
                    )
                }
                "get_billing_report" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        get_billing_report
                    )
                }
                "get_connectivity_rule" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        get_connectivity_rule
                    )
                }
                "get_connectivity_rules" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        get_connectivity_rules
                    )
                }
                "get_current_identity" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        get_current_identity
                    )
                }
                "get_custom_role" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        get_custom_role
                    )
                }
                "get_custom_roles" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        get_custom_roles
                    )
                }
                "get_namespace" => {
                    rpc_call!(connection, call, CloudService, cloud_service, get_namespace)
                }
                "get_namespace_capacity_info" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        get_namespace_capacity_info
                    )
                }
                "get_namespace_export_sink" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        get_namespace_export_sink
                    )
                }
                "get_namespace_export_sinks" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        get_namespace_export_sinks
                    )
                }
                "get_namespaces" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        get_namespaces
                    )
                }
                "get_nexus_endpoint" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        get_nexus_endpoint
                    )
                }
                "get_nexus_endpoints" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        get_nexus_endpoints
                    )
                }
                "get_region" => {
                    rpc_call!(connection, call, CloudService, cloud_service, get_region)
                }
                "get_regions" => {
                    rpc_call!(connection, call, CloudService, cloud_service, get_regions)
                }
                "get_service_account" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        get_service_account
                    )
                }
                "get_service_account_namespace_assignments" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        get_service_account_namespace_assignments
                    )
                }
                "get_service_accounts" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        get_service_accounts
                    )
                }
                "get_usage" => {
                    rpc_call!(connection, call, CloudService, cloud_service, get_usage)
                }
                "get_user" => {
                    rpc_call!(connection, call, CloudService, cloud_service, get_user)
                }
                "get_user_group" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        get_user_group
                    )
                }
                "get_user_group_members" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        get_user_group_members
                    )
                }
                "get_user_group_namespace_assignments" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        get_user_group_namespace_assignments
                    )
                }
                "get_user_groups" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        get_user_groups
                    )
                }
                "get_user_namespace_assignments" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        get_user_namespace_assignments
                    )
                }
                "get_users" => {
                    rpc_call!(connection, call, CloudService, cloud_service, get_users)
                }
                "remove_user_group_member" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        remove_user_group_member
                    )
                }
                "rename_custom_search_attribute" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        rename_custom_search_attribute
                    )
                }
                "set_service_account_namespace_access" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        set_service_account_namespace_access
                    )
                }
                "set_user_group_namespace_access" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        set_user_group_namespace_access
                    )
                }
                "set_user_namespace_access" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        set_user_namespace_access
                    )
                }
                "update_account" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        update_account
                    )
                }
                "update_account_audit_log_sink" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        update_account_audit_log_sink
                    )
                }
                "update_api_key" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        update_api_key
                    )
                }
                "update_custom_role" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        update_custom_role
                    )
                }
                "update_namespace" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        update_namespace
                    )
                }
                "update_namespace_export_sink" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        update_namespace_export_sink
                    )
                }
                "update_namespace_tags" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        update_namespace_tags
                    )
                }
                "update_nexus_endpoint" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        update_nexus_endpoint
                    )
                }
                "update_service_account" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        update_service_account
                    )
                }
                "update_user" => {
                    rpc_call!(connection, call, CloudService, cloud_service, update_user)
                }
                "update_user_group" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        update_user_group
                    )
                }
                "validate_account_audit_log_sink" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        validate_account_audit_log_sink
                    )
                }
                "validate_namespace_export_sink" => {
                    rpc_call!(
                        connection,
                        call,
                        CloudService,
                        cloud_service,
                        validate_namespace_export_sink
                    )
                }
                _ => {
                    return Err(PyValueError::new_err(format!(
                        "Unknown RPC call {}",
                        call.rpc
                    )))
                }
            }?;
            Ok(bytes)
        })
    }

    fn call_test_service<'p>(&self, py: Python<'p>, call: RpcCall) -> PyResult<Bound<'p, PyAny>> {
        self.runtime.assert_same_process("use client")?;
        use temporalio_client::grpc::TestService;
        let mut connection = self.connection.clone();
        self.runtime.future_into_py(py, async move {
            let bytes = match call.rpc.as_str() {
                "get_current_time" => {
                    rpc_call!(
                        connection,
                        call,
                        TestService,
                        test_service,
                        get_current_time
                    )
                }
                "lock_time_skipping" => {
                    rpc_call!(
                        connection,
                        call,
                        TestService,
                        test_service,
                        lock_time_skipping
                    )
                }
                "sleep" => {
                    rpc_call!(connection, call, TestService, test_service, sleep)
                }
                "sleep_until" => {
                    rpc_call!(connection, call, TestService, test_service, sleep_until)
                }
                "unlock_time_skipping" => {
                    rpc_call!(
                        connection,
                        call,
                        TestService,
                        test_service,
                        unlock_time_skipping
                    )
                }
                "unlock_time_skipping_with_sleep" => {
                    rpc_call!(
                        connection,
                        call,
                        TestService,
                        test_service,
                        unlock_time_skipping_with_sleep
                    )
                }
                _ => {
                    return Err(PyValueError::new_err(format!(
                        "Unknown RPC call {}",
                        call.rpc
                    )))
                }
            }?;
            Ok(bytes)
        })
    }

    fn call_health_service<'p>(&self, py: Python<'p>, call: RpcCall) -> PyResult<Bound<'p, PyAny>> {
        self.runtime.assert_same_process("use client")?;
        use temporalio_client::grpc::HealthService;
        let mut connection = self.connection.clone();
        self.runtime.future_into_py(py, async move {
            let bytes = match call.rpc.as_str() {
                "check" => {
                    rpc_call!(connection, call, HealthService, health_service, check)
                }
                _ => {
                    return Err(PyValueError::new_err(format!(
                        "Unknown RPC call {}",
                        call.rpc
                    )))
                }
            }?;
            Ok(bytes)
        })
    }
}
