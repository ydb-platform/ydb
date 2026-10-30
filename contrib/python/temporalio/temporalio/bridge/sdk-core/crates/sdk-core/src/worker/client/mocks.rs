use super::*;
use futures_util::{Future, FutureExt};
use std::sync::Arc;
use temporalio_client::worker::ClientWorkerSet;

pub(crate) static DEFAULT_TEST_CAPABILITIES: &Capabilities = &Capabilities {
    signal_and_query_header: true,
    internal_error_differentiation: true,
    activity_failure_include_heartbeat: true,
    supports_schedules: true,
    encoded_failure_attributes: true,
    build_id_based_versioning: true,
    upsert_memo: true,
    eager_workflow_start: true,
    sdk_metadata: true,
    count_group_by_execution_status: false,
    nexus: false,
    server_scaled_deployments: false,
    server_scaled_provider_cloud_run: false,
};

#[cfg(any(feature = "test-utilities", test))]
/// Create a mock client primed with basic necessary expectations
pub fn mock_worker_client() -> MockWorkerClient {
    let mut r = MockWorkerClient::new();
    let workers = Arc::new(ClientWorkerSet::new());
    r.expect_capabilities()
        .returning(|| Some(*DEFAULT_TEST_CAPABILITIES));
    r.expect_workers().returning(move || workers.clone());
    r.expect_is_mock().returning(|| true);
    r.expect_shutdown_worker()
        .returning(|_, _, _, _| Ok(ShutdownWorkerResponse {}));
    r.expect_sdk_name_and_version()
        .returning(|| ("test-core".to_string(), "0.0.0".to_string()));
    r.expect_identity()
        .returning(|| "test-identity".to_string());
    r.expect_worker_grouping_key().returning(Uuid::new_v4);
    r.expect_worker_instance_key().returning(Uuid::new_v4);
    r.expect_set_heartbeat_client_fields().returning(|hb| {
        hb.sdk_name = "test-core".to_string();
        hb.sdk_version = "0.0.0".to_string();
        hb.worker_identity = "test-identity".to_string();
        hb.heartbeat_time = Some(SystemTime::now().into());
    });
    r
}

/// Create a mock manual client primed with basic necessary expectations
pub(crate) fn mock_manual_worker_client() -> MockManualWorkerClient {
    let mut r = MockManualWorkerClient::new();
    let workers = Arc::new(ClientWorkerSet::new());
    r.expect_capabilities()
        .returning(|| Some(*DEFAULT_TEST_CAPABILITIES));
    r.expect_workers().returning(move || workers.clone());
    r.expect_is_mock().returning(|| true);
    r.expect_shutdown_worker()
        .returning(|_, _, _, _| async { Ok(ShutdownWorkerResponse {}) }.boxed());
    r.expect_sdk_name_and_version()
        .returning(|| ("test-core".to_string(), "0.0.0".to_string()));
    r.expect_identity()
        .returning(|| "test-identity".to_string());
    r.expect_worker_grouping_key().returning(Uuid::new_v4);
    r.expect_worker_instance_key().returning(Uuid::new_v4);
    r
}

// Need a version of the mock that can return futures so we can return potentially pending
// results. This is really annoying b/c of the async trait stuff. Need
// https://github.com/asomers/mockall/issues/189 to be fixed for it to go away.
mockall::mock! {
    pub(crate) ManualWorkerClient {}
    #[allow(unused)]
    impl WorkerClient for ManualWorkerClient {
        fn poll_workflow_task<'a, 'b>(&'a self, poll_options: PollOptions, wf_options: PollWorkflowOptions)
            -> impl Future<Output = Result<PollWorkflowTaskQueueResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn poll_activity_task<'a, 'b>(&self, poll_options: PollOptions, act_options: PollActivityOptions)
            -> impl Future<Output = Result<PollActivityTaskQueueResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn poll_nexus_task<'a, 'b>(&self, poll_options: PollOptions, nexus_options: PollNexusOptions)
            -> impl Future<Output = Result<PollNexusTaskQueueResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn complete_workflow_task<'a, 'b>(
            &self,
            request: WorkflowTaskCompletion,
        ) -> impl Future<Output = Result<RespondWorkflowTaskCompletedResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn complete_activity_task<'a, 'b>(
            &self,
            task_token: TaskToken,
            result: Option<Payloads>,
        ) -> impl Future<Output = Result<RespondActivityTaskCompletedResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn complete_nexus_task<'a, 'b>(
            &self,
            task_token: TaskToken,
            response: nexus::v1::Response,
        ) -> impl Future<Output = Result<RespondNexusTaskCompletedResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn cancel_activity_task<'a, 'b>(
            &self,
            task_token: TaskToken,
            details: Option<Payloads>,
        ) -> impl Future<Output = Result<RespondActivityTaskCanceledResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn fail_activity_task<'a, 'b>(
            &self,
            task_token: TaskToken,
            failure: Option<Failure>,
            last_heartbeat_details: Option<Payloads>,
        ) -> impl Future<Output = Result<RespondActivityTaskFailedResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn fail_workflow_task<'a, 'b>(
            &self,
            task_token: TaskToken,
            cause: WorkflowTaskFailedCause,
            failure: Option<Failure>,
        ) -> impl Future<Output = Result<RespondWorkflowTaskFailedResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn fail_nexus_task<'a, 'b>(
            &self,
            task_token: TaskToken,
            error: NexusTaskFailure,
        ) -> impl Future<Output = Result<RespondNexusTaskFailedResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn record_activity_heartbeat<'a, 'b>(
           &self,
           task_token: TaskToken,
           details: Option<Payloads>,
        ) -> impl Future<Output = Result<RecordActivityTaskHeartbeatResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn get_workflow_execution_history<'a, 'b>(
            &self,
            workflow_id: String,
            run_id: Option<String>,
            page_token: Vec<u8>
        ) -> impl Future<Output = Result<GetWorkflowExecutionHistoryResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn respond_legacy_query<'a, 'b>(
            &self,
            task_token: TaskToken,
            query_result: LegacyQueryResult,
        ) -> impl Future<Output = Result<RespondQueryTaskCompletedResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn describe_namespace<'a, 'b>(&self) ->
          impl Future<Output = Result<DescribeNamespaceResponse>> + Send + 'b
          where 'a: 'b, Self: 'b;

        fn shutdown_worker<'a, 'b>(&self, sticky_task_queue: String, task_queue: String, task_queue_types: Vec<TaskQueueType>, worker_heartbeat: Option<WorkerHeartbeat>) -> impl Future<Output = Result<ShutdownWorkerResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn record_worker_heartbeat<'a, 'b>(
            &self,
            namespace: String,
            heartbeat: Vec<WorkerHeartbeat>
        ) -> impl Future<Output = Result<RecordWorkerHeartbeatResponse>> + Send + 'b where 'a: 'b, Self: 'b;

        fn replace_connection(&self, new_connection: Connection);
        fn capabilities(&self) -> Option<Capabilities>;
        fn workers(&self) -> Arc<ClientWorkerSet>;
        fn is_mock(&self) -> bool;
        fn sdk_name_and_version(&self) -> (String, String);
        fn identity(&self) -> String;
        fn worker_grouping_key(&self) -> Uuid;
        fn worker_instance_key(&self) -> Uuid;
        fn set_heartbeat_client_fields(&self, heartbeat: &mut WorkerHeartbeat);
    }
}
