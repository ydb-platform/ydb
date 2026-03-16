use super::*;
use futures_util::Future;
use std::sync::{Arc, LazyLock};
use temporal_client::SlotManager;

pub(crate) static DEFAULT_WORKERS_REGISTRY: LazyLock<Arc<SlotManager>> =
    LazyLock::new(|| Arc::new(SlotManager::new()));

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
};

#[cfg(test)]
/// Create a mock client primed with basic necessary expectations
pub(crate) fn mock_worker_client() -> MockWorkerClient {
    let mut r = MockWorkerClient::new();
    r.expect_capabilities()
        .returning(|| Some(*DEFAULT_TEST_CAPABILITIES));
    r.expect_workers()
        .returning(|| DEFAULT_WORKERS_REGISTRY.clone());
    r.expect_is_mock().returning(|| true);
    r.expect_shutdown_worker()
        .returning(|_| Ok(ShutdownWorkerResponse {}));
    r.expect_sdk_name_and_version()
        .returning(|| ("test-core".to_string(), "0.0.0".to_string()));
    r.expect_get_identity()
        .returning(|| "test-identity".to_string());
    r
}

/// Create a mock manual client primed with basic necessary expectations
pub(crate) fn mock_manual_worker_client() -> MockManualWorkerClient {
    let mut r = MockManualWorkerClient::new();
    r.expect_capabilities()
        .returning(|| Some(*DEFAULT_TEST_CAPABILITIES));
    r.expect_workers()
        .returning(|| DEFAULT_WORKERS_REGISTRY.clone());
    r.expect_is_mock().returning(|| true);
    r.expect_sdk_name_and_version()
        .returning(|| ("test-core".to_string(), "0.0.0".to_string()));
    r.expect_get_identity()
        .returning(|| "test-identity".to_string());
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

        fn poll_nexus_task<'a, 'b>(&self, poll_options: PollOptions)
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
            error: nexus::v1::HandlerError,
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

        fn shutdown_worker<'a, 'b>(&self, sticky_task_queue: String) -> impl Future<Output = Result<ShutdownWorkerResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn record_worker_heartbeat<'a, 'b>(&self, heartbeat: WorkerHeartbeat) -> impl Future<Output = Result<RecordWorkerHeartbeatResponse>> + Send + 'b where 'a: 'b, Self: 'b;

        fn replace_client(&self, new_client: RetryClient<Client>);
        fn capabilities(&self) -> Option<Capabilities>;
        fn workers(&self) -> Arc<SlotManager>;
        fn is_mock(&self) -> bool;
        fn sdk_name_and_version(&self) -> (String, String);
        fn get_identity(&self) -> String;
    }
}
