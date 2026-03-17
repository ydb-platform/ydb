//! Worker-specific client needs

pub(crate) mod mocks;
use crate::abstractions::dbg_panic;
use crate::protosext::legacy_query_failure;
use crate::worker::heartbeat::HeartbeatFn;
use parking_lot::RwLock;
use std::sync::OnceLock;
use std::{sync::Arc, time::Duration};
use temporal_client::{
    Client, IsWorkerTaskLongPoll, Namespace, NamespacedClient, NoRetryOnMatching, RetryClient,
    SlotManager, WorkflowService,
};
use temporal_sdk_core_api::worker::WorkerVersioningStrategy;
use temporal_sdk_core_protos::temporal::api::worker::v1::WorkerHeartbeat;
use temporal_sdk_core_protos::{
    TaskToken,
    coresdk::{workflow_commands::QueryResult, workflow_completion},
    temporal::api::{
        command::v1::Command,
        common::v1::{
            MeteringMetadata, Payloads, WorkerVersionCapabilities, WorkerVersionStamp,
            WorkflowExecution,
        },
        deployment,
        enums::v1::{
            TaskQueueKind, VersioningBehavior, WorkerVersioningMode, WorkflowTaskFailedCause,
        },
        failure::v1::Failure,
        nexus,
        protocol::v1::Message as ProtocolMessage,
        query::v1::WorkflowQueryResult,
        sdk::v1::WorkflowTaskCompletedMetadata,
        taskqueue::v1::{StickyExecutionAttributes, TaskQueue, TaskQueueMetadata},
        workflowservice::v1::{get_system_info_response::Capabilities, *},
    },
};
use tonic::IntoRequest;

type Result<T, E = tonic::Status> = std::result::Result<T, E>;

pub enum LegacyQueryResult {
    Succeeded(QueryResult),
    Failed(workflow_completion::Failure),
}

/// Contains everything a worker needs to interact with the server
pub(crate) struct WorkerClientBag {
    replaceable_client: RwLock<RetryClient<Client>>,
    namespace: String,
    identity: String,
    worker_versioning_strategy: WorkerVersioningStrategy,
    heartbeat_data: Option<Arc<OnceLock<HeartbeatFn>>>,
}

impl WorkerClientBag {
    pub(crate) fn new(
        client: RetryClient<Client>,
        namespace: String,
        identity: String,
        worker_versioning_strategy: WorkerVersioningStrategy,
        heartbeat_data: Option<Arc<OnceLock<HeartbeatFn>>>,
    ) -> Self {
        Self {
            replaceable_client: RwLock::new(client),
            namespace,
            identity,
            worker_versioning_strategy,
            heartbeat_data,
        }
    }

    fn cloned_client(&self) -> RetryClient<Client> {
        self.replaceable_client.read().clone()
    }

    fn default_capabilities(&self) -> Capabilities {
        self.capabilities().unwrap_or_default()
    }

    fn binary_checksum(&self) -> String {
        if self.default_capabilities().build_id_based_versioning {
            "".to_string()
        } else {
            self.worker_versioning_strategy.build_id().to_owned()
        }
    }

    fn deployment_options(&self) -> Option<deployment::v1::WorkerDeploymentOptions> {
        match &self.worker_versioning_strategy {
            WorkerVersioningStrategy::WorkerDeploymentBased(dopts) => {
                Some(deployment::v1::WorkerDeploymentOptions {
                    deployment_name: dopts.version.deployment_name.clone(),
                    build_id: dopts.version.build_id.clone(),
                    worker_versioning_mode: if dopts.use_worker_versioning {
                        WorkerVersioningMode::Versioned.into()
                    } else {
                        WorkerVersioningMode::Unversioned.into()
                    },
                })
            }
            _ => None,
        }
    }

    fn worker_version_capabilities(&self) -> Option<WorkerVersionCapabilities> {
        if self.default_capabilities().build_id_based_versioning {
            Some(WorkerVersionCapabilities {
                build_id: self.worker_versioning_strategy.build_id().to_owned(),
                use_versioning: self.worker_versioning_strategy.uses_build_id_based(),
                // This will never be used, as it is the v3 version that we never supported in
                // Core SDKs.
                deployment_series_name: "".to_string(),
            })
        } else {
            None
        }
    }

    fn worker_version_stamp(&self) -> Option<WorkerVersionStamp> {
        if self.default_capabilities().build_id_based_versioning {
            Some(WorkerVersionStamp {
                build_id: self.worker_versioning_strategy.build_id().to_owned(),
                use_versioning: self.worker_versioning_strategy.uses_build_id_based(),
            })
        } else {
            None
        }
    }

    fn capture_heartbeat(&self) -> Option<WorkerHeartbeat> {
        if let Some(heartbeat_data) = self.heartbeat_data.as_ref() {
            if let Some(hb) = heartbeat_data.get() {
                hb()
            } else {
                dbg_panic!("Heartbeat function never set");
                None
            }
        } else {
            None
        }
    }
}

/// This trait contains everything workers need to interact with Temporal, and hence provides a
/// minimal mocking surface. Delegates to [WorkflowClientTrait] so see that for details.
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub trait WorkerClient: Sync + Send {
    /// Poll workflow tasks
    async fn poll_workflow_task(
        &self,
        poll_options: PollOptions,
        wf_options: PollWorkflowOptions,
    ) -> Result<PollWorkflowTaskQueueResponse>;
    /// Poll activity tasks
    async fn poll_activity_task(
        &self,
        poll_options: PollOptions,
        act_options: PollActivityOptions,
    ) -> Result<PollActivityTaskQueueResponse>;
    /// Poll Nexus tasks
    async fn poll_nexus_task(
        &self,
        poll_options: PollOptions,
    ) -> Result<PollNexusTaskQueueResponse>;
    /// Complete a workflow task
    async fn complete_workflow_task(
        &self,
        request: WorkflowTaskCompletion,
    ) -> Result<RespondWorkflowTaskCompletedResponse>;
    /// Complete an activity task
    async fn complete_activity_task(
        &self,
        task_token: TaskToken,
        result: Option<Payloads>,
    ) -> Result<RespondActivityTaskCompletedResponse>;
    /// Complete a Nexus task
    async fn complete_nexus_task(
        &self,
        task_token: TaskToken,
        response: nexus::v1::Response,
    ) -> Result<RespondNexusTaskCompletedResponse>;
    /// Record an activity heartbeat
    async fn record_activity_heartbeat(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RecordActivityTaskHeartbeatResponse>;
    /// Cancel an activity task
    async fn cancel_activity_task(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RespondActivityTaskCanceledResponse>;
    /// Fail an activity task
    async fn fail_activity_task(
        &self,
        task_token: TaskToken,
        failure: Option<Failure>,
    ) -> Result<RespondActivityTaskFailedResponse>;
    /// Fail a workflow task
    async fn fail_workflow_task(
        &self,
        task_token: TaskToken,
        cause: WorkflowTaskFailedCause,
        failure: Option<Failure>,
    ) -> Result<RespondWorkflowTaskFailedResponse>;
    /// Fail a Nexus task
    async fn fail_nexus_task(
        &self,
        task_token: TaskToken,
        error: nexus::v1::HandlerError,
    ) -> Result<RespondNexusTaskFailedResponse>;
    /// Get the workflow execution history
    async fn get_workflow_execution_history(
        &self,
        workflow_id: String,
        run_id: Option<String>,
        page_token: Vec<u8>,
    ) -> Result<GetWorkflowExecutionHistoryResponse>;
    /// Respond to a legacy query
    async fn respond_legacy_query(
        &self,
        task_token: TaskToken,
        query_result: LegacyQueryResult,
    ) -> Result<RespondQueryTaskCompletedResponse>;
    /// Describe the namespace
    async fn describe_namespace(&self) -> Result<DescribeNamespaceResponse>;
    /// Shutdown the worker
    async fn shutdown_worker(&self, sticky_task_queue: String) -> Result<ShutdownWorkerResponse>;
    /// Record a worker heartbeat
    async fn record_worker_heartbeat(
        &self,
        heartbeat: WorkerHeartbeat,
    ) -> Result<RecordWorkerHeartbeatResponse>;

    /// Replace the underlying client
    fn replace_client(&self, new_client: RetryClient<Client>);
    /// Return server capabilities
    fn capabilities(&self) -> Option<Capabilities>;
    /// Return workers using this client
    fn workers(&self) -> Arc<SlotManager>;
    /// Indicates if this is a mock client
    fn is_mock(&self) -> bool;
    /// Return name and version of the SDK
    fn sdk_name_and_version(&self) -> (String, String);
    /// Get worker identity
    fn get_identity(&self) -> String;
}

/// Configuration options shared by workflow, activity, and Nexus polling calls
#[derive(Debug, Clone)]
pub struct PollOptions {
    /// The name of the task queue to poll
    pub task_queue: String,
    /// Prevents retrying on specific gRPC statuses
    pub no_retry: Option<NoRetryOnMatching>,
    /// Overrides the default RPC timeout for the poll request
    pub timeout_override: Option<Duration>,
}
/// Additional options specific to workflow task polling
#[derive(Debug, Clone)]
pub struct PollWorkflowOptions {
    /// Optional sticky queue name for session‐based workflow polling
    pub sticky_queue_name: Option<String>,
}
/// Additional options specific to activity task polling
#[derive(Debug, Clone)]
pub struct PollActivityOptions {
    /// Optional rate limit (tasks per second) for activity polling
    pub max_tasks_per_sec: Option<f64>,
}

#[async_trait::async_trait]
impl WorkerClient for WorkerClientBag {
    async fn poll_workflow_task(
        &self,
        poll_options: PollOptions,
        wf_options: PollWorkflowOptions,
    ) -> Result<PollWorkflowTaskQueueResponse> {
        let task_queue = if let Some(sticky) = wf_options.sticky_queue_name {
            TaskQueue {
                name: sticky,
                kind: TaskQueueKind::Sticky.into(),
                normal_name: poll_options.task_queue,
            }
        } else {
            TaskQueue {
                name: poll_options.task_queue,
                kind: TaskQueueKind::Normal.into(),
                normal_name: "".to_string(),
            }
        };
        #[allow(deprecated)] // want to list all fields explicitly
        let mut request = PollWorkflowTaskQueueRequest {
            namespace: self.namespace.clone(),
            task_queue: Some(task_queue),
            identity: self.identity.clone(),
            binary_checksum: self.binary_checksum(),
            worker_version_capabilities: self.worker_version_capabilities(),
            deployment_options: self.deployment_options(),
            worker_heartbeat: None,
        }
        .into_request();
        request.extensions_mut().insert(IsWorkerTaskLongPoll);
        if let Some(nr) = poll_options.no_retry {
            request.extensions_mut().insert(nr);
        }
        if let Some(to) = poll_options.timeout_override {
            request.set_timeout(to);
        }

        Ok(self
            .cloned_client()
            .poll_workflow_task_queue(request)
            .await?
            .into_inner())
    }

    async fn poll_activity_task(
        &self,
        poll_options: PollOptions,
        act_options: PollActivityOptions,
    ) -> Result<PollActivityTaskQueueResponse> {
        #[allow(deprecated)] // want to list all fields explicitly
        let mut request = PollActivityTaskQueueRequest {
            namespace: self.namespace.clone(),
            task_queue: Some(TaskQueue {
                name: poll_options.task_queue,
                kind: TaskQueueKind::Normal as i32,
                normal_name: "".to_string(),
            }),
            identity: self.identity.clone(),
            task_queue_metadata: act_options.max_tasks_per_sec.map(|tps| TaskQueueMetadata {
                max_tasks_per_second: Some(tps),
            }),
            worker_version_capabilities: self.worker_version_capabilities(),
            deployment_options: self.deployment_options(),
            worker_heartbeat: None,
        }
        .into_request();
        request.extensions_mut().insert(IsWorkerTaskLongPoll);
        if let Some(nr) = poll_options.no_retry {
            request.extensions_mut().insert(nr);
        }
        if let Some(to) = poll_options.timeout_override {
            request.set_timeout(to);
        }

        Ok(self
            .cloned_client()
            .poll_activity_task_queue(request)
            .await?
            .into_inner())
    }

    async fn poll_nexus_task(
        &self,
        poll_options: PollOptions,
    ) -> Result<PollNexusTaskQueueResponse> {
        #[allow(deprecated)] // want to list all fields explicitly
        let mut request = PollNexusTaskQueueRequest {
            namespace: self.namespace.clone(),
            task_queue: Some(TaskQueue {
                name: poll_options.task_queue,
                kind: TaskQueueKind::Normal as i32,
                normal_name: "".to_string(),
            }),
            identity: self.identity.clone(),
            worker_version_capabilities: self.worker_version_capabilities(),
            deployment_options: self.deployment_options(),
            worker_heartbeat: self.capture_heartbeat().into_iter().collect(),
        }
        .into_request();
        request.extensions_mut().insert(IsWorkerTaskLongPoll);
        if let Some(nr) = poll_options.no_retry {
            request.extensions_mut().insert(nr);
        }
        if let Some(to) = poll_options.timeout_override {
            request.set_timeout(to);
        }

        Ok(self
            .cloned_client()
            .poll_nexus_task_queue(request)
            .await?
            .into_inner())
    }

    async fn complete_workflow_task(
        &self,
        request: WorkflowTaskCompletion,
    ) -> Result<RespondWorkflowTaskCompletedResponse> {
        #[allow(deprecated)] // want to list all fields explicitly
        let request = RespondWorkflowTaskCompletedRequest {
            task_token: request.task_token.into(),
            commands: request.commands,
            messages: request.messages,
            identity: self.identity.clone(),
            sticky_attributes: request.sticky_attributes,
            return_new_workflow_task: request.return_new_workflow_task,
            force_create_new_workflow_task: request.force_create_new_workflow_task,
            worker_version_stamp: self.worker_version_stamp(),
            binary_checksum: self.binary_checksum(),
            query_results: request
                .query_responses
                .into_iter()
                .map(|qr| {
                    let (id, completed_type, query_result, error_message) = qr.into_components();
                    (
                        id,
                        WorkflowQueryResult {
                            result_type: completed_type as i32,
                            answer: query_result,
                            error_message,
                            // TODO: https://github.com/temporalio/sdk-core/issues/867
                            failure: None,
                        },
                    )
                })
                .collect(),
            namespace: self.namespace.clone(),
            sdk_metadata: Some(request.sdk_metadata),
            metering_metadata: Some(request.metering_metadata),
            capabilities: Some(respond_workflow_task_completed_request::Capabilities {
                discard_speculative_workflow_task_with_events: true,
            }),
            // Will never be set, deprecated.
            deployment: None,
            versioning_behavior: request.versioning_behavior.into(),
            deployment_options: self.deployment_options(),
        };
        Ok(self
            .cloned_client()
            .respond_workflow_task_completed(request)
            .await?
            .into_inner())
    }

    async fn complete_activity_task(
        &self,
        task_token: TaskToken,
        result: Option<Payloads>,
    ) -> Result<RespondActivityTaskCompletedResponse> {
        Ok(self
            .cloned_client()
            .respond_activity_task_completed(
                #[allow(deprecated)] // want to list all fields explicitly
                RespondActivityTaskCompletedRequest {
                    task_token: task_token.0,
                    result,
                    identity: self.identity.clone(),
                    namespace: self.namespace.clone(),
                    worker_version: self.worker_version_stamp(),
                    // Will never be set, deprecated.
                    deployment: None,
                    deployment_options: self.deployment_options(),
                },
            )
            .await?
            .into_inner())
    }

    async fn complete_nexus_task(
        &self,
        task_token: TaskToken,
        response: nexus::v1::Response,
    ) -> Result<RespondNexusTaskCompletedResponse> {
        Ok(self
            .cloned_client()
            .respond_nexus_task_completed(RespondNexusTaskCompletedRequest {
                namespace: self.namespace.clone(),
                identity: self.identity.clone(),
                task_token: task_token.0,
                response: Some(response),
            })
            .await?
            .into_inner())
    }

    async fn record_activity_heartbeat(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RecordActivityTaskHeartbeatResponse> {
        Ok(self
            .cloned_client()
            .record_activity_task_heartbeat(RecordActivityTaskHeartbeatRequest {
                task_token: task_token.0,
                details,
                identity: self.identity.clone(),
                namespace: self.namespace.clone(),
            })
            .await?
            .into_inner())
    }

    async fn cancel_activity_task(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RespondActivityTaskCanceledResponse> {
        Ok(self
            .cloned_client()
            .respond_activity_task_canceled(
                #[allow(deprecated)] // want to list all fields explicitly
                RespondActivityTaskCanceledRequest {
                    task_token: task_token.0,
                    details,
                    identity: self.identity.clone(),
                    namespace: self.namespace.clone(),
                    worker_version: self.worker_version_stamp(),
                    // Will never be set, deprecated.
                    deployment: None,
                    deployment_options: self.deployment_options(),
                },
            )
            .await?
            .into_inner())
    }

    async fn fail_activity_task(
        &self,
        task_token: TaskToken,
        failure: Option<Failure>,
    ) -> Result<RespondActivityTaskFailedResponse> {
        Ok(self
            .cloned_client()
            .respond_activity_task_failed(
                #[allow(deprecated)] // want to list all fields explicitly
                RespondActivityTaskFailedRequest {
                    task_token: task_token.0,
                    failure,
                    identity: self.identity.clone(),
                    namespace: self.namespace.clone(),
                    // TODO: Implement - https://github.com/temporalio/sdk-core/issues/293
                    last_heartbeat_details: None,
                    worker_version: self.worker_version_stamp(),
                    // Will never be set, deprecated.
                    deployment: None,
                    deployment_options: self.deployment_options(),
                },
            )
            .await?
            .into_inner())
    }

    async fn fail_workflow_task(
        &self,
        task_token: TaskToken,
        cause: WorkflowTaskFailedCause,
        failure: Option<Failure>,
    ) -> Result<RespondWorkflowTaskFailedResponse> {
        #[allow(deprecated)] // want to list all fields explicitly
        let request = RespondWorkflowTaskFailedRequest {
            task_token: task_token.0,
            cause: cause as i32,
            failure,
            identity: self.identity.clone(),
            binary_checksum: self.binary_checksum(),
            namespace: self.namespace.clone(),
            messages: vec![],
            worker_version: self.worker_version_stamp(),
            // Will never be set, deprecated.
            deployment: None,
            deployment_options: self.deployment_options(),
        };
        Ok(self
            .cloned_client()
            .respond_workflow_task_failed(request)
            .await?
            .into_inner())
    }

    async fn fail_nexus_task(
        &self,
        task_token: TaskToken,
        error: nexus::v1::HandlerError,
    ) -> Result<RespondNexusTaskFailedResponse> {
        Ok(self
            .cloned_client()
            .respond_nexus_task_failed(RespondNexusTaskFailedRequest {
                namespace: self.namespace.clone(),
                identity: self.identity.clone(),
                task_token: task_token.0,
                error: Some(error),
            })
            .await?
            .into_inner())
    }

    async fn get_workflow_execution_history(
        &self,
        workflow_id: String,
        run_id: Option<String>,
        page_token: Vec<u8>,
    ) -> Result<GetWorkflowExecutionHistoryResponse> {
        Ok(self
            .cloned_client()
            .get_workflow_execution_history(GetWorkflowExecutionHistoryRequest {
                namespace: self.namespace.clone(),
                execution: Some(WorkflowExecution {
                    workflow_id,
                    run_id: run_id.unwrap_or_default(),
                }),
                next_page_token: page_token,
                ..Default::default()
            })
            .await?
            .into_inner())
    }

    async fn respond_legacy_query(
        &self,
        task_token: TaskToken,
        query_result: LegacyQueryResult,
    ) -> Result<RespondQueryTaskCompletedResponse> {
        let mut failure = None;
        let (query_result, cause) = match query_result {
            LegacyQueryResult::Succeeded(s) => (s, WorkflowTaskFailedCause::Unspecified),
            #[allow(deprecated)]
            LegacyQueryResult::Failed(f) => {
                let cause = f.force_cause();
                failure = f.failure.clone();
                (legacy_query_failure(f), cause)
            }
        };
        let (_, completed_type, query_result, error_message) = query_result.into_components();

        Ok(self
            .cloned_client()
            .respond_query_task_completed(RespondQueryTaskCompletedRequest {
                task_token: task_token.into(),
                completed_type: completed_type as i32,
                query_result,
                error_message,
                namespace: self.namespace.clone(),
                failure,
                cause: cause.into(),
            })
            .await?
            .into_inner())
    }

    async fn describe_namespace(&self) -> Result<DescribeNamespaceResponse> {
        Ok(self
            .cloned_client()
            .describe_namespace(
                Namespace::Name(self.namespace.clone()).into_describe_namespace_request(),
            )
            .await?
            .into_inner())
    }

    async fn shutdown_worker(&self, sticky_task_queue: String) -> Result<ShutdownWorkerResponse> {
        let request = ShutdownWorkerRequest {
            namespace: self.namespace.clone(),
            identity: self.identity.clone(),
            sticky_task_queue,
            reason: "graceful shutdown".to_string(),
            worker_heartbeat: self.capture_heartbeat(),
        };

        Ok(
            WorkflowService::shutdown_worker(&mut self.cloned_client(), request)
                .await?
                .into_inner(),
        )
    }

    fn replace_client(&self, new_client: RetryClient<Client>) {
        let mut replaceable_client = self.replaceable_client.write();
        *replaceable_client = new_client;
    }

    async fn record_worker_heartbeat(
        &self,
        heartbeat: WorkerHeartbeat,
    ) -> Result<RecordWorkerHeartbeatResponse> {
        Ok(self
            .cloned_client()
            .record_worker_heartbeat(RecordWorkerHeartbeatRequest {
                namespace: self.namespace.clone(),
                identity: self.identity.clone(),
                worker_heartbeat: vec![heartbeat],
            })
            .await?
            .into_inner())
    }

    fn capabilities(&self) -> Option<Capabilities> {
        let client = self.replaceable_client.read();
        client.get_client().inner().capabilities().cloned()
    }

    fn workers(&self) -> Arc<SlotManager> {
        let client = self.replaceable_client.read();
        client.get_client().inner().workers()
    }

    fn is_mock(&self) -> bool {
        false
    }

    fn sdk_name_and_version(&self) -> (String, String) {
        let lock = self.replaceable_client.read();
        let opts = lock.get_client().inner().options();
        (opts.client_name.clone(), opts.client_version.clone())
    }

    fn get_identity(&self) -> String {
        self.identity.clone()
    }
}

impl NamespacedClient for WorkerClientBag {
    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn get_identity(&self) -> &str {
        &self.identity
    }
}

/// A version of [RespondWorkflowTaskCompletedRequest] that will finish being filled out by the
/// server client
#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowTaskCompletion {
    /// The task token that would've been received from polling for a workflow activation
    pub task_token: TaskToken,
    /// A list of new commands to send to the server, such as starting a timer.
    pub commands: Vec<Command>,
    /// A list of protocol messages to send to the server.
    pub messages: Vec<ProtocolMessage>,
    /// If set, indicate that next task should be queued on sticky queue with given attributes.
    pub sticky_attributes: Option<StickyExecutionAttributes>,
    /// Responses to queries in the `queries` field of the workflow task.
    pub query_responses: Vec<QueryResult>,
    /// Indicate that the task completion should return a new WFT if one is available
    pub return_new_workflow_task: bool,
    /// Force a new WFT to be created after this completion
    pub force_create_new_workflow_task: bool,
    /// SDK-specific metadata to send
    pub sdk_metadata: WorkflowTaskCompletedMetadata,
    /// Metering info
    pub metering_metadata: MeteringMetadata,
    /// Versioning behavior of the workflow, if any.
    pub versioning_behavior: VersioningBehavior,
}
