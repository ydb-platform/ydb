//! Worker-specific client needs

pub(crate) mod mocks;
use crate::{
    protosext::legacy_query_failure,
    worker::{WorkerVersioningStrategy, worker_control_task_queue},
};
use parking_lot::Mutex;
use prost_types::Duration as PbDuration;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};
use temporalio_client::{
    Connection, NamespacedClient, PayloadErrorLimits, RetryOptions, SharedReplaceableClient,
    grpc::{PayloadLimitsClient, WorkflowService},
    request_extensions::{IsWorkerTaskLongPoll, NoRetryOnMatching, RetryConfigForCall},
    worker::ClientWorkerSet,
};
use temporalio_common::protos::{
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
            TaskQueueKind, TaskQueueType, VersioningBehavior, WorkerVersioningMode,
            WorkflowTaskFailedCause,
        },
        failure::v1::Failure,
        nexus::{self, v1::NexusTaskFailure},
        protocol::v1::Message as ProtocolMessage,
        query::v1::WorkflowQueryResult,
        sdk::v1::WorkflowTaskCompletedMetadata,
        taskqueue::v1::{StickyExecutionAttributes, TaskQueue, TaskQueueMetadata},
        worker::v1::{WorkerHeartbeat, WorkerSlotsInfo},
        workflowservice::v1::{get_system_info_response::Capabilities, *},
    },
};
use tonic::IntoRequest;
use uuid::Uuid;

type Result<T, E = tonic::Status> = std::result::Result<T, E>;

/// The result of a legacy query sent via `respond_legacy_query`.
pub enum LegacyQueryResult {
    /// The query handler returned a result successfully.
    Succeeded(QueryResult),
    /// The query handler failed.
    Failed(workflow_completion::Failure),
}

/// Contains everything a worker needs to interact with the server
pub(crate) struct WorkerClientBag {
    /// Shared connection handle, used for management operations (capabilities, identity, client
    /// replacement, etc.).
    connection: SharedReplaceableClient<Connection>,
    /// Issues outbound gRPC calls, automatically attaching this worker's payload/memo error limits
    /// (set via `set_payload_error_limits`) so the gRPC layer can enforce them. Wraps a clone of
    /// `connection`, so a client replacement on `connection` is reflected here too.
    client: PayloadLimitsClient<SharedReplaceableClient<Connection>>,
    namespace: String,
    worker_versioning_strategy: WorkerVersioningStrategy,
    worker_instance_key: Uuid,
    worker_heartbeat_map: Arc<Mutex<HashMap<String, ClientHeartbeatData>>>,
}

impl WorkerClientBag {
    pub(crate) fn new(
        connection: SharedReplaceableClient<Connection>,
        namespace: String,
        worker_versioning_strategy: WorkerVersioningStrategy,
        worker_instance_key: Uuid,
    ) -> Self {
        Self {
            client: PayloadLimitsClient::new(connection.clone()),
            connection,
            namespace,
            worker_versioning_strategy,
            worker_instance_key,
            worker_heartbeat_map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn identity(&self) -> String {
        self.connection.inner_cow().identity().to_owned()
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

    fn worker_control_task_queue(&self) -> String {
        let workers = self.connection.inner_cow().workers();
        if workers.worker_control_task_queue_enabled(&self.namespace) {
            worker_control_task_queue(&self.namespace, &workers.worker_grouping_key().to_string())
        } else {
            String::new()
        }
    }
}

/// This trait contains everything workers need to interact with Temporal, and hence provides a
/// minimal mocking surface.
#[cfg_attr(any(feature = "test-utilities", test), mockall::automock)]
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
        nexus_options: PollNexusOptions,
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
        last_heartbeat_details: Option<Payloads>,
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
        error: NexusTaskFailure,
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
    async fn shutdown_worker(
        &self,
        sticky_task_queue: String,
        task_queue: String,
        task_queue_types: Vec<TaskQueueType>,
        final_heartbeat: Option<WorkerHeartbeat>,
    ) -> Result<ShutdownWorkerResponse>;
    /// Record a worker heartbeat
    async fn record_worker_heartbeat(
        &self,
        namespace: String,
        worker_heartbeat: Vec<WorkerHeartbeat>,
    ) -> Result<RecordWorkerHeartbeatResponse>;

    /// Replace the underlying connection
    fn replace_connection(&self, new_client: Connection);
    /// Return a clone of the current underlying connection, if one is available.
    fn connection(&self) -> Option<Connection> {
        None
    }
    /// Return server capabilities
    fn capabilities(&self) -> Option<Capabilities>;
    /// Return workers using this client
    fn workers(&self) -> Arc<ClientWorkerSet>;
    /// Indicates if this is a mock client
    fn is_mock(&self) -> bool;
    /// Return name and version of the SDK
    fn sdk_name_and_version(&self) -> (String, String);
    /// Get worker identity
    fn identity(&self) -> String;
    /// Get worker grouping key
    fn worker_grouping_key(&self) -> Uuid;
    /// Get worker instance key (unique per worker instance)
    fn worker_instance_key(&self) -> Uuid;
    /// Sets the client-reliant fields for WorkerHeartbeat. This also updates client-level tracking
    /// of heartbeat fields, like last heartbeat timestamp.
    fn set_heartbeat_client_fields(&self, heartbeat: &mut WorkerHeartbeat);
    /// Set the worker's payload/memo error limits
    fn set_payload_error_limits(&self, _limits: Option<PayloadErrorLimits>) {}
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
/// Additional options specific to Nexus task polling
#[derive(Debug, Clone, Default)]
pub struct PollNexusOptions {
    /// If true, poll using `TaskQueueKind::WorkerCommands` — the per-process control queue used
    /// by the shared-namespace worker to receive server-to-worker commands.
    pub worker_commands_queue: bool,
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
            identity: self.identity(),
            binary_checksum: self.binary_checksum(),
            worker_version_capabilities: self.worker_version_capabilities(),
            deployment_options: self.deployment_options(),
            worker_instance_key: self.worker_instance_key.to_string(),
            worker_control_task_queue: self.worker_control_task_queue(),
            poller_group_id: Default::default(),
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
            .client
            .clone()
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
            identity: self.identity(),
            task_queue_metadata: act_options.max_tasks_per_sec.map(|tps| TaskQueueMetadata {
                max_tasks_per_second: Some(tps),
            }),
            worker_version_capabilities: self.worker_version_capabilities(),
            deployment_options: self.deployment_options(),
            worker_instance_key: self.worker_instance_key.to_string(),
            worker_control_task_queue: self.worker_control_task_queue(),
            poller_group_id: Default::default(),
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
            .client
            .clone()
            .poll_activity_task_queue(request)
            .await?
            .into_inner())
    }

    async fn poll_nexus_task(
        &self,
        poll_options: PollOptions,
        nexus_options: PollNexusOptions,
    ) -> Result<PollNexusTaskQueueResponse> {
        let (kind, worker_version_capabilities, deployment_options) =
            if nexus_options.worker_commands_queue {
                // Worker-command partitions do not support versioning, even when the client was
                // created for a versioned application worker.
                (TaskQueueKind::WorkerCommands, None, None)
            } else {
                (
                    TaskQueueKind::Normal,
                    self.worker_version_capabilities(),
                    self.deployment_options(),
                )
            };
        #[allow(deprecated)] // want to list all fields explicitly
        let mut request = PollNexusTaskQueueRequest {
            namespace: self.namespace.clone(),
            task_queue: Some(TaskQueue {
                name: poll_options.task_queue,
                kind: kind as i32,
                normal_name: "".to_string(),
            }),
            identity: self.identity(),
            worker_version_capabilities,
            deployment_options,
            // TODO: Piggyback worker heartbeats here if this is the system nexus worker and reset
            //   heartbeating ticker when done
            worker_heartbeat: Vec::new(),
            worker_instance_key: self.worker_instance_key.to_string(),
            poller_group_id: Default::default(),
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
            .client
            .clone()
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
            identity: self.identity(),
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
            worker_instance_key: self.worker_instance_key.to_string(),
            worker_control_task_queue: self.worker_control_task_queue(),
            resource_id: Default::default(),
            // Pagination fields: default to a single, final page. Pagination logic will
            // populate these when splitting large completions.
            page_number: 0,
            intermediate_page: false,
        };
        Ok(self
            .client
            .clone()
            .respond_workflow_task_completed(request.into_request())
            .await?
            .into_inner())
    }

    async fn complete_activity_task(
        &self,
        task_token: TaskToken,
        result: Option<Payloads>,
    ) -> Result<RespondActivityTaskCompletedResponse> {
        Ok(self
            .client
            .clone()
            .respond_activity_task_completed(
                #[allow(deprecated)] // want to list all fields explicitly
                RespondActivityTaskCompletedRequest {
                    task_token: task_token.0,
                    result,
                    identity: self.identity(),
                    namespace: self.namespace.clone(),
                    worker_version: self.worker_version_stamp(),
                    // Will never be set, deprecated.
                    deployment: None,
                    deployment_options: self.deployment_options(),
                    resource_id: Default::default(),
                }
                .into_request(),
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
            .client
            .clone()
            .respond_nexus_task_completed(
                RespondNexusTaskCompletedRequest {
                    namespace: self.namespace.clone(),
                    identity: self.identity(),
                    task_token: task_token.0,
                    response: Some(response),
                    poller_group_id: Default::default(),
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn record_activity_heartbeat(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RecordActivityTaskHeartbeatResponse> {
        Ok(self
            .client
            .clone()
            .record_activity_task_heartbeat(
                RecordActivityTaskHeartbeatRequest {
                    task_token: task_token.0,
                    details,
                    identity: self.identity(),
                    namespace: self.namespace.clone(),
                    resource_id: Default::default(),
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn cancel_activity_task(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RespondActivityTaskCanceledResponse> {
        Ok(self
            .client
            .clone()
            .respond_activity_task_canceled(
                #[allow(deprecated)] // want to list all fields explicitly
                RespondActivityTaskCanceledRequest {
                    task_token: task_token.0,
                    details,
                    identity: self.identity(),
                    namespace: self.namespace.clone(),
                    worker_version: self.worker_version_stamp(),
                    // Will never be set, deprecated.
                    deployment: None,
                    deployment_options: self.deployment_options(),
                    resource_id: Default::default(),
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn fail_activity_task(
        &self,
        task_token: TaskToken,
        mut failure: Option<Failure>,
        mut last_heartbeat_details: Option<Payloads>,
    ) -> Result<RespondActivityTaskFailedResponse> {
        let payload_error_limits = self.client.error_limits();
        if let (Some(details), Some(limits)) =
            (last_heartbeat_details.as_ref(), payload_error_limits)
        {
            let heartbeat_request = RecordActivityTaskHeartbeatRequest {
                details: Some(details.clone()),
                ..Default::default()
            };
            let payload_limits = temporalio_common::payload_limits::PayloadLimits {
                blob_error: limits.blob,
                memo_error: limits.memo,
                ..Default::default()
            };
            if let Some(violation) =
                temporalio_common::payload_limits::validate_known_payload_limits(
                    &heartbeat_request,
                    &payload_limits,
                )
            {
                failure = Some(crate::worker::activities::make_payloads_too_large_failure(
                    &violation,
                ));
                last_heartbeat_details = None;
            }
        }
        Ok(self
            .client
            .clone()
            .respond_activity_task_failed(
                #[allow(deprecated)] // want to list all fields explicitly
                RespondActivityTaskFailedRequest {
                    task_token: task_token.0,
                    failure,
                    identity: self.identity(),
                    namespace: self.namespace.clone(),
                    last_heartbeat_details,
                    worker_version: self.worker_version_stamp(),
                    // Will never be set, deprecated.
                    deployment: None,
                    deployment_options: self.deployment_options(),
                    resource_id: Default::default(),
                }
                .into_request(),
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
            identity: self.identity(),
            binary_checksum: self.binary_checksum(),
            namespace: self.namespace.clone(),
            messages: vec![],
            worker_version: self.worker_version_stamp(),
            // Will never be set, deprecated.
            deployment: None,
            deployment_options: self.deployment_options(),
            resource_id: Default::default(),
        };
        Ok(self
            .client
            .clone()
            .respond_workflow_task_failed(request.into_request())
            .await?
            .into_inner())
    }

    async fn fail_nexus_task(
        &self,
        task_token: TaskToken,
        error: NexusTaskFailure,
    ) -> Result<RespondNexusTaskFailedResponse> {
        let (error, failure) = match error {
            NexusTaskFailure::Legacy(handler_err) => (Some(handler_err), None),
            NexusTaskFailure::Temporal(failure) => (None, Some(failure)),
        };

        Ok(self
            .client
            .clone()
            .respond_nexus_task_failed(
                #[allow(deprecated)]
                RespondNexusTaskFailedRequest {
                    namespace: self.namespace.clone(),
                    identity: self.identity(),
                    task_token: task_token.0,
                    failure,
                    error,
                    poller_group_id: Default::default(),
                }
                .into_request(),
            )
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
            .client
            .clone()
            .get_workflow_execution_history(
                GetWorkflowExecutionHistoryRequest {
                    namespace: self.namespace.clone(),
                    execution: Some(WorkflowExecution {
                        workflow_id,
                        run_id: run_id.unwrap_or_default(),
                    }),
                    next_page_token: page_token,
                    ..Default::default()
                }
                .into_request(),
            )
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
            .client
            .clone()
            .respond_query_task_completed(
                RespondQueryTaskCompletedRequest {
                    task_token: task_token.into(),
                    completed_type: completed_type as i32,
                    query_result,
                    error_message,
                    namespace: self.namespace.clone(),
                    failure,
                    cause: cause.into(),
                    poller_group_id: Default::default(),
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn describe_namespace(&self) -> Result<DescribeNamespaceResponse> {
        Ok(self
            .client
            .clone()
            .describe_namespace(
                DescribeNamespaceRequest {
                    namespace: self.namespace.clone(),
                    ..Default::default()
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn shutdown_worker(
        &self,
        sticky_task_queue: String,
        task_queue: String,
        task_queue_types: Vec<TaskQueueType>,
        final_heartbeat: Option<WorkerHeartbeat>,
    ) -> Result<ShutdownWorkerResponse> {
        let mut final_heartbeat = final_heartbeat;
        if let Some(w) = final_heartbeat.as_mut() {
            self.set_heartbeat_client_fields(w);
        }
        let mut request = ShutdownWorkerRequest {
            namespace: self.namespace.clone(),
            identity: self.identity(),
            sticky_task_queue,
            reason: "graceful shutdown".to_string(),
            worker_heartbeat: final_heartbeat,
            worker_instance_key: self.worker_instance_key.to_string(),
            task_queue,
            task_queue_types: task_queue_types.into_iter().map(|t| t as i32).collect(),
        }
        .into_request();
        request
            .extensions_mut()
            .insert(RetryConfigForCall(RetryOptions::no_retries()));

        Ok(
            WorkflowService::shutdown_worker(&mut self.client.clone(), request)
                .await?
                .into_inner(),
        )
    }

    async fn record_worker_heartbeat(
        &self,
        namespace: String,
        worker_heartbeat: Vec<WorkerHeartbeat>,
    ) -> Result<RecordWorkerHeartbeatResponse> {
        let request = RecordWorkerHeartbeatRequest {
            namespace,
            identity: self.identity(),
            worker_heartbeat,
            resource_id: Default::default(),
        };
        Ok(self
            .client
            .clone()
            .record_worker_heartbeat(request.into_request())
            .await?
            .into_inner())
    }

    fn replace_connection(&self, new_connection: Connection) {
        self.connection.replace_client(new_connection);
    }

    fn connection(&self) -> Option<Connection> {
        Some(self.connection.inner_clone())
    }

    fn capabilities(&self) -> Option<Capabilities> {
        self.connection.inner_cow().capabilities().cloned()
    }

    fn workers(&self) -> Arc<ClientWorkerSet> {
        self.connection.inner_cow().workers()
    }

    fn is_mock(&self) -> bool {
        false
    }

    fn sdk_name_and_version(&self) -> (String, String) {
        let inner = self.connection.inner_cow();
        (
            inner.client_name().to_owned(),
            inner.client_version().to_owned(),
        )
    }

    fn identity(&self) -> String {
        self.identity()
    }

    fn worker_grouping_key(&self) -> Uuid {
        self.connection.inner_cow().worker_grouping_key()
    }

    fn worker_instance_key(&self) -> Uuid {
        self.worker_instance_key
    }

    fn set_heartbeat_client_fields(&self, heartbeat: &mut WorkerHeartbeat) {
        if let Some(host_info) = heartbeat.host_info.as_mut() {
            host_info.worker_grouping_key = self.worker_grouping_key().to_string();
        }
        heartbeat.worker_identity = WorkerClient::identity(self);
        let sdk_name_and_ver = self.sdk_name_and_version();
        heartbeat.sdk_name = sdk_name_and_ver.0;
        heartbeat.sdk_version = sdk_name_and_ver.1;

        let now = SystemTime::now();
        heartbeat.heartbeat_time = Some(now.into());
        let mut heartbeat_map = self.worker_heartbeat_map.lock();
        let client_heartbeat_data = heartbeat_map
            .entry(heartbeat.worker_instance_key.clone())
            .or_default();
        let elapsed_since_last_heartbeat =
            client_heartbeat_data.last_heartbeat_time.map(|hb_time| {
                let dur = now.duration_since(hb_time).unwrap_or(Duration::ZERO);
                PbDuration {
                    seconds: dur.as_secs() as i64,
                    nanos: dur.subsec_nanos() as i32,
                }
            });
        heartbeat.elapsed_since_last_heartbeat = elapsed_since_last_heartbeat;
        client_heartbeat_data.last_heartbeat_time = Some(now);

        update_slots(
            &mut heartbeat.workflow_task_slots_info,
            &mut client_heartbeat_data.workflow_task_slots_info,
        );
        update_slots(
            &mut heartbeat.activity_task_slots_info,
            &mut client_heartbeat_data.activity_task_slots_info,
        );
        update_slots(
            &mut heartbeat.nexus_task_slots_info,
            &mut client_heartbeat_data.nexus_task_slots_info,
        );
        update_slots(
            &mut heartbeat.local_activity_slots_info,
            &mut client_heartbeat_data.local_activity_slots_info,
        );
    }

    fn set_payload_error_limits(&self, limits: Option<PayloadErrorLimits>) {
        self.client.set_error_limits(limits);
    }
}

impl NamespacedClient for WorkerClientBag {
    fn namespace(&self) -> String {
        self.namespace.clone()
    }

    fn identity(&self) -> String {
        self.identity()
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

#[derive(Clone, Default)]
struct SlotsInfo {
    total_processed_tasks: i32,
    total_failed_tasks: i32,
}

#[derive(Clone, Default)]
struct ClientHeartbeatData {
    last_heartbeat_time: Option<SystemTime>,

    workflow_task_slots_info: SlotsInfo,
    activity_task_slots_info: SlotsInfo,
    nexus_task_slots_info: SlotsInfo,
    local_activity_slots_info: SlotsInfo,
}

fn update_slots(slots_info: &mut Option<WorkerSlotsInfo>, client_heartbeat_data: &mut SlotsInfo) {
    if let Some(wft_slot_info) = slots_info.as_mut() {
        wft_slot_info.last_interval_processed_tasks =
            wft_slot_info.total_processed_tasks - client_heartbeat_data.total_processed_tasks;
        wft_slot_info.last_interval_failure_tasks =
            wft_slot_info.total_failed_tasks - client_heartbeat_data.total_failed_tasks;

        client_heartbeat_data.total_processed_tasks = wft_slot_info.total_processed_tasks;
        client_heartbeat_data.total_failed_tasks = wft_slot_info.total_failed_tasks;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use std::sync::{Arc, Mutex};
    use temporalio_client::{
        ConnectionOptions,
        callback_based::{CallbackBasedGrpcService, GrpcSuccessResponse},
    };
    use temporalio_common::worker::{WorkerDeploymentOptions, WorkerDeploymentVersion};

    #[tokio::test]
    async fn activity_failure_request_includes_last_heartbeat_details() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let requests_clone = requests.clone();
        let service_override = CallbackBasedGrpcService {
            callback: Arc::new(move |request| {
                let requests = requests_clone.clone();
                Box::pin(async move {
                    let proto = match request.rpc.as_str() {
                        "GetSystemInfo" => GetSystemInfoResponse::default().encode_to_vec(),
                        "RespondActivityTaskFailed" => {
                            requests.lock().unwrap().push(
                                RespondActivityTaskFailedRequest::decode(request.proto)
                                    .expect("failure request is valid"),
                            );
                            RespondActivityTaskFailedResponse::default().encode_to_vec()
                        }
                        rpc => panic!("unexpected RPC: {rpc}"),
                    };
                    Ok(GrpcSuccessResponse {
                        headers: Default::default(),
                        proto,
                    })
                })
            }),
        };
        let connection = Connection::connect(
            ConnectionOptions::new(url::Url::parse("http://localhost:7233").unwrap())
                .service_override(service_override)
                .dns_load_balancing(None)
                .build(),
        )
        .await
        .unwrap();
        let client = WorkerClientBag::new(
            SharedReplaceableClient::new(connection),
            "namespace".to_string(),
            WorkerVersioningStrategy::None {
                build_id: String::new(),
            },
            Uuid::new_v4(),
        );
        let last_heartbeat_details = Payloads {
            payloads: vec![
                temporalio_common::protos::temporal::api::common::v1::Payload {
                    data: vec![1, 2, 3],
                    ..Default::default()
                },
            ],
        };

        client
            .fail_activity_task(
                TaskToken(vec![1]),
                None,
                Some(last_heartbeat_details.clone()),
            )
            .await
            .unwrap();

        assert_eq!(
            requests.lock().unwrap()[0].last_heartbeat_details,
            Some(last_heartbeat_details)
        );
    }

    #[allow(deprecated)]
    #[tokio::test]
    async fn worker_command_nexus_polls_omit_versioning_metadata() {
        let strategies = [
            (
                "deployment",
                WorkerVersioningStrategy::WorkerDeploymentBased(
                    WorkerDeploymentOptions::new(WorkerDeploymentVersion {
                        deployment_name: "deployment".to_string(),
                        build_id: "deployment-build".to_string(),
                    })
                    .use_worker_versioning(true)
                    .build(),
                ),
            ),
            (
                "legacy",
                WorkerVersioningStrategy::LegacyBuildIdBased {
                    build_id: "legacy-build".to_string(),
                },
            ),
        ];

        for (strategy_name, strategy) in strategies {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let requests_clone = requests.clone();
            let service_override = CallbackBasedGrpcService {
                callback: Arc::new(move |request| {
                    let requests = requests_clone.clone();
                    Box::pin(async move {
                        let proto = match request.rpc.as_str() {
                            "GetSystemInfo" => GetSystemInfoResponse {
                                capabilities: Some(Capabilities {
                                    build_id_based_versioning: true,
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }
                            .encode_to_vec(),
                            "PollNexusTaskQueue" => {
                                requests.lock().unwrap().push(
                                    PollNexusTaskQueueRequest::decode(request.proto)
                                        .expect("poll request is valid"),
                                );
                                PollNexusTaskQueueResponse::default().encode_to_vec()
                            }
                            rpc => panic!("unexpected RPC: {rpc}"),
                        };
                        Ok(GrpcSuccessResponse {
                            headers: Default::default(),
                            proto,
                        })
                    })
                }),
            };
            let connection = Connection::connect(
                ConnectionOptions::new(url::Url::parse("http://localhost:7233").unwrap())
                    .service_override(service_override)
                    .dns_load_balancing(None)
                    .build(),
            )
            .await
            .unwrap();
            let client = WorkerClientBag::new(
                SharedReplaceableClient::new(connection),
                "namespace".to_string(),
                strategy,
                Uuid::new_v4(),
            );

            client
                .poll_nexus_task(
                    PollOptions {
                        task_queue: "application-queue".to_string(),
                        no_retry: None,
                        timeout_override: None,
                    },
                    PollNexusOptions {
                        worker_commands_queue: false,
                    },
                )
                .await
                .unwrap();
            client
                .poll_nexus_task(
                    PollOptions {
                        task_queue: "worker-command-queue".to_string(),
                        no_retry: None,
                        timeout_override: None,
                    },
                    PollNexusOptions {
                        worker_commands_queue: true,
                    },
                )
                .await
                .unwrap();

            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 2, "{strategy_name}");
            let normal_poll = &requests[0];
            assert_eq!(
                normal_poll.task_queue.as_ref().unwrap().kind,
                TaskQueueKind::Normal as i32,
                "{strategy_name}",
            );
            assert!(
                normal_poll
                    .worker_version_capabilities
                    .as_ref()
                    .is_some_and(|capabilities| capabilities.use_versioning)
                    || normal_poll
                        .deployment_options
                        .as_ref()
                        .is_some_and(|options| {
                            options.worker_versioning_mode == WorkerVersioningMode::Versioned as i32
                        }),
                "{strategy_name}",
            );

            let worker_command_poll = &requests[1];
            assert_eq!(
                worker_command_poll.task_queue.as_ref().unwrap().kind,
                TaskQueueKind::WorkerCommands as i32,
                "{strategy_name}",
            );
            assert!(
                worker_command_poll.worker_version_capabilities.is_none(),
                "{strategy_name}",
            );
            assert!(
                worker_command_poll.deployment_options.is_none(),
                "{strategy_name}",
            );
        }
    }
}
