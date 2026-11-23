//! Handle for completing activities asynchronously via a client.

use crate::{
    CompleteAsyncActivityInput, FailAsyncActivityInput, HeartbeatAsyncActivityInput,
    NamespacedClient, Next, ReportAsyncActivityCancellationInput, RpcOptions, TemporalClientValue,
    errors::AsyncActivityError, grpc::WorkflowService, interceptors,
};
use futures_util::future::BoxFuture;
use temporalio_common::{
    data_converters::{SerializationContext, SerializationContextData, TemporalSerializable},
    error::{ApplicationFailure, OutgoingActivityError, OutgoingError},
    payload_visitor::encode_payloads,
    protos::{
        TaskToken,
        temporal::api::{
            common::v1::Payloads,
            workflowservice::v1::{
                RecordActivityTaskHeartbeatByIdRequest, RecordActivityTaskHeartbeatByIdResponse,
                RecordActivityTaskHeartbeatRequest, RecordActivityTaskHeartbeatResponse,
                RespondActivityTaskCanceledByIdRequest, RespondActivityTaskCanceledRequest,
                RespondActivityTaskCompletedByIdRequest, RespondActivityTaskCompletedRequest,
                RespondActivityTaskFailedByIdRequest, RespondActivityTaskFailedRequest,
            },
        },
    },
};
use tonic::IntoRequest;

async fn encode_optional_value(
    value: Option<Box<dyn TemporalClientValue>>,
    data_converter: &temporalio_common::data_converters::DataConverter,
) -> Result<Option<Payloads>, AsyncActivityError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let unencoded_payloads = {
        let payload_converter = data_converter.payload_converter();
        let context = SerializationContext {
            data: &SerializationContextData::Activity,
            converter: payload_converter,
        };
        value.serialize_payloads(&context)?
    };
    drop(value);
    let payloads = data_converter
        .codec()
        .encode(&SerializationContextData::Activity, unencoded_payloads)
        .await?;
    Ok(Some(Payloads { payloads }))
}

/// Identifies an async activity for completion outside a worker.
#[derive(Debug, Clone)]
pub enum ActivityIdentifier {
    /// Identify activity by its task token
    TaskToken(TaskToken),
    /// Identify workflow activity by workflow and activity IDs.
    ByIdWorkflow {
        /// ID of the workflow that scheduled this activity.
        workflow_id: String,
        /// Run ID of the workflow (optional - if not provided, targets the latest run).
        run_id: String,
        /// ID of the activity to complete.
        activity_id: String,
    },
    /// Identify standalone activity by activity ID.
    ByIdStandalone {
        /// ID of the activity to complete.
        activity_id: String,
        /// Run ID of the activity (optional - if not provided, targets the latest run).
        run_id: String,
    },
}

impl ActivityIdentifier {
    /// Create an identifier from a task token.
    pub fn from_task_token(token: TaskToken) -> Self {
        Self::TaskToken(token)
    }

    /// Create an identifier from workflow and activity IDs. Use an empty run id to target the
    /// latest workflow execution.
    pub fn by_id_workflow(
        workflow_id: impl Into<String>,
        run_id: impl Into<String>,
        activity_id: impl Into<String>,
    ) -> Self {
        Self::ByIdWorkflow {
            workflow_id: workflow_id.into(),
            run_id: run_id.into(),
            activity_id: activity_id.into(),
        }
    }

    /// Create an identifier from standalone activity ID. Use an empty run id to target the
    /// latest activity execution.
    pub fn by_id_standalone(activity_id: impl Into<String>, run_id: impl Into<String>) -> Self {
        Self::ByIdStandalone {
            activity_id: activity_id.into(),
            run_id: run_id.into(),
        }
    }

    /// Returns tuple of (workflow_id, run_id, activity_id).
    fn into_parts(self) -> Option<(String, String, String)> {
        match self {
            Self::TaskToken(_) => None,
            Self::ByIdWorkflow {
                workflow_id,
                run_id,
                activity_id,
            } => Some((workflow_id, run_id, activity_id)),
            Self::ByIdStandalone {
                activity_id,
                run_id,
            } => Some((String::new(), run_id, activity_id)),
        }
    }
}

/// Handle for completing activities asynchronously (outside the worker).
pub struct AsyncActivityHandle<CT> {
    client: CT,
    identifier: ActivityIdentifier,
}

impl<CT> AsyncActivityHandle<CT> {
    /// Create a new async activity handle.
    pub fn new(client: CT, identifier: ActivityIdentifier) -> Self {
        Self { client, identifier }
    }

    /// Get the identifier for this activity.
    pub fn identifier(&self) -> &ActivityIdentifier {
        &self.identifier
    }

    /// Get a reference to the underlying client.
    pub fn client(&self) -> &CT {
        &self.client
    }
}

impl<CT: WorkflowService + NamespacedClient + Clone> AsyncActivityHandle<CT> {
    /// Complete the activity with a successful result.
    pub async fn complete<T>(
        &self,
        result: Option<T>,
        rpc_options: RpcOptions,
    ) -> Result<(), AsyncActivityError>
    where
        T: TemporalSerializable + Send + 'static,
    {
        interceptors::call_complete_async_activity(
            self.client.client_interceptors(),
            CompleteAsyncActivityInput::new(self.identifier.clone(), result, rpc_options),
            Next::new({
                let mut client = self.client.clone();
                move |input: CompleteAsyncActivityInput| -> BoxFuture<
                    '_,
                    Result<(), AsyncActivityError>,
                > {
                    Box::pin(async move {
                        let (identifier, result, rpc_options) = input.into_parts();
                        let result = encode_optional_value(result, client.data_converter()).await?;
                        if let ActivityIdentifier::TaskToken(token) = identifier {
                            let mut request = RespondActivityTaskCompletedRequest {
                                task_token: token.0,
                                result,
                                identity: client.identity(),
                                namespace: client.namespace(),
                                ..Default::default()
                            }
                            .into_request();
                            rpc_options.apply_to(&mut request);
                            WorkflowService::respond_activity_task_completed(
                                &mut client,
                                request,
                            )
                            .await
                            .map_err(AsyncActivityError::from_status)?;
                        } else {
                            let (workflow_id, run_id, activity_id) = identifier.into_parts().unwrap();
                            let mut request = RespondActivityTaskCompletedByIdRequest {
                                namespace: client.namespace(),
                                workflow_id,
                                run_id,
                                activity_id,
                                result,
                                identity: client.identity(),
                                resource_id: Default::default(),
                            }
                            .into_request();
                            rpc_options.apply_to(&mut request);
                            WorkflowService::respond_activity_task_completed_by_id(
                                &mut client,
                                request,
                            )
                            .await
                            .map_err(AsyncActivityError::from_status)?;
                        }
                        Ok(())
                    })
                }
            }),
        )
        .await
    }

    /// Fail the activity with a failure.
    pub async fn fail<E, T>(
        &self,
        failure: E,
        last_heartbeat_details: Option<T>,
        rpc_options: RpcOptions,
    ) -> Result<(), AsyncActivityError>
    where
        E: Into<ApplicationFailure>,
        T: TemporalSerializable + Send + 'static,
    {
        interceptors::call_fail_async_activity(
            self.client.client_interceptors(),
            FailAsyncActivityInput::new(
                self.identifier.clone(),
                failure.into(),
                last_heartbeat_details,
                rpc_options,
            ),
            Next::new({
                let mut client = self.client.clone();
                move |input: FailAsyncActivityInput| -> BoxFuture<
                    '_,
                    Result<(), AsyncActivityError>,
                > {
                    Box::pin(async move {
                        let (identifier, application_failure, details, rpc_options) =
                            input.into_parts();
                        let data_converter = client.data_converter().clone();
                        let mut failure = data_converter.to_failure(
                            &SerializationContextData::Activity,
                            OutgoingError::Activity(OutgoingActivityError::Application(Box::new(
                                application_failure,
                            ))),
                        );
                        encode_payloads(
                            &mut failure,
                            data_converter.codec(),
                            &SerializationContextData::Activity,
                        )
                        .await?;
                        let last_heartbeat_details =
                            encode_optional_value(details, &data_converter).await?;
                        if let ActivityIdentifier::TaskToken(token) = identifier {
                            let mut request = RespondActivityTaskFailedRequest {
                                task_token: token.0,
                                failure: Some(failure),
                                identity: client.identity(),
                                namespace: client.namespace(),
                                last_heartbeat_details,
                                ..Default::default()
                            }
                            .into_request();
                            rpc_options.apply_to(&mut request);
                            WorkflowService::respond_activity_task_failed(
                                &mut client,
                                request,
                            )
                            .await
                            .map_err(AsyncActivityError::from_status)?;
                        } else {
                            let (workflow_id, run_id, activity_id) = identifier.into_parts().unwrap();
                            let mut request = RespondActivityTaskFailedByIdRequest {
                                namespace: client.namespace(),
                                workflow_id,
                                run_id,
                                activity_id,
                                failure: Some(failure),
                                identity: client.identity(),
                                last_heartbeat_details,
                                resource_id: Default::default(),
                            }
                            .into_request();
                            rpc_options.apply_to(&mut request);
                            WorkflowService::respond_activity_task_failed_by_id(
                                &mut client,
                                request,
                            )
                            .await
                            .map_err(AsyncActivityError::from_status)?;
                        }
                        Ok(())
                    })
                }
            }),
        )
        .await
    }

    /// Reports the activity as canceled.
    pub async fn report_cancelation<T>(
        &self,
        details: Option<T>,
        rpc_options: RpcOptions,
    ) -> Result<(), AsyncActivityError>
    where
        T: TemporalSerializable + Send + 'static,
    {
        interceptors::call_report_async_activity_cancellation(
            self.client.client_interceptors(),
            ReportAsyncActivityCancellationInput::new(
                self.identifier.clone(),
                details,
                rpc_options,
            ),
            Next::new({
                let mut client = self.client.clone();
                move |input: ReportAsyncActivityCancellationInput| -> BoxFuture<
                    '_,
                    Result<(), AsyncActivityError>,
                > {
                    Box::pin(async move {
                        let (identifier, details, rpc_options) = input.into_parts();
                        let details = encode_optional_value(details, client.data_converter()).await?;
                        if let ActivityIdentifier::TaskToken(token) = identifier {
                            let mut request = RespondActivityTaskCanceledRequest {
                                task_token: token.0,
                                details,
                                identity: client.identity(),
                                namespace: client.namespace(),
                                ..Default::default()
                            }
                            .into_request();
                            rpc_options.apply_to(&mut request);
                            WorkflowService::respond_activity_task_canceled(
                                &mut client,
                                request,
                            )
                            .await
                            .map_err(AsyncActivityError::from_status)?;
                        } else {
                            let (workflow_id, run_id, activity_id) = identifier.into_parts().unwrap();
                            let mut request = RespondActivityTaskCanceledByIdRequest {
                                namespace: client.namespace(),
                                workflow_id,
                                run_id,
                                activity_id,
                                details,
                                identity: client.identity(),
                                ..Default::default()
                            }
                            .into_request();
                            rpc_options.apply_to(&mut request);
                            WorkflowService::respond_activity_task_canceled_by_id(
                                &mut client,
                                request,
                            )
                            .await
                            .map_err(AsyncActivityError::from_status)?;
                        }
                        Ok(())
                    })
                }
            }),
        )
        .await
    }

    /// Record a heartbeat for the activity.
    ///
    /// Heartbeats let the server know the activity is still running and can carry
    /// progress information. The response indicates if cancellation has been requested.
    pub async fn heartbeat<T>(
        &self,
        details: Option<T>,
        rpc_options: RpcOptions,
    ) -> Result<ActivityHeartbeatResponse, AsyncActivityError>
    where
        T: TemporalSerializable + Send + 'static,
    {
        interceptors::call_heartbeat_async_activity(
            self.client.client_interceptors(),
            HeartbeatAsyncActivityInput::new(self.identifier.clone(), details, rpc_options),
            Next::new({
                let mut client = self.client.clone();
                move |input: HeartbeatAsyncActivityInput| -> BoxFuture<
                    '_,
                    Result<ActivityHeartbeatResponse, AsyncActivityError>,
                > {
                    Box::pin(async move {
                        let (identifier, details, rpc_options) = input.into_parts();
                        let details = encode_optional_value(details, client.data_converter()).await?;
                        if let ActivityIdentifier::TaskToken(token) = identifier {
                            let mut request = RecordActivityTaskHeartbeatRequest {
                                task_token: token.0,
                                details,
                                identity: client.identity(),
                                namespace: client.namespace(),
                                resource_id: Default::default(),
                            }
                            .into_request();
                            rpc_options.apply_to(&mut request);
                            let response = WorkflowService::record_activity_task_heartbeat(
                                &mut client,
                                request,
                            )
                            .await
                            .map_err(AsyncActivityError::from_status)?
                            .into_inner();
                            Ok(ActivityHeartbeatResponse::from(response))
                        } else {
                            let (workflow_id, run_id, activity_id) = identifier.into_parts().unwrap();
                            let mut request = RecordActivityTaskHeartbeatByIdRequest {
                                namespace: client.namespace(),
                                workflow_id,
                                run_id,
                                activity_id,
                                details,
                                identity: client.identity(),
                                resource_id: Default::default(),
                            }
                            .into_request();
                            rpc_options.apply_to(&mut request);
                            let response =
                                WorkflowService::record_activity_task_heartbeat_by_id(
                                    &mut client,
                                    request,
                                )
                                .await
                                .map_err(AsyncActivityError::from_status)?
                                .into_inner();
                            Ok(ActivityHeartbeatResponse::from(response))
                        }
                    })
                }
            }),
        )
        .await
    }
}

/// Response from a heartbeat call.
#[derive(Debug, Clone)]
pub struct ActivityHeartbeatResponse {
    /// True if the activity has been asked to cancel itself.
    pub cancel_requested: bool,
    /// True if the activity is paused.
    pub activity_paused: bool,
    /// True if the activity was reset.
    pub activity_reset: bool,
}

impl From<RecordActivityTaskHeartbeatResponse> for ActivityHeartbeatResponse {
    fn from(resp: RecordActivityTaskHeartbeatResponse) -> Self {
        Self {
            cancel_requested: resp.cancel_requested,
            activity_paused: resp.activity_paused,
            activity_reset: resp.activity_reset,
        }
    }
}

impl From<RecordActivityTaskHeartbeatByIdResponse> for ActivityHeartbeatResponse {
    fn from(resp: RecordActivityTaskHeartbeatByIdResponse) -> Self {
        Self {
            cancel_requested: resp.cancel_requested,
            activity_paused: resp.activity_paused,
            activity_reset: resp.activity_reset,
        }
    }
}
