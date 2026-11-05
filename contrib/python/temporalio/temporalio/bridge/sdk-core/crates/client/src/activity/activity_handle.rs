use crate::{
    ActivityCancelOptions, ActivityDescribeOptions, ActivityExecutionDescription,
    ActivityTerminateOptions, NamespacedClient,
    errors::{ActivityInteractionError, ActivityResultError},
    grpc::WorkflowService,
};
use std::marker::PhantomData;
use temporalio_common::{
    ActivityDefinition,
    data_converters::{DecodablePayloads, NoopDecodeHint, SerializationContextData},
    protos::temporal::api::{
        activity::v1::{ActivityExecutionOutcome, activity_execution_outcome},
        failure::v1::failure::FailureInfo,
        workflowservice::v1::{
            DescribeActivityExecutionRequest, PollActivityExecutionRequest,
            RequestCancelActivityExecutionRequest, TerminateActivityExecutionRequest,
        },
    },
};
use tonic::IntoRequest;
use uuid::Uuid;

/// Handle associated with a standalone activity execution that can be used to wait for the result
/// or to manage execution of the activity. Obtained from
/// [`Client::start_activity`](crate::Client::start_activity) or
/// [`Client::get_activity_handle`](crate::Client::get_activity_handle).
///
/// If [`run_id`](Self::run_id) is set, the handle always targets that specific execution.
/// If [`run_id`](Self::run_id) is `None`, each method call targets the latest run of the specified
/// [`activity_id`](Self::activity_id) at the time the method is called - this means consecutive
/// method calls may target different executions if an activity was started again with the same ID.
pub struct ActivityHandle<ClientT, ActivityT>
where
    ActivityT: ActivityDefinition,
{
    client: ClientT,
    activity_id: String,
    run_id: Option<String>,
    _phantom: PhantomData<ActivityT>,
}

impl<ClientT, ActivityT> ActivityHandle<ClientT, ActivityT>
where
    ActivityT: ActivityDefinition,
{
    pub(crate) fn new(client: ClientT, activity_id: String, run_id: Option<String>) -> Self {
        Self {
            client,
            activity_id,
            run_id,
            _phantom: PhantomData,
        }
    }

    /// Activity ID this handle is associated with.
    pub fn activity_id(&self) -> &str {
        &self.activity_id
    }

    /// Run ID of the activity execution this handle is associated with. If `None`, each method call
    /// targets the latest run of the specified [`activity_id`](Self::activity_id) at the time the
    /// method is called - this means consecutive method calls may target different executions if
    /// an activity was started again with the same ID.
    pub fn run_id(&self) -> Option<&str> {
        self.run_id.as_deref()
    }
}

impl<ClientT, ActivityT> ActivityHandle<ClientT, ActivityT>
where
    ClientT: WorkflowService + NamespacedClient + Clone,
    ActivityT: ActivityDefinition,
{
    /// Wait for the activity to complete and fetch its result. If the activity was not successful
    /// (e.g. failed, canceled, timed out), this method returns [`ActivityResultError::ActivityFailed`].
    pub async fn result(&self) -> Result<ActivityT::Output, ActivityResultError> {
        let mut client = self.client.clone();
        loop {
            let resp = client
                .poll_activity_execution(
                    PollActivityExecutionRequest {
                        namespace: client.namespace(),
                        activity_id: self.activity_id.clone(),
                        run_id: self.run_id.clone().unwrap_or_default(),
                    }
                    .into_request(),
                )
                .await?
                .into_inner();

            // If resp.outcome.value is None, poll again
            let Some(ActivityExecutionOutcome {
                value: Some(outcome),
                ..
            }) = resp.outcome
            else {
                continue;
            };

            let dc = client.data_converter();
            let ctx = SerializationContextData::Activity;

            return match outcome {
                activity_execution_outcome::Value::Result(payloads) => {
                    Ok(dc.from_payloads(&ctx, payloads.payloads).await?)
                }
                activity_execution_outcome::Value::Failure(failure) => {
                    Err(match failure.failure_info {
                        Some(FailureInfo::CanceledFailureInfo(info)) => {
                            let payloads = info.details.unwrap_or_default().payloads;
                            let details = DecodablePayloads::new(
                                payloads,
                                dc.payload_converter().clone(),
                                ctx,
                            );
                            ActivityResultError::Cancelled { details }
                        }
                        Some(FailureInfo::TerminatedFailureInfo(_)) => {
                            ActivityResultError::Terminated
                        }
                        _ => ActivityResultError::ActivityFailed(dc.to_error(
                            &ctx,
                            failure,
                            NoopDecodeHint,
                        )?),
                    })
                }
            };
        }
    }

    /// Describes the current state of the activity execution.
    pub async fn describe(
        &self,
        options: ActivityDescribeOptions,
    ) -> Result<ActivityExecutionDescription<ActivityT>, ActivityInteractionError> {
        let mut client = self.client.clone();
        let resp = client
            .describe_activity_execution(
                DescribeActivityExecutionRequest {
                    namespace: client.namespace(),
                    activity_id: self.activity_id.clone(),
                    run_id: self.run_id.clone().unwrap_or_default(),
                    include_input: options.include_input,
                    include_outcome: options.include_outcome,
                    include_heartbeat_details: options.include_heartbeat_details,
                    include_last_failure: options.include_last_failure,
                    ..Default::default()
                }
                .into_request(),
            )
            .await?
            .into_inner();

        Ok(ActivityExecutionDescription::new(
            client.data_converter().clone(),
            SerializationContextData::Activity,
            resp,
        )?)
    }

    /// Requests cancellation of the activity. Does not wait for the cancellation to complete.
    pub async fn cancel(
        &self,
        options: ActivityCancelOptions,
    ) -> Result<(), ActivityInteractionError> {
        let mut client = self.client.clone();
        client
            .request_cancel_activity_execution(
                RequestCancelActivityExecutionRequest {
                    namespace: client.namespace(),
                    activity_id: self.activity_id.clone(),
                    run_id: self.run_id.clone().unwrap_or_default(),
                    identity: client.identity(),
                    request_id: Uuid::new_v4().to_string(),
                    reason: options.reason,
                }
                .into_request(),
            )
            .await?;

        Ok(())
    }

    /// Terminates activity execution.
    pub async fn terminate(
        &self,
        options: ActivityTerminateOptions,
    ) -> Result<(), ActivityInteractionError> {
        let mut client = self.client.clone();
        client
            .terminate_activity_execution(
                TerminateActivityExecutionRequest {
                    namespace: client.namespace(),
                    activity_id: self.activity_id.clone(),
                    run_id: self.run_id.clone().unwrap_or_default(),
                    identity: client.identity(),
                    request_id: Uuid::new_v4().to_string(),
                    reason: options.reason,
                }
                .into_request(),
            )
            .await?;

        Ok(())
    }
}
