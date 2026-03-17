mod poll_buffer;

pub(crate) use poll_buffer::{
    ActivityTaskOptions, LongPollBuffer, WorkflowTaskOptions, WorkflowTaskPoller,
};
pub use temporal_client::{
    Client, ClientOptions, ClientOptionsBuilder, ClientTlsConfig, RetryClient, RetryConfig,
    TlsConfig, WorkflowClientTrait,
};

use crate::{
    abstractions::{OwnedMeteredSemPermit, TrackedOwnedMeteredSemPermit},
    telemetry::metrics::MetricsContext,
};
use anyhow::{anyhow, bail};
use futures_util::{Stream, stream};
use std::{fmt::Debug, marker::PhantomData};
use temporal_sdk_core_api::worker::{ActivitySlotKind, NexusSlotKind, SlotKind, WorkflowSlotKind};
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::{
    PollActivityTaskQueueResponse, PollNexusTaskQueueResponse, PollWorkflowTaskQueueResponse,
};
use tokio::select;
use tokio_util::sync::CancellationToken;

#[cfg(test)]
use futures_util::Future;
#[cfg(test)]
pub(crate) use poll_buffer::MockPermittedPollBuffer;

pub(crate) type Result<T, E = tonic::Status> = std::result::Result<T, E>;

/// A trait for things that long poll the server.
#[cfg_attr(test, mockall::automock)]
#[cfg_attr(test, allow(unused))]
#[async_trait::async_trait]
pub(crate) trait Poller<PollResult>
where
    PollResult: Send + Sync + 'static,
{
    async fn poll(&self) -> Option<Result<PollResult>>;
    fn notify_shutdown(&self);
    async fn shutdown(self);
    /// Need a separate shutdown to be able to consume boxes :(
    async fn shutdown_box(self: Box<Self>);
}
pub(crate) type BoxedPoller<T> = Box<dyn Poller<T> + Send + Sync + 'static>;
pub(crate) type BoxedWFPoller = BoxedPoller<(
    PollWorkflowTaskQueueResponse,
    OwnedMeteredSemPermit<WorkflowSlotKind>,
)>;
pub(crate) type BoxedActPoller = BoxedPoller<(
    PollActivityTaskQueueResponse,
    OwnedMeteredSemPermit<ActivitySlotKind>,
)>;
pub(crate) type BoxedNexusPoller = BoxedPoller<(
    PollNexusTaskQueueResponse,
    OwnedMeteredSemPermit<NexusSlotKind>,
)>;

#[async_trait::async_trait]
impl<T> Poller<T> for Box<dyn Poller<T> + Send + Sync>
where
    T: Send + Sync + 'static,
{
    async fn poll(&self) -> Option<Result<T>> {
        Poller::poll(self.as_ref()).await
    }

    fn notify_shutdown(&self) {
        Poller::notify_shutdown(self.as_ref())
    }

    async fn shutdown(self) {
        Poller::shutdown(self).await
    }

    async fn shutdown_box(self: Box<Self>) {
        Poller::shutdown_box(self).await
    }
}

#[cfg(test)]
mockall::mock! {
    pub ManualPoller<T: Send + Sync + 'static> {}
    #[allow(unused)]
    impl<T: Send + Sync + 'static> Poller<T> for ManualPoller<T> {
        fn poll<'a, 'b>(&self)
          -> impl Future<Output = Option<Result<T>>> + Send + 'b
            where 'a: 'b, Self: 'b;
        fn notify_shutdown(&self);
        fn shutdown<'a>(self)
          -> impl Future<Output = ()> + Send + 'a
            where Self: 'a;
        fn shutdown_box<'a>(self: Box<Self>)
          -> impl Future<Output = ()> + Send + 'a
            where Self: 'a;
    }
}

#[derive(Debug)]
pub(crate) struct PermittedTqResp<T: ValidatableTask> {
    pub(crate) permit: OwnedMeteredSemPermit<T::SlotKind>,
    pub(crate) resp: T,
}

#[derive(Debug)]
pub(crate) struct TrackedPermittedTqResp<T: ValidatableTask> {
    pub(crate) permit: TrackedOwnedMeteredSemPermit<T::SlotKind>,
    pub(crate) resp: T,
}

pub(crate) trait ValidatableTask:
    Debug + Default + PartialEq + Send + Sync + 'static
{
    type SlotKind: SlotKind;

    fn validate(&self) -> Result<(), anyhow::Error>;
    fn task_name() -> &'static str;
}

pub(crate) struct TaskPollerStream<P, T>
where
    P: Poller<(T, OwnedMeteredSemPermit<T::SlotKind>)>,
    T: ValidatableTask,
{
    poller: P,
    metrics: MetricsContext,
    metrics_no_task: fn(&MetricsContext),
    shutdown_token: CancellationToken,
    poller_was_shutdown: bool,
    _phantom: PhantomData<T>,
}

impl<P, T> TaskPollerStream<P, T>
where
    P: Poller<(T, OwnedMeteredSemPermit<T::SlotKind>)>,
    T: ValidatableTask,
{
    pub(crate) fn new(
        poller: P,
        metrics: MetricsContext,
        metrics_no_task: fn(&MetricsContext),
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            poller,
            metrics,
            metrics_no_task,
            shutdown_token,
            poller_was_shutdown: false,
            _phantom: PhantomData,
        }
    }

    fn into_stream(self) -> impl Stream<Item = Result<PermittedTqResp<T>, tonic::Status>> {
        stream::unfold(self, |mut state| async move {
            loop {
                let poll = async {
                    loop {
                        return match state.poller.poll().await {
                            Some(Ok((task, permit))) => {
                                if task == Default::default() {
                                    // We get the default proto in the event that the long poll
                                    // times out.
                                    debug!("Poll {} task timeout", T::task_name());
                                    (state.metrics_no_task)(&state.metrics);
                                    continue;
                                }

                                if let Err(e) = task.validate() {
                                    warn!(
                                        "Received invalid {} task ({}): {:?}",
                                        T::task_name(),
                                        e,
                                        &task
                                    );
                                    return Some(Err(tonic::Status::invalid_argument(
                                        e.to_string(),
                                    )));
                                }

                                Some(Ok(PermittedTqResp { resp: task, permit }))
                            }
                            Some(Err(e)) => {
                                warn!(error=?e, "Error while polling for {} tasks", T::task_name());
                                Some(Err(e))
                            }
                            // If poller returns None, it's dead, thus we also return None to
                            // terminate this stream.
                            None => None,
                        };
                    }
                };
                if state.poller_was_shutdown {
                    return poll.await.map(|res| (res, state));
                }
                select! {
                    biased;

                    _ = state.shutdown_token.cancelled() => {
                        state.poller.notify_shutdown();
                        state.poller_was_shutdown = true;
                        continue;
                    }
                    res = poll => {
                        return res.map(|res| (res, state));
                    }
                }
            }
        })
    }
}

impl ValidatableTask for PollActivityTaskQueueResponse {
    type SlotKind = ActivitySlotKind;

    fn validate(&self) -> Result<(), anyhow::Error> {
        if self.task_token.is_empty() {
            return Err(anyhow!("missing task token"));
        }
        Ok(())
    }

    fn task_name() -> &'static str {
        "activity"
    }
}

pub(crate) fn new_activity_task_poller(
    poller: BoxedActPoller,
    metrics: MetricsContext,
    shutdown_token: CancellationToken,
) -> impl Stream<Item = Result<PermittedTqResp<PollActivityTaskQueueResponse>, tonic::Status>> {
    TaskPollerStream::new(
        poller,
        metrics,
        MetricsContext::act_poll_timeout,
        shutdown_token,
    )
    .into_stream()
}

impl ValidatableTask for PollNexusTaskQueueResponse {
    type SlotKind = NexusSlotKind;

    fn validate(&self) -> Result<(), anyhow::Error> {
        if self.task_token.is_empty() {
            bail!("missing task token");
        } else if self.request.is_none() {
            bail!("missing request field");
        } else if self
            .request
            .as_ref()
            .expect("just request exists")
            .variant
            .is_none()
        {
            bail!("missing request variant");
        }
        Ok(())
    }

    fn task_name() -> &'static str {
        "nexus"
    }
}

pub(crate) type NexusPollItem = Result<PermittedTqResp<PollNexusTaskQueueResponse>, tonic::Status>;
pub(crate) fn new_nexus_task_poller(
    poller: BoxedNexusPoller,
    metrics: MetricsContext,
    shutdown_token: CancellationToken,
) -> impl Stream<Item = NexusPollItem> {
    TaskPollerStream::new(
        poller,
        metrics,
        MetricsContext::nexus_poll_timeout,
        shutdown_token,
    )
    .into_stream()
}
