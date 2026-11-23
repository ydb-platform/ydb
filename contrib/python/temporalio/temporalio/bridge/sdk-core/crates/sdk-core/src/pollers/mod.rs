mod poll_buffer;

pub(crate) use poll_buffer::{
    ActivityTaskOptions, LongPollBuffer, WorkflowTaskOptions, WorkflowTaskPoller,
};
pub use temporalio_client::{Client, ClientOptions, ClientTlsOptions, RetryOptions, TlsOptions};

use crate::{
    abstractions::{OwnedMeteredSemPermit, TrackedOwnedMeteredSemPermit},
    telemetry::metrics::MetricsContext,
    worker::{ActivitySlotKind, NexusSlotKind, SlotKind, WorkflowSlotKind},
};
use anyhow::{anyhow, bail};
use futures_util::{Stream, stream};
use std::{fmt::Debug, marker::PhantomData};
use temporalio_common::protos::temporal::api::workflowservice::v1::{
    PollActivityTaskQueueResponse, PollNexusTaskQueueResponse, PollWorkflowTaskQueueResponse,
};
use tokio::select;
use tokio_util::sync::CancellationToken;

#[cfg(any(feature = "test-utilities", test))]
use futures_util::Future;
#[cfg(any(feature = "test-utilities", test))]
pub(crate) use poll_buffer::MockPermittedPollBuffer;

pub(crate) type Result<T, E = tonic::Status> = std::result::Result<T, E>;

/// A trait for things that long poll the server.
#[cfg_attr(any(feature = "test-utilities", test), mockall::automock)]
#[cfg_attr(any(feature = "test-utilities", test), allow(unused))]
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

#[cfg(any(feature = "test-utilities", test))]
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
                                    if state.poller_was_shutdown {
                                        // Server sent an empty response after we initiated
                                        // shutdown — this is the graceful shutdown signal.
                                        return None;
                                    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        abstractions::tests::fixed_size_permit_dealer, pollers::MockPermittedPollBuffer,
        test_help::mock_poller, worker::ActivitySlotKind,
    };
    use futures_util::{StreamExt, pin_mut};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    /// Verify that empty responses after shutdown are not treated as poll timeout and retried
    /// indefinitely
    #[tokio::test]
    async fn empty_response_after_shutdown_terminates_stream() {
        let poll_count = Arc::new(AtomicUsize::new(0));
        let poll_count_clone = poll_count.clone();

        let mut mock_poller = mock_poller();
        mock_poller.expect_poll().returning(move || {
            poll_count_clone.fetch_add(1, Ordering::SeqCst);
            Some(Ok(PollActivityTaskQueueResponse::default()))
        });

        let sem = Arc::new(fixed_size_permit_dealer::<ActivitySlotKind>(10));
        let shutdown_token = CancellationToken::new();

        let stream = new_activity_task_poller(
            Box::new(MockPermittedPollBuffer::new(sem, mock_poller)),
            MetricsContext::no_op(),
            shutdown_token.clone(),
        );
        pin_mut!(stream);

        shutdown_token.cancel();

        let result = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next()).await;
        assert!(
            result.is_ok(),
            "Stream should terminate promptly after shutdown, not hang"
        );
        assert!(
            result.unwrap().is_none(),
            "Stream should return None (terminated) on empty response after shutdown"
        );

        let total = poll_count.load(Ordering::SeqCst);
        assert!(
            total < 5,
            "Expected stream to terminate quickly, but poller was called {total} times"
        );
    }

    #[tokio::test]
    async fn empty_response_before_shutdown_retries() {
        let mut mock_poller = mock_poller();
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();
        mock_poller.expect_poll().returning(move || {
            let n = call_count_clone.fetch_add(1, Ordering::SeqCst);
            if n < 2 {
                Some(Ok(PollActivityTaskQueueResponse::default()))
            } else {
                None
            }
        });

        let sem = Arc::new(fixed_size_permit_dealer::<ActivitySlotKind>(10));
        let shutdown_token = CancellationToken::new();

        let stream = new_activity_task_poller(
            Box::new(MockPermittedPollBuffer::new(sem, mock_poller)),
            MetricsContext::no_op(),
            shutdown_token,
        );
        pin_mut!(stream);

        // Without shutdown, empty responses should be skipped and the stream terminates
        // only when the poller returns None.
        let result = stream.next().await;
        assert!(
            result.is_none(),
            "Stream should end when poller returns None"
        );
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }
}
