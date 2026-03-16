//! This module implements support for creating special core instances and workers which can be used
//! to replay canned histories. It should be used by Lang SDKs to provide replay capabilities to
//! users during testing.

use crate::{
    Worker,
    worker::{
        PostActivateHookData,
        client::mocks::{MockManualWorkerClient, mock_manual_worker_client},
    },
};
use futures_util::{FutureExt, Stream, StreamExt};
use parking_lot::Mutex;
use std::{
    pin::Pin,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
};
use temporal_sdk_core_api::worker::{PollerBehavior, WorkerConfig};
pub use temporal_sdk_core_protos::{
    DEFAULT_WORKFLOW_TYPE, HistoryInfo, TestHistoryBuilder, default_wes_attribs,
};
use temporal_sdk_core_protos::{
    coresdk::workflow_activation::remove_from_cache::EvictionReason,
    temporal::api::{
        common::v1::WorkflowExecution,
        history::v1::History,
        workflowservice::v1::{
            RespondWorkflowTaskCompletedResponse, RespondWorkflowTaskFailedResponse,
        },
    },
};
use tokio::sync::{Mutex as TokioMutex, mpsc, mpsc::UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

/// Contains inputs to a replay worker
pub struct ReplayWorkerInput<I> {
    /// The worker's configuration. Some portions will be overridden to required values once the
    /// worker is instantiated.
    pub config: WorkerConfig,
    history_stream: I,
    /// If specified use this as the basis for the internal mocked client
    pub(crate) client_override: Option<MockManualWorkerClient>,
}

impl<I> ReplayWorkerInput<I>
where
    I: Stream<Item = HistoryForReplay> + Send + 'static,
{
    /// Build a replay worker
    pub fn new(config: WorkerConfig, history_stream: I) -> Self {
        Self {
            config,
            history_stream,
            client_override: None,
        }
    }

    pub(crate) fn into_core_worker(mut self) -> Result<Worker, anyhow::Error> {
        self.config.max_cached_workflows = 1;
        self.config.workflow_task_poller_behavior = PollerBehavior::SimpleMaximum(1);
        self.config.no_remote_activities = true;
        let historator = Historator::new(self.history_stream);
        let post_activate = historator.get_post_activate_hook();
        let shutdown_tok = historator.get_shutdown_setter();
        // Create a mock client which can be used by a replay worker to serve up canned histories.
        // It will return the entire history in one workflow task. If a workflow task failure is
        // sent to the mock, it will send the complete response again.
        //
        // Once it runs out of histories to return, it will serve up default responses after a 10s
        // delay
        let mut client = if let Some(c) = self.client_override {
            c
        } else {
            mock_manual_worker_client()
        };

        let hist_allow_tx = historator.replay_done_tx.clone();
        let historator = Arc::new(TokioMutex::new(historator));

        // TODO: Should use `new_with_pollers` and avoid re-doing mocking stuff
        client.expect_poll_workflow_task().returning(move |_, _| {
            let historator = historator.clone();
            async move {
                let mut hlock = historator.lock().await;
                // Always wait for permission before dispatching the next task
                let _ = hlock.allow_stream.next().await;

                if let Some(history) = hlock.next().await {
                    let hist_info = HistoryInfo::new_from_history(&history.hist, None).unwrap();
                    let mut resp = hist_info.as_poll_wft_response();
                    resp.workflow_execution = Some(WorkflowExecution {
                        workflow_id: history.workflow_id,
                        run_id: hist_info.orig_run_id().to_string(),
                    });
                    Ok(resp)
                } else {
                    if let Some(wc) = hlock.worker_closer.get() {
                        wc.cancel();
                    }
                    Ok(Default::default())
                }
            }
            .boxed()
        });

        client.expect_complete_workflow_task().returning(move |_a| {
            async move { Ok(RespondWorkflowTaskCompletedResponse::default()) }.boxed()
        });
        client
            .expect_fail_workflow_task()
            .returning(move |_, _, _| {
                hist_allow_tx.send("Failed".to_string()).unwrap();
                async move { Ok(RespondWorkflowTaskFailedResponse::default()) }.boxed()
            });
        let mut worker = Worker::new(self.config, None, Arc::new(client), None, None);
        worker.set_post_activate_hook(post_activate);
        shutdown_tok(worker.shutdown_token());
        Ok(worker)
    }
}

/// A history which will be used during replay verification. Since histories do not include the
/// workflow id, it must be manually attached.
#[derive(Debug, Clone, derive_more::Constructor)]
pub struct HistoryForReplay {
    hist: History,
    workflow_id: String,
}
impl From<TestHistoryBuilder> for HistoryForReplay {
    fn from(thb: TestHistoryBuilder) -> Self {
        thb.get_full_history_info().unwrap().into()
    }
}
impl From<HistoryInfo> for HistoryForReplay {
    fn from(histinfo: HistoryInfo) -> Self {
        HistoryForReplay::new(histinfo.into(), "fake".to_owned())
    }
}

/// Allows lang to feed histories into the replayer one at a time. Simply drop the feeder to signal
/// to the worker that you're done and it should initiate shutdown.
pub struct HistoryFeeder {
    tx: mpsc::Sender<HistoryForReplay>,
}
/// The stream half of a [HistoryFeeder]
pub struct HistoryFeederStream {
    rcvr: mpsc::Receiver<HistoryForReplay>,
}

impl HistoryFeeder {
    /// Make a new history feeder, which will store at most `buffer_size` histories before `feed`
    /// blocks.
    ///
    /// Returns a feeder which will be used to feed in histories, and a stream you can pass to
    /// one of the replay worker init functions.
    pub fn new(buffer_size: usize) -> (Self, HistoryFeederStream) {
        let (tx, rcvr) = mpsc::channel(buffer_size);
        (Self { tx }, HistoryFeederStream { rcvr })
    }
    /// Feed a new history into the replayer, blocking if there is not room to accept another
    /// history.
    pub async fn feed(&self, history: HistoryForReplay) -> anyhow::Result<()> {
        self.tx.send(history).await?;
        Ok(())
    }
}

impl Stream for HistoryFeederStream {
    type Item = HistoryForReplay;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rcvr.poll_recv(cx)
    }
}

pub(crate) struct Historator {
    iter: Pin<Box<dyn Stream<Item = HistoryForReplay> + Send>>,
    allow_stream: UnboundedReceiverStream<String>,
    worker_closer: Arc<OnceLock<CancellationToken>>,
    dat: Arc<Mutex<HistoratorDat>>,
    replay_done_tx: UnboundedSender<String>,
}
impl Historator {
    pub(crate) fn new(histories: impl Stream<Item = HistoryForReplay> + Send + 'static) -> Self {
        let dat = Arc::new(Mutex::new(HistoratorDat::default()));
        let (replay_done_tx, replay_done_rx) = mpsc::unbounded_channel();
        // Need to allow the first history item
        replay_done_tx.send("fake".to_string()).unwrap();
        Self {
            iter: Box::pin(histories.fuse()),
            allow_stream: UnboundedReceiverStream::new(replay_done_rx),
            worker_closer: Arc::new(OnceLock::new()),
            dat,
            replay_done_tx,
        }
    }

    /// Returns a callback that can be used as the post-activation hook for a worker to indicate
    /// we're ready to replay the next history, or whatever else.
    pub(crate) fn get_post_activate_hook(
        &self,
    ) -> impl Fn(&Worker, PostActivateHookData) + Send + Sync + use<> {
        let done_tx = self.replay_done_tx.clone();
        move |worker, data| {
            if !data.replaying {
                worker.request_wf_eviction(
                    data.run_id,
                    "Always evict workflows after replay",
                    EvictionReason::LangRequested,
                );
                done_tx.send(data.run_id.to_string()).unwrap();
            }
        }
    }

    pub(crate) fn get_shutdown_setter(&self) -> impl FnOnce(CancellationToken) + 'static {
        let wc = self.worker_closer.clone();
        move |ct| {
            wc.set(ct).expect("Shutdown token must only be set once");
        }
    }
}

impl Stream for Historator {
    type Item = HistoryForReplay;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.iter.poll_next_unpin(cx) {
            Poll::Ready(Some(history)) => {
                history
                    .hist
                    .extract_run_id_from_start()
                    .expect(
                        "Histories provided for replay must contain run ids in their workflow \
                                 execution started events",
                    )
                    .to_string();
                Poll::Ready(Some(history))
            }
            Poll::Ready(None) => {
                self.dat.lock().all_dispatched = true;
                Poll::Ready(None)
            }
            o => o,
        }
    }
}

#[derive(Default)]
struct HistoratorDat {
    all_dispatched: bool,
}
