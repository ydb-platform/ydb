//! Integration test helpers - available with test-utilities feature or in test mode

#![allow(missing_docs)]

pub use crate::{
    internal_flags::CoreInternalFlags,
    worker::{
        LEGACY_QUERY_ID,
        client::{LegacyQueryResult, mocks::mock_worker_client},
    },
};

use crate::{
    PollError, PollerBehavior, TaskToken, Worker, WorkerConfig, WorkerConfigBuilder,
    WorkerVersioningStrategy,
    pollers::{BoxedPoller, MockManualPoller},
    protosext::ValidPollWFTQResponse,
    replay::TestHistoryBuilder,
    sticky_q_name_for_worker,
    worker::{
        TaskPollers, WorkerTelemetry,
        client::{MockWorkerClient, WorkerClient, WorkflowTaskCompletion},
        worker_config_builder,
    },
};
use assert_matches::assert_matches;
use async_trait::async_trait;
use bimap::BiMap;
use futures_util::{FutureExt, Stream, StreamExt, future::BoxFuture, stream, stream::BoxStream};
use mockall::TimesRange;
use parking_lot::RwLock;
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fmt::Debug,
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
pub use temporalio_common::telemetry::metrics::TemporalMeter;
use temporalio_common::{
    protos::{
        coresdk::{
            workflow_activation::{WorkflowActivationJob, workflow_activation_job},
            workflow_commands::{CompleteWorkflowExecution, StartTimer},
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            common::v1::WorkflowExecution,
            enums::v1::WorkflowTaskFailedCause,
            failure::v1::Failure,
            protocol::{self, v1::message},
            update,
            workflowservice::v1::{
                DescribeNamespaceResponse, PollActivityTaskQueueResponse,
                PollNexusTaskQueueResponse, PollWorkflowTaskQueueResponse,
                RespondWorkflowTaskCompletedResponse,
            },
        },
        utilities::pack_any,
    },
    worker::WorkerTaskTypes,
};
use tokio::sync::{Notify, mpsc::unbounded_channel};
use tokio_stream::wrappers::UnboundedReceiverStream;
use uuid::Uuid;

/// Default namespace for testing
pub const NAMESPACE: &str = "default";

/// Initiate shutdown, drain the pollers (handling evictions), and wait for shutdown to complete.
pub async fn drain_pollers_and_shutdown(worker: &Worker) {
    worker.initiate_shutdown();
    tokio::join!(
        async {
            assert_matches!(
                worker.poll_activity_task().await.unwrap_err(),
                PollError::ShutDown
            );
        },
        async {
            loop {
                match worker.poll_workflow_activation().await {
                    Err(PollError::ShutDown) => break,
                    Ok(a) if a.is_only_eviction() => {
                        worker
                            .complete_workflow_activation(WorkflowActivationCompletion::empty(
                                a.run_id,
                            ))
                            .await
                            .unwrap();
                    }
                    o => panic!("Unexpected activation while draining: {o:?}"),
                }
            }
        }
    );
    worker.shutdown().await;
}

#[allow(clippy::type_complexity)]
pub fn test_worker_cfg() -> WorkerConfigBuilder<
    worker_config_builder::SetWorkflowTaskPollerBehavior<
        worker_config_builder::SetTaskTypes<
            worker_config_builder::SetIgnoreEvictsOnShutdown<
                worker_config_builder::SetVersioningStrategy<
                    worker_config_builder::SetTaskQueue<worker_config_builder::SetNamespace>,
                >,
            >,
        >,
    >,
> {
    WorkerConfig::builder()
        .namespace(NAMESPACE)
        .task_queue(Uuid::new_v4().to_string())
        .versioning_strategy(WorkerVersioningStrategy::None {
            build_id: "test_bin_id".to_string(),
        })
        .ignore_evicts_on_shutdown(true)
        .task_types(WorkerTaskTypes::all())
        // Serial polling since it makes mocking much easier.
        .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize))
}

/// When constructing responses for mocks, indicates how a given response should be built
#[derive(derive_more::From)]
#[allow(clippy::large_enum_variant)] // Test only code, whatever.
pub enum ResponseType {
    ToTaskNum(usize),
    /// Returns just the history from the WFT completed of the provided task number - 1, through to
    /// the next WFT started. Simulating the incremental history for just the provided task number
    #[from(ignore)]
    OneTask(usize),
    /// Waits until the future resolves before responding as `ToTaskNum` with the provided number
    UntilResolved(BoxFuture<'static, ()>, usize),
    /// Waits until the future resolves before responding with the provided response
    UntilResolvedRaw(BoxFuture<'static, ()>, PollWorkflowTaskQueueResponse),
    AllHistory,
    Raw(PollWorkflowTaskQueueResponse),
}

#[derive(Eq, PartialEq, Hash)]
pub enum HashableResponseType {
    ToTaskNum(usize),
    OneTask(usize),
    UntilResolved(usize),
    UntilResolvedRaw(TaskToken),
    AllHistory,
    Raw(TaskToken),
}

impl ResponseType {
    fn hashable(&self) -> HashableResponseType {
        match self {
            ResponseType::ToTaskNum(x) => HashableResponseType::ToTaskNum(*x),
            ResponseType::OneTask(x) => HashableResponseType::OneTask(*x),
            ResponseType::AllHistory => HashableResponseType::AllHistory,
            ResponseType::Raw(r) => HashableResponseType::Raw(r.task_token.clone().into()),
            ResponseType::UntilResolved(_, x) => HashableResponseType::UntilResolved(*x),
            ResponseType::UntilResolvedRaw(_, r) => {
                HashableResponseType::UntilResolvedRaw(r.task_token.clone().into())
            }
        }
    }
}

impl From<&usize> for ResponseType {
    fn from(u: &usize) -> Self {
        Self::ToTaskNum(*u)
    }
}

/// Given identifiers for a workflow/run, and a test history builder, construct an instance of
/// the a worker with a mock server client that will produce the responses as appropriate.
///
/// `response_batches` is used to control the fake [PollWorkflowTaskQueueResponse]s returned. For
/// each number in the input list, a fake response will be prepared which includes history up to the
/// workflow task with that number, as in [TestHistoryBuilder::get_history_info].
pub fn build_fake_worker(
    wf_id: &str,
    t: TestHistoryBuilder,
    response_batches: impl IntoIterator<Item = impl Into<ResponseType>>,
) -> Worker {
    let response_batches = response_batches.into_iter().map(Into::into).collect();
    let mock_holder = build_multihist_mock_sg(
        vec![FakeWfResponses {
            wf_id: wf_id.to_owned(),
            hist: t,
            response_batches,
        }],
        true,
        0,
    );
    mock_worker(mock_holder)
}

pub fn mock_worker(mocks: MocksHolder) -> Worker {
    let sticky_q = sticky_q_name_for_worker("unit-test", mocks.inputs.config.max_cached_workflows);
    let act_poller = if mocks.inputs.config.task_types.enable_remote_activities {
        mocks.inputs.act_poller
    } else {
        None
    };
    let nexus_poller = if mocks.inputs.config.task_types.enable_nexus {
        mocks.inputs.nexus_poller
    } else {
        None
    };
    Worker::new_with_pollers(
        mocks.inputs.config,
        sticky_q,
        mocks.client,
        TaskPollers::Mocked {
            wft_stream: mocks.inputs.wft_stream,
            act_poller,
            nexus_poller,
        },
        mocks.worker_telemetry,
        None,
        false,
    )
    .unwrap()
}

pub struct FakeWfResponses {
    pub wf_id: String,
    pub hist: TestHistoryBuilder,
    pub response_batches: Vec<ResponseType>,
}

// TODO: Should be all-internal to this module
pub struct MocksHolder {
    client: Arc<dyn WorkerClient>,
    inputs: MockWorkerInputs,
    pub outstanding_task_map: Option<OutstandingWFTMap>,
    worker_telemetry: Option<WorkerTelemetry>,
}

impl MocksHolder {
    pub fn worker_cfg(&mut self, mutator: impl FnOnce(&mut WorkerConfig)) {
        mutator(&mut self.inputs.config);
    }

    pub(crate) fn set_act_poller(&mut self, poller: BoxedPoller<PollActivityTaskQueueResponse>) {
        self.inputs.act_poller = Some(poller);
    }

    /// Helper to create and set an activity poller from responses
    #[cfg(test)]
    pub(crate) fn set_act_poller_from_resps<ACT>(&mut self, act_tasks: ACT)
    where
        ACT: IntoIterator<Item = QueueResponse<PollActivityTaskQueueResponse>>,
        <ACT as IntoIterator>::IntoIter: Send + 'static,
    {
        let act_poller = mock_poller_from_resps(act_tasks);
        self.set_act_poller(act_poller);
    }

    /// Helper to create and set a nexus poller from responses
    #[cfg(test)]
    pub(crate) fn set_nexus_poller_from_resps<NEX>(&mut self, nexus_tasks: NEX)
    where
        NEX: IntoIterator<Item = QueueResponse<PollNexusTaskQueueResponse>>,
        <NEX as IntoIterator>::IntoIter: Send + 'static,
    {
        let nexus_poller = mock_poller_from_resps(nexus_tasks);
        self.inputs.nexus_poller = Some(nexus_poller);
    }

    pub fn set_temporal_meter(&mut self, meter: TemporalMeter) {
        self.worker_telemetry = Some(WorkerTelemetry::from_meter(meter));
    }

    /// Can be used for tests that need to avoid auto-shutdown due to running out of mock responses
    pub fn make_wft_stream_interminable(&mut self) {
        if let Some(old_stream) = self.inputs.wft_stream.take() {
            self.inputs.wft_stream = Some(old_stream.chain(stream::pending()).boxed());
        }
    }
}

pub struct MockWorkerInputs {
    pub(crate) wft_stream: Option<BoxStream<'static, Result<ValidPollWFTQResponse, tonic::Status>>>,
    pub(crate) act_poller: Option<BoxedPoller<PollActivityTaskQueueResponse>>,
    pub(crate) nexus_poller: Option<BoxedPoller<PollNexusTaskQueueResponse>>,
    pub(crate) config: WorkerConfig,
}

impl Default for MockWorkerInputs {
    fn default() -> Self {
        Self::new(stream::pending().boxed())
    }
}

impl MockWorkerInputs {
    pub(crate) fn new(
        wft_stream: BoxStream<'static, Result<ValidPollWFTQResponse, tonic::Status>>,
    ) -> Self {
        Self {
            wft_stream: Some(wft_stream),
            act_poller: None,
            nexus_poller: None,
            config: test_worker_cfg().build().unwrap(),
        }
    }
}

impl MocksHolder {
    pub fn from_mock_worker(
        client: impl WorkerClient + 'static,
        mock_worker: MockWorkerInputs,
    ) -> Self {
        Self {
            client: Arc::new(client),
            inputs: mock_worker,
            outstanding_task_map: None,
            worker_telemetry: None,
        }
    }

    /// Uses the provided list of tasks to create a mock poller with a randomly generated task queue
    pub fn from_client_with_activities<ACT>(
        client: impl WorkerClient + 'static,
        act_tasks: ACT,
    ) -> Self
    where
        ACT: IntoIterator<Item = QueueResponse<PollActivityTaskQueueResponse>>,
        <ACT as IntoIterator>::IntoIter: Send + 'static,
    {
        let mock_act_poller = mock_poller_from_resps(act_tasks);
        let mock_worker = MockWorkerInputs {
            wft_stream: None,
            act_poller: Some(mock_act_poller),
            nexus_poller: None,
            config: test_worker_cfg().build().unwrap(),
        };
        Self {
            client: Arc::new(client),
            inputs: mock_worker,
            outstanding_task_map: None,
            worker_telemetry: None,
        }
    }

    /// Uses the provided list of tasks to create a mock poller with a randomly generated task queue
    pub fn from_client_with_nexus<NEX>(
        client: impl WorkerClient + 'static,
        nexus_tasks: NEX,
    ) -> Self
    where
        NEX: IntoIterator<Item = QueueResponse<PollNexusTaskQueueResponse>>,
        <NEX as IntoIterator>::IntoIter: Send + 'static,
    {
        let mock_nexus_poller = mock_poller_from_resps(nexus_tasks);
        let mock_worker = MockWorkerInputs {
            wft_stream: None,
            act_poller: None,
            nexus_poller: Some(mock_nexus_poller),
            config: test_worker_cfg().build().unwrap(),
        };
        Self {
            client: Arc::new(client),
            inputs: mock_worker,
            outstanding_task_map: None,
            worker_telemetry: None,
        }
    }

    /// Create a MocksHolder with custom combination of task pollers.
    /// Allows any combination of workflow, activity, and nexus tasks.
    pub fn from_client_with_custom<WFT, ACT, NEX>(
        client: impl WorkerClient + 'static,
        wft_stream: Option<WFT>,
        activity_tasks: Option<ACT>,
        nexus_tasks: Option<NEX>,
    ) -> Self
    where
        WFT: Stream<Item = PollWorkflowTaskQueueResponse> + Send + 'static,
        ACT: IntoIterator<Item = QueueResponse<PollActivityTaskQueueResponse>>,
        <ACT as IntoIterator>::IntoIter: Send + 'static,
        NEX: IntoIterator<Item = QueueResponse<PollNexusTaskQueueResponse>>,
        <NEX as IntoIterator>::IntoIter: Send + 'static,
    {
        let wft_stream = wft_stream.map(|s| {
            s.map(|r| Ok(r.try_into().expect("Mock responses must be valid work")))
                .boxed()
        });

        let act_poller = activity_tasks.map(|tasks| mock_poller_from_resps(tasks));
        let nexus_poller = nexus_tasks.map(|tasks| mock_poller_from_resps(tasks));

        let mock_worker = MockWorkerInputs {
            wft_stream,
            act_poller,
            nexus_poller,
            config: test_worker_cfg().build().unwrap(),
        };

        Self {
            client: Arc::new(client),
            inputs: mock_worker,
            outstanding_task_map: None,
            worker_telemetry: None,
        }
    }

    /// Uses the provided task responses and delivers them as quickly as possible when polled.
    /// This is only useful to test buffering, as typically you do not want to pretend that
    /// the server is delivering WFTs super fast for the same run.
    pub fn from_wft_stream(
        client: impl WorkerClient + 'static,
        stream: impl Stream<Item = PollWorkflowTaskQueueResponse> + Send + 'static,
    ) -> Self {
        let wft_stream = Some(
            stream
                .map(|r| Ok(r.try_into().expect("Mock responses must be valid work")))
                .boxed(),
        );
        let mock_worker = MockWorkerInputs {
            wft_stream,
            act_poller: None,
            nexus_poller: None,
            config: test_worker_cfg().build().unwrap(),
        };
        Self {
            client: Arc::new(client),
            inputs: mock_worker,
            outstanding_task_map: None,
            worker_telemetry: None,
        }
    }
}

fn mock_poller_from_resps<T, I>(tasks: I) -> BoxedPoller<T>
where
    T: Send + Sync + 'static,
    I: IntoIterator<Item = QueueResponse<T>>,
    <I as IntoIterator>::IntoIter: Send + 'static,
{
    let mut mock_poller = mock_manual_poller();
    let mut tasks = tasks.into_iter();
    mock_poller.expect_poll().returning(move || {
        if let Some(t) = tasks.next() {
            async move {
                if let Some(f) = t.delay_until {
                    f.await;
                }
                Some(Ok(t.resp))
            }
            .boxed()
        } else {
            async { None }.boxed()
        }
    });
    Box::new(mock_poller) as BoxedPoller<T>
}

pub(crate) fn mock_manual_poller<T>() -> MockManualPoller<T>
where
    T: Send + Sync + 'static,
{
    let mut mock_poller = MockManualPoller::new();
    mock_poller
        .expect_shutdown_box()
        .returning(|| async {}.boxed());
    mock_poller.expect_notify_shutdown().return_const(());
    mock_poller
}

/// Build a mock server client capable of returning multiple different histories for different
/// workflows. It does so by tracking outstanding workflow tasks like is also happening in core
/// (which is unfortunately a bit redundant, we could provide hooks in core but that feels a little
/// nasty). If there is an outstanding task for a given workflow, new chunks of its history are not
/// returned. If there is not, the next batch of history is returned for any workflow without an
/// outstanding task. Outstanding tasks are cleared on completion, failure, or eviction.
///
/// `num_expected_fails` can be provided to set a specific number of expected failed workflow tasks
/// sent to the server.
pub fn build_multihist_mock_sg(
    hists: impl IntoIterator<Item = FakeWfResponses>,
    enforce_correct_number_of_polls: bool,
    num_expected_fails: usize,
) -> MocksHolder {
    let mh = MockPollCfg::new(
        hists.into_iter().collect(),
        enforce_correct_number_of_polls,
        num_expected_fails,
    );
    build_mock_pollers(mh)
}

/// See [build_multihist_mock_sg] -- one history convenience version
pub fn single_hist_mock_sg(
    wf_id: &str,
    t: TestHistoryBuilder,
    response_batches: impl IntoIterator<Item = impl Into<ResponseType>>,
    mock_client: MockWorkerClient,
    enforce_num_polls: bool,
) -> MocksHolder {
    let mut mh = MockPollCfg::from_resp_batches(wf_id, t, response_batches, mock_client);
    mh.enforce_correct_number_of_polls = enforce_num_polls;
    build_mock_pollers(mh)
}

type WFTCompletionMockFn = dyn FnMut(&WorkflowTaskCompletion) -> Result<RespondWorkflowTaskCompletedResponse, tonic::Status>
    + Send;

#[allow(clippy::type_complexity)]
pub struct MockPollCfg {
    pub hists: Vec<FakeWfResponses>,
    pub enforce_correct_number_of_polls: bool,
    pub num_expected_fails: usize,
    pub num_expected_legacy_query_resps: usize,
    pub mock_client: MockWorkerClient,
    /// All calls to fail WFTs must match this predicate
    pub expect_fail_wft_matcher:
        Box<dyn Fn(&TaskToken, &WorkflowTaskFailedCause, &Option<Failure>) -> bool + Send>,
    /// All calls to legacy query responses must match this predicate
    pub expect_legacy_query_matcher: Box<dyn Fn(&TaskToken, &LegacyQueryResult) -> bool + Send>,
    pub completion_mock_fn: Option<Box<WFTCompletionMockFn>>,
    pub num_expected_completions: Option<TimesRange>,
    /// If being used with the Rust SDK, this is set true. It ensures pollers will not error out
    /// early with no work, since we cannot know the exact number of times polling will happen.
    /// Instead, they will just block forever.
    pub using_rust_sdk: bool,
    pub make_poll_stream_interminable: bool,
    /// If set, will create a mock activity poller with the provided responeses. If unset, no poller
    /// is created.
    pub activity_responses: Option<Vec<QueueResponse<PollActivityTaskQueueResponse>>>,
}

impl MockPollCfg {
    pub fn new(
        hists: Vec<FakeWfResponses>,
        enforce_correct_number_of_polls: bool,
        num_expected_fails: usize,
    ) -> Self {
        Self {
            hists,
            enforce_correct_number_of_polls,
            num_expected_fails,
            num_expected_legacy_query_resps: 0,
            mock_client: mock_worker_client(),
            expect_fail_wft_matcher: Box::new(|_, _, _| true),
            expect_legacy_query_matcher: Box::new(|_, _| true),
            completion_mock_fn: None,
            num_expected_completions: None,
            using_rust_sdk: false,
            make_poll_stream_interminable: false,
            activity_responses: None,
        }
    }

    /// Builds a config which will hand out each WFT in the history builder one by one
    pub fn from_hist_builder(t: TestHistoryBuilder) -> Self {
        let full_hist_info = t.get_full_history_info().unwrap();
        let tasks = 1..=full_hist_info.wf_task_count();
        Self::from_resp_batches("fake_wf_id", t, tasks, mock_worker_client())
    }

    pub fn from_resps(
        t: TestHistoryBuilder,
        resps: impl IntoIterator<Item = impl Into<ResponseType>>,
    ) -> Self {
        Self::from_resp_batches("fake_wf_id", t, resps, mock_worker_client())
    }

    pub fn from_resp_batches(
        wf_id: &str,
        t: TestHistoryBuilder,
        resps: impl IntoIterator<Item = impl Into<ResponseType>>,
        mock_client: MockWorkerClient,
    ) -> Self {
        Self {
            hists: vec![FakeWfResponses {
                wf_id: wf_id.to_owned(),
                hist: t,
                response_batches: resps.into_iter().map(Into::into).collect(),
            }],
            enforce_correct_number_of_polls: true,
            num_expected_fails: 0,
            num_expected_legacy_query_resps: 0,
            mock_client,
            expect_fail_wft_matcher: Box::new(|_, _, _| true),
            expect_legacy_query_matcher: Box::new(|_, _| true),
            completion_mock_fn: None,
            num_expected_completions: None,
            using_rust_sdk: false,
            make_poll_stream_interminable: false,
            activity_responses: None,
        }
    }

    pub fn completion_asserts_from_expectations(
        &mut self,
        builder_fn: impl FnOnce(CompletionAssertsBuilder<'_>),
    ) {
        let builder = CompletionAssertsBuilder {
            dest: &mut self.completion_mock_fn,
            assertions: Default::default(),
        };
        builder_fn(builder);
    }
}

#[allow(clippy::type_complexity)]
pub struct CompletionAssertsBuilder<'a> {
    dest: &'a mut Option<Box<WFTCompletionMockFn>>,
    assertions: VecDeque<Box<dyn FnOnce(&WorkflowTaskCompletion) + Send>>,
}

impl CompletionAssertsBuilder<'_> {
    pub fn then(
        &mut self,
        assert: impl FnOnce(&WorkflowTaskCompletion) + Send + 'static,
    ) -> &mut Self {
        self.assertions.push_back(Box::new(assert));
        self
    }
}

impl Drop for CompletionAssertsBuilder<'_> {
    fn drop(&mut self) {
        let mut asserts = std::mem::take(&mut self.assertions);
        *self.dest = Some(Box::new(move |wtc| {
            if let Some(fun) = asserts.pop_front() {
                fun(wtc);
            }
            Ok(Default::default())
        }));
    }
}

#[derive(Default, Clone)]
pub struct OutstandingWFTMap {
    map: Arc<RwLock<BiMap<String, TaskToken>>>,
    waker: Arc<Notify>,
    all_work_delivered: Arc<AtomicBool>,
}

impl OutstandingWFTMap {
    fn has_run(&self, run_id: &str) -> bool {
        self.map.read().contains_left(run_id)
    }

    fn put_token(&self, run_id: String, token: TaskToken) {
        self.map.write().insert(run_id, token);
    }

    fn release_token(&self, token: &TaskToken) {
        self.map.write().remove_by_right(token);
        self.waker.notify_one();
    }

    pub fn release_run(&self, run_id: &str) {
        self.map.write().remove_by_left(run_id);
        self.waker.notify_waiters();
    }

    pub fn all_work_delivered(&self) -> bool {
        self.all_work_delivered.load(Ordering::Acquire)
    }
}

struct EnsuresWorkDoneWFTStream {
    inner: UnboundedReceiverStream<ValidPollWFTQResponse>,
    all_work_was_completed: Arc<AtomicBool>,
}
impl Stream for EnsuresWorkDoneWFTStream {
    type Item = ValidPollWFTQResponse;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}
impl Drop for EnsuresWorkDoneWFTStream {
    fn drop(&mut self) {
        if !self.all_work_was_completed.load(Ordering::Acquire) && !std::thread::panicking() {
            panic!("Not all workflow tasks were taken from mock!");
        }
    }
}

/// Given an iterable of fake responses, return the mocks & associated data to work with them
pub fn build_mock_pollers(mut cfg: MockPollCfg) -> MocksHolder {
    let mut task_q_resps: BTreeMap<String, VecDeque<_>> = BTreeMap::new();
    let all_work_delivered = if cfg.enforce_correct_number_of_polls && !cfg.using_rust_sdk {
        Arc::new(AtomicBool::new(false))
    } else {
        Arc::new(AtomicBool::new(true))
    };

    let outstanding_wf_task_tokens = OutstandingWFTMap {
        map: Arc::new(Default::default()),
        waker: Arc::new(Default::default()),
        all_work_delivered: all_work_delivered.clone(),
    };

    for hist in cfg.hists {
        let full_hist_info = hist.hist.get_full_history_info().unwrap();
        // Ensure no response batch is trying to return more tasks than the history contains
        for respt in &hist.response_batches {
            if let ResponseType::ToTaskNum(rb_wf_num) = respt {
                assert!(
                    *rb_wf_num <= full_hist_info.wf_task_count(),
                    "Wf task count {} is not <= total task count {}",
                    rb_wf_num,
                    full_hist_info.wf_task_count()
                );
            }
        }

        // Convert history batches into poll responses, while also tracking how many times a given
        // history has been returned so we can increment the associated attempt number on the WFT.
        // NOTE: This is hard to use properly with the `AfterEveryReply` testing eviction mode.
        //  Such usages need a history different from other eviction modes which would include
        //  WFT timeouts or something to simulate the task getting dropped.
        let mut attempts_at_task_num = HashMap::new();
        let responses: Vec<_> = hist
            .response_batches
            .into_iter()
            .map(|response| {
                let cur_attempt = attempts_at_task_num.entry(response.hashable()).or_insert(1);
                let mut r = hist_to_poll_resp(&hist.hist, hist.wf_id.clone(), response);
                r.attempt = *cur_attempt;
                *cur_attempt += 1;
                r
            })
            .collect();

        let tasks = VecDeque::from(responses);
        task_q_resps.insert(hist.wf_id, tasks);
    }

    // The poller will return history from any workflow runs that do not have currently
    // outstanding tasks.
    let outstanding = outstanding_wf_task_tokens.clone();
    let outstanding_wakeup = outstanding.waker.clone();
    let (wft_tx, wft_rx) = unbounded_channel();
    tokio::task::spawn(async move {
        loop {
            let mut resp = None;
            let mut resp_iter = task_q_resps.iter_mut();
            for (_, tasks) in &mut resp_iter {
                // Must extract run id from a workflow task associated with this workflow
                // TODO: Case where run id changes for same workflow id is not handled here
                if let Some(t) = tasks.front() {
                    let rid = t.workflow_execution.as_ref().unwrap().run_id.clone();
                    if !outstanding.has_run(&rid) {
                        let t = tasks.pop_front().unwrap();
                        outstanding.put_token(rid, TaskToken(t.task_token.clone()));
                        resp = Some(t);
                        break;
                    }
                }
            }
            let no_tasks_for_anyone = resp_iter.next().is_none();

            if let Some(resp) = resp {
                if let Some(d) = resp.delay_until {
                    d.await;
                }
                if wft_tx
                    .send(
                        resp.resp
                            .try_into()
                            .expect("Mock responses must be valid work"),
                    )
                    .is_err()
                {
                    break;
                }
            }

            // No more work to do
            if task_q_resps.values().all(|q| q.is_empty()) {
                outstanding
                    .all_work_delivered
                    .store(true, Ordering::Release);
                break;
            }

            if no_tasks_for_anyone {
                tokio::select! {
                    _ = outstanding_wakeup.notified() => {}
                    _ = tokio::time::sleep(Duration::from_secs(60)) => {}
                }
            }
        }
    });
    let mock_worker = MockWorkerInputs::new(
        EnsuresWorkDoneWFTStream {
            inner: UnboundedReceiverStream::new(wft_rx),
            all_work_was_completed: all_work_delivered,
        }
        .map(Ok)
        .boxed(),
    );

    let outstanding = outstanding_wf_task_tokens.clone();
    let expect_completes = cfg.mock_client.expect_complete_workflow_task();
    if let Some(range) = cfg.num_expected_completions {
        expect_completes.times(range);
    } else if cfg.completion_mock_fn.is_some() {
        expect_completes.times(1..);
    }
    expect_completes.returning(move |comp| {
        let r = if let Some(ass) = cfg.completion_mock_fn.as_mut() {
            // tee hee
            ass(&comp)
        } else {
            Ok(RespondWorkflowTaskCompletedResponse::default())
        };
        outstanding.release_token(&comp.task_token);
        r
    });
    let outstanding = outstanding_wf_task_tokens.clone();
    cfg.mock_client
        .expect_fail_workflow_task()
        .withf(cfg.expect_fail_wft_matcher)
        .times::<TimesRange>(cfg.num_expected_fails.into())
        .returning(move |tt, _, _| {
            outstanding.release_token(&tt);
            Ok(Default::default())
        });
    let outstanding = outstanding_wf_task_tokens.clone();
    cfg.mock_client
        .expect_respond_legacy_query()
        .withf(cfg.expect_legacy_query_matcher)
        .times::<TimesRange>(cfg.num_expected_legacy_query_resps.into())
        .returning(move |tt, _| {
            outstanding.release_token(&tt);
            Ok(Default::default())
        });

    // Fallback so worker validation (which describes the namespace to discover capabilities) works
    // for mock-based tests that don't care about capabilities. Added last and with an open call
    // count, so any test-provided `describe_namespace` expectation is matched first (mockall checks
    // expectations FIFO) and tests that never validate don't trip an unsatisfied expectation.
    cfg.mock_client
        .expect_describe_namespace()
        .times(0..)
        .returning(|| Ok(DescribeNamespaceResponse::default()));

    let mut mh = MocksHolder {
        client: Arc::new(cfg.mock_client),
        inputs: mock_worker,
        outstanding_task_map: Some(outstanding_wf_task_tokens),
        worker_telemetry: None,
    };
    if let Some(activity_responses) = cfg.activity_responses {
        let act_poller = mock_poller_from_resps(activity_responses);
        mh.set_act_poller(act_poller);
    }
    if cfg.make_poll_stream_interminable {
        mh.make_wft_stream_interminable();
    }
    mh
}

pub struct QueueResponse<T> {
    pub resp: T,
    pub delay_until: Option<BoxFuture<'static, ()>>,
}

impl<T> From<T> for QueueResponse<T> {
    fn from(resp: T) -> Self {
        QueueResponse {
            resp,
            delay_until: None,
        }
    }
}
impl From<QueueResponse<PollWorkflowTaskQueueResponse>> for ResponseType {
    fn from(qr: QueueResponse<PollWorkflowTaskQueueResponse>) -> Self {
        if let Some(du) = qr.delay_until {
            ResponseType::UntilResolvedRaw(du, qr.resp)
        } else {
            ResponseType::Raw(qr.resp)
        }
    }
}
impl<T> Deref for QueueResponse<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.resp
    }
}
impl<T> DerefMut for QueueResponse<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.resp
    }
}
impl<T> Debug for QueueResponse<T>
where
    T: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.resp.fmt(f)
    }
}

pub trait PollWFTRespExt {
    /// Add an update request to the poll response, using the update name "update_fn" and no args.
    /// Returns the inner request body.
    fn add_update_request(
        &mut self,
        update_id: impl ToString,
        after_event_id: i64,
    ) -> update::v1::Request;
}

impl PollWFTRespExt for PollWorkflowTaskQueueResponse {
    fn add_update_request(
        &mut self,
        update_id: impl ToString,
        after_event_id: i64,
    ) -> update::v1::Request {
        let upd_req_body = update::v1::Request {
            meta: Some(update::v1::Meta {
                update_id: update_id.to_string(),
                identity: "agent_id".to_string(),
            }),
            input: Some(update::v1::Input {
                header: None,
                name: "update_fn".to_string(),
                args: None,
            }),
            ..Default::default()
        };
        self.messages.push(protocol::v1::Message {
            id: format!("update-{}", update_id.to_string()),
            protocol_instance_id: update_id.to_string(),
            body: Some(
                pack_any(
                    "type.googleapis.com/temporal.api.update.v1.Request".to_string(),
                    &upd_req_body,
                )
                .unwrap(),
            ),
            sequencing_id: Some(message::SequencingId::EventId(after_event_id)),
        });
        upd_req_body
    }
}

pub fn hist_to_poll_resp(
    t: &TestHistoryBuilder,
    wf_id: impl Into<String>,
    response_type: ResponseType,
) -> QueueResponse<PollWorkflowTaskQueueResponse> {
    let run_id = t.get_orig_run_id();
    let wf = WorkflowExecution {
        workflow_id: wf_id.into(),
        run_id: run_id.to_string(),
    };
    let mut delay_until = None;
    let hist_info = match response_type {
        ResponseType::ToTaskNum(tn) => t.get_history_info(tn).unwrap(),
        ResponseType::OneTask(tn) => t.get_one_wft(tn).unwrap(),
        ResponseType::AllHistory => t.get_full_history_info().unwrap(),
        ResponseType::Raw(r) => {
            return QueueResponse {
                resp: r,
                delay_until: None,
            };
        }
        ResponseType::UntilResolved(fut, tn) => {
            delay_until = Some(fut);
            t.get_history_info(tn).unwrap()
        }
        ResponseType::UntilResolvedRaw(fut, r) => {
            return QueueResponse {
                resp: r,
                delay_until: Some(fut),
            };
        }
    };
    let mut resp = hist_info.as_poll_wft_response();
    resp.workflow_execution = Some(wf);
    QueueResponse { resp, delay_until }
}

/// Forcibly drive a future a number of times, enforcing it is always returning Pending. This is
/// useful for ensuring some future has proceeded "enough" before racing it against another future.
#[macro_export]
macro_rules! advance_fut {
    ($fut:ident) => {
        ::futures_util::pin_mut!($fut);
        {
            let waker = ::futures_util::task::noop_waker();
            let mut cx = core::task::Context::from_waker(&waker);
            for _ in 0..10 {
                assert_matches!($fut.poll_unpin(&mut cx), core::task::Poll::Pending);
                ::tokio::task::yield_now().await;
            }
        }
    };
}

/// Helps easily construct prost proto durations from stdlib duration constructors
#[macro_export]
macro_rules! prost_dur {
    ($dur_call:ident $args:tt) => {
        std::time::Duration::$dur_call$args
            .try_into()
            .expect("test duration fits")
    };
}

#[async_trait]
pub trait WorkerExt {
    /// Initiate shutdown, drain the pollers, and wait for shutdown to complete.
    async fn drain_pollers_and_shutdown(self);
    /// Initiate shutdown, drain the *activity* poller, and wait for shutdown to complete.
    /// Takes a ref because of that one test that needs it.
    async fn drain_activity_poller_and_shutdown(&self);
}

#[async_trait]
impl WorkerExt for Worker {
    async fn drain_pollers_and_shutdown(self) {
        drain_pollers_and_shutdown(&self).await;
        self.finalize_shutdown().await;
    }

    async fn drain_activity_poller_and_shutdown(&self) {
        self.initiate_shutdown();
        assert_matches!(
            self.poll_activity_task().await.unwrap_err(),
            PollError::ShutDown
        );
        self.shutdown().await;
    }
}

#[async_trait::async_trait]
/// Test helper methods for core workers
pub trait WorkerTestHelpers {
    /// Complete a workflow execution
    async fn complete_execution(&self, run_id: &str);
    /// Complete a timer with the given sequence number and duration
    async fn complete_timer(&self, run_id: &str, seq: u32, duration: Duration);
    /// Handle workflow eviction from cache
    async fn handle_eviction(&self);
}

#[async_trait::async_trait]
impl WorkerTestHelpers for Worker {
    async fn complete_execution(&self, run_id: &str) {
        self.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.to_string(),
            vec![CompleteWorkflowExecution { result: None }.into()],
        ))
        .await
        .unwrap();
    }

    async fn complete_timer(&self, run_id: &str, seq: u32, duration: Duration) {
        self.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.to_string(),
            vec![
                StartTimer {
                    seq,
                    start_to_fire_timeout: Some(duration.try_into().expect("duration fits")),
                }
                .into(),
            ],
        ))
        .await
        .unwrap();
    }

    async fn handle_eviction(&self) {
        let task = self.poll_workflow_activation().await.unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
            }]
        );
        self.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
            .await
            .unwrap();
    }
}
