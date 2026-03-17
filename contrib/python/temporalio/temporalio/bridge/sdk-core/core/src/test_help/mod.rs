pub(crate) use temporal_sdk_core_test_utils::canned_histories;

use crate::{
    TaskToken, Worker, WorkerConfig, WorkerConfigBuilder,
    pollers::{BoxedPoller, MockManualPoller, MockPoller},
    protosext::ValidPollWFTQResponse,
    replay::TestHistoryBuilder,
    sticky_q_name_for_worker,
    worker::{
        TaskPollers,
        client::{
            LegacyQueryResult, MockWorkerClient, WorkerClient, WorkflowTaskCompletion,
            mocks::mock_worker_client,
        },
    },
};
use async_trait::async_trait;
use bimap::BiMap;
use futures_util::{FutureExt, Stream, StreamExt, future::BoxFuture, stream, stream::BoxStream};
use mockall::TimesRange;
use parking_lot::RwLock;
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
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
use temporal_sdk::interceptors::FailOnNondeterminismInterceptor;
use temporal_sdk_core_api::{
    Worker as WorkerTrait,
    errors::PollError,
    worker::{PollerBehavior, WorkerVersioningStrategy},
};
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::{WorkflowActivation, workflow_activation_job},
        workflow_commands::workflow_command,
        workflow_completion::{self, WorkflowActivationCompletion, workflow_activation_completion},
    },
    temporal::api::{
        common::v1::WorkflowExecution,
        enums::v1::WorkflowTaskFailedCause,
        failure::v1::Failure,
        protocol,
        protocol::v1::message,
        update,
        workflowservice::v1::{
            PollActivityTaskQueueResponse, PollNexusTaskQueueResponse,
            PollWorkflowTaskQueueResponse, RespondWorkflowTaskCompletedResponse,
        },
    },
    utilities::pack_any,
};
use temporal_sdk_core_test_utils::{NAMESPACE, TestWorker};
use tokio::sync::{Notify, mpsc::unbounded_channel};
use tokio_stream::wrappers::UnboundedReceiverStream;

pub(crate) const TEST_Q: &str = "q";

pub(crate) fn test_worker_cfg() -> WorkerConfigBuilder {
    let mut wcb = WorkerConfigBuilder::default();
    wcb.namespace(NAMESPACE)
        .task_queue(TEST_Q)
        .versioning_strategy(WorkerVersioningStrategy::None {
            build_id: "test_bin_id".to_string(),
        })
        .ignore_evicts_on_shutdown(true)
        // Serial polling since it makes mocking much easier.
        .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize));
    wcb
}

/// When constructing responses for mocks, indicates how a given response should be built
#[derive(derive_more::From)]
#[allow(clippy::large_enum_variant)] // Test only code, whatever.
pub(crate) enum ResponseType {
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
pub(crate) enum HashableResponseType {
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
pub(crate) fn build_fake_worker(
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

pub(crate) fn build_fake_sdk(mock_cfg: MockPollCfg) -> temporal_sdk::Worker {
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|c| {
        c.max_cached_workflows = 1;
        c.ignore_evicts_on_shutdown = false;
    });
    let core = mock_worker(mock);
    let mut worker = temporal_sdk::Worker::new_from_core(Arc::new(core), "replay_q".to_string());
    worker.set_worker_interceptor(FailOnNondeterminismInterceptor {});
    worker
}

pub(crate) fn mock_worker(mocks: MocksHolder) -> Worker {
    let sticky_q = sticky_q_name_for_worker("unit-test", &mocks.inputs.config);
    let act_poller = if mocks.inputs.config.no_remote_activities {
        None
    } else {
        mocks.inputs.act_poller
    };
    Worker::new_with_pollers(
        mocks.inputs.config,
        sticky_q,
        mocks.client,
        TaskPollers::Mocked {
            wft_stream: mocks.inputs.wft_stream,
            act_poller,
            nexus_poller: mocks
                .inputs
                .nexus_poller
                .unwrap_or_else(|| mock_poller_from_resps([])),
        },
        None,
        None,
    )
}

pub(crate) fn mock_sdk(poll_cfg: MockPollCfg) -> TestWorker {
    mock_sdk_cfg(poll_cfg, |_| {})
}
pub(crate) fn mock_sdk_cfg(
    mut poll_cfg: MockPollCfg,
    mutator: impl FnOnce(&mut WorkerConfig),
) -> TestWorker {
    poll_cfg.using_rust_sdk = true;
    let mut mock = build_mock_pollers(poll_cfg);
    mock.worker_cfg(mutator);
    let core = mock_worker(mock);
    TestWorker::new(Arc::new(core), TEST_Q.to_string())
}

pub(crate) struct FakeWfResponses {
    pub(crate) wf_id: String,
    pub(crate) hist: TestHistoryBuilder,
    pub(crate) response_batches: Vec<ResponseType>,
}

// TODO: Should be all-internal to this module
pub(crate) struct MocksHolder {
    client: Arc<dyn WorkerClient>,
    inputs: MockWorkerInputs,
    pub(crate) outstanding_task_map: Option<OutstandingWFTMap>,
}

impl MocksHolder {
    pub(crate) fn worker_cfg(&mut self, mutator: impl FnOnce(&mut WorkerConfig)) {
        mutator(&mut self.inputs.config);
    }

    pub(crate) fn set_act_poller(&mut self, poller: BoxedPoller<PollActivityTaskQueueResponse>) {
        self.inputs.act_poller = Some(poller);
    }

    /// Can be used for tests that need to avoid auto-shutdown due to running out of mock responses
    pub(crate) fn make_wft_stream_interminable(&mut self) {
        let old_stream = std::mem::replace(&mut self.inputs.wft_stream, stream::pending().boxed());
        self.inputs.wft_stream = old_stream.chain(stream::pending()).boxed();
    }
}

pub(crate) struct MockWorkerInputs {
    pub(crate) wft_stream: BoxStream<'static, Result<ValidPollWFTQResponse, tonic::Status>>,
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
            wft_stream,
            act_poller: None,
            nexus_poller: None,
            config: test_worker_cfg().build().unwrap(),
        }
    }
}

impl MocksHolder {
    pub(crate) fn from_mock_worker(
        client: impl WorkerClient + 'static,
        mock_worker: MockWorkerInputs,
    ) -> Self {
        Self {
            client: Arc::new(client),
            inputs: mock_worker,
            outstanding_task_map: None,
        }
    }

    /// Uses the provided list of tasks to create a mock poller for the `TEST_Q`
    pub(crate) fn from_client_with_activities<ACT>(
        client: impl WorkerClient + 'static,
        act_tasks: ACT,
    ) -> Self
    where
        ACT: IntoIterator<Item = QueueResponse<PollActivityTaskQueueResponse>>,
        <ACT as IntoIterator>::IntoIter: Send + 'static,
    {
        let wft_stream = stream::pending().boxed();
        let mock_act_poller = mock_poller_from_resps(act_tasks);
        let mock_worker = MockWorkerInputs {
            wft_stream,
            act_poller: Some(mock_act_poller),
            nexus_poller: None,
            config: test_worker_cfg().build().unwrap(),
        };
        Self {
            client: Arc::new(client),
            inputs: mock_worker,
            outstanding_task_map: None,
        }
    }

    /// Uses the provided task responses and delivers them as quickly as possible when polled.
    /// This is only useful to test buffering, as typically you do not want to pretend that
    /// the server is delivering WFTs super fast for the same run.
    pub(crate) fn from_wft_stream(
        client: impl WorkerClient + 'static,
        stream: impl Stream<Item = PollWorkflowTaskQueueResponse> + Send + 'static,
    ) -> Self {
        let wft_stream = stream
            .map(|r| Ok(r.try_into().expect("Mock responses must be valid work")))
            .boxed();
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
        }
    }
}

// TODO: Un-pub ideally
pub(crate) fn mock_poller_from_resps<T, I>(tasks: I) -> BoxedPoller<T>
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

pub(crate) fn mock_poller<T>() -> MockPoller<T>
where
    T: Send + Sync + 'static,
{
    let mut mock_poller = MockPoller::new();
    mock_poller.expect_shutdown_box().return_const(());
    mock_poller.expect_notify_shutdown().return_const(());
    mock_poller
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
pub(crate) fn build_multihist_mock_sg(
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
pub(crate) fn single_hist_mock_sg(
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
pub(crate) struct MockPollCfg {
    pub(crate) hists: Vec<FakeWfResponses>,
    pub(crate) enforce_correct_number_of_polls: bool,
    pub(crate) num_expected_fails: usize,
    pub(crate) num_expected_legacy_query_resps: usize,
    pub(crate) mock_client: MockWorkerClient,
    /// All calls to fail WFTs must match this predicate
    pub(crate) expect_fail_wft_matcher:
        Box<dyn Fn(&TaskToken, &WorkflowTaskFailedCause, &Option<Failure>) -> bool + Send>,
    /// All calls to legacy query responses must match this predicate
    pub(crate) expect_legacy_query_matcher:
        Box<dyn Fn(&TaskToken, &LegacyQueryResult) -> bool + Send>,
    pub(crate) completion_mock_fn: Option<Box<WFTCompletionMockFn>>,
    pub(crate) num_expected_completions: Option<TimesRange>,
    /// If being used with the Rust SDK, this is set true. It ensures pollers will not error out
    /// early with no work, since we cannot know the exact number of times polling will happen.
    /// Instead, they will just block forever.
    pub(crate) using_rust_sdk: bool,
    pub(crate) make_poll_stream_interminable: bool,
}

impl MockPollCfg {
    pub(crate) fn new(
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
        }
    }

    /// Builds a config which will hand out each WFT in the history builder one by one
    pub(crate) fn from_hist_builder(t: TestHistoryBuilder) -> Self {
        let full_hist_info = t.get_full_history_info().unwrap();
        let tasks = 1..=full_hist_info.wf_task_count();
        Self::from_resp_batches("fake_wf_id", t, tasks, mock_worker_client())
    }

    pub(crate) fn from_resps(
        t: TestHistoryBuilder,
        resps: impl IntoIterator<Item = impl Into<ResponseType>>,
    ) -> Self {
        Self::from_resp_batches("fake_wf_id", t, resps, mock_worker_client())
    }

    pub(crate) fn from_resp_batches(
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
        }
    }

    pub(crate) fn completion_asserts_from_expectations(
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
pub(crate) struct CompletionAssertsBuilder<'a> {
    dest: &'a mut Option<Box<WFTCompletionMockFn>>,
    assertions: VecDeque<Box<dyn FnOnce(&WorkflowTaskCompletion) + Send>>,
}

impl CompletionAssertsBuilder<'_> {
    pub(crate) fn then(
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
pub(crate) struct OutstandingWFTMap {
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

    pub(crate) fn release_run(&self, run_id: &str) {
        self.map.write().remove_by_left(run_id);
        self.waker.notify_waiters();
    }

    pub(crate) fn all_work_delivered(&self) -> bool {
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
pub(crate) fn build_mock_pollers(mut cfg: MockPollCfg) -> MocksHolder {
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
                    dbg!("Exiting mock WFT task because rcv half of stream was dropped");
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

    let mut mh = MocksHolder {
        client: Arc::new(cfg.mock_client),
        inputs: mock_worker,
        outstanding_task_map: Some(outstanding_wf_task_tokens),
    };
    if cfg.make_poll_stream_interminable {
        mh.make_wft_stream_interminable();
    }
    mh
}

pub(crate) struct QueueResponse<T> {
    pub(crate) resp: T,
    pub(crate) delay_until: Option<BoxFuture<'static, ()>>,
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

pub(crate) trait PollWFTRespExt {
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

pub(crate) fn hist_to_poll_resp(
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

type AsserterWithReply<'a> = (
    &'a dyn Fn(&WorkflowActivation),
    workflow_activation_completion::Status,
);

/// Determines when workflows are kept in the cache or evicted for [poll_and_reply] type tests
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum WorkflowCachingPolicy {
    /// Workflows are evicted after each workflow task completion. Note that this is *not* after
    /// each workflow activation - there are often multiple activations per workflow task.
    NonSticky,

    /// Not a real mode, but good for imitating crashes. Evict workflows after *every* reply,
    /// even if there are pending activations
    #[cfg(test)]
    AfterEveryReply,
}

/// This function accepts a list of asserts and replies to workflow activations to run against the
/// provided instance of fake core.
///
/// It handles the business of re-sending the same activation replies over again in the event
/// of eviction or workflow activation failure. Activation failures specifically are only run once,
/// since they clearly can't be returned every time we replay the workflow, or it could never
/// proceed
pub(crate) async fn poll_and_reply<'a>(
    worker: &'a Worker,
    eviction_mode: WorkflowCachingPolicy,
    expect_and_reply: &'a [AsserterWithReply<'a>],
) {
    poll_and_reply_clears_outstanding_evicts(worker, None, eviction_mode, expect_and_reply).await;
}

pub(crate) async fn poll_and_reply_clears_outstanding_evicts<'a>(
    worker: &'a Worker,
    outstanding_map: Option<OutstandingWFTMap>,
    eviction_mode: WorkflowCachingPolicy,
    expect_and_reply: &'a [AsserterWithReply<'a>],
) {
    let mut evictions = 0;
    let expected_evictions = expect_and_reply.len() - 1;
    let mut executed_failures = HashSet::new();
    let expected_fail_count = expect_and_reply
        .iter()
        .filter(|(_, reply)| !reply.is_success())
        .count();

    'outer: loop {
        let expect_iter = expect_and_reply.iter();

        for (i, interaction) in expect_iter.enumerate() {
            let (asserter, reply) = interaction;
            let complete_is_failure = !reply.is_success();
            // Only send activation failures once
            if executed_failures.contains(&i) {
                continue;
            }

            let mut res = worker.poll_workflow_activation().await.unwrap();
            if res.jobs.iter().any(|j| {
                matches!(
                    j.variant,
                    Some(workflow_activation_job::Variant::RemoveFromCache(_))
                )
            }) && res.jobs.len() > 1
            {
                panic!("Saw an activation with an eviction & other work! {res:?}");
            }
            let is_eviction = res.is_only_eviction();

            let mut do_release = false;

            if is_eviction {
                // If the job is an eviction, clear it, since in the tests we don't explicitly
                // specify evict assertions
                res.jobs.clear();
                do_release = true;
            }

            // TODO: Can remove this if?
            if !res.jobs.is_empty() {
                asserter(&res);
            }

            let reply = if res.jobs.is_empty() {
                // Just an eviction
                WorkflowActivationCompletion::empty(res.run_id.clone())
            } else {
                // Eviction plus some work, we still want to issue the reply
                WorkflowActivationCompletion {
                    run_id: res.run_id.clone(),
                    status: Some(reply.clone()),
                }
            };

            let ends_execution = reply.has_execution_ending();

            worker.complete_workflow_activation(reply).await.unwrap();

            if do_release && let Some(omap) = outstanding_map.as_ref() {
                omap.release_run(&res.run_id);
            }
            // Restart assertions from the beginning if it was an eviction (and workflow execution
            // isn't over)
            if is_eviction && !ends_execution {
                continue 'outer;
            }

            if complete_is_failure {
                executed_failures.insert(i);
            }

            match eviction_mode {
                WorkflowCachingPolicy::NonSticky => (),
                WorkflowCachingPolicy::AfterEveryReply => {
                    if evictions < expected_evictions {
                        worker.request_workflow_eviction(&res.run_id);
                        evictions += 1;
                    }
                }
            }
        }

        break;
    }

    assert_eq!(expected_fail_count, executed_failures.len());
    assert_eq!(worker.outstanding_workflow_tasks().await, 0);
}

pub(crate) fn gen_assert_and_reply(
    asserter: &dyn Fn(&WorkflowActivation),
    reply_commands: Vec<workflow_command::Variant>,
) -> AsserterWithReply<'_> {
    (
        asserter,
        workflow_completion::Success::from_variants(reply_commands).into(),
    )
}

pub(crate) fn gen_assert_and_fail(asserter: &dyn Fn(&WorkflowActivation)) -> AsserterWithReply<'_> {
    (
        asserter,
        workflow_completion::Failure {
            failure: Some(Failure {
                message: "Intentional test failure".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }
        .into(),
    )
}

/// Generate asserts for [poll_and_reply] by passing patterns to match against the job list
#[macro_export]
macro_rules! job_assert {
    ($($pat:pat),+) => {
        |res| {
            assert_matches!(
                res.jobs.as_slice(),
                [$(WorkflowActivationJob {
                    variant: Some($pat),
                }),+]
            );
        }
    };
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
pub(crate) trait WorkerExt {
    /// Initiate shutdown, drain the pollers, and wait for shutdown to complete.
    async fn drain_pollers_and_shutdown(self);
    /// Initiate shutdown, drain the *activity* poller, and wait for shutdown to complete.
    /// Takes a ref because of that one test that needs it.
    async fn drain_activity_poller_and_shutdown(&self);
}

#[async_trait]
impl WorkerExt for Worker {
    async fn drain_pollers_and_shutdown(self) {
        self.initiate_shutdown();
        tokio::join!(
            async {
                assert_matches!(
                    self.poll_activity_task().await.unwrap_err(),
                    PollError::ShutDown
                );
            },
            async {
                loop {
                    match self.poll_workflow_activation().await {
                        Err(PollError::ShutDown) => break,
                        Ok(a) if a.is_only_eviction() => {
                            self.complete_workflow_activation(WorkflowActivationCompletion::empty(
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
