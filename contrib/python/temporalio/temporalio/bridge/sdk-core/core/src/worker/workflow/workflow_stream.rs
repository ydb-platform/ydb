use crate::{
    MetricsContext,
    abstractions::dbg_panic,
    worker::workflow::{
        managed_run::RunUpdateAct,
        run_cache::RunCache,
        wft_extraction::{HistfetchRC, HistoryFetchReq, WFTExtractorOutput},
        *,
    },
};
use futures_util::{Stream, StreamExt, stream, stream::PollNext};
use std::{collections::VecDeque, fmt::Debug, future, sync::Arc};
use temporal_sdk_core_api::errors::PollError;
use temporal_sdk_core_protos::coresdk::workflow_activation::remove_from_cache::EvictionReason;
use tokio_util::sync::CancellationToken;
use tracing::{Level, Span};

/// This struct holds all the state needed for tracking the state of currently cached workflow runs
/// and directs all actions which affect them. It is ultimately the top-level arbiter of nearly
/// everything important relating to workflow state.
///
/// See [WFStream::build] for more
pub(super) struct WFStream {
    runs: RunCache,
    /// Buffered polls for new runs which need a cache slot to open up before we can handle them.
    /// The inner list is possibly multiple buffered tasks for one run, which can happen if there
    /// is a backlog of queries.
    buffered_polls_need_cache_slot: VecDeque<Vec<PermittedWFT>>,
    /// Is filled with runs that we decided need to have their history fetched during state
    /// manipulation. Must be drained after handling each input.
    runs_needing_fetching: VecDeque<HistoryFetchReq>,

    history_fetch_refcounter: Arc<HistfetchRC>,
    shutdown_token: CancellationToken,
    ignore_evicts_on_shutdown: bool,

    metrics: MetricsContext,
}
impl WFStream {
    /// Constructs workflow state management and returns a stream which outputs activations.
    ///
    /// * `wft_stream` is a stream of validated poll responses and fetched history pages as returned
    ///   by a poller (or mock), via [WFTExtractor].
    /// * `local_rx` is a stream of actions that workflow state needs to see. Things like
    ///   completions, local activities finishing, etc. See [LocalInputs].
    /// * `local_activity_request_sink` is used to handle outgoing requests to start or cancel
    ///   local activities, and may return resolutions that need to be handled immediately.
    ///
    /// The stream inputs are combined into a stream of [WFActStreamInput]s. The stream processor
    /// then takes action on those inputs, mutating the [WFStream] state, and then may yield
    /// activations.
    ///
    /// Importantly, nothing async happens while actually mutating state. This means all changes to
    /// all workflow state can be represented purely via the stream of inputs, plus the
    /// calls/retvals from the LA request sink, which is the last unfortunate bit of impurity in
    /// the design. Eliminating it would be nice, so that all inputs come from the passed-in streams
    /// and all outputs flow from the return stream, but it's difficult to do so since it would
    /// require "pausing" in-progress changes to a run while sending & waiting for response from
    /// local activity management. Likely the best option would be to move the pure state info
    /// needed to determine immediate responses into LA state machines themselves (out of the LA
    /// manager), which is a quite substantial change.
    pub(super) fn build(
        basics: WorkflowBasics,
        wft_stream: impl Stream<Item = Result<WFTExtractorOutput, tonic::Status>> + Send + 'static,
        local_rx: impl Stream<Item = LocalInput> + Send + 'static,
        local_activity_request_sink: impl LocalActivityRequestSink,
    ) -> impl Stream<Item = Result<WFStreamOutput, PollError>> {
        let all_inputs = stream::select_with_strategy(
            local_rx.map(Into::into),
            wft_stream
                .map(Into::into)
                .chain(stream::once(async { ExternalPollerInputs::PollerDead }))
                .map(Into::into)
                .boxed(),
            // Priority always goes to the local stream
            |_: &mut ()| PollNext::Left,
        );
        Self::build_internal(all_inputs, basics, local_activity_request_sink)
    }

    fn build_internal(
        all_inputs: impl Stream<Item = WFStreamInput>,
        basics: WorkflowBasics,
        local_activity_request_sink: impl LocalActivityRequestSink,
    ) -> impl Stream<Item = Result<WFStreamOutput, PollError>> {
        let mut state = WFStream {
            buffered_polls_need_cache_slot: Default::default(),
            runs: RunCache::new(
                basics.worker_config.clone(),
                (basics.sdk_name.clone(), basics.sdk_version.clone()),
                basics.server_capabilities,
                local_activity_request_sink,
                basics.metrics.clone(),
            ),
            shutdown_token: basics.shutdown_token,
            ignore_evicts_on_shutdown: basics.worker_config.ignore_evicts_on_shutdown,
            metrics: basics.metrics,
            runs_needing_fetching: Default::default(),
            history_fetch_refcounter: Arc::new(HistfetchRC {}),
        };
        all_inputs
            .map(move |action: WFStreamInput| {
                let span = span!(Level::DEBUG, "new_stream_input", action=?action);
                let _span_g = span.enter();

                let mut activations = vec![];
                let maybe_act = match action {
                    WFStreamInput::NewWft(pwft) => {
                        debug!(run_id=%pwft.work.execution.run_id, "New WFT");
                        state.instantiate_or_update(*pwft)
                    }
                    WFStreamInput::Local(local_input) => {
                        let _span_g = local_input.span.enter();
                        if let Some(rid) = local_input.input.run_id()
                            && let Some(rh) = state.runs.get_mut(rid)
                        {
                            rh.record_span_fields(&local_input.span);
                        }
                        match local_input.input {
                            LocalInputs::Completion(completion) => {
                                activations.extend(state.process_completion(
                                    NewOrFetchedComplete::New(Box::new(completion)),
                                ));
                                None // completions can return more than one activation
                            }
                            LocalInputs::FetchedPageCompletion { paginator, update } => {
                                activations.extend(state.process_completion(
                                    NewOrFetchedComplete::Fetched(update, Box::new(paginator)),
                                ));
                                None // completions can return more than one activation
                            }
                            LocalInputs::PostActivation(report) => {
                                state.process_post_activation(*report)
                            }
                            LocalInputs::LocalResolution(res) => state.local_resolution(res),
                            LocalInputs::HeartbeatTimeout(hbt) => {
                                state.process_heartbeat_timeout(hbt)
                            }
                            LocalInputs::RequestEviction(evict) => {
                                state.request_eviction(evict).into_run_update_resp()
                            }
                            LocalInputs::GetStateInfo(gsi) => {
                                let _ = gsi.response_tx.send(WorkflowStateInfo {
                                    cached_workflows: state.runs.len(),
                                    outstanding_wft: state.outstanding_wfts(),
                                });
                                None
                            }
                        }
                    }
                    WFStreamInput::FailedFetch {
                        run_id,
                        err,
                        auto_reply_fail_tt,
                    } => state
                        .request_eviction(RequestEvictMsg {
                            run_id,
                            message: format!("Fetching history failed: {err:?}"),
                            reason: EvictionReason::PaginationOrHistoryFetch,
                            auto_reply_fail_tt,
                        })
                        .into_run_update_resp(),
                    WFStreamInput::PollerDead => {
                        debug!("WFT poller died, beginning shutdown");
                        state.shutdown_token.cancel();
                        None
                    }
                    WFStreamInput::PollerError(e) => {
                        return Err(PollError::TonicError(e));
                    }
                };

                activations.extend(maybe_act);
                activations.extend(state.reconcile_buffered());

                if state.shutdown_done() {
                    info!("Workflow shutdown is done");
                    return Err(PollError::ShutDown);
                }

                Ok(WFStreamOutput {
                    activations: activations.into(),
                    fetch_histories: std::mem::take(&mut state.runs_needing_fetching),
                })
            })
            .inspect(|o| {
                if let Some(e) = o.as_ref().err()
                    && !matches!(e, PollError::ShutDown)
                {
                    error!(
                        "Workflow processing encountered fatal error and must shut down {:?}",
                        e
                    );
                }
            })
            // Stop the stream once we have shut down
            .take_while(|o| future::ready(!matches!(o, Err(PollError::ShutDown))))
    }

    /// Instantiate or update run machines with a new WFT
    #[instrument(skip(self, pwft)
                 fields(run_id=%pwft.work.execution.run_id,
                        workflow_id=%pwft.work.execution.workflow_id))]
    fn instantiate_or_update(&mut self, pwft: PermittedWFT) -> RunUpdateAct {
        match self._instantiate_or_update(pwft) {
            Err(histfetch) => {
                self.runs_needing_fetching.push_back(histfetch);
                Default::default()
            }
            Ok(r) => r,
        }
    }

    fn _instantiate_or_update(
        &mut self,
        pwft: PermittedWFT,
    ) -> Result<RunUpdateAct, HistoryFetchReq> {
        // If the run already exists, possibly buffer the work and return early if we can't handle
        // it yet.
        let pwft = if let Some(rh) = self.runs.get_mut(&pwft.work.execution.run_id) {
            if let Some(w) = rh.buffer_wft_if_outstanding_work(pwft) {
                w
            } else {
                return Ok(None);
            }
        } else {
            pwft
        };

        let run_id = pwft.work.execution.run_id.clone();
        // If our cache is full and this WFT is for an unseen run we must first evict a run before
        // we can deal with this task. So, buffer the task in that case.
        if !self.runs.has_run(&run_id) && self.runs.is_full() {
            self.buffer_resp_on_full_cache(pwft);
            return Ok(None);
        }

        // This check can't really be lifted up higher since we could EX: See it's in the cache,
        // not fetch more history, send the task, see cache is full, buffer it, then evict that
        // run, and now we still have a cache miss.
        if !self.runs.has_run(&run_id) && pwft.work.is_incremental() {
            debug!(run_id=?run_id, "Workflow task has partial history, but workflow is not in \
                   cache. Will fetch history");
            self.metrics.sticky_cache_miss();
            return Err(HistoryFetchReq::Full(
                Box::new(CacheMissFetchReq { original_wft: pwft }),
                self.history_fetch_refcounter.clone(),
            ));
        }

        let rur = self.runs.instantiate_or_update(pwft);
        Ok(rur)
    }

    fn process_completion(&mut self, complete: NewOrFetchedComplete) -> Vec<ActivationOrAuto> {
        let rh = if let Some(rh) = self.runs.get_mut(complete.run_id()) {
            rh
        } else {
            dbg_panic!("Run missing during completion {:?}", complete);
            return vec![];
        };
        let mut acts: Vec<_> = match complete {
            NewOrFetchedComplete::New(complete) => match complete.completion {
                ValidatedCompletion::Success {
                    commands,
                    used_flags,
                    versioning_behavior,
                    ..
                } => match rh.successful_completion(
                    commands,
                    used_flags,
                    versioning_behavior,
                    complete.response_tx,
                    false,
                ) {
                    Ok(acts) => acts,
                    Err(npr) => {
                        self.runs_needing_fetching
                            .push_back(HistoryFetchReq::NextPage(
                                npr,
                                self.history_fetch_refcounter.clone(),
                            ));
                        None
                    }
                },
                ValidatedCompletion::Fail {
                    failure,
                    is_autocomplete,
                    ..
                } => rh.failed_completion(
                    failure.force_cause(),
                    if is_autocomplete {
                        EvictionReason::Unspecified
                    } else {
                        EvictionReason::LangFail
                    },
                    failure,
                    is_autocomplete,
                    complete.response_tx,
                ),
            },
            NewOrFetchedComplete::Fetched(update, paginator) => {
                rh.fetched_page_completion(update, *paginator)
            }
        }
        .into_iter()
        .collect();
        // Always queue evictions after completion when we have a zero-size cache
        if self.runs.cache_capacity() == 0 {
            acts.extend(self.request_eviction_of_lru_run().into_run_update_resp())
        }
        acts
    }

    fn process_post_activation(&mut self, report: PostActivationMsg) -> RunUpdateAct {
        let run_id = &report.run_id;
        let wft_from_complete = report.wft_from_complete;
        if let Some(WFTWithPaginator { wft, .. }) = &wft_from_complete {
            debug!(run_id=%wft.execution.run_id, "New WFT from completion");
            if &wft.execution.run_id != run_id {
                dbg_panic!(
                    "Server returned a WFT on completion for a different run ({}) than the \
                     one being completed ({}). This is a server bug.",
                    wft.execution.run_id,
                    run_id
                );
            }
        }

        let mut res = None;

        let maybe_t = self.complete_wft(run_id, report.wft_report_status);
        // Augment the WFT from complete with the permit if both exist
        let wft_from_complete = wft_from_complete.and_then(|wft| {
            maybe_t.map(|t| PermittedWFT {
                work: wft.wft,
                paginator: wft.paginator,
                permit: t.permit,
            })
        });
        // Delete the activation, but only if the report came from lang, or we know the outstanding
        // activation is expected to be completed internally.
        if let Some((should_evict, mut maybe_buffered)) = self.runs.get_mut(run_id).map(|rh| {
            rh.finish_activation(|act| {
                !report.is_autocomplete || matches!(act, OutstandingActivation::Autocomplete)
            })
        }) {
            if should_evict {
                debug!(run_id=%run_id, "Evicting run");
                self.runs.remove(run_id);
            }
            let maybe_ready_wft = maybe_buffered
                .get_next_wft()
                .or(wft_from_complete)
                .map(|x| vec![x])
                .or_else(|| {
                    // Attempt to apply a buffered poll for some *other* run, if we didn't have a
                    // wft from complete or a buffered poll for *this* run and we evicted
                    if should_evict {
                        self.buffered_polls_need_cache_slot.pop_front()
                    } else {
                        None
                    }
                });
            let mut maybe_wfts = maybe_ready_wft.unwrap_or_default();
            if let Some(first_wft) = maybe_wfts.pop() {
                res = self.instantiate_or_update(first_wft);
                // We accept that there might be query tasks remaining in the buffer if we evicted
                // and re-instantiated here. It's likely those tasks are now invalidated anyway.
                if maybe_buffered.has_tasks() && should_evict {
                    warn!("There were leftover buffered tasks when evicting run");
                }
            }
            // If there happened to be more than one buffered WFT for this run, move them into the
            // now-instantiated run's buffer.
            for wft in maybe_wfts {
                let should_be_nothing = self.instantiate_or_update(wft);
                if should_be_nothing.is_some() {
                    dbg_panic!("Extra buffered run should not have produced an activation");
                }
            }
        }

        if res.is_none()
            && let Some(rh) = self.runs.get_mut(run_id)
        {
            // Attempt to produce the next activation if needed
            res = rh.check_more_activations();
            // If there's no more work and we reported workflow completion to server, evict.
            if res.is_none()
                && rh.workflow_is_finished()
                && matches!(report.wft_report_status, WFTReportStatus::Reported { .. })
            {
                res = rh
                    .request_eviction(RequestEvictMsg {
                        run_id: run_id.to_string(),
                        message: "Workflow completed".to_string(),
                        reason: EvictionReason::WorkflowExecutionEnding,
                        auto_reply_fail_tt: None,
                    })
                    .into_run_update_resp()
            }
        }
        res
    }

    fn local_resolution(&mut self, msg: LocalResolutionMsg) -> RunUpdateAct {
        let run_id = msg.run_id;
        if let Some(rh) = self.runs.get_mut(&run_id) {
            rh.local_resolution(msg.res)
        } else {
            // It isn't an explicit error if the machine is missing when a local activity resolves.
            // This can happen if an activity reports a timeout after we stopped caring about it.
            debug!(run_id = %run_id,
                   "Tried to resolve a local activity for a run we are no longer tracking");
            None
        }
    }

    fn process_heartbeat_timeout(&mut self, run_id: String) -> RunUpdateAct {
        if let Some(rh) = self.runs.get_mut(&run_id) {
            rh.heartbeat_timeout()
        } else {
            None
        }
    }

    /// Request a workflow eviction. This will (eventually, after replay is done) queue up an
    /// activation to evict the workflow from the lang side. Workflow will not *actually* be evicted
    /// until lang replies to that activation
    fn request_eviction(&mut self, info: RequestEvictMsg) -> EvictionRequestResult {
        if let Some(rh) = self.runs.get_mut(&info.run_id) {
            rh.request_eviction(info)
        } else {
            debug!(run_id=%info.run_id, "Eviction requested for unknown run");
            EvictionRequestResult::NotFound
        }
    }

    fn request_eviction_of_lru_run(&mut self) -> EvictionRequestResult {
        if let Some(lru_run_id) = self.runs.current_lru_run() {
            let run_id = lru_run_id.to_string();
            self.request_eviction(RequestEvictMsg {
                run_id,
                message: "Workflow cache full".to_string(),
                reason: EvictionReason::CacheFull,
                auto_reply_fail_tt: None,
            })
        } else {
            // This branch shouldn't really be possible
            EvictionRequestResult::NotFound
        }
    }

    fn complete_wft(
        &mut self,
        run_id: &str,
        wft_report_status: WFTReportStatus,
    ) -> Option<OutstandingTask> {
        // If the WFT completion wasn't sent to the server, but we did see the final event, we still
        // want to clear the workflow task. This can really only happen in replay testing, where we
        // will generate poll responses with complete history but no attached query, and such a WFT
        // would never really exist. The server wouldn't send a workflow task with nothing to do,
        // but they are very useful for testing complete replay.
        let saw_final = self
            .runs
            .get(run_id)
            .map(|r| r.have_seen_terminal_event())
            .unwrap_or_default();
        if !saw_final && matches!(wft_report_status, WFTReportStatus::NotReported) {
            return None;
        }

        if let Some(rh) = self.runs.get_mut(run_id) {
            // Can't mark the WFT complete if there are pending queries, as doing so would destroy
            // them.
            if rh
                .wft()
                .map(|wft| !wft.pending_queries.is_empty())
                .unwrap_or_default()
            {
                return None;
            }

            rh.mark_wft_complete(wft_report_status)
        } else {
            None
        }
    }

    fn buffer_resp_on_full_cache(&mut self, work: PermittedWFT) {
        debug!(run_id=%work.work.execution.run_id, "Buffering WFT because cache is full");
        // If there's already a buffered poll for the run, add to it.
        if let Some(rh) = self.buffered_polls_need_cache_slot.iter_mut().find(|w| {
            w.first()
                .is_some_and(|ww| ww.work.execution.run_id == work.work.execution.run_id)
        }) {
            rh.push(work);
        } else {
            //  Otherwise push it to the back
            self.buffered_polls_need_cache_slot.push_back(vec![work]);
        }
    }

    /// Makes sure we have enough pending evictions to fulfill the needs of buffered WFTs who are
    /// waiting on a cache slot
    fn reconcile_buffered(&mut self) -> Vec<ActivationOrAuto> {
        // We must ensure that there are at least as many pending evictions as there are tasks
        // that we might need to un-buffer (skipping runs which already have buffered tasks for
        // themselves)
        let num_in_buff = self.buffered_polls_need_cache_slot.len();
        let mut evict_these = vec![];
        let num_existing_evictions = self
            .runs
            .runs_lru_order()
            .filter(|(_, h)| h.trying_to_evict().is_some())
            .count();
        let mut num_evicts_needed = num_in_buff.saturating_sub(num_existing_evictions);
        for (rid, handle) in self.runs.runs_lru_order() {
            if num_evicts_needed == 0 {
                break;
            }
            if !handle.has_buffered_wft() {
                num_evicts_needed -= 1;
                evict_these.push(rid.to_string());
            }
        }
        let mut acts = vec![];
        for run_id in evict_these {
            acts.extend(
                self.request_eviction(RequestEvictMsg {
                    run_id,
                    message: "Workflow cache full".to_string(),
                    reason: EvictionReason::CacheFull,
                    auto_reply_fail_tt: None,
                })
                .into_run_update_resp(),
            );
        }
        acts
    }

    fn shutdown_done(&self) -> bool {
        if self.shutdown_token.is_cancelled() {
            if Arc::strong_count(&self.history_fetch_refcounter) > 1 {
                // Don't exit if there are outstanding fetch requests
                return false;
            }
            let all_runs_ready = self
                .runs
                .handles()
                .all(|r| !r.has_any_pending_work(self.ignore_evicts_on_shutdown, false));
            if all_runs_ready {
                return true;
            }
        }
        false
    }

    fn outstanding_wfts(&self) -> usize {
        self.runs.handles().filter(|r| r.wft().is_some()).count()
    }

    // Useful when debugging
    #[allow(dead_code)]
    fn info_dump(&self, run_id: &str) {
        if let Some(r) = self.runs.peek(run_id) {
            info!(run_id, wft=?r.wft(), activation=?r.activation(),
                  buffered_wft=r.has_buffered_wft(),
                  trying_to_evict=r.trying_to_evict().is_some(), more_work=r.more_pending_work());
        } else {
            info!(run_id, "Run not found");
        }
    }
}

/// All possible inputs to the [WFStream]
#[derive(derive_more::From, Debug)]
enum WFStreamInput {
    NewWft(Box<PermittedWFT>),
    Local(LocalInput),
    /// The stream given to us which represents the poller (or a mock) terminated.
    PollerDead,
    /// The stream given to us which represents the poller (or a mock) encountered a non-retryable
    /// error while polling
    PollerError(tonic::Status),
    FailedFetch {
        run_id: String,
        err: tonic::Status,
        auto_reply_fail_tt: Option<TaskToken>,
    },
}

/// A non-poller-received input to the [WFStream]
#[derive(derive_more::Debug)]
#[debug("LocalInput {{ {input:?} }}")]
pub(super) struct LocalInput {
    pub(super) input: LocalInputs,
    pub(super) span: Span,
}
impl From<HeartbeatTimeoutMsg> for LocalInput {
    fn from(hb: HeartbeatTimeoutMsg) -> Self {
        Self {
            input: LocalInputs::HeartbeatTimeout(hb.run_id),
            span: hb.span,
        }
    }
}
/// Everything that _isn't_ a poll which may affect workflow state. Always higher priority than
/// new polls.
#[derive(Debug, derive_more::From)]
pub(super) enum LocalInputs {
    Completion(WFActCompleteMsg),
    FetchedPageCompletion {
        paginator: HistoryPaginator,
        update: HistoryUpdate,
    },
    LocalResolution(LocalResolutionMsg),
    PostActivation(Box<PostActivationMsg>),
    RequestEviction(RequestEvictMsg),
    HeartbeatTimeout(String),
    GetStateInfo(GetStateInfoMsg),
}
impl LocalInputs {
    fn run_id(&self) -> Option<&str> {
        Some(match self {
            LocalInputs::Completion(c) => c.completion.run_id(),
            LocalInputs::FetchedPageCompletion { paginator, .. } => &paginator.run_id,
            LocalInputs::LocalResolution(lr) => &lr.run_id,
            LocalInputs::PostActivation(pa) => &pa.run_id,
            LocalInputs::RequestEviction(re) => &re.run_id,
            LocalInputs::HeartbeatTimeout(hb) => hb,
            LocalInputs::GetStateInfo(_) => return None,
        })
    }
}
#[derive(Debug)]
#[allow(clippy::large_enum_variant)] // PollerDead only ever gets used once, so not important.
enum ExternalPollerInputs {
    NewWft(PermittedWFT),
    PollerDead,
    PollerError(tonic::Status),
    FetchedUpdate(PermittedWFT),
    NextPage {
        paginator: HistoryPaginator,
        update: HistoryUpdate,
        span: Span,
    },
    FailedFetch {
        run_id: String,
        err: tonic::Status,
        auto_reply_fail_tt: Option<TaskToken>,
    },
}
impl From<ExternalPollerInputs> for WFStreamInput {
    fn from(l: ExternalPollerInputs) -> Self {
        match l {
            ExternalPollerInputs::NewWft(v) => WFStreamInput::NewWft(Box::new(v)),
            ExternalPollerInputs::PollerDead => WFStreamInput::PollerDead,
            ExternalPollerInputs::PollerError(e) => WFStreamInput::PollerError(e),
            ExternalPollerInputs::FetchedUpdate(wft) => WFStreamInput::NewWft(Box::new(wft)),
            ExternalPollerInputs::FailedFetch {
                run_id,
                err,
                auto_reply_fail_tt,
            } => WFStreamInput::FailedFetch {
                run_id,
                err,
                auto_reply_fail_tt,
            },
            ExternalPollerInputs::NextPage {
                paginator,
                update,
                span,
            } => WFStreamInput::Local(LocalInput {
                input: LocalInputs::FetchedPageCompletion { paginator, update },
                span,
            }),
        }
    }
}
impl From<Result<WFTExtractorOutput, tonic::Status>> for ExternalPollerInputs {
    fn from(v: Result<WFTExtractorOutput, tonic::Status>) -> Self {
        match v {
            Ok(WFTExtractorOutput::NewWFT(pwft)) => ExternalPollerInputs::NewWft(pwft),
            Ok(WFTExtractorOutput::FetchResult(updated_wft, _)) => {
                ExternalPollerInputs::FetchedUpdate(updated_wft)
            }
            Ok(WFTExtractorOutput::NextPage {
                paginator,
                update,
                span,
                rc: _rc,
            }) => ExternalPollerInputs::NextPage {
                paginator,
                update,
                span,
            },
            Ok(WFTExtractorOutput::FailedFetch {
                run_id,
                err,
                auto_reply_fail_tt,
            }) => ExternalPollerInputs::FailedFetch {
                run_id,
                err,
                auto_reply_fail_tt,
            },
            Ok(WFTExtractorOutput::PollerDead) => ExternalPollerInputs::PollerDead,
            Err(e) => ExternalPollerInputs::PollerError(e),
        }
    }
}
#[derive(Debug)]
enum NewOrFetchedComplete {
    New(Box<WFActCompleteMsg>),
    Fetched(HistoryUpdate, Box<HistoryPaginator>),
}
impl NewOrFetchedComplete {
    fn run_id(&self) -> &str {
        match self {
            NewOrFetchedComplete::New(c) => c.completion.run_id(),
            NewOrFetchedComplete::Fetched(_, p) => &p.run_id,
        }
    }
}
