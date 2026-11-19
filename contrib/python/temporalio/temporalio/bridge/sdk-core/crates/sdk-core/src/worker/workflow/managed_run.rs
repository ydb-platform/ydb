use crate::{
    MetricsContext, WorkerConfig,
    abstractions::dbg_panic,
    internal_flags::CoreInternalFlags,
    protosext::{WorkflowActivationExt, protocol_messages::IncomingProtocolMessage},
    telemetry::metrics,
    worker::{
        LEGACY_QUERY_ID, LocalActRequest, WorkflowErrorType,
        workflow::{
            ActivationAction, ActivationCompleteOutcome, ActivationCompleteResult,
            ActivationOrAuto, BufferedTasks, DrivenWorkflow, EvictionRequestResult,
            FailedActivationWFTReport, HeartbeatTimeoutMsg, HistoryUpdate,
            LocalActivityRequestSink, LocalResolution, NextPageReq, OutstandingActivation,
            OutstandingTask, PermittedWFT, RequestEvictMsg, RunBasics,
            ServerCommandsWithWorkflowInfo, TaskStorageMetrics, WFCommand, WFCommandVariant,
            WFMachinesError, WFT_HEARTBEAT_TIMEOUT_FRACTION, WFTReportStatus, WorkflowTaskInfo,
            history_update::HistoryPaginator,
            machines::{MachinesWFTResponseContent, WorkflowMachines},
        },
    },
};
use futures_util::future::AbortHandle;
use std::{
    collections::HashSet,
    mem,
    ops::{Add, Sub},
    rc::Rc,
    sync::{Arc, mpsc::Sender},
    time::{Duration, Instant},
};
use temporalio_common::protos::{
    TaskToken,
    coresdk::{
        common::ExternalStorageMetrics,
        workflow_activation::{
            WorkflowActivation, create_evict_activation, query_to_job,
            remove_from_cache::EvictionReason, workflow_activation_job,
        },
        workflow_commands::{FailWorkflowExecution, QueryResult},
        workflow_completion,
    },
    temporal::api::{
        enums::v1::{VersioningBehavior, WorkflowTaskFailedCause},
        failure::v1::Failure,
    },
};
use tokio::sync::oneshot;
use tracing::Span;

type Result<T, E = WFMachinesError> = std::result::Result<T, E>;
pub(super) type RunUpdateAct = Option<ActivationOrAuto>;

/// Manages access to a specific workflow run. Everything inside is entirely synchronous and should
/// remain that way.
#[derive(derive_more::Debug)]
#[debug(
    "ManagedRun {{ wft: {:?}, activation: {:?}, task_buffer: {:?} \
           trying_to_evict: {} }}",
    wft,
    activation,
    task_buffer,
    "trying_to_evict.is_some()"
)]
pub(super) struct ManagedRun {
    wfm: WorkflowManager,
    /// Called when the machines need to produce local activity requests. This can't be lifted up
    /// easily as return values, because sometimes local activity requests trigger immediate
    /// resolutions (ex: too many attempts). Thus lifting it up creates a lot of unneeded complexity
    /// pushing things out and then directly back in. The downside is this is the only "impure" part
    /// of the in/out nature of workflow state management. If there's ever a sensible way to lift it
    /// up, that'd be nice.
    ///
    /// This field is `None` when `WorkerTaskTypes.enable_local_activities` is false.
    local_activity_request_sink: Option<Rc<dyn LocalActivityRequestSink>>,
    /// Set if the run is currently waiting on the execution of some local activities.
    waiting_on_la: Option<WaitingOnLAs>,
    /// Is set to true if the machines encounter an error and the only subsequent thing we should
    /// do is be evicted.
    am_broken: bool,
    /// If set, the WFT this run is currently/will be processing.
    wft: Option<OutstandingTask>,
    /// An outstanding activation to lang
    activation: Option<OutstandingActivation>,
    /// Contains buffered poll responses from the server that apply to this run. This can happen
    /// when:
    ///   * Lang takes too long to complete a task and the task times out
    ///   * Many queries are submitted concurrently and reach this worker (in this case, multiple
    ///     tasks can be outstanding)
    ///   * Multiple speculative tasks (ex: for updates) may also exist at once (but only the
    ///     latest one will matter).
    task_buffer: BufferedTasks,
    /// Is set if an eviction has been requested for this run
    trying_to_evict: Option<RequestEvictMsg>,

    /// We track if we have recorded useful debugging values onto a certain span yet, to overcome
    /// duplicating field values. Remove this once https://github.com/tokio-rs/tracing/issues/2334
    /// is fixed.
    recorded_span_ids: HashSet<tracing::Id>,
    metrics: MetricsContext,
    /// We store the paginator used for our own run's history fetching
    paginator: Option<HistoryPaginator>,
    completion_waiting_on_page_fetch: Option<RunActivationCompletion>,
    config: Arc<WorkerConfig>,
}
impl ManagedRun {
    pub(super) fn new(
        basics: RunBasics,
        wft: PermittedWFT,
        local_activity_request_sink: Option<Rc<dyn LocalActivityRequestSink>>,
    ) -> (Self, RunUpdateAct) {
        let metrics = basics.metrics.clone();
        let config = basics.worker_config.clone();
        let wfm = WorkflowManager::new(basics);
        let mut me = Self {
            wfm,
            local_activity_request_sink,
            waiting_on_la: None,
            am_broken: false,
            wft: None,
            activation: None,
            task_buffer: Default::default(),
            trying_to_evict: None,
            recorded_span_ids: Default::default(),
            metrics,
            paginator: None,
            completion_waiting_on_page_fetch: None,
            config,
        };
        let rua = me.incoming_wft(wft);
        (me, rua)
    }

    /// Returns true if there are pending jobs that need to be sent to lang.
    pub(super) fn more_pending_work(&self) -> bool {
        // We don't want to consider there to be more local-only work to be done if there is
        // no workflow task associated with the run right now. This can happen if, ex, we
        // complete a local activity while waiting for server to send us the next WFT.
        // Activating lang would be harmful at this stage, as there might be work returned
        // in that next WFT which should be part of the next activation.
        self.wft.is_some() && self.wfm.machines.has_pending_jobs()
    }

    pub(super) fn waiting_on_local_activities(&self) -> bool {
        self.waiting_on_la.is_some()
    }

    pub(super) fn have_seen_terminal_event(&self) -> bool {
        self.wfm.machines.have_seen_terminal_event
    }

    pub(super) fn workflow_is_finished(&self) -> bool {
        self.wfm.machines.workflow_is_finished()
    }

    /// Returns a ref to info about the currently tracked workflow task, if any.
    pub(super) fn wft(&self) -> Option<&OutstandingTask> {
        self.wft.as_ref()
    }

    /// Returns a ref to info about the currently tracked workflow activation, if any.
    pub(super) fn activation(&self) -> Option<&OutstandingActivation> {
        self.activation.as_ref()
    }

    /// Returns this run's eviction reason if it is going to be evicted
    pub(super) fn trying_to_evict(&self) -> Option<&RequestEvictMsg> {
        self.trying_to_evict.as_ref()
    }

    /// Called whenever a new workflow task is obtained for this run
    pub(super) fn incoming_wft(&mut self, pwft: PermittedWFT) -> RunUpdateAct {
        let res = self._incoming_wft(pwft);
        self.update_to_acts(res.map(Into::into))
    }

    fn _incoming_wft(
        &mut self,
        pwft: PermittedWFT,
    ) -> Result<Option<ActivationOrAuto>, RunUpdateErr> {
        if self.wft.is_some() {
            dbg_panic!("Trying to send a new WFT for a run which already has one!");
        }
        let start_time = Instant::now();

        let work = pwft.work;
        debug!(
            task_token = %&work.task_token,
            update = ?work.update,
            has_legacy_query = %work.legacy_query.is_some(),
            messages = ?work.messages,
            attempt = %work.attempt,
            "Applying new workflow task from server"
        );
        let is_incremental = work.is_incremental();
        let wft_info = WorkflowTaskInfo {
            attempt: work.attempt,
            task_token: work.task_token,
            wf_id: work.execution.workflow_id.clone(),
        };

        let legacy_query_from_poll = work
            .legacy_query
            .map(|q| query_to_job(LEGACY_QUERY_ID.to_string(), q));

        let mut pending_queries = work.query_requests;
        if !pending_queries.is_empty() && legacy_query_from_poll.is_some() {
            error!(
                "Server issued both normal and legacy queries. This should not happen. Please \
                 file a bug report."
            );
            return Err(RunUpdateErr {
                source: WFMachinesError::Fatal(
                    "Server issued both normal and legacy query".to_string(),
                ),
                complete_resp_chan: None,
            });
        }
        let was_legacy_query = legacy_query_from_poll.is_some();
        if let Some(lq) = legacy_query_from_poll {
            pending_queries.push(lq);
        }

        self.paginator = Some(pwft.paginator);
        self.wft = Some(OutstandingTask {
            info: wft_info,
            pending_queries,
            start_time,
            permit: pwft.permit,
        });
        if let Some(waiting) = self.waiting_on_la.as_mut() {
            waiting.hb_timeout_handle.abort();
            waiting.heartbeat_timeout_pending = false;
        }

        if was_legacy_query
            && work.update.wft_started_id == 0
            && work.update.previous_wft_started_id < self.wfm.machines.get_last_wft_started_id()
        {
            return Ok(Some(ActivationOrAuto::AutoFail {
                run_id: self.run_id().to_string(),
                machines_err: WFMachinesError::Fatal("Query expired".to_string()),
            }));
        }

        // The update field is only populated in the event we hit the cache
        let activation = if work.update.is_real() {
            if is_incremental {
                self.metrics.sticky_cache_hit();
            }
            self.wfm.new_work_from_server(work.update, work.messages)?
        } else {
            let r = self.wfm.get_next_activation()?;
            if r.jobs.is_empty() {
                return Err(RunUpdateErr {
                    source: crate::worker::workflow::fatal!(
                        "Machines created for {} with no jobs",
                        self.wfm.machines.run_id
                    ),
                    complete_resp_chan: None,
                });
            }
            r
        };

        if activation.jobs.is_empty() {
            if self.wfm.machines.outstanding_local_activity_count() > 0 {
                // If the activation has no jobs but there are outstanding LAs, we need to restart
                // the WFT heartbeat.
                if let Some(ref mut lawait) = self.waiting_on_la {
                    lawait.hb_timeout_handle.abort();
                    lawait.hb_timeout_handle = sink_heartbeat_timeout_start(
                        self.wfm.machines.run_id.clone(),
                        self.local_activity_request_sink.as_deref(),
                        start_time,
                        lawait.wft_timeout,
                    );
                    // No activation needs to be sent to lang. We just need to wait for another
                    // heartbeat timeout or LAs to resolve
                    return Ok(None);
                } else {
                    panic!(
                        "Got a new WFT while there are outstanding local activities, but there \
                     was no waiting on LA info."
                    )
                }
            } else {
                return Ok(Some(ActivationOrAuto::Autocomplete {
                    run_id: self.wfm.machines.run_id.clone(),
                }));
            }
        }

        Ok(Some(ActivationOrAuto::LangActivation(activation)))
    }

    /// Deletes the currently tracked WFT & records latency metrics. Should be called after it has
    /// been responded to (server has been told). Returns the WFT if there was one.
    pub(super) fn mark_wft_complete(
        &mut self,
        report_status: WFTReportStatus,
        task_storage_metrics: &TaskStorageMetrics,
    ) -> Option<OutstandingTask> {
        debug!("Marking WFT completed");
        let retme = self.wft.take();

        if let Some(ot) = &retme
            && let Some(ct) = report_status.completion_time()
        {
            let task_duration = ct.sub(ot.start_time);
            self.metrics.wf_task_latency(task_duration);
            log_workflow_task_duration(
                &self.wfm.machines.run_id,
                &self.wfm.machines.workflow_type,
                self.wfm.machines.last_processed_event + 1,
                ot.info.attempt,
                self.wfm.machines.history_size_bytes(),
                task_duration,
                task_storage_metrics,
            );
        }

        if let WFTReportStatus::Reported {
            reset_last_started_to,
            ..
        } = report_status
        {
            if let Some(id) = reset_last_started_to {
                self.wfm.machines.reset_last_started_id(id);
            }
            // Tell the LA manager that we're done with the WFT
            if let Some(ref local_act_request_sink) = self.local_activity_request_sink {
                local_act_request_sink.sink_reqs(vec![
                    LocalActRequest::IndicateWorkflowTaskCompleted(
                        self.wfm.machines.run_id.clone(),
                    ),
                ]);
            }
        }

        retme
    }

    /// Checks if any further activations need to go out for this run and produces them if so.
    pub(super) fn check_more_activations(&mut self) -> RunUpdateAct {
        let res = self._check_more_activations();
        self.update_to_acts(res.map(Into::into))
    }

    fn _check_more_activations(&mut self) -> Result<Option<ActivationOrAuto>, RunUpdateErr> {
        // No point in checking for more activations if there's already an outstanding activation.
        if self.activation.is_some() {
            return Ok(None);
        }
        // In the event it's time to evict this run, cancel any outstanding LAs
        if self.trying_to_evict.is_some() {
            self.sink_la_requests(vec![LocalActRequest::CancelAllInRun(
                self.wfm.machines.run_id.clone(),
            )])?;
        }

        if self.wft.is_none() {
            // It doesn't make sense to do workflow work unless we have a WFT
            return Ok(None);
        }

        if self.wfm.machines.has_pending_jobs() && !self.am_broken {
            Ok(Some(ActivationOrAuto::LangActivation(
                self.wfm.get_next_activation()?,
            )))
        } else if self.waiting_on_la.is_some()
            && self.wfm.machines.outstanding_local_activity_count() == 0
        {
            self.waiting_on_la
                .take()
                .expect("waiting_on_la was just checked")
                .hb_timeout_handle
                .abort();
            Ok(Some(ActivationOrAuto::Autocomplete {
                run_id: self.run_id().to_string(),
            }))
        } else {
            if !self.am_broken {
                let has_pending_queries = self
                    .wft
                    .as_ref()
                    .map(|wft| !wft.pending_queries.is_empty())
                    .unwrap_or_default();
                if has_pending_queries {
                    return Ok(Some(ActivationOrAuto::ReadyForQueries(
                        self.wfm.machines.get_wf_activation(),
                    )));
                }
            }
            if self
                .waiting_on_la
                .as_ref()
                .is_some_and(|waiting| waiting.heartbeat_timeout_pending)
            {
                Ok(Some(ActivationOrAuto::Autocomplete {
                    run_id: self.run_id().to_string(),
                }))
            } else if let Some(wte) = self.trying_to_evict.clone() {
                let act =
                    create_evict_activation(self.run_id().to_string(), wte.message, wte.reason);
                Ok(Some(ActivationOrAuto::LangActivation(act)))
            } else {
                Ok(None)
            }
        }
    }

    /// Called whenever lang successfully completes a workflow activation. Commands produced by the
    /// activation are passed in. `resp_chan` will be used to unblock the completion call when
    /// everything we need to do to fulfill it has happened.
    ///
    /// Can return an error in the event that another page of history needs to be fetched before
    /// the completion can proceed.
    pub(super) fn successful_completion(
        &mut self,
        mut commands: Vec<WFCommand>,
        used_flags: Vec<u32>,
        versioning_behavior: VersioningBehavior,
        resp_chan: Option<oneshot::Sender<ActivationCompleteResult>>,
        is_forced_failure: bool,
    ) -> Result<RunUpdateAct, Box<NextPageReq>> {
        let activation_was_only_eviction = self.activation_is_eviction();
        let (task_token, has_pending_query, start_time) = if let Some(entry) = self.wft.as_ref() {
            (
                entry.info.task_token.clone(),
                !entry.pending_queries.is_empty(),
                entry.start_time,
            )
        } else {
            if !activation_was_only_eviction {
                // Not an error if this was an eviction, since it's normal to issue eviction
                // activations without an associated workflow task in that case.
                dbg_panic!(
                    "Attempted to complete activation for run {} without associated workflow task",
                    self.run_id()
                );
            }
            let outcome = if let Some((tt, reason)) = self.trying_to_evict.as_mut().and_then(|te| {
                te.auto_reply_fail_tt
                    .take()
                    .map(|tt| (tt, te.message.clone()))
            }) {
                ActivationCompleteOutcome::ReportWFTFail(FailedActivationWFTReport::Report(
                    tt,
                    WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
                    Failure::application_failure(reason, true).into(),
                ))
            } else {
                ActivationCompleteOutcome::DoNothing
            };
            self.reply_to_complete(outcome, resp_chan);
            return Ok(None);
        };

        // If the only command from the activation is a legacy query response, that means we need
        // to respond differently than a typical activation.
        if matches!(&commands.as_slice(),
                    &[WFCommand {variant: WFCommandVariant::QueryResponse(qr), ..}]
                        if qr.query_id == LEGACY_QUERY_ID)
        {
            let qr = match commands.remove(0) {
                WFCommand {
                    variant: WFCommandVariant::QueryResponse(qr),
                    ..
                } => qr,
                _ => unreachable!("We just verified this is the only command"),
            };
            self.reply_to_complete(
                ActivationCompleteOutcome::ReportWFTSuccess(ServerCommandsWithWorkflowInfo {
                    task_token,
                    action: ActivationAction::RespondLegacyQuery {
                        result: Box::new(qr),
                    },
                    metrics: self.metrics.clone(),
                }),
                resp_chan,
            );
            Ok(None)
        } else {
            let (commands, query_responses) = self.preprocess_command_sequence(commands);

            if activation_was_only_eviction && !commands.is_empty() {
                dbg_panic!("Reply to an eviction included commands");
            }

            let rac = RunActivationCompletion {
                task_token,
                start_time,
                commands,
                activation_was_eviction: self.activation_is_eviction(),
                has_pending_query,
                query_responses,
                used_flags,
                resp_chan,
                is_forced_failure,
                versioning_behavior,
            };

            // Verify we can actually apply the next workflow task, which will happen as part of
            // applying the completion to machines. If we can't, return early indicating we need
            // to fetch a page.
            if !self.wfm.ready_to_apply_next_wft() {
                return if let Some(paginator) = self.paginator.take() {
                    debug!("Need to fetch a history page before next WFT can be applied");
                    self.completion_waiting_on_page_fetch = Some(rac);
                    Err(Box::new(NextPageReq {
                        paginator,
                        span: Span::current(),
                    }))
                } else {
                    Ok(self.update_to_acts(Err(RunUpdateErr {
                        source: WFMachinesError::Fatal(
                            "Run's paginator was absent when attempting to fetch next history \
                                page. This is a Core SDK bug."
                                .to_string(),
                        ),
                        complete_resp_chan: rac.resp_chan,
                    })))
                };
            }

            Ok(self.process_completion(rac))
        }
    }

    /// Core has received from lang a sequence containing all commands generated
    /// by all workflow coroutines. Return a command sequence containing all
    /// non-terminal (i.e. non-workflow-terminating) commands, followed by the
    /// first terminal command if there are any. Also strip out and return query
    /// results (these don't affect machines and are handled separately
    /// downstream)
    ///
    /// The reordering is done in order that all non-terminal commands generated
    /// by workflow coroutines are given a chance for the server to honor them.
    /// For example, in order to deliver an update result to a client as the
    /// workflow completes.
    ///
    /// Behavior here has changed backwards-incompatibly, so a flag is set if
    /// the outcome differs from what the outcome would have been previously.
    /// See also CoreInternalFlags::MoveTerminalCommands docstring and
    /// https://github.com/temporalio/features/issues/481.
    fn preprocess_command_sequence(
        &mut self,
        commands: Vec<WFCommand>,
    ) -> (Vec<WFCommand>, Vec<QueryResult>) {
        if self.wfm.machines.replaying
            && !self
                .wfm
                .machines
                .try_use_flag(CoreInternalFlags::MoveTerminalCommands, false)
        {
            preprocess_command_sequence_old_behavior(commands)
        } else {
            preprocess_command_sequence(commands)
        }
    }

    /// Called after the higher-up machinery has fetched more pages of event history needed to apply
    /// the next workflow task. The history update and paginator used to perform the fetch are
    /// passed in, with the update being used to apply the task, and the paginator stored to be
    /// attached with another fetch request if needed.
    pub(super) fn fetched_page_completion(
        &mut self,
        update: HistoryUpdate,
        paginator: HistoryPaginator,
    ) -> RunUpdateAct {
        let res = self._fetched_page_completion(update, paginator);
        self.update_to_acts(res.map(Into::into))
    }
    fn _fetched_page_completion(
        &mut self,
        update: HistoryUpdate,
        paginator: HistoryPaginator,
    ) -> Result<Option<FulfillableActivationComplete>, RunUpdateErr> {
        self.paginator = Some(paginator);
        if let Some(d) = self.completion_waiting_on_page_fetch.take() {
            self._process_completion(d, Some(update))
        } else {
            dbg_panic!(
                "Shouldn't be possible to be applying a next-page-fetch update when \
                        doing anything other than completing an activation."
            );
            Err(RunUpdateErr::from(WFMachinesError::Fatal(
                "Tried to apply next-page-fetch update to a run that wasn't handling a completion"
                    .to_string(),
            )))
        }
    }

    /// Called whenever either core lang cannot complete a workflow activation. EX: Nondeterminism
    /// or user code threw/panicked. The `cause` and `reason` fields are determined inside core
    /// always. The `failure` field may come from lang. `resp_chan` will be used to unblock the
    /// completion call when everything we need to do to fulfill it has happened.
    pub(super) fn failed_completion(
        &mut self,
        cause: WorkflowTaskFailedCause,
        reason: EvictionReason,
        failure: workflow_completion::Failure,
        is_auto_fail: bool,
        resp_chan: Option<oneshot::Sender<ActivationCompleteResult>>,
    ) -> RunUpdateAct {
        let tt = if let Some(tt) = self.wft.as_ref().map(|t| t.info.task_token.clone()) {
            tt
        } else {
            dbg_panic!(
                "No workflow task for run id {} found when trying to fail activation",
                self.run_id()
            );
            self.reply_to_complete(ActivationCompleteOutcome::DoNothing, resp_chan);
            return None;
        };

        let message = format!("Workflow activation completion failed: {:?}", &failure);
        // We don't want to fail queries that could otherwise be retried
        let is_no_report_query_fail = self.pending_work_is_legacy_query()
            && is_auto_fail
            && matches!(
                reason,
                EvictionReason::Unspecified | EvictionReason::PaginationOrHistoryFetch
            );

        let (should_report, rur) = if is_no_report_query_fail {
            (false, None)
        } else {
            // Blow up any cached data associated with the workflow
            let evict_req_outcome = self.request_eviction(RequestEvictMsg {
                run_id: self.run_id().to_string(),
                message,
                reason,
                auto_reply_fail_tt: None,
            });
            let should_report = match &evict_req_outcome {
                EvictionRequestResult::EvictionRequested(Some(attempt), _)
                | EvictionRequestResult::EvictionAlreadyRequested(Some(attempt)) => *attempt <= 1,
                _ => false,
            };
            let rur = evict_req_outcome.into_run_update_resp();
            (should_report, rur)
        };

        let outcome = if self.pending_work_is_legacy_query() {
            if is_no_report_query_fail {
                ActivationCompleteOutcome::WFTFailedDontReport
            } else {
                ActivationCompleteOutcome::ReportWFTFail(
                    FailedActivationWFTReport::ReportLegacyQueryFailure(tt, failure),
                )
            }
        } else if should_report {
            // Check if we should fail the workflow instead of the WFT because of user's preferences
            if matches!(cause, WorkflowTaskFailedCause::NonDeterministicError)
                && self.config.should_fail_workflow(
                    &self.wfm.machines.workflow_type,
                    &WorkflowErrorType::Nondeterminism,
                )
            {
                warn!(failure=?failure, "Failing workflow due to nondeterminism error");
                return self
                    .successful_completion(
                        vec![WFCommand::new(WFCommandVariant::FailWorkflow(
                            FailWorkflowExecution {
                                failure: failure.failure,
                            },
                        ))],
                        vec![],
                        VersioningBehavior::Unspecified, // Doesn't matter since we're failing wf
                        resp_chan,
                        true,
                    )
                    .unwrap_or_else(|e| {
                        dbg_panic!("Got next page request when auto-failing workflow: {e:?}");
                        None
                    });
            } else {
                ActivationCompleteOutcome::ReportWFTFail(FailedActivationWFTReport::Report(
                    tt, cause, failure,
                ))
            }
        } else {
            ActivationCompleteOutcome::WFTFailedDontReport
        };

        self.metrics
            .with_new_attrs([metrics::failure_reason(cause.into())])
            .wf_task_failed();
        self.reply_to_complete(outcome, resp_chan);
        rur
    }

    /// Must be called after the processing of the activation completion and WFT reporting.
    ///
    /// It will delete the currently tracked workflow activation (if there is one) and `pred`
    /// evaluates to true. In the event the activation was an eviction, the bool part of the return
    /// tuple is true. The [BufferedTasks] part will contain any buffered tasks that may still exist
    /// and need to be instantiated into a new instance of the run, if a `wft_from_complete` was
    /// provided, it will supersede any real WFTs in the buffer as by definition those are now
    /// out-of-date.
    pub(super) fn finish_activation(
        &mut self,
        pred: impl FnOnce(&OutstandingActivation) -> bool,
    ) -> (bool, BufferedTasks) {
        let evict = if self.activation().map(pred).unwrap_or_default() {
            let act = self.activation.take();
            act.map(|a| matches!(a, OutstandingActivation::Eviction))
                .unwrap_or_default()
        } else {
            false
        };
        if evict && let Some(sink) = self.local_activity_request_sink.as_deref() {
            let immediate_resolutions = sink.sink_reqs(vec![LocalActRequest::InvalidateRun(
                self.wfm.machines.run_id.clone(),
            )]);
            if !immediate_resolutions.is_empty() {
                dbg_panic!("Invalidating local activities should not produce resolutions");
            }
        }
        let buffered = if evict {
            mem::take(&mut self.task_buffer)
        } else {
            Default::default()
        };
        (evict, buffered)
    }

    /// Called when local activities resolve
    pub(super) fn local_resolution(&mut self, res: LocalResolution) -> RunUpdateAct {
        let res = self._local_resolution(res);
        self.update_to_acts(res.map(Into::into))
    }

    fn process_completion(&mut self, completion: RunActivationCompletion) -> RunUpdateAct {
        let res = self._process_completion(completion, None);
        self.update_to_acts(res.map(Into::into))
    }

    fn _process_completion(
        &mut self,
        completion: RunActivationCompletion,
        update_from_new_page: Option<HistoryUpdate>,
    ) -> Result<Option<FulfillableActivationComplete>, RunUpdateErr> {
        let completing_heartbeat_autocomplete =
            matches!(self.activation, Some(OutstandingActivation::Autocomplete))
                && self.waiting_on_la.is_some();
        let completing_la_heartbeat = completing_heartbeat_autocomplete
            || self
                .waiting_on_la
                .as_ref()
                .is_some_and(|waiting| waiting.heartbeat_timeout_pending);
        let data = CompletionDataForWFT {
            task_token: completion.task_token,
            query_responses: completion.query_responses,
            has_pending_query: completion.has_pending_query,
            activation_was_eviction: completion.activation_was_eviction,
            is_forced_failure: completion.is_forced_failure,
            versioning_behavior: completion.versioning_behavior,
        };

        self.wfm.machines.add_lang_used_flags(completion.used_flags);

        // If this is just bookkeeping after a reply to an eviction activation, we can bypass
        // everything, since there is no reason to continue trying to update machines.
        if completion.activation_was_eviction {
            return Ok(Some(self.prepare_complete_resp(
                completion.resp_chan,
                data,
                false,
            )));
        }

        let outcome = (|| {
            // Send commands from lang into the machines then check if the workflow run needs
            // another activation and mark it if so
            self.wfm.push_commands_and_iterate(completion.commands)?;
            if let Some(update) = update_from_new_page {
                self.wfm.feed_history_from_new_page(update)?;
            }
            // Don't bother applying the next task if we're evicting at the end of this activation
            // or are otherwise broken.
            if !completion.activation_was_eviction && !self.am_broken {
                self.wfm.apply_next_task_if_ready()?;
            }
            let new_local_acts = self.wfm.drain_queued_local_activities();
            self.sink_la_requests(new_local_acts)?;

            if self.wfm.machines.outstanding_local_activity_count() == 0 {
                Ok(None)
            } else {
                let wft_timeout: Duration = self
                    .wfm
                    .machines
                    .get_started_info()
                    .and_then(|attrs| attrs.workflow_task_timeout)
                    .ok_or_else(|| {
                        WFMachinesError::Fatal(
                            "Workflow's start attribs were missing a well formed task timeout"
                                .to_string(),
                        )
                    })?;
                Ok(Some((completion.start_time, wft_timeout)))
            }
        })();

        match outcome {
            Ok(None) => {
                if let Some(waiting) = self.waiting_on_la.take() {
                    waiting.hb_timeout_handle.abort();
                }
                Ok(Some(self.prepare_complete_resp(
                    completion.resp_chan,
                    data,
                    completing_heartbeat_autocomplete,
                )))
            }
            Ok(Some((start_t, wft_timeout))) => {
                if let Some(wola) = self.waiting_on_la.as_mut() {
                    wola.hb_timeout_handle.abort();
                }
                if completing_la_heartbeat || !data.query_responses.is_empty() {
                    // Reporting a query while an LA is still running must request another WFT;
                    // otherwise the LA could resolve without a task on which to deliver its job.
                    let hb_timeout_handle = sink_heartbeat_timeout_start(
                        self.run_id().to_string(),
                        self.local_activity_request_sink.as_deref(),
                        start_t,
                        wft_timeout,
                    );
                    hb_timeout_handle.abort();
                    self.waiting_on_la = Some(WaitingOnLAs {
                        wft_timeout,
                        hb_timeout_handle,
                        // Keep this set until the replacement WFT arrives. If pending workflow
                        // jobs prevent this completion from being reported, the heartbeat still
                        // needs to be honored after those jobs are processed.
                        heartbeat_timeout_pending: completing_la_heartbeat,
                    });
                    Ok(Some(self.prepare_complete_resp(
                        completion.resp_chan,
                        data,
                        true,
                    )))
                } else {
                    self.waiting_on_la = Some(WaitingOnLAs {
                        wft_timeout,
                        hb_timeout_handle: sink_heartbeat_timeout_start(
                            self.run_id().to_string(),
                            self.local_activity_request_sink.as_deref(),
                            start_t,
                            wft_timeout,
                        ),
                        heartbeat_timeout_pending: false,
                    });
                    Ok(Some(FulfillableActivationComplete {
                        result: ActivationCompleteResult {
                            outcome: ActivationCompleteOutcome::DoNothing,
                            replaying: self.wfm.machines.replaying,
                        },
                        resp_chan: completion.resp_chan,
                    }))
                }
            }
            Err(e) => Err(RunUpdateErr {
                source: e,
                complete_resp_chan: completion.resp_chan,
            }),
        }
    }

    fn _local_resolution(
        &mut self,
        res: LocalResolution,
    ) -> Result<Option<ActivationOrAuto>, RunUpdateErr> {
        debug!(resolution=?res, "Applying local resolution");
        self.wfm.notify_of_local_result(res)?;
        if self.activation.is_none() {
            self._check_more_activations()
        } else {
            Ok(None)
        }
    }

    pub(super) fn heartbeat_timeout(&mut self) -> RunUpdateAct {
        let maybe_act = if self._heartbeat_timeout() {
            Some(ActivationOrAuto::Autocomplete {
                run_id: self.wfm.machines.run_id.clone(),
            })
        } else {
            None
        };
        self.update_to_acts(Ok(maybe_act.into()))
    }
    /// Returns `true` if autocompletion should be issued to report the heartbeat WFT completion.
    fn _heartbeat_timeout(&mut self) -> bool {
        if let Some(ref mut wait_dat) = self.waiting_on_la {
            wait_dat.hb_timeout_handle.abort();
            wait_dat.heartbeat_timeout_pending = true;
            return self.activation.is_none();
        }
        false
    }

    /// Returns true if the managed run has any form of pending work
    /// If `ignore_evicts` is true, pending evictions do not count as pending work.
    /// If `ignore_buffered` is true, buffered workflow tasks do not count as pending work.
    pub(super) fn has_any_pending_work(&self, ignore_evicts: bool, ignore_buffered: bool) -> bool {
        let evict_work = if ignore_evicts {
            false
        } else {
            self.trying_to_evict.is_some()
        };
        let act_work = if ignore_evicts {
            self.activation
                .map(|a| !matches!(a, OutstandingActivation::Eviction))
                .unwrap_or_default()
        } else {
            self.activation.is_some()
        };
        let buffered = if ignore_buffered {
            false
        } else {
            self.task_buffer.has_tasks()
        };
        trace!(wft=self.wft.is_some(), buffered=?buffered, more_work=?self.more_pending_work(),
               act_work, evict_work, "Does run have pending work?");
        self.wft.is_some() || buffered || self.more_pending_work() || act_work || evict_work
    }

    /// Stores some work if there is any outstanding WFT or activation for the run. If there was
    /// not, returns the work back out inside the option.
    pub(super) fn buffer_wft_if_outstanding_work(
        &mut self,
        work: PermittedWFT,
    ) -> Option<PermittedWFT> {
        let about_to_issue_evict = self.trying_to_evict.is_some();
        let has_activation = self.activation().is_some();
        if has_activation || about_to_issue_evict || self.more_pending_work() {
            debug!(run_id = %self.run_id(),
                   "Got new WFT for a run with outstanding work, buffering it act: {:?} wft: {:?} about to evict: {:?}", &self.activation(), &self.wft, about_to_issue_evict);
            self.task_buffer.buffer(work);
            None
        } else {
            Some(work)
        }
    }

    /// Returns true if there is a buffered workflow task for this run.
    pub(super) fn has_buffered_wft(&self) -> bool {
        self.task_buffer.has_tasks()
    }

    pub(super) fn request_eviction(&mut self, info: RequestEvictMsg) -> EvictionRequestResult {
        let attempts = self.wft.as_ref().map(|wt| wt.info.attempt);

        // If we were waiting on a page fetch and we're getting evicted because fetching failed,
        // then make sure we allow the completion to proceed, otherwise we're stuck waiting forever.
        if self.completion_waiting_on_page_fetch.is_some()
            && matches!(info.reason, EvictionReason::PaginationOrHistoryFetch)
        {
            // We just checked it is some, unwrap OK.
            let c = self.completion_waiting_on_page_fetch.take().unwrap();
            let run_upd = self.failed_completion(
                WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
                info.reason,
                Failure::application_failure(info.message, false).into(),
                true,
                c.resp_chan,
            );
            return EvictionRequestResult::EvictionRequested(attempts, run_upd);
        }

        if !self.activation_is_eviction() && self.trying_to_evict.is_none() {
            let outstanding_las = self.wfm.machines.outstanding_local_activity_count();
            if outstanding_las > 0 && self.config.max_cached_workflows == 0 {
                warn!(
                    run_id=%info.run_id,
                    reason=?info.reason,
                    outstanding_local_activities=outstanding_las,
                    "Eviction requested while local activities are still in flight; local activities when using max_cached_workflows=0 are likely to be dropped or retried"
                );
            }
            debug!(run_id=%info.run_id, reason=%info.message, "Eviction requested");
            // If we've requested an eviction because of failure related reasons then we want to
            // delete any pending queries, since handling them no longer makes sense. Evictions
            // because the cache is full should get a chance to finish processing properly.
            if !matches!(info.reason, EvictionReason::CacheFull | EvictionReason::WorkflowExecutionEnding)
                // If the wft was just a legacy query, still reply, otherwise we might try to
                // reply to the task as if it were a task rather than a query.
                && !self.pending_work_is_legacy_query()
                && let Some(wft) = self.wft.as_mut()
            {
                wft.pending_queries.clear();
            }

            self.trying_to_evict = Some(info);
            EvictionRequestResult::EvictionRequested(attempts, self.check_more_activations())
        } else {
            // Always store the most recent eviction reason
            self.trying_to_evict = Some(info);
            EvictionRequestResult::EvictionAlreadyRequested(attempts)
        }
    }

    pub(super) fn record_span_fields(&mut self, span: &Span) {
        if let Some(spid) = span.id() {
            if self.recorded_span_ids.contains(&spid) {
                return;
            }
            self.recorded_span_ids.insert(spid);

            span.record("run_id", self.run_id());
            if let Some(wid) = self.wft().map(|wft| &wft.info.wf_id) {
                span.record("workflow_id", wid.as_str());
            }
        }
    }

    /// Take the result of some update to ourselves and turn it into a return value of zero or more
    /// actions
    fn update_to_acts(&mut self, outcome: Result<ActOrFulfill, RunUpdateErr>) -> RunUpdateAct {
        match outcome {
            Ok(act_or_fulfill) => {
                let (mut maybe_act, maybe_fulfill) = match act_or_fulfill {
                    ActOrFulfill::OutgoingAct(a) => (a, None),
                    ActOrFulfill::FulfillableComplete(c) => (None, c),
                };
                // If there's no activation but is pending work, check and possibly generate one
                if self.more_pending_work() && maybe_act.is_none() {
                    match self._check_more_activations() {
                        Ok(oa) => maybe_act = oa,
                        Err(e) => {
                            return self.update_to_acts(Err(e));
                        }
                    }
                }
                let r = match maybe_act {
                    Some(ActivationOrAuto::LangActivation(activation)) => {
                        if activation.jobs.is_empty() {
                            dbg_panic!("Should not send lang activation with no jobs");
                        }
                        Some(ActivationOrAuto::LangActivation(activation))
                    }
                    Some(ActivationOrAuto::ReadyForQueries(mut act)) => {
                        if let Some(wft) = self.wft.as_mut() {
                            put_queries_in_act(&mut act, wft);
                            Some(ActivationOrAuto::LangActivation(act))
                        } else {
                            dbg_panic!("Ready for queries but no WFT!");
                            None
                        }
                    }
                    a @ Some(
                        ActivationOrAuto::Autocomplete { .. } | ActivationOrAuto::AutoFail { .. },
                    ) => a,
                    None => {
                        if let Some(reason) = self.trying_to_evict.as_ref() {
                            // If we had nothing to do, but we're trying to evict, just do that now
                            // as long as there's no other outstanding work.
                            if self.activation.is_none() && !self.more_pending_work() {
                                let mut evict_act = create_evict_activation(
                                    self.run_id().to_string(),
                                    reason.message.clone(),
                                    reason.reason,
                                );
                                evict_act.history_length =
                                    self.most_recently_processed_event_number() as u32;
                                Some(ActivationOrAuto::LangActivation(evict_act))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                };
                if let Some(f) = maybe_fulfill {
                    f.fulfill();
                }

                match r {
                    // After each run update, check if it's ready to handle any buffered task
                    None | Some(ActivationOrAuto::Autocomplete { .. })
                        if !self.has_any_pending_work(false, true) =>
                    {
                        if let Some(bufft) = self.task_buffer.get_next_wft() {
                            self.incoming_wft(bufft)
                        } else {
                            None
                        }
                    }
                    Some(r) => {
                        self.insert_outstanding_activation(&r);
                        Some(r)
                    }
                    None => None,
                }
            }
            Err(fail) => {
                self.am_broken = true;

                if let Some(resp_chan) = fail.complete_resp_chan {
                    // Automatically fail the workflow task in the event we couldn't update machines
                    let fail_cause = if matches!(&fail.source, WFMachinesError::Nondeterminism(_)) {
                        WorkflowTaskFailedCause::NonDeterministicError
                    } else {
                        WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure
                    };
                    self.failed_completion(
                        fail_cause,
                        fail.source.evict_reason(),
                        fail.source.as_failure(),
                        true,
                        Some(resp_chan),
                    )
                } else {
                    warn!(error=?fail.source, "Error while updating workflow");
                    Some(ActivationOrAuto::AutoFail {
                        run_id: self.run_id().to_owned(),
                        machines_err: fail.source,
                    })
                }
            }
        }
    }

    fn insert_outstanding_activation(&mut self, act: &ActivationOrAuto) {
        let act_type = match &act {
            ActivationOrAuto::LangActivation(act) | ActivationOrAuto::ReadyForQueries(act) => {
                if act.is_only_eviction() {
                    OutstandingActivation::Eviction
                } else if act.is_legacy_query() {
                    OutstandingActivation::LegacyQuery
                } else {
                    OutstandingActivation::Normal
                }
            }
            ActivationOrAuto::Autocomplete { .. } | ActivationOrAuto::AutoFail { .. } => {
                OutstandingActivation::Autocomplete
            }
        };
        if let Some(old_act) = self.activation {
            // This is a panic because we have screwed up core logic if this is violated. It must be
            // upheld.
            panic!(
                "Attempted to insert a new outstanding activation {act:?}, but there already was \
                 one outstanding: {old_act:?}"
            );
        }
        self.activation = Some(act_type);
    }

    fn prepare_complete_resp(
        &mut self,
        resp_chan: Option<oneshot::Sender<ActivationCompleteResult>>,
        data: CompletionDataForWFT,
        due_to_heartbeat_timeout: bool,
    ) -> FulfillableActivationComplete {
        let mut machines_wft_response = self.wfm.prepare_for_wft_response();
        if data.activation_was_eviction
            && (machines_wft_response.commands().peek().is_some()
                || machines_wft_response.has_messages())
            && !self.am_broken
        {
            dbg_panic!(
                "There should not be any outgoing commands or messages when preparing a completion \
                 response if the activation was only an eviction. This is an SDK bug."
            );
        }

        let query_responses = data.query_responses;
        let has_query_responses = !query_responses.is_empty();
        let is_query_playback = data.has_pending_query && !has_query_responses;
        let mut force_new_wft = due_to_heartbeat_timeout;

        // We only actually want to send commands back to the server if there are no more pending
        // activations and we are caught up on replay. We don't want to complete a wft if we already
        // saw the final event in the workflow, or if we are playing back for the express purpose of
        // fulfilling a query. If the activation we sent was *only* an eviction, don't send that
        // either.
        let should_respond = !(machines_wft_response.has_pending_jobs
            || (machines_wft_response.replaying && !data.is_forced_failure)
            || is_query_playback
            || data.activation_was_eviction
            || machines_wft_response.have_seen_terminal_event);
        // If there are pending LA resolutions, and we're responding to a query here,
        // we want to make sure to force a new task, as otherwise once we tell lang about
        // the LA resolution there wouldn't be any task to reply to with the result of iterating
        // the workflow.
        if has_query_responses && machines_wft_response.have_pending_la_resolutions {
            force_new_wft = true;
        }

        let outcome = if should_respond || has_query_responses {
            // If we broke there could be commands or messages in the pipe that we didn't
            // get a chance to handle properly during replay. Don't send them.
            let (commands, messages) = if self.am_broken && data.activation_was_eviction {
                (vec![], vec![])
            } else {
                (
                    machines_wft_response.commands().collect(),
                    machines_wft_response.messages(),
                )
            };

            let attempt = self.wft.as_ref().map(|t| t.info.attempt).unwrap_or(1);
            ActivationCompleteOutcome::ReportWFTSuccess(ServerCommandsWithWorkflowInfo {
                task_token: data.task_token,
                action: ActivationAction::WftComplete {
                    force_new_wft,
                    commands,
                    messages,
                    query_responses,
                    sdk_metadata: machines_wft_response.metadata_for_complete(),
                    versioning_behavior: data.versioning_behavior,
                    attempt,
                },
                metrics: self.metrics.clone(),
            })
        } else {
            ActivationCompleteOutcome::DoNothing
        };
        FulfillableActivationComplete {
            result: ActivationCompleteResult {
                outcome,
                replaying: machines_wft_response.replaying,
            },
            resp_chan,
        }
    }

    /// Pump some local activity requests into the sink, applying any immediate results to the
    /// workflow machines.
    fn sink_la_requests(
        &mut self,
        new_local_acts: Vec<LocalActRequest>,
    ) -> Result<(), WFMachinesError> {
        let immediate_resolutions =
            if let Some(ref local_act_request_sink) = self.local_activity_request_sink {
                local_act_request_sink.sink_reqs(new_local_acts)
            } else {
                Vec::new()
            };
        for resolution in immediate_resolutions {
            self.wfm
                .notify_of_local_result(LocalResolution::LocalActivity(resolution))?;
        }
        Ok(())
    }

    fn reply_to_complete(
        &mut self,
        outcome: ActivationCompleteOutcome,
        chan: Option<oneshot::Sender<ActivationCompleteResult>>,
    ) {
        if let Some(chan) = chan
            && chan
                .send(ActivationCompleteResult {
                    outcome,
                    replaying: self.wfm.machines.replaying,
                })
                .is_err()
        {
            let warnstr = "The workflow task completer went missing! This likely indicates an \
                               SDK bug, please report."
                .to_string();
            warn!(run_id=%self.run_id(), "{}", warnstr);
            self.request_eviction(RequestEvictMsg {
                run_id: self.run_id().to_string(),
                message: warnstr,
                reason: EvictionReason::Fatal,
                auto_reply_fail_tt: None,
            });
        }
    }

    /// Returns true if the handle is currently processing a WFT which contains a legacy query.
    fn pending_work_is_legacy_query(&self) -> bool {
        // Either we know because there is a pending legacy query, or it's already been drained and
        // sent as an activation.
        matches!(self.activation, Some(OutstandingActivation::LegacyQuery))
            || self
                .wft
                .as_ref()
                .map(|t| t.has_pending_legacy_query())
                .unwrap_or_default()
    }

    fn most_recently_processed_event_number(&self) -> i64 {
        self.wfm.machines.last_processed_event
    }

    fn activation_is_eviction(&mut self) -> bool {
        self.activation
            .map(|a| matches!(a, OutstandingActivation::Eviction))
            .unwrap_or_default()
    }

    fn run_id(&self) -> &str {
        &self.wfm.machines.run_id
    }
}

// Construct a new command sequence with query responses removed, and any
// terminal responses removed, except for the first terminal response, which is
// placed at the end. Return new command sequence and query commands. Note that
// multiple coroutines may have generated a terminal command, leading to
// multiple terminal commands in the input to this function.
fn preprocess_command_sequence(commands: Vec<WFCommand>) -> (Vec<WFCommand>, Vec<QueryResult>) {
    let mut query_results = vec![];
    let mut terminals = vec![];

    let mut commands: Vec<_> = commands
        .into_iter()
        .filter_map(|c| {
            if let WFCommandVariant::QueryResponse(qr) = c.variant {
                query_results.push(qr);
                None
            } else if c.variant.is_terminal() {
                terminals.push(c);
                None
            } else {
                Some(c)
            }
        })
        .collect();
    if let Some(first_terminal) = terminals.into_iter().next() {
        commands.push(first_terminal);
    }
    (commands, query_results)
}

fn preprocess_command_sequence_old_behavior(
    commands: Vec<WFCommand>,
) -> (Vec<WFCommand>, Vec<QueryResult>) {
    let mut query_results = vec![];
    let mut seen_terminal = false;

    let commands: Vec<_> = commands
        .into_iter()
        .filter_map(|c| {
            if let WFCommandVariant::QueryResponse(qr) = c.variant {
                query_results.push(qr);
                None
            } else if seen_terminal {
                None
            } else {
                if c.variant.is_terminal() {
                    seen_terminal = true;
                }
                Some(c)
            }
        })
        .collect();
    (commands, query_results)
}

/// Drains pending queries from the workflow task and appends them to the activation's jobs
fn put_queries_in_act(act: &mut WorkflowActivation, wft: &mut OutstandingTask) {
    // Nothing to do if there are no pending queries
    if wft.pending_queries.is_empty() {
        return;
    }

    let has_legacy = wft.has_pending_legacy_query();
    // Cannot dispatch legacy query if there are any other jobs - which can happen if, ex, a local
    // activity resolves while we've gotten a legacy query after heartbeating.
    if has_legacy && !act.jobs.is_empty() {
        return;
    }

    debug!(queries=?wft.pending_queries, "Dispatching queries");
    let query_jobs = wft
        .pending_queries
        .drain(..)
        .map(|q| workflow_activation_job::Variant::QueryWorkflow(q).into());
    act.jobs.extend(query_jobs);
}
fn sink_heartbeat_timeout_start(
    run_id: String,
    sink: Option<&dyn LocalActivityRequestSink>,
    wft_start_time: Instant,
    wft_timeout: Duration,
) -> AbortHandle {
    // The heartbeat deadline is 80% of the WFT timeout
    let deadline = wft_start_time.add(wft_timeout.mul_f32(WFT_HEARTBEAT_TIMEOUT_FRACTION));
    let (abort_handle, abort_reg) = AbortHandle::new_pair();
    if let Some(la_sink) = sink {
        la_sink.sink_reqs(vec![LocalActRequest::StartHeartbeatTimeout {
            send_on_elapse: HeartbeatTimeoutMsg {
                run_id,
                span: Span::current(),
            },
            deadline,
            abort_reg,
        }]);
    }
    abort_handle
}

/// Tracks the heartbeat while a workflow task has outstanding local activities.
struct WaitingOnLAs {
    wft_timeout: Duration,
    /// Can be used to abort heartbeat timeouts
    hb_timeout_handle: AbortHandle,
    /// Defers the heartbeat when lang must finish an outstanding activation before Core can safely
    /// complete the workflow task.
    heartbeat_timeout_pending: bool,
}
#[derive(Debug)]
struct CompletionDataForWFT {
    task_token: TaskToken,
    query_responses: Vec<QueryResult>,
    has_pending_query: bool,
    activation_was_eviction: bool,
    is_forced_failure: bool,
    versioning_behavior: VersioningBehavior,
}

/// Manages an instance of a [WorkflowMachines], which is not thread-safe, as well as other data
/// associated with that specific workflow run.
struct WorkflowManager {
    machines: WorkflowMachines,
    /// Is always `Some` in normal operation. Optional to allow for unit testing with the test
    /// workflow driver, which does not need to complete activations the normal way.
    command_sink: Option<Sender<Vec<WFCommand>>>,
}

impl WorkflowManager {
    /// Create a new workflow manager given workflow history and execution info as would be found
    /// in [PollWorkflowTaskQueueResponse]
    fn new(basics: RunBasics) -> Self {
        let (wfb, cmd_sink) = DrivenWorkflow::new();
        let state_machines = WorkflowMachines::new(basics, wfb);
        Self {
            machines: state_machines,
            command_sink: Some(cmd_sink),
        }
    }

    /// Given info that was just obtained from a new WFT from server, pipe it into this workflow's
    /// machines.
    ///
    /// Should only be called when a workflow has caught up on replay (or is just beginning). It
    /// will return a workflow activation if one is needed.
    fn new_work_from_server(
        &mut self,
        update: HistoryUpdate,
        messages: Vec<IncomingProtocolMessage>,
    ) -> Result<WorkflowActivation> {
        self.machines.new_work_from_server(update, messages)?;
        self.get_next_activation()
    }

    /// Update the machines with some events from fetching another page of history. Does *not*
    /// attempt to pull the next activation, unlike [Self::new_work_from_server].
    fn feed_history_from_new_page(&mut self, update: HistoryUpdate) -> Result<()> {
        self.machines.new_history_from_server(update)
    }

    /// Let this workflow know that something we've been waiting locally on has resolved, like a
    /// local activity or side effect
    ///
    /// Returns true if the resolution did anything. EX: If the activity is already canceled and
    /// used the TryCancel or Abandon modes, the resolution is uninteresting.
    fn notify_of_local_result(&mut self, resolved: LocalResolution) -> Result<bool> {
        self.machines.local_resolution(resolved)
    }

    /// Fetch the next workflow activation for this workflow if one is required. Doing so will apply
    /// the next unapplied workflow task if such a sequence exists in history we already know about.
    ///
    /// Callers may also need to call [get_server_commands] after this to issue any pending commands
    /// to the server.
    fn get_next_activation(&mut self) -> Result<WorkflowActivation> {
        // First check if there are already some pending jobs, which can be a result of replay.
        let activation = self.machines.get_wf_activation();
        if !activation.jobs.is_empty() {
            return Ok(activation);
        }

        self.machines.apply_next_wft_from_history()?;
        Ok(self.machines.get_wf_activation())
    }

    /// Returns true if machines are ready to apply the next WFT sequence, false if events will need
    /// to be fetched in order to create a complete update with the entire next WFT sequence.
    pub(crate) fn ready_to_apply_next_wft(&self) -> bool {
        self.machines.ready_to_apply_next_wft()
    }

    /// If there are no pending jobs for the workflow apply the next workflow task and check again
    /// if there are any jobs. Importantly, does not *drain* jobs.
    fn apply_next_task_if_ready(&mut self) -> Result<()> {
        if self.machines.has_pending_jobs() {
            return Ok(());
        }
        loop {
            let consumed_events = self.machines.apply_next_wft_from_history()?;

            if consumed_events == 0 || !self.machines.replaying || self.machines.has_pending_jobs()
            {
                // Keep applying tasks while there are events, we are still replaying, and there are
                // no jobs
                break;
            }
        }
        Ok(())
    }

    /// Must be called when we're ready to respond to a WFT after handling catching up on replay
    /// and handling all activation completions from lang.
    fn prepare_for_wft_response(&mut self) -> MachinesWFTResponseContent<'_> {
        self.machines.prepare_for_wft_response()
    }

    /// Remove and return all queued local activities. Once this is called, they need to be
    /// dispatched for execution.
    fn drain_queued_local_activities(&mut self) -> Vec<LocalActRequest> {
        self.machines.drain_queued_local_activities()
    }

    /// Feed the workflow machines new commands issued by the executing workflow code, and iterate
    /// the machines.
    fn push_commands_and_iterate(&mut self, cmds: Vec<WFCommand>) -> Result<()> {
        if let Some(cs) = self.command_sink.as_mut() {
            cs.send(cmds).map_err(|_| {
                WFMachinesError::Fatal("Internal error buffering workflow commands".to_string())
            })?;
        }
        self.machines.iterate_machines()?;
        Ok(())
    }
}

#[derive(Debug)]
struct FulfillableActivationComplete {
    result: ActivationCompleteResult,
    resp_chan: Option<oneshot::Sender<ActivationCompleteResult>>,
}
impl FulfillableActivationComplete {
    fn fulfill(self) {
        if let Some(resp_chan) = self.resp_chan {
            let _ = resp_chan.send(self.result);
        }
    }
}

#[derive(Debug)]
struct RunActivationCompletion {
    task_token: TaskToken,
    start_time: Instant,
    commands: Vec<WFCommand>,
    activation_was_eviction: bool,
    has_pending_query: bool,
    query_responses: Vec<QueryResult>,
    used_flags: Vec<u32>,
    is_forced_failure: bool,
    /// Used to notify the worker when the completion is done processing and the completion can
    /// unblock. Must always be `Some` when initialized.
    resp_chan: Option<oneshot::Sender<ActivationCompleteResult>>,
    versioning_behavior: VersioningBehavior,
}
#[derive(Debug, derive_more::From)]
enum ActOrFulfill {
    OutgoingAct(Option<ActivationOrAuto>),
    FulfillableComplete(Option<FulfillableActivationComplete>),
}

#[derive(derive_more::Debug)]
#[debug("RunUpdateErr({source:?})")]
struct RunUpdateErr {
    source: WFMachinesError,
    complete_resp_chan: Option<oneshot::Sender<ActivationCompleteResult>>,
}

impl From<WFMachinesError> for RunUpdateErr {
    fn from(e: WFMachinesError) -> Self {
        RunUpdateErr {
            source: e,
            complete_resp_chan: None,
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn log_workflow_task_duration(
    run_id: &str,
    workflow_type: &str,
    event_id: i64,
    attempt: u32,
    history_size_bytes: u64,
    duration: Duration,
    storage: &TaskStorageMetrics,
) {
    let threshold = wft_duration_warn_threshold();
    if duration <= threshold {
        return;
    }
    let dl = storage.download.as_ref();
    let ul = storage.upload.as_ref();
    let duration_millis = |d: Duration| d.as_millis() as u64;
    let storage_millis = |m: Option<&ExternalStorageMetrics>| -> u64 {
        m.and_then(|m| m.total_duration)
            .and_then(|d| Duration::try_from(d).ok())
            .map(duration_millis)
            .unwrap_or_default()
    };
    warn!(
        workflow_type = %workflow_type,
        event_id = event_id,
        attempt = attempt,
        workflow_task_duration = duration_millis(duration),
        workflow_history_size = history_size_bytes,
        payload_download_count = dl.map(|m| m.payload_count).unwrap_or_default(),
        payload_download_size = dl.map(|m| m.total_size_bytes).unwrap_or_default(),
        payload_download_duration = storage_millis(dl),
        payload_download_drivers = ?dl.map(|m| sorted(&m.driver_names)).unwrap_or_default(),
        payload_upload_count = ul.map(|m| m.payload_count).unwrap_or_default(),
        payload_upload_size = ul.map(|m| m.total_size_bytes).unwrap_or_default(),
        payload_upload_duration = storage_millis(ul),
        payload_upload_drivers = ?ul.map(|m| sorted(&m.driver_names)).unwrap_or_default(),
        "[TMPRL1104] {run_id}:{event_id}:{attempt} Workflow task duration exceeded {} seconds.",
        threshold.as_secs()
    );
}

fn wft_duration_warn_threshold() -> Duration {
    static THRESHOLD: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *THRESHOLD.get_or_init(|| {
        parse_wft_duration_warn_threshold(
            std::env::var("TEMPORAL_WORKFLOW_TASK_DURATION_WARN_SECONDS").ok(),
        )
    })
}

// Separated from the env read so the parse + default fallback can be unit-tested without mutating
// the process environment.
fn parse_wft_duration_warn_threshold(value: Option<String>) -> Duration {
    value
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(5))
}

fn sorted(names: &[String]) -> Vec<String> {
    let mut v = names.to_vec();
    v.sort();
    v
}

#[cfg(test)]
mod tests {
    use super::{
        TaskStorageMetrics, log_workflow_task_duration, parse_wft_duration_warn_threshold,
    };
    use crate::worker::workflow::{WFCommand, WFCommandVariant};
    use std::{
        fmt::Write,
        mem::{Discriminant, discriminant},
        sync::{Arc, Mutex},
        time::Duration,
    };
    use temporalio_common::protos::coresdk::common::ExternalStorageMetrics;
    use tracing::{
        Event, Level, Metadata, Subscriber,
        field::{Field, Visit},
        span,
    };

    use command_utils::*;

    #[derive(Default)]
    struct CapturedEvent {
        level: Option<Level>,
        fields: String,
    }
    #[derive(Default, Clone)]
    struct CapturingSub {
        events: Arc<Mutex<Vec<CapturedEvent>>>,
    }
    struct FieldVisitor(String);
    impl Visit for FieldVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            let _ = write!(self.0, "{}={:?};", field.name(), value);
        }
        fn record_u64(&mut self, field: &Field, value: u64) {
            let _ = write!(self.0, "{}={};", field.name(), value);
        }
        fn record_i64(&mut self, field: &Field, value: i64) {
            let _ = write!(self.0, "{}={};", field.name(), value);
        }
        fn record_str(&mut self, field: &Field, value: &str) {
            let _ = write!(self.0, "{}={};", field.name(), value);
        }
    }
    impl Subscriber for CapturingSub {
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
            span::Id::from_u64(1)
        }
        fn record(&self, _: &span::Id, _: &span::Record<'_>) {}
        fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}
        fn event(&self, event: &Event<'_>) {
            let mut v = FieldVisitor(String::new());
            event.record(&mut v);
            self.events.lock().unwrap().push(CapturedEvent {
                level: Some(*event.metadata().level()),
                fields: v.0,
            });
        }
        fn enter(&self, _: &span::Id) {}
        fn exit(&self, _: &span::Id) {}
    }

    fn capture(duration: Duration, storage: &TaskStorageMetrics) -> Option<CapturedEvent> {
        let sub = CapturingSub::default();
        tracing::subscriber::with_default(sub.clone(), || {
            log_workflow_task_duration("run-1", "MyWorkflow", 12, 3, 4096, duration, storage);
        });
        sub.events.lock().unwrap().drain(..).next()
    }

    #[test]
    fn tmprl1104_warns_only_over_threshold() {
        let none = TaskStorageMetrics::default();
        assert!(capture(Duration::from_secs(2), &none).is_none());
        let warn_ev = capture(Duration::from_secs(7), &none).expect("warn emitted");
        assert_eq!(warn_ev.level, Some(Level::WARN));
        assert!(
            warn_ev.fields.contains("[TMPRL1104]"),
            "fields: {}",
            warn_ev.fields
        );
    }

    #[test]
    fn tmprl1104_threshold_parsing() {
        assert_eq!(
            parse_wft_duration_warn_threshold(None),
            Duration::from_secs(5)
        );
        assert_eq!(
            parse_wft_duration_warn_threshold(Some("10".to_string())),
            Duration::from_secs(10)
        );
        assert_eq!(
            parse_wft_duration_warn_threshold(Some("0".to_string())),
            Duration::from_secs(0)
        );
        // Unparseable / empty / negative values fall back to the default (parsed as u64, so a
        // negative can never yield a threshold).
        assert_eq!(
            parse_wft_duration_warn_threshold(Some("nope".to_string())),
            Duration::from_secs(5)
        );
        assert_eq!(
            parse_wft_duration_warn_threshold(Some(String::new())),
            Duration::from_secs(5)
        );
        assert_eq!(
            parse_wft_duration_warn_threshold(Some("-5".to_string())),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn tmprl1104_fields_present() {
        let storage = TaskStorageMetrics {
            download: Some(ExternalStorageMetrics {
                payload_count: 2,
                total_size_bytes: 1024,
                total_duration: Some(prost_types::Duration {
                    seconds: 0,
                    nanos: 5_000_000,
                }),
                driver_names: vec!["s3".to_string()],
            }),
            upload: None,
        };
        let ev = capture(Duration::from_secs(6), &storage).expect("warn emitted");
        assert!(ev.fields.contains("attempt=3"), "fields: {}", ev.fields);
        assert!(
            ev.fields.contains("[TMPRL1104] run-1:12:3"),
            "fields: {}",
            ev.fields
        );
        // The message names the (default) threshold it exceeded.
        assert!(
            ev.fields.contains("exceeded 5 seconds"),
            "fields: {}",
            ev.fields
        );
        assert!(
            ev.fields.contains("workflow_history_size=4096"),
            "fields: {}",
            ev.fields
        );
        assert!(
            ev.fields.contains("payload_download_count=2"),
            "fields: {}",
            ev.fields
        );
        // No upload occurred; that group must still be present as zero.
        assert!(
            ev.fields.contains("payload_upload_count=0"),
            "fields: {}",
            ev.fields
        );
    }

    #[rstest::rstest]
    #[case::empty(
        vec![],
        vec![])]
    #[case::non_terminal_is_retained(
        vec![update_response()],
        vec![update_response()])]
    #[case::terminal_is_retained(
        vec![complete()],
        vec![complete()])]
    #[case::post_terminal_is_retained(
        vec![complete(), update_response()],
        vec![update_response(), complete()])]
    #[case::second_terminal_is_discarded(
        vec![cancel(), complete()],
        vec![cancel()])]
    #[case::move_terminals_to_end_and_retain_first(
        vec![update_response(), complete(), update_response(), cancel(), update_response()],
        vec![update_response(), update_response(), update_response(), complete()])]
    #[test]
    fn preprocess_command_sequence(
        #[case] commands_in: Vec<WFCommand>,
        #[case] expected_commands: Vec<WFCommand>,
    ) {
        let (commands, _) = super::preprocess_command_sequence(commands_in);
        assert_eq!(command_types(&commands), command_types(&expected_commands));
    }

    #[rstest::rstest]
    #[case::query_responses_extracted(
        vec![query_response(), update_response(), query_response(), complete(), query_response()],
        3,
    )]
    #[test]
    fn preprocess_command_sequence_extracts_queries(
        #[case] commands_in: Vec<WFCommand>,
        #[case] expected_queries_out: usize,
    ) {
        let (_, query_responses_out) = super::preprocess_command_sequence(commands_in);
        assert_eq!(query_responses_out.len(), expected_queries_out);
    }

    #[rstest::rstest]
    #[case::empty(
        vec![],
        vec![])]
    #[case::non_terminal_is_retained(
        vec![update_response()],
        vec![update_response()])]
    #[case::terminal_is_retained(
        vec![complete()],
        vec![complete()])]
    #[case::post_terminal_is_discarded(
        vec![complete(), update_response()],
        vec![complete()])]
    #[case::second_terminal_is_discarded(
        vec![cancel(), complete()],
        vec![cancel()])]
    #[case::truncate_at_first_complete(
        vec![update_response(), complete(), update_response(), cancel()],
        vec![update_response(), complete()])]
    #[test]
    fn preprocess_command_sequence_old_behavior(
        #[case] commands_in: Vec<WFCommand>,
        #[case] expected_out: Vec<WFCommand>,
    ) {
        let (commands_out, _) = super::preprocess_command_sequence_old_behavior(commands_in);
        assert_eq!(command_types(&commands_out), command_types(&expected_out));
    }

    #[rstest::rstest]
    #[case::query_responses_extracted(
        vec![query_response(), update_response(), query_response(), complete(), query_response()],
        3,
    )]
    #[test]
    fn preprocess_command_sequence_old_behavior_extracts_queries(
        #[case] commands_in: Vec<WFCommand>,
        #[case] expected_queries_out: usize,
    ) {
        let (_, query_responses_out) = super::preprocess_command_sequence_old_behavior(commands_in);
        assert_eq!(query_responses_out.len(), expected_queries_out);
    }

    mod command_utils {
        use temporalio_common::protos::coresdk::workflow_commands::{
            CancelWorkflowExecution, CompleteWorkflowExecution, QueryResult, UpdateResponse,
        };

        use super::*;

        pub(crate) fn complete() -> WFCommand {
            WFCommand::new(WFCommandVariant::CompleteWorkflow(
                CompleteWorkflowExecution { result: None },
            ))
        }

        pub(crate) fn cancel() -> WFCommand {
            WFCommand::new(WFCommandVariant::CancelWorkflow(CancelWorkflowExecution {}))
        }

        pub(crate) fn query_response() -> WFCommand {
            WFCommand::new(WFCommandVariant::QueryResponse(QueryResult {
                query_id: "".into(),
                variant: None,
            }))
        }

        pub(crate) fn update_response() -> WFCommand {
            WFCommand::new(WFCommandVariant::UpdateResponse(UpdateResponse {
                protocol_instance_id: "".into(),
                response: None,
            }))
        }

        pub(crate) fn command_types(commands: &[WFCommand]) -> Vec<Discriminant<WFCommand>> {
            commands.iter().map(discriminant).collect()
        }
    }
}
