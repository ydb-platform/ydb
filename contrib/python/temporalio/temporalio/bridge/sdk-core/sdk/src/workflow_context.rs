mod options;

pub use options::{
    ActivityOptions, ChildWorkflowOptions, LocalActivityOptions, NexusOperationOptions, Signal,
    SignalData, SignalWorkflowOptions, TimerOptions,
};

use crate::{
    CancelExternalWfResult, CancellableID, CancellableIDWithReason, CommandCreateRequest,
    CommandSubscribeChildWorkflowCompletion, IntoUpdateHandlerFunc, IntoUpdateValidatorFunc,
    NexusStartResult, RustWfCmd, SignalExternalWfResult, SupportsCancelReason, TimerResult,
    UnblockEvent, Unblockable, UpdateFunctions, workflow_context::options::IntoWorkflowCommand,
};
use futures_util::{FutureExt, Stream, StreamExt, future::Shared, task::Context};
use parking_lot::{RwLock, RwLockReadGuard};
use std::{
    collections::HashMap,
    future,
    future::Future,
    marker::PhantomData,
    ops::Deref,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, Sender},
    },
    task::Poll,
    time::{Duration, SystemTime},
};
use temporal_sdk_core_api::worker::WorkerDeploymentVersion;
use temporal_sdk_core_protos::{
    coresdk::{
        activity_result::{ActivityResolution, activity_resolution},
        child_workflow::ChildWorkflowResult,
        common::NamespacedWorkflowExecution,
        nexus::NexusOperationResult,
        workflow_activation::{
            InitializeWorkflow,
            resolve_child_workflow_execution_start::Status as ChildWorkflowStartStatus,
        },
        workflow_commands::{
            CancelChildWorkflowExecution, ModifyWorkflowProperties,
            RequestCancelExternalWorkflowExecution, SetPatchMarker,
            SignalExternalWorkflowExecution, StartTimer, UpsertWorkflowSearchAttributes,
            WorkflowCommand, signal_external_workflow_execution as sig_we, workflow_command,
        },
    },
    temporal::api::{
        common::v1::{Memo, Payload, SearchAttributes},
        sdk::v1::UserMetadata,
    },
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::wrappers::UnboundedReceiverStream;

/// Used within workflows to issue commands, get info, etc.
#[derive(Clone)]
pub struct WfContext {
    namespace: String,
    task_queue: String,
    inital_information: Arc<InitializeWorkflow>,

    chan: Sender<RustWfCmd>,
    am_cancelled: watch::Receiver<Option<String>>,
    pub(crate) shared: Arc<RwLock<WfContextSharedData>>,

    seq_nums: Arc<RwLock<WfCtxProtectedDat>>,
}

// TODO: Dataconverter type interface to replace Payloads here. Possibly just use serde
//    traits.
impl WfContext {
    /// Create a new wf context, returning the context itself and a receiver which outputs commands
    /// sent from the workflow.
    pub(super) fn new(
        namespace: String,
        task_queue: String,
        init_workflow_job: InitializeWorkflow,
        am_cancelled: watch::Receiver<Option<String>>,
    ) -> (Self, Receiver<RustWfCmd>) {
        // The receiving side is non-async
        let (chan, rx) = std::sync::mpsc::channel();
        (
            Self {
                namespace,
                task_queue,
                shared: Arc::new(RwLock::new(WfContextSharedData {
                    random_seed: init_workflow_job.randomness_seed,
                    search_attributes: init_workflow_job
                        .search_attributes
                        .clone()
                        .unwrap_or_default(),
                    ..Default::default()
                })),
                inital_information: Arc::new(init_workflow_job),
                chan,
                am_cancelled,
                seq_nums: Arc::new(RwLock::new(WfCtxProtectedDat {
                    next_timer_sequence_number: 1,
                    next_activity_sequence_number: 1,
                    next_child_workflow_sequence_number: 1,
                    next_cancel_external_wf_sequence_number: 1,
                    next_signal_external_wf_sequence_number: 1,
                    next_nexus_op_sequence_number: 1,
                })),
            },
            rx,
        )
    }

    /// Return the namespace the workflow is executing in
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Return the task queue the workflow is executing in
    pub fn task_queue(&self) -> &str {
        &self.task_queue
    }

    /// Get the arguments provided to the workflow upon execution start
    pub fn get_args(&self) -> &[Payload] {
        self.inital_information.arguments.as_slice()
    }

    /// Return the current time according to the workflow (which is not wall-clock time).
    pub fn workflow_time(&self) -> Option<SystemTime> {
        self.shared.read().wf_time
    }

    /// Return the length of history so far at this point in the workflow
    pub fn history_length(&self) -> u32 {
        self.shared.read().history_length
    }

    /// Return the deployment version, if any,  as it was when this point in the workflow was first
    /// reached. If this code is being executed for the first time, return this Worker's deployment
    /// version if it has one.
    pub fn current_deployment_version(&self) -> Option<WorkerDeploymentVersion> {
        self.shared.read().current_deployment_version.clone()
    }

    /// Return current values for workflow search attributes
    pub fn search_attributes(&self) -> impl Deref<Target = SearchAttributes> + '_ {
        RwLockReadGuard::map(self.shared.read(), |s| &s.search_attributes)
    }

    /// Return the workflow's randomness seed
    pub fn random_seed(&self) -> u64 {
        self.shared.read().random_seed
    }

    /// Returns true if the current workflow task is happening under replay
    pub fn is_replaying(&self) -> bool {
        self.shared.read().is_replaying
    }

    /// Return various information that the workflow was initialized with. Will eventually become
    /// a proper non-proto workflow info struct.
    pub fn workflow_initial_info(&self) -> &InitializeWorkflow {
        &self.inital_information
    }

    /// A future that resolves if/when the workflow is cancelled, with the user provided cause
    pub async fn cancelled(&self) -> String {
        if let Some(s) = self.am_cancelled.borrow().as_ref() {
            return s.clone();
        }
        self.am_cancelled
            .clone()
            .changed()
            .await
            .expect("Cancelled send half not dropped");
        self.am_cancelled
            .borrow()
            .as_ref()
            .cloned()
            .unwrap_or_default()
    }

    /// Request to create a timer
    pub fn timer<T: Into<TimerOptions>>(&self, opts: T) -> impl CancellableFuture<TimerResult> {
        let opts: TimerOptions = opts.into();
        let seq = self.seq_nums.write().next_timer_seq();
        let (cmd, unblocker) = CancellableWFCommandFut::new(CancellableID::Timer(seq));
        self.send(
            CommandCreateRequest {
                cmd: WorkflowCommand {
                    variant: Some(
                        StartTimer {
                            seq,
                            start_to_fire_timeout: Some(
                                opts.duration
                                    .try_into()
                                    .expect("Durations must fit into 64 bits"),
                            ),
                        }
                        .into(),
                    ),
                    user_metadata: Some(UserMetadata {
                        summary: opts.summary.map(|x| x.as_bytes().into()),
                        details: None,
                    }),
                },
                unblocker,
            }
            .into(),
        );
        cmd
    }

    /// Request to run an activity
    pub fn activity(
        &self,
        mut opts: ActivityOptions,
    ) -> impl CancellableFuture<ActivityResolution> {
        if opts.task_queue.is_none() {
            opts.task_queue = Some(self.task_queue.clone());
        }
        let seq = self.seq_nums.write().next_activity_seq();
        let (cmd, unblocker) = CancellableWFCommandFut::new(CancellableID::Activity(seq));
        self.send(
            CommandCreateRequest {
                cmd: opts.into_command(seq),
                unblocker,
            }
            .into(),
        );
        cmd
    }

    /// Request to run a local activity
    pub fn local_activity(
        &self,
        opts: LocalActivityOptions,
    ) -> impl CancellableFuture<ActivityResolution> + '_ {
        LATimerBackoffFut::new(opts, self)
    }

    /// Request to run a local activity with no implementation of timer-backoff based retrying.
    fn local_activity_no_timer_retry(
        &self,
        opts: LocalActivityOptions,
    ) -> impl CancellableFuture<ActivityResolution> {
        let seq = self.seq_nums.write().next_activity_seq();
        let (cmd, unblocker) = CancellableWFCommandFut::new(CancellableID::LocalActivity(seq));
        self.send(
            CommandCreateRequest {
                cmd: opts.into_command(seq),
                unblocker,
            }
            .into(),
        );
        cmd
    }

    /// Creates a child workflow stub with the provided options
    pub fn child_workflow(&self, opts: ChildWorkflowOptions) -> ChildWorkflow {
        ChildWorkflow { opts }
    }

    /// Check (or record) that this workflow history was created with the provided patch
    pub fn patched(&self, patch_id: &str) -> bool {
        self.patch_impl(patch_id, false)
    }

    /// Record that this workflow history was created with the provided patch, and it is being
    /// phased out.
    pub fn deprecate_patch(&self, patch_id: &str) -> bool {
        self.patch_impl(patch_id, true)
    }

    fn patch_impl(&self, patch_id: &str, deprecated: bool) -> bool {
        self.send(
            workflow_command::Variant::SetPatchMarker(SetPatchMarker {
                patch_id: patch_id.to_string(),
                deprecated,
            })
            .into(),
        );
        // See if we already know about the status of this change
        if let Some(present) = self.shared.read().changes.get(patch_id) {
            return *present;
        }

        // If we don't already know about the change, that means there is no marker in history,
        // and we should return false if we are replaying
        let res = !self.shared.read().is_replaying;

        self.shared
            .write()
            .changes
            .insert(patch_id.to_string(), res);

        res
    }

    /// Send a signal to an external workflow. May resolve as a failure if the signal didn't work
    /// or was cancelled.
    pub fn signal_workflow(
        &self,
        opts: impl Into<SignalWorkflowOptions>,
    ) -> impl CancellableFuture<SignalExternalWfResult> {
        let options: SignalWorkflowOptions = opts.into();
        let target = sig_we::Target::WorkflowExecution(NamespacedWorkflowExecution {
            namespace: self.namespace.clone(),
            workflow_id: options.workflow_id,
            run_id: options.run_id.unwrap_or_default(),
        });
        self.send_signal_wf(target, options.signal)
    }

    /// Add or create a set of search attributes
    pub fn upsert_search_attributes(&self, attr_iter: impl IntoIterator<Item = (String, Payload)>) {
        self.send(RustWfCmd::NewNonblockingCmd(
            workflow_command::Variant::UpsertWorkflowSearchAttributes(
                UpsertWorkflowSearchAttributes {
                    search_attributes: HashMap::from_iter(attr_iter),
                },
            ),
        ))
    }

    /// Add or create a set of search attributes
    pub fn upsert_memo(&self, attr_iter: impl IntoIterator<Item = (String, Payload)>) {
        self.send(RustWfCmd::NewNonblockingCmd(
            workflow_command::Variant::ModifyWorkflowProperties(ModifyWorkflowProperties {
                upserted_memo: Some(Memo {
                    fields: HashMap::from_iter(attr_iter),
                }),
            }),
        ))
    }

    /// Return a stream that produces values when the named signal is sent to this workflow
    pub fn make_signal_channel(&self, signal_name: impl Into<String>) -> DrainableSignalStream {
        let (tx, rx) = mpsc::unbounded_channel();
        self.send(RustWfCmd::SubscribeSignal(signal_name.into(), tx));
        DrainableSignalStream(UnboundedReceiverStream::new(rx))
    }

    /// Force a workflow task failure (EX: in order to retry on non-sticky queue)
    pub fn force_task_fail(&self, with: anyhow::Error) {
        self.send(with.into());
    }

    /// Request the cancellation of an external workflow. May resolve as a failure if the workflow
    /// was not found or the cancel was otherwise unsendable.
    pub fn cancel_external(
        &self,
        target: NamespacedWorkflowExecution,
        reason: String,
    ) -> impl Future<Output = CancelExternalWfResult> {
        let seq = self.seq_nums.write().next_cancel_external_wf_seq();
        let (cmd, unblocker) = WFCommandFut::new();
        self.send(
            CommandCreateRequest {
                cmd: WorkflowCommand {
                    variant: Some(
                        RequestCancelExternalWorkflowExecution {
                            seq,
                            workflow_execution: Some(target),
                            reason,
                        }
                        .into(),
                    ),
                    user_metadata: None,
                },
                unblocker,
            }
            .into(),
        );
        cmd
    }

    /// Register an update handler by providing the handler name, a validator function, and an
    /// update handler. The validator must not mutate workflow state and is synchronous. The handler
    /// may mutate workflow state (though, that's annoying right now in the prototype) and is async.
    ///
    /// Note that if you want a validator that always passes, you will likely need to provide type
    /// annotations to make the compiler happy, like: `|_: &_, _: T| Ok(())`
    pub fn update_handler<Arg, Res>(
        &self,
        name: impl Into<String>,
        validator: impl IntoUpdateValidatorFunc<Arg>,
        handler: impl IntoUpdateHandlerFunc<Arg, Res>,
    ) {
        self.send(RustWfCmd::RegisterUpdate(
            name.into(),
            UpdateFunctions::new(validator, handler),
        ))
    }

    /// Start a nexus operation
    pub fn start_nexus_operation(
        &self,
        opts: NexusOperationOptions,
    ) -> impl CancellableFuture<NexusStartResult> {
        let seq = self.seq_nums.write().next_nexus_op_seq();
        let (result_future, unblocker) = WFCommandFut::new();
        self.send(RustWfCmd::SubscribeNexusOperationCompletion { seq, unblocker });
        let (cmd, unblocker) = CancellableWFCommandFut::new_with_dat(
            CancellableID::NexusOp(seq),
            NexusUnblockData {
                result_future: result_future.shared(),
                schedule_seq: seq,
            },
        );
        self.send(
            CommandCreateRequest {
                cmd: opts.into_command(seq),
                unblocker,
            }
            .into(),
        );
        cmd
    }

    /// Wait for some condition to become true, yielding the workflow if it is not.
    pub fn wait_condition(&self, mut condition: impl FnMut() -> bool) -> impl Future<Output = ()> {
        future::poll_fn(move |_cx: &mut Context<'_>| {
            if condition() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
    }

    /// Buffer a command to be sent in the activation reply
    pub(crate) fn send(&self, c: RustWfCmd) {
        self.chan.send(c).expect("command channel intact");
    }

    fn send_signal_wf(
        &self,
        target: sig_we::Target,
        signal: Signal,
    ) -> impl CancellableFuture<SignalExternalWfResult> {
        let seq = self.seq_nums.write().next_signal_external_wf_seq();
        let (cmd, unblocker) =
            CancellableWFCommandFut::new(CancellableID::SignalExternalWorkflow(seq));
        self.send(
            CommandCreateRequest {
                cmd: WorkflowCommand {
                    variant: Some(
                        SignalExternalWorkflowExecution {
                            seq,
                            signal_name: signal.signal_name,
                            args: signal.data.input,
                            target: Some(target),
                            headers: signal.data.headers,
                        }
                        .into(),
                    ),
                    user_metadata: None,
                },
                unblocker,
            }
            .into(),
        );
        cmd
    }

    /// Cancel any cancellable operation by ID
    fn cancel(&self, cancellable_id: CancellableID) {
        self.send(RustWfCmd::Cancel(cancellable_id));
    }
}

struct WfCtxProtectedDat {
    next_timer_sequence_number: u32,
    next_activity_sequence_number: u32,
    next_child_workflow_sequence_number: u32,
    next_cancel_external_wf_sequence_number: u32,
    next_signal_external_wf_sequence_number: u32,
    next_nexus_op_sequence_number: u32,
}

impl WfCtxProtectedDat {
    fn next_timer_seq(&mut self) -> u32 {
        let seq = self.next_timer_sequence_number;
        self.next_timer_sequence_number += 1;
        seq
    }
    fn next_activity_seq(&mut self) -> u32 {
        let seq = self.next_activity_sequence_number;
        self.next_activity_sequence_number += 1;
        seq
    }
    fn next_child_workflow_seq(&mut self) -> u32 {
        let seq = self.next_child_workflow_sequence_number;
        self.next_child_workflow_sequence_number += 1;
        seq
    }
    fn next_cancel_external_wf_seq(&mut self) -> u32 {
        let seq = self.next_cancel_external_wf_sequence_number;
        self.next_cancel_external_wf_sequence_number += 1;
        seq
    }
    fn next_signal_external_wf_seq(&mut self) -> u32 {
        let seq = self.next_signal_external_wf_sequence_number;
        self.next_signal_external_wf_sequence_number += 1;
        seq
    }
    fn next_nexus_op_seq(&mut self) -> u32 {
        let seq = self.next_nexus_op_sequence_number;
        self.next_nexus_op_sequence_number += 1;
        seq
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct WfContextSharedData {
    /// Maps change ids -> resolved status
    pub(crate) changes: HashMap<String, bool>,
    pub(crate) is_replaying: bool,
    pub(crate) wf_time: Option<SystemTime>,
    pub(crate) history_length: u32,
    pub(crate) current_deployment_version: Option<WorkerDeploymentVersion>,
    pub(crate) search_attributes: SearchAttributes,
    pub(crate) random_seed: u64,
}

/// Helper Wrapper that can drain the channel into a Vec<SignalData> in a blocking way.  Useful
/// for making sure channels are empty before ContinueAsNew-ing a workflow
pub struct DrainableSignalStream(UnboundedReceiverStream<SignalData>);

impl DrainableSignalStream {
    pub fn drain_all(self) -> Vec<SignalData> {
        let mut receiver = self.0.into_inner();
        let mut signals = vec![];
        while let Ok(s) = receiver.try_recv() {
            signals.push(s);
        }
        signals
    }

    pub fn drain_ready(&mut self) -> Vec<SignalData> {
        let mut signals = vec![];
        while let Some(s) = self.0.next().now_or_never().flatten() {
            signals.push(s);
        }
        signals
    }
}

impl Stream for DrainableSignalStream {
    type Item = SignalData;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0).poll_next(cx)
    }
}

/// A Future that can be cancelled.
/// Used in the prototype SDK for cancelling operations like timers and activities.
pub trait CancellableFuture<T>: Future<Output = T> {
    /// Cancel this Future
    fn cancel(&self, cx: &WfContext);
}

/// A Future that can be cancelled with a reason
pub trait CancellableFutureWithReason<T>: CancellableFuture<T> {
    /// Cancel this Future with a reason
    fn cancel_with_reason(&self, cx: &WfContext, reason: String);
}

struct WFCommandFut<T, D> {
    _unused: PhantomData<T>,
    result_rx: oneshot::Receiver<UnblockEvent>,
    other_dat: Option<D>,
}
impl<T> WFCommandFut<T, ()> {
    fn new() -> (Self, oneshot::Sender<UnblockEvent>) {
        Self::new_with_dat(())
    }
}

impl<T, D> WFCommandFut<T, D> {
    fn new_with_dat(other_dat: D) -> (Self, oneshot::Sender<UnblockEvent>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                _unused: PhantomData,
                result_rx: rx,
                other_dat: Some(other_dat),
            },
            tx,
        )
    }
}

impl<T, D> Unpin for WFCommandFut<T, D> where T: Unblockable<OtherDat = D> {}
impl<T, D> Future for WFCommandFut<T, D>
where
    T: Unblockable<OtherDat = D>,
{
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.result_rx.poll_unpin(cx).map(|x| {
            // SAFETY: Because we can only enter this section once the future has resolved, we
            // know it will never be polled again, therefore consuming the option is OK.
            let od = self
                .other_dat
                .take()
                .expect("Other data must exist when resolving command future");
            Unblockable::unblock(x.unwrap(), od)
        })
    }
}

struct CancellableWFCommandFut<T, D, ID = CancellableID> {
    cmd_fut: WFCommandFut<T, D>,
    cancellable_id: ID,
}
impl<T, ID> CancellableWFCommandFut<T, (), ID> {
    fn new(cancellable_id: ID) -> (Self, oneshot::Sender<UnblockEvent>) {
        Self::new_with_dat(cancellable_id, ())
    }
}
impl<T, D, ID> CancellableWFCommandFut<T, D, ID> {
    fn new_with_dat(cancellable_id: ID, other_dat: D) -> (Self, oneshot::Sender<UnblockEvent>) {
        let (cmd_fut, sender) = WFCommandFut::new_with_dat(other_dat);
        (
            Self {
                cmd_fut,
                cancellable_id,
            },
            sender,
        )
    }
}
impl<T, D, ID> Unpin for CancellableWFCommandFut<T, D, ID> where T: Unblockable<OtherDat = D> {}
impl<T, D, ID> Future for CancellableWFCommandFut<T, D, ID>
where
    T: Unblockable<OtherDat = D>,
{
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.cmd_fut.poll_unpin(cx)
    }
}

impl<T, D, ID> CancellableFuture<T> for CancellableWFCommandFut<T, D, ID>
where
    T: Unblockable<OtherDat = D>,
    ID: Clone + Into<CancellableID>,
{
    fn cancel(&self, cx: &WfContext) {
        cx.cancel(self.cancellable_id.clone().into());
    }
}
impl<T, D> CancellableFutureWithReason<T> for CancellableWFCommandFut<T, D, CancellableIDWithReason>
where
    T: Unblockable<OtherDat = D>,
{
    fn cancel_with_reason(&self, cx: &WfContext, reason: String) {
        let new_id = self.cancellable_id.clone().with_reason(reason);
        cx.cancel(new_id);
    }
}

struct LATimerBackoffFut<'a> {
    la_opts: LocalActivityOptions,
    current_fut: Pin<Box<dyn CancellableFuture<ActivityResolution> + Send + Unpin + 'a>>,
    timer_fut: Option<Pin<Box<dyn CancellableFuture<TimerResult> + Send + Unpin + 'a>>>,
    ctx: &'a WfContext,
    next_attempt: u32,
    next_sched_time: Option<prost_types::Timestamp>,
    did_cancel: AtomicBool,
}
impl<'a> LATimerBackoffFut<'a> {
    pub(crate) fn new(opts: LocalActivityOptions, ctx: &'a WfContext) -> Self {
        Self {
            la_opts: opts.clone(),
            current_fut: Box::pin(ctx.local_activity_no_timer_retry(opts)),
            timer_fut: None,
            ctx,
            next_attempt: 1,
            next_sched_time: None,
            did_cancel: AtomicBool::new(false),
        }
    }
}
impl Unpin for LATimerBackoffFut<'_> {}
impl Future for LATimerBackoffFut<'_> {
    type Output = ActivityResolution;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // If the timer exists, wait for it first
        if let Some(tf) = self.timer_fut.as_mut() {
            return match tf.poll_unpin(cx) {
                Poll::Ready(tr) => {
                    self.timer_fut = None;
                    // Schedule next LA if this timer wasn't cancelled
                    if let TimerResult::Fired = tr {
                        let mut opts = self.la_opts.clone();
                        opts.attempt = Some(self.next_attempt);
                        opts.original_schedule_time
                            .clone_from(&self.next_sched_time);
                        self.current_fut = Box::pin(self.ctx.local_activity_no_timer_retry(opts));
                        Poll::Pending
                    } else {
                        Poll::Ready(ActivityResolution {
                            status: Some(
                                activity_resolution::Status::Cancelled(Default::default()),
                            ),
                        })
                    }
                }
                Poll::Pending => Poll::Pending,
            };
        }
        let poll_res = self.current_fut.poll_unpin(cx);
        if let Poll::Ready(ref r) = poll_res
            && let Some(activity_resolution::Status::Backoff(b)) = r.status.as_ref()
        {
            // If we've already said we want to cancel, don't schedule the backoff timer. Just
            // return cancel status. This can happen if cancel comes after the LA says it wants
            // to back off but before we have scheduled the timer.
            if self.did_cancel.load(Ordering::Acquire) {
                return Poll::Ready(ActivityResolution {
                    status: Some(activity_resolution::Status::Cancelled(Default::default())),
                });
            }

            let timer_f = self.ctx.timer::<Duration>(
                b.backoff_duration
                    .expect("Duration is set")
                    .try_into()
                    .expect("duration converts ok"),
            );
            self.timer_fut = Some(Box::pin(timer_f));
            self.next_attempt = b.attempt;
            self.next_sched_time.clone_from(&b.original_schedule_time);
            return Poll::Pending;
        }
        poll_res
    }
}
impl CancellableFuture<ActivityResolution> for LATimerBackoffFut<'_> {
    fn cancel(&self, ctx: &WfContext) {
        self.did_cancel.store(true, Ordering::Release);
        if let Some(tf) = self.timer_fut.as_ref() {
            tf.cancel(ctx);
        }
        self.current_fut.cancel(ctx);
    }
}

/// A stub representing an unstarted child workflow.
#[derive(Default, Debug, Clone)]
pub struct ChildWorkflow {
    opts: ChildWorkflowOptions,
}

pub(crate) struct ChildWfCommon {
    workflow_id: String,
    result_future: CancellableWFCommandFut<ChildWorkflowResult, (), CancellableIDWithReason>,
}

/// Child workflow in pending state
pub struct PendingChildWorkflow {
    /// The status of the child workflow start
    pub status: ChildWorkflowStartStatus,
    pub(crate) common: ChildWfCommon,
}

impl PendingChildWorkflow {
    /// Returns `None` if the child did not start successfully. The returned [StartedChildWorkflow]
    /// can be used to wait on, signal, or cancel the child workflow.
    pub fn into_started(self) -> Option<StartedChildWorkflow> {
        match self.status {
            ChildWorkflowStartStatus::Succeeded(s) => Some(StartedChildWorkflow {
                run_id: s.run_id,
                common: self.common,
            }),
            _ => None,
        }
    }
}

/// Child workflow in started state
pub struct StartedChildWorkflow {
    /// Run ID of the child workflow
    pub run_id: String,
    common: ChildWfCommon,
}

impl ChildWorkflow {
    /// Start the child workflow, the returned Future is cancellable.
    pub fn start(self, cx: &WfContext) -> impl CancellableFutureWithReason<PendingChildWorkflow> {
        let child_seq = cx.seq_nums.write().next_child_workflow_seq();
        // Immediately create the command/future for the result, otherwise if the user does
        // not await the result until *after* we receive an activation for it, there will be nothing
        // to match when unblocking.
        let cancel_seq = cx.seq_nums.write().next_cancel_external_wf_seq();
        let (result_cmd, unblocker) =
            CancellableWFCommandFut::new(CancellableIDWithReason::ExternalWorkflow {
                seqnum: cancel_seq,
                execution: NamespacedWorkflowExecution {
                    workflow_id: self.opts.workflow_id.clone(),
                    ..Default::default()
                },
            });
        cx.send(
            CommandSubscribeChildWorkflowCompletion {
                seq: child_seq,
                unblocker,
            }
            .into(),
        );

        let common = ChildWfCommon {
            workflow_id: self.opts.workflow_id.clone(),
            result_future: result_cmd,
        };

        let (cmd, unblocker) = CancellableWFCommandFut::new_with_dat(
            CancellableIDWithReason::ChildWorkflow { seqnum: child_seq },
            common,
        );
        cx.send(
            CommandCreateRequest {
                cmd: self.opts.into_command(child_seq),
                unblocker,
            }
            .into(),
        );

        cmd
    }
}

impl StartedChildWorkflow {
    /// Consumes self and returns a future that will wait until completion of this child workflow
    /// execution
    pub fn result(self) -> impl CancellableFutureWithReason<ChildWorkflowResult> {
        self.common.result_future
    }

    /// Cancel the child workflow
    pub fn cancel(&self, cx: &WfContext, reason: String) {
        cx.send(RustWfCmd::NewNonblockingCmd(
            CancelChildWorkflowExecution {
                child_workflow_seq: self.common.result_future.cancellable_id.seq_num(),
                reason,
            }
            .into(),
        ));
    }

    /// Signal the child workflow
    pub fn signal<'a, S: Into<Signal>>(
        &self,
        cx: &'a WfContext,
        data: S,
    ) -> impl CancellableFuture<SignalExternalWfResult> + use<'a, S> {
        let target = sig_we::Target::ChildWorkflowId(self.common.workflow_id.clone());
        cx.send_signal_wf(target, data.into())
    }
}

#[derive(derive_more::Debug)]
#[debug("StartedNexusOperation{{ operation_token: {operation_token:?} }}")]
pub struct StartedNexusOperation {
    /// The operation token, if the operation started asynchronously
    pub operation_token: Option<String>,
    pub(crate) unblock_dat: NexusUnblockData,
}

pub(crate) struct NexusUnblockData {
    result_future: Shared<WFCommandFut<NexusOperationResult, ()>>,
    schedule_seq: u32,
}

impl StartedNexusOperation {
    pub async fn result(&self) -> NexusOperationResult {
        self.unblock_dat.result_future.clone().await
    }

    pub fn cancel(&self, cx: &WfContext) {
        cx.cancel(CancellableID::NexusOp(self.unblock_dat.schedule_seq));
    }
}
