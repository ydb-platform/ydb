#include "dq_compute_actor_checkpoints.h"
#include "dq_checkpoints.h"

#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/dq/actors/dq.h>
#include <ydb/library/yql/dq/common/dq_common.h>

#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>

#include <algorithm>

#define LOG_T(s) \
    LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "[" << GraphId << "] Task: " << Task.GetId() << ". " << s)
#define LOG_D(s) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "[" << GraphId << "] Task: " << Task.GetId() << ". " << s)
#define LOG_I(s) \
    LOG_INFO_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, "[" << GraphId << "] Task: " << Task.GetId() << ". " << s)
#define LOG_W(s) \
    LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "[" << GraphId << "] Task: " << Task.GetId() << ". " << s)
#define LOG_E(s) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "[" << GraphId << "] Task: " << Task.GetId() << ". " << s)

#define LOG_CP_T(сheckpoint, s) \
    LOG_T("[Checkpoint " << MakeStringForLog(сheckpoint) << "] " << s)
#define LOG_CP_D(сheckpoint, s) \
    LOG_D("[Checkpoint " << MakeStringForLog(сheckpoint) << "] " << s)
#define LOG_CP_I(сheckpoint, s) \
    LOG_I("[Checkpoint " << MakeStringForLog(сheckpoint) << "] " << s)
#define LOG_CP_W(сheckpoint, s) \
    LOG_W("[Checkpoint " << MakeStringForLog(сheckpoint) << "] " << s)
#define LOG_CP_E(сheckpoint, s) \
    LOG_E("[Checkpoint " << MakeStringForLog(сheckpoint) << "] " << s)

#define LOG_PCP_T(s) \
    LOG_CP_T(*PendingSaveStateCheckpoint.Checkpoint, s)
#define LOG_PCP_D(s) \
    LOG_CP_D(*PendingSaveStateCheckpoint.Checkpoint, s)
#define LOG_PCP_I(s) \
    LOG_CP_I(*PendingSaveStateCheckpoint.Checkpoint, s)
#define LOG_PCP_W(s) \
    LOG_CP_W(*PendingSaveStateCheckpoint.Checkpoint, s)
#define LOG_PCP_E(s) \
    LOG_CP_E(*PendingSaveStateCheckpoint.Checkpoint, s)

namespace NYql::NDq {

using namespace NActors;

namespace {

constexpr TDuration SLOW_CHECKPOINT_DURATION = TDuration::Minutes(1);

TString MakeStringForLog(const NDqProto::TCheckpoint& checkpoint) {
    return TStringBuilder() << checkpoint.GetGeneration() << "." << checkpoint.GetId();
}

std::vector<ui64> TaskIdsFromLoadPlan(const NDqProto::NDqStateLoadPlan::TTaskPlan& plan) {
    std::vector<ui64> taskIds;
    for (const auto& sourcePlan : plan.GetSources()) {
        if (sourcePlan.GetStateType() == NDqProto::NDqStateLoadPlan::STATE_TYPE_FOREIGN) {
            for (const auto& foreignTaskSource : sourcePlan.GetForeignTasksSources()) {
                taskIds.push_back(foreignTaskSource.GetTaskId());
            }
        }
    }
    std::sort(taskIds.begin(), taskIds.end());
    taskIds.erase(std::unique(taskIds.begin(), taskIds.end()), taskIds.end());
    return taskIds;
}

const TSourceState& FindSourceState(
    const NDqProto::NDqStateLoadPlan::TSourcePlan::TForeignTaskSource& foreignTaskSource,
    const std::vector<TComputeActorState>& states,
    const std::vector<ui64>& taskIds)
{
    // Find state index
    const auto stateIndexIt = std::lower_bound(taskIds.begin(), taskIds.end(), foreignTaskSource.GetTaskId());
    YQL_ENSURE(stateIndexIt != taskIds.end(), "Task id was not found in plan");
    const size_t stateIndex = std::distance(taskIds.begin(), stateIndexIt);
    const TComputeActorState& state = states[stateIndex];
    for (const TSourceState& sourceState : state.Sources) {
        if (sourceState.InputIndex == foreignTaskSource.GetInputIndex()) {
            return sourceState;
        }
    }
    YQL_ENSURE(false, "Source input index " << foreignTaskSource.GetInputIndex() << " was not found in state");
    // Make compiler happy
    return state.Sources.front();
}

TComputeActorState CombineForeignState(
    const NDqProto::NDqStateLoadPlan::TTaskPlan& plan,
    const std::vector<TComputeActorState>& states,
    const std::vector<ui64>& taskIds)
{
    TComputeActorState state;
    state.MiniKqlProgram.ConstructInPlace().Data.Version = TDqComputeActorCheckpoints::ComputeActorCurrentStateVersion;
    YQL_ENSURE(plan.GetProgram().GetStateType() == NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY, "Unsupported program state type. Plan: " << plan);
    for (const auto& sinkPlan : plan.GetSinks()) {
        YQL_ENSURE(sinkPlan.GetStateType() == NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY, "Unsupported sink state type. Plan: " << sinkPlan);
    }
    for (const auto& sourcePlan : plan.GetSources()) {
        YQL_ENSURE(sourcePlan.GetStateType() == NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY || sourcePlan.GetStateType() == NDqProto::NDqStateLoadPlan::STATE_TYPE_FOREIGN, "Unsupported sink state type. Plan: " << sourcePlan);
        if (sourcePlan.GetStateType() == NDqProto::NDqStateLoadPlan::STATE_TYPE_FOREIGN) {
            state.Sources.push_back({});
            auto& sourceState = state.Sources.back();
            sourceState.InputIndex = sourcePlan.GetInputIndex();
            for (const auto& foreignTaskSource : sourcePlan.GetForeignTasksSources()) {
                const TSourceState& srcSourceState = FindSourceState(foreignTaskSource, states, taskIds);
                for (const TStateData& data : srcSourceState.Data) {
                    sourceState.Data.emplace_back(data);
                }
            }
            YQL_ENSURE(sourceState.DataSize(), "No data was loaded to source " << sourcePlan.GetInputIndex());
        }
    }
    return state;
}

} // namespace

//// TPendingCheckpointBase

TDqComputeActorCheckpoints::TPendingCheckpointBase::TPendingCheckpointBase(const TDqTaskSettings& task)
    : SinksCount(GetSinksCount(task))
{}

TDqComputeActorCheckpoints::TPendingCheckpointBase& TDqComputeActorCheckpoints::TPendingCheckpointBase::operator=(const NDqProto::TCheckpoint& checkpoint) {
    Y_ABORT_UNLESS(!Checkpoint);
    Checkpoint = checkpoint;
    CheckpointStartTime = TActivationContext::Now();
    return *this;
}

TDqComputeActorCheckpoints::TPendingCheckpointBase::operator bool() const {
    return Checkpoint.Defined();
}

bool TDqComputeActorCheckpoints::TPendingCheckpointBase::IsSlowCheckpoint(TString& diagnostics) const {
    if (!CheckpointStartTime) {
        return false;
    }

    const auto duration = TActivationContext::Now() - CheckpointStartTime;
    if (duration < SLOW_CHECKPOINT_DURATION) {
        return false;
    }

    TStringBuilder checkpointDiagnostic;

    if (Checkpoint) {
        checkpointDiagnostic << "[Checkpoint " << MakeStringForLog(*Checkpoint) << "] ";
    }

    checkpointDiagnostic << "Slow checkpoint. Duration: " << duration.Seconds() << 's';

    if (const auto& info = GetDiagnostics()) {
        checkpointDiagnostic << " " << info;
    }

    diagnostics = checkpointDiagnostic;
    return true;
}

void TDqComputeActorCheckpoints::TPendingCheckpointBase::Clear() {
    Checkpoint = Nothing();
    ProcessedSinksCount = 0;
    CheckpointStartTime = TInstant::Zero();
}

bool TDqComputeActorCheckpoints::TPendingCheckpointBase::IsReady() const {
    Y_ABORT_UNLESS(Checkpoint);
    return ProcessedSinksCount == SinksCount;
}

TString TDqComputeActorCheckpoints::TPendingCheckpointBase::GetDiagnostics() const {
    if (!SinksCount || !Checkpoint) {
        return "";
    }

    return TStringBuilder() << "Sinks: " << ProcessedSinksCount << '/' << SinksCount;
}

size_t TDqComputeActorCheckpoints::TPendingCheckpointBase::GetSinksCount(const TDqTaskSettings& task) {
    size_t sinksCount = 0;
    for (size_t outputIndex = 0, outputsCount = task.OutputsSize(); outputIndex < outputsCount; ++outputIndex) {
        if (task.GetOutputs(outputIndex).HasSink()) {
            ++sinksCount;
        }
    }

    return sinksCount;
}

//// TPendingStateSavingCheckpoint

void TDqComputeActorCheckpoints::TPendingStateSavingCheckpoint::Clear() {
    TBase::Clear();
    SavedComputeActorState = false;
    SavingToDatabase = false;
    ComputeActorState.Clear();
}

bool TDqComputeActorCheckpoints::TPendingStateSavingCheckpoint::IsReady() const {
    return SavedComputeActorState && TBase::IsReady();
}

TString TDqComputeActorCheckpoints::TPendingStateSavingCheckpoint::GetDiagnostics() const {
    TStringBuilder result;

    if (Checkpoint) {
        result << "CA: " << SavedComputeActorState;

        if (const auto& diagnostics = TBase::GetDiagnostics()) {
            result << " " << diagnostics;
        }

        result << " ";
    }

    return result << "SavingToDatabase: " << SavingToDatabase;
}

//// TPendingCommitCheckpoint

void TDqComputeActorCheckpoints::TPendingCommitCheckpoint::Clear() {
    TBase::Clear();
    Cookie = 0;
    CommittedOutputs.clear();
}

//// TDqComputeActorCheckpoints

TDqComputeActorCheckpoints::TDqComputeActorCheckpoints(const NActors::TActorId& owner, const TTxId& txId, TDqTaskSettings task, ICallbacks* computeActor)
    : TActor(&TDqComputeActorCheckpoints::StateFunc)
    , Owner(owner)
    , TxId(txId)
    , Task(std::move(task))
    , IngressTask(IsIngress(Task))
    , CheckpointStorage(MakeCheckpointStorageID())
    , ComputeActor(computeActor)
    , PendingSaveStateCheckpoint(Task)
    , PendingCommitCheckpoint(Task)
{}

void TDqComputeActorCheckpoints::Init(NActors::TActorId computeActorId, NActors::TActorId checkpointsId) {
    EventsQueue.Init(TxId, computeActorId, checkpointsId);
}

STRICT_STFUNC_EXC(TDqComputeActorCheckpoints::StateFunc,
    hFunc(TEvDqCompute::TEvNewCheckpointCoordinator, Handle);
    hFunc(TEvDqCompute::TEvInjectCheckpoint, Handle);
    hFunc(TEvDqCompute::TEvSaveTaskStateResult, Handle);
    hFunc(TEvDqCompute::TEvCommitState, Handle);
    hFunc(TEvDqCompute::TEvRestoreFromCheckpoint, Handle);
    hFunc(TEvDqCompute::TEvGetTaskStateResult, Handle);
    hFunc(TEvDqCompute::TEvRun, Handle);
    hFunc(NActors::TEvInterconnect::TEvNodeDisconnected, Handle);
    hFunc(NActors::TEvInterconnect::TEvNodeConnected, Handle);
    hFunc(NActors::TEvents::TEvUndelivered, Handle);
    hFunc(TEvRetryQueuePrivate::TEvRetry, Handle);
    hFunc(TEvents::TEvWakeup, Handle);
    cFunc(TEvents::TEvPoisonPill::EventType, PassAway);,
    ExceptionFunc(std::exception, HandleException)
)

void TDqComputeActorCheckpoints::HandleException(const std::exception& err) {
    NYql::TIssues issues;
    issues.AddIssue(err.what());
    Send(Owner, NYql::NDq::TEvDq::TEvAbortExecution::InternalError("Internal error in checkpointing", issues));
}

namespace {

// Get generation for protobuf event.
template <class E>
auto GetGeneration(const E& ev) -> decltype(ev->Get()->Record.GetGeneration()) {
    return ev->Get()->Record.GetGeneration();
}

// Get generation for local event.
template <class E>
auto GetGeneration(const E& ev) -> decltype(ev->Get()->Generation) {
    return ev->Get()->Generation;
}

ui64 GetGeneration(const TEvDqCompute::TEvSaveTaskStateResult::TPtr& ev) {
    return ev->Get()->Record.GetCheckpoint().GetGeneration();
}

} // anonymous namespace

template <class E>
bool TDqComputeActorCheckpoints::ShouldIgnoreOldCoordinator(const E& ev, bool verifyOnGenerationFromFuture) {
    const ui64 generation = GetGeneration(ev);
    Y_ABORT_UNLESS(!verifyOnGenerationFromFuture || !CheckpointCoordinator || generation <= CheckpointCoordinator->Generation,
        "Got incorrect checkpoint coordinator generation: %lu > %lu", generation, CheckpointCoordinator->Generation);
    if (CheckpointCoordinator && generation < CheckpointCoordinator->Generation) {
        LOG_W("Ignoring event " << ev->Get()->ToStringHeader() << " from previous coordinator: "
            << generation << " < " << CheckpointCoordinator->Generation);
        return true;
    }
    return false;
}

void TDqComputeActorCheckpoints::Handle(TEvDqCompute::TEvNewCheckpointCoordinator::TPtr& ev) {
    if (ShouldIgnoreOldCoordinator(ev, /* verifyOnGenerationFromFuture */ false)) {
        return;
    }
    const ui64 newGeneration = ev->Get()->Record.GetGeneration();
    LOG_D("Got TEvNewCheckpointCoordinator event: generation " << newGeneration << ", actorId: " << ev->Sender);

    if (CheckpointCoordinator && CheckpointCoordinator->Generation == newGeneration) { // The same message. It was retry from coordinator.
        Y_ABORT_UNLESS(CheckpointCoordinator->ActorId == ev->Sender, "there shouldn't be two different checkpoint coordinators with the same generation");
        Y_ABORT_UNLESS(GraphId == ev->Get()->Record.GetGraphId());
        return;
    }

    if (CheckpointCoordinator) {
        LOG_T("Replace stale checkpoint coordinator (generation = " << CheckpointCoordinator->Generation << ") with a new one");
    } else {
        LOG_T("Assign checkpoint coordinator (generation = " << newGeneration << ")");
    }

    CheckpointCoordinator = TCheckpointCoordinatorId(ev->Sender, newGeneration);
    GraphId = ev->Get()->Record.GetGraphId();

    EventsQueue.OnNewRecipientId(ev->Sender);
    Y_ABORT_UNLESS(EventsQueue.OnEventReceived(ev->Get()));
    EventsQueue.Send(new TEvDqCompute::TEvNewCheckpointCoordinatorAck());

    if (PendingCommitCheckpoint) {
        LOG_CP_W(*PendingCommitCheckpoint.Checkpoint, "Drop pending commit checkpoint since coordinator is stale");
    }

    const bool resumeInputs = PendingSaveStateCheckpoint;
    if (resumeInputs) {
        LOG_PCP_W("Drop pending save state checkpoint since coordinator is stale");
    }

    PendingCommitCheckpoint.Clear();
    PendingSaveStateCheckpoint.Clear();

    if (resumeInputs) {
        ComputeActor->ResumeInputsByCheckpoint();
    }
}

void TDqComputeActorCheckpoints::Handle(TEvDqCompute::TEvInjectCheckpoint::TPtr& ev) {
    if (ShouldIgnoreOldCoordinator(ev)) {
        return;
    }

    if (!EventsQueue.OnEventReceived(ev)) {
        return;
    }

    YQL_ENSURE(IngressTask, "Shouldn't inject barriers into non-ingress tasks");
    YQL_ENSURE(!PendingSaveStateCheckpoint);

    StartCheckpoint(ev->Get()->Record.GetCheckpoint());
    LOG_PCP_D("TEvInjectCheckpoint");
    ComputeActor->ResumeExecution(EResumeSource::CheckpointInject);
}

void TDqComputeActorCheckpoints::Handle(TEvDqCompute::TEvSaveTaskStateResult::TPtr& ev) {
    if (ShouldIgnoreOldCoordinator(ev)) {
        return;
    }

    PendingSaveStateCheckpoint.SavingToDatabase = false;
    PendingSaveStateCheckpoint.CheckpointStartTime = TInstant::Zero();
    EventsQueue.Send(ev->Release().Release(), ev->Cookie);
}

void TDqComputeActorCheckpoints::Handle(TEvDqCompute::TEvRestoreFromCheckpoint::TPtr& ev) {
    if (ShouldIgnoreOldCoordinator(ev)) {
        return;
    }

    if (!EventsQueue.OnEventReceived(ev)) {
        return;
    }

    ComputeActor->Stop();
    StateLoadPlan = ev->Get()->Record.GetStateLoadPlan();
    const auto& checkpoint = ev->Get()->Record.GetCheckpoint();
    LOG_CP_D(checkpoint, "TEvRestoreFromCheckpoint, StateLoadPlan = " << StateLoadPlan);
    switch (StateLoadPlan.GetStateType()) {
    case NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY:
        {
            EventsQueue.Send(MakeHolder<TEvDqCompute::TEvRestoreFromCheckpointResult>(checkpoint, Task.GetId(), NDqProto::TEvRestoreFromCheckpointResult::OK, NYql::TIssues{}));
            break;
        }
    case NDqProto::NDqStateLoadPlan::STATE_TYPE_OWN:
        {
            Send(
                CheckpointStorage,
                new TEvDqCompute::TEvGetTaskState(
                    GraphId,
                    {Task.GetId()},
                    ev->Get()->Record.GetCheckpoint(),
                    CheckpointCoordinator->Generation));
            break;
        }
    case NDqProto::NDqStateLoadPlan::STATE_TYPE_FOREIGN:
        {
            Send(
                CheckpointStorage,
                new TEvDqCompute::TEvGetTaskState(
                    GraphId,
                    TaskIdsFromLoadPlan(StateLoadPlan),
                    ev->Get()->Record.GetCheckpoint(),
                    CheckpointCoordinator->Generation));
            break;
        }
    default:
        {
            auto message = TStringBuilder() << "Unsupported state type: "
                  << NDqProto::NDqStateLoadPlan::EStateType_Name(StateLoadPlan.GetStateType()) << " (" << static_cast<int>(StateLoadPlan.GetStateType()) << ")";
            LOG_CP_E(checkpoint, message);
            NYql::TIssues issues;
            issues.AddIssue(message);
            EventsQueue.Send(MakeHolder<TEvDqCompute::TEvRestoreFromCheckpointResult>(checkpoint, Task.GetId(), NDqProto::TEvRestoreFromCheckpointResult::INTERNAL_ERROR, issues));
            break;
        }
    }
}

void TDqComputeActorCheckpoints::Handle(TEvDqCompute::TEvGetTaskStateResult::TPtr& ev) {
    if (ShouldIgnoreOldCoordinator(ev)) {
        return;
    }

    auto& checkpoint = ev->Get()->Checkpoint;
    std::vector<ui64> taskIds;
    size_t taskIdsSize = 1;
    if (StateLoadPlan.GetStateType() == NDqProto::NDqStateLoadPlan::STATE_TYPE_FOREIGN) {
        taskIds = TaskIdsFromLoadPlan(StateLoadPlan);
        taskIdsSize = taskIds.size();
    }

    if (!ev->Get()->Issues.Empty()) {
        LOG_CP_E(checkpoint, "TEvGetTaskStateResult error: " << ev->Get()->Issues.ToOneLineString());
        EventsQueue.Send(MakeHolder<TEvDqCompute::TEvRestoreFromCheckpointResult>(checkpoint, Task.GetId(), NDqProto::TEvRestoreFromCheckpointResult::STORAGE_ERROR, ev->Get()->Issues), ev->Cookie);
        return;
    }

    if (ev->Get()->States.size() != taskIdsSize) {
        auto message = TStringBuilder() << "TEvGetTaskStateResult unexpected states count: " << ev->Get()->States.size() << ", expected: " << taskIdsSize;
        LOG_CP_E(checkpoint, message);
        NYql::TIssues issues;
        issues.AddIssue(message);
        EventsQueue.Send(MakeHolder<TEvDqCompute::TEvRestoreFromCheckpointResult>(checkpoint, Task.GetId(), NDqProto::TEvRestoreFromCheckpointResult::STORAGE_ERROR, issues), ev->Cookie);
        return;
    }

    LOG_CP_D(checkpoint, "TEvGetTaskStateResult: restoring state");
    RestoringTaskRunnerForCheckpoint = checkpoint;
    RestoringTaskRunnerForEvent = ev->Cookie;
    if (StateLoadPlan.GetStateType() == NDqProto::NDqStateLoadPlan::STATE_TYPE_OWN) {
        ComputeActor->LoadState(std::move(ev->Get()->States[0]));
    } else if (StateLoadPlan.GetStateType() == NDqProto::NDqStateLoadPlan::STATE_TYPE_FOREIGN) {
        TComputeActorState state = CombineForeignState(StateLoadPlan, ev->Get()->States, taskIds);
        ComputeActor->LoadState(std::move(state));
    } else {
        Y_ABORT("Unprocessed state type %s (%d)",
            NDqProto::NDqStateLoadPlan::EStateType_Name(StateLoadPlan.GetStateType()).c_str(),
            static_cast<int>(StateLoadPlan.GetStateType()));
    }
}

void TDqComputeActorCheckpoints::AfterStateLoading(const TMaybe<TString>& error) {
    auto& checkpoint = RestoringTaskRunnerForCheckpoint;
    if (error.Defined()) {
        auto message = TStringBuilder() << "Failed to load state: " << error << ", ABORTED";
        LOG_CP_E(checkpoint, message);
        NYql::TIssues issues;
        issues.AddIssue(message);
        EventsQueue.Send(MakeHolder<TEvDqCompute::TEvRestoreFromCheckpointResult>(checkpoint, Task.GetId(), NDqProto::TEvRestoreFromCheckpointResult::INTERNAL_ERROR, issues), RestoringTaskRunnerForEvent);
        return;
    }
    EventsQueue.Send(MakeHolder<TEvDqCompute::TEvRestoreFromCheckpointResult>(checkpoint, Task.GetId(), NDqProto::TEvRestoreFromCheckpointResult::OK, NYql::TIssues{}), RestoringTaskRunnerForEvent);
    LOG_CP_D(checkpoint, "Checkpoint state restored");
}

void TDqComputeActorCheckpoints::Handle(TEvDqCompute::TEvRun::TPtr& ev) {
    EventsQueue.OnEventReceived(ev);
}

void TDqComputeActorCheckpoints::Handle(TEvDqCompute::TEvCommitState::TPtr& ev) {
    if (ShouldIgnoreOldCoordinator(ev)) {
        return;
    }

    if (!EventsQueue.OnEventReceived(ev)) {
        return;
    }

    YQL_ENSURE(!PendingCommitCheckpoint); // Parallel checkpoints is not supported now
    PendingCommitCheckpoint = ev->Get()->Record.GetCheckpoint();
    PendingCommitCheckpoint.Cookie = ev->Cookie;
    StartSlowCheckpointsMonitoring();
    LOG_CP_D(*PendingCommitCheckpoint.Checkpoint, "TEvCommitState");

    ComputeActor->CommitState(*PendingCommitCheckpoint.Checkpoint);

    if (PendingCommitCheckpoint) {
        TryToFinishPendingCommitCheckpoint();
    }
}

void TDqComputeActorCheckpoints::Handle(NActors::TEvents::TEvPoison::TPtr&) {
    LOG_I("Pass Away");
    PassAway();
}

void TDqComputeActorCheckpoints::Handle(NActors::TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
    LOG_D("Handle disconnected node " << ev->Get()->NodeId);
    EventsQueue.HandleNodeDisconnected(ev->Get()->NodeId);
}

void TDqComputeActorCheckpoints::Handle(NActors::TEvInterconnect::TEvNodeConnected::TPtr& ev) {
    LOG_D("Handle connected node " << ev->Get()->NodeId);
    EventsQueue.HandleNodeConnected(ev->Get()->NodeId);
}

void TDqComputeActorCheckpoints::Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
    LOG_D("Handle undelivered");
    if (EventsQueue.HandleUndelivered(ev) != NYql::NDq::TRetryEventsQueue::ESessionState::WrongSession) {
        LOG_E("TEvUndelivered: " << ev->Get()->SourceType);
    }
}

void TDqComputeActorCheckpoints::Handle(TEvRetryQueuePrivate::TEvRetry::TPtr& ev) {
    Y_UNUSED(ev);
    EventsQueue.Retry();
}

void TDqComputeActorCheckpoints::Handle(NActors::TEvents::TEvWakeup::TPtr&) {
    const auto buildDiagnostics = [task = &Task, ca = ComputeActor](const TPendingCheckpointBase& checkpoint, const TString& type) -> TString {
        TString diagnostics;
        if (!checkpoint.IsSlowCheckpoint(diagnostics)) {
            return "";
        }

        return TStringBuilder()
            << " Stage: " << task->GetStageId()
            << ". Channels version: " << task->GetDqChannelVersion()
            << ". Pending checkpoint type: " << type
            << ". " << diagnostics
            << ". Compute actor state diagnostics. " << ca->GetTaskDebugState();
    };

    if (const auto& diagnostics = buildDiagnostics(PendingSaveStateCheckpoint, "PendingSaveStateCheckpoint")) {
        LOG_W(diagnostics);
    }

    if (const auto& diagnostics = buildDiagnostics(PendingCommitCheckpoint, "PendingCommitCheckpoint")) {
        LOG_W(diagnostics);
    }

    Schedule(SLOW_CHECKPOINT_DURATION, new NActors::TEvents::TEvWakeup());
}

bool TDqComputeActorCheckpoints::HasPendingCheckpoint() const {
    return PendingSaveStateCheckpoint;
}

bool TDqComputeActorCheckpoints::ComputeActorStateSaved() const {
    return PendingSaveStateCheckpoint && PendingSaveStateCheckpoint.SavedComputeActorState;
}

NDqProto::TCheckpoint TDqComputeActorCheckpoints::GetPendingCheckpoint() const {
    Y_ABORT_UNLESS(PendingSaveStateCheckpoint);
    return *PendingSaveStateCheckpoint.Checkpoint;
}

void TDqComputeActorCheckpoints::DoCheckpoint() {
    Y_ABORT_UNLESS(CheckpointCoordinator);
    Y_ABORT_UNLESS(PendingSaveStateCheckpoint);

    LOG_PCP_D("Performing task checkpoint");
    if (SaveState()) {
        LOG_PCP_I("Injecting checkpoint barrier to outputs");
        ComputeActor->InjectBarrierToOutputs(*PendingSaveStateCheckpoint.Checkpoint);
        ComputeActor->ResumeInputsByCheckpoint();
        TryToSavePendingCheckpoint();
    }
}

[[nodiscard]]
bool TDqComputeActorCheckpoints::SaveState() {
    Y_ABORT_UNLESS(PendingSaveStateCheckpoint);

    try {
        Y_ABORT_UNLESS(!PendingSaveStateCheckpoint.SavedComputeActorState);
        PendingSaveStateCheckpoint.SavedComputeActorState = true;
        ComputeActor->SaveState(*PendingSaveStateCheckpoint.Checkpoint, PendingSaveStateCheckpoint.ComputeActorState);
    } catch (const std::exception& e) {
        LOG_PCP_E("Failed to save state: " << e.what());

        auto resultEv = MakeHolder<TEvDqCompute::TEvSaveTaskStateResult>();
        *resultEv->Record.MutableCheckpoint() = *PendingSaveStateCheckpoint.Checkpoint;
        resultEv->Record.SetTaskId(Task.GetId());
        resultEv->Record.SetStatus(NDqProto::TEvSaveTaskStateResult::INTERNAL_ERROR);
        EventsQueue.Send(std::move(resultEv));
        PendingSaveStateCheckpoint.Clear();

        return false;
    }

    LOG_PCP_T("CA state saved");
    return true;
}

void TDqComputeActorCheckpoints::RegisterCheckpoint(const NDqProto::TCheckpoint& checkpoint, ui64 channelId) {
    if (!PendingSaveStateCheckpoint) {
        StartCheckpoint(checkpoint);
    } else {
        YQL_ENSURE(PendingSaveStateCheckpoint.Checkpoint->GetGeneration() == checkpoint.GetGeneration());
        YQL_ENSURE(PendingSaveStateCheckpoint.Checkpoint->GetId() == checkpoint.GetId());
    }
    LOG_PCP_T("Got checkpoint barrier from channel " << channelId);
    ComputeActor->ResumeExecution(EResumeSource::CheckpointRegister);
}

void TDqComputeActorCheckpoints::StartCheckpoint(const NDqProto::TCheckpoint& checkpoint) {
    PendingSaveStateCheckpoint = checkpoint;
    PendingSaveStateCheckpoint.SavingToDatabase = false;
    StartSlowCheckpointsMonitoring();
}

void TDqComputeActorCheckpoints::StartSlowCheckpointsMonitoring() {
    if (!SlowCheckpointsMonitoringStarted) {
        SlowCheckpointsMonitoringStarted = true;
        Schedule(SLOW_CHECKPOINT_DURATION, new NActors::TEvents::TEvWakeup());
    }
}

void TDqComputeActorCheckpoints::OnSinkStateSaved(TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint) {
    Y_ABORT_UNLESS(CheckpointCoordinator);
    Y_ABORT_UNLESS(checkpoint.GetGeneration() <= CheckpointCoordinator->Generation);
    if (checkpoint.GetGeneration() < CheckpointCoordinator->Generation) {
        LOG_W("Ignoring sink[" << outputIndex << "] state saved event from previous coordinator: "
            << checkpoint.GetGeneration() << " < " << CheckpointCoordinator->Generation);
        return;
    }

    Y_ABORT_UNLESS(PendingSaveStateCheckpoint);
    Y_ABORT_UNLESS(PendingSaveStateCheckpoint.Checkpoint->GetId() == checkpoint.GetId(),
        "Expected pending checkpoint id %lu, but got %lu", PendingSaveStateCheckpoint.Checkpoint->GetId(), checkpoint.GetId());

    for (const TSinkState& sinkState : PendingSaveStateCheckpoint.ComputeActorState.Sinks) {
        Y_ABORT_UNLESS(sinkState.OutputIndex != outputIndex, "Double save sink[%lu] state", outputIndex);
    }

    state.OutputIndex = outputIndex; // Set index explicitly to avoid errors
    PendingSaveStateCheckpoint.ComputeActorState.Sinks.emplace_back(std::move(state));
    ++PendingSaveStateCheckpoint.ProcessedSinksCount;
    LOG_PCP_T("Sink[" << outputIndex << "] state saved");

    TryToSavePendingCheckpoint();
}

void TDqComputeActorCheckpoints::OnSinkStateCommitted(ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint) {
    Y_ABORT_UNLESS(CheckpointCoordinator);
    Y_ABORT_UNLESS(checkpoint.GetGeneration() <= CheckpointCoordinator->Generation); // It is ok to recommit state for previous checkpoints during restore

    if (!PendingCommitCheckpoint || checkpoint.GetGeneration() < PendingCommitCheckpoint.Checkpoint->GetGeneration()) {
        // Stale sink commit from previous coordinator
        Y_ABORT_UNLESS(checkpoint.GetGeneration() < CheckpointCoordinator->Generation);
        LOG_W("Ignoring sink[" << outputIndex << "] commit state event from previous coordinator: "
            << checkpoint.GetGeneration() << " < " << CheckpointCoordinator->Generation << " because pending checkpoint "
            << (PendingCommitCheckpoint ? TStringBuilder() << "already has generation " << PendingCommitCheckpoint.Checkpoint->GetGeneration() : TStringBuilder() << "is not set"));
        return;
    }

    Y_ABORT_UNLESS(PendingCommitCheckpoint.Checkpoint->GetId() == checkpoint.GetId(),
        "Expected pending commit checkpoint id %lu, but got %lu", PendingCommitCheckpoint.Checkpoint->GetId(), checkpoint.GetId());

    const auto [_, inserted] = PendingCommitCheckpoint.CommittedOutputs.emplace(outputIndex);
    Y_ABORT_UNLESS(inserted, "Double commit sink[%lu] state", outputIndex);

    ++PendingCommitCheckpoint.ProcessedSinksCount;
    LOG_CP_T(*PendingCommitCheckpoint.Checkpoint, "Sink[" << outputIndex << "] state committed");

    TryToFinishPendingCommitCheckpoint();
}

void TDqComputeActorCheckpoints::OnTransformStateSaved(TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint) {
    Y_UNUSED(state, outputIndex, checkpoint); // Note that we can have both sink and transform on one output index
    Y_ABORT("Transform states are unimplemented");
}

void TDqComputeActorCheckpoints::OnTransformStateCommitted(ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint) {
    Y_UNUSED(outputIndex, checkpoint);
    Y_ABORT("Transform states are unimplemented");
}

void TDqComputeActorCheckpoints::TryToSavePendingCheckpoint() {
    Y_ABORT_UNLESS(PendingSaveStateCheckpoint);
    if (PendingSaveStateCheckpoint.IsReady()) {
        auto saveTaskStateRequest = MakeHolder<TEvDqCompute::TEvSaveTaskState>(GraphId, Task.GetId(), *PendingSaveStateCheckpoint.Checkpoint);
        saveTaskStateRequest->State = std::move(PendingSaveStateCheckpoint.ComputeActorState);
        Send(CheckpointStorage, std::move(saveTaskStateRequest));

        LOG_PCP_D("Task checkpoint is done. Send to storage");
        const auto startTime = PendingSaveStateCheckpoint.CheckpointStartTime;
        PendingSaveStateCheckpoint.Clear();
        PendingSaveStateCheckpoint.CheckpointStartTime = startTime;
        PendingSaveStateCheckpoint.SavingToDatabase = true;
    }
}

void TDqComputeActorCheckpoints::TryToFinishPendingCommitCheckpoint() {
    Y_ABORT_UNLESS(PendingCommitCheckpoint);
    if (PendingCommitCheckpoint.IsReady()) {
        const auto& checkpoint = *PendingCommitCheckpoint.Checkpoint;
        EventsQueue.Send(new TEvDqCompute::TEvStateCommitted(checkpoint.GetId(), checkpoint.GetGeneration(), Task.GetId()), PendingCommitCheckpoint.Cookie);

        LOG_CP_D(*PendingCommitCheckpoint.Checkpoint, "Task checkpoint commit done.");
        PendingCommitCheckpoint.Clear();
    }
}

void TDqComputeActorCheckpoints::PassAway() {
    EventsQueue.Unsubscribe();
    NActors::TActor<TDqComputeActorCheckpoints>::PassAway();
}

static bool IsInfiniteSourceType(const TString& sourceType) {
    return sourceType == PqSource;
}

NDqProto::ECheckpointingMode GetTaskCheckpointingMode(const TDqTaskSettings& task) {
    for (const auto& input : task.GetInputs()) {
        if (const TString& srcType = input.GetSource().GetType(); srcType && IsInfiniteSourceType(srcType)) {
            return NDqProto::CHECKPOINTING_MODE_DEFAULT;
        }
        for (const auto& channel : input.GetChannels()) {
            if (channel.GetCheckpointingMode() != NDqProto::CHECKPOINTING_MODE_DISABLED) {
                return NDqProto::CHECKPOINTING_MODE_DEFAULT;
            }
        }
    }
    return NDqProto::CHECKPOINTING_MODE_DISABLED;
}

bool IsIngress(const TDqTaskSettings& task) {
    // No inputs at all or there is no input channels with checkpoints.
    // We don't want to inject checkpoint into tasks that has checkpointed input channels,
    // otherwise task can be checkpointed twice;
    // once checkpoint will arrive from channels, it will pause reading from sources too.

    const auto& inputs = task.GetInputs();
    if (inputs.empty()) {
        return true;
    }

    bool hasSource = false;
    for (const auto& input : inputs) {
        if (input.HasSource()) {
            hasSource = true;
            continue;
        }

        for (const auto& channel : input.GetChannels()) {
            if (channel.GetCheckpointingMode() != NDqProto::CHECKPOINTING_MODE_DISABLED) {
                return false;
            }
        }
    }

    return hasSource;
}

bool IsEgress(const TDqTaskSettings& task) {
    for (const auto& output : task.GetOutputs()) {
        if (output.HasSink()) {
            return true;
        }
    }
    return false;
}

bool HasState(const TDqTaskSettings& task) {
    Y_UNUSED(task);
    return true;
}

} // namespace NYql::NDq
