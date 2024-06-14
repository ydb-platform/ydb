#include "dq_compute_actor_checkpoints.h"
#include "dq_checkpoints.h"
#include "dq_compute_actor_impl.h"
#include <ydb/library/services/services.pb.h>

#include <ydb/library/yql/minikql/comp_nodes/mkql_saveload.h>

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
    LOG_CP_T(*PendingCheckpoint.Checkpoint, s)
#define LOG_PCP_D(s) \
    LOG_CP_D(*PendingCheckpoint.Checkpoint, s)
#define LOG_PCP_I(s) \
    LOG_CP_I(*PendingCheckpoint.Checkpoint, s)
#define LOG_PCP_W(s) \
    LOG_CP_W(*PendingCheckpoint.Checkpoint, s)
#define LOG_PCP_E(s) \
    LOG_CP_E(*PendingCheckpoint.Checkpoint, s)

namespace NYql::NDq {

using namespace NActors;

namespace {

constexpr TDuration SLOW_CHECKPOINT_DURATION = TDuration::Minutes(1);

TString MakeStringForLog(const NDqProto::TCheckpoint& checkpoint) {
    return TStringBuilder() << checkpoint.GetGeneration() << "." << checkpoint.GetId();
}

bool IsIngressTask(const TDqTaskSettings& task) {
    for (const auto& input : task.GetInputs()) {
        if (!input.HasSource()) {
            return false;
        }
    }
    return true;
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

TDqComputeActorCheckpoints::TDqComputeActorCheckpoints(const NActors::TActorId& owner, const TTxId& txId, TDqTaskSettings task, ICallbacks* computeActor)
    : TActor(&TDqComputeActorCheckpoints::StateFunc)
    , Owner(owner)
    , TxId(txId)
    , Task(std::move(task))
    , IngressTask(IsIngressTask(Task))
    , CheckpointStorage(MakeCheckpointStorageID())
    , ComputeActor(computeActor)
    , PendingCheckpoint(Task)
{
}

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
    if (ShouldIgnoreOldCoordinator(ev, false)) {
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

    const bool resumeInputs = bool(PendingCheckpoint);
    AbortCheckpoint();
    if (resumeInputs) {
        LOG_W("Drop pending checkpoint since coordinator is stale");
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
    YQL_ENSURE(!PendingCheckpoint);

    StartCheckpoint(ev->Get()->Record.GetCheckpoint());
    LOG_PCP_D("TEvInjectCheckpoint");
    ComputeActor->ResumeExecution(EResumeSource::CheckpointInject);
}

void TDqComputeActorCheckpoints::Handle(TEvDqCompute::TEvSaveTaskStateResult::TPtr& ev) {
    if (ShouldIgnoreOldCoordinator(ev)) {
        return;
    }

    SavingToDatabase = false;
    CheckpointStartTime = TInstant::Zero();
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

    // No actual commit at the moment: will be done in further commits
    auto checkpoint = ev->Get()->Record.GetCheckpoint();
    ComputeActor->CommitState(checkpoint);
    EventsQueue.Send(new TEvDqCompute::TEvStateCommitted(checkpoint.GetId(), checkpoint.GetGeneration(), Task.GetId()), ev->Cookie);
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
    if (!EventsQueue.HandleUndelivered(ev)) {
        LOG_E("TEvUndelivered: " << ev->Get()->SourceType);
    }
}

void TDqComputeActorCheckpoints::Handle(TEvRetryQueuePrivate::TEvRetry::TPtr& ev) {
    Y_UNUSED(ev);
    EventsQueue.Retry();
}

void TDqComputeActorCheckpoints::Handle(NActors::TEvents::TEvWakeup::TPtr&) {
    if (CheckpointStartTime && (TActivationContext::Now() - CheckpointStartTime) >= SLOW_CHECKPOINT_DURATION) {
        TStringBuilder checkpointDiagnostic;
        if (PendingCheckpoint.Checkpoint) {
            checkpointDiagnostic << "[Checkpoint " << MakeStringForLog(*PendingCheckpoint.Checkpoint) << "] ";
        }
        checkpointDiagnostic << "Slow checkpoint. Duration: " << (TInstant::Now() - CheckpointStartTime).Seconds() << 's';
        if (PendingCheckpoint) {
            checkpointDiagnostic << " CA: " << PendingCheckpoint.SavedComputeActorState;
            if (PendingCheckpoint.SinksCount) {
                checkpointDiagnostic << " Sinks: " << PendingCheckpoint.SavedSinkStatesCount << '/' << PendingCheckpoint.SinksCount;
            }
        }
        checkpointDiagnostic << " SavingToDatabase: " << SavingToDatabase;
        LOG_W(checkpointDiagnostic);
    }
    Schedule(SLOW_CHECKPOINT_DURATION, new NActors::TEvents::TEvWakeup());
}

bool TDqComputeActorCheckpoints::HasPendingCheckpoint() const {
    return PendingCheckpoint;
}

bool TDqComputeActorCheckpoints::ComputeActorStateSaved() const {
    return PendingCheckpoint && PendingCheckpoint.SavedComputeActorState;
}

NDqProto::TCheckpoint TDqComputeActorCheckpoints::GetPendingCheckpoint() const {
    Y_ABORT_UNLESS(PendingCheckpoint);
    return *PendingCheckpoint.Checkpoint;
}

void TDqComputeActorCheckpoints::DoCheckpoint() {
    Y_ABORT_UNLESS(CheckpointCoordinator);
    Y_ABORT_UNLESS(PendingCheckpoint);

    LOG_PCP_D("Performing task checkpoint");
    if (SaveState()) {
        LOG_PCP_T("Injecting checkpoint barrier to outputs");
        ComputeActor->InjectBarrierToOutputs(*PendingCheckpoint.Checkpoint);
        TryToSavePendingCheckpoint();
    }
}

[[nodiscard]]
bool TDqComputeActorCheckpoints::SaveState() {
    try {
        Y_ABORT_UNLESS(!PendingCheckpoint.SavedComputeActorState);
        PendingCheckpoint.SavedComputeActorState = true;
        ComputeActor->SaveState(*PendingCheckpoint.Checkpoint, PendingCheckpoint.ComputeActorState);
    } catch (const std::exception& e) {
        AbortCheckpoint();
        LOG_PCP_E("Failed to save state: " << e.what());

        auto resultEv = MakeHolder<TEvDqCompute::TEvSaveTaskStateResult>();
        resultEv->Record.MutableCheckpoint()->CopyFrom(*PendingCheckpoint.Checkpoint);
        resultEv->Record.SetTaskId(Task.GetId());
        resultEv->Record.SetStatus(NDqProto::TEvSaveTaskStateResult::INTERNAL_ERROR);
        EventsQueue.Send(std::move(resultEv));

        return false;
    }

    LOG_PCP_T("CA state saved");
    return true;
}

void TDqComputeActorCheckpoints::RegisterCheckpoint(const NDqProto::TCheckpoint& checkpoint, ui64 channelId) {
    if (!PendingCheckpoint) {
        StartCheckpoint(checkpoint);
    } else {
        YQL_ENSURE(PendingCheckpoint.Checkpoint->GetGeneration() == checkpoint.GetGeneration());
        YQL_ENSURE(PendingCheckpoint.Checkpoint->GetId() == checkpoint.GetId());
    }
    LOG_PCP_T("Got checkpoint barrier from channel " << channelId);
    ComputeActor->ResumeExecution(EResumeSource::CheckpointRegister);
}

void TDqComputeActorCheckpoints::StartCheckpoint(const NDqProto::TCheckpoint& checkpoint) {
    PendingCheckpoint = checkpoint;
    CheckpointStartTime = TActivationContext::Now();
    SavingToDatabase = false;

    if (!SlowCheckpointsMonitoringStarted) {
        SlowCheckpointsMonitoringStarted = true;
        Schedule(SLOW_CHECKPOINT_DURATION, new NActors::TEvents::TEvWakeup());
    }
}

void TDqComputeActorCheckpoints::AbortCheckpoint() {
    PendingCheckpoint.Clear();
    CheckpointStartTime = TInstant::Zero();
    SavingToDatabase = false;
}

void TDqComputeActorCheckpoints::OnSinkStateSaved(TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint) {
    Y_ABORT_UNLESS(CheckpointCoordinator);
    Y_ABORT_UNLESS(checkpoint.GetGeneration() <= CheckpointCoordinator->Generation);
    if (checkpoint.GetGeneration() < CheckpointCoordinator->Generation) {
        LOG_W("Ignoring sink[" << outputIndex << "] state saved event from previous coordinator: "
            << checkpoint.GetGeneration() << " < " << CheckpointCoordinator->Generation);
        return;
    }
    Y_ABORT_UNLESS(PendingCheckpoint);
    Y_ABORT_UNLESS(PendingCheckpoint.Checkpoint->GetId() == checkpoint.GetId(),
        "Expected pending checkpoint id %lu, but got %lu", PendingCheckpoint.Checkpoint->GetId(), checkpoint.GetId());
    for (const TSinkState& sinkState : PendingCheckpoint.ComputeActorState.Sinks) {
        Y_ABORT_UNLESS(sinkState.OutputIndex != outputIndex, "Double save sink[%lu] state", outputIndex);
    }

    state.OutputIndex = outputIndex; // Set index explicitly to avoid errors
    PendingCheckpoint.ComputeActorState.Sinks.emplace_back(std::move(state));
    ++PendingCheckpoint.SavedSinkStatesCount;
    LOG_T("Sink[" << outputIndex << "] state saved");

    TryToSavePendingCheckpoint();
}

void TDqComputeActorCheckpoints::TryToSavePendingCheckpoint() {
    Y_ABORT_UNLESS(PendingCheckpoint);
    if (PendingCheckpoint.IsReady()) {
        auto saveTaskStateRequest = MakeHolder<TEvDqCompute::TEvSaveTaskState>(GraphId, Task.GetId(), *PendingCheckpoint.Checkpoint);
        saveTaskStateRequest->State = std::move(PendingCheckpoint.ComputeActorState);
        Send(CheckpointStorage, std::move(saveTaskStateRequest));

        LOG_PCP_D("Task checkpoint is done. Send to storage");
        PendingCheckpoint.Clear();
        SavingToDatabase = true;
    }
}

TDqComputeActorCheckpoints::TPendingCheckpoint& TDqComputeActorCheckpoints::TPendingCheckpoint::operator=(const NDqProto::TCheckpoint& checkpoint) {
    Y_ABORT_UNLESS(!Checkpoint);
    Checkpoint = checkpoint;
    return *this;
}

void TDqComputeActorCheckpoints::TPendingCheckpoint::Clear() {
    Checkpoint = Nothing();
    SavedComputeActorState = false;
    SavedSinkStatesCount = 0;
    ComputeActorState.Clear();
}

size_t TDqComputeActorCheckpoints::TPendingCheckpoint::GetSinksCount(const TDqTaskSettings& task) {
    size_t sinksCount = 0;
    for (int outputIndex = 0, outputsCount = task.OutputsSize(); outputIndex < outputsCount; ++outputIndex) {
        if (task.GetOutputs(outputIndex).HasSink()) {
            ++sinksCount;
        }
    }
    return sinksCount;
}

void TDqComputeActorCheckpoints::PassAway() {
    EventsQueue.Unsubscribe();
    NActors::TActor<TDqComputeActorCheckpoints>::PassAway();
}

static bool IsInfiniteSourceType(const TString& sourceType) {
    return sourceType == "PqSource";
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

} // namespace NYql::NDq
