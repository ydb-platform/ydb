#include "dq_compute_actor_checkpoints.h" 
#include "dq_checkpoints.h" 
#include "dq_compute_actor_impl.h"
 
#include <ydb/library/yql/minikql/comp_nodes/mkql_saveload.h>

#include <algorithm>

#define LOG_D(s) \ 
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "[" << GraphId << "] Task: " << Task.GetId() << ". " << s)
#define LOG_I(s) \ 
    LOG_INFO_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, "[" << GraphId << "] Task: " << Task.GetId() << ". " << s)
#define LOG_W(s) \ 
    LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "[" << GraphId << "] Task: " << Task.GetId() << ". " << s)
#define LOG_E(s) \ 
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "[" << GraphId << "] Task: " << Task.GetId() << ". " << s)
 
#define LOG_CP_D(s) \ 
    LOG_D("[Checkpoint " << MakeStringForLog(*PendingCheckpoint.Checkpoint) << "] " << s)
#define LOG_CP_I(s) \ 
     LOG_I("[Checkpoint " << MakeStringForLog(*PendingCheckpoint.Checkpoint) << "] " << s)
#define LOG_CP_E(s) \ 
    LOG_E("[Checkpoint " << MakeStringForLog(*PendingCheckpoint.Checkpoint) << "] " << s)
 
namespace NYql::NDq {
 
using namespace NActors;

namespace {

TString MakeStringForLog(const NDqProto::TCheckpoint& checkpoint) {
    return TStringBuilder() << checkpoint.GetGeneration() << "." << checkpoint.GetId();
} 
 
bool IsIngressTask(const NDqProto::TDqTask& task) {
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

const NDqProto::TSourceState& FindSourceState(
    const NDqProto::NDqStateLoadPlan::TSourcePlan::TForeignTaskSource& foreignTaskSource,
    const std::vector<NDqProto::TComputeActorState>& states,
    const std::vector<ui64>& taskIds)
{
    // Find state index
    const auto stateIndexIt = std::lower_bound(taskIds.begin(), taskIds.end(), foreignTaskSource.GetTaskId());
    YQL_ENSURE(stateIndexIt != taskIds.end(), "Task id was not found in plan");
    const size_t stateIndex = std::distance(taskIds.begin(), stateIndexIt);
    const NDqProto::TComputeActorState& state = states[stateIndex];
    for (const NDqProto::TSourceState& sourceState : state.GetSources()) {
        if (sourceState.GetInputIndex() == foreignTaskSource.GetInputIndex()) {
            return sourceState;
        }
    }
    YQL_ENSURE(false, "Source input index " << foreignTaskSource.GetInputIndex() << " was not found in state");
    // Make compiler happy
    return state.GetSources(0);
}

NDqProto::TComputeActorState CombineForeignState(
    const NDqProto::NDqStateLoadPlan::TTaskPlan& plan,
    const std::vector<NDqProto::TComputeActorState>& states,
    const std::vector<ui64>& taskIds)
{
    NDqProto::TComputeActorState state;
    state.MutableMiniKqlProgram()->MutableData()->MutableStateData()->SetVersion(ComputeActorCurrentStateVersion);
    YQL_ENSURE(plan.GetProgram().GetStateType() == NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY, "Unsupported program state type. Plan: " << plan);
    for (const auto& sinkPlan : plan.GetSinks()) {
        YQL_ENSURE(sinkPlan.GetStateType() == NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY, "Unsupported sink state type. Plan: " << sinkPlan);
    }
    for (const auto& sourcePlan : plan.GetSources()) {
        YQL_ENSURE(sourcePlan.GetStateType() == NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY || sourcePlan.GetStateType() == NDqProto::NDqStateLoadPlan::STATE_TYPE_FOREIGN, "Unsupported sink state type. Plan: " << sourcePlan);
        if (sourcePlan.GetStateType() == NDqProto::NDqStateLoadPlan::STATE_TYPE_FOREIGN) {
            auto& sourceState = *state.AddSources();
            sourceState.SetInputIndex(sourcePlan.GetInputIndex());
            for (const auto& foreignTaskSource : sourcePlan.GetForeignTasksSources()) {
                const NDqProto::TSourceState& srcSourceState = FindSourceState(foreignTaskSource, states, taskIds);
                for (const NDqProto::TStateData& data : srcSourceState.GetData()) {
                    sourceState.AddData()->CopyFrom(data);
                }
            }
            YQL_ENSURE(sourceState.DataSize(), "No data was loaded to source " << sourcePlan.GetInputIndex());
        }
    }
    return state;
}

} // namespace

TDqComputeActorCheckpoints::TDqComputeActorCheckpoints(const TTxId& txId, NDqProto::TDqTask task, ICallbacks* computeActor)
    : TActor(&TDqComputeActorCheckpoints::StateFunc) 
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

STRICT_STFUNC(TDqComputeActorCheckpoints::StateFunc,
    hFunc(TEvDqCompute::TEvNewCheckpointCoordinator, Handle);
    hFunc(TEvDqCompute::TEvInjectCheckpoint, Handle);
    hFunc(TEvDqCompute::TEvSaveTaskStateResult, Handle);
    hFunc(TEvDqCompute::TEvCommitState, Handle);
    hFunc(TEvDqCompute::TEvRestoreFromCheckpoint, Handle);
    hFunc(TEvDqCompute::TEvGetTaskStateResult, Handle);
    hFunc(TEvDqCompute::TEvRun, Handle);
    hFunc(NActors::TEvInterconnect::TEvNodeDisconnected, Handle);
    hFunc(NActors::TEvInterconnect::TEvNodeConnected, Handle);
    hFunc(TEvRetryQueuePrivate::TEvRetry, Handle);
    cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
)
 
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
    Y_VERIFY(!verifyOnGenerationFromFuture || !CheckpointCoordinator || generation <= CheckpointCoordinator->Generation,
        "Got incorrect checkpoint coordinator generation: %lu > %lu", generation, CheckpointCoordinator->Generation);
    if (CheckpointCoordinator && generation < CheckpointCoordinator->Generation) {
        LOG_D("Ignoring event " << ev->Get()->ToStringHeader() << " from previous coordinator: "
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
    LOG_I("Got TEvNewCheckpointCoordinator event: generation " << newGeneration << ", actorId: " << ev->Sender); 
 
    if (CheckpointCoordinator && CheckpointCoordinator->Generation == newGeneration) { // The same message. It was retry from coordinator.
        Y_VERIFY(CheckpointCoordinator->ActorId == ev->Sender, "there shouldn't be two different checkpoint coordinators with the same generation"); 
        Y_VERIFY(GraphId == ev->Get()->Record.GetGraphId());
        return; 
    } 
 
    if (CheckpointCoordinator) {
        LOG_I("Replace stale checkpoint coordinator (generation = " << CheckpointCoordinator->Generation << ") with a new one");
    } else {
        LOG_I("Assign checkpoint coordinator (generation = " << newGeneration << ")");
    }

    CheckpointCoordinator = TCheckpointCoordinatorId(ev->Sender, newGeneration); 
    GraphId = ev->Get()->Record.GetGraphId();
 
    EventsQueue.OnNewRecipientId(ev->Sender);
    Y_VERIFY(EventsQueue.OnEventReceived(ev->Get()));
    EventsQueue.Send(new TEvDqCompute::TEvNewCheckpointCoordinatorAck());

    if (PendingCheckpoint) { 
        LOG_I("Drop pending checkpoint since coordinator is stale"); 
        PendingCheckpoint.Clear(); 
        ComputeActor->ResumeInputs(); 
    } 
} 
 
void TDqComputeActorCheckpoints::Handle(TEvDqCompute::TEvInjectCheckpoint::TPtr& ev) {
    if (ShouldIgnoreOldCoordinator(ev)) {
        return;
    }

    YQL_ENSURE(IngressTask, "Shouldn't inject barriers into non-ingress tasks");
    YQL_ENSURE(!PendingCheckpoint); 
 
    PendingCheckpoint = ev->Get()->Record.GetCheckpoint(); 
    LOG_CP_I("Got TEvInjectCheckpoint"); 
    ComputeActor->ResumeExecution(); 
} 
 
void TDqComputeActorCheckpoints::Handle(TEvDqCompute::TEvSaveTaskStateResult::TPtr& ev) {
    if (ShouldIgnoreOldCoordinator(ev)) {
        return; 
    } 

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
    TaskLoadPlan = ev->Get()->Record.GetStateLoadPlan();
    const auto& checkpoint = ev->Get()->Record.GetCheckpoint();
    LOG_I("[Checkpoint " << MakeStringForLog(checkpoint) << "] Got TEvRestoreFromCheckpoint event with plan " << TaskLoadPlan);
    switch (TaskLoadPlan.GetStateType()) {
    case NDqProto::NDqStateLoadPlan::STATE_TYPE_EMPTY:
        {
            LOG_I("[Checkpoint " << MakeStringForLog(checkpoint) << "] Restored from empty state");
            EventsQueue.Send(MakeHolder<TEvDqCompute::TEvRestoreFromCheckpointResult>(checkpoint, Task.GetId(), NDqProto::TEvRestoreFromCheckpointResult::OK));
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
                    TaskIdsFromLoadPlan(TaskLoadPlan),
                    ev->Get()->Record.GetCheckpoint(),
                    CheckpointCoordinator->Generation));
            break;
        }
    default:
        {
            LOG_E("[Checkpoint " << MakeStringForLog(checkpoint) << "] Unsupported state type: "
                  << NDqProto::NDqStateLoadPlan::EStateType_Name(TaskLoadPlan.GetStateType()) << " (" << static_cast<int>(TaskLoadPlan.GetStateType()) << ")");
            EventsQueue.Send(MakeHolder<TEvDqCompute::TEvRestoreFromCheckpointResult>(checkpoint, Task.GetId(), NDqProto::TEvRestoreFromCheckpointResult::INTERNAL_ERROR));
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
    if (TaskLoadPlan.GetStateType() == NDqProto::NDqStateLoadPlan::STATE_TYPE_FOREIGN) {
        taskIds = TaskIdsFromLoadPlan(TaskLoadPlan);
        taskIdsSize = taskIds.size();
    }
 
    if (!ev->Get()->Issues.Empty()) {
        LOG_E("[Checkpoint " << MakeStringForLog(checkpoint)
            << "] Can't get state from storage: " << ev->Get()->Issues.ToString());
        EventsQueue.Send(MakeHolder<TEvDqCompute::TEvRestoreFromCheckpointResult>(checkpoint, Task.GetId(), NDqProto::TEvRestoreFromCheckpointResult::STORAGE_ERROR), ev->Cookie);
        return;
    }
 
    if (ev->Get()->States.size() != taskIdsSize) {
        LOG_E("[Checkpoint " << MakeStringForLog(checkpoint)
            << "] Got unexpected states count. States count: " << ev->Get()->States.size()
            << ". Expected states count: " << taskIdsSize);
        EventsQueue.Send(MakeHolder<TEvDqCompute::TEvRestoreFromCheckpointResult>(checkpoint, Task.GetId(), NDqProto::TEvRestoreFromCheckpointResult::STORAGE_ERROR), ev->Cookie);
        return; 
    } 
 
    LOG_I("[Checkpoint " << MakeStringForLog(checkpoint) << "] Got TEvGetTaskStateResult event, restoring state");
    try { 
        if (TaskLoadPlan.GetStateType() == NDqProto::NDqStateLoadPlan::STATE_TYPE_OWN) {
            ComputeActor->LoadState(ev->Get()->States[0]);
        } else if (TaskLoadPlan.GetStateType() == NDqProto::NDqStateLoadPlan::STATE_TYPE_FOREIGN) {
            const NDqProto::TComputeActorState state = CombineForeignState(TaskLoadPlan, ev->Get()->States, taskIds);
            ComputeActor->LoadState(state);
        } else {
            Y_FAIL("Unprocessed state type %s (%d)",
                NDqProto::NDqStateLoadPlan::EStateType_Name(TaskLoadPlan.GetStateType()).c_str(),
                static_cast<int>(TaskLoadPlan.GetStateType()));
        }
    } catch (const std::exception& e) { 
        LOG_E("[Checkpoint " << MakeStringForLog(checkpoint) << "] Failed to load state: " << e.what());
        EventsQueue.Send(MakeHolder<TEvDqCompute::TEvRestoreFromCheckpointResult>(checkpoint, Task.GetId(), NDqProto::TEvRestoreFromCheckpointResult::INTERNAL_ERROR), ev->Cookie);
        LOG_I("[Checkpoint " << MakeStringForLog(checkpoint) << "] Checkpoint state restoration aborted");
        return; 
    } 
 
    EventsQueue.Send(MakeHolder<TEvDqCompute::TEvRestoreFromCheckpointResult>(checkpoint, Task.GetId(), NDqProto::TEvRestoreFromCheckpointResult::OK), ev->Cookie);
    LOG_I("[Checkpoint " << MakeStringForLog(checkpoint) << "] Checkpoint state restored");
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
    LOG_D("pass away"); 
    PassAway(); 
} 
 
void TDqComputeActorCheckpoints::Handle(NActors::TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
    LOG_I("Handle disconnected node " << ev->Get()->NodeId);
    EventsQueue.HandleNodeDisconnected(ev->Get()->NodeId);
}

void TDqComputeActorCheckpoints::Handle(NActors::TEvInterconnect::TEvNodeConnected::TPtr& ev) {
    LOG_D("Handle connected node " << ev->Get()->NodeId);
    EventsQueue.HandleNodeConnected(ev->Get()->NodeId);
}

void TDqComputeActorCheckpoints::Handle(TEvRetryQueuePrivate::TEvRetry::TPtr& ev) {
    Y_UNUSED(ev);
    EventsQueue.Retry();
}

bool TDqComputeActorCheckpoints::HasPendingCheckpoint() const { 
    return PendingCheckpoint;
} 
 
bool TDqComputeActorCheckpoints::ComputeActorStateSaved() const {
    return PendingCheckpoint && PendingCheckpoint.SavedComputeActorState;
}

void TDqComputeActorCheckpoints::DoCheckpoint() { 
    Y_VERIFY(CheckpointCoordinator); 
    Y_VERIFY(PendingCheckpoint);
 
    LOG_CP_I("Performing task checkpoint"); 
    if (SaveState()) { 
        LOG_CP_D("Injecting checkpoint barrier to outputs"); 
        ComputeActor->InjectBarrierToOutputs(*PendingCheckpoint.Checkpoint);
        TryToSavePendingCheckpoint();
    } 
} 
 
[[nodiscard]] 
bool TDqComputeActorCheckpoints::SaveState() { 
    LOG_CP_D("Saving task state"); 
 
    try { 
        Y_VERIFY(!PendingCheckpoint.SavedComputeActorState);
        PendingCheckpoint.SavedComputeActorState = true;
        ComputeActor->SaveState(*PendingCheckpoint.Checkpoint, PendingCheckpoint.ComputeActorState);
    } catch (const std::exception& e) { 
        PendingCheckpoint.Clear();
        LOG_CP_E("Failed to save state: " << e.what()); 
 
        auto resultEv = MakeHolder<TEvDqCompute::TEvSaveTaskStateResult>();
        resultEv->Record.MutableCheckpoint()->CopyFrom(*PendingCheckpoint.Checkpoint);
        resultEv->Record.SetTaskId(Task.GetId()); 
        resultEv->Record.SetStatus(NDqProto::TEvSaveTaskStateResult::INTERNAL_ERROR);
        EventsQueue.Send(std::move(resultEv));
 
        return false; 
    } 
 
    LOG_CP_D("Compute actor state saved");
    return true; 
} 
 
void TDqComputeActorCheckpoints::RegisterCheckpoint(const NDqProto::TCheckpoint& checkpoint, ui64 channelId) {
    if (!PendingCheckpoint) { 
        PendingCheckpoint = checkpoint; 
    } else { 
        YQL_ENSURE(PendingCheckpoint.Checkpoint->GetGeneration() == checkpoint.GetGeneration());
        YQL_ENSURE(PendingCheckpoint.Checkpoint->GetId() == checkpoint.GetId());
    } 
    LOG_CP_I("Got checkpoint barrier from channel " << channelId); 
    ComputeActor->ResumeExecution();
} 
 
void TDqComputeActorCheckpoints::OnSinkStateSaved(NDqProto::TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint) {
    Y_VERIFY(CheckpointCoordinator);
    Y_VERIFY(checkpoint.GetGeneration() <= CheckpointCoordinator->Generation);
    if (checkpoint.GetGeneration() < CheckpointCoordinator->Generation) {
        LOG_D("Ignoring sink[" << outputIndex << "] state saved event from previous coordinator: "
            << checkpoint.GetGeneration() << " < " << CheckpointCoordinator->Generation);
        return;
    }
    Y_VERIFY(PendingCheckpoint);
    Y_VERIFY(PendingCheckpoint.Checkpoint->GetId() == checkpoint.GetId(),
        "Expected pending checkpoint id %lu, but got %lu", PendingCheckpoint.Checkpoint->GetId(), checkpoint.GetId());
    for (const NDqProto::TSinkState& sinkState : PendingCheckpoint.ComputeActorState.GetSinks()) {
        Y_VERIFY(sinkState.GetOutputIndex() != outputIndex, "Double save sink[%lu] state", outputIndex);
    }

    NDqProto::TSinkState* sinkState = PendingCheckpoint.ComputeActorState.AddSinks();
    *sinkState = std::move(state);
    sinkState->SetOutputIndex(outputIndex); // Set index explicitly to avoid errors
    ++PendingCheckpoint.SavedSinkStatesCount;

    TryToSavePendingCheckpoint();
}

void TDqComputeActorCheckpoints::TryToSavePendingCheckpoint() {
    Y_VERIFY(PendingCheckpoint);
    if (PendingCheckpoint.IsReady()) {
        auto saveTaskStateRequest = MakeHolder<TEvDqCompute::TEvSaveTaskState>(GraphId, Task.GetId(), *PendingCheckpoint.Checkpoint);
        saveTaskStateRequest->State.Swap(&PendingCheckpoint.ComputeActorState);
        Send(CheckpointStorage, std::move(saveTaskStateRequest));

        LOG_CP_I("Task checkpoint is done");
        PendingCheckpoint.Clear();
    }
}

TDqComputeActorCheckpoints::TPendingCheckpoint& TDqComputeActorCheckpoints::TPendingCheckpoint::operator=(const NDqProto::TCheckpoint& checkpoint) {
    Y_VERIFY(!Checkpoint);
    Checkpoint = checkpoint;
    return *this;
}

void TDqComputeActorCheckpoints::TPendingCheckpoint::Clear() {
    Checkpoint = Nothing();
    SavedComputeActorState = false;
    SavedSinkStatesCount = 0;
    ComputeActorState.Clear();
}

size_t TDqComputeActorCheckpoints::TPendingCheckpoint::GetSinksCount(const NDqProto::TDqTask& task) {
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

NDqProto::ECheckpointingMode GetTaskCheckpointingMode(const NDqProto::TDqTask& task) {
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
