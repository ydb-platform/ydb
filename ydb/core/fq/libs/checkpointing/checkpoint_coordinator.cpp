#include "utils.h"

#include "checkpoint_coordinator.h"

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/events/events.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_checkpoints.h>
#include <ydb/library/yql/dq/actors/dq.h>
#include <ydb/library/yql/dq/state/dq_state_load_plan.h>

#include <util/string/builder.h>

#include <utility>

#define CC_LOG_D(stream) \
    LOG_STREAMS_CHECKPOINT_COORDINATOR_DEBUG("[" << CoordinatorId << "] " << stream)
#define CC_LOG_I(stream) \
    LOG_STREAMS_CHECKPOINT_COORDINATOR_INFO("[" << CoordinatorId << "] " << stream)
#define CC_LOG_W(stream) \
    LOG_STREAMS_CHECKPOINT_COORDINATOR_WARN("[" << CoordinatorId << "] " << stream)
#define CC_LOG_E(stream) \
    LOG_STREAMS_CHECKPOINT_COORDINATOR_ERROR("[" << CoordinatorId << "] " << stream)

namespace NFq {

TCheckpointCoordinator::TCheckpointCoordinator(TCoordinatorId coordinatorId,
                                               const TActorId& storageProxy,
                                               const TActorId& runActorId,
                                               const TCheckpointCoordinatorConfig& settings,
                                               const ::NMonitoring::TDynamicCounterPtr& counters,
                                               const NProto::TGraphParams& graphParams,
                                               const FederatedQuery::StateLoadMode& stateLoadMode,
                                               const FederatedQuery::StreamingDisposition& streamingDisposition,
                                               // vvv TaskController temporary params vvv
                                               const TString& traceId,
                                               const NActors::TActorId& executerId,
                                               const NActors::TActorId& resultId,
                                               const NYql::TDqConfiguration::TPtr& tcSettings,
                                               const NYql::NCommon::TServiceCounters& serviceCounters,
                                               const TDuration& pingPeriod,
                                               const TDuration& aggrPeriod)
    : NYql::TTaskControllerImpl<TCheckpointCoordinator>(
            traceId,
            executerId,
            resultId,
            tcSettings,
            serviceCounters,
            pingPeriod,
            aggrPeriod,
            &TCheckpointCoordinator::DispatchEvent)
    , CoordinatorId(std::move(coordinatorId))
    , StorageProxy(storageProxy)
    , RunActorId(runActorId)
    , Settings(settings)
    , CheckpointingPeriod(TDuration::MilliSeconds(Settings.GetCheckpointingPeriodMillis()))
    , CheckpointingSnapshotRotationPeriod(Settings.GetCheckpointingSnapshotRotationPeriod())
    , CheckpointingSnapshotRotationIndex(CheckpointingSnapshotRotationPeriod)   // First - snapshot
    , GraphParams(graphParams)
    , Metrics(TCheckpointCoordinatorMetrics(counters))
    , StateLoadMode(stateLoadMode)
    , StreamingDisposition(streamingDisposition)
{
}

void TCheckpointCoordinator::Handle(NYql::NDqs::TEvReadyState::TPtr& ev) {
    NYql::TTaskControllerImpl<TCheckpointCoordinator>::OnReadyState(ev);

    CC_LOG_D("TEvReadyState, streaming disposition " << StreamingDisposition << ", state load mode " << FederatedQuery::StateLoadMode_Name(StateLoadMode));

    int tasksSize = GetTasksSize();
    const auto& actorIds = ev->Get()->Record.GetActorId();
    Y_ABORT_UNLESS(tasksSize == actorIds.size());

    for (int i = 0; i < tasksSize; ++i) {
        const auto& task = GetTask(i);
        auto& actorId = TaskIdToActor[task.GetId()];
        if (actorId) {
            OnInternalError(TStringBuilder() << "Duplicate task id: " << task.GetId());
            return;
        }
        actorId = ActorIdFromProto(actorIds[i]);

        TComputeActorTransportStuff::TPtr transport = AllActors[actorId] = MakeIntrusive<TComputeActorTransportStuff>();
        transport->EventsQueue.Init(CoordinatorId.ToString(), SelfId(), SelfId(), task.GetId());
        transport->EventsQueue.OnNewRecipientId(actorId);
        if (NYql::NDq::GetTaskCheckpointingMode(task) != NYql::NDqProto::CHECKPOINTING_MODE_DISABLED) {
            if (IsIngress(task)) {
                ActorsToTrigger[actorId] = transport;
                ActorsToNotify[actorId] = transport;
                ActorsToNotifySet.insert(actorId);
            }
            if (IsEgress(task)) {
                ActorsToNotify[actorId] = transport;
                ActorsToNotifySet.insert(actorId);
            }
            if (HasState(task)) {
                ActorsToWaitFor[actorId] = transport;
                ActorsToWaitForSet.insert(actorId);
            }
        }
        AllActorsSet.insert(actorId);
    }

    PendingInit = std::make_unique<TPendingInitCoordinator>(AllActors.size());

    CC_LOG_D("Send TEvRegisterCoordinatorRequest");
    Send(StorageProxy, new TEvCheckpointStorage::TEvRegisterCoordinatorRequest(CoordinatorId), IEventHandle::FlagTrackDelivery);
}

void TCheckpointCoordinator::ScheduleNextCheckpoint() {
    Schedule(CheckpointingPeriod, new TEvCheckpointCoordinator::TEvScheduleCheckpointing());
}

void TCheckpointCoordinator::UpdateInProgressMetric() {
    const auto pending = PendingCheckpoints.size();
    const auto pendingCommit = PendingCommitCheckpoints.size();
    Metrics.Pending->Set(pending);
    Metrics.PendingCommit->Set(pendingCommit);
    Metrics.InProgress->Set(pending + pendingCommit);
}

void TCheckpointCoordinator::Handle(const TEvCheckpointStorage::TEvRegisterCoordinatorResponse::TPtr& ev) {
    CC_LOG_D("Got TEvRegisterCoordinatorResponse; issues: " << ev->Get()->Issues.ToOneLineString());
    const auto& issues = ev->Get()->Issues;
    if (issues) {
        CC_LOG_E("Can't register in storage: " + issues.ToOneLineString());
        ++*Metrics.StorageError;
        OnInternalError("Can't register in storage", issues);
        return;
    }

    CC_LOG_D("Successfully registered in storage");
    CC_LOG_I("Send TEvNewCheckpointCoordinator to " << AllActors.size() << " actor(s)");
    for (const auto& [actor, transport] : AllActors) {
        transport->EventsQueue.Send(new NYql::NDq::TEvDqCompute::TEvNewCheckpointCoordinator(CoordinatorId.Generation, CoordinatorId.GraphId));
    }

    const bool needCheckpointMetadata = StateLoadMode == FederatedQuery::StateLoadMode::FROM_LAST_CHECKPOINT || StreamingDisposition.has_from_last_checkpoint();
    if (needCheckpointMetadata) {
        const bool loadGraphDescription = StateLoadMode == FederatedQuery::StateLoadMode::EMPTY && StreamingDisposition.has_from_last_checkpoint(); // Continue mode
        CC_LOG_I("Send TEvGetCheckpointsMetadataRequest; state load mode: " << FederatedQuery::StateLoadMode_Name(StateLoadMode) << "; load graph: " << loadGraphDescription);
        Send(StorageProxy,
            new TEvCheckpointStorage::TEvGetCheckpointsMetadataRequest(
                CoordinatorId.GraphId,
                {ECheckpointStatus::PendingCommit, ECheckpointStatus::Completed},
                1,
                loadGraphDescription),
            IEventHandle::FlagTrackDelivery);
    } else if (StateLoadMode == FederatedQuery::StateLoadMode::EMPTY) {
        ++*Metrics.StartedFromEmptyCheckpoint;
        CheckpointIdGenerator = std::make_unique<TCheckpointIdGenerator>(CoordinatorId);
        InitingZeroCheckpoint = true;
        InitCheckpoint();
        ScheduleNextCheckpoint();
    } else {
        OnInternalError(TStringBuilder() << "Unexpected state load mode (" << FederatedQuery::StateLoadMode_Name(StateLoadMode) << ") and streaming disposition " << StreamingDisposition);
    }
}

void TCheckpointCoordinator::Handle(const NYql::NDq::TEvDqCompute::TEvNewCheckpointCoordinatorAck::TPtr& ev) {
    if (!OnComputeActorEventReceived(ev)) {
        return;
    }

    if (PendingInit) {
        PendingInit->OnNewCheckpointCoordinatorAck();

        if (PendingInit->CanInjectCheckpoint()) {
            auto checkpointId = *PendingInit->CheckpointId;
            InjectCheckpoint(checkpointId, NYql::NDqProto::TCheckpoint::EType::TCheckpoint_EType_SNAPSHOT);
        }
    }
}

void TCheckpointCoordinator::Handle(const TEvCheckpointStorage::TEvGetCheckpointsMetadataResponse::TPtr& ev) {
    const auto event = ev->Get();
    const auto& checkpoints = event->Checkpoints;
    CC_LOG_D("Got TEvGetCheckpointsMetadataResponse");
    Y_ABORT_UNLESS(!PendingRestoreCheckpoint);

    if (event->Issues) {
        ++*Metrics.StorageError;
        CC_LOG_E("Can't get checkpoints to restore: " + event->Issues.ToOneLineString());
        OnInternalError("Can't get checkpoints to restore", event->Issues);
        return;
    }

    Y_ABORT_UNLESS(checkpoints.size() < 2);
    if (!checkpoints.empty()) {
        const auto& checkpoint = checkpoints.at(0);
        CheckpointIdGenerator = std::make_unique<TCheckpointIdGenerator>(CoordinatorId, checkpoint.CheckpointId);
        const bool needRestoreOffsets = StateLoadMode == FederatedQuery::StateLoadMode::EMPTY && StreamingDisposition.has_from_last_checkpoint();
        if (needRestoreOffsets) {
            TryToRestoreOffsetsFromForeignCheckpoint(checkpoint);
        } else {
            RestoreFromOwnCheckpoint(checkpoint);
        }
        return;
    }

    // Not restored from existing checkpoint. Init zero checkpoint
    ++*Metrics.StartedFromEmptyCheckpoint;
    CheckpointIdGenerator = std::make_unique<TCheckpointIdGenerator>(CoordinatorId);
    CC_LOG_I("Found no checkpoints to restore from, creating a 'zero' checkpoint");
    InitingZeroCheckpoint = true;
    InitCheckpoint();
    ScheduleNextCheckpoint();
}

void TCheckpointCoordinator::RestoreFromOwnCheckpoint(const TCheckpointMetadata& checkpoint) {
    CC_LOG_I("Will restore from checkpoint " << checkpoint.CheckpointId);
    PendingRestoreCheckpoint = TPendingRestoreCheckpoint(checkpoint.CheckpointId, checkpoint.Status == ECheckpointStatus::PendingCommit, ActorsToWaitForSet);
    ++*Metrics.RestoredFromSavedCheckpoint;
    for (const auto& [actor, transport] : ActorsToWaitFor) {
        transport->EventsQueue.Send(
            new NYql::NDq::TEvDqCompute::TEvRestoreFromCheckpoint(checkpoint.CheckpointId.SeqNo, checkpoint.CheckpointId.CoordinatorGeneration, CoordinatorId.Generation));
    }
}

void TCheckpointCoordinator::TryToRestoreOffsetsFromForeignCheckpoint(const TCheckpointMetadata& checkpoint) {
    RestoringFromForeignCheckpoint = true;
    CC_LOG_I("Will try to restore streaming offsets from checkpoint " << checkpoint.CheckpointId);
    if (!checkpoint.Graph) {
        ++*Metrics.StorageError;
        const TString message = TStringBuilder() << "Can't get graph params from checkpoint " << checkpoint.CheckpointId;
        CC_LOG_I(message);
        OnInternalError(message);
        return;
    }

    NYql::TIssues issues;
    THashMap<ui64, NYql::NDqProto::NDqStateLoadPlan::TTaskPlan> plan;
    const bool result = NYql::NDq::MakeContinueFromStreamingOffsetsPlan(
        checkpoint.Graph->GetTasks(),
        GraphParams.GetTasks(),
        StreamingDisposition.from_last_checkpoint().force(),
        plan,
        issues);

    if (issues) {
        CC_LOG_I(issues.ToOneLineString());
    }

    if (!result) {
        NYql::TTaskControllerImpl<TCheckpointCoordinator>::OnError(NYql::NDqProto::StatusIds::BAD_REQUEST, "Can't restore from plan given", issues);
        return;
    } else { // Report as transient issues
        Send(RunActorId, new TEvents::TEvRaiseTransientIssues(std::move(issues)));
    }

    PendingRestoreCheckpoint = TPendingRestoreCheckpoint(checkpoint.CheckpointId, false, ActorsToWaitForSet);
    ++*Metrics.RestoredStreamingOffsetsFromCheckpoint;
    for (const auto& [taskId, taskPlan] : plan) {
        const auto actorIdIt = TaskIdToActor.find(taskId);
        if (actorIdIt == TaskIdToActor.end()) {
            const TString msg = TStringBuilder() << "ActorId for task id " << taskId << " was not found";
            CC_LOG_E(msg);
            OnInternalError(msg);
            return;
        }
        const auto transportIt = ActorsToWaitFor.find(actorIdIt->second);
        if (transportIt != ActorsToWaitFor.end()) {
            transportIt->second->EventsQueue.Send(
                new NYql::NDq::TEvDqCompute::TEvRestoreFromCheckpoint(
                    checkpoint.CheckpointId.SeqNo,
                    checkpoint.CheckpointId.CoordinatorGeneration,
                    CoordinatorId.Generation,
                    taskPlan));
        }
    }
}

void TCheckpointCoordinator::Handle(const NYql::NDq::TEvDqCompute::TEvRestoreFromCheckpointResult::TPtr& ev) {
    if (!OnComputeActorEventReceived(ev)) {
        return;
    }

    const auto& record = ev->Get()->Record;
    const auto& checkpointProto = record.GetCheckpoint();
    const TCheckpointId checkpoint(checkpointProto.GetGeneration(), checkpointProto.GetId());
    const auto& status = record.GetStatus();
    const TString& statusName = NYql::NDqProto::TEvRestoreFromCheckpointResult_ERestoreStatus_Name(status);
    CC_LOG_D("[" << checkpoint << "] Got TEvRestoreFromCheckpointResult; taskId: "<< record.GetTaskId()
                 << ", checkpoint: " << checkpoint
                 << ", status: " << statusName);

    if (!PendingRestoreCheckpoint) {
        CC_LOG_E("[" << checkpoint << "] Got TEvRestoreFromCheckpointResult but has no PendingRestoreCheckpoint");
        OnInternalError("Got TEvRestoreFromCheckpointResult but has no PendingRestoreCheckpoint");
        return;
    }

    if (PendingRestoreCheckpoint->CheckpointId != checkpoint) {
        CC_LOG_E("[" << checkpoint << "] Got TEvRestoreFromCheckpointResult event with unexpected checkpoint: " << checkpoint << ", expected: " << PendingRestoreCheckpoint->CheckpointId);
        OnInternalError("Got unexpected checkpoint");
        return;
    }

    if (status != NYql::NDqProto::TEvRestoreFromCheckpointResult_ERestoreStatus_OK) {
        CC_LOG_E("[" << checkpoint << "] Can't restore: " << statusName);
        NYql::TTaskControllerImpl<TCheckpointCoordinator>::OnError(NYql::NDqProto::StatusIds::ABORTED, "Can't restore: " + statusName, {});
        return;
    }

    PendingRestoreCheckpoint->Acknowledge(ev->Sender);
    CC_LOG_D("[" << checkpoint << "] Task state restored, need " << PendingRestoreCheckpoint->NotYetAcknowledgedCount() << " more acks");

    if (PendingRestoreCheckpoint->GotAllAcknowledges()) {
        if (PendingInit) {
            PendingInit = nullptr;
        }

        if (PendingRestoreCheckpoint->CommitAfterRestore) {
            CC_LOG_I("[" << checkpoint << "] State restored, send TEvCommitState to " << ActorsToNotify.size() << " actor(s)");
            PendingCommitCheckpoints.emplace(checkpoint, TPendingCheckpoint(ActorsToNotifySet, NYql::NDqProto::TCheckpoint::EType::TCheckpoint_EType_SNAPSHOT));
            UpdateInProgressMetric();
            for (const auto& [actor, transport] : ActorsToNotify) {
                transport->EventsQueue.Send(new NYql::NDq::TEvDqCompute::TEvCommitState(checkpoint.SeqNo, checkpoint.CoordinatorGeneration, CoordinatorId.Generation));
            }
        }

        if (RestoringFromForeignCheckpoint) {
            InitingZeroCheckpoint = true;
            InitCheckpoint();
        }

        ScheduleNextCheckpoint();
        CC_LOG_I("[" << checkpoint << "] State restored, send TEvRun to " << AllActors.size() << " actors");
        for (const auto& [actor, transport] : AllActors) {
            transport->EventsQueue.Send(new NYql::NDq::TEvDqCompute::TEvRun());
        }
    }
}

void TCheckpointCoordinator::InitCheckpoint() {
    Y_ABORT_UNLESS(CheckpointIdGenerator);
    const auto nextCheckpointId = CheckpointIdGenerator->NextId();
    CC_LOG_I("[" << nextCheckpointId << "] Registering new checkpoint in storage");

    auto checkpointType = NYql::NDqProto::TCheckpoint::EType::TCheckpoint_EType_INCREMENT_OR_SNAPSHOT;
    if (++CheckpointingSnapshotRotationIndex > CheckpointingSnapshotRotationPeriod) {
        checkpointType = NYql::NDqProto::TCheckpoint::EType::TCheckpoint_EType_SNAPSHOT;
        CheckpointingSnapshotRotationIndex = 0;
    }
    PendingCheckpoints.emplace(nextCheckpointId, TPendingCheckpoint(ActorsToWaitForSet, checkpointType));
    UpdateInProgressMetric();
    ++*Metrics.Total;

    std::unique_ptr<TEvCheckpointStorage::TEvCreateCheckpointRequest> req;
    if (GraphDescId) {
        req = std::make_unique<TEvCheckpointStorage::TEvCreateCheckpointRequest>(CoordinatorId, nextCheckpointId, ActorsToWaitForSet.size(), GraphDescId);
    } else {
        NProto::TCheckpointGraphDescription graphDesc;
        graphDesc.MutableGraph()->CopyFrom(GraphParams);
        req = std::make_unique<TEvCheckpointStorage::TEvCreateCheckpointRequest>(CoordinatorId, nextCheckpointId, ActorsToWaitForSet.size(), graphDesc);
    }

    Send(StorageProxy, req.release(), IEventHandle::FlagTrackDelivery);
}

void TCheckpointCoordinator::Handle(const TEvCheckpointCoordinator::TEvScheduleCheckpointing::TPtr&) {
    CC_LOG_D("Got TEvScheduleCheckpointing");
    ScheduleNextCheckpoint();
    const auto checkpointsInFly = PendingCheckpoints.size() + PendingCommitCheckpoints.size();
    if (checkpointsInFly >= Settings.GetMaxInflight() || InitingZeroCheckpoint) {
        CC_LOG_W("Skip schedule checkpoint event since inflight checkpoint limit exceeded: current: " << checkpointsInFly << ", limit: " << Settings.GetMaxInflight());
        Metrics.SkippedDueToInFlightLimit->Inc();
        return;
    }
    Metrics.SkippedDueToInFlightLimit->Set(0);
    InitCheckpoint();
}

void TCheckpointCoordinator::Handle(const TEvCheckpointStorage::TEvCreateCheckpointResponse::TPtr& ev) {
    const auto& checkpointId = ev->Get()->CheckpointId;
    const auto& issues = ev->Get()->Issues;
    CC_LOG_D("[" << checkpointId << "] Got TEvCreateCheckpointResponse");

    if (issues) {
        CC_LOG_E("[" << checkpointId << "] Can't create checkpoint: " << issues.ToOneLineString());
        PendingCheckpoints.erase(checkpointId);
        UpdateInProgressMetric();
        ++*Metrics.FailedToCreate;
        ++*Metrics.StorageError;
        CheckpointingSnapshotRotationIndex = CheckpointingSnapshotRotationPeriod; // Next ceckpoint is snapshot.
        return;
    }

    if (GraphDescId) {
        Y_ABORT_UNLESS(GraphDescId == ev->Get()->GraphDescId);
    } else {
        GraphDescId = ev->Get()->GraphDescId;
        Y_ABORT_UNLESS(GraphDescId);
    }

    if (PendingInit) {
        PendingInit->CheckpointId = checkpointId;
        if (PendingInit->CanInjectCheckpoint()) {
            PendingInit = nullptr;
            InjectCheckpoint(checkpointId, NYql::NDqProto::TCheckpoint::EType::TCheckpoint_EType_SNAPSHOT);
        }
    } else {
        const auto it = PendingCheckpoints.find(checkpointId);
        if (it == PendingCheckpoints.end()) {
            CC_LOG_E("[" << checkpointId << "] Unknown checkpoint response: " << checkpointId);
            return;
        }
        auto& checkpoint = it->second;

        InjectCheckpoint(checkpointId, checkpoint.GetType());
    }
}

void TCheckpointCoordinator::InjectCheckpoint(const TCheckpointId& checkpointId, NYql::NDqProto::TCheckpoint::EType type) {
    CC_LOG_I("[" << checkpointId << "] Checkpoint successfully created, going to inject barriers to " << ActorsToTrigger.size() << " actor(s)");
    for (const auto& [toTrigger, transport] : ActorsToTrigger) {
        transport->EventsQueue.Send(new NYql::NDq::TEvDqCompute::TEvInjectCheckpoint(checkpointId.SeqNo, checkpointId.CoordinatorGeneration, type));
    }

    if (!GraphIsRunning) {
        CC_LOG_I("[" << checkpointId << "] Send TEvRun to all actors");
        for (const auto& [actor, transport] : AllActors) {
            transport->EventsQueue.Send(new NYql::NDq::TEvDqCompute::TEvRun());
        }
        GraphIsRunning = true;
    }
}

void TCheckpointCoordinator::Handle(const NYql::NDq::TEvDqCompute::TEvSaveTaskStateResult::TPtr& ev) {
    const auto& proto = ev->Get()->Record;
    const auto& checkpointProto = proto.GetCheckpoint();
    const auto& status = proto.GetStatus();
    const TString& statusName = NYql::NDqProto::TEvSaveTaskStateResult_EStatus_Name(status);

    if (!OnComputeActorEventReceived(ev)) {
        return;
    }

    TCheckpointId checkpointId(checkpointProto.GetGeneration(), checkpointProto.GetId());

    CC_LOG_D("[" << checkpointId << "] Got TEvSaveTaskStateResult; task " << proto.GetTaskId()
                 << ", status: " << statusName << ", size: " << proto.GetStateSizeBytes());

    const auto it = PendingCheckpoints.find(checkpointId);
    if (it == PendingCheckpoints.end()) {
        return;
    }
    auto& checkpoint = it->second;

    if (status == NYql::NDqProto::TEvSaveTaskStateResult::OK) {
        checkpoint.Acknowledge(ev->Sender, proto.GetStateSizeBytes());
        CC_LOG_D("[" << checkpointId << "] Task state saved, need " << checkpoint.NotYetAcknowledgedCount() << " more acks");
        if (checkpoint.GotAllAcknowledges()) {
            CC_LOG_I("[" << checkpointId << "] Got all acks, changing checkpoint status to 'PendingCommit'");
            Send(StorageProxy, new TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusRequest(CoordinatorId, checkpointId, checkpoint.GetStats().StateSize), IEventHandle::FlagTrackDelivery);
            if (InitingZeroCheckpoint) {
                Send(RunActorId, new TEvCheckpointCoordinator::TEvZeroCheckpointDone());
            }
        }
    } else {
        CC_LOG_E("[" << checkpointId << "] Can't save node state, aborting checkpoint");
        CheckpointingSnapshotRotationIndex = CheckpointingSnapshotRotationPeriod;  // Next ceckpoint is snapshot.
        Send(StorageProxy, new TEvCheckpointStorage::TEvAbortCheckpointRequest(CoordinatorId, checkpointId, "Can't save node state"), IEventHandle::FlagTrackDelivery);
    }
}

void TCheckpointCoordinator::Handle(const TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusResponse::TPtr& ev) {
    const auto& checkpointId = ev->Get()->CheckpointId;
    const auto issues = ev->Get()->Issues;
    CC_LOG_D("[" << checkpointId << "] Got TEvSetCheckpointPendingCommitStatusResponse");
    const auto it = PendingCheckpoints.find(checkpointId);
    if (it == PendingCheckpoints.end()) {
        CC_LOG_W("[" << checkpointId << "] Got TEvSetCheckpointPendingCommitStatusResponse for checkpoint but it is not in PendingCheckpoints");
        return;
    }

    if (issues) {
        CC_LOG_E("[" << checkpointId << "] Can't change checkpoint status to 'PendingCommit': " << issues.ToString());
        ++*Metrics.StorageError;
        PendingCheckpoints.erase(it);
        return;
    }

    CC_LOG_I("[" << checkpointId << "] Checkpoint status changed to 'PendingCommit', committing states");
    PendingCommitCheckpoints.emplace(checkpointId, TPendingCheckpoint(ActorsToNotifySet, it->second.GetType(), it->second.GetStats()));
    PendingCheckpoints.erase(it);
    UpdateInProgressMetric();
    for (const auto& [toTrigger, transport] : ActorsToNotify) {
        transport->EventsQueue.Send(new NYql::NDq::TEvDqCompute::TEvCommitState(checkpointId.SeqNo, checkpointId.CoordinatorGeneration, CoordinatorId.Generation));
    }
}

void TCheckpointCoordinator::Handle(const NYql::NDq::TEvDqCompute::TEvStateCommitted::TPtr& ev) {
    if (!OnComputeActorEventReceived(ev)) {
        return;
    }

    const auto& checkpointPb = ev->Get()->Record.GetCheckpoint();
    TCheckpointId checkpointId(checkpointPb.GetGeneration(), checkpointPb.GetId());
    CC_LOG_D("[" << checkpointId << "] Got TEvStateCommitted; task: " << ev->Get()->Record.GetTaskId());
    const auto it = PendingCommitCheckpoints.find(checkpointId);
    if (it == PendingCommitCheckpoints.end()) {
        CC_LOG_W("[" << checkpointId << "] Got TEvStateCommitted for checkpoint " << checkpointId << " but it is not in PendingCommitCheckpoints");
        return;
    }

    auto& checkpoint = it->second;
    checkpoint.Acknowledge(ev->Sender);
    CC_LOG_D("[" << checkpointId << "] State committed " << ev->Sender.ToString() << ", need " << checkpoint.NotYetAcknowledgedCount() << " more acks");
    if (checkpoint.GotAllAcknowledges()) {
        CC_LOG_I("[" << checkpointId << "] Got all acks, changing checkpoint status to 'Completed'");
        const auto& stats = checkpoint.GetStats();
        auto durationMs = (TInstant::Now() - stats.CreatedAt).MilliSeconds();
        Metrics.LastCheckpointBarrierDeliveryTimeMillis->Set(durationMs);
        Metrics.CheckpointBarrierDeliveryTimeMillis->Collect(durationMs);
        Send(StorageProxy, new TEvCheckpointStorage::TEvCompleteCheckpointRequest(CoordinatorId, checkpointId, stats.StateSize, checkpoint.GetType()), IEventHandle::FlagTrackDelivery);
    }
}

void TCheckpointCoordinator::Handle(const TEvCheckpointStorage::TEvCompleteCheckpointResponse::TPtr& ev) {
    const auto& checkpointId = ev->Get()->CheckpointId;
    CC_LOG_D("[" << checkpointId << "] Got TEvCompleteCheckpointResponse");
    const auto it = PendingCommitCheckpoints.find(checkpointId);
    if (it == PendingCommitCheckpoints.end()) {
        CC_LOG_W("[" << checkpointId << "] Got TEvCompleteCheckpointResponse but related checkpoint is not in progress; checkpointId: " << checkpointId);
        return;
    }
    const auto& issues = ev->Get()->Issues;
    if (!issues) {
        const auto& stats = it->second.GetStats();
        auto durationMs = (TInstant::Now() - stats.CreatedAt).MilliSeconds();
        Metrics.LastCheckpointDurationMillis->Set(durationMs);
        Metrics.LastCheckpointSizeBytes->Set(stats.StateSize);
        Metrics.CheckpointDurationMillis->Collect(durationMs);
        Metrics.CheckpointSizeBytes->Collect(stats.StateSize);
        ++*Metrics.Completed;
        CC_LOG_I("[" << checkpointId << "] Checkpoint completed");
    } else {
        ++*Metrics.StorageError;
        CC_LOG_E("[" << checkpointId << "] Can't change checkpoint status to 'Completed': " << issues.ToString());
    }
    PendingCommitCheckpoints.erase(it);
    UpdateInProgressMetric();
}

void TCheckpointCoordinator::Handle(const TEvCheckpointStorage::TEvAbortCheckpointResponse::TPtr& ev) {
    const auto& checkpointId = ev->Get()->CheckpointId;
    CC_LOG_D("[" << checkpointId << "] Got TEvAbortCheckpointResponse");
    const auto& issues = ev->Get()->Issues;
    if (issues) {
        CC_LOG_E("[" << checkpointId << "] Can't abort checkpoint: " << issues.ToString());
        ++*Metrics.StorageError;
    } else {
        CC_LOG_W("[" << checkpointId << "] Checkpoint aborted");
        ++*Metrics.Aborted;
    }
    PendingCheckpoints.erase(checkpointId);
    PendingCommitCheckpoints.erase(checkpointId);
    UpdateInProgressMetric();
}

void TCheckpointCoordinator::Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvRetry::TPtr& ev) {
    const auto actorIt = TaskIdToActor.find(ev->Get()->EventQueueId);
    Y_ABORT_UNLESS(actorIt != TaskIdToActor.end());
    const auto transportIt = AllActors.find(actorIt->second);
    Y_ABORT_UNLESS(transportIt != AllActors.end());
    transportIt->second->EventsQueue.Retry();
}

void TCheckpointCoordinator::Handle(NActors::TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
    CC_LOG_I("Handle disconnected node " << ev->Get()->NodeId);

    for (const auto& [actorId, transport] : AllActors) {
        transport->EventsQueue.HandleNodeDisconnected(ev->Get()->NodeId);
    }
}

void TCheckpointCoordinator::Handle(NActors::TEvInterconnect::TEvNodeConnected::TPtr& ev) {
    CC_LOG_D("Handle connected node " << ev->Get()->NodeId);

    for (const auto& [actorId, transport] : AllActors) {
        transport->EventsQueue.HandleNodeConnected(ev->Get()->NodeId);
    }
}

void TCheckpointCoordinator::Handle(NActors::TEvents::TEvPoison::TPtr& ev) {
    CC_LOG_D("Got TEvPoison");
    Send(ev->Sender, new NActors::TEvents::TEvPoisonTaken(), 0, ev->Cookie);
    PassAway();
}

void TCheckpointCoordinator::Handle(const TEvCheckpointCoordinator::TEvRunGraph::TPtr&) {
    InitingZeroCheckpoint = false;
    // TODO: run graph only now, not before zero checkpoint inited
}

void TCheckpointCoordinator::PassAway() {
    for (const auto& [actorId, transport] : AllActors) {
        transport->EventsQueue.Unsubscribe();
    }
    NYql::TTaskControllerImpl<TCheckpointCoordinator>::PassAway();
}

void TCheckpointCoordinator::HandleException(const std::exception& err) {
    NYql::TIssues issues;
    issues.AddIssue(err.what());
    OnInternalError("Internal error in checkpoint coordinator", issues);
}

THolder<NActors::IActor> MakeCheckpointCoordinator(
    TCoordinatorId coordinatorId,
    const TActorId& storageProxy,
    const TActorId& runActorId,
    const TCheckpointCoordinatorConfig& settings,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    const NProto::TGraphParams& graphParams,
    const FederatedQuery::StateLoadMode& stateLoadMode /* = FederatedQuery::StateLoadMode::FROM_LAST_CHECKPOINT */,
    const FederatedQuery::StreamingDisposition& streamingDisposition /* = {} */,
    // vvv TaskController temporary params vvv
    const TString& traceId,
    const NActors::TActorId& executerId,
    const NActors::TActorId& resultId,
    const NYql::TDqConfiguration::TPtr& tcSettings,
    const NYql::NCommon::TServiceCounters& serviceCounters,
    const TDuration& pingPeriod,
    const TDuration& aggrPeriod
    ) 
{
    return MakeHolder<TCheckpointCoordinator>(
        coordinatorId,
        storageProxy,
        runActorId,
        settings,
        counters,
        graphParams,
        stateLoadMode,
        streamingDisposition,
        // vvv TaskController temporary params vvv
        traceId,
        executerId,
        resultId,
        tcSettings,
        serviceCounters,
        pingPeriod,
        aggrPeriod
        );
}

} // namespace NFq
