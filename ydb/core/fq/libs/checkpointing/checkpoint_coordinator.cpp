
#include "checkpoint_coordinator.h"

#include <ydb/core/fq/libs/checkpointing/events/events.h>
#include <ydb/core/fq/libs/config/protos/checkpoint_coordinator.pb.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/yql/dq/actors/dq.h>
#include <ydb/library/yql/dq/state/dq_state_load_plan.h>

#include <util/string/builder.h>
#include <util/system/env.h>

#include <utility>

#define YDB_LOG_THIS_FILE_COMPONENT ::NKikimrServices::STREAMS_CHECKPOINT_COORDINATOR

namespace NFq {

using namespace NActors;

TCheckpointCoordinatorSettings::TCheckpointCoordinatorSettings() {
    ui64 ms = 0;
    if (!TryFromString<ui64>(GetEnv("YDB_TEST_DEFAULT_CHECKPOINTING_PERIOD_MS"), ms)) {
        return;
    }
    if (ms) {
        CheckpointingPeriod = TDuration::MilliSeconds(ms);
    }
}

TCheckpointCoordinatorSettings::TCheckpointCoordinatorSettings(const NFq::NConfig::TCheckpointCoordinatorConfig& config)
    : CheckpointingPeriod(TDuration::MilliSeconds(config.GetCheckpointingPeriodMillis() ? config.GetCheckpointingPeriodMillis() : 30'000))
    , CheckpointingSnapshotRotationPeriod(config.GetCheckpointingSnapshotRotationPeriod())
    , MaxInflight(config.GetMaxInflight())
{}

TCheckpointCoordinator::TCheckpointCoordinator(TCoordinatorId coordinatorId,
                                               const TActorId& storageProxy,
                                               const TActorId& runActorId,
                                               const TCheckpointCoordinatorSettings& settings,
                                               const ::NMonitoring::TDynamicCounterPtr& counters,
                                               const NProto::TGraphParams& graphParams,
                                               const FederatedQuery::StateLoadMode& stateLoadMode,
                                               const FederatedQuery::StreamingDisposition& streamingDisposition)
    : NActors::TActor<TCheckpointCoordinator>(&TCheckpointCoordinator::DispatchEvent)
    , CoordinatorId(std::move(coordinatorId))
    , StorageProxy(storageProxy)
    , RunActorId(runActorId)
    , Settings(settings)
    , CheckpointingSnapshotRotationPeriod(Settings.GetCheckpointingSnapshotRotationPeriod())
    , CheckpointingSnapshotRotationIndex(CheckpointingSnapshotRotationPeriod)   // First - snapshot
    , GraphParams(graphParams)
    , Metrics(TCheckpointCoordinatorMetrics(counters))
    , StateLoadMode(stateLoadMode)
    , StreamingDisposition(streamingDisposition)
{
}

void TCheckpointCoordinator::Handle(NFq::TEvCheckpointCoordinator::TEvReadyState::TPtr& ev) {
    YDB_LOG_DEBUG("TEvReadyState, streaming disposition, state load mode, checkpointing period",
        {"coordinatorId", CoordinatorId},
        {"streamingDisposition", StreamingDisposition},
        {"stateLoadMode", FederatedQuery::StateLoadMode_Name(StateLoadMode)},
        {"checkpointingPeriod", Settings.GetCheckpointingPeriod()});
    ControlId = ev->Sender;

    for (const auto& task : ev->Get()->Tasks) {
        Y_ABORT_UNLESS(task.ActorId);
        auto& actorId = TaskIdToActor[task.Id];
        TaskIds.emplace(task.ActorId, task.Id);
        if (actorId) {
            OnInternalError(TStringBuilder() << "Duplicate task id: " << task.Id);
            return;
        }
        actorId = task.ActorId;

        TComputeActorTransportStuff::TPtr transport = AllActors[actorId] = MakeIntrusive<TComputeActorTransportStuff>();
        transport->EventsQueue.Init(CoordinatorId.ToString(), SelfId(), SelfId(), task.Id);
        transport->EventsQueue.OnNewRecipientId(actorId);
        if (task.IsCheckpointingEnabled) {
            if (task.IsIngress) {
                ActorsToTrigger[actorId] = transport;
                ActorsToNotify[actorId] = transport;
                ActorsToNotifySet.insert(actorId);
            }
            if (task.IsEgress) {
                ActorsToNotify[actorId] = transport;
                ActorsToNotifySet.insert(actorId);
            }
            if (task.HasState) {
                ActorsToWaitFor[actorId] = transport;
                ActorsToWaitForSet.insert(actorId);
            }
        }
        AllActorsSet.insert(actorId);
    }

    YDB_LOG_DEBUG("AllActors, ActorsToTrigger, ActorsToNotify, ActorsToWaitFor",
        {"coordinatorId", CoordinatorId},
        {"count", AllActors.size()},
        {"actorsToTriggerCount", ActorsToTrigger.size()},
        {"actorsToNotifyCount", ActorsToNotify.size()},
        {"actorsToWaitForCount", ActorsToWaitFor.size()});

    if (ActorsToTrigger.empty()) {
        YDB_LOG_DEBUG("No ingress tasks, coordinator was disabled",
            {"coordinatorId", CoordinatorId});
        StartAllTasks();
        return;
    }

    PendingInit = std::make_unique<TPendingInitCoordinator>(AllActors.size());

    YDB_LOG_DEBUG("Send TEvRegisterCoordinatorRequest",
        {"coordinatorId", CoordinatorId});
    Send(StorageProxy, new TEvCheckpointStorage::TEvRegisterCoordinatorRequest(CoordinatorId), IEventHandle::FlagTrackDelivery);
}

void TCheckpointCoordinator::ScheduleNextCheckpoint() {
    Schedule(Settings.GetCheckpointingPeriod(), new TEvCheckpointCoordinator::TEvScheduleCheckpointing());
}

void TCheckpointCoordinator::UpdateInProgressMetric() {
    const auto pending = PendingCheckpoints.size();
    const auto pendingCommit = PendingCommitCheckpoints.size();
    Metrics.Pending->Set(pending);
    Metrics.PendingCommit->Set(pendingCommit);
    Metrics.InProgress->Set(pending + pendingCommit);
}

void TCheckpointCoordinator::Handle(const TEvCheckpointStorage::TEvRegisterCoordinatorResponse::TPtr& ev) {
    YDB_LOG_DEBUG("Got TEvRegisterCoordinatorResponse;",
        {"coordinatorId", CoordinatorId},
        {"issues", ev->Get()->Issues.ToOneLineString()});
    const auto& issues = ev->Get()->Issues;
    if (issues) {
        YDB_LOG_ERROR("StorageError: can't register in storage: ",
            {"coordinatorId", CoordinatorId},
            {"issues", issues.ToOneLineString()});
        ++*Metrics.StorageError;
        OnInternalError("Can't register in storage", issues);
        return;
    }

    YDB_LOG_DEBUG("Successfully registered in storage",
        {"coordinatorId", CoordinatorId});
    YDB_LOG_INFO("Send TEvNewCheckpointCoordinator to actor(s)",
        {"coordinatorId", CoordinatorId},
        {"allActorsCount", AllActors.size()});
    for (const auto& [actor, transport] : AllActors) {
        transport->EventsQueue.Send(new NYql::NDq::TEvDqCompute::TEvNewCheckpointCoordinator(CoordinatorId.Generation, CoordinatorId.GraphId));
    }

    const bool needCheckpointMetadata = StateLoadMode == FederatedQuery::StateLoadMode::FROM_LAST_CHECKPOINT || StreamingDisposition.has_from_last_checkpoint();
    if (needCheckpointMetadata) {
        const bool loadGraphDescription = StateLoadMode == FederatedQuery::StateLoadMode::EMPTY && StreamingDisposition.has_from_last_checkpoint(); // Continue mode
        YDB_LOG_INFO("Send TEvGetCheckpointsMetadataRequest; state load; load",
            {"coordinatorId", CoordinatorId},
            {"mode", FederatedQuery::StateLoadMode_Name(StateLoadMode)},
            {"graph", loadGraphDescription});
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
            InjectCheckpoint(checkpointId, NYql::NDqProto::CHECKPOINT_TYPE_SNAPSHOT);
        }
    }
}

void TCheckpointCoordinator::Handle(const TEvCheckpointStorage::TEvGetCheckpointsMetadataResponse::TPtr& ev) {
    const auto event = ev->Get();
    const auto& checkpoints = event->Checkpoints;
    YDB_LOG_DEBUG("Got TEvGetCheckpointsMetadataResponse",
        {"coordinatorId", CoordinatorId});
    Y_ABORT_UNLESS(!PendingRestoreCheckpoint);

    if (event->Issues) {
        ++*Metrics.StorageError;
        YDB_LOG_ERROR("StorageError: can't get checkpoints to restore",
            {"coordinatorId", CoordinatorId},
            {"issues", event->Issues.ToOneLineString()});
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
    YDB_LOG_INFO("Found no checkpoints to restore from, creating a 'zero' checkpoint",
        {"coordinatorId", CoordinatorId});
    InitingZeroCheckpoint = true;
    InitCheckpoint();
    ScheduleNextCheckpoint();
}

void TCheckpointCoordinator::RestoreFromOwnCheckpoint(const TCheckpointMetadata& checkpoint) {
    YDB_LOG_INFO("Will restore from checkpoint",
        {"coordinatorId", CoordinatorId},
        {"checkpointId", checkpoint.CheckpointId});
    PendingRestoreCheckpoint = TPendingRestoreCheckpoint(checkpoint.CheckpointId, checkpoint.Status == ECheckpointStatus::PendingCommit, ActorsToWaitForSet);
    ++*Metrics.RestoredFromSavedCheckpoint;
    for (const auto& [actor, transport] : ActorsToWaitFor) {
        transport->EventsQueue.Send(
            new NYql::NDq::TEvDqCompute::TEvRestoreFromCheckpoint(checkpoint.CheckpointId.SeqNo, checkpoint.CheckpointId.CoordinatorGeneration, CoordinatorId.Generation));
    }
}

void TCheckpointCoordinator::TryToRestoreOffsetsFromForeignCheckpoint(const TCheckpointMetadata& checkpoint) {
    RestoringFromForeignCheckpoint = true;
    YDB_LOG_INFO("Will try to restore streaming offsets from checkpoint",
        {"coordinatorId", CoordinatorId},
        {"checkpointId", checkpoint.CheckpointId});
    if (!checkpoint.Graph) {
        ++*Metrics.StorageError;
        const TString message = TStringBuilder() << "StorageError: can't get graph params from checkpoint " << checkpoint.CheckpointId;
        YDB_LOG_INFO(message,
            {"coordinatorId", CoordinatorId});
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
        YDB_LOG_INFO("Issues while building continue-from-streaming-offsets restore plan",
            {"coordinatorId", CoordinatorId},
            {"issues", issues.ToOneLineString()});
    }

    if (!result) {
        OnError(NYql::NDqProto::StatusIds::BAD_REQUEST, "Can't restore from plan given", issues);
        return;
    } else { // Report as transient issues
        Send(RunActorId, new NFq::TEvCheckpointCoordinator::TEvRaiseTransientIssues(std::move(issues)));
    }

    YDB_LOG_INFO("Going to restore offsets from foreign checkpoint for tasks",
        {"coordinatorId", CoordinatorId},
        {"checkpointId", checkpoint.CheckpointId},
        {"planSize", plan.size()});

    PendingRestoreCheckpoint = TPendingRestoreCheckpoint(checkpoint.CheckpointId, false, ActorsToWaitForSet);
    ++*Metrics.RestoredStreamingOffsetsFromCheckpoint;
    for (const auto& [taskId, taskPlan] : plan) {
        const auto actorIdIt = TaskIdToActor.find(taskId);
        if (actorIdIt == TaskIdToActor.end()) {
            const TString msg = TStringBuilder() << "ActorId for task id " << taskId << " was not found";
            YDB_LOG_ERROR("ActorId for task was not found",
                {"coordinatorId", CoordinatorId},
                {"taskId", taskId});
            OnInternalError(msg);
            return;
        }
        const auto transportIt = ActorsToWaitFor.find(actorIdIt->second);
        if (transportIt != ActorsToWaitFor.end()) {
            YDB_LOG_DEBUG("Restore offsets from foreign checkpoint for task",
                {"coordinatorId", CoordinatorId},
                {"checkpointId", checkpoint.CheckpointId},
                {"taskId", taskId});
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
    YDB_LOG_DEBUG("Got TEvRestoreFromCheckpointResult;",
        {"coordinatorId", CoordinatorId},
        {"checkpoint", checkpoint},
        {"taskId", record.GetTaskId()},
        {"status", statusName},
        {"issues", NYql::IssuesFromMessageAsString(record.GetIssues())});

    if (!PendingRestoreCheckpoint) {
        YDB_LOG_ERROR("Got TEvRestoreFromCheckpointResult but has no PendingRestoreCheckpoint",
            {"coordinatorId", CoordinatorId},
            {"checkpoint", checkpoint});
        OnInternalError("Got TEvRestoreFromCheckpointResult but has no PendingRestoreCheckpoint");
        return;
    }

    if (PendingRestoreCheckpoint->CheckpointId != checkpoint) {
        YDB_LOG_ERROR("Got TEvRestoreFromCheckpointResult event with unexpected",
            {"coordinatorId", CoordinatorId},
            {"checkpoint", checkpoint},
            {"expected", PendingRestoreCheckpoint->CheckpointId});
        OnInternalError("Got unexpected checkpoint");
        return;
    }

    if (status != NYql::NDqProto::TEvRestoreFromCheckpointResult_ERestoreStatus_OK) {
        auto msg = TStringBuilder() << "Can't restore: " << statusName << ", " << NYql::IssuesFromMessageAsString(record.GetIssues());
        YDB_LOG_ERROR("Can't restore",
            {"status", statusName},
            {"coordinatorId", CoordinatorId},
            {"checkpoint", checkpoint},
            {"msg", msg});
        ++*Metrics.RestoringError;
        OnError(NYql::NDqProto::StatusIds::ABORTED, msg, {});
        return;
    }

    PendingRestoreCheckpoint->Acknowledge(ev->Sender);
    YDB_LOG_DEBUG("Task state restored, need more acks",
        {"coordinatorId", CoordinatorId},
        {"checkpoint", checkpoint},
        {"notYetAcknowledgedCount", PendingRestoreCheckpoint->NotYetAcknowledgedCount()});

    if (PendingRestoreCheckpoint->GotAllAcknowledges()) {
        if (PendingInit) {
            PendingInit = nullptr;
        }

        if (PendingRestoreCheckpoint->CommitAfterRestore) {
            YDB_LOG_INFO("State restored, send TEvCommitState to actor(s)",
                {"coordinatorId", CoordinatorId},
                {"checkpoint", checkpoint},
                {"actorsToNotifyCount", ActorsToNotify.size()});
            PendingCommitCheckpoints.emplace(checkpoint, TPendingCheckpoint(ActorsToNotifySet, NYql::NDqProto::CHECKPOINT_TYPE_SNAPSHOT));
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
        StartAllTasks();
    }
}

void TCheckpointCoordinator::InitCheckpoint() {
    Y_ABORT_UNLESS(CheckpointIdGenerator);
    const auto nextCheckpointId = CheckpointIdGenerator->NextId();
    YDB_LOG_INFO("Registering new checkpoint in storage",
        {"coordinatorId", CoordinatorId},
        {"nextCheckpointId", nextCheckpointId});

    auto checkpointType = NYql::NDqProto::CHECKPOINT_TYPE_INCREMENT_OR_SNAPSHOT;
    if (++CheckpointingSnapshotRotationIndex > CheckpointingSnapshotRotationPeriod) {
        checkpointType = NYql::NDqProto::CHECKPOINT_TYPE_SNAPSHOT;
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
    YDB_LOG_DEBUG("Got TEvScheduleCheckpointing",
        {"coordinatorId", CoordinatorId});
    ScheduleNextCheckpoint();
    const auto checkpointsInFly = PendingCheckpoints.size() + PendingCommitCheckpoints.size();
    if (checkpointsInFly >= Settings.GetMaxInflight() || (InitingZeroCheckpoint && !FailedZeroCheckpoint)) {
        YDB_LOG_WARN("Skip schedule checkpoint event since inflight checkpoint limit exceeded",
            {"coordinatorId", CoordinatorId},
            {"current", checkpointsInFly},
            {"limit", Settings.GetMaxInflight()});
        Metrics.SkippedDueToInFlightLimit->Inc();
        ++SkippedDueToInFlightLimitCounter;
        return;
    }
    FailedZeroCheckpoint = false;
    Metrics.SkippedDueToInFlightLimit->Sub(SkippedDueToInFlightLimitCounter);
    SkippedDueToInFlightLimitCounter = 0;
    InitCheckpoint();
}

void TCheckpointCoordinator::Handle(const TEvCheckpointStorage::TEvCreateCheckpointResponse::TPtr& ev) {
    const auto& checkpointId = ev->Get()->CheckpointId;
    const auto& issues = ev->Get()->Issues;
    YDB_LOG_DEBUG("Got TEvCreateCheckpointResponse",
        {"coordinatorId", CoordinatorId},
        {"checkpointId", checkpointId});

    auto cancelCheckpoint = [&](const TString& str) {
        YDB_LOG_ERROR(str,
            {"coordinatorId", CoordinatorId},
            {"checkpointId", checkpointId});
        PendingCheckpoints.erase(checkpointId);
        FailedZeroCheckpoint = InitingZeroCheckpoint;
        UpdateInProgressMetric();
        ++*Metrics.FailedToCreate;
        ++*Metrics.StorageError;
        CheckpointingSnapshotRotationIndex = CheckpointingSnapshotRotationPeriod; // Next checkpoint is snapshot.
    };

    if (issues) {
        cancelCheckpoint("StorageError: can't create checkpoint: " + issues.ToOneLineString());
        return;
    }

    if (GraphDescId) {
        Y_ABORT_UNLESS(GraphDescId == ev->Get()->GraphDescId);
    } else {
        GraphDescId = ev->Get()->GraphDescId;
        if (!GraphDescId) {
            cancelCheckpoint("StorageError (internal error), empty GraphDescId");
            return;
        }
    }

    if (PendingInit) {
        PendingInit->CheckpointId = checkpointId;
        if (PendingInit->CanInjectCheckpoint()) {
            PendingInit = nullptr;
            InjectCheckpoint(checkpointId, NYql::NDqProto::CHECKPOINT_TYPE_SNAPSHOT);
        }
    } else {
        const auto it = PendingCheckpoints.find(checkpointId);
        if (it == PendingCheckpoints.end()) {
            YDB_LOG_ERROR("Unknown checkpoint",
                {"coordinatorId", CoordinatorId},
                {"checkpointId", checkpointId},
                {"response", checkpointId});
            return;
        }
        auto& checkpoint = it->second;
        Metrics.AllCheckpointsSizeBytes->Set(ev->Get()->AllCheckpointsSizeBytes);

        InjectCheckpoint(checkpointId, checkpoint.GetType());
    }
}

void TCheckpointCoordinator::InjectCheckpoint(const TCheckpointId& checkpointId, NYql::NDqProto::ECheckpointType type) {
    YDB_LOG_INFO("Checkpoint successfully created, going to inject barriers to actor(s)",
        {"coordinatorId", CoordinatorId},
        {"checkpointId", checkpointId},
        {"actorsToTriggerCount", ActorsToTrigger.size()});
    for (const auto& [toTrigger, transport] : ActorsToTrigger) {
        transport->EventsQueue.Send(new NYql::NDq::TEvDqCompute::TEvInjectCheckpoint(checkpointId.SeqNo, checkpointId.CoordinatorGeneration, type));
    }

    StartAllTasks();
}

void TCheckpointCoordinator::StartAllTasks() {
    if (!GraphIsRunning) {
        YDB_LOG_INFO("Send TEvRun to all actors",
            {"coordinatorId", CoordinatorId});
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

    YDB_LOG_DEBUG("Got TEvSaveTaskStateResult; task",
        {"coordinatorId", CoordinatorId},
        {"checkpointId", checkpointId},
        {"taskId", proto.GetTaskId()},
        {"status", statusName},
        {"size", proto.GetStateSizeBytes()});

    const auto it = PendingCheckpoints.find(checkpointId);
    if (it == PendingCheckpoints.end()) {
        return;
    }
    auto& checkpoint = it->second;

    if (status == NYql::NDqProto::TEvSaveTaskStateResult::OK) {
        checkpoint.Acknowledge(ev->Sender, proto.GetStateSizeBytes());
        YDB_LOG_DEBUG("Task state saved, need more acks",
            {"coordinatorId", CoordinatorId},
            {"checkpointId", checkpointId},
            {"notYetAcknowledgedCount", checkpoint.NotYetAcknowledgedCount()});
    } else {
        checkpoint.Abort(ev->Sender);
        YDB_LOG_ERROR("StorageError: can't save node state, aborting checkpoint",
            {"coordinatorId", CoordinatorId},
            {"checkpointId", checkpointId});
        ++*Metrics.StorageError;
    }
    if (checkpoint.GotAllAcknowledges()) {
        if (checkpoint.GetStats().Aborted) {
            YDB_LOG_ERROR("Got all acks for aborted checkpoint, aborting in storage",
                {"coordinatorId", CoordinatorId},
                {"checkpointId", checkpointId});
            CheckpointingSnapshotRotationIndex = CheckpointingSnapshotRotationPeriod;  // Next checkpoint is snapshot.
            Send(StorageProxy, new TEvCheckpointStorage::TEvAbortCheckpointRequest(CoordinatorId, checkpointId, "Can't save node state"), IEventHandle::FlagTrackDelivery);
            FailedZeroCheckpoint = InitingZeroCheckpoint;
        } else {
            YDB_LOG_INFO("Got all acks, changing checkpoint status to 'PendingCommit'",
                {"coordinatorId", CoordinatorId},
                {"checkpointId", checkpointId});
            Send(StorageProxy, new TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusRequest(CoordinatorId, checkpointId, checkpoint.GetStats().StateSize), IEventHandle::FlagTrackDelivery);
            if (InitingZeroCheckpoint) {
                Send(RunActorId, new TEvCheckpointCoordinator::TEvZeroCheckpointDone());
            }
        }
    }
}

void TCheckpointCoordinator::Handle(const TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusResponse::TPtr& ev) {
    const auto& checkpointId = ev->Get()->CheckpointId;
    const auto issues = ev->Get()->Issues;
    YDB_LOG_DEBUG("Got TEvSetCheckpointPendingCommitStatusResponse",
        {"coordinatorId", CoordinatorId},
        {"checkpointId", checkpointId});
    const auto it = PendingCheckpoints.find(checkpointId);
    if (it == PendingCheckpoints.end()) {
        YDB_LOG_WARN("Got TEvSetCheckpointPendingCommitStatusResponse for checkpoint but it is not in PendingCheckpoints",
            {"coordinatorId", CoordinatorId},
            {"checkpointId", checkpointId});
        return;
    }

    if (issues) {
        YDB_LOG_ERROR("StorageError: can't change checkpoint status",
            {"coordinatorId", CoordinatorId},
            {"checkpointId", checkpointId},
            {"issues", issues});
        ++*Metrics.StorageError;
        PendingCheckpoints.erase(it);
        FailedZeroCheckpoint = InitingZeroCheckpoint;
        return;
    }

    YDB_LOG_INFO("Checkpoint status changed to 'PendingCommit', committing states to actor(s)",
        {"coordinatorId", CoordinatorId},
        {"checkpointId", checkpointId},
        {"actorsToNotifyCount", ActorsToNotify.size()});
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
    YDB_LOG_DEBUG("Got TEvStateCommitted;",
        {"coordinatorId", CoordinatorId},
        {"checkpointId", checkpointId},
        {"task", ev->Get()->Record.GetTaskId()});
    const auto it = PendingCommitCheckpoints.find(checkpointId);
    if (it == PendingCommitCheckpoints.end()) {
        YDB_LOG_WARN("Got TEvStateCommitted for checkpoint but it is not in PendingCommitCheckpoints",
            {"coordinatorId", CoordinatorId},
            {"checkpointId", checkpointId});
        return;
    }

    auto& checkpoint = it->second;
    checkpoint.Acknowledge(ev->Sender);
    YDB_LOG_DEBUG("State committed, need more acks",
        {"coordinatorId", CoordinatorId},
        {"checkpointId", checkpointId},
        {"sender", ev->Sender},
        {"notYetAcknowledgedCount", checkpoint.NotYetAcknowledgedCount()});
    if (checkpoint.GotAllAcknowledges()) {
        YDB_LOG_INFO("Got all acks, changing checkpoint status to 'Completed'",
            {"coordinatorId", CoordinatorId},
            {"checkpointId", checkpointId});
        const auto& stats = checkpoint.GetStats();
        auto durationMs = (TInstant::Now() - stats.CreatedAt).MilliSeconds();
        Metrics.LastCheckpointBarrierDeliveryTimeMillis->Set(durationMs);
        Metrics.CheckpointBarrierDeliveryTimeMillis->Collect(durationMs);
        Send(StorageProxy, new TEvCheckpointStorage::TEvCompleteCheckpointRequest(CoordinatorId, checkpointId, stats.StateSize, checkpoint.GetType()), IEventHandle::FlagTrackDelivery);
    }
}

void TCheckpointCoordinator::Handle(const NYql::NDq::TEvDqCompute::TEvState::TPtr& ev) {
    auto& state = ev->Get()->Record;
    ui64 taskId = state.GetTaskId();
    YDB_LOG_DEBUG("Got TEvState from, task id",
        {"coordinatorId", CoordinatorId},
        {"sender", ev->Sender},
        {"taskId", taskId},
        {"state", state.GetState()});

    if (state.GetState() == NYql::NDqProto::COMPUTE_STATE_FINISHED) {
        FinishedTasks.insert(taskId);
    }
}

void TCheckpointCoordinator::Handle(const TEvCheckpointStorage::TEvCompleteCheckpointResponse::TPtr& ev) {
    const auto& checkpointId = ev->Get()->CheckpointId;
    YDB_LOG_DEBUG("Got TEvCompleteCheckpointResponse",
        {"coordinatorId", CoordinatorId},
        {"checkpointId", checkpointId});
    const auto it = PendingCommitCheckpoints.find(checkpointId);
    if (it == PendingCommitCheckpoints.end()) {
        YDB_LOG_WARN("Got TEvCompleteCheckpointResponse but related checkpoint is not in progress;",
            {"coordinatorId", CoordinatorId},
            {"checkpointId", checkpointId});
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
        YDB_LOG_INFO("Checkpoint completed",
            {"coordinatorId", CoordinatorId},
            {"checkpointId", checkpointId});
    } else {
        ++*Metrics.StorageError;
        YDB_LOG_ERROR("StorageError: can't change checkpoint status",
            {"coordinatorId", CoordinatorId},
            {"checkpointId", checkpointId},
            {"issues'", issues});
    }
    PendingCommitCheckpoints.erase(it);
    UpdateInProgressMetric();
}

void TCheckpointCoordinator::Handle(const TEvCheckpointStorage::TEvAbortCheckpointResponse::TPtr& ev) {
    const auto& checkpointId = ev->Get()->CheckpointId;
    YDB_LOG_DEBUG("Got TEvAbortCheckpointResponse",
        {"coordinatorId", CoordinatorId},
        {"checkpointId", checkpointId});
    const auto& issues = ev->Get()->Issues;
    if (issues) {
        YDB_LOG_ERROR("StorageError: can't abort",
            {"coordinatorId", CoordinatorId},
            {"checkpointId", checkpointId},
            {"checkpoint", issues});
        ++*Metrics.StorageError;
    } else {
        YDB_LOG_WARN("Checkpoint aborted",
            {"coordinatorId", CoordinatorId},
            {"checkpointId", checkpointId});
        ++*Metrics.Aborted;
    }
    PendingCheckpoints.erase(checkpointId);
    FailedZeroCheckpoint = InitingZeroCheckpoint;
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
    YDB_LOG_INFO("Handle disconnected node",
        {"coordinatorId", CoordinatorId},
        {"nodeId", ev->Get()->NodeId});

    for (const auto& [actorId, transport] : AllActors) {
        transport->EventsQueue.HandleNodeDisconnected(ev->Get()->NodeId);
    }
}

void TCheckpointCoordinator::Handle(NActors::TEvInterconnect::TEvNodeConnected::TPtr& ev) {
    YDB_LOG_DEBUG("Handle connected node",
        {"coordinatorId", CoordinatorId},
        {"nodeId", ev->Get()->NodeId});

    for (const auto& [actorId, transport] : AllActors) {
        transport->EventsQueue.HandleNodeConnected(ev->Get()->NodeId);
    }
}

void TCheckpointCoordinator::Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
    TStringBuilder message;
    message << "Undelivered Event " << ev->Get()->SourceType
        << " from " << SelfId() << " (Self) to " << ev->Sender
        << " Reason: " << ev->Get()->Reason << " Cookie: " << ev->Cookie;

    auto it = TaskIds.find(ev->Sender);
    if (it != TaskIds.end() && FinishedTasks.contains(it->second)) {
        YDB_LOG_DEBUG("Ignore undelivered from finished CAs",
            {"coordinatorId", CoordinatorId});
        return;
    }

    YDB_LOG_DEBUG("Dump coordinatorId, message",
        {"coordinatorId", CoordinatorId},
        {"message", message});
    if (const auto actorIt = AllActors.find(ev->Sender); actorIt != AllActors.end()) {
        actorIt->second->EventsQueue.HandleUndelivered(ev);
    }
    OnError(NYql::NDqProto::StatusIds::UNAVAILABLE, message, {});
}

void TCheckpointCoordinator::Handle(const TEvCheckpointCoordinator::TEvRunGraph::TPtr&) {
    YDB_LOG_DEBUG("Got TEvRunGraph",
        {"coordinatorId", CoordinatorId});
    Y_DEBUG_ABORT_UNLESS(InitingZeroCheckpoint);
    Y_DEBUG_ABORT_UNLESS(!FailedZeroCheckpoint);
    InitingZeroCheckpoint = false;
    // TODO: run graph only now, not before zero checkpoint inited
}

void TCheckpointCoordinator::PassAway() {
    YDB_LOG_DEBUG("PassAway",
        {"coordinatorId", CoordinatorId});
    for (const auto& [actorId, transport] : AllActors) {
        transport->EventsQueue.Unsubscribe();
    }
    Metrics.SkippedDueToInFlightLimit->Sub(SkippedDueToInFlightLimitCounter);
    NActors::TActor<TCheckpointCoordinator>::PassAway();
}

void TCheckpointCoordinator::HandleException(const std::exception& err) {
    NYql::TIssues issues;
    issues.AddIssue(err.what());
    OnInternalError("Internal error in checkpoint coordinator", issues);
}

void TCheckpointCoordinator::OnError(NYql::NDqProto::StatusIds::StatusCode statusCode, const TString& message, const NYql::TIssues& subIssues) {
    NYql::TIssue issue(message);
    for (const NYql::TIssue& i : subIssues) {
        issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
    }
    NYql::TIssues issues;
    issues.AddIssue(std::move(issue));
    auto event = std::make_unique<NYql::NDq::TEvDq::TEvAbortExecution>(statusCode, issues);
    TActivationContext::Send(new IEventHandle(ControlId, NActors::TActorId(), event.release()));
}

void TCheckpointCoordinator::OnInternalError(const TString& message, const NYql::TIssues& subIssues) {
    OnError(NYql::NDqProto::StatusIds::INTERNAL_ERROR, message, subIssues);
}

THolder<NActors::IActor> MakeCheckpointCoordinator(
    TCoordinatorId coordinatorId,
    const TActorId& storageProxy,
    const TActorId& runActorId,
    const TCheckpointCoordinatorSettings& config,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    const NProto::TGraphParams& graphParams,
    const FederatedQuery::StateLoadMode& stateLoadMode,
    const FederatedQuery::StreamingDisposition& streamingDisposition)
{
    return MakeHolder<TCheckpointCoordinator>(
        coordinatorId,
        storageProxy,
        runActorId,
        config,
        counters,
        graphParams,
        stateLoadMode,
        streamingDisposition);
}

} // namespace NFq
