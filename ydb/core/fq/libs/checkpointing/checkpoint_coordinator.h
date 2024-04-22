#pragma once

#include "checkpoint_id_generator.h"
#include "pending_checkpoint.h"

#include <ydb/core/fq/libs/checkpointing/events/events.h>
#include <ydb/core/fq/libs/checkpointing_common/defs.h>
#include <ydb/core/fq/libs/checkpoint_storage/events/events.h>

#include <ydb/core/fq/libs/config/protos/checkpoint_coordinator.pb.h>
#include <ydb/public/api/protos/draft/fq.pb.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/dq/actors/compute/retry_queue.h>
#include <ydb/library/yql/providers/dq/actors/events.h>
#include <ydb/library/yql/providers/dq/actors/task_controller_impl.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NFq {

using namespace NActors;
using namespace NFq::NConfig;

class TCheckpointCoordinator : public NYql::TTaskControllerImpl<TCheckpointCoordinator> {
public:
    TCheckpointCoordinator(TCoordinatorId coordinatorId,
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
                           const TDuration& aggrPeriod
                           );

    using NYql::TTaskControllerImpl<TCheckpointCoordinator>::OnInternalError;

    void Handle(NYql::NDqs::TEvReadyState::TPtr&);
    void Handle(const TEvCheckpointStorage::TEvRegisterCoordinatorResponse::TPtr&);
    void Handle(const NYql::NDq::TEvDqCompute::TEvNewCheckpointCoordinatorAck::TPtr&);
    void Handle(const TEvCheckpointStorage::TEvGetCheckpointsMetadataResponse::TPtr&);
    void Handle(const NYql::NDq::TEvDqCompute::TEvRestoreFromCheckpointResult::TPtr&);
    void Handle(const TEvCheckpointCoordinator::TEvScheduleCheckpointing::TPtr&);
    void Handle(const TEvCheckpointStorage::TEvCreateCheckpointResponse::TPtr&);
    void Handle(const NYql::NDq::TEvDqCompute::TEvSaveTaskStateResult::TPtr&);
    void Handle(const TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusResponse::TPtr&);
    void Handle(const NYql::NDq::TEvDqCompute::TEvStateCommitted::TPtr&);
    void Handle(const TEvCheckpointStorage::TEvCompleteCheckpointResponse::TPtr&);
    void Handle(const TEvCheckpointStorage::TEvAbortCheckpointResponse::TPtr&);
    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvRetry::TPtr& ev);
    void Handle(NActors::TEvents::TEvPoison::TPtr&);
    void Handle(NActors::TEvInterconnect::TEvNodeDisconnected::TPtr& ev);
    void Handle(NActors::TEvInterconnect::TEvNodeConnected::TPtr& ev);
    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev);
    void Handle(const TEvCheckpointCoordinator::TEvRunGraph::TPtr&);
    void HandleException(const std::exception& err);


    STRICT_STFUNC_EXC(DispatchEvent,
        hFunc(NYql::NDqs::TEvReadyState, Handle)
        hFunc(NYql::NDqs::TEvQueryResponse, NYql::TTaskControllerImpl<TCheckpointCoordinator>::OnQueryResult)
        hFunc(NYql::NDqs::TEvDqFailure, NYql::TTaskControllerImpl<TCheckpointCoordinator>::OnResultFailure)

        hFunc(TEvCheckpointCoordinator::TEvScheduleCheckpointing, Handle)
        hFunc(TEvCheckpointCoordinator::TEvRunGraph, Handle)

        hFunc(TEvCheckpointStorage::TEvRegisterCoordinatorResponse, Handle)
        hFunc(TEvCheckpointStorage::TEvGetCheckpointsMetadataResponse, Handle)
        hFunc(TEvCheckpointStorage::TEvCreateCheckpointResponse, Handle)
        hFunc(TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusResponse, Handle)
        hFunc(TEvCheckpointStorage::TEvCompleteCheckpointResponse, Handle)
        hFunc(TEvCheckpointStorage::TEvAbortCheckpointResponse, Handle)
    
        hFunc(NYql::NDq::TEvDqCompute::TEvNewCheckpointCoordinatorAck, Handle)
        hFunc(NYql::NDq::TEvDqCompute::TEvRestoreFromCheckpointResult, Handle)
        hFunc(NYql::NDq::TEvDqCompute::TEvSaveTaskStateResult, Handle)
        hFunc(NYql::NDq::TEvDqCompute::TEvStateCommitted, Handle)
        hFunc(NYql::NDq::TEvDqCompute::TEvState, NYql::TTaskControllerImpl<TCheckpointCoordinator>::OnComputeActorState)
        hFunc(NYql::NDq::TEvDq::TEvAbortExecution, NYql::TTaskControllerImpl<TCheckpointCoordinator>::OnAbortExecution)

        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvRetry, Handle)

        hFunc(NActors::TEvents::TEvPoison, Handle)
        hFunc(NActors::TEvents::TEvWakeup, NYql::TTaskControllerImpl<TCheckpointCoordinator>::OnWakeup)

        hFunc(NActors::TEvInterconnect::TEvNodeDisconnected, Handle)
        hFunc(NActors::TEvInterconnect::TEvNodeConnected, Handle)
        hFunc(NActors::TEvents::TEvUndelivered, Handle)

        , ExceptionFunc(std::exception, HandleException)
    )

    static constexpr char ActorName[] = "YQ_CHECKPOINT_COORDINATOR";

private:
    void InitCheckpoint();
    void InjectCheckpoint(const TCheckpointId& checkpointId, NYql::NDqProto::ECheckpointType type);
    void ScheduleNextCheckpoint();
    void UpdateInProgressMetric();
    void PassAway() override;
    void RestoreFromOwnCheckpoint(const TCheckpointMetadata& checkpoint);
    void TryToRestoreOffsetsFromForeignCheckpoint(const TCheckpointMetadata& checkpoint);

    template <class TEvPtr>
    bool OnComputeActorEventReceived(TEvPtr& ev) {
        const auto actorIt = AllActors.find(ev->Sender);
        Y_ABORT_UNLESS(actorIt != AllActors.end());
        return actorIt->second->EventsQueue.OnEventReceived(ev);
    }

    struct TCheckpointCoordinatorMetrics {
        TCheckpointCoordinatorMetrics(const ::NMonitoring::TDynamicCounterPtr& counters) {
            auto subgroup = counters->GetSubgroup("subsystem", "checkpoint_coordinator");
            InProgress = subgroup->GetCounter("InProgress");
            Pending = subgroup->GetCounter("Pending");
            PendingCommit = subgroup->GetCounter("PendingCommit");
            Completed = subgroup->GetCounter("CompletedCheckpoints", true);
            Aborted = subgroup->GetCounter("AbortedCheckpoints", true);
            StorageError = subgroup->GetCounter("StorageError", true);
            FailedToCreate = subgroup->GetCounter("FailedToCreate", true);
            RestoringError = subgroup->GetCounter("RestoringError", true);
            Total = subgroup->GetCounter("TotalCheckpoints", true);
            LastCheckpointBarrierDeliveryTimeMillis = subgroup->GetCounter("LastCheckpointBarrierDeliveryTimeMillis");
            LastCheckpointDurationMillis = subgroup->GetCounter("LastSuccessfulCheckpointDurationMillis");
            LastCheckpointSizeBytes = subgroup->GetCounter("LastSuccessfulCheckpointSizeBytes");
            CheckpointBarrierDeliveryTimeMillis = subgroup->GetHistogram("CheckpointBarrierDeliveryTimeMillis", NMonitoring::ExponentialHistogram(12, 2, 1024)); // ~ 1s -> ~ 1 h
            CheckpointDurationMillis = subgroup->GetHistogram("CheckpointDurationMillis", NMonitoring::ExponentialHistogram(12, 2, 1024)); // ~ 1s -> ~ 1 h
            CheckpointSizeBytes = subgroup->GetHistogram("CheckpointSizeBytes", NMonitoring::ExponentialHistogram(8, 32, 32));             // 32b -> 1Tb
            SkippedDueToInFlightLimit = subgroup->GetCounter("SkippedDueToInFlightLimit");
            RestoredFromSavedCheckpoint = subgroup->GetCounter("RestoredFromSavedCheckpoint", true);
            StartedFromEmptyCheckpoint = subgroup->GetCounter("StartedFromEmptyCheckpoint", true);
            RestoredStreamingOffsetsFromCheckpoint = subgroup->GetCounter("RestoredStreamingOffsetsFromCheckpoint", true);
        }

        ::NMonitoring::TDynamicCounters::TCounterPtr InProgress;
        ::NMonitoring::TDynamicCounters::TCounterPtr Pending;
        ::NMonitoring::TDynamicCounters::TCounterPtr PendingCommit;
        ::NMonitoring::TDynamicCounters::TCounterPtr Completed;
        ::NMonitoring::TDynamicCounters::TCounterPtr Aborted;
        ::NMonitoring::TDynamicCounters::TCounterPtr StorageError;
        ::NMonitoring::TDynamicCounters::TCounterPtr FailedToCreate;
        ::NMonitoring::TDynamicCounters::TCounterPtr RestoringError;
        ::NMonitoring::TDynamicCounters::TCounterPtr Total;
        ::NMonitoring::TDynamicCounters::TCounterPtr LastCheckpointBarrierDeliveryTimeMillis;
        ::NMonitoring::TDynamicCounters::TCounterPtr LastCheckpointDurationMillis;
        ::NMonitoring::TDynamicCounters::TCounterPtr LastCheckpointSizeBytes;
        ::NMonitoring::TDynamicCounters::TCounterPtr SkippedDueToInFlightLimit;
        ::NMonitoring::TDynamicCounters::TCounterPtr RestoredFromSavedCheckpoint;
        ::NMonitoring::TDynamicCounters::TCounterPtr StartedFromEmptyCheckpoint;
        ::NMonitoring::TDynamicCounters::TCounterPtr RestoredStreamingOffsetsFromCheckpoint;
        NMonitoring::THistogramPtr CheckpointBarrierDeliveryTimeMillis;
        NMonitoring::THistogramPtr CheckpointDurationMillis;
        NMonitoring::THistogramPtr CheckpointSizeBytes;
    };

    struct TComputeActorTransportStuff : public TSimpleRefCount<TComputeActorTransportStuff> {
        using TPtr = TIntrusivePtr<TComputeActorTransportStuff>;

        NYql::NDq::TRetryEventsQueue EventsQueue;
    };

    const TCoordinatorId CoordinatorId;
    const TActorId StorageProxy;
    const TActorId RunActorId;
    std::unique_ptr<TCheckpointIdGenerator> CheckpointIdGenerator;
    TCheckpointCoordinatorConfig Settings;
    const TDuration CheckpointingPeriod;
    ui64 CheckpointingSnapshotRotationPeriod = 0;
    ui64 CheckpointingSnapshotRotationIndex = 0;
    const NProto::TGraphParams GraphParams;
    TString GraphDescId;

    THashMap<TActorId, TComputeActorTransportStuff::TPtr> AllActors;
    THashSet<TActorId> AllActorsSet;
    THashMap<TActorId, TComputeActorTransportStuff::TPtr> ActorsToTrigger;
    THashMap<TActorId, TComputeActorTransportStuff::TPtr> ActorsToWaitFor;
    THashSet<TActorId> ActorsToWaitForSet;
    THashMap<TActorId, TComputeActorTransportStuff::TPtr> ActorsToNotify;
    THashSet<TActorId> ActorsToNotifySet;
    THashMap<ui64, TActorId> TaskIdToActor; // Task id -> actor.
    THashMap<TCheckpointId, TPendingCheckpoint, TCheckpointIdHash> PendingCheckpoints;
    THashMap<TCheckpointId, TPendingCheckpoint, TCheckpointIdHash> PendingCommitCheckpoints;
    TMaybe<TPendingRestoreCheckpoint> PendingRestoreCheckpoint;
    std::unique_ptr<TPendingInitCoordinator> PendingInit;
    bool GraphIsRunning = false;
    bool InitingZeroCheckpoint = false;
    bool RestoringFromForeignCheckpoint = false;

    TCheckpointCoordinatorMetrics Metrics;

    FederatedQuery::StateLoadMode StateLoadMode;
    FederatedQuery::StreamingDisposition StreamingDisposition;
};

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
    );

} // namespace NFq
