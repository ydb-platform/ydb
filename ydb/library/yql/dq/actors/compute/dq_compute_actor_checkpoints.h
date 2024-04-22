#pragma once

#include "dq_compute_actor.h"
#include "dq_compute_actor_async_io.h"
#include "retry_queue.h"

#include <ydb/library/yql/dq/common/dq_common.h>

#include <ydb/library/actors/core/log.h>

#include <util/generic/ptr.h>

#include <algorithm>
#include <deque>
#include <type_traits>

namespace NYql::NDqProto {
enum ECheckpointingMode : int;
} // namespace NYql::NDqProto

namespace NYql::NDq {

NDqProto::ECheckpointingMode GetTaskCheckpointingMode(const TDqTaskSettings& task);

class TDqComputeActorCheckpoints : public NActors::TActor<TDqComputeActorCheckpoints>
{
    struct TCheckpointCoordinatorId {
        NActors::TActorId ActorId;
        ui64 Generation;

        TCheckpointCoordinatorId(NActors::TActorId actorId, ui64 generation)
            : ActorId(actorId)
            , Generation(generation) {
        }
    };

    struct TPendingCheckpoint {
        TPendingCheckpoint(const TDqTaskSettings& task)
            : SinksCount(GetSinksCount(task))
        {
        }

        // New checkpoint (clears previously saved data).
        TPendingCheckpoint& operator=(const NDqProto::TCheckpoint& checkpoint);

        operator bool() const {
            return Checkpoint.Defined();
        }

        void Clear();

        bool IsReady() const {
            Y_ABORT_UNLESS(Checkpoint);
            return SavedComputeActorState && SinksCount == SavedSinkStatesCount;
        }

        static size_t GetSinksCount(const TDqTaskSettings& task);

        const size_t SinksCount;
        TMaybe<NDqProto::TCheckpoint> Checkpoint;
        TComputeActorState ComputeActorState;
        size_t SavedSinkStatesCount = 0;
        bool SavedComputeActorState = false;
    };

public:
    static constexpr char ActorName[] = "DQ_COMPUTE_ACTOR_CHECKPOINTS";

    struct ICallbacks {
        [[nodiscard]]
        virtual bool ReadyToCheckpoint() const = 0;
        virtual void SaveState(const NDqProto::TCheckpoint& checkpoint, TComputeActorState& state) const = 0;
        virtual void CommitState(const NDqProto::TCheckpoint& checkpoint) = 0;
        virtual void InjectBarrierToOutputs(const NDqProto::TCheckpoint& checkpoint) = 0;
        virtual void ResumeInputsByCheckpoint() = 0;

        virtual void Start() = 0;
        virtual void Stop() = 0;
        virtual void ResumeExecution(EResumeSource source) = 0;

        virtual void LoadState(TComputeActorState&& state) = 0;

        virtual ~ICallbacks() = default;
    };

    enum : ui64
    {
        ComputeActorNonProtobufStateVersion = 1,
        ComputeActorCurrentStateVersion = 2,
    };

    TDqComputeActorCheckpoints(const NActors::TActorId& owner, const TTxId& txId, TDqTaskSettings task, ICallbacks* computeActor);
    void Init(NActors::TActorId computeActorId, NActors::TActorId checkpointsId);
    [[nodiscard]]
    bool HasPendingCheckpoint() const;
    bool ComputeActorStateSaved() const;
    void DoCheckpoint();
    bool SaveState();
    NDqProto::TCheckpoint GetPendingCheckpoint() const;
    void RegisterCheckpoint(const NDqProto::TCheckpoint& checkpoint, ui64 channelId);
    void StartCheckpoint(const NDqProto::TCheckpoint& checkpoint);
    void AbortCheckpoint();

    // Sink support.
    void OnSinkStateSaved(TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint);

    void OnTransformStateSaved(TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint) {
        Y_UNUSED(state);
        Y_UNUSED(outputIndex); // Note that we can have both sink and transform on one output index
        Y_UNUSED(checkpoint);
        Y_ABORT("Transform states are unimplemented");
    }

    void TryToSavePendingCheckpoint();

    void AfterStateLoading(const TMaybe<TString>& error);

private:
    STATEFN(StateFunc);
    void Handle(TEvDqCompute::TEvNewCheckpointCoordinator::TPtr&);
    void Handle(TEvDqCompute::TEvInjectCheckpoint::TPtr&);
    void Handle(TEvDqCompute::TEvSaveTaskStateResult::TPtr&);
    void Handle(TEvDqCompute::TEvCommitState::TPtr&);
    void Handle(TEvDqCompute::TEvRestoreFromCheckpoint::TPtr&);
    void Handle(TEvDqCompute::TEvGetTaskStateResult::TPtr&);
    void Handle(TEvDqCompute::TEvRun::TPtr& ev);
    void Handle(NActors::TEvents::TEvPoison::TPtr&);
    void Handle(NActors::TEvInterconnect::TEvNodeDisconnected::TPtr& ev);
    void Handle(NActors::TEvInterconnect::TEvNodeConnected::TPtr& ev);
    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev);
    void Handle(TEvRetryQueuePrivate::TEvRetry::TPtr& ev);
    void Handle(NActors::TEvents::TEvWakeup::TPtr& ev);
    void HandleException(const std::exception& err);

    void PassAway() override;

    // Validates generation and returns true if it is from old coordinator.
    template <class E>
    bool ShouldIgnoreOldCoordinator(const E& ev, bool verifyOnGenerationFromFuture = true);

private:
    const NActors::TActorId Owner;
    const TTxId TxId;
    const TDqTaskSettings Task;
    const bool IngressTask;

    const NActors::TActorId CheckpointStorage;
    TString GraphId;

    ICallbacks* ComputeActor = nullptr;

    TMaybe<TCheckpointCoordinatorId> CheckpointCoordinator;
    TPendingCheckpoint PendingCheckpoint;
    TRetryEventsQueue EventsQueue;

    // Restore
    NYql::NDqProto::NDqStateLoadPlan::TTaskPlan StateLoadPlan;
    NDqProto::TCheckpoint RestoringTaskRunnerForCheckpoint;
    ui64 RestoringTaskRunnerForEvent;

    bool SlowCheckpointsMonitoringStarted = false;
    TInstant CheckpointStartTime;
    bool SavingToDatabase = false;
};

} // namespace NYql::NDq
