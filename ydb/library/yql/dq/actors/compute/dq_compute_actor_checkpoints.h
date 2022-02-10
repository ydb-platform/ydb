#pragma once

#include "dq_compute_actor.h"
#include "dq_compute_actor_sinks.h"
#include "retry_queue.h"

#include <ydb/library/yql/dq/common/dq_common.h> 

#include <library/cpp/actors/core/log.h>

#include <util/generic/ptr.h>

#include <algorithm>
#include <deque>
#include <type_traits>

namespace NYql::NDqProto {
enum ECheckpointingMode;
} // namespace NYql::NDqProto

namespace NYql::NDq {

NDqProto::ECheckpointingMode GetTaskCheckpointingMode(const NDqProto::TDqTask& task);

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
        TPendingCheckpoint(const NDqProto::TDqTask& task)
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
            Y_VERIFY(Checkpoint);
            return SavedComputeActorState && SinksCount == SavedSinkStatesCount;
        }

        static size_t GetSinksCount(const NDqProto::TDqTask& task);

        const size_t SinksCount;
        TMaybe<NDqProto::TCheckpoint> Checkpoint;
        NDqProto::TComputeActorState ComputeActorState;
        size_t SavedSinkStatesCount = 0;
        bool SavedComputeActorState = false;
    };

public:
    static constexpr char ActorName[] = "DQ_COMPUTE_ACTOR_CHECKPOINTS";

    struct ICallbacks {
        [[nodiscard]]
        virtual bool ReadyToCheckpoint() const = 0;
        virtual void SaveState(const NDqProto::TCheckpoint& checkpoint, NDqProto::TComputeActorState& state) const = 0;
        virtual void CommitState(const NDqProto::TCheckpoint& checkpoint) = 0;
        virtual void InjectBarrierToOutputs(const NDqProto::TCheckpoint& checkpoint) = 0;
        virtual void ResumeInputs() = 0;

        virtual void Start() = 0;
        virtual void Stop() = 0;
        virtual void ResumeExecution() = 0;

        virtual void LoadState(const NDqProto::TComputeActorState& state) = 0;

        virtual ~ICallbacks() = default;
    };

    TDqComputeActorCheckpoints(const TTxId& txId, NDqProto::TDqTask task, ICallbacks* computeActor);
    void Init(NActors::TActorId computeActorId, NActors::TActorId checkpointsId);
    [[nodiscard]]
    bool HasPendingCheckpoint() const;
    bool ComputeActorStateSaved() const;
    void DoCheckpoint();
    bool SaveState();
    void RegisterCheckpoint(const NDqProto::TCheckpoint& checkpoint, ui64 channelId);

    // Sink actor support.
    void OnSinkStateSaved(NDqProto::TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint);

    void TryToSavePendingCheckpoint();

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
    void Handle(TEvRetryQueuePrivate::TEvRetry::TPtr& ev);

    void PassAway() override;

    // Validates generation and returns true if it is from old coordinator.
    template <class E>
    bool ShouldIgnoreOldCoordinator(const E& ev, bool verifyOnGenerationFromFuture = true);

private:
    const TTxId TxId;
    const NDqProto::TDqTask Task;
    const bool IngressTask;

    const NActors::TActorId CheckpointStorage;
    TString GraphId;

    ICallbacks* ComputeActor = nullptr;

    TMaybe<TCheckpointCoordinatorId> CheckpointCoordinator;
    TPendingCheckpoint PendingCheckpoint;
    TRetryEventsQueue EventsQueue;

    // Restore
    NYql::NDqProto::NDqStateLoadPlan::TTaskPlan TaskLoadPlan;
};

} // namespace NYql::NDq
