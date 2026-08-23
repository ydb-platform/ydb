#pragma once

#include "dq_compute_actor.h"
#include "dq_compute_actor_async_io.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/yql/dq/actors/common/retry_queue.h>
#include <ydb/library/yql/dq/common/dq_common.h>

#include <util/generic/ptr.h>

namespace NYql {

namespace NDqProto {

enum ECheckpointingMode : int;

} // namespace NDqProto

namespace NDq {

/*

Requirements for MKQL Node Compatibility with Checkpoints

- A node must either support checkpoint creation or be stateless.
- When a node returns `Yield`, any state not saved in the checkpoint must already have been drained from the MKQL node. (e.g. nodes that use spilling are not compatible)
- A node may return `Yield` only after receiving `Yield` from all streaming inputs initialized during the current run.

  **Note:** If a stateful operator is located in an uninitialized input, checkpoint creation will not succeed, and the coordinator will wait for the next checkpoint.

These requirements also ensure compatibility with watermarks.

*/

class TDqComputeActorCheckpoints : public NActors::TActor<TDqComputeActorCheckpoints> {
    struct TCheckpointCoordinatorId {
        NActors::TActorId ActorId;
        ui64 Generation;

        TCheckpointCoordinatorId(NActors::TActorId actorId, ui64 generation)
            : ActorId(actorId)
            , Generation(generation)
        {}
    };

    class TPendingCheckpointBase {
    public:
        const size_t SinksCount = 0;
        size_t SavedSinkStatesCount = 0;
        TMaybe<NDqProto::TCheckpoint> Checkpoint;
        TInstant CheckpointStartTime;

        explicit TPendingCheckpointBase(const TDqTaskSettings& task);

        // New checkpoint (clears previously saved data).
        TPendingCheckpointBase& operator=(const NDqProto::TCheckpoint& checkpoint);

        operator bool() const;

        bool IsSlowCheckpoint(TDuration& duration) const;

        virtual void Clear();

        virtual bool IsReady() const;

    private:
        static size_t GetSinksCount(const TDqTaskSettings& task);
    };

    class TPendingStateSavingCheckpoint final : public TPendingCheckpointBase {
        using TBase = TPendingCheckpointBase;

    public:
        TComputeActorState ComputeActorState;
        bool SavedComputeActorState = false;
        bool SavingToDatabase = false;

        using TBase::TBase;
        using TBase::operator=;

        void Clear() final;

        bool IsReady() const final;
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
        virtual TString GetTaskDebugState() const = 0;

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
    TPendingStateSavingCheckpoint PendingSaveStateCheckpoint;
    TRetryEventsQueue EventsQueue;

    // Restore
    NDqProto::NDqStateLoadPlan::TTaskPlan StateLoadPlan;
    NDqProto::TCheckpoint RestoringTaskRunnerForCheckpoint;
    ui64 RestoringTaskRunnerForEvent;

    bool SlowCheckpointsMonitoringStarted = false;
};

NDqProto::ECheckpointingMode GetTaskCheckpointingMode(const TDqTaskSettings& task);

bool IsIngress(const TDqTaskSettings& task);

bool IsEgress(const TDqTaskSettings& task);

bool HasState(const TDqTaskSettings& task);

} // namespace NDq

} // namespace NYql
