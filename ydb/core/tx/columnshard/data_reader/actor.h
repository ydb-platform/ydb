#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>

namespace NKikimr::NOlap::NDataReader {

class IRestoreTask {
private:
    YDB_READONLY_DEF(TString, TaskId);
    YDB_READONLY(ui64, TabletId, 0);
    YDB_READONLY_DEF(NActors::TActorId, TabletActorId);
    virtual TConclusionStatus DoOnDataChunk(const std::shared_ptr<arrow::Table>& data) = 0;
    virtual TConclusionStatus DoOnFinished() = 0;
    virtual void DoOnError(const TString& errorMessage) = 0;
    virtual std::unique_ptr<TEvColumnShard::TEvInternalScan> DoBuildRequestInitiator() const = 0;

public:
    virtual bool IsActive() const = 0;
    virtual TDuration GetTimeout() const = 0;

    TConclusionStatus OnDataChunk(const std::shared_ptr<arrow::Table>& data) {
        AFL_VERIFY(data->num_rows());
        return DoOnDataChunk(data);
    }

    TConclusionStatus OnFinished() {
        return DoOnFinished();
    }

    void OnError(const TString& errorMessage) {
        DoOnError(errorMessage);
    }

    std::unique_ptr<TEvColumnShard::TEvInternalScan> BuildRequestInitiator() const {
        return DoBuildRequestInitiator();
    }

    IRestoreTask(const ui64 tabletId, const NActors::TActorId& tabletActorId, const TString& taskId)
        : TaskId(taskId)
        , TabletId(tabletId)
        , TabletActorId(tabletActorId)
    {

    }

    virtual ~IRestoreTask() = default;
};

class TActor: public NActors::TActorBootstrapped<TActor> {
private:
    using TBase = NActors::TActorBootstrapped<TActor>;

    enum class EStage {
        Initialization,
        WaitData,
        Finished
    };

    std::optional<TActorId> ScanActorId;

    std::shared_ptr<IRestoreTask> RestoreTask;

    EStage Stage = EStage::Initialization;
    static inline const ui64 FreeSpace = ((ui64)8) << 20;
    void SwitchStage(const std::optional<EStage> from, const EStage to) {
        if (from) {
            AFL_VERIFY(Stage == *from)("from", (ui32)*from)("real", (ui32)Stage)("to", (ui32)to);
        }
        Stage = to;
    }
    std::optional<TMonotonic> LastAck;
    bool AbortedFlag = false;
    bool CheckActivity();

protected:
    void HandleExecute(NKqp::TEvKqpCompute::TEvScanInitActor::TPtr& ev);
    void HandleExecute(NKqp::TEvKqpCompute::TEvScanData::TPtr& ev);
    void HandleExecute(NKqp::TEvKqpCompute::TEvScanError::TPtr& ev);
    void HandleExecute(NActors::TEvents::TEvUndelivered::TPtr& ev);
    void HandleExecute(NActors::TEvents::TEvWakeup::TPtr& ev);

public:
    TActor(const std::shared_ptr<IRestoreTask>& rTask)
        : RestoreTask(rTask)
    {
        AFL_VERIFY(RestoreTask);
    }

    STATEFN(StateFunc) {
        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("tablet_id", RestoreTask->GetTabletId())("tablet_actor_id",
            RestoreTask->GetTabletActorId())("this", (ui64)this)("activity", RestoreTask->IsActive())("task_id", RestoreTask->GetTaskId());
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(NKqp::TEvKqpCompute::TEvScanInitActor, HandleExecute);
                hFunc(NKqp::TEvKqpCompute::TEvScanData, HandleExecute);
                hFunc(NKqp::TEvKqpCompute::TEvScanError, HandleExecute);
                hFunc(NActors::TEvents::TEvUndelivered, HandleExecute);
                hFunc(NActors::TEvents::TEvWakeup, HandleExecute);
                default:
                    AFL_VERIFY(false)("type", ev->GetTypeName());
            }
        } catch (...) {
            AFL_VERIFY(false);
        }
    }

    void Bootstrap(const TActorContext& ctx);
};

}   // namespace NKikimr::NOlap::NExport
