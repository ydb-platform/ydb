#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_DATASHARD

namespace NKikimr {
namespace NDataShard {

class TPlanQueueUnit : public TExecutionUnit {
public:
    TPlanQueueUnit(TDataShard &dataShard,
                   TPipeline &pipeline);
    ~TPlanQueueUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    TOperation::TPtr FindReadyOperation() const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
};

TPlanQueueUnit::TPlanQueueUnit(TDataShard &dataShard,
                               TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::PlanQueue, false, dataShard, pipeline)
{
}

TPlanQueueUnit::~TPlanQueueUnit()
{
}

bool TPlanQueueUnit::IsReadyToExecute(TOperation::TPtr op) const
{
    if (Pipeline.OutOfOrderLimits()) {
        op->TracePipelineEvent("plan queue: out-of-order limit");
        return false;
    }
    if (!Pipeline.CanRunOp(*op)) {
        op->TracePipelineEvent("plan queue: operation cannot run");
        return false;
    }

    auto step = Pipeline.GetLastActivePlannedOpStep();
    auto txId = Pipeline.GetLastActivePlannedOpId();
    const auto next = Pipeline.GetNextPlannedOp(step, txId);
    const bool ready = op == next;
    op->TracePipelineEvent(ready ? "plan queue: ready" : "plan queue: preceding operation",
        ready ? nullptr : next.Get());
    return ready;
}

TOperation::TPtr TPlanQueueUnit::FindReadyOperation() const
{
    if (Pipeline.OutOfOrderLimits()) {
        YDB_LOG_TRACE_CTX(TActivationContext::AsActorContext(), "TPlanQueueUnit::FindReadyOperation: out-of-order limits exceeded",
            {"tabletId", DataShard.TabletID()});
        return nullptr;
    }

    if (!OpsInFly.size()) {
        YDB_LOG_TRACE_CTX(TActivationContext::AsActorContext(), "TPlanQueueUnit::FindReadyOperation: has no attached operations",
            {"tabletId", DataShard.TabletID()});
        return nullptr;
    }

    auto step = Pipeline.GetLastActivePlannedOpStep();
    auto txId = Pipeline.GetLastActivePlannedOpId();
    auto op = Pipeline.GetNextPlannedOp(step, txId);

    if (!op) {
        YDB_LOG_TRACE_CTX(TActivationContext::AsActorContext(), "TPlanQueueUnit::FindReadyOperation: couldn't find next planned operation",
            {"tabletId", DataShard.TabletID()},
            {"step", step},
            {"txId", txId});
        return nullptr;
    }

    if (op->IsInProgress()) {
        op->TracePipelineEvent("plan queue: already in progress");
        YDB_LOG_TRACE_CTX(TActivationContext::AsActorContext(), "TPlanQueueUnit::FindReadyOperation: found next planned operation is already in progress",
            {"tabletId", DataShard.TabletID()},
            {"operation", *op});
        return nullptr;
    }

    if (!Pipeline.CanRunOp(*op)) {
        op->TracePipelineEvent("plan queue: operation cannot run");
        YDB_LOG_TRACE_CTX(TActivationContext::AsActorContext(), "TPlanQueueUnit::FindReadyOperation: cannot run found next planned operation",
            {"tabletId", DataShard.TabletID()},
            {"operation", *op});
        return nullptr;
    }

    if (op->GetCurrentUnit() != Kind) {
        op->TracePipelineEvent("plan queue: another unit");
        YDB_LOG_TRACE_CTX(TActivationContext::AsActorContext(), "TPlanQueueUnit::FindReadyOperation: found next planned operation is executing on another unit",
            {"tabletId", DataShard.TabletID()},
            {"operation", *op},
            {"currentUnit", op->GetCurrentUnit()});
        return nullptr;
    }

    op->TracePipelineEvent("plan queue: ready candidate");
    return op;
}

EExecutionStatus TPlanQueueUnit::Execute(TOperation::TPtr op,
                                         TTransactionContext &,
                                         const TActorContext &)
{
    DataShard.IncCounter(COUNTER_PLAN_QUEUE_LATENCY_MS, op->GetCurrentElapsedAndReset().MilliSeconds());
    return EExecutionStatus::Executed;
}

void TPlanQueueUnit::Complete(TOperation::TPtr,
                              const TActorContext &)
{
}

THolder<TExecutionUnit> CreatePlanQueueUnit(TDataShard &dataShard,
                                            TPipeline &pipeline)
{
    return THolder(new TPlanQueueUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr


#undef YDB_LOG_THIS_FILE_COMPONENT

