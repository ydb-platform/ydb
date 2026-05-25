#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

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
    if (Pipeline.OutOfOrderLimits())
        return false;
    if (!Pipeline.CanRunOp(*op))
        return false;

    auto step = Pipeline.GetLastActivePlannedOpStep();
    auto txId = Pipeline.GetLastActivePlannedOpId();
    return op == Pipeline.GetNextPlannedOp(step, txId);
}

TOperation::TPtr TPlanQueueUnit::FindReadyOperation() const
{
    if (Pipeline.OutOfOrderLimits()) {
        YDB_LOG_CTX_TRACE(TActivationContext::AsActorContext(), "TPlanQueueUnit at out-of-order limits exceeded",
            {"TabletID", DataShard.TabletID()});
        return nullptr;
    }

    if (!OpsInFly.size()) {
        YDB_LOG_CTX_TRACE(TActivationContext::AsActorContext(), "TPlanQueueUnit at has no attached operations",
            {"TabletID", DataShard.TabletID()});
        return nullptr;
    }

    auto step = Pipeline.GetLastActivePlannedOpStep();
    auto txId = Pipeline.GetLastActivePlannedOpId();
    auto op = Pipeline.GetNextPlannedOp(step, txId);

    if (!op) {
        YDB_LOG_CTX_TRACE(TActivationContext::AsActorContext(), "TPlanQueueUnit at couldn't find next planned operation after [",
            {"TabletID", DataShard.TabletID()},
            {"step", step},
            {"txId", txId});
        return nullptr;
    }

    if (op->IsInProgress()) {
        YDB_LOG_CTX_TRACE(TActivationContext::AsActorContext(), "TPlanQueueUnit at found next planned operation which is already in progress",
            {"TabletID", DataShard.TabletID()},
            {"#_*op", *op});
        return nullptr;
    }

    if (!Pipeline.CanRunOp(*op)) {
        YDB_LOG_CTX_TRACE(TActivationContext::AsActorContext(), "TPlanQueueUnit at cannot run found next planned operation",
            {"TabletID", DataShard.TabletID()},
            {"#_*op", *op});
        return nullptr;
    }

    if (op->GetCurrentUnit() != Kind) {
        YDB_LOG_CTX_TRACE(TActivationContext::AsActorContext(), "TPlanQueueUnit at found next planned operation is executing on unit",
            {"TabletID", DataShard.TabletID()},
            {"#_*op", *op},
            {"GetCurrentUnit", op->GetCurrentUnit()});
        return nullptr;
    }

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
