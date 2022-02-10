#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

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
        LOG_TRACE_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                    "TPlanQueueUnit at " << DataShard.TabletID()
                    << " out-of-order limits exceeded");
        return nullptr;
    }

    if (!OpsInFly.size()) {
        LOG_TRACE_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                    "TPlanQueueUnit at " << DataShard.TabletID()
                    << " has no attached operations");
        return nullptr;
    }

    auto step = Pipeline.GetLastActivePlannedOpStep();
    auto txId = Pipeline.GetLastActivePlannedOpId();
    auto op = Pipeline.GetNextPlannedOp(step, txId);

    if (!op) {
        LOG_TRACE_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                    "TPlanQueueUnit at " << DataShard.TabletID()
                    << " couldn't find next planned operation after ["
                    << step << ":" << txId << "]");
        return nullptr;
    }

    if (op->IsInProgress()) {
        LOG_TRACE_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                    "TPlanQueueUnit at " << DataShard.TabletID()
                    << " found next planned operation " << *op << " which is already in progress");
        return nullptr;
    }

    if (!Pipeline.CanRunOp(*op)) {
        LOG_TRACE_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                    "TPlanQueueUnit at " << DataShard.TabletID()
                    << " cannot run found next planned operation " << *op);
        return nullptr;
    }

    if (op->GetCurrentUnit() != Kind) {
        LOG_TRACE_S(TActivationContext::AsActorContext(), NKikimrServices::TX_DATASHARD,
                    "TPlanQueueUnit at " << DataShard.TabletID()
                    << " found next planned operation " << *op
                    << " is executing on unit " << op->GetCurrentUnit());
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
