#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TWaitForPlanUnit : public TExecutionUnit {
public:
    TWaitForPlanUnit(TDataShard &dataShard,
                     TPipeline &pipeline);
    ~TWaitForPlanUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
};

TWaitForPlanUnit::TWaitForPlanUnit(TDataShard &dataShard,
                                   TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::WaitForPlan, false, dataShard, pipeline)
{
}

TWaitForPlanUnit::~TWaitForPlanUnit()
{
}

bool TWaitForPlanUnit::IsReadyToExecute(TOperation::TPtr op) const
{
    return op->GetStep();
}

EExecutionStatus TWaitForPlanUnit::Execute(TOperation::TPtr op,
                                           TTransactionContext &,
                                           const TActorContext &)
{
    op->ResetCurrentTimer();
    return EExecutionStatus::Executed;
}

void TWaitForPlanUnit::Complete(TOperation::TPtr,
                                const TActorContext &)
{
}

THolder<TExecutionUnit> CreateWaitForPlanUnit(TDataShard &dataShard,
                                              TPipeline &pipeline)
{
    return MakeHolder<TWaitForPlanUnit>(dataShard, pipeline);
}

} // namespace NDataShard
} // namespace NKikimr
