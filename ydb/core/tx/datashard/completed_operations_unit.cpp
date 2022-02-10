#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TCompletedOperationsUnit : public TExecutionUnit {
public:
    TCompletedOperationsUnit(TDataShard &dataShard,
                             TPipeline &pipeline);
    ~TCompletedOperationsUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
};

TCompletedOperationsUnit::TCompletedOperationsUnit(TDataShard &dataShard,
                                                   TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::CompletedOperations, false, dataShard, pipeline)
{
}

TCompletedOperationsUnit::~TCompletedOperationsUnit()
{
}

bool TCompletedOperationsUnit::IsReadyToExecute(TOperation::TPtr op) const
{
    Y_UNUSED(op);
    return true;
}

EExecutionStatus TCompletedOperationsUnit::Execute(TOperation::TPtr op,
                                                   TTransactionContext &,
                                                   const TActorContext &)
{
    Pipeline.RemoveActiveOp(op);

    return EExecutionStatus::Executed;
}

void TCompletedOperationsUnit::Complete(TOperation::TPtr,
                                        const TActorContext &)
{
}

THolder<TExecutionUnit> CreateCompletedOperationsUnit(TDataShard &dataShard,
                                                      TPipeline &pipeline)
{
    return THolder(new TCompletedOperationsUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
