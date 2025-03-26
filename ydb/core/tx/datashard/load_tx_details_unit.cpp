#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TLoadTxDetailsUnit : public TExecutionUnit {
public:
    TLoadTxDetailsUnit(TDataShard &dataShard,
                       TPipeline &pipeline);
    ~TLoadTxDetailsUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
};

TLoadTxDetailsUnit::TLoadTxDetailsUnit(TDataShard &dataShard,
                                       TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::LoadTxDetails, true, dataShard, pipeline)
{
}

TLoadTxDetailsUnit::~TLoadTxDetailsUnit()
{
}

bool TLoadTxDetailsUnit::IsReadyToExecute(TOperation::TPtr) const
{
    return true;
}

EExecutionStatus TLoadTxDetailsUnit::Execute(TOperation::TPtr op,
                                             TTransactionContext &txc,
                                             const TActorContext &ctx)
{
    TActiveTransaction::TPtr tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

    if (!Pipeline.LoadTxDetails(txc, ctx, tx))
        return EExecutionStatus::Restart;

    return EExecutionStatus::Executed;
}

void TLoadTxDetailsUnit::Complete(TOperation::TPtr,
                                  const TActorContext &)
{
}

THolder<TExecutionUnit> CreateLoadTxDetailsUnit(TDataShard &dataShard,
                                                TPipeline &pipeline)
{
    return MakeHolder<TLoadTxDetailsUnit>(dataShard, pipeline);
}

} // namespace NDataShard
} // namespace NKikimr
