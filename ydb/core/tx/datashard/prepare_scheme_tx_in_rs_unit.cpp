#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TPrepareSchemeTxInRSUnit : public TExecutionUnit {
public:
    TPrepareSchemeTxInRSUnit(TDataShard &dataShard,
                             TPipeline &pipeline);
    ~TPrepareSchemeTxInRSUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
};

TPrepareSchemeTxInRSUnit::TPrepareSchemeTxInRSUnit(TDataShard &dataShard,
                                                   TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::PrepareSchemeTxInRS, false, dataShard, pipeline)
{
}

TPrepareSchemeTxInRSUnit::~TPrepareSchemeTxInRSUnit()
{
}

bool TPrepareSchemeTxInRSUnit::IsReadyToExecute(TOperation::TPtr) const
{
    return true;
}

EExecutionStatus TPrepareSchemeTxInRSUnit::Execute(TOperation::TPtr op,
                                                   TTransactionContext &,
                                                   const TActorContext &)
{
    TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

    auto &schemeTx = tx->GetSchemeTx();
    if (schemeTx.HasReceiveSnapshot()) {
        ui64 srcTablet = schemeTx.GetReceiveSnapshot().GetReceiveFrom(0).GetShard();
        op->InReadSets().insert(std::make_pair(std::make_pair(srcTablet, DataShard.TabletID()), TVector<TRSData>()));
    }

    return EExecutionStatus::Executed;
}

void TPrepareSchemeTxInRSUnit::Complete(TOperation::TPtr,
                                        const TActorContext &)
{
}

THolder<TExecutionUnit> CreatePrepareSchemeTxInRSUnit(TDataShard &dataShard,
                                                      TPipeline &pipeline)
{
    return THolder(new TPrepareSchemeTxInRSUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
