#include "const.h"
#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TStoreDataTxUnit : public TExecutionUnit {
public:
    TStoreDataTxUnit(TDataShard &dataShard,
                     TPipeline &pipeline);
    ~TStoreDataTxUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
};

TStoreDataTxUnit::TStoreDataTxUnit(TDataShard &dataShard,
                                   TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::StoreDataTx, false, dataShard, pipeline)
{
}

TStoreDataTxUnit::~TStoreDataTxUnit()
{
}

bool TStoreDataTxUnit::IsReadyToExecute(TOperation::TPtr) const
{
    return true;
}

EExecutionStatus TStoreDataTxUnit::Execute(TOperation::TPtr op,
                                           TTransactionContext &txc,
                                           const TActorContext &ctx)
{
    Y_ENSURE(op->IsDataTx() || op->IsReadTable());
    Y_ENSURE(!op->IsAborted() && !op->IsInterrupted());

    TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());
    auto dataTx = tx->GetDataTx();
    Y_ENSURE(dataTx);

    bool cached = Pipeline.SaveForPropose(dataTx);
    if (cached) {
        Pipeline.RegisterDistributedWrites(op, txc.DB);
    }
    Pipeline.ProposeTx(op, tx->GetTxBody(), txc, ctx);

    if (!op->HasVolatilePrepareFlag()) {
        tx->ClearTxBody();
    }

    tx->ClearDataTx();

    return EExecutionStatus::DelayCompleteNoMoreRestarts;
}

void TStoreDataTxUnit::Complete(TOperation::TPtr op,
                                const TActorContext &ctx)
{
    Pipeline.ProposeComplete(op, ctx);
}

THolder<TExecutionUnit> CreateStoreDataTxUnit(TDataShard &dataShard,
                                              TPipeline &pipeline)
{
    return MakeHolder<TStoreDataTxUnit>(dataShard, pipeline);
}

} // namespace NDataShard
} // namespace NKikimr
