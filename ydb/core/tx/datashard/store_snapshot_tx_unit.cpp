#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TStoreSnapshotTxUnit : public TExecutionUnit {
public:
    TStoreSnapshotTxUnit(TDataShard& dataShard, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::StoreSnapshotTx, false, dataShard, pipeline)
    { }

    bool IsReadyToExecute(TOperation::TPtr) const override {
        return true;
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        Y_ENSURE(op->IsSnapshotTx());
        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

        Pipeline.ProposeTx(op, tx->GetTxBody(), txc, ctx);
        tx->ClearTxBody();
        tx->ClearSnapshotTx(); // TODO: caching layer needed

        return EExecutionStatus::DelayCompleteNoMoreRestarts;
    }

    void Complete(TOperation::TPtr op, const TActorContext& ctx) override {
        Pipeline.ProposeComplete(op, ctx);
    }
};

THolder<TExecutionUnit> CreateStoreSnapshotTxUnit(
        TDataShard& dataShard,
        TPipeline& pipeline)
{
    return THolder(new TStoreSnapshotTxUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
