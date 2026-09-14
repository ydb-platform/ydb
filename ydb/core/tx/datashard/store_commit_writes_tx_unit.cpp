#include "datashard_active_transaction.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TStoreCommitWritesTxUnit : public TExecutionUnit {
public:
    TStoreCommitWritesTxUnit(TDataShard& self, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::StoreCommitWritesTx, false, self, pipeline)
    {
    }

    bool IsReadyToExecute(TOperation::TPtr) const override {
        return true;
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        Y_ENSURE(op->IsCommitWritesTx());

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

        Pipeline.ProposeTx(op, tx->GetTxBody(), txc, ctx);
        tx->ClearTxBody();
        tx->ClearCommitWritesTx();

        return EExecutionStatus::DelayCompleteNoMoreRestarts;
    }

    void Complete(TOperation::TPtr op, const TActorContext& ctx) override {
        Pipeline.ProposeComplete(op, ctx);
    }
};

THolder<TExecutionUnit> CreateStoreCommitWritesTxUnit(TDataShard& self, TPipeline& pipeline) {
    return THolder(new TStoreCommitWritesTxUnit(self, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
