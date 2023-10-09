#include "datashard_active_transaction.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TStoreDistributedEraseTxUnit : public TExecutionUnit {
public:
    TStoreDistributedEraseTxUnit(TDataShard& self, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::StoreDistributedEraseTx, false, self, pipeline)
    {
    }

    bool IsReadyToExecute(TOperation::TPtr) const override {
        return true;
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        Y_ABORT_UNLESS(op->IsDistributedEraseTx());

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

        Pipeline.ProposeTx(op, tx->GetTxBody(), txc, ctx);
        tx->ClearTxBody();
        tx->ClearDistributedEraseTx();

        return EExecutionStatus::DelayCompleteNoMoreRestarts;
    }

    void Complete(TOperation::TPtr op, const TActorContext& ctx) override {
        Pipeline.ProposeComplete(op, ctx);
    }
};

THolder<TExecutionUnit> CreateStoreDistributedEraseTxUnit(TDataShard& self, TPipeline& pipeline) {
    return THolder(new TStoreDistributedEraseTxUnit(self, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr
