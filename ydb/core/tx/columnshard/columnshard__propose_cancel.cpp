#include "columnshard_impl.h"

#include <ydb/library/actors/struct_log/log_stack.h>
#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_TX

namespace NKikimr::NColumnShard {

/**
There is a slight chance that TTxProposeCancel is called twice for a single transaction.
It happens if the tx remains in the deadline queue for too long,
gets out of there to be cancelled, and at the same time the shard receives
a TEvCancelTransactionProposal from kqp. So, we have two TTxProposeCancel in the queue.
*/
class TColumnShard::TTxProposeCancel: public TTransactionBase<TColumnShard> {
public:
    TTxProposeCancel(TColumnShard* self, const ui64 txId)
        : TTransactionBase(self)
        , TxId(txId)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_PROPOSE_CANCEL;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        LOG_S_DEBUG("TTxProposeCancel.Execute");

        auto op = Self->ProgressTxController->GetTxOperator(TxId, ETxOperatorStatus::InProgress, /*optional*/ true);
        if (!op) {
            YDB_LOG_WARN("", {"event", "skip_cancel_no_operator"}, {"txId", TxId});
            return true;
        }
        // race TTxProposeCancel vs TTxPlanStep, we do not wanna cancel a planned transaction
        if (op->IsPlanned()) {
            YDB_LOG_WARN("", {"event", "skip_cancel_already_planned"}, {"txId", TxId},
                {"planStep", op->GetStep()});
            return true;
        }
        if (auto* lock = Self->GetOperationsManager().GetLockFeaturesForTxOptional(TxId)) {
            AFL_VERIFY(lock->IsTxIdAssigned())("tx_id", TxId)("lock_id", lock->GetLockId());
            lock->SetNeedsAborting();
            if (lock->ReadyForAborting()) {
                lock->SetAborting();
                Self->ProgressTxController->ExecuteOnCancel(TxId, txc);
                DoComplete = true;
            }
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_S_DEBUG("TTxProposeCancel.Complete");
        if (DoComplete) {
            Self->ProgressTxController->CompleteOnCancel(TxId, ctx);
        }
    }

private:
    ui64 TxId;
    bool DoComplete = false;
};

void TColumnShard::Handle(TEvDataShard::TEvCancelTransactionProposal::TPtr& ev, const TActorContext& /*ctx*/) {
    const auto* msg = ev->Get();
    const ui64 txId = msg->Record.GetTxId();
    CancelTransaction(txId);
}

void TColumnShard::CancelTransaction(const ui64 txId) {
    Execute(new TTxProposeCancel(this, txId));
}

}   // namespace NKikimr::NColumnShard
