#include "columnshard_impl.h"

namespace NKikimr::NColumnShard {

/**
There is a slight chance that TTxProposeCancel is called twice for a single transaction.
It happens if the tx remains in the deadline queue for too long,
gets out of there to be cancelled, and at the same time the shard receives
a TEvCancelTransactionProposal from kqp. So, we have two TTxProposeCancel in the queue.
*/
class TColumnShard::TTxProposeCancel : public TTransactionBase<TColumnShard> {
public:
    TTxProposeCancel(TColumnShard* self, const ui64 txId)
        : TTransactionBase(self)
        , TxId(txId)
    { }

    TTxType GetTxType() const override { return TXTYPE_PROPOSE_CANCEL; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        LOG_S_DEBUG("TTxProposeCancel.Execute");

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

}
