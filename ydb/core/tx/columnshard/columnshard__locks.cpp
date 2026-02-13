#include "columnshard_impl.h"

namespace NKikimr::NColumnShard {

class TAbortWriteTransaction: public NTabletFlatExecutor::TTransactionBase<TColumnShard> {
private:
    using TBase = NTabletFlatExecutor::TTransactionBase<TColumnShard>;

public:
    TAbortWriteTransaction(TColumnShard* self, const ui64 lockId)
        : TBase(self)
        , LockId(lockId) {
    }

    virtual bool Execute(TTransactionContext& txc, const TActorContext&) override {
        Self->GetOperationsManager().AbortTransactionOnExecute(*Self, 0, LockId, txc);
        return true;
    }

    virtual void Complete(const TActorContext& /*ctx*/) override {
        Self->GetOperationsManager().AbortTransactionOnComplete(*Self, 0, LockId);
    }

    TTxType GetTxType() const override {
        return TXTYPE_PROPOSE;
    }

private:
    ui64 LockId;
};

void TColumnShard::SubscribeLockIfNotAlready(const ui64 lockId, const ui32 lockNodeId) const {
    auto& lock = OperationsManager->GetLockVerified(lockId);
    if (!lock.IsSubscribed()) {
        lock.SetSubscribed();
        Send(NLongTxService::MakeLongTxServiceID(SelfId().NodeId()), std::make_unique<NLongTxService::TEvLongTxService::TEvSubscribeLock>(lockId, lockNodeId));
    }
}

void TColumnShard::TransactionToAbort(const ui64 lockId) {
    if (auto lock = OperationsManager->GetLockOptional(lockId)) {
        lock->SetNeedsAborting();
        MaybeAbortTransaction(lockId);
    }
}

void TColumnShard::MaybeAbortTransaction(const ui64 lockId) {
    auto lock = OperationsManager->GetLockOptional(lockId);
    if (!lock || !lock->ReadyForAborting() || lock->IsTxIdAssigned()) {
        return;
    }

    lock->SetAborting();
    Execute(new TAbortWriteTransaction(this, lockId));
}


void TColumnShard::Handle(NLongTxService::TEvLongTxService::TEvLockStatus::TPtr& ev, const TActorContext& /*ctx*/) {
    auto* msg = ev->Get();
    const ui64 lockId = msg->Record.GetLockId();
    switch (msg->Record.GetStatus()) {
        case NKikimrLongTxService::TEvLockStatus::STATUS_NOT_FOUND:
        case NKikimrLongTxService::TEvLockStatus::STATUS_UNAVAILABLE: {
            TransactionToAbort(lockId);
            break;
        }
        default:
            break;
    }
}

} // namespace NKikimr::NColumnShard
