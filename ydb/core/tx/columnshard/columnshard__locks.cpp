#include "columnshard_impl.h"

namespace NKikimr::NColumnShard {

void TColumnShard::SubscribeLock(const ui64 lockId, const ui32 lockNodeId) {
    Send(NLongTxService::MakeLongTxServiceID(SelfId().NodeId()),
        new NLongTxService::TEvLongTxService::TEvSubscribeLock(
            lockId,
            lockNodeId));
}

class TAbortWriteLock: public NTabletFlatExecutor::TTransactionBase<TColumnShard> {
private:
    using TBase = NTabletFlatExecutor::TTransactionBase<TColumnShard>;

public:
    TAbortWriteLock(TColumnShard* self, const ui64 lockId)
        : TBase(self)
        , LockId(lockId) {
    }

    virtual bool Execute(TTransactionContext& txc, const TActorContext&) override {
        Self->GetOperationsManager().AbortLockOnExecute(*Self, LockId, txc);
        return true;
    }

    virtual void Complete(const TActorContext&) override {
        Self->GetOperationsManager().AbortLockOnComplete(*Self, LockId);
    }
    TTxType GetTxType() const override {
        return TXTYPE_PROPOSE;
    }

private:
    ui64 LockId;
};

void TColumnShard::Handle(NLongTxService::TEvLongTxService::TEvLockStatus::TPtr& ev, const TActorContext& /*ctx*/) {
    auto* msg = ev->Get();
    const ui64 lockId = msg->Record.GetLockId();
    switch (msg->Record.GetStatus()) {
        case NKikimrLongTxService::TEvLockStatus::STATUS_NOT_FOUND:
        case NKikimrLongTxService::TEvLockStatus::STATUS_UNAVAILABLE:
            Execute(new TAbortWriteLock(this, lockId));
            break;

        default:
            break;
    }
}

} // namespace NKikimr::NDataShard