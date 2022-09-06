#include "datashard_impl.h"
#include "datashard_locks_db.h"

namespace NKikimr::NDataShard {

using namespace NLongTxService;

class TDataShard::TTxRemoveLock
    : public NTabletFlatExecutor::TTransactionBase<TDataShard>
{
public:
    TTxRemoveLock(TDataShard* self, ui64 lockId)
        : TBase(self)
        , LockId(lockId)
    { }

    TTxType GetTxType() const override { return TXTYPE_REMOVE_LOCK; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        // Remove the lock from memory, it's no longer needed
        // Note: locksDb will also remove uncommitted changes
        //       when removing a persistent lock.
        TDataShardLocksDb locksDb(*Self, txc);
        Self->SysLocks.RemoveSubscribedLock(LockId, &locksDb);

        return true;
    }

    void Complete(const TActorContext&) override {
        // nothing
    }

private:
    const ui64 LockId;
};

void TDataShard::Handle(TEvLongTxService::TEvLockStatus::TPtr& ev, const TActorContext& ctx) {
    auto* msg = ev->Get();
    const ui64 lockId = msg->Record.GetLockId();
    switch (msg->Record.GetStatus()) {
        case NKikimrLongTxService::TEvLockStatus::STATUS_NOT_FOUND:
        case NKikimrLongTxService::TEvLockStatus::STATUS_UNAVAILABLE:
            Execute(new TTxRemoveLock(this, lockId), ctx);
            break;

        default:
            break;
    }
}

void TDataShard::SubscribeNewLocks(const TActorContext&) {
    SubscribeNewLocks();
}

void TDataShard::SubscribeNewLocks() {
    while (auto pendingSubscribeLock = SysLocks.NextPendingSubscribeLock()) {
        Send(MakeLongTxServiceID(SelfId().NodeId()),
            new TEvLongTxService::TEvSubscribeLock(
                pendingSubscribeLock.LockId,
                pendingSubscribeLock.LockNodeId));
    }
}

} // namespace NKikimr::NDataShard
