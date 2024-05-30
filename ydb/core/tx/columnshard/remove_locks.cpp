#include "columnshard_impl.h"

#include <ydb/core/tx/columnshard/transactions/locks_db.h>

namespace NKikimr::NColumnShard {

class TTxRemoveLock : public NTabletFlatExecutor::TTransactionBase<TColumnShard> {
public:
    TTxRemoveLock(TColumnShard* self, ui64 lockId)
        : TBase(self)
        , LockId(lockId)
    { }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        // Remove the lock from memory, it's no longer needed
        // Note: locksDb will also remove uncommitted changes
        //       when removing a persistent lock.
        TColumnShardLocksDb locksDb(*Self, txc);
        Self->SysLocksTable().RemoveSubscribedLock(LockId, &locksDb);
        return true;
    }

    void Complete(const TActorContext&) override {}

private:
    const ui64 LockId;
};

void TColumnShard::Handle(NLongTxService::TEvLongTxService::TEvLockStatus::TPtr& ev, const TActorContext& ctx) {
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

} // namespace NKikimr::NColumnShard
