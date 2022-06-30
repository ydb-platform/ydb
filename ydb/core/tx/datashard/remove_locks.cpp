#include "datashard_impl.h"

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
        // Remove any uncommitted changes with this lock id
        // FIXME: if a distributed tx has already validated (and persisted)
        // its locks, we must preserve uncommitted changes even when lock is
        // removed on the originating node, since the final outcome may
        // actually decide to commit.
        for (const auto& pr : Self->GetUserTables()) {
            auto localTid = pr.second->LocalTid;
            if (txc.DB.HasOpenTx(localTid, LockId)) {
                txc.DB.RemoveTx(localTid, LockId);
            }
        }

        // Remove the lock from memory, it's no longer needed
        Self->SysLocks.RemoveSubscribedLock(LockId);

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
    while (auto pendingSubscribeLock = SysLocks.NextPendingSubscribeLock()) {
        Send(MakeLongTxServiceID(SelfId().NodeId()),
            new TEvLongTxService::TEvSubscribeLock(
                pendingSubscribeLock.LockId,
                pendingSubscribeLock.LockNodeId));
    }
}

} // namespace NKikimr::NDataShard
