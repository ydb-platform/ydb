#include "columnshard_impl.h"

namespace NKikimr::NColumnShard {

void TColumnShard::SubscribeLock(const ui64 lockId, const ui32 lockNodeId) {
    Send(NLongTxService::MakeLongTxServiceID(SelfId().NodeId()), std::make_unique<NLongTxService::TEvLongTxService::TEvSubscribeLock>(lockId, lockNodeId));
}

class TAbortWriteLockTransaction: public NTabletFlatExecutor::TTransactionBase<TColumnShard> {
private:
    using TBase = NTabletFlatExecutor::TTransactionBase<TColumnShard>;

    bool IsLockAborted() const {
        auto lock = Self->GetOperationsManager().GetLockOptional(LockId);
        return lock->IsAborted();
    }

public:
    TAbortWriteLockTransaction(TColumnShard* self, const ui64 lockId)
        : TBase(self)
        , LockId(lockId) {
    }

    virtual bool Execute(TTransactionContext& txc, const TActorContext&) override {
        if (!IsLockAborted()) {
            return true;
        }

        Self->GetOperationsManager().AbortLockOnExecute(*Self, LockId, txc);
        return true;
    }

    virtual void Complete(const TActorContext&) override {
        if (!IsLockAborted()) {
            return;
        }

        Self->GetOperationsManager().AbortLockOnComplete(*Self, LockId);
    }

    TTxType GetTxType() const override {
        return TXTYPE_ABORT_LOCK;
    }

private:
    ui64 LockId;
};

void TColumnShard::MaybeCleanupLock(const ui64 lockId) {
    auto lock = OperationsManager->GetLockOptional(lockId);
    if (!lock || !lock->IsDeleted() || lock->IsAborted() || lock->IsTxIdAssigned() || lock->GetOperationsInProgress()) {
        return;
    }

    if (!lock->SetAborted()) {
        return;
    }

    Execute(new TAbortWriteLockTransaction(this, lockId));
}

void TColumnShard::Handle(NLongTxService::TEvLongTxService::TEvLockStatus::TPtr& ev, const TActorContext& /*ctx*/) {
    auto* msg = ev->Get();
    const ui64 lockId = msg->Record.GetLockId();
    switch (msg->Record.GetStatus()) {
        case NKikimrLongTxService::TEvLockStatus::STATUS_NOT_FOUND:
        case NKikimrLongTxService::TEvLockStatus::STATUS_UNAVAILABLE: {
            if (auto lock = OperationsManager->GetLockOptional(lockId); lock) {
                lock->SetDeleted();
                MaybeCleanupLock(lockId);
            }
            break;
        }
        default:
            break;
    }
}

bool TColumnShard::IsLocksMemoryLimitExceeded() const {
    const auto inFlightLocksRangesBytes = NOlap::TPKRangeFilter::GetFiltersTotalMemorySize();
    const auto inFlightLocksRangesBytesLimit = AppDataVerified().ColumnShardConfig.GetInFlightLocksRangesBytesLimit();
    if (inFlightLocksRangesBytes > inFlightLocksRangesBytesLimit) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "In flight locks ranges bytes limit exceeded")
            ("inFlightLocksRangesBytes", inFlightLocksRangesBytes)
            ("inFlightLocksRangesBytesLimit", inFlightLocksRangesBytesLimit);

        return true;
    }

    return false;
}

void TColumnShard::DeleteLock(const ui64 lockId) {
    if (auto lock = OperationsManager->GetLockOptional(lockId); lock) {
        lock->SetDeleted();
        MaybeCleanupLock(lockId);
    }
}

bool TColumnShard::IsLockDeleted(const ui64 lockId) const {
    if (auto lock = OperationsManager->GetLockOptional(lockId); lock && lock->IsDeleted()) {
        return true;
    }

    return false;
}


} // namespace NKikimr::NColumnShard
