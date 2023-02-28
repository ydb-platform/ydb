#include "datashard_impl.h"

namespace NKikimr::NDataShard {

class TDataShard::TTxRemoveLockChangeRecords
    : public NTabletFlatExecutor::TTransactionBase<TDataShard>
{
public:
    TTxRemoveLockChangeRecords(TDataShard* self)
        : TBase(self)
    { }

    TTxType GetTxType() const override { return TXTYPE_REMOVE_LOCK_CHANGE_RECORDS; }

    static constexpr size_t MaxRecordsToRemove = 1000;

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        NIceDb::TNiceDb db(txc.DB);

        size_t removed = 0;

        while (!Self->PendingLockChangeRecordsToRemove.empty() && removed < MaxRecordsToRemove) {
            ui64 lockId = Self->PendingLockChangeRecordsToRemove.back();

            auto it = Self->LockChangeRecords.find(lockId);
            if (it == Self->LockChangeRecords.end()) {
                // Nothing to remove, just skip it
                Self->PendingLockChangeRecordsToRemove.pop_back();
                continue;
            }

            if (Self->CommittedLockChangeRecords.contains(lockId)) {
                // Don't remove records that are committed
                Self->PendingLockChangeRecordsToRemove.pop_back();
                continue;
            }

            if (Self->GetVolatileTxManager().FindByCommitTxId(lockId)) {
                // Don't remove records that are managed by volatile tx manager
                Self->PendingLockChangeRecordsToRemove.pop_back();
                continue;
            }

            while (!it->second.Changes.empty() && removed < MaxRecordsToRemove) {
                auto& record = it->second.Changes.back();
                db.Table<Schema::LockChangeRecords>().Key(record.LockId, record.LockOffset).Delete();
                db.Table<Schema::LockChangeRecordDetails>().Key(record.LockId, record.LockOffset).Delete();
                it->second.Changes.pop_back();
                it->second.PersistentCount = it->second.Changes.size();
                ++removed;
            }

            if (!it->second.Changes.empty()) {
                // We couldn't remove everything, continue in the next transaction
                break;
            }

            Self->LockChangeRecords.erase(it);
            Self->PendingLockChangeRecordsToRemove.pop_back();
        }

        if (!Self->PendingLockChangeRecordsToRemove.empty()) {
            ctx.Send(ctx.SelfID, new TEvPrivate::TEvRemoveLockChangeRecords());
        }

        return true;
    }

    void Complete(const TActorContext&) override {
        // nothing
    }
};

void TDataShard::Handle(TEvPrivate::TEvRemoveLockChangeRecords::TPtr&, const TActorContext& ctx) {
    Execute(new TTxRemoveLockChangeRecords(this), ctx);
}

} // namespace NKikimr::NDataShard
