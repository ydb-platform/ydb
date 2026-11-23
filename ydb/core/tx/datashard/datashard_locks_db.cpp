#include "datashard_locks_db.h"

namespace NKikimr::NDataShard {

void TDataShardLocksDb::PersistLockCounter(ui64 lockId, ui64 counter) {
    TBase::PersistLockCounter(lockId, counter);
    if (counter == Max<ui64>()) {
        NIceDb::TNiceDb db(DB);
        Self.GetMultiTxIdManager().OnLockBroken(db, lockId);
    }
}

void TDataShardLocksDb::PersistRemoveLock(ui64 lockId) {
    // We remove lock changes unless it's managed by volatile tx manager
    bool isVolatile = Self.GetVolatileTxManager().FindByCommitTxId(lockId);
    if (!isVolatile) {
        for (auto& pr : Self.GetUserTables()) {
            auto tid = pr.second->LocalTid;
            // Removing the lock also removes any uncommitted data
            if (DB.HasOpenTx(tid, lockId)) {
                DB.RemoveTx(tid, lockId);
                Self.AbortHnswIndexChanges(tid, lockId, DB);
                Self.GetConflictsCache().GetTableCache(tid).RemoveUncommittedWrites(lockId, DB);
            }
        }
    }

    using Schema = TDataShard::Schema;
    NIceDb::TNiceDb db(DB);
    db.Table<typename Schema::Locks>().Key(lockId).Delete();
    HasChanges_ = true;

    Self.GetMultiTxIdManager().OnLockRemoved(db, lockId);

    if (!isVolatile) {
        Self.ScheduleRemoveLockChanges(lockId);
    }
}

bool TDataShardLocksDb::MayAddLock(ui64 lockId) {
    for (auto& pr : Self.GetUserTables()) {
        auto tid = pr.second->LocalTid;
        // We cannot start a new lockId if it has any uncompacted data
        if (DB.HasTxData(tid, lockId)) {
            return false;
        }
    }
    return true;
}

bool TDataShardLocksDb::MayActivateLock(ui64 lockId) {
    // Unlike MayAddLock, activation has a known tracker proving there are no
    // local deltas. HasTxData would also see harmless references elsewhere in
    // borrowed parts and incorrectly forbid the first local write.
    for (const auto& [_, table] : Self.GetUserTables()) {
        // Commit/abort decisions still apply to future writes with this id.
        if (DB.HasCommittedTx(table->LocalTid, lockId) || DB.HasRemovedTx(table->LocalTid, lockId)) {
            return false;
        }
    }
    return true;
}

} // namespace NKikimr::NDataShard
