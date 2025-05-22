#include "datashard_locks_db.h"

namespace NKikimr::NDataShard {

void TDataShardLocksDb::PersistRemoveLock(ui64 lockId) {
    // We remove lock changes unless it's managed by volatile tx manager
    bool isVolatile = Self.GetVolatileTxManager().FindByCommitTxId(lockId);
    if (!isVolatile) {
        for (auto& pr : Self.GetUserTables()) {
            auto tid = pr.second->LocalTid;
            // Removing the lock also removes any uncommitted data
            if (DB.HasOpenTx(tid, lockId)) {
                DB.RemoveTx(tid, lockId);
                Self.GetConflictsCache().GetTableCache(tid).RemoveUncommittedWrites(lockId, DB);
            }
        }
    }

    using Schema = TDataShard::Schema;
    NIceDb::TNiceDb db(DB);
    db.Table<typename Schema::Locks>().Key(lockId).Delete();
    HasChanges_ = true;

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

} // namespace NKikimr::NDataShard
