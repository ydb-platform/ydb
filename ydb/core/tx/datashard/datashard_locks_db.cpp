#include "datashard_locks_db.h"

namespace NKikimr::NDataShard {

bool TDataShardLocksDb::Load(TVector<TLockRow>& rows) {
    using Schema = TDataShard::Schema;

    NIceDb::TNiceDb db(DB);

    rows.clear();

    // Load locks
    THashMap<ui64, size_t> lockIndex;
    {
        auto rowset = db.Table<Schema::Locks>().Select();
        if (!rowset.IsReady()) {
            return false;
        }
        while (!rowset.EndOfSet()) {
            auto& lock = rows.emplace_back();
            lock.LockId = rowset.GetValue<Schema::Locks::LockId>();
            lock.LockNodeId = rowset.GetValue<Schema::Locks::LockNodeId>();
            lock.Generation = rowset.GetValue<Schema::Locks::Generation>();
            lock.Counter = rowset.GetValue<Schema::Locks::Counter>();
            lock.CreateTs = rowset.GetValue<Schema::Locks::CreateTimestamp>();
            lock.Flags = rowset.GetValue<Schema::Locks::Flags>();
            lockIndex[lock.LockId] = rows.size() - 1;
            if (!rowset.Next()) {
                return false;
            }
        }
    }

    // Load ranges
    {
        auto rowset = db.Table<Schema::LockRanges>().Select();
        if (!rowset.IsReady()) {
            return false;
        }
        while (!rowset.EndOfSet()) {
            auto lockId = rowset.GetValue<Schema::LockRanges::LockId>();
            auto it = lockIndex.find(lockId);
            if (it != lockIndex.end()) {
                auto& lock = rows[it->second];
                auto& range = lock.Ranges.emplace_back();
                range.RangeId = rowset.GetValue<Schema::LockRanges::RangeId>();
                range.TableId.OwnerId = rowset.GetValue<Schema::LockRanges::PathOwnerId>();
                range.TableId.LocalPathId = rowset.GetValue<Schema::LockRanges::LocalPathId>();
                range.Flags = rowset.GetValue<Schema::LockRanges::Flags>();
                range.Data = rowset.GetValue<Schema::LockRanges::Data>();
            }
            if (!rowset.Next()) {
                return false;
            }
        }
    }

    // Load conflicts
    {
        auto rowset = db.Table<Schema::LockConflicts>().Select();
        if (!rowset.IsReady()) {
            return false;
        }
        while (!rowset.EndOfSet()) {
            auto lockId = rowset.GetValue<Schema::LockConflicts::LockId>();
            auto it = lockIndex.find(lockId);
            if (it != lockIndex.end()) {
                auto& lock = rows[it->second];
                lock.Conflicts.push_back(rowset.GetValue<Schema::LockConflicts::ConflictId>());
            }
            if (!rowset.Next()) {
                return false;
            }
        }
    }

    // Load volatile dependencies
    if (db.HaveTable<Schema::LockVolatileDependencies>()) {
        auto rowset = db.Table<Schema::LockVolatileDependencies>().Select();
        if (!rowset.IsReady()) {
            return false;
        }
        while (!rowset.EndOfSet()) {
            auto lockId = rowset.GetValue<Schema::LockVolatileDependencies::LockId>();
            auto it = lockIndex.find(lockId);
            if (it != lockIndex.end()) {
                auto& lock = rows[it->second];
                auto txId = rowset.GetValue<Schema::LockVolatileDependencies::TxId>();
                lock.VolatileDependencies.push_back(txId);
            }
            if (!rowset.Next()) {
                return false;
            }
        }
    }

    return true;
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

void TDataShardLocksDb::PersistAddLock(ui64 lockId, ui32 lockNodeId, ui32 generation, ui64 counter, ui64 createTs, ui64 flags) {
    using Schema = TDataShard::Schema;
    NIceDb::TNiceDb db(DB);
    db.Table<Schema::Locks>().Key(lockId).Update(
        NIceDb::TUpdate<Schema::Locks::LockNodeId>(lockNodeId),
        NIceDb::TUpdate<Schema::Locks::Generation>(generation),
        NIceDb::TUpdate<Schema::Locks::Counter>(counter),
        NIceDb::TUpdate<Schema::Locks::CreateTimestamp>(createTs),
        NIceDb::TUpdate<Schema::Locks::Flags>(flags));
    HasChanges_ = true;
}

void TDataShardLocksDb::PersistLockCounter(ui64 lockId, ui64 counter) {
    using Schema = TDataShard::Schema;
    NIceDb::TNiceDb db(DB);
    db.Table<Schema::Locks>().Key(lockId).Update(
        NIceDb::TUpdate<Schema::Locks::Counter>(counter));
    HasChanges_ = true;
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
            }
        }
    }

    using Schema = TDataShard::Schema;
    NIceDb::TNiceDb db(DB);
    db.Table<Schema::Locks>().Key(lockId).Delete();
    HasChanges_ = true;

    if (!isVolatile) {
        Self.ScheduleRemoveLockChanges(lockId);
    }
}

void TDataShardLocksDb::PersistAddRange(ui64 lockId, ui64 rangeId, const TPathId& tableId, ui64 flags, const TString& data) {
    using Schema = TDataShard::Schema;
    NIceDb::TNiceDb db(DB);
    db.Table<Schema::LockRanges>().Key(lockId, rangeId).Update(
        NIceDb::TUpdate<Schema::LockRanges::PathOwnerId>(tableId.OwnerId),
        NIceDb::TUpdate<Schema::LockRanges::LocalPathId>(tableId.LocalPathId),
        NIceDb::TUpdate<Schema::LockRanges::Flags>(flags),
        NIceDb::TUpdate<Schema::LockRanges::Data>(data));
    HasChanges_ = true;
}

void TDataShardLocksDb::PersistRangeFlags(ui64 lockId, ui64 rangeId, ui64 flags) {
    using Schema = TDataShard::Schema;
    NIceDb::TNiceDb db(DB);
    db.Table<Schema::LockRanges>().Key(lockId, rangeId).Update(
        NIceDb::TUpdate<Schema::LockRanges::Flags>(flags));
    HasChanges_ = true;
}

void TDataShardLocksDb::PersistRemoveRange(ui64 lockId, ui64 rangeId) {
    using Schema = TDataShard::Schema;
    NIceDb::TNiceDb db(DB);
    db.Table<Schema::LockRanges>().Key(lockId, rangeId).Delete();
    HasChanges_ = true;
}

void TDataShardLocksDb::PersistAddConflict(ui64 lockId, ui64 otherLockId) {
    using Schema = TDataShard::Schema;
    NIceDb::TNiceDb db(DB);
    db.Table<Schema::LockConflicts>().Key(lockId, otherLockId).Update();
    HasChanges_ = true;
}

void TDataShardLocksDb::PersistRemoveConflict(ui64 lockId, ui64 otherLockId) {
    using Schema = TDataShard::Schema;
    NIceDb::TNiceDb db(DB);
    db.Table<Schema::LockConflicts>().Key(lockId, otherLockId).Delete();
    HasChanges_ = true;
}

void TDataShardLocksDb::PersistAddVolatileDependency(ui64 lockId, ui64 txId) {
    using Schema = TDataShard::Schema;
    NIceDb::TNiceDb db(DB);
    db.Table<Schema::LockVolatileDependencies>().Key(lockId, txId).Update();
    HasChanges_ = true;
}

void TDataShardLocksDb::PersistRemoveVolatileDependency(ui64 lockId, ui64 txId) {
    using Schema = TDataShard::Schema;
    NIceDb::TNiceDb db(DB);
    db.Table<Schema::LockVolatileDependencies>().Key(lockId, txId).Delete();
    HasChanges_ = true;
}

} // namespace NKikimr::NDataShard
