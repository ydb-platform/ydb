#pragma once

#include "locks.h"
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr::NLocks {

template<class TShard, class TSchemaDescription>
class TShardLocksDb : public NKikimr::NDataShard::ILocksDb {
public:
    TShardLocksDb(TShard& self, NTabletFlatExecutor::TTransactionContext& txc)
        : Self(self)
        , DB(txc.DB)
    { }

    bool HasChanges() const {
        return HasChanges_;
    }

    bool Load(TVector<TLockRow>& rows) override {
        using Schema = TSchemaDescription;

        NIceDb::TNiceDb db(DB);

        rows.clear();

        // Load locks
        THashMap<ui64, size_t> lockIndex;
        {
            auto rowset = db.Table<typename Schema::Locks>().Select();
            if (!rowset.IsReady()) {
                return false;
            }
            while (!rowset.EndOfSet()) {
                auto& lock = rows.emplace_back();
                lock.LockId = rowset.template GetValue<typename Schema::Locks::LockId>();
                lock.LockNodeId = rowset.template GetValue<typename Schema::Locks::LockNodeId>();
                lock.Generation = rowset.template GetValue<typename Schema::Locks::Generation>();
                lock.Counter = rowset.template GetValue<typename Schema::Locks::Counter>();
                lock.CreateTs = rowset.template GetValue<typename Schema::Locks::CreateTimestamp>();
                lock.Flags = rowset.template GetValue<typename Schema::Locks::Flags>();
                lockIndex[lock.LockId] = rows.size() - 1;
                if (!rowset.Next()) {
                    return false;
                }
            }
        }

        // Load ranges
        {
            auto rowset = db.Table<typename Schema::LockRanges>().Select();
            if (!rowset.IsReady()) {
                return false;
            }
            while (!rowset.EndOfSet()) {
                auto lockId = rowset.template GetValue<typename Schema::LockRanges::LockId>();
                auto it = lockIndex.find(lockId);
                if (it != lockIndex.end()) {
                    auto& lock = rows[it->second];
                    auto& range = lock.Ranges.emplace_back();
                    range.RangeId = rowset.template GetValue<typename Schema::LockRanges::RangeId>();
                    range.TableId.OwnerId = rowset.template GetValue<typename Schema::LockRanges::PathOwnerId>();
                    range.TableId.LocalPathId = rowset.template GetValue<typename Schema::LockRanges::LocalPathId>();
                    range.Flags = rowset.template GetValue<typename Schema::LockRanges::Flags>();
                    range.Data = rowset.template GetValue<typename Schema::LockRanges::Data>();
                }
                if (!rowset.Next()) {
                    return false;
                }
            }
        }

        // Load conflicts
        {
            auto rowset = db.Table<typename Schema::LockConflicts>().Select();
            if (!rowset.IsReady()) {
                return false;
            }
            while (!rowset.EndOfSet()) {
                auto lockId = rowset.template GetValue<typename Schema::LockConflicts::LockId>();
                auto it = lockIndex.find(lockId);
                if (it != lockIndex.end()) {
                    auto& lock = rows[it->second];
                    lock.Conflicts.push_back(rowset.template GetValue<typename Schema::LockConflicts::ConflictId>());
                }
                if (!rowset.Next()) {
                    return false;
                }
            }
        }

        // Load volatile dependencies
        if (db.HaveTable<typename Schema::LockVolatileDependencies>()) {
            auto rowset = db.Table<typename Schema::LockVolatileDependencies>().Select();
            if (!rowset.IsReady()) {
                return false;
            }
            while (!rowset.EndOfSet()) {
                auto lockId = rowset.template GetValue<typename Schema::LockVolatileDependencies::LockId>();
                auto it = lockIndex.find(lockId);
                if (it != lockIndex.end()) {
                    auto& lock = rows[it->second];
                    auto txId = rowset.template GetValue<typename Schema::LockVolatileDependencies::TxId>();
                    lock.VolatileDependencies.push_back(txId);
                }
                if (!rowset.Next()) {
                    return false;
                }
            }
        }

        return true;
    }

    void PersistAddLock(ui64 lockId, ui32 lockNodeId, ui32 generation, ui64 counter, ui64 createTs, ui64 flags = 0) override {
        using Schema = TSchemaDescription;
        NIceDb::TNiceDb db(DB);
        db.Table<typename Schema::Locks>().Key(lockId).Update(
            NIceDb::TUpdate<typename Schema::Locks::LockNodeId>(lockNodeId),
            NIceDb::TUpdate<typename Schema::Locks::Generation>(generation),
            NIceDb::TUpdate<typename Schema::Locks::Counter>(counter),
            NIceDb::TUpdate<typename Schema::Locks::CreateTimestamp>(createTs),
            NIceDb::TUpdate<typename Schema::Locks::Flags>(flags));
        HasChanges_ = true;
    }

    void PersistLockCounter(ui64 lockId, ui64 counter) override {
        using Schema = TSchemaDescription;
        NIceDb::TNiceDb db(DB);
        db.Table<typename Schema::Locks>().Key(lockId).Update(
            NIceDb::TUpdate<typename Schema::Locks::Counter>(counter));
        HasChanges_ = true;
    }

    void PersistLockFlags(ui64 lockId, ui64 flags) override {
        using Schema = TSchemaDescription;
        NIceDb::TNiceDb db(DB);
        db.Table<typename Schema::Locks>().Key(lockId).Update(
            NIceDb::TUpdate<typename Schema::Locks::Flags>(flags));
        HasChanges_ = true;
    }

    // Persist adding/removing info on locked ranges
    void PersistAddRange(ui64 lockId, ui64 rangeId, const TPathId& tableId, ui64 flags = 0, const TString& data = {}) override {
        using Schema = TSchemaDescription;
        NIceDb::TNiceDb db(DB);
        db.Table<typename Schema::LockRanges>().Key(lockId, rangeId).Update(
            NIceDb::TUpdate<typename Schema::LockRanges::PathOwnerId>(tableId.OwnerId),
            NIceDb::TUpdate<typename Schema::LockRanges::LocalPathId>(tableId.LocalPathId),
            NIceDb::TUpdate<typename Schema::LockRanges::Flags>(flags),
            NIceDb::TUpdate<typename Schema::LockRanges::Data>(data));
        HasChanges_ = true;
    }

    void PersistRangeFlags(ui64 lockId, ui64 rangeId, ui64 flags) override {
        using Schema = TSchemaDescription;
        NIceDb::TNiceDb db(DB);
        db.Table<typename Schema::LockRanges>().Key(lockId, rangeId).Update(
            NIceDb::TUpdate<typename Schema::LockRanges::Flags>(flags));
        HasChanges_ = true;
    }

    void PersistRemoveRange(ui64 lockId, ui64 rangeId) override {
        using Schema = TSchemaDescription;
        NIceDb::TNiceDb db(DB);
        db.Table<typename Schema::LockRanges>().Key(lockId, rangeId).Delete();
        HasChanges_ = true;
    }

    // Persist a conflict, i.e. this lock must break some other lock on commit
    void PersistAddConflict(ui64 lockId, ui64 otherLockId) override {
        using Schema = TSchemaDescription;
        NIceDb::TNiceDb db(DB);
        db.Table<typename Schema::LockConflicts>().Key(lockId, otherLockId).Update();
        HasChanges_ = true;
    }

    void PersistRemoveConflict(ui64 lockId, ui64 otherLockId) override {
        using Schema = TSchemaDescription;
        NIceDb::TNiceDb db(DB);
        db.Table<typename Schema::LockConflicts>().Key(lockId, otherLockId).Delete();
        HasChanges_ = true;
    }

    // Persist volatile dependencies, i.e. which undecided transactions must be waited for on commit
    void PersistAddVolatileDependency(ui64 lockId, ui64 txId) override {
        using Schema = TSchemaDescription;
        NIceDb::TNiceDb db(DB);
        db.Table<typename Schema::LockVolatileDependencies>().Key(lockId, txId).Update();
        HasChanges_ = true;
    }

    void PersistRemoveVolatileDependency(ui64 lockId, ui64 txId) override {
        using Schema = TSchemaDescription;
        NIceDb::TNiceDb db(DB);
        db.Table<typename Schema::LockVolatileDependencies>().Key(lockId, txId).Delete();
        HasChanges_ = true;
    }

protected:
    TShard& Self;
    NTable::TDatabase& DB;
    bool HasChanges_ = false;
};
}
