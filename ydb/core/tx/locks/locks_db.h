#pragma once

#include "locks.h"
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr::NLocks {

template<class TShard, class TSchemaDescription>
class TShardLocksDb : public NKikimr::NDataShard::ILocksDb {
public:
    TShardLocksDb(TShard& self, NTabletFlatExecutor::TTransactionContext& txc)
        : TShardLocksDb(self, txc.DB)
    { }

    TShardLocksDb(TShard& self, NTable::TDatabase& db)
        : Self(self)
        , DB(db)
    { }

    bool HasChanges() const override {
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

        // Load write seq nums (DataShard only; ColumnShard has no LockWriteSeqNums table)
        if constexpr (requires { typename Schema::LockWriteSeqNums; }) {
            if (db.HaveTable<typename Schema::LockWriteSeqNums>()) {
                auto rowset = db.Table<typename Schema::LockWriteSeqNums>().Select();
                if (!rowset.IsReady()) {
                    return false;
                }
                while (!rowset.EndOfSet()) {
                    auto lockId = rowset.template GetValue<typename Schema::LockWriteSeqNums::LockId>();
                    auto it = lockIndex.find(lockId);
                    if (it != lockIndex.end()) {
                        auto& lock = rows[it->second];
                        NDataShard::TWriteSeqNumState state;
                        state.WriterIndex = rowset.template GetValue<typename Schema::LockWriteSeqNums::WriterIndex>();
                        state.WriteSeqNum = rowset.template GetValue<typename Schema::LockWriteSeqNums::WriteSeqNum>();
                        state.SerializedResult = rowset.template GetValueOrDefault<typename Schema::LockWriteSeqNums::WriteResult>();
                        if (rowset.template HaveValue<typename Schema::LockWriteSeqNums::GcWriteSeqNum>()) {
                            state.GcWriteSeqNum = rowset.template GetValue<typename Schema::LockWriteSeqNums::GcWriteSeqNum>();
                        }
                        if (state.WriteSeqNum) {
                            lock.WriteSeqNumStates.push_back(std::move(state));
                        }
                    }
                    if (!rowset.Next()) {
                        return false;
                    }
                }
            }
        }

        THashMap<std::tuple<ui64, ui64, ui64>, size_t> ancestorOffsets;
        // Load ancestor shard locks (DataShard only)
        if constexpr (requires { typename Schema::AncestorShardsLocks; }) {
            using T = typename Schema::AncestorShardsLocks;
            if (db.HaveTable<T>()) {
                std::optional<ui64> lastOrphan;
                auto rowset = db.Table<T>().Select();
                if (!rowset.IsReady()) {
                    return false;
                }
                while (!rowset.EndOfSet()) {
                    const auto lockId = rowset.template GetValue<typename T::LockId>();
                    const auto owner = rowset.template GetValue<typename T::TabletId>();
                    const auto createdOn = rowset.template GetValue<typename T::CreatedOn>();
                    const auto key = std::make_tuple(lockId, owner, createdOn);
                    auto it = lockIndex.find(lockId);
                    if (it != lockIndex.end()) {
                        auto& lock = rows[it->second];
                        NDataShard::TAncestorLock ancestor;
                        ancestor.TabletId = owner;
                        ancestor.CreatedOn = createdOn;
                        ancestor.CreatedOnRange = NDataShard::DeserializeLockCreatedOnRange(
                            rowset.template GetValueOrDefault<typename T::CreatedOnRange>());
                        ancestor.Generation = rowset.template GetValue<typename T::Generation>();
                        ancestor.Counter = rowset.template GetValue<typename T::Counter>();
                        ancestor.CreationTime = TInstant::MicroSeconds(rowset.template GetValue<typename T::CreateTimestamp>());
                        ancestor.Flags = NDataShard::ELockFlags(rowset.template GetValue<typename T::Flags>());
                        ancestorOffsets.emplace(key, lock.AncestorLocks.size());
                        lock.AncestorLocks.push_back(std::move(ancestor));
                    } else if (lastOrphan != lockId) {
                        // Rows are ordered by LockId. One task covers every ancestor of an orphan.
                        ScheduleAncestorGc({lockId, std::nullopt});
                        lastOrphan = lockId;
                    }
                    if (!rowset.Next()) {
                        return false;
                    }
                }
            }
        }

        // Load ancestor shard write seq nums (DataShard only)
        if constexpr (requires { typename Schema::AncestorLockWriteSeqNums; }) {
            using T = typename Schema::AncestorLockWriteSeqNums;
            if (db.HaveTable<T>()) {
                auto rowset = db.Table<T>().Select();
                if (!rowset.IsReady()) {
                    return false;
                }
                while (!rowset.EndOfSet()) {
                    const auto lockId = rowset.template GetValue<typename T::LockId>();
                    const auto key = std::make_tuple(lockId, rowset.template GetValue<typename T::TabletId>(),
                        rowset.template GetValue<typename T::CreatedOn>());
                    // A deleted parent can leave writer rows until its full GC task runs.
                    if (const size_t* offset = ancestorOffsets.FindPtr(key)) {
                        auto& lock = rows[lockIndex.at(lockId)];
                        NDataShard::TWriteSeqNumState state;
                        state.WriterIndex = rowset.template GetValue<typename T::WriterIndex>();
                        state.WriteSeqNum = rowset.template GetValue<typename T::WriteSeqNum>();
                        state.SerializedResult = rowset.template GetValueOrDefault<typename T::WriteResult>();
                        if (rowset.template HaveValue<typename T::GcWriteSeqNum>()) {
                            state.GcWriteSeqNum = rowset.template GetValue<typename T::GcWriteSeqNum>();
                        }
                        if (state.WriteSeqNum) {
                            lock.AncestorLocks[*offset].WriteSeqNumStates[state.WriterIndex] = std::move(state);
                        }
                    }
                    if (!rowset.Next()) {
                        return false;
                    }
                }
            }
        }

        if constexpr (requires { typename Schema::AncestorLockBranches; }) {
            using T = typename Schema::AncestorLockBranches;
            if (db.HaveTable<T>()) {
                // A local bound and its writer metadata suffice to recover a
                // task without reading any of the closed branches.
                for (auto& lock : rows) {
                    if (lock.Counter == Max<ui64>()) {
                        continue;
                    }
                    THashSet<ui64> closedWriters;
                    for (const auto& current : lock.WriteSeqNumStates) {
                        if (current.GcWriteSeqNum) {
                            closedWriters.insert(current.WriterIndex);
                        }
                    }
                    for (auto& ancestor : lock.AncestorLocks) {
                        for (auto& [writer, state] : ancestor.WriteSeqNumStates) {
                            if (closedWriters.contains(writer)) {
                                ScheduleAncestorGc({lock.LockId, std::tuple{ancestor.TabletId, ancestor.CreatedOn, writer}});
                                continue;
                            }
                            auto branches = db.Table<T>().Prefix(lock.LockId, ancestor.TabletId, ancestor.CreatedOn, writer)
                                .template Select<typename T::ShardsPath, typename T::WriteSeqNum, typename T::WriteResult>();
                            if (!branches.IsReady()) {
                                return false;
                            }
                            while (!branches.EndOfSet()) {
                                auto& branch = state.Branches.emplace_back();
                                branch.ShardsPath = NDataShard::DeserializeLockShardsPath(branches.template GetValue<typename T::ShardsPath>());
                                branch.WriteSeqNum = branches.template GetValue<typename T::WriteSeqNum>();
                                branch.SerializedResult = branches.template GetValueOrDefault<typename T::WriteResult>();
                                Y_ENSURE(branch.ShardsPath.front() == ancestor.TabletId && branch.WriteSeqNum <= state.WriteSeqNum);
                                if (!branches.Next()) {
                                    return false;
                                }
                            }
                        }
                    }
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

    void PersistLockWriteSeqNum(ui64 lockId, ui64 writerIndex, ui64 writeSeqNum, const TString& serializedResult, std::optional<ui64> gcWriteSeqNum = std::nullopt) override {
        using Schema = TSchemaDescription;
        if constexpr (requires { typename Schema::LockWriteSeqNums; }) {
            NIceDb::TNiceDb db(DB);
            using T = typename Schema::LockWriteSeqNums;
            auto row = db.Table<T>().Key(lockId, writerIndex);
            const NIceDb::TUpdate<typename T::WriteSeqNum> wsn(writeSeqNum);
            const NIceDb::TUpdate<typename T::WriteResult> result(serializedResult);
            if (gcWriteSeqNum) {
                row.Update(wsn, result, NIceDb::TUpdate<typename T::GcWriteSeqNum>(*gcWriteSeqNum));
            } else {
                row.Update(wsn, result, NIceDb::TNull<typename T::GcWriteSeqNum>());
            }
            HasChanges_ = true;
        } else {
            Y_UNUSED(lockId, writerIndex, writeSeqNum, serializedResult, gcWriteSeqNum);
            Y_ABORT("WriteSeqNum is not supported");
        }
    }

    void PersistRemoveLockWriteSeqNum(ui64 lockId, ui64 writerIndex) override {
        using Schema = TSchemaDescription;
        if constexpr (requires { typename Schema::LockWriteSeqNums; }) {
            NIceDb::TNiceDb db(DB);
            db.Table<typename Schema::LockWriteSeqNums>().Key(lockId, writerIndex).Delete();
            HasChanges_ = true;
        } else {
            Y_UNUSED(lockId, writerIndex);
            Y_ABORT("WriteSeqNum is not supported");
        }
    }

    void PersistAddAncestorLock(ui64 lockId, const NDataShard::TAncestorLock& lock) override {
        using Schema = TSchemaDescription;
        if constexpr (requires { typename Schema::AncestorShardsLocks; }) {
            NIceDb::TNiceDb db(DB);
            db.Table<typename Schema::AncestorShardsLocks>().Key(lockId, lock.TabletId, lock.CreatedOn).Update(
                NIceDb::TUpdate<typename Schema::AncestorShardsLocks::Generation>(lock.Generation),
                NIceDb::TUpdate<typename Schema::AncestorShardsLocks::Counter>(lock.Counter),
                NIceDb::TUpdate<typename Schema::AncestorShardsLocks::CreateTimestamp>(lock.CreationTime.MicroSeconds()),
                NIceDb::TUpdate<typename Schema::AncestorShardsLocks::Flags>(ui64(lock.Flags)),
                NIceDb::TUpdate<typename Schema::AncestorShardsLocks::CreatedOnRange>(NDataShard::SerializeLockCreatedOnRange(lock.CreatedOnRange)));
            HasChanges_ = true;
        } else {
            Y_ENSURE(false, "AncestorShardsLocks is not supported");
        }
    }

    void PersistAncestorLockWriteSeqNum(ui64 lockId, NDataShard::TLockKey key, ui64 writerIndex, ui64 writeSeqNum, const TString& serializedResult, std::optional<ui64> gcWriteSeqNum = std::nullopt) override {
        using Schema = TSchemaDescription;
        if constexpr (requires { typename Schema::AncestorLockWriteSeqNums; }) {
            NIceDb::TNiceDb db(DB);
            db.Table<typename Schema::AncestorLockWriteSeqNums>().Key(lockId, key.Owner, key.CreatedOn, writerIndex).Update(
                NIceDb::TUpdate<typename Schema::AncestorLockWriteSeqNums::WriteSeqNum>(writeSeqNum),
                NIceDb::TUpdate<typename Schema::AncestorLockWriteSeqNums::WriteResult>(serializedResult));
            if (gcWriteSeqNum) {
                db.Table<typename Schema::AncestorLockWriteSeqNums>().Key(lockId, key.Owner, key.CreatedOn, writerIndex).Update(
                    NIceDb::TUpdate<typename Schema::AncestorLockWriteSeqNums::GcWriteSeqNum>(*gcWriteSeqNum));
            } else {
                db.Table<typename Schema::AncestorLockWriteSeqNums>().Key(lockId, key.Owner, key.CreatedOn, writerIndex).Update(
                    NIceDb::TNull<typename Schema::AncestorLockWriteSeqNums::GcWriteSeqNum>());
            }
            HasChanges_ = true;
        } else {
            Y_ENSURE(false, "AncestorLockWriteSeqNums is not supported");
        }
    }

    void PersistAncestorLockBranch(ui64 lockId, NDataShard::TLockKey key, ui64 writerIndex,
        const NDataShard::TWriteSeqNumBranch& branch) override
    {
        using Schema = TSchemaDescription;
        if constexpr (requires { typename Schema::AncestorLockBranches; }) {
            NIceDb::TNiceDb db(DB);
            db.Table<typename Schema::AncestorLockBranches>().Key(lockId, key.Owner, key.CreatedOn,
                writerIndex, NDataShard::SerializeLockShardsPath(branch.ShardsPath)).Update(
                    NIceDb::TUpdate<typename Schema::AncestorLockBranches::WriteSeqNum>(branch.WriteSeqNum),
                    NIceDb::TUpdate<typename Schema::AncestorLockBranches::WriteResult>(branch.SerializedResult));
            HasChanges_ = true;
        } else {
            Y_ABORT("AncestorLockBranches is not supported");
        }
    }

    void ScheduleAncestorGc(TAncestorGcTask task) override {
        if constexpr (requires { typename TSchemaDescription::AncestorLockBranches; }) {
            DB.OnCommit([shard = &Self, task] { shard->ScheduleAncestorGc(task); });
        } else {
            Y_ABORT("AncestorLockBranches is not supported");
        }
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

    // Schedules callback when changes are confirmed to be persistent
    void OnPersistent(std::function<void()> callback) override {
        DB.OnPersistent(std::move(callback));
    }

    void OnRollback(std::function<void()> callback) override {
        // Execute must see the activated lock for WSN and lock echo generation;
        // deferring just its flag until commit would expose inconsistent state.
        // Localdb rollback cannot undo the in-memory registrations by itself.
        DB.OnRollback(std::move(callback));
    }

protected:
    TShard& Self;
    NTable::TDatabase& DB;
    bool HasChanges_ = false;
};
}
