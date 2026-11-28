#include "datashard_locks_schema.h"
#include "datashard_locks_db.h"

#include <ydb/core/base/resource_profile.h>
#include <ydb/core/protos/data_events.pb.h>
#include <ydb/core/protos/query_stats.pb.h>
#include <ydb/core/tx/locks/locks_db.h>
#include <ydb/core/tablet_flat/test/libs/table/test_dbase.h>
#include <ydb/core/tablet_flat/flat_executor_ut_common.h>
#include <ydb/library/testlib/helpers.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/scope.h>

namespace NKikimr::NDataShard {
namespace {

using TSchema = TDataShardLocksDb::TSchema;
using NTable::NTest::TDbExec;
using NTable::NTest::EPlay;

struct TLegacySchema : NIceDb::Schema {
    using Sys = TSchema::Sys;
    using Locks = TSchema::Locks;

    struct LockWriteSeqNums : TSchema::LockWriteSeqNums {
        using TColumns = TableColumns<LockId, WriterIndex, WriteSeqNum, WriteResult>;
    };
    struct AncestorShardsLocks : Table<TSchema::LegacyAncestorShardsLocksTableId> {
        struct LockId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct TabletId : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Generation : Column<3, NScheme::NTypeIds::Uint32> {};
        struct Counter : Column<4, NScheme::NTypeIds::Uint64> {};
        struct CreateTimestamp : Column<5, NScheme::NTypeIds::Uint64> {};
        struct Flags : Column<6, NScheme::NTypeIds::Uint64> {};
        using TKey = TableKey<LockId, TabletId>;
        using TColumns = TableColumns<LockId, TabletId, Generation, Counter, CreateTimestamp, Flags>;
    };
    struct AncestorLockWriteSeqNums : Table<TSchema::LegacyAncestorLockWriteSeqNumsTableId> {
        struct LockId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct TabletId : Column<2, NScheme::NTypeIds::Uint64> {};
        struct WriterIndex : Column<3, NScheme::NTypeIds::Uint64> {};
        struct WriteSeqNum : Column<4, NScheme::NTypeIds::Uint64> {};
        struct WriteResult : Column<5, NScheme::NTypeIds::String> {};
        using TKey = TableKey<LockId, TabletId, WriterIndex>;
        using TColumns = TableColumns<LockId, TabletId, WriterIndex, WriteSeqNum, WriteResult>;
    };
    struct UserData : Table<1000> {
        struct Key : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Value : Column<2, NScheme::NTypeIds::Uint64> {};
        using TKey = TableKey<Key>;
        using TColumns = TableColumns<Key, Value>;
    };
    using TTables = SchemaTables<Sys, Locks, LockWriteSeqNums, AncestorShardsLocks, AncestorLockWriteSeqNums, UserData>;
};

void CreateLegacy(NTable::TDatabase& database) {
    NIceDb::TNiceDb db(database);
    db.Materialize<TLegacySchema>();
    db.Table<TLegacySchema::LockWriteSeqNums>().Key(100, 7).Update(
        NIceDb::TUpdate<TLegacySchema::LockWriteSeqNums::WriteSeqNum>(8));
    db.Table<TLegacySchema::AncestorShardsLocks>().Key(100, 10).Update(
        NIceDb::TUpdate<TLegacySchema::AncestorShardsLocks::Counter>(123));
    db.Table<TLegacySchema::AncestorLockWriteSeqNums>().Key(100, 10, 7).Update(
        NIceDb::TUpdate<TLegacySchema::AncestorLockWriteSeqNums::WriteSeqNum>(8));

    // An unrelated ordinary transaction must keep its lock and uncommitted data.
    db.Table<TSchema::Locks>().Key(200).Update(
        NIceDb::TUpdate<TSchema::Locks::Generation>(3),
        NIceDb::TUpdate<TSchema::Locks::Counter>(123));
    const ui64 key = 1;
    const ui64 value = 42;
    const TRawTypeValue rawKey(&key, sizeof(key), NScheme::NTypeIds::Uint64);
    const NTable::TUpdateOp update = NIceDb::TUpdate<TLegacySchema::UserData::Value>(value);
    database.UpdateTx(TLegacySchema::UserData::TableId, NTable::ERowOp::Upsert,
        {&rawKey, 1}, {&update, 1}, 200);
}

bool Upgrade(NTable::TDatabase& database) {
    NIceDb::TNiceDb(database).Materialize<TSchema>();
    return UpdateLocksSchema(database);
}

void FinishUpgrade(TDbExec& exec) {
    exec.Begin();
    UNIT_ASSERT(Upgrade(*exec.operator->()));
    exec.Commit();
}

TString MakeBranchResult(ui64 cpuTime) {
    NKikimrDataEvents::TWriteSeqNumResult result;
    result.MutableTxStats()->SetComputeCpuTimeUsec(cpuTime);
    return result.SerializeAsString();
}

struct TTestShard {
    TSet<ILocksDb::TAncestorGcTask> PendingAncestorGc;
    void ScheduleAncestorGc(ILocksDb::TAncestorGcTask task) { PendingAncestorGc.insert(std::move(task)); }
    ui64 TabletID() const { return 40; }
    ui32 Generation() const { return 1; }
    bool IsUserTable(const TTableId&) const { return true; }
    TRowVersion LastCompleteTxVersion() const { return TRowVersion::Min(); }
    template <typename TCounter, typename TValue>
    void IncCounter(TCounter, TValue) const {}
};

class TTestLocksDb : public NLocks::TShardLocksDb<TTestShard, TSchema> {
public:
    using TShardLocksDb::TShardLocksDb;

    bool MayAdd = true;
    bool MayActivate = true;
    bool HasPendingAncestorGc(ui64 id) const override {
        const auto it = Self.PendingAncestorGc.lower_bound({id, std::nullopt});
        return it != Self.PendingAncestorGc.end() && it->LockId == id;
    }

    bool MayAddLock(ui64 id) override { return MayAdd && !HasPendingAncestorGc(id); }
    bool MayActivateLock(ui64) override { return MayActivate; }

    void PersistRemoveLock(ui64 lockId) override {
        NIceDb::TNiceDb db(DB);
        db.Table<TSchema::Locks>().Key(lockId).Delete();
        HasChanges_ = true;
    }
};

ILocksDb::TLockRow MakeTrackingRow() {
    ILocksDb::TLockRow row{};
    row.LockId = 100;
    row.LockNodeId = 1;
    row.Flags = ui64(ELockFlags::TrackingOnly);
    return row;
}

ILocksDb::TLockRow MakeBrokenRow() {
    ILocksDb::TLockRow row{};
    row.LockId = 100;
    row.LockNodeId = 1;
    row.Counter = Max<ui64>();
    return row;
}

void AssertEmptyState(TDbExec& exec) {
    NIceDb::TNiceDb db(*exec.operator->());
    UNIT_ASSERT(db.Table<TSchema::LockWriteSeqNums>().Select().EndOfSet());
    UNIT_ASSERT(db.Table<TSchema::AncestorShardsLocks>().Select().EndOfSet());
    UNIT_ASSERT(db.Table<TSchema::AncestorLockWriteSeqNums>().Select().EndOfSet());
    UNIT_ASSERT(db.Table<TSchema::AncestorLockBranches>().Select().EndOfSet());
}

void DrainAncestorGc(TDbExec& exec, TTestShard& shard) {
    using EResult = TDataShardLocksDb::EAncestorGcResult;
    for (const auto& task : shard.PendingAncestorGc) {
        TDataShardLocksDb::TAncestorGcCursor cursor;
        EResult result;
        do {
            exec.Begin();
            size_t remaining = TDataShardLocksDb::AncestorGcScanBudget;
            result = TDataShardLocksDb::RemoveAncestorGcBatch(*exec.operator->(), task, cursor, remaining);
            UNIT_ASSERT(result != EResult::NotReady);
            exec.Commit();
        } while (result == EResult::More);
    }
    shard.PendingAncestorGc.clear();
}

class TTxLocksSchemaTest : public NTabletFlatExecutor::ITransaction {
public:
    explicit TTxLocksSchemaTest(std::function<bool(NTable::TDatabase&)> action)
        : Action(std::move(action))
    {}

    bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext&) override {
        return Action(txc.DB);
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }

private:
    std::function<bool(NTable::TDatabase&)> Action;
};

} // namespace

Y_UNIT_TEST_SUITE(DataShardLocksSchema) {
    Y_UNIT_TEST(TrackingOnlyValidatesActivationWithoutPublishingIt) {
        TDbExec exec;
        FinishUpgrade(exec);
        TTestShard shard;
        TSysLocks locks(&shard);
        exec.Begin();
        TTestLocksDb db(shard, *exec.operator->());
        UNIT_ASSERT(locks.RestoreLockFromSplitSrc(MakeTrackingRow(), db));
        TLocksUpdate update;
        update.LockTxId = 100;
        update.LockNodeId = 1;
        locks.SetupUpdate(&update, &db);
        db.MayAdd = false; // Borrowed references must not prohibit activation.
        db.MayActivate = false; // But an existing commit/abort decision must.
        UNIT_ASSERT(locks.EnsureCurrentLock() == EEnsureCurrentLock::Abort);
        db.MayActivate = true;
        update.LockNodeId = 2;
        UNIT_ASSERT(locks.EnsureCurrentLock() == EEnsureCurrentLock::Abort);
        update.LockNodeId = 1;
        UNIT_ASSERT(locks.EnsureCurrentLock(false) == EEnsureCurrentLock::Missing);
        UNIT_ASSERT(!update.Lock);
        UNIT_ASSERT(locks.EnsureCurrentLock() == EEnsureCurrentLock::Success);
        UNIT_ASSERT(update.Lock->IsTrackingOnly());
        // Neither lock-table version exposes a physical lifetime record.
        const TVector<TCell> v1{TCell::Make(ui64(100)), TCell::Make(shard.TabletID())};
        const TVector<TCell> v2{v1[0], v1[1], TCell::Make(ui64(1)), TCell::Make(ui64(2))};
        UNIT_ASSERT(!locks.GetLock(v1).LockId);
        UNIT_ASSERT(!locks.GetLock(v2).LockId);
        UNIT_ASSERT(locks.ApplyLocks().first.empty());
        UNIT_ASSERT(locks.GetRawLock(100)->IsTrackingOnly());
        locks.ResetUpdate();
        UNIT_ASSERT(locks.GetRawLock(100));
        UNIT_ASSERT(locks.GetRawLock(100)->IsTrackingOnly());
        exec.Commit();
    }

    Y_UNIT_TEST_TWIN(TrackingOnlyPromotionSurvivesRestartAndRollback, Ancestor) {
        TDbExec exec;
        FinishUpgrade(exec);
        TTestShard shard;
        const TPathId table(1000, 10001);
        const TVector<NScheme::TTypeInfo> keyTypes{NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)};
        auto migrated = MakeTrackingRow();
        {
            TSysLocks locks(&shard);
            exec.Begin();
            TTestLocksDb db(shard, *exec.operator->());
            UNIT_ASSERT(locks.RestoreLockFromSplitSrc(MakeTrackingRow(), db));
            const auto tracker = locks.GetRawLock(100);
            UNIT_ASSERT(!tracker->IsPersistent());
            migrated.Generation = tracker->GetGeneration();
            migrated.Counter = tracker->GetCounter();
            migrated.CreateTs = tracker->GetCreationTime().MicroSeconds();
            exec.Commit();
        }
        exec.Replay(EPlay::Boot);
        {
            TSysLocks locks(&shard);
            locks.UpdateSchema(table, keyTypes);
            exec.Begin();
            TTestLocksDb db(shard, *exec.operator->());
            UNIT_ASSERT(locks.Load(db));
            UNIT_ASSERT(!locks.GetRawLock(100));
            THashMap<ui64, ILocksDb::TLockRow> rows;
            rows.emplace(100, std::move(migrated));
            locks.RestoreInMemoryLocks(std::move(rows));
            UNIT_ASSERT(!locks.GetRawLock(100)->IsPersistent());
            UNIT_ASSERT(locks.GetRawLock(100)->IsTrackingOnly());
            auto pending = locks.NextPendingSubscribeLock();
            UNIT_ASSERT_VALUES_EQUAL(pending.LockId, 100);
            UNIT_ASSERT_VALUES_EQUAL(pending.LockNodeId, 1);
            exec.Commit();
            for (bool commit : {false, true}) {
                exec.Begin();
                TLocksUpdate update;
                update.LockTxId = 100;
                update.LockNodeId = 1;
                locks.SetupUpdate(&update, &db);
                UNIT_ASSERT(locks.EnsureCurrentLock(false) == EEnsureCurrentLock::Missing);
                update.WriteSeqNumUpdate.ConstructInPlace(7);
                const TVector<ui64> ancestors = Ancestor ? TVector<ui64>{10} : TVector<ui64>{};
                using EStatus = TLocksUpdate::TWriteSeqNumUpdate::EStatus;
                UNIT_ASSERT(update.WriteSeqNumUpdate->StageOperation(nullptr, shard.TabletID(), ancestors, 1, table).Status == EStatus::Fresh);
                UNIT_ASSERT(locks.EnsureCurrentLock() == EEnsureCurrentLock::Success);
                auto [echo, broken] = locks.ApplyLocks();
                locks.ResetUpdate();
                UNIT_ASSERT(broken.empty());
                UNIT_ASSERT_VALUES_EQUAL(echo.size(), 1);
                UNIT_ASSERT(!locks.GetRawLock(100)->IsTrackingOnly());
                if (commit) {
                    exec.Commit();
                } else {
                    exec.Reject();
                    const auto lock = locks.GetRawLock(100);
                    UNIT_ASSERT(lock->IsTrackingOnly());
                    UNIT_ASSERT(!lock->IsPersistent());
                    UNIT_ASSERT(!lock->IsPersisting());
                    UNIT_ASSERT(lock->GetWriteTables().empty());
                    UNIT_ASSERT(lock->GetWriteSeqNumStates().empty());
                    UNIT_ASSERT(lock->GetAncestorLocks().empty());
                }
            }
        }
        exec.Replay(EPlay::Boot);
        exec.Begin();
        TSysLocks locks(&shard);
        locks.UpdateSchema(table, keyTypes);
        TTestLocksDb db(shard, *exec.operator->());
        UNIT_ASSERT(locks.Load(db));
        const auto lock = locks.GetRawLock(100);
        UNIT_ASSERT(!lock->IsTrackingOnly());
        UNIT_ASSERT_VALUES_EQUAL(lock->GetWriteSeqNumStates().empty(), Ancestor);
        UNIT_ASSERT_VALUES_EQUAL(lock->GetAncestorLocks().size(), Ancestor ? 1 : 0);
        UNIT_ASSERT(lock->GetWriteTables().contains(table));
        exec.Commit();
    }

    Y_UNIT_TEST_TWIN(TrackingOnlySurvivesOnlyWithInMemoryState, Migrate) {
        TDbExec exec;
        FinishUpgrade(exec);
        TTestShard shard;
        auto migrated = MakeTrackingRow();
        {
            TSysLocks locks(&shard);
            exec.Begin();
            TTestLocksDb db(shard, *exec.operator->());
            UNIT_ASSERT(locks.RestoreLockFromSplitSrc(MakeTrackingRow(), db));
            const auto tracker = locks.GetRawLock(100);
            UNIT_ASSERT(!tracker->IsPersistent());
            UNIT_ASSERT(!db.HasChanges());
            migrated.Generation = tracker->GetGeneration();
            migrated.Counter = tracker->GetCounter();
            migrated.CreateTs = tracker->GetCreationTime().MicroSeconds();
            exec.Commit();
        }
        exec.Replay(EPlay::Boot);
        TSysLocks locks(&shard);
        exec.Begin();
        TTestLocksDb db(shard, *exec.operator->());
        UNIT_ASSERT(locks.Load(db));
        UNIT_ASSERT(!locks.GetRawLock(100));
        if (Migrate) {
            THashMap<ui64, ILocksDb::TLockRow> rows;
            rows.emplace(100, std::move(migrated));
            locks.RestoreInMemoryLocks(std::move(rows));
            const auto tracker = locks.GetRawLock(100);
            UNIT_ASSERT(tracker->IsTrackingOnly());
            UNIT_ASSERT(!tracker->IsPersistent());
            UNIT_ASSERT_VALUES_EQUAL(locks.TrackingOnlyLocksCount(), 1);
            const auto pending = locks.NextPendingSubscribeLock();
            UNIT_ASSERT_VALUES_EQUAL(pending.LockId, 100);
            UNIT_ASSERT_VALUES_EQUAL(pending.LockNodeId, 1);
            // A tracker restored once must remain transferable to the next generation.
            auto migratedAgain = MakeTrackingRow();
            migratedAgain.Flags = ui64(tracker->GetFlags());
            migratedAgain.Generation = tracker->GetGeneration();
            migratedAgain.Counter = tracker->GetRawCounter();
            migratedAgain.CreateTs = tracker->GetCreationTime().MicroSeconds();
            THashMap<ui64, ILocksDb::TLockRow> nextRows;
            nextRows.emplace(100, std::move(migratedAgain));
            TSysLocks next(&shard);
            UNIT_ASSERT(next.Load(db));
            next.RestoreInMemoryLocks(std::move(nextRows));
            UNIT_ASSERT(next.GetRawLock(100)->IsTrackingOnly());
            UNIT_ASSERT(!next.GetRawLock(100)->IsPersistent());
            UNIT_ASSERT_VALUES_EQUAL(next.TrackingOnlyLocksCount(), 1);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(locks.TrackingOnlyLocksCount(), 0);
            // References left in borrowed parts cannot authorize a new lock.
            db.MayAdd = false;
            TLocksUpdate update;
            update.LockTxId = 100;
            update.LockNodeId = 1;
            locks.SetupUpdate(&update, &db);
            UNIT_ASSERT(locks.EnsureCurrentLock() == EEnsureCurrentLock::Abort);
            locks.ResetUpdate();
        }
        UNIT_ASSERT(!db.HasChanges());
        exec.Commit();
    }

    Y_UNIT_TEST_TWIN(RejectedTrackingOnlyFollowsOwnerLifetime, OwnerEnded) {
        TDbExec exec;
        FinishUpgrade(exec);
        TTestShard shard;
        {
            TSysLocks locks(&shard);
            exec.Begin();
            TTestLocksDb db(shard, *exec.operator->());
            UNIT_ASSERT(locks.RestoreLockFromSplitSrc(MakeTrackingRow(), db));
            UNIT_ASSERT(locks.RestoreLockFromSplitSrc(MakeBrokenRow(), db));
            const auto lock = locks.GetRawLock(100);
            UNIT_ASSERT(lock->IsPersistent());
            UNIT_ASSERT(lock->IsBroken());
            UNIT_ASSERT(!lock->IsTrackingOnly());
            UNIT_ASSERT_VALUES_EQUAL(locks.TrackingOnlyLocksCount(), 0);
            if (OwnerEnded) {
                // Rejection uses the same owner-driven removal as ordinary locks.
                locks.RemoveSubscribedLock(100, &db);
                UNIT_ASSERT(!locks.GetRawLock(100));
            }
            exec.Commit();
        }
        exec.Replay(EPlay::Boot);
        TSysLocks locks(&shard);
        exec.Begin();
        TTestLocksDb db(shard, *exec.operator->());
        UNIT_ASSERT(locks.Load(db));
        const auto lock = locks.GetRawLock(100);
        if (OwnerEnded) {
            UNIT_ASSERT(!lock);
        } else {
            UNIT_ASSERT(lock->IsPersistent());
            UNIT_ASSERT(lock->IsBroken());
            UNIT_ASSERT_VALUES_EQUAL(lock->GetLockNodeId(), 1);
        }
        exec.Commit();
    }

    Y_UNIT_TEST_TWIN(TrackingOnlyMergesWithLogicalLock, TrackingFirst) {
        TDbExec exec;
        FinishUpgrade(exec);
        TTestShard shard;
        TSysLocks locks(&shard);
        auto real = MakeTrackingRow();
        real.Flags = 0;
        auto& ancestor = real.AncestorLocks.emplace_back();
        ancestor.TabletId = ancestor.CreatedOn = 10;
        ancestor.Generation = 1;
        ancestor.Counter = 123;
        exec.Begin();
        TTestLocksDb db(shard, *exec.operator->());
        if (TrackingFirst) {
            UNIT_ASSERT(locks.RestoreLockFromSplitSrc(MakeTrackingRow(), db));
        }
        UNIT_ASSERT(locks.RestoreLockFromSplitSrc(std::move(real), db));
        UNIT_ASSERT(locks.RestoreLockFromSplitSrc(MakeTrackingRow(), db));
        UNIT_ASSERT(!locks.GetRawLock(100)->IsTrackingOnly());
        UNIT_ASSERT_VALUES_EQUAL(locks.GetRawLock(100)->GetAncestorLocks().size(), 1);
        auto wrongNode = MakeTrackingRow();
        wrongNode.LockNodeId = 2;
        UNIT_ASSERT(!locks.RestoreLockFromSplitSrc(std::move(wrongNode), db));
        TLocksUpdate update;
        locks.SetupUpdate(&update, &db);
        locks.BreakLock(100);
        locks.ApplyLocks();
        locks.ResetUpdate();
        UNIT_ASSERT(!locks.RestoreLockFromSplitSrc(MakeTrackingRow(), db));
        exec.Commit();
    }

    Y_UNIT_TEST(LockCreationChecksDataReferences) {
        TDbExec exec;
        FinishUpgrade(exec);
        TTestShard shard;
        TSysLocks locks(&shard);
        exec.Begin();
        TTestLocksDb db(shard, *exec.operator->());
        TLocksUpdate update;
        update.LockTxId = 100;
        locks.SetupUpdate(&update, &db);
        db.MayAdd = false;
        UNIT_ASSERT(locks.EnsureCurrentLock() == EEnsureCurrentLock::Abort);
        UNIT_ASSERT(!locks.GetRawLock(100));
        db.MayAdd = true;
        UNIT_ASSERT(locks.EnsureCurrentLock(false) == EEnsureCurrentLock::Missing);
        UNIT_ASSERT(locks.EnsureCurrentLock() == EEnsureCurrentLock::Success);
        UNIT_ASSERT(update.Lock);
        locks.ResetUpdate();
        exec.Commit();
    }

    Y_UNIT_TEST(SplitMergeRestorePersistsPathsRangesResultsAndGc) {
        TDbExec exec;
        FinishUpgrade(exec);
        TTestShard shard;
        const TPathId table(1, 2);
        const TVector<NScheme::TTypeInfo> keyTypes{NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)};
        auto makeRow = [&](ui64 child, ui64 wsn) {
            ILocksDb::TLockRow row{};
            row.LockId = 100;
            row.LockNodeId = 1;
            row.WriteTables.push_back(table);
            auto& ancestor = row.AncestorLocks.emplace_back();
            ancestor.TabletId = ancestor.CreatedOn = 10;
            ancestor.Generation = 2;
            ancestor.Counter = 123;
            const ui32 from = 10, to = 50;
            const TCell fromCell = TCell::Make(from), toCell = TCell::Make(to);
            ancestor.CreatedOnRange.emplace(TConstArrayRef<TCell>(&fromCell, 1), true,
                TConstArrayRef<TCell>(&toCell, 1), false);
            auto& state = ancestor.WriteSeqNumStates[7];
            state.WriterIndex = 7;
            state.WriteSeqNum = wsn;
            state.SerializedResult = MakeBranchResult(wsn);
            state.GcWriteSeqNum = 5;
            state.Branches = {{{10}, 5, MakeBranchResult(5)}, {{10, child}, wsn, MakeBranchResult(wsn)}};
            return row;
        };
        {
            TSysLocks locks(&shard);
            locks.UpdateSchema(table, keyTypes);
            exec.Begin();
            TTestLocksDb db(shard, *exec.operator->());
            UNIT_ASSERT(locks.RestoreLockFromSplitSrc(makeRow(20, 6), db));
            exec.Commit();
        }
        exec.Replay(EPlay::Boot);
        {
            TSysLocks locks(&shard);
            locks.UpdateSchema(table, keyTypes);
            exec.Begin();
            TTestLocksDb db(shard, *exec.operator->());
            UNIT_ASSERT(locks.Load(db));
            const auto* previous = locks.GetRawLock(100)->FindAncestorLock({10, 10});
            UNIT_ASSERT(previous);
            UNIT_ASSERT_VALUES_EQUAL(previous->WriteSeqNumStates.at(7).SerializedResult, MakeBranchResult(6));
            UNIT_ASSERT(locks.RestoreLockFromSplitSrc(makeRow(30, 7), db));
            auto conflict = makeRow(50, 8);
            conflict.AncestorLocks[0].WriteSeqNumStates.at(7).Branches[0].WriteSeqNum = 6;
            UNIT_ASSERT(!locks.RestoreLockFromSplitSrc(std::move(conflict), db));
            exec.Commit();
        }
        exec.Replay(EPlay::Boot);
        {
            TSysLocks locks(&shard);
            locks.UpdateSchema(table, keyTypes);
            exec.Begin();
            TTestLocksDb db(shard, *exec.operator->());
            UNIT_ASSERT(locks.Load(db));
            const auto* ancestor = locks.GetRawLock(100)->FindAncestorLock({10, 10});
            UNIT_ASSERT(ancestor && ancestor->CreatedOnRange);
            UNIT_ASSERT_VALUES_EQUAL(SerializeLockCreatedOnRange(ancestor->CreatedOnRange),
                SerializeLockCreatedOnRange(makeRow(20, 6).AncestorLocks[0].CreatedOnRange));
            const auto& state = ancestor->WriteSeqNumStates.at(7);
            UNIT_ASSERT_VALUES_EQUAL(state.WriteSeqNum, 7);
            UNIT_ASSERT_VALUES_EQUAL(*state.GcWriteSeqNum, 5);
            UNIT_ASSERT_VALUES_EQUAL(state.Branches.size(), 3);
            for (const auto& branch : state.Branches) {
                UNIT_ASSERT_VALUES_EQUAL(branch.SerializedResult, MakeBranchResult(branch.WriteSeqNum));
            }
            TLocksUpdate update;
            locks.SetupUpdate(&update, &db);
            locks.BreakLock(100);
            locks.ApplyLocks();
            locks.ResetUpdate();
            exec.Commit();
        }
        exec.Replay(EPlay::Boot);
        {
            TSysLocks locks(&shard);
            locks.UpdateSchema(table, keyTypes);
            exec.Begin();
            TTestLocksDb db(shard, *exec.operator->());
            UNIT_ASSERT(locks.Load(db));
            UNIT_ASSERT(locks.GetRawLock(100)->IsBroken());
            UNIT_ASSERT(!locks.RestoreLockFromSplitSrc(makeRow(50, 8), db));
            UNIT_ASSERT(locks.RestoreLockFromSplitSrc(MakeBrokenRow(), db));
            exec.Commit();
        }
        exec.Replay(EPlay::Boot);
        exec.Begin();
        TTestLocksDb db(shard, *exec.operator->());
        TVector<ILocksDb::TLockRow> rows;
        UNIT_ASSERT(db.Load(rows));
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rows[0].LockId, 100);
        UNIT_ASSERT_VALUES_EQUAL(rows[0].Counter, Max<ui64>());
        UNIT_ASSERT_VALUES_EQUAL(rows[0].LockNodeId, 1);
        UNIT_ASSERT_VALUES_EQUAL(rows[0].AncestorLocks.size(), 1);
        const auto& state = rows[0].AncestorLocks[0].WriteSeqNumStates.at(7);
        UNIT_ASSERT_VALUES_EQUAL(state.WriteSeqNum, 7);
        UNIT_ASSERT(state.Branches.empty());
        exec.Commit();
        UNIT_ASSERT(shard.PendingAncestorGc.empty());
    }

    Y_UNIT_TEST(MergeWritesOnlyNewBranchesWithinRedoBudget) {
        TDbExec exec;
        FinishUpgrade(exec);
        TTestShard shard;
        const TPathId table(1, 2);
        const TVector<NScheme::TTypeInfo> keyTypes{NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)};
        auto makeRow = [&](ui64 source) {
            ILocksDb::TLockRow row{};
            row.LockId = 100;
            row.LockNodeId = 1;
            row.WriteTables.push_back(table);
            // Each source contributes 600 KiB of results. Spread them across ancestors
            // so their union exceeds the redo budget with only ten paths per ancestor.
            for (ui64 owner = 1000; owner < 1120; ++owner) {
                auto& ancestor = row.AncestorLocks.emplace_back();
                ancestor.TabletId = ancestor.CreatedOn = owner;
                ancestor.Counter = 1;
                auto& state = ancestor.WriteSeqNumStates[7];
                state.WriterIndex = 7;
                state.WriteSeqNum = source;
                for (ui64 i = 0; i < 5; ++i) {
                    state.Branches.push_back({{owner, source, 100 + i}, source, TString(1024, 'x')});
                }
            }
            return row;
        };
        auto checkBranches = [](const TLockInfo& lock, ui64 source) {
            UNIT_ASSERT_VALUES_EQUAL(lock.GetAncestorLocks().size(), 120);
            for (ui64 owner = 1000; owner < 1120; ++owner) {
                const auto* ancestor = lock.FindAncestorLock({owner, owner});
                UNIT_ASSERT(ancestor);
                const auto& state = ancestor->WriteSeqNumStates.at(7);
                UNIT_ASSERT_VALUES_EQUAL(state.Branches.size(), source == 20 ? 5 : 10);
                UNIT_ASSERT_VALUES_EQUAL(state.WriteSeqNum, source);
            }
        };
        for (ui64 source : {20, 30}) {
            exec.Replay(EPlay::Boot);
            TSysLocks locks(&shard);
            locks.UpdateSchema(table, keyTypes);
            exec.Begin();
            TTestLocksDb db(shard, *exec.operator->());
            UNIT_ASSERT(locks.Load(db));
            UNIT_ASSERT(locks.RestoreLockFromSplitSrc(makeRow(source), db));
            UNIT_ASSERT_C(exec->GetCommitRedoBytes() < 1024 * 1024, exec->GetCommitRedoBytes());
            checkBranches(*locks.GetRawLock(100), source);
            exec.Commit();
        }
        exec.Replay(EPlay::Boot);
        TSysLocks locks(&shard);
        locks.UpdateSchema(table, keyTypes);
        exec.Begin();
        TTestLocksDb db(shard, *exec.operator->());
        UNIT_ASSERT(locks.Load(db));
        checkBranches(*locks.GetRawLock(100), 30);
        UNIT_ASSERT(locks.RestoreLockFromSplitSrc(makeRow(30), db));
        UNIT_ASSERT_VALUES_EQUAL(exec->GetCommitRedoBytes(), 0);
        exec.Commit();
    }

    Y_UNIT_TEST_TWIN(RejectedLockKeepsMetadataUntilOwnerRemoval, Restart) {
        TDbExec exec;
        FinishUpgrade(exec);
        TTestShard shard;
        const TPathId table(1, 2);
        const TVector<NScheme::TTypeInfo> keyTypes{NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)};
        TSysLocks locks(&shard);
        locks.UpdateSchema(table, keyTypes);
        exec.Begin();
        TTestLocksDb db(shard, *exec.operator->());
        ILocksDb::TLockRow row{};
        row.LockId = 100;
        row.WriteTables.push_back(table);
        for (ui64 owner : {10, 20}) {
            auto& ancestor = row.AncestorLocks.emplace_back();
            ancestor.TabletId = ancestor.CreatedOn = owner;
            ancestor.Counter = 1;
            for (ui64 writer : {7, 8}) {
                auto& state = ancestor.WriteSeqNumStates[writer];
                state.WriterIndex = writer;
                state.WriteSeqNum = 5;
                for (ui64 i = 1; i <= state.WriteSeqNum; ++i) {
                    state.Branches.push_back({{owner, 1000 * writer + i}, i, MakeBranchResult(i)});
                }
            }
        }
        UNIT_ASSERT(locks.RestoreLockFromSplitSrc(std::move(row), db));
        locks.GetRawLock(100)->SetWriteSeqNum(9, 1, &db);
        UNIT_ASSERT(locks.GetRawLock(100)->AddVolatileDependency(900, &db));
        ILocksDb::TLockRow other{};
        other.LockId = 200;
        UNIT_ASSERT(locks.RestoreLockFromSplitSrc(std::move(other), db));
        locks.RestoreConflictFromSplitSrc(100, 200, db);
        locks.RestoreConflictFromSplitSrc(200, 100, db);
        exec.Commit();

        exec.Begin();
        UNIT_ASSERT(locks.RestoreLockFromSplitSrc(MakeBrokenRow(), db));
        exec.Commit();
        if (Restart) {
            exec.Replay(EPlay::Boot);
        }
        exec.Begin();
        TTestLocksDb finalDb(shard, *exec.operator->());
        if (Restart) {
            UNIT_ASSERT(locks.Load(finalDb));
        }
        const auto lock = locks.GetRawLock(100);
        UNIT_ASSERT(lock->IsPersistent());
        UNIT_ASSERT(lock->IsBroken());
        UNIT_ASSERT_VALUES_EQUAL(lock->GetAncestorLocks().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(lock->GetWriteSeqNumStates().size(), 1);
        UNIT_ASSERT(!locks.RestoreLockFromSplitSrc(MakeTrackingRow(), finalDb));
        exec.Commit();
        exec.Begin();
        locks.RemoveSubscribedLock(100, &finalDb);
        UNIT_ASSERT(!locks.GetRawLock(100));
        UNIT_ASSERT(locks.GetRawLock(200));
        exec.Commit();
        DrainAncestorGc(exec, shard);
        exec.Begin();
        AssertEmptyState(exec);
        NIceDb::TNiceDb niceDb(*exec.operator->());
        UNIT_ASSERT(niceDb.Table<TSchema::LockRanges>().Select().EndOfSet());
        UNIT_ASSERT(niceDb.Table<TSchema::LockConflicts>().Select().EndOfSet());
        UNIT_ASSERT(niceDb.Table<TSchema::LockVolatileDependencies>().Select().EndOfSet());
        exec.Commit();
    }

    Y_UNIT_TEST_TWIN(BrokenLockSurvivesRestartAndCompaction, Compact) {
        TDbExec exec;
        FinishUpgrade(exec);
        TTestShard shard;
        {
            TSysLocks locks(&shard);
            exec.Begin();
            TTestLocksDb db(shard, *exec.operator->());
            UNIT_ASSERT(locks.RestoreLockFromSplitSrc(MakeTrackingRow(), db));
            UNIT_ASSERT(locks.RestoreLockFromSplitSrc(MakeBrokenRow(), db));
            UNIT_ASSERT(locks.GetRawLock(100)->IsBroken());
            UNIT_ASSERT_VALUES_EQUAL(locks.GetLocks().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(locks.NextPendingSubscribeLock().LockNodeId, 1);
            exec.Commit();
        }
        if (Compact) {
            exec.Snap(TSchema::Locks::TableId).Compact(TSchema::Locks::TableId);
        }
        exec.Replay(EPlay::Boot);
        {
            TSysLocks locks(&shard);
            exec.Begin();
            TTestLocksDb db(shard, *exec.operator->());
            UNIT_ASSERT(locks.Load(db));
            UNIT_ASSERT(locks.GetRawLock(100)->IsBroken());
            UNIT_ASSERT_VALUES_EQUAL(locks.NextPendingSubscribeLock().LockNodeId, 1);
            UNIT_ASSERT(!locks.RestoreLockFromSplitSrc(MakeTrackingRow(), db));
            THashMap<ui64, ILocksDb::TLockRow> migrated;
            migrated.emplace(100, MakeTrackingRow());
            locks.RestoreInMemoryLocks(std::move(migrated));
            UNIT_ASSERT(locks.GetRawLock(100)->IsBroken());
            TLocksUpdate update;
            update.LockTxId = 100;
            update.LockNodeId = 1;
            locks.SetupUpdate(&update, &db);
            UNIT_ASSERT(locks.EnsureCurrentLock() == EEnsureCurrentLock::Broken);
            UNIT_ASSERT(!update.Lock);
            locks.ResetUpdate();
            exec.Commit();
            exec.Begin();
            // The same owner notification removes it just like any other lock.
            locks.RemoveSubscribedLock(100, &db);
            UNIT_ASSERT(!locks.GetRawLock(100));
            exec.Commit();
        }
        exec.Replay(EPlay::Boot);
        exec.Begin();
        TTestLocksDb db(shard, *exec.operator->());
        TVector<ILocksDb::TLockRow> rows;
        UNIT_ASSERT(db.Load(rows));
        UNIT_ASSERT(rows.empty());
        exec.Commit();
    }

    Y_UNIT_TEST(BrokenLockLearnsOwnerFromLaterSnapshot) {
        TDbExec exec;
        FinishUpgrade(exec);
        TTestShard shard;
        {
            TSysLocks locks(&shard);
            exec.Begin();
            TTestLocksDb db(shard, *exec.operator->());
            ILocksDb::TLockRow broken{};
            broken.LockId = 100;
            broken.Counter = Max<ui64>();
            UNIT_ASSERT(locks.RestoreLockFromSplitSrc(std::move(broken), db));
            UNIT_ASSERT(!locks.NextPendingSubscribeLock());
            exec.Commit();
            exec.Begin();
            broken = {};
            broken.LockId = 100;
            broken.LockNodeId = 1;
            broken.Counter = Max<ui64>();
            UNIT_ASSERT(locks.RestoreLockFromSplitSrc(std::move(broken), db));
            UNIT_ASSERT(locks.GetRawLock(100)->IsBroken());
            UNIT_ASSERT_VALUES_EQUAL(locks.NextPendingSubscribeLock().LockNodeId, 1);
            exec.Commit();
        }
        exec.Replay(EPlay::Boot);
        exec.Begin();
        TTestLocksDb db(shard, *exec.operator->());
        TSysLocks locks(&shard);
        UNIT_ASSERT(locks.Load(db));
        UNIT_ASSERT(locks.GetRawLock(100)->IsBroken());
        UNIT_ASSERT_VALUES_EQUAL(locks.GetRawLock(100)->GetLockNodeId(), 1);
        UNIT_ASSERT_VALUES_EQUAL(locks.NextPendingSubscribeLock().LockNodeId, 1);
        exec.Commit();
    }

    Y_UNIT_TEST_TWIN(UncommittedBrokenLockSurvivesMemoryMigration, AlreadyBroken) {
        TDbExec exec;
        FinishUpgrade(exec);
        TTestShard shard;
        THashMap<ui64, ILocksDb::TLockRow> migrated;
        {
            TSysLocks locks(&shard);
            exec.Begin();
            TTestLocksDb db(shard, *exec.operator->());
            auto row = MakeTrackingRow();
            row.Flags = 0;
            UNIT_ASSERT(locks.RestoreLockFromSplitSrc(std::move(row), db));
            auto lock = locks.GetRawLock(100);
            // A no-op write has a consumed WSN but no user-table data/status.
            UNIT_ASSERT(lock->SetWriteSeqNum(7, 1, &db));
            exec.Commit();
            if (AlreadyBroken) {
                exec.Begin();
                TLocksUpdate update;
                locks.SetupUpdate(&update, &db);
                locks.BreakLock(100);
                locks.ApplyLocks();
                locks.ResetUpdate();
                exec.Commit();
            }

            exec.Begin();
            UNIT_ASSERT(locks.RestoreLockFromSplitSrc(MakeBrokenRow(), db));
            const auto broken = locks.GetRawLock(100);
            UNIT_ASSERT(!broken->IsRemoved());
            UNIT_ASSERT(broken->IsBroken());
            UNIT_ASSERT(locks.GetRemovedLocks().empty());
            // Memory migration carries the ordinary broken parent in GetLocks().
            ILocksDb::TLockRow brokenRow{};
            brokenRow.LockId = broken->GetLockId();
            brokenRow.LockNodeId = broken->GetLockNodeId();
            brokenRow.Generation = broken->GetGeneration();
            brokenRow.Counter = broken->GetRawCounter();
            brokenRow.CreateTs = broken->GetCreationTime().MicroSeconds();
            brokenRow.Flags = ui64(broken->GetFlags());
            brokenRow.BreakVersion = broken->GetBreakVersion().value_or(TRowVersion::Max());
            migrated.emplace(brokenRow.LockId, std::move(brokenRow));
            exec.Reject(); // The disk still contains the old physical lock.
        }
        exec.Replay(EPlay::Boot);
        {
            TSysLocks locks(&shard);
            exec.Begin();
            TTestLocksDb db(shard, *exec.operator->());
            UNIT_ASSERT(locks.Load(db));
            UNIT_ASSERT_VALUES_EQUAL(locks.GetRawLock(100)->IsBroken(), AlreadyBroken);
            locks.RestoreInMemoryLocks(std::move(migrated));
            while (locks.RestorePersistentState(&db)) {
                // A failed break may need persisting first, but the parent must
                // survive init until its owner ends.
                UNIT_ASSERT(locks.GetRawLock(100));
                exec.Commit();
                exec.Begin();
            }
            UNIT_ASSERT(locks.GetRawLock(100));
            UNIT_ASSERT(locks.GetRawLock(100)->IsBroken());
            UNIT_ASSERT(locks.GetRemovedLocks().empty());
            exec.Commit();
        }
        // A second reboot must preserve the now-durable rejection.
        exec.Replay(EPlay::Boot);
        {
            TSysLocks locks(&shard);
            exec.Begin();
            TTestLocksDb db(shard, *exec.operator->());
            UNIT_ASSERT(locks.Load(db));
            UNIT_ASSERT(locks.GetRawLock(100)->IsBroken());
            UNIT_ASSERT(locks.RestoreLockFromSplitSrc(MakeBrokenRow(), db));
            exec.Commit();
        }
        exec.Replay(EPlay::Boot);
        TSysLocks locks(&shard);
        exec.Begin();
        TTestLocksDb db(shard, *exec.operator->());
        UNIT_ASSERT(locks.Load(db));
        UNIT_ASSERT(locks.GetRawLock(100)->IsBroken());
        UNIT_ASSERT_VALUES_EQUAL(locks.GetLocks().size(), 1);
        TLocksUpdate update;
        update.LockTxId = 100;
        update.LockNodeId = 1;
        locks.SetupUpdate(&update, &db);
        UNIT_ASSERT(locks.EnsureCurrentLock() == EEnsureCurrentLock::Broken);
        locks.ResetUpdate();
        exec.Commit();
    }

    Y_UNIT_TEST(ShardAncestorQuotaCountsNewKeysAcrossSourcesAndRestarts) {
        TDbExec exec;
        FinishUpgrade(exec);
        TTestShard shard;
        TSysLocks locks(&shard);
        auto row = [](ui64 id, ui64 first, size_t count) {
            ILocksDb::TLockRow result{};
            result.LockId = id;
            result.LockNodeId = 1;
            for (size_t i = 0; i < count; ++i) {
                auto& ancestor = result.AncestorLocks.emplace_back();
                ancestor.TabletId = ancestor.CreatedOn = first + i;
            }
            return result;
        };
        exec.Begin();
        TTestLocksDb db(shard, *exec.operator->());
        UNIT_ASSERT(locks.RestoreLockFromSplitSrc(row(100, 1, 9999), db));
        UNIT_ASSERT(locks.RestoreLockFromSplitSrc(row(200, 10000, 1), db));
        exec.Commit();
        exec.Replay(EPlay::Boot);
        exec.Begin();
        TTestLocksDb restored(shard, *exec.operator->());
        UNIT_ASSERT(locks.Load(restored));
        UNIT_ASSERT_VALUES_EQUAL(locks.AvailableAncestorLocks(), 0);
        UNIT_ASSERT(locks.RestoreLockFromSplitSrc(row(100, 1, 1), restored));
        UNIT_ASSERT(!locks.RestoreLockFromSplitSrc(row(200, 10001, 1), restored));
        UNIT_ASSERT(locks.GetRawLock(200)->IsBroken());
        UNIT_ASSERT(!locks.GetRawLock(100)->IsBroken());
        UNIT_ASSERT_VALUES_EQUAL(locks.GetRawLock(200)->GetAncestorLocks().size(), 1);
        // The owner can remove a broken parent before its background batch.
        locks.RemoveSubscribedLock(200, &restored);
        exec.Commit();
        UNIT_ASSERT_VALUES_EQUAL(locks.AvailableAncestorLocks(), 1);
        exec.Replay(EPlay::Boot);
        shard.PendingAncestorGc.clear();
        exec.Begin();
        TTestLocksDb afterRelease(shard, *exec.operator->());
        UNIT_ASSERT(locks.Load(afterRelease));
        UNIT_ASSERT(!locks.GetRawLock(200));
        // Removing the parent releases quota, but queued GC still reserves its id.
        UNIT_ASSERT_VALUES_EQUAL(locks.AvailableAncestorLocks(), 1);
        exec.Commit();
        exec.Begin();
        UNIT_ASSERT(afterRelease.HasPendingAncestorGc(200));
        UNIT_ASSERT(!locks.RestoreLockFromSplitSrc(row(200, 10001, 1), afterRelease));
        exec.Commit();

        DrainAncestorGc(exec, shard);
        exec.Begin();
        UNIT_ASSERT_VALUES_EQUAL(locks.AvailableAncestorLocks(), 1);
        UNIT_ASSERT(locks.RestoreLockFromSplitSrc(row(200, 10001, 1), afterRelease));
        exec.Commit();
    }

    Y_UNIT_TEST(AncestorQuotaRejectionRespectsPhysicalLockLimit) {
        const auto limit = TLockLocker::LockLimit();
        TLockLocker::SetLockLimit(1);
        Y_DEFER { TLockLocker::SetLockLimit(limit); };

        TDbExec exec;
        FinishUpgrade(exec);
        TTestShard shard;
        TSysLocks locks(&shard);
        exec.Begin();
        TTestLocksDb db(shard, *exec.operator->());
        ILocksDb::TLockRow row{};
        row.LockId = 100;
        UNIT_ASSERT(locks.RestoreLockFromSplitSrc(std::move(row), db));

        TLocksUpdate newLock;
        newLock.LockTxId = 200;
        locks.SetupUpdate(&newLock, &db);
        UNIT_ASSERT(!locks.RejectAncestorLimit());
        locks.ResetUpdate();
        UNIT_ASSERT(!locks.GetRawLock(200));

        TLocksUpdate existing;
        existing.LockTxId = 100;
        locks.SetupUpdate(&existing, &db);
        UNIT_ASSERT(locks.RejectAncestorLimit());
        locks.ResetUpdate();
        UNIT_ASSERT(locks.GetRawLock(100)->IsBroken());
        exec.Commit();

        exec.Replay(EPlay::Boot);
        exec.Begin();
        TTestLocksDb restored(shard, *exec.operator->());
        UNIT_ASSERT(locks.Load(restored));
        UNIT_ASSERT_VALUES_EQUAL(locks.GetLocks().size(), 1);
        UNIT_ASSERT(locks.GetRawLock(100)->IsBroken());
        exec.Commit();
    }

    Y_UNIT_TEST(MergeExceedingAncestorPathLimitKeepsBrokenLockUntilRemoval) {
        TDbExec exec;
        FinishUpgrade(exec);
        TTestShard shard;
        TSysLocks locks(&shard);
        exec.Begin();
        TTestLocksDb db(shard, *exec.operator->());
        auto row = [](ui64 first, ui64 last) {
            ILocksDb::TLockRow result{};
            result.LockId = 100;
            result.LockNodeId = 1;
            auto& ancestor = result.AncestorLocks.emplace_back();
            ancestor.TabletId = ancestor.CreatedOn = 10;
            auto& state = ancestor.WriteSeqNumStates[7];
            state.WriterIndex = 7;
            state.WriteSeqNum = 1;
            for (ui64 next = first; next <= last; ++next) {
                state.Branches.push_back({{10, next}, 1, {}});
            }
            return result;
        };
        constexpr ui64 firstPath = 100;
        constexpr ui64 sharedPath = firstPath + TAncestorLock::MaxPaths / 2;
        constexpr ui64 lastPath = firstPath + TAncestorLock::MaxPaths - 1;
        UNIT_ASSERT(locks.RestoreLockFromSplitSrc(row(firstPath, sharedPath), db));
        UNIT_ASSERT(locks.RestoreLockFromSplitSrc(row(sharedPath, lastPath), db));
        UNIT_ASSERT(locks.RestoreLockFromSplitSrc(row(firstPath, lastPath), db));
        UNIT_ASSERT(!locks.RestoreLockFromSplitSrc(row(lastPath, lastPath + 1), db));
        UNIT_ASSERT(locks.GetRawLock(100)->IsBroken());
        exec.Commit();
        exec.Replay(EPlay::Boot);
        shard.PendingAncestorGc.clear();
        exec.Begin();
        TTestLocksDb restored(shard, *exec.operator->());
        UNIT_ASSERT(locks.Load(restored));
        const auto lock = locks.GetRawLock(100);
        UNIT_ASSERT(lock && lock->IsBroken());
        const auto* ancestor = lock->FindAncestorLock({10, 10});
        UNIT_ASSERT(ancestor);
        UNIT_ASSERT(ancestor->WriteSeqNumStates.at(7).Branches.empty());
        UNIT_ASSERT(!locks.RestoreLockFromSplitSrc(row(firstPath, firstPath), restored));
        exec.Commit();

        UNIT_ASSERT(shard.PendingAncestorGc.empty());

        exec.Begin();
        locks.RemoveSubscribedLock(100, &restored);
        exec.Commit();
        UNIT_ASSERT(shard.PendingAncestorGc.contains({100, std::nullopt}));
        DrainAncestorGc(exec, shard);
        exec.Replay(EPlay::Boot);
        exec.Begin();
        TTestLocksDb afterCleanup(shard, *exec.operator->());
        UNIT_ASSERT(locks.Load(afterCleanup));
        UNIT_ASSERT(!locks.GetRawLock(100));
        AssertEmptyState(exec);
        exec.Commit();
    }

    Y_UNIT_TEST(BranchGcPersistsBoundAndPreservesAggregates) {
        TDbExec exec;
        FinishUpgrade(exec);
        TTestShard shard;
        exec.Begin();
        TTestLocksDb db(shard, *exec.operator->());
        ILocksDb::TLockRow row{};
        row.LockId = 100;
        row.Flags = ui64(ELockFlags::Persistent);
        db.PersistAddLock(100, 1, 1, 123, 0);
        TLockInfo::TPtr lock = new TLockInfo(nullptr, row);
        TAncestorLock ancestor;
        ancestor.TabletId = ancestor.CreatedOn = 10;
        const auto key = ancestor.GetKey();
        lock->PersistAncestorLock(std::move(ancestor), db);
        lock->SetAncestorBranch(key, 7, {{10, 20}, 8, "left"}, &db);
        lock->SetAncestorBranch(key, 7, {{10, 30}, 9, "right"}, &db);
        lock->SetWriteSeqNum(7, 5, &db);
        lock->SetWriteSeqNumResult(7, "current", &db);
        UNIT_ASSERT(lock->SetWriteSeqNumWithGc(7, std::nullopt, 3, db));
        UNIT_ASSERT(!lock->SetWriteSeqNumWithGc(7, std::nullopt, 3, db));
        UNIT_ASSERT(lock->SetWriteSeqNumWithGc(7, std::nullopt, 4, db));
        exec.Commit();
        exec.Replay(EPlay::Boot);

        exec.Begin();
        TTestLocksDb restoredDb(shard, *exec.operator->());
        TVector<ILocksDb::TLockRow> rows;
        UNIT_ASSERT(restoredDb.Load(rows));
        const auto& current = rows.at(0).WriteSeqNumStates.at(0);
        UNIT_ASSERT_VALUES_EQUAL(current.GcWriteSeqNum.value(), 4);
        UNIT_ASSERT_VALUES_EQUAL(current.WriteSeqNum, 5);
        UNIT_ASSERT_VALUES_EQUAL(current.SerializedResult, "current");
        const auto& restored = rows.at(0).AncestorLocks.at(0);
        UNIT_ASSERT(restored.GetKey() == key);
        UNIT_ASSERT_VALUES_EQUAL(restored.WriteSeqNumStates.at(7).WriteSeqNum, 9);
        UNIT_ASSERT(restored.WriteSeqNumStates.at(7).Branches.empty());
        exec.Commit();
    }

    Y_UNIT_TEST_TWIN(ColdBootRecoversAncestorGcUnderTxMemoryLimit, WithTombstones) {
        NTabletFlatExecutor::TMyEnvBase env;
        auto& appData = env->GetAppData();
        appData.ResourceProfiles = new TResourceProfiles;
        TResourceProfiles::TResourceProfile profile;
        profile.SetTabletType(NKikimrTabletBase::TTabletTypes::Unknown);
        profile.SetName("branch-gc");
        profile.SetInitialTxMemory(0);
        profile.SetTxMemoryLimit(512 * 1024);
        appData.ResourceProfiles->AddProfile(profile);

        const auto flags = ui32(NFake::TDummy::EFlg::Comp);
        env.FireDummyTablet(flags);
        auto run = [&](std::function<bool(NTable::TDatabase&)> action) {
            env.SendSync(new NFake::TEvExecute(new TTxLocksSchemaTest(std::move(action))));
        };
        run(Upgrade);
        TTestShard shard;
        using TBranches = TSchema::AncestorLockBranches;
        constexpr ui64 rowCount = 40;
        const auto makePath = [](ui64 row) {
            TVector<ui64> path{10, 100 + row};
            for (ui64 hop = 0; hop < 4096; ++hop) {
                path.push_back((ui64(1) << 55) + hop);
            }
            return path;
        };

        // The parent was removed before reboot. Its ancestor row is still
        // present to recover all branch keys without precharging their pages.
        run([&](NTable::TDatabase& database) {
            TTestLocksDb db(shard, database);
            TAncestorLock ancestor;
            ancestor.TabletId = ancestor.CreatedOn = 10;
            db.PersistAddAncestorLock(100, ancestor);
            db.PersistAncestorLockWriteSeqNum(100, ancestor.GetKey(), 0, 1, {});
            db.PersistAddLock(101, 1, 1, 123, 0);
            db.PersistAddAncestorLock(101, ancestor);
            db.PersistAncestorLockWriteSeqNum(101, ancestor.GetKey(), 0, 1, {});
            db.PersistAncestorLockBranch(101, ancestor.GetKey(), 0, {{10}, 1, "neighbor"});
            return true;
        });
        for (ui64 row = 1; row <= rowCount; ++row) {
            run([&](NTable::TDatabase& database) {
                TTestLocksDb db(shard, database);
                db.PersistAncestorLockBranch(100, {10, 10}, 0, {makePath(row), 1, TString(64 * 1024, 'x')});
                return true;
            });
        }
        env.SendSync(new NFake::TEvCompact(TBranches::TableId));
        env.WaitFor<NFake::TEvCompacted>();
        if (WithTombstones) {
            // The last live branch follows a series of cold tombstones in the same range.
            for (ui64 row = 1; row < rowCount; ++row) {
                run([&](NTable::TDatabase& database) {
                    NIceDb::TNiceDb(database).Table<TBranches>()
                        .Key(100, 10, 10, 0, SerializeLockShardsPath(makePath(row))).Delete();
                    return true;
                });
            }
        }
        run([&](NTable::TDatabase& database) {
            database.Alter().SetExecutorResourceProfile("branch-gc");
            return true;
        });
        env.RestartTablet(flags);
        TSysLocks locks(&shard);
        run([&](NTable::TDatabase& database) {
            TTestLocksDb db(shard, database);
            return locks.Load(db);
        });
        const ILocksDb::TAncestorGcTask task{100, std::nullopt};
        UNIT_ASSERT_VALUES_EQUAL(shard.PendingAncestorGc.size(), 1);
        UNIT_ASSERT(shard.PendingAncestorGc.contains(task));
        UNIT_ASSERT(locks.GetRawLock(101));

        using EResult = TDataShardLocksDb::EAncestorGcResult;
        TDataShardLocksDb::TAncestorGcCursor cursor;
        EResult result;
        size_t batches = 0;
        do {
            run([&](NTable::TDatabase& database) {
                size_t remaining = TDataShardLocksDb::AncestorGcScanBudget;
                result = TDataShardLocksDb::RemoveAncestorGcBatch(database, task, cursor, remaining);
                if (WithTombstones && batches == 0 && result != EResult::NotReady) {
                    UNIT_ASSERT(result == EResult::More && !cursor.LastKey.empty());
                    UNIT_ASSERT(!database.HasChanges());
                }
                return result != EResult::NotReady;
            });
            ++batches;
        } while (result != EResult::Done);
        UNIT_ASSERT_GT(batches, 1u);

        run([&](NTable::TDatabase& database) {
            NIceDb::TNiceDb db(database);
            auto deleted = db.Table<TBranches>().Key(100, 10, 10, 0, SerializeLockShardsPath(makePath(rowCount))).Select();
            const std::array<ui64, 1> neighborPath{10};
            auto neighbor = db.Table<TBranches>().Key(101, 10, 10, 0, SerializeLockShardsPath(neighborPath)).Select();
            if (!deleted.IsReady() || !neighbor.IsReady()) {
                return false;
            }
            UNIT_ASSERT(deleted.EndOfSet());
            UNIT_ASSERT(!neighbor.EndOfSet());
            UNIT_ASSERT_VALUES_EQUAL(neighbor.GetValue<TBranches::WriteResult>(), "neighbor");
            return true;
        });
    }

    Y_UNIT_TEST(LargeBranchGcStaysWithinRedoLimitAfterRestart) {
        constexpr size_t redoLimit = 1024 * 1024;
        TDbExec exec;
        FinishUpgrade(exec);
        TTestShard shard;
        ILocksDb::TLockRow row{};
        row.LockId = 100;
        row.Flags = ui64(ELockFlags::Persistent);
        TLockInfo::TPtr lock = new TLockInfo(nullptr, row);
        const TLockKey key{10, 10};
        const ILocksDb::TAncestorGcTask writerTask{100, std::tuple<ui64, ui64, ui64>{10, 10, 0}};
        const ILocksDb::TAncestorGcTask fullTask{100, std::nullopt};
        using EResult = TDataShardLocksDb::EAncestorGcResult;
        using EStage = TDataShardLocksDb::EAncestorGcStage;

        exec.Begin();
        TTestLocksDb db(shard, *exec.operator->());
        db.PersistAddLock(100, 1, 1, 123, 0);
        TAncestorLock ancestor;
        ancestor.TabletId = ancestor.CreatedOn = 10;
        lock->PersistAncestorLock(std::move(ancestor), db);
        lock->SetAncestorBranch(key, 7, {{10, 50}, 1, "other writer"}, &db);
        exec.Commit();

        // Each key fits on its own. The shorter key sorts first and leaves
        // less than the redo required by the second key in the same batch.
        size_t totalKeyBytes = 0;
        TString previousKey;
        ui64 child = 100;
        for (ui64 hops : {28 * 1024, 115 * 1024 + 1}) {
            exec.Begin();
            TVector<ui64> path{10, child++};
            for (ui64 hop = 0; hop < hops; ++hop) {
                path.push_back((ui64(1) << 55) + hop);
            }
            auto keyBytes = SerializeLockShardsPath(path);
            UNIT_ASSERT(previousKey < keyBytes);
            totalKeyBytes += keyBytes.size();
            previousKey = std::move(keyBytes);
            lock->SetAncestorBranch(key, 0, {std::move(path), 9, {}}, &db);
            UNIT_ASSERT_LT(exec->GetCommitRedoBytes(), redoLimit);
            exec.Commit();
        }
        UNIT_ASSERT_GT(totalKeyBytes, redoLimit);
        exec.Begin();
        UNIT_ASSERT(lock->SetWriteSeqNumWithGc(0, 1, 1, db));
        const auto& gcState = lock->FindAncestorLock(key)->WriteSeqNumStates.at(0);
        UNIT_ASSERT_VALUES_EQUAL(gcState.Branches.capacity(), 0);
        UNIT_ASSERT_VALUES_EQUAL(gcState.WriteSeqNum, 9);
        UNIT_ASSERT_LT(exec->GetCommitRedoBytes(), redoLimit);
        exec.Commit();
        UNIT_ASSERT(shard.PendingAncestorGc.contains(writerTask));

        // Load recovers the writer task from its bound without reading closed branches.
        exec.Replay(EPlay::Boot);
        shard.PendingAncestorGc.clear();
        exec.Begin();
        TVector<ILocksDb::TLockRow> rows;
        TTestLocksDb restoredDb(shard, *exec.operator->());
        UNIT_ASSERT(restoredDb.Load(rows));
        UNIT_ASSERT_VALUES_EQUAL(rows.at(0).WriteSeqNumStates.at(0).GcWriteSeqNum.value(), 1);
        const auto& ancestorStates = rows.at(0).AncestorLocks.at(0).WriteSeqNumStates;
        const auto& restored = ancestorStates.at(0);
        UNIT_ASSERT(restored.Branches.empty());
        UNIT_ASSERT_VALUES_EQUAL(restored.WriteSeqNum, 9);
        UNIT_ASSERT_VALUES_EQUAL(ancestorStates.at(7).Branches.size(), 1);
        exec.Commit();
        TSysLocks restoredLocks(&shard);
        restoredLocks.Load(std::move(rows));
        UNIT_ASSERT_VALUES_EQUAL(shard.PendingAncestorGc.size(), 1);
        UNIT_ASSERT(shard.PendingAncestorGc.contains(writerTask));

        exec.Begin();
        TDataShardLocksDb::TAncestorGcCursor writerCursor;
        size_t writerBudget = TDataShardLocksDb::AncestorGcScanBudget;
        UNIT_ASSERT(TDataShardLocksDb::RemoveAncestorGcBatch(*exec.operator->(), writerTask, writerCursor, writerBudget) == EResult::More);
        UNIT_ASSERT_LT(exec->GetCommitRedoBytes(), redoLimit);
        exec.Commit();

        // Removal queues the full lock even while the narrow task is in progress.
        exec.Begin();
        restoredLocks.RemoveSubscribedLock(100, &restoredDb);
        exec.Commit();
        UNIT_ASSERT_VALUES_EQUAL(shard.PendingAncestorGc.size(), 2);
        UNIT_ASSERT(shard.PendingAncestorGc.contains(fullTask));
        exec.Replay(EPlay::Boot);
        shard.PendingAncestorGc.clear();
        exec.Begin();
        TTestLocksDb orphanDb(shard, *exec.operator->());
        UNIT_ASSERT(orphanDb.Load(rows));
        UNIT_ASSERT(rows.empty());
        exec.Commit();
        UNIT_ASSERT_VALUES_EQUAL(shard.PendingAncestorGc.size(), 1);
        UNIT_ASSERT(shard.PendingAncestorGc.contains(fullTask));
        UNIT_ASSERT(orphanDb.HasPendingAncestorGc(100));

        // Stop the full task just after the branch range is checked. Its
        // metadata must still be sufficient to recover it on the next boot.
        TDataShardLocksDb::TAncestorGcCursor fullCursor;
        while (fullCursor.Stage == EStage::Branches) {
            exec.Begin();
            size_t remaining = 1;
            UNIT_ASSERT(TDataShardLocksDb::RemoveAncestorGcBatch(*exec.operator->(), fullTask, fullCursor, remaining) == EResult::More);
            UNIT_ASSERT_LT(exec->GetCommitRedoBytes(), redoLimit);
            exec.Commit();
        }
        UNIT_ASSERT(fullCursor.Stage == EStage::WriterStates);
        exec.Replay(EPlay::Boot);
        shard.PendingAncestorGc.clear();
        exec.Begin();
        TTestLocksDb afterBranchGc(shard, *exec.operator->());
        UNIT_ASSERT(afterBranchGc.Load(rows));
        UNIT_ASSERT(rows.empty());
        exec.Commit();
        UNIT_ASSERT(shard.PendingAncestorGc.contains(fullTask));

        DrainAncestorGc(exec, shard);
        exec.Begin();
        AssertEmptyState(exec);
        exec.Commit();
    }

    Y_UNIT_TEST(AncestorGcSharesScanBudgetAcrossTasks) {
        TDbExec exec;
        FinishUpgrade(exec);
        TTestShard shard;
        exec.Begin();
        TTestLocksDb db(shard, *exec.operator->());
        TAncestorLock ancestor;
        ancestor.TabletId = ancestor.CreatedOn = 10;
        for (ui64 id : {100, 200}) {
            db.PersistAddLock(id, 1, 1, 123, 0);
            db.PersistAddAncestorLock(id, ancestor);
            db.PersistAncestorLockWriteSeqNum(id, ancestor.GetKey(), 0, 1, {});
            db.PersistAncestorLockBranch(id, ancestor.GetKey(), 0, {{10}, 1, {}});
        }
        exec.Commit();

        using EResult = TDataShardLocksDb::EAncestorGcResult;
        const auto writer = std::tuple<ui64, ui64, ui64>{10, 10, 0};
        TDataShardLocksDb::TAncestorGcCursor first;
        TDataShardLocksDb::TAncestorGcCursor second;
        exec.Begin();
        size_t remaining = 3;
        UNIT_ASSERT(TDataShardLocksDb::RemoveAncestorGcBatch(*exec.operator->(), {100, writer}, first, remaining) == EResult::Done);
        UNIT_ASSERT(TDataShardLocksDb::RemoveAncestorGcBatch(*exec.operator->(), {200, writer}, second, remaining) == EResult::More);
        UNIT_ASSERT_VALUES_EQUAL(remaining, 0);
        exec.Commit();

        exec.Begin();
        remaining = TDataShardLocksDb::AncestorGcScanBudget;
        UNIT_ASSERT(TDataShardLocksDb::RemoveAncestorGcBatch(*exec.operator->(), {200, writer}, second, remaining) == EResult::Done);
        NIceDb::TNiceDb niceDb(*exec.operator->());
        UNIT_ASSERT(niceDb.Table<TSchema::AncestorLockBranches>().Select().EndOfSet());
        UNIT_ASSERT(!niceDb.Table<TSchema::AncestorLockWriteSeqNums>().Key(100, 10, 10, 0).Select().EndOfSet());
        UNIT_ASSERT(!niceDb.Table<TSchema::AncestorLockWriteSeqNums>().Key(200, 10, 10, 0).Select().EndOfSet());
        exec.Commit();
    }

    Y_UNIT_TEST(RetiringLargeAncestorLockDefersMetadataRedo) {
        TDbExec exec;
        FinishUpgrade(exec);
        TTestShard shard;
        exec.Begin();
        TTestLocksDb db(shard, *exec.operator->());
        ILocksDb::TLockRow row{};
        row.LockId = 100;
        row.LockNodeId = 1;
        TSysLocks locks(&shard);
        UNIT_ASSERT(locks.RestoreLockFromSplitSrc(std::move(row), db));
        exec.Commit();

        // Model a large lock already on disk. Each ancestor has a writer row.
        constexpr ui64 count = 8000;
        for (ui64 offset = 1; offset <= count; offset += 100) {
            exec.Begin();
            for (ui64 id = offset; id < offset + 100; ++id) {
                TAncestorLock ancestor;
                ancestor.TabletId = ancestor.CreatedOn = id;
                db.PersistAddAncestorLock(100, ancestor);
                db.PersistAncestorLockWriteSeqNum(100, ancestor.GetKey(), 7, 1, {});
            }
            exec.Commit();
        }
        exec.Replay(EPlay::Boot);
        exec.Begin();
        TTestLocksDb restored(shard, *exec.operator->());
        UNIT_ASSERT(locks.Load(restored));
        UNIT_ASSERT_VALUES_EQUAL(locks.GetRawLock(100)->GetAncestorLocks().size(), count);
        locks.RemoveSubscribedLock(100, &restored);
        UNIT_ASSERT(!locks.GetRawLock(100));
        UNIT_ASSERT_LT(exec->GetCommitRedoBytes(), 128 * 1024u);
        exec.Commit();
        UNIT_ASSERT_VALUES_EQUAL(shard.PendingAncestorGc.size(), 1);
        const ILocksDb::TAncestorGcTask task{100, std::nullopt};
        UNIT_ASSERT(shard.PendingAncestorGc.contains(task));

        // Commit part of the writer metadata cleanup. The ancestor rows
        // must still restore a single task after reboot.
        exec.Begin();
        TDataShardLocksDb::TAncestorGcCursor cursor;
        size_t remaining = TDataShardLocksDb::AncestorGcScanBudget;
        UNIT_ASSERT(TDataShardLocksDb::RemoveAncestorGcBatch(*exec.operator->(), task, cursor, remaining)
            == TDataShardLocksDb::EAncestorGcResult::More);
        UNIT_ASSERT(cursor.Stage == TDataShardLocksDb::EAncestorGcStage::WriterStates);
        exec.Commit();
        exec.Replay(EPlay::Boot);
        shard.PendingAncestorGc.clear();
        exec.Begin();
        TTestLocksDb afterMetadataGc(shard, *exec.operator->());
        UNIT_ASSERT(locks.Load(afterMetadataGc));
        UNIT_ASSERT(!locks.GetRawLock(100));
        exec.Commit();
        UNIT_ASSERT_VALUES_EQUAL(shard.PendingAncestorGc.size(), 1);
        UNIT_ASSERT(shard.PendingAncestorGc.contains(task));
    }

    Y_UNIT_TEST(BranchUpdatesRetainPrefixesAndResultsAfterRestart) {
        TDbExec exec;
        FinishUpgrade(exec);
        TTestShard shard;
        exec.Begin();
        TTestLocksDb db(shard, *exec.operator->());
        db.PersistAddLock(100, 1, 1, 123, 0);
        TAncestorLock ancestor;
        ancestor.TabletId = 10;
        ancestor.CreatedOn = 10;
        ancestor.Generation = 1;
        ancestor.Counter = 123;
        db.PersistAddAncestorLock(100, ancestor);
        db.PersistAncestorLockWriteSeqNum(100, ancestor.GetKey(), 7, 10, {});
        db.PersistAncestorLockBranch(100, ancestor.GetKey(), 7, {{10}, 10, MakeBranchResult(10)});
        exec.Commit();
        exec.Replay(EPlay::Boot);

        exec.Begin();
        TTestLocksDb updateDb(shard, *exec.operator->());
        TVector<ILocksDb::TLockRow> rows;
        UNIT_ASSERT(updateDb.Load(rows));
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
        rows[0].Flags |= ui64(ELockFlags::Persistent);
        TLockInfo::TPtr lock = new TLockInfo(nullptr, rows[0]);
        for (auto& item : rows[0].AncestorLocks) {
            lock->AddAncestorLock(std::move(item));
        }
        TLocksUpdate::TWriteSeqNumUpdate update(7);
        const TPathId table(1, 2);
        for (ui64 child : {20, 30}) {
            UNIT_ASSERT(update.StageOperation(lock.Get(), 40, TVector<ui64>{10, child}, 11, table).Status
                == TLocksUpdate::TWriteSeqNumUpdate::EStatus::Fresh);
        }
        for (const auto& item : update.Branches) {
            lock->SetAncestorBranch(item.Key, 7, item.Branch, &updateDb);
            lock->SetAncestorBranchResult(item.Key, 7, item.Branch.ShardsPath, 11, MakeBranchResult(11), &updateDb);
        }
        exec.Commit();
        exec.Replay(EPlay::Boot);

        exec.Begin();
        TTestLocksDb restoredDb(shard, *exec.operator->());
        rows.clear();
        UNIT_ASSERT(restoredDb.Load(rows));
        TLockInfo::TPtr restored = new TLockInfo(nullptr, rows.at(0));
        for (auto& item : rows[0].AncestorLocks) {
            restored->AddAncestorLock(std::move(item));
        }
        const auto& state = restored->FindAncestorLock({10, 10})->WriteSeqNumStates.at(7);
        UNIT_ASSERT_VALUES_EQUAL(state.WriteSeqNum, 11);
        UNIT_ASSERT_VALUES_EQUAL(state.Branches.size(), 3);
        TLocksUpdate::TWriteSeqNumUpdate retry(7);
        for (ui64 child : {20, 30}) {
            const auto result = retry.StageOperation(restored.Get(), 40, TVector<ui64>{10, child}, 11, table);
            UNIT_ASSERT(result.Status == TLocksUpdate::TWriteSeqNumUpdate::EStatus::Duplicate);
            UNIT_ASSERT_VALUES_EQUAL(result.SerializedResult, MakeBranchResult(11));
        }
        UNIT_ASSERT(retry.StageOperation(restored.Get(), 40, TVector<ui64>{10, 50}, 10, table).Status
            == TLocksUpdate::TWriteSeqNumUpdate::EStatus::Duplicate);
        UNIT_ASSERT(retry.Branches.empty());
        exec.Commit();
    }

    Y_UNIT_TEST(UpgradeUsesNewTableIdsAndRollsBackAtomically) {
        TDbExec exec;
        exec.Begin();
        CreateLegacy(*exec.operator->());
        exec.Commit();

        exec.Begin();
        UNIT_ASSERT(Upgrade(*exec.operator->()));
        exec.Reject();
        UNIT_ASSERT(exec->GetScheme().GetTableInfo(TLegacySchema::AncestorShardsLocks::TableId));
        UNIT_ASSERT(exec->GetScheme().GetTableInfo(TLegacySchema::AncestorLockWriteSeqNums::TableId));
        UNIT_ASSERT(!exec->GetScheme().GetTableInfo(TSchema::AncestorShardsLocks::TableId));
        UNIT_ASSERT(!exec->GetScheme().GetTableInfo(TSchema::AncestorLockWriteSeqNums::TableId));
        exec.Begin();
        {
            NIceDb::TNiceDb db(*exec.operator->());
            auto current = db.Table<TLegacySchema::LockWriteSeqNums>().Key(100, 7).Select();
            UNIT_ASSERT_VALUES_EQUAL(current.GetValue<TLegacySchema::LockWriteSeqNums::WriteSeqNum>(), 8);
            UNIT_ASSERT(!db.Table<TLegacySchema::AncestorShardsLocks>().Select().EndOfSet());
            UNIT_ASSERT(!db.Table<TLegacySchema::AncestorLockWriteSeqNums>().Select().EndOfSet());
            UNIT_ASSERT(db.Table<TSchema::Sys>().Key(TSchema::Sys_LocksFormatVersion).Select().EndOfSet());
        }
        exec.Commit();

        FinishUpgrade(exec);
        UNIT_ASSERT(!exec->GetScheme().GetTableInfo(TLegacySchema::AncestorShardsLocks::TableId));
        UNIT_ASSERT(!exec->GetScheme().GetTableInfo(TLegacySchema::AncestorLockWriteSeqNums::TableId));
        const auto* locks = exec->GetScheme().GetTableInfo(TSchema::AncestorShardsLocks::TableId);
        const auto* writers = exec->GetScheme().GetTableInfo(TSchema::AncestorLockWriteSeqNums::TableId);
        UNIT_ASSERT(locks && writers);
        UNIT_ASSERT(locks->KeyColumns == TVector<ui32>({1, 2, 7}));
        UNIT_ASSERT(writers->KeyColumns == TVector<ui32>({1, 2, 6, 3}));
        exec.Begin();
        AssertEmptyState(exec);
        exec.Commit();
    }

    void CheckUpgradeSurvivesExecutorBoot(bool compact) {
        NTabletFlatExecutor::TMyEnvBase env;
        env.FireDummyTablet(compact ? ui32(NFake::TDummy::EFlg::Comp) : 0);
        auto run = [&](std::function<bool(NTable::TDatabase&)> action) {
            env.SendSync(new NFake::TEvExecute(new TTxLocksSchemaTest(std::move(action))));
        };
        run([](NTable::TDatabase& database) {
            CreateLegacy(database);
            return true;
        });
        if (compact) {
            for (ui32 table : {TSchema::LockWriteSeqNums::TableId,
                    TLegacySchema::AncestorShardsLocks::TableId,
                    TLegacySchema::AncestorLockWriteSeqNums::TableId}) {
                env.SendSync(new NFake::TEvCompact(table));
                env.WaitFor<NFake::TEvCompacted>();
            }
        }
        env.RestartTablet();
        run(Upgrade);
        env.RestartTablet();
        run([](NTable::TDatabase& database) {
            NIceDb::TNiceDb db(database);
            auto locks = db.Table<TSchema::AncestorShardsLocks>().Select();
            auto writers = db.Table<TSchema::AncestorLockWriteSeqNums>().Select();
            auto current = db.Table<TSchema::LockWriteSeqNums>().Select();
            auto ordinaryLock = db.Table<TSchema::Locks>().Key(200).Select();
            if (!locks.IsReady() || !writers.IsReady() || !current.IsReady() || !ordinaryLock.IsReady()) {
                return false;
            }
            UNIT_ASSERT(locks.EndOfSet());
            UNIT_ASSERT(writers.EndOfSet());
            UNIT_ASSERT(current.EndOfSet());
            UNIT_ASSERT(!database.GetScheme().GetTableInfo(TLegacySchema::AncestorShardsLocks::TableId));
            UNIT_ASSERT(!database.GetScheme().GetTableInfo(TLegacySchema::AncestorLockWriteSeqNums::TableId));
            UNIT_ASSERT(database.GetScheme().GetTableInfo(TSchema::AncestorShardsLocks::TableId)->KeyColumns == TVector<ui32>({1, 2, 7}));
            UNIT_ASSERT(database.GetScheme().GetTableInfo(TSchema::AncestorLockWriteSeqNums::TableId)->KeyColumns == TVector<ui32>({1, 2, 6, 3}));
            UNIT_ASSERT_VALUES_EQUAL(ordinaryLock.GetValue<TSchema::Locks::Generation>(), 3);
            UNIT_ASSERT_VALUES_EQUAL(ordinaryLock.GetValue<TSchema::Locks::Counter>(), 123);
            UNIT_ASSERT(database.HasOpenTx(TLegacySchema::UserData::TableId, 200));
            return true;
        });
        run([](NTable::TDatabase& database) {
            database.CommitTx(TLegacySchema::UserData::TableId, 200);
            return true;
        });
        run([](NTable::TDatabase& database) {
            auto row = NIceDb::TNiceDb(database).Table<TLegacySchema::UserData>().Key(1).Select();
            if (!row.IsReady()) {
                return false;
            }
            UNIT_ASSERT(!row.EndOfSet());
            UNIT_ASSERT_VALUES_EQUAL(row.GetValue<TLegacySchema::UserData::Value>(), 42);
            return true;
        });
    }

    Y_UNIT_TEST(UpgradeSurvivesExecutorBoot) {
        CheckUpgradeSurvivesExecutorBoot(false);
    }

    Y_UNIT_TEST(UpgradeCompactedTablesSurvivesExecutorBoot) {
        CheckUpgradeSurvivesExecutorBoot(true);
    }

    Y_UNIT_TEST(FreshShardCreatesCurrentFormat) {
        TDbExec exec;
        FinishUpgrade(exec);
        exec.Begin();
        NIceDb::TNiceDb db(*exec.operator->());
        auto version = db.Table<TSchema::Sys>().Key(TSchema::Sys_LocksFormatVersion).Select();
        UNIT_ASSERT_VALUES_EQUAL(version.GetValue<TSchema::Sys::Uint64>(), ui64(ELocksFormatVersion::Current));
        AssertEmptyState(exec);
        exec.Commit();
    }

    Y_UNIT_TEST(LegacySchemaCanBeMaterializedWithEmptyFeatureTables) {
        NTabletFlatExecutor::TMyEnvBase env;
        env.FireDummyTablet();
        auto run = [&](std::function<bool(NTable::TDatabase&)> action) {
            env.SendSync(new NFake::TEvExecute(new TTxLocksSchemaTest(std::move(action))));
        };
        run([](NTable::TDatabase& database) {
            NIceDb::TNiceDb(database).Materialize<TLegacySchema>();
            return true;
        });
        env.RestartTablet();
        run(Upgrade);
        env.RestartTablet();
        // Emulate an old binary's schema initialization with no feature state.
        run([](NTable::TDatabase& database) {
            NIceDb::TNiceDb(database).Materialize<TLegacySchema>();
            return true;
        });
        env.RestartTablet();
        run(Upgrade);
        env.RestartTablet();
        run([](NTable::TDatabase& database) {
            NIceDb::TNiceDb db(database);
            auto locks = db.Table<TSchema::AncestorShardsLocks>().Select();
            auto writers = db.Table<TSchema::AncestorLockWriteSeqNums>().Select();
            if (!locks.IsReady() || !writers.IsReady()) {
                return false;
            }
            UNIT_ASSERT(locks.EndOfSet() && writers.EndOfSet());
            UNIT_ASSERT(!database.GetScheme().GetTableInfo(TLegacySchema::AncestorShardsLocks::TableId));
            UNIT_ASSERT(!database.GetScheme().GetTableInfo(TLegacySchema::AncestorLockWriteSeqNums::TableId));
            return true;
        });
    }

    Y_UNIT_TEST(LogicalLocksBranchesAndGcSurviveRestartAndPhysicalRemoval) {
        TDbExec exec;
        FinishUpgrade(exec);
        TTestShard shard;

        exec.Begin();
        TTestLocksDb db(shard, *exec.operator->());
        db.PersistAddLock(100, 1, 3, 123, 0);
        db.PersistLockWriteSeqNum(100, 7, 19, "current-result", 15);
        db.PersistLockWriteSeqNum(100, 8, 2, "other-writer");
        // Writer 7 has closed its ancestor branches. Writer 8 still has live
        // branches with the same owner and independent creation sites.
        for (ui64 createdOn : {10, 30}) {
            TAncestorLock ancestor;
            ancestor.TabletId = 10;
            ancestor.CreatedOn = createdOn;
            ancestor.Generation = 2;
            ancestor.Counter = createdOn + 1;
            const ui64 begin = createdOn;
            const ui64 end = createdOn + 10;
            ancestor.CreatedOnRange.emplace(TVector<TCell>{TCell::Make(begin)}, true,
                TVector<TCell>{TCell::Make(end)}, false);
            db.PersistAddAncestorLock(100, ancestor);
            db.PersistAncestorLockWriteSeqNum(100, ancestor.GetKey(), 8, 11, {}, createdOn == 10 ? std::optional<ui64>(5) : std::nullopt);
            const ui64 nextHop = createdOn == 10 ? 20 : createdOn;
            db.PersistAncestorLockBranch(100, ancestor.GetKey(), 8, {{10, nextHop}, 10, MakeBranchResult(10)});
            db.PersistAncestorLockBranch(100, ancestor.GetKey(), 8, {{10, nextHop, 40}, 11, MakeBranchResult(11)});
        }
        exec.Commit();
        exec.Replay(EPlay::Boot);
        // Initialization of an already upgraded shard must preserve new rows.
        FinishUpgrade(exec);
        exec.Replay(EPlay::Boot);

        exec.Begin();
        TTestLocksDb restoredDb(shard, *exec.operator->());
        TVector<ILocksDb::TLockRow> rows;
        UNIT_ASSERT(restoredDb.Load(rows));
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
        auto& row = rows.front();
        row.Flags |= ui64(ELockFlags::Persistent);
        TLockInfo::TPtr lock = new TLockInfo(nullptr, row);
        UNIT_ASSERT_VALUES_EQUAL(lock->GetWriteSeqNum(7), 19);
        UNIT_ASSERT_VALUES_EQUAL(*lock->FindWriteSeqNumState(7)->GcWriteSeqNum, 15);
        UNIT_ASSERT_VALUES_EQUAL(lock->FindWriteSeqNumState(7)->SerializedResult, "current-result");
        UNIT_ASSERT(!lock->FindWriteSeqNumState(8)->GcWriteSeqNum);
        for (auto& ancestor : row.AncestorLocks) {
            lock->AddAncestorLock(std::move(ancestor));
        }
        UNIT_ASSERT_VALUES_EQUAL(lock->GetAncestorLocks().size(), 2);
        for (ui64 createdOn : {10, 30}) {
            const auto* ancestor = lock->FindAncestorLock({10, createdOn});
            UNIT_ASSERT(ancestor && ancestor->CreatedOnRange);
            UNIT_ASSERT_VALUES_EQUAL(ancestor->CreatedOnRange->From.GetCells().front().AsValue<ui64>(), createdOn);
            UNIT_ASSERT_VALUES_EQUAL(ancestor->CreatedOnRange->To.GetCells().front().AsValue<ui64>(), createdOn + 10);
            UNIT_ASSERT(ancestor->CreatedOnRange->FromInclusive);
            UNIT_ASSERT(!ancestor->CreatedOnRange->ToInclusive);
            const auto& state = ancestor->WriteSeqNumStates.at(8);
            UNIT_ASSERT_VALUES_EQUAL(state.WriteSeqNum, 11);
            UNIT_ASSERT_VALUES_EQUAL(state.Branches.size(), 2);
            UNIT_ASSERT(state.GcWriteSeqNum == (createdOn == 10 ? std::optional<ui64>(5) : std::nullopt));
            for (const auto& branch : state.Branches) {
                UNIT_ASSERT(branch.ShardsPath.front() == 10);
                UNIT_ASSERT_VALUES_EQUAL(branch.WriteSeqNum, branch.ShardsPath.size() == 2 ? 10 : 11);
                UNIT_ASSERT_VALUES_EQUAL(branch.SerializedResult, MakeBranchResult(branch.ShardsPath.size() == 2 ? 10 : 11));
            }
        }
        lock->PersistRemoveLock(&restoredDb);
        exec.Commit();
        DrainAncestorGc(exec, shard);
        exec.Replay(EPlay::Boot);
        exec.Begin();
        AssertEmptyState(exec);
        TVector<ILocksDb::TLockRow> empty;
        TTestLocksDb emptyDb(shard, *exec.operator->());
        UNIT_ASSERT(emptyDb.Load(empty));
        UNIT_ASSERT(empty.empty());
        exec.Commit();
    }
}

} // namespace NKikimr::NDataShard
