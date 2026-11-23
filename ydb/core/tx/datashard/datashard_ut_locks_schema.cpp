#include "datashard_locks_schema.h"
#include "datashard_locks_db.h"

#include <ydb/core/protos/data_events.pb.h>
#include <ydb/core/protos/query_stats.pb.h>
#include <ydb/core/tx/locks/locks_db.h>
#include <ydb/core/tablet_flat/test/libs/table/test_dbase.h>
#include <ydb/core/tablet_flat/flat_executor_ut_common.h>
#include <library/cpp/testing/unittest/registar.h>

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

struct TTestShard {};

class TTestLocksDb : public NLocks::TShardLocksDb<TTestShard, TSchema> {
public:
    using TShardLocksDb::TShardLocksDb;

    bool MayAddLock(ui64) override { return true; }

    void PersistRemoveLock(ui64 lockId) override {
        NIceDb::TNiceDb db(DB);
        db.Table<TSchema::Locks>().Key(lockId).Delete();
        HasChanges_ = true;
    }
};

void AssertEmptyState(TDbExec& exec) {
    NIceDb::TNiceDb db(*exec.operator->());
    UNIT_ASSERT(db.Table<TSchema::LockWriteSeqNums>().Select().EndOfSet());
    UNIT_ASSERT(db.Table<TSchema::AncestorShardsLocks>().Select().EndOfSet());
    UNIT_ASSERT(db.Table<TSchema::AncestorLockWriteSeqNums>().Select().EndOfSet());
    UNIT_ASSERT(db.Table<TSchema::AncestorLockBranches>().Select().EndOfSet());
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
        // Same Owner with independent creation sites and disjoint ranges.
        for (ui64 createdOn : {20, 30}) {
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
            db.PersistAncestorLockWriteSeqNum(100, ancestor.GetKey(), 7, 11, {}, createdOn == 20 ? std::optional<ui64>(5) : std::nullopt);
            db.PersistAncestorLockBranch(100, ancestor.GetKey(), 7, {{10, createdOn}, 10, MakeBranchResult(10)});
            db.PersistAncestorLockBranch(100, ancestor.GetKey(), 7, {{10, createdOn, 40}, 11, MakeBranchResult(11)});
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
        for (ui64 createdOn : {20, 30}) {
            const auto* ancestor = lock->FindAncestorLock({10, createdOn});
            UNIT_ASSERT(ancestor && ancestor->CreatedOnRange);
            UNIT_ASSERT_VALUES_EQUAL(ancestor->CreatedOnRange->From.GetCells().front().AsValue<ui64>(), createdOn);
            UNIT_ASSERT_VALUES_EQUAL(ancestor->CreatedOnRange->To.GetCells().front().AsValue<ui64>(), createdOn + 10);
            UNIT_ASSERT(ancestor->CreatedOnRange->FromInclusive);
            UNIT_ASSERT(!ancestor->CreatedOnRange->ToInclusive);
            const auto& state = ancestor->WriteSeqNumStates.at(7);
            UNIT_ASSERT_VALUES_EQUAL(state.WriteSeqNum, 11);
            UNIT_ASSERT_VALUES_EQUAL(state.Branches.size(), 2);
            UNIT_ASSERT(state.GcWriteSeqNum == (createdOn == 20 ? std::optional<ui64>(5) : std::nullopt));
            for (const auto& branch : state.Branches) {
                UNIT_ASSERT(branch.ShardsPath.front() == 10);
                UNIT_ASSERT_VALUES_EQUAL(branch.WriteSeqNum, branch.ShardsPath.size() == 2 ? 10 : 11);
                UNIT_ASSERT_VALUES_EQUAL(branch.SerializedResult, MakeBranchResult(branch.ShardsPath.size() == 2 ? 10 : 11));
            }
        }
        lock->PersistRemoveLock(&restoredDb);
        exec.Commit();
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
