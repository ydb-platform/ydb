#include <ydb/core/tablet_flat/test/libs/exec/owner.h>
#include <ydb/core/tablet_flat/test/libs/exec/runner.h>
#include <ydb/core/tablet_flat/test/libs/exec/dummy.h>
#include <ydb/core/tablet_flat/test/libs/exec/nanny.h>
#include <ydb/core/tablet_flat/test/libs/exec/fuzzy.h>
#include <library/cpp/testing/unittest/registar.h>
#include "flat_database.h"

#include <util/system/sanitizers.h>
#include <util/system/valgrind.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

using ELookup = NTable::ELookup;
using TDbWrapper = NTable::TDbWrapper;
using ITestDb = NTable::ITestDb;

const ui64 MaxActionCount = 12000;
const ui64 MultiPageMaxActionCount = 10000;

class TFuzzyActor : public NFake::TNanny {
public:
    explicit TFuzzyActor(ui32 lives, ui64 limit)
        : Respawn(lives)
        , Actions(limit)
    {
        Spawn();
    }

    EDo Run() override
    {
        using TContext = TTransactionContext;

        Actions -= Min(Actions, ui32(1));

        NFake::TFuncTx::TCall func;

        const ui32 action = RandomNumber<ui32>(55);
        const ui32 table = RandomNumber<ui32>(4);
        const ui32 key = RandomNumber<ui32>(300);

        if (Actions == 0) {
            func = [this](ITestDb& testDb, TContext &txc) {
                return Fuzzy.DropTables(testDb, txc);
            };
        } else if (action < 10) {
            auto size = 81 + RandomNumber<ui32>(342);
            func = [this, table, key, size](ITestDb& testDb, TContext &txc) {
                return Fuzzy.UpdateRowTx(testDb, txc, table, key, size);
            };
        } else if (action < 20) {
            auto size = 3000 + RandomNumber<ui32>(9123);

            func = [this, table, key, size](ITestDb& testDb, TContext &txc) {
                return Fuzzy.UpdateRowTx(testDb, txc, table, key, size);
            };
        } else if (action < 40) {
            func = [this, table](ITestDb& testDb, TContext &txc) {
                return Fuzzy.ReadTx(testDb, txc, table);
            };
        } else if (action < 50) {
            func = [this, table, key](ITestDb& testDb, TContext &txc) {
                return Fuzzy.MutipleTablesTx(testDb, txc, table, key);
            };
        } else if (action < 55) {
            func = [this, table, key](ITestDb& testDb, TContext &txc) {
                return Fuzzy.EraseRowTx(testDb, txc, table, key);
            };
        } else {
            Y_ABORT("Random generator produced unexpected action value");
        }

        QueueTx(func);

        return !Actions ? EDo::Stop : Actions < Rebirth ? Spawn() : EDo::More;
    }

private:
    EDo Spawn() noexcept
    {
        Respawn -= Min(Respawn, ui32(1));

        auto less = Actions / Max(Respawn + 1, ui32(1));

        Rebirth = Actions - Min(Actions, Max(less, ui32(1)));

        return EDo::Born;
    }

private:
    ui32 Respawn = 0;   /* Left tablet restarts     */
    ui32 Actions = 0;   /* Left transaction actions */
    ui32 Rebirth = 0;   /* When to restart tablet   */

    NFake::TFuzzySet Fuzzy{ false };
};


class TDbTestPlayerActor : public NFake::TNanny {
public:
    explicit TDbTestPlayerActor(const TVector<NFake::TFuncTx::TCall>& actions)
        : Actions(actions)
    {
        Y_ABORT_UNLESS(actions.size(), "Have to pass at least one action");
    }

    EDo Run() override
    {
        QueueTx(std::move(Actions.at(Index)));

        return ++Index < Actions.size() ? EDo::More : EDo::Stop;
    }

private:
    TVector<NFake::TFuncTx::TCall> Actions;
    size_t Index = 0;
};

// Mimics schema and transactions that happen inside coordinator
class THeThing : public NFake::TNanny {
private:
    ui64 ActionCount = 0;
    const ui64 MaxActionCount;
    bool SchemaReady = false;

    TIntrusivePtr<IRandomProvider> RandomProvider;

    using TTxId = ui64;

    const ui32 TxTable = 0;
    const ui32 AffectedTable = 4;

    TVector<TDeque<TTxId>> DatashardTxQueues;
    THashMap<TTxId, ui32> UnconfirmedCount;
    TTxId LastTxId = 0;
    ui64 LastDatashardIdx = 0;

    void CreateSchema(TDbWrapper& db) {
        if (std::exchange(SchemaReady, true))
            return;

        NTable::TAlter delta;

        delta.AddTable("TxTable", TxTable);
        delta.AddColumn(TxTable, "TxId", 1, NScheme::TUint64::TypeId, false);
        delta.AddColumn(TxTable, "Plan", 2, NScheme::TUint64::TypeId, false);
        delta.AddColumnToKey(TxTable, 1);

        delta.AddTable("AffectedTable", AffectedTable);
        delta.AddColumn(AffectedTable, "TxId", 1, NScheme::TUint64::TypeId, false);
        delta.AddColumn(AffectedTable, "Datashard", 2, NScheme::TUint64::TypeId, false);
        delta.AddColumn(AffectedTable, "Plan", 3, NScheme::TUint64::TypeId, false);
        delta.AddColumnToKey(AffectedTable, 1);
        delta.AddColumnToKey(AffectedTable, 2);

        {
            NLocalDb::TCompactionPolicy policy;

            policy.InMemStepsToSnapshot = 10;
            policy.InMemForceStepsToSnapshot = 20;

            delta.SetCompactionPolicy(TxTable, policy);
            delta.SetCompactionPolicy(AffectedTable, policy);
        }

        db.Apply(*delta.Flush());
    }

    void AddRandomTx(TDbWrapper& db) {
        db.Apply(db.Update(TxTable).Key(LastTxId).Set("Plan", LastTxId));
        for (ui32 i = 0; i < 3; ++i) {
            // Choose random datashard to be Tx participant
            ui64 datashard = RandomNumber(DatashardTxQueues.size());
            DatashardTxQueues[datashard].push_back(LastTxId);
            db.Apply(db.Update(AffectedTable).Key(LastTxId, datashard).Set("Plan", LastTxId));
            UnconfirmedCount[LastTxId]++;
        }
        LastTxId++;
    }

    void CompleteDatashardTx(ui64 datashard, TDbWrapper& db) {
        if (DatashardTxQueues[datashard].empty())
            return;

        TTxId txId = DatashardTxQueues[datashard].front();
        DatashardTxQueues[datashard].pop_front();
        db.Apply(db.Erase(AffectedTable).Key(txId, datashard));

        ui32 cnt = --UnconfirmedCount[txId];
        if (cnt == 0) {
            // All participants confirmed Tx completion
            UnconfirmedCount.erase(txId);
            db.Apply(db.Erase(TxTable).Key(txId));
        }
    }

    // Inserts in both TxTable and AffectedTable
    void StartTransactions(TDbWrapper& db) {
        CreateSchema(db);

        // Generate some new transactions
        ui64 newTxCount = RandomNumber(5);
        for (ui64 i = 0; i < newTxCount; ++i) {
            AddRandomTx(db);
        }
    }

    // Always deletes from AffectedTable and sometimes from TxTable
    void FinishTransactions(TDbWrapper& db) {
        // Finish some transactions on each datashard
        ui64 txCountInFlight = RandomNumber(1+UnconfirmedCount.size());
        do {
            if (RandomNumber(2))
                CompleteDatashardTx(LastDatashardIdx, db);
            LastDatashardIdx += 1;
            LastDatashardIdx %= DatashardTxQueues.size();
        } while (UnconfirmedCount.size() > txCountInFlight);
    }

    ui64 RandomNumber(ui64 limit) {
        Y_ABORT_UNLESS(limit > 0, "Invalid limit specified [0,%" PRIu64 ")", limit);
        return RandomProvider->GenRand64() % limit;
    }

public:
    THeThing(ui64 maxActionCount, ui64 randomSeed)
        : MaxActionCount(maxActionCount)
        , RandomProvider(CreateDeterministicRandomProvider(randomSeed))
    {
        DatashardTxQueues.resize(20);
    }

    EDo Run() override
    {
        if (RandomNumber(1000) < 4)
            return EDo::Born;

        ui32 action = RandomNumber(8);
        if (action < 3) {
            QueueTx([this](ITestDb& testDb, TTransactionContext&){ TDbWrapper db(testDb); this->StartTransactions(db); return true; });
        } else {
            QueueTx([this](ITestDb& testDb, TTransactionContext&){ TDbWrapper db(testDb); this->FinishTransactions(db); return true; });
        }

        return ++ActionCount < MaxActionCount ? EDo::More : EDo::Stop;
    }
};


// Generates a table with many rows and the does a SelectRange query for the whole table
// If prefetch works properly the SelectRange transaction it is expected not to have restarts
class TFullScan : public NFake::TNanny {
public:
    explicit TFullScan(ui64 rows) : Rows(rows) { }

private:
    EDo Run() override
    {
        if (++RowCount <= Rows) {
            QueueTx([this](ITestDb& testDb, TTransactionContext&){ TDbWrapper db(testDb); this->AddRandomRowTx(db); return true; });
        } else if (RowCount ==  Rows + 1) {
            QueueTx([this](ITestDb& testDb, TTransactionContext&){ TDbWrapper db(testDb); return this->DoFullScanTx(db); });
        } else if (RowCount > Rows + 1) {
            Y_ABORT("Shouldn't request more task after EDo::Stop");
        }

        return RowCount <= Rows ? EDo::More : EDo::Stop;
    }

    void CreateSchema(TDbWrapper& db)
    {
        if (std::exchange(SchemaReady, true))
            return;

        NTable::TAlter delta;

        delta.AddTable("table", Table);
        delta.SetFamily(Table, AltFamily, NTable::NPage::ECache::None, NTable::NPage::ECodec::Plain);
        delta.AddColumn(Table, "Id", 1, NScheme::TUint32::TypeId, false);
        delta.AddColumn(Table, "value", 2, NScheme::TUint64::TypeId, false);
        delta.AddColumn(Table, "large", 3, NScheme::TString::TypeId, false);
        delta.AddColumnToKey(Table, 1);
        delta.AddColumnToFamily(Table, 2, AltFamily);
        delta.AddColumnToFamily(Table, 3, AltFamily);

        {
            const auto comp_g0 = NLocalDb::LegacyQueueIdToTaskName(0);
            const auto comp_g1 = NLocalDb::LegacyQueueIdToTaskName(1);

            NLocalDb::TCompactionPolicy policy;

            policy.InMemSizeToSnapshot = 40 * 1024 *1024;
            policy.InMemStepsToSnapshot = 300;
            policy.InMemForceStepsToSnapshot = 500;
            policy.InMemForceSizeToSnapshot = 64 * 1024 * 1024;
            policy.InMemResourceBrokerTask = comp_g0;
            policy.ReadAheadHiThreshold = 100000;
            policy.ReadAheadLoThreshold = 50000;

            /* should not warm pages to cache after compaction (last
                false), need for testing precharge and exactly one tx
                resrart on full scan.
             */
            policy.Generations = {
                { 200 * 1024 * 1024, 8, 8, 300 * 1024 * 1024, comp_g0, false },
                { 400 * 1024 * 1024, 8, 8, 800 * 1024 * 1024, comp_g1, false }
            };

            delta.SetCompactionPolicy(Table, policy);
        }

        db.Apply(*delta.Flush());
    }

    void AddRandomRowTx(TDbWrapper& db) {
        CreateSchema(db);

        ui64 rowId = RowCount;

        // Add big rows with big values in order to produce many pages
        db.Apply(db.Update(Table).Key(rowId).Set("value", rowId).Set("large", TString(10000, 'A')));
    }

    bool DoFullScanTx(TDbWrapper& db) {
        const std::array<ui32, 2> tags{{ 1 /* Id */, 2 /* value */ }};

        try {
            db->Precharge(Table, { }, { }, tags, 0);
        } catch (NTable::TIteratorNotReady&) {
            Restarts++;
            Cerr << "Precharge restarts " << Restarts << " times" << Endl;
            Y_ABORT_UNLESS(Restarts < 5, "Too many precharge restarts");
            return false;
        }

        try {
            TAutoPtr<NTable::ITestIterator> it = db->Iterate(Table, { }, tags, ELookup::GreaterOrEqualThan);

            while (it->Next(NTable::ENext::All) == NTable::EReady::Data) {
                LastKey = it->GetValues().Columns[0].AsValue<ui32>();
            }

            Y_ABORT_UNLESS(LastKey + 1 == RowCount /* incomplete read */);

            return true;
        } catch (NTable::TIteratorNotReady&) {
            Y_ABORT_UNLESS(false, "All the data should be precharged");
        }
    }

private:
    const ui64 Rows = 0;
    const ui32 Table = 1;
    const ui32 AltFamily = 1;

    ui32 Restarts = 0;
    ui64 RowCount = 0;
    ui64 LastKey = Max<ui64>();
    bool SchemaReady = false;
};

void RunTest(IActor *test)
{
    NFake::TRunner env;

    env->SetLogPriority(NKikimrServices::TABLET_MAIN, NActors::NLog::PRI_CRIT);
    env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_INFO);
    env->SetLogPriority(NKikimrServices::TABLET_OPS_HOST, NActors::NLog::PRI_INFO);
    env->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_INFO);
    env->SetLogPriority(NKikimrServices::OPS_BACKUP, NActors::NLog::PRI_INFO);
    env->SetLogPriority(NKikimrServices::SAUSAGE_BIO, NActors::NLog::PRI_INFO);
    env->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NActors::NLog::PRI_INFO);

    if (false) {
        env->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
        env->SetLogPriority(NKikimrServices::TABLET_FLATBOOT, NActors::NLog::PRI_DEBUG);
        env->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NActors::NLog::PRI_DEBUG);
    }

    env.RunTest(test);
    env.Finalize();
}

Y_UNIT_TEST_SUITE(TExecutorDb) {
    Y_UNIT_TEST(RandomOps)
    {
        RunTest(new TFuzzyActor(5, MaxActionCount));
    }

    Y_UNIT_TEST(FullScan)
    {
        RunTest(new TFullScan(MultiPageMaxActionCount));
    }

    Y_UNIT_TEST(CoordinatorSimulation)
    {
        RunTest(new THeThing(MaxActionCount, 42));
    }

    Y_UNIT_TEST(RandomCoordinatorSimulation)
    {
        RunTest(new THeThing(MaxActionCount, TInstant::Now().Seconds()));
    }

    Y_UNIT_TEST(MultiPage)
    {
        NFake::TFuzzySet fuzzy(false /* no compression */);

        TVector<NFake::TFuncTx::TCall> tx = {
            [&fuzzy](ITestDb& testDb, TTransactionContext &txc){ return fuzzy.UpdateRowTx(testDb, txc, 2, 100, 10000000); },
            [&fuzzy](ITestDb& testDb, TTransactionContext &txc){ return fuzzy.UpdateRowTx(testDb, txc, 2, 101, 10000000); },
            [&fuzzy](ITestDb& testDb, TTransactionContext &txc){ return fuzzy.UpdateRowTx(testDb, txc, 2, 100, 10000000); },
            [&fuzzy](ITestDb& testDb, TTransactionContext &txc){ return fuzzy.ReadTx(testDb, txc, 2); }
        };

        RunTest(new TDbTestPlayerActor(tx));
    }

    Y_UNIT_TEST(EncodedPage)
    {
        NFake::TFuzzySet fuzzy(true /* compress */);

        TVector<NFake::TFuncTx::TCall> tx = {
            [&fuzzy](ITestDb& db, TTransactionContext &txc){ return fuzzy.UpdateRowTx(db, txc, 2, 100, 10000000); },
            [&fuzzy](ITestDb& db, TTransactionContext &txc){ return fuzzy.UpdateRowTx(db, txc, 2, 101, 10000000); },
            [&fuzzy](ITestDb& db, TTransactionContext &txc){ return fuzzy.UpdateRowTx(db, txc, 2, 100, 10000000); },
            [&fuzzy](ITestDb& db, TTransactionContext &txc){ return fuzzy.ReadTx(db, txc, 2); }

            /* There should be yet another tx or something else that checks
                page compression in parts, but this is going to be very wired
                here. Thus check manually OPS_COMPACT log output and compare
                the result with MultiPage output, it works on the same rows.
             */
        };

        RunTest(new TDbTestPlayerActor(tx));
    }

}

}
}
