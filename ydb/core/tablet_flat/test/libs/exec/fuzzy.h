#pragma once

#include <ydb/core/tablet_flat/flat_dbase_scheme.h>
#include <ydb/core/tablet_flat/ut/flat_test_db.h>
#include <ydb/core/tablet_flat/ut/flat_test_db_helpers.h>

namespace NKikimr {
namespace NFake {

    class TFuzzySet {
        using TContext = NTabletFlatExecutor::TTransactionContext;
        using ITestDb = NTable::ITestDb;

    public:
        TFuzzySet(bool compress = false) : Compress(compress) { }

        void CreateTableIfNotExist(NTable::TDbWrapper &db, ui32 table)
        {
            const static ui32 KeyId = 2;

            if (RegisterTable(table)) {
                using namespace NTable;

                NTable::TAlter delta;

                delta.SetRedo(8000 /* always put blobs below to redo log */);
                delta.AddTable("test" + ToString(table), table);
                delta.SetRoom(table, 0 /* default room */, 1, {3}, 2);

                if (Compress) { /* setup default family for page compression */
                    delta.AddFamily(table, 0, 0);
                    delta.SetFamily(table, 0, NPage::ECache::None, NPage::ECodec::LZ4);
                } else {
                    delta.AddFamily(table, 0, 0);
                    delta.SetFamilyBlobs(table, 0, 6500, 7000);
                }

                delta.AddColumn(table, "key", KeyId, NScheme::TInt64::TypeId, false);
                delta.AddColumn(table, "value", 200, NScheme::TString::TypeId, false);
                delta.AddColumnToKey(table, KeyId);

                if (table >= 2) { /* used { 0, 1, 2, 3 } tables */
                    const auto comp_g0 = NLocalDb::LegacyQueueIdToTaskName(0);
                    const auto comp_g1 = NLocalDb::LegacyQueueIdToTaskName(1);

                    NLocalDb::TCompactionPolicy policy;

                    policy.InMemSizeToSnapshot = 40 * 1024;
                    policy.InMemStepsToSnapshot = 100;
                    policy.InMemForceStepsToSnapshot = 200;
                    policy.InMemForceSizeToSnapshot = 640 * 1024;
                    policy.InMemResourceBrokerTask = comp_g0;
                    policy.ReadAheadHiThreshold = 100000;
                    policy.ReadAheadLoThreshold = 50000;

                    policy.Generations = {
                        { 100 * 1024, 3, 3, 200 * 1024, comp_g0, true },
                        { 400 * 1024, 3, 3, 800 * 1024, comp_g1, false }
                    };

                    delta.SetCompactionPolicy(table, policy);
                }

                db.Apply(*delta.Flush());
            }
        }

        bool UpdateRowTx(ITestDb &wrap, TContext&, ui32 table, ui32 key, ui32 bytes)
        {
            NTable::TDbWrapper db(wrap);
            CreateTableIfNotExist(db, table);
            db.Apply(db.Update(table).Key(key).Set("value", TString(bytes, 'b')));

            return true;
        }

        bool EraseRowTx(ITestDb &wrap, TContext&, ui32 table, ui32 key)
        {
            NTable::TDbWrapper db(wrap);
            CreateTableIfNotExist(db, table);
            db.Apply(db.Erase(table).Key(key));

            return true;
        }

        bool MutipleTablesTx(ITestDb &wrap, TContext&, ui32 table, ui32 key)
        {
            const ui32 other = (table + (table + key) % 3) % 4;

            NTable::TDbWrapper db(wrap);
            CreateTableIfNotExist(db, table);
            CreateTableIfNotExist(db, other);

            const auto size1 = 10 + RandomNumber<ui32>(9123);
            const auto size2 = 10 + RandomNumber<ui32>(9123);

            db.Apply(db.Update(table).Key(key).Set("value", TString(size1, 'a')));
            db.Apply(db.Erase(other).Key(key+1));
            db.Apply(db.Update(other).Key(key+3).Set("value", TString(size2, 'a')));

            return true;
        }

        bool ReadTx(ITestDb &wrap, TContext &txc, ui32 table)
        {
            const auto mode = NTable::ELookup::GreaterOrEqualThan;

            if (!wrap.GetScheme().GetTableInfo(table))
                return true;

            const std::array<ui32,2> cols{{ 200, 2 }};

            auto it = txc.DB.Iterate(table, { }, cols, mode);

            while (it->Next(NTable::ENext::All) == NTable::EReady::Data) { }

            return it->Last() != NTable::EReady::Page;
        }

        bool DropTables(ITestDb &wrap, TContext&)
        {
            NTable::TAlter delta;

            for (ui32 table: xrange(8 * sizeof(ui64)))
                if (Tables & (ui64(1) << table))
                    delta.DropTable(table);

            Tables = 0;

            wrap.Apply(*delta.Flush());

            return true;
        }

    protected:
        bool RegisterTable(ui32 table) noexcept
        {
            Y_ABORT_UNLESS(table < 8 * sizeof(Tables));

            auto slot = Tables | (ui64(1) << table);

            return std::exchange(Tables, slot) != slot;
        }

    private:
        const bool Compress = false;    /* Configure page compression */
        ui64 Tables = 0;
    };

}
}
