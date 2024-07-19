#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tablet_flat/test/libs/table/test_dummy.h>
#include <ydb/library/mkql_proto/protos/minikql.pb.h>
#include "flat_cxx_database.h"
#include "flat_dbase_scheme.h"
#include "flat_dbase_apply.h"

namespace NKikimr {
namespace NTable {

Y_UNIT_TEST_SUITE(TFlatEraseCacheTest) {
    using TDummyEnv = NTable::TDummyEnv;

    struct Schema : NIceDb::Schema {
        struct TestTable : Table<1> {
            struct ID1 : Column<1, NScheme::NTypeIds::Uint64> {};
            struct ID2 : Column<2, NScheme::NTypeIds::Uint64> {};
            struct Value : Column<3, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<ID1, ID2>;
            using TColumns = TableColumns<ID1, ID2, Value>;
        };

        struct TestTableStr : Table<2> {
            struct ID1 : Column<1, NScheme::NTypeIds::Uint64> {};
            struct ID2 : Column<2, NScheme::NTypeIds::String> {};
            struct Value : Column<3, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<ID1, ID2>;
            using TColumns = TableColumns<ID1, ID2, Value>;
        };

        using TTables = SchemaTables<TestTable, TestTableStr>;
    };

    struct TRow {
        ui64 ID1;
        ui64 ID2;
        ui64 Value;
    };

    /**
     * Returns cache tree node size in bytes
     */
    size_t GetNodeSize() {
        // Construct fake TKeyCellDefaults
        TVector<NScheme::TTypeInfoOrder> types;
        types.emplace_back(NScheme::TTypeInfo(NScheme::NTypeIds::Uint64));
        TVector<TCell> defs(1);
        TIntrusiveConstPtr<TKeyCellDefaults> keyDefaults = TKeyCellDefaults::Make(types, defs);
        // Empty default config
        TKeyRangeCache cache(*keyDefaults, { });
        // Empty keys, both inclusive
        TKeyRangeEntry entry({ }, { }, true, true, TRowVersion::Min());
        // This will allocate a single node in the map
        cache.Add(entry);
        // This should be the size of that single node
        return cache.GetTotalUsed();
    }

    TString DumpCache(const TDatabase& db, ui32 table) {
        if (auto* cache = db.DebugGetTableErasedKeysCache(table)) {
            TStringStream stream;
            stream << cache->DumpRanges();
            return stream.Str();
        } else {
            return "nullptr";
        }
    }

    Y_UNIT_TEST(BasicUsage) {
        TDatabase DB;
        NIceDb::TNiceDb db(DB);

        auto dumpCache = [&DB]() -> TString {
            return DumpCache(DB, 1);
        };

        {
            TDummyEnv env;
            DB.Begin(1, env);
            db.Materialize<Schema>();
            DB.Alter().SetEraseCache(1, true, 2, 8192);
            DB.Commit(1, true);
        }

        {
            TDummyEnv env;
            DB.Begin(2, env);
            auto table = db.Table<Schema::TestTable>();
            // (1, 1) -> 42
            // (1, x) -> deleted
            // (2, x) -> deleted
            // (2, 32) -> 51
            // (3, x) -> 100 + x
            for (ui64 key2 = 1; key2 <= 32; ++key2) {
                if (key2 == 1) {
                    table.Key(1u, key2).Update<Schema::TestTable::Value>(42u);
                    table.Key(2u, key2).Delete();
                } else if (key2 == 32) {
                    table.Key(1u, key2).Delete();
                    table.Key(2u, key2).Update<Schema::TestTable::Value>(51u);
                } else {
                    table.Key(1u, key2).Delete();
                    table.Key(2u, key2).Delete();
                }
                table.Key(3u, key2).Update<Schema::TestTable::Value>(100u + key2);
            }
            DB.Commit(2, true);
        }

        {
            TDummyEnv env;
            DB.Begin(3, env);
            TVector<TRow> rows;
            auto rowset = db.Table<Schema::TestTable>().Prefix(2u).Select();
            UNIT_ASSERT(rowset.IsReady());
            while (!rowset.EndOfSet()) {
                ui64 id1 = rowset.GetValue<Schema::TestTable::ID1>();
                ui64 id2 = rowset.GetValue<Schema::TestTable::ID2>();
                ui64 value = rowset.GetValue<Schema::TestTable::Value>();
                rows.push_back({ id1, id2, value });
                UNIT_ASSERT(rowset.Next());
            }
            ui64 rowSkips = rowset.Stats()->DeletedRowSkips;
            DB.Commit(3, true);

            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(rows[0].ID1, 2u);
            UNIT_ASSERT_VALUES_EQUAL(rows[0].ID2, 32u);
            UNIT_ASSERT_VALUES_EQUAL(rows[0].Value, 51u);

            // We should have skipped 31 uncached deleted rows
            UNIT_ASSERT_VALUES_EQUAL(rowSkips, 31u);
            UNIT_ASSERT_VALUES_EQUAL(dumpCache(), "TKeyRangeCache{ [{2, 1}, {2, 31}] }");
        }

        {
            TDummyEnv env;
            DB.Begin(4, env);
            auto rowset = db.Table<Schema::TestTable>().LessOrEqual(1u, 16u).Select();
            UNIT_ASSERT(rowset.IsReady());
            while (!rowset.EndOfSet()) {
                UNIT_ASSERT(rowset.Next());
            }
            ui64 rowSkips = rowset.Stats()->DeletedRowSkips;
            DB.Commit(4, true);

            // We should have skipped 15 erase markers (and then stopped)
            UNIT_ASSERT_VALUES_EQUAL(rowSkips, 15u);
            UNIT_ASSERT_VALUES_EQUAL(dumpCache(), "TKeyRangeCache{ [{1, 2}, {1, 16}], [{2, 1}, {2, 31}] }");
        }

        {
            TDummyEnv env;
            DB.Begin(5, env);
            TVector<TRow> rows;
            auto rowset = db.Table<Schema::TestTable>()
                .LessOrEqual(3u, 1u)
                .Select();
            UNIT_ASSERT(rowset.IsReady());
            while (!rowset.EndOfSet()) {
                ui64 id1 = rowset.GetValue<Schema::TestTable::ID1>();
                ui64 id2 = rowset.GetValue<Schema::TestTable::ID2>();
                ui64 value = rowset.GetValue<Schema::TestTable::Value>();
                rows.push_back({ id1, id2, value });
                UNIT_ASSERT(rowset.Next());
            }
            ui64 rowSkips = rowset.Stats()->DeletedRowSkips;
            DB.Commit(5, true);

            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(rows[0].ID1, 1u);
            UNIT_ASSERT_VALUES_EQUAL(rows[0].ID2, 1u);
            UNIT_ASSERT_VALUES_EQUAL(rows[0].Value, 42u);
            UNIT_ASSERT_VALUES_EQUAL(rows[1].ID1, 2u);
            UNIT_ASSERT_VALUES_EQUAL(rows[1].ID2, 32u);
            UNIT_ASSERT_VALUES_EQUAL(rows[1].Value, 51u);
            UNIT_ASSERT_VALUES_EQUAL(rows[2].ID1, 3u);
            UNIT_ASSERT_VALUES_EQUAL(rows[2].ID2, 1u);
            UNIT_ASSERT_VALUES_EQUAL(rows[2].Value, 101u);

            // We should have done 1 jump, then 16 skips, then 1 more jump
            UNIT_ASSERT_VALUES_EQUAL(rowSkips, 18u);
            UNIT_ASSERT_VALUES_EQUAL(dumpCache(), "TKeyRangeCache{ [{1, 2}, {2, 31}] }");
        }

        {
            TDummyEnv env;
            DB.Begin(6, env);
            TVector<TRow> rows;
            auto rowset = db.Table<Schema::TestTable>()
                .GreaterOrEqual(2u, 15u)
                .LessOrEqual(3u, 1u)
                .Select();
            UNIT_ASSERT(rowset.IsReady());
            while (!rowset.EndOfSet()) {
                ui64 id1 = rowset.GetValue<Schema::TestTable::ID1>();
                ui64 id2 = rowset.GetValue<Schema::TestTable::ID2>();
                ui64 value = rowset.GetValue<Schema::TestTable::Value>();
                rows.push_back({ id1, id2, value });
                UNIT_ASSERT(rowset.Next());
            }
            ui64 rowSkips = rowset.Stats()->DeletedRowSkips;
            DB.Commit(6, true);

            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(rows[0].ID1, 2u);
            UNIT_ASSERT_VALUES_EQUAL(rows[0].ID2, 32u);
            UNIT_ASSERT_VALUES_EQUAL(rows[0].Value, 51u);
            UNIT_ASSERT_VALUES_EQUAL(rows[1].ID1, 3u);
            UNIT_ASSERT_VALUES_EQUAL(rows[1].ID2, 1u);
            UNIT_ASSERT_VALUES_EQUAL(rows[1].Value, 101u);

            // We should have done 1 jump
            UNIT_ASSERT_VALUES_EQUAL(rowSkips, 1u);
            UNIT_ASSERT_VALUES_EQUAL(dumpCache(), "TKeyRangeCache{ [{1, 2}, {2, 31}] }");
        }

        {
            TDummyEnv env;
            DB.Begin(7, env);
            db.Table<Schema::TestTable>().Key(2u, 1u).Delete();
            db.Table<Schema::TestTable>().Key(2u, 32u).Update<Schema::TestTable::Value>(99u);
            DB.Commit(7, true);

            // We didn't touch any erased ranges, so it should be invalidated
            UNIT_ASSERT_VALUES_EQUAL(dumpCache(), "TKeyRangeCache{ [{1, 2}, {2, 31}] }");
        }

        {
            TDummyEnv env;
            DB.Begin(8, env);
            db.Table<Schema::TestTable>().Key(2u, 5u).Update<Schema::TestTable::Value>(11u);
            DB.Commit(8, true);

            // We touched an erased range, it should become invalidated
            UNIT_ASSERT_VALUES_EQUAL(dumpCache(), "TKeyRangeCache{ [{1, 2}, {2, 5}) }");
        }

        {
            TDummyEnv env;
            DB.Begin(9, env);
            auto rowset = db.Table<Schema::TestTable>().All().Select();
            UNIT_ASSERT(rowset.IsReady());
            while (!rowset.EndOfSet()) {
                UNIT_ASSERT(rowset.Next());
            }
            DB.Commit(9, true);

            // We've seen all rows, expect correct erased ranges
            UNIT_ASSERT_VALUES_EQUAL(dumpCache(), "TKeyRangeCache{ [{1, 2}, {2, 5}), [{2, 6}, {2, 31}] }");
        }
    }

    Y_UNIT_TEST(BasicUsageReverse) {
        TDatabase DB;
        NIceDb::TNiceDb db(DB);

        auto dumpCache = [&DB]() -> TString {
            return DumpCache(DB, 1);
        };

        {
            TDummyEnv env;
            DB.Begin(1, env);
            db.Materialize<Schema>();
            DB.Alter().SetEraseCache(1, true, 2, 8192);
            DB.Commit(1, true);
        }

        {
            TDummyEnv env;
            DB.Begin(2, env);
            auto table = db.Table<Schema::TestTable>();
            // N.B. mirror of BasicUsage test
            // (1, x) -> 100 + x
            // (2, 1) -> 51
            // (2, x) -> deleted
            // (3, x) -> deleted
            // (3, 32) -> 42
            for (ui64 key2 = 1; key2 <= 32; ++key2) {
                if (key2 == 32) {
                    table.Key(3u, key2).Update<Schema::TestTable::Value>(42u);
                    table.Key(2u, key2).Delete();
                } else if (key2 == 1) {
                    table.Key(3u, key2).Delete();
                    table.Key(2u, key2).Update<Schema::TestTable::Value>(51u);
                } else {
                    table.Key(3u, key2).Delete();
                    table.Key(2u, key2).Delete();
                }
                table.Key(1u, key2).Update<Schema::TestTable::Value>(100u + key2);
            }
            DB.Commit(2, true);
        }

        {
            TDummyEnv env;
            DB.Begin(3, env);
            TVector<TRow> rows;
            auto rowset = db.Table<Schema::TestTable>().Reverse().Prefix(2u).Select();
            UNIT_ASSERT(rowset.IsReady());
            while (!rowset.EndOfSet()) {
                ui64 id1 = rowset.GetValue<Schema::TestTable::ID1>();
                ui64 id2 = rowset.GetValue<Schema::TestTable::ID2>();
                ui64 value = rowset.GetValue<Schema::TestTable::Value>();
                rows.push_back({ id1, id2, value });
                UNIT_ASSERT(rowset.Next());
            }
            ui64 rowSkips = rowset.Stats()->DeletedRowSkips;
            DB.Commit(3, true);

            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(rows[0].ID1, 2u);
            UNIT_ASSERT_VALUES_EQUAL(rows[0].ID2, 1u);
            UNIT_ASSERT_VALUES_EQUAL(rows[0].Value, 51u);

            // We should have skipped 31 uncached deleted rows
            UNIT_ASSERT_VALUES_EQUAL(rowSkips, 31u);
            UNIT_ASSERT_VALUES_EQUAL(dumpCache(), "TKeyRangeCache{ [{2, 2}, {2, 32}] }");
        }

        {
            TDummyEnv env;
            DB.Begin(4, env);
            auto rowset = db.Table<Schema::TestTable>().Reverse().GreaterOrEqual(3u, 17u).Select();
            UNIT_ASSERT(rowset.IsReady());
            while (!rowset.EndOfSet()) {
                UNIT_ASSERT(rowset.Next());
            }
            ui64 rowSkips = rowset.Stats()->DeletedRowSkips;
            DB.Commit(4, true);

            // We should have skipped 15 erase markers (and then stopped)
            UNIT_ASSERT_VALUES_EQUAL(rowSkips, 15u);
            UNIT_ASSERT_VALUES_EQUAL(dumpCache(), "TKeyRangeCache{ [{2, 2}, {2, 32}], [{3, 17}, {3, 31}] }");
        }

        {
            TDummyEnv env;
            DB.Begin(5, env);
            TVector<TRow> rows;
            auto rowset = db.Table<Schema::TestTable>()
                .Reverse()
                .GreaterOrEqual(1u, 32u)
                .Select();
            UNIT_ASSERT(rowset.IsReady());
            while (!rowset.EndOfSet()) {
                ui64 id1 = rowset.GetValue<Schema::TestTable::ID1>();
                ui64 id2 = rowset.GetValue<Schema::TestTable::ID2>();
                ui64 value = rowset.GetValue<Schema::TestTable::Value>();
                rows.push_back({ id1, id2, value });
                UNIT_ASSERT(rowset.Next());
            }
            ui64 rowSkips = rowset.Stats()->DeletedRowSkips;
            DB.Commit(5, true);

            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(rows[0].ID1, 3u);
            UNIT_ASSERT_VALUES_EQUAL(rows[0].ID2, 32u);
            UNIT_ASSERT_VALUES_EQUAL(rows[0].Value, 42u);
            UNIT_ASSERT_VALUES_EQUAL(rows[1].ID1, 2u);
            UNIT_ASSERT_VALUES_EQUAL(rows[1].ID2, 1u);
            UNIT_ASSERT_VALUES_EQUAL(rows[1].Value, 51u);
            UNIT_ASSERT_VALUES_EQUAL(rows[2].ID1, 1u);
            UNIT_ASSERT_VALUES_EQUAL(rows[2].ID2, 32u);
            UNIT_ASSERT_VALUES_EQUAL(rows[2].Value, 132u);

            // We should have done 1 jump, then 16 skips, then 1 more jump
            UNIT_ASSERT_VALUES_EQUAL(rowSkips, 18u);
            UNIT_ASSERT_VALUES_EQUAL(dumpCache(), "TKeyRangeCache{ [{2, 2}, {3, 31}] }");
        }

        {
            TDummyEnv env;
            DB.Begin(6, env);
            TVector<TRow> rows;
            auto rowset = db.Table<Schema::TestTable>()
                .Reverse()
                .LessOrEqual(2u, 16u)
                .GreaterOrEqual(1u, 32u)
                .Select();
            UNIT_ASSERT(rowset.IsReady());
            while (!rowset.EndOfSet()) {
                ui64 id1 = rowset.GetValue<Schema::TestTable::ID1>();
                ui64 id2 = rowset.GetValue<Schema::TestTable::ID2>();
                ui64 value = rowset.GetValue<Schema::TestTable::Value>();
                rows.push_back({ id1, id2, value });
                UNIT_ASSERT(rowset.Next());
            }
            ui64 rowSkips = rowset.Stats()->DeletedRowSkips;
            DB.Commit(6, true);

            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(rows[0].ID1, 2u);
            UNIT_ASSERT_VALUES_EQUAL(rows[0].ID2, 1u);
            UNIT_ASSERT_VALUES_EQUAL(rows[0].Value, 51u);
            UNIT_ASSERT_VALUES_EQUAL(rows[1].ID1, 1u);
            UNIT_ASSERT_VALUES_EQUAL(rows[1].ID2, 32u);
            UNIT_ASSERT_VALUES_EQUAL(rows[1].Value, 132u);

            // We should have done 1 jump
            UNIT_ASSERT_VALUES_EQUAL(rowSkips, 1u);
            UNIT_ASSERT_VALUES_EQUAL(dumpCache(), "TKeyRangeCache{ [{2, 2}, {3, 31}] }");
        }

        {
            TDummyEnv env;
            DB.Begin(7, env);
            db.Table<Schema::TestTable>().Key(2u, 32u).Delete();
            db.Table<Schema::TestTable>().Key(2u, 1u).Update<Schema::TestTable::Value>(99u);
            DB.Commit(7, true);

            // We didn't touch any erased ranges, so it shouldn't be invalidated
            UNIT_ASSERT_VALUES_EQUAL(dumpCache(), "TKeyRangeCache{ [{2, 2}, {3, 31}] }");
        }

        {
            TDummyEnv env;
            DB.Begin(8, env);
            db.Table<Schema::TestTable>().Key(2u, 28u).Update<Schema::TestTable::Value>(11u);
            DB.Commit(8, true);

            // We touched an erased range, it should become invalidated
            UNIT_ASSERT_VALUES_EQUAL(dumpCache(), "TKeyRangeCache{ [{2, 2}, {2, 28}) }");
        }

        {
            TDummyEnv env;
            DB.Begin(9, env);
            auto rowset = db.Table<Schema::TestTable>().Reverse().Select();
            UNIT_ASSERT(rowset.IsReady());
            while (!rowset.EndOfSet()) {
                UNIT_ASSERT(rowset.Next());
            }
            DB.Commit(9, true);

            // We've seen all rows, expect correct erased ranges
            UNIT_ASSERT_VALUES_EQUAL(dumpCache(), "TKeyRangeCache{ [{2, 2}, {2, 28}), [{2, 29}, {3, 31}] }");
        }
    }

    Y_UNIT_TEST(CacheEviction) {
        TDatabase DB;
        NIceDb::TNiceDb db(DB);

        size_t nodeSize = GetNodeSize();

        auto dumpCache = [&DB]() -> TString {
            return DumpCache(DB, 1);
        };

        {
            TDummyEnv env;
            DB.Begin(1, env);
            db.Materialize<Schema>();
            // tuned to exactly 4 nodes with 2 keys each in the pool
            DB.Alter().SetEraseCache(1, true, 2, nodeSize * 4 + sizeof(TCell) * 2 * 8);
            DB.Commit(1, true);
        }

        {
            TDummyEnv env;
            DB.Begin(2, env);
            auto table = db.Table<Schema::TestTable>();
            for (ui64 key1 = 1; key1 <= 64; ++key1) {
                for (ui64 key2 = 1; key2 <= 32; ++key2) {
                    if (key2 == 1) {
                        table.Key(key1, key2).Update<Schema::TestTable::Value>(42u);
                    } else {
                        table.Key(key1, key2).Delete();
                    }
                }
            }
            DB.Commit(2, true);
        }

        {
            TDummyEnv env;
            size_t rows = 0;
            DB.Begin(3, env);
            {
                auto rowset = db.Table<Schema::TestTable>().All().Select();
                UNIT_ASSERT(rowset.IsReady());
                while (!rowset.EndOfSet()) {
                    ++rows;
                    UNIT_ASSERT(rowset.Next());
                }
            }
            DB.Commit(3, true);

            // We only have enough space for the first 4 ranges to cache
            UNIT_ASSERT_VALUES_EQUAL(rows, 64u);
            UNIT_ASSERT_VALUES_EQUAL(dumpCache(), "TKeyRangeCache{ [{1, 2}, {1, 32}], [{2, 2}, {2, 32}], [{3, 2}, {3, 32}], [{4, 2}, {4, 32}] }");
        }

        {
            TDummyEnv env;
            DB.Begin(4, env);
            {
                auto rowset = db.Table<Schema::TestTable>().GreaterOrEqual(7u, 1u).Select();
                UNIT_ASSERT(rowset.IsReady());
                UNIT_ASSERT(!rowset.EndOfSet());
                UNIT_ASSERT(rowset.Next());
                UNIT_ASSERT(!rowset.EndOfSet());
            }
            DB.Commit(4, true);

            // We consider ranges at the start of iteration a lot more precious
            // By this logic [{4, 2}, {5, 1}) is evicted
            UNIT_ASSERT_VALUES_EQUAL(dumpCache(), "TKeyRangeCache{ [{1, 2}, {1, 32}], [{2, 2}, {2, 32}], [{3, 2}, {3, 32}], [{7, 2}, {7, 32}] }");
        }

        {
            TDummyEnv env;
            ui64 rows = 0;
            DB.Begin(5, env);
            {
                auto rowset = db.Table<Schema::TestTable>()
                    .GreaterOrEqual(8u, 1u)
                    .LessOrEqual(10u, 1u)
                    .Select();
                UNIT_ASSERT(rowset.IsReady());
                while (!rowset.EndOfSet()) {
                    ++rows;
                    UNIT_ASSERT(rowset.Next());
                }
            }
            DB.Commit(5, true);

            // This time we should have 2 more ranges in the cache
            // Eating away from the oldest tail of ranges
            UNIT_ASSERT_VALUES_EQUAL(rows, 3u);
            UNIT_ASSERT_VALUES_EQUAL(dumpCache(), "TKeyRangeCache{ [{1, 2}, {1, 32}], [{7, 2}, {7, 32}], [{8, 2}, {8, 32}], [{9, 2}, {9, 32}] }");
        }
    }

    Y_UNIT_TEST(StressGarbageCollection) {
        TDatabase DB;
        NIceDb::TNiceDb db(DB);

        size_t nodeSize = GetNodeSize();

        auto dumpCache = [&DB]() -> TString {
            return DumpCache(DB, 1);
        };

        {
            TDummyEnv env;
            DB.Begin(1, env);
            db.Materialize<Schema>();
            // tuned to exactly 4 nodes with 2 keys each in the pool
            DB.Alter().SetEraseCache(1, true, 2, nodeSize * 4 + sizeof(TCell) * 2 * 8);
            DB.Commit(1, true);
        }

        {
            TDummyEnv env;
            DB.Begin(2, env);
            auto table = db.Table<Schema::TestTable>();
            for (ui64 key1 = 1; key1 <= 64; ++key1) {
                for (ui64 key2 = 1; key2 <= 64; ++key2) {
                    table.Key(key1, key2).Delete();
                }
            }
            DB.Commit(2, true);
        }

        ui64 nextTxStamp = 3;

        {
            auto table = db.Table<Schema::TestTable>();

            for (ui64 key1 = 1; key1 <= 64; ++key1) {
                TDummyEnv env1;
                DB.Begin(nextTxStamp, env1);
                auto rowset1 = table.GreaterOrEqual(key1, 16).LessOrEqual(key1, 48).Select();
                UNIT_ASSERT(rowset1.IsReady());
                UNIT_ASSERT(rowset1.EndOfSet());
                DB.Commit(nextTxStamp++, true);

                TDummyEnv env2;
                DB.Begin(nextTxStamp, env2);
                auto rowset2 = table.Prefix(key1).Select();
                UNIT_ASSERT(rowset2.IsReady());
                UNIT_ASSERT(rowset2.EndOfSet());
                DB.Commit(nextTxStamp++, true);
            }

            UNIT_ASSERT_VALUES_EQUAL(dumpCache(), "TKeyRangeCache{ [{61, 1}, {61, 64}], [{62, 1}, {62, 64}], [{63, 1}, {63, 64}], [{64, 1}, {64, 64}] }");

            for (ui64 key1 = 64; key1 >= 1; --key1) {
                TDummyEnv env;
                DB.Begin(nextTxStamp, env);
                auto rowset = table.Prefix(key1).Select();
                UNIT_ASSERT(rowset.IsReady());
                UNIT_ASSERT(rowset.EndOfSet());
                DB.Commit(nextTxStamp++, true);
            }

            UNIT_ASSERT_VALUES_EQUAL(dumpCache(), "TKeyRangeCache{ [{1, 1}, {1, 64}], [{2, 1}, {2, 64}], [{3, 1}, {3, 64}], [{4, 1}, {4, 64}] }");

            size_t gc = DB.DebugGetTableErasedKeysCache(1)->Stats().GarbageCollections;
            UNIT_ASSERT_C(gc > 0, "Performed " << gc << " garbage collections");
        }
    }

    Y_UNIT_TEST(StressGarbageCollectionWithStrings) {
        TDatabase DB;
        NIceDb::TNiceDb db(DB);

        size_t nodeSize = GetNodeSize();

        auto dumpCache = [&DB]() -> TString {
            return DumpCache(DB, 2);
        };

        {
            TDummyEnv env;
            DB.Begin(1, env);
            db.Materialize<Schema>();
            // tuned to exactly 4 nodes with 2 keys each in the pool
            DB.Alter().SetEraseCache(2, true, 2, nodeSize * 4 + sizeof(TCell) * 2 * 8 + AlignUp(size_t(11)) * 8);
            DB.Commit(1, true);
        }

        {
            TDummyEnv env;
            DB.Begin(2, env);
            auto table = db.Table<Schema::TestTableStr>();
            char key2template[] = "foobarbaz00";
            for (ui64 key1 = 1; key1 <= 64; ++key1) {
                for (ui64 key2 = 1; key2 <= 64; ++key2) {
                    key2template[9] = '0' + (key2 / 10);
                    key2template[10] = '0' + (key2 % 10);
                    table.Key(key1, key2template).Delete();
                }
            }
            DB.Commit(2, true);
        }

        ui64 nextTxStamp = 3;

        {
            auto table = db.Table<Schema::TestTableStr>();

            for (ui64 key1 = 1; key1 <= 64; ++key1) {
                TDummyEnv env;
                DB.Begin(nextTxStamp, env);
                auto rowset = table.Prefix(key1).Select();
                UNIT_ASSERT(rowset.IsReady());
                UNIT_ASSERT(rowset.EndOfSet());
                DB.Commit(nextTxStamp++, true);
            }

            UNIT_ASSERT_VALUES_EQUAL(dumpCache(), "TKeyRangeCache{ [{61, foobarbaz01}, {61, foobarbaz64}], [{62, foobarbaz01}, {62, foobarbaz64}], [{63, foobarbaz01}, {63, foobarbaz64}], [{64, foobarbaz01}, {64, foobarbaz64}] }");

            for (ui64 key1 = 64; key1 >= 1; --key1) {
                TDummyEnv env;
                DB.Begin(nextTxStamp, env);
                auto rowset = table.Prefix(key1).Select();
                UNIT_ASSERT(rowset.IsReady());
                UNIT_ASSERT(rowset.EndOfSet());
                DB.Commit(nextTxStamp++, true);
            }

            UNIT_ASSERT_VALUES_EQUAL(dumpCache(), "TKeyRangeCache{ [{1, foobarbaz01}, {1, foobarbaz64}], [{2, foobarbaz01}, {2, foobarbaz64}], [{3, foobarbaz01}, {3, foobarbaz64}], [{4, foobarbaz01}, {4, foobarbaz64}] }");

            size_t gc = DB.DebugGetTableErasedKeysCache(2)->Stats().GarbageCollections;
            UNIT_ASSERT_C(gc > 0, "Performed " << gc << " garbage collections");
        }
    }
}

}
}
