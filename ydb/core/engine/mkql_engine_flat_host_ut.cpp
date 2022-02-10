#include "mkql_engine_flat_host.h"
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/core/scheme_types/scheme_type_registry.h>
#include <ydb/core/scheme_types/scheme_types_defs.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/tablet_flat/test/libs/table/test_dummy.h>

namespace NKikimr {
namespace NMiniKQL {

using namespace NTabletFlatExecutor;

Y_UNIT_TEST_SUITE(TMiniKQLEngineFlatHostTest) {

    struct Schema : NIceDb::Schema {
        struct TestTable : Table<1> {
            struct ID : Column<1, NUdf::TDataType<ui64>::Id> {};
            struct Value : Column<2, NUdf::TDataType<ui64>::Id> {};
            struct Name : Column<3, NUdf::TDataType<NUdf::TUtf8>::Id> {};
            struct BoolValue : Column<4, NUdf::TDataType<bool>::Id> {};

            using TKey = TableKey<ID>;
            using TColumns = TableColumns<ID, Value, Name, BoolValue>;
        };

        struct TestTable2 : Table<2> {
            struct ID1 : Column<1, NUdf::TDataType<ui64>::Id> {};
            struct ID2 : Column<2, NUdf::TDataType<ui64>::Id> {};

            using TKey = TableKey<ID1, ID2>;
            using TColumns = TableColumns<ID1, ID2>;
        };

        using TTables = SchemaTables<TestTable, TestTable2>;
    };


    Y_UNIT_TEST(ShardId) {
        NTable::TDatabase DB;
        TEngineHostCounters hostCounters;
        TUnversionedEngineHost host(DB, hostCounters, TEngineHostSettings(100));
        UNIT_ASSERT_VALUES_EQUAL(host.GetShardId(), 100);
    }

    Y_UNIT_TEST(Basic) {
        NTable::TDatabase DB;
        NIceDb::TNiceDb db(DB);

        { // Create tables
            NTable::TDummyEnv env;
            DB.Begin(1, env);
            db.Materialize<Schema>();
            DB.Commit(1, true);
        }

        { // Fill tables with some stuff
            NTable::TDummyEnv env;
            DB.Begin(2, env);
            for (ui64 i = 0; i < 1000; ++i) {
                db.Table<Schema::TestTable>().Key(i).Update(NIceDb::TUpdate<Schema::TestTable::Value>(i),
                                                            NIceDb::TUpdate<Schema::TestTable::Name>(ToString(i)),
                                                            NIceDb::TUpdate<Schema::TestTable::BoolValue>(i % 2 == 0));
            }
            DB.Commit(2, true);
        }

        { // Execute some minikql
            NTable::TDummyEnv env;
            DB.Begin(3, env);
            TEngineHostCounters hostCounters;
            TUnversionedEngineHost host(DB, hostCounters);

            // TODO: ... MINIKQL ...

            Y_UNUSED(host);
            DB.Commit(3, true);
        }

        { // Check data
            NTable::TDummyEnv env;
            DB.Begin(4, env);
            for (ui64 i = 0; i < 1000; ++i) {
                auto row = db.Table<Schema::TestTable>().Key(i).Select<Schema::TestTable::Value, Schema::TestTable::Name, Schema::TestTable::BoolValue>();
                UNIT_ASSERT(row.IsReady());
                UNIT_ASSERT(row.IsValid());
                ui64 value = row.GetValue<Schema::TestTable::Value>();
                TString name = row.GetValue<Schema::TestTable::Name>();
                bool boolValue = row.GetValue<Schema::TestTable::BoolValue>();
                UNIT_ASSERT(value == i);
                UNIT_ASSERT(ToString(value) == name);
                UNIT_ASSERT(boolValue == (i % 2 == 0));
            }
            DB.Commit(4, true);
        }
    }
}

}}
