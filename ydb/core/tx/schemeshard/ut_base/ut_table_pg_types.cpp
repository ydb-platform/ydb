#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSchemeShardPgTypesInTables) {

    Y_UNIT_TEST(CreateTableWithPgTypeColumnRejected) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        for (const TString type : {"pgbool", "pgint2", "pgint4", "pgint8", "pgfloat4", "pgfloat8", "pgtext", "pgvarchar", "pgnumeric", "pgdate", "pgtimestamp"}) {
            for (const bool pgKey : {false, true}) {
                TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                    Name: "Table1"
                    Columns { Name: "key" Type: "%s" }
                    Columns { Name: "value" Type: "%s" }
                    KeyColumnNames: ["key"]
                )", pgKey ? type.c_str() : "Uint64", pgKey ? "String" : type.c_str()),
                    {{NKikimrScheme::StatusSchemeError, "is not supported for table columns"}});
                TestLs(runtime, "/MyRoot/Table1", false, NLs::PathNotExist);
            }
        }

        // Rejected schemas must not leave the path unusable.
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(AlterTableAddPgTypeColumnRejected) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        for (const TString type : {"pgint2", "pgint4", "pgint8", "pgtext", "pgnumeric", "pgtimestamp"}) {
            TestAlterTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "Table1"
                Columns { Name: "added" Type: "%s" }
            )", type.c_str()), {{NKikimrScheme::StatusInvalidParameter, "is not supported for table columns"}});
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"), {
                NLs::CheckColumns("Table1", {"key", "value"}, {}, {"key"}, true),
            });
        }

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "added" Type: "Int32" }
        )");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"), {
            NLs::CheckColumns("Table1", {"key", "value", "added"}, {}, {"key"}, true),
        });
    }
}
