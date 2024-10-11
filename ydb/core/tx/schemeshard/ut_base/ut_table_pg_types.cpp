#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSchemeShardPgTypesInTables) {

    Y_UNIT_TEST_FLAG(CreateTableWithPgTypeColumn, EnableTablePgTypes) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableTablePgTypes(EnableTablePgTypes));
        ui64 txId = 100;

        auto expectedResult = []() {
            if constexpr (EnableTablePgTypes) {
                return TExpectedResult(NKikimrScheme::StatusAccepted);
            } else {
                return TExpectedResult(NKikimrScheme::StatusSchemeError, "support for pg types is disabled (EnableTablePgTypes feature flag is off)");
            }
        }();

        AsyncCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "pgint4" }
            KeyColumnNames: ["key"]
        )");
        TestModificationResults(runtime, txId, {expectedResult});
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST_FLAG(AlterTableAddPgTypeColumn, EnableTablePgTypes) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableTablePgTypes(EnableTablePgTypes));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Uint8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto expectedResult = []() {
            if constexpr (EnableTablePgTypes) {
                return TExpectedResult(NKikimrScheme::StatusAccepted);
            } else {
                return TExpectedResult(NKikimrScheme::StatusInvalidParameter, "support for pg types is disabled (EnableTablePgTypes feature flag is off)");
            }
        }();

        AsyncAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "added" Type: "pgint4" }
        )");
        TestModificationResults(runtime, txId, {expectedResult});
        env.TestWaitNotification(runtime, txId);
    }
}
