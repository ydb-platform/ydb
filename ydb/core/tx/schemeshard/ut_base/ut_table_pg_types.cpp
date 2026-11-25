#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSchemeShardPgTypesInTables) {

    Y_UNIT_TEST(PgCompatibilityChangesForNewOperations) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableTablePgTypes(true));
        ui64 txId = 100;
        const TString schema = R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "pgint4" }
            KeyColumnNames: ["key"]
        )";
        const auto updatePgCompatibility = [&](bool disabled) {
            for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
                auto& appData = runtime.GetAppData(node);
                NKikimrConfig::TFeatureFlags flags = appData.FeatureFlags;
                flags.SetDisablePgCompatibility(disabled);
                appData.UpdateRuntimeFlags(flags);
            }
        };

        updatePgCompatibility(true);
        AsyncCreateTable(runtime, ++txId, "/MyRoot", schema);
        TestModificationResults(runtime, txId, {
            {NKikimrScheme::StatusSchemeError, "PostgreSQL compatibility is disabled"}});
        env.TestWaitNotification(runtime, txId);

        updatePgCompatibility(false);
        TestCreateTable(runtime, ++txId, "/MyRoot", schema);
        env.TestWaitNotification(runtime, txId);

        updatePgCompatibility(true);
        AsyncAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "another" Type: "pgint4" }
        )");
        TestModificationResults(runtime, txId, {
            {NKikimrScheme::StatusInvalidParameter, "PostgreSQL compatibility is disabled"}});
        env.TestWaitNotification(runtime, txId);

        AsyncAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            TTLSettings {
                Enabled { ColumnName: "value" ExpireAfterSeconds: 3600 ColumnUnit: UNIT_SECONDS }
            }
        )");
        TestModificationResults(runtime, txId, {
            {NKikimrScheme::StatusInvalidParameter, "PostgreSQL compatibility is disabled"}});
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            DropColumns { Name: "value" }
        )");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(IndexBuildWithInheritedPgKey) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableTablePgTypes(true).DisablePgCompatibility(false));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "pgint4" }
            Columns { Name: "native" Type: "Uint64" }
            Columns { Name: "legacy" Type: "pgint4" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
            runtime.GetAppData(node).FeatureFlags.SetDisablePgCompatibility(true);
        }

        const auto buildIndex = [&](const TBuildIndexConfig& config) {
            TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table1", config);
            env.TestWaitNotification(runtime, txId);
            return TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", txId).GetIndexBuild();
        };
        const auto native = buildIndex({"native_index", NKikimrSchemeOp::EIndexTypeGlobal, {"native"}, {}, {}});
        UNIT_ASSERT_VALUES_EQUAL_C(native.GetState(), Ydb::Table::IndexBuildState::STATE_DONE, native.DebugString());

        const auto pgKey = buildIndex({"pg_key", NKikimrSchemeOp::EIndexTypeGlobal, {"key", "native"}, {}, {}});
        UNIT_ASSERT_VALUES_EQUAL_C(pgKey.GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED, pgKey.DebugString());
        UNIT_ASSERT_C(pgKey.IssuesSize() > 0, pgKey.DebugString());
        UNIT_ASSERT_STRING_CONTAINS(pgKey.GetIssues(0).message(), "PostgreSQL compatibility is disabled for index column 'key'");

        const auto pgData = buildIndex({"pg_data", NKikimrSchemeOp::EIndexTypeGlobal, {"native"}, {"legacy"}, {}});
        UNIT_ASSERT_VALUES_EQUAL_C(pgData.GetState(), Ydb::Table::IndexBuildState::STATE_REJECTED, pgData.DebugString());
        UNIT_ASSERT_C(pgData.IssuesSize() > 0, pgData.DebugString());
        UNIT_ASSERT_STRING_CONTAINS(pgData.GetIssues(0).message(), "PostgreSQL compatibility is disabled for index column 'legacy'");
    }

    Y_UNIT_TEST_FLAG(CreateTableWithPgTypeColumn, EnableTablePgTypes) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableTablePgTypes(EnableTablePgTypes).DisablePgCompatibility(false));
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
        TTestEnv env(runtime, TTestEnvOptions().EnableTablePgTypes(EnableTablePgTypes).DisablePgCompatibility(false));
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
