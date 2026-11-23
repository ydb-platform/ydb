#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <util/generic/algorithm.h>

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

    Y_UNIT_TEST(ExternalTablePgColumnsRespectCompatibilityFlag) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableTablePgTypes(true).EnableReplaceIfExistsForExternalEntities(true));
        ui64 txId = 100;
        const auto updatePgCompatibility = [&](bool disabled) {
            for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
                auto& appData = runtime.GetAppData(node);
                NKikimrConfig::TFeatureFlags flags = appData.FeatureFlags;
                flags.SetDisablePgCompatibility(disabled);
                appData.UpdateRuntimeFlags(flags);
            }
        };

        TestCreateExternalDataSource(runtime, ++txId, "/MyRoot", R"(
            Name: "ExternalDataSource"
            SourceType: "ObjectStorage"
            Location: "https://s3.cloud.net/my_bucket"
            Auth { None {} }
        )", {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);

        const TString table = R"(
            Name: "ExternalTable"
            SourceType: "General"
            DataSourcePath: "/MyRoot/ExternalDataSource"
            Location: "/"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "pgint4" }
        )";
        const TVector<TExpectedResult> rejected{{NKikimrScheme::StatusSchemeError,
            "PostgreSQL compatibility is disabled for column 'value' of type 'pgint4' (DisablePgCompatibility feature flag is on)"}};

        updatePgCompatibility(true);
        TestCreateExternalTable(runtime, ++txId, "/MyRoot", table, rejected);
        TestLs(runtime, "/MyRoot/ExternalTable", false, NLs::PathNotExist);

        updatePgCompatibility(false);
        TestCreateExternalTable(runtime, ++txId, "/MyRoot", table, {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);
        const auto original = DescribePath(runtime, "/MyRoot/ExternalTable");
        TestDescribeResult(original, {NLs::Finished, NLs::IsExternalTable});
        const auto& description = original.GetPathDescription().GetExternalTableDescription();
        UNIT_ASSERT_VALUES_EQUAL(description.ColumnsSize(), 2);
        UNIT_ASSERT(AnyOf(description.GetColumns(), [](const auto& column) {
            return column.GetName() == "value" && column.GetTypeId() == NScheme::NTypeIds::Pg;
        }));

        updatePgCompatibility(true);
        TestCreateExternalTableOrReplace(runtime, ++txId, "/MyRoot", table, rejected);
        const auto after = DescribePath(runtime, "/MyRoot/ExternalTable");
        TestDescribeResult(after, {NLs::Finished, NLs::IsExternalTable});
        UNIT_ASSERT_VALUES_EQUAL(after.GetPathDescription().GetExternalTableDescription().DebugString(), description.DebugString());

        TestCreateExternalTableOrReplace(runtime, ++txId, "/MyRoot", R"(
            Name: "ExternalTable"
            SourceType: "General"
            DataSourcePath: "/MyRoot/ExternalDataSource"
            Location: "/"
            Columns { Name: "key" Type: "Uint64" }
        )", {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);
        const auto migrated = DescribePath(runtime, "/MyRoot/ExternalTable");
        TestDescribeResult(migrated, {NLs::Finished, NLs::IsExternalTable});
        const auto& migratedDescription = migrated.GetPathDescription().GetExternalTableDescription();
        UNIT_ASSERT_VALUES_EQUAL(migratedDescription.ColumnsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(migratedDescription.GetColumns(0).GetName(), "key");
        UNIT_ASSERT_VALUES_EQUAL(migratedDescription.GetColumns(0).GetTypeId(), NScheme::NTypeIds::Uint64);

        const auto nativeTable = [](const char* location) {
            return Sprintf(R"(
                Name: "NativeExternalTable"
                SourceType: "General"
                DataSourcePath: "/MyRoot/ExternalDataSource"
                Location: "%s"
                Columns { Name: "value" Type: "Uint64" }
            )", location);
        };
        TestCreateExternalTable(runtime, ++txId, "/MyRoot", nativeTable("/"), {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/NativeExternalTable"), {NLs::Finished, NLs::IsExternalTable});

        TestCreateExternalTableOrReplace(runtime, ++txId, "/MyRoot", nativeTable("/new_location"), {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);
        const auto replaced = DescribePath(runtime, "/MyRoot/NativeExternalTable");
        TestDescribeResult(replaced, {NLs::Finished, NLs::IsExternalTable});
        UNIT_ASSERT_VALUES_EQUAL(replaced.GetPathDescription().GetExternalTableDescription().GetLocation(), "/new_location");
    }
}
