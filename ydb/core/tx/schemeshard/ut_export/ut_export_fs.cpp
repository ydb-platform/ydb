#include <ydb/public/api/protos/ydb_export.pb.h>

#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSchemeShardExportToFsTests) {
    Y_UNIT_TEST(ShouldSucceedCreateExportToFs) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Test that schemeshard accepts ExportToFsSettings
        TestExport(runtime, ++txId, "/MyRoot", R"(
            ExportToFsSettings {
              base_path: "/mnt/exports"
              items {
                source_path: "/MyRoot/Table"
                destination_path: "backup/Table"
              }
            }
        )");

        // Check that export was created
        auto response = TestGetExport(runtime, txId, "/MyRoot");
        UNIT_ASSERT(response.GetResponse().GetEntry().HasExportToFsSettings());
        
        const auto& settings = response.GetResponse().GetEntry().GetExportToFsSettings();
        UNIT_ASSERT_VALUES_EQUAL(settings.base_path(), "/mnt/exports");
        UNIT_ASSERT_VALUES_EQUAL(settings.items_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(settings.items(0).source_path(), "/MyRoot/Table");
        UNIT_ASSERT_VALUES_EQUAL(settings.items(0).destination_path(), "backup/Table");
    }

    Y_UNIT_TEST(ShouldAcceptCompressionForFs) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestExport(runtime, ++txId, "/MyRoot", R"(
            ExportToFsSettings {
              base_path: "/mnt/exports"
              compression: "zstd-3"
              items {
                source_path: "/MyRoot/Table"
                destination_path: "backup/Table"
              }
            }
        )");

        auto response = TestGetExport(runtime, txId, "/MyRoot");
        UNIT_ASSERT(response.GetResponse().GetEntry().HasExportToFsSettings());
        
        const auto& settings = response.GetResponse().GetEntry().GetExportToFsSettings();
        UNIT_ASSERT_VALUES_EQUAL(settings.compression(), "zstd-3");
    }

    Y_UNIT_TEST(ShouldFailOnNonExistentPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestExport(runtime, ++txId, "/MyRoot", R"(
            ExportToFsSettings {
              base_path: "/mnt/exports"
              items {
                source_path: "/MyRoot/NonExistentTable"
                destination_path: "backup/Table"
              }
            }
        )", "", "", Ydb::StatusIds::BAD_REQUEST);
    }

    Y_UNIT_TEST(ShouldFailOnDeletedPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TableToDelete"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestDropTable(runtime, ++txId, "/MyRoot", "TableToDelete");
        env.TestWaitNotification(runtime, txId);

        TestExport(runtime, ++txId, "/MyRoot", R"(
            ExportToFsSettings {
              base_path: "/mnt/exports"
              items {
                source_path: "/MyRoot/TableToDelete"
                destination_path: "backup/Table"
              }
            }
        )", "", "", Ydb::StatusIds::BAD_REQUEST);
    }

    Y_UNIT_TEST(FsExportWithMultipleTables) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table2"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestExport(runtime, ++txId, "/MyRoot", R"(
            ExportToFsSettings {
              base_path: "/mnt/exports"
              items {
                source_path: "/MyRoot/Table1"
                destination_path: "backup/Table1"
              }
              items {
                source_path: "/MyRoot/Table2"
                destination_path: "backup/Table2"
              }
            }
        )");

        auto response = TestGetExport(runtime, txId, "/MyRoot");
        UNIT_ASSERT(response.GetResponse().GetEntry().HasExportToFsSettings());
        
        const auto& settings = response.GetResponse().GetEntry().GetExportToFsSettings();
        UNIT_ASSERT_VALUES_EQUAL(settings.items_size(), 2);
    }
}
