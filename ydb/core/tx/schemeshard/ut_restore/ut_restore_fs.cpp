#include <ydb/public/api/protos/ydb_import.pb.h>

#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSchemeShardImportFromFsTests) {
    Y_UNIT_TEST(ShouldSucceedCreateImportFromFs) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // Test that schemeshard accepts ImportFromFsSettings
        TestImport(runtime, ++txId, "/MyRoot", R"(
            ImportFromFsSettings {
              base_path: "/mnt/backups"
              items {
                source_path: "backup/Table"
                destination_path: "/MyRoot/RestoredTable"
              }
            }
        )");

        // Check that import was created
        auto response = TestGetImport(runtime, txId, "/MyRoot");
        UNIT_ASSERT(response.GetResponse().GetEntry().HasImportFromFsSettings());
        
        const auto& settings = response.GetResponse().GetEntry().GetImportFromFsSettings();
        UNIT_ASSERT_VALUES_EQUAL(settings.base_path(), "/mnt/backups");
        UNIT_ASSERT_VALUES_EQUAL(settings.items_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(settings.items(0).source_path(), "backup/Table");
        UNIT_ASSERT_VALUES_EQUAL(settings.items(0).destination_path(), "/MyRoot/RestoredTable");
    }

    Y_UNIT_TEST(ShouldAcceptNoAclForFs) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestImport(runtime, ++txId, "/MyRoot", R"(
            ImportFromFsSettings {
              base_path: "/mnt/backups"
              no_acl: true
              items {
                source_path: "backup/Table"
                destination_path: "/MyRoot/RestoredTable"
              }
            }
        )");

        auto response = TestGetImport(runtime, txId, "/MyRoot");
        UNIT_ASSERT(response.GetResponse().GetEntry().HasImportFromFsSettings());
        
        const auto& settings = response.GetResponse().GetEntry().GetImportFromFsSettings();
        UNIT_ASSERT_VALUES_EQUAL(settings.no_acl(), true);
    }

    Y_UNIT_TEST(ShouldAcceptSkipChecksumValidation) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestImport(runtime, ++txId, "/MyRoot", R"(
            ImportFromFsSettings {
              base_path: "/mnt/backups"
              skip_checksum_validation: true
              items {
                source_path: "backup/Table"
                destination_path: "/MyRoot/RestoredTable"
              }
            }
        )");

        auto response = TestGetImport(runtime, txId, "/MyRoot");
        UNIT_ASSERT(response.GetResponse().GetEntry().HasImportFromFsSettings());
        
        const auto& settings = response.GetResponse().GetEntry().GetImportFromFsSettings();
        UNIT_ASSERT_VALUES_EQUAL(settings.skip_checksum_validation(), true);
    }

    Y_UNIT_TEST(ShouldFailOnInvalidDestinationPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // Invalid destination path (empty) should fail validation
        TestImport(runtime, ++txId, "/MyRoot", R"(
            ImportFromFsSettings {
              base_path: "/mnt/backups"
              items {
                source_path: "backup/Table"
                destination_path: ""
              }
            }
        )", "", "", Ydb::StatusIds::BAD_REQUEST);
    }

    Y_UNIT_TEST(ShouldFailOnDuplicateDestination) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // Duplicate destination paths should fail validation
        TestImport(runtime, ++txId, "/MyRoot", R"(
            ImportFromFsSettings {
              base_path: "/mnt/backups"
              items {
                source_path: "backup/Table1"
                destination_path: "/MyRoot/SameName"
              }
              items {
                source_path: "backup/Table2"
                destination_path: "/MyRoot/SameName"
              }
            }
        )", "", "", Ydb::StatusIds::BAD_REQUEST);
    }

    Y_UNIT_TEST(FsImportWithMultipleTables) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestImport(runtime, ++txId, "/MyRoot", R"(
            ImportFromFsSettings {
              base_path: "/mnt/backups"
              items {
                source_path: "backup/Table1"
                destination_path: "/MyRoot/RestoredTable1"
              }
              items {
                source_path: "backup/Table2"
                destination_path: "/MyRoot/RestoredTable2"
              }
            }
        )");

        auto response = TestGetImport(runtime, txId, "/MyRoot");
        UNIT_ASSERT(response.GetResponse().GetEntry().HasImportFromFsSettings());
        
        const auto& settings = response.GetResponse().GetEntry().GetImportFromFsSettings();
        UNIT_ASSERT_VALUES_EQUAL(settings.items_size(), 2);
    }
}


