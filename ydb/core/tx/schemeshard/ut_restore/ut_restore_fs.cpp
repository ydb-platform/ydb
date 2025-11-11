#include <ydb/public/api/protos/ydb_import.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/lib/ydb_cli/dump/files/files.h>

#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/backup/common/metadata.h>

#include <library/cpp/json/json_writer.h>
#include <util/folder/dirut.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/system/fs.h>

using namespace NSchemeShardUT_Private;

namespace {

// Helper class to create temporary backup files for tests
class TTempBackupFiles {
public:
    explicit TTempBackupFiles()
    {
    }

    const TString& GetBasePath() const {
        return TempDir.Name();
    }

    void CreateTableBackup(const TString& tablePath, const TString& tableName) {
        const TString fullPath = TempDir.Name() + "/" + tablePath;
        MakePathIfNotExist(fullPath.c_str());

        // Create metadata.json
        CreateMetadataFile(fullPath);

        // Create scheme.pb (table schema)
        CreateTableSchemeFile(fullPath, tableName);

        // Create permissions.pb (optional, but include it)
        CreatePermissionsFile(fullPath);
    }

private:
    void CreateMetadataFile(const TString& dirPath) {
        NBackup::TMetadata metadata;
        metadata.SetVersion(1);
        metadata.SetEnablePermissions(false);

        TString serialized = metadata.Serialize();

        TFileOutput file(dirPath + "/metadata.json");
        file << serialized;
    }

    void CreateTableSchemeFile(const TString& dirPath, const TString& tableName) {
        Ydb::Table::CreateTableRequest table;
        table.set_path(tableName);

        // Add simple columns
        auto* col1 = table.add_columns();
        col1->set_name("key");
        col1->mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);

        auto* col2 = table.add_columns();
        col2->set_name("value");
        col2->mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);

        // Set primary key
        table.add_primary_key("key");

        TString serialized;
        Y_ABORT_UNLESS(table.SerializeToString(&serialized));

        TFileOutput file(dirPath + "/" + NYdb::NDump::NFiles::TableScheme().FileName);
        file.Write(serialized);
    }

    void CreatePermissionsFile(const TString& dirPath) {
        Ydb::Scheme::ModifyPermissionsRequest permissions;

        TString serialized;
        Y_ABORT_UNLESS(permissions.SerializeToString(&serialized));

        TFileOutput file(dirPath + "/permissions.pb");
        file.Write(serialized);
    }

    TTempDir TempDir;
};

} // namespace

Y_UNIT_TEST_SUITE(TSchemeShardImportFromFsTests) {
    Y_UNIT_TEST(ShouldSucceedCreateImportFromFs) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // Create temporary backup files
        TTempBackupFiles backup;
        backup.CreateTableBackup("backup/Table", "Table");

        // Test that schemeshard accepts ImportFromFsSettings
        TString importSettings = Sprintf(R"(
            ImportFromFsSettings {
              base_path: "%s"
              items {
                source_path: "backup/Table"
                destination_path: "/MyRoot/RestoredTable"
              }
            }
        )", backup.GetBasePath().c_str());

        TestImport(runtime, ++txId, "/MyRoot", importSettings);

        // Check that import was created
        auto response = TestGetImport(runtime, txId, "/MyRoot");
        UNIT_ASSERT(response.GetResponse().GetEntry().HasImportFromFsSettings());
        
        const auto& settings = response.GetResponse().GetEntry().GetImportFromFsSettings();
        UNIT_ASSERT_VALUES_EQUAL(settings.base_path(), backup.GetBasePath());
        UNIT_ASSERT_VALUES_EQUAL(settings.items_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(settings.items(0).source_path(), "backup/Table");
        UNIT_ASSERT_VALUES_EQUAL(settings.items(0).destination_path(), "/MyRoot/RestoredTable");
    }

    Y_UNIT_TEST(ShouldAcceptNoAclForFs) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // Create temporary backup files
        TTempBackupFiles backup;
        backup.CreateTableBackup("backup/Table", "Table");

        TString importSettings = Sprintf(R"(
            ImportFromFsSettings {
              base_path: "%s"
              no_acl: true
              items {
                source_path: "backup/Table"
                destination_path: "/MyRoot/RestoredTable"
              }
            }
        )", backup.GetBasePath().c_str());

        TestImport(runtime, ++txId, "/MyRoot", importSettings);

        auto response = TestGetImport(runtime, txId, "/MyRoot");
        UNIT_ASSERT(response.GetResponse().GetEntry().HasImportFromFsSettings());
        
        const auto& settings = response.GetResponse().GetEntry().GetImportFromFsSettings();
        UNIT_ASSERT_VALUES_EQUAL(settings.no_acl(), true);
    }

    Y_UNIT_TEST(ShouldAcceptSkipChecksumValidation) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // Create temporary backup files
        TTempBackupFiles backup;
        backup.CreateTableBackup("backup/Table", "Table");

        TString importSettings = Sprintf(R"(
            ImportFromFsSettings {
              base_path: "%s"
              skip_checksum_validation: true
              items {
                source_path: "backup/Table"
                destination_path: "/MyRoot/RestoredTable"
              }
            }
        )", backup.GetBasePath().c_str());

        TestImport(runtime, ++txId, "/MyRoot", importSettings);

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

        // Create temporary backup files
        TTempBackupFiles backup;
        backup.CreateTableBackup("backup/Table1", "Table1");
        backup.CreateTableBackup("backup/Table2", "Table2");

        TString importSettings = Sprintf(R"(
            ImportFromFsSettings {
              base_path: "%s"
              items {
                source_path: "backup/Table1"
                destination_path: "/MyRoot/RestoredTable1"
              }
              items {
                source_path: "backup/Table2"
                destination_path: "/MyRoot/RestoredTable2"
              }
            }
        )", backup.GetBasePath().c_str());

        TestImport(runtime, ++txId, "/MyRoot", importSettings);

        auto response = TestGetImport(runtime, txId, "/MyRoot");
        UNIT_ASSERT(response.GetResponse().GetEntry().HasImportFromFsSettings());
        
        const auto& settings = response.GetResponse().GetEntry().GetImportFromFsSettings();
        UNIT_ASSERT_VALUES_EQUAL(settings.items_size(), 2);
    }
}


