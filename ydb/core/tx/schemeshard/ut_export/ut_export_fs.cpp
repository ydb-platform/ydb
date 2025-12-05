#include <ydb/public/api/protos/ydb_export.pb.h>

#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/ut_backup_restore_common.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

#include <google/protobuf/text_format.h>

using namespace NSchemeShardUT_Private;

namespace {

    void Run(TTestBasicRuntime& runtime, TTestEnv& env, const TVector<TString>& tables, 
             const TString& request,
             Ydb::StatusIds::StatusCode expectedStatus = Ydb::StatusIds::SUCCESS) {
        
        ui64 txId = 100;

        for (const auto& table : tables) {
            TestCreateTable(runtime, ++txId, "/MyRoot", table);
            env.TestWaitNotification(runtime, txId);
        }

        runtime.SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::DATASHARD_RESTORE, NActors::NLog::PRI_TRACE);

        const auto initialStatus = expectedStatus == Ydb::StatusIds::PRECONDITION_FAILED
            ? expectedStatus
            : Ydb::StatusIds::SUCCESS;
        TestExport(runtime, ++txId, "/MyRoot", request, "", "", initialStatus);
        env.TestWaitNotification(runtime, txId);

        if (initialStatus != Ydb::StatusIds::SUCCESS) {
            return;
        }

        const ui64 exportId = txId;
        TestGetExport(runtime, exportId, "/MyRoot", expectedStatus);

        TestForgetExport(runtime, ++txId, "/MyRoot", exportId);
        env.TestWaitNotification(runtime, exportId);

        TestGetExport(runtime, exportId, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
    }

    class TFsExportFixture : public NUnitTest::TBaseFixture {
    public:
        void RunFs(const TVector<TString>& tables, const TString& basePath, const TString& destinationPath,
                   Ydb::StatusIds::StatusCode expectedStatus = Ydb::StatusIds::SUCCESS,
                   bool checkFsFilesExistence = true) {
            
            TString requestStr = Sprintf(R"(
                ExportToFsSettings {
                  base_path: "%s"
                  items {
                    source_path: "/MyRoot/%s"
                    destination_path: "%s"
                  }
                }
            )", basePath.c_str(), tables[0].Contains("Name:") ? ExtractTableName(tables[0]).c_str() : "Table", destinationPath.c_str());

            Env(); // Init test env
            Runtime().GetAppData().FeatureFlags.SetEnableChecksumsExport(true);
            Runtime().GetAppData().FeatureFlags.SetEnablePermissionsExport(true);

            Run(Runtime(), Env(), tables, requestStr, expectedStatus);

            if (expectedStatus == Ydb::StatusIds::SUCCESS && checkFsFilesExistence) {
                TFsPath exportPath = TFsPath(basePath) / destinationPath;
                
                // Check metadata file
                TFsPath metadataPath = exportPath / "metadata.json";
                UNIT_ASSERT_C(metadataPath.Exists(), "Metadata file should exist: " << metadataPath.GetPath());
                
                // Check scheme file
                TFsPath schemePath = exportPath / "scheme.pb";
                UNIT_ASSERT_C(schemePath.Exists(), "Scheme file should exist: " << schemePath.GetPath());
                
                // Check permissions file (if enabled)
                if (Runtime().GetAppData().FeatureFlags.GetEnablePermissionsExport()) {
                    TFsPath permissionsPath = exportPath / "permissions.pb";
                    UNIT_ASSERT_C(permissionsPath.Exists(), "Permissions file should exist: " << permissionsPath.GetPath());
                }
                
                // Check checksums (if enabled)
                if (Runtime().GetAppData().FeatureFlags.GetEnableChecksumsExport()) {
                    TFsPath metadataChecksumPath = exportPath / "metadata.json.sha256";
                    UNIT_ASSERT_C(metadataChecksumPath.Exists(), "Metadata checksum should exist: " << metadataChecksumPath.GetPath());
                    
                    TFsPath schemeChecksumPath = exportPath / "scheme.pb.sha256";
                    UNIT_ASSERT_C(schemeChecksumPath.Exists(), "Scheme checksum should exist: " << schemeChecksumPath.GetPath());
                }
            }
        }

        void RunFsMultiTable(const TVector<TString>& tables, const TString& basePath, const TVector<TString>& destinationPaths,
                             Ydb::StatusIds::StatusCode expectedStatus = Ydb::StatusIds::SUCCESS) {
            
            TStringBuilder items;
            for (size_t i = 0; i < tables.size(); ++i) {
                TString tableName = ExtractTableName(tables[i]);
                TString destPath = i < destinationPaths.size() ? destinationPaths[i] : ("backup/" + tableName);
                
                items << "items {"
                      << " source_path: \"/MyRoot/" << tableName << "\""
                      << " destination_path: \"" << destPath << "\""
                      << " }";
            }

            TString requestStr = Sprintf(R"(
                ExportToFsSettings {
                  base_path: "%s"
                  %s
                }
            )", basePath.c_str(), items.c_str());

            Env(); // Init test env
            Runtime().GetAppData().FeatureFlags.SetEnableChecksumsExport(true);
            Runtime().GetAppData().FeatureFlags.SetEnablePermissionsExport(true);

            Run(Runtime(), Env(), tables, requestStr, expectedStatus);

            if (expectedStatus == Ydb::StatusIds::SUCCESS) {
                for (size_t i = 0; i < destinationPaths.size(); ++i) {
                    TFsPath exportPath = TFsPath(basePath) / destinationPaths[i];
                    TFsPath schemePath = exportPath / "scheme.pb";
                    UNIT_ASSERT_C(schemePath.Exists(), "Scheme file should exist for table " << i << ": " << schemePath.GetPath());
                }
            }
        }

        bool HasFsFile(const TString& basePath, const TString& relativePath) {
            TFsPath filePath = TFsPath(basePath) / relativePath;
            return filePath.Exists();
        }

        TString GetFsFileContent(const TString& basePath, const TString& relativePath) {
            TFsPath filePath = TFsPath(basePath) / relativePath;
            if (filePath.Exists()) {
                TFileInput file(filePath.GetPath());
                return file.ReadAll();
            }
            return {};
        }

    protected:
        TTestBasicRuntime& Runtime() {
            if (!TestRuntime) {
                TestRuntime.ConstructInPlace();
            }
            return *TestRuntime;
        }

        TTestEnvOptions& EnvOptions() {
            if (!TestEnvOptions) {
                TestEnvOptions.ConstructInPlace();
            }
            return *TestEnvOptions;
        }

        TTestEnv& Env() {
            if (!TestEnv) {
                TestEnv.ConstructInPlace(Runtime(), EnvOptions());
            }
            return *TestEnv;
        }

        TTempDir& TempDir() {
            if (!TestTempDir) {
                TestTempDir.ConstructInPlace();
            }
            return *TestTempDir;
        }

    private:
        static TString ExtractTableName(const TString& tableSchema) {
            // Extract "Name: "Table"" from schema
            size_t pos = tableSchema.find("Name:");
            if (pos == TString::npos) {
                return "Table";
            }
            pos = tableSchema.find('"', pos);
            if (pos == TString::npos) {
                return "Table";
            }
            size_t endPos = tableSchema.find('"', pos + 1);
            if (endPos == TString::npos) {
                return "Table";
            }
            return tableSchema.substr(pos + 1, endPos - pos - 1);
        }

        TMaybe<TTestBasicRuntime> TestRuntime;
        TMaybe<TTestEnvOptions> TestEnvOptions;
        TMaybe<TTestEnv> TestEnv;
        TMaybe<TTempDir> TestTempDir;
    };

} // anonymous

Y_UNIT_TEST_SUITE_F(TExportToFsTests, TFsExportFixture) {
    Y_UNIT_TEST(ShouldSucceedOnSingleShardTable) {
        TString basePath = TempDir().Path();
        
        RunFs({
            R"(
                Name: "Table"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )",
        }, basePath, "backup/Table");
    }

    Y_UNIT_TEST(ShouldSucceedOnMultiShardTable) {
        TString basePath = TempDir().Path();
        
        RunFs({
            R"(
                Name: "Table"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 2
            )",
        }, basePath, "backup/MultiShardTable");
    }

    Y_UNIT_TEST(ShouldSucceedOnManyTables) {
        TString basePath = TempDir().Path();
        
        RunFsMultiTable({
            R"(
                Name: "Table1"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )",
            R"(
                Name: "Table2"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )",
        }, basePath, {"backup/Table1", "backup/Table2"});
    }

    Y_UNIT_TEST(ShouldCheckFilesCreatedOnDisk) {
        TString basePath = TempDir().Path();
        TString destinationPath = "backup/TestTable";
        
        RunFs({
            R"(
                Name: "TestTable"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )",
        }, basePath, destinationPath);

        // Check all expected files exist
        UNIT_ASSERT_C(HasFsFile(basePath, destinationPath + "/metadata.json"), "metadata.json");
        UNIT_ASSERT_C(HasFsFile(basePath, destinationPath + "/scheme.pb"), "scheme.pb");
        UNIT_ASSERT_C(HasFsFile(basePath, destinationPath + "/permissions.pb"), "permissions.pb");
        UNIT_ASSERT_C(HasFsFile(basePath, destinationPath + "/metadata.json.sha256"), "metadata.json.sha256");
        UNIT_ASSERT_C(HasFsFile(basePath, destinationPath + "/scheme.pb.sha256"), "scheme.pb.sha256");
        UNIT_ASSERT_C(HasFsFile(basePath, destinationPath + "/permissions.pb.sha256"), "permissions.pb.sha256");
        
        // Check scheme content
        TString schemeContent = GetFsFileContent(basePath, destinationPath + "/scheme.pb");
        UNIT_ASSERT_C(!schemeContent.empty(), "Scheme file should not be empty");
        
        Ydb::Table::CreateTableRequest schemeProto;
        UNIT_ASSERT_C(google::protobuf::TextFormat::ParseFromString(schemeContent, &schemeProto),
                     "Should parse scheme protobuf");
        
        UNIT_ASSERT_VALUES_EQUAL(schemeProto.columns_size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(schemeProto.columns(0).name(), "key");
        UNIT_ASSERT_VALUES_EQUAL(schemeProto.columns(1).name(), "value");
        UNIT_ASSERT_VALUES_EQUAL(schemeProto.primary_key_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(schemeProto.primary_key(0), "key");
        
        // Check checksum format
        TString checksumContent = GetFsFileContent(basePath, destinationPath + "/metadata.json.sha256");
        UNIT_ASSERT_C(!checksumContent.empty(), "Checksum should not be empty");
        UNIT_ASSERT_C(checksumContent.Contains("metadata.json"), "Checksum should contain filename");
        UNIT_ASSERT_GE(checksumContent.size(), 64); // sha256 is 64 hex chars
    }

    Y_UNIT_TEST(ShouldAcceptCompressionSettings) {
        TString basePath = TempDir().Path();
        TString destinationPath = "backup/Table";
        ui64 txId = 100;

        Env();
        Runtime().GetAppData().FeatureFlags.SetEnableChecksumsExport(true);
        Runtime().GetAppData().FeatureFlags.SetEnablePermissionsExport(true);

        TestCreateTable(Runtime(), ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        Env().TestWaitNotification(Runtime(), txId);

        TString request = Sprintf(R"(
            ExportToFsSettings {
              base_path: "%s"
              compression: "zstd-3"
              items {
                source_path: "/MyRoot/Table"
                destination_path: "%s"
              }
            }
        )", basePath.c_str(), destinationPath.c_str());

        TestExport(Runtime(), ++txId, "/MyRoot", request);
        Env().TestWaitNotification(Runtime(), txId);

        auto response = TestGetExport(Runtime(), txId, "/MyRoot");
        UNIT_ASSERT(response.GetResponse().GetEntry().HasExportToFsSettings());
        
        const auto& settings = response.GetResponse().GetEntry().GetExportToFsSettings();
        UNIT_ASSERT_VALUES_EQUAL(settings.compression(), "zstd-3");

        // Check that files exist on filesystem
        UNIT_ASSERT_C(HasFsFile(basePath, destinationPath + "/metadata.json"), 
                      "metadata.json should exist");
        UNIT_ASSERT_C(HasFsFile(basePath, destinationPath + "/scheme.pb"), 
                      "scheme.pb should exist");
        UNIT_ASSERT_C(HasFsFile(basePath, destinationPath + "/permissions.pb"), 
                      "permissions.pb should exist");
        
        // Check checksums exist
        UNIT_ASSERT_C(HasFsFile(basePath, destinationPath + "/metadata.json.sha256"), 
                      "metadata.json.sha256 should exist");
        UNIT_ASSERT_C(HasFsFile(basePath, destinationPath + "/scheme.pb.sha256"), 
                      "scheme.pb.sha256 should exist");
        UNIT_ASSERT_C(HasFsFile(basePath, destinationPath + "/permissions.pb.sha256"), 
                      "permissions.pb.sha256 should exist");

        TString schemeContent = GetFsFileContent(basePath, destinationPath + "/scheme.pb");
        UNIT_ASSERT_C(!schemeContent.empty(), "Scheme file should not be empty");
        
        Ydb::Table::CreateTableRequest schemeProto;
        UNIT_ASSERT_C(google::protobuf::TextFormat::ParseFromString(schemeContent, &schemeProto),
                     "Should parse scheme protobuf");
        
        UNIT_ASSERT_VALUES_EQUAL(schemeProto.columns_size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(schemeProto.primary_key_size(), 1);
    }

    Y_UNIT_TEST(ShouldFailOnNonExistentPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestExport(runtime, ++txId, "/MyRoot", R"(
            ExportToFsSettings {
              base_path: "/tmp/ydb_export"
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
              base_path: "/tmp/ydb_export"
              items {
                source_path: "/MyRoot/TableToDelete"
                destination_path: "backup/Table"
              }
            }
        )", "", "", Ydb::StatusIds::BAD_REQUEST);
    }

    Y_UNIT_TEST(ShouldHandleNestedPaths) {
        TString basePath = TempDir().Path();
        
        RunFs({
            R"(
                Name: "Table"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )",
        }, basePath, "deep/nested/directory/structure/backup");
    }
}
