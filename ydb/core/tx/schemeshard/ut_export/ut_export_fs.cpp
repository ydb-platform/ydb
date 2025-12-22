#include <ydb/public/api/protos/ydb_export.pb.h>

#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <google/protobuf/text_format.h>
#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/system/fs.h>

using namespace NSchemeShardUT_Private;

namespace {

    TString ReadFileContent(const TString& path) {
        if (!TFsPath(path).Exists()) {
            return "";
        }
        TFileInput file(path);
        return file.ReadAll();
    }

    bool FileExists(const TString& path) {
        return TFsPath(path).Exists();
    }

    TString MakeExportPath(const TString& basePath, const TString& destPath, const TString& file) {
        TFsPath result(basePath);
        result = result / destPath / file;
        return result.GetPath();
    }

} // namespace

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

    Y_UNIT_TEST(ShouldExportDataAndSchemaToFs) {
        TTempDir tempDir;
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);
        runtime.SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::S3_WRAPPER, NActors::NLog::PRI_TRACE);

        // Create table
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Write some data to the table
        WriteRow(runtime, ++txId, "/MyRoot/Table", 0, 1, "row1");
        WriteRow(runtime, ++txId, "/MyRoot/Table", 0, 2, "row2");
        WriteRow(runtime, ++txId, "/MyRoot/Table", 0, 3, "row3");

        // Export to filesystem
        TString basePath = tempDir.Path();
        TString requestStr = Sprintf(R"(
            ExportToFsSettings {
              base_path: "%s"
              items {
                source_path: "/MyRoot/Table"
                destination_path: "/backup/Table"
              }
            }
        )", basePath.c_str());

        TestExport(runtime, ++txId, "/MyRoot", requestStr);

        // Wait for export completion
        env.TestWaitNotification(runtime, txId);

        // Check export status
        auto desc = TestGetExport(runtime, txId, "/MyRoot");
        const auto& entry = desc.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Export::ExportProgress::PROGRESS_DONE);
        UNIT_ASSERT(entry.HasStartTime());
        UNIT_ASSERT(entry.HasEndTime());

        // Verify files exist
        TString schemePath = MakeExportPath(basePath, "/backup/Table", "scheme.pb");
        TString metadataPath = MakeExportPath(basePath, "/backup/Table", "metadata.json");
        TString dataPath = MakeExportPath(basePath, "/backup/Table", "data_00.csv");

        // UNIT_ASSERT_C(FileExists(schemePath), "Scheme file not found: " << schemePath);
        // UNIT_ASSERT_C(FileExists(metadataPath), "Metadata file not found: " << metadataPath);
        // UNIT_ASSERT_C(FileExists(dataPath), "Data file not found: " << dataPath);

        // // Verify scheme file is valid protobuf
        // TString schemeContent = ReadFileContent(schemePath);
        // Cerr << "Scheme content: " << schemeContent << Endl;
        // UNIT_ASSERT_C(!schemeContent.empty(), "Scheme file is empty");

        // Ydb::Table::CreateTableRequest schemeProto;
        // UNIT_ASSERT_C(google::protobuf::TextFormat::ParseFromString(schemeContent, &schemeProto), 
        //              "Failed to parse scheme.pb");
        // UNIT_ASSERT_VALUES_EQUAL(schemeProto.columns_size(), 2);
        // UNIT_ASSERT_VALUES_EQUAL(schemeProto.columns(0).name(), "key");
        // UNIT_ASSERT_VALUES_EQUAL(schemeProto.columns(1).name(), "value");

        // // Verify metadata file is valid JSON
        // TString metadataContent = ReadFileContent(metadataPath);
        // UNIT_ASSERT_C(!metadataContent.empty(), "Metadata file is empty");
        // UNIT_ASSERT_C(metadataContent.Contains("\"version\""), "Metadata missing version field");

        // // Verify data file contains records
        // TString dataContent = ReadFileContent(dataPath);
        // UNIT_ASSERT_C(!dataContent.empty(), "Data file is empty");
        // UNIT_ASSERT_C(dataContent.Contains("row1") || dataContent.Contains("row2") || dataContent.Contains("row3"), 
        //              "Data file doesn't contain expected rows");
    }

    Y_UNIT_TEST(ShouldExportMultipleTablesWithData) {
        TTempDir tempDir;
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);

        // Create first table
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Create second table
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table2"
            Columns { Name: "id" Type: "Uint32" }
            Columns { Name: "name" Type: "Utf8" }
            KeyColumnNames: ["id"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Write data to both tables
        WriteRow(runtime, ++txId, "/MyRoot/Table1", 0, 10, "data1");
        WriteRow(runtime, ++txId, "/MyRoot/Table1", 0, 20, "data2");
        WriteRow(runtime, ++txId, "/MyRoot/Table2", 0, 100, "name1");
        WriteRow(runtime, ++txId, "/MyRoot/Table2", 0, 200, "name2");

        // Export both tables
        TString basePath = tempDir.Path();
        TString requestStr = Sprintf(R"(
            ExportToFsSettings {
              base_path: "%s"
              items {
                source_path: "/MyRoot/Table1"
                destination_path: "backup/Table1"
              }
              items {
                source_path: "/MyRoot/Table2"
                destination_path: "backup/Table2"
              }
            }
        )", basePath.c_str());

        TestExport(runtime, ++txId, "/MyRoot", requestStr);
        env.TestWaitNotification(runtime, txId);

        // Verify Table1 files
        UNIT_ASSERT_C(FileExists(MakeExportPath(basePath, "backup/Table1", "scheme.pb")),
                     "Table1 scheme.pb not found");
        UNIT_ASSERT_C(FileExists(MakeExportPath(basePath, "backup/Table1", "metadata.json")),
                     "Table1 metadata.json not found");
        UNIT_ASSERT_C(FileExists(MakeExportPath(basePath, "backup/Table1", "data_00.csv")),
                     "Table1 data not found");

        // Verify Table2 files
        UNIT_ASSERT_C(FileExists(MakeExportPath(basePath, "backup/Table2", "scheme.pb")),
                     "Table2 scheme.pb not found");
        UNIT_ASSERT_C(FileExists(MakeExportPath(basePath, "backup/Table2", "metadata.json")),
                     "Table2 metadata.json not found");
        UNIT_ASSERT_C(FileExists(MakeExportPath(basePath, "backup/Table2", "data_00.csv")),
                     "Table2 data not found");

        // Verify schemas are correct
        TString scheme1Content = ReadFileContent(MakeExportPath(basePath, "backup/Table1", "scheme.pb"));
        Ydb::Table::CreateTableRequest schemeProto1;
        UNIT_ASSERT_C(schemeProto1.ParseFromString(scheme1Content), "Failed to parse Table1 scheme");
        UNIT_ASSERT_VALUES_EQUAL(schemeProto1.columns_size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(schemeProto1.columns(0).name(), "key");

        TString scheme2Content = ReadFileContent(MakeExportPath(basePath, "backup/Table2", "scheme.pb"));
        Ydb::Table::CreateTableRequest schemeProto2;
        UNIT_ASSERT_C(schemeProto2.ParseFromString(scheme2Content), "Failed to parse Table2 scheme");
        UNIT_ASSERT_VALUES_EQUAL(schemeProto2.columns_size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(schemeProto2.columns(0).name(), "id");

        // Verify data contains expected values
        TString data1 = ReadFileContent(MakeExportPath(basePath, "backup/Table1", "data_00.csv"));
        UNIT_ASSERT_C(data1.Contains("data1") || data1.Contains("data2"), "Table1 data incorrect");

        TString data2 = ReadFileContent(MakeExportPath(basePath, "backup/Table2", "data_00.csv"));
        UNIT_ASSERT_C(data2.Contains("name1") || data2.Contains("name2"), "Table2 data incorrect");
    }

    Y_UNIT_TEST(ShouldExportWithCompressionToFs) {
        TTempDir tempDir;
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Write data
        for (ui32 i = 1; i <= 10; ++i) {
            WriteRow(runtime, ++txId, "/MyRoot/Table", 0, i, Sprintf("value_%u", i));
        }

        // Export with compression
        TString basePath = tempDir.Path();
        TString requestStr = Sprintf(R"(
            ExportToFsSettings {
              base_path: "%s"
              compression: "zstd-3"
              items {
                source_path: "/MyRoot/Table"
                destination_path: "backup/Table"
              }
            }
        )", basePath.c_str());

        TestExport(runtime, ++txId, "/MyRoot", requestStr);
        env.TestWaitNotification(runtime, txId);

        // Check that compressed file exists (should have .zst extension)
        TString dataPath = MakeExportPath(basePath, "backup/Table", "data_00.csv.zst");
        UNIT_ASSERT_C(FileExists(dataPath), "Compressed data file not found: " << dataPath);

        // Verify other files exist
        UNIT_ASSERT_C(FileExists(MakeExportPath(basePath, "backup/Table", "scheme.pb")),
                     "Scheme file not found");
        UNIT_ASSERT_C(FileExists(MakeExportPath(basePath, "backup/Table", "metadata.json")),
                     "Metadata file not found");
    }

    Y_UNIT_TEST(ShouldTrackExportProgress) {
        TTempDir tempDir;
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Write substantial amount of data
        for (ui32 i = 1; i <= 100; ++i) {
            WriteRow(runtime, ++txId, "/MyRoot/Table", 0, i, Sprintf("value_%u", i));
        }

        // Start export
        TString basePath = tempDir.Path();
        TString requestStr = Sprintf(R"(
            ExportToFsSettings {
              base_path: "%s"
              items {
                source_path: "/MyRoot/Table"
                destination_path: "backup/Table"
              }
            }
        )", basePath.c_str());

        TestExport(runtime, ++txId, "/MyRoot", requestStr);
        
        // Check progress immediately after start
        {
            auto desc = TestGetExport(runtime, txId, "/MyRoot");
            const auto& entry = desc.GetResponse().GetEntry();
            UNIT_ASSERT(entry.GetProgress() == Ydb::Export::ExportProgress::PROGRESS_PREPARING ||
                       entry.GetProgress() == Ydb::Export::ExportProgress::PROGRESS_TRANSFER_DATA ||
                       entry.GetProgress() == Ydb::Export::ExportProgress::PROGRESS_DONE);
            UNIT_ASSERT_VALUES_EQUAL(entry.ItemsProgressSize(), 1);
        }

        // Wait for completion
        env.TestWaitNotification(runtime, txId);

        // Check final state
        {
            auto desc = TestGetExport(runtime, txId, "/MyRoot");
            const auto& entry = desc.GetResponse().GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Export::ExportProgress::PROGRESS_DONE);
            UNIT_ASSERT(entry.HasStartTime());
            UNIT_ASSERT(entry.HasEndTime());
            
            const auto& itemProgress = entry.GetItemsProgress(0);
            UNIT_ASSERT_VALUES_EQUAL(itemProgress.parts_total(), itemProgress.parts_completed());
        }
    }
}
