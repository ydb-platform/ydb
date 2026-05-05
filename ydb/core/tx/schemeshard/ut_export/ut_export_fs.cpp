#include <ydb/public/api/protos/ydb_export.pb.h>

#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/backup/common/encryption.h>

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

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        WriteRow(runtime, ++txId, "/MyRoot/Table", 0, 1, "row1");
        WriteRow(runtime, ++txId, "/MyRoot/Table", 0, 2, "row2");
        WriteRow(runtime, ++txId, "/MyRoot/Table", 0, 3, "row3");

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

        env.TestWaitNotification(runtime, txId);

        auto desc = TestGetExport(runtime, txId, "/MyRoot");
        const auto& entry = desc.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Export::ExportProgress::PROGRESS_DONE);
        UNIT_ASSERT(entry.HasStartTime());
        UNIT_ASSERT(entry.HasEndTime());

        TString schemePath = MakeExportPath(basePath, "backup/Table", "scheme.pb");
        TString metadataPath = MakeExportPath(basePath, "backup/Table", "metadata.json");
        TString dataPath = MakeExportPath(basePath, "backup/Table", "data_00.csv");

        UNIT_ASSERT_C(FileExists(schemePath), "Scheme file not found: " << schemePath);
        UNIT_ASSERT_C(FileExists(metadataPath), "Metadata file not found: " << metadataPath);
        UNIT_ASSERT_C(FileExists(dataPath), "Data file not found: " << dataPath);

        TString schemeContent = ReadFileContent(schemePath);
        Cerr << "Scheme content: " << schemeContent << Endl;
        UNIT_ASSERT_C(!schemeContent.empty(), "Scheme file is empty");

        Ydb::Table::CreateTableRequest schemeProto;
        UNIT_ASSERT_C(google::protobuf::TextFormat::ParseFromString(schemeContent, &schemeProto),
                     "Failed to parse scheme.pb");
        UNIT_ASSERT_VALUES_EQUAL(schemeProto.columns_size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(schemeProto.columns(0).name(), "key");
        UNIT_ASSERT_VALUES_EQUAL(schemeProto.columns(1).name(), "value");

        TString metadataContent = ReadFileContent(metadataPath);
        UNIT_ASSERT_C(!metadataContent.empty(), "Metadata file is empty");
        UNIT_ASSERT_C(metadataContent.Contains("\"version\""), "Metadata missing version field");

        TString dataContent = ReadFileContent(dataPath);
        UNIT_ASSERT_C(!dataContent.empty(), "Data file is empty");
        UNIT_ASSERT_C(dataContent.Contains("row1") || dataContent.Contains("row2") || dataContent.Contains("row3"),
                     "Data file doesn't contain expected rows");
    }

    Y_UNIT_TEST(ShouldExportMultipleTablesWithData) {
        TTempDir tempDir;
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table2"
            Columns { Name: "id" Type: "Uint32" }
            Columns { Name: "name" Type: "Utf8" }
            KeyColumnNames: ["id"]
        )");
        env.TestWaitNotification(runtime, txId);

        WriteRow(runtime, ++txId, "/MyRoot/Table1", 0, 10, "data1");
        WriteRow(runtime, ++txId, "/MyRoot/Table1", 0, 20, "data2");
        WriteRow(runtime, ++txId, "/MyRoot/Table2", 0, 100, "name1");
        WriteRow(runtime, ++txId, "/MyRoot/Table2", 0, 200, "name2");

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

        UNIT_ASSERT_C(FileExists(MakeExportPath(basePath, "backup/Table1", "scheme.pb")),
                     "Table1 scheme.pb not found");
        UNIT_ASSERT_C(FileExists(MakeExportPath(basePath, "backup/Table1", "metadata.json")),
                     "Table1 metadata.json not found");
        UNIT_ASSERT_C(FileExists(MakeExportPath(basePath, "backup/Table1", "data_00.csv")),
                     "Table1 data not found");

        UNIT_ASSERT_C(FileExists(MakeExportPath(basePath, "backup/Table2", "scheme.pb")),
                     "Table2 scheme.pb not found");
        UNIT_ASSERT_C(FileExists(MakeExportPath(basePath, "backup/Table2", "metadata.json")),
                     "Table2 metadata.json not found");
        UNIT_ASSERT_C(FileExists(MakeExportPath(basePath, "backup/Table2", "data_00.csv")),
                     "Table2 data not found");

        TString scheme1Content = ReadFileContent(MakeExportPath(basePath, "backup/Table1", "scheme.pb"));
        Ydb::Table::CreateTableRequest schemeProto1;
        UNIT_ASSERT_C(google::protobuf::TextFormat::ParseFromString(scheme1Content, &schemeProto1),
                     "Failed to parse scheme.pb");
        UNIT_ASSERT_VALUES_EQUAL(schemeProto1.columns_size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(schemeProto1.columns(0).name(), "key");

        TString scheme2Content = ReadFileContent(MakeExportPath(basePath, "backup/Table2", "scheme.pb"));
        Ydb::Table::CreateTableRequest schemeProto2;
        UNIT_ASSERT_C(google::protobuf::TextFormat::ParseFromString(scheme2Content, &schemeProto2),
                     "Failed to parse scheme.pb");
        UNIT_ASSERT_VALUES_EQUAL(schemeProto2.columns_size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(schemeProto2.columns(0).name(), "id");

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

        for (ui32 i = 1; i <= 10; ++i) {
            WriteRow(runtime, ++txId, "/MyRoot/Table", 0, i, Sprintf("value_%u", i));
        }

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

        TString dataPath = MakeExportPath(basePath, "backup/Table", "data_00.csv.zst");
        UNIT_ASSERT_C(FileExists(dataPath), "Compressed data file not found: " << dataPath);

        UNIT_ASSERT_C(FileExists(MakeExportPath(basePath, "backup/Table", "scheme.pb")),
                     "Scheme file not found");
        UNIT_ASSERT_C(FileExists(MakeExportPath(basePath, "backup/Table", "metadata.json")),
                     "Metadata file not found");
    }

    Y_UNIT_TEST(SchemaMapping) {
        TTempDir tempDir;
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);
        runtime.GetAppData().FeatureFlags.SetEnableEncryptedExport(true);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table2"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TString basePath = tempDir.Path();
        TString requestStr = Sprintf(R"(
            ExportToFsSettings {
              base_path: "%s"
              source_path: "/MyRoot"
              items {
                source_path: "/MyRoot/Table1"
              }
              items {
                source_path: "/MyRoot/Table2"
                destination_path: "table2_prefix"
              }
            }
        )", basePath.c_str());

        TestExport(runtime, ++txId, "/MyRoot", requestStr);
        env.TestWaitNotification(runtime, txId);

        auto desc = TestGetExport(runtime, txId, "/MyRoot");
        const auto& entry = desc.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Export::ExportProgress::PROGRESS_DONE);

        TFsPath baseDir(basePath);
        UNIT_ASSERT(FileExists((baseDir / "metadata.json").GetPath()));
        UNIT_ASSERT(FileExists((baseDir / "SchemaMapping" / "metadata.json").GetPath()));
        UNIT_ASSERT(FileExists((baseDir / "SchemaMapping" / "mapping.json").GetPath()));
        UNIT_ASSERT(FileExists((baseDir / "Table1" / "scheme.pb").GetPath()));
        UNIT_ASSERT(FileExists((baseDir / "table2_prefix" / "scheme.pb").GetPath()));

        TString metadataContent = ReadFileContent((baseDir / "metadata.json").GetPath());
        UNIT_ASSERT_STRINGS_EQUAL(metadataContent, "{\"kind\":\"SimpleExportV0\",\"checksum\":\"sha256\"}");
    }

    Y_UNIT_TEST(SchemaMappingEncryption) {
        TTempDir tempDir;
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);
        runtime.GetAppData().FeatureFlags.SetEnableEncryptedExport(true);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table2"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TString basePath = tempDir.Path();
        TString requestStr = Sprintf(R"(
            ExportToFsSettings {
              base_path: "%s"
              items {
                source_path: "/MyRoot/Table1"
              }
              items {
                source_path: "/MyRoot/Table2"
                destination_path: "table2_prefix"
              }
              encryption_settings {
                encryption_algorithm: "AES-128-GCM"
                symmetric_key {
                  key: "0123456789012345"
                }
              }
            }
        )", basePath.c_str());

        TestExport(runtime, ++txId, "/MyRoot", requestStr);
        env.TestWaitNotification(runtime, txId);

        auto desc = TestGetExport(runtime, txId, "/MyRoot");
        const auto& entry = desc.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Export::ExportProgress::PROGRESS_DONE);

        TFsPath baseDir(basePath);
        UNIT_ASSERT(FileExists((baseDir / "metadata.json").GetPath()));
        UNIT_ASSERT(FileExists((baseDir / "SchemaMapping" / "metadata.json.enc").GetPath()));
        UNIT_ASSERT(FileExists((baseDir / "SchemaMapping" / "mapping.json.enc").GetPath()));
        UNIT_ASSERT(FileExists((baseDir / "001" / "scheme.pb.enc").GetPath()));
        UNIT_ASSERT(FileExists((baseDir / "table2_prefix" / "scheme.pb.enc").GetPath()));
    }

    Y_UNIT_TEST(SchemaMappingEncryptionIncorrectKey) {
        TTempDir tempDir;
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);
        runtime.GetAppData().FeatureFlags.SetEnableEncryptedExport(true);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TString basePath = tempDir.Path();
        TString requestStr = Sprintf(R"(
            ExportToFsSettings {
              base_path: "%s"
              source_path: "/MyRoot"
              items {
                source_path: "/MyRoot/Table1"
              }
              encryption_settings {
                encryption_algorithm: "AES-128-GCM"
                symmetric_key {
                    key: "123"
                }
              }
            }
        )", basePath.c_str());

        TestExport(runtime, ++txId, "/MyRoot", requestStr, "", "", Ydb::StatusIds::SUCCESS);
        env.TestWaitNotification(runtime, txId);

        auto desc = TestGetExport(runtime, txId, "/MyRoot", Ydb::StatusIds::CANCELLED);
        const auto& entry = desc.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Export::ExportProgress::PROGRESS_CANCELLED);
    }

    Y_UNIT_TEST(EncryptedExport) {
        TTempDir tempDir;
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);
        runtime.GetAppData().FeatureFlags.SetEnableEncryptedExport(true);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            UniformPartitionsCount: 2
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table2"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            UniformPartitionsCount: 2
        )");
        env.TestWaitNotification(runtime, txId);

        for (ui32 i = 1; i <= 10; ++i) {
            WriteRow(runtime, ++txId, "/MyRoot/Table1", 0, i, Sprintf("value1_%u", i));
            WriteRow(runtime, ++txId, "/MyRoot/Table1", 1, i + 100, Sprintf("value1_%u", i + 100));
        }
        for (ui32 i = 1; i <= 10; ++i) {
            WriteRow(runtime, ++txId, "/MyRoot/Table2", 0, i, Sprintf("value2_%u", i));
            WriteRow(runtime, ++txId, "/MyRoot/Table2", 1, i + 100, Sprintf("value2_%u", i + 100));
        }

        TString basePath = tempDir.Path();
        TString requestStr = Sprintf(R"(
            ExportToFsSettings {
              base_path: "%s"
              items {
                source_path: "/MyRoot/Table1"
              }
              items {
                source_path: "/MyRoot/Table2"
              }
              encryption_settings {
                encryption_algorithm: "AES-128-GCM"
                symmetric_key {
                  key: "0123456789012345"
                }
              }
            }
        )", basePath.c_str());

        TestExport(runtime, ++txId, "/MyRoot", requestStr);
        env.TestWaitNotification(runtime, txId);

        auto desc = TestGetExport(runtime, txId, "/MyRoot");
        const auto& entry = desc.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Export::ExportProgress::PROGRESS_DONE);

        TFsPath baseDir(basePath);
        TVector<TFsPath> expectedFiles = {
            baseDir / "metadata.json",
            baseDir / "SchemaMapping" / "metadata.json.enc",
            baseDir / "SchemaMapping" / "mapping.json.enc",
            baseDir / "001" / "scheme.pb.enc",
            baseDir / "001" / "data_00.csv.enc",
            baseDir / "001" / "data_01.csv.enc",
            baseDir / "002" / "scheme.pb.enc",
            baseDir / "002" / "data_00.csv.enc",
            baseDir / "002" / "data_01.csv.enc",
        };

        for (const auto& file : expectedFiles) {
            UNIT_ASSERT_C(FileExists(file.GetPath()), "File not found: " << file.GetPath());
        }

        TVector<TFsPath> allFiles;
        std::function<void(const TFsPath&)> collectFiles = [&](const TFsPath& dir) {
            TVector<TString> children;
            dir.ListNames(children);
            for (const auto& child : children) {
                TFsPath childPath = dir / child;
                if (childPath.IsDirectory()) {
                    collectFiles(childPath);
                } else if (childPath.IsFile()) {
                    allFiles.push_back(childPath);
                }
            }
        };
        collectFiles(baseDir);

        THashSet<TString> ivs;
        for (const auto& file : allFiles) {
            TString filePath = file.GetPath();
            if (filePath.EndsWith("metadata.json") || filePath.EndsWith(".sha256")) {
                continue;
            }

            UNIT_ASSERT_C(filePath.EndsWith(".enc"), filePath);

            TString content = ReadFileContent(filePath);
            UNIT_ASSERT_C(!content.empty(), "File is empty: " << filePath);

            TBuffer decryptedData;
            NBackup::TEncryptionIV iv;
            UNIT_ASSERT_NO_EXCEPTION_C(
                std::tie(decryptedData, iv) = NBackup::TEncryptedFileDeserializer::DecryptFullFile(
                    NBackup::TEncryptionKey("0123456789012345"),
                    TBuffer(content.data(), content.size())
                ), filePath);

            UNIT_ASSERT_C(ivs.insert(iv.GetBinaryString()).second, "Duplicate IV for: " << filePath);
        }
    }

    Y_UNIT_TEST(IndexMaterializationForFs) {
        TTempDir tempDir;
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);
        runtime.GetAppData().FeatureFlags.SetEnableIndexMaterialization(true);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "Table"
              Columns { Name: "key" Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "index"
              KeyColumnNames: ["value"]
            }
        )");
        env.TestWaitNotification(runtime, txId);

        WriteRow(runtime, ++txId, "/MyRoot/Table", 0, 1, "row1");
        WriteRow(runtime, ++txId, "/MyRoot/Table", 0, 2, "row2");

        TString basePath = tempDir.Path();
        TString requestStr = Sprintf(R"(
            ExportToFsSettings {
              base_path: "%s"
              include_index_data: true
              items {
                source_path: "/MyRoot/Table"
                destination_path: "backup/Table"
              }
            }
        )", basePath.c_str());

        TestExport(runtime, ++txId, "/MyRoot", requestStr);
        env.TestWaitNotification(runtime, txId);

        auto desc = TestGetExport(runtime, txId, "/MyRoot");
        const auto& entry = desc.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Export::ExportProgress::PROGRESS_DONE);

        UNIT_ASSERT(FileExists(MakeExportPath(basePath, "backup/Table", "scheme.pb")));
        UNIT_ASSERT(FileExists(MakeExportPath(basePath, "backup/Table/index/indexImplTable", "scheme.pb")));
    }
}
