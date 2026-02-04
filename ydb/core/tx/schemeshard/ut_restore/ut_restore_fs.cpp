#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/backup/common/metadata.h>
#include <ydb/core/backup/common/checksum.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/api/protos/ydb_import.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/lib/ydb_cli/dump/files/files.h>

#include <library/cpp/json/json_writer.h>
#include <util/folder/dirut.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/system/fs.h>

using namespace NSchemeShardUT_Private;

namespace {

class TTempBackupFiles {
public:
    explicit TTempBackupFiles() = default;

    const TString& GetBasePath() const {
        return TempDir.Name();
    }

    void CreateTableBackup(const TString& tablePath, const TString& tableName) {
        CreateTableBackup(
            tablePath,
            tableName,
            {
                {"key", Ydb::Type::UTF8},
                {"value", Ydb::Type::UTF8}
            },
            {"key"}
        );
    }

    void CreateTableBackup(const TString& tablePath, const TString& tableName,
                          const TVector<std::pair<TString, Ydb::Type::PrimitiveTypeId>>& columns,
                          const TVector<TString>& keyColumns,
                          const TVector<TVector<TString>>& splitPoints = {}) {
        const TString fullPath = TempDir.Name() + "/" + tablePath;
        MakePathIfNotExist(fullPath.c_str());

        // Create metadata.json
        CreateMetadataFile(fullPath);

        // Create scheme.pb
        CreateTableSchemeFile(fullPath, tableName, columns, keyColumns, splitPoints);

        // Create permissions.pb
        CreatePermissionsFile(fullPath);

        // Create empty data_00.csv with checksum
        CreateEmptyDataFile(fullPath);
    }

    void AddDataFile(const TString& tablePath, const TString& csvData, ui32 partNum = 0) {
        const TString fullPath = TempDir.Name() + "/" + tablePath;
        WriteDataFileWithChecksum(fullPath, csvData, partNum);
    }

private:
    static void WriteFileWithChecksum(const TString& dirPath, const TString& fileName, const TString& content) {
        TFileOutput file(dirPath + "/" + fileName);
        file.Write(content);
        file.Finish();

        // Create checksum file
        const TString checksum = NBackup::ComputeChecksum(content);
        const TString checksumFileName = fileName + ".sha256";
        TFileOutput checksumFile(dirPath + "/" + checksumFileName);
        checksumFile.Write(checksum);
        checksumFile.Write(" ");
        checksumFile.Write(fileName);
        checksumFile.Finish();
    }

    static void WriteDataFileWithChecksum(const TString& dirPath, const TString& csvData, ui32 partNum) {
        const TString dataFileName = TStringBuilder() << "data_" << Sprintf("%02d", partNum) << ".csv";
        WriteFileWithChecksum(dirPath, dataFileName, csvData);
    }

    static void CreateEmptyDataFile(const TString& dirPath) {
        // Create empty data_00.csv with checksum
        WriteDataFileWithChecksum(dirPath, "", 0);
    }

    static void CreateMetadataFile(const TString& dirPath) {
        NBackup::TMetadata metadata;
        metadata.SetVersion(1);
        metadata.SetEnablePermissions(true);

        TString serialized = metadata.Serialize();

        WriteFileWithChecksum(dirPath, "metadata.json", serialized);
    }

    static void CreateTableSchemeFile(const TString& dirPath, const TString& tableName,
                                      const TVector<std::pair<TString, Ydb::Type::PrimitiveTypeId>>& columns,
                                      const TVector<TString>& keyColumns,
                                      const TVector<TVector<TString>>& splitPoints = {}) {
        Ydb::Table::CreateTableRequest table;
        table.set_path(tableName);

        for (const auto& [colName, colType] : columns) {
            auto* col = table.add_columns();
            col->set_name(colName);
            col->mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(colType);
        }

        for (const auto& keyCol : keyColumns) {
            table.add_primary_key(keyCol);
        }

        if (!splitPoints.empty()) {
            auto* partitionAtKeys = table.mutable_partition_at_keys();
            for (const auto& point : splitPoints) {
                auto* splitPoint = partitionAtKeys->add_split_points();
                auto* tupleType = splitPoint->mutable_type()->mutable_tuple_type();
                for (size_t i = 0; i < point.size(); ++i) {
                    // For UTF8 key columns
                    tupleType->add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
                }
                for (const auto& value : point) {
                    splitPoint->mutable_value()->add_items()->set_text_value(value);
                }
            }
        }

        TString serialized;
        Y_ABORT_UNLESS(google::protobuf::TextFormat::PrintToString(table, &serialized));

        WriteFileWithChecksum(dirPath, NYdb::NDump::NFiles::TableScheme().FileName, serialized);
    }

    static void CreatePermissionsFile(const TString& dirPath) {
        Ydb::Scheme::ModifyPermissionsRequest permissions;

        TString serialized;
        Y_ABORT_UNLESS(google::protobuf::TextFormat::PrintToString(permissions, &serialized));

        WriteFileWithChecksum(dirPath, "permissions.pb", serialized);
    }

    TTempDir TempDir;
};

} // namespace

Y_UNIT_TEST_SUITE(TImportFromFsTests) {
    Y_UNIT_TEST(ShouldSucceedCreateImportFromFs) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);

        TTempBackupFiles backup;
        backup.CreateTableBackup("backup/Table", "Table");

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
        env.TestWaitNotification(runtime, txId);

        auto response = TestGetImport(runtime, txId, "/MyRoot", Ydb::StatusIds::SUCCESS);
        const auto& entry = response.GetResponse().GetEntry();

        UNIT_ASSERT(entry.HasImportFromFsSettings());
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Import::ImportProgress::PROGRESS_DONE);

        const auto& settings = entry.GetImportFromFsSettings();
        UNIT_ASSERT_VALUES_EQUAL(settings.base_path(), backup.GetBasePath());
        UNIT_ASSERT_VALUES_EQUAL(settings.items_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(settings.items(0).source_path(), "backup/Table");
        UNIT_ASSERT_VALUES_EQUAL(settings.items(0).destination_path(), "/MyRoot/RestoredTable");

        TestDescribeResult(DescribePath(runtime, "/MyRoot/RestoredTable"), {
            NLs::PathExist,
            NLs::IsTable
        });

        auto describe = DescribePath(runtime, "/MyRoot/RestoredTable");
        const auto& table = describe.GetPathDescription().GetTable();
        UNIT_ASSERT_VALUES_EQUAL(table.ColumnsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(table.GetColumns(0).GetName(), "key");
        UNIT_ASSERT_VALUES_EQUAL(table.GetColumns(1).GetName(), "value");
        UNIT_ASSERT_VALUES_EQUAL(table.KeyColumnNamesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(table.GetKeyColumnNames(0), "key");
    }

    Y_UNIT_TEST(ShouldAcceptNoAclForFs) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);

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
        env.TestWaitNotification(runtime, txId);

        auto response = TestGetImport(runtime, txId, "/MyRoot", Ydb::StatusIds::SUCCESS);
        const auto& entry = response.GetResponse().GetEntry();

        UNIT_ASSERT(entry.HasImportFromFsSettings());
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Import::ImportProgress::PROGRESS_DONE);

        const auto& settings = entry.GetImportFromFsSettings();
        UNIT_ASSERT_VALUES_EQUAL(settings.no_acl(), true);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/RestoredTable"), {
            NLs::PathExist,
            NLs::IsTable
        });
    }

    Y_UNIT_TEST(ShouldAcceptSkipChecksumValidation) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);

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
        env.TestWaitNotification(runtime, txId);

        auto response = TestGetImport(runtime, txId, "/MyRoot", Ydb::StatusIds::SUCCESS);
        const auto& entry = response.GetResponse().GetEntry();

        UNIT_ASSERT(entry.HasImportFromFsSettings());
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Import::ImportProgress::PROGRESS_DONE);

        const auto& settings = entry.GetImportFromFsSettings();
        UNIT_ASSERT_VALUES_EQUAL(settings.skip_checksum_validation(), true);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/RestoredTable"), {
            NLs::PathExist,
            NLs::IsTable
        });
    }

    Y_UNIT_TEST(ShouldFailOnInvalidDestinationPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);

        TTempBackupFiles backup;
        backup.CreateTableBackup("backup/Table", "Table");

        // Invalid destination path (empty)
        TestImport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            ImportFromFsSettings {
              base_path: "%s"
              items {
                source_path: "backup/Table"
                destination_path: ""
              }
            }
        )", backup.GetBasePath().c_str()));
        env.TestWaitNotification(runtime, txId);

        TestGetImport(runtime, txId, "/MyRoot", Ydb::StatusIds::CANCELLED);
    }

    Y_UNIT_TEST(ShouldFailOnDuplicateDestination) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);

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
        runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);

        TTempBackupFiles backup;
        backup.CreateTableBackup("backup/Table1", "Table1");
        backup.CreateTableBackup("backup/Table2", "Table2");

        TString table1Data = R"("user1","Alice"
"user2","Bob"
)";
        backup.AddDataFile("backup/Table1", table1Data);
        TString table2Data = R"("order1","product1"
"order2","product2"
)";
        backup.AddDataFile("backup/Table2", table2Data);

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
        env.TestWaitNotification(runtime, txId);

        auto response = TestGetImport(runtime, txId, "/MyRoot", Ydb::StatusIds::SUCCESS);
        const auto& entry = response.GetResponse().GetEntry();

        UNIT_ASSERT(entry.HasImportFromFsSettings());
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Import::ImportProgress::PROGRESS_DONE);

        const auto& settings = entry.GetImportFromFsSettings();
        UNIT_ASSERT_VALUES_EQUAL(settings.items_size(), 2);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/RestoredTable1"), {
            NLs::PathExist,
            NLs::IsTable
        });

        TestDescribeResult(DescribePath(runtime, "/MyRoot/RestoredTable2"), {
            NLs::PathExist,
            NLs::IsTable
        });

        ui32 table1Rows = CountRows(runtime, "/MyRoot/RestoredTable1");
        UNIT_ASSERT_VALUES_EQUAL(table1Rows, 2);

        ui32 table2Rows = CountRows(runtime, "/MyRoot/RestoredTable2");
        UNIT_ASSERT_VALUES_EQUAL(table2Rows, 2);
    }

    Y_UNIT_TEST(ShouldFailOnMissingBackupFiles) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);

        TTempBackupFiles backup;

        TString importSettings = Sprintf(R"(
            ImportFromFsSettings {
              base_path: "%s"
              items {
                source_path: "backup/NonExistentTable"
                destination_path: "/MyRoot/RestoredTable"
              }
            }
        )", backup.GetBasePath().c_str());

        TestImport(runtime, ++txId, "/MyRoot", importSettings);
        env.TestWaitNotification(runtime, txId);

        auto response = TestGetImport(runtime, txId, "/MyRoot", Ydb::StatusIds::CANCELLED);
        const auto& entry = response.GetResponse().GetEntry();

        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Import::ImportProgress::PROGRESS_CANCELLED);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/RestoredTable"), {
            NLs::PathNotExist
        });
    }

    Y_UNIT_TEST(ShouldRestorePartitionedTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::IMPORT, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::FS_WRAPPER, NActors::NLog::PRI_TRACE);

        TTempBackupFiles backup;

        backup.CreateTableBackup(
            "backup/PartitionedTable",
            "PartitionedTable",
            {
                {"key", Ydb::Type::UTF8},
                {"value", Ydb::Type::UTF8}
            },
            {"key"},
            {{"key3"}}
        );

        TString csvData1 = R"("key1","value1"
"key2","value2"
)";
        TString csvData2 = R"("key3","value3"
"key4","value4"
)";
        backup.AddDataFile("backup/PartitionedTable", csvData1, 0);
        backup.AddDataFile("backup/PartitionedTable", csvData2, 1);

        TString importSettings = Sprintf(R"(
            ImportFromFsSettings {
              base_path: "%s"
              items {
                source_path: "backup/PartitionedTable"
                destination_path: "/MyRoot/PartitionedTable"
              }
            }
        )", backup.GetBasePath().c_str());

        TestImport(runtime, ++txId, "/MyRoot", importSettings);
        env.TestWaitNotification(runtime, txId);

        auto response = TestGetImport(runtime, txId, "/MyRoot", Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(response.GetResponse().GetEntry().GetProgress(), Ydb::Import::ImportProgress::PROGRESS_DONE);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/PartitionedTable"), {
            NLs::PathExist,
            NLs::IsTable
        });

        ui32 totalRows = CountRows(runtime, "/MyRoot/PartitionedTable");
        UNIT_ASSERT_VALUES_EQUAL(totalRows, 4);
    }

    void ExportImportWithDataValidationImpl(bool encrypted) {
        TTempDir tempDir;
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);
        runtime.GetAppData().FeatureFlags.SetEnableEncryptedExport(encrypted);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::IMPORT, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::FS_WRAPPER, NActors::NLog::PRI_TRACE);

        // Step 1: Create table with schema
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "OriginalTable"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            Columns { Name: "amount" Type: "Int64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // Step 2: Insert test data
        WriteRow(runtime, ++txId, "/MyRoot/OriginalTable", 0, 1, "apple");
        WriteRow(runtime, ++txId, "/MyRoot/OriginalTable", 0, 2, "banana");
        WriteRow(runtime, ++txId, "/MyRoot/OriginalTable", 0, 3, "cherry");
        WriteRow(runtime, ++txId, "/MyRoot/OriginalTable", 0, 4, "date");
        WriteRow(runtime, ++txId, "/MyRoot/OriginalTable", 0, 5, "elderberry");

        // Verify original data count
        ui32 originalRows = CountRows(runtime, "/MyRoot/OriginalTable");
        UNIT_ASSERT_VALUES_EQUAL(originalRows, 5);

        // Prepare encryption settings
        TString encryptionSettings;
        if (encrypted) {
            encryptionSettings = R"(encryption_settings {
                encryption_algorithm: "ChaCha20-Poly1305"
                symmetric_key {
                    key: "Very very secret export key!!!!!"
                }
            })";
        }

        // Step 3: Export to FS
        TString basePath = tempDir.Path();
        TString exportSettings = Sprintf(R"(
            ExportToFsSettings {
              base_path: "%s"
              items {
                source_path: "/MyRoot/OriginalTable"
                destination_path: "backup/OriginalTable"
              }
              %s
            }
        )", basePath.c_str(), encryptionSettings.c_str());

        TestExport(runtime, ++txId, "/MyRoot", exportSettings);
        const ui64 exportId = txId;
        env.TestWaitNotification(runtime, exportId);

        // Verify export completed successfully
        auto exportResponse = TestGetExport(runtime, exportId, "/MyRoot");
        const auto& exportEntry = exportResponse.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(exportEntry.GetProgress(), Ydb::Export::ExportProgress::PROGRESS_DONE);
        UNIT_ASSERT(exportEntry.HasStartTime());
        UNIT_ASSERT(exportEntry.HasEndTime());

        // Step 4: Import from FS to a new table
        TString importSettings = Sprintf(R"(
            ImportFromFsSettings {
              base_path: "%s"
              items {
                source_path: "backup/OriginalTable"
                destination_path: "/MyRoot/RestoredTable"
              }
              %s
            }
        )", basePath.c_str(), encryptionSettings.c_str());

        TestImport(runtime, ++txId, "/MyRoot", importSettings);
        const ui64 importId = txId;
        env.TestWaitNotification(runtime, importId);

        // Verify import completed successfully
        auto importResponse = TestGetImport(runtime, importId, "/MyRoot", Ydb::StatusIds::SUCCESS);
        const auto& importEntry = importResponse.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(importEntry.GetProgress(), Ydb::Import::ImportProgress::PROGRESS_DONE);

        // Step 5: Verify restored table exists
        auto describe = DescribePath(runtime, "/MyRoot/RestoredTable");
        TestDescribeResult(describe, {
            NLs::PathExist,
            NLs::IsTable
        });

        // Step 6: Verify schema matches
        const auto& table = describe.GetPathDescription().GetTable();
        UNIT_ASSERT_VALUES_EQUAL(table.ColumnsSize(), 3);
        UNIT_ASSERT_VALUES_EQUAL(table.GetColumns(0).GetName(), "key");
        UNIT_ASSERT_VALUES_EQUAL(table.GetColumns(1).GetName(), "value");
        UNIT_ASSERT_VALUES_EQUAL(table.GetColumns(2).GetName(), "amount");
        UNIT_ASSERT_VALUES_EQUAL(table.KeyColumnNamesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(table.GetKeyColumnNames(0), "key");

        // Step 7: Verify data count matches
        ui32 restoredRows = CountRows(runtime, "/MyRoot/RestoredTable");
        UNIT_ASSERT_VALUES_EQUAL(restoredRows, 5);
        UNIT_ASSERT_VALUES_EQUAL(restoredRows, originalRows);

        // Step 8: Verify data content matches
        ui64 schemeshardId = TTestTxConfig::SchemeShard;
        TVector<TString> originalData = ReadShards(runtime, schemeshardId, "/MyRoot/OriginalTable");
        TVector<TString> restoredData = ReadShards(runtime, schemeshardId, "/MyRoot/RestoredTable");
        UNIT_ASSERT_VALUES_EQUAL(originalData.size(), restoredData.size());
        for (size_t i = 0; i < originalData.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(originalData[i], restoredData[i]);
        }
    }

    Y_UNIT_TEST(ShouldExportThenImportWithDataValidation) {
        ExportImportWithDataValidationImpl(false);
    }

    Y_UNIT_TEST(ShouldExportThenImportWithDataValidationEncrypted) {
        ExportImportWithDataValidationImpl(true);
    }
}
