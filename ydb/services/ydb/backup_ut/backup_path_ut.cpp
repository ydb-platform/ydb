#include "s3_backup_test_base.h"

using namespace NYdb;

class TBackupPathTestFixture : public TS3BackupTestFixture {
    void SetUp(NUnitTest::TTestContext& /* context */) override {
        auto res = YdbQueryClient().ExecuteQuery(R"sql(
            CREATE TABLE `/Root/RecursiveFolderProcessing/Table0` (
                key Uint32 NOT NULL,
                value String,
                PRIMARY KEY (key)
            );

            CREATE TABLE `/Root/RecursiveFolderProcessing/dir1/Table1` (
                key Uint32 NOT NULL,
                value String,
                PRIMARY KEY (key)
            );

            CREATE TABLE `/Root/RecursiveFolderProcessing/dir1/dir2/Table2` (
                key Uint32 NOT NULL,
                value String,
                PRIMARY KEY (key)
            );
        )sql", NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        // Empty dir
        auto mkdir = YdbSchemeClient().MakeDirectory("/Root/RecursiveFolderProcessing/dir1/dir2/dir3").GetValueSync();
        UNIT_ASSERT_C(mkdir.IsSuccess(), mkdir.GetIssues().ToString());
    }

    void TearDown(NUnitTest::TTestContext& /* context */) override {
    }
};

Y_UNIT_TEST_SUITE_F(BackupPathTest, TBackupPathTestFixture) {
    Y_UNIT_TEST(ExportWholeDatabase) {
        // Export without source path: source path == database root
        {
            NExport::TExportToS3Settings exportSettings = MakeExportSettings("", "Prefix");
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateS3FileList({
                "/test_bucket/Prefix/metadata.json",
                "/test_bucket/Prefix/SchemaMapping/metadata.json",
                "/test_bucket/Prefix/SchemaMapping/mapping.json",
                "/test_bucket/Prefix/RecursiveFolderProcessing/Table0/metadata.json",
                "/test_bucket/Prefix/RecursiveFolderProcessing/Table0/scheme.pb",
                "/test_bucket/Prefix/RecursiveFolderProcessing/Table0/data_00.csv",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/Table1/metadata.json",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/Table1/scheme.pb",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/Table1/data_00.csv",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/metadata.json",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/scheme.pb",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/data_00.csv",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix");
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbTables({
                "/Root/RestorePrefix/RecursiveFolderProcessing/Table0",
                "/Root/RestorePrefix/RecursiveFolderProcessing/dir1/Table1",
                "/Root/RestorePrefix/RecursiveFolderProcessing/dir1/dir2/Table2",
            });
        }
    }

    Y_UNIT_TEST(ExportWholeDatabaseWithEncryption) {
        {
            NExport::TExportToS3Settings exportSettings = MakeExportSettings("", "Prefix");
            exportSettings
                .SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateS3FileList({
                "/test_bucket/Prefix/metadata.json",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.enc",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.enc",
                "/test_bucket/Prefix/001/metadata.json.enc",
                "/test_bucket/Prefix/001/scheme.pb.enc",
                "/test_bucket/Prefix/001/data_00.csv.enc",
                "/test_bucket/Prefix/002/metadata.json.enc",
                "/test_bucket/Prefix/002/scheme.pb.enc",
                "/test_bucket/Prefix/002/data_00.csv.enc",
                "/test_bucket/Prefix/003/metadata.json.enc",
                "/test_bucket/Prefix/003/scheme.pb.enc",
                "/test_bucket/Prefix/003/data_00.csv.enc",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix");
            importSettings
                .SymmetricKey("Cool random key!");
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbTables({
                "/Root/RestorePrefix/RecursiveFolderProcessing/Table0",
                "/Root/RestorePrefix/RecursiveFolderProcessing/dir1/Table1",
                "/Root/RestorePrefix/RecursiveFolderProcessing/dir1/dir2/Table2",
            });
        }
    }

    Y_UNIT_TEST(ExportWithCommonSourcePath) {
        // Export with common source path == dir1
        {
            NExport::TExportToS3Settings exportSettings = MakeExportSettings("/Root/RecursiveFolderProcessing/dir1", "Prefix");
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateS3FileList({
                "/test_bucket/Prefix/metadata.json",
                "/test_bucket/Prefix/SchemaMapping/metadata.json",
                "/test_bucket/Prefix/SchemaMapping/mapping.json",
                "/test_bucket/Prefix/Table1/metadata.json",
                "/test_bucket/Prefix/Table1/scheme.pb",
                "/test_bucket/Prefix/Table1/data_00.csv",
                "/test_bucket/Prefix/dir2/Table2/metadata.json",
                "/test_bucket/Prefix/dir2/Table2/scheme.pb",
                "/test_bucket/Prefix/dir2/Table2/data_00.csv",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix");
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbTables({
                "/Root/RestorePrefix/Table1",
                "/Root/RestorePrefix/dir2/Table2",
            });
        }
    }

    Y_UNIT_TEST(ExportWithCommonSourcePathAndExplicitTableInside) {
        // Export with directory path == dir1 + explicit table from this subdir (must remove duplicate)
        {
            NExport::TExportToS3Settings exportSettings = MakeExportSettings("", "Prefix");
            exportSettings
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/Root/RecursiveFolderProcessing/dir1/Table1", .Dst = "ExplicitTable1Prefix"})
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/Root/RecursiveFolderProcessing/dir1"});
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateS3FileList({
                "/test_bucket/Prefix/metadata.json",
                "/test_bucket/Prefix/SchemaMapping/metadata.json",
                "/test_bucket/Prefix/SchemaMapping/mapping.json",
                "/test_bucket/Prefix/ExplicitTable1Prefix/metadata.json",
                "/test_bucket/Prefix/ExplicitTable1Prefix/scheme.pb",
                "/test_bucket/Prefix/ExplicitTable1Prefix/data_00.csv",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/metadata.json",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/scheme.pb",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/data_00.csv",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix");
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbTables({
                "/Root/RestorePrefix/RecursiveFolderProcessing/dir1/Table1",
                "/Root/RestorePrefix/RecursiveFolderProcessing/dir1/dir2/Table2",
            });
        }
    }

    Y_UNIT_TEST(RecursiveDirectoryPlusExplicitTable) {
        // Export dir2 + explicit Table0 not from this dir
        {
            NExport::TExportToS3Settings exportSettings = MakeExportSettings("", "Prefix");
            exportSettings
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/Root/RecursiveFolderProcessing/dir1/dir2"})
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/Root/RecursiveFolderProcessing/Table0"});
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateS3FileList({
                "/test_bucket/Prefix/metadata.json",
                "/test_bucket/Prefix/SchemaMapping/metadata.json",
                "/test_bucket/Prefix/SchemaMapping/mapping.json",
                "/test_bucket/Prefix/RecursiveFolderProcessing/Table0/metadata.json",
                "/test_bucket/Prefix/RecursiveFolderProcessing/Table0/scheme.pb",
                "/test_bucket/Prefix/RecursiveFolderProcessing/Table0/data_00.csv",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/metadata.json",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/scheme.pb",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/data_00.csv",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix");
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbTables({
                "/Root/RestorePrefix/RecursiveFolderProcessing/Table0",
                "/Root/RestorePrefix/RecursiveFolderProcessing/dir1/dir2/Table2",
            });
        }
    }

    Y_UNIT_TEST(EmptyDirectoryIsOk) {
        // Specify empty directory and existing table
        {
            NExport::TExportToS3Settings exportSettings = MakeExportSettings("/Root/RecursiveFolderProcessing/dir1/dir2", "Prefix");
            exportSettings
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/Table2"})
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/Root/RecursiveFolderProcessing/dir1/dir2/dir3"}); // absolute paths are also accepted
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateS3FileList({
                "/test_bucket/Prefix/metadata.json",
                "/test_bucket/Prefix/SchemaMapping/metadata.json",
                "/test_bucket/Prefix/SchemaMapping/mapping.json",
                "/test_bucket/Prefix/Table2/metadata.json",
                "/test_bucket/Prefix/Table2/scheme.pb",
                "/test_bucket/Prefix/Table2/data_00.csv",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix");
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbTables({
                "/Root/RestorePrefix/Table2",
            });
        }
    }

    Y_UNIT_TEST(CommonPrefixButExplicitImportItems) {
        // Export with common prefix, import with explicitly specifying prefixes for each item
        {
            NExport::TExportToS3Settings exportSettings = MakeExportSettings("/Root/RecursiveFolderProcessing/dir1", "Prefix");
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateS3FileList({
                "/test_bucket/Prefix/metadata.json",
                "/test_bucket/Prefix/SchemaMapping/metadata.json",
                "/test_bucket/Prefix/SchemaMapping/mapping.json",
                "/test_bucket/Prefix/Table1/metadata.json",
                "/test_bucket/Prefix/Table1/scheme.pb",
                "/test_bucket/Prefix/Table1/data_00.csv",
                "/test_bucket/Prefix/dir2/Table2/metadata.json",
                "/test_bucket/Prefix/dir2/Table2/scheme.pb",
                "/test_bucket/Prefix/dir2/Table2/data_00.csv",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("", "");
            importSettings
                .AppendItem(NImport::TImportFromS3Settings::TItem{.Src = "Prefix/Table1", .Dst = "/Root/RestorePrefix/Table1"})
                .AppendItem(NImport::TImportFromS3Settings::TItem{.Src = "Prefix/dir2/Table2", .Dst = "/Root/RestorePrefix/dir2/yet/another/dir/Table2"});
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbTables({
                "/Root/RestorePrefix/Table1",
                "/Root/RestorePrefix/dir2/yet/another/dir/Table2",
            });
        }
    }

    Y_UNIT_TEST(ExportDirectoryWithEncryption) {
        // Export directory with encryption
        {
            NExport::TExportToS3Settings exportSettings = MakeExportSettings("/Root/RecursiveFolderProcessing/dir1", "Prefix");
            exportSettings
                .SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateS3FileList({
                "/test_bucket/Prefix/metadata.json",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.enc",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.enc",
                "/test_bucket/Prefix/001/metadata.json.enc",
                "/test_bucket/Prefix/001/scheme.pb.enc",
                "/test_bucket/Prefix/001/data_00.csv.enc",
                "/test_bucket/Prefix/002/metadata.json.enc",
                "/test_bucket/Prefix/002/scheme.pb.enc",
                "/test_bucket/Prefix/002/data_00.csv.enc",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix");
            importSettings
                .SymmetricKey("Cool random key!");
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbTables({
                "/Root/RestorePrefix/Table1",
                "/Root/RestorePrefix/dir2/Table2",
            });
        }
    }

    Y_UNIT_TEST(EncryptedExportWithExplicitDestinationPath) { // supported, but not recommended
        // Export with encryption with explicitly specifying destination path (not recommended, opens explicit path with table name)
        {
            NExport::TExportToS3Settings exportSettings = MakeExportSettings("/Root/RecursiveFolderProcessing", "Prefix");
            exportSettings
                .SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!")
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "Table0", .Dst = "UnsafeTableNameShownInEncryptedBackup"})
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "dir1", .Dst = "Dir1Prefix"}); // Recursive proparation
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateS3FileList({
                "/test_bucket/Prefix/metadata.json",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.enc",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.enc",
                "/test_bucket/Prefix/UnsafeTableNameShownInEncryptedBackup/metadata.json.enc",
                "/test_bucket/Prefix/UnsafeTableNameShownInEncryptedBackup/scheme.pb.enc",
                "/test_bucket/Prefix/UnsafeTableNameShownInEncryptedBackup/data_00.csv.enc",
                "/test_bucket/Prefix/Dir1Prefix/Table1/metadata.json.enc",
                "/test_bucket/Prefix/Dir1Prefix/Table1/scheme.pb.enc",
                "/test_bucket/Prefix/Dir1Prefix/Table1/data_00.csv.enc",
                "/test_bucket/Prefix/Dir1Prefix/dir2/Table2/metadata.json.enc",
                "/test_bucket/Prefix/Dir1Prefix/dir2/Table2/scheme.pb.enc",
                "/test_bucket/Prefix/Dir1Prefix/dir2/Table2/data_00.csv.enc",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix");
            importSettings
                .SymmetricKey("Cool random key!");
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbTables({
                "/Root/RestorePrefix/Table0",
                "/Root/RestorePrefix/dir1/Table1",
                "/Root/RestorePrefix/dir1/dir2/Table2",
            });
        }
    }

    Y_UNIT_TEST(EncryptedExportWithExplicitObjectList) {
        // Export with encryption with explicitly specifying objects list
        {
            NExport::TExportToS3Settings exportSettings = MakeExportSettings("", ""); // no common prefix => error, not allowed with encryption
            exportSettings
                .SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!")
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/Root/RecursiveFolderProcessing/Table0", .Dst = "Table0"})
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/Root/RecursiveFolderProcessing/dir1/Table1", .Dst = "Table1"})
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/Root/RecursiveFolderProcessing/dir1/dir2/Table2", .Dst = "Table2"});
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), EStatus::BAD_REQUEST, "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());

            // Add required parameters and check result
            exportSettings
                .DestinationPrefix("Prefix");
            for (auto& item : exportSettings.Item_) {
                item.Dst.clear();
            }

            res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateS3FileList({
                "/test_bucket/Prefix/metadata.json",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.enc",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.enc",
                "/test_bucket/Prefix/001/metadata.json.enc",
                "/test_bucket/Prefix/001/scheme.pb.enc",
                "/test_bucket/Prefix/001/data_00.csv.enc",
                "/test_bucket/Prefix/002/metadata.json.enc",
                "/test_bucket/Prefix/002/scheme.pb.enc",
                "/test_bucket/Prefix/002/data_00.csv.enc",
                "/test_bucket/Prefix/003/metadata.json.enc",
                "/test_bucket/Prefix/003/scheme.pb.enc",
                "/test_bucket/Prefix/003/data_00.csv.enc",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix");
            importSettings
                .SymmetricKey("Cool random key!");
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbTables({
                "/Root/RestorePrefix/RecursiveFolderProcessing/Table0",
                "/Root/RestorePrefix/RecursiveFolderProcessing/dir1/Table1",
                "/Root/RestorePrefix/RecursiveFolderProcessing/dir1/dir2/Table2",
            });
        }
    }

    Y_UNIT_TEST(ExportCommonSourcePathImportExplicitly) {
        // Export with common source path, import without common path and SchemaMapping
        {
            NExport::TExportToS3Settings exportSettings = MakeExportSettings("/Root/RecursiveFolderProcessing", "Prefix");
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateS3FileList({
                "/test_bucket/Prefix/metadata.json",
                "/test_bucket/Prefix/SchemaMapping/metadata.json",
                "/test_bucket/Prefix/SchemaMapping/mapping.json",
                "/test_bucket/Prefix/Table0/metadata.json",
                "/test_bucket/Prefix/Table0/scheme.pb",
                "/test_bucket/Prefix/Table0/data_00.csv",
                "/test_bucket/Prefix/dir1/Table1/metadata.json",
                "/test_bucket/Prefix/dir1/Table1/scheme.pb",
                "/test_bucket/Prefix/dir1/Table1/data_00.csv",
                "/test_bucket/Prefix/dir1/dir2/Table2/metadata.json",
                "/test_bucket/Prefix/dir1/dir2/Table2/scheme.pb",
                "/test_bucket/Prefix/dir1/dir2/Table2/data_00.csv",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("", "");
            importSettings
                .AppendItem(NImport::TImportFromS3Settings::TItem{.Src = "Prefix/Table0", .Dst = "/Root/RestorePrefix/Table0"});
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbTables({
                "/Root/RestorePrefix/Table0",
            });
            ValidateDoesNotHaveYdbTables({
                "/Root/RestorePrefix/dir1/Table1",
                "/Root/RestorePrefix/dir1/dir2/Table2",
            });
        }
    }

    Y_UNIT_TEST(ImportFilterByPrefix) {
        // Filter import by prefix
        {
            NExport::TExportToS3Settings exportSettings = MakeExportSettings("/Root/RecursiveFolderProcessing", "Prefix");
            exportSettings
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "Table0", .Dst = "Table0_Prefix"})
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "dir1/Table1", .Dst = "Table1_Prefix"})
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "dir1/dir2/Table2", .Dst = "Table2_Prefix"});
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateS3FileList({
                "/test_bucket/Prefix/metadata.json",
                "/test_bucket/Prefix/SchemaMapping/metadata.json",
                "/test_bucket/Prefix/SchemaMapping/mapping.json",
                "/test_bucket/Prefix/Table0_Prefix/metadata.json",
                "/test_bucket/Prefix/Table0_Prefix/scheme.pb",
                "/test_bucket/Prefix/Table0_Prefix/data_00.csv",
                "/test_bucket/Prefix/Table1_Prefix/metadata.json",
                "/test_bucket/Prefix/Table1_Prefix/scheme.pb",
                "/test_bucket/Prefix/Table1_Prefix/data_00.csv",
                "/test_bucket/Prefix/Table2_Prefix/metadata.json",
                "/test_bucket/Prefix/Table2_Prefix/scheme.pb",
                "/test_bucket/Prefix/Table2_Prefix/data_00.csv",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix");
            importSettings
                .AppendItem(NImport::TImportFromS3Settings::TItem{.Src = "Table0_Prefix", .Dst = "/Root/RestorePrefix/Table0"});
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbTables({
                "/Root/RestorePrefix/Table0",
            });
            ValidateDoesNotHaveYdbTables({
                "/Root/RestorePrefix/dir1/Table1",
                "/Root/RestorePrefix/dir1/dir2/Table2",
            });
        }
    }

    Y_UNIT_TEST(ImportFilterByYdbObjectPath) {
        // Filter import by YDB object path
        {
            NExport::TExportToS3Settings exportSettings = MakeExportSettings("/Root/RecursiveFolderProcessing", "Prefix");
            exportSettings
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "Table0", .Dst = "Table0_Prefix"})
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "dir1/Table1", .Dst = "Table1_Prefix"})
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/Root/RecursiveFolderProcessing/dir1/dir2/Table2", .Dst = "Table2_Prefix"});
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateS3FileList({
                "/test_bucket/Prefix/metadata.json",
                "/test_bucket/Prefix/SchemaMapping/metadata.json",
                "/test_bucket/Prefix/SchemaMapping/mapping.json",
                "/test_bucket/Prefix/Table0_Prefix/metadata.json",
                "/test_bucket/Prefix/Table0_Prefix/scheme.pb",
                "/test_bucket/Prefix/Table0_Prefix/data_00.csv",
                "/test_bucket/Prefix/Table1_Prefix/metadata.json",
                "/test_bucket/Prefix/Table1_Prefix/scheme.pb",
                "/test_bucket/Prefix/Table1_Prefix/data_00.csv",
                "/test_bucket/Prefix/Table2_Prefix/metadata.json",
                "/test_bucket/Prefix/Table2_Prefix/scheme.pb",
                "/test_bucket/Prefix/Table2_Prefix/data_00.csv",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix");
            importSettings
                .AppendItem(NImport::TImportFromS3Settings::TItem{.Dst = "/Root/RestorePrefix/Table123", .SrcPath = "dir1/dir2//Table2"})
                .AppendItem(NImport::TImportFromS3Settings::TItem{.Dst = "/Root/RestorePrefix/Table321", .SrcPath = "Table0"});
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbTables({
                "/Root/RestorePrefix/Table123",
                "/Root/RestorePrefix/Table321",
            });
            ValidateDoesNotHaveYdbTables({
                "/Root/RestorePrefix/Table0",
                "/Root/RestorePrefix/dir1/Table1",
                "/Root/RestorePrefix/dir1/dir2/Table2",
            });
        }

        {
            // Both src path and src prefix are incorrect
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix");
            importSettings
                .AppendItem(NImport::TImportFromS3Settings::TItem{.Src = "/Root/RestorePrefix/dir1/dir2/Table2", .Dst = "/Root/RestorePrefix/Table0", .SrcPath = "dir1/dir2/Table2"});
            UNIT_ASSERT_EXCEPTION(YdbImportClient().ImportFromS3(importSettings).GetValueSync(), TContractViolation);
        }
    }

    Y_UNIT_TEST(EncryptedImportWithoutCommonPrefix) {
        // Encrypted export with common source path, import without common path and SchemaMapping (error, encrypted export must be with SchemaMapping)
        {
            NExport::TExportToS3Settings exportSettings = MakeExportSettings("/Root/RecursiveFolderProcessing", "Prefix");
            exportSettings
                .SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateS3FileList({
                "/test_bucket/Prefix/metadata.json",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.enc",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.enc",
                "/test_bucket/Prefix/001/metadata.json.enc",
                "/test_bucket/Prefix/001/scheme.pb.enc",
                "/test_bucket/Prefix/001/data_00.csv.enc",
                "/test_bucket/Prefix/002/metadata.json.enc",
                "/test_bucket/Prefix/002/scheme.pb.enc",
                "/test_bucket/Prefix/002/data_00.csv.enc",
                "/test_bucket/Prefix/003/metadata.json.enc",
                "/test_bucket/Prefix/003/scheme.pb.enc",
                "/test_bucket/Prefix/003/data_00.csv.enc",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("", "");
            importSettings
                .SymmetricKey("Cool random key!")
                .AppendItem(NImport::TImportFromS3Settings::TItem{.Src = "Prefix/001", .Dst = "/Root/RestorePrefix/Table0"});
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), EStatus::BAD_REQUEST, "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(ExplicitDuplicatedItems) {
        // Explicitly specify duplicated items (error)
        NExport::TExportToS3Settings exportSettings = MakeExportSettings("/Root/RecursiveFolderProcessing/dir1", "Prefix");
        exportSettings
            .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "dir2"})
            .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/dir2"})
            .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "dir2/"});
        auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
        WaitOpStatus(res, EStatus::BAD_REQUEST);
    }

    Y_UNIT_TEST(ExportUnexistingExplicitPath) {
        // Export unexisting explicit path
        NExport::TExportToS3Settings exportSettings = MakeExportSettings("/Root/RecursiveFolderProcessing/dir1", "Prefix");
        exportSettings
            .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "unexisting"});
        auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
        WaitOpStatus(res, EStatus::SCHEME_ERROR);
    }

    Y_UNIT_TEST(ExportUnexistingCommonSourcePath) {
        // Export unexisting common source path
        NExport::TExportToS3Settings exportSettings = MakeExportSettings("/Root/RecursiveFolderProcessing/unexisting", "Prefix");
        auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
        WaitOpStatus(res, EStatus::SCHEME_ERROR);
    }

    Y_UNIT_TEST(FilterByPathFailsWhenNoSchemaMapping) {
        // Export without common destination prefix, trying to import with filter by YDB path (error, because no SchemaMapping)
        {
            NExport::TExportToS3Settings exportSettings = MakeExportSettings("/Root/RecursiveFolderProcessing/dir1", "");
            exportSettings
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/Table1", .Dst = "Prefix/t1"});
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateS3FileList({
                "/test_bucket/Prefix/t1/metadata.json",
                "/test_bucket/Prefix/t1/scheme.pb",
                "/test_bucket/Prefix/t1/data_00.csv",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("", "");
            importSettings
                .AppendItem(NImport::TImportFromS3Settings::TItem{.Dst = "/Root/RestorePrefix/Table1", .SrcPath = "/Root/RestorePrefix/Table1"});
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), EStatus::BAD_REQUEST, "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "");
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpStatus(res, EStatus::CANCELLED);
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "");
            importSettings
                .AppendItem(NImport::TImportFromS3Settings::TItem{.Dst = "/Root/RestorePrefix/Table1", .SrcPath = "/Root/RestorePrefix/Table1"});
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpStatus(res, EStatus::CANCELLED);
        }
    }

    Y_UNIT_TEST(OnlyOneEmptyDirectory) {
        // Specify empty directory => error, nothing to export
        NExport::TExportToS3Settings exportSettings = MakeExportSettings("", "Prefix");
        exportSettings
            .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/Root/RecursiveFolderProcessing/dir1/dir2/dir3"});
        auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
        UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), EStatus::BAD_REQUEST, "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
    }

    Y_UNIT_TEST(ExportRecursiveWithoutDestinationPrefix) {
        // Export recursive, but without destination prefix
        {
            NExport::TExportToS3Settings exportSettings = MakeExportSettings("", "");
            exportSettings
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/Root/RecursiveFolderProcessing/dir1", .Dst = "Prefix"});
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateS3FileList({
                "/test_bucket/Prefix/Table1/metadata.json",
                "/test_bucket/Prefix/Table1/scheme.pb",
                "/test_bucket/Prefix/Table1/data_00.csv",
                "/test_bucket/Prefix/dir2/Table2/metadata.json",
                "/test_bucket/Prefix/dir2/Table2/scheme.pb",
                "/test_bucket/Prefix/dir2/Table2/data_00.csv",
            });
        }

        {
            // Impossible to import with common prefix
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "");
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpStatus(res, EStatus::CANCELLED);
        }

        {
            // Possible to import in old style with explicit paths
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("", "");
            importSettings
                .AppendItem(NImport::TImportFromS3Settings::TItem{.Src = "Prefix/Table1", .Dst = "/Root/RestorePrefix/Table11"})
                .AppendItem(NImport::TImportFromS3Settings::TItem{.Src = "Prefix/dir2/Table2", .Dst = "/Root/RestorePrefix/Table12"});
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbTables({
                "/Root/RestorePrefix/Table11",
                "/Root/RestorePrefix/Table12",
            });
        }
    }
}
