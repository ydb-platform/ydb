#include "s3_backup_test_base.h"

#include <util/random/random.h>

#include <contrib/libs/fmt/include/fmt/format.h>
#include <ydb/library/testlib/helpers.h>

using namespace NYdb;

class TBackupPathTestFixture : public TS3BackupTestFixture {
    void SetUp(NUnitTest::TTestContext& /* context */) override {
        using namespace fmt::literals;
        const bool isOlap = TStringBuf{Name_}.EndsWith("+IsOlap");

        auto res = YdbQueryClient().ExecuteQuery(fmt::format(R"sql(
            CREATE TABLE `/Root/RecursiveFolderProcessing/Table0` (
                key Uint32 NOT NULL,
                value String,
                PRIMARY KEY (key)
            ) WITH (
                STORE = {store}
                {partition_count}
            );

            CREATE TABLE `/Root/RecursiveFolderProcessing/dir1/Table1` (
                key Uint32 NOT NULL,
                value String,
                PRIMARY KEY (key)
            ) WITH (
                STORE = {store}
                {partition_count}
            );

            CREATE TABLE `/Root/RecursiveFolderProcessing/dir1/dir2/Table2` (
                key Uint32 NOT NULL,
                value String,
                PRIMARY KEY (key)
            ) WITH (
                STORE = {store}
                {partition_count}
            );
        )sql", "store"_a = isOlap ? "COLUMN" : "ROW",
        "partition_count"_a = isOlap ? ", PARTITION_COUNT = 1" : ""), NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        // Empty dir
        auto mkdir = YdbSchemeClient().MakeDirectory("/Root/RecursiveFolderProcessing/dir1/dir2/dir3").GetValueSync();
        UNIT_ASSERT_C(mkdir.IsSuccess(), mkdir.GetIssues().ToString());
    }

    void TearDown(NUnitTest::TTestContext& /* context */) override {
    }
};

Y_UNIT_TEST_SUITE_F(BackupPathTest, TBackupPathTestFixture) {
    Y_UNIT_TEST_TWIN(ExportWholeDatabase, IsOlap) {
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
                "/test_bucket/Prefix/RecursiveFolderProcessing/Table0/permissions.pb",
                "/test_bucket/Prefix/RecursiveFolderProcessing/Table0/data_00.csv",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/Table1/metadata.json",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/Table1/scheme.pb",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/Table1/permissions.pb",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/Table1/data_00.csv",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/metadata.json",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/scheme.pb",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/permissions.pb",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/data_00.csv",

                "/test_bucket/Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
                "/test_bucket/Prefix/RecursiveFolderProcessing/Table0/metadata.json.sha256",
                "/test_bucket/Prefix/RecursiveFolderProcessing/Table0/scheme.pb.sha256",
                "/test_bucket/Prefix/RecursiveFolderProcessing/Table0/permissions.pb.sha256",
                "/test_bucket/Prefix/RecursiveFolderProcessing/Table0/data_00.csv.sha256",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/Table1/metadata.json.sha256",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/Table1/scheme.pb.sha256",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/Table1/permissions.pb.sha256",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/Table1/data_00.csv.sha256",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/metadata.json.sha256",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/scheme.pb.sha256",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/permissions.pb.sha256",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/data_00.csv.sha256",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix");
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbPaths({
                TEntryPath::TablePath("/Root/RestorePrefix/RecursiveFolderProcessing/Table0", IsOlap),
                TEntryPath::TablePath("/Root/RestorePrefix/RecursiveFolderProcessing/dir1/Table1", IsOlap),
                TEntryPath::TablePath("/Root/RestorePrefix/RecursiveFolderProcessing/dir1/dir2/Table2", IsOlap),
            });
        }
    }

    Y_UNIT_TEST_TWIN(ExportWholeDatabaseWithEncryption, IsOlap) {
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
                "/test_bucket/Prefix/001/permissions.pb.enc",
                "/test_bucket/Prefix/001/data_00.csv.enc",
                "/test_bucket/Prefix/002/metadata.json.enc",
                "/test_bucket/Prefix/002/scheme.pb.enc",
                "/test_bucket/Prefix/002/permissions.pb.enc",
                "/test_bucket/Prefix/002/data_00.csv.enc",
                "/test_bucket/Prefix/003/metadata.json.enc",
                "/test_bucket/Prefix/003/scheme.pb.enc",
                "/test_bucket/Prefix/003/permissions.pb.enc",
                "/test_bucket/Prefix/003/data_00.csv.enc",

                "/test_bucket/Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
                "/test_bucket/Prefix/001/metadata.json.sha256",
                "/test_bucket/Prefix/001/scheme.pb.sha256",
                "/test_bucket/Prefix/001/permissions.pb.sha256",
                "/test_bucket/Prefix/001/data_00.csv.sha256",
                "/test_bucket/Prefix/002/metadata.json.sha256",
                "/test_bucket/Prefix/002/scheme.pb.sha256",
                "/test_bucket/Prefix/002/permissions.pb.sha256",
                "/test_bucket/Prefix/002/data_00.csv.sha256",
                "/test_bucket/Prefix/003/metadata.json.sha256",
                "/test_bucket/Prefix/003/scheme.pb.sha256",
                "/test_bucket/Prefix/003/permissions.pb.sha256",
                "/test_bucket/Prefix/003/data_00.csv.sha256",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix");
            importSettings
                .SymmetricKey("Cool random key!");
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbPaths({
                TEntryPath::TablePath("/Root/RestorePrefix/RecursiveFolderProcessing/Table0", IsOlap),
                TEntryPath::TablePath("/Root/RestorePrefix/RecursiveFolderProcessing/dir1/Table1", IsOlap),
                TEntryPath::TablePath("/Root/RestorePrefix/RecursiveFolderProcessing/dir1/dir2/Table2", IsOlap),
            });
        }
    }

    Y_UNIT_TEST_TWIN(ExportWithCommonSourcePath, IsOlap) {
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
                "/test_bucket/Prefix/Table1/permissions.pb",
                "/test_bucket/Prefix/Table1/data_00.csv",
                "/test_bucket/Prefix/dir2/Table2/metadata.json",
                "/test_bucket/Prefix/dir2/Table2/scheme.pb",
                "/test_bucket/Prefix/dir2/Table2/permissions.pb",
                "/test_bucket/Prefix/dir2/Table2/data_00.csv",

                "/test_bucket/Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
                "/test_bucket/Prefix/Table1/metadata.json.sha256",
                "/test_bucket/Prefix/Table1/scheme.pb.sha256",
                "/test_bucket/Prefix/Table1/permissions.pb.sha256",
                "/test_bucket/Prefix/Table1/data_00.csv.sha256",
                "/test_bucket/Prefix/dir2/Table2/metadata.json.sha256",
                "/test_bucket/Prefix/dir2/Table2/scheme.pb.sha256",
                "/test_bucket/Prefix/dir2/Table2/permissions.pb.sha256",
                "/test_bucket/Prefix/dir2/Table2/data_00.csv.sha256",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix");
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbPaths({
                TEntryPath::TablePath("/Root/RestorePrefix/Table1", IsOlap),
                TEntryPath::TablePath("/Root/RestorePrefix/dir2/Table2", IsOlap),
            });
        }
    }

    Y_UNIT_TEST_TWIN(ExportWithExcludeRegexps, IsOlap) {
        // Export with common source path == dir1
        {
            NExport::TExportToS3Settings exportSettings = MakeExportSettings("/Root/RecursiveFolderProcessing", "Prefix");
            exportSettings
                .AppendExcludeRegexp(".*");
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpStatus(res, EStatus::BAD_REQUEST); // Nothing to export
        }

        {
            NExport::TExportToS3Settings exportSettings = MakeExportSettings("/Root/RecursiveFolderProcessing", "Prefix");
            exportSettings
                .AppendExcludeRegexp("invalid regexp)");
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpStatus(res, EStatus::BAD_REQUEST); // Invalid regexp
        }

        {
            NExport::TExportToS3Settings exportSettings = MakeExportSettings("/Root/RecursiveFolderProcessing", "Prefix");
            exportSettings
                .AppendExcludeRegexp("^Table$") // No matching
                .AppendExcludeRegexp("^dir1$") // Partial match does not prevent from exporting children - only full path match
                .AppendExcludeRegexp("^dir1/Table"); // Matches table in directory
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateS3FileList({
                "/test_bucket/Prefix/metadata.json",
                "/test_bucket/Prefix/SchemaMapping/metadata.json",
                "/test_bucket/Prefix/SchemaMapping/mapping.json",
                "/test_bucket/Prefix/Table0/metadata.json",
                "/test_bucket/Prefix/Table0/scheme.pb",
                "/test_bucket/Prefix/Table0/permissions.pb",
                "/test_bucket/Prefix/Table0/data_00.csv",
                "/test_bucket/Prefix/dir1/dir2/Table2/metadata.json",
                "/test_bucket/Prefix/dir1/dir2/Table2/scheme.pb",
                "/test_bucket/Prefix/dir1/dir2/Table2/permissions.pb",
                "/test_bucket/Prefix/dir1/dir2/Table2/data_00.csv",

                "/test_bucket/Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
                "/test_bucket/Prefix/Table0/metadata.json.sha256",
                "/test_bucket/Prefix/Table0/scheme.pb.sha256",
                "/test_bucket/Prefix/Table0/permissions.pb.sha256",
                "/test_bucket/Prefix/Table0/data_00.csv.sha256",
                "/test_bucket/Prefix/dir1/dir2/Table2/metadata.json.sha256",
                "/test_bucket/Prefix/dir1/dir2/Table2/scheme.pb.sha256",
                "/test_bucket/Prefix/dir1/dir2/Table2/permissions.pb.sha256",
                "/test_bucket/Prefix/dir1/dir2/Table2/data_00.csv.sha256",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix");
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbPaths({
                TEntryPath::TablePath("/Root/RestorePrefix/Table0", IsOlap),
                TEntryPath::TablePath("/Root/RestorePrefix/dir1/dir2/Table2", IsOlap),
            });
        }
    }

    Y_UNIT_TEST_TWIN(ImportWithExcludeRegexps, IsOlap) {
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
                "/test_bucket/Prefix/Table0/permissions.pb",
                "/test_bucket/Prefix/Table0/data_00.csv",
                "/test_bucket/Prefix/dir1/Table1/metadata.json",
                "/test_bucket/Prefix/dir1/Table1/scheme.pb",
                "/test_bucket/Prefix/dir1/Table1/permissions.pb",
                "/test_bucket/Prefix/dir1/Table1/data_00.csv",
                "/test_bucket/Prefix/dir1/dir2/Table2/metadata.json",
                "/test_bucket/Prefix/dir1/dir2/Table2/scheme.pb",
                "/test_bucket/Prefix/dir1/dir2/Table2/permissions.pb",
                "/test_bucket/Prefix/dir1/dir2/Table2/data_00.csv",

                "/test_bucket/Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
                "/test_bucket/Prefix/Table0/metadata.json.sha256",
                "/test_bucket/Prefix/Table0/scheme.pb.sha256",
                "/test_bucket/Prefix/Table0/permissions.pb.sha256",
                "/test_bucket/Prefix/Table0/data_00.csv.sha256",
                "/test_bucket/Prefix/dir1/Table1/metadata.json.sha256",
                "/test_bucket/Prefix/dir1/Table1/scheme.pb.sha256",
                "/test_bucket/Prefix/dir1/Table1/permissions.pb.sha256",
                "/test_bucket/Prefix/dir1/Table1/data_00.csv.sha256",
                "/test_bucket/Prefix/dir1/dir2/Table2/metadata.json.sha256",
                "/test_bucket/Prefix/dir1/dir2/Table2/scheme.pb.sha256",
                "/test_bucket/Prefix/dir1/dir2/Table2/permissions.pb.sha256",
                "/test_bucket/Prefix/dir1/dir2/Table2/data_00.csv.sha256",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix");
            importSettings
                .AppendExcludeRegexp(".*");
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpStatus(res, EStatus::CANCELLED); // Nothing to import
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix");
            importSettings
                .AppendExcludeRegexp("invalid regexp)");
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpStatus(res, EStatus::BAD_REQUEST); // Nothing to import
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix");
            importSettings
                .AppendExcludeRegexp("^Table$") // No matching
                .AppendExcludeRegexp("^dir1$") // Partial match does not prevent from importing children - only full path match
                .AppendExcludeRegexp("^dir1/Table"); // Matches table in directory
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbPaths({
                TEntryPath::TablePath("/Root/RestorePrefix/Table0", IsOlap),
                TEntryPath::TablePath("/Root/RestorePrefix/dir1/dir2/Table2", IsOlap),
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix2");
            importSettings
                .AppendItem(NImport::TImportFromS3Settings::TItem{.SrcPath = "dir1"})
                .AppendExcludeRegexp("Table1");
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbPaths({
                TEntryPath::TablePath("/Root/RestorePrefix2/dir1/dir2/Table2", IsOlap),
            });
        }
    }

    Y_UNIT_TEST_TWIN(ExportWithCommonSourcePathAndExplicitTableInside, IsOlap) {
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
                "/test_bucket/Prefix/ExplicitTable1Prefix/permissions.pb",
                "/test_bucket/Prefix/ExplicitTable1Prefix/data_00.csv",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/metadata.json",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/scheme.pb",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/permissions.pb",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/data_00.csv",

                "/test_bucket/Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
                "/test_bucket/Prefix/ExplicitTable1Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/ExplicitTable1Prefix/scheme.pb.sha256",
                "/test_bucket/Prefix/ExplicitTable1Prefix/permissions.pb.sha256",
                "/test_bucket/Prefix/ExplicitTable1Prefix/data_00.csv.sha256",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/metadata.json.sha256",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/scheme.pb.sha256",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/permissions.pb.sha256",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/data_00.csv.sha256",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix");
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbPaths({
                TEntryPath::TablePath("/Root/RestorePrefix/RecursiveFolderProcessing/dir1/Table1", IsOlap),
                TEntryPath::TablePath("/Root/RestorePrefix/RecursiveFolderProcessing/dir1/dir2/Table2", IsOlap),
            });
        }
    }

    Y_UNIT_TEST_TWIN(RecursiveDirectoryPlusExplicitTable, IsOlap) {
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
                "/test_bucket/Prefix/RecursiveFolderProcessing/Table0/permissions.pb",
                "/test_bucket/Prefix/RecursiveFolderProcessing/Table0/data_00.csv",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/metadata.json",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/scheme.pb",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/permissions.pb",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/data_00.csv",

                "/test_bucket/Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
                "/test_bucket/Prefix/RecursiveFolderProcessing/Table0/metadata.json.sha256",
                "/test_bucket/Prefix/RecursiveFolderProcessing/Table0/scheme.pb.sha256",
                "/test_bucket/Prefix/RecursiveFolderProcessing/Table0/permissions.pb.sha256",
                "/test_bucket/Prefix/RecursiveFolderProcessing/Table0/data_00.csv.sha256",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/metadata.json.sha256",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/scheme.pb.sha256",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/permissions.pb.sha256",
                "/test_bucket/Prefix/RecursiveFolderProcessing/dir1/dir2/Table2/data_00.csv.sha256",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix");
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbPaths({
                TEntryPath::TablePath("/Root/RestorePrefix/RecursiveFolderProcessing/Table0", IsOlap),
                TEntryPath::TablePath("/Root/RestorePrefix/RecursiveFolderProcessing/dir1/dir2/Table2", IsOlap),
            });
        }
    }

    Y_UNIT_TEST_TWIN(EmptyDirectoryIsOk, IsOlap) {
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
                "/test_bucket/Prefix/Table2/permissions.pb",
                "/test_bucket/Prefix/Table2/data_00.csv",

                "/test_bucket/Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
                "/test_bucket/Prefix/Table2/metadata.json.sha256",
                "/test_bucket/Prefix/Table2/scheme.pb.sha256",
                "/test_bucket/Prefix/Table2/permissions.pb.sha256",
                "/test_bucket/Prefix/Table2/data_00.csv.sha256",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix");
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbPaths({
                TEntryPath::TablePath("/Root/RestorePrefix/Table2", IsOlap),
            });
        }
    }

    Y_UNIT_TEST_TWIN(CommonPrefixButExplicitImportItems, IsOlap) {
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
                "/test_bucket/Prefix/Table1/permissions.pb",
                "/test_bucket/Prefix/Table1/data_00.csv",
                "/test_bucket/Prefix/dir2/Table2/metadata.json",
                "/test_bucket/Prefix/dir2/Table2/scheme.pb",
                "/test_bucket/Prefix/dir2/Table2/permissions.pb",
                "/test_bucket/Prefix/dir2/Table2/data_00.csv",

                "/test_bucket/Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
                "/test_bucket/Prefix/Table1/metadata.json.sha256",
                "/test_bucket/Prefix/Table1/scheme.pb.sha256",
                "/test_bucket/Prefix/Table1/permissions.pb.sha256",
                "/test_bucket/Prefix/Table1/data_00.csv.sha256",
                "/test_bucket/Prefix/dir2/Table2/metadata.json.sha256",
                "/test_bucket/Prefix/dir2/Table2/scheme.pb.sha256",
                "/test_bucket/Prefix/dir2/Table2/permissions.pb.sha256",
                "/test_bucket/Prefix/dir2/Table2/data_00.csv.sha256",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("", "");
            importSettings
                .AppendItem(NImport::TImportFromS3Settings::TItem{.Src = "Prefix/Table1", .Dst = "/Root/RestorePrefix/Table1"})
                .AppendItem(NImport::TImportFromS3Settings::TItem{.Src = "Prefix/dir2/Table2", .Dst = "/Root/RestorePrefix/dir2/yet/another/dir/Table2"});
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbPaths({
                TEntryPath::TablePath("/Root/RestorePrefix/Table1", IsOlap),
                TEntryPath::TablePath("/Root/RestorePrefix/dir2/yet/another/dir/Table2", IsOlap),
            });
        }
    }

    Y_UNIT_TEST_TWIN(ExportDirectoryWithEncryption, IsOlap) {
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
                "/test_bucket/Prefix/001/permissions.pb.enc",
                "/test_bucket/Prefix/001/data_00.csv.enc",
                "/test_bucket/Prefix/002/metadata.json.enc",
                "/test_bucket/Prefix/002/scheme.pb.enc",
                "/test_bucket/Prefix/002/permissions.pb.enc",
                "/test_bucket/Prefix/002/data_00.csv.enc",

                "/test_bucket/Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
                "/test_bucket/Prefix/001/metadata.json.sha256",
                "/test_bucket/Prefix/001/scheme.pb.sha256",
                "/test_bucket/Prefix/001/permissions.pb.sha256",
                "/test_bucket/Prefix/001/data_00.csv.sha256",
                "/test_bucket/Prefix/002/metadata.json.sha256",
                "/test_bucket/Prefix/002/scheme.pb.sha256",
                "/test_bucket/Prefix/002/permissions.pb.sha256",
                "/test_bucket/Prefix/002/data_00.csv.sha256",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix");
            importSettings
                .SymmetricKey("Cool random key!");
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbPaths({
                TEntryPath::TablePath("/Root/RestorePrefix/Table1", IsOlap),
                TEntryPath::TablePath("/Root/RestorePrefix/dir2/Table2", IsOlap),
            });
        }
    }

    Y_UNIT_TEST_TWIN(EncryptedExportWithExplicitDestinationPath, IsOlap) { // supported, but not recommended
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
                "/test_bucket/Prefix/UnsafeTableNameShownInEncryptedBackup/permissions.pb.enc",
                "/test_bucket/Prefix/UnsafeTableNameShownInEncryptedBackup/data_00.csv.enc",
                "/test_bucket/Prefix/Dir1Prefix/Table1/metadata.json.enc",
                "/test_bucket/Prefix/Dir1Prefix/Table1/scheme.pb.enc",
                "/test_bucket/Prefix/Dir1Prefix/Table1/permissions.pb.enc",
                "/test_bucket/Prefix/Dir1Prefix/Table1/data_00.csv.enc",
                "/test_bucket/Prefix/Dir1Prefix/dir2/Table2/metadata.json.enc",
                "/test_bucket/Prefix/Dir1Prefix/dir2/Table2/scheme.pb.enc",
                "/test_bucket/Prefix/Dir1Prefix/dir2/Table2/permissions.pb.enc",
                "/test_bucket/Prefix/Dir1Prefix/dir2/Table2/data_00.csv.enc",

                "/test_bucket/Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
                "/test_bucket/Prefix/UnsafeTableNameShownInEncryptedBackup/metadata.json.sha256",
                "/test_bucket/Prefix/UnsafeTableNameShownInEncryptedBackup/scheme.pb.sha256",
                "/test_bucket/Prefix/UnsafeTableNameShownInEncryptedBackup/permissions.pb.sha256",
                "/test_bucket/Prefix/UnsafeTableNameShownInEncryptedBackup/data_00.csv.sha256",
                "/test_bucket/Prefix/Dir1Prefix/Table1/metadata.json.sha256",
                "/test_bucket/Prefix/Dir1Prefix/Table1/scheme.pb.sha256",
                "/test_bucket/Prefix/Dir1Prefix/Table1/permissions.pb.sha256",
                "/test_bucket/Prefix/Dir1Prefix/Table1/data_00.csv.sha256",
                "/test_bucket/Prefix/Dir1Prefix/dir2/Table2/metadata.json.sha256",
                "/test_bucket/Prefix/Dir1Prefix/dir2/Table2/scheme.pb.sha256",
                "/test_bucket/Prefix/Dir1Prefix/dir2/Table2/permissions.pb.sha256",
                "/test_bucket/Prefix/Dir1Prefix/dir2/Table2/data_00.csv.sha256",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix");
            importSettings
                .SymmetricKey("Cool random key!");
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbPaths({
                TEntryPath::TablePath("/Root/RestorePrefix/Table0", IsOlap),
                TEntryPath::TablePath("/Root/RestorePrefix/dir1/Table1", IsOlap),
                TEntryPath::TablePath("/Root/RestorePrefix/dir1/dir2/Table2", IsOlap),
            });
        }
    }

    Y_UNIT_TEST_TWIN(EncryptedExportWithExplicitObjectList, IsOlap) {
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
                "/test_bucket/Prefix/001/permissions.pb.enc",
                "/test_bucket/Prefix/001/data_00.csv.enc",
                "/test_bucket/Prefix/002/metadata.json.enc",
                "/test_bucket/Prefix/002/scheme.pb.enc",
                "/test_bucket/Prefix/002/permissions.pb.enc",
                "/test_bucket/Prefix/002/data_00.csv.enc",
                "/test_bucket/Prefix/003/metadata.json.enc",
                "/test_bucket/Prefix/003/scheme.pb.enc",
                "/test_bucket/Prefix/003/permissions.pb.enc",
                "/test_bucket/Prefix/003/data_00.csv.enc",

                "/test_bucket/Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
                "/test_bucket/Prefix/001/metadata.json.sha256",
                "/test_bucket/Prefix/001/scheme.pb.sha256",
                "/test_bucket/Prefix/001/permissions.pb.sha256",
                "/test_bucket/Prefix/001/data_00.csv.sha256",
                "/test_bucket/Prefix/002/metadata.json.sha256",
                "/test_bucket/Prefix/002/scheme.pb.sha256",
                "/test_bucket/Prefix/002/permissions.pb.sha256",
                "/test_bucket/Prefix/002/data_00.csv.sha256",
                "/test_bucket/Prefix/003/metadata.json.sha256",
                "/test_bucket/Prefix/003/scheme.pb.sha256",
                "/test_bucket/Prefix/003/permissions.pb.sha256",
                "/test_bucket/Prefix/003/data_00.csv.sha256",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix");
            importSettings
                .SymmetricKey("Cool random key!");
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbPaths({
                TEntryPath::TablePath("/Root/RestorePrefix/RecursiveFolderProcessing/Table0", IsOlap),
                TEntryPath::TablePath("/Root/RestorePrefix/RecursiveFolderProcessing/dir1/Table1", IsOlap),
                TEntryPath::TablePath("/Root/RestorePrefix/RecursiveFolderProcessing/dir1/dir2/Table2", IsOlap),
            });
        }
    }

    Y_UNIT_TEST_TWIN(ExportCommonSourcePathImportExplicitly, IsOlap) {
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
                "/test_bucket/Prefix/Table0/permissions.pb",
                "/test_bucket/Prefix/Table0/data_00.csv",
                "/test_bucket/Prefix/dir1/Table1/metadata.json",
                "/test_bucket/Prefix/dir1/Table1/scheme.pb",
                "/test_bucket/Prefix/dir1/Table1/permissions.pb",
                "/test_bucket/Prefix/dir1/Table1/data_00.csv",
                "/test_bucket/Prefix/dir1/dir2/Table2/metadata.json",
                "/test_bucket/Prefix/dir1/dir2/Table2/scheme.pb",
                "/test_bucket/Prefix/dir1/dir2/Table2/permissions.pb",
                "/test_bucket/Prefix/dir1/dir2/Table2/data_00.csv",

                "/test_bucket/Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
                "/test_bucket/Prefix/Table0/metadata.json.sha256",
                "/test_bucket/Prefix/Table0/scheme.pb.sha256",
                "/test_bucket/Prefix/Table0/permissions.pb.sha256",
                "/test_bucket/Prefix/Table0/data_00.csv.sha256",
                "/test_bucket/Prefix/dir1/Table1/metadata.json.sha256",
                "/test_bucket/Prefix/dir1/Table1/scheme.pb.sha256",
                "/test_bucket/Prefix/dir1/Table1/permissions.pb.sha256",
                "/test_bucket/Prefix/dir1/Table1/data_00.csv.sha256",
                "/test_bucket/Prefix/dir1/dir2/Table2/metadata.json.sha256",
                "/test_bucket/Prefix/dir1/dir2/Table2/scheme.pb.sha256",
                "/test_bucket/Prefix/dir1/dir2/Table2/permissions.pb.sha256",
                "/test_bucket/Prefix/dir1/dir2/Table2/data_00.csv.sha256",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("", "");
            importSettings
                .AppendItem(NImport::TImportFromS3Settings::TItem{.Src = "Prefix/Table0", .Dst = "/Root/RestorePrefix/Table0"});
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbPaths({
                TEntryPath::TablePath("/Root/RestorePrefix/Table0", IsOlap),
            });
            ValidateDoesNotHaveYdbTables({
                "/Root/RestorePrefix/dir1/Table1",
                "/Root/RestorePrefix/dir1/dir2/Table2",
            });
        }
    }

    Y_UNIT_TEST_TWIN(ImportFilterByPrefix, IsOlap) {
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
                "/test_bucket/Prefix/Table0_Prefix/permissions.pb",
                "/test_bucket/Prefix/Table0_Prefix/data_00.csv",
                "/test_bucket/Prefix/Table1_Prefix/metadata.json",
                "/test_bucket/Prefix/Table1_Prefix/scheme.pb",
                "/test_bucket/Prefix/Table1_Prefix/permissions.pb",
                "/test_bucket/Prefix/Table1_Prefix/data_00.csv",
                "/test_bucket/Prefix/Table2_Prefix/metadata.json",
                "/test_bucket/Prefix/Table2_Prefix/scheme.pb",
                "/test_bucket/Prefix/Table2_Prefix/permissions.pb",
                "/test_bucket/Prefix/Table2_Prefix/data_00.csv",

                "/test_bucket/Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
                "/test_bucket/Prefix/Table0_Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/Table0_Prefix/scheme.pb.sha256",
                "/test_bucket/Prefix/Table0_Prefix/permissions.pb.sha256",
                "/test_bucket/Prefix/Table0_Prefix/data_00.csv.sha256",
                "/test_bucket/Prefix/Table1_Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/Table1_Prefix/scheme.pb.sha256",
                "/test_bucket/Prefix/Table1_Prefix/permissions.pb.sha256",
                "/test_bucket/Prefix/Table1_Prefix/data_00.csv.sha256",
                "/test_bucket/Prefix/Table2_Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/Table2_Prefix/scheme.pb.sha256",
                "/test_bucket/Prefix/Table2_Prefix/permissions.pb.sha256",
                "/test_bucket/Prefix/Table2_Prefix/data_00.csv.sha256",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix");
            importSettings
                .AppendItem(NImport::TImportFromS3Settings::TItem{.Src = "Table0_Prefix", .Dst = "/Root/RestorePrefix/Table0"});
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbPaths({
                TEntryPath::TablePath("/Root/RestorePrefix/Table0", IsOlap),
            });
            ValidateDoesNotHaveYdbTables({
                "/Root/RestorePrefix/dir1/Table1",
                "/Root/RestorePrefix/dir1/dir2/Table2",
            });
        }
    }

    Y_UNIT_TEST_TWIN(ImportFilterByYdbObjectPath, IsOlap) {
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
                "/test_bucket/Prefix/Table0_Prefix/permissions.pb",
                "/test_bucket/Prefix/Table0_Prefix/data_00.csv",
                "/test_bucket/Prefix/Table1_Prefix/metadata.json",
                "/test_bucket/Prefix/Table1_Prefix/scheme.pb",
                "/test_bucket/Prefix/Table1_Prefix/permissions.pb",
                "/test_bucket/Prefix/Table1_Prefix/data_00.csv",
                "/test_bucket/Prefix/Table2_Prefix/metadata.json",
                "/test_bucket/Prefix/Table2_Prefix/scheme.pb",
                "/test_bucket/Prefix/Table2_Prefix/permissions.pb",
                "/test_bucket/Prefix/Table2_Prefix/data_00.csv",

                "/test_bucket/Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
                "/test_bucket/Prefix/Table0_Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/Table0_Prefix/scheme.pb.sha256",
                "/test_bucket/Prefix/Table0_Prefix/permissions.pb.sha256",
                "/test_bucket/Prefix/Table0_Prefix/data_00.csv.sha256",
                "/test_bucket/Prefix/Table1_Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/Table1_Prefix/scheme.pb.sha256",
                "/test_bucket/Prefix/Table1_Prefix/permissions.pb.sha256",
                "/test_bucket/Prefix/Table1_Prefix/data_00.csv.sha256",
                "/test_bucket/Prefix/Table2_Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/Table2_Prefix/scheme.pb.sha256",
                "/test_bucket/Prefix/Table2_Prefix/permissions.pb.sha256",
                "/test_bucket/Prefix/Table2_Prefix/data_00.csv.sha256",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix");
            importSettings
                .AppendItem(NImport::TImportFromS3Settings::TItem{.Dst = "/Root/RestorePrefix/Table123", .SrcPath = "dir1/dir2//Table2"})
                .AppendItem(NImport::TImportFromS3Settings::TItem{.Dst = "/Root/RestorePrefix/Table321", .SrcPath = "Table0"});
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbPaths({
                TEntryPath::TablePath("/Root/RestorePrefix/Table123", IsOlap),
                TEntryPath::TablePath("/Root/RestorePrefix/Table321", IsOlap),
            });
            ValidateDoesNotHaveYdbTables({
                "/Root/RestorePrefix/Table0",
                "/Root/RestorePrefix/dir1/Table1",
                "/Root/RestorePrefix/dir1/dir2/Table2",
            });
        }

        // Recursive filter by directory
        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestorePrefix2");
            importSettings
                .AppendItem(NImport::TImportFromS3Settings::TItem{.SrcPath = "dir1"});
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ValidateHasYdbPaths({
                TEntryPath::TablePath("/Root/RestorePrefix2/dir1/Table1", IsOlap),
                TEntryPath::TablePath("/Root/RestorePrefix2/dir1/dir2/Table2", IsOlap),
            });
            ValidateDoesNotHaveYdbTables({
                "/Root/RestorePrefix2/Table0",
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

    Y_UNIT_TEST_TWIN(EncryptedImportWithoutCommonPrefix, IsOlap) {
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
                "/test_bucket/Prefix/001/permissions.pb.enc",
                "/test_bucket/Prefix/001/data_00.csv.enc",
                "/test_bucket/Prefix/002/metadata.json.enc",
                "/test_bucket/Prefix/002/scheme.pb.enc",
                "/test_bucket/Prefix/002/permissions.pb.enc",
                "/test_bucket/Prefix/002/data_00.csv.enc",
                "/test_bucket/Prefix/003/metadata.json.enc",
                "/test_bucket/Prefix/003/scheme.pb.enc",
                "/test_bucket/Prefix/003/permissions.pb.enc",
                "/test_bucket/Prefix/003/data_00.csv.enc",

                "/test_bucket/Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
                "/test_bucket/Prefix/001/metadata.json.sha256",
                "/test_bucket/Prefix/001/scheme.pb.sha256",
                "/test_bucket/Prefix/001/permissions.pb.sha256",
                "/test_bucket/Prefix/001/data_00.csv.sha256",
                "/test_bucket/Prefix/002/metadata.json.sha256",
                "/test_bucket/Prefix/002/scheme.pb.sha256",
                "/test_bucket/Prefix/002/permissions.pb.sha256",
                "/test_bucket/Prefix/002/data_00.csv.sha256",
                "/test_bucket/Prefix/003/metadata.json.sha256",
                "/test_bucket/Prefix/003/scheme.pb.sha256",
                "/test_bucket/Prefix/003/permissions.pb.sha256",
                "/test_bucket/Prefix/003/data_00.csv.sha256",
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

    Y_UNIT_TEST_TWIN(ExplicitDuplicatedItems, IsOlap) {
        // Explicitly specify duplicated items (error)
        NExport::TExportToS3Settings exportSettings = MakeExportSettings("/Root/RecursiveFolderProcessing/dir1", "Prefix");
        exportSettings
            .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "dir2"})
            .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/dir2"})
            .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "dir2/"});
        auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
        WaitOpStatus(res, EStatus::BAD_REQUEST);
    }

    Y_UNIT_TEST_TWIN(ExportUnexistingExplicitPath, IsOlap) {
        // Export unexisting explicit path
        NExport::TExportToS3Settings exportSettings = MakeExportSettings("/Root/RecursiveFolderProcessing/dir1", "Prefix");
        exportSettings
            .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "unexisting"});
        auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
        WaitOpStatus(res, EStatus::SCHEME_ERROR);
    }

    Y_UNIT_TEST_TWIN(ExportUnexistingCommonSourcePath, IsOlap) {
        // Export unexisting common source path
        NExport::TExportToS3Settings exportSettings = MakeExportSettings("/Root/RecursiveFolderProcessing/unexisting", "Prefix");
        auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
        WaitOpStatus(res, EStatus::SCHEME_ERROR);
    }

    Y_UNIT_TEST_TWIN(FilterByPathFailsWhenNoSchemaMapping, IsOlap) {
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
                "/test_bucket/Prefix/t1/permissions.pb",
                "/test_bucket/Prefix/t1/data_00.csv",

                "/test_bucket/Prefix/t1/metadata.json.sha256",
                "/test_bucket/Prefix/t1/scheme.pb.sha256",
                "/test_bucket/Prefix/t1/permissions.pb.sha256",
                "/test_bucket/Prefix/t1/data_00.csv.sha256",
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

    Y_UNIT_TEST_TWIN(OnlyOneEmptyDirectory, IsOlap) {
        // Specify empty directory => error, nothing to export
        NExport::TExportToS3Settings exportSettings = MakeExportSettings("", "Prefix");
        exportSettings
            .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/Root/RecursiveFolderProcessing/dir1/dir2/dir3"});
        auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
        UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), EStatus::BAD_REQUEST, "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(ExportRecursiveWithoutDestinationPrefix, IsOlap) {
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
                "/test_bucket/Prefix/Table1/permissions.pb",
                "/test_bucket/Prefix/Table1/data_00.csv",
                "/test_bucket/Prefix/dir2/Table2/metadata.json",
                "/test_bucket/Prefix/dir2/Table2/scheme.pb",
                "/test_bucket/Prefix/dir2/Table2/permissions.pb",
                "/test_bucket/Prefix/dir2/Table2/data_00.csv",

                "/test_bucket/Prefix/Table1/metadata.json.sha256",
                "/test_bucket/Prefix/Table1/scheme.pb.sha256",
                "/test_bucket/Prefix/Table1/permissions.pb.sha256",
                "/test_bucket/Prefix/Table1/data_00.csv.sha256",
                "/test_bucket/Prefix/dir2/Table2/metadata.json.sha256",
                "/test_bucket/Prefix/dir2/Table2/scheme.pb.sha256",
                "/test_bucket/Prefix/dir2/Table2/permissions.pb.sha256",
                "/test_bucket/Prefix/dir2/Table2/data_00.csv.sha256",
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

            ValidateHasYdbPaths({
                TEntryPath::TablePath("/Root/RestorePrefix/Table11", IsOlap),
                TEntryPath::TablePath("/Root/RestorePrefix/Table12", IsOlap),
            });
        }
    }

    Y_UNIT_TEST_TWIN(ParallelBackupWholeDatabase, IsOlap) {
        if (IsOlap) {
            return;  // TODO: fix me issue@26498 (need a copy column table here)
        }
        using namespace fmt::literals;
        {
            auto res = YdbQueryClient().ExecuteQuery(R"sql(
                INSERT INTO `/Root/RecursiveFolderProcessing/Table0` (key) VALUES (1);
                INSERT INTO `/Root/RecursiveFolderProcessing/dir1/Table1` (key) VALUES (2);
                INSERT INTO `/Root/RecursiveFolderProcessing/dir1/dir2/Table2` (key) VALUES (3);
            )sql", NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        // Check that backup process does not export directories created by parallel export (/Root/export-123)
        constexpr size_t parallelExportsCount = 5;
        {
            std::vector<NThreading::TFuture<NExport::TExportToS3Response>> parallelBackups(parallelExportsCount);

            // Start parallel backups
            // They are expected not to export special export copies of tables (/Root/export-123), and also ".sys" and ".metadata" folders
            for (size_t i = 0; i < parallelBackups.size(); ++i) {
                auto& backupOp = parallelBackups[i];
                NExport::TExportToS3Settings settings = MakeExportSettings("", TStringBuilder() << "ParallelBackupWholeDatabasePrefix_" << i);
                backupOp = YdbExportClient().ExportToS3(settings);
            }

            // Wait
            for (auto& backupOp : parallelBackups) {
                WaitOpSuccess(backupOp.GetValueSync());
            }

            // Forget
            for (auto& backupOp : parallelBackups) {
                auto forgetResult = YdbOperationClient().Forget(backupOp.GetValueSync().Id()).GetValueSync();
                UNIT_ASSERT_C(forgetResult.IsSuccess(), forgetResult.GetIssues().ToString());
            }
        }

        for (size_t i = 0; i < parallelExportsCount; ++i) {
            NImport::TImportFromS3Settings settings = MakeImportSettings(TStringBuilder() << "ParallelBackupWholeDatabasePrefix_" << i, TStringBuilder() << "/Root/Restored_" << i);

            const auto restoreOp = YdbImportClient().ImportFromS3(settings).GetValueSync();
            WaitOpSuccess(restoreOp);

            // Check that there are only expected tables
            auto checkOneTableInDirectory = [&](const TString& dir, const TString& name) {
                auto listResult = YdbSchemeClient().ListDirectory(TStringBuilder() << "/Root/Restored_" << i << "/" << dir).GetValueSync();
                UNIT_ASSERT_C(listResult.IsSuccess(), listResult.GetIssues().ToString());
                size_t tablesFound = 0;
                size_t tableIndex = 0;
                for (size_t i = 0; i < listResult.GetChildren().size(); ++i) {
                    const auto& child = listResult.GetChildren()[i];
                    if (child.Type == NYdb::NScheme::ESchemeEntryType::Table) {
                        ++tablesFound;
                        tableIndex = i;
                    }
                }
                UNIT_ASSERT_VALUES_EQUAL_C(tablesFound, 1, "Current directory \"/Root/Restored_" << i << "/" << dir << "\" children: " << DebugListDir(TStringBuilder() << "/Root/Restored_" << i << "/" << dir));
                UNIT_ASSERT_VALUES_EQUAL(listResult.GetChildren()[tableIndex].Name, name);
            };
            checkOneTableInDirectory("RecursiveFolderProcessing", "Table0");
            checkOneTableInDirectory("RecursiveFolderProcessing/dir1", "Table1");
            checkOneTableInDirectory("RecursiveFolderProcessing/dir1/dir2", "Table2");
        }

        // Test restore to database root
        {
            // Remove all contents from database
            auto removeTable = [&](const TString& path) {
                auto session = YdbTableClient().GetSession().GetValueSync();
                UNIT_ASSERT_C(session.IsSuccess(), session.GetIssues().ToString());
                auto res = session.GetSession().DropTable(path).GetValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), "Drop table \"" << path << "\" failed: " << res.GetIssues().ToString());
            };
            auto removeDirectory = [&](const TString& path, bool ignoreErrors = false) {
                auto res = YdbSchemeClient().RemoveDirectory(path).GetValueSync();
                UNIT_ASSERT_C(ignoreErrors || res.IsSuccess(), "Drop directory \"" << path << "\" failed: " << res.GetIssues().ToString() << ". Current directory children: " << DebugListDir(path));
            };
            auto remove = [&](const TString& root, bool removeRoot = true) {
                removeTable(TStringBuilder() << root << "/RecursiveFolderProcessing/Table0");
                removeTable(TStringBuilder() << root << "/RecursiveFolderProcessing/dir1/Table1");
                removeTable(TStringBuilder() << root << "/RecursiveFolderProcessing/dir1/dir2/Table2");
                removeDirectory(TStringBuilder() << root << "/RecursiveFolderProcessing/dir1/dir2/dir3", true); // We don't restore empty dirs
                removeDirectory(TStringBuilder() << root << "/RecursiveFolderProcessing/dir1/dir2");
                removeDirectory(TStringBuilder() << root << "/RecursiveFolderProcessing/dir1");
                removeDirectory(TStringBuilder() << root << "/RecursiveFolderProcessing");
                if (removeRoot) {
                    removeDirectory(root);
                }
            };
            for (size_t i = 0; i < parallelExportsCount; ++i) {
                remove(TStringBuilder() << "/Root/Restored_" << i);
            }
            remove("/Root", false);

            const auto listResult = YdbSchemeClient().ListDirectory("/Root").GetValueSync();
            std::unordered_set<std::string> expectedResult = {".sys", ".metadata"};
            UNIT_ASSERT_C(listResult.IsSuccess(), listResult.GetIssues().ToString());
            UNIT_ASSERT_C(listResult.GetChildren().size() <= 2, "Current database directory children: " << DebugListDir("/Root"));
            for (const auto& entry : listResult.GetChildren()) {
                UNIT_ASSERT_C(expectedResult.erase(entry.Name), "Unexpected child: " << entry.Name);
            }

            // Import to database root
            NImport::TImportFromS3Settings settings = MakeImportSettings("ParallelBackupWholeDatabasePrefix_0", "");

            const auto restoreOp = YdbImportClient().ImportFromS3(settings).GetValueSync();
            WaitOpSuccess(restoreOp);

            // Check data
            auto checkTableData = [&](const TString& path, ui32 data) {
                auto result = YdbQueryClient().ExecuteQuery(
                    fmt::format(R"sql(
                        SELECT key FROM `{table_path}`;
                    )sql",
                    "table_path"_a = path
                    ),
                    NQuery::TTxControl::BeginTx().CommitTx()
                ).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto resultSet = result.GetResultSetParser(0);
                UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1);
                UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 1);
                UNIT_ASSERT(resultSet.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUint32(), data);
            };

            checkTableData("/Root/RecursiveFolderProcessing/Table0", 1);
            checkTableData("/Root/RecursiveFolderProcessing/dir1/Table1", 2);
            checkTableData("/Root/RecursiveFolderProcessing/dir1/dir2/Table2", 3);
        }
    }

    Y_UNIT_TEST_TWIN(ChecksumsForSchemaMappingFiles, IsOlap) {
        Server().GetRuntime()->GetAppData().FeatureFlags.SetEnableChecksumsExport(true);

        {
            NExport::TExportToS3Settings settings = MakeExportSettings("/Root/RecursiveFolderProcessing/dir1/dir2", "Prefix");
            settings
                .Compression("zstd");

            auto res = YdbExportClient().ExportToS3(settings).GetValueSync();
            WaitOpSuccess(res);

            ValidateS3FileList({
                "/test_bucket/Prefix/metadata.json",
                "/test_bucket/Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/metadata.json",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/mapping.json",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
                "/test_bucket/Prefix/Table2/metadata.json",
                "/test_bucket/Prefix/Table2/metadata.json.sha256",
                "/test_bucket/Prefix/Table2/scheme.pb",
                "/test_bucket/Prefix/Table2/scheme.pb.sha256",
                "/test_bucket/Prefix/Table2/permissions.pb",
                "/test_bucket/Prefix/Table2/permissions.pb.sha256",
                "/test_bucket/Prefix/Table2/data_00.csv.zst",
                "/test_bucket/Prefix/Table2/data_00.csv.sha256",
            });
        }

        {
            NImport::TImportFromS3Settings importSettings = MakeImportSettings("Prefix", "/Root/RestoredPath");
            auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
            WaitOpSuccess(res);

            ModifyChecksumAndCheckThatImportFails({
                "/test_bucket/Prefix/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/metadata.json.sha256",
                "/test_bucket/Prefix/SchemaMapping/mapping.json.sha256",
                "/test_bucket/Prefix/Table2/metadata.json.sha256",
                "/test_bucket/Prefix/Table2/scheme.pb.sha256",
                "/test_bucket/Prefix/Table2/data_00.csv.sha256",
            }, importSettings);
        }
    }

    // Test that covers races between processing and cancellation
    Y_UNIT_TEST_TWIN(CancelWhileProcessing, IsOlap) {
        using namespace fmt::literals;
        
        if (IsOlap) {
            // TODO: fix issue #26498 (need a copy-column table here,
            // because there isn't a proper cancel for DataShard and ColumnShard,
            // copy table will hide this problem)
            return;  
        }

        // Make tables for parallel export
        auto createSchemaResult = YdbQueryClient().ExecuteQuery(fmt::format(R"sql(
            CREATE TABLE `/Root/Table0` (
                key Uint32 NOT NULL,
                value String,
                PRIMARY KEY (key)
            ) WITH (
                STORE = {store}
                {partition_count}
            );

            CREATE TABLE `/Root/Table1` (
                key Uint32 NOT NULL,
                value String,
                PRIMARY KEY (key)
            ) WITH (
                STORE = {store}
                {partition_count}
            );

            CREATE TABLE `/Root/Table2` (
                key Uint32 NOT NULL,
                value String,
                PRIMARY KEY (key)
            ) WITH (
                STORE = {store}
                {partition_count}
            );

            CREATE TABLE `/Root/Table3` (
                key Uint32 NOT NULL,
                value String,
                PRIMARY KEY (key)
            ) WITH (
                STORE = {store}
                {partition_count}
            );

            CREATE TABLE `/Root/Table4` (
                key Uint32 NOT NULL,
                value String,
                PRIMARY KEY (key)
            ) WITH (
                STORE = {store}
                {partition_count}
            );
        )sql", "store"_a = IsOlap ? "COLUMN" : "ROW",
        "partition_count"_a = IsOlap ? ", PARTITION_COUNT = 1" : ""), NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(createSchemaResult.IsSuccess(), createSchemaResult.GetIssues().ToString());

        for (bool cancelExport : {true, false}) {
            TString exportPrefix = TStringBuilder() << "Prefix_" << cancelExport;
            NExport::TExportToS3Settings exportSettings = MakeExportSettings("", exportPrefix);
            auto exportResult = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            if (cancelExport) {
                Sleep(TDuration::MilliSeconds(RandomNumber<ui64>(1500)));
                YdbOperationClient().Cancel(exportResult.Id()).GetValueSync();
                WaitOpStatus(exportResult, {NYdb::EStatus::SUCCESS, NYdb::EStatus::CANCELLED});
                continue;
            }
            WaitOpSuccess(exportResult);

            NImport::TImportFromS3Settings importSettings = MakeImportSettings(exportPrefix, "/Root/RestorePrefix");
            auto importResult = YdbImportClient().ImportFromS3(importSettings).GetValueSync();

            Sleep(TDuration::MilliSeconds(RandomNumber<ui64>(1500)));
            YdbOperationClient().Cancel(importResult.Id()).GetValueSync();
            WaitOpStatus(importResult, {NYdb::EStatus::SUCCESS, NYdb::EStatus::CANCELLED});
        }
    }
}
