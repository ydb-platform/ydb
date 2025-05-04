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

            // Validate that we have at least these S3 files (YDB recipe is common, => there are extra files here from different test tables)
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
