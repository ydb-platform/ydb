#include <ydb/tests/functional/backup/helpers/backup_test_fixture.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb;

Y_UNIT_TEST_SUITE_F(S3PathStyleBackup, TBackupTestFixture)
{
    Y_UNIT_TEST(DisableVirtualAddressing)
    {
        {
            auto session = YdbTableClient().GetSession().GetValueSync().GetSession();
            auto res = session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/local/Table` (
                    Key Uint32,
                    PRIMARY KEY (Key)
                );
            )").GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        const TString bucketName = "my-bucket";
        CreateBucket(bucketName);

        auto fillS3Settings = [&](auto& settings) {
            settings.Endpoint(S3Endpoint());
            settings.Bucket(bucketName);
            settings.AccessKey("minio");
            settings.SecretKey("minio123");
            settings.UseVirtualAddressing(false);
        };

        {
            NExport::TExportToS3Settings settings;
            fillS3Settings(settings);

            settings.AppendItem({"/local/Table", "Table"});

            const auto backupOp = YdbExportClient().ExportToS3(settings).GetValueSync();
            WaitOpSuccess(backupOp);
        }

        {
            NImport::TImportFromS3Settings settings;
            fillS3Settings(settings);

            settings.AppendItem({"Table", "/local/Restored"});

            const auto restoreOp = YdbImportClient().ImportFromS3(settings).GetValueSync();
            WaitOpSuccess(restoreOp);
        }
    }

    Y_UNIT_TEST(RecursiveFolderProcessing)
    {
        {
            auto res = YdbQueryClient().ExecuteQuery(R"sql(
                CREATE TABLE `/local/RecursiveFolderProcessing/Table0` (
                    key Uint32 NOT NULL,
                    value String,
                    PRIMARY KEY (key)
                );

                CREATE TABLE `/local/RecursiveFolderProcessing/dir1/Table1` (
                    key Uint32 NOT NULL,
                    value String,
                    PRIMARY KEY (key)
                );

                CREATE TABLE `/local/RecursiveFolderProcessing/dir1/dir2/Table2` (
                    key Uint32 NOT NULL,
                    value String,
                    PRIMARY KEY (key)
                );
            )sql", NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

            // Empty dir
            auto mkdir = YdbSchemeClient().MakeDirectory("/local/RecursiveFolderProcessing/dir1/dir2/dir3").GetValueSync();
            UNIT_ASSERT_C(mkdir.IsSuccess(), mkdir.GetIssues().ToString());
        }

        const TString bucketName = "RecursiveFolderProcessingBucket";
        CreateBucket(bucketName);

        auto makeExportSettings = [&](const TString& sourcePath, const TString& destinationPrefix) -> NExport::TExportToS3Settings {
            NExport::TExportToS3Settings exportSettings;
            exportSettings
                .Endpoint(S3Endpoint())
                .Bucket(bucketName)
                .Scheme(ES3Scheme::HTTP)
                .AccessKey("minio")
                .SecretKey("minio123");
            if (destinationPrefix) {
                exportSettings.DestinationPrefix(destinationPrefix);
            }
            if (sourcePath) {
                exportSettings.SourcePath(sourcePath);
            }
            return exportSettings;
        };

        auto makeImportSettings = [&](const TString& sourcePrefix, const TString& destinationPath) -> NImport::TImportFromS3Settings {
            NImport::TImportFromS3Settings importSettings;
            importSettings
                .Endpoint(S3Endpoint())
                .Bucket(bucketName)
                .Scheme(ES3Scheme::HTTP)
                .AccessKey("minio")
                .SecretKey("minio123");
            if (sourcePrefix) {
                importSettings.SourcePrefix(sourcePrefix);
            }
            if (destinationPath) {
                importSettings.DestinationPath(destinationPath);
            }
            return importSettings;
        };

        // Export whole database with encryption
        {

        }

        // Export with common source path == dir1
        {

        }

        // Export with directory path == dir1 + explicit table from this subdir (must remove duplicate)
        {

        }

        // Export dir2 + explicit Table0 not from this dir
        {

        }

        // Specify empty directory => error, nothing to export
        {

        }

        // Specify empty directory and existing table
        {

        }

        // Export with common prefix, import with explicitly specifying prefixes for each item
        {

        }

        // Export without common destination prefix, trying to import with filter by YDB path (error, because no SchemaMapping)
        {

        }

        // Export unexisting common source path
        {

        }

        // Export unexisting explicit path
        {

        }

        // Explicitly specify duplicated items (error)
        {

        }

        // Export directory with encryption
        {

        }

        // Export with encryption with explicitly specifying destination path (not recommended, opens explicit path with table name)
        {

        }

        // Export with encryption with explicitly specifying objects list
        {

        }

        // Export with common source path, import without common path and SchemaMapping
        {

        }

        // Encrypted export with common source path, import without common path and SchemaMapping (error, encrypted export must be with SchemaMapping)
        {

        }

        // Filter import by prefix
        {

        }

        // Filter import by YDB object path
        {

        }
    }
}
