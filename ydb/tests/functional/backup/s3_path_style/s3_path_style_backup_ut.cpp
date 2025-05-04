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
            {
                NExport::TExportToS3Settings exportSettings = makeExportSettings("", "RecursiveFolderProcessingPrefix2");
                exportSettings
                    .SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");
                auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
                WaitOpSuccess(res);

                // Validate that we have at least these S3 files (YDB recipe is common, => there are extra files here from different test tables)
                ValidateHasS3Files({
                    "RecursiveFolderProcessingPrefix2/metadata.json",
                    "RecursiveFolderProcessingPrefix2/SchemaMapping/metadata.json.enc",
                    "RecursiveFolderProcessingPrefix2/SchemaMapping/mapping.json.enc",
                    "RecursiveFolderProcessingPrefix2/001/metadata.json.enc",
                    "RecursiveFolderProcessingPrefix2/001/scheme.pb.enc",
                    "RecursiveFolderProcessingPrefix2/001/data_00.csv.enc",
                    "RecursiveFolderProcessingPrefix2/002/metadata.json.enc",
                    "RecursiveFolderProcessingPrefix2/002/scheme.pb.enc",
                    "RecursiveFolderProcessingPrefix2/002/data_00.csv.enc",
                    "RecursiveFolderProcessingPrefix2/003/metadata.json.enc",
                    "RecursiveFolderProcessingPrefix2/003/scheme.pb.enc",
                    "RecursiveFolderProcessingPrefix2/003/data_00.csv.enc",
                }, bucketName, S3Client(), "RecursiveFolderProcessingPrefix2");
            }

            {
                NImport::TImportFromS3Settings importSettings = makeImportSettings("RecursiveFolderProcessingPrefix2", "/local/RecursiveFolderProcessingRestored2");
                importSettings
                    .SymmetricKey("Cool random key!");
                auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
                WaitOpSuccess(res);

                ValidateHasYdbTables({
                    "/local/RecursiveFolderProcessingRestored2/RecursiveFolderProcessing/Table0",
                    "/local/RecursiveFolderProcessingRestored2/RecursiveFolderProcessing/dir1/Table1",
                    "/local/RecursiveFolderProcessingRestored2/RecursiveFolderProcessing/dir1/dir2/Table2",
                });
            }
        }

        // Export with common source path == dir1
        {
            {
                NExport::TExportToS3Settings exportSettings = makeExportSettings("/local/RecursiveFolderProcessing/dir1", "RecursiveFolderProcessingPrefix3");
                auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
                WaitOpSuccess(res);

                ValidateS3FileList({
                    "RecursiveFolderProcessingPrefix3/metadata.json",
                    "RecursiveFolderProcessingPrefix3/SchemaMapping/metadata.json",
                    "RecursiveFolderProcessingPrefix3/SchemaMapping/mapping.json",
                    "RecursiveFolderProcessingPrefix3/Table1/metadata.json",
                    "RecursiveFolderProcessingPrefix3/Table1/scheme.pb",
                    "RecursiveFolderProcessingPrefix3/Table1/data_00.csv",
                    "RecursiveFolderProcessingPrefix3/dir2/Table2/metadata.json",
                    "RecursiveFolderProcessingPrefix3/dir2/Table2/scheme.pb",
                    "RecursiveFolderProcessingPrefix3/dir2/Table2/data_00.csv",
                }, bucketName, S3Client(), "RecursiveFolderProcessingPrefix3");
            }

            {
                NImport::TImportFromS3Settings importSettings = makeImportSettings("RecursiveFolderProcessingPrefix3", "/local/RecursiveFolderProcessingRestored3");
                auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
                WaitOpSuccess(res);

                ValidateHasYdbTables({
                    "/local/RecursiveFolderProcessingRestored3/Table1",
                    "/local/RecursiveFolderProcessingRestored3/dir2/Table2",
                });
            }
        }

        // Export with directory path == dir1 + explicit table from this subdir (must remove duplicate)
        {
            {
                NExport::TExportToS3Settings exportSettings = makeExportSettings("", "RecursiveFolderProcessingPrefix4");
                exportSettings
                    .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/local/RecursiveFolderProcessing/dir1/Table1", .Dst = "ExplicitTable1Prefix"})
                    .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/local/RecursiveFolderProcessing/dir1"});
                auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
                WaitOpSuccess(res);

                ValidateS3FileList({
                    "RecursiveFolderProcessingPrefix4/metadata.json",
                    "RecursiveFolderProcessingPrefix4/SchemaMapping/metadata.json",
                    "RecursiveFolderProcessingPrefix4/SchemaMapping/mapping.json",
                    "RecursiveFolderProcessingPrefix4/ExplicitTable1Prefix/metadata.json",
                    "RecursiveFolderProcessingPrefix4/ExplicitTable1Prefix/scheme.pb",
                    "RecursiveFolderProcessingPrefix4/ExplicitTable1Prefix/data_00.csv",
                    "RecursiveFolderProcessingPrefix4/RecursiveFolderProcessing/dir1/dir2/Table2/metadata.json",
                    "RecursiveFolderProcessingPrefix4/RecursiveFolderProcessing/dir1/dir2/Table2/scheme.pb",
                    "RecursiveFolderProcessingPrefix4/RecursiveFolderProcessing/dir1/dir2/Table2/data_00.csv",
                }, bucketName, S3Client(), "RecursiveFolderProcessingPrefix4");
            }

            {
                NImport::TImportFromS3Settings importSettings = makeImportSettings("RecursiveFolderProcessingPrefix4", "/local/RecursiveFolderProcessingRestored4");
                auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
                WaitOpSuccess(res);

                ValidateHasYdbTables({
                    "/local/RecursiveFolderProcessingRestored4/RecursiveFolderProcessing/dir1/Table1",
                    "/local/RecursiveFolderProcessingRestored4/RecursiveFolderProcessing/dir1/dir2/Table2",
                });
            }
        }

        // Export dir2 + explicit Table0 not from this dir
        {
            {
                NExport::TExportToS3Settings exportSettings = makeExportSettings("", "RecursiveFolderProcessingPrefix5");
                exportSettings
                    .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/local/RecursiveFolderProcessing/dir1/dir2"})
                    .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/local/RecursiveFolderProcessing/Table0"});
                auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
                WaitOpSuccess(res);

                ValidateS3FileList({
                    "RecursiveFolderProcessingPrefix5/metadata.json",
                    "RecursiveFolderProcessingPrefix5/SchemaMapping/metadata.json",
                    "RecursiveFolderProcessingPrefix5/SchemaMapping/mapping.json",
                    "RecursiveFolderProcessingPrefix5/RecursiveFolderProcessing/Table0/metadata.json",
                    "RecursiveFolderProcessingPrefix5/RecursiveFolderProcessing/Table0/scheme.pb",
                    "RecursiveFolderProcessingPrefix5/RecursiveFolderProcessing/Table0/data_00.csv",
                    "RecursiveFolderProcessingPrefix5/RecursiveFolderProcessing/dir1/dir2/Table2/metadata.json",
                    "RecursiveFolderProcessingPrefix5/RecursiveFolderProcessing/dir1/dir2/Table2/scheme.pb",
                    "RecursiveFolderProcessingPrefix5/RecursiveFolderProcessing/dir1/dir2/Table2/data_00.csv",
                }, bucketName, S3Client(), "RecursiveFolderProcessingPrefix5");
            }

            {
                NImport::TImportFromS3Settings importSettings = makeImportSettings("RecursiveFolderProcessingPrefix5", "/local/RecursiveFolderProcessingRestored5");
                auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
                WaitOpSuccess(res);

                ValidateHasYdbTables({
                    "/local/RecursiveFolderProcessingRestored5/RecursiveFolderProcessing/Table0",
                    "/local/RecursiveFolderProcessingRestored5/RecursiveFolderProcessing/dir1/dir2/Table2",
                });
            }
        }

        // Specify empty directory => error, nothing to export
        {
            NExport::TExportToS3Settings exportSettings = makeExportSettings("", "Prefix");
            exportSettings
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/local/RecursiveFolderProcessing/dir1/dir2/dir3"});
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), EStatus::BAD_REQUEST, "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
        }

        // Specify empty directory and existing table
        {
            {
                NExport::TExportToS3Settings exportSettings = makeExportSettings("/local/RecursiveFolderProcessing/dir1/dir2", "RecursiveFolderProcessingPrefix6");
                exportSettings
                    .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/Table2"})
                    .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/local/RecursiveFolderProcessing/dir1/dir2/dir3"}); // absolute paths are also accepted
                auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
                WaitOpSuccess(res);

                ValidateS3FileList({
                    "RecursiveFolderProcessingPrefix6/metadata.json",
                    "RecursiveFolderProcessingPrefix6/SchemaMapping/metadata.json",
                    "RecursiveFolderProcessingPrefix6/SchemaMapping/mapping.json",
                    "RecursiveFolderProcessingPrefix6/Table2/metadata.json",
                    "RecursiveFolderProcessingPrefix6/Table2/scheme.pb",
                    "RecursiveFolderProcessingPrefix6/Table2/data_00.csv",
                }, bucketName, S3Client(), "RecursiveFolderProcessingPrefix6");
            }

            {
                NImport::TImportFromS3Settings importSettings = makeImportSettings("RecursiveFolderProcessingPrefix6", "/local/RecursiveFolderProcessingRestored6");
                auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
                WaitOpSuccess(res);

                ValidateHasYdbTables({
                    "/local/RecursiveFolderProcessingRestored6/Table2",
                });
            }
        }

        // Export with common prefix, import with explicitly specifying prefixes for each item
        {
            {
                NExport::TExportToS3Settings exportSettings = makeExportSettings("/local/RecursiveFolderProcessing/dir1", "RecursiveFolderProcessingPrefix7");
                auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
                WaitOpSuccess(res);

                ValidateS3FileList({
                    "RecursiveFolderProcessingPrefix7/metadata.json",
                    "RecursiveFolderProcessingPrefix7/SchemaMapping/metadata.json",
                    "RecursiveFolderProcessingPrefix7/SchemaMapping/mapping.json",
                    "RecursiveFolderProcessingPrefix7/Table1/metadata.json",
                    "RecursiveFolderProcessingPrefix7/Table1/scheme.pb",
                    "RecursiveFolderProcessingPrefix7/Table1/data_00.csv",
                    "RecursiveFolderProcessingPrefix7/dir2/Table2/metadata.json",
                    "RecursiveFolderProcessingPrefix7/dir2/Table2/scheme.pb",
                    "RecursiveFolderProcessingPrefix7/dir2/Table2/data_00.csv",
                }, bucketName, S3Client(), "RecursiveFolderProcessingPrefix7");
            }

            {
                NImport::TImportFromS3Settings importSettings = makeImportSettings("", "");
                importSettings
                    .AppendItem(NImport::TImportFromS3Settings::TItem{.Src = "RecursiveFolderProcessingPrefix7/Table1", .Dst = "/local/RecursiveFolderProcessingRestored7/Table1"})
                    .AppendItem(NImport::TImportFromS3Settings::TItem{.Src = "RecursiveFolderProcessingPrefix7/dir2/Table2", .Dst = "/local/RecursiveFolderProcessingRestored7/dir2/yet/another/dir/Table2"});
                auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
                WaitOpSuccess(res);

                ValidateHasYdbTables({
                    "/local/RecursiveFolderProcessingRestored7/Table1",
                    "/local/RecursiveFolderProcessingRestored7/dir2/yet/another/dir/Table2",
                });
            }
        }

        // Export without common destination prefix, trying to import with filter by YDB path (error, because no SchemaMapping)
        {
            {
                NExport::TExportToS3Settings exportSettings = makeExportSettings("/local/RecursiveFolderProcessing/dir1", "");
                exportSettings
                    .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/Table1", .Dst = "RecursiveFolderProcessingPrefix8/t1"});
                auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
                WaitOpSuccess(res);

                ValidateS3FileList({
                    "RecursiveFolderProcessingPrefix8/t1/metadata.json",
                    "RecursiveFolderProcessingPrefix8/t1/scheme.pb",
                    "RecursiveFolderProcessingPrefix8/t1/data_00.csv",
                }, bucketName, S3Client(), "RecursiveFolderProcessingPrefix8");
            }

            {
                NImport::TImportFromS3Settings importSettings = makeImportSettings("", "");
                importSettings
                    .AppendItem(NImport::TImportFromS3Settings::TItem{.Dst = "/local/RecursiveFolderProcessingRestored8/Table1", .SrcPath = "/local/RecursiveFolderProcessingRestored8/Table1"});
                auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
                UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), EStatus::BAD_REQUEST, "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
            }

            {
                NImport::TImportFromS3Settings importSettings = makeImportSettings("RecursiveFolderProcessingPrefix8", "");
                auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
                WaitOpStatus(res, EStatus::CANCELLED);
            }

            {
                NImport::TImportFromS3Settings importSettings = makeImportSettings("RecursiveFolderProcessingPrefix8", "");
                importSettings
                    .AppendItem(NImport::TImportFromS3Settings::TItem{.Dst = "/local/RecursiveFolderProcessingRestored8/Table1", .SrcPath = "/local/RecursiveFolderProcessingRestored8/Table1"});
                auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
                WaitOpStatus(res, EStatus::CANCELLED);
            }
        }

        // Export unexisting common source path
        {
            NExport::TExportToS3Settings exportSettings = makeExportSettings("/local/RecursiveFolderProcessing/unexisting", "Prefix");
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpStatus(res, EStatus::SCHEME_ERROR);
        }

        // Export unexisting explicit path
        {
            NExport::TExportToS3Settings exportSettings = makeExportSettings("/local/RecursiveFolderProcessing/dir1", "Prefix");
            exportSettings
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "unexisting"});
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpStatus(res, EStatus::SCHEME_ERROR);
        }

        // Explicitly specify duplicated items (error)
        {
            NExport::TExportToS3Settings exportSettings = makeExportSettings("/local/RecursiveFolderProcessing/dir1", "Prefix");
            exportSettings
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "dir2"})
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/dir2"})
                .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "dir2/"});
            auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
            WaitOpStatus(res, EStatus::BAD_REQUEST);
        }

        // Export directory with encryption
        {
            {
                NExport::TExportToS3Settings exportSettings = makeExportSettings("/local/RecursiveFolderProcessing/dir1", "RecursiveFolderProcessingPrefix9");
                exportSettings
                    .SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");
                auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
                WaitOpSuccess(res);

                ValidateS3FileList({
                    "RecursiveFolderProcessingPrefix9/metadata.json",
                    "RecursiveFolderProcessingPrefix9/SchemaMapping/metadata.json.enc",
                    "RecursiveFolderProcessingPrefix9/SchemaMapping/mapping.json.enc",
                    "RecursiveFolderProcessingPrefix9/001/metadata.json.enc",
                    "RecursiveFolderProcessingPrefix9/001/scheme.pb.enc",
                    "RecursiveFolderProcessingPrefix9/001/data_00.csv.enc",
                    "RecursiveFolderProcessingPrefix9/002/metadata.json.enc",
                    "RecursiveFolderProcessingPrefix9/002/scheme.pb.enc",
                    "RecursiveFolderProcessingPrefix9/002/data_00.csv.enc",
                }, bucketName, S3Client(), "RecursiveFolderProcessingPrefix9");
            }

            {
                NImport::TImportFromS3Settings importSettings = makeImportSettings("RecursiveFolderProcessingPrefix9", "/local/RecursiveFolderProcessingRestored9");
                importSettings
                    .SymmetricKey("Cool random key!");
                auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
                WaitOpSuccess(res);

                ValidateHasYdbTables({
                    "/local/RecursiveFolderProcessingRestored9/Table1",
                    "/local/RecursiveFolderProcessingRestored9/dir2/Table2",
                });
            }
        }

        // Export with encryption with explicitly specifying destination path (not recommended, opens explicit path with table name)
        {
            {
                NExport::TExportToS3Settings exportSettings = makeExportSettings("/local/RecursiveFolderProcessing", "RecursiveFolderProcessingPrefix10");
                exportSettings
                    .SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!")
                    .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "Table0", .Dst = "UnsafeTableNameShownInEncryptedBackup"})
                    .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "dir1", .Dst = "Dir1Prefix"}); // Recursive proparation
                auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
                WaitOpSuccess(res);

                ValidateS3FileList({
                    "RecursiveFolderProcessingPrefix10/metadata.json",
                    "RecursiveFolderProcessingPrefix10/SchemaMapping/metadata.json.enc",
                    "RecursiveFolderProcessingPrefix10/SchemaMapping/mapping.json.enc",
                    "RecursiveFolderProcessingPrefix10/UnsafeTableNameShownInEncryptedBackup/metadata.json.enc",
                    "RecursiveFolderProcessingPrefix10/UnsafeTableNameShownInEncryptedBackup/scheme.pb.enc",
                    "RecursiveFolderProcessingPrefix10/UnsafeTableNameShownInEncryptedBackup/data_00.csv.enc",
                    "RecursiveFolderProcessingPrefix10/Dir1Prefix/Table1/metadata.json.enc",
                    "RecursiveFolderProcessingPrefix10/Dir1Prefix/Table1/scheme.pb.enc",
                    "RecursiveFolderProcessingPrefix10/Dir1Prefix/Table1/data_00.csv.enc",
                    "RecursiveFolderProcessingPrefix10/Dir1Prefix/dir2/Table2/metadata.json.enc",
                    "RecursiveFolderProcessingPrefix10/Dir1Prefix/dir2/Table2/scheme.pb.enc",
                    "RecursiveFolderProcessingPrefix10/Dir1Prefix/dir2/Table2/data_00.csv.enc",
                }, bucketName, S3Client(), "RecursiveFolderProcessingPrefix10");
            }

            {
                NImport::TImportFromS3Settings importSettings = makeImportSettings("RecursiveFolderProcessingPrefix10", "/local/RecursiveFolderProcessingRestored10");
                importSettings
                    .SymmetricKey("Cool random key!");
                auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
                WaitOpSuccess(res);

                ValidateHasYdbTables({
                    "/local/RecursiveFolderProcessingRestored10/Table0",
                    "/local/RecursiveFolderProcessingRestored10/dir1/Table1",
                    "/local/RecursiveFolderProcessingRestored10/dir1/dir2/Table2",
                });
            }
        }

        // Export with encryption with explicitly specifying objects list
        {
            {
                NExport::TExportToS3Settings exportSettings = makeExportSettings("", ""); // no common prefix => error, not allowed with encryption
                exportSettings
                    .SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!")
                    .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/local/RecursiveFolderProcessing/Table0", .Dst = "Table0"})
                    .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/local/RecursiveFolderProcessing/dir1/Table1", .Dst = "Table1"})
                    .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/local/RecursiveFolderProcessing/dir1/dir2/Table2", .Dst = "Table2"});
                auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
                UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), EStatus::BAD_REQUEST, "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());

                // Add required parameters and check result
                exportSettings
                    .DestinationPrefix("RecursiveFolderProcessingPrefix11");
                for (auto& item : exportSettings.Item_) {
                    item.Dst.clear();
                }

                res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
                WaitOpSuccess(res);

                ValidateS3FileList({
                    "RecursiveFolderProcessingPrefix11/metadata.json",
                    "RecursiveFolderProcessingPrefix11/SchemaMapping/metadata.json.enc",
                    "RecursiveFolderProcessingPrefix11/SchemaMapping/mapping.json.enc",
                    "RecursiveFolderProcessingPrefix11/001/metadata.json.enc",
                    "RecursiveFolderProcessingPrefix11/001/scheme.pb.enc",
                    "RecursiveFolderProcessingPrefix11/001/data_00.csv.enc",
                    "RecursiveFolderProcessingPrefix11/002/metadata.json.enc",
                    "RecursiveFolderProcessingPrefix11/002/scheme.pb.enc",
                    "RecursiveFolderProcessingPrefix11/002/data_00.csv.enc",
                    "RecursiveFolderProcessingPrefix11/003/metadata.json.enc",
                    "RecursiveFolderProcessingPrefix11/003/scheme.pb.enc",
                    "RecursiveFolderProcessingPrefix11/003/data_00.csv.enc",
                }, bucketName, S3Client(), "RecursiveFolderProcessingPrefix11");
            }

            {
                NImport::TImportFromS3Settings importSettings = makeImportSettings("RecursiveFolderProcessingPrefix11", "/local/RecursiveFolderProcessingRestored11");
                importSettings
                    .SymmetricKey("Cool random key!");
                auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
                WaitOpSuccess(res);

                ValidateHasYdbTables({
                    "/local/RecursiveFolderProcessingRestored11/RecursiveFolderProcessing/Table0",
                    "/local/RecursiveFolderProcessingRestored11/RecursiveFolderProcessing/dir1/Table1",
                    "/local/RecursiveFolderProcessingRestored11/RecursiveFolderProcessing/dir1/dir2/Table2",
                });
            }
        }

        // Export with common source path, import without common path and SchemaMapping
        {
            {
                NExport::TExportToS3Settings exportSettings = makeExportSettings("/local/RecursiveFolderProcessing", "RecursiveFolderProcessingPrefix12");
                auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
                WaitOpSuccess(res);

                ValidateS3FileList({
                    "RecursiveFolderProcessingPrefix12/metadata.json",
                    "RecursiveFolderProcessingPrefix12/SchemaMapping/metadata.json",
                    "RecursiveFolderProcessingPrefix12/SchemaMapping/mapping.json",
                    "RecursiveFolderProcessingPrefix12/Table0/metadata.json",
                    "RecursiveFolderProcessingPrefix12/Table0/scheme.pb",
                    "RecursiveFolderProcessingPrefix12/Table0/data_00.csv",
                    "RecursiveFolderProcessingPrefix12/dir1/Table1/metadata.json",
                    "RecursiveFolderProcessingPrefix12/dir1/Table1/scheme.pb",
                    "RecursiveFolderProcessingPrefix12/dir1/Table1/data_00.csv",
                    "RecursiveFolderProcessingPrefix12/dir1/dir2/Table2/metadata.json",
                    "RecursiveFolderProcessingPrefix12/dir1/dir2/Table2/scheme.pb",
                    "RecursiveFolderProcessingPrefix12/dir1/dir2/Table2/data_00.csv",
                }, bucketName, S3Client(), "RecursiveFolderProcessingPrefix12");
            }

            {
                NImport::TImportFromS3Settings importSettings = makeImportSettings("", "");
                importSettings
                    .AppendItem(NImport::TImportFromS3Settings::TItem{.Src = "RecursiveFolderProcessingPrefix12/Table0", .Dst = "/local/RecursiveFolderProcessingRestored12/Table0"});
                auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
                WaitOpSuccess(res);

                ValidateHasYdbTables({
                    "/local/RecursiveFolderProcessingRestored12/Table0",
                });
                ValidateDoesNotHaveYdbTables({
                    "/local/RecursiveFolderProcessingRestored12/dir1/Table1",
                    "/local/RecursiveFolderProcessingRestored12/dir1/dir2/Table2",
                });
            }
        }

        // Encrypted export with common source path, import without common path and SchemaMapping (error, encrypted export must be with SchemaMapping)
        {
            {
                NExport::TExportToS3Settings exportSettings = makeExportSettings("/local/RecursiveFolderProcessing", "RecursiveFolderProcessingPrefix13");
                exportSettings
                    .SymmetricEncryption(NExport::TExportToS3Settings::TEncryptionAlgorithm::AES_128_GCM, "Cool random key!");
                auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
                WaitOpSuccess(res);

                ValidateS3FileList({
                    "RecursiveFolderProcessingPrefix13/metadata.json",
                    "RecursiveFolderProcessingPrefix13/SchemaMapping/metadata.json.enc",
                    "RecursiveFolderProcessingPrefix13/SchemaMapping/mapping.json.enc",
                    "RecursiveFolderProcessingPrefix13/001/metadata.json.enc",
                    "RecursiveFolderProcessingPrefix13/001/scheme.pb.enc",
                    "RecursiveFolderProcessingPrefix13/001/data_00.csv.enc",
                    "RecursiveFolderProcessingPrefix13/002/metadata.json.enc",
                    "RecursiveFolderProcessingPrefix13/002/scheme.pb.enc",
                    "RecursiveFolderProcessingPrefix13/002/data_00.csv.enc",
                    "RecursiveFolderProcessingPrefix13/003/metadata.json.enc",
                    "RecursiveFolderProcessingPrefix13/003/scheme.pb.enc",
                    "RecursiveFolderProcessingPrefix13/003/data_00.csv.enc",
                }, bucketName, S3Client(), "RecursiveFolderProcessingPrefix13");
            }

            {
                NImport::TImportFromS3Settings importSettings = makeImportSettings("", "");
                importSettings
                    .SymmetricKey("Cool random key!")
                    .AppendItem(NImport::TImportFromS3Settings::TItem{.Src = "RecursiveFolderProcessingPrefix13/001", .Dst = "/local/RecursiveFolderProcessingRestored13/Table0"});
                auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
                UNIT_ASSERT_EQUAL_C(res.Status().GetStatus(), EStatus::BAD_REQUEST, "Status: " << res.Status().GetStatus() << Endl << res.Status().GetIssues().ToString());
            }
        }

        // Filter import by prefix
        {
            {
                NExport::TExportToS3Settings exportSettings = makeExportSettings("/local/RecursiveFolderProcessing", "RecursiveFolderProcessingPrefix14");
                exportSettings
                    .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "Table0", .Dst = "Table0_Prefix"})
                    .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "dir1/Table1", .Dst = "Table1_Prefix"})
                    .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "dir1/dir2/Table2", .Dst = "Table2_Prefix"});
                auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
                WaitOpSuccess(res);

                ValidateS3FileList({
                    "RecursiveFolderProcessingPrefix14/metadata.json",
                    "RecursiveFolderProcessingPrefix14/SchemaMapping/metadata.json",
                    "RecursiveFolderProcessingPrefix14/SchemaMapping/mapping.json",
                    "RecursiveFolderProcessingPrefix14/Table0_Prefix/metadata.json",
                    "RecursiveFolderProcessingPrefix14/Table0_Prefix/scheme.pb",
                    "RecursiveFolderProcessingPrefix14/Table0_Prefix/data_00.csv",
                    "RecursiveFolderProcessingPrefix14/Table1_Prefix/metadata.json",
                    "RecursiveFolderProcessingPrefix14/Table1_Prefix/scheme.pb",
                    "RecursiveFolderProcessingPrefix14/Table1_Prefix/data_00.csv",
                    "RecursiveFolderProcessingPrefix14/Table2_Prefix/metadata.json",
                    "RecursiveFolderProcessingPrefix14/Table2_Prefix/scheme.pb",
                    "RecursiveFolderProcessingPrefix14/Table2_Prefix/data_00.csv",
                }, bucketName, S3Client(), "RecursiveFolderProcessingPrefix14");
            }

            {
                NImport::TImportFromS3Settings importSettings = makeImportSettings("RecursiveFolderProcessingPrefix14", "/local/RecursiveFolderProcessingRestored14");
                importSettings
                    .AppendItem(NImport::TImportFromS3Settings::TItem{.Src = "Table0_Prefix", .Dst = "/local/RecursiveFolderProcessingRestored14/Table0"});
                auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
                WaitOpSuccess(res);

                ValidateHasYdbTables({
                    "/local/RecursiveFolderProcessingRestored14/Table0",
                });
                ValidateDoesNotHaveYdbTables({
                    "/local/RecursiveFolderProcessingRestored14/dir1/Table1",
                    "/local/RecursiveFolderProcessingRestored14/dir1/dir2/Table2",
                });
            }
        }

        // Filter import by YDB object path
        {
            {
                NExport::TExportToS3Settings exportSettings = makeExportSettings("/local/RecursiveFolderProcessing", "RecursiveFolderProcessingPrefix15");
                exportSettings
                    .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "Table0", .Dst = "Table0_Prefix"})
                    .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "dir1/Table1", .Dst = "Table1_Prefix"})
                    .AppendItem(NExport::TExportToS3Settings::TItem{.Src = "/local/RecursiveFolderProcessing/dir1/dir2/Table2", .Dst = "Table2_Prefix"});
                auto res = YdbExportClient().ExportToS3(exportSettings).GetValueSync();
                WaitOpSuccess(res);

                ValidateS3FileList({
                    "RecursiveFolderProcessingPrefix15/metadata.json",
                    "RecursiveFolderProcessingPrefix15/SchemaMapping/metadata.json",
                    "RecursiveFolderProcessingPrefix15/SchemaMapping/mapping.json",
                    "RecursiveFolderProcessingPrefix15/Table0_Prefix/metadata.json",
                    "RecursiveFolderProcessingPrefix15/Table0_Prefix/scheme.pb",
                    "RecursiveFolderProcessingPrefix15/Table0_Prefix/data_00.csv",
                    "RecursiveFolderProcessingPrefix15/Table1_Prefix/metadata.json",
                    "RecursiveFolderProcessingPrefix15/Table1_Prefix/scheme.pb",
                    "RecursiveFolderProcessingPrefix15/Table1_Prefix/data_00.csv",
                    "RecursiveFolderProcessingPrefix15/Table2_Prefix/metadata.json",
                    "RecursiveFolderProcessingPrefix15/Table2_Prefix/scheme.pb",
                    "RecursiveFolderProcessingPrefix15/Table2_Prefix/data_00.csv",
                }, bucketName, S3Client(), "RecursiveFolderProcessingPrefix15");
            }

            {
                NImport::TImportFromS3Settings importSettings = makeImportSettings("RecursiveFolderProcessingPrefix15", "/local/RecursiveFolderProcessingRestored15");
                importSettings
                    .AppendItem(NImport::TImportFromS3Settings::TItem{.Dst = "/local/RecursiveFolderProcessingRestored15/Table123", .SrcPath = "dir1/dir2//Table2"})
                    .AppendItem(NImport::TImportFromS3Settings::TItem{.Dst = "/local/RecursiveFolderProcessingRestored15/Table321", .SrcPath = "Table0"});
                auto res = YdbImportClient().ImportFromS3(importSettings).GetValueSync();
                WaitOpSuccess(res);

                ValidateHasYdbTables({
                    "/local/RecursiveFolderProcessingRestored15/Table123",
                    "/local/RecursiveFolderProcessingRestored15/Table321",
                });
                ValidateDoesNotHaveYdbTables({
                    "/local/RecursiveFolderProcessingRestored15/Table0",
                    "/local/RecursiveFolderProcessingRestored15/dir1/Table1",
                    "/local/RecursiveFolderProcessingRestored15/dir1/dir2/Table2",
                });
            }

            {
                // Both src path and src prefix are incorrect
                NImport::TImportFromS3Settings importSettings = makeImportSettings("RecursiveFolderProcessingPrefix15", "/local/RecursiveFolderProcessingRestored15");
                importSettings
                    .AppendItem(NImport::TImportFromS3Settings::TItem{.Src = "/local/RecursiveFolderProcessingRestored15/dir1/dir2/Table2", .Dst = "/local/RecursiveFolderProcessingRestored15/Table0", .SrcPath = "dir1/dir2/Table2"});
                UNIT_ASSERT_EXCEPTION(YdbImportClient().ImportFromS3(importSettings).GetValueSync(), TContractViolation);
            }
        }
    }
}
