#include <ydb/tests/functional/backup/helpers/backup_test_fixture.h>

#include <library/cpp/testing/unittest/registar.h>

#include <vector>

using namespace NYdb;

Y_UNIT_TEST_SUITE_F(ParallelBackupDatabase, TBackupTestFixture)
{
    Y_UNIT_TEST(ParallelBackupWholeDatabase)
    {
        {
            auto res = YdbQueryClient().ExecuteQuery(R"sql(
                CREATE TABLE `/local/Table0` (
                    Key Uint32,
                    PRIMARY KEY (Key)
                );
                CREATE TABLE `/local/dir1/Table1` (
                    Key Uint32,
                    PRIMARY KEY (Key)
                );
                CREATE TABLE `/local/dir1/dir2/Table2` (
                    Key Uint32,
                    PRIMARY KEY (Key)
                );
            )sql", NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

            auto res2 = YdbQueryClient().ExecuteQuery(R"sql(
                INSERT INTO `/local/Table0` (Key) VALUES (0);
                INSERT INTO `/local/dir1/Table1` (Key) VALUES (1);
                INSERT INTO `/local/dir1/dir2/Table2` (Key) VALUES (2);
            )sql", NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(res2.IsSuccess(), res.GetIssues().ToString());
        }

        const TString bucketName = "ParallelBackupWholeDatabaseBucket";
        CreateBucket(bucketName);

        auto fillS3Settings = [&](auto& settings) {
            settings.Endpoint(S3Endpoint());
            settings.Bucket(bucketName);
            settings.AccessKey("minio");
            settings.SecretKey("minio123");
            settings.UseVirtualAddressing(false);
        };

        constexpr size_t parallelExportsCount = 5;
        {
            NExport::TExportToS3Settings settings;
            fillS3Settings(settings);

            std::vector<NThreading::TFuture<NExport::TExportToS3Response>> parallelBackups(parallelExportsCount);

            // Start parallel backups
            // They are expected not to export special export copies of tables (/local/export-123)
            for (size_t i = 0; i < parallelBackups.size(); ++i) {
                auto& backupOp = parallelBackups[i];
                settings.DestinationPrefix(TStringBuilder() << "ParallelBackupWholeDatabasePrefix_" << i);
                backupOp = YdbExportClient().ExportToS3(settings);
            }

            // Wait
            for (auto& backupOp : parallelBackups) {
                auto& val = backupOp.GetValueSync();
                if (val.Ready()) {
                    UNIT_ASSERT_C(val.Status().IsSuccess(), val.Status().GetIssues().ToString());
                } else {
                    TMaybe<TOperation> op = val;
                    WaitOp<NExport::TExportToS3Response>(op);
                    UNIT_ASSERT_C(op->Status().IsSuccess(), op->Status().GetIssues().ToString());
                }
            }
        }

        for (size_t i = 0; i < parallelExportsCount; ++i) {
            NImport::TImportFromS3Settings settings;
            fillS3Settings(settings);

            settings
                .SourcePrefix(TStringBuilder() << "ParallelBackupWholeDatabasePrefix_" << i)
                .DestinationPath(TStringBuilder() << "/local/Restored_" << i);

            const auto restoreOp = YdbImportClient().ImportFromS3(settings).GetValueSync();

            if (restoreOp.Ready()) {
                UNIT_ASSERT_C(restoreOp.Status().IsSuccess(), restoreOp.Status().GetIssues().ToString());
            } else {
                TMaybe<TOperation> op = restoreOp;
                WaitOp<NImport::TImportFromS3Response>(op);
                UNIT_ASSERT_C(op->Status().IsSuccess(), op->Status().GetIssues().ToString());
            }

            // Check that there are only expected tables
            auto checkOneTableInDirectory = [&](const TString& dir, const TString& name) {
                auto listResult = YdbSchemeClient().ListDirectory(TStringBuilder() << "/local/Restored_" << i << "/" << dir).GetValueSync();
                UNIT_ASSERT_C(listResult.IsSuccess(), listResult.GetIssues().ToString());
                size_t tablesFound = 0;
                size_t tableIndex = 0;
                for (size_t i = 0; i < listResult.GetChildren().size(); ++i) {
                    const auto& child = listResult.GetChildren()[i];
                    if (child.Type == NScheme::ESchemeEntryType::Table) {
                        ++tablesFound;
                        tableIndex = i;
                    }
                }
                UNIT_ASSERT_VALUES_EQUAL(tablesFound, 1);
                UNIT_ASSERT_VALUES_EQUAL(listResult.GetChildren()[tableIndex].Name, name);
            };
            checkOneTableInDirectory("", "Table0");
            checkOneTableInDirectory("dir1", "Table1");
            checkOneTableInDirectory("dir1/dir2", "Table2");
        }

        // Test restore to database root
    }
}
