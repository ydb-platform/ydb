#include <ydb/tests/functional/backup/helpers/backup_test_fixture.h>

#include <library/cpp/testing/unittest/registar.h>

#include <fmt/format.h>

#include <vector>

using namespace NYdb;
using namespace fmt::literals;

Y_UNIT_TEST_SUITE_F(ParallelBackupDatabase, TBackupTestFixture)
{
    Y_UNIT_TEST(ParallelBackupWholeDatabase)
    {
        {
            auto res = YdbQueryClient().ExecuteQuery(R"sql(
                CREATE TABLE `/local/Table0` (
                    Key Uint32 NOT NULL,
                    PRIMARY KEY (Key)
                );
                CREATE TABLE `/local/dir1/Table1` (
                    Key Uint32 NOT NULL,
                    PRIMARY KEY (Key)
                );
                CREATE TABLE `/local/dir1/dir2/Table2` (
                    Key Uint32 NOT NULL,
                    PRIMARY KEY (Key)
                );
            )sql", NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

            auto res2 = YdbQueryClient().ExecuteQuery(R"sql(
                INSERT INTO `/local/Table0` (Key) VALUES (1);
                INSERT INTO `/local/dir1/Table1` (Key) VALUES (2);
                INSERT INTO `/local/dir1/dir2/Table2` (Key) VALUES (3);
            )sql", NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(res2.IsSuccess(), res.GetIssues().ToString());
        }

        const TString bucketName = "ParallelBackupWholeDatabaseBucket";
        CreateBucket(bucketName);

        auto fillS3Settings = [&](auto& settings) {
            settings.Endpoint(S3Endpoint());
            settings.Bucket(bucketName);
            settings.Scheme(NYdb::ES3Scheme::HTTP);
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
            // They are expected not to export special export copies of tables (/local/export-123), and also ".sys" and ".metadata" folders
            for (size_t i = 0; i < parallelBackups.size(); ++i) {
                auto& backupOp = parallelBackups[i];
                settings.DestinationPrefix(TStringBuilder() << "ParallelBackupWholeDatabasePrefix_" << i);
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
            NImport::TImportFromS3Settings settings;
            fillS3Settings(settings);

            settings
                .SourcePrefix(TStringBuilder() << "ParallelBackupWholeDatabasePrefix_" << i)
                .DestinationPath(TStringBuilder() << "/local/Restored_" << i);

            const auto restoreOp = YdbImportClient().ImportFromS3(settings).GetValueSync();
            WaitOpSuccess(restoreOp);

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
                UNIT_ASSERT_VALUES_EQUAL_C(tablesFound, 1, "Current directory children: " << DebugListDir(TStringBuilder() << "/local/Restored_" << i << "/" << dir));
                UNIT_ASSERT_VALUES_EQUAL(listResult.GetChildren()[tableIndex].Name, name);
            };
            checkOneTableInDirectory("", "Table0");
            checkOneTableInDirectory("dir1", "Table1");
            checkOneTableInDirectory("dir1/dir2", "Table2");
        }

        // Test restore to database root
        {
            // Remove all contents from database
            auto removeTable = [&](const TString& path) {
                auto session = YdbTableClient().GetSession().GetValueSync();
                UNIT_ASSERT_C(session.IsSuccess(), session.GetIssues().ToString());
                auto res = session.GetSession().DropTable(path).GetValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
            };
            auto removeDirectory = [&](const TString& path) {
                auto res = YdbSchemeClient().RemoveDirectory(path).GetValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString() << ". Current directory children: " << DebugListDir(path));
            };
            auto remove = [&](const TString& root, bool removeRoot = true) {
                removeTable(TStringBuilder() << root << "/Table0");
                removeTable(TStringBuilder() << root << "/dir1/Table1");
                removeTable(TStringBuilder() << root << "/dir1/dir2/Table2");
                removeDirectory(TStringBuilder() << root << "/dir1/dir2");
                removeDirectory(TStringBuilder() << root << "/dir1");
                if (removeRoot) {
                    removeDirectory(root);
                }
            };
            for (size_t i = 0; i < parallelExportsCount; ++i) {
                remove(TStringBuilder() << "/local/Restored_" << i);
            }
            remove("/local", false);
            auto listResult = YdbSchemeClient().ListDirectory("/local").GetValueSync();
            UNIT_ASSERT_C(listResult.IsSuccess(), listResult.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(listResult.GetChildren().size(), 2, "Current database directory children: " << DebugListDir("/local"));

            // Import to database root
            NImport::TImportFromS3Settings settings;
            fillS3Settings(settings);

            settings.SourcePrefix(TStringBuilder() << "ParallelBackupWholeDatabasePrefix_0");

            const auto restoreOp = YdbImportClient().ImportFromS3(settings).GetValueSync();
            WaitOpSuccess(restoreOp);

            // Check data
            auto checkTableData = [&](const TString& path, ui32 data) {
                auto result = YdbQueryClient().ExecuteQuery(
                    fmt::format(R"sql(
                        SELECT Key FROM `{table_path}`;
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

            checkTableData("/local/Table0", 1);
            checkTableData("/local/dir1/Table1", 2);
            checkTableData("/local/dir1/dir2/Table2", 3);
        }
    }
}
