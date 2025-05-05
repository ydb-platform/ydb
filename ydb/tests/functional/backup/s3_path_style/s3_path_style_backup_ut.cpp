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
}
