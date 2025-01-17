#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_export/export.h>
#include <ydb/public/sdk/cpp/client/ydb_import/import.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/library/testlib/s3_recipe_helper/s3_recipe_helper.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/system/env.h>

using namespace NYdb;
using namespace NYdb::NTable;

namespace {
    template<typename TOp>
    void WaitOp(TMaybe<TOperation>& op, NOperation::TOperationClient& opClient) {
        int attempt = 20;
        while (--attempt) {
            op = opClient.Get<TOp>(op->Id()).GetValueSync();
            if (op->Ready()) {
                break;
            } 
            Sleep(TDuration::Seconds(1));
        }
        UNIT_ASSERT_C(attempt, "Unable to wait completion of backup");
    }
}

Y_UNIT_TEST_SUITE(S3PathStyleBackup)
{
    Y_UNIT_TEST(DisableVirtualAddressing)
    {
        TString connectionString = GetEnv("YDB_ENDPOINT") + "/?database=" + GetEnv("YDB_DATABASE");
        auto config = TDriverConfig(connectionString);
        auto driver = TDriver(config);
        auto tableClient = TTableClient(driver);
        auto session = tableClient.GetSession().GetValueSync().GetSession();

        {
            auto res = session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/local/Table` (
                    Key Uint32,
                    PRIMARY KEY (Key)
                );
            )").GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        Aws::Client::ClientConfiguration s3ClientConfig;
        s3ClientConfig.endpointOverride = GetEnv("S3_ENDPOINT");
        s3ClientConfig.scheme = Aws::Http::Scheme::HTTP;
        auto s3Client = Aws::S3::S3Client(
            std::make_shared<Aws::Auth::AnonymousAWSCredentialsProvider>(),
            s3ClientConfig,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            false
        );
        const TString bucketName = "my-bucket";
        NTestUtils::CreateBucket(bucketName, s3Client);

        auto fillS3Settings = [bucketName](auto& settings) {
            settings.Endpoint(GetEnv("S3_ENDPOINT"));
            settings.Bucket(bucketName);
            settings.AccessKey("minio");
            settings.SecretKey("minio123");
            settings.UseVirtualAddressing(false);
        };

        {
            NExport::TExportToS3Settings settings;
            fillS3Settings(settings);

            settings.AppendItem({"/local/Table", "Table"});

            auto exportClient = NExport::TExportClient(driver);
            auto operationClient = NOperation::TOperationClient(driver);

            const auto backupOp = exportClient.ExportToS3(settings).GetValueSync();

            if (backupOp.Ready()) {
                UNIT_ASSERT_C(backupOp.Status().IsSuccess(), backupOp.Status().GetIssues().ToString());
            } else {
                TMaybe<TOperation> op = backupOp;
                WaitOp<NExport::TExportToS3Response>(op, operationClient);
                UNIT_ASSERT_C(op->Status().IsSuccess(), op->Status().GetIssues().ToString());
            }
        }

        {
            NImport::TImportFromS3Settings settings;
            fillS3Settings(settings);

            settings.AppendItem({"Table", "/local/Restored"});

            auto importClient = NImport::TImportClient(driver);
            auto operationClient = NOperation::TOperationClient(driver);

            const auto restoreOp = importClient.ImportFromS3(settings).GetValueSync();

            if (restoreOp.Ready()) {
                UNIT_ASSERT_C(restoreOp.Status().IsSuccess(), restoreOp.Status().GetIssues().ToString());
            } else {
                TMaybe<TOperation> op = restoreOp;
                WaitOp<NImport::TImportFromS3Response>(op, operationClient);
                UNIT_ASSERT_C(op->Status().IsSuccess(), op->Status().GetIssues().ToString());
            }
        }
    }
}

