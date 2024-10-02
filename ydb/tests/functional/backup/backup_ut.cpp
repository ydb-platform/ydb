#include <util/system/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/testlib/s3_recipe_helper/s3_recipe_helper.h>

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_export/export.h>
#include <ydb/public/sdk/cpp/client/ydb_import/import.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/draft/ydb_scripting.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>
#include <library/cpp/yson/writer.h>

#include <library/cpp/threading/local_executor/local_executor.h>

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

    TString ReformatYson(const TString& yson) {
        TStringStream ysonInput(yson);
        TStringStream output;
        NYson::ReformatYsonStream(&ysonInput, &output, NYson::EYsonFormat::Text);
        return output.Str();
    }

    void CompareYson(const TString& expected, const TString& actual) {
        UNIT_ASSERT_NO_DIFF(ReformatYson(expected), ReformatYson(actual));
    }
}

Y_UNIT_TEST_SUITE(Backup)
{
    Y_UNIT_TEST(UuidValue)
    {
        TString connectionString = GetEnv("YDB_ENDPOINT") + "/?database=" + GetEnv("YDB_DATABASE");
        auto config = TDriverConfig(connectionString);
        auto driver = TDriver(config);
        auto tableClient = TTableClient(driver);
        auto session = tableClient.GetSession().GetValueSync().GetSession();

        {
            auto res = session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/local/ProducerUuidValue` (
                    Key Uint32,
                    Value1 Uuid,
                    Value2 Uuid NOT NULL,
                    PRIMARY KEY (Key)
                );
            )").GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        {
            auto sessionResult = tableClient.GetSession().GetValueSync();
            UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
            auto s = sessionResult.GetSession();

            {
                const TString query = "UPSERT INTO ProducerUuidValue (Key, Value1, Value2) VALUES"
                    "(1, "
                      "CAST(\"5b99a330-04ef-4f1a-9b64-ba6d5f44ea01\" as Uuid), "
                      "UNWRAP(CAST(\"5b99a330-04ef-4f1a-9b64-ba6d5f44ea02\" as Uuid)"
                    "));";
                auto res = s.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
            }
        }

        const TString bucketName = "bbb";
        NTestUtils::CreateBucket(bucketName);

        auto fillS3Settings = [bucketName](auto& settings) {
            settings.Endpoint(GetEnv("S3_ENDPOINT"));
            settings.Bucket(bucketName);
            settings.AccessKey("minio");
            settings.SecretKey("minio123");
        };

        {
            NExport::TExportToS3Settings settings;
            fillS3Settings(settings);

            settings.AppendItem({"/local/ProducerUuidValue", "ProducerUuidValueBackup"});

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

        auto ob = NTestUtils::GetObjectKeys(bucketName);
        std::sort(ob.begin(), ob.end());
        UNIT_ASSERT_VALUES_EQUAL(ob.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(ob[0], "ProducerUuidValueBackup/data_00.csv");
        UNIT_ASSERT_VALUES_EQUAL(ob[1], "ProducerUuidValueBackup/metadata.json");
        UNIT_ASSERT_VALUES_EQUAL(ob[2], "ProducerUuidValueBackup/permissions.pb");
        UNIT_ASSERT_VALUES_EQUAL(ob[3], "ProducerUuidValueBackup/scheme.pb");

        {
            NImport::TImportFromS3Settings settings;
            fillS3Settings(settings);

            settings.AppendItem({"ProducerUuidValueBackup", "/local/restore"});

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

        {
            auto sessionResult = tableClient.GetSession().GetValueSync();
            UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
            auto s = sessionResult.GetSession();

            {
                const TString query = "SELECT * FROM `/local/restore`;";
                auto res = s.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

                auto yson = NYdb::FormatResultSetYson(res.GetResultSet(0));

                const TString& expected = "[[[1u];[\"5b99a330-04ef-4f1a-9b64-ba6d5f44ea01\"];\"5b99a330-04ef-4f1a-9b64-ba6d5f44ea02\"]]";
                CompareYson(expected, yson);
            }
        }
    }
}

