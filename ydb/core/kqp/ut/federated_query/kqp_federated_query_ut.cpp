#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <util/system/env.h>

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/Aws.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/S3Client.h>

#include <ydb/library/yql/utils/log/log.h>

#include <fmt/format.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

constexpr TStringBuf TEST_CONTENT =
R"({"key": "1", "value": "trololo"}
{"key": "2", "value": "hello world"}
)"sv;

const TString TEST_SCHEMA = R"(["StructType";[["key";["DataType";"Utf8";];];["value";["DataType";"Utf8";];];];])";

bool InitAwsApi() {
    Aws::InitAPI(Aws::SDKOptions());
    return true;
}

bool EnsureAwsApiInited() {
    static const bool inited = InitAwsApi();
    return inited;
}

void CreateBucketWithObject(const TString& bucket, const TString& object, const TStringBuf& content) {
    EnsureAwsApiInited();

    Aws::Client::ClientConfiguration s3ClientConfig;
    s3ClientConfig.endpointOverride = GetEnv("S3_ENDPOINT");
    s3ClientConfig.scheme = Aws::Http::Scheme::HTTP;
    Aws::S3::S3Client s3Client(std::make_shared<Aws::Auth::AnonymousAWSCredentialsProvider>(), s3ClientConfig);

    {
        Aws::S3::Model::CreateBucketRequest req;
        req.SetBucket(bucket);
        req.SetACL(Aws::S3::Model::BucketCannedACL::public_read_write);
        const Aws::S3::Model::CreateBucketOutcome result = s3Client.CreateBucket(req);
        UNIT_ASSERT_C(result.IsSuccess(), "Error creating bucket \"" << bucket << "\": " << result.GetError().GetExceptionName() << ": " << result.GetError().GetMessage());
    }

    {
        Aws::S3::Model::PutObjectRequest req;
        req.WithBucket(bucket).WithKey(object);

        auto inputStream = std::make_shared<std::stringstream>();
        *inputStream << content;
        req.SetBody(inputStream);
        const Aws::S3::Model::PutObjectOutcome result = s3Client.PutObject(req);
        UNIT_ASSERT_C(result.IsSuccess(), "Error uploading object \"" << object << "\" to a bucket \"" << bucket << "\": " << result.GetError().GetExceptionName() << ": " << result.GetError().GetMessage());
    }
}

Y_UNIT_TEST_SUITE(KqpFederatedQuery) {
    Y_UNIT_TEST(ExecuteScript) {
        CreateBucketWithObject("test_bucket", "Root/test_object", TEST_CONTENT);
        SetEnv("TEST_S3_BUCKET", "test_bucket");
        SetEnv("TEST_S3_OBJECT", "test_object");
        SetEnv("TEST_S3_CONNECTION", "test_connection");
        SetEnv("TEST_S3_BINDING", "test_binding");
        SetEnv("TEST_S3_FORMAT", "json_each_row");
        SetEnv("TEST_S3_SCHEMA", TEST_SCHEMA);

        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto executeScrptsResult = db.ExecuteScript(R"(
            SELECT * FROM bindings.test_binding
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(executeScrptsResult.Status().GetStatus(), EStatus::SUCCESS, executeScrptsResult.Status().GetIssues().ToString());
        UNIT_ASSERT(executeScrptsResult.Metadata().ExecutionId);

        TMaybe<TFetchScriptResultsResult> results;
        do {
            Sleep(TDuration::MilliSeconds(50));
            TAsyncFetchScriptResultsResult future = db.FetchScriptResults(executeScrptsResult.Metadata().ExecutionId);
            results.ConstructInPlace(future.ExtractValueSync());
            if (!results->IsSuccess()) {
                UNIT_ASSERT_C(results->GetStatus() == NYdb::EStatus::BAD_REQUEST, results->GetStatus() << ": " << results->GetIssues().ToString());
                UNIT_ASSERT_STRING_CONTAINS(results->GetIssues().ToOneLineString(), "Results are not ready");
            }
        } while (!results->HasResultSet());
        TResultSetParser resultSet(results->ExtractResultSet());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 2);

        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "1");
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "trololo");

        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "2");
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "hello world");
    }

    Y_UNIT_TEST(ExecuteScriptWithExternalTableResolve) {
        using namespace fmt::literals;
        const TString externalDataSourceName = "/Root/external_data_source";
        const TString externalTableName = "/Root/test_binding_resolve";
        const TString bucket = "test_bucket1";
        const TString object = "Root/test_object";

        CreateBucketWithObject(bucket, object, TEST_CONTENT);

        auto kikimr = DefaultKikimrRunner();
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);

        auto tc = kikimr.GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        const TString query = fmt::format(R"(
            CREATE EXTERNAL DATA SOURCE `{external_source}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{location}",
                AUTH_METHOD="NONE"
            );
            CREATE EXTERNAL TABLE `{external_table}` (
                key Utf8 NOT NULL,
                value Utf8 NOT NULL
            ) WITH (
                DATA_SOURCE="{external_source}",
                LOCATION="{object}",
                FORMAT="json_each_row"
            );)",
            "external_source"_a = externalTableName,
            "external_table"_a = externalDataSourceName,
            "location"_a = GetEnv("S3_ENDPOINT") + "/" + bucket + "/",
            "object"_a = object
            );
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto db = kikimr.GetQueryClient();
        auto executeScrptsResult = db.ExecuteScript(fmt::format(R"(
            SELECT * FROM `{external_table}`
        )", "external_table"_a=externalDataSourceName)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(executeScrptsResult.Status().GetStatus(), EStatus::SUCCESS, executeScrptsResult.Status().GetIssues().ToString());
        UNIT_ASSERT(executeScrptsResult.Metadata().ExecutionId);

        TMaybe<TFetchScriptResultsResult> results;
        do {
            Sleep(TDuration::MilliSeconds(50));
            TAsyncFetchScriptResultsResult future = db.FetchScriptResults(executeScrptsResult.Metadata().ExecutionId);
            results.ConstructInPlace(future.ExtractValueSync());
            if (!results->IsSuccess()) {
                UNIT_ASSERT_C(results->GetStatus() == NYdb::EStatus::BAD_REQUEST, results->GetStatus() << ": " << results->GetIssues().ToString());
                UNIT_ASSERT_STRING_CONTAINS(results->GetIssues().ToOneLineString(), "Results are not ready");
            }
        } while (!results->HasResultSet());
        TResultSetParser resultSet(results->ExtractResultSet());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 2);

        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "1");
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "trololo");

        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "2");
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "hello world");
    }


}

} // namespace NKqp
} // namespace NKikimr
