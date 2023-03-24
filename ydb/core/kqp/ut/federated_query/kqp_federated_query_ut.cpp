#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <util/system/env.h>

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/Aws.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/S3Client.h>

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
        Cerr << "S3 endpoint: " << GetEnv("S3_ENDPOINT") << Endl;
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
        return;
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1);
        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetInt32(), 42);
    }
}

} // namespace NKqp
} // namespace NKikimr
