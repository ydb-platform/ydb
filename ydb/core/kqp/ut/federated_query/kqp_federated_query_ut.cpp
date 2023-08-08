#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <util/system/env.h>

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/Aws.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/S3Client.h>

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_types/operation/operation.h>

#include <fmt/format.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

constexpr TStringBuf TEST_CONTENT =
R"({"key": "1", "value": "trololo"}
{"key": "2", "value": "hello world"}
)"sv;

constexpr TStringBuf TEST_CONTENT_KEYS =
R"({"key": "1"}
{"key": "3"}
)"sv;

const TString TEST_SCHEMA = R"(["StructType";[["key";["DataType";"Utf8";];];["value";["DataType";"Utf8";];];];])";

const TString TEST_SCHEMA_IDS = R"(["StructType";[["key";["DataType";"Utf8";];];];])";

bool InitAwsApi() {
    Aws::InitAPI(Aws::SDKOptions());
    return true;
}

bool EnsureAwsApiInited() {
    static const bool inited = InitAwsApi();
    return inited;
}

Aws::S3::S3Client MakeS3Client() {
    EnsureAwsApiInited();

    Aws::Client::ClientConfiguration s3ClientConfig;
    s3ClientConfig.endpointOverride = GetEnv("S3_ENDPOINT");
    s3ClientConfig.scheme = Aws::Http::Scheme::HTTP;
    return Aws::S3::S3Client(std::make_shared<Aws::Auth::AnonymousAWSCredentialsProvider>(), s3ClientConfig);
}

void CreateBucket(const TString& bucket, Aws::S3::S3Client& s3Client) {
    Aws::S3::Model::CreateBucketRequest req;
    req.SetBucket(bucket);
    req.SetACL(Aws::S3::Model::BucketCannedACL::public_read_write);
    const Aws::S3::Model::CreateBucketOutcome result = s3Client.CreateBucket(req);
    UNIT_ASSERT_C(result.IsSuccess(), "Error creating bucket \"" << bucket << "\": " << result.GetError().GetExceptionName() << ": " << result.GetError().GetMessage());
}

void CreateBucket(const TString& bucket) {
    Aws::S3::S3Client s3Client = MakeS3Client();

    CreateBucket(bucket, s3Client);
}

void UploadObject(const TString& bucket, const TString& object, const TStringBuf& content, Aws::S3::S3Client& s3Client) {
    Aws::S3::Model::PutObjectRequest req;
    req.WithBucket(bucket).WithKey(object);

    auto inputStream = std::make_shared<std::stringstream>();
    *inputStream << content;
    req.SetBody(inputStream);
    const Aws::S3::Model::PutObjectOutcome result = s3Client.PutObject(req);
    UNIT_ASSERT_C(result.IsSuccess(), "Error uploading object \"" << object << "\" to a bucket \"" << bucket << "\": " << result.GetError().GetExceptionName() << ": " << result.GetError().GetMessage());
}

void UploadObject(const TString& bucket, const TString& object, const TStringBuf& content) {
    Aws::S3::S3Client s3Client = MakeS3Client();

    UploadObject(bucket, object, content, s3Client);
}

void CreateBucketWithObject(const TString& bucket, const TString& object, const TStringBuf& content, Aws::S3::S3Client& s3Client) {
    CreateBucket(bucket, s3Client);
    UploadObject(bucket, object, content, s3Client);
}

void CreateBucketWithObject(const TString& bucket, const TString& object, const TStringBuf& content) {
    Aws::S3::S3Client s3Client = MakeS3Client();

    CreateBucketWithObject(bucket, object, content, s3Client);
}

TString GetObject(const TString& bucket, const TString& object, Aws::S3::S3Client& s3Client) {
    Aws::S3::Model::GetObjectRequest req;
    req.WithBucket(bucket).WithKey(object);

    Aws::S3::Model::GetObjectOutcome outcome = s3Client.GetObject(req);
    UNIT_ASSERT(outcome.IsSuccess());
    Aws::S3::Model::GetObjectResult& result = outcome.GetResult();
    std::istreambuf_iterator<char> eos;
    std::string objContent(std::istreambuf_iterator<char>(result.GetBody()), eos);
    Cerr << "Got object content from \"" << bucket << "." << object << "\"\n" << objContent << Endl;
    return objContent;
}

TString GetObject(const TString& bucket, const TString& object) {
    Aws::S3::S3Client s3Client = MakeS3Client();

    return GetObject(bucket, object, s3Client);
}

std::vector<TString> GetObjectKeys(const TString& bucket, Aws::S3::S3Client& s3Client) {
    Aws::S3::Model::ListObjectsRequest listReq;
    listReq.WithBucket(bucket);

    Aws::S3::Model::ListObjectsOutcome outcome = s3Client.ListObjects(listReq);
    UNIT_ASSERT(outcome.IsSuccess());

    std::vector<TString> keys;
    for (auto& obj : outcome.GetResult().GetContents()) {
        keys.push_back(TString(obj.GetKey()));
        Cerr << "Found S3 object: \"" << obj.GetKey() << "\"" << Endl;
    }
    return keys;
}

std::vector<TString> GetObjectKeys(const TString& bucket) {
    Aws::S3::S3Client s3Client = MakeS3Client();

    return GetObjectKeys(bucket, s3Client);
}

TString GetAllObjects(const TString& bucket, TStringBuf separator, Aws::S3::S3Client& s3Client) {
    std::vector<TString> keys = GetObjectKeys(bucket, s3Client);
    TString result;
    bool firstObject = true;
    for (const TString& key : keys) {
        result += GetObject(bucket, key, s3Client);
        if (!firstObject) {
            result += separator;
        }
        firstObject = false;
    }
    return result;
}

TString GetAllObjects(const TString& bucket, TStringBuf separator = "") {
    Aws::S3::S3Client s3Client = MakeS3Client();

    return GetAllObjects(bucket, separator, s3Client);
}

TString GetBucketLocation(const TStringBuf bucket) {
    return TStringBuilder() << GetEnv("S3_ENDPOINT") << '/' << bucket << '/';
}

NYdb::NQuery::TScriptExecutionOperation WaitScriptExecutionOperation(const NYdb::TOperation::TOperationId& operationId, const NYdb::TDriver& ydbDriver) {
    NYdb::NOperation::TOperationClient client(ydbDriver);
    NThreading::TFuture<NYdb::NQuery::TScriptExecutionOperation> op;
    do {
        if (op.Initialized()) {
            Sleep(TDuration::MilliSeconds(10));
        }
        op = client.Get<NYdb::NQuery::TScriptExecutionOperation>(operationId);
        UNIT_ASSERT_C(op.GetValueSync().Status().IsSuccess(), op.GetValueSync().Status().GetStatus() << ":" << op.GetValueSync().Status().GetIssues().ToString());
    } while (!op.GetValueSync().Ready());
    return op.GetValueSync();
}

Y_UNIT_TEST_SUITE(KqpFederatedQuery) {
    Y_UNIT_TEST(ExecuteScriptWithExternalTableResolve) {
        using namespace fmt::literals;
        const TString externalDataSourceName = "/Root/external_data_source";
        const TString externalTableName = "/Root/test_binding_resolve";
        const TString bucket = "test_bucket1";
        const TString object = "test_object";

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
            "external_source"_a = externalDataSourceName,
            "external_table"_a = externalTableName,
            "location"_a = GetBucketLocation(bucket),
            "object"_a = object
            );
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        const TString sql = fmt::format(R"(
                PRAGMA s3.AtomicUploadCommit = "1";  --Check that pragmas are OK
                SELECT * FROM `{external_table}`
            )", "external_table"_a=externalTableName);

        auto db = kikimr.GetQueryClient();
        auto scriptExecutionOperation = db.ExecuteScript(sql).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
        UNIT_ASSERT(scriptExecutionOperation.Metadata().ExecutionId);

        NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr.GetDriver());
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecStatus, EExecStatus::Completed);
        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());

        TResultSetParser resultSet(results.ExtractResultSet());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 2);

        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "1");
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "trololo");

        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "2");
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "hello world");
    }

    Y_UNIT_TEST(ExecuteQueryWithExternalTableResolve) {
        using namespace fmt::literals;
        const TString externalDataSourceName = "/Root/external_data_source";
        const TString externalTableName = "/Root/test_binding_resolve";
        const TString bucket = "test_bucket_execute_query";
        const TString object = "test_object";

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
            "external_source"_a = externalDataSourceName,
            "external_table"_a = externalTableName,
            "location"_a = GetBucketLocation(bucket),
            "object"_a = object
            );
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        const TString sql = fmt::format(R"(
                SELECT * FROM `{external_table}`
            )", "external_table"_a=externalTableName);

        auto db = kikimr.GetQueryClient();
        auto executeQueryIterator = db.StreamExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();

        size_t currentRow = 0;
        while (true) {
            auto part = executeQueryIterator.ReadNext().ExtractValueSync();
            if (!part.IsSuccess()) {
                UNIT_ASSERT_C(part.EOS(), part.GetIssues().ToString());
                break;
            }

            UNIT_ASSERT(part.HasResultSet());

            auto result = part.GetResultSet();

            TResultSetParser resultSet(result);
            while (resultSet.TryNextRow()) {
                if (currentRow == 0) {
                    UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "1");
                    UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "trololo");
                } else if (currentRow == 1) {
                    UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "2");
                    UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "hello world");
                } else {
                    UNIT_ASSERT(false);
                }
                ++currentRow;
            }
            UNIT_ASSERT(currentRow > 0);
        }
    }

    Y_UNIT_TEST(ExecuteScriptWithDataSource) {
        using namespace fmt::literals;
        const TString externalDataSourceName = "/Root/external_data_source";
        const TString bucket = "test_bucket3";

        CreateBucketWithObject(bucket, "test_object", TEST_CONTENT);

        auto kikimr = DefaultKikimrRunner();
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
        auto tc = kikimr.GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        const TString query = fmt::format(R"(
            CREATE EXTERNAL DATA SOURCE `{external_source}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{location}",
                AUTH_METHOD="NONE"
            );)",
            "external_source"_a = externalDataSourceName,
            "location"_a = GetBucketLocation(bucket)
            );
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        auto db = kikimr.GetQueryClient();
        auto scriptExecutionOperation = db.ExecuteScript(fmt::format(R"(
            SELECT * FROM `{external_source}`.`/` WITH (
                format="json_each_row",
                schema(
                    key Utf8 NOT NULL,
                    value Utf8 NOT NULL
                )
            )
        )", "external_source"_a = externalDataSourceName)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
        UNIT_ASSERT(scriptExecutionOperation.Metadata().ExecutionId);

        NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr.GetDriver());
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecStatus, EExecStatus::Completed);
        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());

        TResultSetParser resultSet(results.ExtractResultSet());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 2);
        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "1");
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "trololo");
        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "2");
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "hello world");
    }

    Y_UNIT_TEST(ExecuteScriptWithDataSourceJoinYdb) {
        using namespace fmt::literals;
        const TString externalDataSourceName = "/Root/external_data_source_2";
        const TString ydbTable = "/Root/ydb_table";
        const TString bucket = "test_bucket4";

        CreateBucketWithObject(bucket, "test_object", TEST_CONTENT);

        auto kikimr = DefaultKikimrRunner();
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
        auto tc = kikimr.GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        {
            const TString query = fmt::format(R"(
                CREATE EXTERNAL DATA SOURCE `{external_source}` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="{location}",
                    AUTH_METHOD="NONE"
                );
                CREATE TABLE `{ydb_table}` (
                    key Utf8,
                    value Utf8,
                    PRIMARY KEY (key)
                );
                )",
                "external_source"_a = externalDataSourceName,
                "location"_a = GetBucketLocation(bucket),
                "ydb_table"_a = ydbTable
                );
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            const TString query = fmt::format(R"(
                REPLACE INTO `{ydb_table}` (key, value) VALUES
                    ("1", "one"),
                    ("2", "two")
                )",
                "ydb_table"_a = ydbTable
                );
            auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }
        auto db = kikimr.GetQueryClient();
        auto scriptExecutionOperation = db.ExecuteScript(fmt::format(R"(
            SELECT t1.key as key, t1.value as v1, t2.value as v2 FROM `{external_source}`.`/` WITH (
                format="json_each_row",
                schema(
                    key Utf8 NOT NULL,
                    value Utf8 NOT NULL
                )
            ) AS t1 JOIN `ydb_table` AS t2 ON t1.key = t2.key
        )"
        , "external_source"_a = externalDataSourceName
        , "ydb_table"_a = ydbTable)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
        UNIT_ASSERT(scriptExecutionOperation.Metadata().ExecutionId);

        NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr.GetDriver());
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecStatus, EExecStatus::Completed);
        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());

        TResultSetParser resultSet(results.ExtractResultSet());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 3);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 2);
        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "1");
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "trololo");
        UNIT_ASSERT_VALUES_EQUAL(*resultSet.ColumnParser(2).GetOptionalUtf8(), "one");
        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "2");
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "hello world");
        UNIT_ASSERT_VALUES_EQUAL(*resultSet.ColumnParser(2).GetOptionalUtf8(), "two");
    }

    Y_UNIT_TEST(ExecuteScriptWithExternalTableResolveCheckPragma) {
        using namespace fmt::literals;
        const TString externalDataSourceName = "/Root/external_data_source";
        const TString externalTableName = "/Root/test_binding_resolve";
        const TString bucket = "test_bucket5";
        const TString object = "test_object";

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
            "external_source"_a = externalDataSourceName,
            "external_table"_a = externalTableName,
            "location"_a = GetBucketLocation(bucket),
            "object"_a = object
            );
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto db = kikimr.GetQueryClient();
        auto scriptExecutionOperation = db.ExecuteScript(fmt::format(R"(
            PRAGMA s3.JsonListSizeLimit = "10";
            PRAGMA s3.SourceCoroActor = 'true';
            PRAGMA Kikimr.OptEnableOlapPushdown = "false";
            SELECT * FROM `{external_table}`
        )", "external_table"_a=externalTableName)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
        UNIT_ASSERT(scriptExecutionOperation.Metadata().ExecutionId);

        NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr.GetDriver());
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecStatus, EExecStatus::Completed);
        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());

        TResultSetParser resultSet(results.ExtractResultSet());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 2);

        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "1");
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "trololo");

        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "2");
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "hello world");
    }

    Y_UNIT_TEST(ExecuteScriptWithDataSourceJoinYdbCheckPragma) {
        using namespace fmt::literals;
        const TString externalDataSourceName = "/Root/external_data_source_2";
        const TString ydbTable = "/Root/ydb_table";
        const TString bucket = "test_bucket6";

        CreateBucketWithObject(bucket, "test_object", TEST_CONTENT);

        auto kikimr = DefaultKikimrRunner();
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
        auto tc = kikimr.GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        {
            const TString query = fmt::format(R"(
                CREATE EXTERNAL DATA SOURCE `{external_source}` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="{location}",
                    AUTH_METHOD="NONE"
                );
                CREATE TABLE `{ydb_table}` (
                    key Utf8,
                    value Utf8,
                    PRIMARY KEY (key)
                );
                )",
                "external_source"_a = externalDataSourceName,
                "location"_a = GetBucketLocation(bucket),
                "ydb_table"_a = ydbTable
                );
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            const TString query = fmt::format(R"(
                REPLACE INTO `{ydb_table}` (key, value) VALUES
                    ("1", "one"),
                    ("2", "two")
                )",
                "ydb_table"_a = ydbTable
                );
            auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }
        auto db = kikimr.GetQueryClient();
        auto scriptExecutionOperation = db.ExecuteScript(fmt::format(R"(
            PRAGMA s3.JsonListSizeLimit = "10";
            PRAGMA s3.SourceCoroActor = 'true';
            PRAGMA Kikimr.OptEnableOlapPushdown = "false";
            SELECT t1.key as key, t1.value as v1, t2.value as v2 FROM `{external_source}`.`/` WITH (
                format="json_each_row",
                schema(
                    key Utf8 NOT NULL,
                    value Utf8 NOT NULL
                )
            ) AS t1 JOIN `ydb_table` AS t2 ON t1.key = t2.key
        )"
        , "external_source"_a = externalDataSourceName
        , "ydb_table"_a = ydbTable)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
        UNIT_ASSERT(scriptExecutionOperation.Metadata().ExecutionId);

        NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr.GetDriver());
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecStatus, EExecStatus::Completed);
        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());

        TResultSetParser resultSet(results.ExtractResultSet());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 3);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 2);
        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "1");
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "trololo");
        UNIT_ASSERT_VALUES_EQUAL(*resultSet.ColumnParser(2).GetOptionalUtf8(), "one");
        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "2");
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "hello world");
        UNIT_ASSERT_VALUES_EQUAL(*resultSet.ColumnParser(2).GetOptionalUtf8(), "two");
    }

    Y_UNIT_TEST(ExecuteScriptWithDataSourceAndTablePathPrefix) {
        using namespace fmt::literals;
        const TString externalDataSourceName = "external_data_source";
        const TString bucket = "test_bucket7";

        CreateBucketWithObject(bucket, "test_object", TEST_CONTENT);

        auto kikimr = DefaultKikimrRunner();
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
        auto tc = kikimr.GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        const TString query = fmt::format(R"(
            CREATE EXTERNAL DATA SOURCE `{external_source}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{location}",
                AUTH_METHOD="NONE"
            );)",
            "external_source"_a = externalDataSourceName,
            "location"_a = GetEnv("S3_ENDPOINT") + "/" + bucket + "/"
            );
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        auto db = kikimr.GetQueryClient();
        auto scriptExecutionOperation = db.ExecuteScript(fmt::format(R"(
            SELECT * FROM `{external_source}`.`*` WITH (
                format="json_each_row",
                schema(
                    key Utf8 NOT NULL,
                    value Utf8 NOT NULL
                )
            )
        )", "external_source"_a = externalDataSourceName)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
        UNIT_ASSERT(scriptExecutionOperation.Metadata().ExecutionId);

        NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr.GetDriver());
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecStatus, EExecStatus::Completed);
        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());

        TResultSetParser resultSet(results.ExtractResultSet());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 2);
        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "1");
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "trololo");
        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "2");
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "hello world");
    }

    std::pair<NYdb::NQuery::TScriptExecutionOperation, TFetchScriptResultsResult> ExecuteScriptOverBinding(NKikimrConfig::TTableServiceConfig::EBindingsMode mode) {
        using namespace fmt::literals;
        const TString externalDataSourceName = "/Root/external_data_source";
        const TString externalTableName = "/Root/test_binding_resolve";
        const TString bucket = "test_bucket1";
        const TString object = "test_object";

        CreateBucketWithObject(bucket, object, TEST_CONTENT);

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetBindingsMode(mode);

        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableExternalDataSources(true);
        featureFlags.SetEnableScriptExecutionOperations(true);

        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetFeatureFlags(featureFlags);

        TKikimrRunner kikimr(settings);

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
            "external_source"_a = externalDataSourceName,
            "external_table"_a = externalTableName,
            "location"_a = GetEnv("S3_ENDPOINT") + "/" + bucket + "/",
            "object"_a = object
            );
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        const TString sql = fmt::format(R"(
                SELECT * FROM bindings.`{external_table}`
            )", "external_table"_a=externalTableName);

        auto db = kikimr.GetQueryClient();
        auto scriptExecutionOperation = db.ExecuteScript(sql).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
        UNIT_ASSERT(scriptExecutionOperation.Metadata().ExecutionId);

        NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr.GetDriver());
        TFetchScriptResultsResult results(TStatus(EStatus::SUCCESS, {}));
        if (readyOp.Metadata().ExecStatus == EExecStatus::Completed) {
            results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
            UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());
        }
        return {readyOp, results};
    }

    Y_UNIT_TEST(ExecuteScriptWithDifferentBindingsMode) {
        {
            auto [readyOp, results] = ExecuteScriptOverBinding(NKikimrConfig::TTableServiceConfig::BM_DROP);
            UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecStatus, EExecStatus::Completed);
            UNIT_ASSERT_VALUES_EQUAL(readyOp.Status().GetIssues().ToString(), "");

            TResultSetParser resultSet(results.ExtractResultSet());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 2);

            UNIT_ASSERT(resultSet.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "1");
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "trololo");

            UNIT_ASSERT(resultSet.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "2");
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "hello world");

        }

        {
            auto [readyOp, results] = ExecuteScriptOverBinding(NKikimrConfig::TTableServiceConfig::BM_DROP_WITH_WARNING);
            UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecStatus, EExecStatus::Completed);
            UNIT_ASSERT_VALUES_EQUAL(readyOp.Status().GetIssues().ToString(), "<main>:2:31: Warning: Please remove 'bindings.' from your query, the support for this syntax will be dropped soon, code: 4538\n");

            TResultSetParser resultSet(results.ExtractResultSet());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 2);

            UNIT_ASSERT(resultSet.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "1");
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "trololo");

            UNIT_ASSERT(resultSet.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "2");
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "hello world");
        }

        {
            auto [readyOp, results] = ExecuteScriptOverBinding(NKikimrConfig::TTableServiceConfig::BM_ENABLED);
            UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecStatus, EExecStatus::Failed);
            UNIT_ASSERT_VALUES_EQUAL(readyOp.Status().GetIssues().ToString(), "<main>:2:40: Error: Table binding `/Root/test_binding_resolve` is not defined\n");
        }

        {
            auto [readyOp, results] = ExecuteScriptOverBinding(NKikimrConfig::TTableServiceConfig::BM_DISABLED);
            UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecStatus, EExecStatus::Failed);
            UNIT_ASSERT_VALUES_EQUAL(readyOp.Status().GetIssues().ToString(), "<main>:2:31: Error: Please remove 'bindings.' from your query, the support for this syntax has ended, code: 4601\n");
        }
    }

    Y_UNIT_TEST(InsertIntoBucket) {
        using namespace fmt::literals;
        const TString readDataSourceName = "/Root/read_data_source";
        const TString readTableName = "/Root/read_binding";
        const TString readBucket = "test_bucket_read";
        const TString readObject = "test_object_read";
        const TString writeDataSourceName = "/Root/write_data_source";
        const TString writeTableName = "/Root/write_binding";
        const TString writeBucket = "test_bucket_write";
        const TString writeObject = "test_object_write/";

        {
            Aws::S3::S3Client s3Client = MakeS3Client();
            CreateBucketWithObject(readBucket, readObject, TEST_CONTENT, s3Client);
            CreateBucket(writeBucket, s3Client);
        }

        auto kikimr = DefaultKikimrRunner();
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);

        auto tc = kikimr.GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        const TString query = fmt::format(R"(
            CREATE EXTERNAL DATA SOURCE `{read_source}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{read_location}",
                AUTH_METHOD="NONE"
            );
            CREATE EXTERNAL TABLE `{read_table}` (
                key Utf8 NOT NULL,
                value Utf8 NOT NULL
            ) WITH (
                DATA_SOURCE="{read_source}",
                LOCATION="{read_object}",
                FORMAT="json_each_row"
            );

            CREATE EXTERNAL DATA SOURCE `{write_source}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{write_location}",
                AUTH_METHOD="NONE"
            );
            CREATE EXTERNAL TABLE `{write_table}` (
                key Utf8 NOT NULL,
                value Utf8 NOT NULL
            ) WITH (
                DATA_SOURCE="{write_source}",
                LOCATION="{write_object}",
                FORMAT="tsv_with_names"
            );
            )",
            "read_source"_a = readDataSourceName,
            "read_table"_a = readTableName,
            "read_location"_a = GetBucketLocation(readBucket),
            "read_object"_a = readObject,
            "write_source"_a = writeDataSourceName,
            "write_table"_a = writeTableName,
            "write_location"_a = GetBucketLocation(writeBucket),
            "write_object"_a = writeObject
            );
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        const TString sql = fmt::format(R"(
                INSERT INTO `{write_table}`
                SELECT * FROM `{read_table}`
            )",
            "read_table"_a=readTableName,
            "write_table"_a = writeTableName);

        auto db = kikimr.GetQueryClient();
        auto resultFuture = db.ExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx());
        resultFuture.Wait();
        UNIT_ASSERT_C(resultFuture.GetValueSync().IsSuccess(), resultFuture.GetValueSync().GetIssues().ToString());

        TString content = GetAllObjects(writeBucket);
        UNIT_ASSERT_STRING_CONTAINS(content, "key\tvalue\n"); // tsv header
        UNIT_ASSERT_STRING_CONTAINS(content, "1\ttrololo\n");
        UNIT_ASSERT_STRING_CONTAINS(content, "2\thello world\n");
    }

    Y_UNIT_TEST(JoinTwoSources) {
        using namespace fmt::literals;
        const TString dataSource = "/Root/data_source";
        const TString bucket = "test_bucket_mixed";
        const TString dataTable = "/Root/data";
        const TString dataObject = "data";
        const TString keysTable = "/Root/keys";
        const TString keysObject = "keys";

        {
            Aws::S3::S3Client s3Client = MakeS3Client();
            CreateBucket(bucket, s3Client);
            UploadObject(bucket, dataObject, TEST_CONTENT, s3Client);
            UploadObject(bucket, keysObject, TEST_CONTENT_KEYS, s3Client);
        }

        auto kikimr = DefaultKikimrRunner();
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);

        auto tc = kikimr.GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        const TString query = fmt::format(R"(
            CREATE EXTERNAL DATA SOURCE `{data_source}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{bucket_location}",
                AUTH_METHOD="NONE"
            );

            CREATE EXTERNAL TABLE `{data_table}` (
                key Utf8 NOT NULL,
                value Utf8 NOT NULL
            ) WITH (
                DATA_SOURCE="{data_source}",
                LOCATION="{data_object}",
                FORMAT="json_each_row"
            );

            CREATE EXTERNAL TABLE `{keys_table}` (
                key Utf8 NOT NULL
            ) WITH (
                DATA_SOURCE="{data_source}",
                LOCATION="{keys_object}",
                FORMAT="json_each_row"
            );
            )",
            "data_source"_a = dataSource,
            "bucket_location"_a = GetBucketLocation(bucket),
            "data_table"_a = dataTable,
            "data_object"_a = dataObject,
            "keys_table"_a = keysTable,
            "keys_object"_a = keysObject
            );
        auto schemeQueryesult = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(schemeQueryesult.GetStatus() == NYdb::EStatus::SUCCESS, schemeQueryesult.GetIssues().ToString());

        const TString sql = fmt::format(R"(
                SELECT * FROM `{data_table}`
                WHERE key IN (
                    SELECT key FROM `{keys_table}`
                )
            )",
            "data_table"_a = dataTable,
            "keys_table"_a = keysTable);

        auto db = kikimr.GetQueryClient();
        auto resultFuture = db.ExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx());
        resultFuture.Wait();
        UNIT_ASSERT_C(resultFuture.GetValueSync().IsSuccess(), resultFuture.GetValueSync().GetIssues().ToString());
        auto result = resultFuture.GetValueSync().GetResultSetParser(0);
        UNIT_ASSERT_VALUES_EQUAL(result.RowsCount(), 1);
        UNIT_ASSERT(result.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(result.ColumnParser("key").GetUtf8(), "1");
        UNIT_ASSERT_VALUES_EQUAL(result.ColumnParser("value").GetUtf8(), "trololo");
        UNIT_ASSERT(!result.TryNextRow());
    }
}

} // namespace NKqp
} // namespace NKikimr
