#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <util/system/env.h>

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/Aws.h>
#include <aws/s3/model/CreateBucketRequest.h>
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

NYdb::NQuery::TScriptExecutionOperation WaitScriptExecutionOperation(const NYdb::TOperation::TOperationId& operationId, const NYdb::TDriver& ydbDriver) {
    NYdb::NOperation::TOperationClient client(ydbDriver);
    NThreading::TFuture<NYdb::NQuery::TScriptExecutionOperation> op;
    do {
        if (!op.Initialized()) {
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
            "location"_a = GetEnv("S3_ENDPOINT") + "/" + bucket + "/",
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
        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Metadata().ExecutionId).ExtractValueSync();
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
            "location"_a = GetEnv("S3_ENDPOINT") + "/" + bucket + "/",
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
            "location"_a = GetEnv("S3_ENDPOINT") + "/" + bucket + "/"
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
        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Metadata().ExecutionId).ExtractValueSync();
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
                "location"_a = GetEnv("S3_ENDPOINT") + "/" + bucket + "/",
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
        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Metadata().ExecutionId).ExtractValueSync();
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
            "location"_a = GetEnv("S3_ENDPOINT") + "/" + bucket + "/",
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
        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Metadata().ExecutionId).ExtractValueSync();
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
                "location"_a = GetEnv("S3_ENDPOINT") + "/" + bucket + "/",
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
        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Metadata().ExecutionId).ExtractValueSync();
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
}

} // namespace NKqp
} // namespace NKikimr
