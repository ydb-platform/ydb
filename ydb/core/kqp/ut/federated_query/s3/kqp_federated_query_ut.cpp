#include "s3_recipe_ut_helpers.h"

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_scripting.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/operation/operation.h>

#include <yql/essentials/utils/log/log.h>

#include <library/cpp/protobuf/interop/cast.h>

#include <fmt/format.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NKikimr::NKqp::NFederatedQueryTest;
using namespace NTestUtils;
using namespace fmt::literals;

Y_UNIT_TEST_SUITE(KqpFederatedQuery) {
    Y_UNIT_TEST(ExecuteScriptWithExternalTableResolve) {
        const TString externalDataSourceName = "/Root/external_data_source";
        const TString externalTableName = "/Root/test_binding_resolve";
        const TString bucket = "test_bucket1";
        const TString object = TStringBuilder() << "test_" << GetSymbolsString(' ', '~', "*?{}") << "_object";

        CreateBucketWithObject(bucket, object, TEST_CONTENT);

        auto kikimr = NTestUtils::MakeKikimrRunner();

        auto tc = kikimr->GetTableClient();
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
            "object"_a = EscapeC(object)
            );
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        const TString sql = fmt::format(R"(
                PRAGMA s3.AtomicUploadCommit = "1";  --Check that pragmas are OK
                SELECT * FROM `{external_table}`
            )", "external_table"_a=externalTableName);

        auto db = kikimr->GetQueryClient();
        auto scriptExecutionOperation = db.ExecuteScript(sql).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
        UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

        TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
        UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToOneLineString());
        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(results.GetStatus(), EStatus::SUCCESS, results.GetIssues().ToString());

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
        const TString externalDataSourceName = "/Root/external_data_source";
        const TString externalTableName = "/Root/test_binding_resolve";
        const TString bucket = "test_bucket_execute_query";
        const TString object = "test_object";

        CreateBucketWithObject(bucket, object, TEST_CONTENT);

        auto kikimr = NTestUtils::MakeKikimrRunner();

        auto tc = kikimr->GetTableClient();
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
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        const TString sql = fmt::format(R"(
                SELECT * FROM `{external_table}`
            )", "external_table"_a=externalTableName);

        auto db = kikimr->GetQueryClient();
        auto executeQueryIterator = db.StreamExecuteQuery(sql, TTxControl::BeginTx().CommitTx()).ExtractValueSync();

        size_t currentRow = 0;
        while (true) {
            auto part = executeQueryIterator.ReadNext().ExtractValueSync();
            if (!part.IsSuccess()) {
                UNIT_ASSERT_C(part.EOS(), part.GetIssues().ToString());
                break;
            }

            if (!part.HasResultSet()) {
                continue;
            }

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
                    UNIT_FAIL("Unexpected row " << currentRow);
                }
                ++currentRow;
            }
            UNIT_ASSERT_GE(currentRow, 1);
        }
    }

    Y_UNIT_TEST(ExecuteScriptWithS3ReadNotCached) {
        const TString externalDataSourceName = "/Root/external_data_source";
        const TString externalTableName = "/Root/test_binding_resolve";
        const TString bucket = "test_bucket1";
        const TString object = "test_object";

        CreateBucketWithObject(bucket, object, TEST_CONTENT);

        auto kikimr = NTestUtils::MakeKikimrRunner();

        auto tc = kikimr->GetTableClient();
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
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto settings = TExecuteScriptSettings().StatsMode(EStatsMode::Basic);

        const TString sql = fmt::format(R"(
                SELECT * FROM `{external_table}`
            )", "external_table"_a=externalTableName);

        auto db = kikimr->GetQueryClient();
        auto scriptExecutionOperation = db.ExecuteScript(sql, settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
        UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

        TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
        UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(TProtoAccessor().GetProto(readyOp.Metadata().ExecStats).compilation().from_cache(), false);

        scriptExecutionOperation = db.ExecuteScript(sql, settings).ExtractValueSync();
        readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
        UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(TProtoAccessor().GetProto(readyOp.Metadata().ExecStats).compilation().from_cache(), false);
    }

    Y_UNIT_TEST(ExecuteScriptWithDataSource) {
        const TString externalDataSourceName = "/Root/external_data_source";
        const TString bucket = "test_bucket3";

        CreateBucketWithObject(bucket, "test_object", TEST_CONTENT);

        auto kikimr = NTestUtils::MakeKikimrRunner();
        auto tc = kikimr->GetTableClient();
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
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto db = kikimr->GetQueryClient();
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
        UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

        TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
        UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToOneLineString());
        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(results.GetStatus(), EStatus::SUCCESS, results.GetIssues().ToString());

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
        const TString externalDataSourceName = "/Root/external_data_source_2";
        const TString ydbTable = "/Root/ydb_table";
        const TString bucket = "test_bucket4";

        CreateBucketWithObject(bucket, "test_object", TEST_CONTENT);

        auto kikimr = NTestUtils::MakeKikimrRunner();
        auto tc = kikimr->GetTableClient();
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
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
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
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        auto db = kikimr->GetQueryClient();
        auto scriptExecutionOperation = db.ExecuteScript(fmt::format(R"(
            SELECT t1.key as key, t1.value as v1, t2.value as v2 FROM `{external_source}`.`/` WITH (
                format="json_each_row",
                schema(
                    key Utf8 NOT NULL,
                    value Utf8 NOT NULL
                )
            ) AS t1 JOIN `ydb_table` AS t2 ON t1.key = t2.key
            ORDER BY key
        )"
        , "external_source"_a = externalDataSourceName
        , "ydb_table"_a = ydbTable)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
        UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

        TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
        UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToOneLineString());
        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(results.GetStatus(), EStatus::SUCCESS, results.GetIssues().ToString());

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
        const TString externalDataSourceName = "/Root/external_data_source";
        const TString externalTableName = "/Root/test_binding_resolve";
        const TString bucket = "test_bucket5";
        const TString object = "test_object";

        CreateBucketWithObject(bucket, object, TEST_CONTENT);

        auto kikimr = NTestUtils::MakeKikimrRunner();

        auto tc = kikimr->GetTableClient();
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
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto db = kikimr->GetQueryClient();
        auto scriptExecutionOperation = db.ExecuteScript(fmt::format(R"(
            PRAGMA s3.JsonListSizeLimit = "10";
            PRAGMA s3.SourceCoroActor = 'true';
            PRAGMA kikimr.OptEnableOlapPushdown = "false";
            SELECT * FROM `{external_table}`
        )", "external_table"_a=externalTableName)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
        UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

        TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
        UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToString());
        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(results.GetStatus(), EStatus::SUCCESS, results.GetIssues().ToString());

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
        const TString externalDataSourceName = "/Root/external_data_source_2";
        const TString ydbTable = "/Root/ydb_table";
        const TString bucket = "test_bucket6";

        CreateBucketWithObject(bucket, "test_object", TEST_CONTENT);

        auto kikimr = NTestUtils::MakeKikimrRunner();
        auto tc = kikimr->GetTableClient();
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
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
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
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        auto db = kikimr->GetQueryClient();
        auto scriptExecutionOperation = db.ExecuteScript(fmt::format(R"(
            PRAGMA s3.JsonListSizeLimit = "10";
            PRAGMA s3.SourceCoroActor = 'true';
            PRAGMA kikimr.OptEnableOlapPushdown = "false";
            SELECT t1.key as key, t1.value as v1, t2.value as v2 FROM `{external_source}`.`/` WITH (
                format="json_each_row",
                schema(
                    key Utf8 NOT NULL,
                    value Utf8 NOT NULL
                )
            ) AS t1 JOIN `ydb_table` AS t2 ON t1.key = t2.key
            ORDER BY key
        )"
        , "external_source"_a = externalDataSourceName
        , "ydb_table"_a = ydbTable)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
        UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

        TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
        UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToOneLineString());
        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(results.GetStatus(), EStatus::SUCCESS, results.GetIssues().ToString());

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
        const TString externalDataSourceName = "external_data_source";
        const TString bucket = "test_bucket7";

        CreateBucketWithObject(bucket, "test_object", TEST_CONTENT);

        auto kikimr = NTestUtils::MakeKikimrRunner();
        auto tc = kikimr->GetTableClient();
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
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto db = kikimr->GetQueryClient();
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
        UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

        TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
        UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToOneLineString());
        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(results.GetStatus(), EStatus::SUCCESS, results.GetIssues().ToString());

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

    std::pair<TScriptExecutionOperation, TFetchScriptResultsResult> ExecuteScriptOverBinding(NKikimrConfig::TTableServiceConfig::EBindingsMode mode) {
        const TString externalDataSourceName = "/Root/external_data_source";
        const TString externalTableName = "/Root/test_binding_resolve";
        const TString bucket = "test_bucket1";
        const TString object = "test_object";

        CreateBucketWithObject(bucket, object, TEST_CONTENT);

        auto appConfig = std::make_optional<NKikimrConfig::TAppConfig>();
        appConfig->MutableTableServiceConfig()->SetBindingsMode(mode);

        auto kikimr = NTestUtils::MakeKikimrRunner(appConfig);

        auto tc = kikimr->GetTableClient();
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
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        const TString sql = fmt::format(R"(
                SELECT * FROM bindings.`{external_table}`
            )", "external_table"_a=externalTableName);

        auto db = kikimr->GetQueryClient();
        auto scriptExecutionOperation = db.ExecuteScript(sql).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
        UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

        TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
        TFetchScriptResultsResult results(TStatus(EStatus::SUCCESS, {}));
        if (readyOp.Metadata().ExecStatus == EExecStatus::Completed) {
            results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(results.GetStatus(), EStatus::SUCCESS, results.GetIssues().ToString());
        }
        return {readyOp, results};
    }

    Y_UNIT_TEST(ExecuteScriptWithDifferentBindingsMode) {
        {
            auto [readyOp, results] = ExecuteScriptOverBinding(NKikimrConfig::TTableServiceConfig::BM_DROP);
            UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToOneLineString());
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
            UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(readyOp.Status().GetIssues().ToString(), "<main>:2:31: Warning: Please remove 'bindings.' from your query, the support for this syntax will be dropped soon, code: 4538\n");

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
            UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Failed, readyOp.Status().GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(readyOp.Status().GetIssues().ToString(), "<main>:2:40: Error: Table binding `/Root/test_binding_resolve` is not defined\n");
        }

        {
            auto [readyOp, results] = ExecuteScriptOverBinding(NKikimrConfig::TTableServiceConfig::BM_DISABLED);
            UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Failed, readyOp.Status().GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(readyOp.Status().GetIssues().ToString(), "<main>:2:31: Error: Please remove 'bindings.' from your query, the support for this syntax has ended, code: 4601\n");
        }
    }

    Y_UNIT_TEST(MultiStatementSelect) {
        const TString readDataSourceName = "/Root/read_data_source";
        const TString readBucket = "test_read_muti_statement";

        {
            Aws::S3::S3Client s3Client = MakeS3Client();
            CreateBucket(readBucket, s3Client);
        }

        auto kikimr = NTestUtils::MakeKikimrRunner();

        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        const TString query = fmt::format(R"(
            CREATE EXTERNAL DATA SOURCE `{read_source}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{read_location}",
                AUTH_METHOD="NONE"
            );
            )",
            "read_source"_a = readDataSourceName,
            "read_location"_a = GetBucketLocation(readBucket)
        );

        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        const TString sql = fmt::format(R"(
                SELECT 42;

                SELECT * FROM `{read_source}`.`some/path/` WITH (
                    FORMAT = "csv_with_names",
                    SCHEMA = (
                        id Int32,
                        payload String
                    )
                );

                SELECT 84;
            )",
            "read_source"_a = readDataSourceName
        );

        auto db = kikimr->GetQueryClient();
        auto scriptExecutionOperation = db.ExecuteScript(sql).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());

        TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
        UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToString());
        const int64_t numberResultSets = 3;
        UNIT_ASSERT_VALUES_EQUAL(readyOp.Metadata().ResultSetsMeta.size(), numberResultSets);


        for (int64_t resultSetId = 0; resultSetId < numberResultSets; ++resultSetId) {
            TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), resultSetId).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(results.GetStatus(), EStatus::SUCCESS, "Result set id: " << resultSetId << ", reason: " << results.GetIssues().ToString());
            TResultSetParser resultSet(results.ExtractResultSet());

            if (resultSetId == 1) {
                UNIT_ASSERT_VALUES_EQUAL_C(resultSet.ColumnsCount(), 2, resultSetId);
                UNIT_ASSERT_VALUES_EQUAL_C(resultSet.RowsCount(), 0, resultSetId);
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(resultSet.ColumnsCount(), 1, resultSetId);
                UNIT_ASSERT_VALUES_EQUAL_C(resultSet.RowsCount(), 1, resultSetId);

                UNIT_ASSERT_C(resultSet.TryNextRow(), resultSetId);
                if (resultSetId == 0) {
                    UNIT_ASSERT_VALUES_EQUAL_C(resultSet.ColumnParser(0).GetInt32(), 42, resultSetId);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL_C(resultSet.ColumnParser(0).GetInt32(), 84, resultSetId);
                }
            }
        }
    }

    Y_UNIT_TEST(InsertIntoBucket) {
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

        auto kikimr = NTestUtils::MakeKikimrRunner();

        auto tc = kikimr->GetTableClient();
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
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        const TString sql = fmt::format(R"(
                INSERT INTO `{write_table}`
                SELECT * FROM `{read_table}`
            )",
            "read_table"_a=readTableName,
            "write_table"_a = writeTableName);

        auto db = kikimr->GetQueryClient();
        auto resultFuture = db.ExecuteQuery(sql, TTxControl::BeginTx().CommitTx());
        resultFuture.Wait();
        const auto& results = resultFuture.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(results.GetStatus(), EStatus::SUCCESS, results.GetIssues().ToString());

        TString content = GetAllObjects(writeBucket);
        UNIT_ASSERT_STRING_CONTAINS(content, "key\tvalue\n"); // tsv header
        UNIT_ASSERT_STRING_CONTAINS(content, "1\ttrololo\n");
        UNIT_ASSERT_STRING_CONTAINS(content, "2\thello world\n");
    }

    Y_UNIT_TEST(InsertIntoBucketWithSelect) {
        const TString writeDataSourceName = "/Root/write_data_source";
        const TString writeBucket = "test_bucket_write_with_select";

        // Also tests large object path with size >= 128 
        // for atomic upload commit case
        const TString writeObject = TStringBuilder() << "test_object_write/" << TString(512, 'x') << "/";

        {
            Aws::S3::S3Client s3Client = MakeS3Client();
            CreateBucket(writeBucket, s3Client);
        }

        auto kikimr = NTestUtils::MakeKikimrRunner();

        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        const TString query = fmt::format(R"(
            CREATE EXTERNAL DATA SOURCE `{write_source}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{write_location}",
                AUTH_METHOD="NONE"
            );
            )",
            "write_source"_a = writeDataSourceName,
            "write_location"_a = GetBucketLocation(writeBucket)
        );

        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        const TString sql = fmt::format(R"(
                PRAGMA s3.AtomicUploadCommit = "true";

                INSERT INTO `{write_source}`.`{write_object}` WITH (FORMAT = "csv_with_names")
                SELECT * FROM AS_TABLE([<|id: 0, payload: "#######"|>]);

                SELECT * FROM `{write_source}`.`some/path/` WITH (
                    FORMAT = "csv_with_names",
                    SCHEMA = (
                        id Int32,
                        payload String
                    )
                )
            )",
            "write_source"_a = writeDataSourceName,
            "write_object"_a = writeObject
        );

        auto db = kikimr->GetQueryClient();
        auto scriptExecutionOperation = db.ExecuteScript(sql).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());

        TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
        UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Status().GetStatus(), EStatus::SUCCESS, readyOp.Status().GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(readyOp.Metadata().ResultSetsMeta.size(), 1);

        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(results.GetStatus(), EStatus::SUCCESS, results.GetIssues().ToString());
        TResultSetParser resultSet(results.ExtractResultSet());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 0);

        TString content = GetAllObjects(writeBucket);
        UNIT_ASSERT_STRING_CONTAINS(content, "\"id\",\"payload\"\n");
        UNIT_ASSERT_STRING_CONTAINS(content, "0,\"#######\"\n");
    }

    void ExecuteInsertQuery(TQueryClient& client, const TString& writeTableName, const TString&  readTableName, bool expectCached) {
        const TString sql = fmt::format(R"(
                INSERT INTO `{write_table}`
                SELECT * FROM `{read_table}`;
            )",
            "write_table"_a = writeTableName,
            "read_table"_a = readTableName);
        auto settings = TExecuteQuerySettings().StatsMode(EStatsMode::Basic);
        auto resultFuture = client.ExecuteQuery(sql, TTxControl::BeginTx().CommitTx(), settings);
        resultFuture.Wait();
        auto results = resultFuture.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(results.GetStatus(), EStatus::SUCCESS, results.GetIssues().ToString());
        auto& stats = TProtoAccessor::GetProto(*results.GetStats());
        UNIT_ASSERT_EQUAL_C(stats.compilation().from_cache(), expectCached, "expected: "  << expectCached);
    }

    Y_UNIT_TEST(InsertIntoBucketCaching) {
        const TString writeDataSourceName = "/Root/write_data_source";
        const TString writeTableName = "/Root/write_binding";
        const TString writeBucket = "test_bucket_cache";
        const TString writeObject = "test_object_write/";
        const TString writeAnotherObject = "test_another_object_write/";
        const TString readTableName = "/Root/read_table";
        {
            Aws::S3::S3Client s3Client = MakeS3Client();
            CreateBucket(writeBucket, s3Client);
        }

        auto kikimr = NTestUtils::MakeKikimrRunner();

        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        {
            const TString query = fmt::format(R"(
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

                CREATE TABLE `{read_table}` (
                    key Utf8 NOT NULL,
                    value Utf8 NOT NULL,
                    PRIMARY KEY (key)
                );
                )",
                "write_source"_a = writeDataSourceName,
                "write_table"_a = writeTableName,
                "write_location"_a = GetBucketLocation(writeBucket),
                "write_object"_a = writeObject,
                "read_table"_a = readTableName);


            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            const TString query = fmt::format(R"(
                REPLACE INTO `{read_table}` (key, value) VALUES
                    ("1", "one")
                )",
                "read_table"_a = readTableName);
            auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        auto db = kikimr->GetQueryClient();
        ExecuteInsertQuery(db, writeTableName, readTableName, false);
        ExecuteInsertQuery(db, writeTableName, readTableName, true);
        {
            const TString modifyQuery = fmt::format(R"(
                DROP EXTERNAL TABLE `{write_table}`;
                CREATE EXTERNAL TABLE `{write_table}` (
                    key Utf8 NOT NULL,
                    value Utf8 NOT NULL
                ) WITH (
                    DATA_SOURCE="{write_source}",
                    LOCATION="{write_object}",
                    FORMAT="tsv_with_names"
                );
                )",
                "write_table"_a = writeTableName,
                "write_object"_a = writeAnotherObject,
                "write_source"_a = writeDataSourceName);
            auto result = session.ExecuteSchemeQuery(modifyQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        ExecuteInsertQuery(db, writeTableName, readTableName, false);

        UNIT_ASSERT_VALUES_EQUAL(GetObjectKeys(writeBucket).size(), 3);
    }

    Y_UNIT_TEST(InsertIntoBucketValuesCast) {
        const TString writeDataSourceName = "/Root/write_data_source";
        const TString writeTableName = "/Root/write_binding";
        const TString writeBucket = "test_bucket_values_cast";
        const TString writeObject = "test_object_write/";
        {
            Aws::S3::S3Client s3Client = MakeS3Client();
            CreateBucket(writeBucket, s3Client);
        }

        auto kikimr = NTestUtils::MakeKikimrRunner();

        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        {
            const TString query = fmt::format(R"(
                CREATE EXTERNAL DATA SOURCE `{write_source}` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="{write_location}",
                    AUTH_METHOD="NONE"
                );
                CREATE EXTERNAL TABLE `{write_table}` (
                    key Uint64 NOT NULL,
                    value String NOT NULL
                ) WITH (
                    DATA_SOURCE="{write_source}",
                    LOCATION="{write_object}",
                    FORMAT="tsv_with_names"
                );
                )",
                "write_source"_a = writeDataSourceName,
                "write_table"_a = writeTableName,
                "write_location"_a = GetBucketLocation(writeBucket),
                "write_object"_a = writeObject);

            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        }

        auto db = kikimr->GetQueryClient();
        {
            const TString query = fmt::format(R"(
                INSERT INTO `{write_table}`
                    (key, value)
                VALUES
                    (1, "#######"),
                    (4294967295u, "#######");

                INSERT INTO `{write_source}`.`{write_object}` WITH (FORMAT = "tsv_with_names")
                    (key, value)
                VALUES
                    (1, "#######"),
                    (4294967295u, "#######");

                INSERT INTO `{write_table}` SELECT * FROM AS_TABLE([
                    <|key: 1, value: "#####"|>,
                    <|key: 4294967295u, value: "#####"|>
                ]);

                INSERT INTO `{write_source}`.`{write_object}` WITH (FORMAT = "tsv_with_names")
                SELECT * FROM AS_TABLE([
                    <|key: 1, value: "#####"|>,
                    <|key: 4294967295u, value: "#####"|>
                ]);
                )",
                "write_source"_a = writeDataSourceName,
                "write_table"_a = writeTableName,
                "write_object"_a = writeObject);

            const auto result = db.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        }

        UNIT_ASSERT_VALUES_EQUAL(GetObjectKeys(writeBucket).size(), 4);
    }

    Y_UNIT_TEST(UpdateExternalTable) {
        const TString readDataSourceName = "/Root/read_data_source";
        const TString readTableName = "/Root/read_binding";
        const TString readBucket = "test_bucket_read";
        const TString readObject = "test_object_read";

        auto kikimr = NTestUtils::MakeKikimrRunner();

        auto tc = kikimr->GetTableClient();
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
            )",
            "read_source"_a = readDataSourceName,
            "read_table"_a = readTableName,
            "read_location"_a = GetBucketLocation(readBucket),
            "read_object"_a = readObject
            );
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        const TString sql = R"(
                UPDATE `/Root/read_binding`
                SET key = "abc"u
            )";

        auto db = kikimr->GetQueryClient();
        auto resultFuture = db.ExecuteQuery(sql, TTxControl::BeginTx().CommitTx());
        resultFuture.Wait();
        UNIT_ASSERT_C(!resultFuture.GetValueSync().IsSuccess(), resultFuture.GetValueSync().GetIssues().ToString());

        UNIT_ASSERT_NO_DIFF(resultFuture.GetValueSync().GetIssues().ToString(), "<main>: Error: Pre type annotation, code: 1020\n"
        "    <main>:3:27: Error: Write mode 'update' is not supported for external entities\n");
    }

    Y_UNIT_TEST(JoinTwoSources) {
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

        auto kikimr = NTestUtils::MakeKikimrRunner();

        auto tc = kikimr->GetTableClient();
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
        auto schemeQueryResult = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(schemeQueryResult.GetStatus(), EStatus::SUCCESS, schemeQueryResult.GetIssues().ToString());

        const TString sql = fmt::format(R"(
                SELECT * FROM `{data_table}`
                WHERE key IN (
                    SELECT key FROM `{keys_table}`
                )
            )",
            "data_table"_a = dataTable,
            "keys_table"_a = keysTable);

        auto db = kikimr->GetQueryClient();
        auto resultFuture = db.ExecuteQuery(sql, TTxControl::BeginTx().CommitTx());
        resultFuture.Wait();
        auto results = resultFuture.GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(results.GetStatus(), EStatus::SUCCESS, results.GetIssues().ToString());
        auto result = results.GetResultSetParser(0);
        UNIT_ASSERT_VALUES_EQUAL(result.RowsCount(), 1);
        UNIT_ASSERT(result.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(result.ColumnParser("key").GetUtf8(), "1");
        UNIT_ASSERT_VALUES_EQUAL(result.ColumnParser("value").GetUtf8(), "trololo");
        UNIT_ASSERT(!result.TryNextRow());
    }

    Y_UNIT_TEST(ExecuteScriptWithExternalTableResolveCheckPartitionedBy) {
        const TString externalDataSourceName = "/Root/external_data_source";
        const TString externalTableName = "/Root/test_binding_resolve";
        const TString bucket = "test_bucket1";
        const TString object = TStringBuilder() << "year=1/month=2/test_" << GetSymbolsString(' ', '~') << "_object";
        const TString content = "data,year,month\ntest,1,2";

        CreateBucketWithObject(bucket, object, content);

        auto kikimr = NTestUtils::MakeKikimrRunner();

        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        const TString query = fmt::format(R"(
        CREATE EXTERNAL DATA SOURCE `{external_source}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{location}",
                AUTH_METHOD="NONE"
            );
            CREATE EXTERNAL TABLE `{external_table}` (
                data STRING NOT NULL,
                year UINT32 NOT NULL,
                month UINT32 NOT NULL
            ) WITH (
                DATA_SOURCE="{external_source}",
                LOCATION="/",
                FORMAT="csv_with_names",
                PARTITIONED_BY="[year, month]"
            );)",
            "external_source"_a = externalDataSourceName,
            "external_table"_a = externalTableName,
            "location"_a = GetBucketLocation(bucket)
            );

        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto db = kikimr->GetQueryClient();
        const TString sql = fmt::format(R"(
                SELECT * FROM `{external_table}`
            )", "external_table"_a = externalTableName);

        auto scriptExecutionOperation = db.ExecuteScript(sql).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());

        TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
        UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToOneLineString());
        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(results.GetStatus(), EStatus::SUCCESS, results.GetIssues().ToString());

        TResultSetParser resultSet(results.ExtractResultSet());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 3);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1);

        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("data").GetString(), "test");
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("year").GetUint32(), 1);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("month").GetUint32(), 2);
    }

    Y_UNIT_TEST(ExecuteScriptWithEmptyCustomPartitioning) {
        const TString bucket = "test_bucket1";
        const TString object = "year=2021/test_object";

        CreateBucketWithObject(bucket, object, "");

        auto kikimr = NTestUtils::MakeKikimrRunner();

        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        const TString query = fmt::format(R"(
            CREATE EXTERNAL DATA SOURCE `/Root/external_data_source` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{location}",
                AUTH_METHOD="NONE"
            );)",
            "location"_a = GetBucketLocation(bucket)
        );

        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto db = kikimr->GetQueryClient();
        const TString sql = R"(
            $projection = @@ {
                "projection.enabled" : "true",
                "storage.location.template" : "/${date}",
                "projection.date.type" : "date",
                "projection.date.min" : "2022-11-02",
                "projection.date.max" : "2023-12-02",
                "projection.date.interval" : "1",
                "projection.date.format" : "/year=%Y",
                "projection.date.unit" : "YEARS"
            } @@;

            SELECT *
            FROM `/Root/external_data_source`.`/`
            WITH (
                FORMAT="raw",

                SCHEMA=(
                    `data` String NOT NULL,
                    `date` Date NOT NULL
                ),

                partitioned_by=(`date`),
                projection=$projection
            )
        )";

        auto scriptExecutionOperation = db.ExecuteScript(sql).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());

        TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
        UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToOneLineString());
        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(results.GetStatus(), EStatus::SUCCESS, results.GetIssues().ToString());

        TResultSetParser resultSet(results.ExtractResultSet());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 0);
    }

    Y_UNIT_TEST(ExecuteScriptWithTruncatedMultiplyResults) {
        const TString bucket = "test_bucket";
        CreateBucket(bucket);

        constexpr size_t NUMBER_OF_OBJECTS = 20;
        constexpr size_t ROWS_LIMIT = NUMBER_OF_OBJECTS / 2;

        const TString object = "/test_object";
        const TString content = "test content";
        for (size_t i = 0; i < NUMBER_OF_OBJECTS; ++i) {
            UploadObject(bucket, object + ToString(i), content);
        }

        NKikimrConfig::TAppConfig appCfg;
        appCfg.MutableQueryServiceConfig()->set_scriptresultrowslimit(ROWS_LIMIT);
        appCfg.MutableTableServiceConfig()->MutableQueryLimits()->set_resultrowslimit(ROWS_LIMIT);

        auto kikimr = NTestUtils::MakeKikimrRunner(appCfg);

        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        const TString query = fmt::format(R"(
            CREATE EXTERNAL DATA SOURCE `/Root/external_data_source` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{location}",
                AUTH_METHOD="NONE"
            );)",
            "location"_a = GetBucketLocation(bucket)
        );

        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto db = kikimr->GetQueryClient();
        const TString sql = R"(
            SELECT `data`
            FROM `/Root/external_data_source`.`/`
            WITH (
                FORMAT="raw",
                SCHEMA=(
                    `data` String NOT NULL
                )
            )
        )";

        auto scriptExecutionOperation = db.ExecuteScript(sql).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());

        TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
        UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToOneLineString());
        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(results.GetStatus(), EStatus::SUCCESS, results.GetIssues().ToString());

        TResultSetParser resultSet(results.ExtractResultSet());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), ROWS_LIMIT);

        for (size_t i = 0; i < ROWS_LIMIT; ++i) {
            resultSet.TryNextRow();
            UNIT_ASSERT_VALUES_EQUAL(resultSet.GetValue(0).GetProto().bytes_value(), content);
        }
    }

    Y_UNIT_TEST(ForbiddenCallablesForYdbTables) {
        const TString readDataSourceName = "/Root/read_data_source";
        const TString readTableName = "/Root/read_table";
        const TString readBucket = "test_read_bucket_forbidden_callables";
        const TString readObject = "test_object_forbidden_callables";
        const TString writeDataSourceName = "/Root/write_data_source";
        const TString writeTableName = "/Root/write_table";
        const TString writeBucket = "test_write_bucket_forbidden_callables";
        const TString writeObject = "test_object_forbidden_callables/";
        const TString writeYdbTable = "/Root/test_ydb_table";

        {
            Aws::S3::S3Client s3Client = MakeS3Client();
            CreateBucketWithObject(readBucket, readObject, TEST_CONTENT, s3Client);
            CreateBucket(writeBucket, s3Client);
        }

        auto kikimr = NTestUtils::MakeKikimrRunner();

        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        const TString query = fmt::format(R"(
            CREATE EXTERNAL DATA SOURCE `{read_source}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{read_location}",
                AUTH_METHOD="NONE"
            );
            CREATE EXTERNAL TABLE `{read_table}` (
                key Utf8, -- Nullable
                value Utf8 -- Nullable
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
                FORMAT="json_each_row"
            );

            CREATE TABLE `{write_ydb_table}` (
                key Utf8 NOT NULL,
                value Utf8 NOT NULL,
                PRIMARY KEY (key)
            );
            )",
            "read_source"_a = readDataSourceName,
            "read_table"_a = readTableName,
            "read_location"_a = GetBucketLocation(readBucket),
            "read_object"_a = readObject,
            "write_source"_a = writeDataSourceName,
            "write_table"_a = writeTableName,
            "write_location"_a = GetBucketLocation(writeBucket),
            "write_object"_a = writeObject,
            "write_ydb_table"_a = writeYdbTable
            );
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        // Forbidden callable like Unwrap is allowed in S3-only queries,
        // but not allowed in mixed queries.
        {
            const TString sql = fmt::format(R"(
                    INSERT INTO `{write_table}`
                    SELECT Unwrap(key) AS key, Unwrap(value) AS value FROM `{read_table}`
                )",
                "read_table"_a=readTableName,
                "write_table"_a = writeTableName);

            auto db = kikimr->GetQueryClient();
            auto resultFuture = db.ExecuteQuery(sql, TTxControl::BeginTx().CommitTx());
            resultFuture.Wait();
            auto results = resultFuture.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(results.GetStatus(), EStatus::SUCCESS, results.GetIssues().ToString());
        }

        // Unwrap is used in query with effect applied to YDB table.
        {
            const TString sql = fmt::format(R"(
                    INSERT INTO `{write_table}`
                    SELECT Unwrap(key) AS key, Unwrap(value) AS value FROM `{read_table}`;

                    DELETE FROM `{write_ydb_table}`
                    WHERE key = "42";
                )",
                "read_table"_a=readTableName,
                "write_table"_a = writeTableName,
                "write_ydb_table"_a = writeYdbTable);

            auto db = kikimr->GetQueryClient();
            auto resultFuture = db.ExecuteQuery(sql, TTxControl::BeginTx().CommitTx());
            resultFuture.Wait();
            auto results = resultFuture.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(results.GetStatus(), EStatus::SUCCESS, results.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(ExecuteScriptWithLocationWithoutSlashAtTheEnd) {
        const TString externalDataSourceName = "/Root/external_data_source";
        const TString externalTableName = "/Root/test_binding_resolve";
        const TString bucket = "test_bucket_with_location_without_slash_at_the_end";
        const TString object = "year=1/month=2/test_object";
        const TString content = "data,year,month\ntest,1,2";

        CreateBucketWithObject(bucket, object, content);

        auto kikimr = NTestUtils::MakeKikimrRunner();

        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        const TString query = fmt::format(R"(
        CREATE EXTERNAL DATA SOURCE `{external_source}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{location}",
                AUTH_METHOD="NONE"
            );
            CREATE EXTERNAL TABLE `{external_table}` (
                data STRING NOT NULL,
                year UINT32 NOT NULL,
                month UINT32 NOT NULL
            ) WITH (
                DATA_SOURCE="{external_source}",
                LOCATION="/",
                FORMAT="csv_with_names",
                PARTITIONED_BY="[year, month]"
            );)",
            "external_source"_a = externalDataSourceName,
            "external_table"_a = externalTableName,
            "location"_a = TStringBuilder() << GetEnv("S3_ENDPOINT") << '/' << bucket
            );

        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto db = kikimr->GetQueryClient();
        const TString sql = fmt::format(R"(
                SELECT * FROM `{external_table}`
            )", "external_table"_a = externalTableName);

        auto scriptExecutionOperation = db.ExecuteScript(sql).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());

        TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
        UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToOneLineString());
        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(results.GetStatus(), EStatus::SUCCESS, results.GetIssues().ToString());

        TResultSetParser resultSet(results.ExtractResultSet());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 3);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1);

        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("data").GetString(), "test");
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("year").GetUint32(), 1);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("month").GetUint32(), 2);
    }

    TString CreateSimpleGenericQuery(std::shared_ptr<TKikimrRunner> kikimr, const TString& bucket) {
        const TString externalDataSourceName = "/Root/external_data_source";
        const TString object = "test_object";
        const TString content = "key\n1";

        CreateBucketWithObject(bucket, object, content);

        auto tc = kikimr->GetTableClient();
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
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        return fmt::format(R"(
                SELECT * FROM `{external_source}`.`/` WITH (
                    FORMAT="csv_with_names",
                    SCHEMA (
                        key Int NOT NULL
                    )
                )
            )", "external_source"_a=externalDataSourceName);
    }

    Y_UNIT_TEST(StreamExecuteScriptWithGenericAutoDetection) {
        auto kikimr = NTestUtils::MakeKikimrRunner();
        const TString sql = CreateSimpleGenericQuery(kikimr, "test_bucket_stream_execute_generic_auto_detection");

        auto driver = kikimr->GetDriver();
        NScripting::TScriptingClient yqlScriptClient(driver);

        auto it = yqlScriptClient.StreamExecuteYqlScript(sql).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());

        CompareYson("[[[1]]]", StreamResultToYson(it));
    }

    Y_UNIT_TEST(ExecuteScriptWithGenericAutoDetection) {
        auto kikimr = NTestUtils::MakeKikimrRunner();
        const TString sql = CreateSimpleGenericQuery(kikimr, "test_bucket_execute_generic_auto_detection");

        auto driver = kikimr->GetDriver();
        NScripting::TScriptingClient yqlScriptClient(driver);

        auto scriptResult = yqlScriptClient.ExecuteYqlScript(sql).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptResult.GetStatus(), EStatus::SUCCESS, scriptResult.GetIssues().ToString());

        TResultSetParser resultSet(scriptResult.GetResultSet(0));
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1);

        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("key").GetInt32(), 1);
    }

    Y_UNIT_TEST(ExplainScriptWithGenericAutoDetection) {
        auto kikimr = NTestUtils::MakeKikimrRunner();
        const TString sql = CreateSimpleGenericQuery(kikimr, "test_bucket_explain_generic_auto_detection");

        auto driver = kikimr->GetDriver();
        NScripting::TScriptingClient yqlScriptClient(driver);

        NScripting::TExplainYqlRequestSettings settings;
        settings.Mode(NScripting::ExplainYqlRequestMode::Plan);

        auto scriptResult = yqlScriptClient.ExplainYqlScript(sql, settings).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptResult.GetStatus(), EStatus::SUCCESS, scriptResult.GetIssues().ToString());
        UNIT_ASSERT(!scriptResult.GetPlan().empty());
    }

    Y_UNIT_TEST(ReadFromDataSourceWithoutTable) {
        const TString externalDataSourceName = "/Root/external_data_source";
        const TString bucket = "test_bucket_inline_desc";
        const TString object = "test_object_inline_desc";

        CreateBucketWithObject(bucket, object, TEST_CONTENT);

        auto kikimr = NTestUtils::MakeKikimrRunner();

        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        const TString query = fmt::format(R"sql(
            CREATE EXTERNAL DATA SOURCE `{external_source}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{location}",
                AUTH_METHOD="NONE"
            );)sql",
            "external_source"_a = externalDataSourceName,
            "location"_a = GetBucketLocation(bucket),
            "object"_a = object
            );
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        {
            const TString sql = fmt::format(R"sql(
                    SELECT * FROM `{external_data_source}`;
                )sql",
                "external_data_source"_a=externalDataSourceName);

            auto db = kikimr->GetQueryClient();
            auto scriptExecutionOperation = db.ExecuteScript(sql).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
            UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

            TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
            UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Failed, readyOp.Status().GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(readyOp.Status().GetIssues().ToString(), "Attempt to read from external data source");
        }

        // select using inline syntax is well
        {
            const TString sql = fmt::format(R"sql(
                    SELECT * FROM `{external_data_source}`.`{obj_path}`
                    WITH (
                        SCHEMA = (
                            key Utf8 NOT NULL,
                            value Utf8 NOT NULL
                        ),
                        FORMAT = "json_each_row"
                    )
                )sql",
                "external_data_source"_a=externalDataSourceName,
                "obj_path"_a = object);

            auto db = kikimr->GetQueryClient();
            auto scriptExecutionOperation = db.ExecuteScript(sql).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
            UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

            TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
            UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToOneLineString());
            TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(results.GetStatus(), EStatus::SUCCESS, results.GetIssues().ToString());

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
    }

    Y_UNIT_TEST(InsertIntoDataSourceWithoutTable) {
        const TString readDataSourceName = "/Root/read_data_source";
        const TString readTableName = "/Root/read_binding";
        const TString readBucket = "test_bucket_read_insert_into_data_source";
        const TString readObject = "test_object_read";
        const TString writeDataSourceName = "/Root/write_data_source";
        const TString writeTableName = "/Root/write_binding";
        const TString writeBucket = "test_bucket_write_insert_into_data_source";
        const TString writeObject = "test_object_write/";

        {
            Aws::S3::S3Client s3Client = MakeS3Client();
            CreateBucketWithObject(readBucket, readObject, TEST_CONTENT, s3Client);
            CreateBucket(writeBucket, s3Client);
        }

        auto kikimr = NTestUtils::MakeKikimrRunner();

        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        const TString query = fmt::format(R"sql(
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
            )sql",
            "read_source"_a = readDataSourceName,
            "read_table"_a = readTableName,
            "read_location"_a = GetBucketLocation(readBucket),
            "read_object"_a = readObject,
            "write_source"_a = writeDataSourceName,
            "write_table"_a = writeTableName,
            "write_location"_a = GetBucketLocation(writeBucket)
            );
        auto schemeResult = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(schemeResult.GetStatus(), EStatus::SUCCESS, schemeResult.GetIssues().ToString());

        {
            const TString sql = fmt::format(R"sql(
                    INSERT INTO `{write_source}`
                    SELECT * FROM `{read_table}`
                )sql",
                "read_table"_a=readTableName,
                "write_source"_a = writeDataSourceName);

            auto db = kikimr->GetQueryClient();
            auto result = db.ExecuteQuery(sql, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, static_cast<int>(result.GetStatus()) << ", " << result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Attempt to write to external data source");
        }

        // insert with inline syntax is well
        {
            Cerr << "Run inplace insert" << Endl;
            const TString sql = fmt::format(R"sql(
                    INSERT INTO `{write_source}`.`{write_object}`
                    WITH (
                        SCHEMA = (
                            key Utf8 NOT NULL,
                            value Utf8 NOT NULL
                        ),
                        FORMAT = "json_each_row"
                    )
                    SELECT * FROM `{read_table}`
                )sql",
                "read_table"_a=readTableName,
                "write_source"_a = writeDataSourceName,
                "write_object"_a = writeObject);

            auto db = kikimr->GetQueryClient();
            auto result = db.ExecuteQuery(sql, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(SpecifyExternalTableInsteadOfExternalDataSource) {
        const TString externalDataSourceName = "external_data_source";
        const TString externalTableName = "external_table";
        const TString bucket = "test_bucket_specify_external_table";
        const TString object = "test_object_specify_external_table";

        CreateBucketWithObject(bucket, object, TEST_CONTENT);

        auto kikimr = NTestUtils::MakeKikimrRunner();

        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        const TString query = fmt::format(R"sql(
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
            );
            )sql",
            "external_source"_a = externalDataSourceName,
            "external_table"_a = externalTableName,
            "location"_a = GetBucketLocation(bucket),
            "object"_a = object
            );
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        {
            const TString sql = fmt::format(R"sql(
                    SELECT * FROM `{external_table}`.`{object}`
                    WITH (
                        SCHEMA = (
                            key Utf8 NOT NULL,
                            value Utf8 NOT NULL
                        ),
                        FORMAT = "json_each_row"
                    );
                )sql",
                "external_table"_a=externalTableName,
                "object"_a = object);

            auto db = kikimr->GetQueryClient();
            auto queryExecutionOperation = db.ExecuteQuery(sql, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_EQUAL_C(queryExecutionOperation.GetStatus(), EStatus::BAD_REQUEST, static_cast<int>(queryExecutionOperation.GetStatus()) << ", " << queryExecutionOperation.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(queryExecutionOperation.GetIssues().ToString(), "\"/Root/external_table\" is expected to be external data source");
        }

        {
            const TString sql = fmt::format(R"sql(
                    INSERT INTO `{external_table}`.`{object}`
                    WITH (
                        SCHEMA = (
                            key Utf8 NOT NULL,
                            value Utf8 NOT NULL
                        ),
                        FORMAT = "json_each_row"
                    )
                    SELECT * FROM `{external_table}` WHERE key = '42';
                )sql",
                "external_table"_a=externalTableName,
                "object"_a = object);

            auto db = kikimr->GetQueryClient();
            auto queryExecutionOperation = db.ExecuteQuery(sql, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_EQUAL_C(queryExecutionOperation.GetStatus(), EStatus::BAD_REQUEST, static_cast<int>(queryExecutionOperation.GetStatus()) << ", " << queryExecutionOperation.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(queryExecutionOperation.GetIssues().ToString(), "\"/Root/external_table\" is expected to be external data source");
        }
    }

    Y_UNIT_TEST(QueryWithNoDataInS3) {
        const TString externalDataSourceName = "tpc_h_s3_storage_connection";
        const TString bucket = "test_bucket_no_data";

        Aws::S3::S3Client s3Client = MakeS3Client();
        CreateBucket(bucket, s3Client);
        // Uncomment if you want to compare with query with data
        //UploadObject(bucket, "l/l", R"json({"l_extendedprice": 0.0, "l_discount": 1.0, "l_partkey": 1})json", s3Client);
        //UploadObject(bucket, "p/p", R"json({"p_partkey": 1, "p_type": "t"})json", s3Client);

        auto kikimr = NTestUtils::MakeKikimrRunner();
        auto client = kikimr->GetQueryClient();

        {
            const TString query = fmt::format(R"sql(
                CREATE EXTERNAL DATA SOURCE `{external_source}` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="{location}",
                    AUTH_METHOD="NONE"
                );
                )sql",
                "external_source"_a = externalDataSourceName,
                "location"_a = GetBucketLocation(bucket)
                );
            auto result = client.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            // YQ-2750
            const TString query = fmt::format(R"sql(
                $border = Date("1994-08-01");
                select
                    100.00 * sum(case
                        when StartsWith(p.p_type, 'PROMO')
                            then l.l_extendedprice * (1 - l.l_discount)
                        else 0
                    end) / sum(l.l_extendedprice * (1 - l.l_discount)) as promo_revenue
                from
                    {external_source}.`l/` with ( schema (
                        l_extendedprice double,
                        l_discount double,
                        l_partkey int64,
                        l_shipdate date
                    ),
                    format = "json_each_row"
                    ) as l
                join
                    {external_source}.`p/` with ( schema (
                        p_partkey int64,
                        p_type string
                    ),
                    format = "json_each_row"
                    ) as p
                on
                    l.l_partkey = p.p_partkey
                where
                    cast(l.l_shipdate as timestamp) >= $border
                    and cast(l.l_shipdate as timestamp) < ($border + Interval("P31D"));
                )sql",
                "external_source"_a = externalDataSourceName
                );
            auto result = client.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            auto rs = result.GetResultSetParser(0);
            UNIT_ASSERT_VALUES_EQUAL(rs.RowsCount(), 1);
            rs.TryNextRow();
            std::optional<double> sum = rs.ColumnParser(0).GetOptionalDouble();
            UNIT_ASSERT(!sum);
        }
    }

    std::shared_ptr<TKikimrRunner> CreateSampleDataSource(const TString& externalDataSourceName, const TString& externalTableName, bool enableOltp) {
        const TString bucket = "test_bucket3";
        const TString object = "test_object";

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        appConfig.MutableTableServiceConfig()->SetEnableOltpSink(enableOltp);
        appConfig.MutableTableServiceConfig()->SetEnableCreateTableAs(true);
        appConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        appConfig.MutableTableServiceConfig()->SetEnableDataShardCreateTableAs(true);
        appConfig.MutableFeatureFlags()->SetEnableTempTables(true);
        auto kikimr = NTestUtils::MakeKikimrRunner(appConfig, {.DomainRoot = "TestDomain"});

        CreateBucketWithObject(bucket, "test_object", TEST_CONTENT);

        auto tc = kikimr->GetTableClient();
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
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        return kikimr;
    }

    void ValidateResult(const TExecuteQueryResult& result) {
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetResultSets().size(), 1, "Unexpected result sets count");

        TResultSetParser resultSet(result.GetResultSet(0));
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 2);

        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "1");
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "trololo");

        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "2");
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "hello world");

    }

    void ValidateTables(TQueryClient& client, const TString& oltpTable, const TString& olapTable, bool enableOltp) {
        if (enableOltp) {
            const TString query = TStringBuilder() << "SELECT Unwrap(key), Unwrap(value) FROM `" << oltpTable << "`;";
            ValidateResult(client.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync());
        }

        {
            const TString query = TStringBuilder() << "SELECT key, value FROM `" << olapTable << "` ORDER BY key;";
            ValidateResult(client.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync());
        }
    }

    void DoCreateTableAsSelectFromExternalDataSource(std::function<void(const TString&, TQueryClient&, const TDriver&)> requestRunner, bool enableOltp) {
        const TString externalDataSourceName = "external_data_source";
        const TString externalTableName = "test_binding_resolve";

        auto kikimr = CreateSampleDataSource(externalDataSourceName, externalTableName, enableOltp);
        auto client = kikimr->GetQueryClient();

        const TString oltpTable = "DestinationOltp";
        if (enableOltp) {
            const TString query = fmt::format(R"(
                PRAGMA TablePathPrefix = "TestDomain";
                CREATE TABLE `{destination}` (
                    PRIMARY KEY (key, value)
                )
                AS SELECT *
                FROM `{external_source}`.`/` WITH (
                    format="json_each_row",
                    schema(
                        key Utf8 NOT NULL,
                        value Utf8 NOT NULL
                    )
                );)",
                "destination"_a = oltpTable,
                "external_source"_a = externalDataSourceName
            );
            requestRunner(query, client, kikimr->GetDriver());
        }

        const TString olapTable = "DestinationOlap";
        {
            const TString query = fmt::format(R"(
                PRAGMA TablePathPrefix = "TestDomain";
                CREATE TABLE `{destination}` (
                    PRIMARY KEY (key, value)
                )
                WITH (STORE = COLUMN)
                AS SELECT *
                FROM `{external_source}`.`/` WITH (
                    format="json_each_row",
                    schema(
                        key Utf8 NOT NULL,
                        value Utf8 NOT NULL
                    )
                );)",
                "destination"_a = olapTable,
                "external_source"_a = externalDataSourceName
            );
            requestRunner(query, client, kikimr->GetDriver());
        }

        ValidateTables(client, oltpTable, olapTable, enableOltp);
    }

    void RunGenericQuery(const TString& query, TQueryClient& client, const TDriver&) {
        auto result = client.ExecuteQuery(
            query,
            TTxControl::NoTx(),
            TExecuteQuerySettings().StatsMode(EStatsMode::Profile)
        ).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetStats());
        UNIT_ASSERT(result.GetStats()->GetAst());
    }

    void RunGenericScript(const TString& script, TQueryClient& client, const TDriver& driver) {
        auto scriptExecutionOperation = client.ExecuteScript(
            script,
            TExecuteScriptSettings().StatsMode(EStatsMode::Profile)
        ).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
        UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

        TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), driver);
        UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToOneLineString());
        UNIT_ASSERT(readyOp.Metadata().ExecStats.GetAst());
    }

    Y_UNIT_TEST(CreateTableAsSelectFromExternalDataSourceGenericQuery) {
        DoCreateTableAsSelectFromExternalDataSource(&RunGenericQuery, true);
    }

    Y_UNIT_TEST(CreateTableAsSelectFromExternalDataSourceGenericScript) {
        DoCreateTableAsSelectFromExternalDataSource(&RunGenericScript, false);
    }

    void DoCreateTableAsSelectFromExternalTable(std::function<void(const TString&, TQueryClient&, const TDriver&)> requestRunner, bool enableOltp) {
        const TString externalDataSourceName = "external_data_source";
        const TString externalTableName = "test_binding_resolve";

        auto kikimr = CreateSampleDataSource(externalDataSourceName, externalTableName, enableOltp);
        auto client = kikimr->GetQueryClient();

        const TString oltpTable = "DestinationOltp";
        if (enableOltp) {
            const TString query = fmt::format(R"(
                PRAGMA TablePathPrefix = "TestDomain";
                CREATE TABLE `{destination}` (
                    PRIMARY KEY (key, value)
                )
                AS SELECT *
                FROM `{external_table}`;)",
                "destination"_a = oltpTable,
                "external_table"_a = externalTableName
            );
            requestRunner(query, client, kikimr->GetDriver());
        }

        const TString olapTable = "DestinationOlap";
        {
            const TString query = fmt::format(R"(
                PRAGMA TablePathPrefix = "TestDomain";
                CREATE TABLE `{destination}` (
                    PRIMARY KEY (key, value)
                )
                WITH (STORE = COLUMN)
                AS SELECT *
                FROM `{external_table}`;)",
                "destination"_a = olapTable,
                "external_table"_a = externalTableName
            );
            requestRunner(query, client, kikimr->GetDriver());
        }

        ValidateTables(client, oltpTable, olapTable, enableOltp);
    }

    Y_UNIT_TEST(CreateTableAsSelectFromExternalTableGenericQuery) {
        DoCreateTableAsSelectFromExternalTable(&RunGenericQuery, true);
    }

    Y_UNIT_TEST(CreateTableAsSelectFromExternalTableGenericScript) {
        DoCreateTableAsSelectFromExternalTable(&RunGenericScript, false);
    }

    Y_UNIT_TEST(OverridePlannerDefaults) {
        const TString root = "/Root/";
        const TString source = "source";
        const TString table1 = "table1";
        const TString table2 = "table2";
        const TString bucket  = "bucket";
        const TString object1 = "object1";
        const TString object2 = "object2";
        const TString content1 = "foo,bar\naaa,0\nbbb,2";
        const TString content2 = "foo,bar\naaa,1\nbbb,3";

        Aws::S3::S3Client s3Client = MakeS3Client();
        CreateBucket(bucket, s3Client);
        UploadObject(bucket, table1 + "/" + object1, content1, s3Client);
        UploadObject(bucket, table1 + "/" + object2, content2, s3Client);
        UploadObject(bucket, table2 + "/" + object1, content1, s3Client);
        UploadObject(bucket, table2 + "/" + object2, content2, s3Client);

        auto kikimr = NTestUtils::MakeKikimrRunner();
        WaitResourcesPublish(*kikimr);

        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        const TString query = fmt::format(R"(
            CREATE EXTERNAL DATA SOURCE `{source}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{location}",
                AUTH_METHOD="NONE"
            );
            CREATE EXTERNAL TABLE `{table1}` (
                foo STRING NOT NULL,
                bar UINT32 NOT NULL
            ) WITH (
                DATA_SOURCE="{source}",
                LOCATION="/{location_table1}/",
                FORMAT="csv_with_names"
            );
            CREATE EXTERNAL TABLE `{table2}` (
                foo STRING NOT NULL,
                bar UINT32 NOT NULL
            ) WITH (
                DATA_SOURCE="{source}",
                LOCATION="/{location_table2}/",
                FORMAT="csv_with_names"
            );
            )",
            "source"_a = root + source,
            "table1"_a = root + table1,
            "table2"_a = root + table2,
            "location_table1"_a = table1,
            "location_table2"_a = table2,
            "location"_a = TStringBuilder() << GetEnv("S3_ENDPOINT") << '/' << bucket
            );

        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        ui32 source1_id = 0;
        ui32 source2_id = 0;
        ui32 join_id = 0;
        ui32 limit_id = 0;
        auto queryClient = kikimr->GetQueryClient();

        {
            // default planner values

            const TString sql = fmt::format(R"(
                    pragma s3.UseRuntimeListing = "false";
                    pragma ydb.CostBasedOptimizationLevel = "1";

                    SELECT SUM(t1.bar + t2.bar) as sum FROM `{table1}` as t1 JOIN /*+grace()*/ `{table2}`as t2 ON t1.foo = t2.foo
                )",
                "table1"_a = root + table1,
                "table2"_a = root + table2);

            TExecuteQueryResult queryResult = queryClient.ExecuteQuery(
                sql,
                TTxControl::BeginTx().CommitTx(),
                TExecuteQuerySettings().ExecMode(EExecMode::Execute).StatsMode(EStatsMode::Full)).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(queryResult.GetStatus(), EStatus::SUCCESS, queryResult.GetIssues().ToString());
                UNIT_ASSERT(queryResult.GetStats());
                UNIT_ASSERT(queryResult.GetStats()->GetPlan());

                NJson::TJsonValue plan;
                UNIT_ASSERT(NJson::ReadJsonTree(*queryResult.GetStats()->GetPlan(), &plan));
                UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe(), 2);
                UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][1]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe(), 2);
                UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe(), 4);
                UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe(), 1);

                source1_id = plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Stats"]["PhysicalStageId"].GetIntegerSafe();
                source2_id = plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][1]["Plans"][0]["Stats"]["PhysicalStageId"].GetIntegerSafe();
                join_id    = plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Stats"]["PhysicalStageId"].GetIntegerSafe();
                limit_id   = plan["Plan"]["Plans"][0]["Plans"][0]["Stats"]["PhysicalStageId"].GetIntegerSafe();
        }

        {
            // scale down

            const TString sql = fmt::format(R"(
                    pragma s3.UseRuntimeListing = "false";
                    pragma ydb.CostBasedOptimizationLevel = "1";
                    pragma ydb.OverridePlanner = @@ [
                        {{ "tx": 0, "stage": {source1_id}, "tasks": 1 }},
                        {{ "tx": 0, "stage": {source2_id}, "tasks": 1 }},
                        {{ "tx": 0, "stage": {join_id}, "tasks": 1 }},
                        {{ "tx": 0, "stage": {limit_id}, "tasks": 1 }}
                    ] @@;

                    SELECT SUM(t1.bar + t2.bar) as sum FROM `{table1}` as t1 JOIN /*+grace()*/ `{table2}`as t2 ON t1.foo = t2.foo
                )",
                "source1_id"_a = source1_id,
                "source2_id"_a = source2_id,
                "join_id"_a = join_id,
                "limit_id"_a = limit_id,
                "table1"_a = root + table1,
                "table2"_a = root + table2);

            TExecuteQueryResult queryResult = queryClient.ExecuteQuery(
                sql,
                TTxControl::BeginTx().CommitTx(),
                TExecuteQuerySettings().ExecMode(EExecMode::Execute).StatsMode(EStatsMode::Full)).GetValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(queryResult.GetStatus(), EStatus::SUCCESS, queryResult.GetIssues().ToString());
            UNIT_ASSERT(queryResult.GetStats());
            UNIT_ASSERT(queryResult.GetStats()->GetPlan());
            NJson::TJsonValue plan;
            UNIT_ASSERT(NJson::ReadJsonTree(*queryResult.GetStats()->GetPlan(), &plan));
            UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe(), 1);
            UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][1]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe(), 1);
            UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe(), 1);
            UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe(), 1);
        }

        {
            // scale up

            const TString sql = fmt::format(R"(
                    pragma s3.UseRuntimeListing = "false";
                    pragma ydb.CostBasedOptimizationLevel = "1";
                    pragma ydb.OverridePlanner = @@ [
                        {{ "tx": 0, "stage": {source1_id}, "tasks": 10 }},
                        {{ "tx": 0, "stage": {source2_id}, "tasks": 10 }},
                        {{ "tx": 0, "stage": {join_id}, "tasks": 10 }},
                        {{ "tx": 0, "stage": {limit_id}, "tasks": 10 }}
                    ] @@;

                    SELECT SUM(t1.bar + t2.bar) as sum FROM `{table1}` as t1 JOIN /*+grace()*/ `{table2}`as t2 ON t1.foo = t2.foo
                )",
                "source1_id"_a = source1_id,
                "source2_id"_a = source2_id,
                "join_id"_a = join_id,
                "limit_id"_a = limit_id,
                "table1"_a = root + table1,
                "table2"_a = root + table2);

            TExecuteQueryResult queryResult = queryClient.ExecuteQuery(
                sql,
                TTxControl::BeginTx().CommitTx(),
                TExecuteQuerySettings().ExecMode(EExecMode::Execute).StatsMode(EStatsMode::Full)).GetValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(queryResult.GetStatus(), EStatus::SUCCESS, queryResult.GetIssues().ToString());
            UNIT_ASSERT(queryResult.GetStats());
            UNIT_ASSERT(queryResult.GetStats()->GetPlan());
            NJson::TJsonValue plan;
            UNIT_ASSERT(NJson::ReadJsonTree(*queryResult.GetStats()->GetPlan(), &plan));
            // only 2 files => sources stay with 2 tasks
            // join scales to 10 tasks
            // limit ignores hint and keeps being in the only task
            UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe(), 2);
            UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][1]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe(), 2);
            UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe(), 10);
            UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe(), 1);
        }
    }

    Y_UNIT_TEST(TestReadEmptyFileWithCsvFormat) {
        const TString externalDataSourceName = "/Root/external_data_source";
        const TString bucket = "test_bucket12";

        CreateBucketWithObject(bucket, "test_object", "");

        auto kikimr = NTestUtils::MakeKikimrRunner();

        auto tc = kikimr->GetTableClient();
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
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        const TString sql = fmt::format(R"(
                SELECT * FROM `{external_source}`.`/`
                WITH (
                    SCHEMA = (
                        data String
                    ),
                    FORMAT = "csv_with_names"
                )
            )", "external_source"_a=externalDataSourceName);

        auto db = kikimr->GetQueryClient();
        auto scriptExecutionOperation = db.ExecuteScript(sql).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
        UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

        TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
        UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToString());
    }

    Y_UNIT_TEST(TestWildcardValidation) {
        const TString bucket = "test_bucket13";

        CreateBucket(bucket);

        auto kikimr = NTestUtils::MakeKikimrRunner();

        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        const TString query = fmt::format(R"(
            CREATE EXTERNAL DATA SOURCE `/Root/external_data_source` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{location}",
                AUTH_METHOD="NONE"
            );)",
            "location"_a = GetBucketLocation(bucket)
        );
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto db = kikimr->GetQueryClient();

        {  // path validation
            const TString sql = R"(
                    SELECT * FROM `/Root/external_data_source`.`/{` WITH (
                        SCHEMA = (data String),
                        FORMAT = "csv_with_names"
                    ))";

            auto scriptExecutionOperation = db.ExecuteScript(sql).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());

            TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
            UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Failed, readyOp.Status().GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(readyOp.Status().GetIssues().ToString(), "Path '/{' contains invalid wildcard:");
        }

        {  // file pattern validation
            const TString sql = R"(
                    SELECT * FROM `/Root/external_data_source`.`/` WITH (
                        SCHEMA = (data String),
                        FORMAT = "csv_with_names",
                        FILE_PATTERN = "{"
                    ))";

            auto scriptExecutionOperation = db.ExecuteScript(sql).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());

            TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
            UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Failed, readyOp.Status().GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(readyOp.Status().GetIssues().ToString(), "File pattern '{' contains invalid wildcard:");
        }
    }

    Y_UNIT_TEST_TWIN(TestSecretsExistingValidation, UseSchemaSecrets) {
        const TString bucket = "test_bucket14";

        CreateBucket(bucket);

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableExternalSourceSchemaInference(true);
        auto kikimr = NTestUtils::MakeKikimrRunner(appConfig);

        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        { // provide grants
            const auto result = session.ExecuteSchemeQuery("GRANT ALL ON `/Root` TO `test@builtin`").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        { // create secret
            const auto result = session.ExecuteSchemeQuery(
                UseSchemaSecrets ?
                    "CREATE SECRET TestSecret WITH (value = 'test_value')" :
                    "CREATE OBJECT TestSecret (TYPE SECRET) WITH value = `test_value`"
            ).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        const TString query = fmt::format(R"(
            CREATE EXTERNAL DATA SOURCE `/Root/external_data_source` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{location}",
                AUTH_METHOD="SERVICE_ACCOUNT",
                SERVICE_ACCOUNT_ID="TestSa",
                {secret_param_name}="TestSecret"
            );)",
            "location"_a = GetBucketLocation(bucket),
            "secret_param_name"_a = UseSchemaSecrets ? "SERVICE_ACCOUNT_SECRET_PATH" : "SERVICE_ACCOUNT_SECRET_NAME"
        );
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto db = kikimr->GetQueryClient(TClientSettings().AuthToken("test@builtin"));

        const TString sql = R"(
            SELECT * FROM `/Root/external_data_source`.`/` WITH (
                SCHEMA = (data String),
                FORMAT = "csv_with_names"
            ))";

        auto scriptExecutionOperation = db.ExecuteScript(sql).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());

        TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
        UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Failed, readyOp.Status().GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(
            readyOp.Status().GetIssues().ToString(),
            UseSchemaSecrets ?
                "secret `/Root/TestSecret` not found" :
                "secret with name 'TestSecret' not found"
        );
    }

    Y_UNIT_TEST(TestOlapToS3Insert) {
        const TString root = "/Root/";
        const TString source = "source";
        const TString table1 = "table1";
        const TString table2 = "table2";
        const TString bucket = "bucket";

        CreateBucket(bucket);

        auto kikimr = NTestUtils::MakeKikimrRunner();

        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();

        const TString olapTable = "DestinationOlap";

        const TString query = fmt::format(R"(
            CREATE EXTERNAL DATA SOURCE `{source}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{location}",
                AUTH_METHOD="NONE"
            );
            CREATE EXTERNAL TABLE `{table1}` (
                key Int64 NOT NULL,
                value String NOT NULL,
            ) WITH (
                DATA_SOURCE="{source}",
                LOCATION="/{location_table1}/",
                FORMAT="csv_with_names"
            );
            CREATE EXTERNAL TABLE `{table2}` (
                key Int64 NOT NULL,
                value String NOT NULL,
                year String NOT NULL
            ) WITH (
                DATA_SOURCE="{source}",
                LOCATION="/{location_table2}/",
                FORMAT="csv_with_names",
                PARTITIONED_BY="['year']"
            );
            CREATE TABLE `{olap_table}` (
                key Int64 NOT NULL,
                value String NOT NULL,
                PRIMARY KEY (key)
            )
            WITH (STORE = COLUMN);)",
            "location"_a = GetBucketLocation(bucket),
            "source"_a = root + source,
            "table1"_a = root + table1,
            "table2"_a = root + table2,
            "location_table1"_a = table1,
            "location_table2"_a = table2,
            "olap_table"_a = olapTable
        );
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto db = kikimr->GetQueryClient();

        {
            const TString sql = fmt::format(R"(
                    INSERT INTO {destination}
                        SELECT key, value FROM {source};)",
                    "destination"_a = table1,
                    "source"_a = olapTable);

            auto scriptExecutionOperation = db.ExecuteScript(sql).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
            UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

            TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
            UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToString());
        }

        {
            const TString sql = fmt::format(R"(
                    INSERT INTO {destination}
                        SELECT key, value FROM {source} LIMIT 1;)",
                    "destination"_a = table1,
                    "source"_a = olapTable);

            auto scriptExecutionOperation = db.ExecuteScript(sql).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
            UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

            TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
            UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToString());
        }

        {
            const TString sql = fmt::format(R"(
                    INSERT INTO {destination}
                        SELECT key, value, "2024" AS year FROM {source};)",
                    "destination"_a = table2,
                    "source"_a = olapTable);

            auto scriptExecutionOperation = db.ExecuteScript(sql).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
            UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

            TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
            UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToString());
        }

        {
            const TString sql = fmt::format(R"(
                    INSERT INTO {destination}
                        SELECT key, value, "2024" AS year FROM {source} LIMIT 1;)",
                    "destination"_a = table2,
                    "source"_a = olapTable);

            auto scriptExecutionOperation = db.ExecuteScript(sql).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
            UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

            TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
            UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToString());
        }
    }

    void ReadLargeParquetFiles(std::shared_ptr<NKikimr::NKqp::TKikimrRunner> kikimr, const TString& bucket) {
        CreateBucket(bucket);

        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        {
            const TString query = fmt::format(R"(
                CREATE EXTERNAL DATA SOURCE `test_bucket` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="{location}",
                    AUTH_METHOD="NONE"
                );)",
                "location"_a = GetBucketLocation(bucket)
            );
            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto db = kikimr->GetQueryClient();
        {
            const TString query = R"(
                PRAGMA s3.AtomicUploadCommit = "true";
                INSERT INTO test_bucket.`test-inset-splitting/` WITH (FORMAT = "parquet")
                SELECT Random(data) AS data FROM AS_TABLE(ListReplicate(
                    <|data: "x"|>,
                    1000000
                ));
            )";

            // Create two large parquet files
            {
                const auto result = db.ExecuteScript(query).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.Status().GetStatus(), EStatus::SUCCESS, result.Status().GetIssues().ToString());
                const auto readyOp = WaitScriptExecutionOperation(result.Id(), kikimr->GetDriver());
                UNIT_ASSERT_EQUAL_C(readyOp.Status().GetStatus(), EStatus::SUCCESS, readyOp.Status().GetIssues().ToString());
            }
            {
                const auto result = db.ExecuteScript(query).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.Status().GetStatus(), EStatus::SUCCESS, result.Status().GetIssues().ToString());
                const auto readyOp = WaitScriptExecutionOperation(result.Id(), kikimr->GetDriver());
                UNIT_ASSERT_EQUAL_C(readyOp.Status().GetStatus(), EStatus::SUCCESS, readyOp.Status().GetIssues().ToString());
            }
        }

        {   // Actual splitting here:
            // start two read tasks on different nodes
            // write tasks will be scheduled locally
            // so one of the channels is remote
            const TString query = R"(
                PRAGMA s3.AtomicUploadCommit = "true";
                PRAGMA s3.UseRuntimeListing = "false";
                PRAGMA ydb.OverridePlanner = @@ [
                    { "tx": 0, "stage": 0, "tasks": 2 }
                ] @@;

                INSERT INTO test_bucket.`/result/` WITH (FORMAT = "parquet")
                SELECT * FROM test_bucket.`/test-inset-splitting/` WITH (
                    FORMAT = "parquet",
                    SCHEMA (
                        data Double
                    )
                )
            )";
            const auto result = db.ExecuteScript(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.Status().GetStatus(), EStatus::SUCCESS, result.Status().GetIssues().ToString());
            const auto readyOp = WaitScriptExecutionOperation(result.Id(), kikimr->GetDriver());
            UNIT_ASSERT_EQUAL_C(readyOp.Status().GetStatus(), EStatus::SUCCESS, readyOp.Status().GetIssues().ToString());
        }

        const TString query = R"(
            SELECT COUNT(*) FROM test_bucket.`/result/` WITH (
                FORMAT = "parquet",
                SCHEMA (
                    data Double
                )
            )
        )";
        const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);

        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(parser.RowsCount(), 1);
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser(0).GetUint64(), 2000000);
    }

    Y_UNIT_TEST(TestReadLargeParquetFile) {
        NKikimrConfig::TAppConfig config;
        auto& tableService = *config.MutableTableServiceConfig();
        tableService.SetArrayBufferMinFillPercentage(75);
        tableService.SetBlockChannelsMode(NKikimrConfig::TTableServiceConfig::BLOCK_CHANNELS_FORCE);
        tableService.MutableResourceManager()->SetChannelChunkSizeLimit(100000);

        auto kikimr = NTestUtils::MakeKikimrRunner(config, {.NodeCount = 2});
        WaitResourcesPublish(*kikimr);
        ReadLargeParquetFiles(kikimr, "test_read_large_file_bucket");
    }

    Y_UNIT_TEST(TestLocalReadLargeParquetFile) {
        NKikimrConfig::TAppConfig config;
        auto& tableService = *config.MutableTableServiceConfig();
        tableService.ClearArrayBufferMinFillPercentage();
        tableService.SetBlockChannelsMode(NKikimrConfig::TTableServiceConfig::BLOCK_CHANNELS_FORCE);
        tableService.MutableResourceManager()->SetChannelChunkSizeLimit(100000);

        auto kikimr = NTestUtils::MakeKikimrRunner(config);
        ReadLargeParquetFiles(kikimr, "test_local_read_large_file_bucket");
    }

    Y_UNIT_TEST(TestBlockInsertNullColumn) {
        NKikimrConfig::TAppConfig config;
        config.MutableTableServiceConfig()->SetBlockChannelsMode(NKikimrConfig::TTableServiceConfig::BLOCK_CHANNELS_FORCE);
        auto kikimr = NTestUtils::MakeKikimrRunner(config);

        const TString bucket = "test_block_insert_null_column_bucket";
        CreateBucket(bucket);

        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        {
            const TString query = fmt::format(R"(
                CREATE EXTERNAL DATA SOURCE `test_bucket` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="{location}",
                    AUTH_METHOD="NONE"
                );)",
                "location"_a = GetBucketLocation(bucket)
            );
            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto db = kikimr->GetQueryClient();
        {
            const TString query = R"(
                CREATE TABLE `test_table` (
                    PRIMARY KEY (key)
                )
                WITH (STORE = COLUMN)
                AS SELECT * FROM
                AS_TABLE([<|key: 1, value: Nothing(Optional<String>)|>]);
            )";
            const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const TString query = R"(
                PRAGMA s3.UseBlocksSink = "true";

                INSERT INTO test_bucket.`/result/` WITH (FORMAT = "parquet")
                SELECT value FROM test_table
            )";
            const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        const TString query = R"(
            SELECT * FROM test_bucket.`/result/` WITH (
                FORMAT = "parquet",
                SCHEMA (
                    value String
                )
            )
        )";
        const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);

        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(parser.RowsCount(), 1);
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT(!parser.ColumnParser(0).GetOptionalString());
    }

    Y_UNIT_TEST(TestRawFormatInsertValidation) {
        const TString bucket = "test_raw_format_insert_validation_bucket";
        CreateBucket(bucket);

        auto kikimr = NTestUtils::MakeKikimrRunner();

        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        {
            const TString query = fmt::format(R"(
                CREATE EXTERNAL DATA SOURCE `test_bucket` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="{location}",
                    AUTH_METHOD="NONE"
                );)",
                "location"_a = GetBucketLocation(bucket)
            );
            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto db = kikimr->GetQueryClient();
        const TString query = R"(
            INSERT INTO test_bucket.`/result/` WITH (
                FORMAT = "raw",
                SCHEMA (
                    data String??
                )
            )
                (data)
            VALUES
                ("some_string")
        )";
        const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        const auto& issues = result.GetIssues().ToString();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, issues);
        UNIT_ASSERT_STRING_CONTAINS(issues, "Only a column with a primitive type is allowed for the raw format");
    }

    Y_UNIT_TEST(TestPartitionedByInsertValidation) {
        const TString bucket = "test_partitioned_by_insert_validation_bucket";
        CreateBucket(bucket);

        auto kikimr = NTestUtils::MakeKikimrRunner();

        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        {
            const TString query = fmt::format(R"(
                CREATE EXTERNAL DATA SOURCE `test_bucket` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="{location}",
                    AUTH_METHOD="NONE"
                );)",
                "location"_a = GetBucketLocation(bucket)
            );
            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto db = kikimr->GetQueryClient();
        {
            const TString query = R"(
                INSERT INTO test_bucket.`/result/` WITH (
                    FORMAT = "csv_with_names",
                    PARTITIONED_BY = (data)
                )
                    (data)
                VALUES
                    ("some_string")
            )";
            const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            const auto& issues = result.GetIssues().ToString();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, issues);
            UNIT_ASSERT_STRING_CONTAINS(issues, "Write schema contains no columns except partitioning columns.");
        }

        {
            const TString query = R"(
                INSERT INTO test_bucket.`/result/` WITH (
                    FORMAT = "csv_with_names",
                    PARTITIONED_BY = (data)
                )
                    (data)
                VALUES
                    (CurrentUtcTimestamp() + Interval("PT2H"))
            )";
            const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            const auto& issues = result.GetIssues().ToString();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, issues);
            UNIT_ASSERT_STRING_CONTAINS(issues, "At partition key: 'data'");
            UNIT_ASSERT_STRING_CONTAINS(issues, "Expected data type, but got: Optional<Timestamp>");
        }
    }

    Y_UNIT_TEST(TestVariantTypeValidation) {
        const TString bucket = "test_partitioned_by_insert_validation_bucket";
        CreateBucket(bucket);

        auto kikimr = NTestUtils::MakeKikimrRunner();

        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        {
            const TString query = fmt::format(R"(
                CREATE EXTERNAL DATA SOURCE `test_bucket` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="{location}",
                    AUTH_METHOD="NONE"
                );)",
                "location"_a = GetBucketLocation(bucket)
            );
            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto db = kikimr->GetQueryClient();
        {
            const TString query = R"(
                INSERT INTO test_bucket.`/result/` WITH (
                    FORMAT = "json_list"
                )
                    (data)
                VALUES
                    (Variant(6, "foo", Variant<foo: Int32, bar: Bool>))
            )";
            const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        const TString query = R"(
            SELECT * FROM test_bucket.`/result/` WITH (
                FORMAT = "json_list",
                SCHEMA (
                    data Variant<foo: Int32, bar: Bool>
                )
            )
        )";
        const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        const auto& issues = result.GetIssues().ToString();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, issues);
        UNIT_ASSERT_STRING_CONTAINS(issues, "Field 'data' has incompatible with S3 json_list input format type: Variant");
    }

    Y_UNIT_TEST(TestReadingFromFileValidation) {
        {
            TFileOutput out("data.txt");
            out << R"({"data": "test_data"})";
        }

        auto kikimr = NTestUtils::MakeKikimrRunner();
        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().ExtractValueSync().GetSession();
        {
            const TString query = R"(
                CREATE EXTERNAL DATA SOURCE test_bucket WITH (
                    SOURCE_TYPE = "ObjectStorage",
                    LOCATION = "file://./",
                    AUTH_METHOD = "NONE"
                );)";
            const auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto db = kikimr->GetQueryClient();
        {
            const TString query = R"(
                SELECT * FROM test_bucket.`data.txt` WITH (
                    FORMAT = raw,
                    SCHEMA (
                        data String
                    )
                );
            )";
            const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            const auto& issues = result.GetIssues().ToString();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, issues);
            UNIT_ASSERT_STRING_CONTAINS(issues, "Reading from files is not supported with format 'raw'");
        }

        {
            const TString query = R"(
                PRAGMA s3.AsyncDecompressing = "TRUE";

                SELECT * FROM test_bucket.`data.txt` WITH (
                    FORMAT = json_each_row,
                    SCHEMA (
                        data String
                    )
                );
            )";
            const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            const auto& issues = result.GetIssues().ToString();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, issues);
            UNIT_ASSERT_STRING_CONTAINS(issues, "Reading from files is not supported with enabled pragma s3.AsyncDecompressing, to read from files use: PRAGMA s3.AsyncDecompressing = \"FALSE\"");
        }

        {
            const TString query = R"(
                PRAGMA s3.UseRuntimeListing = "TRUE";

                SELECT * FROM test_bucket.`/` WITH (
                    FORMAT = json_each_row,
                    SCHEMA (
                        data String
                    )
                );
            )";
            const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            const auto& issues = result.GetIssues().ToOneLineString();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, issues);
            UNIT_ASSERT_STRING_CONTAINS(issues, "Subdirectory listing is not supported for local files (can not use delimiter: '/')");
        }
    }

    Y_UNIT_TEST(TestAsyncDecompressionErrorHandle) {
        const TString bucket = "test_async_decompressing_error_bucket";
        CreateBucketWithObject(bucket, "test/data.json", R"({"data": "test_data"})");

        auto kikimr = NTestUtils::MakeKikimrRunner();
        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().ExtractValueSync().GetSession();
        {
            const TString query = fmt::format(R"(
                CREATE EXTERNAL DATA SOURCE test_bucket WITH (
                    SOURCE_TYPE = "ObjectStorage",
                    LOCATION = "{location}",
                    AUTH_METHOD = "NONE"
                );)",
                "location"_a = GetBucketLocation(bucket)
            );
            const auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto db = kikimr->GetQueryClient();

        const TString query = R"(
            PRAGMA s3.AsyncDecompressing = "TRUE";

            SELECT * FROM test_bucket.`test/data.json` WITH (
                FORMAT = json_each_row,
                COMPRESSION = zstd,
                SCHEMA (
                    data String
                )
            );
        )";
        const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        const auto& issues = result.GetIssues().ToString();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, issues);
        UNIT_ASSERT_STRING_CONTAINS(issues, "Error while reading file test/data.json");
        UNIT_ASSERT_STRING_CONTAINS(issues, "Decompress failed: Unknown frame descriptor");
    }

    Y_UNIT_TEST(TestAsyncDecompressionWithLargeFile) {
        const TString bucket = "test_async_decompressing_with_large_file_bucket";
        CreateBucket(bucket);

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableQueryServiceConfig()->MutableS3()->SetDataInflight(1_KB);

        auto kikimr = NTestUtils::MakeKikimrRunner(appConfig);
        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().ExtractValueSync().GetSession();
        {
            const TString query = fmt::format(R"(
                CREATE EXTERNAL DATA SOURCE test_bucket WITH (
                    SOURCE_TYPE = "ObjectStorage",
                    LOCATION = "{location}",
                    AUTH_METHOD = "NONE"
                );)",
                "location"_a = GetBucketLocation(bucket)
            );
            const auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto db = kikimr->GetQueryClient();
        {
            const TString query = fmt::format(R"(
                INSERT INTO test_bucket.`test/` WITH (
                    FORMAT = csv_with_names,
                    COMPRESSION = zstd
                ) SELECT
                    data,
                    RandomNumber(TableRow()) AS guid
                FROM AS_TABLE(ListReplicate(<|data: "{payload}"|>, 10000)))",
                "payload"_a = TString(200, 'X')
            );
            const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        const TString query = R"(
            PRAGMA s3.AsyncDecompressing = "TRUE";

            SELECT COUNT(*) FROM test_bucket.`test/` WITH (
                FORMAT = csv_with_names,
                COMPRESSION = zstd,
                SCHEMA (
                    data String NOT NULL,
                    guid Uint64 NOT NULL,
                )
            );
        )";
        const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);

        auto resultSet = result.GetResultSetParser(0);
        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUint64(), 10000);
    }

    Y_UNIT_TEST(TestRestartQueryAndCleanupWithGetOperation) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableDataShardCreateTableAs(true);
        auto kikimr = NTestUtils::MakeKikimrRunner(appConfig, {.EnableScriptExecutionBackgroundChecks = false});
        auto db = kikimr->GetQueryClient();

        constexpr char BUCKET[] = "test_restart_query_and_cleanup_with_get_operation_bucket";
        constexpr char EXTERNAL_DATA_SOURCE_NAME[] = "test_bucket";
        CreateBucket(BUCKET);

        {   // Create external data source
            const TString query = fmt::format(R"(
                CREATE EXTERNAL DATA SOURCE `{source_name}` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="{location}",
                    AUTH_METHOD="NONE"
                ))",
                "location"_a = GetBucketLocation(BUCKET),
                "source_name"_a = EXTERNAL_DATA_SOURCE_NAME
            );
            const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        constexpr char TABLE_NAME[] = "failure_table";

        {   // Create sample table
            const TString query = fmt::format(R"(
                CREATE TABLE `{table_name}` (
                    PRIMARY KEY (Key)
                ) AS
                    SELECT 42 AS Key, Nothing(String?) AS Value)",
                "table_name"_a = TABLE_NAME
            );
            const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        constexpr TDuration TEST_TIMEOUT = TDuration::Seconds(15);
        constexpr TDuration BACKOFF_DURATION = TDuration::Seconds(5);

        auto& runtime = *kikimr->GetTestServer().GetRuntime();
        TString executionId;
        TOperation::TOperationId operationId;

        {   // Start query to retry
            const TString query = fmt::format(R"(
                PRAGMA s3.AtomicUploadCommit = "true";

                ---- First transaction ----

                INSERT INTO `{source_name}`.`test-retries/` WITH (FORMAT = "raw") SELECT 42;
                UPSERT INTO `{table_name}`(Key) VALUES (84);

                ---- Second transaction ----

                SELECT Unwrap(Value) AS Value FROM `{table_name}` WHERE Key = 42)",
                "table_name"_a = TABLE_NAME,
                "source_name"_a = EXTERNAL_DATA_SOURCE_NAME
            );

            NKikimrKqp::TScriptExecutionRetryState::TMapping retryMapping;
            retryMapping.AddStatusCode(Ydb::StatusIds::PRECONDITION_FAILED);
            auto& policy = *retryMapping.MutableExponentialDelayPolicy();
            policy.SetBackoffMultiplier(1.5);
            *policy.MutableInitialBackoff() = NProtoInterop::CastToProto(TDuration::Seconds(1));
            *policy.MutableMaxBackoff() = NProtoInterop::CastToProto(TDuration::Seconds(5));

            NKikimrKqp::TEvQueryRequest queryProto;
            queryProto.SetUserToken(NACLib::TUserToken(BUILTIN_ACL_ROOT, TVector<NACLib::TSID>{runtime.GetAppData().AllAuthenticatedUsers}).SerializeAsString());
            auto& req = *queryProto.MutableRequest();
            req.SetDatabase("/Root");
            req.SetQuery(query);
            req.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
            req.SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT);

            auto ev = MakeHolder<TEvKqp::TEvScriptRequest>();
            ev->Record = queryProto;
            ev->ForgetAfter = TEST_TIMEOUT;
            ev->ResultsTtl = TEST_TIMEOUT;
            ev->RetryMapping = {retryMapping};

            const auto edgeActor = runtime.AllocateEdgeActor();
            runtime.Send(MakeKqpProxyID(runtime.GetNodeId()), edgeActor, ev.Release());

            const auto reply = runtime.GrabEdgeEvent<TEvKqp::TEvScriptResponse>(edgeActor, TEST_TIMEOUT);
            UNIT_ASSERT_C(reply, "CreateScript response is empty");
            UNIT_ASSERT_VALUES_EQUAL_C(reply->Get()->Status, Ydb::StatusIds::SUCCESS, reply->Get()->Issues.ToString());
            UNIT_ASSERT(reply->Get()->ExecutionId);

            executionId = reply->Get()->ExecutionId;
            operationId = TOperation::TOperationId(ScriptExecutionOperationFromExecutionId(executionId));
        }

        // Wait query fail

        NOperation::TOperationClient opClient(kikimr->GetDriver());

        const auto timeout = TInstant::Now() + TEST_TIMEOUT;
        while (true) {
            const auto op = opClient.Get<TScriptExecutionOperation>(operationId).ExtractValueSync();

            const auto& status = op.Status();
            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToString());
            UNIT_ASSERT_C(!op.Ready(), "Operation unexpectedly finished");

            const auto& meta = op.Metadata();
            UNIT_ASSERT_VALUES_EQUAL(meta.ExecutionId, executionId);

            const auto execStatus = meta.ExecStatus;
            if (execStatus == EExecStatus::Failed) {
                break;
            }

            UNIT_ASSERT_C(IsIn({EExecStatus::Starting, EExecStatus::Running}, execStatus), execStatus);

            if (TInstant::Now() > timeout) {
                UNIT_FAIL("Failed to wait for query to finish (before retry)");
            }

            Sleep(TDuration::MilliSeconds(100));
        }

        {   // Fix query failure
            const TString query = fmt::format(R"(
                UPSERT INTO `{table_name}`(Key, Value) VALUES (42, "Some-Val"))",
                "table_name"_a = TABLE_NAME
            );
            const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // Check query results

        Sleep(BACKOFF_DURATION);

        const auto readyOp = WaitScriptExecutionOperation(operationId, kikimr->GetDriver());
        UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Status().GetStatus(), EStatus::SUCCESS, readyOp.Status().GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(readyOp.Status().GetIssues().ToString(), "Script execution operation failed with code PRECONDITION_FAILED and will be restarted");
        UNIT_ASSERT_VALUES_EQUAL(readyOp.Metadata().ExecStatus, EExecStatus::Completed);

        auto results = db.FetchScriptResults(operationId, 0).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(results.GetStatus(), EStatus::SUCCESS, results.GetIssues().ToString());

        TResultSetParser resultSet(results.ExtractResultSet());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1);

        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("Value").GetString(), "Some-Val");

        // Check s3 results

        UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(BUCKET), "42");
        UNIT_ASSERT_VALUES_EQUAL(GetObjectKeys(BUCKET).size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(GetUncommittedUploadsCount(BUCKET), 0);
    }

    Y_UNIT_TEST(TestScriptExecutionsDisabled) {
        const TString bucket = "test_bucket1";
        CreateBucket(bucket);

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableScriptExecutionOperations(false);
        appConfig.MutableFeatureFlags()->SetEnableExternalDataSources(true);

        auto kikimr = std::make_shared<TKikimrRunner>(NKqp::TKikimrSettings(appConfig)
            .SetEnableExternalDataSources(true)
            .SetEnableScriptExecutionOperations(false)
            .SetInitFederatedQuerySetupFactory(true));

        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();

        constexpr char externalDataSourceName[] = "testSource";
        {
            const TString query = fmt::format(R"(
                CREATE EXTERNAL DATA SOURCE `{external_source}` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="{location}",
                    AUTH_METHOD="NONE"
                );)",
                "external_source"_a = externalDataSourceName,
                "location"_a = GetBucketLocation(bucket)
            );
            const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        }

        auto db = kikimr->GetQueryClient();

        {
            const TString query = fmt::format(R"(
                SELECT * FROM `{external_source}`.`/` WITH (
                    FORMAT="raw",
                    SCHEMA (
                        Data String
                    )
                );)",
                "external_source"_a = externalDataSourceName
            );
            const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Unsupported. Failed to load metadata for table: /Root/testSource.[/] (external source factory is doesn't set), please contact internal support");
        }
    }

    void CheckAccessDenied(std::shared_ptr<TKikimrRunner> kikimr, const TString& userSID, const NOperationId::TOperationId& opId, const TString& error) {
        auto db = kikimr->GetQueryClient(userSID ? TClientSettings().AuthToken(userSID) : TClientSettings());
        NOperation::TOperationClient client(
            kikimr->GetDriver(),
            userSID ? TCommonClientSettings().AuthToken(userSID) : TCommonClientSettings()
        );

        {
            const auto result = client.Get<TScriptExecutionOperation>(opId).ExtractValueSync();
            const auto& status = result.Status();
            const auto& issues = status.GetIssues();
            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::UNAUTHORIZED, issues.ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), error);
        }

        {
            const auto result = client.List<TScriptExecutionOperation>().ExtractValueSync();
            const auto& issues = result.GetIssues();

            if (userSID) {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, issues.ToOneLineString());
                UNIT_ASSERT_VALUES_EQUAL(result.GetList().size(), 0);
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNAUTHORIZED, issues.ToOneLineString());
                UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), error);
            }
        }

        {
            const auto result = db.FetchScriptResults(opId, 0).ExtractValueSync();
            const auto& issues = result.GetIssues();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNAUTHORIZED, issues.ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), error);
        }

        {
            const auto result = client.Cancel(opId).ExtractValueSync();
            const auto& issues = result.GetIssues();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNAUTHORIZED, issues.ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), error);
        }

        {
            const auto result = client.Forget(opId).ExtractValueSync();
            const auto& issues = result.GetIssues();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNAUTHORIZED, issues.ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), error);
        }
    }

    Y_UNIT_TEST(TestSecureScriptExecutions) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableSecureScriptExecutions(true);

        auto kikimr = std::make_shared<TKikimrRunner>(NKqp::TKikimrSettings(appConfig)
            .SetEnableSecureScriptExecutions(true)
            .SetInitFederatedQuerySetupFactory(true));

        constexpr char aToken[] = "A@" BUILTIN_ACL_DOMAIN;
        auto userA = kikimr->GetQueryClient(TClientSettings().AuthToken(aToken));
        NOperation::TOperationClient clientA(kikimr->GetDriver(), TCommonClientSettings().AuthToken(aToken));

        // Create sample operation
        NOperationId::TOperationId opId;
        {
            const auto createdOp = userA.ExecuteScript("SELECT 42").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(createdOp.Status().GetStatus(), EStatus::SUCCESS, createdOp.Status().GetIssues().ToOneLineString());
            UNIT_ASSERT(!createdOp.Metadata().ExecutionId.empty());
            opId = createdOp.Id();

            const auto readyOp = WaitScriptExecutionOperation(opId, kikimr->GetDriver(), aToken);
            UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Status().GetStatus(), EStatus::SUCCESS, readyOp.Status().GetIssues().ToOneLineString());
            UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToOneLineString());

            auto results = userA.FetchScriptResults(opId, 0).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(results.GetStatus(), EStatus::SUCCESS, results.GetIssues().ToOneLineString());
            TResultSetParser resultSet(results.ExtractResultSet());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1);
            UNIT_ASSERT(resultSet.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetInt32(), 42);

            const auto listResult = clientA.List<TScriptExecutionOperation>(/* pageSize */ 10).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(listResult.GetStatus(), EStatus::SUCCESS, listResult.GetIssues().ToOneLineString());
            const auto& listedOps = listResult.GetList();
            UNIT_ASSERT_VALUES_EQUAL(listedOps.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(listedOps[0].Metadata().ExecStatus, EExecStatus::Completed);

            const auto cancelResult = clientA.Cancel(opId).ExtractValueSync();
            const auto& cancelIssues = cancelResult.GetIssues();
            UNIT_ASSERT_VALUES_EQUAL_C(cancelResult.GetStatus(), EStatus::PRECONDITION_FAILED, cancelIssues.ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(cancelIssues.ToString(), "Script execution operation is already finished");
        }

        // Test operations from client B
        CheckAccessDenied(kikimr, "B@" BUILTIN_ACL_DOMAIN, opId, "User is not owner of script execution operation");

        // Test operations from client without token
        CheckAccessDenied(kikimr, "", opId, "Access to script execution operations without user token is not allowed");

        // Forget operation from client A
        {
            const auto result = clientA.Forget(opId).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        }
    }

    Y_UNIT_TEST(TestCacheInvalidationOnExternalDataSourceChange) {
        constexpr char bucket[] = "testCacheInvalidationOnExternalDataSourceChange";
        CreateBucket(bucket);

        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableReplaceIfExistsForExternalEntities(true);

        auto kikimr = NTestUtils::MakeKikimrRunner(config);
        auto db = kikimr->GetQueryClient();

        constexpr char externalDataSourceName[] = "externalDataSource";
        {
            const auto result = db.ExecuteQuery(fmt::format(R"(
                CREATE EXTERNAL DATA SOURCE `{external_source}` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="{location}",
                    AUTH_METHOD="NONE"
                );)",
                "external_source"_a = externalDataSourceName,
                "location"_a = GetBucketLocation(bucket)
            ), TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        }

        const auto query = fmt::format(R"(
            INSERT INTO `{external_source}`.`path/` WITH (
                FORMAT = json_each_row
            ) (year, month) VALUES (2020, 1))",
            "external_source"_a = externalDataSourceName
        );

        {
            const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(bucket), "{\"month\":1,\"year\":2020}\n");
        }

        {
            const auto result = db.ExecuteQuery(fmt::format(R"(
                CREATE OR REPLACE EXTERNAL DATA SOURCE `{external_source}` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="{location}",
                    AUTH_METHOD="NONE"
                );)",
                "external_source"_a = externalDataSourceName,
                "location"_a = TStringBuilder() << GetEnv("S3_ENDPOINT") << "/unknown_bucket/"
            ), TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        }

        {
            const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "The specified bucket does not exist");
            UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(bucket), "{\"month\":1,\"year\":2020}\n");
        }
    }

    Y_UNIT_TEST(TestCacheInvalidationOnExternalTableChange) {
        constexpr char bucket[] = "testCacheInvalidationOnExternalTableChange";
        CreateBucket(bucket);

        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableReplaceIfExistsForExternalEntities(true);

        auto kikimr = NTestUtils::MakeKikimrRunner(config);
        auto db = kikimr->GetQueryClient();

        constexpr char externalDataSourceName[] = "externalDataSource";
        constexpr char externalTableName[] = "externalTable";
        {
            const auto result = db.ExecuteQuery(fmt::format(R"(
                CREATE EXTERNAL DATA SOURCE `{external_source}` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="{location}",
                    AUTH_METHOD="NONE"
                );
                CREATE EXTERNAL TABLE `{external_table}` (
                    year Int32 NOT NULL,
                    month Int32 NOT NULL
                ) WITH (
                    DATA_SOURCE="{external_source}",
                    LOCATION="path/",
                    FORMAT="json_each_row"
                );)",
                "external_source"_a = externalDataSourceName,
                "external_table"_a = externalTableName,
                "location"_a = GetBucketLocation(bucket)
            ), TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        }

        const auto query = fmt::format(
            "INSERT INTO `{external_table}` (year, month) VALUES (2020, 1)",
            "external_table"_a = externalTableName
        );

        {
            const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(bucket), "{\"month\":1,\"year\":2020}\n");
        }

        {
            const auto result = db.ExecuteQuery(fmt::format(R"(
                CREATE OR REPLACE EXTERNAL TABLE `{external_table}` (
                    yearx String NOT NULL,
                    monthx String NOT NULL
                ) WITH (
                    DATA_SOURCE="{external_source}",
                    LOCATION="path/",
                    FORMAT="json_each_row"
                );)",
                "external_source"_a = externalDataSourceName,
                "external_table"_a = externalTableName
            ), TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        }

        {
            const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Failed to convert input columns types to scheme types");
            UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(bucket), "{\"month\":1,\"year\":2020}\n");
        }
    }

    Y_UNIT_TEST(TestCacheInvalidationOnUnderlyingExternalDataSourceChange) {
        constexpr char bucket[] = "testCacheInvalidationOnUnderlyingExternalDataSourceChange";
        CreateBucket(bucket);

        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableReplaceIfExistsForExternalEntities(true);

        auto kikimr = NTestUtils::MakeKikimrRunner(config);
        auto db = kikimr->GetQueryClient();

        constexpr char externalDataSourceName[] = "externalDataSource";
        constexpr char externalTableName[] = "externalTable";
        {
            const auto result = db.ExecuteQuery(fmt::format(R"(
                CREATE EXTERNAL DATA SOURCE `{external_source}` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="{location}",
                    AUTH_METHOD="NONE"
                );
                CREATE EXTERNAL TABLE `{external_table}` (
                    year Int32 NOT NULL,
                    month Int32 NOT NULL
                ) WITH (
                    DATA_SOURCE="{external_source}",
                    LOCATION="path/",
                    FORMAT="json_each_row"
                );)",
                "external_source"_a = externalDataSourceName,
                "external_table"_a = externalTableName,
                "location"_a = GetBucketLocation(bucket)
            ), TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        }

        const auto query = fmt::format(
            "INSERT INTO `{external_table}` (year, month) VALUES (2020, 1)",
            "external_table"_a = externalTableName
        );

        {
            const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(bucket), "{\"month\":1,\"year\":2020}\n");
        }

        {
            const auto result = db.ExecuteQuery(fmt::format(R"(
                CREATE OR REPLACE EXTERNAL DATA SOURCE `{external_source}` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="{location}",
                    AUTH_METHOD="NONE"
                );)",
                "external_source"_a = externalDataSourceName,
                "location"_a = TStringBuilder() << GetEnv("S3_ENDPOINT") << "/unknown_bucket/"
            ), TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        }

        {
            const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "The specified bucket does not exist");
            UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(bucket), "{\"month\":1,\"year\":2020}\n");
        }
    }

    Y_UNIT_TEST(ExecuteQueryWithReplicatedS3Write) {
        const TString externalDataSourceName = "/Root/external_data_source";
        const TString externalTableName = "/Root/test_binding_resolve";
        const TString bucket = "test_bucket_s3_replicate";
        const TString object = "object/test_object";

        CreateBucketWithObject(bucket, object, "test-data");

        auto kikimr = NTestUtils::MakeKikimrRunner();
        auto db = kikimr->GetQueryClient();

        {
            const auto result = db.ExecuteQuery(fmt::format(R"(
                CREATE EXTERNAL DATA SOURCE `{external_source}` WITH (
                    SOURCE_TYPE = "ObjectStorage",
                    LOCATION = "{location}",
                    AUTH_METHOD = "NONE"
                );
                CREATE EXTERNAL TABLE `{external_table}` (
                    Data String NOT NULL
                ) WITH (
                    DATA_SOURCE = "{external_source}",
                    LOCATION = "{object}",
                    FORMAT = "raw"
                );)",
                "external_source"_a = externalDataSourceName,
                "external_table"_a = externalTableName,
                "location"_a = GetBucketLocation(bucket),
                "object"_a = EscapeC(object)
            ), TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        }

        {
            const auto result = db.ExecuteQuery(fmt::format(R"(
                INSERT INTO `{external_source}`.`test-1/` WITH (FORMAT = "csv_with_names")
                SELECT * FROM `{external_table}`;

                INSERT INTO `{external_source}`.`test-2/` WITH (FORMAT = "csv_with_names")
                SELECT * FROM `{external_table}`;)",
                "external_source"_a = externalDataSourceName,
                "external_table"_a = externalTableName
            ), TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        }

        UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(bucket), "test-data\"Data\"\n\"test-data\"\n\"Data\"\n\"test-data\"\n");
    }
}

} // namespace NKikimr::NKqp
