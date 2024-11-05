#include "s3_recipe_ut_helpers.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/sdk/cpp/client/ydb_types/operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_query/client.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

#include <fmt/format.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NKikimr::NKqp::NFederatedQueryTest;
using namespace NTestUtils;
using namespace fmt::literals;

Y_UNIT_TEST_SUITE(KqpS3PlanTest) {
    Y_UNIT_TEST(S3Source) {
        CreateBucketWithObject("test_bucket_plan_s3_source", "test_object_plan_s3_source", TEST_CONTENT);

        auto kikimr = NTestUtils::MakeKikimrRunner();

        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        const TString query = fmt::format(R"sql(
            CREATE EXTERNAL DATA SOURCE external_data_source WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{location}",
                AUTH_METHOD="NONE"
            );
            CREATE EXTERNAL TABLE external_table (
                key Utf8 NOT NULL,
                value Utf8 NOT NULL
            ) WITH (
                DATA_SOURCE="external_data_source",
                LOCATION="test_object_plan_s3_source",
                FORMAT="json_each_row"
            );)sql",
            "location"_a = GetBucketLocation("test_bucket_plan_s3_source")
            );
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        const TString sql = R"sql(SELECT * FROM external_table)sql";

        auto queryClient = kikimr->GetQueryClient();
        TExecuteQueryResult queryResult = queryClient.ExecuteQuery(
            sql,
            TTxControl::BeginTx().CommitTx(),
            TExecuteQuerySettings().ExecMode(EExecMode::Explain)).GetValueSync();

        UNIT_ASSERT_C(queryResult.IsSuccess(), queryResult.GetIssues().ToString());
        UNIT_ASSERT(queryResult.GetStats());
        UNIT_ASSERT(queryResult.GetStats()->GetPlan());
        Cerr << "Plan: " << *queryResult.GetStats()->GetPlan() << Endl;
        NJson::TJsonValue plan;
        UNIT_ASSERT(NJson::ReadJsonTree(*queryResult.GetStats()->GetPlan(), &plan));

        const auto& stagePlan = plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0];
        UNIT_ASSERT_VALUES_EQUAL(stagePlan["Node Type"].GetStringSafe(), "Stage");
        const auto& sourceOp = stagePlan["Plans"][0]["Operators"].GetArraySafe()[0];
        UNIT_ASSERT_VALUES_EQUAL(sourceOp["ExternalDataSource"].GetStringSafe(), "external_data_source");
        UNIT_ASSERT_VALUES_EQUAL(sourceOp["Format"].GetStringSafe(), "json_each_row");
        UNIT_ASSERT_VALUES_EQUAL(sourceOp["Name"].GetStringSafe(), "external_table");
        UNIT_ASSERT_VALUES_EQUAL(sourceOp["SourceType"].GetStringSafe(), "s3");
        UNIT_ASSERT(!IsIn(sourceOp.GetMap(), "RowsLimitHint"));
        UNIT_ASSERT_VALUES_EQUAL(sourceOp["ReadColumns"].GetArraySafe()[0].GetStringSafe(), "key");
        UNIT_ASSERT_VALUES_EQUAL(sourceOp["ReadColumns"].GetArraySafe()[1].GetStringSafe(), "value");
    }

    Y_UNIT_TEST(S3Sink) {
        {
            Aws::S3::S3Client s3Client = MakeS3Client();
            CreateBucketWithObject("test_bucket_read", "test_object_read", TEST_CONTENT, s3Client);
            CreateBucket("test_bucket_write", s3Client);
        }

        auto kikimr = NTestUtils::MakeKikimrRunner();

        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        const TString query = fmt::format(R"sql(
            CREATE EXTERNAL DATA SOURCE read_data_source WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{read_location}",
                AUTH_METHOD="NONE"
            );
            CREATE EXTERNAL TABLE read_table (
                key Utf8 NOT NULL,
                value Utf8 NOT NULL
            ) WITH (
                DATA_SOURCE="read_data_source",
                LOCATION="test_object_read",
                FORMAT="json_each_row"
            );

            CREATE EXTERNAL DATA SOURCE write_data_source WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{write_location}",
                AUTH_METHOD="NONE"
            );
            CREATE EXTERNAL TABLE write_table (
                key Utf8 NOT NULL,
                value Utf8 NOT NULL
            ) WITH (
                DATA_SOURCE="write_data_source",
                LOCATION="test_object_write/",
                FORMAT="json_each_row",
                COMPRESSION="gzip"
            );
            )sql",
            "read_location"_a = GetBucketLocation("test_bucket_read"),
            "write_location"_a = GetBucketLocation("test_bucket_write")
            );
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        const TString sql = R"sql(
                INSERT INTO write_table
                SELECT * FROM read_table
                LIMIT 10
            )sql";

        auto queryClient = kikimr->GetQueryClient();
        TExecuteQueryResult queryResult = queryClient.ExecuteQuery(
            sql,
            TTxControl::BeginTx().CommitTx(),
            TExecuteQuerySettings().ExecMode(EExecMode::Explain)).GetValueSync();

        UNIT_ASSERT_C(queryResult.IsSuccess(), queryResult.GetIssues().ToString());
        UNIT_ASSERT(queryResult.GetStats());
        UNIT_ASSERT(queryResult.GetStats()->GetPlan());
        Cerr << "Plan: " << *queryResult.GetStats()->GetPlan() << Endl;
        NJson::TJsonValue plan;
        UNIT_ASSERT(NJson::ReadJsonTree(*queryResult.GetStats()->GetPlan(), &plan));

        const auto& writeStagePlan = plan["Plan"]["Plans"][0]["Plans"][0];
        UNIT_ASSERT_VALUES_EQUAL(writeStagePlan["Node Type"].GetStringSafe(), "Limit-Sink");
        UNIT_ASSERT(writeStagePlan["Operators"].GetArraySafe().size() >= 2);
        const auto& sinkOp = writeStagePlan["Operators"].GetArraySafe()[1];
        UNIT_ASSERT_VALUES_EQUAL(sinkOp["ExternalDataSource"].GetStringSafe(), "write_data_source");
        UNIT_ASSERT_VALUES_EQUAL(sinkOp["Compression"].GetStringSafe(), "gzip");

        const auto& readStagePlan = plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0];
        UNIT_ASSERT_VALUES_EQUAL(readStagePlan["Node Type"].GetStringSafe(), "Source");
        const auto& sourceOp = readStagePlan["Operators"].GetArraySafe()[0];
        UNIT_ASSERT_VALUES_EQUAL(sourceOp["ExternalDataSource"].GetStringSafe(), "read_data_source");

        UNIT_ASSERT_VALUES_EQUAL(sourceOp["RowsLimitHint"].GetStringSafe(), "10");
        UNIT_ASSERT_VALUES_EQUAL(sourceOp["ReadColumns"].GetArraySafe()[0].GetStringSafe(), "key");
        UNIT_ASSERT_VALUES_EQUAL(sourceOp["ReadColumns"].GetArraySafe()[1].GetStringSafe(), "value");
    }
}

} // namespace NKikimr::NKqp
