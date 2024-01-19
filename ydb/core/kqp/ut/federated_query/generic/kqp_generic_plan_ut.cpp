#include "ch_recipe_ut_helpers.h"
#include "connector_recipe_ut_helpers.h"
#include "pg_recipe_ut_helpers.h"
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/federated_query/common/common.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>

#include <ydb/library/yql/providers/generic/connector/libcpp/client.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

#include <fmt/format.h>

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NTestUtils;
using namespace fmt::literals;

Y_UNIT_TEST_SUITE(KqpGenericPlanTest) {
    Y_UNIT_TEST(PgSource) {
        pqxx::connection pgConnection = CreatePostgresqlConnection();

        {
            pqxx::work work{pgConnection};
            const TString sql = R"sql(
                CREATE TABLE pg_table_plan_test (
                    key INT4 PRIMARY KEY,
                    name TEXT,
                    value INT4
                )
            )sql";
            work.exec(sql);
            work.commit();
        }

        std::shared_ptr<NKikimr::NKqp::TKikimrRunner> kikimr = MakeKikimrRunnerWithConnector();

        auto tableCLient = kikimr->GetTableClient();
        auto session = tableCLient.CreateSession().GetValueSync().GetSession();

        // external tables to pg/ch
        {
            const TString sql = fmt::format(
                R"sql(
                CREATE OBJECT pg_password_obj (TYPE SECRET) WITH (value="{pg_password}");
                CREATE EXTERNAL DATA SOURCE pg_data_source WITH (
                    SOURCE_TYPE="PostgreSQL",
                    LOCATION="{pg_host}:{pg_port}",
                    DATABASE_NAME="{pg_database}",
                    USE_TLS="FALSE",
                    AUTH_METHOD="BASIC",
                    PROTOCOL="NATIVE",
                    LOGIN="{pg_user}",
                    PASSWORD_SECRET_NAME="pg_password_obj"
                );
                )sql",
                "pg_host"_a = GetPgHost(),
                "pg_port"_a = GetPgPort(),
                "pg_user"_a = GetPgUser(),
                "pg_password"_a = GetPgPassword(),
                "pg_database"_a = GetPgDatabase());
            auto result = session.ExecuteSchemeQuery(sql).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        const TString sql = R"sql(
            PRAGMA generic.UsePredicatePushdown="true";
            SELECT * FROM pg_data_source.pg_table_plan_test
            WHERE key > 42 AND value <> 0
        )sql";

        auto queryClient = kikimr->GetQueryClient();
        TExecuteQueryResult queryResult = queryClient.ExecuteQuery(
                                                         sql,
                                                         TTxControl::BeginTx().CommitTx(),
                                                         TExecuteQuerySettings().ExecMode(EExecMode::Explain))
                                              .GetValueSync();

        UNIT_ASSERT_C(queryResult.IsSuccess(), queryResult.GetIssues().ToString());
        UNIT_ASSERT(queryResult.GetStats());
        UNIT_ASSERT(queryResult.GetStats()->GetPlan());
        Cerr << "Plan: " << *queryResult.GetStats()->GetPlan() << Endl;
        NJson::TJsonValue plan;
        UNIT_ASSERT(NJson::ReadJsonTree(*queryResult.GetStats()->GetPlan(), &plan));

        const auto& stagePlan = plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0];
        UNIT_ASSERT_VALUES_EQUAL(stagePlan["Node Type"].GetStringSafe(), "Source");
        const auto& sourceOp = stagePlan["Operators"].GetArraySafe()[0];
        UNIT_ASSERT_VALUES_EQUAL(sourceOp["ExternalDataSource"].GetStringSafe(), "pg_data_source");
        UNIT_ASSERT_VALUES_EQUAL(sourceOp["Database"].GetStringSafe(), GetPgDatabase());
        UNIT_ASSERT_VALUES_EQUAL(sourceOp["Protocol"].GetStringSafe(), "Native");
        UNIT_ASSERT_VALUES_EQUAL(sourceOp["Table"].GetStringSafe(), "pg_table_plan_test");
        UNIT_ASSERT_VALUES_EQUAL(sourceOp["Name"].GetStringSafe(), "Read pg_data_source");
        UNIT_ASSERT_VALUES_EQUAL(sourceOp["SourceType"].GetStringSafe(), "PostgreSql");
        UNIT_ASSERT_VALUES_EQUAL(sourceOp["ReadColumns"].GetArraySafe()[0].GetStringSafe(), "key");
        UNIT_ASSERT_VALUES_EQUAL(sourceOp["ReadColumns"].GetArraySafe()[1].GetStringSafe(), "name");
        UNIT_ASSERT_VALUES_EQUAL(sourceOp["ReadColumns"].GetArraySafe()[2].GetStringSafe(), "value");
        UNIT_ASSERT_VALUES_EQUAL(sourceOp["Filter"].GetStringSafe(), "item.key > 42 And item.value != 0");
    }

    Y_UNIT_TEST(ChSource) {
        NClickHouse::TClient chClient = CreateClickhouseClient();

        // ch_table_plan_test
        {
            const TString sql = R"sql(
                CREATE TABLE ch_table_plan_test (
                    key INT PRIMARY KEY,
                    name TEXT NULL
                )
                ENGINE = MergeTree
            )sql";
            chClient.Execute(sql);
        }

        std::shared_ptr<NKikimr::NKqp::TKikimrRunner> kikimr = MakeKikimrRunnerWithConnector();

        auto tableCLient = kikimr->GetTableClient();
        auto session = tableCLient.CreateSession().GetValueSync().GetSession();

        // external tables to pg/ch
        {
            const TString sql = fmt::format(
                R"sql(
                CREATE OBJECT ch_password_obj (TYPE SECRET) WITH (value="{ch_password}");
                CREATE EXTERNAL DATA SOURCE ch_data_source WITH (
                    SOURCE_TYPE="ClickHouse",
                    LOCATION="{ch_host}:{ch_port}",
                    DATABASE_NAME="{ch_database}",
                    AUTH_METHOD="BASIC",
                    PROTOCOL="NATIVE",
                    LOGIN="{ch_user}",
                    PASSWORD_SECRET_NAME="ch_password_obj"
                );
                )sql",
                "ch_host"_a = GetChHost(),
                "ch_port"_a = GetChPort(),
                "ch_database"_a = GetChDatabase(),
                "ch_user"_a = GetChUser(),
                "ch_password"_a = GetChPassword());
            auto result = session.ExecuteSchemeQuery(sql).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        const TString sql = R"sql(
            PRAGMA generic.UsePredicatePushdown="true";
            SELECT * FROM ch_data_source.ch_table_plan_test
            WHERE name IS NOT NULL
        )sql";

        auto queryClient = kikimr->GetQueryClient();
        TExecuteQueryResult queryResult = queryClient.ExecuteQuery(
                                                         sql,
                                                         TTxControl::BeginTx().CommitTx(),
                                                         TExecuteQuerySettings().ExecMode(EExecMode::Explain))
                                              .GetValueSync();

        UNIT_ASSERT_C(queryResult.IsSuccess(), queryResult.GetIssues().ToString());
        UNIT_ASSERT(queryResult.GetStats());
        UNIT_ASSERT(queryResult.GetStats()->GetPlan());
        Cerr << "Plan: " << *queryResult.GetStats()->GetPlan() << Endl;
        NJson::TJsonValue plan;
        UNIT_ASSERT(NJson::ReadJsonTree(*queryResult.GetStats()->GetPlan(), &plan));

        const auto& stagePlan = plan["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0]["Plans"][0];
        UNIT_ASSERT_VALUES_EQUAL(stagePlan["Node Type"].GetStringSafe(), "Source");
        const auto& sourceOp = stagePlan["Operators"].GetArraySafe()[0];
        UNIT_ASSERT_VALUES_EQUAL(sourceOp["ExternalDataSource"].GetStringSafe(), "ch_data_source");
        UNIT_ASSERT_VALUES_EQUAL(sourceOp["Database"].GetStringSafe(), GetChDatabase());
        UNIT_ASSERT_VALUES_EQUAL(sourceOp["Protocol"].GetStringSafe(), "Native");
        UNIT_ASSERT_VALUES_EQUAL(sourceOp["Table"].GetStringSafe(), "ch_table_plan_test");
        UNIT_ASSERT_VALUES_EQUAL(sourceOp["Name"].GetStringSafe(), "Read ch_data_source");
        UNIT_ASSERT_VALUES_EQUAL(sourceOp["SourceType"].GetStringSafe(), "ClickHouse");
        UNIT_ASSERT_VALUES_EQUAL(sourceOp["ReadColumns"].GetArraySafe()[0].GetStringSafe(), "key");
        UNIT_ASSERT_VALUES_EQUAL(sourceOp["ReadColumns"].GetArraySafe()[1].GetStringSafe(), "name");
        UNIT_ASSERT_VALUES_EQUAL(sourceOp["Filter"].GetStringSafe(), "Exist(item.name)");
    }
}
