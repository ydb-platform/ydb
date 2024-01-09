#include "ch_recipe_ut_helpers.h"
#include "connector_recipe_ut_helpers.h"
#include "pg_recipe_ut_helpers.h"

#include <ydb/library/yql/providers/generic/connector/libcpp/client.h>

#include <library/cpp/testing/unittest/registar.h>

#include <fmt/format.h>

using namespace NTestUtils;
using namespace fmt::literals;

Y_UNIT_TEST_SUITE(FederatedQueryJoin) {
    Y_UNIT_TEST(InnerJoinChPg) {
        pqxx::connection pgConnection = CreatePostgresqlConnection();
        NClickHouse::TClient chClient = CreateClickhouseClient();

        // pg_table_inner_join_test
        {
            pqxx::work work{pgConnection};
            const TString sql = R"sql(
                CREATE TABLE pg_table_inner_join_test (
                    key INT PRIMARY KEY,
                    name TEXT
                )
            )sql";
            work.exec(sql);

            const TString insertData = R"sql(
                INSERT INTO pg_table_inner_join_test
                    (key, name)
                VALUES
                    (1,    'A'),
                    (2,    'B'),
                    (1000, 'C');
            )sql";
            work.exec(insertData);

            work.commit();
        }

        // ch_table_inner_join_test
        {
            const TString sql = R"sql(
                CREATE TABLE ch_table_inner_join_test (
                    key INT PRIMARY KEY,
                    name TEXT
                )
                ENGINE = MergeTree
            )sql";
            chClient.Execute(sql);

            const TString insertData = R"sql(
                INSERT INTO ch_table_inner_join_test
                    (key, name)
                VALUES
                    (1,    'X'),
                    (3,    'Y'),
                    (1000, 'Z');
            )sql";
            chClient.Execute(insertData);
        }

        std::shared_ptr<NKikimr::NKqp::TKikimrRunner> kikimr = MakeKikimrRunnerWithConnector();
        auto queryClient = kikimr->GetQueryClient();

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
                "pg_host"_a = GetPgHost(),
                "pg_port"_a = GetPgPort(),
                "pg_user"_a = GetPgUser(),
                "pg_password"_a = GetPgPassword(),
                "pg_database"_a = GetPgDatabase(),
                "ch_host"_a = GetChHost(),
                "ch_port"_a = GetChPort(),
                "ch_database"_a = GetChDatabase(),
                "ch_user"_a = GetChUser(),
                "ch_password"_a = GetChPassword());
            auto result = queryClient.ExecuteQuery(sql, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // join
        const TString sql = R"sql(
            SELECT pg.* FROM ch_data_source.ch_table_inner_join_test AS ch
            INNER JOIN pg_data_source.pg_table_inner_join_test AS pg
            ON ch.key = pg.key
            WHERE ch.key > 998
        )sql";

        auto result = queryClient.ExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        // results
        auto resultSet = result.GetResultSetParser(0);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1);
        UNIT_ASSERT(resultSet.TryNextRow());

        const TMaybe<i32> key = resultSet.ColumnParser("key").GetOptionalInt32();
        UNIT_ASSERT(key);
        UNIT_ASSERT_VALUES_EQUAL(*key, 1000);

        const TMaybe<TString> name = resultSet.ColumnParser("name").GetOptionalUtf8();
        UNIT_ASSERT(name);
        UNIT_ASSERT_VALUES_EQUAL(name, "C");
    }
}
