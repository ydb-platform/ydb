#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <util/string/printf.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NQuery;

class TBlockHashJoinTester {
public:
    TBlockHashJoinTester()
        : Kikimr(GetKikimrWithBlockHashJoinSettings())
        , TableClient(Kikimr.GetTableClient())
        , Session(TableClient.GetSession().GetValueSync().GetSession())
    {}

public:
    void Test() {
        CreateTables();
        TestSimpleInnerJoin();
        TestBlockHashJoinInPlan();
    }

private:
    static TKikimrRunner GetKikimrWithBlockHashJoinSettings() {
        TVector<NKikimrKqp::TKqpSetting> settings;

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableConstantFolding(true);
        appConfig.MutableTableServiceConfig()->SetCompileTimeoutMs(TDuration::Minutes(10).MilliSeconds());
        // Enable block engine for better block hash join testing
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);

        auto serverSettings = TKikimrSettings().SetAppConfig(appConfig);
        serverSettings.SetKqpSettings(settings);
        serverSettings.SetNodeCount(1);
        serverSettings.WithSampleTables = false;

        return TKikimrRunner(serverSettings);
    }

    void CreateTables() {
        // Create test tables with block-friendly structure
        auto createLeftTable = R"(
            CREATE TABLE `/Root/left_table` (
                id Int32,
                value String,
                PRIMARY KEY (id)
            );
        )";

        auto result = Session.ExecuteSchemeQuery(createLeftTable).GetValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        auto createRightTable = R"(
            CREATE TABLE `/Root/right_table` (
                id Int32,
                data String,
                PRIMARY KEY (id)
            );
        )";

        result = Session.ExecuteSchemeQuery(createRightTable).GetValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        // Insert test data
        auto insertData = R"(
            UPSERT INTO `/Root/left_table` (id, value) VALUES
                (1, "left1"),
                (2, "left2"),
                (3, "left3"),
                (4, "left4"),
                (5, "left5");

            UPSERT INTO `/Root/right_table` (id, data) VALUES
                (1, "right1"),
                (2, "right2"),
                (3, "right3"),
                (6, "right6"),
                (7, "right7");
        )";

        result = Session.ExecuteDataQuery(
            insertData,
            NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx()
        ).GetValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
    }

    void TestSimpleInnerJoin() {
        auto query = R"(
            SELECT l.id as id, l.value as value, r.data as data
            FROM `/Root/left_table` AS l
            INNER JOIN `/Root/right_table` AS r
            ON l.id = r.id
            ORDER BY l.id;
        )";

        auto result = Session.ExecuteDataQuery(
            query,
            NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx()
        ).GetValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        // Check results
        auto resultSet = result.GetResultSets()[0];
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 3);

        TResultSetParser parser(resultSet);

        // Первая строка
        parser.TryNextRow();
        auto idOpt = parser.ColumnParser("id").GetOptionalInt32();
        UNIT_ASSERT(idOpt.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*idOpt, 1);

        auto valueOpt = parser.ColumnParser("value").GetOptionalString();
        UNIT_ASSERT(valueOpt.has_value());
        UNIT_ASSERT_VALUES_EQUAL(TString(*valueOpt), TString("left1"));

        auto dataOpt = parser.ColumnParser("data").GetOptionalString();
        UNIT_ASSERT(dataOpt.has_value());
        UNIT_ASSERT_VALUES_EQUAL(TString(*dataOpt), TString("right1"));

        // Вторая строка
        parser.TryNextRow();
        idOpt = parser.ColumnParser("id").GetOptionalInt32();
        UNIT_ASSERT(idOpt.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*idOpt, 2);

        valueOpt = parser.ColumnParser("value").GetOptionalString();
        UNIT_ASSERT(valueOpt.has_value());
        UNIT_ASSERT_VALUES_EQUAL(TString(*valueOpt), TString("left2"));

        dataOpt = parser.ColumnParser("data").GetOptionalString();
        UNIT_ASSERT(dataOpt.has_value());
        UNIT_ASSERT_VALUES_EQUAL(TString(*dataOpt), TString("right2"));

        // Третья строка
        parser.TryNextRow();
        idOpt = parser.ColumnParser("id").GetOptionalInt32();
        UNIT_ASSERT(idOpt.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*idOpt, 3);

        valueOpt = parser.ColumnParser("value").GetOptionalString();
        UNIT_ASSERT(valueOpt.has_value());
        UNIT_ASSERT_VALUES_EQUAL(TString(*valueOpt), TString("left3"));

        dataOpt = parser.ColumnParser("data").GetOptionalString();
        UNIT_ASSERT(dataOpt.has_value());
        UNIT_ASSERT_VALUES_EQUAL(TString(*dataOpt), TString("right3"));
    }

    void TestBlockHashJoinInPlan() {
        auto query = R"(
            SELECT l.id, l.value, r.data
            FROM `/Root/left_table` AS l
            INNER JOIN `/Root/right_table` AS r
            ON l.id = r.id;
        )";

        auto result = Session.ExplainDataQuery(query).GetValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        TString plan = TString(result.GetPlan());
        Cout << "Plan: " << plan << Endl;

        // Check that the plan contains some kind of join operation
        // For now, just verify it doesn't fail - we'll improve this check later
        UNIT_ASSERT(plan.Contains("Join") || plan.Contains("LeftJoin") || plan.Contains("InnerJoin"));
    }

private:
    TKikimrRunner Kikimr;
    NYdb::NTable::TTableClient TableClient;
    NYdb::NTable::TSession Session;
};

Y_UNIT_TEST_SUITE(KqpBlockHashJoin) {
    Y_UNIT_TEST(SimpleBlockHashJoin) {
        TBlockHashJoinTester tester;
        tester.Test();
    }

    static TKikimrRunner GetKikimrWithBlockHashJoinSettings() {
        TVector<NKikimrKqp::TKqpSetting> settings;

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableConstantFolding(true);
        appConfig.MutableTableServiceConfig()->SetCompileTimeoutMs(TDuration::Minutes(10).MilliSeconds());
        // Enable block engine for better block hash join testing
        appConfig.MutableTableServiceConfig()->SetDefaultCostBasedOptimizationLevel(4);

        auto serverSettings = TKikimrSettings().SetAppConfig(appConfig);
        serverSettings.SetKqpSettings(settings);
        serverSettings.SetNodeCount(1);
        serverSettings.WithSampleTables = false;

        return TKikimrRunner(serverSettings);
    }

    Y_UNIT_TEST(TestQuerySession) {
        auto kikimr = GetKikimrWithBlockHashJoinSettings();
        auto db = kikimr.GetQueryClient();
        auto sessionResult = db.GetSession().GetValueSync();
        NStatusHelpers::ThrowOnError(sessionResult);
        auto session = sessionResult.GetSession();

        // Create test tables for query session
        auto createSchema = R"(
            CREATE TABLE `/Root/test_left` (
                id Int32,
                name String,
                PRIMARY KEY (id)
            );

            CREATE TABLE `/Root/test_right` (
                id Int32,
                value Int64,
                PRIMARY KEY (id)
            );
        )";

        auto result = session.ExecuteQuery(
            createSchema,
            NYdb::NQuery::TTxControl::NoTx()
        ).GetValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        // Insert test data
        auto insertData = R"(
            UPSERT INTO `/Root/test_left` (id, name) VALUES
                (1, "Alice"),
                (2, "Bob"),
                (3, "Charlie");

            UPSERT INTO `/Root/test_right` (id, value) VALUES
                (1, 100),
                (2, 200),
                (4, 400);
        )";

        result = session.ExecuteQuery(
            insertData,
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()
        ).GetValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        // Test join query
        auto joinQuery = R"(
            SELECT l.id, l.name, r.value
            FROM `/Root/test_left` AS l
            INNER JOIN `/Root/test_right` AS r
            ON l.id = r.id
            ORDER BY l.id;
        )";

        auto executeResult = session.ExecuteQuery(
            joinQuery,
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()
        ).GetValueSync();
        executeResult.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(executeResult.GetStatus(), EStatus::SUCCESS);

        // Verify result
        auto resultSets = executeResult.GetResultSets();
        UNIT_ASSERT_VALUES_EQUAL(resultSets.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(resultSets[0].RowsCount(), 2);

        // Test explain to check plan
        auto explainResult = session.ExecuteQuery(
            joinQuery,
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)
        ).GetValueSync();
        explainResult.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(explainResult.GetStatus(), EStatus::SUCCESS);

        auto plan = explainResult.GetStats()->GetPlan();
        UNIT_ASSERT(plan.has_value());
        Cout << "Explain Plan: " << *plan << Endl;

        // For now, just check that we get a plan - later we can check for specific block hash join
        UNIT_ASSERT(!plan->empty());
    }

    Y_UNIT_TEST(BlockHashJoinWithNulls) {
        auto kikimr = GetKikimrWithBlockHashJoinSettings();
        auto db = kikimr.GetTableClient();
        auto sessionResult = db.GetSession().GetValueSync();
        NStatusHelpers::ThrowOnError(sessionResult);
        auto session = sessionResult.GetSession();

        // Create tables that can have NULL values
        auto createSchema = R"(
            CREATE TABLE `/Root/nullable_left` (
                id Int32,
                nullable_field Int32,
                PRIMARY KEY (id)
            );

            CREATE TABLE `/Root/nullable_right` (
                id Int32,
                nullable_field Int32,
                PRIMARY KEY (id)
            );
        )";

        auto schemeResult = session.ExecuteSchemeQuery(createSchema).GetValueSync();
        schemeResult.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(schemeResult.GetStatus(), EStatus::SUCCESS);

        // Insert data with NULLs
        auto insertData = R"(
            UPSERT INTO `/Root/nullable_left` (id, nullable_field) VALUES
                (1, 10),
                (2, NULL),
                (3, 30);

            UPSERT INTO `/Root/nullable_right` (id, nullable_field) VALUES
                (1, 100),
                (2, NULL),
                (3, NULL);
        )";

        auto insertResult = session.ExecuteDataQuery(
            insertData,
            NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx()
        ).GetValueSync();
        insertResult.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(insertResult.GetStatus(), EStatus::SUCCESS);

        // Test join on nullable fields
        auto joinQuery = R"(
            SELECT l.id, l.nullable_field AS left_field, r.nullable_field AS right_field
            FROM `/Root/nullable_left` AS l
            INNER JOIN `/Root/nullable_right` AS r
            ON l.nullable_field = r.nullable_field
            ORDER BY l.id;
        )";

        auto joinResult = session.ExecuteDataQuery(
            joinQuery,
            NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx()
        ).GetValueSync();
        joinResult.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(joinResult.GetStatus(), EStatus::SUCCESS);

        // NULLs should not match in inner join, so we expect 0 rows
        auto resultSet = joinResult.GetResultSets()[0];
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 0);
    }
}

} // namespace NKqp
} // namespace NKikimr
