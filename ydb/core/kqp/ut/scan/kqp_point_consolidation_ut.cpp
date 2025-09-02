#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/query.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

using ResultSetRow = std::tuple<ui32, std::optional<ui64>>;
using ResultSetDoubleRow = std::tuple<ui32, std::optional<std::string>, std::optional<ui64>>;

NKikimrConfig::TAppConfig GetAppConfig(bool pointConsolidation = false) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableParallelPointReadConsolidation(pointConsolidation);
    return appConfig;
}

class TTaskCountExtractor : public NJson::IScanCallback {
public:
    THashMap<int, int> TasksCountPerStage;

    bool Do(const TString& path, NJson::TJsonValue* parent, NJson::TJsonValue& value) {
        Y_UNUSED(path, parent);

        if (value.IsMap() && value.Has("Tasks")) {
            int taskCount = value["Tasks"].GetIntegerSafe();
            UNIT_ASSERT(value.Has("PhysicalStageId"));
            int stageId = value["PhysicalStageId"].GetIntegerSafe();
            auto [_, success] = TasksCountPerStage.emplace(stageId, taskCount);
            UNIT_ASSERT_C(success, TStringBuilder() << "duplicatedStage " << stageId);
        }

        return true;
    }

};

void CreateSampleTables(TSession& session) {
    {
        const auto query = Q_(R"(
            CREATE TABLE `TestSharded` (
                Id Uint32 NOT NULL,
                Value Uint64,
                PRIMARY KEY(Id)
            ) WITH (
                UNIFORM_PARTITIONS = 16
            );

            CREATE TABLE `TestShardedDoublePrimary` (
                Id1 Uint32 NOT NULL,
                Id2 String,
                Value Uint64,
                PRIMARY KEY(Id1, Id2)
            ) WITH (
                PARTITION_AT_KEYS = ((1, "a"), (5, "b"), (7, "c"), (10, "d"))
            );
        )");

        const auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    {
        const auto query = Q_(R"(
            UPSERT INTO `TestSharded` (Id, Value) VALUES
                (0, 0), (70640909, 4), (141281818, 3), (211922728, 3),
                (268435455, 2), (353204547, 2), (423845456, 1), (494486365, 0),
                (536870911, 0), (635768184, 4), (706409094, 4), (777050003, 3),
                (805306367, 3), (918331822, 2), (988972731, 1), (1073741823, 1),
                (1130254550, 0), (1200895460, 0), (1271536369, 4), (1342177279, 4);

            UPSERT INTO `TestShardedDoublePrimary` (Id1, Id2, Value) VALUES
                (0, "a", 0), (0, "aaa", 1), (0, "bc", 2), (0, "cde", 3),
                (0, "fg", 4), (1, "a", 0), (1, "aaa", 1), (1, "bc", 2),
                (1, "cde", 3), (1, "fg", 4), (2, "a", 0), (2, "aaa", 1),
                (2, "bc", 2), (2, "cde", 3), (2, "fg", 4), (3, "a", 0),
                (3, "aaa", 1), (3, "bc", 2), (3, "cde", 3), (3, "fg", 4),
                (5, "a", 0), (5, "aaa", 1), (5, "bc", 2), (5, "cde", 3),
                (5, "fg", 4), (6, "a", 0), (6, "aaa", 1), (6, "bc", 2),
                (6, "cde", 3), (6, "fg", 4), (7, "a", 0), (7, "aaa", 1),
                (7, "bc", 2), (7, "cde", 3), (7, "fg", 4), (9, "a", 0),
                (9, "aaa", 1), (9, "bc", 2), (9, "cde", 3), (9, "fg", 4),
                (10, "a", 0), (10, "aaa", 1), (10, "bc", 2), (10, "cde", 3),
                (10, "fg", 4), (11, "a", 0), (11, "aaa", 1), (11, "bc", 2),
                (11, "cde", 3), (11, "fg", 4);
        )");

        const auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
}

std::vector<ResultSetRow> ResultSetToSortedVector(const TExecuteQueryResult& result) {
    std::vector<ResultSetRow> resultSet;
    size_t size = result.GetResultSets().size();

    for (size_t i = 0; i < size; ++i) {
        TResultSetParser parser(result.GetResultSet(i));
        while (parser.TryNextRow()) {
            resultSet.emplace_back(parser.ColumnParser(0).GetUint32(), parser.ColumnParser(1).GetOptionalUint64());
        }
    }

    std::sort(resultSet.begin(), resultSet.end());
    return resultSet;
}

std::vector<ResultSetDoubleRow> ResultSetDoubleToSortedVector(const TExecuteQueryResult& result) {
    std::vector<ResultSetDoubleRow> resultSet;
    size_t size = result.GetResultSets().size();

    for (size_t i = 0; i < size; ++i) {
        TResultSetParser parser(result.GetResultSet(i));
        while (parser.TryNextRow()) {
            resultSet.emplace_back(parser.ColumnParser(0).GetUint32(), parser.ColumnParser(1).GetOptionalString(), parser.ColumnParser(2).GetOptionalUint64());
        }
    }

    std::sort(resultSet.begin(), resultSet.end());
    return resultSet;
}

} // namespace

Y_UNIT_TEST_SUITE(KqpPointConsolidation) {
    Y_UNIT_TEST_TWIN(TasksCount, PointConsolidation) {
        TKikimrRunner kikimr(GetAppConfig(PointConsolidation));
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        {
            /*
                There are only 2 points in the range:
                [0;0], [268435455;268435455]

                With EnableParallelPointReadConsolidation the query will use 1 task for reading, without it 2 tasks.
            */

            const auto query = Q_(R"(
                SELECT * FROM `TestSharded`
                WHERE Id IN (0, 268435455);
            )");

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
                TExecuteQuerySettings{}.StatsMode(EStatsMode::Full)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets()[0].RowsCount(), 2);

            NJson::TJsonValue plan;

            Cerr << result.GetStats()->GetPlan() << Endl;
            Cerr << result.GetStats()->GetAst() << Endl;

            UNIT_ASSERT(NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan));
            auto statsExtractor = TTaskCountExtractor();
            plan.Scan(statsExtractor);

            UNIT_ASSERT_VALUES_EQUAL(statsExtractor.TasksCountPerStage.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(statsExtractor.TasksCountPerStage.at(0), 1);
        }
        {
            /*
                There are only 6 points in the range:
                [0;0], [268435455;268435455], [536870911;536870911], ...

                With EnableParallelPointReadConsolidation the query will use 1 task for reading, without it 4 (max) tasks.
            */

            const auto query = Q_(R"(
                SELECT * FROM `TestSharded`
                WHERE Id IN (0, 268435455, 536870911, 805306367, 1073741823, 1342177279);
            )");

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
                TExecuteQuerySettings{}.StatsMode(EStatsMode::Full)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets()[0].RowsCount(), 6);

            NJson::TJsonValue plan;
            UNIT_ASSERT(NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan));

            auto statsExtractor = TTaskCountExtractor();
            plan.Scan(statsExtractor);

            UNIT_ASSERT_VALUES_EQUAL(statsExtractor.TasksCountPerStage.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(statsExtractor.TasksCountPerStage.at(0), 1);
        }
        {
            /*
                There are only 2 points in the range with the double primary key:
                [(0, "aaa");(0, "aaa")], [(9, "cde");(9, "cde")]

                With EnableParallelPointReadConsolidation the query will use 1 task for reading, without it 2 tasks.
            */

            const auto query = Q_(R"(
                SELECT * FROM `TestShardedDoublePrimary`
                WHERE (Id1, Id2) IN ((0, "aaa"), (9, "cde"));
            )");

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
                TExecuteQuerySettings{}.StatsMode(EStatsMode::Full)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets()[0].RowsCount(), 2);

            NJson::TJsonValue plan;
            UNIT_ASSERT(NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan));

            auto statsExtractor = TTaskCountExtractor();
            plan.Scan(statsExtractor);

            UNIT_ASSERT_VALUES_EQUAL(statsExtractor.TasksCountPerStage.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(statsExtractor.TasksCountPerStage.at(0), 1);

        }
        {
            /*
                There is a segment with another points as ranges:
                [0;1], [268435455;268435455], [536870911;536870911], ...

                The query will use 4 (max) tasks regardless of the consolidation setting.
            */

            const auto query = Q_(R"(
                SELECT * FROM `TestSharded`
                WHERE Id IN (0, 1, 268435455, 536870911, 805306367, 1073741823, 1342177279);
            )");

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
                TExecuteQuerySettings{}.StatsMode(EStatsMode::Full)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets()[0].RowsCount(), 6);

            NJson::TJsonValue plan;
            UNIT_ASSERT(NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan));

            auto statsExtractor = TTaskCountExtractor();
            plan.Scan(statsExtractor);

            UNIT_ASSERT_VALUES_EQUAL(statsExtractor.TasksCountPerStage.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(statsExtractor.TasksCountPerStage.at(0), 1);
        }
        {
            /*
                This is a full range scan starting from 0 to infinity:
                (0;+infinity)

                The query will use 4 (max) tasks regardless of the consolidation setting.
            */

            const auto query = Q_(R"(
                SELECT * FROM `TestSharded`
                WHERE Id > 0;
            )");

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
                TExecuteQuerySettings{}.StatsMode(EStatsMode::Full)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets()[0].RowsCount(), 19);

            NJson::TJsonValue plan;
            UNIT_ASSERT(NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan));

            auto statsExtractor = TTaskCountExtractor();
            plan.Scan(statsExtractor);

            UNIT_ASSERT_VALUES_EQUAL(statsExtractor.TasksCountPerStage.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(statsExtractor.TasksCountPerStage.at(0), 1);

        }
        {
            /*
                There are only 2 points in the range with some order:
                [0;0], [268435455;268435455]

                The query is sorted, so the consolidation is not applied and 2 tasks are used.
            */

            const auto query = Q_(R"(
                SELECT * FROM `TestSharded`
                WHERE Id IN (0, 268435455) ORDER BY Id;
            )");

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
                TExecuteQuerySettings{}.StatsMode(EStatsMode::Full)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets()[0].RowsCount(), 2);

            NJson::TJsonValue plan;
            UNIT_ASSERT(NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan));

            auto statsExtractor = TTaskCountExtractor();
            plan.Scan(statsExtractor);

            UNIT_ASSERT_VALUES_EQUAL(statsExtractor.TasksCountPerStage.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(statsExtractor.TasksCountPerStage.at(0), 2);
        }
    }

    Y_UNIT_TEST(ReadRanges) {
        TKikimrRunner kikimr(GetAppConfig(/* pointConsolidation */ true));
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        {
            const auto query = Q_(R"(
                SELECT Id, Value FROM `TestSharded`
                WHERE Id IN (0, 268435455);
            )");

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
                TExecuteQuerySettings{}.StatsMode(EStatsMode::Full)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto resultSet = ResultSetToSortedVector(result);
            auto expected = std::vector<ResultSetRow>{
                {0, 0}, {268435455, 2}
            };
            UNIT_ASSERT_VALUES_EQUAL(resultSet, expected);
        }
        {
            const auto query = Q_(R"(
                SELECT Id, Value FROM `TestSharded`
                WHERE Id IN (0, 268435455, 536870911, 805306367, 1073741823, 1342177279);
            )");

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
                TExecuteQuerySettings{}.StatsMode(EStatsMode::Full)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto resultSet = ResultSetToSortedVector(result);
            auto expected = std::vector<ResultSetRow>{
                {0, 0}, {268435455, 2}, {536870911, 0}, {805306367, 3},
                {1073741823, 1}, {1342177279, 4}
            };

            UNIT_ASSERT_VALUES_EQUAL(resultSet, expected);
        }
        {
            const auto query = Q_(R"(
                SELECT Id, Value FROM `TestSharded`
                WHERE Id IN (0, 1, 268435455, 536870911, 805306367, 1073741823, 1342177279);
            )");

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
                TExecuteQuerySettings{}.StatsMode(EStatsMode::Full)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto resultSet = ResultSetToSortedVector(result);
            auto expected = std::vector<ResultSetRow>{
                {0, 0}, {268435455, 2}, {536870911, 0}, {805306367, 3},
                {1073741823, 1}, {1342177279, 4}
            };

            UNIT_ASSERT_VALUES_EQUAL(resultSet, expected);
        }
        {
            const auto query = Q_(R"(
                SELECT Id, Value FROM `TestSharded`
                WHERE Id > 0;
            )");

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
                TExecuteQuerySettings{}.StatsMode(EStatsMode::Full)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto resultSet = ResultSetToSortedVector(result);
            auto expected = std::vector<ResultSetRow>{
                {70640909, 4}, {141281818, 3}, {211922728, 3}, {268435455, 2},
                {353204547, 2}, {423845456, 1}, {494486365, 0}, {536870911, 0},
                {635768184, 4}, {706409094, 4}, {777050003, 3}, {805306367, 3},
                {918331822, 2}, {988972731, 1}, {1073741823, 1}, {1130254550, 0},
                {1200895460, 0}, {1271536369, 4}, {1342177279, 4}
            };

            UNIT_ASSERT_VALUES_EQUAL(resultSet, expected);
        }
        {
            const auto query = Q_(R"(
                SELECT Id1, Id2, Value FROM `TestShardedDoublePrimary`
                WHERE (Id1, Id2) IN ((0, "aaa"), (2, "a"), (5, "bc"), (9, "cde"), (10, "fg"));
            )");

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(),
                TExecuteQuerySettings{}.StatsMode(EStatsMode::Full)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto resultSet = ResultSetDoubleToSortedVector(result);
            auto expected = std::vector<ResultSetDoubleRow>{
                {0, "aaa", 1}, {2, "a", 0}, {5, "bc", 2},
                {9, "cde", 3}, {10, "fg", 4}
            };

            UNIT_ASSERT_VALUES_EQUAL(resultSet, expected);
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
