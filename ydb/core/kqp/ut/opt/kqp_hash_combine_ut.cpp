#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {
    TKikimrSettings CreateSettings()
    {
        TKikimrSettings settings = TKikimrSettings().SetWithSampleTables(false);

        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        // Enable manual override of _KqpYqlCombinerMemoryLimit
        settings.AppConfig.MutableTableServiceConfig()->MutableResourceManager()->SetMkqlHeavyProgramMemoryLimit(0);
        NKikimrKqp::TKqpSetting combinerMemLimit;
        combinerMemLimit.SetName("_KqpYqlCombinerMemoryLimit");
        combinerMemLimit.SetValue("1000000");
        settings.KqpSettings.emplace_back(combinerMemLimit);

        return settings;
    }

    void PrefillTables(NYdb::NQuery::TQueryClient& queryClient, bool large = false)
    {
        {
            const TString partitioning = large
                ? "PARTITION BY HASH (id) WITH (STORE = COLUMN, PARTITION_COUNT = 4)"
                : "WITH (STORE = COLUMN)";
            auto status = queryClient.ExecuteQuery(
                TStringBuilder() << R"(
                    CREATE TABLE `/Root/aggregatable` (
                        id Int64 NOT NULL,
                        group_key Int64 NOT NULL,
                        data Int64 NOT NULL,
                        PRIMARY KEY (id)
                    )
                )" << partitioning << ";", NYdb::NQuery::TTxControl::NoTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }
        {
            TStringBuilder insert;
            insert << "INSERT INTO `/Root/aggregatable` (id, group_key, data) VALUES\n";
            if (large) {
                constexpr size_t rows = 4096;
                constexpr size_t groups = 1024;
                for (size_t i = 0; i < rows; ++i) {
                    insert << "(" << i << ", " << i % groups << ", 1)"
                        << (i + 1 == rows ? ";\n" : ",\n");
                }
            } else {
                insert << R"(
                    (1, 0, 100),
                    (2, 0, 600),
                    (3, 1, 300),
                    (4, 1, 400)
                )";
            }
            auto status = queryClient.ExecuteQuery(
                insert, NYdb::NQuery::TTxControl::BeginTx().CommitTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }
    }

    int CountDqCombines(std::string_view ast) {
        int hashCombines = 0;
        size_t pos = 0;
        const std::string_view combinerName {"DqPhyHashCombine"};
        while ((pos = ast.find(combinerName, pos)) != std::string::npos) {
            ++hashCombines;
            ++pos;
        }
        return hashCombines;
    }

    void CheckGroupByResultSet(TResultSet& resultSet)
    {
        // Check the result of sum(data) as data_sum group by group_key
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 2);
        TResultSetParser rp(resultSet);
        while (rp.TryNextRow()) {
            ssize_t idx = rp.ColumnIndex("data_sum");
            UNIT_ASSERT(idx >= 0);
            UNIT_ASSERT(rp.GetValue(idx).GetProto().int64_value() == 700);
        }
    }

    class TTaskCountExtractor : public NJson::IScanCallback {
    public:
        THashMap<int, int> TasksCountPerStage;

        bool Do(const TString&, NJson::TJsonValue*, NJson::TJsonValue& value) override
        {
            if (value.IsMap() && value.Has("Tasks") && value.Has("PhysicalStageId")) {
                const int taskCount = value["Tasks"].GetIntegerSafe();
                const int stageId = value["PhysicalStageId"].GetIntegerSafe();
                const auto [_, inserted] = TasksCountPerStage.emplace(stageId, taskCount);
                UNIT_ASSERT_C(inserted, TStringBuilder() << "Duplicate stage " << stageId);
            }
            return true;
        }
    };
}

Y_UNIT_TEST_SUITE(KqpHashCombineReplacement) {
    Y_UNIT_TEST_QUAD(DqHashCombineTest, UseDqHashCombine, UseDqHashAggregate) {
        auto settings = CreateSettings();
        TKikimrRunner kikimr(settings);

        auto queryClient = kikimr.GetQueryClient();
        PrefillTables(queryClient);

        {
            TString hints = R"(
                PRAGMA TablePathPrefix = "/Root";
            )";
            TString dqHashCombinePragma = Sprintf("PRAGMA ydb.UseDqHashCombine = \"%s\";\n\n", UseDqHashCombine ? "true" : "false");
            TString dqHashAggregatePragma = Sprintf("PRAGMA ydb.UseDqHashAggregate = \"%s\";\n\n", UseDqHashAggregate ? "true" : "false");
            TString select = R"(
                PRAGMA ydb.OptUseFinalizeByKey = "true";
                PRAGMA ydb.OptEnableOlapPushdown = "false"; -- need this to force intermediate/final combiner pair over the sample table

                SELECT T.group_key as group_key, SUM(T.data) as data_sum
                FROM `aggregatable` AS T
                GROUP BY group_key
            )";

            TString groupQuery = TStringBuilder() << hints << dqHashCombinePragma << dqHashAggregatePragma << select;

            auto status = queryClient.ExecuteQuery(groupQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
            auto resultSet = status.GetResultSets()[0];
            CheckGroupByResultSet(resultSet);

            auto explainResult = queryClient.ExecuteQuery(
                groupQuery,
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)
            ).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(explainResult.GetStatus(), EStatus::SUCCESS, explainResult.GetIssues().ToString());

            auto astOpt = explainResult.GetStats()->GetAst();
            UNIT_ASSERT(astOpt.has_value());
            Cout << TString(*astOpt) << Endl;
            TString ast = TString(*astOpt);
            Cout << "AST (HashCombine=" << (UseDqHashCombine ? "true" : "false") << ", HashAggregate=" << (UseDqHashAggregate ? "true" : "false") << "): " << ast << Endl;

            int hashCombinesExpected = (UseDqHashCombine ? 1 : 0) + (UseDqHashAggregate ? 1 : 0);
            UNIT_ASSERT_C(hashCombinesExpected == CountDqCombines(ast),
                TStringBuilder() << "AST should contain " << hashCombinesExpected << " DqPhyHashCombine instances; actual AST: " << groupQuery << Endl << ast);
        }
    }

    Y_UNIT_TEST(DqHashCombineBlockTest) {
        auto settings = CreateSettings();
        TKikimrRunner kikimr(settings);

        auto queryClient = kikimr.GetQueryClient();
        PrefillTables(queryClient, true);

        TString hints = R"(
            PRAGMA TablePathPrefix = "/Root";
            PRAGMA ydb.MaxTasksPerStage = "40";
            PRAGMA ydb.OverridePlanner = @@ [
                { "tx": 0, "stage": 0, "tasks": 40 },
                { "tx": 0, "stage": 1, "tasks": 40 }
            ] @@;
            PRAGMA ydb.UseDqHashCombine = "true";
            PRAGMA ydb.UseDqHashAggregate = "true";
            PRAGMA ydb.DqHashOperatorsUseBlocks = "true";
            PRAGMA ydb.OptUseFinalizeByKey = "true";
            PRAGMA ydb.OptEnableOlapPushdown = "false"; -- need this to force intermediate/final combiner pair over the sample table
        )";
        TString select = R"(
            SELECT T.group_key as group_key, SUM(T.data) as data_sum
            FROM `aggregatable` AS T
            GROUP BY group_key
        )";

        TString groupQuery = TStringBuilder() << hints << select;
        auto status = queryClient.ExecuteQuery(
            groupQuery,
            NYdb::NQuery::TTxControl::BeginTx().CommitTx(),
            NYdb::NQuery::TExecuteQuerySettings().StatsMode(NYdb::NQuery::EStatsMode::Full)
        ).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        auto resultSet = status.GetResultSets()[0];
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1024);
        TResultSetParser resultParser(resultSet);
        while (resultParser.TryNextRow()) {
            UNIT_ASSERT_VALUES_EQUAL(resultParser.ColumnParser("data_sum").GetInt64(), 4);
        }

        NJson::TJsonValue plan;
        UNIT_ASSERT(status.GetStats()->GetPlan().has_value());
        UNIT_ASSERT(NJson::ReadJsonTree(*status.GetStats()->GetPlan(), &plan));
        TTaskCountExtractor taskCounts;
        plan.Scan(taskCounts);
        UNIT_ASSERT(taskCounts.TasksCountPerStage.contains(0));
        UNIT_ASSERT(taskCounts.TasksCountPerStage.contains(1));
        UNIT_ASSERT_VALUES_EQUAL(taskCounts.TasksCountPerStage.at(0), 40);
        UNIT_ASSERT_VALUES_EQUAL(taskCounts.TasksCountPerStage.at(1), 40);

        TString supportedTypesQuery = TStringBuilder() << hints << R"(
            SELECT
                T.string_key AS string_key,
                T.utf8_key AS utf8_key,
                SUM(T.data) AS data_sum
            FROM (
                SELECT
                    CAST(group_key AS String) AS string_key,
                    CAST(group_key AS Utf8) AS utf8_key,
                    CAST(data AS Uint64) AS data
                FROM `aggregatable`
            ) AS T
            GROUP BY string_key, utf8_key
        )";
        auto supportedTypesStatus = queryClient.ExecuteQuery(
            supportedTypesQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(supportedTypesStatus.IsSuccess(), supportedTypesStatus.GetIssues().ToString());

        auto supportedTypesResultSet = supportedTypesStatus.GetResultSets()[0];
        UNIT_ASSERT_VALUES_EQUAL(supportedTypesResultSet.RowsCount(), 1024);
        TResultSetParser parser(supportedTypesResultSet);
        THashSet<TString> seen;
        while (parser.TryNextRow()) {
            const TString stringKey(parser.ColumnParser("string_key").GetString());
            const TString utf8Key(parser.ColumnParser("utf8_key").GetUtf8());
            UNIT_ASSERT_VALUES_EQUAL(stringKey, utf8Key);
            seen.insert(stringKey);

            const ssize_t dataSumIndex = parser.ColumnIndex("data_sum");
            UNIT_ASSERT(dataSumIndex >= 0);
            UNIT_ASSERT_VALUES_EQUAL(parser.GetValue(dataSumIndex).GetProto().uint64_value(), 4);
        }
        UNIT_ASSERT_VALUES_EQUAL(seen.size(), 1024);

        auto explainResult = queryClient.ExecuteQuery(
            groupQuery,
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)
        ).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(explainResult.GetStatus(), EStatus::SUCCESS, explainResult.GetIssues().ToString());
        auto astOpt = explainResult.GetStats()->GetAst();
        UNIT_ASSERT(astOpt.has_value());

        TString ast = TString(*astOpt);
        Cout << "AST: " << ast << Endl;

        UNIT_ASSERT(CountDqCombines(ast) == 2);

        UNIT_ASSERT_C(ast.Contains("(return (DqPhyHashCombine"),
            TStringBuilder() << "AST should return the result of DqPhyHashCombine directly: " << groupQuery << Endl << ast);
    }
}

} // namespace NKqp
} // namespace NKikimr
