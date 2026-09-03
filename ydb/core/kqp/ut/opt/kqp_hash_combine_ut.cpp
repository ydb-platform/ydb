#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <util/folder/dirut.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {
    // Low compute memory limits + spilling, the resource manager spilling threshold and the operator
    // memory quota (RFC dq_memory_quota_20) set as requested
    TKikimrSettings CreateSpillingSettings(double spillingPercent, bool enableOperatorMemoryQuota)
    {
        TKikimrSettings settings = TKikimrSettings().SetWithSampleTables(false);

        auto* ts = settings.AppConfig.MutableTableServiceConfig();
        ts->SetEnableOlapSink(true);
        ts->SetEnableQueryServiceSpilling(true);

        auto* rm = ts->MutableResourceManager();
        rm->SetMkqlLightProgramMemoryLimit(100);
        rm->SetMkqlHeavyProgramMemoryLimit(300);
        rm->SetSpillingPercent(spillingPercent);
        rm->SetEnableOperatorMemoryQuota(enableOperatorMemoryQuota);

        auto* spilling = ts->MutableSpillingServiceConfig()->MutableLocalFileConfig();
        spilling->SetRoot("./spilling/");
        MakeDirIfNotExist("./spilling");

        return settings;
    }

    void RunAggregateSpillingCase(double spillingPercent, bool expectSpilling, bool enableOperatorMemoryQuota)
    {
        // the planner spreads the final aggregation over many tasks (96 on a large dev box, fewer on CI); every
        // task needs a hash table of more than 1024 keys (LowerFixedRowCount) before the operator considers
        // spilling, so the group count is sized for the largest task count
        constexpr i64 rowCount = 600000;
        constexpr i64 groupCount = 200000;
        constexpr i64 rowsPerBatch = 30000;

        TKikimrRunner kikimr(CreateSpillingSettings(spillingPercent, enableOperatorMemoryQuota));
        auto queryClient = kikimr.GetQueryClient();

        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    CREATE TABLE `/Root/aggregatable` (
                        id Int64 NOT NULL,
                        group_key Int64 NOT NULL,
                        data Int64 NOT NULL,
                        PRIMARY KEY (id)
                    )
                    WITH (STORE = COLUMN);
                )",  NYdb::NQuery::TTxControl::NoTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }
        for (i64 start = 0; start < rowCount; start += rowsPerBatch) {
            auto status = queryClient.ExecuteQuery(Sprintf(R"(
                UPSERT INTO `/Root/aggregatable`
                SELECT id, Unwrap(id %% %ld) AS group_key, id AS data
                FROM AS_TABLE(ListMap(ListFromRange(%ldL, %ldL), ($i) -> (<|id: $i|>)));
            )", groupCount, start, start + rowsPerBatch), NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        TString groupQuery = R"(
            PRAGMA TablePathPrefix = "/Root";
            PRAGMA ydb.UseDqHashCombine = "true";
            PRAGMA ydb.UseDqHashAggregate = "true";
            PRAGMA ydb.OptUseFinalizeByKey = "true";
            PRAGMA ydb.OptEnableOlapPushdown = "false";

            SELECT T.group_key AS group_key, SUM(T.data) AS data_sum
            FROM `aggregatable` AS T
            GROUP BY group_key
        )";

        auto status = queryClient.ExecuteQuery(groupQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        auto resultSet = status.GetResultSets()[0];
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), groupCount);

        // group g holds ids g, g + groupCount, ... : rowCount / groupCount values
        constexpr i64 valuesPerGroup = rowCount / groupCount;
        TResultSetParser rp(resultSet);
        while (rp.TryNextRow()) {
            const i64 groupKey = rp.ColumnParser("group_key").GetInt64();
            const i64 dataSum = rp.ColumnParser("data_sum").GetInt64();
            UNIT_ASSERT_VALUES_EQUAL(dataSum, valuesPerGroup * groupKey + groupCount * valuesPerGroup * (valuesPerGroup - 1) / 2);
        }

        TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);
        if (expectSpilling) {
            UNIT_ASSERT(counters.ComputeSpilling.WriteBlobs->Val() > 0);
            UNIT_ASSERT(counters.ComputeSpilling.ReadBlobs->Val() > 0);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(counters.ComputeSpilling.WriteBlobs->Val(), 0);
        }
    }

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

    void PrefillTables(NYdb::NQuery::TQueryClient& queryClient)
    {
        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    CREATE TABLE `/Root/aggregatable` (
                        id Int64 NOT NULL,
                        group_key Int64 NOT NULL,
                        data Int64 NOT NULL,
                        PRIMARY KEY (id)
                    )
                    WITH (STORE = COLUMN);
                )",  NYdb::NQuery::TTxControl::NoTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }
        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    INSERT INTO `/Root/aggregatable` (id, group_key, data) VALUES
                        (1, 0, 100),
                        (2, 0, 600),
                        (3, 1, 300),
                        (4, 1, 400)
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()
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

    Y_UNIT_TEST(DqHashAggregateSpillingUnderLowLimits) {
        RunAggregateSpillingCase(0.01, /* expectSpilling = */ true, /* enableOperatorMemoryQuota = */ false);
    }

    Y_UNIT_TEST(DqHashAggregateSpillingUnderLowLimitsWithOperatorMemoryQuota) {
        // the negative memory availability of the resource manager drives the aggregation to spill
        RunAggregateSpillingCase(0.01, /* expectSpilling = */ true, /* enableOperatorMemoryQuota = */ true);
    }

    Y_UNIT_TEST(DqHashAggregateNoSpillingAtHighPercentWithOperatorMemoryQuota) {
        RunAggregateSpillingCase(100, /* expectSpilling = */ false, /* enableOperatorMemoryQuota = */ true);
    }

    Y_UNIT_TEST(DqHashCombineBlockTest) {
        auto settings = CreateSettings();
        TKikimrRunner kikimr(settings);

        auto queryClient = kikimr.GetQueryClient();
        PrefillTables(queryClient);

        TString hints = R"(
            PRAGMA TablePathPrefix = "/Root";
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

        TString ast = TString(*astOpt);
        Cout << "AST: " << ast << Endl;

        UNIT_ASSERT(CountDqCombines(ast) == 2);

        UNIT_ASSERT_C(ast.Contains("(return (DqPhyHashCombine"),
            TStringBuilder() << "AST should return the result of DqPhyHashCombine directly: " << groupQuery << Endl << ast);
    }
}

} // namespace NKqp
} // namespace NKikimr
