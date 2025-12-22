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
    Y_UNIT_TEST_TWIN(DqHashCombineTest, UseDqHashCombine) {
        auto settings = CreateSettings();
        TKikimrRunner kikimr(settings);

        auto queryClient = kikimr.GetQueryClient();
        PrefillTables(queryClient);

        {
            TString hints = R"(
                PRAGMA TablePathPrefix = "/Root";
            )";
            TString dqHashPragma = Sprintf("PRAGMA ydb.UseDqHashCombine = \"%s\";\n\n", UseDqHashCombine ? "true" : "false");
            TString select = R"(
                PRAGMA ydb.OptUseFinalizeByKey = "true";
                PRAGMA ydb.OptEnableOlapPushdown = "false"; -- need this to force intermediate/final combiner pair over the sample table

                SELECT T.group_key as group_key, SUM(T.data) as data_sum
                FROM `aggregatable` AS T
                GROUP BY group_key
            )";

            TString groupQuery = TStringBuilder() << hints << dqHashPragma << select;

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
            Cout << "AST (DqPhyHashCombine=" << (UseDqHashCombine ? "true" : "false") << "): " << ast << Endl;

            if (UseDqHashCombine) {
                UNIT_ASSERT_C(ast.Contains("DqPhyHashCombine"),
                    TStringBuilder() << "AST should contain DqPhyHashCombine when enabled! Actual AST: " << groupQuery << Endl << ast);
            } else {
                UNIT_ASSERT_C(!ast.Contains("DqPhyHashCombine"),
                    TStringBuilder() << "AST should NOT contain DqPhyHashCombine when disabled! Actual AST: " << ast);
            }
        }
    }

    Y_UNIT_TEST(DqHashCombineBlockTest) {
        auto settings = CreateSettings();
        TKikimrRunner kikimr(settings);

        auto queryClient = kikimr.GetQueryClient();
        PrefillTables(queryClient);

        TString hints = R"(
            PRAGMA TablePathPrefix = "/Root";
            PRAGMA ydb.UseDqHashCombine = "true";
            PRAGMA ydb.DqHashCombineUsesBlocks = "true";
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

        UNIT_ASSERT_C(ast.Contains("(return (DqPhyHashCombine"),
            TStringBuilder() << "AST should return the result of DqPhyHashCombine directly: " << groupQuery << Endl << ast);
        UNIT_ASSERT_C(ast.Contains("(WideCombiner (ToFlow (WideFromBlocks"),
            TStringBuilder() << "WideCombiner input should be a block stream" << groupQuery << Endl << ast);
    }
}

} // namespace NKqp
} // namespace NKikimr
