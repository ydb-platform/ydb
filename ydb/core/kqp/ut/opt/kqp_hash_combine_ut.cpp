#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpHashCombineReplacement) {
    Y_UNIT_TEST_TWIN(DqHashCombineTest, UseDqHashCombine) {
        TKikimrSettings settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        TKikimrRunner kikimr(settings);

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

        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    INSERT INTO `/Root/aggregatable` (id, group_key, data) VALUES
                        (1, 0, 100),
                        (2, 0, 200),
                        (3, 1, 300),
                        (4, 1, 400)
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {
            TString hints = R"(
                PRAGMA TablePathPrefix='/Root';
            )";
            TString blocks = UseDqHashCombine ? "PRAGMA ydb.UseDqHashCombine = \"true\";\n\n" : "";
            TString select = R"(
                PRAGMA ydb.OptUseFinalizeByKey = "true";
                PRAGMA ydb.OptEnableOlapPushdown = "false"; -- need this to force intermediate/final combiner pair over the sample table

                SELECT T.group_key, SUM(T.data)
                FROM `aggregatable` AS T
                GROUP BY group_key
            )";

            TString groupQuery = TStringBuilder() << hints << blocks << select;

            auto status = queryClient.ExecuteQuery(groupQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();

            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

            auto resultSet = status.GetResultSets()[0];
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 2);

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
}

} // namespace NKqp
} // namespace NKikimr
