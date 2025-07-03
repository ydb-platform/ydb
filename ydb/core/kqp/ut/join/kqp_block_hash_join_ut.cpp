
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpBlockHashJoin) {
    Y_UNIT_TEST_TWIN(BlockHashJoinTest, UseBlockHashJoin) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto queryClient = kikimr.GetQueryClient();
        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    CREATE TABLE `/Root/left_table` (
                        id Int32 NOT NULL,
                        value String,
                        PRIMARY KEY (id)
                    )
                    WITH (STORE = COLUMN);

                    CREATE TABLE `/Root/right_table` (
                        id Int32 NOT NULL,
                        data String,
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
                    INSERT INTO `/Root/left_table` (id, value) VALUES
                        (1, "left1"),
                        (2, "left2"),
                        (3, "left3");

                    INSERT INTO `/Root/right_table` (id, data) VALUES
                        (1, "right1"),
                        (2, "right2"),
                        (3, "right3");
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {

            TString hints = R"(
                PRAGMA TablePathPrefix='/Root';
                PRAGMA ydb.OptimizerHints=
                    '
                        Rows(left_table # 10e12)
                        Rows(right_table # 10e12)
                    ';
                PRAGMA ydb.UseGraceJoinCoreForMap = "true";
            )";
            TString blocks = UseBlockHashJoin ? "PRAGMA ydb.UseBlockHashJoin = \"true\";\n\n" : "";
            TString select = R"(
                SELECT *
                FROM `left_table` AS L
                INNER JOIN `right_table` AS R
                ON L.id = R.id;
            )";

            TString joinQuery = TStringBuilder() << hints << blocks << select;

            auto status = queryClient.ExecuteQuery(joinQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();

            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());


            auto resultSet = status.GetResultSets()[0];
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 3);

            auto explainResult = queryClient.ExecuteQuery(
                joinQuery, 
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)
            ).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(explainResult.GetStatus(), EStatus::SUCCESS, explainResult.GetIssues().ToString());

            auto astOpt = explainResult.GetStats()->GetAst();
            UNIT_ASSERT(astOpt.has_value());
            TString ast = TString(*astOpt);
            Cout << "AST (UseBlockHashJoin=" << (UseBlockHashJoin ? "true" : "false") << "): " << ast << Endl;

            if (UseBlockHashJoin) {
                UNIT_ASSERT_C(ast.Contains("BlockHashJoin") || ast.Contains("DqBlockHashJoin"),
                    TStringBuilder() << "AST should contain BlockHashJoin when enabled! Actual AST: " << ast);
            } else {
                UNIT_ASSERT_C(!ast.Contains("BlockHashJoin") && !ast.Contains("DqBlockHashJoin"),
                    TStringBuilder() << "AST should NOT contain BlockHashJoin when disabled! Actual AST: " << ast);
            }
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
