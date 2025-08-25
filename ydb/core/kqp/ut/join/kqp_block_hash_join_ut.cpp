
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpBlockHashJoin) {
    Y_UNIT_TEST_TWIN(BlockHashJoinTest, UseBlockHashJoin) {
        TKikimrSettings settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        TKikimrRunner kikimr(settings);

        auto queryClient = kikimr.GetQueryClient();
        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    CREATE TABLE `/Root/left_table` (
                        id Int32 NOT NULL,
                        data String NOT NULL,
                        PRIMARY KEY (id, data)
                    )
                    WITH (STORE = COLUMN);

                    CREATE TABLE `/Root/right_table` (
                        id Int32 NOT NULL,
                        data String NOT NULL,
                        PRIMARY KEY (id, data)
                    )
                    WITH (STORE = COLUMN);
                )",  NYdb::NQuery::TTxControl::NoTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {
            auto status = queryClient.ExecuteQuery(
                R"(
                    INSERT INTO `/Root/left_table` (id, data) VALUES
                        (1, "1"),
                        (2, "2"),
                        (3, "3");

                    INSERT INTO `/Root/right_table` (id, data) VALUES
                        (1, "1"),
                        (2, "2"),
                        (3, "3");
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()
            ).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }

        {

            TString hints = R"(
                PRAGMA TablePathPrefix='/Root';
                PRAGMA ydb.OptimizerHints=
                    '
                        Bytes(L # 10e12)
                        Bytes(R # 10e12)
                    ';
            )";
            TString blocks = UseBlockHashJoin ? "PRAGMA ydb.UseBlockHashJoin = \"true\";\n\n" : "";
            TString select = R"(
                SELECT L.*
                FROM `left_table` AS L
                INNER JOIN `right_table` AS R
                ON L.id = R.id AND L.data = R.data;
            )";

            TString joinQuery = TStringBuilder() << hints << blocks << select;

            auto status = queryClient.ExecuteQuery(joinQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();

            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

            auto resultSet = status.GetResultSets()[0];
            // Current Join implementation is simple and returns all the rows
            auto expectedRowsCount = UseBlockHashJoin ? 6 : 3;
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), expectedRowsCount);

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
