
#include <counters/kqp_counters.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpScalarHashJoin) {
    Y_UNIT_TEST_TWIN(ScalarHashJoinTest, UseScalarHashJoin) {
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
            TString scalar = "PRAGMA ydb.UseScalarHashJoin = \"" + TString(UseScalarHashJoin ? "true" : "false") + "\";";
            TString select = R"(
                SELECT L.*
                FROM `left_table` AS L
                INNER JOIN `right_table` AS R
                ON L.id = R.id AND L.data = R.data;
            )";

            TString joinQuery = TStringBuilder() << hints << scalar << select;

            auto status = queryClient.ExecuteQuery(joinQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();

            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

            auto resultSet = status.GetResultSets()[0];
            auto expectedRowsCount = 3;
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
            Cout << "AST (UseScalarHashJoin=" << (UseScalarHashJoin ? "true" : "false") << "): " << ast << Endl;

            if (UseScalarHashJoin) {
                UNIT_ASSERT_C(ast.Contains("ScalarHashJoin"),
                    TStringBuilder() << "AST should contain ScalarHashJoin when enabled! Actual AST: " << ast);
            } else {
                UNIT_ASSERT_C(!ast.Contains("ScalarHashJoin"),
                    TStringBuilder() << "AST should NOT contain ScalarHashJoin when disabled! Actual AST: " << ast);
            }
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
