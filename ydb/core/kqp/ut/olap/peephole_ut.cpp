#include "helpers/local.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

using namespace NYdb;

Y_UNIT_TEST_SUITE(KqpOlapPeephole) {

    Y_UNIT_TEST(EliminateWideMapPackUnpackOnSelectStarLimit) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        TKikimrRunner kikimr(settings);

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        {
            auto result = session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/TestTable` (
                    id Uint64 NOT NULL, c1 String, c2 String, c3 String,
                    PRIMARY KEY (id)
                )
                PARTITION BY HASH(id)
                WITH (STORE = COLUMN, PARTITION_COUNT = 1)
            )").GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto client = kikimr.GetQueryClient();
        NQuery::TExecuteQuerySettings explainSettings;
        explainSettings.ExecMode(NQuery::EExecMode::Explain);
        auto it = client.StreamExecuteQuery(
            "SELECT * FROM `/Root/TestTable` LIMIT 1",
            NQuery::TTxControl::BeginTx().CommitTx(), explainSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        auto plan = CollectStreamResult(it);
        UNIT_ASSERT(plan.QueryStats.Defined());
        const auto& ast = plan.QueryStats->Getquery_ast();

        UNIT_ASSERT_C(ast.Contains("(WideTakeBlocks (FromFlow"),
            "Scan stage: expected WideTakeBlocks directly on FromFlow "
            "(WideMap pack/unpack roundtrip should be eliminated). AST: " + ast);
    }
}

} // namespace NKikimr::NKqp
