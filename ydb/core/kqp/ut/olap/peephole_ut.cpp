#include "helpers/local.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

using namespace NYdb;

namespace {

struct TOlapTestSetup {
    std::unique_ptr<NYDBTest::ICSController::TGuard> CsController;
    std::unique_ptr<TKikimrRunner> Kikimr;
    std::unique_ptr<NTable::TSession> Session;
    std::unique_ptr<NQuery::TQueryClient> Client;

    TOlapTestSetup() {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        CsController = std::make_unique<NYDBTest::ICSController::TGuard>(
            NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>());
        Kikimr = std::make_unique<TKikimrRunner>(settings);
        auto session = Kikimr->GetTableClient().CreateSession().GetValueSync().GetSession();
        Session = std::make_unique<NTable::TSession>(session);
        Client = std::make_unique<NQuery::TQueryClient>(Kikimr->GetQueryClient());
    }

    void CreateOlapTable(const TString& path, const TVector<std::pair<TString, TString>>& columns,
                         const TString& primaryKey)
    {
        TStringBuilder query;
        query << "CREATE TABLE `" << path << "` (";
        for (size_t i = 0; i < columns.size(); ++i) {
            if (i > 0) query << ", ";
            query << columns[i].first << " " << columns[i].second;
        }
        query << ", PRIMARY KEY (" << primaryKey << "))";
        query << " PARTITION BY HASH(" << primaryKey << ")";
        query << " WITH (STORE = COLUMN, PARTITION_COUNT = 1)";

        auto result = Session->ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
    }

    TString ExplainQuery(const TString& query) {
        NQuery::TExecuteQuerySettings explainSettings;
        explainSettings.ExecMode(NQuery::EExecMode::Explain);
        auto it = Client->StreamExecuteQuery(
            query, NQuery::TTxControl::BeginTx().CommitTx(), explainSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        auto plan = CollectStreamResult(it);
        UNIT_ASSERT(plan.QueryStats.Defined());
        return plan.QueryStats->Getquery_ast();
    }
};

} // namespace

Y_UNIT_TEST_SUITE(KqpOlapPeephole) {

    Y_UNIT_TEST(EliminateWideMapPackUnpackOnSelectStarLimit) {
        TOlapTestSetup setup;
        setup.CreateOlapTable("/Root/TestTable", {
            {"id", "Uint64 NOT NULL"},
            {"c1", "String"},
            {"c2", "String"},
            {"c3", "String"},
        }, "id");

        const auto ast = setup.ExplainQuery("SELECT * FROM `/Root/TestTable` LIMIT 1");

        UNIT_ASSERT_C(ast.Contains("(WideTakeBlocks (FromFlow"),
            "Scan stage: expected WideTakeBlocks directly on FromFlow "
            "(WideMap pack/unpack roundtrip should be eliminated). AST: " + ast);

        UNIT_ASSERT_C(!ast.Contains("(WideMap (WideTakeBlocks (WideMap (FromFlow"),
            "Scan stage: WideMap pack/unpack roundtrip should not be present. AST: " + ast);
    }

    Y_UNIT_TEST(PreserveWideMapInComputeStage) {
        TOlapTestSetup setup;
        setup.CreateOlapTable("/Root/TestTable", {
            {"id", "Uint64 NOT NULL"},
            {"c1", "String"},
            {"c2", "String"},
            {"c3", "String"},
        }, "id");

        const auto ast = setup.ExplainQuery("SELECT * FROM `/Root/TestTable` LIMIT 1");

        UNIT_ASSERT_C(ast.Contains("(WideMap (WideTakeBlocks"),
            "Compute stage: WideMap around WideTakeBlocks should be preserved "
            "(our optimization only eliminates scan stage pack/unpack roundtrip). AST: " + ast);
    }

    Y_UNIT_TEST(EliminatePackUnpackWithColumnSubset) {
        TOlapTestSetup setup;
        setup.CreateOlapTable("/Root/TestTable", {
            {"id", "Uint64 NOT NULL"},
            {"c1", "String"},
            {"c2", "String"},
            {"c3", "String"},
        }, "id");

        const auto ast = setup.ExplainQuery("SELECT id, c1 FROM `/Root/TestTable` LIMIT 1");

        UNIT_ASSERT_C(ast.Contains("(WideTakeBlocks (FromFlow"),
            "Scan stage: expected WideTakeBlocks directly on FromFlow "
            "even with column subset (pack/unpack roundtrip should be eliminated). AST: " + ast);
    }
}

} // namespace NKikimr::NKqp
