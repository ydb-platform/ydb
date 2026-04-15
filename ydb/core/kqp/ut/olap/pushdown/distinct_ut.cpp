#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/olap/helpers/local.h>
#include <ydb/core/kqp/ut/olap/helpers/writer.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpOlapDistinctPushdown) {
    Y_UNIT_TEST(DistinctPushdownAstAndSsa_NoLimit) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();
        const TString query = R"(
                --!syntax_v1
                PRAGMA Kikimr.OptEnableOlapPushdown = "true";
                PRAGMA Kikimr.OptEnableOlapPushdownDistinct = "true";
                SELECT DISTINCT `level` FROM `/Root/olapStore/olapTable`
            )";

        NYdb::NTable::TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);
        auto it = tableClient.StreamExecuteScanQuery(query, scanSettings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        auto planRes = CollectStreamResult(it);
        const TString& ast = planRes.QueryStats->Getquery_ast();
        UNIT_ASSERT_C(ast.find("TKqpOlapDistinct") != TString::npos, ast);

        NJson::TJsonValue planJson;
        NJson::ReadJsonTree(*planRes.PlanJson, &planJson, true);
        auto readNode = FindPlanNodeByKv(planJson, "Node Type", "Aggregate-TableFullScan");
        UNIT_ASSERT(readNode.IsDefined());

        bool sawDistinctCmd = false;
        bool sawSsaLimit = false;
        auto& operators = readNode.GetMapSafe().at("Operators").GetArraySafe();
        for (auto& op : operators) {
            if (op.GetMapSafe().at("Name") == "TableFullScan") {
                UNIT_ASSERT(op.GetMapSafe().at("SsaProgram").IsDefined());
                const auto& ssaProgram = op.GetMapSafe().at("SsaProgram");
                UNIT_ASSERT(
                    !FindPlanNodes(ssaProgram, "Distinct").empty()
                    || !FindPlanNodes(ssaProgram, "distinct").empty());
                sawDistinctCmd = true;
                sawSsaLimit = !FindPlanNodes(ssaProgram, "Limit").empty()
                    || !FindPlanNodes(ssaProgram, "limit").empty();
                break;
            }
        }
        UNIT_ASSERT(sawDistinctCmd);
        UNIT_ASSERT_C(!sawSsaLimit, "SELECT DISTINCT without LIMIT must not set SSA Distinct.Limit");
    }

    Y_UNIT_TEST(DistinctPushdownAstAndSsa_WithLimit) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();
        const TString query = R"(
                --!syntax_v1
                PRAGMA Kikimr.OptEnableOlapPushdown = "true";
                PRAGMA Kikimr.OptEnableOlapPushdownDistinct = "true";
                SELECT DISTINCT `level` FROM `/Root/olapStore/olapTable` LIMIT 100
            )";

        NYdb::NTable::TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);
        auto it = tableClient.StreamExecuteScanQuery(query, scanSettings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        auto planRes = CollectStreamResult(it);
        const TString& ast = planRes.QueryStats->Getquery_ast();
        UNIT_ASSERT_C(ast.find("TKqpOlapDistinct") != TString::npos, ast);

        NJson::TJsonValue planJson;
        NJson::ReadJsonTree(*planRes.PlanJson, &planJson, true);
        auto readNode = FindPlanNodeByKv(planJson, "Node Type", "Aggregate-TableFullScan");
        UNIT_ASSERT(readNode.IsDefined());

        bool sawDistinctCmd = false;
        bool sawSsaLimit = false;
        auto& operators = readNode.GetMapSafe().at("Operators").GetArraySafe();
        for (auto& op : operators) {
            if (op.GetMapSafe().at("Name") == "TableFullScan") {
                UNIT_ASSERT(op.GetMapSafe().at("SsaProgram").IsDefined());
                const auto& ssaProgram = op.GetMapSafe().at("SsaProgram");
                UNIT_ASSERT(
                    !FindPlanNodes(ssaProgram, "Distinct").empty()
                    || !FindPlanNodes(ssaProgram, "distinct").empty());
                sawDistinctCmd = true;
                sawSsaLimit = !FindPlanNodes(ssaProgram, "Limit").empty()
                    || !FindPlanNodes(ssaProgram, "limit").empty();
                break;
            }
        }
        UNIT_ASSERT(sawDistinctCmd);
        UNIT_ASSERT_C(sawSsaLimit, "SELECT DISTINCT ... LIMIT must set SSA Distinct.Limit (ItemsLimit + compiler)");
    }

    Y_UNIT_TEST(DistinctLimit_NoPushdownWithoutPragma) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();
        const TString query = R"(
                --!syntax_v1
                PRAGMA Kikimr.OptEnableOlapPushdown = "true";
                SELECT DISTINCT `level` FROM `/Root/olapStore/olapTable`
            )";

        NYdb::NTable::TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);
        auto it = tableClient.StreamExecuteScanQuery(query, scanSettings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        auto planRes = CollectStreamResult(it);
        const TString& ast = planRes.QueryStats->Getquery_ast();
        UNIT_ASSERT_C(ast.find("TKqpOlapDistinct") == TString::npos, ast);
    }
}

} // namespace NKikimr::NKqp
