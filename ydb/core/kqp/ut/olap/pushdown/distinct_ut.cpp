#include "../helpers/aggregation.h"
#include "../helpers/local.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

namespace {

void AssertAstItemsLimitBoundToLetUint64(const TString& ast, ui64 limit) {
    constexpr TStringBuf itemsLimitKey = "\"ItemsLimit\"";
    const size_t keyPos = TStringBuf(ast).find(itemsLimitKey);
    UNIT_ASSERT_C(keyPos != TString::npos, ast);

    // Depending on optimizer pipeline, ItemsLimit may be:
    // - inline literal: ..."ItemsLimit" (Uint64 '10)...
    // - a reference to a let-bound var: ..."ItemsLimit" $N... and (let $N (Uint64 '10))
    const TString inlineBinding1 = TStringBuilder() << "\"ItemsLimit\" (Uint64 '" << limit << "')";
    const TString inlineBinding2 = TStringBuilder() << "\"ItemsLimit\" (Uint64 '\"" << limit << "\")";
    if (ast.find(inlineBinding1, keyPos) != TString::npos || ast.find(inlineBinding2, keyPos) != TString::npos) {
        return;
    }

    size_t p = keyPos + itemsLimitKey.size();
    while (p < ast.size() && (ast[p] == ' ' || ast[p] == '\t' || ast[p] == '\n')) {
        ++p;
    }

    UNIT_ASSERT_C(p < ast.size() && ast[p] == '$', ast);
    ++p;
    UNIT_ASSERT_C(p < ast.size() && ast[p] >= '0' && ast[p] <= '9', ast);

    TString var;
    var.push_back('$');
    while (p < ast.size() && ast[p] >= '0' && ast[p] <= '9') {
        var.push_back(ast[p]);
        ++p;
    }

    const TString letBinding = TStringBuilder() << "(let " << var << " (Uint64 '" << limit << "))";
    UNIT_ASSERT_C(ast.find(letBinding) != TString::npos, ast);
}

} // namespace

Y_UNIT_TEST_SUITE(KqpOlapDistinctPushdown) {
    Y_UNIT_TEST(SimpleDistinctWithLimit_PushesDistinctAndItemsLimit) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        const TString query = R"(
            --!syntax_v1
            PRAGMA Kikimr.OptEnableOlapPushdown = "true";
            PRAGMA Kikimr.OptForceOlapPushdownDistinct = "level";
            PRAGMA Kikimr.OptForceOlapPushdownDistinctLimit = "10";

            SELECT DISTINCT `level` FROM `/Root/olapStore/olapTable` LIMIT 10
        )";

        auto res = StreamExplainQuery(query, tableClient);
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        const auto planRes = CollectStreamResult(res);
        const TString ast = TString(planRes.QueryStats->Getquery_ast());
        UNIT_ASSERT_C(ast.find("KqpOlapDistinct") != TString::npos, ast);
        AssertAstItemsLimitBoundToLetUint64(ast, 10);

        NJson::TJsonValue planJson;
        UNIT_ASSERT_C(NJson::ReadJsonTree(*planRes.PlanJson, &planJson, true), "Failed to parse plan json");

        const TString planStr = planRes.PlanJson.GetOrElse("");
        UNIT_ASSERT_C(FindPlanNodeByKv(planJson, "ReadLimit", "10").IsDefined(), planStr);

        const auto distinctNodes = FindPlanNodes(planJson, "Distinct");
        UNIT_ASSERT_C(!distinctNodes.empty(), planRes.PlanJson.GetOrElse(""));
    }

    Y_UNIT_TEST(ForceDistinctWrongColumn_Fails) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        const TString query = R"(
            --!syntax_v1
            PRAGMA Kikimr.OptEnableOlapPushdown = "true";
            PRAGMA Kikimr.OptForceOlapPushdownDistinct = "msg";
            PRAGMA Kikimr.OptForceOlapPushdownDistinctLimit = "10";

            SELECT DISTINCT `message` FROM `/Root/olapStore/olapTable`
            LIMIT 10
        )";

        auto res = StreamExplainQuery(query, tableClient);
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        auto part = res.ReadNext().GetValueSync();
        UNIT_ASSERT_C(!part.IsSuccess(), "Expected scan query stream to fail");
        const TString issues = part.GetIssues().ToString();
        UNIT_ASSERT_C(issues.Contains("OptForceOlapPushdownDistinct"), issues);
        UNIT_ASSERT_C(issues.Contains("does not match DISTINCT key column"), issues);
    }

    Y_UNIT_TEST(ForceItemsLimitWithoutSqlLimit_PushesItemsLimit) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        const TString query = R"(
            --!syntax_v1
            PRAGMA Kikimr.OptEnableOlapPushdown = "true";
            PRAGMA Kikimr.OptForceOlapPushdownDistinct = "level";
            PRAGMA Kikimr.OptForceOlapPushdownDistinctLimit = "10";

            SELECT DISTINCT `level` FROM `/Root/olapStore/olapTable`
        )";

        auto res = StreamExplainQuery(query, tableClient);
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        const auto planRes = CollectStreamResult(res);
        const TString ast = TString(planRes.QueryStats->Getquery_ast());
        UNIT_ASSERT_C(ast.find("KqpOlapDistinct") != TString::npos, ast);
        AssertAstItemsLimitBoundToLetUint64(ast, 10);

        NJson::TJsonValue planJson;
        UNIT_ASSERT_C(NJson::ReadJsonTree(*planRes.PlanJson, &planJson, true), "Failed to parse plan json");
        const TString planStr = planRes.PlanJson.GetOrElse("");
        UNIT_ASSERT_C(FindPlanNodeByKv(planJson, "ReadLimit", "10").IsDefined(), planStr);
    }

    Y_UNIT_TEST(SqlLimitWithoutForcePragma_DoesNotPushItemsLimit) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        const TString query = R"(
            --!syntax_v1
            PRAGMA Kikimr.OptEnableOlapPushdown = "true";
            PRAGMA Kikimr.OptForceOlapPushdownDistinct = "level";

            SELECT DISTINCT `level` FROM `/Root/olapStore/olapTable` LIMIT 10
        )";

        auto res = StreamExplainQuery(query, tableClient);
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        const auto planRes = CollectStreamResult(res);
        const TString ast = TString(planRes.QueryStats->Getquery_ast());
        UNIT_ASSERT_C(ast.find("KqpOlapDistinct") != TString::npos, ast);
        UNIT_ASSERT_C(ast.find("ItemsLimit") == TString::npos, ast);

        NJson::TJsonValue planJson;
        UNIT_ASSERT_C(NJson::ReadJsonTree(*planRes.PlanJson, &planJson, true), "Failed to parse plan json");
        const TString planStr = planRes.PlanJson.GetOrElse("");
        UNIT_ASSERT_C(FindPlanNodes(planJson, "ReadLimit").empty(), planStr);
    }

    Y_UNIT_TEST(SimpleDistinctWithoutLimit_PushesDistinctNoItemsLimit) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        const TString query = R"(
            --!syntax_v1
            PRAGMA Kikimr.OptEnableOlapPushdown = "true";
            PRAGMA Kikimr.OptForceOlapPushdownDistinct = "level";

            SELECT DISTINCT `level` FROM `/Root/olapStore/olapTable`
        )";

        auto res = StreamExplainQuery(query, tableClient);
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        const auto planRes = CollectStreamResult(res);
        const TString ast = TString(planRes.QueryStats->Getquery_ast());
        UNIT_ASSERT_C(ast.find("KqpOlapDistinct") != TString::npos, ast);
        UNIT_ASSERT_C(ast.find("ItemsLimit") == TString::npos, ast);

        NJson::TJsonValue planJson;
        UNIT_ASSERT_C(NJson::ReadJsonTree(*planRes.PlanJson, &planJson, true), "Failed to parse plan json");

        const TString planStr = planRes.PlanJson.GetOrElse("");
        UNIT_ASSERT_C(FindPlanNodes(planJson, "ReadLimit").empty(), planStr);

        const auto distinctNodes = FindPlanNodes(planJson, "Distinct");
        UNIT_ASSERT_C(!distinctNodes.empty(), planStr);
    }

    Y_UNIT_TEST(TwoColumnDistinct_DoesNotPush) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        const TString query = R"(
            --!syntax_v1
            PRAGMA Kikimr.OptEnableOlapPushdown = "true";

            SELECT DISTINCT `level`, `resource_id` FROM `/Root/olapStore/olapTable` LIMIT 10
        )";

        auto res = StreamExplainQuery(query, tableClient);
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        const auto planRes = CollectStreamResult(res);
        const TString ast = TString(planRes.QueryStats->Getquery_ast());
        UNIT_ASSERT_C(ast.find("KqpOlapDistinct") == TString::npos, ast);
    }

    Y_UNIT_TEST(FilteredDistinct_DoesNotPush) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        const TString query = R"(
            --!syntax_v1
            PRAGMA Kikimr.OptEnableOlapPushdown = "true";
            PRAGMA Kikimr.OptForceOlapPushdownDistinct = "level";

            SELECT DISTINCT `level` FROM `/Root/olapStore/olapTable` WHERE `level` > 0 LIMIT 10
        )";

        auto res = StreamExplainQuery(query, tableClient);
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        const auto planRes = CollectStreamResult(res);
        const TString ast = TString(planRes.QueryStats->Getquery_ast());
        UNIT_ASSERT_C(ast.find("KqpOlapDistinct") != TString::npos, ast);
    }

    // PK is (timestamp, uid): filter only on the first key column — allowed for pushdown.
    Y_UNIT_TEST(FilterOnFirstPkOnly_CompositeKey_PushesDistinct) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        const TString query = R"(
            --!syntax_v1
            PRAGMA Kikimr.OptEnableOlapPushdown = "true";
            PRAGMA Kikimr.OptForceOlapPushdownDistinct = "level";

            SELECT DISTINCT `level` FROM `/Root/olapStore/olapTable`
            WHERE `timestamp` >= DateTime::FromSeconds(100) AND `timestamp` < DateTime::FromSeconds(200)
            LIMIT 100
        )";

        auto res = StreamExplainQuery(query, tableClient);
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        const auto planRes = CollectStreamResult(res);
        const TString ast = TString(planRes.QueryStats->Getquery_ast());
        UNIT_ASSERT_C(ast.find("KqpOlapDistinct") != TString::npos, ast);
    }

    // PK is (timestamp, uid): predicate only on the second key column — no pushdown.
    Y_UNIT_TEST(FilterOnSecondPkOnly_CompositeKey_DoesNotPush) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        const TString query = R"(
            --!syntax_v1
            PRAGMA Kikimr.OptEnableOlapPushdown = "true";
            PRAGMA Kikimr.OptForceOlapPushdownDistinct = "level";

            SELECT DISTINCT `level` FROM `/Root/olapStore/olapTable`
            WHERE `uid` = "x" LIMIT 10
        )";

        auto res = StreamExplainQuery(query, tableClient);
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        const auto planRes = CollectStreamResult(res);
        const TString ast = TString(planRes.QueryStats->Getquery_ast());
        UNIT_ASSERT_C(ast.find("KqpOlapDistinct") != TString::npos, ast);
    }

    Y_UNIT_TEST(FilterOnOnlyPkColumn_SingleColumnPk_PushesDistinct) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperSinglePkShard(kikimr).CreateTestOlapTableSinglePkColumn();
        auto tableClient = kikimr.GetTableClient();

        const TString query = R"(
            --!syntax_v1
            PRAGMA Kikimr.OptEnableOlapPushdown = "true";
            PRAGMA Kikimr.OptForceOlapPushdownDistinct = "payload";

            SELECT DISTINCT `payload` FROM `/Root/olapStoreOnePk/onePkOlap`
            WHERE `timestamp` >= DateTime::FromSeconds(1) AND `timestamp` < DateTime::FromSeconds(10)
            LIMIT 50
        )";

        auto res = StreamExplainQuery(query, tableClient);
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        const auto planRes = CollectStreamResult(res);
        const TString ast = TString(planRes.QueryStats->Getquery_ast());
        UNIT_ASSERT_C(ast.find("KqpOlapDistinct") != TString::npos, ast);
    }
};

} // namespace NKikimr::NKqp
