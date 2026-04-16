#include "../helpers/aggregation.h"
#include "../helpers/local.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpOlapDistinctPushdown) {
    Y_UNIT_TEST(SimpleDistinctWithLimit_PushesDistinctAndReadLimit) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapPushdownDistinct(true);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        const TString query = R"(
            --!syntax_v1
            PRAGMA Kikimr.OptEnableOlapPushdown = "true";
            PRAGMA Kikimr.OptEnableOlapPushdownDistinct = "true";

            SELECT DISTINCT `level` FROM `/Root/olapStore/olapTable` LIMIT 10
        )";

        auto res = StreamExplainQuery(query, tableClient);
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        const auto planRes = CollectStreamResult(res);
        const TString ast = TString(planRes.QueryStats->Getquery_ast());
        UNIT_ASSERT_C(ast.find("KqpOlapDistinct") != TString::npos, ast);

        NJson::TJsonValue planJson;
        UNIT_ASSERT_C(NJson::ReadJsonTree(*planRes.PlanJson, &planJson, true), "Failed to parse plan json");

        UNIT_ASSERT_C(FindPlanNodeByKv(planJson, "ReadLimit", "10").IsDefined(), planRes.PlanJson.GetOrElse(""));

        const auto distinctNodes = FindPlanNodes(planJson, "Distinct");
        UNIT_ASSERT_C(!distinctNodes.empty(), planRes.PlanJson.GetOrElse(""));
    }

    Y_UNIT_TEST(TwoColumnDistinct_DoesNotPush) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapPushdownDistinct(true);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        const TString query = R"(
            --!syntax_v1
            PRAGMA Kikimr.OptEnableOlapPushdown = "true";
            PRAGMA Kikimr.OptEnableOlapPushdownDistinct = "true";

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
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapPushdownDistinct(true);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        const TString query = R"(
            --!syntax_v1
            PRAGMA Kikimr.OptEnableOlapPushdown = "true";
            PRAGMA Kikimr.OptEnableOlapPushdownDistinct = "true";

            SELECT DISTINCT `level` FROM `/Root/olapStore/olapTable` WHERE `level` > 0 LIMIT 10
        )";

        auto res = StreamExplainQuery(query, tableClient);
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        const auto planRes = CollectStreamResult(res);
        const TString ast = TString(planRes.QueryStats->Getquery_ast());
        UNIT_ASSERT_C(ast.find("KqpOlapDistinct") == TString::npos, ast);
    }
};

} // namespace NKikimr::NKqp
