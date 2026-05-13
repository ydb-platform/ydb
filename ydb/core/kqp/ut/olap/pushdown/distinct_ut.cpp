#include "../helpers/aggregation.h"
#include "../helpers/local.h"
#include "../helpers/writer.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/string/split.h>

namespace NKikimr::NKqp {

namespace {

static TIntrusivePtr<NMonitoring::TDynamicCounters> ResolveScanCounterGroup(TIntrusivePtr<NMonitoring::TDynamicCounters> root, const TString& path) {
    TVector<TString> parts;
    Split(path, "/", parts);
    if (parts.empty()) {
        return nullptr;
    }
    const TString service = parts[0];
    auto current = GetServiceCounters(root, service);
    if (!current) {
        return nullptr;
    }
    for (size_t i = 1; i + 1 < parts.size(); i += 2) {
        const TString& key = parts[i];
        const TString& value = parts[i + 1];
        current = current->FindSubgroup(key, value);
        if (!current) {
            return nullptr;
        }
    }
    return current;
}

static i64 ReadDistinctLimitSyncPointInvocations(TKikimrRunner& kikimr) {
    auto* runtime = kikimr.GetTestServer().GetRuntime();
    UNIT_ASSERT(runtime != nullptr);
    auto root = runtime->GetAppData().Counters;
    UNIT_ASSERT(root != nullptr);
    auto group = ResolveScanCounterGroup(root, "tablets/subsystem/columnshard/module_id/Scan");
    UNIT_ASSERT_C(group != nullptr, "Scan counter subgroup not found");
    auto counter = group->FindCounter("Deriviative/DistinctLimit/SyncPoint/Invocations");
    UNIT_ASSERT_C(counter != nullptr, "DistinctLimit sync point counter not found");
    return counter->Val();
}

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
        UNIT_ASSERT_C(
            issues.Contains("does not match DISTINCT key column") || issues.Contains("cannot be validated against DISTINCT key"),
            issues
        );
    }

    Y_UNIT_TEST(JsonValueDistinctWithLimit_PushesProjectionsAndDistinct) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        auto queryClient = kikimr.GetQueryClient();
        auto result = queryClient.GetSession().GetValueSync();
        NYdb::NStatusHelpers::ThrowOnError(result);
        auto querySession = result.GetSession();

        auto res = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/foo_json_distinct` (
                a Int64 NOT NULL,
                b Int32,
                payload JsonDocument,
                primary key(a)
            )
            PARTITION BY HASH(a)
            WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        auto insertRes = querySession.ExecuteQuery(R"(
            INSERT INTO `/Root/foo_json_distinct` (a, b, payload)
            VALUES (1, 1, JsonDocument('{"a.b.c" : "a1"}'));
            INSERT INTO `/Root/foo_json_distinct` (a, b, payload)
            VALUES (2, 11, JsonDocument('{"a.b.c" : "a2"}'));
            INSERT INTO `/Root/foo_json_distinct` (a, b, payload)
            VALUES (3, 11, JsonDocument('{"a.b.c" : "a3"}'));
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(insertRes.IsSuccess(), insertRes.GetIssues().ToString());

        const TString query = R"(
            --!syntax_v1
            PRAGMA Kikimr.OptEnableOlapPushdown = "true";
            PRAGMA Kikimr.OptEnableOlapPushdownProjections = "true";
            PRAGMA Kikimr.OptForceOlapPushdownDistinct = "jsonDoc";
            PRAGMA Kikimr.OptForceOlapPushdownDistinctLimit = "10";

            SELECT DISTINCT JSON_VALUE(payload, "$.\"a.b.c\"") AS jsonDoc
            FROM `/Root/foo_json_distinct`
            LIMIT 10
        )";

        auto explainRes = StreamExplainQuery(query, tableClient);
        UNIT_ASSERT_C(explainRes.IsSuccess(), explainRes.GetIssues().ToString());
        const auto planRes = CollectStreamResult(explainRes);

        const TString ast = TString(planRes.QueryStats->Getquery_ast());
        UNIT_ASSERT_C(ast.find("KqpOlapDistinct") != TString::npos, ast);
        UNIT_ASSERT_C(ast.find("KqpOlapProjections") != TString::npos || ast.find("KqpOlapProjection") != TString::npos, ast);

        NJson::TJsonValue planJson;
        UNIT_ASSERT_C(NJson::ReadJsonTree(*planRes.PlanJson, &planJson, true), "Failed to parse plan json");
        const TString planStr = planRes.PlanJson.GetOrElse("");

        // DISTINCT is lowered to aggregate / shuffle over key columns in JSON plans (no literal "Distinct" node).
        UNIT_ASSERT_C(planStr.find("jsonDoc") != TString::npos && planStr.find("Aggregate") != TString::npos, planStr);
        UNIT_ASSERT_C(
            FindPlanNodeByKv(planJson, "ReadLimit", "10").IsDefined() || FindPlanNodeByKv(planJson, "Limit", "10").IsDefined(),
            planStr
        );
        // Distinct-limit sync point counter: see DistinctLimitSyncPoint_IncrementsScanCounter (physical key).
        // SSA/projection distinct keys use numeric field names in the stage batch (same as ToGeneralContainer).
    }

    Y_UNIT_TEST(ForceDistinctPragmas_DoNotBreak_SumDistinctWithAggPushdown) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        const TString query = R"(
            --!syntax_v1
            PRAGMA Kikimr.OptEnableOlapPushdown = "true";
            PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";

            PRAGMA Kikimr.OptForceOlapPushdownDistinct = "msg";
            PRAGMA Kikimr.OptForceOlapPushdownDistinctLimit = "10";

            SELECT SUM(DISTINCT `level`) FROM `/Root/olapStore/olapTable`
        )";

        auto res = StreamExplainQuery(query, tableClient);
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        const auto planRes = CollectStreamResult(res);
        const TString ast = TString(planRes.QueryStats->Getquery_ast());
        UNIT_ASSERT_C(ast.find("KqpOlapDistinct") == TString::npos, ast);
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

    Y_UNIT_TEST(FilteredDistinct_ForcePushdown_InjectsOlapDistinct) {
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

    // PK is (timestamp, uid): predicate only on the second key column blocks natural pushdown; pragma still injects OlapDistinct.
    Y_UNIT_TEST(FilterOnSecondPkOnly_CompositeKey_ForcePushdown_InjectsOlapDistinct) {
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

    Y_UNIT_TEST(SumDistinct_GroupBy_NoKqpOlapDistinctWithoutForce) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        const TString query = R"(
            --!syntax_v1
            PRAGMA Kikimr.OptEnableOlapPushdown = "true";

            SELECT `resource_id`, SUM(DISTINCT `level`) AS s
            FROM `/Root/olapStore/olapTable`
            GROUP BY `resource_id`
        )";

        auto res = StreamExplainQuery(query, tableClient);
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        const auto planRes = CollectStreamResult(res);
        const TString ast = TString(planRes.QueryStats->Getquery_ast());
        UNIT_ASSERT_C(ast.find("KqpOlapDistinct") == TString::npos, ast);
    }

    Y_UNIT_TEST(SumDistinct_NoGroupBy_NoKqpOlapDistinctWithoutForce) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        const TString query = R"(
            --!syntax_v1
            PRAGMA Kikimr.OptEnableOlapPushdown = "true";

            SELECT SUM(DISTINCT `level`) FROM `/Root/olapStore/olapTable`
        )";

        auto res = StreamExplainQuery(query, tableClient);
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        const auto planRes = CollectStreamResult(res);
        const TString ast = TString(planRes.QueryStats->Getquery_ast());
        UNIT_ASSERT_C(ast.find("KqpOlapDistinct") == TString::npos, ast);
    }

    Y_UNIT_TEST(CountDistinct_GroupBy_NoKqpOlapDistinctWithoutForce) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        const TString query = R"(
            --!syntax_v1
            PRAGMA Kikimr.OptEnableOlapPushdown = "true";

            SELECT `resource_id`, COUNT(DISTINCT `level`) AS c
            FROM `/Root/olapStore/olapTable`
            GROUP BY `resource_id`
        )";

        auto res = StreamExplainQuery(query, tableClient);
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        const auto planRes = CollectStreamResult(res);
        const TString ast = TString(planRes.QueryStats->Getquery_ast());
        UNIT_ASSERT_C(ast.find("KqpOlapDistinct") == TString::npos, ast);
    }

    Y_UNIT_TEST(DistinctLimitSyncPoint_IncrementsScanCounter) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 100);

        auto queryClient = kikimr.GetQueryClient();
        auto sessionRes = queryClient.GetSession().GetValueSync();
        UNIT_ASSERT_C(sessionRes.IsSuccess(), sessionRes.GetIssues().ToString());
        auto session = sessionRes.GetSession();

        const TString query = R"(
            --!syntax_v1
            PRAGMA Kikimr.OptEnableOlapPushdown = "true";
            PRAGMA Kikimr.OptForceOlapPushdownDistinct = "level";
            PRAGMA Kikimr.OptForceOlapPushdownDistinctLimit = "10";

            SELECT DISTINCT `level` FROM `/Root/olapStore/olapTable` LIMIT 10
        )";

        const i64 before = ReadDistinctLimitSyncPointInvocations(kikimr);
        auto it = session.StreamExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        const auto collected = CollectStreamResult(it);
        UNIT_ASSERT_C(collected.RowsCount > 0, collected.ResultSetYson);

        const i64 after = ReadDistinctLimitSyncPointInvocations(kikimr);
        UNIT_ASSERT_C(after > before, TStringBuilder() << "sync point counter: before=" << before << " after=" << after);
    }
};

} // namespace NKikimr::NKqp
