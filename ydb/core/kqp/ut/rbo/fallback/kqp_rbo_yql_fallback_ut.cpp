#include <ydb/core/kqp/ut/olap/helpers/get_value.h>
#include <ydb/core/kqp/ut/olap/helpers/query_executor.h>
#include <ydb/core/kqp/ut/olap/helpers/local.h>
#include <ydb/core/kqp/ut/olap/helpers/writer.h>
#include <ydb/core/kqp/ut/olap/helpers/test_case.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/query_id.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/common/simple/settings.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/opt/rbo/kqp_plan_conversion_utils.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>
#include <ydb/core/kqp/ut/rbo/kqp_rbo_test_helpers.h>
#include <ydb/core/kqp/opt/rbo/traces/kqp_rbo_trace_output.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/kqp/query_data/kqp_prepared_query.h>
#include <ydb/core/statistics/ut_common/ut_common.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/aclib/aclib.h>
#include <yql/essentials/core/pg_settings/guc_settings.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/parser/pg_wrapper/interface/codec.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <ydb/public/lib/ut_helpers/ut_helpers_query.h>
#include <ydb/public/lib/ydb_cli/common/format.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/string/split.h>

#include <algorithm>
#include <array>
#include <ctime>
#include <optional>
#include <regex>
#include <fstream>
#include <utility>

// These headers import NKikimr::NKqp into the global namespace, so include them last
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_aggregation_builder.h>
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_join_builder.h>

#include <ydb/core/kqp/ut/rbo/kqp_rbo_yql_query_helpers.h>

namespace NKikimr::NKqp {
namespace {

using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYql::NNodes;
using namespace NStat;
using namespace NKikimr::NKqp::NYqlTests;

// Returns the `request` object of the [REQ_JSON] completed record whose query text contains `marker`.
std::optional<NJson::TJsonValue> FindReqJsonCompleted(TStringBuf logs, TStringBuf marker) {
    constexpr TStringBuf fieldPrefix = "requestJson=";
    for (TStringBuf line : StringSplitter(logs).Split('\n')) {
        const auto pos = line.find(fieldPrefix);
        if (!line.Contains("[REQ_JSON]") || pos == TStringBuf::npos) {
            continue;
        }
        std::string::size_type valuePos = pos + fieldPrefix.size();
        const auto value = NActors::NStructuredLog::TTextWriter::UnescapeFieldValue(TString(line), valuePos);
        NJson::TJsonValue json;
        if (!value || !NJson::ReadJsonTree(*value, &json, /*throwOnError=*/false)) {
            continue;
        }
        const auto& request = json["request"];
        if (request["event"].GetStringSafe("") == "completed" && request["data"].GetStringSafe("").Contains(marker)) {
            return request;
        }
    }
    return std::nullopt;
}

void CreateSimpleTable(TKikimrRunner &kikimr) {
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    auto result = session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/t1` (
            a Int64 NOT NULL,
	            b Int64,
            c Int64,
            primary key(a)
        );
    )").GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

bool HasFallbackIssue(const NYdb::NIssue::TIssues& issues) {
    for (const auto& issue : issues) {
        if (issue.GetSeverity() == NYdb::NIssue::ESeverity::Info &&
            issue.GetMessage().find("Compilation with the new RBO failed") != std::string::npos)
        {
            return true;
        }
    }
    return false;
}

void TestFallbackToYql(bool fallbackToYqlEnabled, const std::vector<std::string>& queries,
                       const std::vector<std::pair<ui32, ui32>>& expectedCompileCounters, const std::vector<bool>& expectedResult,
                       NQuery::EExecMode execMode) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(fallbackToYqlEnabled);

    TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
    CreateSimpleTable(kikimr);

    std::pair<ui32, ui32> intermediateResult{0, 0};
    for (ui32 i = 0; i < queries.size(); ++i) {
        auto queryClient = kikimr.GetQueryClient();
        const auto& query = queries[i];
        auto session = queryClient.GetSession().GetValueSync().GetSession();
        auto result =
            session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(execMode))
                .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.IsSuccess(), expectedResult[i], result.GetIssues().ToString());

        const bool expectedFallbackIssue = fallbackToYqlEnabled && expectedCompileCounters[i].second == 1;
        UNIT_ASSERT_VALUES_EQUAL_C(HasFallbackIssue(result.GetIssues()), expectedFallbackIssue,
                                   result.GetIssues().ToString());

        intermediateResult.first += expectedCompileCounters[i].first;
        intermediateResult.second += expectedCompileCounters[i].second;
        UNIT_ASSERT_VALUES_EQUAL(GetNewRBOCompileCounters(kikimr), intermediateResult);
    }
}

std::vector<std::string> GetQueriesToTestFallbackToYql() {
    std::vector<std::string> queries = {
        // Insert is not supported.
        R"(
            INSERT INTO `/Root/t1` (a, b, c) VALUES (1, 2, 3);
        )",
        // Simple supported query in new RBO.
        R"(
            select t1.a from `/Root/t1` as t1;
        )",
        // Simple query with a fallback pragma
        R"(
            PRAGMA ydb.OptFallbackToLegacyOptimizer = 'true';
            select t1.a from `/Root/t1` as t1;
        )"
    };

    return queries;
}

std::vector<std::pair<ui32, ui32>> GetCompileCountersToTestFallbackToYql() {
    // Represents the number of successes and fails for each query with new RBO compiler pipeline.
    std::vector<std::pair<ui32, ui32>> expectedCompileCounters = {
        {0, 1},
        {1, 0},
        {0, 1}
    };

    return expectedCompileCounters;
}

void TestLegacyOptimizerWithStats(const TString& query, bool fallbackEnabled, bool profile) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
    appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(fallbackEnabled);
    appConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
    CreateSimpleTable(kikimr);

    TValueBuilder rows;
    rows.BeginList();
    for (i64 key : {1, 2}) {
        rows.AddListItem().BeginStruct()
            .AddMember("a").Int64(key)
            .AddMember("b").Int64(key + 1)
            .AddMember("c").Int64(key + 2)
            .EndStruct();
    }
    rows.EndList();
    auto upsert = kikimr.GetTableClient().BulkUpsert("/Root/t1", rows.Build()).GetValueSync();
    UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());

    auto queryClient = kikimr.GetQueryClient();
    auto session = queryClient.GetSession().GetValueSync().GetSession();
    const auto countersBefore = GetNewRBOCompileCounters(kikimr);
    for (bool fromCache : {false, true}) {
        auto result = session.ExecuteQuery(query, NQuery::TTxControl::NoTx(),
            NQuery::TExecuteQuerySettings().StatsMode(profile ? NQuery::EStatsMode::Profile : NQuery::EStatsMode::Full))
            .ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), "[[1]]");
        UNIT_ASSERT(result.GetStats().has_value());
        UNIT_ASSERT(result.GetStats()->GetPlan().has_value());

        const TString plan = *result.GetStats()->GetPlan();
        NJson::TJsonValue json;
        UNIT_ASSERT_C(NJson::ReadJsonTree(plan, &json, true), plan);
        UNIT_ASSERT_C(json.Has("SimplifiedPlan"), plan);
        UNIT_ASSERT_VALUES_EQUAL(json["Plan"]["Node Type"].GetStringSafe(), "Query");
        UNIT_ASSERT_VALUES_EQUAL(json["Plan"]["Stats"]["Compilation"]["FromCache"].GetBooleanSafe(), fromCache);
    }

    const auto countersAfter = GetNewRBOCompileCounters(kikimr);
    UNIT_ASSERT_VALUES_EQUAL(countersAfter.first, countersBefore.first);
    UNIT_ASSERT_VALUES_EQUAL(countersAfter.second, countersBefore.second + (fallbackEnabled ? 1 : 0));
}

} // namespace

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYqlTests;

Y_UNIT_TEST_SUITE(KqpRboYql) {

    Y_UNIT_TEST(QualifiedStarSubqueryFallsBackToYqlOptimizer) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());

        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        auto schemeResult = tableSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `doc` (
                `id` String,
                `flag` Bool,
                PRIMARY KEY (`id`)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        rows.AddListItem().BeginStruct()
            .AddMember("id").String("disabled")
            .AddMember("flag").Bool(false)
            .EndStruct();
        rows.AddListItem().BeginStruct()
            .AddMember("id").String("enabled")
            .AddMember("flag").Bool(true)
            .EndStruct();
        rows.EndList();

        auto upsertResult = tableClient.BulkUpsert("/Root/doc", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();
        const auto compileCountersBefore = GetNewRBOCompileCounters(kikimr);
        auto result = querySession.ExecuteQuery(R"(
            SELECT `d`.`id` FROM (SELECT `d_src`.* FROM `doc` AS `d_src` WHERE `d_src`.`flag`) AS `d`;
        )",
            NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().StatsMode(NYdb::NQuery::EStatsMode::Full)
        ).ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), R"([[["enabled"]]])");
        UNIT_ASSERT_C(result.GetStats().has_value(), "Missing full query statistics");
        UNIT_ASSERT_C(result.GetStats()->GetPlan().has_value(), "Missing query plan in full statistics");

        const auto plan = TString{*result.GetStats()->GetPlan()};
        NJson::TJsonValue planJson;
        UNIT_ASSERT_C(NJson::ReadJsonTree(plan, &planJson, true), plan);
        UNIT_ASSERT_C(planJson.GetMapSafe().contains("SimplifiedPlan"), plan);
        const auto& planRoot = planJson.GetMapSafe().at("Plan").GetMapSafe();
        UNIT_ASSERT_VALUES_EQUAL_C(planRoot.at("Node Type").GetStringSafe(), "Query", plan);
        UNIT_ASSERT_C(planRoot.contains("Stats"), plan);

        const auto compileCountersAfter = GetNewRBOCompileCounters(kikimr);
        UNIT_ASSERT_VALUES_EQUAL(compileCountersAfter.first, compileCountersBefore.first);
        UNIT_ASSERT_VALUES_EQUAL(compileCountersAfter.second, compileCountersBefore.second + 1);
    }

    Y_UNIT_TEST(ReqJsonLogsActualOptimizer) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(true);
        appConfig.MutableTableServiceConfig()->SetDefaultLangVer(NYql::GetMaxLangVersion());

        TCapturedLog logs(/* appendNewline */ true);
        auto settings = NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false).SetLogCapture(logs);
        // Successful queries are logged to [REQ_JSON] at DEBUG.
        settings.LogSettings = TTestLogSettings().AddLogPriority(NKikimrServices::KQP_REQUEST, NActors::NLog::PRI_DEBUG);
        settings.LogSettings->DefaultLogPriority = NActors::NLog::PRI_CRIT;

        TKikimrRunner kikimr(settings);
        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        auto schemeResult = tableSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `doc` (
                `id` String,
                `flag` Bool,
                PRIMARY KEY (`id`)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        // Log records are written asynchronously, so wait for the one of the query marked with `marker`.
        const auto getCompletedRequest = [&](TStringBuf marker) -> NJson::TJsonValue {
            const TInstant deadline = TInstant::Now() + TDuration::Seconds(10);
            while (true) {
                const TString text = logs.Snapshot();

                if (auto request = FindReqJsonCompleted(text, marker)) {
                    UNIT_ASSERT_C(request->Has("used_new_rbo"), request->GetStringRobust());
                    return std::move(*request);
                }

                UNIT_ASSERT_C(TInstant::Now() < deadline, "No [REQ_JSON] record for " << marker << " in logs:\n" << text);
                Sleep(TDuration::MilliSeconds(100));
            }
        };

        auto queryClient = kikimr.GetQueryClient();
        auto querySession = queryClient.GetSession().GetValueSync().GetSession();

        auto newRboResult = querySession.ExecuteQuery(R"(
            /* req-json-new-rbo */
            SELECT `id` FROM `doc` WHERE `flag`;
        )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(newRboResult.IsSuccess(), newRboResult.GetIssues().ToString());
        const auto newRboRequest = getCompletedRequest("req-json-new-rbo");
        UNIT_ASSERT_C(newRboRequest["used_new_rbo"].GetBooleanSafe(), newRboRequest.GetStringRobust());

        // New RBO does not support this query, so it is compiled again with the old one.
        auto fallbackResult = querySession.ExecuteQuery(R"(
            /* req-json-fallback */
            SELECT `d`.`id` FROM (SELECT `d_src`.* FROM `doc` AS `d_src` WHERE `d_src`.`flag`) AS `d`;
        )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(fallbackResult.IsSuccess(), fallbackResult.GetIssues().ToString());
        const auto fallbackRequest = getCompletedRequest("req-json-fallback");
        UNIT_ASSERT_C(!fallbackRequest["used_new_rbo"].GetBooleanSafe(), fallbackRequest.GetStringRobust());

        auto allNewRboResult = querySession.ExecuteQuery(R"(
            /* req-json-all-new-rbo */
            SELECT `id` FROM `doc` WHERE `flag`;
            SELECT `id` FROM `doc` WHERE NOT `flag`;
        )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(allNewRboResult.IsSuccess(), allNewRboResult.GetIssues().ToString());
        const auto allNewRboRequest = getCompletedRequest("req-json-all-new-rbo");
        UNIT_ASSERT_C(allNewRboRequest["used_new_rbo"].GetBooleanSafe(), allNewRboRequest.GetStringRobust());

        // Statements are compiled together, so one unsupported statement sends the whole query to the old RBO.
        auto mixedResult = querySession.ExecuteQuery(R"(
            /* req-json-mixed */
            SELECT `id` FROM `doc` WHERE `flag`;
            SELECT `d`.`id` FROM (SELECT `d_src`.* FROM `doc` AS `d_src` WHERE `d_src`.`flag`) AS `d`;
        )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(mixedResult.IsSuccess(), mixedResult.GetIssues().ToString());
        const auto mixedRequest = getCompletedRequest("req-json-mixed");
        UNIT_ASSERT_C(!mixedRequest["used_new_rbo"].GetBooleanSafe(), mixedRequest.GetStringRobust());
    }

    Y_UNIT_TEST(FallbackToYqlEnabled) {
        // All queries should succeded because fallback to yql is enabled.
        const std::vector<bool> expectedResult{true, true, true};
        TestFallbackToYql(/*fallbackToYqlEnabled=*/true, GetQueriesToTestFallbackToYql(), GetCompileCountersToTestFallbackToYql(),
                          expectedResult, NQuery::EExecMode::Explain);
    }

    Y_UNIT_TEST(FallbackToYqlDisabled) {
        // First 2 queries should fail because fallback to yql is disabled.
        const std::vector<bool> expectedResult{false, true, false};
        TestFallbackToYql(/*fallbackToYqlEnabled=*/false, GetQueriesToTestFallbackToYql(), GetCompileCountersToTestFallbackToYql(),
                          expectedResult, NQuery::EExecMode::Explain);
    }

    Y_UNIT_TEST(FallbackToYqlEnabledExecute) {
        // All queries should succeded because fallback to yql is enabled.
        const std::vector<bool> expectedResult{true, true, true};
        TestFallbackToYql(/*fallbackToYqlEnabled=*/true, GetQueriesToTestFallbackToYql(), GetCompileCountersToTestFallbackToYql(),
                          expectedResult, NQuery::EExecMode::Execute);
    }

    Y_UNIT_TEST_TWIN(SysViewNodes, NewRbo) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(NewRbo);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false).SetNodeCount(3));
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();
        const auto firstNode = kikimr.GetTestServer().GetRuntime()->GetNodeId(0);
        const auto countersBefore = GetNewRBOCompileCounters(kikimr);

        const TVector<std::pair<TString, TString>> queries = {
            // The test runtime reports zero CPU threads; the sum must still be non-NULL.
            {"SELECT SUM(CpuThreads) FROM `/Root/.sys/nodes`;", "[[[0u]]]"},
            {"SELECT COUNT(*) FROM `/Root/.sys/nodes`;", "[[3u]]"},
            {"SELECT NodeId, Host FROM `/Root/.sys/nodes` ORDER BY NodeId;",
                Sprintf(R"([[[%du];["::1"]];[[%du];["::1"]];[[%du];["::1"]]])",
                    firstNode, firstNode + 1, firstNode + 2)},
            {Sprintf("SELECT NodeId FROM `/Root/.sys/nodes` WHERE NodeId = %du;", firstNode + 1),
                Sprintf("[[[%du]]]", firstNode + 1)},
            {Sprintf("SELECT NodeId FROM `/Root/.sys/nodes` WHERE NodeId > %du ORDER BY NodeId DESC;", firstNode),
                Sprintf("[[[%du]];[[%du]]]", firstNode + 2, firstNode + 1)},
            {Sprintf("SELECT SUM(CpuThreads), COUNT(*) FROM `/Root/.sys/nodes` WHERE NodeId > %du;", firstNode + 2),
                "[[#;0u]]"},
        };
        for (const auto& [query, expected] : queries) {
            auto result = session.ExecuteQuery(query, NQuery::TTxControl::NoTx(),
                NQuery::TExecuteQuerySettings().StatsMode(NQuery::EStatsMode::Full)).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), query << ": " << result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(FormatResultSetYson(result.GetResultSet(0)), expected, query);
        }

        auto scan = kikimr.GetTableClient().StreamExecuteScanQuery(queries.front().first).GetValueSync();
        UNIT_ASSERT_C(scan.IsSuccess(), scan.GetIssues().ToString());
        CompareYson(queries.front().second, StreamResultToYson(scan));

        const auto countersAfter = GetNewRBOCompileCounters(kikimr);
        UNIT_ASSERT_VALUES_EQUAL(countersAfter.second, countersBefore.second);
        UNIT_ASSERT_VALUES_EQUAL(countersAfter.first - countersBefore.first, NewRbo ? queries.size() + 1 : 0);
    }

    Y_UNIT_TEST_TWIN(SemiJoinFallbackWithStats, Profile) {
        TestLegacyOptimizerWithStats(R"(
            SELECT l.a FROM `/Root/t1` AS l
            LEFT SEMI JOIN `/Root/t1` AS r ON l.b = r.a;
        )", /*fallbackEnabled=*/true, Profile);
    }

    Y_UNIT_TEST_TWIN(SamplingWithStatsUsesLegacyOptimizer, Profile) {
        // Sampling selects the legacy pipeline inside the host, without an
        // unsuccessful RBO compilation or an error-fallback retry.
        TestLegacyOptimizerWithStats(R"(
            SELECT a FROM `/Root/t1`
            WITH (sampling_rate="1", sampling_seed="42", sampling_memtable_stride="1")
            ORDER BY a LIMIT 1;
        )", /*fallbackEnabled=*/false, Profile);
    }

    Y_UNIT_TEST(FallbackToYqlDisabledExecute) {
        // First 2 queries should fail because fallback to yql is disabled.
        const std::vector<bool> expectedResult{false, true, false};
        TestFallbackToYql(/*fallbackToYqlEnabled=*/false, GetQueriesToTestFallbackToYql(), GetCompileCountersToTestFallbackToYql(),
                          expectedResult, NQuery::EExecMode::Execute);
    }

}

} // namespace NKikimr::NKqp
