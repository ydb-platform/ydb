#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <library/cpp/json/writer/json.h>
#include <yql/essentials/public/langver/yql_langver.h>
#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/system/datetime.h>
#include <util/string/split.h>

#include <regex>
#include <set>

namespace NKikimr::NKqp {
namespace {

// Describe the existing RBO benchmark fixtures, not a second set of expected
// plans. Schema/statistics names also determine the canonical snapshot suffix,
// so profile selection must stay aligned with the original canonization suite.
struct TBenchmark {
    TString Name;
    TString QueryDirectory;
    TString Schema;
    TString NormalStatistics;
    TString SmallStatistics;
    ui32 QueryCount;
};

TString ReadRboData(const TString& path) {
    return TIFStream(SRC_("../../rbo/data/" + path)).ReadAll();
}

// Reuse the original filename convention, including scale and column storage.
// A snapshot for another statistics scale is not a valid expected join order.
TString CanonicalPath(const TBenchmark& benchmark, const TString& statistics, ui32 queryId) {
    const TString statisticsName = statistics == "normal" ? benchmark.NormalStatistics : benchmark.SmallStatistics;
    const TString scale = statisticsName.substr(benchmark.Schema.size());
    return SRC_(TStringBuilder() << "../../rbo/data/join_order/" << benchmark.Schema << queryId
        << "_" << scale << "_column_store.json");
}

// Keep join algorithms and shuffle placement in the oracle, not just table
// leaf order. Serialize for useful failure output; comparison below is JSON
// structure equality, so irrelevant indentation does not affect correctness.
TString FormatJoinOrder(const TString& plan) {
    const auto order = GetDetailedJoinOrder(plan, TGetPlanParams{.IncludeShuffles = true});
    UNIT_ASSERT_C(order.IsMap() && order.Has("op_name"), "Missing join-order tree in plan:\n" << plan);
    TStringStream stream;
    NJsonWriter::TBuf writer;
    writer.SetIndentSpaces(2);
    writer.WriteJsonValue(&order);
    writer.FlushTo(&stream);
    stream << Endl;
    return stream.Str();
}

// The compile actor attaches the SDK request trace ID to KQP_YQL records.
// Check only that request, including all four frontend fallback stages.
void RequireNativeLowering(const TCapturedLog& logs, size_t logStart, const TString& traceId, const TString& context) {
    const TString tag = "TraceId: " + traceId + ", ";
    // Logs arrive asynchronously after query completion. Wait only for this
    // request's success/fallback evidence; another query's native success must
    // never make an unsupported workload appear to have lowered natively.
    const auto deadline = TInstant::Now() + TDuration::Seconds(10);
    TString evidence;
    size_t nativeRoots = 0;
    bool fallback = false;
    do {
        evidence.clear();
        nativeRoots = 0;
        fallback = false;
        TVector<TString> lines;
        Split(logs.Snapshot().substr(logStart), "\n", lines);
        for (const auto& line : lines) {
            if (!line.Contains(tag) || !line.Contains("New YQL ")) {
                continue;
            }
            evidence += line + "\n";
            nativeRoots += line.Contains("New YQL RBO lowering succeeded");
            fallback |= line.Contains(" fallback:");
        }
        if (nativeRoots || fallback) {
            break;
        }
        Sleep(TDuration::MilliSeconds(10));
    } while (TInstant::Now() < deadline);
    // Exactly one native root is required for each single-result fixture.
    // Disabling optimizer fallback alone cannot prove this: the frontend can
    // still feed the legacy-converted plan into the same RBO optimizer.
    UNIT_ASSERT_C(!fallback && nativeRoots == 1,
        context << ": native AST-to-RBO lowering required before canonical comparison; native roots=" << nativeRoots
        << "\n" << (evidence.empty() ? TString("No frontend evidence for request ") + traceId : evidence));
}

// JOIN_ORDER_QUERIES selects comma-separated query IDs and overrides the disabled list.
// JOIN_ORDER_STATISTICS selects normal (default) or small.
// Original RBO snapshots are read-only correctness oracles for this frontend.
void RunJoinOrderBenchmark(const TBenchmark& benchmark, const std::set<ui32>& disabled = {},
    const std::set<ui32>& multipleStatements = {}) {
    std::set<ui32> selected;
    const TString selection = GetTestParam("JOIN_ORDER_QUERIES");
    if (!selection.empty()) {
        TVector<TString> names;
        Split(selection, ",", names);
        for (const auto& name : names) {
            ui32 id = 0;
            UNIT_ASSERT_C(TryFromString(name, id) && id >= 1 && id <= benchmark.QueryCount,
                "Invalid " << benchmark.Name << " query ID: " << name);
            UNIT_ASSERT_C(!multipleStatements.contains(id),
                benchmark.Name << " Q" << id << " has multiple result statements");
            selected.insert(id);
        }
    }
    TString profile = GetTestParam("JOIN_ORDER_STATISTICS");
    if (profile.empty()) {
        profile = "normal";
    }
    UNIT_ASSERT_C(profile == "normal" || profile == "small",
        "JOIN_ORDER_STATISTICS must be normal or small");
    // These are correctness checks against an independent existing oracle.
    // Reject canonization requests so a frontend regression cannot rewrite its
    // expected output and hide a mismatch.
    TString canonize = GetTestParam("CANONIZE_JOIN_ORDER_TESTS");
    canonize.to_lower();
    UNIT_ASSERT_C(canonize.empty() || canonize == "false",
        "New-parser checks cannot rewrite the original RBO canonical join orders");

    TString errors;
    const TString statisticsPath = "stats/" + (profile == "normal"
        ? benchmark.NormalStatistics : benchmark.SmallStatistics) + ".json";
    // A missing profile must fail clearly rather than reuse another scale
    // and compare plans produced under different optimizer assumptions.
    const TString fullStatisticsPath = SRC_("../../rbo/data/" + statisticsPath);
    UNIT_ASSERT_C(TFsPath(fullStatisticsPath).Exists(),
        "Missing " << profile << " statistics: " << fullStatisticsPath);

    // Match the original column-store canonization setup: the same statistics,
    // CBO level and order optimization must be active or a join-order difference
    // could come from configuration instead of frontend lowering.
    NKikimrConfig::TAppConfig appConfig;
    auto* config = appConfig.MutableTableServiceConfig();
    config->SetEnableNewRBO(true);
    config->SetEnableNewYqlParser(true);
    config->SetEnableFallbackToYqlOptimizer(false);
    config->SetEnableFallbackOnMultipleStatements(false);
    config->SetEnableKqpDataQueryStreamIdxLookupJoin(false);
    config->SetEnableOrderOptimizaionFSM(true);
    config->SetDefaultCostBasedOptimizationLevel(4);
    config->SetCompileTimeoutMs(TDuration::Minutes(1).MilliSeconds());
    config->SetAllowOlapDataQuery(true);
    config->SetDefaultLangVer(NYql::GetMaxLangVersion());
    config->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

    NKikimrKqp::TKqpSetting statistics;
    statistics.SetName("OptOverrideStatistics");
    statistics.SetValue(ReadRboData(statisticsPath));
    TCapturedLog logs(true);
    auto settings = TKikimrSettings(appConfig).SetWithSampleTables(false).SetKqpSettings({statistics}).SetLogCapture(logs);
    settings.LogSettings = TTestLogSettings().AddLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_INFO);
    settings.LogSettings->DefaultLogPriority = NActors::NLog::PRI_CRIT;
    settings.FeatureFlags.SetEnableSeparationComputeActorsFromRead(true);
    TKikimrRunner kikimr(settings);
    auto sessionResult = kikimr.GetQueryClient().GetSession().GetValueSync();
    UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
    auto session = sessionResult.GetSession();

    // Apply the original fixture's column-store/partition options in memory.
    // Keep the checked-in schema and canonical snapshots untouched.
    const std::regex tables(R"(CREATE TABLE [^\(]+ \([^;]*\))", std::regex::multiline);
    const std::string schema = std::regex_replace(std::string(ReadRboData("schema/" + benchmark.Schema + ".sql")), tables,
        "$& WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16);");
    auto create = session.ExecuteQuery(TString(schema), NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
    UNIT_ASSERT_C(create.IsSuccess(), create.GetIssues().ToString());

    ui32 checked = 0;
    ui32 matched = 0;
    for (ui32 id = 1; id <= benchmark.QueryCount; ++id) {
        if (!selected.empty() && !selected.contains(id)) {
            continue;
        }
        if (multipleStatements.contains(id) || (selected.empty() && disabled.contains(id))) {
            Cerr << "Skipping " << benchmark.Name << " Q" << id << Endl;
            continue;
        }
        const TString context = TStringBuilder() << benchmark.Name << "/" << profile << "/q" << id;
        Cerr << "Checking native join order: " << context << Endl;
        ++checked;
        try {
            const TString path = CanonicalPath(benchmark, profile, id);
            NJson::TJsonValue expectedOrder;
            const TString expected = TIFStream(path).ReadAll();
            NJson::ReadJsonTree(expected, &expectedOrder, true);
            UNIT_ASSERT_C(expectedOrder.IsMap() && expectedOrder.Has("op_name"), "Invalid canonical join order: " << path);
            // Preserve the original helper preamble, including named lambda bindings.
            // If the frontend cannot represent it, RequireNativeLowering must fail;
            // removing helpers here would test a different workload from the oracle.
            const TString query = R"(
                $to_decimal = ($x) -> { return cast($x as Decimal(12, 2)); };
                $to_decimal_max_precision = ($x) -> { return cast($x as Decimal(35, 2)); };
                $round = ($x,$y) -> {return $x;};
            )" + ReadRboData(benchmark.QueryDirectory + "/q" + ToString(id) + ".yql");
            // Bind frontend evidence to this request. A previous query or a
            // background metadata query must never satisfy the native guard.
            const TString traceId = TStringBuilder() << "new-parser-join-order-" << benchmark.Name << '-' << profile << "-q" << id;
            const size_t logStart = logs.Snapshot().size();
            auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)
                    .TraceId(std::string(traceId))).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            // Prove native import before examining the canonical plan. Legacy
            // fallback could produce the expected join order perfectly while the
            // new AST-to-RBO path never ran.
            RequireNativeLowering(logs, logStart, traceId, context);
            UNIT_ASSERT_C(result.GetStats() && result.GetStats()->GetPlan(), "Missing explain plan");
            const TString actual = FormatJoinOrder(TString(*result.GetStats()->GetPlan()));
            NJson::TJsonValue actualOrder;
            NJson::ReadJsonTree(actual, &actualOrder, true);
            UNIT_ASSERT_C(actualOrder == expectedOrder,
                context << ": native AST-to-RBO join order differs from " << path
                << "\nActual join order:\n" << actual
                << "\nExpected canonical join order:\n" << expected);
            // Count only queries that passed both the native-path guard and the
            // canonical comparison. Successful explain alone is not a match.
            ++matched;
        // Continue through the corpus after a failure so one unsupported
        // construct does not hide coverage gaps in all subsequent queries.
        // The accumulated errors still fail the enclosing benchmark test.
        } catch (const std::exception& error) {
            errors += context + ": " + error.what() + "\n";
            Cerr << context << ": " << error.what() << Endl;
        }
    }
    Cerr << benchmark.Name << ": " << matched << '/' << checked << " native canonical join orders matched" << Endl;
    UNIT_ASSERT_C(checked > 0, "No queries selected");
    UNIT_ASSERT_C(errors.empty(), errors);
}

} // namespace

Y_UNIT_TEST_SUITE(KqpJoinOrderNewParser) {
    Y_UNIT_TEST(TPCH) {
        RunJoinOrderBenchmark({"TPCH", "yql-tpch", "tpch", "tpch1000s", "tpch100s", 22}, {});
    }

    Y_UNIT_TEST(TPCDS) {
        // Retain the original canonization suite's exclusions (17/44).
        // Q14/Q23/Q39 have two result statements and are outside this path.
        RunJoinOrderBenchmark({"TPCDS", "yql-tpcds", "tpcds", "tpcds1000s", "tpcds10s", 99}, {17, 44}, {14, 23, 39});
    }
}

} // namespace NKikimr::NKqp
