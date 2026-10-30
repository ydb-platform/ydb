#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <library/cpp/json/writer/json.h>
#include <yql/essentials/public/langver/yql_langver.h>
#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/string/split.h>

#include <regex>
#include <set>

namespace NKikimr::NKqp {
namespace {

struct TBenchmark {
    TString Name;
    TString QueryDirectory;
    TString Schema;
    TString NormalStatistics;
    TString SmallStatistics;
    ui32 QueryCount;
};

TString ReadRboData(const TString& path) {
    return TIFStream(SRC_("data/" + path)).ReadAll();
}

TString CanonicalPath(const TBenchmark& benchmark, const TString& statistics, ui32 queryId) {
    const TString statisticsName = statistics == "normal" ? benchmark.NormalStatistics : benchmark.SmallStatistics;
    const TString scale = statisticsName.substr(benchmark.Schema.size());
    return SRC_(TStringBuilder() << "data/join_order/" << benchmark.Schema << queryId
        << "_" << scale << "_column_store.json");
}

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

// JOIN_ORDER_QUERIES selects comma-separated query IDs and overrides the disabled list.
// JOIN_ORDER_STATISTICS selects normal (default) or small.
// CANONIZE_JOIN_ORDER_TESTS=true rewrites only successfully explained queries.
void RunJoinOrderBenchmark(const TBenchmark& benchmark, const std::set<ui32>& disabled = {}) {
    std::set<ui32> selected;
    const TString selection = GetTestParam("JOIN_ORDER_QUERIES");
    if (!selection.empty()) {
        TVector<TString> names;
        Split(selection, ",", names);
        for (const auto& name : names) {
            ui32 id = 0;
            UNIT_ASSERT_C(TryFromString(name, id) && id >= 1 && id <= benchmark.QueryCount,
                "Invalid " << benchmark.Name << " query ID: " << name);
            selected.insert(id);
        }
    }
    TString profile = GetTestParam("JOIN_ORDER_STATISTICS");
    if (profile.empty()) {
        profile = "normal";
    }
    UNIT_ASSERT_C(profile == "normal" || profile == "small",
        "JOIN_ORDER_STATISTICS must be normal or small");
    TString canonize = GetTestParam("CANONIZE_JOIN_ORDER_TESTS");
    canonize.to_lower();
    UNIT_ASSERT_C(canonize.empty() || canonize == "true" || canonize == "false",
        "CANONIZE_JOIN_ORDER_TESTS must be true or false");

    TString errors;
    const TString statisticsPath = "stats/" + (profile == "normal"
        ? benchmark.NormalStatistics : benchmark.SmallStatistics) + ".json";
    const TString fullStatisticsPath = SRC_("data/" + statisticsPath);
    UNIT_ASSERT_C(TFsPath(fullStatisticsPath).Exists(),
        "Missing " << profile << " statistics: " << fullStatisticsPath);

    NKikimrConfig::TAppConfig appConfig;
    auto* config = appConfig.MutableTableServiceConfig();
    config->SetEnableNewRBO(true);
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
    auto settings = TKikimrSettings(appConfig).SetWithSampleTables(false).SetKqpSettings({statistics});
    settings.FeatureFlags.SetEnableSeparationComputeActorsFromRead(true);
    TKikimrRunner kikimr(settings);
    auto sessionResult = kikimr.GetQueryClient().GetSession().GetValueSync();
    UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
    auto session = sessionResult.GetSession();

    const std::regex tables(R"(CREATE TABLE [^\(]+ \([^;]*\))", std::regex::multiline);
    const std::string schema = std::regex_replace(std::string(ReadRboData("schema/" + benchmark.Schema + ".sql")), tables,
        "$& WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16);");
    auto create = session.ExecuteQuery(TString(schema), NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
    UNIT_ASSERT_C(create.IsSuccess(), create.GetIssues().ToString());

    for (ui32 id = 1; id <= benchmark.QueryCount; ++id) {
        if (!selected.empty() && !selected.contains(id)) {
            continue;
        }
        if (selected.empty() && disabled.contains(id)) {
            Cerr << "Skipping " << benchmark.Name << " Q" << id << Endl;
            continue;
        }
        const TString context = TStringBuilder() << benchmark.Name << "/" << profile << "/q" << id;
        Cerr << "Checking join order: " << context << Endl;
        try {
            const TString query = R"(
                $to_decimal = ($x) -> { return cast($x as Decimal(12, 2)); };
                $to_decimal_max_precision = ($x) -> { return cast($x as Decimal(35, 2)); };
                $round = ($x,$y) -> {return $x;};
            )" + ReadRboData(benchmark.QueryDirectory + "/q" + ToString(id) + ".yql");
            auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_C(result.GetStats() && result.GetStats()->GetPlan(), "Missing explain plan");
            const TString actual = FormatJoinOrder(TString(*result.GetStats()->GetPlan()));
            const TString path = CanonicalPath(benchmark, profile, id);
            if (canonize == "true") {
                TFsPath(path).Parent().MkDirs();
                TOFStream output(path);
                output << actual;
                output.Finish();
                Cerr << "Canonized " << path << Endl;
            } else {
                const TString expected = TIFStream(path).ReadAll();
                NJson::TJsonValue actualOrder;
                NJson::TJsonValue expectedOrder;
                NJson::ReadJsonTree(actual, &actualOrder, true);
                NJson::ReadJsonTree(expected, &expectedOrder, true);
                UNIT_ASSERT_C(actualOrder == expectedOrder,
                    context << ": join order changed; use JOIN_ORDER_QUERIES=" << id
                    << " with CANONIZE_JOIN_ORDER_TESTS=true to update it"
                    << "\nActual join order:\n" << actual
                    << "\nExpected join order:\n" << expected);
            }
        } catch (const std::exception& error) {
            errors += context + ": " + error.what() + "\n";
            Cerr << context << ": " << error.what() << Endl;
        }
    }
    UNIT_ASSERT_C(errors.empty(), errors);
}

} // namespace

Y_UNIT_TEST_SUITE(KqpJoinOrderNewRbo) {
    Y_UNIT_TEST(TPCH) {
        RunJoinOrderBenchmark({"TPCH", "yql-tpch", "tpch", "tpch1000s", "tpch100s", 22}, {});
    }

    Y_UNIT_TEST(TPCDS) {
        // Match the queries excluded from KqpRboYql::TPCDS_YQL.
        RunJoinOrderBenchmark({"TPCDS", "yql-tpcds", "tpcds", "tpcds1000s", "tpcds10s", 99}, {17, 44});
    }
}

} // namespace NKikimr::NKqp
