#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/rbo/kqp_rbo_yql_workload_helpers.h>
#include <ydb/core/kqp/ut/rbo/statistics/helpers/kqp_analyzed_benchmarks.h>

#include <library/cpp/json/writer/json.h>
#include <library/cpp/testing/common/env.h>

#include <util/stream/file.h>
#include <util/string/split.h>

#include <cmath>
#include <set>

namespace NKikimr::NKqp {
namespace {

struct TBenchmark {
    TString Name;
    NYqlTests::EBenchType Type;
    ui32 QueryCount;
    std::unique_ptr<TAnalyzedBenchmark> (*Load)(double, bool);
};

TString FormatJoinOrder(const TString& plan) {
    // Canonize the same structural view as the original suite: join order,
    // algorithms and shuffles, without transient stage IDs or runtime details.
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

void RunJoinOrderBenchmark(const TBenchmark& benchmark, bool columnStore, const std::set<ui32>& disabled = {}) {
    double scale = 0.1;
    const TString scaleParam = GetTestParam("JOIN_ORDER_SCALE");
    if (!scaleParam.empty()) {
        UNIT_ASSERT_C(TryFromString(scaleParam, scale) && std::isfinite(scale) && scale > 0,
            "JOIN_ORDER_SCALE must be a positive finite number");
    }
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
    TString canonize = GetTestParam("CANONIZE_JOIN_ORDER_TESTS");
    canonize.to_lower();
    UNIT_ASSERT_C(canonize.empty() || canonize == "true" || canonize == "false",
        "CANONIZE_JOIN_ORDER_TESTS must be true or false");

    // Load and analyze once per benchmark/storage combination. The helper pins
    // base row counts and byte sizes; column statistics come from ANALYZE.
    auto database = benchmark.Load(scale, columnStore);
    auto& session = database->GetSession();
    const TString profile = benchmark.Name + "_sf" + ToString(scale) + (columnStore ? "_column" : "_row");
    const auto artifacts = GetOutputPath().Child(profile);
    artifacts.MkDirs();
    TString errors;
    for (ui32 id = 1; id <= benchmark.QueryCount; ++id) {
        if (!selected.empty() && !selected.contains(id)) {
            continue;
        }
        if (selected.empty() && disabled.contains(id)) {
            Cerr << "Skipping " << benchmark.Name << " Q" << id << Endl;
            continue;
        }
        const TString context = profile + "/q" + ToString(id);
        Cerr << "Checking analyzed join order: " << context << Endl;
        try {
            const TString query = R"(
                $to_decimal = ($x) -> { return cast($x as Decimal(12, 2)); };
                $to_decimal_max_precision = ($x) -> { return cast($x as Decimal(35, 2)); };
                $round = ($x,$y) -> {return $x;};
            )" + NYqlTests::ReadBenchmarkQuery(benchmark.Type, id, database->GetTablesPath());
            // Benchmark queries are compiled only; CLI import and ANALYZE above populate statistics.
            auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_C(result.GetStats() && result.GetStats()->GetPlan(), "Missing explain plan");
            const TString plan(*result.GetStats()->GetPlan());
            {
                TOFStream output(artifacts.Child("q" + ToString(id) + ".explain.json"));
                output << plan << Endl;
                output.Finish();
            }
            // Keep row/column and scale baselines independent. Updating a
            // baseline is explicit; a missing file fails ordinary test runs.
            const TString actual = FormatJoinOrder(plan);
            const TString path = SRC_("canondata/" + context + ".json");
            if (canonize == "true") {
                TFsPath(path).Parent().MkDirs();
                TOFStream output(path);
                output << actual;
                output.Finish();
                Cerr << "Canonized " << path << Endl;
            } else {
                UNIT_ASSERT_C(TFsPath(path).Exists(),
                    "Missing baseline " << path << "; run with CANONIZE_JOIN_ORDER_TESTS=true");
                NJson::TJsonValue actualOrder;
                NJson::TJsonValue expectedOrder;
                NJson::ReadJsonTree(actual, &actualOrder, true);
                NJson::ReadJsonTree(TIFStream(path).ReadAll(), &expectedOrder, true);
                UNIT_ASSERT_C(actualOrder == expectedOrder,
                    context << ": join order changed; use JOIN_ORDER_QUERIES=" << id
                    << " with CANONIZE_JOIN_ORDER_TESTS=true to update it"
                    << "\nActual join order:\n" << actual
                    << "\nExpected join order:\n" << expectedOrder);
            }
        } catch (const std::exception& error) {
            errors += context + ": " + error.what() + "\n";
            Cerr << context << ": " << error.what() << Endl;
        }
    }
    UNIT_ASSERT_C(errors.empty(), errors);
}

} // namespace

Y_UNIT_TEST_SUITE(KqpJoinOrderAnalyzedStatistics) {
    Y_UNIT_TEST_TWIN(TPCH, ColumnStore) {
        RunJoinOrderBenchmark({"tpch", NYqlTests::TPCH, 22, LoadTpchAndAnalyze}, ColumnStore);
    }

    Y_UNIT_TEST_TWIN(TPCDS, ColumnStore) {
        // Keep the same exclusions as the original KqpJoinOrderNewRbo suite.
        RunJoinOrderBenchmark({"tpcds", NYqlTests::TPCDS, 99, LoadTpcdsAndAnalyze}, ColumnStore, {17, 44});
    }
}

} // namespace NKikimr::NKqp
