#include <ydb/library/plan2svg/format.h>
#include <ydb/library/plan2svg/metrics.h>
#include <ydb/library/plan2svg/parse.h>
#include <ydb/library/plan2svg/plan2svg.h>
#include <ydb/library/plan2svg/svg.h>
#include <ydb/library/plan2svg/visualizer.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/xml/document/xml-document.h>

#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/system/env.h>

using namespace NPlan2Svg;

namespace {

// Set PLAN2SVG_UT_CANONIZE=1 to (re)generate the golden .svg files in the source tree
// instead of comparing against them. Review the resulting diff before committing it.
bool Canonizing() {
    return !GetEnv("PLAN2SVG_UT_CANONIZE").empty();
}

TFsPath DataDir() {
    return TFsPath(ArcadiaFromCurrentLocation(__SOURCE_FILE__, "data"));
}

TString ReadPlan(const TString& name) {
    return TFileInput(DataDir() / (name + ".json")).ReadAll();
}

TString RenderPlan(const TString& name, bool simplified) {
    TPlanVisualizer viz;
    viz.LoadPlans(ReadPlan(name), simplified);
    return viz.PrintSvg();
}

// A browser parses an SVG as XML and refuses to render a document that is not well-formed,
// so plan text reaching the output unescaped breaks the whole picture, not one label.
void AssertWellFormed(const TString& svg, const TString& what) {
    try {
        NXml::TDocument document(svg, NXml::TDocument::String);
        Y_UNUSED(document);
    } catch (const std::exception& e) {
        UNIT_FAIL(what + " is not well-formed XML: " + e.what());
    }
}

// Reports the first difference with some context, otherwise a 300 KB blob lands in the log.
TString Diff(const TString& expected, const TString& actual) {
    size_t pos = 0;
    while (pos < expected.size() && pos < actual.size() && expected[pos] == actual[pos]) {
        pos++;
    }
    size_t lineNo = 1;
    size_t lineStart = 0;
    for (size_t i = 0; i < pos; i++) {
        if (expected[i] == '\n') {
            lineNo++;
            lineStart = i + 1;
        }
    }
    auto cut = [lineStart](const TString& s) {
        return s.substr(lineStart, Min<size_t>(200, s.size() - Min(lineStart, s.size())));
    };
    return TStringBuilder()
        << "sizes " << expected.size() << " vs " << actual.size()
        << ", first difference at line " << lineNo << " (offset " << pos << ")\n"
        << "expected: " << cut(expected) << "\n"
        << "actual:   " << cut(actual);
}

void CheckGolden(const TString& name, bool simplified = false) {
    auto svg = RenderPlan(name, simplified);

    UNIT_ASSERT_C(svg.StartsWith("<svg"), "unexpected SVG prologue: " + svg.substr(0, 64));
    UNIT_ASSERT_C(svg.EndsWith("</svg>\n") || svg.EndsWith("</svg>"), "unexpected SVG epilogue");
    AssertWellFormed(svg, name);

    auto goldenName = simplified ? (name + ".simplified.svg") : (name + ".svg");
    auto golden = DataDir() / goldenName;

    if (Canonizing()) {
        TFileOutput(golden).Write(svg);
        return;
    }

    UNIT_ASSERT_C(golden.Exists(), "no golden file " + goldenName + ", rerun with PLAN2SVG_UT_CANONIZE=1");
    auto expected = TFileInput(golden).ReadAll();
    if (expected != svg) {
        // Keep the actual output around; the test working dir is preserved by the test machinery.
        TFileOutput(goldenName + ".actual").Write(svg);
        UNIT_FAIL(goldenName + " mismatch: " + Diff(expected, svg));
    }
}

NJson::TJsonValue Json(TStringBuf text) {
    NJson::TJsonValue value;
    UNIT_ASSERT(NJson::ReadJsonTree(text, &value));
    return value;
}

} // namespace

// Golden tests over real query plans. They pin the current rendering byte for byte, so any
// refactoring of plan2svg.cpp that is meant to preserve behaviour must leave them untouched.
Y_UNIT_TEST_SUITE(TPlan2SvgGolden) {

    // TPC-H style plan with a CTE subplan and a single stats block.
    Y_UNIT_TEST(CteSubplan) {
        CheckGolden("cte_subplan");
    }

    Y_UNIT_TEST(CteSubplanSimplified) {
        CheckGolden("cte_subplan", /* simplified */ true);
    }

    // Several CTEs plus ingress stages.
    Y_UNIT_TEST(CteIngress) {
        CheckGolden("cte_ingress");
    }

    // Plan without any execution statistics (EXPLAIN output).
    Y_UNIT_TEST(ExplainOnly) {
        CheckGolden("explain_only");
    }

    // Per-node ("NodeId") statistics, external stages, operator level metrics.
    Y_UNIT_TEST(ClusterNodes) {
        CheckGolden("cluster_nodes");
    }

    // The widest sample: external stages, operators, ingress, CTEs.
    Y_UNIT_TEST(OperatorsExternal) {
        CheckGolden("operators_external");
    }
}

Y_UNIT_TEST_SUITE(TPlan2SvgLoad) {

    Y_UNIT_TEST(EmptyInputProducesNoPlans) {
        TVisualizer viz;
        viz.LoadPlans(TString());
        UNIT_ASSERT(viz.Plans.empty());
    }

    Y_UNIT_TEST(MalformedJsonProducesNoPlans) {
        TVisualizer viz;
        viz.LoadPlans(TString("{\"Plan\": "));
        UNIT_ASSERT(viz.Plans.empty());
    }

    Y_UNIT_TEST(UnknownRootProducesNoPlans) {
        TVisualizer viz;
        viz.LoadPlans(TString("{\"NotAPlan\": {}}"));
        UNIT_ASSERT(viz.Plans.empty());
    }

    Y_UNIT_TEST(QueriesRootIsAccepted) {
        TVisualizer viz;
        viz.LoadPlans(TString(R"({"queries":[{"Plan":{"Node Type":"Query","Plans":[{"Node Type":"ResultSet"}]}}]})"));
        UNIT_ASSERT_VALUES_EQUAL(viz.Plans.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(viz.Plans[0]->NodeType, "ResultSet");
    }

    Y_UNIT_TEST(SimplifiedSelectsSimplifiedPlan) {
        const TString plans = R"({
            "Plan": {"Node Type": "Query", "Plans": [{"Node Type": "Full"}]},
            "SimplifiedPlan": {"Node Type": "Query", "Plans": [{"Node Type": "Simple"}]}
        })";

        TVisualizer full;
        full.LoadPlans(plans, false);
        UNIT_ASSERT_VALUES_EQUAL(full.Plans.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(full.Plans[0]->NodeType, "Full");
        UNIT_ASSERT(!full.Config.Simplified);

        TVisualizer simplified;
        simplified.LoadPlans(plans, true);
        UNIT_ASSERT_VALUES_EQUAL(simplified.Plans.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(simplified.Plans[0]->NodeType, "Simple");
        UNIT_ASSERT(simplified.Config.Simplified);
    }

    Y_UNIT_TEST(PrintSvgSafeOnEmptyPlan) {
        TPlanVisualizer viz;
        auto svg = viz.PrintSvgSafe();
        UNIT_ASSERT(svg.StartsWith("<svg"));
    }

    // Every input in data/ must stay loadable and renderable through the safe entry point.
    Y_UNIT_TEST(AllSamplesRenderThroughSafeEntryPoint) {
        TVector<TFsPath> children;
        DataDir().List(children);
        size_t seen = 0;
        for (const auto& child : children) {
            if (child.GetExtension() != "json") {
                continue;
            }
            seen++;
            TPlanVisualizer viz;
            viz.LoadPlans(TFileInput(child).ReadAll());
            auto svg = viz.PrintSvgSafe();
            UNIT_ASSERT_C(svg.StartsWith("<svg"), child.GetName());
            UNIT_ASSERT_C(svg.size() > 1024, child.GetName() + " rendered only " + ToString(svg.size()) + " bytes");
            AssertWellFormed(svg, child.GetName());
        }
        UNIT_ASSERT_C(seen > 0, "no sample plans found in " + DataDir().GetPath());
    }
}

Y_UNIT_TEST_SUITE(TPlan2SvgEscape) {

    Y_UNIT_TEST(PlainTextIsUnchanged) {
        UNIT_ASSERT_VALUES_EQUAL(SvgEscape(""), "");
        UNIT_ASSERT_VALUES_EQUAL(SvgEscape("Stage 5: Filter"), "Stage 5: Filter");
        // Quotes only matter inside attributes, and no plan text is written into one.
        UNIT_ASSERT_VALUES_EQUAL(SvgEscape("a'b\"c"), "a'b\"c");
    }

    Y_UNIT_TEST(XmlSpecialsAreEscaped) {
        UNIT_ASSERT_VALUES_EQUAL(SvgEscape("a & b"), "a &amp; b");
        UNIT_ASSERT_VALUES_EQUAL(SvgEscape("x := <expr>"), "x := &lt;expr&gt;");
        UNIT_ASSERT_VALUES_EQUAL(SvgEscape("<&>"), "&lt;&amp;&gt;");
        // Already escaped text is escaped again: nothing in the pipeline pre-escapes.
        UNIT_ASSERT_VALUES_EQUAL(SvgEscape("&lt;"), "&amp;lt;");
    }

    // Operator descriptions routinely contain markup-looking text ("_col := <expr>",
    // "a <- b", "x && y"), which used to reach the output verbatim.
    Y_UNIT_TEST(PlanTextWithMarkupStaysWellFormed) {
        TPlanVisualizer viz;
        viz.LoadPlans(TString(R"({"Plan":{"Plans":[{"Node Type":"ResultSet <&>","Plans":[
            {"Node Type":"Stage","Operators":[{"Name":"Filter","Predicate":"item.a < 1 && item.b > 2"}]}
        ]}]}})"));
        auto svg = viz.PrintSvg();
        AssertWellFormed(svg, "plan with markup in operator info");
        UNIT_ASSERT(svg.Contains("&amp;&amp;"));
        UNIT_ASSERT(svg.Contains("&lt;"));
    }
}

Y_UNIT_TEST_SUITE(TPlan2SvgFormat) {

    Y_UNIT_TEST(DurationMs) {
        UNIT_ASSERT_VALUES_EQUAL(FormatDurationMs(0), "0.00s");
        UNIT_ASSERT_VALUES_EQUAL(FormatDurationMs(1), "1ms");
        UNIT_ASSERT_VALUES_EQUAL(FormatDurationMs(99), "99ms");
        UNIT_ASSERT_VALUES_EQUAL(FormatDurationMs(100), "0.10s");
        UNIT_ASSERT_VALUES_EQUAL(FormatDurationMs(1'500), "1.50s");
        UNIT_ASSERT_VALUES_EQUAL(FormatDurationMs(59'999), "59.99s");
        UNIT_ASSERT_VALUES_EQUAL(FormatDurationMs(60'000), "1m 00s");
        UNIT_ASSERT_VALUES_EQUAL(FormatDurationMs(61'000), "1m 01s");
        UNIT_ASSERT_VALUES_EQUAL(FormatDurationMs(3'600'000), "1h 00m");
        UNIT_ASSERT_VALUES_EQUAL(FormatDurationMs(3'900'000), "1h 05m");
        UNIT_ASSERT_VALUES_EQUAL(FormatDurationMs(24ull * 3'600'000), "24h");
    }

    Y_UNIT_TEST(DurationUs) {
        UNIT_ASSERT_VALUES_EQUAL(FormatDurationUs(0), "0.00s");
        UNIT_ASSERT_VALUES_EQUAL(FormatDurationUs(1), "1us");
        UNIT_ASSERT_VALUES_EQUAL(FormatDurationUs(999), "999us");
        UNIT_ASSERT_VALUES_EQUAL(FormatDurationUs(1'000), "1ms");
        UNIT_ASSERT_VALUES_EQUAL(FormatDurationUs(1'500'000), "1.50s");
    }

    Y_UNIT_TEST(IntegerValue) {
        UNIT_ASSERT_VALUES_EQUAL(FormatInteger(0), "0");
        UNIT_ASSERT_VALUES_EQUAL(FormatInteger(999), "999");
        UNIT_ASSERT_VALUES_EQUAL(FormatInteger(1'000), "1.00K");
        UNIT_ASSERT_VALUES_EQUAL(FormatInteger(1'234), "1.23K");
        UNIT_ASSERT_VALUES_EQUAL(FormatInteger(1'000'000), "1.00M");
        UNIT_ASSERT_VALUES_EQUAL(FormatInteger(1'500'000'000), "1.50G");
        UNIT_ASSERT_VALUES_EQUAL(FormatIntegerValue(5, 1000, "rows"), "5rows");
    }

    Y_UNIT_TEST(Bytes) {
        UNIT_ASSERT_VALUES_EQUAL(FormatBytes(0), "0B");
        UNIT_ASSERT_VALUES_EQUAL(FormatBytes(1023), "1023B");
        UNIT_ASSERT_VALUES_EQUAL(FormatBytes(1024), "1.00KB");
        UNIT_ASSERT_VALUES_EQUAL(FormatBytes(1536), "1.50KB");
        UNIT_ASSERT_VALUES_EQUAL(FormatBytes(1024ull * 1024), "1.00MB");
        UNIT_ASSERT_VALUES_EQUAL(FormatBytes(1024ull * 1024 * 1024), "1.00GB");
    }

    Y_UNIT_TEST(TimeMs) {
        UNIT_ASSERT_VALUES_EQUAL(FormatTimeMs(0), "0:00.00");
        UNIT_ASSERT_VALUES_EQUAL(FormatTimeMs(1'230), "0:01.23");
        UNIT_ASSERT_VALUES_EQUAL(FormatTimeMs(9'990), "0:09.99");
        UNIT_ASSERT_VALUES_EQUAL(FormatTimeMs(10'000), "0:10");
        UNIT_ASSERT_VALUES_EQUAL(FormatTimeMs(75'000), "1:15");
        UNIT_ASSERT_VALUES_EQUAL(FormatTimeMs(3'600'000), "1:00:00");
        UNIT_ASSERT_VALUES_EQUAL(FormatTimeMs(3'661'000), "1:01:01");
    }

    Y_UNIT_TEST(TimeAgg) {
        TAggregation agg;
        agg.Min = 1'000;
        agg.Avg = 2'000;
        agg.Max = 3'000;
        UNIT_ASSERT_VALUES_EQUAL(FormatTimeAgg(agg), "0:01.00 | 0:02.00 | 0:03.00");
    }

    Y_UNIT_TEST(MCpu) {
        UNIT_ASSERT_VALUES_EQUAL(FormatMCpu(0), "0.00");
        UNIT_ASSERT_VALUES_EQUAL(FormatMCpu(1'000), "1.00");
        UNIT_ASSERT_VALUES_EQUAL(FormatMCpu(1'234), "1.23");
        UNIT_ASSERT_VALUES_EQUAL(FormatMCpu(12'345), "12.34");
    }
}

Y_UNIT_TEST_SUITE(TPlan2SvgParse) {

    Y_UNIT_TEST(TableOrIndexName) {
        UNIT_ASSERT_VALUES_EQUAL(ParseTableOrIndexName("table"), "table");
        UNIT_ASSERT_VALUES_EQUAL(ParseTableOrIndexName("/Root/db/table"), "table");
        UNIT_ASSERT_VALUES_EQUAL(ParseTableOrIndexName("/table"), "table");
        // For an implementation table of a secondary index the table and index names are
        // reported instead, so that "idx" is not shown on its own with no table context.
        UNIT_ASSERT_VALUES_EQUAL(ParseTableOrIndexName("table/idx/indexImplTable"), "table/idx");
        UNIT_ASSERT_VALUES_EQUAL(ParseTableOrIndexName("/Root/db/table/idx/indexImplTable"), "table/idx");
        // Without a table segment to prepend only the index name is left.
        UNIT_ASSERT_VALUES_EQUAL(ParseTableOrIndexName("/idx/indexImplTable"), "idx");
        UNIT_ASSERT_VALUES_EQUAL(ParseTableOrIndexName("idx/indexImplTable"), "idx");
    }

    Y_UNIT_TEST(MinMaxIgnoreZero) {
        ui64 m = 0;
        UpdateMin(m, 0);
        UNIT_ASSERT_VALUES_EQUAL(m, 0);
        UpdateMin(m, 5);
        UNIT_ASSERT_VALUES_EQUAL(m, 5);
        UpdateMin(m, 7);
        UNIT_ASSERT_VALUES_EQUAL(m, 5);
        UpdateMin(m, 3);
        UNIT_ASSERT_VALUES_EQUAL(m, 3);

        ui64 x = 0;
        UpdateMax(x, 0);
        UNIT_ASSERT_VALUES_EQUAL(x, 0);
        UpdateMax(x, 5);
        UNIT_ASSERT_VALUES_EQUAL(x, 5);
        UpdateMax(x, 3);
        UNIT_ASSERT_VALUES_EQUAL(x, 5);
        UpdateMax(x, 7);
        UNIT_ASSERT_VALUES_EQUAL(x, 7);
    }
}

Y_UNIT_TEST_SUITE(TPlan2SvgAggregation) {

    Y_UNIT_TEST(LoadFull) {
        TAggregation agg;
        UNIT_ASSERT(agg.Load(Json(R"({"Count": 4, "Sum": 100, "Min": 10, "Max": 40})")));
        UNIT_ASSERT_VALUES_EQUAL(agg.Count, 4);
        UNIT_ASSERT_VALUES_EQUAL(agg.Sum, 100);
        UNIT_ASSERT_VALUES_EQUAL(agg.Min, 10);
        UNIT_ASSERT_VALUES_EQUAL(agg.Max, 40);
        UNIT_ASSERT_VALUES_EQUAL(agg.Avg, 25);
    }

    Y_UNIT_TEST(LoadWithoutCountIsIgnored) {
        TAggregation agg;
        UNIT_ASSERT(!agg.Load(Json(R"({"Sum": 100})")));
        UNIT_ASSERT_VALUES_EQUAL(agg.Sum, 0);
    }

    Y_UNIT_TEST(LoadZeroCountIsIgnored) {
        TAggregation agg;
        UNIT_ASSERT(!agg.Load(Json(R"({"Count": 0, "Sum": 100})")));
        UNIT_ASSERT_VALUES_EQUAL(agg.Sum, 0);
    }

    Y_UNIT_TEST(LoadWithoutMinMaxDefaultsToAvg) {
        TAggregation agg;
        UNIT_ASSERT(agg.Load(Json(R"({"Count": 4, "Sum": 100})")));
        UNIT_ASSERT_VALUES_EQUAL(agg.Avg, 25);
        UNIT_ASSERT_VALUES_EQUAL(agg.Min, 25);
        UNIT_ASSERT_VALUES_EQUAL(agg.Max, 25);
    }

    // Avg is clamped into [Min, Max] because Sum/Count can fall outside of the reported range.
    Y_UNIT_TEST(AvgIsClampedIntoMinMax) {
        TAggregation low;
        UNIT_ASSERT(low.Load(Json(R"({"Count": 4, "Sum": 4, "Min": 10, "Max": 40})")));
        UNIT_ASSERT_VALUES_EQUAL(low.Avg, 10);

        TAggregation high;
        UNIT_ASSERT(high.Load(Json(R"({"Count": 4, "Sum": 1000, "Min": 10, "Max": 40})")));
        UNIT_ASSERT_VALUES_EQUAL(high.Avg, 40);
    }
}

Y_UNIT_TEST_SUITE(TPlan2SvgMetricHistory) {

    Y_UNIT_TEST(TooShortIsIgnored) {
        TMetricHistory history;
        history.Load(Json("[1000, 10]"), 0, 0);
        UNIT_ASSERT(history.Values.empty());
        UNIT_ASSERT(history.Deriv.empty());
        UNIT_ASSERT_VALUES_EQUAL(history.MaxValue, 0);
    }

    Y_UNIT_TEST(LoadInterleavedTimeAndValue) {
        TMetricHistory history;
        history.Load(Json("[1000, 10, 2000, 30, 3000, 60]"), 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(history.MinTime, 1000);
        UNIT_ASSERT_VALUES_EQUAL(history.MaxTime, 3000);
        UNIT_ASSERT_VALUES_EQUAL(history.MaxValue, 60);
        UNIT_ASSERT_VALUES_EQUAL(history.Values.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(history.Values[0].first, 1000);
        UNIT_ASSERT_VALUES_EQUAL(history.Values[0].second, 10);
        UNIT_ASSERT_VALUES_EQUAL(history.Values.back().first, 3000);
        UNIT_ASSERT_VALUES_EQUAL(history.Values.back().second, 60);
    }

    // The history is resampled into a fixed number of buckets; the total increment is preserved.
    Y_UNIT_TEST(DerivIsResampledIntoFixedRanges) {
        TMetricHistory history;
        history.Load(Json("[1000, 0, 2000, 32, 3000, 64]"), 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(history.Deriv.size(), 33);
        UNIT_ASSERT_VALUES_EQUAL(history.Deriv.front().first, 1000);
        UNIT_ASSERT_VALUES_EQUAL(history.Deriv.back().first, 3000);
        ui64 total = 0;
        for (const auto& d : history.Deriv) {
            total += d.second;
        }
        UNIT_ASSERT_VALUES_EQUAL(total, 64);
        // A sample that spans many ranges is split between the range it lands in and the
        // preceding one proportionally to the time each covers, so the bulk of the increment
        // ends up in the range before the sample.
        UNIT_ASSERT_VALUES_EQUAL(history.MaxDeriv, 30);
        UNIT_ASSERT_VALUES_EQUAL(history.Deriv[15].second, 30);
        UNIT_ASSERT_VALUES_EQUAL(history.Deriv[16].second, 2);
    }

    // Time must increase monotonously, the rest of the series is dropped.
    Y_UNIT_TEST(NonMonotonicTailIsDropped) {
        TMetricHistory history;
        history.Load(Json("[1000, 10, 2000, 20, 1500, 30, 4000, 40]"), 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(history.MaxTime, 2000);
        UNIT_ASSERT_VALUES_EQUAL(history.MaxValue, 20);
    }

    Y_UNIT_TEST(ExplicitBoundsClipTheSeries) {
        TMetricHistory history;
        history.Load(Json("[1000, 10, 2000, 20, 3000, 30, 4000, 40]"), 2000, 3000);
        UNIT_ASSERT_VALUES_EQUAL(history.MinTime, 2000);
        UNIT_ASSERT_VALUES_EQUAL(history.MaxTime, 3000);
        for (const auto& v : history.Values) {
            UNIT_ASSERT_GE(v.first, 2000);
            UNIT_ASSERT_LE(v.first, 3000);
        }
    }

    Y_UNIT_TEST(LoadValuesOnlyIsPaddedToTimes) {
        TMetricHistory history;
        std::vector<ui64> times = {1000, 2000, 3000, 4000};
        history.Load(times, Json("[10, 20]"), 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(history.Values.size(), 4);
        // The last known value is repeated for the missing tail.
        UNIT_ASSERT_VALUES_EQUAL(history.Values[2].second, 20);
        UNIT_ASSERT_VALUES_EQUAL(history.Values[3].second, 20);
    }

    Y_UNIT_TEST(IntegrateAndAverage) {
        TMetricHistory history;
        std::vector<ui64> times = {0, 10, 20};
        std::vector<ui64> values = {0, 10, 10};
        history.Load(times, values, 0, 0);
        // Trapezoids: (0+10)/2*10 + (10+10)/2*10 = 50 + 100
        UNIT_ASSERT_VALUES_EQUAL(history.Integrate(), 150);
        UNIT_ASSERT_VALUES_EQUAL(history.Average(), 7);
    }

    Y_UNIT_TEST(AverageOfEmptyIsZero) {
        TMetricHistory history;
        UNIT_ASSERT_VALUES_EQUAL(history.Integrate(), 0);
        UNIT_ASSERT_VALUES_EQUAL(history.Average(), 0);
    }
}

Y_UNIT_TEST_SUITE(TPlan2SvgSummaryMetric) {

    Y_UNIT_TEST(AddTracksMinMaxSumCount) {
        TSummaryMetric metric;
        UNIT_ASSERT_VALUES_EQUAL(metric.Average(), 0);
        metric.Add(10);
        UNIT_ASSERT_VALUES_EQUAL(metric.Min, 10);
        UNIT_ASSERT_VALUES_EQUAL(metric.Max, 10);
        metric.Add(30);
        metric.Add(20);
        UNIT_ASSERT_VALUES_EQUAL(metric.Min, 10);
        UNIT_ASSERT_VALUES_EQUAL(metric.Max, 30);
        UNIT_ASSERT_VALUES_EQUAL(metric.Value, 60);
        UNIT_ASSERT_VALUES_EQUAL(metric.Count, 3);
        UNIT_ASSERT_VALUES_EQUAL(metric.Average(), 20);
    }

    // The first Add() seeds Min/Max, so a zero does not stick as a permanent minimum.
    Y_UNIT_TEST(FirstAddSeedsMinMax) {
        TSummaryMetric metric;
        metric.Add(100);
        metric.Add(0);
        UNIT_ASSERT_VALUES_EQUAL(metric.Min, 0);
        UNIT_ASSERT_VALUES_EQUAL(metric.Max, 100);
    }
}
