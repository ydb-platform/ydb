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

#include <algorithm>
#include <cmath>

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

    // Stages that wait on input while their peers wait on output, i.e. the plan
    // that draws the "W" warning badge at the bottom of a stage.
    Y_UNIT_TEST(WaitInputPeer) {
        CheckGolden("wait_input_peer");
    }

    // Two queries in a single plan, so the second one starts at a non-zero time
    // offset: everything it draws on the timeline, the hot (red) CPU regions
    // included, has to be shifted by that offset.
    Y_UNIT_TEST(MultiQueryHot) {
        CheckGolden("multi_query_hot");
    }

    // cluster_nodes with per-node task counts (Stats.Nodes) on most stages: the
    // task column draws the per-node task profile instead of the dashed line.
    // Covers a partially finished node, a node with nothing finished, tasks not
    // started yet, a single node, unsorted input, and two stages plus the
    // external sources that keep the dashed line.
    Y_UNIT_TEST(StageNodes) {
        CheckGolden("stage_nodes");
    }
}

Y_UNIT_TEST_SUITE(TPlan2SvgStageNodes) {

    TString StagePlan(TStringBuf stats) {
        return TStringBuilder() << R"({"Plan":{"Plans":[{"Node Type":"ResultSet","PlanNodeId":2,"Plans":[
            {"Node Type":"Stage","PlanNodeId":1,"Operators":[{"Name":"Filter","Predicate":"x"}],"Stats":{)"
            << stats << "}}]}]}}";
    }

    // The extent of a node with this many tasks, when the stage runs total
    // tasks on nodes: value / average thirds of the column, to the nearest pixel.
    i32 Width(ui64 tasks, ui64 nodes, ui64 total) {
        return std::lround(double(tasks * nodes * TPlanViewConfig().TaskWidth) / double(3 * total));
    }

    // The extent of a node with the average task count: a third of the column.
    i32 PlotUnit() {
        return Width(1, 1, 1);
    }

    // The plot's own viewport over the task column: pixels across, half bands
    // down, whatever the stage height.
    TString PlotViewport(ui32 nodes) {
        TPlanViewConfig config;
        return TStringBuilder()
            << "<svg x='" << config.TaskLeft << "' y='0' width='" << config.TaskWidth << "' height='100%' viewBox='0 0 "
            << config.TaskWidth << ' ' << 2 * nodes << "' preserveAspectRatio='none' pointer-events='none'>";
    }

    // Both areas end the same way: same colour, half transparent.
    TString AreaEnd() {
        return TStringBuilder() << "z' stroke='none' fill='" << TPlanViewConfig().Palette.Cpu.Medium << "' opacity='0.5'/>";
    }

    // The curve between two band centres: a step, both control points at the
    // mid height.
    TString Step(i32 dx) {
        return TStringBuilder() << "c0,1," << dx << ",1," << dx << ",2";
    }

    TString RenderStage(const TString& plan) {
        TVisualizer viz;
        viz.LoadPlans(plan);
        UNIT_ASSERT_VALUES_EQUAL(viz.Plans.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(viz.Plans[0]->Stages.size(), 1);
        return viz.PrintSvg();
    }

    size_t CountAreas(const TString& svg) {
        size_t count = 0;
        for (size_t pos = svg.find(AreaEnd()); pos != TString::npos; pos = svg.find(AreaEnd(), pos + 1)) {
            count++;
        }
        return count;
    }

    Y_UNIT_TEST(TwoNodesAreTwoBands) {
        auto svg = RenderStage(StagePlan(R"("PhysicalStageId":5,"Tasks":16,"FinishedTasks":9,
            "Nodes":[{"NodeId":7,"Tasks":6,"Finished":3},{"NodeId":3,"Tasks":6,"Finished":6}])"));
        AssertWellFormed(svg, "stage with Nodes");

        UNIT_ASSERT_C(svg.Contains("<title>Stage 5 tasks: finished 9 of 16; 2 nodes, 6 tasks each; running on 1 node; not started: 4</title>"), svg);
        UNIT_ASSERT_C(svg.Contains(PlotViewport(2)), svg);

        // Two bands: node 3 centred at y=1, node 7 at y=3. Both have the
        // average count, so the all-tasks area is one unit wide throughout.
        i32 w = PlotUnit();
        UNIT_ASSERT_C(svg.Contains(TStringBuilder()
            << "<path d='M0,0L" << w << ",0L" << w << ",1" << Step(0)
            << 'L' << w << ",4L0,4" << AreaEnd()), svg);
        // Running: node 3 has none, node 7 has 3 of the 6 (half a unit); the
        // area steps from the left edge to the half unit between the bands.
        i32 running = Width(3, 2, 12);
        UNIT_ASSERT_C(svg.Contains(TStringBuilder()
            << "<path d='M0,0L0,0L0,1" << Step(running)
            << 'L' << running << ",4L0,4" << AreaEnd()), svg);
        UNIT_ASSERT_VALUES_EQUAL_C(CountAreas(svg), 2, svg);
    }

    // A stage that only reports its totals is drawn as a single node: the
    // all-tasks area is one unit wide, the running one its share of it.
    Y_UNIT_TEST(NoNodesIsASingleNode) {
        auto svg = RenderStage(StagePlan(R"("PhysicalStageId":5,"Tasks":16,"FinishedTasks":12)"));
        AssertWellFormed(svg, "stage without Nodes");

        UNIT_ASSERT_C(svg.Contains("<title>Stage 5 tasks: finished 12 of 16</title>"), svg);
        UNIT_ASSERT_C(svg.Contains(PlotViewport(1)), svg);
        i32 w = PlotUnit();
        i32 running = Width(4, 1, 16);
        UNIT_ASSERT_C(svg.Contains(TStringBuilder()
            << "<path d='M0,0L" << w << ",0L" << w << ",1L" << w << ",2L0,2" << AreaEnd()), svg);
        UNIT_ASSERT_C(svg.Contains(TStringBuilder()
            << "<path d='M0,0L" << running << ",0L" << running << ",1L" << running << ",2L0,2" << AreaEnd()), svg);
        UNIT_ASSERT_VALUES_EQUAL_C(CountAreas(svg), 2, svg);
    }

    // A single node has no bands to curve between: the area is a rectangle,
    // and with everything finished there is no running area at all.
    Y_UNIT_TEST(SingleFinishedNodeIsARectangle) {
        auto svg = RenderStage(StagePlan(R"("PhysicalStageId":5,"Tasks":4,"FinishedTasks":4,
            "Nodes":[{"NodeId":1,"Tasks":4,"Finished":4}])"));
        i32 w = PlotUnit();
        UNIT_ASSERT_C(svg.Contains(TStringBuilder()
            << "<path d='M0,0L" << w << ",0L" << w << ",1L" << w << ",2L0,2" << AreaEnd()), svg);
        UNIT_ASSERT_VALUES_EQUAL_C(CountAreas(svg), 1, svg);
    }

    // The average node is one unit wide; a busier node grows past it and a
    // node too small for a pixel still gets one.
    Y_UNIT_TEST(AverageNodeIsOneUnit) {
        auto svg = RenderStage(StagePlan(R"("PhysicalStageId":5,"Tasks":101,"FinishedTasks":100,
            "Nodes":[{"NodeId":1,"Tasks":100,"Finished":100},{"NodeId":2,"Tasks":1,"Finished":0}])"));
        i32 w = PlotUnit();
        // Average 50.5: the wide node is 100 / 50.5 units, the small one 1px.
        i32 wide = Width(100, 2, 101);
        UNIT_ASSERT_C(wide > w, svg);
        UNIT_ASSERT_C(svg.Contains(TStringBuilder()
            << "<path d='M0,0L" << wide << ",0L" << wide << ",1" << Step(1 - wide)
            << "L1,4L0,4" << AreaEnd()), svg);
        // Running: none on the wide node, the one task of the small node.
        UNIT_ASSERT_C(svg.Contains(TStringBuilder()
            << "<path d='M0,0L0,0L0,1" << Step(1)
            << "L1,4L0,4" << AreaEnd()), svg);
    }

    // A node far above the average is cut at the column width.
    Y_UNIT_TEST(BusyNodeIsCutAtTheColumn) {
        auto svg = RenderStage(StagePlan(R"("PhysicalStageId":5,"Tasks":103,"FinishedTasks":103,
            "Nodes":[{"NodeId":1,"Tasks":100,"Finished":100},{"NodeId":2,"Tasks":1,"Finished":1},{"NodeId":3,"Tasks":1,"Finished":1},{"NodeId":4,"Tasks":1,"Finished":1}])"));
        TPlanViewConfig config;
        ui32 w = config.TaskWidth;
        UNIT_ASSERT_C(Width(100, 4, 103) > i32(w), svg);
        UNIT_ASSERT_C(svg.Contains(TStringBuilder() << "<path d='M0,0L" << w << ",0L" << w << ",1c0,"), svg);
        UNIT_ASSERT_VALUES_EQUAL_C(CountAreas(svg), 1, svg);
    }

    // The profile is one path however many nodes there are, stretched over
    // the stage box: one step per band boundary, each with its control points
    // at the mid height, the last band centred just above the bottom edge.
    // With more nodes than the stage has pixels, in pixels the band centres
    // and the control points rounded together: steps collapsed flat or bent
    // into hooks.
    Y_UNIT_TEST(ManyNodesFitTheStageHeight) {
        // Nodes 1, 2, 3, 4 ... 100 have 2, 3, 1, 2 ... 2 tasks, 2 on average.
        auto tasks = [](ui32 i) -> ui32 {
            return 1 + i % 3;
        };
        TStringBuilder nodes;
        for (ui32 i = 1; i <= 100; i++) {
            nodes << (i > 1 ? "," : "") << R"({"NodeId":)" << i << R"(,"Tasks":)" << tasks(i) << R"(,"Finished":)" << i % 2 << "}";
        }
        TVisualizer viz;
        viz.LoadPlans(StagePlan(TStringBuilder() << R"("PhysicalStageId":5,"Tasks":200,"FinishedTasks":50,"Nodes":[)" << nodes << "]"));
        UNIT_ASSERT_VALUES_EQUAL(viz.Plans.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(viz.Plans[0]->Stages.size(), 1);
        UNIT_ASSERT_C(viz.Plans[0]->Stages[0]->Height < 100, viz.Plans[0]->Stages[0]->Height);
        auto svg = viz.PrintSvg();
        AssertWellFormed(svg, "stage with 100 nodes");
        UNIT_ASSERT_C(svg.Contains(PlotViewport(100)), svg);

        auto extent = [&](ui32 i) -> i32 {
            return Width(tasks(i), 100, 200);
        };
        TStringBuilder all;
        all << "<path d='M0,0L" << extent(1) << ",0L" << extent(1) << ",1";
        for (ui32 i = 2; i <= 100; i++) {
            all << Step(extent(i) - extent(i - 1));
        }
        all << 'L' << extent(100) << ",200L0,200" << AreaEnd();
        UNIT_ASSERT_C(svg.Contains(all), svg);
    }

    // A stage with a nested table read is loaded twice over the same Stats
    // block (LoadSubPlans re-enters LoadStage); the nodes must not double up.
    Y_UNIT_TEST(NestedTableReadLoadsNodesOnce) {
        TVisualizer viz;
        viz.LoadPlans(TString(R"({"Plan":{"Plans":[{"Node Type":"ResultSet","PlanNodeId":3,"Plans":[
            {"Node Type":"TopSort-Filter","PlanNodeId":2,"Operators":[{"Name":"TopSort","Limit":"10"},{"Name":"Filter","Predicate":"x"}],
             "Stats":{"PhysicalStageId":1,"Tasks":16,"FinishedTasks":9,"Nodes":[{"NodeId":3,"Tasks":6,"Finished":6},{"NodeId":7,"Tasks":6,"Finished":3}]},
             "Plans":[{"Node Type":"TableFullScan","PlanNodeId":1,"Operators":[{"Name":"TableFullScan","Table":"t","ReadColumns":["a"]}]}]}
        ]}]}})"));
        UNIT_ASSERT_VALUES_EQUAL(viz.Plans.size(), 1);
        // The stage itself plus the External source stage the table read spawns.
        UNIT_ASSERT_VALUES_EQUAL(viz.Plans[0]->Stages.size(), 2);
        UNIT_ASSERT(!viz.Plans[0]->Stages[0]->External);
        UNIT_ASSERT_VALUES_EQUAL(viz.Plans[0]->Stages[0]->Nodes.size(), 2);
        UNIT_ASSERT(viz.Plans[0]->Stages[1]->External);
        UNIT_ASSERT(viz.Plans[0]->Stages[1]->Nodes.empty());

        auto svg = viz.PrintSvg();
        AssertWellFormed(svg, "stage with a nested table read");
        UNIT_ASSERT_C(svg.Contains("<title>Stage 1 tasks: finished 9 of 16; 2 nodes, 6 tasks each; running on 1 node; not started: 4</title>"), svg);
        // Two equal nodes, one unit wide: doubled nodes would have doubled the
        // bands.
        UNIT_ASSERT_C(svg.Contains(PlotViewport(2)), svg);
        UNIT_ASSERT_C(svg.Contains(TStringBuilder()
            << "<path d='M0,0L" << PlotUnit() << ",0L" << PlotUnit() << ",1c0,"), svg);
    }

    // The tooltip sums the nodes up instead of listing them: the spread of the
    // task counts and where the busiest node is, by its id when it is alone.
    Y_UNIT_TEST(TooltipSummarizesNodes) {
        auto svg = RenderStage(StagePlan(R"("PhysicalStageId":5,"Tasks":7,"FinishedTasks":0,
            "Nodes":[{"NodeId":1,"Tasks":1},{"NodeId":2,"Tasks":4},{"NodeId":3,"Tasks":2}])"));
        UNIT_ASSERT_C(svg.Contains("<title>Stage 5 tasks: finished 0 of 7; 3 nodes, 1 to 4 tasks each, 4 on node 2</title>"), svg);

        svg = RenderStage(StagePlan(R"("PhysicalStageId":5,"Tasks":5,"FinishedTasks":3,
            "Nodes":[{"NodeId":1,"Tasks":2,"Finished":2},{"NodeId":2,"Tasks":1,"Finished":1},{"NodeId":3,"Tasks":2}])"));
        UNIT_ASSERT_C(svg.Contains("<title>Stage 5 tasks: finished 3 of 5; 3 nodes, 1 to 2 tasks each, 2 on 2 nodes; running on 1 node</title>"), svg);

        svg = RenderStage(StagePlan(R"("PhysicalStageId":5,"Tasks":3,"FinishedTasks":1,
            "Nodes":[{"NodeId":9,"Tasks":1,"Finished":1},{"NodeId":4,"Tasks":1}])"));
        UNIT_ASSERT_C(svg.Contains("<title>Stage 5 tasks: finished 1 of 3; 2 nodes, 1 task each; running on 1 node; not started: 1</title>"), svg);

        svg = RenderStage(StagePlan(R"("PhysicalStageId":5,"Tasks":1,"FinishedTasks":0,"Nodes":[{"NodeId":7,"Tasks":1}])"));
        UNIT_ASSERT_C(svg.Contains("<title>Stage 5 tasks: finished 0 of 1; on node 7</title>"), svg);
    }

    // A cluster node draws its tasks as a single node, in the same viewport.
    Y_UNIT_TEST(ClusterNodeIsASingleNode) {
        TVisualizer viz;
        viz.LoadPlans(TString(R"({"Plan":{"Plans":[{"Node Type":"ResultSet","PlanNodeId":2,
            "Nodes":[{"NodeId":1,"Tasks":4,"FinishedTasks":1}],"Plans":[
            {"Node Type":"Stage","PlanNodeId":1,"Operators":[{"Name":"Filter","Predicate":"x"}],"Stats":{"PhysicalStageId":5}}]}]}})"));
        UNIT_ASSERT_VALUES_EQUAL(viz.Plans.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(viz.Plans[0]->Nodes.size(), 1);
        auto svg = viz.PrintSvg();
        AssertWellFormed(svg, "plan with a cluster node");

        UNIT_ASSERT_C(svg.Contains("data-stage='inner node'"), svg);
        i32 w = PlotUnit();
        i32 running = Width(3, 1, 4);
        UNIT_ASSERT_C(svg.Contains(TStringBuilder()
            << PlotViewport(1) << "\n"
            << "<path d='M0,0L" << w << ",0L" << w << ",1L" << w << ",2L0,2" << AreaEnd() << "\n"
            << "<path d='M0,0L" << running << ",0L" << running << ",1L" << running << ",2L0,2" << AreaEnd()), svg);
        // The stage has no tasks: the cluster node draws the only plot.
        UNIT_ASSERT_VALUES_EQUAL_C(CountAreas(svg), 2, svg);
    }

    Y_UNIT_TEST(LoaderSortsAndDropsEmptyNodes) {
        TVisualizer viz;
        viz.LoadPlans(StagePlan(R"("PhysicalStageId":5,"Tasks":4,"FinishedTasks":1,
            "Nodes":[{"NodeId":9,"Tasks":1,"Finished":1},{"NodeId":2,"Tasks":0,"Finished":0},{"NodeId":4,"Tasks":3}])"));
        UNIT_ASSERT_VALUES_EQUAL(viz.Plans.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(viz.Plans[0]->Stages.size(), 1);
        const auto& nodes = viz.Plans[0]->Stages[0]->Nodes;
        UNIT_ASSERT_VALUES_EQUAL(nodes.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(nodes[0].NodeId, 4);
        UNIT_ASSERT_VALUES_EQUAL(nodes[0].Tasks, 3);
        UNIT_ASSERT_VALUES_EQUAL(nodes[0].Finished, 0);
        UNIT_ASSERT_VALUES_EQUAL(nodes[1].NodeId, 9);
        UNIT_ASSERT_VALUES_EQUAL(nodes[1].Tasks, 1);
        UNIT_ASSERT_VALUES_EQUAL(nodes[1].Finished, 1);
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

    // A plan node the loader does not understand is rejected rather than drawn wrong.
    static TString UnsupportedPlan() {
        return R"({"Plan":{"Plans":[{"Node Type":"R","Plans":[
            {"Node Type":"S","PlanNodeType":"<Weird & Type>"}
        ]}]}})";
    }

    Y_UNIT_TEST(LoadPlansThrowsOnUnsupportedPlan) {
        TVisualizer viz;
        UNIT_ASSERT_EXCEPTION(viz.LoadPlans(UnsupportedPlan()), yexception);
    }

    // The safe pair never throws at the caller; the failure comes back as a picture.
    Y_UNIT_TEST(LoadPlansSafeReportsErrorThroughPrintSvgSafe) {
        TVisualizer viz;
        viz.LoadPlansSafe(UnsupportedPlan());
        UNIT_ASSERT(viz.LoadError);

        auto svg = viz.PrintSvgSafe();
        AssertWellFormed(svg, "error svg");
        UNIT_ASSERT_C(svg.Contains("&lt;Weird &amp; Type&gt;"), svg);

        // The half-loaded plans behind the failure are not rendered instead.
        UNIT_ASSERT_EXCEPTION(viz.PrintSvg(), yexception);
    }

    // A failed load is rolled back and the error is not sticky, so a reused
    // visualizer behaves as if the failed call never happened.
    Y_UNIT_TEST(LoadPlansSafeRollsBackAFailedLoad) {
        TVisualizer viz;
        viz.LoadPlansSafe(UnsupportedPlan());
        UNIT_ASSERT(viz.LoadError);
        UNIT_ASSERT(viz.Plans.empty());

        viz.LoadPlansSafe(ReadPlan("cte_subplan"));
        UNIT_ASSERT_C(!viz.LoadError, viz.LoadError);
        UNIT_ASSERT(!viz.Plans.empty());
        UNIT_ASSERT_VALUES_EQUAL(viz.PrintSvgSafe(), RenderPlan("cte_subplan", false));
    }

    Y_UNIT_TEST(LoadPlansSafeRendersAGoodPlanNormally) {
        TPlanVisualizer safe;
        safe.LoadPlansSafe(ReadPlan("cte_subplan"));
        UNIT_ASSERT_C(!safe.GetLoadError(), safe.GetLoadError());
        UNIT_ASSERT_VALUES_EQUAL(safe.PrintSvgSafe(), RenderPlan("cte_subplan", false));
    }

    // The facade exposes the recorded failure for callers that report it through
    // their own channel (an issue, a log line) rather than as a picture.
    Y_UNIT_TEST(GetLoadErrorCarriesTheFailure) {
        TPlanVisualizer viz;
        viz.LoadPlansSafe(UnsupportedPlan());
        UNIT_ASSERT(viz.GetLoadError().Contains("Unexpected plan node type"));
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

    // Control characters other than \t \n \r cannot appear in XML 1.0 at all,
    // even as entities; a query over a binary string literal puts them into
    // operator info via JSON \u escapes.
    Y_UNIT_TEST(ControlCharactersAreReplaced) {
        UNIT_ASSERT_VALUES_EQUAL(SvgEscape(TStringBuf("a\001b", 3)), "a?b");
        UNIT_ASSERT_VALUES_EQUAL(SvgEscape(TStringBuf("\000\037", 2)), "??");
        UNIT_ASSERT_VALUES_EQUAL(SvgEscape("a\tb\nc\rd"), "a\tb\nc\rd");
        UNIT_ASSERT_VALUES_EQUAL(SvgEscape(TStringBuf("<\001>", 3)), "&lt;?&gt;");
    }

    // Operator descriptions routinely contain markup-looking text ("_col := <expr>",
    // "a <- b", "x && y"), which used to reach the output verbatim. The \u0001 is
    // decoded by the JSON reader into a raw control byte XML cannot carry.
    Y_UNIT_TEST(PlanTextWithMarkupStaysWellFormed) {
        TPlanVisualizer viz;
        viz.LoadPlans(TString(R"({"Plan":{"Plans":[{"Node Type":"ResultSet <&>","Plans":[
            {"Node Type":"Stage","Operators":[{"Name":"Filter","Predicate":"item.a < 1 && item.b > \u0001"}]}
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
        // The derivative covers only the in-window increment (20 -> 30). The
        // sample at 4000 lies entirely past the window: its interval starts at
        // 3000, so it contributes nothing, not its full out-of-window delta.
        ui64 total = 0;
        for (const auto& d : history.Deriv) {
            total += d.second;
        }
        UNIT_ASSERT_VALUES_EQUAL(total, 10);
        UNIT_ASSERT_VALUES_EQUAL(history.Deriv.back().second, 0);
    }

    // An interval straddling the window end is split by time, like every other
    // bucket-crossing interval: the last bucket gets the inside share.
    Y_UNIT_TEST(TailBeyondWindowIsSplitProportionally) {
        TMetricHistory history;
        // 1000 grows over [2999, 3099]; 1 of those 100ms is inside the window.
        history.Load(Json("[1000, 0, 2999, 0, 3099, 1000]"), 0, 3000);
        UNIT_ASSERT_VALUES_EQUAL(history.Deriv.back().second, 10);
        UNIT_ASSERT_VALUES_EQUAL(history.MaxDeriv, 10);
    }

    // The explicit-vector entry point gets time arrays straight from the JSON,
    // with no parser in between to drop a non-monotonic tail. A series that
    // comes back to a repeated timestamp used to divide by zero.
    Y_UNIT_TEST(NonMonotonicExplicitTimesAreTruncated) {
        TMetricHistory history;
        std::vector<ui64> times = {0, 100, 50, 50, 200};
        history.Load(times, Json("[1, 2, 3, 4, 5]"), 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(history.MaxTime, 100);
        UNIT_ASSERT_VALUES_EQUAL(history.Values.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(history.Values.back().second, 2);
    }

    Y_UNIT_TEST(InvertedExplicitWindowLoadsNothing) {
        TMetricHistory history;
        std::vector<ui64> times = {1000, 2000};
        std::vector<ui64> values = {10, 20};
        history.Load(times, values, 3000, 1500);
        UNIT_ASSERT(history.Values.empty());
        UNIT_ASSERT(history.Deriv.empty());
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
