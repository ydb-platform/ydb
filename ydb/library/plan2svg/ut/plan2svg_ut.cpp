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
#include <util/string/cast.h>

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

    Y_UNIT_TEST(SimplifiedResultSetPreservesTopOperator) {
        const TString plans = R"({"SimplifiedPlan":{
            "Node Type":"ResultSet","PlanNodeType":"ResultSet","Plans":[{
                "Node Type":"Sort","PlanNodeId":1,
                "Operators":[{"Name":"Sort","SortBy":[{"Column":"a","Ascending":true,"NullsFirst":true}]}],
                "Plans":[{
                    "Node Type":"Merge","PlanNodeType":"Connection","PlanNodeId":2,
                    "SortColumns":[{"Column":"a","Ascending":true,"NullsFirst":true}],
                    "Plans":[{
                        "Node Type":"TableFullScan","PlanNodeId":3,
                        "Operators":[{"Name":"TableFullScan","Table":"t","ReadColumns":["a"]}]
                    }]
                }]
            }]
        }})";

        TVisualizer viz;
        viz.LoadPlans(plans, /*simplified=*/true);
        UNIT_ASSERT_VALUES_EQUAL(viz.Plans.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(viz.Plans[0]->NodeType, "ResultSet");
        UNIT_ASSERT_VALUES_EQUAL(viz.Plans[0]->Stages[0]->NodeType, "Sort");
        AssertWellFormed(viz.PrintSvg(), "simplified result set");
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

    Y_UNIT_TEST(StructuredProperties) {
        const auto sort = Json(R"([{"Column":"t.a","Ascending":false,"NullsFirst":false}])");
        for (const auto* name : {"SortBy", "TopSortBy", "OrderBy", "SortColumns"}) {
            UNIT_ASSERT_VALUES_EQUAL(FormatPlanProperty(name, sort), "t.a desc nulls last");
        }
        const auto columns = Json(R"(["b","a, c"])");
        for (const auto* name : {"Columns", "GroupBy", "PartitionBy", "LookupKeyPrefix"}) {
            UNIT_ASSERT_VALUES_EQUAL(FormatPlanProperty(name, columns), "b, a, c");
        }
        UNIT_ASSERT_VALUES_EQUAL(FormatPlanProperty("Aggregation", Json(R"([
            {"Output":"n","Function":"count","Arguments":["t.a"],"Distinct":true},
            {"Output":"s","Function":"sum","Arguments":["t.b"],"Distinct":false}
        ])")), "{n: count(distinct t.a), s: sum(t.b)}");
        UNIT_ASSERT_VALUES_EQUAL(FormatPlanProperty("Frame", Json(R"({
            "Type":"Rows","Begin":{"Kind":"Preceding","Value":0},"End":{"Kind":"UnboundedFollowing"}
        })")), "Rows[Preceding 0, UnboundedFollowing]");
        UNIT_ASSERT_VALUES_EQUAL(FormatSortColumn(NJson::TJsonValue("a (Desc)")), "a (Desc)");
    }

    Y_UNIT_TEST(NumericEstimatesAndBooleanFlags) {
        UNIT_ASSERT_VALUES_EQUAL(
            GetEstimation(Json(R"({"E-Cost":123.5,"E-Rows":1000,"E-Size":4096})")),
            GetEstimation(Json(R"({"E-Cost":"123.5","E-Rows":"1000","E-Size":"4096"})")));
        for (const auto* flag : {"true", "false", "\"True\"", "\"False\""}) {
            bool value = true;
            ReadBoolString(Json(TStringBuilder() << "{\"Parallel\":" << flag << "}"), "Parallel", value);
            UNIT_ASSERT_VALUES_EQUAL(value, TString(flag) == "true" || TString(flag) == "\"True\"");
        }
    }

    Y_UNIT_TEST(StructuredOperatorsRenderLikeLegacy) {
        auto render = [](TStringBuf operators) {
            TPlanVisualizer viz;
            viz.LoadPlans(TStringBuilder() << R"({"Plan":{"Plans":[{"Node Type":"ResultSet","PlanNodeId":2,"Plans":[
                {"Node Type":"Stage","PlanNodeId":1,"Operators":)" << operators << "}]}]}}");
            return viz.PrintSvg();
        };
        const auto legacy = render(R"([
            {"Name":"Sort","SortBy":"a desc nulls last","E-Cost":"123","E-Rows":"45","E-Size":"2048"},
            {"Name":"Aggregate","GroupBy":"b, c","Aggregation":"{n: count(distinct a)}","Pushdown":"True"},
            {"Name":"TopSort","Limit":"5","TopSortBy":"b asc nulls first"}
        ])");
        const auto structured = render(R"([
            {"Name":"Sort","SortBy":[{"Column":"a","Ascending":false,"NullsFirst":false}],"E-Cost":123,"E-Rows":45,"E-Size":2048},
            {"Name":"Aggregate","GroupBy":["b","c"],"Aggregation":[{"Output":"n","Function":"count","Arguments":["a"],"Distinct":true}],"Pushdown":true},
            {"Name":"TopSort","Limit":"5","TopSortBy":[{"Column":"b","Ascending":true,"NullsFirst":true}]}
        ])");
        UNIT_ASSERT_C(legacy == structured, Diff(legacy, structured));
        AssertWellFormed(structured, "structured properties");
    }

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

Y_UNIT_TEST_SUITE(TPlan2SvgColumnShard) {

    TString RenderScans(TStringBuf scans) {
        TPlanVisualizer viz;
        viz.LoadPlans(TStringBuilder() << R"({"Plan":{"Plans":[{"Node Type":"ResultSet","Plans":[
            {"Node Type":"Stage","Stats":{"PhysicalStageId":3,"ColumnShardScans":)" << scans << "}}]}]}}");
        auto svg = viz.PrintSvg();
        AssertWellFormed(svg, "ColumnShard statistics");
        return svg;
    }

    TString CsRow(TStringBuf svg, TStringBuf meta, TStringBuf path, TStringBuf kind, TStringBuf family = "SIMPLE/TRIVIAL") {
        const TString marker = TStringBuilder() << "data-cs-family='" << family << "' data-cs-meta='" << meta
            << "' data-cs-path='" << path << "' data-cs-kind='" << kind << '\'';
        const auto position = svg.find(marker);
        UNIT_ASSERT_C(position != TStringBuf::npos, "Missing CS row: " << marker);
        const auto begin = svg.rfind("<svg", position);
        const auto end = svg.find('>', position);
        UNIT_ASSERT(begin != TStringBuf::npos && end != TStringBuf::npos);
        return TString(svg.substr(begin, end - begin + 1));
    }

    size_t CsOccurrences(TStringBuf text, TStringBuf marker) {
        size_t count = 0;
        size_t position = 0;
        while ((position = text.find(marker, position)) != TStringBuf::npos) {
            ++count;
            position += marker.size();
        }
        return count;
    }

    void CsAssertOrder(TStringBuf svg, TStringBuf meta, std::initializer_list<TStringBuf> kinds) {
        size_t previous = 0;
        bool first = true;
        for (TStringBuf kind : kinds) {
            const TString marker = TStringBuilder() << "data-cs-meta='" << meta << "' data-cs-path='' data-cs-kind='" << kind << '\'';
            const auto position = svg.find(marker);
            UNIT_ASSERT_C(position != TStringBuf::npos, "Missing ordered row: " << marker);
            UNIT_ASSERT_C(first || previous < position, "Wrong child order in " << meta << ": " << kind);
            previous = position;
            first = false;
        }
    }

    std::vector<TString> CsOpeningRows(TStringBuf svg, TStringBuf marker) {
        std::vector<TString> result;
        size_t position = 0;
        while ((position = svg.find(marker, position)) != TStringBuf::npos) {
            const auto begin = svg.rfind("<svg", position), end = svg.find('>', position);
            UNIT_ASSERT(begin != TStringBuf::npos && end != TStringBuf::npos);
            result.emplace_back(svg.substr(begin, end - begin + 1));
            position = end + 1;
        }
        return result;
    }

    TString StorageRow(TStringBuf svg, TStringBuf row, TStringBuf kind = {}, TStringBuf name = {}, TStringBuf read = {}) {
        const TString marker = "data-cs-storage-row='" + TString(row) + '\'';
        for (const auto& opening : CsOpeningRows(svg, marker)) {
            if ((!kind || opening.Contains("data-cs-source-kind='" + TString(kind) + '\'')) &&
                (!name || opening.Contains("data-cs-source-name='" + TString(name) + '\'')) && (!read || opening.Contains(read))) {
                return opening;
            }
        }
        UNIT_ASSERT_C(false, "Missing Storage row " << row << '/' << kind << '/' << name << '/' << read);
        return {};
    }

    Y_UNIT_TEST(StorageV3IsAdditiveAcrossReadsAndDeduplicatesDecorations) {
        TPlanVisualizer viz;
        viz.LoadPlans(TString(R"json({"Plan":{"Plans":[{"Node Type":"ResultSet","Plans":[
          {"Node Type":"Stage","Stats":{"PhysicalStageId":1,"ColumnShardScans":[
            {"ExpectedContextOnly":true,"StatsGroupId":"ga","ParticipatingShards":1,"ParticipatingTablets":["1"]},
            {"StatsScanId":"a","StatsGroupId":"ga","TabletId":"1","ReaderKind":"TRIVIAL","Finished":true,
             "StorageStatsVersion":1,"TablePath":"/A","StorageReads":[
              {"SourceKind":"BlobStorage","SourceName":"default","ReadBytes":1024,"ReadCount":1,"DataKind":"unknown"},
              {"SourceKind":"BlobCache","SourceName":"blob","ReadBytes":128,"ReadCount":1},
              {"SourceKind":"Memory","ReadBytes":0,"ReadCount":0}],
             "CacheStats":{"ReturnedBytes":99999,"CacheServedBytes":99999},
             "Steps":[{"StepId":1,"StepKind":"StorageRead","MetaStep":"Filter","ReadBytes":99999,"OwnedExecutionDuration":1}]}
          ]},"Plans":[{"Node Type":"Stage","Stats":{"PhysicalStageId":2,"ColumnShardScans":[
            {"ExpectedContextOnly":true,"StatsGroupId":"gb","ParticipatingShards":1,"ParticipatingTablets":["2"]},
            {"StatsScanId":"b","StatsGroupId":"gb","TabletId":"2","ReaderKind":"PLAIN","Finished":true,
             "StorageStatsVersion":1,"TablePath":"/B","StorageReads":[
              {"SourceKind":"BlobStorage","SourceName":"default","ReadBytes":512,"ReadCount":1},
              {"SourceKind":"ExternalStorage","SourceName":"tier<&'\"Ж","ReadBytes":256,"ReadCount":1}],
             "Steps":[{"StepId":1,"StepKind":"ReaderInit","MetaStep":"Preparation","OwnedExecutionDuration":1}]},
            {"StatsScanId":"a","StatsGroupId":"ga","TabletId":"1","ReaderKind":"TRIVIAL","Finished":true,
             "StorageStatsVersion":1,"TablePath":"/A","StorageReads":[
              {"SourceKind":"BlobStorage","SourceName":"default","ReadBytes":1024,"ReadCount":1,"DataKind":"data"},
              {"SourceKind":"BlobCache","SourceName":"blob","ReadBytes":128,"ReadCount":1}]}
          ]}}]}
        ]}]}})json"));
        const TString svg = viz.PrintSvg();
        AssertWellFormed(svg, "Storage v3");
        UNIT_ASSERT_C(StorageRow(svg, "grand-total").Contains("data-cs-storage-bytes='1920'"), svg);
        UNIT_ASSERT_C(StorageRow(svg, "source-total", "BlobStorage", "default").Contains("data-cs-storage-bytes='1536'"), svg);
        UNIT_ASSERT_C(StorageRow(svg, "read", "BlobStorage", "default", "/A").Contains("data-cs-storage-bytes='1024'"), svg);
        UNIT_ASSERT_C(StorageRow(svg, "read", "BlobStorage", "default", "/B").Contains("data-cs-storage-bytes='512'"), svg);
        UNIT_ASSERT_C(CsOccurrences(svg, "data-cs-storage-row='grand-total'") == 1, svg);
        UNIT_ASSERT_C(!svg.Contains("data-cs-row='cache'") && !svg.Contains("data-cs-source-kind='Memory'"), svg);
        UNIT_ASSERT_C(svg.Contains("data-cs-plan-area='true'") && svg.Contains("Read · table / stage"), svg);
        UNIT_ASSERT_C(svg.Contains("tier&lt;&amp;&apos;&quot;Ж"), svg);
    }

    Y_UNIT_TEST(StorageV3UnknownSharedBytesAreNotZeroOrAnExtraOwnedCharge) {
        const TString svg = RenderScans(R"json([
          {"ExpectedContextOnly":true,"StatsGroupId":"g","ParticipatingShards":1,"ParticipatingTablets":["1"]},
          {"StatsScanId":"a","StatsGroupId":"g","TabletId":"1","ReaderKind":"TRIVIAL","Finished":false,
           "PendingOperations":1,"StorageStatsVersion":1,"StorageReads":[
            {"SourceKind":"BlobStorage","ReadBytes":100,"ReadCount":1},
            {"SourceKind":"ColumnCache","ReadBytes":5,"ReadCount":2,"UnknownByteReads":1,"Shared":true}],
           "Steps":[{"StepId":1,"StepKind":"FetchColumns","MetaStep":"Filter","OwnedExecutionDuration":1,
             "DataSourceReads":[{"SourceKind":"ColumnCache","ReadBytes":5,"ReadCount":2,"UnknownByteReads":1,"Shared":true}]}]}
        ])json");
        const TString total = StorageRow(svg, "grand-total");
        UNIT_ASSERT_C(total.Contains("data-cs-storage-bytes='105'") && total.Contains("data-cs-storage-partial='true'"), svg);
        UNIT_ASSERT_C(total.Contains("data-cs-storage-unknown='1'"), svg);
        UNIT_ASSERT_C(CsRow(svg, "Filter", "", "").Contains("&quot;ReadBytes&quot;:5"), svg);
        UNIT_ASSERT_C(svg.Contains("shared provenance") && svg.Contains("Storage · observed sources (PARTIAL)"), svg);
    }

    Y_UNIT_TEST(StorageV3UnknownOriginDoesNotMakeOtherCompleteOriginsPartial) {
        const TString svg = RenderScans(R"json([
          {"ExpectedContextOnly":true,"StatsGroupId":"ga","ParticipatingShards":1,"ParticipatingTablets":["1"]},
          {"ExpectedContextOnly":true,"StatsGroupId":"gb","ParticipatingShards":1,"ParticipatingTablets":["2"]},
          {"StatsScanId":"a","StatsGroupId":"ga","TabletId":"1","ReaderKind":"TRIVIAL","Finished":true,
           "ParticipatingShards":1,"ParticipatingTablets":["1"],
           "StorageStatsVersion":1,"TablePath":"/A","StorageReads":[
            {"SourceKind":"BlobStorage","SourceName":"default","ReadBytes":100,"ReadCount":1},
            {"SourceKind":"BlobCache","SourceName":"blob","ReadBytes":20,"ReadCount":1}],
           "Steps":[{"StepId":1,"StepKind":"ReaderInit","MetaStep":"Preparation","OwnedExecutionDuration":1}]},
          {"StatsScanId":"b","StatsGroupId":"gb","TabletId":"2","ReaderKind":"TRIVIAL","Finished":true,
           "ParticipatingShards":1,"ParticipatingTablets":["2"],
           "StorageStatsVersion":1,"TablePath":"/B","StorageReads":[
            {"SourceKind":"BlobStorage","SourceName":"default","ReadCount":1,"UnknownByteReads":1},
            {"SourceKind":"PortionMetadataCache","SourceName":"metadata","ReadCount":1,"UnknownByteReads":1}],
           "Steps":[{"StepId":1,"StepKind":"ReaderInit","MetaStep":"Preparation","OwnedExecutionDuration":1}]}
        ])json");
        UNIT_ASSERT_C(StorageRow(svg, "read", "BlobStorage", "default", "/A").Contains("data-cs-storage-partial='false'"), svg);
        UNIT_ASSERT_C(StorageRow(svg, "read", "BlobStorage", "default", "/B").Contains("data-cs-storage-partial='true'"), svg);
        UNIT_ASSERT_C(StorageRow(svg, "source-total", "BlobStorage", "default").Contains("data-cs-storage-partial='true'"), svg);
        UNIT_ASSERT_C(StorageRow(svg, "source-total", "BlobCache", "blob").Contains("data-cs-storage-partial='false'"), svg);
        UNIT_ASSERT_C(StorageRow(svg, "source-total", "PortionMetadataCache", "metadata").Contains("data-cs-storage-partial='true'"), svg);
        UNIT_ASSERT_C(StorageRow(svg, "grand-total").Contains("data-cs-storage-bytes='120'") &&
            StorageRow(svg, "grand-total").Contains("data-cs-storage-partial='true'"), svg);
    }

    Y_UNIT_TEST(StorageV3SourceDetailsRespectSemanticOwnersAndNormalizeReservePaths) {
        const TString svg = RenderScans(R"json([
          {"ExpectedContextOnly":true,"StatsGroupId":"g","ParticipatingShards":1,"ParticipatingTablets":["1"]},
          {"StatsScanId":"a","StatsGroupId":"g","TabletId":"1","ReaderKind":"TRIVIAL","Finished":true,
           "StorageStatsVersion":1,"StorageReads":[{"SourceKind":"BlobStorage","ReadBytes":100,"ReadCount":1}],
           "Steps":[
            {"StepId":1,"StepKind":"MemoryReserve","MetaStep":"Filter","OwnedExecutionDuration":1,"Details":{"ReservationBytes":128}},
            {"StepId":2,"ParentStepId":10,"StepKind":"FetchColumns","MetaStep":"Filter","CanonicalPath":"Reserve","OwnedExecutionDuration":2},
            {"StepId":3,"ParentStepId":2,"StepKind":"StorageRead","MetaStep":"Filter","CanonicalPath":"ReserveMemory/Fetching","OwnedExecutionDuration":3,
             "DataSourceReads":[{"SourceKind":"BlobStorage","ReadBytes":100,"ReadCount":1}]},
            {"StepId":10,"StepKind":"Reader","MetaStep":"PKFilter","OwnedExecutionDuration":1}
           ]}
        ])json");
        UNIT_ASSERT_C(CsRow(svg, "Filter", "", "ReserveMemory").Contains("data-cs-calls='1'"), svg);
        UNIT_ASSERT_C(CsOccurrences(svg, "data-cs-meta='Filter' data-cs-path='ReserveMemory' data-cs-kind='Fetching'") == 1, svg);
        UNIT_ASSERT_C(CsRow(svg, "Filter", "", "").Contains("&quot;ReadBytes&quot;:100"), svg);
        UNIT_ASSERT_C(CsRow(svg, "PKFilter", "", "Reader").Contains("&quot;ReadBytes&quot;:100"), svg);
        UNIT_ASSERT_C(CsRow(svg, "PKFilter", "", "").Contains("data-cs-source-reads='[]'"), svg);
        UNIT_ASSERT_C(StorageRow(svg, "grand-total").Contains("data-cs-storage-bytes='100'"), svg);
        UNIT_ASSERT_C(!svg.Contains("data-cs-kind='Reserve'") && !svg.Contains("data-cs-path='Reserve/"), svg);
    }

    Y_UNIT_TEST(IndexV3KeepsIdentityCheckOrderAndOneSharedReserveOwner) {
        const TString svg = RenderScans(R"json([
          {"StatsScanId":"a","TabletId":"1","ReaderKind":"TRIVIAL","Finished":true,"TablePath":"/A","Steps":[
            {"StepId":1,"StepKind":"MemoryReserve","MetaStep":"IndexFilter","OwnedExecutionDuration":5,
             "SharedIndexInputs":true,"IndexDescriptors":[
              {"IndexId":11,"Name":"bloom_a","Type":"BLOOM_FILTER","Columns":["a"]},
              {"IndexId":12,"Name":"bloom_b","Type":"BLOOM_FILTER","Columns":["b"]}],"Details":{"ReservationBytes":12345}},
            {"StepId":2,"StepKind":"IndexCheck","MetaStep":"IndexFilter","OwnedExecutionDuration":3,"ApplicationOrder":2,
             "IndexDescriptors":[{"IndexId":11,"Name":"bloom_a","Type":"BLOOM_FILTER","Columns":["a"]}],
             "Details":{"CandidateInputRows":100,"CandidateOutputRows":20,"InputRows":100,"OutputRows":100,
              "IndexAvailable":true,"IndexDataAvailable":true,"AppliedToResourceFilter":false}},
            {"StepId":3,"StepKind":"IndexCheck","MetaStep":"IndexFilter","OwnedExecutionDuration":4,"ApplicationOrder":3,
             "IndexDescriptors":[{"IndexId":12,"Name":"bloom_b","Type":"BLOOM_FILTER","Columns":["b"]}],
             "Details":{"CandidateInputRows":100,"CandidateOutputRows":10,"InputRows":100,"OutputRows":10,"AppliedToResourceFilter":true}},
            {"StepId":4,"StepKind":"IndexCheck","MetaStep":"IndexFilter","OwnedExecutionDuration":2,"ApplicationOrder":1,
             "IndexDescriptors":[{"IndexId":13,"Name":"min<&'\"Ж","Type":"MIN_MAX","Columns":["level"]}],
             "Details":{"InputRows":100,"OutputRows":100,"IndexAvailable":true,"IndexDataAvailable":false}}
          ]}
        ])json");
        const auto indexes = CsOpeningRows(svg, "data-cs-row='meta' data-cs-family='SIMPLE/TRIVIAL' data-cs-meta='IndexFilter'");
        UNIT_ASSERT_VALUES_EQUAL_C(indexes.size(), 4, svg);
        UNIT_ASSERT_C(indexes[0].Contains("id:13") && indexes[0].Contains("data-cs-index-positions='[1]'"), svg);
        UNIT_ASSERT_C(indexes[1].Contains("data-cs-index-shared='true'"), svg);
        UNIT_ASSERT_C(indexes[2].Contains("id:11") && indexes[3].Contains("id:12"), svg);
        UNIT_ASSERT_C(indexes[2].Contains("data-cs-index-candidate-input-rows='100'") &&
            indexes[2].Contains("data-cs-index-candidate-output-rows='20'"), svg);
        UNIT_ASSERT_C(CsOccurrences(svg, "data-cs-kind='ReserveMemory'") == 1, svg);
        UNIT_ASSERT_C(CsRow(svg, "", "", "").Contains("data-cs-active-sum='14'"), svg);
        UNIT_ASSERT_C(svg.Contains("shared inputs") && svg.Contains("Actual input owners"), svg);
        UNIT_ASSERT_C(svg.Contains("min&lt;&amp;"), svg);
    }

    Y_UNIT_TEST(IndexV3HeaderInputsKeepTheirFilterOwnerWithoutDuplicatingQuota) {
        const TString svg = RenderScans(R"json([
          {"StatsScanId":"a","TabletId":"1","ReaderKind":"TRIVIAL","Finished":true,"Steps":[
            {"StepId":1,"StepKind":"MemoryReserve","MetaStep":"Filter","OwnedExecutionDuration":5,"SharedIndexInputs":true,
             "IndexDescriptors":[{"IndexId":11,"Name":"bloom","Type":"BLOOM_FILTER","Columns":["a"]}],
             "Details":{"ReservationBytes":4096}},
            {"StepId":2,"StepKind":"IndexCheck","MetaStep":"IndexFilter","OwnedExecutionDuration":3,"ApplicationOrder":1,
             "IndexDescriptors":[{"IndexId":11,"Name":"bloom","Type":"BLOOM_FILTER","Columns":["a"]}],"Details":{"InputRows":10,"OutputRows":10}}
          ]}
        ])json");
        UNIT_ASSERT_C(CsRow(svg, "Filter", "", "").Contains("data-cs-active-sum='5'"), svg);
        UNIT_ASSERT_C(CsOccurrences(svg, "data-cs-kind='ReserveMemory'") == 1, svg);
        UNIT_ASSERT_C(svg.Contains("Filter/ReserveMemory") && svg.Contains("references, not additional quota/time"), svg);
        const auto indexes = CsOpeningRows(svg, "data-cs-row='meta' data-cs-family='SIMPLE/TRIVIAL' data-cs-meta='IndexFilter'");
        UNIT_ASSERT_VALUES_EQUAL_C(indexes.size(), 1, svg);
        UNIT_ASSERT_C(indexes[0].Contains("data-cs-active-sum='3'"), svg);
    }

    Y_UNIT_TEST(StorageV3OlderCounterReplayDoesNotMakeTotalsPartial) {
        const TString svg = RenderScans(R"json([
          {"ExpectedContextOnly":true,"StatsGroupId":"g","ParticipatingShards":1,"ParticipatingTablets":["1"]},
          {"StatsScanId":"a","StatsGroupId":"g","TabletId":"1","ParticipatingShards":1,"TablePath":"/A","ReaderKind":"TRIVIAL",
           "Finished":true,"PendingOperations":0,"StorageStatsVersion":1,"StorageReads":[
             {"SourceKind":"BlobStorage","SourceName":"default","ReadBytes":100,"ReadCount":2,"DataKind":"data"}]},
          {"StatsScanId":"a","StatsGroupId":"g","TabletId":"1","ParticipatingShards":1,"TablePath":"/A","ReaderKind":"TRIVIAL",
           "Finished":false,"PendingOperations":2,"StorageStatsVersion":1,"StorageReads":[
             {"SourceKind":"BlobStorage","SourceName":"default","ReadBytes":80,"ReadCount":1,"DataKind":"data"}]},
          {"StatsScanId":"a","StatsGroupId":"g","TabletId":"1","ParticipatingShards":1,"TablePath":"/A","ReaderKind":"TRIVIAL",
           "Finished":true,"StorageStatsVersion":1,"StorageReads":[
             {"SourceKind":"BlobStorage","SourceName":"default","ReadBytes":80,"ReadCount":1,"DataKind":"data"}]}
        ])json");
        const TString row = StorageRow(svg, "grand-total");
        UNIT_ASSERT_C(row.Contains("data-cs-storage-bytes='100'"), svg);
        UNIT_ASSERT_C(row.Contains("data-cs-storage-reads='2'"), svg);
        UNIT_ASSERT_C(row.Contains("data-cs-storage-partial='false'"), svg);
    }

    Y_UNIT_TEST(StorageV3SparseFinalDoesNotHideOlderSourceSnapshots) {
        const TString header = R"json({"ExpectedContextOnly":true,"StatsGroupId":"g","ParticipatingShards":1,"ParticipatingTablets":["1"]})json";
        const TString final = R"json({"StatsScanId":"a","Finished":true,"PendingOperations":0})json";
        auto render = [&](bool finalFirst, bool truncated) {
            const TString body = TStringBuilder() << R"json({"StatsScanId":"a","StatsGroupId":"g","TabletId":"1","ParticipatingShards":1,
              "TablePath":"/A","ReaderKind":"TRIVIAL","Finished":false,"PendingOperations":2,"Truncated":)json"
                << (truncated ? "true" : "false") << R"json(,"StorageStatsVersion":1,"StorageReads":[
                {"SourceKind":"BlobStorage","SourceName":"default","ReadBytes":100,"ReadCount":2,"DataKind":"data"}]})json";
            return RenderScans(TStringBuilder() << '[' << header << ',' << (finalFirst ? final : body)
                << ',' << (finalFirst ? body : final) << ']');
        };
        for (const bool finalFirst : {false, true}) {
            const TString svg = render(finalFirst, false);
            const TString row = StorageRow(svg, "grand-total");
            UNIT_ASSERT_C(row.Contains("data-cs-storage-bytes='100'"), svg);
            UNIT_ASSERT_C(row.Contains("data-cs-storage-reads='2'"), svg);
            UNIT_ASSERT_C(row.Contains("data-cs-storage-partial='false'"), svg);
            UNIT_ASSERT_C(StorageRow(svg, "read", "BlobStorage", "default", "/A")
                .Contains("data-cs-storage-bytes='100'"), svg);
            const TString truncated = render(finalFirst, true);
            UNIT_ASSERT_C(StorageRow(truncated, "grand-total").Contains("data-cs-storage-partial='true'"), truncated);
        }
    }

    Y_UNIT_TEST(StorageV3KnownZeroDoesNotInventReadSources) {
        const TString svg = RenderScans(R"json([
          {"ExpectedContextOnly":true,"StatsGroupId":"g","ParticipatingShards":1,"ParticipatingTablets":["1"]},
          {"StatsScanId":"a","StatsGroupId":"g","TabletId":"1","ParticipatingShards":1,"ReaderKind":"TRIVIAL","Finished":true,
           "StorageStatsVersion":1,"Steps":[{"StepId":1,"StepKind":"ReaderInit","MetaStep":"Preparation","OwnedExecutionDuration":1}]}
        ])json");
        UNIT_ASSERT_C(StorageRow(svg, "grand-total").Contains("data-cs-storage-bytes='0'"), svg);
        UNIT_ASSERT_C(StorageRow(svg, "grand-total").Contains("data-cs-storage-partial='false'"), svg);
        UNIT_ASSERT_C(CsOccurrences(svg, "data-cs-storage-row='read'") == 0 && !svg.Contains("data-cs-row='cache'"), svg);
    }

    Y_UNIT_TEST(ColumnShardPlanIdentityDisambiguatesRepeatedPhysicalStageIds) {
        TPlanVisualizer viz;
        viz.LoadPlans(TString(R"json({"Plan":{"Plans":[
          {"Node Type":"ResultSet","Plans":[{"Node Type":"Stage","Stats":{"PhysicalStageId":0,"ColumnShardScans":[
            {"StatsScanId":"first","TabletId":"1","ReaderKind":"TRIVIAL","Finished":true,
             "Steps":[{"StepId":1,"StepKind":"ReaderInit","MetaStep":"Preparation","OwnedExecutionDuration":1}]}
          ]}}]},
          {"Node Type":"ResultSet","Plans":[{"Node Type":"Stage","Stats":{"PhysicalStageId":0,"ColumnShardScans":[
            {"StatsScanId":"second","TabletId":"2","ReaderKind":"TRIVIAL","Finished":true,
             "Steps":[{"StepId":1,"StepKind":"ReaderInit","MetaStep":"Preparation","OwnedExecutionDuration":5}]}
          ]}}]}
        ]}})json"));
        const TString svg = viz.PrintSvg();
        AssertWellFormed(svg, "Repeated physical stage IDs");
        const auto panels = CsOpeningRows(svg, "data-cs-panel='0'");
        UNIT_ASSERT_VALUES_EQUAL_C(panels.size(), 2, svg);
        UNIT_ASSERT_C(panels[0].Contains("data-cs-plan-index='0'") && panels[1].Contains("data-cs-plan-index='1'"), svg);
        const auto first = svg.find("data-columnshard='read' data-cs-plan-index='0'");
        const auto second = svg.find("data-columnshard='read' data-cs-plan-index='1'");
        UNIT_ASSERT_C(first != TString::npos && second != TString::npos && first < second, svg);
        const TStringBuf firstCard = TStringBuf(svg).substr(first, second - first);
        const TStringBuf secondCard = TStringBuf(svg).substr(second);
        UNIT_ASSERT_C(firstCard.Contains("data-cs-owned-sum='1'") && !firstCard.Contains("data-cs-owned-sum='5'"), svg);
        UNIT_ASSERT_C(secondCard.Contains("data-cs-owned-sum='5'") && !secondCard.Contains("data-cs-owned-sum='1'"), svg);
    }

    Y_UNIT_TEST(MetaStatsChildOrderDiffersFromMetastepOrder) {
        auto svg = RenderScans(R"([
            {"StatsScanId":"a","TabletId":"1","ReaderKind":"TRIVIAL","Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Filter","MetaStep":"Filter","OwnedExecutionDuration":1},
                {"StepId":2,"StepKind":"Calculation","MetaStep":"Filter","OwnedExecutionDuration":1},
                {"StepId":3,"StepKind":"Decode","MetaStep":"Filter","OwnedExecutionDuration":1},
                {"StepId":4,"StepKind":"Assemble","MetaStep":"Filter","OwnedExecutionDuration":1},
                {"StepId":5,"StepKind":"FetchColumns","MetaStep":"Filter","OwnedExecutionDuration":1},
                {"StepId":6,"StepKind":"MemoryReserve","MetaStep":"Filter","OwnedExecutionDuration":1},
                {"StepId":7,"StepKind":"Aggregation","MetaStep":"Aggregation","OwnedExecutionDuration":1},
                {"StepId":8,"StepKind":"Calculation","MetaStep":"Aggregation","OwnedExecutionDuration":1},
                {"StepId":9,"StepKind":"FetchColumns","MetaStep":"Aggregation","OwnedExecutionDuration":1},
                {"StepId":10,"StepKind":"MemoryReserve","MetaStep":"Aggregation","OwnedExecutionDuration":1},
                {"StepId":11,"StepKind":"Assemble","MetaStep":"Aggregation","OwnedExecutionDuration":1},
                {"StepId":12,"StepKind":"Decode","MetaStep":"Aggregation","OwnedExecutionDuration":1},
                {"StepId":13,"StepKind":"ReaderInit","MetaStep":"Preparation","OwnedExecutionDuration":1},
                {"StepId":14,"StepKind":"SelectPortions","MetaStep":"Preparation","OwnedExecutionDuration":1},
                {"StepId":15,"StepKind":"ParseProgram","MetaStep":"Preparation","OwnedExecutionDuration":1},
                {"StepId":16,"StepKind":"BuildResult","MetaStep":"BuildResult","OwnedExecutionDuration":1},
                {"StepId":17,"StepKind":"PrepareResult","MetaStep":"BuildResult","OwnedExecutionDuration":1}]}
        ])");
        CsAssertOrder(svg, "Filter", {"ReserveMemory", "Fetching", "Assemble", "Decode", "Calculation", "Filter"});
        CsAssertOrder(svg, "Aggregation", {"ReserveMemory", "Fetching", "Assemble", "Decode", "Calculation", "Aggregation"});
        CsAssertOrder(svg, "Preparation", {"ParseProgram", "SelectPortions", "ReaderInit"});
        CsAssertOrder(svg, "BuildResult", {"PrepareResult", "BuildResult"});
    }

    Y_UNIT_TEST(MetaStatsReaderWaitAndReadBoundariesDoNotDoubleCount) {
        auto svg = RenderScans(R"([
            {"StatsScanId":"a","TabletId":"1","ReaderKind":"PLAIN","Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Reader","MetaStep":"PKFilter","OwnedExecutionDuration":1,"IntegralWaitDuration":100},
                {"StepId":2,"StepKind":"FetchColumns","MetaStep":"PKFilter","OwnedExecutionDuration":1,"IntegralWaitDuration":20,"ReadBytes":100},
                {"StepId":3,"StepKind":"StorageRead","MetaStep":"PKFilter","CanonicalPath":"Fetching","OwnedExecutionDuration":1,"IntegralWaitDuration":17,"ReadBytes":80}]}
        ])");
        auto meta = CsRow(svg, "PKFilter", "", "", "PLAIN");
        UNIT_ASSERT_C(meta.Contains("data-cs-wait-sum='20'"), meta);
        UNIT_ASSERT_C(!meta.Contains("data-cs-read-bytes="), meta);
        UNIT_ASSERT_C(CsRow(svg, "PKFilter", "", "Reader", "PLAIN").Contains("data-cs-wait-sum='100'"), svg);
        auto fetching = CsRow(svg, "PKFilter", "", "Fetching", "PLAIN");
        auto backend = CsRow(svg, "PKFilter", "Fetching", "StorageRead", "PLAIN");
        UNIT_ASSERT_C(fetching.Contains("data-cs-read-bytes='100'"), fetching);
        UNIT_ASSERT_C(fetching.Contains("data-cs-read-boundary='delivered'"), fetching);
        UNIT_ASSERT_C(backend.Contains("data-cs-read-bytes='80'"), backend);
        UNIT_ASSERT_C(backend.Contains("data-cs-read-boundary='backend'"), backend);
    }

    Y_UNIT_TEST(MetaStatsCallVolumesAreNotMetaBoundariesOrFinalGroups) {
        auto svg = RenderScans(R"([
            {"StatsScanId":"a","TabletId":"1","ReaderKind":"TRIVIAL","Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Filter","MetaStep":"PKFilter","OwnedExecutionDuration":1,"Details":{"InputRows":1000,"OutputRows":100,"Purpose":"PkRange"}},
                {"StepId":2,"StepKind":"Filter","MetaStep":"PKFilter","OwnedExecutionDuration":1,"Details":{"InputRows":100,"OutputRows":10,"Purpose":"Deletion"}},
                {"StepId":3,"StepKind":"Aggregation","MetaStep":"Aggregation","OwnedExecutionDuration":1,"Details":{"InputRows":1000,"OutputRows":1,"Functions":["COUNT"]}},
                {"StepId":4,"StepKind":"Aggregation","MetaStep":"Aggregation","OwnedExecutionDuration":1,"Details":{"InputRows":1000,"OutputRows":1,"Functions":["SUM"]}}]}
        ])");
        auto masks = CsRow(svg, "PKFilter", "", "");
        UNIT_ASSERT_C(masks.Contains("data-cs-call-input-rows='1100'"), masks);
        UNIT_ASSERT_C(masks.Contains("data-cs-call-output-rows='110'"), masks);
        UNIT_ASSERT_C(!masks.Contains("data-cs-input-rows="), masks);
        auto scalar = CsRow(svg, "Aggregation", "", "");
        UNIT_ASSERT_C(scalar.Contains("data-cs-call-output-rows='2'"), scalar);
        UNIT_ASSERT_C(!scalar.Contains("data-cs-output-rows="), scalar);
        UNIT_ASSERT_C(svg.Contains("Scalar output rows are not groups"), svg);
        UNIT_ASSERT_C(svg.Contains("processed filter candidates (calls)"), svg);
        UNIT_ASSERT_C(svg.Contains("aggregate call rows"), svg);
    }

    Y_UNIT_TEST(MetaStatsReaderOverviewIsExecutionOverheadNotAMetastep) {
        auto svg = RenderScans(R"([
            {"StatsScanId":"a","TabletId":"1","ReaderKind":"PLAIN","Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Reader","Operation":"PROGRAM_EXECUTION","OwnedExecutionDuration":2,"IntegralExecutionDuration":9,"IntegralWaitDuration":100},
                {"StepId":2,"StepKind":"Filter","MetaStep":"Filter","OwnedExecutionDuration":3,"IntegralWaitDuration":0},
                {"StepId":3,"StepKind":"Aggregation","MetaStep":"Aggregation","OwnedExecutionDuration":4,"IntegralWaitDuration":0}]}
        ])");
        auto execution = CsRow(svg, "", "", "", "PLAIN");
        UNIT_ASSERT_C(execution.Contains("data-cs-active-sum='9'"), execution);
        UNIT_ASSERT_C(execution.Contains("data-cs-reader-overhead-owned='2'"), execution);
        UNIT_ASSERT_C(execution.Contains("data-cs-wait-sum='0'"), execution);
        UNIT_ASSERT_C(!svg.Contains("data-cs-meta='Unclassified'"), svg);
        UNIT_ASSERT_C(svg.Contains("reader overhead"), svg);
        UNIT_ASSERT_C(CsRow(svg, "Filter", "", "", "PLAIN").Contains("data-cs-active-sum='3'"), svg);
        UNIT_ASSERT_C(CsRow(svg, "Aggregation", "", "", "PLAIN").Contains("data-cs-active-sum='4'"), svg);
    }

    Y_UNIT_TEST(MetaStatsPendingContextIsNotCallsAndTerminalStatusWins) {
        auto pending = RenderScans(R"([
            {"StatsScanId":"a","TabletId":"1","ReaderKind":"TRIVIAL","Finished":false,"Truncated":false,"PendingOperations":3,"Steps":[
                {"StepId":1,"StepKind":"Filter","MetaStep":"Filter","OwnedExecutionDuration":1}]}
        ])");
        UNIT_ASSERT_C(pending.Contains("data-cs-pending-attempts='1'"), pending);
        UNIT_ASSERT_C(pending.Contains("data-cs-max-pending-operations='3'"), pending);
        UNIT_ASSERT_C(CsRow(pending, "Filter", "", "Filter").Contains("data-cs-calls='1'"), pending);
        UNIT_ASSERT_C(pending.Contains("later costs are absent"), pending);
        auto finished = RenderScans(R"([
            {"StatsScanId":"a","TabletId":"1","ReaderKind":"TRIVIAL","Finished":true,"Truncated":false,"PendingOperations":0,"Steps":[
                {"StepId":1,"StepKind":"Filter","MetaStep":"Filter","OwnedExecutionDuration":1}]},
            {"StatsScanId":"a","TabletId":"1","ReaderKind":"TRIVIAL","Finished":false,"Truncated":false,"PendingOperations":3,"Steps":[]}
        ])");
        UNIT_ASSERT_C(finished.Contains("data-cs-pending-attempts='0'"), finished);
        UNIT_ASSERT_C(!finished.Contains("PARTIAL population"), finished);
    }

    Y_UNIT_TEST(MetaStatsAggregateTabletTotalsAndMeasuredZero) {
        auto svg = RenderScans(R"([
            {"StatsScanId":"a","TabletId":"10","ReaderKind":"TRIVIAL","Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Calculation","MetaStep":"Filter","Operation":"first","OwnedExecutionDuration":5},
                {"StepId":2,"StepKind":"Calculation","MetaStep":"Filter","Operation":"second","OwnedExecutionDuration":15}]},
            {"StatsScanId":"b","TabletId":20,"ReaderKind":"SIMPLE","Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Calculation","MetaStep":"Filter","OwnedExecutionDuration":40}]},
            {"StatsScanId":"c","TabletId":"30","ReaderKind":"TRIVIAL","Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Calculation","MetaStep":"Filter","OwnedExecutionDuration":0}]},
            {"StatsScanId":"d","TabletId":"40","ReaderKind":"TRIVIAL","Finished":true,"Truncated":false,"Steps":[]}
        ])");
        auto row = CsRow(svg, "Filter", "", "Calculation");
        UNIT_ASSERT_C(row.Contains("data-cs-active-shards='3'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-participating-shards='4'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-calls='4'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-active-sum='60'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-active-mean='20'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-active-min='0'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-active-max='40'"), row);
        UNIT_ASSERT_C(CsOccurrences(svg, "data-cs-meta='Filter' data-cs-path='' data-cs-kind='Calculation'") == 1, svg);
    }

    Y_UNIT_TEST(MetaStatsCanonicalKindsAndParentPaths) {
        auto svg = RenderScans(R"([
            {"StatsScanId":"a","TabletId":"10","ReaderKind":"TRIVIAL","Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"MemoryReserve","MetaStep":"Filter","Operation":"reader","OwnedExecutionDuration":1},
                {"StepId":2,"StepKind":"Reserve","MetaStep":"Filter","Operation":"SSA","OwnedExecutionDuration":2},
                {"StepId":3,"StepKind":"FetchColumns","MetaStep":"Filter","OwnedExecutionDuration":3},
                {"StepId":4,"StepKind":"Fetching","MetaStep":"PKFilter","OwnedExecutionDuration":4},
                {"StepId":5,"StepKind":"Decode","MetaStep":"Filter","CanonicalPath":"Assemble","OwnedExecutionDuration":5},
                {"StepId":6,"StepKind":"Decode","MetaStep":"Filter","CanonicalPath":"Calculation","OwnedExecutionDuration":6},
                {"StepId":7,"StepKind":"Filter","MetaStep":"PKFilter","Operation":"PK","OwnedExecutionDuration":7},
                {"StepId":8,"StepKind":"Filter","MetaStep":"PKFilter","Operation":"deletion","OwnedExecutionDuration":8},
                {"StepId":9,"StepKind":"Limit","MetaStep":"Output","Operation":"first","OwnedExecutionDuration":1},
                {"StepId":10,"StepKind":"Limit","MetaStep":"Output","Operation":"second","OwnedExecutionDuration":1}]}
        ])");
        UNIT_ASSERT_C(CsRow(svg, "Filter", "", "ReserveMemory").Contains("data-cs-calls='2'"), svg);
        UNIT_ASSERT_C(CsRow(svg, "PKFilter", "", "Filter").Contains("data-cs-calls='2'"), svg);
        UNIT_ASSERT_C(CsRow(svg, "Output", "", "Limit").Contains("data-cs-calls='2'"), svg);
        CsRow(svg, "Filter", "", "Fetching");
        CsRow(svg, "PKFilter", "", "Fetching");
        CsRow(svg, "Filter", "Assemble", "Decode");
        CsRow(svg, "Filter", "Calculation", "Decode");
        UNIT_ASSERT_C(CsOccurrences(svg, "data-cs-meta='Filter' data-cs-path='' data-cs-kind='ReserveMemory'") == 1, svg);
        const auto index = svg.find("data-cs-meta='IndexFilter' data-cs-path='' data-cs-kind=''");
        const auto filter = svg.find("data-cs-meta='Filter' data-cs-path='' data-cs-kind=''");
        const auto aggregation = svg.find("data-cs-meta='Aggregation' data-cs-path='' data-cs-kind=''");
        UNIT_ASSERT(index < filter && filter < aggregation);
    }

    Y_UNIT_TEST(MetaStatsSeparateReaderLayoutsAndPlainMerge) {
        auto svg = RenderScans(R"([
            {"StatsScanId":"a","TabletId":"10","ReaderKind":"TRIVIAL","Finished":true,"Truncated":false,"Steps":[]},
            {"StatsScanId":"b","TabletId":"20","ReaderKind":"PLAIN","Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Merge","MetaStep":"PKFilter","OwnedExecutionDuration":20},
                {"StepId":2,"StepKind":"Filter","MetaStep":"Filter","OwnedExecutionDuration":3},
                {"StepId":3,"StepKind":"IndexCheck","MetaStep":"IndexFilter","OwnedExecutionDuration":1}]}
        ])");
        UNIT_ASSERT_VALUES_EQUAL(CsOccurrences(svg, "data-columnshard='read'"), 2);
        CsRow(svg, "PKFilter", "", "Merge", "PLAIN");
        UNIT_ASSERT_C(!svg.Contains("data-cs-meta='Merge'"), svg);
        UNIT_ASSERT_C(svg.Contains("inputs materialized before merge"), svg);
        UNIT_ASSERT_C(svg.Contains("no physical index fetching after merge"), svg);
        UNIT_ASSERT_C(svg.Contains("program-only checks"), svg);
    }

    Y_UNIT_TEST(MetaStatsOwnedWorkAndWaitFrontier) {
        auto svg = RenderScans(R"([
            {"StatsScanId":"a","TabletId":"10","ReaderKind":"TRIVIAL","Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Assemble","MetaStep":"Filter","IntegralExecutionDuration":10,"OwnedExecutionDuration":2,"IntegralWaitDuration":0},
                {"StepId":2,"StepKind":"Decode","MetaStep":"Filter","CanonicalPath":"Assemble","IntegralExecutionDuration":8,"OwnedExecutionDuration":8,"IntegralWaitDuration":0},
                {"StepId":3,"StepKind":"FetchColumns","MetaStep":"Filter","OwnedExecutionDuration":1,"IntegralWaitDuration":20},
                {"StepId":4,"StepKind":"StorageRead","MetaStep":"Filter","CanonicalPath":"Fetching","OwnedExecutionDuration":1,"IntegralWaitDuration":17}]}
        ])");
        auto meta = CsRow(svg, "Filter", "", "");
        UNIT_ASSERT_C(meta.Contains("data-cs-active-sum='12'"), meta);
        UNIT_ASSERT_C(meta.Contains("data-cs-wait-sum='20'"), meta);
        UNIT_ASSERT_C(CsRow(svg, "Filter", "", "Assemble").Contains("data-cs-active-sum='2'"), svg);
        UNIT_ASSERT_C(CsRow(svg, "Filter", "Assemble", "Decode").Contains("data-cs-active-sum='8'"), svg);
    }

    Y_UNIT_TEST(MetaStatsMissingSkippedAndPartialAreDifferent) {
        auto svg = RenderScans(R"([
            {"StatsScanId":"a","TabletId":"10","ReaderKind":"TRIVIAL","Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Decode","MetaStep":"Filter","OwnedExecutionDuration":0}]},
            {"StatsScanId":"b","TabletId":"20","ReaderKind":"TRIVIAL","Finished":true,"Truncated":true,"Steps":[
                {"StepId":1,"StepKind":"Decode","MetaStep":"Filter","IntegralExecutionDuration":8}]},
            {"StatsScanId":"c","TabletId":"30","ReaderKind":"TRIVIAL","Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Decode","MetaStep":"Filter","Outcome":"skipped","OwnedExecutionDuration":0}]}
        ])");
        auto row = CsRow(svg, "Filter", "", "Decode");
        UNIT_ASSERT_C(row.Contains("data-cs-active-shards='2'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-calls='2'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-active-known-shards='1'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-active-eligible-shards='2'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-partial='true'"), row);
        UNIT_ASSERT_C(svg.Contains("PARTIAL population"), svg);
        UNIT_ASSERT_C(svg.Contains("skipped=1"), svg);
        auto missingOwned = RenderScans(R"([
            {"StatsScanId":"a","TabletId":"1","ReaderKind":"TRIVIAL","StatsGroupId":"g","ParticipatingShards":1,"ParticipatingTablets":["1"],"Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Decode","MetaStep":"Filter","IntegralExecutionDuration":8}]}
        ])");
        UNIT_ASSERT_C(CsRow(missingOwned, "Filter", "", "Decode").Contains("data-cs-partial='true'"), missingOwned);
    }

    Y_UNIT_TEST(MetaStatsCacheUsesRatioOfSumsAndCompleteOrigins) {
        auto svg = RenderScans(R"([
            {"StatsScanId":"a","TabletId":"10","ReaderKind":"TRIVIAL","StatsGroupId":"g","ParticipatingShards":2,"ParticipatingTablets":["10","20"],"Finished":true,"Truncated":false,"Steps":[],
             "CacheStats":{"CompletedReadRanges":1,"CacheServedReadRanges":1,"ReturnedBytes":1,"CacheServedBytes":1}},
            {"StatsScanId":"b","TabletId":"20","ReaderKind":"TRIVIAL","StatsGroupId":"g","ParticipatingShards":2,"Finished":true,"Truncated":false,"Steps":[],
             "CacheStats":{"CompletedReadRanges":99,"CacheServedReadRanges":0,"ReturnedBytes":99,"CacheServedBytes":0}}
        ])");
        UNIT_ASSERT_C(svg.Contains("cache served ranges 1.0%; bytes 1.0%"), svg);
        UNIT_ASSERT_C(svg.Contains("data-cs-cache-bytes='100' data-cs-cache-served-bytes='1'"), svg);
        auto partial = RenderScans(R"([
            {"StatsScanId":"a","TabletId":"10","ReaderKind":"TRIVIAL","Finished":true,"Truncated":false,"Steps":[],
             "CacheStats":{"CompletedReadRanges":1,"ReturnedBytes":1}}
        ])");
        UNIT_ASSERT_C(partial.Contains("cache ratios unavailable"), partial);
    }

    Y_UNIT_TEST(MetaStatsRepeatedSnapshotsReplaceObservations) {
        auto svg = RenderScans(R"([
            {"StatsScanId":"a","TabletId":"10","ReaderKind":"TRIVIAL","StatsGroupId":"g","ParticipatingShards":1,"ParticipatingTablets":["10"],"Finished":false,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Calculation","MetaStep":"Filter","OwnedExecutionDuration":3}]},
            {"StatsScanId":"a","Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Calculation","MetaStep":"Filter","OwnedExecutionDuration":9}]},
            {"StatsScanId":"a","Finished":true,"Truncated":false,"Steps":[]}
        ])");
        auto row = CsRow(svg, "Filter", "", "Calculation");
        UNIT_ASSERT_C(row.Contains("data-cs-active-shards='1'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-calls='1'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-active-sum='9'"), row);
        UNIT_ASSERT_C(!svg.Contains("PARTIAL population"), svg);
    }

    Y_UNIT_TEST(MetaStatsCohortRostersGiveAuthoritativeDenominator) {
        auto svg = RenderScans(R"([
            {"StatsScanId":"a","TabletId":"10","ReaderKind":"TRIVIAL","StatsGroupId":"g1","ParticipatingShards":2,"ParticipatingTablets":["10","11"],"Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Filter","MetaStep":"Filter","OwnedExecutionDuration":5}]},
            {"StatsScanId":"b","TabletId":"20","ReaderKind":"TRIVIAL","StatsGroupId":"g2","ParticipatingShards":3,"ParticipatingTablets":["20","21","22"],"Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Filter","MetaStep":"Filter","OwnedExecutionDuration":7}]},
            {"StatsScanId":"c","TabletId":"21","ReaderKind":"TRIVIAL","StatsGroupId":"g2","ParticipatingShards":3,"Finished":true,"Truncated":false,"Steps":[]}
        ])");
        auto row = CsRow(svg, "Filter", "", "Filter");
        UNIT_ASSERT_C(row.Contains("data-cs-participating-shards='5'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-received-shards='3'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-active-shards='2'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-expected-known='true'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-denominator='expected'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-partial='false'"), row);
        UNIT_ASSERT_C(svg.Contains("not observed / may not have started"), svg);
    }

    Y_UNIT_TEST(MetaStatsRepeatedCohortHeaderDoesNotMultiplyDenominator) {
        auto svg = RenderScans(R"([
            {"StatsScanId":"a","TabletId":"1","ReaderKind":"TRIVIAL","StatsGroupId":"g","ParticipatingShards":2,"ParticipatingTablets":["1","2"],"Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Filter","MetaStep":"Filter","OwnedExecutionDuration":1}]},
            {"StatsScanId":"b","TabletId":"2","ReaderKind":"TRIVIAL","StatsGroupId":"g","ParticipatingShards":2,"ParticipatingTablets":[],"Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Filter","MetaStep":"Filter","OwnedExecutionDuration":1}]}
        ])");
        auto row = CsRow(svg, "Filter", "", "Filter");
        UNIT_ASSERT_C(row.Contains("data-cs-participating-shards='2'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-expected-known='true'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-partial='false'"), row);
    }

    Y_UNIT_TEST(MetaStatsOverlappingCohortRostersUsePhysicalTabletUnion) {
        auto svg = RenderScans(R"([
            {"StatsScanId":"a","TabletId":"1","ReaderKind":"TRIVIAL","StatsGroupId":"g1","ParticipatingShards":2,"ParticipatingTablets":["1","2"],"Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Filter","MetaStep":"Filter","OwnedExecutionDuration":1}]},
            {"StatsScanId":"b","TabletId":"2","ReaderKind":"TRIVIAL","StatsGroupId":"g1","ParticipatingShards":2,"Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Filter","MetaStep":"Filter","OwnedExecutionDuration":1}]},
            {"StatsScanId":"c","TabletId":"2","ReaderKind":"TRIVIAL","StatsGroupId":"g2","ParticipatingShards":3,"ParticipatingTablets":["2","3","4"],"Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Filter","MetaStep":"Filter","OwnedExecutionDuration":1}]},
            {"StatsScanId":"d","TabletId":"3","ReaderKind":"TRIVIAL","StatsGroupId":"g2","ParticipatingShards":3,"Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Filter","MetaStep":"Filter","OwnedExecutionDuration":1}]},
            {"StatsScanId":"e","TabletId":"4","ReaderKind":"TRIVIAL","StatsGroupId":"g2","ParticipatingShards":3,"Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Filter","MetaStep":"Filter","OwnedExecutionDuration":1}]}
        ])");
        auto row = CsRow(svg, "Filter", "", "Filter");
        UNIT_ASSERT_C(row.Contains("data-cs-participating-shards='4'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-active-shards='4'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-calls='5'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-active-sum='5'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-active-mean='1.25'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-partial='false'"), row);
    }

    Y_UNIT_TEST(MetaStatsMissingAndConflictingCohortHeadersStayUnknown) {
        auto conflicting = RenderScans(R"([
            {"StatsScanId":"a","TabletId":"1","ReaderKind":"TRIVIAL","StatsGroupId":"g","ParticipatingShards":2,"ParticipatingTablets":["1","2"],"Finished":true,"Truncated":false,"Steps":[]},
            {"StatsScanId":"b","TabletId":"2","ReaderKind":"TRIVIAL","StatsGroupId":"g","ParticipatingShards":3,"Finished":true,"Truncated":false,"Steps":[]}
        ])");
        auto conflictRow = CsRow(conflicting, "Filter", "", "");
        UNIT_ASSERT_C(conflictRow.Contains("data-cs-expected-known='false'"), conflictRow);
        UNIT_ASSERT_C(conflictRow.Contains("data-cs-denominator='received'"), conflictRow);
        auto missing = RenderScans(R"([
            {"StatsScanId":"a","TabletId":"1","ReaderKind":"TRIVIAL","StatsGroupId":"g","ParticipatingShards":2,"ParticipatingTablets":["1","2"],"Finished":true,"Truncated":false,"Steps":[]},
            {"StatsScanId":"b","TabletId":"3","ReaderKind":"TRIVIAL","Finished":true,"Truncated":false,"Steps":[]}
        ])");
        auto missingRow = CsRow(missing, "Filter", "", "");
        UNIT_ASSERT_C(missingRow.Contains("data-cs-expected-known='false'"), missingRow);
        UNIT_ASSERT_C(missingRow.Contains("data-cs-known-expected-shards='2'"), missingRow);
        auto countOnly = RenderScans(R"([
            {"StatsScanId":"a","TabletId":"1","ReaderKind":"TRIVIAL","StatsGroupId":"g","ParticipatingShards":99,"Finished":true,"Truncated":false,"Steps":[]}
        ])");
        auto countRow = CsRow(countOnly, "Filter", "", "");
        UNIT_ASSERT_C(countRow.Contains("data-cs-participating-shards='1'"), countRow);
        UNIT_ASSERT_C(countRow.Contains("data-cs-denominator='received'"), countRow);
    }

    Y_UNIT_TEST(MetaStatsExpectedContextIsNotAnExecution) {
        auto svg = RenderScans(R"([
            {"StatsScanId":"g:expected","ExpectedContextOnly":true,"StatsGroupId":"g","ParticipatingShards":16,
             "ParticipatingTablets":["1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16"]},
            {"StatsScanId":"a","TabletId":"1","ReaderKind":"TRIVIAL","StatsGroupId":"g","ParticipatingShards":16,"Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Filter","MetaStep":"Filter","OwnedExecutionDuration":7}]}
        ])");
        auto row = CsRow(svg, "Filter", "", "Filter");
        UNIT_ASSERT_C(row.Contains("data-cs-participating-shards='16'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-received-shards='1'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-active-shards='1'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-calls='1'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-partial='false'"), row);
        UNIT_ASSERT_C(svg.Contains("data-cs-attempts='1'"), svg);
        UNIT_ASSERT_C(!svg.Contains("PARTIAL population"), svg);
        UNIT_ASSERT_VALUES_EQUAL(CsOccurrences(svg, "data-columnshard='read'"), 1);
        auto headerOnly = RenderScans(R"([
            {"StatsScanId":"g:expected","ExpectedContextOnly":true,"StatsGroupId":"g","ParticipatingShards":2,"ParticipatingTablets":["1","2"]}
        ])");
        UNIT_ASSERT_C(headerOnly.Contains("data-columnshard='read'"), headerOnly);
        UNIT_ASSERT_C(headerOnly.Contains("data-cs-attempts='0'"), headerOnly);
        UNIT_ASSERT_C(headerOnly.Contains("reader/runtime not observed"), headerOnly);
        UNIT_ASSERT_C(headerOnly.Contains("data-cs-received-shards='0'"), headerOnly);
        UNIT_ASSERT_C(headerOnly.Contains("data-cs-participating-shards='2'"), headerOnly);
        UNIT_ASSERT_C(!headerOnly.Contains("data-cs-active-shards='1'"), headerOnly);
        auto empty = RenderScans(R"([
            {"StatsScanId":"g:expected","ExpectedContextOnly":true,"StatsGroupId":"g","ParticipatingShards":0}
        ])");
        UNIT_ASSERT_C(empty.Contains("data-cs-attempts='0'"), empty);
        UNIT_ASSERT_C(empty.Contains("data-cs-participating-shards='0'"), empty);
        UNIT_ASSERT_C(empty.Contains("data-cs-expected-known='true'"), empty);
    }

    Y_UNIT_TEST(MetaStatsRosterRegistryIsSharedBetweenReaderFamilies) {
        auto svg = RenderScans(R"([
            {"StatsScanId":"a","TabletId":"1","ReaderKind":"TRIVIAL","StatsGroupId":"g","ParticipatingShards":2,"ParticipatingTablets":["1","2"],"Finished":true,"Truncated":false,"Steps":[]},
            {"StatsScanId":"b","TabletId":"2","ReaderKind":"PLAIN","StatsGroupId":"g","ParticipatingShards":2,"Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Merge","MetaStep":"PKFilter","OwnedExecutionDuration":1}]}
        ])");
        auto row = CsRow(svg, "PKFilter", "", "Merge", "PLAIN");
        UNIT_ASSERT_C(row.Contains("data-cs-participating-shards='2'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-received-shards='2'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-family-shards='1'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-expected-known='true'"), row);
    }

    Y_UNIT_TEST(MetaStatsPreparationCountersAreMetadataEstimates) {
        auto svg = RenderScans(R"([
            {"StatsScanId":"a","TabletId":"1","ReaderKind":"TRIVIAL","StatsGroupId":"g","ParticipatingShards":2,"ParticipatingTablets":["1","2"],"Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"SelectPortions","MetaStep":"Preparation","OwnedExecutionDuration":1,
                 "Details":{"SelectedPortions":2,"SelectedVersionRows":10,"SelectedBlobBytes":100}}]},
            {"StatsScanId":"b","TabletId":"2","ReaderKind":"TRIVIAL","StatsGroupId":"g","ParticipatingShards":2,"Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"SelectPortions","MetaStep":"Preparation","OwnedExecutionDuration":1,
                 "Details":{"SelectedPortions":3,"SelectedVersionRows":30}}]}
        ])");
        auto row = CsRow(svg, "Preparation", "", "");
        UNIT_ASSERT_C(row.Contains("data-cs-selected-portions='5'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-selected-portions-known-shards='2'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-selected-version-rows='40'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-selected-blob-bytes='100'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-selected-blob-bytes-known-shards='1'"), row);
        UNIT_ASSERT_C(svg.Contains("metadata versions"), svg);
        UNIT_ASSERT_C(svg.Contains("not actual read"), svg);
        UNIT_ASSERT_C(CsRow(svg, "Preparation", "", "SelectPortions").Contains("data-cs-selected-portions='5'"), svg);
    }

    Y_UNIT_TEST(MetaStatsOutputCountersKeepPacketAndArrowBoundaries) {
        auto svg = RenderScans(R"([
            {"StatsScanId":"a","TabletId":"1","ReaderKind":"TRIVIAL","StatsGroupId":"g","ParticipatingShards":2,"ParticipatingTablets":["1","2"],"Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Send","MetaStep":"Output","OwnedExecutionDuration":1,"Details":{"Rows":2,"DataBatches":1,"ArrowBytes":16}},
                {"StepId":2,"StepKind":"Send","MetaStep":"Output","OwnedExecutionDuration":1,"Details":{"Rows":0,"DataBatches":0}},
                {"StepId":3,"StepKind":"BuildResult","MetaStep":"BuildResult","OwnedExecutionDuration":1,"Details":{"ResultRows":2,"ResultBytes":16}}]},
            {"StatsScanId":"b","TabletId":"2","ReaderKind":"TRIVIAL","StatsGroupId":"g","ParticipatingShards":2,"Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Send","MetaStep":"Output","OwnedExecutionDuration":1,"Details":{"Rows":0,"DataBatches":0}}]}
        ])");
        auto row = CsRow(svg, "Output", "", "");
        UNIT_ASSERT_C(row.Contains("data-cs-calls='3'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-active-shards='2'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-output-rows='2'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-output-data-batches='1'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-output-arrow-bytes='16'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-output-arrow-bytes-known-shards='1'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-output-arrow-bytes-eligible-shards='1'"), row);
        UNIT_ASSERT_C(CsRow(svg, "Output", "", "Send").Contains("data-cs-output-rows='2'"), svg);
        UNIT_ASSERT_C(svg.Contains("not serialized/network bytes"), svg);
        auto unknown = RenderScans(R"([
            {"StatsScanId":"a","TabletId":"1","ReaderKind":"TRIVIAL","StatsGroupId":"g","ParticipatingShards":1,"ParticipatingTablets":["1"],"Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Send","MetaStep":"Output","OwnedExecutionDuration":1,"Details":{"Rows":2,"DataBatches":1}}]}
        ])");
        UNIT_ASSERT_C(CsRow(unknown, "Output", "", "Send").Contains("data-cs-output-arrow-bytes-known-shards='0'"), unknown);
        auto zero = RenderScans(R"([
            {"StatsScanId":"a","TabletId":"1","ReaderKind":"TRIVIAL","StatsGroupId":"g","ParticipatingShards":1,"ParticipatingTablets":["1"],"Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Send","MetaStep":"Output","OwnedExecutionDuration":0,"Details":{"Rows":0,"DataBatches":0,"ArrowBytes":0}}]}
        ])");
        UNIT_ASSERT_C(CsRow(zero, "Output", "", "Send").Contains("data-cs-output-arrow-bytes-known-shards='1'"), zero);
    }

    Y_UNIT_TEST(MetaStatsSemanticLabelsPreserveVariantsAndNamedInputs) {
        auto svg = RenderScans(R"json([
            {"StatsScanId":"a","TabletId":"10","ReaderKind":"TRIVIAL","Finished":true,"Steps":[
                {"StepId":1,"StepKind":"Filter","MetaStep":"Filter","OwnedExecutionDuration":1,"Operation":"Filter((column_a > 10) AND (column_c < 300))"},
                {"StepId":2,"StepKind":"FetchColumns","MetaStep":"FetchOtherColumns","OwnedExecutionDuration":1,"Details":{"Columns":["payload","category"]}},
                {"StepId":3,"StepKind":"Aggregation","MetaStep":"Aggregation","OwnedExecutionDuration":1,"Details":{"Keys":["category"],"Functions":["COUNT(*)","SUM(metric)"]}},
                {"StepId":4,"StepKind":"Projection","MetaStep":"Projection","OwnedExecutionDuration":1,"Details":{"OutputColumns":["category","SUM(metric)"]}}]},
            {"StatsScanId":"b","TabletId":"11","ReaderKind":"TRIVIAL","Finished":true,"Steps":[
                {"StepId":1,"StepKind":"Filter","MetaStep":"Filter","OwnedExecutionDuration":1,"Operation":"Filter(column_a = 5)"}]}
        ])json");
        const auto filter = CsRow(svg, "Filter", "", "Filter");
        UNIT_ASSERT_C(filter.Contains("data-cs-active-shards='2'"), filter);
        UNIT_ASSERT_C(svg.Contains("Filter ((column_a") && svg.Contains("column_c"), svg);
        UNIT_ASSERT_C(svg.Contains(" | column_a = 5)"), svg);
        UNIT_ASSERT_C(!svg.Contains("AND column_a = 5"), svg);
        UNIT_ASSERT_C(svg.Contains("Fetching (category, payload)"), svg);
        UNIT_ASSERT_C(svg.Contains("Aggregation (GROUP BY category: COUNT(*), SUM(metric))"), svg);
        UNIT_ASSERT_C(svg.Contains("Projection (SUM(metric), category)"), svg);
        UNIT_ASSERT_C(!svg.Contains("column_c < 300"), svg);
    }

    Y_UNIT_TEST(MetaStatsSemanticVariantsAreBoundedAndRetainedInTooltip) {
        TStringBuilder scans;
        scans << R"([{"StatsScanId":"a","TabletId":"10","ReaderKind":"TRIVIAL","Finished":true,"Steps":[)";
        for (ui32 i = 0; i < 70; ++i) {
            if (i) { scans << ','; }
            scans << "{\"StepId\":" << i + 1 << ",\"StepKind\":\"Filter\",\"MetaStep\":\"Filter\",\"OwnedExecutionDuration\":1,\"Operation\":\"Filter(column_" << i << " > 10)\"}";
        }
        scans << "]}]";
        const auto svg = RenderScans(scans);
        UNIT_ASSERT_C(svg.Contains("variants)"), svg);
        UNIT_ASSERT_C(svg.Contains("Description limit reached"), svg);
        UNIT_ASSERT_C(svg.Contains("column_63"), svg);
        UNIT_ASSERT_C(!svg.Contains("column_69"), svg);
    }

    Y_UNIT_TEST(MetaStatsKeepDifferentMetastepDetails) {
        auto svg = RenderScans(R"([
            {"StatsScanId":"a","TabletId":"10","ReaderKind":"TRIVIAL","Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Filter","MetaStep":"PKFilter","OwnedExecutionDuration":1,"Details":{"InputRows":10,"OutputRows":8,"Purpose":"Deletion"}},
                {"StepId":2,"StepKind":"IndexCheck","MetaStep":"IndexFilter","OwnedExecutionDuration":1,"Details":{"InputRows":8,"OutputRows":8,"IndexType":"BLOOM"}},
                {"StepId":3,"StepKind":"Filter","MetaStep":"Filter","OwnedExecutionDuration":1,"Details":{"InputRows":8,"OutputRows":2}},
                {"StepId":4,"StepKind":"FetchColumns","MetaStep":"FetchOtherColumns","OwnedExecutionDuration":1,"ReadBytes":100,"Details":{"RequestedBytes":200}},
                {"StepId":5,"StepKind":"Aggregation","MetaStep":"Aggregation","OwnedExecutionDuration":1,"Details":{"InputRows":2,"OutputRows":1}},
                {"StepId":6,"StepKind":"Projection","MetaStep":"Projection","OwnedExecutionDuration":1},
                {"StepId":7,"StepKind":"BuildResult","MetaStep":"BuildResult","OwnedExecutionDuration":1,"Details":{"ResultRows":1,"ResultBytes":16}}]}
        ])");
        for (TStringBuf meta : {TStringBuf("PKFilter"), TStringBuf("IndexFilter"), TStringBuf("Filter"), TStringBuf("FetchOtherColumns"),
                 TStringBuf("Aggregation"), TStringBuf("Projection"), TStringBuf("BuildResult")}) {
            UNIT_ASSERT_C(CsRow(svg, meta, "", "").Contains("data-cs-active-shards='1'"), svg);
        }
        UNIT_ASSERT_C(svg.Contains("check candidates"), svg);
        UNIT_ASSERT_C(svg.Contains("processed filter candidates (calls)"), svg);
        UNIT_ASSERT_C(svg.Contains("aggregate call rows"), svg);
        UNIT_ASSERT_C(svg.Contains("delivered"), svg);
        UNIT_ASSERT_C(svg.Contains("BLOOM"), svg);
    }

    Y_UNIT_TEST(MetaStatsOverflowAndEscapingRemainExplicit) {
        auto svg = RenderScans(R"([
            {"StatsScanId":"a","TabletId":"10","ReaderKind":"TRIVIAL","Finished":true,"Truncated":false,"Steps":[
                {"StepId":1,"StepKind":"Calculation","MetaStep":"Filter","Operation":"<script>bad&evil</script>","OwnedExecutionDuration":18446744073709551615},
                {"StepId":2,"StepKind":"Calculation","MetaStep":"Filter","OwnedExecutionDuration":1},
                {"StepId":3,"StepKind":"Filter' data-pwned='yes","MetaStep":"Filter","CanonicalPath":"Assemble' onload='evil","OwnedExecutionDuration":0}]}
        ])");
        auto row = CsRow(svg, "Filter", "", "Calculation");
        UNIT_ASSERT_C(row.Contains("data-cs-active-overflow='true'"), row);
        UNIT_ASSERT_C(svg.Contains("overflow"), svg);
        UNIT_ASSERT_C(!svg.Contains("<script>bad"), svg);
        UNIT_ASSERT_C(svg.Contains("&lt;script&gt;bad&amp;evil&lt;/script&gt;"), svg);
        UNIT_ASSERT_C(svg.Contains("data-cs-kind='Filter&apos; data-pwned=&apos;yes'"), svg);
        UNIT_ASSERT_C(svg.Contains("data-cs-path='Assemble&apos; onload=&apos;evil'"), svg);
        UNIT_ASSERT_C(!svg.Contains("data-cs-kind='Filter' data-pwned='yes'"), svg);
    }

    Y_UNIT_TEST(MetaStatsThousandTabletsDoNotMultiplyRows) {
        TStringBuilder scans;
        scans << '[';
        for (unsigned i = 0; i < 1000; ++i) {
            if (i) {
                scans << ',';
            }
            scans << "{\"StatsScanId\":\"scan-" << i << "\",\"TabletId\":" << i
                << ",\"ReaderKind\":\"TRIVIAL\",\"Finished\":true,\"Truncated\":false,\"Steps\":["
                << "{\"StepId\":1,\"StepKind\":\"Filter\",\"MetaStep\":\"Filter\",\"OwnedExecutionDuration\":1}]}";
        }
        scans << ']';
        auto svg = RenderScans(scans);
        UNIT_ASSERT_VALUES_EQUAL(CsOccurrences(svg, "data-columnshard='read'"), 1);
        UNIT_ASSERT_VALUES_EQUAL(CsOccurrences(svg, "data-cs-meta='Filter' data-cs-path='' data-cs-kind='Filter'"), 1);
        auto row = CsRow(svg, "Filter", "", "Filter");
        UNIT_ASSERT_C(row.Contains("data-cs-active-shards='1000'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-calls='1000'"), row);
        UNIT_ASSERT_C(row.Contains("data-cs-active-sum='1000'"), row);
    }

    Y_UNIT_TEST(AbsentAndEmptyStatisticsAreIdentical) {
        auto root = Json(ReadPlan("columnshard_steps"));
        auto* stats = root.GetValueByPath("Plan.Plans.[0].Plans.[0].Stats");
        (*stats)["ColumnShardScans"] = NJson::TJsonValue();
        TPlanVisualizer absent;
        absent.LoadPlans(root["Plan"]);
        (*stats)["ColumnShardScans"] = NJson::TJsonValue(NJson::JSON_ARRAY);
        TPlanVisualizer empty;
        empty.LoadPlans(root["Plan"]);
        UNIT_ASSERT_VALUES_EQUAL(absent.PrintSvg(), empty.PrintSvg());
        UNIT_ASSERT(!absent.PrintSvg().Contains("data-columnshard"));
    }

    Y_UNIT_TEST(ColumnShardDetailsBelongToStageAndStartCollapsed) {
        auto root = Json(ReadPlan("columnshard_steps"));
        TPlanVisualizer withStats;
        withStats.LoadPlans(root["Plan"]);
        *root.GetValueByPath("Plan.Plans.[0].Plans.[0].Stats.ColumnShardScans") = NJson::TJsonValue();
        TPlanVisualizer withoutStats;
        withoutStats.LoadPlans(root["Plan"]);
        auto before = withoutStats.PrintSvg();
        auto after = withStats.PrintSvg();
        auto heightStart = before.find("height='") + 8;
        auto oldHeight = FromString<ui32>(before.substr(heightStart, before.find('\'', heightStart) - heightStart));
        auto newHeightStart = after.find("height='") + 8;
        UNIT_ASSERT_VALUES_EQUAL(FromString<ui32>(after.substr(newHeightStart, after.find('\'', newHeightStart) - newHeightStart)), oldHeight);
        auto panel = after.find("<svg data-cs-panel='");
        auto read = after.find("<svg data-columnshard='scan'");
        UNIT_ASSERT(panel != TString::npos && read > panel);
        UNIT_ASSERT(after.substr(panel, read - panel).Contains("height='0' style='display:none'"));
        UNIT_ASSERT(after.Contains("class='cs-stage-toggle button' role='button' tabindex='0' aria-expanded='false'"));
        UNIT_ASSERT(after.find("data-stage='inner ") < panel);
        UNIT_ASSERT(read < after.find("<g id='columnHeaders'>"));
        AssertWellFormed(after, "ColumnShard details inside stage");
    }

    Y_UNIT_TEST(ColumnShardLongDescriptionsFitCellsAndKeepFullTooltip) {
        const auto svg = RenderScans(R"json([{"StatsScanId":"wide","TabletId":"1","ReaderKind":"TRIVIAL","Finished":true,"Steps":[
            {"StepId":1,"StepKind":"Filter","MetaStep":"Filter","CanonicalPath":"","OwnedExecutionDuration":1,
             "Operation":"Filter(колонка_с_длинным_именем > 10 AND another_very_long_column_name < 300 AND final_column = 42)"}]}])json");
        const auto title = svg.find("<title>Filter (колонка_с_длинным_именем");
        UNIT_ASSERT_C(title != TString::npos, svg);
        const auto text = svg.find("<text", title);
        const auto end = svg.find("</text>", text);
        const auto display = svg.substr(text, end - text);
        UNIT_ASSERT_C(display.Contains("..."), display);
        UNIT_ASSERT_C(!display.Contains("final_column"), display);
        UNIT_ASSERT_C(svg.substr(title, text - title).Contains("final_column = 42"), svg);
        UNIT_ASSERT_C(svg.Contains("style='overflow:hidden'><title>Filter ("), svg);
        AssertWellFormed(svg, "long UTF-8 descriptions remain clipped and retain complete tooltips");
    }

    Y_UNIT_TEST(ColumnShardPanelsDoNotLeakBetweenStages) {
        TVisualizer viz;
        viz.LoadPlans(ReadPlan("cte_subplan"));
        std::vector<std::shared_ptr<TStage>> owners;
        for (const auto& plan : viz.Plans) {
            for (const auto& stage : plan->Stages) {
                if (!stage->External && owners.size() < 2) {
                    owners.push_back(stage);
                }
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(owners.size(), 2);
        for (size_t i = 0; i < owners.size(); ++i) {
            owners[i]->ColumnShardScans = Json(TStringBuilder() << "[{\"StatsScanId\":\"owner-" << i
                << "\",\"TabletId\":\"" << i + 1 << "\",\"ReaderKind\":\"TRIVIAL\",\"Finished\":true,"
                << "\"Truncated\":false,\"Steps\":[{\"StepId\":1,\"StepKind\":\"Filter\","
                << "\"MetaStep\":\"Filter\",\"CanonicalPath\":\"\",\"OwnedExecutionDuration\":" << 10 + i << "}]}]");
        }
        auto svg = viz.PrintSvg();
        UNIT_ASSERT_VALUES_EQUAL(CsOccurrences(svg, "<svg data-cs-panel='"), 2);
        UNIT_ASSERT_VALUES_EQUAL(CsOccurrences(svg, "<g class='cs-stage-toggle button'"), 2);
        for (size_t i = 0; i < owners.size(); ++i) {
            UNIT_ASSERT(owners[i]->ColumnShardHeight > 0);
            UNIT_ASSERT_C(owners[i]->ColumnShardSvg.Contains(TStringBuilder() << "data-cs-owned-sum='" << 10 + i << '\''), owners[i]->ColumnShardSvg);
            UNIT_ASSERT(!owners[i]->ColumnShardSvg.Contains(TStringBuilder() << "data-cs-owned-sum='" << 11 - i << '\''));
        }
        AssertWellFormed(svg, "separate stage-owned ColumnShard panels");
    }

    Y_UNIT_TEST(GroupsPreserveZerosCoverageAndIndependentExtrema) {
        auto svg = RenderPlan("columnshard_steps", false);
        AssertWellFormed(svg, "ColumnShard fixture");
        UNIT_ASSERT(svg.Contains("Active elapsed: coverage 2/2, sum 20us, min 0.00s, max 20us, mean 10us, argmin StepId 2, argmax StepId 3"));
        UNIT_ASSERT(svg.Contains("Wait: coverage 1/2, sum 5us"));
        UNIT_ASSERT(svg.Contains("Read bytes (this boundary only): coverage 1/2, sum 0B"));
        UNIT_ASSERT(svg.Contains("Max active example:"));
        UNIT_ASSERT(svg.Contains("Max wait example:"));
        UNIT_ASSERT(svg.Contains("rows 2.00K → 110 (5.5%)"));
        UNIT_ASSERT(svg.Contains("100B → 1000B (10.0x)"));
        UNIT_ASSERT(svg.Contains("requested 1.00KB; example archive&lt;&amp;/ExternalStorage"));
        UNIT_ASSERT(svg.Contains("quota 8.00MB"));
        UNIT_ASSERT(svg.Contains("18446744073709551615"));
        UNIT_ASSERT(svg.Contains("scan&lt;&amp;1"));
        UNIT_ASSERT(svg.Contains("value &lt; 10 &amp; valid"));
        UNIT_ASSERT(svg.Contains("PARTIAL population (truncated)"));
        UNIT_ASSERT(svg.Contains("bytes 25.0%, ranges 40.0%"));
        UNIT_ASSERT(svg.Contains("ratios unavailable (partial or unclassified population)"));
        UNIT_ASSERT(svg.Contains("class='plus button'"));
    }

    Y_UNIT_TEST(StringInputOwnsStatisticsAfterLoadReturns) {
        TVisualizer viz;
        viz.LoadPlans(ReadPlan("columnshard_steps"));
        UNIT_ASSERT_VALUES_EQUAL(viz.Plans[0]->Stages[0]->ColumnShardScans.GetArray().size(), 2);
        TString overwrite(1 << 20, 'x');
        UNIT_ASSERT(viz.PrintSvg().Contains("scan&lt;&amp;1"));
        Y_UNUSED(overwrite);
        auto stage = viz.Plans[0]->Stages[0];
        stage->StatsNode = nullptr;
        viz.Plans[0]->LoadStageStats(stage, nullptr, nullptr);
        UNIT_ASSERT(!viz.PrintSvg().Contains("data-columnshard"));
    }

    Y_UNIT_TEST(MalformedOptionalValuesDoNotBecomeMeasuredZeros) {
        auto svg = RenderScans(R"([null,{"StatsScanId":"invalid","Finished":true,"Truncated":false,"Steps":[
            null,{"StepId":1,"StepKind":"Filter","IntegralExecutionDuration":-1,"IntegralWaitDuration":"12",
            "ReadBytes":false,"Details":{"InputRows":1.5,"OutputRows":null}}],
            "CacheStats":{"CompletedReadRanges":1,"CacheServedReadRanges":2,"ReturnedBytes":0,"CacheServedBytes":0}}])");
        UNIT_ASSERT(svg.Contains("Active elapsed: coverage 0/1"));
        UNIT_ASSERT(svg.Contains("Wait: coverage 0/1"));
        UNIT_ASSERT(svg.Contains("Input rows: coverage 0/1"));
        UNIT_ASSERT(svg.Contains("rows — → —"));
        UNIT_ASSERT(!svg.Contains("bytes 0.0%"));
        UNIT_ASSERT(svg.Contains("ratios unavailable"));
        UNIT_ASSERT(!RenderScans("{}").Contains("data-columnshard"));
    }

    Y_UNIT_TEST(UnsignedValuesAndOverflowDoNotWrap) {
        auto svg = RenderScans(R"([{"StatsScanId":"large","Finished":true,"Truncated":false,"Steps":[
            {"StepId":18446744073709551615,"StepKind":"Reader","Operation":"read","ReadBytes":18446744073709551615},
            {"StepId":2,"StepKind":"Reader","Operation":"read","ReadBytes":1}]}])");
        UNIT_ASSERT(svg.Contains("Read bytes (this boundary only): coverage 2/2, sum overflow"));
        UNIT_ASSERT(svg.Contains("argmax StepId 18446744073709551615"));
    }
}
