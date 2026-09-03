#include <ydb/library/computation_graph/computation_graph.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NComputationGraph;

// GroupByHop fixture — meta + Plan only; Stats absent.
static const TString PlanWithoutStats = R"({
    "meta": {"version": "0.2", "type": "query"},
    "Plan": {
        "Node Type": "Query",
        "PlanNodeType": "Query",
        "Plans": [{
            "PlanNodeId": 5,
            "Node Type": "Sink",
            "Operators": [{"Name": "Write pq", "SinkType": "pq", "ExternalDataSource": "pq", "Inputs": []}],
            "Plans": [{
                "PlanNodeId": 4,
                "Node Type": "Stage",
                "Plans": [{
                    "PlanNodeId": 3,
                    "Node Type": "HashShuffle",
                    "PlanNodeType": "Connection",
                    "Plans": [{
                        "PlanNodeId": 2,
                        "Node Type": "Stage",
                        "Plans": [{
                            "PlanNodeId": 1,
                            "Node Type": "Source",
                            "Operators": [{"Name": "Read pq", "SourceType": "pq", "ExternalDataSource": "pq", "Inputs": []}]
                        }]
                    }]
                }]
            }]
        }]
    }
})";

static const TString PlanWithStats = R"({
    "meta": {"version": "0.2", "type": "query"},
    "Plan": {
        "Node Type": "Query",
        "PlanNodeType": "Query",
        "Plans": [{
            "PlanNodeId": 5,
            "Node Type": "Sink",
            "Operators": [{"Name": "Write pq", "SinkType": "pq", "ExternalDataSource": "pq", "Inputs": []}],
            "Stats": {
                "Tasks": 1, "FinishedTasks": 1,
                "EgressRows":  {"Min": 40, "Max": 60, "Sum": 100, "Count": 2},
                "EgressBytes": {"Min": 400, "Max": 600, "Sum": 1000, "Count": 2}
            },
            "Plans": [{
                "PlanNodeId": 4,
                "Node Type": "Stage",
                "Stats": {"Tasks": 0, "FinishedTasks": 0},
                "Plans": [{
                    "PlanNodeId": 3,
                    "Node Type": "HashShuffle",
                    "PlanNodeType": "Connection",
                    "Plans": [{
                        "PlanNodeId": 2,
                        "Node Type": "Stage",
                        "Stats": {
                            "Tasks": 2, "FinishedTasks": 0,
                            "OutputRows":  {"Min": 40, "Max": 60, "Sum": 100, "Count": 2},
                            "OutputBytes": {"Min": 400, "Max": 600, "Sum": 1000, "Count": 2},
                            "CpuTimeUs":   {"Min": 1000, "Max": 3000, "Sum": 4000, "Count": 2}
                        },
                        "Plans": [{
                            "PlanNodeId": 1,
                            "Node Type": "Source",
                            "Operators": [{"Name": "Read pq", "SourceType": "pq", "ExternalDataSource": "pq", "Inputs": []}],
                            "Stats": {
                                "Tasks": 2, "FinishedTasks": 0,
                                "IngressRows":  {"Min": 40, "Max": 60, "Sum": 100, "Count": 2},
                                "IngressBytes": {"Min": 400, "Max": 600, "Sum": 1000, "Count": 2}
                            }
                        }]
                    }]
                }]
            }]
        }]
    }
})";

namespace {

TGraph Build(TStringBuf json) {
    NJson::TJsonValue doc;
    NJson::ReadJsonTree(json, &doc, false);
    return BuildGraph(doc);
}

bool HasLink(const TGraph& g, ui32 from, ui32 to) {
    for (const auto& lnk : g.Links) {
        if (lnk.Source == from && lnk.Target == to) {
            return true;
        }
    }
    return false;
}

const TNode& NodeByName(const TGraph& g, TStringBuf name) {
    for (const auto& n : g.Nodes) {
        if (n.Name == name) {
            return n;
        }
    }
    UNIT_ASSERT_C(false, TString("node not found: ") + name);
    return g.Nodes.front();
}

} // namespace

Y_UNIT_TEST_SUITE(ComputationGraph) {

Y_UNIT_TEST(EmptyPlanGivesEmptyGraph) {
    {
        TGraph g = BuildGraph(NJson::TJsonValue());
        UNIT_ASSERT(g.Nodes.empty());
        UNIT_ASSERT(g.Links.empty());
    }
    {
        TGraph g = Build("{}");
        UNIT_ASSERT(g.Nodes.empty());
        UNIT_ASSERT(g.Links.empty());
    }
}

Y_UNIT_TEST(FixtureShape) {
    TGraph g = Build(PlanWithoutStats);
    UNIT_ASSERT_EQUAL(g.Nodes.size(), 6u);

    UNIT_ASSERT_EQUAL(NodeByName(g, "Read pq").Type,  ENodeType::Input);
    UNIT_ASSERT_EQUAL(NodeByName(g, "Source").Type,   ENodeType::Operation);
    UNIT_ASSERT_EQUAL(NodeByName(g, "Sink").Type,     ENodeType::Operation);
    UNIT_ASSERT_EQUAL(NodeByName(g, "Write pq").Type, ENodeType::Output);

    UNIT_ASSERT_EQUAL(g.Nodes[2].Name, "Stage");
    UNIT_ASSERT_EQUAL(g.Nodes[2].Type, ENodeType::Operation);
    UNIT_ASSERT_EQUAL(g.Nodes[3].Name, "Stage");
    UNIT_ASSERT_EQUAL(g.Nodes[3].Type, ENodeType::Operation);

    ui32 idReadPq         = NodeByName(g, "Read pq").Id;
    ui32 idSource         = NodeByName(g, "Source").Id;
    ui32 idStageAfterSource = g.Nodes[3].Id;
    ui32 idStageBeforeSink  = g.Nodes[2].Id;
    ui32 idSink           = NodeByName(g, "Sink").Id;
    ui32 idWritePq        = NodeByName(g, "Write pq").Id;

    UNIT_ASSERT_EQUAL(g.Links.size(), 5u);
    UNIT_ASSERT(HasLink(g, idReadPq, idSource));
    UNIT_ASSERT(HasLink(g, idSource, idStageAfterSource));
    UNIT_ASSERT(HasLink(g, idStageAfterSource, idStageBeforeSink));
    UNIT_ASSERT(HasLink(g, idStageBeforeSink, idSink));
    UNIT_ASSERT(HasLink(g, idSink, idWritePq));

    for (const auto& n : g.Nodes) {
        UNIT_ASSERT_UNEQUAL(n.Name, "HashShuffle");
    }
}

Y_UNIT_TEST(LevelsFollowDataFlow) {
    TGraph g = Build(PlanWithoutStats);
    UNIT_ASSERT_EQUAL(NodeByName(g, "Read pq").Level,  0u);
    UNIT_ASSERT_EQUAL(NodeByName(g, "Source").Level,   1u);
    UNIT_ASSERT_EQUAL(g.Nodes[3].Level,                2u); // Stage PlanNodeId 2
    UNIT_ASSERT_EQUAL(g.Nodes[2].Level,                3u); // Stage PlanNodeId 4
    UNIT_ASSERT_EQUAL(NodeByName(g, "Sink").Level,     4u);
    UNIT_ASSERT_EQUAL(NodeByName(g, "Write pq").Level, 5u);
}

Y_UNIT_TEST(NoStatsMeansPending) {
    TGraph g = Build(PlanWithoutStats);
    for (const auto& n : g.Nodes) {
        if (n.Type != ENodeType::Operation) {
            continue;
        }
        UNIT_ASSERT_EQUAL(n.State, ENodeState::Pending);
        UNIT_ASSERT_EQUAL(n.Tasks, 0u);
        UNIT_ASSERT_EQUAL(n.Stats.IngressRows, 0u);
        UNIT_ASSERT_EQUAL(n.Stats.OutputRows, 0u);
        UNIT_ASSERT_EQUAL(n.Stats.CpuTimeUs, 0u);
    }
}

Y_UNIT_TEST(StatsAreAggregatedPerStage) {
    TGraph g = Build(PlanWithStats);
    const TNode& sink = g.Nodes[0];
    UNIT_ASSERT_EQUAL(sink.State, ENodeState::Finished);
    UNIT_ASSERT_EQUAL(sink.Tasks, 1u);
    UNIT_ASSERT_EQUAL(sink.FinishedTasks, 1u);
    UNIT_ASSERT_EQUAL(sink.Stats.EgressRows, 100u);
    UNIT_ASSERT_EQUAL(sink.Stats.EgressBytes, 1000u);

    UNIT_ASSERT_EQUAL(g.Nodes[2].State, ENodeState::Pending);
    UNIT_ASSERT_EQUAL(g.Nodes[2].Tasks, 0u);

    const TNode& midStage = g.Nodes[3];
    UNIT_ASSERT_EQUAL(midStage.State, ENodeState::Running);
    UNIT_ASSERT_EQUAL(midStage.Tasks, 2u);
    UNIT_ASSERT_EQUAL(midStage.Stats.OutputRows, 100u);
    UNIT_ASSERT_EQUAL(midStage.Stats.CpuTimeUs, 4000u);

    const TNode& src = g.Nodes[4];
    UNIT_ASSERT_EQUAL(src.State, ENodeState::Running);
    UNIT_ASSERT_EQUAL(src.Stats.IngressRows, 100u);
    UNIT_ASSERT_EQUAL(src.Stats.IngressBytes, 1000u);
}

Y_UNIT_TEST(ConnectionFanIn) {
    TGraph g = Build(R"({
        "Plan": {
            "Node Type": "Query",
            "PlanNodeType": "Query",
            "Plans": [{
                "Node Type": "StageA",
                "Plans": [{
                    "Node Type": "UnionAll",
                    "PlanNodeType": "Connection",
                    "Plans": [
                        {"Node Type": "StageB"},
                        {"Node Type": "StageC"}
                    ]
                }]
            }]
        }
    })");
    UNIT_ASSERT_EQUAL(g.Nodes.size(), 3u);
    ui32 idA = NodeByName(g, "StageA").Id;
    ui32 idB = NodeByName(g, "StageB").Id;
    ui32 idC = NodeByName(g, "StageC").Id;
    UNIT_ASSERT(HasLink(g, idB, idA));
    UNIT_ASSERT(HasLink(g, idC, idA));
    UNIT_ASSERT_EQUAL(NodeByName(g, "StageB").Level, 0u);
    UNIT_ASSERT_EQUAL(NodeByName(g, "StageC").Level, 0u);
    UNIT_ASSERT_EQUAL(NodeByName(g, "StageA").Level, 1u);
}

Y_UNIT_TEST(ResultSetIsOutput) {
    TGraph g = Build(R"({
        "Plan": {
            "Node Type": "Query",
            "PlanNodeType": "Query",
            "Plans": [{
                "Node Type": "ResultSet",
                "PlanNodeType": "ResultSet",
                "Plans": [{"Node Type": "Stage"}]
            }]
        }
    })");
    const TNode& rs = NodeByName(g, "ResultSet");
    UNIT_ASSERT_EQUAL(rs.Type, ENodeType::Output);
    UNIT_ASSERT(HasLink(g, NodeByName(g, "Stage").Id, rs.Id));
}

Y_UNIT_TEST(MixedSourceSinkStage) {
    TGraph g = Build(R"({
        "Plan": {
            "Node Type": "Query",
            "PlanNodeType": "Query",
            "Plans": [{
                "Node Type": "Transform",
                "Operators": [
                    {"Name": "ReadSrc", "SourceType": "kafka"},
                    {"Name": "WriteDst", "SinkType": "pq"}
                ]
            }]
        }
    })");
    UNIT_ASSERT_EQUAL(g.Nodes.size(), 3u);
    const TNode& op = NodeByName(g, "Transform");
    const TNode& in = NodeByName(g, "ReadSrc");
    const TNode& out = NodeByName(g, "WriteDst");
    UNIT_ASSERT_EQUAL(in.Type,  ENodeType::Input);
    UNIT_ASSERT_EQUAL(op.Type,  ENodeType::Operation);
    UNIT_ASSERT_EQUAL(out.Type, ENodeType::Output);
    UNIT_ASSERT(HasLink(g, in.Id, op.Id));
    UNIT_ASSERT(HasLink(g, op.Id, out.Id));
}

Y_UNIT_TEST(SvgOfEmptyGraphIsValid) {
    TString svg = ToSvg(TGraph{});
    UNIT_ASSERT(svg.StartsWith("<svg"));
    UNIT_ASSERT(svg.EndsWith("</svg>"));
    UNIT_ASSERT(!svg.Contains("<circle"));
}

Y_UNIT_TEST(SvgHasOneShapePerNode) {
    TString svg = ToSvg(Build(PlanWithStats));
    auto countOf = [&](const char* sub) {
        int n = 0;
        size_t len = strlen(sub);
        for (size_t p = 0; (p = svg.find(sub, p)) != TString::npos; p += len) {
            ++n;
        }
        return n;
    };
    UNIT_ASSERT_EQUAL(countOf("<rect x="), 2);
    UNIT_ASSERT_GE(countOf("<text"), 6);
    UNIT_ASSERT_EQUAL(countOf("<line x1="), 5);
}

Y_UNIT_TEST(SvgShowsTasksAndState) {
    {
        TString svg = ToSvg(Build(PlanWithStats));
        UNIT_ASSERT(svg.Contains(">2<"));
        UNIT_ASSERT(svg.Contains("&#x2713;"));
    }
    {
        TString svg = ToSvg(Build(PlanWithoutStats));
        UNIT_ASSERT(!svg.Contains("&#x2713;"));
    }
}

Y_UNIT_TEST(SvgEscapesNames) {
    TGraph g = Build(R"({
        "Plan": {
            "Node Type": "Query",
            "PlanNodeType": "Query",
            "Plans": [{"Node Type": "A<B&C"}]
        }
    })");
    TString svg = ToSvg(g);
    UNIT_ASSERT(svg.Contains("A&lt;B&amp;C"));
    UNIT_ASSERT(!svg.Contains("A<B"));
}

Y_UNIT_TEST(SvgLevelsGoLeftToRight) {
    TString svg = ToSvg(Build(PlanWithoutStats));
    auto centerX = [&](const char* label) {
        TString needle = TString(">") + label + "<";
        size_t pos = svg.find(needle);
        UNIT_ASSERT_C(pos != TString::npos, TString("label not in SVG: ") + label);
        size_t xAt = svg.rfind("x=\"", pos);
        UNIT_ASSERT_C(xAt != TString::npos, "x attr not found before label");
        xAt += 3;
        int v = 0;
        while (xAt < svg.size() && svg[xAt] != '"') {
            v = v * 10 + (svg[xAt] - '0');
            ++xAt;
        }
        return v;
    };
    UNIT_ASSERT_LT(centerX("Read pq"), centerX("Write pq"));
}

} // Y_UNIT_TEST_SUITE(ComputationGraph)
