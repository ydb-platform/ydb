#include <ydb/core/kqp/opt/rbo/kqp_stage_graph.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpStageGraph) {
    Y_UNIT_TEST(ConnectivityAndDirectedCycles) {
        TStageGraph graph;
        UNIT_ASSERT(!graph.IsConnected());
        UNIT_ASSERT(!graph.ContainsCycles());
        UNIT_ASSERT(graph.TopologicalOrder().empty());

        const auto& a = graph.AddStage();
        UNIT_ASSERT(graph.IsConnected());
        const auto& b = graph.AddStage();
        UNIT_ASSERT(!graph.IsConnected());
        graph.Connect(a, b, MakeIntrusive<TMapConnection>(graph.AllocateOutput(a)));
        graph.Connect(a, b, MakeIntrusive<TMapConnection>(graph.AllocateOutput(a)));
        UNIT_ASSERT(graph.IsConnected());
        UNIT_ASSERT(!graph.ContainsCycles());
        UNIT_ASSERT_VALUES_EQUAL(graph.TopologicalOrder().size(), 2);
        UNIT_ASSERT(!graph.FindUniqueEdge(a, b));

        // Deliberately violate the DAG contract to exercise the debug diagnostic.
        graph.Connect(b, a, MakeIntrusive<TMapConnection>(graph.AllocateOutput(b)));
        UNIT_ASSERT(graph.ContainsCycles());
    }

    Y_UNIT_TEST(CycleInDisconnectedComponentAndSelfLoop) {
        TStageGraph graph;
        graph.AddStage();
        const auto& cycle = graph.AddStage();
        graph.Connect(cycle, cycle, MakeIntrusive<TMapConnection>(graph.AllocateOutput(cycle)));
        UNIT_ASSERT(!graph.IsConnected());
        UNIT_ASSERT(graph.ContainsCycles());
    }

    Y_UNIT_TEST(DiamondAndInterleavedInputsPreserveOrder) {
        TStageGraph graph;
        const auto& result = graph.AddStage();
        const auto& left = graph.AddStage();
        const auto& right = graph.AddStage();
        const auto& source = graph.AddSourceStage(NYql::EStorageType::ColumnStorage);
        const auto& first = graph.Connect(left, result, MakeIntrusive<TMapConnection>(graph.AllocateOutput(left)));
        const auto& second = graph.Connect(right, result, MakeIntrusive<TBroadcastConnection>(graph.AllocateOutput(right)));
        const auto& third = graph.Connect(left, result, MakeIntrusive<TUnionAllConnection>(graph.AllocateOutput(left)));
        graph.Connect(source, left, MakeIntrusive<TMapConnection>(graph.AllocateOutput(source)));
        graph.Connect(source, right, MakeIntrusive<TMapConnection>(graph.AllocateOutput(source)));
        UNIT_ASSERT(graph.IsConnected());
        UNIT_ASSERT(!graph.ContainsCycles());
        const auto order = graph.TopologicalOrder();
        UNIT_ASSERT(order.front() == &source);
        UNIT_ASSERT(order.back() == &result);
        UNIT_ASSERT(order == graph.TopologicalOrder());
        UNIT_ASSERT(graph.Stages().front() == &result);
        const auto inputs = graph.IncomingEdges(result);
        UNIT_ASSERT_VALUES_EQUAL(inputs.size(), 3);
        UNIT_ASSERT(inputs[0] == &first);
        UNIT_ASSERT(inputs[1] == &second);
        UNIT_ASSERT(inputs[2] == &third);
        UNIT_ASSERT(graph.FindUniqueEdge(right, result) == &second);
    }

    Y_UNIT_TEST(ReplacementPreservesOutputAndOwnsSettings) {
        TStageGraph graph;
        const auto& source = graph.AddStage();
        const auto& consumer = graph.AddStage();
        auto settings = MakeIntrusive<TShuffleConnection>(TOrderedIUs<>{}, 7);
        auto& edge = graph.Connect(source, consumer, settings);
        edge.SetShuffleHashFunction(NYql::NDq::EHashShuffleFuncType::HashV2);
        UNIT_ASSERT(!settings->HashFuncType);
        UNIT_ASSERT(static_cast<const TShuffleConnection&>(edge.Connection()).HashFuncType ==
            NYql::NDq::EHashShuffleFuncType::HashV2);
        UNIT_ASSERT(&graph.GetEdge(edge.Id()) == &edge);
        UNIT_ASSERT_EXCEPTION(edge.SetStreamLookupInputType({}), yexception);
        UNIT_ASSERT_VALUES_EQUAL(graph.AllocateOutput(source), 8);
        UNIT_ASSERT_VALUES_EQUAL(graph.GetOutput(source, 7), 7);
        UNIT_ASSERT_EXCEPTION(edge.ReplaceConnection(MakeIntrusive<TMapConnection>(0)), yexception);
        const auto id = edge.Id();
        edge.ReplaceConnection(MakeIntrusive<TMergeConnection>(TSortIUs{}, 7));
        UNIT_ASSERT(edge.Id() == id);
        UNIT_ASSERT(IsConnection<TMergeConnection>(edge.Connection()));
        UNIT_ASSERT_VALUES_EQUAL(edge.OutputIndex(), 7);
        UNIT_ASSERT(&edge.Producer() == &source);
        UNIT_ASSERT(&edge.Consumer() == &consumer);
        UNIT_ASSERT(graph.IncomingEdges(consumer).front() == &edge);
        UNIT_ASSERT_EXCEPTION(edge.SetShuffleHashFunction(NYql::NDq::EHashShuffleFuncType::HashV2), yexception);
        UNIT_ASSERT_EXCEPTION(graph.GetOutput(source, 2), yexception);
    }

    Y_UNIT_TEST(ObjectsSurviveGrowthAndMovesAndRejectForeignGraph) {
        TStageGraph graph;
        const auto& source = graph.AddStage();
        const auto& consumer = graph.AddStage();
        const auto output = graph.AllocateOutput(source);
        auto& edge = graph.Connect(source, consumer, MakeIntrusive<TMapConnection>(output));
        for (ui32 i = 0; i < 100; ++i) {
            const auto& next = graph.AddStage();
            graph.Connect(consumer, next, MakeIntrusive<TMapConnection>(graph.AllocateOutput(consumer)));
        }
        TStageGraph moved(std::move(graph));
        UNIT_ASSERT(&moved.GetStage(source.Id()) == &source);
        UNIT_ASSERT(&moved.GetEdge(edge.Id()) == &edge);
        UNIT_ASSERT(moved.IsConnected());
        TStageGraph other;
        const auto& foreign = other.AddStage();
        const auto& foreignConsumer = other.AddStage();
        other.Connect(foreign, foreignConsumer, MakeIntrusive<TMapConnection>(other.AllocateOutput(foreign)));
        UNIT_ASSERT_EXCEPTION(moved.IncomingEdges(foreign), yexception);
        UNIT_ASSERT_EXCEPTION(moved.Connect(source, foreign, MakeIntrusive<TMapConnection>(output)), yexception);
    }

    Y_UNIT_TEST(QueryWideStageIds) {
        auto allocator = std::make_shared<TStageIdAllocator>();
        TStageGraph first;
        TStageGraph second;
        first.SetIdAllocator(allocator);
        second.SetIdAllocator(allocator);
        const auto& a = first.AddStage();
        const auto& b = second.AddStage();
        const auto& c = first.AddStage();
        UNIT_ASSERT(a.Id() != b.Id());
        UNIT_ASSERT(a.Id() != c.Id());
        UNIT_ASSERT(b.Id() != c.Id());
        UNIT_ASSERT_EXCEPTION(first.SetIdAllocator(allocator), yexception);
    }
}

} // namespace NKikimr::NKqp
