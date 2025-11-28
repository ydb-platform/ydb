#include <benchmark/benchmark.h>

#include <library/cpp/yson/node/node.h>

using NYT::TNode;
using TMapType = TNode::TMapType;

static const TNode Empty = TNode();

static const TNode EmptyWithAttributes = []{
    TNode node = Empty;
    node.Attributes()("attr1", "something")("attr2", 10);
    return node;
}();

static const TNode Filled = TNode()("key1", "value")("key2", 10)("key3", TNode());

static const TNode FilledWithAttributes = []{
    TNode node = Filled;
    node.Attributes()("attr1", "something")("attr2", 10);
    return node;
}();

static const int SomeInt = 10;
static const TStringBuf SomeString = "something";
static const TMapType SomeHashmap = Filled.AsMap();

static constexpr size_t kIter = 1000;

template <typename T, bool UseTNode>
static void BM_InitiliazeManyTimes(benchmark::State& state, const std::tuple<const TNode&, T>& input) {
    const auto& initial = std::get<0>(input);
    auto value = std::get<1>(input);
    for (auto _ : state) {
        TVector<TNode> nodes(kIter, initial);
        for (size_t i = 0; i < kIter; ++i) {
            if constexpr (UseTNode) {
                nodes[i] = TNode(value);
            } else {
                nodes[i] = value;
            }
        }
        benchmark::DoNotOptimize(nodes);
    }
    state.SetItemsProcessed(state.iterations() * kIter);
}

template <typename T, bool UseTNode>
static void BM_AddManyTimes(benchmark::State& state, const T& value) {
    for (auto _ : state) {
        TNode node = TNode::CreateList();
        // node.AsList().reserve(kIter);
        for (size_t i = 0; i < kIter; ++i) {
            if constexpr (UseTNode) {
                node.Add(TNode(value));
            } else {
                node.Add(value);
            }
        }
        benchmark::DoNotOptimize(node);
    }
    state.SetItemsProcessed(state.iterations() * kIter);
}

template <typename T, bool UseTNode>
static void BM_CallManyTimes(benchmark::State& state, const T& value) {
    for (auto _ : state) {
        TNode node = TNode::CreateMap();
        node.AsMap().reserve(kIter);
        for (size_t i = 0; i < kIter; ++i) {
            if constexpr (UseTNode) {
                node(ToString(i), TNode(value));
            } else {
                node(ToString(i), value);
            }
        }
        benchmark::DoNotOptimize(node);
    }
    state.SetItemsProcessed(state.iterations() * kIter);
}

BENCHMARK_TEMPLATE2_CAPTURE(BM_InitiliazeManyTimes, int, true, initialize_Empty_TNode_int, std::make_tuple(Empty, SomeInt));
BENCHMARK_TEMPLATE2_CAPTURE(BM_InitiliazeManyTimes, TStringBuf, true, initialize_Empty_TNode_TStringBuf, std::make_tuple(Empty, SomeString));
BENCHMARK_TEMPLATE2_CAPTURE(BM_InitiliazeManyTimes, TMapType, true, initialize_Empty_TNode_Map, std::make_tuple(Empty, SomeHashmap));

BENCHMARK_TEMPLATE2_CAPTURE(BM_InitiliazeManyTimes, int, false, initialize_Empty_int, std::make_tuple(Empty, SomeInt));
BENCHMARK_TEMPLATE2_CAPTURE(BM_InitiliazeManyTimes, TStringBuf, false, initialize_Empty_TStringBuf, std::make_tuple(Empty, SomeString));
BENCHMARK_TEMPLATE2_CAPTURE(BM_InitiliazeManyTimes, TMapType, false, initialize_Empty_Map, std::make_tuple(Empty, SomeHashmap));

BENCHMARK_TEMPLATE2_CAPTURE(BM_InitiliazeManyTimes, int, true, initialize_EmptyWithAttributes_TNode_int, std::make_tuple(EmptyWithAttributes, SomeInt));
BENCHMARK_TEMPLATE2_CAPTURE(BM_InitiliazeManyTimes, TStringBuf, true, initialize_EmptyWithAttributes_TNode_TStringBuf, std::make_tuple(EmptyWithAttributes, SomeString));
BENCHMARK_TEMPLATE2_CAPTURE(BM_InitiliazeManyTimes, TMapType, true, initialize_EmptyWithAttributes_TNode_Map, std::make_tuple(EmptyWithAttributes, SomeHashmap));

BENCHMARK_TEMPLATE2_CAPTURE(BM_InitiliazeManyTimes, int, false, initialize_EmptyWithAttributes_int, std::make_tuple(EmptyWithAttributes, SomeInt));
BENCHMARK_TEMPLATE2_CAPTURE(BM_InitiliazeManyTimes, TStringBuf, false, initialize_EmptyWithAttributes_TStringBuf, std::make_tuple(EmptyWithAttributes, SomeString));
BENCHMARK_TEMPLATE2_CAPTURE(BM_InitiliazeManyTimes, TMapType, false, initialize_EmptyWithAttributes_Map, std::make_tuple(EmptyWithAttributes, SomeHashmap));

BENCHMARK_TEMPLATE2_CAPTURE(BM_InitiliazeManyTimes, int, true, initialize_Filled_TNode_int, std::make_tuple(Filled, SomeInt));
BENCHMARK_TEMPLATE2_CAPTURE(BM_InitiliazeManyTimes, TStringBuf, true, initialize_Filled_TNode_TStringBuf, std::make_tuple(Filled, SomeString));
BENCHMARK_TEMPLATE2_CAPTURE(BM_InitiliazeManyTimes, TMapType, true, initialize_Filled_TNode_Map, std::make_tuple(Filled, SomeHashmap));

BENCHMARK_TEMPLATE2_CAPTURE(BM_InitiliazeManyTimes, int, false, initialize_Filled_int, std::make_tuple(Filled, SomeInt));
BENCHMARK_TEMPLATE2_CAPTURE(BM_InitiliazeManyTimes, TStringBuf, false, initialize_Filled_TStringBuf, std::make_tuple(Filled, SomeString));
BENCHMARK_TEMPLATE2_CAPTURE(BM_InitiliazeManyTimes, TMapType, false, initialize_Filled_Map, std::make_tuple(Filled, SomeHashmap));

BENCHMARK_TEMPLATE2_CAPTURE(BM_InitiliazeManyTimes, int, true, initialize_FilledWithAttributes_TNode_int, std::make_tuple(FilledWithAttributes, SomeInt));
BENCHMARK_TEMPLATE2_CAPTURE(BM_InitiliazeManyTimes, TStringBuf, true, initialize_FilledWithAttributes_TNode_TStringBuf, std::make_tuple(FilledWithAttributes, SomeString));
BENCHMARK_TEMPLATE2_CAPTURE(BM_InitiliazeManyTimes, TMapType, true, initialize_FilledWithAttributes_TNode_Map, std::make_tuple(FilledWithAttributes, SomeHashmap));

BENCHMARK_TEMPLATE2_CAPTURE(BM_InitiliazeManyTimes, int, false, initialize_FilledWithAttributes_int, std::make_tuple(FilledWithAttributes, SomeInt));
BENCHMARK_TEMPLATE2_CAPTURE(BM_InitiliazeManyTimes, TStringBuf, false, initialize_FilledWithAttributes_TStringBuf, std::make_tuple(FilledWithAttributes, SomeString));
BENCHMARK_TEMPLATE2_CAPTURE(BM_InitiliazeManyTimes, TMapType, false, initialize_FilledWithAttributes_Map, std::make_tuple(FilledWithAttributes, SomeHashmap));

BENCHMARK_TEMPLATE2_CAPTURE(BM_AddManyTimes, int, true, add_TNode_int, SomeInt);
BENCHMARK_TEMPLATE2_CAPTURE(BM_AddManyTimes, TStringBuf, true, add_TNode_TStringBuf, SomeString);
BENCHMARK_TEMPLATE2_CAPTURE(BM_AddManyTimes, TMapType, true, add_TNode_Map, SomeHashmap);

BENCHMARK_TEMPLATE2_CAPTURE(BM_AddManyTimes, int, false, add_int, SomeInt);
BENCHMARK_TEMPLATE2_CAPTURE(BM_AddManyTimes, TStringBuf, false, add_TStringBuf, SomeString);
BENCHMARK_TEMPLATE2_CAPTURE(BM_AddManyTimes, TMapType, false, add_Map, SomeHashmap);

BENCHMARK_TEMPLATE2_CAPTURE(BM_CallManyTimes, int, true, call_TNode_int, SomeInt);
BENCHMARK_TEMPLATE2_CAPTURE(BM_CallManyTimes, TStringBuf, true, call_TNode_TStringBuf, SomeString);
BENCHMARK_TEMPLATE2_CAPTURE(BM_CallManyTimes, TMapType, true, call_TNode_Map, SomeHashmap);

BENCHMARK_TEMPLATE2_CAPTURE(BM_CallManyTimes, int, false, call_int, SomeInt);
BENCHMARK_TEMPLATE2_CAPTURE(BM_CallManyTimes, TStringBuf, false, call_TStringBuf, SomeString);
BENCHMARK_TEMPLATE2_CAPTURE(BM_CallManyTimes, TMapType, false, call_Map, SomeHashmap);
