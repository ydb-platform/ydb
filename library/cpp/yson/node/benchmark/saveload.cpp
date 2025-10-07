#include <benchmark/benchmark.h>

#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/resource/resource.h>

using namespace NYT;

namespace {

static NYT::TNode GenerateList(size_t size)
{
    NYT::TNode result = NYT::TNode::CreateList();

    for (size_t i = 0; i < size; ++i) {
        result.AsList().emplace_back(NYT::TNode("val"));
    }

    return result;
}

static TString GetComplexBinaryYson() {
    return NResource::Find("complex.yson");
}

} // namespace

static void BM_SaveLoadGreedy(benchmark::State& state, size_t size)
{
    auto list = GenerateList(size);

    TString bytes;
    TStringOutput outputStream{bytes};
    NodeToYsonStream(list, &outputStream, ::NYson::EYsonFormat::Binary);

    for (const auto& _ : state) {
        TStringInput inputStream{bytes};
        NodeFromYsonStream(&inputStream);
    }
}

static void BM_SaveLoadNonGreedy(benchmark::State& state, size_t size)
{
    auto list = GenerateList(size);

    TString bytes;
    TStringOutput outputStream{bytes};
    NodeToYsonStream(list, &outputStream, ::NYson::EYsonFormat::Binary);

    for (const auto& _ : state) {
        TStringInput inputStream{bytes};
        NodeFromYsonStreamNonGreedy(&inputStream);
    }
}

static void BM_LoadComplexBinaryYson(benchmark::State& state)
{
    auto serialized = GetComplexBinaryYson();

    for (const auto& _ : state) {
        auto node = NodeFromYsonString(serialized, NYT::NYson::EYsonType::Node);
        benchmark::DoNotOptimize(node);
    }
}

BENCHMARK_CAPTURE(BM_SaveLoadGreedy, greedy_10, 10ul);
BENCHMARK_CAPTURE(BM_SaveLoadNonGreedy, non_greedy_10, 10ul);
BENCHMARK_CAPTURE(BM_SaveLoadGreedy, greedy_100, 100ul);
BENCHMARK_CAPTURE(BM_SaveLoadNonGreedy, non_greedy_100, 100ul);
BENCHMARK_CAPTURE(BM_SaveLoadGreedy, greedy_1000, 1000ul);
BENCHMARK_CAPTURE(BM_SaveLoadNonGreedy, non_greedy_1000, 1000ul);
BENCHMARK_CAPTURE(BM_SaveLoadGreedy, greedy_10000, 10000ul);
BENCHMARK_CAPTURE(BM_SaveLoadNonGreedy, non_greedy_10000, 10000ul);

BENCHMARK(BM_LoadComplexBinaryYson);
