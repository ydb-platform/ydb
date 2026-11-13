#include <yql/essentials/minikql/dom/json.h>
#include <yql/essentials/minikql/dom/test_support/fixtures.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_value_builder.h>

#include <benchmark/benchmark.h>
#include <util/system/yassert.h>

namespace {

void JsonValidate(benchmark::State& state) {
    const auto document = NYql::NDom::NTest::GetJsonDocument();
    for (auto _ : state) {
        auto valid = NYql::NDom::IsValidJson(document);
        benchmark::DoNotOptimize(valid);
        Y_ENSURE(valid, "Json validation failed");
    }
}
BENCHMARK(JsonValidate);

void JsonParse(benchmark::State& state) {
    NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    NKikimr::NMiniKQL::TMemoryUsageInfo memInfo("Memory");
    NKikimr::NMiniKQL::THolderFactory holderFactory(alloc.Ref(), memInfo, /*functionRegistry=*/nullptr);
    NKikimr::NMiniKQL::TDefaultValueBuilder builder(holderFactory);
    const auto document = NYql::NDom::NTest::GetJsonDocument();
    for (auto _ : state) {
        auto value = NYql::NDom::TryParseJsonDom(document, &builder);
        benchmark::DoNotOptimize(value);
        Y_ENSURE(value, "Json operation produced an empty value");
    }
}
BENCHMARK(JsonParse);

void JsonSerialize(benchmark::State& state) {
    NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    NKikimr::NMiniKQL::TMemoryUsageInfo memInfo("Memory");
    NKikimr::NMiniKQL::THolderFactory holderFactory(alloc.Ref(), memInfo, /*functionRegistry=*/nullptr);
    NKikimr::NMiniKQL::TDefaultValueBuilder builder(holderFactory);
    const auto dom = NYql::NDom::TryParseJsonDom(NYql::NDom::NTest::GetJsonDocument(), &builder);
    Y_ENSURE(dom, "Failed to parse Json document for serialization");
    for (auto _ : state) {
        auto value = builder.NewString(NYql::NDom::SerializeJsonDom(dom));
        benchmark::DoNotOptimize(value);
        Y_ENSURE(value, "Json operation produced an empty value");
    }
}
BENCHMARK(JsonSerialize);

} // namespace
