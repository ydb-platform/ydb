#include <yql/essentials/minikql/dom/yson.h>
#include <yql/essentials/minikql/dom/test_support/fixtures.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_value_builder.h>

#include <benchmark/benchmark.h>
#include <util/system/yassert.h>

namespace {

void YsonValidate(benchmark::State& state) {
    const auto document = NYql::NDom::NTest::GetYsonDocument();
    for (auto _ : state) {
        auto valid = NYql::NDom::IsValidYson(document);
        benchmark::DoNotOptimize(valid);
        Y_ENSURE(valid, "Yson validation failed");
    }
}
BENCHMARK(YsonValidate);

void YsonParse(benchmark::State& state) {
    NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    NKikimr::NMiniKQL::TMemoryUsageInfo memInfo("Memory");
    NKikimr::NMiniKQL::THolderFactory holderFactory(alloc.Ref(), memInfo, /*functionRegistry=*/nullptr);
    NKikimr::NMiniKQL::TDefaultValueBuilder builder(holderFactory);
    const auto document = NYql::NDom::NTest::GetYsonDocument();
    for (auto _ : state) {
        auto value = NYql::NDom::TryParseYsonDom(document, &builder);
        benchmark::DoNotOptimize(value);
        Y_ENSURE(value, "Yson operation produced an empty value");
    }
}
BENCHMARK(YsonParse);

void YsonSerialize(benchmark::State& state) {
    NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    NKikimr::NMiniKQL::TMemoryUsageInfo memInfo("Memory");
    NKikimr::NMiniKQL::THolderFactory holderFactory(alloc.Ref(), memInfo, /*functionRegistry=*/nullptr);
    NKikimr::NMiniKQL::TDefaultValueBuilder builder(holderFactory);
    const auto dom = NYql::NDom::TryParseYsonDom(NYql::NDom::NTest::GetYsonDocument(), &builder);
    Y_ENSURE(dom, "Failed to parse Yson document for serialization");
    for (auto _ : state) {
        auto value = builder.NewString(NYql::NDom::SerializeYsonDomToBinary(dom));
        benchmark::DoNotOptimize(value);
        Y_ENSURE(value, "Yson operation produced an empty value");
    }
}
BENCHMARK(YsonSerialize);

void YsonSerializeText(benchmark::State& state) {
    NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    NKikimr::NMiniKQL::TMemoryUsageInfo memInfo("Memory");
    NKikimr::NMiniKQL::THolderFactory holderFactory(alloc.Ref(), memInfo, /*functionRegistry=*/nullptr);
    NKikimr::NMiniKQL::TDefaultValueBuilder builder(holderFactory);
    const auto dom = NYql::NDom::TryParseYsonDom(NYql::NDom::NTest::GetYsonDocument(), &builder);
    Y_ENSURE(dom, "Failed to parse Yson document for serialization");
    for (auto _ : state) {
        auto value = builder.NewString(NYql::NDom::SerializeYsonDomToText(dom));
        benchmark::DoNotOptimize(value);
        Y_ENSURE(value, "Yson operation produced an empty value");
    }
}
BENCHMARK(YsonSerializeText);

void YsonSerializePrettyText(benchmark::State& state) {
    NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    NKikimr::NMiniKQL::TMemoryUsageInfo memInfo("Memory");
    NKikimr::NMiniKQL::THolderFactory holderFactory(alloc.Ref(), memInfo, /*functionRegistry=*/nullptr);
    NKikimr::NMiniKQL::TDefaultValueBuilder builder(holderFactory);
    const auto dom = NYql::NDom::TryParseYsonDom(NYql::NDom::NTest::GetYsonDocument(), &builder);
    Y_ENSURE(dom, "Failed to parse Yson document for serialization");
    for (auto _ : state) {
        auto value = builder.NewString(NYql::NDom::SerializeYsonDomToPrettyText(dom));
        benchmark::DoNotOptimize(value);
        Y_ENSURE(value, "Yson operation produced an empty value");
    }
}
BENCHMARK(YsonSerializePrettyText);

} // namespace
