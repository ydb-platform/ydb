#include <yql/essentials/minikql/runtime_settings/runtime_settings_hash.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings_serialization.h>

#include <benchmark/benchmark.h>

namespace {

void DeserializeRuntimeSettingsFromProtoEmpty(benchmark::State& state) {
    const NYql::NProto::TRuntimeSettings proto;

    for (auto _ : state) {
        auto settings = NYql::DeserializeRuntimeSettingsFromProto(proto);
        benchmark::DoNotOptimize(settings);
    }
}

void SerializeRuntimeSettingsToProtoEmpty(benchmark::State& state) {
    const NYql::TRuntimeSettings settings;

    for (auto _ : state) {
        auto proto = NYql::SerializeRuntimeSettingsToProto(settings);
        benchmark::DoNotOptimize(proto);
    }
}

void DefaultConstructRuntimeSettings(benchmark::State& state) {
    for ([[maybe_unused]] auto iteration : state) {
        NYql::TRuntimeSettings settings;
        benchmark::DoNotOptimize(settings);
    }
}

void StableHashRuntimeSettingsEmpty(benchmark::State& state) {
    const NYql::TRuntimeSettings settings;

    for ([[maybe_unused]] auto iteration : state) {
        auto hash = NYql::StableHashRuntimeSettings(settings);
        benchmark::DoNotOptimize(hash);
    }
}

BENCHMARK(DeserializeRuntimeSettingsFromProtoEmpty);
BENCHMARK(SerializeRuntimeSettingsToProtoEmpty);
BENCHMARK(DefaultConstructRuntimeSettings);
BENCHMARK(StableHashRuntimeSettingsEmpty);

} // namespace
