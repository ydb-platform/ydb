#include "benchmark/benchmark.h"
#include <benchmark/benchmark.h>

#include <library/cpp/yt/cpu_clock/clock.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

void BenchmarkCpuTime(benchmark::State& state)
{
    for (auto _ : state) {
        benchmark::DoNotOptimize(GetCpuInstant());
    }
}

BENCHMARK(BenchmarkCpuTime);

void BenchmarkInstantNow(benchmark::State& state)
{
    for (auto _ : state) {
        benchmark::DoNotOptimize(TInstant::Now());
    }
}

BENCHMARK(BenchmarkInstantNow);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
