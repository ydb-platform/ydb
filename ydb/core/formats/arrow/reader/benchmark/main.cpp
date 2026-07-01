
// #include <contrib/restricted/google/benchmark/include/benchmark/benchmark.h>
#include <library/cpp/testing/gbenchmark/benchmark.h>

#include "merges.h"
#include "remapper_bench.h"


void RunMerge(const TFixture& f, benchmark::State& state) {
    for (auto _ : state) {
        auto result = MergeOnce(f);
        benchmark::DoNotOptimize(result.get());
    }
}

// Альтернативный алгоритм: concat всех источников + arrow::compute::SortIndices + Take.
// Не делает дедупликацию по версии — голый sort+gather для сравнения стоимости сортировки.
void RunSortIndicesMerge(const TFixture20& f, benchmark::State& state) {

    for (auto _ : state) {
        auto result = MergeOnceArrow20(f);
        benchmark::DoNotOptimize(result.get());
    }
}

void RunHashFirstMerge(const TFixture20& f, benchmark::State& state) {
    for (auto _ : state) {
        auto result = HashFirstMergeOnce(f);
        benchmark::DoNotOptimize(result.get());
    }
}

void HashFirst_2src_10k(benchmark::State& state)  { RunHashFirstMerge(Get20_2src_10k(),  state); }
void HashFirst_5src_10k(benchmark::State& state)  { RunHashFirstMerge(Get20_5src_10k(),  state); }
void HashFirst_10src_10k(benchmark::State& state) { RunHashFirstMerge(Get20_10src_10k(), state); }
void HashFirst_20src_10k(benchmark::State& state) { RunHashFirstMerge(Get20_20src_10k(), state); }

void Merge_2src_10k(benchmark::State& state)  { RunMerge(Get_2src_10k(),  state); }
void Merge_5src_10k(benchmark::State& state)  { RunMerge(Get_5src_10k(),  state); }
void Merge_10src_10k(benchmark::State& state) { RunMerge(Get_10src_10k(), state); }
void Merge_20src_10k(benchmark::State& state) { RunMerge(Get_20src_10k(), state); }

void SortIndices_2src_10k(benchmark::State& state)  { RunSortIndicesMerge(Get20_2src_10k(),  state); }
void SortIndices_5src_10k(benchmark::State& state)  { RunSortIndicesMerge(Get20_5src_10k(),  state); }
void SortIndices_10src_10k(benchmark::State& state) { RunSortIndicesMerge(Get20_10src_10k(), state); }
void SortIndices_20src_10k(benchmark::State& state) { RunSortIndicesMerge(Get20_20src_10k(), state); }

// ---- production remapper: TSortIndicesMerger::BuildRemapper vs TMergePartialStream::DrainAllParts ----
// Оба строят remapper-батчи (indexFields) из одних и тех же TGeneralContainer-источников.

void RunBuildRemapperBench(const NRemapperBench::TRemapperFixture& f, benchmark::State& state) {
    for (auto _ : state) {
        auto result = NRemapperBench::RunBuildRemapper(f);
        benchmark::DoNotOptimize(result);
    }
}

void RunDrainAllPartsBench(const NRemapperBench::TRemapperFixture& f, benchmark::State& state) {
    for (auto _ : state) {
        auto result = NRemapperBench::RunDrainAllParts(f);
        benchmark::DoNotOptimize(result);
    }
}

void BuildRemapper_2src_10k(benchmark::State& state)  { RunBuildRemapperBench(NRemapperBench::GetRemap_2src(),  state); }
void BuildRemapper_5src_10k(benchmark::State& state)  { RunBuildRemapperBench(NRemapperBench::GetRemap_5src(),  state); }
void BuildRemapper_10src_10k(benchmark::State& state) { RunBuildRemapperBench(NRemapperBench::GetRemap_10src(), state); }
void BuildRemapper_20src_10k(benchmark::State& state) { RunBuildRemapperBench(NRemapperBench::GetRemap_20src(), state); }

void DrainAllParts_2src_10k(benchmark::State& state)  { RunDrainAllPartsBench(NRemapperBench::GetRemap_2src(),  state); }
void DrainAllParts_5src_10k(benchmark::State& state)  { RunDrainAllPartsBench(NRemapperBench::GetRemap_5src(),  state); }
void DrainAllParts_10src_10k(benchmark::State& state) { RunDrainAllPartsBench(NRemapperBench::GetRemap_10src(), state); }
void DrainAllParts_20src_10k(benchmark::State& state) { RunDrainAllPartsBench(NRemapperBench::GetRemap_20src(), state); }

// ---- arrow_next 20: sort + Grouper + hash_first (с дедупликацией по max(ver)) ----

BENCHMARK(HashFirst_2src_10k);
BENCHMARK(HashFirst_5src_10k);
BENCHMARK(HashFirst_10src_10k);
BENCHMARK(HashFirst_20src_10k);

// ---- TMergePartialStream ----
BENCHMARK(Merge_2src_10k);
BENCHMARK(Merge_5src_10k);
BENCHMARK(Merge_10src_10k);
BENCHMARK(Merge_20src_10k);

// ---- arrow::compute::SortIndices + Take ----
BENCHMARK(SortIndices_2src_10k);
BENCHMARK(SortIndices_5src_10k);
BENCHMARK(SortIndices_10src_10k);
BENCHMARK(SortIndices_20src_10k);

// ---- TSortIndicesMerger::BuildRemapper (production) ----
BENCHMARK(BuildRemapper_2src_10k);
BENCHMARK(BuildRemapper_5src_10k);
BENCHMARK(BuildRemapper_10src_10k);
BENCHMARK(BuildRemapper_20src_10k);

// ---- TMergePartialStream::DrainAllParts (production) ----
BENCHMARK(DrainAllParts_2src_10k);
BENCHMARK(DrainAllParts_5src_10k);
BENCHMARK(DrainAllParts_10src_10k);
BENCHMARK(DrainAllParts_20src_10k);
