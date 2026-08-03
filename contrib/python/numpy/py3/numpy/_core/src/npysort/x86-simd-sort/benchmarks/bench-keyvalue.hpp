#include "x86simdsort-scalar.h"

template <typename T, class... Args>
static void scalarkvsort(benchmark::State &state, Args &&...args)
{
    // Get args
    auto args_tuple = std::make_tuple(std::move(args)...);
    size_t arrsize = std::get<0>(args_tuple);
    std::string arrtype = std::get<1>(args_tuple);
    // set up array
    std::vector<T> key = get_array<T>(arrtype, arrsize);
    std::vector<T> val = get_array<T>("random", arrsize);
    std::vector<T> key_bkp = key;
    // benchmark
    for (auto _ : state) {
        xss::scalar::keyvalue_qsort(
                key.data(), val.data(), arrsize, false, false);
        state.PauseTiming();
        key = key_bkp;
        state.ResumeTiming();
    }
}

template <typename T, class... Args>
static void simdkvsort(benchmark::State &state, Args &&...args)
{
    auto args_tuple = std::make_tuple(std::move(args)...);
    size_t arrsize = std::get<0>(args_tuple);
    std::string arrtype = std::get<1>(args_tuple);
    // set up array
    std::vector<T> key = get_array<T>(arrtype, arrsize);
    std::vector<T> val = get_array<T>("random", arrsize);
    std::vector<T> key_bkp = key;
    // benchmark
    for (auto _ : state) {
        x86simdsort::keyvalue_qsort(key.data(), val.data(), arrsize);
        state.PauseTiming();
        key = key_bkp;
        state.ResumeTiming();
    }
}

#define BENCH_BOTH_KVSORT(type) \
    BENCH_SORT(simdkvsort, type) \
    BENCH_SORT(scalarkvsort, type)

BENCH_BOTH_KVSORT(uint64_t)
BENCH_BOTH_KVSORT(int64_t)
BENCH_BOTH_KVSORT(double)
BENCH_BOTH_KVSORT(uint32_t)
BENCH_BOTH_KVSORT(int32_t)
BENCH_BOTH_KVSORT(float)
