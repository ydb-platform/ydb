#include "bench.h"
#define VQSORT_ONLY_STATIC 1
#include "hwy/contrib/sort/vqsort-inl.h"

template <typename T, class... Args>
static void vqsort(benchmark::State &state, Args &&...args)
{
    // Get args
    auto args_tuple = std::make_tuple(std::move(args)...);
    size_t arrsize = std::get<0>(args_tuple);
    std::string arrtype = std::get<1>(args_tuple);
    // set up array
    std::vector<T> arr = get_array<T>(arrtype, arrsize);
    std::vector<T> arr_bkp = arr;
    // benchmark
    for (auto _ : state) {
        hwy::HWY_NAMESPACE::VQSortStatic(
                arr.data(), arrsize, hwy::SortAscending());
        state.PauseTiming();
        arr = arr_bkp;
        state.ResumeTiming();
    }
}

BENCH_SORT(vqsort, uint64_t)
BENCH_SORT(vqsort, int64_t)
BENCH_SORT(vqsort, uint32_t)
BENCH_SORT(vqsort, int32_t)
BENCH_SORT(vqsort, float)
BENCH_SORT(vqsort, double)
