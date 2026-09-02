template <typename T, class... Args>
static void scalarsort(benchmark::State &state, Args &&...args)
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
        std::sort(arr.begin(), arr.end());
        state.PauseTiming();
        arr = arr_bkp;
        state.ResumeTiming();
    }
}

template <typename T, class... Args>
static void simdsort(benchmark::State &state, Args &&...args)
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
        x86simdsort::qsort(arr.data(), arrsize);
        state.PauseTiming();
        arr = arr_bkp;
        state.ResumeTiming();
    }
}

template <typename T, class... Args>
static void scalar_revsort(benchmark::State &state, Args &&...args)
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
        std::sort(arr.rbegin(), arr.rend());
        state.PauseTiming();
        arr = arr_bkp;
        state.ResumeTiming();
    }
}

template <typename T, class... Args>
static void simd_revsort(benchmark::State &state, Args &&...args)
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
        x86simdsort::qsort(arr.data(), arrsize, false, true);
        state.PauseTiming();
        arr = arr_bkp;
        state.ResumeTiming();
    }
}

#define BENCH_BOTH_QSORT(type) \
    BENCH_SORT(simdsort, type) \
    BENCH_SORT(scalarsort, type) \
    BENCH_SORT(simd_revsort, type) \
    BENCH_SORT(scalar_revsort, type)

BENCH_BOTH_QSORT(uint64_t)
BENCH_BOTH_QSORT(int64_t)
BENCH_BOTH_QSORT(uint32_t)
BENCH_BOTH_QSORT(int32_t)
BENCH_BOTH_QSORT(uint16_t)
BENCH_BOTH_QSORT(int16_t)
BENCH_BOTH_QSORT(float)
BENCH_BOTH_QSORT(double)
#ifdef __FLT16_MAX__
BENCH_BOTH_QSORT(_Float16)
#endif
