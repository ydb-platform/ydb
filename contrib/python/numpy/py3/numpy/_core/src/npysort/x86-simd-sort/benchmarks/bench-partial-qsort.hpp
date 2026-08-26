template <typename T, class... Args>
static void simdpartialsort(benchmark::State &state, Args &&...args)
{
    // Perform setup here
    auto args_tuple = std::make_tuple(std::move(args)...);
    int64_t ARRSIZE = std::get<0>(args_tuple);
    int64_t k = std::get<1>(args_tuple);
    std::vector<T> arr;
    std::vector<T> arr_bkp;

    /* Initialize elements */
    arr = get_uniform_rand_array<T>(ARRSIZE);
    arr_bkp = arr;

    /* call simdpartialsort */
    for (auto _ : state) {
        x86simdsort::partial_qsort<T>(arr.data(), k, ARRSIZE);

        state.PauseTiming();
        arr = arr_bkp;
        state.ResumeTiming();
    }
}

template <typename T, class... Args>
static void scalarpartialsort(benchmark::State &state, Args &&...args)
{
    // Perform setup here
    auto args_tuple = std::make_tuple(std::move(args)...);
    int64_t ARRSIZE = std::get<0>(args_tuple);
    int64_t k = std::get<1>(args_tuple);
    std::vector<T> arr;
    std::vector<T> arr_bkp;

    /* Initialize elements */
    arr = get_uniform_rand_array<T>(ARRSIZE);
    arr_bkp = arr;

    /* call std::partial_sort */
    for (auto _ : state) {
        std::partial_sort(arr.begin(), arr.begin() + k, arr.end());

        state.PauseTiming();
        arr = arr_bkp;
        state.ResumeTiming();
    }
}

#define BENCH_BOTH_PARTIAL(type) \
    BENCH_PARTIAL(simdpartialsort, type) \
    BENCH_PARTIAL(scalarpartialsort, type)

BENCH_BOTH_PARTIAL(uint64_t)
BENCH_BOTH_PARTIAL(int64_t)
BENCH_BOTH_PARTIAL(uint32_t)
BENCH_BOTH_PARTIAL(int32_t)
BENCH_BOTH_PARTIAL(uint16_t)
BENCH_BOTH_PARTIAL(int16_t)
BENCH_BOTH_PARTIAL(float)
BENCH_BOTH_PARTIAL(double)
#ifdef __FLT16_MAX__
BENCH_BOTH_PARTIAL(_Float16)
#endif
