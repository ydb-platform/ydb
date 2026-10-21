template <typename T, class... Args>
static void simdqselect(benchmark::State &state, Args &&...args)
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

    /* call avx512 quickselect */
    for (auto _ : state) {
        x86simdsort::qselect<T>(arr.data(), k, ARRSIZE);

        state.PauseTiming();
        arr = arr_bkp;
        state.ResumeTiming();
    }
}

template <typename T, class... Args>
static void scalarqselect(benchmark::State &state, Args &&...args)
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

    /* call std::nth_element */
    for (auto _ : state) {
        std::nth_element(arr.begin(), arr.begin() + k, arr.end());

        state.PauseTiming();
        arr = arr_bkp;
        state.ResumeTiming();
    }
}

#define BENCH_BOTH_QSELECT(type) \
    BENCH_PARTIAL(simdqselect, type) \
    BENCH_PARTIAL(scalarqselect, type)

BENCH_BOTH_QSELECT(uint64_t)
BENCH_BOTH_QSELECT(int64_t)
BENCH_BOTH_QSELECT(uint32_t)
BENCH_BOTH_QSELECT(int32_t)
BENCH_BOTH_QSELECT(uint16_t)
BENCH_BOTH_QSELECT(int16_t)
BENCH_BOTH_QSELECT(float)
BENCH_BOTH_QSELECT(double)
#ifdef __FLT16_MAX__
BENCH_BOTH_QSELECT(_Float16)
#endif
