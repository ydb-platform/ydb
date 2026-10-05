template <typename T>
std::vector<size_t> stdargsort(const std::vector<T> &array)
{
    std::vector<size_t> indices(array.size());
    std::iota(indices.begin(), indices.end(), 0);
    std::sort(indices.begin(),
              indices.end(),
              [&array](size_t left, size_t right) -> bool {
                  // sort indices according to corresponding array element
                  return array[left] < array[right];
              });

    return indices;
}

template <typename T, class... Args>
static void scalarargsort(benchmark::State &state, Args &&...args)
{
    // get args
    auto args_tuple = std::make_tuple(std::move(args)...);
    size_t arrsize = std::get<0>(args_tuple);
    std::string arrtype = std::get<1>(args_tuple);
    // set up array
    std::vector<T> arr = get_array<T>(arrtype, arrsize);
    std::vector<size_t> inx;
    // benchmark
    for (auto _ : state) {
        inx = stdargsort(arr);
    }
}

template <typename T, class... Args>
static void simdargsort(benchmark::State &state, Args &&...args)
{
    // get args
    auto args_tuple = std::make_tuple(std::move(args)...);
    size_t arrsize = std::get<0>(args_tuple);
    std::string arrtype = std::get<1>(args_tuple);
    // set up array
    std::vector<T> arr = get_array<T>(arrtype, arrsize);
    std::vector<size_t> inx;
    // benchmark
    for (auto _ : state) {
        inx = x86simdsort::argsort(arr.data(), arrsize);
    }
}

template <typename T, class... Args>
static void simd_revargsort(benchmark::State &state, Args &&...args)
{
    // get args
    auto args_tuple = std::make_tuple(std::move(args)...);
    size_t arrsize = std::get<0>(args_tuple);
    std::string arrtype = std::get<1>(args_tuple);
    // set up array
    std::vector<T> arr = get_array<T>(arrtype, arrsize);
    std::vector<size_t> inx;
    // benchmark
    for (auto _ : state) {
        inx = x86simdsort::argsort(arr.data(), arrsize, false, true);
    }
}

template <typename T, class... Args>
static void simd_ordern_argsort(benchmark::State &state, Args &&...args)
{
    // get args
    auto args_tuple = std::make_tuple(std::move(args)...);
    size_t arrsize = std::get<0>(args_tuple);
    std::string arrtype = std::get<1>(args_tuple);
    // set up array
    std::vector<T> arr = get_array<T>(arrtype, arrsize);
    std::vector<int32_t> arg(arrsize);
    std::iota(arg.begin(), arg.end(), 0);
    // benchmark
    for (auto _ : state) {
        std::vector<T> arr_bkp = arr;
        x86simdsort::keyvalue_qsort(arr_bkp.data(), arg.data(), arrsize);
        state.PauseTiming();
        std::iota(arg.begin(), arg.end(), 0);
        state.ResumeTiming();
    }
}

#define BENCH_BOTH(type) \
    BENCH_SORT(simdargsort, type) \
    BENCH_SORT(simd_revargsort, type) \
    BENCH_SORT(simd_ordern_argsort, type) \
    BENCH_SORT(scalarargsort, type)

BENCH_BOTH(int64_t)
BENCH_BOTH(uint64_t)
BENCH_BOTH(double)
BENCH_BOTH(int32_t)
BENCH_BOTH(uint32_t)
BENCH_BOTH(float)
