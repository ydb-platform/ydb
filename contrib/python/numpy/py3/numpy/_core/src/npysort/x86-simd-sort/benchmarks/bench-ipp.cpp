#include "bench.h"
#include "ipp.h"

template <typename T, class... Args>
static void ippsort(benchmark::State &state, Args &&...args)
{
    auto args_tuple = std::make_tuple(std::move(args)...);
    size_t arrsize = std::get<0>(args_tuple);
    /* IPP set up */
    int bufsize = 10;
    if constexpr (std::is_same_v<T, float>) {
        ippsSortRadixGetBufferSize(arrsize, ipp32f, &bufsize);
    }
    else if constexpr (std::is_same_v<T, double>) {
        ippsSortRadixGetBufferSize(arrsize, ipp64f, &bufsize);
    }
    unsigned char *temp = new unsigned char[bufsize];

    // Get args
    std::string arrtype = std::get<1>(args_tuple);
    // set up array
    std::vector<T> arr = get_array<T>(arrtype, arrsize);
    std::vector<T> arr_bkp = arr;
    // benchmark
    for (auto _ : state) {
        if constexpr (std::is_same_v<T, float>) {
            ippsSortRadixAscend_32f_I(arr.data(), arrsize, temp);
        }
        else if constexpr (std::is_same_v<T, double>) {
            ippsSortRadixAscend_64f_I(arr.data(), arrsize, temp);
        }
        state.PauseTiming();
        arr = arr_bkp;
        state.ResumeTiming();
    }
}

template <typename T, class... Args>
static void ippargsort(benchmark::State &state, Args &&...args)
{
    auto args_tuple = std::make_tuple(std::move(args)...);
    size_t arrsize = std::get<0>(args_tuple);
    /* IPP set up */
    int bufsize = 10;
    if constexpr (std::is_same_v<T, float>) {
        ippsSortRadixIndexGetBufferSize(arrsize, ipp32f, &bufsize);
    }
    else if constexpr (std::is_same_v<T, double>) {
        ippsSortRadixIndexGetBufferSize(arrsize, ipp64f, &bufsize);
    }
    else if constexpr (std::is_same_v<T, int32_t>) {
        ippsSortRadixIndexGetBufferSize(arrsize, ipp32s, &bufsize);
    }
    unsigned char *temp = new unsigned char[bufsize];

    // set up array
    std::string arrtype = std::get<1>(args_tuple);
    std::vector<T> arr = get_array<T>(arrtype, arrsize);
    std::vector<T> arr_bkp = arr;
    std::vector<int32_t> arg(arrsize);
    std::iota(arg.begin(), arg.end(), 0);

    // benchmark
    for (auto _ : state) {
        if constexpr (std::is_same_v<T, float>) {
            ippsSortRadixIndexAscend_32f(
                    arr.data(), 4, arg.data(), arrsize, temp);
        }
        else if constexpr (std::is_same_v<T, double>) {
            ippsSortRadixIndexAscend_64f(
                    arr.data(), 8, arg.data(), arrsize, temp);
        }
        else if constexpr (std::is_same_v<T, int32_t>) {
            ippsSortRadixIndexAscend_32s(
                    arr.data(), 4, arg.data(), arrsize, temp);
        }
        state.PauseTiming();
        arr = arr_bkp;
        std::iota(arg.begin(), arg.end(), 0);
        state.ResumeTiming();
    }
}

BENCH_SORT(ippsort, double)
BENCH_SORT(ippsort, float)
BENCH_SORT(ippargsort, double)
BENCH_SORT(ippargsort, float)
BENCH_SORT(ippargsort, int32_t)
