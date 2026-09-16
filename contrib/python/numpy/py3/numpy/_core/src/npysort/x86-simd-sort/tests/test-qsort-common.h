#ifndef AVX512_TEST_COMMON
#define AVX512_TEST_COMMON

#define XSS_DO_NOT_SET_SEED

#include "custom-compare.h"
#include "rand_array.h"
#include "x86simdsort.h"
#include <gtest/gtest.h>

#define EXPECT_UNIQUE(arg) \
    auto sorted_arg = arg; \
    std::sort(sorted_arg.begin(), sorted_arg.end()); \
    std::vector<size_t> expected_arg(sorted_arg.size()); \
    std::iota(expected_arg.begin(), expected_arg.end(), 0); \
    EXPECT_EQ(sorted_arg, expected_arg) \
            << "Indices aren't unique. Array size = " << sorted_arg.size();

#define REPORT_FAIL(msg, size, type, k) \
    ASSERT_TRUE(false) << msg << ". arr size = " << size \
                       << ", type = " << type << ", k = " << k;

template <typename T>
void IS_SORTED(std::vector<T> sorted, std::vector<T> arr, std::string type)
{
    if (memcmp(arr.data(), sorted.data(), arr.size() * sizeof(T)) != 0) {
        REPORT_FAIL("Array not sorted", arr.size(), type, -1);
    }
}

template <typename T>
void IS_ARG_SORTED(std::vector<T> sortedarr,
                   std::vector<T> arr,
                   std::vector<size_t> arg,
                   std::string type)
{
    EXPECT_UNIQUE(arg)
    std::vector<T> arr_backup;
    for (auto ii : arg) {
        arr_backup.push_back(arr[ii]);
    }
    IS_SORTED(sortedarr, arr_backup, type);
}

template <typename T>
void IS_ARR_PARTITIONED(std::vector<T> arr,
                        size_t k,
                        T true_kth,
                        std::string type,
                        bool descending = false)
{
    std::function<bool(T, T)> cmp_eq, cmp_less, cmp_leq, cmp_geq;
    cmp_eq = compare<T, std::equal_to<T>>();

    if (!descending) {
        cmp_less = compare<T, std::less<T>>();
        cmp_leq = compare<T, std::less_equal<T>>();
        cmp_geq = compare<T, std::greater_equal<T>>();
    }
    else {
        cmp_less = compare<T, std::greater<T>>();
        cmp_leq = compare<T, std::greater_equal<T>>();
        cmp_geq = compare<T, std::less_equal<T>>();
    }

    // 1) arr[k] == sorted[k]; use memcmp to handle nan
    if (!cmp_eq(arr[k], true_kth)) {
        REPORT_FAIL("kth element is incorrect", arr.size(), type, k);
    }
    // ( 2) Elements to the left of k should be atmost arr[k]
    if (k >= 1) {
        T max_left
                = *std::max_element(arr.begin(), arr.begin() + k - 1, cmp_less);
        if (!cmp_geq(arr[k], max_left)) {
            REPORT_FAIL("incorrect left partition", arr.size(), type, k);
        }
    }
    // 3) Elements to the right of k should be atleast arr[k]
    if (k != (size_t)(arr.size() - 1)) {
        T min_right
                = *std::min_element(arr.begin() + k + 1, arr.end(), cmp_less);
        if (!cmp_leq(arr[k], min_right)) {
            REPORT_FAIL("incorrect right partition", arr.size(), type, k);
        }
    }
}

template <typename T>
void IS_ARR_PARTIALSORTED(std::vector<T> arr,
                          size_t k,
                          std::vector<T> sorted,
                          std::string type)
{
    if (memcmp(arr.data(), sorted.data(), k * sizeof(T)) != 0) {
        REPORT_FAIL("Partial array not sorted", arr.size(), type, k);
    }
}

template <typename T>
void IS_ARG_PARTITIONED(std::vector<T> arr,
                        std::vector<size_t> arg,
                        T true_kth,
                        size_t k,
                        std::string type)
{
    EXPECT_UNIQUE(arg)
    std::vector<T> part_arr;
    for (auto ii : arg) {
        part_arr.push_back(arr[ii]);
    }
    IS_ARR_PARTITIONED(part_arr, k, true_kth, type);
}
#endif
