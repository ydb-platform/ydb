/*******************************************
 * * Copyright (C) 2022 Intel Corporation
 * * SPDX-License-Identifier: BSD-3-Clause
 * *******************************************/

#include "test-qsort-common.h"

template <typename T>
class simdsort : public ::testing::Test {
public:
    simdsort()
    {
        std::iota(arrsize.begin(), arrsize.end(), 0);
        std::iota(arrsize_long.begin(), arrsize_long.end(), 0);
#ifdef XSS_USE_OPENMP
        // These extended tests are only needed for the OpenMP logic
        arrsize_long.push_back(10'000);
        arrsize_long.push_back(100'000);
        arrsize_long.push_back(1'000'000);
#endif

        arrtype = {"random",
                   "constant",
                   "sorted",
                   "reverse",
                   "smallrange",
                   "max_at_the_end",
                   "random_5d",
                   "rand_max",
                   "rand_with_nan",
                   "rand_with_max_and_nan"};
    }
    std::vector<std::string> arrtype;
    std::vector<size_t> arrsize = std::vector<size_t>(1024);
    std::vector<size_t> arrsize_long = std::vector<size_t>(1024);
};

TYPED_TEST_SUITE_P(simdsort);

TYPED_TEST_P(simdsort, test_qsort_ascending)
{
    for (auto type : this->arrtype) {
        bool hasnan = is_nan_test(type);
        for (auto size : this->arrsize_long) {
            std::vector<TypeParam> basearr = get_array<TypeParam>(type, size);

            // Ascending order
            std::vector<TypeParam> arr = basearr;
            std::vector<TypeParam> sortedarr = arr;

            x86simdsort::qsort(arr.data(), arr.size(), hasnan);
#ifndef XSS_ASAN_CI_NOCHECK
            std::sort(sortedarr.begin(),
                      sortedarr.end(),
                      compare<TypeParam, std::less<TypeParam>>());
            IS_SORTED(sortedarr, arr, type);
#endif
            arr.clear();
            sortedarr.clear();
        }
    }
}

TYPED_TEST_P(simdsort, test_qsort_descending)
{
    for (auto type : this->arrtype) {
        bool hasnan = is_nan_test(type);
        for (auto size : this->arrsize_long) {
            std::vector<TypeParam> basearr = get_array<TypeParam>(type, size);

            // Descending order
            std::vector<TypeParam> arr = basearr;
            std::vector<TypeParam> sortedarr = arr;

            x86simdsort::qsort(arr.data(), arr.size(), hasnan, true);
#ifndef XSS_ASAN_CI_NOCHECK
            std::sort(sortedarr.begin(),
                      sortedarr.end(),
                      compare<TypeParam, std::greater<TypeParam>>());
            IS_SORTED(sortedarr, arr, type);
#endif
            arr.clear();
            sortedarr.clear();
        }
    }
}

TYPED_TEST_P(simdsort, test_argsort_ascending)
{
    for (auto type : this->arrtype) {
        bool hasnan = is_nan_test(type);
        for (auto size : this->arrsize_long) {
            std::vector<TypeParam> arr = get_array<TypeParam>(type, size);
            std::vector<TypeParam> sortedarr = arr;

            auto arg = x86simdsort::argsort(arr.data(), arr.size(), hasnan);
#ifndef XSS_ASAN_CI_NOCHECK
            std::sort(sortedarr.begin(),
                      sortedarr.end(),
                      compare<TypeParam, std::less<TypeParam>>());
            IS_ARG_SORTED(sortedarr, arr, arg, type);
#endif
            arr.clear();
            arg.clear();
        }
    }
}

TYPED_TEST_P(simdsort, test_argsort_descending)
{
    for (auto type : this->arrtype) {
        bool hasnan = is_nan_test(type);
        for (auto size : this->arrsize_long) {
            std::vector<TypeParam> arr = get_array<TypeParam>(type, size);
            std::vector<TypeParam> sortedarr = arr;

            auto arg = x86simdsort::argsort(
                    arr.data(), arr.size(), hasnan, true);
#ifndef XSS_ASAN_CI_NOCHECK
            std::sort(sortedarr.begin(),
                      sortedarr.end(),
                      compare<TypeParam, std::greater<TypeParam>>());
            IS_ARG_SORTED(sortedarr, arr, arg, type);
#endif
            arr.clear();
            arg.clear();
        }
    }
}

TYPED_TEST_P(simdsort, test_qselect_ascending)
{
    for (auto type : this->arrtype) {
        bool hasnan = is_nan_test(type);
        for (auto size : this->arrsize) {
            size_t k = size != 0 ? rand() % size : 0;
            std::vector<TypeParam> basearr = get_array<TypeParam>(type, size);

            // Ascending order
            std::vector<TypeParam> arr = basearr;
            std::vector<TypeParam> sortedarr = arr;

            x86simdsort::qselect(arr.data(), k, arr.size(), hasnan);
#ifndef XSS_ASAN_CI_NOCHECK
            std::nth_element(sortedarr.begin(),
                             sortedarr.begin() + k,
                             sortedarr.end(),
                             compare<TypeParam, std::less<TypeParam>>());
            if (size == 0) continue;
            IS_ARR_PARTITIONED(arr, k, sortedarr[k], type);
#endif
            arr.clear();
            sortedarr.clear();
        }
    }
}

TYPED_TEST_P(simdsort, test_qselect_descending)
{
    for (auto type : this->arrtype) {
        bool hasnan = is_nan_test(type);
        for (auto size : this->arrsize) {
            size_t k = size != 0 ? rand() % size : 0;
            std::vector<TypeParam> basearr = get_array<TypeParam>(type, size);

            // Descending order
            std::vector<TypeParam> arr = basearr;
            std::vector<TypeParam> sortedarr = arr;

            x86simdsort::qselect(arr.data(), k, arr.size(), hasnan, true);
#ifndef XSS_ASAN_CI_NOCHECK
            std::nth_element(sortedarr.begin(),
                             sortedarr.begin() + k,
                             sortedarr.end(),
                             compare<TypeParam, std::greater<TypeParam>>());
            if (size == 0) continue;
            IS_ARR_PARTITIONED(arr, k, sortedarr[k], type, true);
#endif
            arr.clear();
            sortedarr.clear();
        }
    }
}

TYPED_TEST_P(simdsort, test_argselect)
{
    for (auto type : this->arrtype) {
        bool hasnan = is_nan_test(type);
        for (auto size : this->arrsize) {
            size_t k = size != 0 ? rand() % size : 0;
            std::vector<TypeParam> arr = get_array<TypeParam>(type, size);
            std::vector<TypeParam> sortedarr = arr;

            auto arg
                    = x86simdsort::argselect(arr.data(), k, arr.size(), hasnan);
#ifndef XSS_ASAN_CI_NOCHECK
            std::sort(sortedarr.begin(),
                      sortedarr.end(),
                      compare<TypeParam, std::less<TypeParam>>());
            if (size == 0) continue;
            IS_ARG_PARTITIONED(arr, arg, sortedarr[k], k, type);
#endif
            arr.clear();
            sortedarr.clear();
        }
    }
}

TYPED_TEST_P(simdsort, test_partial_qsort_ascending)
{
    for (auto type : this->arrtype) {
        bool hasnan = is_nan_test(type);
        for (auto size : this->arrsize) {
            size_t k = size != 0 ? rand() % size : 0;
            std::vector<TypeParam> basearr = get_array<TypeParam>(type, size);

            // Ascending order
            std::vector<TypeParam> arr = basearr;
            std::vector<TypeParam> sortedarr = arr;

            x86simdsort::partial_qsort(arr.data(), k, arr.size(), hasnan);
#ifndef XSS_ASAN_CI_NOCHECK
            std::sort(sortedarr.begin(),
                      sortedarr.end(),
                      compare<TypeParam, std::less<TypeParam>>());
            if (size == 0) continue;
            IS_ARR_PARTIALSORTED(arr, k, sortedarr, type);
#endif
            arr.clear();
            sortedarr.clear();
        }
    }
}

TYPED_TEST_P(simdsort, test_partial_qsort_descending)
{
    for (auto type : this->arrtype) {
        bool hasnan = is_nan_test(type);
        for (auto size : this->arrsize) {
            size_t k = size != 0 ? rand() % size : 0;
            std::vector<TypeParam> basearr = get_array<TypeParam>(type, size);

            // Descending order
            std::vector<TypeParam> arr = basearr;
            std::vector<TypeParam> sortedarr = arr;

            x86simdsort::partial_qsort(arr.data(), k, arr.size(), hasnan, true);
#ifndef XSS_ASAN_CI_NOCHECK
            std::sort(sortedarr.begin(),
                      sortedarr.end(),
                      compare<TypeParam, std::greater<TypeParam>>());
            if (size == 0) continue;
            IS_ARR_PARTIALSORTED(arr, k, sortedarr, type);
#endif
            arr.clear();
            sortedarr.clear();
        }
    }
}

TYPED_TEST_P(simdsort, test_comparator)
{
    if constexpr (xss::fp::is_floating_point_v<TypeParam>) {
        auto less = compare<TypeParam, std::less<TypeParam>>();
        auto leq = compare<TypeParam, std::less_equal<TypeParam>>();
        auto greater = compare<TypeParam, std::greater<TypeParam>>();
        auto geq = compare<TypeParam, std::greater_equal<TypeParam>>();
        auto equal = compare<TypeParam, std::equal_to<TypeParam>>();
        TypeParam nan = xss::fp::quiet_NaN<TypeParam>();
        TypeParam inf = xss::fp::infinity<TypeParam>();
        ASSERT_EQ(less(nan, inf), false);
        ASSERT_EQ(less(nan, nan), false);
        ASSERT_EQ(less(inf, nan), true);
        ASSERT_EQ(less(inf, inf), false);
        ASSERT_EQ(leq(nan, inf), false);
        ASSERT_EQ(leq(nan, nan), true);
        ASSERT_EQ(leq(inf, nan), true);
        ASSERT_EQ(leq(inf, inf), true);
        ASSERT_EQ(geq(nan, inf), true);
        ASSERT_EQ(geq(nan, nan), true);
        ASSERT_EQ(geq(inf, nan), false);
        ASSERT_EQ(geq(inf, inf), true);
        ASSERT_EQ(greater(nan, inf), true);
        ASSERT_EQ(greater(nan, nan), false);
        ASSERT_EQ(greater(inf, nan), false);
        ASSERT_EQ(greater(inf, inf), false);
        ASSERT_EQ(equal(nan, inf), false);
        ASSERT_EQ(equal(nan, nan), true);
        ASSERT_EQ(equal(inf, nan), false);
        ASSERT_EQ(equal(inf, inf), true);
    }
}

REGISTER_TYPED_TEST_SUITE_P(simdsort,
                            test_qsort_ascending,
                            test_qsort_descending,
                            test_argsort_ascending,
                            test_argsort_descending,
                            test_argselect,
                            test_qselect_ascending,
                            test_qselect_descending,
                            test_partial_qsort_ascending,
                            test_partial_qsort_descending,
                            test_comparator);

using QSortTestTypes = testing::Types<uint16_t,
                                      int16_t,
// support for _Float16 is incomplete in gcc-12, clang < 6
#if __GNUC__ >= 13 || __clang_major__ >= 6
                                      _Float16,
#endif
                                      float,
                                      double,
                                      uint32_t,
                                      int32_t,
                                      uint64_t,
                                      int64_t>;

INSTANTIATE_TYPED_TEST_SUITE_P(xss, simdsort, QSortTestTypes);
