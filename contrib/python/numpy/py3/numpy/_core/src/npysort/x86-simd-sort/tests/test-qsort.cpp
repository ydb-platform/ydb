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
        std::iota(arrsize.begin(), arrsize.end(), 1);
        arrtype = {"random",
                   "constant",
                   "sorted",
                   "reverse",
                   "smallrange",
                   "max_at_the_end",
                   "random_5d",
                   "rand_max",
                   "rand_with_nan"};
    }
    std::vector<std::string> arrtype;
    std::vector<size_t> arrsize = std::vector<size_t>(1024);
};

TYPED_TEST_SUITE_P(simdsort);

TYPED_TEST_P(simdsort, test_qsort_ascending)
{
    for (auto type : this->arrtype) {
        bool hasnan = (type == "rand_with_nan") ? true : false;
        for (auto size : this->arrsize) {
            std::vector<TypeParam> basearr = get_array<TypeParam>(type, size);

            // Ascending order
            std::vector<TypeParam> arr = basearr;
            std::vector<TypeParam> sortedarr = arr;
            std::sort(sortedarr.begin(),
                      sortedarr.end(),
                      compare<TypeParam, std::less<TypeParam>>());
            x86simdsort::qsort(arr.data(), arr.size(), hasnan);
            IS_SORTED(sortedarr, arr, type);

            arr.clear();
            sortedarr.clear();
        }
    }
}

TYPED_TEST_P(simdsort, test_qsort_descending)
{
    for (auto type : this->arrtype) {
        bool hasnan = (type == "rand_with_nan") ? true : false;
        for (auto size : this->arrsize) {
            std::vector<TypeParam> basearr = get_array<TypeParam>(type, size);

            // Descending order
            std::vector<TypeParam> arr = basearr;
            std::vector<TypeParam> sortedarr = arr;
            std::sort(sortedarr.begin(),
                      sortedarr.end(),
                      compare<TypeParam, std::greater<TypeParam>>());
            x86simdsort::qsort(arr.data(), arr.size(), hasnan, true);
            IS_SORTED(sortedarr, arr, type);

            arr.clear();
            sortedarr.clear();
        }
    }
}

TYPED_TEST_P(simdsort, test_argsort_ascending)
{
    for (auto type : this->arrtype) {
        bool hasnan = (type == "rand_with_nan") ? true : false;
        for (auto size : this->arrsize) {
            std::vector<TypeParam> arr = get_array<TypeParam>(type, size);
            std::vector<TypeParam> sortedarr = arr;
            std::sort(sortedarr.begin(),
                      sortedarr.end(),
                      compare<TypeParam, std::less<TypeParam>>());
            auto arg = x86simdsort::argsort(arr.data(), arr.size(), hasnan);
            IS_ARG_SORTED(sortedarr, arr, arg, type);
            arr.clear();
            arg.clear();
        }
    }
}

TYPED_TEST_P(simdsort, test_argsort_descending)
{
    for (auto type : this->arrtype) {
        bool hasnan = (type == "rand_with_nan") ? true : false;
        for (auto size : this->arrsize) {
            std::vector<TypeParam> arr = get_array<TypeParam>(type, size);
            std::vector<TypeParam> sortedarr = arr;
            std::sort(sortedarr.begin(),
                      sortedarr.end(),
                      compare<TypeParam, std::greater<TypeParam>>());
            auto arg = x86simdsort::argsort(
                    arr.data(), arr.size(), hasnan, true);
            IS_ARG_SORTED(sortedarr, arr, arg, type);
            arr.clear();
            arg.clear();
        }
    }
}

TYPED_TEST_P(simdsort, test_qselect_ascending)
{
    for (auto type : this->arrtype) {
        bool hasnan = (type == "rand_with_nan") ? true : false;
        for (auto size : this->arrsize) {
            size_t k = rand() % size;
            std::vector<TypeParam> basearr = get_array<TypeParam>(type, size);

            // Ascending order
            std::vector<TypeParam> arr = basearr;
            std::vector<TypeParam> sortedarr = arr;
            std::nth_element(sortedarr.begin(),
                             sortedarr.begin() + k,
                             sortedarr.end(),
                             compare<TypeParam, std::less<TypeParam>>());
            x86simdsort::qselect(arr.data(), k, arr.size(), hasnan);
            IS_ARR_PARTITIONED(arr, k, sortedarr[k], type);

            arr.clear();
            sortedarr.clear();
        }
    }
}

TYPED_TEST_P(simdsort, test_qselect_descending)
{
    for (auto type : this->arrtype) {
        bool hasnan = (type == "rand_with_nan") ? true : false;
        for (auto size : this->arrsize) {
            size_t k = rand() % size;
            std::vector<TypeParam> basearr = get_array<TypeParam>(type, size);

            // Descending order
            std::vector<TypeParam> arr = basearr;
            std::vector<TypeParam> sortedarr = arr;
            std::nth_element(sortedarr.begin(),
                             sortedarr.begin() + k,
                             sortedarr.end(),
                             compare<TypeParam, std::greater<TypeParam>>());
            x86simdsort::qselect(arr.data(), k, arr.size(), hasnan, true);
            IS_ARR_PARTITIONED(arr, k, sortedarr[k], type, true);

            arr.clear();
            sortedarr.clear();
        }
    }
}

TYPED_TEST_P(simdsort, test_argselect)
{
    for (auto type : this->arrtype) {
        bool hasnan = (type == "rand_with_nan") ? true : false;
        for (auto size : this->arrsize) {
            size_t k = rand() % size;
            std::vector<TypeParam> arr = get_array<TypeParam>(type, size);
            std::vector<TypeParam> sortedarr = arr;
            std::sort(sortedarr.begin(),
                      sortedarr.end(),
                      compare<TypeParam, std::less<TypeParam>>());
            auto arg
                    = x86simdsort::argselect(arr.data(), k, arr.size(), hasnan);
            IS_ARG_PARTITIONED(arr, arg, sortedarr[k], k, type);
            arr.clear();
            sortedarr.clear();
        }
    }
}

TYPED_TEST_P(simdsort, test_partial_qsort_ascending)
{
    for (auto type : this->arrtype) {
        bool hasnan = (type == "rand_with_nan") ? true : false;
        for (auto size : this->arrsize) {
            size_t k = rand() % size;
            std::vector<TypeParam> basearr = get_array<TypeParam>(type, size);

            // Ascending order
            std::vector<TypeParam> arr = basearr;
            std::vector<TypeParam> sortedarr = arr;
            std::sort(sortedarr.begin(),
                      sortedarr.end(),
                      compare<TypeParam, std::less<TypeParam>>());
            x86simdsort::partial_qsort(arr.data(), k, arr.size(), hasnan);
            IS_ARR_PARTIALSORTED(arr, k, sortedarr, type);

            arr.clear();
            sortedarr.clear();
        }
    }
}

TYPED_TEST_P(simdsort, test_partial_qsort_descending)
{
    for (auto type : this->arrtype) {
        bool hasnan = (type == "rand_with_nan") ? true : false;
        for (auto size : this->arrsize) {
            // k should be at least 1
            size_t k = std::max((size_t)1, rand() % size);
            std::vector<TypeParam> basearr = get_array<TypeParam>(type, size);

            // Descending order
            std::vector<TypeParam> arr = basearr;
            std::vector<TypeParam> sortedarr = arr;
            std::sort(sortedarr.begin(),
                      sortedarr.end(),
                      compare<TypeParam, std::greater<TypeParam>>());
            x86simdsort::partial_qsort(arr.data(), k, arr.size(), hasnan, true);
            IS_ARR_PARTIALSORTED(arr, k, sortedarr, type);

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
// support for _Float16 is incomplete in gcc-12
#if __GNUC__ >= 13
                                      _Float16,
#endif
                                      float,
                                      double,
                                      uint32_t,
                                      int32_t,
                                      uint64_t,
                                      int64_t>;

INSTANTIATE_TYPED_TEST_SUITE_P(xss, simdsort, QSortTestTypes);
