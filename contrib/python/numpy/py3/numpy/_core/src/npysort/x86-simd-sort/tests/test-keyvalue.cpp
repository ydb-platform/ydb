/*******************************************
 * * Copyright (C) 2022-2023 Intel Corporation
 * * SPDX-License-Identifier: BSD-3-Clause
 * *******************************************/

#include "rand_array.h"
#include "x86simdsort.h"
#include "x86simdsort-scalar.h"
#include "test-qsort-common.h"
#include <gtest/gtest.h>

template <typename T>
class simdkvsort : public ::testing::Test {
public:
    simdkvsort()
    {
        std::iota(arrsize.begin(), arrsize.end(), 1);
        arrtype = {"random",
                   "constant",
                   "sorted",
                   "reverse",
                   "smallrange",
                   "max_at_the_end",
                   "random_5d",
                   "rand_max"};
    }
    std::vector<std::string> arrtype;
    std::vector<size_t> arrsize = std::vector<size_t>(1024);
};

TYPED_TEST_SUITE_P(simdkvsort);

template <typename T>
bool same_values(T *v1, T *v2, size_t size)
{
    // Checks that the values are the same except ordering
    auto cmp_eq = compare<T, std::equal_to<T>>();

    x86simdsort::qsort(v1, size, true);
    x86simdsort::qsort(v2, size, true);

    for (size_t i = 0; i < size; i++) {
        if (!cmp_eq(v1[i], v2[i])) { return false; }
    }

    return true;
}

template <typename T1, typename T2>
bool is_kv_sorted(
        T1 *keys_comp, T2 *vals_comp, T1 *keys_ref, T2 *vals_ref, size_t size)
{
    auto cmp_eq = compare<T1, std::equal_to<T1>>();

    // First check keys are exactly identical
    for (size_t i = 0; i < size; i++) {
        if (!cmp_eq(keys_comp[i], keys_ref[i])) { return false; }
    }

    size_t i_start = 0;
    T1 key_start = keys_comp[0];
    // Loop through all identical keys in a block, then compare the sets of values to make sure they are identical
    // We need the index after the loop
    size_t i = 0;
    for (; i < size; i++) {
        if (!cmp_eq(keys_comp[i], key_start)) {
            // Check that every value in this block of constant keys

            if (!same_values(
                        vals_ref + i_start, vals_comp + i_start, i - i_start)) {
                return false;
            }

            // Now setup the start variables to begin gathering keys for the next group
            i_start = i;
            key_start = keys_comp[i];
        }
    }

    // Handle the last group
    if (!same_values(vals_ref + i_start, vals_comp + i_start, i - i_start)) {
        return false;
    }

    return true;
}

template <typename T1, typename T2>
bool is_kv_partialsorted(T1 *keys_comp,
                         T2 *vals_comp,
                         T1 *keys_ref,
                         T2 *vals_ref,
                         size_t size,
                         size_t k)
{
    auto cmp_eq = compare<T1, std::equal_to<T1>>();

    // First check keys are exactly identical (up to k)
    for (size_t i = 0; i < k; i++) {
        if (!cmp_eq(keys_comp[i], keys_ref[i])) { return false; }
    }

    size_t i_start = 0;
    T1 key_start = keys_comp[0];
    // Loop through all identical keys in a block, then compare the sets of values to make sure they are identical
    for (size_t i = 0; i < k; i++) {
        if (!cmp_eq(keys_comp[i], key_start)) {
            // Check that every value in this block of constant keys

            if (!same_values(
                        vals_ref + i_start, vals_comp + i_start, i - i_start)) {
                return false;
            }

            // Now setup the start variables to begin gathering keys for the next group
            i_start = i;
            key_start = keys_comp[i];
        }
    }

    // Now, we need to do some more work to handle keys exactly equal to the true kth
    // First, fully kvsort both arrays
    xss::scalar::keyvalue_qsort<T1, T2>(keys_ref, vals_ref, size, true, false);
    xss::scalar::keyvalue_qsort<T1, T2>(
            keys_comp, vals_comp, size, true, false);

    auto trueKth = keys_ref[k];
    bool notFoundFirst = true;
    size_t i = 0;

    for (; i < size; i++) {
        if (notFoundFirst && cmp_eq(keys_ref[i], trueKth)) {
            notFoundFirst = false;
            i_start = i;
        }
        else if (!notFoundFirst && !cmp_eq(keys_ref[i], trueKth)) {
            break;
        }
    }

    if (notFoundFirst) return false;

    if (!same_values(vals_ref + i_start, vals_comp + i_start, i - i_start)) {
        return false;
    }

    return true;
}

TYPED_TEST_P(simdkvsort, test_kvsort_ascending)
{
    using T1 = typename std::tuple_element<0, decltype(TypeParam())>::type;
    using T2 = typename std::tuple_element<1, decltype(TypeParam())>::type;
    for (auto type : this->arrtype) {
        bool hasnan = (type == "rand_with_nan") ? true : false;
        for (auto size : this->arrsize) {
            std::vector<T1> key = get_array<T1>(type, size);
            std::vector<T2> val = get_array<T2>(type, size);
            std::vector<T1> key_bckp = key;
            std::vector<T2> val_bckp = val;
            x86simdsort::keyvalue_qsort(
                    key.data(), val.data(), size, hasnan, false);
            xss::scalar::keyvalue_qsort(
                    key_bckp.data(), val_bckp.data(), size, hasnan, false);

            bool is_kv_sorted_ = is_kv_sorted<T1, T2>(key.data(),
                                                      val.data(),
                                                      key_bckp.data(),
                                                      val_bckp.data(),
                                                      size);
            ASSERT_EQ(is_kv_sorted_, true);

            key.clear();
            val.clear();
            key_bckp.clear();
            val_bckp.clear();
        }
    }
}

TYPED_TEST_P(simdkvsort, test_kvsort_descending)
{
    using T1 = typename std::tuple_element<0, decltype(TypeParam())>::type;
    using T2 = typename std::tuple_element<1, decltype(TypeParam())>::type;
    for (auto type : this->arrtype) {
        bool hasnan = (type == "rand_with_nan") ? true : false;
        for (auto size : this->arrsize) {
            std::vector<T1> key = get_array<T1>(type, size);
            std::vector<T2> val = get_array<T2>(type, size);
            std::vector<T1> key_bckp = key;
            std::vector<T2> val_bckp = val;
            x86simdsort::keyvalue_qsort(
                    key.data(), val.data(), size, hasnan, true);
            xss::scalar::keyvalue_qsort(
                    key_bckp.data(), val_bckp.data(), size, hasnan, true);

            bool is_kv_sorted_ = is_kv_sorted<T1, T2>(key.data(),
                                                      val.data(),
                                                      key_bckp.data(),
                                                      val_bckp.data(),
                                                      size);
            ASSERT_EQ(is_kv_sorted_, true);

            key.clear();
            val.clear();
            key_bckp.clear();
            val_bckp.clear();
        }
    }
}

TYPED_TEST_P(simdkvsort, test_kvselect_ascending)
{
    using T1 = typename std::tuple_element<0, decltype(TypeParam())>::type;
    using T2 = typename std::tuple_element<1, decltype(TypeParam())>::type;
    for (auto type : this->arrtype) {
        bool hasnan = (type == "rand_with_nan") ? true : false;
        for (auto size : this->arrsize) {
            size_t k = rand() % size;

            std::vector<T1> key = get_array<T1>(type, size);
            std::vector<T2> val = get_array<T2>(type, size);
            std::vector<T1> key_bckp = key;
            std::vector<T2> val_bckp = val;

            xss::scalar::keyvalue_qsort(
                    key_bckp.data(), val_bckp.data(), size, hasnan, false);

            // Test select by using it as part of partial_sort
            x86simdsort::keyvalue_select(
                    key.data(), val.data(), k, size, hasnan, false);
            IS_ARR_PARTITIONED<T1>(key, k, key_bckp[k], type);
            xss::scalar::keyvalue_qsort(
                    key.data(), val.data(), k, hasnan, false);

            ASSERT_EQ(key[k], key_bckp[k]);

            bool is_kv_partialsorted_
                    = is_kv_partialsorted<T1, T2>(key.data(),
                                                  val.data(),
                                                  key_bckp.data(),
                                                  val_bckp.data(),
                                                  size,
                                                  k);
            ASSERT_EQ(is_kv_partialsorted_, true);

            key.clear();
            val.clear();
            key_bckp.clear();
            val_bckp.clear();
        }
    }
}

TYPED_TEST_P(simdkvsort, test_kvselect_descending)
{
    using T1 = typename std::tuple_element<0, decltype(TypeParam())>::type;
    using T2 = typename std::tuple_element<1, decltype(TypeParam())>::type;
    for (auto type : this->arrtype) {
        bool hasnan = (type == "rand_with_nan") ? true : false;
        for (auto size : this->arrsize) {
            size_t k = rand() % size;

            std::vector<T1> key = get_array<T1>(type, size);
            std::vector<T2> val = get_array<T2>(type, size);
            std::vector<T1> key_bckp = key;
            std::vector<T2> val_bckp = val;

            xss::scalar::keyvalue_qsort(
                    key_bckp.data(), val_bckp.data(), size, hasnan, true);

            // Test select by using it as part of partial_sort
            x86simdsort::keyvalue_select(
                    key.data(), val.data(), k, size, hasnan, true);
            IS_ARR_PARTITIONED<T1>(key, k, key_bckp[k], type, true);
            xss::scalar::keyvalue_qsort(
                    key.data(), val.data(), k, hasnan, true);

            ASSERT_EQ(key[k], key_bckp[k]);

            bool is_kv_partialsorted_
                    = is_kv_partialsorted<T1, T2>(key.data(),
                                                  val.data(),
                                                  key_bckp.data(),
                                                  val_bckp.data(),
                                                  size,
                                                  k);
            ASSERT_EQ(is_kv_partialsorted_, true);

            key.clear();
            val.clear();
            key_bckp.clear();
            val_bckp.clear();
        }
    }
}

TYPED_TEST_P(simdkvsort, test_kvpartial_sort_ascending)
{
    using T1 = typename std::tuple_element<0, decltype(TypeParam())>::type;
    using T2 = typename std::tuple_element<1, decltype(TypeParam())>::type;
    for (auto type : this->arrtype) {
        bool hasnan = (type == "rand_with_nan") ? true : false;
        for (auto size : this->arrsize) {
            size_t k = rand() % size;

            std::vector<T1> key = get_array<T1>(type, size);
            std::vector<T2> val = get_array<T2>(type, size);
            std::vector<T1> key_bckp = key;
            std::vector<T2> val_bckp = val;
            x86simdsort::keyvalue_partial_sort(
                    key.data(), val.data(), k, size, hasnan, false);
            xss::scalar::keyvalue_qsort(
                    key_bckp.data(), val_bckp.data(), size, hasnan, false);

            IS_ARR_PARTIALSORTED<T1>(key, k, key_bckp, type);

            bool is_kv_partialsorted_
                    = is_kv_partialsorted<T1, T2>(key.data(),
                                                  val.data(),
                                                  key_bckp.data(),
                                                  val_bckp.data(),
                                                  size,
                                                  k);
            ASSERT_EQ(is_kv_partialsorted_, true);

            key.clear();
            val.clear();
            key_bckp.clear();
            val_bckp.clear();
        }
    }
}

TYPED_TEST_P(simdkvsort, test_kvpartial_sort_descending)
{
    using T1 = typename std::tuple_element<0, decltype(TypeParam())>::type;
    using T2 = typename std::tuple_element<1, decltype(TypeParam())>::type;
    for (auto type : this->arrtype) {
        bool hasnan = (type == "rand_with_nan") ? true : false;
        for (auto size : this->arrsize) {
            size_t k = rand() % size;

            std::vector<T1> key = get_array<T1>(type, size);
            std::vector<T2> val = get_array<T2>(type, size);
            std::vector<T1> key_bckp = key;
            std::vector<T2> val_bckp = val;
            x86simdsort::keyvalue_partial_sort(
                    key.data(), val.data(), k, size, hasnan, true);
            xss::scalar::keyvalue_qsort(
                    key_bckp.data(), val_bckp.data(), size, hasnan, true);

            IS_ARR_PARTIALSORTED<T1>(key, k, key_bckp, type);

            bool is_kv_partialsorted_
                    = is_kv_partialsorted<T1, T2>(key.data(),
                                                  val.data(),
                                                  key_bckp.data(),
                                                  val_bckp.data(),
                                                  size,
                                                  k);
            ASSERT_EQ(is_kv_partialsorted_, true);

            key.clear();
            val.clear();
            key_bckp.clear();
            val_bckp.clear();
        }
    }
}

REGISTER_TYPED_TEST_SUITE_P(simdkvsort,
                            test_kvsort_ascending,
                            test_kvsort_descending,
                            test_kvselect_ascending,
                            test_kvselect_descending,
                            test_kvpartial_sort_ascending,
                            test_kvpartial_sort_descending);

#define CREATE_TUPLES(type) \
    std::tuple<double, type>, std::tuple<uint64_t, type>, \
            std::tuple<int64_t, type>, std::tuple<float, type>, \
            std::tuple<uint32_t, type>, std::tuple<int32_t, type>

using QKVSortTestTypes = testing::Types<CREATE_TUPLES(double),
                                        CREATE_TUPLES(uint64_t),
                                        CREATE_TUPLES(int64_t),
                                        CREATE_TUPLES(uint32_t),
                                        CREATE_TUPLES(int32_t),
                                        CREATE_TUPLES(float)>;

INSTANTIATE_TYPED_TEST_SUITE_P(xss, simdkvsort, QKVSortTestTypes);
