/*******************************************
 * * Copyright (C) 2022-2023 Intel Corporation
 * * SPDX-License-Identifier: BSD-3-Clause
 * *******************************************/

#include "rand_array.h"
#include "x86simdsort.h"
#include <gtest/gtest.h>

template <typename T>
struct P {
    T x, y;
    T metric() const
    {
        return x;
    }
    bool operator==(const P<T> &a) const
    {
        return a.x == x; // && a.y == y;
    }
};

template <typename T>
class simdobjsort : public ::testing::Test {
public:
    simdobjsort()
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

TYPED_TEST_SUITE_P(simdobjsort);

TYPED_TEST_P(simdobjsort, test_objsort)
{
    for (auto type : this->arrtype) {
        for (auto size : this->arrsize) {
            std::vector<TypeParam> x = get_array<TypeParam>(type, size);
            std::vector<TypeParam> y = get_array<TypeParam>("random", size);
            std::vector<P<TypeParam>> arr(size);
            for (size_t ii = 0; ii < size; ++ii) {
                arr[ii].x = x[ii];
                arr[ii].y = y[ii];
            }
            std::vector<P<TypeParam>> arr_bckp = arr;
            x86simdsort::object_qsort(arr.data(), size, [](P<TypeParam> p) {
                return p.metric();
            });
            std::sort(arr_bckp.begin(),
                      arr_bckp.end(),
                      [](const P<TypeParam> &a, const P<TypeParam> &b) {
                          return a.metric() < b.metric();
                      });
            ASSERT_EQ(arr, arr_bckp);
            arr.clear();
            arr_bckp.clear();
        }
    }
}

REGISTER_TYPED_TEST_SUITE_P(simdobjsort, test_objsort);

using QObjSortTestTypes
        = testing::Types<double, uint64_t, int64_t, uint32_t, int32_t, float>;

INSTANTIATE_TYPED_TEST_SUITE_P(xss, simdobjsort, QObjSortTestTypes);
