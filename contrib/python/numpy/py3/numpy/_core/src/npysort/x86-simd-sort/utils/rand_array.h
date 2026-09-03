/*******************************************
 * * Copyright (C) 2022 Intel Corporation
 * * SPDX-License-Identifier: BSD-3-Clause
 * *******************************************/
#ifndef UTILS_RAND_ARRAY
#define UTILS_RAND_ARRAY

#include <iostream>
#include <random>
#include <type_traits>
#include <vector>
#include <algorithm>
#include "xss-custom-float.h"

template <typename T>
static std::vector<T> get_uniform_rand_array(int64_t arrsize,
                                             T max = xss::fp::max<T>(),
                                             T min = xss::fp::min<T>())
{
    std::vector<T> arr;
    std::random_device rd;
    if constexpr (std::is_floating_point_v<T>) {
        std::mt19937 gen(rd());
#ifndef XSS_DO_NOT_SET_SEED
        gen.seed(42);
#endif
        std::uniform_real_distribution<T> dis(min, max);
        for (int64_t ii = 0; ii < arrsize; ++ii) {
            arr.emplace_back(dis(gen));
        }
    }
#ifdef __FLT16_MAX__
    else if constexpr (std::is_same_v<T, _Float16>) {
        (void)(max);
        (void)(min);
        for (auto jj = 0; jj < arrsize; ++jj) {
            float temp = (float)rand() / (float)(RAND_MAX);
            arr.push_back((_Float16)temp);
        }
    }
#endif
    else if constexpr (std::is_integral_v<T>) {
        std::default_random_engine e1(rd());
#ifndef XSS_DO_NOT_SET_SEED
        e1.seed(42);
#endif
        std::uniform_int_distribution<T> uniform_dist(min, max);
        for (int64_t ii = 0; ii < arrsize; ++ii) {
            arr.emplace_back(uniform_dist(e1));
        }
    }
    return arr;
}

template <typename T>
static std::vector<T> get_uniform_rand_array_with_uniquevalues(
        int64_t arrsize, T max = xss::fp::max<T>(), T min = xss::fp::min<T>())
{
    std::vector<T> arr = get_uniform_rand_array<T>(arrsize, max, min);
    typename std::vector<T>::iterator ip
            = std::unique(arr.begin(), arr.begin() + arrsize);
    arr.resize(std::distance(arr.begin(), ip));
    return arr;
}

template <typename T>
static std::vector<T> get_array(std::string arrtype,
                                size_t arrsize,
                                T min = xss::fp::min<T>(),
                                T max = xss::fp::max<T>())
{
    std::vector<T> arr;
    if (arrtype == "random") {
        arr = get_uniform_rand_array<T>(arrsize, max, min);
    }
    else if (arrtype == "sorted") {
        arr = get_uniform_rand_array<T>(arrsize, max, min);
        std::sort(arr.begin(), arr.end());
    }
    else if (arrtype == "constant") {
        T temp = get_uniform_rand_array<T>(1, max, min)[0];
        for (size_t ii = 0; ii < arrsize; ++ii) {
            arr.push_back(temp);
        }
    }
    else if (arrtype == "reverse") {
        arr = get_uniform_rand_array<T>(arrsize, max, min);
        std::sort(arr.begin(), arr.end());
        std::reverse(arr.begin(), arr.end());
    }
    else if (arrtype == "smallrange") {
        arr = get_uniform_rand_array<T>(arrsize, 20, 1);
    }
    else if (arrtype == "random_5d") {
        size_t temp = std::max((size_t)1, (size_t)(0.5 * arrsize));
        std::vector<T> temparr = get_uniform_rand_array<T>(temp);
        for (size_t ii = 0; ii < arrsize; ++ii) {
            if (ii < temp) { arr.push_back(temparr[ii]); }
            else {
                arr.push_back((T)0);
            }
        }
        std::shuffle(arr.begin(), arr.end(), std::default_random_engine(42));
    }
    else if (arrtype == "max_at_the_end") {
        arr = get_uniform_rand_array<T>(arrsize, max, min);
        if (xss::fp::is_floating_point_v<T>) {
            arr[arrsize - 1] = xss::fp::infinity<T>();
        }
        else {
            arr[arrsize - 1] = std::numeric_limits<T>::max();
        }
    }
    else if (arrtype == "rand_with_nan") {
        arr = get_uniform_rand_array<T>(arrsize, max, min);
        int64_t num_nans = 10 % arrsize;
        std::vector<int64_t> rand_indx
                = get_uniform_rand_array<int64_t>(num_nans, arrsize - 1, 0);
        T val;
        if constexpr (xss::fp::is_floating_point_v<T>) {
            val = xss::fp::quiet_NaN<T>();
        }
        else {
            val = std::numeric_limits<T>::max();
        }
        for (auto ind : rand_indx) {
            arr[ind] = val;
        }
    }
    else if (arrtype == "rand_max") {
        arr = get_uniform_rand_array<T>(arrsize, max, min);
        T val;
        if constexpr (xss::fp::is_floating_point_v<T>) {
            val = xss::fp::infinity<T>();
        }
        else {
            val = std::numeric_limits<T>::max();
        }
        for (size_t ii = 1; ii <= arrsize; ++ii) {
            if (rand() % 0x1) { arr[ii] = val; }
        }
    }
    else {
        std::cout << "Warning: unrecognized array type " << arrtype
                  << std::endl;
    }
    return arr;
}

#endif // UTILS_RAND_ARRAY
