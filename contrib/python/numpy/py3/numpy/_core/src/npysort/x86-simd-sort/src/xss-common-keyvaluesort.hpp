/*******************************************************************
 * Copyright (C) 2022 Intel Corporation
 * SPDX-License-Identifier: BSD-3-Clause
 * Authors: Liu Zhuan <zhuan.liu@intel.com>
 *          Tang Xi <xi.tang@intel.com>
 * ****************************************************************/

#ifndef AVX512_QSORT_64BIT_KV
#define AVX512_QSORT_64BIT_KV

#include "xss-common-qsort.h"
#include "xss-network-keyvaluesort.hpp"

/*
 * Sort all the NAN's to end of the array and return the index of the last elem
 * in the array which is not a nan
 */
template <typename T1, typename T2, typename vtype>
X86_SIMD_SORT_INLINE arrsize_t move_nans_to_end_of_array(T1 *keys,
                                                         T2 *vals,
                                                         arrsize_t size)
{
    using reg_t = typename vtype::reg_t;

    arrsize_t jj = size - 1;
    arrsize_t ii = 0;
    arrsize_t count = 0;

    while (ii + vtype::numlanes < jj) {
        reg_t in = vtype::loadu(keys + ii);
        auto nanmask = vtype::convert_mask_to_int(
                vtype::template fpclass<0x01 | 0x80>(in));

        // Check if there are any nans in this vector, and process them if so
        if (nanmask != 0x00) {
            for (size_t offset = 0; offset < vtype::numlanes; offset++) {
                if (is_a_nan(keys[ii])) {
                    std::swap(keys[ii], keys[jj]);
                    std::swap(vals[ii], vals[jj]);
                    jj -= 1;
                    count++;
                }
                else {
                    ii += 1;
                }
            }
        }
        else {
            ii += vtype::numlanes;
        }
    }

    // Handle the remainders once we have less than 1 vector worth
    while (ii < jj) {
        if (is_a_nan(keys[ii])) {
            std::swap(keys[ii], keys[jj]);
            std::swap(vals[ii], vals[jj]);
            jj -= 1;
            count++;
        }
        else {
            ii += 1;
        }
    }

    /* Haven't checked for nan when ii == jj */
    if (is_a_nan(keys[ii])) { count++; }
    return size - count - 1;
}

/*
 * Parition one ZMM register based on the pivot and returns the index of the
 * last element that is less than equal to the pivot.
 */
template <typename vtype1,
          typename vtype2,
          typename type_t1 = typename vtype1::type_t,
          typename type_t2 = typename vtype2::type_t,
          typename reg_t1 = typename vtype1::reg_t,
          typename reg_t2 = typename vtype2::reg_t>
X86_SIMD_SORT_INLINE int32_t partition_vec(type_t1 *keys,
                                           type_t2 *indexes,
                                           arrsize_t left,
                                           arrsize_t right,
                                           const reg_t1 keys_vec,
                                           const reg_t2 indexes_vec,
                                           const reg_t1 pivot_vec,
                                           reg_t1 *smallest_vec,
                                           reg_t1 *biggest_vec)
{
    /* which elements are larger than the pivot */
    typename vtype1::opmask_t gt_mask = vtype1::ge(keys_vec, pivot_vec);

    int32_t amount_gt_pivot = vtype1::double_compressstore(
            keys + left, keys + right - vtype1::numlanes, gt_mask, keys_vec);
    vtype2::double_compressstore(indexes + left,
                                 indexes + right - vtype2::numlanes,
                                 resize_mask<vtype1, vtype2>(gt_mask),
                                 indexes_vec);

    *smallest_vec = vtype1::min(keys_vec, *smallest_vec);
    *biggest_vec = vtype1::max(keys_vec, *biggest_vec);
    return amount_gt_pivot;
}
/*
 * Parition an array based on the pivot and returns the index of the
 * last element that is less than equal to the pivot.
 */
template <typename vtype1,
          typename vtype2,
          typename type_t1 = typename vtype1::type_t,
          typename type_t2 = typename vtype2::type_t,
          typename reg_t1 = typename vtype1::reg_t,
          typename reg_t2 = typename vtype2::reg_t>
X86_SIMD_SORT_INLINE arrsize_t kvpartition(type_t1 *keys,
                                           type_t2 *indexes,
                                           arrsize_t left,
                                           arrsize_t right,
                                           type_t1 pivot,
                                           type_t1 *smallest,
                                           type_t1 *biggest)
{
    /* make array length divisible by vtype1::numlanes , shortening the array */
    for (int32_t i = (right - left) % vtype1::numlanes; i > 0; --i) {
        *smallest = std::min(*smallest, keys[left]);
        *biggest = std::max(*biggest, keys[left]);
        if (keys[left] >= pivot) {
            right--;
            std::swap(keys[left], keys[right]);
            std::swap(indexes[left], indexes[right]);
        }
        else {
            ++left;
        }
    }

    if (left == right)
        return left; /* less than vtype1::numlanes elements in the array */

    reg_t1 pivot_vec = vtype1::set1(pivot);
    reg_t1 min_vec = vtype1::set1(*smallest);
    reg_t1 max_vec = vtype1::set1(*biggest);

    if (right - left == vtype1::numlanes) {
        reg_t1 keys_vec = vtype1::loadu(keys + left);
        int32_t amount_gt_pivot;

        reg_t2 indexes_vec = vtype2::loadu(indexes + left);
        amount_gt_pivot = partition_vec<vtype1, vtype2>(keys,
                                                        indexes,
                                                        left,
                                                        left + vtype1::numlanes,
                                                        keys_vec,
                                                        indexes_vec,
                                                        pivot_vec,
                                                        &min_vec,
                                                        &max_vec);

        *smallest = vtype1::reducemin(min_vec);
        *biggest = vtype1::reducemax(max_vec);
        return left + (vtype1::numlanes - amount_gt_pivot);
    }

    // first and last vtype1::numlanes values are partitioned at the end
    reg_t1 keys_vec_left = vtype1::loadu(keys + left);
    reg_t1 keys_vec_right = vtype1::loadu(keys + (right - vtype1::numlanes));
    reg_t2 indexes_vec_left;
    reg_t2 indexes_vec_right;
    indexes_vec_left = vtype2::loadu(indexes + left);
    indexes_vec_right = vtype2::loadu(indexes + (right - vtype1::numlanes));

    // store points of the vectors
    arrsize_t r_store = right - vtype1::numlanes;
    arrsize_t l_store = left;
    // indices for loading the elements
    left += vtype1::numlanes;
    right -= vtype1::numlanes;
    while (right - left != 0) {
        reg_t1 keys_vec;
        reg_t2 indexes_vec;
        /*
         * if fewer elements are stored on the right side of the array,
         * then next elements are loaded from the right side,
         * otherwise from the left side
         */
        if ((r_store + vtype1::numlanes) - right < left - l_store) {
            right -= vtype1::numlanes;
            keys_vec = vtype1::loadu(keys + right);
            indexes_vec = vtype2::loadu(indexes + right);
        }
        else {
            keys_vec = vtype1::loadu(keys + left);
            indexes_vec = vtype2::loadu(indexes + left);
            left += vtype1::numlanes;
        }
        // partition the current vector and save it on both sides of the array
        int32_t amount_gt_pivot;

        amount_gt_pivot
                = partition_vec<vtype1, vtype2>(keys,
                                                indexes,
                                                l_store,
                                                r_store + vtype1::numlanes,
                                                keys_vec,
                                                indexes_vec,
                                                pivot_vec,
                                                &min_vec,
                                                &max_vec);
        r_store -= amount_gt_pivot;
        l_store += (vtype1::numlanes - amount_gt_pivot);
    }

    /* partition and save vec_left and vec_right */
    int32_t amount_gt_pivot;
    amount_gt_pivot = partition_vec<vtype1, vtype2>(keys,
                                                    indexes,
                                                    l_store,
                                                    r_store + vtype1::numlanes,
                                                    keys_vec_left,
                                                    indexes_vec_left,
                                                    pivot_vec,
                                                    &min_vec,
                                                    &max_vec);
    l_store += (vtype1::numlanes - amount_gt_pivot);
    amount_gt_pivot = partition_vec<vtype1, vtype2>(keys,
                                                    indexes,
                                                    l_store,
                                                    l_store + vtype1::numlanes,
                                                    keys_vec_right,
                                                    indexes_vec_right,
                                                    pivot_vec,
                                                    &min_vec,
                                                    &max_vec);
    l_store += (vtype1::numlanes - amount_gt_pivot);
    *smallest = vtype1::reducemin(min_vec);
    *biggest = vtype1::reducemax(max_vec);
    return l_store;
}

template <typename vtype1,
          typename vtype2,
          int num_unroll,
          typename type_t1 = typename vtype1::type_t,
          typename type_t2 = typename vtype2::type_t,
          typename reg_t1 = typename vtype1::reg_t,
          typename reg_t2 = typename vtype2::reg_t>
X86_SIMD_SORT_INLINE arrsize_t kvpartition_unrolled(type_t1 *keys,
                                                    type_t2 *indexes,
                                                    arrsize_t left,
                                                    arrsize_t right,
                                                    type_t1 pivot,
                                                    type_t1 *smallest,
                                                    type_t1 *biggest)
{
    if (right - left <= 8 * num_unroll * vtype1::numlanes) {
        return kvpartition<vtype1, vtype2>(
                keys, indexes, left, right, pivot, smallest, biggest);
    }

    /* make array length divisible by vtype1::numlanes , shortening the array */
    for (int32_t i = ((right - left) % (num_unroll * vtype1::numlanes)); i > 0;
         --i) {
        *smallest = std::min(*smallest, keys[left]);
        *biggest = std::max(*biggest, keys[left]);
        if (keys[left] >= pivot) {
            right--;
            std::swap(keys[left], keys[right]);
            std::swap(indexes[left], indexes[right]);
        }
        else {
            ++left;
        }
    }

    if (left == right) return left;

    reg_t1 pivot_vec = vtype1::set1(pivot);
    reg_t1 min_vec = vtype1::set1(*smallest);
    reg_t1 max_vec = vtype1::set1(*biggest);

    // first and last vtype1::numlanes values are partitioned at the end
    reg_t1 key_left[num_unroll], key_right[num_unroll];
    reg_t2 indx_left[num_unroll], indx_right[num_unroll];
    X86_SIMD_SORT_UNROLL_LOOP(8)
    for (int ii = 0; ii < num_unroll; ++ii) {
        indx_left[ii] = vtype2::loadu(indexes + left + vtype2::numlanes * ii);
        key_left[ii] = vtype1::loadu(keys + left + vtype1::numlanes * ii);
        indx_right[ii] = vtype2::loadu(
                indexes + (right - vtype2::numlanes * (num_unroll - ii)));
        key_right[ii] = vtype1::loadu(
                keys + (right - vtype1::numlanes * (num_unroll - ii)));
    }
    // store points of the vectors
    arrsize_t r_store = right - vtype1::numlanes;
    arrsize_t l_store = left;
    // indices for loading the elements
    left += num_unroll * vtype1::numlanes;
    right -= num_unroll * vtype1::numlanes;
    while (right - left != 0) {
        reg_t2 indx_vec[num_unroll];
        reg_t1 curr_vec[num_unroll];
        /*
         * if fewer elements are stored on the right side of the array,
         * then next elements are loaded from the right side,
         * otherwise from the left side
         */
        if ((r_store + vtype1::numlanes) - right < left - l_store) {
            right -= num_unroll * vtype1::numlanes;
            X86_SIMD_SORT_UNROLL_LOOP(8)
            for (int ii = 0; ii < num_unroll; ++ii) {
                indx_vec[ii] = vtype2::loadu(indexes + right
                                             + ii * vtype2::numlanes);
                curr_vec[ii]
                        = vtype1::loadu(keys + right + ii * vtype1::numlanes);
            }
        }
        else {
            X86_SIMD_SORT_UNROLL_LOOP(8)
            for (int ii = 0; ii < num_unroll; ++ii) {
                indx_vec[ii]
                        = vtype2::loadu(indexes + left + ii * vtype2::numlanes);
                curr_vec[ii]
                        = vtype1::loadu(keys + left + ii * vtype1::numlanes);
            }
            left += num_unroll * vtype1::numlanes;
        }
        // partition the current vector and save it on both sides of the array
        X86_SIMD_SORT_UNROLL_LOOP(8)
        for (int ii = 0; ii < num_unroll; ++ii) {
            int32_t amount_gt_pivot
                    = partition_vec<vtype1, vtype2>(keys,
                                                    indexes,
                                                    l_store,
                                                    r_store + vtype1::numlanes,
                                                    curr_vec[ii],
                                                    indx_vec[ii],
                                                    pivot_vec,
                                                    &min_vec,
                                                    &max_vec);
            l_store += (vtype1::numlanes - amount_gt_pivot);
            r_store -= amount_gt_pivot;
        }
    }

    /* partition and save key_left and key_right */
    X86_SIMD_SORT_UNROLL_LOOP(8)
    for (int ii = 0; ii < num_unroll; ++ii) {
        int32_t amount_gt_pivot
                = partition_vec<vtype1, vtype2>(keys,
                                                indexes,
                                                l_store,
                                                r_store + vtype1::numlanes,
                                                key_left[ii],
                                                indx_left[ii],
                                                pivot_vec,
                                                &min_vec,
                                                &max_vec);
        l_store += (vtype1::numlanes - amount_gt_pivot);
        r_store -= amount_gt_pivot;
    }
    X86_SIMD_SORT_UNROLL_LOOP(8)
    for (int ii = 0; ii < num_unroll; ++ii) {
        int32_t amount_gt_pivot
                = partition_vec<vtype1, vtype2>(keys,
                                                indexes,
                                                l_store,
                                                r_store + vtype1::numlanes,
                                                key_right[ii],
                                                indx_right[ii],
                                                pivot_vec,
                                                &min_vec,
                                                &max_vec);
        l_store += (vtype1::numlanes - amount_gt_pivot);
        r_store -= amount_gt_pivot;
    }
    *smallest = vtype1::reducemin(min_vec);
    *biggest = vtype1::reducemax(max_vec);
    return l_store;
}

template <typename vtype1,
          typename vtype2,
          typename type1_t = typename vtype1::type_t,
          typename type2_t = typename vtype2::type_t>
X86_SIMD_SORT_INLINE void
heapify(type1_t *keys, type2_t *indexes, arrsize_t idx, arrsize_t size)
{
    arrsize_t i = idx;
    while (true) {
        arrsize_t j = 2 * i + 1;
        if (j >= size) { break; }
        arrsize_t k = j + 1;
        if (k < size && keys[j] < keys[k]) { j = k; }
        if (keys[j] < keys[i]) { break; }
        std::swap(keys[i], keys[j]);
        std::swap(indexes[i], indexes[j]);
        i = j;
    }
}
template <typename vtype1,
          typename vtype2,
          typename type1_t = typename vtype1::type_t,
          typename type2_t = typename vtype2::type_t>
X86_SIMD_SORT_INLINE void
heap_sort(type1_t *keys, type2_t *indexes, arrsize_t size)
{
    if (size <= 1) return;
    for (arrsize_t i = size / 2 - 1;; i--) {
        heapify<vtype1, vtype2>(keys, indexes, i, size);
        if (i == 0) { break; }
    }
    for (arrsize_t i = size - 1; i > 0; i--) {
        std::swap(keys[0], keys[i]);
        std::swap(indexes[0], indexes[i]);
        heapify<vtype1, vtype2>(keys, indexes, 0, i);
    }
}

template <typename vtype1,
          typename vtype2,
          typename type1_t = typename vtype1::type_t,
          typename type2_t = typename vtype2::type_t>
X86_SIMD_SORT_INLINE void kvsort_(type1_t *keys,
                                  type2_t *indexes,
                                  arrsize_t left,
                                  arrsize_t right,
                                  int max_iters,
                                  arrsize_t task_threshold)
{
    /*
     * Resort to std::sort if quicksort isnt making any progress
     */
    if (max_iters <= 0) {
        heap_sort<vtype1, vtype2>(
                keys + left, indexes + left, right - left + 1);
        return;
    }
    /*
     * Base case: use bitonic networks to sort arrays <= 128
     */
    if (right + 1 - left <= 128) {
        kvsort_n<vtype1, vtype2, 128>(
                keys + left, indexes + left, (int32_t)(right + 1 - left));
        return;
    }

    // Ascending comparator for this vtype
    using comparator = Comparator<vtype1, false>;
    type1_t pivot;
    auto pivot_result
            = get_pivot_smart<vtype1, comparator, type1_t>(keys, left, right);
    pivot = pivot_result.pivot;

    if (pivot_result.result == pivot_result_t::Sorted) { return; }

    type1_t smallest = vtype1::type_max();
    type1_t biggest = vtype1::type_min();
    arrsize_t pivot_index = kvpartition_unrolled<vtype1, vtype2, 4>(
            keys, indexes, left, right + 1, pivot, &smallest, &biggest);

    if (pivot_result.result == pivot_result_t::Only2Values) { return; }

#ifdef XSS_COMPILE_OPENMP
    if (pivot != smallest) {
        bool parallel_left = (pivot_index - left) > task_threshold;
        if (parallel_left) {
#pragma omp task
            kvsort_<vtype1, vtype2>(keys,
                                    indexes,
                                    left,
                                    pivot_index - 1,
                                    max_iters - 1,
                                    task_threshold);
        }
        else {
            kvsort_<vtype1, vtype2>(keys,
                                    indexes,
                                    left,
                                    pivot_index - 1,
                                    max_iters - 1,
                                    task_threshold);
        }
    }
    if (pivot != biggest) {
        bool parallel_right = (right - pivot_index) > task_threshold;

        if (parallel_right) {
#pragma omp task
            kvsort_<vtype1, vtype2>(keys,
                                    indexes,
                                    pivot_index,
                                    right,
                                    max_iters - 1,
                                    task_threshold);
        }
        else {
            kvsort_<vtype1, vtype2>(keys,
                                    indexes,
                                    pivot_index,
                                    right,
                                    max_iters - 1,
                                    task_threshold);
        }
    }
#else
    UNUSED(task_threshold);

    if (pivot != smallest) {
        kvsort_<vtype1, vtype2>(
                keys, indexes, left, pivot_index - 1, max_iters - 1, 0);
    }
    if (pivot != biggest) {
        kvsort_<vtype1, vtype2>(
                keys, indexes, pivot_index, right, max_iters - 1, 0);
    }
#endif
}

template <typename vtype1,
          typename vtype2,
          typename type1_t = typename vtype1::type_t,
          typename type2_t = typename vtype2::type_t>
X86_SIMD_SORT_INLINE void kvselect_(type1_t *keys,
                                    type2_t *indexes,
                                    arrsize_t pos,
                                    arrsize_t left,
                                    arrsize_t right,
                                    int max_iters)
{
    /*
     * Resort to std::sort if quicksort isnt making any progress
     */
    if (max_iters <= 0) {
        heap_sort<vtype1, vtype2>(
                keys + left, indexes + left, right - left + 1);
        return;
    }
    /*
     * Base case: use bitonic networks to sort arrays <= 128
     */
    if (right + 1 - left <= 128) {

        kvsort_n<vtype1, vtype2, 128>(
                keys + left, indexes + left, (int32_t)(right + 1 - left));
        return;
    }

    type1_t pivot = get_pivot_blocks<vtype1>(keys, left, right);
    type1_t smallest = vtype1::type_max();
    type1_t biggest = vtype1::type_min();
    arrsize_t pivot_index = kvpartition_unrolled<vtype1, vtype2, 4>(
            keys, indexes, left, right + 1, pivot, &smallest, &biggest);

    if ((pivot != smallest) && (pos < pivot_index)) {
        kvselect_<vtype1, vtype2>(
                keys, indexes, pos, left, pivot_index - 1, max_iters - 1);
    }
    else if ((pivot != biggest) && (pos >= pivot_index)) {
        kvselect_<vtype1, vtype2>(
                keys, indexes, pos, pivot_index, right, max_iters - 1);
    }
}

template <typename T1,
          typename T2,
          template <typename...>
          typename full_vector,
          template <typename...>
          typename half_vector>
X86_SIMD_SORT_INLINE void xss_qsort_kv(
        T1 *keys, T2 *indexes, arrsize_t arrsize, bool hasnan, bool descending)
{
    using keytype =
            typename std::conditional<sizeof(T1) != sizeof(T2)
                                              && sizeof(T1) == sizeof(int32_t),
                                      half_vector<T1>,
                                      full_vector<T1>>::type;
    using valtype =
            typename std::conditional<sizeof(T1) != sizeof(T2)
                                              && sizeof(T2) == sizeof(int32_t),
                                      half_vector<T2>,
                                      full_vector<T2>>::type;

    // Exit early if no work would be done
    if (arrsize <= 1) return;

#ifdef XSS_TEST_KEYVALUE_BASE_CASE
    int maxiters = -1;
    bool minarrsize = true;
#else
    int maxiters = 2 * log2(arrsize);
    bool minarrsize = arrsize > 1 ? true : false;
#endif // XSS_TEST_KEYVALUE_BASE_CASE

    if (minarrsize) {
        arrsize_t index_last_elem = arrsize - 1;
        if constexpr (xss::fp::is_floating_point_v<T1>) {
            if (UNLIKELY(hasnan)) {
                index_last_elem
                        = move_nans_to_end_of_array<T1, T2, full_vector<T1>>(
                                keys, indexes, arrsize);
            }
        }
        else {
            UNUSED(hasnan);
        }

#ifdef XSS_COMPILE_OPENMP

        bool use_parallel = arrsize > 10000;

        if (use_parallel) {
            int thread_count = xss_get_num_threads();
            arrsize_t task_threshold
                    = std::max((arrsize_t)10000, arrsize / 100);

            // We use omp parallel and then omp single to setup the threads that will run the omp task calls in kvsort_
            // The omp single prevents multiple threads from running the initial kvsort_ simultaneously and causing problems
            // Note that we do not use the if(...) clause built into OpenMP, because it causes a performance regression for small arrays
#pragma omp parallel num_threads(thread_count)
#pragma omp single
            kvsort_<keytype, valtype>(keys,
                                      indexes,
                                      0,
                                      index_last_elem,
                                      maxiters,
                                      task_threshold);
#pragma omp taskwait
        }
        else {
            kvsort_<keytype, valtype>(keys,
                                      indexes,
                                      0,
                                      index_last_elem,
                                      maxiters,
                                      std::numeric_limits<arrsize_t>::max());
        }
#else
        kvsort_<keytype, valtype>(
                keys, indexes, 0, index_last_elem, maxiters, 0);
#endif

        if (descending) {
            std::reverse(keys, keys + arrsize);
            std::reverse(indexes, indexes + arrsize);
        }
    }

#ifdef __MMX__
    // Workaround for compiler bug generating MMX instructions without emms
    _mm_empty();
#endif
}

template <typename T1,
          typename T2,
          template <typename...>
          typename full_vector,
          template <typename...>
          typename half_vector>
X86_SIMD_SORT_INLINE void xss_select_kv(T1 *keys,
                                        T2 *indexes,
                                        arrsize_t k,
                                        arrsize_t arrsize,
                                        bool hasnan,
                                        bool descending)
{
    using keytype =
            typename std::conditional<sizeof(T1) != sizeof(T2)
                                              && sizeof(T1) == sizeof(int32_t),
                                      half_vector<T1>,
                                      full_vector<T1>>::type;
    using valtype =
            typename std::conditional<sizeof(T1) != sizeof(T2)
                                              && sizeof(T2) == sizeof(int32_t),
                                      half_vector<T2>,
                                      full_vector<T2>>::type;

    // Exit early if no work would be done
    if (arrsize <= 1) return;

#ifdef XSS_TEST_KEYVALUE_BASE_CASE
    int maxiters = -1;
    bool minarrsize = true;
#else
    int maxiters = 2 * log2(arrsize);
    bool minarrsize = arrsize > 1 ? true : false;
#endif // XSS_TEST_KEYVALUE_BASE_CASE

    if (minarrsize) {
        if (descending) { k = arrsize - 1 - k; }

        arrsize_t index_last_elem = arrsize - 1;
        if constexpr (xss::fp::is_floating_point_v<T1>) {
            if (UNLIKELY(hasnan)) {
                index_last_elem
                        = move_nans_to_end_of_array<T1, T2, full_vector<T1>>(
                                keys, indexes, arrsize);
            }
        }

        UNUSED(hasnan);
        if (index_last_elem >= k) {
            kvselect_<keytype, valtype>(
                    keys, indexes, k, 0, index_last_elem, maxiters);
        }

        if (descending) {
            std::reverse(keys, keys + arrsize);
            std::reverse(indexes, indexes + arrsize);
        }
    }

#ifdef __MMX__
    // Workaround for compiler bug generating MMX instructions without emms
    _mm_empty();
#endif
}

template <typename T1,
          typename T2,
          template <typename...>
          typename full_vector,
          template <typename...>
          typename half_vector>
X86_SIMD_SORT_INLINE void xss_partial_sort_kv(T1 *keys,
                                              T2 *indexes,
                                              arrsize_t k,
                                              arrsize_t arrsize,
                                              bool hasnan,
                                              bool descending)
{
    if (k == 0) return;
    xss_select_kv<T1, T2, full_vector, half_vector>(
            keys, indexes, k - 1, arrsize, hasnan, descending);
    xss_qsort_kv<T1, T2, full_vector, half_vector>(
            keys, indexes, k - 1, hasnan, descending);
}

template <typename T1, typename T2>
X86_SIMD_SORT_INLINE void avx512_qsort_kv(T1 *keys,
                                          T2 *indexes,
                                          arrsize_t arrsize,
                                          bool hasnan = false,
                                          bool descending = false)
{
    xss_qsort_kv<T1, T2, zmm_vector, ymm_vector>(
            keys, indexes, arrsize, hasnan, descending);
}

template <typename T1, typename T2>
X86_SIMD_SORT_INLINE void avx2_qsort_kv(T1 *keys,
                                        T2 *indexes,
                                        arrsize_t arrsize,
                                        bool hasnan = false,
                                        bool descending = false)
{
    xss_qsort_kv<T1, T2, avx2_vector, avx2_half_vector>(
            keys, indexes, arrsize, hasnan, descending);
}

template <typename T1, typename T2>
X86_SIMD_SORT_INLINE void avx512_select_kv(T1 *keys,
                                           T2 *indexes,
                                           arrsize_t k,
                                           arrsize_t arrsize,
                                           bool hasnan = false,
                                           bool descending = false)
{
    xss_select_kv<T1, T2, zmm_vector, ymm_vector>(
            keys, indexes, k, arrsize, hasnan, descending);
}

template <typename T1, typename T2>
X86_SIMD_SORT_INLINE void avx2_select_kv(T1 *keys,
                                         T2 *indexes,
                                         arrsize_t k,
                                         arrsize_t arrsize,
                                         bool hasnan = false,
                                         bool descending = false)
{
    xss_select_kv<T1, T2, avx2_vector, avx2_half_vector>(
            keys, indexes, k, arrsize, hasnan, descending);
}

template <typename T1, typename T2>
X86_SIMD_SORT_INLINE void avx512_partial_sort_kv(T1 *keys,
                                                 T2 *indexes,
                                                 arrsize_t k,
                                                 arrsize_t arrsize,
                                                 bool hasnan = false,
                                                 bool descending = false)
{
    xss_partial_sort_kv<T1, T2, zmm_vector, ymm_vector>(
            keys, indexes, k, arrsize, hasnan, descending);
}

template <typename T1, typename T2>
X86_SIMD_SORT_INLINE void avx2_partial_sort_kv(T1 *keys,
                                               T2 *indexes,
                                               arrsize_t k,
                                               arrsize_t arrsize,
                                               bool hasnan = false,
                                               bool descending = false)
{
    xss_partial_sort_kv<T1, T2, avx2_vector, avx2_half_vector>(
            keys, indexes, k, arrsize, hasnan, descending);
}
#endif // AVX512_QSORT_64BIT_KV
