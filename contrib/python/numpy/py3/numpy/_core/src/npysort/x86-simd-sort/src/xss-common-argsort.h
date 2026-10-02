/*******************************************************************
 * Copyright (C) 2022 Intel Corporation
 * SPDX-License-Identifier: BSD-3-Clause
 * Authors: Raghuveer Devulapalli <raghuveer.devulapalli@intel.com>
 * ****************************************************************/

#ifndef XSS_COMMON_ARGSORT
#define XSS_COMMON_ARGSORT

#include "xss-network-keyvaluesort.hpp"
#include <numeric>

template <typename T>
X86_SIMD_SORT_INLINE void std_argselect_withnan(
        T *arr, arrsize_t *arg, arrsize_t k, arrsize_t left, arrsize_t right)
{
    std::nth_element(arg + left,
                     arg + k,
                     arg + right,
                     [arr](arrsize_t a, arrsize_t b) -> bool {
                         if ((!std::isnan(arr[a])) && (!std::isnan(arr[b]))) {
                             return arr[a] < arr[b];
                         }
                         else if (std::isnan(arr[a])) {
                             return false;
                         }
                         else {
                             return true;
                         }
                     });
}

/* argsort using std::sort */
template <typename T>
X86_SIMD_SORT_INLINE void
std_argsort_withnan(T *arr, arrsize_t *arg, arrsize_t left, arrsize_t right)
{
    std::sort(arg + left,
              arg + right,
              [arr](arrsize_t left, arrsize_t right) -> bool {
                  if ((!std::isnan(arr[left])) && (!std::isnan(arr[right]))) {
                      return arr[left] < arr[right];
                  }
                  else if (std::isnan(arr[left])) {
                      return false;
                  }
                  else {
                      return true;
                  }
              });
}

/* argsort using std::sort */
template <typename T>
X86_SIMD_SORT_INLINE void
std_argsort(T *arr, arrsize_t *arg, arrsize_t left, arrsize_t right)
{
    std::sort(arg + left,
              arg + right,
              [arr](arrsize_t left, arrsize_t right) -> bool {
                  // sort indices according to corresponding array element
                  return arr[left] < arr[right];
              });
}

/*
 * Parition one ZMM register based on the pivot and returns the index of the
 * last element that is less than equal to the pivot.
 */
template <typename vtype,
          typename argtype,
          typename type_t,
          typename reg_t,
          typename argreg_t>
X86_SIMD_SORT_INLINE int32_t partition_vec_avx512(type_t *arg,
                                                  arrsize_t left,
                                                  arrsize_t right,
                                                  const argreg_t arg_vec,
                                                  const reg_t curr_vec,
                                                  const reg_t pivot_vec,
                                                  reg_t *smallest_vec,
                                                  reg_t *biggest_vec)
{
    /* which elements are larger than the pivot */
    typename vtype::opmask_t gt_mask = vtype::ge(curr_vec, pivot_vec);
    int32_t amount_gt_pivot = _mm_popcnt_u32((int32_t)gt_mask);
    argtype::mask_compressstoreu(
            arg + left, vtype::knot_opmask(gt_mask), arg_vec);
    argtype::mask_compressstoreu(
            arg + right - amount_gt_pivot, gt_mask, arg_vec);
    *smallest_vec = vtype::min(curr_vec, *smallest_vec);
    *biggest_vec = vtype::max(curr_vec, *biggest_vec);
    return amount_gt_pivot;
}

/*
 * Parition one AVX2 register based on the pivot and returns the index of the
 * last element that is less than equal to the pivot.
 */
template <typename vtype,
          typename argtype,
          typename type_t,
          typename reg_t,
          typename argreg_t>
X86_SIMD_SORT_INLINE int32_t partition_vec_avx2(type_t *arg,
                                                arrsize_t left,
                                                arrsize_t right,
                                                const argreg_t arg_vec,
                                                const reg_t curr_vec,
                                                const reg_t pivot_vec,
                                                reg_t *smallest_vec,
                                                reg_t *biggest_vec)
{
    /* which elements are larger than the pivot */
    typename vtype::opmask_t ge_mask_vtype = vtype::ge(curr_vec, pivot_vec);
    typename argtype::opmask_t ge_mask
            = resize_mask<vtype, argtype>(ge_mask_vtype);

    auto l_store = arg + left;
    auto r_store = arg + right - vtype::numlanes;

    int amount_ge_pivot
            = argtype::double_compressstore(l_store, r_store, ge_mask, arg_vec);

    *smallest_vec = vtype::min(curr_vec, *smallest_vec);
    *biggest_vec = vtype::max(curr_vec, *biggest_vec);

    return amount_ge_pivot;
}

template <typename vtype,
          typename argtype,
          typename type_t,
          typename reg_t,
          typename argreg_t>
X86_SIMD_SORT_INLINE int32_t partition_vec(type_t *arg,
                                           arrsize_t left,
                                           arrsize_t right,
                                           const argreg_t arg_vec,
                                           const reg_t curr_vec,
                                           const reg_t pivot_vec,
                                           reg_t *smallest_vec,
                                           reg_t *biggest_vec)
{
    if constexpr (vtype::vec_type == simd_type::AVX512) {
        return partition_vec_avx512<vtype, argtype, type_t>(arg,
                                                            left,
                                                            right,
                                                            arg_vec,
                                                            curr_vec,
                                                            pivot_vec,
                                                            smallest_vec,
                                                            biggest_vec);
    }
    else if constexpr (vtype::vec_type == simd_type::AVX2) {
        return partition_vec_avx2<vtype, argtype, type_t>(arg,
                                                          left,
                                                          right,
                                                          arg_vec,
                                                          curr_vec,
                                                          pivot_vec,
                                                          smallest_vec,
                                                          biggest_vec);
    }
    else {
        static_assert(sizeof(argreg_t) == 0, "Should not get here");
    }
}

/*
 * Parition an array based on the pivot and returns the index of the
 * last element that is less than equal to the pivot.
 */
template <typename vtype, typename argtype, typename type_t>
X86_SIMD_SORT_INLINE arrsize_t argpartition(type_t *arr,
                                            arrsize_t *arg,
                                            arrsize_t left,
                                            arrsize_t right,
                                            type_t pivot,
                                            type_t *smallest,
                                            type_t *biggest)
{
    /* make array length divisible by vtype::numlanes , shortening the array */
    for (int32_t i = (right - left) % vtype::numlanes; i > 0; --i) {
        *smallest = std::min(*smallest, arr[arg[left]], comparison_func<vtype>);
        *biggest = std::max(*biggest, arr[arg[left]], comparison_func<vtype>);
        if (!comparison_func<vtype>(arr[arg[left]], pivot)) {
            std::swap(arg[left], arg[--right]);
        }
        else {
            ++left;
        }
    }

    if (left == right)
        return left; /* less than vtype::numlanes elements in the array */

    using reg_t = typename vtype::reg_t;
    using argreg_t = typename argtype::reg_t;
    reg_t pivot_vec = vtype::set1(pivot);
    reg_t min_vec = vtype::set1(*smallest);
    reg_t max_vec = vtype::set1(*biggest);

    if (right - left == vtype::numlanes) {
        argreg_t argvec = argtype::loadu(arg + left);
        reg_t vec = vtype::i64gather(arr, arg + left);
        int32_t amount_gt_pivot
                = partition_vec<vtype, argtype>(arg,
                                                left,
                                                left + vtype::numlanes,
                                                argvec,
                                                vec,
                                                pivot_vec,
                                                &min_vec,
                                                &max_vec);
        *smallest = vtype::reducemin(min_vec);
        *biggest = vtype::reducemax(max_vec);
        return left + (vtype::numlanes - amount_gt_pivot);
    }

    // first and last vtype::numlanes values are partitioned at the end
    argreg_t argvec_left = argtype::loadu(arg + left);
    reg_t vec_left = vtype::i64gather(arr, arg + left);
    argreg_t argvec_right = argtype::loadu(arg + (right - vtype::numlanes));
    reg_t vec_right = vtype::i64gather(arr, arg + (right - vtype::numlanes));
    // store points of the vectors
    arrsize_t r_store = right - vtype::numlanes;
    arrsize_t l_store = left;
    // indices for loading the elements
    left += vtype::numlanes;
    right -= vtype::numlanes;
    while (right - left != 0) {
        argreg_t arg_vec;
        reg_t curr_vec;
        /*
         * if fewer elements are stored on the right side of the array,
         * then next elements are loaded from the right side,
         * otherwise from the left side
         */
        if ((r_store + vtype::numlanes) - right < left - l_store) {
            right -= vtype::numlanes;
            arg_vec = argtype::loadu(arg + right);
            curr_vec = vtype::i64gather(arr, arg + right);
        }
        else {
            arg_vec = argtype::loadu(arg + left);
            curr_vec = vtype::i64gather(arr, arg + left);
            left += vtype::numlanes;
        }
        // partition the current vector and save it on both sides of the array
        int32_t amount_gt_pivot
                = partition_vec<vtype, argtype>(arg,
                                                l_store,
                                                r_store + vtype::numlanes,
                                                arg_vec,
                                                curr_vec,
                                                pivot_vec,
                                                &min_vec,
                                                &max_vec);
        ;
        r_store -= amount_gt_pivot;
        l_store += (vtype::numlanes - amount_gt_pivot);
    }

    /* partition and save vec_left and vec_right */
    int32_t amount_gt_pivot
            = partition_vec<vtype, argtype>(arg,
                                            l_store,
                                            r_store + vtype::numlanes,
                                            argvec_left,
                                            vec_left,
                                            pivot_vec,
                                            &min_vec,
                                            &max_vec);
    l_store += (vtype::numlanes - amount_gt_pivot);
    amount_gt_pivot = partition_vec<vtype, argtype>(arg,
                                                    l_store,
                                                    l_store + vtype::numlanes,
                                                    argvec_right,
                                                    vec_right,
                                                    pivot_vec,
                                                    &min_vec,
                                                    &max_vec);
    l_store += (vtype::numlanes - amount_gt_pivot);
    *smallest = vtype::reducemin(min_vec);
    *biggest = vtype::reducemax(max_vec);
    return l_store;
}

template <typename vtype,
          typename argtype,
          int num_unroll,
          typename type_t = typename vtype::type_t>
X86_SIMD_SORT_INLINE arrsize_t argpartition_unrolled(type_t *arr,
                                                     arrsize_t *arg,
                                                     arrsize_t left,
                                                     arrsize_t right,
                                                     type_t pivot,
                                                     type_t *smallest,
                                                     type_t *biggest)
{
    if (right - left <= 8 * num_unroll * vtype::numlanes) {
        return argpartition<vtype, argtype>(
                arr, arg, left, right, pivot, smallest, biggest);
    }
    /* make array length divisible by vtype::numlanes , shortening the array */
    for (int32_t i = ((right - left) % (num_unroll * vtype::numlanes)); i > 0;
         --i) {
        *smallest = std::min(*smallest, arr[arg[left]], comparison_func<vtype>);
        *biggest = std::max(*biggest, arr[arg[left]], comparison_func<vtype>);
        if (!comparison_func<vtype>(arr[arg[left]], pivot)) {
            std::swap(arg[left], arg[--right]);
        }
        else {
            ++left;
        }
    }

    if (left == right)
        return left; /* less than vtype::numlanes elements in the array */

    using reg_t = typename vtype::reg_t;
    using argreg_t = typename argtype::reg_t;
    reg_t pivot_vec = vtype::set1(pivot);
    reg_t min_vec = vtype::set1(*smallest);
    reg_t max_vec = vtype::set1(*biggest);

    // first and last vtype::numlanes values are partitioned at the end
    reg_t vec_left[num_unroll], vec_right[num_unroll];
    argreg_t argvec_left[num_unroll], argvec_right[num_unroll];
    X86_SIMD_SORT_UNROLL_LOOP(8)
    for (int ii = 0; ii < num_unroll; ++ii) {
        argvec_left[ii] = argtype::loadu(arg + left + vtype::numlanes * ii);
        vec_left[ii] = vtype::i64gather(arr, arg + left + vtype::numlanes * ii);
        argvec_right[ii] = argtype::loadu(
                arg + (right - vtype::numlanes * (num_unroll - ii)));
        vec_right[ii] = vtype::i64gather(
                arr, arg + (right - vtype::numlanes * (num_unroll - ii)));
    }
    // store points of the vectors
    arrsize_t r_store = right - vtype::numlanes;
    arrsize_t l_store = left;
    // indices for loading the elements
    left += num_unroll * vtype::numlanes;
    right -= num_unroll * vtype::numlanes;
    while (right - left != 0) {
        argreg_t arg_vec[num_unroll];
        reg_t curr_vec[num_unroll];
        /*
         * if fewer elements are stored on the right side of the array,
         * then next elements are loaded from the right side,
         * otherwise from the left side
         */
        if ((r_store + vtype::numlanes) - right < left - l_store) {
            right -= num_unroll * vtype::numlanes;
            X86_SIMD_SORT_UNROLL_LOOP(8)
            for (int ii = 0; ii < num_unroll; ++ii) {
                arg_vec[ii]
                        = argtype::loadu(arg + right + ii * vtype::numlanes);
                curr_vec[ii] = vtype::i64gather(
                        arr, arg + right + ii * vtype::numlanes);
            }
        }
        else {
            X86_SIMD_SORT_UNROLL_LOOP(8)
            for (int ii = 0; ii < num_unroll; ++ii) {
                arg_vec[ii] = argtype::loadu(arg + left + ii * vtype::numlanes);
                curr_vec[ii] = vtype::i64gather(
                        arr, arg + left + ii * vtype::numlanes);
            }
            left += num_unroll * vtype::numlanes;
        }
        // partition the current vector and save it on both sides of the array
        X86_SIMD_SORT_UNROLL_LOOP(8)
        for (int ii = 0; ii < num_unroll; ++ii) {
            int32_t amount_gt_pivot
                    = partition_vec<vtype, argtype>(arg,
                                                    l_store,
                                                    r_store + vtype::numlanes,
                                                    arg_vec[ii],
                                                    curr_vec[ii],
                                                    pivot_vec,
                                                    &min_vec,
                                                    &max_vec);
            l_store += (vtype::numlanes - amount_gt_pivot);
            r_store -= amount_gt_pivot;
        }
    }

    /* partition and save vec_left and vec_right */
    X86_SIMD_SORT_UNROLL_LOOP(8)
    for (int ii = 0; ii < num_unroll; ++ii) {
        int32_t amount_gt_pivot
                = partition_vec<vtype, argtype>(arg,
                                                l_store,
                                                r_store + vtype::numlanes,
                                                argvec_left[ii],
                                                vec_left[ii],
                                                pivot_vec,
                                                &min_vec,
                                                &max_vec);
        l_store += (vtype::numlanes - amount_gt_pivot);
        r_store -= amount_gt_pivot;
    }
    X86_SIMD_SORT_UNROLL_LOOP(8)
    for (int ii = 0; ii < num_unroll; ++ii) {
        int32_t amount_gt_pivot
                = partition_vec<vtype, argtype>(arg,
                                                l_store,
                                                r_store + vtype::numlanes,
                                                argvec_right[ii],
                                                vec_right[ii],
                                                pivot_vec,
                                                &min_vec,
                                                &max_vec);
        l_store += (vtype::numlanes - amount_gt_pivot);
        r_store -= amount_gt_pivot;
    }
    *smallest = vtype::reducemin(min_vec);
    *biggest = vtype::reducemax(max_vec);
    return l_store;
}

template <typename vtype, typename type_t>
X86_SIMD_SORT_INLINE type_t get_pivot_64bit(type_t *arr,
                                            arrsize_t *arg,
                                            const arrsize_t left,
                                            const arrsize_t right)
{
    if constexpr (vtype::numlanes == 8) {
        if (right - left >= vtype::numlanes) {
            // median of 8
            arrsize_t size = (right - left) / 8;
            using reg_t = typename vtype::reg_t;
            reg_t rand_vec = vtype::set(arr[arg[left + size]],
                                        arr[arg[left + 2 * size]],
                                        arr[arg[left + 3 * size]],
                                        arr[arg[left + 4 * size]],
                                        arr[arg[left + 5 * size]],
                                        arr[arg[left + 6 * size]],
                                        arr[arg[left + 7 * size]],
                                        arr[arg[left + 8 * size]]);
            // pivot will never be a nan, since there are no nan's!
            reg_t sort = vtype::sort_vec(rand_vec);
            return ((type_t *)&sort)[4];
        }
        else {
            return arr[arg[right]];
        }
    }
    else if constexpr (vtype::numlanes == 4) {
        if (right - left >= vtype::numlanes) {
            // median of 4
            arrsize_t size = (right - left) / 4;
            using reg_t = typename vtype::reg_t;
            reg_t rand_vec = vtype::set(arr[arg[left + size]],
                                        arr[arg[left + 2 * size]],
                                        arr[arg[left + 3 * size]],
                                        arr[arg[left + 4 * size]]);
            // pivot will never be a nan, since there are no nan's!
            reg_t sort = vtype::sort_vec(rand_vec);
            return ((type_t *)&sort)[2];
        }
        else {
            return arr[arg[right]];
        }
    }
}

template <typename vtype, typename argtype, typename type_t>
X86_SIMD_SORT_INLINE void argsort_(type_t *arr,
                                   arrsize_t *arg,
                                   arrsize_t left,
                                   arrsize_t right,
                                   arrsize_t max_iters,
                                   arrsize_t task_threshold)
{
    /*
     * Resort to std::sort if quicksort isnt making any progress
     */
    if (max_iters <= 0) {
        std_argsort(arr, arg, left, right + 1);
        return;
    }
    /*
     * Base case: use bitonic networks to sort arrays <= 64
     */
    if (right + 1 - left <= 256) {
        argsort_n<vtype, argtype, 256>(
                arr, arg + left, (int32_t)(right + 1 - left));
        return;
    }
    type_t pivot = get_pivot_64bit<vtype>(arr, arg, left, right);
    type_t smallest = vtype::type_max();
    type_t biggest = vtype::type_min();
    arrsize_t pivot_index = argpartition_unrolled<vtype, argtype, 4>(
            arr, arg, left, right + 1, pivot, &smallest, &biggest);
#ifdef XSS_COMPILE_OPENMP
    if (pivot != smallest) {
        bool parallel_left = (pivot_index - left) > task_threshold;
        if (parallel_left) {
#pragma omp task
            argsort_<vtype, argtype>(arr,
                                     arg,
                                     left,
                                     pivot_index - 1,
                                     max_iters - 1,
                                     task_threshold);
        }
        else {
            argsort_<vtype, argtype>(arr,
                                     arg,
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
            argsort_<vtype, argtype>(arr,
                                     arg,
                                     pivot_index,
                                     right,
                                     max_iters - 1,
                                     task_threshold);
        }
        else {
            argsort_<vtype, argtype>(arr,
                                     arg,
                                     pivot_index,
                                     right,
                                     max_iters - 1,
                                     task_threshold);
        }
    }
#else
    UNUSED(task_threshold);
    if (pivot != smallest)
        argsort_<vtype, argtype>(
                arr, arg, left, pivot_index - 1, max_iters - 1, 0);
    if (pivot != biggest)
        argsort_<vtype, argtype>(
                arr, arg, pivot_index, right, max_iters - 1, 0);
#endif
}

template <typename vtype, typename argtype, typename type_t>
X86_SIMD_SORT_INLINE void argselect_(type_t *arr,
                                     arrsize_t *arg,
                                     arrsize_t pos,
                                     arrsize_t left,
                                     arrsize_t right,
                                     arrsize_t max_iters)
{
    /*
     * Resort to std::sort if quicksort isnt making any progress
     */
    if (max_iters <= 0) {
        std_argsort(arr, arg, left, right + 1);
        return;
    }
    /*
     * Base case: use bitonic networks to sort arrays <= 64
     */
    if (right + 1 - left <= 256) {
        argsort_n<vtype, argtype, 256>(
                arr, arg + left, (int32_t)(right + 1 - left));
        return;
    }
    type_t pivot = get_pivot_64bit<vtype>(arr, arg, left, right);
    type_t smallest = vtype::type_max();
    type_t biggest = vtype::type_min();
    arrsize_t pivot_index = argpartition_unrolled<vtype, argtype, 4>(
            arr, arg, left, right + 1, pivot, &smallest, &biggest);
    if ((pivot != smallest) && (pos < pivot_index))
        argselect_<vtype, argtype>(
                arr, arg, pos, left, pivot_index - 1, max_iters - 1);
    else if ((pivot != biggest) && (pos >= pivot_index))
        argselect_<vtype, argtype>(
                arr, arg, pos, pivot_index, right, max_iters - 1);
}

/* argsort methods for 32-bit and 64-bit dtypes */
template <typename T,
          template <typename...>
          typename full_vector,
          template <typename...>
          typename half_vector>
X86_SIMD_SORT_INLINE void xss_argsort(T *arr,
                                      arrsize_t *arg,
                                      arrsize_t arrsize,
                                      bool hasnan = false,
                                      bool descending = false)
{

    using vectype = typename std::conditional<sizeof(T) == sizeof(int32_t),
                                              half_vector<T>,
                                              full_vector<T>>::type;

    using argtype =
            typename std::conditional<sizeof(arrsize_t) == sizeof(int32_t),
                                      half_vector<arrsize_t>,
                                      full_vector<arrsize_t>>::type;

    if (arrsize > 1) {
        /* simdargsort does not work for float/double arrays with nan */
        if constexpr (xss::fp::is_floating_point_v<T>) {
            if ((hasnan) && (array_has_nan<vectype>(arr, arrsize))) {
                std_argsort_withnan(arr, arg, 0, arrsize);

                if (descending) { std::reverse(arg, arg + arrsize); }

                return;
            }
        }
        UNUSED(hasnan);

        /* early exit for already sorted arrays: float/double with nan never reach here*/
        auto comp = descending ? Comparator<vectype, true>::STDSortComparator
                               : Comparator<vectype, false>::STDSortComparator;
        if (std::is_sorted(arr, arr + arrsize, comp)) { return; }

#ifdef XSS_COMPILE_OPENMP

        bool use_parallel = arrsize > 10000;

        if (use_parallel) {
            int thread_count = xss_get_num_threads();
            arrsize_t task_threshold
                    = std::max((arrsize_t)10000, arrsize / 100);

            // We use omp parallel and then omp single to setup the threads that will run the omp task calls in qsort_
            // The omp single prevents multiple threads from running the initial qsort_ simultaneously and causing problems
            // Note that we do not use the if(...) clause built into OpenMP, because it causes a performance regression for small arrays
#pragma omp parallel num_threads(thread_count)
#pragma omp single
            argsort_<vectype, argtype>(arr,
                                       arg,
                                       0,
                                       arrsize - 1,
                                       2 * (arrsize_t)(63 - __builtin_clzll((unsigned long long)arrsize)),
                                       task_threshold);
#pragma omp taskwait
        }
        else {
            argsort_<vectype, argtype>(arr,
                                       arg,
                                       0,
                                       arrsize - 1,
                                       2 * (arrsize_t)(63 - __builtin_clzll((unsigned long long)arrsize)),
                                       std::numeric_limits<arrsize_t>::max());
        }
#else
        argsort_<vectype, argtype>(
                arr, arg, 0, arrsize - 1, 2 * (arrsize_t)(63 - __builtin_clzll((unsigned long long)arrsize)), 0);
#endif

        if (descending) { std::reverse(arg, arg + arrsize); }
    }

#ifdef __MMX__
    // Workaround for compiler bug generating MMX instructions without emms
    _mm_empty();
#endif
}

template <typename T>
X86_SIMD_SORT_INLINE void avx512_argsort(T *arr,
                                         arrsize_t *arg,
                                         arrsize_t arrsize,
                                         bool hasnan = false,
                                         bool descending = false)
{
    xss_argsort<T, zmm_vector, ymm_vector>(
            arr, arg, arrsize, hasnan, descending);
}

template <typename T>
X86_SIMD_SORT_INLINE void avx2_argsort(T *arr,
                                       arrsize_t *arg,
                                       arrsize_t arrsize,
                                       bool hasnan = false,
                                       bool descending = false)
{
    xss_argsort<T, avx2_vector, avx2_half_vector>(
            arr, arg, arrsize, hasnan, descending);
}

/* argselect methods for 32-bit and 64-bit dtypes */
template <typename T,
          template <typename...>
          typename full_vector,
          template <typename...>
          typename half_vector>
X86_SIMD_SORT_INLINE void xss_argselect(T *arr,
                                        arrsize_t *arg,
                                        arrsize_t k,
                                        arrsize_t arrsize,
                                        bool hasnan = false)
{
    /* TODO optimization: on 32-bit, use full_vector for 32-bit dtype */
    using vectype = typename std::conditional<sizeof(T) == sizeof(int32_t),
                                              half_vector<T>,
                                              full_vector<T>>::type;

    using argtype =
            typename std::conditional<sizeof(arrsize_t) == sizeof(int32_t),
                                      half_vector<arrsize_t>,
                                      full_vector<arrsize_t>>::type;

    if (arrsize > 1) {
        if constexpr (xss::fp::is_floating_point_v<T>) {
            if ((hasnan) && (array_has_nan<vectype>(arr, arrsize))) {
                std_argselect_withnan(arr, arg, k, 0, arrsize);
                return;
            }
        }
        UNUSED(hasnan);
        argselect_<vectype, argtype>(
                arr, arg, k, 0, arrsize - 1, 2 * (arrsize_t)(63 - __builtin_clzll((unsigned long long)arrsize)));
    }

#ifdef __MMX__
    // Workaround for compiler bug generating MMX instructions without emms
    _mm_empty();
#endif
}

template <typename T>
X86_SIMD_SORT_INLINE void avx512_argselect(T *arr,
                                           arrsize_t *arg,
                                           arrsize_t k,
                                           arrsize_t arrsize,
                                           bool hasnan = false)
{
    xss_argselect<T, zmm_vector, ymm_vector>(arr, arg, k, arrsize, hasnan);
}

template <typename T>
X86_SIMD_SORT_INLINE void avx2_argselect(T *arr,
                                         arrsize_t *arg,
                                         arrsize_t k,
                                         arrsize_t arrsize,
                                         bool hasnan = false)
{
    xss_argselect<T, avx2_vector, avx2_half_vector>(
            arr, arg, k, arrsize, hasnan);
}

#endif // XSS_COMMON_ARGSORT
