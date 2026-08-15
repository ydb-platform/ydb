#ifndef X86_SIMD_SORT_STATIC_METHODS
#define X86_SIMD_SORT_STATIC_METHODS
#include <vector>
#include <stdlib.h>
#include "xss-common-includes.h"

// Supported methods declared here for a quick reference:
namespace x86simdsortStatic {
template <typename T>
X86_SIMD_SORT_FINLINE void
qsort(T *arr, size_t size, bool hasnan = false, bool descending = false);

template <typename T>
X86_SIMD_SORT_FINLINE void qselect(T *arr,
                                   size_t k,
                                   size_t size,
                                   bool hasnan = false,
                                   bool descending = false);

template <typename T>
X86_SIMD_SORT_FINLINE void partial_qsort(T *arr,
                                         size_t k,
                                         size_t size,
                                         bool hasnan = false,
                                         bool descending = false);

template <typename T>
X86_SIMD_SORT_FINLINE std::vector<size_t>
argsort(T *arr, size_t size, bool hasnan = false, bool descending = false);

/* argsort API required by NumPy: */
template <typename T>
X86_SIMD_SORT_FINLINE void argsort(T *arr,
                                   size_t *arg,
                                   size_t size,
                                   bool hasnan = false,
                                   bool descending = false);

template <typename T>
X86_SIMD_SORT_FINLINE std::vector<size_t>
argselect(T *arr, size_t k, size_t size, bool hasnan = false);

/* argselect API required by NumPy: */
template <typename T>
void X86_SIMD_SORT_FINLINE
argselect(T *arr, size_t *arg, size_t k, size_t size, bool hasnan = false);

template <typename T1, typename T2>
X86_SIMD_SORT_FINLINE void keyvalue_qsort(T1 *key,
                                          T2 *val,
                                          size_t size,
                                          bool hasnan = false,
                                          bool descending = false);

template <typename T1, typename T2>
X86_SIMD_SORT_FINLINE void keyvalue_select(T1 *key,
                                           T2 *val,
                                           size_t k,
                                           size_t size,
                                           bool hasnan = false,
                                           bool descending = false);

template <typename T1, typename T2>
X86_SIMD_SORT_FINLINE void keyvalue_partial_sort(T1 *key,
                                                 T2 *val,
                                                 size_t k,
                                                 size_t size,
                                                 bool hasnan = false,
                                                 bool descending = false);

} // namespace x86simdsortStatic

#define XSS_METHODS(ISA) \
    template <typename T> \
    X86_SIMD_SORT_FINLINE void x86simdsortStatic::qsort( \
            T *arr, size_t size, bool hasnan, bool descending) \
    { \
        ISA##_qsort(arr, size, hasnan, descending); \
    } \
    template <typename T> \
    X86_SIMD_SORT_FINLINE void x86simdsortStatic::qselect( \
            T *arr, size_t k, size_t size, bool hasnan, bool descending) \
    { \
        ISA##_qselect(arr, k, size, hasnan, descending); \
    } \
    template <typename T> \
    X86_SIMD_SORT_FINLINE void x86simdsortStatic::partial_qsort( \
            T *arr, size_t k, size_t size, bool hasnan, bool descending) \
    { \
        ISA##_partial_qsort(arr, k, size, hasnan, descending); \
    } \
    template <typename T> \
    X86_SIMD_SORT_FINLINE void x86simdsortStatic::argsort( \
            T *arr, size_t *arg, size_t size, bool hasnan, bool descending) \
    { \
        ISA##_argsort(arr, arg, size, hasnan, descending); \
    } \
    template <typename T> \
    X86_SIMD_SORT_FINLINE std::vector<size_t> x86simdsortStatic::argsort( \
            T *arr, size_t size, bool hasnan, bool descending) \
    { \
        std::vector<size_t> indices(size); \
        std::iota(indices.begin(), indices.end(), 0); \
        x86simdsortStatic::argsort( \
                arr, indices.data(), size, hasnan, descending); \
        return indices; \
    } \
    template <typename T> \
    X86_SIMD_SORT_FINLINE void x86simdsortStatic::argselect( \
            T *arr, size_t *arg, size_t k, size_t size, bool hasnan) \
    { \
        ISA##_argselect(arr, arg, k, size, hasnan); \
    } \
    template <typename T> \
    X86_SIMD_SORT_FINLINE std::vector<size_t> x86simdsortStatic::argselect( \
            T *arr, size_t k, size_t size, bool hasnan) \
    { \
        std::vector<size_t> indices(size); \
        std::iota(indices.begin(), indices.end(), 0); \
        x86simdsortStatic::argselect(arr, indices.data(), k, size, hasnan); \
        return indices; \
    } \
    template <typename T1, typename T2> \
    X86_SIMD_SORT_FINLINE void x86simdsortStatic::keyvalue_qsort( \
            T1 *key, T2 *val, size_t size, bool hasnan, bool descending) \
    { \
        ISA##_qsort_kv(key, val, size, hasnan, descending); \
    } \
    template <typename T1, typename T2> \
    X86_SIMD_SORT_FINLINE void x86simdsortStatic::keyvalue_select( \
            T1 *key, \
            T2 *val, \
            size_t k, \
            size_t size, \
            bool hasnan, \
            bool descending) \
    { \
        ISA##_select_kv(key, val, k, size, hasnan, descending); \
    } \
    template <typename T1, typename T2> \
    X86_SIMD_SORT_FINLINE void x86simdsortStatic::keyvalue_partial_sort( \
            T1 *key, \
            T2 *val, \
            size_t k, \
            size_t size, \
            bool hasnan, \
            bool descending) \
    { \
        ISA##_partial_sort_kv(key, val, k, size, hasnan, descending); \
    }

/*
 * qsort, qselect, partial, argsort key-value sort template functions.
 */
#include "xss-common-qsort.h"
#include "xss-common-argsort.h"
#include "xss-common-keyvaluesort.hpp"

#if defined(__AVX512DQ__) && defined(__AVX512VL__)
/* 32-bit and 64-bit dtypes vector definitions on SKX */
#include "avx512-32bit-qsort.hpp"
#include "avx512-64bit-qsort.hpp"
#include "avx512-64bit-argsort.hpp"

/* 16-bit dtypes vector definitions on ICL */
#if defined(__AVX512BW__) && defined(__AVX512VBMI2__)
#include "avx512-16bit-qsort.hpp"
/* _Float16 vector definition on SPR*/
#if defined(__FLT16_MAX__) && defined(__AVX512BW__) && defined(__AVX512FP16__)
#include "avx512fp16-16bit-qsort.hpp"
#endif // __FLT16_MAX__
#endif // __AVX512VBMI2__

XSS_METHODS(avx512)

#elif defined(__AVX512F__)
#error "x86simdsort requires AVX512DQ and AVX512VL to be enabled in addition to AVX512F to use AVX512"

#elif defined(__AVX2__) && !defined(__AVX512F__)
/* 32-bit and 64-bit dtypes vector definitions on AVX2 */
#include "avx2-32bit-half.hpp"
#include "avx2-32bit-qsort.hpp"
#include "avx2-64bit-qsort.hpp"
XSS_METHODS(avx2)

#else
#error "x86simdsortStatic methods needs to be compiled with avx512/avx2 specific flags"
#endif // (__AVX512VL__ && __AVX512DQ__) || AVX2

#endif // X86_SIMD_SORT_STATIC_METHODS
