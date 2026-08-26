#ifndef XSS_INTERNAL_METHODS
#define XSS_INTERNAL_METHODS
#include "x86simdsort.h"
#include <stdint.h>
#include <vector>

namespace xss {
namespace avx512 {
    // quicksort
    template <typename T>
    XSS_HIDE_SYMBOL void
    qsort(T *arr, size_t arrsize, bool hasnan = false, bool descending = false);
    // key-value quicksort
    template <typename T1, typename T2>
    XSS_HIDE_SYMBOL void keyvalue_qsort(T1 *key,
                                        T2 *val,
                                        size_t arrsize,
                                        bool hasnan = false,
                                        bool descending = false);
    // quickselect
    template <typename T>
    XSS_HIDE_SYMBOL void qselect(T *arr,
                                 size_t k,
                                 size_t arrsize,
                                 bool hasnan = false,
                                 bool descending = false);
    // key-value select
    template <typename T1, typename T2>
    XSS_HIDE_SYMBOL void keyvalue_select(T1 *key,
                                         T2 *val,
                                         size_t k,
                                         size_t arrsize,
                                         bool hasnan = false,
                                         bool descending = false);
    // partial sort
    template <typename T>
    XSS_HIDE_SYMBOL void partial_qsort(T *arr,
                                       size_t k,
                                       size_t arrsize,
                                       bool hasnan = false,
                                       bool descending = false);
    // key-value partial sort
    template <typename T1, typename T2>
    XSS_HIDE_SYMBOL void keyvalue_partial_sort(T1 *key,
                                               T2 *val,
                                               size_t k,
                                               size_t arrsize,
                                               bool hasnan = false,
                                               bool descending = false);
    // argsort
    template <typename T>
    XSS_HIDE_SYMBOL std::vector<size_t> argsort(T *arr,
                                                size_t arrsize,
                                                bool hasnan = false,
                                                bool descending = false);
    // argselect
    template <typename T>
    XSS_HIDE_SYMBOL std::vector<size_t>
    argselect(T *arr, size_t k, size_t arrsize, bool hasnan = false);
} // namespace avx512
namespace avx2 {
    // quicksort
    template <typename T>
    XSS_HIDE_SYMBOL void
    qsort(T *arr, size_t arrsize, bool hasnan = false, bool descending = false);
    // key-value quicksort
    template <typename T1, typename T2>
    XSS_HIDE_SYMBOL void keyvalue_qsort(T1 *key,
                                        T2 *val,
                                        size_t arrsize,
                                        bool hasnan = false,
                                        bool descending = false);
    // quickselect
    template <typename T>
    XSS_HIDE_SYMBOL void qselect(T *arr,
                                 size_t k,
                                 size_t arrsize,
                                 bool hasnan = false,
                                 bool descending = false);
    // key-value select
    template <typename T1, typename T2>
    XSS_HIDE_SYMBOL void keyvalue_select(T1 *key,
                                         T2 *val,
                                         size_t k,
                                         size_t arrsize,
                                         bool hasnan = false,
                                         bool descending = false);
    // partial sort
    template <typename T>
    XSS_HIDE_SYMBOL void partial_qsort(T *arr,
                                       size_t k,
                                       size_t arrsize,
                                       bool hasnan = false,
                                       bool descending = false);
    // key-value partial sort
    template <typename T1, typename T2>
    XSS_HIDE_SYMBOL void keyvalue_partial_sort(T1 *key,
                                               T2 *val,
                                               size_t k,
                                               size_t arrsize,
                                               bool hasnan = false,
                                               bool descending = false);
    // argsort
    template <typename T>
    XSS_HIDE_SYMBOL std::vector<size_t> argsort(T *arr,
                                                size_t arrsize,
                                                bool hasnan = false,
                                                bool descending = false);
    // argselect
    template <typename T>
    XSS_HIDE_SYMBOL std::vector<size_t>
    argselect(T *arr, size_t k, size_t arrsize, bool hasnan = false);
} // namespace avx2
namespace scalar {
    // quicksort
    template <typename T>
    XSS_HIDE_SYMBOL void
    qsort(T *arr, size_t arrsize, bool hasnan = false, bool descending = false);
    // key-value quicksort
    template <typename T1, typename T2>
    XSS_HIDE_SYMBOL void keyvalue_qsort(T1 *key,
                                        T2 *val,
                                        size_t arrsize,
                                        bool hasnan = false,
                                        bool descending = false);
    // quickselect
    template <typename T>
    XSS_HIDE_SYMBOL void qselect(T *arr,
                                 size_t k,
                                 size_t arrsize,
                                 bool hasnan = false,
                                 bool descending = false);
    // key-value select
    template <typename T1, typename T2>
    XSS_HIDE_SYMBOL void keyvalue_select(T1 *key,
                                         T2 *val,
                                         size_t k,
                                         size_t arrsize,
                                         bool hasnan = false,
                                         bool descending = false);
    // partial sort
    template <typename T>
    XSS_HIDE_SYMBOL void partial_qsort(T *arr,
                                       size_t k,
                                       size_t arrsize,
                                       bool hasnan = false,
                                       bool descending = false);
    // key-value partial sort
    template <typename T1, typename T2>
    XSS_HIDE_SYMBOL void keyvalue_partial_sort(T1 *key,
                                               T2 *val,
                                               size_t k,
                                               size_t arrsize,
                                               bool hasnan = false,
                                               bool descending = false);
    // argsort
    template <typename T>
    XSS_HIDE_SYMBOL std::vector<size_t> argsort(T *arr,
                                                size_t arrsize,
                                                bool hasnan = false,
                                                bool descending = false);
    // argselect
    template <typename T>
    XSS_HIDE_SYMBOL std::vector<size_t>
    argselect(T *arr, size_t k, size_t arrsize, bool hasnan = false);
} // namespace scalar
} // namespace xss
#endif
