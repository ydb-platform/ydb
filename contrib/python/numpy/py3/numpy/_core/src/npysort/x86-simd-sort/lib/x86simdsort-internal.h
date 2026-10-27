#ifndef XSS_INTERNAL_METHODS
#define XSS_INTERNAL_METHODS
#include "x86simdsort.h"
#include <stdint.h>
#include <vector>

#define DECLAREALLFUNCS(name) \
    namespace name { \
    template <typename T> \
    XSS_HIDE_SYMBOL void qsort(T *arr, \
                               size_t arrsize, \
                               bool hasnan = false, \
                               bool descending = false); \
    template <typename T1, typename T2> \
    XSS_HIDE_SYMBOL void keyvalue_qsort(T1 *key, \
                                        T2 *val, \
                                        size_t arrsize, \
                                        bool hasnan = false, \
                                        bool descending = false); \
    template <typename T> \
    XSS_HIDE_SYMBOL void qselect(T *arr, \
                                 size_t k, \
                                 size_t arrsize, \
                                 bool hasnan = false, \
                                 bool descending = false); \
    template <typename T1, typename T2> \
    XSS_HIDE_SYMBOL void keyvalue_select(T1 *key, \
                                         T2 *val, \
                                         size_t k, \
                                         size_t arrsize, \
                                         bool hasnan = false, \
                                         bool descending = false); \
    template <typename T> \
    XSS_HIDE_SYMBOL void partial_qsort(T *arr, \
                                       size_t k, \
                                       size_t arrsize, \
                                       bool hasnan = false, \
                                       bool descending = false); \
    template <typename T1, typename T2> \
    XSS_HIDE_SYMBOL void keyvalue_partial_sort(T1 *key, \
                                               T2 *val, \
                                               size_t k, \
                                               size_t arrsize, \
                                               bool hasnan = false, \
                                               bool descending = false); \
    template <typename T> \
    XSS_HIDE_SYMBOL std::vector<size_t> argsort(T *arr, \
                                                size_t arrsize, \
                                                bool hasnan = false, \
                                                bool descending = false); \
    template <typename T> \
    XSS_HIDE_SYMBOL std::vector<size_t> \
    argselect(T *arr, size_t k, size_t arrsize, bool hasnan = false); \
    }

namespace xss {
DECLAREALLFUNCS(avx512)
DECLAREALLFUNCS(avx2)
DECLAREALLFUNCS(scalar)
DECLAREALLFUNCS(fp16_spr)
DECLAREALLFUNCS(fp16_icl)
} // namespace xss
#endif
