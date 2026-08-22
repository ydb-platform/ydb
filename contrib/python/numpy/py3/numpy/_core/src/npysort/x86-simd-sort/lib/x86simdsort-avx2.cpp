// AVX2 specific routines:

#include "x86simdsort-static-incl.h"
#include "x86simdsort-internal.h"

#define DEFINE_ALL_METHODS(type) \
    template <> \
    void qsort(type *arr, size_t arrsize, bool hasnan, bool descending) \
    { \
        x86simdsortStatic::qsort(arr, arrsize, hasnan, descending); \
    } \
    template <> \
    void qselect( \
            type *arr, size_t k, size_t arrsize, bool hasnan, bool descending) \
    { \
        x86simdsortStatic::qselect(arr, k, arrsize, hasnan, descending); \
    } \
    template <> \
    void partial_qsort( \
            type *arr, size_t k, size_t arrsize, bool hasnan, bool descending) \
    { \
        x86simdsortStatic::partial_qsort(arr, k, arrsize, hasnan, descending); \
    } \
    template <> \
    std::vector<size_t> argsort( \
            type *arr, size_t arrsize, bool hasnan, bool descending) \
    { \
        return x86simdsortStatic::argsort(arr, arrsize, hasnan, descending); \
    } \
    template <> \
    std::vector<size_t> argselect( \
            type *arr, size_t k, size_t arrsize, bool hasnan) \
    { \
        return x86simdsortStatic::argselect(arr, k, arrsize, hasnan); \
    }

#define DEFINE_KEYVALUE_METHODS_BASE(type1, type2) \
    template <> \
    void keyvalue_qsort(type1 *key, \
                        type2 *val, \
                        size_t arrsize, \
                        bool hasnan, \
                        bool descending) \
    { \
        x86simdsortStatic::keyvalue_qsort( \
                key, val, arrsize, hasnan, descending); \
    } \
    template <> \
    void keyvalue_select(type1 *key, \
                         type2 *val, \
                         size_t k, \
                         size_t arrsize, \
                         bool hasnan, \
                         bool descending) \
    { \
        x86simdsortStatic::keyvalue_select( \
                key, val, k, arrsize, hasnan, descending); \
    } \
    template <> \
    void keyvalue_partial_sort(type1 *key, \
                               type2 *val, \
                               size_t k, \
                               size_t arrsize, \
                               bool hasnan, \
                               bool descending) \
    { \
        x86simdsortStatic::keyvalue_partial_sort( \
                key, val, k, arrsize, hasnan, descending); \
    }

#define DEFINE_KEYVALUE_METHODS(type) \
    DEFINE_KEYVALUE_METHODS_BASE(type, uint64_t) \
    DEFINE_KEYVALUE_METHODS_BASE(type, int64_t) \
    DEFINE_KEYVALUE_METHODS_BASE(type, double) \
    DEFINE_KEYVALUE_METHODS_BASE(type, uint32_t) \
    DEFINE_KEYVALUE_METHODS_BASE(type, int32_t) \
    DEFINE_KEYVALUE_METHODS_BASE(type, float)

namespace xss {
namespace avx2 {
    DEFINE_ALL_METHODS(uint32_t)
    DEFINE_ALL_METHODS(int32_t)
    DEFINE_ALL_METHODS(float)
    DEFINE_ALL_METHODS(uint64_t)
    DEFINE_ALL_METHODS(int64_t)
    DEFINE_ALL_METHODS(double)
    DEFINE_KEYVALUE_METHODS(uint64_t)
    DEFINE_KEYVALUE_METHODS(int64_t)
    DEFINE_KEYVALUE_METHODS(double)
    DEFINE_KEYVALUE_METHODS(uint32_t)
    DEFINE_KEYVALUE_METHODS(int32_t)
    DEFINE_KEYVALUE_METHODS(float)
} // namespace avx2
} // namespace xss