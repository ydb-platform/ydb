// ICL specific routines:
#include "x86simdsort-static-incl.h"
#include "x86simdsort-internal.h"

namespace xss {
namespace avx512 {
    template <>
    void qsort(uint16_t *arr, size_t size, bool hasnan, bool descending)
    {
        x86simdsortStatic::qsort(arr, size, hasnan, descending);
    }
    template <>
    void qselect(uint16_t *arr,
                 size_t k,
                 size_t arrsize,
                 bool hasnan,
                 bool descending)
    {
        x86simdsortStatic::qselect(arr, k, arrsize, hasnan, descending);
    }
    template <>
    void partial_qsort(uint16_t *arr,
                       size_t k,
                       size_t arrsize,
                       bool hasnan,
                       bool descending)
    {
        x86simdsortStatic::partial_qsort(arr, k, arrsize, hasnan, descending);
    }
    template <>
    void qsort(int16_t *arr, size_t size, bool hasnan, bool descending)
    {
        x86simdsortStatic::qsort(arr, size, hasnan, descending);
    }
    template <>
    void qselect(int16_t *arr,
                 size_t k,
                 size_t arrsize,
                 bool hasnan,
                 bool descending)
    {
        x86simdsortStatic::qselect(arr, k, arrsize, hasnan, descending);
    }
    template <>
    void partial_qsort(int16_t *arr,
                       size_t k,
                       size_t arrsize,
                       bool hasnan,
                       bool descending)
    {
        x86simdsortStatic::partial_qsort(arr, k, arrsize, hasnan, descending);
    }
} // namespace avx512
} // namespace xss
