// SPR specific routines:
#include "x86simdsort-static-incl.h"
#include "x86simdsort-internal.h"

namespace xss {
namespace fp16_spr {
    template <>
    void qsort(_Float16 *arr, size_t size, bool hasnan, bool descending)
    {
        x86simdsortStatic::qsort(arr, size, hasnan, descending);
    }
    template <>
    void qselect(_Float16 *arr,
                 size_t k,
                 size_t arrsize,
                 bool hasnan,
                 bool descending)
    {
        x86simdsortStatic::qselect(arr, k, arrsize, hasnan, descending);
    }
    template <>
    void partial_qsort(_Float16 *arr,
                       size_t k,
                       size_t arrsize,
                       bool hasnan,
                       bool descending)
    {
        x86simdsortStatic::partial_qsort(arr, k, arrsize, hasnan, descending);
    }
} // namespace fp16_spr
} // namespace xss
