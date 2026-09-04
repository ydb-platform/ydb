#ifndef X86_SIMD_SORT
#define X86_SIMD_SORT
#include <stdint.h>
#include <vector>
#include <cstddef>
#include <functional>
#include <numeric>

#define XSS_EXPORT_SYMBOL __attribute__((visibility("default")))
#define XSS_HIDE_SYMBOL __attribute__((visibility("hidden")))
#define UNUSED(x) (void)(x)

namespace x86simdsort {

// quicksort
template <typename T>
XSS_EXPORT_SYMBOL void
qsort(T *arr, size_t arrsize, bool hasnan = false, bool descending = false);

// quickselect
template <typename T>
XSS_EXPORT_SYMBOL void qselect(T *arr,
                               size_t k,
                               size_t arrsize,
                               bool hasnan = false,
                               bool descending = false);

// partial sort
template <typename T>
XSS_EXPORT_SYMBOL void partial_qsort(T *arr,
                                     size_t k,
                                     size_t arrsize,
                                     bool hasnan = false,
                                     bool descending = false);

// argsort
template <typename T>
XSS_EXPORT_SYMBOL std::vector<size_t>
argsort(T *arr, size_t arrsize, bool hasnan = false, bool descending = false);

// argselect
template <typename T>
XSS_EXPORT_SYMBOL std::vector<size_t>
argselect(T *arr, size_t k, size_t arrsize, bool hasnan = false);

// keyvalue sort
template <typename T1, typename T2>
XSS_EXPORT_SYMBOL void keyvalue_qsort(T1 *key,
                                      T2 *val,
                                      size_t arrsize,
                                      bool hasnan = false,
                                      bool descending = false);

// keyvalue select
template <typename T1, typename T2>
XSS_EXPORT_SYMBOL void keyvalue_select(T1 *key,
                                       T2 *val,
                                       size_t k,
                                       size_t arrsize,
                                       bool hasnan = false,
                                       bool descending = false);

// keyvalue partial sort
template <typename T1, typename T2>
XSS_EXPORT_SYMBOL void keyvalue_partial_sort(T1 *key,
                                             T2 *val,
                                             size_t k,
                                             size_t arrsize,
                                             bool hasnan = false,
                                             bool descending = false);

// sort an object
template <typename T, typename Func>
XSS_EXPORT_SYMBOL void object_qsort(T *arr, uint32_t arrsize, Func key_func)
{
    /* (1) Create a vector a keys */
    using return_type_of =
            typename decltype(std::function {key_func})::result_type;
    std::vector<return_type_of> keys(arrsize);
    for (size_t ii = 0; ii < arrsize; ++ii) {
        keys[ii] = key_func(arr[ii]);
    }

    /* (2) Call arg based on keys using the keyvalue sort */
    std::vector<uint32_t> arg(arrsize);
    std::iota(arg.begin(), arg.end(), 0);
    x86simdsort::keyvalue_qsort(keys.data(), arg.data(), arrsize);

    /* (3) Permute obj array in-place */
    std::vector<bool> done(arrsize);
    for (size_t i = 0; i < arrsize; ++i) {
        if (done[i]) { continue; }
        done[i] = true;
        size_t prev_j = i;
        size_t j = arg[i];
        while (i != j) {
            std::swap(arr[prev_j], arr[j]);
            done[j] = true;
            prev_j = j;
            j = arg[j];
        }
    }
}

} // namespace x86simdsort
#endif
