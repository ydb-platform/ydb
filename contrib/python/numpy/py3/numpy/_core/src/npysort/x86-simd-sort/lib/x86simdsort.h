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
template <typename T, typename U, typename Func>
XSS_EXPORT_SYMBOL void object_qsort(T *arr, U arrsize, Func key_func)
{
    static_assert(std::is_integral<U>::value, "arrsize must be an integral type");
    static_assert(sizeof(U) == sizeof(int32_t) || sizeof(U) == sizeof(int64_t),
                  "arrsize must be 32 or 64 bits");
    using return_type_of = typename decltype(std::function{key_func})::result_type;
    static_assert(sizeof(return_type_of) == sizeof(int32_t) || sizeof(return_type_of) == sizeof(int64_t),
                  "key_func return type must be 32 or 64 bits");
    std::vector<return_type_of> keys(arrsize);
    for (U ii = 0; ii < arrsize; ++ii) {
        keys[ii] = key_func(arr[ii]);
    }

    /* (2) Call arg based on keys using the keyvalue sort */
    std::vector<U> arg(arrsize);
    std::iota(arg.begin(), arg.end(), 0);
    x86simdsort::keyvalue_qsort(keys.data(), arg.data(), arrsize);

    /* (3) Permute obj array in-place */
    std::vector<bool> done(arrsize);
    for (U i = 0; i < arrsize; ++i) {
        if (done[i]) { continue; }
        done[i] = true;
        U prev_j = i;
        U j = arg[i];
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
