# x86-simd-sort

C++ header file library for SIMD based 16-bit, 32-bit and 64-bit data type
sorting algorithms on x86 processors. We currently have AVX-512 and AVX2
(32-bit and 64-bit only) based implementation of quicksort, quickselect &
partialsort and AVX-512 implementations of argsort, argselect and key-value
sort. The following API's are currently supported:

#### Quicksort

Equivalent to `qsort` in
[C](https://www.tutorialspoint.com/c_standard_library/c_function_qsort.htm) or
`std::sort` in [C++](https://en.cppreference.com/w/cpp/algorithm/sort).

```cpp
void avx512_qsort<T>(T* arr, size_t arrsize, bool hasnan = false, bool descending = false);
void avx2_qsort<T>(T* arr, size_t arrsize, bool hasnan = false, bool descending = false);
```
Supported datatypes: `uint16_t`, `int16_t`, `_Float16`, `uint32_t`, `int32_t`,
`float`, `uint64_t`, `int64_t` and `double`. AVX2 versions currently support
32-bit and 64-bit dtypes only. For floating-point types, if `arr` contains
NaNs, they are moved to the end and replaced with a quiet NaN. That is, the
original, bit-exact NaNs in the input are not preserved.

#### Quickselect
Equivalent to `std::nth_element` in
[C++](https://en.cppreference.com/w/cpp/algorithm/nth_element) or
`np.partition` in
[NumPy](https://numpy.org/doc/stable/reference/generated/numpy.partition.html).


```cpp
void avx512_qselect<T>(T* arr, size_t arrsize, bool hasnan = false, bool descending = false);
void avx2_qselect<T>(T* arr, size_t arrsize, bool hasnan = false, bool descending = false);
```
Supported datatypes: `uint16_t`, `int16_t`, `_Float16`, `uint32_t`, `int32_t`,
`float`, `uint64_t`, `int64_t` and `double`. AVX2 versions currently support
32-bit and 64-bit dtypes only. For floating-point types, if `bool hasnan` is
set, NaNs are moved to the end of the array, preserving the bit-exact NaNs in
the input. If NaNs are present but `hasnan` is `false`, the behavior is
undefined.

#### Partialsort
Equivalent to `std::partial_sort` in
[C++](https://en.cppreference.com/w/cpp/algorithm/partial_sort).


```cpp
void avx512_partial_qsort<T>(T* arr, size_t arrsize, bool hasnan = false, bool descending = false)
void avx2_partial_qsort<T>(T* arr, size_t arrsize, bool hasnan = false, bool descending = false)
```
Supported datatypes: `uint16_t`, `int16_t`, `_Float16`, `uint32_t`, `int32_t`,
`float`, `uint64_t`, `int64_t` and `double`. AVX2 versions currently support
32-bit and 64-bit dtypes only. For floating-point types, if `bool hasnan` is
set, NaNs are moved to the end of the array, preserving the bit-exact NaNs in
the input. If NaNs are present but `hasnan` is `false`, the behavior is
undefined.

#### Argsort
Equivalent to `np.argsort` in
[NumPy](https://numpy.org/doc/stable/reference/generated/numpy.argsort.html).

```cpp
std::vector<size_t> arg = avx512_argsort<T>(T* arr, size_t arrsize, bool hasnan = false, bool descending = false);
void avx512_argsort<T>(T* arr, size_t *arg, size_t arrsize, bool hasnan = false, bool descending = false);
```
Supported datatypes: `uint32_t`, `int32_t`, `float`, `uint64_t`, `int64_t` and
`double`.

The algorithm resorts to scalar `std::sort` if the array contains NaNs.

#### Argselect
Equivalent to `np.argselect` in
[NumPy](https://numpy.org/doc/stable/reference/generated/numpy.argpartition.html).

```cpp
std::vector<size_t> arg = avx512_argselect<T>(T* arr, size_t k, size_t arrsize);
void avx512_argselect<T>(T* arr, size_t *arg, size_t k, size_t arrsize);
```
Supported datatypes: `uint32_t`, `int32_t`, `float`, `uint64_t`, `int64_t` and
`double`.

The algorithm resorts to scalar `std::sort` if the array contains NaNs.

#### Key-value sort
```cpp
void avx512_qsort_kv<T>(T1* key, T2* value , size_t arrsize)
```
Supported datatypes: `uint64_t, int64_t and double`

## Algorithm details

The ideas and code are based on these two research papers [1] and [2]. On a
high level, the idea is to vectorize quicksort partitioning using AVX-512
compressstore instructions. If the array size is less than a certain threshold
(typically 512, 256, 128 or 64), then we use sorting networks [4,5] implemented
on AVX512/AVX registers. Article [4] is a good resource for bitonic sorting
network. Article [5] lists optimal sorting newtorks for various array sizes.
The core implementations of the vectorized qsort functions `avx*_qsort<T>(T*,
size_t)` are modified versions of avx2 quicksort presented in the paper [2] and
source code associated with that paper [3].

## Example to include and build this in a C++ code

### Sample code `main.cpp`

```cpp
#include "src/avx512-32bit-qsort.hpp"

int main() {
    const int ARRSIZE = 1000;
    std::vector<float> arr;

    /* Initialize elements is reverse order */
    for (int ii = 0; ii < ARRSIZE; ++ii) {
        arr.push_back(ARRSIZE - ii);
    }

    /* call avx512 quicksort */
    avx512_qsort(arr.data(), ARRSIZE);
    return 0;
}

```

### Build using g++

```
g++ main.cpp -mavx512f -mavx512dq -O3
```

If you are using src files directly, then it is a header file only and we do
not provide any compile time and run time checks which is recommended while
including this in your source code. The header files are integrated into
[NumPy](https://github.com/numpy/numpy) code base and this [pull
request](https://github.com/numpy/numpy/pull/22315) is a good reference for how
to include and build this library with your source code.

## Build requirements

The sorting routines relies only on the C++ Standard Library and requires a
relatively modern compiler to build (gcc 8.x and above).

## Instruction set requirements

The `avx512_*` routines can only run on processors that have AVX-512.
Specifically, the 32-bit and 64-bit require AVX-512F and AVX-512DQ instruction
set. The 16-bit sorting requires the AVX-512F, AVX-512BW and AVX-512 VMBI2
instruction set. Sorting `_Float16` will require AVX-512FP16.

The `avx2_*` routines require AVX/AVX2 instruction set. We currently only
support 32-bit and 64-bit data for AVX2 based methods with plans to extend that
to all the other routines and data types.

## References

* [1] Fast and Robust Vectorized In-Place Sorting of Primitive Types
    https://drops.dagstuhl.de/opus/volltexte/2021/13775/

* [2] A Novel Hybrid Quicksort Algorithm Vectorized using AVX-512 on Intel
Skylake https://arxiv.org/pdf/1704.08579.pdf

* [3] https://github.com/simd-sorting/fast-and-robust: SPDX-License-Identifier: MIT

* [4] http://mitp-content-server.mit.edu:18180/books/content/sectbyfn?collid=books_pres_0&fn=Chapter%2027.pdf&id=8030

* [5] https://bertdobbelaere.github.io/sorting_networks.html
