/*******************************************************************************
 * tlx/sort/strings_parallel.hpp
 *
 * Front-end for parallel string sorting algorithms.
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2019 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_SORT_STRINGS_PARALLEL_HEADER
#define TLX_SORT_STRINGS_PARALLEL_HEADER

#include <tlx/sort/strings/parallel_sample_sort.hpp>

#include <cstdint>
#include <string>
#include <vector>

namespace tlx {

//! \addtogroup tlx_sort
//! \{
//! \name String Sorting Algorithms
//! \{

/******************************************************************************/

/*!
 * Sort a set of strings in parallel represented by C-style uint8_t* in place.
 *
 * The memory limit is currently not used.
 */
static inline
void sort_strings_parallel(unsigned char** strings, size_t size,
                           size_t memory = 0) {
    sort_strings_detail::parallel_sample_sort(
        sort_strings_detail::StringPtr<sort_strings_detail::UCharStringSet>(
            sort_strings_detail::UCharStringSet(strings, strings + size)),
        /* depth */ 0, memory);
}

/*!
 * Sort a set of strings in parallel represented by C-style char* in place.
 *
 * The strings are sorted as _unsigned_ 8-bit characters, not signed characters!
 * The memory limit is currently not used.
 */
static inline
void sort_strings_parallel(char** strings, size_t size, size_t memory = 0) {
    return sort_strings_parallel(
        reinterpret_cast<unsigned char**>(strings), size, memory);
}

/*!
 * Sort a set of strings in parallel represented by C-style uint8_t* in place.
 *
 * The memory limit is currently not used.
 */
static inline
void sort_strings_parallel(const unsigned char** strings, size_t size,
                           size_t memory = 0) {
    sort_strings_detail::parallel_sample_sort(
        sort_strings_detail::StringPtr<sort_strings_detail::CUCharStringSet>(
            sort_strings_detail::CUCharStringSet(strings, strings + size)),
        /* depth */ 0, memory);
}

/*!
 * Sort a set of strings in parallel represented by C-style char* in place.
 *
 * The strings are sorted as _unsigned_ 8-bit characters, not signed characters!
 * The memory limit is currently not used.
 */
static inline
void sort_strings_parallel(const char** strings, size_t size,
                           size_t memory = 0) {
    return sort_strings_parallel(
        reinterpret_cast<const unsigned char**>(strings), size, memory);
}

/******************************************************************************/

/*!
 * Sort a set of strings in parallel represented by C-style char* in place.
 *
 * The strings are sorted as _unsigned_ 8-bit characters, not signed characters!
 * The memory limit is currently not used.
 */
static inline
void sort_strings_parallel(std::vector<char*>& strings, size_t memory = 0) {
    return sort_strings_parallel(strings.data(), strings.size(), memory);
}

/*!
 * Sort a set of strings in parallel represented by C-style uint8_t* in place.
 *
 * The memory limit is currently not used.
 */
static inline
void sort_strings_parallel(std::vector<unsigned char*>& strings,
                           size_t memory = 0) {
    return sort_strings_parallel(strings.data(), strings.size(), memory);
}

/*!
 * Sort a set of strings in parallel represented by C-style char* in place.
 *
 * The strings are sorted as _unsigned_ 8-bit characters, not signed characters!
 * The memory limit is currently not used.
 */
static inline
void sort_strings_parallel(std::vector<const char*>& strings,
                           size_t memory = 0) {
    return sort_strings_parallel(strings.data(), strings.size(), memory);
}

/*!
 * Sort a set of strings in parallel represented by C-style uint8_t* in place.
 *
 * The memory limit is currently not used.
 */
static inline
void sort_strings_parallel(std::vector<const unsigned char*>& strings,
                           size_t memory = 0) {
    return sort_strings_parallel(strings.data(), strings.size(), memory);
}

/******************************************************************************/

/*!
 * Sort a set of std::strings in place in parallel.
 *
 * The strings are sorted as _unsigned_ 8-bit characters, not signed characters!
 * The memory limit is currently not used.
 */
static inline
void sort_strings_parallel(std::string* strings, size_t size,
                           size_t memory = 0) {
    sort_strings_detail::parallel_sample_sort(
        sort_strings_detail::StringPtr<sort_strings_detail::StdStringSet>(
            sort_strings_detail::StdStringSet(strings, strings + size)),
        /* depth */ 0, memory);
}

/*!
 * Sort a vector of std::strings in place in parallel.
 *
 * The strings are sorted as _unsigned_ 8-bit characters, not signed characters!
 * The memory limit is currently not used.
 */
static inline
void sort_strings_parallel(std::vector<std::string>& strings,
                           size_t memory = 0) {
    return sort_strings_parallel(strings.data(), strings.size(), memory);
}

/******************************************************************************/
/******************************************************************************/
/******************************************************************************/

/*!
 * Sort a set of strings in parallel represented by C-style uint8_t* in place.
 *
 * The memory limit is currently not used.
 */
static inline
void sort_strings_parallel_lcp(unsigned char** strings, size_t size,
                               std::uint32_t* lcp, size_t memory = 0) {
    sort_strings_detail::parallel_sample_sort(
        sort_strings_detail::StringLcpPtr<
            sort_strings_detail::UCharStringSet, std::uint32_t>(
            sort_strings_detail::UCharStringSet(strings, strings + size), lcp),
        /* depth */ 0, memory);
}

/*!
 * Sort a set of strings in parallel represented by C-style char* in place.
 *
 * The strings are sorted as _unsigned_ 8-bit characters, not signed characters!
 * The memory limit is currently not used.
 */
static inline
void sort_strings_parallel_lcp(char** strings, size_t size, std::uint32_t* lcp,
                               size_t memory = 0) {
    return sort_strings_parallel_lcp(
        reinterpret_cast<unsigned char**>(strings), size, lcp, memory);
}

/*!
 * Sort a set of strings in parallel represented by C-style uint8_t* in place.
 *
 * The memory limit is currently not used.
 */
static inline
void sort_strings_parallel_lcp(const unsigned char** strings, size_t size,
                               std::uint32_t* lcp, size_t memory = 0) {
    sort_strings_detail::parallel_sample_sort(
        sort_strings_detail::StringLcpPtr<
            sort_strings_detail::CUCharStringSet, std::uint32_t>(
            sort_strings_detail::CUCharStringSet(strings, strings + size), lcp),
        /* depth */ 0, memory);
}

/*!
 * Sort a set of strings in parallel represented by C-style char* in place.
 *
 * The strings are sorted as _unsigned_ 8-bit characters, not signed characters!
 * The memory limit is currently not used.
 */
static inline
void sort_strings_parallel_lcp(const char** strings, size_t size,
                               std::uint32_t* lcp, size_t memory = 0) {
    return sort_strings_parallel_lcp(
        reinterpret_cast<const unsigned char**>(strings), size, lcp, memory);
}

/******************************************************************************/

/*!
 * Sort a set of strings in parallel represented by C-style char* in place.
 *
 * The strings are sorted as _unsigned_ 8-bit characters, not signed characters!
 * The memory limit is currently not used.
 */
static inline
void sort_strings_parallel_lcp(std::vector<char*>& strings, std::uint32_t* lcp,
                               size_t memory = 0) {
    return sort_strings_parallel_lcp(
        strings.data(), strings.size(), lcp, memory);
}

/*!
 * Sort a set of strings in parallel represented by C-style uint8_t* in place.
 *
 * The memory limit is currently not used.
 */
static inline
void sort_strings_parallel_lcp(std::vector<unsigned char*>& strings,
                               std::uint32_t* lcp, size_t memory = 0) {
    return sort_strings_parallel_lcp(
        strings.data(), strings.size(), lcp, memory);
}

/*!
 * Sort a set of strings in parallel represented by C-style char* in place.
 *
 * The strings are sorted as _unsigned_ 8-bit characters, not signed characters!
 * The memory limit is currently not used.
 */
static inline
void sort_strings_parallel_lcp(std::vector<const char*>& strings, std::uint32_t* lcp,
                               size_t memory = 0) {
    return sort_strings_parallel_lcp(
        strings.data(), strings.size(), lcp, memory);
}

/*!
 * Sort a set of strings in parallel represented by C-style uint8_t* in place.
 *
 * The memory limit is currently not used.
 */
static inline
void sort_strings_parallel_lcp(std::vector<const unsigned char*>& strings,
                               std::uint32_t* lcp, size_t memory = 0) {
    return sort_strings_parallel_lcp(
        strings.data(), strings.size(), lcp, memory);
}

/******************************************************************************/

/*!
 * Sort a set of std::strings in place in parallel.
 *
 * The strings are sorted as _unsigned_ 8-bit characters, not signed characters!
 * The memory limit is currently not used.
 */
static inline
void sort_strings_parallel_lcp(std::string* strings, size_t size,
                               std::uint32_t* lcp, size_t memory = 0) {
    sort_strings_detail::parallel_sample_sort(
        sort_strings_detail::StringLcpPtr<
            sort_strings_detail::StdStringSet, std::uint32_t>(
            sort_strings_detail::StdStringSet(strings, strings + size), lcp),
        /* depth */ 0, memory);
}

/*!
 * Sort a vector of std::strings in place in parallel.
 *
 * The strings are sorted as _unsigned_ 8-bit characters, not signed characters!
 * The memory limit is currently not used.
 */
static inline
void sort_strings_parallel_lcp(std::vector<std::string>& strings,
                               std::uint32_t* lcp, size_t memory = 0) {
    return sort_strings_parallel_lcp(
        strings.data(), strings.size(), lcp, memory);
}

/******************************************************************************/

//! \}
//! \}

} // namespace tlx

#endif // !TLX_SORT_STRINGS_PARALLEL_HEADER

/******************************************************************************/
