/*******************************************************************************
 * tlx/sort/strings.hpp
 *
 * Front-end for string sorting algorithms.
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_SORT_STRINGS_HEADER
#define TLX_SORT_STRINGS_HEADER

#include "../sort/strings/insertion_sort.hpp"
#include "../sort/strings/multikey_quicksort.hpp"
#include "../sort/strings/radix_sort.hpp"

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
 * Sort a set of strings represented by C-style uint8_t* in place.
 *
 * If the memory limit is non zero, possibly slower algorithms will be selected
 * to stay within the memory limit.
 */
static inline
void sort_strings(unsigned char** strings, size_t size, size_t memory = 0) {
    sort_strings_detail::radixsort_CE3(
        sort_strings_detail::StringPtr<sort_strings_detail::UCharStringSet>(
            sort_strings_detail::UCharStringSet(strings, strings + size)),
        /* depth */ 0, memory);
}

/*!
 * Sort a set of strings represented by C-style char* in place.
 *
 * The strings are sorted as _unsigned_ 8-bit characters, not signed characters!
 * If the memory limit is non zero, possibly slower algorithms will be selected
 * to stay within the memory limit.
 */
static inline
void sort_strings(char** strings, size_t size, size_t memory = 0) {
    return sort_strings(
        reinterpret_cast<unsigned char**>(strings), size, memory);
}

/*!
 * Sort a set of strings represented by C-style uint8_t* in place.
 *
 * If the memory limit is non zero, possibly slower algorithms will be selected
 * to stay within the memory limit.
 */
static inline
void sort_strings(const unsigned char** strings, size_t size,
                  size_t memory = 0) {
    sort_strings_detail::radixsort_CE3(
        sort_strings_detail::StringPtr<sort_strings_detail::CUCharStringSet>(
            sort_strings_detail::CUCharStringSet(strings, strings + size)),
        /* depth */ 0, memory);
}

/*!
 * Sort a set of strings represented by C-style char* in place.
 *
 * The strings are sorted as _unsigned_ 8-bit characters, not signed characters!
 * If the memory limit is non zero, possibly slower algorithms will be selected
 * to stay within the memory limit.
 */
static inline
void sort_strings(const char** strings, size_t size, size_t memory = 0) {
    return sort_strings(
        reinterpret_cast<const unsigned char**>(strings), size, memory);
}

/******************************************************************************/

/*!
 * Sort a set of strings represented by C-style char* in place.
 *
 * The strings are sorted as _unsigned_ 8-bit characters, not signed characters!
 * If the memory limit is non zero, possibly slower algorithms will be selected
 * to stay within the memory limit.
 */
static inline
void sort_strings(std::vector<char*>& strings, size_t memory = 0) {
    return sort_strings(strings.data(), strings.size(), memory);
}

/*!
 * Sort a set of strings represented by C-style uint8_t* in place.
 *
 * If the memory limit is non zero, possibly slower algorithms will be selected
 * to stay within the memory limit.
 */
static inline
void sort_strings(std::vector<unsigned char*>& strings, size_t memory = 0) {
    return sort_strings(strings.data(), strings.size(), memory);
}

/*!
 * Sort a set of strings represented by C-style char* in place.
 *
 * The strings are sorted as _unsigned_ 8-bit characters, not signed characters!
 * If the memory limit is non zero, possibly slower algorithms will be selected
 * to stay within the memory limit.
 */
static inline
void sort_strings(std::vector<const char*>& strings, size_t memory = 0) {
    return sort_strings(strings.data(), strings.size(), memory);
}

/*!
 * Sort a set of strings represented by C-style uint8_t* in place.
 *
 * If the memory limit is non zero, possibly slower algorithms will be selected
 * to stay within the memory limit.
 */
static inline
void sort_strings(std::vector<const unsigned char*>& strings,
                  size_t memory = 0) {
    return sort_strings(strings.data(), strings.size(), memory);
}

/******************************************************************************/

/*!
 * Sort a set of std::strings in place.
 *
 * The strings are sorted as _unsigned_ 8-bit characters, not signed characters!
 * If the memory limit is non zero, possibly slower algorithms will be selected
 * to stay within the memory limit.
 */
static inline
void sort_strings(std::string* strings, size_t size, size_t memory = 0) {
    sort_strings_detail::radixsort_CE3(
        sort_strings_detail::StringPtr<sort_strings_detail::StdStringSet>(
            sort_strings_detail::StdStringSet(strings, strings + size)),
        /* depth */ 0, memory);
}

/*!
 * Sort a vector of std::strings in place.
 *
 * The strings are sorted as _unsigned_ 8-bit characters, not signed characters!
 * If the memory limit is non zero, possibly slower algorithms will be selected
 * to stay within the memory limit.
 */
static inline
void sort_strings(std::vector<std::string>& strings, size_t memory = 0) {
    return sort_strings(strings.data(), strings.size(), memory);
}

/******************************************************************************/
/******************************************************************************/
/******************************************************************************/

/*!
 * Sort a set of strings represented by C-style uint8_t* in place.
 *
 * If the memory limit is non zero, possibly slower algorithms will be selected
 * to stay within the memory limit.
 */
static inline
void sort_strings_lcp(unsigned char** strings, size_t size, std::uint32_t* lcp,
                      size_t memory = 0) {
    sort_strings_detail::radixsort_CE3(
        sort_strings_detail::StringLcpPtr<
            sort_strings_detail::UCharStringSet, std::uint32_t>(
            sort_strings_detail::UCharStringSet(strings, strings + size), lcp),
        /* depth */ 0, memory);
}

/*!
 * Sort a set of strings represented by C-style char* in place.
 *
 * The strings are sorted as _unsigned_ 8-bit characters, not signed characters!
 * If the memory limit is non zero, possibly slower algorithms will be selected
 * to stay within the memory limit.
 */
static inline
void sort_strings_lcp(char** strings, size_t size, std::uint32_t* lcp,
                      size_t memory = 0) {
    return sort_strings_lcp(
        reinterpret_cast<unsigned char**>(strings), size, lcp, memory);
}

/*!
 * Sort a set of strings represented by C-style uint8_t* in place.
 *
 * If the memory limit is non zero, possibly slower algorithms will be selected
 * to stay within the memory limit.
 */
static inline
void sort_strings_lcp(const unsigned char** strings, size_t size, std::uint32_t* lcp,
                      size_t memory = 0) {
    sort_strings_detail::radixsort_CE3(
        sort_strings_detail::StringLcpPtr<
            sort_strings_detail::CUCharStringSet, std::uint32_t>(
            sort_strings_detail::CUCharStringSet(strings, strings + size), lcp),
        /* depth */ 0, memory);
}

/*!
 * Sort a set of strings represented by C-style char* in place.
 *
 * The strings are sorted as _unsigned_ 8-bit characters, not signed characters!
 * If the memory limit is non zero, possibly slower algorithms will be selected
 * to stay within the memory limit.
 */
static inline
void sort_strings_lcp(const char** strings, size_t size, std::uint32_t* lcp,
                      size_t memory = 0) {
    return sort_strings_lcp(
        reinterpret_cast<const unsigned char**>(strings), size, lcp, memory);
}

/******************************************************************************/

/*!
 * Sort a set of strings represented by C-style char* in place.
 *
 * The strings are sorted as _unsigned_ 8-bit characters, not signed characters!
 * If the memory limit is non zero, possibly slower algorithms will be selected
 * to stay within the memory limit.
 */
static inline
void sort_strings_lcp(std::vector<char*>& strings, std::uint32_t* lcp,
                      size_t memory = 0) {
    return sort_strings_lcp(strings.data(), strings.size(), lcp, memory);
}

/*!
 * Sort a set of strings represented by C-style uint8_t* in place.
 *
 * If the memory limit is non zero, possibly slower algorithms will be selected
 * to stay within the memory limit.
 */
static inline
void sort_strings_lcp(std::vector<unsigned char*>& strings, std::uint32_t* lcp,
                      size_t memory = 0) {
    return sort_strings_lcp(strings.data(), strings.size(), lcp, memory);
}

/*!
 * Sort a set of strings represented by C-style char* in place.
 *
 * The strings are sorted as _unsigned_ 8-bit characters, not signed characters!
 * If the memory limit is non zero, possibly slower algorithms will be selected
 * to stay within the memory limit.
 */
static inline
void sort_strings_lcp(std::vector<const char*>& strings, std::uint32_t* lcp,
                      size_t memory = 0) {
    return sort_strings_lcp(strings.data(), strings.size(), lcp, memory);
}

/*!
 * Sort a set of strings represented by C-style uint8_t* in place.
 *
 * If the memory limit is non zero, possibly slower algorithms will be selected
 * to stay within the memory limit.
 */
static inline
void sort_strings_lcp(std::vector<const unsigned char*>& strings, std::uint32_t* lcp,
                      size_t memory = 0) {
    return sort_strings_lcp(strings.data(), strings.size(), lcp, memory);
}

/******************************************************************************/

/*!
 * Sort a set of std::strings in place.
 *
 * The strings are sorted as _unsigned_ 8-bit characters, not signed characters!
 * If the memory limit is non zero, possibly slower algorithms will be selected
 * to stay within the memory limit.
 */
static inline
void sort_strings_lcp(std::string* strings, size_t size, std::uint32_t* lcp,
                      size_t memory = 0) {
    sort_strings_detail::radixsort_CE3(
        sort_strings_detail::StringLcpPtr<
            sort_strings_detail::StdStringSet, std::uint32_t>(
            sort_strings_detail::StdStringSet(strings, strings + size), lcp),
        /* depth */ 0, memory);
}

/*!
 * Sort a vector of std::strings in place.
 *
 * The strings are sorted as _unsigned_ 8-bit characters, not signed characters!
 * If the memory limit is non zero, possibly slower algorithms will be selected
 * to stay within the memory limit.
 */
static inline
void sort_strings_lcp(std::vector<std::string>& strings, std::uint32_t* lcp,
                      size_t memory = 0) {
    return sort_strings_lcp(strings.data(), strings.size(), lcp, memory);
}

/******************************************************************************/

//! \}
//! \}

} // namespace tlx

#endif // !TLX_SORT_STRINGS_HEADER

/******************************************************************************/
