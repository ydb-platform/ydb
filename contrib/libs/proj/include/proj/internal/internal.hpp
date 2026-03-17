/******************************************************************************
 *
 * Project:  PROJ
 * Purpose:  ISO19111:2019 implementation
 * Author:   Even Rouault <even dot rouault at spatialys dot com>
 *
 ******************************************************************************
 * Copyright (c) 2018, Even Rouault <even dot rouault at spatialys dot com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 ****************************************************************************/

#ifndef FROM_PROJ_CPP
#error This file should only be included from a PROJ cpp file
#endif

#ifndef INTERNAL_HH_INCLUDED
#define INTERNAL_HH_INCLUDED

#if !(__cplusplus >= 201103L || (defined(_MSC_VER) && _MSC_VER >= 1900))
#error Must have C++11 or newer.
#endif

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#ifndef DOXYGEN_ENABLED
#include <type_traits> // for std::is_base_of
#endif
#include <vector>

#include "proj/util.hpp"

//! @cond Doxygen_Suppress

// Use "PROJ_FALLTHROUGH;" to annotate deliberate fall-through in switches,
// use it analogously to "break;".  The trailing semi-colon is required.
#if !defined(PROJ_FALLTHROUGH) && defined(__has_cpp_attribute)
#if __cplusplus >= 201703L && __has_cpp_attribute(fallthrough)
#define PROJ_FALLTHROUGH [[fallthrough]]
#elif __cplusplus >= 201103L && __has_cpp_attribute(gnu::fallthrough)
#define PROJ_FALLTHROUGH [[gnu::fallthrough]]
#elif __cplusplus >= 201103L && __has_cpp_attribute(clang::fallthrough)
#define PROJ_FALLTHROUGH [[clang::fallthrough]]
#endif
#endif

#ifndef PROJ_FALLTHROUGH
#define PROJ_FALLTHROUGH ((void)0)
#endif

#if defined(__clang__) || defined(_MSC_VER)
#define COMPILER_WARNS_ABOUT_ABSTRACT_VBASE_INIT
#endif

NS_PROJ_START

namespace operation {
class OperationParameterValue;
} // namespace operation

namespace internal {

/** Use cpl::down_cast<Derived*>(pointer_to_base) as equivalent of
 * static_cast<Derived*>(pointer_to_base) with safe checking in debug
 * mode.
 *
 * Only works if no virtual inheritance is involved.
 *
 * @param f pointer to a base class
 * @return pointer to a derived class
 */
template <typename To, typename From> inline To down_cast(From *f) {
    static_assert(
        (std::is_base_of<From, typename std::remove_pointer<To>::type>::value),
        "target type not derived from source type");
    assert(f == nullptr || dynamic_cast<To>(f) != nullptr);
    return static_cast<To>(f);
}

PROJ_FOR_TEST std::string replaceAll(const std::string &str,
                                     const std::string &before,
                                     const std::string &after);

PROJ_DLL size_t ci_find(const std::string &osStr, const char *needle) noexcept;

size_t ci_find(const std::string &osStr, const std::string &needle,
               size_t startPos = 0) noexcept;

inline bool starts_with(const std::string &str,
                        const std::string &prefix) noexcept {
    if (str.size() < prefix.size()) {
        return false;
    }
    return std::memcmp(str.c_str(), prefix.c_str(), prefix.size()) == 0;
}

inline bool starts_with(const std::string &str, const char *prefix) noexcept {
    const size_t prefixSize = std::strlen(prefix);
    if (str.size() < prefixSize) {
        return false;
    }
    return std::memcmp(str.c_str(), prefix, prefixSize) == 0;
}

bool ci_less(const std::string &a, const std::string &b) noexcept;

PROJ_DLL bool ci_starts_with(const char *str, const char *prefix) noexcept;

bool ci_starts_with(const std::string &str, const std::string &prefix) noexcept;

bool ends_with(const std::string &str, const std::string &suffix) noexcept;

PROJ_FOR_TEST std::string tolower(const std::string &osStr);

std::string toupper(const std::string &osStr);

PROJ_FOR_TEST std::vector<std::string> split(const std::string &osStr,
                                             char separator);

PROJ_FOR_TEST std::vector<std::string> split(const std::string &osStr,
                                             const std::string &separator);

bool ci_equal(const char *a, const char *b) noexcept;

bool ci_equal(const char *a, const std::string &b) = delete;

PROJ_FOR_TEST bool ci_equal(const std::string &a, const char *b) noexcept;

PROJ_FOR_TEST bool ci_equal(const std::string &a,
                            const std::string &b) noexcept;

std::string stripQuotes(const std::string &osStr);

std::string toString(int val);

PROJ_FOR_TEST std::string toString(double val, int precision = 15);

PROJ_FOR_TEST double
c_locale_stod(const std::string &s); // throw(std::invalid_argument)

// Variant of above that doesn't emit exceptions
double c_locale_stod(const std::string &s, bool &success);

std::string concat(const std::string &, const std::string &) = delete;
std::string concat(const char *, const char *) = delete;
std::string concat(const char *a, const std::string &b);
std::string concat(const std::string &, const char *) = delete;
std::string concat(const char *, const char *, const char *) = delete;
std::string concat(const char *, const char *, const std::string &) = delete;
std::string concat(const char *a, const std::string &b, const char *c);
std::string concat(const char *, const std::string &,
                   const std::string &) = delete;
std::string concat(const std::string &, const char *, const char *) = delete;
std::string concat(const std::string &, const char *,
                   const std::string &) = delete;
std::string concat(const std::string &, const std::string &,
                   const char *) = delete;
std::string concat(const std::string &, const std::string &,
                   const std::string &) = delete;

double getRoundedEpochInDecimalYear(double year);

} // namespace internal

NS_PROJ_END

//! @endcond

#endif // INTERNAL_HH_INCLUDED
