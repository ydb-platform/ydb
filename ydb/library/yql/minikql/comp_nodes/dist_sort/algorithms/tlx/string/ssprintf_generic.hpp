/*******************************************************************************
 * tlx/string/ssprintf_generic.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2019 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_SSPRINTF_GENERIC_HEADER
#define TLX_STRING_SSPRINTF_GENERIC_HEADER

#include <tlx/define/attribute_format_printf.hpp>

#include <cstdarg>
#include <cstdio>
#include <string>

namespace tlx {

//! \addtogroup tlx_string
//! \{

/*!
 * Helper for return the result of a sprintf() call inside a string object.
 *
 * \param fmt printf format and additional parameters
 */
template <typename String = std::string>
String ssprintf_generic(const char* fmt, ...)
TLX_ATTRIBUTE_FORMAT_PRINTF(1, 2);

template <typename String>
String ssprintf_generic(const char* fmt, ...) {
    String out;
    out.resize(128);

    va_list args;
    va_start(args, fmt);

    int size = std::vsnprintf(
        const_cast<char*>(out.data()), out.size() + 1, fmt, args);

    if (size >= static_cast<int>(out.size())) {
        // error, grow buffer and try again.
        out.resize(size);
        size = std::vsnprintf(
            const_cast<char*>(out.data()), out.size() + 1, fmt, args);
    }

    out.resize(size);

    va_end(args);

    return out;
}

/*!
 * Helper for return the result of a snprintf() call inside a string object.
 *
 * \param max_size maximum length of output string, longer ones are truncated.
 * \param fmt printf format and additional parameters
 */
template <typename String = std::string>
String ssnprintf_generic(size_t max_size, const char* fmt, ...)
TLX_ATTRIBUTE_FORMAT_PRINTF(2, 3);

template <typename String>
String ssnprintf_generic(size_t max_size, const char* fmt, ...) {
    String out;
    out.resize(max_size);

    va_list args;
    va_start(args, fmt);

    int size = std::vsnprintf(
        const_cast<char*>(out.data()), out.size() + 1, fmt, args);

    if (static_cast<size_t>(size) < max_size)
        out.resize(static_cast<size_t>(size));

    va_end(args);

    return out;
}

//! \}

} // namespace tlx

#endif // !TLX_STRING_SSPRINTF_GENERIC_HEADER

/******************************************************************************/
