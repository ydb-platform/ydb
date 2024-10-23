/*******************************************************************************
 * tlx/string/ssprintf.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2019 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/string/ssprintf.hpp>

#include <cstdarg>
#include <cstdio>

namespace tlx {

std::string ssprintf(const char* fmt, ...) {
    std::string out;
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

std::string ssnprintf(size_t max_size, const char* fmt, ...) {
    std::string out;
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

} // namespace tlx

/******************************************************************************/
