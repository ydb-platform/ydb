/*******************************************************************************
 * tlx/string/extract_between.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/string/extract_between.hpp>

#include <cstring>

namespace tlx {

template <typename Separator1, typename Separator2>
static inline
std::string extract_between_template(
    const std::string& str, const Separator1& sep1, size_t sep1_size,
    const Separator2& sep2) {

    std::string::size_type start = str.find(sep1);
    if (start == std::string::npos)
        return std::string();

    start += sep1_size;

    std::string::size_type limit = str.find(sep2, start);

    if (limit == std::string::npos)
        return std::string();

    return str.substr(start, limit - start);
}

std::string extract_between(const std::string& str, const char* sep1,
                            const char* sep2) {
    return extract_between_template(str, sep1, strlen(sep1), sep2);
}

std::string extract_between(const std::string& str, const char* sep1,
                            const std::string& sep2) {
    return extract_between_template(str, sep1, strlen(sep1), sep2);
}

std::string extract_between(const std::string& str, const std::string& sep1,
                            const char* sep2) {
    return extract_between_template(str, sep1, sep1.size(), sep2);
}

std::string extract_between(const std::string& str, const std::string& sep1,
                            const std::string& sep2) {
    return extract_between_template(str, sep1, sep1.size(), sep2);
}

} // namespace tlx

/******************************************************************************/
