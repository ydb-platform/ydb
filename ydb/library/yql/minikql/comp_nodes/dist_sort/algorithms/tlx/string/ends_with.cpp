/*******************************************************************************
 * tlx/string/ends_with.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2019 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/string/ends_with.hpp>

#include <algorithm>
#include <cstring>

#include <tlx/string/to_lower.hpp>

namespace tlx {

/******************************************************************************/

bool ends_with(const char* str, const char* match) {
    size_t str_size = 0, match_size = 0;
    while (*str != 0)
        ++str, ++str_size;
    while (*match != 0)
        ++match, ++match_size;
    if (match_size > str_size)
        return false;

    while (match_size != 0) {
        --str, --match, --match_size;
        if (*str != *match) return false;
    }
    return true;
}

bool ends_with(const char* str, const std::string& match) {
    size_t str_size = 0, match_size = match.size();
    while (*str != 0)
        ++str, ++str_size;
    if (match_size > str_size)
        return false;

    std::string::const_iterator m = match.end();
    while (m != match.begin()) {
        --str, --m;
        if (*str != *m) return false;
    }
    return true;
}

bool ends_with(const std::string& str, const char* match) {
    size_t str_size = str.size(), match_size = strlen(match);
    if (match_size > str_size)
        return false;

    std::string::const_iterator s = str.end() - match_size;
    while (*match != 0) {
        if (*s != *match) return false;
        ++s, ++match;
    }
    return true;
}

bool ends_with(const std::string& str, const std::string& match) {
    if (match.size() > str.size())
        return false;

    return std::equal(match.begin(), match.end(), str.end() - match.size());
}

/******************************************************************************/

bool ends_with_icase(const char* str, const char* match) {
    size_t str_size = 0, match_size = 0;
    while (*str != 0)
        ++str, ++str_size;
    while (*match != 0)
        ++match, ++match_size;
    if (match_size > str_size)
        return false;

    while (match_size != 0) {
        --str, --match, --match_size;
        if (to_lower(*str) != to_lower(*match)) return false;
    }
    return true;
}

bool ends_with_icase(const char* str, const std::string& match) {
    size_t str_size = 0, match_size = match.size();
    while (*str != 0)
        ++str, ++str_size;
    if (match_size > str_size)
        return false;

    std::string::const_iterator m = match.end();
    while (m != match.begin()) {
        --str, --m;
        if (to_lower(*str) != to_lower(*m)) return false;
    }
    return true;
}

bool ends_with_icase(const std::string& str, const char* match) {
    size_t str_size = str.size(), match_size = strlen(match);
    if (match_size > str_size)
        return false;

    std::string::const_iterator s = str.end() - match_size;
    while (*match != 0) {
        if (to_lower(*s) != to_lower(*match)) return false;
        ++s, ++match;
    }
    return true;
}

bool ends_with_icase(const std::string& str, const std::string& match) {
    if (match.size() > str.size())
        return false;

    return std::equal(match.begin(), match.end(), str.end() - match.size(),
                      [](const char& c1, const char& c2) {
                          return to_lower(c1) == to_lower(c2);
                      });
}

/******************************************************************************/

} // namespace tlx

/******************************************************************************/
