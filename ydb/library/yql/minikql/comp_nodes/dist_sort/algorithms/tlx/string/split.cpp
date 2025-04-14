/*******************************************************************************
 * tlx/string/split.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/string/split.hpp>

#include <cstring>

namespace tlx {

/******************************************************************************/
// split() returning std::vector<std::string>

std::vector<std::string> split(
    char sep, const std::string& str, std::string::size_type limit) {
    // call base method with new std::vector
    std::vector<std::string> out;
    split(&out, sep, str, limit);
    return out;
}

std::vector<std::string> split(
    const char* sep, const std::string& str, std::string::size_type limit) {
    // call base method with new std::vector
    std::vector<std::string> out;
    split(&out, sep, str, limit);
    return out;
}

std::vector<std::string> split(const std::string& sep, const std::string& str,
                               std::string::size_type limit) {
    // call base method with new std::vector
    std::vector<std::string> out;
    split(&out, sep, str, limit);
    return out;
}

/******************************************************************************/
// split() returning std::vector<std::string> with minimum fields

std::vector<std::string> split(
    char sep, const std::string& str,
    std::string::size_type min_fields, std::string::size_type limit) {
    // call base method with new std::vector
    std::vector<std::string> out;
    split(&out, sep, str, min_fields, limit);
    return out;
}

std::vector<std::string> split(
    const char* sep, const std::string& str,
    std::string::size_type min_fields, std::string::size_type limit) {
    // call base method with new std::vector
    std::vector<std::string> out;
    split(&out, sep, str, min_fields, limit);
    return out;
}

std::vector<std::string> split(
    const std::string& sep, const std::string& str,
    std::string::size_type min_fields, std::string::size_type limit) {
    // call base method with new std::vector
    std::vector<std::string> out;
    split(&out, sep, str, min_fields, limit);
    return out;
}

/******************************************************************************/
// split() into std::vector<std::string>

std::vector<std::string>& split(
    std::vector<std::string>* into,
    char sep, const std::string& str, std::string::size_type limit) {

    into->clear();
    if (limit == 0) return *into;

    std::string::const_iterator it = str.begin(), last = it;

    for ( ; it != str.end(); ++it)
    {
        if (*it == sep)
        {
            if (into->size() + 1 >= limit)
            {
                into->emplace_back(last, str.end());
                return *into;
            }

            into->emplace_back(last, it);
            last = it + 1;
        }
    }

    into->emplace_back(last, it);

    return *into;
}

static inline
std::vector<std::string>& split(
    std::vector<std::string>* into,
    const char* sep, size_t sep_size, const std::string& str,
    std::string::size_type limit) {

    into->clear();
    if (limit == 0) return *into;

    if (sep_size == 0)
    {
        std::string::const_iterator it = str.begin();
        while (it != str.end()) {
            into->emplace_back(it, it + 1);
            ++it;
        }
        return *into;
    }

    std::string::const_iterator it = str.begin(), last = it;

    for ( ; it + sep_size < str.end(); ++it)
    {
        if (std::equal(sep, sep + sep_size, it))
        {
            if (into->size() + 1 >= limit)
            {
                into->emplace_back(last, str.end());
                return *into;
            }

            into->emplace_back(last, it);
            last = it + sep_size;
        }
    }

    into->emplace_back(last, str.end());

    return *into;
}

std::vector<std::string>& split(
    std::vector<std::string>* into,
    const char* sep, const std::string& str,
    std::string::size_type limit) {
    // call base method
    return split(into, sep, strlen(sep), str, limit);
}

std::vector<std::string>& split(
    std::vector<std::string>* into,
    const std::string& sep, const std::string& str,
    std::string::size_type limit) {
    // call base method
    return split(into, sep.data(), sep.size(), str, limit);
}

/******************************************************************************/
// split() into std::vector<std::string> with minimum fields

std::vector<std::string>& split(
    std::vector<std::string>* into,
    char sep, const std::string& str,
    std::string::size_type min_fields, std::string::size_type limit) {
    // call base method
    split(into, sep, str, limit);

    if (into->size() < min_fields)
        into->resize(min_fields);

    return *into;
}

std::vector<std::string>& split(
    std::vector<std::string>* into,
    const char* sep, const std::string& str,
    std::string::size_type min_fields, std::string::size_type limit) {
    // call base method
    split(into, sep, str, limit);

    if (into->size() < min_fields)
        into->resize(min_fields);

    return *into;
}

std::vector<std::string>& split(
    std::vector<std::string>* into,
    const std::string& sep, const std::string& str,
    std::string::size_type min_fields, std::string::size_type limit) {
    // call base method
    split(into, sep, str, limit);

    if (into->size() < min_fields)
        into->resize(min_fields);

    return *into;
}

} // namespace tlx

/******************************************************************************/
