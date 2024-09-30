/*******************************************************************************
 * tlx/string/erase_all.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/string/erase_all.hpp>

namespace tlx {

/******************************************************************************/
// erase_all() in-place

std::string& erase_all(std::string* str, char drop) {
    std::string::size_type pos1 = std::string::npos, pos2;

    while ((pos1 = str->find_last_of(drop, pos1)) != std::string::npos) {
        pos2 = str->find_last_not_of(drop, pos1);
        if (pos2 == std::string::npos) {
            str->erase(0, pos1 - pos2);
            return *str;
        }
        str->erase(pos2 + 1, pos1 - pos2);
        pos1 = pos2;
    }

    return *str;
}

std::string& erase_all(std::string* str, const char* drop) {
    std::string::size_type pos1 = std::string::npos, pos2;

    while ((pos1 = str->find_last_of(drop, pos1)) != std::string::npos) {
        pos2 = str->find_last_not_of(drop, pos1);
        if (pos2 == std::string::npos) {
            str->erase(0, pos1 - pos2);
            return *str;
        }
        str->erase(pos2 + 1, pos1 - pos2);
        pos1 = pos2;
    }

    return *str;
}

std::string& erase_all(std::string* str, const std::string& drop) {
    return erase_all(str, drop.c_str());
}

/******************************************************************************/
// erase_all() copy

std::string erase_all(const std::string& str, char drop) {
    std::string out;
    out.reserve(str.size());

    std::string::const_iterator si = str.begin();
    while (si != str.end()) {
        if (*si != drop)
            out += *si;
        ++si;
    }

    return out;
}

std::string erase_all(const std::string& str, const char* drop) {
    std::string out;
    out.reserve(str.size());

    std::string::const_iterator si = str.begin();
    while (si != str.end()) {
        // search for letter
        const char* d = drop;
        while (*d != 0) {
            if (*si == *d) break;
            ++d;
        }
        // append if not found
        if (*d == 0)
            out += *si;
        ++si;
    }

    return out;
}

std::string erase_all(const std::string& str, const std::string& drop) {
    return erase_all(str, drop.c_str());
}

} // namespace tlx

/******************************************************************************/
