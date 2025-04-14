/*******************************************************************************
 * tlx/string/trim.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/string/trim.hpp>

#include <algorithm>
#include <cstring>

namespace tlx {

/******************************************************************************/

std::string& trim(std::string* str) {
    return trim(str, " \r\n\t");
}

std::string& trim(std::string* str, const char* drop) {
    std::string::size_type pos = str->find_last_not_of(drop);
    if (pos != std::string::npos) {
        str->erase(pos + 1);
        pos = str->find_first_not_of(drop);
        if (pos != std::string::npos) str->erase(0, pos);
    }
    else
        str->erase(str->begin(), str->end());

    return *str;
}

std::string& trim(std::string* str, const std::string& drop) {
    std::string::size_type pos = str->find_last_not_of(drop);
    if (pos != std::string::npos) {
        str->erase(pos + 1);
        pos = str->find_first_not_of(drop);
        if (pos != std::string::npos) str->erase(0, pos);
    }
    else
        str->erase(str->begin(), str->end());

    return *str;
}

std::string trim(const std::string& str) {
    return trim(str, " \r\n\t");
}

std::string trim(const std::string& str, const char* drop) {
    // trim beginning
    std::string::size_type pos1 = str.find_first_not_of(drop);
    if (pos1 == std::string::npos) return std::string();

    // copy middle and end
    std::string out = str.substr(pos1, std::string::npos);

    // trim end
    std::string::size_type pos2 = out.find_last_not_of(drop);
    if (pos2 != std::string::npos)
        out.erase(pos2 + 1);

    return out;
}

std::string trim(const std::string& str,
                 const std::string& drop) {
    std::string out;
    out.reserve(str.size());

    // trim beginning
    std::string::const_iterator it = str.begin();
    while (it != str.end() && drop.find(*it) != std::string::npos)
        ++it;

    // copy middle and end
    out.resize(str.end() - it);
    std::copy(it, str.end(), out.begin());

    // trim end
    std::string::size_type pos = out.find_last_not_of(drop);
    if (pos != std::string::npos)
        out.erase(pos + 1);

    return out;
}

/******************************************************************************/

std::string& trim_right(std::string* str) {
    return trim_right(str, " \r\n\t");
}

std::string& trim_right(std::string* str, const char* drop) {
    str->erase(str->find_last_not_of(drop) + 1, std::string::npos);
    return *str;
}

std::string& trim_right(std::string* str, const std::string& drop) {
    str->erase(str->find_last_not_of(drop) + 1, std::string::npos);
    return *str;
}

std::string trim_right(const std::string& str) {
    return trim_right(str, " \r\n\t");
}

std::string trim_right(const std::string& str, const char* drop) {
    std::string::size_type pos = str.find_last_not_of(drop);
    if (pos == std::string::npos) return std::string();

    return str.substr(0, pos + 1);
}

std::string trim_right(const std::string& str, const std::string& drop) {
    std::string::size_type pos = str.find_last_not_of(drop);
    if (pos == std::string::npos) return std::string();

    return str.substr(0, pos + 1);
}

/******************************************************************************/

std::string& trim_left(std::string* str) {
    return trim_left(str, " \r\n\t");
}

std::string& trim_left(std::string* str, const char* drop) {
    str->erase(0, str->find_first_not_of(drop));
    return *str;
}

std::string& trim_left(std::string* str, const std::string& drop) {
    str->erase(0, str->find_first_not_of(drop));
    return *str;
}

std::string trim_left(const std::string& str) {
    return trim_left(str, " \r\n\t");
}

std::string trim_left(const std::string& str, const char* drop) {
    std::string::size_type pos = str.find_first_not_of(drop);
    if (pos == std::string::npos) return std::string();

    return str.substr(pos, std::string::npos);
}

std::string trim_left(const std::string& str, const std::string& drop) {
    std::string::size_type pos = str.find_first_not_of(drop);
    if (pos == std::string::npos) return std::string();

    return str.substr(pos, std::string::npos);
}

} // namespace tlx

/******************************************************************************/
