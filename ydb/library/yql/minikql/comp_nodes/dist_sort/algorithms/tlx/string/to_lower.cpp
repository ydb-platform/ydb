/*******************************************************************************
 * tlx/string/to_lower.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/string/to_lower.hpp>

#include <algorithm>

namespace tlx {

char to_lower(char ch) {
    if (static_cast<unsigned>(ch - 'A') < 26u)
        ch = static_cast<char>(ch - 'A' + 'a');
    return ch;
}

std::string& to_lower(std::string* str) {
    std::transform(str->begin(), str->end(), str->begin(),
                   [](char c) { return to_lower(c); });
    return *str;
}

std::string to_lower(const std::string& str) {
    std::string str_copy(str.size(), 0);
    std::transform(str.begin(), str.end(), str_copy.begin(),
                   [](char c) { return to_lower(c); });
    return str_copy;
}

} // namespace tlx

/******************************************************************************/
