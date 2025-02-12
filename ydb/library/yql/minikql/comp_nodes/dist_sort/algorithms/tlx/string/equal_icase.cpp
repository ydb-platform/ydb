/*******************************************************************************
 * tlx/string/equal_icase.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/string/equal_icase.hpp>
#include <tlx/string/to_lower.hpp>

#include <algorithm>

namespace tlx {

bool equal_icase(const char* a, const char* b) {

    while (*a != 0 && *b != 0 && to_lower(*a) == to_lower(*b))
        ++a, ++b;

    return *a == 0 && *b == 0;
}

bool equal_icase(const char* a, const std::string& b) {
    std::string::const_iterator bi = b.begin();

    while (*a != 0 && bi != b.end() && to_lower(*a) == to_lower(*bi))
        ++a, ++bi;

    return *a == 0 && bi == b.end();
}

bool equal_icase(const std::string& a, const char* b) {
    std::string::const_iterator ai = a.begin();

    while (ai != a.end() && *b != 0 && to_lower(*ai) == to_lower(*b))
        ++ai, ++b;

    return ai == a.end() && *b != 0;
}

bool equal_icase(const std::string& a, const std::string& b) {
    if (a.size() != b.size()) return false;

    return std::equal(
        a.begin(), a.end(), b.begin(),
        [](char c1, char c2) { return to_lower(c1) == to_lower(c2); });
}

} // namespace tlx

/******************************************************************************/
