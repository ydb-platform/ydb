/*******************************************************************************
 * tlx/string/compare_icase.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/string/compare_icase.hpp>
#include <tlx/string/to_lower.hpp>

#include <algorithm>

namespace tlx {

int compare_icase(const char* a, const char* b) {

    while (*a != 0 && *b != 0)
    {
        int ca = to_lower(*a++);
        int cb = to_lower(*b++);

        if (ca == cb) continue;
        if (ca < cb) return -1;
        else return +1;
    }

    if (*a == 0 && *b != 0) return +1;
    else if (*a != 0 && *b == 0) return -1;
    else return 0;
}

int compare_icase(const char* a, const std::string& b) {

    std::string::const_iterator bi = b.begin();

    while (*a != 0 && bi != b.end())
    {
        int ca = to_lower(*a++);
        int cb = to_lower(*bi++);

        if (ca == cb) continue;
        if (ca < cb) return -1;
        else return +1;
    }

    if (*a == 0 && bi != b.end()) return +1;
    else if (*a != 0 && bi == b.end()) return -1;
    else return 0;
}

int compare_icase(const std::string& a, const char* b) {
    std::string::const_iterator ai = a.begin();

    while (ai != a.end() && *b != 0)
    {
        int ca = to_lower(*ai++);
        int cb = to_lower(*b++);

        if (ca == cb) continue;
        if (ca < cb) return -1;
        else return +1;
    }

    if (ai == a.end() && *b != 0) return +1;
    else if (ai != a.end() && *b == 0) return -1;
    else return 0;
}

int compare_icase(const std::string& a, const std::string& b) {
    std::string::const_iterator ai = a.begin();
    std::string::const_iterator bi = b.begin();

    while (ai != a.end() && bi != b.end())
    {
        int ca = to_lower(*ai++);
        int cb = to_lower(*bi++);

        if (ca == cb) continue;
        if (ca < cb) return -1;
        else return +1;
    }

    if (ai == a.end() && bi != b.end()) return +1;
    else if (ai != a.end() && bi == b.end()) return -1;
    else return 0;
}

} // namespace tlx

/******************************************************************************/
