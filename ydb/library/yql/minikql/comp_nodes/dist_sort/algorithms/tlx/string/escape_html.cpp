/*******************************************************************************
 * tlx/string/escape_html.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/string/escape_html.hpp>

#include <cstring>

namespace tlx {

std::string escape_html(const std::string& str) {
    std::string os;
    os.reserve(str.size() + str.size() / 16);

    for (std::string::const_iterator si = str.begin(); si != str.end(); ++si)
    {
        if (*si == '&') os += "&amp;";
        else if (*si == '<') os += "&lt;";
        else if (*si == '>') os += "&gt;";
        else if (*si == '"') os += "&quot;";
        else os += *si;
    }

    return os;
}

std::string escape_html(const char* str) {
    size_t slen = strlen(str);
    std::string os;
    os.reserve(slen + slen / 16);

    for (const char* si = str; *si != 0; ++si)
    {
        if (*si == '&') os += "&amp;";
        else if (*si == '<') os += "&lt;";
        else if (*si == '>') os += "&gt;";
        else if (*si == '"') os += "&quot;";
        else os += *si;
    }

    return os;
}

} // namespace tlx

/******************************************************************************/
