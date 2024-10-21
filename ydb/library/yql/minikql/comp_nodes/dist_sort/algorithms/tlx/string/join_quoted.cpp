/*******************************************************************************
 * tlx/string/join_quoted.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/string/join_quoted.hpp>

namespace tlx {

std::string join_quoted(
    const std::vector<std::string>& vec, char sep, char quote, char escape) {

    std::string out;
    if (vec.empty()) return out;

    for (size_t i = 0; i < vec.size(); ++i) {
        if (i != 0)
            out += sep;

        if (vec[i].find(sep) != std::string::npos) {
            out += quote;
            for (std::string::const_iterator it = vec[i].begin();
                 it != vec[i].end(); ++it) {
                if (*it == quote || *it == escape) {
                    out += escape, out += *it;
                }
                else if (*it == '\n') {
                    out += escape, out += 'n';
                }
                else if (*it == '\r') {
                    out += escape, out += 'r';
                }
                else if (*it == '\t') {
                    out += escape, out += 't';
                }
                else {
                    out += *it;
                }
            }
            out += quote;
        }
        else {
            out += vec[i];
        }
    }

    return out;
}

std::string join_quoted(const std::vector<std::string>& vec) {
    return join_quoted(vec, ' ', '"', '\\');
}

} // namespace tlx

/******************************************************************************/
