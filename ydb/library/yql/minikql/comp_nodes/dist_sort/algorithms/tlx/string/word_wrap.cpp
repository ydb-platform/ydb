/*******************************************************************************
 * tlx/string/word_wrap.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/string/word_wrap.hpp>

#include <cctype>

namespace tlx {

bool is_space(char ch) {
    return ch == ' ' || ch == '\f' || ch == '\t' ||
           ch == '\r' || ch == '\n' || ch == '\v';
}

std::string word_wrap(const std::string& str, unsigned int wrap) {
    std::string out;
    out.resize(str.size());

    std::string::size_type i = 0, last_space;

    while (i < str.size())
    {
        last_space = std::string::npos;

        // copy string until the end of the line is reached
        for (std::string::size_type count = 0; count < wrap; ++count)
        {
            if (i == str.size()) {
                // end of string reached
                return out;
            }

            out[i] = str[i];

            // check for newlines in input and reset counter
            if (out[i] == '\n')
                count = 0;
            // save last space position
            if (is_space(out[i]))
                last_space = i;

            ++i;
        }

        if (last_space != std::string::npos)
        {
            // turn last space into newline and step counter back
            out[last_space] = '\n';

            if (i == str.size()) {
                // end of string reached
                return out;
            }

            i = last_space + 1;
        }
        else
        {
            // no space in last line, copy until we find one
            while (i != str.size() && !is_space(str[i]))
                out[i] = str[i], ++i;

            if (i == str.size()) {
                // end of string reached
                return out;
            }

            out[i] = '\n';
            ++i;
        }
    }

    return out;
}

} // namespace tlx

/******************************************************************************/
