/*******************************************************************************
 * tlx/string/escape_uri.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/string/escape_uri.hpp>

#include <cstring>

namespace tlx {

std::string escape_uri(const std::string& str) {
    std::string result;
    result.reserve(str.size() + str.size() / 16);

    for (std::string::const_iterator it = str.begin(); it != str.end(); ++it)
    {
        switch (*it) {
        // alnum
        case 'A': case 'B': case 'C': case 'D': case 'E': case 'F': case 'G':
        case 'H': case 'I': case 'J': case 'K': case 'L': case 'M': case 'N':
        case 'O': case 'P': case 'Q': case 'R': case 'S': case 'T': case 'U':
        case 'V': case 'W': case 'X': case 'Y': case 'Z':
        case 'a': case 'b': case 'c': case 'd': case 'e': case 'f': case 'g':
        case 'h': case 'i': case 'j': case 'k': case 'l': case 'm': case 'n':
        case 'o': case 'p': case 'q': case 'r': case 's': case 't': case 'u':
        case 'v': case 'w': case 'x': case 'y': case 'z':
        case '0': case '1': case '2': case '3': case '4': case '5': case '6':
        case '7': case '8': case '9':
        // mark
        case '-': case '_': case '.': case '~':
            result.append(1, *it);
            break;
        // escape
        default: {
            char first = (*it & 0xF0) / 16;
            first += first > 9 ? 'A' - 10 : '0';
            char second = *it & 0x0F;
            second += second > 9 ? 'A' - 10 : '0';

            result.append(1, '%');
            result.append(1, first);
            result.append(1, second);
            break;
        }
        }
    }

    return result;
}

std::string escape_uri(const char* str) {
    size_t slen = strlen(str);
    std::string result;
    result.reserve(slen + slen / 16);

    for (const char* it = str; *it != 0; ++it)
    {
        switch (*it) {
        // alnum
        case 'A': case 'B': case 'C': case 'D': case 'E': case 'F': case 'G':
        case 'H': case 'I': case 'J': case 'K': case 'L': case 'M': case 'N':
        case 'O': case 'P': case 'Q': case 'R': case 'S': case 'T': case 'U':
        case 'V': case 'W': case 'X': case 'Y': case 'Z':
        case 'a': case 'b': case 'c': case 'd': case 'e': case 'f': case 'g':
        case 'h': case 'i': case 'j': case 'k': case 'l': case 'm': case 'n':
        case 'o': case 'p': case 'q': case 'r': case 's': case 't': case 'u':
        case 'v': case 'w': case 'x': case 'y': case 'z':
        case '0': case '1': case '2': case '3': case '4': case '5': case '6':
        case '7': case '8': case '9':
        // mark
        case '-': case '_': case '.': case '~':
            result.append(1, *it);
            break;
        // escape
        default: {
            char first = (*it & 0xF0) / 16;
            first += first > 9 ? 'A' - 10 : '0';
            char second = *it & 0x0F;
            second += second > 9 ? 'A' - 10 : '0';

            result.append(1, '%');
            result.append(1, first);
            result.append(1, second);
            break;
        }
        }
    }

    return result;
}

} // namespace tlx

/******************************************************************************/
