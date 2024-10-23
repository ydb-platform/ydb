/*******************************************************************************
 * tlx/string/base64.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/string/base64.hpp>

#include <cstdint>
#include <stdexcept>

namespace tlx {

/*
 * Code in this file is based on source code from http://libb64.sourceforge.net/
 * which is in the public domain.
 */

/******************************************************************************/
// Base64 Encoding and Decoding

std::string base64_encode(const void* data, size_t size, size_t line_break) {
    const std::uint8_t* in = reinterpret_cast<const std::uint8_t*>(data);
    const std::uint8_t* in_end = in + size;
    std::string out;

    if (size == 0) return out;

    // calculate output string's size in advance
    size_t outsize = (((size - 1) / 3) + 1) * 4;
    if (line_break > 0) outsize += outsize / line_break;
    out.reserve(outsize);

    static const char encoding64[64] = {
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
        'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
        'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'
    };

    std::uint8_t result = 0;
    size_t line_begin = 0;

    while (true)
    {
        // step 0: if the string is finished here, no padding is needed
        if (in == in_end) {
            return out;
        }

        // step 0: process first byte, write first letter
        std::uint8_t fragment = *in++;
        result = (fragment & 0xFC) >> 2;
        out += encoding64[result];
        result = static_cast<std::uint8_t>((fragment & 0x03) << 4);

        // step 1: if string finished here, add two padding '='s
        if (in == in_end) {
            out += encoding64[result];
            out += '=';
            out += '=';
            return out;
        }

        // step 1: process second byte together with first, write second
        // letter
        fragment = *in++;
        result |= (fragment & 0xF0) >> 4;
        out += encoding64[result];
        result = static_cast<std::uint8_t>((fragment & 0x0F) << 2);

        // step 2: if string finished here, add one padding '='
        if (in == in_end) {
            out += encoding64[result];
            out += '=';
            return out;
        }

        // step 2: process third byte and write third and fourth letters.
        fragment = *in++;

        result |= (fragment & 0xC0) >> 6;
        out += encoding64[result];

        result = (fragment & 0x3F) >> 0;
        out += encoding64[result];

        // wrap base64 encoding into lines if desired, but only after whole
        // blocks of 4 letters.
        if (line_break > 0 && out.size() - line_begin >= line_break)
        {
            out += '\n';
            line_begin = out.size();
        }
    }
}

std::string base64_encode(const std::string& str, size_t line_break) {
    return base64_encode(str.data(), str.size(), line_break);
}

/******************************************************************************/

std::string base64_decode(const void* data, size_t size, bool strict) {
    const std::uint8_t* in = reinterpret_cast<const std::uint8_t*>(data);
    const std::uint8_t* in_end = in + size;
    std::string out;

    // estimate the output size, assume that the whole input string is
    // base64 encoded.
    out.reserve(size * 3 / 4);

    static constexpr std::uint8_t ex = 255;
    static constexpr std::uint8_t ws = 254;
    // value lookup table: -1 -> exception, -2 -> skip whitespace
    static const std::uint8_t decoding64[256] = {
        ex, ex, ex, ex, ex, ex, ex, ex, ex, ws, ws, ex, ex, ws, ex, ex,
        ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex,
        ws, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, 62, ex, ex, ex, 63,
        52, 53, 54, 55, 56, 57, 58, 59, 60, 61, ex, ex, ex, ws, ex, ex,
        ex, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
        15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, ex, ex, ex, ex, ex,
        ex, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
        41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, ex, ex, ex, ex, ex,
        ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex,
        ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex,
        ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex,
        ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex,
        ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex,
        ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex,
        ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex,
        ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex, ex
    };

    std::uint8_t outchar, fragment;

    static const char* ex_message =
        "Invalid character encountered during base64 decoding.";

    while (true)
    {
        // step 0: save first valid letter. do not output a byte, yet.
        do {
            if (in == in_end) return out;

            fragment = decoding64[*in++];

            if (fragment == ex && strict)
                throw std::runtime_error(ex_message);
        } while (fragment >= ws);

        outchar = static_cast<std::uint8_t>((fragment & 0x3F) << 2);

        // step 1: get second valid letter. output the first byte.
        do {
            if (in == in_end) return out;

            fragment = decoding64[*in++];

            if (fragment == ex && strict)
                throw std::runtime_error(ex_message);
        } while (fragment >= ws);

        outchar = static_cast<std::uint8_t>(outchar | ((fragment & 0x30) >> 4));
        out += static_cast<char>(outchar);

        outchar = static_cast<std::uint8_t>((fragment & 0x0F) << 4);

        // step 2: get third valid letter. output the second byte.
        do {
            if (in == in_end) return out;

            fragment = decoding64[*in++];

            if (fragment == ex && strict)
                throw std::runtime_error(ex_message);
        } while (fragment >= ws);

        outchar = static_cast<std::uint8_t>(outchar | ((fragment & 0x3C) >> 2));
        out += static_cast<char>(outchar);

        outchar = static_cast<std::uint8_t>((fragment & 0x03) << 6);

        // step 3: get fourth valid letter. output the third byte.
        do {
            if (in == in_end) return out;

            fragment = decoding64[*in++];

            if (fragment == ex && strict)
                throw std::runtime_error(ex_message);
        } while (fragment >= ws);

        outchar = static_cast<std::uint8_t>(outchar | ((fragment & 0x3F) >> 0));
        out += static_cast<char>(outchar);
    }
}

std::string base64_decode(const std::string& str, bool strict) {
    return base64_decode(str.data(), str.size(), strict);
}

} // namespace tlx

/******************************************************************************/
