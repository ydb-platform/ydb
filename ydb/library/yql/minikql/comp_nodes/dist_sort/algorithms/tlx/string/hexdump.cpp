/*******************************************************************************
 * tlx/string/hexdump.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/string/hexdump.hpp>

#include <cstdint>
#include <sstream>
#include <stdexcept>

namespace tlx {

/******************************************************************************/
// Uppercase Hexdump Methods

std::string hexdump(const void* const data, size_t size) {
    const unsigned char* const cdata =
        static_cast<const unsigned char*>(data);

    std::string out;
    out.resize(size * 2);

    static const char xdigits[16] = {
        '0', '1', '2', '3', '4', '5', '6', '7',
        '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
    };

    std::string::iterator oi = out.begin();
    for (const unsigned char* si = cdata; si != cdata + size; ++si) {
        *oi++ = xdigits[(*si & 0xF0) >> 4];
        *oi++ = xdigits[(*si & 0x0F)];
    }

    return out;
}

std::string hexdump(const std::string& str) {
    return hexdump(str.data(), str.size());
}

std::string hexdump(const std::vector<char>& data) {
    return hexdump(data.data(), data.size());
}

std::string hexdump(const std::vector<std::uint8_t>& data) {
    return hexdump(data.data(), data.size());
}

std::string hexdump_sourcecode(
    const std::string& str, const std::string& var_name) {

    std::ostringstream header;
    header << "const std::uint8_t " << var_name << "[" << str.size() << "] = {\n";

    static const int perline = 16;

    std::string out = header.str();
    out.reserve(out.size() + (str.size() * 5) - 1 + (str.size() / 16) + 4);

    static const char xdigits[16] = {
        '0', '1', '2', '3', '4', '5', '6', '7',
        '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
    };

    std::string::size_type ci = 0;

    for (std::string::const_iterator si = str.begin(); si != str.end();
         ++si, ++ci) {

        out += "0x";
        out += xdigits[(*si & 0xF0) >> 4];
        out += xdigits[(*si & 0x0F)];

        if (ci + 1 < str.size()) {
            out += ',';

            if (ci % perline == perline - 1)
                out += '\n';
        }
    }

    out += "\n};\n";

    return out;
}

/******************************************************************************/
// Lowercase Hexdump Methods

std::string hexdump_lc(const void* const data, size_t size) {
    const unsigned char* const cdata =
        static_cast<const unsigned char*>(data);

    std::string out;
    out.resize(size * 2);

    static const char xdigits[16] = {
        '0', '1', '2', '3', '4', '5', '6', '7',
        '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
    };

    std::string::iterator oi = out.begin();
    for (const unsigned char* si = cdata; si != cdata + size; ++si) {
        *oi++ = xdigits[(*si & 0xF0) >> 4];
        *oi++ = xdigits[(*si & 0x0F)];
    }

    return out;
}

std::string hexdump_lc(const std::string& str) {
    return hexdump_lc(str.data(), str.size());
}

std::string hexdump_lc(const std::vector<char>& data) {
    return hexdump_lc(data.data(), data.size());
}

std::string hexdump_lc(const std::vector<std::uint8_t>& data) {
    return hexdump_lc(data.data(), data.size());
}

/******************************************************************************/
// Parser for Hex Digit Sequence

std::string parse_hexdump(const std::string& str) {
    std::string out;

    for (std::string::const_iterator si = str.begin(); si != str.end(); ++si) {

        unsigned char c = 0;

        // read first character of pair
        switch (*si) {
        case '0': c |= 0x00;
            break;
        case '1': c |= 0x10;
            break;
        case '2': c |= 0x20;
            break;
        case '3': c |= 0x30;
            break;
        case '4': c |= 0x40;
            break;
        case '5': c |= 0x50;
            break;
        case '6': c |= 0x60;
            break;
        case '7': c |= 0x70;
            break;
        case '8': c |= 0x80;
            break;
        case '9': c |= 0x90;
            break;
        case 'A':
        case 'a': c |= 0xA0;
            break;
        case 'B':
        case 'b': c |= 0xB0;
            break;
        case 'C':
        case 'c': c |= 0xC0;
            break;
        case 'D':
        case 'd': c |= 0xD0;
            break;
        case 'E':
        case 'e': c |= 0xE0;
            break;
        case 'F':
        case 'f': c |= 0xF0;
            break;
        default: throw std::runtime_error("Invalid string for hex conversion");
        }

        ++si;
        if (si == str.end())
            throw std::runtime_error("Invalid string for hex conversion");

        // read second character of pair
        switch (*si) {
        case '0': c |= 0x00;
            break;
        case '1': c |= 0x01;
            break;
        case '2': c |= 0x02;
            break;
        case '3': c |= 0x03;
            break;
        case '4': c |= 0x04;
            break;
        case '5': c |= 0x05;
            break;
        case '6': c |= 0x06;
            break;
        case '7': c |= 0x07;
            break;
        case '8': c |= 0x08;
            break;
        case '9': c |= 0x09;
            break;
        case 'A':
        case 'a': c |= 0x0A;
            break;
        case 'B':
        case 'b': c |= 0x0B;
            break;
        case 'C':
        case 'c': c |= 0x0C;
            break;
        case 'D':
        case 'd': c |= 0x0D;
            break;
        case 'E':
        case 'e': c |= 0x0E;
            break;
        case 'F':
        case 'f': c |= 0x0F;
            break;
        default: throw std::runtime_error("Invalid string for hex conversion");
        }

        out += static_cast<char>(c);
    }

    return out;
}

} // namespace tlx

/******************************************************************************/
