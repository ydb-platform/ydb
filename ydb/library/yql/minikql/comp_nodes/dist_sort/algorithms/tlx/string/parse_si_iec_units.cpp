/*******************************************************************************
 * tlx/string/parse_si_iec_units.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/string/parse_si_iec_units.hpp>

#include <cstdint>
#include <cstdlib>

namespace tlx {

bool parse_si_iec_units(
    const char* str, std::uint64_t* out_size, char default_unit) {

    char* endptr;
    *out_size = strtoul(str, &endptr, 10);
    if (endptr == nullptr) return false;        // parse failed, no number

    while (*endptr == ' ') ++endptr;            // skip over spaces

    // multiply with base ^ power
    unsigned int base = 1000;
    unsigned int power = 0;

    if (*endptr == 'k' || *endptr == 'K')
        power = 1, ++endptr;
    else if (*endptr == 'm' || *endptr == 'M')
        power = 2, ++endptr;
    else if (*endptr == 'g' || *endptr == 'G')
        power = 3, ++endptr;
    else if (*endptr == 't' || *endptr == 'T')
        power = 4, ++endptr;
    else if (*endptr == 'p' || *endptr == 'P')
        power = 5, ++endptr;

    // switch to power of two (only if power was set above)
    if ((*endptr == 'i' || *endptr == 'I') && power != 0)
        base = 1024, ++endptr;

    // byte indicator
    if (*endptr == 'b' || *endptr == 'B') {
        ++endptr;
    }
    else if (power == 0)
    {
        // no explicit power indicator, and no 'b' or 'B' -> apply default unit
        switch (default_unit)
        {
        default: break;
        case 'k': power = 1, base = 1000;
            break;
        case 'm': power = 2, base = 1000;
            break;
        case 'g': power = 3, base = 1000;
            break;
        case 't': power = 4, base = 1000;
            break;
        case 'p': power = 5, base = 1000;
            break;
        case 'K': power = 1, base = 1024;
            break;
        case 'M': power = 2, base = 1024;
            break;
        case 'G': power = 3, base = 1024;
            break;
        case 'T': power = 4, base = 1024;
            break;
        case 'P': power = 5, base = 1024;
            break;
        }
    }

    // skip over spaces
    while (*endptr == ' ') ++endptr;

    // multiply size
    for (unsigned int p = 0; p < power; ++p)
        *out_size *= base;

    return (*endptr == 0);
}

bool parse_si_iec_units(
    const std::string& str, std::uint64_t* out_size, char default_unit) {
    return parse_si_iec_units(str.c_str(), out_size, default_unit);
}

} // namespace tlx

/******************************************************************************/
