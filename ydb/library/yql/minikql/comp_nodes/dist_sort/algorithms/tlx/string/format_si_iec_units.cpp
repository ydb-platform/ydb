/*******************************************************************************
 * tlx/string/format_si_iec_units.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/string/format_si_iec_units.hpp>

#include <cstdint>
#include <iomanip>
#include <sstream>

namespace tlx {

//! Format number as something like 1 TB
std::string format_si_units(std::uint64_t number, int precision) {
    // may not overflow, std::numeric_limits<std::uint64_t>::max() == 16 EiB
    double multiplier = 1000.0;
    static const char* SI_endings[] = {
        "", "k", "M", "G", "T", "P", "E"
    };
    unsigned int scale = 0;
    double number_d = static_cast<double>(number);
    while (number_d >= multiplier) {
        number_d /= multiplier;
        ++scale;
    }
    std::ostringstream out;
    out << std::fixed << std::setprecision(precision) << number_d
        << ' ' << SI_endings[scale];
    return out.str();
}

//! Format number as something like 1 TiB
std::string format_iec_units(std::uint64_t number, int precision) {
    // may not overflow, std::numeric_limits<std::uint64_t>::max() == 16 EiB
    double multiplier = 1024.0;
    static const char* IEC_endings[] = {
        "", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei"
    };
    unsigned int scale = 0;
    double number_d = static_cast<double>(number);
    while (number_d >= multiplier) {
        number_d /= multiplier;
        ++scale;
    }
    std::ostringstream out;
    out << std::fixed << std::setprecision(precision) << number_d
        << ' ' << IEC_endings[scale];
    return out.str();
}

} // namespace tlx

/******************************************************************************/
