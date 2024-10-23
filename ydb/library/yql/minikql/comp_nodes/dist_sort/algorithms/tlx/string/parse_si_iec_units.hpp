/*******************************************************************************
 * tlx/string/parse_si_iec_units.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_PARSE_SI_IEC_UNITS_HEADER
#define TLX_STRING_PARSE_SI_IEC_UNITS_HEADER

#include <cstdint>
#include <string>

namespace tlx {

//! \addtogroup tlx_string
//! \{

/*!
 * Parse a string like "343KB" or "44 GiB" into the corresponding size in
 * bytes. Returns the number of bytes and sets ok = true if the string could be
 * parsed correctly. If no units indicator is given, use def_unit in k/m/g/t/p
 * (powers of ten) or in K/M/G/T/P (power of two).
 */
bool parse_si_iec_units(
    const char* str, std::uint64_t* out_size, char default_unit = 0);

/*!
 * Parse a string like "343KB" or "44 GiB" into the corresponding size in
 * bytes. Returns the number of bytes and sets ok = true if the string could be
 * parsed correctly. If no units indicator is given, use def_unit in k/m/g/t/p
 * (powers of ten) or in K/M/G/T/P (power of two).
 */
bool parse_si_iec_units(
    const std::string& str, std::uint64_t* out_size, char default_unit = 0);

//! \}

} // namespace tlx

#endif // !TLX_STRING_PARSE_SI_IEC_UNITS_HEADER

/******************************************************************************/
