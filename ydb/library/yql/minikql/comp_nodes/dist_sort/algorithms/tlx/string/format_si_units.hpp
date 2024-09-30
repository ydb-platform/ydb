/*******************************************************************************
 * tlx/string/format_si_units.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_FORMAT_SI_UNITS_HEADER
#define TLX_STRING_FORMAT_SI_UNITS_HEADER

#include <cstdint>
#include <string>

namespace tlx {

//! \addtogroup tlx_string
//! \{

//! Format a byte size using SI (K, M, G, T) suffixes (powers of ten). Returns
//! "123 M" or similar.
std::string format_si_units(std::uint64_t number, int precision = 3);

//! \}

} // namespace tlx

#endif // !TLX_STRING_FORMAT_SI_UNITS_HEADER

/******************************************************************************/
