/*******************************************************************************
 * tlx/string/pad.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_PAD_HEADER
#define TLX_STRING_PAD_HEADER

#include <string>

namespace tlx {

//! \addtogroup tlx_string
//! \{

/*!
 * Truncate or pad string to exactly len characters.
 */
std::string pad(const std::string& s, size_t len, char pad_char = ' ');

//! \}

} // namespace tlx

#endif // !TLX_STRING_PAD_HEADER

/******************************************************************************/
