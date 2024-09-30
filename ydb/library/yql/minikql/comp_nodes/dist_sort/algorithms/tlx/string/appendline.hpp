/*******************************************************************************
 * tlx/string/appendline.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2019 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_APPENDLINE_HEADER
#define TLX_STRING_APPENDLINE_HEADER

#include <istream>
#include <string>

namespace tlx {

//! \addtogroup tlx_string
//! \{

/******************************************************************************/
// appendline()

//! like std::getline(istream, string, delim) except that it appends to the
//! string, possibly reusing buffer capacity.
std::istream& appendline(std::istream& is, std::string& str, char delim = '\n');

//! \}

} // namespace tlx

#endif // !TLX_STRING_APPENDLINE_HEADER

/******************************************************************************/
