/*******************************************************************************
 * tlx/string/contains.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_CONTAINS_HEADER
#define TLX_STRING_CONTAINS_HEADER

#include <string>

namespace tlx {

//! \addtogroup tlx_string
//! \{

/******************************************************************************/
// contains()

//! Tests of string contains pattern
bool contains(const std::string& str, const std::string& pattern);

//! Tests of string contains pattern
bool contains(const std::string& str, const char* pattern);

//! Tests of string contains character
bool contains(const std::string& str, const char ch);

//! \}

} // namespace tlx

#endif // !TLX_STRING_CONTAINS_HEADER

/******************************************************************************/
