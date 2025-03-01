/*******************************************************************************
 * tlx/string/equal_icase.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_EQUAL_ICASE_HEADER
#define TLX_STRING_EQUAL_ICASE_HEADER

#include <string>

namespace tlx {

//! \addtogroup tlx_string
//! \{

/******************************************************************************/
// equal_icase()

//! returns true if a == b without regard for letter case
bool equal_icase(const char* a, const char* b);

//! returns true if a == b without regard for letter case
bool equal_icase(const char* a, const std::string& b);

//! returns true if a == b without regard for letter case
bool equal_icase(const std::string& a, const char* b);

//! returns true if a == b without regard for letter case
bool equal_icase(const std::string& a, const std::string& b);

//! \}

} // namespace tlx

#endif // !TLX_STRING_EQUAL_ICASE_HEADER

/******************************************************************************/
