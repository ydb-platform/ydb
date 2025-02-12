/*******************************************************************************
 * tlx/string/to_lower.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_TO_LOWER_HEADER
#define TLX_STRING_TO_LOWER_HEADER

#include <string>

namespace tlx {

//! \addtogroup tlx_string
//! \{

//! Transform the given character to lower case without any localization.
char to_lower(char ch);

/*!
 * Transforms the given string to lowercase and returns a reference to it.
 *
 * \param str   string to process
 * \return      reference to the modified string
 */
std::string& to_lower(std::string* str);

/*!
 * Returns a copy of the given string converted to lowercase.
 *
 * \param str   string to process
 * \return      new string lowercased
 */
std::string to_lower(const std::string& str);

//! \}

} // namespace tlx

#endif // !TLX_STRING_TO_LOWER_HEADER

/******************************************************************************/
