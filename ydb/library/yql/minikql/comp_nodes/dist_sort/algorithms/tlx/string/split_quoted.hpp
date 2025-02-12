/*******************************************************************************
 * tlx/string/split_quoted.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_SPLIT_QUOTED_HEADER
#define TLX_STRING_SPLIT_QUOTED_HEADER

#include <string>
#include <vector>

namespace tlx {

//! \addtogroup tlx_string
//! \{
//! \name Split and Join
//! \{

/*!
 * Split the given string at each separator character into distinct
 * substrings. Multiple separators are joined and will not result in empty split
 * substrings. Quoted fields extend to the next quote. Quoted fields may
 * containg escaped quote, and \\n \\r \\t \\\\ sequences.
 */
std::vector<std::string> split_quoted(
    const std::string& str, char sep, char quote, char escape);

/*!
 * Split the given string at each space into distinct substrings. Multiple
 * spaces are joined and will not result in empty split substrings. Quoted
 * fields extend to the next quote. Quoted fields may containg escaped quote,
 * and \\n \\r \\t \\\\ sequences.
 */
std::vector<std::string> split_quoted(const std::string& str);

//! \}
//! \}

} // namespace tlx

#endif // !TLX_STRING_SPLIT_QUOTED_HEADER

/******************************************************************************/
