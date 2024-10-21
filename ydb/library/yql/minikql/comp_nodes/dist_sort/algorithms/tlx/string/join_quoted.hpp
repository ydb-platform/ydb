/*******************************************************************************
 * tlx/string/join_quoted.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_JOIN_QUOTED_HEADER
#define TLX_STRING_JOIN_QUOTED_HEADER

#include <string>
#include <vector>

namespace tlx {

//! \addtogroup tlx_string
//! \{
//! \name Split and Join
//! \{

/*!
 * Join a vector of strings using a separator character. If any string contains
 * the separator, quote the field. In the quoted string, escape all quotes,
 * escapes, \\n, \\r, \\t sequences. This is the opposite of split_quoted().
 */
std::string join_quoted(
    const std::vector<std::string>& str, char sep, char quote, char escape);

/*!
 * Join a vector of strings using spaces as separator character. If any string
 * contains a space, quote the field. In the quoted string, escape all quotes,
 * escapes, \\n, \\r, \\t sequences. This is the opposite of split_quoted().
 */
std::string join_quoted(const std::vector<std::string>& str);

//! \}
//! \}

} // namespace tlx

#endif // !TLX_STRING_JOIN_QUOTED_HEADER

/******************************************************************************/
