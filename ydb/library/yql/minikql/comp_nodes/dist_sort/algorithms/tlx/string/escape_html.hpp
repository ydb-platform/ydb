/*******************************************************************************
 * tlx/string/escape_html.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_ESCAPE_HTML_HEADER
#define TLX_STRING_ESCAPE_HTML_HEADER

#include <string>

namespace tlx {

//! \addtogroup tlx_string
//! \{

/*!
 * Escape characters for inclusion in HTML documents: replaces the characters <,
 * >, & and " with HTML entities.
 */
std::string escape_html(const std::string& str);

/*!
 * Escape characters for inclusion in HTML documents: replaces the characters <,
 * >, & and " with HTML entities.
 */
std::string escape_html(const char* str);

//! \}

} // namespace tlx

#endif // !TLX_STRING_ESCAPE_HTML_HEADER

/******************************************************************************/
