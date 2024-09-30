/*******************************************************************************
 * tlx/string/ssprintf.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2019 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_SSPRINTF_HEADER
#define TLX_STRING_SSPRINTF_HEADER

#include <tlx/define/attribute_format_printf.hpp>

#include <string>

namespace tlx {

//! \addtogroup tlx_string
//! \{

/*!
 * Helper for return the result of a sprintf() call inside a std::string.
 *
 * \param fmt printf format and additional parameters
 */
std::string ssprintf(const char* fmt, ...)
TLX_ATTRIBUTE_FORMAT_PRINTF(1, 2);

/*!
 * Helper for return the result of a snprintf() call inside a std::string.
 *
 * \param max_size maximum length of output string, longer ones are truncated.
 * \param fmt printf format and additional parameters
 */
std::string ssnprintf(size_t max_size, const char* fmt, ...)
TLX_ATTRIBUTE_FORMAT_PRINTF(2, 3);

//! \}

} // namespace tlx

#endif // !TLX_STRING_SSPRINTF_HEADER

/******************************************************************************/
