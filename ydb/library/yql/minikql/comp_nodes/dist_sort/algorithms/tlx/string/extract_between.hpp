/*******************************************************************************
 * tlx/string/extract_between.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_EXTRACT_BETWEEN_HEADER
#define TLX_STRING_EXTRACT_BETWEEN_HEADER

#include <string>

namespace tlx {

//! \addtogroup tlx_string
//! \{

/*!
 * Search the string for given start and end separators and extract all
 * characters between the both, if they are found. Otherwise return an empty
 * string.
 *
 * \param str   string to search in
 * \param sep1  start boundary
 * \param sep2  end boundary
 */
std::string extract_between(const std::string& str, const char* sep1,
                            const char* sep2);

/*!
 * Search the string for given start and end separators and extract all
 * characters between the both, if they are found. Otherwise return an empty
 * string.
 *
 * \param str   string to search in
 * \param sep1  start boundary
 * \param sep2  end boundary
 */
std::string extract_between(const std::string& str, const char* sep1,
                            const std::string& sep2);

/*!
 * Search the string for given start and end separators and extract all
 * characters between the both, if they are found. Otherwise return an empty
 * string.
 *
 * \param str   string to search in
 * \param sep1  start boundary
 * \param sep2  end boundary
 */
std::string extract_between(const std::string& str, const std::string& sep1,
                            const char* sep2);

/*!
 * Search the string for given start and end separators and extract all
 * characters between the both, if they are found. Otherwise return an empty
 * string.
 *
 * \param str   string to search in
 * \param sep1  start boundary
 * \param sep2  end boundary
 */
std::string extract_between(const std::string& str, const std::string& sep1,
                            const std::string& sep2);

//! \}

} // namespace tlx

#endif // !TLX_STRING_EXTRACT_BETWEEN_HEADER

/******************************************************************************/
