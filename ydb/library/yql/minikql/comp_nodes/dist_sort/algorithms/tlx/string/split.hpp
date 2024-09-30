/*******************************************************************************
 * tlx/string/split.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_SPLIT_HEADER
#define TLX_STRING_SPLIT_HEADER

#include <string>
#include <vector>

namespace tlx {

//! \addtogroup tlx_string
//! \{
//! \name Split and Join
//! \{

/******************************************************************************/
// split() returning std::vector<std::string>

/*!
 * Split the given string at each separator character into distinct substrings.
 * Multiple consecutive separators are considered individually and will result
 * in empty split substrings.
 *
 * \param sep    separator character
 * \param str    string to split
 * \param limit  maximum number of parts returned
 * \return       vector containing each split substring
 */
std::vector<std::string> split(
    char sep, const std::string& str,
    std::string::size_type limit = std::string::npos);

/*!
 * Split the given string at each separator string into distinct substrings.
 * Multiple consecutive separators are considered individually and will result
 * in empty split substrings.
 *
 * \param sep    separator string
 * \param str    string to split
 * \param limit  maximum number of parts returned
 * \return       vector containing each split substring
 */
std::vector<std::string> split(
    const char* sep, const std::string& str,
    std::string::size_type limit = std::string::npos);

/*!
 * Split the given string at each separator string into distinct substrings.
 * Multiple consecutive separators are considered individually and will result
 * in empty split substrings.
 *
 * \param sep    separator string
 * \param str    string to split
 * \param limit  maximum number of parts returned
 * \return       vector containing each split substring
 */
std::vector<std::string> split(
    const std::string& sep, const std::string& str,
    std::string::size_type limit = std::string::npos);

/******************************************************************************/
// split() returning std::vector<std::string> with minimum fields

/*!
 * Split the given string at each separator character into distinct substrings.
 * Multiple consecutive separators are considered individually and will result
 * in empty split substrings.  Returns a vector of strings with at least
 * min_fields and at most limit_fields, empty fields are added if needed.
 *
 * \param sep         separator string
 * \param str         string to split
 * \param min_fields  minimum number of parts returned
 * \param limit       maximum number of parts returned
 * \return            vector containing each split substring
 */
std::vector<std::string> split(
    char sep, const std::string& str,
    std::string::size_type min_fields, std::string::size_type limit);

/*!
 * Split the given string at each separator string into distinct substrings.
 * Multiple consecutive separators are considered individually and will result
 * in empty split substrings.  Returns a vector of strings with at least
 * min_fields and at most limit_fields, empty fields are added if needed.
 *
 * \param sep         separator string
 * \param str         string to split
 * \param min_fields  minimum number of parts returned
 * \param limit       maximum number of parts returned
 * \return            vector containing each split substring
 */
std::vector<std::string> split(
    const char* sep, const std::string& str,
    std::string::size_type min_fields, std::string::size_type limit);

/*!
 * Split the given string at each separator string into distinct substrings.
 * Multiple consecutive separators are considered individually and will result
 * in empty split substrings.  Returns a vector of strings with at least
 * min_fields and at most limit_fields, empty fields are added if needed.
 *
 * \param sep         separator string
 * \param str         string to split
 * \param min_fields  minimum number of parts returned
 * \param limit       maximum number of parts returned
 * \return            vector containing each split substring
 */
std::vector<std::string> split(
    const std::string& sep, const std::string& str,
    std::string::size_type min_fields, std::string::size_type limit);

/******************************************************************************/
// split() into std::vector<std::string>

/*!
 * Split the given string at each separator character into distinct substrings.
 * Multiple consecutive separators are considered individually and will result
 * in empty split substrings.
 *
 * \param into   destination std::vector
 * \param sep    separator character
 * \param str    string to split
 * \param limit  maximum number of parts returned
 * \return       vector containing each split substring
 */
std::vector<std::string>& split(
    std::vector<std::string>* into,
    char sep, const std::string& str,
    std::string::size_type limit = std::string::npos);

/*!
 * Split the given string at each separator string into distinct substrings.
 * Multiple consecutive separators are considered individually and will result
 * in empty split substrings.
 *
 * \param into   destination std::vector
 * \param sep    separator string
 * \param str    string to split
 * \param limit  maximum number of parts returned
 * \return       vector containing each split substring
 */
std::vector<std::string>& split(
    std::vector<std::string>* into,
    const char* sep, const std::string& str,
    std::string::size_type limit = std::string::npos);

/*!
 * Split the given string at each separator string into distinct substrings.
 * Multiple consecutive separators are considered individually and will result
 * in empty split substrings.
 *
 * \param into   destination std::vector
 * \param sep    separator string
 * \param str    string to split
 * \param limit  maximum number of parts returned
 * \return       vector containing each split substring
 */
std::vector<std::string>& split(
    std::vector<std::string>* into,
    const std::string& sep, const std::string& str,
    std::string::size_type limit = std::string::npos);

/******************************************************************************/
// split() into std::vector<std::string> with minimum fields

/*!
 * Split the given string at each separator character into distinct substrings.
 * Multiple consecutive separators are considered individually and will result
 * in empty split substrings.  Returns a vector of strings with at least
 * min_fields and at most limit_fields, empty fields are added if needed.
 *
 * \param into        destination std::vector
 * \param sep         separator character
 * \param str         string to split
 * \param min_fields  minimum number of parts returned
 * \param limit       maximum number of parts returned
 * \return            vector containing each split substring
 */
std::vector<std::string>& split(
    std::vector<std::string>* into,
    char sep, const std::string& str,
    std::string::size_type min_fields, std::string::size_type limit);

/*!
 * Split the given string at each separator string into distinct substrings.
 * Multiple consecutive separators are considered individually and will result
 * in empty split substrings.  Returns a vector of strings with at least
 * min_fields and at most limit_fields, empty fields are added if needed.
 *
 * \param into        destination std::vector
 * \param sep         separator string
 * \param str         string to split
 * \param min_fields  minimum number of parts returned
 * \param limit       maximum number of parts returned
 * \return            vector containing each split substring
 */
std::vector<std::string>& split(
    std::vector<std::string>* into,
    const char* sep, const std::string& str,
    std::string::size_type min_fields, std::string::size_type limit);

/*!
 * Split the given string at each separator string into distinct substrings.
 * Multiple consecutive separators are considered individually and will result
 * in empty split substrings.  Returns a vector of strings with at least
 * min_fields and at most limit_fields, empty fields are added if needed.
 *
 * \param into        destination std::vector
 * \param sep         separator string
 * \param str         string to split
 * \param min_fields  minimum number of parts returned
 * \param limit       maximum number of parts returned
 * \return            vector containing each split substring
 */
std::vector<std::string>& split(
    std::vector<std::string>* into,
    const std::string& sep, const std::string& str,
    std::string::size_type min_fields, std::string::size_type limit);

//! \}
//! \}

} // namespace tlx

#endif // !TLX_STRING_SPLIT_HEADER

/******************************************************************************/
