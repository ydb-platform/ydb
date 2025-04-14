/*******************************************************************************
 * tlx/string/join.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_JOIN_HEADER
#define TLX_STRING_JOIN_HEADER

#include <string>
#include <vector>

namespace tlx {

//! \addtogroup tlx_string
//! \{
//! \name Split and Join
//! \{

/******************************************************************************/
// join()

/*!
 * Join a vector of strings by some glue character between each pair from the
 * sequence.
 *
 * \param glue     character for glue
 * \param parts    the vector of strings to join
 * \return string  constructed from the vector with the glue between two strings
 */
std::string join(
    char glue, const std::vector<std::string>& parts);

/*!
 * Join a vector of strings by some glue string between each pair from the
 * sequence.
 *
 * \param glue     string to glue
 * \param parts    the vector of strings to join
 * \return string  constructed from the vector with the glue between two strings
 */
std::string join(
    const char* glue, const std::vector<std::string>& parts);

/*!
 * Join a vector of strings by some glue string between each pair from the
 * sequence.
 *
 * \param glue     string to glue
 * \param parts    the vector of strings to join
 * \return string  constructed from the vector with the glue between two strings
 */
std::string join(
    const std::string& glue, const std::vector<std::string>& parts);

//! \}
//! \}

} // namespace tlx

#endif // !TLX_STRING_JOIN_HEADER

/******************************************************************************/
