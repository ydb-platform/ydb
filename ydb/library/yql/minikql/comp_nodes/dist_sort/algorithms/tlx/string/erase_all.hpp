/*******************************************************************************
 * tlx/string/erase_all.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_ERASE_ALL_HEADER
#define TLX_STRING_ERASE_ALL_HEADER

#include <string>

namespace tlx {

//! \addtogroup tlx_string
//! \{

/******************************************************************************/
// erase_all() in-place

/*!
 * Remove all occurrences of the given character in-place.
 *
 * \param str   string to process
 * \param drop  remove this character
 * \return      reference to the modified string
 */
std::string& erase_all(std::string* str, char drop = ' ');

/*!
 * Remove all occurrences of the given characters in-place.
 *
 * \param str   string to process
 * \param drop  remove these characters
 * \return      reference to the modified string
 */
std::string& erase_all(std::string* str, const char* drop);

/*!
 * Remove all occurrences of the given characters in-place.
 *
 * \param str   string to process
 * \param drop  remove these characters
 * \return      reference to the modified string
 */
std::string& erase_all(std::string* str, const std::string& drop);

/******************************************************************************/
// erase_all() copy

/*!
 * Remove all occurrences of the given character, return copy of string.
 *
 * \param str   string to process
 * \param drop  remove this character
 * \return      copy of string possibly with less characters
 */
std::string erase_all(const std::string& str, char drop = ' ');

/*!
 * Remove all occurrences of the given characters, return copy of string.
 *
 * \param str   string to process
 * \param drop  remove these characters
 * \return      copy of string possibly with less characters
 */
std::string erase_all(const std::string& str, const char* drop);

/*!
 * Remove all occurrences of the given characters, return copy of string.
 *
 * \param str   string to process
 * \param drop  remove these characters
 * \return      copy of string possibly with less characters
 */
std::string erase_all(const std::string& str, const std::string& drop);

//! \}

} // namespace tlx

#endif // !TLX_STRING_ERASE_ALL_HEADER

/******************************************************************************/
