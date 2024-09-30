/*******************************************************************************
 * tlx/string/contains_word.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_CONTAINS_WORD_HEADER
#define TLX_STRING_CONTAINS_WORD_HEADER

#include <string>

namespace tlx {

//! \addtogroup tlx_string
//! \{

/******************************************************************************/
// contains_word()

/*!
 * Search the given string for a whitespace-delimited word. It works as if the
 * str was split_words() and the resulting vector checked for a given
 * word. However this function does not create a vector, it scans the string
 * directly. Whitespace is space, tab, newline or carriage-return.
 *
 * \param str   whitespace-delimited string to check
 * \param word  word to find
 * \return      true if the word was found
 */
bool contains_word(const std::string& str, const char* word);

/*!
 * Search the given string for a whitespace-delimited word. It works as if the
 * str was split_words() and the resulting vector checked for a given
 * word. However this function does not create a vector, it scans the string
 * directly. Whitespace is space, tab, newline or carriage-return.
 *
 * \param str   whitespace-delimited string to check
 * \param word  word to find
 * \return      true if the word was found
 */
bool contains_word(const std::string& str, const std::string& word);

//! \}

} // namespace tlx

#endif // !TLX_STRING_CONTAINS_WORD_HEADER

/******************************************************************************/
