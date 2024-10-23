/*******************************************************************************
 * tlx/string/index_of.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_INDEX_OF_HEADER
#define TLX_STRING_INDEX_OF_HEADER

#include <string>
#include <vector>

namespace tlx {

//! \addtogroup tlx_string
//! \{

/*!
 * Attempts to find str in the list and return the index. Throws a
 * std::runtime_error if it is not found.
 */
size_t index_of(const std::vector<std::string>& list, const char* str);

/*!
 * Attempts to find str in the list and return the index. Throws a
 * std::runtime_error if it is not found.
 */
size_t index_of(const std::vector<std::string>& list, const std::string& str);

/*!
 * Attempts to find str in the list and return the index using case-insensitive
 * comparisons. Throws a std::runtime_error if it is not found.
 */
size_t index_of_icase(const std::vector<std::string>& list, const char* str);

/*!
 * Attempts to find str in the list and return the index using case-insensitive
 * comparisons. Throws a std::runtime_error if it is not found.
 */
size_t
index_of_icase(const std::vector<std::string>& list, const std::string& str);

//! \}

} // namespace tlx

#endif // !TLX_STRING_INDEX_OF_HEADER

/******************************************************************************/
