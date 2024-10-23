/*******************************************************************************
 * tlx/string/expand_environment_variables.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_EXPAND_ENVIRONMENT_VARIABLES_HEADER
#define TLX_STRING_EXPAND_ENVIRONMENT_VARIABLES_HEADER

#include <string>

namespace tlx {

//! \addtogroup tlx_string
//! \{

/*!
 * Expand substrings $ABC_123 and ${ABC_123} into the corresponding environment
 * variables. Matches all substrings "$[a-zA-Z_][a-zA-Z0-9_]*" and
 * "${[^}]*}". Replaces all substrings in-place.
 */
std::string& expand_environment_variables(std::string* s);

/*!
 * Expand substrings $ABC_123 and ${ABC_123} into the corresponding environment
 * variables. Matches all substrings "$[a-zA-Z_][a-zA-Z0-9_]*" and
 * "${[^}]*}". Returns a copy of the string with all substrings replaced.
 */
std::string expand_environment_variables(const std::string& s);

/*!
 * Expand substrings $ABC_123 and ${ABC_123} into the corresponding environment
 * variables. Matches all substrings "$[a-zA-Z_][a-zA-Z0-9_]*" and
 * "${[^}]*}". Returns a copy of the string with all substrings replaced.
 */
std::string expand_environment_variables(const char* s);

//! \}

} // namespace tlx

#endif // !TLX_STRING_EXPAND_ENVIRONMENT_VARIABLES_HEADER

/******************************************************************************/
