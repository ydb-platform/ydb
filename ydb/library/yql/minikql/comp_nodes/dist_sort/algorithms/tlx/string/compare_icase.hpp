/*******************************************************************************
 * tlx/string/compare_icase.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_COMPARE_ICASE_HEADER
#define TLX_STRING_COMPARE_ICASE_HEADER

#include <string>

namespace tlx {

//! \addtogroup tlx_string
//! \{

/******************************************************************************/
// compare_icase()

//! returns +1/0/-1 like strcmp(a, b) but without regard for letter case
int compare_icase(const char* a, const char* b);

//! returns +1/0/-1 like strcmp(a, b) but without regard for letter case
int compare_icase(const char* a, const std::string& b);

//! returns +1/0/-1 like strcmp(a, b) but without regard for letter case
int compare_icase(const std::string& a, const char* b);

//! returns +1/0/-1 like strcmp(a, b) but without regard for letter case
int compare_icase(const std::string& a, const std::string& b);

//! \}

} // namespace tlx

#endif // !TLX_STRING_COMPARE_ICASE_HEADER

/******************************************************************************/
