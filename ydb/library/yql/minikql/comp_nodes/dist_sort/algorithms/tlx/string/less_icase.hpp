/*******************************************************************************
 * tlx/string/less_icase.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2019 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_LESS_ICASE_HEADER
#define TLX_STRING_LESS_ICASE_HEADER

#include <string>

namespace tlx {

//! \addtogroup tlx_string
//! \{

/******************************************************************************/
// less_icase()

//! returns true if a < b without regard for letter case
bool less_icase(const char* a, const char* b);

//! returns true if a < b without regard for letter case
bool less_icase(const char* a, const std::string& b);

//! returns true if a < b without regard for letter case
bool less_icase(const std::string& a, const char* b);

//! returns true if a < b without regard for letter case
bool less_icase(const std::string& a, const std::string& b);

/******************************************************************************/
// order_less_icase: case-insensitive order relation functional classes

//! Case-insensitive less order relation functional class for std::map, etc.
struct less_icase_asc {
    inline bool operator () (const std::string& a, const std::string& b) const {
        return less_icase(a, b);
    }
};

//! Descending case-insensitive less order relation functional class for
//! std::map, etc.
struct less_icase_desc {
    inline bool operator () (const std::string& a, const std::string& b) const {
        return !less_icase(a, b);
    }
};

//! \}

} // namespace tlx

#endif // !TLX_STRING_LESS_ICASE_HEADER

/******************************************************************************/
