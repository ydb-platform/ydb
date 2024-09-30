/*******************************************************************************
 * tlx/string/split_view.hpp
 *
 * Split a std::string and call a functor with tlx::string_view for each part.
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2019 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_SPLIT_VIEW_HEADER
#define TLX_STRING_SPLIT_VIEW_HEADER

#include <tlx/container/string_view.hpp>

namespace tlx {

//! \addtogroup tlx_string
//! \{
//! \name Split and Join
//! \{

/*!
 * Split the given string at each separator character into distinct substrings,
 * and call the given callback with a StringView for each substring. Multiple
 * consecutive separators are considered individually and will result in empty
 * split substrings.
 *
 * \param str       string to split
 * \param sep       separator character
 * \param callback  callback taking StringView of substring
 * \param limit     maximum number of parts returned
 */
template <typename Functor>
static inline
void split_view(
    char sep, const std::string& str, Functor&& callback,
    std::string::size_type limit = std::string::npos) {

    if (limit == 0) {
        callback(StringView(str.begin(), str.end()));
        return;
    }

    std::string::size_type count = 0;
    auto it = str.begin(), last = it;

    for ( ; it != str.end(); ++it)
    {
        if (*it == sep)
        {
            if (count == limit)
            {
                callback(StringView(last, str.end()));
                return;
            }
            callback(StringView(last, it));
            ++count;
            last = it + 1;
        }
    }
    callback(StringView(last, it));
}

//! \}
//! \}

} // namespace tlx

#endif // !TLX_STRING_SPLIT_VIEW_HEADER

/******************************************************************************/
