/*******************************************************************************
 * tlx/string/parse_uri.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2020 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_PARSE_URI_HEADER
#define TLX_STRING_PARSE_URI_HEADER

#include <tlx/container/string_view.hpp>

namespace tlx {

//! \addtogroup tlx_string
//! \{

/*!
 * Parse a URI like "/path1/path2?query=string&submit=submit#fragment" into the
 * parts path, query_string, and fragment. The parts are returned as
 * tlx::string_views to avoid copying data.
 */
static inline
void parse_uri(const char* uri, tlx::string_view* path,
               tlx::string_view* query_string, tlx::string_view* fragment) {
    const char* c = uri;

    // find path part
    const char* begin = c;
    while (!(*c == '?' || *c == '#' || *c == 0)) {
        ++c;
    }
    *path = tlx::string_view(begin, c - begin);

    // find query string
    begin = c;
    if (*c == '?') {
        begin = ++c;
        while (!(*c == '#' || *c == 0)) {
            ++c;
        }
    }
    *query_string = tlx::string_view(begin, c - begin);

    // find fragment
    begin = c;
    if (*c == '#') {
        begin = ++c;
        while (*c != 0) {
            ++c;
        }
    }
    *fragment = tlx::string_view(begin, c - begin);
}

/*!
 * Parse a URI like "/path1/path2?query=string&submit=submit#fragment" into the
 * parts path, query_string, and fragment. The parts are returned as
 * tlx::string_views to avoid copying data.
 */
static inline
void parse_uri(const std::string& uri, tlx::string_view* path,
               tlx::string_view* query_string, tlx::string_view* fragment) {
    return parse_uri(uri.c_str(), path, query_string, fragment);
}

//! \}

} // namespace tlx

#endif // !TLX_STRING_PARSE_URI_HEADER

/******************************************************************************/
