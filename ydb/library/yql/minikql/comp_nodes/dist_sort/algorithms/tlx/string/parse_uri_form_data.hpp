/*******************************************************************************
 * tlx/string/parse_uri_form_data.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2020 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_PARSE_URI_FORM_DATA_HEADER
#define TLX_STRING_PARSE_URI_FORM_DATA_HEADER

#include <cstring>
#include <string>
#include <vector>

namespace tlx {

//! \addtogroup tlx_string
//! \{

/*!
 * Helper function to decode %20 and + in urlencoded form data like
 * "query=string+with+spaces&submit=yes%21&".
 */
static inline
std::string parse_uri_form_data_decode(
    const char* str, const char* end = nullptr) {
    std::string out;
    if (end == nullptr)
        out.reserve(strlen(str));
    else
        out.reserve(end - str);
    char a, b;

    while (*str && str != end) {
        if (*str == '%' && (a = str[1]) != 0 && (b = str[2]) != 0) {
            if (a >= '0' && a <= '9')
                a -= '0';
            else if (a >= 'a' && a <= 'f')
                a -= 'a' - 10;
            else if (a >= 'A' && a <= 'F')
                a -= 'A' - 10;
            else {
                // invalid hex digits, copy '%' and continue
                out += *str++;
                continue;
            }

            if (b >= '0' && b <= '9')
                b -= '0';
            else if (b >= 'a' && b <= 'f')
                b -= 'a' - 10;
            else if (b >= 'A' && b <= 'F')
                b -= 'A' - 10;
            else {
                // invalid hex digits, copy '%' and continue
                out += *str++;
                continue;
            }

            out += static_cast<char>(16 * a + b);
            str += 3;
        }
        else if (*str == '+') {
            out += ' ';
            str++;
        }
        else {
            out += *str++;
        }
    }
    return out;
}

/*!
 * Parse a urlencoded form data like "query=string+with+spaces&submit=yes%21&"
 * into a list of keys and values. The keys and values are returned as pairs in
 * the two vectors, to avoid using std::pair or another struct.
 */
static inline
void parse_uri_form_data(const char* query_string,
                         std::vector<std::string>* key,
                         std::vector<std::string>* value) {

    key->clear(), value->clear();
    const char* c = query_string;

    while (*c != 0) {
        const char* begin = c;
        while (*c != '=' && *c != 0) {
            ++c;
        }

        if (c == begin)
            return;

        std::string k = parse_uri_form_data_decode(begin, c);

        if (*c == 0) {
            key->emplace_back(std::move(k));
            value->emplace_back(std::string());
            return;
        }

        begin = ++c;
        while (*c != '&' && *c != 0) {
            ++c;
        }

        std::string v = parse_uri_form_data_decode(begin, c);

        key->emplace_back(std::move(k));
        value->emplace_back(std::move(v));

        if (*c != '&')
            break;
        ++c;
    }
}

/*!
 * Parse a urlencoded form data like "query=string+with+spaces&submit=yes%21&"
 * into a list of keys and values. The keys and values are returned as pairs in
 * the two vectors, to avoid using std::pair or another struct.
 */
static inline
void parse_uri_form_data(const std::string& query_string,
                         std::vector<std::string>* key,
                         std::vector<std::string>* value) {
    return parse_uri_form_data(query_string.c_str(), key, value);
}

//! \}

} // namespace tlx

#endif // !TLX_STRING_PARSE_URI_FORM_DATA_HEADER

/******************************************************************************/
