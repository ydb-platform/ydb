/*******************************************************************************
 * tlx/string/base64.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_BASE64_HEADER
#define TLX_STRING_BASE64_HEADER

#include <string>

namespace tlx {

//! \addtogroup tlx_string
//! \{
//! \name Base64 Encoding and Decoding
//! \{

/******************************************************************************/
// Base64 Encoding and Decoding

/*!
 * Encode the given binary data into base64 representation as described in RFC
 * 2045 or RFC 3548. The output string contains only characters [A-Za-z0-9+/]
 * and is roughly 33% longer than the input. The output string can be broken
 * into lines after n characters, where n must be a multiple of 4.
 *
 * \param data        input data to encode
 * \param size        size of input data to encode
 * \param line_break  break the output string every n characters
 * \return            base64 encoded string
 */
std::string base64_encode(const void* data, size_t size, size_t line_break = 0);

/*!
 * Encode the given binary string into base64 representation as described in RFC
 * 2045 or RFC 3548. The output string contains only characters [A-Za-z0-9+/]
 * and is roughly 33% longer than the input. The output string can be broken
 * into lines after n characters, where n must be a multiple of 4.
 *
 * \param str         input string to encode
 * \param line_break  break the output string every n characters
 * \return            base64 encoded string
 */
std::string base64_encode(const std::string& str, size_t line_break = 0);

/*!
 * Decode a string in base64 representation as described in RFC 2045 or RFC 3548
 * and return the original data. If a non-whitespace invalid base64 character is
 * encountered _and_ the parameter "strict" is true, then this function will
 * throw a std::runtime_error. If "strict" is false, the character is silently
 * ignored.
 *
 * \param data    input data to decode
 * \param size    size of input data to decode
 * \param strict  throw exception on invalid character
 * \return        decoded binary data
 */
std::string base64_decode(const void* data, size_t size, bool strict = true);

/*!
 * Decode a string in base64 representation as described in RFC 2045 or RFC 3548
 * and return the original data. If a non-whitespace invalid base64 character is
 * encountered _and_ the parameter "strict" is true, then this function will
 * throw a std::runtime_error. If "strict" is false, the character is silently
 * ignored.
 *
 * \param str     input string to encode
 * \param strict  throw exception on invalid character
 * \return        decoded binary data
 */
std::string base64_decode(const std::string& str, bool strict = true);

//! \}
//! \}

} // namespace tlx

#endif // !TLX_STRING_BASE64_HEADER

/******************************************************************************/
