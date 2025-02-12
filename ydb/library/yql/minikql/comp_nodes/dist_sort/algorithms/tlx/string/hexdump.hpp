/*******************************************************************************
 * tlx/string/hexdump.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_HEXDUMP_HEADER
#define TLX_STRING_HEXDUMP_HEADER

#include <cstdint>
#include <string>
#include <vector>

namespace tlx {

//! \addtogroup tlx_string
//! \{
//! \name Hexdump Methods
//! \{

/******************************************************************************/
// Uppercase Hexdump Methods

/*!
 * Dump a (binary) string as a sequence of uppercase hexadecimal pairs.
 *
 * \param data  binary data to output in hex
 * \param size  length of binary data
 * \return      string of hexadecimal pairs
 */
std::string hexdump(const void* const data, size_t size);

/*!
 * Dump a (binary) string as a sequence of uppercase hexadecimal pairs.
 *
 * \param str  binary data to output in hex
 * \return     string of hexadecimal pairs
 */
std::string hexdump(const std::string& str);

/*!
 * Dump a (binary) item as a sequence of uppercase hexadecimal pairs.
 *
 * \param t  binary data to output in hex
 * \return   string of hexadecimal pairs
 */
template <typename Type>
std::string hexdump_type(const Type& t) {
    return hexdump(&t, sizeof(t));
}

/*!
 * Dump a char vector as a sequence of uppercase hexadecimal pairs.
 *
 * \param data  binary data to output in hex
 * \return      string of hexadecimal pairs
 */
std::string hexdump(const std::vector<char>& data);

/*!
 * Dump a uint8_t vector as a sequence of uppercase hexadecimal pairs.
 *
 * \param data  binary data to output in hex
 * \return      string of hexadecimal pairs
 */
std::string hexdump(const std::vector<std::uint8_t>& data);

/*!
 * Dump a (binary) string into a C source code snippet. The snippet defines an
 * array of const std::uint8_t* holding the data of the string.
 *
 * \param str       string to output as C source array
 * \param var_name  name of the array variable in the outputted code snippet
 * \return          string holding C source snippet
 */
std::string hexdump_sourcecode(
    const std::string& str, const std::string& var_name = "name");

/******************************************************************************/
// Lowercase Hexdump Methods

/*!
 * Dump a (binary) string as a sequence of lowercase hexadecimal pairs.
 *
 * \param data  binary data to output in hex
 * \param size  length of binary data
 * \return      string of hexadecimal pairs
 */
std::string hexdump_lc(const void* const data, size_t size);

/*!
 * Dump a (binary) string as a sequence of lowercase hexadecimal pairs.
 *
 * \param str  binary data to output in hex
 * \return     string of hexadecimal pairs
 */
std::string hexdump_lc(const std::string& str);

/*!
 * Dump a (binary) item as a sequence of lowercase hexadecimal pairs.
 *
 * \param t  binary data to output in hex
 * \return   string of hexadecimal pairs
 */
template <typename Type>
std::string hexdump_lc_type(const Type& t) {
    return hexdump_lc(&t, sizeof(t));
}

/*!
 * Dump a char vector as a sequence of lowercase hexadecimal pairs.
 *
 * \param data  binary data to output in hex
 * \return      string of hexadecimal pairs
 */
std::string hexdump_lc(const std::vector<char>& data);

/*!
 * Dump a uint8_t vector as a sequence of lowercase hexadecimal pairs.
 *
 * \param data  binary data to output in hex
 * \return      string of hexadecimal pairs
 */
std::string hexdump_lc(const std::vector<std::uint8_t>& data);

/******************************************************************************/
// Parser for Hex Digit Sequence

/*!
 * Read a string as a sequence of hexadecimal pairs. Converts each pair of
 * hexadecimal digits into a byte of the output string. Throws
 * std::runtime_error() if an unknown letter is encountered.
 *
 * \param str  string to parse as hex digits
 * \return     string of read bytes
 */
std::string parse_hexdump(const std::string& str);

//! \}
//! \}

} // namespace tlx

#endif // !TLX_STRING_HEXDUMP_HEADER

/******************************************************************************/
