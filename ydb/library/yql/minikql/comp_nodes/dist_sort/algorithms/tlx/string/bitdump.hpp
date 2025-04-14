/*******************************************************************************
 * tlx/string/bitdump.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2019 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_BITDUMP_HEADER
#define TLX_STRING_BITDUMP_HEADER

#include <cstdint>
#include <string>

namespace tlx {

//! \addtogroup tlx_string
//! \{
//! \name Bitdump Methods
//! \{

/******************************************************************************/
// Bitdump Methods

/*!
 * Dump a (binary) string of 8-bit bytes as a sequence of '0' and '1'
 * characters, with the most significant bits (msb) first. Each 8-bit byte is
 * represented with a block of '0'/'1's separated by spaces.
 *
 * \param data  binary data to output as bits
 * \param size  length of binary data
 * \return      string of binary digits
 */
std::string bitdump_8_msb(const void* const data, size_t size);

/*!
 * Dump a (binary) string of 8-bit bytes as a sequence of '0' and '1'
 * characters, with the most significant bits (msb) first. Each 8-bit byte is
 * represented with a block of '0'/'1's separated by spaces.
 *
 * \param str  binary data to output as bits
 * \return     string of binary digits
 */
std::string bitdump_8_msb(const std::string& str);

/*!
 * Dump a (binary) item of 8-bit bytes as a sequence of '0' and '1' characters,
 * with the most significant bits (msb) first. Each 8-bit byte is represented
 * with a block of '0'/'1's separated by spaces.
 *
 * \param t  binary data to output as bits
 * \return   string of binary digits
 */
template <typename Type>
std::string bitdump_8_msb_type(const Type& t) {
    return bitdump_8_msb(&t, sizeof(t));
}

/*----------------------------------------------------------------------------*/

//! deprecated method: unclear naming and documentation.
std::string bitdump_le8(const void* const data, size_t size);

//! deprecated method: unclear naming and documentation.
std::string bitdump_le8(const std::string& str);

//! deprecated method: unclear naming and documentation.
template <typename Type>
std::string bitdump_le8_type(const Type& t) {
    return bitdump_8_msb_type(t);
}

/*----------------------------------------------------------------------------*/

/*!
 * Dump a (binary) string of 8-bit bytes as a sequence of '0' and '1'
 * characters, with the least significant bits (lsb) first. Each 8-bit byte is
 * represented with a block of '0'/'1's separated by spaces.
 *
 * \param data  binary data to output as bits
 * \param size  length of binary data
 * \return      string of binary digits
 */
std::string bitdump_8_lsb(const void* const data, size_t size);

/*!
 * Dump a (binary) string of 8-bit bytes as a sequence of '0' and '1'
 * characters, with the least significant bits (lsb) first. Each 8-bit byte is
 * represented with a block of '0'/'1's separated by spaces.
 *
 * \param str  binary data to output as bits
 * \return     string of binary digits
 */
std::string bitdump_8_lsb(const std::string& str);

/*!
 * Dump a (binary) item of 8-bit bytes as a sequence of '0' and '1' characters,
 * with the least significant bits (lsb) first. Each 8-bit byte is represented
 * with a block of '0'/'1's separated by spaces.
 *
 * \param t  binary data to output as bits
 * \return   string of binary digits
 */
template <typename Type>
std::string bitdump_8_lsb_type(const Type& t) {
    return bitdump_8_lsb(&t, sizeof(t));
}

/*----------------------------------------------------------------------------*/

//! deprecated method: unclear naming and documentation.
std::string bitdump_be8(const void* const data, size_t size);

//! deprecated method: unclear naming and documentation.
std::string bitdump_be8(const std::string& str);

//! deprecated method: unclear naming and documentation.
template <typename Type>
std::string bitdump_be8_type(const Type& t) {
    return bitdump_8_lsb_type(t);
}

/*----------------------------------------------------------------------------*/

//! \}
//! \}

} // namespace tlx

#endif // !TLX_STRING_BITDUMP_HEADER

/******************************************************************************/
