/*******************************************************************************
 * tlx/string/hash_sdbm.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2019 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_HASH_SDBM_HEADER
#define TLX_STRING_HASH_SDBM_HEADER

#include <cstdint>
#include <string>

namespace tlx {

//! \addtogroup tlx_string
//! \{

/*!
 * Simple, fast, but "insecure" string hash method by sdbm database from
 * http://www.cse.yorku.ca/~oz/hash.html
 */
static inline
std::uint32_t hash_sdbm(const unsigned char* str) {
    std::uint32_t hash = 0;
    unsigned char c;
    while ((c = *str++) != 0) {
        hash = c + (hash << 6) + (hash << 16) - hash;
    }
    return hash;
}

/*!
 * Simple, fast, but "insecure" string hash method by sdbm database from
 * http://www.cse.yorku.ca/~oz/hash.html
 */
static inline
std::uint32_t hash_sdbm(const char* str) {
    return hash_sdbm(reinterpret_cast<const unsigned char*>(str));
}

/*!
 * Simple, fast, but "insecure" string hash method by sdbm database from
 * http://www.cse.yorku.ca/~oz/hash.html
 */
static inline
std::uint32_t hash_sdbm(const unsigned char* str, size_t size) {
    std::uint32_t hash = 0;
    while (size-- > 0) {
        hash = static_cast<unsigned char>(*str++)
               + (hash << 6) + (hash << 16) - hash;
    }
    return hash;
}

/*!
 * Simple, fast, but "insecure" string hash method by sdbm database from
 * http://www.cse.yorku.ca/~oz/hash.html
 */
static inline
std::uint32_t hash_sdbm(const char* str, size_t size) {
    return hash_sdbm(reinterpret_cast<const unsigned char*>(str), size);
}

/*!
 * Simple, fast, but "insecure" string hash method by sdbm database from
 * http://www.cse.yorku.ca/~oz/hash.html
 */
static inline
std::uint32_t hash_sdbm(const std::string& str) {
    return hash_sdbm(str.data(), str.size());
}

//! \}

} // namespace tlx

#endif // !TLX_STRING_HASH_SDBM_HEADER

/******************************************************************************/
