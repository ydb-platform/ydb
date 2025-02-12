/*******************************************************************************
 * tlx/string/hash_djb2.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2019 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_HASH_DJB2_HEADER
#define TLX_STRING_HASH_DJB2_HEADER

#include <cstdint>
#include <string>

namespace tlx {

//! \addtogroup tlx_string
//! \{

/*!
 * Simple, fast, but "insecure" string hash method by Dan Bernstein from
 * http://www.cse.yorku.ca/~oz/hash.html
 */
static inline
std::uint32_t hash_djb2(const unsigned char* str) {
    std::uint32_t hash = 5381;
    unsigned char c;
    while ((c = *str++) != 0) {
        // hash * 33 + c
        hash = ((hash << 5) + hash) + c;
    }
    return hash;
}

/*!
 * Simple, fast, but "insecure" string hash method by Dan Bernstein from
 * http://www.cse.yorku.ca/~oz/hash.html
 */
static inline
std::uint32_t hash_djb2(const char* str) {
    return hash_djb2(reinterpret_cast<const unsigned char*>(str));
}

/*!
 * Simple, fast, but "insecure" string hash method by Dan Bernstein from
 * http://www.cse.yorku.ca/~oz/hash.html
 */
static inline
std::uint32_t hash_djb2(const unsigned char* str, size_t size) {
    std::uint32_t hash = 5381;
    while (size-- > 0) {
        // hash * 33 + c
        hash = ((hash << 5) + hash) + static_cast<unsigned char>(*str++);
    }
    return hash;
}

/*!
 * Simple, fast, but "insecure" string hash method by Dan Bernstein from
 * http://www.cse.yorku.ca/~oz/hash.html
 */
static inline
std::uint32_t hash_djb2(const char* str, size_t size) {
    return hash_djb2(reinterpret_cast<const unsigned char*>(str), size);
}

/*!
 * Simple, fast, but "insecure" string hash method by Dan Bernstein from
 * http://www.cse.yorku.ca/~oz/hash.html
 */
static inline
std::uint32_t hash_djb2(const std::string& str) {
    return hash_djb2(str.data(), str.size());
}

//! \}

} // namespace tlx

#endif // !TLX_STRING_HASH_DJB2_HEADER

/******************************************************************************/
