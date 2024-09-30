/*******************************************************************************
 * tlx/define/endian.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_DEFINE_ENDIAN_HEADER
#define TLX_DEFINE_ENDIAN_HEADER

namespace tlx {

//! \addtogroup tlx_define
//! \{

// borrowed from https://stackoverflow.com/a/27054190

#if (defined(__BYTE_ORDER) && __BYTE_ORDER == __BIG_ENDIAN) ||             \
    (defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__) || \
    defined(__BIG_ENDIAN__) ||                                             \
    defined(__ARMEB__) ||                                                  \
    defined(__THUMBEB__) ||                                                \
    defined(__AARCH64EB__) ||                                              \
    defined(_MIBSEB) || defined(__MIBSEB) || defined(__MIBSEB__)

// It's a big-endian target architecture
#define TLX_BIG_ENDIAN 1

#elif (defined(__BYTE_ORDER) && __BYTE_ORDER == __LITTLE_ENDIAN) ||           \
    (defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__) || \
    defined(__LITTLE_ENDIAN__) ||                                             \
    defined(__ARMEL__) ||                                                     \
    defined(__THUMBEL__) ||                                                   \
    defined(__AARCH64EL__) ||                                                 \
    defined(_MIPSEL) || defined(__MIPSEL) || defined(__MIPSEL__) ||           \
    defined(_MSC_VER)

// It's a little-endian target architecture
#define TLX_LITTLE_ENDIAN 1

#else
#error "tlx: I don't know what architecture this is!"
#endif

//! \}

} // namespace tlx

#endif // !TLX_DEFINE_ENDIAN_HEADER

/******************************************************************************/
