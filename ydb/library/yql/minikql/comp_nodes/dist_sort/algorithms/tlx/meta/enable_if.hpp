/*******************************************************************************
 * tlx/meta/enable_if.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2019 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_META_ENABLE_IF_HEADER
#define TLX_META_ENABLE_IF_HEADER

namespace tlx {

//! \addtogroup tlx_meta
//! \{

//! SFINAE enable_if -- copy of std::enable_if<> with less extra cruft.
template <bool, typename T = void>
struct enable_if
{ };

template <typename T>
struct enable_if<true, T> {
    typedef T type;
};

//! \}

} // namespace tlx

#endif // !TLX_META_ENABLE_IF_HEADER

/******************************************************************************/
