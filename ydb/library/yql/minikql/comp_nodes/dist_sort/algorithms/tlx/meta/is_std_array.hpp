/*******************************************************************************
 * tlx/meta/is_std_array.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_META_IS_STD_ARRAY_HEADER
#define TLX_META_IS_STD_ARRAY_HEADER

#include <array>
#include <cstddef>

namespace tlx {

//! \addtogroup tlx_meta
//! \{

//! test if is std::array<T, N>
template <typename T>
struct is_std_array : public std::false_type { };

template <typename T, size_t N>
struct is_std_array<std::array<T, N> >: public std::true_type { };

//! \}

} // namespace tlx

#endif // !TLX_META_IS_STD_ARRAY_HEADER

/******************************************************************************/
