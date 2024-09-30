/*******************************************************************************
 * tlx/meta/is_std_pair.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_META_IS_STD_PAIR_HEADER
#define TLX_META_IS_STD_PAIR_HEADER

#include <utility>

namespace tlx {

//! \addtogroup tlx_meta
//! \{

//! test if is a std::pair<...>
template <typename T>
struct is_std_pair : public std::false_type { };

template <typename S, typename T>
struct is_std_pair<std::pair<S, T> >: public std::true_type { };

//! \}

} // namespace tlx

#endif // !TLX_META_IS_STD_PAIR_HEADER

/******************************************************************************/
