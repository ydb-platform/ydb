/*******************************************************************************
 * tlx/meta/is_std_tuple.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_META_IS_STD_TUPLE_HEADER
#define TLX_META_IS_STD_TUPLE_HEADER

#include <tuple>

namespace tlx {

//! \addtogroup tlx_meta
//! \{

//! test if is a std::tuple<...>
template <typename T>
struct is_std_tuple : public std::false_type { };

template <typename... Ts>
struct is_std_tuple<std::tuple<Ts...> >: public std::true_type { };

//! \}

} // namespace tlx

#endif // !TLX_META_IS_STD_TUPLE_HEADER

/******************************************************************************/
