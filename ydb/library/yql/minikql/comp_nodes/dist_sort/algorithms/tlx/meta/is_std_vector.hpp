/*******************************************************************************
 * tlx/meta/is_std_vector.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_META_IS_STD_VECTOR_HEADER
#define TLX_META_IS_STD_VECTOR_HEADER

#include <vector>

namespace tlx {

//! \addtogroup tlx_meta
//! \{

//! test if is std::vector<T>
template <typename T>
struct is_std_vector : public std::false_type { };

template <typename T>
struct is_std_vector<std::vector<T> >: public std::true_type { };

//! \}

} // namespace tlx

#endif // !TLX_META_IS_STD_VECTOR_HEADER

/******************************************************************************/
