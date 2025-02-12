/*******************************************************************************
 * tlx/vector_free.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2019 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_VECTOR_FREE_HEADER
#define TLX_VECTOR_FREE_HEADER

#include <vector>

namespace tlx {

//! Simple method to free the underlying memory in a vector, because .clear()
//! need not do it.
template <typename Type>
void vector_free(std::vector<Type>& v) {
    std::vector<Type>().swap(v);
}

} // namespace tlx

#endif // !TLX_VECTOR_FREE_HEADER

/******************************************************************************/
