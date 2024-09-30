/*******************************************************************************
 * tlx/meta/static_index.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_META_STATIC_INDEX_HEADER
#define TLX_META_STATIC_INDEX_HEADER

#include <cstddef>

namespace tlx {

//! \addtogroup tlx_meta
//! \{

//! Helper for call_foreach_with_index() to save the index as a compile-time
//! index
template <size_t Index>
struct StaticIndex {
    //! compile-time index
    static constexpr size_t index = Index;

    //! implicit conversion to a run-time index.
    operator size_t () const { return index; }
};

//! \}

} // namespace tlx

#endif // !TLX_META_STATIC_INDEX_HEADER

/******************************************************************************/
