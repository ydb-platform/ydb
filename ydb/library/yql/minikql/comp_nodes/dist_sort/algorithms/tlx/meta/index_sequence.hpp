/*******************************************************************************
 * tlx/meta/index_sequence.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_META_INDEX_SEQUENCE_HEADER
#define TLX_META_INDEX_SEQUENCE_HEADER

#include <cstddef>

namespace tlx {

//! \addtogroup tlx_meta
//! \{

// Compile-time integer sequences, an implementation of std::index_sequence and
// std::make_index_sequence, as these are not available in many current
// libraries (MS Visual C++).
template <size_t... Indexes>
struct index_sequence {
    static size_t size() { return sizeof ... (Indexes); }
};

namespace meta_detail {

template <size_t CurrentIndex, size_t... Indexes>
struct make_index_sequence_helper;

template <size_t... Indexes>
struct make_index_sequence_helper<0, Indexes...> {
    using type = index_sequence<Indexes...>;
};

template <size_t CurrentIndex, size_t... Indexes>
struct make_index_sequence_helper {
    using type = typename make_index_sequence_helper<
        CurrentIndex - 1, CurrentIndex - 1, Indexes...>::type;
};

} // namespace meta_detail

template <size_t Size>
struct make_index_sequence
    : public meta_detail::make_index_sequence_helper<Size>::type { };

//! \}

} // namespace tlx

#endif // !TLX_META_INDEX_SEQUENCE_HEADER

/******************************************************************************/
