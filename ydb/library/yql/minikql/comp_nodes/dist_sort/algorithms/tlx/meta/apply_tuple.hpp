/*******************************************************************************
 * tlx/meta/apply_tuple.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_META_APPLY_TUPLE_HEADER
#define TLX_META_APPLY_TUPLE_HEADER

#include <tuple>
#include <utility>

#include <tlx/meta/index_sequence.hpp>

namespace tlx {

//! \addtogroup tlx_meta
//! \{

/******************************************************************************/
// Tuple Applier: takes a std::tuple<> and applies a variadic template function
// to it. Hence, this expands the content of the tuple as the arguments.

namespace meta_detail {

template <typename Functor, typename Tuple, std::size_t... Is>
auto apply_tuple_impl(Functor&& f, Tuple&& t, index_sequence<Is...>) {
    return std::forward<Functor>(f)(
        std::get<Is>(std::forward<Tuple>(t)) ...);
}

} // namespace meta_detail

//! Call the functor f with the contents of t as arguments.
template <typename Functor, typename Tuple>
auto apply_tuple(Functor&& f, Tuple&& t) {
    using Indices = make_index_sequence<
        std::tuple_size<typename std::decay<Tuple>::type>::value>;
    return meta_detail::apply_tuple_impl(
        std::forward<Functor>(f), std::forward<Tuple>(t), Indices());
}

//! \}

} // namespace tlx

#endif // !TLX_META_APPLY_TUPLE_HEADER

/******************************************************************************/
