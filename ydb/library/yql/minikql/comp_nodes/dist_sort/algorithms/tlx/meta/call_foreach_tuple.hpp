/*******************************************************************************
 * tlx/meta/call_foreach_tuple.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_META_CALL_FOREACH_TUPLE_HEADER
#define TLX_META_CALL_FOREACH_TUPLE_HEADER

#include <tuple>

#include <tlx/meta/call_foreach.hpp>
#include <tlx/meta/index_sequence.hpp>

namespace tlx {

//! \addtogroup tlx_meta
//! \{

/******************************************************************************/
// Variadic Template Expander: run a generic templated functor (like a generic
// lambda) for each component of a std::tuple.
//
// Called with func(Argument arg).

namespace meta_detail {

//! helper for call_foreach_tuple
template <typename Functor, typename Tuple, std::size_t... Is>
void call_foreach_tuple_impl(
    Functor&& f, Tuple&& t, index_sequence<Is...>) {
    return call_foreach(
        std::forward<Functor>(f), std::get<Is>(std::forward<Tuple>(t)) ...);
}

} // namespace meta_detail

//! Call a generic functor (like a generic lambda) to each components of a tuple
//! together with its zero-based index.
template <typename Functor, typename Tuple>
void call_foreach_tuple(Functor&& f, Tuple&& t) {
    using Indices = make_index_sequence<
        std::tuple_size<typename std::decay<Tuple>::type>::value>;
    meta_detail::call_foreach_tuple_impl(
        std::forward<Functor>(f), std::forward<Tuple>(t), Indices());
}

//! \}

} // namespace tlx

#endif // !TLX_META_CALL_FOREACH_TUPLE_HEADER

/******************************************************************************/
