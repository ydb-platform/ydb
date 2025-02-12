/*******************************************************************************
 * tlx/meta/vmap_foreach_tuple.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Hung Tran <hung@ae.cs.uni-frankfurt.de>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_META_VMAP_FOREACH_TUPLE_HEADER
#define TLX_META_VMAP_FOREACH_TUPLE_HEADER

#include <tlx/meta/index_sequence.hpp>
#include <tlx/meta/vmap_foreach.hpp>
#include <tuple>

namespace tlx {

//! \addtogroup tlx_meta
//! \{

/******************************************************************************/
// Variadic Template Expander: run a generic templated functor (like a generic
// lambda) for each component of a tuple, and collect the returned values in a
// generic std::tuple.

namespace meta_detail {

//! helper for vmap_foreach_tuple: forwards tuple entries
template <typename Functor, typename Tuple, std::size_t... Is>
auto vmap_foreach_tuple_impl(
    Functor&& f, Tuple&& t, index_sequence<Is...>) {
    return vmap_foreach(std::forward<Functor>(f),
                        std::get<Is>(std::forward<Tuple>(t)) ...);
}

} // namespace meta_detail

//! Call a generic functor (like a generic lambda) for each variadic template
//! argument and collect the result in a std::tuple<>.
template <typename Functor, typename Tuple>
auto vmap_foreach_tuple(Functor&& f, Tuple&& t) {
    using Indices = make_index_sequence<
        std::tuple_size<typename std::decay<Tuple>::type>::value>;
    return meta_detail::vmap_foreach_tuple_impl(
        std::forward<Functor>(f), std::forward<Tuple>(t), Indices());
}

//! \}

} // namespace tlx

#endif // !TLX_META_VMAP_FOREACH_TUPLE_HEADER

/******************************************************************************/
