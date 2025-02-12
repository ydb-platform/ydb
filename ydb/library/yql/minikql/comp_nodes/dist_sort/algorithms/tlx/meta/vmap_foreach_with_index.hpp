/*******************************************************************************
 * tlx/meta/vmap_foreach_with_index.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_META_VMAP_FOREACH_WITH_INDEX_HEADER
#define TLX_META_VMAP_FOREACH_WITH_INDEX_HEADER

#include <tuple>
#include <utility>

#include <tlx/meta/static_index.hpp>

namespace tlx {

//! \addtogroup tlx_meta
//! \{

/******************************************************************************/
// Variadic Template Expander: run a generic templated functor (like a generic
// lambda) for each of the variadic template parameters, and collect the return
// values in a generic std::tuple.
//
// Called with func(StaticIndex<> index, Argument arg).

namespace meta_detail {

//! helper for vmap_foreach_with_index: base case
template <size_t Index, typename Functor, typename Arg>
auto vmap_foreach_with_index_impl(Functor&& f, Arg&& arg) {
    return std::make_tuple(
        std::forward<Functor>(f)(StaticIndex<Index>(), std::forward<Arg>(arg)));
}

//! helper for vmap_foreach_with_index: general recursive case
template <size_t Index, typename Functor, typename Arg, typename... MoreArgs>
auto vmap_foreach_with_index_impl(Functor&& f, Arg&& arg, MoreArgs&& ... rest) {
    auto x =
        std::forward<Functor>(f)(StaticIndex<Index>(), std::forward<Arg>(arg));
    return std::tuple_cat(
        std::make_tuple(std::move(x)),
        vmap_foreach_with_index_impl<Index + 1>(
            std::forward<Functor>(f), std::forward<MoreArgs>(rest) ...));
}

} // namespace meta_detail

//! Call a generic functor (like a generic lambda) for each variadic template
//! argument together with its zero-based index.
template <typename Functor, typename... Args>
auto vmap_foreach_with_index(Functor&& f, Args&& ... args) {
    return meta_detail::vmap_foreach_with_index_impl<0>(
        std::forward<Functor>(f), std::forward<Args>(args) ...);
}

//! \}

} // namespace tlx

#endif // !TLX_META_VMAP_FOREACH_WITH_INDEX_HEADER

/******************************************************************************/
