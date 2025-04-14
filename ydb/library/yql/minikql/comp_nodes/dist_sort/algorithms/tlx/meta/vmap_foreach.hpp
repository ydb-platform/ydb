/*******************************************************************************
 * tlx/meta/vmap_foreach.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_META_VMAP_FOREACH_HEADER
#define TLX_META_VMAP_FOREACH_HEADER

#include <tuple>
#include <utility>

namespace tlx {

//! \addtogroup tlx_meta
//! \{

/******************************************************************************/
// Variadic Template Expander: run a generic templated functor (like a generic
// lambda) for each of the variadic template parameters, and collect the return
// values in a generic std::tuple.

namespace meta_detail {

//! helper for vmap_foreach: base case
template <typename Functor, typename Arg>
auto vmap_foreach_impl(Functor&& f, Arg&& arg) {
    return std::make_tuple(
        std::forward<Functor>(f)(std::forward<Arg>(arg)));
}

//! helper for vmap_foreach: general recursive case
template <typename Functor, typename Arg, typename... MoreArgs>
auto vmap_foreach_impl(Functor&& f, Arg&& arg, MoreArgs&& ... rest) {
    auto x = std::forward<Functor>(f)(std::forward<Arg>(arg));
    return std::tuple_cat(
        std::make_tuple(std::move(x)),
        vmap_foreach_impl(
            std::forward<Functor>(f), std::forward<MoreArgs>(rest) ...));
}

} // namespace meta_detail

//! Call a generic functor (like a generic lambda) for each variadic template
//! argument.
template <typename Functor, typename... Args>
auto vmap_foreach(Functor&& f, Args&& ... args) {
    return meta_detail::vmap_foreach_impl(
        std::forward<Functor>(f), std::forward<Args>(args) ...);
}

//! \}

} // namespace tlx

#endif // !TLX_META_VMAP_FOREACH_HEADER

/******************************************************************************/
