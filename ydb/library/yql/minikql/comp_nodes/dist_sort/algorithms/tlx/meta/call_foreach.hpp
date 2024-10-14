/*******************************************************************************
 * tlx/meta/call_foreach.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_META_CALL_FOREACH_HEADER
#define TLX_META_CALL_FOREACH_HEADER

#include <utility>

namespace tlx {

//! \addtogroup tlx_meta
//! \{

/******************************************************************************/
// Variadic Template Expander: run a generic templated functor (like a generic
// lambda) for each of the variadic template parameters.

namespace meta_detail {

//! helper for call_foreach: base case
template <typename Functor, typename Arg>
void call_foreach_impl(Functor&& f, Arg&& arg) {
    std::forward<Functor>(f)(std::forward<Arg>(arg));
}

//! helper for call_foreach: general recursive case
template <typename Functor, typename Arg, typename... MoreArgs>
void call_foreach_impl(
    Functor&& f, Arg&& arg, MoreArgs&& ... rest) {
    std::forward<Functor>(f)(std::forward<Arg>(arg));
    call_foreach_impl(
        std::forward<Functor>(f), std::forward<MoreArgs>(rest) ...);
}

} // namespace meta_detail

//! Call a generic functor (like a generic lambda) for each variadic template
//! argument.
template <typename Functor, typename... Args>
void call_foreach(Functor&& f, Args&& ... args) {
    meta_detail::call_foreach_impl(
        std::forward<Functor>(f), std::forward<Args>(args) ...);
}

//! \}

} // namespace tlx

#endif // !TLX_META_CALL_FOREACH_HEADER

/******************************************************************************/
