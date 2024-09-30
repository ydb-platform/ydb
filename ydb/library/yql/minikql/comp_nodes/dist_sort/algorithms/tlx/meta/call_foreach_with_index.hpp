/*******************************************************************************
 * tlx/meta/call_foreach_with_index.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_META_CALL_FOREACH_WITH_INDEX_HEADER
#define TLX_META_CALL_FOREACH_WITH_INDEX_HEADER

#include <utility>

#include <tlx/meta/static_index.hpp>

namespace tlx {

//! \addtogroup tlx_meta
//! \{

/******************************************************************************/
// Variadic Template Expander: run a generic templated functor (like a generic
// lambda) for each of the variadic template parameters.
//
// Called with func(StaticIndex<> index, Argument arg).

namespace meta_detail {

//! helper for call_foreach_with_index: base case
template <size_t Index, typename Functor, typename Arg>
void call_foreach_with_index_impl(Functor&& f, Arg&& arg) {
    std::forward<Functor>(f)(StaticIndex<Index>(), std::forward<Arg>(arg));
}

//! helper for call_foreach_with_index: general recursive case
template <size_t Index, typename Functor, typename Arg, typename... MoreArgs>
void call_foreach_with_index_impl(Functor&& f, Arg&& arg, MoreArgs&& ... rest) {
    std::forward<Functor>(f)(StaticIndex<Index>(), std::forward<Arg>(arg));
    call_foreach_with_index_impl<Index + 1>(
        std::forward<Functor>(f), std::forward<MoreArgs>(rest) ...);
}

} // namespace meta_detail

//! Call a generic functor (like a generic lambda) for each variadic template
//! argument together with its zero-based index.
template <typename Functor, typename... Args>
void call_foreach_with_index(Functor&& f, Args&& ... args) {
    meta_detail::call_foreach_with_index_impl<0>(
        std::forward<Functor>(f), std::forward<Args>(args) ...);
}

//! \}

} // namespace tlx

#endif // !TLX_META_CALL_FOREACH_WITH_INDEX_HEADER

/******************************************************************************/
