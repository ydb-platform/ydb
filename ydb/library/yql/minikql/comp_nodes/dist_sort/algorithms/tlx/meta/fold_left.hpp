/*******************************************************************************
 * tlx/meta/fold_left.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Hung Tran <hung@ae.cs.uni-frankfurt.de>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_META_FOLD_LEFT_HEADER
#define TLX_META_FOLD_LEFT_HEADER

#include <tlx/meta/index_sequence.hpp>
#include <tuple>

namespace tlx {

//! \addtogroup tlx_meta
//! \{

/******************************************************************************/
// Variadic Template Expander: Implements fold_left() on the variadic template
// arguments. Implements (init ... op ... pack) of C++17.

namespace meta_detail {

//! helper for fold_left(): base case
template <typename Reduce, typename Initial, typename Arg>
auto fold_left_impl(Reduce&& r, Initial&& init, Arg&& arg) {
    return std::forward<Reduce>(r)(
        std::forward<Initial>(init), std::forward<Arg>(arg));
}

//! helper for fold_left(): general recursive case
template <typename Reduce, typename Initial, typename Arg, typename... MoreArgs>
auto fold_left_impl(Reduce&& r, Initial&& init,
                    Arg&& arg, MoreArgs&& ... rest) {
    return fold_left_impl(
        std::forward<Reduce>(r),
        std::forward<Reduce>(r)(
            std::forward<Initial>(init), std::forward<Arg>(arg)),
        std::forward<MoreArgs>(rest) ...);
}

} // namespace meta_detail

//! Implements fold_left() -- ((a * b) * c) -- with a binary Reduce operation
//! and initial value.
template <typename Reduce, typename Initial, typename... Args>
auto fold_left(Reduce&& r, Initial&& init, Args&& ... args) {
    return meta_detail::fold_left_impl(
        std::forward<Reduce>(r), std::forward<Initial>(init),
        std::forward<Args>(args) ...);
}

//! \}

} // namespace tlx

#endif // !TLX_META_FOLD_LEFT_HEADER

/******************************************************************************/
