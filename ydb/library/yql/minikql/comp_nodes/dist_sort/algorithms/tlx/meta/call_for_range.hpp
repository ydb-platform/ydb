/*******************************************************************************
 * tlx/meta/call_for_range.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_META_CALL_FOR_RANGE_HEADER
#define TLX_META_CALL_FOR_RANGE_HEADER

#include <utility>

#include <tlx/meta/static_index.hpp>

namespace tlx {

//! \addtogroup tlx_meta
//! \{

/******************************************************************************/
// Variadic Template Enumerator: run a generic templated functor (like a generic
// lambda) for the integers 0 .. Size-1 or more general [Begin,End).
//
// Called with func(StaticIndex<> index).

namespace meta_detail {

//! helper for call_for_range: general recursive case
template <size_t Index, size_t Size, typename Functor>
class CallForRangeImpl
{
public:
    static void call(Functor&& f) {
        std::forward<Functor>(f)(StaticIndex<Index>());
        CallForRangeImpl<Index + 1, Size - 1, Functor>::call(
            std::forward<Functor>(f));
    }
};

//! helper for call_for_range: base case
template <size_t Index, typename Functor>
class CallForRangeImpl<Index, 0, Functor>
{
public:
    static void call(Functor&& /* f */) { }
};

} // namespace meta_detail

//! Call a generic functor (like a generic lambda) for the integers [0,Size).
template <size_t Size, typename Functor>
void call_for_range(Functor&& f) {
    meta_detail::CallForRangeImpl<0, Size, Functor>::call(
        std::forward<Functor>(f));
}

//! Call a generic functor (like a generic lambda) for the integers [Begin,End).
template <size_t Begin, size_t End, typename Functor>
void call_for_range(Functor&& f) {
    meta_detail::CallForRangeImpl<Begin, End - Begin, Functor>::call(
        std::forward<Functor>(f));
}

//! \}

} // namespace tlx

#endif // !TLX_META_CALL_FOR_RANGE_HEADER

/******************************************************************************/
