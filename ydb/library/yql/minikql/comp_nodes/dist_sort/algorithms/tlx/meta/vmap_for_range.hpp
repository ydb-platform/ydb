/*******************************************************************************
 * tlx/meta/vmap_for_range.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_META_VMAP_FOR_RANGE_HEADER
#define TLX_META_VMAP_FOR_RANGE_HEADER

#include <tuple>
#include <utility>

#include <tlx/meta/static_index.hpp>

namespace tlx {

//! \addtogroup tlx_meta
//! \{

/******************************************************************************/
// Variadic Template Enumerate Mapper: run a generic templated functor (like a
// generic lambda) for each value from [Begin,End), and collect the return
// values in a generic std::tuple.
//
// Called with func(StaticIndex<> index).

namespace meta_detail {

//! helper for vmap_for_range: general recursive case
template <size_t Index, size_t Size, typename Functor>
class VMapForRangeImpl
{
public:
    static auto call(Functor&& f) {
        // call this index before recursion
        auto x = std::forward<Functor>(f)(StaticIndex<Index>());
        return std::tuple_cat(
            std::make_tuple(std::move(x)),
            VMapForRangeImpl<Index + 1, Size - 1, Functor>::call(
                std::forward<Functor>(f)));
    }
};

//! helper for vmap_for_range: base case
template <size_t Index, typename Functor>
class VMapForRangeImpl<Index, 0, Functor>
{
public:
    static auto call(Functor&& /* f */) {
        return std::tuple<>();
    }
};

} // namespace meta_detail

//! Vmap a generic functor (like a generic lambda) for the integers [0,Size).
template <size_t Size, typename Functor>
auto vmap_for_range(Functor&& f) {
    return meta_detail::VMapForRangeImpl<0, Size, Functor>::call(
        std::forward<Functor>(f));
}

//! Vmap a generic functor (like a generic lambda) for the integers [Begin,End).
template <size_t Begin, size_t End, typename Functor>
auto vmap_for_range(Functor&& f) {
    return meta_detail::VMapForRangeImpl<Begin, End - Begin, Functor>::call(
        std::forward<Functor>(f));
}

//! \}

} // namespace tlx

#endif // !TLX_META_VMAP_FOR_RANGE_HEADER

/******************************************************************************/
