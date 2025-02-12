/*******************************************************************************
 * tlx/logger/wrap_unprintable.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2019 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_LOGGER_WRAP_UNPRINTABLE_HEADER
#define TLX_LOGGER_WRAP_UNPRINTABLE_HEADER

#include <tlx/meta/enable_if.hpp>

#include <ostream>
#include <utility>

namespace tlx {

//! SFINAE magic helper for wrap_unprintable()
template <typename, typename = void>
struct has_ostream_operator
{ static constexpr bool value = false; };

template <typename Type>
struct has_ostream_operator<
    Type, decltype(
        std::declval<std::ostream&>() << std::declval<Type const&>(), void())>
{ static constexpr bool value = true; };

//! SFINAE magic to return "<unprintable>" instead if the value HAS NO ostream
//! operator << available.  Identical to shorter wrap_unp().
template <typename Type>
typename enable_if<!has_ostream_operator<Type>::value, const char*>::type
wrap_unprintable(Type, const char* instead = "<unprintable>") {
    return instead;
}

//! SFINAE magic to return the value if the value HAS a ostream operator <<
//! available.  Identical to shorter wrap_unp().
template <typename Type>
typename enable_if<has_ostream_operator<Type>::value, Type>::type
wrap_unprintable(Type value, const char* = nullptr) {
    return value;
}

//! SFINAE magic to return "<unprintable>" instead if the value HAS NO ostream
//! operator << available.  Shortened name of wrap_unprintable()
template <typename Type>
typename enable_if<!has_ostream_operator<Type>::value, const char*>::type
wrap_unp(Type, const char* instead = "<unprintable>") {
    return instead;
}

//! SFINAE magic to return the value if the value HAS a ostream operator <<
//! available.  Shortened name of wrap_unprintable()
template <typename Type>
typename enable_if<has_ostream_operator<Type>::value, Type>::type
wrap_unp(Type value, const char* = nullptr) {
    return value;
}

} // namespace tlx

#endif // !TLX_LOGGER_WRAP_UNPRINTABLE_HEADER

/******************************************************************************/
