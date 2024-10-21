/*******************************************************************************
 * tlx/logger/unordered_set.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_LOGGER_UNORDERED_SET_HEADER
#define TLX_LOGGER_UNORDERED_SET_HEADER

#include <tlx/logger/core.hpp>

#include <unordered_set>

namespace tlx {

template <typename T, typename H, typename E, typename A>
class LoggerFormatter<std::unordered_set<T, H, E, A> >
{
public:
    static void print(std::ostream& os,
                      const std::unordered_set<T, H, E, A>& data) {
        os << '{';
        for (typename std::unordered_set<T, H, E, A>::const_iterator
             it = data.begin(); it != data.end(); ++it)
        {
            if (it != data.begin()) os << ',';
            LoggerFormatter<T>::print(os, *it);
        }
        os << '}';
    }
};

template <typename T, typename H, typename E, typename A>
class LoggerFormatter<std::unordered_multiset<T, H, E, A> >
{
public:
    static void print(std::ostream& os,
                      const std::unordered_multiset<T, H, E, A>& data) {
        os << '{';
        for (typename std::unordered_multiset<T, H, E, A>::const_iterator
             it = data.begin(); it != data.end(); ++it)
        {
            if (it != data.begin()) os << ',';
            LoggerFormatter<T>::print(os, *it);
        }
        os << '}';
    }
};

} // namespace tlx

#endif // !TLX_LOGGER_UNORDERED_SET_HEADER

/******************************************************************************/
