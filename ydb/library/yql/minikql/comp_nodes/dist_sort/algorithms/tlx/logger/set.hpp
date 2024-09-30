/*******************************************************************************
 * tlx/logger/set.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_LOGGER_SET_HEADER
#define TLX_LOGGER_SET_HEADER

#include <tlx/logger/core.hpp>

#include <set>

namespace tlx {

template <typename T, typename C, typename A>
class LoggerFormatter<std::set<T, C, A> >
{
public:
    static void print(std::ostream& os, const std::set<T, C, A>& data) {
        os << '{';
        for (typename std::set<T, C, A>::const_iterator it = data.begin();
             it != data.end(); ++it)
        {
            if (it != data.begin()) os << ',';
            LoggerFormatter<T>::print(os, *it);
        }
        os << '}';
    }
};

template <typename T, typename C, typename A>
class LoggerFormatter<std::multiset<T, C, A> >
{
public:
    static void print(std::ostream& os, const std::multiset<T, C, A>& data) {
        os << '{';
        for (typename std::multiset<T, C, A>::const_iterator it = data.begin();
             it != data.end(); ++it)
        {
            if (it != data.begin()) os << ',';
            LoggerFormatter<T>::print(os, *it);
        }
        os << '}';
    }
};

} // namespace tlx

#endif // !TLX_LOGGER_SET_HEADER

/******************************************************************************/
