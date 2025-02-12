/*******************************************************************************
 * tlx/logger/map.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_LOGGER_MAP_HEADER
#define TLX_LOGGER_MAP_HEADER

#include <tlx/logger/core.hpp>

#include <map>

namespace tlx {

template <typename K, typename V, typename C, typename A>
class LoggerFormatter<std::map<K, V, C, A> >
{
public:
    static void print(std::ostream& os, const std::map<K, V, C, A>& data) {
        os << '{';
        for (typename std::map<K, V, C, A>::const_iterator it = data.begin();
             it != data.end(); ++it)
        {
            if (it != data.begin()) os << ',';
            LoggerFormatter<K>::print(os, it->first);
            os << '=';
            LoggerFormatter<V>::print(os, it->second);
        }
        os << '}';
    }
};

template <typename K, typename V, typename C, typename A>
class LoggerFormatter<std::multimap<K, V, C, A> >
{
public:
    static void print(std::ostream& os, const std::multimap<K, V, C, A>& data) {
        os << '{';
        for (typename std::multimap<K, V, C, A>::const_iterator it = data.begin();
             it != data.end(); ++it)
        {
            if (it != data.begin()) os << ',';
            LoggerFormatter<K>::print(os, it->first);
            os << '=';
            LoggerFormatter<V>::print(os, it->second);
        }
        os << '}';
    }
};

} // namespace tlx

#endif // !TLX_LOGGER_MAP_HEADER

/******************************************************************************/
