/*******************************************************************************
 * tlx/logger/unordered_map.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_LOGGER_UNORDERED_MAP_HEADER
#define TLX_LOGGER_UNORDERED_MAP_HEADER

#include <tlx/logger/core.hpp>

#include <unordered_map>

namespace tlx {

template <typename K, typename V, typename H, typename E, typename A>
class LoggerFormatter<std::unordered_map<K, V, H, E, A> >
{
public:
    static void print(std::ostream& os,
                      const std::unordered_map<K, V, H, E, A>& data) {
        os << '{';
        for (typename std::unordered_map<K, V, H, E, A>::const_iterator
             it = data.begin(); it != data.end(); ++it)
        {
            if (it != data.begin()) os << ',';
            LoggerFormatter<K>::print(os, it->first);
            os << '=';
            LoggerFormatter<V>::print(os, it->second);
        }
        os << '}';
    }
};

template <typename K, typename V, typename H, typename E, typename A>
class LoggerFormatter<std::unordered_multimap<K, V, H, E, A> >
{
public:
    static void print(std::ostream& os,
                      const std::unordered_multimap<K, V, H, E, A>& data) {
        os << '{';
        for (typename std::unordered_multimap<K, V, H, E, A>::const_iterator
             it = data.begin(); it != data.end(); ++it)
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

#endif // !TLX_LOGGER_UNORDERED_MAP_HEADER

/******************************************************************************/
