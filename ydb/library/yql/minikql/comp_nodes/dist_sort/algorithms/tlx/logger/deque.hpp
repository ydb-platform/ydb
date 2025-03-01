/*******************************************************************************
 * tlx/logger/deque.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_LOGGER_DEQUE_HEADER
#define TLX_LOGGER_DEQUE_HEADER

#include <tlx/logger/core.hpp>

#include <deque>

namespace tlx {

template <typename T, typename A>
class LoggerFormatter<std::deque<T, A> >
{
public:
    static void print(std::ostream& os, const std::deque<T, A>& data) {
        os << '[';
        for (typename std::deque<T, A>::const_iterator it = data.begin();
             it != data.end(); ++it)
        {
            if (it != data.begin()) os << ',';
            LoggerFormatter<T>::print(os, *it);
        }
        os << ']';
    }
};

} // namespace tlx

#endif // !TLX_LOGGER_DEQUE_HEADER

/******************************************************************************/
