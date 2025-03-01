/*******************************************************************************
 * tlx/logger/tuple.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_LOGGER_TUPLE_HEADER
#define TLX_LOGGER_TUPLE_HEADER

#include <tlx/logger/core.hpp>
#include <tlx/meta/call_foreach_tuple_with_index.hpp>

#include <tuple>

namespace tlx {

class LoggerTupleFormatter
{
public:
    explicit LoggerTupleFormatter(std::ostream& os) : os_(os) { }
    template <typename Index, typename Arg>
    void operator () (const Index&, const Arg& a) const {
        if (Index::index != 0) os_ << ',';
        LoggerFormatter<typename std::decay<Arg>::type>::print(os_, a);
    }
    std::ostream& os_;
};

template <typename... Args>
class LoggerFormatter<std::tuple<Args...> >
{
public:
    static void print(std::ostream& os, const std::tuple<Args...>& t) {
        os << '(';
        call_foreach_tuple_with_index(LoggerTupleFormatter(os), t);
        os << ')';
    }
};

template <>
class LoggerFormatter<std::tuple<> >
{
public:
    static void print(std::ostream& os, const std::tuple<>&) {
        os << '(' << ')';
    }
};

} // namespace tlx

#endif // !TLX_LOGGER_TUPLE_HEADER

/******************************************************************************/
