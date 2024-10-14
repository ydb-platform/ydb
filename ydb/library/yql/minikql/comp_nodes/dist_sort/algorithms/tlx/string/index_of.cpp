/*******************************************************************************
 * tlx/string/index_of.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/string/index_of.hpp>

#include <tlx/string/equal_icase.hpp>

#include <stdexcept>

namespace tlx {

size_t index_of(const std::vector<std::string>& list, const char* str) {
    for (size_t i = 0; i < list.size(); ++i) {
        if (list[i] == str)
            return i;
    }
    std::string reason = "Could not find index_of() ";
    reason += str;
    throw std::runtime_error(reason);
}

size_t index_of(const std::vector<std::string>& list, const std::string& str) {
    for (size_t i = 0; i < list.size(); ++i) {
        if (list[i] == str)
            return i;
    }
    std::string reason = "Could not find index_of() ";
    reason += str;
    throw std::runtime_error(reason);
}

size_t index_of_icase(const std::vector<std::string>& list, const char* str) {
    for (size_t i = 0; i < list.size(); ++i) {
        if (tlx::equal_icase(list[i], str))
            return i;
    }
    std::string reason = "Could not find index_of_icase() ";
    reason += str;
    throw std::runtime_error(reason);
}

size_t
index_of_icase(const std::vector<std::string>& list, const std::string& str) {
    for (size_t i = 0; i < list.size(); ++i) {
        if (tlx::equal_icase(list[i], str))
            return i;
    }
    std::string reason = "Could not find index_of_icase() ";
    reason += str;
    throw std::runtime_error(reason);
}

} // namespace tlx

/******************************************************************************/
