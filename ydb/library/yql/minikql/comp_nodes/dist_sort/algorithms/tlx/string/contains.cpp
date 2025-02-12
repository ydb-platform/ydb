/*******************************************************************************
 * tlx/string/contains.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/string/contains.hpp>

namespace tlx {

bool contains(const std::string& str, const std::string& pattern) {
    return str.find(pattern) != std::string::npos;
}

bool contains(const std::string& str, const char* pattern) {
    return str.find(pattern) != std::string::npos;
}

bool contains(const std::string& str, const char ch) {
    return str.find(ch) != std::string::npos;
}

} // namespace tlx

/******************************************************************************/
