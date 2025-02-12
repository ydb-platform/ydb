/*******************************************************************************
 * tlx/string/join.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/string/join.hpp>
#include <tlx/string/join_generic.hpp>

namespace tlx {

std::string join(char glue, const std::vector<std::string>& parts) {
    return join(glue, parts.begin(), parts.end());
}

std::string join(const char* glue, const std::vector<std::string>& parts) {
    return join(glue, parts.begin(), parts.end());
}

std::string join(
    const std::string& glue, const std::vector<std::string>& parts) {
    return join(glue, parts.begin(), parts.end());
}

} // namespace tlx

/******************************************************************************/
