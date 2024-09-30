/*******************************************************************************
 * tlx/string/pad.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/string/pad.hpp>

namespace tlx {

std::string pad(const std::string& s, size_t len, char pad_char) {
    std::string str = s;
    str.resize(len, pad_char);
    return str;
}

} // namespace tlx

/******************************************************************************/
