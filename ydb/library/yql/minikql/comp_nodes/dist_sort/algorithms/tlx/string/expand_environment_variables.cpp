/*******************************************************************************
 * tlx/string/expand_environment_variables.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/string/expand_environment_variables.hpp>

#include <cctype>
#include <cstdlib>
#include <cstring>

namespace tlx {

std::string& expand_environment_variables(std::string* sp) {
    std::string& s = *sp;
    size_t p = 0;
    while (p < s.size()) {
        // find a dollar sing
        std::string::size_type dp = s.find('$', p);
        if (dp == std::string::npos)
            return s;

        if (dp + 1 < s.size() && s[dp + 1] == '{') {
            // match "${[^}]*}"

            // find matching '}'
            std::string::size_type de = s.find('}', dp + 2);
            if (de == std::string::npos) {
                p = dp + 1;
                continue;
            }

            // cut out variable name
            std::string var = s.substr(dp + 2, de - (dp + 2));

            const char* v = getenv(var.c_str());
            if (v == nullptr)
                v = "";
            size_t vlen = std::strlen(v);

            // replace with value
            s.replace(dp, de - dp + 1, v);

            p = dp + vlen + 1;
        }
        else if (dp + 1 < s.size() &&
                 (std::isalpha(s[dp + 1]) || s[dp + 1] == '_')) {

            // match "$[a-zA-Z][a-zA-Z0-9]*"
            std::string::size_type de = dp + 1;
            while (de < s.size() &&
                   (std::isalnum(s[de]) || s[de] == '_'))
                ++de;

            // cut out variable name
            std::string var = s.substr(dp + 1, de - (dp + 1));

            const char* v = getenv(var.c_str());
            if (v == nullptr)
                v = "";
            size_t vlen = std::strlen(v);

            // replace with value
            s.replace(dp, de - dp, v);

            p = dp + vlen;
        }
        else {
            p = dp + 1;
        }
    }
    return s;
}

std::string expand_environment_variables(const std::string& s) {
    std::string copy = s;
    expand_environment_variables(&copy);
    return copy;
}

std::string expand_environment_variables(const char* s) {
    std::string copy = s;
    expand_environment_variables(&copy);
    return copy;
}

} // namespace tlx

/******************************************************************************/
