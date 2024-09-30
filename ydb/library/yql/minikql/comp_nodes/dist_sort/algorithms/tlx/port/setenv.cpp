/*******************************************************************************
 * tlx/port/setenv.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2020 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/port/setenv.hpp>

#include <cstdlib>

namespace tlx {

// Windows porting madness because setenv() is apparently dangerous.
#if defined(_MSC_VER)

int setenv(const char* name, const char* value, int overwrite) {
    if (!overwrite) {
        size_t envsize = 0;
        int errcode = getenv_s(&envsize, nullptr, 0, name);
        if (errcode || envsize) return errcode;
    }
    return _putenv_s(name, value);
}

// More porting weirdness for MinGW (32 and 64)
#elif defined(__MINGW32__)

int setenv(const char* name, const char* value, int overwrite) {
    if (!overwrite) {
        const char* current = getenv(name);
        if (current) return 0;
    }
    return _putenv_s(name, value);
}

#else

int setenv(const char* name, const char* value, int overwrite) {
    return ::setenv(name, value, overwrite);
}

#endif

} // namespace tlx

/******************************************************************************/
