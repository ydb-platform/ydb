/******************************************************************************
 *
 * Project:  PROJ
 * Purpose:  Tracing/profiling
 * Author:   Even Rouault <even dot rouault at spatialys dot com>
 *
 ******************************************************************************
 * Copyright (c) 2019, Even Rouault <even dot rouault at spatialys dot com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 ****************************************************************************/

#ifndef TRACING_HH_INCLUDED
#define TRACING_HH_INCLUDED

//! @cond Doxygen_Suppress

#include <string>

#include "proj/util.hpp"

#ifdef ENABLE_TRACING

NS_PROJ_START

namespace tracing {

void logTrace(const std::string &str,
              const std::string &component = std::string());

class EnterBlock {
  public:
    EnterBlock(const std::string &msg);
    ~EnterBlock();

  private:
    PROJ_OPAQUE_PRIVATE_DATA
};

#define TRACING_MERGE(a, b) a##b
#define TRACING_UNIQUE_NAME(a) TRACING_MERGE(unique_name_, a)

#define ENTER_BLOCK(x) EnterBlock TRACING_UNIQUE_NAME(__LINE__)(x)
#define ENTER_FUNCTION() ENTER_BLOCK(__FUNCTION__ + std::string("()"))

} // namespace tracing

NS_PROJ_END

using namespace NS_PROJ::tracing;

#else // ENABLE_TRACING

inline void logTrace(const std::string &, const std::string & = std::string()) {
}

#define ENTER_BLOCK(x)                                                         \
    do {                                                                       \
    } while (0);

#define ENTER_FUNCTION()                                                       \
    do {                                                                       \
    } while (0)

#endif // ENABLE_TRACING

//! @endcond

#endif // TRACING_HH_INCLUDED
