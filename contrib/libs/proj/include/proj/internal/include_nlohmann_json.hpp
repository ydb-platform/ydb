/******************************************************************************
 *
 * Project:  PROJ
 * Purpose:  Wrapper for nlohmann/json.hpp
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

#ifndef INCLUDE_NLOHMANN_JSON_HPP
#define INCLUDE_NLOHMANN_JSON_HPP

#if defined(__GNUC__)
#pragma GCC system_header
#endif

#ifdef EXTERNAL_NLOHMANN_JSON

#include <nlohmann/json.hpp>

#else // !EXTERNAL_NLOHMANN_JSON

// to avoid any clash if PROJ users have another version of nlohmann/json.hpp
#define nlohmann proj_nlohmann

#if !defined(DOXYGEN_ENABLED)
#error #include "vendor/nlohmann/json.hpp"
#endif

#endif // EXTERNAL_NLOHMANN_JSON

#endif // INCLUDE_NLOHMANN_JSON_HPP
