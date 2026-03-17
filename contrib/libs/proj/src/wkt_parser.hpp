/******************************************************************************
 * Project:  PROJ
 * Purpose:  WKT parser common routines
 * Author:   Even Rouault, <even.rouault at spatialys.com>
 *
 ******************************************************************************
 * Copyright (c) 2018 Even Rouault, <even.rouault at spatialys.com>
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

#ifndef PJ_WKT_PARSER_H_INCLUDED
#define PJ_WKT_PARSER_H_INCLUDED

//! @cond Doxygen_Suppress

#include <string>

struct pj_wkt_parse_context {
    const char *pszInput = nullptr;
    const char *pszLastSuccess = nullptr;
    const char *pszNext = nullptr;
    std::string errorMsg{};

    pj_wkt_parse_context() = default;
    pj_wkt_parse_context(const pj_wkt_parse_context &) = delete;
    pj_wkt_parse_context &operator=(const pj_wkt_parse_context &) = delete;
};

void pj_wkt_error(pj_wkt_parse_context *context, const char *msg);

//! @endcond

#endif // PJ_WKT_PARSER_H_INCLUDED
