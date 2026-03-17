/******************************************************************************
 * Project:  PROJ
 * Purpose:  WKT1 parser grammar
 * Author:   Even Rouault, <even dot rouault at mines dash paris dot org>
 *
 ******************************************************************************
 * Copyright (c) 2013, Even Rouault <even dot rouault at mines-paris dot org>
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

#ifndef PJ_WKT1_PARSER_H_INCLUDED
#define PJ_WKT1_PARSER_H_INCLUDED

#ifndef DOXYGEN_SKIP

#ifdef __cplusplus
extern "C" {
#endif

typedef struct pj_wkt1_parse_context pj_wkt1_parse_context;

#include "wkt1_generated_parser.h"

void pj_wkt1_error(pj_wkt1_parse_context *context, const char *msg);
int pj_wkt1_lex(YYSTYPE *pNode, pj_wkt1_parse_context *context);
int pj_wkt1_parse(pj_wkt1_parse_context *context);

#ifdef __cplusplus
}

std::string pj_wkt1_parse(const std::string &wkt);

#endif

#endif /* #ifndef DOXYGEN_SKIP */

#endif /*  PJ_WKT1_PARSER_H_INCLUDED */
