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

#ifndef FROM_PROJ_CPP
#define FROM_PROJ_CPP
#endif

#include "proj/internal/internal.hpp"

#include <algorithm>
#include <cstring>
#include <string>

#include "wkt1_parser.h"
#include "wkt_parser.hpp"

using namespace NS_PROJ::internal;

//! @cond Doxygen_Suppress

// ---------------------------------------------------------------------------

struct pj_wkt1_parse_context : public pj_wkt_parse_context {};

// ---------------------------------------------------------------------------

void pj_wkt1_error(pj_wkt1_parse_context *context, const char *msg) {
    pj_wkt_error(context, msg);
}

// ---------------------------------------------------------------------------

std::string pj_wkt1_parse(const std::string &wkt) {
    pj_wkt1_parse_context context;
    context.pszInput = wkt.c_str();
    context.pszLastSuccess = wkt.c_str();
    context.pszNext = wkt.c_str();
    if (pj_wkt1_parse(&context) != 0) {
        return context.errorMsg;
    }
    return std::string();
}

// ---------------------------------------------------------------------------

typedef struct {
    const char *pszToken;
    int nTokenVal;
} osr_cs_wkt_tokens;

#define PAIR(X)                                                                \
    {                                                                          \
#X, T_##X                                                              \
    }

static const osr_cs_wkt_tokens tokens[] = {
    PAIR(PARAM_MT),       PAIR(PARAMETER),  PAIR(CONCAT_MT),   PAIR(INVERSE_MT),
    PAIR(PASSTHROUGH_MT),

    PAIR(PROJCS),         PAIR(PROJECTION), PAIR(GEOGCS),      PAIR(DATUM),
    PAIR(SPHEROID),       PAIR(PRIMEM),     PAIR(UNIT),        PAIR(GEOCCS),
    PAIR(AUTHORITY),      PAIR(VERT_CS),    PAIR(VERTCS),      PAIR(VERT_DATUM),
    PAIR(VDATUM),         PAIR(COMPD_CS),   PAIR(AXIS),        PAIR(TOWGS84),
    PAIR(FITTED_CS),      PAIR(LOCAL_CS),   PAIR(LOCAL_DATUM), PAIR(LINUNIT),

    PAIR(EXTENSION)};

// ---------------------------------------------------------------------------

int pj_wkt1_lex(YYSTYPE * /*pNode */, pj_wkt1_parse_context *context) {
    size_t i;
    const char *pszInput = context->pszNext;

    /* -------------------------------------------------------------------- */
    /*      Skip white space.                                               */
    /* -------------------------------------------------------------------- */
    while (*pszInput == ' ' || *pszInput == '\t' || *pszInput == 10 ||
           *pszInput == 13)
        pszInput++;

    context->pszLastSuccess = pszInput;

    if (*pszInput == '\0') {
        context->pszNext = pszInput;
        return EOF;
    }

    /* -------------------------------------------------------------------- */
    /*      Recognize node names.                                           */
    /* -------------------------------------------------------------------- */
    if (isalpha(*pszInput)) {
        for (i = 0; i < sizeof(tokens) / sizeof(tokens[0]); i++) {
            if (ci_starts_with(pszInput, tokens[i].pszToken) &&
                !isalpha(pszInput[strlen(tokens[i].pszToken)])) {
                context->pszNext = pszInput + strlen(tokens[i].pszToken);
                return tokens[i].nTokenVal;
            }
        }
    }

    /* -------------------------------------------------------------------- */
    /*      Recognize double quoted strings.                                */
    /* -------------------------------------------------------------------- */
    if (*pszInput == '"') {
        pszInput++;
        while (*pszInput != '\0' && *pszInput != '"')
            pszInput++;
        if (*pszInput == '\0') {
            context->pszNext = pszInput;
            return EOF;
        }
        context->pszNext = pszInput + 1;
        return T_STRING;
    }

    /* -------------------------------------------------------------------- */
    /*      Recognize numerical values.                                     */
    /* -------------------------------------------------------------------- */

    if (((*pszInput == '-' || *pszInput == '+') && pszInput[1] >= '0' &&
         pszInput[1] <= '9') ||
        (*pszInput >= '0' && *pszInput <= '9')) {
        if (*pszInput == '-' || *pszInput == '+')
            pszInput++;

        // collect non-decimal part of number
        while (*pszInput >= '0' && *pszInput <= '9')
            pszInput++;

        // collect decimal places.
        if (*pszInput == '.') {
            pszInput++;
            while (*pszInput >= '0' && *pszInput <= '9')
                pszInput++;
        }

        // collect exponent
        if (*pszInput == 'e' || *pszInput == 'E') {
            pszInput++;
            if (*pszInput == '-' || *pszInput == '+')
                pszInput++;
            while (*pszInput >= '0' && *pszInput <= '9')
                pszInput++;
        }

        context->pszNext = pszInput;

        return T_NUMBER;
    }

    /* -------------------------------------------------------------------- */
    /*      Recognize identifiers.                                          */
    /* -------------------------------------------------------------------- */
    if ((*pszInput >= 'A' && *pszInput <= 'Z') ||
        (*pszInput >= 'a' && *pszInput <= 'z')) {
        pszInput++;
        while ((*pszInput >= 'A' && *pszInput <= 'Z') ||
               (*pszInput >= 'a' && *pszInput <= 'z'))
            pszInput++;
        context->pszNext = pszInput;
        return T_IDENTIFIER;
    }

    /* -------------------------------------------------------------------- */
    /*      Handle special tokens.                                          */
    /* -------------------------------------------------------------------- */
    context->pszNext = pszInput + 1;
    return *pszInput;
}

//! @endcond
