/******************************************************************************
 * Project:  PROJ
 * Purpose:  WKT2 parser grammar
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

#ifndef FROM_PROJ_CPP
#define FROM_PROJ_CPP
#endif

#include "proj/internal/internal.hpp"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <string>

#include "proj_constants.h"
#include "wkt2_parser.h"
#include "wkt_parser.hpp"

using namespace NS_PROJ::internal;

//! @cond Doxygen_Suppress

// ---------------------------------------------------------------------------

struct pj_wkt2_parse_context : public pj_wkt_parse_context {};

// ---------------------------------------------------------------------------

void pj_wkt2_error(pj_wkt2_parse_context *context, const char *msg) {
    pj_wkt_error(context, msg);
}

// ---------------------------------------------------------------------------

std::string pj_wkt2_parse(const std::string &wkt) {
    pj_wkt2_parse_context context;
    context.pszInput = wkt.c_str();
    context.pszLastSuccess = wkt.c_str();
    context.pszNext = wkt.c_str();
    if (pj_wkt2_parse(&context) != 0) {
        return context.errorMsg;
    }
    return std::string();
}

// ---------------------------------------------------------------------------

typedef struct {
    const char *pszToken;
    int nTokenVal;
} wkt2_tokens;

#define PAIR(X)                                                                \
    {                                                                          \
#X, T_##X                                                              \
    }

static const wkt2_tokens tokens[] = {
    PAIR(PARAMETER),
    PAIR(PROJECTION),
    PAIR(DATUM),
    PAIR(SPHEROID),
    PAIR(PRIMEM),
    PAIR(UNIT),
    PAIR(AXIS),

    PAIR(GEODCRS),
    PAIR(LENGTHUNIT),
    PAIR(ANGLEUNIT),
    PAIR(SCALEUNIT),
    PAIR(TIMEUNIT),
    PAIR(ELLIPSOID),
    PAIR(CS),
    PAIR(ID),
    PAIR(PROJCRS),
    PAIR(BASEGEODCRS),
    PAIR(MERIDIAN),
    PAIR(BEARING),
    PAIR(ORDER),
    PAIR(ANCHOR),
    PAIR(ANCHOREPOCH),
    PAIR(CONVERSION),
    PAIR(METHOD),
    PAIR(REMARK),
    PAIR(GEOGCRS),
    PAIR(BASEGEOGCRS),
    PAIR(SCOPE),
    PAIR(AREA),
    PAIR(BBOX),
    PAIR(CITATION),
    PAIR(URI),
    PAIR(VERTCRS),
    PAIR(VDATUM),
    PAIR(GEOIDMODEL),
    PAIR(COMPOUNDCRS),
    PAIR(PARAMETERFILE),
    PAIR(COORDINATEOPERATION),
    PAIR(SOURCECRS),
    PAIR(TARGETCRS),
    PAIR(INTERPOLATIONCRS),
    PAIR(OPERATIONACCURACY),
    PAIR(CONCATENATEDOPERATION),
    PAIR(STEP),
    PAIR(BOUNDCRS),
    PAIR(ABRIDGEDTRANSFORMATION),
    PAIR(DERIVINGCONVERSION),
    PAIR(TDATUM),
    PAIR(CALENDAR),
    PAIR(TIMEORIGIN),
    PAIR(TIMECRS),
    PAIR(VERTICALEXTENT),
    PAIR(TIMEEXTENT),
    PAIR(USAGE),
    PAIR(DYNAMIC),
    PAIR(FRAMEEPOCH),
    PAIR(MODEL),
    PAIR(VELOCITYGRID),
    PAIR(ENSEMBLE),
    PAIR(MEMBER),
    PAIR(ENSEMBLEACCURACY),
    PAIR(DERIVEDPROJCRS),
    PAIR(BASEPROJCRS),
    PAIR(EDATUM),
    PAIR(ENGCRS),
    PAIR(PDATUM),
    PAIR(PARAMETRICCRS),
    PAIR(PARAMETRICUNIT),
    PAIR(BASEVERTCRS),
    PAIR(BASEENGCRS),
    PAIR(BASEPARAMCRS),
    PAIR(BASETIMECRS),
    PAIR(GEODETICCRS),
    PAIR(GEODETICDATUM),
    PAIR(PROJECTEDCRS),
    PAIR(PRIMEMERIDIAN),
    PAIR(GEOGRAPHICCRS),
    PAIR(TRF),
    PAIR(VERTICALCRS),
    PAIR(VERTICALDATUM),
    PAIR(VRF),
    PAIR(TIMEDATUM),
    PAIR(TEMPORALQUANTITY),
    PAIR(ENGINEERINGDATUM),
    PAIR(ENGINEERINGCRS),
    PAIR(PARAMETRICDATUM),
    PAIR(EPOCH),
    PAIR(COORDEPOCH),
    PAIR(COORDINATEMETADATA),
    PAIR(POINTMOTIONOPERATION),
    PAIR(VERSION),
    PAIR(AXISMINVALUE),
    PAIR(AXISMAXVALUE),
    PAIR(RANGEMEANING),
    PAIR(exact),
    PAIR(wraparound),
    PAIR(DEFININGTRANSFORMATION),

    // CS types
    PAIR(AFFINE),
    PAIR(CARTESIAN),
    PAIR(CYLINDRICAL),
    PAIR(ELLIPSOIDAL),
    PAIR(LINEAR),
    PAIR(PARAMETRIC),
    PAIR(POLAR),
    PAIR(SPHERICAL),
    PAIR(VERTICAL),
    PAIR(TEMPORAL),
    PAIR(TEMPORALCOUNT),
    PAIR(TEMPORALMEASURE),
    PAIR(ORDINAL),
    PAIR(TEMPORALDATETIME),

    // Axis directions
    PAIR(NORTH),
    PAIR(NORTHNORTHEAST),
    PAIR(NORTHEAST),
    PAIR(EASTNORTHEAST),
    PAIR(EAST),
    PAIR(EASTSOUTHEAST),
    PAIR(SOUTHEAST),
    PAIR(SOUTHSOUTHEAST),
    PAIR(SOUTH),
    PAIR(SOUTHSOUTHWEST),
    PAIR(SOUTHWEST),
    PAIR(WESTSOUTHWEST),
    PAIR(WEST),
    PAIR(WESTNORTHWEST),
    PAIR(NORTHWEST),
    PAIR(NORTHNORTHWEST),
    PAIR(UP),
    PAIR(DOWN),
    PAIR(GEOCENTRICX),
    PAIR(GEOCENTRICY),
    PAIR(GEOCENTRICZ),
    PAIR(COLUMNPOSITIVE),
    PAIR(COLUMNNEGATIVE),
    PAIR(ROWPOSITIVE),
    PAIR(ROWNEGATIVE),
    PAIR(DISPLAYRIGHT),
    PAIR(DISPLAYLEFT),
    PAIR(DISPLAYUP),
    PAIR(DISPLAYDOWN),
    PAIR(FORWARD),
    PAIR(AFT),
    PAIR(PORT),
    PAIR(STARBOARD),
    PAIR(CLOCKWISE),
    PAIR(COUNTERCLOCKWISE),
    PAIR(TOWARDS),
    PAIR(AWAYFROM),
    PAIR(FUTURE),
    PAIR(PAST),
    PAIR(UNSPECIFIED),
};

// ---------------------------------------------------------------------------

int pj_wkt2_lex(YYSTYPE * /*pNode */, pj_wkt2_parse_context *context) {
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
    /*      Recognize unsigned integer                                      */
    /* -------------------------------------------------------------------- */

    if (*pszInput >= '0' && *pszInput <= '9') {

        // Special case for 1, 2, 3
        if ((*pszInput == '1' || *pszInput == '2' || *pszInput == '3') &&
            !(pszInput[1] >= '0' && pszInput[1] <= '9')) {
            context->pszNext = pszInput + 1;
            return *pszInput;
        }

        pszInput++;
        while (*pszInput >= '0' && *pszInput <= '9')
            pszInput++;

        context->pszNext = pszInput;

        return T_UNSIGNED_INTEGER_DIFFERENT_ONE_TWO_THREE;
    }

    /* -------------------------------------------------------------------- */
    /*      Recognize double quoted strings.                                */
    /* -------------------------------------------------------------------- */
    if (*pszInput == '"') {
        pszInput++;
        while (*pszInput != '\0') {
            if (*pszInput == '"') {
                if (pszInput[1] == '"')
                    pszInput++;
                else
                    break;
            }
            pszInput++;
        }
        if (*pszInput == '\0') {
            context->pszNext = pszInput;
            return EOF;
        }
        context->pszNext = pszInput + 1;
        return T_STRING;
    }

    // As used in examples of OGC 12-063r5
    const char *startPrintedQuote = "\xE2\x80\x9C";
    const char *endPrintedQuote = "\xE2\x80\x9D";
    if (strncmp(pszInput, startPrintedQuote, 3) == 0) {
        context->pszNext = strstr(pszInput, endPrintedQuote);
        if (context->pszNext == nullptr) {
            context->pszNext = pszInput + strlen(pszInput);
            return EOF;
        }
        context->pszNext += 3;
        return T_STRING;
    }

    /* -------------------------------------------------------------------- */
    /*      Handle special tokens.                                          */
    /* -------------------------------------------------------------------- */
    context->pszNext = pszInput + 1;
    return *pszInput;
}

//! @endcond
