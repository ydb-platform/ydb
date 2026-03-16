/******************************************************************************
 *
 * Derived from GDAL port/cpl_strtod.cpp
 * Purpose:  Functions to convert ASCII string to floating point number.
 * Author:   Andrey Kiselev, dron@ak4719.spb.edu.
 *
 ******************************************************************************
 * Copyright (c) 2006, Andrey Kiselev
 * Copyright (c) 2008-2012, Even Rouault <even dot rouault at mines-paris dot
 *org>
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

#include <errno.h>
#include <locale.h>
#include <stdlib.h>
#include <string.h>

#include "proj.h"
#include "proj_config.h"
#include "proj_internal.h"

/************************************************************************/
/*                              pj_atof()                               */
/************************************************************************/

/**
 * Converts ASCII string to floating point number.
 *
 * This function converts the initial portion of the string pointed to
 * by nptr to double floating point representation. The behavior is the
 * same as
 *
 *   pj_strtod(nptr, nullptr);
 *
 * This function does the same as standard atof(3), but does not take
 * locale in account. That means, the decimal delimiter is always '.'
 * (decimal point).
 *
 * @param nptr Pointer to string to convert.
 *
 * @return Converted value.
 */
double pj_atof(const char *nptr) { return pj_strtod(nptr, nullptr); }

/************************************************************************/
/*                     replace_point_by_locale_point()               */
/************************************************************************/

static char *replace_point_by_locale_point(const char *pszNumber, char point) {
#if !defined(HAVE_LOCALECONV)

#if defined(_MSC_VER) /* Visual C++ */
#pragma message("localeconv not available")
#else
#warning "localeconv not available"
#endif

    static char byPoint = 0;
    if (byPoint == 0) {
        char szBuf[16];
        snprintf(szBuf, sizeof(szBuf), "%.1f", 1.0);
        byPoint = szBuf[1];
    }
    if (point != byPoint) {
        const char *pszPoint = strchr(pszNumber, point);
        if (pszPoint) {
            char *pszNew = pj_strdup(pszNumber);
            if (!pszNew)
                return nullptr;
            pszNew[pszPoint - pszNumber] = byPoint;
            return pszNew;
        }
    }

    return nullptr;

#else

    const struct lconv *poLconv = localeconv();
    if (poLconv && poLconv->decimal_point &&
        poLconv->decimal_point[0] != '\0') {
        char byPoint = poLconv->decimal_point[0];

        if (point != byPoint) {
            const char *pszLocalePoint = strchr(pszNumber, byPoint);
            const char *pszPoint = strchr(pszNumber, point);
            if (pszPoint || pszLocalePoint) {
                char *pszNew = pj_strdup(pszNumber);
                if (!pszNew)
                    return nullptr;
                if (pszLocalePoint)
                    pszNew[pszLocalePoint - pszNumber] = ' ';
                if (pszPoint)
                    pszNew[pszPoint - pszNumber] = byPoint;
                return pszNew;
            }
        }
    }

    return nullptr;

#endif
}

/************************************************************************/
/*                            pj_strtod()                               */
/************************************************************************/

/**
 * Converts ASCII string to floating point number.
 *
 * This function converts the initial portion of the string pointed to
 * by nptr to double floating point representation. This function does the
 * same as standard strtod(3), but does not take locale in account and use
 * decimal point.
 *
 * @param nptr Pointer to string to convert.
 * @param endptr If is not NULL, a pointer to the character after the last
 * character used in the conversion is stored in the location referenced
 * by endptr.
 *
 * @return Converted value.
 */
double pj_strtod(const char *nptr, char **endptr) {
    /* -------------------------------------------------------------------- */
    /*  We are implementing a simple method here: copy the input string     */
    /*  into the temporary buffer, replace the specified decimal delimiter (.)
     */
    /*  with the one taken from locale settings (ex ',') and then use standard
     * strtod()  */
    /*  on that buffer.                                                     */
    /* -------------------------------------------------------------------- */
    char *pszNumber = replace_point_by_locale_point(nptr, '.');
    if (pszNumber) {
        char *pszNumberEnd;
        double dfValue = strtod(pszNumber, &pszNumberEnd);

        int nError = errno;

        if (endptr) {
            ptrdiff_t offset = pszNumberEnd - pszNumber;
            *endptr = const_cast<char *>(nptr + offset);
        }

        free(pszNumber);

        errno = nError;

        return dfValue;
    } else {
        return strtod(nptr, endptr);
    }
}
