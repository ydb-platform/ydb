/***********************************************************************

 proj_strtod: Convert string to double, accepting underscore separators

                    Thomas Knudsen, 2017-01-17/09-19

************************************************************************

Conventionally, PROJ.4 does not honor locale settings, consistently
behaving as if LC_ALL=C.

For this to work, we have, for many years, been using other solutions
than the C standard library strtod/atof functions for converting strings
to doubles.

In the early versions of proj, iirc, a gnu version of strtod was used,
mostly to work around cases where the same system library was used for
C and Fortran linking, hence making strtod accept "D" and "d" as
exponentiation indicators, following Fortran Double Precision constant
syntax. This broke the proj angular syntax, accepting a "d" to mean
"degree": 12d34'56", meaning 12 degrees 34 minutes and 56 seconds.

With an explicit MIT licence, PROJ.4 could not include GPL code any
longer, and apparently at some time, the GPL code was replaced by the
current C port of a GDAL function (in pj_strtod.c), which reads the
LC_NUMERIC setting and, behind the back of the user, momentarily changes
the conventional '.' delimiter to whatever the locale requires, then
calls the system supplied strtod.

While this requires a minimum amount of coding, it only solves one
problem, and not in a very generic way.

Another problem, I would like to see solved, is the handling of underscores
as generic delimiters. This is getting popular in a number of programming
languages (Ada, C++, C#, D, Java, Julia, Perl 5, Python, Rust, etc.
cf. e.g. https://www.python.org/dev/peps/pep-0515/), and in our case of
handling numbers being in the order of magnitude of the Earth's dimensions,
and a resolution of submillimetre, i.e. having 10 or more significant digits,
splitting the "wall of digits" into smaller chunks is of immense value.

Hence this reimplementation of strtod, which hardcodes '.' as indicator of
numeric fractions, and accepts '_' anywhere in a numerical string sequence:
So a typical northing value can be written

                            6_098_907.8250 m
rather than
                             6098907.8250 m

which, in my humble opinion, is well worth the effort.

While writing this code, I took ample inspiration from Michael Ringgaard's
strtod version over at http://www.jbox.dk/sanos/source/lib/strtod.c.html,
and Yasuhiro Matsumoto's public domain version over at
https://gist.github.com/mattn/1890186. The code below is, however, not
copied from any of the two mentioned - it is a reimplementation, and
probably suffers from its own set of bugs. So for now, it is intended
not as a replacement of pj_strtod, but only as an experimental piece of
code for use in an experimental new transformation program, cct.

************************************************************************

Thomas Knudsen, thokn@sdfe.dk, 2017-01-17/2017-09-18

************************************************************************

* Copyright (c) 2017 Thomas Knudsen & SDFE
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

***********************************************************************/

#define FROM_PROJ_CPP

#include "proj_strtod.h"

#include <ctype.h>
#include <errno.h>
#include <float.h>  /* for HUGE_VAL */
#include <math.h>   /* for pow() */
#include <stdlib.h> /* for abs */
#include <string.h> /* for strchr, strncmp */

#include <limits>

#include "proj/internal/internal.hpp"

using namespace NS_PROJ::internal;

double proj_strtod(const char *str, char **endptr) {
    double number = 0, integral_part = 0;
    int exponent = 0;
    int fraction_is_nonzero = 0;
    int sign = 0;
    const char *p = str;
    int n = 0;
    int num_digits_total = 0;
    int num_digits_after_comma = 0;
    int num_prefixed_zeros = 0;

    if (nullptr == str) {
        errno = EFAULT;
        if (endptr)
            *endptr = nullptr;
        return HUGE_VAL;
    }

    /* First skip leading whitespace */
    while (isspace(*p))
        p++;

    /* Empty string? */
    if (0 == *p) {
        if (endptr)
            *endptr = const_cast<char *>(str);
        return 0;
    }

    /* NaN */
    if (ci_starts_with(p, "NaN")) {
        if (endptr)
            *endptr = const_cast<char *>(p + 3);
        return std::numeric_limits<double>::quiet_NaN();
    }

    /* non-numeric? */
    if (nullptr == strchr("0123456789+-._", *p)) {
        if (endptr)
            *endptr = const_cast<char *>(str);
        return 0;
    }

    /* Then handle optional prefixed sign and skip prefix zeros */
    switch (*p) {
    case '-':
        sign = -1;
        p++;
        break;
    case '+':
        sign = 1;
        p++;
        break;
    default:
        if (isdigit(*p) || '_' == *p || '.' == *p)
            break;
        if (endptr)
            *endptr = const_cast<char *>(str);
        return 0;
    }

    /* stray sign, as in "+/-"? */
    if (0 != sign && (nullptr == strchr("0123456789._", *p) || 0 == *p)) {
        if (endptr)
            *endptr = const_cast<char *>(str);
        return 0;
    }

    /* skip prefixed zeros before '.' */
    while ('0' == *p || '_' == *p)
        p++;

    /* zero? */
    if ((0 == *p) || nullptr == strchr("0123456789eE.", *p) || isspace(*p)) {
        if (endptr)
            *endptr = const_cast<char *>(p);
        if (sign == -1)
            return -number;
        return number;
    }

    /* Now expect a (potentially zero-length) string of digits */
    while (isdigit(*p) || ('_' == *p)) {
        if ('_' == *p) {
            p++;
            continue;
        }
        number = number * 10. + (*p - '0');
        p++;
        num_digits_total++;
    }
    integral_part = number;

    /* Done? */
    if (0 == *p) {
        if (endptr)
            *endptr = const_cast<char *>(p);
        if (sign == -1)
            return -number;
        return number;
    }

    /* Do we have a fractional part? */
    if ('.' == *p) {
        p++;

        /* keep on skipping prefixed zeros (i.e. allow writing 1e-20 */
        /* as 0.00000000000000000001 without losing precision) */
        if (0 == integral_part)
            while ('0' == *p || '_' == *p) {
                if ('0' == *p)
                    num_prefixed_zeros++;
                p++;
            }

        /* if the next character is nonnumeric, we have reached the end */
        if (0 == *p || nullptr == strchr("_0123456789eE+-", *p)) {
            if (endptr)
                *endptr = const_cast<char *>(p);
            if (sign == -1)
                return -number;
            return number;
        }

        while (isdigit(*p) || '_' == *p) {
            /* Don't let pathologically long fractions destroy precision */
            if ('_' == *p || num_digits_total > 17) {
                p++;
                continue;
            }

            number = number * 10. + (*p - '0');
            if (*p != '0')
                fraction_is_nonzero = 1;
            p++;
            num_digits_total++;
            num_digits_after_comma++;
        }

        /* Avoid having long zero-tails (4321.000...000) destroy precision */
        if (fraction_is_nonzero)
            exponent = -(num_digits_after_comma + num_prefixed_zeros);
        else
            number = integral_part;
    } /* end of fractional part */

    /* non-digit */
    if (0 == num_digits_total) {
        errno = EINVAL;
        if (endptr)
            *endptr = const_cast<char *>(p);
        return HUGE_VAL;
    }

    if (sign == -1)
        number = -number;

    /* Do we have an exponent part? */
    while (*p == 'e' || *p == 'E') {
        p++;

        /* Just a stray "e", as in 100elephants? */
        if (0 == *p || nullptr == strchr("0123456789+-_", *p)) {
            p--;
            break;
        }

        while ('_' == *p)
            p++;
        /* Does it have a sign? */
        sign = 0;
        if ('-' == *p) {
            sign = -1;
            p++;
        } else if ('+' == *p) {
            sign = +1;
            p++;
        } else if (!(isdigit(*p))) {
            if (endptr)
                *endptr = const_cast<char *>(p);
            return HUGE_VAL;
        }

        /* Go on and read the exponent */
        n = 0;
        while (isdigit(*p) || '_' == *p) {
            if ('_' == *p) {
                p++;
                continue;
            }
            n = n * 10 + (*p - '0');
            p++;
        }

        if (-1 == sign)
            n = -n;
        exponent += n;
        break;
    }

    if (endptr)
        *endptr = const_cast<char *>(p);

    if ((exponent < DBL_MIN_EXP) || (exponent > DBL_MAX_EXP)) {
        errno = ERANGE;
        return HUGE_VAL;
    }

    /* on some platforms pow() is very slow - so don't call it if exponent is
     * close to 0 */
    if (0 == exponent)
        return number;
    if (abs(exponent) < 20) {
        double ex = 1;
        int absexp = exponent < 0 ? -exponent : exponent;
        while (absexp--)
            ex *= 10;
        number = exponent < 0 ? number / ex : number * ex;
    } else
        number *= pow(10.0, static_cast<double>(exponent));

    return number;
}

double proj_atof(const char *str) { return proj_strtod(str, nullptr); }

#ifdef TEST

/* compile/run: gcc -DTEST -o proj_strtod_test proj_strtod.c  &&
 * proj_strtod_test */

#include <stdio.h>

char *un_underscore(char *s) {
    static char u[1024];
    int i, m, n;
    for (i = m = 0, n = strlen(s); i < n; i++) {
        if (s[i] == '_') {
            m++;
            continue;
        }
        u[i - m] = s[i];
    }
    u[n - m] = 0;
    return u;
}

int thetest(char *s, int line) {
    char *endp, *endq, *u;
    double p, q;
    int errnop, errnoq, prev_errno;

    prev_errno = errno;

    u = un_underscore(s);

    errno = 0;
    p = proj_strtod(s, &endp);
    errnop = errno;
    errno = 0;
    q = strtod(u, &endq);
    errnoq = errno;

    errno = prev_errno;

    if (q == p && 0 == strcmp(endp, endq) && errnop == errnoq)
        return 0;

    errno = line;
    printf("Line: %3.3d  -  [%s] [%s]\n", line, s, u);
    printf("proj_strtod: %2d %.17g  [%s]\n", errnop, p, endp);
    printf("libc_strtod: %2d %.17g  [%s]\n", errnoq, q, endq);
    return 1;
}

#define test(s) thetest(s, __LINE__)

int main(int argc, char **argv) {
    double res;
    char *endptr;

    errno = 0;

    test("");
    test("     ");
    test("     abcde");
    test("     edcba");
    test("abcde");
    test("edcba");
    test("+");
    test("-");
    test("+ ");
    test("- ");
    test(" + ");
    test(" - ");
    test("e 1");
    test("e1");
    test("0 66");
    test("1.");
    test("0.");
    test("1.0");
    test("0.0");
    test("1 ");
    test("0 ");
    test("-0 ");
    test("0_ ");
    test("0_");
    test("1e");
    test("_1.0");
    test("_0.0");
    test("1_.0");
    test("0_.0");
    test("1__.0");
    test("0__.0");
    test("1.__0");
    test("0.__0");
    test("1.0___");
    test("0.0___");
    test("1e2");
    test("__123_456_789_._10_11_12");
    test("1______");
    test("1___e__2__");
    test("-1");
    test("-1.0");
    test("-0");
    test("-1e__-_2__rest");
    test("0.00002");
    test("0.00001");
    test("-0.00002");
    test("-0.00001");
    test("-0.00001e-2");
    test("-0.00001e2");
    test("1e9999");

    /* We expect this one to differ */
    test("0."
         "000000000000000000000000000000000000000000000000000000000000000000000"
         "00"
         "0002");

    if (errno)
        printf("First discrepancy in line %d\n", errno);

    if (argc < 2)
        return 0;
    res = proj_strtod(argv[1], &endptr);
    printf("res = %20.15g. Rest = [%s],  errno = %d\n", res, endptr,
           (int)errno);
    return 0;
}
#endif
