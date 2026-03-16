/******************************************************************************
 *
 * Project:  PROJ
 * Purpose:  Utilities for command line arguments
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

#include "utils.h"

#include <stdlib.h>
#include <string.h>

bool validate_form_string_for_numbers(const char *formatString) {
    /* Only accepts '%[+]?[number]?[.]?[number]?[e|E|f|F|g|G]' */
    bool valid = true;
    if (formatString[0] != '%')
        valid = false;
    else {
        const auto oformLen = strlen(formatString);
        for (int i = 1; i < static_cast<int>(oformLen) - 1; i++) {
            if (!(formatString[i] == '.' || formatString[i] == '+' ||
                  (formatString[i] >= '0' && formatString[i] <= '9'))) {
                valid = false;
                break;
            }
        }
        if (valid) {
            valid = formatString[oformLen - 1] == 'e' ||
                    formatString[oformLen - 1] == 'E' ||
                    formatString[oformLen - 1] == 'f' ||
                    formatString[oformLen - 1] == 'F' ||
                    formatString[oformLen - 1] == 'g' ||
                    formatString[oformLen - 1] == 'G';
        }
    }
    return valid;
}

// Some older versions of mingw_w64 do not support the F formatter
// GCC 7 might not be the exact version...
#if defined(__MINGW32__) && __GNUC__ <= 7
#define MY_FPRINTF0(fmt0, fmt, ...)                                            \
    do {                                                                       \
        if (*ptr == 'e')                                                       \
            fprintf(f, "%" fmt0 fmt "e", __VA_ARGS__);                         \
        else if (*ptr == 'E')                                                  \
            fprintf(f, "%" fmt0 fmt "E", __VA_ARGS__);                         \
        else if (*ptr == 'f')                                                  \
            fprintf(f, "%" fmt0 fmt "f", __VA_ARGS__);                         \
        else if (*ptr == 'g')                                                  \
            fprintf(f, "%" fmt0 fmt "g", __VA_ARGS__);                         \
        else if (*ptr == 'G')                                                  \
            fprintf(f, "%" fmt0 fmt "G", __VA_ARGS__);                         \
        else {                                                                 \
            fprintf(stderr, "Wrong formatString '%s'\n", formatString);        \
            return;                                                            \
        }                                                                      \
        ++ptr;                                                                 \
    } while (0)
#else
#define MY_FPRINTF0(fmt0, fmt, ...)                                            \
    do {                                                                       \
        if (*ptr == 'e')                                                       \
            fprintf(f, "%" fmt0 fmt "e", __VA_ARGS__);                         \
        else if (*ptr == 'E')                                                  \
            fprintf(f, "%" fmt0 fmt "E", __VA_ARGS__);                         \
        else if (*ptr == 'f')                                                  \
            fprintf(f, "%" fmt0 fmt "f", __VA_ARGS__);                         \
        else if (*ptr == 'F')                                                  \
            fprintf(f, "%" fmt0 fmt "F", __VA_ARGS__);                         \
        else if (*ptr == 'g')                                                  \
            fprintf(f, "%" fmt0 fmt "g", __VA_ARGS__);                         \
        else if (*ptr == 'G')                                                  \
            fprintf(f, "%" fmt0 fmt "G", __VA_ARGS__);                         \
        else {                                                                 \
            fprintf(stderr, "Wrong formatString '%s'\n", formatString);        \
            return;                                                            \
        }                                                                      \
        ++ptr;                                                                 \
    } while (0)
#endif

#define MY_FPRINTF(fmt, ...)                                                   \
    do {                                                                       \
        if (withPlus)                                                          \
            MY_FPRINTF0("+", fmt, __VA_ARGS__);                                \
        else                                                                   \
            MY_FPRINTF0("", fmt, __VA_ARGS__);                                 \
    } while (0)

static int parseInt(const char *&ptr) {
    int val = 0;
    while (*ptr >= '0' && *ptr <= '9') {
        val = val * 10 + (*ptr - '0');
        if (val > 1000) {
            return -1;
        }
        ++ptr;
    }
    return val;
}

// This function is a limited version of fprintf(f, formatString, val) where
// formatString is a subset of formatting strings accepted by
// validate_form_string_for_numbers().
// This methods makes CodeQL cpp/tainted-format-string check happy.
void limited_fprintf_for_number(FILE *f, const char *formatString, double val) {
    const char *ptr = formatString;
    if (*ptr != '%') {
        fprintf(stderr, "Wrong formatString '%s'\n", formatString);
        return;
    }
    ++ptr;
    const bool withPlus = (*ptr == '+');
    if (withPlus) {
        ++ptr;
    }
    if (*ptr >= '0' && *ptr <= '9') {
        const bool isLeadingZero = *ptr == '0';
        const int w = parseInt(ptr);
        if (w < 0 || *ptr == 0) {
            fprintf(stderr, "Wrong formatString '%s'\n", formatString);
            return;
        }
        if (*ptr == '.') {
            ++ptr;
            if (*ptr >= '0' && *ptr <= '9') {
                const int p = parseInt(ptr);
                if (p < 0 || *ptr == 0) {
                    fprintf(stderr, "Wrong formatString '%s'\n", formatString);
                    return;
                }
                if (isLeadingZero) {
                    MY_FPRINTF("0*.*", w, p, val);
                } else {
                    MY_FPRINTF("*.*", w, p, val);
                }
            } else {
                if (isLeadingZero) {
                    MY_FPRINTF("0*.", w, val);
                } else {
                    MY_FPRINTF("*.", w, val);
                }
            }
        } else {
            if (isLeadingZero) {
                MY_FPRINTF("0*", w, val);
            } else {
                MY_FPRINTF("*", w, val);
            }
        }
    } else if (*ptr == '.') {
        ++ptr;
        if (*ptr >= '0' && *ptr <= '9') {
            const int p = parseInt(ptr);
            if (p < 0 || *ptr == 0) {
                fprintf(stderr, "Wrong formatString '%s'\n", formatString);
                return;
            }
            MY_FPRINTF(".*", p, val);
        } else {
            MY_FPRINTF(".", val);
        }
    } else {
        MY_FPRINTF("", val);
    }
    if (*ptr != 0) {
        fprintf(stderr, "Wrong formatString '%s'\n", formatString);
        return;
    }
}
