/*
 * Copyright (c) 2016-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file
 *
 * Buffer strnlen function for portability to archaic platforms.
 */

#ifndef PMIX_STRNLEN_H
#define PMIX_STRNLEN_H

#include <src/include/pmix_config.h>

#if defined(HAVE_STRNLEN)
#define PMIX_STRNLEN(c, a, b)       \
    (c) = strnlen(a, b)
#else
#define PMIX_STRNLEN(c, a, b)           \
    do {                                \
        size_t _x;                      \
        (c) = 0;                        \
        for (_x=0; _x < (b); _x++) {    \
            if ('\0' == (a)[_x]) {      \
                break;                  \
            }                           \
            ++(c);                      \
        }                               \
    } while(0)
#endif

#endif /* PMIX_STRNLEN_H */
