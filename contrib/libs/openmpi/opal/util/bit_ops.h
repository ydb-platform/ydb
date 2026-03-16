/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2011 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_BIT_OPS_H
#define OPAL_BIT_OPS_H

#include "opal/prefetch.h"

/**
 * Calculates the highest bit in an integer
 *
 * @param value The integer value to examine
 * @param start Position to start looking
 *
 * @returns pos Position of highest-set integer or -1 if none are set.
 *
 * Look at the integer "value" starting at position "start", and move
 * to the right.  Return the index of the highest bit that is set to
 * 1.
 *
 * WARNING: *NO* error checking is performed.  This is meant to be a
 * fast inline function.
 * Using __builtin_clz (count-leading-zeros) uses 3 cycles instead
 * of 17 cycles (on average value, with start=32)
 * compared to the loop-version (on Intel Nehalem -- with icc-12.1.0 -O2).
 */
static inline int opal_hibit(int value, int start)
{
    unsigned int mask;

#if OPAL_C_HAVE_BUILTIN_CLZ
    /* Only look at the part that the caller wanted looking at */
    mask = value & ((1 << start) - 1);

    if (OPAL_UNLIKELY (0 == mask)) {
        return -1;
    }

    start = (8*sizeof(int)-1) - __builtin_clz(mask);
#else
    --start;
    mask = 1 << start;

    for (; start >= 0; --start, mask >>= 1) {
        if (value & mask) {
            break;
        }
    }
#endif

    return start;
}


/**
 * Returns the cube dimension of a given value.
 *
 * @param value The integer value to examine
 *
 * @returns cubedim The smallest cube dimension containing that value
 *
 * Look at the integer "value" and calculate the smallest power of two
 * dimension that contains that value.
 *
 * WARNING: *NO* error checking is performed.  This is meant to be a
 * fast inline function.
 * Using __builtin_clz (count-leading-zeros) uses 3 cycles instead of 50 cycles
 * compared to the loop-version (on Intel Nehalem -- with icc-12.1.0 -O2).
 */
static inline int opal_cube_dim(int value)
{
    int dim, size;

#if OPAL_C_HAVE_BUILTIN_CLZ
    if (OPAL_UNLIKELY (1 >= value)) {
        return 0;
    }
    size = 8 * sizeof(int);
    dim = size - __builtin_clz(value-1);
#else
    for (dim = 0, size = 1; size < value; ++dim, size <<= 1) /* empty */;
#endif

    return dim;
}


/**
 * @brief Returns next power-of-two of the given value.
 *
 * @param value The integer value to return power of 2
 *
 * @returns The next power of two
 *
 * WARNING: *NO* error checking is performed.  This is meant to be a
 * fast inline function.
 * Using __builtin_clz (count-leading-zeros) uses 4 cycles instead of 77
 * compared to the loop-version (on Intel Nehalem -- with icc-12.1.0 -O2).
 */
static inline int opal_next_poweroftwo(int value)
{
    int power2;

#if OPAL_C_HAVE_BUILTIN_CLZ
    if (OPAL_UNLIKELY (0 == value)) {
        return 1;
    }
    power2 = 1 << (8 * sizeof (int) - __builtin_clz(value));
#else
    for (power2 = 1; value > 0; value >>= 1, power2 <<= 1) /* empty */;
#endif

    return power2;
}


/**
 * @brief Returns next power-of-two of the given value (and the value itselve if already power-of-two).
 *
 * @param value The integer value to return power of 2
 *
 * @returns The next power of two (inclusive)
 *
 * WARNING: *NO* error checking is performed.  This is meant to be a
 * fast inline function.
 * Using __builtin_clz (count-leading-zeros) uses 4 cycles instead of 56
 * compared to the loop-version (on Intel Nehalem -- with icc-12.1.0 -O2).
 */
static inline int opal_next_poweroftwo_inclusive(int value)
{
    int power2;

#if OPAL_C_HAVE_BUILTIN_CLZ
    if (OPAL_UNLIKELY (1 >= value)) {
        return 1;
    }
    power2 = 1 << (8 * sizeof (int) - __builtin_clz(value - 1));
#else
    for (power2 = 1 ; power2 < value; power2 <<= 1) /* empty */;
#endif

    return power2;
}


#endif /* OPAL_BIT_OPS_H */

