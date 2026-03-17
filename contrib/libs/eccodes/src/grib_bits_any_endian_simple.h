/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#pragma once

#include "grib_api_internal.h"

/* A mask with x least-significant bits set, possibly 0 or >=32 */
/* -1UL is 1111111... in every bit in binary representation */
#define BIT_MASK1(x) \
    (((x) >= max_nbits) ? (unsigned long)-1UL : (1UL << (x)) - 1)


template <typename T>
int grib_decode_array(const unsigned char* p, long* bitp, long bitsPerValue,
                             double reference_value, double s, double d,
                             size_t n_vals, T* val)
{
    size_t i               = 0;
    unsigned long lvalue = 0;
    T x;

#ifdef SLOW_OLD_CODE
    /* slow reference code */
    int j=0;
    for(i=0; i < n_vals; i++) {
        lvalue=0;
        for(j=0; j< bitsPerValue;j++){
            lvalue <<= 1;
            if(grib_get_bit( p, *bitp)) lvalue += 1;
            *bitp += 1;
        }
        x=((lvalue*s)+reference_value)*d;
        val[i] = x;
    }
#endif
    if (bitsPerValue % 8 == 0) {
        /* See ECC-386 */
        int bc;
        int l    = bitsPerValue / 8;
        size_t o = 0;

        for (i = 0; i < n_vals; i++) {
            lvalue = 0;
            lvalue <<= 8;
            lvalue |= p[o++];

            for (bc = 1; bc < l; bc++) {
                lvalue <<= 8;
                lvalue |= p[o++];
            }
            x      = ((lvalue * s) + reference_value) * d;
            val[i] = x;
            /*  *bitp += bitsPerValue * n_vals; */
        }
    }
    else {
        unsigned long mask = BIT_MASK1(bitsPerValue);

        /* pi: position of bitp in p[]. >>3 == /8 */
        long pi = *bitp / 8;
        /* some bits might of the current byte at pi might be used */
        /* by the previous number usefulBitsInByte gives remaining unused bits */
        /* number of useful bits in current byte */
        int usefulBitsInByte = 8 - (*bitp & 7);
        for (i = 0; i < n_vals; i++) {
            /* value read as long */
            long bitsToRead = 0;
            lvalue          = 0;
            bitsToRead      = bitsPerValue;
            /* read one byte after the other to lvalue until >= bitsPerValue are read */
            while (bitsToRead > 0) {
                lvalue <<= 8;
                lvalue += p[pi];
                pi++;
                bitsToRead -= usefulBitsInByte;
                usefulBitsInByte = 8;
            }
            *bitp += bitsPerValue;
            /* bitsToRead is now <= 0, remove the last bits */
            lvalue >>= -1 * bitsToRead;
            /* set leading bits to 0 - removing bits used for previous number */
            lvalue &= mask;

            usefulBitsInByte = -1 * bitsToRead; /* prepare for next round */
            if (usefulBitsInByte > 0) {
                pi--; /* reread the current byte */
            }
            else {
                usefulBitsInByte = 8; /* start with next full byte */
            }
            /* scaling and move value to output */
            x      = ((lvalue * s) + reference_value) * d;
            val[i] = x;
        }
    }
    return 0;
}
