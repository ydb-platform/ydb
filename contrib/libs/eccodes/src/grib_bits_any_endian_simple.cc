/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

/***************************************************************************
 *   Enrico Fucile  - 19.06.2007                                           *
 ***************************************************************************/

#include "grib_bits_any_endian_simple.h"

/**
 * decode an array of n_vals values from a octet-stream
 *
 * @param p input bitstream, for technical reasons put into octets
 * @param bitp current position in the bitstream
 * @param bitsPerValue number of bits needed to build a number (e.g. 8=byte, 16=short, 32=int, but also other sizes allowed)
 * @param n_vals number of values to decode
 * @param val output, values encoded as 32/64bit numbers
 */
int grib_decode_long_array(const unsigned char* p, long* bitp, long bitsPerValue,
                           size_t n_vals, long* val)
{
    unsigned long mask = BIT_MASK1(bitsPerValue);

    /* pi: position of bitp in p[]. >>3 == /8 */
    long pi = *bitp / 8;
    size_t i;
    /* number of useful bits in current byte */
    int usefulBitsInByte = 8 - (*bitp & 7);
    for (i = 0; i < n_vals; i++) {
        /* read at least enough bits (byte by byte) from input */
        long bitsToRead = bitsPerValue;
        long ret        = 0;
        while (bitsToRead > 0) {
            ret <<= 8;
            /*   ret += p[pi];         */
            /*   ECCODES_ASSERT( (ret & p[pi]) == 0 ); */
            ret = ret | p[pi];
            pi++;
            bitsToRead -= usefulBitsInByte;
            usefulBitsInByte = 8;
        }
        *bitp += bitsPerValue;
        /*fprintf(stderr, "%d %d %d %d %d\n", bitsPerValue, *bitp, pi, ret, bitsToRead);*/
        /* bitsToRead might now be negative (too many bits read) */
        /* remove those which are too much */
        ret >>= -1 * bitsToRead;
        /* remove leading bits (from previous value) */
        ret &= mask;
        val[i] = ret;

        usefulBitsInByte = -1 * bitsToRead; /* prepare for next round */
        if (usefulBitsInByte > 0) {
            pi--; /* reread the current byte */
        }
        else {
            usefulBitsInByte = 8; /* start with next full byte */
        }
    }
    return 0;
}

/**
 * decode an array of n_vals values from an octet-bitstream to double-representation
 *
 * @param p input bitstream, for technical reasons put into octets
 * @param bitp current position in the bitstream
 * @param bitsPerValue number of bits needed to build a number (e.g. 8=byte, 16=short, 32=int, but also other sizes allowed)
 * @param n_vals number of values to decode
 * @param val output, values encoded as 32/64bit numbers
 */
//int grib_decode_double_array(const unsigned char* p, long* bitp, long bitsPerValue,
//                             double reference_value, double s, double d,
//                             size_t n_vals, double* val)
//{
//    long i               = 0;
//    unsigned long lvalue = 0;
//    double x;

//#if OLD_REF_CODE
//    [> slow reference code <]
//    int j=0;
//    for(i=0;i < n_vals;i++) {
//        lvalue=0;
//        for(j=0; j< bitsPerValue;j++){
//            lvalue <<= 1;
//            if(grib_get_bit( p, *bitp)) lvalue += 1;
//            *bitp += 1;
//        }
//        x=((lvalue*s)+reference_value)*d;
//        val[i] = (double)x;
//    }
//#endif
//    if (bitsPerValue % 8 == 0) {
//        [> See ECC-386 <]
//        int bc;
//        int l    = bitsPerValue / 8;
//        size_t o = 0;

//        for (i = 0; i < n_vals; i++) {
//            lvalue = 0;
//            lvalue <<= 8;
//            lvalue |= p[o++];

//            for (bc = 1; bc < l; bc++) {
//                lvalue <<= 8;
//                lvalue |= p[o++];
//            }
//            x      = ((lvalue * s) + reference_value) * d;
//            val[i] = (double)x;
//            [>  *bitp += bitsPerValue * n_vals; <]
//        }
//    }
//    else {
//        unsigned long mask = BIT_MASK1(bitsPerValue);

//        [> pi: position of bitp in p[]. >>3 == /8 <]
//        long pi = *bitp / 8;
//        [> some bits might of the current byte at pi might be used <]
//        [> by the previous number usefulBitsInByte gives remaining unused bits <]
//        [> number of useful bits in current byte <]
//        int usefulBitsInByte = 8 - (*bitp & 7);
//        for (i = 0; i < n_vals; i++) {
//            [> value read as long <]
//            long bitsToRead = 0;
//            lvalue          = 0;
//            bitsToRead      = bitsPerValue;
//            [> read one byte after the other to lvalue until >= bitsPerValue are read <]
//            while (bitsToRead > 0) {
//                lvalue <<= 8;
//                lvalue += p[pi];
//                pi++;
//                bitsToRead -= usefulBitsInByte;
//                usefulBitsInByte = 8;
//            }
//            *bitp += bitsPerValue;
//            [> bitsToRead is now <= 0, remove the last bits <]
//            lvalue >>= -1 * bitsToRead;
//            [> set leading bits to 0 - removing bits used for previous number <]
//            lvalue &= mask;

//            usefulBitsInByte = -1 * bitsToRead; [> prepare for next round <]
//            if (usefulBitsInByte > 0) {
//                pi--; [> reread the current byte <]
//            }
//            else {
//                usefulBitsInByte = 8; [> start with next full byte <]
//            }
//            [> scaling and move value to output <]
//            x      = ((lvalue * s) + reference_value) * d;
//            val[i] = (double)x;
//        }
//    }
//    return 0;
//}

int grib_decode_double_array_complex(const unsigned char* p, long* bitp, long nbits, double reference_value, double s, double* d, size_t size, double* val)
{
    return GRIB_NOT_IMPLEMENTED;
}

int grib_encode_long_array(size_t n_vals, const long* val, long bits_per_value, unsigned char* p, long* off)
{
    size_t i                   = 0;
    unsigned long unsigned_val = 0;
    unsigned char* encoded     = p;
    if (bits_per_value % 8) {
        for (i = 0; i < n_vals; i++) {
            unsigned_val = val[i];
            grib_encode_unsigned_longb(encoded, unsigned_val, off, bits_per_value);
        }
    }
    else {
        for (i = 0; i < n_vals; i++) {
            int blen     = 0;
            blen         = bits_per_value;
            unsigned_val = val[i];
            while (blen >= 8) {
                blen -= 8;
                *encoded = (unsigned_val >> blen);
                encoded++;
                *off += 8;
            }
        }
    }
    return GRIB_SUCCESS;
}

int grib_encode_double_array(size_t n_vals, const double* val, long bits_per_value, double reference_value, double d, double divisor, unsigned char* p, long* off)
{
    size_t i                   = 0;
    unsigned long unsigned_val = 0;
    unsigned char* encoded     = p;
    double x;
    if (bits_per_value % 8) {
        for (i = 0; i < n_vals; i++) {
            x            = (((val[i] * d) - reference_value) * divisor) + 0.5;
            unsigned_val = (unsigned long)x;
            grib_encode_unsigned_longb(encoded, unsigned_val, off, bits_per_value);
        }
    }
    else {
        for (i = 0; i < n_vals; i++) {
            int blen     = 0;
            blen         = bits_per_value;
            x            = ((((val[i] * d) - reference_value) * divisor) + 0.5);
            unsigned_val = (unsigned long)x;
            while (blen >= 8) {
                blen -= 8;
                *encoded = (unsigned_val >> blen);
                encoded++;
                *off += 8;
            }
        }
    }
    return GRIB_SUCCESS;
}

int grib_encode_double_array_complex(size_t n_vals, double* val, long nbits, double reference_value,
                                     double* scal, double d, double divisor, unsigned char* p, long* bitp)
{
    return GRIB_NOT_IMPLEMENTED;
}
