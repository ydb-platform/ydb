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

#ifdef ECCODES_ON_WINDOWS
#include <stdint.h>
#endif

#include "grib_scaling.h"

#if GRIB_PTHREADS
static pthread_once_t once   = PTHREAD_ONCE_INIT;
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

static void init_mutex()
{
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&mutex, &attr);
    pthread_mutexattr_destroy(&attr);
}
#elif GRIB_OMP_THREADS
static int once = 0;
static omp_nest_lock_t mutex;

static void init_mutex()
{
    GRIB_OMP_CRITICAL(lock_grib_bits_any_endian_c)
    {
        if (once == 0) {
            omp_init_nest_lock(&mutex);
            once = 1;
        }
    }
}
#endif

typedef struct bits_all_one_t
{
    int inited;
    int size;
    int64_t v[128];
} bits_all_one_t;

static bits_all_one_t bits_all_one = { 0, 0, {0,} };

static void init_bits_all_one()
{
    int size            = sizeof(int64_t) * 8;
    int64_t* v             = 0;
    uint64_t cmask = -1;
    DEBUG_ASSERT(!bits_all_one.inited);

    bits_all_one.size   = size;
    bits_all_one.inited = 1;
    v                   = bits_all_one.v + size;
    /*
        * The result of a shift operation is undefined if the RHS is negative or
        * greater than or equal to the number of bits in the (promoted) shift-expression
        */
    /* *v= cmask << size; */
    *v = -1;
    while (size > 0)
        *(--v) = ~(cmask << --size);
}

static void init_bits_all_one_if_needed()
{
    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex);
    if (!bits_all_one.inited)
        init_bits_all_one();
    GRIB_MUTEX_UNLOCK(&mutex);
}
int grib_is_all_bits_one(int64_t val, long nbits)
{
    /*if (!bits_all_one.inited) init_bits_all_one();*/
    init_bits_all_one_if_needed();
    return bits_all_one.v[nbits] == val;
}

int grib_encode_string(unsigned char* bitStream, long* bitOffset, size_t numberOfCharacters, const char* string)
{
    size_t i = 0, slen = 0;
    int err         = 0;
    long byteOffset = *bitOffset / 8;
    int remainder   = *bitOffset % 8;
    unsigned char c;
    unsigned char* p;
    const unsigned char mask[]    = { 0, 0x80, 0xC0, 0xE0, 0xF0, 0xF8, 0xFC, 0xFE };
    int remainderComplement = 8 - remainder;
    char str[512]           = {0,};
    char* s = str;

    ECCODES_ASSERT(numberOfCharacters < 512);

    /* There is a case where string == NULL:
     * bufr_dump -Efortran data/bufr/btem_109.bufr
     * This writes:
     *    call codes_set(ibufr,'shipOrMobileLandStationIdentifier','')
     * For some odd reason this gets passed in as a NULL string here!
     * To be further investigated
    */
    if (string) {
        slen = strlen(string);
        if (slen > numberOfCharacters) {
            return GRIB_ENCODING_ERROR;
        }
        memcpy(s, string, slen);
    }
    /* if (remainder) byteOffset++; */

    if (numberOfCharacters == 0)
        return err;

    p = (unsigned char*)bitStream + byteOffset;

    if (remainder == 0) {
        memcpy(p, str, numberOfCharacters);
        *bitOffset += numberOfCharacters * 8;
        return err;
    }
    DEBUG_ASSERT(remainderComplement >= 0);
    for (i = 0; i < numberOfCharacters; i++) {
        c = ((*s) >> remainder) & ~mask[remainder];
        *p |= c;
        p++;
        /* See ECC-1396: left-shift operator is undefined on a negative number */
        if (*s > 0)
            *p = ((*s) << remainderComplement) & mask[remainder];
        else
            *p = (*s) & mask[remainder];
        s++;
    }
    *bitOffset += numberOfCharacters * 8;
    return err;
}

char* grib_decode_string(const unsigned char* bitStream, long* bitOffset, size_t numberOfCharacters, char* string)
{
    size_t i;
    long byteOffset = *bitOffset / 8;
    int remainder   = *bitOffset % 8;
    unsigned char c;
    unsigned char* p;
    char* s                 = string;
    const unsigned char mask[] = { 0, 0x80, 0xC0, 0xE0, 0xF0, 0xF8, 0xFC, 0xFE };
    int remainderComplement = 8 - remainder;

    if (numberOfCharacters == 0)
        return string;

    p = (unsigned char*)bitStream + byteOffset;

    if (remainder == 0) {
        memcpy(string, bitStream + byteOffset, numberOfCharacters);
        *bitOffset += numberOfCharacters * 8;
        return string;
    }

    DEBUG_ASSERT(remainderComplement >= 0);
    for (i = 0; i < numberOfCharacters; i++) {
        c = (*p) << remainder;
        p++;
        *s = (c | ((*p) & mask[remainder]) >> remainderComplement);
        s++;
    }
    *bitOffset += numberOfCharacters * 8;
    return string;
}

/* A mask with x least-significant bits set, possibly 0 or >=32 */
/* -1UL is 1111111... in every bit in binary representation */
#define BIT_MASK(x) \
    (((x) == max_nbits) ? (unsigned long)-1UL : (1UL << (x)) - 1)
/**
 * decode a value consisting of nbits from an octet-bitstream to long-representation
 *
 * @param p input bitstream, for technical reasons put into octets
 * @param bitp current start position in the bitstream
 * @param nbits number of bits needed to build a number (e.g. 8=byte, 16=short, 32=int, but also other sizes allowed)
 * @return value encoded as 32/64bit numbers
 */
unsigned long grib_decode_unsigned_long(const unsigned char* p, long* bitp, long nbits)
{
    unsigned long ret    = 0;
    long oc              = *bitp / 8;
    unsigned long mask   = 0;
    long pi              = 0;
    int usefulBitsInByte = 0;
    long bitsToRead      = 0;

    if (nbits == 0)
        return 0;

    if (nbits > max_nbits) {
        int bits = nbits;
        int mod  = bits % max_nbits;

        if (mod != 0) {
            int e = grib_decode_unsigned_long(p, bitp, mod);
            ECCODES_ASSERT(e == 0);
            bits -= mod;
        }

        while (bits > max_nbits) {
            int e = grib_decode_unsigned_long(p, bitp, max_nbits);
            ECCODES_ASSERT(e == 0);
            bits -= max_nbits;
        }

        return grib_decode_unsigned_long(p, bitp, bits);
    }

    // Old algorithm:
    //  long ret2 = 0;
    //  for(i=0; i< nbits;i++){
    //     ret2 <<= 1;
    //     if(grib_get_bit( p, *bitp)) ret2 += 1;
    //     *bitp += 1;
    //  }
    //  *bitp -= nbits;

    mask = BIT_MASK(nbits);
    /* pi: position of bitp in p[]. >>3 == /8 */
    pi = oc;
    /* number of useful bits in current byte */
    usefulBitsInByte = 8 - (*bitp & 7);
    /* read at least enough bits (byte by byte) from input */
    bitsToRead = nbits;
    while (bitsToRead > 0) {
        ret <<= 8;
        /*   ret += p[pi];     */
        DEBUG_ASSERT((ret & p[pi]) == 0);
        ret = ret | p[pi];
        pi++;
        bitsToRead -= usefulBitsInByte;
        usefulBitsInByte = 8;
    }
    *bitp += nbits;
    /* printf("%d %d %d\n", pi, ret, offset); */
    /* bitsToRead might now be negative (too many bits read) */
    /* remove those which are too much */
    ret >>= -1 * bitsToRead;
    /* remove leading bits (from previous value) */
    ret &= mask;
    /* printf("%d %d\n", ret2, ret);*/

    return ret;
}

int grib_encode_unsigned_long(unsigned char* p, unsigned long val, long* bitp, long nbits)
{
    long len          = nbits;
    int s             = *bitp % 8;
    int n             = 8 - s;
    unsigned char tmp = 0; /*for temporary results*/

    if (nbits > max_nbits) {
        /* TODO: Do some real code here, to support long longs */
        int bits  = nbits;
        int mod   = bits % max_nbits;
        long zero = 0;

        if (mod != 0) {
            int e = grib_encode_unsigned_long(p, zero, bitp, mod);
            /* printf(" -> : encoding %ld bits=%ld %ld\n",zero,(long)mod,*bitp); */
            ECCODES_ASSERT(e == 0);
            bits -= mod;
        }

        while (bits > max_nbits) {
            int e = grib_encode_unsigned_long(p, zero, bitp, max_nbits);
            /* printf(" -> : encoding %ld bits=%ld %ld\n",zero,(long)max_nbits,*bitp); */
            ECCODES_ASSERT(e == 0);
            bits -= max_nbits;
        }

        /* printf(" -> : encoding %ld bits=%ld %ld\n",val,(long)bits,*bitp); */
        return grib_encode_unsigned_long(p, val, bitp, bits);
    }

    p += (*bitp >> 3); /* skip the bytes */

    /* head */
    if (s) {
        len -= n;
        if (len < 0) {
            tmp = ((val << -len) | ((*p) & dmasks[n]));
        }
        else {
            tmp = ((val >> len) | ((*p) & dmasks[n]));
        }
        *p++ = tmp;
    }

    /*  write the middle words */
    while (len >= 8) {
        len -= 8;
        *p++ = (val >> len);
    }

    /*  write the end bits */
    if (len)
        *p = (val << (8 - len));

    *bitp += nbits;
    return GRIB_SUCCESS;
}

/*
 * Note: On x64 Micrsoft Windows a "long" is 32 bits but "size_t" is 64 bits
 */
#define BIT_MASK_SIZE_T(x) \
    (((x) == max_nbits_size_t) ? (size_t)-1ULL : (1ULL << (x)) - 1)

size_t grib_decode_size_t(const unsigned char* p, long* bitp, long nbits)
{
    size_t ret           = 0;
    long oc              = *bitp / 8;
    size_t mask          = 0;
    long pi              = 0;
    int usefulBitsInByte = 0;
    long bitsToRead      = 0;

    if (nbits == 0)
        return 0;

    if (nbits > max_nbits_size_t) {
        int bits = nbits;
        int mod  = bits % max_nbits_size_t;

        if (mod != 0) {
            int e = grib_decode_size_t(p, bitp, mod);
            ECCODES_ASSERT(e == 0);
            bits -= mod;
        }

        while (bits > max_nbits_size_t) {
            int e = grib_decode_size_t(p, bitp, max_nbits_size_t);
            ECCODES_ASSERT(e == 0);
            bits -= max_nbits_size_t;
        }

        return grib_decode_size_t(p, bitp, bits);
    }

    mask = BIT_MASK_SIZE_T(nbits);
    /* pi: position of bitp in p[]. >>3 == /8 */
    pi = oc;
    /* number of useful bits in current byte */
    usefulBitsInByte = 8 - (*bitp & 7);
    /* read at least enough bits (byte by byte) from input */
    bitsToRead = nbits;
    while (bitsToRead > 0) {
        ret <<= 8;
        /*   ret += p[pi];     */
        DEBUG_ASSERT((ret & p[pi]) == 0);
        ret = ret | p[pi];
        pi++;
        bitsToRead -= usefulBitsInByte;
        usefulBitsInByte = 8;
    }
    *bitp += nbits;

    /* bitsToRead might now be negative (too many bits read) */
    /* remove those which are too much */
    ret >>= -1 * bitsToRead;
    /* remove leading bits (from previous value) */
    ret &= mask;

    return ret;
}

int grib_encode_unsigned_longb(unsigned char* p, unsigned long val, long* bitp, long nb)
{
    if (nb > max_nbits) {
        fprintf(stderr, "Number of bits (%ld) exceeds maximum number of bits (%d)\n", nb, max_nbits);
        ECCODES_ASSERT(0);
        return GRIB_INTERNAL_ERROR;
    }

    const unsigned long maxV = codes_power<double>(nb, 2) - 1;
    if (val > maxV) {
        fprintf(stderr, "ECCODES WARNING :  %s: Trying to encode value of %lu but the maximum allowable value is %lu (number of bits=%ld)\n",
                __func__, val, maxV, nb);
    }

    for (long i = nb - 1; i >= 0; i--) {
        if (test(val, i))
            grib_set_bit_on(p, bitp);
        else
            grib_set_bit_off(p, bitp);
    }
    return GRIB_SUCCESS;
}

/*
 * Note: On x64 Micrsoft Windows a "long" is 32 bits but "size_t" is 64 bits
 */
int grib_encode_size_tb(unsigned char* p, size_t val, long* bitp, long nb)
{
    if (nb > max_nbits_size_t) {
        fprintf(stderr, "Number of bits (%ld) exceeds maximum number of bits (%d)\n", nb, max_nbits_size_t);
        ECCODES_ASSERT(0);
    }

    const size_t maxV = codes_power<double>(nb, 2) - 1;
    if (val > maxV) {
        fprintf(stderr, "ECCODES WARNING :  %s: Trying to encode value of %zu but the maximum allowable value is %zu (number of bits=%ld)\n",
                __func__, val, maxV, nb);
    }

    for (long i = nb - 1; i >= 0; i--) {
        if (test(val, i))
            grib_set_bit_on(p, bitp);
        else
            grib_set_bit_off(p, bitp);
    }
    return GRIB_SUCCESS;
}

#if OMP_PACKING
#error #include "grib_bits_any_endian_omp.cc"
#elif VECTOR
#error #include "grib_bits_any_endian_vector.cc"  /* Experimental */
#else
#include "grib_bits_any_endian_simple.cc"
#endif
