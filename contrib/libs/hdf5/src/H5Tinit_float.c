/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://www.hdfgroup.org/licenses.               *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Purpose: Initialize native floating-point datatypes
 */

/****************/
/* Module Setup */
/****************/

#include "H5Tmodule.h" /* This source code file is part of the H5T module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                */
#include "H5Eprivate.h"  /* Error handling                   */
#include "H5FLprivate.h" /* Free Lists                       */
#include "H5Iprivate.h"  /* IDs                              */
#include "H5Tpkg.h"      /* Datatypes                        */

/****************/
/* Local Macros */
/****************/

/* This could also go in H5private.h, but this is the only place we
 * need to turn off the sanitizers and we don't want to encourage
 * this.
 */
#if defined(__has_attribute)
#if __has_attribute(no_sanitize)
#define H5_NO_UBSAN __attribute__((no_sanitize("undefined")))
#else
#define H5_NO_UBSAN
#endif
#else
#define H5_NO_UBSAN
#endif

/*-------------------------------------------------------------------------
 * Function:    DETECT_F
 *
 * Purpose:     This macro takes a floating point type like `double' and
 *              a base name like `natd' and detects byte order, mantissa
 *              location, exponent location, sign bit location, presence or
 *              absence of implicit mantissa bit, and exponent bias and
 *              initializes a detected_t structure with those properties.
 *-------------------------------------------------------------------------
 */
#define DETECT_F(TYPE, VAR, INFO)                                                                            \
    do {                                                                                                     \
        TYPE          _v1, _v2, _v3;                                                                         \
        unsigned char _buf1[sizeof(TYPE)], _buf3[sizeof(TYPE)];                                              \
        unsigned char _pad_mask[sizeof(TYPE)];                                                               \
        unsigned char _byte_mask;                                                                            \
        int           _i, _j, _last = (-1);                                                                  \
                                                                                                             \
        memset(&INFO, 0, sizeof(INFO));                                                                      \
        INFO.size = sizeof(TYPE);                                                                            \
                                                                                                             \
        /* Initialize padding mask */                                                                        \
        memset(_pad_mask, 0, sizeof(_pad_mask));                                                             \
                                                                                                             \
        /* Padding bits.  Set a variable to 4.0, then flip each bit and see if                               \
         * the modified variable is equal ("==") to the original.  Build a                                   \
         * padding bitmask to indicate which bits in the type are padding (i.e.                              \
         * have no effect on the value and should be ignored by subsequent                                   \
         * steps).  This is necessary because padding bits can change arbitrarily                            \
         * and interfere with detection of the various properties below unless we                            \
         * know to ignore them. */                                                                           \
        _v1 = (TYPE)4.0L;                                                                                    \
        H5MM_memcpy(_buf1, (const void *)&_v1, sizeof(TYPE));                                                \
        for (_i = 0; _i < (int)sizeof(TYPE); _i++)                                                           \
            for (_byte_mask = (unsigned char)1; _byte_mask; _byte_mask = (unsigned char)(_byte_mask << 1)) { \
                _buf1[_i] ^= _byte_mask;                                                                     \
                H5MM_memcpy((void *)&_v2, (const void *)_buf1, sizeof(TYPE));                                \
                H5_GCC_CLANG_DIAG_OFF("float-equal")                                                         \
                if (_v1 != _v2)                                                                              \
                    _pad_mask[_i] |= _byte_mask;                                                             \
                H5_GCC_CLANG_DIAG_ON("float-equal")                                                          \
                _buf1[_i] ^= _byte_mask;                                                                     \
            }                                                                                                \
                                                                                                             \
        /* Byte Order */                                                                                     \
        for (_i = 0, _v1 = (TYPE)0.0L, _v2 = (TYPE)1.0L; _i < (int)sizeof(TYPE); _i++) {                     \
            _v3 = _v1;                                                                                       \
            _v1 += _v2;                                                                                      \
            _v2 /= (TYPE)256.0L;                                                                             \
            H5MM_memcpy(_buf1, (const void *)&_v1, sizeof(TYPE));                                            \
            H5MM_memcpy(_buf3, (const void *)&_v3, sizeof(TYPE));                                            \
            _j = H5T__byte_cmp(sizeof(TYPE), _buf3, _buf1, _pad_mask);                                       \
            if (_j >= 0) {                                                                                   \
                INFO.perm[_i] = _j;                                                                          \
                _last         = _i;                                                                          \
            }                                                                                                \
        }                                                                                                    \
        if (H5T__fix_order(sizeof(TYPE), _last, INFO.perm, &INFO.order) < 0)                                 \
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "failed to detect byte order");                    \
                                                                                                             \
        /* Implicit mantissa bit */                                                                          \
        _v1 = (TYPE)0.5L;                                                                                    \
        _v2 = (TYPE)1.0L;                                                                                    \
        if (H5T__imp_bit(sizeof(TYPE), INFO.perm, &_v1, &_v2, _pad_mask, &(INFO.imp)) < 0)                   \
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "failed to determine implicit bit");               \
        INFO.norm = INFO.imp ? H5T_NORM_IMPLIED : H5T_NORM_NONE;                                             \
                                                                                                             \
        /* Sign bit */                                                                                       \
        _v1 = (TYPE)1.0L;                                                                                    \
        _v2 = (TYPE)-1.0L;                                                                                   \
        if (H5T__bit_cmp(sizeof(TYPE), INFO.perm, &_v1, &_v2, _pad_mask, &(INFO.sign)) < 0)                  \
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "failed to detect byte order");                    \
                                                                                                             \
        /* Mantissa */                                                                                       \
        INFO.mpos = 0;                                                                                       \
                                                                                                             \
        _v1 = (TYPE)1.0L;                                                                                    \
        _v2 = (TYPE)1.5L;                                                                                    \
        if (H5T__bit_cmp(sizeof(TYPE), INFO.perm, &_v1, &_v2, _pad_mask, &(INFO.msize)) < 0)                 \
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "failed to detect byte order");                    \
        INFO.msize += 1 + (unsigned)(INFO.imp ? 0 : 1) - INFO.mpos;                                          \
                                                                                                             \
        /* Exponent */                                                                                       \
        INFO.epos = INFO.mpos + INFO.msize;                                                                  \
                                                                                                             \
        INFO.esize = INFO.sign - INFO.epos;                                                                  \
                                                                                                             \
        _v1        = (TYPE)1.0L;                                                                             \
        INFO.ebias = H5T__find_bias(INFO.epos, INFO.esize, INFO.perm, &_v1);                                 \
        H5T__set_precision(&(INFO));                                                                         \
        COMP_ALIGNMENT(TYPE, INFO.comp_align);                                                               \
    } while (0)

/* Detect alignment for C structure */
#define COMP_ALIGNMENT(TYPE, COMP_ALIGN)                                                                     \
    {                                                                                                        \
        struct {                                                                                             \
            char c;                                                                                          \
            TYPE x;                                                                                          \
        } s;                                                                                                 \
                                                                                                             \
        COMP_ALIGN = (unsigned)((char *)(&(s.x)) - (char *)(&s));                                            \
    }

/******************/
/* Local Typedefs */
/******************/

/* Holds detected information about a native floating-point type */
typedef struct H5T_fpoint_det_t {
    unsigned      size;             /* Total byte size                  */
    unsigned      prec;             /* Meaningful bits                  */
    unsigned      offset;           /* Bit offset to meaningful bits    */
    int           perm[32];         /* For detection of byte order      */
    H5T_order_t   order;            /* byte order                       */
    unsigned      sign;             /* Location of sign bit             */
    unsigned      mpos, msize, imp; /* Information about mantissa       */
    H5T_norm_t    norm;             /* Information about mantissa       */
    unsigned      epos, esize;      /* Information about exponent       */
    unsigned long ebias;            /* Exponent bias for floating point */
    unsigned      comp_align;       /* Alignment for structure          */
} H5T_fpoint_det_t;

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

/********************/
/* Public Variables */
/********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*********************/
/* Package Variables */
/*********************/

/*******************/
/* Local Variables */
/*******************/

/* Functions used in the DETECT_F() macro */
static int      H5T__byte_cmp(int, const void *, const void *, const unsigned char *);
static herr_t   H5T__bit_cmp(unsigned, int *, void *, void *, const unsigned char *, unsigned *);
static herr_t   H5T__fix_order(int, int, int *, H5T_order_t *);
static herr_t   H5T__imp_bit(unsigned, int *, void *, void *, const unsigned char *, unsigned *);
static unsigned H5T__find_bias(unsigned, unsigned, int *, void *);
static void     H5T__set_precision(H5T_fpoint_det_t *);

/*-------------------------------------------------------------------------
 * Function:    H5T__byte_cmp
 *
 * Purpose:     Compares two chunks of memory A and B and returns the
 *              byte index into those arrays of the first byte that
 *              differs between A and B.  Ignores differences where the
 *              corresponding bit in pad_mask is set to 0.
 *
 * Return:      Success:    Index of differing byte.
 *              Failure:    -1 if all bytes are the same.
 *-------------------------------------------------------------------------
 */
static int
H5T__byte_cmp(int n, const void *_a, const void *_b, const unsigned char *pad_mask)
{
    const unsigned char *a         = (const unsigned char *)_a;
    const unsigned char *b         = (const unsigned char *)_b;
    int                  ret_value = -1;

    FUNC_ENTER_PACKAGE_NOERR

    for (int i = 0; i < n; i++)
        if ((a[i] & pad_mask[i]) != (b[i] & pad_mask[i]))
            HGOTO_DONE(i);

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5T__bit_cmp
 *
 * Purpose:     Compares two bit vectors and returns the index for the
 *              first bit that differs between the two vectors.     The
 *              size of the vector is NBYTES.  PERM is a mapping from
 *              actual order to little endian.  Ignores differences where
 *              the corresponding bit in pad_mask is set to 0.
 *
 *              Sets `first` to the index of the first differing bit
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__bit_cmp(unsigned nbytes, int *perm, void *_a, void *_b, const unsigned char *pad_mask, unsigned *first)
{
    unsigned char *a = (unsigned char *)_a;
    unsigned char *b = (unsigned char *)_b;
    unsigned char  aa, bb;
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    *first = 0;

    for (unsigned i = 0; i < nbytes; i++) {
        if (perm[i] >= (int)nbytes)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "failure in bit comparison");
        if ((aa = (unsigned char)(a[perm[i]] & pad_mask[perm[i]])) !=
            (bb = (unsigned char)(b[perm[i]] & pad_mask[perm[i]]))) {

            for (unsigned j = 0; j < 8; j++, aa >>= 1, bb >>= 1) {
                if ((aa & 1) != (bb & 1)) {
                    *first = i * 8 + j;
                    HGOTO_DONE(SUCCEED);
                }
            }
        }
    }

    /* If we got here and didn't set a value, error out */
    HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "didn't find a value for `first`");

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5T__fix_order
 *
 * Purpose:     Given an array PERM with elements FIRST through LAST
 *              initialized with zero origin byte numbers, this function
 *              creates a permutation vector that maps the actual order
 *              of a floating point number to little-endian.
 *
 *              This function assumes that the mantissa byte ordering
 *              implies the total ordering.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__fix_order(int n, int last, int *perm, H5T_order_t *order)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    if (last <= 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "failed to detect byte order");

    /* We have at least three points to consider */
    if (perm[last] < perm[last - 1] && perm[last - 1] < perm[last - 2]) {
        /* Little endian */
        *order = H5T_ORDER_LE;
        for (int i = 0; i < n; i++)
            perm[i] = i;
    }
    else if (perm[last] > perm[last - 1] && perm[last - 1] > perm[last - 2]) {
        /* Big endian */
        *order = H5T_ORDER_BE;
        for (int i = 0; i < n; i++)
            perm[i] = (n - 1) - i;
    }
    else {
        /* Undetermined endianness - defaults to 'VAX' for historical
         * reasons, but there are other mixed-endian systems (like ARM
         * in rare cases)
         */
        if (0 != n % 2)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "n is not a power of 2");

        *order = H5T_ORDER_VAX;
        for (int i = 0; i < n; i += 2) {
            perm[i]     = (n - 2) - i;
            perm[i + 1] = (n - 1) - i;
        }
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5T__imp_bit
 *
 * Purpose:     Looks for an implicit bit in the mantissa.  The value
 *              of _A should be 1.0 and the value of _B should be 0.5.
 *              Some floating-point formats discard the most significant
 *              bit of the mantissa after normalizing since it will always
 *              be a one (except for 0.0).  If this is true for the native
 *              floating point values stored in _A and _B then the function
 *              returns non-zero.
 *
 *              This function assumes that the exponent occupies higher
 *              order bits than the mantissa and that the most significant
 *              bit of the mantissa is next to the least significant bit
 *              of the exponent.
 *
 *
 * Return:      imp_bit will be set to 1 if the most significant bit
 *              of the mantissa is discarded (ie, the mantissa has an
 *              implicit `one' as the most significant bit). Otherwise,
 *              imp_bit will be set to zero zero.
 *
 *              SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__imp_bit(unsigned n, int *perm, void *_a, void *_b, const unsigned char *pad_mask, unsigned *imp_bit)
{
    unsigned char *a = (unsigned char *)_a;
    unsigned char *b = (unsigned char *)_b;
    unsigned       changed;
    unsigned       major;
    unsigned       minor;
    unsigned       msmb; /* Most significant mantissa bit */
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Look for the least significant bit that has changed between
     * A and B.  This is the least significant bit of the exponent.
     */
    if (H5T__bit_cmp(n, perm, a, b, pad_mask, &changed) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "couldn't find LSB");

    /* The bit to the right (less significant) of the changed bit should
     * be the most significant bit of the mantissa.  If it is non-zero
     * then the format does not remove the leading `1' of the mantissa.
     */
    msmb  = changed - 1;
    major = msmb / 8;
    minor = msmb % 8;

    *imp_bit = (a[perm[major]] >> minor) & 0x01 ? 0 : 1;

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:  find_bias
 *
 * Purpose:   Determines the bias of the exponent.  This function should
 *            be called with _A having a value of `1'.
 *
 * Return:    The exponent bias
 *-------------------------------------------------------------------------
 */
H5_ATTR_PURE static unsigned
H5T__find_bias(unsigned epos, unsigned esize, int *perm, void *_a)
{
    unsigned char *a = (unsigned char *)_a;
    unsigned char  mask;
    unsigned       b, shift = 0, nbits, bias = 0;

    FUNC_ENTER_PACKAGE_NOERR

    while (esize > 0) {
        nbits = MIN(esize, (8 - epos % 8));
        mask  = (unsigned char)((1 << nbits) - 1);
        b     = (unsigned)(a[perm[epos / 8]] >> (epos % 8)) & mask;
        bias |= b << shift;

        shift += nbits;
        esize -= nbits;
        epos += nbits;
    }

    FUNC_LEAVE_NOAPI(bias)
}

/*-------------------------------------------------------------------------
 * Function:    H5T__set_precision
 *
 * Purpose:     Determine the precision and offset
 *
 * Return:      void
 *-------------------------------------------------------------------------
 */
static void
H5T__set_precision(H5T_fpoint_det_t *d)
{
    FUNC_ENTER_PACKAGE_NOERR

    d->offset = MIN3(d->mpos, d->epos, d->sign);
    d->prec   = d->msize + d->esize + 1;

    FUNC_LEAVE_NOAPI_VOID
}

/*-------------------------------------------------------------------------
 * Function:    H5T__init_native_float_types
 *
 * Purpose:     Initialize native floating-point datatypes
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *-------------------------------------------------------------------------
 */
herr_t H5_NO_UBSAN
H5T__init_native_float_types(void)
{
    H5T_fpoint_det_t det;
    H5T_t           *dt        = NULL;
    herr_t           ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* H5T_NATIVE_FLOAT */

    /* Get the type's characteristics */
    memset(&det, 0, sizeof(H5T_fpoint_det_t));
    DETECT_F(float, FLOAT, det);

    /* Allocate and fill type structure */
    if (NULL == (dt = H5T__alloc()))
        HGOTO_ERROR(H5E_DATATYPE, H5E_NOSPACE, FAIL, "datatype allocation failed");
    dt->shared->state              = H5T_STATE_IMMUTABLE;
    dt->shared->type               = H5T_FLOAT;
    dt->shared->size               = det.size;
    dt->shared->u.atomic.order     = det.order;
    dt->shared->u.atomic.offset    = det.offset;
    dt->shared->u.atomic.prec      = det.prec;
    dt->shared->u.atomic.lsb_pad   = H5T_PAD_ZERO;
    dt->shared->u.atomic.msb_pad   = H5T_PAD_ZERO;
    dt->shared->u.atomic.u.f.sign  = det.sign;
    dt->shared->u.atomic.u.f.epos  = det.epos;
    dt->shared->u.atomic.u.f.esize = det.esize;
    dt->shared->u.atomic.u.f.ebias = det.ebias;
    dt->shared->u.atomic.u.f.mpos  = det.mpos;
    dt->shared->u.atomic.u.f.msize = det.msize;
    dt->shared->u.atomic.u.f.norm  = det.norm;
    dt->shared->u.atomic.u.f.pad   = H5T_PAD_ZERO;

    /* Register the type and set global variables */
    if ((H5T_NATIVE_FLOAT_g = H5I_register(H5I_DATATYPE, dt, false)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't register ID for built-in datatype");
    H5T_NATIVE_FLOAT_ALIGN_g = det.comp_align;

    /* H5T_NATIVE_DOUBLE */

    /* Get the type's characteristics */
    memset(&det, 0, sizeof(H5T_fpoint_det_t));
    DETECT_F(double, DOUBLE, det);

    /* Allocate and fill type structure */
    if (NULL == (dt = H5T__alloc()))
        HGOTO_ERROR(H5E_DATATYPE, H5E_NOSPACE, FAIL, "datatype allocation failed");
    dt->shared->state              = H5T_STATE_IMMUTABLE;
    dt->shared->type               = H5T_FLOAT;
    dt->shared->size               = det.size;
    dt->shared->u.atomic.order     = det.order;
    dt->shared->u.atomic.offset    = det.offset;
    dt->shared->u.atomic.prec      = det.prec;
    dt->shared->u.atomic.lsb_pad   = H5T_PAD_ZERO;
    dt->shared->u.atomic.msb_pad   = H5T_PAD_ZERO;
    dt->shared->u.atomic.u.f.sign  = det.sign;
    dt->shared->u.atomic.u.f.epos  = det.epos;
    dt->shared->u.atomic.u.f.esize = det.esize;
    dt->shared->u.atomic.u.f.ebias = det.ebias;
    dt->shared->u.atomic.u.f.mpos  = det.mpos;
    dt->shared->u.atomic.u.f.msize = det.msize;
    dt->shared->u.atomic.u.f.norm  = det.norm;
    dt->shared->u.atomic.u.f.pad   = H5T_PAD_ZERO;

    /* Register the type and set global variables */
    if ((H5T_NATIVE_DOUBLE_g = H5I_register(H5I_DATATYPE, dt, false)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't register ID for built-in datatype");
    H5T_NATIVE_DOUBLE_ALIGN_g = det.comp_align;

    /* H5T_NATIVE_LDOUBLE */

    /* Get the type's characteristics */
    memset(&det, 0, sizeof(H5T_fpoint_det_t));
    DETECT_F(long double, LDOUBLE, det);

    /* Allocate and fill type structure */
    if (NULL == (dt = H5T__alloc()))
        HGOTO_ERROR(H5E_DATATYPE, H5E_NOSPACE, FAIL, "datatype allocation failed");
    dt->shared->state              = H5T_STATE_IMMUTABLE;
    dt->shared->type               = H5T_FLOAT;
    dt->shared->size               = det.size;
    dt->shared->u.atomic.order     = det.order;
    dt->shared->u.atomic.offset    = det.offset;
    dt->shared->u.atomic.prec      = det.prec;
    dt->shared->u.atomic.lsb_pad   = H5T_PAD_ZERO;
    dt->shared->u.atomic.msb_pad   = H5T_PAD_ZERO;
    dt->shared->u.atomic.u.f.sign  = det.sign;
    dt->shared->u.atomic.u.f.epos  = det.epos;
    dt->shared->u.atomic.u.f.esize = det.esize;
    dt->shared->u.atomic.u.f.ebias = det.ebias;
    dt->shared->u.atomic.u.f.mpos  = det.mpos;
    dt->shared->u.atomic.u.f.msize = det.msize;
    dt->shared->u.atomic.u.f.norm  = det.norm;
    dt->shared->u.atomic.u.f.pad   = H5T_PAD_ZERO;

    /* Register the type and set global variables */
    if ((H5T_NATIVE_LDOUBLE_g = H5I_register(H5I_DATATYPE, dt, false)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't register ID for built-in datatype");
    H5T_NATIVE_LDOUBLE_ALIGN_g = det.comp_align;

    /* Set the platform's alignment (assumes long double's order
     * is true for all types)
     */
    H5T_native_order_g = det.order;

done:
    if (ret_value < 0) {
        if (dt != NULL) {
            dt->shared = H5FL_FREE(H5T_shared_t, dt->shared);
            dt         = H5FL_FREE(H5T_t, dt);
        }
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__init_native_float_types() */
