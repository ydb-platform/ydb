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

#include "H5Zmodule.h" /* This source code file is part of the H5Z module */

#include "H5private.h"   /* Generic Functions            */
#include "H5ACprivate.h" /* Metadata cache            */
#include "H5Eprivate.h"  /* Error handling              */
#include "H5Iprivate.h"  /* IDs                      */
#include "H5MMprivate.h" /* Memory management            */
#include "H5Pprivate.h"  /* Property lists                       */
#include "H5Oprivate.h"  /* Object headers                       */
#include "H5Sprivate.h"  /* Dataspaces                     */
#include "H5Tprivate.h"  /* Datatypes                     */
#include "H5Zpkg.h"      /* Data filters                */

/* Struct of parameters needed for compressing/decompressing one atomic datatype */
typedef struct {
    unsigned size;      /* datatype size */
    uint32_t minbits;   /* minimum bits to compress one value of such datatype */
    unsigned mem_order; /* current memory endianness order */
} parms_atomic;

enum H5Z_scaleoffset_t {
    t_bad   = 0,
    t_uchar = 1,
    t_ushort,
    t_uint,
    t_ulong,
    t_ulong_long,
    t_schar,
    t_short,
    t_int,
    t_long,
    t_long_long,
    t_float,
    t_double
};

/* Local function prototypes */
static htri_t                 H5Z__can_apply_scaleoffset(hid_t dcpl_id, hid_t type_id, hid_t space_id);
static enum H5Z_scaleoffset_t H5Z__scaleoffset_get_type(unsigned dtype_class, unsigned dtype_size,
                                                        unsigned dtype_sign);
static herr_t                 H5Z__scaleoffset_set_parms_fillval(H5P_genplist_t *dcpl_plist, H5T_t *type,
                                                                 enum H5Z_scaleoffset_t scale_type, unsigned cd_values[],
                                                                 int need_convert);
static herr_t                 H5Z__set_local_scaleoffset(hid_t dcpl_id, hid_t type_id, hid_t space_id);
static size_t H5Z__filter_scaleoffset(unsigned flags, size_t cd_nelmts, const unsigned cd_values[],
                                      size_t nbytes, size_t *buf_size, void **buf);
static void   H5Z__scaleoffset_convert(void *buf, unsigned d_nelmts, unsigned dtype_size);
static H5_ATTR_CONST unsigned H5Z__scaleoffset_log2(unsigned long long num);
static void   H5Z__scaleoffset_precompress_i(void *data, unsigned d_nelmts, enum H5Z_scaleoffset_t type,
                                             unsigned filavail, const unsigned cd_values[], uint32_t *minbits,
                                             unsigned long long *minval);
static void   H5Z__scaleoffset_postdecompress_i(void *data, unsigned d_nelmts, enum H5Z_scaleoffset_t type,
                                                unsigned filavail, const unsigned cd_values[], uint32_t minbits,
                                                unsigned long long minval);
static herr_t H5Z__scaleoffset_precompress_fd(void *data, unsigned d_nelmts, enum H5Z_scaleoffset_t type,
                                              unsigned filavail, const unsigned cd_values[],
                                              uint32_t *minbits, unsigned long long *minval, double D_val);
static herr_t H5Z__scaleoffset_postdecompress_fd(void *data, unsigned d_nelmts, enum H5Z_scaleoffset_t type,
                                                 unsigned filavail, const unsigned cd_values[],
                                                 uint32_t minbits, unsigned long long minval, double D_val);
static void   H5Z__scaleoffset_next_byte(size_t *j, unsigned *buf_len);
static void   H5Z__scaleoffset_decompress_one_byte(unsigned char *data, size_t data_offset, unsigned k,
                                                   unsigned begin_i, const unsigned char *buffer, size_t *j,
                                                   unsigned *buf_len, parms_atomic p, unsigned dtype_len);
static void   H5Z__scaleoffset_compress_one_byte(const unsigned char *data, size_t data_offset, unsigned k,
                                                 unsigned begin_i, unsigned char *buffer, size_t *j,
                                                 unsigned *buf_len, parms_atomic p, unsigned dtype_len);
static void   H5Z__scaleoffset_decompress_one_atomic(unsigned char *data, size_t data_offset,
                                                     unsigned char *buffer, size_t *j, unsigned *buf_len,
                                                     parms_atomic p);
static void   H5Z__scaleoffset_compress_one_atomic(unsigned char *data, size_t data_offset,
                                                   unsigned char *buffer, size_t *j, unsigned *buf_len,
                                                   parms_atomic p);
static void   H5Z__scaleoffset_decompress(unsigned char *data, unsigned d_nelmts, unsigned char *buffer,
                                          parms_atomic p);
static void   H5Z__scaleoffset_compress(unsigned char *data, unsigned d_nelmts, unsigned char *buffer,
                                        size_t buffer_size, parms_atomic p);

/* This message derives from H5Z */
H5Z_class2_t H5Z_SCALEOFFSET[1] = {{
    H5Z_CLASS_T_VERS,           /* H5Z_class_t version */
    H5Z_FILTER_SCALEOFFSET,     /* Filter id number        */
    1,                          /* Assume encoder present: check before registering */
    1,                          /* decoder_present flag (set to true) */
    "scaleoffset",              /* Filter name for debugging    */
    H5Z__can_apply_scaleoffset, /* The "can apply" callback     */
    H5Z__set_local_scaleoffset, /* The "set local" callback     */
    H5Z__filter_scaleoffset,    /* The actual filter function    */
}};

/* Local macros */
#define H5Z_SCALEOFFSET_TOTAL_NPARMS     20 /* Total number of parameters for filter */
#define H5Z_SCALEOFFSET_PARM_SCALETYPE   0  /* "User" parameter for scale type */
#define H5Z_SCALEOFFSET_PARM_SCALEFACTOR 1  /* "User" parameter for scale factor */
#define H5Z_SCALEOFFSET_PARM_NELMTS      2  /* "Local" parameter for number of elements in the chunk */
#define H5Z_SCALEOFFSET_PARM_CLASS       3  /* "Local" parameter for datatype class */
#define H5Z_SCALEOFFSET_PARM_SIZE        4  /* "Local" parameter for datatype size */
#define H5Z_SCALEOFFSET_PARM_SIGN        5  /* "Local" parameter for integer datatype sign */
#define H5Z_SCALEOFFSET_PARM_ORDER       6  /* "Local" parameter for datatype byte order */
#define H5Z_SCALEOFFSET_PARM_FILAVAIL    7  /* "Local" parameter for dataset fill value existence */
#define H5Z_SCALEOFFSET_PARM_FILVAL      8 /* "Local" parameter for start location to store dataset fill value */

#define H5Z_SCALEOFFSET_CLS_INTEGER 0 /* Integer (datatype class) */
#define H5Z_SCALEOFFSET_CLS_FLOAT   1 /* Floatig-point (datatype class) */

#define H5Z_SCALEOFFSET_SGN_NONE 0 /* Unsigned integer type */
#define H5Z_SCALEOFFSET_SGN_2    1 /* Two's complement signed integer type */

#define H5Z_SCALEOFFSET_ORDER_LE 0 /* Little endian (datatype byte order) */
#define H5Z_SCALEOFFSET_ORDER_BE 1 /* Big endian (datatype byte order) */

#define H5Z_SCALEOFFSET_FILL_UNDEFINED 0 /* Fill value is not defined */
#define H5Z_SCALEOFFSET_FILL_DEFINED   1 /* Fill value is defined */

/* Store fill value in cd_values[] */
#define H5Z_scaleoffset_save_filval(type, cd_values, fill_val)                                               \
    {                                                                                                        \
        unsigned _i = H5Z_SCALEOFFSET_PARM_FILVAL; /* index into cd_values  */                               \
        uint32_t _cd_value;                        /* Current cd_value */                                    \
        char    *_fv_p;                            /* Pointer to current byte in fill_val */                 \
        size_t   _copy_size = 4;                   /* # of bytes to copy this iteration */                   \
        size_t   _size_rem  = sizeof(type);        /* # of bytes left to copy to cd_values */                \
                                                                                                             \
        /* Store the fill value as the last entry in cd_values[]                                             \
         * Store byte by byte from least significant byte to most significant byte                           \
         * Plenty of space left for the fill value (from index 8 to 19)                                      \
         * H5O_pline_encode will byte-swap each individual cd value, but we still                            \
         * need to swap the cd values as a whole if we are on a BE machine.  Note                            \
         * that we need to make sure to put the data only in the lowest 4 bytes of                           \
         * each, if sizeof(unsigned) > 4.                                                                    \
         */                                                                                                  \
        if (H5T_native_order_g == H5T_ORDER_LE) {                                                            \
            _fv_p = (char *)&(fill_val);                                                                     \
            /* Copy 4 bytes at a time to each cd value */                                                    \
            do {                                                                                             \
                if (_size_rem < 4) {                                                                         \
                    /* Amount left to copy is smaller than a cd_value, adjust copy                           \
                     * size and initialize cd_value as it will not be fully                                  \
                     * overwritten */                                                                        \
                    _copy_size = _size_rem;                                                                  \
                    _cd_value  = (uint32_t)0;                                                                \
                } /* end if */                                                                               \
                                                                                                             \
                /* Copy the value */                                                                         \
                H5MM_memcpy(&_cd_value, _fv_p, _copy_size);                                                  \
                (cd_values)[_i] = (unsigned)_cd_value;                                                       \
                                                                                                             \
                /* Next field */                                                                             \
                _i++;                                                                                        \
                _fv_p += _copy_size;                                                                         \
                _size_rem -= _copy_size;                                                                     \
            } while (_size_rem);                                                                             \
        } /* end if */                                                                                       \
        else {                                                                                               \
            assert(H5T_native_order_g == H5T_ORDER_BE);                                                      \
                                                                                                             \
            /* Copy 4 bytes at a time to each cd value, but start at the end                                 \
             * (highest address) of fill_val */                                                              \
            _fv_p = ((char *)&(fill_val)) + sizeof(type) - MIN(4, _size_rem);                                \
            while (_size_rem >= 4) {                                                                         \
                /* Copy the value */                                                                         \
                H5MM_memcpy(&_cd_value, _fv_p, _copy_size);                                                  \
                (cd_values)[_i] = (unsigned)_cd_value;                                                       \
                                                                                                             \
                /* Next field */                                                                             \
                _i++;                                                                                        \
                _size_rem -= 4;                                                                              \
                if (_size_rem >= 4)                                                                          \
                    _fv_p -= 4;                                                                              \
                else                                                                                         \
                    _fv_p -= _size_rem;                                                                      \
            } /* end while */                                                                                \
                                                                                                             \
            assert(_fv_p == (char *)&(fill_val));                                                            \
            if (_size_rem) {                                                                                 \
                /* Amount left to copy is smaller than a cd_value, initialize                                \
                 * _cd_value as it will not be fully overwritten and copy to the end                         \
                 * of _cd value as it is BE. */                                                              \
                _cd_value = (uint32_t)0;                                                                     \
                H5MM_memcpy((char *)&_cd_value + 4 - _size_rem, _fv_p, _size_rem);                           \
                (cd_values)[_i] = (unsigned)_cd_value;                                                       \
            } /* end if */                                                                                   \
        }     /* end else */                                                                                 \
    }

/* Set the fill value parameter in cd_values[] for unsigned integer type */
#define H5Z_scaleoffset_set_filval_1(type, dcpl_plist, dt, cd_values, need_convert)                          \
    do {                                                                                                     \
        type fill_val;                                                                                       \
                                                                                                             \
        /* Get dataset fill value */                                                                         \
        if (H5P_get_fill_value(dcpl_plist, dt, &fill_val) < 0)                                               \
            HGOTO_ERROR(H5E_PLINE, H5E_CANTGET, FAIL, "unable to get fill value");                           \
                                                                                                             \
        if (need_convert)                                                                                    \
            H5Z__scaleoffset_convert(&fill_val, 1, sizeof(type));                                            \
                                                                                                             \
        H5Z_scaleoffset_save_filval(type, cd_values, fill_val)                                               \
    } while (0)

/* Set the fill value parameter in cd_values[] for signed integer type */
#define H5Z_scaleoffset_set_filval_2(type, dcpl_plist, dt, cd_values, need_convert)                          \
    do {                                                                                                     \
        type fill_val;                                                                                       \
                                                                                                             \
        /* Get dataset fill value */                                                                         \
        if (H5P_get_fill_value(dcpl_plist, dt, &fill_val) < 0)                                               \
            HGOTO_ERROR(H5E_PLINE, H5E_CANTGET, FAIL, "unable to get fill value");                           \
                                                                                                             \
        if (need_convert)                                                                                    \
            H5Z__scaleoffset_convert(&fill_val, 1, sizeof(type));                                            \
                                                                                                             \
        H5Z_scaleoffset_save_filval(unsigned type, cd_values, fill_val)                                      \
    } while (0)

/* Set the fill value parameter in cd_values[] for character integer type */
#define H5Z_scaleoffset_set_filval_3(type, dcpl_plist, dt, cd_values, need_convert)                          \
    do {                                                                                                     \
        type fill_val;                                                                                       \
                                                                                                             \
        /* Get dataset fill value */                                                                         \
        if (H5P_get_fill_value(dcpl_plist, dt, &fill_val) < 0)                                               \
            HGOTO_ERROR(H5E_PLINE, H5E_CANTGET, FAIL, "unable to get fill value");                           \
                                                                                                             \
        /* Store the fill value as the last entry in cd_values[] */                                          \
        (cd_values)[H5Z_SCALEOFFSET_PARM_FILVAL] = (unsigned)((unsigned char)fill_val);                      \
    } while (0)

/* Set the fill value parameter in cd_values[] for floating-point type */
#define H5Z_scaleoffset_set_filval_4(type, dcpl_plist, dt, cd_values, need_convert)                          \
    do {                                                                                                     \
        type fill_val;                                                                                       \
                                                                                                             \
        /* Get dataset fill value */                                                                         \
        if (H5P_get_fill_value(dcpl_plist, dt, &fill_val) < 0)                                               \
            HGOTO_ERROR(H5E_PLINE, H5E_CANTGET, FAIL, "unable to get fill value");                           \
                                                                                                             \
        if (need_convert)                                                                                    \
            H5Z__scaleoffset_convert(&fill_val, 1, sizeof(type));                                            \
                                                                                                             \
        H5Z_scaleoffset_save_filval(type, cd_values, fill_val)                                               \
    } while (0)

/* Get the fill value for integer type */
#define H5Z_scaleoffset_get_filval_1(type, cd_values, fill_val)                                              \
    do {                                                                                                     \
        unsigned _i = H5Z_SCALEOFFSET_PARM_FILVAL; /* index into cd_values  */                               \
        uint32_t _cd_value;                        /* Current cd_value */                                    \
        char    *_fv_p;                            /* Pointer to current byte in fill_val */                 \
        size_t   _copy_size = 4;                   /* # of bytes to copy this iteration */                   \
        size_t   _size_rem  = sizeof(type);        /* # of bytes left to copy to filval */                   \
                                                                                                             \
        /* Retrieve the fill value from the last entry in cd_values[]                                        \
         * Store byte by byte from least significant byte to most significant byte                           \
         * Plenty of space left for the fill value (from index 8 to 19)                                      \
         * H5O_pline_encode will byte-swap each individual cd value, but we still                            \
         * need to swap the cd values as a whole if we are on a BE machine.  Note                            \
         * that we need to make sure to put the data only in the lowest 4 bytes of                           \
         * each, if sizeof(unsigned) > 4.                                                                    \
         */                                                                                                  \
        if (H5T_native_order_g == H5T_ORDER_LE) {                                                            \
            _fv_p = (char *)&(fill_val);                                                                     \
            /* Copy 4 bytes at a time to each cd value */                                                    \
            do {                                                                                             \
                if (_size_rem < 4)                                                                           \
                    /* Amount left to copy is smaller than a cd_value, adjust copy                           \
                     * size and initialize cd_value as it will not be fully                                  \
                     * overwritten */                                                                        \
                    _copy_size = _size_rem;                                                                  \
                                                                                                             \
                /* Copy the value */                                                                         \
                _cd_value = (uint32_t)(cd_values)[_i];                                                       \
                H5MM_memcpy(_fv_p, &_cd_value, _copy_size);                                                  \
                                                                                                             \
                /* Next field */                                                                             \
                _i++;                                                                                        \
                _fv_p += _copy_size;                                                                         \
                _size_rem -= _copy_size;                                                                     \
            } while (_size_rem);                                                                             \
        } /* end if */                                                                                       \
        else {                                                                                               \
            assert(H5T_native_order_g == H5T_ORDER_BE);                                                      \
                                                                                                             \
            /* Copy 4 bytes at a time to each cd value, but start at the end                                 \
             * (highest address) of fill_val */                                                              \
            _fv_p = ((char *)&(fill_val)) + sizeof(type) - MIN(4, _size_rem);                                \
            while (_size_rem >= 4) {                                                                         \
                /* Copy the value */                                                                         \
                _cd_value = (uint32_t)(cd_values)[_i];                                                       \
                H5MM_memcpy(_fv_p, &_cd_value, _copy_size);                                                  \
                                                                                                             \
                /* Next field */                                                                             \
                _i++;                                                                                        \
                _size_rem -= 4;                                                                              \
                if (_size_rem >= 4)                                                                          \
                    _fv_p -= 4;                                                                              \
                else                                                                                         \
                    _fv_p -= _size_rem;                                                                      \
            } /* end while */                                                                                \
                                                                                                             \
            assert(_fv_p == (char *)&(fill_val));                                                            \
            if (_size_rem) {                                                                                 \
                /* Amount left to copy is smaller than a cd_value, initialize                                \
                 * _cd_value as it will not be fully overwritten and copy to the end                         \
                 * of _cd value as it is BE. */                                                              \
                _cd_value = (uint32_t)(cd_values)[_i];                                                       \
                H5MM_memcpy(_fv_p, (char *)&_cd_value + 4 - _size_rem, _size_rem);                           \
            } /* end if */                                                                                   \
        }     /* end else */                                                                                 \
    } while (0)

/* Get the fill value for floating-point type */
#define H5Z_scaleoffset_get_filval_2(type, cd_values, filval)                                                \
    do {                                                                                                     \
        if (sizeof(type) <= sizeof(long long))                                                               \
            H5Z_scaleoffset_get_filval_1(type, cd_values, filval);                                           \
        else                                                                                                 \
            HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "cannot find matched integer datatype");               \
    } while (0)

/* Find maximum and minimum values of a buffer with fill value defined for integer type */
#define H5Z_scaleoffset_max_min_1(i, d_nelmts, buf, filval, max, min)                                        \
    {                                                                                                        \
        i = 0;                                                                                               \
        while (i < d_nelmts && buf[i] == filval)                                                             \
            i++;                                                                                             \
        if (i < d_nelmts)                                                                                    \
            min = max = buf[i];                                                                              \
        for (; i < d_nelmts; i++) {                                                                          \
            if (buf[i] == filval)                                                                            \
                continue; /* ignore fill value */                                                            \
            if (buf[i] > max)                                                                                \
                max = buf[i];                                                                                \
            if (buf[i] < min)                                                                                \
                min = buf[i];                                                                                \
        }                                                                                                    \
    }

/* Find maximum and minimum values of a buffer with fill value undefined */
#define H5Z_scaleoffset_max_min_2(i, d_nelmts, buf, max, min)                                                \
    {                                                                                                        \
        min = max = buf[0];                                                                                  \
        for (i = 0; i < d_nelmts; i++) {                                                                     \
            if (buf[i] > max)                                                                                \
                max = buf[i];                                                                                \
            if (buf[i] < min)                                                                                \
                min = buf[i];                                                                                \
        }                                                                                                    \
    }

/* Find maximum and minimum values of a buffer with fill value defined for floating-point type */
#define H5Z_scaleoffset_max_min_3(i, d_nelmts, buf, filval, max, min, D_val)                                 \
    {                                                                                                        \
        i = 0;                                                                                               \
        while (i < d_nelmts && fabs((double)(buf[i] - filval)) < pow(10.0, -D_val))                          \
            i++;                                                                                             \
        if (i < d_nelmts)                                                                                    \
            min = max = buf[i];                                                                              \
        for (; i < d_nelmts; i++) {                                                                          \
            if (fabs((double)(buf[i] - filval)) < pow(10.0, -D_val))                                         \
                continue; /* ignore fill value */                                                            \
            if (buf[i] > max)                                                                                \
                max = buf[i];                                                                                \
            if (buf[i] < min)                                                                                \
                min = buf[i];                                                                                \
        }                                                                                                    \
    }

/* Find minimum value of a buffer with fill value defined for integer type */
#define H5Z_scaleoffset_min_1(i, d_nelmts, buf, filval, min)                                                 \
    {                                                                                                        \
        i = 0;                                                                                               \
        while (i < d_nelmts && buf[i] == filval)                                                             \
            i++;                                                                                             \
        if (i < d_nelmts)                                                                                    \
            min = buf[i];                                                                                    \
        for (; i < d_nelmts; i++) {                                                                          \
            if (buf[i] == filval)                                                                            \
                continue; /* ignore fill value */                                                            \
            if (buf[i] < min)                                                                                \
                min = buf[i];                                                                                \
        }                                                                                                    \
    }

/* Find minimum value of a buffer with fill value undefined */
#define H5Z_scaleoffset_min_2(i, d_nelmts, buf, min)                                                         \
    {                                                                                                        \
        min = buf[0];                                                                                        \
        for (i = 0; i < d_nelmts; i++)                                                                       \
            if (buf[i] < min)                                                                                \
                min = buf[i];                                                                                \
    }

/* Check and handle special situation for unsigned integer type */
#define H5Z_scaleoffset_check_1(type, max, min, minbits)                                                     \
    {                                                                                                        \
        if (max - min > (type)(~(type)0 - 2)) {                                                              \
            *minbits = sizeof(type) * 8;                                                                     \
            return;                                                                                          \
        }                                                                                                    \
    }

/* Check and handle special situation for signed integer type */
#define H5Z_scaleoffset_check_2(type, max, min, minbits)                                                     \
    {                                                                                                        \
        if ((unsigned type)(max - min) > (unsigned type)(~(unsigned type)0 - 2)) {                           \
            *minbits = sizeof(type) * 8;                                                                     \
            return;                                                                                          \
        }                                                                                                    \
    }

/* Check and handle special situation for floating-point type */
#define H5Z_scaleoffset_check_3(i, type, pow_fun, round_fun, max, min, minbits, D_val)                       \
    {                                                                                                        \
        if (sizeof(type) == sizeof(int)) {                                                                   \
            if (round_fun(max * pow_fun((type)10, (type)D_val) - min * pow_fun((type)10, (type)D_val)) >     \
                pow_fun((type)2, (type)(sizeof(int) * 8 - 1))) {                                             \
                *minbits = sizeof(int) * 8;                                                                  \
                goto done;                                                                                   \
            }                                                                                                \
        }                                                                                                    \
        else if (sizeof(type) == sizeof(long)) {                                                             \
            if (round_fun(max * pow_fun((type)10, (type)D_val) - min * pow_fun((type)10, (type)D_val)) >     \
                pow_fun((type)2, (type)(sizeof(long) * 8 - 1))) {                                            \
                *minbits = sizeof(long) * 8;                                                                 \
                goto done;                                                                                   \
            }                                                                                                \
        }                                                                                                    \
        else if (sizeof(type) == sizeof(long long)) {                                                        \
            if (round_fun(max * pow_fun((type)10, (type)D_val) - min * pow_fun((type)10, (type)D_val)) >     \
                pow_fun((type)2, (type)(sizeof(long long) * 8 - 1))) {                                       \
                *minbits = sizeof(long long) * 8;                                                            \
                goto done;                                                                                   \
            }                                                                                                \
        }                                                                                                    \
        else                                                                                                 \
            HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "cannot find matched integer datatype");               \
    }

/* Precompress for unsigned integer type */
#define H5Z_scaleoffset_precompress_1(type, data, d_nelmts, filavail, cd_values, minbits, minval)            \
    do {                                                                                                     \
        type    *buf = (type *)data, min = 0, max = 0, span, filval = 0;                                     \
        unsigned i;                                                                                          \
                                                                                                             \
        if (filavail == H5Z_SCALEOFFSET_FILL_DEFINED) { /* fill value defined */                             \
            H5Z_scaleoffset_get_filval_1(type, cd_values, filval);                                           \
            if (*minbits ==                                                                                  \
                H5Z_SO_INT_MINBITS_DEFAULT) { /* minbits not set yet, calculate max, min, and minbits */     \
                H5Z_scaleoffset_max_min_1(i, d_nelmts, buf, filval, max, min)                                \
                    H5Z_scaleoffset_check_1(type, max, min, minbits) span = (type)(max - min + 1);           \
                *minbits = H5Z__scaleoffset_log2((unsigned long long)(span + 1));                            \
            }                                                                                                \
            else /* minbits already set, only calculate min */                                               \
                H5Z_scaleoffset_min_1(i, d_nelmts, buf, filval, min);                                        \
            if (*minbits != sizeof(type) * 8) /* change values if minbits != full precision */               \
                for (i = 0; i < d_nelmts; i++)                                                               \
                    buf[i] = (type)((buf[i] == filval) ? (((type)1 << *minbits) - 1) : (buf[i] - min));      \
        }                                                                                                    \
        else { /* fill value undefined */                                                                    \
            if (*minbits ==                                                                                  \
                H5Z_SO_INT_MINBITS_DEFAULT) { /* minbits not set yet, calculate max, min, and minbits */     \
                H5Z_scaleoffset_max_min_2(i, d_nelmts, buf, max, min);                                       \
                H5Z_scaleoffset_check_1(type, max, min, minbits);                                            \
                span     = (type)(max - min + 1);                                                            \
                *minbits = H5Z__scaleoffset_log2((unsigned long long)span);                                  \
            }                                                                                                \
            else /* minbits already set, only calculate min */                                               \
                H5Z_scaleoffset_min_2(i, d_nelmts, buf, min);                                                \
            if (*minbits != sizeof(type) * 8) /* change values if minbits != full precision */               \
                for (i = 0; i < d_nelmts; i++)                                                               \
                    buf[i] = (type)(buf[i] - min);                                                           \
        }                                                                                                    \
        *minval = min;                                                                                       \
    } while (0)

/* Precompress for signed integer type */
#define H5Z_scaleoffset_precompress_2(type, data, d_nelmts, filavail, cd_values, minbits, minval)            \
    do {                                                                                                     \
        type         *buf = (type *)data, min = 0, max = 0, filval = 0;                                      \
        unsigned type span;                                                                                  \
        unsigned      i;                                                                                     \
                                                                                                             \
        if (filavail == H5Z_SCALEOFFSET_FILL_DEFINED) { /* fill value defined */                             \
            H5Z_scaleoffset_get_filval_1(type, cd_values, filval);                                           \
            if (*minbits ==                                                                                  \
                H5Z_SO_INT_MINBITS_DEFAULT) { /* minbits not set yet, calculate max, min, and minbits */     \
                H5Z_scaleoffset_max_min_1(i, d_nelmts, buf, filval, max, min)                                \
                    H5Z_scaleoffset_check_2(type, max, min, minbits) span = (unsigned type)(max - min + 1);  \
                *minbits = H5Z__scaleoffset_log2((unsigned long long)(span + 1));                            \
            }                                                                                                \
            else /* minbits already set, only calculate min */                                               \
                H5Z_scaleoffset_min_1(i, d_nelmts, buf, filval, min);                                        \
            if (*minbits != sizeof(type) * 8) /* change values if minbits != full precision */               \
                for (i = 0; i < d_nelmts; i++)                                                               \
                    buf[i] = (type)((buf[i] == filval) ? (type)(((unsigned type)1 << *minbits) - 1)          \
                                                       : (buf[i] - min));                                    \
        }                                                                                                    \
        else { /* fill value undefined */                                                                    \
            if (*minbits ==                                                                                  \
                H5Z_SO_INT_MINBITS_DEFAULT) { /* minbits not set yet, calculate max, min, and minbits */     \
                H5Z_scaleoffset_max_min_2(i, d_nelmts, buf, max, min)                                        \
                    H5Z_scaleoffset_check_2(type, max, min, minbits) span = (unsigned type)(max - min + 1);  \
                *minbits = H5Z__scaleoffset_log2((unsigned long long)span);                                  \
            }                                                                                                \
            else /* minbits already set, only calculate min */                                               \
                H5Z_scaleoffset_min_2(                                                                       \
                    i, d_nelmts, buf,                                                                        \
                    min) if (*minbits != sizeof(type) * 8) /* change values if minbits != full precision */  \
                    for (i = 0; i < d_nelmts; i++) buf[i] = (type)(buf[i] - min);                            \
        }                                                                                                    \
        *minval = (unsigned long long)min;                                                                   \
    } while (0)

/* Modify values of data in precompression if fill value defined for floating-point type */
#define H5Z_scaleoffset_modify_1(i, type, pow_fun, abs_fun, lround_fun, llround_fun, buf, d_nelmts, filval,  \
                                 minbits, min, D_val)                                                        \
    {                                                                                                        \
        if (sizeof(type) == sizeof(int))                                                                     \
            for (i = 0; i < d_nelmts; i++) {                                                                 \
                if (abs_fun(buf[i] - filval) < pow_fun((type)10, (type)-D_val))                              \
                    *(int *)((void *)&buf[i]) = (int)(((unsigned int)1 << *minbits) - 1);                    \
                else                                                                                         \
                    *(int *)((void *)&buf[i]) = (int)lround_fun(buf[i] * pow_fun((type)10, (type)D_val) -    \
                                                                min * pow_fun((type)10, (type)D_val));       \
            }                                                                                                \
        else if (sizeof(type) == sizeof(long))                                                               \
            for (i = 0; i < d_nelmts; i++) {                                                                 \
                if (abs_fun(buf[i] - filval) < pow_fun((type)10, (type)-D_val))                              \
                    *(long *)((void *)&buf[i]) = (long)(((unsigned long)1 << *minbits) - 1);                 \
                else                                                                                         \
                    *(long *)((void *)&buf[i]) = lround_fun(buf[i] * pow_fun((type)10, (type)D_val) -        \
                                                            min * pow_fun((type)10, (type)D_val));           \
            }                                                                                                \
        else if (sizeof(type) == sizeof(long long))                                                          \
            for (i = 0; i < d_nelmts; i++) {                                                                 \
                if (abs_fun(buf[i] - filval) < pow_fun((type)10, (type)-D_val))                              \
                    *(long long *)((void *)&buf[i]) = (long long)(((unsigned long long)1 << *minbits) - 1);  \
                else                                                                                         \
                    *(long long *)((void *)&buf[i]) = llround_fun(buf[i] * pow_fun((type)10, (type)D_val) -  \
                                                                  min * pow_fun((type)10, (type)D_val));     \
            }                                                                                                \
        else                                                                                                 \
            HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "cannot find matched integer datatype");               \
    }

/* Modify values of data in precompression if fill value undefined for floating-point type */
#define H5Z_scaleoffset_modify_2(i, type, pow_fun, lround_fun, llround_fun, buf, d_nelmts, min, D_val)       \
    {                                                                                                        \
        if (sizeof(type) == sizeof(int))                                                                     \
            for (i = 0; i < d_nelmts; i++)                                                                   \
                *(int *)((void *)&buf[i]) = (int)lround_fun(buf[i] * pow_fun((type)10, (type)D_val) -        \
                                                            min * pow_fun((type)10, (type)D_val));           \
        else if (sizeof(type) == sizeof(long))                                                               \
            for (i = 0; i < d_nelmts; i++)                                                                   \
                *(long *)((void *)&buf[i]) = lround_fun(buf[i] * pow_fun((type)10, (type)D_val) -            \
                                                        min * pow_fun((type)10, (type)D_val));               \
        else if (sizeof(type) == sizeof(long long))                                                          \
            for (i = 0; i < d_nelmts; i++)                                                                   \
                *(long long *)((void *)&buf[i]) = llround_fun(buf[i] * pow_fun((type)10, (type)D_val) -      \
                                                              min * pow_fun((type)10, (type)D_val));         \
        else                                                                                                 \
            HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "cannot find matched integer datatype");               \
    }

/* Save the minimum value for floating-point type */
#define H5Z_scaleoffset_save_min(i, type, minval, min)                                                       \
    {                                                                                                        \
        if (sizeof(type) <= sizeof(long long))                                                               \
            /* Save min value to corresponding position                                                      \
             * byte-order will be swapped as appropriate, but be sure to                                     \
             * account for offset in BE if sizes differ                                                      \
             */                                                                                              \
            if (H5T_native_order_g == H5T_ORDER_LE)                                                          \
                H5MM_memcpy(minval, &min, sizeof(type));                                                     \
            else {                                                                                           \
                assert(H5T_native_order_g == H5T_ORDER_BE);                                                  \
                H5MM_memcpy(((char *)minval) + (sizeof(long long) - sizeof(type)), &min, sizeof(type));      \
            } /* end else */                                                                                 \
        else                                                                                                 \
            HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "cannot find matched integer datatype");               \
    }

/* Precompress for floating-point type using variable-minimum-bits method */
#define H5Z_scaleoffset_precompress_3(type, pow_fun, abs_fun, round_fun, lround_fun, llround_fun, data,      \
                                      d_nelmts, filavail, cd_values, minbits, minval, D_val)                 \
    do {                                                                                                     \
        type              *buf = (type *)data, min = 0, max = 0, filval = 0;                                 \
        unsigned long long span;                                                                             \
        unsigned           i;                                                                                \
                                                                                                             \
        *minval = 0;                                                                                         \
        if (filavail == H5Z_SCALEOFFSET_FILL_DEFINED) { /* fill value defined */                             \
            H5Z_scaleoffset_get_filval_2(type, cd_values, filval);                                           \
            H5Z_scaleoffset_max_min_3(i, d_nelmts, buf, filval, max, min, D_val);                            \
            H5Z_scaleoffset_check_3(i, type, pow_fun, round_fun, max, min, minbits, D_val);                  \
            span     = (unsigned long long)(llround_fun(max * pow_fun((type)10, (type)D_val) -               \
                                                        min * pow_fun((type)10, (type)D_val)) +              \
                                        1);                                                              \
            *minbits = H5Z__scaleoffset_log2(span + 1);                                                      \
            if (*minbits != sizeof(type) * 8) /* change values if minbits != full precision */               \
                H5Z_scaleoffset_modify_1(i, type, pow_fun, abs_fun, lround_fun, llround_fun, buf, d_nelmts,  \
                                         filval, minbits, min, D_val);                                       \
        }                                                                                                    \
        else { /* fill value undefined */                                                                    \
            H5Z_scaleoffset_max_min_2(i, d_nelmts, buf, max, min);                                           \
            H5Z_scaleoffset_check_3(i, type, pow_fun, round_fun, max, min, minbits, D_val);                  \
            span     = (unsigned long long)(llround_fun(max * pow_fun((type)10, (type)D_val) -               \
                                                        min * pow_fun((type)10, (type)D_val)) +              \
                                        1);                                                              \
            *minbits = H5Z__scaleoffset_log2(span);                                                          \
            if (*minbits != sizeof(type) * 8) /* change values if minbits != full precision */               \
                H5Z_scaleoffset_modify_2(i, type, pow_fun, lround_fun, llround_fun, buf, d_nelmts, min,      \
                                         D_val);                                                             \
        }                                                                                                    \
        H5Z_scaleoffset_save_min(i, type, minval, min);                                                      \
    } while (0)

/* Postdecompress for unsigned integer type */
#define H5Z_scaleoffset_postdecompress_1(type, data, d_nelmts, filavail, cd_values, minbits, minval)         \
    do {                                                                                                     \
        type    *buf = (type *)data, filval = 0;                                                             \
        unsigned i;                                                                                          \
                                                                                                             \
        if (filavail == H5Z_SCALEOFFSET_FILL_DEFINED) { /* fill value defined */                             \
            H5Z_scaleoffset_get_filval_1(type, cd_values, filval);                                           \
            for (i = 0; i < d_nelmts; i++)                                                                   \
                buf[i] = (type)((buf[i] == (((type)1 << minbits) - 1)) ? filval : (buf[i] + minval));        \
        }                                                                                                    \
        else /* fill value undefined */                                                                      \
            for (i = 0; i < d_nelmts; i++)                                                                   \
                buf[i] = (type)(buf[i] + (type)(minval));                                                    \
    } while (0)

/* Postdecompress for signed integer type */
#define H5Z_scaleoffset_postdecompress_2(type, data, d_nelmts, filavail, cd_values, minbits, minval)         \
    do {                                                                                                     \
        type    *buf = (type *)data, filval = 0;                                                             \
        unsigned i;                                                                                          \
                                                                                                             \
        if (filavail == H5Z_SCALEOFFSET_FILL_DEFINED) { /* fill value defined */                             \
            H5Z_scaleoffset_get_filval_1(type, cd_values, filval);                                           \
            for (i = 0; i < d_nelmts; i++)                                                                   \
                buf[i] = (type)(((unsigned type)buf[i] == (((unsigned type)1 << minbits) - 1))               \
                                    ? filval                                                                 \
                                    : (buf[i] + minval));                                                    \
        }                                                                                                    \
        else /* fill value undefined */                                                                      \
            for (i = 0; i < d_nelmts; i++)                                                                   \
                buf[i] = (type)(buf[i] + (type)(minval));                                                    \
    } while (0)

/* Retrieve minimum value of floating-point type */
#define H5Z_scaleoffset_get_min(type, minval, min)                                                           \
    do {                                                                                                     \
        if (sizeof(type) <= sizeof(long long))                                                               \
            /* retrieve min value from corresponding position                                                \
             * byte-order has already been swapped as appropriate, but be sure to                            \
             * account for offset in BE if sizes differ                                                      \
             */                                                                                              \
            if (H5T_native_order_g == H5T_ORDER_LE)                                                          \
                H5MM_memcpy(&min, &minval, sizeof(type));                                                    \
            else {                                                                                           \
                assert(H5T_native_order_g == H5T_ORDER_BE);                                                  \
                H5MM_memcpy(&min, ((char *)&minval) + (sizeof(long long) - sizeof(type)), sizeof(type));     \
            } /* end else */                                                                                 \
        else                                                                                                 \
            HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "cannot find matched integer datatype");               \
    } while (0)

/* Modify values of data in postdecompression if fill value defined for floating-point type */
#define H5Z_scaleoffset_modify_3(i, type, pow_fun, buf, d_nelmts, filval, minbits, min, D_val)               \
    do {                                                                                                     \
        if (sizeof(type) == sizeof(int))                                                                     \
            for (i = 0; i < d_nelmts; i++)                                                                   \
                buf[i] =                                                                                     \
                    (type)((*(int *)((void *)&buf[i]) == (int)(((unsigned int)1 << minbits) - 1))            \
                               ? filval                                                                      \
                               : (type)(*(int *)((void *)&buf[i])) / pow_fun((type)10, (type)D_val) + min);  \
        else if (sizeof(type) == sizeof(long))                                                               \
            for (i = 0; i < d_nelmts; i++)                                                                   \
                buf[i] =                                                                                     \
                    (type)((*(long *)((void *)&buf[i]) == (long)(((unsigned long)1 << minbits) - 1))         \
                               ? filval                                                                      \
                               : (type)(*(long *)((void *)&buf[i])) / pow_fun((type)10, (type)D_val) + min); \
        else if (sizeof(type) == sizeof(long long))                                                          \
            for (i = 0; i < d_nelmts; i++)                                                                   \
                buf[i] =                                                                                     \
                    (type)((*(long long *)((void *)&buf[i]) ==                                               \
                            (long long)(((unsigned long long)1 << minbits) - 1))                             \
                               ? filval                                                                      \
                               : (type)(*(long long *)((void *)&buf[i])) / pow_fun((type)10, (type)D_val) +  \
                                     min);                                                                   \
        else                                                                                                 \
            HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "cannot find matched integer datatype");               \
    } while (0)

/* Modify values of data in postdecompression if fill value undefined for floating-point type */
#define H5Z_scaleoffset_modify_4(i, type, pow_fun, buf, d_nelmts, min, D_val)                                \
    do {                                                                                                     \
        if (sizeof(type) == sizeof(int))                                                                     \
            for (i = 0; i < d_nelmts; i++)                                                                   \
                buf[i] = ((type)(*(int *)((void *)&buf[i])) / pow_fun((type)10, (type)D_val) + min);         \
        else if (sizeof(type) == sizeof(long))                                                               \
            for (i = 0; i < d_nelmts; i++)                                                                   \
                buf[i] = ((type)(*(long *)((void *)&buf[i])) / pow_fun((type)10, (type)D_val) + min);        \
        else if (sizeof(type) == sizeof(long long))                                                          \
            for (i = 0; i < d_nelmts; i++)                                                                   \
                buf[i] = ((type)(*(long long *)((void *)&buf[i])) / pow_fun((type)10, (type)D_val) + min);   \
        else                                                                                                 \
            HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "cannot find matched integer datatype");               \
    } while (0)

/* Postdecompress for floating-point type using variable-minimum-bits method */
#define H5Z_scaleoffset_postdecompress_3(type, pow_fun, data, d_nelmts, filavail, cd_values, minbits,        \
                                         minval, D_val)                                                      \
    do {                                                                                                     \
        type    *buf = (type *)data, filval = 0, min = 0;                                                    \
        unsigned i;                                                                                          \
                                                                                                             \
        H5Z_scaleoffset_get_min(type, minval, min);                                                          \
                                                                                                             \
        if (filavail == H5Z_SCALEOFFSET_FILL_DEFINED) { /* fill value defined */                             \
            H5Z_scaleoffset_get_filval_2(type, cd_values, filval);                                           \
            H5Z_scaleoffset_modify_3(i, type, pow_fun, buf, d_nelmts, filval, minbits, min, D_val);          \
        }                                                                                                    \
        else /* fill value undefined */                                                                      \
            H5Z_scaleoffset_modify_4(i, type, pow_fun, buf, d_nelmts, min, D_val);                           \
    } while (0)

/*-------------------------------------------------------------------------
 * Function:    H5Z__can_apply_scaleoffset
 *
 * Purpose:    Check the parameters for scaleoffset compression for
 *              validity and whether they fit a particular dataset.
 *
 * Return:    Success: Non-negative
 *        Failure: Negative
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5Z__can_apply_scaleoffset(hid_t H5_ATTR_UNUSED dcpl_id, hid_t type_id, hid_t H5_ATTR_UNUSED space_id)
{
    const H5T_t *type;             /* Datatype */
    H5T_class_t  dtype_class;      /* Datatype's class */
    H5T_order_t  dtype_order;      /* Datatype's endianness order */
    htri_t       ret_value = true; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get datatype */
    if (NULL == (type = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");

    /* Get datatype's class, for checking the "datatype class" */
    if ((dtype_class = H5T_get_class(type, true)) == H5T_NO_CLASS)
        HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad datatype class");

    /* Get datatype's size, for checking the "datatype size" */
    if (H5T_get_size(type) == 0)
        HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad datatype size");

    if (dtype_class == H5T_INTEGER || dtype_class == H5T_FLOAT) {
        /* Get datatype's endianness order */
        if ((dtype_order = H5T_get_order(type)) == H5T_ORDER_ERROR)
            HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "can't retrieve datatype endianness order");

        /* Range check datatype's endianness order */
        if (dtype_order != H5T_ORDER_LE && dtype_order != H5T_ORDER_BE)
            HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, false, "bad datatype endianness order");
    }
    else
        HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, false, "datatype class not supported by scaleoffset");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z__can_apply_scaleoffset() */

/*-------------------------------------------------------------------------
 * Function:    H5Z__scaleoffset_get_type
 *
 * Purpose:    Get the specific integer type based on datatype size and sign
 *              or floating-point type based on size
 *
 * Return:    Success: id number of integer type
 *        Failure: 0
 *
 *-------------------------------------------------------------------------
 */
static enum H5Z_scaleoffset_t
H5Z__scaleoffset_get_type(unsigned dtype_class, unsigned dtype_size, unsigned dtype_sign)
{
    enum H5Z_scaleoffset_t type      = t_bad; /* integer type */
    enum H5Z_scaleoffset_t ret_value = t_bad; /* Return value */

    FUNC_ENTER_PACKAGE

    if (dtype_class == H5Z_SCALEOFFSET_CLS_INTEGER) {
        if (dtype_sign == H5Z_SCALEOFFSET_SGN_NONE) { /* unsigned integer */
            if (dtype_size == sizeof(unsigned char))
                type = t_uchar;
            else if (dtype_size == sizeof(unsigned short))
                type = t_ushort;
            else if (dtype_size == sizeof(unsigned int))
                type = t_uint;
            else if (dtype_size == sizeof(unsigned long))
                type = t_ulong;
#if H5_SIZEOF_LONG != H5_SIZEOF_LONG_LONG
            else if (dtype_size == sizeof(unsigned long long))
                type = t_ulong_long;
#endif /* H5_SIZEOF_LONG != H5_SIZEOF_LONG_LONG */
            else
                HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, t_bad, "cannot find matched memory datatype");
        }

        if (dtype_sign == H5Z_SCALEOFFSET_SGN_2) { /* signed integer */
            if (dtype_size == sizeof(signed char))
                type = t_schar;
            else if (dtype_size == sizeof(short))
                type = t_short;
            else if (dtype_size == sizeof(int))
                type = t_int;
            else if (dtype_size == sizeof(long))
                type = t_long;
#if H5_SIZEOF_LONG != H5_SIZEOF_LONG_LONG
            else if (dtype_size == sizeof(long long))
                type = t_long_long;
#endif /* H5_SIZEOF_LONG != H5_SIZEOF_LONG_LONG */
            else
                HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, t_bad, "cannot find matched memory datatype");
        }
    }

    if (dtype_class == H5Z_SCALEOFFSET_CLS_FLOAT) {
        if (dtype_size == sizeof(float))
            type = t_float;
        else if (dtype_size == sizeof(double))
            type = t_double;
        else
            HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, t_bad, "cannot find matched memory datatype");
    }

    /* Set return value */
    ret_value = type;

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5Z__scaleoffset_set_parms_fillval
 *
 * Purpose:    Get the fill value of the dataset and store in cd_values[]
 *
 * Return:    Success: Non-negative
 *        Failure: Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5Z__scaleoffset_set_parms_fillval(H5P_genplist_t *dcpl_plist, H5T_t *type, enum H5Z_scaleoffset_t scale_type,
                                   unsigned cd_values[], int need_convert)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    if (scale_type == t_uchar)
        H5Z_scaleoffset_set_filval_3(unsigned char, dcpl_plist, type, cd_values, need_convert);
    else if (scale_type == t_ushort)
        H5Z_scaleoffset_set_filval_1(unsigned short, dcpl_plist, type, cd_values, need_convert);
    else if (scale_type == t_uint)
        H5Z_scaleoffset_set_filval_1(unsigned int, dcpl_plist, type, cd_values, need_convert);
    else if (scale_type == t_ulong)
        H5Z_scaleoffset_set_filval_1(unsigned long, dcpl_plist, type, cd_values, need_convert);
    else if (scale_type == t_ulong_long)
        H5Z_scaleoffset_set_filval_1(unsigned long long, dcpl_plist, type, cd_values, need_convert);
    else if (scale_type == t_schar)
        H5Z_scaleoffset_set_filval_3(signed char, dcpl_plist, type, cd_values, need_convertd);
    else if (scale_type == t_short)
        H5Z_scaleoffset_set_filval_2(short, dcpl_plist, type, cd_values, need_convert);
    else if (scale_type == t_int)
        H5Z_scaleoffset_set_filval_2(int, dcpl_plist, type, cd_values, need_convert);
    else if (scale_type == t_long)
        H5Z_scaleoffset_set_filval_2(long, dcpl_plist, type, cd_values, need_convert);
    else if (scale_type == t_long_long)
        H5Z_scaleoffset_set_filval_2(long long, dcpl_plist, type, cd_values, need_convert);
    else if (scale_type == t_float)
        H5Z_scaleoffset_set_filval_4(float, dcpl_plist, type, cd_values, need_convert);
    else if (scale_type == t_double)
        H5Z_scaleoffset_set_filval_4(double, dcpl_plist, type, cd_values, need_convert);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z__scaleoffset_set_parms_fillval() */

/*-------------------------------------------------------------------------
 * Function:    H5Z__set_local_scaleoffset
 *
 * Purpose:    Set the "local" dataset parameters for scaleoffset
 *              compression.
 *
 * Return:    Success: Non-negative
 *        Failure: Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5Z__set_local_scaleoffset(hid_t dcpl_id, hid_t type_id, hid_t space_id)
{
    H5P_genplist_t        *dcpl_plist;                              /* Property list pointer */
    H5T_t                 *type;                                    /* Datatype */
    const H5S_t           *ds;                                      /* Dataspace */
    unsigned               flags;                                   /* Filter flags */
    size_t                 cd_nelmts = H5Z_SCALEOFFSET_USER_NPARMS; /* Number of filter parameters */
    unsigned               cd_values[H5Z_SCALEOFFSET_TOTAL_NPARMS]; /* Filter parameters */
    hssize_t               npoints;                                 /* Number of points in the dataspace */
    H5T_class_t            dtype_class;                             /* Datatype's class */
    H5T_order_t            dtype_order;                             /* Datatype's endianness order */
    size_t                 dtype_size;                              /* Datatype's size (in bytes) */
    H5T_sign_t             dtype_sign;                              /* Datatype's sign */
    enum H5Z_scaleoffset_t scale_type;                              /* Specific datatype */
    H5D_fill_value_t       status;              /* Status of fill value in property list */
    herr_t                 ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get the plist structure */
    if (NULL == (dcpl_plist = H5P_object_verify(dcpl_id, H5P_DATASET_CREATE)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get datatype */
    if (NULL == (type = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");

    /* Initialize the parameters to a known state */
    memset(cd_values, 0, sizeof(cd_values));

    /* Get the filter's current parameters */
    if (H5P_get_filter_by_id(dcpl_plist, H5Z_FILTER_SCALEOFFSET, &flags, &cd_nelmts, cd_values, (size_t)0,
                             NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTGET, FAIL, "can't get scaleoffset parameters");

    /* Get dataspace */
    if (NULL == (ds = (H5S_t *)H5I_object_verify(space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataspace");

    /* Get total number of elements in the chunk */
    if ((npoints = H5S_GET_EXTENT_NPOINTS(ds)) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTGET, FAIL, "unable to get number of points in the dataspace");

    /* Set "local" parameter for this dataset's number of elements */
    H5_CHECKED_ASSIGN(cd_values[H5Z_SCALEOFFSET_PARM_NELMTS], unsigned, npoints, hssize_t);

    /* Get datatype's class */
    if ((dtype_class = H5T_get_class(type, true)) == H5T_NO_CLASS)
        HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad datatype class");

    /* Set "local" parameter for datatype's class */
    switch (dtype_class) {
        case H5T_INTEGER:
            cd_values[H5Z_SCALEOFFSET_PARM_CLASS] = H5Z_SCALEOFFSET_CLS_INTEGER;
            break;

        case H5T_FLOAT:
            cd_values[H5Z_SCALEOFFSET_PARM_CLASS] = H5Z_SCALEOFFSET_CLS_FLOAT;
            break;

        case H5T_NO_CLASS:
        case H5T_TIME:
        case H5T_STRING:
        case H5T_BITFIELD:
        case H5T_OPAQUE:
        case H5T_COMPOUND:
        case H5T_REFERENCE:
        case H5T_ENUM:
        case H5T_VLEN:
        case H5T_ARRAY:
        case H5T_NCLASSES:
        default:
            HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "datatype class not supported by scaleoffset");
    } /* end switch */

    /* Get datatype's size */
    if ((dtype_size = H5T_get_size(type)) == 0)
        HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad datatype size");

    /* Set "local" parameter for datatype size */
    H5_CHECK_OVERFLOW(dtype_size, size_t, unsigned);
    cd_values[H5Z_SCALEOFFSET_PARM_SIZE] = (unsigned)dtype_size;

    if (dtype_class == H5T_INTEGER) {
        /* Get datatype's sign */
        if ((dtype_sign = H5T_get_sign(type)) == H5T_SGN_ERROR)
            HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad datatype sign");

        /* Set "local" parameter for integer datatype sign */
        switch (dtype_sign) {
            case H5T_SGN_NONE:
                cd_values[H5Z_SCALEOFFSET_PARM_SIGN] = H5Z_SCALEOFFSET_SGN_NONE;
                break;

            case H5T_SGN_2:
                cd_values[H5Z_SCALEOFFSET_PARM_SIGN] = H5Z_SCALEOFFSET_SGN_2;
                break;

            case H5T_SGN_ERROR:
            case H5T_NSGN:
            default:
                HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad integer sign");
        } /* end switch */
    }     /* end if */

    /* Get datatype's endianness order */
    if ((dtype_order = H5T_get_order(type)) == H5T_ORDER_ERROR)
        HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad datatype endianness order");

    /* Set "local" parameter for datatype endianness */
    switch (dtype_order) {
        case H5T_ORDER_LE: /* Little-endian byte order */
            cd_values[H5Z_SCALEOFFSET_PARM_ORDER] = H5Z_SCALEOFFSET_ORDER_LE;
            break;

        case H5T_ORDER_BE: /* Big-endian byte order */
            cd_values[H5Z_SCALEOFFSET_PARM_ORDER] = H5Z_SCALEOFFSET_ORDER_BE;
            break;

        case H5T_ORDER_ERROR:
        case H5T_ORDER_VAX:
        case H5T_ORDER_MIXED:
        case H5T_ORDER_NONE:
        default:
            HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad datatype endianness order");
    } /* end switch */

    /* Check whether fill value is defined for dataset */
    if (H5P_fill_value_defined(dcpl_plist, &status) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTGET, FAIL, "unable to determine if fill value is defined");

    /* Set local parameter for availability of fill value */
    if (status == H5D_FILL_VALUE_UNDEFINED)
        cd_values[H5Z_SCALEOFFSET_PARM_FILAVAIL] = H5Z_SCALEOFFSET_FILL_UNDEFINED;
    else {
        int need_convert = false; /* Flag indicating conversion of byte order */

        cd_values[H5Z_SCALEOFFSET_PARM_FILAVAIL] = H5Z_SCALEOFFSET_FILL_DEFINED;

        /* Check if memory byte order matches dataset datatype byte order */
        if (H5T_native_order_g != dtype_order)
            need_convert = true;

        /* Before getting fill value, get its type */
        if ((scale_type = H5Z__scaleoffset_get_type(cd_values[H5Z_SCALEOFFSET_PARM_CLASS],
                                                    cd_values[H5Z_SCALEOFFSET_PARM_SIZE],
                                                    cd_values[H5Z_SCALEOFFSET_PARM_SIGN])) == 0)
            HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "cannot use C integer datatype for cast");

        /* Get dataset fill value and store in cd_values[] */
        if (H5Z__scaleoffset_set_parms_fillval(dcpl_plist, type, scale_type, cd_values, need_convert) < 0)
            HGOTO_ERROR(H5E_PLINE, H5E_CANTSET, FAIL, "unable to set fill value");
    } /* end else */

    /* Modify the filter's parameters for this dataset */
    if (H5P_modify_filter(dcpl_plist, H5Z_FILTER_SCALEOFFSET, flags, (size_t)H5Z_SCALEOFFSET_TOTAL_NPARMS,
                          cd_values) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTSET, FAIL, "can't set local scaleoffset parameters");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z__set_local_scaleoffset() */

/*-------------------------------------------------------------------------
 * Function:    H5Z__filter_scaleoffset
 *
 * Purpose:    Implement an I/O filter for storing packed integer
 *              data using scale and offset method.
 *
 * Return:    Success: Size of buffer filtered
 *        Failure: 0
 *
 *-------------------------------------------------------------------------
 */
static size_t
H5Z__filter_scaleoffset(unsigned flags, size_t cd_nelmts, const unsigned cd_values[], size_t nbytes,
                        size_t *buf_size, void **buf)
{
    size_t                 ret_value = 0; /* return value */
    size_t                 size_out  = 0; /* size of output buffer */
    unsigned               d_nelmts  = 0; /* number of data elements in the chunk */
    unsigned               dtype_class;   /* datatype class */
    unsigned               dtype_sign;    /* integer datatype sign */
    unsigned               filavail;      /* flag indicating if fill value is defined or not */
    H5Z_SO_scale_type_t    scale_type   = H5Z_SO_FLOAT_DSCALE; /* scale type */
    int                    scale_factor = 0;                   /* scale factor */
    double                 D_val        = 0.0;                 /* decimal scale factor */
    uint32_t               minbits      = 0;                   /* minimum number of bits to store values */
    unsigned long long     minval       = 0;                   /* minimum value of input buffer */
    enum H5Z_scaleoffset_t type;                 /* memory type corresponding to dataset datatype */
    int                    need_convert = false; /* flag indicating conversion of byte order */
    unsigned char         *outbuf       = NULL;  /* pointer to new output buffer */
    unsigned               buf_offset   = 21;    /* buffer offset because of parameters stored in file */
    unsigned               i;                    /* index */
    parms_atomic           p;                    /* parameters needed for compress/decompress functions */

    FUNC_ENTER_PACKAGE

    /* check arguments */
    if (cd_nelmts != H5Z_SCALEOFFSET_TOTAL_NPARMS)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, 0, "invalid scaleoffset number of parameters");

    /* Check if memory byte order matches dataset datatype byte order */
    switch (H5T_native_order_g) {
        case H5T_ORDER_LE: /* memory is little-endian byte order */
            if (cd_values[H5Z_SCALEOFFSET_PARM_ORDER] == H5Z_SCALEOFFSET_ORDER_BE)
                need_convert = true;
            break;

        case H5T_ORDER_BE: /* memory is big-endian byte order */
            if (cd_values[H5Z_SCALEOFFSET_PARM_ORDER] == H5Z_SCALEOFFSET_ORDER_LE)
                need_convert = true;
            break;

        case H5T_ORDER_ERROR:
        case H5T_ORDER_VAX:
        case H5T_ORDER_MIXED:
        case H5T_ORDER_NONE:
        default:
            HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, 0, "bad H5T_NATIVE_INT endianness order");
    } /* end switch */

    /* copy filter parameters to local variables */
    d_nelmts     = cd_values[H5Z_SCALEOFFSET_PARM_NELMTS];
    dtype_class  = cd_values[H5Z_SCALEOFFSET_PARM_CLASS];
    dtype_sign   = cd_values[H5Z_SCALEOFFSET_PARM_SIGN];
    filavail     = cd_values[H5Z_SCALEOFFSET_PARM_FILAVAIL];
    scale_factor = (int)cd_values[H5Z_SCALEOFFSET_PARM_SCALEFACTOR];
    scale_type   = (H5Z_SO_scale_type_t)cd_values[H5Z_SCALEOFFSET_PARM_SCALETYPE];

    /* check and assign proper values set by user to related parameters
     * scale type can be H5Z_SO_FLOAT_DSCALE (0), H5Z_SO_FLOAT_ESCALE (1) or H5Z_SO_INT (other)
     * H5Z_SO_FLOAT_DSCALE : floating-point type, variable-minimum-bits method,
     *                      scale factor is decimal scale factor
     * H5Z_SO_FLOAT_ESCALE : floating-point type, fixed-minimum-bits method,
     *                      scale factor is the fixed minimum number of bits
     * H5Z_SO_INT          : integer type, scale_factor is minimum number of bits
     */
    if (dtype_class == H5Z_SCALEOFFSET_CLS_FLOAT) { /* floating-point type */
        if (scale_type != H5Z_SO_FLOAT_DSCALE && scale_type != H5Z_SO_FLOAT_ESCALE)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, 0, "invalid scale type");
    }

    if (dtype_class == H5Z_SCALEOFFSET_CLS_INTEGER) { /* integer type */
        if (scale_type != H5Z_SO_INT)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, 0, "invalid scale type");

        /* if scale_factor is less than 0 for integer, library will reset it to 0
         * in this case, library will calculate the minimum-bits
         */
        if (scale_factor < 0)
            scale_factor = 0;
    }

    /* fixed-minimum-bits method is not implemented and is forbidden */
    if (scale_type == H5Z_SO_FLOAT_ESCALE)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, 0, "E-scaling method not supported");

    if (scale_type == H5Z_SO_FLOAT_DSCALE) { /* floating-point type, variable-minimum-bits */
        D_val = (double)scale_factor;
    }
    else { /* integer type, or floating-point type with fixed-minimum-bits method */
        if (scale_factor > (int)(cd_values[H5Z_SCALEOFFSET_PARM_SIZE] * 8))
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, 0, "minimum number of bits exceeds maximum");

        /* no need to process data */
        if (scale_factor == (int)(cd_values[H5Z_SCALEOFFSET_PARM_SIZE] * 8)) {
            ret_value = *buf_size;
            goto done;
        }
        minbits = (uint32_t)scale_factor;
    }

    /* prepare parameters to pass to compress/decompress functions */
    p.size      = cd_values[H5Z_SCALEOFFSET_PARM_SIZE];
    p.mem_order = (unsigned)H5T_native_order_g;

    /* input; decompress */
    if (flags & H5Z_FLAG_REVERSE) {
        /* retrieve values of minbits and minval from input compressed buffer
         * retrieve them corresponding to how they are stored during compression
         */
        uint32_t           minbits_mask = 0;
        unsigned long long minval_mask  = 0;
        unsigned           minval_size  = 0;

        minbits = 0;
        for (i = 0; i < 4; i++) {
            minbits_mask = ((unsigned char *)*buf)[i];
            minbits_mask <<= i * 8;
            minbits |= minbits_mask;
        }

        /* retrieval of minval takes into consideration situation where sizeof
         * unsigned long long (datatype of minval) may change from compression
         * to decompression, only smaller size is used
         */
        minval_size = sizeof(unsigned long long) <= ((unsigned char *)*buf)[4] ? sizeof(unsigned long long)
                                                                               : ((unsigned char *)*buf)[4];
        minval      = 0;
        for (i = 0; i < minval_size; i++) {
            minval_mask = ((unsigned char *)*buf)[5 + i];
            minval_mask <<= i * 8;
            minval |= minval_mask;
        }

        assert(minbits <= p.size * 8);
        p.minbits = minbits;

        /* calculate size of output buffer after decompression */
        size_out = d_nelmts * (size_t)p.size;

        /* allocate memory space for decompressed buffer */
        if (NULL == (outbuf = (unsigned char *)H5MM_malloc(size_out)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, 0,
                        "memory allocation failed for scaleoffset decompression");

        /* special case: minbits equal to full precision */
        if (minbits == p.size * 8) {
            H5MM_memcpy(outbuf, (unsigned char *)(*buf) + buf_offset, size_out);
            /* free the original buffer */
            H5MM_xfree(*buf);

            /* convert to dataset datatype endianness order if needed */
            if (need_convert)
                H5Z__scaleoffset_convert(outbuf, d_nelmts, p.size);

            *buf      = outbuf;
            outbuf    = NULL;
            *buf_size = size_out;
            ret_value = size_out;
            goto done;
        }

        /* decompress the buffer if minbits not equal to zero */
        if (minbits != 0)
            H5Z__scaleoffset_decompress(outbuf, d_nelmts, (unsigned char *)(*buf) + buf_offset, p);
        else {
            /* fill value is not defined and all data elements have the same value */
            for (i = 0; i < size_out; i++)
                outbuf[i] = 0;
        }

        /* before postprocess, get memory type */
        if ((type = H5Z__scaleoffset_get_type(dtype_class, p.size, dtype_sign)) == 0)
            HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, 0, "cannot use C integer datatype for cast");

        /* postprocess after decompression */
        if (dtype_class == H5Z_SCALEOFFSET_CLS_INTEGER)
            H5Z__scaleoffset_postdecompress_i(outbuf, d_nelmts, type, filavail, cd_values, minbits, minval);

        if (dtype_class == H5Z_SCALEOFFSET_CLS_FLOAT)
            if (scale_type == 0) { /* variable-minimum-bits method */
                if (H5Z__scaleoffset_postdecompress_fd(outbuf, d_nelmts, type, filavail, cd_values, minbits,
                                                       minval, D_val) == FAIL)
                    HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, 0, "post-decompression failed");
            }

        /* after postprocess, convert to dataset datatype endianness order if needed */
        if (need_convert)
            H5Z__scaleoffset_convert(outbuf, d_nelmts, p.size);
    }
    /* output; compress */
    else {
        size_t used_bytes;
        size_t unused_bytes;

        assert(nbytes == d_nelmts * p.size);

        /* before preprocess, convert to memory endianness order if needed */
        if (need_convert)
            H5Z__scaleoffset_convert(*buf, d_nelmts, p.size);

        /* before preprocess, get memory type */
        if ((type = H5Z__scaleoffset_get_type(dtype_class, p.size, dtype_sign)) == 0)
            HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, 0, "cannot use C integer datatype for cast");

        /* preprocess before compression */
        if (dtype_class == H5Z_SCALEOFFSET_CLS_INTEGER)
            H5Z__scaleoffset_precompress_i(*buf, d_nelmts, type, filavail, cd_values, &minbits, &minval);

        if (dtype_class == H5Z_SCALEOFFSET_CLS_FLOAT)
            if (scale_type == 0) { /* variable-minimum-bits method */
                if (H5Z__scaleoffset_precompress_fd(*buf, d_nelmts, type, filavail, cd_values, &minbits,
                                                    &minval, D_val) == FAIL)
                    HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, 0, "pre-compression failed");
            }

        assert(minbits <= p.size * 8);

        /* calculate buffer size after compression
         * minbits and minval are stored in the front of the compressed buffer
         */
        p.minbits = minbits;
        size_out  = buf_offset + nbytes * p.minbits / (p.size * 8) + 1; /* may be 1 larger */

        /* allocate memory space for compressed buffer */
        if (NULL == (outbuf = (unsigned char *)H5MM_malloc(size_out)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, 0, "memory allocation failed for scaleoffset compression");

        /* store minbits and minval in the front of output compressed buffer
         * store byte by byte from least significant byte to most significant byte
         * constant buffer size (21 bytes) is left for these two parameters
         * 4 bytes for minbits, 1 byte for size of minval, 16 bytes for minval
         */
        for (i = 0; i < 4; i++)
            ((unsigned char *)outbuf)[i] = (unsigned char)((minbits & ((uint32_t)0xff << i * 8)) >> i * 8);

        ((unsigned char *)outbuf)[4] = sizeof(unsigned long long);

        for (i = 0; i < sizeof(unsigned long long); i++)
            ((unsigned char *)outbuf)[5 + i] =
                (unsigned char)((minval & ((unsigned long long)0xff << i * 8)) >> i * 8);

        /* Zero out remaining, unused bytes */
        /* (Looks like an error in the original determination of how many
         *      bytes would be needed for parameters. - QAK, 2010/08/19)
         */
        used_bytes = 4 + 1 + sizeof(unsigned long long);
        assert(used_bytes <= size_out);
        unused_bytes = size_out - used_bytes;
        memset(outbuf + 13, 0, unused_bytes);

        /* special case: minbits equal to full precision */
        if (minbits == p.size * 8) {
            H5MM_memcpy(outbuf + buf_offset, *buf, nbytes);
            /* free the original buffer */
            H5MM_xfree(*buf);

            *buf      = outbuf;
            outbuf    = NULL;
            *buf_size = size_out;
            ret_value = buf_offset + nbytes;
            goto done;
        }

        /* compress the buffer if minbits not equal to zero
         * minbits equal to zero only when fill value is not defined and
         * all data elements have the same value
         */
        if (minbits != 0)
            H5Z__scaleoffset_compress((unsigned char *)*buf, d_nelmts, outbuf + buf_offset,
                                      size_out - buf_offset, p);
    }

    /* free the input buffer */
    H5MM_xfree(*buf);

    /* set return values */
    *buf      = outbuf;
    outbuf    = NULL;
    *buf_size = size_out;
    ret_value = size_out;

done:
    if (outbuf)
        H5MM_xfree(outbuf);
    FUNC_LEAVE_NOAPI(ret_value)
}

/* ============ Scaleoffset Algorithm ===============================================
 * assume one byte has 8 bit
 * assume padding bit is 0
 * assume size of unsigned char is one byte
 * assume one data item of certain datatype is stored continuously in bytes
 * atomic datatype is treated on byte basis
 */

/* change byte order of input buffer either from little-endian to big-endian
 * or from big-endian to little-endian  2/21/2005
 */
static void
H5Z__scaleoffset_convert(void *buf, unsigned d_nelmts, unsigned dtype_size)
{
    if (dtype_size > 1) {
        size_t         i, j;
        unsigned char *buffer, temp;

        buffer = (unsigned char *)buf;
        for (i = 0; i < d_nelmts * (size_t)dtype_size; i += dtype_size)
            for (j = 0; j < dtype_size / 2; j++) {
                /* swap pair of bytes */
                temp                           = buffer[i + j];
                buffer[i + j]                  = buffer[i + dtype_size - 1 - j];
                buffer[i + dtype_size - 1 - j] = temp;
            } /* end for */
    }         /* end if */
} /* end H5Z__scaleoffset_convert() */

/* return ceiling of floating-point log2 function
 * receive unsigned integer as argument 3/10/2005
 */
static unsigned
H5Z__scaleoffset_log2(unsigned long long num)
{
    unsigned           v           = 0;
    unsigned long long lower_bound = 1; /* is power of 2, largest value <= num */
    unsigned long long val         = num;

    while (val >>= 1) {
        v++;
        lower_bound <<= 1;
    }

    if (num == lower_bound)
        return v;
    else
        return v + 1;
}

/* precompress for integer type */
static void
H5Z__scaleoffset_precompress_i(void *data, unsigned d_nelmts, enum H5Z_scaleoffset_t type, unsigned filavail,
                               const unsigned cd_values[], uint32_t *minbits, unsigned long long *minval)
{
    if (type == t_uchar)
        H5Z_scaleoffset_precompress_1(unsigned char, data, d_nelmts, filavail, cd_values, minbits, minval);
    else if (type == t_ushort)
        H5Z_scaleoffset_precompress_1(unsigned short, data, d_nelmts, filavail, cd_values, minbits, minval);
    else if (type == t_uint)
        H5Z_scaleoffset_precompress_1(unsigned int, data, d_nelmts, filavail, cd_values, minbits, minval);
    else if (type == t_ulong)
        H5Z_scaleoffset_precompress_1(unsigned long, data, d_nelmts, filavail, cd_values, minbits, minval);
    else if (type == t_ulong_long)
        H5Z_scaleoffset_precompress_1(unsigned long long, data, d_nelmts, filavail, cd_values, minbits,
                                      minval);
    else if (type == t_schar) {
        signed char  *buf = (signed char *)data, min = 0, max = 0, filval = 0;
        unsigned char span;
        unsigned      i;

        if (filavail == H5Z_SCALEOFFSET_FILL_DEFINED) { /* fill value defined */
            H5Z_scaleoffset_get_filval_1(signed char, cd_values, filval);
            if (*minbits ==
                H5Z_SO_INT_MINBITS_DEFAULT) { /* minbits not set yet, calculate max, min, and minbits */
                H5Z_scaleoffset_max_min_1(i, d_nelmts, buf, filval, max,
                                          min) if ((unsigned char)(max - min) >
                                                   (unsigned char)(~(unsigned char)0 - 2))
                {
                    *minbits = sizeof(signed char) * 8;
                    return;
                }
                span     = (unsigned char)(max - min + 1);
                *minbits = H5Z__scaleoffset_log2((unsigned long long)(span + 1));
            }
            else /* minbits already set, only calculate min */
                H5Z_scaleoffset_min_1(i, d_nelmts, buf, filval,
                                      min) if (*minbits !=
                                               sizeof(signed char) *
                                                   8) /* change values if minbits != full precision */
                    for (i = 0; i < d_nelmts; i++) buf[i] =
                        (signed char)((buf[i] == filval) ? (((unsigned char)1 << *minbits) - 1)
                                                         : (buf[i] - min));
        }
        else { /* fill value undefined */
            if (*minbits ==
                H5Z_SO_INT_MINBITS_DEFAULT) { /* minbits not set yet, calculate max, min, and minbits */
                H5Z_scaleoffset_max_min_2(i, d_nelmts, buf, max,
                                          min) if ((unsigned char)(max - min) >
                                                   (unsigned char)(~(unsigned char)0 - 2))
                {
                    *minbits = sizeof(signed char) * 8;
                    *minval  = (unsigned long long)min;
                    return;
                }
                span     = (unsigned char)(max - min + 1);
                *minbits = H5Z__scaleoffset_log2((unsigned long long)span);
            }
            else /* minbits already set, only calculate min */
                H5Z_scaleoffset_min_2(i, d_nelmts, buf,
                                      min) if (*minbits !=
                                               sizeof(signed char) *
                                                   8) /* change values if minbits != full precision */
                    for (i = 0; i < d_nelmts; i++) buf[i] = (signed char)(buf[i] - min);
        }
        *minval = (unsigned long long)min;
    }
    else if (type == t_short)
        H5Z_scaleoffset_precompress_2(short, data, d_nelmts, filavail, cd_values, minbits, minval);
    else if (type == t_int)
        H5Z_scaleoffset_precompress_2(int, data, d_nelmts, filavail, cd_values, minbits, minval);
    else if (type == t_long)
        H5Z_scaleoffset_precompress_2(long, data, d_nelmts, filavail, cd_values, minbits, minval);
    else if (type == t_long_long)
        H5Z_scaleoffset_precompress_2(long long, data, d_nelmts, filavail, cd_values, minbits, minval);
}

/* postdecompress for integer type */
static void
H5Z__scaleoffset_postdecompress_i(void *data, unsigned d_nelmts, enum H5Z_scaleoffset_t type,
                                  unsigned filavail, const unsigned cd_values[], uint32_t minbits,
                                  unsigned long long minval)
{
    long long sminval = *(long long *)&minval; /* for signed integer types */

    if (type == t_uchar)
        H5Z_scaleoffset_postdecompress_1(unsigned char, data, d_nelmts, filavail, cd_values, minbits, minval);
    else if (type == t_ushort)
        H5Z_scaleoffset_postdecompress_1(unsigned short, data, d_nelmts, filavail, cd_values, minbits,
                                         minval);
    else if (type == t_uint)
        H5Z_scaleoffset_postdecompress_1(unsigned int, data, d_nelmts, filavail, cd_values, minbits, minval);
    else if (type == t_ulong)
        H5Z_scaleoffset_postdecompress_1(unsigned long, data, d_nelmts, filavail, cd_values, minbits, minval);
    else if (type == t_ulong_long)
        H5Z_scaleoffset_postdecompress_1(unsigned long long, data, d_nelmts, filavail, cd_values, minbits,
                                         minval);
    else if (type == t_schar) {
        signed char *buf = (signed char *)data, filval = 0;
        unsigned     i;

        if (filavail == H5Z_SCALEOFFSET_FILL_DEFINED) { /* fill value defined */
            H5Z_scaleoffset_get_filval_1(signed char, cd_values, filval);
            for (i = 0; i < d_nelmts; i++)
                buf[i] = (signed char)((buf[i] == (((unsigned char)1 << minbits) - 1)) ? filval
                                                                                       : (buf[i] + sminval));
        }
        else /* fill value undefined */
            for (i = 0; i < d_nelmts; i++)
                buf[i] = (signed char)(buf[i] + sminval);
    }
    else if (type == t_short)
        H5Z_scaleoffset_postdecompress_2(short, data, d_nelmts, filavail, cd_values, minbits, sminval);
    else if (type == t_int)
        H5Z_scaleoffset_postdecompress_2(int, data, d_nelmts, filavail, cd_values, minbits, sminval);
    else if (type == t_long)
        H5Z_scaleoffset_postdecompress_2(long, data, d_nelmts, filavail, cd_values, minbits, sminval);
    else if (type == t_long_long)
        H5Z_scaleoffset_postdecompress_2(long long, data, d_nelmts, filavail, cd_values, minbits, sminval);
}

/* precompress for floating-point type, variable-minimum-bits method
   success: non-negative, failure: negative 4/15/05 */
static herr_t
H5Z__scaleoffset_precompress_fd(void *data, unsigned d_nelmts, enum H5Z_scaleoffset_t type, unsigned filavail,
                                const unsigned cd_values[], uint32_t *minbits, unsigned long long *minval,
                                double D_val)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    if (type == t_float)
        H5Z_scaleoffset_precompress_3(float, powf, fabsf, roundf, lroundf, llroundf, data, d_nelmts, filavail,
                                      cd_values, minbits, minval, D_val);
    else if (type == t_double)
        H5Z_scaleoffset_precompress_3(double, pow, fabs, round, lround, llround, data, d_nelmts, filavail,
                                      cd_values, minbits, minval, D_val);

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/* postdecompress for floating-point type, variable-minimum-bits method
   success: non-negative, failure: negative 4/15/05 */
static herr_t
H5Z__scaleoffset_postdecompress_fd(void *data, unsigned d_nelmts, enum H5Z_scaleoffset_t type,
                                   unsigned filavail, const unsigned cd_values[], uint32_t minbits,
                                   unsigned long long minval, double D_val)
{
    long long sminval   = (long long)minval; /* for signed integer types */
    herr_t    ret_value = SUCCEED;           /* Return value */

    FUNC_ENTER_PACKAGE

    if (type == t_float)
        H5Z_scaleoffset_postdecompress_3(float, powf, data, d_nelmts, filavail, cd_values, minbits, sminval,
                                         D_val);
    else if (type == t_double)
        H5Z_scaleoffset_postdecompress_3(double, pow, data, d_nelmts, filavail, cd_values, minbits, sminval,
                                         D_val);

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

static void
H5Z__scaleoffset_next_byte(size_t *j, unsigned *buf_len)
{
    ++(*j);
    *buf_len = 8 * sizeof(unsigned char);
}

static void
H5Z__scaleoffset_decompress_one_byte(unsigned char *data, size_t data_offset, unsigned k, unsigned begin_i,
                                     const unsigned char *buffer, size_t *j, unsigned *buf_len,
                                     parms_atomic p, unsigned dtype_len)
{
    unsigned      dat_len; /* dat_len is the number of bits to be copied in each data byte */
    unsigned char val;     /* value to be copied in each data byte */

    /* initialize value and bits of unsigned char to be copied */
    val = buffer[*j];
    if (k == begin_i)
        dat_len = 8 - (dtype_len - p.minbits) % 8;
    else
        dat_len = 8;

    if (*buf_len > dat_len) {
        data[data_offset + k] =
            (unsigned char)((unsigned)(val >> (*buf_len - dat_len)) & (unsigned)(~((unsigned)~0 << dat_len)));
        *buf_len -= dat_len;
    } /* end if */
    else {
        data[data_offset + k] =
            (unsigned char)((val & ~((unsigned)(~0) << *buf_len)) << (dat_len - *buf_len));
        dat_len -= *buf_len;
        H5Z__scaleoffset_next_byte(j, buf_len);
        if (dat_len == 0)
            return;

        val = buffer[*j];
        data[data_offset + k] |=
            (unsigned char)((unsigned)(val >> (*buf_len - dat_len)) & ~((unsigned)(~0) << dat_len));
        *buf_len -= dat_len;
    } /* end else */
}

static void
H5Z__scaleoffset_decompress_one_atomic(unsigned char *data, size_t data_offset, unsigned char *buffer,
                                       size_t *j, unsigned *buf_len, parms_atomic p)
{
    /* begin_i: the index of byte having first significant bit */
    unsigned begin_i;
    unsigned dtype_len;
    int      k;

    assert(p.minbits > 0);

    dtype_len = p.size * 8;

    if (p.mem_order == H5Z_SCALEOFFSET_ORDER_LE) { /* little endian */
        begin_i = p.size - 1 - (dtype_len - p.minbits) / 8;

        for (k = (int)begin_i; k >= 0; k--)
            H5Z__scaleoffset_decompress_one_byte(data, data_offset, (unsigned)k, begin_i, buffer, j, buf_len,
                                                 p, dtype_len);
    }
    else { /* big endian */
        assert(p.mem_order == H5Z_SCALEOFFSET_ORDER_BE);

        begin_i = (dtype_len - p.minbits) / 8;

        for (k = (int)begin_i; k <= (int)(p.size - 1); k++)
            H5Z__scaleoffset_decompress_one_byte(data, data_offset, (unsigned)k, begin_i, buffer, j, buf_len,
                                                 p, dtype_len);
    }
}

static void
H5Z__scaleoffset_decompress(unsigned char *data, unsigned d_nelmts, unsigned char *buffer, parms_atomic p)
{
    /* i: index of data, j: index of buffer,
       buf_len: number of bits to be filled in current byte */
    size_t   i, j;
    unsigned buf_len;

    /* must initialize to zeros */
    for (i = 0; i < d_nelmts * (size_t)p.size; i++)
        data[i] = 0;

    /* initialization before the loop */
    j       = 0;
    buf_len = sizeof(unsigned char) * 8;

    /* decompress */
    for (i = 0; i < d_nelmts; i++)
        H5Z__scaleoffset_decompress_one_atomic(data, i * p.size, buffer, &j, &buf_len, p);
}

static void
H5Z__scaleoffset_compress_one_byte(const unsigned char *data, size_t data_offset, unsigned k,
                                   unsigned begin_i, unsigned char *buffer, size_t *j, unsigned *buf_len,
                                   parms_atomic p, unsigned dtype_len)
{
    unsigned      dat_len; /* dat_len is the number of bits to be copied in each data byte */
    unsigned char val;     /* value to be copied in each data byte */

    /* initialize value and bits of unsigned char to be copied */
    val = data[data_offset + k];
    if (k == begin_i)
        dat_len = 8 - (dtype_len - p.minbits) % 8;
    else
        dat_len = 8;

    if (*buf_len > dat_len) {
        buffer[*j] |= (unsigned char)((val & ~((unsigned)(~0) << dat_len)) << (*buf_len - dat_len));
        *buf_len -= dat_len;
    }
    else {
        buffer[*j] |=
            (unsigned char)((unsigned)(val >> (dat_len - *buf_len)) & ~((unsigned)(~0) << *buf_len));
        dat_len -= *buf_len;
        H5Z__scaleoffset_next_byte(j, buf_len);
        if (dat_len == 0)
            return;

        buffer[*j] = (unsigned char)((val & ~((unsigned)(~0) << dat_len)) << (*buf_len - dat_len));
        *buf_len -= dat_len;
    } /* end else */
}

static void
H5Z__scaleoffset_compress_one_atomic(unsigned char *data, size_t data_offset, unsigned char *buffer,
                                     size_t *j, unsigned *buf_len, parms_atomic p)
{
    /* begin_i: the index of byte having first significant bit */
    unsigned begin_i;
    unsigned dtype_len;
    int      k;

    assert(p.minbits > 0);

    dtype_len = p.size * 8;

    if (p.mem_order == H5Z_SCALEOFFSET_ORDER_LE) { /* little endian */
        begin_i = p.size - 1 - (dtype_len - p.minbits) / 8;

        for (k = (int)begin_i; k >= 0; k--)
            H5Z__scaleoffset_compress_one_byte(data, data_offset, (unsigned)k, begin_i, buffer, j, buf_len, p,
                                               dtype_len);
    }
    else { /* big endian */
        assert(p.mem_order == H5Z_SCALEOFFSET_ORDER_BE);
        begin_i = (dtype_len - p.minbits) / 8;

        for (k = (int)begin_i; k <= (int)(p.size - 1); k++)
            H5Z__scaleoffset_compress_one_byte(data, data_offset, (unsigned)k, begin_i, buffer, j, buf_len, p,
                                               dtype_len);
    }
}

static void
H5Z__scaleoffset_compress(unsigned char *data, unsigned d_nelmts, unsigned char *buffer, size_t buffer_size,
                          parms_atomic p)
{
    /* i: index of data, j: index of buffer,
       buf_len: number of bits to be filled in current byte */
    size_t   i, j;
    unsigned buf_len;

    /* must initialize buffer to be zeros */
    for (j = 0; j < buffer_size; j++)
        buffer[j] = 0;

    /* initialization before the loop */
    j       = 0;
    buf_len = sizeof(unsigned char) * 8;

    /* compress */
    for (i = 0; i < d_nelmts; i++)
        H5Z__scaleoffset_compress_one_atomic(data, i * p.size, buffer, &j, &buf_len, p);
}
