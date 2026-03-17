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
 * Module Info: This module contains most of the "core" functionality of
 *      the H5T interface, including the API initialization code, etc.
 *      Many routines that are infrequently used, or are specialized for
 *      one particular datatype class are in another module.
 */

/****************/
/* Module Setup */
/****************/

#include "H5Tmodule.h" /* This source code file is part of the H5T module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5ACprivate.h" /* Metadata cache                           */
#include "H5CXprivate.h" /* API Contexts                             */
#include "H5Dprivate.h"  /* Datasets                                 */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5ESprivate.h" /* Event Sets                               */
#include "H5Fprivate.h"  /* Files                                    */
#include "H5FLprivate.h" /* Free Lists                               */
#include "H5FOprivate.h" /* File objects                             */
#include "H5Gprivate.h"  /* Groups                                   */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5MMprivate.h" /* Memory management                        */
#include "H5Pprivate.h"  /* Property lists                           */
#include "H5Tpkg.h"      /* Datatypes                                */
#include "H5VLprivate.h" /* Virtual Object Layer                     */

/****************/
/* Local Macros */
/****************/

#define H5T_ENCODE_VERSION 0

/*
 * Type initialization macros
 *
 * These use the "template macro" technique to reduce the amount of gratuitous
 * duplicated code when initializing the datatypes for the library.  The main
 * template macro is the H5T_INIT_TYPE() macro below.
 *
 */

/* Define the code template for bitfields for the "GUTS" in the H5T_INIT_TYPE macro */
#define H5T_INIT_TYPE_BITFIELD_CORE                                                                          \
    {                                                                                                        \
        dt->shared->type = H5T_BITFIELD;                                                                     \
    }

#define H5T_INIT_TYPE_BITFIELD_COMMON(ENDIANNESS)                                                            \
    {                                                                                                        \
        H5T_INIT_TYPE_NUM_COMMON(ENDIANNESS)                                                                 \
        H5T_INIT_TYPE_BITFIELD_CORE;                                                                         \
    }

#define H5T_INIT_TYPE_BITFIELDLE_CORE                                                                        \
    {                                                                                                        \
        H5T_INIT_TYPE_BITFIELD_COMMON(H5T_ORDER_LE)                                                          \
    }

#define H5T_INIT_TYPE_BITFIELDBE_CORE                                                                        \
    {                                                                                                        \
        H5T_INIT_TYPE_BITFIELD_COMMON(H5T_ORDER_BE)                                                          \
    }

/* Define the code template for times for the "GUTS" in the H5T_INIT_TYPE macro */
#define H5T_INIT_TYPE_TIME_CORE                                                                              \
    {                                                                                                        \
        dt->shared->type = H5T_TIME;                                                                         \
    }

/* Define the code template for types which reset the offset for the "GUTS" in the H5T_INIT_TYPE macro */
#define H5T_INIT_TYPE_OFFSET_CORE                                                                            \
    {                                                                                                        \
        dt->shared->u.atomic.offset = 0;                                                                     \
    }

/* Define common code for all numeric types (floating-point & int, signed & unsigned) */
#define H5T_INIT_TYPE_NUM_COMMON(ENDIANNESS)                                                                 \
    {                                                                                                        \
        dt->shared->u.atomic.order   = ENDIANNESS;                                                           \
        dt->shared->u.atomic.offset  = 0;                                                                    \
        dt->shared->u.atomic.lsb_pad = H5T_PAD_ZERO;                                                         \
        dt->shared->u.atomic.msb_pad = H5T_PAD_ZERO;                                                         \
    }

/* Define the code templates for standard floats for the "GUTS" in the H5T_INIT_TYPE macro */
#define H5T_INIT_TYPE_FLOAT_COMMON(ENDIANNESS)                                                               \
    {                                                                                                        \
        H5T_INIT_TYPE_NUM_COMMON(ENDIANNESS)                                                                 \
        dt->shared->u.atomic.u.f.sign  = 31;                                                                 \
        dt->shared->u.atomic.u.f.epos  = 23;                                                                 \
        dt->shared->u.atomic.u.f.esize = 8;                                                                  \
        dt->shared->u.atomic.u.f.ebias = 0x7f;                                                               \
        dt->shared->u.atomic.u.f.mpos  = 0;                                                                  \
        dt->shared->u.atomic.u.f.msize = 23;                                                                 \
        dt->shared->u.atomic.u.f.norm  = H5T_NORM_IMPLIED;                                                   \
        dt->shared->u.atomic.u.f.pad   = H5T_PAD_ZERO;                                                       \
    }

#define H5T_INIT_TYPE_FLOATLE_CORE                                                                           \
    {                                                                                                        \
        H5T_INIT_TYPE_FLOAT_COMMON(H5T_ORDER_LE)                                                             \
    }

#define H5T_INIT_TYPE_FLOATBE_CORE                                                                           \
    {                                                                                                        \
        H5T_INIT_TYPE_FLOAT_COMMON(H5T_ORDER_BE)                                                             \
    }

/* Define the code templates for standard doubles for the "GUTS" in the H5T_INIT_TYPE macro */
#define H5T_INIT_TYPE_DOUBLE_COMMON(ENDIANNESS)                                                              \
    {                                                                                                        \
        H5T_INIT_TYPE_NUM_COMMON(ENDIANNESS)                                                                 \
        dt->shared->u.atomic.u.f.sign  = 63;                                                                 \
        dt->shared->u.atomic.u.f.epos  = 52;                                                                 \
        dt->shared->u.atomic.u.f.esize = 11;                                                                 \
        dt->shared->u.atomic.u.f.ebias = 0x03ff;                                                             \
        dt->shared->u.atomic.u.f.mpos  = 0;                                                                  \
        dt->shared->u.atomic.u.f.msize = 52;                                                                 \
        dt->shared->u.atomic.u.f.norm  = H5T_NORM_IMPLIED;                                                   \
        dt->shared->u.atomic.u.f.pad   = H5T_PAD_ZERO;                                                       \
    }

#define H5T_INIT_TYPE_DOUBLELE_CORE                                                                          \
    {                                                                                                        \
        H5T_INIT_TYPE_DOUBLE_COMMON(H5T_ORDER_LE)                                                            \
    }

#define H5T_INIT_TYPE_DOUBLEBE_CORE                                                                          \
    {                                                                                                        \
        H5T_INIT_TYPE_DOUBLE_COMMON(H5T_ORDER_BE)                                                            \
    }

/* Define the code templates for VAX float for the "GUTS" in the H5T_INIT_TYPE macro */
#define H5T_INIT_TYPE_FLOATVAX_CORE                                                                          \
    {                                                                                                        \
        H5T_INIT_TYPE_NUM_COMMON(H5T_ORDER_VAX)                                                              \
        dt->shared->u.atomic.u.f.sign  = 31;                                                                 \
        dt->shared->u.atomic.u.f.epos  = 23;                                                                 \
        dt->shared->u.atomic.u.f.esize = 8;                                                                  \
        dt->shared->u.atomic.u.f.ebias = 0x81;                                                               \
        dt->shared->u.atomic.u.f.mpos  = 0;                                                                  \
        dt->shared->u.atomic.u.f.msize = 23;                                                                 \
        dt->shared->u.atomic.u.f.norm  = H5T_NORM_IMPLIED;                                                   \
        dt->shared->u.atomic.u.f.pad   = H5T_PAD_ZERO;                                                       \
        dt->shared->version            = H5O_DTYPE_VERSION_3;                                                \
    }

/* Define the code templates for VAX double for the "GUTS" in the H5T_INIT_TYPE macro */
#define H5T_INIT_TYPE_DOUBLEVAX_CORE                                                                         \
    {                                                                                                        \
        H5T_INIT_TYPE_NUM_COMMON(H5T_ORDER_VAX)                                                              \
        dt->shared->u.atomic.u.f.sign  = 63;                                                                 \
        dt->shared->u.atomic.u.f.epos  = 52;                                                                 \
        dt->shared->u.atomic.u.f.esize = 11;                                                                 \
        dt->shared->u.atomic.u.f.ebias = 0x0401;                                                             \
        dt->shared->u.atomic.u.f.mpos  = 0;                                                                  \
        dt->shared->u.atomic.u.f.msize = 52;                                                                 \
        dt->shared->u.atomic.u.f.norm  = H5T_NORM_IMPLIED;                                                   \
        dt->shared->u.atomic.u.f.pad   = H5T_PAD_ZERO;                                                       \
        dt->shared->version            = H5O_DTYPE_VERSION_3;                                                \
    }

/* Define the code templates for standard signed integers for the "GUTS" in the H5T_INIT_TYPE macro */
#define H5T_INIT_TYPE_SINT_COMMON(ENDIANNESS)                                                                \
    {                                                                                                        \
        H5T_INIT_TYPE_NUM_COMMON(ENDIANNESS)                                                                 \
        dt->shared->u.atomic.u.i.sign = H5T_SGN_2;                                                           \
    }

#define H5T_INIT_TYPE_SINTLE_CORE                                                                            \
    {                                                                                                        \
        H5T_INIT_TYPE_SINT_COMMON(H5T_ORDER_LE)                                                              \
    }

#define H5T_INIT_TYPE_SINTBE_CORE                                                                            \
    {                                                                                                        \
        H5T_INIT_TYPE_SINT_COMMON(H5T_ORDER_BE)                                                              \
    }

/* Define the code templates for standard unsigned integers for the "GUTS" in the H5T_INIT_TYPE macro */
#define H5T_INIT_TYPE_UINT_COMMON(ENDIANNESS)                                                                \
    {                                                                                                        \
        H5T_INIT_TYPE_NUM_COMMON(ENDIANNESS)                                                                 \
        dt->shared->u.atomic.u.i.sign = H5T_SGN_NONE;                                                        \
    }

#define H5T_INIT_TYPE_UINTLE_CORE                                                                            \
    {                                                                                                        \
        H5T_INIT_TYPE_UINT_COMMON(H5T_ORDER_LE)                                                              \
    }

#define H5T_INIT_TYPE_UINTBE_CORE                                                                            \
    {                                                                                                        \
        H5T_INIT_TYPE_UINT_COMMON(H5T_ORDER_BE)                                                              \
    }

/* Define a macro for common code for all newly allocate datatypes */
#define H5T_INIT_TYPE_ALLOC_COMMON(TYPE)                                                                     \
    {                                                                                                        \
        dt->sh_loc.type  = H5O_SHARE_TYPE_UNSHARED;                                                          \
        dt->shared->type = TYPE;                                                                             \
    }

/* Define the code templates for opaque for the "GUTS" in the H5T_INIT_TYPE macro */
#define H5T_INIT_TYPE_OPAQ_CORE                                                                              \
    {                                                                                                        \
        H5T_INIT_TYPE_ALLOC_COMMON(H5T_OPAQUE)                                                               \
        dt->shared->u.opaque.tag = H5MM_xstrdup("");                                                         \
    }

/* Define the code templates for strings for the "GUTS" in the H5T_INIT_TYPE macro */
#define H5T_INIT_TYPE_STRING_COMMON                                                                          \
    {                                                                                                        \
        H5T_INIT_TYPE_ALLOC_COMMON(H5T_STRING)                                                               \
        H5T_INIT_TYPE_NUM_COMMON(H5T_ORDER_NONE)                                                             \
        dt->shared->u.atomic.u.s.cset = H5F_DEFAULT_CSET;                                                    \
    }

#define H5T_INIT_TYPE_CSTRING_CORE                                                                           \
    {                                                                                                        \
        H5T_INIT_TYPE_STRING_COMMON                                                                          \
        dt->shared->u.atomic.u.s.pad = H5T_STR_NULLTERM;                                                     \
    }

#define H5T_INIT_TYPE_FORSTRING_CORE                                                                         \
    {                                                                                                        \
        H5T_INIT_TYPE_STRING_COMMON                                                                          \
        dt->shared->u.atomic.u.s.pad = H5T_STR_SPACEPAD;                                                     \
    }

/* Define the code templates for references for the "GUTS" in the H5T_INIT_TYPE macro */
#define H5T_INIT_TYPE_REF_COMMON                                                                             \
    {                                                                                                        \
        H5T_INIT_TYPE_ALLOC_COMMON(H5T_REFERENCE)                                                            \
        H5T_INIT_TYPE_NUM_COMMON(H5T_ORDER_NONE)                                                             \
        dt->shared->force_conv        = true;                                                                \
        dt->shared->u.atomic.u.r.file = NULL;                                                                \
        dt->shared->u.atomic.u.r.loc  = H5T_LOC_BADLOC;                                                      \
        dt->shared->u.atomic.u.r.cls  = NULL;                                                                \
    }

#define H5T_INIT_TYPE_OBJREF_CORE                                                                            \
    {                                                                                                        \
        H5T_INIT_TYPE_REF_COMMON                                                                             \
        dt->shared->u.atomic.u.r.rtype   = H5R_OBJECT1;                                                      \
        dt->shared->u.atomic.u.r.opaque  = false;                                                            \
        dt->shared->u.atomic.u.r.version = 0;                                                                \
    }

#define H5T_INIT_TYPE_REGREF_CORE                                                                            \
    {                                                                                                        \
        H5T_INIT_TYPE_REF_COMMON                                                                             \
        dt->shared->u.atomic.u.r.rtype   = H5R_DATASET_REGION1;                                              \
        dt->shared->u.atomic.u.r.opaque  = false;                                                            \
        dt->shared->u.atomic.u.r.version = 0;                                                                \
    }

/* rtype value is only used as a placeholder to differentiate the type from
 * other types, any opaque (i.e. "new") reference type could be used.
 */
#define H5T_INIT_TYPE_REF_CORE                                                                               \
    {                                                                                                        \
        H5T_INIT_TYPE_REF_COMMON                                                                             \
        dt->shared->u.atomic.u.r.rtype   = H5R_OBJECT2;                                                      \
        dt->shared->u.atomic.u.r.opaque  = true;                                                             \
        dt->shared->u.atomic.u.r.version = H5R_ENCODE_VERSION;                                               \
        dt->shared->version              = H5O_DTYPE_VERSION_4;                                              \
    }

/* Define the code templates for the "SIZE_TMPL" in the H5T_INIT_TYPE macro */
#define H5T_INIT_TYPE_SET_SIZE(SIZE)                                                                         \
    {                                                                                                        \
        dt->shared->size          = SIZE;                                                                    \
        dt->shared->u.atomic.prec = 8 * (SIZE);                                                              \
    }

#define H5T_INIT_TYPE_NOSET_SIZE(SIZE)                                                                       \
    {                                                                                                        \
    }

/* Define the code templates for the "CRT_TMPL" in the H5T_INIT_TYPE macro */
#define H5T_INIT_TYPE_COPY_CREATE(BASE)                                                                      \
    {                                                                                                        \
        /* Base off of existing datatype */                                                                  \
        if (NULL == (dt = H5T_copy(BASE, H5T_COPY_TRANSIENT)))                                               \
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, FAIL, "duplicating base type failed");                   \
    }

#define H5T_INIT_TYPE_ALLOC_CREATE(BASE)                                                                     \
    {                                                                                                        \
        /* Allocate new datatype info */                                                                     \
        if (NULL == (dt = H5T__alloc()))                                                                     \
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTALLOC, FAIL, "memory allocation failed");                      \
    }

#define H5T_INIT_TYPE(GUTS, GLOBAL, CRT_TMPL, BASE, SIZE_TMPL, SIZE)                                         \
    {                                                                                                        \
        /* Get new datatype struct */                                                                        \
        H5_GLUE3(H5T_INIT_TYPE_, CRT_TMPL, _CREATE)                                                          \
        (BASE)                                                                                               \
                                                                                                             \
            /* Adjust information for all types */                                                           \
            dt->shared->state = H5T_STATE_IMMUTABLE;                                                         \
        H5_GLUE3(H5T_INIT_TYPE_, SIZE_TMPL, _SIZE)                                                           \
        (SIZE)                                                                                               \
                                                                                                             \
            /* Adjust information for this type */                                                           \
            H5_GLUE3(H5T_INIT_TYPE_, GUTS, _CORE)                                                            \
                                                                                                             \
            /* Register result */                                                                            \
            if (((GLOBAL) = H5I_register(H5I_DATATYPE, dt, false)) < 0)                                      \
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREGISTER, FAIL, "unable to register datatype atom");       \
    }

/******************/
/* Local Typedefs */
/******************/

/* Typedef for recursive const-correct datatype copying routines */
typedef H5T_t *(*H5T_copy_func_t)(H5T_t *old_dt);

/********************/
/* Local Prototypes */
/********************/
static herr_t H5T__register_int(H5T_pers_t pers, const char *name, H5T_t *src, H5T_t *dst,
                                H5T_lib_conv_t func);
static herr_t H5T__register(H5T_pers_t pers, const char *name, H5T_t *src, H5T_t *dst, H5T_conv_func_t *conv);
static herr_t H5T__unregister(H5T_pers_t pers, const char *name, H5T_t *src, H5T_t *dst, H5T_conv_t func);
static htri_t H5T__compiler_conv(H5T_t *src, H5T_t *dst);
static herr_t H5T__set_size(H5T_t *dt, size_t size);
static herr_t H5T__close_cb(H5T_t *dt, void **request);
static H5T_path_t *H5T__path_find_real(const H5T_t *src, const H5T_t *dst, const char *name,
                                       H5T_conv_func_t *conv);
static bool        H5T__detect_vlen_ref(const H5T_t *dt);
static H5T_t      *H5T__initiate_copy(const H5T_t *old_dt);
static H5T_t      *H5T__copy_transient(H5T_t *old_dt);
static H5T_t      *H5T__copy_all(H5T_t *old_dt);
static herr_t      H5T__complete_copy(H5T_t *new_dt, const H5T_t *old_dt, H5T_shared_t *reopened_fo,
                                      bool set_memory_type, H5T_copy_func_t copyfn);

/*****************************/
/* Library Private Variables */
/*****************************/

/* The native endianness of the platform */
H5T_order_t H5T_native_order_g = H5T_ORDER_ERROR;

/*********************/
/* Package Variables */
/*********************/

/*
 * Predefined data types. These are initialized at runtime by H5T_init().
 *
 * If more of these are added, the new ones must be added to the list of
 * types to reset in H5T_term_package().
 */
hid_t H5T_IEEE_F32BE_g = FAIL;
hid_t H5T_IEEE_F32LE_g = FAIL;
hid_t H5T_IEEE_F64BE_g = FAIL;
hid_t H5T_IEEE_F64LE_g = FAIL;

hid_t H5T_VAX_F32_g = FAIL;
hid_t H5T_VAX_F64_g = FAIL;

hid_t H5T_STD_I8BE_g        = FAIL;
hid_t H5T_STD_I8LE_g        = FAIL;
hid_t H5T_STD_I16BE_g       = FAIL;
hid_t H5T_STD_I16LE_g       = FAIL;
hid_t H5T_STD_I32BE_g       = FAIL;
hid_t H5T_STD_I32LE_g       = FAIL;
hid_t H5T_STD_I64BE_g       = FAIL;
hid_t H5T_STD_I64LE_g       = FAIL;
hid_t H5T_STD_U8BE_g        = FAIL;
hid_t H5T_STD_U8LE_g        = FAIL;
hid_t H5T_STD_U16BE_g       = FAIL;
hid_t H5T_STD_U16LE_g       = FAIL;
hid_t H5T_STD_U32BE_g       = FAIL;
hid_t H5T_STD_U32LE_g       = FAIL;
hid_t H5T_STD_U64BE_g       = FAIL;
hid_t H5T_STD_U64LE_g       = FAIL;
hid_t H5T_STD_B8BE_g        = FAIL;
hid_t H5T_STD_B8LE_g        = FAIL;
hid_t H5T_STD_B16BE_g       = FAIL;
hid_t H5T_STD_B16LE_g       = FAIL;
hid_t H5T_STD_B32BE_g       = FAIL;
hid_t H5T_STD_B32LE_g       = FAIL;
hid_t H5T_STD_B64BE_g       = FAIL;
hid_t H5T_STD_B64LE_g       = FAIL;
hid_t H5T_STD_REF_OBJ_g     = FAIL;
hid_t H5T_STD_REF_DSETREG_g = FAIL;
hid_t H5T_STD_REF_g         = FAIL;

hid_t H5T_UNIX_D32BE_g = FAIL;
hid_t H5T_UNIX_D32LE_g = FAIL;
hid_t H5T_UNIX_D64BE_g = FAIL;
hid_t H5T_UNIX_D64LE_g = FAIL;

hid_t H5T_C_S1_g = FAIL;

hid_t H5T_FORTRAN_S1_g = FAIL;

hid_t H5T_NATIVE_SCHAR_g   = FAIL;
hid_t H5T_NATIVE_UCHAR_g   = FAIL;
hid_t H5T_NATIVE_SHORT_g   = FAIL;
hid_t H5T_NATIVE_USHORT_g  = FAIL;
hid_t H5T_NATIVE_INT_g     = FAIL;
hid_t H5T_NATIVE_UINT_g    = FAIL;
hid_t H5T_NATIVE_LONG_g    = FAIL;
hid_t H5T_NATIVE_ULONG_g   = FAIL;
hid_t H5T_NATIVE_LLONG_g   = FAIL;
hid_t H5T_NATIVE_ULLONG_g  = FAIL;
hid_t H5T_NATIVE_FLOAT_g   = FAIL;
hid_t H5T_NATIVE_DOUBLE_g  = FAIL;
hid_t H5T_NATIVE_LDOUBLE_g = FAIL;
hid_t H5T_NATIVE_B8_g      = FAIL;
hid_t H5T_NATIVE_B16_g     = FAIL;
hid_t H5T_NATIVE_B32_g     = FAIL;
hid_t H5T_NATIVE_B64_g     = FAIL;
hid_t H5T_NATIVE_OPAQUE_g  = FAIL;
hid_t H5T_NATIVE_HADDR_g   = FAIL;
hid_t H5T_NATIVE_HSIZE_g   = FAIL;
hid_t H5T_NATIVE_HSSIZE_g  = FAIL;
hid_t H5T_NATIVE_HERR_g    = FAIL;
hid_t H5T_NATIVE_HBOOL_g   = FAIL;

hid_t H5T_NATIVE_INT8_g        = FAIL;
hid_t H5T_NATIVE_UINT8_g       = FAIL;
hid_t H5T_NATIVE_INT_LEAST8_g  = FAIL;
hid_t H5T_NATIVE_UINT_LEAST8_g = FAIL;
hid_t H5T_NATIVE_INT_FAST8_g   = FAIL;
hid_t H5T_NATIVE_UINT_FAST8_g  = FAIL;

hid_t H5T_NATIVE_INT16_g        = FAIL;
hid_t H5T_NATIVE_UINT16_g       = FAIL;
hid_t H5T_NATIVE_INT_LEAST16_g  = FAIL;
hid_t H5T_NATIVE_UINT_LEAST16_g = FAIL;
hid_t H5T_NATIVE_INT_FAST16_g   = FAIL;
hid_t H5T_NATIVE_UINT_FAST16_g  = FAIL;

hid_t H5T_NATIVE_INT32_g        = FAIL;
hid_t H5T_NATIVE_UINT32_g       = FAIL;
hid_t H5T_NATIVE_INT_LEAST32_g  = FAIL;
hid_t H5T_NATIVE_UINT_LEAST32_g = FAIL;
hid_t H5T_NATIVE_INT_FAST32_g   = FAIL;
hid_t H5T_NATIVE_UINT_FAST32_g  = FAIL;

hid_t H5T_NATIVE_INT64_g        = FAIL;
hid_t H5T_NATIVE_UINT64_g       = FAIL;
hid_t H5T_NATIVE_INT_LEAST64_g  = FAIL;
hid_t H5T_NATIVE_UINT_LEAST64_g = FAIL;
hid_t H5T_NATIVE_INT_FAST64_g   = FAIL;
hid_t H5T_NATIVE_UINT_FAST64_g  = FAIL;

/*
 * Alignment constraints for HDF5 types.  Accessing objects of these
 * types with improper alignment invokes C undefined behavior, so the
 * library lays out objects with correct alignment, always.
 *
 * A value of N indicates that the data must be aligned on an address
 * ADDR such that 0 == ADDR mod N. When N=1 no alignment is required;
 * N=0 implies that alignment constraints were not calculated.  These
 * values are used for structure alignment.
 */
size_t H5T_POINTER_ALIGN_g     = 0;
size_t H5T_HVL_ALIGN_g         = 0;
size_t H5T_HOBJREF_ALIGN_g     = 0;
size_t H5T_HDSETREGREF_ALIGN_g = 0;
size_t H5T_REF_ALIGN_g         = 0;

size_t H5T_NATIVE_SCHAR_ALIGN_g   = 0;
size_t H5T_NATIVE_UCHAR_ALIGN_g   = 0;
size_t H5T_NATIVE_SHORT_ALIGN_g   = 0;
size_t H5T_NATIVE_USHORT_ALIGN_g  = 0;
size_t H5T_NATIVE_INT_ALIGN_g     = 0;
size_t H5T_NATIVE_UINT_ALIGN_g    = 0;
size_t H5T_NATIVE_LONG_ALIGN_g    = 0;
size_t H5T_NATIVE_ULONG_ALIGN_g   = 0;
size_t H5T_NATIVE_LLONG_ALIGN_g   = 0;
size_t H5T_NATIVE_ULLONG_ALIGN_g  = 0;
size_t H5T_NATIVE_FLOAT_ALIGN_g   = 0;
size_t H5T_NATIVE_DOUBLE_ALIGN_g  = 0;
size_t H5T_NATIVE_LDOUBLE_ALIGN_g = 0;

/* Alignment constraints for C99 types */
size_t H5T_NATIVE_INT8_ALIGN_g        = 0;
size_t H5T_NATIVE_UINT8_ALIGN_g       = 0;
size_t H5T_NATIVE_INT_LEAST8_ALIGN_g  = 0;
size_t H5T_NATIVE_UINT_LEAST8_ALIGN_g = 0;
size_t H5T_NATIVE_INT_FAST8_ALIGN_g   = 0;
size_t H5T_NATIVE_UINT_FAST8_ALIGN_g  = 0;

size_t H5T_NATIVE_INT16_ALIGN_g        = 0;
size_t H5T_NATIVE_UINT16_ALIGN_g       = 0;
size_t H5T_NATIVE_INT_LEAST16_ALIGN_g  = 0;
size_t H5T_NATIVE_UINT_LEAST16_ALIGN_g = 0;
size_t H5T_NATIVE_INT_FAST16_ALIGN_g   = 0;
size_t H5T_NATIVE_UINT_FAST16_ALIGN_g  = 0;

size_t H5T_NATIVE_INT32_ALIGN_g        = 0;
size_t H5T_NATIVE_UINT32_ALIGN_g       = 0;
size_t H5T_NATIVE_INT_LEAST32_ALIGN_g  = 0;
size_t H5T_NATIVE_UINT_LEAST32_ALIGN_g = 0;
size_t H5T_NATIVE_INT_FAST32_ALIGN_g   = 0;
size_t H5T_NATIVE_UINT_FAST32_ALIGN_g  = 0;

size_t H5T_NATIVE_INT64_ALIGN_g        = 0;
size_t H5T_NATIVE_UINT64_ALIGN_g       = 0;
size_t H5T_NATIVE_INT_LEAST64_ALIGN_g  = 0;
size_t H5T_NATIVE_UINT_LEAST64_ALIGN_g = 0;
size_t H5T_NATIVE_INT_FAST64_ALIGN_g   = 0;
size_t H5T_NATIVE_UINT_FAST64_ALIGN_g  = 0;

/* Useful floating-point values for conversion routines */
/* (+/- Inf for all floating-point types) */
float  H5T_NATIVE_FLOAT_POS_INF_g  = 0.0F;
float  H5T_NATIVE_FLOAT_NEG_INF_g  = 0.0F;
double H5T_NATIVE_DOUBLE_POS_INF_g = 0.0;
double H5T_NATIVE_DOUBLE_NEG_INF_g = 0.0;

/* Declare the free list for H5T_t's and H5T_shared_t's */
H5FL_DEFINE(H5T_t);
H5FL_DEFINE(H5T_shared_t);

/* Format version bounds for datatype */
const unsigned H5O_dtype_ver_bounds[] = {
    H5O_DTYPE_VERSION_1,     /* H5F_LIBVER_EARLIEST */
    H5O_DTYPE_VERSION_3,     /* H5F_LIBVER_V18 */
    H5O_DTYPE_VERSION_3,     /* H5F_LIBVER_V110 */
    H5O_DTYPE_VERSION_4,     /* H5F_LIBVER_V112 */
    H5O_DTYPE_VERSION_LATEST /* H5F_LIBVER_LATEST */
};

/*******************/
/* Local Variables */
/*******************/

/*
 * The path database. Each path has a source and destination data type pair
 * which is used as the key by which the `entries' array is sorted.
 */
static struct {
    int          npaths; /*number of paths defined               */
    size_t       apaths; /*number of paths allocated             */
    H5T_path_t **path;   /*sorted array of path pointers         */
    int          nsoft;  /*number of soft conversions defined    */
    size_t       asoft;  /*number of soft conversions allocated  */
    H5T_soft_t  *soft;   /*unsorted array of soft conversions    */
} H5T_g;

/* Declare the free list for H5T_path_t's */
H5FL_DEFINE_STATIC(H5T_path_t);

/* Datatype ID class */
static const H5I_class_t H5I_DATATYPE_CLS[1] = {{
    H5I_DATATYPE,             /* ID class value */
    0,                        /* Class flags    */
    8,                        /* # of reserved IDs for class */
    (H5I_free_t)H5T__close_cb /* Callback routine for closing objects of this class */
}};

/*-------------------------------------------------------------------------
 * Function:    H5T__init_inf
 *
 * Purpose:    Initialize the +/- Infinity floating-poing values for type
 *              conversion.
 *
 * Return:    Success:    non-negative
 *            Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__init_inf(void)
{
    H5T_t        *dst_p;               /* Datatype type operate on */
    H5T_atomic_t *dst;                 /* Datatype's atomic info   */
    uint8_t      *d;                   /* Pointer to value to set  */
    size_t        half_size;           /* Half the type size       */
    size_t        u;                   /* Local index value        */
    herr_t        ret_value = SUCCEED; /* Return value             */

    FUNC_ENTER_PACKAGE

    /* Get the float datatype */
    if (NULL == (dst_p = (H5T_t *)H5I_object(H5T_NATIVE_FLOAT_g)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    dst = &dst_p->shared->u.atomic;

    /* Check that we can re-order the bytes correctly */
    if (H5T_ORDER_LE != H5T_native_order_g && H5T_ORDER_BE != H5T_native_order_g)
        HGOTO_ERROR(H5E_DATATYPE, H5E_UNSUPPORTED, FAIL, "unsupported byte order");

    /* +Inf */
    d = (uint8_t *)&H5T_NATIVE_FLOAT_POS_INF_g;
    H5T__bit_set(d, dst->u.f.sign, (size_t)1, false);
    H5T__bit_set(d, dst->u.f.epos, dst->u.f.esize, true);
    H5T__bit_set(d, dst->u.f.mpos, dst->u.f.msize, false);

    /* Swap the bytes if the machine architecture is big-endian */
    if (H5T_ORDER_BE == H5T_native_order_g) {
        half_size = dst_p->shared->size / 2;
        for (u = 0; u < half_size; u++) {
            uint8_t tmp                      = d[dst_p->shared->size - (u + 1)];
            d[dst_p->shared->size - (u + 1)] = d[u];
            d[u]                             = tmp;
        } /* end for */
    }     /* end if */

    /* -Inf */
    d = (uint8_t *)&H5T_NATIVE_FLOAT_NEG_INF_g;
    H5T__bit_set(d, dst->u.f.sign, (size_t)1, true);
    H5T__bit_set(d, dst->u.f.epos, dst->u.f.esize, true);
    H5T__bit_set(d, dst->u.f.mpos, dst->u.f.msize, false);

    /* Swap the bytes if the machine architecture is big-endian */
    if (H5T_ORDER_BE == H5T_native_order_g) {
        half_size = dst_p->shared->size / 2;
        for (u = 0; u < half_size; u++) {
            uint8_t tmp                      = d[dst_p->shared->size - (u + 1)];
            d[dst_p->shared->size - (u + 1)] = d[u];
            d[u]                             = tmp;
        } /* end for */
    }     /* end if */

    /* Get the double datatype */
    if (NULL == (dst_p = (H5T_t *)H5I_object(H5T_NATIVE_DOUBLE_g)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    dst = &dst_p->shared->u.atomic;

    /* Check that we can re-order the bytes correctly */
    if (H5T_ORDER_LE != H5T_native_order_g && H5T_ORDER_BE != H5T_native_order_g)
        HGOTO_ERROR(H5E_DATATYPE, H5E_UNSUPPORTED, FAIL, "unsupported byte order");

    /* +Inf */
    d = (uint8_t *)&H5T_NATIVE_DOUBLE_POS_INF_g;
    H5T__bit_set(d, dst->u.f.sign, (size_t)1, false);
    H5T__bit_set(d, dst->u.f.epos, dst->u.f.esize, true);
    H5T__bit_set(d, dst->u.f.mpos, dst->u.f.msize, false);

    /* Swap the bytes if the machine architecture is big-endian */
    if (H5T_ORDER_BE == H5T_native_order_g) {
        half_size = dst_p->shared->size / 2;
        for (u = 0; u < half_size; u++) {
            uint8_t tmp                      = d[dst_p->shared->size - (u + 1)];
            d[dst_p->shared->size - (u + 1)] = d[u];
            d[u]                             = tmp;
        } /* end for */
    }     /* end if */

    /* -Inf */
    d = (uint8_t *)&H5T_NATIVE_DOUBLE_NEG_INF_g;
    H5T__bit_set(d, dst->u.f.sign, (size_t)1, true);
    H5T__bit_set(d, dst->u.f.epos, dst->u.f.esize, true);
    H5T__bit_set(d, dst->u.f.mpos, dst->u.f.msize, false);

    /* Swap the bytes if the machine architecture is big-endian */
    if (H5T_ORDER_BE == H5T_native_order_g) {
        half_size = dst_p->shared->size / 2;
        for (u = 0; u < half_size; u++) {
            uint8_t tmp                      = d[dst_p->shared->size - (u + 1)];
            d[dst_p->shared->size - (u + 1)] = d[u];
            d[u]                             = tmp;
        } /* end for */
    }     /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__init_inf() */

/*-------------------------------------------------------------------------
 * Function:    H5T_init
 *
 * Purpose:     Initialize the interface from some other layer.
 *
 * Return:      Success:        non-negative
 *              Failure:        negative
 *-------------------------------------------------------------------------
 */
herr_t
H5T_init(void)
{
    H5T_t  *native_schar   = NULL; /* Datatype structure for native signed char */
    H5T_t  *native_uchar   = NULL; /* Datatype structure for native unsigned char */
    H5T_t  *native_short   = NULL; /* Datatype structure for native short */
    H5T_t  *native_ushort  = NULL; /* Datatype structure for native unsigned short */
    H5T_t  *native_int     = NULL; /* Datatype structure for native int */
    H5T_t  *native_uint    = NULL; /* Datatype structure for native unsigned int */
    H5T_t  *native_long    = NULL; /* Datatype structure for native long */
    H5T_t  *native_ulong   = NULL; /* Datatype structure for native unsigned long */
    H5T_t  *native_llong   = NULL; /* Datatype structure for native long long */
    H5T_t  *native_ullong  = NULL; /* Datatype structure for native unsigned long long */
    H5T_t  *native_float   = NULL; /* Datatype structure for native float */
    H5T_t  *native_double  = NULL; /* Datatype structure for native double */
    H5T_t  *native_ldouble = NULL; /* Datatype structure for native long double */
    H5T_t  *std_u8le       = NULL; /* Datatype structure for unsigned 8-bit little-endian integer */
    H5T_t  *std_u8be       = NULL; /* Datatype structure for unsigned 8-bit big-endian integer */
    H5T_t  *std_u16le      = NULL; /* Datatype structure for unsigned 16-bit little-endian integer */
    H5T_t  *std_u16be      = NULL; /* Datatype structure for unsigned 16-bit big-endian integer */
    H5T_t  *std_u32le      = NULL; /* Datatype structure for unsigned 32-bit little-endian integer */
    H5T_t  *std_u32be      = NULL; /* Datatype structure for unsigned 32-bit big-endian integer */
    H5T_t  *std_u64le      = NULL; /* Datatype structure for unsigned 64-bit little-endian integer */
    H5T_t  *std_u64be      = NULL; /* Datatype structure for unsigned 64-bit big-endian integer */
    H5T_t  *dt             = NULL;
    H5T_t  *fixedpt        = NULL; /* Datatype structure for native int */
    H5T_t  *floatpt        = NULL; /* Datatype structure for native float */
    H5T_t  *string         = NULL; /* Datatype structure for C string */
    H5T_t  *bitfield       = NULL; /* Datatype structure for bitfield */
    H5T_t  *compound       = NULL; /* Datatype structure for compound objects */
    H5T_t  *enum_type      = NULL; /* Datatype structure for enum objects */
    H5T_t  *vlen           = NULL; /* Datatype structure for vlen objects */
    H5T_t  *array          = NULL; /* Datatype structure for array objects */
    H5T_t  *objref         = NULL; /* Datatype structure for deprecated reference objects */
    H5T_t  *regref         = NULL; /* Datatype structure for deprecated region references */
    H5T_t  *ref            = NULL; /* Datatype structure for opaque references */
    hsize_t dim[1]         = {1};  /* Dimension info for array datatype */
    herr_t  status;
    bool    copied_dtype =
        true; /* Flag to indicate whether datatype was copied or allocated (for error cleanup) */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Initialize the ID group for the file IDs */
    if (H5I_register_type(H5I_DATATYPE_CLS) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to initialize interface");

    /* Make certain there aren't too many classes of datatypes defined */
    /* Only 16 (numbered 0-15) are supported in the current file format */
    HDcompile_assert(H5T_NCLASSES < 16);

    /* Initialize native floating-point datatypes */
    if (H5T__init_native_float_types() < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to initialize floating-point types");

    /* Initialize all other native types */
    if (H5T__init_native_internal() < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to initialize integers");

    /* Get the atomic datatype structures needed by the initialization code below */
    if (NULL == (native_schar = (H5T_t *)H5I_object(H5T_NATIVE_SCHAR_g)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype object");
    if (NULL == (native_uchar = (H5T_t *)H5I_object(H5T_NATIVE_UCHAR_g)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype object");
    if (NULL == (native_short = (H5T_t *)H5I_object(H5T_NATIVE_SHORT_g)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype object");
    if (NULL == (native_ushort = (H5T_t *)H5I_object(H5T_NATIVE_USHORT_g)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype object");
    if (NULL == (native_int = (H5T_t *)H5I_object(H5T_NATIVE_INT_g)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype object");
    if (NULL == (native_uint = (H5T_t *)H5I_object(H5T_NATIVE_UINT_g)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype object");
    if (NULL == (native_long = (H5T_t *)H5I_object(H5T_NATIVE_LONG_g)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype object");
    if (NULL == (native_ulong = (H5T_t *)H5I_object(H5T_NATIVE_ULONG_g)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype object");
    if (NULL == (native_llong = (H5T_t *)H5I_object(H5T_NATIVE_LLONG_g)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype object");
    if (NULL == (native_ullong = (H5T_t *)H5I_object(H5T_NATIVE_ULLONG_g)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype object");
    if (NULL == (native_float = (H5T_t *)H5I_object(H5T_NATIVE_FLOAT_g)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype object");
    if (NULL == (native_double = (H5T_t *)H5I_object(H5T_NATIVE_DOUBLE_g)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype object");
    if (NULL == (native_ldouble = (H5T_t *)H5I_object(H5T_NATIVE_LDOUBLE_g)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype object");

    /*------------------------------------------------------------
     * Derived native types
     *------------------------------------------------------------
     */

    /* 1-byte bit field */
    H5T_INIT_TYPE(BITFIELD, H5T_NATIVE_B8_g, COPY, native_uint, SET, 1)

    /* 2-byte bit field */
    H5T_INIT_TYPE(BITFIELD, H5T_NATIVE_B16_g, COPY, native_uint, SET, 2)

    /* 4-byte bit field */
    H5T_INIT_TYPE(BITFIELD, H5T_NATIVE_B32_g, COPY, native_uint, SET, 4)

    /* 8-byte bit field */
    H5T_INIT_TYPE(BITFIELD, H5T_NATIVE_B64_g, COPY, native_uint, SET, 8)

    /* haddr_t */
    H5T_INIT_TYPE(OFFSET, H5T_NATIVE_HADDR_g, COPY, native_uint, SET, sizeof(haddr_t))

    /* hsize_t */
    H5T_INIT_TYPE(OFFSET, H5T_NATIVE_HSIZE_g, COPY, native_uint, SET, sizeof(hsize_t))

    /* hssize_t */
    H5T_INIT_TYPE(OFFSET, H5T_NATIVE_HSSIZE_g, COPY, native_int, SET, sizeof(hssize_t))

    /* herr_t */
    H5T_INIT_TYPE(OFFSET, H5T_NATIVE_HERR_g, COPY, native_int, SET, sizeof(herr_t))

    /* bool */
    H5T_INIT_TYPE(OFFSET, H5T_NATIVE_HBOOL_g, COPY, native_uint, SET, sizeof(bool))

    /*------------------------------------------------------------
     * IEEE Types
     *------------------------------------------------------------
     */

    /* IEEE 4-byte little-endian float */
    H5T_INIT_TYPE(FLOATLE, H5T_IEEE_F32LE_g, COPY, native_double, SET, 4)

    /* IEEE 4-byte big-endian float */
    H5T_INIT_TYPE(FLOATBE, H5T_IEEE_F32BE_g, COPY, native_double, SET, 4)

    /* IEEE 8-byte little-endian float */
    H5T_INIT_TYPE(DOUBLELE, H5T_IEEE_F64LE_g, COPY, native_double, SET, 8)

    /* IEEE 8-byte big-endian float */
    H5T_INIT_TYPE(DOUBLEBE, H5T_IEEE_F64BE_g, COPY, native_double, SET, 8)

    /*------------------------------------------------------------
     * VAX Types
     *------------------------------------------------------------
     */

    /* VAX 4-byte float */
    H5T_INIT_TYPE(FLOATVAX, H5T_VAX_F32_g, COPY, native_double, SET, 4)

    /* VAX 8-byte double */
    H5T_INIT_TYPE(DOUBLEVAX, H5T_VAX_F64_g, COPY, native_double, SET, 8)

    /*------------------------------------------------------------
     * C99 types
     *------------------------------------------------------------
     */

    /* 1-byte little-endian (endianness is irrelevant) signed integer */
    H5T_INIT_TYPE(SINTLE, H5T_STD_I8LE_g, COPY, native_int, SET, 1)

    /* 1-byte big-endian (endianness is irrelevant) signed integer */
    H5T_INIT_TYPE(SINTBE, H5T_STD_I8BE_g, COPY, native_int, SET, 1)

    /* 2-byte little-endian signed integer */
    H5T_INIT_TYPE(SINTLE, H5T_STD_I16LE_g, COPY, native_int, SET, 2)

    /* 2-byte big-endian signed integer */
    H5T_INIT_TYPE(SINTBE, H5T_STD_I16BE_g, COPY, native_int, SET, 2)

    /* 4-byte little-endian signed integer */
    H5T_INIT_TYPE(SINTLE, H5T_STD_I32LE_g, COPY, native_int, SET, 4)

    /* 4-byte big-endian signed integer */
    H5T_INIT_TYPE(SINTBE, H5T_STD_I32BE_g, COPY, native_int, SET, 4)

    /* 8-byte little-endian signed integer */
    H5T_INIT_TYPE(SINTLE, H5T_STD_I64LE_g, COPY, native_int, SET, 8)

    /* 8-byte big-endian signed integer */
    H5T_INIT_TYPE(SINTBE, H5T_STD_I64BE_g, COPY, native_int, SET, 8)

    /* 1-byte little-endian (endianness is irrelevant) unsigned integer */
    H5T_INIT_TYPE(UINTLE, H5T_STD_U8LE_g, COPY, native_uint, SET, 1)
    std_u8le = dt; /* Keep type for later */

    /* 1-byte big-endian (endianness is irrelevant) unsigned integer */
    H5T_INIT_TYPE(UINTBE, H5T_STD_U8BE_g, COPY, native_uint, SET, 1)
    std_u8be = dt; /* Keep type for later */

    /* 2-byte little-endian unsigned integer */
    H5T_INIT_TYPE(UINTLE, H5T_STD_U16LE_g, COPY, native_uint, SET, 2)
    std_u16le = dt; /* Keep type for later */

    /* 2-byte big-endian unsigned integer */
    H5T_INIT_TYPE(UINTBE, H5T_STD_U16BE_g, COPY, native_uint, SET, 2)
    std_u16be = dt; /* Keep type for later */

    /* 4-byte little-endian unsigned integer */
    H5T_INIT_TYPE(UINTLE, H5T_STD_U32LE_g, COPY, native_uint, SET, 4)
    std_u32le = dt; /* Keep type for later */

    /* 4-byte big-endian unsigned integer */
    H5T_INIT_TYPE(UINTBE, H5T_STD_U32BE_g, COPY, native_uint, SET, 4)
    std_u32be = dt; /* Keep type for later */

    /* 8-byte little-endian unsigned integer */
    H5T_INIT_TYPE(UINTLE, H5T_STD_U64LE_g, COPY, native_uint, SET, 8)
    std_u64le = dt; /* Keep type for later */

    /* 8-byte big-endian unsigned integer */
    H5T_INIT_TYPE(UINTBE, H5T_STD_U64BE_g, COPY, native_uint, SET, 8)
    std_u64be = dt; /* Keep type for later */

    /*------------------------------------------------------------
     * Native, Little- & Big-endian bitfields
     *------------------------------------------------------------
     */

    /* little-endian (order is irrelevant) 8-bit bitfield */
    H5T_INIT_TYPE(BITFIELDLE, H5T_STD_B8LE_g, COPY, std_u8le, NOSET, -)
    bitfield = dt; /* Keep type for later */

    /* big-endian (order is irrelevant) 8-bit bitfield */
    H5T_INIT_TYPE(BITFIELDBE, H5T_STD_B8BE_g, COPY, std_u8be, NOSET, -)

    /* Little-endian 16-bit bitfield */
    H5T_INIT_TYPE(BITFIELDLE, H5T_STD_B16LE_g, COPY, std_u16le, NOSET, -)

    /* Big-endian 16-bit bitfield */
    H5T_INIT_TYPE(BITFIELDBE, H5T_STD_B16BE_g, COPY, std_u16be, NOSET, -)

    /* Little-endian 32-bit bitfield */
    H5T_INIT_TYPE(BITFIELDLE, H5T_STD_B32LE_g, COPY, std_u32le, NOSET, -)

    /* Big-endian 32-bit bitfield */
    H5T_INIT_TYPE(BITFIELDBE, H5T_STD_B32BE_g, COPY, std_u32be, NOSET, -)

    /* Little-endian 64-bit bitfield */
    H5T_INIT_TYPE(BITFIELDLE, H5T_STD_B64LE_g, COPY, std_u64le, NOSET, -)

    /* Big-endian 64-bit bitfield */
    H5T_INIT_TYPE(BITFIELDBE, H5T_STD_B64BE_g, COPY, std_u64be, NOSET, -)

    /*------------------------------------------------------------
     * The Unix architecture for dates and times.
     *------------------------------------------------------------
     */

    /* Little-endian 32-bit UNIX time_t */
    H5T_INIT_TYPE(TIME, H5T_UNIX_D32LE_g, COPY, std_u32le, NOSET, -)

    /* Big-endian 32-bit UNIX time_t */
    H5T_INIT_TYPE(TIME, H5T_UNIX_D32BE_g, COPY, std_u32be, NOSET, -)

    /* Little-endian 64-bit UNIX time_t */
    H5T_INIT_TYPE(TIME, H5T_UNIX_D64LE_g, COPY, std_u64le, NOSET, -)

    /* Big-endian 64-bit UNIX time_t */
    H5T_INIT_TYPE(TIME, H5T_UNIX_D64BE_g, COPY, std_u64be, NOSET, -)

    /* Indicate that the types that are created from here down are allocated
     * H5FL_ALLOC(), not copied with H5T_copy()
     */
    copied_dtype = false;

    /* Opaque data */
    H5T_INIT_TYPE(OPAQ, H5T_NATIVE_OPAQUE_g, ALLOC, -, SET, 1)

    /*------------------------------------------------------------
     * The `C' architecture
     *------------------------------------------------------------
     */

    /* One-byte character string */
    H5T_INIT_TYPE(CSTRING, H5T_C_S1_g, ALLOC, -, SET, 1)
    string = dt; /* Keep type for later */

    /*------------------------------------------------------------
     * The `Fortran' architecture
     *------------------------------------------------------------
     */

    /* One-byte character string */
    H5T_INIT_TYPE(FORSTRING, H5T_FORTRAN_S1_g, ALLOC, -, SET, 1)

    /*------------------------------------------------------------
     * Reference types
     *------------------------------------------------------------
     */

    /* Deprecated object reference type */
    H5T_INIT_TYPE(OBJREF, H5T_STD_REF_OBJ_g, ALLOC, -, NOSET, -)
    if (H5T_set_loc(dt, NULL, H5T_LOC_MEMORY) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "invalid datatype location");
    objref = dt; /* Keep type for later */

    /* Deprecated region reference type */
    H5T_INIT_TYPE(REGREF, H5T_STD_REF_DSETREG_g, ALLOC, -, NOSET, -)
    if (H5T_set_loc(dt, NULL, H5T_LOC_MEMORY) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "invalid datatype location");
    regref = dt; /* Keep type for later */

    /* Opaque reference type */
    H5T_INIT_TYPE(REF, H5T_STD_REF_g, ALLOC, -, NOSET, -)
    if (H5T_set_loc(dt, NULL, H5T_LOC_MEMORY) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "invalid datatype location");
    ref = dt; /* Keep type for later */

    /*
     * Register conversion functions beginning with the most general and
     * ending with the most specific.
     */
    fixedpt = native_int;
    floatpt = native_float;
    if (NULL == (compound = H5T__create(H5T_COMPOUND, (size_t)1)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    if (NULL == (enum_type = H5T__create(H5T_ENUM, (size_t)1)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    if (NULL == (vlen = H5T__vlen_create(native_int)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    if (NULL == (array = H5T__array_create(native_int, 1, dim)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    status = 0;

    status |= H5T__register_int(H5T_PERS_SOFT, "i_i", fixedpt, fixedpt, H5T__conv_i_i);
    status |= H5T__register_int(H5T_PERS_SOFT, "i_f", fixedpt, floatpt, H5T__conv_i_f);
    status |= H5T__register_int(H5T_PERS_SOFT, "f_f", floatpt, floatpt, H5T__conv_f_f);
    status |= H5T__register_int(H5T_PERS_SOFT, "f_i", floatpt, fixedpt, H5T__conv_f_i);
    status |= H5T__register_int(H5T_PERS_SOFT, "s_s", string, string, H5T__conv_s_s);
    status |= H5T__register_int(H5T_PERS_SOFT, "b_b", bitfield, bitfield, H5T__conv_b_b);
    status |= H5T__register_int(H5T_PERS_SOFT, "ibo", fixedpt, fixedpt, H5T__conv_order);
    status |= H5T__register_int(H5T_PERS_SOFT, "ibo(opt)", fixedpt, fixedpt, H5T__conv_order_opt);
    status |= H5T__register_int(H5T_PERS_SOFT, "fbo", floatpt, floatpt, H5T__conv_order);
    status |= H5T__register_int(H5T_PERS_SOFT, "fbo(opt)", floatpt, floatpt, H5T__conv_order_opt);
    status |= H5T__register_int(H5T_PERS_SOFT, "struct(no-opt)", compound, compound, H5T__conv_struct);
    status |= H5T__register_int(H5T_PERS_SOFT, "struct(opt)", compound, compound, H5T__conv_struct_opt);
    status |= H5T__register_int(H5T_PERS_SOFT, "enum", enum_type, enum_type, H5T__conv_enum);
    status |= H5T__register_int(H5T_PERS_SOFT, "enum_i", enum_type, fixedpt, H5T__conv_enum_numeric);
    status |= H5T__register_int(H5T_PERS_SOFT, "enum_f", enum_type, floatpt, H5T__conv_enum_numeric);
    status |= H5T__register_int(H5T_PERS_SOFT, "vlen", vlen, vlen, H5T__conv_vlen);
    status |= H5T__register_int(H5T_PERS_SOFT, "array", array, array, H5T__conv_array);
    status |= H5T__register_int(H5T_PERS_SOFT, "objref", objref, objref, H5T__conv_noop);
    status |= H5T__register_int(H5T_PERS_SOFT, "regref", regref, regref, H5T__conv_noop);
    status |= H5T__register_int(H5T_PERS_SOFT, "ref", ref, ref, H5T__conv_ref);
    status |= H5T__register_int(H5T_PERS_SOFT, "objref_ref", objref, ref, H5T__conv_ref);
    status |= H5T__register_int(H5T_PERS_SOFT, "regref_ref", regref, ref, H5T__conv_ref);

    /*
     * Native conversions should be listed last since we can use hardware to
     * perform the conversion.  We list the odd types like `llong', `long',
     * and `short' before the usual types like `int' and `char' so that when
     * diagnostics are printed we favor the usual names over the odd names
     * when two or more types are the same size.
     */

    /* floating point */
    status |=
        H5T__register_int(H5T_PERS_HARD, "flt_dbl", native_float, native_double, H5T__conv_float_double);
    status |=
        H5T__register_int(H5T_PERS_HARD, "dbl_flt", native_double, native_float, H5T__conv_double_float);
    status |=
        H5T__register_int(H5T_PERS_HARD, "flt_ldbl", native_float, native_ldouble, H5T__conv_float_ldouble);
    status |=
        H5T__register_int(H5T_PERS_HARD, "dbl_ldbl", native_double, native_ldouble, H5T__conv_double_ldouble);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ldbl_flt", native_ldouble, native_float, H5T__conv_ldouble_float);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ldbl_dbl", native_ldouble, native_double, H5T__conv_ldouble_double);

    /* from long long */
    status |=
        H5T__register_int(H5T_PERS_HARD, "llong_ullong", native_llong, native_ullong, H5T__conv_llong_ullong);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ullong_llong", native_ullong, native_llong, H5T__conv_ullong_llong);
    status |= H5T__register_int(H5T_PERS_HARD, "llong_long", native_llong, native_long, H5T__conv_llong_long);
    status |=
        H5T__register_int(H5T_PERS_HARD, "llong_ulong", native_llong, native_ulong, H5T__conv_llong_ulong);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ullong_long", native_ullong, native_long, H5T__conv_ullong_long);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ullong_ulong", native_ullong, native_ulong, H5T__conv_ullong_ulong);
    status |=
        H5T__register_int(H5T_PERS_HARD, "llong_short", native_llong, native_short, H5T__conv_llong_short);
    status |=
        H5T__register_int(H5T_PERS_HARD, "llong_ushort", native_llong, native_ushort, H5T__conv_llong_ushort);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ullong_short", native_ullong, native_short, H5T__conv_ullong_short);
    status |= H5T__register_int(H5T_PERS_HARD, "ullong_ushort", native_ullong, native_ushort,
                                H5T__conv_ullong_ushort);
    status |= H5T__register_int(H5T_PERS_HARD, "llong_int", native_llong, native_int, H5T__conv_llong_int);
    status |= H5T__register_int(H5T_PERS_HARD, "llong_uint", native_llong, native_uint, H5T__conv_llong_uint);
    status |= H5T__register_int(H5T_PERS_HARD, "ullong_int", native_ullong, native_int, H5T__conv_ullong_int);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ullong_uint", native_ullong, native_uint, H5T__conv_ullong_uint);
    status |=
        H5T__register_int(H5T_PERS_HARD, "llong_schar", native_llong, native_schar, H5T__conv_llong_schar);
    status |=
        H5T__register_int(H5T_PERS_HARD, "llong_uchar", native_llong, native_uchar, H5T__conv_llong_uchar);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ullong_schar", native_ullong, native_schar, H5T__conv_ullong_schar);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ullong_uchar", native_ullong, native_uchar, H5T__conv_ullong_uchar);

    /* From long */
    status |= H5T__register_int(H5T_PERS_HARD, "long_llong", native_long, native_llong, H5T__conv_long_llong);
    status |=
        H5T__register_int(H5T_PERS_HARD, "long_ullong", native_long, native_ullong, H5T__conv_long_ullong);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ulong_llong", native_ulong, native_llong, H5T__conv_ulong_llong);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ulong_ullong", native_ulong, native_ullong, H5T__conv_ulong_ullong);
    status |= H5T__register_int(H5T_PERS_HARD, "long_ulong", native_long, native_ulong, H5T__conv_long_ulong);
    status |= H5T__register_int(H5T_PERS_HARD, "ulong_long", native_ulong, native_long, H5T__conv_ulong_long);
    status |= H5T__register_int(H5T_PERS_HARD, "long_short", native_long, native_short, H5T__conv_long_short);
    status |=
        H5T__register_int(H5T_PERS_HARD, "long_ushort", native_long, native_ushort, H5T__conv_long_ushort);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ulong_short", native_ulong, native_short, H5T__conv_ulong_short);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ulong_ushort", native_ulong, native_ushort, H5T__conv_ulong_ushort);
    status |= H5T__register_int(H5T_PERS_HARD, "long_int", native_long, native_int, H5T__conv_long_int);
    status |= H5T__register_int(H5T_PERS_HARD, "long_uint", native_long, native_uint, H5T__conv_long_uint);
    status |= H5T__register_int(H5T_PERS_HARD, "ulong_int", native_ulong, native_int, H5T__conv_ulong_int);
    status |= H5T__register_int(H5T_PERS_HARD, "ulong_uint", native_ulong, native_uint, H5T__conv_ulong_uint);
    status |= H5T__register_int(H5T_PERS_HARD, "long_schar", native_long, native_schar, H5T__conv_long_schar);
    status |= H5T__register_int(H5T_PERS_HARD, "long_uchar", native_long, native_uchar, H5T__conv_long_uchar);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ulong_schar", native_ulong, native_schar, H5T__conv_ulong_schar);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ulong_uchar", native_ulong, native_uchar, H5T__conv_ulong_uchar);

    /* From short */
    status |=
        H5T__register_int(H5T_PERS_HARD, "short_llong", native_short, native_llong, H5T__conv_short_llong);
    status |=
        H5T__register_int(H5T_PERS_HARD, "short_ullong", native_short, native_ullong, H5T__conv_short_ullong);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ushort_llong", native_ushort, native_llong, H5T__conv_ushort_llong);
    status |= H5T__register_int(H5T_PERS_HARD, "ushort_ullong", native_ushort, native_ullong,
                                H5T__conv_ushort_ullong);
    status |= H5T__register_int(H5T_PERS_HARD, "short_long", native_short, native_long, H5T__conv_short_long);
    status |=
        H5T__register_int(H5T_PERS_HARD, "short_ulong", native_short, native_ulong, H5T__conv_short_ulong);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ushort_long", native_ushort, native_long, H5T__conv_ushort_long);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ushort_ulong", native_ushort, native_ulong, H5T__conv_ushort_ulong);
    status |=
        H5T__register_int(H5T_PERS_HARD, "short_ushort", native_short, native_ushort, H5T__conv_short_ushort);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ushort_short", native_ushort, native_short, H5T__conv_ushort_short);
    status |= H5T__register_int(H5T_PERS_HARD, "short_int", native_short, native_int, H5T__conv_short_int);
    status |= H5T__register_int(H5T_PERS_HARD, "short_uint", native_short, native_uint, H5T__conv_short_uint);
    status |= H5T__register_int(H5T_PERS_HARD, "ushort_int", native_ushort, native_int, H5T__conv_ushort_int);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ushort_uint", native_ushort, native_uint, H5T__conv_ushort_uint);
    status |=
        H5T__register_int(H5T_PERS_HARD, "short_schar", native_short, native_schar, H5T__conv_short_schar);
    status |=
        H5T__register_int(H5T_PERS_HARD, "short_uchar", native_short, native_uchar, H5T__conv_short_uchar);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ushort_schar", native_ushort, native_schar, H5T__conv_ushort_schar);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ushort_uchar", native_ushort, native_uchar, H5T__conv_ushort_uchar);

    /* From int */
    status |= H5T__register_int(H5T_PERS_HARD, "int_llong", native_int, native_llong, H5T__conv_int_llong);
    status |= H5T__register_int(H5T_PERS_HARD, "int_ullong", native_int, native_ullong, H5T__conv_int_ullong);
    status |= H5T__register_int(H5T_PERS_HARD, "uint_llong", native_uint, native_llong, H5T__conv_uint_llong);
    status |=
        H5T__register_int(H5T_PERS_HARD, "uint_ullong", native_uint, native_ullong, H5T__conv_uint_ullong);
    status |= H5T__register_int(H5T_PERS_HARD, "int_long", native_int, native_long, H5T__conv_int_long);
    status |= H5T__register_int(H5T_PERS_HARD, "int_ulong", native_int, native_ulong, H5T__conv_int_ulong);
    status |= H5T__register_int(H5T_PERS_HARD, "uint_long", native_uint, native_long, H5T__conv_uint_long);
    status |= H5T__register_int(H5T_PERS_HARD, "uint_ulong", native_uint, native_ulong, H5T__conv_uint_ulong);
    status |= H5T__register_int(H5T_PERS_HARD, "int_short", native_int, native_short, H5T__conv_int_short);
    status |= H5T__register_int(H5T_PERS_HARD, "int_ushort", native_int, native_ushort, H5T__conv_int_ushort);
    status |= H5T__register_int(H5T_PERS_HARD, "uint_short", native_uint, native_short, H5T__conv_uint_short);
    status |=
        H5T__register_int(H5T_PERS_HARD, "uint_ushort", native_uint, native_ushort, H5T__conv_uint_ushort);
    status |= H5T__register_int(H5T_PERS_HARD, "int_uint", native_int, native_uint, H5T__conv_int_uint);
    status |= H5T__register_int(H5T_PERS_HARD, "uint_int", native_uint, native_int, H5T__conv_uint_int);
    status |= H5T__register_int(H5T_PERS_HARD, "int_schar", native_int, native_schar, H5T__conv_int_schar);
    status |= H5T__register_int(H5T_PERS_HARD, "int_uchar", native_int, native_uchar, H5T__conv_int_uchar);
    status |= H5T__register_int(H5T_PERS_HARD, "uint_schar", native_uint, native_schar, H5T__conv_uint_schar);
    status |= H5T__register_int(H5T_PERS_HARD, "uint_uchar", native_uint, native_uchar, H5T__conv_uint_uchar);

    /* From char */
    status |=
        H5T__register_int(H5T_PERS_HARD, "schar_llong", native_schar, native_llong, H5T__conv_schar_llong);
    status |=
        H5T__register_int(H5T_PERS_HARD, "schar_ullong", native_schar, native_ullong, H5T__conv_schar_ullong);
    status |=
        H5T__register_int(H5T_PERS_HARD, "uchar_llong", native_uchar, native_llong, H5T__conv_uchar_llong);
    status |=
        H5T__register_int(H5T_PERS_HARD, "uchar_ullong", native_uchar, native_ullong, H5T__conv_uchar_ullong);
    status |= H5T__register_int(H5T_PERS_HARD, "schar_long", native_schar, native_long, H5T__conv_schar_long);
    status |=
        H5T__register_int(H5T_PERS_HARD, "schar_ulong", native_schar, native_ulong, H5T__conv_schar_ulong);
    status |= H5T__register_int(H5T_PERS_HARD, "uchar_long", native_uchar, native_long, H5T__conv_uchar_long);
    status |=
        H5T__register_int(H5T_PERS_HARD, "uchar_ulong", native_uchar, native_ulong, H5T__conv_uchar_ulong);
    status |=
        H5T__register_int(H5T_PERS_HARD, "schar_short", native_schar, native_short, H5T__conv_schar_short);
    status |=
        H5T__register_int(H5T_PERS_HARD, "schar_ushort", native_schar, native_ushort, H5T__conv_schar_ushort);
    status |=
        H5T__register_int(H5T_PERS_HARD, "uchar_short", native_uchar, native_short, H5T__conv_uchar_short);
    status |=
        H5T__register_int(H5T_PERS_HARD, "uchar_ushort", native_uchar, native_ushort, H5T__conv_uchar_ushort);
    status |= H5T__register_int(H5T_PERS_HARD, "schar_int", native_schar, native_int, H5T__conv_schar_int);
    status |= H5T__register_int(H5T_PERS_HARD, "schar_uint", native_schar, native_uint, H5T__conv_schar_uint);
    status |= H5T__register_int(H5T_PERS_HARD, "uchar_int", native_uchar, native_int, H5T__conv_uchar_int);
    status |= H5T__register_int(H5T_PERS_HARD, "uchar_uint", native_uchar, native_uint, H5T__conv_uchar_uint);
    status |=
        H5T__register_int(H5T_PERS_HARD, "schar_uchar", native_schar, native_uchar, H5T__conv_schar_uchar);
    status |=
        H5T__register_int(H5T_PERS_HARD, "uchar_schar", native_uchar, native_schar, H5T__conv_uchar_schar);

    /* From char to floats */
    status |=
        H5T__register_int(H5T_PERS_HARD, "schar_flt", native_schar, native_float, H5T__conv_schar_float);
    status |=
        H5T__register_int(H5T_PERS_HARD, "schar_dbl", native_schar, native_double, H5T__conv_schar_double);
    status |=
        H5T__register_int(H5T_PERS_HARD, "schar_ldbl", native_schar, native_ldouble, H5T__conv_schar_ldouble);

    /* From unsigned char to floats */
    status |=
        H5T__register_int(H5T_PERS_HARD, "uchar_flt", native_uchar, native_float, H5T__conv_uchar_float);
    status |=
        H5T__register_int(H5T_PERS_HARD, "uchar_dbl", native_uchar, native_double, H5T__conv_uchar_double);
    status |=
        H5T__register_int(H5T_PERS_HARD, "uchar_ldbl", native_uchar, native_ldouble, H5T__conv_uchar_ldouble);

    /* From short to floats */
    status |=
        H5T__register_int(H5T_PERS_HARD, "short_flt", native_short, native_float, H5T__conv_short_float);
    status |=
        H5T__register_int(H5T_PERS_HARD, "short_dbl", native_short, native_double, H5T__conv_short_double);
    status |=
        H5T__register_int(H5T_PERS_HARD, "short_ldbl", native_short, native_ldouble, H5T__conv_short_ldouble);

    /* From unsigned short to floats */
    status |=
        H5T__register_int(H5T_PERS_HARD, "ushort_flt", native_ushort, native_float, H5T__conv_ushort_float);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ushort_dbl", native_ushort, native_double, H5T__conv_ushort_double);
    status |= H5T__register_int(H5T_PERS_HARD, "ushort_ldbl", native_ushort, native_ldouble,
                                H5T__conv_ushort_ldouble);

    /* From int to floats */
    status |= H5T__register_int(H5T_PERS_HARD, "int_flt", native_int, native_float, H5T__conv_int_float);
    status |= H5T__register_int(H5T_PERS_HARD, "int_dbl", native_int, native_double, H5T__conv_int_double);
    status |= H5T__register_int(H5T_PERS_HARD, "int_ldbl", native_int, native_ldouble, H5T__conv_int_ldouble);

    /* From unsigned int to floats */
    status |= H5T__register_int(H5T_PERS_HARD, "uint_flt", native_uint, native_float, H5T__conv_uint_float);
    status |= H5T__register_int(H5T_PERS_HARD, "uint_dbl", native_uint, native_double, H5T__conv_uint_double);
    status |=
        H5T__register_int(H5T_PERS_HARD, "uint_ldbl", native_uint, native_ldouble, H5T__conv_uint_ldouble);

    /* From long to floats */
    status |= H5T__register_int(H5T_PERS_HARD, "long_flt", native_long, native_float, H5T__conv_long_float);
    status |= H5T__register_int(H5T_PERS_HARD, "long_dbl", native_long, native_double, H5T__conv_long_double);
    status |=
        H5T__register_int(H5T_PERS_HARD, "long_ldbl", native_long, native_ldouble, H5T__conv_long_ldouble);

    /* From unsigned long to floats */
    status |=
        H5T__register_int(H5T_PERS_HARD, "ulong_flt", native_ulong, native_float, H5T__conv_ulong_float);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ulong_dbl", native_ulong, native_double, H5T__conv_ulong_double);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ulong_ldbl", native_ulong, native_ldouble, H5T__conv_ulong_ldouble);

    /* From long long to floats */
    status |=
        H5T__register_int(H5T_PERS_HARD, "llong_flt", native_llong, native_float, H5T__conv_llong_float);
    status |=
        H5T__register_int(H5T_PERS_HARD, "llong_dbl", native_llong, native_double, H5T__conv_llong_double);
#ifdef H5T_CONV_INTERNAL_LLONG_LDOUBLE
    status |=
        H5T__register_int(H5T_PERS_HARD, "llong_ldbl", native_llong, native_ldouble, H5T__conv_llong_ldouble);
#endif /* H5T_CONV_INTERNAL_LLONG_LDOUBLE */

    /* From unsigned long long to floats */
    status |=
        H5T__register_int(H5T_PERS_HARD, "ullong_flt", native_ullong, native_float, H5T__conv_ullong_float);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ullong_dbl", native_ullong, native_double, H5T__conv_ullong_double);
#ifdef H5T_CONV_INTERNAL_ULLONG_LDOUBLE
    status |= H5T__register_int(H5T_PERS_HARD, "ullong_ldbl", native_ullong, native_ldouble,
                                H5T__conv_ullong_ldouble);
#endif /* H5T_CONV_INTERNAL_ULLONG_LDOUBLE */

    /* From floats to char */
    status |=
        H5T__register_int(H5T_PERS_HARD, "flt_schar", native_float, native_schar, H5T__conv_float_schar);
    status |=
        H5T__register_int(H5T_PERS_HARD, "dbl_schar", native_double, native_schar, H5T__conv_double_schar);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ldbl_schar", native_ldouble, native_schar, H5T__conv_ldouble_schar);

    /* From floats to unsigned char */
    status |=
        H5T__register_int(H5T_PERS_HARD, "flt_uchar", native_float, native_uchar, H5T__conv_float_uchar);
    status |=
        H5T__register_int(H5T_PERS_HARD, "dbl_uchar", native_double, native_uchar, H5T__conv_double_uchar);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ldbl_uchar", native_ldouble, native_uchar, H5T__conv_ldouble_uchar);

    /* From floats to short */
    status |=
        H5T__register_int(H5T_PERS_HARD, "flt_short", native_float, native_short, H5T__conv_float_short);
    status |=
        H5T__register_int(H5T_PERS_HARD, "dbl_short", native_double, native_short, H5T__conv_double_short);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ldbl_short", native_ldouble, native_short, H5T__conv_ldouble_short);

    /* From floats to unsigned short */
    status |=
        H5T__register_int(H5T_PERS_HARD, "flt_ushort", native_float, native_ushort, H5T__conv_float_ushort);
    status |=
        H5T__register_int(H5T_PERS_HARD, "dbl_ushort", native_double, native_ushort, H5T__conv_double_ushort);
    status |= H5T__register_int(H5T_PERS_HARD, "ldbl_ushort", native_ldouble, native_ushort,
                                H5T__conv_ldouble_ushort);

    /* From floats to int */
    status |= H5T__register_int(H5T_PERS_HARD, "flt_int", native_float, native_int, H5T__conv_float_int);
    status |= H5T__register_int(H5T_PERS_HARD, "dbl_int", native_double, native_int, H5T__conv_double_int);
    status |= H5T__register_int(H5T_PERS_HARD, "ldbl_int", native_ldouble, native_int, H5T__conv_ldouble_int);

    /* From floats to unsigned int */
    status |= H5T__register_int(H5T_PERS_HARD, "flt_uint", native_float, native_uint, H5T__conv_float_uint);
    status |= H5T__register_int(H5T_PERS_HARD, "dbl_uint", native_double, native_uint, H5T__conv_double_uint);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ldbl_uint", native_ldouble, native_uint, H5T__conv_ldouble_uint);

    /* From floats to long */
    status |= H5T__register_int(H5T_PERS_HARD, "flt_long", native_float, native_long, H5T__conv_float_long);
    status |= H5T__register_int(H5T_PERS_HARD, "dbl_long", native_double, native_long, H5T__conv_double_long);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ldbl_long", native_ldouble, native_long, H5T__conv_ldouble_long);

    /* From floats to unsigned long */
    status |=
        H5T__register_int(H5T_PERS_HARD, "flt_ulong", native_float, native_ulong, H5T__conv_float_ulong);
    status |=
        H5T__register_int(H5T_PERS_HARD, "dbl_ulong", native_double, native_ulong, H5T__conv_double_ulong);
    status |=
        H5T__register_int(H5T_PERS_HARD, "ldbl_ulong", native_ldouble, native_ulong, H5T__conv_ldouble_ulong);

    /* From floats to long long */
    status |=
        H5T__register_int(H5T_PERS_HARD, "flt_llong", native_float, native_llong, H5T__conv_float_llong);
    status |=
        H5T__register_int(H5T_PERS_HARD, "dbl_llong", native_double, native_llong, H5T__conv_double_llong);
#ifdef H5T_CONV_INTERNAL_LDOUBLE_LLONG
    status |=
        H5T__register_int(H5T_PERS_HARD, "ldbl_llong", native_ldouble, native_llong, H5T__conv_ldouble_llong);
#endif /* H5T_CONV_INTERNAL_LDOUBLE_LLONG */

    /* From floats to unsigned long long */
    status |=
        H5T__register_int(H5T_PERS_HARD, "flt_ullong", native_float, native_ullong, H5T__conv_float_ullong);
    status |=
        H5T__register_int(H5T_PERS_HARD, "dbl_ullong", native_double, native_ullong, H5T__conv_double_ullong);
#if H5T_CONV_INTERNAL_LDOUBLE_ULLONG
    status |= H5T__register_int(H5T_PERS_HARD, "ldbl_ullong", native_ldouble, native_ullong,
                                H5T__conv_ldouble_ullong);
#endif /* H5T_CONV_INTERNAL_LDOUBLE_ULLONG */

    /*
     * The special no-op conversion is the fastest, so we list it last. The
     * data types we use are not important as long as the source and
     * destination are equal.
     */
    status |= H5T__register_int(H5T_PERS_HARD, "no-op", native_int, native_int, H5T__conv_noop);

    /* Initialize the +/- Infinity values for floating-point types */
    status |= H5T__init_inf();

    if (status < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to register conversion function(s)");

    /* Register datatype creation property class properties here.  See similar
     * code in H5D_init(), etc. for example.
     */

    /* Only register the default property list if it hasn't been created yet */
    if (H5P_LST_DATATYPE_CREATE_ID_g == (-1)) {
        /* ========== Datatype Creation Property Class Initialization ============*/
        assert(H5P_CLS_DATATYPE_CREATE_g != NULL);

        /* Register the default datatype creation property list */
        if ((H5P_LST_DATATYPE_CREATE_ID_g = H5P_create_id(H5P_CLS_DATATYPE_CREATE_g, false)) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTREGISTER, FAIL, "can't insert property into class");
    } /* end if */

done:
    /* General cleanup */
    if (compound != NULL)
        (void)H5T_close_real(compound);
    if (enum_type != NULL)
        (void)H5T_close_real(enum_type);
    if (vlen != NULL)
        (void)H5T_close_real(vlen);
    if (array != NULL)
        (void)H5T_close_real(array);

    /* Error cleanup */
    if (ret_value < 0) {
        if (dt) {
            /* Check if we should call H5T_close_real or H5FL_FREE */
            if (copied_dtype)
                (void)H5T_close_real(dt);
            else {
                if (dt->shared->owned_vol_obj && H5VL_free_object(dt->shared->owned_vol_obj) < 0)
                    HDONE_ERROR(H5E_DATATYPE, H5E_CANTCLOSEOBJ, FAIL, "unable to close owned VOL object");
                dt->shared = H5FL_FREE(H5T_shared_t, dt->shared);
                dt         = H5FL_FREE(H5T_t, dt);
            } /* end else */
        }     /* end if */
    }         /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_init() */

/*-------------------------------------------------------------------------
 * Function:   H5T__unlock_cb
 *
 * Purpose:    Clear the immutable flag for a datatype.  This function is
 *             called when the library is closing in order to unlock all
 *             registered datatypes and thus make them free-able.
 *
 * Return:     Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static int
H5T__unlock_cb(void *_dt, hid_t H5_ATTR_UNUSED id, void *_udata)
{
    H5T_t *dt = (H5T_t *)_dt;
    int   *n  = (int *)_udata;

    FUNC_ENTER_PACKAGE_NOERR

    assert(dt);
    assert(dt->shared);

    if (H5T_STATE_IMMUTABLE == dt->shared->state) {
        dt->shared->state = H5T_STATE_RDONLY;
        (*n)++;
    } /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5T__unlock_cb() */

/*-------------------------------------------------------------------------
 * Function:   H5T_top_term_package
 *
 * Purpose:    Close the "top" of the interface, releasing IDs, etc.
 *
 * Return:     Success:    Positive if any action might have caused a
 *                            change in some other interface; zero otherwise.
 *             Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
int
H5T_top_term_package(void)
{
    int n = 0;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Unregister all conversion functions */
    if (H5T_g.path) {
        int i, nprint = 0;

        for (i = 0; i < H5T_g.npaths; i++) {
            H5T_path_t *path;

            path = H5T_g.path[i];
            assert(path);
            if (path->conv.u.app_func) {
                H5T__print_stats(path, &nprint /*in,out*/);
                path->cdata.command = H5T_CONV_FREE;
                if (path->conv.is_app) {
                    if ((path->conv.u.app_func)((hid_t)FAIL, (hid_t)FAIL, &(path->cdata), (size_t)0,
                                                (size_t)0, (size_t)0, NULL, NULL, H5CX_get_dxpl()) < 0) {
#ifdef H5T_DEBUG
                        if (H5DEBUG(T)) {
                            fprintf(H5DEBUG(T),
                                    "H5T: conversion function "
                                    "0x%016zx failed to free private data for "
                                    "%s (ignored)\n",
                                    (size_t)path->conv.u.app_func, path->name);
                        } /* end if */
#endif
                        H5E_clear_stack(NULL); /*ignore the error*/
                    }                          /* end if */
                }                              /* end if */
                else {
                    if ((path->conv.u.lib_func)((hid_t)FAIL, (hid_t)FAIL, &(path->cdata), (size_t)0,
                                                (size_t)0, (size_t)0, NULL, NULL) < 0) {
#ifdef H5T_DEBUG
                        if (H5DEBUG(T)) {
                            fprintf(H5DEBUG(T),
                                    "H5T: conversion function "
                                    "0x%016zx failed to free private data for "
                                    "%s (ignored)\n",
                                    (size_t)path->conv.u.lib_func, path->name);
                        } /* end if */
#endif
                        H5E_clear_stack(NULL); /*ignore the error*/
                    }                          /* end if */
                }                              /* end else */
            }                                  /* end if */

            if (path->src)
                (void)H5T_close_real(path->src);
            if (path->dst)
                (void)H5T_close_real(path->dst);
            path          = H5FL_FREE(H5T_path_t, path);
            H5T_g.path[i] = NULL;
        } /* end for */

        /* Clear conversion tables */
        H5T_g.path   = (H5T_path_t **)H5MM_xfree(H5T_g.path);
        H5T_g.npaths = 0;
        H5T_g.apaths = 0;
        H5T_g.soft   = (H5T_soft_t *)H5MM_xfree(H5T_g.soft);
        H5T_g.nsoft  = 0;
        H5T_g.asoft  = 0;

        n++;
    } /* end if */

    /* Unlock all datatypes, then free them */
    /* note that we are ignoring the return value from H5I_iterate() */
    /* Also note that we are incrementing 'n' in the callback */
    H5I_iterate(H5I_DATATYPE, H5T__unlock_cb, &n, false);

    /* Release all datatype IDs */
    if (H5I_nmembers(H5I_DATATYPE) > 0) {
        (void)H5I_clear_type(H5I_DATATYPE, false, false);
        n++; /*H5I*/
    }        /* end if */

    /* Reset all the datatype IDs */
    if (H5T_IEEE_F32BE_g > 0) {
        H5T_IEEE_F32BE_g = FAIL;
        H5T_IEEE_F32LE_g = FAIL;
        H5T_IEEE_F64BE_g = FAIL;
        H5T_IEEE_F64LE_g = FAIL;

        H5T_STD_I8BE_g        = FAIL;
        H5T_STD_I8LE_g        = FAIL;
        H5T_STD_I16BE_g       = FAIL;
        H5T_STD_I16LE_g       = FAIL;
        H5T_STD_I32BE_g       = FAIL;
        H5T_STD_I32LE_g       = FAIL;
        H5T_STD_I64BE_g       = FAIL;
        H5T_STD_I64LE_g       = FAIL;
        H5T_STD_U8BE_g        = FAIL;
        H5T_STD_U8LE_g        = FAIL;
        H5T_STD_U16BE_g       = FAIL;
        H5T_STD_U16LE_g       = FAIL;
        H5T_STD_U32BE_g       = FAIL;
        H5T_STD_U32LE_g       = FAIL;
        H5T_STD_U64BE_g       = FAIL;
        H5T_STD_U64LE_g       = FAIL;
        H5T_STD_B8BE_g        = FAIL;
        H5T_STD_B8LE_g        = FAIL;
        H5T_STD_B16BE_g       = FAIL;
        H5T_STD_B16LE_g       = FAIL;
        H5T_STD_B32BE_g       = FAIL;
        H5T_STD_B32LE_g       = FAIL;
        H5T_STD_B64BE_g       = FAIL;
        H5T_STD_B64LE_g       = FAIL;
        H5T_STD_REF_OBJ_g     = FAIL;
        H5T_STD_REF_DSETREG_g = FAIL;
        H5T_STD_REF_g         = FAIL;

        H5T_UNIX_D32BE_g = FAIL;
        H5T_UNIX_D32LE_g = FAIL;
        H5T_UNIX_D64BE_g = FAIL;
        H5T_UNIX_D64LE_g = FAIL;

        H5T_C_S1_g = FAIL;

        H5T_FORTRAN_S1_g = FAIL;

        H5T_NATIVE_SCHAR_g   = FAIL;
        H5T_NATIVE_UCHAR_g   = FAIL;
        H5T_NATIVE_SHORT_g   = FAIL;
        H5T_NATIVE_USHORT_g  = FAIL;
        H5T_NATIVE_INT_g     = FAIL;
        H5T_NATIVE_UINT_g    = FAIL;
        H5T_NATIVE_LONG_g    = FAIL;
        H5T_NATIVE_ULONG_g   = FAIL;
        H5T_NATIVE_LLONG_g   = FAIL;
        H5T_NATIVE_ULLONG_g  = FAIL;
        H5T_NATIVE_FLOAT_g   = FAIL;
        H5T_NATIVE_DOUBLE_g  = FAIL;
        H5T_NATIVE_LDOUBLE_g = FAIL;
        H5T_NATIVE_B8_g      = FAIL;
        H5T_NATIVE_B16_g     = FAIL;
        H5T_NATIVE_B32_g     = FAIL;
        H5T_NATIVE_B64_g     = FAIL;
        H5T_NATIVE_OPAQUE_g  = FAIL;
        H5T_NATIVE_HADDR_g   = FAIL;
        H5T_NATIVE_HSIZE_g   = FAIL;
        H5T_NATIVE_HSSIZE_g  = FAIL;
        H5T_NATIVE_HERR_g    = FAIL;
        H5T_NATIVE_HBOOL_g   = FAIL;

        H5T_NATIVE_INT8_g        = FAIL;
        H5T_NATIVE_UINT8_g       = FAIL;
        H5T_NATIVE_INT_LEAST8_g  = FAIL;
        H5T_NATIVE_UINT_LEAST8_g = FAIL;
        H5T_NATIVE_INT_FAST8_g   = FAIL;
        H5T_NATIVE_UINT_FAST8_g  = FAIL;

        H5T_NATIVE_INT16_g        = FAIL;
        H5T_NATIVE_UINT16_g       = FAIL;
        H5T_NATIVE_INT_LEAST16_g  = FAIL;
        H5T_NATIVE_UINT_LEAST16_g = FAIL;
        H5T_NATIVE_INT_FAST16_g   = FAIL;
        H5T_NATIVE_UINT_FAST16_g  = FAIL;

        H5T_NATIVE_INT32_g        = FAIL;
        H5T_NATIVE_UINT32_g       = FAIL;
        H5T_NATIVE_INT_LEAST32_g  = FAIL;
        H5T_NATIVE_UINT_LEAST32_g = FAIL;
        H5T_NATIVE_INT_FAST32_g   = FAIL;
        H5T_NATIVE_UINT_FAST32_g  = FAIL;

        H5T_NATIVE_INT64_g        = FAIL;
        H5T_NATIVE_UINT64_g       = FAIL;
        H5T_NATIVE_INT_LEAST64_g  = FAIL;
        H5T_NATIVE_UINT_LEAST64_g = FAIL;
        H5T_NATIVE_INT_FAST64_g   = FAIL;
        H5T_NATIVE_UINT_FAST64_g  = FAIL;

        n++;
    } /* end if */

    FUNC_LEAVE_NOAPI(n)
} /* end H5T_top_term_package() */

/*-------------------------------------------------------------------------
 * Function:   H5T_term_package
 *
 * Purpose:    Close this interface.
 *
 * Note:    Finishes shutting down the interface, after
 *          H5T_top_term_package() is called
 *
 * Return:   Success:    Positive if any action might have caused a
 *                change in some other interface; zero
 *                otherwise.
 *           Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
int
H5T_term_package(void)
{
    int n = 0;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    assert(0 == H5I_nmembers(H5I_DATATYPE));

    /* Destroy the datatype object id group */
    n += (H5I_dec_type_ref(H5I_DATATYPE) > 0);

    FUNC_LEAVE_NOAPI(n)
} /* end H5T_term_package() */

/*-------------------------------------------------------------------------
 * Function:    H5T__close_cb
 *
 * Purpose:     Called when the ref count reaches zero on the datatype's ID
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__close_cb(H5T_t *dt, void **request)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(dt);
    assert(dt->shared);

    /* If this datatype is VOL-managed (i.e.: has a VOL object),
     * close it through the VOL connector.
     */
    if (NULL != dt->vol_obj) {
        /* Close the connector-managed datatype data */
        if (H5VL_datatype_close(dt->vol_obj, H5P_DATASET_XFER_DEFAULT, request) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "unable to close datatype");

        /* Free the VOL object */
        if (H5VL_free_object(dt->vol_obj) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTDEC, FAIL, "unable to free VOL object");
        dt->vol_obj = NULL;
    } /* end if */

    /* Close the datatype */
    if (H5T_close(dt) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "unable to close datatype");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__close_cb() */

/*-------------------------------------------------------------------------
 * Function:   H5Tcreate
 *
 * Purpose:    Create a new type and initialize it to reasonable values.
 *             The type is a member of type class TYPE and is SIZE bytes.
 *
 * Return:    Success:    A new type identifier.
 *
 *            Failure:    Negative
 *
 * Errors:
 *        ARGS      BADVALUE    Invalid size.
 *        DATATYPE  CANTINIT    Can't create type.
 *        DATATYPE  CANTREGISTER    Can't register datatype ID.
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Tcreate(H5T_class_t type, size_t size)
{
    H5T_t *dt = NULL; /* New datatype constructed */
    hid_t  ret_value; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("i", "Ttz", type, size);

    /* check args. We support string (fixed-size or variable-length) now. */
    if (size <= 0 && size != H5T_VARIABLE)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "size must be positive");

    /* create the type */
    if (NULL == (dt = H5T__create(type, size)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to create type");

    /* Get an ID for the datatype */
    if ((ret_value = H5I_register(H5I_DATATYPE, dt, true)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREGISTER, FAIL, "unable to register datatype ID");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tcreate() */

/*-------------------------------------------------------------------------
 * Function:    H5Tcopy
 *
 * Purpose:     Copies a datatype.  The resulting datatype is not locked.
 *              The datatype should be closed when no longer needed by
 *              calling H5Tclose().
 *
 * Return:      Success:    The ID of a new datatype
 *
 *              Failure:    H5I_INVALID_HID
 *
 * Note:        The returned type is always transient and unlocked. If the
 *              TYPE_ID argument is a dataset instead of a datatype then
 *              this function returns a transient, modifiable datatype
 *              which is a copy of the dataset's datatype.
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Tcopy(hid_t obj_id)
{
    H5T_t *dt        = NULL;            /* Pointer to the datatype to copy */
    H5T_t *new_dt    = NULL;            /* Pointer to the new datatype */
    hid_t  dset_tid  = H5I_INVALID_HID; /* Datatype ID from dataset */
    hid_t  ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE1("i", "i", obj_id);

    switch (H5I_get_type(obj_id)) {
        case H5I_DATATYPE:
            /* The argument is a datatype handle */
            if (NULL == (dt = (H5T_t *)H5I_object(obj_id)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "obj_id is not a datatype ID");
            break;

        case H5I_DATASET: {
            H5VL_object_t          *vol_obj;     /* Object for obj_id */
            H5VL_dataset_get_args_t vol_cb_args; /* Arguments to VOL callback */

            /* The argument is a dataset handle */
            if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(obj_id, H5I_DATASET)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "type_id is not a dataset ID");

            /* Set up VOL callback arguments */
            vol_cb_args.op_type               = H5VL_DATASET_GET_TYPE;
            vol_cb_args.args.get_type.type_id = H5I_INVALID_HID;

            /* Get the datatype from the dataset
             * NOTE: This will have to be closed after we're done with it.
             */
            if (H5VL_dataset_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, H5I_INVALID_HID,
                            "unable to get datatype from the dataset");
            dset_tid = vol_cb_args.args.get_type.type_id;

            /* Unwrap the type ID */
            if (NULL == (dt = (H5T_t *)H5I_object(dset_tid)))
                HGOTO_ERROR(H5E_DATATYPE, H5E_BADTYPE, H5I_INVALID_HID,
                            "received invalid datatype from the dataset");
        } break;

        case H5I_UNINIT:
        case H5I_BADID:
        case H5I_FILE:
        case H5I_GROUP:
        case H5I_DATASPACE:
        case H5I_ATTR:
        case H5I_MAP:
        case H5I_VFL:
        case H5I_VOL:
        case H5I_GENPROP_CLS:
        case H5I_GENPROP_LST:
        case H5I_ERROR_CLASS:
        case H5I_ERROR_MSG:
        case H5I_ERROR_STACK:
        case H5I_SPACE_SEL_ITER:
        case H5I_EVENTSET:
        case H5I_NTYPES:
        default:
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not a datatype or dataset");
    } /* end switch */

    /* Copy datatype */
    if (NULL == (new_dt = H5T_copy(dt, H5T_COPY_TRANSIENT)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, H5I_INVALID_HID, "unable to copy");

    /* Get an ID for the copied datatype */
    if ((ret_value = H5I_register(H5I_DATATYPE, new_dt, true)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register datatype atom");

done:

    /* If we got a type ID from a passed-in dataset, we need to close that */
    if (dset_tid != H5I_INVALID_HID)
        if (H5I_dec_app_ref(dset_tid) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_BADID, FAIL, "problem freeing temporary dataset type ID");

    /* Close the new datatype on errors */
    if (H5I_INVALID_HID == ret_value)
        if (new_dt && H5T_close_real(new_dt) < 0)
            HDONE_ERROR(H5E_DATATYPE, H5E_CANTRELEASE, H5I_INVALID_HID, "unable to release datatype info");

    FUNC_LEAVE_API(ret_value)
} /* end H5Tcopy() */

/*-------------------------------------------------------------------------
 * Function:    H5Tclose
 *
 * Purpose:     Frees a datatype and all associated memory.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tclose(hid_t type_id)
{
    H5T_t *dt;                  /* Pointer to datatype to close */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", type_id);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    if (H5T_STATE_IMMUTABLE == dt->shared->state)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "immutable datatype");

    /* When the reference count reaches zero the resources are freed */
    if (H5I_dec_app_ref(type_id) < 0)
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "problem freeing id");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tclose() */

/*-------------------------------------------------------------------------
 * Function:    H5Tclose_async
 *
 * Purpose:     Asynchronous version of H5Tclose.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tclose_async(const char *app_file, const char *app_func, unsigned app_line, hid_t type_id, hid_t es_id)
{
    H5T_t         *dt;                          /* Pointer to datatype to close */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    H5VL_object_t *vol_obj   = NULL;            /* VOL object of dset_id */
    H5VL_t        *connector = NULL;            /* VOL connector */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "*s*sIuii", app_file, app_func, app_line, type_id, es_id);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    if (H5T_STATE_IMMUTABLE == dt->shared->state)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "immutable datatype");

    /* Get dataset object's connector */
    if (NULL == (vol_obj = H5VL_vol_object(type_id)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "can't get VOL object for dataset");

    /* Prepare for possible asynchronous operation */
    if (H5ES_NONE != es_id) {
        /* Increase connector's refcount, so it doesn't get closed if closing
         * the dataset closes the file */
        connector = vol_obj->connector;
        H5VL_conn_inc_rc(connector);

        /* Point at token for operation to set up */
        token_ptr = &token;
    } /* end if */

    /* When the reference count reaches zero the resources are freed */
    if (H5I_dec_app_ref_async(type_id, token_ptr) < 0)
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "problem freeing id");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE5(__func__, "*s*sIuii", app_file, app_func, app_line, type_id, es_id)) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    if (connector && H5VL_conn_dec_rc(connector) < 0)
        HDONE_ERROR(H5E_DATATYPE, H5E_CANTDEC, FAIL, "can't decrement ref count on connector");

    FUNC_LEAVE_API(ret_value)
} /* end H5Tclose_async() */

/*-------------------------------------------------------------------------
 * Function:  H5Tequal
 *
 * Purpose:   Determines if two datatypes are equal.
 *
 * Return:    Success:    true if equal, false if unequal
 *
 *            Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5Tequal(hid_t type1_id, hid_t type2_id)
{
    const H5T_t *dt1;       /* Pointer to first datatype */
    const H5T_t *dt2;       /* Pointer to second datatype */
    htri_t       ret_value; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("t", "ii", type1_id, type2_id);

    /* check args */
    if (NULL == (dt1 = (H5T_t *)H5I_object_verify(type1_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    if (NULL == (dt2 = (H5T_t *)H5I_object_verify(type2_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");

    ret_value = (0 == H5T_cmp(dt1, dt2, false)) ? true : false;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tequal() */

/*-------------------------------------------------------------------------
 * Function:    H5Tlock
 *
 * Purpose:    Locks a type, making it read only and non-destructable.
 *        This is normally done by the library for predefined datatypes so
 *        the application doesn't inadvertently change or delete a
 *        predefined type.
 *
 *        Once a datatype is locked it can never be unlocked unless
 *        the entire library is closed.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tlock(hid_t type_id)
{
    H5T_t *dt;                  /* Datatype to operate on */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", type_id);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    if (H5T_STATE_NAMED == dt->shared->state || H5T_STATE_OPEN == dt->shared->state)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to lock named datatype");

    if (H5T_lock(dt, true) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to lock transient datatype");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tlock() */

/*-------------------------------------------------------------------------
 * Function:  H5Tget_class
 *
 * Purpose:   Returns the datatype class identifier for datatype TYPE_ID.
 *
 * Return:    Success:    One of the non-negative datatype class constants.
 *
 *            Failure:    H5T_NO_CLASS (Negative)
 *
 *-------------------------------------------------------------------------
 */
H5T_class_t
H5Tget_class(hid_t type_id)
{
    H5T_t      *dt;        /* Pointer to datatype */
    H5T_class_t ret_value; /* Return value */

    FUNC_ENTER_API(H5T_NO_CLASS)
    H5TRACE1("Tt", "i", type_id);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5T_NO_CLASS, "not a datatype");

    /* Set return value */
    ret_value = H5T_get_class(dt, false);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tget_class() */

/*-------------------------------------------------------------------------
 * Function:  H5T_get_class
 *
 * Purpose:   Returns the data type class identifier for a datatype ptr.
 *
 * Return:    Success:    One of the non-negative data type class constants.
 *
 *            Failure:    H5T_NO_CLASS (Negative)
 *
 *-------------------------------------------------------------------------
 */
H5T_class_t
H5T_get_class(const H5T_t *dt, htri_t internal)
{
    H5T_class_t ret_value = H5T_NO_CLASS; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    assert(dt);

    /* Externally, a VL string is a string; internally, a VL string is a VL. */
    if (internal) {
        ret_value = dt->shared->type;
    }
    else {
        if (H5T_IS_VL_STRING(dt->shared))
            ret_value = H5T_STRING;
        else
            ret_value = dt->shared->type;
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_get_class() */

/*-------------------------------------------------------------------------
 * Function:   H5Tdetect_class
 *
 * Purpose:    Check whether a datatype contains (or is) a certain type of
 *             datatype.
 *
 * Return:     true (1) or false (0) on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5Tdetect_class(hid_t type, H5T_class_t cls)
{
    H5T_t *dt;        /* Datatype to query */
    htri_t ret_value; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("t", "iTt", type, cls);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5T_NO_CLASS, "not a datatype");
    if (!(cls > H5T_NO_CLASS && cls < H5T_NCLASSES))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5T_NO_CLASS, "not a datatype class");

    /* Set return value */
    if ((ret_value = H5T_detect_class(dt, cls, true)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, H5T_NO_CLASS, "can't get datatype class");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tdetect_class() */

/*-------------------------------------------------------------------------
 * Function:   H5T_detect_class
 *
 * Purpose:    Check whether a datatype contains (or is) a certain type of
 *             datatype.
 *
 * Return:     true (1) or false (0) on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5T_detect_class(const H5T_t *dt, H5T_class_t cls, bool from_api)
{
    unsigned i;
    htri_t   ret_value = false; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    assert(dt);
    assert(cls > H5T_NO_CLASS && cls < H5T_NCLASSES);

    /* Consider VL string as a string for API, as a VL for internal use. */
    /* (note that this check must be performed before checking if the VL
     *  string belongs to the H5T_VLEN class, which would otherwise return
     *  true. -QAK)
     */
    if (from_api && H5T_IS_VL_STRING(dt->shared))
        HGOTO_DONE(H5T_STRING == cls);

    /* Check if this type is the correct type */
    if (dt->shared->type == cls)
        HGOTO_DONE(true);

    /* check for types that might have the correct type as a component */
    switch (dt->shared->type) {
        case H5T_COMPOUND:
            for (i = 0; i < dt->shared->u.compnd.nmembs; i++) {
                htri_t nested_ret; /* Return value from nested call */

                /* Check if this field's type is the correct type */
                if (dt->shared->u.compnd.memb[i].type->shared->type == cls)
                    HGOTO_DONE(true);

                /* Recurse if it's VL, compound, enum or array */
                if (H5T_IS_COMPLEX(dt->shared->u.compnd.memb[i].type->shared->type))
                    if ((nested_ret = H5T_detect_class(dt->shared->u.compnd.memb[i].type, cls, from_api)) !=
                        false)
                        HGOTO_DONE(nested_ret);
            } /* end for */
            break;

        case H5T_ARRAY:
        case H5T_VLEN:
        case H5T_ENUM:
            HGOTO_DONE(H5T_detect_class(dt->shared->parent, cls, from_api));
            break;
        case H5T_NO_CLASS:
        case H5T_INTEGER:
        case H5T_FLOAT:
        case H5T_TIME:
        case H5T_STRING:
        case H5T_BITFIELD:
        case H5T_OPAQUE:
        case H5T_REFERENCE:
        case H5T_NCLASSES:
        default:
            break;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_detect_class() */

/*-------------------------------------------------------------------------
 * Function:  H5Tis_variable_str
 *
 * Purpose:   Check whether a datatype is a variable-length string
 *
 * Return:    true (1) or false (0) on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5Tis_variable_str(hid_t dtype_id)
{
    H5T_t *dt;        /* Datatype to query */
    htri_t ret_value; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("t", "i", dtype_id);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(dtype_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");

    /* Set return value */
    if ((ret_value = H5T_is_variable_str(dt)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_UNSUPPORTED, FAIL, "can't determine if datatype is VL-string");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tis_variable_str() */

/*-------------------------------------------------------------------------
 * Function:  H5T_is_variable_str
 *
 * Purpose:   Check whether a datatype is a variable-length string
 *
 * Return:    true (1) or false (0) on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5T_is_variable_str(const H5T_t *dt)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    FUNC_LEAVE_NOAPI(H5T_IS_VL_STRING(dt->shared))
} /* end H5T_is_variable_str() */

/*-------------------------------------------------------------------------
 * Function:  H5Tget_size
 *
 * Purpose:   Determines the total size of a datatype in bytes.
 *
 * Return:    Success:    Size of the datatype in bytes.     The size of
 *                datatype is the size of an instance of that
 *                datatype.
 *
 *            Failure:    0 (valid datatypes are never zero size)
 *
 *-------------------------------------------------------------------------
 */
size_t
H5Tget_size(hid_t type_id)
{
    H5T_t *dt;        /* Datatype to query */
    size_t ret_value; /* Return value */

    FUNC_ENTER_API(0)
    H5TRACE1("z", "i", type_id);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, 0, "not a datatype");

    /* size */
    ret_value = H5T_GET_SIZE(dt);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tget_size() */

/*-------------------------------------------------------------------------
 * Function:    H5Tset_size
 *
 * Purpose:    Sets the total size in bytes for a datatype (this operation
 *        is not permitted on reference datatypes).  If the size is
 *        decreased so that the significant bits of the datatype
 *        extend beyond the edge of the new size, then the `offset'
 *        property is decreased toward zero.  If the `offset' becomes
 *        zero and the significant bits of the datatype still hang
 *        over the edge of the new size, then the number of significant
 *        bits is decreased.
 *
 *        Adjusting the size of an H5T_STRING automatically sets the
 *        precision to 8*size.
 *
 *        All datatypes have a positive size.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tset_size(hid_t type_id, size_t size)
{
    H5T_t *dt;                  /* Datatype to modify */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iz", type_id, size);

    /* Check args */
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    if (H5T_STATE_TRANSIENT != dt->shared->state)
        HGOTO_ERROR(H5E_ARGS, H5E_CANTINIT, FAIL, "datatype is read-only");
    if (size <= 0 && size != H5T_VARIABLE)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "size must be positive");
    if (size == H5T_VARIABLE && !H5T_IS_STRING(dt->shared))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "only strings may be variable length");
    if (H5T_ENUM == dt->shared->type && dt->shared->u.enumer.nmembs > 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "operation not allowed after members are defined");
    if (H5T_REFERENCE == dt->shared->type)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "operation not defined for this datatype");

    /* Modify the datatype */
    if (H5T__set_size(dt, size) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to set size for datatype");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tset_size() */

/*-------------------------------------------------------------------------
 * Function:  H5Tget_super
 *
 * Purpose:   Returns the type from which TYPE is derived. In the case of
 *            an enumeration type the return value is an integer type.
 *
 * Return:    Success:    Type ID for base datatype.
 *
 *            Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Tget_super(hid_t type)
{
    H5T_t *dt;                          /* Datatype to query */
    H5T_t *super     = NULL;            /* Supertype */
    hid_t  ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE1("i", "i", type);

    if (NULL == (dt = (H5T_t *)H5I_object_verify(type, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not a datatype");
    if (NULL == (super = H5T_get_super(dt)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, H5I_INVALID_HID, "not a datatype");
    if ((ret_value = H5I_register(H5I_DATATYPE, super, true)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register parent datatype");

done:
    if (H5I_INVALID_HID == ret_value)
        if (super && H5T_close_real(super) < 0)
            HDONE_ERROR(H5E_DATATYPE, H5E_CANTRELEASE, H5I_INVALID_HID,
                        "unable to release super datatype info")

    FUNC_LEAVE_API(ret_value)
} /* end H5Tget_super() */

/*-------------------------------------------------------------------------
 * Function:  H5T_get_super
 *
 * Purpose:   Private function for H5Tget_super.  Returns the type from
 *            which TYPE is derived. In the case of an enumeration type
 *            the return value is an integer type.
 *
 * Return:    Success:    Data type for base data type.
 *            Failure:        NULL
 *
 *-------------------------------------------------------------------------
 */
H5T_t *
H5T_get_super(const H5T_t *dt)
{
    H5T_t *ret_value = NULL;

    FUNC_ENTER_NOAPI(NULL)

    assert(dt);

    if (!dt->shared->parent)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "not a derived data type");
    if (NULL == (ret_value = H5T_copy(dt->shared->parent, H5T_COPY_ALL)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "unable to copy parent data type");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_get_super() */

/*-------------------------------------------------------------------------
 * Function:    H5T__register_int
 *
 * Purpose:    Register a library internal datatype conversion routine.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__register_int(H5T_pers_t pers, const char *name, H5T_t *src, H5T_t *dst, H5T_lib_conv_t func)
{
    H5T_conv_func_t conv_func;           /* Conversion function wrapper */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(H5T_PERS_HARD == pers || H5T_PERS_SOFT == pers);
    assert(name && *name);
    assert(src);
    assert(dst);
    assert(func);

    /* Set up conversion function wrapper */
    conv_func.is_app     = false;
    conv_func.u.lib_func = func;

    /* Register conversion */
    if (H5T__register(pers, name, src, dst, &conv_func) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL,
                    "unable to register internal datatype conversion routine");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__register_int() */

/*-------------------------------------------------------------------------
 * Function:    H5T__register
 *
 * Purpose:    Register a hard or soft conversion function for a data type
 *        conversion path.  The path is specified by the source and
 *        destination data types SRC_ID and DST_ID (for soft functions
 *        only the class of these types is important). If FUNC is a
 *        hard function then it replaces any previous path; if it's a
 *        soft function then it replaces all existing paths to which it
 *        applies and is used for any new path to which it applies as
 *        long as that path doesn't have a hard function.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__register(H5T_pers_t pers, const char *name, H5T_t *src, H5T_t *dst, H5T_conv_func_t *conv)
{
    hid_t       tmp_sid = -1, tmp_did = -1; /*temporary data type IDs    */
    H5T_path_t *old_path = NULL;            /*existing conversion path   */
    H5T_path_t *new_path = NULL;            /*new conversion path        */
    H5T_cdata_t cdata;                      /*temporary conversion data  */
    int         nprint = 0;                 /*number of paths shut down  */
    int         i;                          /*counter                    */
    herr_t      ret_value = SUCCEED;        /*return value               */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(src);
    assert(dst);
    assert(conv);
    assert(H5T_PERS_HARD == pers || H5T_PERS_SOFT == pers);
    assert(name && *name);

    if (H5T_PERS_HARD == pers) {
        /* Only bother to register the path if it's not a no-op path (for this machine) */
        if (H5T_cmp(src, dst, false)) {
            /* Locate or create a new conversion path */
            if (NULL == (new_path = H5T__path_find_real(src, dst, name, conv)))
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to locate/allocate conversion path");

            /* Notify all other functions to recalculate private data since some
             * functions might cache a list of conversion functions.  For
             * instance, the compound type converter caches a list of conversion
             * functions for the members, so adding a new function should cause
             * the list to be recalculated to use the new function.
             */
            for (i = 0; i < H5T_g.npaths; i++)
                if (new_path != H5T_g.path[i])
                    H5T_g.path[i]->cdata.recalc = true;
        } /* end if */
    }     /* end if */
    else {
        /* Add function to end of soft list */
        if ((size_t)H5T_g.nsoft >= H5T_g.asoft) {
            size_t      na = MAX(32, 2 * H5T_g.asoft);
            H5T_soft_t *x;

            if (NULL == (x = (H5T_soft_t *)H5MM_realloc(H5T_g.soft, na * sizeof(H5T_soft_t))))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");
            H5T_g.asoft = na;
            H5T_g.soft  = x;
        } /* end if */
        strncpy(H5T_g.soft[H5T_g.nsoft].name, name, (size_t)H5T_NAMELEN);
        H5T_g.soft[H5T_g.nsoft].name[H5T_NAMELEN - 1] = '\0';
        H5T_g.soft[H5T_g.nsoft].src                   = src->shared->type;
        H5T_g.soft[H5T_g.nsoft].dst                   = dst->shared->type;
        H5T_g.soft[H5T_g.nsoft].conv                  = *conv;
        H5T_g.nsoft++;

        /*
         * Any existing path (except the no-op path) to which this new soft
         * conversion function applies should be replaced by a new path that
         * uses this function.
         */
        for (i = 1; i < H5T_g.npaths; i++) {
            old_path = H5T_g.path[i];
            assert(old_path);

            /* Does the new soft conversion function apply to this path? */
            if (old_path->is_hard || old_path->src->shared->type != src->shared->type ||
                old_path->dst->shared->type != dst->shared->type)
                continue;

            if ((tmp_sid = H5I_register(H5I_DATATYPE, H5T_copy(old_path->src, H5T_COPY_ALL), false)) < 0 ||
                (tmp_did = H5I_register(H5I_DATATYPE, H5T_copy(old_path->dst, H5T_COPY_ALL), false)) < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREGISTER, FAIL,
                            "unable to register data types for conv query");
            memset(&cdata, 0, sizeof cdata);
            cdata.command = H5T_CONV_INIT;
            if (conv->is_app) {
                if ((conv->u.app_func)(tmp_sid, tmp_did, &cdata, (size_t)0, (size_t)0, (size_t)0, NULL, NULL,
                                       H5CX_get_dxpl()) < 0) {
                    H5I_dec_ref(tmp_sid);
                    H5I_dec_ref(tmp_did);
                    tmp_sid = tmp_did = -1;
                    H5E_clear_stack(NULL);
                    continue;
                } /* end if */
            }     /* end if */
            else if ((conv->u.lib_func)(tmp_sid, tmp_did, &cdata, (size_t)0, (size_t)0, (size_t)0, NULL,
                                        NULL) < 0) {
                H5I_dec_ref(tmp_sid);
                H5I_dec_ref(tmp_did);
                tmp_sid = tmp_did = -1;
                H5E_clear_stack(NULL);
                continue;
            } /* end if */

            /* Create a new conversion path */
            if (NULL == (new_path = H5FL_CALLOC(H5T_path_t)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");
            strncpy(new_path->name, name, (size_t)H5T_NAMELEN);
            new_path->name[H5T_NAMELEN - 1] = '\0';
            if (NULL == (new_path->src = H5T_copy(old_path->src, H5T_COPY_ALL)) ||
                NULL == (new_path->dst = H5T_copy(old_path->dst, H5T_COPY_ALL)))
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to copy data types");
            new_path->conv    = *conv;
            new_path->is_hard = false;
            new_path->cdata   = cdata;

            /* Replace previous path */
            H5T_g.path[i] = new_path;
            new_path      = NULL; /*so we don't free it on error*/

            /* Free old path */
            H5T__print_stats(old_path, &nprint);
            old_path->cdata.command = H5T_CONV_FREE;
            if (old_path->conv.is_app) {
                if ((old_path->conv.u.app_func)(tmp_sid, tmp_did, &(old_path->cdata), (size_t)0, (size_t)0,
                                                (size_t)0, NULL, NULL, H5CX_get_dxpl()) < 0) {
#ifdef H5T_DEBUG
                    if (H5DEBUG(T))
                        fprintf(H5DEBUG(T),
                                "H5T: conversion function 0x%016zx "
                                "failed to free private data for %s (ignored)\n",
                                (size_t)old_path->conv.u.app_func, old_path->name);
#endif
                } /* end if */
            }     /* end if */
            else if ((old_path->conv.u.lib_func)(tmp_sid, tmp_did, &(old_path->cdata), (size_t)0, (size_t)0,
                                                 (size_t)0, NULL, NULL) < 0) {
#ifdef H5T_DEBUG
                if (H5DEBUG(T))
                    fprintf(H5DEBUG(T),
                            "H5T: conversion function 0x%016zx "
                            "failed to free private data for %s (ignored)\n",
                            (size_t)old_path->conv.u.lib_func, old_path->name);
#endif
            } /* end if */
            (void)H5T_close_real(old_path->src);
            (void)H5T_close_real(old_path->dst);
            old_path = H5FL_FREE(H5T_path_t, old_path);

            /* Release temporary atoms */
            H5I_dec_ref(tmp_sid);
            H5I_dec_ref(tmp_did);
            tmp_sid = tmp_did = -1;

            /* We don't care about any failures during the freeing process */
            H5E_clear_stack(NULL);
        } /* end for */
    }     /* end else */

done:
    if (ret_value < 0) {
        if (new_path) {
            if (new_path->src)
                (void)H5T_close_real(new_path->src);
            if (new_path->dst)
                (void)H5T_close_real(new_path->dst);
            new_path = H5FL_FREE(H5T_path_t, new_path);
        } /* end if */
        if (tmp_sid >= 0)
            H5I_dec_ref(tmp_sid);
        if (tmp_did >= 0)
            H5I_dec_ref(tmp_did);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__register() */

/*-------------------------------------------------------------------------
 * Function:    H5Tregister
 *
 * Purpose:    Register a hard or soft conversion function for a data type
 *        conversion path.  The path is specified by the source and
 *        destination data types SRC_ID and DST_ID (for soft functions
 *        only the class of these types is important). If FUNC is a
 *        hard function then it replaces any previous path; if it's a
 *        soft function then it replaces all existing paths to which it
 *        applies and is used for any new path to which it applies as
 *        long as that path doesn't have a hard function.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tregister(H5T_pers_t pers, const char *name, hid_t src_id, hid_t dst_id, H5T_conv_t func)
{
    H5T_t          *src;                 /*source data type descriptor    */
    H5T_t          *dst;                 /*destination data type desc     */
    H5T_conv_func_t conv_func;           /* Conversion function wrapper */
    herr_t          ret_value = SUCCEED; /*return value                   */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "Te*siiTC", pers, name, src_id, dst_id, func);

    /* Check args */
    if (H5T_PERS_HARD != pers && H5T_PERS_SOFT != pers)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid function persistence");
    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "conversion must have a name for debugging");
    if (NULL == (src = (H5T_t *)H5I_object_verify(src_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a data type");
    if (NULL == (dst = (H5T_t *)H5I_object_verify(dst_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a data type");
    if (!func)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no conversion function specified");

    /* Set up conversion function wrapper */
    conv_func.is_app     = true;
    conv_func.u.app_func = func;

    /* Go register the function */
    if (H5T__register(pers, name, src, dst, &conv_func) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't register conversion function");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tregister() */

/*-------------------------------------------------------------------------
 * Function:   H5T__unregister
 *
 * Purpose:    Removes conversion paths that match the specified criteria.
 *        All arguments are optional. Missing arguments are wild cards.
 *        The special no-op path cannot be removed.
 *
 * Return:    Success:    non-negative
 *            Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__unregister(H5T_pers_t pers, const char *name, H5T_t *src, H5T_t *dst, H5T_conv_t func)
{
    H5T_path_t *path   = NULL; /*conversion path             */
    H5T_soft_t *soft   = NULL; /*soft conversion information */
    int         nprint = 0;    /*number of paths shut down   */
    int         i;             /*counter                     */

    FUNC_ENTER_PACKAGE_NOERR

    /* Remove matching entries from the soft list */
    if (H5T_PERS_DONTCARE == pers || H5T_PERS_SOFT == pers) {
        for (i = H5T_g.nsoft - 1; i >= 0; --i) {
            soft = H5T_g.soft + i;
            assert(soft);
            if (name && *name && strcmp(name, soft->name) != 0)
                continue;
            if (src && src->shared->type != soft->src)
                continue;
            if (dst && dst->shared->type != soft->dst)
                continue;
            if (func && func != soft->conv.u.app_func)
                continue;

            memmove(H5T_g.soft + i, H5T_g.soft + i + 1, (size_t)(H5T_g.nsoft - (i + 1)) * sizeof(H5T_soft_t));
            --H5T_g.nsoft;
        } /* end for */
    }     /* end if */

    /* Remove matching conversion paths, except no-op path */
    for (i = H5T_g.npaths - 1; i > 0; --i) {
        path = H5T_g.path[i];
        assert(path);

        /* Not a match */
        if (((H5T_PERS_SOFT == pers && path->is_hard) || (H5T_PERS_HARD == pers && !path->is_hard)) ||
            (name && *name && strcmp(name, path->name) != 0) || (src && H5T_cmp(src, path->src, false)) ||
            (dst && H5T_cmp(dst, path->dst, false)) || (func && func != path->conv.u.app_func)) {
            /*
             * Notify all other functions to recalculate private data since some
             * functions might cache a list of conversion functions.  For
             * instance, the compound type converter caches a list of conversion
             * functions for the members, so removing a function should cause
             * the list to be recalculated to avoid the removed function.
             */
            path->cdata.recalc = true;
        } /* end if */
        else {
            /* Remove from table */
            memmove(H5T_g.path + i, H5T_g.path + i + 1,
                    (size_t)(H5T_g.npaths - (i + 1)) * sizeof(H5T_path_t *));
            --H5T_g.npaths;

            /* Shut down path */
            H5T__print_stats(path, &nprint);
            path->cdata.command = H5T_CONV_FREE;
            if (path->conv.is_app) {
                if ((path->conv.u.app_func)((hid_t)FAIL, (hid_t)FAIL, &(path->cdata), (size_t)0, (size_t)0,
                                            (size_t)0, NULL, NULL, H5CX_get_dxpl()) < 0) {
#ifdef H5T_DEBUG
                    if (H5DEBUG(T))
                        fprintf(H5DEBUG(T),
                                "H5T: conversion function 0x%016zx failed "
                                "to free private data for %s (ignored)\n",
                                (size_t)path->conv.u.app_func, path->name);
#endif
                } /* end if */
            }     /* end if */
            else if ((path->conv.u.lib_func)((hid_t)FAIL, (hid_t)FAIL, &(path->cdata), (size_t)0, (size_t)0,
                                             (size_t)0, NULL, NULL) < 0) {
#ifdef H5T_DEBUG
                if (H5DEBUG(T))
                    fprintf(H5DEBUG(T),
                            "H5T: conversion function 0x%016zx failed "
                            "to free private data for %s (ignored)\n",
                            (size_t)path->conv.u.lib_func, path->name);
#endif
            } /* end if */
            (void)H5T_close_real(path->src);
            (void)H5T_close_real(path->dst);
            path = H5FL_FREE(H5T_path_t, path);
            H5E_clear_stack(NULL); /*ignore all shutdown errors*/
        }                          /* end else */
    }                              /* end for */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5T__unregister() */

/*-------------------------------------------------------------------------
 * Function:  H5Tunregister
 *
 * Purpose:   Removes conversion paths that match the specified criteria.
 *        All arguments are optional. Missing arguments are wild cards.
 *        The special no-op path cannot be removed.
 *
 * Return:    Success:    non-negative
 *
 *            Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tunregister(H5T_pers_t pers, const char *name, hid_t src_id, hid_t dst_id, H5T_conv_t func)
{
    H5T_t *src = NULL, *dst = NULL; /* Datatype descriptors */
    herr_t ret_value = SUCCEED;     /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "Te*siiTC", pers, name, src_id, dst_id, func);

    /* Check arguments */
    if (src_id > 0 && (NULL == (src = (H5T_t *)H5I_object_verify(src_id, H5I_DATATYPE))))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "src is not a data type");
    if (dst_id > 0 && (NULL == (dst = (H5T_t *)H5I_object_verify(dst_id, H5I_DATATYPE))))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "dst is not a data type");

    if (H5T__unregister(pers, name, src, dst, func) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTDELETE, FAIL, "internal unregister function failed");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tunregister() */

/*-------------------------------------------------------------------------
 * Function:  H5Tfind
 *
 * Purpose:   Finds a conversion function that can handle a conversion from
 *        type SRC_ID to type DST_ID.  The PCDATA argument is a pointer
 *        to a pointer to type conversion data which was created and
 *        initialized by the type conversion function of this path
 *        when the conversion function was installed on the path.
 *
 * Return:    Success:    A pointer to a suitable conversion function.
 *
 *            Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
H5T_conv_t
H5Tfind(hid_t src_id, hid_t dst_id, H5T_cdata_t **pcdata /*out*/)
{
    H5T_t      *src, *dst;
    H5T_path_t *path;
    H5T_conv_t  ret_value; /* Return value */

    FUNC_ENTER_API(NULL)
    H5TRACE3("TC", "iix", src_id, dst_id, pcdata);

    /* Check args */
    if (NULL == (src = (H5T_t *)H5I_object_verify(src_id, H5I_DATATYPE)) ||
        NULL == (dst = (H5T_t *)H5I_object_verify(dst_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a data type");
    if (!pcdata)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "no address to receive cdata pointer");

    /* Find it */
    if (NULL == (path = H5T_path_find(src, dst)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_NOTFOUND, NULL, "conversion function not found");

    if (pcdata)
        *pcdata = &(path->cdata);

    /* Set return value */
    ret_value = path->conv.u.app_func;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tfind() */

/*-------------------------------------------------------------------------
 * Function:  H5Tcompiler_conv
 *
 * Purpose:   Finds out whether the library's conversion function from
 *            type src_id to type dst_id is a compiler (hard) conversion.
 *            A hard conversion uses compiler's casting; a soft conversion
 *            uses the library's own conversion function.
 *
 * Return:    true:           hard conversion.
 *            false:          soft conversion.
 *            FAIL:           failed.
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5Tcompiler_conv(hid_t src_id, hid_t dst_id)
{
    H5T_t *src, *dst;
    htri_t ret_value; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("t", "ii", src_id, dst_id);

    /* Check args */
    if (NULL == (src = (H5T_t *)H5I_object_verify(src_id, H5I_DATATYPE)) ||
        NULL == (dst = (H5T_t *)H5I_object_verify(dst_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a data type");

    /* Find it */
    if ((ret_value = H5T__compiler_conv(src, dst)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_NOTFOUND, FAIL, "conversion function not found");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tcompiler_conv() */

/*-------------------------------------------------------------------------
 * Function:  H5Tconvert
 *
 * Purpose:   Convert NELMTS elements from type SRC_ID to type DST_ID.  The
 *        source elements are packed in BUF and on return the
 *        destination will be packed in BUF.  That is, the conversion
 *        is performed in place.  The optional background buffer is an
 *        array of NELMTS values of destination type which are merged
 *        with the converted values to fill in cracks (for instance,
 *        BACKGROUND might be an array of structs with the `a' and `b'
 *        fields already initialized and the conversion of BUF supplies
 *        the `c' and `d' field values).  The PLIST_ID a dataset transfer
 *        property list which is passed to the conversion functions.  (It's
 *        currently only used to pass along the VL datatype custom allocation
 *        information -QAK 7/1/99)
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tconvert(hid_t src_id, hid_t dst_id, size_t nelmts, void *buf, void *background, hid_t dxpl_id)
{
    H5T_path_t *tpath;               /* type conversion info    */
    H5T_t      *src, *dst;           /* unregistered types      */
    herr_t      ret_value = SUCCEED; /* Return value            */

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "iiz*x*xi", src_id, dst_id, nelmts, buf, background, dxpl_id);

    /* Check args */
    if (NULL == (src = (H5T_t *)H5I_object_verify(src_id, H5I_DATATYPE)) ||
        NULL == (dst = (H5T_t *)H5I_object_verify(dst_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a data type");
    if (H5P_DEFAULT == dxpl_id)
        dxpl_id = H5P_DATASET_XFER_DEFAULT;
    else if (true != H5P_isa_class(dxpl_id, H5P_DATASET_XFER))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not dataset transfer property list");

    /* Set DXPL for operation */
    H5CX_set_dxpl(dxpl_id);

    /* Find the conversion function */
    if (NULL == (tpath = H5T_path_find(src, dst)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to convert between src and dst data types");

    if (H5T_convert(tpath, src_id, dst_id, nelmts, (size_t)0, (size_t)0, buf, background) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "data type conversion failed");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tconvert() */

/*-------------------------------------------------------------------------
 * Function:    H5Treclaim
 *
 * Purpose: Frees the buffers allocated for storing variable-length data
 *      in memory.  Only frees the VL data in the selection defined in the
 *      dataspace.  The dataset transfer property list is required to find the
 *      correct allocation/free methods for the VL data in the buffer.
 *
 * Return:  Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Treclaim(hid_t type_id, hid_t space_id, hid_t dxpl_id, void *buf)
{
    H5S_t *space;     /* Dataspace for iteration */
    herr_t ret_value; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "iii*x", type_id, space_id, dxpl_id, buf);

    /* Check args */
    if (H5I_DATATYPE != H5I_get_type(type_id) || buf == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid argument");
    if (NULL == (space = (H5S_t *)H5I_object_verify(space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid dataspace");
    if (!(H5S_has_extent(space)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "dataspace does not have extent set");

    /* Get the default dataset transfer property list if the user didn't provide one */
    if (H5P_DEFAULT == dxpl_id)
        dxpl_id = H5P_DATASET_XFER_DEFAULT;
    else if (true != H5P_isa_class(dxpl_id, H5P_DATASET_XFER))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not xfer parms");

    /* Set DXPL for operation */
    H5CX_set_dxpl(dxpl_id);

    /* Call internal routine */
    ret_value = H5T_reclaim(type_id, space, buf);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Treclaim() */

/*-------------------------------------------------------------------------
 * Function:  H5Tencode
 *
 * Purpose:   Given an datatype ID, converts the object description into
 *            binary in a buffer.
 *
 * Return:    Success:    non-negative
 *
 *            Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tencode(hid_t obj_id, void *buf, size_t *nalloc)
{
    H5T_t *dtype;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "i*x*z", obj_id, buf, nalloc);

    /* Check argument and retrieve object */
    if (NULL == (dtype = (H5T_t *)H5I_object_verify(obj_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    if (nalloc == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "NULL pointer for buffer size");

    /* Go encode the datatype */
    if (H5T_encode(dtype, (unsigned char *)buf, nalloc) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTENCODE, FAIL, "can't encode datatype");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tencode() */

/*-------------------------------------------------------------------------
 * Function:  H5Tdecode
 *
 * Purpose:   Decode a binary object description and return a new object
 *            handle.
 *
 * Return:    Success:    datatype ID(non-negative)
 *
 *            Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Tdecode(const void *buf)
{
    H5T_t *dt;
    hid_t  ret_value; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("i", "*x", buf);

    /* Check args */
    if (buf == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "empty buffer");

    /* Create datatype by decoding buffer
     * There is no way to get the size of the buffer, so we pass in
     * SIZE_MAX and assume the caller knows what they are doing.
     * Really fixing this will require an H5Tdecode2() call that
     * takes a size parameter.
     */
    if (NULL == (dt = H5T_decode(SIZE_MAX, (const unsigned char *)buf)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTDECODE, FAIL, "can't decode object");

    /* Register the type and return the ID */
    if ((ret_value = H5I_register(H5I_DATATYPE, dt, true)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREGISTER, FAIL, "unable to register data type");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tdecode() */

/*-------------------------------------------------------------------------
 * API functions are above; library-private functions are below...
 *-------------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 * Function:  H5T_encode
 *
 * Purpose:   Private function for H5Tencode.  Converts an object
 *            description into binary in a buffer.
 *
 * Return:    Success:    non-negative
 *
 *            Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5T_encode(H5T_t *obj, unsigned char *buf, size_t *nalloc)
{
    size_t buf_size;         /* Encoded size of datatype */
    H5F_t *f         = NULL; /* Fake file structure*/
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

    /* Allocate "fake" file structure */
    if (NULL == (f = H5F_fake_alloc((uint8_t)0)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTALLOC, FAIL, "can't allocate fake file struct");

    /* Find out the size of buffer needed */
    if ((buf_size = H5O_msg_raw_size(f, H5O_DTYPE_ID, true, obj)) == 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_BADSIZE, FAIL, "can't find datatype size");

    /* Don't encode if buffer size isn't big enough or buffer is empty */
    if (!buf || *nalloc < (buf_size + 1 + 1))
        *nalloc = buf_size + 1 + 1;
    else {
        /* Encode the type of the information */
        *buf++ = H5O_DTYPE_ID;

        /* Encode the version of the dataspace information */
        *buf++ = H5T_ENCODE_VERSION;

        /* Encode into user's buffer */
        if (H5O_msg_encode(f, H5O_DTYPE_ID, true, buf, obj) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTENCODE, FAIL, "can't encode object");
    } /* end else */

done:
    /* Release fake file structure */
    if (f && H5F_fake_free(f) < 0)
        HDONE_ERROR(H5E_DATATYPE, H5E_CANTRELEASE, FAIL, "unable to release fake file struct");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_encode() */

/*-------------------------------------------------------------------------
 * Function:  H5T_decode
 *
 * Purpose:   Private function for H5Tdecode.  Reconstructs a binary
 *            description of datatype and returns a new object handle.
 *
 * Return:    Success:    datatype ID(non-negative)
 *
 *            Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
H5T_t *
H5T_decode(size_t buf_size, const unsigned char *buf)
{
    H5F_t *f         = NULL; /* Fake file structure*/
    H5T_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Allocate "fake" file structure */
    if (NULL == (f = H5F_fake_alloc((uint8_t)0)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTALLOC, NULL, "can't allocate fake file struct");

    /* Decode the type of the information */
    if (*buf++ != H5O_DTYPE_ID)
        HGOTO_ERROR(H5E_DATATYPE, H5E_BADMESG, NULL, "not an encoded datatype");

    /* Decode the version of the datatype information */
    if (*buf++ != H5T_ENCODE_VERSION)
        HGOTO_ERROR(H5E_DATATYPE, H5E_VERSION, NULL, "unknown version of encoded datatype");

    /* Decode the serialized datatype message */
    if (NULL == (ret_value = (H5T_t *)H5O_msg_decode(f, NULL, H5O_DTYPE_ID, buf_size, buf)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTDECODE, NULL, "can't decode object");

    /* Mark datatype as being in memory now */
    if (H5T_set_loc(ret_value, NULL, H5T_LOC_MEMORY) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "invalid datatype location");

    /* No VOL object */
    ret_value->vol_obj = NULL;
done:
    /* Release fake file structure */
    if (f && H5F_fake_free(f) < 0)
        HDONE_ERROR(H5E_DATATYPE, H5E_CANTRELEASE, NULL, "unable to release fake file struct");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_decode() */

/*-------------------------------------------------------------------------
 * Function:  H5T__create
 *
 * Purpose:   Creates a new data type and initializes it to reasonable
 *            values.     The new data type is SIZE bytes and an instance of
 *            the class TYPE.
 *
 * Return:    Success:    Pointer to the new type.
 *
 *            Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
H5T_t *
H5T__create(H5T_class_t type, size_t size)
{
    H5T_t *dt        = NULL;
    H5T_t *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    switch (type) {
        case H5T_INTEGER:
        case H5T_FLOAT:
        case H5T_TIME:
        case H5T_STRING: {
            H5T_t *origin_dt = NULL;

            if (NULL == (origin_dt = (H5T_t *)H5I_object(H5T_C_S1)))
                HGOTO_ERROR(H5E_DATATYPE, H5E_BADTYPE, NULL, "can't get structure for string type");

            /* Copy the default string datatype */
            if (NULL == (dt = H5T_copy(origin_dt, H5T_COPY_TRANSIENT)))
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "unable to copy");

            /* Modify the datatype */
            if (H5T__set_size(dt, size) < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "unable to set size for string type");
        } break;

        case H5T_BITFIELD:
            HGOTO_ERROR(H5E_DATATYPE, H5E_UNSUPPORTED, NULL, "type class is not appropriate - use H5Tcopy()");

        case H5T_OPAQUE:
        case H5T_COMPOUND:
            if (NULL == (dt = H5T__alloc()))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");
            dt->shared->type = type;

            if (type == H5T_COMPOUND) {
                dt->shared->u.compnd.packed    = false; /* Start out unpacked */
                dt->shared->u.compnd.memb_size = 0;
            } /* end if */
            else if (type == H5T_OPAQUE)
                /* Initialize the tag in case it's not set later.  A null tag will
                 * cause problems for later operations. */
                dt->shared->u.opaque.tag = H5MM_strdup("");
            break;

        case H5T_ENUM: {
            hid_t  subtype;
            H5T_t *sub_t_obj;

            if (sizeof(char) == size)
                subtype = H5T_NATIVE_SCHAR_g;
            else if (sizeof(short) == size)
                subtype = H5T_NATIVE_SHORT_g;
            else if (sizeof(int) == size)
                subtype = H5T_NATIVE_INT_g;
            else if (sizeof(long) == size)
                subtype = H5T_NATIVE_LONG_g;
#if H5_SIZEOF_LONG != H5_SIZEOF_LONG_LONG
            else if (sizeof(long long) == size)
                subtype = H5T_NATIVE_LLONG_g;
#endif /* H5_SIZEOF_LONG != H5_SIZEOF_LONG_LONG */
            else
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "no applicable native integer type");
            if (NULL == (dt = H5T__alloc()))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");
            dt->shared->type = type;
            if (NULL == (sub_t_obj = (H5T_t *)H5I_object(subtype)))
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, NULL, "unable to get datatype object");
            if (NULL == (dt->shared->parent = H5T_copy(sub_t_obj, H5T_COPY_ALL)))
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, NULL, "unable to copy base datatype");
        } break;

        case H5T_VLEN: /* Variable length datatype */
            HGOTO_ERROR(H5E_DATATYPE, H5E_UNSUPPORTED, NULL, "base type required - use H5Tvlen_create()");

        case H5T_ARRAY: /* Array datatype */
            HGOTO_ERROR(H5E_DATATYPE, H5E_UNSUPPORTED, NULL, "base type required - use H5Tarray_create2()");

        case H5T_NO_CLASS:
        case H5T_REFERENCE:
        case H5T_NCLASSES:
        default:
            HGOTO_ERROR(H5E_DATATYPE, H5E_UNSUPPORTED, NULL, "unknown data type class");
    } /* end switch */

    /* Set the size except VL string */
    if (H5T_STRING != type || H5T_VARIABLE != size)
        dt->shared->size = size;

    /* No VOL object */
    dt->vol_obj = NULL;

    /* Set return value */
    ret_value = dt;

done:
    if (NULL == ret_value) {
        if (dt) {
            if (dt->shared->owned_vol_obj && H5VL_free_object(dt->shared->owned_vol_obj) < 0)
                HDONE_ERROR(H5E_DATATYPE, H5E_CANTCLOSEOBJ, NULL, "unable to close owned VOL object");
            dt->shared = H5FL_FREE(H5T_shared_t, dt->shared);
            dt         = H5FL_FREE(H5T_t, dt);
        }
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__create() */

/*-------------------------------------------------------------------------
 * Function:  H5T__initiate_copy
 *
 * Purpose:   Allocates datatype structures, copies core fields, and initializes
 *            VOL fields.
 *
 * Return:    Success:    Pointer to a new copy of the OLD_DT argument.
 *            Failure:    NULL
 *
 * Note:      Common code for both H5T_copy and H5T_copy_reopen, as part of
 *            the const-correct datatype copying routines.
 *
 *-------------------------------------------------------------------------
 */
static H5T_t *
H5T__initiate_copy(const H5T_t *old_dt)
{
    H5T_t *new_dt    = NULL; /* Copy of datatype */
    H5T_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Allocate space */
    if (NULL == (new_dt = H5FL_MALLOC(H5T_t)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTALLOC, NULL, "H5T_t memory allocation failed");
    if (NULL == (new_dt->shared = H5FL_MALLOC(H5T_shared_t)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTALLOC, NULL, "H5T_shared_t memory allocation failed");

    /* Copy shared information */
    *(new_dt->shared) = *(old_dt->shared);

    /* Increment ref count on owned VOL object */
    if (new_dt->shared->owned_vol_obj)
        (void)H5VL_object_inc_rc(new_dt->shared->owned_vol_obj);

    /* Reset vol_obj field */
    new_dt->vol_obj = NULL;

    /* Set return value */
    ret_value = new_dt;

done:
    if (ret_value == NULL)
        if (new_dt) {
            if (new_dt->shared) {
                if (new_dt->shared->owned_vol_obj && H5VL_free_object(new_dt->shared->owned_vol_obj) < 0)
                    HDONE_ERROR(H5E_DATATYPE, H5E_CANTCLOSEOBJ, NULL, "unable to close owned VOL object");
                new_dt->shared = H5FL_FREE(H5T_shared_t, new_dt->shared);
            } /* end if */
            new_dt = H5FL_FREE(H5T_t, new_dt);
        } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__initiate_copy() */

/*-------------------------------------------------------------------------
 * Function:  H5T__copy_transient
 *
 * Purpose:   Part of recursive framework for const-correct datatype copying.
 *
 * Return:    Success:    Pointer to a new copy of the OLD_DT argument.
 *            Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static H5T_t *
H5T__copy_transient(H5T_t *old_dt)
{
    H5T_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Copy datatype, with correct method */
    if (NULL == (ret_value = H5T_copy(old_dt, H5T_COPY_TRANSIENT)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, NULL, "can't make 'transient' copy of datatype");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__copy_transient() */

/*-------------------------------------------------------------------------
 * Function:  H5T__copy_all
 *
 * Purpose:   Part of recursive framework for const-correct datatype copying.
 *
 * Return:    Success:    Pointer to a new copy of the OLD_DT argument.
 *            Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static H5T_t *
H5T__copy_all(H5T_t *old_dt)
{
    H5T_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Copy datatype, with correct method */
    if (NULL == (ret_value = H5T_copy(old_dt, H5T_COPY_ALL)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, NULL, "can't make 'all' copy of datatype");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__copy_transient() */

/*-------------------------------------------------------------------------
 * Function:  H5T__complete_copy
 *
 * Purpose:   Completes copying datatype fields, as part of the recursive
 *            const-correct datatype copy routines.
 *
 * Return:    Success:    non-negative
 *            Failure:    negative
 *
 * Note:      Common code for both H5T_copy and H5T_copy_reopen.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__complete_copy(H5T_t *new_dt, const H5T_t *old_dt, H5T_shared_t *reopened_fo, bool set_memory_type,
                   H5T_copy_func_t copyfn)
{
    H5T_t   *tmp = NULL;          /* Temporary copy of compound field's datatype */
    char    *s;                   /* Temporary copy of compound field name / enum value name */
    unsigned i;                   /* Local index variable */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Update fields in the new struct, if we aren't sharing an already opened
     * committed datatype */
    if (!reopened_fo) {
        /* Copy parent information */
        if (old_dt->shared->parent)
            if (NULL == (new_dt->shared->parent = (*copyfn)(old_dt->shared->parent)))
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, FAIL, "can't copy datatype's parent type");

        switch (new_dt->shared->type) {
            case H5T_COMPOUND: {
                ssize_t accum_change = 0; /* Amount of change in the offset of the fields */

                /*
                 * Copy all member fields to new type, then overwrite the
                 * name and type fields of each new member with copied values.
                 * That is, H5T_copy() is a deep copy.
                 */
                /* Only malloc if space has been allocated for members - NAF */
                if (new_dt->shared->u.compnd.nalloc > 0) {
                    if (NULL == (new_dt->shared->u.compnd.memb =
                                     H5MM_malloc(new_dt->shared->u.compnd.nalloc * sizeof(H5T_cmemb_t))))
                        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTALLOC, FAIL, "memory allocation failed");

                    H5MM_memcpy(new_dt->shared->u.compnd.memb, old_dt->shared->u.compnd.memb,
                                new_dt->shared->u.compnd.nmembs * sizeof(H5T_cmemb_t));
                } /* end if */

                for (i = 0; i < new_dt->shared->u.compnd.nmembs; i++) {
                    unsigned j;
                    int      old_match;

                    if (NULL == (s = H5MM_xstrdup(new_dt->shared->u.compnd.memb[i].name)))
                        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, FAIL,
                                    "can't copy string for compound field's name");
                    new_dt->shared->u.compnd.memb[i].name = s;
                    if (NULL == (tmp = (*copyfn)(old_dt->shared->u.compnd.memb[i].type)))
                        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, FAIL, "can't copy compound field's datatype");
                    new_dt->shared->u.compnd.memb[i].type = tmp;
                    assert(tmp != NULL);

                    /* Range check against compound member's offset */
                    if ((accum_change < 0) &&
                        ((ssize_t)new_dt->shared->u.compnd.memb[i].offset < accum_change))
                        HGOTO_ERROR(H5E_DATATYPE, H5E_BADVALUE, FAIL, "invalid field size in datatype");

                    /* Apply the accumulated size change to the offset of the field */
                    new_dt->shared->u.compnd.memb[i].offset += (size_t)accum_change;

                    if (old_dt->shared->u.compnd.sorted != H5T_SORT_VALUE) {
                        for (old_match = -1, j = 0; j < old_dt->shared->u.compnd.nmembs; j++) {
                            if (!strcmp(new_dt->shared->u.compnd.memb[i].name,
                                        old_dt->shared->u.compnd.memb[j].name)) {
                                old_match = (int)j;
                                break;
                            } /* end if */
                        }     /* end for */

                        /* check if we couldn't find a match */
                        if (old_match < 0)
                            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, FAIL, "fields in datatype corrupted");
                    } /* end if */
                    else
                        old_match = (int)i;

                    /* If the field changed size, add that change to the accumulated size change */
                    if (new_dt->shared->u.compnd.memb[i].type->shared->size !=
                        old_dt->shared->u.compnd.memb[old_match].type->shared->size) {
                        /* Adjust the size of the member */
                        new_dt->shared->u.compnd.memb[i].size =
                            (old_dt->shared->u.compnd.memb[old_match].size * tmp->shared->size) /
                            old_dt->shared->u.compnd.memb[old_match].type->shared->size;

                        accum_change +=
                            (ssize_t)(new_dt->shared->u.compnd.memb[i].type->shared->size -
                                      old_dt->shared->u.compnd.memb[old_match].type->shared->size);
                    } /* end if */
                }     /* end for */

                /* Range check against datatype size */
                if ((accum_change < 0) && ((ssize_t)new_dt->shared->size < accum_change))
                    HGOTO_ERROR(H5E_DATATYPE, H5E_BADVALUE, FAIL, "invalid field size in datatype");

                /* Apply the accumulated size change to the size of the compound struct */
                new_dt->shared->size += (size_t)accum_change;
            } break;

            case H5T_ENUM:
                /*
                 * Copy all member fields to new type, then overwrite the name fields
                 * of each new member with copied values. That is, H5T_copy() is a
                 * deep copy.
                 */
                if (old_dt->shared->u.enumer.nalloc > 0) {
                    if (NULL == (new_dt->shared->u.enumer.name =
                                     H5MM_malloc(new_dt->shared->u.enumer.nalloc * sizeof(char *))))
                        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTALLOC, FAIL,
                                    "enam name array memory allocation failed");
                    if (NULL == (new_dt->shared->u.enumer.value =
                                     H5MM_malloc(new_dt->shared->u.enumer.nalloc * new_dt->shared->size)))
                        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTALLOC, FAIL,
                                    "enam value array memory allocation failed");
                    H5MM_memcpy(new_dt->shared->u.enumer.value, old_dt->shared->u.enumer.value,
                                new_dt->shared->u.enumer.nmembs * new_dt->shared->size);
                    for (i = 0; i < new_dt->shared->u.enumer.nmembs; i++) {
                        if (NULL == (s = H5MM_xstrdup(old_dt->shared->u.enumer.name[i])))
                            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, FAIL,
                                        "can't copy string for enum value's name");
                        new_dt->shared->u.enumer.name[i] = s;
                    }
                }
                else {
                    /* Empty enum */
                    memset(&new_dt->shared->u.enumer, 0, sizeof(H5T_enum_t));
                }
                break;

            case H5T_VLEN:
            case H5T_REFERENCE:
                if (set_memory_type)
                    /* H5T_copy converts any type into a memory type */
                    if (H5T_set_loc(new_dt, NULL, H5T_LOC_MEMORY) < 0)
                        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "invalid datatype location");
                break;

            case H5T_OPAQUE:
                /*
                 * Copy the tag name.
                 */
                new_dt->shared->u.opaque.tag = H5MM_xstrdup(new_dt->shared->u.opaque.tag);
                break;

            case H5T_ARRAY:
                /* Re-compute the array's size, in case it's base type changed size */
                new_dt->shared->size = new_dt->shared->u.array.nelem * new_dt->shared->parent->shared->size;
                break;

            case H5T_NO_CLASS:
            case H5T_INTEGER:
            case H5T_FLOAT:
            case H5T_TIME:
            case H5T_STRING:
            case H5T_BITFIELD:
            case H5T_NCLASSES:
            default:
                break;
        } /* end switch */
    }     /* end if */

    /* Set the cached location & name path if the original type was a named
     * type and the new type is also named.
     */
    if (H5O_loc_reset(&new_dt->oloc) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTRESET, FAIL, "unable to initialize location");
    if (H5G_name_reset(&new_dt->path) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, FAIL, "unable to reset path");

    if (new_dt->shared->state == H5T_STATE_NAMED || new_dt->shared->state == H5T_STATE_OPEN) {
        if (H5O_loc_copy_deep(&(new_dt->oloc), &(old_dt->oloc)) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, FAIL, "can't copy object location");
        if (H5G_name_copy(&(new_dt->path), &(old_dt->path), H5_COPY_DEEP) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, FAIL, "unable to copy path");
    } /* end if */

    /* Copy shared location information if the new type is named or if it is
     * shared in the heap.
     */
    if ((old_dt->sh_loc.type == H5O_SHARE_TYPE_SOHM || old_dt->sh_loc.type == H5O_SHARE_TYPE_HERE) ||
        new_dt->shared->state == H5T_STATE_NAMED || new_dt->shared->state == H5T_STATE_OPEN) {
        if (H5O_set_shared(&(new_dt->sh_loc), &(old_dt->sh_loc)) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, FAIL, "can't copy shared information");
    } /* end if */
    else
        /* Reset shared component info */
        H5O_msg_reset_share(H5O_DTYPE_ID, new_dt);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__complete_copy() */

/*-------------------------------------------------------------------------
 * Function:  H5T_copy
 *
 * Purpose:   Copies datatype OLD_DT.     The resulting data type is not
 *            locked and is a transient type.
 *
 * Return:    Success:    Pointer to a new copy of the OLD_DT argument.
 *            Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
H5T_t *
H5T_copy(const H5T_t *old_dt, H5T_copy_t method)
{
    H5T_t          *new_dt = NULL;    /* New datatype */
    H5T_copy_func_t copyfn;           /* Pointer to correct copy routine */
    H5T_t          *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /* check args */
    assert(old_dt);

    /* Allocate and copy core datatype information */
    if (NULL == (new_dt = H5T__initiate_copy(old_dt)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, NULL, "can't copy core datatype info");

    /* Check what sort of copy we are making */
    switch (method) {
        case H5T_COPY_TRANSIENT:
            /*
             * Return an unlocked transient type.
             */
            new_dt->shared->state = H5T_STATE_TRANSIENT;
            copyfn                = H5T__copy_transient;
            break;

        case H5T_COPY_ALL:
            /*
             * Return a transient type (locked or unlocked) or an unopened named
             * type.  Immutable transient types are degraded to read-only.
             */
            if (H5T_STATE_OPEN == old_dt->shared->state)
                new_dt->shared->state = H5T_STATE_NAMED;
            else if (H5T_STATE_IMMUTABLE == old_dt->shared->state)
                new_dt->shared->state = H5T_STATE_RDONLY;
            copyfn = H5T__copy_all;
            break;

        default:
            HGOTO_ERROR(H5E_DATATYPE, H5E_BADTYPE, NULL, "invalid copy method type");
    } /* end switch */

    /* Finish making the copy of the datatype */
    if (H5T__complete_copy(new_dt, old_dt, NULL, (method == H5T_COPY_TRANSIENT), copyfn) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't complete datatype initialization");

    /* Set return value */
    ret_value = new_dt;

done:
    if (ret_value == NULL)
        if (new_dt) {
            assert(new_dt->shared);
            if (new_dt->shared->owned_vol_obj && H5VL_free_object(new_dt->shared->owned_vol_obj) < 0)
                HDONE_ERROR(H5E_DATATYPE, H5E_CANTCLOSEOBJ, NULL, "unable to close owned VOL object");
            new_dt->shared = H5FL_FREE(H5T_shared_t, new_dt->shared);
            new_dt         = H5FL_FREE(H5T_t, new_dt);
        } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_copy() */

/*-------------------------------------------------------------------------
 * Function:  H5T_copy_reopen
 *
 * Purpose:   Copy a datatype, possibly reopening a named datatype, as part
 *            the const-correct datatype copying routines.
 *
 * Return:    Success:    Pointer to a new copy of the OLD_DT argument.
 *            Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
H5T_t *
H5T_copy_reopen(H5T_t *old_dt)
{
    H5T_t        *new_dt      = NULL; /* New datatype */
    H5T_shared_t *reopened_fo = NULL; /* Pointer to reopened existing named datatype */
    H5T_t        *ret_value   = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /* check args */
    assert(old_dt);

    /* Allocate and copy core datatype information */
    if (NULL == (new_dt = H5T__initiate_copy(old_dt)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, NULL, "can't copy core datatype info");

    /*
     * Return a transient type (locked or unlocked) or an opened named
     * type.  Immutable transient types are degraded to read-only.
     */
    if (old_dt->sh_loc.type == H5O_SHARE_TYPE_COMMITTED) {
        /* Check if the object is already open */
        if (NULL ==
            (reopened_fo = (H5T_shared_t *)H5FO_opened(old_dt->sh_loc.file, old_dt->sh_loc.u.loc.oh_addr))) {
            /* Clear any errors from H5FO_opened() */
            H5E_clear_stack(NULL);

            /* Open named datatype again */
            if (H5O_open(&old_dt->oloc) < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, NULL, "unable to reopen named data type");

            /* Insert opened named datatype into opened object list for the file */
            if (H5FO_insert(old_dt->sh_loc.file, old_dt->sh_loc.u.loc.oh_addr, new_dt->shared, false) < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINSERT, NULL,
                            "can't insert datatype into list of open objects");

            /* Increment object count for the object in the top file */
            if (H5FO_top_incr(old_dt->sh_loc.file, old_dt->sh_loc.u.loc.oh_addr) < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINC, NULL, "can't increment object count");

            new_dt->shared->fo_count = 1;
        } /* end if */
        else {
            /* The object is already open.  Free the H5T_shared_t struct
             * we had been using and use the one that already exists.
             * Not terribly efficient. */
            if (new_dt->shared->owned_vol_obj && H5VL_free_object(new_dt->shared->owned_vol_obj) < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCLOSEOBJ, NULL, "unable to close owned VOL object");
            new_dt->shared = H5FL_FREE(H5T_shared_t, new_dt->shared);
            new_dt->shared = reopened_fo;

            reopened_fo->fo_count++;

            /* Check if the object has been opened through the top file yet */
            if (H5FO_top_count(old_dt->sh_loc.file, old_dt->sh_loc.u.loc.oh_addr) == 0) {
                /* Open the object through this top file */
                if (H5O_open(&old_dt->oloc) < 0)
                    HGOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, NULL, "unable to open object header");
            } /* end if */

            /* Increment object count for the object in the top file */
            if (H5FO_top_incr(old_dt->sh_loc.file, old_dt->sh_loc.u.loc.oh_addr) < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINC, NULL, "can't increment object count");
        } /* end else */

        /* Set state for new datatype */
        new_dt->shared->state = H5T_STATE_OPEN;
    } /* end if */
    else
        /* Downgrade immutable datatypes to read-only */
        if (H5T_STATE_IMMUTABLE == old_dt->shared->state)
            new_dt->shared->state = H5T_STATE_RDONLY;

    /* Finish making the copy of the datatype */
    if (H5T__complete_copy(new_dt, old_dt, reopened_fo, true, H5T_copy_reopen) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "can't complete datatype initialization");

    /* Set return value */
    ret_value = new_dt;

done:
    if (ret_value == NULL)
        if (new_dt) {
            assert(new_dt->shared);
            if (new_dt->shared->owned_vol_obj && H5VL_free_object(new_dt->shared->owned_vol_obj) < 0)
                HDONE_ERROR(H5E_DATATYPE, H5E_CANTCLOSEOBJ, NULL, "unable to close owned VOL object");
            new_dt->shared = H5FL_FREE(H5T_shared_t, new_dt->shared);
            new_dt         = H5FL_FREE(H5T_t, new_dt);
        } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_copy_reopen() */

/*-------------------------------------------------------------------------
 * Function:  H5T_lock
 *
 * Purpose:   Lock a transient data type making it read-only.  If IMMUTABLE
 *        is set then the type cannot be closed except when the library
 *        itself closes.
 *
 *        This function is a no-op if the type is not transient or if
 *        the type is already read-only or immutable.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5T_lock(H5T_t *dt, bool immutable)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    assert(dt);

    switch (dt->shared->state) {
        case H5T_STATE_TRANSIENT:
            dt->shared->state = immutable ? H5T_STATE_IMMUTABLE : H5T_STATE_RDONLY;
            break;
        case H5T_STATE_RDONLY:
            if (immutable)
                dt->shared->state = H5T_STATE_IMMUTABLE;
            break;
        case H5T_STATE_IMMUTABLE:
        case H5T_STATE_NAMED:
        case H5T_STATE_OPEN:
            /*void*/
            break;
        default:
            HGOTO_ERROR(H5E_DATATYPE, H5E_BADTYPE, FAIL, "invalid datatype state");
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:  H5T__alloc
 *
 * Purpose:   Allocates a new H5T_t structure, initializing it correctly.
 *
 * Return:    Pointer to new H5T_t on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5T_t *
H5T__alloc(void)
{
    H5T_t *dt        = NULL; /* Pointer to datatype allocated */
    H5T_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Allocate & initialize datatype wrapper info */
    if (NULL == (dt = H5FL_CALLOC(H5T_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");
    H5O_loc_reset(&(dt->oloc));
    H5G_name_reset(&(dt->path));
    H5O_msg_reset_share(H5O_DTYPE_ID, dt);

    /* Allocate & initialize shared datatype structure */
    if (NULL == (dt->shared = H5FL_CALLOC(H5T_shared_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");
    dt->shared->version = H5O_DTYPE_VERSION_1;

    /* No VOL object initially */
    dt->vol_obj = NULL;

    /* Assign return value */
    ret_value = dt;

done:
    if (ret_value == NULL)
        if (dt) {
            if (dt->shared) {
                assert(!dt->shared->owned_vol_obj);
                dt->shared = H5FL_FREE(H5T_shared_t, dt->shared);
            } /* end if */
            dt = H5FL_FREE(H5T_t, dt);
        } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__alloc() */

/*-------------------------------------------------------------------------
 * Function:  H5T__free
 *
 * Purpose:   Frees all memory associated with a datatype, but does not
 *            free the H5T_t or H5T_shared_t structures (which should
 *            be done in H5T_close / H5T_close_real).
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5T__free(H5T_t *dt)
{
    unsigned i;
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(dt && dt->shared);

    /* Free the ID to name info */
    H5G_name_free(&(dt->path));

    /* Don't free locked datatypes */
    if (H5T_STATE_IMMUTABLE == dt->shared->state)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "unable to close immutable datatype");

    /* Close the datatype */
    switch (dt->shared->type) {
        case H5T_COMPOUND:
            for (i = 0; i < dt->shared->u.compnd.nmembs; i++) {
                dt->shared->u.compnd.memb[i].name = (char *)H5MM_xfree(dt->shared->u.compnd.memb[i].name);
                (void)H5T_close_real(dt->shared->u.compnd.memb[i].type);
            }
            dt->shared->u.compnd.memb   = (H5T_cmemb_t *)H5MM_xfree(dt->shared->u.compnd.memb);
            dt->shared->u.compnd.nmembs = 0;
            break;

        case H5T_ENUM:
            for (i = 0; i < dt->shared->u.enumer.nmembs; i++)
                dt->shared->u.enumer.name[i] = (char *)H5MM_xfree(dt->shared->u.enumer.name[i]);
            dt->shared->u.enumer.name   = (char **)H5MM_xfree(dt->shared->u.enumer.name);
            dt->shared->u.enumer.value  = (uint8_t *)H5MM_xfree(dt->shared->u.enumer.value);
            dt->shared->u.enumer.nmembs = 0;
            break;

        case H5T_OPAQUE:
            dt->shared->u.opaque.tag = (char *)H5MM_xfree(dt->shared->u.opaque.tag);
            break;

        case H5T_NO_CLASS:
        case H5T_INTEGER:
        case H5T_FLOAT:
        case H5T_TIME:
        case H5T_STRING:
        case H5T_BITFIELD:
        case H5T_REFERENCE:
        case H5T_VLEN:
        case H5T_ARRAY:
        case H5T_NCLASSES:
        default:
            break;
    } /* end switch */
    dt->shared->type = H5T_NO_CLASS;

    /* Close the parent */
    assert(dt->shared->parent != dt);
    if (dt->shared->parent && H5T_close_real(dt->shared->parent) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCLOSEOBJ, FAIL, "unable to close parent data type");
    dt->shared->parent = NULL;

    /* Close the owned VOL object */
    if (dt->shared->owned_vol_obj && H5VL_free_object(dt->shared->owned_vol_obj) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCLOSEOBJ, FAIL, "unable to close owned VOL object");
    dt->shared->owned_vol_obj = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__free() */

/*-------------------------------------------------------------------------
 * Function:  H5T_close_real
 *
 * Purpose:   Frees a datatype and all associated memory.
 *
 * Note:      Does _not_ deal with open named datatypes, etc. so this
 *            should never see a type managed by a VOL connector.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5T_close_real(H5T_t *dt)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(dt && dt->shared);

    /* Clean up resources, depending on shared state */
    if (dt->shared->state != H5T_STATE_OPEN) {
        if (H5T__free(dt) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTFREE, FAIL, "unable to free datatype");

        assert(!dt->shared->owned_vol_obj);
        dt->shared = H5FL_FREE(H5T_shared_t, dt->shared);
    } /* end if */
    else
        /* Free the group hier. path since we're not calling H5T__free() */
        H5G_name_free(&(dt->path));

    /* Free the 'top' datatype struct */
    dt = H5FL_FREE(H5T_t, dt);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_close_real() */

/*-------------------------------------------------------------------------
 * Function:    H5T_close
 *
 * Purpose:     Frees a data type and all associated memory.  Deals with
 *              open named datatypes appropriately.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5T_close(H5T_t *dt)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(dt);
    assert(dt->shared);

    /* Named datatype cleanups */
    if (dt->shared->state == H5T_STATE_OPEN) {
        /* Decrement refcount count on open named datatype */
        dt->shared->fo_count--;

        /* Sanity checks */
        assert(dt->sh_loc.type == H5O_SHARE_TYPE_COMMITTED);
        assert(H5_addr_defined(dt->sh_loc.u.loc.oh_addr));
        assert(H5_addr_defined(dt->oloc.addr));

        /* If a named type is being closed then close the object header and
         * remove from the list of open objects in the file.
         */

        /* Decrement the ref. count for this object in the top file */
        if (H5FO_top_decr(dt->sh_loc.file, dt->sh_loc.u.loc.oh_addr) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTRELEASE, FAIL, "can't decrement count for object");

        /* Close things down if this is the last reference to the open named datatype */
        if (0 == dt->shared->fo_count) {
            bool corked; /* Whether the named datatype is corked or not */

            /* Uncork cache entries with object address tag for named datatype */
            if (H5AC_cork(dt->oloc.file, dt->oloc.addr, H5AC__GET_CORKED, &corked) < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "unable to retrieve an object's cork status");
            if (corked)
                if (H5AC_cork(dt->oloc.file, dt->oloc.addr, H5AC__UNCORK, NULL) < 0)
                    HGOTO_ERROR(H5E_DATATYPE, H5E_CANTUNCORK, FAIL, "unable to uncork an object");

            /* Remove the datatype from the list of opened objects in the file */
            if (H5FO_delete(dt->sh_loc.file, dt->sh_loc.u.loc.oh_addr) < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTRELEASE, FAIL,
                            "can't remove datatype from list of open objects");
            if (H5O_close(&dt->oloc, NULL) < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to close data type object header");

            /* Mark named datatype closed now */
            dt->shared->state = H5T_STATE_NAMED;
        } /* end if */
        else {
            /* Check reference count for this object in the top file */
            if (H5FO_top_count(dt->sh_loc.file, dt->sh_loc.u.loc.oh_addr) == 0) {
                /* Close object location for named datatype */
                if (H5O_close(&dt->oloc, NULL) < 0)
                    HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to close");
            } /* end if */
            else
                /* Free object location (i.e. "unhold" the file if appropriate) */
                if (H5O_loc_free(&(dt->oloc)) < 0)
                    HGOTO_ERROR(H5E_DATATYPE, H5E_CANTRELEASE, FAIL, "problem attempting to free location");
        } /* end else */
    }     /* end if */

    /* Clean up resources */
    if (H5T_close_real(dt) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTRELEASE, FAIL, "unable to free datatype");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_close() */

/*-------------------------------------------------------------------------
 * Function:    H5T__set_size
 *
 * Purpose:    Sets the total size in bytes for a data type (this operation
 *        is not permitted on reference data types).  If the size is
 *        decreased so that the significant bits of the data type
 *        extend beyond the edge of the new size, then the `offset'
 *        property is decreased toward zero.  If the `offset' becomes
 *        zero and the significant bits of the data type still hang
 *        over the edge of the new size, then the number of significant
 *        bits is decreased.
 *
 *        Adjusting the size of an H5T_STRING automatically sets the
 *        precision to 8*size.
 *
 *        All data types have a positive size.
 *
 * Return:    Success:    non-negative
 *
 *            Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__set_size(H5T_t *dt, size_t size)
{
    size_t prec, offset;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(dt);
    assert(dt->shared);
    assert(size != 0);
    assert(H5T_REFERENCE != dt->shared->type);
    assert(!(H5T_ENUM == dt->shared->type && 0 == dt->shared->u.enumer.nmembs));

    if (dt->shared->parent) {
        if (H5T__set_size(dt->shared->parent, size) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to set size for parent data type");

        /* Adjust size of datatype appropriately */
        if (dt->shared->type == H5T_ARRAY)
            dt->shared->size = dt->shared->parent->shared->size * dt->shared->u.array.nelem;
        else if (dt->shared->type != H5T_VLEN)
            dt->shared->size = dt->shared->parent->shared->size;
    }
    else {
        if (H5T_IS_ATOMIC(dt->shared)) {
            offset = dt->shared->u.atomic.offset;
            prec   = dt->shared->u.atomic.prec;

            /* Decrement the offset and precision if necessary */
            if (prec > 8 * size)
                offset = 0;
            else if (offset + prec > 8 * size)
                offset = 8 * size - prec;
            if (prec > 8 * size)
                prec = 8 * size;
        }
        else
            prec = offset = 0;

        switch (dt->shared->type) {
            case H5T_INTEGER:
            case H5T_TIME:
            case H5T_BITFIELD:
            case H5T_OPAQUE:
                /* nothing to check */
                break;

            case H5T_COMPOUND:
                /* If decreasing size, check the last member isn't being cut. */
                if (size < dt->shared->size) {
                    int      num_membs = 0;
                    unsigned i, max_index            = 0;
                    size_t   memb_offset, max_offset = 0;
                    size_t   max_size;

                    if ((num_membs = H5T_get_nmembers(dt)) < 0)
                        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to get number of members");

                    if (num_membs) {
                        for (i = 0; i < (unsigned)num_membs; i++) {
                            memb_offset = H5T_get_member_offset(dt, i);
                            if (memb_offset > max_offset) {
                                max_offset = memb_offset;
                                max_index  = i;
                            } /* end if */
                        }     /* end for */

                        max_size = H5T__get_member_size(dt, max_index);

                        if (size < (max_offset + max_size))
                            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                                        "size shrinking will cut off last member ");
                    } /* end if */

                    /* Compound must not have been packed previously */
                    /* We will check if resizing changed the packed state of
                     * this type at the end of this function */
                    assert(!dt->shared->u.compnd.packed);
                } /* end if */

                break;

            case H5T_STRING:
                /* Convert string to variable-length datatype */
                if (size == H5T_VARIABLE) {
                    H5T_t     *base = NULL; /* base data type */
                    H5T_cset_t tmp_cset;    /* Temp. cset info */
                    H5T_str_t  tmp_strpad;  /* Temp. strpad info */

                    /* Get a copy of unsigned char type as the base/parent type */
                    if (NULL == (base = (H5T_t *)H5I_object(H5T_NATIVE_UCHAR)))
                        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid base datatype");
                    dt->shared->parent = H5T_copy(base, H5T_COPY_ALL);

                    /* change this datatype into a VL string */
                    dt->shared->type = H5T_VLEN;

                    /*
                     * Force conversions (i.e. memory to memory conversions
                     * should duplicate data, not point to the same VL strings)
                     */
                    dt->shared->force_conv = true;

                    /* Before we mess with the info in the union, extract the
                     * values we need */
                    tmp_cset   = dt->shared->u.atomic.u.s.cset;
                    tmp_strpad = dt->shared->u.atomic.u.s.pad;

                    /* This is a string, not a sequence */
                    dt->shared->u.vlen.type = H5T_VLEN_STRING;

                    /* Set character set and padding information */
                    dt->shared->u.vlen.cset = tmp_cset;
                    dt->shared->u.vlen.pad  = tmp_strpad;

                    /* Set up VL information */
                    if (H5T_set_loc(dt, NULL, H5T_LOC_MEMORY) < 0)
                        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "invalid datatype location");
                } /* end if */
                else {
                    prec   = 8 * size;
                    offset = 0;
                } /* end else */
                break;

            case H5T_FLOAT:
                /*
                 * The sign, mantissa, and exponent fields should be adjusted
                 * first when decreasing the size of a floating point type.
                 */
                if (dt->shared->u.atomic.u.f.sign >= prec + offset ||
                    dt->shared->u.atomic.u.f.epos + dt->shared->u.atomic.u.f.esize > prec + offset ||
                    dt->shared->u.atomic.u.f.mpos + dt->shared->u.atomic.u.f.msize > prec + offset) {
                    HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                                "adjust sign, mantissa, and exponent fields first");
                }
                break;

            case H5T_ENUM:
            case H5T_VLEN:
            case H5T_ARRAY:
            case H5T_REFERENCE:
                assert("can't happen" && 0);
                break;

            case H5T_NO_CLASS:
            case H5T_NCLASSES:
                assert("invalid type" && 0);
                break;

            default:
                assert("not implemented yet" && 0);
                break;
        } /* end switch */

        /* Commit (if we didn't convert this type to a VL string) */
        if (dt->shared->type != H5T_VLEN) {
            dt->shared->size = size;
            if (H5T_IS_ATOMIC(dt->shared)) {
                dt->shared->u.atomic.offset = offset;
                dt->shared->u.atomic.prec   = prec;
            }
        } /* end if */

        /* Check if the new compound type is packed */
        if (dt->shared->type == H5T_COMPOUND)
            H5T__update_packed(dt);
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__set_size() */

/*-------------------------------------------------------------------------
 * Function:  H5T_get_size
 *
 * Purpose:   Determines the total size of a data type in bytes.
 *
 * Return:    Success:    Size of the data type in bytes.     The size of
 *                the data type is the size of an instance of
 *                that data type.
 *
 *            Failure:    0 (valid data types are never zero size)
 *
 *-------------------------------------------------------------------------
 */
size_t
H5T_get_size(const H5T_t *dt)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* check args */
    assert(dt);
    assert(dt->shared);

    FUNC_LEAVE_NOAPI(dt->shared->size)
} /* end H5T_get_size() */

/*-------------------------------------------------------------------------
 * Function:  H5T_get_force_conv
 *
 * Purpose:   Determines if the type has forced conversion. This will be
 *            true if and only if the type keeps a pointer to a file VOL
 *            object internally.
 *
 * Return:    true/false (never fails)
 *
 *-------------------------------------------------------------------------
 */
bool
H5T_get_force_conv(const H5T_t *dt)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* check args */
    assert(dt);
    assert(dt->shared);

    FUNC_LEAVE_NOAPI(dt->shared->force_conv)
} /* end H5T_get_force_conv() */

/*-------------------------------------------------------------------------
 * Function:  H5T_cmp
 *
 * Purpose:   Compares two data types.
 *
 * Return:    Success:    0 if DT1 and DT2 are equal.
 *                       <0 if DT1 is less than DT2.
 *                       >0 if DT1 is greater than DT2.
 *
 *            Failure:    0, never fails
 *
 *-------------------------------------------------------------------------
 */
int
H5T_cmp(const H5T_t *dt1, const H5T_t *dt2, bool superset)
{
    unsigned *idx1 = NULL, *idx2 = NULL;
    size_t    base_size;
    bool      swapped;
    unsigned  u;
    int       tmp;
    int       ret_value = 0;

    FUNC_ENTER_NOAPI(0)

    /* Sanity check */
    assert(dt1);
    assert(dt2);

    /* the easy case */
    if (dt1 == dt2)
        HGOTO_DONE(0);

    assert(dt1->shared);
    assert(dt2->shared);

    /* compare */
    if (dt1->shared->type < dt2->shared->type)
        HGOTO_DONE(-1);
    if (dt1->shared->type > dt2->shared->type)
        HGOTO_DONE(1);

    if (dt1->shared->size < dt2->shared->size)
        HGOTO_DONE(-1);
    if (dt1->shared->size > dt2->shared->size)
        HGOTO_DONE(1);

    if (dt1->shared->parent && !dt2->shared->parent)
        HGOTO_DONE(-1);
    if (!dt1->shared->parent && dt2->shared->parent)
        HGOTO_DONE(1);
    if (dt1->shared->parent) {
        tmp = H5T_cmp(dt1->shared->parent, dt2->shared->parent, superset);
        if (tmp < 0)
            HGOTO_DONE(-1);
        if (tmp > 0)
            HGOTO_DONE(1);
    } /* end if */

    switch (dt1->shared->type) {
        case H5T_COMPOUND:
            /*
             * Compound data types...
             */
            if (dt1->shared->u.compnd.nmembs < dt2->shared->u.compnd.nmembs)
                HGOTO_DONE(-1);
            if (dt1->shared->u.compnd.nmembs > dt2->shared->u.compnd.nmembs)
                HGOTO_DONE(1);

            /* Build an index for each type so the names are sorted */
            if (NULL == (idx1 = (unsigned *)H5MM_malloc(dt1->shared->u.compnd.nmembs * sizeof(unsigned))) ||
                NULL == (idx2 = (unsigned *)H5MM_malloc(dt2->shared->u.compnd.nmembs * sizeof(unsigned))))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, 0, "memory allocation failed");
            for (u = 0; u < dt1->shared->u.compnd.nmembs; u++)
                idx1[u] = idx2[u] = u;
            if (dt1->shared->u.enumer.nmembs > 1) {
                int i;

                for (i = (int)dt1->shared->u.compnd.nmembs - 1, swapped = true; swapped && i >= 0; --i) {
                    int j;

                    for (j = 0, swapped = false; j < i; j++)
                        if (strcmp(dt1->shared->u.compnd.memb[idx1[j]].name,
                                   dt1->shared->u.compnd.memb[idx1[j + 1]].name) > 0) {
                            unsigned tmp_idx = idx1[j];
                            idx1[j]          = idx1[j + 1];
                            idx1[j + 1]      = tmp_idx;
                            swapped          = true;
                        }
                }
                for (i = (int)dt2->shared->u.compnd.nmembs - 1, swapped = true; swapped && i >= 0; --i) {
                    int j;

                    for (j = 0, swapped = false; j < i; j++)
                        if (strcmp(dt2->shared->u.compnd.memb[idx2[j]].name,
                                   dt2->shared->u.compnd.memb[idx2[j + 1]].name) > 0) {
                            unsigned tmp_idx = idx2[j];
                            idx2[j]          = idx2[j + 1];
                            idx2[j + 1]      = tmp_idx;
                            swapped          = true;
                        }
                }
            } /* end if */

#ifdef H5T_DEBUG
            /* I don't quite trust the code above yet :-)  --RPM */
            for (u = 0; u < dt1->shared->u.compnd.nmembs - 1; u++) {
                assert(strcmp(dt1->shared->u.compnd.memb[idx1[u]].name,
                              dt1->shared->u.compnd.memb[idx1[u + 1]].name));
                assert(strcmp(dt2->shared->u.compnd.memb[idx2[u]].name,
                              dt2->shared->u.compnd.memb[idx2[u + 1]].name));
            }
#endif

            /* Compare the members */
            for (u = 0; u < dt1->shared->u.compnd.nmembs; u++) {
                tmp = strcmp(dt1->shared->u.compnd.memb[idx1[u]].name,
                             dt2->shared->u.compnd.memb[idx2[u]].name);
                if (tmp < 0)
                    HGOTO_DONE(-1);
                if (tmp > 0)
                    HGOTO_DONE(1);

                if (dt1->shared->u.compnd.memb[idx1[u]].offset < dt2->shared->u.compnd.memb[idx2[u]].offset)
                    HGOTO_DONE(-1);
                if (dt1->shared->u.compnd.memb[idx1[u]].offset > dt2->shared->u.compnd.memb[idx2[u]].offset)
                    HGOTO_DONE(1);

                if (dt1->shared->u.compnd.memb[idx1[u]].size < dt2->shared->u.compnd.memb[idx2[u]].size)
                    HGOTO_DONE(-1);
                if (dt1->shared->u.compnd.memb[idx1[u]].size > dt2->shared->u.compnd.memb[idx2[u]].size)
                    HGOTO_DONE(1);

                tmp = H5T_cmp(dt1->shared->u.compnd.memb[idx1[u]].type,
                              dt2->shared->u.compnd.memb[idx2[u]].type, superset);
                if (tmp < 0)
                    HGOTO_DONE(-1);
                if (tmp > 0)
                    HGOTO_DONE(1);
            }
            break;

        case H5T_ENUM:
            /*
             * Enumeration data types...
             */

            /* If we are doing a "superset" comparison, dt2 is allowed to have
             * more members than dt1
             */
            if (superset) {
                if (dt1->shared->u.enumer.nmembs > dt2->shared->u.enumer.nmembs)
                    HGOTO_DONE(1);
            } /* end if */
            else {
                if (dt1->shared->u.enumer.nmembs < dt2->shared->u.enumer.nmembs)
                    HGOTO_DONE(-1);
                if (dt1->shared->u.enumer.nmembs > dt2->shared->u.enumer.nmembs)
                    HGOTO_DONE(1);
            } /* end else */

            /* Build an index for each type so the names are sorted */
            if (NULL == (idx1 = (unsigned *)H5MM_malloc(dt1->shared->u.enumer.nmembs * sizeof(unsigned))) ||
                NULL == (idx2 = (unsigned *)H5MM_malloc(dt2->shared->u.enumer.nmembs * sizeof(unsigned))))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, 0, "memory allocation failed");
            for (u = 0; u < dt1->shared->u.enumer.nmembs; u++)
                idx1[u] = u;
            if (dt1->shared->u.enumer.nmembs > 1) {
                int i;
                for (i = (int)dt1->shared->u.enumer.nmembs - 1, swapped = true; swapped && i >= 0; --i) {
                    int j;

                    for (j = 0, swapped = false; j < i; j++)
                        if (strcmp(dt1->shared->u.enumer.name[idx1[j]],
                                   dt1->shared->u.enumer.name[idx1[j + 1]]) > 0) {
                            unsigned tmp_idx = idx1[j];
                            idx1[j]          = idx1[j + 1];
                            idx1[j + 1]      = tmp_idx;
                            swapped          = true;
                        }
                }
            }
            for (u = 0; u < dt2->shared->u.enumer.nmembs; u++)
                idx2[u] = u;
            if (dt2->shared->u.enumer.nmembs > 1) {
                int i;

                for (i = (int)dt2->shared->u.enumer.nmembs - 1, swapped = true; swapped && i >= 0; --i) {
                    int j;

                    for (j = 0, swapped = false; j < i; j++)
                        if (strcmp(dt2->shared->u.enumer.name[idx2[j]],
                                   dt2->shared->u.enumer.name[idx2[j + 1]]) > 0) {
                            unsigned tmp_idx = idx2[j];
                            idx2[j]          = idx2[j + 1];
                            idx2[j + 1]      = tmp_idx;
                            swapped          = true;
                        }
                }
            }

#ifdef H5T_DEBUG
            /* I don't quite trust the code above yet :-)  --RPM */
            for (u = 0; u < dt1->shared->u.enumer.nmembs - 1; u++) {
                assert(strcmp(dt1->shared->u.enumer.name[idx1[u]], dt1->shared->u.enumer.name[idx1[u + 1]]));
                assert(strcmp(dt2->shared->u.enumer.name[idx2[u]], dt2->shared->u.enumer.name[idx2[u + 1]]));
            }
#endif

            /* Compare the members */
            base_size = dt1->shared->parent->shared->size;
            for (u = 0; u < dt1->shared->u.enumer.nmembs; u++) {
                unsigned idx = 0;

                if (superset) {
                    unsigned lt  = 0, rt; /* Final, left & right key indices */
                    int      cmp = 1;     /* Key comparison value */

                    /* If a superset is allowed, dt2 may have more members
                     * than dt1, so binary search for matching member name in
                     * dt2
                     */
                    rt = dt2->shared->u.enumer.nmembs;

                    while (lt < rt && cmp) {
                        idx = (lt + rt) / 2;

                        /* compare */
                        if ((cmp = strcmp(dt1->shared->u.enumer.name[idx1[u]],
                                          dt2->shared->u.enumer.name[idx2[idx]])) < 0)
                            rt = idx;
                        else
                            lt = idx + 1;
                    }
                    /* Leave, if we couldn't find match */
                    if (cmp)
                        HGOTO_DONE(-1);
                } /* end if */
                else {
                    /* Check for exact member name match when not doing
                     * "superset" comparison
                     */
                    tmp = strcmp(dt1->shared->u.enumer.name[idx1[u]], dt2->shared->u.enumer.name[idx2[u]]);
                    if (tmp < 0)
                        HGOTO_DONE(-1);
                    if (tmp > 0)
                        HGOTO_DONE(1);

                    /* Set index value appropriately */
                    idx = u;
                } /* end else */

                tmp = memcmp((uint8_t *)dt1->shared->u.enumer.value + idx1[u] * base_size,
                             (uint8_t *)dt2->shared->u.enumer.value + idx2[idx] * base_size, base_size);
                if (tmp < 0)
                    HGOTO_DONE(-1);
                if (tmp > 0)
                    HGOTO_DONE(1);
            }
            break;

        case H5T_VLEN:
            assert(dt1->shared->u.vlen.type > H5T_VLEN_BADTYPE &&
                   dt1->shared->u.vlen.type < H5T_VLEN_MAXTYPE);
            assert(dt2->shared->u.vlen.type > H5T_VLEN_BADTYPE &&
                   dt2->shared->u.vlen.type < H5T_VLEN_MAXTYPE);
            assert(dt1->shared->u.vlen.loc >= H5T_LOC_BADLOC && dt1->shared->u.vlen.loc < H5T_LOC_MAXLOC);
            assert(dt2->shared->u.vlen.loc >= H5T_LOC_BADLOC && dt2->shared->u.vlen.loc < H5T_LOC_MAXLOC);

            /* Arbitrarily sort sequence VL datatypes before string VL datatypes */
            if (dt1->shared->u.vlen.type == H5T_VLEN_SEQUENCE &&
                dt2->shared->u.vlen.type == H5T_VLEN_STRING) {
                HGOTO_DONE(-1);
            }
            else if (dt1->shared->u.vlen.type == H5T_VLEN_STRING &&
                     dt2->shared->u.vlen.type == H5T_VLEN_SEQUENCE) {
                HGOTO_DONE(1);
            }
            /* Arbitrarily sort VL datatypes in memory before disk */
            if (dt1->shared->u.vlen.loc == H5T_LOC_MEMORY && dt2->shared->u.vlen.loc == H5T_LOC_DISK) {
                HGOTO_DONE(-1);
            }
            else if (dt1->shared->u.vlen.loc == H5T_LOC_DISK && dt2->shared->u.vlen.loc == H5T_LOC_MEMORY) {
                HGOTO_DONE(1);
            }
            else if (dt1->shared->u.vlen.loc == H5T_LOC_BADLOC && dt2->shared->u.vlen.loc != H5T_LOC_BADLOC) {
                HGOTO_DONE(1);
            }

            /* Don't allow VL types in different files to compare as equal */
            if (dt1->shared->u.vlen.file < dt2->shared->u.vlen.file)
                HGOTO_DONE(-1);
            if (dt1->shared->u.vlen.file > dt2->shared->u.vlen.file)
                HGOTO_DONE(1);
            break;

        case H5T_OPAQUE:
            if (dt1->shared->u.opaque.tag && dt2->shared->u.opaque.tag)
                HGOTO_DONE(strcmp(dt1->shared->u.opaque.tag, dt2->shared->u.opaque.tag));
            break;

        case H5T_ARRAY:
            if (dt1->shared->u.array.ndims < dt2->shared->u.array.ndims)
                HGOTO_DONE(-1);
            if (dt1->shared->u.array.ndims > dt2->shared->u.array.ndims)
                HGOTO_DONE(1);

            for (u = 0; u < dt1->shared->u.array.ndims; u++) {
                if (dt1->shared->u.array.dim[u] < dt2->shared->u.array.dim[u])
                    HGOTO_DONE(-1);
                if (dt1->shared->u.array.dim[u] > dt2->shared->u.array.dim[u])
                    HGOTO_DONE(1);
            }

            tmp = H5T_cmp(dt1->shared->parent, dt2->shared->parent, superset);
            if (tmp < 0)
                HGOTO_DONE(-1);
            if (tmp > 0)
                HGOTO_DONE(1);
            break;

        case H5T_NO_CLASS:
        case H5T_INTEGER:
        case H5T_FLOAT:
        case H5T_TIME:
        case H5T_STRING:
        case H5T_BITFIELD:
        case H5T_REFERENCE:
        case H5T_NCLASSES:
        default:
            /*
             * Atomic datatypes...
             */
            if (dt1->shared->u.atomic.order < dt2->shared->u.atomic.order)
                HGOTO_DONE(-1);
            if (dt1->shared->u.atomic.order > dt2->shared->u.atomic.order)
                HGOTO_DONE(1);

            if (dt1->shared->u.atomic.prec < dt2->shared->u.atomic.prec)
                HGOTO_DONE(-1);
            if (dt1->shared->u.atomic.prec > dt2->shared->u.atomic.prec)
                HGOTO_DONE(1);

            if (dt1->shared->u.atomic.offset < dt2->shared->u.atomic.offset)
                HGOTO_DONE(-1);
            if (dt1->shared->u.atomic.offset > dt2->shared->u.atomic.offset)
                HGOTO_DONE(1);

            if (dt1->shared->u.atomic.lsb_pad < dt2->shared->u.atomic.lsb_pad)
                HGOTO_DONE(-1);
            if (dt1->shared->u.atomic.lsb_pad > dt2->shared->u.atomic.lsb_pad)
                HGOTO_DONE(1);

            if (dt1->shared->u.atomic.msb_pad < dt2->shared->u.atomic.msb_pad)
                HGOTO_DONE(-1);
            if (dt1->shared->u.atomic.msb_pad > dt2->shared->u.atomic.msb_pad)
                HGOTO_DONE(1);

            switch (dt1->shared->type) {
                case H5T_INTEGER:
                    if (dt1->shared->u.atomic.u.i.sign < dt2->shared->u.atomic.u.i.sign)
                        HGOTO_DONE(-1);
                    if (dt1->shared->u.atomic.u.i.sign > dt2->shared->u.atomic.u.i.sign)
                        HGOTO_DONE(1);
                    break;

                case H5T_FLOAT:
                    if (dt1->shared->u.atomic.u.f.sign < dt2->shared->u.atomic.u.f.sign)
                        HGOTO_DONE(-1);
                    if (dt1->shared->u.atomic.u.f.sign > dt2->shared->u.atomic.u.f.sign)
                        HGOTO_DONE(1);

                    if (dt1->shared->u.atomic.u.f.epos < dt2->shared->u.atomic.u.f.epos)
                        HGOTO_DONE(-1);
                    if (dt1->shared->u.atomic.u.f.epos > dt2->shared->u.atomic.u.f.epos)
                        HGOTO_DONE(1);

                    if (dt1->shared->u.atomic.u.f.esize < dt2->shared->u.atomic.u.f.esize)
                        HGOTO_DONE(-1);
                    if (dt1->shared->u.atomic.u.f.esize > dt2->shared->u.atomic.u.f.esize)
                        HGOTO_DONE(1);

                    if (dt1->shared->u.atomic.u.f.ebias < dt2->shared->u.atomic.u.f.ebias)
                        HGOTO_DONE(-1);
                    if (dt1->shared->u.atomic.u.f.ebias > dt2->shared->u.atomic.u.f.ebias)
                        HGOTO_DONE(1);

                    if (dt1->shared->u.atomic.u.f.mpos < dt2->shared->u.atomic.u.f.mpos)
                        HGOTO_DONE(-1);
                    if (dt1->shared->u.atomic.u.f.mpos > dt2->shared->u.atomic.u.f.mpos)
                        HGOTO_DONE(1);

                    if (dt1->shared->u.atomic.u.f.msize < dt2->shared->u.atomic.u.f.msize)
                        HGOTO_DONE(-1);
                    if (dt1->shared->u.atomic.u.f.msize > dt2->shared->u.atomic.u.f.msize)
                        HGOTO_DONE(1);

                    if (dt1->shared->u.atomic.u.f.norm < dt2->shared->u.atomic.u.f.norm)
                        HGOTO_DONE(-1);
                    if (dt1->shared->u.atomic.u.f.norm > dt2->shared->u.atomic.u.f.norm)
                        HGOTO_DONE(1);

                    if (dt1->shared->u.atomic.u.f.pad < dt2->shared->u.atomic.u.f.pad)
                        HGOTO_DONE(-1);
                    if (dt1->shared->u.atomic.u.f.pad > dt2->shared->u.atomic.u.f.pad)
                        HGOTO_DONE(1);

                    break;

                case H5T_TIME: /* order and precision are checked above */
                    /*void */
                    break;

                case H5T_STRING:
                    if (dt1->shared->u.atomic.u.s.cset < dt2->shared->u.atomic.u.s.cset)
                        HGOTO_DONE(-1);
                    if (dt1->shared->u.atomic.u.s.cset > dt2->shared->u.atomic.u.s.cset)
                        HGOTO_DONE(1);

                    if (dt1->shared->u.atomic.u.s.pad < dt2->shared->u.atomic.u.s.pad)
                        HGOTO_DONE(-1);
                    if (dt1->shared->u.atomic.u.s.pad > dt2->shared->u.atomic.u.s.pad)
                        HGOTO_DONE(1);

                    break;

                case H5T_BITFIELD:
                    /*void */
                    break;

                case H5T_REFERENCE:
                    if (dt1->shared->u.atomic.u.r.rtype < dt2->shared->u.atomic.u.r.rtype)
                        HGOTO_DONE(-1);
                    if (dt1->shared->u.atomic.u.r.rtype > dt2->shared->u.atomic.u.r.rtype)
                        HGOTO_DONE(1);
                    if (dt1->shared->u.atomic.u.r.loc < dt2->shared->u.atomic.u.r.loc)
                        HGOTO_DONE(-1);
                    if (dt1->shared->u.atomic.u.r.loc > dt2->shared->u.atomic.u.r.loc)
                        HGOTO_DONE(1);
                    if (dt1->shared->u.atomic.u.r.file < dt2->shared->u.atomic.u.r.file)
                        HGOTO_DONE(-1);
                    if (dt1->shared->u.atomic.u.r.file > dt2->shared->u.atomic.u.r.file)
                        HGOTO_DONE(1);
                    break;

                case H5T_NO_CLASS:
                case H5T_OPAQUE:
                case H5T_COMPOUND:
                case H5T_ENUM:
                case H5T_VLEN:
                case H5T_ARRAY:
                case H5T_NCLASSES:
                default:
                    assert("not implemented yet" && 0);
                    break;
            }
            break;
    } /* end switch */

done:
    if (NULL != idx1)
        H5MM_xfree(idx1);
    if (NULL != idx2)
        H5MM_xfree(idx2);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_cmp() */

/*-------------------------------------------------------------------------
 * Function:    H5T_path_find
 *
 * Purpose:    Library-internal wrapper to find the path which converts type
 *              SRC_ID to type DST_ID.
 *
 *              If SRC and DST are both null pointers then the special no-op
 *              conversion path is used.
 *
 * Return:    Success:    Pointer to the path, valid until the path
 *                        database is modified.
 *
 *            Failure:    NULL if the path does not exist and no
 *                        function can be found to apply to the new path.
 *
 *-------------------------------------------------------------------------
 */
H5T_path_t *
H5T_path_find(const H5T_t *src, const H5T_t *dst)
{
    H5T_conv_func_t conv_func;        /* Conversion function wrapper */
    H5T_path_t     *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /* Sanity check */
    assert(src);
    assert(src->shared);
    assert(dst);
    assert(dst->shared);

    /* Set up conversion function wrapper */
    conv_func.is_app     = false;
    conv_func.u.lib_func = NULL;

    /* Call the internal routine, with additional parameters */
    if (NULL == (ret_value = H5T__path_find_real(src, dst, NULL, &conv_func)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, NULL, "can't find datatype conversion path");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_path_find() */

/*-------------------------------------------------------------------------
 * Function:    H5T__path_find_real
 *
 * Purpose:    Finds the path which converts type SRC_ID to type DST_ID,
 *        creating a new path if necessary.  If FUNC is non-zero then
 *        it is set as the hard conversion function for that path
 *        regardless of whether the path previously existed. Changing
 *        the conversion function of a path causes statistics to be
 *        reset to zero after printing them.  The NAME is used only
 *        when creating a new path and is just for debugging.
 *
 *        If SRC and DST are both null pointers then the special no-op
 *        conversion path is used.  This path is always stored as the
 *        first path in the path table.
 *
 * Return:    Success:    Pointer to the path, valid until the path
 *                        database is modified.
 *
 *            Failure:    NULL if the path does not exist and no
 *                        function can be found to apply to the new path.
 *
 *-------------------------------------------------------------------------
 */
static H5T_path_t *
H5T__path_find_real(const H5T_t *src, const H5T_t *dst, const char *name, H5T_conv_func_t *conv)
{
    int         lt, rt;                   /* left and right edges */
    int         md;                       /* middle */
    int         cmp;                      /* comparison result  */
    int         old_npaths;               /* Previous number of paths in table */
    H5T_path_t *table  = NULL;            /* path existing in the table */
    H5T_path_t *path   = NULL;            /* new path */
    hid_t       src_id = -1, dst_id = -1; /* src and dst type identifiers */
    int         i;                        /* counter */
    int         nprint    = 0;            /* lines of output printed */
    H5T_path_t *ret_value = NULL;         /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(src);
    assert(src->shared);
    assert(dst);
    assert(dst->shared);

    /*
     * Make sure the first entry in the table is the no-op conversion path.
     */
    if (0 == H5T_g.npaths) {
        if (NULL == (H5T_g.path = (H5T_path_t **)H5MM_malloc(128 * sizeof(H5T_path_t *))))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL,
                        "memory allocation failed for type conversion path table");
        H5T_g.apaths = 128;
        if (NULL == (H5T_g.path[0] = H5FL_CALLOC(H5T_path_t)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL,
                        "memory allocation failed for no-op conversion path");
        snprintf(H5T_g.path[0]->name, sizeof(H5T_g.path[0]->name), "no-op");
        H5T_g.path[0]->conv.is_app     = false;
        H5T_g.path[0]->conv.u.lib_func = H5T__conv_noop;
        H5T_g.path[0]->cdata.command   = H5T_CONV_INIT;
        if (H5T__conv_noop((hid_t)FAIL, (hid_t)FAIL, &(H5T_g.path[0]->cdata), (size_t)0, (size_t)0, (size_t)0,
                           NULL, NULL) < 0) {
#ifdef H5T_DEBUG
            if (H5DEBUG(T))
                fprintf(H5DEBUG(T), "H5T: unable to initialize no-op conversion function (ignored)\n");
#endif
            H5E_clear_stack(NULL); /*ignore the error*/
        }                          /* end if */
        H5T_g.path[0]->is_noop = true;
        H5T_g.npaths           = 1;
    } /* end if */

    /* Find the conversion path.  If source and destination types are equal
     * then use entry[0], otherwise do a binary search over the
     * remaining entries.
     *
     * Only allow the no-op conversion to occur if no "force conversion" flags
     * are set
     */
    if (src->shared->force_conv == false && dst->shared->force_conv == false &&
        0 == H5T_cmp(src, dst, true)) {
        table = H5T_g.path[0];
        cmp   = 0;
        md    = 0;
    } /* end if */
    else {
        lt = md = 1;
        rt      = H5T_g.npaths;
        cmp     = -1;

        while (cmp && lt < rt) {
            md = (lt + rt) / 2;
            assert(H5T_g.path[md]);
            cmp = H5T_cmp(src, H5T_g.path[md]->src, false);
            if (0 == cmp)
                cmp = H5T_cmp(dst, H5T_g.path[md]->dst, false);
            if (cmp < 0)
                rt = md;
            else if (cmp > 0)
                lt = md + 1;
            else
                table = H5T_g.path[md];
        } /* end while */
    }     /* end else */

    /* Keep a record of the number of paths in the table, in case one of the
     * initialization calls below (hard or soft) causes more entries to be
     * added to the table - QAK, 1/26/02
     */
    old_npaths = H5T_g.npaths;

    /* If we didn't find the path, if the caller is an API function specifying
     * a new hard conversion function, or if the caller is a private function
     * specifying a new hard conversion and the path is a soft conversion, then
     * create a new path and add the new function to the path.
     */
    if (!table || (table && conv->is_app && conv->u.app_func) ||
        (table && !table->is_hard && !conv->is_app && conv->u.lib_func)) {
        if (NULL == (path = H5FL_CALLOC(H5T_path_t)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed for type conversion path");
        if (name && *name) {
            strncpy(path->name, name, (size_t)H5T_NAMELEN);
            path->name[H5T_NAMELEN - 1] = '\0';
        } /* end if */
        else
            snprintf(path->name, sizeof(path->name), "NONAME");
        if (NULL == (path->src = H5T_copy(src, H5T_COPY_ALL)))
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "unable to copy datatype for conversion path");
        if (NULL == (path->dst = H5T_copy(dst, H5T_COPY_ALL)))
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "unable to copy datatype for conversion path");
    } /* end if */
    else
        path = table;

    /* If a hard conversion function is specified and none is defined for the
     * path, or the caller is an API function, or the caller is a private function but
     * the existing path is a soft function, then add the new conversion to the path
     * and initialize its conversion data.
     */
    if (conv->u.app_func &&
        (!table || (table && conv->is_app) || (table && !table->is_hard && !conv->is_app))) {
        assert(path != table);
        assert(NULL == path->conv.u.app_func);
        if (path->src && (src_id = H5I_register(H5I_DATATYPE, H5T_copy(path->src, H5T_COPY_ALL), false)) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREGISTER, NULL,
                        "unable to register source conversion type for query");
        if (path->dst && (dst_id = H5I_register(H5I_DATATYPE, H5T_copy(path->dst, H5T_COPY_ALL), false)) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREGISTER, NULL,
                        "unable to register destination conversion type for query");
        path->cdata.command = H5T_CONV_INIT;
        if (conv->is_app) {
            if ((conv->u.app_func)(src_id, dst_id, &(path->cdata), (size_t)0, (size_t)0, (size_t)0, NULL,
                                   NULL, H5CX_get_dxpl()) < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "unable to initialize conversion function");
        } /* end if */
        else if ((conv->u.lib_func)(src_id, dst_id, &(path->cdata), (size_t)0, (size_t)0, (size_t)0, NULL,
                                    NULL) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "unable to initialize conversion function");
        if (src_id >= 0)
            H5I_dec_ref(src_id);
        if (dst_id >= 0)
            H5I_dec_ref(dst_id);
        src_id = dst_id = -1;
        path->conv      = *conv;
        path->is_hard   = true;
    } /* end if */

    /*
     * If the path doesn't have a function by now (because it's a new path
     * and the caller didn't supply a hard function) then scan the soft list
     * for an applicable function and add it to the path.  This can't happen
     * for the no-op conversion path.
     */
    assert(path->conv.u.app_func || (src && dst));
    for (i = H5T_g.nsoft - 1; i >= 0 && !path->conv.u.app_func; --i) {
        bool path_init_error = false;

        if (src->shared->type != H5T_g.soft[i].src || dst->shared->type != H5T_g.soft[i].dst)
            continue;
        if ((src_id = H5I_register(H5I_DATATYPE, H5T_copy(path->src, H5T_COPY_ALL), false)) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREGISTER, NULL,
                        "unable to register src conversion type for query");
        if ((dst_id = H5I_register(H5I_DATATYPE, H5T_copy(path->dst, H5T_COPY_ALL), false)) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREGISTER, NULL,
                        "unable to register dst conversion type for query");
        path->cdata.command = H5T_CONV_INIT;
        if (H5T_g.soft[i].conv.is_app) {
            if ((H5T_g.soft[i].conv.u.app_func)(src_id, dst_id, &(path->cdata), (size_t)0, (size_t)0,
                                                (size_t)0, NULL, NULL, H5CX_get_dxpl()) < 0) {
                memset(&(path->cdata), 0, sizeof(H5T_cdata_t));
                H5E_clear_stack(NULL); /*ignore the error*/
                path_init_error = true;
            } /* end if */
        }     /* end if */
        else if ((H5T_g.soft[i].conv.u.lib_func)(src_id, dst_id, &(path->cdata), (size_t)0, (size_t)0,
                                                 (size_t)0, NULL, NULL) < 0) {
            memset(&(path->cdata), 0, sizeof(H5T_cdata_t));
            H5E_clear_stack(NULL); /*ignore the error*/
            path_init_error = true;
        } /* end if */

        /* Finish operation, if no error */
        if (!path_init_error) {
            strncpy(path->name, H5T_g.soft[i].name, (size_t)H5T_NAMELEN);
            path->name[H5T_NAMELEN - 1] = '\0';
            path->conv                  = H5T_g.soft[i].conv;
            path->is_hard               = false;
        } /* end else */
        H5I_dec_ref(src_id);
        H5I_dec_ref(dst_id);
        src_id = dst_id = -1;
    } /* end for */
    if (!path->conv.u.app_func)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "no appropriate function for conversion path");

    /* Check if paths were inserted into the table through a recursive call
     * and re-compute the correct location for this path if so. - QAK, 1/26/02
     */
    if (old_npaths != H5T_g.npaths) {
        lt = md = 1;
        rt      = H5T_g.npaths;
        cmp     = -1;

        while (cmp && lt < rt) {
            md = (lt + rt) / 2;
            assert(H5T_g.path[md]);
            cmp = H5T_cmp(src, H5T_g.path[md]->src, false);
            if (0 == cmp)
                cmp = H5T_cmp(dst, H5T_g.path[md]->dst, false);
            if (cmp < 0)
                rt = md;
            else if (cmp > 0)
                lt = md + 1;
            else
                table = H5T_g.path[md];
        } /* end while */
    }     /* end if */

    /* Replace an existing table entry or add a new entry */
    if (table && path != table) {
        assert(table == H5T_g.path[md]);
        H5T__print_stats(table, &nprint /*in,out*/);
        table->cdata.command = H5T_CONV_FREE;
        if (table->conv.is_app) {
            if ((table->conv.u.app_func)((hid_t)FAIL, (hid_t)FAIL, &(table->cdata), (size_t)0, (size_t)0,
                                         (size_t)0, NULL, NULL, H5CX_get_dxpl()) < 0) {
#ifdef H5T_DEBUG
                if (H5DEBUG(T))
                    fprintf(H5DEBUG(T), "H5T: conversion function 0x%016zx free failed for %s (ignored)\n",
                            (size_t)path->conv.u.app_func, path->name);
#endif
                H5E_clear_stack(NULL); /*ignore the failure*/
            }                          /* end if */
        }                              /* end if */
        else if ((table->conv.u.lib_func)((hid_t)FAIL, (hid_t)FAIL, &(table->cdata), (size_t)0, (size_t)0,
                                          (size_t)0, NULL, NULL) < 0) {
#ifdef H5T_DEBUG
            if (H5DEBUG(T))
                fprintf(H5DEBUG(T), "H5T: conversion function 0x%016zx free failed for %s (ignored)\n",
                        (size_t)path->conv.u.lib_func, path->name);
#endif
            H5E_clear_stack(NULL); /*ignore the failure*/
        }                          /* end if */
        if (table->src)
            (void)H5T_close_real(table->src);
        if (table->dst)
            (void)H5T_close_real(table->dst);
        table          = H5FL_FREE(H5T_path_t, table);
        table          = path;
        H5T_g.path[md] = path;
    } /* end if */
    else if (path != table) {
        assert(cmp);
        if ((size_t)H5T_g.npaths >= H5T_g.apaths) {
            size_t       na = MAX(128, 2 * H5T_g.apaths);
            H5T_path_t **x;

            if (NULL == (x = (H5T_path_t **)H5MM_realloc(H5T_g.path, na * sizeof(H5T_path_t *))))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");
            H5T_g.apaths = na;
            H5T_g.path   = x;
        } /* end if */
        if (cmp > 0)
            md++;
        memmove(H5T_g.path + md + 1, H5T_g.path + md, (size_t)(H5T_g.npaths - md) * sizeof(H5T_path_t *));
        H5T_g.npaths++;
        H5T_g.path[md] = path;
        table          = path;
    } /* end else-if */

    /* Set the flag to indicate both source and destination types are compound types
     * for the optimization of data reading (in H5Dio.c).
     * Make sure that path->are_compounds is only true for compound types.
     */
    path->are_compounds = false;
    if (H5T_COMPOUND == H5T_get_class(src, true) && H5T_COMPOUND == H5T_get_class(dst, true))
        path->are_compounds = true;

    /* Set return value */
    ret_value = path;

done:
    if (!ret_value && path && path != table) {
        if (path->src)
            (void)H5T_close_real(path->src);
        if (path->dst)
            (void)H5T_close_real(path->dst);
        path = H5FL_FREE(H5T_path_t, path);
    } /* end if */
    if (src_id >= 0)
        H5I_dec_ref(src_id);
    if (dst_id >= 0)
        H5I_dec_ref(dst_id);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__path_find_real() */

/*-------------------------------------------------------------------------
 * Function:  H5T_path_noop
 *
 * Purpose:   Is the path the special no-op path? The no-op function can be
 *            set by the application and there might be more than one no-op
 *            path in a multi-threaded application if one thread is using
 *            the no-op path when some other thread changes its definition.
 *
 * Return:    true/false (can't fail)
 *
 *-------------------------------------------------------------------------
 */
bool
H5T_path_noop(const H5T_path_t *p)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(p);

    FUNC_LEAVE_NOAPI(p->is_noop || (p->is_hard && 0 == H5T_cmp(p->src, p->dst, false)))
} /* end H5T_path_noop() */

/*-------------------------------------------------------------------------
 * Function:  H5T_path_compound_subset
 *
 * Purpose:   Checks if the source and destination types are both compound.
 *            Tells whether whether the source members are a subset of
 *            destination, and the order is the same, and no conversion
 *            is needed.  For example:
 *                  struct source {            struct destination {
 *                      TYPE1 A;      -->          TYPE1 A;
 *                      TYPE2 B;      -->          TYPE2 B;
 *                      TYPE3 C;      -->          TYPE3 C;
 *                  };                             TYPE4 D;
 *                                                 TYPE5 E;
 *                                             };
 *
 * Return:    A pointer to the subset info struct in p, or NULL if there are
 *            no compounds.  Points directly into the H5T_path_t structure.
 *
 *-------------------------------------------------------------------------
 */
H5T_subset_info_t *
H5T_path_compound_subset(const H5T_path_t *p)
{
    H5T_subset_info_t *ret_value = NULL;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(p);

    if (p->are_compounds)
        ret_value = H5T__conv_struct_subset(&(p->cdata));

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_path_compound_subset */

/*-------------------------------------------------------------------------
 * Function:  H5T_path_bkg
 *
 * Purpose:   Get the "background" flag for the conversion path.
 *
 * Return:    Background flag (can't fail)
 *
 *-------------------------------------------------------------------------
 */
H5T_bkg_t
H5T_path_bkg(const H5T_path_t *p)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(p);

    FUNC_LEAVE_NOAPI(p->cdata.need_bkg)
} /* end H5T_path_bkg() */

/*-------------------------------------------------------------------------
 * Function:  H5T__compiler_conv
 *
 * Purpose:   Private function for H5Tcompiler_conv.  Finds out whether the
 *            library's conversion function from type SRC to type DST
 *            is a hard conversion.
 *
 * Return:    true:           hard conversion.
 *            false:          soft conversion.
 *            FAIL:           function failed.
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5T__compiler_conv(H5T_t *src, H5T_t *dst)
{
    H5T_path_t *path;
    htri_t      ret_value = FAIL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Find it */
    if (NULL == (path = H5T_path_find(src, dst)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_NOTFOUND, FAIL, "conversion function not found");

    ret_value = (htri_t)path->is_hard;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__compiler_conv() */

/*-------------------------------------------------------------------------
 * Function:  H5T_convert
 *
 * Purpose:   Call a conversion function to convert from source to
 *            destination data type and accumulate timing statistics.
 *
 * Return:    Success:    non-negative
 *
 *            Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5T_convert(H5T_path_t *tpath, hid_t src_id, hid_t dst_id, size_t nelmts, size_t buf_stride,
            size_t bkg_stride, void *buf, void *bkg)
{
#ifdef H5T_DEBUG
    H5_timer_t timer; /* Timer for conversion */
#endif
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

#ifdef H5T_DEBUG
    if (H5DEBUG(T)) {
        /* Initialize and start timer */
        H5_timer_init(&timer);
        H5_timer_start(&timer);
    } /* end if */
#endif

    /* Call the appropriate conversion callback */
    tpath->cdata.command = H5T_CONV_CONV;
    if (tpath->conv.is_app) {
        if ((tpath->conv.u.app_func)(src_id, dst_id, &(tpath->cdata), nelmts, buf_stride, bkg_stride, buf,
                                     bkg, H5CX_get_dxpl()) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCONVERT, FAIL, "datatype conversion failed");
    } /* end if */
    else if ((tpath->conv.u.lib_func)(src_id, dst_id, &(tpath->cdata), nelmts, buf_stride, bkg_stride, buf,
                                      bkg) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCONVERT, FAIL, "datatype conversion failed");
#ifdef H5T_DEBUG
    if (H5DEBUG(T)) {
        /* Stop timer */
        H5_timer_stop(&timer);

        /* Record elapsed timer info */
        H5_timer_get_times(timer, &tpath->stats.times);

        /* Increment # of calls and # of elements converted */
        tpath->stats.ncalls++;
        tpath->stats.nelmts += nelmts;
    } /* end if */
#endif

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_convert() */

/*-------------------------------------------------------------------------
 * Function:  H5T_oloc
 *
 * Purpose:   Returns a pointer to the object location for a named datatype.
 *
 * Return:    Success:    Ptr directly into named datatype
 *            Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
H5O_loc_t *
H5T_oloc(H5T_t *dt)
{
    H5O_loc_t *ret_value = NULL;

    FUNC_ENTER_NOAPI(NULL)

    assert(dt);

    switch (dt->shared->state) {
        case H5T_STATE_TRANSIENT:
        case H5T_STATE_RDONLY:
        case H5T_STATE_IMMUTABLE:
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "not a named datatype");
        case H5T_STATE_NAMED:
        case H5T_STATE_OPEN:
            assert(dt->sh_loc.type == H5O_SHARE_TYPE_COMMITTED);
            ret_value = &dt->oloc;
            break;
        default:
            HGOTO_ERROR(H5E_DATATYPE, H5E_BADTYPE, NULL, "invalid datatype state");
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_oloc() */

/*-------------------------------------------------------------------------
 * Function:  H5T_nameof
 *
 * Purpose:   Returns a pointer to the path for a named datatype.
 *
 * Return:    Success:    Ptr directly into named datatype
 *            Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
H5G_name_t *
H5T_nameof(H5T_t *dt)
{
    H5G_name_t *ret_value = NULL;

    FUNC_ENTER_NOAPI(NULL)

    assert(dt);

    switch (dt->shared->state) {
        case H5T_STATE_TRANSIENT:
        case H5T_STATE_RDONLY:
        case H5T_STATE_IMMUTABLE:
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "not a named datatype");
        case H5T_STATE_NAMED:
        case H5T_STATE_OPEN:
            ret_value = &(dt->path);
            break;
        default:
            HGOTO_ERROR(H5E_DATATYPE, H5E_BADTYPE, NULL, "invalid datatype state");
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_nameof() */

/*-------------------------------------------------------------------------
 * Function:    H5T_is_immutable
 *
 * Purpose:     Check if a datatype is immutable.
 *
 * Return:      true
 *
 *              false
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5T_is_immutable(const H5T_t *dt)
{
    htri_t ret_value = false;

    FUNC_ENTER_NOAPI_NOERR

    assert(dt);

    if (dt->shared->state == H5T_STATE_IMMUTABLE)
        ret_value = true;

    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5T_is_named
 *
 * Purpose:     Check if a datatype is named/committed.
 *
 * Return:      true/false/FAIL
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5T_is_named(const H5T_t *dt)
{
    htri_t ret_value = false;

    FUNC_ENTER_NOAPI_NOERR

    assert(dt);

    if (dt->vol_obj)
        ret_value = true;
    else
        ret_value = (H5T_STATE_OPEN == dt->shared->state || H5T_STATE_NAMED == dt->shared->state);

    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5T_convert_committed_datatype
 *
 * Purpose:     To convert the committed datatype "dt" to a transient embedded
 *        type if the file location associated with the committed datatype is
 *        different from the parameter "f".
 *        "f" is the file location where the dataset or attribute will be created.
 *
 * Notes:       See HDFFV-9940
 *
 * Return:      Success:        non-negative
 *              Failure:        negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5T_convert_committed_datatype(H5T_t *dt, H5F_t *f)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    assert(dt);
    assert(f);

    if (H5T_is_named(dt) && (dt->sh_loc.file != f)) {
        assert(dt->sh_loc.type == H5O_SHARE_TYPE_COMMITTED);

        H5O_msg_reset_share(H5O_DTYPE_ID, dt);
        if (H5O_loc_free(&dt->oloc) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTRESET, FAIL, "unable to initialize location");
        if (H5G_name_free(&dt->path) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, FAIL, "unable to reset path");

        /* If the datatype is committed through the VOL, close it */
        if (NULL != dt->vol_obj) {
            H5VL_object_t *vol_obj = dt->vol_obj;

            /* Close the datatype through the VOL*/
            if (H5VL_datatype_close(vol_obj, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, FAIL, "unable to close datatype");

            /* Free the datatype and set the VOL object pointer to NULL */
            if (H5VL_free_object(vol_obj) < 0)
                HGOTO_ERROR(H5E_ATTR, H5E_CANTDEC, FAIL, "unable to free VOL object");
            dt->vol_obj = NULL;
        } /* end if */

        dt->shared->state = H5T_STATE_TRANSIENT;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_convert_committed_datatype() */

/*--------------------------------------------------------------------------
 * Function:    H5T_get_ref_type
 *
 * Purpose:     Retrieves the type of reference for a datatype
 *              H5T_t *dt;  IN: datatype pointer for the reference datatype
 *
 * Return:      Success:        A reference type defined in H5Rpublic.h
 *              Failure:        H5R_BADTYPE
 * Notes:       Given a reference datatype object, this function returns the reference type
 *              of the datatype.
 *--------------------------------------------------------------------------
 */
H5R_type_t
H5T_get_ref_type(const H5T_t *dt)
{
    H5R_type_t ret_value = H5R_BADTYPE;

    FUNC_ENTER_NOAPI_NOERR

    assert(dt);

    if (dt->shared->type == H5T_REFERENCE)
        ret_value = dt->shared->u.atomic.u.r.rtype;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_get_ref_type() */

/*-------------------------------------------------------------------------
 * Function:  H5T_is_sensible
 *
 * Purpose:   Determines if a data type is sensible to store on disk
 *            (i.e. not partially initialized)
 *
 * Return:    Success:    true, false
 *
 *            Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5T_is_sensible(const H5T_t *dt)
{
    htri_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    assert(dt);

    switch (dt->shared->type) {
        case H5T_COMPOUND:
            /* Only allow compound datatypes with at least one member to be stored on disk */
            if (dt->shared->u.compnd.nmembs > 0)
                ret_value = true;
            else
                ret_value = false;
            break;

        case H5T_ENUM:
            /* Only allow enum datatypes with at least one member to be stored on disk */
            if (dt->shared->u.enumer.nmembs > 0)
                ret_value = true;
            else
                ret_value = false;
            break;

        case H5T_NO_CLASS:
        case H5T_INTEGER:
        case H5T_FLOAT:
        case H5T_TIME:
        case H5T_STRING:
        case H5T_BITFIELD:
        case H5T_OPAQUE:
        case H5T_REFERENCE:
        case H5T_VLEN:
        case H5T_ARRAY:
        case H5T_NCLASSES:
        default:
            /* Assume all other datatype are sensible to store on disk */
            ret_value = true;
            break;
    } /* end switch */

    FUNC_LEAVE_NOAPI(ret_value)
}

/*--------------------------------------------------------------------------
 NAME
    H5T_set_loc
 PURPOSE
    Recursively mark any datatypes as on disk/in memory
 USAGE
    htri_t H5T_set_loc(dt,f,loc)
        H5T_t *dt;              IN/OUT: Pointer to the datatype to mark
        H5F_t *f;               IN: Pointer to the file the datatype is in
        H5T_loc_t loc           IN: location of type

 RETURNS
    One of two values on success:
        true - If the location of any vlen types changed
        false - If the location of any vlen types is the same
    <0 is returned on failure
 DESCRIPTION
    Recursively descends any VL or compound datatypes to mark all VL datatypes
    as either on disk or in memory.
 --------------------------------------------------------------------------
 */
htri_t
H5T_set_loc(H5T_t *dt, H5VL_object_t *file, H5T_loc_t loc)
{
    htri_t   changed;       /* Whether H5T_set_loc changed the type (even if the size didn't change) */
    htri_t   ret_value = 0; /* Indicate that success, but no location change */
    unsigned i;             /* Local index variable */
    size_t   old_size;      /* Previous size of a field */

    FUNC_ENTER_NOAPI(FAIL)

    assert(dt);
    assert(loc >= H5T_LOC_BADLOC && loc < H5T_LOC_MAXLOC);

    /* Datatypes can't change in size if the force_conv flag is not set */
    if (dt->shared->force_conv) {
        /* Check the datatype of this element */
        switch (dt->shared->type) {
            case H5T_ARRAY: /* Recurse on VL, compound and array base element type */
                /* Recurse if it's VL, compound, enum or array */
                /* (If the force_conv flag is _not_ set, the type cannot change in size, so don't recurse) */
                if (dt->shared->parent->shared->force_conv &&
                    H5T_IS_COMPLEX(dt->shared->parent->shared->type)) {
                    /* Keep the old base element size for later */
                    old_size = dt->shared->parent->shared->size;

                    /* Mark the VL, compound or array type */
                    if ((changed = H5T_set_loc(dt->shared->parent, file, loc)) < 0)
                        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "Unable to set VL location");
                    if (changed > 0)
                        ret_value = changed;

                    /* Check if the field changed size */
                    if (old_size != dt->shared->parent->shared->size) {
                        /* Adjust the size of the array */
                        dt->shared->size = dt->shared->u.array.nelem * dt->shared->parent->shared->size;
                    } /* end if */
                }     /* end if */
                break;

            case H5T_COMPOUND: /* Check each field and recurse on VL, compound and array type */
            {
                ssize_t accum_change = 0; /* Amount of change in the offset of the fields */

                /* Sort the fields based on offsets */
                H5T__sort_value(dt, NULL);

                for (i = 0; i < dt->shared->u.compnd.nmembs; i++) {
                    H5T_t *memb_type; /* Member's datatype pointer */

                    /* Range check against compound member's offset */
                    if ((accum_change < 0) && ((ssize_t)dt->shared->u.compnd.memb[i].offset < accum_change))
                        HGOTO_ERROR(H5E_DATATYPE, H5E_BADVALUE, FAIL, "invalid field size in datatype");

                    /* Apply the accumulated size change to the offset of the field */
                    dt->shared->u.compnd.memb[i].offset += (size_t)accum_change;

                    /* Set the member type pointer (for convenience) */
                    memb_type = dt->shared->u.compnd.memb[i].type;

                    /* Recurse if it's VL, compound, enum or array */
                    /* (If the force_conv flag is _not_ set, the type cannot change in size, so don't recurse)
                     */
                    if (memb_type->shared->force_conv && H5T_IS_COMPLEX(memb_type->shared->type)) {
                        /* Keep the old field size for later */
                        old_size = memb_type->shared->size;

                        /* Mark the VL, compound, enum or array type */
                        if ((changed = H5T_set_loc(memb_type, file, loc)) < 0)
                            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "Unable to set VL location");
                        if (changed > 0)
                            ret_value = changed;

                        /* Check if the field changed size */
                        if (old_size != memb_type->shared->size) {

                            /* Fail if the old_size is zero */
                            if (0 == old_size)
                                HGOTO_ERROR(H5E_DATATYPE, H5E_BADVALUE, FAIL,
                                            "old_size of zero would cause division by zero");

                            /* Adjust the size of the member */
                            dt->shared->u.compnd.memb[i].size =
                                (dt->shared->u.compnd.memb[i].size * memb_type->shared->size) / old_size;

                            /* Add that change to the accumulated size change */
                            accum_change += (ssize_t)(memb_type->shared->size - old_size);
                        } /* end if */
                    }     /* end if */
                }         /* end for */

                /* Range check against datatype size */
                if ((accum_change < 0) && ((ssize_t)dt->shared->size < accum_change))
                    HGOTO_ERROR(H5E_DATATYPE, H5E_BADVALUE, FAIL, "invalid field size in datatype");

                /* Apply the accumulated size change to the datatype */
                dt->shared->size += (size_t)accum_change;
            } break;

            case H5T_VLEN: /* Recurse on the VL information if it's VL, compound or array, then free VL
                              sequence */
                /* Recurse if it's VL, compound, enum or array (ignore references here so that we can encode
                 * them as part of the same blob)*/
                /* (If the force_conv flag is _not_ set, the type cannot change in size, so don't recurse) */
                if (dt->shared->parent->shared->force_conv &&
                    H5T_IS_COMPLEX(dt->shared->parent->shared->type) &&
                    (dt->shared->parent->shared->type != H5T_REFERENCE)) {
                    if ((changed = H5T_set_loc(dt->shared->parent, file, loc)) < 0)
                        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "Unable to set VL location");
                    if (changed > 0)
                        ret_value = changed;
                } /* end if */

                /* Mark this VL sequence */
                if ((changed = H5T__vlen_set_loc(dt, file, loc)) < 0)
                    HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "Unable to set VL location");
                if (changed > 0)
                    ret_value = changed;
                break;

            case H5T_REFERENCE:
                /* Reference types go through type conversion */
                if ((ret_value = H5T__ref_set_loc(dt, file, loc)) < 0)
                    HGOTO_ERROR(H5E_DATATYPE, H5E_CANTSET, FAIL, "Unable to set reference location");
                break;

            case H5T_NO_CLASS:
            case H5T_INTEGER:
            case H5T_FLOAT:
            case H5T_TIME:
            case H5T_STRING:
            case H5T_BITFIELD:
            case H5T_OPAQUE:
            case H5T_ENUM:
            case H5T_NCLASSES:
            default:
                break;
        } /* end switch */
    }     /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_set_loc() */

/*-------------------------------------------------------------------------
 * Function:    H5T_is_relocatable
 *
 * Purpose:     Check if a datatype will change between disk and memory.
 *
 * Notes:       Currently, only variable-length and references change
 *              between disk & memory (see cases where things are changed in
 *              the H5T_set_loc() code above).
 *
 * Return:
 *  One of two values on success:
 *      true - If the location of any vlen types changed
 *      false - If the location of any vlen types is the same
 *  <0 is returned on failure
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5T_is_relocatable(const H5T_t *dt)
{
    htri_t ret_value = false;

    FUNC_ENTER_NOAPI_NOERR

    /* Sanity check */
    assert(dt);

    /* VL and reference datatypes are relocatable */
    if (H5T_detect_class(dt, H5T_VLEN, false) || H5T_detect_class(dt, H5T_REFERENCE, false))
        ret_value = true;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_is_relocatable() */

/*-------------------------------------------------------------------------
 * Function:   H5T__detect_vlen_ref
 *
 * Purpose:    Check whether a datatype contains (or is) a vlen reference
 *             datatype.
 *
 * Return:      true (1) or false (0) on success
 *        (Can't fail)
 *
 *-------------------------------------------------------------------------
 */
static bool
H5T__detect_vlen_ref(const H5T_t *dt)
{
    unsigned u;                 /* Local index variable */
    bool     ret_value = false; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(dt);

    /* Check if this datatype is a vlen reference */
    /* TODO currently H5T_STD_REF is always considered as a vlen type */
    if (H5T_REFERENCE == dt->shared->type && !dt->shared->u.atomic.u.r.opaque)
        HGOTO_DONE(true);

    /* Check for types that might have the correct type as a component */
    switch (dt->shared->type) {
        case H5T_COMPOUND:
            /* Iterate over all the compound datatype's fields */
            for (u = 0; u < dt->shared->u.compnd.nmembs; u++)
                /* Recurse on field's datatype */
                if (H5T__detect_vlen_ref(dt->shared->u.compnd.memb[u].type))
                    HGOTO_DONE(true);
            break;

        case H5T_ARRAY:
        case H5T_VLEN:
        case H5T_ENUM:
            HGOTO_DONE(H5T__detect_vlen_ref(dt->shared->parent));
            break;

        case H5T_NO_CLASS:
        case H5T_INTEGER:
        case H5T_FLOAT:
        case H5T_TIME:
        case H5T_STRING:
        case H5T_BITFIELD:
        case H5T_OPAQUE:
        case H5T_REFERENCE:
        case H5T_NCLASSES:
        default:
            break;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__detect_vlen_ref() */

/*-------------------------------------------------------------------------
 * Function:    H5T_is_vl_storage
 *
 * Purpose:     Check if a datatype will be stored in a variable-length form.
 *
 * Notes:       Currently, only variable-length string & sequences and region
 *              references are stored in a variable-length form.
 *
 * Return:
 *  One of two values on success:
 *      true - If the datatype will be stored in a variable-length form
 *      false - If the datatype will NOT be stored in a variable-length form
 *  <0 is returned on failure
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5T_is_vl_storage(const H5T_t *dt)
{
    htri_t ret_value = false;

    FUNC_ENTER_NOAPI_NOERR

    /* Sanity check */
    assert(dt);

    /* VL and region reference datatypes are stored in variable-length form */
    if (H5T_detect_class(dt, H5T_VLEN, false))
        ret_value = true;
    else if (H5T_detect_class(dt, H5T_REFERENCE, false))
        ret_value = H5T__detect_vlen_ref(dt);
    else
        ret_value = false;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_is_vl_storage() */

/*-------------------------------------------------------------------------
 * Function:    H5T__upgrade_version_cb
 *
 * Purpose:     H5T__visit callback to Upgrade the version of a datatype
 *              (if there's any benefit to doing so)
 *
 * Note:        The behavior below is tightly coupled with the "better"
 *              encodings for datatype messages in the datatype message
 *              encoding routine.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__upgrade_version_cb(H5T_t *dt, void *op_value)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(dt);
    assert(op_value);

    /* Special behavior for each type of datatype */
    switch (dt->shared->type) {
        case H5T_COMPOUND:
        case H5T_ARRAY:
        case H5T_ENUM:
            /* These types benefit from "upgrading" their version */
            if (*(unsigned *)op_value > dt->shared->version)
                dt->shared->version = *(unsigned *)op_value;
            break;

        case H5T_VLEN:
            if (dt->shared->parent->shared->version > dt->shared->version)
                dt->shared->version = dt->shared->parent->shared->version;
            break;

        case H5T_NO_CLASS:
        case H5T_INTEGER:
        case H5T_FLOAT:
        case H5T_TIME:
        case H5T_STRING:
        case H5T_BITFIELD:
        case H5T_OPAQUE:
        case H5T_REFERENCE:
        case H5T_NCLASSES:
        default:
            break;
    } /* end switch */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5T__upgrade_version_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5T__upgrade_version
 *
 * Purpose:     Upgrade the version of a datatype (if there's any benefit to
 *              doing so) and recursively apply to compound members and/or
 *              parent datatypes.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5T__upgrade_version(H5T_t *dt, unsigned new_version)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(dt);

    /* Iterate over entire datatype, upgrading the version of components, if it's useful */
    if (H5T__visit(dt, (H5T_VISIT_SIMPLE | H5T_VISIT_COMPLEX_LAST), H5T__upgrade_version_cb, &new_version) <
        0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_BADITER, FAIL, "iteration to upgrade datatype encoding version failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__upgrade_version() */

/*-------------------------------------------------------------------------
 * Function:    H5T_set_version
 *
 * Purpose:     Set the encoding for a datatype to the version indicated by
 *              the file's low bound if that is higher than the datatype's
 *              version.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5T_set_version(H5F_t *f, H5T_t *dt)
{
    unsigned vers;                /* The version */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(f);
    assert(dt);

    vers = H5O_dtype_ver_bounds[H5F_LOW_BOUND(f)];
    if (vers > dt->shared->version) {
        /* Upgrade the format version for the datatype */
        if (H5T__upgrade_version(dt, vers) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTSET, FAIL, "can't upgrade datatype encoding");
    }

    /* Version bounds check */
    if (dt->shared->version > H5O_dtype_ver_bounds[H5F_HIGH_BOUND(f)])
        HGOTO_ERROR(H5E_DATATYPE, H5E_BADRANGE, FAIL, "Datatype version out of bounds");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_set_version() */

/*-------------------------------------------------------------------------
 * Function:    H5T_patch_file
 *
 * Purpose:     Patch the top-level file pointers contained in dt to point
 *              to f, if dt is a committed type.  This is possible because
 *              the top-level file pointer can be closed out from under
 *              dt while dt is contained in the shared file's cache.
 *
 * Return:      SUCCEED
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5T_patch_file(H5T_t *dt, H5F_t *f)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOERR

    /* Sanity check */
    assert(dt);
    assert(f);

    if (H5T_STATE_OPEN == dt->shared->state || H5T_STATE_NAMED == dt->shared->state) {
        dt->oloc.file   = f;
        dt->sh_loc.file = f;
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_patch_file() */

/*-------------------------------------------------------------------------
 * Function:    H5T_patch_vlen_file
 *
 * Purpose:     Patch the top-level file pointer contained in (dt->shared->u.vlen.file)
 *              to point to file.  This is possible because
 *              the top-level file pointer can be closed out from under
 *              dt while dt is contained in the shared file's cache.
 *
 * Return:      SUCCEED
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5T_patch_vlen_file(H5T_t *dt, H5VL_object_t *file)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    assert(dt);
    assert(dt->shared);
    assert(file);

    if ((dt->shared->type == H5T_VLEN) && dt->shared->u.vlen.file != file)
        dt->shared->u.vlen.file = file;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5T_patch_vlen_file() */

/*-------------------------------------------------------------------------
 * Function:    H5T_own_vol_obj
 *
 * Purpose:     Transfers ownership of the supplied VOL object to the
 *              datatype, the VOL object will be freed when the datatype
 *              is closed.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5T_own_vol_obj(H5T_t *dt, H5VL_object_t *vol_obj)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(dt);
    assert(dt->shared);
    assert(vol_obj);

    /* Currently no support for owning multiple VOL objects, free the previous
     * owned object.  Currently this is only used for holding open VOL objects
     * used in the "loc" for vlens and references, so if this is being
     * overwritten we don't need the old one anyways. */
    if (dt->shared->owned_vol_obj && H5VL_free_object(dt->shared->owned_vol_obj) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCLOSEOBJ, FAIL, "unable to close owned VOL object");

    /* Take ownership */
    dt->shared->owned_vol_obj = vol_obj;
    (void)H5VL_object_inc_rc(vol_obj);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T_own_vol_obj() */
