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

#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Iprivate.h"  /* IDs			  		*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5Ppublic.h"   /* Property lists			*/
#include "H5Oprivate.h"  /* Object headers                       */
#include "H5Sprivate.h"  /* Dataspaces         			*/
#include "H5Tprivate.h"  /* Datatypes         			*/
#include "H5Zpkg.h"      /* Data filters				*/

/* Struct of parameters needed for compressing/decompressing
 * one nbit atomic datatype: integer or floating-point
 */
typedef struct {
    unsigned size;      /* size of datatype */
    unsigned order;     /* datatype endianness order */
    unsigned precision; /* datatype precision */
    unsigned offset;    /* datatype offset */
} parms_atomic;

/* Local function prototypes */
static htri_t H5Z__can_apply_nbit(hid_t dcpl_id, hid_t type_id, hid_t space_id);
static herr_t H5Z__set_local_nbit(hid_t dcpl_id, hid_t type_id, hid_t space_id);
static size_t H5Z__filter_nbit(unsigned flags, size_t cd_nelmts, const unsigned cd_values[], size_t nbytes,
                               size_t *buf_size, void **buf);

static void   H5Z__calc_parms_nooptype(size_t *cd_values_actual_nparms);
static void   H5Z__calc_parms_atomic(size_t *cd_values_actual_nparms);
static herr_t H5Z__calc_parms_array(const H5T_t *type, size_t *cd_values_actual_nparms);
static herr_t H5Z__calc_parms_compound(const H5T_t *type, size_t *cd_values_actual_nparms);

static herr_t H5Z__set_parms_nooptype(const H5T_t *type, unsigned *cd_values_index, unsigned cd_values[]);
static herr_t H5Z__set_parms_atomic(const H5T_t *type, unsigned *cd_values_index, unsigned cd_values[],
                                    bool *need_not_compress);
static herr_t H5Z__set_parms_array(const H5T_t *type, unsigned *cd_values_index, unsigned cd_values[],
                                   bool *need_not_compress);
static herr_t H5Z__set_parms_compound(const H5T_t *type, unsigned *cd_values_index, unsigned cd_values[],
                                      bool *need_not_compress);

static void   H5Z__nbit_next_byte(size_t *j, size_t *buf_len);
static void   H5Z__nbit_decompress_one_byte(unsigned char *data, size_t data_offset, unsigned k,
                                            unsigned begin_i, unsigned end_i, const unsigned char *buffer,
                                            size_t *j, size_t *buf_len, const parms_atomic *p,
                                            size_t datatype_len);
static void   H5Z__nbit_compress_one_byte(const unsigned char *data, size_t data_offset, unsigned k,
                                          unsigned begin_i, unsigned end_i, unsigned char *buffer, size_t *j,
                                          size_t *buf_len, const parms_atomic *p, size_t datatype_len);
static void   H5Z__nbit_decompress_one_nooptype(unsigned char *data, size_t data_offset,
                                                const unsigned char *buffer, size_t *j, size_t *buf_len,
                                                unsigned size);
static void   H5Z__nbit_decompress_one_atomic(unsigned char *data, size_t data_offset, unsigned char *buffer,
                                              size_t *j, size_t *buf_len, const parms_atomic *p);
static herr_t H5Z__nbit_decompress_one_array(unsigned char *data, size_t data_offset, unsigned char *buffer,
                                             size_t *j, size_t *buf_len, const unsigned parms[],
                                             unsigned *parms_index);
static herr_t H5Z__nbit_decompress_one_compound(unsigned char *data, size_t data_offset,
                                                unsigned char *buffer, size_t *j, size_t *buf_len,
                                                const unsigned parms[], unsigned *parms_index);
static herr_t H5Z__nbit_decompress(unsigned char *data, unsigned d_nelmts, unsigned char *buffer,
                                   const unsigned parms[]);
static void   H5Z__nbit_compress_one_nooptype(const unsigned char *data, size_t data_offset,
                                              unsigned char *buffer, size_t *j, size_t *buf_len, unsigned size);
static void   H5Z__nbit_compress_one_array(unsigned char *data, size_t data_offset, unsigned char *buffer,
                                           size_t *j, size_t *buf_len, const unsigned parms[],
                                           unsigned *parms_index);
static void   H5Z__nbit_compress_one_compound(unsigned char *data, size_t data_offset, unsigned char *buffer,
                                              size_t *j, size_t *buf_len, const unsigned parms[],
                                              unsigned *parms_index);
static void   H5Z__nbit_compress(unsigned char *data, unsigned d_nelmts, unsigned char *buffer,
                                 size_t *buffer_size, const unsigned parms[]);

/* This message derives from H5Z */
H5Z_class2_t H5Z_NBIT[1] = {{
    H5Z_CLASS_T_VERS,    /* H5Z_class_t version */
    H5Z_FILTER_NBIT,     /* Filter id number		*/
    1,                   /* Assume encoder present: check before registering */
    1,                   /* decoder_present flag (set to true) */
    "nbit",              /* Filter name for debugging	*/
    H5Z__can_apply_nbit, /* The "can apply" callback     */
    H5Z__set_local_nbit, /* The "set local" callback     */
    H5Z__filter_nbit,    /* The actual filter function	*/
}};

/* Local macros */
#define H5Z_NBIT_ATOMIC     1    /* Atomic datatype class: integer/floating-point */
#define H5Z_NBIT_ARRAY      2    /* Array datatype class */
#define H5Z_NBIT_COMPOUND   3    /* Compound datatype class */
#define H5Z_NBIT_NOOPTYPE   4    /* Other datatype class: nbit does no compression */
#define H5Z_NBIT_MAX_NPARMS 4096 /* Max number of parameters for filter */
#define H5Z_NBIT_ORDER_LE   0    /* Little endian for datatype byte order */
#define H5Z_NBIT_ORDER_BE   1    /* Big endian for datatype byte order */

/* Local variables */

/*-------------------------------------------------------------------------
 * Function:	H5Z__can_apply_nbit
 *
 * Purpose:	Check the parameters for nbit compression for validity and
 *              whether they fit a particular dataset.
 *
 * Return:	Success: Non-negative
 *		Failure: Negative
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5Z__can_apply_nbit(hid_t H5_ATTR_UNUSED dcpl_id, hid_t type_id, hid_t H5_ATTR_UNUSED space_id)
{
    const H5T_t *type;             /* Datatype */
    htri_t       ret_value = true; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get datatype */
    if (NULL == (type = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");

    /* Get datatype's class, for checking the "datatype class" */
    if (H5T_get_class(type, true) == H5T_NO_CLASS)
        HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad datatype class");

    /* Get datatype's size, for checking the "datatype size" */
    if (H5T_get_size(type) == 0)
        HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad datatype size");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z__can_apply_nbit() */

/*-------------------------------------------------------------------------
 * Function:    H5Z__calc_parms_nooptype
 *
 * Purpose:     Calculate the number of parameters of array cd_values[]
 *              of datatype that is not integer, nor floating-point, nor
 *              compound, and nor array.
 *
 *-------------------------------------------------------------------------
 */
static void
H5Z__calc_parms_nooptype(size_t *cd_values_actual_nparms)
{
    /* Store datatype class code */
    *cd_values_actual_nparms += 1;

    /* Store datatype size */
    *cd_values_actual_nparms += 1;
}

/*-------------------------------------------------------------------------
 * Function:    H5Z__calc_parms_atomic
 *
 * Purpose:     Calculate the number of parameters of array cd_values[]
 *              of atomic datatype whose datatype class is integer
 *              or floating point
 *
 *-------------------------------------------------------------------------
 */
static void
H5Z__calc_parms_atomic(size_t *cd_values_actual_nparms)
{
    /* Store datatype class code */
    *cd_values_actual_nparms += 1;

    /* Store datatype size */
    *cd_values_actual_nparms += 1;

    /* Store datatype endianness */
    *cd_values_actual_nparms += 1;

    /* Store datatype's precision */
    *cd_values_actual_nparms += 1;

    /* Store datatype's offset */
    *cd_values_actual_nparms += 1;
}

/*-------------------------------------------------------------------------
 * Function:    H5Z__calc_parms_array
 *
 * Purpose:     Calculate the number of parameters of array cd_values[]
 *              for a given datatype identifier type_id
 *              if its datatype class is array datatype
 *
 * Return:      Success: Non-negative
 *              Failure: Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5Z__calc_parms_array(const H5T_t *type, size_t *cd_values_actual_nparms)
{
    H5T_t      *dtype_base = NULL;   /* Array datatype's base datatype */
    H5T_class_t dtype_base_class;    /* Array datatype's base datatype's class */
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Store datatype class code */
    *cd_values_actual_nparms += 1;

    /* Store array datatype's size */
    *cd_values_actual_nparms += 1;

    /* Get array datatype's base datatype */
    if (NULL == (dtype_base = H5T_get_super(type)))
        HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad base datatype");

    /* Get base datatype's class */
    if ((dtype_base_class = H5T_get_class(dtype_base, true)) == H5T_NO_CLASS)
        HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad base datatype class");

    /* Calculate number of the rest parameters according to base datatype's class */
    switch (dtype_base_class) {
        case H5T_INTEGER:
        case H5T_FLOAT:
            H5Z__calc_parms_atomic(cd_values_actual_nparms);
            break;

        case H5T_ARRAY:
            if (H5Z__calc_parms_array(dtype_base, cd_values_actual_nparms) == FAIL)
                HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "nbit cannot compute parameters for datatype");
            break;

        case H5T_COMPOUND:
            if (H5Z__calc_parms_compound(dtype_base, cd_values_actual_nparms) == FAIL)
                HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "nbit cannot compute parameters for datatype");
            break;

        case H5T_TIME:
        case H5T_STRING:
        case H5T_BITFIELD:
        case H5T_OPAQUE:
        case H5T_REFERENCE:
        case H5T_ENUM:
        case H5T_VLEN:
            /* Other datatype classes: nbit does no compression */
            H5Z__calc_parms_nooptype(cd_values_actual_nparms);
            break;

        case H5T_NO_CLASS:
        case H5T_NCLASSES:
        default:
            /* Badness */
            HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "nbit received bad datatype");
            break;
    } /* end switch */

done:
    if (dtype_base)
        if (H5T_close_real(dtype_base) < 0)
            HDONE_ERROR(H5E_PLINE, H5E_CLOSEERROR, FAIL, "Unable to close base datatype");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z__calc_parms_array() */

/*-------------------------------------------------------------------------
 * Function:    H5Z__calc_parms_compound
 *
 * Purpose:     Calculate the number of parameters of array cd_values[]
 *              for a given datatype identifier type_id
 *              if its datatype class is compound datatype
 *
 * Return:      Success: Non-negative
 *              Failure: Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5Z__calc_parms_compound(const H5T_t *type, size_t *cd_values_actual_nparms)
{
    int      nmembers;            /* Compound datatype's number of members */
    H5T_t   *dtype_member = NULL; /* Compound datatype's member datatype */
    unsigned u;                   /* Local index variable */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Store compound datatype class code */
    *cd_values_actual_nparms += 1;

    /* Store compound datatype's size */
    *cd_values_actual_nparms += 1;

    /* Get number of members */
    if ((nmembers = H5T_get_nmembers(type)) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad datatype number of members");

    /* Store number of members */
    *cd_values_actual_nparms += 1;

    /* For each member, calculate parameters */
    for (u = 0; u < (unsigned)nmembers; u++) {
        H5T_class_t dtype_member_class; /* Compound datatype's member datatype's class */

        /* Get member datatype */
        if (NULL == (dtype_member = H5T_get_member_type(type, u)))
            HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad member datatype");

        /* Get member datatype's class */
        if ((dtype_member_class = H5T_get_class(dtype_member, true)) == H5T_NO_CLASS)
            HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad member datatype class");

        /* Store member offset */
        *cd_values_actual_nparms += 1;

        /* Calculate parameters according to member's datatype class */
        switch (dtype_member_class) {
            case H5T_INTEGER:
            case H5T_FLOAT:
                H5Z__calc_parms_atomic(cd_values_actual_nparms);
                break;

            case H5T_ARRAY:
                if (H5Z__calc_parms_array(dtype_member, cd_values_actual_nparms) == FAIL)
                    HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "nbit cannot compute parameters for datatype");
                break;

            case H5T_COMPOUND:
                if (H5Z__calc_parms_compound(dtype_member, cd_values_actual_nparms) == FAIL)
                    HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "nbit cannot compute parameters for datatype");
                break;

            case H5T_TIME:
            case H5T_STRING:
            case H5T_BITFIELD:
            case H5T_OPAQUE:
            case H5T_REFERENCE:
            case H5T_ENUM:
            case H5T_VLEN:
                /* Other datatype classes: nbit does no compression */
                H5Z__calc_parms_nooptype(cd_values_actual_nparms);
                break;

            case H5T_NO_CLASS:
            case H5T_NCLASSES:
            default:
                /* Badness */
                HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "nbit received bad datatype");
                break;
        } /* end switch */

        /* Close member datatype */
        if (H5T_close_real(dtype_member) < 0)
            HGOTO_ERROR(H5E_PLINE, H5E_CLOSEERROR, FAIL, "Unable to close member datatype");
        dtype_member = NULL;
    } /* end for */

done:
    if (dtype_member)
        if (H5T_close_real(dtype_member) < 0)
            HDONE_ERROR(H5E_PLINE, H5E_CLOSEERROR, FAIL, "Unable to close member datatype");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z_calc_params_compound */

/*-------------------------------------------------------------------------
 * Function:    H5Z__set_parms_nooptype
 *
 * Purpose:     Set the array cd_values[] for a given datatype identifier
 *              type_id if its datatype class is not integer, nor
 *              floating-point, nor array, nor compound, nor VL datatype,
 *              and nor VL string
 *
 * Return:      Success: Non-negative
 *              Failure: Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5Z__set_parms_nooptype(const H5T_t *type, unsigned *cd_values_index, unsigned cd_values[])
{
    size_t dtype_size;          /* No-op datatype's size (in bytes) */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Set datatype class code */
    cd_values[(*cd_values_index)++] = H5Z_NBIT_NOOPTYPE;

    /* Get datatype's size */
    if ((dtype_size = H5T_get_size(type)) == 0)
        HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad datatype size");

    /* Set "local" parameter for datatype size */
    H5_CHECK_OVERFLOW(dtype_size, size_t, unsigned);
    cd_values[(*cd_values_index)++] = (unsigned)dtype_size;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z__set_parms_nooptype() */

/*-------------------------------------------------------------------------
 * Function:    H5Z__set_parms_atomic
 *
 * Purpose:     Set the array cd_values[] for a given datatype identifier
 *              type_id if its datatype class is integer or floating point
 *
 * Return:      Success: Non-negative
 *              Failure: Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5Z__set_parms_atomic(const H5T_t *type, unsigned *cd_values_index, unsigned cd_values[],
                      bool *need_not_compress)
{
    H5T_order_t dtype_order;         /* Atomic datatype's endianness order */
    size_t      dtype_size;          /* Atomic datatype's size (in bytes) */
    size_t      dtype_precision;     /* Atomic datatype's precision (in bits) */
    int         sdtype_offset;       /* Atomic datatype's offset (in bits) */
    unsigned    dtype_offset;        /* Atomic datatype's offset (in bits) */
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Set datatype class code */
    cd_values[(*cd_values_index)++] = H5Z_NBIT_ATOMIC;

    /* Get datatype's size */
    if ((dtype_size = H5T_get_size(type)) == 0)
        HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad datatype size");

    /* Set "local" parameter for datatype size */
    H5_CHECK_OVERFLOW(dtype_size, size_t, unsigned);
    cd_values[(*cd_values_index)++] = (unsigned)dtype_size;

    /* Get datatype's endianness order */
    if ((dtype_order = H5T_get_order(type)) == H5T_ORDER_ERROR)
        HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad datatype endianness order");

    /* Set "local" parameter for datatype endianness */
    switch (dtype_order) {
        case H5T_ORDER_LE: /* Little-endian byte order */
            cd_values[(*cd_values_index)++] = H5Z_NBIT_ORDER_LE;
            break;

        case H5T_ORDER_BE: /* Big-endian byte order */
            cd_values[(*cd_values_index)++] = H5Z_NBIT_ORDER_BE;
            break;

        case H5T_ORDER_VAX:
        case H5T_ORDER_MIXED:
        case H5T_ORDER_ERROR:
        case H5T_ORDER_NONE:
        default:
            HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad datatype endianness order");
    } /* end switch */

    /* Get datatype's precision */
    if ((dtype_precision = H5T_get_precision(type)) == 0)
        HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad datatype precision");

    /* Get datatype's offset */
    if ((sdtype_offset = H5T_get_offset(type)) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad datatype offset");
    dtype_offset = (unsigned)sdtype_offset;

    /* Check values of precision and offset */
    if (dtype_precision > dtype_size * 8 || (dtype_precision + dtype_offset) > dtype_size * 8)
        HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "invalid datatype precision/offset");

    /* Set "local" parameter for datatype precision */
    H5_CHECK_OVERFLOW(dtype_precision, size_t, unsigned);
    cd_values[(*cd_values_index)++] = (unsigned)dtype_precision;

    /* Set "local" parameter for datatype offset */
    cd_values[(*cd_values_index)++] = dtype_offset;

    /* If before this point, there is no need to compress, check the need to
     * compress at this point. If current datatype is not full-precision,
     * flag need_not_compress should be set to false.
     */
    if (*need_not_compress) /* so far no need to compress */
        if (dtype_offset != 0 || dtype_precision != dtype_size * 8)
            *need_not_compress = false;
done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z__set_parms_atomic() */

/*-------------------------------------------------------------------------
 * Function:    H5Z__set_parms_array
 *
 * Purpose:     Set the array cd_values[] for a given datatype identifier
 *              type_id if its datatype class is array datatype
 *
 * Return:      Success: Non-negative
 *              Failure: Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5Z__set_parms_array(const H5T_t *type, unsigned *cd_values_index, unsigned cd_values[],
                     bool *need_not_compress)
{
    H5T_t      *dtype_base = NULL;   /* Array datatype's base datatype */
    H5T_class_t dtype_base_class;    /* Array datatype's base datatype's class */
    size_t      dtype_size;          /* Array datatype's size (in bytes) */
    htri_t      is_vlstring;         /* flag indicating if datatype is variable-length string */
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Set datatype class code */
    cd_values[(*cd_values_index)++] = H5Z_NBIT_ARRAY;

    /* Get array datatype's size */
    if ((dtype_size = H5T_get_size(type)) == 0)
        HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad datatype size");

    /* Set "local" parameter for array datatype's size */
    H5_CHECK_OVERFLOW(dtype_size, size_t, unsigned);
    cd_values[(*cd_values_index)++] = (unsigned)dtype_size;

    /* Get array datatype's base datatype */
    if (NULL == (dtype_base = H5T_get_super(type)))
        HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad base datatype");

    /* Get base datatype's class */
    if ((dtype_base_class = H5T_get_class(dtype_base, true)) == H5T_NO_CLASS)
        HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad base datatype class");

    /* Call appropriate function according to base datatype's class */
    switch (dtype_base_class) {
        case H5T_INTEGER:
        case H5T_FLOAT:
            if (H5Z__set_parms_atomic(dtype_base, cd_values_index, cd_values, need_not_compress) < 0)
                HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "nbit cannot set parameters for datatype");
            break;

        case H5T_ARRAY:
            if (H5Z__set_parms_array(dtype_base, cd_values_index, cd_values, need_not_compress) < 0)
                HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "nbit cannot set parameters for datatype");
            break;

        case H5T_COMPOUND:
            if (H5Z__set_parms_compound(dtype_base, cd_values_index, cd_values, need_not_compress) < 0)
                HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "nbit cannot set parameters for datatype");
            break;

        case H5T_VLEN:
            /* Check if base datatype is a variable-length string */
            if ((is_vlstring = H5T_is_variable_str(dtype_base)) < 0)
                HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL,
                            "cannot determine if datatype is a variable-length string");

            /* base datatype of VL or VL-string is not supported */
            if (dtype_base_class == H5T_VLEN || is_vlstring)
                HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "datatype not supported by nbit");

            if (H5Z__set_parms_nooptype(dtype_base, cd_values_index, cd_values) < 0)
                HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "nbit cannot set parameters for datatype");
            break;

        case H5T_TIME:
        case H5T_STRING:
        case H5T_BITFIELD:
        case H5T_OPAQUE:
        case H5T_REFERENCE:
        case H5T_ENUM:
            if (H5Z__set_parms_nooptype(dtype_base, cd_values_index, cd_values) < 0)
                HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "nbit cannot set parameters for datatype");
            break;

        case H5T_NO_CLASS:
        case H5T_NCLASSES:
        default:
            /* Badness */
            HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "nbit received bad datatype");
            break;
    } /* end switch */

done:
    if (dtype_base)
        if (H5T_close_real(dtype_base) < 0)
            HDONE_ERROR(H5E_PLINE, H5E_CLOSEERROR, FAIL, "Unable to close base datatype");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z__set_parms_array() */

/*-------------------------------------------------------------------------
 * Function:    H5Z__set_parms_compound
 *
 * Purpose:     Set the array cd_values[] for a given datatype identifier
 *              type_id if its datatype class is compound datatype
 *
 * Return:      Success: Non-negative
 *              Failure: Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5Z__set_parms_compound(const H5T_t *type, unsigned *cd_values_index, unsigned cd_values[],
                        bool *need_not_compress)
{
    int         snmembers;           /* Compound datatype's number of members */
    unsigned    nmembers;            /* Compound datatype's number of members */
    H5T_t      *dtype_member = NULL; /* Compound datatype's member datatype */
    H5T_class_t dtype_member_class;  /* Compound datatype's member datatype's class */
    size_t      dtype_member_offset; /* Compound datatype's current member datatype's offset (in bytes) */
    size_t      dtype_next_member_offset; /* Compound datatype's next member datatype's offset (in bytes) */
    size_t      dtype_size;               /* Compound datatype's size (in bytes) */
    htri_t      is_vlstring;              /* flag indicating if datatype is variable-length string */
    unsigned    u;                        /* Local index variable */
    herr_t      ret_value = SUCCEED;      /* Return value */

    FUNC_ENTER_PACKAGE

    /* Set "local" parameter for compound datatype class code */
    cd_values[(*cd_values_index)++] = H5Z_NBIT_COMPOUND;

    /* Get datatype's size */
    if ((dtype_size = H5T_get_size(type)) == 0)
        HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad datatype size");

    /* Set "local" parameter for compound datatype size */
    H5_CHECK_OVERFLOW(dtype_size, size_t, unsigned);
    cd_values[(*cd_values_index)++] = (unsigned)dtype_size;

    /* Get number of members */
    if ((snmembers = H5T_get_nmembers(type)) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad datatype number of members");
    nmembers = (unsigned)snmembers;

    /* Set "local" parameter for number of members */
    cd_values[(*cd_values_index)++] = nmembers;

    /* For each member, set parameters */
    for (u = 0; u < nmembers; u++) {
        /* Get member datatype */
        if (NULL == (dtype_member = H5T_get_member_type(type, u)))
            HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad member datatype");

        /* Get member datatype's class */
        if ((dtype_member_class = H5T_get_class(dtype_member, true)) < 0)
            HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad member datatype class");

        /* Get member offset, success if H5T_get_class() success */
        dtype_member_offset = H5T_get_member_offset(type, u);

        /* Set "local" parameter for member offset */
        H5_CHECK_OVERFLOW(dtype_member_offset, size_t, unsigned);
        cd_values[(*cd_values_index)++] = (unsigned)dtype_member_offset;

        /* Call appropriate function according to member's datatype class */
        switch (dtype_member_class) {
            case H5T_INTEGER:
            case H5T_FLOAT:
                if (H5Z__set_parms_atomic(dtype_member, cd_values_index, cd_values, need_not_compress) < 0)
                    HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "nbit cannot set parameters for datatype");
                break;

            case H5T_ARRAY:
                if (H5Z__set_parms_array(dtype_member, cd_values_index, cd_values, need_not_compress) < 0)
                    HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "nbit cannot set parameters for datatype");
                break;

            case H5T_COMPOUND:
                if (H5Z__set_parms_compound(dtype_member, cd_values_index, cd_values, need_not_compress) < 0)
                    HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "nbit cannot set parameters for datatype");
                break;

            case H5T_VLEN:
                /* Check if datatype is a variable-length string */
                if ((is_vlstring = H5T_is_variable_str(dtype_member)) < 0)
                    HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL,
                                "cannot determine if datatype is a variable-length string");

                /* Because for some no-op datatype (VL datatype and VL string datatype), its
                 * size can not be retrieved correctly by using function call H5T_get_size,
                 * special handling is needed for getting the size. Here the difference between
                 * adjacent member offset is used (if alignment is present, the result can be
                 * larger, but it does not affect the nbit filter's correctness).
                 */
                if (dtype_member_class == H5T_VLEN || is_vlstring) {
                    /* Set datatype class code */
                    cd_values[(*cd_values_index)++] = H5Z_NBIT_NOOPTYPE;

                    if (u != nmembers - 1)
                        dtype_next_member_offset = H5T_get_member_offset(type, u + 1);
                    else /* current member is the last member */
                        dtype_next_member_offset = dtype_size;

                    /* Set "local" parameter for datatype size */
                    H5_CHECK_OVERFLOW(dtype_member_offset, size_t, unsigned);
                    H5_CHECK_OVERFLOW(dtype_next_member_offset, size_t, unsigned);
                    cd_values[(*cd_values_index)++] =
                        (unsigned)dtype_next_member_offset - (unsigned)dtype_member_offset;
                }
                break;

            case H5T_TIME:
            case H5T_STRING:
            case H5T_BITFIELD:
            case H5T_OPAQUE:
            case H5T_REFERENCE:
            case H5T_ENUM:
                /* other datatype that nbit does no compression */
                if (H5Z__set_parms_nooptype(dtype_member, cd_values_index, cd_values) < 0)
                    HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "nbit cannot set parameters for datatype");
                break;

            case H5T_NO_CLASS:
            case H5T_NCLASSES:
            default:
                /* Badness */
                HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "nbit was passed bad datatype");
                break;
        } /* end switch */

        /* Close member datatype */
        if (H5T_close_real(dtype_member) < 0)
            HGOTO_ERROR(H5E_PLINE, H5E_CLOSEERROR, FAIL, "Unable to close member datatype");
        dtype_member = NULL;
    } /* end for */

done:
    if (dtype_member)
        if (H5T_close_real(dtype_member) < 0)
            HDONE_ERROR(H5E_PLINE, H5E_CLOSEERROR, FAIL, "Unable to close member datatype");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z_set_params_compound */

/*-------------------------------------------------------------------------
 * Function:	H5Z__set_local_nbit
 *
 * Purpose:	Set the "local" dataset parameters for nbit compression.
 *
 * Return:	Success: Non-negative
 *		Failure: Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5Z__set_local_nbit(hid_t dcpl_id, hid_t type_id, hid_t space_id)
{
    H5P_genplist_t *dcpl_plist;                       /* Property list pointer */
    const H5T_t    *type;                             /* Datatype */
    const H5S_t    *ds;                               /* Dataspace */
    unsigned        flags;                            /* Filter flags */
    unsigned        cd_values_index;                  /* Index of array cd_values */
    size_t          cd_values_actual_nparms;          /* Number of parameters in array cd_values[] */
    size_t          cd_nelmts = H5Z_NBIT_USER_NPARMS; /* Number of filter parameters */
    unsigned       *cd_values = NULL;                 /* Filter parameters */
    hssize_t        npoints;                          /* Number of points in the dataspace */
    H5T_class_t     dtype_class;                      /* Datatype's class */
    bool            need_not_compress;   /* Flag if true indicating no need to do nbit compression */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get datatype */
    if (NULL == (type = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");

    /* Get datatype's class */
    if ((dtype_class = H5T_get_class(type, true)) == H5T_NO_CLASS)
        HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "bad datatype class");

    /* Calculate how many parameters will fill the cd_values array
     * First three parameters reserved for:
     *   1. number of parameters in array cd_values
     *   2. flag if true indicating no need to do nbit compression
     *   3. number of elements in the chunk
     */
    cd_values_actual_nparms = 3;
    switch (dtype_class) {
        case H5T_INTEGER:
        case H5T_FLOAT:
            H5Z__calc_parms_atomic(&cd_values_actual_nparms);
            break;

        case H5T_ARRAY:
            if (H5Z__calc_parms_array(type, &cd_values_actual_nparms) < 0)
                HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "nbit cannot compute parameters for datatype");
            break;

        case H5T_COMPOUND:
            if (H5Z__calc_parms_compound(type, &cd_values_actual_nparms) < 0)
                HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "nbit cannot compute parameters for datatype");
            break;

        case H5T_TIME:
        case H5T_STRING:
        case H5T_BITFIELD:
        case H5T_OPAQUE:
        case H5T_REFERENCE:
        case H5T_ENUM:
        case H5T_VLEN:
            /* No need to calculate other datatypes at top level */
            break;

        case H5T_NO_CLASS:
        case H5T_NCLASSES:
        default:
            /* Badness */
            HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "nbit received bad datatype");
            break;
    } /* end switch */

    /* Check if the number of parameters exceed what cd_values[] can store */
    if (cd_values_actual_nparms > H5Z_NBIT_MAX_NPARMS)
        HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "datatype needs too many nbit parameters");

    /* Allocate memory space for cd_values[] */
    if (NULL == (cd_values = (unsigned *)H5MM_malloc(cd_values_actual_nparms * sizeof(unsigned))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed for cd_values[]");

    /* Get the plist structure */
    if (NULL == (dcpl_plist = H5P_object_verify(dcpl_id, H5P_DATASET_CREATE)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get the filter's current parameters */
    if (H5P_get_filter_by_id(dcpl_plist, H5Z_FILTER_NBIT, &flags, &cd_nelmts, cd_values, (size_t)0, NULL,
                             NULL) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTGET, FAIL, "can't get nbit parameters");

    /* Get dataspace */
    if (NULL == (ds = (H5S_t *)H5I_object_verify(space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataspace");

    /* Get total number of elements in the chunk */
    if ((npoints = H5S_GET_EXTENT_NPOINTS(ds)) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTGET, FAIL, "unable to get number of points in the dataspace");
    assert(npoints);

    /* Initialize index for cd_values array starting from the third entry */
    cd_values_index = 2;

    /* Set "local" parameter for number of elements in the chunk */
    H5_CHECK_OVERFLOW(npoints, hssize_t, unsigned);
    cd_values[cd_values_index++] = (unsigned)npoints;

    /* Assume no need to compress now, will be changed to false later if not */
    need_not_compress = true;

    /* Call appropriate function according to the datatype class */
    switch (dtype_class) {
        case H5T_INTEGER:
        case H5T_FLOAT:
            if (H5Z__set_parms_atomic(type, &cd_values_index, cd_values, &need_not_compress) < 0)
                HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "nbit cannot set parameters for datatype");
            break;

        case H5T_ARRAY:
            if (H5Z__set_parms_array(type, &cd_values_index, cd_values, &need_not_compress) < 0)
                HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "nbit cannot set parameters for datatype");
            break;

        case H5T_COMPOUND:
            if (H5Z__set_parms_compound(type, &cd_values_index, cd_values, &need_not_compress) < 0)
                HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "nbit cannot set parameters for datatype");
            break;

        case H5T_TIME:
        case H5T_STRING:
        case H5T_BITFIELD:
        case H5T_OPAQUE:
        case H5T_REFERENCE:
        case H5T_ENUM:
        case H5T_VLEN:
            /* No need to set parameters for other datatypes at top level */
            break;

        case H5T_NO_CLASS:
        case H5T_NCLASSES:
        default:
            /* Badness */
            HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "nbit received bad datatype");
            break;
    } /* end switch */

    /* Check if calculation of parameters matches with setting of parameters */
    assert(cd_values_actual_nparms == cd_values_index);

    /* Finally set the first two entries of cd_values[] */
    H5_CHECK_OVERFLOW(cd_values_actual_nparms, size_t, unsigned);
    cd_values[0] = (unsigned)cd_values_actual_nparms;
    cd_values[1] = (unsigned)need_not_compress;

    /* Modify the filter's parameters for this dataset */
    if (H5P_modify_filter(dcpl_plist, H5Z_FILTER_NBIT, flags, cd_values_actual_nparms, cd_values) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTSET, FAIL, "can't set local nbit parameters");

done:
    if (cd_values)
        H5MM_xfree(cd_values);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z__set_local_nbit() */

/*-------------------------------------------------------------------------
 * Function:	H5Z__filter_nbit
 *
 * Purpose:	Implement an I/O filter for storing packed nbit data
 *
 * Return:	Success: Size of buffer filtered
 *		Failure: 0
 *
 *-------------------------------------------------------------------------
 */
static size_t
H5Z__filter_nbit(unsigned flags, size_t cd_nelmts, const unsigned cd_values[], size_t nbytes,
                 size_t *buf_size, void **buf)
{
    unsigned char *outbuf;        /* pointer to new output buffer */
    size_t         size_out  = 0; /* size of output buffer */
    unsigned       d_nelmts  = 0; /* number of elements in the chunk */
    size_t         ret_value = 0; /* return value */

    FUNC_ENTER_PACKAGE

    /* check arguments
     * cd_values[0] stores actual number of parameters in cd_values[]
     */
    if (cd_nelmts != cd_values[0])
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, 0, "invalid nbit aggression level");

    /* check if need to do nbit compress or decompress
     * cd_values[1] stores the flag if true indicating no need to compress
     */
    if (cd_values[1])
        HGOTO_DONE(*buf_size);

    /* copy a filter parameter to d_nelmts */
    d_nelmts = cd_values[2];

    /* input; decompress */
    if (flags & H5Z_FLAG_REVERSE) {
        size_out = d_nelmts * (size_t)cd_values[4]; /* cd_values[4] stores datatype size */

        /* allocate memory space for decompressed buffer */
        if (NULL == (outbuf = (unsigned char *)H5MM_malloc(size_out)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, 0, "memory allocation failed for nbit decompression");

        /* decompress the buffer */
        if (H5Z__nbit_decompress(outbuf, d_nelmts, (unsigned char *)*buf, cd_values) < 0)
            HGOTO_ERROR(H5E_PLINE, H5E_CANTFILTER, 0, "can't decompress buffer");
    } /* end if */
    /* output; compress */
    else {
        assert(nbytes == d_nelmts * cd_values[4]);

        size_out = nbytes;

        /* allocate memory space for compressed buffer */
        if (NULL == (outbuf = (unsigned char *)H5MM_malloc(size_out)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, 0, "memory allocation failed for nbit compression");

        /* compress the buffer, size_out will be changed */
        H5Z__nbit_compress((unsigned char *)*buf, d_nelmts, outbuf, &size_out, cd_values);
    } /* end else */

    /* free the input buffer */
    H5MM_xfree(*buf);

    /* set return values */
    *buf      = outbuf;
    *buf_size = size_out;
    ret_value = size_out;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5Z__filter_nbit() */

/* ======== Nbit Algorithm ===============================================
 * assume one byte has 8 bit
 * assume padding bit is 0
 * assume size of unsigned char is one byte
 * assume one data item of certain datatype is stored continuously in bytes
 * atomic datatype is treated on byte basis
 */

static void
H5Z__nbit_next_byte(size_t *j, size_t *buf_len)
{
    ++(*j);
    *buf_len = 8 * sizeof(unsigned char);
}

static void
H5Z__nbit_decompress_one_byte(unsigned char *data, size_t data_offset, unsigned k, unsigned begin_i,
                              unsigned end_i, const unsigned char *buffer, size_t *j, size_t *buf_len,
                              const parms_atomic *p, size_t datatype_len)
{
    size_t        dat_len; /* dat_len is the number of bits to be copied in each data byte */
    size_t        dat_offset;
    unsigned char val; /* value to be copied in each data byte */

    /* initialize value and bits of unsigned char to be copied */
    val        = buffer[*j];
    dat_offset = 0;

    if (begin_i != end_i) { /* significant bits occupy >1 unsigned char */
        if (k == begin_i)
            dat_len = 8 - (datatype_len - p->precision - p->offset) % 8;
        else if (k == end_i) {
            dat_len    = 8 - p->offset % 8;
            dat_offset = 8 - dat_len;
        }
        else
            dat_len = 8;
    }
    else { /* all significant bits in one unsigned char */
        dat_offset = p->offset % 8;
        dat_len    = p->precision;
    }

    if (*buf_len > dat_len) {
        data[data_offset + k] = (unsigned char)(((unsigned)(val >> (*buf_len - dat_len)) &
                                                 (unsigned)(~((unsigned)(~0) << dat_len)))
                                                << dat_offset);
        *buf_len -= dat_len;
    }
    else {
        data[data_offset + k] =
            (unsigned char)(((val & ~((unsigned)(~0) << *buf_len)) << (dat_len - *buf_len)) << dat_offset);
        dat_len -= *buf_len;
        H5Z__nbit_next_byte(j, buf_len);
        if (dat_len == 0)
            return;

        val = buffer[*j];
        data[data_offset + k] |= (unsigned char)(((unsigned)(val >> (*buf_len - dat_len)) &
                                                  (unsigned)(~((unsigned)(~0) << dat_len)))
                                                 << dat_offset);
        *buf_len -= dat_len;
    }
}

static void
H5Z__nbit_decompress_one_nooptype(unsigned char *data, size_t data_offset, const unsigned char *buffer,
                                  size_t *j, size_t *buf_len, unsigned size)
{
    unsigned      i;       /* index */
    size_t        dat_len; /* dat_len is the number of bits to be copied in each data byte */
    unsigned char val;     /* value to be copied in each data byte */

    for (i = 0; i < size; i++) {
        /* initialize value and bits of unsigned char to be copied */
        val     = buffer[*j];
        dat_len = sizeof(unsigned char) * 8;

        data[data_offset + i] =
            (unsigned char)(((val & ~((unsigned)(~0) << *buf_len)) << (dat_len - *buf_len)));
        dat_len -= *buf_len;
        H5Z__nbit_next_byte(j, buf_len);
        if (dat_len == 0)
            continue;

        val = buffer[*j];
        data[data_offset + i] |= (unsigned char)((unsigned)(val >> (*buf_len - dat_len)) &
                                                 (unsigned)(~((unsigned)(~0) << dat_len)));
        *buf_len -= dat_len;
    }
}

static void
H5Z__nbit_decompress_one_atomic(unsigned char *data, size_t data_offset, unsigned char *buffer, size_t *j,
                                size_t *buf_len, const parms_atomic *p)
{
    /* begin_i: the index of byte having first significant bit
       end_i: the index of byte having last significant bit */
    int      k;
    unsigned begin_i, end_i;
    size_t   datatype_len;

    datatype_len = p->size * 8;

    if (p->order == H5Z_NBIT_ORDER_LE) { /* little endian */
        /* calculate begin_i and end_i */
        if ((p->precision + p->offset) % 8 != 0)
            begin_i = (p->precision + p->offset) / 8;
        else
            begin_i = (p->precision + p->offset) / 8 - 1;
        end_i = p->offset / 8;

        for (k = (int)begin_i; k >= (int)end_i; k--)
            H5Z__nbit_decompress_one_byte(data, data_offset, (unsigned)k, begin_i, end_i, buffer, j, buf_len,
                                          p, datatype_len);
    }
    else { /* big endian */
        /* Sanity check */
        assert(p->order == H5Z_NBIT_ORDER_BE);

        /* calculate begin_i and end_i */
        begin_i = ((unsigned)datatype_len - p->precision - p->offset) / 8;
        if (p->offset % 8 != 0)
            end_i = ((unsigned)datatype_len - p->offset) / 8;
        else
            end_i = ((unsigned)datatype_len - p->offset) / 8 - 1;

        for (k = (int)begin_i; k <= (int)end_i; k++)
            H5Z__nbit_decompress_one_byte(data, data_offset, (unsigned)k, begin_i, end_i, buffer, j, buf_len,
                                          p, datatype_len);
    }
}

static herr_t
H5Z__nbit_decompress_one_array(unsigned char *data, size_t data_offset, unsigned char *buffer, size_t *j,
                               size_t *buf_len, const unsigned parms[], unsigned *parms_index)
{
    unsigned     i, total_size, base_class, base_size, n, begin_index;
    parms_atomic p;
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    total_size = parms[(*parms_index)++];
    base_class = parms[(*parms_index)++];

    switch (base_class) {
        case H5Z_NBIT_ATOMIC:
            p.size      = parms[(*parms_index)++];
            p.order     = parms[(*parms_index)++];
            p.precision = parms[(*parms_index)++];
            p.offset    = parms[(*parms_index)++];

            /* Check values of precision and offset */
            if (p.precision > p.size * 8 || (p.precision + p.offset) > p.size * 8)
                HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "invalid datatype precision/offset");

            n = total_size / p.size;
            for (i = 0; i < n; i++)
                H5Z__nbit_decompress_one_atomic(data, data_offset + i * (size_t)p.size, buffer, j, buf_len,
                                                &p);
            break;

        case H5Z_NBIT_ARRAY:
            base_size   = parms[*parms_index];    /* read in advance */
            n           = total_size / base_size; /* number of base_type elements inside the array datatype */
            begin_index = *parms_index;
            for (i = 0; i < n; i++) {
                if (H5Z__nbit_decompress_one_array(data, data_offset + i * (size_t)base_size, buffer, j,
                                                   buf_len, parms, parms_index) < 0)
                    HGOTO_ERROR(H5E_PLINE, H5E_CANTFILTER, FAIL, "can't decompress array");
                *parms_index = begin_index;
            }
            break;

        case H5Z_NBIT_COMPOUND:
            base_size   = parms[*parms_index];    /* read in advance */
            n           = total_size / base_size; /* number of base_type elements inside the array datatype */
            begin_index = *parms_index;
            for (i = 0; i < n; i++) {
                if (H5Z__nbit_decompress_one_compound(data, data_offset + i * (size_t)base_size, buffer, j,
                                                      buf_len, parms, parms_index) < 0)
                    HGOTO_ERROR(H5E_PLINE, H5E_CANTFILTER, FAIL, "can't decompress compound");
                *parms_index = begin_index;
            }
            break;

        case H5Z_NBIT_NOOPTYPE:
            (*parms_index)++; /* skip size of no-op type */
            H5Z__nbit_decompress_one_nooptype(data, data_offset, buffer, j, buf_len, total_size);
            break;

        default:
            assert(0 && "This Should never be executed!");
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

static herr_t
H5Z__nbit_decompress_one_compound(unsigned char *data, size_t data_offset, unsigned char *buffer, size_t *j,
                                  size_t *buf_len, const unsigned parms[], unsigned *parms_index)
{
    unsigned     i, nmembers, member_offset, member_class, member_size, used_size = 0, size;
    parms_atomic p;
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    size     = parms[(*parms_index)++];
    nmembers = parms[(*parms_index)++];

    for (i = 0; i < nmembers; i++) {
        member_offset = parms[(*parms_index)++];
        member_class  = parms[(*parms_index)++];

        /* Check for overflow */
        member_size = parms[*parms_index];
        used_size += member_size;
        if (used_size > size)
            HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "compound member offset overflowed compound size");
        switch (member_class) {
            case H5Z_NBIT_ATOMIC:
                p.size = member_size;
                /* Advance past member size */
                (*parms_index)++;
                p.order     = parms[(*parms_index)++];
                p.precision = parms[(*parms_index)++];
                p.offset    = parms[(*parms_index)++];

                /* Check values of precision and offset */
                if (p.precision > p.size * 8 || (p.precision + p.offset) > p.size * 8)
                    HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "invalid datatype precision/offset");

                H5Z__nbit_decompress_one_atomic(data, data_offset + member_offset, buffer, j, buf_len, &p);
                break;

            case H5Z_NBIT_ARRAY:
                if (H5Z__nbit_decompress_one_array(data, data_offset + member_offset, buffer, j, buf_len,
                                                   parms, parms_index) < 0)
                    HGOTO_ERROR(H5E_PLINE, H5E_CANTFILTER, FAIL, "can't decompress array");
                break;

            case H5Z_NBIT_COMPOUND:
                if (H5Z__nbit_decompress_one_compound(data, data_offset + member_offset, buffer, j, buf_len,
                                                      parms, parms_index) < 0)
                    HGOTO_ERROR(H5E_PLINE, H5E_CANTFILTER, FAIL, "can't decompress compound");
                break;

            case H5Z_NBIT_NOOPTYPE:
                /* Advance past member size */
                (*parms_index)++;
                H5Z__nbit_decompress_one_nooptype(data, data_offset + member_offset, buffer, j, buf_len,
                                                  member_size);
                break;

            default:
                assert(0 && "This Should never be executed!");
        } /* end switch */
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

static herr_t
H5Z__nbit_decompress(unsigned char *data, unsigned d_nelmts, unsigned char *buffer, const unsigned parms[])
{
    /* i: index of data, j: index of buffer,
       buf_len: number of bits to be filled in current byte */
    unsigned     i;
    size_t       j, size;
    size_t       buf_len;
    parms_atomic p;
    unsigned     parms_index;         /* index in array parms used by compression/decompression functions */
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* may not have to initialize to zeros */
    memset(data, 0, d_nelmts * (size_t)parms[4]);

    /* initialization before the loop */
    j       = 0;
    buf_len = sizeof(unsigned char) * 8;

    switch (parms[3]) {
        case H5Z_NBIT_ATOMIC:
            p.size      = parms[4];
            p.order     = parms[5];
            p.precision = parms[6];
            p.offset    = parms[7];

            /* Check values of precision and offset */
            if (p.precision > p.size * 8 || (p.precision + p.offset) > p.size * 8)
                HGOTO_ERROR(H5E_PLINE, H5E_BADTYPE, FAIL, "invalid datatype precision/offset");

            for (i = 0; i < d_nelmts; i++)
                H5Z__nbit_decompress_one_atomic(data, i * (size_t)p.size, buffer, &j, &buf_len, &p);
            break;

        case H5Z_NBIT_ARRAY:
            size        = parms[4];
            parms_index = 4; /* set the index before goto function call */
            for (i = 0; i < d_nelmts; i++) {
                if (H5Z__nbit_decompress_one_array(data, i * size, buffer, &j, &buf_len, parms,
                                                   &parms_index) < 0)
                    HGOTO_ERROR(H5E_PLINE, H5E_CANTFILTER, FAIL, "can't decompress array");
                parms_index = 4;
            }
            break;

        case H5Z_NBIT_COMPOUND:
            size        = parms[4];
            parms_index = 4; /* set the index before goto function call */
            for (i = 0; i < d_nelmts; i++) {
                if (H5Z__nbit_decompress_one_compound(data, i * size, buffer, &j, &buf_len, parms,
                                                      &parms_index) < 0)
                    HGOTO_ERROR(H5E_PLINE, H5E_CANTFILTER, FAIL, "can't decompress compound");
                parms_index = 4;
            }
            break;

        default:
            assert(0 && "This Should never be executed!");
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

static void
H5Z__nbit_compress_one_byte(const unsigned char *data, size_t data_offset, unsigned k, unsigned begin_i,
                            unsigned end_i, unsigned char *buffer, size_t *j, size_t *buf_len,
                            const parms_atomic *p, size_t datatype_len)
{
    size_t        dat_len; /* dat_len is the number of bits to be copied in each data byte */
    unsigned char val;     /* value to be copied in each data byte */

    /* initialize value and bits of unsigned char to be copied */
    val = data[data_offset + k];
    if (begin_i != end_i) { /* significant bits occupy >1 unsigned char */
        if (k == begin_i)
            dat_len = 8 - (datatype_len - p->precision - p->offset) % 8;
        else if (k == end_i) {
            dat_len = 8 - p->offset % 8;
            val     = (unsigned char)(val >> (8 - dat_len));
        }
        else
            dat_len = 8;
    }
    else { /* all significant bits in one unsigned char */
        val     = (unsigned char)(val >> (p->offset % 8));
        dat_len = p->precision;
    }

    if (*buf_len > dat_len) {
        buffer[*j] |= (unsigned char)((val & ~((unsigned)(~0) << dat_len)) << (*buf_len - dat_len));
        *buf_len -= dat_len;
    }
    else {
        buffer[*j] |=
            (unsigned char)((unsigned)(val >> (dat_len - *buf_len)) & ~((unsigned)(~0) << *buf_len));
        dat_len -= *buf_len;
        H5Z__nbit_next_byte(j, buf_len);
        if (dat_len == 0)
            return;

        buffer[*j] = (unsigned char)((val & ~((unsigned)(~0) << dat_len)) << (*buf_len - dat_len));
        *buf_len -= dat_len;
    }
}

static void
H5Z__nbit_compress_one_nooptype(const unsigned char *data, size_t data_offset, unsigned char *buffer,
                                size_t *j, size_t *buf_len, unsigned size)
{
    unsigned      i;       /* index */
    size_t        dat_len; /* dat_len is the number of bits to be copied in each data byte */
    unsigned char val;     /* value to be copied in each data byte */

    for (i = 0; i < size; i++) {
        /* initialize value and bits of unsigned char to be copied */
        val     = data[data_offset + i];
        dat_len = sizeof(unsigned char) * 8;

        buffer[*j] |=
            (unsigned char)((unsigned)(val >> (dat_len - *buf_len)) & ~((unsigned)(~0) << *buf_len));
        dat_len -= *buf_len;
        H5Z__nbit_next_byte(j, buf_len);
        if (dat_len == 0)
            continue;

        buffer[*j] = (unsigned char)((val & ~((unsigned)(~0) << dat_len)) << (*buf_len - dat_len));
        *buf_len -= dat_len;
    }
}

static void
H5Z__nbit_compress_one_atomic(unsigned char *data, size_t data_offset, unsigned char *buffer, size_t *j,
                              size_t *buf_len, const parms_atomic *p)
{
    /* begin_i: the index of byte having first significant bit
       end_i: the index of byte having last significant bit */
    int      k;
    unsigned begin_i, end_i;
    size_t   datatype_len;

    datatype_len = p->size * 8;

    if (p->order == H5Z_NBIT_ORDER_LE) { /* little endian */
        /* calculate begin_i and end_i */
        if ((p->precision + p->offset) % 8 != 0)
            begin_i = (p->precision + p->offset) / 8;
        else
            begin_i = (p->precision + p->offset) / 8 - 1;
        end_i = p->offset / 8;

        for (k = (int)begin_i; k >= (int)end_i; k--)
            H5Z__nbit_compress_one_byte(data, data_offset, (unsigned)k, begin_i, end_i, buffer, j, buf_len, p,
                                        datatype_len);
    }
    else { /* big endian */
        /* Sanity check */
        assert(p->order == H5Z_NBIT_ORDER_BE);

        /* calculate begin_i and end_i */
        begin_i = ((unsigned)datatype_len - p->precision - p->offset) / 8;
        if (p->offset % 8 != 0)
            end_i = ((unsigned)datatype_len - p->offset) / 8;
        else
            end_i = ((unsigned)datatype_len - p->offset) / 8 - 1;

        for (k = (int)begin_i; k <= (int)end_i; k++)
            H5Z__nbit_compress_one_byte(data, data_offset, (unsigned)k, begin_i, end_i, buffer, j, buf_len, p,
                                        datatype_len);
    }
}

static void
H5Z__nbit_compress_one_array(unsigned char *data, size_t data_offset, unsigned char *buffer, size_t *j,
                             size_t *buf_len, const unsigned parms[], unsigned *parms_index)
{
    unsigned     i, total_size, base_class, base_size, n, begin_index;
    parms_atomic p;

    total_size = parms[(*parms_index)++];
    base_class = parms[(*parms_index)++];

    switch (base_class) {
        case H5Z_NBIT_ATOMIC:
            p.size      = parms[(*parms_index)++];
            p.order     = parms[(*parms_index)++];
            p.precision = parms[(*parms_index)++];
            p.offset    = parms[(*parms_index)++];
            n           = total_size / p.size;
            for (i = 0; i < n; i++)
                H5Z__nbit_compress_one_atomic(data, data_offset + i * (size_t)p.size, buffer, j, buf_len, &p);
            break;

        case H5Z_NBIT_ARRAY:
            base_size   = parms[*parms_index];    /* read in advance */
            n           = total_size / base_size; /* number of base_type elements inside the array datatype */
            begin_index = *parms_index;
            for (i = 0; i < n; i++) {
                H5Z__nbit_compress_one_array(data, data_offset + i * (size_t)base_size, buffer, j, buf_len,
                                             parms, parms_index);
                *parms_index = begin_index;
            }
            break;

        case H5Z_NBIT_COMPOUND:
            base_size   = parms[*parms_index];    /* read in advance */
            n           = total_size / base_size; /* number of base_type elements inside the array datatype */
            begin_index = *parms_index;
            for (i = 0; i < n; i++) {
                H5Z__nbit_compress_one_compound(data, data_offset + i * (size_t)base_size, buffer, j, buf_len,
                                                parms, parms_index);
                *parms_index = begin_index;
            }
            break;

        case H5Z_NBIT_NOOPTYPE:
            (*parms_index)++; /* skip size of no-op type */
            H5Z__nbit_compress_one_nooptype(data, data_offset, buffer, j, buf_len, total_size);
            break;

        default:
            assert(0 && "This Should never be executed!");
    } /* end switch */
}

static void
H5Z__nbit_compress_one_compound(unsigned char *data, size_t data_offset, unsigned char *buffer, size_t *j,
                                size_t *buf_len, const unsigned parms[], unsigned *parms_index)
{
    unsigned     i, nmembers, member_offset, member_class, size;
    parms_atomic p;

    (*parms_index)++; /* skip size of compound datatype */
    nmembers = parms[(*parms_index)++];

    for (i = 0; i < nmembers; i++) {
        member_offset = parms[(*parms_index)++];
        member_class  = parms[(*parms_index)++];

        switch (member_class) {
            case H5Z_NBIT_ATOMIC:
                p.size      = parms[(*parms_index)++];
                p.order     = parms[(*parms_index)++];
                p.precision = parms[(*parms_index)++];
                p.offset    = parms[(*parms_index)++];
                H5Z__nbit_compress_one_atomic(data, data_offset + member_offset, buffer, j, buf_len, &p);
                break;

            case H5Z_NBIT_ARRAY:
                H5Z__nbit_compress_one_array(data, data_offset + member_offset, buffer, j, buf_len, parms,
                                             parms_index);
                break;

            case H5Z_NBIT_COMPOUND:
                H5Z__nbit_compress_one_compound(data, data_offset + member_offset, buffer, j, buf_len, parms,
                                                parms_index);
                break;

            case H5Z_NBIT_NOOPTYPE:
                size = parms[(*parms_index)++];
                H5Z__nbit_compress_one_nooptype(data, data_offset + member_offset, buffer, j, buf_len, size);
                break;

            default:
                assert(0 && "This Should never be executed!");
        } /* end switch */
    }
}

static void
H5Z__nbit_compress(unsigned char *data, unsigned d_nelmts, unsigned char *buffer, size_t *buffer_size,
                   const unsigned parms[])
{
    /* i: index of data, new_size: index of buffer,
       buf_len: number of bits to be filled in current byte */
    unsigned     i;
    size_t       size;
    size_t       new_size = 0;
    size_t       buf_len;
    parms_atomic p;
    unsigned     parms_index; /* index in array parms used by compression/decompression functions */

    /* must initialize buffer to be zeros */
    memset(buffer, 0, *buffer_size);

    /* initialization before the loop */
    buf_len = sizeof(unsigned char) * 8;

    switch (parms[3]) {
        case H5Z_NBIT_ATOMIC:
            p.size      = parms[4];
            p.order     = parms[5];
            p.precision = parms[6];
            p.offset    = parms[7];

            for (i = 0; i < d_nelmts; i++)
                H5Z__nbit_compress_one_atomic(data, i * (size_t)p.size, buffer, &new_size, &buf_len, &p);
            break;

        case H5Z_NBIT_ARRAY:
            size        = parms[4];
            parms_index = 4;
            for (i = 0; i < d_nelmts; i++) {
                H5Z__nbit_compress_one_array(data, i * size, buffer, &new_size, &buf_len, parms,
                                             &parms_index);
                parms_index = 4;
            }
            break;

        case H5Z_NBIT_COMPOUND:
            size        = parms[4];
            parms_index = 4;
            for (i = 0; i < d_nelmts; i++) {
                H5Z__nbit_compress_one_compound(data, i * size, buffer, &new_size, &buf_len, parms,
                                                &parms_index);
                parms_index = 4;
            }
            break;

        default:
            assert(0 && "This Should never be executed!");
    } /* end switch */

    /* Update the size to the new value after compression.  If there are any bits hanging over in
     * the last byte, increment the value by 1. */
    *buffer_size = new_size + 1;
}
