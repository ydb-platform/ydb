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

/*-------------------------------------------------------------------------
 *
 * Created:        H5Pdapl.c
 *
 * Purpose:        Dataset access property list class routines
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Pmodule.h" /* This source code file is part of the H5P module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5Dprivate.h"  /* Datasets                                 */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5Fprivate.h"  /* Files                                    */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5MMprivate.h" /* Memory management                        */
#include "H5Ppkg.h"      /* Property lists                           */
#include "H5VMprivate.h" /* Vector Functions                         */

/****************/
/* Local Macros */
/****************/

/* ========= Dataset Access properties ============ */
/* Definitions for size of raw data chunk cache(slots) */
#define H5D_ACS_DATA_CACHE_NUM_SLOTS_SIZE sizeof(size_t)
#define H5D_ACS_DATA_CACHE_NUM_SLOTS_DEF  H5D_CHUNK_CACHE_NSLOTS_DEFAULT
#define H5D_ACS_DATA_CACHE_NUM_SLOTS_ENC  H5P__encode_chunk_cache_nslots
#define H5D_ACS_DATA_CACHE_NUM_SLOTS_DEC  H5P__decode_chunk_cache_nslots
/* Definition for size of raw data chunk cache(bytes) */
#define H5D_ACS_DATA_CACHE_BYTE_SIZE_SIZE sizeof(size_t)
#define H5D_ACS_DATA_CACHE_BYTE_SIZE_DEF  H5D_CHUNK_CACHE_NBYTES_DEFAULT
#define H5D_ACS_DATA_CACHE_BYTE_SIZE_ENC  H5P__encode_chunk_cache_nbytes
#define H5D_ACS_DATA_CACHE_BYTE_SIZE_DEC  H5P__decode_chunk_cache_nbytes
/* Definition for preemption read chunks first */
#define H5D_ACS_PREEMPT_READ_CHUNKS_SIZE sizeof(double)
#define H5D_ACS_PREEMPT_READ_CHUNKS_DEF  H5D_CHUNK_CACHE_W0_DEFAULT
#define H5D_ACS_PREEMPT_READ_CHUNKS_ENC  H5P__encode_double
#define H5D_ACS_PREEMPT_READ_CHUNKS_DEC  H5P__decode_double
/* Definitions for VDS view option */
#define H5D_ACS_VDS_VIEW_SIZE sizeof(H5D_vds_view_t)
#define H5D_ACS_VDS_VIEW_DEF  H5D_VDS_LAST_AVAILABLE
#define H5D_ACS_VDS_VIEW_ENC  H5P__dacc_vds_view_enc
#define H5D_ACS_VDS_VIEW_DEC  H5P__dacc_vds_view_dec
/* Definitions for VDS printf gap */
#define H5D_ACS_VDS_PRINTF_GAP_SIZE sizeof(hsize_t)
#define H5D_ACS_VDS_PRINTF_GAP_DEF  (hsize_t)0
#define H5D_ACS_VDS_PRINTF_GAP_ENC  H5P__encode_hsize_t
#define H5D_ACS_VDS_PRINTF_GAP_DEC  H5P__decode_hsize_t
/* Definitions for VDS file prefix */
#define H5D_ACS_VDS_PREFIX_SIZE  sizeof(char *)
#define H5D_ACS_VDS_PREFIX_DEF   NULL /*default is no prefix */
#define H5D_ACS_VDS_PREFIX_SET   H5P__dapl_vds_file_pref_set
#define H5D_ACS_VDS_PREFIX_GET   H5P__dapl_vds_file_pref_get
#define H5D_ACS_VDS_PREFIX_ENC   H5P__dapl_vds_file_pref_enc
#define H5D_ACS_VDS_PREFIX_DEC   H5P__dapl_vds_file_pref_dec
#define H5D_ACS_VDS_PREFIX_DEL   H5P__dapl_vds_file_pref_del
#define H5D_ACS_VDS_PREFIX_COPY  H5P__dapl_vds_file_pref_copy
#define H5D_ACS_VDS_PREFIX_CMP   H5P__dapl_vds_file_pref_cmp
#define H5D_ACS_VDS_PREFIX_CLOSE H5P__dapl_vds_file_pref_close
/* Definition for append flush */
#define H5D_ACS_APPEND_FLUSH_SIZE sizeof(H5D_append_flush_t)
#define H5D_ACS_APPEND_FLUSH_DEF                                                                             \
    {                                                                                                        \
        0, {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, \
            NULL, NULL                                                                                       \
    }
/* Definitions for external file prefix */
#define H5D_ACS_EFILE_PREFIX_SIZE  sizeof(char *)
#define H5D_ACS_EFILE_PREFIX_DEF   NULL /*default is no prefix */
#define H5D_ACS_EFILE_PREFIX_SET   H5P__dapl_efile_pref_set
#define H5D_ACS_EFILE_PREFIX_GET   H5P__dapl_efile_pref_get
#define H5D_ACS_EFILE_PREFIX_ENC   H5P__dapl_efile_pref_enc
#define H5D_ACS_EFILE_PREFIX_DEC   H5P__dapl_efile_pref_dec
#define H5D_ACS_EFILE_PREFIX_DEL   H5P__dapl_efile_pref_del
#define H5D_ACS_EFILE_PREFIX_COPY  H5P__dapl_efile_pref_copy
#define H5D_ACS_EFILE_PREFIX_CMP   H5P__dapl_efile_pref_cmp
#define H5D_ACS_EFILE_PREFIX_CLOSE H5P__dapl_efile_pref_close

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

/* Property class callbacks */
static herr_t H5P__dacc_reg_prop(H5P_genclass_t *pclass);
static herr_t H5P__encode_chunk_cache_nslots(const void *value, void **_pp, size_t *size);
static herr_t H5P__decode_chunk_cache_nslots(const void **_pp, void *_value);
static herr_t H5P__encode_chunk_cache_nbytes(const void *value, void **_pp, size_t *size);
static herr_t H5P__decode_chunk_cache_nbytes(const void **_pp, void *_value);

/* Property list callbacks */
static herr_t H5P__dacc_vds_view_enc(const void *value, void **pp, size_t *size);
static herr_t H5P__dacc_vds_view_dec(const void **pp, void *value);
static herr_t H5P__dapl_vds_file_pref_set(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__dapl_vds_file_pref_get(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__dapl_vds_file_pref_enc(const void *value, void **_pp, size_t *size);
static herr_t H5P__dapl_vds_file_pref_dec(const void **_pp, void *value);
static herr_t H5P__dapl_vds_file_pref_del(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__dapl_vds_file_pref_copy(const char *name, size_t size, void *value);
static int    H5P__dapl_vds_file_pref_cmp(const void *value1, const void *value2, size_t size);
static herr_t H5P__dapl_vds_file_pref_close(const char *name, size_t size, void *value);

/* Property list callbacks */
static herr_t H5P__dapl_efile_pref_set(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__dapl_efile_pref_get(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__dapl_efile_pref_enc(const void *value, void **_pp, size_t *size);
static herr_t H5P__dapl_efile_pref_dec(const void **_pp, void *value);
static herr_t H5P__dapl_efile_pref_del(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__dapl_efile_pref_copy(const char *name, size_t size, void *value);
static int    H5P__dapl_efile_pref_cmp(const void *value1, const void *value2, size_t size);
static herr_t H5P__dapl_efile_pref_close(const char *name, size_t size, void *value);

/*********************/
/* Package Variables */
/*********************/

/* Dataset access property list class library initialization object */
const H5P_libclass_t H5P_CLS_DACC[1] = {{
    "dataset access",        /* Class name for debugging     */
    H5P_TYPE_DATASET_ACCESS, /* Class type                   */

    &H5P_CLS_LINK_ACCESS_g,       /* Parent class                 */
    &H5P_CLS_DATASET_ACCESS_g,    /* Pointer to class             */
    &H5P_CLS_DATASET_ACCESS_ID_g, /* Pointer to class ID          */
    &H5P_LST_DATASET_ACCESS_ID_g, /* Pointer to default property list ID */
    H5P__dacc_reg_prop,           /* Default property registration routine */

    NULL, /* Class creation callback      */
    NULL, /* Class creation callback info */
    NULL, /* Class copy callback          */
    NULL, /* Class copy callback info     */
    NULL, /* Class close callback         */
    NULL  /* Class close callback info    */
}};

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Property value defaults */
static const H5D_append_flush_t H5D_def_append_flush_g =
    H5D_ACS_APPEND_FLUSH_DEF; /* Default setting for append flush */
static const char *H5D_def_efile_prefix_g =
    H5D_ACS_EFILE_PREFIX_DEF;                                     /* Default external file prefix string */
static const char *H5D_def_vds_prefix_g = H5D_ACS_VDS_PREFIX_DEF; /* Default vds prefix string */

/*-------------------------------------------------------------------------
 * Function:    H5P__dacc_reg_prop
 *
 * Purpose:     Register the dataset access property list class's
 *              properties
 *
 * Return:      Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dacc_reg_prop(H5P_genclass_t *pclass)
{
    size_t rdcc_nslots = H5D_ACS_DATA_CACHE_NUM_SLOTS_DEF;    /* Default raw data chunk cache # of slots */
    size_t rdcc_nbytes = H5D_ACS_DATA_CACHE_BYTE_SIZE_DEF;    /* Default raw data chunk cache # of bytes */
    double rdcc_w0     = H5D_ACS_PREEMPT_READ_CHUNKS_DEF;     /* Default raw data chunk cache dirty ratio */
    H5D_vds_view_t virtual_view = H5D_ACS_VDS_VIEW_DEF;       /* Default VDS view option */
    hsize_t        printf_gap   = H5D_ACS_VDS_PRINTF_GAP_DEF; /* Default VDS printf gap */
    herr_t         ret_value    = SUCCEED;                    /* Return value */

    FUNC_ENTER_PACKAGE

    /* Register the size of raw data chunk cache (elements) */
    if (H5P__register_real(pclass, H5D_ACS_DATA_CACHE_NUM_SLOTS_NAME, H5D_ACS_DATA_CACHE_NUM_SLOTS_SIZE,
                           &rdcc_nslots, NULL, NULL, NULL, H5D_ACS_DATA_CACHE_NUM_SLOTS_ENC,
                           H5D_ACS_DATA_CACHE_NUM_SLOTS_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the size of raw data chunk cache(bytes) */
    if (H5P__register_real(pclass, H5D_ACS_DATA_CACHE_BYTE_SIZE_NAME, H5D_ACS_DATA_CACHE_BYTE_SIZE_SIZE,
                           &rdcc_nbytes, NULL, NULL, NULL, H5D_ACS_DATA_CACHE_BYTE_SIZE_ENC,
                           H5D_ACS_DATA_CACHE_BYTE_SIZE_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the preemption for reading chunks */
    if (H5P__register_real(pclass, H5D_ACS_PREEMPT_READ_CHUNKS_NAME, H5D_ACS_PREEMPT_READ_CHUNKS_SIZE,
                           &rdcc_w0, NULL, NULL, NULL, H5D_ACS_PREEMPT_READ_CHUNKS_ENC,
                           H5D_ACS_PREEMPT_READ_CHUNKS_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the VDS view option */
    if (H5P__register_real(pclass, H5D_ACS_VDS_VIEW_NAME, H5D_ACS_VDS_VIEW_SIZE, &virtual_view, NULL, NULL,
                           NULL, H5D_ACS_VDS_VIEW_ENC, H5D_ACS_VDS_VIEW_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the VDS printf gap */
    if (H5P__register_real(pclass, H5D_ACS_VDS_PRINTF_GAP_NAME, H5D_ACS_VDS_PRINTF_GAP_SIZE, &printf_gap,
                           NULL, NULL, NULL, H5D_ACS_VDS_PRINTF_GAP_ENC, H5D_ACS_VDS_PRINTF_GAP_DEC, NULL,
                           NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register property for vds prefix */
    if (H5P__register_real(pclass, H5D_ACS_VDS_PREFIX_NAME, H5D_ACS_VDS_PREFIX_SIZE, &H5D_def_vds_prefix_g,
                           NULL, H5D_ACS_VDS_PREFIX_SET, H5D_ACS_VDS_PREFIX_GET, H5D_ACS_VDS_PREFIX_ENC,
                           H5D_ACS_VDS_PREFIX_DEC, H5D_ACS_VDS_PREFIX_DEL, H5D_ACS_VDS_PREFIX_COPY,
                           H5D_ACS_VDS_PREFIX_CMP, H5D_ACS_VDS_PREFIX_CLOSE) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register info for append flush */
    /* (Note: this property should not have an encode/decode callback -QAK) */
    if (H5P__register_real(pclass, H5D_ACS_APPEND_FLUSH_NAME, H5D_ACS_APPEND_FLUSH_SIZE,
                           &H5D_def_append_flush_g, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register property for external file prefix */
    if (H5P__register_real(pclass, H5D_ACS_EFILE_PREFIX_NAME, H5D_ACS_EFILE_PREFIX_SIZE,
                           &H5D_def_efile_prefix_g, NULL, H5D_ACS_EFILE_PREFIX_SET, H5D_ACS_EFILE_PREFIX_GET,
                           H5D_ACS_EFILE_PREFIX_ENC, H5D_ACS_EFILE_PREFIX_DEC, H5D_ACS_EFILE_PREFIX_DEL,
                           H5D_ACS_EFILE_PREFIX_COPY, H5D_ACS_EFILE_PREFIX_CMP,
                           H5D_ACS_EFILE_PREFIX_CLOSE) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dacc_reg_prop() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dapl_vds_file_pref_set
 *
 * Purpose:     Copies a vds file prefix property when it's set
 *              for a property list
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dapl_vds_file_pref_set(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                            size_t H5_ATTR_UNUSED size, void *value)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(value);

    /* Copy the prefix */
    *(char **)value = H5MM_xstrdup(*(const char **)value);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dapl_vds_file_pref_set() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dapl_vds_file_pref_get
 *
 * Purpose:     Copies a vds file prefix property when it's retrieved
 *              from a property list
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dapl_vds_file_pref_get(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                            size_t H5_ATTR_UNUSED size, void *value)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(value);

    /* Copy the prefix */
    *(char **)value = H5MM_xstrdup(*(const char **)value);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dapl_vds_file_pref_get() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dapl_vds_file_pref_enc
 *
 * Purpose:     Callback routine which is called whenever the vds file flags
 *              property in the dataset access property list is
 *              encoded.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dapl_vds_file_pref_enc(const void *value, void **_pp, size_t *size)
{
    const char *vds_file_pref = *(const char *const *)value;
    uint8_t   **pp            = (uint8_t **)_pp;
    size_t      len           = 0;
    uint64_t    enc_value;
    unsigned    enc_size;

    FUNC_ENTER_PACKAGE_NOERR

    HDcompile_assert(sizeof(size_t) <= sizeof(uint64_t));

    /* calculate prefix length */
    if (NULL != vds_file_pref)
        len = strlen(vds_file_pref);

    enc_value = (uint64_t)len;
    enc_size  = H5VM_limit_enc_size(enc_value);
    assert(enc_size < 256);

    if (NULL != *pp) {
        /* encode the length of the prefix */
        *(*pp)++ = (uint8_t)enc_size;
        UINT64ENCODE_VAR(*pp, enc_value, enc_size);

        /* encode the prefix */
        if (NULL != vds_file_pref) {
            H5MM_memcpy(*(char **)pp, vds_file_pref, len);
            *pp += len;
        } /* end if */
    }     /* end if */

    *size += (1 + enc_size);
    if (NULL != vds_file_pref)
        *size += len;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dapl_vds_file_pref_enc() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dapl_vds_file_pref_dec
 *
 * Purpose:     Callback routine which is called whenever the vds file prefix
 *              property in the dataset access property list is
 *              decoded.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dapl_vds_file_pref_dec(const void **_pp, void *_value)
{
    char          **vds_file_pref = (char **)_value;
    const uint8_t **pp            = (const uint8_t **)_pp;
    size_t          len;
    uint64_t        enc_value; /* Decoded property value */
    unsigned        enc_size;  /* Size of encoded property */
    herr_t          ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(pp);
    assert(*pp);
    assert(vds_file_pref);
    HDcompile_assert(sizeof(size_t) <= sizeof(uint64_t));

    /* Decode the size */
    enc_size = *(*pp)++;
    assert(enc_size < 256);

    /* Decode the value */
    UINT64DECODE_VAR(*pp, enc_value, enc_size);
    len = (size_t)enc_value;

    if (0 != len) {
        /* Make a copy of the user's prefix string */
        if (NULL == (*vds_file_pref = (char *)H5MM_malloc(len + 1)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTINIT, FAIL, "memory allocation failed for prefix");
        strncpy(*vds_file_pref, *(const char **)pp, len);
        (*vds_file_pref)[len] = '\0';

        *pp += len;
    } /* end if */
    else
        *vds_file_pref = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dapl_vds_file_pref_dec() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dapl_vds_file_pref_del
 *
 * Purpose:     Frees memory used to store the vds file prefix string
 *
 * Return:      SUCCEED (Can't fail)
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dapl_vds_file_pref_del(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                            size_t H5_ATTR_UNUSED size, void *value)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(value);

    H5MM_xfree(*(void **)value);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dapl_vds_file_pref_del() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dapl_vds_file_pref_copy
 *
 * Purpose:     Creates a copy of the vds file prefix string
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dapl_vds_file_pref_copy(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(value);

    *(char **)value = H5MM_xstrdup(*(const char **)value);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dapl_vds_file_pref_copy() */

/*-------------------------------------------------------------------------
 * Function:       H5P__dapl_vds_file_pref_cmp
 *
 * Purpose:        Callback routine which is called whenever the vds file prefix
 *                 property in the dataset creation property list is
 *                 compared.
 *
 * Return:         zero if VALUE1 and VALUE2 are equal, non zero otherwise.
 *-------------------------------------------------------------------------
 */
static int
H5P__dapl_vds_file_pref_cmp(const void *value1, const void *value2, size_t H5_ATTR_UNUSED size)
{
    const char *pref1     = *(const char *const *)value1;
    const char *pref2     = *(const char *const *)value2;
    int         ret_value = 0;

    FUNC_ENTER_PACKAGE_NOERR

    if (NULL == pref1 && NULL != pref2)
        HGOTO_DONE(1);
    if (NULL != pref1 && NULL == pref2)
        HGOTO_DONE(-1);
    if (NULL != pref1 && NULL != pref2)
        ret_value = strcmp(pref1, pref2);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dapl_vds_file_pref_cmp() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dapl_vds_file_pref_close
 *
 * Purpose:     Frees memory used to store the vds file prefix string
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dapl_vds_file_pref_close(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(value);

    H5MM_xfree(*(void **)value);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dapl_vds_file_pref_close() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dapl_efile_pref_set
 *
 * Purpose:     Copies an external file prefix property when it's set
 *              for a property list
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dapl_efile_pref_set(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                         size_t H5_ATTR_UNUSED size, void *value)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(value);

    /* Copy the prefix */
    *(char **)value = H5MM_xstrdup(*(const char **)value);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dapl_efile_pref_set() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dapl_efile_pref_get
 *
 * Purpose:     Copies an external file prefix property when it's retrieved
 *              from a property list
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dapl_efile_pref_get(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                         size_t H5_ATTR_UNUSED size, void *value)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(value);

    /* Copy the prefix */
    *(char **)value = H5MM_xstrdup(*(const char **)value);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dapl_efile_pref_get() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dapl_efile_pref_enc
 *
 * Purpose:     Callback routine which is called whenever the efile flags
 *              property in the dataset access property list is
 *              encoded.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dapl_efile_pref_enc(const void *value, void **_pp, size_t *size)
{
    const char *efile_pref = *(const char *const *)value;
    uint8_t   **pp         = (uint8_t **)_pp;
    size_t      len        = 0;
    uint64_t    enc_value;
    unsigned    enc_size;

    FUNC_ENTER_PACKAGE_NOERR

    HDcompile_assert(sizeof(size_t) <= sizeof(uint64_t));

    /* calculate prefix length */
    if (NULL != efile_pref)
        len = strlen(efile_pref);

    enc_value = (uint64_t)len;
    enc_size  = H5VM_limit_enc_size(enc_value);
    assert(enc_size < 256);

    if (NULL != *pp) {
        /* encode the length of the prefix */
        *(*pp)++ = (uint8_t)enc_size;
        UINT64ENCODE_VAR(*pp, enc_value, enc_size);

        /* encode the prefix */
        if (NULL != efile_pref) {
            H5MM_memcpy(*(char **)pp, efile_pref, len);
            *pp += len;
        } /* end if */
    }     /* end if */

    *size += (1 + enc_size);
    if (NULL != efile_pref)
        *size += len;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dapl_efile_pref_enc() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dapl_efile_pref_dec
 *
 * Purpose:     Callback routine which is called whenever the efile prefix
 *              property in the dataset access property list is
 *              decoded.
 *
 * Return:        SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dapl_efile_pref_dec(const void **_pp, void *_value)
{
    char          **efile_pref = (char **)_value;
    const uint8_t **pp         = (const uint8_t **)_pp;
    size_t          len;
    uint64_t        enc_value; /* Decoded property value */
    unsigned        enc_size;  /* Size of encoded property */
    herr_t          ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(pp);
    assert(*pp);
    assert(efile_pref);
    HDcompile_assert(sizeof(size_t) <= sizeof(uint64_t));

    /* Decode the size */
    enc_size = *(*pp)++;
    assert(enc_size < 256);

    /* Decode the value */
    UINT64DECODE_VAR(*pp, enc_value, enc_size);
    len = (size_t)enc_value;

    if (0 != len) {
        /* Make a copy of the user's prefix string */
        if (NULL == (*efile_pref = (char *)H5MM_malloc(len + 1)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTINIT, FAIL, "memory allocation failed for prefix");
        strncpy(*efile_pref, *(const char **)pp, len);
        (*efile_pref)[len] = '\0';

        *pp += len;
    } /* end if */
    else
        *efile_pref = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dapl_efile_pref_dec() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dapl_efile_pref_del
 *
 * Purpose:     Frees memory used to store the external file prefix string
 *
 * Return:      SUCCEED (Can't fail)
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dapl_efile_pref_del(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                         size_t H5_ATTR_UNUSED size, void *value)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(value);

    H5MM_xfree(*(void **)value);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dapl_efile_pref_del() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dapl_efile_pref_copy
 *
 * Purpose:     Creates a copy of the external file prefix string
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dapl_efile_pref_copy(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(value);

    *(char **)value = H5MM_xstrdup(*(const char **)value);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dapl_efile_pref_copy() */

/*-------------------------------------------------------------------------
 * Function:       H5P__dapl_efile_pref_cmp
 *
 * Purpose:        Callback routine which is called whenever the efile prefix
 *                 property in the dataset creation property list is
 *                 compared.
 *
 * Return:         zero if VALUE1 and VALUE2 are equal, non zero otherwise.
 *-------------------------------------------------------------------------
 */
static int
H5P__dapl_efile_pref_cmp(const void *value1, const void *value2, size_t H5_ATTR_UNUSED size)
{
    const char *pref1     = *(const char *const *)value1;
    const char *pref2     = *(const char *const *)value2;
    int         ret_value = 0;

    FUNC_ENTER_PACKAGE_NOERR

    if (NULL == pref1 && NULL != pref2)
        HGOTO_DONE(1);
    if (NULL != pref1 && NULL == pref2)
        HGOTO_DONE(-1);
    if (NULL != pref1 && NULL != pref2)
        ret_value = strcmp(pref1, pref2);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dapl_efile_pref_cmp() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dapl_efile_pref_close
 *
 * Purpose:     Frees memory used to store the external file prefix string
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dapl_efile_pref_close(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(value);

    H5MM_xfree(*(void **)value);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dapl_efile_pref_close() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_chunk_cache
 *
 * Purpose:    Set the number of objects in the meta data cache and the
 *        maximum number of chunks and bytes in the raw data chunk cache.
 *        Once set, these values will override the values in the file access
 *        property list.  Each of these values can be individually unset
 *        (or not set at all) by passing the macros:
 *        H5D_CHUNK_CACHE_NCHUNKS_DEFAULT,
 *        H5D_CHUNK_CACHE_NSLOTS_DEFAULT, and/or
 *        H5D_CHUNK_CACHE_W0_DEFAULT
 *        as appropriate.
 *
 *        The RDCC_W0 value should be between 0 and 1 inclusive and
 *        indicates how much chunks that have been fully read or fully
 *        written are favored for preemption.  A value of zero means
 *        fully read or written chunks are treated no differently than
 *        other chunks (the preemption is strictly LRU) while a value
 *        of one means fully read chunks are always preempted before
 *        other chunks.
 *
 * Return:    Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_chunk_cache(hid_t dapl_id, size_t rdcc_nslots, size_t rdcc_nbytes, double rdcc_w0)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "izzd", dapl_id, rdcc_nslots, rdcc_nbytes, rdcc_w0);

    /* Check arguments.  Note that we allow negative values - they are
     * considered to "unset" the property. */
    if (rdcc_w0 > 1.0)
        HGOTO_ERROR(
            H5E_ARGS, H5E_BADVALUE, FAIL,
            "raw data cache w0 value must be between 0.0 and 1.0 inclusive, or H5D_CHUNK_CACHE_W0_DEFAULT");

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(dapl_id, H5P_DATASET_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set sizes */
    if (H5P_set(plist, H5D_ACS_DATA_CACHE_NUM_SLOTS_NAME, &rdcc_nslots) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set data cache number of chunks");
    if (H5P_set(plist, H5D_ACS_DATA_CACHE_BYTE_SIZE_NAME, &rdcc_nbytes) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set data cache byte size");
    if (H5P_set(plist, H5D_ACS_PREEMPT_READ_CHUNKS_NAME, &rdcc_w0) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set preempt read chunks");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_chunk_cache() */

/*-------------------------------------------------------------------------
 * Function: H5Pget_chunk_cache
 *
 * Purpose:  Retrieves the maximum possible number of elements in the meta
 *        data cache and the maximum possible number of elements and
 *        bytes and the RDCC_W0 value in the raw data chunk cache.  Any
 *        (or all) arguments may be null pointers in which case the
 *        corresponding datum is not returned.  If these properties have
 *        not been set on this property list, the default values for a
 *        file access property list are returned.
 *
 * Return:  Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_chunk_cache(hid_t dapl_id, size_t *rdcc_nslots /*out*/, size_t *rdcc_nbytes /*out*/,
                   double *rdcc_w0 /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    H5P_genplist_t *def_plist;           /* Default file access property list */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "ixxx", dapl_id, rdcc_nslots, rdcc_nbytes, rdcc_w0);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(dapl_id, H5P_DATASET_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get default file access plist */
    if (NULL == (def_plist = (H5P_genplist_t *)H5I_object(H5P_FILE_ACCESS_DEFAULT)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for default fapl ID");

    /* Get the properties.  If a property is set to the default value, the value
     * from the default fapl is used. */
    if (rdcc_nslots) {
        if (H5P_get(plist, H5D_ACS_DATA_CACHE_NUM_SLOTS_NAME, rdcc_nslots) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get data cache number of slots");
        if (*rdcc_nslots == H5D_CHUNK_CACHE_NSLOTS_DEFAULT)
            if (H5P_get(def_plist, H5F_ACS_DATA_CACHE_NUM_SLOTS_NAME, rdcc_nslots) < 0)
                HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get default data cache number of slots");
    } /* end if */
    if (rdcc_nbytes) {
        if (H5P_get(plist, H5D_ACS_DATA_CACHE_BYTE_SIZE_NAME, rdcc_nbytes) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get data cache byte size");
        if (*rdcc_nbytes == H5D_CHUNK_CACHE_NBYTES_DEFAULT)
            if (H5P_get(def_plist, H5F_ACS_DATA_CACHE_BYTE_SIZE_NAME, rdcc_nbytes) < 0)
                HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get default data cache byte size");
    } /* end if */
    if (rdcc_w0) {
        if (H5P_get(plist, H5D_ACS_PREEMPT_READ_CHUNKS_NAME, rdcc_w0) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get preempt read chunks");
        if (*rdcc_w0 < 0)
            if (H5P_get(def_plist, H5F_ACS_PREEMPT_READ_CHUNKS_NAME, rdcc_w0) < 0)
                HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get default preempt read chunks");
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_chunk_cache() */

/*-------------------------------------------------------------------------
 * Function:       H5P__encode_chunk_cache_nslots
 *
 * Purpose:        Encode the rdcc_nslots parameter to a serialized
 *                 property list.  Similar to H5P__encode_size_t except
 *                 the value of 255 for the enc_size field is reserved to
 *                 indicate H5D_ACS_DATA_CACHE_NUM_SLOTS_DEF, in which
 *                 nothing further is encoded.
 *
 * Return:         Success:     Non-negative
 *                 Failure:     Negative
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__encode_chunk_cache_nslots(const void *value, void **_pp, size_t *size)
{
    uint64_t  enc_value = 0; /* Property value to encode */
    uint8_t **pp        = (uint8_t **)_pp;
    unsigned  enc_size; /* Size of encoded property */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    HDcompile_assert(sizeof(size_t) <= sizeof(uint64_t));
    assert(size);

    /* Determine if this is the default value, in which case only encode
     * enc_size (as 255).  Also set size needed for encoding. */
    if (*(const size_t *)value == H5D_ACS_DATA_CACHE_NUM_SLOTS_DEF) {
        enc_size = 0;
        *size += 1;
    } /* end if */
    else {
        enc_value = (uint64_t) * (const size_t *)value;
        enc_size  = H5VM_limit_enc_size(enc_value);
        assert(enc_size > 0);
        *size += (1 + enc_size);
    } /* end else */

    assert(enc_size < 256);

    if (NULL != *pp) {
        /* Encode the size */
        *(*pp)++ = (uint8_t)enc_size;

        /* Encode the value if necessary */
        if (enc_size != 0) {
            UINT64ENCODE_VAR(*pp, enc_value, enc_size);
        } /* end if */
    }     /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__encode_chunk_cache_nslots() */

/*-------------------------------------------------------------------------
 * Function:       H5P__decode_chunk_cache_nslots
 *
 * Purpose:        Decode the rdcc_nslots parameter from a serialized
 *                 property list.  Similar to H5P__decode_size_t except
 *                 the value of 255 for the enc_size field is reserved to
 *                 indicate H5D_ACS_DATA_CACHE_NUM_SLOTS_DEF, in which
 *                 nothing further needs to be decoded.
 *
 * Return:         Success:     Non-negative
 *                 Failure:     Negative
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__decode_chunk_cache_nslots(const void **_pp, void *_value)
{
    size_t         *value = (size_t *)_value; /* Property value to return */
    const uint8_t **pp    = (const uint8_t **)_pp;
    uint64_t        enc_value; /* Decoded property value */
    unsigned        enc_size;  /* Size of encoded property */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    HDcompile_assert(sizeof(size_t) <= sizeof(uint64_t));
    assert(pp);
    assert(*pp);
    assert(value);

    /* Decode the size */
    enc_size = *(*pp)++;
    assert(enc_size < 256);

    /* Determine if enc_size indicates that this is the default value, in which
     * case set value to H5D_ACS_DATA_CACHE_NUM_SLOTS_DEF and return */
    if (enc_size == 0)
        *value = H5D_ACS_DATA_CACHE_NUM_SLOTS_DEF;
    else {
        /* Decode the value */
        UINT64DECODE_VAR(*pp, enc_value, enc_size);
        H5_CHECKED_ASSIGN(*value, uint64_t, enc_value, size_t);
    } /* end else */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__decode_chunk_cache_nslots() */

/*-------------------------------------------------------------------------
 * Function:       H5P__encode_chunk_cache_nbytes
 *
 * Purpose:        Encode the rdcc_nbytes parameter to a serialized
 *                 property list.  Similar to H5P__encode_size_t except
 *                 the value of 255 for the enc_size field is reserved to
 *                 indicate H5D_ACS_DATA_CACHE_BYTE_SIZE_DEF, in which
 *                 nothing further is encoded.
 *
 * Return:         Success:     Non-negative
 *                 Failure:     Negative
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__encode_chunk_cache_nbytes(const void *value, void **_pp, size_t *size)
{
    uint64_t  enc_value = 0; /* Property value to encode */
    uint8_t **pp        = (uint8_t **)_pp;
    unsigned  enc_size; /* Size of encoded property */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    HDcompile_assert(sizeof(size_t) <= sizeof(uint64_t));
    assert(size);

    /* Determine if this is the default value, in which case only encode
     * enc_size (as 255).  Also set size needed for encoding. */
    if (*(const size_t *)value == H5D_ACS_DATA_CACHE_BYTE_SIZE_DEF) {
        enc_size = 0;
        *size += 1;
    } /* end if */
    else {
        enc_value = (uint64_t) * (const size_t *)value;
        enc_size  = H5VM_limit_enc_size(enc_value);
        assert(enc_size > 0);
        *size += (1 + enc_size);
    } /* end else */

    assert(enc_size < 256);

    if (NULL != *pp) {
        /* Encode the size */
        *(*pp)++ = (uint8_t)enc_size;

        /* Encode the value if necessary */
        if (enc_size != 0) {
            UINT64ENCODE_VAR(*pp, enc_value, enc_size);
        } /* end if */
    }     /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__encode_chunk_cache_nbytes() */

/*-------------------------------------------------------------------------
 * Function:       H5P__decode_chunk_cache_nbytes
 *
 * Purpose:        Decode the rdcc_nbytes parameter from a serialized
 *                 property list.  Similar to H5P__decode_size_t except
 *                 the value of 255 for the enc_size field is reserved to
 *                 indicate H5D_ACS_DATA_CACHE_BYTE_SIZE_DEF, in which
 *                 nothing further needs to be decoded.
 *
 * Return:         Success:     Non-negative
 *                 Failure:     Negative
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__decode_chunk_cache_nbytes(const void **_pp, void *_value)
{
    size_t         *value = (size_t *)_value; /* Property value to return */
    const uint8_t **pp    = (const uint8_t **)_pp;
    uint64_t        enc_value; /* Decoded property value */
    unsigned        enc_size;  /* Size of encoded property */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    HDcompile_assert(sizeof(size_t) <= sizeof(uint64_t));
    assert(pp);
    assert(*pp);
    assert(value);

    /* Decode the size */
    enc_size = *(*pp)++;
    assert(enc_size < 256);

    /* Determine if enc_size indicates that this is the default value, in which
     * case set value to H5D_ACS_DATA_CACHE_BYTE_SIZE_DEF and return */
    if (enc_size == 0)
        *value = H5D_ACS_DATA_CACHE_BYTE_SIZE_DEF;
    else {
        /* Decode the value */
        UINT64DECODE_VAR(*pp, enc_value, enc_size);
        H5_CHECKED_ASSIGN(*value, uint64_t, enc_value, size_t);
    } /* end else */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__decode_chunk_cache_nbytes() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_virtual_view
 *
 * Purpose:     Takes the access property list for the virtual dataset,
 *              dapl_id, and the flag, view, and sets the VDS view
 *              according to the flag value.  The view will include all
 *              data before the first missing mapped data found if the
 *              flag is set to H5D_VDS_FIRST_MISSING or to include all
 *              available mapped data if the flag is set to
 *              H5D_VDS_LAST_AVAIALBLE.  Missing mapped data will be
 *              filled with the fill value according to the VDS creation
 *              property settings.  For VDS with unlimited mappings, the
 *              view defines the extent.
 *
 * Return:      Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_virtual_view(hid_t plist_id, H5D_vds_view_t view)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iDv", plist_id, view);

    /* Check argument */
    if ((view != H5D_VDS_FIRST_MISSING) && (view != H5D_VDS_LAST_AVAILABLE))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a valid bounds option");

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Update property list */
    if (H5P_set(plist, H5D_ACS_VDS_VIEW_NAME, &view) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "unable to set value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_virtual_view() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_virtual_view
 *
 * Purpose:     Takes the access property list for the virtual dataset,
 *              dapl_id, and gets the flag, view, set by the
 *              H5Pset_virtual_view call.  The possible values of view are
 *              H5D_VDS_FIRST_MISSING or H5D_VDS_LAST_AVAIALBLE.
 *
 * Return:      Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_virtual_view(hid_t plist_id, H5D_vds_view_t *view /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", plist_id, view);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get value from property list */
    if (view)
        if (H5P_get(plist, H5D_ACS_VDS_VIEW_NAME, view) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "unable to get value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_virtual_view() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dacc_vds_view_enc
 *
 * Purpose:     Callback routine which is called whenever the vds view
 *              property in the dataset access property list is encoded.
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dacc_vds_view_enc(const void *value, void **_pp, size_t *size)
{
    const H5D_vds_view_t *view = (const H5D_vds_view_t *)value; /* Create local alias for values */
    uint8_t             **pp   = (uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(view);
    assert(size);

    if (NULL != *pp)
        /* Encode EDC property */
        *(*pp)++ = (uint8_t)*view;

    /* Size of EDC property */
    (*size)++;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dacc_vds_view_enc() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dacc_vds_view_dec
 *
 * Purpose:     Callback routine which is called whenever the vds view
 *              property in the dataset access property list is encoded.
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dacc_vds_view_dec(const void **_pp, void *_value)
{
    H5D_vds_view_t *view = (H5D_vds_view_t *)_value;
    const uint8_t **pp   = (const uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(pp);
    assert(*pp);
    assert(view);

    /* Decode EDC property */
    *view = (H5D_vds_view_t) * (*pp)++;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dacc_vds_view_dec() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_virtual_printf_gap
 *
 * Purpose:     Sets the access property list for the virtual dataset,
 *              dapl_id, to instruct the library to stop looking for the
 *              mapped data stored in the files and/or datasets with the
 *              printf-style names after not finding gap_size files and/or
 *              datasets.  The found source files and datasets will
 *              determine the extent of the unlimited VDS with the printf
 *              -style mappings.
 *
 *              For example, if regularly spaced blocks of VDS are mapped
 *              to datasets with the names d-1, d-2, d-3, ..., d-N, ...,
 *              and d-2 dataset is missing and gap_size is set to 0, then
 *              VDS will contain only data found in d-1.  If d-2 and d-3
 *              are missing and gap_size is set to 2, then VDS will
 *              contain the data from d-1, d-3, ..., d-N, ....  The blocks
 *              that are mapped to d-2 and d-3 will be filled according to
 *              the VDS fill value setting.
 *
 * Return:      Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_virtual_printf_gap(hid_t plist_id, hsize_t gap_size)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ih", plist_id, gap_size);

    /* Check argument */
    if (gap_size == HSIZE_UNDEF)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a valid printf gap size");

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Update property list */
    if (H5P_set(plist, H5D_ACS_VDS_PRINTF_GAP_NAME, &gap_size) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "unable to set value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_virtual_printf_gap() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_virtual_printf_gap
 *
 * Purpose:     Gets the maximum number of missing printf-style files
 *              and/or datasets for determining the extent of the
 *              unlimited VDS, gap_size, using the access property list
 *              for the virtual dataset, dapl_id.  The default library
 *              value for gap_size is 0.
 *
 * Return:      Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_virtual_printf_gap(hid_t plist_id, hsize_t *gap_size /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", plist_id, gap_size);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get value from property list */
    if (gap_size)
        if (H5P_get(plist, H5D_ACS_VDS_PRINTF_GAP_NAME, gap_size) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "unable to get value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_virtual_printf_gap() */

/*-------------------------------------------------------------------------
 * Function: H5Pset_append_flush
 *
 * Purpose:  Sets the boundary, callback function, and user data in the
 *           property list.
 *           "ndims": number of array elements for boundary
 *           "boundary": used to determine whether the current dimension hits
 *              a boundary; if so, invoke the callback function and
 *              flush the dataset.
 *           "func": the callback function to invoke when the boundary is hit
 *           "udata": the user data to pass as parameter with the callback function
 *
 * Return:   Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_append_flush(hid_t plist_id, unsigned ndims, const hsize_t *boundary, H5D_append_cb_t func,
                    void *udata)
{
    H5P_genplist_t    *plist;               /* Property list pointer */
    H5D_append_flush_t info;                /* Property for append flush parameters */
    unsigned           u;                   /* Local index variable */
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "iIu*hDA*x", plist_id, ndims, boundary, func, udata);

    /* Check arguments */
    if (0 == ndims)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "dimensionality cannot be zero");
    if (ndims > H5S_MAX_RANK)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "dimensionality is too large");
    if (!boundary)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no boundary dimensions specified");

    /* Check if the callback function is NULL and the user data is non-NULL.
     * This is almost certainly an error as the user data will not be used. */
    if (!func && udata)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "callback is NULL while user data is not");

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set up values */
    info.ndims = ndims;
    info.func  = func;
    info.udata = udata;

    memset(info.boundary, 0, sizeof(info.boundary));
    /* boundary can be 0 to indicate no boundary is set */
    for (u = 0; u < ndims; u++) {
        if (boundary[u] != (boundary[u] & 0xffffffff)) /* negative value (including H5S_UNLIMITED) */
            HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "all boundary dimensions must be less than 2^32");
        info.boundary[u] = boundary[u]; /* Store user's boundary dimensions */
    }                                   /* end for */

    /* Set values */
    if (H5P_set(plist, H5D_ACS_APPEND_FLUSH_NAME, &info) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set append flush");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Pset_append_flush() */

/*-------------------------------------------------------------------------
 * Function: H5Pget_append_flush()
 *
 * Purpose:  Retrieves the boundary, callback function and user data set in
 *           property list.
 *           Note that the # of boundary sizes to retrieve will not exceed
 *           the parameter "ndims" and the ndims set previously via
 *           H5Pset_append_flush().
 *
 * Return:   Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_append_flush(hid_t plist_id, unsigned ndims, hsize_t boundary[], H5D_append_cb_t *func /*out*/,
                    void **udata /*out*/)
{
    H5P_genplist_t    *plist; /* property list pointer */
    H5D_append_flush_t info;
    unsigned           u;                   /* local index variable */
    herr_t             ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "iIu*hxx", plist_id, ndims, boundary, func, udata);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Retrieve info for append flush */
    if (H5P_get(plist, H5D_ACS_APPEND_FLUSH_NAME, &info) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get object flush callback");

    /* Assign return values */
    if (boundary) {
        memset(boundary, 0, ndims * sizeof(hsize_t));
        if (info.ndims > 0)
            for (u = 0; u < info.ndims && u < ndims; u++)
                boundary[u] = info.boundary[u];
    } /* end if */
    if (func)
        *func = info.func;
    if (udata)
        *udata = info.udata;

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Pget_append_flush() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_efile_prefix
 *
 * Purpose:     Set a prefix to be used for any external files.
 *
 *              If the prefix starts with ${ORIGIN}, this will be replaced by
 *              the absolute path of the directory of the HDF5 file containing
 *              the dataset.
 *
 *              If the prefix is ".", no prefix will be applied.
 *
 *              This property can be overwritten by the environment variable
 *              HDF5_EXTFILE_PREFIX.
 *
 * Return:      Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_efile_prefix(hid_t plist_id, const char *prefix)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "i*s", plist_id, prefix);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set prefix */
    if (H5P_set(plist, H5D_ACS_EFILE_PREFIX_NAME, &prefix) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set prefix info");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_efile_prefix() */

/*-------------------------------------------------------------------------
 * Function: H5Pget_efile_prefix
 *
 * Purpose:  Gets the prefix to be used for any external files.
 *           If the pointer is not NULL, it points to a user-allocated
 *           buffer.
 *
 * Return:   Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
ssize_t
H5Pget_efile_prefix(hid_t plist_id, char *prefix /*out*/, size_t size)
{
    H5P_genplist_t *plist;     /* Property list pointer */
    char           *my_prefix; /* Library's copy of the prefix */
    size_t          len;       /* Length of prefix string */
    ssize_t         ret_value; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("Zs", "ixz", plist_id, prefix, size);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get the current prefix */
    if (H5P_peek(plist, H5D_ACS_EFILE_PREFIX_NAME, &my_prefix) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get external file prefix");

    /* Check for prefix being set */
    if (my_prefix) {
        /* Copy to user's buffer, if given */
        len = strlen(my_prefix);
        if (prefix) {
            strncpy(prefix, my_prefix, size);
            if (len >= size)
                prefix[size - 1] = '\0';
        } /* end if */
    }     /* end if */
    else
        len = 0;

    /* Set return value */
    ret_value = (ssize_t)len;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_efile_prefix() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_virtual_prefix
 *
 * Purpose:     Set a prefix to be applied to the path of any vds files
 *              traversed.
 *
 *              If the prefix starts with ${ORIGIN}, this will be replaced by
 *              the absolute path of the directory of the HDF5 file containing
 *              the dataset.
 *
 *              If the prefix is ".", no prefix will be applied.
 *
 *              This property can be overwritten by the environment variable
 *              HDF5_VDS_PREFIX.
 *
 * Return:    Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_virtual_prefix(hid_t plist_id, const char *prefix)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "i*s", plist_id, prefix);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set prefix */
    if (H5P_set(plist, H5D_ACS_VDS_PREFIX_NAME, &prefix) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set prefix info");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_virtual_prefix() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_virtual_prefix
 *
 * Purpose:    Gets the prefix to be applied to any vds file
 *              traversals made using this property list.
 *
 *              If the pointer is not NULL, it points to a user-allocated
 *              buffer.
 *
 * Return:    Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
ssize_t
H5Pget_virtual_prefix(hid_t plist_id, char *prefix /*out*/, size_t size)
{
    H5P_genplist_t *plist;     /* Property list pointer */
    char           *my_prefix; /* Library's copy of the prefix */
    size_t          len;       /* Length of prefix string */
    ssize_t         ret_value; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("Zs", "ixz", plist_id, prefix, size);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get the current prefix */
    if (H5P_peek(plist, H5D_ACS_VDS_PREFIX_NAME, &my_prefix) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get vds file prefix");

    /* Check for prefix being set */
    if (my_prefix) {
        /* Copy to user's buffer, if given */
        len = strlen(my_prefix);
        if (prefix) {
            strncpy(prefix, my_prefix, size);
            if (len >= size)
                prefix[size - 1] = '\0';
        } /* end if */
    }     /* end if */
    else
        len = 0;

    /* Set return value */
    ret_value = (ssize_t)len;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_virtual_prefix() */
