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
 * Created:     H5Pdcpl.c
 *
 * Purpose:     Dataset creation property list class routines
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Pmodule.h" /* This source code file is part of the H5P module */
#define H5D_FRIEND     /* Suppress error about including H5Dpkg	       */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5CXprivate.h" /* API Contexts                             */
#include "H5Dpkg.h"      /* Datasets                                 */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5FLprivate.h" /* Free Lists                               */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5MMprivate.h" /* Memory management                        */
#include "H5Oprivate.h"  /* Object headers                           */
#include "H5Ppkg.h"      /* Property lists                           */
#include "H5Sprivate.h"  /* Dataspaces                               */
#include "H5Tprivate.h"  /* Datatypes                                */
#include "H5VMprivate.h" /* Vectors and arrays                       */
#include "H5Zprivate.h"  /* Data filters                             */

/****************/
/* Local Macros */
/****************/

/* Define default layout information */
#define H5D_DEF_STORAGE_COMPACT_INIT                                                                         \
    {                                                                                                        \
        false, (size_t)0, NULL                                                                               \
    }
#define H5D_DEF_STORAGE_CONTIG_INIT                                                                          \
    {                                                                                                        \
        HADDR_UNDEF, (hsize_t)0                                                                              \
    }
#define H5D_DEF_STORAGE_CHUNK_INIT                                                                           \
    {                                                                                                        \
        H5D_CHUNK_IDX_BTREE, HADDR_UNDEF, H5D_COPS_BTREE,                                                    \
        {                                                                                                    \
            {                                                                                                \
                HADDR_UNDEF, NULL                                                                            \
            }                                                                                                \
        }                                                                                                    \
    }
#define H5D_DEF_LAYOUT_CHUNK_INIT                                                                            \
    {                                                                                                        \
        H5D_CHUNK_IDX_BTREE, (uint8_t)0, (unsigned)0, {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,    \
                                                       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},      \
            (unsigned)0, (uint32_t)0, (hsize_t)0, (hsize_t)0,                                                \
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,                                              \
             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},                                                \
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,                                              \
             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},                                                \
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,                                              \
             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},                                                \
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,                                              \
             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},                                                \
        {                                                                                                    \
            {                                                                                                \
                {                                                                                            \
                    (uint8_t)0                                                                               \
                }                                                                                            \
            }                                                                                                \
        }                                                                                                    \
    }
#define H5D_DEF_STORAGE_VIRTUAL_INIT                                                                         \
    {                                                                                                        \
        {HADDR_UNDEF, 0}, 0, NULL, 0, {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,                       \
                                       0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},                      \
            H5D_VDS_ERROR, HSIZE_UNDEF, -1, -1, false                                                        \
    }
#define H5D_DEF_STORAGE_COMPACT                                                                              \
    {                                                                                                        \
        H5D_COMPACT,                                                                                         \
        {                                                                                                    \
            .compact = H5D_DEF_STORAGE_COMPACT_INIT                                                          \
        }                                                                                                    \
    }
#define H5D_DEF_STORAGE_CONTIG                                                                               \
    {                                                                                                        \
        H5D_CONTIGUOUS,                                                                                      \
        {                                                                                                    \
            .contig = H5D_DEF_STORAGE_CONTIG_INIT                                                            \
        }                                                                                                    \
    }
#define H5D_DEF_STORAGE_CHUNK                                                                                \
    {                                                                                                        \
        H5D_CHUNKED,                                                                                         \
        {                                                                                                    \
            .chunk = H5D_DEF_STORAGE_CHUNK_INIT                                                              \
        }                                                                                                    \
    }
#define H5D_DEF_STORAGE_VIRTUAL                                                                              \
    {                                                                                                        \
        H5D_VIRTUAL,                                                                                         \
        {                                                                                                    \
            .virt = H5D_DEF_STORAGE_VIRTUAL_INIT                                                             \
        }                                                                                                    \
    }
#define H5D_DEF_LAYOUT_COMPACT                                                                               \
    {                                                                                                        \
        H5D_COMPACT, H5O_LAYOUT_VERSION_DEFAULT, H5D_LOPS_COMPACT, {H5D_DEF_LAYOUT_CHUNK_INIT},              \
            H5D_DEF_STORAGE_COMPACT                                                                          \
    }
#define H5D_DEF_LAYOUT_CONTIG                                                                                \
    {                                                                                                        \
        H5D_CONTIGUOUS, H5O_LAYOUT_VERSION_DEFAULT, H5D_LOPS_CONTIG, {H5D_DEF_LAYOUT_CHUNK_INIT},            \
            H5D_DEF_STORAGE_CONTIG                                                                           \
    }
#define H5D_DEF_LAYOUT_CHUNK                                                                                 \
    {                                                                                                        \
        H5D_CHUNKED, H5O_LAYOUT_VERSION_DEFAULT, H5D_LOPS_CHUNK, {H5D_DEF_LAYOUT_CHUNK_INIT},                \
            H5D_DEF_STORAGE_CHUNK                                                                            \
    }
#define H5D_DEF_LAYOUT_VIRTUAL                                                                               \
    {                                                                                                        \
        H5D_VIRTUAL, H5O_LAYOUT_VERSION_4, H5D_LOPS_VIRTUAL, {H5D_DEF_LAYOUT_CHUNK_INIT},                    \
            H5D_DEF_STORAGE_VIRTUAL                                                                          \
    }

/* ========  Dataset creation properties ======== */
/* Definitions for storage layout property */
#define H5D_CRT_LAYOUT_SIZE  sizeof(H5O_layout_t)
#define H5D_CRT_LAYOUT_DEF   H5D_DEF_LAYOUT_CONTIG
#define H5D_CRT_LAYOUT_SET   H5P__dcrt_layout_set
#define H5D_CRT_LAYOUT_GET   H5P__dcrt_layout_get
#define H5D_CRT_LAYOUT_ENC   H5P__dcrt_layout_enc
#define H5D_CRT_LAYOUT_DEC   H5P__dcrt_layout_dec
#define H5D_CRT_LAYOUT_DEL   H5P__dcrt_layout_del
#define H5D_CRT_LAYOUT_COPY  H5P__dcrt_layout_copy
#define H5D_CRT_LAYOUT_CMP   H5P__dcrt_layout_cmp
#define H5D_CRT_LAYOUT_CLOSE H5P__dcrt_layout_close
/* Definitions for fill value.  size=0 means fill value will be 0 as
 * library default; size=-1 means fill value is undefined. */
#define H5D_CRT_FILL_VALUE_SIZE sizeof(H5O_fill_t)
#define H5D_CRT_FILL_VALUE_DEF                                                                               \
    {                                                                                                        \
        {0, NULL, H5O_NULL_ID, {{0, HADDR_UNDEF}}}, H5O_FILL_VERSION_2, NULL, 0, NULL, H5D_ALLOC_TIME_LATE,  \
            H5D_FILL_TIME_IFSET, false                                                                       \
    }
#define H5D_CRT_FILL_VALUE_SET   H5P__dcrt_fill_value_set
#define H5D_CRT_FILL_VALUE_GET   H5P__dcrt_fill_value_get
#define H5D_CRT_FILL_VALUE_ENC   H5P__dcrt_fill_value_enc
#define H5D_CRT_FILL_VALUE_DEC   H5P__dcrt_fill_value_dec
#define H5D_CRT_FILL_VALUE_DEL   H5P__dcrt_fill_value_del
#define H5D_CRT_FILL_VALUE_COPY  H5P__dcrt_fill_value_copy
#define H5D_CRT_FILL_VALUE_CMP   H5P_fill_value_cmp
#define H5D_CRT_FILL_VALUE_CLOSE H5P__dcrt_fill_value_close
/* Definitions for space allocation time state */
#define H5D_CRT_ALLOC_TIME_STATE_SIZE sizeof(unsigned)
#define H5D_CRT_ALLOC_TIME_STATE_DEF  1
#define H5D_CRT_ALLOC_TIME_STATE_ENC  H5P__encode_unsigned
#define H5D_CRT_ALLOC_TIME_STATE_DEC  H5P__decode_unsigned
/* Definitions for external file list */
#define H5D_CRT_EXT_FILE_LIST_SIZE sizeof(H5O_efl_t)
#define H5D_CRT_EXT_FILE_LIST_DEF                                                                            \
    {                                                                                                        \
        HADDR_UNDEF, 0, 0, NULL                                                                              \
    }
#define H5D_CRT_EXT_FILE_LIST_SET   H5P__dcrt_ext_file_list_set
#define H5D_CRT_EXT_FILE_LIST_GET   H5P__dcrt_ext_file_list_get
#define H5D_CRT_EXT_FILE_LIST_ENC   H5P__dcrt_ext_file_list_enc
#define H5D_CRT_EXT_FILE_LIST_DEC   H5P__dcrt_ext_file_list_dec
#define H5D_CRT_EXT_FILE_LIST_DEL   H5P__dcrt_ext_file_list_del
#define H5D_CRT_EXT_FILE_LIST_COPY  H5P__dcrt_ext_file_list_copy
#define H5D_CRT_EXT_FILE_LIST_CMP   H5P__dcrt_ext_file_list_cmp
#define H5D_CRT_EXT_FILE_LIST_CLOSE H5P__dcrt_ext_file_list_close
/* Definitions for dataset object header minimization */
#define H5D_CRT_MIN_DSET_HDR_SIZE_SIZE sizeof(bool)
#define H5D_CRT_MIN_DSET_HDR_SIZE_DEF  false
#define H5D_CRT_MIN_DSET_HDR_SIZE_ENC  H5P__encode_bool
#define H5D_CRT_MIN_DSET_HDR_SIZE_DEC  H5P__decode_bool

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

/* General routines */
static herr_t H5P__set_layout(H5P_genplist_t *plist, const H5O_layout_t *layout);

/* Property class callbacks */
static herr_t H5P__dcrt_reg_prop(H5P_genclass_t *pclass);

/* Property callbacks */
static herr_t H5P__dcrt_layout_set(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__dcrt_layout_get(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__dcrt_layout_enc(const void *value, void **pp, size_t *size);
static herr_t H5P__dcrt_layout_dec(const void **pp, void *value);
static herr_t H5P__dcrt_layout_del(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__dcrt_layout_copy(const char *name, size_t size, void *value);
static int    H5P__dcrt_layout_cmp(const void *value1, const void *value2, size_t size);
static herr_t H5P__dcrt_layout_close(const char *name, size_t size, void *value);
static herr_t H5P__dcrt_fill_value_set(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__dcrt_fill_value_get(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__dcrt_fill_value_enc(const void *value, void **pp, size_t *size);
static herr_t H5P__dcrt_fill_value_dec(const void **pp, void *value);
static herr_t H5P__dcrt_fill_value_del(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__dcrt_fill_value_copy(const char *name, size_t size, void *value);
static herr_t H5P__dcrt_fill_value_close(const char *name, size_t size, void *value);
static herr_t H5P__dcrt_ext_file_list_set(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__dcrt_ext_file_list_get(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__dcrt_ext_file_list_enc(const void *value, void **pp, size_t *size);
static herr_t H5P__dcrt_ext_file_list_dec(const void **pp, void *value);
static herr_t H5P__dcrt_ext_file_list_del(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__dcrt_ext_file_list_copy(const char *name, size_t size, void *value);
static int    H5P__dcrt_ext_file_list_cmp(const void *value1, const void *value2, size_t size);
static herr_t H5P__dcrt_ext_file_list_close(const char *name, size_t size, void *value);

/*********************/
/* Package Variables */
/*********************/

/* Dataset creation property list class library initialization object */
const H5P_libclass_t H5P_CLS_DCRT[1] = {{
    "dataset create",        /* Class name for debugging     */
    H5P_TYPE_DATASET_CREATE, /* Class type                   */

    &H5P_CLS_OBJECT_CREATE_g,     /* Parent class                 */
    &H5P_CLS_DATASET_CREATE_g,    /* Pointer to class             */
    &H5P_CLS_DATASET_CREATE_ID_g, /* Pointer to class ID          */
    &H5P_LST_DATASET_CREATE_ID_g, /* Pointer to default property list ID */
    H5P__dcrt_reg_prop,           /* Default property registration routine */

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

/* Declare extern the free list to manage blocks of type conversion data */
H5FL_BLK_EXTERN(type_conv);

/***************************/
/* Local Private Variables */
/***************************/

/* Property value defaults */
static const H5O_layout_t H5D_def_layout_g = H5D_CRT_LAYOUT_DEF;     /* Default storage layout */
static const H5O_fill_t   H5D_def_fill_g   = H5D_CRT_FILL_VALUE_DEF; /* Default fill value */
static const unsigned     H5D_def_alloc_time_state_g =
    H5D_CRT_ALLOC_TIME_STATE_DEF;                                     /* Default allocation time state */
static const H5O_efl_t H5D_def_efl_g = H5D_CRT_EXT_FILE_LIST_DEF;     /* Default external file list */
static const unsigned H5O_ohdr_min_g = H5D_CRT_MIN_DSET_HDR_SIZE_DEF; /* Default object header minimization */

/* Defaults for each type of layout */
static const H5O_layout_t H5D_def_layout_compact_g = H5D_DEF_LAYOUT_COMPACT;
static const H5O_layout_t H5D_def_layout_contig_g  = H5D_DEF_LAYOUT_CONTIG;
static const H5O_layout_t H5D_def_layout_chunk_g   = H5D_DEF_LAYOUT_CHUNK;
static const H5O_layout_t H5D_def_layout_virtual_g = H5D_DEF_LAYOUT_VIRTUAL;

/*-------------------------------------------------------------------------
 * Function:    H5P__dcrt_reg_prop
 *
 * Purpose:     Register the dataset creation property list class's properties
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dcrt_reg_prop(H5P_genclass_t *pclass)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Register the storage layout property */
    if (H5P__register_real(pclass, H5D_CRT_LAYOUT_NAME, H5D_CRT_LAYOUT_SIZE, &H5D_def_layout_g, NULL,
                           H5D_CRT_LAYOUT_SET, H5D_CRT_LAYOUT_GET, H5D_CRT_LAYOUT_ENC, H5D_CRT_LAYOUT_DEC,
                           H5D_CRT_LAYOUT_DEL, H5D_CRT_LAYOUT_COPY, H5D_CRT_LAYOUT_CMP,
                           H5D_CRT_LAYOUT_CLOSE) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the fill value property */
    if (H5P__register_real(pclass, H5D_CRT_FILL_VALUE_NAME, H5D_CRT_FILL_VALUE_SIZE, &H5D_def_fill_g, NULL,
                           H5D_CRT_FILL_VALUE_SET, H5D_CRT_FILL_VALUE_GET, H5D_CRT_FILL_VALUE_ENC,
                           H5D_CRT_FILL_VALUE_DEC, H5D_CRT_FILL_VALUE_DEL, H5D_CRT_FILL_VALUE_COPY,
                           H5D_CRT_FILL_VALUE_CMP, H5D_CRT_FILL_VALUE_CLOSE) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the space allocation time state property */
    if (H5P__register_real(pclass, H5D_CRT_ALLOC_TIME_STATE_NAME, H5D_CRT_ALLOC_TIME_STATE_SIZE,
                           &H5D_def_alloc_time_state_g, NULL, NULL, NULL, H5D_CRT_ALLOC_TIME_STATE_ENC,
                           H5D_CRT_ALLOC_TIME_STATE_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the external file list property */
    if (H5P__register_real(pclass, H5D_CRT_EXT_FILE_LIST_NAME, H5D_CRT_EXT_FILE_LIST_SIZE, &H5D_def_efl_g,
                           NULL, H5D_CRT_EXT_FILE_LIST_SET, H5D_CRT_EXT_FILE_LIST_GET,
                           H5D_CRT_EXT_FILE_LIST_ENC, H5D_CRT_EXT_FILE_LIST_DEC, H5D_CRT_EXT_FILE_LIST_DEL,
                           H5D_CRT_EXT_FILE_LIST_COPY, H5D_CRT_EXT_FILE_LIST_CMP,
                           H5D_CRT_EXT_FILE_LIST_CLOSE) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the object header minimization property */
    if (H5P__register_real(pclass, H5D_CRT_MIN_DSET_HDR_SIZE_NAME, H5D_CRT_MIN_DSET_HDR_SIZE_SIZE,
                           &H5O_ohdr_min_g, NULL, NULL, NULL, H5D_CRT_MIN_DSET_HDR_SIZE_ENC,
                           H5D_CRT_MIN_DSET_HDR_SIZE_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dcrt_reg_prop() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dcrt_layout_set
 *
 * Purpose:     Copies a layout property when it's set for a property list
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dcrt_layout_set(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                     size_t H5_ATTR_UNUSED size, void *value)
{
    H5O_layout_t *layout = (H5O_layout_t *)value; /* Create local aliases for values */
    H5O_layout_t  new_layout;
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(value);

    /* Make copy of layout */
    if (NULL == H5O_msg_copy(H5O_LAYOUT_ID, layout, &new_layout))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't copy layout");

    /* Copy new layout message over old one */
    *layout = new_layout;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dcrt_layout_set() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dcrt_layout_get
 *
 * Purpose:     Copies a layout property when it's retrieved from a property list
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dcrt_layout_get(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                     size_t H5_ATTR_UNUSED size, void *value)
{
    H5O_layout_t *layout = (H5O_layout_t *)value; /* Create local aliases for values */
    H5O_layout_t  new_layout;
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(value);

    /* Make copy of layout */
    if (NULL == H5O_msg_copy(H5O_LAYOUT_ID, layout, &new_layout))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't copy layout");

    /* Copy new layout message over old one */
    *layout = new_layout;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dcrt_layout_get() */

/*-------------------------------------------------------------------------
 * Function:       H5P__dcrt_layout_enc
 *
 * Purpose:        Callback routine which is called whenever the layout
 *                 property in the dataset creation property list is
 *                 encoded.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dcrt_layout_enc(const void *value, void **_pp, size_t *size)
{
    const H5O_layout_t *layout = (const H5O_layout_t *)value; /* Create local aliases for values */
    uint8_t           **pp     = (uint8_t **)_pp;
    uint8_t            *tmp_p;
    size_t              tmp_size;
    size_t              u;                   /* Local index variable */
    herr_t              ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(layout);
    assert(size);

    if (NULL != *pp) {
        /* Encode layout type */
        *(*pp)++ = (uint8_t)layout->type;
        *size += sizeof(uint8_t);

        /* If layout is chunked, encode chunking structure */
        if (H5D_CHUNKED == layout->type) {
            /* Encode rank */
            *(*pp)++ = (uint8_t)layout->u.chunk.ndims;
            *size += sizeof(uint8_t);

            /* Encode chunk dims */
            HDcompile_assert(sizeof(uint32_t) == sizeof(layout->u.chunk.dim[0]));
            for (u = 0; u < (size_t)layout->u.chunk.ndims; u++) {
                UINT32ENCODE(*pp, layout->u.chunk.dim[u]);
                *size += sizeof(uint32_t);
            } /* end for */
        }     /* end if */
        else if (H5D_VIRTUAL == layout->type) {
            uint64_t nentries = (uint64_t)layout->storage.u.virt.list_nused;

            /* Encode number of entries */
            UINT64ENCODE(*pp, nentries);
            *size += (size_t)8;

            /* Iterate over entries */
            for (u = 0; u < layout->storage.u.virt.list_nused; u++) {
                /* Source file name */
                tmp_size = strlen(layout->storage.u.virt.list[u].source_file_name) + (size_t)1;
                H5MM_memcpy(*pp, layout->storage.u.virt.list[u].source_file_name, tmp_size);
                *pp += tmp_size;
                *size += tmp_size;

                /* Source dataset name */
                tmp_size = strlen(layout->storage.u.virt.list[u].source_dset_name) + (size_t)1;
                H5MM_memcpy(*pp, layout->storage.u.virt.list[u].source_dset_name, tmp_size);
                *pp += tmp_size;
                *size += tmp_size;

                /* Source selection.  Note that we are not passing the real
                 * allocated size because we do not know it.  H5P__encode should
                 * have verified that the buffer is large enough for the entire
                 * list before we get here. */
                tmp_size = (size_t)-1;
                tmp_p    = *pp;
                if (H5S_encode(layout->storage.u.virt.list[u].source_select, pp, &tmp_size) < 0)
                    HGOTO_ERROR(H5E_PLIST, H5E_CANTENCODE, FAIL, "unable to serialize source selection");
                *size += (size_t)(*pp - tmp_p);

                /* Virtual dataset selection.  Same notes as above apply. */
                tmp_size = (size_t)-1;
                tmp_p    = *pp;
                if (H5S_encode(layout->storage.u.virt.list[u].source_dset.virtual_select, pp, &tmp_size) < 0)
                    HGOTO_ERROR(H5E_PLIST, H5E_CANTENCODE, FAIL, "unable to serialize virtual selection");
                *size += (size_t)(*pp - tmp_p);
            } /* end for */
        }     /* end if */
    }         /* end if */
    else {
        /* Size of layout type */
        *size += sizeof(uint8_t);

        /* If layout is chunked, calculate chunking structure */
        if (H5D_CHUNKED == layout->type) {
            *size += sizeof(uint8_t);
            *size += layout->u.chunk.ndims * sizeof(uint32_t);
        } /* end if */
        else if (H5D_VIRTUAL == layout->type) {
            /* Calculate size of virtual layout info */
            /* number of entries */
            *size += (size_t)8;

            /* Iterate over entries */
            for (u = 0; u < layout->storage.u.virt.list_nused; u++) {
                /* Source file name */
                tmp_size = strlen(layout->storage.u.virt.list[u].source_file_name) + (size_t)1;
                *size += tmp_size;

                /* Source dataset name */
                tmp_size = strlen(layout->storage.u.virt.list[u].source_dset_name) + (size_t)1;
                *size += tmp_size;

                /* Source selection */
                tmp_size = (size_t)0;
                tmp_p    = NULL;
                if (H5S_encode(layout->storage.u.virt.list[u].source_select, &tmp_p, &tmp_size) < 0)
                    HGOTO_ERROR(H5E_PLIST, H5E_CANTENCODE, FAIL, "unable to serialize source selection");
                *size += tmp_size;

                /* Virtual dataset selection */
                tmp_size = (size_t)0;
                tmp_p    = NULL;
                if (H5S_encode(layout->storage.u.virt.list[u].source_dset.virtual_select, &tmp_p, &tmp_size) <
                    0)
                    HGOTO_ERROR(H5E_PLIST, H5E_CANTENCODE, FAIL, "unable to serialize virtual selection");
                *size += tmp_size;
            } /* end for */
        }     /* end if */
    }         /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dcrt_layout_enc() */

/*-------------------------------------------------------------------------
 * Function:       H5P__dcrt_layout_dec
 *
 * Purpose:        Callback routine which is called whenever the layout
 *                 property in the dataset creation property list is
 *                 decoded.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dcrt_layout_dec(const void **_pp, void *value)
{
    const H5O_layout_t *layout;     /* Storage layout */
    H5O_layout_t        tmp_layout; /* Temporary local layout structure */
    H5D_layout_t        type;       /* Layout type */
    const uint8_t     **pp        = (const uint8_t **)_pp;
    herr_t              ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(pp);
    assert(*pp);
    assert(value);

    /* Decode layout type */
    type = (H5D_layout_t) * (*pp)++;

    /* set default layout in case the type is compact or contiguous, otherwise
     * decode the chunked structure and set chunked layout */
    switch (type) {
        case H5D_COMPACT:
            layout = &H5D_def_layout_compact_g;
            break;

        case H5D_CONTIGUOUS:
            layout = &H5D_def_layout_contig_g;
            break;

        case H5D_CHUNKED: {
            unsigned ndims; /* Number of chunk dimensions */

            /* Decode the number of chunk dimensions */
            ndims = *(*pp)++;

            /* default chunk layout */
            if (0 == ndims)
                layout = &H5D_def_layout_chunk_g;
            else {          /* chunk layout structure is encoded*/
                unsigned u; /* Local index variable */

                /* Initialize to default values */
                tmp_layout = H5D_def_layout_chunk_g;

                /* Set rank & dimensions */
                tmp_layout.u.chunk.ndims = (unsigned)ndims;
                for (u = 0; u < ndims; u++)
                    UINT32DECODE(*pp, tmp_layout.u.chunk.dim[u]);

                /* Point at the newly set up struct */
                layout = &tmp_layout;
            } /* end else */
        } break;

        case H5D_VIRTUAL: {
            uint64_t nentries; /* Number of VDS mappings */

            /* Decode number of entries */
            UINT64DECODE(*pp, nentries);

            if (nentries == (uint64_t)0)
                /* Just use the default struct */
                layout = &H5D_def_layout_virtual_g;
            else {
                size_t tmp_size;
                size_t u; /* Local index variable */

                /* Initialize to default values */
                tmp_layout = H5D_def_layout_virtual_g;

                /* Allocate entry list */
                if (NULL == (tmp_layout.storage.u.virt.list = (H5O_storage_virtual_ent_t *)H5MM_calloc(
                                 (size_t)nentries * sizeof(H5O_storage_virtual_ent_t))))
                    HGOTO_ERROR(H5E_PLIST, H5E_CANTALLOC, FAIL, "unable to allocate heap block");
                tmp_layout.storage.u.virt.list_nalloc = (size_t)nentries;
                tmp_layout.storage.u.virt.list_nused  = (size_t)nentries;

                /* Decode each entry */
                for (u = 0; u < (size_t)nentries; u++) {
                    /* Source file name */
                    tmp_size = strlen((const char *)*pp) + 1;
                    if (NULL ==
                        (tmp_layout.storage.u.virt.list[u].source_file_name = (char *)H5MM_malloc(tmp_size)))
                        HGOTO_ERROR(H5E_PLIST, H5E_CANTALLOC, FAIL,
                                    "unable to allocate memory for source file name");
                    H5MM_memcpy(tmp_layout.storage.u.virt.list[u].source_file_name, *pp, tmp_size);
                    *pp += tmp_size;

                    /* Source dataset name */
                    tmp_size = strlen((const char *)*pp) + 1;
                    if (NULL ==
                        (tmp_layout.storage.u.virt.list[u].source_dset_name = (char *)H5MM_malloc(tmp_size)))
                        HGOTO_ERROR(H5E_PLIST, H5E_CANTALLOC, FAIL,
                                    "unable to allocate memory for source dataset name");
                    H5MM_memcpy(tmp_layout.storage.u.virt.list[u].source_dset_name, *pp, tmp_size);
                    *pp += tmp_size;

                    /* Source selection */
                    if (NULL == (tmp_layout.storage.u.virt.list[u].source_select = H5S_decode(pp)))
                        HGOTO_ERROR(H5E_PLIST, H5E_CANTDECODE, FAIL, "can't decode source space selection");
                    tmp_layout.storage.u.virt.list[u].source_space_status = H5O_VIRTUAL_STATUS_USER;

                    /* Virtual selection */
                    if (NULL ==
                        (tmp_layout.storage.u.virt.list[u].source_dset.virtual_select = H5S_decode(pp)))
                        HGOTO_ERROR(H5E_PLIST, H5E_CANTDECODE, FAIL, "can't decode virtual space selection");
                    tmp_layout.storage.u.virt.list[u].virtual_space_status = H5O_VIRTUAL_STATUS_USER;

                    /* Parse source file and dataset names for "printf"
                     * style format specifiers */
                    if (H5D_virtual_parse_source_name(
                            tmp_layout.storage.u.virt.list[u].source_file_name,
                            &tmp_layout.storage.u.virt.list[u].parsed_source_file_name,
                            &tmp_layout.storage.u.virt.list[u].psfn_static_strlen,
                            &tmp_layout.storage.u.virt.list[u].psfn_nsubs) < 0)
                        HGOTO_ERROR(H5E_PLIST, H5E_CANTINIT, FAIL, "can't parse source file name");
                    if (H5D_virtual_parse_source_name(
                            tmp_layout.storage.u.virt.list[u].source_dset_name,
                            &tmp_layout.storage.u.virt.list[u].parsed_source_dset_name,
                            &tmp_layout.storage.u.virt.list[u].psdn_static_strlen,
                            &tmp_layout.storage.u.virt.list[u].psdn_nsubs) < 0)
                        HGOTO_ERROR(H5E_PLIST, H5E_CANTINIT, FAIL, "can't parse source dataset name");

                    /* Set source names in source_dset struct */
                    if ((tmp_layout.storage.u.virt.list[u].psfn_nsubs == 0) &&
                        (tmp_layout.storage.u.virt.list[u].psdn_nsubs == 0)) {
                        if (tmp_layout.storage.u.virt.list[u].parsed_source_file_name)
                            tmp_layout.storage.u.virt.list[u].source_dset.file_name =
                                tmp_layout.storage.u.virt.list[u].parsed_source_file_name->name_segment;
                        else
                            tmp_layout.storage.u.virt.list[u].source_dset.file_name =
                                tmp_layout.storage.u.virt.list[u].source_file_name;
                        if (tmp_layout.storage.u.virt.list[u].parsed_source_dset_name)
                            tmp_layout.storage.u.virt.list[u].source_dset.dset_name =
                                tmp_layout.storage.u.virt.list[u].parsed_source_dset_name->name_segment;
                        else
                            tmp_layout.storage.u.virt.list[u].source_dset.dset_name =
                                tmp_layout.storage.u.virt.list[u].source_dset_name;
                    } /* end if */

                    /* unlim_dim fields */
                    tmp_layout.storage.u.virt.list[u].unlim_dim_source =
                        H5S_get_select_unlim_dim(tmp_layout.storage.u.virt.list[u].source_select);
                    tmp_layout.storage.u.virt.list[u].unlim_dim_virtual = H5S_get_select_unlim_dim(
                        tmp_layout.storage.u.virt.list[u].source_dset.virtual_select);
                    tmp_layout.storage.u.virt.list[u].unlim_extent_source  = HSIZE_UNDEF;
                    tmp_layout.storage.u.virt.list[u].unlim_extent_virtual = HSIZE_UNDEF;
                    tmp_layout.storage.u.virt.list[u].clip_size_source     = HSIZE_UNDEF;
                    tmp_layout.storage.u.virt.list[u].clip_size_virtual    = HSIZE_UNDEF;

                    /* Clipped selections */
                    if (tmp_layout.storage.u.virt.list[u].unlim_dim_virtual < 0) {
                        tmp_layout.storage.u.virt.list[u].source_dset.clipped_source_select =
                            tmp_layout.storage.u.virt.list[u].source_select;
                        tmp_layout.storage.u.virt.list[u].source_dset.clipped_virtual_select =
                            tmp_layout.storage.u.virt.list[u].source_dset.virtual_select;
                    } /* end if */

                    /* Update min_dims */
                    if (H5D_virtual_update_min_dims(&tmp_layout, u) < 0)
                        HGOTO_ERROR(H5E_PLIST, H5E_CANTINIT, FAIL,
                                    "unable to update virtual dataset minimum dimensions");
                } /* end for */

                /* Point at the newly set up struct */
                layout = &tmp_layout;
            } /* end else */
        }     /* end block */
        break;

        case H5D_LAYOUT_ERROR:
        case H5D_NLAYOUTS:
        default:
            HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "bad layout type");
    } /* end switch */

    /* Set the value */
    H5MM_memcpy(value, layout, sizeof(H5O_layout_t));

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dcrt_layout_dec() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dcrt_layout_del
 *
 * Purpose:     Frees memory used to store the layout property
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dcrt_layout_del(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                     size_t H5_ATTR_UNUSED size, void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(value);

    /* Reset the old layout */
    if (H5O_msg_reset(H5O_LAYOUT_ID, value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTRESET, FAIL, "can't release layout message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dcrt_layout_del() */

/*--------------------------------------------------------------------------
 * Function:    H5P__dcrt_layout_copy
 *
 * Purpose:     Copy the layout property
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *--------------------------------------------------------------------------
 */
static herr_t
H5P__dcrt_layout_copy(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    H5O_layout_t *layout = (H5O_layout_t *)value; /* Create local aliases for values */
    H5O_layout_t  new_layout;
    herr_t        ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(layout);

    /* Make copy of layout */
    if (NULL == H5O_msg_copy(H5O_LAYOUT_ID, layout, &new_layout))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't copy layout");

    /* Set new layout message directly into property list */
    *layout = new_layout;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dcrt_layout_copy() */

/*-------------------------------------------------------------------------
 * Function:       H5P__dcrt_layout_cmp
 *
 * Purpose:        Callback routine which is called whenever the layout
 *                 property in the dataset creation property list is
 *                 compared.
 *
 * Return:         positive if VALUE1 is greater than VALUE2, negative if
 *                      VALUE2 is greater than VALUE1 and zero if VALUE1 and
 *                      VALUE2 are equal.
 *
 *-------------------------------------------------------------------------
 */
static int
H5P__dcrt_layout_cmp(const void *_layout1, const void *_layout2, size_t H5_ATTR_UNUSED size)
{
    const H5O_layout_t *layout1 = (const H5O_layout_t *)_layout1, /* Create local aliases for values */
        *layout2                = (const H5O_layout_t *)_layout2;
    herr_t ret_value            = 0; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(layout1);
    assert(layout2);
    assert(size == sizeof(H5O_layout_t));

    /* Check for different layout type */
    if (layout1->type < layout2->type)
        HGOTO_DONE(-1);
    if (layout1->type > layout2->type)
        HGOTO_DONE(1);

    /* Compare non-dataset-specific fields in layout info */
    switch (layout1->type) {
        case H5D_COMPACT:
        case H5D_CONTIGUOUS:
            break;

        case H5D_CHUNKED: {
            unsigned u; /* Local index variable */

            /* Check the number of dimensions */
            if (layout1->u.chunk.ndims < layout2->u.chunk.ndims)
                HGOTO_DONE(-1);
            if (layout1->u.chunk.ndims > layout2->u.chunk.ndims)
                HGOTO_DONE(1);

            /* Compare the chunk dims */
            for (u = 0; u < layout1->u.chunk.ndims - 1; u++) {
                if (layout1->u.chunk.dim[u] < layout2->u.chunk.dim[u])
                    HGOTO_DONE(-1);
                if (layout1->u.chunk.dim[u] > layout2->u.chunk.dim[u])
                    HGOTO_DONE(1);
            } /* end for */
        }     /* end case */
        break;

        case H5D_VIRTUAL: {
            htri_t equal;
            int    strcmp_ret;
            size_t u; /* Local index variable */

            /* Compare number of mappings */
            if (layout1->storage.u.virt.list_nused < layout2->storage.u.virt.list_nused)
                HGOTO_DONE(-1);
            if (layout1->storage.u.virt.list_nused > layout2->storage.u.virt.list_nused)
                HGOTO_DONE(1);

            /* Iterate over mappings */
            for (u = 0; u < layout1->storage.u.virt.list_nused; u++) {
                /* Compare virtual spaces.  Note we cannot tell which is
                 * "greater", so just return 1 if different, -1 on failure.
                 */
                if ((equal = H5S_extent_equal(layout1->storage.u.virt.list[u].source_dset.virtual_select,
                                              layout2->storage.u.virt.list[u].source_dset.virtual_select)) <
                    0)
                    HGOTO_DONE(-1);
                if (!equal)
                    HGOTO_DONE(1);
                if ((equal = H5S_SELECT_SHAPE_SAME(
                         layout1->storage.u.virt.list[u].source_dset.virtual_select,
                         layout2->storage.u.virt.list[u].source_dset.virtual_select)) < 0)
                    HGOTO_DONE(-1);
                if (!equal)
                    HGOTO_DONE(1);

                /* Compare source file names */
                strcmp_ret = strcmp(layout1->storage.u.virt.list[u].source_file_name,
                                    layout2->storage.u.virt.list[u].source_file_name);
                if (strcmp_ret < 0)
                    HGOTO_DONE(-1);
                if (strcmp_ret > 0)
                    HGOTO_DONE(1);

                /* Compare source dataset names */
                strcmp_ret = strcmp(layout1->storage.u.virt.list[u].source_dset_name,
                                    layout2->storage.u.virt.list[u].source_dset_name);
                if (strcmp_ret < 0)
                    HGOTO_DONE(-1);
                if (strcmp_ret > 0)
                    HGOTO_DONE(1);

                /* Compare source spaces.  Note we cannot tell which is
                 * "greater", so just return 1 if different, -1 on failure.
                 */
                if ((equal = H5S_extent_equal(layout1->storage.u.virt.list[u].source_select,
                                              layout2->storage.u.virt.list[u].source_select)) < 0)
                    HGOTO_DONE(-1);
                if (!equal)
                    HGOTO_DONE(1);
                if ((equal = H5S_SELECT_SHAPE_SAME(layout1->storage.u.virt.list[u].source_select,
                                                   layout2->storage.u.virt.list[u].source_select)) < 0)
                    HGOTO_DONE(-1);
                if (!equal)
                    HGOTO_DONE(1);
            } /* end for */
        }     /* end block */
        break;

        case H5D_LAYOUT_ERROR:
        case H5D_NLAYOUTS:
        default:
            assert(0 && "Unknown layout type!");
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dcrt_layout_cmp() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dcrt_layout_close
 *
 * Purpose:     Frees memory used to store the layout property
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dcrt_layout_close(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(value);

    /* Reset the old layout */
    if (H5O_msg_reset(H5O_LAYOUT_ID, value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTRESET, FAIL, "can't release layout message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dcrt_layout_close() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dcrt_fill_value_set
 *
 * Purpose:     Copies a fill value property when it's set for a property list
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dcrt_fill_value_set(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                         size_t H5_ATTR_UNUSED size, void *value)
{
    H5O_fill_t *fill = (H5O_fill_t *)value; /* Create local aliases for values */
    H5O_fill_t  new_fill;
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(value);

    /* Make copy of fill value */
    if (NULL == H5O_msg_copy(H5O_FILL_ID, fill, &new_fill))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't copy fill value");

    /* Copy new fill value message over old one */
    *fill = new_fill;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dcrt_fill_value_set() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dcrt_fill_value_get
 *
 * Purpose:     Copies a fill value property when it's retrieved from a property list
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dcrt_fill_value_get(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                         size_t H5_ATTR_UNUSED size, void *value)
{
    H5O_fill_t *fill = (H5O_fill_t *)value; /* Create local aliases for values */
    H5O_fill_t  new_fill;
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(value);

    /* Make copy of fill value */
    if (NULL == H5O_msg_copy(H5O_FILL_ID, fill, &new_fill))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't copy fill value");

    /* Copy new fill value message over old one */
    *fill = new_fill;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dcrt_fill_value_get() */

/*-------------------------------------------------------------------------
 * Function:       H5P__dcrt_fill_value_enc
 *
 * Purpose:        Callback routine which is called whenever the fill value
 *                 property in the dataset creation property list is
 *                 encoded.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dcrt_fill_value_enc(const void *value, void **_pp, size_t *size)
{
    const H5O_fill_t *fill      = (const H5O_fill_t *)value; /* Create local aliases for values */
    size_t            dt_size   = 0;                         /* Size of encoded datatype */
    herr_t            ret_value = SUCCEED;                   /* Return value */
    uint8_t         **pp        = (uint8_t **)_pp;
    uint64_t          enc_value;
    unsigned          enc_size = 0;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    HDcompile_assert(sizeof(size_t) <= sizeof(uint64_t));
    HDcompile_assert(sizeof(ssize_t) <= sizeof(int64_t));
    assert(fill);
    assert(size);

    if (NULL != *pp) {
        /* Encode alloc and fill time */
        *(*pp)++ = (uint8_t)fill->alloc_time;
        *(*pp)++ = (uint8_t)fill->fill_time;

        /* Encode size of fill value */
        INT64ENCODE(*pp, fill->size);

        /* Encode the fill value & datatype */
        if (fill->size > 0) {
            /* Encode the fill value itself */
            H5MM_memcpy(*pp, (uint8_t *)fill->buf, (size_t)fill->size);
            *pp += fill->size;

            /* Encode fill value datatype */
            assert(fill->type);

            if (H5T_encode(fill->type, NULL, &dt_size) < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTENCODE, FAIL, "can't encode datatype");

            /* Encode the size of a size_t */
            enc_value = (uint64_t)dt_size;
            enc_size  = H5VM_limit_enc_size(enc_value);
            assert(enc_size < 256);

            /* Encode the size */
            *(*pp)++ = (uint8_t)enc_size;

            /* Encode the size of the encoded datatype */
            UINT64ENCODE_VAR(*pp, enc_value, enc_size);

            if (H5T_encode(fill->type, *pp, &dt_size) < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTENCODE, FAIL, "can't encode datatype");
            *pp += dt_size;
        } /* end if */
    }     /* end if */

    /* Calculate size needed for encoding */
    *size += 2;
    *size += sizeof(int64_t);
    if (fill->size > 0) {
        /* The size of the fill value buffer */
        *size += (size_t)fill->size;

        /* calculate those if they were not calculated earlier */
        if (NULL == *pp) {
            /* Get the size of the encoded datatype */
            assert(fill->type);
            if (H5T_encode(fill->type, NULL, &dt_size) < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTENCODE, FAIL, "can't encode datatype");
            enc_value = (uint64_t)dt_size;
            enc_size  = H5VM_limit_enc_size(enc_value);
        }
        *size += (1 + enc_size);
        *size += dt_size;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dcrt_fill_value_enc() */

/*-------------------------------------------------------------------------
 * Function:       H5P__dcrt_fill_value_dec
 *
 * Purpose:        Callback routine which is called whenever the fill value
 *                 property in the dataset creation property list is
 *                 decoded.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dcrt_fill_value_dec(const void **_pp, void *_value)
{
    H5O_fill_t     *fill      = (H5O_fill_t *)_value; /* Fill value */
    const uint8_t **pp        = (const uint8_t **)_pp;
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    HDcompile_assert(sizeof(size_t) <= sizeof(uint64_t));
    HDcompile_assert(sizeof(ssize_t) <= sizeof(int64_t));

    /* Set property to default value */
    *fill = H5D_def_fill_g;

    /* Decode alloc and fill time */
    fill->alloc_time = (H5D_alloc_time_t) * (*pp)++;
    fill->fill_time  = (H5D_fill_time_t) * (*pp)++;

    /* Decode fill size */
    INT64DECODE(*pp, fill->size);

    /* Check if there's a fill value */
    if (fill->size > 0) {
        size_t   dt_size = 0;
        uint64_t enc_value;
        unsigned enc_size;

        /* Allocate fill buffer and copy the contents in it */
        if (NULL == (fill->buf = H5MM_malloc((size_t)fill->size)))
            HGOTO_ERROR(H5E_PLIST, H5E_CANTALLOC, FAIL, "memory allocation failed for fill value buffer");
        H5MM_memcpy((uint8_t *)fill->buf, *pp, (size_t)fill->size);
        *pp += fill->size;

        enc_size = *(*pp)++;
        assert(enc_size < 256);

        /* Decode the size of encoded datatype */
        UINT64DECODE_VAR(*pp, enc_value, enc_size);
        dt_size = (size_t)enc_value;

        /* Decode type */
        if (NULL == (fill->type = H5T_decode(dt_size, *pp)))
            HGOTO_ERROR(H5E_PLIST, H5E_CANTDECODE, FAIL, "can't decode fill value datatype");
        *pp += dt_size;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dcrt_fill_value_dec() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dcrt_fill_value_del
 *
 * Purpose:     Frees memory used to store the fill value property
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dcrt_fill_value_del(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                         size_t H5_ATTR_UNUSED size, void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(value);

    /* Reset the old fill value message */
    if (H5O_msg_reset(H5O_FILL_ID, value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTRESET, FAIL, "can't release fill value message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dcrt_fill_value_del() */

/*--------------------------------------------------------------------------
 * Function:    H5P__dcrt_fill_value_copy
 *
 * Purpose:     Copy the fill value property
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *--------------------------------------------------------------------------
 */
static herr_t
H5P__dcrt_fill_value_copy(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    H5O_fill_t *fill = (H5O_fill_t *)value; /* Create local aliases for values */
    H5O_fill_t  new_fill;
    herr_t      ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(fill);

    /* Make copy of fill value message */
    if (NULL == H5O_msg_copy(H5O_FILL_ID, fill, &new_fill))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't copy fill value");

    /* Set new fill value message directly into property list */
    *fill = new_fill;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dcrt_fill_value_copy() */

/*-------------------------------------------------------------------------
 * Function:       H5P_fill_value_cmp
 *
 * Purpose:        Callback routine which is called whenever the fill value
 *                 property in the dataset creation property list is compared.
 *
 * Return:         positive if VALUE1 is greater than VALUE2, negative if
 *                      VALUE2 is greater than VALUE1 and zero if VALUE1 and
 *                      VALUE2 are equal.
 *
 *-------------------------------------------------------------------------
 */
int
H5P_fill_value_cmp(const void *_fill1, const void *_fill2, size_t H5_ATTR_UNUSED size)
{
    const H5O_fill_t *fill1 = (const H5O_fill_t *)_fill1, /* Create local aliases for values */
        *fill2              = (const H5O_fill_t *)_fill2;
    int    cmp_value;     /* Value from comparison */
    herr_t ret_value = 0; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity check */
    assert(fill1);
    assert(fill2);
    assert(size == sizeof(H5O_fill_t));

    /* Check the size of fill values */
    if (fill1->size < fill2->size)
        HGOTO_DONE(-1);
    if (fill1->size > fill2->size)
        HGOTO_DONE(1);

    /* Check the types of the fill values */
    if (fill1->type == NULL && fill2->type != NULL)
        HGOTO_DONE(-1);
    if (fill1->type != NULL && fill2->type == NULL)
        HGOTO_DONE(1);
    if (fill1->type != NULL)
        if ((cmp_value = H5T_cmp(fill1->type, fill2->type, false)) != 0)
            HGOTO_DONE(cmp_value);

    /* Check the fill values in the buffers */
    if (fill1->buf == NULL && fill2->buf != NULL)
        HGOTO_DONE(-1);
    if (fill1->buf != NULL && fill2->buf == NULL)
        HGOTO_DONE(1);
    if (fill1->buf != NULL)
        if ((cmp_value = memcmp(fill1->buf, fill2->buf, (size_t)fill1->size)) != 0)
            HGOTO_DONE(cmp_value);

    /* Check the allocation time for the fill values */
    if (fill1->alloc_time < fill2->alloc_time)
        HGOTO_DONE(-1);
    if (fill1->alloc_time > fill2->alloc_time)
        HGOTO_DONE(1);

    /* Check the fill time for the fill values */
    if (fill1->fill_time < fill2->fill_time)
        HGOTO_DONE(-1);
    if (fill1->fill_time > fill2->fill_time)
        HGOTO_DONE(1);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P_fill_value_cmp() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dcrt_fill_value_close
 *
 * Purpose:     Frees memory used to store the fill value property
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dcrt_fill_value_close(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(value);

    /* Reset the old fill value message */
    if (H5O_msg_reset(H5O_FILL_ID, value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTRESET, FAIL, "can't release fill value message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dcrt_fill_value_close() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dcrt_ext_file_list_set
 *
 * Purpose:     Copies an external file list property when it's set for a property list
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dcrt_ext_file_list_set(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                            size_t H5_ATTR_UNUSED size, void *value)
{
    H5O_efl_t *efl = (H5O_efl_t *)value; /* Create local aliases for values */
    H5O_efl_t  new_efl;
    herr_t     ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(value);

    /* Make copy of external file list */
    if (NULL == H5O_msg_copy(H5O_EFL_ID, efl, &new_efl))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't copy external file list");

    /* Copy new external file list message over old one */
    *efl = new_efl;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dcrt_ext_file_list_set() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dcrt_ext_file_list_get
 *
 * Purpose:     Copies an external file list property when it's retrieved from a property list
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dcrt_ext_file_list_get(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                            size_t H5_ATTR_UNUSED size, void *value)
{
    H5O_efl_t *efl = (H5O_efl_t *)value; /* Create local aliases for values */
    H5O_efl_t  new_efl;
    herr_t     ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(value);

    /* Make copy of external file list */
    if (NULL == H5O_msg_copy(H5O_EFL_ID, efl, &new_efl))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't copy external file list");

    /* Copy new external file list message over old one */
    *efl = new_efl;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dcrt_ext_file_list_get() */

/*-------------------------------------------------------------------------
 * Function:       H5P__dcrt_ext_file_list_enc
 *
 * Purpose:        Callback routine which is called whenever the efl
 *                 property in the dataset creation property list is
 *                 encoded.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dcrt_ext_file_list_enc(const void *value, void **_pp, size_t *size)
{
    const H5O_efl_t *efl = (const H5O_efl_t *)value; /* Create local aliases for values */
    size_t           len = 0;                        /* String length of slot name */
    size_t           u;                              /* Local index variable */
    uint8_t        **pp = (uint8_t **)_pp;
    unsigned         enc_size;
    uint64_t         enc_value;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(efl);
    HDcompile_assert(sizeof(size_t) <= sizeof(uint64_t));
    HDcompile_assert(sizeof(off_t) <= sizeof(uint64_t));
    HDcompile_assert(sizeof(hsize_t) <= sizeof(uint64_t));
    assert(size);

    if (NULL != *pp) {
        /* Encode number of slots used */
        enc_value = (uint64_t)efl->nused;
        enc_size  = H5VM_limit_enc_size(enc_value);
        assert(enc_size < 256);
        *(*pp)++ = (uint8_t)enc_size;
        UINT64ENCODE_VAR(*pp, enc_value, enc_size);

        /* Encode file list */
        for (u = 0; u < efl->nused; u++) {
            /* Calculate length of slot name and encode it */
            len       = strlen(efl->slot[u].name) + 1;
            enc_value = (uint64_t)len;
            enc_size  = H5VM_limit_enc_size(enc_value);
            assert(enc_size < 256);
            *(*pp)++ = (uint8_t)enc_size;
            UINT64ENCODE_VAR(*pp, enc_value, enc_size);

            /* Encode name */
            H5MM_memcpy(*pp, (uint8_t *)(efl->slot[u].name), len);
            *pp += len;

            /* Encode offset */
            enc_value = (uint64_t)efl->slot[u].offset;
            enc_size  = H5VM_limit_enc_size(enc_value);
            assert(enc_size < 256);
            *(*pp)++ = (uint8_t)enc_size;
            UINT64ENCODE_VAR(*pp, enc_value, enc_size);

            /* encode size */
            enc_value = (uint64_t)efl->slot[u].size;
            enc_size  = H5VM_limit_enc_size(enc_value);
            assert(enc_size < 256);
            *(*pp)++ = (uint8_t)enc_size;
            UINT64ENCODE_VAR(*pp, enc_value, enc_size);
        } /* end for */
    }     /* end if */

    /* Calculate size needed for encoding */
    *size += (1 + H5VM_limit_enc_size((uint64_t)efl->nused));
    for (u = 0; u < efl->nused; u++) {
        len = strlen(efl->slot[u].name) + 1;
        *size += (1 + H5VM_limit_enc_size((uint64_t)len));
        *size += len;
        *size += (1 + H5VM_limit_enc_size((uint64_t)efl->slot[u].offset));
        *size += (1 + H5VM_limit_enc_size((uint64_t)efl->slot[u].size));
    } /* end for */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__dcrt_ext_file_list_enc() */

/*-------------------------------------------------------------------------
 * Function:       H5P__dcrt_ext_file_list_dec
 *
 * Purpose:        Callback routine which is called whenever the efl
 *                 property in the dataset creation property list is
 *                 decoded.
 *
 * Return:	   Success:	Non-negative
 *		   Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dcrt_ext_file_list_dec(const void **_pp, void *_value)
{
    H5O_efl_t      *efl = (H5O_efl_t *)_value; /* External file list */
    const uint8_t **pp  = (const uint8_t **)_pp;
    size_t          u, nused;
    unsigned        enc_size;
    uint64_t        enc_value;
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(pp);
    assert(*pp);
    assert(efl);
    HDcompile_assert(sizeof(size_t) <= sizeof(uint64_t));
    HDcompile_assert(sizeof(off_t) <= sizeof(uint64_t));
    HDcompile_assert(sizeof(hsize_t) <= sizeof(uint64_t));

    /* Set property to default value */
    *efl = H5D_def_efl_g;

    /* Decode number of slots used */
    enc_size = *(*pp)++;
    assert(enc_size < 256);
    UINT64DECODE_VAR(*pp, enc_value, enc_size);
    nused = (size_t)enc_value;

    /* Decode information for each slot */
    for (u = 0; u < nused; u++) {
        size_t len;
        if (efl->nused >= efl->nalloc) {
            size_t           na = efl->nalloc + H5O_EFL_ALLOC;
            H5O_efl_entry_t *x  = (H5O_efl_entry_t *)H5MM_realloc(efl->slot, na * sizeof(H5O_efl_entry_t));
            if (!x)
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "memory allocation failed");

            efl->nalloc = na;
            efl->slot   = x;
        } /* end if */

        /* Decode length of slot name */
        enc_size = *(*pp)++;
        assert(enc_size < 256);
        UINT64DECODE_VAR(*pp, enc_value, enc_size);
        len = (size_t)enc_value;

        /* Allocate name buffer and decode the name into it */
        efl->slot[u].name = H5MM_xstrdup((const char *)(*pp));
        *pp += len;

        /* decode offset */
        enc_size = *(*pp)++;
        assert(enc_size < 256);
        UINT64DECODE_VAR(*pp, enc_value, enc_size);
        efl->slot[u].offset = (off_t)enc_value;

        /* decode size */
        enc_size = *(*pp)++;
        assert(enc_size < 256);
        UINT64DECODE_VAR(*pp, enc_value, enc_size);
        efl->slot[u].size = (hsize_t)enc_value;

        efl->slot[u].name_offset = 0; /*not entered into heap yet*/
        efl->nused++;
    } /* end for */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dcrt_ext_file_list_dec() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dcrt_ext_file_list_del
 *
 * Purpose:     Frees memory used to store the efl property
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dcrt_ext_file_list_del(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                            size_t H5_ATTR_UNUSED size, void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(value);

    /* Reset the old efl message */
    if (H5O_msg_reset(H5O_EFL_ID, value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTRESET, FAIL, "can't release external file list message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dcrt_ext_file_list_del() */

/*--------------------------------------------------------------------------
 * Function:    H5P__dcrt_ext_file_list_copy
 *
 * Purpose:     Copy the efl property
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *--------------------------------------------------------------------------
 */
static herr_t
H5P__dcrt_ext_file_list_copy(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    H5O_efl_t *efl = (H5O_efl_t *)value; /* Create local aliases for values */
    H5O_efl_t  new_efl;
    herr_t     ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(efl);

    /* Make copy of efl message */
    if (NULL == H5O_msg_copy(H5O_EFL_ID, efl, &new_efl))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't copy external file list");

    /* Set new efl message directly into property list */
    *efl = new_efl;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dcrt_ext_file_list_copy() */

/*-------------------------------------------------------------------------
 * Function:       H5P__dcrt_ext_file_list_cmp
 *
 * Purpose:        Callback routine which is called whenever the external file
 *                 list property in the dataset creation property list is
 *                 compared.
 *
 * Return:         positive if VALUE1 is greater than VALUE2, negative if
 *                      VALUE2 is greater than VALUE1 and zero if VALUE1 and
 *                      VALUE2 are equal.
 *
 *-------------------------------------------------------------------------
 */
static int
H5P__dcrt_ext_file_list_cmp(const void *_efl1, const void *_efl2, size_t H5_ATTR_UNUSED size)
{
    const H5O_efl_t *efl1 = (const H5O_efl_t *)_efl1, /* Create local aliases for values */
        *efl2             = (const H5O_efl_t *)_efl2;
    int    cmp_value;     /* Value from comparison */
    herr_t ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(efl1);
    assert(efl2);
    assert(size == sizeof(H5O_efl_t));

    /* Check the number of allocated efl entries */
    if (efl1->nalloc < efl2->nalloc)
        HGOTO_DONE(-1);
    if (efl1->nalloc > efl2->nalloc)
        HGOTO_DONE(1);

    /* Check the number of used efl entries */
    if (efl1->nused < efl2->nused)
        HGOTO_DONE(-1);
    if (efl1->nused > efl2->nused)
        HGOTO_DONE(1);

    /* Check the efl entry information */
    if (efl1->slot == NULL && efl2->slot != NULL)
        HGOTO_DONE(-1);
    if (efl1->slot != NULL && efl2->slot == NULL)
        HGOTO_DONE(1);
    if (efl1->slot != NULL && efl1->nused > 0) {
        size_t u; /* Local index variable */

        /* Loop through all entries, comparing them */
        for (u = 0; u < efl1->nused; u++) {
            /* Check the name offset of the efl entry */
            if (efl1->slot[u].name_offset < efl2->slot[u].name_offset)
                HGOTO_DONE(-1);
            if (efl1->slot[u].name_offset > efl2->slot[u].name_offset)
                HGOTO_DONE(1);

            /* Check the name of the efl entry */
            if (efl1->slot[u].name == NULL && efl2->slot[u].name != NULL)
                HGOTO_DONE(-1);
            if (efl1->slot[u].name != NULL && efl2->slot[u].name == NULL)
                HGOTO_DONE(1);
            if (efl1->slot[u].name != NULL)
                if ((cmp_value = strcmp(efl1->slot[u].name, efl2->slot[u].name)) != 0)
                    HGOTO_DONE(cmp_value);

            /* Check the file offset of the efl entry */
            if (efl1->slot[u].offset < efl2->slot[u].offset)
                HGOTO_DONE(-1);
            if (efl1->slot[u].offset > efl2->slot[u].offset)
                HGOTO_DONE(1);

            /* Check the file size of the efl entry */
            if (efl1->slot[u].size < efl2->slot[u].size)
                HGOTO_DONE(-1);
            if (efl1->slot[u].size > efl2->slot[u].size)
                HGOTO_DONE(1);
        } /* end for */
    }     /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dcrt_ext_file_list_cmp() */

/*-------------------------------------------------------------------------
 * Function:    H5P__dcrt_ext_file_list_close
 *
 * Purpose:     Frees memory used to store the efl property
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__dcrt_ext_file_list_close(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(value);

    /* Reset the old efl message */
    if (H5O_msg_reset(H5O_EFL_ID, value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTRESET, FAIL, "can't release external file list message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__dcrt_ext_file_list_close() */

/*-------------------------------------------------------------------------
 * Function:  H5P__set_layout
 *
 * Purpose:   Sets the layout of raw data in the file.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__set_layout(H5P_genplist_t *plist, const H5O_layout_t *layout)
{
    unsigned alloc_time_state;    /* State of allocation time property */
    herr_t   ret_value = SUCCEED; /* return value */

    FUNC_ENTER_PACKAGE

    /* Get the allocation time state */
    if (H5P_get(plist, H5D_CRT_ALLOC_TIME_STATE_NAME, &alloc_time_state) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get space allocation time state");

    /* If we still have the "default" allocation time, change it according to the new layout */
    if (alloc_time_state) {
        H5O_fill_t fill; /* Fill value */

        /* Get current fill value info */
        if (H5P_peek(plist, H5D_CRT_FILL_VALUE_NAME, &fill) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get fill value");

        /* Set the default based on layout */
        switch (layout->type) {
            case H5D_COMPACT:
                fill.alloc_time = H5D_ALLOC_TIME_EARLY;
                break;

            case H5D_CONTIGUOUS:
                fill.alloc_time = H5D_ALLOC_TIME_LATE;
                break;

            case H5D_CHUNKED:
            case H5D_VIRTUAL:
                fill.alloc_time = H5D_ALLOC_TIME_INCR;
                break;

            case H5D_LAYOUT_ERROR:
            case H5D_NLAYOUTS:
            default:
                HGOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL, "unknown layout type");
        } /* end switch */

        /* Set updated fill value info */
        if (H5P_poke(plist, H5D_CRT_FILL_VALUE_NAME, &fill) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set space allocation time");
    } /* end if */

    /* Set layout value */
    if (H5P_set(plist, H5D_CRT_LAYOUT_NAME, layout) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINIT, FAIL, "can't set layout");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__set_layout() */

/*-------------------------------------------------------------------------
 * Function:	H5Pset_layout
 *
 * Purpose:	Sets the layout of raw data in the file.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_layout(hid_t plist_id, H5D_layout_t layout_type)
{
    H5P_genplist_t     *plist;               /* Property list pointer */
    const H5O_layout_t *layout;              /* Pointer to default layout information for type specified */
    herr_t              ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iDl", plist_id, layout_type);

    /* Check arguments */
    if (layout_type < 0 || layout_type >= H5D_NLAYOUTS)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "raw data layout method is not valid");

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_CREATE)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get pointer to correct default layout */
    switch (layout_type) {
        case H5D_COMPACT:
            layout = &H5D_def_layout_compact_g;
            break;

        case H5D_CONTIGUOUS:
            layout = &H5D_def_layout_contig_g;
            break;

        case H5D_CHUNKED:
            layout = &H5D_def_layout_chunk_g;
            break;

        case H5D_VIRTUAL:
            layout = &H5D_def_layout_virtual_g;
            break;

        case H5D_LAYOUT_ERROR:
        case H5D_NLAYOUTS:
        default:
            HGOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL, "unknown layout type");
    } /* end switch */

    /* Set value */
    if (H5P__set_layout(plist, layout) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINIT, FAIL, "can't set layout");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_layout() */

/*-------------------------------------------------------------------------
 * Function:	H5Pget_layout
 *
 * Purpose:	Retrieves layout type of a dataset creation property list.
 *
 * Return:	Success:	The layout type
 *
 *		Failure:	H5D_LAYOUT_ERROR (negative)
 *
 *-------------------------------------------------------------------------
 */
H5D_layout_t
H5Pget_layout(hid_t plist_id)
{
    H5P_genplist_t *plist;     /* Property list pointer */
    H5O_layout_t    layout;    /* Layout property */
    H5D_layout_t    ret_value; /* Return value */

    FUNC_ENTER_API(H5D_LAYOUT_ERROR)
    H5TRACE1("Dl", "i", plist_id);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_CREATE)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, H5D_LAYOUT_ERROR, "can't find object for ID");

    /* Peek at layout property */
    if (H5P_peek(plist, H5D_CRT_LAYOUT_NAME, &layout) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, H5D_LAYOUT_ERROR, "can't get layout");

    /* Set return value */
    ret_value = layout.type;

done:
    FUNC_LEAVE_API(ret_value)
} /* ed H5Pget_layout() */

/*-------------------------------------------------------------------------
 * Function:	H5Pset_chunk
 *
 * Purpose:	Sets the number of dimensions and the size of each chunk to
 *		the values specified.  The dimensionality of the chunk should
 *		match the dimensionality of the dataspace.
 *
 *		As a side effect, the layout method is changed to
 *		H5D_CHUNKED.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_chunk(hid_t plist_id, int ndims, const hsize_t dim[/*ndims*/])
{
    H5P_genplist_t *plist;               /* Property list pointer */
    H5O_layout_t    chunk_layout;        /* Layout information for setting chunk info */
    uint64_t        chunk_nelmts;        /* Number of elements in chunk */
    unsigned        u;                   /* Local index variable */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "iIs*[a1]h", plist_id, ndims, dim);

    /* Check arguments */
    if (ndims <= 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "chunk dimensionality must be positive");
    if (ndims > H5S_MAX_RANK)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "chunk dimensionality is too large");
    if (!dim)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no chunk dimensions specified");

    /* Verify & initialize property's chunk dims */
    H5MM_memcpy(&chunk_layout, &H5D_def_layout_chunk_g, sizeof(H5D_def_layout_chunk_g));
    memset(&chunk_layout.u.chunk.dim, 0, sizeof(chunk_layout.u.chunk.dim));
    chunk_nelmts = 1;
    for (u = 0; u < (unsigned)ndims; u++) {
        if (dim[u] == 0)
            HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "all chunk dimensions must be positive");
        if (dim[u] != (dim[u] & 0xffffffff))
            HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "all chunk dimensions must be less than 2^32");
        chunk_nelmts *= dim[u];
        if (chunk_nelmts > (uint64_t)0xffffffff)
            HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "number of elements in chunk must be < 4GB");
        chunk_layout.u.chunk.dim[u] = (uint32_t)dim[u]; /* Store user's chunk dimensions */
    }                                                   /* end for */

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_CREATE)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set chunk information in property list */
    chunk_layout.u.chunk.ndims = (unsigned)ndims;
    if (H5P__set_layout(plist, &chunk_layout) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set layout");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_chunk() */

/*-------------------------------------------------------------------------
 * Function:	H5Pget_chunk
 *
 * Purpose:	Retrieves the chunk size of chunked layout.  The chunk
 *		dimensionality is returned and the chunk size in each
 *		dimension is returned through the DIM argument.	 At most
 *		MAX_NDIMS elements of DIM will be initialized.
 *
 * Return:	Success:	Positive Chunk dimensionality.
 *
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
int
H5Pget_chunk(hid_t plist_id, int max_ndims, hsize_t dim[] /*out*/)
{
    H5P_genplist_t *plist;     /* Property list pointer */
    H5O_layout_t    layout;    /* Layout information */
    int             ret_value; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("Is", "iIsx", plist_id, max_ndims, dim);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_CREATE)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Peek at the layout property */
    if (H5P_peek(plist, H5D_CRT_LAYOUT_NAME, &layout) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't get layout");
    if (H5D_CHUNKED != layout.type)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a chunked storage layout");

    if (dim) {
        unsigned u; /* Local index variable */

        /* Get the dimension sizes */
        for (u = 0; u < layout.u.chunk.ndims && u < (unsigned)max_ndims; u++)
            dim[u] = layout.u.chunk.dim[u];
    } /* end if */

    /* Set the return value */
    ret_value = (int)layout.u.chunk.ndims;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_chunk() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_virtual
 *
 * Purpose:     Maps elements of the virtual dataset described by the
 *              virtual dataspace identifier vspace_id to the elements of
 *              the source dataset described by the source dataset
 *              dataspace identifier src_space_id.  The source dataset is
 *              identified by the name of the file where it is located,
 *              src_file_name, and the name of the dataset, src_dset_name.
 *
 *              As a side effect, the layout method is changed to
 *              H5D_VIRTUAL.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_virtual(hid_t dcpl_id, hid_t vspace_id, const char *src_file_name, const char *src_dset_name,
               hid_t src_space_id)
{
    H5P_genplist_t            *plist = NULL;               /* Property list pointer */
    H5O_layout_t               virtual_layout;             /* Layout information for setting virtual info */
    H5S_t                     *vspace;                     /* Virtual dataset space selection */
    H5S_t                     *src_space;                  /* Source dataset space selection */
    H5O_storage_virtual_ent_t *old_list         = NULL;    /* List pointer previously on property list */
    H5O_storage_virtual_ent_t *ent              = NULL;    /* Convenience pointer to new VDS entry */
    bool                       retrieved_layout = false;   /* Whether the layout has been retrieved */
    bool                       free_list        = false;   /* Whether to free the list of virtual entries */
    herr_t                     ret_value        = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "ii*s*si", dcpl_id, vspace_id, src_file_name, src_dset_name, src_space_id);

    /* Check arguments */
    if (!src_file_name)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "source file name not provided");
    if (!src_dset_name)
        HGOTO_ERROR(H5E_PLIST, H5E_BADRANGE, FAIL, "source dataset name not provided");
    if (NULL == (vspace = (H5S_t *)H5I_object_verify(vspace_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "not a dataspace");
    if (NULL == (src_space = (H5S_t *)H5I_object_verify(src_space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "not a dataspace");

    /* Check selections for validity */
    if (H5D_virtual_check_mapping_pre(vspace, src_space, H5O_VIRTUAL_STATUS_USER) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "invalid mapping selections");

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(dcpl_id, H5P_DATASET_CREATE)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get the current layout */
    if (H5P_peek(plist, H5D_CRT_LAYOUT_NAME, &virtual_layout) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get layout");
    retrieved_layout = true;

    /* If the layout was not already virtual, Start with default virtual layout.
     * Otherwise, add the mapping to the current list. */
    if (virtual_layout.type == H5D_VIRTUAL)
        /* Save old list pointer for error recovery */
        old_list = virtual_layout.storage.u.virt.list;
    else {
        /* Reset the old layout */
        if (H5O_msg_reset(H5O_LAYOUT_ID, &virtual_layout) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTRESET, FAIL, "can't release layout message");

        /* Copy the default virtual layout */
        H5MM_memcpy(&virtual_layout, &H5D_def_layout_virtual_g, sizeof(H5D_def_layout_virtual_g));

        /* Sanity check */
        assert(virtual_layout.storage.u.virt.list_nalloc == 0);
    } /* end else */

    /* Expand list if necessary */
    if (virtual_layout.storage.u.virt.list_nused == virtual_layout.storage.u.virt.list_nalloc) {
        H5O_storage_virtual_ent_t *x; /* Pointer to the new list */
        size_t new_alloc = MAX(H5D_VIRTUAL_DEF_LIST_SIZE, virtual_layout.storage.u.virt.list_nalloc * 2);

        /* Expand size of entry list */
        if (NULL == (x = (H5O_storage_virtual_ent_t *)H5MM_realloc(
                         virtual_layout.storage.u.virt.list, new_alloc * sizeof(H5O_storage_virtual_ent_t))))
            HGOTO_ERROR(H5E_PLIST, H5E_RESOURCE, FAIL, "can't reallocate virtual dataset mapping list");
        virtual_layout.storage.u.virt.list        = x;
        virtual_layout.storage.u.virt.list_nalloc = new_alloc;
    } /* end if */

    /* Add virtual dataset mapping entry */
    ent = &virtual_layout.storage.u.virt.list[virtual_layout.storage.u.virt.list_nused];
    memset(ent, 0, sizeof(H5O_storage_virtual_ent_t)); /* Clear before starting to set up */
    if (NULL == (ent->source_dset.virtual_select = H5S_copy(vspace, false, true)))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "unable to copy virtual selection");
    if (NULL == (ent->source_file_name = H5MM_xstrdup(src_file_name)))
        HGOTO_ERROR(H5E_PLIST, H5E_RESOURCE, FAIL, "can't duplicate source file name");
    if (NULL == (ent->source_dset_name = H5MM_xstrdup(src_dset_name)))
        HGOTO_ERROR(H5E_PLIST, H5E_RESOURCE, FAIL, "can't duplicate source file name");
    if (NULL == (ent->source_select = H5S_copy(src_space, false, true)))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "unable to copy source selection");
    if (H5D_virtual_parse_source_name(ent->source_file_name, &ent->parsed_source_file_name,
                                      &ent->psfn_static_strlen, &ent->psfn_nsubs) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINIT, FAIL, "can't parse source file name");
    if (H5D_virtual_parse_source_name(ent->source_dset_name, &ent->parsed_source_dset_name,
                                      &ent->psdn_static_strlen, &ent->psdn_nsubs) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINIT, FAIL, "can't parse source dataset name");
    if ((ent->psfn_nsubs == 0) && (ent->psdn_nsubs == 0)) {
        if (ent->parsed_source_file_name)
            ent->source_dset.file_name = ent->parsed_source_file_name->name_segment;
        else
            ent->source_dset.file_name = ent->source_file_name;
        if (ent->parsed_source_dset_name)
            ent->source_dset.dset_name = ent->parsed_source_dset_name->name_segment;
        else
            ent->source_dset.dset_name = ent->source_dset_name;
    } /* end if */
    ent->unlim_dim_source  = H5S_get_select_unlim_dim(src_space);
    ent->unlim_dim_virtual = H5S_get_select_unlim_dim(vspace);
    if (ent->unlim_dim_virtual < 0) {
        ent->source_dset.clipped_source_select  = ent->source_select;
        ent->source_dset.clipped_virtual_select = ent->source_dset.virtual_select;
    } /* end if */
    ent->unlim_extent_source  = HSIZE_UNDEF;
    ent->unlim_extent_virtual = HSIZE_UNDEF;
    ent->clip_size_source     = HSIZE_UNDEF;
    ent->clip_size_virtual    = HSIZE_UNDEF;
    ent->source_space_status  = H5O_VIRTUAL_STATUS_USER;
    ent->virtual_space_status = H5O_VIRTUAL_STATUS_USER;

    /* Check entry for validity */
    if (H5D_virtual_check_mapping_post(ent) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid mapping entry");

    /* Update min_dims */
    if (H5D_virtual_update_min_dims(&virtual_layout, virtual_layout.storage.u.virt.list_nused) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINIT, FAIL, "unable to update virtual dataset minimum dimensions");

    /* Finish adding entry */
    virtual_layout.storage.u.virt.list_nused++;

done:
    /* Set VDS layout information in property list */
    /* (Even on failure, so there's not a mangled layout struct in the list) */
    if (retrieved_layout) {
        if (H5P_poke(plist, H5D_CRT_LAYOUT_NAME, &virtual_layout) < 0) {
            HDONE_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set layout");
            if (old_list != virtual_layout.storage.u.virt.list)
                free_list = true;
        } /* end if */
    }     /* end if */

    /* Check if the entry has been partly allocated but not added to the
     * property list or not included in list_nused */
    if (ret_value < 0) {
        /* Free incomplete entry if present */
        if (ent) {
            ent->source_file_name = (char *)H5MM_xfree(ent->source_file_name);
            ent->source_dset_name = (char *)H5MM_xfree(ent->source_dset_name);
            if (ent->source_dset.virtual_select && H5S_close(ent->source_dset.virtual_select) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "unable to release virtual selection");
            ent->source_dset.virtual_select = NULL;
            if (ent->source_select && H5S_close(ent->source_select) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "unable to release source selection");
            ent->source_select = NULL;
            H5D_virtual_free_parsed_name(ent->parsed_source_file_name);
            ent->parsed_source_file_name = NULL;
            H5D_virtual_free_parsed_name(ent->parsed_source_dset_name);
            ent->parsed_source_dset_name = NULL;
        } /* end if */

        /* Free list if necessary */
        if (free_list)
            virtual_layout.storage.u.virt.list =
                (H5O_storage_virtual_ent_t *)H5MM_xfree(virtual_layout.storage.u.virt.list);
    } /* end if */

    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_virtual() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_virtual_count
 *
 * Purpose:     Gets the number of mappings for the virtual dataset that
 *              has a creation property list specified by the dcpl_id
 *              parameter.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_virtual_count(hid_t dcpl_id, size_t *count /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    H5O_layout_t    layout;              /* Layout information */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", dcpl_id, count);

    if (count) {
        /* Get the plist structure */
        if (NULL == (plist = H5P_object_verify(dcpl_id, H5P_DATASET_CREATE)))
            HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

        /* Retrieve the layout property */
        if (H5P_peek(plist, H5D_CRT_LAYOUT_NAME, &layout) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't get layout");
        if (H5D_VIRTUAL != layout.type)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a virtual storage layout");

        /* Return the number of mappings  */
        *count = layout.storage.u.virt.list_nused;
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_virtual_count() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_virtual_vspace
 *
 * Purpose:     Takes the dataset creation property list for the virtual
 *              dataset, dcpl_id, and the mapping index, index, and
 *              returns a dataspace identifier for the selection within
 *              the virtual dataset used in the mapping.
 *
 * Return:      Returns a dataspace identifier if successful; otherwise
 *              returns a negative value.
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Pget_virtual_vspace(hid_t dcpl_id, size_t idx)
{
    H5P_genplist_t *plist;        /* Property list pointer */
    H5O_layout_t    layout;       /* Layout information */
    H5S_t          *space = NULL; /* Dataspace pointer */
    hid_t           ret_value;    /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("i", "iz", dcpl_id, idx);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(dcpl_id, H5P_DATASET_CREATE)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Retrieve the layout property */
    if (H5P_peek(plist, H5D_CRT_LAYOUT_NAME, &layout) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't get layout");
    if (H5D_VIRTUAL != layout.type)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a virtual storage layout");

    /* Get the virtual space */
    if (idx >= layout.storage.u.virt.list_nused)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "invalid index (out of range)");
    assert(layout.storage.u.virt.list_nused <= layout.storage.u.virt.list_nalloc);
    if (NULL == (space = H5S_copy(layout.storage.u.virt.list[idx].source_dset.virtual_select, false, true)))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "unable to copy virtual selection");

    /* Register ID */
    if ((ret_value = H5I_register(H5I_DATASPACE, space, true)) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTREGISTER, FAIL, "unable to register dataspace");

done:
    /* Free space on failure */
    if ((ret_value < 0) && space)
        if (H5S_close(space) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "unable to release source selection");

    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_virtual_vspace() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_virtual_srcspace
 *
 * Purpose:     Takes the dataset creation property list for the virtual
 *              dataset, dcpl_id, and the mapping index, index, and
 *              returns a dataspace identifier for the selection within
 *              the source dataset used in the mapping.
 *
 * Return:      Returns a dataspace identifier if successful; otherwise
 *              returns a negative value.
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Pget_virtual_srcspace(hid_t dcpl_id, size_t idx)
{
    H5P_genplist_t *plist;            /* Property list pointer */
    H5O_layout_t    layout;           /* Layout information */
    H5S_t          *space     = NULL; /* Dataspace pointer */
    hid_t           ret_value = FAIL; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("i", "iz", dcpl_id, idx);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(dcpl_id, H5P_DATASET_CREATE)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Retrieve the layout property */
    if (H5P_peek(plist, H5D_CRT_LAYOUT_NAME, &layout) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't get layout");
    if (H5D_VIRTUAL != layout.type)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a virtual storage layout");

    /* Check index */
    if (idx >= layout.storage.u.virt.list_nused)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "invalid index (out of range)");
    assert(layout.storage.u.virt.list_nused <= layout.storage.u.virt.list_nalloc);

    /* Attempt to open source dataset and patch extent if extent status is not
     * H5O_VIRTUAL_STATUS_CORRECT?  -NAF */
    /* If source space status is H5O_VIRTUAL_STATUS_INVALID, patch with bounds
     * of selection */
    if ((H5O_VIRTUAL_STATUS_INVALID == layout.storage.u.virt.list[idx].source_space_status) &&
        (layout.storage.u.virt.list[idx].unlim_dim_source < 0)) {
        hsize_t bounds_start[H5S_MAX_RANK];
        hsize_t bounds_end[H5S_MAX_RANK];
        int     rank;
        int     i;

        /* Get rank of source space */
        if ((rank = H5S_GET_EXTENT_NDIMS(layout.storage.u.virt.list[idx].source_select)) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get source space rank");

        /* Get bounds of selection */
        if (H5S_SELECT_BOUNDS(layout.storage.u.virt.list[idx].source_select, bounds_start, bounds_end) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get selection bounds");

        /* Adjust bounds to extent */
        for (i = 0; i < rank; i++)
            bounds_end[i]++;

        /* Set extent */
        if (H5S_set_extent_simple(layout.storage.u.virt.list[idx].source_select, (unsigned)rank, bounds_end,
                                  NULL) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set source space extent");

        /* Update source space status */
        layout.storage.u.virt.list[idx].source_space_status = H5O_VIRTUAL_STATUS_SEL_BOUNDS;
    } /* end if */

    /* Get the source space */
    if (NULL == (space = H5S_copy(layout.storage.u.virt.list[idx].source_select, false, true)))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "unable to copy source selection");

    /* Register ID */
    if ((ret_value = H5I_register(H5I_DATASPACE, space, true)) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTREGISTER, FAIL, "unable to register dataspace");

done:
    /* Free space on failure */
    if ((ret_value < 0) && space)
        if (H5S_close(space) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "unable to release source selection");

    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_virtual_srcspace() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_virtual_filename
 *
 * Purpose:     Takes the dataset creation property list for the virtual
 *              dataset, dcpl_id, and the mapping index, index, and
 *              retrieves a name of a file for a source dataset used in
 *              the mapping.
 *
 *              Up to size characters of the filename are returned in
 *              name; additional characters, if any, are not returned to
 *              the user application.
 *
 *              If the length of the filename, which determines the
 *              required value of size, is unknown, a preliminary call to
 *              H5Pget_virtual_filename with 'name' set to NULL and 'size'
 *              set to zero can be made. The return value of this call will
 *              be the size in bytes of the filename.  That value, plus 1
 *              for a NULL terminator, is then assigned to size for a
 *              second H5Pget_virtual_filename call, which will retrieve
 *              the actual filename.
 *
 * Return:      Returns the length of the name if successful, otherwise
 *              returns a negative value.
 *
 *-------------------------------------------------------------------------
 */
ssize_t
H5Pget_virtual_filename(hid_t dcpl_id, size_t idx, char *name /*out*/, size_t size)
{
    H5P_genplist_t *plist;     /* Property list pointer */
    H5O_layout_t    layout;    /* Layout information */
    ssize_t         ret_value; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("Zs", "izxz", dcpl_id, idx, name, size);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(dcpl_id, H5P_DATASET_CREATE)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Retrieve the layout property */
    if (H5P_peek(plist, H5D_CRT_LAYOUT_NAME, &layout) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't get layout");
    if (H5D_VIRTUAL != layout.type)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a virtual storage layout");

    /* Get the virtual filename */
    if (idx >= layout.storage.u.virt.list_nused)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "invalid index (out of range)");
    assert(layout.storage.u.virt.list_nused <= layout.storage.u.virt.list_nalloc);
    assert(layout.storage.u.virt.list[idx].source_file_name);
    if (name && (size > 0))
        (void)strncpy(name, layout.storage.u.virt.list[idx].source_file_name, size);
    ret_value = (ssize_t)strlen(layout.storage.u.virt.list[idx].source_file_name);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_virtual_filename() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_virtual_dsetname
 *
 * Purpose:     Takes the dataset creation property list for the virtual
 *              dataset, dcpl_id, and the mapping index, index, and
 *              retrieves the name of a source dataset used in the mapping.
 *
 *              Up to size characters of the name are returned in name;
 *              additional characters, if any, are not returned to the
 *              user application.
 *
 *              If the length of the dataset name, which determines the
 *              required value of size, is unknown, a preliminary call to
 *              H5Pget_virtual_dsetname with 'name' set to NULL and 'size'
 *              set to zero can be made.  The return value of this call will
 *              be the size in bytes of the dataset name.  That value, plus 1
 *              for a NULL terminator, is then assigned to size for a
 *              second H5Pget_virtual_dsetname call, which will retrieve
 *              the actual dataset name.
 *
 * Return:      Returns the length of the name if successful, otherwise
 *              returns a negative value.
 *
 *-------------------------------------------------------------------------
 */
ssize_t
H5Pget_virtual_dsetname(hid_t dcpl_id, size_t idx, char *name /*out*/, size_t size)
{
    H5P_genplist_t *plist;     /* Property list pointer */
    H5O_layout_t    layout;    /* Layout information */
    ssize_t         ret_value; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("Zs", "izxz", dcpl_id, idx, name, size);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(dcpl_id, H5P_DATASET_CREATE)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Retrieve the layout property */
    if (H5P_peek(plist, H5D_CRT_LAYOUT_NAME, &layout) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't get layout");
    if (H5D_VIRTUAL != layout.type)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a virtual storage layout");

    /* Get the virtual filename */
    if (idx >= layout.storage.u.virt.list_nused)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "invalid index (out of range)");
    assert(layout.storage.u.virt.list_nused <= layout.storage.u.virt.list_nalloc);
    assert(layout.storage.u.virt.list[idx].source_dset_name);
    if (name && (size > 0))
        (void)strncpy(name, layout.storage.u.virt.list[idx].source_dset_name, size);
    ret_value = (ssize_t)strlen(layout.storage.u.virt.list[idx].source_dset_name);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_virtual_dsetname() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_chunk_opts
 *
 * Purpose:     Sets the options related to chunked storage for a dataset.
 *              The storage must already be set to chunked.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_chunk_opts(hid_t plist_id, unsigned options)
{
    H5P_genplist_t *plist;                  /* Property list pointer */
    H5O_layout_t    layout;                 /* Layout information for setting chunk info */
    uint8_t         layout_flags = 0;       /* "options" translated into layout message flags format */
    herr_t          ret_value    = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iIu", plist_id, options);

    /* Check arguments */
    if (options & ~(H5D_CHUNK_DONT_FILTER_PARTIAL_CHUNKS))
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "unknown chunk options");

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_CREATE)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Retrieve the layout property */
    if (H5P_peek(plist, H5D_CRT_LAYOUT_NAME, &layout) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't get layout");
    if (H5D_CHUNKED != layout.type)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a chunked storage layout");

    /* Translate options into flags that can be used with the layout message */
    if (options & H5D_CHUNK_DONT_FILTER_PARTIAL_CHUNKS)
        layout_flags |= H5O_LAYOUT_CHUNK_DONT_FILTER_PARTIAL_BOUND_CHUNKS;

    /* Update the layout message, including the version (if necessary) */
    /* This probably isn't the right way to do this, and should be changed once
     * this branch gets the "real" way to set the layout version */
    layout.u.chunk.flags = layout_flags;
    if (layout.version < H5O_LAYOUT_VERSION_4)
        layout.version = H5O_LAYOUT_VERSION_4;

    /* Set layout value */
    if (H5P_poke(plist, H5D_CRT_LAYOUT_NAME, &layout) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINIT, FAIL, "can't set layout");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_chunk_opts() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_chunk_opts
 *
 * Purpose:     Gets the options related to chunked storage for a dataset.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_chunk_opts(hid_t plist_id, unsigned *options /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    H5O_layout_t    layout;              /* Layout information for setting chunk info */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", plist_id, options);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_CREATE)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Retrieve the layout property */
    if (H5P_peek(plist, H5D_CRT_LAYOUT_NAME, &layout) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't get layout");
    if (H5D_CHUNKED != layout.type)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a chunked storage layout");

    if (options) {
        /* Translate options from flags that can be used with the layout message
         * to those known to the public */
        *options = 0;
        if (layout.u.chunk.flags & H5O_LAYOUT_CHUNK_DONT_FILTER_PARTIAL_BOUND_CHUNKS)
            *options |= H5D_CHUNK_DONT_FILTER_PARTIAL_CHUNKS;
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_chunk_opts() */

/*-------------------------------------------------------------------------
 * Function:	H5Pset_external
 *
 * Purpose:	Adds an external file to the list of external files. PLIST_ID
 *		should be an object ID for a dataset creation property list.
 *		NAME is the name of an external file, OFFSET is the location
 *		where the data starts in that file, and SIZE is the number of
 *		bytes reserved in the file for the data.
 *
 *		If a dataset is split across multiple files then the files
 *		should be defined in order. The total size of the dataset is
 *		the sum of the SIZE arguments for all the external files.  If
 *		the total size is larger than the size of a dataset then the
 *		dataset can be extended (provided the dataspace also allows
 *		the extending).
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_external(hid_t plist_id, const char *name, off_t offset, hsize_t size)
{
    size_t          idx;
    hsize_t         total, tmp;
    H5O_efl_t       efl;
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "i*soh", plist_id, name, offset, size);

    /* Check arguments */
    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no name given");
    if (offset < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "negative external file offset");

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_CREATE)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    if (H5P_peek(plist, H5D_CRT_EXT_FILE_LIST_NAME, &efl) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get external file list");
    if (efl.nused > 0 && H5O_EFL_UNLIMITED == efl.slot[efl.nused - 1].size)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "previous file size is unlimited");

    if (H5O_EFL_UNLIMITED != size) {
        for (idx = 0, total = size; idx < efl.nused; idx++, total = tmp) {
            tmp = total + efl.slot[idx].size;
            if (tmp <= total)
                HGOTO_ERROR(H5E_EFL, H5E_OVERFLOW, FAIL, "total external data size overflowed");
        } /* end for */
    }     /* end if */

    /* Add to the list */
    if (efl.nused >= efl.nalloc) {
        size_t           na = efl.nalloc + H5O_EFL_ALLOC;
        H5O_efl_entry_t *x  = (H5O_efl_entry_t *)H5MM_realloc(efl.slot, na * sizeof(H5O_efl_entry_t));

        if (!x)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "memory allocation failed");
        efl.nalloc = na;
        efl.slot   = x;
    } /* end if */
    idx                       = efl.nused;
    efl.slot[idx].name_offset = 0; /*not entered into heap yet*/
    efl.slot[idx].name        = H5MM_xstrdup(name);
    efl.slot[idx].offset      = offset;
    efl.slot[idx].size        = size;
    efl.nused++;

    if (H5P_poke(plist, H5D_CRT_EXT_FILE_LIST_NAME, &efl) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set external file list");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_external() */

/*-------------------------------------------------------------------------
 * Function:	H5Pget_external_count
 *
 * Purpose:	Returns the number of external files for this dataset.
 *
 * Return:	Success:	Number of external files
 *
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
int
H5Pget_external_count(hid_t plist_id)
{
    H5O_efl_t       efl;
    H5P_genplist_t *plist;     /* Property list pointer */
    int             ret_value; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("Is", "i", plist_id);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_CREATE)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get value */
    if (H5P_peek(plist, H5D_CRT_EXT_FILE_LIST_NAME, &efl) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get external file list");

    /* Set return value */
    ret_value = (int)efl.nused;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_external_count() */

/*-------------------------------------------------------------------------
 * Function:	H5Pget_external
 *
 * Purpose:	Returns information about an external file.  External files
 *		are numbered from zero to N-1 where N is the value returned
 *		by H5Pget_external_count().  At most NAME_SIZE characters are
 *		copied into the NAME array.  If the external file name is
 *		longer than NAME_SIZE with the null terminator, then the
 *		return value is not null terminated (similar to strncpy()).
 *
 *		If NAME_SIZE is zero or NAME is the null pointer then the
 *		external file name is not returned.  If OFFSET or SIZE are
 *		null pointers then the corresponding information is not
 *		returned.
 *
 * See Also:	H5Pset_external()
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_external(hid_t plist_id, unsigned idx, size_t name_size, char *name /*out*/, off_t *offset /*out*/,
                hsize_t *size /*out*/)
{
    H5O_efl_t       efl;
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "iIuzxxx", plist_id, idx, name_size, name, offset, size);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_CREATE)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get value */
    if (H5P_peek(plist, H5D_CRT_EXT_FILE_LIST_NAME, &efl) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get external file list");

    if (idx >= efl.nused)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "external file index is out of range");

    /* Return values */
    if (name_size > 0 && name)
        strncpy(name, efl.slot[idx].name, name_size);
    /* XXX: Badness!
     *
     * The offset parameter is of type off_t and the offset field of H5O_efl_entry_t
     * is HDoff_t which is a different type on Windows (off_t is a 32-bit long,
     * HDoff_t is __int64, a 64-bit type).
     *
     * In a future API reboot, we'll either want to make this parameter a haddr_t
     * or define a 64-bit HDF5-specific offset type that is platform-independent.
     */
    if (offset)
        *offset = (off_t)efl.slot[idx].offset;
    if (size)
        *size = efl.slot[idx].size;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_external() */

/*-------------------------------------------------------------------------
 * Function:	H5Pset_szip
 *
 * Purpose:	Sets the compression method for a permanent or transient
 *		filter pipeline (depending on whether PLIST_ID is a dataset
 *		creation or transfer property list) to H5Z_FILTER_SZIP
 *		Szip is a special compression package that is said to be good
 *              for scientific data.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_szip(hid_t plist_id, unsigned options_mask, unsigned pixels_per_block)
{
    H5O_pline_t     pline;
    H5P_genplist_t *plist;        /* Property list pointer */
    unsigned        cd_values[2]; /* Filter parameters */
    unsigned int    config_flags;
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "iIuIu", plist_id, options_mask, pixels_per_block);

    if (H5Z_get_filter_info(H5Z_FILTER_SZIP, &config_flags) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "can't get filter info");

    if (!(config_flags & H5Z_FILTER_CONFIG_ENCODE_ENABLED))
        HGOTO_ERROR(H5E_PLINE, H5E_NOENCODER, FAIL, "Filter present but encoding is disabled.");

    /* Check arguments */
    if ((pixels_per_block % 2) == 1)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "pixels_per_block is not even");
    if (pixels_per_block > H5_SZIP_MAX_PIXELS_PER_BLOCK)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "pixels_per_block is too large");

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_CREATE)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Always set K13 compression (and un-set CHIP compression) */
    options_mask &= (unsigned)(~H5_SZIP_CHIP_OPTION_MASK);
    options_mask |= H5_SZIP_ALLOW_K13_OPTION_MASK;

    /* Always set "raw" (no szip header) flag for data */
    options_mask |= H5_SZIP_RAW_OPTION_MASK;

    /* Mask off the LSB and MSB options, if they were given */
    /* (The HDF5 library sets them internally, as needed) */
    options_mask &= (unsigned)(~(H5_SZIP_LSB_OPTION_MASK | H5_SZIP_MSB_OPTION_MASK));

    /* Set the parameters for the filter */
    cd_values[0] = options_mask;
    cd_values[1] = pixels_per_block;

    /* Add the filter */
    if (H5P_peek(plist, H5O_CRT_PIPELINE_NAME, &pline) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get pipeline");
    if (H5Z_append(&pline, H5Z_FILTER_SZIP, H5Z_FLAG_OPTIONAL, (size_t)2, cd_values) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTINIT, FAIL, "unable to add szip filter to pipeline");
    if (H5P_poke(plist, H5O_CRT_PIPELINE_NAME, &pline) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTINIT, FAIL, "unable to set pipeline");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_szip() */

/*-------------------------------------------------------------------------
 * Function:	H5Pset_shuffle
 *
 * Purpose:	Sets the shuffling method for a permanent
 *		filter to H5Z_FILTER_SHUFFLE
 *		and bytes of the datatype of the array to be shuffled
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_shuffle(hid_t plist_id)
{
    H5O_pline_t     pline;
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", plist_id);

    /* Check arguments */
    if (true != H5P_isa_class(plist_id, H5P_DATASET_CREATE))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataset creation property list");

    /* Get the plist structure */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object(plist_id)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Add the filter */
    if (H5P_peek(plist, H5O_CRT_PIPELINE_NAME, &pline) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get pipeline");
    if (H5Z_append(&pline, H5Z_FILTER_SHUFFLE, H5Z_FLAG_OPTIONAL, (size_t)0, NULL) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTINIT, FAIL, "unable to shuffle the data");
    if (H5P_poke(plist, H5O_CRT_PIPELINE_NAME, &pline) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTINIT, FAIL, "unable to set pipeline");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_shuffle() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_nbit
 *
 * Purpose:     Sets nbit filter for a dataset creation property list
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_nbit(hid_t plist_id)
{
    H5O_pline_t     pline;
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", plist_id);

    /* Check arguments */
    if (true != H5P_isa_class(plist_id, H5P_DATASET_CREATE))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataset creation property list");

    /* Get the plist structure */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object(plist_id)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Add the nbit filter */
    if (H5P_peek(plist, H5O_CRT_PIPELINE_NAME, &pline) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get pipeline");
    if (H5Z_append(&pline, H5Z_FILTER_NBIT, H5Z_FLAG_OPTIONAL, (size_t)0, NULL) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTINIT, FAIL, "unable to add nbit filter to pipeline");
    if (H5P_poke(plist, H5O_CRT_PIPELINE_NAME, &pline) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTINIT, FAIL, "unable to set pipeline");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_nbit() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_scaleoffset
 *
 * Purpose:     Sets scaleoffset filter for a dataset creation property list
 *              and user-supplied parameters
 *
 * Parameters:  scale_factor:
                              for integer datatype,
                              this parameter will be
                              minimum-bits, if this value is set to 0,
                              scaleoffset filter will calculate the minimum-bits.

                              For floating-point datatype,
                              For variable-minimum-bits method, this will be
                              the decimal precision of the filter,
                              For fixed-minimum-bits method, this will be
                              the minimum-bit of the filter.
                scale_type:   0 for floating-point variable-minimum-bits,
                              1 for floating-point fixed-minimum-bits,
                              other values, for integer datatype

 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_scaleoffset(hid_t plist_id, H5Z_SO_scale_type_t scale_type, int scale_factor)
{
    H5O_pline_t     pline;
    H5P_genplist_t *plist;               /* Property list pointer */
    unsigned        cd_values[2];        /* Filter parameters */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "iZaIs", plist_id, scale_type, scale_factor);

    /* Check arguments */
    if (true != H5P_isa_class(plist_id, H5P_DATASET_CREATE))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataset creation property list");

    if (scale_factor < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "scale factor must be >= 0");
    if (scale_type != H5Z_SO_FLOAT_DSCALE && scale_type != H5Z_SO_FLOAT_ESCALE && scale_type != H5Z_SO_INT)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid scale type");

    /* Get the plist structure */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object(plist_id)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set parameters for the filter
     * scale_type = 0:     floating-point type, filter uses variable-minimum-bits method,
     *                     scale_factor is decimal scale factor
     * scale_type = 1:     floating-point type, filter uses fixed-minimum-bits method,
     *                     scale_factor is the fixed minimum number of bits
     * scale type = other: integer type, scale_factor is minimum number of bits
     *                     if scale_factor = 0, then filter calculates minimum number of bits
     */
    cd_values[0] = scale_type;
    cd_values[1] = (unsigned)scale_factor;

    /* Add the scaleoffset filter */
    if (H5P_peek(plist, H5O_CRT_PIPELINE_NAME, &pline) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get pipeline");
    if (H5Z_append(&pline, H5Z_FILTER_SCALEOFFSET, H5Z_FLAG_OPTIONAL, (size_t)2, cd_values) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTINIT, FAIL, "unable to add scaleoffset filter to pipeline");
    if (H5P_poke(plist, H5O_CRT_PIPELINE_NAME, &pline) < 0)
        HGOTO_ERROR(H5E_PLINE, H5E_CANTINIT, FAIL, "unable to set pipeline");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_scaleoffset() */

/*-------------------------------------------------------------------------
 * Function:	H5Pset_fill_value
 *
 * Purpose:	Set the fill value for a dataset creation property list. The
 *		VALUE is interpreted as being of type TYPE, which need not
 *		be the same type as the dataset but the library must be able
 *		to convert VALUE to the dataset type when the dataset is
 *		created.  If VALUE is NULL, it will be interpreted as
 *		undefining fill value.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_fill_value(hid_t plist_id, hid_t type_id, const void *value)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    H5O_fill_t      fill;                /* Fill value to modify */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ii*x", plist_id, type_id, value);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_CREATE)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get the current fill value */
    if (H5P_peek(plist, H5D_CRT_FILL_VALUE_NAME, &fill) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get fill value");

    /* Release the dynamic fill value components */
    H5O_fill_reset_dyn(&fill);

    if (value) {
        H5T_t      *type;  /* Datatype for fill value */
        H5T_path_t *tpath; /* Conversion information */

        /* Retrieve pointer to datatype */
        if (NULL == (type = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");

        /* Set the fill value */
        if (NULL == (fill.type = H5T_copy(type, H5T_COPY_TRANSIENT)))
            HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't copy datatype");
        fill.size = (ssize_t)H5T_get_size(type);
        if (NULL == (fill.buf = H5MM_malloc((size_t)fill.size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTINIT, FAIL, "memory allocation failed for fill value");
        H5MM_memcpy(fill.buf, value, (size_t)fill.size);

        /* Set up type conversion function */
        if (NULL == (tpath = H5T_path_find(type, type)))
            HGOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL,
                        "unable to convert between src and dest data types");

        /* If necessary, convert fill value datatypes (which copies VL components, etc.) */
        if (!H5T_path_noop(tpath)) {
            uint8_t *bkg_buf = NULL; /* Background conversion buffer */

            /* Allocate a background buffer */
            if (H5T_path_bkg(tpath) && NULL == (bkg_buf = H5FL_BLK_CALLOC(type_conv, (size_t)fill.size)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");

            /* Convert the fill value */
            if (H5T_convert(tpath, type_id, type_id, (size_t)1, (size_t)0, (size_t)0, fill.buf, bkg_buf) <
                0) {
                if (bkg_buf)
                    bkg_buf = H5FL_BLK_FREE(type_conv, bkg_buf);
                HGOTO_ERROR(H5E_DATASET, H5E_CANTCONVERT, FAIL, "datatype conversion failed");
            } /* end if */

            /* Release the background buffer */
            if (bkg_buf)
                bkg_buf = H5FL_BLK_FREE(type_conv, bkg_buf);
        } /* end if */
    }     /* end if */
    else
        fill.size = (-1);

    /* Update fill value in property list */
    if (H5P_poke(plist, H5D_CRT_FILL_VALUE_NAME, &fill) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't set fill value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_fill_value() */

/*-------------------------------------------------------------------------
 * Function:	H5P_get_fill_value
 *
 * Purpose:	Queries the fill value property of a dataset creation
 *		property list.  The fill value is returned through the VALUE
 *		pointer and the memory is allocated by the caller.  The fill
 *		value will be converted from its current datatype to the
 *		specified TYPE.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5P_get_fill_value(H5P_genplist_t *plist, const H5T_t *type, void *value /*out*/)
{
    H5O_fill_t  fill;                /* Fill value to retrieve */
    H5T_path_t *tpath;               /*type conversion info	*/
    void       *buf       = NULL;    /*conversion buffer	*/
    void       *bkg       = NULL;    /*conversion buffer	*/
    hid_t       src_id    = -1;      /*source datatype id	*/
    hid_t       dst_id    = -1;      /*destination datatype id	*/
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /*
     * If no fill value is defined then return an error.  We can't even
     * return zero because we don't know the datatype of the dataset and
     * datatype conversion might not have resulted in zero.  If fill value
     * is undefined, also return error.
     */
    if (H5P_peek(plist, H5D_CRT_FILL_VALUE_NAME, &fill) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get fill value");
    if (fill.size == -1)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "fill value is undefined");

    /* Check for "default" fill value */
    if (fill.size == 0) {
        memset(value, 0, H5T_get_size(type));
        HGOTO_DONE(SUCCEED);
    } /* end if */

    /*
     * Can we convert between the source and destination datatypes?
     */
    if (NULL == (tpath = H5T_path_find(fill.type, type)))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINIT, FAIL, "unable to convert between src and dst datatypes");
    if ((src_id = H5I_register(H5I_DATATYPE, H5T_copy(fill.type, H5T_COPY_TRANSIENT), false)) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINIT, FAIL, "unable to copy/register datatype");

    /*
     * Data type conversions are always done in place, so we need a buffer
     * other than the fill value buffer that is large enough for both source
     * and destination.  The app-supplied buffer might do okay.
     */
    if (H5T_get_size(type) >= H5T_get_size(fill.type)) {
        buf = value;
        if (H5T_path_bkg(tpath) && NULL == (bkg = H5MM_calloc(H5T_get_size(type))))
            HGOTO_ERROR(H5E_PLIST, H5E_CANTALLOC, FAIL, "memory allocation failed for type conversion");
    } /* end if */
    else {
        if (NULL == (buf = H5MM_calloc(H5T_get_size(fill.type))))
            HGOTO_ERROR(H5E_PLIST, H5E_CANTALLOC, FAIL, "memory allocation failed for type conversion");
        if (H5T_path_bkg(tpath) && NULL == (bkg = H5MM_calloc(H5T_get_size(fill.type))))
            HGOTO_ERROR(H5E_PLIST, H5E_CANTALLOC, FAIL, "memory allocation failed for type conversion");
    } /* end else */
    H5MM_memcpy(buf, fill.buf, H5T_get_size(fill.type));

    /* Do the conversion */
    if ((dst_id = H5I_register(H5I_DATATYPE, H5T_copy(type, H5T_COPY_ALL), false)) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINIT, FAIL, "unable to copy/register datatype");
    if (H5T_convert(tpath, src_id, dst_id, (size_t)1, (size_t)0, (size_t)0, buf, bkg) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINIT, FAIL, "datatype conversion failed");
    if (buf != value)
        H5MM_memcpy(value, buf, H5T_get_size(type));

done:
    if (buf != value)
        H5MM_xfree(buf);
    if (bkg != value)
        H5MM_xfree(bkg);
    if (src_id >= 0 && H5I_dec_ref(src_id) < 0)
        HDONE_ERROR(H5E_PLIST, H5E_CANTDEC, FAIL, "can't decrement ref count of temp ID");
    if (dst_id >= 0 && H5I_dec_ref(dst_id) < 0)
        HDONE_ERROR(H5E_PLIST, H5E_CANTDEC, FAIL, "can't decrement ref count of temp ID");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P_get_fill_value() */

/*-------------------------------------------------------------------------
 * Function:	H5Pget_fill_value
 *
 * Purpose:	Queries the fill value property of a dataset creation
 *		property list.  The fill value is returned through the VALUE
 *		pointer and the memory is allocated by the caller.  The fill
 *		value will be converted from its current datatype to the
 *		specified TYPE.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_fill_value(hid_t plist_id, hid_t type_id, void *value /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    H5T_t          *type;                /* Datatype		*/
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "iix", plist_id, type_id, value);

    /* Check arguments */
    if (NULL == (type = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    if (!value)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no fill value output buffer");

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_CREATE)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get the fill value */
    if (H5P_get_fill_value(plist, type, value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get fill value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_fill_value() */

/*-------------------------------------------------------------------------
 * Function:    H5P_is_fill_value_defined
 *
 * Purpose:	Check if fill value is defined.  Internal version of function
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5P_is_fill_value_defined(const H5O_fill_t *fill, H5D_fill_value_t *status)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    assert(fill);
    assert(status);

    /* Check if the fill value was "unset" */
    if (fill->size == -1 && !fill->buf)
        *status = H5D_FILL_VALUE_UNDEFINED;
    /* Check if the fill value was set to the default fill value by the library */
    else if (fill->size == 0 && !fill->buf)
        *status = H5D_FILL_VALUE_DEFAULT;
    /* Check if the fill value was set by the application */
    else if (fill->size > 0 && fill->buf)
        *status = H5D_FILL_VALUE_USER_DEFINED;
    else {
        *status = H5D_FILL_VALUE_ERROR;
        HGOTO_ERROR(H5E_PLIST, H5E_BADRANGE, FAIL, "invalid combination of fill-value info");
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P_is_fill_value_defined() */

/*-------------------------------------------------------------------------
 * Function:    H5P_fill_value_defined
 *
 * Purpose:	Check if fill value is defined.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5P_fill_value_defined(H5P_genplist_t *plist, H5D_fill_value_t *status)
{
    H5O_fill_t fill; /* Fill value to query */
    herr_t     ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    assert(status);

    /* Get the fill value struct */
    if (H5P_peek(plist, H5D_CRT_FILL_VALUE_NAME, &fill) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get fill value");

    /* Get the fill-value status */
    if (H5P_is_fill_value_defined(&fill, status) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't check fill value status");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P_fill_value_defined() */

/*-------------------------------------------------------------------------
 * Function:    H5Pfill_value_defined
 *
 * Purpose:	Check if fill value is defined.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pfill_value_defined(hid_t plist_id, H5D_fill_value_t *status)
{
    H5P_genplist_t *plist; /* Property list to query */
    herr_t          ret_value = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "i*DF", plist_id, status);

    assert(status);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_CREATE)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get the fill-value status */
    if (H5P_fill_value_defined(plist, status) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't check fill value status");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pfill_value_defined() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_alloc_time
 *
 * Purpose:     Set space allocation time for dataset during creation.
 *		Valid values are H5D_ALLOC_TIME_DEFAULT, H5D_ALLOC_TIME_EARLY,
 *			H5D_ALLOC_TIME_LATE, H5D_ALLOC_TIME_INCR
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_alloc_time(hid_t plist_id, H5D_alloc_time_t alloc_time)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    H5O_fill_t      fill;                /* Fill value property to modify */
    unsigned        alloc_time_state;    /* State of allocation time property */
    herr_t          ret_value = SUCCEED; /* return value 	 */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iDa", plist_id, alloc_time);

    /* Check arguments */
    if (alloc_time < H5D_ALLOC_TIME_DEFAULT || alloc_time > H5D_ALLOC_TIME_INCR)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid allocation time setting");

    /* Get the property list structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_CREATE)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Check for resetting to default for layout type */
    if (alloc_time == H5D_ALLOC_TIME_DEFAULT) {
        H5O_layout_t layout; /* Type of storage layout */

        /* Peek at the storage layout */
        if (H5P_peek(plist, H5D_CRT_LAYOUT_NAME, &layout) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get layout");

        /* Set the default based on layout */
        switch (layout.type) {
            case H5D_COMPACT:
                alloc_time = H5D_ALLOC_TIME_EARLY;
                break;

            case H5D_CONTIGUOUS:
                alloc_time = H5D_ALLOC_TIME_LATE;
                break;

            case H5D_CHUNKED:
                alloc_time = H5D_ALLOC_TIME_INCR;
                break;

            case H5D_VIRTUAL:
                alloc_time = H5D_ALLOC_TIME_INCR;
                break;

            case H5D_LAYOUT_ERROR:
            case H5D_NLAYOUTS:
            default:
                HGOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL, "unknown layout type");
        } /* end switch */

        /* Reset the "state" of the allocation time property back to the "default" */
        alloc_time_state = 1;
    } /* end if */
    else
        /* Set the "state" of the allocation time property to indicate the user modified it */
        alloc_time_state = 0;

    /* Retrieve previous fill value settings */
    if (H5P_peek(plist, H5D_CRT_FILL_VALUE_NAME, &fill) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get fill value");

    /* Update property value */
    fill.alloc_time = alloc_time;

    /* Set values */
    if (H5P_poke(plist, H5D_CRT_FILL_VALUE_NAME, &fill) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set fill value");
    if (H5P_set(plist, H5D_CRT_ALLOC_TIME_STATE_NAME, &alloc_time_state) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set space allocation time");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Pset_alloc_time() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_alloc_time
 *
 * Purpose:     Get space allocation time for dataset creation.
 *		Valid values are H5D_ALLOC_TIME_DEFAULT, H5D_ALLOC_TIME_EARLY,
 *			H5D_ALLOC_TIME_LATE, H5D_ALLOC_TIME_INCR
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_alloc_time(hid_t plist_id, H5D_alloc_time_t *alloc_time /*out*/)
{
    herr_t ret_value = SUCCEED; /* return value          */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", plist_id, alloc_time);

    /* Get values */
    if (alloc_time) {
        H5P_genplist_t *plist; /* Property list pointer */
        H5O_fill_t      fill;  /* Fill value property to query */

        /* Get the property list structure */
        if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_CREATE)))
            HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

        /* Retrieve fill value settings */
        if (H5P_peek(plist, H5D_CRT_FILL_VALUE_NAME, &fill) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get fill value");

        /* Set user's value */
        *alloc_time = fill.alloc_time;
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_alloc_time() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_fill_time
 *
 * Purpose:	Set fill value writing time for dataset.  Valid values are
 *		H5D_FILL_TIME_ALLOC and H5D_FILL_TIME_NEVER.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_fill_time(hid_t plist_id, H5D_fill_time_t fill_time)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    H5O_fill_t      fill;                /* Fill value property to modify */
    herr_t          ret_value = SUCCEED; /* return value          */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iDf", plist_id, fill_time);

    /* Check arguments */
    if (fill_time < H5D_FILL_TIME_ALLOC || fill_time > H5D_FILL_TIME_IFSET)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid fill time setting");

    /* Get the property list structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_CREATE)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Retrieve previous fill value settings */
    if (H5P_peek(plist, H5D_CRT_FILL_VALUE_NAME, &fill) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get fill value");

    /* Update property value */
    fill.fill_time = fill_time;

    /* Set values */
    if (H5P_poke(plist, H5D_CRT_FILL_VALUE_NAME, &fill) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set fill value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_fill_time() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_fill_time
 *
 * Purpose:	Get fill value writing time.  Valid values are H5D_NEVER
 *		and H5D_ALLOC.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_fill_time(hid_t plist_id, H5D_fill_time_t *fill_time /*out*/)
{
    herr_t ret_value = SUCCEED; /* return value          */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", plist_id, fill_time);

    /* Set values */
    if (fill_time) {
        H5P_genplist_t *plist; /* Property list pointer */
        H5O_fill_t      fill;  /* Fill value property to query */

        /* Get the property list structure */
        if (NULL == (plist = H5P_object_verify(plist_id, H5P_DATASET_CREATE)))
            HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

        /* Retrieve fill value settings */
        if (H5P_peek(plist, H5D_CRT_FILL_VALUE_NAME, &fill) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get fill value");

        /* Set user's value */
        *fill_time = fill.fill_time;
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_fill_time() */

/*-----------------------------------------------------------------------------
 * Function: H5Pget_dset_no_attrs_hint
 *
 * Purpose:
 *
 *     Access the flag for whether or not datasets created by the given dcpl
 *     will be created with a "minimized" object header.
 *
 * Return:
 *
 *     Failure: Negative value (FAIL)
 *     Success: Non-negative value (SUCCEED)
 *
 *-----------------------------------------------------------------------------
 */
herr_t
H5Pget_dset_no_attrs_hint(hid_t dcpl_id, hbool_t *minimize /*out*/)
{
    bool            setting   = false;
    H5P_genplist_t *plist     = NULL;
    herr_t          ret_value = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", dcpl_id, minimize);

    if (NULL == minimize)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "receiving pointer cannot be NULL");

    plist = H5P_object_verify(dcpl_id, H5P_DATASET_CREATE);
    if (NULL == plist)
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    if (H5P_peek(plist, H5D_CRT_MIN_DSET_HDR_SIZE_NAME, &setting) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get dset oh minimize flag value");

    *minimize = setting;

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Pget_dset_no_attrs_hint() */

/*-----------------------------------------------------------------------------
 * Function: H5Pset_dset_no_attrs_hint
 *
 * Purpose:
 *
 *     Set the dcpl to minimize (or explicitly to not minimized) dataset object
 *     headers upon creation.
 *
 * Return:
 *
 *     Failure: Negative value (FAIL)
 *     Success: Non-negative value (SUCCEED)
 *
 *-----------------------------------------------------------------------------
 */
herr_t
H5Pset_dset_no_attrs_hint(hid_t dcpl_id, hbool_t minimize)
{
    H5P_genplist_t *plist     = NULL;
    bool            prev_set  = false;
    herr_t          ret_value = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ib", dcpl_id, minimize);

    plist = H5P_object_verify(dcpl_id, H5P_DATASET_CREATE);
    if (NULL == plist)
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    if (H5P_peek(plist, H5D_CRT_MIN_DSET_HDR_SIZE_NAME, &prev_set) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get extant dset oh minimize flag value");

    if (H5P_poke(plist, H5D_CRT_MIN_DSET_HDR_SIZE_NAME, &minimize) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't get dset oh minimize flag value");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Pset_dset_no_attrs_hint() */
