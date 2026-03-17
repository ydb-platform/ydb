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
 * Created:     H5Ocopy_ref.c
 *
 * Purpose:     Object with references copying routines
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Omodule.h" /* This source code file is part of the H5O module */
#define H5R_FRIEND     /* Suppress error about including H5Rpkg   */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5Fprivate.h"  /* File                                     */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5Lprivate.h"  /* Links                                    */
#include "H5MMprivate.h" /* Memory management                        */
#include "H5Opkg.h"      /* Object headers                           */
#include "H5Rpkg.h"      /* References                               */

#include "H5VLnative_private.h" /* Native VOL connector                     */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

static herr_t H5O__copy_obj_by_ref(H5O_loc_t *src_oloc, H5O_loc_t *dst_oloc, H5G_loc_t *dst_root_loc,
                                   H5O_copy_t *cpy_info);
static herr_t H5O__copy_expand_ref_object1(H5O_loc_t *src_oloc, const void *buf_src, H5O_loc_t *dst_oloc,
                                           H5G_loc_t *dst_root_loc, void *buf_dst, size_t ref_count,
                                           H5O_copy_t *cpy_info);
static herr_t H5O__copy_expand_ref_region1(H5O_loc_t *src_oloc, const void *buf_src, H5O_loc_t *dst_oloc,
                                           H5G_loc_t *dst_root_loc, void *buf_dst, size_t ref_count,
                                           H5O_copy_t *cpy_info);
static herr_t H5O__copy_expand_ref_object2(H5O_loc_t *src_oloc, hid_t tid_src, const H5T_t *dt_src,
                                           const void *buf_src, size_t nbytes_src, H5O_loc_t *dst_oloc,
                                           H5G_loc_t *dst_root_loc, void *buf_dst, size_t ref_count,
                                           H5O_copy_t *cpy_info);

/*********************/
/* Package Variables */
/*********************/

/* Declare extern the free list to manage blocks of type conversion data */
H5FL_BLK_EXTERN(type_conv);

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:    H5O__copy_obj_by_ref
 *
 * Purpose:     Copy the object pointed to by src_oloc.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__copy_obj_by_ref(H5O_loc_t *src_oloc, H5O_loc_t *dst_oloc, H5G_loc_t *dst_root_loc, H5O_copy_t *cpy_info)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(src_oloc);
    assert(dst_oloc);

    /* Perform the copy, or look up existing copy */
    if ((ret_value = H5O_copy_header_map(src_oloc, dst_oloc, cpy_info, false, NULL, NULL)) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTCOPY, FAIL, "unable to copy object");

    /* Check if a new valid object is copied to the destination */
    if (H5_addr_defined(dst_oloc->addr) && (ret_value > SUCCEED)) {
        char       tmp_obj_name[80];
        H5G_name_t new_path;
        H5O_loc_t  new_oloc;
        H5G_loc_t  new_loc;

        /* Set up group location for new object */
        new_loc.oloc = &new_oloc;
        new_loc.path = &new_path;
        H5G_loc_reset(&new_loc);
        new_oloc.file = dst_oloc->file;
        new_oloc.addr = dst_oloc->addr;

        /* Pick a default name for the new object */
        snprintf(tmp_obj_name, sizeof(tmp_obj_name), "~obj_pointed_by_%llu",
                 (unsigned long long)dst_oloc->addr);

        /* Create a link to the newly copied object */
        /* Note: since H5O_copy_header_map actually copied the target object, it
         * must exist either in cache or on disk, therefore it is safe to not
         * pass the obj_type and udata fields returned by H5O_copy_header_map.
         * This could be changed in the future to slightly improve performance
         * --NAF */
        if (H5L_link(dst_root_loc, tmp_obj_name, &new_loc, cpy_info->lcpl_id) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to insert link");

        H5G_loc_free(&new_loc);
    } /* if (H5_addr_defined(dst_oloc.addr)) */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__copy_obj_by_ref() */

/*-------------------------------------------------------------------------
 * Function:    H5O__copy_expand_ref_object1
 *
 * Purpose: Copy the object pointed by a deprecated object reference.
 *
 * Return:  Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__copy_expand_ref_object1(H5O_loc_t *src_oloc, const void *buf_src, H5O_loc_t *dst_oloc,
                             H5G_loc_t *dst_root_loc, void *buf_dst, size_t ref_count, H5O_copy_t *cpy_info)
{
    const hobj_ref_t   *src_ref                     = (const hobj_ref_t *)buf_src;
    hobj_ref_t         *dst_ref                     = (hobj_ref_t *)buf_dst;
    const unsigned char zeros[H5R_OBJ_REF_BUF_SIZE] = {0};
    size_t              buf_size                    = H5R_OBJ_REF_BUF_SIZE;
    size_t              i; /* Local index variable */
    size_t              token_size = H5F_SIZEOF_ADDR(src_oloc->file);
    herr_t              ret_value  = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Making equivalent references in the destination file */
    for (i = 0; i < ref_count; i++) {
        const unsigned char *src_buf   = (const unsigned char *)&src_ref[i];
        unsigned char       *dst_buf   = (unsigned char *)&dst_ref[i];
        H5O_token_t          tmp_token = {0};

        /* If data is not initialized, copy zeros and skip */
        if (0 == memcmp(src_buf, zeros, buf_size))
            memset(dst_buf, 0, buf_size);
        else {
            /* Set up for the object copy for the reference */
            if (H5R__decode_token_obj_compat(src_buf, &buf_size, &tmp_token, token_size) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTDECODE, FAIL, "unable to decode src object address");
            if (H5VL_native_token_to_addr(src_oloc->file, H5I_FILE, tmp_token, &src_oloc->addr) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTUNSERIALIZE, FAIL,
                            "can't deserialize object token into address");

            if (!H5_addr_defined(src_oloc->addr) || src_oloc->addr == 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "undefined reference pointer");
            dst_oloc->addr = HADDR_UNDEF;

            /* Attempt to copy object from source to destination file */
            if (H5O__copy_obj_by_ref(src_oloc, dst_oloc, dst_root_loc, cpy_info) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTCOPY, FAIL, "unable to copy object");

            /* Set the object reference info for the destination file */
            if (H5VL_native_addr_to_token(dst_oloc->file, H5I_FILE, dst_oloc->addr, &tmp_token) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTSERIALIZE, FAIL, "can't serialize address into object token");
            if (H5R__encode_token_obj_compat((const H5O_token_t *)&tmp_token, token_size, dst_buf,
                                             &buf_size) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTDECODE, FAIL, "unable to encode dst object address");
        } /* end else */
    }     /* end for */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__copy_expand_ref_object1() */

/*-------------------------------------------------------------------------
 * Function:    H5O__copy_expand_ref_region1
 *
 * Purpose: Copy the object pointed by a deprecated region reference.
 *
 * Return:  Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__copy_expand_ref_region1(H5O_loc_t *src_oloc, const void *buf_src, H5O_loc_t *dst_oloc,
                             H5G_loc_t *dst_root_loc, void *buf_dst, size_t ref_count, H5O_copy_t *cpy_info)
{
    const hdset_reg_ref_t *src_ref                          = (const hdset_reg_ref_t *)buf_src;
    hdset_reg_ref_t       *dst_ref                          = (hdset_reg_ref_t *)buf_dst;
    const unsigned char    zeros[H5R_DSET_REG_REF_BUF_SIZE] = {0};
    size_t                 buf_size                         = H5R_DSET_REG_REF_BUF_SIZE;
    size_t                 i; /* Local index variable */
    herr_t                 ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Making equivalent references in the destination file */
    for (i = 0; i < ref_count; i++) {
        const unsigned char *src_buf = (const unsigned char *)&src_ref[i];
        unsigned char       *dst_buf = (unsigned char *)&dst_ref[i];
        unsigned char       *data    = NULL;
        size_t               data_size;
        const uint8_t       *p;
        uint8_t             *q;

        /* If data is not initialized, copy zeros and skip */
        if (0 == memcmp(src_buf, zeros, buf_size))
            memset(dst_buf, 0, buf_size);
        else {
            /* Read from heap */
            if (H5R__decode_heap(src_oloc->file, src_buf, &buf_size, &data, &data_size) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTDECODE, FAIL, "unable to decode dataset region information");

            /* Get object address */
            p = (const uint8_t *)data;
            H5F_addr_decode(src_oloc->file, &p, &src_oloc->addr);
            if (!H5_addr_defined(src_oloc->addr) || src_oloc->addr == 0) {
                H5MM_free(data);
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "undefined reference pointer");
            }
            dst_oloc->addr = HADDR_UNDEF;

            /* Attempt to copy object from source to destination file */
            if (H5O__copy_obj_by_ref(src_oloc, dst_oloc, dst_root_loc, cpy_info) < 0) {
                H5MM_free(data);
                HGOTO_ERROR(H5E_OHDR, H5E_CANTCOPY, FAIL, "unable to copy object");
            } /* end if */

            /* Serialize object addr */
            q = (uint8_t *)data;
            H5F_addr_encode(dst_oloc->file, &q, dst_oloc->addr);

            /* Write to heap */
            if (H5R__encode_heap(dst_oloc->file, dst_buf, &buf_size, data, (size_t)data_size) < 0) {
                H5MM_free(data);
                HGOTO_ERROR(H5E_OHDR, H5E_CANTENCODE, FAIL, "unable to encode dataset region information");
            }

            /* Free the buffer allocated in H5R__decode_heap() */
            H5MM_free(data);
        } /* end else */
    }     /* end for */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__copy_expand_ref_region1() */

/*-------------------------------------------------------------------------
 * Function:    H5O__copy_expand_ref_object2
 *
 * Purpose: Copy the object pointed by a reference (object, region, attribute).
 *
 * Return:  Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__copy_expand_ref_object2(H5O_loc_t *src_oloc, hid_t tid_src, const H5T_t *dt_src, const void *buf_src,
                             size_t nbytes_src, H5O_loc_t *dst_oloc, H5G_loc_t *dst_root_loc, void *buf_dst,
                             size_t ref_count, H5O_copy_t *cpy_info)
{
    H5T_t              *dt_mem        = NULL;                        /* Memory datatype */
    H5T_t              *dt_dst        = NULL;                        /* Destination datatype */
    hid_t               tid_mem       = H5I_INVALID_HID;             /* Datatype ID for memory datatype */
    hid_t               tid_dst       = H5I_INVALID_HID;             /* Datatype ID for memory datatype */
    H5T_path_t         *tpath_src_mem = NULL, *tpath_mem_dst = NULL; /* Datatype conversion paths */
    size_t              i;                                           /* Local index variable */
    bool                reg_tid_src             = (tid_src == H5I_INVALID_HID);
    hid_t               dst_loc_id              = H5I_INVALID_HID;
    void               *conv_buf                = NULL;        /* Buffer for converting data */
    size_t              conv_buf_size           = 0;           /* Buffer size */
    void               *reclaim_buf             = NULL;        /* Buffer for reclaiming data */
    H5S_t              *buf_space               = NULL;        /* Dataspace describing buffer */
    hsize_t             buf_dim[1]              = {ref_count}; /* Dimension for buffer */
    size_t              token_size              = H5F_SIZEOF_ADDR(src_oloc->file);
    const unsigned char zeros[H5R_REF_BUF_SIZE] = {0};
    herr_t              ret_value               = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Create datatype ID for src datatype. */
    if ((tid_src == H5I_INVALID_HID) && (tid_src = H5I_register(H5I_DATATYPE, dt_src, false)) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTREGISTER, FAIL, "unable to register source file datatype");

    /* create a memory copy of the reference datatype */
    if (NULL == (dt_mem = H5T_copy(dt_src, H5T_COPY_TRANSIENT)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, FAIL, "unable to copy");
    if ((tid_mem = H5I_register(H5I_DATATYPE, dt_mem, false)) < 0) {
        (void)H5T_close_real(dt_mem);
        HGOTO_ERROR(H5E_OHDR, H5E_CANTREGISTER, FAIL, "unable to register memory datatype");
    } /* end if */

    /* create reference datatype at the destination file */
    if (NULL == (dt_dst = H5T_copy(dt_src, H5T_COPY_TRANSIENT)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, FAIL, "unable to copy");
    if (H5T_set_loc(dt_dst, H5F_VOL_OBJ(dst_oloc->file), H5T_LOC_DISK) < 0) {
        (void)H5T_close_real(dt_dst);
        HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, FAIL, "cannot mark datatype on disk");
    } /* end if */
    if ((tid_dst = H5I_register(H5I_DATATYPE, dt_dst, false)) < 0) {
        (void)H5T_close_real(dt_dst);
        HGOTO_ERROR(H5E_OHDR, H5E_CANTREGISTER, FAIL, "unable to register destination file datatype");
    } /* end if */

    /* Set up the conversion functions */
    if (NULL == (tpath_src_mem = H5T_path_find(dt_src, dt_mem)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, FAIL, "unable to convert between src and mem datatypes");
    if (NULL == (tpath_mem_dst = H5T_path_find(dt_mem, dt_dst)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, FAIL, "unable to convert between mem and dst datatypes");

    /* Use extra conversion buffer (TODO we should avoid using an extra buffer once the H5Ocopy code has been
     * reworked) */
    conv_buf_size = MAX(H5T_get_size(dt_src), H5T_get_size(dt_mem)) * ref_count;
    if (NULL == (conv_buf = H5FL_BLK_MALLOC(type_conv, conv_buf_size)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed for copy buffer");
    H5MM_memcpy(conv_buf, buf_src, nbytes_src);

    /* Convert from source file to memory */
    if (H5T_convert(tpath_src_mem, tid_src, tid_mem, ref_count, (size_t)0, (size_t)0, conv_buf, NULL) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTCONVERT, FAIL, "datatype conversion failed");

    /* Retrieve loc ID */
    if ((dst_loc_id = H5F_get_id(dst_oloc->file)) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file or file object");

    /* Making equivalent references in the destination file */
    for (i = 0; i < ref_count; i++) {
        H5R_ref_t      *ref_ptr = (H5R_ref_t *)conv_buf;
        H5R_ref_priv_t *ref     = (H5R_ref_priv_t *)&ref_ptr[i];

        /* Check for null reference - only expand reference if it is not null */
        if (memcmp(ref, zeros, H5R_REF_BUF_SIZE)) {
            H5O_token_t tmp_token = {0};

            /* Get src object address */
            if (H5R__get_obj_token(ref, &tmp_token, &token_size) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "unable to get object token");
            if (H5VL_native_token_to_addr(src_oloc->file, H5I_FILE, tmp_token, &src_oloc->addr) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTUNSERIALIZE, FAIL,
                            "can't deserialize object token into address");

            /* Attempt to copy object from source to destination file */
            if (H5O__copy_obj_by_ref(src_oloc, dst_oloc, dst_root_loc, cpy_info) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTCOPY, FAIL, "unable to copy object");

            /* Set dst object address */
            if (H5VL_native_addr_to_token(dst_oloc->file, H5I_FILE, dst_oloc->addr, &tmp_token) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTSERIALIZE, FAIL, "can't serialize address into object token");
            if (H5R__set_obj_token(ref, (const H5O_token_t *)&tmp_token, token_size) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "unable to set object token");
            /* Do not set app_ref since references are released once the copy is done */
            if (H5R__set_loc_id(ref, dst_loc_id, true, false) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "unable to set destination loc id");
        } /* end if */
    }     /* end for */

    /* Copy into another buffer, to reclaim memory later */
    if (NULL == (reclaim_buf = H5FL_BLK_MALLOC(type_conv, conv_buf_size)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed for copy buffer");
    H5MM_memcpy(reclaim_buf, conv_buf, conv_buf_size);
    if (NULL == (buf_space = H5S_create_simple((unsigned)1, buf_dim, NULL)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTCREATE, FAIL, "can't create simple dataspace");

    /* Convert from memory to destination file */
    if (H5T_convert(tpath_mem_dst, tid_mem, tid_dst, ref_count, (size_t)0, (size_t)0, conv_buf, NULL) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTCONVERT, FAIL, "datatype conversion failed");
    H5MM_memcpy(buf_dst, conv_buf, nbytes_src);

    /* Reclaim space from reference data */
    if (H5T_reclaim(tid_mem, buf_space, reclaim_buf) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_BADITER, FAIL, "unable to reclaim reference data");

done:
    if (buf_space && (H5S_close(buf_space) < 0))
        HDONE_ERROR(H5E_OHDR, H5E_CANTFREE, FAIL, "Can't close dataspace");
    /* Don't decrement ID, we want to keep underlying datatype */
    if (reg_tid_src && (tid_src > 0) && (NULL == H5I_remove(tid_src)))
        HDONE_ERROR(H5E_OHDR, H5E_CANTFREE, FAIL, "Can't decrement temporary datatype ID");
    if ((tid_mem > 0) && H5I_dec_ref(tid_mem) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTFREE, FAIL, "Can't decrement temporary datatype ID");
    if ((tid_dst > 0) && H5I_dec_ref(tid_dst) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTFREE, FAIL, "Can't decrement temporary datatype ID");
    if (reclaim_buf)
        reclaim_buf = H5FL_BLK_FREE(type_conv, reclaim_buf);
    if (conv_buf)
        conv_buf = H5FL_BLK_FREE(type_conv, conv_buf);
    if ((dst_loc_id != H5I_INVALID_HID) && (H5I_dec_ref(dst_loc_id) < 0))
        HDONE_ERROR(H5E_OHDR, H5E_CANTDEC, FAIL, "unable to decrement refcount on location id");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__copy_expand_ref_object2() */

/*-------------------------------------------------------------------------
 * Function:	H5O_copy_expand_ref
 *
 * Purpose:	Copy the object pointed by a reference.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_copy_expand_ref(H5F_t *file_src, hid_t tid_src, const H5T_t *dt_src, void *buf_src, size_t nbytes_src,
                    H5F_t *file_dst, void *buf_dst, H5O_copy_t *cpy_info)
{
    H5O_loc_t dst_oloc;     /* Copied object object location */
    H5O_loc_t src_oloc;     /* Temporary object location for source object */
    H5G_loc_t dst_root_loc; /* The location of root group of the destination file */
    size_t    ref_count;
    herr_t    ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(file_src);
    assert(buf_src);
    assert(file_dst);
    assert(buf_dst);
    assert(nbytes_src);
    assert(cpy_info);

    /* Initialize object locations */
    H5O_loc_reset(&src_oloc);
    H5O_loc_reset(&dst_oloc);
    src_oloc.file = file_src;
    dst_oloc.file = file_dst;

    /* Set up the root group in the destination file */
    if (NULL == (dst_root_loc.oloc = H5G_oloc(H5G_rootof(file_dst))))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to get object location for root group");
    if (NULL == (dst_root_loc.path = H5G_nameof(H5G_rootof(file_dst))))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to get path for root group");

    /* Determine # of reference elements to copy */
    ref_count = nbytes_src / H5T_get_size(dt_src);

    /* Copy object references */
    switch (H5T_get_ref_type(dt_src)) {
        case H5R_OBJECT1:
            if (H5O__copy_expand_ref_object1(&src_oloc, buf_src, &dst_oloc, &dst_root_loc, buf_dst, ref_count,
                                             cpy_info) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_BADVALUE, FAIL, "unable to expand H5R_OBJECT1 reference");
            break;
        case H5R_DATASET_REGION1:
            if (H5O__copy_expand_ref_region1(&src_oloc, buf_src, &dst_oloc, &dst_root_loc, buf_dst, ref_count,
                                             cpy_info) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_BADVALUE, FAIL, "unable to expand H5R_DATASET_REGION1 reference");
            break;
        case H5R_DATASET_REGION2:
        case H5R_ATTR:
        case H5R_OBJECT2:
            if (H5O__copy_expand_ref_object2(&src_oloc, tid_src, dt_src, buf_src, nbytes_src, &dst_oloc,
                                             &dst_root_loc, buf_dst, ref_count, cpy_info) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_BADVALUE, FAIL, "unable to expand reference");
            break;
        case H5R_BADTYPE:
        case H5R_MAXTYPE:
        default:
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid reference type");
            break;
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_copy_expand_ref() */
