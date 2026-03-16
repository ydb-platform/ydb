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
 * Module Info: This module contains the functionality for reference
 *      datatypes in the H5T interface.
 */

#include "H5Tmodule.h" /* This source code file is part of the H5T module */
#define H5F_FRIEND     /*suppress error about including H5Fpkg   */
#define H5R_FRIEND     /*suppress error about including H5Rpkg   */

#include "H5private.h"   /* Generic Functions                        */
#include "H5CXprivate.h" /* API Contexts                             */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5Fpkg.h"      /* File                                     */
#include "H5MMprivate.h" /* Memory management                        */
#include "H5Rpkg.h"      /* References                               */
#include "H5Tpkg.h"      /* Datatypes                                */

#include "H5VLnative_private.h" /* Native VOL connector                     */

/****************/
/* Local Macros */
/****************/

#define H5T_REF_MEM_SIZE         (H5R_REF_BUF_SIZE)
#define H5T_REF_OBJ_MEM_SIZE     (H5R_OBJ_REF_BUF_SIZE)
#define H5T_REF_DSETREG_MEM_SIZE (H5R_DSET_REG_REF_BUF_SIZE)

#define H5T_REF_OBJ_DISK_SIZE(f)     (H5F_SIZEOF_ADDR(f))
#define H5T_REF_DSETREG_DISK_SIZE(f) (H5HG_HEAP_ID_SIZE(f))

/* Debug */
// #define H5T_REF_DEBUG
#ifdef H5T_REF_DEBUG
#define H5T_REF_LOG_DEBUG(...)                                                                               \
    do {                                                                                                     \
        fprintf(stdout, " # %s(): ", __func__);                                                              \
        fprintf(stdout, __VA_ARGS__);                                                                        \
        fprintf(stdout, "\n");                                                                               \
        fflush(stdout);                                                                                      \
    } while (0)
#else
#define H5T_REF_LOG_DEBUG(...)                                                                               \
    do {                                                                                                     \
    } while (0)
#endif

/******************/
/* Local Typedefs */
/******************/

/* For region compatibility support */
struct H5Tref_dsetreg {
    H5O_token_t token; /* Object token */
    H5S_t      *space; /* Dataspace */
};

/********************/
/* Local Prototypes */
/********************/

static herr_t H5T__ref_mem_isnull(const H5VL_object_t *src_file, const void *src_buf, bool *isnull);
static herr_t H5T__ref_mem_setnull(H5VL_object_t *dst_file, void *dst_buf, void *bg_buf);
static size_t H5T__ref_mem_getsize(H5VL_object_t *src_file, const void *src_buf, size_t src_size,
                                   H5VL_object_t *dst_file, bool *dst_copy);
static herr_t H5T__ref_mem_read(H5VL_object_t *src_file, const void *src_buf, size_t src_size,
                                H5VL_object_t *dst_file, void *dst_buf, size_t dst_size);
static herr_t H5T__ref_mem_write(H5VL_object_t *src_file, const void *src_buf, size_t src_size,
                                 H5R_type_t src_type, H5VL_object_t *dst_file, void *dst_buf, size_t dst_size,
                                 void *bg_buf);

static herr_t H5T__ref_disk_isnull(const H5VL_object_t *src_file, const void *src_buf, bool *isnull);
static herr_t H5T__ref_disk_setnull(H5VL_object_t *dst_file, void *dst_buf, void *bg_buf);
static size_t H5T__ref_disk_getsize(H5VL_object_t *src_file, const void *src_buf, size_t src_size,
                                    H5VL_object_t *dst_file, bool *dst_copy);
static herr_t H5T__ref_disk_read(H5VL_object_t *src_file, const void *src_buf, size_t src_size,
                                 H5VL_object_t *dst_file, void *dst_buf, size_t dst_size);
static herr_t H5T__ref_disk_write(H5VL_object_t *src_file, const void *src_buf, size_t src_size,
                                  H5R_type_t src_type, H5VL_object_t *dst_file, void *dst_buf,
                                  size_t dst_size, void *bg_buf);

/* For compatibility */
static herr_t H5T__ref_obj_disk_isnull(const H5VL_object_t *src_file, const void *src_buf, bool *isnull);
static size_t H5T__ref_obj_disk_getsize(H5VL_object_t *src_file, const void *src_buf, size_t src_size,
                                        H5VL_object_t *dst_file, bool *dst_copy);
static herr_t H5T__ref_obj_disk_read(H5VL_object_t *src_file, const void *src_buf, size_t src_size,
                                     H5VL_object_t *dst_file, void *dst_buf, size_t dst_size);

static herr_t H5T__ref_dsetreg_disk_isnull(const H5VL_object_t *src_file, const void *src_buf, bool *isnull);
static size_t H5T__ref_dsetreg_disk_getsize(H5VL_object_t *src_file, const void *src_buf, size_t src_size,
                                            H5VL_object_t *dst_file, bool *dst_copy);
static herr_t H5T__ref_dsetreg_disk_read(H5VL_object_t *src_file, const void *src_buf, size_t src_size,
                                         H5VL_object_t *dst_file, void *dst_buf, size_t dst_size);

/*******************/
/* Local Variables */
/*******************/

/* Class for reference in memory */
static const H5T_ref_class_t H5T_ref_mem_g = {
    H5T__ref_mem_isnull,  /* 'isnull' */
    H5T__ref_mem_setnull, /* 'setnull' */
    H5T__ref_mem_getsize, /* 'getsize' */
    H5T__ref_mem_read,    /* 'read' */
    H5T__ref_mem_write    /* 'write' */
};

static const H5T_ref_class_t H5T_ref_disk_g = {
    H5T__ref_disk_isnull,  /* 'isnull' */
    H5T__ref_disk_setnull, /* 'setnull' */
    H5T__ref_disk_getsize, /* 'getsize' */
    H5T__ref_disk_read,    /* 'read' */
    H5T__ref_disk_write    /* 'write' */
};

static const H5T_ref_class_t H5T_ref_obj_disk_g = {
    H5T__ref_obj_disk_isnull,  /* 'isnull' */
    NULL,                      /* 'setnull' */
    H5T__ref_obj_disk_getsize, /* 'getsize' */
    H5T__ref_obj_disk_read,    /* 'read' */
    NULL                       /* 'write' */
};

static const H5T_ref_class_t H5T_ref_dsetreg_disk_g = {
    H5T__ref_dsetreg_disk_isnull,  /* 'isnull' */
    NULL,                          /* 'setnull' */
    H5T__ref_dsetreg_disk_getsize, /* 'getsize' */
    H5T__ref_dsetreg_disk_read,    /* 'read' */
    NULL                           /* 'write' */
};

/*-------------------------------------------------------------------------
 * Function: H5T__ref_set_loc
 *
 * Purpose:	Sets the location of a reference datatype to be either on disk
 *          or in memory
 *
 * Return:
 *  One of two values on success:
 *      true - If the location of any reference types changed
 *      false - If the location of any reference types is the same
 *  Negative value is returned on failure
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5T__ref_set_loc(H5T_t *dt, H5VL_object_t *file, H5T_loc_t loc)
{
    htri_t ret_value = false; /* Indicate success, but no location change */

    FUNC_ENTER_PACKAGE
    H5T_REF_LOG_DEBUG("loc=%d", (int)loc);

    assert(dt);
    /* f is NULL when loc == H5T_LOC_MEMORY */
    assert(loc >= H5T_LOC_BADLOC && loc < H5T_LOC_MAXLOC);

    /* Only change the location if it's different */
    if (loc == dt->shared->u.atomic.u.r.loc && file == dt->shared->u.atomic.u.r.file)
        HGOTO_DONE(false);

    switch (loc) {
        case H5T_LOC_MEMORY: /* Memory based reference datatype */

            /* NB. We allow for the file to be non-NULL when doing
             * memory-to-memory conversion */

            /* Mark this type as being stored in memory */
            dt->shared->u.atomic.u.r.loc = H5T_LOC_MEMORY;

            /* Release owned file */
            if (dt->shared->owned_vol_obj) {
                if (H5VL_free_object(dt->shared->owned_vol_obj) < 0)
                    HGOTO_ERROR(H5E_REFERENCE, H5E_CANTCLOSEOBJ, FAIL, "unable to close owned VOL object");
                dt->shared->owned_vol_obj = NULL;
            } /* end if */

            /* Reset file ID (since this reference is in memory) */
            dt->shared->u.atomic.u.r.file = file; /* file is NULL */

            if (dt->shared->u.atomic.u.r.opaque) {
                /* Size in memory, disk size is different */
                dt->shared->size          = H5T_REF_MEM_SIZE;
                dt->shared->u.atomic.prec = 8 * dt->shared->size;

                /* Set up the function pointers to access the reference in memory */
                dt->shared->u.atomic.u.r.cls = &H5T_ref_mem_g;
            } /* end if */
            else if (dt->shared->u.atomic.u.r.rtype == H5R_OBJECT1) {
                /* Size in memory, disk size is different */
                dt->shared->size          = H5T_REF_OBJ_MEM_SIZE;
                dt->shared->u.atomic.prec = 8 * dt->shared->size;

                /* Unused for now */
                dt->shared->u.atomic.u.r.cls = NULL;
            } /* end else-if */
            else if (dt->shared->u.atomic.u.r.rtype == H5R_DATASET_REGION1) {
                /* Size in memory, disk size is different */
                dt->shared->size          = H5T_REF_DSETREG_MEM_SIZE;
                dt->shared->u.atomic.prec = 8 * dt->shared->size;

                /* Unused for now */
                dt->shared->u.atomic.u.r.cls = NULL;
            } /* end else-if */
            else
                HGOTO_ERROR(H5E_DATATYPE, H5E_BADTYPE, FAIL, "invalid location");
            break;

        case H5T_LOC_DISK: /* Disk based reference datatype */
            assert(file);

            /* Mark this type as being stored on disk */
            dt->shared->u.atomic.u.r.loc = H5T_LOC_DISK;

            /* Set file pointer (since this reference is on disk) */
            dt->shared->u.atomic.u.r.file = file;

            /* dt now owns a reference to file */
            if (H5T_own_vol_obj(dt, file) < 0)
                HGOTO_ERROR(H5E_REFERENCE, H5E_CANTINIT, FAIL, "can't give ownership of VOL object");

            if (dt->shared->u.atomic.u.r.rtype == H5R_OBJECT1) {
                H5F_t *f;

#ifndef NDEBUG
                {
                    bool is_native = false; /* Whether the file is using the native VOL connector */

                    /* Check if using native VOL connector */
                    if (H5VL_object_is_native(file, &is_native) < 0)
                        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, FAIL,
                                    "can't query if file uses native VOL connector");

                    /* Must use native VOL connector for this operation */
                    assert(is_native);
                }
#endif /* NDEBUG */

                /* Retrieve file from VOL object */
                if (NULL == (f = (H5F_t *)H5VL_object_data(file)))
                    HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid VOL object");

                /* Size on disk, memory size is different */
                dt->shared->size          = H5T_REF_OBJ_DISK_SIZE(f);
                dt->shared->u.atomic.prec = 8 * dt->shared->size;

                /* Set up the function pointers to access the reference in memory */
                dt->shared->u.atomic.u.r.cls = &H5T_ref_obj_disk_g;
            } /* end if */
            else if (dt->shared->u.atomic.u.r.rtype == H5R_DATASET_REGION1) {
                H5F_t *f;

#ifndef NDEBUG
                {
                    bool is_native = false; /* Whether the file is using the native VOL connector */

                    /* Check if using native VOL connector */
                    if (H5VL_object_is_native(file, &is_native) < 0)
                        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, FAIL,
                                    "can't query if file uses native VOL connector");

                    /* Must use native VOL connector for this operation */
                    assert(is_native);
                }
#endif /* NDEBUG */

                /* Retrieve file from VOL object */
                if (NULL == (f = (H5F_t *)H5VL_object_data(file)))
                    HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid VOL object");

                /* Size on disk, memory size is different */
                dt->shared->size          = H5T_REF_DSETREG_DISK_SIZE(f);
                dt->shared->u.atomic.prec = 8 * dt->shared->size;

                /* Set up the function pointers to access the reference in memory */
                dt->shared->u.atomic.u.r.cls = &H5T_ref_dsetreg_disk_g;
            } /* end else-if */
            else {
                H5VL_file_cont_info_t cont_info = {H5VL_CONTAINER_INFO_VERSION, 0, 0, 0};
                H5VL_file_get_args_t  vol_cb_args; /* Arguments to VOL callback */
                size_t                ref_encode_size;
                H5R_ref_priv_t        fixed_ref;

                /* Set up VOL callback arguments */
                vol_cb_args.op_type                 = H5VL_FILE_GET_CONT_INFO;
                vol_cb_args.args.get_cont_info.info = &cont_info;

                /* Get container info */
                if (H5VL_file_get(file, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
                    HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "unable to get container info");

                /* Retrieve min encode size (when references have no vlen part) */
                memset(&fixed_ref, 0, sizeof(fixed_ref));
                fixed_ref.type       = (int8_t)H5R_OBJECT2;
                fixed_ref.token_size = (uint8_t)cont_info.token_size;
                if (H5R__encode(NULL, &fixed_ref, NULL, &ref_encode_size, 0) < 0)
                    HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, FAIL, "can't get encode size");

                /* Size on disk, memory size is different */
                dt->shared->size =
                    MAX(sizeof(uint32_t) + H5R_ENCODE_HEADER_SIZE + cont_info.blob_id_size, ref_encode_size);
                dt->shared->u.atomic.prec = 8 * dt->shared->size;

                /* Set up the function pointers to access the information on
                 * disk. Region and attribute references are stored identically
                 * on disk, so use the same functions.
                 */
                dt->shared->u.atomic.u.r.cls = &H5T_ref_disk_g;
            }
            break;

        case H5T_LOC_BADLOC:
            /* Allow undefined location. In H5Odtype.c, H5O_dtype_decode sets undefined
             * location for reference type and leaves it for the caller to decide.
             */
            dt->shared->u.atomic.u.r.loc = H5T_LOC_BADLOC;

            /* Reset file pointer */
            dt->shared->u.atomic.u.r.file = NULL;

            /* Reset the function pointers */
            dt->shared->u.atomic.u.r.cls = NULL;

            break;

        case H5T_LOC_MAXLOC: /* MAXLOC is invalid */
        default:
            HGOTO_ERROR(H5E_DATATYPE, H5E_BADRANGE, FAIL, "invalid reference datatype location");
    } /* end switch */

    /* Indicate that the location changed */
    ret_value = true;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__ref_set_loc() */

/*-------------------------------------------------------------------------
 * Function:    H5T__ref_mem_isnull
 *
 * Purpose: Check if it's a NULL / uninitialized reference.
 *
 * Return:  Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__ref_mem_isnull(const H5VL_object_t H5_ATTR_UNUSED *src_file, const void *src_buf, bool *isnull)
{
    const unsigned char zeros[H5T_REF_MEM_SIZE] = {0};
    herr_t              ret_value               = SUCCEED;

    FUNC_ENTER_PACKAGE_NOERR
    H5T_REF_LOG_DEBUG("");

    /* Check parameters */
    assert(src_buf);
    assert(isnull);

    *isnull = (0 == memcmp(src_buf, zeros, H5T_REF_MEM_SIZE)) ? true : false;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__ref_mem_isnull() */

/*-------------------------------------------------------------------------
 * Function:    H5T__ref_mem_setnull
 *
 * Purpose: Set a reference as NULL / uninitialized.
 *
 * Return:  Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__ref_mem_setnull(H5VL_object_t H5_ATTR_UNUSED *dst_file, void *dst_buf, H5_ATTR_UNUSED void *bg_buf)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE_NOERR
    H5T_REF_LOG_DEBUG("");

    memset(dst_buf, 0, H5T_REF_MEM_SIZE);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__ref_mem_setnull() */

/*-------------------------------------------------------------------------
 * Function:	H5T__ref_mem_getsize
 *
 * Purpose:	Retrieves the size of a memory based reference.
 *
 * Return:	Non-negative on success/zero on failure
 *
 *-------------------------------------------------------------------------
 */
static size_t
H5T__ref_mem_getsize(H5VL_object_t H5_ATTR_UNUSED *src_file, const void *src_buf,
                     size_t H5_ATTR_UNUSED src_size, H5VL_object_t *dst_file, bool *dst_copy)
{
    H5VL_object_t        *vol_obj = NULL; /* VOL object for src ref's location */
    const H5R_ref_priv_t *src_ref = (const H5R_ref_priv_t *)src_buf;
    char                 *file_name_buf_dyn =
        NULL; /* Pointer to dynamically allocated buffer for file name, if static buffer is too small */
    unsigned flags     = 0; /* References flags */
    size_t   ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE
    H5T_REF_LOG_DEBUG("");

    /* Sanity check */
    assert(src_buf);
    assert(src_size == H5T_REF_MEM_SIZE);

    if (NULL != dst_file) {
        bool files_equal = true; /* Whether src & dst references are in same file */

        /* Retrieve VOL object */
        if (NULL == (vol_obj = H5VL_vol_object(src_ref->loc_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, 0, "invalid location identifier");

        /* Set external flag if referenced file is not destination file */
        if (H5VL_file_is_same(vol_obj, dst_file, &files_equal) < 0)
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTCOMPARE, 0, "can't check if files are equal");
        flags |= !files_equal ? H5R_IS_EXTERNAL : 0;
    }

    /* Force re-calculating encoding size if any flags are set */
    if (flags || !src_ref->encode_size) {
        H5VL_file_get_args_t vol_cb_args;               /* Arguments to VOL callback */
        char                *file_name = NULL;          /* Actual file name */
        char                 file_name_buf_static[256]; /* File name */
        size_t               file_name_len = 0;         /* Length of file name */

        /* Pass the correct encoding version for the selection depending on the
         * file libver bounds, this is later retrieved in H5S hyper encode */
        if (src_ref->type == (int8_t)H5R_DATASET_REGION2) {
            bool is_native = false; /* Whether the dest. file is using the native VOL connector */

            /* Check if using native VOL connector */
            if (H5VL_object_is_native(dst_file, &is_native) < 0)
                HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, 0, "can't query if file uses native VOL connector");

            /* Set the file's libver bounds if using the native VOL connector */
            if (is_native) {
                H5F_t *dst_f; /* Native file struct */

                if (NULL == (dst_f = (H5F_t *)H5VL_object_data(dst_file)))
                    HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, 0, "invalid VOL object");
                H5CX_set_libver_bounds(dst_f);
            } /* end if */
            else
                H5CX_set_libver_bounds(NULL);
        } /* end if */

        /* Set up VOL callback arguments */
        vol_cb_args.op_type                     = H5VL_FILE_GET_NAME;
        vol_cb_args.args.get_name.type          = H5I_FILE;
        vol_cb_args.args.get_name.buf_size      = sizeof(file_name_buf_static);
        vol_cb_args.args.get_name.buf           = file_name_buf_static;
        vol_cb_args.args.get_name.file_name_len = &file_name_len;

        /* Get file name */
        if (H5VL_file_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, NULL) < 0)
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, 0, "can't get file name");

        /* Check if we need to allocate a buffer for the file name */
        if (file_name_len >= sizeof(file_name_buf_static)) {
            /* Allocate file name buffer */
            if (NULL == (file_name_buf_dyn = H5MM_malloc(file_name_len + 1)))
                HGOTO_ERROR(H5E_REFERENCE, H5E_CANTALLOC, 0, "can't allocate space for file name");

            /* Update VOL callback arguments */
            vol_cb_args.args.get_name.buf_size = file_name_len + 1;
            vol_cb_args.args.get_name.buf      = file_name_buf_dyn;

            /* Get file name again */
            if (H5VL_file_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, NULL) < 0)
                HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, 0, "can't get file name");

            file_name = file_name_buf_dyn;
        } /* end if */
        else
            file_name = file_name_buf_static;

        /* Determine encoding size */
        if (H5R__encode(file_name, src_ref, NULL, &ret_value, flags) < 0)
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTENCODE, 0, "unable to determine encoding size");
    } /* end if */
    else {
        /* Can do a direct copy and skip blob decoding */
        if (src_ref->type == (int8_t)H5R_OBJECT2)
            *dst_copy = true;

        /* Get cached encoding size */
        ret_value = src_ref->encode_size;
    } /* end else */

done:
    H5MM_xfree(file_name_buf_dyn);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__ref_mem_getsize() */

/*-------------------------------------------------------------------------
 * Function:	H5T__ref_mem_read
 *
 * Purpose:	"Reads" the memory based reference into a buffer
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__ref_mem_read(H5VL_object_t H5_ATTR_UNUSED *src_file, const void *src_buf, size_t H5_ATTR_UNUSED src_size,
                  H5VL_object_t *dst_file, void *dst_buf, size_t dst_size)
{
    H5VL_object_t        *vol_obj; /* VOL object for src ref's location */
    const H5R_ref_priv_t *src_ref     = (const H5R_ref_priv_t *)src_buf;
    bool                  files_equal = true; /* Whether src & dst references are in same file */
    char                 *file_name   = NULL; /* Actual file name */
    char                  file_name_buf_static[256] = {'\0'}; /* File name */
    char                 *file_name_buf_dyn =
        NULL; /* Pointer to dynamically allocated buffer for file name, if static buffer is too small */
    unsigned flags     = 0;       /* References flags */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE
    H5T_REF_LOG_DEBUG("");

    /* Sanity check */
    assert(src_buf);
    assert(src_size == H5T_REF_MEM_SIZE);
    assert(dst_buf);
    assert(dst_size);

    /* Memory-to-memory conversion to support vlen conversion */
    if (NULL == dst_file) {
        H5MM_memcpy(dst_buf, src_buf, dst_size);
        HGOTO_DONE(ret_value);
    }

    /* Retrieve VOL object */
    if (NULL == (vol_obj = H5VL_vol_object(src_ref->loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, 0, "invalid location identifier");

    /* Set external flag if referenced file is not destination file */
    if (H5VL_file_is_same(vol_obj, dst_file, &files_equal) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTCOMPARE, FAIL, "can't check if files are equal");
    flags |= !files_equal ? H5R_IS_EXTERNAL : 0;

    /* Pass the correct encoding version for the selection depending on the
     * file libver bounds, this is later retrieved in H5S hyper encode */
    if (src_ref->type == (int8_t)H5R_DATASET_REGION2) {
        bool is_native = false; /* Whether the dest. file is using the native VOL connector */

        /* Check if using native VOL connector */
        if (H5VL_object_is_native(dst_file, &is_native) < 0)
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, 0, "can't query if file uses native VOL connector");

        /* Set the file's libver bounds if using the native VOL connector */
        if (is_native) {
            H5F_t *dst_f;

            if (NULL == (dst_f = (H5F_t *)H5VL_object_data(dst_file)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, 0, "invalid VOL object");
            H5CX_set_libver_bounds(dst_f);
        } /* end if */
        else
            H5CX_set_libver_bounds(NULL);
    } /* end if */

    /* Get file name (if external reference) */
    if (flags) {
        H5VL_file_get_args_t vol_cb_args;       /* Arguments to VOL callback */
        size_t               file_name_len = 0; /* Length of file name */

        /* Set up VOL callback arguments */
        vol_cb_args.op_type                     = H5VL_FILE_GET_NAME;
        vol_cb_args.args.get_name.type          = H5I_FILE;
        vol_cb_args.args.get_name.buf_size      = sizeof(file_name_buf_static);
        vol_cb_args.args.get_name.buf           = file_name_buf_static;
        vol_cb_args.args.get_name.file_name_len = &file_name_len;

        /* Get file name */
        if (H5VL_file_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, NULL) < 0)
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, 0, "can't get file name");

        /* Check if we need to allocate a buffer for the file name */
        if (file_name_len >= sizeof(file_name_buf_static)) {
            /* Allocate file name buffer */
            if (NULL == (file_name_buf_dyn = H5MM_malloc(file_name_len + 1)))
                HGOTO_ERROR(H5E_REFERENCE, H5E_CANTALLOC, 0, "can't allocate space for file name");

            /* Update VOL callback arguments */
            vol_cb_args.args.get_name.buf_size = file_name_len + 1;
            vol_cb_args.args.get_name.buf      = file_name_buf_dyn;

            /* Get file name again */
            if (H5VL_file_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, NULL) < 0)
                HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, 0, "can't get file name");

            file_name = file_name_buf_dyn;
        } /* end if */
        else
            file_name = file_name_buf_static;
    } /* end if */

    /* Encode reference */
    if (H5R__encode(file_name, src_ref, (unsigned char *)dst_buf, &dst_size, flags) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTENCODE, FAIL, "Cannot encode reference");

done:
    H5MM_xfree(file_name_buf_dyn);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__ref_mem_read() */

/*-------------------------------------------------------------------------
 * Function:	H5T__ref_mem_write
 *
 * Purpose:	"Writes" the memory reference from a buffer
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__ref_mem_write(H5VL_object_t *src_file, const void *src_buf, size_t src_size, H5R_type_t src_type,
                   H5VL_object_t H5_ATTR_UNUSED *dst_file, void *dst_buf,
                   size_t H5_ATTR_NDEBUG_UNUSED dst_size, void H5_ATTR_UNUSED *bg_buf)
{
    H5F_t          *src_f   = NULL;
    hid_t           file_id = H5I_INVALID_HID;
    H5R_ref_priv_t *dst_ref = (H5R_ref_priv_t *)dst_buf;
    H5R_ref_priv_t  tmp_ref; /* Temporary reference to decode into */
    herr_t          ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE
    H5T_REF_LOG_DEBUG("");

    /* Sanity check */
    assert(src_buf);
    assert(src_size);
    assert(dst_buf);
    assert(dst_size == H5T_REF_MEM_SIZE);
    HDcompile_assert(sizeof(*dst_ref) == sizeof(tmp_ref));

    /* Memory-to-memory conversion to support vlen conversion */
    if (NULL == src_file) {
        H5MM_memcpy(dst_buf, src_buf, src_size);
        HGOTO_DONE(ret_value);
    }

#ifndef NDEBUG
    if ((src_type == H5R_OBJECT1) || (src_type == H5R_DATASET_REGION1)) {
        bool is_native = false; /* Whether the src file is using the native VOL connector */

        /* Check if using native VOL connector */
        if (H5VL_object_is_native(src_file, &is_native) < 0)
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, FAIL, "can't query if file uses native VOL connector");

        /* Must use native VOL connector for this operation */
        assert(is_native);
    }
#endif /* NDEBUG */

    /* Retrieve file from VOL object */
    if (NULL == (src_f = (H5F_t *)H5VL_object_data(src_file)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid VOL object");

    /* Make sure reference buffer is correctly initialized */
    memset(&tmp_ref, 0, sizeof(tmp_ref));

    switch (src_type) {
        case H5R_OBJECT1: {
            size_t token_size = H5F_SIZEOF_ADDR(src_f);

            if (H5R__create_object((const H5O_token_t *)src_buf, token_size, &tmp_ref) < 0)
                HGOTO_ERROR(H5E_REFERENCE, H5E_CANTCREATE, FAIL, "unable to create object reference");
        } break;

        case H5R_DATASET_REGION1: {
            const struct H5Tref_dsetreg *src_reg    = (const struct H5Tref_dsetreg *)src_buf;
            size_t                       token_size = H5F_SIZEOF_ADDR(src_f);

            if (H5R__create_region(&src_reg->token, token_size, src_reg->space, &tmp_ref) < 0)
                HGOTO_ERROR(H5E_REFERENCE, H5E_CANTCREATE, FAIL, "unable to create region reference");

            /* create_region creates its internal copy of the space */
            if (H5S_close(src_reg->space) < 0)
                HGOTO_ERROR(H5E_REFERENCE, H5E_CANTFREE, FAIL, "Cannot close dataspace");
        } break;

        case H5R_DATASET_REGION2:
            /* Pass the correct encoding version for the selection depending on the
             * file libver bounds, this is later retrieved in H5S hyper decode */
            H5CX_set_libver_bounds(src_f);
            /* FALLTHROUGH */
            H5_ATTR_FALLTHROUGH
        case H5R_OBJECT2:
        case H5R_ATTR:
            /* Decode reference */
            if (H5R__decode((const unsigned char *)src_buf, &src_size, &tmp_ref) < 0)
                HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDECODE, FAIL, "Cannot decode reference");
            break;

        case H5R_BADTYPE:
        case H5R_MAXTYPE:
        default:
            assert("unknown reference type" && 0);
            HGOTO_ERROR(H5E_REFERENCE, H5E_UNSUPPORTED, FAIL, "internal error (unknown reference type)");
    } /* end switch */

    /* If no filename set, this is not an external reference */
    if (NULL == H5R_REF_FILENAME(&tmp_ref)) {
        /* TODO temporary hack to retrieve file object */
        if ((file_id = H5F_get_file_id(src_file, H5I_FILE, false)) < 0)
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file or file object");

        /* Attach loc ID to reference and hold reference to it, this is a
         * user exposed reference so set app_ref to true. */
        if (H5R__set_loc_id(&tmp_ref, file_id, true, true) < 0)
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTSET, FAIL, "unable to attach location id to reference");
    } /* end if */

    /* Set output info */
    H5MM_memcpy(dst_ref, &tmp_ref, sizeof(tmp_ref));

done:
    if ((file_id != H5I_INVALID_HID) && (H5I_dec_ref(file_id) < 0))
        HDONE_ERROR(H5E_REFERENCE, H5E_CANTDEC, FAIL, "unable to decrement refcount on location id");
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__ref_mem_write() */

/*-------------------------------------------------------------------------
 * Function:    H5T__ref_disk_isnull
 *
 * Purpose: Check if it's a NULL / uninitialized reference.
 *
 * Return:  Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
/*
 * It is the correct thing to do to have the reference buffer
 * be const-qualified here, but the VOL "blob specific" routine
 * needs a non-const pointer since an operation might modify
 * the "blob". Disable this warning here (at least temporarily)
 * for this reason.
 */
H5_GCC_CLANG_DIAG_OFF("cast-qual")
static herr_t
H5T__ref_disk_isnull(const H5VL_object_t *src_file, const void *src_buf, bool *isnull)
{
    const uint8_t *p = (const uint8_t *)src_buf;
    H5R_type_t     ref_type;
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE
    H5T_REF_LOG_DEBUG("");

    /* Check parameters */
    assert(src_file);
    assert(src_buf);
    assert(isnull);

    /* Try to check encoded reference type */
    ref_type = (H5R_type_t)*p++;
    if (ref_type) {
        /* This is a valid reference */
        *isnull = false;
    }
    else {
        H5VL_blob_specific_args_t vol_cb_args; /* Arguments to VOL callback */

        /* Skip the size / header */
        p = (const uint8_t *)src_buf + H5R_ENCODE_HEADER_SIZE + sizeof(uint32_t);

        /* Set up VOL callback arguments */
        vol_cb_args.op_type             = H5VL_BLOB_ISNULL;
        vol_cb_args.args.is_null.isnull = isnull;

        /* Check if blob ID is "nil" */
        if (H5VL_blob_specific(src_file, (void *)p, &vol_cb_args) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "unable to check if a blob ID is 'nil'");
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__ref_disk_isnull() */
H5_GCC_CLANG_DIAG_ON("cast-qual")

/*-------------------------------------------------------------------------
 * Function:    H5T__ref_disk_setnull
 *
 * Purpose: Set a reference as NULL / uninitialized.
 *
 * Return:  Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__ref_disk_setnull(H5VL_object_t *dst_file, void *dst_buf, void *bg_buf)
{
    H5VL_blob_specific_args_t vol_cb_args; /* Arguments to VOL callback */
    uint8_t                  *q         = (uint8_t *)dst_buf;
    uint8_t                  *p_bg      = (uint8_t *)bg_buf;
    herr_t                    ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE
    H5T_REF_LOG_DEBUG("");

    assert(dst_file);
    assert(dst_buf);

    /* TODO Should get rid of bg stuff */
    if (p_bg) {
        /* Skip the size / header */
        p_bg += (sizeof(uint32_t) + H5R_ENCODE_HEADER_SIZE);

        /* Set up VOL callback arguments */
        vol_cb_args.op_type = H5VL_BLOB_DELETE;

        /* Remove blob for old data */
        if (H5VL_blob_specific(dst_file, (void *)p_bg, &vol_cb_args) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREMOVE, FAIL, "unable to delete blob");
    } /* end if */

    /* Copy header manually so that it does not get encoded into the blob */
    memset(q, 0, H5R_ENCODE_HEADER_SIZE);
    q += H5R_ENCODE_HEADER_SIZE;

    /* Set the size */
    UINT32ENCODE(q, 0);

    /* Set up VOL callback arguments */
    vol_cb_args.op_type = H5VL_BLOB_SETNULL;

    /* Set blob ID to "nil" */
    if (H5VL_blob_specific(dst_file, q, &vol_cb_args) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTSET, FAIL, "unable to set a blob ID to 'nil'");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__ref_disk_setnull() */

/*-------------------------------------------------------------------------
 * Function:	H5T__ref_disk_getsize
 *
 * Purpose:	Retrieves the length of a disk based reference.
 *
 * Return:	Non-negative value (cannot fail)
 *
 *-------------------------------------------------------------------------
 */
static size_t
H5T__ref_disk_getsize(H5VL_object_t H5_ATTR_UNUSED *src_file, const void *src_buf, size_t src_size,
                      H5VL_object_t H5_ATTR_UNUSED *dst_file, bool *dst_copy)
{
    const uint8_t *p = (const uint8_t *)src_buf;
    unsigned       flags;
    H5R_type_t     ref_type;
    size_t         ret_value = 0;

    FUNC_ENTER_PACKAGE
    H5T_REF_LOG_DEBUG("");

    assert(src_buf);

    /* Set reference type */
    ref_type = (H5R_type_t)*p++;
    if (ref_type <= H5R_BADTYPE || ref_type >= H5R_MAXTYPE)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, 0, "invalid reference type");

    /* Set flags */
    flags = (unsigned)*p++;

    if (!(flags & H5R_IS_EXTERNAL) && (ref_type == H5R_OBJECT2)) {
        /* Can do a direct copy and skip blob decoding */
        *dst_copy = true;

        ret_value = src_size;
    } /* end if */
    else {
        /* Retrieve encoded data size */
        UINT32DECODE(p, ret_value);

        /* Add size of the header */
        ret_value += H5R_ENCODE_HEADER_SIZE;
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__ref_disk_getsize() */

/*-------------------------------------------------------------------------
 * Function:	H5T__ref_disk_read
 *
 * Purpose:	Reads the disk based reference into a buffer
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__ref_disk_read(H5VL_object_t *src_file, const void *src_buf, size_t H5_ATTR_NDEBUG_UNUSED src_size,
                   H5VL_object_t H5_ATTR_UNUSED *dst_file, void *dst_buf, size_t dst_size)
{
    const uint8_t *p         = (const uint8_t *)src_buf;
    uint8_t       *q         = (uint8_t *)dst_buf;
    size_t         blob_size = dst_size;
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE
    H5T_REF_LOG_DEBUG("");

    assert(src_file);
    assert(src_buf);
    assert(dst_buf);
    assert(dst_size);

    /* Copy header manually */
    H5MM_memcpy(q, p, H5R_ENCODE_HEADER_SIZE);
    p += H5R_ENCODE_HEADER_SIZE;
    q += H5R_ENCODE_HEADER_SIZE;
    blob_size -= H5R_ENCODE_HEADER_SIZE;

    /* Skip the size */
    p += sizeof(uint32_t);
    assert(src_size > (H5R_ENCODE_HEADER_SIZE + sizeof(uint32_t)));

    /* Retrieve blob */
    if (H5VL_blob_get(src_file, p, q, blob_size, NULL) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "unable to get blob");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__ref_disk_read() */

/*-------------------------------------------------------------------------
 * Function:	H5T__ref_disk_write
 *
 * Purpose:	Writes the disk based reference from a buffer
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__ref_disk_write(H5VL_object_t H5_ATTR_UNUSED *src_file, const void *src_buf, size_t src_size,
                    H5R_type_t H5_ATTR_UNUSED src_type, H5VL_object_t *dst_file, void *dst_buf,
                    size_t H5_ATTR_NDEBUG_UNUSED dst_size, void *bg_buf)
{
    const uint8_t *p         = (const uint8_t *)src_buf;
    uint8_t       *q         = (uint8_t *)dst_buf;
    uint8_t       *p_bg      = (uint8_t *)bg_buf;
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE
    H5T_REF_LOG_DEBUG("");

    assert(src_buf);
    assert(src_size);
    assert(dst_file);
    assert(dst_buf);

    /* TODO Should get rid of bg stuff */
    if (p_bg) {
        H5VL_blob_specific_args_t vol_cb_args; /* Arguments to VOL callback */

        /* Skip the size / header */
        p_bg += (sizeof(uint32_t) + H5R_ENCODE_HEADER_SIZE);

#ifndef NDEBUG
        size_t p_buf_size_left = dst_size;
        assert(p_buf_size_left > (sizeof(uint32_t) + H5R_ENCODE_HEADER_SIZE));
#endif

        /* Set up VOL callback arguments */
        vol_cb_args.op_type = H5VL_BLOB_DELETE;

        /* Remove blob for old data */
        if (H5VL_blob_specific(dst_file, (void *)p_bg, &vol_cb_args) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREMOVE, FAIL, "unable to delete blob");
    } /* end if */

    /* Copy header manually so that it does not get encoded into the blob */
    H5MM_memcpy(q, p, H5R_ENCODE_HEADER_SIZE);
    p += H5R_ENCODE_HEADER_SIZE;
    q += H5R_ENCODE_HEADER_SIZE;
    src_size -= H5R_ENCODE_HEADER_SIZE;

#ifndef NDEBUG
    {
        size_t buf_size_left = dst_size - sizeof(uint32_t);
        assert(buf_size_left > sizeof(uint32_t));
    }
#endif

    /* Set the size */
    UINT32ENCODE(q, src_size);

    /* Store blob */
    if (H5VL_blob_put(dst_file, p, src_size, q, NULL) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTSET, FAIL, "unable to put blob");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__ref_disk_write() */

/*-------------------------------------------------------------------------
 * Function:    H5T__ref_obj_disk_isnull
 *
 * Purpose: Check if it's a NULL / uninitialized reference.
 *
 * Return:  Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__ref_obj_disk_isnull(const H5VL_object_t *src_file, const void *src_buf, bool *isnull)
{
    H5F_t         *src_f;
    const uint8_t *p = (const uint8_t *)src_buf;
    haddr_t        addr;
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE
    H5T_REF_LOG_DEBUG("");

    /* Check parameters */
    assert(src_file);
    assert(src_buf);
    assert(isnull);

#ifndef NDEBUG
    {
        bool is_native = false; /* Whether the src file is using the native VOL connector */

        /* Check if using native VOL connector */
        if (H5VL_object_is_native(src_file, &is_native) < 0)
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, FAIL, "can't query if file uses native VOL connector");

        /* Must use native VOL connector for this operation */
        assert(is_native);
    }
#endif /* NDEBUG */

    /* Retrieve file from VOL object */
    if (NULL == (src_f = (H5F_t *)H5VL_object_data(src_file)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid VOL object");

    /* Get the object address */
    H5F_addr_decode(src_f, &p, &addr);

    /* Check if heap address is 'nil' */
    *isnull = (addr == 0) ? true : false;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__ref_obj_disk_isnull() */

/*-------------------------------------------------------------------------
 * Function:    H5T__ref_obj_disk_getsize
 *
 * Purpose: Retrieves the length of a disk based reference.
 *
 * Return:  Non-negative value (cannot fail)
 *
 *-------------------------------------------------------------------------
 */
static size_t
H5T__ref_obj_disk_getsize(H5VL_object_t *src_file, const void H5_ATTR_UNUSED *src_buf,
                          size_t H5_ATTR_UNUSED src_size, H5VL_object_t H5_ATTR_UNUSED *dst_file,
                          bool H5_ATTR_UNUSED *dst_copy)
{
    H5F_t *src_f;
    size_t ret_value = 0;

    FUNC_ENTER_PACKAGE
    H5T_REF_LOG_DEBUG("");

    assert(src_file);
    assert(src_buf);

#ifndef NDEBUG
    {
        bool is_native = false; /* Whether the src file is using the native VOL connector */

        /* Check if using native VOL connector */
        if (H5VL_object_is_native(src_file, &is_native) < 0)
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, 0, "can't query if file uses native VOL connector");

        /* Must use native VOL connector for this operation */
        assert(is_native);
    }
#endif /* NDEBUG */

    /* Retrieve file from VOL object */
    if (NULL == (src_f = (H5F_t *)H5VL_object_data(src_file)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, 0, "invalid VOL object");

    assert(src_size == H5T_REF_OBJ_DISK_SIZE(src_f));

    ret_value = H5T_REF_OBJ_DISK_SIZE(src_f);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__ref_obj_disk_getsize() */

/*-------------------------------------------------------------------------
 * Function:    H5T__ref_obj_disk_read
 *
 * Purpose: Reads the disk based reference into a buffer
 *
 * Return:  Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__ref_obj_disk_read(H5VL_object_t *src_file, const void *src_buf, size_t src_size,
                       H5VL_object_t H5_ATTR_UNUSED *dst_file, void *dst_buf, size_t H5_ATTR_UNUSED dst_size)
{
    H5F_t *src_f;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE
    H5T_REF_LOG_DEBUG("");

    assert(src_file);
    assert(src_buf);
    assert(dst_buf);

#ifndef NDEBUG
    {
        bool is_native = false; /* Whether the src file is using the native VOL connector */

        /* Check if using native VOL connector */
        if (H5VL_object_is_native(src_file, &is_native) < 0)
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, FAIL, "can't query if file uses native VOL connector");

        /* Must use native VOL connector for this operation */
        assert(is_native);
    }
#endif /* NDEBUG */

    /* Retrieve file from VOL object */
    if (NULL == (src_f = (H5F_t *)H5VL_object_data(src_file)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid VOL object");

    assert(src_size == H5T_REF_OBJ_DISK_SIZE(src_f));
    assert(dst_size == H5F_SIZEOF_ADDR(src_f));

    /* Get object address */
    if (H5R__decode_token_obj_compat((const unsigned char *)src_buf, &src_size, (H5O_token_t *)dst_buf,
                                     H5F_SIZEOF_ADDR(src_f)) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDECODE, FAIL, "unable to get object address");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__ref_obj_disk_read() */

/*-------------------------------------------------------------------------
 * Function:    H5T__ref_dsetreg_disk_isnull
 *
 * Purpose: Check if it's a NULL / uninitialized reference.
 *
 * Return:  Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__ref_dsetreg_disk_isnull(const H5VL_object_t *src_file, const void *src_buf, bool *isnull)
{
    H5F_t         *src_f;
    const uint8_t *p = (const uint8_t *)src_buf;
    haddr_t        addr;
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE
    H5T_REF_LOG_DEBUG("");

    /* Check parameters */
    assert(src_file);
    assert(src_buf);
    assert(isnull);

#ifndef NDEBUG
    {
        bool is_native = false; /* Whether the src file is using the native VOL connector */

        /* Check if using native VOL connector */
        if (H5VL_object_is_native(src_file, &is_native) < 0)
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, FAIL, "can't query if file uses native VOL connector");

        /* Must use native VOL connector for this operation */
        assert(is_native);
    }
#endif /* NDEBUG */

    /* Retrieve file from VOL object */
    if (NULL == (src_f = (H5F_t *)H5VL_object_data(src_file)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid VOL object");

    /* Get the heap address */
    H5F_addr_decode(src_f, &p, &addr);

    /* Check if heap address is 'nil' */
    *isnull = (addr == 0) ? true : false;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__ref_dsetreg_disk_isnull() */

/*-------------------------------------------------------------------------
 * Function:    H5T__ref_dsetreg_disk_getsize
 *
 * Purpose: Retrieves the length of a disk based reference.
 *
 * Return:  Non-negative value (cannot fail)
 *
 *-------------------------------------------------------------------------
 */
static size_t
H5T__ref_dsetreg_disk_getsize(H5VL_object_t H5_ATTR_UNUSED *src_file, const void H5_ATTR_UNUSED *src_buf,
                              size_t H5_ATTR_UNUSED src_size, H5VL_object_t H5_ATTR_UNUSED *dst_file,
                              bool H5_ATTR_UNUSED *dst_copy)
{
    size_t ret_value = sizeof(struct H5Tref_dsetreg);

#ifndef NDEBUG
    FUNC_ENTER_PACKAGE
#else
    FUNC_ENTER_PACKAGE_NOERR
#endif
    H5T_REF_LOG_DEBUG("");

    assert(src_buf);

#ifndef NDEBUG
    {
        H5F_t *src_f;
        bool   is_native = false; /* Whether the src file is using the native VOL connector */

        /* Check if using native VOL connector */
        if (H5VL_object_is_native(src_file, &is_native) < 0)
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, 0, "can't query if file uses native VOL connector");

        /* Must use native VOL connector for this operation */
        assert(is_native);

        /* Retrieve file from VOL object */
        if (NULL == (src_f = (H5F_t *)H5VL_object_data(src_file)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, 0, "invalid VOL object");

        assert(src_size == H5T_REF_DSETREG_DISK_SIZE(src_f));
    }
#endif /* NDEBUG */

#ifndef NDEBUG
done:
#endif
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__ref_dsetreg_disk_getsize() */

/*-------------------------------------------------------------------------
 * Function:    H5T__ref_dsetreg_disk_read
 *
 * Purpose: Reads the disk based reference into a buffer
 *
 * Return:  Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__ref_dsetreg_disk_read(H5VL_object_t *src_file, const void *src_buf, size_t src_size,
                           H5VL_object_t H5_ATTR_UNUSED *dst_file, void *dst_buf,
                           size_t H5_ATTR_UNUSED dst_size)
{
    H5F_t                 *src_f;
    struct H5Tref_dsetreg *dst_reg   = (struct H5Tref_dsetreg *)dst_buf;
    herr_t                 ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE
    H5T_REF_LOG_DEBUG("");

    assert(src_file);
    assert(src_buf);
    assert(dst_buf);
    assert(dst_size == sizeof(struct H5Tref_dsetreg));

#ifndef NDEBUG
    {
        bool is_native = false; /* Whether the src file is using the native VOL connector */

        /* Check if using native VOL connector */
        if (H5VL_object_is_native(src_file, &is_native) < 0)
            HGOTO_ERROR(H5E_REFERENCE, H5E_CANTGET, FAIL, "can't query if file uses native VOL connector");

        /* Must use native VOL connector for this operation */
        assert(is_native);
    }
#endif /* NDEBUG */

    /* Retrieve file from VOL object */
    if (NULL == (src_f = (H5F_t *)H5VL_object_data(src_file)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid VOL object");

    assert(src_size == H5T_REF_DSETREG_DISK_SIZE(src_f));

    /* Retrieve object address and space */
    if (H5R__decode_token_region_compat(src_f, (const unsigned char *)src_buf, &src_size, &dst_reg->token,
                                        H5F_SIZEOF_ADDR(src_f), &dst_reg->space) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTDECODE, FAIL, "unable to get object address");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__ref_dsetreg_disk_read() */

/*-------------------------------------------------------------------------
 * Function:    H5T__ref_reclaim
 *
 * Purpose: Internal routine to free reference datatypes
 *
 * Return:  Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5T__ref_reclaim(void *elem, const H5T_t *dt)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE
    H5T_REF_LOG_DEBUG("");

    /* Sanity checks */
    assert(elem);
    assert(dt && (dt->shared->type == H5T_REFERENCE));

    if (dt->shared->u.atomic.u.r.opaque && H5R__destroy((H5R_ref_priv_t *)elem) < 0)
        HGOTO_ERROR(H5E_REFERENCE, H5E_CANTFREE, FAIL, "cannot free reference");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__ref_reclaim() */
