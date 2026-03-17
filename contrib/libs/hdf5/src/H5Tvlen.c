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
 * Module Info: This module contains the functionality for variable-length
 *      datatypes in the H5T interface.
 */

/****************/
/* Module Setup */
/****************/

#include "H5Tmodule.h" /* This source code file is part of the H5T module */
#define H5F_FRIEND     /*suppress error about including H5Fpkg   */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions    */
#include "H5CXprivate.h" /* API Contexts         */
#include "H5Eprivate.h"  /* Error handling       */
#include "H5Fpkg.h"      /* File                 */
#include "H5Iprivate.h"  /* IDs                  */
#include "H5MMprivate.h" /* Memory management    */
#include "H5Tpkg.h"      /* Datatypes            */
#include "H5VLprivate.h" /* Virtual Object Layer                     */

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

/* Memory-based VL sequence callbacks */
static herr_t H5T__vlen_mem_seq_getlen(H5VL_object_t *file, const void *_vl, size_t *len);
static void  *H5T__vlen_mem_seq_getptr(void *_vl);
static herr_t H5T__vlen_mem_seq_isnull(const H5VL_object_t *file, void *_vl, bool *isnull);
static herr_t H5T__vlen_mem_seq_setnull(H5VL_object_t *file, void *_vl, void *_bg);
static herr_t H5T__vlen_mem_seq_read(H5VL_object_t *file, void *_vl, void *_buf, size_t len);
static herr_t H5T__vlen_mem_seq_write(H5VL_object_t *file, const H5T_vlen_alloc_info_t *vl_alloc_info,
                                      void *_vl, void *_buf, void *_bg, size_t seq_len, size_t base_size);

/* Memory-based VL string callbacks */
static herr_t H5T__vlen_mem_str_getlen(H5VL_object_t *file, const void *_vl, size_t *len);
static void  *H5T__vlen_mem_str_getptr(void *_vl);
static herr_t H5T__vlen_mem_str_isnull(const H5VL_object_t *file, void *_vl, bool *isnull);
static herr_t H5T__vlen_mem_str_setnull(H5VL_object_t *file, void *_vl, void *_bg);
static herr_t H5T__vlen_mem_str_read(H5VL_object_t *file, void *_vl, void *_buf, size_t len);
static herr_t H5T__vlen_mem_str_write(H5VL_object_t *file, const H5T_vlen_alloc_info_t *vl_alloc_info,
                                      void *_vl, void *_buf, void *_bg, size_t seq_len, size_t base_size);

/* Disk-based VL sequence (and string) callbacks */
static herr_t H5T__vlen_disk_getlen(H5VL_object_t *file, const void *_vl, size_t *len);
static herr_t H5T__vlen_disk_isnull(const H5VL_object_t *file, void *_vl, bool *isnull);
static herr_t H5T__vlen_disk_setnull(H5VL_object_t *file, void *_vl, void *_bg);
static herr_t H5T__vlen_disk_read(H5VL_object_t *file, void *_vl, void *_buf, size_t len);
static herr_t H5T__vlen_disk_write(H5VL_object_t *file, const H5T_vlen_alloc_info_t *vl_alloc_info, void *_vl,
                                   void *_buf, void *_bg, size_t seq_len, size_t base_size);
static herr_t H5T__vlen_disk_delete(H5VL_object_t *file, void *_vl);

/*********************/
/* Public Variables */
/*********************/

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Class for VL sequences in memory */
static const H5T_vlen_class_t H5T_vlen_mem_seq_g = {
    H5T__vlen_mem_seq_getlen,  /* 'getlen' */
    H5T__vlen_mem_seq_getptr,  /* 'getptr' */
    H5T__vlen_mem_seq_isnull,  /* 'isnull' */
    H5T__vlen_mem_seq_setnull, /* 'setnull' */
    H5T__vlen_mem_seq_read,    /* 'read' */
    H5T__vlen_mem_seq_write,   /* 'write' */
    NULL                       /* 'delete' */
};

/* Class for VL strings in memory */
static const H5T_vlen_class_t H5T_vlen_mem_str_g = {
    H5T__vlen_mem_str_getlen,  /* 'getlen' */
    H5T__vlen_mem_str_getptr,  /* 'getptr' */
    H5T__vlen_mem_str_isnull,  /* 'isnull' */
    H5T__vlen_mem_str_setnull, /* 'setnull' */
    H5T__vlen_mem_str_read,    /* 'read' */
    H5T__vlen_mem_str_write,   /* 'write' */
    NULL                       /* 'delete' */
};

/* Class for both VL strings and sequences in file */
static const H5T_vlen_class_t H5T_vlen_disk_g = {
    H5T__vlen_disk_getlen,  /* 'getlen' */
    NULL,                   /* 'getptr' */
    H5T__vlen_disk_isnull,  /* 'isnull' */
    H5T__vlen_disk_setnull, /* 'setnull' */
    H5T__vlen_disk_read,    /* 'read' */
    H5T__vlen_disk_write,   /* 'write' */
    H5T__vlen_disk_delete   /* 'delete' */
};

/*-------------------------------------------------------------------------
 * Function:	H5Tvlen_create
 *
 * Purpose:	Create a new variable-length datatype based on the specified
 *		BASE_TYPE.
 *
 * Return:	Success:	ID of new VL datatype
 *
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Tvlen_create(hid_t base_id)
{
    H5T_t *base = NULL; /*base datatype	*/
    H5T_t *dt   = NULL; /*new datatype	*/
    hid_t  ret_value;   /*return value			*/

    FUNC_ENTER_API(FAIL)
    H5TRACE1("i", "i", base_id);

    /* Check args */
    if (NULL == (base = (H5T_t *)H5I_object_verify(base_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not an valid base datatype");

    /* Create up VL datatype */
    if ((dt = H5T__vlen_create(base)) == NULL)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "invalid VL location");

    /* Register the type */
    if ((ret_value = H5I_register(H5I_DATATYPE, dt, true)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREGISTER, FAIL, "unable to register datatype");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tvlen_create() */

/*-------------------------------------------------------------------------
 * Function:	H5T__vlen_create
 *
 * Purpose:	Create a new variable-length datatype based on the specified
 *		BASE_TYPE.
 *
 * Return:	Success:	new VL datatype
 *
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
H5T_t *
H5T__vlen_create(const H5T_t *base)
{
    H5T_t *dt        = NULL; /* New VL datatype */
    H5T_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(base);

    /* Build new type */
    if (NULL == (dt = H5T__alloc()))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTALLOC, NULL, "memory allocation failed");
    dt->shared->type = H5T_VLEN;

    /*
     * Force conversions (i.e. memory to memory conversions should duplicate
     * data, not point to the same VL sequences)
     */
    dt->shared->force_conv = true;
    if (NULL == (dt->shared->parent = H5T_copy(base, H5T_COPY_ALL)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCOPY, NULL, "can't copy base datatype");

    /* Inherit encoding version from base type */
    dt->shared->version = base->shared->version;

    /* This is a sequence, not a string */
    dt->shared->u.vlen.type = H5T_VLEN_SEQUENCE;

    /* Set up VL information */
    if (H5T_set_loc(dt, NULL, H5T_LOC_MEMORY) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, NULL, "invalid datatype location");

    /* Set return value */
    ret_value = dt;

done:
    if (!ret_value)
        if (dt && H5T_close_real(dt) < 0)
            HDONE_ERROR(H5E_DATATYPE, H5E_CANTRELEASE, NULL, "unable to release datatype info");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__vlen_create() */

/*-------------------------------------------------------------------------
 * Function: H5T__vlen_set_loc
 *
 * Purpose:	Sets the location of a VL datatype to be either on disk or in memory
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
H5T__vlen_set_loc(H5T_t *dt, H5VL_object_t *file, H5T_loc_t loc)
{
    htri_t ret_value = false; /* Indicate success, but no location change */

    FUNC_ENTER_PACKAGE

    /* check parameters */
    assert(dt);
    assert(loc >= H5T_LOC_BADLOC && loc < H5T_LOC_MAXLOC);

    /* Only change the location if it's different */
    if (loc != dt->shared->u.vlen.loc || file != dt->shared->u.vlen.file) {
        switch (loc) {
            case H5T_LOC_MEMORY: /* Memory based VL datatype */
                assert(NULL == file);

                /* Mark this type as being stored in memory */
                dt->shared->u.vlen.loc = H5T_LOC_MEMORY;

                if (dt->shared->u.vlen.type == H5T_VLEN_SEQUENCE) {
                    /* Size in memory, disk size is different */
                    dt->shared->size = sizeof(hvl_t);

                    /* Set up the function pointers to access the VL sequence in memory */
                    dt->shared->u.vlen.cls = &H5T_vlen_mem_seq_g;
                } /* end if */
                else if (dt->shared->u.vlen.type == H5T_VLEN_STRING) {
                    /* Size in memory, disk size is different */
                    dt->shared->size = sizeof(char *);

                    /* Set up the function pointers to access the VL string in memory */
                    dt->shared->u.vlen.cls = &H5T_vlen_mem_str_g;
                } /* end else-if */
                else
                    assert(0 && "Invalid VL type");

                /* Release owned file */
                if (dt->shared->owned_vol_obj) {
                    if (H5VL_free_object(dt->shared->owned_vol_obj) < 0)
                        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTCLOSEOBJ, FAIL, "unable to close owned VOL object");
                    dt->shared->owned_vol_obj = NULL;
                } /* end if */

                /* Reset file pointer (since this VL is in memory) */
                dt->shared->u.vlen.file = NULL;
                break;

            /* Disk based VL datatype */
            case H5T_LOC_DISK: {
                H5VL_file_cont_info_t cont_info = {H5VL_CONTAINER_INFO_VERSION, 0, 0, 0};
                H5VL_file_get_args_t  vol_cb_args; /* Arguments to VOL callback */

                assert(file);

                /* Mark this type as being stored on disk */
                dt->shared->u.vlen.loc = H5T_LOC_DISK;

                /* Set up VOL callback arguments */
                vol_cb_args.op_type                 = H5VL_FILE_GET_CONT_INFO;
                vol_cb_args.args.get_cont_info.info = &cont_info;

                /* Get container info */
                if (H5VL_file_get(file, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
                    HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "unable to get container info");

                /* The datatype size is equal to 4 bytes for the sequence length
                 * plus the size of a blob id */
                dt->shared->size = 4 + cont_info.blob_id_size;

                /* Set up the function pointers to access the VL information on disk */
                /* VL sequences and VL strings are stored identically on disk, so use the same functions */
                dt->shared->u.vlen.cls = &H5T_vlen_disk_g;

                /* Set file ID (since this VL is on disk) */
                dt->shared->u.vlen.file = file;

                /* dt now owns a reference to file */
                if (H5T_own_vol_obj(dt, file) < 0)
                    HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "can't give ownership of VOL object");
                break;
            }

            case H5T_LOC_BADLOC:
                /* Allow undefined location. In H5Odtype.c, H5O_dtype_decode sets undefined
                 * location for VL type and leaves it for the caller to decide.
                 */
                dt->shared->u.vlen.loc = H5T_LOC_BADLOC;

                /* Reset the function pointers to access the VL information */
                dt->shared->u.vlen.cls = NULL;

                /* Reset file pointer */
                dt->shared->u.vlen.file = NULL;
                break;

            case H5T_LOC_MAXLOC:
                /* MAXLOC is invalid */
            default:
                HGOTO_ERROR(H5E_DATATYPE, H5E_BADRANGE, FAIL, "invalid VL datatype location");
        } /* end switch */ /*lint !e788 All appropriate cases are covered */

        /* Indicate that the location changed */
        ret_value = true;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__vlen_set_loc() */

/*-------------------------------------------------------------------------
 * Function:	H5T__vlen_mem_seq_getlen
 *
 * Purpose:	Retrieves the length of a memory based VL element.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__vlen_mem_seq_getlen(H5VL_object_t H5_ATTR_UNUSED *file, const void *_vl, size_t *len)
{
    hvl_t vl; /* User's hvl_t information */

    FUNC_ENTER_PACKAGE_NOERR

    assert(_vl);
    assert(len);

    /* Copy to ensure correct alignment */
    H5MM_memcpy(&vl, _vl, sizeof(hvl_t));

    *len = vl.len;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5T__vlen_mem_seq_getlen() */

/*-------------------------------------------------------------------------
 * Function:	H5T__vlen_mem_seq_getptr
 *
 * Purpose:	Retrieves the pointer for a memory based VL element.
 *
 * Return:	Non-NULL on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
static void *
H5T__vlen_mem_seq_getptr(void *_vl)
{
    hvl_t vl; /* User's hvl_t information */

    FUNC_ENTER_PACKAGE_NOERR

    assert(_vl);

    /* Copy to ensure correct alignment */
    H5MM_memcpy(&vl, _vl, sizeof(hvl_t));

    FUNC_LEAVE_NOAPI(vl.p)
} /* end H5T__vlen_mem_seq_getptr() */

/*-------------------------------------------------------------------------
 * Function:	H5T__vlen_mem_seq_isnull
 *
 * Purpose:	Checks if a memory sequence is the "null" sequence
 *
 * Return:	Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__vlen_mem_seq_isnull(const H5VL_object_t H5_ATTR_UNUSED *file, void *_vl, bool *isnull)
{
    hvl_t vl; /* User's hvl_t information */

    FUNC_ENTER_PACKAGE_NOERR

    assert(_vl);

    /* Copy to ensure correct alignment */
    H5MM_memcpy(&vl, _vl, sizeof(hvl_t));

    *isnull = ((vl.len == 0 || vl.p == NULL) ? true : false);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5T__vlen_mem_seq_isnull() */

/*-------------------------------------------------------------------------
 * Function:	H5T__vlen_mem_seq_setnull
 *
 * Purpose:	Sets a VL info object in memory to the "nil" value
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__vlen_mem_seq_setnull(H5VL_object_t H5_ATTR_UNUSED *file, void *_vl, void H5_ATTR_UNUSED *_bg)
{
    hvl_t vl; /* Temporary hvl_t to use during operation */

    FUNC_ENTER_PACKAGE_NOERR

    /* check parameters */
    assert(_vl);

    /* Set the "nil" hvl_t */
    vl.len = 0;
    vl.p   = NULL;

    /* Set pointer in user's buffer with memcpy, to avoid alignment issues */
    H5MM_memcpy(_vl, &vl, sizeof(hvl_t));

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5T__vlen_mem_seq_setnull() */

/*-------------------------------------------------------------------------
 * Function:	H5T__vlen_mem_seq_read
 *
 * Purpose:	"Reads" the memory based VL sequence into a buffer
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__vlen_mem_seq_read(H5VL_object_t H5_ATTR_UNUSED *file, void *_vl, void *buf, size_t len)
{
    hvl_t vl; /* User's hvl_t information */

    FUNC_ENTER_PACKAGE_NOERR

    assert(buf);
    assert(_vl);

    /* Copy to ensure correct alignment */
    H5MM_memcpy(&vl, _vl, sizeof(hvl_t));
    assert(vl.p);

    H5MM_memcpy(buf, vl.p, len);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5T__vlen_mem_seq_read() */

/*-------------------------------------------------------------------------
 * Function:	H5T__vlen_mem_seq_write
 *
 * Purpose:	"Writes" the memory based VL sequence from a buffer
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__vlen_mem_seq_write(H5VL_object_t H5_ATTR_UNUSED *file, const H5T_vlen_alloc_info_t *vl_alloc_info,
                        void *_vl, void *buf, void H5_ATTR_UNUSED *_bg, size_t seq_len, size_t base_size)
{
    hvl_t  vl;                  /* Temporary hvl_t to use during operation */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check parameters */
    assert(_vl);
    assert(buf);

    if (seq_len) {
        size_t len = seq_len * base_size; /* Sequence size */

        /* Use the user's memory allocation routine is one is defined */
        if (vl_alloc_info->alloc_func != NULL) {
            if (NULL == (vl.p = (vl_alloc_info->alloc_func)(len, vl_alloc_info->alloc_info)))
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTALLOC, FAIL,
                            "application memory allocation routine failed for VL data");
        }    /* end if */
        else /* Default to system malloc */
            if (NULL == (vl.p = malloc(len)))
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTALLOC, FAIL, "memory allocation failed for VL data");

        /* Copy the data into the newly allocated buffer */
        H5MM_memcpy(vl.p, buf, len);
    } /* end if */
    else
        vl.p = NULL;

    /* Set the sequence length */
    vl.len = seq_len;

    /* Set pointer in user's buffer with memcpy, to avoid alignment issues */
    H5MM_memcpy(_vl, &vl, sizeof(hvl_t));

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__vlen_mem_seq_write() */

/*-------------------------------------------------------------------------
 * Function:	H5T__vlen_mem_str_getlen
 *
 * Purpose:	Retrieves the length of a memory based VL string.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__vlen_mem_str_getlen(H5VL_object_t H5_ATTR_UNUSED *file, const void *_vl, size_t *len)
{
    const char *s = NULL; /* Pointer to the user's string information */

    FUNC_ENTER_PACKAGE_NOERR

    assert(_vl);

    /* Copy to ensure correct alignment */
    H5MM_memcpy(&s, _vl, sizeof(char *));

    *len = strlen(s);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5T__vlen_mem_str_getlen() */

/*-------------------------------------------------------------------------
 * Function:	H5T__vlen_mem_str_getptr
 *
 * Purpose:	Retrieves the pointer for a memory based VL string.
 *
 * Return:	Non-NULL on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
static void *
H5T__vlen_mem_str_getptr(void *_vl)
{
    char *s = NULL; /* Pointer to the user's string information */

    FUNC_ENTER_PACKAGE_NOERR

    assert(_vl);

    /* Copy to ensure correct alignment */
    H5MM_memcpy(&s, _vl, sizeof(char *));

    FUNC_LEAVE_NOAPI(s)
} /* end H5T__vlen_mem_str_getptr() */

/*-------------------------------------------------------------------------
 * Function:	H5T__vlen_mem_str_isnull
 *
 * Purpose:	Checks if a memory string is a NULL pointer
 *
 * Return:	Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__vlen_mem_str_isnull(const H5VL_object_t H5_ATTR_UNUSED *file, void *_vl, bool *isnull)
{
    char *s = NULL; /* Pointer to the user's string information */

    FUNC_ENTER_PACKAGE_NOERR

    /* Copy to ensure correct alignment */
    H5MM_memcpy(&s, _vl, sizeof(char *));

    *isnull = (s == NULL ? true : false);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5T__vlen_mem_str_isnull() */

/*-------------------------------------------------------------------------
 * Function:	H5T__vlen_mem_str_setnull
 *
 * Purpose:	Sets a VL info object in memory to the "null" value
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__vlen_mem_str_setnull(H5VL_object_t H5_ATTR_UNUSED *file, void *_vl, void H5_ATTR_UNUSED *_bg)
{
    char *t = NULL; /* Pointer to temporary buffer allocated */

    FUNC_ENTER_PACKAGE_NOERR

    /* Set pointer in user's buffer with memcpy, to avoid alignment issues */
    H5MM_memcpy(_vl, &t, sizeof(char *));

    FUNC_LEAVE_NOAPI(SUCCEED) /*lint !e429 The pointer in 't' has been copied */
} /* end H5T__vlen_mem_str_setnull() */

/*-------------------------------------------------------------------------
 * Function:	H5T__vlen_mem_str_read
 *
 * Purpose:	"Reads" the memory based VL string into a buffer
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__vlen_mem_str_read(H5VL_object_t H5_ATTR_UNUSED *file, void *_vl, void *buf, size_t len)
{
    char *s; /* Pointer to the user's string information */

    FUNC_ENTER_PACKAGE_NOERR

    if (len > 0) {
        assert(buf);
        assert(_vl);

        /* Copy to ensure correct alignment */
        H5MM_memcpy(&s, _vl, sizeof(char *));
        H5MM_memcpy(buf, s, len);
    }

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5T__vlen_mem_str_read() */

/*-------------------------------------------------------------------------
 * Function:	H5T__vlen_mem_str_write
 *
 * Purpose:	"Writes" the memory based VL string from a buffer
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__vlen_mem_str_write(H5VL_object_t H5_ATTR_UNUSED *file, const H5T_vlen_alloc_info_t *vl_alloc_info,
                        void *_vl, void *buf, void H5_ATTR_UNUSED *_bg, size_t seq_len, size_t base_size)
{
    char  *t;                   /* Pointer to temporary buffer allocated */
    size_t len;                 /* Maximum length of the string to copy */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check parameters */
    assert(buf);

    /* Use the user's memory allocation routine if one is defined */
    if (vl_alloc_info->alloc_func != NULL) {
        if (NULL ==
            (t = (char *)(vl_alloc_info->alloc_func)((seq_len + 1) * base_size, vl_alloc_info->alloc_info)))
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTALLOC, FAIL,
                        "application memory allocation routine failed for VL data");
    }    /* end if */
    else /* Default to system malloc */
        if (NULL == (t = (char *)malloc((seq_len + 1) * base_size)))
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTALLOC, FAIL, "memory allocation failed for VL data");

    /* 'write' the string into the buffer, with memcpy() */
    len = (seq_len * base_size);
    H5MM_memcpy(t, buf, len);
    t[len] = '\0';

    /* Set pointer in user's buffer with memcpy, to avoid alignment issues */
    H5MM_memcpy(_vl, &t, sizeof(char *));

done:
    FUNC_LEAVE_NOAPI(ret_value) /*lint !e429 The pointer in 't' has been copied */
} /* end H5T__vlen_mem_str_write() */

/*-------------------------------------------------------------------------
 * Function:	H5T__vlen_disk_getlen
 *
 * Purpose:	Retrieves the length of a disk based VL element.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__vlen_disk_getlen(H5VL_object_t H5_ATTR_UNUSED *file, const void *_vl, size_t *seq_len)
{
    const uint8_t *vl = (const uint8_t *)_vl; /* Pointer to the user's hvl_t information */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check parameters */
    assert(vl);
    assert(seq_len);

    /* Get length of sequence (different from blob size) */
    UINT32DECODE(vl, *seq_len);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5T__vlen_disk_getlen() */

/*-------------------------------------------------------------------------
 * Function:	H5T__vlen_disk_isnull
 *
 * Purpose:	Checks if a disk VL info object is the "nil" object
 *
 * Return:	Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__vlen_disk_isnull(const H5VL_object_t *file, void *_vl, bool *isnull)
{
    H5VL_blob_specific_args_t vol_cb_args;                /* Arguments to VOL callback */
    uint8_t                  *vl        = (uint8_t *)_vl; /* Pointer to the user's hvl_t information */
    herr_t                    ret_value = SUCCEED;        /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check parameters */
    assert(file);
    assert(vl);
    assert(isnull);

    /* Skip the sequence's length */
    vl += 4;

    /* Set up VOL callback arguments */
    vol_cb_args.op_type             = H5VL_BLOB_ISNULL;
    vol_cb_args.args.is_null.isnull = isnull;

    /* Check if blob ID is "nil" */
    if (H5VL_blob_specific(file, vl, &vol_cb_args) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "unable to check if a blob ID is 'nil'");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__vlen_disk_isnull() */

/*-------------------------------------------------------------------------
 * Function:	H5T__vlen_disk_setnull
 *
 * Purpose:	Sets a VL info object on disk to the "nil" value
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__vlen_disk_setnull(H5VL_object_t *file, void *_vl, void *bg)
{
    H5VL_blob_specific_args_t vol_cb_args;                /* Arguments to VOL callback */
    uint8_t                  *vl        = (uint8_t *)_vl; /* Pointer to the user's hvl_t information */
    herr_t                    ret_value = SUCCEED;        /* Return value */

    FUNC_ENTER_PACKAGE

    /* check parameters */
    assert(file);
    assert(vl);

    /* Free heap object for old data */
    if (bg != NULL)
        /* Delete sequence in destination location */
        if (H5T__vlen_disk_delete(file, bg) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREMOVE, FAIL, "unable to remove background heap object");

    /* Set the length of the sequence */
    UINT32ENCODE(vl, 0);

    /* Set up VOL callback arguments */
    vol_cb_args.op_type = H5VL_BLOB_SETNULL;

    /* Set blob ID to "nil" */
    if (H5VL_blob_specific(file, vl, &vol_cb_args) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTSET, FAIL, "unable to set a blob ID to 'nil'");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__vlen_disk_setnull() */

/*-------------------------------------------------------------------------
 * Function:	H5T__vlen_disk_read
 *
 * Purpose:	Reads the disk based VL element into a buffer
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__vlen_disk_read(H5VL_object_t *file, void *_vl, void *buf, size_t len)
{
    const uint8_t *vl        = (const uint8_t *)_vl; /* Pointer to the user's hvl_t information */
    herr_t         ret_value = SUCCEED;              /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check parameters */
    assert(file);
    assert(vl);
    assert(buf);

    /* Skip the length of the sequence */
    vl += 4;

    /* Retrieve blob */
    if (H5VL_blob_get(file, vl, buf, len, NULL) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "unable to get blob");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__vlen_disk_read() */

/*-------------------------------------------------------------------------
 * Function:	H5T__vlen_disk_write
 *
 * Purpose:	Writes the disk based VL element from a buffer
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__vlen_disk_write(H5VL_object_t *file, const H5T_vlen_alloc_info_t H5_ATTR_UNUSED *vl_alloc_info,
                     void *_vl, void *buf, void *_bg, size_t seq_len, size_t base_size)
{
    uint8_t *vl        = (uint8_t *)_vl; /* Pointer to the user's hvl_t information */
    uint8_t *bg        = (uint8_t *)_bg; /* Pointer to the old data hvl_t */
    herr_t   ret_value = SUCCEED;        /* Return value */

    FUNC_ENTER_PACKAGE

    /* check parameters */
    assert(vl);
    assert(seq_len == 0 || buf);
    assert(file);

    /* Free heap object for old data, if non-NULL */
    if (bg != NULL)
        if (H5T__vlen_disk_delete(file, bg) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREMOVE, FAIL, "unable to remove background heap object");

    /* Set the length of the sequence */
    UINT32ENCODE(vl, seq_len);

    /* Store blob */
    if (H5VL_blob_put(file, buf, (seq_len * base_size), vl, NULL) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTSET, FAIL, "unable to put blob");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__vlen_disk_write() */

/*-------------------------------------------------------------------------
 * Function:	H5T__vlen_disk_delete
 *
 * Purpose:	Deletes a disk-based VL element
 *
 * Return:	Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5T__vlen_disk_delete(H5VL_object_t *file, void *_vl)
{
    uint8_t *vl        = (uint8_t *)_vl; /* Pointer to the user's hvl_t information */
    herr_t   ret_value = SUCCEED;        /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check parameters */
    assert(file);

    /* Free heap object for old data */
    if (vl != NULL) {
        size_t seq_len; /* VL sequence's length */

        /* Get length of sequence */
        UINT32DECODE(vl, seq_len);

        /* Delete object, if length > 0 */
        if (seq_len > 0) {
            H5VL_blob_specific_args_t vol_cb_args; /* Arguments to VOL callback */

            /* Set up VOL callback arguments */
            vol_cb_args.op_type = H5VL_BLOB_DELETE;

            if (H5VL_blob_specific(file, vl, &vol_cb_args) < 0)
                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREMOVE, FAIL, "unable to delete blob");
        } /* end if */
    }     /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__vlen_disk_delete() */

/*-------------------------------------------------------------------------
 * Function:    H5T__vlen_reclaim
 *
 * Purpose: Internal recursive routine to free VL datatypes
 *
 * Return:  Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5T__vlen_reclaim(void *elem, const H5T_t *dt, H5T_vlen_alloc_info_t *alloc_info)
{
    unsigned    u;                   /* Local index variable */
    H5MM_free_t free_func;           /* Free function */
    void       *free_info;           /* Free info */
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(elem);
    assert(dt);
    assert(alloc_info);

    free_func = alloc_info->free_func;
    free_info = alloc_info->free_info;

    /* Check the datatype of this element */
    switch (dt->shared->type) {
        case H5T_ARRAY:
            /* Recurse on each element, if the array's base type is array, VL, enum or compound */
            if (H5T_IS_COMPLEX(dt->shared->parent->shared->type)) {
                void *off; /* offset of field */

                /* Calculate the offset member and recurse on it */
                for (u = 0; u < dt->shared->u.array.nelem; u++) {
                    off = ((uint8_t *)elem) + u * (dt->shared->parent->shared->size);
                    if (H5T_reclaim_cb(off, dt->shared->parent, 0, NULL, alloc_info) < 0)
                        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTFREE, FAIL, "unable to free array element");
                } /* end for */
            }     /* end if */
            break;

        case H5T_COMPOUND:
            /* Check each field and recurse on VL, compound, enum or array ones */
            for (u = 0; u < dt->shared->u.compnd.nmembs; u++) {
                /* Recurse if it's VL, compound, enum or array */
                if (H5T_IS_COMPLEX(dt->shared->u.compnd.memb[u].type->shared->type)) {
                    void *off; /* offset of field */

                    /* Calculate the offset member and recurse on it */
                    off = ((uint8_t *)elem) + dt->shared->u.compnd.memb[u].offset;
                    if (H5T_reclaim_cb(off, dt->shared->u.compnd.memb[u].type, 0, NULL, alloc_info) < 0)
                        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTFREE, FAIL, "unable to free compound field");
                } /* end if */
            }     /* end for */
            break;

        case H5T_VLEN:
            /* Recurse on the VL information if it's VL, compound, enum or array, then free VL sequence */
            if (dt->shared->u.vlen.type == H5T_VLEN_SEQUENCE) {
                hvl_t *vl = (hvl_t *)elem; /* Temp. ptr to the vl info */

                /* Check if there is anything actually in this sequence */
                if (vl->len != 0) {
                    /* Recurse if it's VL, array, enum or compound */
                    if (H5T_IS_COMPLEX(dt->shared->parent->shared->type)) {
                        void *off; /* offset of field */

                        /* Calculate the offset of each array element and recurse on it */
                        while (vl->len > 0) {
                            off = ((uint8_t *)vl->p) + (vl->len - 1) * dt->shared->parent->shared->size;
                            if (H5T_reclaim_cb(off, dt->shared->parent, 0, NULL, alloc_info) < 0)
                                HGOTO_ERROR(H5E_DATATYPE, H5E_CANTFREE, FAIL, "unable to free VL element");
                            vl->len--;
                        } /* end while */
                    }     /* end if */

                    /* Free the VL sequence */
                    if (free_func != NULL)
                        (*free_func)(vl->p, free_info);
                    else
                        free(vl->p);
                } /* end if */
            }
            else if (dt->shared->u.vlen.type == H5T_VLEN_STRING) {
                /* Free the VL string */
                if (free_func != NULL)
                    (*free_func)(*(char **)elem, free_info);
                else
                    free(*(char **)elem);
            }
            else {
                assert(0 && "Invalid VL type");
            } /* end else */
            break;

        /* Don't do anything for simple types */
        case H5T_INTEGER:
        case H5T_FLOAT:
        case H5T_TIME:
        case H5T_STRING:
        case H5T_BITFIELD:
        case H5T_OPAQUE:
        case H5T_ENUM:
            break;

        /* Should never have these values */
        case H5T_REFERENCE:
        case H5T_NO_CLASS:
        case H5T_NCLASSES:
        default:
            HGOTO_ERROR(H5E_DATATYPE, H5E_BADRANGE, FAIL, "invalid VL datatype class");
            break;

    } /* end switch */ /*lint !e788 All appropriate cases are covered */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5T__vlen_reclaim() */

/*-------------------------------------------------------------------------
 * Function:	H5T_vlen_reclaim_elmt
 *
 * Purpose: Alternative method to reclaim any VL data for a buffer element.
 *
 *          Use this function when the datatype is already available, but
 *          the allocation info is needed from the context before jumping
 *          into recursion.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5T_vlen_reclaim_elmt(void *elem, H5T_t *dt)
{
    H5T_vlen_alloc_info_t vl_alloc_info;       /* VL allocation info */
    herr_t                ret_value = SUCCEED; /* return value */

    assert(dt);
    assert(elem);

    FUNC_ENTER_NOAPI(FAIL)

    /* Get VL allocation info */
    if (H5CX_get_vlen_alloc_info(&vl_alloc_info) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTGET, FAIL, "unable to retrieve VL allocation info");

    /* Recurse on buffer to free dynamic fields */
    if (H5T__vlen_reclaim(elem, dt, &vl_alloc_info) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTFREE, FAIL, "can't reclaim vlen elements");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5T_vlen_reclaim_elmt() */
