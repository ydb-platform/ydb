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
 * Created:		H5Fsuper_cache.c
 *
 * Purpose:		Implement file superblock & driver info metadata cache methods
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Fmodule.h" /* This source code file is part of the H5F module */
#define H5G_FRIEND     /*suppress error about including H5Gpkg	  */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Fpkg.h"      /* File access				*/
#include "H5FDprivate.h" /* File drivers				*/
#include "H5FLprivate.h" /* Free Lists                           */
#include "H5Gpkg.h"      /* Groups		  		*/
#include "H5Iprivate.h"  /* IDs			  		*/
#include "H5MMprivate.h" /* Memory management                    */
#include "H5Pprivate.h"  /* Property lists			*/

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

/* Metadata cache (H5AC) callbacks */
static herr_t H5F__cache_superblock_get_initial_load_size(void *udata, size_t *image_len);
static herr_t H5F__cache_superblock_get_final_load_size(const void *image_ptr, size_t image_len, void *udata,
                                                        size_t *actual_len);
static htri_t H5F__cache_superblock_verify_chksum(const void *image_ptr, size_t len, void *udata_ptr);
static void  *H5F__cache_superblock_deserialize(const void *image, size_t len, void *udata, bool *dirty);
static herr_t H5F__cache_superblock_image_len(const void *thing, size_t *image_len);
static herr_t H5F__cache_superblock_serialize(const H5F_t *f, void *image, size_t len, void *thing);
static herr_t H5F__cache_superblock_free_icr(void *thing);

static herr_t H5F__cache_drvrinfo_get_initial_load_size(void *udata, size_t *image_len);
static herr_t H5F__cache_drvrinfo_get_final_load_size(const void *image_ptr, size_t image_len, void *udata,
                                                      size_t *actual_len);
static void  *H5F__cache_drvrinfo_deserialize(const void *image, size_t len, void *udata, bool *dirty);
static herr_t H5F__cache_drvrinfo_image_len(const void *thing, size_t *image_len);
static herr_t H5F__cache_drvrinfo_serialize(const H5F_t *f, void *image, size_t len, void *thing);
static herr_t H5F__cache_drvrinfo_free_icr(void *thing);

/* Local encode/decode routines */
static herr_t H5F__superblock_prefix_decode(H5F_super_t *sblock, const uint8_t **image_ref, size_t len,
                                            const H5F_superblock_cache_ud_t *udata, bool extend_eoa);
static herr_t H5F__drvrinfo_prefix_decode(H5O_drvinfo_t *drvinfo, char *drv_name, const uint8_t **image_ref,
                                          size_t len, H5F_drvrinfo_cache_ud_t *udata, bool extend_eoa);

/*********************/
/* Package Variables */
/*********************/

/* H5F superblock inherits cache-like properties from H5AC */
const H5AC_class_t H5AC_SUPERBLOCK[1] = {{
    H5AC_SUPERBLOCK_ID,                          /* Metadata client ID */
    "Superblock",                                /* Metadata client name (for debugging) */
    H5FD_MEM_SUPER,                              /* File space memory type for client */
    H5AC__CLASS_SPECULATIVE_LOAD_FLAG,           /* Client class behavior flags */
    H5F__cache_superblock_get_initial_load_size, /* 'get_initial_load_size' callback */
    H5F__cache_superblock_get_final_load_size,   /* 'get_final_load_size' callback */
    H5F__cache_superblock_verify_chksum,         /* 'verify_chksum' callback */
    H5F__cache_superblock_deserialize,           /* 'deserialize' callback */
    H5F__cache_superblock_image_len,             /* 'image_len' callback */
    NULL,                                        /* 'pre_serialize' callback */
    H5F__cache_superblock_serialize,             /* 'serialize' callback */
    NULL,                                        /* 'notify' callback */
    H5F__cache_superblock_free_icr,              /* 'free_icr' callback */
    NULL,                                        /* 'fsf_size' callback */
}};

/* H5F driver info block inherits cache-like properties from H5AC */
const H5AC_class_t H5AC_DRVRINFO[1] = {{
    H5AC_DRVRINFO_ID,                          /* Metadata client ID */
    "Driver info block",                       /* Metadata client name (for debugging) */
    H5FD_MEM_SUPER,                            /* File space memory type for client */
    H5AC__CLASS_SPECULATIVE_LOAD_FLAG,         /* Client class behavior flags */
    H5F__cache_drvrinfo_get_initial_load_size, /* 'get_initial_load_size' callback */
    H5F__cache_drvrinfo_get_final_load_size,   /* 'get_final_load_size' callback */
    NULL,                                      /* 'verify_chksum' callback */
    H5F__cache_drvrinfo_deserialize,           /* 'deserialize' callback */
    H5F__cache_drvrinfo_image_len,             /* 'image_len' callback */
    NULL,                                      /* 'pre_serialize' callback */
    H5F__cache_drvrinfo_serialize,             /* 'serialize' callback */
    NULL,                                      /* 'notify' callback */
    H5F__cache_drvrinfo_free_icr,              /* 'free_icr' callback */
    NULL,                                      /* 'fsf_size' callback */
}};

/*****************************/
/* Library Private Variables */
/*****************************/

/* Declare extern the free list to manage the H5F_super_t struct */
H5FL_EXTERN(H5F_super_t);

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:    H5F__superblock_prefix_decode
 *
 * Purpose:     Decode a superblock prefix
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5F__superblock_prefix_decode(H5F_super_t *sblock, const uint8_t **image_ref, size_t len,
                              const H5F_superblock_cache_ud_t *udata, bool extend_eoa)
{
    const uint8_t *image     = (const uint8_t *)*image_ref; /* Pointer into raw data buffer */
    const uint8_t *end       = image + len - 1;             /* Pointer to end of buffer */
    htri_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(sblock);
    assert(image_ref);
    assert(image);
    assert(udata);
    assert(udata->f);

    /* Skip over signature (already checked when locating the superblock) */
    if (H5_IS_BUFFER_OVERFLOW(image, H5F_SIGNATURE_LEN, end))
        HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
    image += H5F_SIGNATURE_LEN;

    /* Superblock version */
    if (H5_IS_BUFFER_OVERFLOW(image, 1, end))
        HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
    sblock->super_vers = *image++;
    if (sblock->super_vers > HDF5_SUPERBLOCK_VERSION_LATEST)
        HGOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "bad superblock version number");

    /* Size check */
    if (((size_t)(image - (const uint8_t *)*image_ref)) != H5F_SUPERBLOCK_FIXED_SIZE)
        HGOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "bad superblock (fixed) size");

    /* Determine the size of addresses & size of offsets, for computing the
     * variable-sized portion of the superblock.
     */
    if (sblock->super_vers < HDF5_SUPERBLOCK_VERSION_2) {
        if (H5_IS_BUFFER_OVERFLOW(image, 6, end))
            HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
        sblock->sizeof_addr = image[4];
        sblock->sizeof_size = image[5];
    }
    else {
        if (H5_IS_BUFFER_OVERFLOW(image, 2, end))
            HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
        sblock->sizeof_addr = image[0];
        sblock->sizeof_size = image[1];
    }

    if (sblock->sizeof_addr != 2 && sblock->sizeof_addr != 4 && sblock->sizeof_addr != 8 &&
        sblock->sizeof_addr != 16 && sblock->sizeof_addr != 32)
        HGOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "bad byte number in an address");
    if (sblock->sizeof_size != 2 && sblock->sizeof_size != 4 && sblock->sizeof_size != 8 &&
        sblock->sizeof_size != 16 && sblock->sizeof_size != 32)
        HGOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "bad byte number for object size");

    /* Check for extending the EOA for the file */
    if (extend_eoa) {
        size_t variable_size; /* Variable size of superblock */

        /* Determine the size of the variable-length part of the superblock */
        variable_size =
            (size_t)H5F_SUPERBLOCK_VARLEN_SIZE(sblock->super_vers, sblock->sizeof_addr, sblock->sizeof_size);
        if (variable_size == 0)
            HGOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "variable size can't be zero");

        /* Make certain we can read the variable-sized portion of the superblock */
        if (H5F__set_eoa(udata->f, H5FD_MEM_SUPER, (haddr_t)(H5F_SUPERBLOCK_FIXED_SIZE + variable_size)) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "set end of space allocation request failed");
    }

    /* Update the image buffer pointer */
    *image_ref = image;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__superblock_prefix_decode() */

/*-------------------------------------------------------------------------
 * Function:    H5F__drvrinfo_prefix_decode
 *
 * Purpose:     Decode a driver info prefix
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5F__drvrinfo_prefix_decode(H5O_drvinfo_t *drvrinfo, char *drv_name, const uint8_t **image_ref, size_t len,
                            H5F_drvrinfo_cache_ud_t *udata, bool extend_eoa)
{
    const uint8_t *image = (const uint8_t *)*image_ref; /* Pointer into raw data buffer */
    const uint8_t *end   = image + len - 1;             /* Pointer to end of buffer */
    unsigned       drv_vers;                            /* Version of driver info block */
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(drvrinfo);
    assert(image_ref);
    assert(image);
    assert(udata);
    assert(udata->f);

    /* Version number */
    if (H5_IS_BUFFER_OVERFLOW(image, 1, end))
        HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
    drv_vers = *image++;
    if (drv_vers != HDF5_DRIVERINFO_VERSION_0)
        HGOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "bad driver information block version number");

    /* Reserved bytes */
    if (H5_IS_BUFFER_OVERFLOW(image, 3, end))
        HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
    image += 3;

    /* Driver info size */
    if (H5_IS_BUFFER_OVERFLOW(image, 4, end))
        HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
    UINT32DECODE(image, drvrinfo->len);

    /* Driver name and/or version */
    if (drv_name) {
        if (H5_IS_BUFFER_OVERFLOW(image, 8, end))
            HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
        H5MM_memcpy(drv_name, (const char *)image, (size_t)8);
        drv_name[8] = '\0';
        image += 8; /* advance past name/version */
    }

    /* Extend the EOA if required so that we can read the complete driver info block */
    if (extend_eoa) {
        haddr_t eoa;     /* Current EOA for the file */
        haddr_t min_eoa; /* Minimum EOA needed for reading the driver info */

        /* Get current EOA... */
        eoa = H5FD_get_eoa(udata->f->shared->lf, H5FD_MEM_SUPER);
        if (!H5_addr_defined(eoa))
            HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "driver get_eoa request failed");

        /* ... if it is too small, extend it. */
        min_eoa = udata->driver_addr + H5F_DRVINFOBLOCK_HDR_SIZE + drvrinfo->len;

        /* If it grew, set it */
        if (H5_addr_gt(min_eoa, eoa))
            if (H5FD_set_eoa(udata->f->shared->lf, H5FD_MEM_SUPER, min_eoa) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "set end of space allocation request failed");
    }

    /* Update the image buffer pointer */
    *image_ref = image;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__drvrinfo_prefix_decode() */

/*-------------------------------------------------------------------------
 * Function:    H5F__cache_superblock_get_initial_load_size
 *
 * Purpose:     Compute the size of the data structure on disk
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5F__cache_superblock_get_initial_load_size(void H5_ATTR_UNUSED *_udata, size_t *image_len)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(image_len);

    /* Set the initial image length size */
    *image_len = H5F_SUPERBLOCK_FIXED_SIZE + /* Fixed size of superblock */
                 H5F_SUPERBLOCK_MINIMAL_VARLEN_SIZE;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5F__cache_superblock_get_initial_load_size() */

/*-------------------------------------------------------------------------
 * Function:    H5F__cache_superblock_get_final_load_size
 *
 * Purpose:     Compute the final size of the data structure on disk
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5F__cache_superblock_get_final_load_size(const void *_image, size_t image_len, void *_udata,
                                          size_t *actual_len)
{
    const uint8_t             *image = _image;                              /* Pointer into raw data buffer */
    H5F_superblock_cache_ud_t *udata = (H5F_superblock_cache_ud_t *)_udata; /* User data */
    H5F_super_t                sblock;                                      /* Temporary file superblock */
    htri_t                     ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(image);
    assert(udata);
    assert(actual_len);
    assert(*actual_len == image_len);
    assert(image_len >= H5F_SUPERBLOCK_FIXED_SIZE + 6);

    /* Deserialize the file superblock's prefix */
    if (H5F__superblock_prefix_decode(&sblock, &image, image_len, udata, true) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTDECODE, FAIL, "can't decode file superblock prefix");

    /* Save the version to be used in verify_chksum callback */
    udata->super_vers = sblock.super_vers;

    /* Set the final size for the cache image */
    *actual_len = H5F_SUPERBLOCK_FIXED_SIZE + (size_t)H5F_SUPERBLOCK_VARLEN_SIZE(
                                                  sblock.super_vers, sblock.sizeof_addr, sblock.sizeof_size);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__cache_superblock_get_final_load_size() */

/*-------------------------------------------------------------------------
 * Function:    H5F__cache_superblock_verify_chksum
 *
 * Purpose:     Verify the computed checksum of the data structure is the
 *              same as the stored chksum.
 *
 * Return:      Success:    true/false
 *              Failure:    Negative
 *-------------------------------------------------------------------------
 */
static htri_t
H5F__cache_superblock_verify_chksum(const void *_image, size_t len, void *_udata)
{
    const uint8_t             *image = _image;                              /* Pointer into raw data buffer */
    H5F_superblock_cache_ud_t *udata = (H5F_superblock_cache_ud_t *)_udata; /* User data */
    uint32_t                   stored_chksum;   /* Stored metadata checksum value */
    uint32_t                   computed_chksum; /* Computed metadata checksum value */
    htri_t                     ret_value = true;

    FUNC_ENTER_PACKAGE_NOERR

    assert(image);
    assert(udata);

    /* No checksum for version 0 & 1 */
    if (udata->super_vers >= HDF5_SUPERBLOCK_VERSION_2) {

        /* Get stored and computed checksums */
        H5F_get_checksums(image, len, &stored_chksum, &computed_chksum);

        if (stored_chksum != computed_chksum)
            ret_value = false;
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__cache_superblock_verify_chksum() */

/*-------------------------------------------------------------------------
 * Function:    H5F__cache_superblock_deserialize
 *
 * Purpose:     Load an object from the disk
 *
 * Return:      Success:    Pointer to new object
 *              Failure:    NULL
 *-------------------------------------------------------------------------
 */
static void *
H5F__cache_superblock_deserialize(const void *_image, size_t len, void *_udata, bool H5_ATTR_UNUSED *dirty)
{
    H5F_super_t               *sblock    = NULL;                                /* File's superblock */
    H5F_superblock_cache_ud_t *udata     = (H5F_superblock_cache_ud_t *)_udata; /* User data */
    const uint8_t             *image     = _image;          /* Pointer into raw data buffer */
    const uint8_t             *end       = image + len - 1; /* Pointer to end of buffer */
    H5F_super_t               *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    assert(image);
    assert(udata);
    assert(udata->f);
    assert(len >= H5F_SUPERBLOCK_FIXED_SIZE + 6);

    /* Allocate space for the superblock */
    if (NULL == (sblock = H5FL_CALLOC(H5F_super_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Deserialize the file superblock's prefix */
    if (H5F__superblock_prefix_decode(sblock, &image, len, udata, false) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTDECODE, NULL, "can't decode file superblock prefix");

    /* Check for older version of superblock format */
    if (sblock->super_vers < HDF5_SUPERBLOCK_VERSION_2) {
        uint32_t status_flags;  /* File status flags	   */
        unsigned sym_leaf_k;    /* Symbol table leaf node's 'K' value */
        unsigned snode_btree_k; /* B-tree symbol table internal node 'K' value */
        unsigned chunk_btree_k; /* B-tree chunk internal node 'K' value */

        /* Freespace version (hard-wired) */
        if (H5_IS_BUFFER_OVERFLOW(image, 1, end))
            HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, NULL, "image pointer is out of bounds");
        if (HDF5_FREESPACE_VERSION != *image++)
            HGOTO_ERROR(H5E_FILE, H5E_BADVALUE, NULL, "bad free space version number");

        /* Root group version number (hard-wired) */
        if (H5_IS_BUFFER_OVERFLOW(image, 1, end))
            HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, NULL, "image pointer is out of bounds");
        if (HDF5_OBJECTDIR_VERSION != *image++)
            HGOTO_ERROR(H5E_FILE, H5E_BADVALUE, NULL, "bad object directory version number");

        /* Skip over reserved byte */
        if (H5_IS_BUFFER_OVERFLOW(image, 1, end))
            HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, NULL, "image pointer is out of bounds");
        image++;

        /* Shared header version number (hard-wired) */
        if (H5_IS_BUFFER_OVERFLOW(image, 1, end))
            HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, NULL, "image pointer is out of bounds");
        if (HDF5_SHAREDHEADER_VERSION != *image++)
            HGOTO_ERROR(H5E_FILE, H5E_BADVALUE, NULL, "bad shared-header format version number");

        /* Skip over size of file addresses (already decoded) */
        if (H5_IS_BUFFER_OVERFLOW(image, 1, end))
            HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, NULL, "image pointer is out of bounds");
        image++;
        udata->f->shared->sizeof_addr = sblock->sizeof_addr; /* Keep a local copy also */

        /* Skip over size of file sizes (already decoded) */
        if (H5_IS_BUFFER_OVERFLOW(image, 1, end))
            HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, NULL, "image pointer is out of bounds");
        image++;
        udata->f->shared->sizeof_size = sblock->sizeof_size; /* Keep a local copy also */

        /* Skip over reserved byte */
        if (H5_IS_BUFFER_OVERFLOW(image, 1, end))
            HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, NULL, "image pointer is out of bounds");
        image++;

        /* Various B-tree sizes */
        if (H5_IS_BUFFER_OVERFLOW(image, 2, end))
            HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, NULL, "image pointer is out of bounds");
        UINT16DECODE(image, sym_leaf_k);
        if (sym_leaf_k == 0)
            HGOTO_ERROR(H5E_FILE, H5E_BADRANGE, NULL, "bad symbol table leaf node 1/2 rank");
        udata->sym_leaf_k = sym_leaf_k; /* Keep a local copy also */

        /* Need 'get' call to set other array values */
        if (H5_IS_BUFFER_OVERFLOW(image, 2, end))
            HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, NULL, "image pointer is out of bounds");
        UINT16DECODE(image, snode_btree_k);
        if (snode_btree_k == 0)
            HGOTO_ERROR(H5E_FILE, H5E_BADRANGE, NULL, "bad 1/2 rank for btree internal nodes");
        udata->btree_k[H5B_SNODE_ID] = snode_btree_k;

        /* Delay setting the value in the property list until we've checked
         * for the indexed storage B-tree internal 'K' value later.
         */

        /* File status flags (not really used yet) */
        if (H5_IS_BUFFER_OVERFLOW(image, 4, end))
            HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, NULL, "image pointer is out of bounds");
        UINT32DECODE(image, status_flags);
        if (status_flags > 255)
            HGOTO_ERROR(H5E_FILE, H5E_BADVALUE, NULL, "bad superblock status flags");
        sblock->status_flags = (uint8_t)status_flags;
        if (sblock->status_flags & ~H5F_SUPER_ALL_FLAGS)
            HGOTO_ERROR(H5E_FILE, H5E_BADVALUE, NULL, "bad flag value for superblock");

        /* If the superblock version # is greater than 0, read in the indexed
         * storage B-tree internal 'K' value
         */
        if (sblock->super_vers > HDF5_SUPERBLOCK_VERSION_DEF) {
            if (H5_IS_BUFFER_OVERFLOW(image, 2, end))
                HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, NULL, "image pointer is out of bounds");
            UINT16DECODE(image, chunk_btree_k);

            /* Reserved bytes are present only in version 1 */
            if (sblock->super_vers == HDF5_SUPERBLOCK_VERSION_1) {
                /* Reserved */
                if (H5_IS_BUFFER_OVERFLOW(image, 2, end))
                    HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, NULL, "image pointer is out of bounds");
                image += 2;
            }
        }
        else
            chunk_btree_k = HDF5_BTREE_CHUNK_IK_DEF;
        udata->btree_k[H5B_CHUNK_ID] = chunk_btree_k;

        /* Remainder of "variable-sized" portion of superblock */
        if (H5_IS_BUFFER_OVERFLOW(image, H5F_sizeof_addr(udata->f) * 4, end))
            HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, NULL, "image pointer is out of bounds");
        H5F_addr_decode(udata->f, (const uint8_t **)&image, &sblock->base_addr /*out*/);
        H5F_addr_decode(udata->f, (const uint8_t **)&image, &sblock->ext_addr /*out*/);
        H5F_addr_decode(udata->f, (const uint8_t **)&image, &udata->stored_eof /*out*/);
        H5F_addr_decode(udata->f, (const uint8_t **)&image, &sblock->driver_addr /*out*/);

        /* Allocate space for the root group symbol table entry */
        if (sblock->root_ent)
            HGOTO_ERROR(H5E_FILE, H5E_BADVALUE, NULL, "root entry should not exist yet");
        if (NULL == (sblock->root_ent = (H5G_entry_t *)H5MM_calloc(sizeof(H5G_entry_t))))
            HGOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL,
                        "can't allocate space for root group symbol table entry");

        /* Decode the root group symbol table entry */
        if (H5G_ent_decode(udata->f, (const uint8_t **)&image, sblock->root_ent, end) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTDECODE, NULL, "can't decode root group symbol table entry");

        /* Set the root group address to the correct value */
        sblock->root_addr = sblock->root_ent->header;

        /* This step is for h5repart tool only. If user wants to change file driver
         *  from family to sec2 while using h5repart, set the driver address to
         *  undefined to let the library ignore the family driver information saved
         *  in the superblock.
         */
        if (udata->ignore_drvrinfo && H5_addr_defined(sblock->driver_addr)) {
            /* Eliminate the driver info */
            sblock->driver_addr     = HADDR_UNDEF;
            udata->drvrinfo_removed = true;
        }

        /* NOTE: Driver info block is decoded separately, later */
    }
    else {
        uint32_t read_chksum; /* Checksum read from file  */

        /* Skip over size of file addresses (already decoded) */
        image++;
        udata->f->shared->sizeof_addr = sblock->sizeof_addr; /* Keep a local copy also */
        /* Skip over size of file sizes (already decoded) */
        image++;
        udata->f->shared->sizeof_size = sblock->sizeof_size; /* Keep a local copy also */

        /* Check whether the image pointer is out of bounds */
        if (H5_IS_BUFFER_OVERFLOW(image, 1, end))
            HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, NULL, "image pointer is out of bounds");

        /* File status flags (not really used yet) */
        sblock->status_flags = *image++;
        if (sblock->status_flags & ~H5F_SUPER_ALL_FLAGS)
            HGOTO_ERROR(H5E_FILE, H5E_BADVALUE, NULL, "bad flag value for superblock");

        /* Check whether the image pointer will be out of bounds */
        if (H5_IS_BUFFER_OVERFLOW(image, H5F_SIZEOF_ADDR(udata->f) * 4, end))
            HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, NULL, "image pointer is out of bounds");

        /* Base, superblock extension, end of file & root group object header addresses */
        H5F_addr_decode(udata->f, (const uint8_t **)&image, &sblock->base_addr /*out*/);
        H5F_addr_decode(udata->f, (const uint8_t **)&image, &sblock->ext_addr /*out*/);
        H5F_addr_decode(udata->f, (const uint8_t **)&image, &udata->stored_eof /*out*/);
        H5F_addr_decode(udata->f, (const uint8_t **)&image, &sblock->root_addr /*out*/);

        /* checksum verification already done in verify_chksum cb */

        /* Check whether the image pointer will be out of bounds */
        if (H5_IS_BUFFER_OVERFLOW(image, sizeof(uint32_t), end))
            HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, NULL, "image pointer is out of bounds");

        /* Decode checksum */
        UINT32DECODE(image, read_chksum);

        /* The Driver Information Block may not appear with the version
         * 2 super block.  Thus we set the driver_addr field of the in
         * core representation of the super block HADDR_UNDEF to prevent
         * any attempt to load the Driver Information Block.
         */
        sblock->driver_addr = HADDR_UNDEF;
    }

    /* Size check */
    if ((size_t)(image - (const uint8_t *)_image) > len)
        HDONE_ERROR(H5E_FILE, H5E_BADVALUE, NULL, "bad decoded superblock size");

    ret_value = sblock;

done:
    /* Release the [possibly partially initialized] superblock on error */
    if (!ret_value && sblock)
        if (H5F__super_free(sblock) < 0)
            HDONE_ERROR(H5E_FILE, H5E_CANTFREE, NULL, "unable to destroy superblock data");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__cache_superblock_deserialize() */

/*-------------------------------------------------------------------------
 * Function:    H5F__cache_superblock_image_len
 *
 * Purpose:     Compute the size of the data structure on disk
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5F__cache_superblock_image_len(const void *_thing, size_t *image_len)
{
    const H5F_super_t *sblock = (const H5F_super_t *)_thing; /* Pointer to the object */

    FUNC_ENTER_PACKAGE_NOERR

    assert(sblock);
    assert(sblock->cache_info.type == H5AC_SUPERBLOCK);
    assert(image_len);

    /* Set the image length size */
    *image_len = (size_t)H5F_SUPERBLOCK_SIZE(sblock);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5F__cache_superblock_image_len() */

/*-------------------------------------------------------------------------
 * Function:	H5F__cache_superblock_serialize
 *
 * Purpose:     Flush a dirty object to disk
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5F__cache_superblock_serialize(const H5F_t *f, void *_image, size_t H5_ATTR_UNUSED len, void *_thing)
{
    H5F_super_t *sblock = (H5F_super_t *)_thing; /* Pointer to the object */
    uint8_t     *image  = _image;                /* Pointer into raw data buffer */
    haddr_t      rel_eof;                        /* Relative EOF for file */
    herr_t       ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(image);
    assert(sblock);

    /* Assert that the superblock is marked as being flushed last (and
       collectively in parallel) */
    /* (We'll rely on the cache to make sure it actually *is* flushed
       last (and collectively in parallel), but this check doesn't hurt) */
    assert(sblock->cache_info.flush_me_last);

    /* Encode the common portion of the file superblock for all versions */
    H5MM_memcpy(image, H5F_SIGNATURE, (size_t)H5F_SIGNATURE_LEN);
    image += H5F_SIGNATURE_LEN;
    *image++ = (uint8_t)sblock->super_vers;

    /* Check for older version of superblock format */
    if (sblock->super_vers < HDF5_SUPERBLOCK_VERSION_2) {
        *image++ = (uint8_t)HDF5_FREESPACE_VERSION; /* (hard-wired) */
        *image++ = (uint8_t)HDF5_OBJECTDIR_VERSION; /* (hard-wired) */
        *image++ = 0;                               /* reserved*/

        *image++ = (uint8_t)HDF5_SHAREDHEADER_VERSION; /* (hard-wired) */
        *image++ = sblock->sizeof_addr;
        *image++ = sblock->sizeof_size;
        *image++ = 0; /* reserved */

        UINT16ENCODE(image, sblock->sym_leaf_k);
        UINT16ENCODE(image, sblock->btree_k[H5B_SNODE_ID]);
        UINT32ENCODE(image, (uint32_t)sblock->status_flags);

        /*
         * Versions of the superblock >0 have the indexed storage B-tree
         * internal 'K' value stored
         */
        if (sblock->super_vers > HDF5_SUPERBLOCK_VERSION_DEF) {
            UINT16ENCODE(image, sblock->btree_k[H5B_CHUNK_ID]);
            *image++ = 0; /*reserved */
            *image++ = 0; /*reserved */
        }                 /* end if */

        /* Encode the base address */
        H5F_addr_encode(f, &image, sblock->base_addr);

        /* Encode the address of global free-space index */
        H5F_addr_encode(f, &image, sblock->ext_addr);

        /* Encode the end-of-file address. Note that at this point in time,
         * the EOF value itself may not be reflective of the file's size, as
         * we will eventually truncate the file to match the EOA value. As
         * such, use the EOA value in its place, knowing that the current EOF
         * value will ultimately match it. */
        if ((rel_eof = H5FD_get_eoa(f->shared->lf, H5FD_MEM_SUPER)) == HADDR_UNDEF)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGET, FAIL, "driver get_eoa request failed");
        H5F_addr_encode(f, &image, (rel_eof + sblock->base_addr));

        /* Encode the driver information block address */
        H5F_addr_encode(f, &image, sblock->driver_addr);

        /* Encode the root group object entry, including the cached stab info */
        if (H5G_ent_encode(f, &image, sblock->root_ent) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTENCODE, FAIL, "can't encode root group symbol table entry");

        /* NOTE: Driver info block is handled separately */

    }                         /* end if */
    else {                    /* sblock->super_vers >= HDF5_SUPERBLOCK_VERSION_2 */
        uint32_t   chksum;    /* Checksum temporary variable      */
        H5O_loc_t *root_oloc; /* Pointer to root group's object location */

        /* Size of file addresses & offsets, and status flags */
        *image++ = sblock->sizeof_addr;
        *image++ = sblock->sizeof_size;
        *image++ = sblock->status_flags;

        /* Encode the base address */
        H5F_addr_encode(f, &image, sblock->base_addr);

        /* Encode the address of the superblock extension */
        H5F_addr_encode(f, &image, sblock->ext_addr);

        /* At this point in time, the EOF value itself may
         * not be reflective of the file's size, since we'll eventually
         * truncate it to match the EOA value. As such, use the EOA value
         * in its place, knowing that the current EOF value will
         * ultimately match it. */
        if ((rel_eof = H5FD_get_eoa(f->shared->lf, H5FD_MEM_SUPER)) == HADDR_UNDEF)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGET, FAIL, "driver get_eoa request failed");
        H5F_addr_encode(f, &image, (rel_eof + sblock->base_addr));

        /* Retrieve information for root group */
        if (NULL == (root_oloc = H5G_oloc(f->shared->root_grp)))
            HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "unable to retrieve root group information");

        /* Encode address of root group's object header */
        H5F_addr_encode(f, &image, root_oloc->addr);

        /* Compute superblock checksum */
        chksum = H5_checksum_metadata(_image, ((size_t)H5F_SUPERBLOCK_SIZE(sblock) - H5F_SIZEOF_CHKSUM), 0);

        /* Superblock checksum */
        UINT32ENCODE(image, chksum);

        /* Sanity check */
        assert((size_t)(image - (uint8_t *)_image) == (size_t)H5F_SUPERBLOCK_SIZE(sblock));
    }

    /* Sanity check */
    assert((size_t)(image - (uint8_t *)_image) == len);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5F__cache_superblock_serialize() */

/*-------------------------------------------------------------------------
 * Function:	H5F__cache_superblock_free_icr
 *
 * Purpose:     Destroy/release an "in core representation" of a data
 *              structure
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5F__cache_superblock_free_icr(void *_thing)
{
    H5F_super_t *sblock    = (H5F_super_t *)_thing; /* Pointer to the object */
    herr_t       ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(sblock);
    assert(sblock->cache_info.type == H5AC_SUPERBLOCK);

    /* Destroy superblock */
    if (H5F__super_free(sblock) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTFREE, FAIL, "unable to free superblock");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5F__cache_superblock_free_icr() */

/*-------------------------------------------------------------------------
 * Function:    H5F__cache_drvrinfo_get_initial_load_size
 *
 * Purpose:     Compute the initial size of the data structure on disk.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5F__cache_drvrinfo_get_initial_load_size(void H5_ATTR_UNUSED *_udata, size_t *image_len)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(image_len);

    /* Set the initial image length size */
    *image_len = H5F_DRVINFOBLOCK_HDR_SIZE; /* Fixed size portion of driver info block */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5F__cache_drvrinfo_get_initial_load_size() */

/*-------------------------------------------------------------------------
 * Function:    H5F__cache_drvrinfo_get_final_load_size
 *
 * Purpose:     Compute the final size of the data structure on disk.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5F__cache_drvrinfo_get_final_load_size(const void *_image, size_t image_len, void *_udata,
                                        size_t *actual_len)
{
    const uint8_t           *image = _image;                            /* Pointer into raw data buffer */
    H5F_drvrinfo_cache_ud_t *udata = (H5F_drvrinfo_cache_ud_t *)_udata; /* User data */
    H5O_drvinfo_t            drvrinfo;                                  /* Driver info */
    herr_t                   ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(image);
    assert(udata);
    assert(actual_len);
    assert(*actual_len == image_len);
    assert(image_len == H5F_DRVINFOBLOCK_HDR_SIZE);

    /* Deserialize the file driver info's prefix */
    if (H5F__drvrinfo_prefix_decode(&drvrinfo, NULL, &image, image_len, udata, true) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTDECODE, FAIL, "can't decode file driver info prefix");

    /* Set the final size for the cache image */
    *actual_len = H5F_DRVINFOBLOCK_HDR_SIZE + drvrinfo.len;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__cache_drvrinfo_get_final_load_size() */

/*-------------------------------------------------------------------------
 * Function:	H5F__cache_drvrinfo_deserialize
 *
 * Purpose:     Loads an object from the disk
 *
 * Return:      Success:    Pointer to a new driver info struct
 *              Failure:    NULL
 *-------------------------------------------------------------------------
 */
static void *
H5F__cache_drvrinfo_deserialize(const void *_image, size_t len, void *_udata, bool H5_ATTR_UNUSED *dirty)
{
    H5O_drvinfo_t           *drvinfo = NULL;                              /* Driver info */
    H5F_drvrinfo_cache_ud_t *udata   = (H5F_drvrinfo_cache_ud_t *)_udata; /* User data */
    const uint8_t           *image   = _image;                            /* Pointer into raw data buffer */
    char                     drv_name[9];                                 /* Name of driver */
    H5O_drvinfo_t           *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    assert(image);
    assert(len >= H5F_DRVINFOBLOCK_HDR_SIZE);
    assert(udata);
    assert(udata->f);

    /* Allocate space for the driver info */
    if (NULL == (drvinfo = (H5O_drvinfo_t *)H5MM_calloc(sizeof(H5O_drvinfo_t))))
        HGOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "memory allocation failed for driver info message");

    /* Deserialize the file driver info's prefix */
    if (H5F__drvrinfo_prefix_decode(drvinfo, drv_name, &image, len, udata, false) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTDECODE, NULL, "can't decode file driver info prefix");

    /* Sanity check */
    assert(len == (H5F_DRVINFOBLOCK_HDR_SIZE + drvinfo->len));

    /* Validate and decode driver information */
    if (H5FD_sb_load(udata->f->shared->lf, drv_name, image) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTDECODE, NULL, "unable to decode driver information");

    /* Sanity check */
    assert((size_t)(image - (const uint8_t *)_image) <= len);

    ret_value = drvinfo;

done:
    /* Release the [possibly partially initialized] driver info message on error */
    if (!ret_value && drvinfo)
        H5MM_xfree(drvinfo);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F__cache_drvrinfo_deserialize() */

/*-------------------------------------------------------------------------
 * Function:    H5F__cache_drvrinfo_image_len
 *
 * Purpose:     Compute the size of the data structure on disk
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5F__cache_drvrinfo_image_len(const void *_thing, size_t *image_len)
{
    const H5O_drvinfo_t *drvinfo = (const H5O_drvinfo_t *)_thing; /* Pointer to the object */

    FUNC_ENTER_PACKAGE_NOERR

    assert(drvinfo);
    assert(drvinfo->cache_info.type == H5AC_DRVRINFO);
    assert(image_len);

    /* Set the image length size */
    *image_len = (size_t)(H5F_DRVINFOBLOCK_HDR_SIZE + /* Fixed-size portion of driver info block */
                          drvinfo->len);              /* Variable-size portion of driver info block */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5F__cache_drvrinfo_image_len() */

/*-------------------------------------------------------------------------
 * Function:	H5F__cache_drvrinfo_serialize
 *
 * Purpose:     Flush a dirty object to disk
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5F__cache_drvrinfo_serialize(const H5F_t *f, void *_image, size_t H5_ATTR_NDEBUG_UNUSED len, void *_thing)
{
    H5O_drvinfo_t *drvinfo = (H5O_drvinfo_t *)_thing; /* Pointer to the object */
    uint8_t       *image   = _image;                  /* Pointer into raw data buffer */
    uint8_t       *dbuf;                              /* Pointer to beginning of driver info */
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(image);
    assert(drvinfo);
    assert(drvinfo->cache_info.type == H5AC_DRVRINFO);
    assert(len == (size_t)(H5F_DRVINFOBLOCK_HDR_SIZE + drvinfo->len));

    /* Save pointer to beginning of driver info */
    dbuf = image;

    /* Encode the driver information block */
    *image++ = HDF5_DRIVERINFO_VERSION_0; /* Version */
    *image++ = 0;                         /* reserved */
    *image++ = 0;                         /* reserved */
    *image++ = 0;                         /* reserved */

    /* Driver info size, excluding header */
    UINT32ENCODE(image, drvinfo->len);

    /* Encode driver-specific data */
    if (H5FD_sb_encode(f->shared->lf, (char *)image, dbuf + H5F_DRVINFOBLOCK_HDR_SIZE) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "unable to encode driver information");

    /* Advance buffer pointer past name & variable-sized portion of driver info */
    image += 8 + drvinfo->len;

    /* Sanity check */
    assert((size_t)(image - (uint8_t *)_image) == len);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5F__cache_drvrinfo_serialize() */

/*-------------------------------------------------------------------------
 * Function:	H5F__cache_drvrinfo_free_icr
 *
 * Purpose:     Destroy/release an "in core representation" of a data
 *              structure
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5F__cache_drvrinfo_free_icr(void *_thing)
{
    H5O_drvinfo_t *drvinfo = (H5O_drvinfo_t *)_thing; /* Pointer to the object */

    FUNC_ENTER_PACKAGE_NOERR

    assert(drvinfo);
    assert(drvinfo->cache_info.type == H5AC_DRVRINFO);

    /* Destroy driver info message */
    H5MM_xfree(drvinfo);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5F__cache_drvrinfo_free_icr() */
