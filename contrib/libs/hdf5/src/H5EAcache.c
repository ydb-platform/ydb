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
 * Created:		H5EAcache.c
 *
 * Purpose:		Implement extensible array metadata cache methods.
 *
 *-------------------------------------------------------------------------
 */

/**********************/
/* Module Declaration */
/**********************/

#include "H5EAmodule.h" /* This source code file is part of the H5EA module */

/***********************/
/* Other Packages Used */
/***********************/

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5EApkg.h"     /* Extensible Arrays			*/
#include "H5MFprivate.h" /* File memory management		*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5VMprivate.h" /* Vectors and arrays 			*/
#include "H5WBprivate.h" /* Wrapped Buffers                      */

/****************/
/* Local Macros */
/****************/

/* Fractal heap format version #'s */
#define H5EA_HDR_VERSION    0 /* Header */
#define H5EA_IBLOCK_VERSION 0 /* Index block */
#define H5EA_SBLOCK_VERSION 0 /* Super block */
#define H5EA_DBLOCK_VERSION 0 /* Data block */

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
static herr_t H5EA__cache_hdr_get_initial_load_size(void *udata, size_t *image_len);
static htri_t H5EA__cache_hdr_verify_chksum(const void *image_ptr, size_t len, void *udata_ptr);
static void  *H5EA__cache_hdr_deserialize(const void *image, size_t len, void *udata, bool *dirty);
static herr_t H5EA__cache_hdr_image_len(const void *thing, size_t *image_len);
static herr_t H5EA__cache_hdr_serialize(const H5F_t *f, void *image, size_t len, void *thing);
static herr_t H5EA__cache_hdr_notify(H5AC_notify_action_t action, void *thing);
static herr_t H5EA__cache_hdr_free_icr(void *thing);

static herr_t H5EA__cache_iblock_get_initial_load_size(void *udata, size_t *image_len);
static htri_t H5EA__cache_iblock_verify_chksum(const void *image_ptr, size_t len, void *udata_ptr);
static void  *H5EA__cache_iblock_deserialize(const void *image, size_t len, void *udata, bool *dirty);
static herr_t H5EA__cache_iblock_image_len(const void *thing, size_t *image_len);
static herr_t H5EA__cache_iblock_serialize(const H5F_t *f, void *image, size_t len, void *thing);
static herr_t H5EA__cache_iblock_notify(H5AC_notify_action_t action, void *thing);
static herr_t H5EA__cache_iblock_free_icr(void *thing);

static herr_t H5EA__cache_sblock_get_initial_load_size(void *udata, size_t *image_len);
static htri_t H5EA__cache_sblock_verify_chksum(const void *image_ptr, size_t len, void *udata_ptr);
static void  *H5EA__cache_sblock_deserialize(const void *image, size_t len, void *udata, bool *dirty);
static herr_t H5EA__cache_sblock_image_len(const void *thing, size_t *image_len);
static herr_t H5EA__cache_sblock_serialize(const H5F_t *f, void *image, size_t len, void *thing);
static herr_t H5EA__cache_sblock_notify(H5AC_notify_action_t action, void *thing);
static herr_t H5EA__cache_sblock_free_icr(void *thing);

static herr_t H5EA__cache_dblock_get_initial_load_size(void *udata, size_t *image_len);
static htri_t H5EA__cache_dblock_verify_chksum(const void *image_ptr, size_t len, void *udata_ptr);
static void  *H5EA__cache_dblock_deserialize(const void *image, size_t len, void *udata, bool *dirty);
static herr_t H5EA__cache_dblock_image_len(const void *thing, size_t *image_len);
static herr_t H5EA__cache_dblock_serialize(const H5F_t *f, void *image, size_t len, void *thing);
static herr_t H5EA__cache_dblock_notify(H5AC_notify_action_t action, void *thing);
static herr_t H5EA__cache_dblock_free_icr(void *thing);
static herr_t H5EA__cache_dblock_fsf_size(const void *thing, hsize_t *fsf_size);

static herr_t H5EA__cache_dblk_page_get_initial_load_size(void *udata, size_t *image_len);
static htri_t H5EA__cache_dblk_page_verify_chksum(const void *image_ptr, size_t len, void *udata_ptr);
static void  *H5EA__cache_dblk_page_deserialize(const void *image, size_t len, void *udata, bool *dirty);
static herr_t H5EA__cache_dblk_page_image_len(const void *thing, size_t *image_len);
static herr_t H5EA__cache_dblk_page_serialize(const H5F_t *f, void *image, size_t len, void *thing);
static herr_t H5EA__cache_dblk_page_notify(H5AC_notify_action_t action, void *thing);
static herr_t H5EA__cache_dblk_page_free_icr(void *thing);

/*********************/
/* Package Variables */
/*********************/

/* H5EA header inherits cache-like properties from H5AC */
const H5AC_class_t H5AC_EARRAY_HDR[1] = {{
    H5AC_EARRAY_HDR_ID,                    /* Metadata client ID */
    "Extensible Array Header",             /* Metadata client name (for debugging) */
    H5FD_MEM_EARRAY_HDR,                   /* File space memory type for client */
    H5AC__CLASS_NO_FLAGS_SET,              /* Client class behavior flags */
    H5EA__cache_hdr_get_initial_load_size, /* 'get_initial_load_size' callback */
    NULL,                                  /* 'get_final_load_size' callback */
    H5EA__cache_hdr_verify_chksum,         /* 'verify_chksum' callback */
    H5EA__cache_hdr_deserialize,           /* 'deserialize' callback */
    H5EA__cache_hdr_image_len,             /* 'image_len' callback */
    NULL,                                  /* 'pre_serialize' callback */
    H5EA__cache_hdr_serialize,             /* 'serialize' callback */
    H5EA__cache_hdr_notify,                /* 'notify' callback */
    H5EA__cache_hdr_free_icr,              /* 'free_icr' callback */
    NULL,                                  /* 'fsf_size' callback */
}};

/* H5EA index block inherits cache-like properties from H5AC */
const H5AC_class_t H5AC_EARRAY_IBLOCK[1] = {{
    H5AC_EARRAY_IBLOCK_ID,                    /* Metadata client ID */
    "Extensible Array Index Block",           /* Metadata client name (for debugging) */
    H5FD_MEM_EARRAY_IBLOCK,                   /* File space memory type for client */
    H5AC__CLASS_NO_FLAGS_SET,                 /* Client class behavior flags */
    H5EA__cache_iblock_get_initial_load_size, /* 'get_initial_load_size' callback */
    NULL,                                     /* 'get_final_load_size' callback */
    H5EA__cache_iblock_verify_chksum,         /* 'verify_chksum' callback */
    H5EA__cache_iblock_deserialize,           /* 'deserialize' callback */
    H5EA__cache_iblock_image_len,             /* 'image_len' callback */
    NULL,                                     /* 'pre_serialize' callback */
    H5EA__cache_iblock_serialize,             /* 'serialize' callback */
    H5EA__cache_iblock_notify,                /* 'notify' callback */
    H5EA__cache_iblock_free_icr,              /* 'free_icr' callback */
    NULL,                                     /* 'fsf_size' callback */
}};

/* H5EA super block inherits cache-like properties from H5AC */
const H5AC_class_t H5AC_EARRAY_SBLOCK[1] = {{
    H5AC_EARRAY_SBLOCK_ID,                    /* Metadata client ID */
    "Extensible Array Super Block",           /* Metadata client name (for debugging) */
    H5FD_MEM_EARRAY_SBLOCK,                   /* File space memory type for client */
    H5AC__CLASS_NO_FLAGS_SET,                 /* Client class behavior flags */
    H5EA__cache_sblock_get_initial_load_size, /* 'get_initial_load_size' callback */
    NULL,                                     /* 'get_final_load_size' callback */
    H5EA__cache_sblock_verify_chksum,         /* 'verify_chksum' callback */
    H5EA__cache_sblock_deserialize,           /* 'deserialize' callback */
    H5EA__cache_sblock_image_len,             /* 'image_len' callback */
    NULL,                                     /* 'pre_serialize' callback */
    H5EA__cache_sblock_serialize,             /* 'serialize' callback */
    H5EA__cache_sblock_notify,                /* 'notify' callback */
    H5EA__cache_sblock_free_icr,              /* 'free_icr' callback */
    NULL,                                     /* 'fsf_size' callback */
}};

/* H5EA data block inherits cache-like properties from H5AC */
const H5AC_class_t H5AC_EARRAY_DBLOCK[1] = {{
    H5AC_EARRAY_DBLOCK_ID,                    /* Metadata client ID */
    "Extensible Array Data Block",            /* Metadata client name (for debugging) */
    H5FD_MEM_EARRAY_DBLOCK,                   /* File space memory type for client */
    H5AC__CLASS_NO_FLAGS_SET,                 /* Client class behavior flags */
    H5EA__cache_dblock_get_initial_load_size, /* 'get_initial_load_size' callback */
    NULL,                                     /* 'get_final_load_size' callback */
    H5EA__cache_dblock_verify_chksum,         /* 'verify_chksum' callback */
    H5EA__cache_dblock_deserialize,           /* 'deserialize' callback */
    H5EA__cache_dblock_image_len,             /* 'image_len' callback */
    NULL,                                     /* 'pre_serialize' callback */
    H5EA__cache_dblock_serialize,             /* 'serialize' callback */
    H5EA__cache_dblock_notify,                /* 'notify' callback */
    H5EA__cache_dblock_free_icr,              /* 'free_icr' callback */
    H5EA__cache_dblock_fsf_size,              /* 'fsf_size' callback */
}};

/* H5EA data block page inherits cache-like properties from H5AC */
const H5AC_class_t H5AC_EARRAY_DBLK_PAGE[1] = {{
    H5AC_EARRAY_DBLK_PAGE_ID,                    /* Metadata client ID */
    "Extensible Array Data Block Page",          /* Metadata client name (for debugging) */
    H5FD_MEM_EARRAY_DBLK_PAGE,                   /* File space memory type for client */
    H5AC__CLASS_NO_FLAGS_SET,                    /* Client class behavior flags */
    H5EA__cache_dblk_page_get_initial_load_size, /* 'get_initial_load_size' callback */
    NULL,                                        /* 'get_final_load_size' callback */
    H5EA__cache_dblk_page_verify_chksum,         /* 'verify_chksum' callback */
    H5EA__cache_dblk_page_deserialize,           /* 'deserialize' callback */
    H5EA__cache_dblk_page_image_len,             /* 'image_len' callback */
    NULL,                                        /* 'pre_serialize' callback */
    H5EA__cache_dblk_page_serialize,             /* 'serialize' callback */
    H5EA__cache_dblk_page_notify,                /* 'notify' callback */
    H5EA__cache_dblk_page_free_icr,              /* 'free_icr' callback */
    NULL,                                        /* 'fsf_size' callback */
}};

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:    H5EA__cache_hdr_get_initial_load_size
 *
 * Purpose:     Compute the size of the data structure on disk.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__cache_hdr_get_initial_load_size(void *_udata, size_t *image_len)
{
    H5EA_hdr_cache_ud_t *udata = (H5EA_hdr_cache_ud_t *)_udata; /* User data for callback */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(udata);
    assert(udata->f);
    assert(image_len);

    /* Set the image length size */
    *image_len = (size_t)H5EA_HEADER_SIZE_FILE(udata->f);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5EA__cache_hdr_get_initial_load_size() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__cache_hdr_verify_chksum
 *
 * Purpose:     Verify the computed checksum of the data structure is the
 *              same as the stored chksum.
 *
 * Return:      Success:        true/false
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5EA__cache_hdr_verify_chksum(const void *_image, size_t len, void H5_ATTR_UNUSED *_udata)
{
    const uint8_t *image = (const uint8_t *)_image; /* Pointer into raw data buffer */
    uint32_t       stored_chksum;                   /* Stored metadata checksum value */
    uint32_t       computed_chksum;                 /* Computed metadata checksum value */
    htri_t         ret_value = true;

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(image);

    /* Get stored and computed checksums */
    H5F_get_checksums(image, len, &stored_chksum, &computed_chksum);

    if (stored_chksum != computed_chksum)
        ret_value = false;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__cache_hdr_verify_chksum() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__cache_hdr_deserialize
 *
 * Purpose:	Loads a data structure from the disk.
 *
 * Return:	Success:	Pointer to a new B-tree.
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5EA__cache_hdr_deserialize(const void *_image, size_t len, void *_udata, bool H5_ATTR_UNUSED *dirty)
{
    H5EA_cls_id_t        id;           /* ID of extensible array class, as found in file */
    H5EA_hdr_t          *hdr   = NULL; /* Extensible array info */
    H5EA_hdr_cache_ud_t *udata = (H5EA_hdr_cache_ud_t *)_udata;
    const uint8_t       *image = (const uint8_t *)_image; /* Pointer into raw data buffer */
    uint32_t             stored_chksum;                   /* Stored metadata checksum value */
    void                *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(image);
    assert(udata);
    assert(udata->f);
    assert(H5_addr_defined(udata->addr));

    /* Allocate space for the extensible array data structure */
    if (NULL == (hdr = H5EA__hdr_alloc(udata->f)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTALLOC, NULL,
                    "memory allocation failed for extensible array shared header");

    /* Set the extensible array header's address */
    hdr->addr = udata->addr;

    /* Magic number */
    if (memcmp(image, H5EA_HDR_MAGIC, (size_t)H5_SIZEOF_MAGIC) != 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_BADVALUE, NULL, "wrong extensible array header signature");
    image += H5_SIZEOF_MAGIC;

    /* Version */
    if (*image++ != H5EA_HDR_VERSION)
        HGOTO_ERROR(H5E_EARRAY, H5E_VERSION, NULL, "wrong extensible array header version");

    /* Extensible array class */
    id = (H5EA_cls_id_t)*image++;
    if (id >= H5EA_NUM_CLS_ID)
        HGOTO_ERROR(H5E_EARRAY, H5E_BADTYPE, NULL, "incorrect extensible array class");
    hdr->cparam.cls = H5EA_client_class_g[id];

    /* General array creation/configuration information */
    hdr->cparam.raw_elmt_size = *image++; /* Element size in file (in bytes) */
    hdr->cparam.max_nelmts_bits =
        *image++; /* Log2(Max. # of elements in array) - i.e. # of bits needed to store max. # of elements */
    hdr->cparam.idx_blk_elmts         = *image++; /* # of elements to store in index block */
    hdr->cparam.data_blk_min_elmts    = *image++; /* Min. # of elements per data block */
    hdr->cparam.sup_blk_min_data_ptrs = *image++; /* Min. # of data block pointers for a super block */
    hdr->cparam.max_dblk_page_nelmts_bits =
        *image++; /* Log2(Max. # of elements in data block page) - i.e. # of bits needed to store max. # of
                     elements in data block page */

    /* Array statistics */
    hdr->stats.computed.hdr_size = len;                                   /* Size of header in file */
    H5F_DECODE_LENGTH(udata->f, image, hdr->stats.stored.nsuper_blks);    /* Number of super blocks created */
    H5F_DECODE_LENGTH(udata->f, image, hdr->stats.stored.super_blk_size); /* Size of super blocks created */
    H5F_DECODE_LENGTH(udata->f, image, hdr->stats.stored.ndata_blks);     /* Number of data blocks created */
    H5F_DECODE_LENGTH(udata->f, image, hdr->stats.stored.data_blk_size);  /* Size of data blocks created */
    H5F_DECODE_LENGTH(udata->f, image, hdr->stats.stored.max_idx_set);    /* Max. index set (+1) */
    H5F_DECODE_LENGTH(udata->f, image, hdr->stats.stored.nelmts);         /* Number of elements 'realized' */

    /* Internal information */
    H5F_addr_decode(udata->f, &image, &hdr->idx_blk_addr); /* Address of index block */

    /* Index block statistics */
    if (H5_addr_defined(hdr->idx_blk_addr)) {
        H5EA_iblock_t iblock; /* Fake index block for computing size */

        /* Set index block count for file */
        hdr->stats.computed.nindex_blks = 1;

        /* Set up fake index block for computing size on disk */
        iblock.hdr         = hdr;
        iblock.nsblks      = H5EA_SBLK_FIRST_IDX(hdr->cparam.sup_blk_min_data_ptrs);
        iblock.ndblk_addrs = 2 * ((size_t)hdr->cparam.sup_blk_min_data_ptrs - 1);
        iblock.nsblk_addrs = hdr->nsblks - iblock.nsblks;

        /* Compute size of index block in file */
        hdr->stats.computed.index_blk_size = H5EA_IBLOCK_SIZE(&iblock);
    } /* end if */
    else {
        hdr->stats.computed.nindex_blks    = 0; /* Number of index blocks in file */
        hdr->stats.computed.index_blk_size = 0; /* Size of index blocks in file */
    }                                           /* end else */

    /* Sanity check */
    /* (allow for checksum not decoded yet) */
    assert((size_t)(image - (const uint8_t *)_image) == (len - H5EA_SIZEOF_CHKSUM));

    /* checksum verification already done in verify_chksum cb */

    /* Metadata checksum */
    UINT32DECODE(image, stored_chksum);

    /* Sanity check */
    assert((size_t)(image - (const uint8_t *)_image) == len);

    /* Finish initializing extensible array header */
    if (H5EA__hdr_init(hdr, udata->ctx_udata) < 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTINIT, NULL, "initialization failed for extensible array header");
    assert(hdr->size == len);

    /* Set return value */
    ret_value = hdr;

done:
    /* Release resources */
    if (!ret_value)
        if (hdr && H5EA__hdr_dest(hdr) < 0)
            HDONE_ERROR(H5E_EARRAY, H5E_CANTFREE, NULL, "unable to destroy extensible array header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__cache_hdr_deserialize() */

/*-------------------------------------------------------------------------
 * Function:    H5EA__cache_hdr_image_len
 *
 * Purpose:     Compute the size of the data structure on disk.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__cache_hdr_image_len(const void *_thing, size_t *image_len)
{
    const H5EA_hdr_t *hdr = (const H5EA_hdr_t *)_thing; /* Pointer to the object */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(hdr);
    assert(image_len);

    /* Set the image length size */
    *image_len = hdr->size;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5EA__cache_hdr_image_len() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__cache_hdr_serialize
 *
 * Purpose:	Flushes a dirty object to disk.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__cache_hdr_serialize(const H5F_t *f, void *_image, size_t H5_ATTR_UNUSED len, void *_thing)
{
    H5EA_hdr_t *hdr   = (H5EA_hdr_t *)_thing; /* Pointer to the extensible array header */
    uint8_t    *image = (uint8_t *)_image;    /* Pointer into raw data buffer */
    uint32_t    metadata_chksum;              /* Computed metadata checksum value */

    FUNC_ENTER_PACKAGE_NOERR

    /* check arguments */
    assert(f);
    assert(image);
    assert(hdr);

    /* Magic number */
    H5MM_memcpy(image, H5EA_HDR_MAGIC, (size_t)H5_SIZEOF_MAGIC);
    image += H5_SIZEOF_MAGIC;

    /* Version # */
    *image++ = H5EA_HDR_VERSION;

    /* Extensible array type */
    assert(hdr->cparam.cls->id <= 255);
    *image++ = (uint8_t)hdr->cparam.cls->id;

    /* General array creation/configuration information */
    *image++ = hdr->cparam.raw_elmt_size;      /* Element size in file (in bytes) */
    *image++ = hdr->cparam.max_nelmts_bits;    /* Log2(Max. # of elements in array) - i.e. # of bits needed to
                                                  store    max. # of elements */
    *image++ = hdr->cparam.idx_blk_elmts;      /* # of elements to store in index block */
    *image++ = hdr->cparam.data_blk_min_elmts; /* Min. # of elements per data block */
    *image++ = hdr->cparam.sup_blk_min_data_ptrs; /* Min. # of data block pointers for a super block */
    *image++ =
        hdr->cparam.max_dblk_page_nelmts_bits; /* Log2(Max. # of elements in data block page) - i.e. # of bits
                                                  needed to store max. # of elements in data block page */

    /* Array statistics */
    H5F_ENCODE_LENGTH(f, image, hdr->stats.stored.nsuper_blks);    /* Number of super blocks created */
    H5F_ENCODE_LENGTH(f, image, hdr->stats.stored.super_blk_size); /* Size of super blocks created */
    H5F_ENCODE_LENGTH(f, image, hdr->stats.stored.ndata_blks);     /* Number of data blocks created */
    H5F_ENCODE_LENGTH(f, image, hdr->stats.stored.data_blk_size);  /* Size of data blocks created */
    H5F_ENCODE_LENGTH(f, image, hdr->stats.stored.max_idx_set);    /* Max. index set (+1) */
    H5F_ENCODE_LENGTH(f, image, hdr->stats.stored.nelmts);         /* Number of elements 'realized' */

    /* Internal information */
    H5F_addr_encode(f, &image, hdr->idx_blk_addr); /* Address of index block */

    /* Compute metadata checksum */
    metadata_chksum = H5_checksum_metadata(_image, (size_t)(image - (uint8_t *)_image), 0);

    /* Metadata checksum */
    UINT32ENCODE(image, metadata_chksum);

    /* Sanity check */
    assert((size_t)(image - (uint8_t *)_image) == len);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5EA__cache_hdr_serialize() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__cache_hdr_notify
 *
 * Purpose:	Handle cache action notifications
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__cache_hdr_notify(H5AC_notify_action_t action, void *_thing)
{
    H5EA_hdr_t *hdr       = (H5EA_hdr_t *)_thing; /* Pointer to the object */
    herr_t      ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(hdr);

    /* Check if the file was opened with SWMR-write access */
    if (hdr->swmr_write) {
        /* Determine which action to take */
        switch (action) {
            case H5AC_NOTIFY_ACTION_AFTER_INSERT:
            case H5AC_NOTIFY_ACTION_AFTER_LOAD:
            case H5AC_NOTIFY_ACTION_AFTER_FLUSH:
            case H5AC_NOTIFY_ACTION_ENTRY_DIRTIED:
            case H5AC_NOTIFY_ACTION_ENTRY_CLEANED:
            case H5AC_NOTIFY_ACTION_CHILD_DIRTIED:
            case H5AC_NOTIFY_ACTION_CHILD_CLEANED:
            case H5AC_NOTIFY_ACTION_CHILD_UNSERIALIZED:
            case H5AC_NOTIFY_ACTION_CHILD_SERIALIZED:
                /* do nothing */
                break;

            case H5AC_NOTIFY_ACTION_BEFORE_EVICT:
                /* If hdr->parent != NULL, hdr->parent is used to destroy
                 * the flush dependency before the header is evicted.
                 */
                if (hdr->parent) {
                    /* Sanity check */
                    assert(hdr->top_proxy);

                    /* Destroy flush dependency on object header proxy */
                    if (H5AC_proxy_entry_remove_child((H5AC_proxy_entry_t *)hdr->parent,
                                                      (void *)hdr->top_proxy) < 0)
                        HGOTO_ERROR(H5E_EARRAY, H5E_CANTUNDEPEND, FAIL,
                                    "unable to destroy flush dependency between extensible array and proxy");
                    hdr->parent = NULL;
                } /* end if */

                /* Detach from 'top' proxy for extensible array */
                if (hdr->top_proxy) {
                    if (H5AC_proxy_entry_remove_child(hdr->top_proxy, hdr) < 0)
                        HGOTO_ERROR(H5E_EARRAY, H5E_CANTUNDEPEND, FAIL,
                                    "unable to destroy flush dependency between header and "
                                    "extensible array 'top' proxy");
                    /* Don't reset hdr->top_proxy here, it's destroyed when the header is freed -QAK */
                } /* end if */
                break;

            default:
#ifdef NDEBUG
                HGOTO_ERROR(H5E_EARRAY, H5E_BADVALUE, FAIL, "unknown action from metadata cache");
#else     /* NDEBUG */
                assert(0 && "Unknown action?!?");
#endif    /* NDEBUG */
        } /* end switch */
    }     /* end if */
    else
        assert(NULL == hdr->parent);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__cache_hdr_notify() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__cache_hdr_free_icr
 *
 * Purpose:	Destroy/release an "in core representation" of a data
 *              structure
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__cache_hdr_free_icr(void *thing)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(thing);

    /* Release the extensible array header */
    if (H5EA__hdr_dest((H5EA_hdr_t *)thing) < 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTFREE, FAIL, "can't free extensible array header");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__cache_hdr_free_icr() */

/*-------------------------------------------------------------------------
 * Function:    H5EA__cache_iblock_get_initial_load_size
 *
 * Purpose:     Compute the size of the data structure on disk.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__cache_iblock_get_initial_load_size(void *_udata, size_t *image_len)
{
    H5EA_hdr_t   *hdr = (H5EA_hdr_t *)_udata; /* User data for callback */
    H5EA_iblock_t iblock;                     /* Fake index block for computing size */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(hdr);
    assert(image_len);

    /* Set up fake index block for computing size on disk */
    memset(&iblock, 0, sizeof(iblock));
    iblock.hdr         = (H5EA_hdr_t *)hdr;
    iblock.nsblks      = H5EA_SBLK_FIRST_IDX(hdr->cparam.sup_blk_min_data_ptrs);
    iblock.ndblk_addrs = 2 * ((size_t)hdr->cparam.sup_blk_min_data_ptrs - 1);
    iblock.nsblk_addrs = hdr->nsblks - iblock.nsblks;

    /* Set the image length size */
    *image_len = (size_t)H5EA_IBLOCK_SIZE(&iblock);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5EA__cache_iblock_get_initial_load_size() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__cache_iblock_verify_chksum
 *
 * Purpose:     Verify the computed checksum of the data structure is the
 *              same as the stored chksum.
 *
 * Return:      Success:        true/false
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5EA__cache_iblock_verify_chksum(const void *_image, size_t len, void H5_ATTR_UNUSED *_udata)
{
    const uint8_t *image = (const uint8_t *)_image; /* Pointer into raw data buffer */
    uint32_t       stored_chksum;                   /* Stored metadata checksum value */
    uint32_t       computed_chksum;                 /* Computed metadata checksum value */
    htri_t         ret_value = true;

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(image);

    /* Get stored and computed checksums */
    H5F_get_checksums(image, len, &stored_chksum, &computed_chksum);

    if (stored_chksum != computed_chksum)
        ret_value = false;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__cache_iblock_verify_chksum() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__cache_iblock_deserialize
 *
 * Purpose:	Loads a data structure from the disk.
 *
 * Return:	Success:	Pointer to a new B-tree.
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5EA__cache_iblock_deserialize(const void *_image, size_t len, void *_udata, bool H5_ATTR_UNUSED *dirty)
{
    H5EA_iblock_t *iblock = NULL;                    /* Index block info */
    H5EA_hdr_t    *hdr    = (H5EA_hdr_t *)_udata;    /* User data for callback */
    const uint8_t *image  = (const uint8_t *)_image; /* Pointer into raw data buffer */
    uint32_t       stored_chksum;                    /* Stored metadata checksum value */
    haddr_t        arr_addr;                         /* Address of array header in the file */
    size_t         u;                                /* Local index variable */
    void          *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(image);
    assert(hdr);

    /* Allocate the extensible array index block */
    if (NULL == (iblock = H5EA__iblock_alloc(hdr)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTALLOC, NULL,
                    "memory allocation failed for extensible array index block");

    /* Set the extensible array index block's address */
    iblock->addr = hdr->idx_blk_addr;

    /* Magic number */
    if (memcmp(image, H5EA_IBLOCK_MAGIC, (size_t)H5_SIZEOF_MAGIC) != 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_BADVALUE, NULL, "wrong extensible array index block signature");
    image += H5_SIZEOF_MAGIC;

    /* Version */
    if (*image++ != H5EA_IBLOCK_VERSION)
        HGOTO_ERROR(H5E_EARRAY, H5E_VERSION, NULL, "wrong extensible array index block version");

    /* Extensible array type */
    if (*image++ != (uint8_t)hdr->cparam.cls->id)
        HGOTO_ERROR(H5E_EARRAY, H5E_BADTYPE, NULL, "incorrect extensible array class");

    /* Address of header for array that owns this block (just for file integrity checks) */
    H5F_addr_decode(hdr->f, &image, &arr_addr);
    if (H5_addr_ne(arr_addr, hdr->addr))
        HGOTO_ERROR(H5E_EARRAY, H5E_BADVALUE, NULL, "wrong extensible array header address");

    /* Internal information */

    /* Decode elements in index block */
    if (hdr->cparam.idx_blk_elmts > 0) {
        /* Convert from raw elements on disk into native elements in memory */
        if ((hdr->cparam.cls->decode)(image, iblock->elmts, (size_t)hdr->cparam.idx_blk_elmts, hdr->cb_ctx) <
            0)
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTDECODE, NULL, "can't decode extensible array index elements");
        image += (hdr->cparam.idx_blk_elmts * hdr->cparam.raw_elmt_size);
    } /* end if */

    /* Decode data block addresses in index block */
    if (iblock->ndblk_addrs > 0) {
        /* Decode addresses of data blocks in index block */
        for (u = 0; u < iblock->ndblk_addrs; u++)
            H5F_addr_decode(hdr->f, &image, &iblock->dblk_addrs[u]);
    } /* end if */

    /* Decode super block addresses in index block */
    if (iblock->nsblk_addrs > 0) {
        /* Decode addresses of super blocks in index block */
        for (u = 0; u < iblock->nsblk_addrs; u++)
            H5F_addr_decode(hdr->f, &image, &iblock->sblk_addrs[u]);
    } /* end if */

    /* Sanity check */
    /* (allow for checksum not decoded yet) */
    assert((size_t)(image - (const uint8_t *)_image) == (len - H5EA_SIZEOF_CHKSUM));

    /* Save the index block's size */
    iblock->size = len;

    /* checksum verification already done in verify_chksum cb */

    /* Metadata checksum */
    UINT32DECODE(image, stored_chksum);

    /* Sanity check */
    assert((size_t)(image - (const uint8_t *)_image) == iblock->size);

    /* Set return value */
    ret_value = iblock;

done:
    /* Release resources */
    if (!ret_value)
        if (iblock && H5EA__iblock_dest(iblock) < 0)
            HDONE_ERROR(H5E_EARRAY, H5E_CANTFREE, NULL, "unable to destroy extensible array index block");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__cache_iblock_deserialize() */

/*-------------------------------------------------------------------------
 * Function:    H5EA__cache_iblock_image_len
 *
 * Purpose:     Compute the size of the data structure on disk.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__cache_iblock_image_len(const void *_thing, size_t *image_len)
{
    const H5EA_iblock_t *iblock = (const H5EA_iblock_t *)_thing; /* Pointer to the object */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(iblock);
    assert(image_len);

    /* Set the image length size */
    *image_len = iblock->size;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5EA__cache_iblock_image_len() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__cache_iblock_serialize
 *
 * Purpose:	Flushes a dirty object to disk.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__cache_iblock_serialize(const H5F_t *f, void *_image, size_t H5_ATTR_UNUSED len, void *_thing)
{
    H5EA_iblock_t *iblock = (H5EA_iblock_t *)_thing; /* Pointer to the object to serialize */
    uint8_t       *image  = (uint8_t *)_image;       /* Pointer into raw data buffer */
    uint32_t       metadata_chksum;                  /* Computed metadata checksum value */
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* check arguments */
    assert(f);
    assert(image);
    assert(iblock);
    assert(iblock->hdr);

    /* Get temporary pointer to serialized info */

    /* Magic number */
    H5MM_memcpy(image, H5EA_IBLOCK_MAGIC, (size_t)H5_SIZEOF_MAGIC);
    image += H5_SIZEOF_MAGIC;

    /* Version # */
    *image++ = H5EA_IBLOCK_VERSION;

    /* Extensible array type */
    assert(iblock->hdr->cparam.cls->id <= 255);
    *image++ = (uint8_t)iblock->hdr->cparam.cls->id;

    /* Address of array header for array which owns this block */
    H5F_addr_encode(f, &image, iblock->hdr->addr);

    /* Internal information */

    /* Encode elements in index block */
    if (iblock->hdr->cparam.idx_blk_elmts > 0) {
        /* Convert from native elements in memory into raw elements on disk */
        if ((iblock->hdr->cparam.cls->encode)(image, iblock->elmts, (size_t)iblock->hdr->cparam.idx_blk_elmts,
                                              iblock->hdr->cb_ctx) < 0)
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTENCODE, FAIL, "can't encode extensible array index elements");
        image += (iblock->hdr->cparam.idx_blk_elmts * iblock->hdr->cparam.raw_elmt_size);
    } /* end if */

    /* Encode data block addresses in index block */
    if (iblock->ndblk_addrs > 0) {
        size_t u; /* Local index variable */

        /* Encode addresses of data blocks in index block */
        for (u = 0; u < iblock->ndblk_addrs; u++)
            H5F_addr_encode(f, &image, iblock->dblk_addrs[u]);
    } /* end if */

    /* Encode data block addresses in index block */
    if (iblock->nsblk_addrs > 0) {
        size_t u; /* Local index variable */

        /* Encode addresses of super blocks in index block */
        for (u = 0; u < iblock->nsblk_addrs; u++)
            H5F_addr_encode(f, &image, iblock->sblk_addrs[u]);
    } /* end if */

    /* Compute metadata checksum */
    metadata_chksum = H5_checksum_metadata(_image, (size_t)(image - (uint8_t *)_image), 0);

    /* Metadata checksum */
    UINT32ENCODE(image, metadata_chksum);

    /* Sanity check */
    assert((size_t)(image - (uint8_t *)_image) == len);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__cache_iblock_serialize() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__cache_iblock_notify
 *
 * Purpose:	Handle cache action notifications
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__cache_iblock_notify(H5AC_notify_action_t action, void *_thing)
{
    H5EA_iblock_t *iblock    = (H5EA_iblock_t *)_thing; /* Pointer to the object */
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(iblock);

    /* Determine which action to take */
    switch (action) {
        case H5AC_NOTIFY_ACTION_AFTER_INSERT:
        case H5AC_NOTIFY_ACTION_AFTER_LOAD:
            /* Create flush dependency on extensible array header */
            if (H5EA__create_flush_depend((H5AC_info_t *)iblock->hdr, (H5AC_info_t *)iblock) < 0)
                HGOTO_ERROR(
                    H5E_EARRAY, H5E_CANTDEPEND, FAIL,
                    "unable to create flush dependency between index block and header, address = %llu",
                    (unsigned long long)iblock->addr);
            break;

        case H5AC_NOTIFY_ACTION_AFTER_FLUSH:
        case H5AC_NOTIFY_ACTION_ENTRY_DIRTIED:
        case H5AC_NOTIFY_ACTION_ENTRY_CLEANED:
        case H5AC_NOTIFY_ACTION_CHILD_DIRTIED:
        case H5AC_NOTIFY_ACTION_CHILD_CLEANED:
        case H5AC_NOTIFY_ACTION_CHILD_UNSERIALIZED:
        case H5AC_NOTIFY_ACTION_CHILD_SERIALIZED:
            /* do nothing */
            break;

        case H5AC_NOTIFY_ACTION_BEFORE_EVICT:
            /* Destroy flush dependency on extensible array header */
            if (H5EA__destroy_flush_depend((H5AC_info_t *)iblock->hdr, (H5AC_info_t *)iblock) < 0)
                HGOTO_ERROR(
                    H5E_EARRAY, H5E_CANTUNDEPEND, FAIL,
                    "unable to destroy flush dependency between index block and header, address = %llu",
                    (unsigned long long)iblock->addr);

            /* Detach from 'top' proxy for extensible array */
            if (iblock->top_proxy) {
                if (H5AC_proxy_entry_remove_child(iblock->top_proxy, iblock) < 0)
                    HGOTO_ERROR(H5E_EARRAY, H5E_CANTUNDEPEND, FAIL,
                                "unable to destroy flush dependency between index block and "
                                "extensible array 'top' proxy");
                iblock->top_proxy = NULL;
            } /* end if */
            break;

        default:
#ifdef NDEBUG
            HGOTO_ERROR(H5E_EARRAY, H5E_BADVALUE, FAIL, "unknown action from metadata cache");
#else  /* NDEBUG */
            assert(0 && "Unknown action?!?");
#endif /* NDEBUG */
    }  /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__cache_iblock_notify() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__cache_iblock_free_icr
 *
 * Purpose:	Destroy/release an "in core representation" of a data
 *              structure
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__cache_iblock_free_icr(void *thing)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(thing);

    /* Release the extensible array index block */
    if (H5EA__iblock_dest((H5EA_iblock_t *)thing) < 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTFREE, FAIL, "can't free extensible array index block");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__cache_iblock_free_icr() */

/*-------------------------------------------------------------------------
 * Function:    H5EA__cache_sblock_get_initial_load_size
 *
 * Purpose:     Compute the size of the data structure on disk.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__cache_sblock_get_initial_load_size(void *_udata, size_t *image_len)
{
    H5EA_sblock_cache_ud_t *udata = (H5EA_sblock_cache_ud_t *)_udata; /* User data */
    H5EA_sblock_t           sblock; /* Fake super block for computing size */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(udata);
    assert(udata->hdr);
    assert(udata->sblk_idx > 0);
    assert(H5_addr_defined(udata->sblk_addr));
    assert(image_len);

    /* Set up fake super block for computing size on disk */
    /* (Note: extracted from H5EA__sblock_alloc) */
    memset(&sblock, 0, sizeof(sblock));
    sblock.hdr         = udata->hdr;
    sblock.ndblks      = udata->hdr->sblk_info[udata->sblk_idx].ndblks;
    sblock.dblk_nelmts = udata->hdr->sblk_info[udata->sblk_idx].dblk_nelmts;

    /* Check if # of elements in data blocks requires paging */
    if (sblock.dblk_nelmts > udata->hdr->dblk_page_nelmts) {
        /* Compute # of pages in each data block from this super block */
        sblock.dblk_npages = sblock.dblk_nelmts / udata->hdr->dblk_page_nelmts;

        /* Sanity check that we have at least 2 pages in data block */
        assert(sblock.dblk_npages > 1);

        /* Sanity check for integer truncation */
        assert((sblock.dblk_npages * udata->hdr->dblk_page_nelmts) == sblock.dblk_nelmts);

        /* Compute size of buffer for each data block's 'page init' bitmask */
        sblock.dblk_page_init_size = ((sblock.dblk_npages) + 7) / 8;
        assert(sblock.dblk_page_init_size > 0);
    } /* end if */

    /* Set the image length size */
    *image_len = (size_t)H5EA_SBLOCK_SIZE(&sblock);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5EA__cache_sblock_get_initial_load_size() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__cache_sblock_verify_chksum
 *
 * Purpose:     Verify the computed checksum of the data structure is the
 *              same as the stored chksum.
 *
 * Return:      Success:        true/false
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5EA__cache_sblock_verify_chksum(const void *_image, size_t len, void H5_ATTR_UNUSED *_udata)
{
    const uint8_t *image = (const uint8_t *)_image; /* Pointer into raw data buffer */
    uint32_t       stored_chksum;                   /* Stored metadata checksum value */
    uint32_t       computed_chksum;                 /* Computed metadata checksum value */
    htri_t         ret_value = true;

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(image);

    /* Get stored and computed checksums */
    H5F_get_checksums(image, len, &stored_chksum, &computed_chksum);

    if (stored_chksum != computed_chksum)
        ret_value = false;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__cache_sblock_verify_chksum() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__cache_sblock_deserialize
 *
 * Purpose:	Loads a data structure from the disk.
 *
 * Return:	Success:	Pointer to a new B-tree.
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5EA__cache_sblock_deserialize(const void *_image, size_t len, void *_udata, bool H5_ATTR_UNUSED *dirty)
{
    H5EA_sblock_t          *sblock = NULL;                             /* Super block info */
    H5EA_sblock_cache_ud_t *udata  = (H5EA_sblock_cache_ud_t *)_udata; /* User data */
    const uint8_t          *image  = (const uint8_t *)_image;          /* Pointer into raw data buffer */
    uint32_t                stored_chksum;                             /* Stored metadata checksum value */
    haddr_t                 arr_addr; /* Address of array header in the file */
    size_t                  u;        /* Local index variable */
    void                   *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(udata);
    assert(udata->hdr);
    assert(udata->parent);
    assert(udata->sblk_idx > 0);
    assert(H5_addr_defined(udata->sblk_addr));

    /* Allocate the extensible array super block */
    if (NULL == (sblock = H5EA__sblock_alloc(udata->hdr, udata->parent, udata->sblk_idx)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTALLOC, NULL,
                    "memory allocation failed for extensible array super block");

    /* Set the extensible array super block's address */
    sblock->addr = udata->sblk_addr;

    /* Magic number */
    if (memcmp(image, H5EA_SBLOCK_MAGIC, (size_t)H5_SIZEOF_MAGIC) != 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_BADVALUE, NULL, "wrong extensible array super block signature");
    image += H5_SIZEOF_MAGIC;

    /* Version */
    if (*image++ != H5EA_SBLOCK_VERSION)
        HGOTO_ERROR(H5E_EARRAY, H5E_VERSION, NULL, "wrong extensible array super block version");

    /* Extensible array type */
    if (*image++ != (uint8_t)udata->hdr->cparam.cls->id)
        HGOTO_ERROR(H5E_EARRAY, H5E_BADTYPE, NULL, "incorrect extensible array class");

    /* Address of header for array that owns this block (just for file integrity checks) */
    H5F_addr_decode(udata->hdr->f, &image, &arr_addr);
    if (H5_addr_ne(arr_addr, udata->hdr->addr))
        HGOTO_ERROR(H5E_EARRAY, H5E_BADVALUE, NULL, "wrong extensible array header address");

    /* Offset of block within the array's address space */
    UINT64DECODE_VAR(image, sblock->block_off, udata->hdr->arr_off_size);

    /* Internal information */

    /* Check for 'page init' bitmasks for this super block */
    if (sblock->dblk_npages > 0) {
        size_t tot_page_init_size =
            sblock->ndblks * sblock->dblk_page_init_size; /* Compute total size of 'page init' buffer */

        /* Retrieve the 'page init' bitmasks */
        H5MM_memcpy(sblock->page_init, image, tot_page_init_size);
        image += tot_page_init_size;
    } /* end if */

    /* Decode data block addresses */
    for (u = 0; u < sblock->ndblks; u++)
        H5F_addr_decode(udata->hdr->f, &image, &sblock->dblk_addrs[u]);

    /* Sanity check */
    /* (allow for checksum not decoded yet) */
    assert((size_t)(image - (const uint8_t *)_image) == (len - H5EA_SIZEOF_CHKSUM));

    /* Save the super block's size */
    sblock->size = len;

    /* checksum verification already done in verify_chksum cb */

    /* Metadata checksum */
    UINT32DECODE(image, stored_chksum);

    /* Sanity check */
    assert((size_t)(image - (const uint8_t *)_image) == sblock->size);

    /* Set return value */
    ret_value = sblock;

done:
    /* Release resources */
    if (!ret_value)
        if (sblock && H5EA__sblock_dest(sblock) < 0)
            HDONE_ERROR(H5E_EARRAY, H5E_CANTFREE, NULL, "unable to destroy extensible array super block");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__cache_sblock_deserialize() */

/*-------------------------------------------------------------------------
 * Function:    H5EA__cache_sblock_image_len
 *
 * Purpose:     Compute the size of the data structure on disk.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__cache_sblock_image_len(const void *_thing, size_t *image_len)
{
    const H5EA_sblock_t *sblock = (const H5EA_sblock_t *)_thing; /* Pointer to the object */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(sblock);
    assert(image_len);

    /* Set the image length size */
    *image_len = sblock->size;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5EA__cache_sblock_image_len() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__cache_sblock_serialize
 *
 * Purpose:	Flushes a dirty object to disk.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__cache_sblock_serialize(const H5F_t *f, void *_image, size_t H5_ATTR_UNUSED len, void *_thing)
{
    H5EA_sblock_t *sblock = (H5EA_sblock_t *)_thing; /* Pointer to the object to serialize */
    uint8_t       *image  = (uint8_t *)_image;       /* Pointer into raw data buffer */
    uint32_t       metadata_chksum;                  /* Computed metadata checksum value */
    size_t         u;                                /* Local index variable */

    FUNC_ENTER_PACKAGE_NOERR

    /* check arguments */
    assert(f);
    assert(image);
    assert(sblock);
    assert(sblock->hdr);

    /* Magic number */
    H5MM_memcpy(image, H5EA_SBLOCK_MAGIC, (size_t)H5_SIZEOF_MAGIC);
    image += H5_SIZEOF_MAGIC;

    /* Version # */
    *image++ = H5EA_SBLOCK_VERSION;

    /* Extensible array type */
    assert(sblock->hdr->cparam.cls->id <= 255);
    *image++ = (uint8_t)sblock->hdr->cparam.cls->id;

    /* Address of array header for array which owns this block */
    H5F_addr_encode(f, &image, sblock->hdr->addr);

    /* Offset of block in array */
    UINT64ENCODE_VAR(image, sblock->block_off, sblock->hdr->arr_off_size);

    /* Internal information */

    /* Check for 'page init' bitmasks for this super block */
    if (sblock->dblk_npages > 0) {
        size_t tot_page_init_size =
            sblock->ndblks * sblock->dblk_page_init_size; /* Compute total size of 'page init' buffer */

        /* Store the 'page init' bitmasks */
        H5MM_memcpy(image, sblock->page_init, tot_page_init_size);
        image += tot_page_init_size;
    } /* end if */

    /* Encode addresses of data blocks in super block */
    for (u = 0; u < sblock->ndblks; u++)
        H5F_addr_encode(f, &image, sblock->dblk_addrs[u]);

    /* Compute metadata checksum */
    metadata_chksum = H5_checksum_metadata(_image, (size_t)(image - (uint8_t *)_image), 0);

    /* Metadata checksum */
    UINT32ENCODE(image, metadata_chksum);

    /* Sanity check */
    assert((size_t)(image - (uint8_t *)_image) == len);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5EA__cache_sblock_serialize() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__cache_sblock_notify
 *
 * Purpose:	Handle cache action notifications
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__cache_sblock_notify(H5AC_notify_action_t action, void *_thing)
{
    H5EA_sblock_t *sblock    = (H5EA_sblock_t *)_thing; /* Pointer to the object */
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(sblock);

    /* Determine which action to take */
    switch (action) {
        case H5AC_NOTIFY_ACTION_AFTER_INSERT:
        case H5AC_NOTIFY_ACTION_AFTER_LOAD:
            /* Create flush dependency on index block */
            if (H5EA__create_flush_depend((H5AC_info_t *)sblock->parent, (H5AC_info_t *)sblock) < 0)
                HGOTO_ERROR(
                    H5E_EARRAY, H5E_CANTDEPEND, FAIL,
                    "unable to create flush dependency between super block and index block, address = %llu",
                    (unsigned long long)sblock->addr);
            break;

        case H5AC_NOTIFY_ACTION_AFTER_FLUSH:
            /* Destroy flush dependency on extensible array header, if set */
            if (sblock->has_hdr_depend) {
                if (H5EA__destroy_flush_depend((H5AC_info_t *)sblock->hdr, (H5AC_info_t *)sblock) < 0)
                    HGOTO_ERROR(
                        H5E_EARRAY, H5E_CANTUNDEPEND, FAIL,
                        "unable to destroy flush dependency between super block and header, address = %llu",
                        (unsigned long long)sblock->addr);
                sblock->has_hdr_depend = false;
            } /* end if */
            break;

        case H5AC_NOTIFY_ACTION_BEFORE_EVICT:
            /* Destroy flush dependency on index block */
            if (H5EA__destroy_flush_depend((H5AC_info_t *)sblock->parent, (H5AC_info_t *)sblock) < 0)
                HGOTO_ERROR(
                    H5E_EARRAY, H5E_CANTUNDEPEND, FAIL,
                    "unable to destroy flush dependency between super block and index block, address = %llu",
                    (unsigned long long)sblock->addr);

            /* Destroy flush dependency on extensible array header, if set */
            if (sblock->has_hdr_depend) {
                if (H5EA__destroy_flush_depend((H5AC_info_t *)sblock->hdr, (H5AC_info_t *)sblock) < 0)
                    HGOTO_ERROR(
                        H5E_EARRAY, H5E_CANTUNDEPEND, FAIL,
                        "unable to destroy flush dependency between super block and header, address = %llu",
                        (unsigned long long)sblock->addr);
                sblock->has_hdr_depend = false;
            } /* end if */

            /* Detach from 'top' proxy for extensible array */
            if (sblock->top_proxy) {
                if (H5AC_proxy_entry_remove_child(sblock->top_proxy, sblock) < 0)
                    HGOTO_ERROR(H5E_EARRAY, H5E_CANTUNDEPEND, FAIL,
                                "unable to destroy flush dependency between super block and "
                                "extensible array 'top' proxy");
                sblock->top_proxy = NULL;
            } /* end if */
            break;

        case H5AC_NOTIFY_ACTION_ENTRY_DIRTIED:
        case H5AC_NOTIFY_ACTION_ENTRY_CLEANED:
        case H5AC_NOTIFY_ACTION_CHILD_DIRTIED:
        case H5AC_NOTIFY_ACTION_CHILD_CLEANED:
        case H5AC_NOTIFY_ACTION_CHILD_UNSERIALIZED:
        case H5AC_NOTIFY_ACTION_CHILD_SERIALIZED:
            /* do nothing */
            break;

        default:
#ifdef NDEBUG
            HGOTO_ERROR(H5E_EARRAY, H5E_BADVALUE, FAIL, "unknown action from metadata cache");
#else  /* NDEBUG */
            assert(0 && "Unknown action?!?");
#endif /* NDEBUG */
    }  /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__cache_sblock_notify() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__cache_sblock_free_icr
 *
 * Purpose:	Destroy/release an "in core representation" of a data
 *              structure
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__cache_sblock_free_icr(void *thing)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(thing);

    /* Release the extensible array super block */
    if (H5EA__sblock_dest((H5EA_sblock_t *)thing) < 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTFREE, FAIL, "can't free extensible array super block");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__cache_sblock_free_icr() */

/*-------------------------------------------------------------------------
 * Function:    H5EA__cache_dblock_get_initial_load_size
 *
 * Purpose:     Compute the size of the data structure on disk.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__cache_dblock_get_initial_load_size(void *_udata, size_t *image_len)
{
    H5EA_dblock_cache_ud_t *udata = (H5EA_dblock_cache_ud_t *)_udata; /* User data */
    H5EA_dblock_t           dblock;                                   /* Fake data block for computing size */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(udata);
    assert(udata->hdr);
    assert(udata->nelmts > 0);
    assert(image_len);

    /* Set up fake data block for computing size on disk */
    /* (Note: extracted from H5EA__dblock_alloc) */
    memset(&dblock, 0, sizeof(dblock));

    /* need to set:
     *
     *    dblock.hdr
     *    dblock.npages
     *    dblock.nelmts
     *
     * before we invoke either H5EA_DBLOCK_PREFIX_SIZE() or
     * H5EA_DBLOCK_SIZE().
     */
    dblock.hdr    = udata->hdr;
    dblock.nelmts = udata->nelmts;

    if (udata->nelmts > udata->hdr->dblk_page_nelmts) {
        /* Set the # of pages in the direct block */
        dblock.npages = udata->nelmts / udata->hdr->dblk_page_nelmts;
        assert(udata->nelmts == (dblock.npages * udata->hdr->dblk_page_nelmts));
    } /* end if */

    /* Set the image length size */
    if (!dblock.npages)
        *image_len = H5EA_DBLOCK_SIZE(&dblock);
    else
        *image_len = H5EA_DBLOCK_PREFIX_SIZE(&dblock);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5EA__cache_dblock_get_initial_load_size() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__cache_dblock_verify_chksum
 *
 * Purpose:     Verify the computed checksum of the data structure is the
 *              same as the stored chksum.
 *
 * Return:      Success:        true/false
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5EA__cache_dblock_verify_chksum(const void *_image, size_t len, void H5_ATTR_UNUSED *_udata)
{
    const uint8_t *image = (const uint8_t *)_image; /* Pointer into raw data buffer */
    uint32_t       stored_chksum;                   /* Stored metadata checksum value */
    uint32_t       computed_chksum;                 /* Computed metadata checksum value */
    htri_t         ret_value = true;

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(image);

    /* Get stored and computed checksums */
    H5F_get_checksums(image, len, &stored_chksum, &computed_chksum);

    if (stored_chksum != computed_chksum)
        ret_value = false;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__cache_sblock_verify_chksum() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__cache_dblock_deserialize
 *
 * Purpose:	Loads a data structure from the disk.
 *
 * Return:	Success:	Pointer to a new B-tree.
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5EA__cache_dblock_deserialize(const void *_image, size_t H5_ATTR_NDEBUG_UNUSED len, void *_udata,
                               bool H5_ATTR_UNUSED *dirty)
{
    H5EA_dblock_t          *dblock = NULL;                             /* Data block info */
    H5EA_dblock_cache_ud_t *udata  = (H5EA_dblock_cache_ud_t *)_udata; /* User data */
    const uint8_t          *image  = (const uint8_t *)_image;          /* Pointer into raw data buffer */
    uint32_t                stored_chksum;                             /* Stored metadata checksum value */
    haddr_t                 arr_addr; /* Address of array header in the file */
    void                   *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(udata);
    assert(udata->hdr);
    assert(udata->parent);
    assert(udata->nelmts > 0);
    assert(H5_addr_defined(udata->dblk_addr));

    /* Allocate the extensible array data block */
    if (NULL == (dblock = H5EA__dblock_alloc(udata->hdr, udata->parent, udata->nelmts)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTALLOC, NULL,
                    "memory allocation failed for extensible array data block");

    assert(((!dblock->npages) && (len == H5EA_DBLOCK_SIZE(dblock))) ||
           (len == H5EA_DBLOCK_PREFIX_SIZE(dblock)));

    /* Set the extensible array data block's information */
    dblock->addr = udata->dblk_addr;

    /* Magic number */
    if (memcmp(image, H5EA_DBLOCK_MAGIC, (size_t)H5_SIZEOF_MAGIC) != 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_BADVALUE, NULL, "wrong extensible array data block signature");
    image += H5_SIZEOF_MAGIC;

    /* Version */
    if (*image++ != H5EA_DBLOCK_VERSION)
        HGOTO_ERROR(H5E_EARRAY, H5E_VERSION, NULL, "wrong extensible array data block version");

    /* Extensible array type */
    if (*image++ != (uint8_t)udata->hdr->cparam.cls->id)
        HGOTO_ERROR(H5E_EARRAY, H5E_BADTYPE, NULL, "incorrect extensible array class");

    /* Address of header for array that owns this block (just for file integrity checks) */
    H5F_addr_decode(udata->hdr->f, &image, &arr_addr);
    if (H5_addr_ne(arr_addr, udata->hdr->addr))
        HGOTO_ERROR(H5E_EARRAY, H5E_BADVALUE, NULL, "wrong extensible array header address");

    /* Offset of block within the array's address space */
    UINT64DECODE_VAR(image, dblock->block_off, udata->hdr->arr_off_size);

    /* Internal information */

    /* Only decode elements if the data block is not paged */
    if (!dblock->npages) {
        /* Decode elements in data block */
        /* Convert from raw elements on disk into native elements in memory */
        if ((udata->hdr->cparam.cls->decode)(image, dblock->elmts, udata->nelmts, udata->hdr->cb_ctx) < 0)
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTDECODE, NULL, "can't decode extensible array data elements");
        image += (udata->nelmts * udata->hdr->cparam.raw_elmt_size);
    } /* end if */

    /* Sanity check */
    /* (allow for checksum not decoded yet) */
    assert((size_t)(image - (const uint8_t *)_image) == (len - H5EA_SIZEOF_CHKSUM));

    /* Set the data block's size */
    /* (Note: This is not the same as the image length, for paged data blocks) */
    dblock->size = H5EA_DBLOCK_SIZE(dblock);

    /* checksum verification already done in verify_chksum cb */

    /* Metadata checksum */
    UINT32DECODE(image, stored_chksum);

    /* Sanity check */
    assert((size_t)(image - (const uint8_t *)_image) == len);

    /* Set return value */
    ret_value = dblock;

done:

    /* Release resources */
    if (!ret_value)
        if (dblock && H5EA__dblock_dest(dblock) < 0)
            HDONE_ERROR(H5E_EARRAY, H5E_CANTFREE, NULL, "unable to destroy extensible array data block");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__cache_dblock_deserialize() */

/*-------------------------------------------------------------------------
 * Function:    H5EA__cache_dblock_image_len
 *
 * Purpose:     Compute the size of the data structure on disk.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__cache_dblock_image_len(const void *_thing, size_t *image_len)
{
    const H5EA_dblock_t *dblock = (const H5EA_dblock_t *)_thing; /* Pointer to the object */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(dblock);
    assert(image_len);

    /* Set the image length size */
    if (!dblock->npages)
        *image_len = dblock->size;
    else
        *image_len = (size_t)H5EA_DBLOCK_PREFIX_SIZE(dblock);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5EA__cache_dblock_image_len() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__cache_dblock_serialize
 *
 * Purpose:	Flushes a dirty object to disk.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__cache_dblock_serialize(const H5F_t *f, void *_image, size_t H5_ATTR_UNUSED len, void *_thing)
{
    H5EA_dblock_t *dblock = (H5EA_dblock_t *)_thing; /* Pointer to the object to serialize */
    uint8_t       *image  = (uint8_t *)_image;       /* Pointer into raw data buffer */
    uint32_t       metadata_chksum;                  /* Computed metadata checksum value */
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* check arguments */
    assert(f);
    assert(image);
    assert(dblock);
    assert(dblock->hdr);

    /* Magic number */
    H5MM_memcpy(image, H5EA_DBLOCK_MAGIC, (size_t)H5_SIZEOF_MAGIC);
    image += H5_SIZEOF_MAGIC;

    /* Version # */
    *image++ = H5EA_DBLOCK_VERSION;

    /* Extensible array type */
    assert(dblock->hdr->cparam.cls->id <= 255);
    *image++ = (uint8_t)dblock->hdr->cparam.cls->id;

    /* Address of array header for array which owns this block */
    H5F_addr_encode(f, &image, dblock->hdr->addr);

    /* Offset of block in array */
    UINT64ENCODE_VAR(image, dblock->block_off, dblock->hdr->arr_off_size);

    /* Internal information */

    /* Only encode elements if the data block is not paged */
    if (!dblock->npages) {
        /* Encode elements in data block */

        /* Convert from native elements in memory into raw elements on disk */
        if ((dblock->hdr->cparam.cls->encode)(image, dblock->elmts, dblock->nelmts, dblock->hdr->cb_ctx) < 0)
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTENCODE, FAIL, "can't encode extensible array data elements");
        image += (dblock->nelmts * dblock->hdr->cparam.raw_elmt_size);
    } /* end if */

    /* Compute metadata checksum */
    metadata_chksum = H5_checksum_metadata(_image, (size_t)(image - (uint8_t *)_image), 0);

    /* Metadata checksum */
    UINT32ENCODE(image, metadata_chksum);

    /* Sanity check */
    assert((size_t)(image - (uint8_t *)_image) == len);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__cache_dblock_serialize() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__cache_dblock_notify
 *
 * Purpose:	Handle cache action notifications
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__cache_dblock_notify(H5AC_notify_action_t action, void *_thing)
{
    H5EA_dblock_t *dblock    = (H5EA_dblock_t *)_thing; /* Pointer to the object */
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(dblock);

    /* Determine which action to take */
    switch (action) {
        case H5AC_NOTIFY_ACTION_AFTER_INSERT:
        case H5AC_NOTIFY_ACTION_AFTER_LOAD:
            /* Create flush dependency on parent */
            if (H5EA__create_flush_depend((H5AC_info_t *)dblock->parent, (H5AC_info_t *)dblock) < 0)
                HGOTO_ERROR(H5E_EARRAY, H5E_CANTDEPEND, FAIL,
                            "unable to create flush dependency between data block and parent, address = %llu",
                            (unsigned long long)dblock->addr);
            break;

        case H5AC_NOTIFY_ACTION_AFTER_FLUSH:
            /* Destroy flush dependency on extensible array header, if set */
            if (dblock->has_hdr_depend) {
                if (H5EA__destroy_flush_depend((H5AC_info_t *)dblock->hdr, (H5AC_info_t *)dblock) < 0)
                    HGOTO_ERROR(
                        H5E_EARRAY, H5E_CANTUNDEPEND, FAIL,
                        "unable to destroy flush dependency between direct block and header, address = %llu",
                        (unsigned long long)dblock->addr);
                dblock->has_hdr_depend = false;
            } /* end if */
            break;

        case H5AC_NOTIFY_ACTION_BEFORE_EVICT:
            /* Destroy flush dependency on parent */
            if (H5EA__destroy_flush_depend((H5AC_info_t *)dblock->parent, (H5AC_info_t *)dblock) < 0)
                HGOTO_ERROR(
                    H5E_EARRAY, H5E_CANTUNDEPEND, FAIL,
                    "unable to destroy flush dependency between data block and parent, address = %llu",
                    (unsigned long long)dblock->addr);

            /* Destroy flush dependency on extensible array header, if set */
            if (dblock->has_hdr_depend) {
                if (H5EA__destroy_flush_depend((H5AC_info_t *)dblock->hdr, (H5AC_info_t *)dblock) < 0)
                    HGOTO_ERROR(
                        H5E_EARRAY, H5E_CANTUNDEPEND, FAIL,
                        "unable to destroy flush dependency between data block and header, address = %llu",
                        (unsigned long long)dblock->addr);
                dblock->has_hdr_depend = false;
            } /* end if */

            /* Detach from 'top' proxy for extensible array */
            if (dblock->top_proxy) {
                if (H5AC_proxy_entry_remove_child(dblock->top_proxy, dblock) < 0)
                    HGOTO_ERROR(H5E_EARRAY, H5E_CANTUNDEPEND, FAIL,
                                "unable to destroy flush dependency between data block and "
                                "extensible array 'top' proxy");
                dblock->top_proxy = NULL;
            } /* end if */
            break;

        case H5AC_NOTIFY_ACTION_ENTRY_DIRTIED:
        case H5AC_NOTIFY_ACTION_ENTRY_CLEANED:
        case H5AC_NOTIFY_ACTION_CHILD_DIRTIED:
        case H5AC_NOTIFY_ACTION_CHILD_CLEANED:
        case H5AC_NOTIFY_ACTION_CHILD_UNSERIALIZED:
        case H5AC_NOTIFY_ACTION_CHILD_SERIALIZED:
            /* do nothing */
            break;

        default:
#ifdef NDEBUG
            HGOTO_ERROR(H5E_EARRAY, H5E_BADVALUE, FAIL, "unknown action from metadata cache");
#else  /* NDEBUG */
            assert(0 && "Unknown action?!?");
#endif /* NDEBUG */
    }  /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__cache_dblock_notify() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__cache_dblock_free_icr
 *
 * Purpose:	Destroy/release an "in core representation" of a data
 *              structure
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__cache_dblock_free_icr(void *thing)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(thing);

    /* Release the extensible array data block */
    if (H5EA__dblock_dest((H5EA_dblock_t *)thing) < 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTFREE, FAIL, "can't free extensible array data block");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__cache_dblock_free_icr() */

/*-------------------------------------------------------------------------
 * Function:    H5EA__cache_dblock_fsf_size
 *
 * Purpose:     Tell the metadata cache the actual amount of file space
 *              to free when a dblock entry is destroyed with the free
 *              file space block set.
 *
 *              This function is needed when the data block is paged, as
 *              the datablock header and all its pages are allocated as a
 *              single contiguous chunk of file space, and must be
 *              deallocated the same way.
 *
 *              The size of the chunk of memory in which the dblock
 *              header and all its pages is stored in the size field,
 *              so we simply pass that value back to the cache.
 *
 *              If the datablock is not paged, then the size field of
 *              the cache_info contains the correct size.  However this
 *              value will be the same as the size field, so we return
 *              the contents of the size field to the cache in this case
 *              as well.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__cache_dblock_fsf_size(const void *_thing, hsize_t *fsf_size)
{
    const H5EA_dblock_t *dblock = (const H5EA_dblock_t *)_thing; /* Pointer to the object */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(dblock);
    assert(dblock->cache_info.type == H5AC_EARRAY_DBLOCK);
    assert(fsf_size);

    *fsf_size = dblock->size;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5EA__cache_dblock_fsf_size() */

/*-------------------------------------------------------------------------
 * Function:    H5EA__cache_dblk_page_get_initial_load_size
 *
 * Purpose:     Compute the size of the data structure on disk.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__cache_dblk_page_get_initial_load_size(void *_udata, size_t *image_len)
{
    H5EA_dblk_page_cache_ud_t *udata = (H5EA_dblk_page_cache_ud_t *)_udata; /* User data */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(udata);
    assert(udata->hdr);
    assert(image_len);

    /* Set the image length size */
    *image_len = (size_t)H5EA_DBLK_PAGE_SIZE(udata->hdr);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5EA__cache_dblk_page_get_initial_load_size() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__cache_dblk_page_verify_chksum
 *
 * Purpose:     Verify the computed checksum of the data structure is the
 *              same as the stored chksum.
 *
 * Return:      Success:        true/false
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5EA__cache_dblk_page_verify_chksum(const void *_image, size_t len, void H5_ATTR_UNUSED *_udata)
{
    const uint8_t *image = (const uint8_t *)_image; /* Pointer into raw data buffer */
    uint32_t       stored_chksum;                   /* Stored metadata checksum value */
    uint32_t       computed_chksum;                 /* Computed metadata checksum value */
    htri_t         ret_value = true;

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(image);

    /* Get stored and computed checksums */
    H5F_get_checksums(image, len, &stored_chksum, &computed_chksum);

    if (stored_chksum != computed_chksum)
        ret_value = false;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__cache_dblk_page_verify_chksum() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__cache_dblk_page_deserialize
 *
 * Purpose:	Loads a data structure from the disk.
 *
 * Return:	Success:	Pointer to a new B-tree.
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5EA__cache_dblk_page_deserialize(const void *_image, size_t len, void *_udata, bool H5_ATTR_UNUSED *dirty)
{
    H5EA_dblk_page_t          *dblk_page = NULL; /* Data block page info */
    H5EA_dblk_page_cache_ud_t *udata =
        (H5EA_dblk_page_cache_ud_t *)_udata;        /* User data for loading data block page */
    const uint8_t *image = (const uint8_t *)_image; /* Pointer into raw data buffer */
    uint32_t       stored_chksum;                   /* Stored metadata checksum value */
    void          *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(udata);
    assert(udata->hdr);
    assert(udata->parent);
    assert(H5_addr_defined(udata->dblk_page_addr));

    /* Allocate the extensible array data block page */
    if (NULL == (dblk_page = H5EA__dblk_page_alloc(udata->hdr, udata->parent)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTALLOC, NULL,
                    "memory allocation failed for extensible array data block page");

    /* Set the extensible array data block page's information */
    dblk_page->addr = udata->dblk_page_addr;

    /* Internal information */

    /* Decode elements in data block page */
    /* Convert from raw elements on disk into native elements in memory */
    if ((udata->hdr->cparam.cls->decode)(image, dblk_page->elmts, udata->hdr->dblk_page_nelmts,
                                         udata->hdr->cb_ctx) < 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTDECODE, NULL, "can't decode extensible array data elements");
    image += (udata->hdr->dblk_page_nelmts * udata->hdr->cparam.raw_elmt_size);

    /* Sanity check */
    /* (allow for checksum not decoded yet) */
    assert((size_t)(image - (const uint8_t *)_image) == (len - H5EA_SIZEOF_CHKSUM));

    /* Set the data block page's size */
    dblk_page->size = len;

    /* checksum verification already done in verify_chksum cb */

    /* Metadata checksum */
    UINT32DECODE(image, stored_chksum);

    /* Sanity check */
    assert((size_t)(image - (const uint8_t *)_image) == dblk_page->size);

    /* Set return value */
    ret_value = dblk_page;

done:
    /* Release resources */
    if (!ret_value)
        if (dblk_page && H5EA__dblk_page_dest(dblk_page) < 0)
            HDONE_ERROR(H5E_EARRAY, H5E_CANTFREE, NULL, "unable to destroy extensible array data block page");
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__cache_dblk_page_deserialize() */

/*-------------------------------------------------------------------------
 * Function:    H5EA__cache_dblk_page_image_len
 *
 * Purpose:     Compute the size of the data structure on disk.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__cache_dblk_page_image_len(const void *_thing, size_t *image_len)
{
    const H5EA_dblk_page_t *dblk_page = (const H5EA_dblk_page_t *)_thing; /* Pointer to the object */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(dblk_page);
    assert(image_len);

    /* Set the image length size */
    *image_len = dblk_page->size;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5EA__cache_dblk_page_image_len() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__cache_dblk_page_serialize
 *
 * Purpose:	Flushes a dirty object to disk.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__cache_dblk_page_serialize(const H5F_t H5_ATTR_NDEBUG_UNUSED *f, void *_image, size_t H5_ATTR_UNUSED len,
                                void *_thing)
{
    H5EA_dblk_page_t *dblk_page = (H5EA_dblk_page_t *)_thing; /* Pointer to the object to serialize */
    uint8_t          *image     = (uint8_t *)_image;          /* Pointer into raw data buffer */
    uint32_t          metadata_chksum;                        /* Computed metadata checksum value */
    herr_t            ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(f);
    assert(image);
    assert(dblk_page);
    assert(dblk_page->hdr);

    /* Internal information */

    /* Encode elements in data block page */

    /* Convert from native elements in memory into raw elements on disk */
    if ((dblk_page->hdr->cparam.cls->encode)(image, dblk_page->elmts, dblk_page->hdr->dblk_page_nelmts,
                                             dblk_page->hdr->cb_ctx) < 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTENCODE, FAIL, "can't encode extensible array data elements");
    image += (dblk_page->hdr->dblk_page_nelmts * dblk_page->hdr->cparam.raw_elmt_size);

    /* Compute metadata checksum */
    metadata_chksum = H5_checksum_metadata(_image, (size_t)(image - (uint8_t *)_image), 0);

    /* Metadata checksum */
    UINT32ENCODE(image, metadata_chksum);

    /* Sanity check */
    assert((size_t)(image - (uint8_t *)_image) == len);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__cache_dblk_page_serialize() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__cache_dblk_page_notify
 *
 * Purpose:	Handle cache action notifications
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__cache_dblk_page_notify(H5AC_notify_action_t action, void *_thing)
{
    H5EA_dblk_page_t *dblk_page = (H5EA_dblk_page_t *)_thing; /* Pointer to the object */
    herr_t            ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(dblk_page);

    /* Determine which action to take */
    switch (action) {
        case H5AC_NOTIFY_ACTION_AFTER_INSERT:
        case H5AC_NOTIFY_ACTION_AFTER_LOAD:
            /* Create flush dependency on parent */
            if (H5EA__create_flush_depend((H5AC_info_t *)dblk_page->parent, (H5AC_info_t *)dblk_page) < 0)
                HGOTO_ERROR(
                    H5E_EARRAY, H5E_CANTDEPEND, FAIL,
                    "unable to create flush dependency between data block page and parent, address = %llu",
                    (unsigned long long)dblk_page->addr);
            break;

        case H5AC_NOTIFY_ACTION_AFTER_FLUSH:
            /* Destroy flush dependency on extensible array header, if set */
            if (dblk_page->has_hdr_depend) {
                if (H5EA__destroy_flush_depend((H5AC_info_t *)dblk_page->hdr, (H5AC_info_t *)dblk_page) < 0)
                    HGOTO_ERROR(H5E_EARRAY, H5E_CANTUNDEPEND, FAIL,
                                "unable to destroy flush dependency between data block page and header, "
                                "address = %llu",
                                (unsigned long long)dblk_page->addr);
                dblk_page->has_hdr_depend = false;
            } /* end if */
            break;

        case H5AC_NOTIFY_ACTION_BEFORE_EVICT:
            /* Destroy flush dependency on parent */
            if (H5EA__destroy_flush_depend((H5AC_info_t *)dblk_page->parent, (H5AC_info_t *)dblk_page) < 0)
                HGOTO_ERROR(
                    H5E_EARRAY, H5E_CANTUNDEPEND, FAIL,
                    "unable to destroy flush dependency between data block page and parent, address = %llu",
                    (unsigned long long)dblk_page->addr);

            /* Destroy flush dependency on extensible array header, if set */
            if (dblk_page->has_hdr_depend) {
                if (H5EA__destroy_flush_depend((H5AC_info_t *)dblk_page->hdr, (H5AC_info_t *)dblk_page) < 0)
                    HGOTO_ERROR(H5E_EARRAY, H5E_CANTUNDEPEND, FAIL,
                                "unable to destroy flush dependency between data block page and header, "
                                "address = %llu",
                                (unsigned long long)dblk_page->addr);
                dblk_page->has_hdr_depend = false;
            } /* end if */

            /* Detach from 'top' proxy for extensible array */
            if (dblk_page->top_proxy) {
                if (H5AC_proxy_entry_remove_child(dblk_page->top_proxy, dblk_page) < 0)
                    HGOTO_ERROR(H5E_EARRAY, H5E_CANTUNDEPEND, FAIL,
                                "unable to destroy flush dependency between data block page and "
                                "extensible array 'top' proxy");
                dblk_page->top_proxy = NULL;
            } /* end if */
            break;

        case H5AC_NOTIFY_ACTION_ENTRY_DIRTIED:
        case H5AC_NOTIFY_ACTION_ENTRY_CLEANED:
        case H5AC_NOTIFY_ACTION_CHILD_DIRTIED:
        case H5AC_NOTIFY_ACTION_CHILD_CLEANED:
        case H5AC_NOTIFY_ACTION_CHILD_UNSERIALIZED:
        case H5AC_NOTIFY_ACTION_CHILD_SERIALIZED:
            /* do nothing */
            break;

        default:
#ifdef NDEBUG
            HGOTO_ERROR(H5E_EARRAY, H5E_BADVALUE, FAIL, "unknown action from metadata cache");
#else  /* NDEBUG */
            assert(0 && "Unknown action?!?");
#endif /* NDEBUG */
    }  /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__cache_dblk_page_notify() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__cache_dblk_page_free_icr
 *
 * Purpose:	Destroy/release an "in core representation" of a data
 *              structure
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5EA__cache_dblk_page_free_icr(void *thing)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(thing);

    /* Release the extensible array data block page */
    if (H5EA__dblk_page_dest((H5EA_dblk_page_t *)thing) < 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTFREE, FAIL, "can't free extensible array data block page");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__cache_dblk_page_free_icr() */
