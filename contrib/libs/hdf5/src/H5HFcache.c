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
 * Created:		H5HFcache.c
 *
 * Purpose:		Implement fractal heap metadata cache methods
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5HFmodule.h" /* This source code file is part of the H5HF module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5ACprivate.h" /* Metadata cache			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5HFpkg.h"     /* Fractal heaps			*/
#include "H5MFprivate.h" /* File memory management		*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5VMprivate.h" /* Vectors and arrays 			*/
#include "H5WBprivate.h" /* Wrapped Buffers                      */

/****************/
/* Local Macros */
/****************/

/* Fractal heap format version #'s */
#define H5HF_HDR_VERSION    0 /* Header */
#define H5HF_DBLOCK_VERSION 0 /* Direct block */
#define H5HF_IBLOCK_VERSION 0 /* Indirect block */

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

/* Local encode/decode routines */
static herr_t H5HF__hdr_prefix_decode(H5HF_hdr_t *hdr, const uint8_t **image_ref);
static herr_t H5HF__dtable_encode(H5F_t *f, uint8_t **pp, const H5HF_dtable_t *dtable);
static herr_t H5HF__dtable_decode(H5F_t *f, const uint8_t **pp, H5HF_dtable_t *dtable);

/* Metadata cache (H5AC) callbacks */
static herr_t H5HF__cache_hdr_get_initial_load_size(void *udata, size_t *image_len);
static herr_t H5HF__cache_hdr_get_final_load_size(const void *image_ptr, size_t image_len, void *udata,
                                                  size_t *actual_len);
static htri_t H5HF__cache_hdr_verify_chksum(const void *image_ptr, size_t len, void *udata_ptr);
static void  *H5HF__cache_hdr_deserialize(const void *image, size_t len, void *udata, bool *dirty);
static herr_t H5HF__cache_hdr_image_len(const void *thing, size_t *image_len);
static herr_t H5HF__cache_hdr_pre_serialize(H5F_t *f, void *thing, haddr_t addr, size_t len,
                                            haddr_t *new_addr, size_t *new_len, unsigned *flags);
static herr_t H5HF__cache_hdr_serialize(const H5F_t *f, void *image, size_t len, void *thing);
static herr_t H5HF__cache_hdr_free_icr(void *thing);

static herr_t H5HF__cache_iblock_get_initial_load_size(void *udata, size_t *image_len);
static htri_t H5HF__cache_iblock_verify_chksum(const void *image_ptr, size_t len, void *udata_ptr);
static void  *H5HF__cache_iblock_deserialize(const void *image, size_t len, void *udata, bool *dirty);
static herr_t H5HF__cache_iblock_image_len(const void *thing, size_t *image_len);
static herr_t H5HF__cache_iblock_pre_serialize(H5F_t *f, void *thing, haddr_t addr, size_t len,
                                               haddr_t *new_addr, size_t *new_len, unsigned *flags);
static herr_t H5HF__cache_iblock_serialize(const H5F_t *f, void *image, size_t len, void *thing);
static herr_t H5HF__cache_iblock_notify(H5AC_notify_action_t action, void *thing);
static herr_t H5HF__cache_iblock_free_icr(void *thing);

static herr_t H5HF__cache_dblock_get_initial_load_size(void *udata, size_t *image_len);
static htri_t H5HF__cache_dblock_verify_chksum(const void *image_ptr, size_t len, void *udata_ptr);
static void  *H5HF__cache_dblock_deserialize(const void *image, size_t len, void *udata, bool *dirty);
static herr_t H5HF__cache_dblock_image_len(const void *thing, size_t *image_len);
static herr_t H5HF__cache_dblock_pre_serialize(H5F_t *f, void *thing, haddr_t addr, size_t len,
                                               haddr_t *new_addr, size_t *new_len, unsigned *flags);
static herr_t H5HF__cache_dblock_serialize(const H5F_t *f, void *image, size_t len, void *thing);
static herr_t H5HF__cache_dblock_notify(H5AC_notify_action_t action, void *thing);
static herr_t H5HF__cache_dblock_free_icr(void *thing);
static herr_t H5HF__cache_dblock_fsf_size(const void *_thing, hsize_t *fsf_size);

/* Debugging Function Prototypes */
#ifndef NDEBUG
static herr_t H5HF__cache_verify_hdr_descendants_clean(H5F_t *f, H5HF_hdr_t *hdr, bool *fd_clean,
                                                       bool *clean);
static herr_t H5HF__cache_verify_iblock_descendants_clean(H5F_t *f, haddr_t fd_parent_addr,
                                                          H5HF_indirect_t *iblock, unsigned *iblock_status,
                                                          bool *fd_clean, bool *clean);
static herr_t H5HF__cache_verify_iblocks_dblocks_clean(H5F_t *f, haddr_t fd_parent_addr,
                                                       H5HF_indirect_t *iblock, bool *fd_clean, bool *clean,
                                                       bool *has_dblocks);
static herr_t H5HF__cache_verify_descendant_iblocks_clean(H5F_t *f, haddr_t fd_parent_addr,
                                                          H5HF_indirect_t *iblock, bool *fd_clean,
                                                          bool *clean, bool *has_iblocks);
#endif /* NDEBUG */

/*********************/
/* Package Variables */
/*********************/

/* H5HF header inherits cache-like properties from H5AC */
const H5AC_class_t H5AC_FHEAP_HDR[1] = {{
    H5AC_FHEAP_HDR_ID,                     /* Metadata client ID */
    "fractal heap header",                 /* Metadata client name (for debugging) */
    H5FD_MEM_FHEAP_HDR,                    /* File space memory type for client */
    H5AC__CLASS_SPECULATIVE_LOAD_FLAG,     /* Client class behavior flags */
    H5HF__cache_hdr_get_initial_load_size, /* 'get_initial_load_size' callback */
    H5HF__cache_hdr_get_final_load_size,   /* 'get_final_load_size' callback */
    H5HF__cache_hdr_verify_chksum,         /* 'verify_chksum' callback */
    H5HF__cache_hdr_deserialize,           /* 'deserialize' callback */
    H5HF__cache_hdr_image_len,             /* 'image_len' callback */
    H5HF__cache_hdr_pre_serialize,         /* 'pre_serialize' callback */
    H5HF__cache_hdr_serialize,             /* 'serialize' callback */
    NULL,                                  /* 'notify' callback */
    H5HF__cache_hdr_free_icr,              /* 'free_icr' callback */
    NULL,                                  /* 'fsf_size' callback */
}};

/* H5HF indirect block inherits cache-like properties from H5AC */
const H5AC_class_t H5AC_FHEAP_IBLOCK[1] = {{
    H5AC_FHEAP_IBLOCK_ID,                     /* Metadata client ID */
    "fractal heap indirect block",            /* Metadata client name (for debugging) */
    H5FD_MEM_FHEAP_IBLOCK,                    /* File space memory type for client */
    H5AC__CLASS_NO_FLAGS_SET,                 /* Client class behavior flags */
    H5HF__cache_iblock_get_initial_load_size, /* 'get_initial_load_size' callback */
    NULL,                                     /* 'get_final_load_size' callback */
    H5HF__cache_iblock_verify_chksum,         /* 'verify_chksum' callback */
    H5HF__cache_iblock_deserialize,           /* 'deserialize' callback */
    H5HF__cache_iblock_image_len,             /* 'image_len' callback */
    H5HF__cache_iblock_pre_serialize,         /* 'pre_serialize' callback */
    H5HF__cache_iblock_serialize,             /* 'serialize' callback */
    H5HF__cache_iblock_notify,                /* 'notify' callback */
    H5HF__cache_iblock_free_icr,              /* 'free_icr' callback */
    NULL,                                     /* 'fsf_size' callback */
}};

/* H5HF direct block inherits cache-like properties from H5AC */
const H5AC_class_t H5AC_FHEAP_DBLOCK[1] = {{
    H5AC_FHEAP_DBLOCK_ID,                     /* Metadata client ID */
    "fractal heap direct block",              /* Metadata client name (for debugging) */
    H5FD_MEM_FHEAP_DBLOCK,                    /* File space memory type for client */
    H5AC__CLASS_NO_FLAGS_SET,                 /* Client class behavior flags */
    H5HF__cache_dblock_get_initial_load_size, /* 'get_initial_load_size' callback */
    NULL,                                     /* 'get_final_load_size' callback */
    H5HF__cache_dblock_verify_chksum,         /* 'verify_chksum' callback */
    H5HF__cache_dblock_deserialize,           /* 'deserialize' callback */
    H5HF__cache_dblock_image_len,             /* 'image_len' callback */
    H5HF__cache_dblock_pre_serialize,         /* 'pre_serialize' callback */
    H5HF__cache_dblock_serialize,             /* 'serialize' callback */
    H5HF__cache_dblock_notify,                /* 'notify' callback */
    H5HF__cache_dblock_free_icr,              /* 'free_icr' callback */
    H5HF__cache_dblock_fsf_size,              /* 'fsf_size' callback */
}};

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Declare a free list to manage heap direct block data to/from disk */
H5FL_BLK_DEFINE(direct_block);

/*-------------------------------------------------------------------------
 * Function:	H5HF__hdr_prefix_decode()
 *
 * Purpose:	Decode a fractal heap header's prefix
 *
 * Return:	Success:	SUCCEED
 *		Failure:	FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__hdr_prefix_decode(H5HF_hdr_t *hdr, const uint8_t **image_ref)
{
    const uint8_t *image     = *image_ref; /* Pointer into into supplied image */
    herr_t         ret_value = SUCCEED;    /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(hdr);
    assert(image);

    /* Magic number */
    if (memcmp(image, H5HF_HDR_MAGIC, (size_t)H5_SIZEOF_MAGIC) != 0)
        HGOTO_ERROR(H5E_HEAP, H5E_BADVALUE, FAIL, "wrong fractal heap header signature");
    image += H5_SIZEOF_MAGIC;

    /* Version */
    if (*image++ != H5HF_HDR_VERSION)
        HGOTO_ERROR(H5E_HEAP, H5E_VERSION, FAIL, "wrong fractal heap header version");

    /* General heap information */
    UINT16DECODE(image, hdr->id_len);     /* Heap ID length */
    UINT16DECODE(image, hdr->filter_len); /* I/O filters' encoded length */

    /* Update the image buffer pointer */
    *image_ref = image;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__hdr_prefix_decode() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__dtable_decode
 *
 * Purpose:	Decodes the metadata for a doubling table
 *
 * Return:	Success:	Pointer to a new fractal heap
 *
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__dtable_decode(H5F_t *f, const uint8_t **pp, H5HF_dtable_t *dtable)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(f);
    assert(pp && *pp);
    assert(dtable);

    /* Table width */
    UINT16DECODE(*pp, dtable->cparam.width);

    /* Starting block size */
    H5F_DECODE_LENGTH(f, *pp, dtable->cparam.start_block_size);

    /* Maximum direct block size */
    H5F_DECODE_LENGTH(f, *pp, dtable->cparam.max_direct_size);

    /* Maximum heap size (as # of bits) */
    UINT16DECODE(*pp, dtable->cparam.max_index);

    /* Starting # of rows in root indirect block */
    UINT16DECODE(*pp, dtable->cparam.start_root_rows);

    /* Address of table */
    H5F_addr_decode(f, pp, &(dtable->table_addr));

    /* Current # of rows in root indirect block */
    UINT16DECODE(*pp, dtable->curr_root_rows);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HF__dtable_decode() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__dtable_encode
 *
 * Purpose:	Encodes the metadata for a doubling table
 *
 * Return:	Success:	Pointer to a new fractal heap
 *
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__dtable_encode(H5F_t *f, uint8_t **pp, const H5HF_dtable_t *dtable)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(f);
    assert(pp && *pp);
    assert(dtable);

    /* Table width */
    UINT16ENCODE(*pp, dtable->cparam.width);

    /* Starting block size */
    H5F_ENCODE_LENGTH(f, *pp, dtable->cparam.start_block_size);

    /* Maximum direct block size */
    H5F_ENCODE_LENGTH(f, *pp, dtable->cparam.max_direct_size);

    /* Maximum heap size (as # of bits) */
    UINT16ENCODE(*pp, dtable->cparam.max_index);

    /* Starting # of rows in root indirect block */
    UINT16ENCODE(*pp, dtable->cparam.start_root_rows);

    /* Address of root direct/indirect block */
    H5F_addr_encode(f, pp, dtable->table_addr);

    /* Current # of rows in root indirect block */
    UINT16ENCODE(*pp, dtable->curr_root_rows);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HF__dtable_encode() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__cache_hdr_get_initial_load_size()
 *
 * Purpose:	Determine the size of the fractal heap header on disk,
 *		and set *image_len to this value.
 *
 *		Note also that the value returned by this function presumes that
 *		there is no I/O filtering data in the header.  If there is, the
 *		size reported will be too small, and H5C__load_entry()
 *		will have to make two tries to load the fractal heap header.
 *
 * Return:	Success:	SUCCEED
 *		Failure:	FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__cache_hdr_get_initial_load_size(void *_udata, size_t *image_len)
{
    H5HF_hdr_cache_ud_t *udata = (H5HF_hdr_cache_ud_t *)_udata; /* Pointer to user data */
    H5HF_hdr_t           dummy_hdr;                             /* Dummy header -- to compute size */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(udata);
    assert(image_len);

    /* Set the internal parameters for the heap */
    dummy_hdr.f           = udata->f;
    dummy_hdr.sizeof_size = H5F_SIZEOF_SIZE(udata->f);
    dummy_hdr.sizeof_addr = H5F_SIZEOF_ADDR(udata->f);

    /* Compute the 'base' size of the fractal heap header on disk */
    *image_len = (size_t)H5HF_HEADER_SIZE(&dummy_hdr);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HF__cache_hdr_get_initial_load_size() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__cache_hdr_get_final_load_size()
 *
 * Purpose:	Determine the final size of the fractal heap header on disk,
 *		and set *actual_len to this value.
 *
 * Return:	Success:	SUCCEED
 *		Failure:	FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__cache_hdr_get_final_load_size(const void *_image, size_t H5_ATTR_NDEBUG_UNUSED image_len, void *_udata,
                                    size_t *actual_len)
{
    H5HF_hdr_t           hdr;                                       /* Temporary fractal heap header */
    const uint8_t       *image     = (const uint8_t *)_image;       /* Pointer into into supplied image */
    H5HF_hdr_cache_ud_t *udata     = (H5HF_hdr_cache_ud_t *)_udata; /* User data for callback */
    herr_t               ret_value = SUCCEED;                       /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(image);
    assert(udata);
    assert(actual_len);
    assert(*actual_len == image_len);

    /* Deserialize the fractal heap header's prefix */
    if (H5HF__hdr_prefix_decode(&hdr, &image) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTDECODE, FAIL, "can't decode fractal heap header prefix");

    /* Check for I/O filter info on this heap */
    if (hdr.filter_len > 0)
        /* Compute the extra heap header size */
        *actual_len += (size_t)(H5F_SIZEOF_SIZE(udata->f) /* Size of size for filtered root direct block */
                                + (unsigned)4      /* Size of filter mask for filtered root direct block */
                                + hdr.filter_len); /* Size of encoded I/O filter info */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__cache_hdr_get_final_load_size() */

/*-------------------------------------------------------------------------
 * Function:    H5HF__cache_hdr_verify_chksum
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
H5HF__cache_hdr_verify_chksum(const void *_image, size_t len, void H5_ATTR_UNUSED *_udata)
{
    const uint8_t *image = (const uint8_t *)_image; /* Pointer into raw data buffer */
    uint32_t       stored_chksum;                   /* Stored metadata checksum value */
    uint32_t       computed_chksum;                 /* Computed metadata checksum value */
    htri_t         ret_value = true;                /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(image);

    /* Get stored and computed checksums */
    H5F_get_checksums(image, len, &stored_chksum, &computed_chksum);

    if (stored_chksum != computed_chksum)
        ret_value = false;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__cache_hdr_verify_chksum() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__cache_hdr_deserialize
 *
 * Purpose:	Given a buffer containing an on disk image of a fractal heap
 *		header block, allocate an instance of H5HF_hdr_t, load the contents
 *		of the buffer into into the new instance of H5HF_hdr_t, and then
 *		return a pointer to the new instance.
 *
 * Return:	Success:	Pointer to in core representation
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5HF__cache_hdr_deserialize(const void *_image, size_t len, void *_udata, bool H5_ATTR_UNUSED *dirty)
{
    H5HF_hdr_t          *hdr   = NULL;                          /* Fractal heap info */
    H5HF_hdr_cache_ud_t *udata = (H5HF_hdr_cache_ud_t *)_udata; /* User data for callback */
    const uint8_t       *image = (const uint8_t *)_image;       /* Pointer into into supplied image */
    uint32_t             stored_chksum;                         /* Stored metadata checksum value */
    uint8_t              heap_flags;                            /* Status flags for heap */
    void                *ret_value = NULL;                      /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(image);
    assert(len > 0);
    assert(udata);
    assert(dirty);

    /* Allocate space for the fractal heap data structure */
    if (NULL == (hdr = H5HF__hdr_alloc(udata->f)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Deserialize the fractal heap header's prefix */
    if (H5HF__hdr_prefix_decode(hdr, &image) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTDECODE, NULL, "can't decode fractal heap header prefix");

    /* Heap status flags */
    /* (bit 0: "huge" object IDs have wrapped) */
    /* (bit 1: checksum direct blocks) */
    heap_flags            = *image++;
    hdr->huge_ids_wrapped = heap_flags & H5HF_HDR_FLAGS_HUGE_ID_WRAPPED;
    hdr->checksum_dblocks = heap_flags & H5HF_HDR_FLAGS_CHECKSUM_DBLOCKS;

    /* "Huge" object information */
    UINT32DECODE(image, hdr->max_man_size);                 /* Max. size of "managed" objects */
    H5F_DECODE_LENGTH(udata->f, image, hdr->huge_next_id);  /* Next ID to use for "huge" object */
    H5F_addr_decode(udata->f, &image, &hdr->huge_bt2_addr); /* Address of "huge" object tracker B-tree */

    /* "Managed" object free space information */
    H5F_DECODE_LENGTH(udata->f, image,
                      hdr->total_man_free);           /* Internal free space in managed direct blocks */
    H5F_addr_decode(udata->f, &image, &hdr->fs_addr); /* Address of free section header */

    /* Heap statistics */
    H5F_DECODE_LENGTH(udata->f, image, hdr->man_size);
    H5F_DECODE_LENGTH(udata->f, image, hdr->man_alloc_size);
    H5F_DECODE_LENGTH(udata->f, image, hdr->man_iter_off);
    H5F_DECODE_LENGTH(udata->f, image, hdr->man_nobjs);
    H5F_DECODE_LENGTH(udata->f, image, hdr->huge_size);
    H5F_DECODE_LENGTH(udata->f, image, hdr->huge_nobjs);
    H5F_DECODE_LENGTH(udata->f, image, hdr->tiny_size);
    H5F_DECODE_LENGTH(udata->f, image, hdr->tiny_nobjs);

    /* Managed objects' doubling-table info */
    if (H5HF__dtable_decode(hdr->f, &image, &(hdr->man_dtable)) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTENCODE, NULL, "unable to encode managed obj. doubling table info");

    /* Set the fractal heap header's 'base' size */
    hdr->heap_size = (size_t)H5HF_HEADER_SIZE(hdr);

    /* Sanity check */
    /* (allow for checksum not decoded yet) */
    assert((size_t)(image - (const uint8_t *)_image) == (hdr->heap_size - H5HF_SIZEOF_CHKSUM));

    /* Check for I/O filter information to decode */
    if (hdr->filter_len > 0) {
        H5O_pline_t *pline; /* Pipeline information from the header on disk */

        /* Sanity check */
        assert(len > hdr->heap_size); /* A header with filter info is > than a standard header */

        /* Compute the heap header's size */
        hdr->heap_size += (size_t)(hdr->sizeof_size /* Size of size for filtered root direct block */
                                   + (unsigned)4    /* Size of filter mask for filtered root direct block */
                                   + hdr->filter_len); /* Size of encoded I/O filter info */

        /* Decode the size of a filtered root direct block */
        H5F_DECODE_LENGTH(udata->f, image, hdr->pline_root_direct_size);

        /* Decode the filter mask for a filtered root direct block */
        UINT32DECODE(image, hdr->pline_root_direct_filter_mask);

        /* Decode I/O filter information */
        if (NULL == (pline = (H5O_pline_t *)H5O_msg_decode(hdr->f, NULL, H5O_PLINE_ID, len, image)))
            HGOTO_ERROR(H5E_HEAP, H5E_CANTDECODE, NULL, "can't decode I/O pipeline filters");

        /* Advance past filter info to checksum */
        image += hdr->filter_len;

        /* Copy the information into the header's I/O pipeline structure */
        if (NULL == H5O_msg_copy(H5O_PLINE_ID, pline, &(hdr->pline)))
            HGOTO_ERROR(H5E_HEAP, H5E_CANTCOPY, NULL, "can't copy I/O filter pipeline");

        /* Release the space allocated for the I/O pipeline filters */
        H5O_msg_free(H5O_PLINE_ID, pline);
    } /* end if */

    /* Metadata checksum */
    UINT32DECODE(image, stored_chksum);

    /* Sanity check */
    assert((size_t)(image - (const uint8_t *)_image) == hdr->heap_size);

    /* Finish initialization of heap header */
    if (H5HF__hdr_finish_init(hdr) < 0)
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTINIT, NULL, "can't finish initializing shared fractal heap header");

    /* Set return value */
    ret_value = (void *)hdr;

done:
    if (!ret_value && hdr)
        if (H5HF__hdr_free(hdr) < 0)
            HDONE_ERROR(H5E_HEAP, H5E_CANTRELEASE, NULL, "unable to release fractal heap header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__cache_hdr_deserialize() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__cache_hdr_image_len
 *
 * Purpose:	Return the actual size of the fractal heap header on
 *		disk image.
 *
 *		If the header contains filter information, this size will be
 *		larger than the value returned by H5HF__cache_hdr_get_initial_load_size().
 *
 * Return:	Success:	SUCCEED
 *		Failure:	FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__cache_hdr_image_len(const void *_thing, size_t *image_len)
{
    const H5HF_hdr_t *hdr = (const H5HF_hdr_t *)_thing; /* Fractal heap info */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(hdr);
    assert(hdr->cache_info.type == H5AC_FHEAP_HDR);
    assert(image_len);

    *image_len = hdr->heap_size;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HF__cache_hdr_image_len() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__cache_hdr_pre_serialize
 *
 * Purpose:	As best I can tell, fractal heap header blocks are always
 *		allocated in real file space.  Thus this routine simply verifies
 *		this, verifies that the len parameter contains the expected
 *		value, and returns an error if either of these checks fail.
 *
 *		When compiled in debug mode, the function also verifies that all
 *		indirect and direct blocks that are children of the header are
 *		either clean, or not in the metadata cache.
 *
 * Return:	Success:	SUCCEED
 *		Failure:	FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__cache_hdr_pre_serialize(H5F_t *f, void *_thing, haddr_t addr, size_t len,
                              haddr_t H5_ATTR_UNUSED *new_addr, size_t H5_ATTR_UNUSED *new_len,
                              unsigned *flags)
{
    H5HF_hdr_t *hdr       = (H5HF_hdr_t *)_thing; /* Fractal heap info */
    herr_t      ret_value = SUCCEED;              /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(hdr);
    assert(hdr->cache_info.type == H5AC_FHEAP_HDR);
    assert(H5_addr_defined(addr));
    assert(addr == hdr->heap_addr);
    assert(new_addr);
    assert(new_len);
    assert(flags);

#ifndef NDEBUG
    {
        bool descendants_clean = true;
        bool fd_children_clean = true;

        /* Verify that flush dependencies are working correctly.  Do this
         * by verifying that either:
         *
         * 1) the header has a root iblock, and that the root iblock and all
         *    of its children are clean, or
         *
         * 2) The header has a root dblock, which is clean, or
         *
         * 3) The heap is empty, and thus the header has neither a root
         *    iblock no a root dblock.  In this case, the flush ordering
         *    constraint is met by default.
         *
         * Do this with a call to H5HF__cache_verify_hdr_descendants_clean().
         *
         * Note that descendants need not be clean if the pre_serialize call
         * is made during a cache serialization instead of an entry or cache
         * flush.
         *
         * Note also that with the recent change in the definition of flush
         * dependency, not all descendants need be clean -- only direct flush
         * dependency children.
         *
         * Finally, observe that the H5HF__cache_verify_hdr_descendants_clean()
         * call still looks for dirty descendants.  At present we do not check
         * this value.
         */
        if (H5HF__cache_verify_hdr_descendants_clean((H5F_t *)f, hdr, &fd_children_clean,
                                                     &descendants_clean) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_SYSTEM, FAIL, "can't verify hdr descendants clean.");
        assert(fd_children_clean);
    }
#endif /* NDEBUG */

    if (H5F_IS_TMP_ADDR(f, addr))
        HGOTO_ERROR(H5E_HEAP, H5E_BADVALUE, FAIL, "addr in temporary space?!?.");

    if (len != hdr->heap_size)
        HGOTO_ERROR(H5E_HEAP, H5E_BADVALUE, FAIL, "unexpected image len.");

    *flags = 0;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__cache_hdr_pre_serialize() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__cache_hdr_serialize
 *
 * Purpose:	Construct the on disk image of the header, and place it in
 *		the buffer pointed to by image.  Return SUCCEED on success,
 *		and FAIL on failure.
 *
 * Return:	Success:	SUCCEED
 *		Failure:	FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__cache_hdr_serialize(const H5F_t *f, void *_image, size_t H5_ATTR_NDEBUG_UNUSED len, void *_thing)
{
    H5HF_hdr_t *hdr   = (H5HF_hdr_t *)_thing; /* Fractal heap info */
    uint8_t    *image = (uint8_t *)_image;    /* Pointer into raw data buffer */
    uint8_t     heap_flags;                   /* Status flags for heap */
    uint32_t    metadata_chksum;              /* Computed metadata checksum value */
    herr_t      ret_value = SUCCEED;          /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(image);
    assert(hdr);
    assert(hdr->cache_info.type == H5AC_FHEAP_HDR);
    assert(len == hdr->heap_size);

    /* Set the shared heap header's file context for this operation */
    H5_GCC_DIAG_OFF("discarded-qualifiers")
    H5_CLANG_DIAG_OFF("incompatible-pointer-types-discards-qualifiers")
    hdr->f = f;
    H5_GCC_DIAG_ON("discarded-qualifiers")
    H5_CLANG_DIAG_ON("incompatible-pointer-types-discards-qualifiers")

    /* Magic number */
    H5MM_memcpy(image, H5HF_HDR_MAGIC, (size_t)H5_SIZEOF_MAGIC);
    image += H5_SIZEOF_MAGIC;

    /* Version # */
    *image++ = H5HF_HDR_VERSION;

    /* General heap information */
    UINT16ENCODE(image, hdr->id_len);     /* Heap ID length */
    UINT16ENCODE(image, hdr->filter_len); /* I/O filters' encoded length */

    /* Heap status flags */
    /* (bit 0: "huge" object IDs have wrapped) */
    /* (bit 1: checksum direct blocks) */
    heap_flags = 0;
    heap_flags = (uint8_t)(heap_flags | (hdr->huge_ids_wrapped ? H5HF_HDR_FLAGS_HUGE_ID_WRAPPED : 0));
    heap_flags = (uint8_t)(heap_flags | (hdr->checksum_dblocks ? H5HF_HDR_FLAGS_CHECKSUM_DBLOCKS : 0));
    *image++   = heap_flags;

    /* "Huge" object information */
    UINT32ENCODE(image, hdr->max_man_size);         /* Max. size of "managed" objects */
    H5F_ENCODE_LENGTH(f, image, hdr->huge_next_id); /* Next ID to use for "huge" object */
    H5F_addr_encode(f, &image, hdr->huge_bt2_addr); /* Address of "huge" object tracker B-tree */

    /* "Managed" object free space information */
    H5F_ENCODE_LENGTH(f, image, hdr->total_man_free); /* Internal free space in managed direct blocks */
    H5F_addr_encode(f, &image, hdr->fs_addr);         /* Address of free section header */

    /* Heap statistics */
    H5F_ENCODE_LENGTH(f, image, hdr->man_size);
    H5F_ENCODE_LENGTH(f, image, hdr->man_alloc_size);
    H5F_ENCODE_LENGTH(f, image, hdr->man_iter_off);
    H5F_ENCODE_LENGTH(f, image, hdr->man_nobjs);
    H5F_ENCODE_LENGTH(f, image, hdr->huge_size);
    H5F_ENCODE_LENGTH(f, image, hdr->huge_nobjs);
    H5F_ENCODE_LENGTH(f, image, hdr->tiny_size);
    H5F_ENCODE_LENGTH(f, image, hdr->tiny_nobjs);

    /* Managed objects' doubling-table info */
    if (H5HF__dtable_encode(hdr->f, &image, &(hdr->man_dtable)) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTENCODE, FAIL, "unable to encode managed obj. doubling table info");

    /* Check for I/O filter information to encode */
    if (hdr->filter_len > 0) {
        /* Encode the size of a filtered root direct block */
        H5F_ENCODE_LENGTH(f, image, hdr->pline_root_direct_size);

        /* Encode the filter mask for a filtered root direct block */
        UINT32ENCODE(image, hdr->pline_root_direct_filter_mask);

        /* Encode I/O filter information */
        if (H5O_msg_encode(hdr->f, H5O_PLINE_ID, false, image, &(hdr->pline)) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTENCODE, FAIL, "can't encode I/O pipeline filters");
        image += hdr->filter_len;
    } /* end if */

    /* Compute metadata checksum */
    metadata_chksum = H5_checksum_metadata(_image, (size_t)(image - (uint8_t *)_image), 0);

    /* Metadata checksum */
    UINT32ENCODE(image, metadata_chksum);

    /* sanity check */
    assert((size_t)(image - (uint8_t *)_image) == len);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__cache_hdr_serialize() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__cache_hdr_free_icr
 *
 * Purpose:	Free the in core representation of the fractal heap header.
 *
 *		This routine frees just the header itself, not the
 *		associated version 2 B-Tree, the associated Free Space Manager,
 *		nor the indirect/direct block tree that is rooted in the header.
 *
 *		This routine also does not free the file space that may
 *		be allocated to the header.
 *
 * Return:	Success:	SUCCEED
 *		Failure:	FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__cache_hdr_free_icr(void *_thing)
{
    H5HF_hdr_t *hdr       = (H5HF_hdr_t *)_thing; /* Fractal heap info */
    herr_t      ret_value = SUCCEED;              /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(hdr);
    assert(hdr->cache_info.type == H5AC_FHEAP_HDR);
    assert(hdr->rc == 0);

    if (H5HF__hdr_free(hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "unable to release fractal heap header");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__cache_hdr_free_icr() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__cache_iblock_get_initial_load_size()
 *
 * Purpose:	Compute the size of the on disk image of the indirect
 *		block, and place this value in *image_len.
 *
 * Return:	Success:	SUCCEED
 *		Failure:	FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__cache_iblock_get_initial_load_size(void *_udata, size_t *image_len)
{
    H5HF_iblock_cache_ud_t *udata = (H5HF_iblock_cache_ud_t *)_udata; /* User data for callback */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(udata);
    assert(udata->par_info);
    assert(udata->par_info->hdr);
    assert(image_len);

    /* Set the image length size */
    *image_len = (size_t)H5HF_MAN_INDIRECT_SIZE(udata->par_info->hdr, *udata->nrows);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HF__cache_iblock_get_initial_load_size() */

/*-------------------------------------------------------------------------
 * Function:    H5HF__cache_iblock_verify_chksum
 *
 * Purpose:     Verify the computed checksum of the data structure is the
 *              same as the stored checksum.
 *
 * Return:      Success:        true/false
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5HF__cache_iblock_verify_chksum(const void *_image, size_t len, void H5_ATTR_UNUSED *_udata)
{
    const uint8_t *image = (const uint8_t *)_image; /* Pointer into raw data buffer */
    uint32_t       stored_chksum;                   /* Stored metadata checksum value */
    uint32_t       computed_chksum;                 /* Computed metadata checksum value */
    htri_t         ret_value = true;                /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(image);

    /* Get stored and computed checksums */
    H5F_get_checksums(image, len, &stored_chksum, &computed_chksum);

    if (stored_chksum != computed_chksum)
        ret_value = false;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__cache_iblock_verify_chksum() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__cache_iblock_deserialize
 *
 * Purpose:	Given a buffer containing the on disk image of the indirect
 *		block, allocate an instance of H5HF_indirect_t, load the data
 *		in the buffer into this new instance, and return a pointer to
 *		it.
 *
 *		As best I can tell, the size of the indirect block image is fully
 *		know before the image is loaded, so this function should succeed
 *		unless the image is corrupt or memory allocation fails.
 *
 * Return:	Success:	Pointer to in core representation
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5HF__cache_iblock_deserialize(const void *_image, size_t H5_ATTR_NDEBUG_UNUSED len, void *_udata,
                               bool H5_ATTR_UNUSED *dirty)
{
    H5HF_hdr_t             *hdr;                                       /* Shared fractal heap information */
    H5HF_iblock_cache_ud_t *udata  = (H5HF_iblock_cache_ud_t *)_udata; /* User data for callback */
    H5HF_indirect_t        *iblock = NULL;                             /* Indirect block info */
    const uint8_t          *image  = (const uint8_t *)_image;          /* Pointer into raw data buffer */
    haddr_t                 heap_addr;        /* Address of heap header in the file */
    uint32_t                stored_chksum;    /* Stored metadata checksum value */
    unsigned                u;                /* Local index variable */
    void                   *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(image);
    assert(udata);
    assert(dirty);
    hdr = udata->par_info->hdr;
    assert(hdr->f);

    /* Set the shared heap header's file context for this operation */
    hdr->f = udata->f;

    /* Allocate space for the fractal heap indirect block */
    if (NULL == (iblock = H5FL_CALLOC(H5HF_indirect_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Share common heap information */
    iblock->hdr = hdr;
    if (H5HF__hdr_incr(hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINC, NULL, "can't increment reference count on shared heap header");

    /* Set block's internal information */
    iblock->rc        = 0;
    iblock->nrows     = *udata->nrows;
    iblock->nchildren = 0;

    /* Compute size of indirect block */
    iblock->size = H5HF_MAN_INDIRECT_SIZE(hdr, iblock->nrows);

    /* sanity check */
    assert(iblock->size == len);

    /* Magic number */
    if (memcmp(image, H5HF_IBLOCK_MAGIC, (size_t)H5_SIZEOF_MAGIC) != 0)
        HGOTO_ERROR(H5E_HEAP, H5E_BADVALUE, NULL, "wrong fractal heap indirect block signature");
    image += H5_SIZEOF_MAGIC;

    /* Version */
    if (*image++ != H5HF_IBLOCK_VERSION)
        HGOTO_ERROR(H5E_HEAP, H5E_VERSION, NULL, "wrong fractal heap direct block version");

    /* Address of heap that owns this block */
    H5F_addr_decode(udata->f, &image, &heap_addr);
    if (H5_addr_ne(heap_addr, hdr->heap_addr))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTLOAD, NULL, "incorrect heap header address for direct block");

    /* Address of parent block */
    iblock->parent = udata->par_info->iblock;
    /* this copy of the parent pointer is needed by the notify callback so */
    /* that it can take down flush dependencies on eviction even if        */
    /* the parent pointer has been nulled out.             JRM -- 5/18/14  */
    if (udata->par_info->iblock)
        iblock->fd_parent = udata->par_info->iblock;
    else
        iblock->fd_parent = udata->par_info->hdr;
    iblock->par_entry = udata->par_info->entry;
    if (iblock->parent) {
        /* Share parent block */
        if (H5HF__iblock_incr(iblock->parent) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTINC, NULL,
                        "can't increment reference count on shared indirect block");

        /* Set max. # of rows in this block */
        iblock->max_rows = iblock->nrows;
    } /* end if */
    else {
        /* Set max. # of rows in this block */
        iblock->max_rows = hdr->man_dtable.max_root_rows;
    } /* end else */

    /* Offset of heap within the heap's address space */
    UINT64DECODE_VAR(image, iblock->block_off, hdr->heap_off_size);

    /* Allocate & decode child block entry tables */
    assert(iblock->nrows > 0);
    if (NULL == (iblock->ents = H5FL_SEQ_MALLOC(H5HF_indirect_ent_t,
                                                (size_t)(iblock->nrows * hdr->man_dtable.cparam.width))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed for direct entries");

    if (hdr->filter_len > 0) {
        unsigned dir_rows; /* Number of direct rows in this indirect block */

        /* Compute the number of direct rows for this indirect block */
        dir_rows = MIN(iblock->nrows, hdr->man_dtable.max_direct_rows);

        /* Allocate indirect block filtered entry array */
        if (NULL == (iblock->filt_ents = H5FL_SEQ_MALLOC(H5HF_indirect_filt_ent_t,
                                                         (size_t)(dir_rows * hdr->man_dtable.cparam.width))))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed for block entries");
    } /* end if */
    else
        iblock->filt_ents = NULL;

    for (u = 0; u < (iblock->nrows * hdr->man_dtable.cparam.width); u++) {
        /* Decode child block address */
        H5F_addr_decode(udata->f, &image, &(iblock->ents[u].addr));

        /* Check for heap with I/O filters */
        if (hdr->filter_len > 0) {
            /* Sanity check */
            assert(iblock->filt_ents);

            /* Decode extra information for direct blocks */
            if (u < (hdr->man_dtable.max_direct_rows * hdr->man_dtable.cparam.width)) {
                /* Size of filtered direct block */
                H5F_DECODE_LENGTH(udata->f, image, iblock->filt_ents[u].size);

                /* Sanity check */
                /* (either both the address & size are defined or both are
                 *  not defined)
                 */
                assert((H5_addr_defined(iblock->ents[u].addr) && iblock->filt_ents[u].size) ||
                       (!H5_addr_defined(iblock->ents[u].addr) && iblock->filt_ents[u].size == 0));

                /* I/O filter mask for filtered direct block */
                UINT32DECODE(image, iblock->filt_ents[u].filter_mask);
            } /* end if */
        }     /* end if */

        /* Count child blocks */
        if (H5_addr_defined(iblock->ents[u].addr)) {
            iblock->nchildren++;
            iblock->max_child = u;
        } /* end if */
    }     /* end for */

    /* Sanity check */
    assert(iblock->nchildren); /* indirect blocks w/no children should have been deleted */

    /* checksum verification already done by verify_chksum cb */

    /* Metadata checksum */
    UINT32DECODE(image, stored_chksum);

    /* Sanity check */
    assert((size_t)(image - (const uint8_t *)_image) == iblock->size);

    /* Check if we have any indirect block children */
    if (iblock->nrows > hdr->man_dtable.max_direct_rows) {
        unsigned indir_rows; /* Number of indirect rows in this indirect block */

        /* Compute the number of indirect rows for this indirect block */
        indir_rows = iblock->nrows - hdr->man_dtable.max_direct_rows;

        /* Allocate & initialize child indirect block pointer array */
        if (NULL == (iblock->child_iblocks = H5FL_SEQ_CALLOC(
                         H5HF_indirect_ptr_t, (size_t)(indir_rows * hdr->man_dtable.cparam.width))))
            HGOTO_ERROR(H5E_HEAP, H5E_NOSPACE, NULL, "memory allocation failed for block entries");
    } /* end if */
    else
        iblock->child_iblocks = NULL;

    /* Set return value */
    ret_value = (void *)iblock;

done:
    if (!ret_value && iblock)
        if (H5HF__man_iblock_dest(iblock) < 0)
            HDONE_ERROR(H5E_HEAP, H5E_CANTFREE, NULL, "unable to destroy fractal heap indirect block");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__cache_iblock_deserialize() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__cache_iblock_image_len
 *
 * Purpose:	Return the size of the on disk image of the iblock.
 *
 * Return:	Success:	SUCCEED
 *		Failure:	FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__cache_iblock_image_len(const void *_thing, size_t *image_len)
{
    const H5HF_indirect_t *iblock = (const H5HF_indirect_t *)_thing; /* Indirect block info */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(iblock);
    assert(iblock->cache_info.type == H5AC_FHEAP_IBLOCK);
    assert(image_len);

    *image_len = iblock->size;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HF__cache_iblock_image_len() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__cache_iblock_pre_serialize
 *
 * Purpose:	The primary objective of this function is to determine if the
 *		indirect block is currently allocated in temporary file space,
 *		and if so, to move it to real file space before the entry is
 *		serialized.
 *
 *		In debug compiles, this function also verifies that all
 *		immediate flush dependency children of this indirect block
 *		are either clean or are not in cache.
 *
 * Return:	Success:	SUCCEED
 *		Failure:	FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__cache_iblock_pre_serialize(H5F_t *f, void *_thing, haddr_t addr, size_t H5_ATTR_UNUSED len,
                                 haddr_t *new_addr, size_t H5_ATTR_UNUSED *new_len, unsigned *flags)
{
    H5HF_hdr_t      *hdr;                                   /* Shared fractal heap information */
    H5HF_indirect_t *iblock    = (H5HF_indirect_t *)_thing; /* Indirect block info */
    herr_t           ret_value = SUCCEED;                   /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(iblock);
    assert(iblock->cache_info.type == H5AC_FHEAP_IBLOCK);
    assert(iblock->cache_info.size == iblock->size);
    assert(H5_addr_defined(addr));
    assert(H5_addr_eq(iblock->addr, addr));
    assert(new_addr);
    assert(new_len);
    assert(flags);
    hdr = iblock->hdr;
    assert(hdr);
    assert(hdr->cache_info.type == H5AC_FHEAP_HDR);

#ifndef NDEBUG
    {
        bool     descendants_clean = true;
        bool     fd_children_clean = true;
        unsigned iblock_status     = 0;

        /* verify that flush dependencies are working correctly.  Do this
         * by verifying that all immediate flush dependency children of this
         * iblock are clean.
         */
        if (H5AC_get_entry_status(f, iblock->addr, &iblock_status) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't get iblock status");

        /* since the current iblock is the guest of honor in a flush, we know
         * that it is locked into the cache for the duration of the call.  Hence
         * there is no need to check to see if it is pinned or protected, or to
         * protect it if it is not.
         */
        if (H5HF__cache_verify_iblock_descendants_clean((H5F_t *)f, iblock->addr, iblock, &iblock_status,
                                                        &fd_children_clean, &descendants_clean) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_SYSTEM, FAIL, "can't verify descendants clean.");
        assert(fd_children_clean);
    }
#endif /* NDEBUG */

    /* Check to see if we must re-allocate the iblock from temporary to
     * normal (AKA real) file space.
     */
    if (H5F_IS_TMP_ADDR(f, addr)) {
        haddr_t iblock_addr;

        /* Allocate 'normal' space for the new indirect block on disk */
        if (HADDR_UNDEF ==
            (iblock_addr = H5MF_alloc((H5F_t *)f, H5FD_MEM_FHEAP_IBLOCK, (hsize_t)iblock->size)))
            HGOTO_ERROR(H5E_HEAP, H5E_NOSPACE, FAIL,
                        "file allocation failed for fractal heap indirect block");

        /* Sanity check */
        assert(!H5_addr_eq(iblock->addr, iblock_addr));

        /* Let the metadata cache know the block moved */
        if (H5AC_move_entry((H5F_t *)f, H5AC_FHEAP_IBLOCK, iblock->addr, iblock_addr) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTMOVE, FAIL, "unable to move indirect block");

        /* Update the internal address for the block */
        iblock->addr = iblock_addr;

        /* Check for root indirect block */
        if (NULL == iblock->parent) {
            /* Update information about indirect block's location */
            hdr->man_dtable.table_addr = iblock_addr;

            /* Mark that heap header was modified */
            if (H5HF__hdr_dirty(hdr) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTDIRTY, FAIL, "can't mark heap header as dirty");
        } /* end if */
        else {
            H5HF_indirect_t *par_iblock; /* Parent indirect block */
            unsigned         par_entry;  /* Entry in parent indirect block */

            /* Get parent information */
            par_iblock = iblock->parent;
            par_entry  = iblock->par_entry;

            /* Update information about indirect block's location */
            par_iblock->ents[par_entry].addr = iblock_addr;

            /* Mark that parent was modified */
            if (H5HF__iblock_dirty(par_iblock) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTDIRTY, FAIL, "can't mark heap header as dirty");
        } /* end if */

        *new_addr = iblock_addr;
        *flags    = H5AC__SERIALIZE_MOVED_FLAG;
    } /* end if */
    else
        *flags = 0;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__cache_iblock_pre_serialize() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__cache_iblock_serialize
 *
 * Purpose:	Given a pointer to an iblock, and a pointer to a buffer of
 *		the appropriate size, write the contents of the iblock to the
 *		buffer in format appropriate for writing to disk.
 *
 * Return:	Success:	SUCCEED
 *		Failure:	FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__cache_iblock_serialize(const H5F_t *f, void *_image, size_t H5_ATTR_NDEBUG_UNUSED len, void *_thing)
{
    H5HF_hdr_t      *hdr;                                /* Shared fractal heap information */
    H5HF_indirect_t *iblock = (H5HF_indirect_t *)_thing; /* Indirect block info */
    uint8_t         *image  = (uint8_t *)_image;         /* Pointer into raw data buffer */
#ifndef NDEBUG
    unsigned nchildren = 0;       /* Track # of children */
    size_t   max_child = 0;       /* Track max. child entry used */
#endif                            /* NDEBUG */
    uint32_t metadata_chksum;     /* Computed metadata checksum value */
    size_t   u;                   /* Local index variable */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(f);
    assert(image);
    assert(iblock);
    assert(iblock->cache_info.type == H5AC_FHEAP_IBLOCK);
    assert(iblock->cache_info.size == iblock->size);
    assert(len == iblock->size);

    /* Indirect block must be in 'normal' file space */
    assert(!H5F_IS_TMP_ADDR(f, iblock->addr));
    assert(H5_addr_eq(iblock->addr, iblock->cache_info.addr));

    /* Get the pointer to the shared heap header */
    hdr = iblock->hdr;

    /* Set the shared heap header's file context for this operation */
    H5_GCC_DIAG_OFF("discarded-qualifiers")
    H5_CLANG_DIAG_OFF("incompatible-pointer-types-discards-qualifiers")
    hdr->f = f;
    H5_GCC_DIAG_ON("discarded-qualifiers")
    H5_CLANG_DIAG_ON("incompatible-pointer-types-discards-qualifiers")

    /* Magic number */
    H5MM_memcpy(image, H5HF_IBLOCK_MAGIC, (size_t)H5_SIZEOF_MAGIC);
    image += H5_SIZEOF_MAGIC;

    /* Version # */
    *image++ = H5HF_IBLOCK_VERSION;

    /* Address of heap header for heap which owns this block */
    H5F_addr_encode(f, &image, hdr->heap_addr);

    /* Offset of block in heap */
    UINT64ENCODE_VAR(image, iblock->block_off, hdr->heap_off_size);

    /* Encode indirect block-specific fields */
    for (u = 0; u < (iblock->nrows * hdr->man_dtable.cparam.width); u++) {
        /* Encode child block address */
        H5F_addr_encode(f, &image, iblock->ents[u].addr);

        /* Check for heap with I/O filters */
        if (hdr->filter_len > 0) {
            /* Sanity check */
            assert(iblock->filt_ents);

            /* Encode extra information for direct blocks */
            if (u < (hdr->man_dtable.max_direct_rows * hdr->man_dtable.cparam.width)) {
                /* Sanity check */
                /* (either both the address & size are defined or both are
                 *  not defined)
                 */
                assert((H5_addr_defined(iblock->ents[u].addr) && iblock->filt_ents[u].size) ||
                       (!H5_addr_defined(iblock->ents[u].addr) && iblock->filt_ents[u].size == 0));

                /* Size of filtered direct block */
                H5F_ENCODE_LENGTH(f, image, iblock->filt_ents[u].size);

                /* I/O filter mask for filtered direct block */
                UINT32ENCODE(image, iblock->filt_ents[u].filter_mask);
            } /* end if */
        }     /* end if */

#ifndef NDEBUG
        /* Count child blocks */
        if (H5_addr_defined(iblock->ents[u].addr)) {
            nchildren++;
            if (u > max_child)
                max_child = u;
        } /* end if */
#endif    /* NDEBUG */
    }     /* end for */

    /* Compute checksum */
    metadata_chksum = H5_checksum_metadata((uint8_t *)_image, (size_t)(image - (uint8_t *)_image), 0);

    /* Metadata checksum */
    UINT32ENCODE(image, metadata_chksum);

    /* Sanity checks */
    assert((size_t)(image - (uint8_t *)_image) == iblock->size);
#ifndef NDEBUG
    assert(nchildren == iblock->nchildren);
    assert(max_child == iblock->max_child);
#endif /* NDEBUG */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__cache_iblock_serialize() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__cache_iblock_notify
 *
 * Purpose:	This function is used to create and destroy flush dependency
 *		relationships between iblocks and their parents as indirect blocks
 *		are loaded / inserted and evicted from the metadata cache.
 *
 *		In general, the parent will be another iblock, but it may be the
 *		header if the iblock in question is the root iblock.
 *
 * Return:	Success:	SUCCEED
 *		Failure:	FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__cache_iblock_notify(H5AC_notify_action_t action, void *_thing)
{
    H5HF_indirect_t *iblock    = (H5HF_indirect_t *)_thing; /* Indirect block info */
    herr_t           ret_value = SUCCEED;                   /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(iblock);
    assert(iblock->cache_info.type == H5AC_FHEAP_IBLOCK);
    assert(iblock->hdr);

    /* further sanity checks */
    if (iblock->parent == NULL) {
        /* pointer from hdr to root iblock will not be set up unless */
        /* the fractal heap has already pinned the hdr.  Do what     */
        /* sanity checking we can.                                   */
        if ((iblock->block_off == 0) && (iblock->hdr->root_iblock_flags & H5HF_ROOT_IBLOCK_PINNED))
            assert(iblock->hdr->root_iblock == iblock);
    } /* end if */
    else {
        /* if this is a child iblock, verify that the pointers are */
        /* either uninitialized or set up correctly.               */
        H5HF_indirect_t H5_ATTR_NDEBUG_UNUSED *par_iblock = iblock->parent;
        unsigned H5_ATTR_NDEBUG_UNUSED         indir_idx; /* Index in parent's child iblock pointer array */

        /* Sanity check */
        assert(par_iblock->child_iblocks);
        assert(iblock->par_entry >=
               (iblock->hdr->man_dtable.max_direct_rows * iblock->hdr->man_dtable.cparam.width));

        /* Compute index in parent's child iblock pointer array */
        indir_idx = iblock->par_entry -
                    (iblock->hdr->man_dtable.max_direct_rows * iblock->hdr->man_dtable.cparam.width);

        /* The pointer to iblock in the parent may not be set yet -- */
        /* verify that it is either NULL, or that it has been set to */
        /* iblock.                                                   */
        assert((NULL == par_iblock->child_iblocks[indir_idx]) ||
               (par_iblock->child_iblocks[indir_idx] == iblock));
    } /* end else */

    switch (action) {
        case H5AC_NOTIFY_ACTION_AFTER_INSERT:
        case H5AC_NOTIFY_ACTION_AFTER_LOAD:
            /* Create flush dependency with parent, if there is one */
            if (iblock->fd_parent)
                if (H5AC_create_flush_dependency(iblock->fd_parent, iblock) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTDEPEND, FAIL, "unable to create flush dependency");
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
            if (iblock->fd_parent) {
                /* Destroy flush dependency with parent */
                if (H5AC_destroy_flush_dependency(iblock->fd_parent, iblock) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTUNDEPEND, FAIL, "unable to destroy flush dependency");
                iblock->fd_parent = NULL;
            } /* end if */
            break;

        default:
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unknown action from metadata cache");
            break;
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__cache_iblock_notify() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__cache_iblock_free_icr
 *
 * Purpose:	Unlink the supplied instance of H5HF_indirect_t from the
 *		fractal heap and free its memory.
 *
 * Return:	Success:	SUCCEED
 *		Failure:	FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__cache_iblock_free_icr(void *thing)
{
    H5HF_indirect_t *iblock    = (H5HF_indirect_t *)thing; /* Fractal heap indirect block to free */
    herr_t           ret_value = SUCCEED;                  /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(iblock);
    assert(iblock->cache_info.type == H5AC_FHEAP_IBLOCK);
    assert(iblock->rc == 0);
    assert(iblock->hdr);

    /* Destroy fractal heap indirect block */
    if (H5HF__man_iblock_dest(iblock) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "unable to destroy fractal heap indirect block");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__cache_iblock_free_icr() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__cache_dblock_get_initial_load_size()
 *
 * Purpose:	Determine the size of the direct block on disk image, and
 *		return it in *image_len.
 *
 * Return:	Success:	SUCCEED
 *		Failure:	FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__cache_dblock_get_initial_load_size(void *_udata, size_t *image_len)
{
    const H5HF_dblock_cache_ud_t *udata = (const H5HF_dblock_cache_ud_t *)_udata; /* User data for callback */
    const H5HF_parent_t          *par_info; /* Pointer to parent information */
    const H5HF_hdr_t             *hdr;      /* Shared fractal heap information */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(udata);
    assert(image_len);

    /* Convenience variables */
    par_info = (const H5HF_parent_t *)(&(udata->par_info));
    assert(par_info);
    hdr = par_info->hdr;
    assert(hdr);

    /* Check for I/O filters on this heap */
    if (hdr->filter_len > 0) {
        /* Check for root direct block */
        if (par_info->iblock == NULL)
            /* filtered root direct block */
            *image_len = hdr->pline_root_direct_size;
        else
            /* filtered direct block */
            *image_len = par_info->iblock->filt_ents[par_info->entry].size;
    } /* end if */
    else
        *image_len = udata->dblock_size;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HF__cache_dblock_get_initial_load_size() */

/*-------------------------------------------------------------------------
 * Function:    H5HF__cache_dblock_verify_chksum
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
H5HF__cache_dblock_verify_chksum(const void *_image, size_t len, void *_udata)
{
    const uint8_t          *image    = (const uint8_t *)_image;          /* Pointer into raw data buffer */
    H5HF_dblock_cache_ud_t *udata    = (H5HF_dblock_cache_ud_t *)_udata; /* User data for callback */
    void                   *read_buf = NULL;                             /* Pointer to buffer to read in */
    H5HF_hdr_t             *hdr;                                         /* Shared fractal heap information */
    H5HF_parent_t          *par_info;                                    /* Pointer to parent information */
    uint32_t                stored_chksum;                               /* Stored metadata checksum value */
    uint32_t                computed_chksum;  /* Computed metadata checksum value */
    size_t                  chk_size;         /* The size for validating checksum */
    uint8_t                *chk_p;            /* Pointer to the area for validating checksum */
    htri_t                  ret_value = true; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(image);
    assert(udata);
    par_info = (H5HF_parent_t *)(&(udata->par_info));
    assert(par_info);
    hdr = par_info->hdr;
    assert(hdr);

    /* Get out if data block is not checksummed */
    if (!(hdr->checksum_dblocks))
        HGOTO_DONE(true);

    if (hdr->filter_len > 0) {
        size_t   nbytes;      /* Number of bytes used in buffer, after applying reverse filters */
        unsigned filter_mask; /* Excluded filters for direct block */
        H5Z_cb_t filter_cb;   /* Filter callback structure */

        /* Initialize the filter callback struct */
        filter_cb.op_data = NULL;
        filter_cb.func    = NULL; /* no callback function when failed */

        /* Allocate buffer to perform I/O filtering on and copy image into
         * it.  Must do this as H5Z_pipeline() may re-size the buffer
         * provided to it.
         */
        if (NULL == (read_buf = H5MM_malloc(len)))
            HGOTO_ERROR(H5E_HEAP, H5E_NOSPACE, FAIL, "memory allocation failed for pipeline buffer");

        /* Set up parameters for filter pipeline */
        nbytes      = len;
        filter_mask = udata->filter_mask;
        H5MM_memcpy(read_buf, image, len);

        /* Push direct block data through I/O filter pipeline */
        if (H5Z_pipeline(&(hdr->pline), H5Z_FLAG_REVERSE, &filter_mask, H5Z_ENABLE_EDC, filter_cb, &nbytes,
                         &len, &read_buf) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTFILTER, FAIL, "output pipeline failed");

        /* Update info about direct block */
        udata->decompressed = true;
        len                 = nbytes;
    }
    else {
        /* If the data are unfiltered, we just point to the image, which we
         * never modify. Casting away const is okay here.
         */
        H5_GCC_CLANG_DIAG_OFF("cast-qual")
        read_buf = (void *)image;
        H5_GCC_CLANG_DIAG_OFF("cast-qual")
    }

    /* Decode checksum */
    chk_size = (size_t)(H5HF_MAN_ABS_DIRECT_OVERHEAD(hdr) - H5HF_SIZEOF_CHKSUM);
    chk_p    = (uint8_t *)read_buf + chk_size;

    /* Metadata checksum */
    UINT32DECODE(chk_p, stored_chksum);

    chk_p -= H5HF_SIZEOF_CHKSUM;

    /* Reset checksum field, for computing the checksum */
    memset(chk_p, 0, (size_t)H5HF_SIZEOF_CHKSUM);

    /* Compute checksum on entire direct block */
    computed_chksum = H5_checksum_metadata(read_buf, len, 0);

    /* Restore the checksum */
    UINT32ENCODE(chk_p, stored_chksum);

    /* Verify checksum */
    if (stored_chksum != computed_chksum)
        HGOTO_DONE(false);

    /* Save the decompressed data to be used later in deserialize callback */
    if (hdr->filter_len > 0) {
        /* Sanity check */
        assert(udata->decompressed);
        assert(len == udata->dblock_size);

        /* Allocate block buffer */
        if (NULL == (udata->dblk = H5FL_BLK_MALLOC(direct_block, (size_t)len)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");

        /* Copy un-filtered data into block's buffer */
        H5MM_memcpy(udata->dblk, read_buf, len);
    } /* end if */

done:
    /* Release the read buffer */
    if (read_buf && read_buf != image)
        H5MM_xfree(read_buf);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__cache_dblock_verify_chksum() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__cache_dblock_deserialize
 *
 * Purpose:	Given a buffer containing the on disk image of a direct
 *		block, allocate an instance of H5HF_direct_t, load the data
 *		in the buffer into this new instance, and return a pointer to
 *		it.
 *
 *		As best I can tell, the size of the direct block image is fully
 *		know before the image is loaded, so this function should succeed
 *		unless the image is corrupt or memory allocation fails.
 *
 * Return:	Success:	Pointer to in core representation
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5HF__cache_dblock_deserialize(const void *_image, size_t len, void *_udata, bool H5_ATTR_UNUSED *dirty)
{
    H5HF_hdr_t             *hdr;                                      /* Shared fractal heap information */
    H5HF_dblock_cache_ud_t *udata = (H5HF_dblock_cache_ud_t *)_udata; /* User data for callback */
    H5HF_parent_t          *par_info;                                 /* Pointer to parent information */
    H5HF_direct_t          *dblock   = NULL;                          /* Direct block info */
    const uint8_t          *image    = (const uint8_t *)_image;       /* Pointer into raw data buffer */
    void                   *read_buf = NULL;                          /* Pointer to buffer to decompress */
    haddr_t                 heap_addr;                                /* Address of heap header in the file */
    void                   *ret_value = NULL;                         /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(image);
    assert(udata);
    par_info = (H5HF_parent_t *)(&(udata->par_info));
    assert(par_info);
    hdr = par_info->hdr;
    assert(hdr);
    assert(hdr->cache_info.type == H5AC_FHEAP_HDR);
    assert(dirty);

    /* Allocate space for the fractal heap direct block */
    if (NULL == (dblock = H5FL_CALLOC(H5HF_direct_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");
    memset(&dblock->cache_info, 0, sizeof(H5AC_info_t));

    /* Set the shared heap header's file context for this operation */
    hdr->f = udata->f;

    /* Share common heap information */
    dblock->hdr = hdr;
    if (H5HF__hdr_incr(hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINC, NULL, "can't increment reference count on shared heap header");

    /* Set block's internal information */
    dblock->size = udata->dblock_size;

    /* Check for I/O filters on this heap */
    if (hdr->filter_len > 0) {
        /* Direct block is already decompressed in verify_chksum callback */
        if (udata->decompressed) {
            /* Sanity check */
            assert(udata->dblk);

            /* Take ownership of the decompressed direct block */
            dblock->blk = udata->dblk;
            udata->dblk = NULL;
        } /* end if */
        else {
            H5Z_cb_t filter_cb;   /* Filter callback structure */
            size_t   nbytes;      /* Number of bytes used in buffer, after applying reverse filters */
            unsigned filter_mask; /* Excluded filters for direct block */

            /* Sanity check */
            assert(udata->dblk == NULL);

            /* Initialize the filter callback struct */
            filter_cb.op_data = NULL;
            filter_cb.func    = NULL; /* no callback function when failed */

            /* Allocate buffer to perform I/O filtering on and copy image into
             * it.  Must do this as H5Z_pipeline() may resize the buffer
             * provided to it.
             */
            if (NULL == (read_buf = H5MM_malloc(len)))
                HGOTO_ERROR(H5E_HEAP, H5E_NOSPACE, NULL, "memory allocation failed for pipeline buffer");

            /* Copy compressed image into buffer */
            H5MM_memcpy(read_buf, image, len);

            /* Push direct block data through I/O filter pipeline */
            nbytes      = len;
            filter_mask = udata->filter_mask;
            if (H5Z_pipeline(&(hdr->pline), H5Z_FLAG_REVERSE, &filter_mask, H5Z_ENABLE_EDC, filter_cb,
                             &nbytes, &len, &read_buf) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTFILTER, NULL, "output pipeline failed");

            /* Sanity check */
            assert(nbytes == dblock->size);

            /* Copy un-filtered data into block's buffer */
            H5MM_memcpy(dblock->blk, read_buf, dblock->size);
        } /* end if */
    }     /* end if */
    else {
        /* Sanity checks */
        assert(udata->dblk == NULL);
        assert(!udata->decompressed);

        /* Allocate block buffer */
        /* XXX: Change to using free-list factories */
        if (NULL == (dblock->blk = H5FL_BLK_MALLOC(direct_block, (size_t)dblock->size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

        /* Copy image to dblock->blk */
        assert(dblock->size == len);
        H5MM_memcpy(dblock->blk, image, dblock->size);
    } /* end else */

    /* Start decoding direct block */
    image = dblock->blk;

    /* Magic number */
    if (memcmp(image, H5HF_DBLOCK_MAGIC, (size_t)H5_SIZEOF_MAGIC) != 0)
        HGOTO_ERROR(H5E_HEAP, H5E_BADVALUE, NULL, "wrong fractal heap direct block signature");
    image += H5_SIZEOF_MAGIC;

    /* Version */
    if (*image++ != H5HF_DBLOCK_VERSION)
        HGOTO_ERROR(H5E_HEAP, H5E_VERSION, NULL, "wrong fractal heap direct block version");

    /* Address of heap that owns this block (just for file integrity checks) */
    H5F_addr_decode(udata->f, &image, &heap_addr);
    if (H5_addr_ne(heap_addr, hdr->heap_addr))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTLOAD, NULL, "incorrect heap header address for direct block");

    /* Address of parent block */
    dblock->parent = par_info->iblock;
    if (par_info->iblock)
        dblock->fd_parent = par_info->iblock;
    else
        dblock->fd_parent = par_info->hdr;
    dblock->par_entry = par_info->entry;
    if (dblock->parent) {
        /* Share parent block */
        if (H5HF__iblock_incr(dblock->parent) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTINC, NULL,
                        "can't increment reference count on shared indirect block");
    } /* end if */

    /* Offset of heap within the heap's address space */
    UINT64DECODE_VAR(image, dblock->block_off, hdr->heap_off_size);

    /* Decode checksum on direct block, if requested */
    if (hdr->checksum_dblocks) {
        uint32_t stored_chksum; /* Metadata checksum value */

        /* checksum verification already done in verify_chksum cb */

        /* Metadata checksum */
        UINT32DECODE(image, stored_chksum);
    } /* end if */

    /* Sanity check */
    assert((size_t)(image - dblock->blk) == (size_t)H5HF_MAN_ABS_DIRECT_OVERHEAD(hdr));

    /* Set return value */
    ret_value = (void *)dblock;

done:
    /* Release the read buffer */
    if (read_buf)
        H5MM_xfree(read_buf);

    /* Cleanup on error */
    if (!ret_value && dblock)
        if (H5HF__man_dblock_dest(dblock) < 0)
            HDONE_ERROR(H5E_HEAP, H5E_CANTFREE, NULL, "unable to destroy fractal heap direct block");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__cache_dblock_deserialize() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__cache_dblock_image_len
 *
 * Purpose:	Report the actual size of the direct block image on disk.
 *		Note that this value will probably be incorrect if compression
 *		is enabled and the entry is dirty.
 *
 * Return:	Success:	SUCCEED
 *		Failure:	FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__cache_dblock_image_len(const void *_thing, size_t *image_len)
{
    const H5HF_direct_t *dblock = (const H5HF_direct_t *)_thing; /* Direct block info */
    const H5HF_hdr_t    *hdr;                                    /* Shared fractal heap information */
    size_t               size;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(dblock);
    assert(dblock->cache_info.type == H5AC_FHEAP_DBLOCK);
    assert(image_len);

    /* Set up convenience variables */
    hdr = dblock->hdr;
    assert(hdr);

    /* Check for I/O filters on this heap */
    if (hdr->filter_len > 0) {
        /*
         * If the data is available, set to the compressed
         * size of the direct block -- otherwise set it equal to the
         * uncompressed size.
         *
         * We have three possible scenarios here.
         *
         * First, the block may never have been flushed.  In this
         * case, both dblock->file_size and the size stored in the
         * parent (either the header or the parent iblock) will all
         * be zero.  In this case, return the uncompressed size
         * stored in dblock->size as the size.
         *
         * Second, the block may have just been serialized, in which
         * case, dblock->file_size should be zero, and the correct
         * on disk size should be stored in the parent (again, either
         * the header or the parent iblock as case may be).
         *
         * Third, we may be in the process of discarding this
         * dblock without writing it.  In this case, dblock->file_size
         * should be non-zero and have the correct size.  Note that
         * in this case, the direct block will have been detached,
         * and thus looking up the parent will likely return incorrect
         * data.
         */
        if (dblock->file_size != 0)
            size = dblock->file_size;
        else {
            const H5HF_indirect_t *par_iblock = dblock->parent; /* Parent iblock */

            if (par_iblock)
                size = par_iblock->filt_ents[dblock->par_entry].size;
            else
                size = hdr->pline_root_direct_size;

            if (size == 0)
                size = dblock->size;
        } /* end else */
    }     /* end if */
    else
        size = dblock->size;

    /* Set the image size */
    assert(size > 0);
    *image_len = size;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HF__cache_dblock_image_len() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__cache_dblock_pre_serialize
 *
 * Purpose:	In principle, the purpose of this function is to determine
 *		the size and location of the disk image of the target direct
 *		block.  In this case, the uncompressed size of the block is
 *		fixed, but since the direct block could be compressed,
 *		we may need to compute and report the compressed size.
 *
 *		This is a bit sticky in the case of a direct block when I/O
 *		filters are enabled, as the size of the compressed version
 *		of the on disk image is not known until the direct block has
 *		been run through the filters.  Further, the location of the
 *		on disk image may change if the compressed size of the image
 *		changes as well.
 *
 *		To complicate matters further, the direct block may have been
 *		initially allocated in temporary (AKA imaginary) file space.
 *		In this case, we must relocate the direct block's on-disk
 *		image to "real" file space regardless of whether it has changed
 *		size.
 *
 *		One simplifying factor is the direct block's "blk" field,
 *		which contains a pointer to a buffer which (with the exception
 *		of a small header) contains the on disk image in uncompressed
 *		form.
 *
 *		To square this particular circle, this function does
 *		everything the serialize function usually does, with the
 *		exception of copying the image into the image buffer provided
 *		to the serialize function by the metadata cache.  The data to
 *		copy is provided to the serialize function in a buffer pointed
 *		to by the write_buf field.
 *
 *		If I/O filters are enabled, on exit,
 *		H5HF__cache_dblock_pre_serialize() sets the write_buf field to
 *		point to a buffer containing the filtered image of the direct
 *		block.  The serialize function should free this block, and set
 *		the write_buf field to NULL after copying it into the image
 *		buffer provided by the metadata cache.
 *
 *		If I/O filters are not enabled, this function prepares
 *		the buffer pointed to by the blk field for copying to the
 *		image buffer provided by the metadata cache, and sets the
 *		write_buf field equal to the blk field.  In this case, the
 *		serialize function should simply set the write_buf field to
 *		NULL after copying the direct block image into the image
 *		buffer.
 *
 *		In both of the above cases, the length of the buffer pointed
 *		to by write_buf is provided in the write_len field.  This
 *		field must contain 0 on entry to this function, and should
 *		be set back to 0 at the end of the serialize function.
 *
 * Return:	Success:	SUCCEED
 *		Failure:	FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__cache_dblock_pre_serialize(H5F_t *f, void *_thing, haddr_t addr, size_t len, haddr_t *new_addr,
                                 size_t *new_len, unsigned *flags)
{
    bool at_tmp_addr; /* Flag to indicate direct block is */
                      /* at temporary address */
    haddr_t          dblock_addr;
    H5HF_hdr_t      *hdr;                              /* Shared fractal heap information */
    H5HF_direct_t   *dblock = (H5HF_direct_t *)_thing; /* Direct block info */
    H5HF_indirect_t *par_iblock;                       /* Parent indirect block */
    unsigned         par_entry = 0;                    /* Entry in parent indirect block */
    void            *write_buf;                        /* Pointer to buffer to write out */
    size_t           write_size;                       /* Size of buffer to write out */
    uint8_t         *image;                            /* Pointer into raw data buffer */
    unsigned         dblock_flags = 0;
    herr_t           ret_value    = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(dblock);
    assert(dblock->cache_info.type == H5AC_FHEAP_DBLOCK);
    assert(dblock->write_buf == NULL);
    assert(dblock->write_size == 0);
    assert(dblock->cache_info.size == len);
    assert(H5_addr_defined(addr));
    assert(new_addr);
    assert(new_len);
    assert(flags);

    /* Set up local variables */
    hdr         = dblock->hdr;
    dblock_addr = addr; /* will update dblock_addr if we move the block */

    /* Set the shared heap header's file context for this operation */
    hdr->f = (H5F_t *)f;

    assert(hdr);
    assert(hdr->cache_info.type == H5AC_FHEAP_HDR);

    if (dblock->parent) {
        /* this is the common case, in which the direct block is the child
         * of an indirect block.  Set up the convenience variables we will
         * need if the address and/or compressed size of the on disk image
         * of the direct block changes, and do some sanity checking in
         * passing.
         */
        par_iblock = dblock->parent;
        par_entry  = dblock->par_entry;

        assert(par_iblock->cache_info.type == H5AC_FHEAP_IBLOCK);
        assert(H5_addr_eq(par_iblock->ents[par_entry].addr, addr));
    } /* end if */
    else {
        /* the direct block is a root direct block -- just set par_iblock
         * to NULL, as the field will not be used.
         */
        par_iblock = NULL;
    } /* end else */

    at_tmp_addr = H5F_IS_TMP_ADDR(f, addr);

    /* Begin by preping the direct block to be written to disk.  Do
     * this by writing the correct magic number, the dblock version,
     * the address of the header, the offset of the block in the heap,
     * and the checksum at the beginning of the block.
     */

    assert(dblock->blk);
    image = dblock->blk;

    /* Magic number */
    H5MM_memcpy(image, H5HF_DBLOCK_MAGIC, (size_t)H5_SIZEOF_MAGIC);
    image += H5_SIZEOF_MAGIC;

    /* Version # */
    *image++ = H5HF_DBLOCK_VERSION;

    /* Address of heap header for heap which owns this block */
    H5F_addr_encode(f, &image, hdr->heap_addr);

    /* Offset of block in heap */
    UINT64ENCODE_VAR(image, dblock->block_off, hdr->heap_off_size);

    /* Metadata checksum */
    if (hdr->checksum_dblocks) {
        uint32_t metadata_chksum; /* Computed metadata checksum value */

        /* Clear the checksum field, to compute the checksum */
        memset(image, 0, (size_t)H5HF_SIZEOF_CHKSUM);

        /* Compute checksum on entire direct block */
        metadata_chksum = H5_checksum_metadata(dblock->blk, dblock->size, 0);

        /* Metadata checksum */
        UINT32ENCODE(image, metadata_chksum);
    } /* end if */

    /* at this point, dblock->blk should point to an uncompressed image of
     * the direct block.  If I/O filters are not enabled, this image should
     * be ready to hand off to the metadata cache.
     */

    /* Sanity check */
    assert((size_t)(image - dblock->blk) == (size_t)H5HF_MAN_ABS_DIRECT_OVERHEAD(hdr));

    /* If I/O filters are enabled on this heap, we must run the direct block
     * image through the filters to obtain the image that we will hand off
     * to the metadata cache.
     */

    /* Check for I/O filters on this heap */
    if (hdr->filter_len > 0) {
        H5Z_cb_t filter_cb;       /* Filter callback structure */
        size_t   nbytes;          /* Number of bytes used */
        unsigned filter_mask = 0; /* Filter mask for block */

        /* Initialize the filter callback struct */
        filter_cb.op_data = NULL;
        filter_cb.func    = NULL; /* no callback function when failed */

        /* Allocate buffer to perform I/O filtering on */
        write_size = dblock->size;
        if (NULL == (write_buf = H5MM_malloc(write_size)))
            HGOTO_ERROR(H5E_HEAP, H5E_NOSPACE, FAIL, "memory allocation failed for pipeline buffer");

        /* Copy the direct block's image into the buffer to compress */
        H5MM_memcpy(write_buf, dblock->blk, write_size);

        /* Push direct block data through I/O filter pipeline */
        nbytes = write_size;
        if (H5Z_pipeline(&(hdr->pline), 0, &filter_mask, H5Z_ENABLE_EDC, filter_cb, &nbytes, &write_size,
                         &write_buf) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_WRITEERROR, FAIL, "output pipeline failed");

        /* Use the compressed number of bytes as the size to write */
        write_size = nbytes;

        /* If the size and/or location of the on disk image of the
         * direct block changes, we must touch up its parent to reflect
         * these changes.  Do this differently depending on whether the
         * direct block's parent is an indirect block or (rarely) the
         * fractal heap header.  In this case, the direct block is known
         * as a root direct block.
         */

        /* Check for root direct block */
        if (dblock->parent == NULL) {
            bool hdr_changed = false; /* Whether the header info changed */

            /* Sanity check */
            assert(H5_addr_eq(hdr->man_dtable.table_addr, addr));
            assert(hdr->pline_root_direct_size > 0);

            /* Check if the filter mask changed */
            if (hdr->pline_root_direct_filter_mask != filter_mask) {
                hdr->pline_root_direct_filter_mask = filter_mask;
                hdr_changed                        = true;
            } /* end if */

            /* verify that the cache's last record of the compressed
             * size matches the heap's last record.  This value will
             * likely change shortly.
             */
            assert(len == hdr->pline_root_direct_size);

            /* Check if we need to re-size the block on disk */
            if (hdr->pline_root_direct_size != write_size || at_tmp_addr) {
                /* Check if the direct block is NOT currently allocated
                 * in temp. file space
                 *
                 * (temp. file space does not need to be freed)
                 */
                if (!at_tmp_addr)
                    /* Release direct block's current disk space */
                    if (H5MF_xfree(f, H5FD_MEM_FHEAP_DBLOCK, addr, (hsize_t)hdr->pline_root_direct_size) < 0)
                        HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "unable to free fractal heap direct block");

                /* Allocate space for the compressed direct block */
                if (HADDR_UNDEF ==
                    (dblock_addr = H5MF_alloc((H5F_t *)f, H5FD_MEM_FHEAP_DBLOCK, (hsize_t)write_size)))
                    HGOTO_ERROR(H5E_HEAP, H5E_NOSPACE, FAIL,
                                "file allocation failed for fractal heap direct block");

                /* Update information about compressed direct block's
                 * location & size
                 */
                assert(hdr->man_dtable.table_addr == addr);
                assert(hdr->pline_root_direct_size == len);
                hdr->man_dtable.table_addr  = dblock_addr;
                hdr->pline_root_direct_size = write_size;

                /* Note that heap header was modified */
                hdr_changed = true;
            } /* end if */

            /* Check if heap header was modified */
            if (hdr_changed)
                if (H5HF__hdr_dirty(hdr) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTDIRTY, FAIL, "can't mark heap header as dirty");
        }                             /* end if */
        else {                        /* the direct block's parent is an indirect block */
            bool par_changed = false; /* Whether the parent's infochanged */

            /* Sanity check */
            assert(par_iblock);
            assert(par_iblock->filt_ents[par_entry].size > 0);

            /* Check if the filter mask changed */
            if (par_iblock->filt_ents[par_entry].filter_mask != filter_mask) {
                par_iblock->filt_ents[par_entry].filter_mask = filter_mask;
                par_changed                                  = true;
            } /* end if */

            /* verify that the cache's last record of the compressed
             * size matches the heap's last record.  This value will
             * likely change shortly.
             */
            assert(len == par_iblock->filt_ents[par_entry].size);

            /* Check if we need to re-size the block on disk */
            if (par_iblock->filt_ents[par_entry].size != write_size || at_tmp_addr) {
                /* Check if the direct block is NOT currently allocated
                 * in temp. file space
                 *
                 * (temp. file space does not need to be freed)
                 */
                if (!at_tmp_addr)
                    /* Release direct block's current disk space */
                    if (H5MF_xfree(f, H5FD_MEM_FHEAP_DBLOCK, addr,
                                   (hsize_t)par_iblock->filt_ents[par_entry].size) < 0)
                        HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "unable to free fractal heap direct block");

                /* Allocate space for the compressed direct block */
                if (HADDR_UNDEF ==
                    (dblock_addr = H5MF_alloc((H5F_t *)f, H5FD_MEM_FHEAP_DBLOCK, (hsize_t)write_size)))
                    HGOTO_ERROR(H5E_HEAP, H5E_NOSPACE, FAIL,
                                "file allocation failed for fractal heap direct block");

                /* Update information about compressed direct block's
                 * location & size
                 */
                assert(par_iblock->ents[par_entry].addr == addr);
                assert(par_iblock->filt_ents[par_entry].size == len);
                par_iblock->ents[par_entry].addr      = dblock_addr;
                par_iblock->filt_ents[par_entry].size = write_size;

                /* Note that parent was modified */
                par_changed = true;
            } /* end if */

            /* Check if parent was modified */
            if (par_changed)
                if (H5HF__iblock_dirty(par_iblock) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTDIRTY, FAIL, "can't mark heap header as dirty");
        } /* end else */
    }     /* end if */
    else {
        /* I/O filters are not enabled -- thus all we need to do is check to
         * see if the direct block is in temporary (AKA imaginary) file
         * space, and move it to real file space if it is.
         *
         * As in the I/O filters case above, we will have to touch up the
         * direct blocks parent if the direct block is relocated.
         *
         * Recall that temporary file space need not be freed, which
         * simplifies matters slightly.
         */
        write_buf  = dblock->blk;
        write_size = dblock->size;

        /* Check to see if we must re-allocate direct block from 'temp.'
         * to 'normal' file space
         */
        if (at_tmp_addr) {
            /* Allocate 'normal' space for the direct block */
            if (HADDR_UNDEF ==
                (dblock_addr = H5MF_alloc((H5F_t *)f, H5FD_MEM_FHEAP_DBLOCK, (hsize_t)write_size)))
                HGOTO_ERROR(H5E_HEAP, H5E_NOSPACE, FAIL,
                            "file allocation failed for fractal heap direct block");

            /* Check for root direct block */
            if (NULL == dblock->parent) {
                /* Sanity checks */
                assert(H5_addr_eq(hdr->man_dtable.table_addr, addr));
                assert(!H5_addr_eq(hdr->man_dtable.table_addr, dblock_addr));

                /* Update information about direct block's location */
                hdr->man_dtable.table_addr = dblock_addr;

                /* Mark that heap header was modified */
                if (H5HF__hdr_dirty(hdr) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTDIRTY, FAIL, "can't mark heap header as dirty");
            }      /* end if */
            else { /* the direct block's parent is an indirect block */
                /* Sanity checks */
                assert(par_iblock);
                assert(par_iblock->ents);
                assert(H5_addr_eq(par_iblock->ents[par_entry].addr, addr));
                assert(!H5_addr_eq(par_iblock->ents[par_entry].addr, dblock_addr));

                /* Update information about direct block's location */
                par_iblock->ents[par_entry].addr = dblock_addr;

                /* Mark that parent was modified */
                if (H5HF__iblock_dirty(par_iblock) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTDIRTY, FAIL, "can't mark heap header as dirty");
            } /* end else */
        }     /* end if */
    }         /* end else */

    /* At this point, write_buf points to a buffer containing the image
     * of the direct block that is ready to copy into the image buffer,
     * and write_size contains the length of this buffer.
     *
     * Also, if image size or address has changed, the direct block's
     * parent has been modified to reflect the change.
     *
     * Now, make note of the pointer and length of the above buffer for
     * use by the serialize function.
     */
    dblock->write_buf  = (uint8_t *)write_buf;
    dblock->write_size = write_size;

    /* finally, pass data back to the metadata cache as appropriate */
    if (!H5_addr_eq(addr, dblock_addr)) {
        dblock_flags |= H5AC__SERIALIZE_MOVED_FLAG;
        *new_addr = dblock_addr;
    } /* end if */

    if ((hdr->filter_len > 0) && (len != write_size)) {
        dblock_flags |= H5AC__SERIALIZE_RESIZED_FLAG;
        *new_len = write_size;
    } /* end if */

    *flags = dblock_flags;

    /* final sanity check */
    assert(dblock->write_buf);
    assert(dblock->write_size > 0);

done:
    /* discard the write buf if we have an error */
    if (write_buf && (write_buf != dblock->blk) && (dblock->write_buf == NULL))
        H5MM_xfree(write_buf);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__cache_dblock_pre_serialize() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__cache_dblock_serialize
 *
 * Purpose:	In principle, this function is supposed to construct the on
 *		disk image of the direct block, and place that image in the
 *		image buffer provided by the metadata cache.
 *
 *		However, since there are cases in which the pre_serialize
 *		function has to construct the on disk image to determine its size
 *		and address, this function simply copies the image prepared by
 *		the pre-serialize function into the supplied image buffer, and
 *		discards a buffer if necessary.
 *
 * Return:	Success:	SUCCEED
 *		Failure:	FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__cache_dblock_serialize(const H5F_t H5_ATTR_NDEBUG_UNUSED *f, void *image,
                             size_t H5_ATTR_NDEBUG_UNUSED len, void *_thing)
{
    H5HF_direct_t *dblock    = (H5HF_direct_t *)_thing; /* Direct block info */
    herr_t         ret_value = SUCCEED;                 /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(f);
    assert(image);
    assert(len > 0);
    assert(dblock);
    assert(dblock->cache_info.type == H5AC_FHEAP_DBLOCK);
    assert((dblock->blk != dblock->write_buf) || (dblock->cache_info.size == dblock->size));
    assert(dblock->write_buf);
    assert(dblock->write_size > 0);
    assert((dblock->blk != dblock->write_buf) || (dblock->write_size == dblock->size));
    assert(dblock->write_size == len);

    /* Copy the image from *(dblock->write_buf) to *image */
    H5MM_memcpy(image, dblock->write_buf, dblock->write_size);

    /* Free *(dblock->write_buf) if it was allocated by the
     * pre-serialize function
     */
    if (dblock->write_buf != dblock->blk)
        H5MM_xfree(dblock->write_buf);

    /* Reset the write_buf and write_size fields */
    dblock->write_buf  = NULL;
    dblock->write_size = 0;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__cache_dblock_serialize() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__cache_dblock_notify
 *
 * Purpose:	Setup / takedown flush dependencies as direct blocks
 *		are loaded / inserted and evicted from the metadata cache.
 *
 * Return:	Success:	SUCCEED
 *		Failure:	FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__cache_dblock_notify(H5AC_notify_action_t action, void *_thing)
{
    H5HF_direct_t *dblock    = (H5HF_direct_t *)_thing; /* Fractal heap direct block */
    herr_t         ret_value = SUCCEED;                 /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(dblock);
    assert(dblock->cache_info.type == H5AC_FHEAP_DBLOCK);
    assert(dblock->hdr);

    switch (action) {
        case H5AC_NOTIFY_ACTION_AFTER_INSERT:
        case H5AC_NOTIFY_ACTION_AFTER_LOAD:
            /* Create flush dependency with parent, if there is one */
            if (dblock->fd_parent)
                if (H5AC_create_flush_dependency(dblock->fd_parent, dblock) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTDEPEND, FAIL, "unable to create flush dependency");
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
            if (dblock->fd_parent) {
                /* Destroy flush dependency with parent */
                if (H5AC_destroy_flush_dependency(dblock->fd_parent, dblock) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTUNDEPEND, FAIL, "unable to destroy flush dependency");
                dblock->fd_parent = NULL;
            } /* end if */
            break;

        default:
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unknown action from metadata cache");
            break;
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__cache_dblock_notify() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__cache_dblock_free_icr
 *
 * Purpose:	Free the in core memory allocated to the supplied direct
 *		block.
 *
 * Return:	Success:	SUCCEED
 *		Failure:	FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__cache_dblock_free_icr(void *_thing)
{
    H5HF_direct_t *dblock    = (H5HF_direct_t *)_thing; /* Fractal heap direct block */
    herr_t         ret_value = SUCCEED;                 /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(dblock);
    assert(dblock->cache_info.type == H5AC_FHEAP_DBLOCK);

    /* Destroy fractal heap direct block */
    if (H5HF__man_dblock_dest(dblock) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "unable to destroy fractal heap direct block");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__cache_dblock_free_icr() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__cache_dblock_fsf_size
 *
 * Purpose:     Tell the metadata cache the actual amount of file space
 *              to free when a dblock entry is destroyed with the free
 *              file space flag set.
 *
 * Return:	Success:	SUCCEED
 *		Failure:	FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__cache_dblock_fsf_size(const void *_thing, hsize_t *fsf_size)
{
    const H5HF_direct_t *dblock = (const H5HF_direct_t *)_thing; /* Fractal heap direct block */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(dblock);
    assert(dblock->cache_info.type == H5AC_FHEAP_DBLOCK);
    assert(dblock->file_size > 0);
    assert(fsf_size);

    /* Set free space in file size */
    *fsf_size = dblock->file_size;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HF__cache_dblock_fsf_size() */

/*------------------------------------------------------------------------
 * Function:	H5HF__cache_verify_hdr_descendants_clean
 *
 * Purpose:	Sanity checking routine that verifies that all indirect
 *		and direct blocks that are descendants of the supplied
 *		instance of H5HF_hdr_t are clean.  Set *clean to
 *		true if this is the case, and to false otherwise.
 *
 *		Update -- 8/24/15
 *
 *		With the advent of the metadata cache image feature, it is
 *		possible for the pre-serialize and serialize calls to be
 *		invoked outside of a flush.  While this serialization
 *		observes flush dependencies for the order of serialization,
 *		the entries are not written to disk, and hence dirty entries
 *		remain dirty.
 *
 *		To address this, updated the sanity checks in this function
 *		to treat entries whose images are up to date as clean if
 *		a cache serialization is in progress.
 *
 *		Update -- 9/29/16
 *
 *		The implementation of flush dependencies has been changed.
 *		Prior to this change, a flush dependency parent could be
 *		flushed if and only if all its flush dependency descendants
 *		were clean.  In the new definition, a flush dependency
 *		parent can be flushed if all its immediate flush dependency
 *		children are clean, regardless of any other dirty
 *		descendants.
 *
 *		Further, metadata cache entries are now allowed to have
 *		multiple flush dependency parents.
 *
 *		This means that the fractal heap is no longer necessarily
 *		flushed from the bottom up.
 *
 *		For example, it is now possible for a dirty fractal heap
 *		header to be flushed before a dirty dblock, as long as the
 *		there in an intervening iblock, and the header has no
 *		dirty immediate flush dependency children.
 *
 *		Also, I gather that under some circumstances, a dblock
 *		will be direct a flush dependency child both of the iblock
 *		that points to it, and of the fractal heap header.
 *
 *		As a result of these changes, the functionality of these
 *		sanity checking routines has been modified significantly.
 *		Instead of scanning the fractal heap from a starting point
 *		down, and verifying that there were no dirty entries, the
 *		functions now scan downward from the starting point and
 *		verify that there are no dirty flush dependency children
 *		of the specified flush dependency parent.  In passing,
 *		they also walk the data structure, and verify it.
 *
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
#ifndef NDEBUG
static herr_t
H5HF__cache_verify_hdr_descendants_clean(H5F_t *f, H5HF_hdr_t *hdr, bool *fd_clean, bool *clean)
{
    bool     fd_exists = false;    /* whether flush dependency exists. */
    haddr_t  hdr_addr;             /* Address of header */
    unsigned hdr_status = 0;       /* Header cache entry status */
    herr_t   ret_value  = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(hdr);
    assert(hdr->cache_info.type == H5AC_FHEAP_HDR);
    assert(fd_clean);
    assert(clean);
    hdr_addr = hdr->cache_info.addr;
    assert(hdr_addr == hdr->heap_addr);

    if (H5AC_get_entry_status(f, hdr_addr, &hdr_status) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't get hdr status");
    assert(hdr_status & H5AC_ES__IN_CACHE);

    /* We have three basic scenarios we have to deal with:
     *
     * The first, and most common case, is that there is a root iblock.
     * In this case we need to verify that the root iblock and all its
     * children are clean.
     *
     * The second, and much less common case, is that in which the
     * the fractal heap contains only one direct block, which is
     * pointed to by hdr->man_dtable.table_addr.  In this case, all we
     * need to do is verify that the root direct block is clean.
     *
     * Finally, it is possible that the fractal heap is empty, and
     * has neither a root indirect block nor a root direct block.
     * In this case, we have nothing to do.
     */

    /* There are two ways in which we can arrive at the first scenario.
     *
     * By far the most common is when hdr->root_iblock contains a pointer
     * to the root iblock -- in this case the root iblock is almost certainly
     * pinned, although we can't count on that.
     *
     * However, it is also possible that there is a root iblock that
     * is no longer pointed to by the header.  In this case, the on
     * disk address of the iblock will be in hdr->man_dtable.table_addr
     * and hdr->man_dtable.curr_root_rows will contain a positive value.
     *
     * Since the former case is far and away the most common, we don't
     * worry too much about efficiency in the second case.
     */
    if (hdr->root_iblock ||
        ((hdr->man_dtable.curr_root_rows > 0) && (HADDR_UNDEF != hdr->man_dtable.table_addr))) {
        H5HF_indirect_t *root_iblock = hdr->root_iblock;
        haddr_t          root_iblock_addr;
        unsigned         root_iblock_status = 0;
        bool             root_iblock_in_cache;

        /* make note of the on disk address of the root iblock */
        if (root_iblock == NULL)
            /* hdr->man_dtable.table_addr must contain address of root
             * iblock.  Check to see if it is in cache.  If it is,
             * protect it and put its address in root_iblock.
             */
            root_iblock_addr = hdr->man_dtable.table_addr;
        else
            root_iblock_addr = root_iblock->addr;

        /* get the status of the root iblock */
        assert(root_iblock_addr != HADDR_UNDEF);
        if (H5AC_get_entry_status(f, root_iblock_addr, &root_iblock_status) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't get root iblock status");

        root_iblock_in_cache = ((root_iblock_status & H5AC_ES__IN_CACHE) != 0);
        assert(root_iblock_in_cache || (root_iblock == NULL));

        if (!root_iblock_in_cache) { /* we are done */
            *clean    = true;
            *fd_clean = true;
        } /* end if */
        else if ((root_iblock_status & H5AC_ES__IS_DIRTY) &&
                 (((root_iblock_status & H5AC_ES__IMAGE_IS_UP_TO_DATE) == 0) ||
                  (!H5AC_get_serialization_in_progress(f)))) {
            *clean = false;

            /* verify that a flush dependency exists between the header and
             * the root inode.
             */
            if (H5AC_flush_dependency_exists(f, hdr->heap_addr, root_iblock_addr, &fd_exists) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't check flush dependency");
            assert(fd_exists);

            *fd_clean = false;
        }      /* end else-if */
        else { /* must examine children */
            bool unprotect_root_iblock = false;

            /* At this point, the root iblock may be pinned, protected,
             * both, or neither, and we may or may not have a pointer
             * to root iblock in memory.
             *
             * Before we call H5HF__cache_verify_iblock_descendants_clean(),
             * we must ensure that the root iblock is either pinned or
             * protected or both, and that we have a pointer to it.
             * Do this as follows:
             */
            if (root_iblock == NULL) { /* we don't have ptr to root iblock */
                if (0 == (root_iblock_status & H5AC_ES__IS_PROTECTED)) {
                    /* just protect the root iblock -- this will give us
                     * the pointer we need to proceed, and ensure that
                     * it is locked into the metadata cache for the
                     * duration.
                     *
                     * Note that the udata is only used in the load callback.
                     * While the fractal heap makes heavy use of the udata
                     * in this case, since we know that the entry is in cache,
                     * we can pass NULL udata.
                     *
                     * The tag specified in the API context we received
                     * as a parameter (via API context) may not be correct.
                     * Grab the (hopefully) correct tag from the header,
                     * and load it into the API context via the H5_BEGIN_TAG and
                     * H5_END_TAG macros.  Note that any error bracked by
                     * these macros must be reported with HGOTO_ERROR_TAG.
                     */
                    H5_BEGIN_TAG(hdr->heap_addr)

                    if (NULL == (root_iblock = (H5HF_indirect_t *)H5AC_protect(
                                     f, H5AC_FHEAP_IBLOCK, root_iblock_addr, NULL, H5AC__READ_ONLY_FLAG)))
                        HGOTO_ERROR_TAG(H5E_HEAP, H5E_CANTPROTECT, FAIL, "H5AC_protect() failed.");

                    H5_END_TAG

                    unprotect_root_iblock = true;
                } /* end if */
                else {
                    /* the root iblock is protected, and we have no
                     * legitimate way of getting a pointer to it.
                     *
                     * We square this circle by using the
                     * H5AC_get_entry_ptr_from_addr() to get the needed
                     * pointer.
                     *
                     * WARNING: This call should be used only in debugging
                     *          routines, and it should be avoided there when
                     *          possible.
                     *
                     *          Further, if we ever multi-thread the cache,
                     *          this routine will have to be either discarded
                     *          or heavily re-worked.
                     *
                     *          Finally, keep in mind that the entry whose
                     *          pointer is obtained in this fashion may not
                     *          be in a stable state.
                     *
                     * Assuming that the flush dependency code is working
                     * as it should, the only reason for the root iblock to
                     * be unpinned is if none of its children are in cache.
                     * This unfortunately means that if it is protected and
                     * not pinned, the fractal heap is in the process of loading
                     * or inserting one of its children.  The obvious
                     * implication is that there is a significant chance that
                     * the root iblock is in an unstable state.
                     *
                     * All this suggests that using
                     * H5AC_get_entry_ptr_from_addr() to obtain the pointer
                     * to the protected root iblock is questionable here.
                     * However, since this is test/debugging code, I expect
                     * that we will use this approach until it causes problems,
                     *  or we think of a better way.
                     */
                    if (H5AC_get_entry_ptr_from_addr(f, root_iblock_addr, (void **)(&root_iblock)) < 0)
                        HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "H5AC_get_entry_ptr_from_addr() failed.");
                    assert(root_iblock);
                }  /* end else */
            }      /* end if */
            else { /* root_iblock != NULL */
                   /* we have the pointer to the root iblock.  Protect it
                    * if it is neither pinned nor protected -- otherwise we
                    * are ready to go.
                    */
                H5HF_indirect_t *iblock = NULL;

                if (((root_iblock_status & H5AC_ES__IS_PINNED) == 0) &&
                    ((root_iblock_status & H5AC_ES__IS_PROTECTED) == 0)) {
                    /* the root iblock is neither pinned nor protected -- hence
                     * we must protect it before we proceed
                     *
                     * Note that the udata is only used in the load callback.
                     * While the fractal heap makes heavy use of the udata
                     * in this case, since we know that the entry is in cache,
                     * we can pass NULL udata.
                     *
                     * The tag associated specified in the API context we received
                     * as a parameter (via API context) may not be correct.
                     * Grab the (hopefully) correct tag from the header,
                     * and load it into the API context via the H5_BEGIN_TAG and
                     * H5_END_TAG macros.  Note that any error bracked by
                     * these macros must be reported with HGOTO_ERROR_TAG.
                     */
                    H5_BEGIN_TAG(hdr->heap_addr)

                    if (NULL == (iblock = (H5HF_indirect_t *)H5AC_protect(
                                     f, H5AC_FHEAP_IBLOCK, root_iblock_addr, NULL, H5AC__READ_ONLY_FLAG)))
                        HGOTO_ERROR_TAG(H5E_HEAP, H5E_CANTPROTECT, FAIL, "H5AC_protect() failed.");

                    H5_END_TAG

                    unprotect_root_iblock = true;
                    assert(iblock == root_iblock);
                } /* end if */
            }     /* end else */

            /* at this point, one way or another, the root iblock is locked
             * in memory for the duration of the call.  Do some sanity checks,
             * and then call H5HF__cache_verify_iblock_descendants_clean().
             */
            assert(root_iblock->cache_info.type == H5AC_FHEAP_IBLOCK);

            if (H5HF__cache_verify_iblock_descendants_clean(f, hdr->heap_addr, root_iblock,
                                                            &root_iblock_status, fd_clean, clean) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_SYSTEM, FAIL, "can't verify root iblock & descendants clean.");

            /* Unprotect the root indirect block if required */
            if (unprotect_root_iblock) {
                assert(root_iblock);
                if (H5AC_unprotect(f, H5AC_FHEAP_IBLOCK, root_iblock_addr, root_iblock, H5AC__NO_FLAGS_SET) <
                    0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "H5AC_unprotect() failed.");
            } /* end if */
        }     /* end else */
    }         /* end if */
    else if ((hdr->man_dtable.curr_root_rows == 0) && (HADDR_UNDEF != hdr->man_dtable.table_addr)) {
        haddr_t  root_dblock_addr;
        unsigned root_dblock_status = 0;
        bool     in_cache;
        bool     type_ok;

        /* this is scenario 2 -- we have a root dblock */
        root_dblock_addr = hdr->man_dtable.table_addr;
        if (H5AC_get_entry_status(f, root_dblock_addr, &root_dblock_status) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't get root dblock status");

        if (root_dblock_status & H5AC_ES__IN_CACHE) {
            if (H5AC_verify_entry_type(f, root_dblock_addr, &H5AC_FHEAP_DBLOCK[0], &in_cache, &type_ok) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't check dblock type");
            assert(in_cache);
            if (!type_ok)
                HGOTO_ERROR(H5E_HEAP, H5E_SYSTEM, FAIL, "root dblock addr doesn't refer to a dblock?!?");

            /* If a root dblock is in cache, it must have a flush
             * dependency relationship with the header, and it
             * may not be the parent in any flush dependency
             * relationship.
             *
             * We don't test this fully, but we will verify that
             * the root iblock is a child in a flush dependency
             * relationship with the header.
             */
            if (H5AC_flush_dependency_exists(f, hdr->heap_addr, root_dblock_addr, &fd_exists) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't check flush dependency");
            if (!fd_exists)
                HGOTO_ERROR(H5E_HEAP, H5E_SYSTEM, FAIL, "root dblock is not a flush dep parent of header.");

            if (0 != (root_dblock_status & H5AC_ES__IS_FLUSH_DEP_PARENT))
                HGOTO_ERROR(H5E_HEAP, H5E_SYSTEM, FAIL, "root dblock in cache and is a flush dep parent.");

            *clean = !((root_dblock_status & H5AC_ES__IS_DIRTY) &&
                       (((root_dblock_status & H5AC_ES__IMAGE_IS_UP_TO_DATE) == 0) ||
                        (!H5AC_get_serialization_in_progress(f))));

            *fd_clean = *clean;
        }      /* end if */
        else { /* root dblock not in cache */
            *fd_clean = true;
            *clean    = true;
        } /* end else */
    }     /* end else-if */
    else {
        /* this is scenario 3 -- the fractal heap is empty, and we
         * have nothing to do.
         */
        *fd_clean = true;
        *clean    = true;
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__cache_verify_hdr_descendants_clean() */
#endif /* NDEBUG */

/*------------------------------------------------------------------------
 * Function:	H5HF__cache_verify_iblock_descendants_clean
 *
 * Purpose:	Sanity checking routine that verifies that all indirect
 *		and direct blocks that are descendants of the supplied
 *		instance of H5HF_indirect_t are clean.  Set *clean
 *		to true if this is the case, and to false otherwise.
 *
 *		In passing, the function also does a cursory check to
 *		spot any obvious errors in the flush dependency setup.
 *		If any problems are found, the function returns failure.
 *		Note that these checks are not exhaustive, thus passing
 *		them does not mean that the flush dependencies are
 *		correct -- only that there is nothing obviously wrong
 *		with them.
 *
 *		WARNING:  At its top level call, this function is
 *		intended to be called from H5HF_cache_iblock_flush(),
 *		and thus presumes that the supplied indirect block
 *		is in cache.  Any other use of this function and
 *		its descendants must insure that this assumption is
 *		met.
 *
 *		Note that this function and
 *		H5HF__cache_verify_descendant_iblocks_clean() are
 *		recursive co-routines.
 *
 *		Update -- 9/29/16
 *
 *		The implementation of flush dependencies has been changed.
 *		Prior to this change, a flush dependency parent could be
 *		flushed if and only if all its flush dependency descendants
 *		were clean.  In the new definition, a flush dependency
 *		parent can be flushed if all its immediate flush dependency
 *		children are clean, regardless of any other dirty
 *		descendants.
 *
 *		Further, metadata cache entries are now allowed to have
 *		multiple flush dependency parents.
 *
 *		This means that the fractal heap is no longer necessarily
 *		flushed from the bottom up.
 *
 *		For example, it is now possible for a dirty fractal heap
 *		header to be flushed before a dirty dblock, as long as the
 *		there in an intervening iblock, and the header has no
 *		dirty immediate flush dependency children.
 *
 *		Also, I gather that under some circumstances, a dblock
 *		will be direct a flush dependency child both of the iblock
 *		that points to it, and of the fractal heap header.
 *
 *		As a result of these changes, the functionality of these
 *		sanity checking routines has been modified significantly.
 *		Instead of scanning the fractal heap from a starting point
 *		down, and verifying that there were no dirty entries, the
 *		functions now scan downward from the starting point and
 *		verify that there are no dirty flush dependency children
 *		of the specified flush dependency parent.  In passing,
 *		they also walk the data structure, and verify it.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
#ifndef NDEBUG
static herr_t
H5HF__cache_verify_iblock_descendants_clean(H5F_t *f, haddr_t fd_parent_addr, H5HF_indirect_t *iblock,
                                            unsigned *iblock_status, bool *fd_clean, bool *clean)
{
    bool   has_dblocks = false;
    bool   has_iblocks = false;
    herr_t ret_value   = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(H5_addr_defined(fd_parent_addr));
    assert(iblock);
    assert(iblock->cache_info.type == H5AC_FHEAP_IBLOCK);
    assert(iblock_status);
    assert(fd_clean);
    assert(*fd_clean);
    assert(clean); /* note that *clean need not be true */

    if ((*fd_clean) && H5HF__cache_verify_iblocks_dblocks_clean(f, fd_parent_addr, iblock, fd_clean, clean,
                                                                &has_dblocks) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_SYSTEM, FAIL, "can't verify dblocks clean.");

    if ((*fd_clean) && H5HF__cache_verify_descendant_iblocks_clean(f, fd_parent_addr, iblock, fd_clean, clean,
                                                                   &has_iblocks) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_SYSTEM, FAIL, "can't verify iblocks clean.");

    /* verify that flush dependency setup is plausible */
    if (0 == (*iblock_status & H5AC_ES__IS_FLUSH_DEP_CHILD))
        HGOTO_ERROR(H5E_HEAP, H5E_SYSTEM, FAIL, "iblock is not a flush dep child.");
    if (((has_dblocks || has_iblocks)) && (0 == (*iblock_status & H5AC_ES__IS_FLUSH_DEP_PARENT)))
        HGOTO_ERROR(H5E_HEAP, H5E_SYSTEM, FAIL, "iblock has children and is not a flush dep parent.");
    if (((has_dblocks || has_iblocks)) && (0 == (*iblock_status & H5AC_ES__IS_PINNED)))
        HGOTO_ERROR(H5E_HEAP, H5E_SYSTEM, FAIL, "iblock has children and is not pinned.");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__cache_verify_iblock_descendants_clean() */
#endif /* NDEBUG */

/*------------------------------------------------------------------------
 * Function:	H5HF__cache_verify_iblocks_dblocks_clean
 *
 * Purpose:	Sanity checking routine that attempts to verify that all
 *		direct blocks pointed to by the supplied indirect block
 *		are either clean, or not in the cache.
 *
 *		In passing, the function also does a cursory check to
 *		spot any obvious errors in the flush dependency setup.
 *		If any problems are found, the function returns failure.
 *		Note that these checks are not exhaustive, thus passing
 *		them does not mean that the flush dependencies are
 *		correct -- only that there is nothing obviously wrong
 *		with them.
 *
 *		WARNING:  This function presumes that the supplied
 *		iblock is in the cache, and will not be removed
 *		during the call.  Caller must ensure that this is
 *		the case before the call.
 *
 *      Update -- 8/24/15
 *
 *      With the advent of the metadata cache image feature, it is
 *      possible for the pre-serialize and serialize calls to be
 *      invoked outside of a flush.  While this serialization
 *      observes flush dependencies for the order of serialization,
 *      the entries are not written to disk, and hence dirty entries
 *      remain dirty.
 *
 *      To address this, updated the sanity checks in this function
 *      to treat entries whose images are up to date as clean if
 *      a cache serialization is in progress.
 *
 *		Update -- 9/29/16
 *
 *		The implementation of flush dependencies has been changed.
 *		Prior to this change, a flush dependency parent could be
 *		flushed if and only if all its flush dependency descendants
 *		were clean.  In the new definition, a flush dependency
 *		parent can be flushed if all its immediate flush dependency
 *		children are clean, regardless of any other dirty
 *		descendants.
 *
 *		Further, metadata cache entries are now allowed to have
 *		multiple flush dependency parents.
 *
 *		This means that the fractal heap is no longer necessarily
 *		flushed from the bottom up.
 *
 *		For example, it is now possible for a dirty fractal heap
 *		header to be flushed before a dirty dblock, as long as the
 *		there in an intervening iblock, and the header has no
 *		dirty immediate flush dependency children.
 *
 *		Also, I gather that under some circumstances, a dblock
 *		will be direct a flush dependency child both of the iblock
 *		that points to it, and of the fractal heap header.
 *
 *		As a result of these changes, the functionality of these
 *		sanity checking routines has been modified significantly.
 *		Instead of scanning the fractal heap from a starting point
 *		down, and verifying that there were no dirty entries, the
 *		functions now scan downward from the starting point and
 *		verify that there are no dirty flush dependency children
 *		of the specified flush dependency parent.  In passing,
 *		they also walk the data structure, and verify it.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
#ifndef NDEBUG
static herr_t
H5HF__cache_verify_iblocks_dblocks_clean(H5F_t *f, haddr_t fd_parent_addr, H5HF_indirect_t *iblock,
                                         bool *fd_clean, bool *clean, bool *has_dblocks)
{
    unsigned num_direct_rows;
    unsigned max_dblock_index;
    unsigned i;
    haddr_t  iblock_addr;
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(H5_addr_defined(fd_parent_addr));
    assert(iblock);
    assert(iblock->cache_info.type == H5AC_FHEAP_IBLOCK);
    assert(fd_clean);
    assert(*fd_clean);
    assert(clean); /* note that *clean need not be true */
    assert(has_dblocks);

    i               = 0;
    num_direct_rows = MIN(iblock->nrows, iblock->hdr->man_dtable.max_direct_rows);
    assert(num_direct_rows <= iblock->nrows);
    max_dblock_index = (num_direct_rows * iblock->hdr->man_dtable.cparam.width) - 1;
    iblock_addr      = iblock->addr;
    assert(H5_addr_defined(iblock_addr));

    while ((*fd_clean) && (i <= max_dblock_index)) {
        haddr_t dblock_addr;

        dblock_addr = iblock->ents[i].addr;
        if (H5_addr_defined(dblock_addr)) {
            bool in_cache;
            bool type_ok;

            if (H5AC_verify_entry_type(f, dblock_addr, &H5AC_FHEAP_DBLOCK[0], &in_cache, &type_ok) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't check dblock type");

            if (in_cache) { /* dblock is in cache */
                bool     fd_exists;
                unsigned dblock_status = 0;

                if (!type_ok)
                    HGOTO_ERROR(H5E_HEAP, H5E_SYSTEM, FAIL, "dblock addr doesn't refer to a dblock?!?");

                if (H5AC_get_entry_status(f, dblock_addr, &dblock_status) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't get dblock status");

                assert(dblock_status & H5AC_ES__IN_CACHE);

                *has_dblocks = true;

                if ((dblock_status & H5AC_ES__IS_DIRTY) &&
                    (((dblock_status & H5AC_ES__IMAGE_IS_UP_TO_DATE) == 0) ||
                     (!H5AC_get_serialization_in_progress(f)))) {
                    *clean = false;

                    if (H5AC_flush_dependency_exists(f, fd_parent_addr, dblock_addr, &fd_exists) < 0)
                        HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't check flush dependency");

                    if (fd_exists)
                        *fd_clean = false;
                } /* end if */

                /* If a child dblock is in cache, it must have a flush
                 * dependency relationship with this iblock.  Test this
                 * here.
                 */
                if (H5AC_flush_dependency_exists(f, iblock_addr, dblock_addr, &fd_exists) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't check flush dependency");

                if (!fd_exists)
                    HGOTO_ERROR(H5E_HEAP, H5E_SYSTEM, FAIL,
                                "dblock in cache and not a flush dep child of iblock.");
            } /* end if */
        }     /* end if */

        i++;
    } /* end while */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__cache_verify_iblocks_dblocks_clean() */
#endif /* NDEBUG */

/*------------------------------------------------------------------------
 * Function:	H5HF__cache_verify_descendant_iblocks_clean
 *
 * Purpose:	Sanity checking routine that attempts to verify that all
 *		direct blocks pointed to by the supplied indirect block
 *		are either clean, or not in the cache.
 *
 *		In passing, the function also does a cursory check to
 *		spot any obvious errors in the flush dependency setup.
 *		If any problems are found, the function returns failure.
 *		Note that these checks are not exhaustive, thus passing
 *		them does not mean that the flush dependencies are
 *		correct -- only that there is nothing obviously wrong
 *		with them.
 *
 *		WARNING:  This function presumes that the supplied
 *		iblock is in the cache, and will not be removed
 *		during the call.  Caller must ensure that this is
 *		the case before the call.
 *
 *              Update -- 8/24/15
 *
 *              With the advent of the metadata cache image feature, it is
 *              possible for the pre-serialize and serialize calls to be
 *              invoked outside of a flush.  While this serialization
 *              observes flush dependencies for the order of serialization,
 *              the entries are not written to disk, and hence dirty entries
 *              remain dirty.
 *
 *              To address this, updated the sanity checks in this function
 *              to treat entries whose images are up to date as clean if
 *              a cache serialization is in progress.
 *
 *		Update -- 9/29/16
 *
 *		The implementation of flush dependencies has been changed.
 *		Prior to this change, a flush dependency parent could be
 *		flushed if and only if all its flush dependency descendants
 *		were clean.  In the new definition, a flush dependency
 *		parent can be flushed if all its immediate flush dependency
 *		children are clean, regardless of any other dirty
 *		descendants.
 *
 *		Further, metadata cache entries are now allowed to have
 *		multiple flush dependency parents.
 *
 *		This means that the fractal heap is no longer necessarily
 *		flushed from the bottom up.
 *
 *		For example, it is now possible for a dirty fractal heap
 *		header to be flushed before a dirty dblock, as long as the
 *		there in an intervening iblock, and the header has no
 *		dirty immediate flush dependency children.
 *
 *		Also, I gather that under some circumstances, a dblock
 *		will be direct a flush dependency child both of the iblock
 *		that points to it, and of the fractal heap header.
 *
 *		As a result of these changes, the functionality of these
 *		sanity checking routines has been modified significantly.
 *		Instead of scanning the fractal heap from a starting point
 *		down, and verifying that there were no dirty entries, the
 *		functions now scan downward from the starting point and
 *		verify that there are no dirty flush dependency children
 *		of the specified flush dependency parent.  In passing,
 *		they also walk the data structure, and verify it.
 *
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
#ifndef NDEBUG
static herr_t
H5HF__cache_verify_descendant_iblocks_clean(H5F_t *f, haddr_t fd_parent_addr, H5HF_indirect_t *iblock,
                                            bool *fd_clean, bool *clean, bool *has_iblocks)
{
    unsigned first_iblock_index;
    unsigned last_iblock_index;
    unsigned num_direct_rows;
    unsigned i;
    haddr_t  iblock_addr;
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(H5_addr_defined(fd_parent_addr));
    assert(iblock);
    assert(iblock->cache_info.type == H5AC_FHEAP_IBLOCK);
    assert(fd_clean);
    assert(*fd_clean);
    assert(clean); /* note that *clean need not be true */
    assert(has_iblocks);
    num_direct_rows = MIN(iblock->nrows, iblock->hdr->man_dtable.max_direct_rows);
    assert(num_direct_rows <= iblock->nrows);

    iblock_addr        = iblock->addr;
    first_iblock_index = num_direct_rows * iblock->hdr->man_dtable.cparam.width;
    last_iblock_index  = (iblock->nrows * iblock->hdr->man_dtable.cparam.width) - 1;

    i = first_iblock_index;
    while ((*fd_clean) && (i <= last_iblock_index)) {
        haddr_t child_iblock_addr = iblock->ents[i].addr;

        if (H5_addr_defined(child_iblock_addr)) {
            unsigned child_iblock_status = 0;

            if (H5AC_get_entry_status(f, child_iblock_addr, &child_iblock_status) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't get iblock status");

            if (child_iblock_status & H5AC_ES__IN_CACHE) {
                bool fd_exists;

                *has_iblocks = true;

                if ((child_iblock_status & H5AC_ES__IS_DIRTY) &&
                    (((child_iblock_status & H5AC_ES__IMAGE_IS_UP_TO_DATE) == 0) ||
                     (!H5AC_get_serialization_in_progress(f)))) {

                    *clean = false;

                    if (H5AC_flush_dependency_exists(f, fd_parent_addr, child_iblock_addr, &fd_exists) < 0)
                        HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't check flush dependency");

                    if (fd_exists)
                        *fd_clean = false;
                } /* end if */

                /* if the child iblock is in cache and *fd_clean is true,
                 * we must continue to explore down the fractal heap tree
                 * structure to verify that all descendant blocks that are
                 * flush dependency children of the entry at parent_addr are
                 * either clean, or not in the metadata cache.  We do this
                 * with a recursive call to
                 * H5HF__cache_verify_iblock_descendants_clean().
                 * However, we can't make this call unless the child iblock
                 * is somehow locked into the cache -- typically via either
                 * pinning or protecting.
                 *
                 * If the child iblock is pinned, we can look up its pointer
                 * on the current iblock's pinned child iblock list, and
                 * and use that pointer in the recursive call.
                 *
                 * If the entry is unprotected and unpinned, we simply
                 * protect it.
                 *
                 * If, however, the child iblock is already protected,
                 * but not pinned, we have a bit of a problem, as we have
                 * no legitimate way of looking up its pointer in memory.
                 *
                 * To solve this problem, I have added a new metadata cache
                 * call to obtain the pointer.
                 *
                 * WARNING: This call should be used only in debugging
                 * 	    routines, and it should be avoided there when
                 *	    possible.
                 *
                 *          Further, if we ever multi-thread the cache,
                 *	    this routine will have to be either discarded
                 *	    or heavily re-worked.
                 *
                 *	    Finally, keep in mind that the entry whose
                 *	    pointer is obtained in this fashion may not
                 *          be in a stable state.
                 *
                 * Assuming that the flush dependency code is working
                 * as it should, the only reason for the child entry to
                 * be unpinned is if none of its children are in cache.
                 * This unfortunately means that if it is protected and
                 * not pinned, the fractal heap is in the process of loading
                 * or inserting one of its children.  The obvious implication
                 * is that there is a significant chance that the child
                 * iblock is in an unstable state.
                 *
                 * All this suggests that using the new call to obtain the
                 * pointer to the protected child iblock is questionable
                 * here.  However, since this is test/debugging code, I
                 * expect that we will use this approach until it causes
                 * problems, or we think of a better way.
                 */
                if (*fd_clean) {
                    H5HF_indirect_t *child_iblock           = NULL;
                    bool             unprotect_child_iblock = false;

                    if (0 == (child_iblock_status & H5AC_ES__IS_PINNED)) {
                        /* child iblock is not pinned */
                        if (0 == (child_iblock_status & H5AC_ES__IS_PROTECTED)) {
                            /* child iblock is unprotected, and unpinned */
                            /* protect it.  Note that the udata is only  */
                            /* used in the load callback.  While the     */
                            /* fractal heap makes heavy use of the udata */
                            /* in this case, since we know that the      */
                            /* entry is in cache, we can pass NULL udata */
                            /*                                           */
                            /* The tag associated specified in the API context  */
                            /* we received as a parameter (via API context)  */
                            /* may not be correct.                       */
                            /*                                           */
                            /* Grab the (hopefully) correct tag from the */
                            /* parent iblock, and load it into the API context  */
                            /* via the H5_BEGIN_TAG and H5_END_TAG       */
                            /* macros.  Note that any error bracked by   */
                            /* these macros must be reported with        */
                            /* HGOTO_ERROR_TAG.                          */

                            H5_BEGIN_TAG(iblock->hdr->heap_addr)

                            if (NULL ==
                                (child_iblock = (H5HF_indirect_t *)H5AC_protect(
                                     f, H5AC_FHEAP_IBLOCK, child_iblock_addr, NULL, H5AC__READ_ONLY_FLAG)))
                                HGOTO_ERROR_TAG(H5E_HEAP, H5E_CANTPROTECT, FAIL, "H5AC_protect() failed.");

                            H5_END_TAG

                            unprotect_child_iblock = true;
                        } /* end if */
                        else {
                            /* child iblock is protected -- use             */
                            /* H5AC_get_entry_ptr_from_addr() to get a      */
                            /* pointer to the entry.  This is very slimy -- */
                            /* come up with a better solution.              */
                            if (H5AC_get_entry_ptr_from_addr(f, child_iblock_addr, (void **)(&child_iblock)) <
                                0)
                                HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL,
                                            "H5AC_get_entry_ptr_from_addr() failed.");
                            assert(child_iblock);
                        } /* end else */
                    }     /* end if */
                    else {
                        /* child iblock is pinned -- look it up in the */
                        /* parent iblocks child_iblocks array.         */
                        assert(iblock->child_iblocks);
                        child_iblock = iblock->child_iblocks[i - first_iblock_index];
                    } /* end else */

                    /* At this point, one way or another we should have
                     * a pointer to the child iblock.  Verify that we
                     * that we have the correct one.
                     */
                    assert(child_iblock);
                    assert(child_iblock->cache_info.type == H5AC_FHEAP_IBLOCK);
                    assert(child_iblock->addr == child_iblock_addr);

                    /* now make the recursive call */
                    if (H5HF__cache_verify_iblock_descendants_clean(
                            f, fd_parent_addr, child_iblock, &child_iblock_status, fd_clean, clean) < 0)
                        HGOTO_ERROR(H5E_HEAP, H5E_SYSTEM, FAIL, "can't verify child iblock clean.");

                    /* if iblock_addr != fd_parent_addr, verify that a flush
                     * dependency relationship exists between iblock and
                     * the child iblock.
                     */
                    if (fd_parent_addr != iblock_addr) {
                        if (H5AC_flush_dependency_exists(f, iblock_addr, child_iblock_addr, &fd_exists) < 0)
                            HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't check flush dependency");

                        if (!fd_exists)
                            HGOTO_ERROR(H5E_HEAP, H5E_SYSTEM, FAIL,
                                        "iblock is not a flush dep parent of child_iblock.");
                    } /* end if */

                    /* if we protected the child iblock, unprotect it now */
                    if (unprotect_child_iblock) {
                        if (H5AC_unprotect(f, H5AC_FHEAP_IBLOCK, child_iblock_addr, child_iblock,
                                           H5AC__NO_FLAGS_SET) < 0)
                            HGOTO_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "H5AC_unprotect() failed.");
                    } /* end if */
                }     /* end if */
            }         /* end if */
        }             /* end if */

        i++;
    } /* end while */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF__cache_verify_descendant_iblocks_clean() */
#endif /* NDEBUG */
