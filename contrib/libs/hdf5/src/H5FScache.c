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
 * Created:     H5FScache.c
 *
 * Purpose:     Implement file free space metadata cache methods.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5FSmodule.h" /* This source code file is part of the H5FS module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5ACprivate.h" /* Metadata cache                       */
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Fprivate.h"  /* File          			*/
#include "H5FSpkg.h"     /* File free space			*/
#include "H5MFprivate.h" /* File memory management		*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5VMprivate.h" /* Vectors and arrays 			*/
#include "H5WBprivate.h" /* Wrapped Buffers                      */

/****************/
/* Local Macros */
/****************/

/* File free space format version #'s */
#define H5FS_HDR_VERSION   0 /* Header */
#define H5FS_SINFO_VERSION 0 /* Serialized sections */

/******************/
/* Local Typedefs */
/******************/

/* User data for skip list iterator callback for iterating over section size nodes when syncing */
typedef struct {
    H5FS_sinfo_t *sinfo;         /* Free space section info */
    uint8_t     **image;         /* Pointer to address of buffer pointer to serialize with */
    unsigned      sect_cnt_size; /* # of bytes to encode section size counts in */
} H5FS_iter_ud_t;

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

/* Section info routines */
static herr_t H5FS__sinfo_serialize_sect_cb(void *_item, void H5_ATTR_UNUSED *key, void *_udata);
static herr_t H5FS__sinfo_serialize_node_cb(void *_item, void H5_ATTR_UNUSED *key, void *_udata);

/* Metadata cache callbacks */
static herr_t H5FS__cache_hdr_get_initial_load_size(void *udata, size_t *image_len);
static htri_t H5FS__cache_hdr_verify_chksum(const void *image_ptr, size_t len, void *udata_ptr);
static void  *H5FS__cache_hdr_deserialize(const void *image, size_t len, void *udata, bool *dirty);
static herr_t H5FS__cache_hdr_image_len(const void *thing, size_t *image_len);
static herr_t H5FS__cache_hdr_pre_serialize(H5F_t *f, void *thing, haddr_t addr, size_t len,
                                            haddr_t *new_addr, size_t *new_len, unsigned *flags);
static herr_t H5FS__cache_hdr_serialize(const H5F_t *f, void *image, size_t len, void *thing);
static herr_t H5FS__cache_hdr_notify(H5AC_notify_action_t action, void *thing);
static herr_t H5FS__cache_hdr_free_icr(void *thing);

static herr_t H5FS__cache_sinfo_get_initial_load_size(void *udata, size_t *image_len);
static htri_t H5FS__cache_sinfo_verify_chksum(const void *image_ptr, size_t len, void *udata_ptr);
static void  *H5FS__cache_sinfo_deserialize(const void *image, size_t len, void *udata, bool *dirty);
static herr_t H5FS__cache_sinfo_image_len(const void *thing, size_t *image_len);
static herr_t H5FS__cache_sinfo_pre_serialize(H5F_t *f, void *thing, haddr_t addr, size_t len,
                                              haddr_t *new_addr, size_t *new_len, unsigned *flags);
static herr_t H5FS__cache_sinfo_serialize(const H5F_t *f, void *image, size_t len, void *thing);
static herr_t H5FS__cache_sinfo_notify(H5AC_notify_action_t action, void *thing);
static herr_t H5FS__cache_sinfo_free_icr(void *thing);

/*********************/
/* Package Variables */
/*********************/

/* H5FS header inherits cache-like properties from H5AC */
const H5AC_class_t H5AC_FSPACE_HDR[1] = {{
    H5AC_FSPACE_HDR_ID,                    /* Metadata client ID */
    "Free Space Header",                   /* Metadata client name (for debugging) */
    H5FD_MEM_FSPACE_HDR,                   /* File space memory type for client */
    H5AC__CLASS_NO_FLAGS_SET,              /* Client class behavior flags */
    H5FS__cache_hdr_get_initial_load_size, /* 'get_initial_load_size' callback */
    NULL,                                  /* 'get_final_load_size' callback */
    H5FS__cache_hdr_verify_chksum,         /* 'verify_chksum' callback */
    H5FS__cache_hdr_deserialize,           /* 'deserialize' callback */
    H5FS__cache_hdr_image_len,             /* 'image_len' callback */
    H5FS__cache_hdr_pre_serialize,         /* 'pre_serialize' callback */
    H5FS__cache_hdr_serialize,             /* 'serialize' callback */
    H5FS__cache_hdr_notify,                /* 'notify' callback */
    H5FS__cache_hdr_free_icr,              /* 'free_icr' callback */
    NULL,                                  /* 'fsf_size' callback */
}};

/* H5FS section info inherits cache-like properties from H5AC */
const H5AC_class_t H5AC_FSPACE_SINFO[1] = {{
    H5AC_FSPACE_SINFO_ID,                    /* Metadata client ID */
    "Free Space Section Info",               /* Metadata client name (for debugging) */
    H5FD_MEM_FSPACE_SINFO,                   /* File space memory type for client */
    H5AC__CLASS_NO_FLAGS_SET,                /* Client class behavior flags */
    H5FS__cache_sinfo_get_initial_load_size, /* 'get_initial_load_size' callback */
    NULL,                                    /* 'get_final_load_size' callback */
    H5FS__cache_sinfo_verify_chksum,         /* 'verify_chksum' callback */
    H5FS__cache_sinfo_deserialize,           /* 'deserialize' callback */
    H5FS__cache_sinfo_image_len,             /* 'image_len' callback */
    H5FS__cache_sinfo_pre_serialize,         /* 'pre_serialize' callback */
    H5FS__cache_sinfo_serialize,             /* 'serialize' callback */
    H5FS__cache_sinfo_notify,                /* 'notify' callback */
    H5FS__cache_sinfo_free_icr,              /* 'free_icr' callback */
    NULL,                                    /* 'fsf_size' callback */
}};

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:    H5FS__cache_hdr_get_initial_load_size
 *
 * Purpose:     Compute the size of the data structure on disk.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__cache_hdr_get_initial_load_size(void *_udata, size_t *image_len)
{
    H5FS_hdr_cache_ud_t *udata = (H5FS_hdr_cache_ud_t *)_udata; /* User-data for metadata cache callback */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(udata);
    assert(udata->f);
    assert(image_len);

    /* Set the image length size */
    *image_len = (size_t)H5FS_HEADER_SIZE(udata->f);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FS__cache_hdr_get_initial_load_size() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__cache_hdr_verify_chksum
 *
 * Purpose:     Verify the computed checksum of the data structure is the
 *              same as the stored chksum.
 *
 * Return:      Success:        true/false
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5FS__cache_hdr_verify_chksum(const void *_image, size_t len, void H5_ATTR_UNUSED *_udata)
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
} /* end H5FS__cache_hdr_verify_chksum() */

/*-------------------------------------------------------------------------
 * Function:	H5FS__cache_hdr_deserialize
 *
 * Purpose:	Given a buffer containing the on disk image of the free space
 *      	manager section info, allocate an instance of H5FS_t, load
 *      	it with the data contained in the image, and return a pointer
 *              to the new instance.
 *
 * Return:	Success:	Pointer to new object
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5FS__cache_hdr_deserialize(const void *_image, size_t H5_ATTR_NDEBUG_UNUSED len, void *_udata,
                            bool H5_ATTR_UNUSED *dirty)
{
    H5FS_t              *fspace = NULL;                          /* Free space header info */
    H5FS_hdr_cache_ud_t *udata  = (H5FS_hdr_cache_ud_t *)_udata; /* User data for callback */
    const uint8_t       *image  = (const uint8_t *)_image;       /* Pointer into raw data buffer */
    uint32_t             stored_chksum;                          /* Stored metadata checksum value */
    unsigned             nclasses;                               /* Number of section classes */
    H5FS_t              *ret_value = NULL;                       /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(image);
    assert(udata);
    assert(udata->f);

    /* Allocate a new free space manager */
    if (NULL == (fspace = H5FS__new(udata->f, udata->nclasses, udata->classes, udata->cls_init_udata)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Set free space manager's internal information */
    fspace->addr = udata->addr;

    /* Magic number */
    if (memcmp(image, H5FS_HDR_MAGIC, (size_t)H5_SIZEOF_MAGIC) != 0)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTLOAD, NULL, "wrong free space header signature");
    image += H5_SIZEOF_MAGIC;

    /* Version */
    if (*image++ != H5FS_HDR_VERSION)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTLOAD, NULL, "wrong free space header version");

    /* Client ID */
    fspace->client = (H5FS_client_t)*image++;
    if (fspace->client >= H5FS_NUM_CLIENT_ID)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTLOAD, NULL, "unknown client ID in free space header");

    /* Total space tracked */
    H5F_DECODE_LENGTH(udata->f, image, fspace->tot_space);

    /* Total # of free space sections tracked */
    H5F_DECODE_LENGTH(udata->f, image, fspace->tot_sect_count);

    /* # of serializable free space sections tracked */
    H5F_DECODE_LENGTH(udata->f, image, fspace->serial_sect_count);

    /* # of ghost free space sections tracked */
    H5F_DECODE_LENGTH(udata->f, image, fspace->ghost_sect_count);

    /* # of section classes */
    /* (only check if we actually have some classes) */
    UINT16DECODE(image, nclasses);
    if (fspace->nclasses > 0 && nclasses > fspace->nclasses)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTLOAD, NULL, "section class count mismatch");

    /* Shrink percent */
    UINT16DECODE(image, fspace->shrink_percent);

    /* Expand percent */
    UINT16DECODE(image, fspace->expand_percent);

    /* Size of address space free space sections are within
     * (log2 of actual value)
     */
    UINT16DECODE(image, fspace->max_sect_addr);

    /* Max. size of section to track */
    H5F_DECODE_LENGTH(udata->f, image, fspace->max_sect_size);

    /* Address of serialized free space sections */
    H5F_addr_decode(udata->f, &image, &fspace->sect_addr);

    /* Size of serialized free space sections */
    H5F_DECODE_LENGTH(udata->f, image, fspace->sect_size);

    /* Allocated size of serialized free space sections */
    H5F_DECODE_LENGTH(udata->f, image, fspace->alloc_sect_size);

    /* checksum verification already done in verify_chksum cb */

    /* Metadata checksum */
    UINT32DECODE(image, stored_chksum);

    /* Sanity check */
    assert((size_t)(image - (const uint8_t *)_image) <= len);

    /* Set return value */
    ret_value = fspace;

done:
    /* Release resources */
    if (!ret_value && fspace)
        if (H5FS__hdr_dest(fspace) < 0)
            HDONE_ERROR(H5E_FSPACE, H5E_CANTFREE, NULL, "unable to destroy free space header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FS__cache_hdr_deserialize() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__cache_hdr_image_len
 *
 * Purpose:     Compute the size of the data structure on disk and return
 *              it in *image_len.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__cache_hdr_image_len(const void *_thing, size_t *image_len)
{
    const H5FS_t *fspace = (const H5FS_t *)_thing; /* Pointer to the object */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(fspace);
    assert(fspace->cache_info.type == H5AC_FSPACE_HDR);
    assert(image_len);

    /* Set the image length size */
    *image_len = fspace->hdr_size;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FS__cache_hdr_image_len() */

/*-------------------------------------------------------------------------
 * Function:	H5FS__cache_hdr_pre_serialize
 *
 * Purpose:	The free space manager header contains the address, size, and
 *		allocation size of the free space manager section info.  However,
 *		since it is possible for the section info to either not be allocated
 *		at all, or be allocated in temporary (AKA imaginary) files space,
 *		it is possible for the above mentioned fields to contain giberish
 *		when the free space manager header is serialized.
 *
 *		This function exists to prevent this problem.  It does so by
 *		forcing allocation of real file space for the section information.
 *
 *		Note that in the Version 2 cache, this problem was dealt with by
 *		simply flushing the section info before flushing the header.  This
 *		was possible, since the clients handled file I/O directly.  As
 *		this responsibility has moved to the cache in Version 3, this
 *		solution is no longer directly applicable.
 *
 * Return:	Success:	SUCCEED
 *		Failure:	FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__cache_hdr_pre_serialize(H5F_t *f, void *_thing, haddr_t addr, size_t H5_ATTR_UNUSED len,
                              haddr_t H5_ATTR_NDEBUG_UNUSED *new_addr, size_t H5_ATTR_NDEBUG_UNUSED *new_len,
                              unsigned *flags)
{
    H5FS_t     *fspace    = (H5FS_t *)_thing; /* Pointer to the object */
    H5AC_ring_t orig_ring = H5AC_RING_INV;    /* Original ring value */
    herr_t      ret_value = SUCCEED;          /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(f);
    assert(fspace);
    assert(fspace->cache_info.type == H5AC_FSPACE_HDR);
    assert(H5_addr_defined(addr));
    assert(new_addr);
    assert(new_len);
    assert(flags);

    if (fspace->sinfo) {
        H5AC_ring_t ring;

        /* Retrieve the ring type for the header */
        if (H5AC_get_entry_ring(f, addr, &ring) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTGET, FAIL, "unable to get property value");

        /* Set the ring type for the section info in the API context */
        H5AC_set_ring(ring, &orig_ring);

        /* This implies that the header "owns" the section info.
         *
         * Unfortunately, the comments in the code are not clear as to
         * what this means, but from reviewing the code (most particularly
         * H5FS_close(), H5FS_sinfo_lock, and H5FS_sinfo_unlock()), I
         * gather that it means that the header is maintaining a pointer to
         * an instance of H5FS_sinfo_t in which free space data is
         * maintained, and either:
         *
         * 1) The instance of H5FS_sinfo_t is not in the metadata cache.
         *
         *    This will be true iff H5_addr_defined(fspace->sect_addr)
         *    is false, and fspace->sinfo is not NULL.  This is sometimes
         *    referred to as "floating" section info in the comments.
         *
         *    If the section info structure contains free space data
         *    that must be placed on disk eventually, then
         *
         *        fspace->serial_sect_count > 0
         *
         *    and
         *
         *        H5_addr_defined(fspace->addr)
         *
         *    will both be true.  If this condition does not hold, then
         *    either the free space info is not persistent
         *    (!H5_addr_defined(fspace->addr)???) or the section info
         *    contains no free space data that must be written to file
         *    ( fspace->serial_sect_count == 0 ).
         *
         * 2) The instance of H5FS_sinfo_t is in the metadata cache with
         *    address in temporary file space (AKA imaginary file space).
         *    The entry may or may not be protected, and if protected, it
         *    may be protected either RW or RO (as indicated by
         *    fspace->sinfo_protected and  fspace->sinfo_accmod).
         *
         * 3) The instance of H5FS_sinfo_t is in the metadata cache with
         *    address in real file space.  As in case 2) above, the entry
         *    may or may not be protected, and if protected, it
         *    may be protected either RW or RO (as indicated by
         *    fspace->sinfo_protected and  fspace->sinfo_accmod).
         *
         * Observe that fspace->serial_sect_count > 0 must be true in
         * cases 2) and 3), as the section info should not be stored on
         * disk if it doesn't exist.  Similarly, since the section info
         * will not be stored to disk unless the header is,
         * H5_addr_defined(fspace->addr) must hold as well.
         *
         * As the objective is to touch up the free space manager header
         * so that it contains sensical data on the size and location of
         * the section information, we have to handle each of the above
         * cases differently.
         *
         * Case 1) If either fspace->serial_sect_count == 0 or
         *         ! H5_addr_defined(fspace->addr) do nothing as either
         *         the free space manager data is not persistent, or the
         *         section info is empty.
         *
         *         Otherwise, allocate space for the section info in real
         *         file space, insert the section info at this location, and
         *         set fspace->sect_addr, fspace->sect_size, and
         *         fspace->alloc_sect_size to reflect the new location
         *         of the section info.  Note that it is not necessary to
         *         force a write of the section info.
         *
         * Case 2) Allocate space for the section info in real file space,
         *         and tell the metadata cache to relocate the entry.
         *         Update fspace->sect_addr, fspace->sect_size, and
         *         fspace->alloc_sect_size to reflect the new location.
         *
         * Case 3) Nothing to be done in this case, although it is useful
         *         to perform sanity checks.
         *
         * Note that while we may alter the contents of the free space
         * header in cases 1) and 2), there is no need to mark the header
         * as dirty, as the metadata cache would not be attempting to
         * serialize the header if it thought it was clean.
         */
        if (fspace->serial_sect_count > 0 && H5_addr_defined(fspace->addr)) {
            /* Sanity check */
            assert(fspace->sect_size > 0);

            if (!H5_addr_defined(fspace->sect_addr)) { /* case 1 */
                haddr_t tag = HADDR_UNDEF;
                haddr_t sect_addr;
                hsize_t saved_sect_size, new_sect_size;

                /* allocate file space for the section info, and insert it
                 * into the metadata cache.
                 */
                saved_sect_size = fspace->sect_size;
                if (HADDR_UNDEF ==
                    (sect_addr = H5MF_alloc((H5F_t *)f, H5FD_MEM_FSPACE_SINFO, fspace->sect_size)))
                    HGOTO_ERROR(H5E_FSPACE, H5E_NOSPACE, FAIL,
                                "file allocation failed for free space sections");

                /* fspace->sect_size may change in size after H5MF_alloc().
                 * If increased in size, free the previous allocation and
                 * allocate again with the bigger fspace->sect_size.
                 */
                if (fspace->sect_size > saved_sect_size) {

                    new_sect_size = fspace->sect_size;

                    if (H5MF_xfree(f, H5FD_MEM_FSPACE_SINFO, sect_addr, saved_sect_size) < 0)
                        HGOTO_ERROR(H5E_FSPACE, H5E_CANTFREE, FAIL, "unable to free free space sections");

                    if (HADDR_UNDEF ==
                        (sect_addr = H5MF_alloc((H5F_t *)f, H5FD_MEM_FSPACE_SINFO, new_sect_size)))
                        HGOTO_ERROR(H5E_FSPACE, H5E_NOSPACE, FAIL,
                                    "file allocation failed for free space sections");
                    fspace->sect_size       = new_sect_size;
                    fspace->alloc_sect_size = new_sect_size;
                }
                else {
                    fspace->alloc_sect_size = saved_sect_size;
                    fspace->sect_size       = saved_sect_size;
                }
                fspace->sect_addr = sect_addr;

                /* Get the tag for this free space manager and use it to insert the entry */
                if (H5AC_get_tag((const void *)fspace, &tag) < 0)
                    HGOTO_ERROR(H5E_FSPACE, H5E_CANTTAG, FAIL, "can't get tag for metadata cache object");
                H5_BEGIN_TAG(tag)
                if (H5AC_insert_entry((H5F_t *)f, H5AC_FSPACE_SINFO, fspace->sect_addr, fspace->sinfo,
                                      H5AC__NO_FLAGS_SET) < 0)
                    HGOTO_ERROR_TAG(H5E_FSPACE, H5E_CANTINIT, FAIL, "can't add free space sections to cache");
                H5_END_TAG

                assert(fspace->sinfo->cache_info.size == fspace->alloc_sect_size);

                /* the metadata cache is now managing the section info,
                 * so set fspace->sinfo to NULL.
                 */
                fspace->sinfo = NULL;
            }                                                 /* end if */
            else if (H5F_IS_TMP_ADDR(f, fspace->sect_addr)) { /* case 2 */
                haddr_t new_sect_addr;

                /* move the section info from temporary (AKA imaginary) file
                 * space to real file space.
                 */

                /* if my reading of the code is correct, this should always
                 * be the case.  If not, we will have to add code to resize
                 * file space allocation for section info as well as moving it.
                 */
                assert(fspace->sect_size > 0);
                assert(fspace->alloc_sect_size == (size_t)fspace->sect_size);

                /* Allocate space for the section info in file */
                if (HADDR_UNDEF ==
                    (new_sect_addr = H5MF_alloc((H5F_t *)f, H5FD_MEM_FSPACE_SINFO, fspace->sect_size)))
                    HGOTO_ERROR(H5E_FSPACE, H5E_NOSPACE, FAIL,
                                "file allocation failed for free space sections");

                fspace->alloc_sect_size = (size_t)fspace->sect_size;
                assert(fspace->sinfo->cache_info.size == fspace->alloc_sect_size);

                /* Let the metadata cache know the section info moved */
                if (H5AC_move_entry((H5F_t *)f, H5AC_FSPACE_SINFO, fspace->sect_addr, new_sect_addr) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTMOVE, FAIL, "unable to move section info");

                fspace->sect_addr = new_sect_addr;
            }      /* end else-if */
            else { /* case 3 -- nothing to do but sanity checking */
                /* if my reading of the code is correct, this should always
                 * be the case.  If not, we will have to add code to resize
                 * file space allocation for section info.
                 */
                assert(fspace->sect_size > 0);
                assert(fspace->alloc_sect_size == (size_t)fspace->sect_size);
            } /* end else */
        }     /* end else */
        else {
            /* for one reason or another (see comment above) there should
             * not be any file space allocated for the section info.
             */
            assert(!H5_addr_defined(fspace->sect_addr));
        } /* end else */
    }     /* end if */
    else if (H5_addr_defined(fspace->sect_addr)) {
        /* Here the metadata cache is managing the section info.
         *
         * Do some sanity checks, and then test to see if the section
         * info is in real file space.  If it isn't relocate it into
         * real file space lest the header be written to file with
         * a nonsense section info address.
         */
        if (!H5F_POINT_OF_NO_RETURN(f)) {
            assert(fspace->sect_size > 0);
            assert(fspace->alloc_sect_size == (size_t)fspace->sect_size);
        } /* end if */

        if (H5F_IS_TMP_ADDR(f, fspace->sect_addr)) {
            unsigned sect_status = 0;
            haddr_t  new_sect_addr;

            /* we have work to do -- must relocate section info into
             * real file space.
             *
             * Since the section info address is in temporary space (AKA
             * imaginary space), it follows that the entry must be in
             * cache.  Further, since fspace->sinfo is NULL, it must be
             * unprotected and un-pinned.  Start by verifying this.
             */
            if (H5AC_get_entry_status(f, fspace->sect_addr, &sect_status) < 0)
                HGOTO_ERROR(H5E_FSPACE, H5E_CANTGET, FAIL, "can't get section info status");

            assert(sect_status & H5AC_ES__IN_CACHE);
            assert((sect_status & H5AC_ES__IS_PROTECTED) == 0);
            assert((sect_status & H5AC_ES__IS_PINNED) == 0);

            /* Allocate space for the section info in file */
            if (HADDR_UNDEF ==
                (new_sect_addr = H5MF_alloc((H5F_t *)f, H5FD_MEM_FSPACE_SINFO, fspace->sect_size)))
                HGOTO_ERROR(H5E_FSPACE, H5E_NOSPACE, FAIL, "file allocation failed for free space sections");

            fspace->alloc_sect_size = (size_t)fspace->sect_size;

            /* Sanity check */
            assert(!H5_addr_eq(fspace->sect_addr, new_sect_addr));

            /* Let the metadata cache know the section info moved */
            if (H5AC_move_entry((H5F_t *)f, H5AC_FSPACE_SINFO, fspace->sect_addr, new_sect_addr) < 0)
                HGOTO_ERROR(H5E_FSPACE, H5E_CANTMOVE, FAIL, "unable to move section info");

            /* Update the internal address for the section info */
            fspace->sect_addr = new_sect_addr;

            /* No need to mark the header dirty, as we are about to
             * serialize it.
             */
        }  /* end if */
    }      /* end else-if */
    else { /* there is no section info at present */
        /* do some sanity checks */
        assert(fspace->serial_sect_count == 0);
        assert(fspace->tot_sect_count == fspace->ghost_sect_count);
    } /* end else */

    /* what ever happened above, set *flags to 0 */
    *flags = 0;

done:
    /* Reset the ring in the API context */
    if (orig_ring != H5AC_RING_INV)
        H5AC_set_ring(orig_ring, NULL);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FS__cache_hdr_pre_serialize() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__cache_hdr_serialize
 *
 * Purpose: 	Given an instance of H5FS_t and a suitably sized buffer,
 *      	serialize the contents of the instance of H5FS_t and write
 *      	its contents to the buffer.  This buffer will be used to
 *      	write the image of the instance to file.
 *
 * Return:      Success:        SUCCEED
 *              Failure:        FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__cache_hdr_serialize(const H5F_t *f, void *_image, size_t H5_ATTR_NDEBUG_UNUSED len, void *_thing)
{
    H5FS_t  *fspace = (H5FS_t *)_thing;  /* Pointer to the object */
    uint8_t *image  = (uint8_t *)_image; /* Pointer into raw data buffer */
    uint32_t metadata_chksum;            /* Computed metadata checksum value */
    herr_t   ret_value = SUCCEED;        /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(f);
    assert(image);
    assert(fspace);
    assert(fspace->cache_info.type == H5AC_FSPACE_HDR);
    assert(fspace->hdr_size == len);

    /* The section information does not always exits, and if it does,
     * it is not always in the cache.  To make matters more interesting,
     * even if it is in the cache, it may not be in real file space.
     *
     * The pre-serialize function should have moved the section info
     * into real file space if necessary before this function was called.
     * The following asserts are a cursory check on this.
     */
    assert((!H5_addr_defined(fspace->sect_addr)) || (!H5F_IS_TMP_ADDR(f, fspace->sect_addr)));

    if (!H5F_POINT_OF_NO_RETURN(f))
        assert((!H5_addr_defined(fspace->sect_addr)) ||
               ((fspace->sect_size > 0) && (fspace->alloc_sect_size == (size_t)fspace->sect_size)));

    /* Magic number */
    H5MM_memcpy(image, H5FS_HDR_MAGIC, (size_t)H5_SIZEOF_MAGIC);
    image += H5_SIZEOF_MAGIC;

    /* Version # */
    *image++ = H5FS_HDR_VERSION;

    /* Client ID */
    H5_CHECKED_ASSIGN(*image++, uint8_t, fspace->client, int);

    /* Total space tracked */
    H5F_ENCODE_LENGTH(f, image, fspace->tot_space);

    /* Total # of free space sections tracked */
    H5F_ENCODE_LENGTH(f, image, fspace->tot_sect_count);

    /* # of serializable free space sections tracked */
    H5F_ENCODE_LENGTH(f, image, fspace->serial_sect_count);

    /* # of ghost free space sections tracked */
    H5F_ENCODE_LENGTH(f, image, fspace->ghost_sect_count);

    /* # of section classes */
    UINT16ENCODE(image, fspace->nclasses);

    /* Shrink percent */
    UINT16ENCODE(image, fspace->shrink_percent);

    /* Expand percent */
    UINT16ENCODE(image, fspace->expand_percent);

    /* Size of address space free space sections are within (log2 of
     * actual value)
     */
    UINT16ENCODE(image, fspace->max_sect_addr);

    /* Max. size of section to track */
    H5F_ENCODE_LENGTH(f, image, fspace->max_sect_size);

    /* Address of serialized free space sections */
    H5F_addr_encode(f, &image, fspace->sect_addr);

    /* Size of serialized free space sections */
    H5F_ENCODE_LENGTH(f, image, fspace->sect_size);

    /* Allocated size of serialized free space sections */
    H5F_ENCODE_LENGTH(f, image, fspace->alloc_sect_size);

    /* Compute checksum */
    metadata_chksum = H5_checksum_metadata((uint8_t *)_image, (size_t)(image - (uint8_t *)_image), 0);

    /* Metadata checksum */
    UINT32ENCODE(image, metadata_chksum);

    /* sanity checks */
    assert((size_t)(image - (uint8_t *)_image) == fspace->hdr_size);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS__cache_hdr_serialize() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__cache_hdr_notify
 *
 * Purpose:     Handle cache action notifications
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FS__cache_hdr_notify(H5AC_notify_action_t action, void *_thing)
{
    H5FS_t *fspace    = (H5FS_t *)_thing; /* Pointer to the object */
    herr_t  ret_value = SUCCEED;          /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Sanity check */
    assert(fspace);

    /* Determine which action to take */
    switch (action) {
        case H5AC_NOTIFY_ACTION_AFTER_INSERT:
        case H5AC_NOTIFY_ACTION_AFTER_LOAD:
        case H5AC_NOTIFY_ACTION_AFTER_FLUSH:
            /* do nothing */
            break;

        case H5AC_NOTIFY_ACTION_ENTRY_DIRTIED:
            if (H5AC_unsettle_entry_ring(fspace) < 0)
                HGOTO_ERROR(H5E_FSPACE, H5E_CANTFLUSH, FAIL, "unable to mark FSM ring as unsettled");
            break;

        case H5AC_NOTIFY_ACTION_ENTRY_CLEANED:
        case H5AC_NOTIFY_ACTION_CHILD_DIRTIED:
        case H5AC_NOTIFY_ACTION_CHILD_CLEANED:
        case H5AC_NOTIFY_ACTION_CHILD_UNSERIALIZED:
        case H5AC_NOTIFY_ACTION_CHILD_SERIALIZED:
        case H5AC_NOTIFY_ACTION_BEFORE_EVICT:
            /* do nothing */
            break;

        default:
#ifdef NDEBUG
            HGOTO_ERROR(H5E_FSPACE, H5E_BADVALUE, FAIL, "unknown action from metadata cache");
#else  /* NDEBUG */
            assert(0 && "Unknown action?!?");
#endif /* NDEBUG */
    }  /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FS__cache_hdr_notify() */

/*-------------------------------------------------------------------------
 * Function:	H5FS__cache_hdr_free_icr
 *
 * Purpose:	Destroys a free space header in memory.
 *
 * Return:	Success:        SUCCEED
 *              Failure:        FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__cache_hdr_free_icr(void *_thing)
{
    H5FS_t *fspace    = (H5FS_t *)_thing; /* Pointer to the object */
    herr_t  ret_value = SUCCEED;          /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(fspace);
    assert(fspace->cache_info.type == H5AC_FSPACE_HDR);

    /* We should not still be holding on to the free space section info */
    assert(!fspace->sinfo);

    /* Destroy free space header */
    if (H5FS__hdr_dest(fspace) < 0)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTFREE, FAIL, "unable to destroy free space header");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FS__cache_hdr_free_icr() */

/*-------------------------------------------------------------------------
 * Function:	H5FS__cache_sinfo_get_initial_load_size()
 *
 * Purpose:	Compute the size of the on disk image of the free space
 *		manager section info, and place this value in *image_len.
 *
 * Return:	Success:	SUCCEED
 *		Failure:	FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__cache_sinfo_get_initial_load_size(void *_udata, size_t *image_len)
{
    const H5FS_t          *fspace;                                  /* free space manager */
    H5FS_sinfo_cache_ud_t *udata = (H5FS_sinfo_cache_ud_t *)_udata; /* User data for callback */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(udata);
    fspace = udata->fspace;
    assert(fspace);
    assert(fspace->sect_size > 0);
    assert(image_len);

    /* Set the image length size */
    *image_len = (size_t)(fspace->sect_size);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FS__cache_sinfo_get_initial_load_size() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__cache_sinfo_verify_chksum
 *
 * Purpose:     Verify the computed checksum of the data structure is the
 *              same as the stored chksum.
 *
 * Return:      Success:        true/false
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5FS__cache_sinfo_verify_chksum(const void *_image, size_t len, void H5_ATTR_UNUSED *_udata)
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
} /* end H5FS__cache_sinfo_verify_chksum() */

/*-------------------------------------------------------------------------
 * Function:	H5FS__cache_sinfo_deserialize
 *
 * Purpose:	Given a buffer containing the on disk image of the free space
 *		manager section info, allocate an instance of H5FS_sinfo_t, load
 *		it with the data contained in the image, and return a pointer to
 *		the new instance.
 *
 * Return:	Success:	Pointer to in core representation
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5FS__cache_sinfo_deserialize(const void *_image, size_t H5_ATTR_NDEBUG_UNUSED len, void *_udata,
                              bool H5_ATTR_NDEBUG_UNUSED *dirty)
{
    H5FS_sinfo_cache_ud_t *udata = (H5FS_sinfo_cache_ud_t *)_udata; /* User data for callback */
    H5FS_t                *fspace;                                  /* free space manager */
    H5FS_sinfo_t          *sinfo = NULL;                            /* Free space section info */
    haddr_t                fs_addr;                                 /* Free space header address */
    size_t                 old_sect_size;                           /* Old section size */
    const uint8_t         *image = (const uint8_t *)_image;         /* Pointer into raw data buffer */
    const uint8_t         *chksum_image;                            /* Points to chksum location */
    uint32_t               stored_chksum;                           /* Stored metadata checksum  */
    void                  *ret_value = NULL;                        /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(image);
    assert(udata);
    fspace = udata->fspace;
    assert(fspace);
    assert(fspace->sect_size == len);
    assert(dirty);

    /* Allocate a new free space section info */
    if (NULL == (sinfo = H5FS__sinfo_new(udata->f, fspace)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* initialize old_sect_size */
    H5_CHECKED_ASSIGN(old_sect_size, size_t, fspace->sect_size, hsize_t);

    /* Magic number */
    if (memcmp(image, H5FS_SINFO_MAGIC, (size_t)H5_SIZEOF_MAGIC) != 0)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTLOAD, NULL, "wrong free space sections signature");
    image += H5_SIZEOF_MAGIC;

    /* Version */
    if (*image++ != H5FS_SINFO_VERSION)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTLOAD, NULL, "wrong free space sections version");

    /* Address of free space header for these sections */
    H5F_addr_decode(udata->f, &image, &fs_addr);
    if (H5_addr_ne(fs_addr, fspace->addr))
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTLOAD, NULL, "incorrect header address for free space sections");

    /* Check for any serialized sections */
    if (fspace->serial_sect_count > 0) {
        hsize_t old_tot_sect_count; /* Total section count from header */
        hsize_t H5_ATTR_NDEBUG_UNUSED
            old_serial_sect_count;                          /* Total serializable section count from header */
        hsize_t H5_ATTR_NDEBUG_UNUSED old_ghost_sect_count; /* Total ghost section count from header */
        hsize_t H5_ATTR_NDEBUG_UNUSED old_tot_space;        /* Total space managed from header */
        unsigned                      sect_cnt_size;        /* The size of the section size counts */

        /* Compute the size of the section counts */
        sect_cnt_size = H5VM_limit_enc_size((uint64_t)fspace->serial_sect_count);

        /* Reset the section count, the "add" routine will update it */
        old_tot_sect_count        = fspace->tot_sect_count;
        old_serial_sect_count     = fspace->serial_sect_count;
        old_ghost_sect_count      = fspace->ghost_sect_count;
        old_tot_space             = fspace->tot_space;
        fspace->tot_sect_count    = 0;
        fspace->serial_sect_count = 0;
        fspace->ghost_sect_count  = 0;
        fspace->tot_space         = 0;

        /* Walk through the image, deserializing sections */
        do {
            hsize_t sect_size  = 0; /* Current section size */
            size_t  node_count = 0; /* # of sections of this size */
            size_t  u;              /* Local index variable */

            /* The number of sections of this node's size */
            UINT64DECODE_VAR(image, node_count, sect_cnt_size);
            assert(node_count);

            /* The size of the sections for this node */
            UINT64DECODE_VAR(image, sect_size, sinfo->sect_len_size);
            assert(sect_size);

            /* Loop over nodes of this size */
            for (u = 0; u < node_count; u++) {
                H5FS_section_info_t *new_sect;      /* Section that was deserialized */
                haddr_t              sect_addr = 0; /* Address of free space section in the address space */
                unsigned             sect_type;     /* Type of free space section */
                unsigned             des_flags;     /* Flags from deserialize callback */

                /* The address of the section */
                UINT64DECODE_VAR(image, sect_addr, sinfo->sect_off_size);

                /* The type of this section */
                sect_type = *image++;

                /* Call 'deserialize' callback for this section */
                des_flags = 0;
                assert(fspace->sect_cls[sect_type].deserialize);
                if (NULL == (new_sect = (*fspace->sect_cls[sect_type].deserialize)(
                                 &fspace->sect_cls[sect_type], image, sect_addr, sect_size, &des_flags)))
                    HGOTO_ERROR(H5E_FSPACE, H5E_CANTDECODE, NULL, "can't deserialize section");

                /* Update offset in serialization image */
                image += fspace->sect_cls[sect_type].serial_size;

                /* Insert section in free space manager, unless requested not to */
                if (!(des_flags & H5FS_DESERIALIZE_NO_ADD))
                    if (H5FS_sect_add(udata->f, fspace, new_sect, H5FS_ADD_DESERIALIZING, udata) < 0)
                        HGOTO_ERROR(H5E_FSPACE, H5E_CANTINSERT, NULL,
                                    "can't add section to free space manager");
            } /* end for */

            if (fspace->tot_sect_count == old_tot_sect_count)
                break;

        } while (image < (((const uint8_t *)_image + old_sect_size) - H5FS_SIZEOF_CHKSUM));

        /* Sanity check */
        assert((size_t)(image - (const uint8_t *)_image) <= (old_sect_size - H5FS_SIZEOF_CHKSUM));
        assert(old_sect_size == fspace->sect_size);
        assert(old_tot_sect_count == fspace->tot_sect_count);
        assert(old_serial_sect_count == fspace->serial_sect_count);
        assert(old_ghost_sect_count == fspace->ghost_sect_count);
        assert(old_tot_space == fspace->tot_space);
    } /* end if */

    /* checksum verification already done in verify_chksum cb */

    /* There may be empty space between entries and chksum */
    chksum_image = (const uint8_t *)(_image) + old_sect_size - H5FS_SIZEOF_CHKSUM;
    /* Metadata checksum */
    UINT32DECODE(chksum_image, stored_chksum);

    /* Sanity check */
    assert((image == chksum_image) ||
           ((size_t)((image - (const uint8_t *)_image) + (chksum_image - image)) == old_sect_size));

    /* Set return value */
    ret_value = sinfo;

done:
    if (!ret_value && sinfo)
        if (H5FS__sinfo_dest(sinfo) < 0)
            HDONE_ERROR(H5E_FSPACE, H5E_CANTFREE, NULL, "unable to destroy free space info");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FS__cache_sinfo_deserialize() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__cache_sinfo_image_len
 *
 * Purpose:     Compute the size of the data structure on disk and return
 *              it in *image_len.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__cache_sinfo_image_len(const void *_thing, size_t *image_len)
{
    const H5FS_sinfo_t *sinfo = (const H5FS_sinfo_t *)_thing; /* Pointer to the object */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(sinfo);
    assert(sinfo->cache_info.type == H5AC_FSPACE_SINFO);
    assert(sinfo->fspace);
    assert(sinfo->fspace->cache_info.type == H5AC_FSPACE_HDR);
    assert(image_len);

    /* Set the image length size */
    H5_CHECKED_ASSIGN(*image_len, size_t, sinfo->fspace->alloc_sect_size, hsize_t);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FS__cache_sinfo_image_len() */

/*-------------------------------------------------------------------------
 * Function:	H5FS__cache_sinfo_pre_serialize
 *
 * Purpose:	The objective of this function is to test to see if file space
 *		for the section info is located in temporary (AKA imaginary) file
 *		space.  If it is, relocate file space for the section info to
 *		regular file space.
 *
 * Return:	Success:	SUCCEED
 *		Failure:	FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__cache_sinfo_pre_serialize(H5F_t *f, void *_thing, haddr_t addr, size_t H5_ATTR_NDEBUG_UNUSED len,
                                haddr_t *new_addr, size_t H5_ATTR_NDEBUG_UNUSED *new_len, unsigned *flags)
{
    H5FS_sinfo_t *sinfo = (H5FS_sinfo_t *)_thing; /* Pointer to the object */
    H5FS_t       *fspace;                         /* Free space header */
    haddr_t       sinfo_addr;                     /* Address for section info */
    herr_t        ret_value = SUCCEED;            /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(sinfo);
    assert(sinfo->cache_info.type == H5AC_FSPACE_SINFO);
    fspace = sinfo->fspace;
    assert(fspace->cache_info.type == H5AC_FSPACE_HDR);
    assert(fspace->cache_info.is_pinned);
    assert(H5_addr_defined(addr));
    assert(H5_addr_eq(fspace->sect_addr, addr));
    assert(fspace->sect_size == len);
    assert(new_addr);
    assert(new_len);
    assert(flags);

    sinfo_addr = addr; /* this will change if we relocate the section data */

    /* Check for section info at temporary address */
    if (H5F_IS_TMP_ADDR(f, fspace->sect_addr)) {
        /* Sanity check */
        assert(fspace->sect_size > 0);
        assert(H5_addr_eq(fspace->sect_addr, addr));

        /* Allocate space for the section info in file */
        if (HADDR_UNDEF == (sinfo_addr = H5MF_alloc((H5F_t *)f, H5FD_MEM_FSPACE_SINFO, fspace->sect_size)))
            HGOTO_ERROR(H5E_FSPACE, H5E_NOSPACE, FAIL, "file allocation failed for free space sections");

        fspace->alloc_sect_size = (size_t)fspace->sect_size;

        /* Sanity check */
        assert(!H5_addr_eq(sinfo->fspace->sect_addr, sinfo_addr));

        /* Let the metadata cache know the section info moved */
        if (H5AC_move_entry((H5F_t *)f, H5AC_FSPACE_SINFO, sinfo->fspace->sect_addr, sinfo_addr) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTMOVE, FAIL, "unable to move section info");

        /* Update the internal address for the section info */
        sinfo->fspace->sect_addr = sinfo_addr;

        /* Mark free space header as dirty */
        if (H5AC_mark_entry_dirty(fspace) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTMARKDIRTY, FAIL, "unable to mark free space header as dirty");
    } /* end if */

    if (!H5_addr_eq(addr, sinfo_addr)) {
        *new_addr = sinfo_addr;
        *flags    = H5C__SERIALIZE_MOVED_FLAG;
    } /* end if */
    else
        *flags = 0;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FS__cache_sinfo_pre_serialize() */

/*-------------------------------------------------------------------------
 * Function:	H5FS__cache_sinfo_serialize
 *
 * Purpose:	Given an instance of H5FS_sinfo_t and a suitably sized buffer,
 *		serialize the contents of the instance of H5FS_sinfo_t and write
 *		its contents to the buffer.  This buffer will be used to write
 *		the image of the instance to file.
 *
 * Return:	Success:	SUCCEED
 *		Failure:	FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__cache_sinfo_serialize(const H5F_t *f, void *_image, size_t len, void *_thing)
{
    H5FS_sinfo_t  *sinfo = (H5FS_sinfo_t *)_thing; /* Pointer to the object */
    H5FS_iter_ud_t udata;                          /* User data for callbacks */
    ptrdiff_t      gap_size;
    uint8_t       *image        = (uint8_t *)_image; /* Pointer into raw data buffer */
    uint8_t       *chksum_image = NULL;              /* Points to chksum location */
    uint32_t       metadata_chksum;                  /* Computed metadata checksum value */
    unsigned       bin;                              /* Current bin we are on */
    herr_t         ret_value = SUCCEED;              /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(image);
    assert(sinfo);
    assert(sinfo->cache_info.type == H5AC_FSPACE_SINFO);
    assert(sinfo->fspace->cache_info.type == H5AC_FSPACE_HDR);
    assert(sinfo->fspace->cache_info.is_pinned);
    assert(sinfo->fspace->sect_size == len);
    assert(sinfo->fspace->sect_cls);

    /* Magic number */
    H5MM_memcpy(image, H5FS_SINFO_MAGIC, (size_t)H5_SIZEOF_MAGIC);
    image += H5_SIZEOF_MAGIC;

    /* Version # */
    *image++ = H5FS_SINFO_VERSION;

    /* Address of free space header for these sections */
    H5F_addr_encode(f, &image, sinfo->fspace->addr);

    /* Set up user data for iterator */
    udata.sinfo         = sinfo;
    udata.image         = &image;
    udata.sect_cnt_size = H5VM_limit_enc_size((uint64_t)sinfo->fspace->serial_sect_count);

    /* Iterate over all the bins */
    for (bin = 0; bin < sinfo->nbins; bin++)
        /* Check if there are any sections in this bin */
        if (sinfo->bins[bin].bin_list)
            /* Iterate over list of section size nodes for bin */
            if (H5SL_iterate(sinfo->bins[bin].bin_list, H5FS__sinfo_serialize_node_cb, &udata) < 0)
                HGOTO_ERROR(H5E_FSPACE, H5E_BADITER, FAIL, "can't iterate over section size nodes");

    /* Compute checksum */

    /* There may be empty space between entries and chksum */
    chksum_image = (uint8_t *)(_image) + len - H5FS_SIZEOF_CHKSUM;

    /*
     * If there is any empty space between the entries and
     * checksum, make sure that the space is initialized
     * before serializing it
     */
    gap_size = chksum_image - image;
    if (gap_size > 0)
        memset(image, 0, (size_t)gap_size);

    metadata_chksum = H5_checksum_metadata(_image, (size_t)(chksum_image - (uint8_t *)_image), 0);
    /* Metadata checksum */
    UINT32ENCODE(chksum_image, metadata_chksum);

    /* Sanity check */
    assert((chksum_image == image) ||
           ((size_t)((image - (uint8_t *)_image) + (chksum_image - image)) == sinfo->fspace->sect_size));
    assert(sinfo->fspace->sect_size <= sinfo->fspace->alloc_sect_size);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FS__cache_sinfo_serialize() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__cache_sinfo_notify
 *
 * Purpose:     Handle cache action notifications
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FS__cache_sinfo_notify(H5AC_notify_action_t action, void *_thing)
{
    H5FS_sinfo_t *sinfo     = (H5FS_sinfo_t *)_thing;
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(sinfo);

    /* Check if the file was opened with SWMR-write access */
    if (sinfo->fspace->swmr_write) {
        /* Determine which action to take */
        switch (action) {
            case H5AC_NOTIFY_ACTION_AFTER_INSERT:
            case H5AC_NOTIFY_ACTION_AFTER_LOAD:
                /* Create flush dependency on parent */
                if (H5FS__create_flush_depend((H5AC_info_t *)sinfo->fspace, (H5AC_info_t *)sinfo) < 0)
                    HGOTO_ERROR(
                        H5E_FSPACE, H5E_CANTDEPEND, FAIL,
                        "unable to create flush dependency between data block and header, address = %llu",
                        (unsigned long long)sinfo->fspace->sect_addr);
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
                /* Destroy flush dependency on parent */
                if (H5FS__destroy_flush_depend((H5AC_info_t *)sinfo->fspace, (H5AC_info_t *)sinfo) < 0)
                    HGOTO_ERROR(H5E_FSPACE, H5E_CANTUNDEPEND, FAIL, "unable to destroy flush dependency");
                break;

            default:
#ifdef NDEBUG
                HGOTO_ERROR(H5E_FSPACE, H5E_BADVALUE, FAIL, "unknown action from metadata cache");
#else     /* NDEBUG */
                assert(0 && "Unknown action?!?");
#endif    /* NDEBUG */
        } /* end switch */
    }     /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FS__cache_sinfo_notify() */

/*-------------------------------------------------------------------------
 * Function:	H5FS__cache_sinfo_free_icr
 *
 * Purpose:	Free the memory used for the in core representation of the
 *		free space manager section info.
 *
 * Return:	Success:	SUCCEED
 *		Failure:	FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__cache_sinfo_free_icr(void *_thing)
{
    H5FS_sinfo_t *sinfo     = (H5FS_sinfo_t *)_thing; /* Pointer to the object */
    herr_t        ret_value = SUCCEED;                /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(sinfo);
    assert(sinfo->cache_info.type == H5AC_FSPACE_SINFO);
    assert(sinfo->fspace->cache_info.type == H5AC_FSPACE_HDR);
    assert(sinfo->fspace->cache_info.is_pinned);

    /* Destroy free space info */
    if (H5FS__sinfo_dest(sinfo) < 0)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTFREE, FAIL, "unable to destroy free space info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FS__cache_sinfo_free_icr() */

/*-------------------------------------------------------------------------
 * Function:	H5FS__sinfo_serialize_sect_cb
 *
 * Purpose:	Skip list iterator callback to serialize free space sections
 *              of a particular size
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__sinfo_serialize_sect_cb(void *_item, void H5_ATTR_UNUSED *key, void *_udata)
{
    H5FS_section_class_t *sect_cls;                                 /* Class of section */
    H5FS_section_info_t  *sect      = (H5FS_section_info_t *)_item; /* Free space section to work on */
    H5FS_iter_ud_t       *udata     = (H5FS_iter_ud_t *)_udata;     /* Callback info */
    herr_t                ret_value = SUCCEED;                      /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(sect);
    assert(udata->sinfo);
    assert(udata->image);

    /* Get section's class */
    sect_cls = &udata->sinfo->fspace->sect_cls[sect->type];

    /* Check if this section should be serialized (i.e. is not a ghost section) */
    if (!(sect_cls->flags & H5FS_CLS_GHOST_OBJ)) {
        /* The address of the section */
        UINT64ENCODE_VAR(*udata->image, sect->addr, udata->sinfo->sect_off_size);

        /* The type of this section */
        *(*udata->image)++ = (uint8_t)sect->type;

        /* Call 'serialize' callback for this section */
        if (sect_cls->serialize) {
            if ((*sect_cls->serialize)(sect_cls, sect, *udata->image) < 0)
                HGOTO_ERROR(H5E_FSPACE, H5E_CANTSERIALIZE, FAIL, "can't synchronize section");

            /* Update offset in serialization buffer */
            (*udata->image) += sect_cls->serial_size;
        } /* end if */
        else
            assert(sect_cls->serial_size == 0);
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS__sinfo_serialize_sect_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5FS__sinfo_serialize_node_cb
 *
 * Purpose:	Skip list iterator callback to serialize free space sections
 *              in a bin
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__sinfo_serialize_node_cb(void *_item, void H5_ATTR_UNUSED *key, void *_udata)
{
    H5FS_node_t    *fspace_node = (H5FS_node_t *)_item;     /* Free space size node to work on */
    H5FS_iter_ud_t *udata       = (H5FS_iter_ud_t *)_udata; /* Callback info */
    herr_t          ret_value   = SUCCEED;                  /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(fspace_node);
    assert(udata->sinfo);
    assert(udata->image);

    /* Check if this node has any serializable sections */
    if (fspace_node->serial_count > 0) {
        /* The number of serializable sections of this node's size */
        UINT64ENCODE_VAR(*udata->image, fspace_node->serial_count, udata->sect_cnt_size);

        /* The size of the sections for this node */
        UINT64ENCODE_VAR(*udata->image, fspace_node->sect_size, udata->sinfo->sect_len_size);

        /* Iterate through all the sections of this size */
        assert(fspace_node->sect_list);
        if (H5SL_iterate(fspace_node->sect_list, H5FS__sinfo_serialize_sect_cb, udata) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_BADITER, FAIL, "can't iterate over section nodes");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS__sinfo_serialize_node_cb() */
