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
 * Created:		H5Ocache.c
 *
 * Purpose:		Object header metadata cache virtual functions
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Omodule.h" /* This source code file is part of the H5O module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions           */
#include "H5Eprivate.h"  /* Error handling              */
#include "H5FLprivate.h" /* Free lists                  */
#include "H5MFprivate.h" /* File memory management      */
#include "H5MMprivate.h" /* Memory management           */
#include "H5Opkg.h"      /* Object headers              */
#include "H5WBprivate.h" /* Wrapped Buffers             */

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

/* Metadata cache callbacks */
static herr_t H5O__cache_get_initial_load_size(void *udata, size_t *image_len);
static herr_t H5O__cache_get_final_load_size(const void *image_ptr, size_t image_len, void *udata,
                                             size_t *actual_len);
static htri_t H5O__cache_verify_chksum(const void *image_ptr, size_t len, void *udata_ptr);
static void  *H5O__cache_deserialize(const void *image, size_t len, void *udata, bool *dirty);
static herr_t H5O__cache_image_len(const void *thing, size_t *image_len);
static herr_t H5O__cache_serialize(const H5F_t *f, void *image, size_t len, void *thing);
static herr_t H5O__cache_notify(H5AC_notify_action_t action, void *_thing);
static herr_t H5O__cache_free_icr(void *thing);

static herr_t H5O__cache_chk_get_initial_load_size(void *udata, size_t *image_len);
static htri_t H5O__cache_chk_verify_chksum(const void *image_ptr, size_t len, void *udata_ptr);
static void  *H5O__cache_chk_deserialize(const void *image, size_t len, void *udata, bool *dirty);
static herr_t H5O__cache_chk_image_len(const void *thing, size_t *image_len);
static herr_t H5O__cache_chk_serialize(const H5F_t *f, void *image, size_t len, void *thing);
static herr_t H5O__cache_chk_notify(H5AC_notify_action_t action, void *_thing);
static herr_t H5O__cache_chk_free_icr(void *thing);

/* Prefix routines */
static herr_t H5O__prefix_deserialize(const uint8_t *image, size_t len, H5O_cache_ud_t *udata);

/* Chunk routines */
static herr_t H5O__chunk_deserialize(H5O_t *oh, haddr_t addr, size_t chunk_size, const uint8_t *image,
                                     size_t len, H5O_common_cache_ud_t *udata, bool *dirty);
static herr_t H5O__chunk_serialize(const H5F_t *f, H5O_t *oh, unsigned chunkno);

/* Misc. routines */
static herr_t H5O__add_cont_msg(H5O_cont_msgs_t *cont_msg_info, const H5O_cont_t *cont);

/*********************/
/* Package Variables */
/*********************/

/* H5O object header prefix inherits cache-like properties from H5AC */
const H5AC_class_t H5AC_OHDR[1] = {{
    H5AC_OHDR_ID,                      /* Metadata client ID */
    "object header",                   /* Metadata client name (for debugging) */
    H5FD_MEM_OHDR,                     /* File space memory type for client */
    H5AC__CLASS_SPECULATIVE_LOAD_FLAG, /* Client class behavior flags */
    H5O__cache_get_initial_load_size,  /* 'get_initial_load_size' callback */
    H5O__cache_get_final_load_size,    /* 'get_final_load_size' callback */
    H5O__cache_verify_chksum,          /* 'verify_chksum' callback */
    H5O__cache_deserialize,            /* 'deserialize' callback */
    H5O__cache_image_len,              /* 'image_len' callback */
    NULL,                              /* 'pre_serialize' callback */
    H5O__cache_serialize,              /* 'serialize' callback */
    H5O__cache_notify,                 /* 'notify' callback */
    H5O__cache_free_icr,               /* 'free_icr' callback */
    NULL,                              /* 'fsf_size' callback */
}};

/* H5O object header chunk inherits cache-like properties from H5AC */
const H5AC_class_t H5AC_OHDR_CHK[1] = {{
    H5AC_OHDR_CHK_ID,                     /* Metadata client ID */
    "object header continuation chunk",   /* Metadata client name (for debugging) */
    H5FD_MEM_OHDR,                        /* File space memory type for client */
    H5AC__CLASS_NO_FLAGS_SET,             /* Client class behavior flags */
    H5O__cache_chk_get_initial_load_size, /* 'get_initial_load_size' callback */
    NULL,                                 /* 'get_final_load_size' callback */
    H5O__cache_chk_verify_chksum,         /* 'verify_chksum' callback */
    H5O__cache_chk_deserialize,           /* 'deserialize' callback */
    H5O__cache_chk_image_len,             /* 'image_len' callback */
    NULL,                                 /* 'pre_serialize' callback */
    H5O__cache_chk_serialize,             /* 'serialize' callback */
    H5O__cache_chk_notify,                /* 'notify' callback */
    H5O__cache_chk_free_icr,              /* 'free_icr' callback */
    NULL,                                 /* 'fsf_size' callback */
}};

/* Declare external the free list for H5O_unknown_t's */
H5FL_EXTERN(H5O_unknown_t);

/* Declare extern the free list for H5O_chunk_proxy_t's */
H5FL_EXTERN(H5O_chunk_proxy_t);

/* Declare the free list for H5O_cont_t sequences */
H5FL_SEQ_DEFINE(H5O_cont_t);

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:    H5O__cache_get_initial_load_size()
 *
 * Purpose:     Tell the metadata cache how much data to read from file in
 *              the first speculative read for the object header.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__cache_get_initial_load_size(void H5_ATTR_UNUSED *_udata, size_t *image_len)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(image_len);

    /* Set the image length size */
    *image_len = H5O_SPEC_READ_SIZE;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__cache_get_initial_load_size() */

/*-------------------------------------------------------------------------
 * Function:    H5O__cache_get_final_load_size()
 *
 * Purpose:     Tell the metadata cache the final size of an object header.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__cache_get_final_load_size(const void *image, size_t image_len, void *_udata, size_t *actual_len)
{
    H5O_cache_ud_t *udata     = (H5O_cache_ud_t *)_udata; /* User data for callback */
    herr_t          ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(image);
    assert(udata);
    assert(actual_len);
    assert(*actual_len == image_len);

    /* Deserialize the object header prefix */
    if (H5O__prefix_deserialize((const uint8_t *)image, image_len, udata) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTDECODE, FAIL, "can't deserialize object header prefix");

    /* Sanity check */
    assert(udata->oh);

    /* Set the final size for the cache image */
    *actual_len = udata->chunk0_size + (size_t)H5O_SIZEOF_HDR(udata->oh);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__cache_get_final_load_size() */

/*-------------------------------------------------------------------------
 * Function:    H5O__cache_verify_chksum
 *
 * Purpose:     Verify the computed checksum of the data structure is the
 *              same as the stored chksum.
 *
 * Return:      Success:        true/false
 *              Failure:        Negative
 *-------------------------------------------------------------------------
 */
static htri_t
H5O__cache_verify_chksum(const void *_image, size_t len, void *_udata)
{
    const uint8_t  *image     = (const uint8_t *)_image;  /* Pointer into raw data buffer */
    H5O_cache_ud_t *udata     = (H5O_cache_ud_t *)_udata; /* User data for callback */
    htri_t          ret_value = true;

    FUNC_ENTER_PACKAGE_NOERR

    assert(image);
    assert(udata);
    assert(udata->oh);

    /* There is no checksum for version 1 */
    if (udata->oh->version != H5O_VERSION_1) {
        uint32_t stored_chksum;   /* Stored metadata checksum value */
        uint32_t computed_chksum; /* Computed metadata checksum value */

        /* Get stored and computed checksums */
        H5F_get_checksums(image, len, &stored_chksum, &computed_chksum);

        if (stored_chksum != computed_chksum) {
            /* These fields are not deserialized yet in H5O__prefix_deserialize() */
            assert(udata->oh->chunk == NULL);
            assert(udata->oh->mesg == NULL);
            assert(udata->oh->proxy == NULL);

            /* Indicate that udata->oh is to be freed later
               in H5O__prefix_deserialize() */
            udata->free_oh = true;
            ret_value      = false;
        }
    }
    else
        assert(!(udata->common.file_intent & H5F_ACC_SWMR_WRITE));

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__cache_verify_chksum() */

/*-------------------------------------------------------------------------
 * Function:    H5O__cache_deserialize
 *
 * Purpose:     Attempt to deserialize the object header contained in the
 *              supplied buffer, load the data into an instance of H5O_t, and
 *              return a pointer to the new instance.
 *
 *              Note that the object header is read with with a speculative
 *              read. If the initial read is too small, make note of this fact
 *              and return without error.  H5C__load_entry() will note the
 *              size discrepancy and retry the deserialize operation with
 *              the correct size read.
 *
 * Return:      Success:        Pointer to in core representation
 *              Failure:        NULL
 *-------------------------------------------------------------------------
 */
static void *
H5O__cache_deserialize(const void *image, size_t len, void *_udata, bool *dirty)
{
    H5O_t          *oh        = NULL;                     /* Object header read in */
    H5O_cache_ud_t *udata     = (H5O_cache_ud_t *)_udata; /* User data for callback */
    void           *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    assert(image);
    assert(len > 0);
    assert(udata);
    assert(udata->common.f);
    assert(udata->common.cont_msg_info);
    assert(dirty);

    /* Check for partially deserialized object header
     *
     * The Object header prefix will be deserialized if the object header came
     * through the 'get_final_load_size' callback and not deserialized if
     * the object header is coming from a cache image.
     */
    if (NULL == udata->oh) {
        /* Deserialize the object header prefix */
        if (H5O__prefix_deserialize((const uint8_t *)image, len, udata) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTDECODE, NULL, "can't deserialize object header prefix");
        assert(udata->oh);
    }

    /* Retrieve partially deserialized object header from user data */
    oh = udata->oh;

    /* Set SWMR flag, if appropriate */
    oh->swmr_write = !!(H5F_INTENT(udata->common.f) & H5F_ACC_SWMR_WRITE);

    /* Create object header proxy if doing SWMR writes */
    if (oh->swmr_write) {
        /* Create virtual entry, for use as proxy */
        if (NULL == (oh->proxy = H5AC_proxy_entry_create()))
            HGOTO_ERROR(H5E_OHDR, H5E_CANTCREATE, NULL, "can't create object header proxy");
    }
    else
        oh->proxy = NULL;

    /* Parse the first chunk */
    if (H5O__chunk_deserialize(oh, udata->common.addr, udata->chunk0_size, (const uint8_t *)image, len,
                               &(udata->common), dirty) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, NULL, "can't deserialize first object header chunk");

    /* Note that we've loaded the object header from the file */
    udata->made_attempt = true;

    /* Set return value */
    ret_value = oh;

done:
    /* Release the [possibly partially initialized] object header on errors */
    if (!ret_value && oh)
        if (H5O__free(oh, false) < 0)
            HDONE_ERROR(H5E_OHDR, H5E_CANTRELEASE, NULL, "unable to destroy object header data");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__cache_deserialize() */

/*-------------------------------------------------------------------------
 * Function:    H5O__cache_image_len
 *
 * Purpose:     Compute the size in bytes of the specified instance of
 *              H5O_t on disk, and return it in *image_len.  On failure,
 *              the value of *image_len is undefined.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__cache_image_len(const void *_thing, size_t *image_len)
{
    const H5O_t *oh = (const H5O_t *)_thing; /* Object header to query */

    FUNC_ENTER_PACKAGE_NOERR

    assert(oh);
    assert(oh->cache_info.type == H5AC_OHDR);
    assert(image_len);

    /* Report the object header's prefix+first chunk length */
    *image_len = oh->chunk[0].size;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__cache_image_len() */

/*-------------------------------------------------------------------------
 * Function:    H5O__cache_serialize
 *
 * Purpose:     Serialize the contents of the supplied object header, and
 *              load this data into the supplied buffer.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__cache_serialize(const H5F_t *f, void *image, size_t len, void *_thing)
{
    H5O_t   *oh = (H5O_t *)_thing; /* Object header to encode */
    uint8_t *chunk_image;          /* Pointer to object header prefix buffer */
    herr_t   ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(image);
    assert(oh);
    assert(oh->cache_info.type == H5AC_OHDR);
    assert(oh->chunk[0].size == len);
#ifdef H5O_DEBUG
    H5O__assert(oh);
#endif /* H5O_DEBUG */

    /* Point to raw data 'image' for first chunk, which
     * has room for the prefix
     */
    chunk_image = oh->chunk[0].image;

    /* Later versions of object header prefix have different format and
     * also require that chunk 0 always be updated, since the checksum
     * on the entire block of memory needs to be updated if anything is
     * modified
     */
    if (oh->version > H5O_VERSION_1) {
        uint64_t chunk0_size; /* Size of chunk 0's data */

        assert(oh->chunk[0].size >= (size_t)H5O_SIZEOF_HDR(oh));
        chunk0_size = oh->chunk[0].size - (size_t)H5O_SIZEOF_HDR(oh);

        /* Verify magic number */
        assert(!memcmp(chunk_image, H5O_HDR_MAGIC, H5_SIZEOF_MAGIC));
        chunk_image += H5_SIZEOF_MAGIC;

        /* Version */
        *chunk_image++ = oh->version;

        /* Flags */
        *chunk_image++ = oh->flags;

        /* Time fields */
        if (oh->flags & H5O_HDR_STORE_TIMES) {
            UINT32ENCODE(chunk_image, oh->atime);
            UINT32ENCODE(chunk_image, oh->mtime);
            UINT32ENCODE(chunk_image, oh->ctime);
            UINT32ENCODE(chunk_image, oh->btime);
        }

        /* Attribute fields */
        if (oh->flags & H5O_HDR_ATTR_STORE_PHASE_CHANGE) {
            UINT16ENCODE(chunk_image, oh->max_compact);
            UINT16ENCODE(chunk_image, oh->min_dense);
        }

        /* First chunk size */
        switch (oh->flags & H5O_HDR_CHUNK0_SIZE) {
            case 0: /* 1 byte size */
                assert(chunk0_size < 256);
                *chunk_image++ = (uint8_t)chunk0_size;
                break;

            case 1: /* 2 byte size */
                assert(chunk0_size < 65536);
                UINT16ENCODE(chunk_image, chunk0_size);
                break;

            case 2: /* 4 byte size */
                /* use <= 2**32 -1 to stay within 4 bytes integer range */
                assert(chunk0_size <= 4294967295UL);
                UINT32ENCODE(chunk_image, chunk0_size);
                break;

            case 3: /* 8 byte size */
                UINT64ENCODE(chunk_image, chunk0_size);
                break;

            default:
                HGOTO_ERROR(H5E_OHDR, H5E_BADVALUE, FAIL, "bad size for chunk 0");
        }
    }
    else {
        /* Version */
        *chunk_image++ = oh->version;

        /* Reserved */
        *chunk_image++ = 0;

        /* Number of messages */
#ifdef H5O_ENABLE_BAD_MESG_COUNT
        if (oh->store_bad_mesg_count)
            UINT16ENCODE(chunk_image, (oh->nmesgs - 1));
        else
#endif /* H5O_ENABLE_BAD_MESG_COUNT */
            UINT16ENCODE(chunk_image, oh->nmesgs);

        /* Link count */
        UINT32ENCODE(chunk_image, oh->nlink);

        /* First chunk size */
        UINT32ENCODE(chunk_image, (oh->chunk[0].size - (size_t)H5O_SIZEOF_HDR(oh)));

        /* Zero to alignment */
        memset(chunk_image, 0, (size_t)(H5O_SIZEOF_HDR(oh) - 12));
        chunk_image += (size_t)(H5O_SIZEOF_HDR(oh) - 12);
    }

    assert((size_t)(chunk_image - oh->chunk[0].image) ==
           (size_t)(H5O_SIZEOF_HDR(oh) - H5O_SIZEOF_CHKSUM_OH(oh)));

    /* Serialize messages for this chunk */
    if (H5O__chunk_serialize(f, oh, (unsigned)0) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSERIALIZE, FAIL, "unable to serialize first object header chunk");

    /* copy the chunk into the image -- this is potentially expensive.
     * Can we rework things so that the object header and the cache
     * share a buffer?
     */
    H5MM_memcpy(image, oh->chunk[0].image, len);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__cache_serialize() */

/*-------------------------------------------------------------------------
 * Function:    H5O__cache_notify
 *
 * Purpose:     Handle cache action notifications
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__cache_notify(H5AC_notify_action_t action, void *_thing)
{
    H5O_t *oh        = (H5O_t *)_thing;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(oh);

    switch (action) {
        case H5AC_NOTIFY_ACTION_AFTER_INSERT:
        case H5AC_NOTIFY_ACTION_AFTER_LOAD:
            if (oh->swmr_write) {
                /* Sanity check */
                assert(oh->proxy);

                /* Register the object header as a parent of the virtual entry */
                if (H5AC_proxy_entry_add_parent(oh->proxy, oh) < 0)
                    HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't add object header as parent of proxy");
            }
            break;

        case H5AC_NOTIFY_ACTION_AFTER_FLUSH:
        case H5AC_NOTIFY_ACTION_ENTRY_DIRTIED:
            /* Do nothing */
            break;

        case H5AC_NOTIFY_ACTION_ENTRY_CLEANED: {
            unsigned u; /* Local index variable */

            /* Mark messages stored with the object header (i.e. messages in chunk 0) as clean */
            for (u = 0; u < oh->nmesgs; u++)
                if (oh->mesg[u].chunkno == 0)
                    oh->mesg[u].dirty = false;
#ifndef NDEBUG
            /* Reset the number of messages dirtied by decoding */
            oh->ndecode_dirtied = 0;
#endif
        } break;

        case H5AC_NOTIFY_ACTION_CHILD_DIRTIED:
        case H5AC_NOTIFY_ACTION_CHILD_CLEANED:
        case H5AC_NOTIFY_ACTION_CHILD_UNSERIALIZED:
        case H5AC_NOTIFY_ACTION_CHILD_SERIALIZED:
            /* Do nothing */
            break;

        case H5AC_NOTIFY_ACTION_BEFORE_EVICT:
            if (oh->swmr_write) {
                /* Unregister the object header as a parent of the virtual entry */
                if (H5AC_proxy_entry_remove_parent(oh->proxy, oh) < 0)
                    HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't remove object header as parent of proxy");
            }
            break;

        default:
            HGOTO_ERROR(H5E_OHDR, H5E_BADVALUE, FAIL, "unknown action from metadata cache");
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__cache_notify() */

/*-------------------------------------------------------------------------
 * Function:    H5O__cache_free_icr
 *
 * Purpose:     Free the in core representation of the supplied object header.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__cache_free_icr(void *_thing)
{
    H5O_t *oh        = (H5O_t *)_thing; /* Object header to destroy */
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(oh);
    assert(oh->cache_info.type == H5AC_OHDR);

    /* Destroy object header */
    if (H5O__free(oh, false) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTRELEASE, FAIL, "can't destroy object header");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__cache_free_icr() */

/*-------------------------------------------------------------------------
 * Function:    H5O__cache_chk_get_initial_load_size()
 *
 * Purpose:     Tell the metadata cache how large the on disk image of the
 *              chunk proxy is, so it can load the image into a buffer for the
 *              deserialize call.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__cache_chk_get_initial_load_size(void *_udata, size_t *image_len)
{
    const H5O_chk_cache_ud_t *udata = (const H5O_chk_cache_ud_t *)_udata; /* User data for callback */

    FUNC_ENTER_PACKAGE_NOERR

    assert(udata);
    assert(udata->oh);
    assert(image_len);

    /* Set the image length size */
    *image_len = udata->size;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__cache_chk_get_initial_load_size() */

/*-------------------------------------------------------------------------
 * Function:    H5B2__cache_chk_verify_chksum
 *
 * Purpose:     Verify the computed checksum of the data structure is the
 *              same as the stored chksum.
 *
 * Return:      Success:        true/false
 *              Failure:        Negative
 *-------------------------------------------------------------------------
 */
static htri_t
H5O__cache_chk_verify_chksum(const void *_image, size_t len, void *_udata)
{
    const uint8_t      *image     = (const uint8_t *)_image;      /* Pointer into raw data buffer */
    H5O_chk_cache_ud_t *udata     = (H5O_chk_cache_ud_t *)_udata; /* User data for callback */
    htri_t              ret_value = true;

    FUNC_ENTER_PACKAGE_NOERR

    assert(image);

    /* There is no checksum for version 1 */
    if (udata->oh->version != H5O_VERSION_1) {
        uint32_t stored_chksum;   /* Stored metadata checksum value */
        uint32_t computed_chksum; /* Computed metadata checksum value */

        /* Get stored and computed checksums */
        H5F_get_checksums(image, len, &stored_chksum, &computed_chksum);

        if (stored_chksum != computed_chksum)
            ret_value = false;
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__cache_chk_verify_chksum() */

/*-------------------------------------------------------------------------
 * Function:    H5O__cache_chk_deserialize
 *
 * Purpose:     Attempt to deserialize the object header continuation chunk
 *              contained in the supplied buffer, load the data into an instance
 *              of H5O_chunk_proxy_t, and return a pointer to the new instance.
 *
 * Return:      Success:        Pointer to in core representation
 *              Failure:        NULL
 *-------------------------------------------------------------------------
 */
static void *
H5O__cache_chk_deserialize(const void *image, size_t len, void *_udata, bool *dirty)
{
    H5O_chunk_proxy_t  *chk_proxy = NULL;                         /* Chunk proxy object */
    H5O_chk_cache_ud_t *udata     = (H5O_chk_cache_ud_t *)_udata; /* User data for callback */
    void               *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    assert(image);
    assert(len > 0);
    assert(udata);
    assert(udata->oh);
    assert(dirty);

    /* Allocate space for the object header data structure */
    if (NULL == (chk_proxy = H5FL_CALLOC(H5O_chunk_proxy_t)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTALLOC, NULL, "memory allocation failed");

    /* Check if we are still decoding the object header */
    /* (as opposed to bringing a piece of it back from the file) */
    if (udata->decoding) {
        assert(udata->common.f);
        assert(udata->common.cont_msg_info);

        /* Parse the chunk */
        if (H5O__chunk_deserialize(udata->oh, udata->common.addr, udata->size, (const uint8_t *)image, len,
                                   &(udata->common), dirty) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, NULL, "can't deserialize object header chunk");

        /* Set the chunk number for the chunk proxy */
        H5_CHECKED_ASSIGN(chk_proxy->chunkno, unsigned, udata->oh->nchunks - 1, size_t);
    }
    else {
        /* Sanity check */
        assert(udata->chunkno < udata->oh->nchunks);

        /* Set the chunk number for the chunk proxy */
        chk_proxy->chunkno = udata->chunkno;

        /* Sanity check that the chunk representation we have in memory is
         * the same as the one being brought in from disk.
         */
        assert(0 == memcmp(image, udata->oh->chunk[chk_proxy->chunkno].image,
                           udata->oh->chunk[chk_proxy->chunkno].size));
    }

    /* Increment reference count of object header */
    if (H5O__inc_rc(udata->oh) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTINC, NULL, "can't increment reference count on object header");
    chk_proxy->oh = udata->oh;

    /* Set return value */
    ret_value = chk_proxy;

done:
    if (NULL == ret_value)
        if (chk_proxy && H5O__chunk_dest(chk_proxy) < 0)
            HDONE_ERROR(H5E_OHDR, H5E_CANTRELEASE, NULL, "unable to destroy object header chunk");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__cache_chk_deserialize() */

/*-------------------------------------------------------------------------
 * Function:    H5O__cache_chk_image_len
 *
 * Purpose:     Return the on disk image size of a object header chunk to the
 *              metadata cache via the image_len.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__cache_chk_image_len(const void *_thing, size_t *image_len)
{
    const H5O_chunk_proxy_t *chk_proxy = (const H5O_chunk_proxy_t *)_thing; /* Chunk proxy to query */

    FUNC_ENTER_PACKAGE_NOERR

    assert(chk_proxy);
    assert(chk_proxy->cache_info.type == H5AC_OHDR_CHK);
    assert(chk_proxy->oh);
    assert(image_len);

    *image_len = chk_proxy->oh->chunk[chk_proxy->chunkno].size;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__cache_chk_image_len() */

/*-------------------------------------------------------------------------
 * Function:    H5O__cache_chk_serialize
 *
 * Purpose:     Given a pointer to an instance of an object header chunk and an
 *              appropriately sized buffer, serialize the contents of the
 *              instance for writing to disk, and copy the serialized data
 *              into the buffer.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__cache_chk_serialize(const H5F_t *f, void *image, size_t len, void *_thing)
{
    H5O_chunk_proxy_t *chk_proxy = (H5O_chunk_proxy_t *)_thing; /* Object header chunk to serialize */
    herr_t             ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(image);
    assert(chk_proxy);
    assert(chk_proxy->cache_info.type == H5AC_OHDR_CHK);
    assert(chk_proxy->oh);
    assert(chk_proxy->oh->chunk[chk_proxy->chunkno].size == len);

    /* Serialize messages for this chunk */
    if (H5O__chunk_serialize(f, chk_proxy->oh, chk_proxy->chunkno) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSERIALIZE, FAIL,
                    "unable to serialize object header continuation chunk");

    /* copy the chunk into the image -- this is potentially expensive.
     * Can we rework things so that the chunk and the cache share a buffer?
     */
    H5MM_memcpy(image, chk_proxy->oh->chunk[chk_proxy->chunkno].image, len);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__cache_chk_serialize() */

/*-------------------------------------------------------------------------
 * Function:    H5O__cache_chk_notify
 *
 * Purpose:     Handle cache action notifications
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__cache_chk_notify(H5AC_notify_action_t action, void *_thing)
{
    H5O_chunk_proxy_t *chk_proxy = (H5O_chunk_proxy_t *)_thing;
    herr_t             ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(chk_proxy);
    assert(chk_proxy->oh);

    switch (action) {
        case H5AC_NOTIFY_ACTION_AFTER_INSERT:
        case H5AC_NOTIFY_ACTION_AFTER_LOAD:
            if (chk_proxy->oh->swmr_write) {
                /* Add flush dependency on chunk with continuation, if one exists */
                if (chk_proxy->fd_parent) {
                    /* Sanity checks */
                    assert(((H5C_cache_entry_t *)(chk_proxy->fd_parent))->type);
                    assert((((H5C_cache_entry_t *)(chk_proxy->fd_parent))->type->id == H5AC_OHDR_ID) ||
                           (((H5C_cache_entry_t *)(chk_proxy->fd_parent))->type->id == H5AC_OHDR_CHK_ID));

                    /* Add flush dependency from chunk containing the continuation message
                     * that points to this chunk (either oh or another chunk proxy object)
                     */
                    if (H5AC_create_flush_dependency(chk_proxy->fd_parent, chk_proxy) < 0)
                        HGOTO_ERROR(H5E_OHDR, H5E_CANTDEPEND, FAIL, "unable to create flush dependency");
                }

                /* Add flush dependency on object header */
                {
                    if (H5AC_create_flush_dependency(chk_proxy->oh, chk_proxy) < 0)
                        HGOTO_ERROR(H5E_OHDR, H5E_CANTDEPEND, FAIL, "unable to create flush dependency");
                }

                /* Add flush dependency on object header proxy, if proxy exists */
                {
                    /* Sanity check */
                    assert(chk_proxy->oh->proxy);

                    /* Register the object header chunk as a parent of the virtual entry */
                    if (H5AC_proxy_entry_add_parent(chk_proxy->oh->proxy, chk_proxy) < 0)
                        HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL,
                                    "can't add object header chunk as parent of proxy");
                }
            }
            break;

        case H5AC_NOTIFY_ACTION_AFTER_FLUSH:
        case H5AC_NOTIFY_ACTION_ENTRY_DIRTIED:
            /* Do nothing */
            break;

        case H5AC_NOTIFY_ACTION_ENTRY_CLEANED: {
            unsigned u; /* Local index variable */

            /* Mark messages in chunk as clean */
            for (u = 0; u < chk_proxy->oh->nmesgs; u++)
                if (chk_proxy->oh->mesg[u].chunkno == chk_proxy->chunkno)
                    chk_proxy->oh->mesg[u].dirty = false;
        } break;

        case H5AC_NOTIFY_ACTION_CHILD_DIRTIED:
        case H5AC_NOTIFY_ACTION_CHILD_CLEANED:
        case H5AC_NOTIFY_ACTION_CHILD_UNSERIALIZED:
        case H5AC_NOTIFY_ACTION_CHILD_SERIALIZED:
            /* Do nothing */
            break;

        case H5AC_NOTIFY_ACTION_BEFORE_EVICT:
            if (chk_proxy->oh->swmr_write) {
                /* Remove flush dependency on parent object header chunk, if one is set */
                if (chk_proxy->fd_parent) {
                    /* Sanity checks */
                    assert(((H5C_cache_entry_t *)(chk_proxy->fd_parent))->type);
                    assert((((H5C_cache_entry_t *)(chk_proxy->fd_parent))->type->id == H5AC_OHDR_ID) ||
                           (((H5C_cache_entry_t *)(chk_proxy->fd_parent))->type->id == H5AC_OHDR_CHK_ID));

                    if (H5AC_destroy_flush_dependency(chk_proxy->fd_parent, chk_proxy) < 0)
                        HGOTO_ERROR(H5E_OHDR, H5E_CANTUNDEPEND, FAIL, "unable to destroy flush dependency");
                    chk_proxy->fd_parent = NULL;
                }

                /* Unregister the object header as a parent of the virtual entry */
                if (H5AC_destroy_flush_dependency(chk_proxy->oh, chk_proxy) < 0)
                    HGOTO_ERROR(H5E_OHDR, H5E_CANTUNDEPEND, FAIL, "unable to destroy flush dependency");

                /* Unregister the object header chunk as a parent of the virtual entry */
                if (H5AC_proxy_entry_remove_parent(chk_proxy->oh->proxy, chk_proxy) < 0)
                    HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL,
                                "can't remove object header chunk as parent of proxy");
            }
            break;

        default:
            HGOTO_ERROR(H5E_OHDR, H5E_BADVALUE, FAIL, "unknown action from metadata cache");
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__cache_chk_notify() */

/*-------------------------------------------------------------------------
 * Function:    H5O__cache_chk_free_icr
 *
 * Purpose:     Free the in core memory associated with the supplied object
 *              header continuation chunk.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__cache_chk_free_icr(void *_thing)
{
    H5O_chunk_proxy_t *chk_proxy = (H5O_chunk_proxy_t *)_thing; /* Object header chunk proxy to release */
    herr_t             ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(chk_proxy);
    assert(chk_proxy->cache_info.type == H5AC_OHDR_CHK);

    /* Destroy object header chunk proxy */
    if (H5O__chunk_dest(chk_proxy) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTRELEASE, FAIL, "unable to destroy object header chunk proxy");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__cache_chk_free_icr() */

/*-------------------------------------------------------------------------
 * Function:	H5O__add_cont_msg
 *
 * Purpose:     Add information from a continuation message to the list of
 *              continuation messages in the object header
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__add_cont_msg(H5O_cont_msgs_t *cont_msg_info, const H5O_cont_t *cont)
{
    size_t contno; /* Continuation message index */
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(cont_msg_info);
    assert(cont);

    /* Increase chunk array size, if necessary */
    if (cont_msg_info->nmsgs >= cont_msg_info->alloc_nmsgs) {
        size_t na = MAX(H5O_NCHUNKS, cont_msg_info->alloc_nmsgs * 2); /* Double # of messages allocated */
        H5O_cont_t *x;

        if (NULL == (x = H5FL_SEQ_REALLOC(H5O_cont_t, cont_msg_info->msgs, na)))
            HGOTO_ERROR(H5E_OHDR, H5E_NOSPACE, FAIL, "memory allocation failed");
        cont_msg_info->alloc_nmsgs = na;
        cont_msg_info->msgs        = x;
    }

    /* Init the continuation message info */
    contno                              = cont_msg_info->nmsgs++;
    cont_msg_info->msgs[contno].addr    = cont->addr;
    cont_msg_info->msgs[contno].size    = cont->size;
    cont_msg_info->msgs[contno].chunkno = cont->chunkno;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__add_cont_msg() */

/*-------------------------------------------------------------------------
 * Function:    H5O__prefix_deserialize()
 *
 * Purpose:     Deserialize an object header prefix
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__prefix_deserialize(const uint8_t *_image, size_t len, H5O_cache_ud_t *udata)
{
    const uint8_t *image     = (const uint8_t *)_image; /* Pointer into raw data buffer */
    const uint8_t *p_end     = image + len - 1;         /* End of image buffer */
    H5O_t         *oh        = NULL;                    /* Object header read in */
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(image);
    assert(udata);

    /* Allocate space for the new object header data structure */
    if (NULL == (oh = H5FL_CALLOC(H5O_t)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTALLOC, FAIL, "memory allocation failed");

    /* File-specific, non-stored information */
    oh->sizeof_size = H5F_SIZEOF_SIZE(udata->common.f);
    oh->sizeof_addr = H5F_SIZEOF_ADDR(udata->common.f);

    /* Check for presence of magic number */
    /* (indicates version 2 or later) */
    if (H5_IS_BUFFER_OVERFLOW(image, H5_SIZEOF_MAGIC, p_end))
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
    if (!memcmp(image, H5O_HDR_MAGIC, (size_t)H5_SIZEOF_MAGIC)) {

        /* Magic number (bounds checked above) */
        image += H5_SIZEOF_MAGIC;

        /* Version */
        if (H5_IS_BUFFER_OVERFLOW(image, 1, p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
        oh->version = *image++;
        if (H5O_VERSION_2 != oh->version)
            HGOTO_ERROR(H5E_OHDR, H5E_VERSION, FAIL, "bad object header version number");

        /* Flags */
        if (H5_IS_BUFFER_OVERFLOW(image, 1, p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
        oh->flags = *image++;
        if (oh->flags & ~H5O_HDR_ALL_FLAGS)
            HGOTO_ERROR(H5E_OHDR, H5E_BADVALUE, FAIL, "unknown object header status flag(s)");

        /* Number of links to object (unless overridden by refcount message) */
        oh->nlink = 1;

        /* Time fields */
        if (oh->flags & H5O_HDR_STORE_TIMES) {
            uint32_t tmp;

            if (H5_IS_BUFFER_OVERFLOW(image, 4 + 4 + 4 + 4, p_end))
                HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");

            UINT32DECODE(image, tmp);
            oh->atime = (time_t)tmp;
            UINT32DECODE(image, tmp);
            oh->mtime = (time_t)tmp;
            UINT32DECODE(image, tmp);
            oh->ctime = (time_t)tmp;
            UINT32DECODE(image, tmp);
            oh->btime = (time_t)tmp;
        }
        else
            oh->atime = oh->mtime = oh->ctime = oh->btime = 0;

        /* Attribute fields */
        if (oh->flags & H5O_HDR_ATTR_STORE_PHASE_CHANGE) {
            if (H5_IS_BUFFER_OVERFLOW(image, 2 + 2, p_end))
                HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");

            UINT16DECODE(image, oh->max_compact);
            UINT16DECODE(image, oh->min_dense);
            if (oh->max_compact < oh->min_dense)
                HGOTO_ERROR(H5E_OHDR, H5E_BADVALUE, FAIL, "bad object header attribute phase change values");
        }
        else {
            oh->max_compact = H5O_CRT_ATTR_MAX_COMPACT_DEF;
            oh->min_dense   = H5O_CRT_ATTR_MIN_DENSE_DEF;
        }

        /* First chunk size */
        switch (oh->flags & H5O_HDR_CHUNK0_SIZE) {
            case 0: /* 1 byte size */
                if (H5_IS_BUFFER_OVERFLOW(image, 1, p_end))
                    HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
                udata->chunk0_size = *image++;
                break;

            case 1: /* 2 byte size */
                if (H5_IS_BUFFER_OVERFLOW(image, 2, p_end))
                    HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
                UINT16DECODE(image, udata->chunk0_size);
                break;

            case 2: /* 4 byte size */
                if (H5_IS_BUFFER_OVERFLOW(image, 4, p_end))
                    HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
                UINT32DECODE(image, udata->chunk0_size);
                break;

            case 3: /* 8 byte size */
                if (H5_IS_BUFFER_OVERFLOW(image, 8, p_end))
                    HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
                UINT64DECODE(image, udata->chunk0_size);
                break;

            default:
                HGOTO_ERROR(H5E_OHDR, H5E_BADVALUE, FAIL, "bad size for chunk 0");
        }
        if (udata->chunk0_size > 0 && udata->chunk0_size < H5O_SIZEOF_MSGHDR_OH(oh))
            HGOTO_ERROR(H5E_OHDR, H5E_BADVALUE, FAIL, "bad object header chunk size");
    }
    else {
        /* Version */
        if (H5_IS_BUFFER_OVERFLOW(image, 1, p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
        oh->version = *image++;
        if (H5O_VERSION_1 != oh->version)
            HGOTO_ERROR(H5E_OHDR, H5E_VERSION, FAIL, "bad object header version number");

        /* Flags */
        oh->flags = H5O_CRT_OHDR_FLAGS_DEF;

        /* Reserved */
        if (H5_IS_BUFFER_OVERFLOW(image, 1, p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
        image++;

        /* Number of messages */
        if (H5_IS_BUFFER_OVERFLOW(image, 2, p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
        UINT16DECODE(image, udata->v1_pfx_nmesgs);

        /* Link count */
        if (H5_IS_BUFFER_OVERFLOW(image, 4, p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
        UINT32DECODE(image, oh->nlink);

        /* Reset unused time fields */
        oh->atime = oh->mtime = oh->ctime = oh->btime = 0;

        /* Reset unused attribute fields */
        oh->max_compact = 0;
        oh->min_dense   = 0;

        /* First chunk size */
        if (H5_IS_BUFFER_OVERFLOW(image, 4, p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
        UINT32DECODE(image, udata->chunk0_size);
        if ((udata->v1_pfx_nmesgs > 0 && udata->chunk0_size < H5O_SIZEOF_MSGHDR_OH(oh)) ||
            (udata->v1_pfx_nmesgs == 0 && udata->chunk0_size > 0))
            HGOTO_ERROR(H5E_OHDR, H5E_BADVALUE, FAIL, "bad object header chunk size");

        /* Reserved, in version 1 (for 8-byte alignment padding) */
        if (H5_IS_BUFFER_OVERFLOW(image, 4, p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
        image += 4;
    }

    /* Verify object header prefix length */
    if ((size_t)(image - _image) != (size_t)(H5O_SIZEOF_HDR(oh) - H5O_SIZEOF_CHKSUM_OH(oh)))
        HGOTO_ERROR(H5E_OHDR, H5E_BADVALUE, FAIL, "bad object header prefix length");

    /* If udata->oh is to be freed (see H5O__cache_verify_chksum),
     * save the pointer to udata->oh and free it later after setting
     * udata->oh with the new object header
     */
    if (udata->free_oh) {
        H5O_t *saved_oh = udata->oh;
        assert(udata->oh);

        /* Save the object header for later use in 'deserialize' callback */
        udata->oh = oh;
        if (H5O__free(saved_oh, false) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTRELEASE, FAIL, "can't destroy object header");
        udata->free_oh = false;
    }
    else
        /* Save the object header for later use in 'deserialize' callback */
        udata->oh = oh;

    oh = NULL;

done:
    /* Release the [possibly partially initialized] object header on errors */
    if (ret_value < 0 && oh)
        if (H5O__free(oh, false) < 0)
            HDONE_ERROR(H5E_OHDR, H5E_CANTRELEASE, FAIL, "unable to destroy object header data");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__prefix_deserialize() */

/*-------------------------------------------------------------------------
 * Function:	H5O__chunk_deserialize
 *
 * Purpose:     Deserialize a chunk for an object header
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__chunk_deserialize(H5O_t *oh, haddr_t addr, size_t chunk_size, const uint8_t *image, size_t len,
                       H5O_common_cache_ud_t *udata, bool *dirty)
{
    const uint8_t *chunk_image = NULL;   /* Pointer into buffer to decode */
    const uint8_t *p_end       = NULL;   /* End of image buffer */
    uint8_t       *eom_ptr;              /* Pointer to end of messages for a chunk */
    unsigned       merged_null_msgs = 0; /* Number of null messages merged together */
    unsigned       chunkno;              /* Current chunk's index */
    unsigned       nullcnt;              /* Count of null messages (for sanity checking gaps in chunks) */
    bool           mesgs_modified =
        false; /* Whether any messages were modified when the object header was deserialized */
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(oh);
    assert(H5_addr_defined(addr));
    assert(image);
    assert(len);
    assert(udata->f);
    assert(udata->cont_msg_info);

    /* Increase chunk array size, if necessary */
    if (oh->nchunks >= oh->alloc_nchunks) {
        size_t       na = MAX(H5O_NCHUNKS, oh->alloc_nchunks * 2); /* Double # of chunks allocated */
        H5O_chunk_t *x;

        if (NULL == (x = H5FL_SEQ_REALLOC(H5O_chunk_t, oh->chunk, na)))
            HGOTO_ERROR(H5E_OHDR, H5E_CANTALLOC, FAIL, "memory allocation failed");
        oh->alloc_nchunks = na;
        oh->chunk         = x;
    }

    /* Init the chunk data info */
    chunkno                 = (unsigned)oh->nchunks++;
    oh->chunk[chunkno].gap  = 0;
    oh->chunk[chunkno].addr = addr;
    if (chunkno == 0)
        /* First chunk's 'image' includes room for the object header prefix */
        oh->chunk[0].size = chunk_size + (size_t)H5O_SIZEOF_HDR(oh);
    else
        oh->chunk[chunkno].size = chunk_size;
    if (NULL == (oh->chunk[chunkno].image = H5FL_BLK_MALLOC(chunk_image, oh->chunk[chunkno].size)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTALLOC, FAIL, "memory allocation failed");
    oh->chunk[chunkno].chunk_proxy = NULL;

    /* Copy disk image into chunk's image */
    if (len < oh->chunk[chunkno].size)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTCOPY, FAIL, "attempted to copy too many disk image bytes into buffer");
    H5MM_memcpy(oh->chunk[chunkno].image, image, oh->chunk[chunkno].size);

    /* Point into chunk image to decode */
    chunk_image = oh->chunk[chunkno].image;
    p_end       = chunk_image + oh->chunk[chunkno].size - 1;

    /* Skip over [already decoded] prefix in special case of chunk 0 */
    if (chunkno == 0) {
        size_t skip = (size_t)(H5O_SIZEOF_HDR(oh) - H5O_SIZEOF_CHKSUM_OH(oh));

        if (H5_IS_BUFFER_OVERFLOW(chunk_image, skip, p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
        chunk_image += skip;
    }

    /* Check for magic # on chunks > 0 in later versions of the format */
    else if (chunkno > 0 && oh->version > H5O_VERSION_1) {
        /* Magic number */
        if (H5_IS_BUFFER_OVERFLOW(chunk_image, H5_SIZEOF_MAGIC, p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
        if (memcmp(chunk_image, H5O_CHK_MAGIC, H5_SIZEOF_MAGIC) != 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, FAIL, "wrong object header chunk signature");
        chunk_image += H5_SIZEOF_MAGIC;
    }

    /* Decode messages from this chunk */
    eom_ptr = oh->chunk[chunkno].image + (oh->chunk[chunkno].size - H5O_SIZEOF_CHKSUM_OH(oh));
    nullcnt = 0;

    while (chunk_image < eom_ptr) {
        size_t            mesg_size;   /* Size of message read in */
        unsigned          id;          /* ID (type) of current message */
        uint8_t           flags;       /* Flags for current message */
        H5O_msg_crt_idx_t crt_idx = 0; /* Creation index for current message */

        /* Decode message prefix info */

        /* Version # */
        if (oh->version == H5O_VERSION_1) {
            if (H5_IS_BUFFER_OVERFLOW(chunk_image, 2, p_end))
                HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
            UINT16DECODE(chunk_image, id);
        }
        else {
            if (H5_IS_BUFFER_OVERFLOW(chunk_image, 1, p_end))
                HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
            id = *chunk_image++;
        }

        /* Message size */
        if (H5_IS_BUFFER_OVERFLOW(chunk_image, 2, p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
        UINT16DECODE(chunk_image, mesg_size);
        if (mesg_size != H5O_ALIGN_OH(oh, mesg_size))
            HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, FAIL, "message not aligned");

        /* Message flags */
        if (H5_IS_BUFFER_OVERFLOW(chunk_image, 1, p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
        flags = *chunk_image++;
        if (flags & ~H5O_MSG_FLAG_BITS)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, FAIL, "unknown flag for message");
        if ((flags & H5O_MSG_FLAG_SHARED) && (flags & H5O_MSG_FLAG_DONTSHARE))
            HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, FAIL, "bad flag combination for message");
        if ((flags & H5O_MSG_FLAG_WAS_UNKNOWN) && (flags & H5O_MSG_FLAG_FAIL_IF_UNKNOWN_AND_OPEN_FOR_WRITE))
            HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, FAIL, "bad flag combination for message");
        if ((flags & H5O_MSG_FLAG_WAS_UNKNOWN) && !(flags & H5O_MSG_FLAG_MARK_IF_UNKNOWN))
            HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, FAIL, "bad flag combination for message");
        /* Delay checking the "shareable" flag until we've made sure id
         * references a valid message class that this version of the library
         * knows about */

        /* Reserved bytes/creation index */
        if (oh->version == H5O_VERSION_1) {
            /* Reserved bytes */
            if (H5_IS_BUFFER_OVERFLOW(chunk_image, 3, p_end))
                HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
            chunk_image += 3;
        }
        else {
            /* Only decode creation index if they are being tracked */
            if (oh->flags & H5O_HDR_ATTR_CRT_ORDER_TRACKED) {
                if (H5_IS_BUFFER_OVERFLOW(chunk_image, 2, p_end))
                    HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
                UINT16DECODE(chunk_image, crt_idx);
            }
        }

        /* Try to detect invalidly formatted object header message that
         *  extends past end of chunk.
         */
        if (chunk_image + mesg_size > eom_ptr)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, FAIL, "corrupt object header");

        /* Increment count of null messages */
        if (H5O_NULL_ID == id)
            nullcnt++;

        /* Check for combining two adjacent 'null' messages */
        if ((udata->file_intent & H5F_ACC_RDWR) && H5O_NULL_ID == id && oh->nmesgs > 0 &&
            H5O_NULL_ID == oh->mesg[oh->nmesgs - 1].type->id && oh->mesg[oh->nmesgs - 1].chunkno == chunkno) {

            size_t mesgno; /* Current message to operate on */

            /* Combine adjacent null messages */
            mesgno = oh->nmesgs - 1;
            oh->mesg[mesgno].raw_size += (size_t)H5O_SIZEOF_MSGHDR_OH(oh) + mesg_size;
            oh->mesg[mesgno].dirty = true;
            merged_null_msgs++;
        }
        else {
            H5O_mesg_t *mesg;        /* Pointer to new message */
            unsigned    ioflags = 0; /* Flags for decode routine */

            /* Check if we need to extend message table to hold the new message */
            if (oh->nmesgs >= oh->alloc_nmesgs)
                if (H5O__alloc_msgs(oh, (size_t)1) < 0)
                    HGOTO_ERROR(H5E_OHDR, H5E_CANTALLOC, FAIL, "can't allocate more space for messages");

            /* Get pointer to message to set up */
            mesg = &oh->mesg[oh->nmesgs];

            /* Increment # of messages */
            oh->nmesgs++;

            /* Initialize information about message */
            mesg->dirty   = false;
            mesg->flags   = flags;
            mesg->crt_idx = crt_idx;
            mesg->native  = NULL;
            H5_GCC_CLANG_DIAG_OFF("cast-qual")
            mesg->raw = (uint8_t *)chunk_image;
            H5_GCC_CLANG_DIAG_ON("cast-qual")
            mesg->raw_size = mesg_size;
            mesg->chunkno  = chunkno;

            /* Point unknown messages at 'unknown' message class */
            /* (Usually from future versions of the library) */
            if (id >= H5O_UNKNOWN_ID ||
#ifdef H5O_ENABLE_BOGUS
                id == H5O_BOGUS_VALID_ID ||
#endif
                NULL == H5O_msg_class_g[id]) {

                H5O_unknown_t *unknown; /* Pointer to "unknown" message info */

                /* Allocate "unknown" message info */
                if (NULL == (unknown = H5FL_MALLOC(H5O_unknown_t)))
                    HGOTO_ERROR(H5E_OHDR, H5E_CANTALLOC, FAIL, "memory allocation failed");

                /* Save the original message type ID */
                *unknown = id;

                /* Save 'native' form of unknown message */
                mesg->native = unknown;

                /* Set message to "unknown" class */
                mesg->type = H5O_msg_class_g[H5O_UNKNOWN_ID];

                /* Check for "fail if unknown" message flags */
                if (((udata->file_intent & H5F_ACC_RDWR) &&
                     (flags & H5O_MSG_FLAG_FAIL_IF_UNKNOWN_AND_OPEN_FOR_WRITE)) ||
                    (flags & H5O_MSG_FLAG_FAIL_IF_UNKNOWN_ALWAYS))
                    HGOTO_ERROR(H5E_OHDR, H5E_BADMESG, FAIL,
                                "unknown message with 'fail if unknown' flag found");
                /* Check for "mark if unknown" message flag, etc. */
                else if ((flags & H5O_MSG_FLAG_MARK_IF_UNKNOWN) && !(flags & H5O_MSG_FLAG_WAS_UNKNOWN) &&
                         (udata->file_intent & H5F_ACC_RDWR)) {

                    /* Mark the message as "unknown" */
                    /* This is a bit aggressive, since the application may
                     * never change anything about the object (metadata or
                     * raw data), but we can sort out the finer details
                     * when/if we start using the flag.
                     */
                    /* Also, it's possible that this functionality may not
                     * get invoked if the object header is brought into
                     * the metadata cache in some other "weird" way, like
                     * using H5Ocopy().
                     */
                    mesg->flags |= H5O_MSG_FLAG_WAS_UNKNOWN;

                    /* Mark the message and chunk as dirty */
                    mesg->dirty    = true;
                    mesgs_modified = true;
                }
            }
            else {
                /* Check for message of unshareable class marked as "shareable"
                 */
                if ((flags & H5O_MSG_FLAG_SHAREABLE) && H5O_msg_class_g[id] &&
                    !(H5O_msg_class_g[id]->share_flags & H5O_SHARE_IS_SHARABLE))
                    HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, FAIL,
                                "message of unshareable class flagged as shareable");

                /* Set message class for "known" messages */
                mesg->type = H5O_msg_class_g[id];
            }

            /* Do some inspection/interpretation of new messages from this chunk */
            /* (detect continuation messages, ref. count messages, etc.) */

            /* Check if message is a continuation message */
            if (H5O_CONT_ID == id) {
                H5O_cont_t *cont;

                /* Decode continuation message */
                if (NULL == (cont = (H5O_cont_t *)(H5O_MSG_CONT->decode)(udata->f, NULL, 0, &ioflags,
                                                                         mesg->raw_size, mesg->raw)))
                    HGOTO_ERROR(H5E_OHDR, H5E_BADMESG, FAIL, "bad continuation message found");
                H5_CHECKED_ASSIGN(cont->chunkno, unsigned, udata->cont_msg_info->nmsgs + 1,
                                  size_t); /* the next continuation message/chunk */

                /* Save 'native' form of continuation message */
                mesg->native = cont;

                /* Add to continuation messages left to interpret */
                if (H5O__add_cont_msg(udata->cont_msg_info, cont) < 0)
                    HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't add continuation message");
            }
            /* Check if message is a ref. count message */
            else if (H5O_REFCOUNT_ID == id) {
                H5O_refcount_t *refcount;

                /* Decode ref. count message */
                if (oh->version <= H5O_VERSION_1)
                    HGOTO_ERROR(H5E_OHDR, H5E_VERSION, FAIL,
                                "object header version does not support reference count message");
                refcount = (H5O_refcount_t *)(H5O_MSG_REFCOUNT->decode)(udata->f, NULL, 0, &ioflags,
                                                                        mesg->raw_size, mesg->raw);

                /* Save 'native' form of ref. count message */
                mesg->native = refcount;

                /* Set object header values */
                oh->has_refcount_msg = true;
                if (!refcount)
                    HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't decode refcount");
                oh->nlink = *refcount;
            }
            /* Check if message is a link message */
            else if (H5O_LINK_ID == id) {
                /* Increment the count of link messages */
                oh->link_msgs_seen++;
            }
            /* Check if message is an attribute message */
            else if (H5O_ATTR_ID == id) {
                /* Increment the count of attribute messages */
                oh->attr_msgs_seen++;
            }

            /* Mark the message & chunk as dirty if the message was changed by decoding */
            if ((ioflags & H5O_DECODEIO_DIRTY) && (udata->file_intent & H5F_ACC_RDWR)) {
                mesg->dirty    = true;
                mesgs_modified = true;
            }
        }

        /* Advance decode pointer past message */
        chunk_image += mesg_size;

        /* Check for 'gap' at end of chunk */
        if ((eom_ptr - chunk_image) > 0 && (eom_ptr - chunk_image) < H5O_SIZEOF_MSGHDR_OH(oh)) {
            /* Gaps can only occur in later versions of the format */
            if (oh->version == H5O_VERSION_1)
                HGOTO_ERROR(H5E_OHDR, H5E_BADMESG, FAIL, "gap found in early version of file format");

            /* Gaps should only occur in chunks with no null messages */
            if (nullcnt != 0)
                HGOTO_ERROR(H5E_OHDR, H5E_BADMESG, FAIL, "gap in chunk with no null messages");

            /* Set gap information for chunk */
            oh->chunk[chunkno].gap = (size_t)(eom_ptr - chunk_image);

            /* Increment location in chunk */
            chunk_image += oh->chunk[chunkno].gap;
        }
    }

    /* Check for correct checksum on chunks, in later versions of the format */
    if (oh->version > H5O_VERSION_1) {
        uint32_t stored_chksum; /* Checksum from file */

        /* checksum verification already done in verify_chksum cb */

        /* Metadata checksum */
        if (H5_IS_BUFFER_OVERFLOW(chunk_image, 4, p_end))
            HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
        UINT32DECODE(chunk_image, stored_chksum);
    }

    /* Size check */
    if (chunk_image != oh->chunk[chunkno].image + oh->chunk[chunkno].size)
        HGOTO_ERROR(H5E_OHDR, H5E_OVERFLOW, FAIL, "object header image size mismatch");

    /* Mark the chunk dirty if we've modified messages */
    if (mesgs_modified)
        *dirty = true;

    /* Mark the chunk dirty if we've merged null messages */
    if (merged_null_msgs > 0) {
        udata->merged_null_msgs += merged_null_msgs;
        *dirty = true;
    }

done:
    if (ret_value < 0 && udata->cont_msg_info->msgs) {
        udata->cont_msg_info->msgs        = H5FL_SEQ_FREE(H5O_cont_t, udata->cont_msg_info->msgs);
        udata->cont_msg_info->alloc_nmsgs = 0;
    }
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__chunk_deserialize() */

/*-------------------------------------------------------------------------
 * Function:    H5O__chunk_serialize
 *
 * Purpose:     Serialize a chunk for an object header
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__chunk_serialize(const H5F_t *f, H5O_t *oh, unsigned chunkno)
{
    H5O_mesg_t *curr_msg; /* Pointer to current message being operated on */
    unsigned    u;        /* Local index variable */
    herr_t      ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(oh);

    /* Encode any dirty messages in this chunk */
    for (u = 0, curr_msg = &oh->mesg[0]; u < oh->nmesgs; u++, curr_msg++)
        if (curr_msg->dirty && curr_msg->chunkno == chunkno) {
            H5_GCC_CLANG_DIAG_OFF("cast-qual")
            if (H5O_msg_flush((H5F_t *)f, oh, curr_msg) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTENCODE, FAIL, "unable to encode object header message");
            H5_GCC_CLANG_DIAG_ON("cast-qual")
        }

    /* Sanity checks */
    if (oh->version > H5O_VERSION_1)
        /* Make certain the magic # is present */
        assert(!memcmp(oh->chunk[chunkno].image, (chunkno == 0 ? H5O_HDR_MAGIC : H5O_CHK_MAGIC),
                       H5_SIZEOF_MAGIC));
    else
        /* Gaps should never occur in version 1 of the format */
        assert(oh->chunk[chunkno].gap == 0);

    /* Extra work, for later versions of the format */
    if (oh->version > H5O_VERSION_1) {
        uint32_t metadata_chksum; /* Computed metadata checksum value */
        uint8_t *chunk_image;     /* Pointer into object header chunk */

        /* Check for gap in chunk & zero it out */
        if (oh->chunk[chunkno].gap)
            memset((oh->chunk[chunkno].image + oh->chunk[chunkno].size) -
                       (H5O_SIZEOF_CHKSUM + oh->chunk[chunkno].gap),
                   0, oh->chunk[chunkno].gap);

        /* Compute metadata checksum */
        metadata_chksum =
            H5_checksum_metadata(oh->chunk[chunkno].image, (oh->chunk[chunkno].size - H5O_SIZEOF_CHKSUM), 0);

        /* Metadata checksum */
        chunk_image = oh->chunk[chunkno].image + (oh->chunk[chunkno].size - H5O_SIZEOF_CHKSUM);
        UINT32ENCODE(chunk_image, metadata_chksum);
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__chunk_serialize() */
