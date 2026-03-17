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
 * Created:		H5HGcache.c
 *
 * Purpose:		Implement global heap metadata cache methods
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5HGmodule.h" /* This source code file is part of the H5HG module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions           */
#include "H5Eprivate.h"  /* Error handling              */
#include "H5Fprivate.h"  /* File access                 */
#include "H5HGpkg.h"     /* Global heaps                */
#include "H5MFprivate.h" /* File memory management      */
#include "H5MMprivate.h" /* Memory management           */

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
static herr_t H5HG__cache_heap_get_initial_load_size(void *udata, size_t *image_len);
static herr_t H5HG__cache_heap_get_final_load_size(const void *_image, size_t image_len, void *udata,
                                                   size_t *actual_len);
static void  *H5HG__cache_heap_deserialize(const void *image, size_t len, void *udata, bool *dirty);
static herr_t H5HG__cache_heap_image_len(const void *thing, size_t *image_len);
static herr_t H5HG__cache_heap_serialize(const H5F_t *f, void *image, size_t len, void *thing);
static herr_t H5HG__cache_heap_free_icr(void *thing);

/* Prefix deserialization */
static herr_t H5HG__hdr_deserialize(H5HG_heap_t *heap, const uint8_t *image, size_t len, const H5F_t *f);

/*********************/
/* Package Variables */
/*********************/

/* H5HG inherits cache-like properties from H5AC */
const H5AC_class_t H5AC_GHEAP[1] = {{
    H5AC_GHEAP_ID,                          /* Metadata client ID */
    "global heap",                          /* Metadata client name (for debugging) */
    H5FD_MEM_GHEAP,                         /* File space memory type for client */
    H5AC__CLASS_SPECULATIVE_LOAD_FLAG,      /* Client class behavior flags */
    H5HG__cache_heap_get_initial_load_size, /* 'get_initial_load_size' callback */
    H5HG__cache_heap_get_final_load_size,   /* 'get_final_load_size' callback */
    NULL,                                   /* 'verify_chksum' callback */
    H5HG__cache_heap_deserialize,           /* 'deserialize' callback */
    H5HG__cache_heap_image_len,             /* 'image_len' callback */
    NULL,                                   /* 'pre_serialize' callback */
    H5HG__cache_heap_serialize,             /* 'serialize' callback */
    NULL,                                   /* 'notify' callback */
    H5HG__cache_heap_free_icr,              /* 'free_icr' callback */
    NULL,                                   /* 'fsf_size' callback */
}};

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:    H5HG__hdr_deserialize
 *
 * Purpose:     Decode a global heap's header
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5HG__hdr_deserialize(H5HG_heap_t *heap, const uint8_t *image, size_t len, const H5F_t *f)
{
    const uint8_t *p_end     = image + len - 1; /* End of image buffer */
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(heap);
    assert(image);
    assert(f);

    /* Magic number */
    if (H5_IS_BUFFER_OVERFLOW(image, H5_SIZEOF_MAGIC, p_end))
        HGOTO_ERROR(H5E_HEAP, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
    if (memcmp(image, H5HG_MAGIC, (size_t)H5_SIZEOF_MAGIC) != 0)
        HGOTO_ERROR(H5E_HEAP, H5E_BADVALUE, FAIL, "bad global heap collection signature");
    image += H5_SIZEOF_MAGIC;

    /* Version */
    if (H5_IS_BUFFER_OVERFLOW(image, 1, p_end))
        HGOTO_ERROR(H5E_HEAP, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
    if (H5HG_VERSION != *image++)
        HGOTO_ERROR(H5E_HEAP, H5E_VERSION, FAIL, "wrong version number in global heap");

    /* Reserved */
    if (H5_IS_BUFFER_OVERFLOW(image, 3, p_end))
        HGOTO_ERROR(H5E_HEAP, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
    image += 3;

    /* Size */
    if (H5_IS_BUFFER_OVERFLOW(image, H5F_sizeof_size(f), p_end))
        HGOTO_ERROR(H5E_HEAP, H5E_OVERFLOW, FAIL, "ran off end of input buffer while decoding");
    H5F_DECODE_LENGTH(f, image, heap->size);
    if (heap->size < H5HG_MINSIZE)
        HGOTO_ERROR(H5E_HEAP, H5E_BADVALUE, FAIL, "global heap size is too small");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HG__hdr_deserialize() */

/*-------------------------------------------------------------------------
 * Function:    H5HG__cache_heap_get_initial_load_size
 *
 * Purpose:     Return the initial speculative read size to the metadata
 *              cache.  This size will be used in the initial attempt to read
 *              the global heap.  If this read is too small, the cache will
 *              try again with the correct value obtained from
 *              H5HG__cache_get_final_load_size().
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5HG__cache_heap_get_initial_load_size(void H5_ATTR_UNUSED *_udata, size_t *image_len)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(image_len);

    *image_len = H5HG_MINSIZE;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HG__cache_heap_get_initial_load_size() */

/*-------------------------------------------------------------------------
 * Function:    H5HG__cache_heap_get_final_load_size
 *
 * Purpose:     Return the final read size for a speculatively ready heap to
 *              the metadata cache.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5HG__cache_heap_get_final_load_size(const void *image, size_t image_len, void *udata, size_t *actual_len)
{
    H5HG_heap_t heap;
    herr_t      ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(image);
    assert(udata);
    assert(actual_len);
    assert(*actual_len == image_len);
    assert(image_len == H5HG_MINSIZE);

    /* Deserialize the heap's header */
    heap.size = 0;
    if (H5HG__hdr_deserialize(&heap, (const uint8_t *)image, image_len, (const H5F_t *)udata) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTDECODE, FAIL, "can't decode global heap prefix");

    /* Set the actual global heap size */
    *actual_len = heap.size;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HG__cache_heap_get_final_load_size() */

/*-------------------------------------------------------------------------
 * Function:    H5HG__cache_heap_deserialize
 *
 * Purpose:     Given a buffer containing the on disk image of the global
 *              heap, deserialize it, load its contents into a newly allocated
 *              instance of H5HG_heap_t, and return a pointer to the new
 *              instance.
 *
 * Return:      Success:    Pointer to a new global heap
 *              Failure:    NULL
 *-------------------------------------------------------------------------
 */
static void *
H5HG__cache_heap_deserialize(const void *_image, size_t len, void *_udata, bool H5_ATTR_UNUSED *dirty)
{
    H5F_t         *f         = (H5F_t *)_udata; /* File pointer */
    H5HG_heap_t   *heap      = NULL;            /* New global heap */
    uint8_t       *p         = NULL;            /* Pointer to objects in (copied) image buffer */
    const uint8_t *p_end     = NULL;            /* End of (copied) image buffer */
    size_t         max_idx   = 0;               /* Maximum heap object index seen */
    size_t         nalloc    = 0;               /* Number of objects allocated */
    void          *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    assert(_image);
    assert(len >= (size_t)H5HG_MINSIZE);
    assert(f);
    assert(dirty);

    /* Allocate a new global heap */
    if (NULL == (heap = H5FL_CALLOC(H5HG_heap_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");
    heap->shared = H5F_SHARED(f);
    if (NULL == (heap->chunk = H5FL_BLK_MALLOC(gheap_chunk, len)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Copy the image buffer into the newly allocated chunk */
    H5MM_memcpy(heap->chunk, _image, len);

    /* Set p_end
     *
     * Note that parsing moves along p / heap->chunk, so p_end
     * has to refer to the end of that buffer and NOT _image
     */
    p_end = heap->chunk + len - 1;

    /* Deserialize the heap's header */
    if (H5_IS_BUFFER_OVERFLOW(heap->chunk, H5HG_SIZEOF_HDR(f), p_end))
        HGOTO_ERROR(H5E_HEAP, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
    if (H5HG__hdr_deserialize(heap, (const uint8_t *)heap->chunk, len, f) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTDECODE, NULL, "can't decode global heap header");

    /* Decode each object */

    /* Set the p pointer to the objects in heap->chunk */
    p = heap->chunk + H5HG_SIZEOF_HDR(f);

    /* Set the number of allocated objects */
    nalloc = H5HG_NOBJS(f, heap->size);

    /* Calloc the obj array because the file format spec makes no guarantee
     * about the order of the objects, and unused slots must be set to zero.
     */
    if (NULL == (heap->obj = H5FL_SEQ_CALLOC(H5HG_obj_t, nalloc)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");
    heap->nalloc = nalloc;

    while (p < (heap->chunk + heap->size)) {

        if ((p + H5HG_SIZEOF_OBJHDR(f)) > (heap->chunk + heap->size)) {

            /* The last bit of space is too tiny for an object header, so
             * we assume that it's free space.
             */
            if (NULL != heap->obj[0].begin)
                HGOTO_ERROR(H5E_HEAP, H5E_BADVALUE, NULL, "object 0 should not be set");
            heap->obj[0].size  = (size_t)(((const uint8_t *)heap->chunk + heap->size) - p);
            heap->obj[0].begin = p;

            /* No buffer overflow check here since this just moves the pointer
             * to the end of the buffer, which was calculated above
             */
            p += heap->obj[0].size;
        }
        else {
            size_t   need = 0;  /* # bytes needed to store the object */
            unsigned idx;       /* Heap object index */
            uint8_t *begin = p; /* Pointer to start of object */

            /* Parse a normal heap entry */

            if (H5_IS_BUFFER_OVERFLOW(p, 2, p_end))
                HGOTO_ERROR(H5E_HEAP, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
            UINT16DECODE(p, idx);

            /* Check if we need more room to store heap objects */
            if (idx >= heap->nalloc) {
                size_t      new_alloc; /* New allocation number */
                H5HG_obj_t *new_obj;   /* New array of object descriptions */

                /* Determine the new number of objects to index */
                new_alloc = MAX(heap->nalloc * 2, (idx + 1));
                if (idx >= new_alloc)
                    HGOTO_ERROR(H5E_HEAP, H5E_BADVALUE, NULL, "inappropriate heap index");

                /* Reallocate array of objects */
                if (NULL == (new_obj = H5FL_SEQ_REALLOC(H5HG_obj_t, heap->obj, new_alloc)))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

                /* Clear newly allocated space */
                memset(&new_obj[heap->nalloc], 0, (new_alloc - heap->nalloc) * sizeof(heap->obj[0]));

                /* Update heap information */
                heap->nalloc = new_alloc;
                heap->obj    = new_obj;
                if (heap->nalloc <= heap->nused)
                    HGOTO_ERROR(H5E_HEAP, H5E_BADVALUE, NULL, "inappropriate # allocated slots");
            }

            /* Number of references */
            if (H5_IS_BUFFER_OVERFLOW(p, 2, p_end))
                HGOTO_ERROR(H5E_HEAP, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
            UINT16DECODE(p, heap->obj[idx].nrefs);

            /* Reserved bytes */
            if (H5_IS_BUFFER_OVERFLOW(p, 4, p_end))
                HGOTO_ERROR(H5E_HEAP, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
            p += 4;

            /* Object length */
            if (H5_IS_BUFFER_OVERFLOW(p, H5F_sizeof_size(f), p_end))
                HGOTO_ERROR(H5E_HEAP, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
            H5F_DECODE_LENGTH(f, p, heap->obj[idx].size);

            /* Object
             *
             * Points to beginning of object, INCLUDING the header.
             */
            heap->obj[idx].begin = begin;

            /* The total storage size includes the size of the object
             * header and is zero padded so the next object header is
             * properly aligned. The entire obj array was calloc'ed,
             * so no need to zero the space here. The last bit of space
             * is the free space object whose size is never padded and
             * already includes the object header.
             */
            if (idx > 0) {
                need = H5HG_SIZEOF_OBJHDR(f) + H5HG_ALIGN(heap->obj[idx].size);
                if (idx > max_idx)
                    max_idx = idx;
            }
            else
                need = heap->obj[idx].size;

            /* Make sure the extra padding doesn't cause us to overrun
             * the buffer
             */
            if (H5_IS_BUFFER_OVERFLOW(begin, need, p_end))
                HGOTO_ERROR(H5E_HEAP, H5E_OVERFLOW, NULL, "ran off end of input buffer while decoding");
            p = begin + need;
        }
    }

    /* Post-parse checks */
    if (p != heap->chunk + heap->size)
        HGOTO_ERROR(H5E_HEAP, H5E_BADVALUE, NULL, "partially decoded global heap");
    if (false == H5HG_ISALIGNED(heap->obj[0].size))
        HGOTO_ERROR(H5E_HEAP, H5E_BADVALUE, NULL, "decoded global heap is not aligned");

    /* Set the next index value to use when creating a new object */
    if (max_idx > 0)
        heap->nused = max_idx + 1;
    else
        heap->nused = 1;

    if (max_idx >= heap->nused)
        HGOTO_ERROR(H5E_HEAP, H5E_BADVALUE, NULL, "bad `next unused` heap index value");

    /* Add the new heap to the CWFS list for the file */
    if (H5F_cwfs_add(f, heap) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, NULL, "unable to add global heap collection to file's CWFS");

    ret_value = heap;

done:
    if (!ret_value && heap)
        if (H5HG__free(heap) < 0)
            HDONE_ERROR(H5E_HEAP, H5E_CANTFREE, NULL, "unable to destroy global heap collection");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HG__cache_heap_deserialize() */

/*-------------------------------------------------------------------------
 * Function:    H5HG__cache_heap_image_len
 *
 * Purpose:     Return the on disk image size of the global heap to the
 *              metadata cache via the image_len.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5HG__cache_heap_image_len(const void *_thing, size_t *image_len)
{
    const H5HG_heap_t *heap = (const H5HG_heap_t *)_thing;

    FUNC_ENTER_PACKAGE_NOERR

    assert(heap);
    assert(heap->cache_info.type == H5AC_GHEAP);
    assert(heap->size >= H5HG_MINSIZE);
    assert(image_len);

    *image_len = heap->size;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HG__cache_heap_image_len() */

/*-------------------------------------------------------------------------
 * Function:    H5HG__cache_heap_serialize
 *
 * Purpose:     Given an appropriately sized buffer and an instance of
 *              H5HG_heap_t, serialize the global heap for writing to file,
 *              and copy the serialized version into the buffer.
 *
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5HG__cache_heap_serialize(const H5F_t H5_ATTR_NDEBUG_UNUSED *f, void *image, size_t len, void *_thing)
{
    H5HG_heap_t *heap = (H5HG_heap_t *)_thing;

    FUNC_ENTER_PACKAGE_NOERR

    assert(f);
    assert(image);
    assert(heap);
    assert(heap->cache_info.type == H5AC_GHEAP);
    assert(heap->size == len);
    assert(heap->chunk);

    /* Copy the image into the buffer */
    H5MM_memcpy(image, heap->chunk, len);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HG__cache_heap_serialize() */

/*-------------------------------------------------------------------------
 * Function:    H5HG__cache_heap_free_icr
 *
 * Purpose:     Free the in memory representation of the supplied global heap.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5HG__cache_heap_free_icr(void *_thing)
{
    H5HG_heap_t *heap      = (H5HG_heap_t *)_thing;
    herr_t       ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(heap);
    assert(heap->cache_info.type == H5AC_GHEAP);

    /* Destroy global heap collection */
    if (H5HG__free(heap) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "unable to destroy global heap collection");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HG__cache_heap_free_icr() */
