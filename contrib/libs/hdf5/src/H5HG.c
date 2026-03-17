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
 * Purpose:	Operations on the global heap.  The global heap is the set of
 *		all collections and each collection contains one or more
 *		global heap objects.  An object belongs to exactly one
 *		collection.  A collection is treated as an atomic entity for
 *		the purposes of I/O and caching.
 *
 *		Each file has a small cache of global heap collections called
 *		the CWFS list and recently accessed collections with free
 *		space appear on this list.  As collections are accessed the
 *		collection is moved toward the front of the list.  New
 *		collections are added to the front of the list while old
 *		collections are added to the end of the list.
 *
 *		The collection model reduces the overhead which would be
 *		incurred if the global heap were a single object, and the
 *		CWFS list allows the library to cheaply choose a collection
 *		for a new object based on object size, amount of free space
 *		in the collection, and temporal locality.
 */

/****************/
/* Module Setup */
/****************/

#include "H5HGmodule.h" /* This source code file is part of the H5HG module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Fprivate.h"  /* File access				*/
#include "H5HGpkg.h"     /* Global heaps				*/
#include "H5MFprivate.h" /* File memory management		*/
#include "H5MMprivate.h" /* Memory management			*/

/****************/
/* Local Macros */
/****************/

/*
 * The maximum number of links allowed to a global heap object.
 */
#define H5HG_MAXLINK 65535

/*
 * The maximum number of indices allowed in a global heap object.
 */
#define H5HG_MAXIDX 65535

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

static haddr_t H5HG__create(H5F_t *f, size_t size);
static size_t  H5HG__alloc(H5F_t *f, H5HG_heap_t *heap, size_t size, unsigned *heap_flags_ptr);

/*********************/
/* Package Variables */
/*********************/

/* Declare a free list to manage the H5HG_heap_t struct */
H5FL_DEFINE(H5HG_heap_t);

/* Declare a free list to manage sequences of H5HG_obj_t's */
H5FL_SEQ_DEFINE(H5HG_obj_t);

/* Declare a PQ free list to manage heap chunks */
H5FL_BLK_DEFINE(gheap_chunk);

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:	H5HG__create
 *
 * Purpose:	Creates a global heap collection of the specified size.  If
 *		SIZE is less than some minimum it will be readjusted.  The
 *		new collection is allocated in the file and added to the
 *		beginning of the CWFS list.
 *
 * Return:	Success:	Ptr to a cached heap.  The pointer is valid
 *				only until some other hdf5 library function
 *				is called.
 *
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5HG__create(H5F_t *f, size_t size)
{
    H5HG_heap_t *heap = NULL;
    uint8_t     *p    = NULL;
    haddr_t      addr = HADDR_UNDEF;
    size_t       n;
    haddr_t      ret_value = HADDR_UNDEF; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(f);
    if (size < H5HG_MINSIZE)
        size = H5HG_MINSIZE;
    size = H5HG_ALIGN(size);

    /* Create it */
    H5_CHECK_OVERFLOW(size, size_t, hsize_t);
    if (HADDR_UNDEF == (addr = H5MF_alloc(f, H5FD_MEM_GHEAP, (hsize_t)size)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, HADDR_UNDEF, "unable to allocate file space for global heap");
    if (NULL == (heap = H5FL_CALLOC(H5HG_heap_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, HADDR_UNDEF, "memory allocation failed");
    heap->addr   = addr;
    heap->size   = size;
    heap->shared = H5F_SHARED(f);

    if (NULL == (heap->chunk = H5FL_BLK_MALLOC(gheap_chunk, size)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, HADDR_UNDEF, "memory allocation failed");
    memset(heap->chunk, 0, size);
    heap->nalloc = H5HG_NOBJS(f, size);
    heap->nused  = 1; /* account for index 0, which is used for the free object */
    if (NULL == (heap->obj = H5FL_SEQ_MALLOC(H5HG_obj_t, heap->nalloc)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, HADDR_UNDEF, "memory allocation failed");

    /* Initialize the header */
    H5MM_memcpy(heap->chunk, H5HG_MAGIC, (size_t)H5_SIZEOF_MAGIC);
    p    = heap->chunk + H5_SIZEOF_MAGIC;
    *p++ = H5HG_VERSION;
    *p++ = 0; /*reserved*/
    *p++ = 0; /*reserved*/
    *p++ = 0; /*reserved*/
    H5F_ENCODE_LENGTH(f, p, size);

    /*
     * Padding so free space object is aligned. If malloc returned memory
     * which was always at least H5HG_ALIGNMENT aligned then we could just
     * align the pointer, but this might not be the case.
     */
    n = (size_t)H5HG_ALIGN(p - heap->chunk) - (size_t)(p - heap->chunk);
    p += n;

    /* The freespace object */
    heap->obj[0].size = size - H5HG_SIZEOF_HDR(f);
    assert(H5HG_ISALIGNED(heap->obj[0].size));
    heap->obj[0].nrefs = 0;
    heap->obj[0].begin = p;
    UINT16ENCODE(p, 0); /*object ID*/
    UINT16ENCODE(p, 0); /*reference count*/
    UINT32ENCODE(p, 0); /*reserved*/
    H5F_ENCODE_LENGTH(f, p, heap->obj[0].size);

    /* Add this heap to the beginning of the CWFS list */
    if (H5F_cwfs_add(f, heap) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, HADDR_UNDEF,
                    "unable to add global heap collection to file's CWFS");

    /* Add the heap to the cache */
    if (H5AC_insert_entry(f, H5AC_GHEAP, addr, heap, H5AC__NO_FLAGS_SET) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, HADDR_UNDEF, "unable to cache global heap collection");

    ret_value = addr;

done:
    /* Cleanup on error */
    if (!H5_addr_defined(ret_value)) {
        if (H5_addr_defined(addr)) {
            /* Release the space on disk */
            if (H5MF_xfree(f, H5FD_MEM_GHEAP, addr, (hsize_t)size) < 0)
                HDONE_ERROR(H5E_BTREE, H5E_CANTFREE, HADDR_UNDEF, "unable to free global heap");

            /* Check if the heap object was allocated */
            if (heap)
                /* Destroy the heap object */
                if (H5HG__free(heap) < 0)
                    HDONE_ERROR(H5E_HEAP, H5E_CANTFREE, HADDR_UNDEF,
                                "unable to destroy global heap collection");
        } /* end if */
    }     /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HG__create() */

/*-------------------------------------------------------------------------
 * Function:	H5HG__protect
 *
 * Purpose:	Convenience wrapper around H5AC_protect on an indirect block
 *
 * Return:	Pointer to indirect block on success, NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5HG_heap_t *
H5HG__protect(H5F_t *f, haddr_t addr, unsigned flags)
{
    H5HG_heap_t *heap;             /* Global heap */
    H5HG_heap_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(f);
    assert(H5_addr_defined(addr));

    /* only H5AC__READ_ONLY_FLAG may appear in flags */
    assert((flags & (unsigned)(~H5AC__READ_ONLY_FLAG)) == 0);

    /* Lock the heap into memory */
    if (NULL == (heap = (H5HG_heap_t *)H5AC_protect(f, H5AC_GHEAP, addr, f, flags)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, NULL, "unable to protect global heap");

    /* Set the heap's address */
    heap->addr = addr;

    /* Set the return value */
    ret_value = heap;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HG__protect() */

/*-------------------------------------------------------------------------
 * Function:	H5HG__alloc
 *
 * Purpose:	Given a heap with enough free space, this function will split
 *		the free space to make a new empty heap object and initialize
 *		the header.  SIZE is the exact size of the object data to be
 *		stored. It will be increased to make room for the object
 *		header and then rounded up for alignment.
 *
 * Return:	Success:	The heap object ID of the new object.
 *
 *		Failure:	0
 *
 *-------------------------------------------------------------------------
 */
static size_t
H5HG__alloc(H5F_t *f, H5HG_heap_t *heap, size_t size, unsigned *heap_flags_ptr)
{
    size_t   idx;
    uint8_t *p;
    size_t   need      = H5HG_SIZEOF_OBJHDR(f) + H5HG_ALIGN(size);
    size_t   ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(heap);
    assert(heap->obj[0].size >= need);
    assert(heap_flags_ptr);

    /*
     * Find an ID for the new object. ID zero is reserved for the free space
     * object.
     */
    if (heap->nused <= H5HG_MAXIDX)
        idx = heap->nused++;
    else {
        for (idx = 1; idx < heap->nused; idx++)
            if (NULL == heap->obj[idx].begin)
                break;
    } /* end else */

    assert(idx < heap->nused);

    /* Check if we need more room to store heap objects */
    if (idx >= heap->nalloc) {
        size_t      new_alloc; /* New allocation number */
        H5HG_obj_t *new_obj;   /* New array of object descriptions */

        /* Determine the new number of objects to index */
        /* nalloc is *not* guaranteed to be a power of 2! - NAF 10/26/09 */
        new_alloc = MIN(MAX(heap->nalloc * 2, (idx + 1)), (H5HG_MAXIDX + 1));
        assert(idx < new_alloc);

        /* Reallocate array of objects */
        if (NULL == (new_obj = H5FL_SEQ_REALLOC(H5HG_obj_t, heap->obj, new_alloc)))
            HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, 0, "memory allocation failed");

        /* Clear newly allocated space */
        memset(&new_obj[heap->nalloc], 0, (new_alloc - heap->nalloc) * sizeof(heap->obj[0]));

        /* Update heap information */
        heap->nalloc = new_alloc;
        heap->obj    = new_obj;
        assert(heap->nalloc > heap->nused);
    } /* end if */

    /* Initialize the new object */
    heap->obj[idx].nrefs = 0;
    heap->obj[idx].size  = size;
    heap->obj[idx].begin = heap->obj[0].begin;
    p                    = heap->obj[idx].begin;
    UINT16ENCODE(p, idx);
    UINT16ENCODE(p, 0); /*nrefs*/
    UINT32ENCODE(p, 0); /*reserved*/
    H5F_ENCODE_LENGTH(f, p, size);

    /* Fix the free space object */
    if (need == heap->obj[0].size) {
        /*
         * All free space has been exhausted from this collection.
         */
        heap->obj[0].size  = 0;
        heap->obj[0].begin = NULL;
    } /* end if */
    else if (heap->obj[0].size - need >= H5HG_SIZEOF_OBJHDR(f)) {
        /*
         * Some free space remains and it's larger than a heap object header,
         * so write the new free heap object header to the heap.
         */
        heap->obj[0].size -= need;
        heap->obj[0].begin += need;
        p = heap->obj[0].begin;
        UINT16ENCODE(p, 0); /*id*/
        UINT16ENCODE(p, 0); /*nrefs*/
        UINT32ENCODE(p, 0); /*reserved*/
        H5F_ENCODE_LENGTH(f, p, heap->obj[0].size);
        assert(H5HG_ISALIGNED(heap->obj[0].size));
    } /* end else-if */
    else {
        /*
         * Some free space remains but it's smaller than a heap object header,
         * so we don't write the header.
         */
        heap->obj[0].size -= need;
        heap->obj[0].begin += need;
        assert(H5HG_ISALIGNED(heap->obj[0].size));
    }

    /* Mark the heap as dirty */
    *heap_flags_ptr |= H5AC__DIRTIED_FLAG;

    /* Set the return value */
    ret_value = idx;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HG__alloc() */

/*-------------------------------------------------------------------------
 * Function:	H5HG_extend
 *
 * Purpose:	Extend a heap to hold an object of SIZE bytes.
 *		SIZE is the exact size of the object data to be
 *		stored. It will be increased to make room for the object
 *		header and then rounded up for alignment.
 *
 * Return:	Success:	Non-negative
 *
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HG_extend(H5F_t *f, haddr_t addr, size_t need)
{
    H5HG_heap_t *heap       = NULL;               /* Pointer to heap to extend */
    unsigned     heap_flags = H5AC__NO_FLAGS_SET; /* Flags to unprotecting heap */
    size_t       old_size;                        /* Previous size of the heap's chunk */
    uint8_t     *new_chunk;                       /* Pointer to new chunk information */
    uint8_t     *p;                               /* Pointer to raw heap info */
    unsigned     u;                               /* Local index variable */
    herr_t       ret_value = SUCCEED;             /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Check args */
    assert(f);
    assert(H5_addr_defined(addr));

    /* Protect the heap */
    if (NULL == (heap = H5HG__protect(f, addr, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to protect global heap");

    /* Re-allocate the heap information in memory */
    if (NULL == (new_chunk = H5FL_BLK_REALLOC(gheap_chunk, heap->chunk, (heap->size + need))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "new heap allocation failed");
    memset(new_chunk + heap->size, 0, need);

    /* Adjust the size of the heap */
    old_size = heap->size;
    heap->size += need;

    /* Encode the new size of the heap */
    p = new_chunk + H5_SIZEOF_MAGIC + 1 /* version */ + 3 /* reserved */;
    H5F_ENCODE_LENGTH(f, p, heap->size);

    /* Move the pointers to the existing objects to their new locations */
    for (u = 0; u < heap->nused; u++)
        if (heap->obj[u].begin)
            heap->obj[u].begin = new_chunk + (heap->obj[u].begin - heap->chunk);

    /* Update the heap chunk pointer now */
    heap->chunk = new_chunk;

    /* Update the free space information for the heap  */
    heap->obj[0].size += need;
    if (heap->obj[0].begin == NULL)
        heap->obj[0].begin = heap->chunk + old_size;
    p = heap->obj[0].begin;
    UINT16ENCODE(p, 0); /*id*/
    UINT16ENCODE(p, 0); /*nrefs*/
    UINT32ENCODE(p, 0); /*reserved*/
    H5F_ENCODE_LENGTH(f, p, heap->obj[0].size);
    assert(H5HG_ISALIGNED(heap->obj[0].size));

    /* Resize the heap in the cache */
    if (H5AC_resize_entry(heap, heap->size) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTRESIZE, FAIL, "unable to resize global heap in cache");

    /* Mark the heap as dirty */
    heap_flags |= H5AC__DIRTIED_FLAG;

done:
    if (heap && H5AC_unprotect(f, H5AC_GHEAP, heap->addr, heap, heap_flags) < 0)
        HDONE_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to unprotect heap");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HG_extend() */

/*-------------------------------------------------------------------------
 * Function:	H5HG_insert
 *
 * Purpose:	A new object is inserted into the global heap.  It will be
 *		placed in the first collection on the CWFS list which has
 *		enough free space and that collection will be advanced one
 *		position in the list.  If no collection on the CWFS list has
 *		enough space then  a new collection will be created.
 *
 *		It is legal to push a zero-byte object onto the heap to get
 *		the reference count features of heap objects.
 *
 * Return:	Success:	Non-negative, and a heap object handle returned
 *				through the HOBJ pointer.
 *
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HG_insert(H5F_t *f, size_t size, const void *obj, H5HG_t *hobj /*out*/)
{
    size_t       need; /*total space needed for object		*/
    size_t       idx;
    haddr_t      addr; /* Address of heap to add object within */
    H5HG_heap_t *heap       = NULL;
    unsigned     heap_flags = H5AC__NO_FLAGS_SET;
    herr_t       ret_value  = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_TAG(H5AC__GLOBALHEAP_TAG, FAIL)

    /* Check args */
    assert(f);
    assert(0 == size || obj);
    assert(hobj);

    if (0 == (H5F_INTENT(f) & H5F_ACC_RDWR))
        HGOTO_ERROR(H5E_HEAP, H5E_WRITEERROR, FAIL, "no write intent on file");

    /* Find a large enough collection on the CWFS list */
    need = H5HG_SIZEOF_OBJHDR(f) + H5HG_ALIGN(size);

    /* Look for a heap in the file's CWFS that has enough space for the object */
    addr = HADDR_UNDEF;
    if (H5F_cwfs_find_free_heap(f, need, &addr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_NOTFOUND, FAIL, "error trying to locate heap");

    /*
     * If we didn't find any collection with enough free space then allocate a
     * new collection large enough for the message plus the collection header.
     */
    if (!H5_addr_defined(addr)) {
        addr = H5HG__create(f, need + H5HG_SIZEOF_HDR(f));

        if (!H5_addr_defined(addr))
            HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "unable to allocate a global heap collection");
    } /* end if */
    assert(H5_addr_defined(addr));

    if (NULL == (heap = H5HG__protect(f, addr, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to protect global heap");

    /* Split the free space to make room for the new object */
    if (0 == (idx = H5HG__alloc(f, heap, size, &heap_flags)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, FAIL, "unable to allocate global heap object");

    /* Copy data into the heap */
    if (size > 0)
        H5MM_memcpy(heap->obj[idx].begin + H5HG_SIZEOF_OBJHDR(f), obj, size);
    heap_flags |= H5AC__DIRTIED_FLAG;

    /* Return value */
    hobj->addr = heap->addr;
    hobj->idx  = idx;

done:
    if (heap && H5AC_unprotect(f, H5AC_GHEAP, heap->addr, heap, heap_flags) < 0)
        HDONE_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to unprotect heap.");

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* H5HG_insert() */

/*-------------------------------------------------------------------------
 * Function:	H5HG_read
 *
 * Purpose:	Reads the specified global heap object into the buffer OBJECT
 *		supplied by the caller.  If the caller doesn't supply a
 *		buffer then one will be allocated.  The buffer should be
 *		large enough to hold the result.
 *
 * Return:	Success:	The buffer containing the result.
 *
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5HG_read(H5F_t *f, H5HG_t *hobj, void *object /*out*/, size_t *buf_size)
{
    H5HG_heap_t *heap = NULL;          /* Pointer to global heap object */
    size_t       size;                 /* Size of the heap object */
    uint8_t     *p;                    /* Pointer to object in heap buffer */
    void        *orig_object = object; /* Keep a copy of the original object pointer */
    void        *ret_value   = NULL;   /* Return value */

    FUNC_ENTER_NOAPI_TAG(H5AC__GLOBALHEAP_TAG, NULL)

    /* Check args */
    assert(f);
    assert(hobj);

    /* Load the heap */
    if (NULL == (heap = H5HG__protect(f, hobj->addr, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, NULL, "unable to protect global heap");

    assert(hobj->idx < heap->nused);
    assert(heap->obj[hobj->idx].begin);
    size = heap->obj[hobj->idx].size;
    p    = heap->obj[hobj->idx].begin + H5HG_SIZEOF_OBJHDR(f);

    /* Allocate a buffer for the object read in, if the user didn't give one */
    if (!object && NULL == (object = H5MM_malloc(size)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");
    H5MM_memcpy(object, p, size);

    /*
     * Advance the heap in the CWFS list. We might have done this already
     * with the H5AC_protect(), but it won't hurt to do it twice.
     */
    if (heap->obj[0].begin) {
        if (H5F_cwfs_advance_heap(f, heap, false) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTMODIFY, NULL, "can't adjust file's CWFS");
    } /* end if */

    /* If the caller would like to know the heap object's size, set that */
    if (buf_size)
        *buf_size = size;

    /* Set return value */
    ret_value = object;

done:
    if (heap && H5AC_unprotect(f, H5AC_GHEAP, hobj->addr, heap, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, NULL, "unable to release object header");

    if (NULL == ret_value && NULL == orig_object && object)
        H5MM_free(object);

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5HG_read() */

/*-------------------------------------------------------------------------
 * Function:	H5HG_link
 *
 * Purpose:	Adjusts the link count for a global heap object by adding
 *		ADJUST to the current value.  This function will fail if the
 *		new link count would overflow.  Nothing special happens when
 *		the link count reaches zero; in order for a heap object to be
 *		removed one must call H5HG_remove().
 *
 * Return:	Success:	Number of links present after the adjustment.
 *
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
int
H5HG_link(H5F_t *f, const H5HG_t *hobj, int adjust)
{
    H5HG_heap_t *heap       = NULL;
    unsigned     heap_flags = H5AC__NO_FLAGS_SET;
    int          ret_value  = -1; /* Return value */

    FUNC_ENTER_NOAPI_TAG(H5AC__GLOBALHEAP_TAG, FAIL)

    /* Check args */
    assert(f);
    assert(hobj);
    if (0 == (H5F_INTENT(f) & H5F_ACC_RDWR))
        HGOTO_ERROR(H5E_HEAP, H5E_WRITEERROR, FAIL, "no write intent on file");

    /* Load the heap */
    if (NULL == (heap = H5HG__protect(f, hobj->addr, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to protect global heap");

    if (adjust != 0) {
        assert(hobj->idx < heap->nused);
        assert(heap->obj[hobj->idx].begin);
        if ((heap->obj[hobj->idx].nrefs + adjust) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_BADRANGE, FAIL, "new link count would be out of range");
        if ((heap->obj[hobj->idx].nrefs + adjust) > H5HG_MAXLINK)
            HGOTO_ERROR(H5E_HEAP, H5E_BADVALUE, FAIL, "new link count would be out of range");
        heap->obj[hobj->idx].nrefs += adjust;
        heap_flags |= H5AC__DIRTIED_FLAG;
    } /* end if */

    /* Set return value */
    ret_value = heap->obj[hobj->idx].nrefs;

done:
    if (heap && H5AC_unprotect(f, H5AC_GHEAP, hobj->addr, heap, heap_flags) < 0)
        HDONE_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to release object header");

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5HG_link() */

/*-------------------------------------------------------------------------
 * Function:    H5HG_get_obj_size
 *
 * Purpose:     Returns the size of a global heap object.
 * Return:      Success:        Non-negative
 *
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HG_get_obj_size(H5F_t *f, H5HG_t *hobj, size_t *obj_size)
{
    H5HG_heap_t *heap      = NULL;    /* Pointer to global heap object */
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_TAG(H5AC__GLOBALHEAP_TAG, FAIL)

    /* Check args */
    assert(f);
    assert(hobj);
    assert(obj_size);

    /* Load the heap */
    if (NULL == (heap = H5HG__protect(f, hobj->addr, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to protect global heap");

    assert(hobj->idx < heap->nused);
    assert(heap->obj[hobj->idx].begin);

    /* Set object size */
    *obj_size = heap->obj[hobj->idx].size;

done:
    if (heap && H5AC_unprotect(f, H5AC_GHEAP, hobj->addr, heap, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to release object header");

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5HG_get_obj_size() */

/*-------------------------------------------------------------------------
 * Function:	H5HG_remove
 *
 * Purpose:	Removes the specified object from the global heap.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HG_remove(H5F_t *f, H5HG_t *hobj)
{
    H5HG_heap_t *heap = NULL;
    uint8_t     *p = NULL, *obj_start = NULL;
    size_t       need;
    unsigned     u;
    unsigned     flags     = H5AC__NO_FLAGS_SET; /* Whether the heap gets deleted */
    herr_t       ret_value = SUCCEED;            /* Return value */

    FUNC_ENTER_NOAPI_TAG(H5AC__GLOBALHEAP_TAG, FAIL)

    /* Check args */
    assert(f);
    assert(hobj);
    if (0 == (H5F_INTENT(f) & H5F_ACC_RDWR))
        HGOTO_ERROR(H5E_HEAP, H5E_WRITEERROR, FAIL, "no write intent on file");

    /* Load the heap */
    if (NULL == (heap = H5HG__protect(f, hobj->addr, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to protect global heap");

    assert(hobj->idx < heap->nused);

    /* When the application selects the same location to rewrite the VL element by using H5Sselect_elements,
     * it can happen that the entry has been removed by first rewrite.  Here we simply skip the removal of
     * the entry and let the second rewrite happen (see HDFFV-10635).  In the future, it'd be nice to handle
     * this situation in H5T_conv_vlen in H5Tconv.c instead of this level (HDFFV-10648). */
    if (heap->obj[hobj->idx].nrefs == 0 && heap->obj[hobj->idx].size == 0 && !heap->obj[hobj->idx].begin)
        HGOTO_DONE(ret_value);

    obj_start = heap->obj[hobj->idx].begin;
    /* Include object header size */
    need = H5HG_ALIGN(heap->obj[hobj->idx].size) + H5HG_SIZEOF_OBJHDR(f);

    /* Move the new free space to the end of the heap */
    for (u = 0; u < heap->nused; u++)
        if (heap->obj[u].begin > heap->obj[hobj->idx].begin)
            heap->obj[u].begin -= need;
    if (NULL == heap->obj[0].begin) {
        heap->obj[0].begin = heap->chunk + (heap->size - need);
        heap->obj[0].size  = need;
        heap->obj[0].nrefs = 0;
    } /* end if */
    else
        heap->obj[0].size += need;
    memmove(obj_start, obj_start + need, heap->size - (size_t)((obj_start + need) - heap->chunk));
    if (heap->obj[0].size >= H5HG_SIZEOF_OBJHDR(f)) {
        p = heap->obj[0].begin;
        UINT16ENCODE(p, 0); /*id*/
        UINT16ENCODE(p, 0); /*nrefs*/
        UINT32ENCODE(p, 0); /*reserved*/
        H5F_ENCODE_LENGTH(f, p, heap->obj[0].size);
    } /* end if */
    memset(heap->obj + hobj->idx, 0, sizeof(H5HG_obj_t));
    flags |= H5AC__DIRTIED_FLAG;

    if ((heap->obj[0].size + H5HG_SIZEOF_HDR(f)) == heap->size) {
        /*
         * The collection is empty. Remove it from the CWFS list and return it
         * to the file free list.
         */
        flags |=
            H5AC__DELETED_FLAG |
            H5AC__FREE_FILE_SPACE_FLAG; /* Indicate that the object was deleted, for the unprotect call */
    }                                   /* end if */
    else {
        /*
         * If the heap is in the CWFS list then advance it one position.  The
         * H5AC_protect() might have done that too, but that's okay.  If the
         * heap isn't on the CWFS list then add it to the end.
         */
        if (H5F_cwfs_advance_heap(f, heap, true) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTMODIFY, FAIL, "can't adjust file's CWFS");
    } /* end else */

done:
    if (heap && H5AC_unprotect(f, H5AC_GHEAP, hobj->addr, heap, flags) < 0)
        HDONE_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to release object header");

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5HG_remove() */

/*-------------------------------------------------------------------------
 * Function:    H5HG__free
 *
 * Purpose:     Destroys a global heap collection in memory
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HG__free(H5HG_heap_t *heap)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments */
    assert(heap);

    /* Remove the heap from the CWFS list */
    if (H5F_cwfs_remove_heap(heap->shared, heap) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTREMOVE, FAIL, "can't remove heap from file's CWFS");

    if (heap->chunk)
        heap->chunk = H5FL_BLK_FREE(gheap_chunk, heap->chunk);
    if (heap->obj)
        heap->obj = H5FL_SEQ_FREE(H5HG_obj_t, heap->obj);
    heap = H5FL_FREE(H5HG_heap_t, heap);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HG__free() */
