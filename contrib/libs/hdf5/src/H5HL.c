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
 * Created:         H5HL.c
 *
 * Purpose:         Heap functions for the local heaps used by symbol
 *                  tables to store names (among other things).
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5HLmodule.h" /* This source code file is part of the H5HL module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5ACprivate.h" /* Metadata Cache                           */
#include "H5Eprivate.h"  /* Error Handling                           */
#include "H5Fprivate.h"  /* Files                                    */
#include "H5FLprivate.h" /* Free Lists                               */
#include "H5HLpkg.h"     /* Local Heaps                              */
#include "H5MFprivate.h" /* File Memory Management                   */
#include "H5MMprivate.h" /* Memory Management                        */

/****************/
/* Local Macros */
/****************/

#define H5HL_MIN_HEAP 128 /* Minimum size to reduce heap buffer to */

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

static H5HL_free_t *H5HL__remove_free(H5HL_t *heap, H5HL_free_t *fl);
static herr_t       H5HL__minimize_heap_space(H5F_t *f, H5HL_t *heap);
static herr_t       H5HL__dirty(H5HL_t *heap);

/*********************/
/* Package Variables */
/*********************/

/* Declare a free list to manage the H5HL_free_t struct */
H5FL_DEFINE(H5HL_free_t);

/* Declare a PQ free list to manage the heap chunk information */
H5FL_BLK_DEFINE(lheap_chunk);

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:    H5HL_create
 *
 * Purpose:     Creates a new heap data structure on disk and caches it
 *              in memory.  SIZE_HINT is a hint for the initial size of the
 *              data area of the heap.  If size hint is invalid then a
 *              reasonable (but probably not optimal) size will be chosen.
 *
 * Return:      Success:    SUCCEED. The file address of new heap is
 *                          returned through the ADDR argument.
 *              Failure:    FAIL.  addr_p will be HADDR_UNDEF.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HL_create(H5F_t *f, size_t size_hint, haddr_t *addr_p /*out*/)
{
    H5HL_t      *heap       = NULL; /* Heap created                 */
    H5HL_prfx_t *prfx       = NULL; /* Heap prefix                  */
    hsize_t      total_size = 0;    /* Total heap size on disk      */
    herr_t       ret_value  = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* check arguments */
    assert(f);
    assert(addr_p);

    /* Adjust size hint as necessary */
    if (size_hint && size_hint < H5HL_SIZEOF_FREE(f))
        size_hint = H5HL_SIZEOF_FREE(f);
    size_hint = H5HL_ALIGN(size_hint);

    /* Allocate new heap structure */
    if (NULL == (heap = H5HL__new(H5F_SIZEOF_SIZE(f), H5F_SIZEOF_ADDR(f), H5HL_SIZEOF_HDR(f))))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, FAIL, "can't allocate new heap struct");

    /* Allocate file space */
    total_size = heap->prfx_size + size_hint;
    if (HADDR_UNDEF == (heap->prfx_addr = H5MF_alloc(f, H5FD_MEM_LHEAP, total_size)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, FAIL, "unable to allocate file memory");

    /* Initialize info */
    heap->single_cache_obj = true;
    heap->dblk_addr        = heap->prfx_addr + (hsize_t)heap->prfx_size;
    heap->dblk_size        = size_hint;
    if (size_hint)
        if (NULL == (heap->dblk_image = H5FL_BLK_CALLOC(lheap_chunk, size_hint)))
            HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, FAIL, "memory allocation failed");

    /* free list */
    if (size_hint) {
        if (NULL == (heap->freelist = H5FL_MALLOC(H5HL_free_t)))
            HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, FAIL, "memory allocation failed");
        heap->freelist->offset = 0;
        heap->freelist->size   = size_hint;
        heap->freelist->prev = heap->freelist->next = NULL;
        heap->free_block                            = 0;
    }
    else {
        heap->freelist   = NULL;
        heap->free_block = H5HL_FREE_NULL;
    }

    /* Allocate the heap prefix */
    if (NULL == (prfx = H5HL__prfx_new(heap)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, FAIL, "memory allocation failed");

    /* Add to cache */
    if (FAIL == H5AC_insert_entry(f, H5AC_LHEAP_PRFX, heap->prfx_addr, prfx, H5AC__NO_FLAGS_SET))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "unable to cache local heap prefix");

    /* Set address to return */
    *addr_p = heap->prfx_addr;

done:
    if (ret_value < 0) {
        *addr_p = HADDR_UNDEF;
        if (prfx) {
            if (FAIL == H5HL__prfx_dest(prfx))
                HDONE_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "unable to destroy local heap prefix");
        }
        else {
            if (heap) {
                if (H5_addr_defined(heap->prfx_addr))
                    if (FAIL == H5MF_xfree(f, H5FD_MEM_LHEAP, heap->prfx_addr, total_size))
                        HDONE_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "can't release heap data?");
                if (FAIL == H5HL__dest(heap))
                    HDONE_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "unable to destroy local heap");
            }
        }
    }
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HL_create() */

/*-------------------------------------------------------------------------
 * Function:    H5HL__minimize_heap_space
 *
 * Purpose:     Go through the heap's freelist and determine if we can
 *              eliminate the free blocks at the tail of the buffer.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HL__minimize_heap_space(H5F_t *f, H5HL_t *heap)
{
    size_t new_heap_size = heap->dblk_size; /* New size of heap */
    herr_t ret_value     = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(f);
    assert(heap);

    /* Check to see if we can reduce the size of the heap in memory by
     * eliminating free blocks at the tail of the buffer before flushing the
     * buffer out.
     */
    if (heap->freelist) {
        H5HL_free_t *tmp_fl;
        H5HL_free_t *last_fl = NULL;

        /* Search for a free block at the end of the buffer */
        for (tmp_fl = heap->freelist; tmp_fl; tmp_fl = tmp_fl->next)
            /* Check if the end of this free block is at the end of the buffer */
            if (tmp_fl->offset + tmp_fl->size == heap->dblk_size) {
                last_fl = tmp_fl;
                break;
            }

        /* Found free block at the end of the buffer, decide what to do
         * about it
         */
        if (last_fl) {
            /* If the last free block's size is more than half the memory
             * buffer size (and the memory buffer is larger than the
             * minimum size), reduce or eliminate it.
             */
            if (last_fl->size >= (heap->dblk_size / 2) && heap->dblk_size > H5HL_MIN_HEAP) {
                /* Reduce size of buffer until it's too small or would
                 * eliminate the free block
                 */
                while (new_heap_size > H5HL_MIN_HEAP &&
                       new_heap_size >= (last_fl->offset + H5HL_SIZEOF_FREE(f)))
                    new_heap_size /= 2;

                /* Check if reducing the memory buffer size would
                 * eliminate the free block
                 */
                if (new_heap_size < (last_fl->offset + H5HL_SIZEOF_FREE(f))) {
                    /* Check if this is the only block on the free list */
                    if (last_fl->prev == NULL && last_fl->next == NULL) {
                        /* Double the new memory size */
                        new_heap_size *= 2;

                        /* Truncate the free block */
                        last_fl->size = H5HL_ALIGN(new_heap_size - last_fl->offset);
                        new_heap_size = last_fl->offset + last_fl->size;
                        assert(last_fl->size >= H5HL_SIZEOF_FREE(f));
                    }
                    else {
                        /* Set the size of the memory buffer to the start
                         * of the free list
                         */
                        new_heap_size = last_fl->offset;

                        /* Eliminate the free block from the list */
                        last_fl = H5HL__remove_free(heap, last_fl);
                    }
                }
                else {
                    /* Truncate the free block */
                    last_fl->size = H5HL_ALIGN(new_heap_size - last_fl->offset);
                    new_heap_size = last_fl->offset + last_fl->size;
                    assert(last_fl->size >= H5HL_SIZEOF_FREE(f));
                    assert(last_fl->size == H5HL_ALIGN(last_fl->size));
                }
            }
        }
    }

    /* If the heap grew smaller than disk storage then move the
     * data segment of the heap to another contiguous block of disk
     * storage.
     */
    if (new_heap_size != heap->dblk_size) {
        assert(new_heap_size < heap->dblk_size);

        /* Resize the memory buffer */
        if (NULL == (heap->dblk_image = H5FL_BLK_REALLOC(lheap_chunk, heap->dblk_image, new_heap_size)))
            HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, FAIL, "memory allocation failed");

        /* Reallocate data block in file */
        if (FAIL == H5HL__dblk_realloc(f, heap, new_heap_size))
            HGOTO_ERROR(H5E_HEAP, H5E_CANTRESIZE, FAIL, "reallocating data block failed");
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HL__minimize_heap_space() */

/*-------------------------------------------------------------------------
 * Function:    H5HL_protect
 *
 * Purpose:     This function is a wrapper for the H5AC_protect call.
 *
 * Return:      Success:    Non-NULL pointer to the local heap prefix.
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
H5HL_t *
H5HL_protect(H5F_t *f, haddr_t addr, unsigned flags)
{
    H5HL_cache_prfx_ud_t prfx_udata;                /* User data for protecting local heap prefix       */
    H5HL_prfx_t         *prfx = NULL;               /* Local heap prefix                                */
    H5HL_dblk_t         *dblk = NULL;               /* Local heap data block                            */
    H5HL_t              *heap = NULL;               /* Heap data structure                              */
    unsigned prfx_cache_flags = H5AC__NO_FLAGS_SET; /* Cache flags for unprotecting prefix entry        */
    unsigned dblk_cache_flags = H5AC__NO_FLAGS_SET; /* Cache flags for unprotecting data block entry    */
    H5HL_t  *ret_value        = NULL;

    FUNC_ENTER_NOAPI(NULL)

    /* Check arguments */
    assert(f);
    assert(H5_addr_defined(addr));

    /* Only the H5AC__READ_ONLY_FLAG may appear in flags */
    assert((flags & (unsigned)(~H5AC__READ_ONLY_FLAG)) == 0);

    /* Construct the user data for protect callback */
    prfx_udata.sizeof_size = H5F_SIZEOF_SIZE(f);
    prfx_udata.sizeof_addr = H5F_SIZEOF_ADDR(f);
    prfx_udata.prfx_addr   = addr;
    prfx_udata.sizeof_prfx = H5HL_SIZEOF_HDR(f);

    /* Protect the local heap prefix */
    if (NULL == (prfx = (H5HL_prfx_t *)H5AC_protect(f, H5AC_LHEAP_PRFX, addr, &prfx_udata, flags)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, NULL, "unable to load heap prefix");

    /* Get the pointer to the heap */
    heap = prfx->heap;

    /* Check if the heap is already pinned in memory */
    /* (for re-entrant situation) */
    if (heap->prots == 0) {
        /* Check if heap has separate data block */
        if (heap->single_cache_obj)
            /* Set the flag for pinning the prefix when unprotecting it */
            prfx_cache_flags |= H5AC__PIN_ENTRY_FLAG;
        else {
            /* Protect the local heap data block */
            if (NULL ==
                (dblk = (H5HL_dblk_t *)H5AC_protect(f, H5AC_LHEAP_DBLK, heap->dblk_addr, heap, flags)))
                HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, NULL, "unable to load heap data block");

            /* Set the flag for pinning the data block when unprotecting it */
            dblk_cache_flags |= H5AC__PIN_ENTRY_FLAG;
        }
    }

    /* Increment # of times heap is protected */
    heap->prots++;

    /* Set return value */
    ret_value = heap;

done:
    /* Release the prefix from the cache, now pinned */
    if (prfx && heap && H5AC_unprotect(f, H5AC_LHEAP_PRFX, heap->prfx_addr, prfx, prfx_cache_flags) < 0)
        HDONE_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, NULL, "unable to release local heap prefix");

    /* Release the data block from the cache, now pinned */
    if (dblk && heap && H5AC_unprotect(f, H5AC_LHEAP_DBLK, heap->dblk_addr, dblk, dblk_cache_flags) < 0)
        HDONE_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, NULL, "unable to release local heap data block");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HL_protect() */

/*-------------------------------------------------------------------------
 * Function:    H5HL_offset_into
 *
 * Purpose:     Called directly after the call to H5HL_protect so that
 *              a pointer to the object in the heap can be obtained.
 *
 * Return:      Success:    Valid pointer
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5HL_offset_into(const H5HL_t *heap, size_t offset)
{
    void *ret_value = NULL;

    FUNC_ENTER_NOAPI(NULL)

    /* Sanity check */
    assert(heap);
    if (offset >= heap->dblk_size)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, NULL, "unable to offset into local heap data block");

    ret_value = heap->dblk_image + offset;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HL_offset_into() */

/*-------------------------------------------------------------------------
 * Function:    H5HL_unprotect
 *
 * Purpose:     Unprotect the data retrieved by the H5HL_protect call.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HL_unprotect(H5HL_t *heap)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments */
    assert(heap);

    /* Decrement # of times heap is protected */
    heap->prots--;

    /* Check for last unprotection of heap */
    if (heap->prots == 0) {
        /* Check for separate heap data block */
        if (heap->single_cache_obj) {
            /* Mark local heap prefix as evictable again */
            if (FAIL == H5AC_unpin_entry(heap->prfx))
                HGOTO_ERROR(H5E_HEAP, H5E_CANTUNPIN, FAIL, "unable to unpin local heap data block");
        }
        else {
            /* Sanity check */
            assert(heap->dblk);

            /* Mark local heap data block as evictable again */
            /* (data block still pins prefix) */
            if (FAIL == H5AC_unpin_entry(heap->dblk))
                HGOTO_ERROR(H5E_HEAP, H5E_CANTUNPIN, FAIL, "unable to unpin local heap data block");
        }
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HL_unprotect() */

/*-------------------------------------------------------------------------
 * Function:    H5HL__remove_free
 *
 * Purpose:     Removes free list element FL from the specified heap and
 *              frees it.
 *
 * Return:      NULL
 *
 *-------------------------------------------------------------------------
 */
static H5HL_free_t *
H5HL__remove_free(H5HL_t *heap, H5HL_free_t *fl)
{
    H5HL_free_t *ret_value = NULL;

    FUNC_ENTER_PACKAGE_NOERR

    if (fl->prev)
        fl->prev->next = fl->next;
    if (fl->next)
        fl->next->prev = fl->prev;

    if (!fl->prev)
        heap->freelist = fl->next;

    /* H5FL_FREE always returns NULL so we can't check for errors */
    ret_value = (H5HL_free_t *)H5FL_FREE(H5HL_free_t, fl);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HL__remove_free() */

/*-------------------------------------------------------------------------
 * Function:    H5HL__dirty
 *
 * Purpose:     Mark heap as dirty
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HL__dirty(H5HL_t *heap)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(heap);
    assert(heap->prfx);

    /* Mark heap data block as dirty, if there is one */
    if (!heap->single_cache_obj) {
        /* Sanity check */
        assert(heap->dblk);

        if (FAIL == H5AC_mark_entry_dirty(heap->dblk))
            HGOTO_ERROR(H5E_HEAP, H5E_CANTMARKDIRTY, FAIL, "unable to mark heap data block as dirty");
    }

    /* Mark heap prefix as dirty */
    if (FAIL == H5AC_mark_entry_dirty(heap->prfx))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTMARKDIRTY, FAIL, "unable to mark heap prefix as dirty");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HL__dirty() */

/*-------------------------------------------------------------------------
 * Function:    H5HL_insert
 *
 * Purpose:     Inserts a new item into the heap.
 *
 * Return:      Success:    SUCCEED
 *                          Offset set to location of new item within heap
 *
 *              Failure:    FAIL
 *                          Offset set to SIZE_MAX
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HL_insert(H5F_t *f, H5HL_t *heap, size_t buf_size, const void *buf, size_t *offset_out)
{
    H5HL_free_t *fl = NULL, *last_fl = NULL;
    size_t       need_size;
    size_t       offset = 0;
    bool         found;
    herr_t       ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments */
    assert(f);
    assert(heap);
    assert(buf_size > 0);
    assert(buf);
    assert(offset_out);

    /* Mark heap as dirty in cache */
    /* (A bit early in the process, but it's difficult to determine in the
     *  code below where to mark the heap as dirty, especially in error cases,
     *  so we just accept that an extra flush of the heap info could occur
     *  if an error occurs -QAK)
     */
    if (FAIL == H5HL__dirty(heap))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTMARKDIRTY, FAIL, "unable to mark heap as dirty");

    /* In order to keep the free list descriptors aligned on word boundaries,
     * whatever that might mean, we round the size up to the next multiple of
     * a word.
     */
    need_size = H5HL_ALIGN(buf_size);

    /* Look for a free slot large enough for this object and which would
     * leave zero or at least H5G_SIZEOF_FREE bytes left over.
     */
    for (fl = heap->freelist, found = false; fl; fl = fl->next) {
        if (fl->size > need_size && fl->size - need_size >= H5HL_SIZEOF_FREE(f)) {
            /* A big enough free block was found */
            offset = fl->offset;
            fl->offset += need_size;
            fl->size -= need_size;
            assert(fl->offset == H5HL_ALIGN(fl->offset));
            assert(fl->size == H5HL_ALIGN(fl->size));
            found = true;
            break;
        }
        else if (fl->size == need_size) {
            /* Free block of exact size found */
            offset = fl->offset;
            fl     = H5HL__remove_free(heap, fl);
            found  = true;
            break;
        }
        else if (!last_fl || last_fl->offset < fl->offset) {
            /* Track free space that's closest to end of heap */
            last_fl = fl;
        }
    }

    /* If no free chunk was large enough, then allocate more space and
     * add it to the free list.	 If the heap ends with a free chunk, we
     * can extend that free chunk.  Otherwise we'll have to make another
     * free chunk.  If the heap must expand, we double its size.
     */
    if (found == false) {
        size_t need_more;     /* How much more space we need */
        size_t new_dblk_size; /* Final size of space allocated for heap data block */
        size_t old_dblk_size; /* Previous size of space allocated for heap data block */
        htri_t was_extended;  /* Whether the local heap's data segment on disk was extended */

        /* At least double the heap's size, making certain there's enough room
         * for the new object
         */
        need_more = MAX(need_size, heap->dblk_size);

        /* If there is no last free block or it's not at the end of the heap,
         * and the amount of space to allocate is not big enough to include at
         * least the new object and a free-list info, trim down the amount of
         * space requested to just the amount of space needed.  (Generally
         * speaking, this only occurs when the heap is small -QAK)
         */
        if (!(last_fl && last_fl->offset + last_fl->size == heap->dblk_size) &&
            (need_more < (need_size + H5HL_SIZEOF_FREE(f))))
            need_more = need_size;

        new_dblk_size = heap->dblk_size + need_more;
        assert(heap->dblk_size < new_dblk_size);
        old_dblk_size = heap->dblk_size;
        H5_CHECK_OVERFLOW(heap->dblk_size, size_t, hsize_t);
        H5_CHECK_OVERFLOW(new_dblk_size, size_t, hsize_t);

        /* Extend current heap if possible */
        was_extended = H5MF_try_extend(f, H5FD_MEM_LHEAP, heap->dblk_addr, (hsize_t)(heap->dblk_size),
                                       (hsize_t)need_more);
        if (FAIL == was_extended)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTEXTEND, FAIL, "error trying to extend heap");

        /* Check if we extended the heap data block in file */
        if (was_extended == true) {
            /* Check for prefix & data block contiguous */
            if (heap->single_cache_obj) {
                /* Resize prefix+data block */
                if (FAIL == H5AC_resize_entry(heap->prfx, (size_t)(heap->prfx_size + new_dblk_size)))
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTRESIZE, FAIL, "unable to resize heap prefix in cache");
            }
            else {
                /* Resize 'standalone' data block */
                if (FAIL == H5AC_resize_entry(heap->dblk, (size_t)new_dblk_size))
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTRESIZE, FAIL, "unable to resize heap data block in cache");
            }

            /* Note new size */
            heap->dblk_size = new_dblk_size;
        }
        else { /* ...if we can't, allocate a new chunk & release the old */
            /* Reallocate data block in file */
            if (FAIL == H5HL__dblk_realloc(f, heap, new_dblk_size))
                HGOTO_ERROR(H5E_HEAP, H5E_CANTRESIZE, FAIL, "reallocating data block failed");
        }

        /* If the last free list in the heap is at the end of the heap, extend it */
        if (last_fl && last_fl->offset + last_fl->size == old_dblk_size) {
            /*
             * Increase the size of the last free block.
             */
            offset = last_fl->offset;
            last_fl->offset += need_size;
            last_fl->size += need_more - need_size;
            assert(last_fl->offset == H5HL_ALIGN(last_fl->offset));
            assert(last_fl->size == H5HL_ALIGN(last_fl->size));

            if (last_fl->size < H5HL_SIZEOF_FREE(f)) {
#ifdef H5HL_DEBUG
                if (H5DEBUG(HL) && last_fl->size) {
                    fprintf(H5DEBUG(HL), "H5HL: lost %lu bytes at line %d\n", (unsigned long)(last_fl->size),
                            __LINE__);
                }
#endif
                last_fl = H5HL__remove_free(heap, last_fl);
            }
        }
        else {
            /* Create a new free list element large enough that we can
             * take some space out of it right away.
             */
            offset = old_dblk_size;
            if (need_more - need_size >= H5HL_SIZEOF_FREE(f)) {
                if (NULL == (fl = H5FL_MALLOC(H5HL_free_t)))
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, FAIL, "memory allocation failed");
                fl->offset = old_dblk_size + need_size;
                fl->size   = need_more - need_size;
                assert(fl->offset == H5HL_ALIGN(fl->offset));
                assert(fl->size == H5HL_ALIGN(fl->size));
                fl->prev = NULL;
                fl->next = heap->freelist;
                if (heap->freelist)
                    heap->freelist->prev = fl;
                heap->freelist = fl;
#ifdef H5HL_DEBUG
            }
            else if (H5DEBUG(HL) && need_more > need_size) {
                fprintf(H5DEBUG(HL), "H5HL_insert: lost %lu bytes at line %d\n",
                        (unsigned long)(need_more - need_size), __LINE__);
#endif
            }
        }

#ifdef H5HL_DEBUG
        if (H5DEBUG(HL)) {
            fprintf(H5DEBUG(HL), "H5HL: resize mem buf from %lu to %lu bytes\n",
                    (unsigned long)(old_dblk_size), (unsigned long)(old_dblk_size + need_more));
        }
#endif
        if (NULL == (heap->dblk_image = H5FL_BLK_REALLOC(lheap_chunk, heap->dblk_image, heap->dblk_size)))
            HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, FAIL, "memory allocation failed");

        /* Clear new section so junk doesn't appear in the file */
        /* (Avoid clearing section which will be overwritten with newly inserted data) */
        memset(heap->dblk_image + offset + buf_size, 0, (new_dblk_size - (offset + buf_size)));
    }

    /* Copy the data into the heap */
    H5MM_memcpy(heap->dblk_image + offset, buf, buf_size);

    *offset_out = offset;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HL_insert() */

/*-------------------------------------------------------------------------
 * Function:    H5HL_remove
 *
 * Purpose:     Removes an object or part of an object from the heap at
 *              address ADDR of file F.	 The object (or part) to remove
 *              begins at byte OFFSET from the beginning of the heap and
 *              continues for SIZE bytes.
 *
 *              Once part of an object is removed, one must not attempt
 *              to access that part.  Removing the beginning of an object
 *              results in the object OFFSET increasing by the amount
 *              truncated.  Removing the end of an object results in
 *              object truncation.  Removing the middle of an object results
 *              in two separate objects, one at the original offset and
 *              one at the first offset past the removed portion.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HL_remove(H5F_t *f, H5HL_t *heap, size_t offset, size_t size)
{
    H5HL_free_t *fl        = NULL;
    herr_t       ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments */
    assert(f);
    assert(heap);
    assert(size > 0);
    assert(offset == H5HL_ALIGN(offset));

    size = H5HL_ALIGN(size);

    assert(offset < heap->dblk_size);
    assert(offset + size <= heap->dblk_size);

    /* Mark heap as dirty in cache
     *
     * (A bit early in the process, but it's difficult to determine in the
     *  code below where to mark the heap as dirty, especially in error cases,
     *  so we just accept that an extra flush of the heap info could occur
     *  if an error occurs -QAK)
     */
    if (FAIL == H5HL__dirty(heap))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTMARKDIRTY, FAIL, "unable to mark heap as dirty");

    /* Check if this chunk can be prepended or appended to an already
     * free chunk.  It might also fall between two chunks in such a way
     * that all three chunks can be combined into one.
     */
    fl = heap->freelist;
    while (fl) {
        H5HL_free_t *fl2 = NULL;

        if ((offset + size) == fl->offset) {
            fl->offset = offset;
            fl->size += size;
            assert(fl->offset == H5HL_ALIGN(fl->offset));
            assert(fl->size == H5HL_ALIGN(fl->size));
            fl2 = fl->next;
            while (fl2) {
                if ((fl2->offset + fl2->size) == fl->offset) {
                    fl->offset = fl2->offset;
                    fl->size += fl2->size;
                    assert(fl->offset == H5HL_ALIGN(fl->offset));
                    assert(fl->size == H5HL_ALIGN(fl->size));
                    fl2 = H5HL__remove_free(heap, fl2);
                    if (((fl->offset + fl->size) == heap->dblk_size) && ((2 * fl->size) > heap->dblk_size)) {
                        if (FAIL == H5HL__minimize_heap_space(f, heap))
                            HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "heap size minimization failed");
                    }
                    HGOTO_DONE(SUCCEED);
                }
                fl2 = fl2->next;
            }
            if (((fl->offset + fl->size) == heap->dblk_size) && ((2 * fl->size) > heap->dblk_size)) {
                if (FAIL == H5HL__minimize_heap_space(f, heap))
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "heap size minimization failed");
            }
            HGOTO_DONE(SUCCEED);
        }
        else if (fl->offset + fl->size == offset) {
            fl->size += size;
            fl2 = fl->next;
            assert(fl->size == H5HL_ALIGN(fl->size));
            while (fl2) {
                if (fl->offset + fl->size == fl2->offset) {
                    fl->size += fl2->size;
                    assert(fl->size == H5HL_ALIGN(fl->size));
                    fl2 = H5HL__remove_free(heap, fl2);
                    if (((fl->offset + fl->size) == heap->dblk_size) && ((2 * fl->size) > heap->dblk_size)) {
                        if (FAIL == H5HL__minimize_heap_space(f, heap))
                            HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "heap size minimization failed");
                    }
                    HGOTO_DONE(SUCCEED);
                }
                fl2 = fl2->next;
            }
            if (((fl->offset + fl->size) == heap->dblk_size) && ((2 * fl->size) > heap->dblk_size)) {
                if (FAIL == H5HL__minimize_heap_space(f, heap))
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "heap size minimization failed");
            }
            HGOTO_DONE(SUCCEED);
        }
        fl = fl->next;
    }

    /*
     * The amount which is being removed must be large enough to
     * hold the free list data.	 If not, the freed chunk is forever
     * lost.
     */
    if (size < H5HL_SIZEOF_FREE(f)) {
#ifdef H5HL_DEBUG
        if (H5DEBUG(HL)) {
            fprintf(H5DEBUG(HL), "H5HL: lost %lu bytes\n", (unsigned long)size);
        }
#endif
        HGOTO_DONE(SUCCEED);
    }

    /* Add an entry to the free list */
    if (NULL == (fl = H5FL_MALLOC(H5HL_free_t)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, FAIL, "memory allocation failed");
    fl->offset = offset;
    fl->size   = size;
    assert(fl->offset == H5HL_ALIGN(fl->offset));
    assert(fl->size == H5HL_ALIGN(fl->size));
    fl->prev = NULL;
    fl->next = heap->freelist;
    if (heap->freelist)
        heap->freelist->prev = fl;
    heap->freelist = fl;

    if (((fl->offset + fl->size) == heap->dblk_size) && ((2 * fl->size) > heap->dblk_size))
        if (FAIL == H5HL__minimize_heap_space(f, heap))
            HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "heap size minimization failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HL_remove() */

/*-------------------------------------------------------------------------
 * Function:    H5HL_delete
 *
 * Purpose:     Deletes a local heap from disk, freeing disk space used.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HL_delete(H5F_t *f, haddr_t addr)
{
    H5HL_t              *heap = NULL;                      /* Local heap to delete */
    H5HL_cache_prfx_ud_t prfx_udata;                       /* User data for protecting local heap prefix */
    H5HL_prfx_t         *prfx        = NULL;               /* Local heap prefix */
    H5HL_dblk_t         *dblk        = NULL;               /* Local heap data block */
    unsigned             cache_flags = H5AC__NO_FLAGS_SET; /* Flags for unprotecting heap */
    herr_t               ret_value   = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments */
    assert(f);
    assert(H5_addr_defined(addr));

    /* Construct the user data for protect callback */
    prfx_udata.sizeof_size = H5F_SIZEOF_SIZE(f);
    prfx_udata.sizeof_addr = H5F_SIZEOF_ADDR(f);
    prfx_udata.prfx_addr   = addr;
    prfx_udata.sizeof_prfx = H5HL_SIZEOF_HDR(f);

    /* Protect the local heap prefix */
    if (NULL ==
        (prfx = (H5HL_prfx_t *)H5AC_protect(f, H5AC_LHEAP_PRFX, addr, &prfx_udata, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to load heap prefix");

    /* Get the pointer to the heap */
    heap = prfx->heap;

    /* Check if heap has separate data block */
    if (!heap->single_cache_obj)
        /* Protect the local heap data block */
        if (NULL == (dblk = (H5HL_dblk_t *)H5AC_protect(f, H5AC_LHEAP_DBLK, heap->dblk_addr, heap,
                                                        H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to load heap data block");

    /* Set the flags for releasing the prefix and data block */
    cache_flags |= H5AC__DIRTIED_FLAG | H5AC__DELETED_FLAG | H5AC__FREE_FILE_SPACE_FLAG;

done:
    /* Release the data block from the cache, now deleted */
    if (dblk && heap && H5AC_unprotect(f, H5AC_LHEAP_DBLK, heap->dblk_addr, dblk, cache_flags) < 0)
        HDONE_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to release local heap data block");

    /* Release the prefix from the cache, now deleted */
    if (prfx && heap && H5AC_unprotect(f, H5AC_LHEAP_PRFX, heap->prfx_addr, prfx, cache_flags) < 0)
        HDONE_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to release local heap prefix");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HL_delete() */

/*-------------------------------------------------------------------------
 * Function:    H5HL_get_size
 *
 * Purpose:     Retrieves the current size of a heap
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HL_get_size(H5F_t *f, haddr_t addr, size_t *size)
{
    H5HL_cache_prfx_ud_t prfx_udata;       /* User data for protecting local heap prefix */
    H5HL_prfx_t         *prfx      = NULL; /* Local heap prefix */
    H5HL_t              *heap      = NULL; /* Heap data structure */
    herr_t               ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments */
    assert(f);
    assert(H5_addr_defined(addr));
    assert(size);

    /* Construct the user data for protect callback */
    prfx_udata.sizeof_size = H5F_SIZEOF_SIZE(f);
    prfx_udata.sizeof_addr = H5F_SIZEOF_ADDR(f);
    prfx_udata.prfx_addr   = addr;
    prfx_udata.sizeof_prfx = H5HL_SIZEOF_HDR(f);

    /* Protect the local heap prefix */
    if (NULL ==
        (prfx = (H5HL_prfx_t *)H5AC_protect(f, H5AC_LHEAP_PRFX, addr, &prfx_udata, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to load heap prefix");

    /* Get the pointer to the heap */
    heap = prfx->heap;

    /* Set the size to return */
    *size = heap->dblk_size;

done:
    if (prfx && FAIL == H5AC_unprotect(f, H5AC_LHEAP_PRFX, heap->prfx_addr, prfx, H5AC__NO_FLAGS_SET))
        HDONE_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to release local heap prefix");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HL_get_size() */

/*-------------------------------------------------------------------------
 * Function:    H5HL_heapsize
 *
 * Purpose:     Compute the size in bytes of the specified instance of
 *              H5HL_t via H5HL_size()
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HL_heapsize(H5F_t *f, haddr_t addr, hsize_t *heap_size)
{
    H5HL_cache_prfx_ud_t prfx_udata;       /* User data for protecting local heap prefix */
    H5HL_prfx_t         *prfx      = NULL; /* Local heap prefix */
    H5HL_t              *heap      = NULL; /* Heap data structure */
    herr_t               ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments */
    assert(f);
    assert(H5_addr_defined(addr));
    assert(heap_size);

    /* Construct the user data for protect callback */
    prfx_udata.sizeof_size = H5F_SIZEOF_SIZE(f);
    prfx_udata.sizeof_addr = H5F_SIZEOF_ADDR(f);
    prfx_udata.prfx_addr   = addr;
    prfx_udata.sizeof_prfx = H5HL_SIZEOF_HDR(f);

    /* Protect the local heap prefix */
    if (NULL ==
        (prfx = (H5HL_prfx_t *)H5AC_protect(f, H5AC_LHEAP_PRFX, addr, &prfx_udata, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to load heap prefix");

    /* Get the pointer to the heap */
    heap = prfx->heap;

    /* Accumulate the size of the local heap */
    *heap_size += (hsize_t)(heap->prfx_size + heap->dblk_size);

done:
    if (prfx && FAIL == H5AC_unprotect(f, H5AC_LHEAP_PRFX, heap->prfx_addr, prfx, H5AC__NO_FLAGS_SET))
        HDONE_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to release local heap prefix");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HL_heapsize() */
