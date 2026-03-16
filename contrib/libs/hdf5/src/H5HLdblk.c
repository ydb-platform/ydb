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
 * Created:     H5HLdblk.c
 *
 * Purpose:     Data block routines for local heaps.
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
#include "H5private.h"   /* Generic Functions            */
#include "H5Eprivate.h"  /* Error handling               */
#include "H5FLprivate.h" /* Free lists                   */
#include "H5HLpkg.h"     /* Local Heaps                  */
#include "H5MFprivate.h" /* File memory management       */

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

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Declare a free list to manage the H5HL_dblk_t struct */
H5FL_DEFINE_STATIC(H5HL_dblk_t);

/*-------------------------------------------------------------------------
 * Function:    H5HL__dblk_new
 *
 * Purpose:     Create a new local heap data block object
 *
 * Return:      Success:    non-NULL pointer to new local heap data block
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
H5HL_dblk_t *
H5HL__dblk_new(H5HL_t *heap)
{
    H5HL_dblk_t *dblk      = NULL; /* New local heap data block */
    H5HL_dblk_t *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    /* check arguments */
    assert(heap);

    /* Allocate new local heap data block */
    if (NULL == (dblk = H5FL_CALLOC(H5HL_dblk_t)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, NULL, "memory allocation failed for local heap data block");

    /* Increment ref. count on heap data structure */
    if (FAIL == H5HL__inc_rc(heap))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINC, NULL, "can't increment heap ref. count");

    /* Link the heap & the data block */
    dblk->heap = heap;
    heap->dblk = dblk;

    /* Set the return value */
    ret_value = dblk;

done:
    /* Ensure that the data block memory is deallocated on errors */
    if (!ret_value && dblk != NULL)
        /* H5FL_FREE always returns NULL so we can't check for errors */
        dblk = H5FL_FREE(H5HL_dblk_t, dblk);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HL__dblk_new() */

/*-------------------------------------------------------------------------
 * Function:    H5HL__dblk_dest
 *
 * Purpose:     Destroy a local heap data block object
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HL__dblk_dest(H5HL_dblk_t *dblk)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* check arguments */
    assert(dblk);

    /* Check if data block was initialized */
    if (dblk->heap) {
        /* Unlink data block from heap */
        dblk->heap->dblk = NULL;

        /* Decrement ref. count on heap data structure */
        if (FAIL == H5HL__dec_rc(dblk->heap))
            HGOTO_ERROR(H5E_HEAP, H5E_CANTDEC, FAIL, "can't decrement heap ref. count");

        /* Unlink heap from data block */
        dblk->heap = NULL;
    }

done:
    /* Free local heap data block */
    /* H5FL_FREE always returns NULL so we can't check for errors */
    dblk = H5FL_FREE(H5HL_dblk_t, dblk);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HL__dblk_dest() */

/*-------------------------------------------------------------------------
 * Function:    H5HL__dblk_realloc
 *
 * Purpose:     Reallocate data block for heap
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HL__dblk_realloc(H5F_t *f, H5HL_t *heap, size_t new_heap_size)
{
    H5HL_dblk_t *dblk;          /* Local heap data block */
    haddr_t      old_addr;      /* Old location of heap data block */
    haddr_t      new_addr;      /* New location of heap data block */
    size_t       old_heap_size; /* Old size of heap data block */
    herr_t       ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(heap);
    assert(new_heap_size > 0);

    /* Release old space on disk */
    old_addr      = heap->dblk_addr;
    old_heap_size = heap->dblk_size;
    H5_CHECK_OVERFLOW(old_heap_size, size_t, hsize_t);
    if (FAIL == H5MF_xfree(f, H5FD_MEM_LHEAP, old_addr, (hsize_t)old_heap_size))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "can't free old local heap data");

    /* Allocate new space on disk */
    H5_CHECK_OVERFLOW(new_heap_size, size_t, hsize_t);
    if (HADDR_UNDEF == (new_addr = H5MF_alloc(f, H5FD_MEM_LHEAP, (hsize_t)new_heap_size)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, FAIL, "unable to allocate file space for local heap");

    /* Update heap info*/
    heap->dblk_addr = new_addr;
    heap->dblk_size = new_heap_size;

    /* Check if heap data block actually moved in the file */
    if (H5_addr_eq(old_addr, new_addr)) {
        /* Check if heap data block is contiguous w/prefix */
        if (heap->single_cache_obj) {
            /* Sanity check */
            assert(H5_addr_eq(heap->prfx_addr + heap->prfx_size, old_addr));
            assert(heap->prfx);

            /* Resize the heap prefix in the cache */
            if (FAIL == H5AC_resize_entry(heap->prfx, (size_t)(heap->prfx_size + new_heap_size)))
                HGOTO_ERROR(H5E_HEAP, H5E_CANTRESIZE, FAIL, "unable to resize heap in cache");
        }
        else {
            /* Sanity check */
            assert(H5_addr_ne(heap->prfx_addr + heap->prfx_size, old_addr));
            assert(heap->dblk);

            /* Resize the heap data block in the cache */
            if (H5AC_resize_entry(heap->dblk, (size_t)new_heap_size) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTRESIZE, FAIL, "unable to resize heap (data block) in cache");
        }
    }
    else {
        /* Check if heap data block was contiguous w/prefix previously */
        if (heap->single_cache_obj) {
            /* Create new heap data block */
            if (NULL == (dblk = H5HL__dblk_new(heap)))
                HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, FAIL, "unable to allocate local heap data block");

            /* Resize current heap prefix */
            heap->prfx_size = H5HL_SIZEOF_HDR(f);
            if (FAIL == H5AC_resize_entry(heap->prfx, (size_t)heap->prfx_size))
                HGOTO_ERROR(H5E_HEAP, H5E_CANTRESIZE, FAIL, "unable to resize heap prefix in cache");

            /* Insert data block into cache (pinned) */
            if (FAIL == H5AC_insert_entry(f, H5AC_LHEAP_DBLK, new_addr, dblk, H5AC__PIN_ENTRY_FLAG))
                HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "unable to cache local heap data block");

            dblk = NULL;

            /* Reset 'single cache object' flag */
            heap->single_cache_obj = false;
        }
        else {
            /* Resize the heap data block in the cache */
            /* (ignore [unlikely] case where heap data block ends up
             *      contiguous w/heap prefix again.
             */
            if (FAIL == H5AC_resize_entry(heap->dblk, (size_t)new_heap_size))
                HGOTO_ERROR(H5E_HEAP, H5E_CANTRESIZE, FAIL, "unable to resize heap data block in cache");

            /* Relocate the heap data block in the cache */
            if (FAIL == H5AC_move_entry(f, H5AC_LHEAP_DBLK, old_addr, new_addr))
                HGOTO_ERROR(H5E_HEAP, H5E_CANTMOVE, FAIL, "unable to move heap data block in cache");
        }
    }

done:
    /* Restore old heap address & size on errors */
    if (FAIL == ret_value) {
        heap->dblk_addr = old_addr;
        heap->dblk_size = old_heap_size;
    }
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HL__dblk_realloc() */
