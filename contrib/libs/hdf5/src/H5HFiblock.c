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
 * Created:		H5HFiblock.c
 *
 * Purpose:		Indirect block routines for fractal heaps.
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
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Fprivate.h"  /* File access				*/
#include "H5HFpkg.h"     /* Fractal heaps			*/
#include "H5MFprivate.h" /* File memory management		*/
#include "H5VMprivate.h" /* Vectors and arrays 			*/

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
static herr_t H5HF__iblock_pin(H5HF_indirect_t *iblock);
static herr_t H5HF__iblock_unpin(H5HF_indirect_t *iblock);
static herr_t H5HF__man_iblock_root_halve(H5HF_indirect_t *root_iblock);
static herr_t H5HF__man_iblock_root_revert(H5HF_indirect_t *root_iblock);

/*********************/
/* Package Variables */
/*********************/

/* Declare a free list to manage the H5HF_indirect_t struct */
H5FL_DEFINE(H5HF_indirect_t);

/* Declare a free list to manage the H5HF_indirect_ent_t sequence information */
H5FL_SEQ_DEFINE(H5HF_indirect_ent_t);

/* Declare a free list to manage the H5HF_indirect_filt_ent_t sequence information */
H5FL_SEQ_DEFINE(H5HF_indirect_filt_ent_t);

/* Declare a free list to manage the H5HF_indirect_t * sequence information */
H5FL_SEQ_DEFINE(H5HF_indirect_ptr_t);

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:	H5HF__iblock_pin
 *
 * Purpose:	Pin an indirect block in memory
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__iblock_pin(H5HF_indirect_t *iblock)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(iblock);

    /* Mark block as un-evictable */
    if (H5AC_pin_protected_entry(iblock) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTPIN, FAIL, "unable to pin fractal heap indirect block");

    /* If this indirect block has a parent, update it's child iblock pointer */
    if (iblock->parent) {
        H5HF_indirect_t *par_iblock = iblock->parent; /* Parent indirect block */
        unsigned         indir_idx;                   /* Index in parent's child iblock pointer array */

        /* Sanity check */
        assert(par_iblock->child_iblocks);
        assert(iblock->par_entry >=
               (iblock->hdr->man_dtable.max_direct_rows * iblock->hdr->man_dtable.cparam.width));

        /* Compute index in parent's child iblock pointer array */
        indir_idx = iblock->par_entry -
                    (iblock->hdr->man_dtable.max_direct_rows * iblock->hdr->man_dtable.cparam.width);

        /* Set pointer to pinned indirect block in parent */
        assert(par_iblock->child_iblocks[indir_idx] == NULL);
        par_iblock->child_iblocks[indir_idx] = iblock;
    } /* end if */
    else {
        /* Check for pinning the root indirect block */
        if (iblock->block_off == 0) {
            /* Sanity check - shouldn't be recursively pinning root indirect block */
            assert(0 == (iblock->hdr->root_iblock_flags & H5HF_ROOT_IBLOCK_PINNED));

            /* Check if we should set the root iblock pointer */
            if (0 == iblock->hdr->root_iblock_flags) {
                assert(NULL == iblock->hdr->root_iblock);
                iblock->hdr->root_iblock = iblock;
            } /* end if */

            /* Indicate that the root indirect block is pinned */
            iblock->hdr->root_iblock_flags |= H5HF_ROOT_IBLOCK_PINNED;
        } /* end if */
    }     /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__iblock_pin() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__iblock_unpin
 *
 * Purpose:	Unpin an indirect block in the metadata cache
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__iblock_unpin(H5HF_indirect_t *iblock)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(iblock);

    /* Mark block as evictable again */
    if (H5AC_unpin_entry(iblock) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTUNPIN, FAIL, "unable to unpin fractal heap indirect block");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__iblock_unpin() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__iblock_incr
 *
 * Purpose:	Increment reference count on shared indirect block
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__iblock_incr(H5HF_indirect_t *iblock)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(iblock);
    assert(iblock->block_off == 0 || iblock->parent);

    /* Mark block as un-evictable when a child block is depending on it */
    if (iblock->rc == 0)
        if (H5HF__iblock_pin(iblock) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTPIN, FAIL, "unable to pin fractal heap indirect block");

    /* Increment reference count on shared indirect block */
    iblock->rc++;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__iblock_incr() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__iblock_decr
 *
 * Purpose:	Decrement reference count on shared indirect block
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__iblock_decr(H5HF_indirect_t *iblock)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(iblock);

    /* Decrement reference count on shared indirect block */
    iblock->rc--;

    /* Check for last reference to block */
    if (iblock->rc == 0) {

        /* If this indirect block has a parent, reset it's child iblock pointer */
        if (iblock->parent) {
            H5HF_indirect_t *par_iblock = iblock->parent; /* Parent indirect block */
            unsigned         indir_idx;                   /* Index in parent's child iblock pointer array */

            /* Sanity check */
            assert(par_iblock->child_iblocks);
            assert(iblock->par_entry >=
                   (iblock->hdr->man_dtable.max_direct_rows * iblock->hdr->man_dtable.cparam.width));

            /* Compute index in parent's child iblock pointer array */
            indir_idx = iblock->par_entry -
                        (iblock->hdr->man_dtable.max_direct_rows * iblock->hdr->man_dtable.cparam.width);

            /* Reset pointer to pinned child indirect block in parent */
            assert(par_iblock->child_iblocks[indir_idx]);
            par_iblock->child_iblocks[indir_idx] = NULL;
        } /* end if */
        else {
            /* Check for root indirect block */
            if (iblock->block_off == 0) {
                /* Sanity check - shouldn't be recursively unpinning root indirect block */
                assert(iblock->hdr->root_iblock_flags & H5HF_ROOT_IBLOCK_PINNED);

                /* Check if we should reset the root iblock pointer */
                if (H5HF_ROOT_IBLOCK_PINNED == iblock->hdr->root_iblock_flags) {
                    assert(NULL != iblock->hdr->root_iblock);
                    iblock->hdr->root_iblock = NULL;
                } /* end if */

                /* Indicate that the root indirect block is unpinned */
                iblock->hdr->root_iblock_flags &= (unsigned)(~(H5HF_ROOT_IBLOCK_PINNED));
            } /* end if */
        }     /* end else */

        /* Check if the block is still in the cache */
        if (!iblock->removed_from_cache) {
            /* Unpin the indirect block, making it evictable again */
            if (H5HF__iblock_unpin(iblock) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTUNPIN, FAIL, "unable to unpin fractal heap indirect block");
        } /* end if */
        else {
            /* Destroy the indirect block */
            if (H5HF__man_iblock_dest(iblock) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "unable to destroy fractal heap indirect block");
        } /* end else */
    }     /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__iblock_decr() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__iblock_dirty
 *
 * Purpose:	Mark indirect block as dirty
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__iblock_dirty(H5HF_indirect_t *iblock)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(iblock);

    /* Mark indirect block as dirty in cache */
    if (H5AC_mark_entry_dirty(iblock) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTMARKDIRTY, FAIL, "unable to mark fractal heap indirect block as dirty");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__iblock_dirty() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_iblock_root_create
 *
 * Purpose:	Create root indirect block
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_iblock_root_create(H5HF_hdr_t *hdr, size_t min_dblock_size)
{
    H5HF_indirect_t *iblock;              /* Pointer to indirect block */
    haddr_t          iblock_addr;         /* Indirect block's address */
    hsize_t          acc_dblock_free;     /* Accumulated free space in direct blocks */
    bool             have_direct_block;   /* Flag to indicate a direct block already exists */
    bool             did_protect;         /* Whether we protected the indirect block or not */
    unsigned         nrows;               /* Number of rows for root indirect block */
    unsigned         u;                   /* Local index variable */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check for allocating entire root indirect block initially */
    if (hdr->man_dtable.cparam.start_root_rows == 0)
        nrows = hdr->man_dtable.max_root_rows;
    else {
        unsigned rows_needed;   /* Number of rows needed to get to direct block size */
        unsigned block_row_off; /* Row offset from larger block sizes */

        nrows = hdr->man_dtable.cparam.start_root_rows;

        block_row_off = H5VM_log2_of2((uint32_t)min_dblock_size) -
                        H5VM_log2_of2((uint32_t)hdr->man_dtable.cparam.start_block_size);
        if (block_row_off > 0)
            block_row_off++; /* Account for the pair of initial rows of the initial block size */
        rows_needed = 1 + block_row_off;
        if (nrows < rows_needed)
            nrows = rows_needed;
    } /* end else */

    /* Allocate root indirect block */
    if (H5HF__man_iblock_create(hdr, NULL, 0, nrows, hdr->man_dtable.max_root_rows, &iblock_addr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, FAIL, "can't allocate fractal heap indirect block");

    /* Move current direct block (used as root) into new indirect block */

    /* Lock new indirect block */
    if (NULL == (iblock = H5HF__man_iblock_protect(hdr, iblock_addr, nrows, NULL, 0, false,
                                                   H5AC__NO_FLAGS_SET, &did_protect)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to protect fractal heap indirect block");

    /* Check if there's already a direct block as root) */
    have_direct_block = H5_addr_defined(hdr->man_dtable.table_addr);
    if (have_direct_block) {
        H5HF_direct_t *dblock; /* Pointer to direct block to query */

        /* Lock first (root) direct block */
        if (NULL == (dblock = H5HF__man_dblock_protect(hdr, hdr->man_dtable.table_addr,
                                                       hdr->man_dtable.cparam.start_block_size, NULL, 0,
                                                       H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to protect fractal heap direct block");

        /* Attach direct block to new root indirect block */
        dblock->parent    = iblock;
        dblock->par_entry = 0;

        /* Destroy flush dependency between direct block and header */
        if (H5AC_destroy_flush_dependency(dblock->fd_parent, dblock) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTUNDEPEND, FAIL, "unable to destroy flush dependency");
        dblock->fd_parent = NULL;

        /* Create flush dependency between direct block and new root indirect block */
        if (H5AC_create_flush_dependency(iblock, dblock) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTDEPEND, FAIL, "unable to create flush dependency");
        dblock->fd_parent = iblock;

        if (H5HF__man_iblock_attach(iblock, 0, hdr->man_dtable.table_addr) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTATTACH, FAIL,
                        "can't attach root direct block to parent indirect block");

        /* Check for I/O filters on this heap */
        if (hdr->filter_len > 0) {
            /* Set the pipeline filter information from the header */
            iblock->filt_ents[0].size        = hdr->pline_root_direct_size;
            iblock->filt_ents[0].filter_mask = hdr->pline_root_direct_filter_mask;

            /* Reset the header's pipeline information */
            hdr->pline_root_direct_size        = 0;
            hdr->pline_root_direct_filter_mask = 0;
        } /* end if */

        /* Scan free space sections to set any 'parent' pointers to new indirect block */
        if (H5HF__space_create_root(hdr, iblock) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTSET, FAIL,
                        "can't set free space section info to new root indirect block");

        /* Unlock first (previously the root) direct block */
        if (H5AC_unprotect(hdr->f, H5AC_FHEAP_DBLOCK, hdr->man_dtable.table_addr, dblock,
                           H5AC__NO_FLAGS_SET) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to release fractal heap direct block");
        dblock = NULL;
    } /* end if */

    /* Start iterator at correct location */
    if (H5HF__hdr_start_iter(hdr, iblock,
                             (hsize_t)(have_direct_block ? hdr->man_dtable.cparam.start_block_size : 0),
                             have_direct_block) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't initialize block iterator");

    /* Check for skipping over direct blocks, in order to get to large enough block */
    if (min_dblock_size > hdr->man_dtable.cparam.start_block_size)
        /* Add skipped blocks to heap's free space */
        if (H5HF__hdr_skip_blocks(hdr, iblock, have_direct_block,
                                  ((nrows - 1) * hdr->man_dtable.cparam.width) - have_direct_block) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTDEC, FAIL, "can't add skipped blocks to heap's free space");

    /* Mark indirect block as modified */
    if (H5HF__iblock_dirty(iblock) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTDIRTY, FAIL, "can't mark indirect block as dirty");

    /* Unprotect root indirect block (it's pinned by the iterator though) */
    if (H5HF__man_iblock_unprotect(iblock, H5AC__DIRTIED_FLAG, did_protect) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to release fractal heap indirect block");
    iblock = NULL;

    /* Point heap header at new indirect block */
    hdr->man_dtable.curr_root_rows = nrows;
    hdr->man_dtable.table_addr     = iblock_addr;

    /* Compute free space in direct blocks referenced from entries in root indirect block */
    acc_dblock_free = 0;
    for (u = 0; u < nrows; u++)
        acc_dblock_free += hdr->man_dtable.row_tot_dblock_free[u] * hdr->man_dtable.cparam.width;

    /* Account for potential initial direct block */
    if (have_direct_block)
        acc_dblock_free -= hdr->man_dtable.row_tot_dblock_free[0];

    /* Extend heap to cover new root indirect block */
    if (H5HF__hdr_adjust_heap(hdr, hdr->man_dtable.row_block_off[nrows], (hssize_t)acc_dblock_free) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTEXTEND, FAIL, "can't increase space to cover root direct block");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_iblock_root_create() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_iblock_root_double
 *
 * Purpose:	Double size of root indirect block
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_iblock_root_double(H5HF_hdr_t *hdr, size_t min_dblock_size)
{
    H5HF_indirect_t *iblock;          /* Pointer to root indirect block */
    haddr_t          new_addr;        /* New address of indirect block */
    hsize_t          acc_dblock_free; /* Accumulated free space in direct blocks */
    hsize_t          next_size;       /* The previous value of the "next size" for the new block iterator */
    hsize_t          old_iblock_size; /* Old size of indirect block */
    unsigned         next_row;        /* The next row to allocate block in */
    unsigned         next_entry;      /* The previous value of the "next entry" for the new block iterator */
    unsigned         new_next_entry = 0; /* The new value of the "next entry" for the new block iterator */
    unsigned         min_nrows      = 0; /* Min. # of direct rows */
    unsigned         old_nrows;          /* Old # of rows */
    unsigned         new_nrows;          /* New # of rows */
    bool             skip_direct_rows = false; /* Whether we are skipping direct rows */
    size_t           u;                        /* Local index variable */
    herr_t           ret_value = SUCCEED;      /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get "new block" iterator information */
    if (H5HF__man_iter_curr(&hdr->next_block, &next_row, NULL, &next_entry, &iblock) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "unable to retrieve current block iterator location");
    next_size = hdr->man_dtable.row_block_size[next_row];

    /* Make certain the iterator is at the root indirect block */
    assert(iblock->parent == NULL);
    assert(iblock->block_off == 0);

    /* Keep this for later */
    old_nrows = iblock->nrows;

    /* Check for skipping over direct block rows */
    if (iblock->nrows < hdr->man_dtable.max_direct_rows && min_dblock_size > next_size) {
        /* Sanity check */
        assert(min_dblock_size > hdr->man_dtable.cparam.start_block_size);

        /* Set flag */
        skip_direct_rows = true;

        /* Make certain we allocate at least the required row for the block requested */
        min_nrows = 1 + H5HF__dtable_size_to_row(&hdr->man_dtable, min_dblock_size);

        /* Set the information for the next block, of the appropriate size */
        new_next_entry = (min_nrows - 1) * hdr->man_dtable.cparam.width;
    } /* end if */

    /* Compute new # of rows in indirect block */
    new_nrows = MAX(min_nrows, MIN(2 * iblock->nrows, iblock->max_rows));

    /* Check if the indirect block is NOT currently allocated in temp. file space */
    /* (temp. file space does not need to be freed) */
    if (!H5F_IS_TMP_ADDR(hdr->f, iblock->addr))
        /* Free previous indirect block disk space */
        if (H5MF_xfree(hdr->f, H5FD_MEM_FHEAP_IBLOCK, iblock->addr, (hsize_t)iblock->size) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL,
                        "unable to free fractal heap indirect block file space");

    /* Compute size of buffer needed for new indirect block */
    iblock->nrows   = new_nrows;
    old_iblock_size = iblock->size;
    iblock->size    = H5HF_MAN_INDIRECT_SIZE(hdr, iblock->nrows);

    /* Allocate [temporary] space for the new indirect block on disk */
    if (H5F_USE_TMP_SPACE(hdr->f)) {
        if (HADDR_UNDEF == (new_addr = H5MF_alloc_tmp(hdr->f, (hsize_t)iblock->size)))
            HGOTO_ERROR(H5E_HEAP, H5E_NOSPACE, FAIL,
                        "file allocation failed for fractal heap indirect block");
    } /* end if */
    else {
        if (HADDR_UNDEF == (new_addr = H5MF_alloc(hdr->f, H5FD_MEM_FHEAP_IBLOCK, (hsize_t)iblock->size)))
            HGOTO_ERROR(H5E_HEAP, H5E_NOSPACE, FAIL,
                        "file allocation failed for fractal heap indirect block");
    } /* end else */

    /* Resize pinned indirect block in the cache, if its changed size */
    if (old_iblock_size != iblock->size) {
        if (H5AC_resize_entry(iblock, (size_t)iblock->size) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTRESIZE, FAIL, "unable to resize fractal heap indirect block");
    } /* end if */

    /* Move object in cache, if it actually was relocated */
    if (H5_addr_ne(iblock->addr, new_addr)) {
        if (H5AC_move_entry(hdr->f, H5AC_FHEAP_IBLOCK, iblock->addr, new_addr) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTMOVE, FAIL, "unable to move fractal heap root indirect block");
        iblock->addr = new_addr;
    } /* end if */

    /* Re-allocate child block entry array */
    if (NULL == (iblock->ents = H5FL_SEQ_REALLOC(H5HF_indirect_ent_t, iblock->ents,
                                                 (size_t)(iblock->nrows * hdr->man_dtable.cparam.width))))
        HGOTO_ERROR(H5E_HEAP, H5E_NOSPACE, FAIL, "memory allocation failed for direct entries");

    /* Check for skipping over rows and add free section for skipped rows */
    if (skip_direct_rows)
        /* Add skipped blocks to heap's free space */
        if (H5HF__hdr_skip_blocks(hdr, iblock, next_entry, (new_next_entry - next_entry)) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTDEC, FAIL, "can't add skipped blocks to heap's free space");

    /* Initialize new direct block entries in rows added */
    acc_dblock_free = 0;
    for (u = (old_nrows * hdr->man_dtable.cparam.width); u < (iblock->nrows * hdr->man_dtable.cparam.width);
         u++) {
        unsigned row = (unsigned)(u / hdr->man_dtable.cparam.width); /* Row for current entry */

        iblock->ents[u].addr = HADDR_UNDEF;
        acc_dblock_free += hdr->man_dtable.row_tot_dblock_free[row];
    } /* end for */

    /* Check for needing to re-allocate filtered entry array */
    if (hdr->filter_len > 0 && old_nrows < hdr->man_dtable.max_direct_rows) {
        unsigned dir_rows; /* Number of direct rows in this indirect block */

        /* Compute the number of direct rows for this indirect block */
        dir_rows = MIN(iblock->nrows, hdr->man_dtable.max_direct_rows);
        assert(dir_rows > old_nrows);

        /* Re-allocate filtered direct block entry array */
        if (NULL == (iblock->filt_ents = H5FL_SEQ_REALLOC(H5HF_indirect_filt_ent_t, iblock->filt_ents,
                                                          (size_t)(dir_rows * hdr->man_dtable.cparam.width))))
            HGOTO_ERROR(H5E_HEAP, H5E_NOSPACE, FAIL, "memory allocation failed for filtered direct entries");

        /* Initialize new entries allocated */
        for (u = (old_nrows * hdr->man_dtable.cparam.width); u < (dir_rows * hdr->man_dtable.cparam.width);
             u++) {
            iblock->filt_ents[u].size        = 0;
            iblock->filt_ents[u].filter_mask = 0;
        } /* end for */
    }     /* end if */

    /* Check for needing to re-allocate child iblock pointer array */
    if (iblock->nrows > hdr->man_dtable.max_direct_rows) {
        unsigned indir_rows;     /* Number of indirect rows in this indirect block */
        unsigned old_indir_rows; /* Previous number of indirect rows in this indirect block */

        /* Compute the number of direct rows for this indirect block */
        indir_rows = iblock->nrows - hdr->man_dtable.max_direct_rows;

        /* Re-allocate child indirect block array */
        if (NULL ==
            (iblock->child_iblocks = H5FL_SEQ_REALLOC(H5HF_indirect_ptr_t, iblock->child_iblocks,
                                                      (size_t)(indir_rows * hdr->man_dtable.cparam.width))))
            HGOTO_ERROR(H5E_HEAP, H5E_NOSPACE, FAIL, "memory allocation failed for filtered direct entries");

        /* Compute the previous # of indirect rows in this block */
        if (old_nrows < hdr->man_dtable.max_direct_rows)
            old_indir_rows = 0;
        else
            old_indir_rows = old_nrows - hdr->man_dtable.max_direct_rows;

        /* Initialize new entries allocated */
        for (u = (old_indir_rows * hdr->man_dtable.cparam.width);
             u < (indir_rows * hdr->man_dtable.cparam.width); u++)
            iblock->child_iblocks[u] = NULL;
    } /* end if */

    /* Mark indirect block as dirty */
    if (H5HF__iblock_dirty(iblock) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTDIRTY, FAIL, "can't mark indirect block as dirty");

    /* Update other shared header info */
    hdr->man_dtable.curr_root_rows = new_nrows;
    hdr->man_dtable.table_addr     = new_addr;

    /* Extend heap to cover new root indirect block */
    if (H5HF__hdr_adjust_heap(hdr, 2 * hdr->man_dtable.row_block_off[new_nrows - 1],
                              (hssize_t)acc_dblock_free) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTEXTEND, FAIL, "can't increase space to cover root direct block");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_iblock_root_double() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_iblock_root_halve
 *
 * Purpose:	Halve size of root indirect block
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__man_iblock_root_halve(H5HF_indirect_t *iblock)
{
    H5HF_hdr_t *hdr = iblock->hdr;   /* Pointer to heap header */
    haddr_t     new_addr;            /* New address of indirect block */
    hsize_t     acc_dblock_free;     /* Accumulated free space in direct blocks */
    hsize_t     old_size;            /* Old size of indirect block */
    unsigned    max_child_row;       /* Row for max. child entry */
    unsigned    old_nrows;           /* Old # of rows */
    unsigned    new_nrows;           /* New # of rows */
    unsigned    u;                   /* Local index variable */
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(iblock);
    assert(iblock->parent == NULL);
    assert(hdr);

    /* Compute maximum row used by child of indirect block */
    max_child_row = iblock->max_child / hdr->man_dtable.cparam.width;

    /* Compute new # of rows in root indirect block */
    new_nrows = (unsigned)1 << (1 + H5VM_log2_gen((uint64_t)max_child_row));

    /* Check if the indirect block is NOT currently allocated in temp. file space */
    /* (temp. file space does not need to be freed) */
    if (!H5F_IS_TMP_ADDR(hdr->f, iblock->addr))
        /* Free previous indirect block disk space */
        if (H5MF_xfree(hdr->f, H5FD_MEM_FHEAP_IBLOCK, iblock->addr, (hsize_t)iblock->size) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL,
                        "unable to free fractal heap indirect block file space");

    /* Compute free space in rows to delete */
    acc_dblock_free = 0;
    for (u = new_nrows; u < iblock->nrows; u++)
        acc_dblock_free += hdr->man_dtable.row_tot_dblock_free[u] * hdr->man_dtable.cparam.width;

    /* Compute size of buffer needed for new indirect block */
    old_nrows     = iblock->nrows;
    iblock->nrows = new_nrows;
    old_size      = iblock->size;
    iblock->size  = H5HF_MAN_INDIRECT_SIZE(hdr, iblock->nrows);

    /* Allocate [temporary] space for the new indirect block on disk */
    if (H5F_USE_TMP_SPACE(hdr->f)) {
        if (HADDR_UNDEF == (new_addr = H5MF_alloc_tmp(hdr->f, (hsize_t)iblock->size)))
            HGOTO_ERROR(H5E_HEAP, H5E_NOSPACE, FAIL,
                        "file allocation failed for fractal heap indirect block");
    } /* end if */
    else {
        if (HADDR_UNDEF == (new_addr = H5MF_alloc(hdr->f, H5FD_MEM_FHEAP_IBLOCK, (hsize_t)iblock->size)))
            HGOTO_ERROR(H5E_HEAP, H5E_NOSPACE, FAIL,
                        "file allocation failed for fractal heap indirect block");
    } /* end else */

    /* Resize pinned indirect block in the cache, if it has changed size */
    if (old_size != iblock->size) {
        if (H5AC_resize_entry(iblock, (size_t)iblock->size) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTRESIZE, FAIL, "unable to resize fractal heap indirect block");
    } /* end if */

    /* Move object in cache, if it actually was relocated */
    if (H5_addr_ne(iblock->addr, new_addr)) {
        if (H5AC_move_entry(hdr->f, H5AC_FHEAP_IBLOCK, iblock->addr, new_addr) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTSPLIT, FAIL, "unable to move fractal heap root indirect block");
        iblock->addr = new_addr;
    } /* end if */

    /* Re-allocate child block entry array */
    if (NULL == (iblock->ents = H5FL_SEQ_REALLOC(H5HF_indirect_ent_t, iblock->ents,
                                                 (size_t)(iblock->nrows * hdr->man_dtable.cparam.width))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed for direct entries");

    /* Check for needing to re-allocate filtered entry array */
    if (hdr->filter_len > 0 && new_nrows < hdr->man_dtable.max_direct_rows) {
        /* Re-allocate filtered direct block entry array */
        if (NULL ==
            (iblock->filt_ents = H5FL_SEQ_REALLOC(H5HF_indirect_filt_ent_t, iblock->filt_ents,
                                                  (size_t)(iblock->nrows * hdr->man_dtable.cparam.width))))
            HGOTO_ERROR(H5E_HEAP, H5E_NOSPACE, FAIL, "memory allocation failed for filtered direct entries");
    } /* end if */

    /* Check for needing to re-allocate child iblock pointer array */
    if (old_nrows > hdr->man_dtable.max_direct_rows) {
        /* Check for shrinking away child iblock pointer array */
        if (iblock->nrows > hdr->man_dtable.max_direct_rows) {
            unsigned indir_rows; /* Number of indirect rows in this indirect block */

            /* Compute the number of direct rows for this indirect block */
            indir_rows = iblock->nrows - hdr->man_dtable.max_direct_rows;

            /* Re-allocate child indirect block array */
            if (NULL == (iblock->child_iblocks =
                             H5FL_SEQ_REALLOC(H5HF_indirect_ptr_t, iblock->child_iblocks,
                                              (size_t)(indir_rows * hdr->man_dtable.cparam.width))))
                HGOTO_ERROR(H5E_HEAP, H5E_NOSPACE, FAIL,
                            "memory allocation failed for filtered direct entries");
        } /* end if */
        else
            iblock->child_iblocks =
                (H5HF_indirect_ptr_t *)H5FL_SEQ_FREE(H5HF_indirect_ptr_t, iblock->child_iblocks);
    } /* end if */

    /* Mark indirect block as dirty */
    if (H5HF__iblock_dirty(iblock) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTDIRTY, FAIL, "can't mark indirect block as dirty");

    /* Update other shared header info */
    hdr->man_dtable.curr_root_rows = new_nrows;
    hdr->man_dtable.table_addr     = new_addr;

    /* Shrink heap to only cover new root indirect block */
    if (H5HF__hdr_adjust_heap(hdr, 2 * hdr->man_dtable.row_block_off[new_nrows - 1],
                              -(hssize_t)acc_dblock_free) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTSHRINK, FAIL, "can't reduce space to cover root direct block");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_iblock_root_halve() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_iblock_root_revert
 *
 * Purpose:	Revert root indirect block back to root direct block
 *
 * Note:	Any sections left pointing to the  old root indirect block
 *              will be cleaned up by the free space manager
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__man_iblock_root_revert(H5HF_indirect_t *root_iblock)
{
    H5HF_hdr_t    *hdr;                 /* Pointer to heap's header */
    H5HF_direct_t *dblock = NULL;       /* Pointer to new root indirect block */
    haddr_t        dblock_addr;         /* Direct block's address in the file */
    size_t         dblock_size;         /* Direct block's size */
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(root_iblock);

    /* Set up local convenience variables */
    hdr         = root_iblock->hdr;
    dblock_addr = root_iblock->ents[0].addr;
    dblock_size = hdr->man_dtable.cparam.start_block_size;

    /* Get pointer to last direct block */
    if (NULL == (dblock = H5HF__man_dblock_protect(hdr, dblock_addr, dblock_size, root_iblock, 0,
                                                   H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to protect fractal heap direct block");
    assert(dblock->parent == root_iblock);
    assert(dblock->par_entry == 0);

    /* Check for I/O filters on this heap */
    if (hdr->filter_len > 0) {
        /* Set the header's pipeline information from the indirect block */
        hdr->pline_root_direct_size        = root_iblock->filt_ents[0].size;
        hdr->pline_root_direct_filter_mask = root_iblock->filt_ents[0].filter_mask;
    } /* end if */

    /* Destroy flush dependency between old root iblock and new root direct block */
    if (H5AC_destroy_flush_dependency(dblock->fd_parent, dblock) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTUNDEPEND, FAIL, "unable to destroy flush dependency");
    dblock->fd_parent = NULL;

    /* Detach direct block from parent */
    if (H5HF__man_iblock_detach(dblock->parent, 0) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTATTACH, FAIL, "can't detach direct block from parent indirect block");
    dblock->parent    = NULL;
    dblock->par_entry = 0;

    /* Create flush dependency between header and new root direct block */
    if (H5AC_create_flush_dependency(hdr, dblock) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTDEPEND, FAIL, "unable to create flush dependency");
    dblock->fd_parent = hdr;

    /* Point root at direct block */
    hdr->man_dtable.curr_root_rows = 0;
    hdr->man_dtable.table_addr     = dblock_addr;

    /* Reset 'next block' iterator */
    if (H5HF__hdr_reset_iter(hdr, (hsize_t)dblock_size) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't reset block iterator");

    /* Extend heap to just cover first direct block */
    if (H5HF__hdr_adjust_heap(hdr, (hsize_t)hdr->man_dtable.cparam.start_block_size,
                              (hssize_t)hdr->man_dtable.row_tot_dblock_free[0]) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTEXTEND, FAIL, "can't increase space to cover root direct block");

    /* Scan free space sections to reset any 'parent' pointers */
    if (H5HF__space_revert_root(hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTRESET, FAIL, "can't reset free space section info");

done:
    if (dblock && H5AC_unprotect(hdr->f, H5AC_FHEAP_DBLOCK, dblock_addr, dblock, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to release fractal heap direct block");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_iblock_root_revert() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_iblock_alloc_row
 *
 * Purpose:	Allocate a "single" section for an object, out of a
 *              "row" section.
 *
 * Note:	Creates necessary direct & indirect blocks
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_iblock_alloc_row(H5HF_hdr_t *hdr, H5HF_free_section_t **sec_node)
{
    H5HF_indirect_t     *iblock       = NULL;      /* Pointer to indirect block */
    H5HF_free_section_t *old_sec_node = *sec_node; /* Pointer to old indirect section node */
    unsigned             dblock_entry;             /* Entry for direct block */
    bool                 iblock_held = false;      /* Flag to indicate that indirect block is held */
    herr_t               ret_value   = SUCCEED;    /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(sec_node && old_sec_node);
    assert(old_sec_node->u.row.row < hdr->man_dtable.max_direct_rows);

    /* Check for serialized row section, or serialized / deleted indirect
     * section under it. */
    if (old_sec_node->sect_info.state == H5FS_SECT_SERIALIZED ||
        (H5FS_SECT_SERIALIZED == old_sec_node->u.row.under->sect_info.state) ||
        (true == old_sec_node->u.row.under->u.indirect.u.iblock->removed_from_cache))
        /* Revive row and / or indirect section */
        if (H5HF__sect_row_revive(hdr, old_sec_node) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTREVIVE, FAIL, "can't revive indirect section");

    /* Get a pointer to the indirect block covering the section */
    if (NULL == (iblock = H5HF__sect_row_get_iblock(old_sec_node)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't retrieve indirect block for row section");

    /* Hold indirect block in memory, until direct block can point to it */
    if (H5HF__iblock_incr(iblock) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINC, FAIL, "can't increment reference count on shared indirect block");
    iblock_held = true;

    /* Reduce (& possibly re-add) 'row' section */
    if (H5HF__sect_row_reduce(hdr, old_sec_node, &dblock_entry) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTSHRINK, FAIL, "can't reduce row section node");

    /* Create direct block & single section */
    if (H5HF__man_dblock_create(hdr, iblock, dblock_entry, NULL, sec_node) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, FAIL, "can't allocate fractal heap direct block");

done:
    /* Release hold on indirect block */
    if (iblock_held)
        if (H5HF__iblock_decr(iblock) < 0)
            HDONE_ERROR(H5E_HEAP, H5E_CANTDEC, FAIL,
                        "can't decrement reference count on shared indirect block")

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_iblock_alloc_row() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_iblock_create
 *
 * Purpose:	Allocate & initialize a managed indirect block
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_iblock_create(H5HF_hdr_t *hdr, H5HF_indirect_t *par_iblock, unsigned par_entry, unsigned nrows,
                        unsigned max_rows, haddr_t *addr_p)
{
    H5HF_indirect_t *iblock = NULL;       /* Pointer to indirect block */
    size_t           u;                   /* Local index variable */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(nrows > 0);
    assert(addr_p);

    /*
     * Allocate file and memory data structures.
     */
    if (NULL == (iblock = H5FL_MALLOC(H5HF_indirect_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                    "memory allocation failed for fractal heap indirect block");

    /* Reset the metadata cache info for the heap header */
    memset(&iblock->cache_info, 0, sizeof(H5AC_info_t));

    /* Share common heap information */
    iblock->hdr = hdr;
    if (H5HF__hdr_incr(hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINC, FAIL, "can't increment reference count on shared heap header");

    /* Set info for indirect block */
    iblock->rc                 = 0;
    iblock->nrows              = nrows;
    iblock->max_rows           = max_rows;
    iblock->removed_from_cache = false;

    /* Compute size of buffer needed for indirect block */
    iblock->size = H5HF_MAN_INDIRECT_SIZE(hdr, iblock->nrows);

    /* Allocate child block entry array */
    if (NULL == (iblock->ents = H5FL_SEQ_MALLOC(H5HF_indirect_ent_t,
                                                (size_t)(iblock->nrows * hdr->man_dtable.cparam.width))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed for block entries");

    /* Initialize indirect block entry tables */
    for (u = 0; u < (iblock->nrows * hdr->man_dtable.cparam.width); u++)
        iblock->ents[u].addr = HADDR_UNDEF;

    /* Check for I/O filters to apply to this heap */
    if (hdr->filter_len > 0) {
        unsigned dir_rows; /* Number of direct rows in this indirect block */

        /* Compute the number of direct rows for this indirect block */
        dir_rows = MIN(iblock->nrows, hdr->man_dtable.max_direct_rows);

        /* Allocate & initialize indirect block filtered entry array */
        if (NULL == (iblock->filt_ents = H5FL_SEQ_CALLOC(H5HF_indirect_filt_ent_t,
                                                         (size_t)(dir_rows * hdr->man_dtable.cparam.width))))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed for block entries");
    } /* end if */
    else
        iblock->filt_ents = NULL;

    /* Check if we have any indirect block children */
    if (iblock->nrows > hdr->man_dtable.max_direct_rows) {
        unsigned indir_rows; /* Number of indirect rows in this indirect block */

        /* Compute the number of indirect rows for this indirect block */
        indir_rows = iblock->nrows - hdr->man_dtable.max_direct_rows;

        /* Allocate & initialize child indirect block pointer array */
        if (NULL == (iblock->child_iblocks = H5FL_SEQ_CALLOC(
                         H5HF_indirect_ptr_t, (size_t)(indir_rows * hdr->man_dtable.cparam.width))))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed for block entries");
    } /* end if */
    else
        iblock->child_iblocks = NULL;

    /* Allocate [temporary] space for the indirect block on disk */
    if (H5F_USE_TMP_SPACE(hdr->f)) {
        if (HADDR_UNDEF == (*addr_p = H5MF_alloc_tmp(hdr->f, (hsize_t)iblock->size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                        "file allocation failed for fractal heap indirect block");
    } /* end if */
    else {
        if (HADDR_UNDEF == (*addr_p = H5MF_alloc(hdr->f, H5FD_MEM_FHEAP_IBLOCK, (hsize_t)iblock->size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                        "file allocation failed for fractal heap indirect block");
    } /* end else */
    iblock->addr = *addr_p;

    /* Attach to parent indirect block, if there is one */
    iblock->parent    = par_iblock;
    iblock->par_entry = par_entry;
    if (iblock->parent) {
        /* Attach new block to parent */
        if (H5HF__man_iblock_attach(iblock->parent, par_entry, *addr_p) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTATTACH, FAIL,
                        "can't attach indirect block to parent indirect block");

        /* Compute the indirect block's offset in the heap's address space */
        /* (based on parent's block offset) */
        iblock->block_off = par_iblock->block_off;
        iblock->block_off += hdr->man_dtable.row_block_off[par_entry / hdr->man_dtable.cparam.width];
        iblock->block_off += hdr->man_dtable.row_block_size[par_entry / hdr->man_dtable.cparam.width] *
                             (par_entry % hdr->man_dtable.cparam.width);

        /* Set indirect block parent as flush dependency parent */
        iblock->fd_parent = par_iblock;
    } /* end if */
    else {
        iblock->block_off = 0; /* Must be the root indirect block... */

        /* Set heap header as flush dependency parent */
        iblock->fd_parent = hdr;
    } /* end else */

    /* Update indirect block's statistics */
    iblock->nchildren = 0;
    iblock->max_child = 0;

    /* Cache the new indirect block */
    if (H5AC_insert_entry(hdr->f, H5AC_FHEAP_IBLOCK, *addr_p, iblock, H5AC__NO_FLAGS_SET) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't add fractal heap indirect block to cache");

done:
    if (ret_value < 0)
        if (iblock)
            if (H5HF__man_iblock_dest(iblock) < 0)
                HDONE_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "unable to destroy fractal heap indirect block");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_iblock_create() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_iblock_protect
 *
 * Purpose:	Convenience wrapper around H5AC_protect on an indirect block
 *
 * Return:	Pointer to indirect block on success, NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5HF_indirect_t *
H5HF__man_iblock_protect(H5HF_hdr_t *hdr, haddr_t iblock_addr, unsigned iblock_nrows,
                         H5HF_indirect_t *par_iblock, unsigned par_entry, bool must_protect, unsigned flags,
                         bool *did_protect)
{
    H5HF_parent_t    par_info;               /* Parent info for loading block */
    H5HF_indirect_t *iblock         = NULL;  /* Indirect block from cache */
    bool             should_protect = false; /* Whether we should protect the indirect block or not */
    H5HF_indirect_t *ret_value      = NULL;  /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(H5_addr_defined(iblock_addr));
    assert(iblock_nrows > 0);
    assert(did_protect);

    /* only H5AC__READ_ONLY_FLAG may appear in flags */
    assert((flags & (unsigned)(~H5AC__READ_ONLY_FLAG)) == 0);

    /* Check if we are allowed to use existing pinned iblock pointer */
    if (!must_protect) {
        /* Check for this block already being pinned */
        if (par_iblock) {
            unsigned indir_idx; /* Index in parent's child iblock pointer array */

            /* Sanity check */
            assert(par_iblock->child_iblocks);
            assert(par_entry >= (hdr->man_dtable.max_direct_rows * hdr->man_dtable.cparam.width));

            /* Compute index in parent's child iblock pointer array */
            indir_idx = par_entry - (hdr->man_dtable.max_direct_rows * hdr->man_dtable.cparam.width);

            /* Check for pointer to pinned indirect block in parent */
            if (par_iblock->child_iblocks[indir_idx])
                iblock = par_iblock->child_iblocks[indir_idx];
            else
                should_protect = true;
        } /* end if */
        else {
            /* Check for root indirect block */
            if (H5_addr_eq(iblock_addr, hdr->man_dtable.table_addr)) {
                /* Check for valid pointer to pinned indirect block in root */
                if (H5HF_ROOT_IBLOCK_PINNED == hdr->root_iblock_flags) {
                    /* Sanity check */
                    assert(NULL != hdr->root_iblock);

                    /* Return the pointer to the pinned root indirect block */
                    iblock = hdr->root_iblock;
                } /* end if */
                else {
                    /* Sanity check */
                    assert(NULL == hdr->root_iblock);

                    should_protect = true;
                } /* end else */
            }     /* end if */
            else
                should_protect = true;
        } /* end else */
    }     /* end if */

    /* Check for protecting indirect block */
    if (must_protect || should_protect) {
        H5HF_iblock_cache_ud_t cache_udata; /* User-data for callback */

        /* Set up parent info */
        par_info.hdr    = hdr;
        par_info.iblock = par_iblock;
        par_info.entry  = par_entry;

        /* Set up user data for protect call */
        cache_udata.f        = hdr->f;
        cache_udata.par_info = &par_info;
        cache_udata.nrows    = &iblock_nrows;

        /* Protect the indirect block */
        if (NULL == (iblock = (H5HF_indirect_t *)H5AC_protect(hdr->f, H5AC_FHEAP_IBLOCK, iblock_addr,
                                                              &cache_udata, flags)))
            HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, NULL, "unable to protect fractal heap indirect block");

        /* Set the indirect block's address */
        iblock->addr = iblock_addr;

        /* Check for root indirect block */
        if (iblock->block_off == 0) {
            /* Sanity check - shouldn't be recursively protecting root indirect block */
            assert(0 == (hdr->root_iblock_flags & H5HF_ROOT_IBLOCK_PROTECTED));

            /* Check if we should set the root iblock pointer */
            if (0 == hdr->root_iblock_flags) {
                assert(NULL == hdr->root_iblock);
                hdr->root_iblock = iblock;
            } /* end if */

            /* Indicate that the root indirect block is protected */
            hdr->root_iblock_flags |= H5HF_ROOT_IBLOCK_PROTECTED;
        } /* end if */

        /* Indicate that the indirect block was protected */
        *did_protect = true;
    } /* end if */
    else
        /* Indicate that the indirect block was _not_ protected */
        *did_protect = false;

    /* Set the return value */
    ret_value = iblock;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_iblock_protect() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_iblock_unprotect
 *
 * Purpose:	Convenience wrapper around H5AC_unprotect on an indirect block
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_iblock_unprotect(H5HF_indirect_t *iblock, unsigned cache_flags, bool did_protect)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(iblock);

    /* Check if we previously protected this indirect block */
    /* (as opposed to using an existing pointer to a pinned child indirect block) */
    if (did_protect) {
        /* Check for root indirect block */
        if (iblock->block_off == 0) {
            /* Sanity check - shouldn't be recursively unprotecting root indirect block */
            assert(iblock->hdr->root_iblock_flags & H5HF_ROOT_IBLOCK_PROTECTED);

            /* Check if we should reset the root iblock pointer */
            if (H5HF_ROOT_IBLOCK_PROTECTED == iblock->hdr->root_iblock_flags) {
                assert(NULL != iblock->hdr->root_iblock);
                iblock->hdr->root_iblock = NULL;
            } /* end if */

            /* Indicate that the root indirect block is unprotected */
            iblock->hdr->root_iblock_flags &= (unsigned)(~(H5HF_ROOT_IBLOCK_PROTECTED));
        } /* end if */

        /* Unprotect the indirect block */
        if (H5AC_unprotect(iblock->hdr->f, H5AC_FHEAP_IBLOCK, iblock->addr, iblock, cache_flags) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to release fractal heap indirect block");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_iblock_unprotect() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_iblock_attach
 *
 * Purpose:	Attach a child block (direct or indirect) to an indirect block
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_iblock_attach(H5HF_indirect_t *iblock, unsigned entry, haddr_t child_addr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(iblock);
    assert(H5_addr_defined(child_addr));
    assert(!H5_addr_defined(iblock->ents[entry].addr));

    /* Increment the reference count on this indirect block */
    if (H5HF__iblock_incr(iblock) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINC, FAIL, "can't increment reference count on shared indirect block");

    /* Point at the child block */
    iblock->ents[entry].addr = child_addr;

    /* Check for I/O filters on this heap */
    if (iblock->hdr->filter_len > 0) {
        unsigned row; /* Row for entry */

        /* Sanity check */
        assert(iblock->filt_ents);

        /* Compute row for entry */
        row = entry / iblock->hdr->man_dtable.cparam.width;

        /* If this is a direct block, set its initial size */
        if (row < iblock->hdr->man_dtable.max_direct_rows)
            iblock->filt_ents[entry].size = iblock->hdr->man_dtable.row_block_size[row];
    } /* end if */

    /* Check for max. entry used */
    if (entry > iblock->max_child)
        iblock->max_child = entry;

    /* Increment the # of child blocks */
    iblock->nchildren++;

    /* Mark indirect block as modified */
    if (H5HF__iblock_dirty(iblock) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTDIRTY, FAIL, "can't mark indirect block as dirty");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_iblock_attach() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_iblock_detach
 *
 * Purpose:	Detach a child block (direct or indirect) from an indirect block
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_iblock_detach(H5HF_indirect_t *iblock, unsigned entry)
{
    H5HF_hdr_t      *hdr;                 /* Fractal heap header */
    H5HF_indirect_t *del_iblock = NULL;   /* Pointer to protected indirect block, when deleting */
    unsigned         row;                 /* Row for entry */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(iblock);
    assert(iblock->nchildren);

    /* Set up convenience variables */
    hdr = iblock->hdr;

    /* Reset address of entry */
    iblock->ents[entry].addr = HADDR_UNDEF;

    /* Compute row for entry */
    row = entry / hdr->man_dtable.cparam.width;

    /* Check for I/O filters on this heap */
    if (hdr->filter_len > 0) {
        /* Sanity check */
        assert(iblock->filt_ents);

        /* If this is a direct block, reset its initial size */
        if (row < hdr->man_dtable.max_direct_rows) {
            iblock->filt_ents[entry].size        = 0;
            iblock->filt_ents[entry].filter_mask = 0;
        } /* end if */
    }     /* end if */

    /* Check for indirect block being detached */
    if (row >= hdr->man_dtable.max_direct_rows) {
        unsigned indir_idx; /* Index in parent's child iblock pointer array */

        /* Sanity check */
        assert(iblock->child_iblocks);

        /* Compute index in child iblock pointer array */
        indir_idx = entry - (hdr->man_dtable.max_direct_rows * hdr->man_dtable.cparam.width);

        /* Sanity check */
        assert(iblock->child_iblocks[indir_idx]);

        /* Reset pointer to child indirect block in parent */
        iblock->child_iblocks[indir_idx] = NULL;
    } /* end if */

    /* Decrement the # of child blocks */
    /* (If the number of children drop to 0, the indirect block will be
     *  removed from the heap when its ref. count drops to zero and the
     *  metadata cache calls the indirect block destructor)
     */
    iblock->nchildren--;

    /* Reduce the max. entry used, if necessary */
    if (entry == iblock->max_child) {
        if (iblock->nchildren > 0)
            while (!H5_addr_defined(iblock->ents[iblock->max_child].addr))
                iblock->max_child--;
        else
            iblock->max_child = 0;
    } /* end if */

    /* If this is the root indirect block handle some special cases */
    if (iblock->block_off == 0) {
        /* If the number of children drops to 1, and that child is the first
         *      direct block in the heap, convert the heap back to using a root
         *      direct block
         */
        if (iblock->nchildren == 1 && H5_addr_defined(iblock->ents[0].addr))
            if (H5HF__man_iblock_root_revert(iblock) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTSHRINK, FAIL,
                            "can't convert root indirect block back to root direct block");

        /* If the indirect block wasn't removed already (by reverting it) */
        if (!iblock->removed_from_cache) {
            /* Check for reducing size of root indirect block */
            if (iblock->nchildren > 0 && hdr->man_dtable.cparam.start_root_rows != 0 &&
                entry > iblock->max_child) {
                unsigned max_child_row; /* Row for max. child entry */

                /* Compute information needed for determining whether to reduce size of root indirect block */
                max_child_row = iblock->max_child / hdr->man_dtable.cparam.width;

                /* Check if the root indirect block should be reduced */
                if (iblock->nrows > 1 && max_child_row <= (iblock->nrows / 2))
                    if (H5HF__man_iblock_root_halve(iblock) < 0)
                        HGOTO_ERROR(H5E_HEAP, H5E_CANTSHRINK, FAIL,
                                    "can't reduce size of root indirect block");
            } /* end if */
        }     /* end if */
    }         /* end if */

    /* If the indirect block wasn't removed already (by reverting it) */
    if (!iblock->removed_from_cache) {
        /* Mark indirect block as modified */
        if (H5HF__iblock_dirty(iblock) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTDIRTY, FAIL, "can't mark indirect block as dirty");

        /* Check for last child being removed from indirect block */
        if (iblock->nchildren == 0) {
            bool did_protect = false; /* Whether the indirect block was protected */

            /* If this indirect block's refcount is >1, then it's being deleted
             *	from the fractal heap (since its nchildren == 0), but is still
             *	referred to from free space sections in the heap (refcount >1).
             *	Its space in the file needs to be freed now, and it also needs
             *	to be removed from the metadata cache now, in case the space in
             *	the file is reused by another piece of metadata that is inserted
             *	into the cache before the indirect block's entry is evicted
             *	(having two entries at the same address would be an error, from
             *	the cache's perspective).
             */
            /* Lock indirect block for deletion */
            if (NULL == (del_iblock = H5HF__man_iblock_protect(hdr, iblock->addr, iblock->nrows,
                                                               iblock->parent, iblock->par_entry, true,
                                                               H5AC__NO_FLAGS_SET, &did_protect)))
                HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to protect fractal heap indirect block");
            assert(did_protect == true);

            /* Check for deleting root indirect block (and no root direct block) */
            if (iblock->block_off == 0 && hdr->man_dtable.curr_root_rows > 0)
                /* Reset header information back to "empty heap" state */
                if (H5HF__hdr_empty(hdr) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTSHRINK, FAIL, "can't make heap empty");

            /* Detach from parent indirect block */
            if (iblock->parent) {
                /* Destroy flush dependency between indirect block and parent */
                if (H5AC_destroy_flush_dependency(iblock->fd_parent, iblock) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTUNDEPEND, FAIL, "unable to destroy flush dependency");
                iblock->fd_parent = NULL;

                /* Detach from parent indirect block */
                if (H5HF__man_iblock_detach(iblock->parent, iblock->par_entry) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTATTACH, FAIL, "can't detach from parent indirect block");
                iblock->parent    = NULL;
                iblock->par_entry = 0;
            } /* end if */
        }     /* end if */
    }         /* end if */

    /* Decrement the reference count on this indirect block if we're not deleting it */
    /* (should be after iblock needs to be modified, so that potential 'unpin'
     * on this indirect block doesn't invalidate the 'iblock' variable, if it's
     * not being deleted)
     */
    if (H5HF__iblock_decr(iblock) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTDEC, FAIL, "can't decrement reference count on shared indirect block");
    iblock = NULL;

    /* Delete indirect block from cache, if appropriate */
    if (del_iblock) {
        unsigned cache_flags    = H5AC__NO_FLAGS_SET; /* Flags for unprotect */
        bool     took_ownership = false; /* Flag to indicate that block ownership has transitioned */

        /* If the refcount is still >0, unpin the block and take ownership
         * 	from the cache, otherwise let the cache destroy it.
         */
        if (del_iblock->rc > 0) {
            cache_flags |= (H5AC__DELETED_FLAG | H5AC__TAKE_OWNERSHIP_FLAG);
            cache_flags |= H5AC__UNPIN_ENTRY_FLAG;
            took_ownership = true;
        } /* end if */
        else {
            /* Entry should be removed from the cache */
            cache_flags |= H5AC__DELETED_FLAG;

            /* If the indirect block is in real file space, tell
             * the cache to free its file space as well.
             */
            if (!H5F_IS_TMP_ADDR(hdr->f, del_iblock->addr))
                cache_flags |= H5AC__FREE_FILE_SPACE_FLAG;
        } /* end else */

        /* Unprotect the indirect block, with appropriate flags */
        if (H5HF__man_iblock_unprotect(del_iblock, cache_flags, true) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to release fractal heap indirect block");

        /* if took ownership, free file space & mark block as removed from cache */
        if (took_ownership) {
            /* Free indirect block disk space, if it's in real space */
            if (!H5F_IS_TMP_ADDR(hdr->f, del_iblock->addr))
                if (H5MF_xfree(hdr->f, H5FD_MEM_FHEAP_IBLOCK, del_iblock->addr, (hsize_t)del_iblock->size) <
                    0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL,
                                "unable to free fractal heap indirect block file space");
            del_iblock->addr = HADDR_UNDEF;

            /* Mark block as removed from the cache */
            del_iblock->removed_from_cache = true;
        } /* end if */
    }     /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_iblock_detach() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_iblock_entry_addr
 *
 * Purpose:	Retrieve the address of an indirect block's child
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_iblock_entry_addr(H5HF_indirect_t *iblock, unsigned entry, haddr_t *child_addr)
{
    FUNC_ENTER_PACKAGE_NOERR

    /*
     * Check arguments.
     */
    assert(iblock);
    assert(child_addr);

    /* Retrieve address of entry */
    *child_addr = iblock->ents[entry].addr;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HF__man_iblock_entry_addr() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_iblock_delete
 *
 * Purpose:	Delete a managed indirect block
 *
 * Note:	This routine does _not_ modify any indirect block that points
 *              to this indirect block, it is assumed that the whole heap is
 *              being deleted in a top-down fashion.
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_iblock_delete(H5HF_hdr_t *hdr, haddr_t iblock_addr, unsigned iblock_nrows,
                        H5HF_indirect_t *par_iblock, unsigned par_entry)
{
    H5HF_indirect_t *iblock;                           /* Pointer to indirect block */
    unsigned         row, col;                         /* Current row & column in indirect block */
    unsigned         entry;                            /* Current entry in row */
    unsigned         cache_flags = H5AC__NO_FLAGS_SET; /* Flags for unprotecting indirect block */
    bool             did_protect;                      /* Whether we protected the indirect block or not */
    herr_t           ret_value = SUCCEED;              /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(H5_addr_defined(iblock_addr));
    assert(iblock_nrows > 0);

    /* Lock indirect block */
    if (NULL == (iblock = H5HF__man_iblock_protect(hdr, iblock_addr, iblock_nrows, par_iblock, par_entry,
                                                   true, H5AC__NO_FLAGS_SET, &did_protect)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to protect fractal heap indirect block");
    assert(iblock->nchildren > 0);
    assert(did_protect == true);

    /* Iterate over rows in this indirect block */
    entry = 0;
    for (row = 0; row < iblock->nrows; row++) {
        /* Iterate over entries in this row */
        for (col = 0; col < hdr->man_dtable.cparam.width; col++, entry++) {
            /* Check for child entry at this position */
            if (H5_addr_defined(iblock->ents[entry].addr)) {
                /* Are we in a direct or indirect block row */
                if (row < hdr->man_dtable.max_direct_rows) {
                    hsize_t dblock_size; /* Size of direct block on disk */

                    /* Check for I/O filters on this heap */
                    if (hdr->filter_len > 0)
                        dblock_size = iblock->filt_ents[entry].size;
                    else
                        dblock_size = hdr->man_dtable.row_block_size[row];

                    /* Delete child direct block */
                    if (H5HF__man_dblock_delete(hdr->f, iblock->ents[entry].addr, dblock_size) < 0)
                        HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL,
                                    "unable to release fractal heap child direct block");
                } /* end if */
                else {
                    hsize_t  row_block_size; /* The size of blocks in this row */
                    unsigned child_nrows;    /* Number of rows in new indirect block */

                    /* Get the row's block size */
                    row_block_size = (hsize_t)hdr->man_dtable.row_block_size[row];

                    /* Compute # of rows in next child indirect block to use */
                    child_nrows = H5HF__dtable_size_to_rows(&hdr->man_dtable, row_block_size);

                    /* Delete child indirect block */
                    if (H5HF__man_iblock_delete(hdr, iblock->ents[entry].addr, child_nrows, iblock, entry) <
                        0)
                        HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL,
                                    "unable to release fractal heap child indirect block");
                } /* end else */
            }     /* end if */
        }         /* end for */
    }             /* end row */

#ifndef NDEBUG
    {
        unsigned iblock_status = 0; /* Indirect block's status in the metadata cache */

        /* Check the indirect block's status in the metadata cache */
        if (H5AC_get_entry_status(hdr->f, iblock_addr, &iblock_status) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL,
                        "unable to check metadata cache status for indirect block");

        /* Check if indirect block is pinned */
        assert(!(iblock_status & H5AC_ES__IS_PINNED));
    }
#endif /* NDEBUG */

    /* Indicate that the indirect block should be deleted  */
    cache_flags |= H5AC__DIRTIED_FLAG | H5AC__DELETED_FLAG;

    /* If the indirect block is in real file space, tell
     * the cache to free its file space as well.
     */
    if (!H5F_IS_TMP_ADDR(hdr->f, iblock_addr))
        cache_flags |= H5AC__FREE_FILE_SPACE_FLAG;

done:
    /* Unprotect the indirect block, with appropriate flags */
    if (iblock && H5HF__man_iblock_unprotect(iblock, cache_flags, did_protect) < 0)
        HDONE_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to release fractal heap indirect block");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_iblock_delete() */

/*-------------------------------------------------------------------------
 *
 * Function:    H5HF__man_iblock_size
 *
 * Purpose:     Gather storage used for the indirect block in fractal heap
 *
 * Return:      non-negative on success, negative on error
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_iblock_size(H5F_t *f, H5HF_hdr_t *hdr, haddr_t iblock_addr, unsigned nrows,
                      H5HF_indirect_t *par_iblock, unsigned par_entry, hsize_t *heap_size)
{
    H5HF_indirect_t *iblock = NULL;       /* Pointer to indirect block */
    bool             did_protect;         /* Whether we protected the indirect block or not */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);
    assert(hdr);
    assert(H5_addr_defined(iblock_addr));
    assert(heap_size);

    /* Protect the indirect block */
    if (NULL == (iblock = H5HF__man_iblock_protect(hdr, iblock_addr, nrows, par_iblock, par_entry, false,
                                                   H5AC__READ_ONLY_FLAG, &did_protect)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTLOAD, FAIL, "unable to load fractal heap indirect block");

    /* Accumulate size of this indirect block */
    *heap_size += iblock->size;

    /* Indirect entries in this indirect block */
    if (iblock->nrows > hdr->man_dtable.max_direct_rows) {
        unsigned first_row_bits;    /* Number of bits used bit addresses in first row */
        unsigned num_indirect_rows; /* Number of rows of blocks in each indirect block */
        unsigned entry;             /* Current entry in row */
        size_t   u;                 /* Local index variable */

        entry          = hdr->man_dtable.max_direct_rows * hdr->man_dtable.cparam.width;
        first_row_bits = H5VM_log2_of2((uint32_t)hdr->man_dtable.cparam.start_block_size) +
                         H5VM_log2_of2(hdr->man_dtable.cparam.width);
        num_indirect_rows = (H5VM_log2_gen(hdr->man_dtable.row_block_size[hdr->man_dtable.max_direct_rows]) -
                             first_row_bits) +
                            1;
        for (u = hdr->man_dtable.max_direct_rows; u < iblock->nrows; u++, num_indirect_rows++) {
            size_t v; /* Local index variable */

            for (v = 0; v < hdr->man_dtable.cparam.width; v++, entry++)
                if (H5_addr_defined(iblock->ents[entry].addr))
                    if (H5HF__man_iblock_size(f, hdr, iblock->ents[entry].addr, num_indirect_rows, iblock,
                                              entry, heap_size) < 0)
                        HGOTO_ERROR(H5E_HEAP, H5E_CANTLOAD, FAIL,
                                    "unable to get fractal heap storage info for indirect block");
        } /* end for */
    }     /* end if */

done:
    /* Release the indirect block */
    if (iblock && H5HF__man_iblock_unprotect(iblock, H5AC__NO_FLAGS_SET, did_protect) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to release fractal heap indirect block");
    iblock = NULL;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_iblock_size() */

/*-------------------------------------------------------------------------
 *
 * Function:    H5HF_man_iblock_parent_info
 *
 * Purpose:     Determine the parent block's offset and entry location
 *		(within it's parent) of an indirect block, given its offset
 *		within the heap.
 *
 * Return:	Non-negative on success / Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_iblock_parent_info(const H5HF_hdr_t *hdr, hsize_t block_off, hsize_t *ret_par_block_off,
                             unsigned *ret_entry)
{
    hsize_t  par_block_off;              /* Offset of parent within heap */
    hsize_t  prev_par_block_off;         /* Offset of previous parent block within heap */
    unsigned row, col;                   /* Row & column for block */
    unsigned prev_row = 0, prev_col = 0; /* Previous row & column for block */
    herr_t   ret_value = SUCCEED;        /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(block_off > 0);
    assert(ret_entry);

    /* Look up row & column for object */
    if (H5HF__dtable_lookup(&hdr->man_dtable, block_off, &row, &col) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTCOMPUTE, FAIL, "can't compute row & column of block");

    /* Sanity check - first lookup must be an indirect block */
    assert(row >= hdr->man_dtable.max_direct_rows);

    /* Traverse down, until a direct block at the offset is found, then
     *	use previous (i.e. parent's) offset, row, and column.
     */
    prev_par_block_off = par_block_off = 0;
    while (row >= hdr->man_dtable.max_direct_rows) {
        /* Retain previous parent block offset */
        prev_par_block_off = par_block_off;

        /* Compute the new parent indirect block's offset in the heap's address space */
        /* (based on previous block offset) */
        par_block_off += hdr->man_dtable.row_block_off[row];
        par_block_off += hdr->man_dtable.row_block_size[row] * col;

        /* Preserve current row & column */
        prev_row = row;
        prev_col = col;

        /* Look up row & column in new indirect block for object */
        if (H5HF__dtable_lookup(&hdr->man_dtable, (block_off - par_block_off), &row, &col) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTCOMPUTE, FAIL, "can't compute row & column of block");
    } /* end while */

    /* Sanity check */
    assert(row == 0);
    assert(col == 0);

    /* Set return parameters */
    *ret_par_block_off = prev_par_block_off;
    *ret_entry         = (prev_row * hdr->man_dtable.cparam.width) + prev_col;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_iblock_par_info() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_iblock_dest
 *
 * Purpose:	Destroys a fractal heap indirect block in memory.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_iblock_dest(H5HF_indirect_t *iblock)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(iblock);
    assert(iblock->rc == 0);

    /* Decrement reference count on shared info */
    assert(iblock->hdr);
    if (H5HF__hdr_decr(iblock->hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTDEC, FAIL, "can't decrement reference count on shared heap header");
    if (iblock->parent)
        if (H5HF__iblock_decr(iblock->parent) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTDEC, FAIL,
                        "can't decrement reference count on shared indirect block");

    /* Release entry tables */
    if (iblock->ents)
        iblock->ents = H5FL_SEQ_FREE(H5HF_indirect_ent_t, iblock->ents);
    if (iblock->filt_ents)
        iblock->filt_ents = H5FL_SEQ_FREE(H5HF_indirect_filt_ent_t, iblock->filt_ents);
    if (iblock->child_iblocks)
        iblock->child_iblocks = H5FL_SEQ_FREE(H5HF_indirect_ptr_t, iblock->child_iblocks);

    /* Free fractal heap indirect block info */
    iblock = H5FL_FREE(H5HF_indirect_t, iblock);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_iblock_dest() */
