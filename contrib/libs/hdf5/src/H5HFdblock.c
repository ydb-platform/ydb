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
 * Created:		H5HFdblock.c
 *
 * Purpose:		Direct block routines for fractal heaps.
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

/*********************/
/* Package Variables */
/*********************/

/* Declare a free list to manage the H5HF_direct_t struct */
H5FL_DEFINE(H5HF_direct_t);

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_dblock_create
 *
 * Purpose:	Allocate & initialize a managed direct block
 *
 * Return:	Pointer to new direct block on success, NULL on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_dblock_create(H5HF_hdr_t *hdr, H5HF_indirect_t *par_iblock, unsigned par_entry, haddr_t *addr_p,
                        H5HF_free_section_t **ret_sec_node)
{
    H5HF_free_section_t *sec_node;            /* Pointer to free space section for block */
    H5HF_direct_t       *dblock = NULL;       /* Pointer to direct block */
    haddr_t              dblock_addr;         /* Direct block's address */
    size_t               free_space;          /* Free space in new block */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);

    /*
     * Allocate file and memory data structures.
     */
    if (NULL == (dblock = H5FL_MALLOC(H5HF_direct_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                    "memory allocation failed for fractal heap direct block");

    /* Reset the metadata cache info for the heap header */
    memset(&dblock->cache_info, 0, sizeof(H5AC_info_t));

    /* Share common heap information */
    dblock->hdr = hdr;
    if (H5HF__hdr_incr(hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINC, FAIL, "can't increment reference count on shared heap header");

    /* Set info for direct block */
    if (par_iblock) {
        unsigned par_row = par_entry / hdr->man_dtable.cparam.width; /* Row for block */

        /* Compute offset & size, based on parent's information */
        dblock->block_off = par_iblock->block_off;
        dblock->block_off += hdr->man_dtable.row_block_off[par_row];
        dblock->block_off +=
            hdr->man_dtable.row_block_size[par_row] * (par_entry % hdr->man_dtable.cparam.width);
        H5_CHECKED_ASSIGN(dblock->size, size_t, hdr->man_dtable.row_block_size[par_row], hsize_t);
    } /* end if */
    else {
        /* Must be the root direct block */
        dblock->block_off = 0;
        dblock->size      = hdr->man_dtable.cparam.start_block_size;
    } /* end else */
    dblock->file_size = 0;
    free_space        = dblock->size - H5HF_MAN_ABS_DIRECT_OVERHEAD(hdr);

    /* Allocate buffer for block */
    /* XXX: Change to using free-list factories */
    if ((dblock->blk = H5FL_BLK_MALLOC(direct_block, dblock->size)) == NULL)
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");
    memset(dblock->blk, 0, dblock->size);

    dblock->write_buf  = NULL;
    dblock->write_size = 0;

    /* Allocate [temporary] space for the direct block on disk */
    if (H5F_USE_TMP_SPACE(hdr->f)) {
        if (HADDR_UNDEF == (dblock_addr = H5MF_alloc_tmp(hdr->f, (hsize_t)dblock->size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                        "file allocation failed for fractal heap direct block");
    } /* end if */
    else {
        if (HADDR_UNDEF == (dblock_addr = H5MF_alloc(hdr->f, H5FD_MEM_FHEAP_DBLOCK, (hsize_t)dblock->size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                        "file allocation failed for fractal heap direct block");
    } /* end else */

    /* Attach to parent indirect block, if there is one */
    dblock->parent = par_iblock;
    if (dblock->parent) {
        if (H5HF__man_iblock_attach(dblock->parent, par_entry, dblock_addr) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTATTACH, FAIL, "can't attach direct block to parent indirect block");
        dblock->fd_parent = par_iblock;
    } /* end if */
    else
        dblock->fd_parent = hdr;
    dblock->par_entry = par_entry;

    /* Create a new 'single' section for the free space in the block */
    if (NULL == (sec_node = H5HF__sect_single_new((dblock->block_off + H5HF_MAN_ABS_DIRECT_OVERHEAD(hdr)),
                                                  free_space, dblock->parent, dblock->par_entry)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't create section for new direct block's free space");

    /* Check what to do with section node */
    if (ret_sec_node)
        /* Pass back the pointer to the section instead of adding it to the free list */
        *ret_sec_node = sec_node;
    else {
        /* Add new free space to the heap's list of space */
        if (H5HF__space_add(hdr, sec_node, 0) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't add direct block free space to global list");
    } /* end else */

    /* Cache the new fractal heap direct block */
    if (H5AC_insert_entry(hdr->f, H5AC_FHEAP_DBLOCK, dblock_addr, dblock, H5AC__NO_FLAGS_SET) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't add fractal heap direct block to cache");

    /* Increase the allocated heap size */
    if (H5HF__hdr_inc_alloc(hdr, dblock->size) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't increase allocated heap size");

    /* Set the address of of direct block, if requested */
    if (addr_p)
        *addr_p = dblock_addr;

done:
    if (ret_value < 0)
        if (dblock)
            if (H5HF__man_dblock_dest(dblock) < 0)
                HDONE_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "unable to destroy fractal heap direct block");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_dblock_create() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_dblock_destroy
 *
 * Purpose:	Destroy a managed direct block
 *
 * Note:	This routine does _not_ insert a range section for the
 *              destroyed direct block, that must be handled by the
 *              caller.
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_dblock_destroy(H5HF_hdr_t *hdr, H5HF_direct_t *dblock, haddr_t dblock_addr, bool *parent_removed)
{
    hsize_t  dblock_size;                      /* Size of direct block on disk */
    unsigned cache_flags = H5AC__NO_FLAGS_SET; /* Flags for unprotecting indirect block */
    herr_t   ret_value   = SUCCEED;            /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(dblock);

    /* Check for I/O filters on this heap */
    if (hdr->filter_len > 0) {
        /* Check for root direct block */
        if (dblock->parent == NULL)
            /* Get direct block's actual size */
            dblock_size = (hsize_t)hdr->pline_root_direct_size;
        else {
            H5HF_indirect_t *par_iblock; /* Parent indirect block */
            unsigned         par_entry;  /* Entry in parent indirect block */

            /* Get parent information */
            par_iblock = dblock->parent;
            par_entry  = dblock->par_entry;

            /* Get direct block's actual size */
            dblock_size = (hsize_t)par_iblock->filt_ents[par_entry].size;
        } /* end else */
    }     /* end if */
    else
        dblock_size = (hsize_t)dblock->size;

    /* Reset the parent_removed flag */
    if (parent_removed)
        *parent_removed = false;

    /* Check for root direct block */
    if (hdr->man_dtable.curr_root_rows == 0) {
        /* Sanity check */
        assert(hdr->man_dtable.table_addr == dblock_addr);
        assert(hdr->man_dtable.cparam.start_block_size == dblock->size);

        /* Sanity check block iterator */
        assert(!H5HF__man_iter_ready(&hdr->next_block));

        /* Reset header information back to "empty heap" state */
        if (H5HF__hdr_empty(hdr) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTSHRINK, FAIL, "can't make heap empty");
    } /* end if */
    else {
        /* Adjust heap statistics */
        hdr->man_alloc_size -= dblock->size;

        /* Check for this direct block being the highest in the heap */
        if ((dblock->block_off + dblock->size) == hdr->man_iter_off)
            /* Move 'next block' iterator backwards (may shrink heap) */
            if (H5HF__hdr_reverse_iter(hdr, dblock_addr) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't reverse 'next block' iterator");

        /* Detach from parent indirect block */
        if (dblock->parent) {
            /* Destroy flush dependency between direct block and parent */
            if (H5AC_destroy_flush_dependency(dblock->fd_parent, dblock) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTUNDEPEND, FAIL, "unable to destroy flush dependency");
            dblock->fd_parent = NULL;

            /* If this is the last direct block for the indirect block, the
             *  indirect block will be removed when this direct block is detached
             */
            if (parent_removed && 1 == dblock->parent->nchildren)
                *parent_removed = true;

            if (H5HF__man_iblock_detach(dblock->parent, dblock->par_entry) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTATTACH, FAIL, "can't detach from parent indirect block");
            dblock->parent    = NULL;
            dblock->par_entry = 0;
        } /* end if */
    }     /* end else */

    /* Indicate that the direct block should be deleted */
    dblock->file_size = dblock_size;
    cache_flags |= H5AC__DIRTIED_FLAG | H5AC__DELETED_FLAG;

    /* If the dblock is in real file space, also tell the cache to free its file space */
    if (!H5F_IS_TMP_ADDR(hdr->f, dblock_addr))
        cache_flags |= H5AC__FREE_FILE_SPACE_FLAG;

done:
    /* Unprotect the indirect block, with appropriate flags */
    if (H5AC_unprotect(hdr->f, H5AC_FHEAP_DBLOCK, dblock_addr, dblock, cache_flags) < 0)
        HDONE_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to release fractal heap direct block");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_dblock_destroy() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_dblock_new
 *
 * Purpose:	Create a direct block large enough to hold an object of
 *              the requested size
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_dblock_new(H5HF_hdr_t *hdr, size_t request, H5HF_free_section_t **ret_sec_node)
{
    haddr_t dblock_addr;         /* Address of new direct block */
    size_t  min_dblock_size;     /* Min. size of direct block to allocate */
    herr_t  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(request > 0);

    /* Compute the min. size of the direct block needed to fulfill the request */
    if (request < hdr->man_dtable.cparam.start_block_size)
        min_dblock_size = hdr->man_dtable.cparam.start_block_size;
    else {
        min_dblock_size = ((size_t)1) << (1 + H5VM_log2_gen((uint64_t)request));
        assert(min_dblock_size <= hdr->man_dtable.cparam.max_direct_size);
    } /* end else */

    /* Adjust the size of block needed to fulfill request, with overhead */
    if (min_dblock_size < H5HF_MAN_ABS_DIRECT_OVERHEAD(hdr) + request)
        min_dblock_size *= 2;

    /* Check if this is the first block in the heap */
    if (!H5_addr_defined(hdr->man_dtable.table_addr) &&
        min_dblock_size == hdr->man_dtable.cparam.start_block_size) {
        /* Create new direct block at starting offset */
        if (H5HF__man_dblock_create(hdr, NULL, 0, &dblock_addr, ret_sec_node) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, FAIL, "can't allocate fractal heap direct block");

        /* Point root at new direct block */
        hdr->man_dtable.curr_root_rows = 0;
        hdr->man_dtable.table_addr     = dblock_addr;
        if (hdr->filter_len > 0) {
            hdr->pline_root_direct_size        = hdr->man_dtable.cparam.start_block_size;
            hdr->pline_root_direct_filter_mask = 0;
        } /* end if */

        /* Extend heap to cover new direct block */
        if (H5HF__hdr_adjust_heap(hdr, (hsize_t)hdr->man_dtable.cparam.start_block_size,
                                  (hssize_t)hdr->man_dtable.row_tot_dblock_free[0]) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTEXTEND, FAIL, "can't increase space to cover root direct block");
    } /* end if */
    /* Root entry already exists, allocate direct block from root indirect block */
    else {
        H5HF_indirect_t *iblock;     /* Pointer to indirect block to create */
        unsigned         next_row;   /* Iterator's next block row */
        unsigned         next_entry; /* Iterator's next block entry */
        size_t           next_size;  /* Size of next direct block to create */

        /* Update iterator to reflect any previous increments as well as allow for requested direct block size
         */
        if (H5HF__hdr_update_iter(hdr, min_dblock_size) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTUPDATE, FAIL, "unable to update block iterator");

        /* Retrieve information about current iterator position */
        if (H5HF__man_iter_curr(&hdr->next_block, &next_row, NULL, &next_entry, &iblock) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "unable to retrieve current block iterator location");
        assert(next_row < iblock->nrows);
        H5_CHECKED_ASSIGN(next_size, size_t, hdr->man_dtable.row_block_size[next_row], hsize_t);

        /* Check for skipping over blocks */
        if (min_dblock_size > next_size) {
            fprintf(stderr,
                    "%s: Skipping direct block sizes not supported, min_dblock_size = %zu, next_size = %zu\n",
                    __func__, min_dblock_size, next_size);
            HGOTO_ERROR(H5E_HEAP, H5E_UNSUPPORTED, FAIL, "skipping direct block sizes not supported yet");
        } /* end if */

        /* Advance "next block" iterator to next direct block entry */
        if (H5HF__hdr_inc_iter(hdr, (hsize_t)next_size, 1) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTINC, FAIL, "can't increment 'next block' iterator");

        /* Create new direct block at current location*/
        if (H5HF__man_dblock_create(hdr, iblock, next_entry, &dblock_addr, ret_sec_node) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, FAIL, "can't allocate fractal heap direct block");
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_dblock_new() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_dblock_protect
 *
 * Purpose:	Convenience wrapper around H5AC_protect on a direct block
 *              (Use H5AC_unprotect to unprotect it for now)
 *
 * Return:	Pointer to direct block on success, NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5HF_direct_t *
H5HF__man_dblock_protect(H5HF_hdr_t *hdr, haddr_t dblock_addr, size_t dblock_size,
                         H5HF_indirect_t *par_iblock, unsigned par_entry, unsigned flags)
{
    H5HF_direct_t         *dblock;           /* Direct block from cache */
    H5HF_dblock_cache_ud_t udata;            /* parent and other info for deserializing direct block */
    H5HF_direct_t         *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(H5_addr_defined(dblock_addr));
    assert(dblock_size > 0);

    /* only H5AC__READ_ONLY_FLAG may appear in flags */
    assert((flags & (unsigned)(~H5AC__READ_ONLY_FLAG)) == 0);

    /* Set up parent info */
    udata.par_info.hdr    = hdr;
    udata.par_info.iblock = par_iblock;
    udata.par_info.entry  = par_entry;

    /* set up the file pointer in the user data */
    udata.f = hdr->f;

    /* set up the direct block size */
    udata.dblock_size = dblock_size;

    /* compute the on disk image size -- observe that odi_size and
     * dblock_size will be identical if there is no filtering.
     */
    if (hdr->filter_len > 0) {
        if (par_iblock == NULL) {
            udata.odi_size    = hdr->pline_root_direct_size;
            udata.filter_mask = hdr->pline_root_direct_filter_mask;
        } /* end if */
        else {
            /* Sanity check */
            assert(H5_addr_eq(par_iblock->ents[par_entry].addr, dblock_addr));

            /* Set up parameters to read filtered direct block */
            udata.odi_size    = par_iblock->filt_ents[par_entry].size;
            udata.filter_mask = par_iblock->filt_ents[par_entry].filter_mask;
        } /* end else */
    }     /* end if */
    else {
        udata.odi_size    = dblock_size;
        udata.filter_mask = 0;
    } /* end else */

    /* Reset compression context info */
    udata.decompressed = false;
    udata.dblk         = NULL;

    /* Protect the direct block */
    if (NULL ==
        (dblock = (H5HF_direct_t *)H5AC_protect(hdr->f, H5AC_FHEAP_DBLOCK, dblock_addr, &udata, flags)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, NULL, "unable to protect fractal heap direct block");

    /* Set the return value */
    ret_value = dblock;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_dblock_protect() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_dblock_locate
 *
 * Purpose:	Locate a direct block in a managed heap
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_dblock_locate(H5HF_hdr_t *hdr, hsize_t obj_off, H5HF_indirect_t **ret_iblock, unsigned *ret_entry,
                        bool *ret_did_protect, unsigned flags)
{
    haddr_t          iblock_addr;         /* Indirect block's address */
    H5HF_indirect_t *iblock;              /* Pointer to indirect block */
    bool             did_protect;         /* Whether we protected the indirect block or not */
    unsigned         row, col;            /* Row & column for object's block */
    unsigned         entry;               /* Entry of block */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(hdr->man_dtable.curr_root_rows); /* Only works for heaps with indirect root block */
    assert(ret_iblock);
    assert(ret_did_protect);

    /* only H5AC__READ_ONLY_FLAG may appear in flags */
    assert((flags & (unsigned)(~H5AC__READ_ONLY_FLAG)) == 0);

    /* Look up row & column for object */
    if (H5HF__dtable_lookup(&hdr->man_dtable, obj_off, &row, &col) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTCOMPUTE, FAIL, "can't compute row & column of object");

    /* Set initial indirect block info */
    iblock_addr = hdr->man_dtable.table_addr;

    /* Lock root indirect block */
    if (NULL == (iblock = H5HF__man_iblock_protect(hdr, iblock_addr, hdr->man_dtable.curr_root_rows, NULL, 0,
                                                   false, flags, &did_protect)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to protect fractal heap indirect block");

    /* Check for indirect block row */
    while (row >= hdr->man_dtable.max_direct_rows) {
        H5HF_indirect_t *new_iblock;      /* Pointer to new indirect block */
        bool             new_did_protect; /* Whether we protected the indirect block or not */
        unsigned         nrows;           /* Number of rows in new indirect block */
        unsigned         cache_flags = H5AC__NO_FLAGS_SET; /* Flags for unprotecting parent indirect block */

        /* Compute # of rows in child indirect block */
        nrows = (H5VM_log2_gen(hdr->man_dtable.row_block_size[row]) - hdr->man_dtable.first_row_bits) + 1;
        assert(nrows < iblock->nrows); /* child must be smaller than parent */

        /* Compute indirect block's entry */
        entry = (row * hdr->man_dtable.cparam.width) + col;

        /* Locate child indirect block */
        iblock_addr = iblock->ents[entry].addr;

        /* Check if we need to (re-)create the child indirect block */
        if (!H5_addr_defined(iblock_addr)) {
            if (H5HF__man_iblock_create(hdr, iblock, entry, nrows, nrows, &iblock_addr) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, FAIL, "can't allocate fractal heap indirect block");

            /* Indicate that the parent indirect block was modified */
            cache_flags |= H5AC__DIRTIED_FLAG;
        } /* end if */

        /* Lock child indirect block */
        if (NULL == (new_iblock = H5HF__man_iblock_protect(hdr, iblock_addr, nrows, iblock, entry, false,
                                                           flags, &new_did_protect)))
            HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to protect fractal heap indirect block");

        /* Release the current indirect block */
        if (H5HF__man_iblock_unprotect(iblock, cache_flags, did_protect) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to release fractal heap indirect block");

        /* Switch variables to use new indirect block */
        iblock      = new_iblock;
        did_protect = new_did_protect;

        /* Look up row & column in new indirect block for object */
        if (H5HF__dtable_lookup(&hdr->man_dtable, (obj_off - iblock->block_off), &row, &col) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTCOMPUTE, FAIL, "can't compute row & column of object");
        assert(row < iblock->nrows); /* child must be smaller than parent */
    }                                /* end while */

    /* Set return parameters */
    if (ret_entry)
        *ret_entry = (row * hdr->man_dtable.cparam.width) + col;
    *ret_iblock      = iblock;
    *ret_did_protect = did_protect;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_dblock_locate() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_dblock_delete
 *
 * Purpose:	Delete a managed direct block
 *
 * Note:	This routine does _not_ modify any indirect block that points
 *              to this direct block, it is assumed that the whole heap is
 *              being deleted.  (H5HF__man_dblock_destroy modifies the indirect
 *              block)
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_dblock_delete(H5F_t *f, haddr_t dblock_addr, hsize_t dblock_size)
{
    unsigned dblock_status = 0;       /* Direct block's status in the metadata cache */
    herr_t   ret_value     = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);
    assert(H5_addr_defined(dblock_addr));
    assert(dblock_size > 0);

    /* Check the direct block's status in the metadata cache */
    if (H5AC_get_entry_status(f, dblock_addr, &dblock_status) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "unable to check metadata cache status for direct block");

    /* If the direct block is in the cache, expunge it now */
    if (dblock_status & H5AC_ES__IN_CACHE) {
        /* Sanity checks on direct block */
        assert(!(dblock_status & H5AC_ES__IS_PINNED));
        assert(!(dblock_status & H5AC_ES__IS_PROTECTED));

        /* Evict the direct block from the metadata cache */
        if (H5AC_expunge_entry(f, H5AC_FHEAP_DBLOCK, dblock_addr, H5AC__NO_FLAGS_SET) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTREMOVE, FAIL, "unable to remove direct block from cache");
    } /* end if */

    /* Check if the direct block is NOT currently allocated in temp. file space */
    /* (temp. file space does not need to be freed) */
    if (!H5F_IS_TMP_ADDR(f, dblock_addr))
        /* Release direct block's disk space */
        /* (XXX: Under the best of circumstances, this block's space in the file
         *          would be freed in the H5AC_expunge_entry() call above (and the
         *          H5AC__FREE_FILE_SPACE_FLAG used there), but since the direct
         *          block structure might have a different size on disk than in
         *          the heap's 'abstract' address space, we would need to set the
         *          "file_size" field for the direct block structure.  In order to
         *          do that, we'd have to protect/unprotect the direct block and
         *          that would add a bunch of unnecessary overhead to the process,
         *          so we just release the file space here, directly.  -QAK)
         */
        if (H5MF_xfree(f, H5FD_MEM_FHEAP_DBLOCK, dblock_addr, dblock_size) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "unable to free fractal heap direct block file space");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_dblock_delete() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_dblock_dest
 *
 * Purpose:	Destroys a fractal heap direct block in memory.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_dblock_dest(H5HF_direct_t *dblock)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(dblock);

    /* Decrement reference count on shared fractal heap info */
    assert(dblock->hdr != NULL);
    if (H5HF__hdr_decr(dblock->hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTDEC, FAIL, "can't decrement reference count on shared heap header");
    if (dblock->parent)
        if (H5HF__iblock_decr(dblock->parent) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTDEC, FAIL,
                        "can't decrement reference count on shared indirect block");

    /* Free block's buffer */
    dblock->blk = H5FL_BLK_FREE(direct_block, dblock->blk);

    /* Free fractal heap direct block info */
    dblock = H5FL_FREE(H5HF_direct_t, dblock);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_dblock_dest() */
