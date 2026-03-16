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
 * Created:		H5HFiter.c
 *
 * Purpose:		Block iteration routines for fractal heaps.
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
#include "H5HFpkg.h"     /* Fractal heaps			*/
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

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Declare a free list to manage the H5HF_block_loc_t struct */
H5FL_DEFINE_STATIC(H5HF_block_loc_t);

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_iter_init
 *
 * Purpose:	Initialize a block iterator for walking over all the blocks
 *              in a fractal heap.  (initialization finishes when iterator is
 *              actually used)
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_iter_init(H5HF_block_iter_t *biter)
{
    FUNC_ENTER_PACKAGE_NOERR

    /*
     * Check arguments.
     */
    assert(biter);

    /* Reset block iterator information */
    memset(biter, 0, sizeof(H5HF_block_iter_t));

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HF__man_iter_init() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_iter_start_offset
 *
 * Purpose:	Initialize a block iterator to a particular location, given
 *              an offset in the heap
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_iter_start_offset(H5HF_hdr_t *hdr, H5HF_block_iter_t *biter, hsize_t offset)
{
    H5HF_indirect_t *iblock;               /* Indirect block for location context */
    haddr_t          iblock_addr;          /* Address of indirect block */
    unsigned         iblock_nrows;         /* # of rows in indirect block */
    H5HF_indirect_t *iblock_parent;        /* Parent indirect block of location context */
    unsigned         iblock_par_entry;     /* Entry within parent indirect block */
    hsize_t          curr_offset;          /* Current offset, as adjusted */
    unsigned         row;                  /* Current row we are on */
    unsigned         col;                  /* Column in row */
    bool             root_block = true;    /* Flag to indicate the current block is the root indirect block */
    herr_t           ret_value  = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(biter);
    assert(!biter->ready);

    /* Check for empty heap */
    assert(offset >= hdr->man_dtable.cparam.start_block_size);

    /* Allocate level structure */
    if (NULL == (biter->curr = H5FL_MALLOC(H5HF_block_loc_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                    "memory allocation failed for direct block free list section");

    /*
    1:  <Scan down block offsets for dtable rows until find a row >= offset>
        <Set current location's row, col, entry & size>
        <If row < max_direct_rows>
            <Done>
        <Else - row > max_direct_rows>
            <Create new block level>
            <Link new block level into iterator>
            <Adjust offset for block offset for row>
            <Make new block level the current context>
            <Goto 1>

    */
    do {
        bool did_protect; /* Whether we protected the indirect block or not */

        /* Walk down the rows in the doubling table until we've found the correct row for the next block */
        for (row = 0; row < hdr->man_dtable.max_root_rows; row++)
            if ((offset >= hdr->man_dtable.row_block_off[row]) &&
                (offset < hdr->man_dtable.row_block_off[row] +
                              (hdr->man_dtable.cparam.width * hdr->man_dtable.row_block_size[row])))
                break;

        /* Adjust offset by row offset */
        curr_offset = offset - hdr->man_dtable.row_block_off[row];

        /* Compute column */
        H5_CHECK_OVERFLOW((curr_offset / hdr->man_dtable.row_block_size[row]), hsize_t, unsigned);
        col = (unsigned)(curr_offset / hdr->man_dtable.row_block_size[row]);

        /* Set the current level's context */
        biter->curr->row   = row;
        biter->curr->col   = col;
        biter->curr->entry = (row * hdr->man_dtable.cparam.width) + col;

        /* Get the context indirect block's information */
        if (root_block) {
            iblock_addr      = hdr->man_dtable.table_addr;
            iblock_nrows     = hdr->man_dtable.curr_root_rows;
            iblock_parent    = NULL;
            iblock_par_entry = 0;

            /* The root block can't go up further... */
            biter->curr->up = NULL;

            /* Next time through the loop will not be with the root indirect block */
            root_block = false;
        } /* end if */
        else {
            hsize_t child_size; /* Size of new indirect block to create */

            /* Retrieve the parent information from the previous context location */
            iblock_parent    = biter->curr->up->context;
            iblock_par_entry = biter->curr->up->entry;

            /* Look up the address of context indirect block */
            iblock_addr = iblock_parent->ents[iblock_par_entry].addr;

            /* Compute # of rows in context indirect block */
            child_size   = hdr->man_dtable.row_block_size[biter->curr->up->row];
            iblock_nrows = (H5VM_log2_gen(child_size) - hdr->man_dtable.first_row_bits) + 1;
        } /* end else */

        /* Load indirect block for this context location */
        if (NULL ==
            (iblock = H5HF__man_iblock_protect(hdr, iblock_addr, iblock_nrows, iblock_parent,
                                               iblock_par_entry, false, H5AC__NO_FLAGS_SET, &did_protect)))
            HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to protect fractal heap indirect block");

        /* Make indirect block the context for the current location */
        biter->curr->context = iblock;

        /* Hold the indirect block with the location */
        if (H5HF__iblock_incr(biter->curr->context) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTINC, FAIL,
                        "can't increment reference count on shared indirect block");

        /* Release the current indirect block */
        if (H5HF__man_iblock_unprotect(iblock, H5AC__NO_FLAGS_SET, did_protect) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to release fractal heap indirect block");
        iblock = NULL;

        /* See if the location falls in a direct block row */
        /* Or, if the offset has just filled up a direct or indirect block */
        if (curr_offset == (col * hdr->man_dtable.row_block_size[row]) ||
            row < hdr->man_dtable.max_direct_rows) {
            assert(curr_offset - (col * hdr->man_dtable.row_block_size[row]) == 0);
            break; /* Done now */
        }          /* end if */
        /* Indirect block row */
        else {
            H5HF_block_loc_t *new_loc; /* Pointer to new block location */

            /* Allocate level structure */
            if (NULL == (new_loc = H5FL_MALLOC(H5HF_block_loc_t)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                            "memory allocation failed for direct block free list section");

            /* Link new level into iterator */
            new_loc->up = biter->curr;

            /* Adjust offset for new level */
            offset = curr_offset - (col * hdr->man_dtable.row_block_size[row]);

            /* Make new block the current context */
            biter->curr = new_loc;
        }        /* end else */
    } while (1); /* Breaks out in middle */

    /* Set flag to indicate block iterator finished initializing */
    biter->ready = true;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_iter_start_offset() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_iter_set_entry
 *
 * Purpose:	Set the current entry for the iterator
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_iter_set_entry(const H5HF_hdr_t *hdr, H5HF_block_iter_t *biter, unsigned entry)
{
    FUNC_ENTER_PACKAGE_NOERR

    /*
     * Check arguments.
     */
    assert(biter);

    /* Set location context */
    biter->curr->entry = entry;
    biter->curr->row   = entry / hdr->man_dtable.cparam.width;
    biter->curr->col   = entry % hdr->man_dtable.cparam.width;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HF__man_iter_set_entry() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_iter_start_entry
 *
 * Purpose:	Initialize a block iterator to a particular location within
 *              an indirect block
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_iter_start_entry(H5HF_hdr_t *hdr, H5HF_block_iter_t *biter, H5HF_indirect_t *iblock,
                           unsigned start_entry)
{
    H5HF_block_loc_t *new_loc   = NULL;    /* Pointer to new block location */
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(biter);
    assert(!biter->ready);
    assert(iblock);

    /* Create new location for iterator */
    if (NULL == (new_loc = H5FL_MALLOC(H5HF_block_loc_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                    "memory allocation failed for direct block free list section");

    /* Set up location context */
    new_loc->entry   = start_entry;
    new_loc->row     = start_entry / hdr->man_dtable.cparam.width;
    new_loc->col     = start_entry % hdr->man_dtable.cparam.width;
    new_loc->context = iblock;
    new_loc->up      = NULL;

    /* Increment reference count on indirect block */
    if (H5HF__iblock_incr(new_loc->context) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINC, FAIL, "can't increment reference count on shared indirect block");

    /* Make new location the current location */
    biter->curr = new_loc;

    /* Set flag to indicate block iterator finished initializing */
    biter->ready = true;

done:
    if (ret_value < 0 && new_loc)
        new_loc = H5FL_FREE(H5HF_block_loc_t, new_loc);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_iter_start_entry() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_iter_reset
 *
 * Purpose:	Reset a block iterator to it's initial state, freeing any
 *              location context it currently has
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_iter_reset(H5HF_block_iter_t *biter)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(biter);

    /* Free any location contexts that exist */
    if (biter->curr) {
        H5HF_block_loc_t *curr_loc; /* Pointer to current block location */
        H5HF_block_loc_t *next_loc; /* Pointer to next block location */

        /* Free location contexts */
        curr_loc = biter->curr;
        while (curr_loc) {
            /* Get pointer to next node */
            next_loc = curr_loc->up;

            /* If this node is holding an indirect block, release the block */
            if (curr_loc->context)
                if (H5HF__iblock_decr(curr_loc->context) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTDEC, FAIL,
                                "can't decrement reference count on shared indirect block");

            /* Free the current location context */
            curr_loc = H5FL_FREE(H5HF_block_loc_t, curr_loc);

            /* Advance to next location */
            curr_loc = next_loc;
        } /* end while */

        /* Reset top location context */
        biter->curr = NULL;
    } /* end if */

    /* Reset block iterator flags */
    biter->ready = false;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_iter_reset() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_iter_next
 *
 * Purpose:	Advance to the next block within the current block of the heap
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_iter_next(H5HF_hdr_t *hdr, H5HF_block_iter_t *biter, unsigned nentries)
{
    FUNC_ENTER_PACKAGE_NOERR

    /*
     * Check arguments.
     */
    assert(biter);
    assert(biter->curr);
    assert(biter->curr->context);
    assert(biter->curr->row < biter->curr->context->nrows);

    /* Advance entry in current block */
    biter->curr->entry += nentries;
    biter->curr->row = biter->curr->entry / hdr->man_dtable.cparam.width;
    biter->curr->col = biter->curr->entry % hdr->man_dtable.cparam.width;
    /*    assert(biter->curr->row <= biter->curr->context->nrows); */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HF__man_iter_next() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_iter_up
 *
 * Purpose:	Move iterator up one level
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_iter_up(H5HF_block_iter_t *biter)
{
    H5HF_block_loc_t *up_loc;              /* Pointer to 'up' block location */
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(biter);
    assert(biter->ready);
    assert(biter->curr);
    assert(biter->curr->up);
    assert(biter->curr->context);

    /* Release hold on current location's indirect block */
    if (H5HF__iblock_decr(biter->curr->context) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTDEC, FAIL, "can't decrement reference count on shared indirect block");

    /* Get pointer to location context above this one */
    up_loc = biter->curr->up;

    /* Release this location */
    biter->curr = H5FL_FREE(H5HF_block_loc_t, biter->curr);

    /* Point location to next location up */
    biter->curr = up_loc;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_iter_up() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_iter_down
 *
 * Purpose:	Move iterator down one level
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_iter_down(H5HF_block_iter_t *biter, H5HF_indirect_t *iblock)
{
    H5HF_block_loc_t *down_loc  = NULL;    /* Pointer to new 'down' block location */
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(biter);
    assert(biter->ready);
    assert(biter->curr);
    assert(biter->curr->context);

    /* Create new location to move down to */
    if (NULL == (down_loc = H5FL_MALLOC(H5HF_block_loc_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                    "memory allocation failed for direct block free list section");

    /* Initialize down location */
    down_loc->row     = 0;
    down_loc->col     = 0;
    down_loc->entry   = 0;
    down_loc->context = iblock;
    down_loc->up      = biter->curr;

    /* Increment reference count on indirect block */
    if (H5HF__iblock_incr(down_loc->context) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINC, FAIL, "can't increment reference count on shared indirect block");

    /* Make down location the current location */
    biter->curr = down_loc;

done:
    if (ret_value < 0 && down_loc)
        down_loc = H5FL_FREE(H5HF_block_loc_t, down_loc);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_iter_down() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_iter_curr
 *
 * Purpose:	Retrieve information about the current block iterator location
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_iter_curr(H5HF_block_iter_t *biter, unsigned *row, unsigned *col, unsigned *entry,
                    H5HF_indirect_t **block)
{
    FUNC_ENTER_PACKAGE_NOERR

    /*
     * Check arguments.
     */
    assert(biter);
    assert(biter->ready);

    /* Retrieve the information asked for */
    if (row)
        *row = biter->curr->row;
    if (col)
        *col = biter->curr->col;
    if (entry)
        *entry = biter->curr->entry;
    if (block)
        *block = biter->curr->context;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HF__man_iter_curr() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_iter_ready
 *
 * Purpose:	Query if iterator is ready to use
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
bool
H5HF__man_iter_ready(H5HF_block_iter_t *biter)
{
    FUNC_ENTER_PACKAGE_NOERR

    /*
     * Check arguments.
     */
    assert(biter);

    FUNC_LEAVE_NOAPI(biter->ready)
} /* end H5HF__man_iter_ready() */
