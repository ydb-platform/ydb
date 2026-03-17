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
 * Created:		H5HFdtable.c
 *
 * Purpose:		"Doubling table" routines for fractal heaps.
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
#include "H5MMprivate.h" /* Memory management			*/
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

/*-------------------------------------------------------------------------
 * Function:	H5HF__dtable_init
 *
 * Purpose:	Initialize values for doubling table
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__dtable_init(H5HF_dtable_t *dtable)
{
    hsize_t tmp_block_size;      /* Temporary block size */
    hsize_t acc_block_off;       /* Accumulated block offset */
    size_t  u;                   /* Local index variable */
    herr_t  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(dtable);

    /* Compute/cache some values */
    dtable->start_bits           = H5VM_log2_of2((uint32_t)dtable->cparam.start_block_size);
    dtable->first_row_bits       = dtable->start_bits + H5VM_log2_of2(dtable->cparam.width);
    dtable->max_root_rows        = (dtable->cparam.max_index - dtable->first_row_bits) + 1;
    dtable->max_direct_bits      = H5VM_log2_of2((uint32_t)dtable->cparam.max_direct_size);
    dtable->max_direct_rows      = (dtable->max_direct_bits - dtable->start_bits) + 2;
    dtable->num_id_first_row     = dtable->cparam.start_block_size * dtable->cparam.width;
    dtable->max_dir_blk_off_size = H5HF_SIZEOF_OFFSET_LEN(dtable->cparam.max_direct_size);

    /* Build table of block sizes for each row */
    if (NULL == (dtable->row_block_size = (hsize_t *)H5MM_malloc(dtable->max_root_rows * sizeof(hsize_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "can't create doubling table block size table");
    if (NULL == (dtable->row_block_off = (hsize_t *)H5MM_malloc(dtable->max_root_rows * sizeof(hsize_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "can't create doubling table block offset table");
    if (NULL ==
        (dtable->row_tot_dblock_free = (hsize_t *)H5MM_malloc(dtable->max_root_rows * sizeof(hsize_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                    "can't create doubling table total direct block free space table");
    if (NULL == (dtable->row_max_dblock_free = (size_t *)H5MM_malloc(dtable->max_root_rows * sizeof(size_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                    "can't create doubling table max. direct block free space table");
    tmp_block_size            = dtable->cparam.start_block_size;
    acc_block_off             = dtable->cparam.start_block_size * dtable->cparam.width;
    dtable->row_block_size[0] = dtable->cparam.start_block_size;
    dtable->row_block_off[0]  = 0;
    for (u = 1; u < dtable->max_root_rows; u++) {
        dtable->row_block_size[u] = tmp_block_size;
        dtable->row_block_off[u]  = acc_block_off;
        tmp_block_size *= 2;
        acc_block_off *= 2;
    } /* end for */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__dtable_init() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__dtable_lookup
 *
 * Purpose:	Compute the row & col of an offset in a doubling-table
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__dtable_lookup(const H5HF_dtable_t *dtable, hsize_t off, unsigned *row, unsigned *col)
{
    FUNC_ENTER_PACKAGE_NOERR

    /*
     * Check arguments.
     */
    assert(dtable);
    assert(row);
    assert(col);

    /* Check for offset in first row */
    if (off < dtable->num_id_first_row) {
        *row = 0;
        H5_CHECKED_ASSIGN(*col, unsigned, (off / dtable->cparam.start_block_size), hsize_t);
    } /* end if */
    else {
        unsigned high_bit = H5VM_log2_gen(off);       /* Determine the high bit in the offset */
        hsize_t  off_mask = ((hsize_t)1) << high_bit; /* Compute mask for determining column */

        *row = (high_bit - dtable->first_row_bits) + 1;
        H5_CHECKED_ASSIGN(*col, unsigned, ((off - off_mask) / dtable->row_block_size[*row]), hsize_t);
    } /* end else */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HF__dtable_lookup() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__dtable_dest
 *
 * Purpose:	Release information for doubling table
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__dtable_dest(H5HF_dtable_t *dtable)
{
    FUNC_ENTER_PACKAGE_NOERR

    /*
     * Check arguments.
     */
    assert(dtable);

    /* Free the block size lookup table for the doubling table */
    H5MM_xfree(dtable->row_block_size);

    /* Free the block offset lookup table for the doubling table */
    H5MM_xfree(dtable->row_block_off);

    /* Free the total direct block free space lookup table for the doubling table */
    H5MM_xfree(dtable->row_tot_dblock_free);

    /* Free the max. direct block free space lookup table for the doubling table */
    H5MM_xfree(dtable->row_max_dblock_free);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HF__dtable_dest() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__dtable_size_to_row
 *
 * Purpose:	Compute row that can hold block of a certain size
 *
 * Return:	Non-negative on success (can't fail)
 *
 *-------------------------------------------------------------------------
 */
unsigned
H5HF__dtable_size_to_row(const H5HF_dtable_t *dtable, size_t block_size)
{
    unsigned row = 0; /* Row where block will fit */

    FUNC_ENTER_PACKAGE_NOERR

    /*
     * Check arguments.
     */
    assert(dtable);

    if (block_size == dtable->cparam.start_block_size)
        row = 0;
    else
        row =
            (H5VM_log2_of2((uint32_t)block_size) - H5VM_log2_of2((uint32_t)dtable->cparam.start_block_size)) +
            1;

    FUNC_LEAVE_NOAPI(row)
} /* end H5HF__dtable_size_to_row() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__dtable_size_to_rows
 *
 * Purpose:	Compute # of rows of indirect block of a given size
 *
 * Return:	Non-negative on success (can't fail)
 *
 *-------------------------------------------------------------------------
 */
unsigned
H5HF__dtable_size_to_rows(const H5HF_dtable_t *dtable, hsize_t size)
{
    unsigned rows = 0; /* # of rows required for indirect block */

    FUNC_ENTER_PACKAGE_NOERR

    /*
     * Check arguments.
     */
    assert(dtable);

    rows = (H5VM_log2_gen(size) - dtable->first_row_bits) + 1;

    FUNC_LEAVE_NOAPI(rows)
} /* end H5HF__dtable_size_to_rows() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__dtable_span_size
 *
 * Purpose:	Compute the size covered by a span of entries
 *
 * Return:	Non-zero span size on success/zero on failure
 *
 *-------------------------------------------------------------------------
 */
hsize_t
H5HF__dtable_span_size(const H5HF_dtable_t *dtable, unsigned start_row, unsigned start_col,
                       unsigned num_entries)
{
    unsigned start_entry;       /* Entry for first block covered */
    unsigned end_row;           /* Row for last block covered */
    unsigned end_col;           /* Column for last block covered */
    unsigned end_entry;         /* Entry for last block covered */
    hsize_t  acc_span_size = 0; /* Accumulated span size */

    FUNC_ENTER_PACKAGE_NOERR

    /*
     * Check arguments.
     */
    assert(dtable);
    assert(num_entries > 0);

    /* Compute starting entry */
    start_entry = (start_row * dtable->cparam.width) + start_col;

    /* Compute ending entry, column & row */
    end_entry = (start_entry + num_entries) - 1;
    end_row   = end_entry / dtable->cparam.width;
    end_col   = end_entry % dtable->cparam.width;

    /* Initialize accumulated span size */
    acc_span_size = 0;

    /* Compute span size covered */

    /* Check for multi-row span */
    if (start_row != end_row) {
        /* Accommodate partial starting row */
        if (start_col > 0) {
            acc_span_size = dtable->row_block_size[start_row] * (dtable->cparam.width - start_col);
            start_row++;
        } /* end if */

        /* Accumulate full rows */
        while (start_row < end_row) {
            acc_span_size += dtable->row_block_size[start_row] * dtable->cparam.width;
            start_row++;
        } /* end while */

        /* Accommodate partial ending row */
        acc_span_size += dtable->row_block_size[start_row] * (end_col + 1);
    } /* end if */
    else {
        /* Span is in same row */
        acc_span_size = dtable->row_block_size[start_row] * ((end_col - start_col) + 1);
    } /* end else */

    FUNC_LEAVE_NOAPI(acc_span_size)
} /* end H5HF__dtable_span_size() */
