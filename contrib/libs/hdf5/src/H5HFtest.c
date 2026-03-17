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
 * Purpose:	Fractal heap testing functions.
 *
 */

/****************/
/* Module Setup */
/****************/

#include "H5HFmodule.h" /* This source code file is part of the H5HF module */
#define H5HF_TESTING    /*suppress warning about H5HF testing funcs*/

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5HFpkg.h"     /* Fractal heaps			*/
#include "H5MMprivate.h" /* Memory management			*/

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
 * Function:	H5HF_get_cparam_test
 *
 * Purpose:	Retrieve the parameters used to create the fractal heap
 *
 * Return:	Success:	non-negative
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF_get_cparam_test(const H5HF_t *fh, H5HF_create_t *cparam)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check arguments. */
    assert(fh);
    assert(cparam);

    /* Get fractal heap creation parameters */
    if (fh->hdr->id_len == (unsigned)(1 + fh->hdr->heap_off_size + fh->hdr->heap_len_size))
        cparam->id_len = 0;
    else if (fh->hdr->id_len == (unsigned)(1 + fh->hdr->sizeof_size + fh->hdr->sizeof_addr))
        cparam->id_len = 1;
    else
        H5_CHECKED_ASSIGN(cparam->id_len, uint16_t, fh->hdr->id_len, unsigned);
    cparam->max_man_size = fh->hdr->max_man_size;
    H5MM_memcpy(&(cparam->managed), &(fh->hdr->man_dtable.cparam), sizeof(H5HF_dtable_cparam_t));
    H5O_msg_copy(H5O_PLINE_ID, &(fh->hdr->pline), &(cparam->pline));

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF_get_cparam_test() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_cmp_cparam_test
 *
 * Purpose:	Compare the parameters used to create the fractal heap
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
int
H5HF_cmp_cparam_test(const H5HF_create_t *cparam1, const H5HF_create_t *cparam2)
{
    int ret_value = 0; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check arguments. */
    assert(cparam1);
    assert(cparam2);

    /* Compare doubling table parameters */
    if (cparam1->managed.width < cparam2->managed.width)
        HGOTO_DONE(-1);
    else if (cparam1->managed.width > cparam2->managed.width)
        HGOTO_DONE(1);
    if (cparam1->managed.start_block_size < cparam2->managed.start_block_size)
        HGOTO_DONE(-1);
    else if (cparam1->managed.start_block_size > cparam2->managed.start_block_size)
        HGOTO_DONE(1);
    if (cparam1->managed.max_direct_size < cparam2->managed.max_direct_size)
        HGOTO_DONE(-1);
    else if (cparam1->managed.max_direct_size > cparam2->managed.max_direct_size)
        HGOTO_DONE(1);
    if (cparam1->managed.max_index < cparam2->managed.max_index)
        HGOTO_DONE(-1);
    else if (cparam1->managed.max_index > cparam2->managed.max_index)
        HGOTO_DONE(1);
    if (cparam1->managed.start_root_rows < cparam2->managed.start_root_rows)
        HGOTO_DONE(-1);
    else if (cparam1->managed.start_root_rows > cparam2->managed.start_root_rows)
        HGOTO_DONE(1);

    /* Compare other general parameters for heap */
    if (cparam1->max_man_size < cparam2->max_man_size)
        HGOTO_DONE(-1);
    else if (cparam1->max_man_size > cparam2->max_man_size)
        HGOTO_DONE(1);
    if (cparam1->id_len < cparam2->id_len)
        HGOTO_DONE(-1);
    else if (cparam1->id_len > cparam2->id_len)
        HGOTO_DONE(1);

    /* Compare "important" parameters for any I/O pipeline filters */
    if (cparam1->pline.nused < cparam2->pline.nused)
        HGOTO_DONE(-1);
    else if (cparam1->pline.nused > cparam2->pline.nused)
        HGOTO_DONE(1);
    else {
        size_t u, v; /* Local index variables */

        /* Compare each filter */
        for (u = 0; u < cparam1->pline.nused; u++) {
            /* Check filter ID */
            if (cparam1->pline.filter[u].id < cparam2->pline.filter[u].id)
                HGOTO_DONE(-1);
            else if (cparam1->pline.filter[u].id > cparam2->pline.filter[u].id)
                HGOTO_DONE(1);

            /* Check filter flags */
            if (cparam1->pline.filter[u].flags < cparam2->pline.filter[u].flags)
                HGOTO_DONE(-1);
            else if (cparam1->pline.filter[u].flags > cparam2->pline.filter[u].flags)
                HGOTO_DONE(1);

/* Don't worry about comparing the filter names right now... */
/* (they are expanded during the encode/decode process, but aren't copied
 *      during the H5Z_append operation, generating false positive failures -QAK)
 */
#if 0
            /* Check filter name */
            if(!cparam1->pline.filter[u].name && cparam2->pline.filter[u].name)
                HGOTO_DONE(-1);
            else if(cparam1->pline.filter[u].name && !cparam2->pline.filter[u].name)
                HGOTO_DONE(1);
            else if(cparam1->pline.filter[u].name && cparam2->pline.filter[u].name) {
                if((ret_value = strcmp(cparam1->pline.filter[u].name, cparam2->pline.filter[u].name)))
                    HGOTO_DONE(ret_value);
            } /* end if */
#endif

            /* Check # of filter parameters */
            if (cparam1->pline.filter[u].cd_nelmts < cparam2->pline.filter[u].cd_nelmts)
                HGOTO_DONE(-1);
            else if (cparam1->pline.filter[u].cd_nelmts > cparam2->pline.filter[u].cd_nelmts)
                HGOTO_DONE(1);

            /* Check filter parameters */
            for (v = 0; v < cparam1->pline.filter[u].cd_nelmts; v++) {
                if (cparam1->pline.filter[u].cd_values[v] < cparam2->pline.filter[u].cd_values[v])
                    HGOTO_DONE(-1);
                else if (cparam1->pline.filter[u].cd_values[v] > cparam2->pline.filter[u].cd_values[v])
                    HGOTO_DONE(1);
            } /* end for */

        } /* end for */
    }     /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF_cmp_cparam_test() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_get_max_root_rows
 *
 * Purpose:	Retrieve the max. # of rows in the root indirect block
 *
 * Return:	Success:	Max. # of rows in root indirect block
 *
 *		Failure:	0
 *
 *-------------------------------------------------------------------------
 */
unsigned
H5HF_get_max_root_rows(const H5HF_t *fh)
{
    unsigned ret_value = 0; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check arguments. */
    assert(fh);

    /* Return max. # of rows in root indirect block */
    ret_value = fh->hdr->man_dtable.max_root_rows;

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF_get_max_root_rows() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_get_dtable_width_test
 *
 * Purpose:	Retrieve the width of the doubling table for a heap
 *
 * Return:	Success:	Width of the doubling table
 *
 *		Failure:	0
 *
 *-------------------------------------------------------------------------
 */
unsigned
H5HF_get_dtable_width_test(const H5HF_t *fh)
{
    unsigned ret_value = 0; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check arguments. */
    assert(fh);

    /* Return width of doubling table */
    ret_value = fh->hdr->man_dtable.cparam.width;

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF_get_dtable_width_test() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_get_dtable_max_drows_test
 *
 * Purpose:	Retrieve the max. # of direct block rows in any indirect block
 *
 * Return:	Success:	Max. # of direct block rows
 *
 *		Failure:	0
 *
 *-------------------------------------------------------------------------
 */
unsigned
H5HF_get_dtable_max_drows_test(const H5HF_t *fh)
{
    unsigned ret_value = 0; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check arguments. */
    assert(fh);

    /* Return max. # of direct blocks in any indirect block */
    ret_value = fh->hdr->man_dtable.max_direct_rows;

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF_get_dtable_max_drows_test() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_get_iblock_max_drows_test
 *
 * Purpose:	Retrieve the max. # of direct block rows in an indirect block
 *
 * Note:        POS is indexed from 1 and is only really working for the
 *              2nd-level indirect blocks (i.e. indirect blocks with
 *              only direct block children)
 *
 * Return:	Success:	Max. # of direct block rows
 *
 *		Failure:	0
 *
 *-------------------------------------------------------------------------
 */
unsigned
H5HF_get_iblock_max_drows_test(const H5HF_t *fh, unsigned pos)
{
    unsigned ret_value = 0; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check arguments. */
    assert(fh);
    assert(pos);

    /* Return max. # of direct blocks in this indirect block row */
    ret_value = pos + (fh->hdr->man_dtable.max_direct_bits - fh->hdr->man_dtable.first_row_bits) + 1;

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF_get_iblock_max_drows_test() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_get_dblock_size_test
 *
 * Purpose:	Retrieve the size of a direct block for a given row
 *
 * Return:	Success:	Size of direct block
 *
 *		Failure:	0
 *
 *-------------------------------------------------------------------------
 */
hsize_t
H5HF_get_dblock_size_test(const H5HF_t *fh, unsigned row)
{
    hsize_t ret_value = 0; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check arguments. */
    assert(fh);

    /* Return direct block free space */
    ret_value = fh->hdr->man_dtable.row_block_size[row];

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF_get_dblock_size_test() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_get_dblock_free_test
 *
 * Purpose:	Retrieve the size of direct block free space for a given
 *              direct or indirect block size
 *
 * Return:	Success:	Size of direct block free space
 *
 *		Failure:	0
 *
 *-------------------------------------------------------------------------
 */
hsize_t
H5HF_get_dblock_free_test(const H5HF_t *fh, unsigned row)
{
    hsize_t ret_value = 0; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check arguments. */
    assert(fh);

    /* Return direct block free space */
    ret_value = fh->hdr->man_dtable.row_tot_dblock_free[row];

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5HF_get_dblock_free_test() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_get_id_off_test
 *
 * Purpose:	Retrieve the offset for a [managed] heap ID
 *
 * Return:	Success:	non-negative
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF_get_id_off_test(const H5HF_t *fh, const void *_id, hsize_t *obj_off)
{
    const uint8_t *id = (const uint8_t *)_id; /* Object ID */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check arguments. */
    assert(fh);
    assert(fh->hdr);
    assert(id);
    assert(obj_off);

    /* Get the offset for a 'normal' heap ID */
    id++;
    UINT64DECODE_VAR(id, *obj_off, fh->hdr->heap_off_size);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF_get_id_off_test() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_get_id_type_test
 *
 * Purpose:	Retrieve the type of a heap ID
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF_get_id_type_test(const void *_id, unsigned char *obj_type)
{
    const uint8_t *id = (const uint8_t *)_id; /* Object ID */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check arguments. */
    assert(id);
    assert(obj_type);

    /* Get the type for a heap ID */
    *obj_type = (uint8_t)(*id & H5HF_ID_TYPE_MASK);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF_get_id_type_test() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_get_tiny_info_test
 *
 * Purpose:	Retrieve information about tiny object's ID length
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF_get_tiny_info_test(const H5HF_t *fh, size_t *max_len, bool *len_extended)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check arguments. */
    assert(fh);
    assert(fh->hdr);
    assert(max_len);
    assert(len_extended);

    /* Retrieve information about tiny object's ID encoding in a heap */
    *max_len      = fh->hdr->tiny_max_len;
    *len_extended = fh->hdr->tiny_len_extended;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF_get_tiny_info_test() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_get_huge_info_test
 *
 * Purpose:	Retrieve information about huge object's ID length
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF_get_huge_info_test(const H5HF_t *fh, hsize_t *next_id, bool *ids_direct)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check arguments. */
    assert(fh);
    assert(fh->hdr);
    assert(ids_direct);

    /* Retrieve information about tiny object's ID encoding in a heap */
    if (next_id)
        *next_id = fh->hdr->huge_next_id;
    *ids_direct = fh->hdr->huge_ids_direct;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5HF_get_huge_info_test() */
