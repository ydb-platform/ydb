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
 * Purpose:     v2 B-tree metadata statistics functions.
 *
 */

/****************/
/* Module Setup */
/****************/

#include "H5B2module.h" /* This source code file is part of the H5B2 module */

/***********/
/* Headers */
/***********/
#include "H5private.h"  /* Generic Functions                        */
#include "H5B2pkg.h"    /* v2 B-trees                               */
#include "H5Eprivate.h" /* Error handling                           */

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
 * Function:    H5B2_stat_info
 *
 * Purpose:     Retrieve metadata statistics for a v2 B-tree
 *
 * Return:      SUCCEED (Can't fail)
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2_stat_info(H5B2_t *bt2, H5B2_stat_t *info)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check arguments. */
    assert(info);

    /* Get information about the B-tree */
    info->depth    = bt2->hdr->depth;
    info->nrecords = bt2->hdr->root.all_nrec;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5B2_stat_info() */

/*-------------------------------------------------------------------------
 * Function:    H5B2_size
 *
 * Purpose:     Iterate over all the records in the B-tree, collecting
 *              storage info.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2_size(H5B2_t *bt2, hsize_t *btree_size)
{
    H5B2_hdr_t *hdr;                 /* Pointer to the B-tree header */
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments. */
    assert(bt2);
    assert(btree_size);

    /* Set the shared v2 B-tree header's file context for this operation */
    bt2->hdr->f = bt2->f;

    /* Get the v2 B-tree header */
    hdr = bt2->hdr;

    /* Add size of header to B-tree metadata total */
    *btree_size += hdr->hdr_size;

    /* Iterate through records */
    if (hdr->root.node_nrec > 0) {
        /* Check for root node being a leaf */
        if (hdr->depth == 0)
            *btree_size += hdr->node_size;
        else
            /* Iterate through nodes */
            if (H5B2__node_size(hdr, hdr->depth, &hdr->root, hdr, btree_size) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTLIST, FAIL, "node iteration failed");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5B2_size() */
