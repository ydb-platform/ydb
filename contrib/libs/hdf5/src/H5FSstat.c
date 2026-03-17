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
 * Purpose:	Free-space metadata statistics functions.
 *
 */

/****************/
/* Module Setup */
/****************/

#include "H5FSmodule.h" /* This source code file is part of the H5FS module */

/***********/
/* Headers */
/***********/
#include "H5private.h"  /* Generic Functions                        */
#include "H5Eprivate.h" /* Error handling                           */
#include "H5FSpkg.h"    /* Free-space manager                       */

/****************/
/* Local Macros */
/****************/

/********************/
/* Package Typedefs */
/********************/

/******************/
/* Local Typedefs */
/******************/

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
 * Function:    H5FS_stat_info
 *
 * Purpose:     Retrieve metadata statistics for the free-space manager
 *
 * Return:      SUCCEED (Can't fail)
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FS_stat_info(const H5F_t *f, const H5FS_t *frsp, H5FS_stat_t *stats)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check arguments. */
    assert(frsp);
    assert(stats);

    /* Report statistics for free space */
    stats->tot_space         = frsp->tot_space;
    stats->tot_sect_count    = frsp->tot_sect_count;
    stats->serial_sect_count = frsp->serial_sect_count;
    stats->ghost_sect_count  = frsp->ghost_sect_count;
    stats->addr              = frsp->addr;
    stats->hdr_size          = (hsize_t)H5FS_HEADER_SIZE(f);
    stats->sect_addr         = frsp->sect_addr;
    stats->alloc_sect_size   = frsp->alloc_sect_size;
    stats->sect_size         = frsp->sect_size;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5FS_stat_info() */
