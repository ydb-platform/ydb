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
 * Created:         H5FAstat.c
 *
 * Purpose:         Fixed array metadata statistics functions.
 *
 *-------------------------------------------------------------------------
 */

/**********************/
/* Module Declaration */
/**********************/

#include "H5FAmodule.h" /* This source code file is part of the H5FA module */

/***********************/
/* Other Packages Used */
/***********************/

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                    */
#include "H5Eprivate.h"  /* Error handling                       */
#include "H5FApkg.h"     /* Fixed Arrays                         */
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
 * Function:    H5FA_get_stats
 *
 * Purpose:     Query the metadata stats of an array
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FA_get_stats(const H5FA_t *fa, H5FA_stat_t *stats)
{
    FUNC_ENTER_NOAPI_NOERR

#ifdef H5FA_DEBUG
    fprintf(stderr, "%s: Called\n", __func__);
#endif /* H5FA_DEBUG */

    /* Check arguments */
    assert(fa);
    assert(stats);

    /* Copy fixed array statistics */
    H5MM_memcpy(stats, &fa->hdr->stats, sizeof(fa->hdr->stats));

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FA_get_stats() */
