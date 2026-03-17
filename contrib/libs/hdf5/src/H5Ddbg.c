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

/****************/
/* Module Setup */
/****************/

#include "H5Dmodule.h" /* This source code file is part of the H5D module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5CXprivate.h" /* API Contexts                             */
#include "H5Dpkg.h"      /* Datasets                                 */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5VLprivate.h" /* Virtual Object Layer                     */

/****************/
/* Local Macros */
/****************/

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
 * Function:    H5Ddebug
 *
 * Purpose:     Prints various information about a dataset.  This function is
 *              not to be documented in the API at this time.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Ddebug(hid_t dset_id)
{
    H5D_t *dset;                /* Dataset to debug */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", dset_id);

    /* Check args */
    if (NULL == (dset = (H5D_t *)H5VL_object_verify(dset_id, H5I_DATASET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataset");

    /* Print B-tree information */
    if (H5D_CHUNKED == dset->shared->layout.type)
        (void)H5D__chunk_dump_index(dset, stdout);
    else if (H5D_CONTIGUOUS == dset->shared->layout.type)
        fprintf(stdout, "    %-10s %" PRIuHADDR "\n", "Address:", dset->shared->layout.storage.u.contig.addr);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Ddebug() */
