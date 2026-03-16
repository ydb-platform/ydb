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

#include "H5SMmodule.h" /* This source code file is part of the H5SM module */
#define H5SM_TESTING    /*suppress warning about H5SM testing funcs*/

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5ACprivate.h" /* Metadata cache			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Fprivate.h"  /* File access                          */
#include "H5FLprivate.h" /* Free Lists                           */
#include "H5SMpkg.h"     /* Shared object header messages        */

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
 * Function:    H5SM__get_mesg_count_test
 *
 * Purpose:     Retrieve the number of messages tracked of a certain type
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5SM__get_mesg_count_test(H5F_t *f, unsigned type_id, size_t *mesg_count)
{
    H5SM_master_table_t *table     = NULL;    /* SOHM master table */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_TAG(H5AC__SOHM_TAG)

    /* Sanity check */
    assert(f);
    assert(mesg_count);

    /* Check for shared messages being enabled */
    if (H5_addr_defined(H5F_SOHM_ADDR(f))) {
        H5SM_index_header_t  *header;      /* Index header for message type */
        H5SM_table_cache_ud_t cache_udata; /* User-data for callback */
        ssize_t               index_num;   /* Table index for message type */

        /* Set up user data for callback */
        cache_udata.f = f;

        /* Look up the master SOHM table */
        if (NULL == (table = (H5SM_master_table_t *)H5AC_protect(f, H5AC_SOHM_TABLE, H5F_SOHM_ADDR(f),
                                                                 &cache_udata, H5AC__READ_ONLY_FLAG)))
            HGOTO_ERROR(H5E_SOHM, H5E_CANTPROTECT, FAIL, "unable to load SOHM master table");

        /* Find the correct index for this message type */
        if ((index_num = H5SM__get_index(table, type_id)) < 0)
            HGOTO_ERROR(H5E_SOHM, H5E_NOTFOUND, FAIL, "unable to find correct SOHM index");
        header = &(table->indexes[index_num]);

        /* Set the message count for the type */
        *mesg_count = header->num_messages;
    } /* end if */
    else
        /* No shared messages of any type */
        *mesg_count = 0;

done:
    /* Release resources */
    if (table && H5AC_unprotect(f, H5AC_SOHM_TABLE, H5F_SOHM_ADDR(f), table, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_SOHM, H5E_CANTUNPROTECT, FAIL, "unable to close SOHM master table");

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5SM__get_mesg_count_test() */
