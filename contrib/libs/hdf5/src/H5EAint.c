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
 * Created:		H5EAint.c
 *
 * Purpose:		Internal routines for extnsible arrays.
 *
 *-------------------------------------------------------------------------
 */

/**********************/
/* Module Declaration */
/**********************/

#include "H5EAmodule.h" /* This source code file is part of the H5EA module */

/***********************/
/* Other Packages Used */
/***********************/

/***********/
/* Headers */
/***********/
#include "H5private.h"  /* Generic Functions			*/
#include "H5Eprivate.h" /* Error handling		  	*/
#include "H5EApkg.h"    /* Extensible Arrays			*/

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
 * Function:	H5EA__create_flush_depend
 *
 * Purpose:	Create a flush dependency between two data structure components
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5EA__create_flush_depend(H5AC_info_t *parent_entry, H5AC_info_t *child_entry)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(parent_entry);
    assert(child_entry);

    /* Create a flush dependency between parent and child entry */
    if (H5AC_create_flush_dependency(parent_entry, child_entry) < 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTDEPEND, FAIL, "unable to create flush dependency");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__create_flush_depend() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__destroy_flush_depend
 *
 * Purpose:	Destroy a flush dependency between two data structure components
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5EA__destroy_flush_depend(H5AC_info_t *parent_entry, H5AC_info_t *child_entry)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(parent_entry);
    assert(child_entry);

    /* Destroy a flush dependency between parent and child entry */
    if (H5AC_destroy_flush_dependency(parent_entry, child_entry) < 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTUNDEPEND, FAIL, "unable to destroy flush dependency");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__destroy_flush_depend() */
