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
 * Created:		H5FDtest.c
 *			    Fall 2014
 *
 * Purpose:		File driver testing routines.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5FDmodule.h" /* This source code file is part of the H5FD module */
#define H5FD_TESTING    /* Suppress warning about H5FD testing funcs    */

/***********/
/* Headers */
/***********/
#include "H5private.h" /* Generic Functions    */
#include "H5FDpkg.h"   /* File Drivers         */

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
 * Function:	H5FD__supports_swmr_test()
 *
 * Purpose:	    Determines if a VFD supports SWMR.
 *
 *              The function determines SWMR support by inspecting the
 *              HDF5_DRIVER environment variable, not by checking the
 *              VFD feature flags (which do not exist until the driver
 *              is instantiated).
 *
 *              This function is only intended for use in the test code.
 *
 * Return:	    true (1) if the VFD supports SWMR I/O or vfd_name is
 *              NULL or the empty string (which implies the default VFD).
 *
 *              false (0) if it does not
 *
 *              This function cannot fail at this time so there is no
 *              error return value.
 *-------------------------------------------------------------------------
 */
bool
H5FD__supports_swmr_test(const char *vfd_name)
{
    bool ret_value = false;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    if (!vfd_name || !strcmp(vfd_name, "") || !strcmp(vfd_name, "nomatch"))
        ret_value = true;
    else
        ret_value = !strcmp(vfd_name, "log") || !strcmp(vfd_name, "sec2");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__supports_swmr_test() */
