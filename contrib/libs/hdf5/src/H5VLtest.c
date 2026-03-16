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
 * Created:		H5VLtest.c
 *
 * Purpose:		Virtual Object Layer (VOL) testing routines.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5VLmodule.h" /* This source code file is part of the H5VL module */
#define H5VL_TESTING    /* Suppress warning about H5VL testing funcs        */

/***********/
/* Headers */
/***********/
#include "H5private.h"  /* Generic Functions                    */
#include "H5Eprivate.h" /* Error handling                       */
#include "H5VLpkg.h"    /* Virtual Object Layer                 */

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
 * Function:    H5VL__reparse_def_vol_conn_variable_test
 *
 * Purpose:     Re-parse the default VOL connector environment variable.
 *
 *              Since getenv(3) is fairly expensive, we only parse it once,
 *              when the library opens. This test function is used to
 *              re-parse the environment variable after we've changed it
 *              with setenv(3).
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__reparse_def_vol_conn_variable_test(void)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Re-check for the HDF5_VOL_CONNECTOR environment variable */
    if (H5VL__set_def_conn() < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "unable to initialize default VOL connector");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__reparse_def_vol_conn_variable_test() */
