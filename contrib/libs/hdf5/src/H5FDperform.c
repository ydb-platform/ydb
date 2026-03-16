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

#include "H5FDmodule.h" /* This source code file is part of the H5FD module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5CXprivate.h" /* API Contexts                             */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5FDpkg.h"     /* File Drivers                             */
#include "H5Iprivate.h"  /* IDs                                      */

/*-------------------------------------------------------------------------
 * Function:    H5FDperform_init
 *
 * Purpose:     Ensure that the library is initialized and then call
 *              the provided VFD initializer
 *
 * Return:      Success:        Identifier for the VFD just initialized
 *              Failure:        H5I_INVALID_HID
 *-------------------------------------------------------------------------
 */
hid_t
H5FDperform_init(H5FD_init_t op)
{
    hid_t ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API_NOINIT
    /*NO TRACE*/

    /* It is possible that an application will evaluate an
     * `H5FD_*` symbol (`H5FD_FAMILY`, `H5FD_MULTI`, `H5FD_SEC2`, etc.
     * before the library has had an opportunity to initialize. Call
     * H5_init_library() to make sure that the library has been initialized
     * before `init` is run.
     */
    if (H5_init_library() < 0)
        HGOTO_ERROR(H5E_FUNC, H5E_CANTINIT, H5I_INVALID_HID, "library initialization failed");

    ret_value = op();

done:
    FUNC_LEAVE_API_NOINIT(ret_value)
}
