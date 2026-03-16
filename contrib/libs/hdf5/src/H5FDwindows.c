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

#include "H5private.h"   /* Generic Functions        */
#include "H5Eprivate.h"  /* Error handling           */
#include "H5Fprivate.h"  /* File access              */
#include "H5FDprivate.h" /* File drivers             */
#include "H5FDwindows.h" /* Windows file driver      */
#include "H5FDsec2.h"    /* Windows file driver      */
#include "H5FLprivate.h" /* Free Lists               */
#include "H5Iprivate.h"  /* IDs                      */
#include "H5MMprivate.h" /* Memory management        */
#include "H5Pprivate.h"  /* Property lists           */

#ifdef H5_HAVE_WINDOWS

/*-------------------------------------------------------------------------
 * Function:    H5Pset_fapl_windows
 *
 * Purpose: Modify the file access property list to use the H5FD_WINDOWS
 *          driver defined in this source file.  There are no driver
 *          specific properties.
 *
 * NOTE: The Windows VFD was merely a merge of the SEC2 and STDIO drivers
 *       so it has been retired.  Selecting the Windows VFD will actually
 *       set the SEC2 VFD (though for backwards compatibility, we'll keep
 *       the H5FD_WINDOWS symbol).
 *
 *
 * Return:  Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_fapl_windows(hid_t fapl_id)
{
    H5P_genplist_t *plist; /* Property list pointer */
    herr_t          ret_value;

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", fapl_id);

    if (NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");

    ret_value = H5P_set_driver(plist, H5FD_WINDOWS, NULL, NULL);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_fapl_windows() */

#endif /* H5_HAVE_WINDOWS */
