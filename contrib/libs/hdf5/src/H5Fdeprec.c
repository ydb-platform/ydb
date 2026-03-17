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
 * Created:	H5Fdeprec.c
 *
 * Purpose:	Deprecated functions from the H5F interface.  These
 *              functions are here for compatibility purposes and may be
 *              removed in the future.  Applications should switch to the
 *              newer APIs.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Fmodule.h" /* This source code file is part of the H5F module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5CXprivate.h" /* API Contexts                             */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5Fpkg.h"      /* File access                              */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5SMprivate.h" /* Shared object header messages            */

#include "H5VLnative_private.h" /* Native VOL connector                     */

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

#ifndef H5_NO_DEPRECATED_SYMBOLS

/*-------------------------------------------------------------------------
 * Function:    H5Fget_info1
 *
 * Purpose:     Gets general information about the file, including:
 *              1. Get storage size for superblock extension if there is one.
 *              2. Get the amount of btree and heap storage for entries
 *                 in the SOHM table if there is one.
 *              3. The amount of free space tracked in the file.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Fget_info1(hid_t obj_id, H5F_info1_t *finfo /*out*/)
{
    H5VL_object_t                   *vol_obj = NULL;
    H5VL_optional_args_t             vol_cb_args;   /* Arguments to VOL callback */
    H5VL_native_file_optional_args_t file_opt_args; /* Arguments for optional operation */
    H5I_type_t                       type;
    H5F_info2_t                      finfo2;              /* Current file info struct */
    herr_t                           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", obj_id, finfo);

    /* Check args */
    if (!finfo)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no info struct");

    /* Check the type */
    type = H5I_get_type(obj_id);
    if (H5I_FILE != type && H5I_GROUP != type && H5I_DATATYPE != type && H5I_DATASET != type &&
        H5I_ATTR != type)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file or file object");

    /* Get the file object */
    if (NULL == (vol_obj = H5VL_vol_object(obj_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid object identifier");

    /* Set up VOL callback arguments */
    file_opt_args.get_info.type  = type;
    file_opt_args.get_info.finfo = &finfo2;
    vol_cb_args.op_type          = H5VL_NATIVE_FILE_GET_INFO;
    vol_cb_args.args             = &file_opt_args;

    /* Get the file information */
    if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "unable to retrieve file info");

    /* Copy the compatible fields into the older struct */
    finfo->super_ext_size = finfo2.super.super_ext_size;
    finfo->sohm.hdr_size  = finfo2.sohm.hdr_size;
    finfo->sohm.msgs_info = finfo2.sohm.msgs_info;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fget_info1() */

/*-------------------------------------------------------------------------
 * Function:    H5Fis_hdf5
 *
 * Purpose:     Check the file signature to detect an HDF5 file.
 *
 * Bugs:        This function is not robust: it only uses the default file
 *              driver when attempting to open the file when in fact it
 *              should use all known file drivers.
 *
 * Return:      true/false/FAIL
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5Fis_hdf5(const char *name)
{
    H5VL_file_specific_args_t vol_cb_args;           /* Arguments to VOL callback */
    bool                      is_accessible = false; /* Whether file is accessible */
    htri_t                    ret_value;             /* Return value */

    FUNC_ENTER_API((-1))
    H5TRACE1("t", "*s", name);

    /* Check args and all the boring stuff. */
    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, (-1), "no file name specified");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                       = H5VL_FILE_IS_ACCESSIBLE;
    vol_cb_args.args.is_accessible.filename   = name;
    vol_cb_args.args.is_accessible.fapl_id    = H5P_FILE_ACCESS_DEFAULT;
    vol_cb_args.args.is_accessible.accessible = &is_accessible;

    /* Check if file is accessible */
    if (H5VL_file_specific(NULL, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_NOTHDF5, (-1), "unable to determine if file is accessible as HDF5");

    /* Set return value */
    ret_value = (htri_t)is_accessible;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fis_hdf5() */

/*-------------------------------------------------------------------------
 * Function:    H5Fset_latest_format
 *
 * Purpose:     Enable switching between latest or non-latest format while
 *              a file is open.
 *              This is deprecated starting release 1.10.2 and is modified
 *              to call the private H5F_set_libver_bounds() to set the
 *              bounds.
 *
 *              Before release 1.10.2, the library supports only two
 *              combinations of low/high bounds:
 *                  (earliest, latest)
 *                  (latest, latest)
 *              Thus, this public routine does the job in switching
 *              between the two combinations listed above.
 *
 *              Starting release 1.10.2, we add v18 to the enumerated
 *              define H5F_libver_t and the library supports five combinations
 *              as below:
 *                  (earliest, v18)
 *                  (earliest, v10)
 *                  (v18, v18)
 *                  (v18, v10)
 *                  (v10, v10)
 *              So we introduce the new public routine H5Fset_libver_bounds()
 *              in place of H5Fset_latest_format().
 *              See also RFC: Setting Bounds for Object Creation in HDF5 1.10.0.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Fset_latest_format(hid_t file_id, hbool_t latest_format)
{
    H5VL_object_t                   *vol_obj;                       /* File as VOL object           */
    H5VL_optional_args_t             vol_cb_args;                   /* Arguments to VOL callback */
    H5VL_native_file_optional_args_t file_opt_args;                 /* Arguments for optional operation */
    H5F_libver_t                     low       = H5F_LIBVER_LATEST; /* Low bound 		    */
    herr_t                           ret_value = SUCCEED;           /* Return value                 */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ib", file_id, latest_format);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(file_id, H5I_FILE)))
        HGOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "not a file ID");

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(file_id) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTSET, FAIL, "can't set collective metadata read info");

    /* 'low' and 'high' are both initialized to LATEST.
     * If latest format is not expected, set 'low' to EARLIEST
     */
    if (!latest_format)
        low = H5F_LIBVER_EARLIEST;

    /* Set up VOL callback arguments */
    file_opt_args.set_libver_bounds.low  = low;
    file_opt_args.set_libver_bounds.high = H5F_LIBVER_LATEST;
    vol_cb_args.op_type                  = H5VL_NATIVE_FILE_SET_LIBVER_BOUNDS;
    vol_cb_args.args                     = &file_opt_args;

    /* Set the library's version bounds */
    if (H5VL_file_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_FILE, H5E_CANTSET, FAIL, "can't set library version bounds");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Fset_latest_format() */
#endif /* H5_NO_DEPRECATED_SYMBOLS */
