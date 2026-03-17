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
 * Created:	H5Ddeprec.c
 *
 * Purpose:	Deprecated functions from the H5D interface.  These
 *              functions are here for compatibility purposes and may be
 *              removed in the future.  Applications should switch to the
 *              newer APIs.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Dmodule.h" /* This source code file is part of the H5D module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5CXprivate.h" /* API Contexts                         */
#include "H5Dpkg.h"      /* Datasets 				*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Iprivate.h"  /* IDs			  		*/
#include "H5VLprivate.h" /* Virtual Object Layer                 */

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
 * Function:    H5Dcreate1
 *
 * Purpose:     Creates a new dataset named NAME at LOC_ID, opens the
 *              dataset for access, and associates with that dataset constant
 *              and initial persistent properties including the type of each
 *              datapoint as stored in the file (TYPE_ID), the size of the
 *              dataset (SPACE_ID), and other initial miscellaneous
 *              properties (DCPL_ID).
 *
 *              All arguments are copied into the dataset, so the caller is
 *              allowed to derive new types, data spaces, and creation
 *              parameters from the old ones and reuse them in calls to
 *              create other datasets.
 *
 * Return:      Success:    The object ID of the new dataset. At this
 *                          point, the dataset is ready to receive its
 *                          raw data. Attempting to read raw data from
 *                          the dataset will probably return the fill
 *                          value. The dataset should be closed when
 *                          the caller is no longer interested in it.
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Dcreate1(hid_t loc_id, const char *name, hid_t type_id, hid_t space_id, hid_t dcpl_id)
{
    void             *dset    = NULL; /* dset object from VOL connector */
    H5VL_object_t    *vol_obj = NULL; /* object of loc_id */
    H5VL_loc_params_t loc_params;
    hid_t             ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE5("i", "i*siii", loc_id, name, type_id, space_id, dcpl_id);

    /* Check arguments */
    if (!name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "name parameter cannot be NULL");
    if (!*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "name parameter cannot be an empty string");

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(loc_id) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, H5I_INVALID_HID, "can't set collective metadata read");

    if (H5P_DEFAULT == dcpl_id)
        dcpl_id = H5P_DATASET_CREATE_DEFAULT;
    else if (true != H5P_isa_class(dcpl_id, H5P_DATASET_CREATE))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not dataset create property list ID");

    /* Set the DCPL for the API context */
    H5CX_set_dcpl(dcpl_id);

    /* Set location parameters */
    loc_params.type     = H5VL_OBJECT_BY_SELF;
    loc_params.obj_type = H5I_get_type(loc_id);

    /* get the location object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid location identifier");

    /* Create the dataset */
    if (NULL == (dset = H5VL_dataset_create(vol_obj, &loc_params, name, H5P_LINK_CREATE_DEFAULT, type_id,
                                            space_id, dcpl_id, H5P_DATASET_ACCESS_DEFAULT,
                                            H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, H5I_INVALID_HID, "unable to create dataset");

    /* Register the new dataset to get an ID for it */
    if ((ret_value = H5VL_register(H5I_DATASET, dset, vol_obj->connector, true)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register dataset");

done:
    if (H5I_INVALID_HID == ret_value)
        if (dset && H5VL_dataset_close(vol_obj, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, H5I_INVALID_HID, "unable to release dataset");

    FUNC_LEAVE_API(ret_value)
} /* end H5Dcreate1() */

/*-------------------------------------------------------------------------
 * Function:    H5Dopen1
 *
 * Purpose:     Finds a dataset named NAME at LOC_ID, opens it, and returns
 *              its ID. The dataset should be close when the caller is no
 *              longer interested in it.
 *
 * Note:        Deprecated in favor of H5Dopen2
 *
 * Return:      Success:    A new dataset ID
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Dopen1(hid_t loc_id, const char *name)
{
    void             *dset    = NULL; /* dset object from VOL connector */
    H5VL_object_t    *vol_obj = NULL; /* object of loc_id */
    H5VL_loc_params_t loc_params;
    hid_t             ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE2("i", "i*s", loc_id, name);

    /* Check args */
    if (!name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "name parameter cannot be NULL");
    if (!*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "name parameter cannot be an empty string");

    /* Set location parameters */
    loc_params.type     = H5VL_OBJECT_BY_SELF;
    loc_params.obj_type = H5I_get_type(loc_id);

    /* get the location object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid location identifier");

    /* Open the dataset */
    if (NULL == (dset = H5VL_dataset_open(vol_obj, &loc_params, name, H5P_DATASET_ACCESS_DEFAULT,
                                          H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, H5I_INVALID_HID, "unable to open dataset");

    /* Get an ID for the dataset */
    if ((ret_value = H5VL_register(H5I_DATASET, dset, vol_obj->connector, true)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTREGISTER, H5I_INVALID_HID, "can't register dataset ID");

done:
    if (H5I_INVALID_HID == ret_value)
        if (dset && H5VL_dataset_close(vol_obj, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, H5I_INVALID_HID, "unable to release dataset");

    FUNC_LEAVE_API(ret_value)
} /* end H5Dopen1() */

/*-------------------------------------------------------------------------
 * Function:    H5Dextend
 *
 * Purpose:     This function makes sure that the dataset is at least of size
 *              SIZE. The dimensionality of SIZE is the same as the data
 *              space of the dataset being changed.
 *
 * Note:        Deprecated in favor of H5Dset_extent
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dextend(hid_t dset_id, const hsize_t size[])
{
    H5VL_object_t               *vol_obj;                 /* Object for loc_id */
    H5VL_dataset_get_args_t      vol_get_cb_args;         /* Arguments to VOL callback */
    H5VL_dataset_specific_args_t vol_spec_cb_args;        /* Arguments to VOL callback */
    hid_t                        sid = H5I_INVALID_HID;   /* Dataspace ID */
    H5S_t                       *ds  = NULL;              /* Dataspace struct */
    int                          ndims;                   /* Dataset/space rank */
    hsize_t                      dset_dims[H5S_MAX_RANK]; /* Current dataset dimensions */
    int                          i;                       /* Local index variable */
    herr_t                       ret_value = SUCCEED;     /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "i*h", dset_id, size);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(dset_id, H5I_DATASET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid dataset identifier");
    if (!size)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no size specified");

    /* Set up VOL callback arguments */
    vol_get_cb_args.op_type                 = H5VL_DATASET_GET_SPACE;
    vol_get_cb_args.args.get_space.space_id = H5I_INVALID_HID;

    /* Get the dataspace pointer for the dataset */
    if (H5VL_dataset_get(vol_obj, &vol_get_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "unable to get dataspace");
    sid = vol_get_cb_args.args.get_space.space_id;
    if (H5I_INVALID_HID == sid)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "received an invalid dataspace from the dataset");
    if (NULL == (ds = (H5S_t *)H5I_object_verify(sid, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "couldn't get dataspace structure from ID");

    /* Get the dataset's current extent */
    if (H5S_get_simple_extent_dims(ds, dset_dims, NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get dataset dimensions");

    /* Get the dataset dimensions */
    ndims = H5S_GET_EXTENT_NDIMS(ds);

    /* Make certain that the dataset dimensions don't decrease in any dimension.
     *
     * (Shrinking dimensions is possible with H5Dset_extent, but not H5Dextend)
     *
     * XXX (VOL_MERGE): I feel like we should fail here instead of just silently
     *                  not doing what we're supposed to do.
     */
    for (i = 0; i < ndims; i++)
        if (size[i] > dset_dims[i])
            dset_dims[i] = size[i];

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(dset_id) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set collective metadata read info");

    /* Set up VOL callback arguments */
    vol_spec_cb_args.op_type              = H5VL_DATASET_SET_EXTENT;
    vol_spec_cb_args.args.set_extent.size = dset_dims;

    /* Increase size */
    if (H5VL_dataset_specific(vol_obj, &vol_spec_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "unable to extend dataset");

done:
    /* Close the dataspace */
    if (sid != H5I_INVALID_HID && H5I_dec_app_ref(sid) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "can't close dataspace");

    FUNC_LEAVE_API(ret_value)
} /* end H5Dextend() */

/*-------------------------------------------------------------------------
 * Function:    H5Dvlen_reclaim
 *
 * Purpose: Frees the buffers allocated for storing variable-length data
 *      in memory.  Only frees the VL data in the selection defined in the
 *      dataspace.  The dataset transfer property list is required to find the
 *      correct allocation/free methods for the VL data in the buffer.
 *
 * Return:  Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dvlen_reclaim(hid_t type_id, hid_t space_id, hid_t dxpl_id, void *buf)
{
    H5S_t *space;     /* Dataspace for iteration */
    herr_t ret_value; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "iii*x", type_id, space_id, dxpl_id, buf);

    /* Check args */
    if (H5I_DATATYPE != H5I_get_type(type_id) || buf == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid argument");
    if (NULL == (space = (H5S_t *)H5I_object_verify(space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid dataspace");
    if (!(H5S_has_extent(space)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "dataspace does not have extent set");

    /* Get the default dataset transfer property list if the user didn't provide one */
    if (H5P_DEFAULT == dxpl_id)
        dxpl_id = H5P_DATASET_XFER_DEFAULT;
    else if (true != H5P_isa_class(dxpl_id, H5P_DATASET_XFER))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not xfer parms");

    /* Set DXPL for operation */
    H5CX_set_dxpl(dxpl_id);

    /* Call internal routine */
    ret_value = H5T_reclaim(type_id, space, buf);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Dvlen_reclaim() */

#endif /* H5_NO_DEPRECATED_SYMBOLS */
