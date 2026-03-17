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
#include "H5ESprivate.h" /* Event Sets                               */
#include "H5FLprivate.h" /* Free lists                               */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5MMprivate.h" /* Memory management                        */
#include "H5VLprivate.h" /* Virtual Object Layer                     */

#include "H5VLnative_private.h" /* Native VOL connector                     */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Local Prototypes */
/********************/

/* Helper routines for sync/async API calls */
static hid_t  H5D__create_api_common(hid_t loc_id, const char *name, hid_t type_id, hid_t space_id,
                                     hid_t lcpl_id, hid_t dcpl_id, hid_t dapl_id, void **token_ptr,
                                     H5VL_object_t **_vol_obj_ptr);
static hid_t  H5D__open_api_common(hid_t loc_id, const char *name, hid_t dapl_id, void **token_ptr,
                                   H5VL_object_t **_vol_obj_ptr);
static hid_t  H5D__get_space_api_common(hid_t dset_id, void **token_ptr, H5VL_object_t **_vol_obj_ptr);
static herr_t H5D__read_api_common(size_t count, hid_t dset_id[], hid_t mem_type_id[], hid_t mem_space_id[],
                                   hid_t file_space_id[], hid_t dxpl_id, void *buf[], void **token_ptr,
                                   H5VL_object_t **_vol_obj_ptr);
static herr_t H5D__write_api_common(size_t count, hid_t dset_id[], hid_t mem_type_id[], hid_t mem_space_id[],
                                    hid_t file_space_id[], hid_t dxpl_id, const void *buf[], void **token_ptr,
                                    H5VL_object_t **_vol_obj_ptr);
static herr_t H5D__set_extent_api_common(hid_t dset_id, const hsize_t size[], void **token_ptr,
                                         H5VL_object_t **_vol_obj_ptr);

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/* Declare extern free list to manage the H5S_sel_iter_t struct */
H5FL_EXTERN(H5S_sel_iter_t);

/* Declare extern the free list to manage blocks of type conversion data */
H5FL_BLK_EXTERN(type_conv);

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:    H5D__create_api_common
 *
 * Purpose:     This is the common function for creating HDF5 datasets.
 *
 * Return:      Success:    A dataset ID
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
static hid_t
H5D__create_api_common(hid_t loc_id, const char *name, hid_t type_id, hid_t space_id, hid_t lcpl_id,
                       hid_t dcpl_id, hid_t dapl_id, void **token_ptr, H5VL_object_t **_vol_obj_ptr)
{
    void           *dset        = NULL; /* New dataset's info */
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_loc_params_t loc_params;                     /* Location parameters for object access */
    hid_t             ret_value = H5I_INVALID_HID;    /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (!name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "name parameter cannot be NULL");
    if (!*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "name parameter cannot be an empty string");

    /* Set up object access arguments */
    if (H5VL_setup_acc_args(loc_id, H5P_CLS_DACC, true, &dapl_id, vol_obj_ptr, &loc_params) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, H5I_INVALID_HID, "can't set object access arguments");

    /* Get link creation property list */
    if (H5P_DEFAULT == lcpl_id)
        lcpl_id = H5P_LINK_CREATE_DEFAULT;
    else if (true != H5P_isa_class(lcpl_id, H5P_LINK_CREATE))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "lcpl_id is not a link creation property list");

    /* Get dataset creation property list */
    if (H5P_DEFAULT == dcpl_id)
        dcpl_id = H5P_DATASET_CREATE_DEFAULT;
    else if (true != H5P_isa_class(dcpl_id, H5P_DATASET_CREATE))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID,
                    "dcpl_id is not a dataset create property list ID");

    /* Set the DCPL for the API context */
    H5CX_set_dcpl(dcpl_id);

    /* Set the LCPL for the API context */
    H5CX_set_lcpl(lcpl_id);

    /* Create the dataset */
    if (NULL == (dset = H5VL_dataset_create(*vol_obj_ptr, &loc_params, name, lcpl_id, type_id, space_id,
                                            dcpl_id, dapl_id, H5P_DATASET_XFER_DEFAULT, token_ptr)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCREATE, H5I_INVALID_HID, "unable to create dataset");

    /* Get an ID for the dataset */
    if ((ret_value = H5VL_register(H5I_DATASET, dset, (*vol_obj_ptr)->connector, true)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register dataset");

done:
    if (H5I_INVALID_HID == ret_value)
        if (dset && H5VL_dataset_close(*vol_obj_ptr, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, H5I_INVALID_HID, "unable to release dataset");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__create_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Dcreate2
 *
 * Purpose:     Creates a new dataset named NAME at LOC_ID, opens the
 *              dataset for access, and associates with that dataset constant
 *              and initial persistent properties including the type of each
 *              datapoint as stored in the file (TYPE_ID), the size of the
 *              dataset (SPACE_ID), and other initial miscellaneous
 *              properties (DCPL_ID).
 *
 *              All arguments are copied into the dataset, so the caller is
 *              allowed to derive new types, dataspaces, and creation
 *              parameters from the old ones and reuse them in calls to
 *              create other datasets.
 *
 * Return:      Success:    The object ID of the new dataset. At this
 *                          point, the dataset is ready to receive its
 *                          raw data. Attempting to read raw data from
 *                          the dataset will probably return the fill
 *                          value. The dataset should be closed when the
 *                          caller is no longer interested in it.
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Dcreate2(hid_t loc_id, const char *name, hid_t type_id, hid_t space_id, hid_t lcpl_id, hid_t dcpl_id,
           hid_t dapl_id)
{
    hid_t ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE7("i", "i*siiiii", loc_id, name, type_id, space_id, lcpl_id, dcpl_id, dapl_id);

    /* Create the dataset synchronously */
    if ((ret_value = H5D__create_api_common(loc_id, name, type_id, space_id, lcpl_id, dcpl_id, dapl_id, NULL,
                                            NULL)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCREATE, H5I_INVALID_HID, "unable to synchronously create dataset");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Dcreate2() */

/*-------------------------------------------------------------------------
 * Function:    H5Dcreate_async
 *
 * Purpose:     Asynchronous version of H5Dcreate
 *
 * Return:      Success:    A dataset ID
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Dcreate_async(const char *app_file, const char *app_func, unsigned app_line, hid_t loc_id, const char *name,
                hid_t type_id, hid_t space_id, hid_t lcpl_id, hid_t dcpl_id, hid_t dapl_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    hid_t          ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE11("i", "*s*sIui*siiiiii", app_file, app_func, app_line, loc_id, name, type_id, space_id, lcpl_id,
              dcpl_id, dapl_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Create the dataset asynchronously */
    if ((ret_value = H5D__create_api_common(loc_id, name, type_id, space_id, lcpl_id, dcpl_id, dapl_id,
                                            token_ptr, &vol_obj)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCREATE, H5I_INVALID_HID, "unable to asynchronously create dataset");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE11(__func__, "*s*sIui*siiiiii", app_file, app_func, app_line, loc_id, name, type_id, space_id, lcpl_id, dcpl_id, dapl_id, es_id)) < 0) {
            /* clang-format on */
            if (H5I_dec_app_ref_always_close(ret_value) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CANTDEC, H5I_INVALID_HID, "can't decrement count on dataset ID");
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINSERT, H5I_INVALID_HID, "can't insert token into event set");
        } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Dcreate_async() */

/*-------------------------------------------------------------------------
 * Function:    H5Dcreate_anon
 *
 * Purpose:     Creates a new dataset named NAME at LOC_ID, opens the
 *              dataset for access, and associates with that dataset constant
 *              and initial persistent properties including the type of each
 *              datapoint as stored in the file (TYPE_ID), the size of the
 *              dataset (SPACE_ID), and other initial miscellaneous
 *              properties (DCPL_ID).
 *
 *              All arguments are copied into the dataset, so the caller is
 *              allowed to derive new types, dataspaces, and creation
 *              parameters from the old ones and reuse them in calls to
 *              create other datasets.
 *
 *              The resulting ID should be linked into the file with
 *              H5Olink or it will be deleted when closed.
 *
 * Return:      Success:    The object ID of the new dataset. At this
 *                          point, the dataset is ready to receive its
 *                          raw data. Attempting to read raw data from
 *                          the dataset will probably return the fill
 *                          value. The dataset should be linked into
 *                          the group hierarchy before being closed or
 *                          it will be deleted. The dataset should be
 *                          closed when the caller is no longer interested
 *                          in it.
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Dcreate_anon(hid_t loc_id, hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id)
{
    void             *dset    = NULL;              /* dset object from VOL connector */
    H5VL_object_t    *vol_obj = NULL;              /* Object for loc_id */
    H5VL_loc_params_t loc_params;                  /* Location parameters for object access */
    hid_t             ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE5("i", "iiiii", loc_id, type_id, space_id, dcpl_id, dapl_id);

    /* Check arguments */
    if (H5P_DEFAULT == dcpl_id)
        dcpl_id = H5P_DATASET_CREATE_DEFAULT;
    else if (true != H5P_isa_class(dcpl_id, H5P_DATASET_CREATE))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not dataset create property list ID");

    if (H5P_DEFAULT == dapl_id)
        dapl_id = H5P_DATASET_ACCESS_DEFAULT;
    else if (true != H5P_isa_class(dapl_id, H5P_DATASET_ACCESS))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not dataset access property list ID");

    /* Set the DCPL for the API context */
    H5CX_set_dcpl(dcpl_id);

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&dapl_id, H5P_CLS_DACC, loc_id, true) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, H5I_INVALID_HID, "can't set access property list info");

    /* get the location object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid location identifier");

    /* Set location parameters */
    loc_params.type     = H5VL_OBJECT_BY_SELF;
    loc_params.obj_type = H5I_get_type(loc_id);

    /* Create the dataset */
    if (NULL ==
        (dset = H5VL_dataset_create(vol_obj, &loc_params, NULL, H5P_LINK_CREATE_DEFAULT, type_id, space_id,
                                    dcpl_id, dapl_id, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, H5I_INVALID_HID, "unable to create dataset");

    /* Get an ID for the dataset */
    if ((ret_value = H5VL_register(H5I_DATASET, dset, vol_obj->connector, true)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register dataset");

done:
    /* Cleanup on failure */
    if (H5I_INVALID_HID == ret_value)
        if (dset && H5VL_dataset_close(vol_obj, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, H5I_INVALID_HID, "unable to release dataset");

    FUNC_LEAVE_API(ret_value)
} /* end H5Dcreate_anon() */

/*-------------------------------------------------------------------------
 * Function:    H5D__open_api_common
 *
 * Purpose:     This is the common function for opening a dataset
 *
 * Return:      Success:    Object ID of the dataset
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
static hid_t
H5D__open_api_common(hid_t loc_id, const char *name, hid_t dapl_id, void **token_ptr,
                     H5VL_object_t **_vol_obj_ptr)
{
    void           *dset        = NULL; /* dset object from VOL connector */
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_loc_params_t loc_params;                     /* Location parameters for object access */
    hid_t             ret_value = H5I_INVALID_HID;    /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    if (!name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "name parameter cannot be NULL");
    if (!*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "name parameter cannot be an empty string");

    /* Set up object access arguments */
    if (H5VL_setup_acc_args(loc_id, H5P_CLS_DACC, false, &dapl_id, vol_obj_ptr, &loc_params) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, H5I_INVALID_HID, "can't set object access arguments");

    /* Open the dataset */
    if (NULL == (dset = H5VL_dataset_open(*vol_obj_ptr, &loc_params, name, dapl_id, H5P_DATASET_XFER_DEFAULT,
                                          token_ptr)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, H5I_INVALID_HID, "unable to open dataset");

    /* Register an atom for the dataset */
    if ((ret_value = H5VL_register(H5I_DATASET, dset, (*vol_obj_ptr)->connector, true)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTREGISTER, H5I_INVALID_HID, "can't register dataset ID");

done:
    if (H5I_INVALID_HID == ret_value)
        if (dset && H5VL_dataset_close(*vol_obj_ptr, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, H5I_INVALID_HID, "unable to release dataset");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__open_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Dopen2
 *
 * Purpose:     Finds a dataset named NAME at LOC_ID, opens it, and returns
 *              its ID. The dataset should be close when the caller is no
 *              longer interested in it.
 *
 *              Takes a dataset access property list
 *
 * Return:      Success:    Object ID of the dataset
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Dopen2(hid_t loc_id, const char *name, hid_t dapl_id)
{
    hid_t ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE3("i", "i*si", loc_id, name, dapl_id);

    /* Open the dataset synchronously */
    if ((ret_value = H5D__open_api_common(loc_id, name, dapl_id, NULL, NULL)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, H5I_INVALID_HID, "unable to synchronously open dataset");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Dopen2() */

/*-------------------------------------------------------------------------
 * Function:    H5Dopen_async
 *
 * Purpose:     Asynchronous version of H5Dopen2
 *
 * Return:      Success:    A group ID
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Dopen_async(const char *app_file, const char *app_func, unsigned app_line, hid_t loc_id, const char *name,
              hid_t dapl_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    hid_t          ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE7("i", "*s*sIui*sii", app_file, app_func, app_line, loc_id, name, dapl_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Open the dataset asynchronously */
    if ((ret_value = H5D__open_api_common(loc_id, name, dapl_id, token_ptr, &vol_obj)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, H5I_INVALID_HID, "unable to asynchronously open dataset");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE7(__func__, "*s*sIui*sii", app_file, app_func, app_line, loc_id, name, dapl_id, es_id)) < 0) {
            /* clang-format on */
            if (H5I_dec_app_ref_always_close(ret_value) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CANTDEC, H5I_INVALID_HID, "can't decrement count on dataset ID");
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINSERT, H5I_INVALID_HID, "can't insert token into event set");
        } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Dopen_async() */

/*-------------------------------------------------------------------------
 * Function:    H5Dclose
 *
 * Purpose:     Closes access to a dataset and releases resources used by
 *              it. It is illegal to subsequently use that same dataset
 *              ID in calls to other dataset functions.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dclose(hid_t dset_id)
{
    herr_t ret_value = SUCCEED; /* Return value                     */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", dset_id);

    /* Check args */
    if (H5I_DATASET != H5I_get_type(dset_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataset ID");

    /* Decrement the counter on the dataset.  It will be freed if the count
     * reaches zero.
     */
    if (H5I_dec_app_ref_always_close(dset_id) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "can't decrement count on dataset ID");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Dclose() */

/*-------------------------------------------------------------------------
 * Function:    H5Dclose_async
 *
 * Purpose:     Asynchronous version of H5Dclose
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dclose_async(const char *app_file, const char *app_func, unsigned app_line, hid_t dset_id, hid_t es_id)
{
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    H5VL_object_t *vol_obj   = NULL;            /* VOL object of dset_id */
    H5VL_t        *connector = NULL;            /* VOL connector */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "*s*sIuii", app_file, app_func, app_line, dset_id, es_id);

    /* Check args */
    if (H5I_DATASET != H5I_get_type(dset_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataset ID");

    /* Get dataset object's connector */
    if (NULL == (vol_obj = H5VL_vol_object(dset_id)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get VOL object for dataset");

    /* Prepare for possible asynchronous operation */
    if (H5ES_NONE != es_id) {
        /* Increase connector's refcount, so it doesn't get closed if closing
         * the dataset closes the file */
        connector = vol_obj->connector;
        H5VL_conn_inc_rc(connector);

        /* Point at token for operation to set up */
        token_ptr = &token;
    } /* end if */

    /* Decrement the counter on the dataset.  It will be freed if the count
     * reaches zero.
     */
    if (H5I_dec_app_ref_always_close_async(dset_id, token_ptr) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "can't decrement count on dataset ID");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE5(__func__, "*s*sIuii", app_file, app_func, app_line, dset_id, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    if (connector && H5VL_conn_dec_rc(connector) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "can't decrement ref count on connector");

    FUNC_LEAVE_API(ret_value)
} /* end H5Dclose_async() */

/*-------------------------------------------------------------------------
 * Function:    H5D__get_space_api_common
 *
 * Purpose:     This is the common function for getting a dataset's dataspace
 *
 * Return:      Success:    ID for a copy of the dataspace.
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
static hid_t
H5D__get_space_api_common(hid_t dset_id, void **token_ptr, H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj);    /* Ptr to object ptr for loc_id */
    H5VL_dataset_get_args_t vol_cb_args;                 /* Arguments to VOL callback */
    hid_t                   ret_value = H5I_INVALID_HID; /* Return value         */

    FUNC_ENTER_PACKAGE

    /* Check args */
    if (NULL == (*vol_obj_ptr = (H5VL_object_t *)H5I_object_verify(dset_id, H5I_DATASET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid dataset identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                 = H5VL_DATASET_GET_SPACE;
    vol_cb_args.args.get_space.space_id = H5I_INVALID_HID;

    /* Get the dataspace */
    if (H5VL_dataset_get(*vol_obj_ptr, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, token_ptr) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, H5I_INVALID_HID, "unable to get dataspace");

    /* Set return value */
    ret_value = vol_cb_args.args.get_space.space_id;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__get_space_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Dget_space
 *
 * Purpose:     Returns a copy of the file dataspace for a dataset.
 *
 * Return:      Success:    ID for a copy of the dataspace.  The data
 *                          space should be released by calling
 *                          H5Sclose().
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Dget_space(hid_t dset_id)
{
    hid_t ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE1("i", "i", dset_id);

    /* Get the dataset's dataspace synchronously */
    if ((ret_value = H5D__get_space_api_common(dset_id, NULL, NULL)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, H5I_INVALID_HID, "unable to synchronously get dataspace");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Dget_space() */

/*-------------------------------------------------------------------------
 * Function:    H5Dget_space_async
 *
 * Purpose:     Asynchronous version of H5Dget_space
 *
 * Return:      Success:    ID for a copy of the dataspace.  The data
 *                          space should be released by calling
 *                          H5Sclose().
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Dget_space_async(const char *app_file, const char *app_func, unsigned app_line, hid_t dset_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    hid_t          ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE5("i", "*s*sIuii", app_file, app_func, app_line, dset_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Get the dataset's dataspace asynchronously */
    if ((ret_value = H5D__get_space_api_common(dset_id, token_ptr, &vol_obj)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, H5I_INVALID_HID, "unable to asynchronously get dataspace");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE5(__func__, "*s*sIuii", app_file, app_func, app_line, dset_id, es_id)) < 0) {
            /* clang-format on */
            if (H5I_dec_app_ref(ret_value) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CANTDEC, H5I_INVALID_HID,
                            "can't decrement count on dataspace ID");
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINSERT, H5I_INVALID_HID, "can't insert token into event set");
        } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Dget_space_async() */

/*-------------------------------------------------------------------------
 * Function:    H5Dget_space_status
 *
 * Purpose:     Returns the status of dataspace allocation.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dget_space_status(hid_t dset_id, H5D_space_status_t *allocation /*out*/)
{
    H5VL_object_t          *vol_obj;             /* Object for loc_id */
    H5VL_dataset_get_args_t vol_cb_args;         /* Arguments to VOL callback */
    herr_t                  ret_value = SUCCEED; /* Return value         */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", dset_id, allocation);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(dset_id, H5I_DATASET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid dataset identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                      = H5VL_DATASET_GET_SPACE_STATUS;
    vol_cb_args.args.get_space_status.status = allocation;

    /* Get dataspace status */
    if (H5VL_dataset_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "unable to get space status");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Dget_space_status() */

/*-------------------------------------------------------------------------
 * Function:    H5Dget_type
 *
 * Purpose:     Returns a copy of the file datatype for a dataset.
 *
 * Return:      Success:    ID for a copy of the datatype. The data
 *                          type should be released by calling
 *                          H5Tclose().
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Dget_type(hid_t dset_id)
{
    H5VL_object_t          *vol_obj;                     /* Object for loc_id */
    H5VL_dataset_get_args_t vol_cb_args;                 /* Arguments to VOL callback */
    hid_t                   ret_value = H5I_INVALID_HID; /* Return value         */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE1("i", "i", dset_id);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(dset_id, H5I_DATASET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid dataset identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type               = H5VL_DATASET_GET_TYPE;
    vol_cb_args.args.get_type.type_id = H5I_INVALID_HID;

    /* Get the datatype */
    if (H5VL_dataset_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, H5I_INVALID_HID, "unable to get datatype");

    /* Set return value */
    ret_value = vol_cb_args.args.get_type.type_id;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Dget_type() */

/*-------------------------------------------------------------------------
 * Function:    H5Dget_create_plist
 *
 * Purpose:     Returns a copy of the dataset creation property list.
 *
 * Return:      Success:    ID for a copy of the dataset creation
 *                          property list.  The template should be
 *                          released by calling H5P_close().
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Dget_create_plist(hid_t dset_id)
{
    H5VL_object_t          *vol_obj;                     /* Object for loc_id */
    H5VL_dataset_get_args_t vol_cb_args;                 /* Arguments to VOL callback */
    hid_t                   ret_value = H5I_INVALID_HID; /* Return value         */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE1("i", "i", dset_id);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(dset_id, H5I_DATASET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid dataset identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type               = H5VL_DATASET_GET_DCPL;
    vol_cb_args.args.get_dcpl.dcpl_id = H5I_INVALID_HID;

    /* Get the dataset creation property list */
    if (H5VL_dataset_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, H5I_INVALID_HID, "unable to get dataset creation properties");

    /* Set return value */
    ret_value = vol_cb_args.args.get_dcpl.dcpl_id;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Dget_create_plist() */

/*-------------------------------------------------------------------------
 * Function:    H5Dget_access_plist
 *
 * Purpose:     Returns a copy of the dataset access property list.
 *
 * Description: H5Dget_access_plist returns the dataset access property
 *              list identifier of the specified dataset.
 *
 *              The chunk cache parameters in the returned property lists will be
 *              those used by the dataset.  If the properties in the file access
 *              property list were used to determine the dataset's chunk cache
 *              configuration, then those properties will be present in the
 *              returned dataset access property list.  If the dataset does not
 *              use a chunked layout, then the chunk cache properties will be set
 *              to the default.  The chunk cache properties in the returned list
 *              are considered to be “set”, and any use of this list will override
 *              the corresponding properties in the file's file access property
 *              list.
 *
 *              All link access properties in the returned list will be set to the
 *              default values.
 *
 * Return:      Success:    ID for a copy of the dataset access
 *                          property list.  The template should be
 *                          released by calling H5Pclose().
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Dget_access_plist(hid_t dset_id)
{
    H5VL_object_t          *vol_obj;                     /* Object for loc_id */
    H5VL_dataset_get_args_t vol_cb_args;                 /* Arguments to VOL callback */
    hid_t                   ret_value = H5I_INVALID_HID; /* Return value         */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE1("i", "i", dset_id);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(dset_id, H5I_DATASET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid dataset identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type               = H5VL_DATASET_GET_DAPL;
    vol_cb_args.args.get_dapl.dapl_id = H5I_INVALID_HID;

    /* Get the dataset access property list */
    if (H5VL_dataset_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, H5I_INVALID_HID, "unable to get dataset access properties");

    /* Set return value */
    ret_value = vol_cb_args.args.get_dapl.dapl_id;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Dget_access_plist() */

/*-------------------------------------------------------------------------
 * Function:    H5Dget_storage_size
 *
 * Purpose:     Returns the amount of storage that is required for the
 *              dataset. For chunked datasets this is the number of allocated
 *              chunks times the chunk size.
 *
 * Return:      Success:    The amount of storage space allocated for the
 *                          dataset, not counting meta data. The return
 *                          value may be zero if no data has been stored.
 *
 *              Failure:    Zero
 *
 *-------------------------------------------------------------------------
 */
hsize_t
H5Dget_storage_size(hid_t dset_id)
{
    H5VL_object_t          *vol_obj;          /* Object for loc_id */
    H5VL_dataset_get_args_t vol_cb_args;      /* Arguments to VOL callback */
    hsize_t                 storage_size = 0; /* Storage size of dataset */
    hsize_t                 ret_value    = 0; /* Return value                 */

    FUNC_ENTER_API(0)
    H5TRACE1("h", "i", dset_id);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(dset_id, H5I_DATASET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, 0, "invalid dataset identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                            = H5VL_DATASET_GET_STORAGE_SIZE;
    vol_cb_args.args.get_storage_size.storage_size = &storage_size;

    /* Get the storage size */
    if (H5VL_dataset_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, 0, "unable to get storage size");

    /* Set return value */
    ret_value = storage_size;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Dget_storage_size() */

/*-------------------------------------------------------------------------
 * Function:    H5Dget_offset
 *
 * Purpose:     Returns the address of dataset in file.
 *
 * Return:      Success:    The address of dataset
 *
 *              Failure:    HADDR_UNDEF (can also be a valid return value!)
 *
 *-------------------------------------------------------------------------
 */
haddr_t
H5Dget_offset(hid_t dset_id)
{
    H5VL_object_t                      *vol_obj;                   /* Dataset for this operation   */
    H5VL_optional_args_t                vol_cb_args;               /* Arguments to VOL callback */
    H5VL_native_dataset_optional_args_t dset_opt_args;             /* Arguments for optional operation */
    haddr_t                             dset_offset = HADDR_UNDEF; /* Dataset's offset */
    haddr_t                             ret_value   = HADDR_UNDEF; /* Return value                 */

    FUNC_ENTER_API(HADDR_UNDEF)
    H5TRACE1("a", "i", dset_id);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(dset_id, H5I_DATASET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, HADDR_UNDEF, "invalid dataset identifier");

    /* Set up VOL callback arguments */
    dset_opt_args.get_offset.offset = &dset_offset;
    vol_cb_args.op_type             = H5VL_NATIVE_DATASET_GET_OFFSET;
    vol_cb_args.args                = &dset_opt_args;

    /* Get the offset */
    if (H5VL_dataset_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, HADDR_UNDEF, "unable to get offset");

    /* Set return value */
    ret_value = dset_offset;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Dget_offset() */

/*-------------------------------------------------------------------------
 * Function:    H5D__read_api_common
 *
 * Purpose:     Common helper routine for sync/async dataset read operations.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__read_api_common(size_t count, hid_t dset_id[], hid_t mem_type_id[], hid_t mem_space_id[],
                     hid_t file_space_id[], hid_t dxpl_id, void *buf[], void **token_ptr,
                     H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    void   *obj_local;                                /* Local buffer for obj */
    void  **obj = &obj_local;                         /* Array of object pointers */
    H5VL_t *connector;                                /* VOL connector pointer */
    size_t  i;                                        /* Local index variable */
    herr_t  ret_value = SUCCEED;                      /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (count == 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "count must be greater than 0");
    if (!dset_id)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "dset_id array not provided");
    if (!mem_type_id)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "mem_type_id array not provided");
    if (!mem_space_id)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "mem_space_id array not provided");
    if (!file_space_id)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file_space_id array not provided");
    if (!buf)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "buf array not provided");

    /* Allocate obj array if necessary */
    if (count > 1)
        if (NULL == (obj = (void **)H5MM_malloc(count * sizeof(void *))))
            HGOTO_ERROR(H5E_VOL, H5E_CANTALLOC, FAIL, "can't allocate space for object array");

    /* Get vol_obj_ptr (return just the first dataset to caller if requested) */
    if (NULL == (*vol_obj_ptr = (H5VL_object_t *)H5I_object_verify(dset_id[0], H5I_DATASET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "dset_id is not a dataset ID");

    /* Save the connector of the first dataset.  Unpack the connector and call
     * the "direct" read function here to avoid allocating an array of count
     * H5VL_object_ts. */
    connector = (*vol_obj_ptr)->connector;

    /* Build obj array */
    obj[0] = (*vol_obj_ptr)->data;
    for (i = 1; i < count; i++) {
        /* Get the object */
        if (NULL == (tmp_vol_obj = (H5VL_object_t *)H5I_object_verify(dset_id[i], H5I_DATASET)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "dset_id is not a dataset ID");
        obj[i] = tmp_vol_obj->data;

        /* Make sure the class matches */
        if (tmp_vol_obj->connector->cls->value != connector->cls->value)
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL,
                        "datasets are accessed through different VOL connectors and can't be used in the "
                        "same I/O call");
    }

    /* Get the default dataset transfer property list if the user didn't provide one */
    if (H5P_DEFAULT == dxpl_id)
        dxpl_id = H5P_DATASET_XFER_DEFAULT;
    else if (true != H5P_isa_class(dxpl_id, H5P_DATASET_XFER))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not xfer parms");

    /* Read the data */
    if (H5VL_dataset_read_direct(count, obj, connector, mem_type_id, mem_space_id, file_space_id, dxpl_id,
                                 buf, token_ptr) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "can't read data");

done:
    /* Free memory */
    if (obj != &obj_local)
        H5MM_free(obj);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__read_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Dread
 *
 * Purpose:     Reads (part of) a DSET from the file into application
 *              memory BUF. The part of the dataset to read is defined with
 *              MEM_SPACE_ID and FILE_SPACE_ID. The data points are
 *              converted from their file type to the MEM_TYPE_ID specified.
 *              Additional miscellaneous data transfer properties can be
 *              passed to this function with the PLIST_ID argument.
 *
 *              The FILE_SPACE_ID can be the constant H5S_ALL which indicates
 *              that the entire file dataspace is to be referenced.
 *
 *              The MEM_SPACE_ID can be the constant H5S_ALL in which case
 *              the memory dataspace is the same as the file dataspace
 *              defined when the dataset was created.  The MEM_SPACE_ID can
 *              also be the constant H5S_BLOCK, which indicates that the
 *              buffer provided is a single contiguous block of memory, with
 *              the same # of elements as specified in the FILE_SPACE_ID
 *              selection.
 *
 *              The number of elements in the memory dataspace must match
 *              the number of elements in the file dataspace.
 *
 *              The PLIST_ID can be the constant H5P_DEFAULT in which
 *              case the default data transfer properties are used.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dread(hid_t dset_id, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t dxpl_id,
        void *buf /*out*/)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "iiiiix", dset_id, mem_type_id, mem_space_id, file_space_id, dxpl_id, buf);

    /* Read the data */
    if (H5D__read_api_common(1, &dset_id, &mem_type_id, &mem_space_id, &file_space_id, dxpl_id, &buf, NULL,
                             NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "can't synchronously read data");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Dread() */

/*-------------------------------------------------------------------------
 * Function:    H5Dread_async
 *
 * Purpose:     Asynchronously read dataset elements.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dread_async(const char *app_file, const char *app_func, unsigned app_line, hid_t dset_id, hid_t mem_type_id,
              hid_t mem_space_id, hid_t file_space_id, hid_t dxpl_id, void *buf /*out*/, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Dataset VOL object */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE10("e", "*s*sIuiiiiixi", app_file, app_func, app_line, dset_id, mem_type_id, mem_space_id,
              file_space_id, dxpl_id, buf, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Read the data */
    if (H5D__read_api_common(1, &dset_id, &mem_type_id, &mem_space_id, &file_space_id, dxpl_id, &buf,
                             token_ptr, &vol_obj) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "can't asynchronously read data");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE10(__func__, "*s*sIuiiiiixi", app_file, app_func, app_line, dset_id, mem_type_id, mem_space_id, file_space_id, dxpl_id, buf, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Dread_async() */

/*-------------------------------------------------------------------------
 * Function:	H5Dread_multi
 *
 * Purpose:	Multi-version of H5Dread(), which reads selections from
 *              multiple datasets from a file into application memory BUFS.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dread_multi(size_t count, hid_t dset_id[], hid_t mem_type_id[], hid_t mem_space_id[], hid_t file_space_id[],
              hid_t dxpl_id, void *buf[] /*out*/)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE7("e", "z*i*i*i*iix", count, dset_id, mem_type_id, mem_space_id, file_space_id, dxpl_id, buf);

    if (count == 0)
        HGOTO_DONE(SUCCEED);

    /* Read the data */
    if (H5D__read_api_common(count, dset_id, mem_type_id, mem_space_id, file_space_id, dxpl_id, buf, NULL,
                             NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "can't synchronously read data");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Dread_multi() */

/*-------------------------------------------------------------------------
 * Function:    H5Dread_multi_async
 *
 * Purpose:     Asynchronously read dataset elements from multiple
 *              datasets.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dread_multi_async(const char *app_file, const char *app_func, unsigned app_line, size_t count,
                    hid_t dset_id[], hid_t mem_type_id[], hid_t mem_space_id[], hid_t file_space_id[],
                    hid_t dxpl_id, void *buf[] /*out*/, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Dataset VOL object */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE11("e", "*s*sIuz*i*i*i*iixi", app_file, app_func, app_line, count, dset_id, mem_type_id,
              mem_space_id, file_space_id, dxpl_id, buf, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Read the data */
    if (H5D__read_api_common(count, dset_id, mem_type_id, mem_space_id, file_space_id, dxpl_id, buf,
                             token_ptr, &vol_obj) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "can't asynchronously read data");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE11(__func__, "*s*sIuz*i*i*i*iixi", app_file, app_func, app_line, count, dset_id, mem_type_id, mem_space_id, file_space_id, dxpl_id, buf, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Dread_multi_async() */

/*-------------------------------------------------------------------------
 * Function:    H5Dread_chunk
 *
 * Purpose:     Reads an entire chunk from the file directly.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5Dread_chunk(hid_t dset_id, hid_t dxpl_id, const hsize_t *offset, uint32_t *filters, void *buf /*out*/)
{
    H5VL_object_t                      *vol_obj;             /* Dataset for this operation   */
    H5VL_optional_args_t                vol_cb_args;         /* Arguments to VOL callback */
    H5VL_native_dataset_optional_args_t dset_opt_args;       /* Arguments for optional operation */
    herr_t                              ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "ii*h*Iux", dset_id, dxpl_id, offset, filters, buf);

    /* Check arguments */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(dset_id, H5I_DATASET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "dset_id is not a dataset ID");
    if (!buf)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "buf cannot be NULL");
    if (!offset)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "offset cannot be NULL");
    if (!filters)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "filters cannot be NULL");

    /* Get the default dataset transfer property list if the user didn't provide one */
    if (H5P_DEFAULT == dxpl_id)
        dxpl_id = H5P_DATASET_XFER_DEFAULT;
    else if (true != H5P_isa_class(dxpl_id, H5P_DATASET_XFER))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "dxpl_id is not a dataset transfer property list ID");

    /* Set up VOL callback arguments */
    dset_opt_args.chunk_read.offset  = offset;
    dset_opt_args.chunk_read.filters = 0;
    dset_opt_args.chunk_read.buf     = buf;
    vol_cb_args.op_type              = H5VL_NATIVE_DATASET_CHUNK_READ;
    vol_cb_args.args                 = &dset_opt_args;

    /* Read the raw chunk */
    if (H5VL_dataset_optional(vol_obj, &vol_cb_args, dxpl_id, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "can't read unprocessed chunk data");

    /* Set return value */
    *filters = dset_opt_args.chunk_read.filters;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Dread_chunk() */

/*-------------------------------------------------------------------------
 * Function:    H5D__write_api_common
 *
 * Purpose:     Common helper routine for sync/async dataset write operations.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__write_api_common(size_t count, hid_t dset_id[], hid_t mem_type_id[], hid_t mem_space_id[],
                      hid_t file_space_id[], hid_t dxpl_id, const void *buf[], void **token_ptr,
                      H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    void   *obj_local;                                /* Local buffer for obj */
    void  **obj = &obj_local;                         /* Array of object pointers */
    H5VL_t *connector;                                /* VOL connector pointer */
    size_t  i;                                        /* Local index variable */
    herr_t  ret_value = SUCCEED;                      /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (count == 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "count must be greater than 0");
    if (!dset_id)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "dset_id array not provided");
    if (!mem_type_id)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "mem_type_id array not provided");
    if (!mem_space_id)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "mem_space_id array not provided");
    if (!file_space_id)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file_space_id array not provided");
    if (!buf)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "buf array not provided");

    /* Allocate obj array if necessary */
    if (count > 1)
        if (NULL == (obj = (void **)H5MM_malloc(count * sizeof(void *))))
            HGOTO_ERROR(H5E_VOL, H5E_CANTALLOC, FAIL, "can't allocate space for object array");

    /* Get vol_obj_ptr (return just the first dataset to caller if requested) */
    if (NULL == (*vol_obj_ptr = (H5VL_object_t *)H5I_object_verify(dset_id[0], H5I_DATASET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "dset_id is not a dataset ID");

    /* Save the connector of the first dataset.  Unpack the connector and call
     * the "direct" write function here to avoid allocating an array of count
     * H5VL_object_ts. */
    connector = (*vol_obj_ptr)->connector;

    /* Build obj array */
    obj[0] = (*vol_obj_ptr)->data;
    for (i = 1; i < count; i++) {
        /* Get the object */
        if (NULL == (tmp_vol_obj = (H5VL_object_t *)H5I_object_verify(dset_id[i], H5I_DATASET)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "dset_id is not a dataset ID");
        obj[i] = tmp_vol_obj->data;

        /* Make sure the class matches */
        if (tmp_vol_obj->connector->cls->value != connector->cls->value)
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL,
                        "datasets are accessed through different VOL connectors and can't be used in the "
                        "same I/O call");
    }

    /* Get the default dataset transfer property list if the user didn't provide one */
    if (H5P_DEFAULT == dxpl_id)
        dxpl_id = H5P_DATASET_XFER_DEFAULT;
    else if (true != H5P_isa_class(dxpl_id, H5P_DATASET_XFER))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not xfer parms");

    /* Write the data */
    if (H5VL_dataset_write_direct(count, obj, connector, mem_type_id, mem_space_id, file_space_id, dxpl_id,
                                  buf, token_ptr) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "can't write data");

done:
    /* Free memory */
    if (obj != &obj_local)
        H5MM_free(obj);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__write_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Dwrite
 *
 * Purpose:     Writes (part of) a DSET from application memory BUF to the
 *              file. The part of the dataset to write is defined with the
 *              MEM_SPACE_ID and FILE_SPACE_ID arguments. The data points
 *              are converted from their current type (MEM_TYPE_ID) to their
 *              file datatype. Additional miscellaneous data transfer
 *              properties can be passed to this function with the
 *              PLIST_ID argument.
 *
 *              The FILE_SPACE_ID can be the constant H5S_ALL which indicates
 *              that the entire file dataspace is to be referenced.
 *
 *              The MEM_SPACE_ID can be the constant H5S_ALL in which case
 *              the memory dataspace is the same as the file dataspace
 *              defined when the dataset was created.  The MEM_SPACE_ID can
 *              also be the constant H5S_BLOCK, which indicates that the
 *              buffer provided is a single contiguous block of memory, with
 *              the same # of elements as specified in the FILE_SPACE_ID
 *              selection.
 *
 *              The number of elements in the memory dataspace must match
 *              the number of elements in the file dataspace.
 *
 *              The PLIST_ID can be the constant H5P_DEFAULT in which
 *              case the default data transfer properties are used.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dwrite(hid_t dset_id, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t dxpl_id,
         const void *buf)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "iiiii*x", dset_id, mem_type_id, mem_space_id, file_space_id, dxpl_id, buf);

    /* Write the data */
    if (H5D__write_api_common(1, &dset_id, &mem_type_id, &mem_space_id, &file_space_id, dxpl_id, &buf, NULL,
                              NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "can't synchronously write data");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Dwrite() */

/*-------------------------------------------------------------------------
 * Function:    H5Dwrite_async
 *
 * Purpose:     For asynchronous VOL with request token
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dwrite_async(const char *app_file, const char *app_func, unsigned app_line, hid_t dset_id,
               hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t dxpl_id, const void *buf,
               hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Dataset VOL object */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE10("e", "*s*sIuiiiii*xi", app_file, app_func, app_line, dset_id, mem_type_id, mem_space_id,
              file_space_id, dxpl_id, buf, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Write the data */
    if (H5D__write_api_common(1, &dset_id, &mem_type_id, &mem_space_id, &file_space_id, dxpl_id, &buf,
                              token_ptr, &vol_obj) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "can't asynchronously write data");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE10(__func__, "*s*sIuiiiii*xi", app_file, app_func, app_line, dset_id, mem_type_id, mem_space_id, file_space_id, dxpl_id, buf, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Dwrite_async() */

/*-------------------------------------------------------------------------
 * Function:	H5Dwrite_multi
 *
 * Purpose:	Multi-version of H5Dwrite(), which writes selections from
 *              application memory BUFs into multiple datasets in a file.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dwrite_multi(size_t count, hid_t dset_id[], hid_t mem_type_id[], hid_t mem_space_id[],
               hid_t file_space_id[], hid_t dxpl_id, const void *buf[])
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE7("e", "z*i*i*i*ii**x", count, dset_id, mem_type_id, mem_space_id, file_space_id, dxpl_id, buf);

    if (count == 0)
        HGOTO_DONE(SUCCEED);

    /* Write the data */
    if (H5D__write_api_common(count, dset_id, mem_type_id, mem_space_id, file_space_id, dxpl_id, buf, NULL,
                              NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "can't synchronously write data");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Dwrite_multi() */

/*-------------------------------------------------------------------------
 * Function:    H5Dwrite_multi_async
 *
 * Purpose:     Asynchronously write dataset elements to multiple
 *              datasets.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dwrite_multi_async(const char *app_file, const char *app_func, unsigned app_line, size_t count,
                     hid_t dset_id[], hid_t mem_type_id[], hid_t mem_space_id[], hid_t file_space_id[],
                     hid_t dxpl_id, const void *buf[], hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Dataset VOL object */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE11("e", "*s*sIuz*i*i*i*ii**xi", app_file, app_func, app_line, count, dset_id, mem_type_id,
              mem_space_id, file_space_id, dxpl_id, buf, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Write the data */
    if (H5D__write_api_common(count, dset_id, mem_type_id, mem_space_id, file_space_id, dxpl_id, buf,
                              token_ptr, &vol_obj) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "can't asynchronously write data");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE11(__func__, "*s*sIuz*i*i*i*ii**xi", app_file, app_func, app_line, count, dset_id, mem_type_id, mem_space_id, file_space_id, dxpl_id, buf, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Dwrite_multi_async() */

/*-------------------------------------------------------------------------
 * Function:    H5Dwrite_chunk
 *
 * Purpose:     Writes an entire chunk to the file directly.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dwrite_chunk(hid_t dset_id, hid_t dxpl_id, uint32_t filters, const hsize_t *offset, size_t data_size,
               const void *buf)
{
    H5VL_object_t                      *vol_obj;       /* Dataset for this operation   */
    H5VL_optional_args_t                vol_cb_args;   /* Arguments to VOL callback */
    H5VL_native_dataset_optional_args_t dset_opt_args; /* Arguments for optional operation */
    uint32_t                            data_size_32;  /* Chunk data size (limited to 32-bits currently) */
    herr_t                              ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "iiIu*hz*x", dset_id, dxpl_id, filters, offset, data_size, buf);

    /* Check arguments */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(dset_id, H5I_DATASET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid dataset ID");
    if (!buf)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "buf cannot be NULL");
    if (!offset)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "offset cannot be NULL");
    if (0 == data_size)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "data_size cannot be zero");

    /* Make sure data size is less than 4 GiB */
    data_size_32 = (uint32_t)data_size;
    if (data_size != (size_t)data_size_32)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid data_size - chunks cannot be > 4 GiB");

    /* Get the default dataset transfer property list if the user didn't provide one */
    if (H5P_DEFAULT == dxpl_id)
        dxpl_id = H5P_DATASET_XFER_DEFAULT;
    else if (true != H5P_isa_class(dxpl_id, H5P_DATASET_XFER))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "dxpl_id is not a dataset transfer property list ID");

    /* Set up VOL callback arguments */
    dset_opt_args.chunk_write.offset  = offset;
    dset_opt_args.chunk_write.filters = filters;
    dset_opt_args.chunk_write.size    = data_size_32;
    dset_opt_args.chunk_write.buf     = buf;
    vol_cb_args.op_type               = H5VL_NATIVE_DATASET_CHUNK_WRITE;
    vol_cb_args.args                  = &dset_opt_args;

    /* Write chunk */
    if (H5VL_dataset_optional(vol_obj, &vol_cb_args, dxpl_id, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "can't write unprocessed chunk data");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Dwrite_chunk() */

/*-------------------------------------------------------------------------
 * Function:    H5Dscatter
 *
 * Purpose:     Scatters data provided by the callback op to the
 *              destination buffer dst_buf, where the dimensions of
 *              dst_buf and the selection to be scattered to are specified
 *              by the dataspace dst_space_id.  The type of the data to be
 *              scattered is specified by type_id.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dscatter(H5D_scatter_func_t op, void *op_data, hid_t type_id, hid_t dst_space_id, void *dst_buf /*out*/)
{
    H5T_t          *type;                     /* Datatype */
    H5S_t          *dst_space;                /* Dataspace */
    H5S_sel_iter_t *iter           = NULL;    /* Selection iteration info*/
    bool            iter_init      = false;   /* Selection iteration info has been initialized */
    const void     *src_buf        = NULL;    /* Source (contiguous) data buffer */
    size_t          src_buf_nbytes = 0;       /* Size of src_buf */
    size_t          type_size;                /* Datatype element size */
    hssize_t        nelmts;                   /* Number of remaining elements in selection */
    size_t          nelmts_scatter = 0;       /* Number of elements to scatter to dst_buf */
    herr_t          ret_value      = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "DS*xiix", op, op_data, type_id, dst_space_id, dst_buf);

    /* Check args */
    if (op == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid callback function pointer");
    if (NULL == (type = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    if (NULL == (dst_space = (H5S_t *)H5I_object_verify(dst_space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataspace");
    if (dst_buf == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no destination buffer provided");

    /* Get datatype element size */
    if (0 == (type_size = H5T_GET_SIZE(type)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get datatype size");

    /* Get number of elements in dataspace */
    if ((nelmts = (hssize_t)H5S_GET_SELECT_NPOINTS(dst_space)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCOUNT, FAIL, "unable to get number of elements in selection");

    /* Allocate the selection iterator */
    if (NULL == (iter = H5FL_MALLOC(H5S_sel_iter_t)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate selection iterator");

    /* Initialize selection iterator */
    if (H5S_select_iter_init(iter, dst_space, type_size, 0) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to initialize selection iterator information");
    iter_init = true;

    /* Loop until all data has been scattered */
    while (nelmts > 0) {
        /* Make callback to retrieve data */
        if (op(&src_buf, &src_buf_nbytes, op_data) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CALLBACK, FAIL, "callback operator returned failure");

        /* Calculate number of elements */
        nelmts_scatter = src_buf_nbytes / type_size;

        /* Check callback results */
        if (!src_buf)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "callback did not return a buffer");
        if (src_buf_nbytes == 0)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "callback returned a buffer size of 0");
        if (src_buf_nbytes % type_size)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "buffer size is not a multiple of datatype size");
        if (nelmts_scatter > (size_t)nelmts)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "callback returned more elements than in selection");

        /* Scatter data */
        if (H5D__scatter_mem(src_buf, iter, nelmts_scatter, dst_buf) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "scatter failed");

        nelmts -= (hssize_t)nelmts_scatter;
    } /* end while */

done:
    /* Release selection iterator */
    if (iter_init && H5S_SELECT_ITER_RELEASE(iter) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "can't release selection iterator");
    if (iter)
        iter = H5FL_FREE(H5S_sel_iter_t, iter);

    FUNC_LEAVE_API(ret_value)
} /* H5Dscatter() */

/*-------------------------------------------------------------------------
 * Function:    H5Dgather
 *
 * Purpose:     Gathers data provided from the source buffer src_buf to
 *              contiguous buffer dst_buf, then calls the callback op.
 *              The dimensions of src_buf and the selection to be gathered
 *              are specified by the dataspace src_space_id.  The type of
 *              the data to be gathered is specified by type_id.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dgather(hid_t src_space_id, const void *src_buf, hid_t type_id, size_t dst_buf_size, void *dst_buf /*out*/,
          H5D_gather_func_t op, void *op_data)
{
    H5T_t          *type;                /* Datatype */
    H5S_t          *src_space;           /* Dataspace */
    H5S_sel_iter_t *iter      = NULL;    /* Selection iteration info*/
    bool            iter_init = false;   /* Selection iteration info has been initialized */
    size_t          type_size;           /* Datatype element size */
    hssize_t        nelmts;              /* Number of remaining elements in selection */
    size_t          dst_buf_nelmts;      /* Number of elements that can fit in dst_buf */
    size_t          nelmts_gathered;     /* Number of elements gathered from src_buf */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE7("e", "i*xizxDg*x", src_space_id, src_buf, type_id, dst_buf_size, dst_buf, op, op_data);

    /* Check args */
    if (NULL == (src_space = (H5S_t *)H5I_object_verify(src_space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataspace");
    if (src_buf == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no source buffer provided");
    if (NULL == (type = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    if (dst_buf_size == 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "destination buffer size is 0");
    if (dst_buf == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no destination buffer provided");

    /* Get datatype element size */
    if (0 == (type_size = H5T_GET_SIZE(type)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get datatype size");

    /* Get number of elements in dst_buf_size */
    dst_buf_nelmts = dst_buf_size / type_size;
    if (dst_buf_nelmts == 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                    "destination buffer is not large enough to hold one element");

    /* Get number of elements in dataspace */
    if ((nelmts = (hssize_t)H5S_GET_SELECT_NPOINTS(src_space)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTCOUNT, FAIL, "unable to get number of elements in selection");

    /* If dst_buf is not large enough to hold all the elements, make sure there
     * is a callback */
    if (((size_t)nelmts > dst_buf_nelmts) && (op == NULL))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no callback supplied and destination buffer too small");

    /* Allocate the selection iterator */
    if (NULL == (iter = H5FL_MALLOC(H5S_sel_iter_t)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate selection iterator");

    /* Initialize selection iterator */
    if (H5S_select_iter_init(iter, src_space, type_size, 0) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to initialize selection iterator information");
    iter_init = true;

    /* Loop until all data has been scattered */
    while (nelmts > 0) {
        /* Gather data */
        if (0 ==
            (nelmts_gathered = H5D__gather_mem(src_buf, iter, MIN(dst_buf_nelmts, (size_t)nelmts), dst_buf)))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "gather failed");
        assert(nelmts_gathered == MIN(dst_buf_nelmts, (size_t)nelmts));

        /* Make callback to process dst_buf */
        if (op && op(dst_buf, nelmts_gathered * type_size, op_data) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CALLBACK, FAIL, "callback operator returned failure");

        nelmts -= (hssize_t)nelmts_gathered;
        assert(op || (nelmts == 0));
    } /* end while */

done:
    /* Release selection iterator */
    if (iter_init && H5S_SELECT_ITER_RELEASE(iter) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "can't release selection iterator");
    if (iter)
        iter = H5FL_FREE(H5S_sel_iter_t, iter);

    FUNC_LEAVE_API(ret_value)
} /* H5Dgather() */

/*--------------------------------------------------------------------------
 NAME
    H5Dfill
 PURPOSE
    Fill a selection in memory with a value
 USAGE
    herr_t H5Dfill(fill, fill_type, space, buf, buf_type)
        const void *fill;       IN: Pointer to fill value to use
        hid_t fill_type_id;     IN: Datatype of the fill value
        void *buf;              IN/OUT: Memory buffer to fill selection within
        hid_t buf_type_id;      IN: Datatype of the elements in buffer
        hid_t space_id;         IN: Dataspace describing memory buffer &
                                    containing selection to use.
 RETURNS
    Non-negative on success/Negative on failure.
 DESCRIPTION
    Use the selection in the dataspace to fill elements in a memory buffer.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    If "fill" parameter is NULL, use all zeros as fill value
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5Dfill(const void *fill, hid_t fill_type_id, void *buf, hid_t buf_type_id, hid_t space_id)
{
    H5S_t *space;               /* Dataspace */
    H5T_t *fill_type;           /* Fill-value datatype */
    H5T_t *buf_type;            /* Buffer datatype */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "*xi*xii", fill, fill_type_id, buf, buf_type_id, space_id);

    /* Check args */
    if (buf == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid buffer");
    if (NULL == (space = (H5S_t *)H5I_object_verify(space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, 0, "not a dataspace");
    if (NULL == (fill_type = (H5T_t *)H5I_object_verify(fill_type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, 0, "not a datatype");
    if (NULL == (buf_type = (H5T_t *)H5I_object_verify(buf_type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, 0, "not a datatype");

    /* Fill the selection in the memory buffer */
    if (H5D__fill(fill, fill_type, buf, buf_type, space) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTENCODE, FAIL, "filling selection failed");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Dfill() */

/*-------------------------------------------------------------------------
 * Function:    H5Diterate
 *
 * Purpose: This routine iterates over all the elements selected in a memory
 *      buffer.  The callback function is called once for each element selected
 *      in the dataspace.  The selection in the dataspace is modified so
 *      that any elements already iterated over are removed from the selection
 *      if the iteration is interrupted (by the H5D_operator_t function
 *      returning non-zero) in the "middle" of the iteration and may be
 *      re-started by the user where it left off.
 *
 *      NOTE: Until "subtracting" elements from a selection is implemented,
 *          the selection is not modified.
 *
 * Parameters:
 *      void *buf;          IN/OUT: Pointer to the buffer in memory containing
 *                              the elements to iterate over.
 *      hid_t type_id;      IN: Datatype ID for the elements stored in BUF.
 *      hid_t space_id;     IN: Dataspace ID for BUF, also contains the
 *                              selection to iterate over.
 *      H5D_operator_t op; IN: Function pointer to the routine to be
 *                              called for each element in BUF iterated over.
 *      void *operator_data;    IN/OUT: Pointer to any user-defined data
 *                              associated with the operation.
 *
 * Operation information:
 *      H5D_operator_t is defined as:
 *          typedef herr_t (*H5D_operator_t)(void *elem, hid_t type_id,
 *              unsigned ndim, const hsize_t *point, void *operator_data);
 *
 *      H5D_operator_t parameters:
 *          void *elem;         IN/OUT: Pointer to the element in memory containing
 *                                  the current point.
 *          hid_t type_id;      IN: Datatype ID for the elements stored in ELEM.
 *          unsigned ndim;       IN: Number of dimensions for POINT array
 *          const hsize_t *point; IN: Array containing the location of the element
 *                                  within the original dataspace.
 *          void *operator_data;    IN/OUT: Pointer to any user-defined data
 *                                  associated with the operation.
 *
 *      The return values from an operator are:
 *          Zero causes the iterator to continue, returning zero when all
 *              elements have been processed.
 *          Positive causes the iterator to immediately return that positive
 *              value, indicating short-circuit success.  The iterator can be
 *              restarted at the next element.
 *          Negative causes the iterator to immediately return that value,
 *              indicating failure. The iterator can be restarted at the next
 *              element.
 *
 * Return:  Returns the return value of the last operator if it was non-zero,
 *          or zero if all elements were processed. Otherwise returns a
 *          negative value.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Diterate(void *buf, hid_t type_id, hid_t space_id, H5D_operator_t op, void *operator_data)
{
    H5T_t            *type;      /* Datatype */
    H5S_t            *space;     /* Dataspace for iteration */
    H5S_sel_iter_op_t dset_op;   /* Operator for iteration */
    herr_t            ret_value; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "*xiiDO*x", buf, type_id, space_id, op, operator_data);

    /* Check args */
    if (NULL == op)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid operator");
    if (NULL == buf)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid buffer");
    if (H5I_DATATYPE != H5I_get_type(type_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid datatype");
    if (NULL == (type = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not an valid base datatype");
    if (NULL == (space = (H5S_t *)H5I_object_verify(space_id, H5I_DATASPACE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid dataspace");
    if (!(H5S_has_extent(space)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "dataspace does not have extent set");

    dset_op.op_type          = H5S_SEL_ITER_OP_APP;
    dset_op.u.app_op.op      = op;
    dset_op.u.app_op.type_id = type_id;

    ret_value = H5S_select_iterate(buf, type, space, &dset_op, operator_data);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Diterate() */

/*-------------------------------------------------------------------------
 * Function:    H5Dvlen_get_buf_size
 *
 * Purpose: This routine checks the number of bytes required to store the VL
 *      data from the dataset, using the space_id for the selection in the
 *      dataset on disk and the type_id for the memory representation of the
 *      VL data, in memory.  The *size value is modified according to how many
 *      bytes are required to store the VL data in memory.
 *
 * Return:  Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dvlen_get_buf_size(hid_t dataset_id, hid_t type_id, hid_t space_id, hsize_t *size /*out*/)
{
    H5VL_object_t *vol_obj;   /* Dataset for this operation */
    uint64_t       supported; /* Whether 'get vlen buf size' operation is supported by VOL connector */
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "iiix", dataset_id, type_id, space_id, size);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(dataset_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid dataset identifier");
    if (H5I_DATATYPE != H5I_get_type(type_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid datatype identifier");
    if (H5I_DATASPACE != H5I_get_type(space_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid dataspace identifier");
    if (size == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid 'size' pointer");

    /* Check if the 'get_vlen_buf_size' callback is supported */
    supported = 0;
    if (H5VL_introspect_opt_query(vol_obj, H5VL_SUBCLS_DATASET, H5VL_NATIVE_DATASET_GET_VLEN_BUF_SIZE,
                                  &supported) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't check for 'get vlen buf size' operation");
    if (supported & H5VL_OPT_QUERY_SUPPORTED) {
        H5VL_optional_args_t                vol_cb_args;   /* Arguments to VOL callback */
        H5VL_native_dataset_optional_args_t dset_opt_args; /* Arguments for optional operation */

        /* Set up VOL callback arguments */
        dset_opt_args.get_vlen_buf_size.type_id  = type_id;
        dset_opt_args.get_vlen_buf_size.space_id = space_id;
        dset_opt_args.get_vlen_buf_size.size     = size;
        vol_cb_args.op_type                      = H5VL_NATIVE_DATASET_GET_VLEN_BUF_SIZE;
        vol_cb_args.args                         = &dset_opt_args;

        /* Make the 'get_vlen_buf_size' callback */
        if (H5VL_dataset_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "unable to get vlen buf size");
    } /* end if */
    else {
        /* Perform a generic operation that will work with all VOL connectors */
        if (H5D__vlen_get_buf_size_gen(vol_obj, type_id, space_id, size) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "unable to get vlen buf size");
    } /* end else */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Dvlen_get_buf_size() */

/*-------------------------------------------------------------------------
 * Function:    H5D__set_extent_api_common
 *
 * Purpose:     This is the common function for changing a dataset's dimensions
 *
 * Return:	    Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__set_extent_api_common(hid_t dset_id, const hsize_t size[], void **token_ptr,
                           H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_dataset_specific_args_t vol_cb_args;         /* Arguments to VOL callback */
    herr_t                       ret_value = SUCCEED; /* Return value                 */

    FUNC_ENTER_PACKAGE

    /* Check args */
    if (NULL == (*vol_obj_ptr = (H5VL_object_t *)H5I_object_verify(dset_id, H5I_DATASET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid dataset identifier");
    if (!size)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "size array cannot be NULL");

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(dset_id) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set collective metadata read info");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type              = H5VL_DATASET_SET_EXTENT;
    vol_cb_args.args.set_extent.size = size;

    /* Set the extent */
    if (H5VL_dataset_specific(*vol_obj_ptr, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, token_ptr) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "unable to set dataset extent");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__set_extent_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Dset_extent
 *
 * Purpose:     Modifies the dimensions of a dataset.
 *              Can change to a smaller dimension.
 *
 * Return:	    Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dset_extent(hid_t dset_id, const hsize_t size[])
{
    herr_t ret_value = SUCCEED; /* Return value                 */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "i*h", dset_id, size);

    /* Change a datset's dimensions synchronously */
    if ((ret_value = H5D__set_extent_api_common(dset_id, size, NULL, NULL)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "unable to synchronously change a dataset's dimensions");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Dset_extent() */

/*-------------------------------------------------------------------------
 * Function:    H5Dset_extent_async
 *
 * Purpose:     Asynchronous version of H5Dset_extent
 *
 * Return:	    Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dset_extent_async(const char *app_file, const char *app_func, unsigned app_line, hid_t dset_id,
                    const hsize_t size[], hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "*s*sIui*hi", app_file, app_func, app_line, dset_id, size, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Change a datset's dimensions asynchronously */
    if (H5D__set_extent_api_common(dset_id, size, token_ptr, &vol_obj) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "unable to asynchronously change a dataset's dimensions");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                H5ARG_TRACE6(__func__, "*s*sIui*hi", app_file, app_func, app_line, dset_id, size, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Dset_extent_async() */

/*-------------------------------------------------------------------------
 * Function:    H5Dflush
 *
 * Purpose:     Flushes all buffers associated with a dataset.
 *
 * Return:      Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dflush(hid_t dset_id)
{
    H5VL_object_t               *vol_obj;             /* Object for loc_id */
    H5VL_dataset_specific_args_t vol_cb_args;         /* Arguments to VOL callback */
    herr_t                       ret_value = SUCCEED; /* Return value                 */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", dset_id);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(dset_id, H5I_DATASET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "dset_id parameter is not a valid dataset identifier");

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(dset_id) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set collective metadata read info");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type            = H5VL_DATASET_FLUSH;
    vol_cb_args.args.flush.dset_id = dset_id;

    /* Flush dataset information cached in memory
     * XXX: Note that we need to pass the ID to the VOL since the H5F_flush_cb_t
     *      callback needs it and that's in the public API.
     */
    if (H5VL_dataset_specific(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTFLUSH, FAIL, "unable to flush dataset");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Dflush */

/*-------------------------------------------------------------------------
 * Function:    H5Drefresh
 *
 * Purpose:     Refreshes all buffers associated with a dataset.
 *
 * Return:      Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Drefresh(hid_t dset_id)
{
    H5VL_object_t               *vol_obj;             /* Object for loc_id */
    H5VL_dataset_specific_args_t vol_cb_args;         /* Arguments to VOL callback */
    herr_t                       ret_value = SUCCEED; /* Return value                 */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", dset_id);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(dset_id, H5I_DATASET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "dset_id parameter is not a valid dataset identifier");

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(dset_id) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set collective metadata read info");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type              = H5VL_DATASET_REFRESH;
    vol_cb_args.args.refresh.dset_id = dset_id;

    /* Refresh the dataset object */
    if (H5VL_dataset_specific(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTLOAD, FAIL, "unable to refresh dataset");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Drefresh() */

/*-------------------------------------------------------------------------
 * Function:    H5Dformat_convert (Internal)
 *
 * Purpose:     For chunked:
 *                  Convert the chunk indexing type to version 1 B-tree if not
 *              For compact/contiguous:
 *                  Downgrade layout version to 3 if greater than 3
 *              For virtual:
 *                  No conversion
 *
 * Return:      Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dformat_convert(hid_t dset_id)
{
    H5VL_object_t       *vol_obj;             /* Dataset for this operation   */
    H5VL_optional_args_t vol_cb_args;         /* Arguments to VOL callback */
    herr_t               ret_value = SUCCEED; /* Return value                 */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", dset_id);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(dset_id, H5I_DATASET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "dset_id parameter is not a valid dataset identifier");

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(dset_id) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set collective metadata read info");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type = H5VL_NATIVE_DATASET_FORMAT_CONVERT;
    vol_cb_args.args    = NULL;

    /* Convert the dataset */
    if (H5VL_dataset_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_INTERNAL, FAIL, "can't convert dataset format");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Dformat_convert */

/*-------------------------------------------------------------------------
 * Function:    H5Dget_chunk_index_type (Internal)
 *
 * Purpose:     Retrieve a dataset's chunk indexing type
 *
 * Return:      Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dget_chunk_index_type(hid_t dset_id, H5D_chunk_index_t *idx_type /*out*/)
{
    H5VL_object_t                      *vol_obj;             /* Dataset for this operation   */
    H5VL_optional_args_t                vol_cb_args;         /* Arguments to VOL callback */
    H5VL_native_dataset_optional_args_t dset_opt_args;       /* Arguments for optional operation */
    herr_t                              ret_value = SUCCEED; /* Return value                 */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", dset_id, idx_type);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(dset_id, H5I_DATASET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "dset_id parameter is not a valid dataset identifier");
    if (NULL == idx_type)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "idx_type parameter cannot be NULL");

    /* Set up VOL callback arguments */
    dset_opt_args.get_chunk_idx_type.idx_type = idx_type;
    vol_cb_args.op_type                       = H5VL_NATIVE_DATASET_GET_CHUNK_INDEX_TYPE;
    vol_cb_args.args                          = &dset_opt_args;

    /* Get the chunk indexing type */
    if (H5VL_dataset_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get chunk index type");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Dget_chunk_index_type() */

/*-------------------------------------------------------------------------
 * Function:    H5Dget_chunk_storage_size
 *
 * Purpose:     Returns the size of an allocated chunk.
 *
 *              Intended for use with the H5D(O)read_chunk API call so
 *              the caller can construct an appropriate buffer.
 *
 * Return:	Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dget_chunk_storage_size(hid_t dset_id, const hsize_t *offset, hsize_t *chunk_nbytes /*out*/)
{
    H5VL_object_t                      *vol_obj;             /* Dataset for this operation   */
    H5VL_optional_args_t                vol_cb_args;         /* Arguments to VOL callback */
    H5VL_native_dataset_optional_args_t dset_opt_args;       /* Arguments for optional operation */
    herr_t                              ret_value = SUCCEED; /* Return value                 */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "i*hx", dset_id, offset, chunk_nbytes);

    /* Check arguments */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(dset_id, H5I_DATASET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "dset_id parameter is not a valid dataset identifier");
    if (NULL == offset)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "offset parameter cannot be NULL");
    if (NULL == chunk_nbytes)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "chunk_nbytes parameter cannot be NULL");

    /* Set up VOL callback arguments */
    dset_opt_args.get_chunk_storage_size.offset = offset;
    dset_opt_args.get_chunk_storage_size.size   = chunk_nbytes;
    vol_cb_args.op_type                         = H5VL_NATIVE_DATASET_GET_CHUNK_STORAGE_SIZE;
    vol_cb_args.args                            = &dset_opt_args;

    /* Get the dataset creation property list */
    if (H5VL_dataset_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get storage size of chunk");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Dget_chunk_storage_size() */

/*-------------------------------------------------------------------------
 * Function:    H5Dget_num_chunks
 *
 * Purpose:     Retrieves the number of chunks that have non-empty intersection
 *              with a specified selection.
 *
 * Note:        Currently, this function only gets the number of all written
 *              chunks, regardless the dataspace.
 *
 * Parameters:
 *              hid_t dset_id;      IN: Chunked dataset ID
 *              hid_t fspace_id;    IN: File dataspace ID
 *              hsize_t *nchunks;   OUT: Number of non-empty chunks
 *
 * Return:      Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dget_num_chunks(hid_t dset_id, hid_t fspace_id, hsize_t *nchunks /*out*/)
{
    H5VL_object_t                      *vol_obj = NULL; /* Dataset for this operation */
    H5VL_optional_args_t                vol_cb_args;    /* Arguments to VOL callback */
    H5VL_native_dataset_optional_args_t dset_opt_args;  /* Arguments for optional operation */
    herr_t                              ret_value = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "iix", dset_id, fspace_id, nchunks);

    /* Check arguments */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(dset_id, H5I_DATASET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid dataset identifier");
    if (NULL == nchunks)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid argument (null)");

    /* Set up VOL callback arguments */
    dset_opt_args.get_num_chunks.space_id = fspace_id;
    dset_opt_args.get_num_chunks.nchunks  = nchunks;
    vol_cb_args.op_type                   = H5VL_NATIVE_DATASET_GET_NUM_CHUNKS;
    vol_cb_args.args                      = &dset_opt_args;

    /* Get the number of written chunks */
    if (H5VL_dataset_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of chunks");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Dget_num_chunks() */

/*-------------------------------------------------------------------------
 * Function:    H5Dget_chunk_info
 *
 * Purpose:     Retrieves information about a chunk specified by its index.
 *
 * Parameters:
 *              hid_t dset_id;          IN: Chunked dataset ID
 *              hid_t fspace_id;        IN: File dataspace ID
 *              hsize_t index;          IN: Index of written chunk
 *              hsize_t *offset         OUT: Logical position of the chunk's
 *                                           first element in the dataspace
 *              unsigned *filter_mask   OUT: Mask for identifying the filters in use
 *              haddr_t *addr           OUT: Address of the chunk
 *              hsize_t *size           OUT: Size of the chunk
 *
 * Return:      Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dget_chunk_info(hid_t dset_id, hid_t fspace_id, hsize_t chk_index, hsize_t *offset /*out*/,
                  unsigned *filter_mask /*out*/, haddr_t *addr /*out*/, hsize_t *size /*out*/)
{
    H5VL_object_t                      *vol_obj = NULL; /* Dataset for this operation */
    H5VL_optional_args_t                vol_cb_args;    /* Arguments to VOL callback */
    H5VL_native_dataset_optional_args_t dset_opt_args;  /* Arguments for optional operation */
    hsize_t                             nchunks   = 0;  /* Number of chunks */
    herr_t                              ret_value = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE7("e", "iihxxxx", dset_id, fspace_id, chk_index, offset, filter_mask, addr, size);

    /* Check arguments */
    if (NULL == offset && NULL == filter_mask && NULL == addr && NULL == size)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                    "invalid arguments, must have at least one non-null output argument");
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(dset_id, H5I_DATASET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid dataset identifier");

    /* Set up VOL callback arguments */
    dset_opt_args.get_num_chunks.space_id = fspace_id;
    dset_opt_args.get_num_chunks.nchunks  = &nchunks;
    vol_cb_args.op_type                   = H5VL_NATIVE_DATASET_GET_NUM_CHUNKS;
    vol_cb_args.args                      = &dset_opt_args;

    /* Get the number of written chunks to check range */
    if (H5VL_dataset_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get number of chunks");

    /* Check range for chunk index */
    if (chk_index >= nchunks)
        HGOTO_ERROR(H5E_DATASET, H5E_BADRANGE, FAIL, "chunk index is out of range");

    /* Set up VOL callback arguments */
    dset_opt_args.get_chunk_info_by_idx.space_id    = fspace_id;
    dset_opt_args.get_chunk_info_by_idx.chk_index   = chk_index;
    dset_opt_args.get_chunk_info_by_idx.offset      = offset;
    dset_opt_args.get_chunk_info_by_idx.filter_mask = filter_mask;
    dset_opt_args.get_chunk_info_by_idx.addr        = addr;
    dset_opt_args.get_chunk_info_by_idx.size        = size;
    vol_cb_args.op_type                             = H5VL_NATIVE_DATASET_GET_CHUNK_INFO_BY_IDX;
    vol_cb_args.args                                = &dset_opt_args;

    /* Call private function to get the chunk info given the chunk's index */
    if (H5VL_dataset_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get chunk info by index");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Dget_chunk_info() */

/*-------------------------------------------------------------------------
 * Function:    H5Dget_chunk_info_by_coord
 *
 * Purpose:     Retrieves information about a chunk specified by its logical
 *              coordinates.
 *
 * Parameters:
 *              hid_t dset_id;          IN: Chunked dataset ID
 *              hsize_t *offset         IN: Logical position of the chunk's
 *                                           first element in the dataspace
 *              unsigned *filter_mask   OUT: Mask for identifying the filters in use
 *              haddr_t *addr           OUT: Address of the chunk
 *              hsize_t *size           OUT: Size of the chunk
 *
 * Return:      Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dget_chunk_info_by_coord(hid_t dset_id, const hsize_t *offset, unsigned *filter_mask /*out*/,
                           haddr_t *addr /*out*/, hsize_t *size /*out*/)
{
    H5VL_object_t                      *vol_obj = NULL; /* Dataset for this operation */
    H5VL_optional_args_t                vol_cb_args;    /* Arguments to VOL callback */
    H5VL_native_dataset_optional_args_t dset_opt_args;  /* Arguments for optional operation */
    herr_t                              ret_value = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "i*hxxx", dset_id, offset, filter_mask, addr, size);

    /* Check arguments */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(dset_id, H5I_DATASET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid dataset identifier");
    if (NULL == filter_mask && NULL == addr && NULL == size)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                    "invalid arguments, must have at least one non-null output argument");
    if (NULL == offset)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid argument (null)");

    /* Set up VOL callback arguments */
    dset_opt_args.get_chunk_info_by_coord.offset      = offset;
    dset_opt_args.get_chunk_info_by_coord.filter_mask = filter_mask;
    dset_opt_args.get_chunk_info_by_coord.addr        = addr;
    dset_opt_args.get_chunk_info_by_coord.size        = size;
    vol_cb_args.op_type                               = H5VL_NATIVE_DATASET_GET_CHUNK_INFO_BY_COORD;
    vol_cb_args.args                                  = &dset_opt_args;

    /* Call private function to get the chunk info given the chunk's index */
    if (H5VL_dataset_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get chunk info by its logical coordinates");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Dget_chunk_info_by_coord() */

/*-------------------------------------------------------------------------
 * Function:    H5Dchunk_iter
 *
 * Purpose:     Iterates over all chunks in dataset with given callback and user data.
 *
 * Parameters:
 *              hid_t dset_id;          IN: Chunked dataset ID
 *              hid_t dxpl_id;          IN: Dataset transfer property list ID
 *              H5D_chunk_iter_op_t cb  IN: User callback function, called for every chunk.
 *              void *op_data           IN/OUT: Optional user data passed on to user callback.
 *
 * Return:      Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Dchunk_iter(hid_t dset_id, hid_t dxpl_id, H5D_chunk_iter_op_t op, void *op_data)
{
    H5VL_object_t                      *vol_obj = NULL; /* Dataset for this operation */
    H5VL_optional_args_t                vol_cb_args;    /* Arguments to VOL callback */
    H5VL_native_dataset_optional_args_t dset_opt_args;  /* Arguments for optional operation */
    herr_t                              ret_value = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "iix*x", dset_id, dxpl_id, op, op_data);

    /* Check arguments */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(dset_id, H5I_DATASET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid dataset identifier");
    if (NULL == op)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid callback to chunk iteration");

    /* Get the default dataset transfer property list if the user didn't provide one */
    if (H5P_DEFAULT == dxpl_id)
        dxpl_id = H5P_DATASET_XFER_DEFAULT;
    else if (true != H5P_isa_class(dxpl_id, H5P_DATASET_XFER))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "dxpl_id is not a dataset transfer property list ID");

    /* Set up VOL callback arguments */
    dset_opt_args.chunk_iter.op      = op;
    dset_opt_args.chunk_iter.op_data = op_data;
    vol_cb_args.op_type              = H5VL_NATIVE_DATASET_CHUNK_ITER;
    vol_cb_args.args                 = &dset_opt_args;

    /* Iterate over the chunks */
    if ((ret_value = H5VL_dataset_optional(vol_obj, &vol_cb_args, dxpl_id, H5_REQUEST_NULL)) < 0)
        HERROR(H5E_BADITER, H5E_BADITER, "error iterating over dataset chunks");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Dchunk_iter() */
