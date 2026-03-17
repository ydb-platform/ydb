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

#include "H5Amodule.h" /* This source code file is part of the H5A module */
#define H5O_FRIEND     /* Suppress error about including H5Opkg */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5Apkg.h"      /* Attributes                               */
#include "H5CXprivate.h" /* API Contexts                             */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5ESprivate.h" /* Event Sets                               */
#include "H5FLprivate.h" /* Free Lists                               */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5MMprivate.h" /* Memory management                        */
#include "H5Opkg.h"      /* Object headers                           */
#include "H5Sprivate.h"  /* Dataspace functions                      */
#include "H5VLprivate.h" /* Virtual Object Layer                     */

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

/* Helper routines for sync/async API calls */
static hid_t  H5A__create_common(H5VL_object_t *vol_obj, H5VL_loc_params_t *loc_params, const char *attr_name,
                                 hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id,
                                 void **token_ptr);
static hid_t  H5A__create_api_common(hid_t loc_id, const char *attr_name, hid_t type_id, hid_t space_id,
                                     hid_t acpl_id, hid_t aapl_id, void **token_ptr,
                                     H5VL_object_t **_vol_obj_ptr);
static hid_t  H5A__create_by_name_api_common(hid_t loc_id, const char *obj_name, const char *attr_name,
                                             hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id,
                                             hid_t lapl_id, void **token_ptr, H5VL_object_t **_vol_obj_ptr);
static hid_t  H5A__open_common(H5VL_object_t *vol_obj, H5VL_loc_params_t *loc_params, const char *attr_name,
                               hid_t aapl_id, void **token_ptr);
static hid_t  H5A__open_api_common(hid_t loc_id, const char *attr_name, hid_t aapl_id, void **token_ptr,
                                   H5VL_object_t **_vol_obj_ptr);
static hid_t  H5A__open_by_name_api_common(hid_t loc_id, const char *obj_name, const char *attr_name,
                                           hid_t aapl_id, hid_t lapl_id, void **token_ptr,
                                           H5VL_object_t **_vol_obj_ptr);
static hid_t  H5A__open_by_idx_api_common(hid_t loc_id, const char *obj_name, H5_index_t idx_type,
                                          H5_iter_order_t order, hsize_t n, hid_t aapl_id, hid_t lapl_id,
                                          void **token_ptr, H5VL_object_t **_vol_obj_ptr);
static herr_t H5A__write_api_common(hid_t attr_id, hid_t type_id, const void *buf, void **token_ptr,
                                    H5VL_object_t **_vol_obj_ptr);
static herr_t H5A__read_api_common(hid_t attr_id, hid_t dtype_id, void *buf, void **token_ptr,
                                   H5VL_object_t **_vol_obj_ptr);
static herr_t H5A__rename_common(H5VL_object_t *vol_obj, H5VL_loc_params_t *loc_params, const char *old_name,
                                 const char *new_name, void **token_ptr);
static herr_t H5A__rename_api_common(hid_t loc_id, const char *old_name, const char *new_name,
                                     void **token_ptr, H5VL_object_t **_vol_obj_ptr);
static herr_t H5A__rename_by_name_api_common(hid_t loc_id, const char *obj_name, const char *old_attr_name,
                                             const char *new_attr_name, hid_t lapl_id, void **token_ptr,
                                             H5VL_object_t **_vol_obj_ptr);
static herr_t H5A__exists_common(H5VL_object_t *vol_obj, H5VL_loc_params_t *loc_params, const char *attr_name,
                                 bool *attr_exists, void **token_ptr);
static herr_t H5A__exists_api_common(hid_t obj_id, const char *attr_name, bool *attr_exists, void **token_ptr,
                                     H5VL_object_t **_vol_obj_ptr);
static herr_t H5A__exists_by_name_api_common(hid_t obj_id, const char *obj_name, const char *attr_name,
                                             bool *attr_exists, hid_t lapl_id, void **token_ptr,
                                             H5VL_object_t **_vol_obj_ptr);

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*--------------------------------------------------------------------------
 * Function:    H5A__create_common
 *
 * Purpose:     This is the common function for creating HDF5 datasets.
 *
 * Return:      Success:    An attribute ID
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
static hid_t
H5A__create_common(H5VL_object_t *vol_obj, H5VL_loc_params_t *loc_params, const char *attr_name,
                   hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id, void **token_ptr)
{
    void *attr      = NULL;            /* Attribute created */
    hid_t ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(vol_obj);
    assert(loc_params);
    assert(attr_name);

    /* Create the attribute */
    if (NULL == (attr = H5VL_attr_create(vol_obj, loc_params, attr_name, type_id, space_id, acpl_id, aapl_id,
                                         H5P_DATASET_XFER_DEFAULT, token_ptr)))
        HGOTO_ERROR(H5E_ATTR, H5E_CANTINIT, H5I_INVALID_HID, "unable to create attribute");

    /* Register the new attribute and get an ID for it */
    if ((ret_value = H5VL_register(H5I_ATTR, attr, vol_obj->connector, true)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register attribute for ID");

done:
    /* Cleanup on failure */
    if (H5I_INVALID_HID == ret_value)
        if (attr && H5VL_attr_close(vol_obj, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, H5I_INVALID_HID, "can't close attribute");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5A__create_common() */

/*--------------------------------------------------------------------------
 * Function:    H5A__create_api_common
 *
 * Purpose:     This is the common function for creating HDF5 attributes
 *
 * Return:      Success:    An attribute ID
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
static hid_t
H5A__create_api_common(hid_t loc_id, const char *attr_name, hid_t type_id, hid_t space_id, hid_t acpl_id,
                       hid_t aapl_id, void **token_ptr, H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_loc_params_t loc_params;                     /* Location parameters for object access */
    hid_t             ret_value = H5I_INVALID_HID;    /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (H5I_ATTR == H5I_get_type(loc_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "location is not valid for an attribute");
    if (!attr_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "attr_name parameter cannot be NULL");
    if (!*attr_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "attr_name parameter cannot be an empty string");

    /* Set up object access arguments */
    if (H5VL_setup_acc_args(loc_id, H5P_CLS_AACC, true, &aapl_id, vol_obj_ptr, &loc_params) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTSET, H5I_INVALID_HID, "can't set object access arguments");

    /* Get correct property list */
    if (H5P_DEFAULT == acpl_id)
        acpl_id = H5P_ATTRIBUTE_CREATE_DEFAULT;

    /* Create the attribute */
    if ((ret_value = H5A__create_common(*vol_obj_ptr, &loc_params, attr_name, type_id, space_id, acpl_id,
                                        aapl_id, token_ptr)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTCREATE, H5I_INVALID_HID, "unable to create attribute");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5A__create_api_common() */

/*--------------------------------------------------------------------------
 * Function:    H5Acreate2
 *
 * Purpose:     Creates an attribute on an object
 *
 * Usage:
 *              hid_t H5Acreate2(loc_id, attr_name, type_id, space_id, acpl_id,
 *                  aapl_id)
 *
 * Description: This function creates an attribute that is attached to the
 *              object specified with 'loc_id'. The name specified with
 *              'attr_name' for each attribute for an object must be unique
 *              for that object. The 'type_id' and 'space_id' are created
 *              with the H5T and H5S interfaces, respectively. The 'aapl_id'
 *              property list is currently unused, but will be used in the
 *              future for optional attribute access properties. The
 *              attribute ID returned from this function must be released
 *              with H5Aclose or resource leaks will develop.
 *
 * Parameters:
 *              hid_t loc_id;           IN: Object (dataset or group) to be attached to
 *              const char *attr_name;  IN: Name of attribute to locate and open
 *              hid_t type_id;          IN: ID of datatype for attribute
 *              hid_t space_id;         IN: ID of dataspace for attribute
 *              hid_t acpl_id;          IN: ID of creation property list
 *              hid_t aapl_id;          IN: ID of Attribute access property list (currently not used)
 *
 * Return:      Success:    An ID for the created attribute
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Acreate2(hid_t loc_id, const char *attr_name, hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id)
{
    hid_t ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE6("i", "i*siiii", loc_id, attr_name, type_id, space_id, acpl_id, aapl_id);

    /* Create the attribute synchronously */
    if ((ret_value =
             H5A__create_api_common(loc_id, attr_name, type_id, space_id, acpl_id, aapl_id, NULL, NULL)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTCREATE, H5I_INVALID_HID, "unable to synchronously create attribute");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Acreate2() */

/*--------------------------------------------------------------------------
 * Function:    H5Acreate_sync
 *
 * Purpose:     Asynchronous version of H5Acreate
 *
 * Return:      Success:    An attribute ID
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Acreate_async(const char *app_file, const char *app_func, unsigned app_line, hid_t loc_id,
                const char *attr_name, hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id,
                hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    hid_t          ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE10("i", "*s*sIui*siiiii", app_file, app_func, app_line, loc_id, attr_name, type_id, space_id,
              acpl_id, aapl_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Create the attribute asynchronously */
    if ((ret_value = H5A__create_api_common(loc_id, attr_name, type_id, space_id, acpl_id, aapl_id, token_ptr,
                                            &vol_obj)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTCREATE, H5I_INVALID_HID, "unable to asynchronously create attribute");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE10(__func__, "*s*sIui*siiiii", app_file, app_func, app_line, loc_id, attr_name, type_id, space_id, acpl_id, aapl_id, es_id)) < 0) {
            /* clang-format on */
            if (H5I_dec_app_ref(ret_value) < 0)
                HDONE_ERROR(H5E_ATTR, H5E_CANTDEC, H5I_INVALID_HID, "can't decrement count on attribute ID");
            HGOTO_ERROR(H5E_ATTR, H5E_CANTINSERT, H5I_INVALID_HID, "can't insert token into event set");
        } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Acreate_async() */

/*--------------------------------------------------------------------------
 * Function:    H5A__create_by_name_api_common
 *
 * Purpose:     This is the common function for creating HDF5 attributes by name
 *
 * Return:      Success:    An attribute ID
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
static hid_t
H5A__create_by_name_api_common(hid_t loc_id, const char *obj_name, const char *attr_name, hid_t type_id,
                               hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t lapl_id, void **token_ptr,
                               H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_loc_params_t loc_params;                     /* Location parameters for object access */
    hid_t             ret_value = H5I_INVALID_HID;    /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (H5I_ATTR == H5I_get_type(loc_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "location is not valid for an attribute");
    if (!attr_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "attr_name parameter cannot be NULL");
    if (!*attr_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "attr_name parameter cannot be an empty string");

    /* obj_name is verified in H5VL_setup_name_args() */
    /* Set up object access arguments */
    if (H5VL_setup_name_args(loc_id, obj_name, true, lapl_id, vol_obj_ptr, &loc_params) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTSET, H5I_INVALID_HID, "can't set object access arguments");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&aapl_id, H5P_CLS_AACC, loc_id, true) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTSET, H5I_INVALID_HID, "can't set attribute access property list info");

    /* Get correct property list */
    if (H5P_DEFAULT == acpl_id)
        acpl_id = H5P_ATTRIBUTE_CREATE_DEFAULT;

    /* Create the attribute */
    if ((ret_value = H5A__create_common(*vol_obj_ptr, &loc_params, attr_name, type_id, space_id, acpl_id,
                                        aapl_id, token_ptr)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTCREATE, H5I_INVALID_HID, "unable to create attribute");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5A__create_by_name_api_common() */

/*--------------------------------------------------------------------------
 NAME
    H5Acreate_by_name
 PURPOSE
    Creates an attribute on an object
 USAGE
    hid_t H5Acreate_by_name(loc_id, obj_name, attr_name, type_id, space_id, acpl_id,
            aapl_id, lapl_id)
        hid_t loc_id;       IN: Object (dataset or group) to be attached to
        const char *obj_name;   IN: Name of object relative to location
        const char *attr_name;  IN: Name of attribute to locate and open
        hid_t type_id;          IN: ID of datatype for attribute
        hid_t space_id;         IN: ID of dataspace for attribute
        hid_t acpl_id;          IN: ID of creation property list
        hid_t aapl_id;          IN: ID of Attribute access property list (currently not used)
        hid_t lapl_id;          IN: Link access property list
 RETURNS
    Non-negative on success/H5I_INVALID_HID on failure

 DESCRIPTION
        This function creates an attribute that is attached to the object
    specified with 'loc_id/obj_name'.  The name specified with 'attr_name' for
    each attribute for an object must be unique for that object.  The 'type_id'
    and 'space_id' are created with the H5T and H5S interfaces respectively.
    The 'aapl_id' property list is currently unused, but will be used in the
    future for optional attribute access properties.  The attribute ID returned
    from this function must be released with H5Aclose or resource leaks will
    develop.

--------------------------------------------------------------------------*/
hid_t
H5Acreate_by_name(hid_t loc_id, const char *obj_name, const char *attr_name, hid_t type_id, hid_t space_id,
                  hid_t acpl_id, hid_t aapl_id, hid_t lapl_id)
{
    hid_t ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE8("i", "i*s*siiiii", loc_id, obj_name, attr_name, type_id, space_id, acpl_id, aapl_id, lapl_id);

    /* Create the attribute synchronously */
    if ((ret_value = H5A__create_by_name_api_common(loc_id, obj_name, attr_name, type_id, space_id, acpl_id,
                                                    aapl_id, lapl_id, NULL, NULL)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTCREATE, H5I_INVALID_HID, "unable to synchronously create attribute");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Acreate_by_name() */

/*--------------------------------------------------------------------------
 * Function:    H5Acreate_by_name_async
 *
 * Purpose:     Asynchronous version of H5Acreate_by_name
 *
 * Return:      Success:    An attribute ID
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Acreate_by_name_async(const char *app_file, const char *app_func, unsigned app_line, hid_t loc_id,
                        const char *obj_name, const char *attr_name, hid_t type_id, hid_t space_id,
                        hid_t acpl_id, hid_t aapl_id, hid_t lapl_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    hid_t          ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE12("i", "*s*sIui*s*siiiiii", app_file, app_func, app_line, loc_id, obj_name, attr_name, type_id,
              space_id, acpl_id, aapl_id, lapl_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Create the attribute asynchronously */
    if ((ret_value = H5A__create_by_name_api_common(loc_id, obj_name, attr_name, type_id, space_id, acpl_id,
                                                    aapl_id, lapl_id, token_ptr, &vol_obj)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTCREATE, H5I_INVALID_HID, "unable to asynchronously create attribute");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE12(__func__, "*s*sIui*s*siiiiii", app_file, app_func, app_line, loc_id, obj_name, attr_name, type_id, space_id, acpl_id, aapl_id, lapl_id, es_id)) < 0) {
            /* clang-format on */
            if (H5I_dec_app_ref(ret_value) < 0)
                HDONE_ERROR(H5E_ATTR, H5E_CANTDEC, H5I_INVALID_HID, "can't decrement count on attribute ID");
            HGOTO_ERROR(H5E_ATTR, H5E_CANTINSERT, H5I_INVALID_HID, "can't insert token into event set");
        } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Acreate_by_name_async() */

/*-------------------------------------------------------------------------
 * Function:    H5A__open_common
 *
 * Purpose:     This is the common function for opening an attribute
 *
 * Return:      Success:    A group ID
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
static hid_t
H5A__open_common(H5VL_object_t *vol_obj, H5VL_loc_params_t *loc_params, const char *attr_name, hid_t aapl_id,
                 void **token_ptr)
{
    void *attr      = NULL; /* attr object from VOL connector */
    hid_t ret_value = H5I_INVALID_HID;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(vol_obj);
    assert(loc_params);

    /* Open the attribute */
    if (NULL ==
        (attr = H5VL_attr_open(vol_obj, loc_params, attr_name, aapl_id, H5P_DATASET_XFER_DEFAULT, token_ptr)))
        HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, H5I_INVALID_HID, "unable to open attribute: '%s'", attr_name);

    /* Register the attribute and get an ID for it */
    if ((ret_value = H5VL_register(H5I_ATTR, attr, vol_obj->connector, true)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register attribute for ID");

done:
    /* Cleanup on failure */
    if (H5I_INVALID_HID == ret_value)
        if (attr && H5VL_attr_close(vol_obj, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, H5I_INVALID_HID, "can't close attribute");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5A__open_common() */

/*-------------------------------------------------------------------------
 * Function:    H5A__open_api_common
 *
 * Purpose:     This is the common function for opening an attribute
 *
 * Return:      Success:    A group ID
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
static hid_t
H5A__open_api_common(hid_t loc_id, const char *attr_name, hid_t aapl_id, void **token_ptr,
                     H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_loc_params_t loc_params;                     /* Location parameters for object access */
    hid_t             ret_value = H5I_INVALID_HID;    /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (H5I_ATTR == H5I_get_type(loc_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "location is not valid for an attribute");
    if (!attr_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "name parameter cannot be NULL");
    if (!*attr_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "name parameter cannot be an empty string");

    /* Set up object access arguments */
    if (H5VL_setup_acc_args(loc_id, H5P_CLS_AACC, false, &aapl_id, vol_obj_ptr, &loc_params) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTSET, H5I_INVALID_HID, "can't set object access arguments");

    /* Open the attribute */
    if ((ret_value = H5A__open_common(*vol_obj_ptr, &loc_params, attr_name, aapl_id, token_ptr)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, H5I_INVALID_HID, "unable to open attribute: '%s'", attr_name);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5A__open_api_common() */

/*--------------------------------------------------------------------------
 NAME
    H5Aopen
 PURPOSE
    Opens an attribute for an object by looking up the attribute name
 USAGE
    hid_t H5Aopen(loc_id, attr_name, aapl_id)
        hid_t loc_id;           IN: Object that attribute is attached to
        const char *attr_name;  IN: Name of attribute to locate and open
        hid_t aapl_id;          IN: Attribute access property list
 RETURNS
    ID of attribute on success, H5I_INVALID_HID on failure

 DESCRIPTION
        This function opens an existing attribute for access.  The attribute
    name specified is used to look up the corresponding attribute for the
    object.  The attribute ID returned from this function must be released with
    H5Aclose or resource leaks will develop.
--------------------------------------------------------------------------*/
hid_t
H5Aopen(hid_t loc_id, const char *attr_name, hid_t aapl_id)
{
    hid_t ret_value = H5I_INVALID_HID;

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE3("i", "i*si", loc_id, attr_name, aapl_id);

    /* Open the attribute synchronously */
    if ((ret_value = H5A__open_api_common(loc_id, attr_name, aapl_id, NULL, NULL)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTCREATE, H5I_INVALID_HID, "unable to synchronously open attribute");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Aopen() */

/*--------------------------------------------------------------------------
 *  NAME
 *      H5Aopen_async
 *  PURPOSE
 *      Asynchronous version of H5Aopen
 *
 *  RETURNS
 *      ID of attribute on success, H5I_INVALID_HID on failure
 *
 *--------------------------------------------------------------------------*/
hid_t
H5Aopen_async(const char *app_file, const char *app_func, unsigned app_line, hid_t loc_id,
              const char *attr_name, hid_t aapl_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    hid_t          ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE7("i", "*s*sIui*sii", app_file, app_func, app_line, loc_id, attr_name, aapl_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Open the attribute asynchronously */
    if ((ret_value = H5A__open_api_common(loc_id, attr_name, aapl_id, token_ptr, &vol_obj)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTCREATE, H5I_INVALID_HID, "unable to asynchronously open attribute");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE7(__func__, "*s*sIui*sii", app_file, app_func, app_line, loc_id, attr_name, aapl_id, es_id)) < 0) {
            /* clang-format on */
            if (H5I_dec_app_ref(ret_value) < 0)
                HDONE_ERROR(H5E_ATTR, H5E_CANTDEC, H5I_INVALID_HID, "can't decrement count on attribute ID");
            HGOTO_ERROR(H5E_ATTR, H5E_CANTINSERT, H5I_INVALID_HID, "can't insert token into event set");
        } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Aopen_async() */

/*-------------------------------------------------------------------------
 * Function:    H5A__open_by_name_api_common
 *
 * Purpose:     This is the common function for opening an attribute
 *
 * Return:      Success:    A group ID
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
static hid_t
H5A__open_by_name_api_common(hid_t loc_id, const char *obj_name, const char *attr_name, hid_t aapl_id,
                             hid_t lapl_id, void **token_ptr, H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_loc_params_t loc_params;                     /* Location parameters for object access */
    hid_t             ret_value = H5I_INVALID_HID;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (H5I_ATTR == H5I_get_type(loc_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "location is not valid for an attribute");

    if (!attr_name || !*attr_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "no attribute name");

    /* obj_name is verified in H5VL_setup_name_args() */
    /* Set up object access arguments */
    if (H5VL_setup_name_args(loc_id, obj_name, false, lapl_id, vol_obj_ptr, &loc_params) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTSET, H5I_INVALID_HID, "can't set object access arguments");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&aapl_id, H5P_CLS_AACC, loc_id, false) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTSET, H5I_INVALID_HID, "can't set attribute access property list info");

    /* Open the attribute */
    if ((ret_value = H5A__open_common(*vol_obj_ptr, &loc_params, attr_name, aapl_id, token_ptr)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, H5I_INVALID_HID, "unable to open attribute: '%s'", attr_name);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5A__open_by_name_api_common() */

/*--------------------------------------------------------------------------
 NAME
    H5Aopen_by_name
 PURPOSE
    Opens an attribute for an object by looking up the attribute name
 USAGE
    hid_t H5Aopen_by_name(loc_id, obj_name, attr_name, aapl_id, lapl_id)
        hid_t loc_id;           IN: Object that attribute is attached to
        const char *obj_name;   IN: Name of the object relative to location
        const char *attr_name;  IN: Name of attribute to locate and open
        hid_t aapl_id;          IN: Attribute access property list
        hid_t lapl_id;          IN: Link access property list
 RETURNS
    ID of attribute on success, H5I_INVALID_HID on failure

 DESCRIPTION
        This function opens an existing attribute for access.  The attribute
    name specified is used to look up the corresponding attribute for the
    object.  The attribute ID returned from this function must be released with
    H5Aclose or resource leaks will develop.
--------------------------------------------------------------------------*/
hid_t
H5Aopen_by_name(hid_t loc_id, const char *obj_name, const char *attr_name, hid_t aapl_id, hid_t lapl_id)
{
    hid_t ret_value = H5I_INVALID_HID;

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE5("i", "i*s*sii", loc_id, obj_name, attr_name, aapl_id, lapl_id);

    /* Open the attribute by name asynchronously */
    if ((ret_value =
             H5A__open_by_name_api_common(loc_id, obj_name, attr_name, aapl_id, lapl_id, NULL, NULL)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, H5I_INVALID_HID, "unable to synchronously open attribute");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Aopen_by_name() */

/*--------------------------------------------------------------------------
 *  NAME
 *      H5Aopen_by_name_async
 *  PURPOSE
 *      Asynchronous version of H5Aopen_by_name
 *
 *  RETURNS
 *      ID of attribute on success, H5I_INVALID_HID on failure
 *
 *--------------------------------------------------------------------------*/
hid_t
H5Aopen_by_name_async(const char *app_file, const char *app_func, unsigned app_line, hid_t loc_id,
                      const char *obj_name, const char *attr_name, hid_t aapl_id, hid_t lapl_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    hid_t          ret_value = H5I_INVALID_HID;

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE9("i", "*s*sIui*s*siii", app_file, app_func, app_line, loc_id, obj_name, attr_name, aapl_id,
             lapl_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Open the attribute by name asynchronously */
    if ((ret_value = H5A__open_by_name_api_common(loc_id, obj_name, attr_name, aapl_id, lapl_id, token_ptr,
                                                  &vol_obj)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, H5I_INVALID_HID, "unable to asynchronously open attribute");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE9(__func__, "*s*sIui*s*siii", app_file, app_func, app_line, loc_id, obj_name, attr_name, aapl_id, lapl_id, es_id)) < 0) {
            /* clang-format on */
            if (H5I_dec_app_ref(ret_value) < 0)
                HDONE_ERROR(H5E_ATTR, H5E_CANTDEC, H5I_INVALID_HID, "can't decrement count on attribute ID");
            HGOTO_ERROR(H5E_ATTR, H5E_CANTINSERT, H5I_INVALID_HID, "can't insert token into event set");
        } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Aopen_by_name_async() */

/*-------------------------------------------------------------------------
 * Function:    H5A__open_by_idx_api_common
 *
 * Purpose:     This is the common function for opening an attribute
 *
 * Return:      Success:    A group ID
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
static hid_t
H5A__open_by_idx_api_common(hid_t loc_id, const char *obj_name, H5_index_t idx_type, H5_iter_order_t order,
                            hsize_t n, hid_t aapl_id, hid_t lapl_id, void **token_ptr,
                            H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_loc_params_t loc_params;                     /* Location parameters for object access */
    hid_t             ret_value = H5I_INVALID_HID;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (H5I_ATTR == H5I_get_type(loc_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "location is not valid for an attribute");
    if (!obj_name || !*obj_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "no object name");
    if (idx_type <= H5_INDEX_UNKNOWN || idx_type >= H5_INDEX_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "invalid index type specified");
    if (order <= H5_ITER_UNKNOWN || order >= H5_ITER_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "invalid iteration order specified");

    /* Set up object access arguments */
    if (H5VL_setup_idx_args(loc_id, obj_name, idx_type, order, n, false, lapl_id, vol_obj_ptr, &loc_params) <
        0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTSET, H5I_INVALID_HID, "can't set object access arguments");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&aapl_id, H5P_CLS_AACC, loc_id, false) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTSET, H5I_INVALID_HID, "can't set attribute access property list info");

    /* Open the attribute */
    if ((ret_value = H5A__open_common(*vol_obj_ptr, &loc_params, NULL, aapl_id, token_ptr)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, H5I_INVALID_HID, "unable to open attribute");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5A__open_by_idx_api_common() */

/*--------------------------------------------------------------------------
 NAME
    H5Aopen_by_idx
 PURPOSE
    Opens the n'th attribute for an object, according to the order within
    an index
 USAGE
    hid_t H5Aopen_by_idx(loc_id, obj_ame, idx_type, order, n, aapl_id, lapl_id)
        hid_t loc_id;           IN: Object that attribute is attached to
        const char *obj_name;   IN: Name of the object relative to location
        H5_index_t idx_type;    IN: Type of index to use
        H5_iter_order_t order;  IN: Order to iterate over index
        hsize_t n;              IN: Index (0-based) attribute to open
        hid_t aapl_id;          IN: Attribute access property list
        hid_t lapl_id;          IN: Link access property list
 RETURNS
    ID of attribute on success, H5I_INVALID_HID on failure

 DESCRIPTION
        This function opens an existing attribute for access.  The attribute
    index specified is used to look up the corresponding attribute for the
    object.  The attribute ID returned from this function must be released with
    H5Aclose or resource leaks will develop.
--------------------------------------------------------------------------*/
hid_t
H5Aopen_by_idx(hid_t loc_id, const char *obj_name, H5_index_t idx_type, H5_iter_order_t order, hsize_t n,
               hid_t aapl_id, hid_t lapl_id)
{
    hid_t ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE7("i", "i*sIiIohii", loc_id, obj_name, idx_type, order, n, aapl_id, lapl_id);

    /* Open the attribute by idx synchronously */
    if ((ret_value = H5A__open_by_idx_api_common(loc_id, obj_name, idx_type, order, n, aapl_id, lapl_id, NULL,
                                                 NULL)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTCREATE, H5I_INVALID_HID, "unable to synchronously open attribute");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Aopen_by_idx() */

/*--------------------------------------------------------------------------
 *  NAME
 *      H5Aopen_by_idx_async
 *  PURPOSE
 *      Asynchronous version of H5Aopen_by_idx
 *
 *  RETURNS
 *      ID of attribute on success, H5I_INVALID_HID on failure
 *
 *--------------------------------------------------------------------------*/
hid_t
H5Aopen_by_idx_async(const char *app_file, const char *app_func, unsigned app_line, hid_t loc_id,
                     const char *obj_name, H5_index_t idx_type, H5_iter_order_t order, hsize_t n,
                     hid_t aapl_id, hid_t lapl_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    hid_t          ret_value = H5I_INVALID_HID;

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE11("i", "*s*sIui*sIiIohiii", app_file, app_func, app_line, loc_id, obj_name, idx_type, order, n,
              aapl_id, lapl_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Open the attribute by idx asynchronously */
    if ((ret_value = H5A__open_by_idx_api_common(loc_id, obj_name, idx_type, order, n, aapl_id, lapl_id,
                                                 token_ptr, &vol_obj)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTCREATE, H5I_INVALID_HID, "unable to asynchronously open attribute");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE11(__func__, "*s*sIui*sIiIohiii", app_file, app_func, app_line, loc_id, obj_name, idx_type, order, n, aapl_id, lapl_id, es_id)) < 0) {
            /* clang-format on */
            if (H5I_dec_app_ref(ret_value) < 0)
                HDONE_ERROR(H5E_ATTR, H5E_CANTDEC, H5I_INVALID_HID, "can't decrement count on attribute ID");
            HGOTO_ERROR(H5E_ATTR, H5E_CANTINSERT, H5I_INVALID_HID, "can't insert token into event set");
        } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Aopen_by_idx_async() */

/*--------------------------------------------------------------------------
 NAME
    H5A__write_api_common
 PURPOSE
    Common helper routine for sync/async dataset write operations.
 RETURNS
    Non-negative on success/Negative on failure
--------------------------------------------------------------------------*/
static herr_t
H5A__write_api_common(hid_t attr_id, hid_t type_id, const void *buf, void **token_ptr,
                      H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    herr_t ret_value = SUCCEED;                       /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (H5I_DATATYPE != H5I_get_type(type_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    if (NULL == buf)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "buf parameter can't be NULL");

    /* Get attribute pointer */
    if (H5VL_setup_args(attr_id, H5I_ATTR, vol_obj_ptr) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get VOL object for attribute");

    /* Write the attribute data */
    if (H5VL_attr_write(*vol_obj_ptr, type_id, buf, H5P_DATASET_XFER_DEFAULT, token_ptr) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_WRITEERROR, FAIL, "unable to write attribute");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5A__write_api_common() */

/*--------------------------------------------------------------------------
 NAME
    H5Awrite
 PURPOSE
    Write out data to an attribute
 USAGE
    herr_t H5Awrite (attr_id, dtype_id, buf)
        hid_t attr_id;       IN: Attribute to write
        hid_t dtype_id;       IN: Memory datatype of buffer
        const void *buf;     IN: Buffer of data to write
 RETURNS
    Non-negative on success/Negative on failure

 DESCRIPTION
        This function writes a complete attribute to disk.
--------------------------------------------------------------------------*/
herr_t
H5Awrite(hid_t attr_id, hid_t dtype_id, const void *buf)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ii*x", attr_id, dtype_id, buf);

    /* Synchronously write the data */
    if (H5A__write_api_common(attr_id, dtype_id, buf, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_WRITEERROR, FAIL, "can't synchronously write data");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Awrite() */

/*--------------------------------------------------------------------------
 NAME
    H5Awrite_async
 PURPOSE
    Asynchronous version of H5Awrite
 RETURNS
    Non-negative on success/Negative on failure
--------------------------------------------------------------------------*/
herr_t
H5Awrite_async(const char *app_file, const char *app_func, unsigned app_line, hid_t attr_id, hid_t dtype_id,
               const void *buf, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for attr_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE7("e", "*s*sIuii*xi", app_file, app_func, app_line, attr_id, dtype_id, buf, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Asynchronously write the data */
    if (H5A__write_api_common(attr_id, dtype_id, buf, token_ptr, &vol_obj) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_WRITEERROR, FAIL, "can't asynchronously write data");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE7(__func__, "*s*sIuii*xi", app_file, app_func, app_line, attr_id, dtype_id, buf, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_ATTR, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Awrite_async() */

/*--------------------------------------------------------------------------
 *  NAME
 *       H5A__read_api_common
 *  PURPOSE
 *      Common helper routine for sync/async attribute read operations.
 *  RETURNS
 *      Non-negative on success/Negative on failure
 *--------------------------------------------------------------------------*/
static herr_t
H5A__read_api_common(hid_t attr_id, hid_t dtype_id, void *buf, void **token_ptr, H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    herr_t ret_value = SUCCEED;                       /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (H5I_DATATYPE != H5I_get_type(dtype_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    if (NULL == buf)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "buf parameter can't be NULL");

    /* Get attribute object pointer */
    if (NULL == (*vol_obj_ptr = (H5VL_object_t *)H5I_object_verify(attr_id, H5I_ATTR)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not an attribute");

    /* Read the attribute data */
    if (H5VL_attr_read(*vol_obj_ptr, dtype_id, buf, H5P_DATASET_XFER_DEFAULT, token_ptr) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_READERROR, FAIL, "unable to read attribute");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5A__read_api_common() */

/*--------------------------------------------------------------------------
 NAME
    H5Aread
 PURPOSE
    Read in data from an attribute
 USAGE
    herr_t H5Aread (attr_id, dtype_id, buf)
        hid_t attr_id;       IN: Attribute to read
        hid_t dtype_id;       IN: Memory datatype of buffer
        void *buf;           IN: Buffer for data to read
 RETURNS
    Non-negative on success/Negative on failure

 DESCRIPTION
        This function reads a complete attribute from disk.
--------------------------------------------------------------------------*/
herr_t
H5Aread(hid_t attr_id, hid_t dtype_id, void *buf /*out*/)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "iix", attr_id, dtype_id, buf);

    /* Synchronously read the data */
    if (H5A__read_api_common(attr_id, dtype_id, buf, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_READERROR, FAIL, "can't synchronously read data");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Aread() */

/*--------------------------------------------------------------------------
 *  NAME
 *      H5Aread_async
 *  PURPOSE
 *      Asynchronous version of H5Aread
 *  RETURNS
 *      Non-negative on success/Negative on failure
 *--------------------------------------------------------------------------*/
herr_t
H5Aread_async(const char *app_file, const char *app_func, unsigned app_line, hid_t attr_id, hid_t dtype_id,
              void *buf /*out*/, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for attr_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE7("e", "*s*sIuiixi", app_file, app_func, app_line, attr_id, dtype_id, buf, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Asynchronously read the data */
    if (H5A__read_api_common(attr_id, dtype_id, buf, token_ptr, &vol_obj) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_READERROR, FAIL, "can't asynchronously read data");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE7(__func__, "*s*sIuiixi", app_file, app_func, app_line, attr_id, dtype_id, buf, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_ATTR, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Aread_async() */

/*--------------------------------------------------------------------------
 NAME
    H5Aget_space
 PURPOSE
    Gets a copy of the dataspace for an attribute
 USAGE
    hid_t H5Aget_space (attr_id)
        hid_t attr_id;       IN: Attribute to get dataspace of
 RETURNS
    A dataspace ID on success, H5I_INVALID_HID on failure

 DESCRIPTION
        This function retrieves a copy of the dataspace for an attribute.
    The dataspace ID returned from this function must be released with H5Sclose
    or resource leaks will develop.
--------------------------------------------------------------------------*/
hid_t
H5Aget_space(hid_t attr_id)
{
    H5VL_object_t       *vol_obj = NULL;              /* Attribute object for ID */
    H5VL_attr_get_args_t vol_cb_args;                 /* Arguments to VOL callback */
    hid_t                ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE1("i", "i", attr_id);

    /* Check arguments */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(attr_id, H5I_ATTR)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not an attribute");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                 = H5VL_ATTR_GET_SPACE;
    vol_cb_args.args.get_space.space_id = H5I_INVALID_HID;

    /* Get the dataspace */
    if (H5VL_attr_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, H5I_INVALID_HID, "unable to get dataspace of attribute");

    /* Set the return value */
    ret_value = vol_cb_args.args.get_space.space_id;

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Aget_space() */

/*--------------------------------------------------------------------------
 NAME
    H5Aget_type
 PURPOSE
    Gets a copy of the datatype for an attribute
 USAGE
    hid_t H5Aget_type (attr_id)
        hid_t attr_id;       IN: Attribute to get datatype of
 RETURNS
    A datatype ID on success, H5I_INVALID_HID on failure

 DESCRIPTION
        This function retrieves a copy of the datatype for an attribute.
    The datatype ID returned from this function must be released with H5Tclose
    or resource leaks will develop.
--------------------------------------------------------------------------*/
hid_t
H5Aget_type(hid_t attr_id)
{
    H5VL_object_t       *vol_obj = NULL;              /* Attribute object for ID */
    H5VL_attr_get_args_t vol_cb_args;                 /* Arguments to VOL callback */
    hid_t                ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE1("i", "i", attr_id);

    /* Check arguments */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(attr_id, H5I_ATTR)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not an attribute");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type               = H5VL_ATTR_GET_TYPE;
    vol_cb_args.args.get_type.type_id = H5I_INVALID_HID;

    /* Get the datatype */
    if (H5VL_attr_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, H5I_INVALID_HID, "unable to get datatype of attribute");

    /* Set the return value */
    ret_value = vol_cb_args.args.get_type.type_id;

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Aget_type() */

/*--------------------------------------------------------------------------
 NAME
    H5Aget_create_plist
 PURPOSE
    Gets a copy of the creation property list for an attribute
 USAGE
    hssize_t H5Aget_create_plist (attr_id, buf_size, buf)
        hid_t attr_id;      IN: Attribute to get the name of
 RETURNS
    This function returns the ID of a copy of the attribute's creation
    property list, or H5I_INVALID_HID on failure.

 ERRORS

 DESCRIPTION
        This function returns a copy of the creation property list for
    an attribute.  The resulting ID must be closed with H5Pclose() or
    resource leaks will occur.
--------------------------------------------------------------------------*/
hid_t
H5Aget_create_plist(hid_t attr_id)
{
    H5VL_object_t       *vol_obj = NULL;              /* Attribute object for ID */
    H5VL_attr_get_args_t vol_cb_args;                 /* Arguments to VOL callback */
    hid_t                ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE1("i", "i", attr_id);

    assert(H5P_LST_ATTRIBUTE_CREATE_ID_g != -1);

    /* Check arguments */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(attr_id, H5I_ATTR)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not an attribute");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type               = H5VL_ATTR_GET_ACPL;
    vol_cb_args.args.get_acpl.acpl_id = H5I_INVALID_HID;

    /* Get the acpl */
    if (H5VL_attr_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, H5I_INVALID_HID,
                    "unable to get creation property list for attribute");

    /* Set the return value */
    ret_value = vol_cb_args.args.get_acpl.acpl_id;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Aget_create_plist() */

/*--------------------------------------------------------------------------
 NAME
    H5Aget_name
 PURPOSE
    Gets a copy of the name for an attribute
 USAGE
    hssize_t H5Aget_name (attr_id, buf_size, buf)
        hid_t attr_id;      IN: Attribute to get the name of
        size_t buf_size;    IN: The size of the buffer to store the string in.
        char *buf;          IN: Buffer to store name in
 RETURNS
    This function returns the length of the attribute's name (which may be
    longer than 'buf_size') on success or negative for failure.

 DESCRIPTION
        This function retrieves the name of an attribute for an attribute ID.
    Up to 'buf_size' characters are stored in 'buf' followed by a '\0' string
    terminator.  If the name of the attribute is longer than 'buf_size'-1,
    the string terminator is stored in the last position of the buffer to
    properly terminate the string.
--------------------------------------------------------------------------*/
ssize_t
H5Aget_name(hid_t attr_id, size_t buf_size, char *buf /*out*/)
{
    H5VL_object_t       *vol_obj = NULL;     /* Attribute object for ID */
    H5VL_attr_get_args_t vol_cb_args;        /* Arguments to VOL callback */
    size_t               attr_name_len = 0;  /* Length of attribute name */
    ssize_t              ret_value     = -1; /* Return value */

    FUNC_ENTER_API((-1))
    H5TRACE3("Zs", "izx", attr_id, buf_size, buf);

    /* check arguments */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(attr_id, H5I_ATTR)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, (-1), "not an attribute");
    if (!buf && buf_size)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, (-1), "buf cannot be NULL if buf_size is non-zero");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                           = H5VL_ATTR_GET_NAME;
    vol_cb_args.args.get_name.loc_params.type     = H5VL_OBJECT_BY_SELF;
    vol_cb_args.args.get_name.loc_params.obj_type = H5I_get_type(attr_id);
    vol_cb_args.args.get_name.buf_size            = buf_size;
    vol_cb_args.args.get_name.buf                 = buf;
    vol_cb_args.args.get_name.attr_name_len       = &attr_name_len;

    /* Get the attribute name */
    if (H5VL_attr_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, (-1), "unable to get attribute name");

    /* Set the return value */
    ret_value = (ssize_t)attr_name_len;

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Aget_name() */

/*-------------------------------------------------------------------------
 * Function:	H5Aget_name_by_idx
 *
 * Purpose:	Retrieve the name of an attribute, according to the
 *		order within an index.
 *
 *              Same pattern of behavior as H5Iget_name.
 *
 * Return:	Success:	Non-negative length of name, with information
 *				in NAME buffer
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
ssize_t
H5Aget_name_by_idx(hid_t loc_id, const char *obj_name, H5_index_t idx_type, H5_iter_order_t order, hsize_t n,
                   char *name /*out*/, size_t size, hid_t lapl_id)
{
    H5VL_object_t       *vol_obj = NULL;    /* Attribute object for ID */
    H5VL_attr_get_args_t vol_cb_args;       /* Arguments to VOL callback */
    size_t               attr_name_len = 0; /* Length of attribute name */
    ssize_t              ret_value;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE8("Zs", "i*sIiIohxzi", loc_id, obj_name, idx_type, order, n, name, size, lapl_id);

    /* Check args */
    if (H5I_ATTR == H5I_get_type(loc_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "location is not valid for an attribute");
    if (!obj_name || !*obj_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no name");
    if (!name && size)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "name cannot be NULL if size is non-zero");
    if (idx_type <= H5_INDEX_UNKNOWN || idx_type >= H5_INDEX_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid index type specified");
    if (order <= H5_ITER_UNKNOWN || order >= H5_ITER_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid iteration order specified");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&lapl_id, H5P_CLS_LACC, loc_id, false) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTSET, FAIL, "can't set access property list info");

    /* Get the object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid object identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                                               = H5VL_ATTR_GET_NAME;
    vol_cb_args.args.get_name.loc_params.type                         = H5VL_OBJECT_BY_IDX;
    vol_cb_args.args.get_name.loc_params.loc_data.loc_by_idx.name     = obj_name;
    vol_cb_args.args.get_name.loc_params.loc_data.loc_by_idx.idx_type = idx_type;
    vol_cb_args.args.get_name.loc_params.loc_data.loc_by_idx.order    = order;
    vol_cb_args.args.get_name.loc_params.loc_data.loc_by_idx.n        = n;
    vol_cb_args.args.get_name.loc_params.loc_data.loc_by_idx.lapl_id  = lapl_id;
    vol_cb_args.args.get_name.loc_params.obj_type                     = H5I_get_type(loc_id);
    vol_cb_args.args.get_name.buf_size                                = size;
    vol_cb_args.args.get_name.buf                                     = name;
    vol_cb_args.args.get_name.attr_name_len                           = &attr_name_len;

    /* Get the name */
    if (H5VL_attr_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "unable to get name");

    /* Set the return value */
    ret_value = (ssize_t)attr_name_len;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Aget_name_by_idx() */

/*-------------------------------------------------------------------------
 * Function:	H5Aget_storage_size
 *
 * Purpose:	Returns the amount of storage size that is required for this
 *		attribute.
 *
 * Return:	Success:	The amount of storage size allocated for the
 *				attribute.  The return value may be zero
 *                              if no data has been stored.
 *
 *		Failure:	Zero
 *
 *-------------------------------------------------------------------------
 */
hsize_t
H5Aget_storage_size(hid_t attr_id)
{
    H5VL_object_t       *vol_obj = NULL;   /* Attribute object for ID */
    H5VL_attr_get_args_t vol_cb_args;      /* Arguments to VOL callback */
    hsize_t              storage_size = 0; /* Storage size of attribute */
    hsize_t              ret_value;        /* Return value */

    FUNC_ENTER_API(0)
    H5TRACE1("h", "i", attr_id);

    /* Check arguments */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(attr_id, H5I_ATTR)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, 0, "not an attribute");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                         = H5VL_ATTR_GET_STORAGE_SIZE;
    vol_cb_args.args.get_storage_size.data_size = &storage_size;

    /* Get the storage size */
    if (H5VL_attr_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, 0, "unable to get storage size");

    /* Set the return value */
    ret_value = storage_size;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Aget_storage_size() */

/*-------------------------------------------------------------------------
 * Function:	H5Aget_info
 *
 * Purpose:	Retrieve information about an attribute.
 *
 * Return:	Success:	Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Aget_info(hid_t attr_id, H5A_info_t *ainfo /*out*/)
{
    H5VL_object_t       *vol_obj = NULL;      /* Attribute object for ID */
    H5VL_attr_get_args_t vol_cb_args;         /* Arguments to VOL callback */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", attr_id, ainfo);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(attr_id, H5I_ATTR)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not an attribute");
    if (!ainfo)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "attribute_info parameter cannot be NULL");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                           = H5VL_ATTR_GET_INFO;
    vol_cb_args.args.get_info.loc_params.type     = H5VL_OBJECT_BY_SELF;
    vol_cb_args.args.get_info.loc_params.obj_type = H5I_get_type(attr_id);
    vol_cb_args.args.get_info.attr_name           = NULL;
    vol_cb_args.args.get_info.ainfo               = ainfo;

    /* Get the attribute information */
    if (H5VL_attr_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "unable to get attribute info");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Aget_info() */

/*-------------------------------------------------------------------------
 * Function:	H5Aget_info_by_name
 *
 * Purpose:	Retrieve information about an attribute by name.
 *
 * Return:	Success:	Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Aget_info_by_name(hid_t loc_id, const char *obj_name, const char *attr_name, H5A_info_t *ainfo /*out*/,
                    hid_t lapl_id)
{
    H5VL_object_t       *vol_obj = NULL;      /* Attribute object for ID */
    H5VL_attr_get_args_t vol_cb_args;         /* Arguments to VOL callback */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "i*s*sxi", loc_id, obj_name, attr_name, ainfo, lapl_id);

    /* Check args */
    if (H5I_ATTR == H5I_get_type(loc_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "location is not valid for an attribute");
    if (!obj_name || !*obj_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no object name");
    if (!attr_name || !*attr_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no attribute name");
    if (NULL == ainfo)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid info pointer");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&lapl_id, H5P_CLS_LACC, loc_id, false) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTSET, FAIL, "can't set access property list info");

    /* Get the object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid object identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                                               = H5VL_ATTR_GET_INFO;
    vol_cb_args.args.get_info.loc_params.type                         = H5VL_OBJECT_BY_NAME;
    vol_cb_args.args.get_info.loc_params.loc_data.loc_by_name.name    = obj_name;
    vol_cb_args.args.get_info.loc_params.loc_data.loc_by_name.lapl_id = lapl_id;
    vol_cb_args.args.get_info.loc_params.obj_type                     = H5I_get_type(loc_id);
    vol_cb_args.args.get_info.attr_name                               = attr_name;
    vol_cb_args.args.get_info.ainfo                                   = ainfo;

    /* Get the attribute information */
    if (H5VL_attr_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "unable to get attribute info");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Aget_info_by_name() */

/*-------------------------------------------------------------------------
 * Function:	H5Aget_info_by_idx
 *
 * Purpose:	Retrieve information about an attribute, according to the
 *		order within an index.
 *
 * Return:	Success:	Non-negative with information in AINFO
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Aget_info_by_idx(hid_t loc_id, const char *obj_name, H5_index_t idx_type, H5_iter_order_t order, hsize_t n,
                   H5A_info_t *ainfo /*out*/, hid_t lapl_id)
{
    H5VL_object_t       *vol_obj = NULL;      /* Attribute object for ID */
    H5VL_attr_get_args_t vol_cb_args;         /* Arguments to VOL callback */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE7("e", "i*sIiIohxi", loc_id, obj_name, idx_type, order, n, ainfo, lapl_id);

    /* Check args */
    if (H5I_ATTR == H5I_get_type(loc_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "location is not valid for an attribute");
    if (!obj_name || !*obj_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no name");
    if (idx_type <= H5_INDEX_UNKNOWN || idx_type >= H5_INDEX_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid index type specified");
    if (order <= H5_ITER_UNKNOWN || order >= H5_ITER_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid iteration order specified");
    if (NULL == ainfo)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid info pointer");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&lapl_id, H5P_CLS_LACC, loc_id, false) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTSET, FAIL, "can't set access property list info");

    /* Get the object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid object identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                                               = H5VL_ATTR_GET_INFO;
    vol_cb_args.args.get_info.loc_params.type                         = H5VL_OBJECT_BY_IDX;
    vol_cb_args.args.get_info.loc_params.loc_data.loc_by_idx.name     = obj_name;
    vol_cb_args.args.get_info.loc_params.loc_data.loc_by_idx.idx_type = idx_type;
    vol_cb_args.args.get_info.loc_params.loc_data.loc_by_idx.order    = order;
    vol_cb_args.args.get_info.loc_params.loc_data.loc_by_idx.n        = n;
    vol_cb_args.args.get_info.loc_params.loc_data.loc_by_idx.lapl_id  = lapl_id;
    vol_cb_args.args.get_info.loc_params.obj_type                     = H5I_get_type(loc_id);
    vol_cb_args.args.get_info.attr_name                               = NULL;
    vol_cb_args.args.get_info.ainfo                                   = ainfo;

    /* Get the attribute information */
    if (H5VL_attr_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "unable to get attribute info");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Aget_info_by_idx() */

/*--------------------------------------------------------------------------
 NAME
    H5A__rename_common
 PURPOSE
    Common helper routine for sync/async attribute rename operations
 RETURNS
    Non-negative on success/Negative on failure
--------------------------------------------------------------------------*/
static herr_t
H5A__rename_common(H5VL_object_t *vol_obj, H5VL_loc_params_t *loc_params, const char *old_name,
                   const char *new_name, void **token_ptr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(vol_obj);
    assert(loc_params);
    assert(old_name);
    assert(new_name);

    /* Avoid thrashing things if the names are the same */
    if (strcmp(old_name, new_name) != 0) {
        H5VL_attr_specific_args_t vol_cb_args; /* Arguments to VOL callback */

        /* Set up VOL callback arguments */
        vol_cb_args.op_type              = H5VL_ATTR_RENAME;
        vol_cb_args.args.rename.old_name = old_name;
        vol_cb_args.args.rename.new_name = new_name;

        /* Rename the attribute */
        if (H5VL_attr_specific(vol_obj, loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, token_ptr) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTRENAME, FAIL, "can't rename attribute from '%s' to '%s'", old_name,
                        new_name);
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5A__rename_common() */

/*--------------------------------------------------------------------------
 NAME
    H5A__rename_api_common
 PURPOSE
    Common helper routine for sync/async attribute rename operations
 RETURNS
    Non-negative on success/Negative on failure
--------------------------------------------------------------------------*/
static herr_t
H5A__rename_api_common(hid_t loc_id, const char *old_name, const char *new_name, void **token_ptr,
                       H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_loc_params_t loc_params;                     /* Location parameters for object access */
    herr_t            ret_value = SUCCEED;            /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (H5I_ATTR == H5I_get_type(loc_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "location is not valid for an attribute");
    if (!old_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "old attribute name cannot be NULL");
    if (!*old_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "old attribute name cannot be an empty string");
    if (!new_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "new attribute name cannot be NULL");
    if (!*new_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "new attribute name cannot be an empty string");

    /* Set up object access arguments */
    if (H5VL_setup_loc_args(loc_id, vol_obj_ptr, &loc_params) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTSET, FAIL, "can't set object access arguments");

    /* Rename the attribute */
    if (H5A__rename_common(*vol_obj_ptr, &loc_params, old_name, new_name, token_ptr) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTRENAME, FAIL, "can't rename attribute");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5A__rename_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Arename
 *
 * Purpose:     Rename an attribute
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Arename(hid_t loc_id, const char *old_name, const char *new_name)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "i*s*s", loc_id, old_name, new_name);

    /* Synchronously rename the attribute */
    if (H5A__rename_api_common(loc_id, old_name, new_name, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTRENAME, FAIL, "can't synchronously rename attribute");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Arename() */

/*--------------------------------------------------------------------------
 *  NAME
 *      H5Arename_async
 *  PURPOSE
 *      Asynchronous version of H5Arename
 *  RETURNS
 *      Non-negative on success/Negative on failure
 *--------------------------------------------------------------------------*/
herr_t
H5Arename_async(const char *app_file, const char *app_func, unsigned app_line, hid_t loc_id,
                const char *old_name, const char *new_name, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE7("e", "*s*sIui*s*si", app_file, app_func, app_line, loc_id, old_name, new_name, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Asynchronously rename the attribute */
    if (H5A__rename_api_common(loc_id, old_name, new_name, token_ptr, &vol_obj) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTRENAME, FAIL, "can't asynchronously rename attribute");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE7(__func__, "*s*sIui*s*si", app_file, app_func, app_line, loc_id, old_name, new_name, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_ATTR, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Arename_async() */

/*--------------------------------------------------------------------------
 NAME
    H5A__rename_by_name_api_common
 PURPOSE
    Common helper routine for sync/async attribute rename operations
 RETURNS
    Non-negative on success/Negative on failure
--------------------------------------------------------------------------*/
static herr_t
H5A__rename_by_name_api_common(hid_t loc_id, const char *obj_name, const char *old_name, const char *new_name,
                               hid_t lapl_id, void **token_ptr, H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_loc_params_t loc_params;                     /* Location parameters for object access */
    herr_t            ret_value = SUCCEED;            /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (H5I_ATTR == H5I_get_type(loc_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "location is not valid for an attribute");

    if (!old_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "old attribute name cannot be NULL");
    if (!*old_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "old attribute name cannot be an empty string");
    if (!new_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "new attribute name cannot be NULL");
    if (!*new_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "new attribute name cannot be an empty string");

    /* obj_name is verified in H5VL_setup_name_args() */
    /* Set up object access arguments */
    if (H5VL_setup_name_args(loc_id, obj_name, true, lapl_id, vol_obj_ptr, &loc_params) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTSET, FAIL, "can't set object access arguments");

    /* Rename the attribute */
    if (H5A__rename_common(*vol_obj_ptr, &loc_params, old_name, new_name, token_ptr) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTRENAME, FAIL, "can't rename attribute");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5A__rename_by_name_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Arename_by_name
 *
 * Purpose:     Rename an attribute
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Arename_by_name(hid_t loc_id, const char *obj_name, const char *old_attr_name, const char *new_attr_name,
                  hid_t lapl_id)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "i*s*s*si", loc_id, obj_name, old_attr_name, new_attr_name, lapl_id);

    /* Synchronously rename the attribute */
    if (H5A__rename_by_name_api_common(loc_id, obj_name, old_attr_name, new_attr_name, lapl_id, NULL, NULL) <
        0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTRENAME, FAIL, "can't synchronously rename attribute");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Arename_by_name() */

/*--------------------------------------------------------------------------
 *  NAME
 *      H5Arename_by_name_async
 *  PURPOSE
 *      Asynchronous version of H5Arename_by_name
 *  RETURNS
 *      Non-negative on success/Negative on failure
 *--------------------------------------------------------------------------*/
herr_t
H5Arename_by_name_async(const char *app_file, const char *app_func, unsigned app_line, hid_t loc_id,
                        const char *obj_name, const char *old_attr_name, const char *new_attr_name,
                        hid_t lapl_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE9("e", "*s*sIui*s*s*sii", app_file, app_func, app_line, loc_id, obj_name, old_attr_name,
             new_attr_name, lapl_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Asynchronously rename the attribute */
    if (H5A__rename_by_name_api_common(loc_id, obj_name, old_attr_name, new_attr_name, lapl_id, token_ptr,
                                       &vol_obj) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTRENAME, FAIL, "can't synchronously rename attribute");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE9(__func__, "*s*sIui*s*s*sii", app_file, app_func, app_line, loc_id, obj_name, old_attr_name, new_attr_name, lapl_id, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_ATTR, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Arename_by_name_async() */

/*--------------------------------------------------------------------------
 NAME
    H5Aiterate2
 PURPOSE
    Calls a user's function for each attribute on an object
 USAGE
    herr_t H5Aiterate2(loc_id, idx_type, order, idx, op, op_data)
        hid_t loc_id;           IN: Base location for object
        H5_index_t idx_type;    IN: Type of index to use
        H5_iter_order_t order;  IN: Order to iterate over index
        hsize_t *idx;           IN/OUT: Starting (IN) & Ending (OUT) attribute
                                    in index & order
        H5A_operator2_t op;     IN: User's function to pass each attribute to
        void *op_data;          IN/OUT: User's data to pass through to iterator
                                    operator function
 RETURNS
        Returns a negative value if an error occurs, the return value of the
    last operator if it was non-zero (which can be a negative value), or zero
    if all attributes were processed.

 DESCRIPTION
        This function iterates over the attributes of dataset or group
    specified with 'loc_id' & 'obj_name'.  For each attribute of the object,
    the 'op_data' and some additional information (specified below) are passed
    to the 'op' function.  The iteration begins with the '*idx'
    object in the group and the next attribute to be processed by the operator
    is returned in '*idx'.
        The operation receives the ID for the group or dataset being iterated
    over ('loc_id'), the name of the current attribute about the object
    ('attr_name'), the attribute's "info" struct ('ainfo') and the pointer to
    the operator data passed into H5Aiterate2 ('op_data').  The return values
    from an operator are:
        A. Zero causes the iterator to continue, returning zero when all
            attributes have been processed.
        B. Positive causes the iterator to immediately return that positive
            value, indicating short-circuit success.  The iterator can be
            restarted at the next attribute.
        C. Negative causes the iterator to immediately return that value,
            indicating failure.  The iterator can be restarted at the next
            attribute.
--------------------------------------------------------------------------*/
herr_t
H5Aiterate2(hid_t loc_id, H5_index_t idx_type, H5_iter_order_t order, hsize_t *idx /*in,out */,
            H5A_operator2_t op, void *op_data)
{
    H5VL_object_t            *vol_obj = NULL; /* Object for loc_id */
    H5VL_loc_params_t         loc_params;     /* Location parameters for object access */
    H5VL_attr_specific_args_t vol_cb_args;    /* Arguments to VOL callback */
    herr_t                    ret_value;      /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "iIiIo*hAO*x", loc_id, idx_type, order, idx, op, op_data);

    /* Check arguments */
    if (H5I_ATTR == H5I_get_type(loc_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "location is not valid for an attribute");
    if (idx_type <= H5_INDEX_UNKNOWN || idx_type >= H5_INDEX_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid index type specified");
    if (order <= H5_ITER_UNKNOWN || order >= H5_ITER_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid iteration order specified");

    /* Get the loc object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set the location access parameters */
    loc_params.type     = H5VL_OBJECT_BY_SELF;
    loc_params.obj_type = H5I_get_type(loc_id);

    /* Set up VOL callback arguments */
    vol_cb_args.op_type               = H5VL_ATTR_ITER;
    vol_cb_args.args.iterate.idx_type = idx_type;
    vol_cb_args.args.iterate.order    = order;
    vol_cb_args.args.iterate.idx      = idx;
    vol_cb_args.args.iterate.op       = op;
    vol_cb_args.args.iterate.op_data  = op_data;

    /* Iterate over attributes */
    if ((ret_value = H5VL_attr_specific(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT,
                                        H5_REQUEST_NULL)) < 0)
        HERROR(H5E_ATTR, H5E_BADITER, "error iterating over attributes");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Aiterate2() */

/*--------------------------------------------------------------------------
 NAME
    H5Aiterate_by_name
 PURPOSE
    Calls a user's function for each attribute on an object
 USAGE
    herr_t H5Aiterate2(loc_id, obj_name, idx_type, order, idx, op, op_data, lapl_id)
        hid_t loc_id;           IN: Base location for object
        const char *obj_name;   IN: Name of object relative to location
        H5_index_t idx_type;    IN: Type of index to use
        H5_iter_order_t order;  IN: Order to iterate over index
        hsize_t *idx;           IN/OUT: Starting (IN) & Ending (OUT) attribute
                                    in index & order
        H5A_operator2_t op;     IN: User's function to pass each attribute to
        void *op_data;          IN/OUT: User's data to pass through to iterator
                                    operator function
        hid_t lapl_id;          IN: Link access property list
 RETURNS
        Returns a negative value if an error occurs, the return value of the
    last operator if it was non-zero (which can be a negative value), or zero
    if all attributes were processed.

 DESCRIPTION
        This function iterates over the attributes of dataset or group
    specified with 'loc_id' & 'obj_name'.  For each attribute of the object,
    the 'op_data' and some additional information (specified below) are passed
    to the 'op' function.  The iteration begins with the '*idx'
    object in the group and the next attribute to be processed by the operator
    is returned in '*idx'.
        The operation receives the ID for the group or dataset being iterated
    over ('loc_id'), the name of the current attribute about the object
    ('attr_name'), the attribute's "info" struct ('ainfo') and the pointer to
    the operator data passed into H5Aiterate_by_name ('op_data').  The return values
    from an operator are:
        A. Zero causes the iterator to continue, returning zero when all
            attributes have been processed.
        B. Positive causes the iterator to immediately return that positive
            value, indicating short-circuit success.  The iterator can be
            restarted at the next attribute.
        C. Negative causes the iterator to immediately return that value,
            indicating failure.  The iterator can be restarted at the next
            attribute.
--------------------------------------------------------------------------*/
herr_t
H5Aiterate_by_name(hid_t loc_id, const char *obj_name, H5_index_t idx_type, H5_iter_order_t order,
                   hsize_t *idx /*in,out */, H5A_operator2_t op, void *op_data, hid_t lapl_id)
{
    H5VL_object_t            *vol_obj = NULL;      /* Object for loc_id */
    H5VL_loc_params_t         loc_params;          /* Location parameters for object access */
    H5VL_attr_specific_args_t vol_cb_args;         /* Arguments to VOL callback */
    herr_t                    ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE8("e", "i*sIiIo*hAO*xi", loc_id, obj_name, idx_type, order, idx, op, op_data, lapl_id);

    /* Check arguments */
    if (H5I_ATTR == H5I_get_type(loc_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "location is not valid for an attribute");
    if (!obj_name || !*obj_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no object name");
    if (idx_type <= H5_INDEX_UNKNOWN || idx_type >= H5_INDEX_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid index type specified");
    if (order <= H5_ITER_UNKNOWN || order >= H5_ITER_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid iteration order specified");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&lapl_id, H5P_CLS_LACC, loc_id, false) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTSET, FAIL, "can't set access property list info");

    /* get the loc object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set the location access parameters */
    loc_params.type                         = H5VL_OBJECT_BY_NAME;
    loc_params.obj_type                     = H5I_get_type(loc_id);
    loc_params.loc_data.loc_by_name.name    = obj_name;
    loc_params.loc_data.loc_by_name.lapl_id = lapl_id;

    /* Set up VOL callback arguments */
    vol_cb_args.op_type               = H5VL_ATTR_ITER;
    vol_cb_args.args.iterate.idx_type = idx_type;
    vol_cb_args.args.iterate.order    = order;
    vol_cb_args.args.iterate.idx      = idx;
    vol_cb_args.args.iterate.op       = op;
    vol_cb_args.args.iterate.op_data  = op_data;

    /* Iterate over attributes */
    if ((ret_value = H5VL_attr_specific(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT,
                                        H5_REQUEST_NULL)) < 0)
        HERROR(H5E_ATTR, H5E_BADITER, "attribute iteration failed");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Aiterate_by_name() */

/*--------------------------------------------------------------------------
 NAME
    H5Adelete
 PURPOSE
    Deletes an attribute from a location
 USAGE
    herr_t H5Adelete(loc_id, name)
        hid_t loc_id;       IN: Object (dataset or group) to have attribute deleted from
        const char *name;   IN: Name of attribute to delete
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    This function removes the named attribute from a dataset or group.
--------------------------------------------------------------------------*/
herr_t
H5Adelete(hid_t loc_id, const char *name)
{
    H5VL_object_t            *vol_obj = NULL;      /* Object for loc_id */
    H5VL_loc_params_t         loc_params;          /* Location parameters for object access */
    H5VL_attr_specific_args_t vol_cb_args;         /* Arguments to VOL callback */
    herr_t                    ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "i*s", loc_id, name);

    /* Check arguments */
    if (H5I_ATTR == H5I_get_type(loc_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "location is not valid for an attribute");
    if (!name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "name parameter cannot be NULL");
    if (!*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "name parameter cannot be an empty string");

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(loc_id) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTSET, FAIL, "can't set collective metadata read");

    /* Get the object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid object identifier");

    /* Set the location access parameters */
    loc_params.type     = H5VL_OBJECT_BY_SELF;
    loc_params.obj_type = H5I_get_type(loc_id);

    /* Set up VOL callback arguments */
    vol_cb_args.op_type       = H5VL_ATTR_DELETE;
    vol_cb_args.args.del.name = name;

    /* Delete the attribute */
    if (H5VL_attr_specific(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTDELETE, FAIL, "unable to delete attribute");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Adelete() */

/*--------------------------------------------------------------------------
 NAME
    H5Adelete_by_name
 PURPOSE
    Deletes an attribute from a location
 USAGE
    herr_t H5Adelete_by_name(loc_id, obj_name, attr_name, lapl_id)
        hid_t loc_id;           IN: Base location for object
        const char *obj_name;   IN: Name of object relative to location
        const char *attr_name;  IN: Name of attribute to delete
        hid_t lapl_id;          IN: Link access property list
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    This function removes the named attribute from an object.
--------------------------------------------------------------------------*/
herr_t
H5Adelete_by_name(hid_t loc_id, const char *obj_name, const char *attr_name, hid_t lapl_id)
{
    H5VL_object_t            *vol_obj = NULL;      /* Object for loc_id */
    H5VL_loc_params_t         loc_params;          /* Location parameters for object access */
    H5VL_attr_specific_args_t vol_cb_args;         /* Arguments to VOL callback */
    herr_t                    ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "i*s*si", loc_id, obj_name, attr_name, lapl_id);

    /* Check arguments */
    if (H5I_ATTR == H5I_get_type(loc_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "location is not valid for an attribute");
    if (!obj_name || !*obj_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no object name");
    if (!attr_name || !*attr_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no attribute name");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&lapl_id, H5P_CLS_LACC, loc_id, true) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTSET, FAIL, "can't set access property list info");

    /* Get the object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid object identifier");

    /* Set the location access parameters */
    loc_params.type                         = H5VL_OBJECT_BY_NAME;
    loc_params.loc_data.loc_by_name.name    = obj_name;
    loc_params.loc_data.loc_by_name.lapl_id = lapl_id;
    loc_params.obj_type                     = H5I_get_type(loc_id);

    /* Set up VOL callback arguments */
    vol_cb_args.op_type       = H5VL_ATTR_DELETE;
    vol_cb_args.args.del.name = attr_name;

    /* Delete the attribute */
    if (H5VL_attr_specific(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTDELETE, FAIL, "unable to delete attribute");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Adelete_by_name() */

/*--------------------------------------------------------------------------
 NAME
    H5Adelete_by_idx
 PURPOSE
    Deletes an attribute from a location, according to the order within an index
 USAGE
    herr_t H5Adelete_by_idx(loc_id, obj_name, idx_type, order, n, lapl_id)
        hid_t loc_id;           IN: Base location for object
        const char *obj_name;   IN: Name of object relative to location
        H5_index_t idx_type;    IN: Type of index to use
        H5_iter_order_t order;  IN: Order to iterate over index
        hsize_t n;              IN: Offset within index
        hid_t lapl_id;          IN: Link access property list
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
        This function removes an attribute from an object, using the IDX_TYPE
    index to delete the N'th attribute in ORDER direction in the index.  The
    object is specified relative to the LOC_ID with the OBJ_NAME path.  To
    remove an attribute on the object specified by LOC_ID, pass in "." for
    OBJ_NAME.  The link access property list, LAPL_ID, controls aspects of
    the group hierarchy traversal when using the OBJ_NAME to locate the final
    object to operate on.
--------------------------------------------------------------------------*/
herr_t
H5Adelete_by_idx(hid_t loc_id, const char *obj_name, H5_index_t idx_type, H5_iter_order_t order, hsize_t n,
                 hid_t lapl_id)
{
    H5VL_object_t            *vol_obj = NULL;      /* Object for loc_id */
    H5VL_loc_params_t         loc_params;          /* Location parameters for object access */
    H5VL_attr_specific_args_t vol_cb_args;         /* Arguments to VOL callback */
    herr_t                    ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "i*sIiIohi", loc_id, obj_name, idx_type, order, n, lapl_id);

    /* check arguments */
    if (H5I_ATTR == H5I_get_type(loc_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "location is not valid for an attribute");
    if (!obj_name || !*obj_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no object name");
    if (idx_type <= H5_INDEX_UNKNOWN || idx_type >= H5_INDEX_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid index type specified");
    if (order <= H5_ITER_UNKNOWN || order >= H5_ITER_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid iteration order specified");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&lapl_id, H5P_CLS_LACC, loc_id, true) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTSET, FAIL, "can't set access property list info");

    /* get the object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid object identifier");

    /* Set the location access parameters */
    loc_params.type                         = H5VL_OBJECT_BY_NAME;
    loc_params.loc_data.loc_by_name.name    = obj_name;
    loc_params.loc_data.loc_by_name.lapl_id = lapl_id;
    loc_params.obj_type                     = H5I_get_type(loc_id);

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                     = H5VL_ATTR_DELETE_BY_IDX;
    vol_cb_args.args.delete_by_idx.idx_type = idx_type;
    vol_cb_args.args.delete_by_idx.order    = order;
    vol_cb_args.args.delete_by_idx.n        = n;

    /* Delete the attribute */
    if (H5VL_attr_specific(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTDELETE, FAIL, "unable to delete attribute");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Adelete_by_idx() */

/*-------------------------------------------------------------------------
 * Function:    H5Aclose
 *
 * Purpose:     Closes access to an attribute and releases resources used by
 *              it. It is illegal to subsequently use that same dataset
 *              ID in calls to other attribute functions.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Aclose(hid_t attr_id)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", attr_id);

    /* Check arguments */
    if (H5I_ATTR != H5I_get_type(attr_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not an attribute ID");

    /* Decrement the counter on the attribute ID. It will be freed if the count
     * reaches zero.
     */
    if (H5I_dec_app_ref(attr_id) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTDEC, FAIL, "decrementing attribute ID failed");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Aclose() */

/*-------------------------------------------------------------------------
 * Function:    H5Aclose_async
 *
 * Purpose:     Asynchronous version of H5Aclose
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Aclose_async(const char *app_file, const char *app_func, unsigned app_line, hid_t attr_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    H5VL_t        *connector = NULL;            /* VOL connector */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "*s*sIuii", app_file, app_func, app_line, attr_id, es_id);

    /* Check arguments */
    if (H5I_ATTR != H5I_get_type(attr_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a attribute ID");

    /* Prepare for possible asynchronous operation */
    if (H5ES_NONE != es_id) {
        /* Get attribute object's connector */
        if (NULL == (vol_obj = H5VL_vol_object(attr_id)))
            HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't get VOL object for attribute");

        /* Increase connector's refcount, so it doesn't get closed if closing
         * the attribute closes the file */
        connector = vol_obj->connector;
        H5VL_conn_inc_rc(connector);

        /* Point at token for operation to set up */
        token_ptr = &token;
    } /* end if */

    /* Decrement the counter on the attribute ID. It will be freed if the count
     * reaches zero.
     */
    if (H5I_dec_app_ref_async(attr_id, token_ptr) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTDEC, FAIL, "decrementing attribute ID failed");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE5(__func__, "*s*sIuii", app_file, app_func, app_line, attr_id, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_ATTR, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    if (connector && H5VL_conn_dec_rc(connector) < 0)
        HDONE_ERROR(H5E_ATTR, H5E_CANTDEC, FAIL, "can't decrement ref count on connector");

    FUNC_LEAVE_API(ret_value)
} /* H5Aclose_async() */

/*--------------------------------------------------------------------------
 *  NAME
 *      H5A__exists_common
 *  PURPOSE
 *      Common helper routine for sync/async check if an attribute exists
 *  RETURNS
 *      Non-negative on success/Negative on failure
 *--------------------------------------------------------------------------*/
static herr_t
H5A__exists_common(H5VL_object_t *vol_obj, H5VL_loc_params_t *loc_params, const char *attr_name,
                   bool *attr_exists, void **token_ptr)
{
    H5VL_attr_specific_args_t vol_cb_args;         /* Arguments to VOL callback */
    herr_t                    ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(vol_obj);
    assert(loc_params);

    /* Check arguments */
    if (!attr_name || !*attr_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no attribute name");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type            = H5VL_ATTR_EXISTS;
    vol_cb_args.args.exists.name   = attr_name;
    vol_cb_args.args.exists.exists = attr_exists;

    /* Check if the attribute exists */
    if (H5VL_attr_specific(vol_obj, loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, token_ptr) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "unable to determine if attribute exists");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5A__exists_common() */

/*--------------------------------------------------------------------------
 *  NAME
 *      H5A__exists_api_common
 *  PURPOSE
 *      Common helper routine for sync/async check if an attribute exists
 *  RETURNS
 *      Non-negative on success/Negative on failure
 *--------------------------------------------------------------------------*/
static herr_t
H5A__exists_api_common(hid_t obj_id, const char *attr_name, bool *attr_exists, void **token_ptr,
                       H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_loc_params_t loc_params;                     /* Location parameters for object access */
    herr_t            ret_value = SUCCEED;            /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (H5I_ATTR == H5I_get_type(obj_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "location is not valid for an attribute");
    if (!attr_name || !*attr_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no attribute name");
    if (NULL == attr_exists)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid pointer for attribute existence");

    /* Set up object access arguments */
    if (H5VL_setup_self_args(obj_id, vol_obj_ptr, &loc_params) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTSET, FAIL, "can't set object access arguments");

    /* Check if the attribute exists */
    if (H5A__exists_common(*vol_obj_ptr, &loc_params, attr_name, attr_exists, token_ptr) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "unable to determine if attribute exists");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5A__exists_api_common() */

/*-------------------------------------------------------------------------
 * Function:	H5Aexists
 *
 * Purpose:	Checks if an attribute with a given name exists on an opened
 *              object.
 *
 * Return:	Success:	true/false
 * 		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5Aexists(hid_t obj_id, const char *attr_name)
{
    bool   exists;           /* Flag for attribute existence */
    htri_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("t", "i*s", obj_id, attr_name);

    /* Synchronously check if an attribute exists */
    exists = false;
    if (H5A__exists_api_common(obj_id, attr_name, &exists, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't synchronously check if attribute exists");

    /* Set return value */
    ret_value = (htri_t)exists;

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Aexists() */

/*--------------------------------------------------------------------------
 * NAME
 *      H5Aexists_async
 * PURPOSE
 *      Asynchronous version of H5Aexists
 * RETURNS
 *      Non-negative on success/Negative on failure
 *--------------------------------------------------------------------------*/
herr_t
H5Aexists_async(const char *app_file, const char *app_func, unsigned app_line, hid_t obj_id,
                const char *attr_name, hbool_t *attr_exists, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE7("e", "*s*sIui*s*bi", app_file, app_func, app_line, obj_id, attr_name, attr_exists, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Asynchronously check if an attribute exists */
    if (H5A__exists_api_common(obj_id, attr_name, attr_exists, token_ptr, &vol_obj) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't asynchronously check if attribute exists");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE7(__func__, "*s*sIui*s*bi", app_file, app_func, app_line, obj_id, attr_name, attr_exists, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_ATTR, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Aexists_async() */

/*--------------------------------------------------------------------------
 *  NAME
 *      H5A__exists_by_name_api_common
 *  PURPOSE
 *      Common helper routine for sync/async check if an attribute exists
 *  RETURNS
 *      Non-negative on success/Negative on failure
 *--------------------------------------------------------------------------*/
static herr_t
H5A__exists_by_name_api_common(hid_t loc_id, const char *obj_name, const char *attr_name, bool *attr_exists,
                               hid_t lapl_id, void **token_ptr, H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_loc_params_t loc_params;                     /* Location parameters for object access */
    herr_t            ret_value = SUCCEED;            /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (H5I_ATTR == H5I_get_type(loc_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "location is not valid for an attribute");
    if (!attr_name || !*attr_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no attribute name");
    if (NULL == attr_exists)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid pointer for attribute existence");

    /* obj_name is verified in H5VL_setup_name_args() */
    /* Set up object access arguments */
    if (H5VL_setup_name_args(loc_id, obj_name, false, lapl_id, vol_obj_ptr, &loc_params) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTSET, FAIL, "can't set object access arguments");

    /* Check if the attribute exists */
    if (H5A__exists_common(*vol_obj_ptr, &loc_params, attr_name, attr_exists, token_ptr) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "unable to determine if attribute exists");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5A__exists_by_name_api_common() */

/*-------------------------------------------------------------------------
 * Function:	H5Aexists_by_name
 *
 * Purpose:	Checks if an attribute with a given name exists on an object.
 *
 * Return:	Success:	true/false
 * 		    Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5Aexists_by_name(hid_t loc_id, const char *obj_name, const char *attr_name, hid_t lapl_id)
{
    bool   exists;           /* Flag for attribute existence */
    htri_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("t", "i*s*si", loc_id, obj_name, attr_name, lapl_id);

    /* Synchronously check if an attribute exists */
    exists = false;
    if (H5A__exists_by_name_api_common(loc_id, obj_name, attr_name, &exists, lapl_id, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't synchronously determine if attribute exists by name");

    /* Set return value */
    ret_value = (htri_t)exists;

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Aexists_by_name() */

/*--------------------------------------------------------------------------
 * NAME
 *      H5Aexists_by_name_async
 * PURPOSE
 *      Asynchronous version of H5Aexists_by_name
 * RETURNS
 *      Non-negative on success/Negative on failure
 *--------------------------------------------------------------------------*/
herr_t
H5Aexists_by_name_async(const char *app_file, const char *app_func, unsigned app_line, hid_t loc_id,
                        const char *obj_name, const char *attr_name, hbool_t *attr_exists, hid_t lapl_id,
                        hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE9("e", "*s*sIui*s*s*bii", app_file, app_func, app_line, loc_id, obj_name, attr_name, attr_exists,
             lapl_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Asynchronously check if an attribute exists */
    if (H5A__exists_by_name_api_common(loc_id, obj_name, attr_name, attr_exists, lapl_id, token_ptr,
                                       &vol_obj) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL,
                    "can't asynchronously determine if attribute exists by name");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE9(__func__, "*s*sIui*s*s*bii", app_file, app_func, app_line, loc_id, obj_name, attr_name, attr_exists, lapl_id, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_ATTR, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Aexists_by_name_async() */
