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
 * Created:	H5Adeprec.c
 *
 * Purpose:	Deprecated functions from the H5A interface.  These
 *              functions are here for compatibility purposes and may be
 *              removed in the future.  Applications should switch to the
 *              newer APIs.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Amodule.h" /* This source code file is part of the H5A module */
#define H5O_FRIEND     /*suppress error about including H5Opkg	*/

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5Apkg.h"      /* Attributes                               */
#include "H5CXprivate.h" /* API Contexts                             */
#include "H5Dprivate.h"  /* Datasets                                 */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5Opkg.h"      /* Object headers                           */
#include "H5VLprivate.h" /* Virtual object layer                     */

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

/*--------------------------------------------------------------------------
 NAME
    H5Acreate1
 PURPOSE
    Creates an attribute on an object
 USAGE
    hid_t H5Acreate1(loc_id, name, type_id, space_id, acpl_id)
        hid_t loc_id;       IN: Object (dataset or group) to be attached to
        const char *name;   IN: Name of attribute to create
        hid_t type_id;      IN: ID of datatype for attribute
        hid_t space_id;     IN: ID of dataspace for attribute
        hid_t acpl_id;      IN: ID of creation property list (currently not used)
 RETURNS
    Non-negative on success/H5I_INVALID_HID on failure

 DESCRIPTION
        This function creates an attribute which is attached to the object
    specified with 'location_id'.  The name specified with 'name' for each
    attribute for an object must be unique for that object.  The 'type_id'
    and 'space_id' are created with the H5T and H5S interfaces respectively.
    The attribute ID returned from this function must be released with H5Aclose
    or resource leaks will develop.

 NOTE
    Deprecated in favor of H5Acreate2

--------------------------------------------------------------------------*/
hid_t
H5Acreate1(hid_t loc_id, const char *name, hid_t type_id, hid_t space_id, hid_t acpl_id)
{
    void             *attr    = NULL; /* attr object from VOL connector */
    H5VL_object_t    *vol_obj = NULL; /* Object of loc_id */
    H5VL_loc_params_t loc_params;
    hid_t             ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE5("i", "i*siii", loc_id, name, type_id, space_id, acpl_id);

    /* Check arguments */
    if (H5I_ATTR == H5I_get_type(loc_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "location is not valid for an attribute");
    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "no name");

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(loc_id) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTSET, H5I_INVALID_HID, "can't set collective metadata read");

    /* Get correct property list */
    if (H5P_DEFAULT == acpl_id)
        acpl_id = H5P_ATTRIBUTE_CREATE_DEFAULT;

    /* Set location parameters */
    loc_params.type     = H5VL_OBJECT_BY_SELF;
    loc_params.obj_type = H5I_get_type(loc_id);

    /* Get the location object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid location identifier");

    /* Create the attribute */
    if (NULL ==
        (attr = H5VL_attr_create(vol_obj, &loc_params, name, type_id, space_id, acpl_id,
                                 H5P_ATTRIBUTE_ACCESS_DEFAULT, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL)))
        HGOTO_ERROR(H5E_ATTR, H5E_CANTINIT, H5I_INVALID_HID, "unable to create attribute");

    /* Register the new attribute and get an ID for it */
    if ((ret_value = H5VL_register(H5I_ATTR, attr, vol_obj->connector, true)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register attribute for ID");

done:
    /* Clean up on failure */
    if (H5I_INVALID_HID == ret_value)
        if (attr && H5VL_attr_close(vol_obj, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, H5I_INVALID_HID, "can't close attribute");

    FUNC_LEAVE_API(ret_value)
} /* end H5Acreate1() */

/*--------------------------------------------------------------------------
 NAME
    H5Aopen_name
 PURPOSE
    Opens an attribute for an object by looking up the attribute name
 USAGE
    hid_t H5Aopen_name (loc_id, name)
        hid_t loc_id;       IN: Object (dataset or group) to be attached to
        const char *name;   IN: Name of attribute to locate and open
 RETURNS
    ID of attribute on success, H5I_INVALID_HID on failure

 DESCRIPTION
        This function opens an existing attribute for access.  The attribute
    name specified is used to look up the corresponding attribute for the
    object.  The attribute ID returned from this function must be released with
    H5Aclose or resource leaks will develop.
        The location object may be either a group or a dataset, both of
    which may have any sort of attribute.
 NOTE
    Deprecated in favor of H5Aopen
--------------------------------------------------------------------------*/
hid_t
H5Aopen_name(hid_t loc_id, const char *name)
{
    void             *attr    = NULL; /* attr object from VOL connector */
    H5VL_object_t    *vol_obj = NULL; /* Object of loc_id */
    H5VL_loc_params_t loc_params;
    hid_t             ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE2("i", "i*s", loc_id, name);

    /* Check arguments */
    if (H5I_ATTR == H5I_get_type(loc_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "location is not valid for an attribute");
    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "no name");

    /* Set location parameters */
    loc_params.type     = H5VL_OBJECT_BY_SELF;
    loc_params.obj_type = H5I_get_type(loc_id);

    /* Get the location object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid location identifier");

    /* Open the attribute */
    if (NULL == (attr = H5VL_attr_open(vol_obj, &loc_params, name, H5P_ATTRIBUTE_ACCESS_DEFAULT,
                                       H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL)))
        HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, H5I_INVALID_HID, "unable to open attribute");

    /* Register the attribute and get an ID for it */
    if ((ret_value = H5VL_register(H5I_ATTR, attr, vol_obj->connector, true)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register attribute handle");

done:
    /* Clean up on failure */
    if (H5I_INVALID_HID == ret_value)
        if (attr && H5VL_attr_close(vol_obj, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, H5I_INVALID_HID, "can't close attribute");

    FUNC_LEAVE_API(ret_value)
} /* H5Aopen_name() */

/*--------------------------------------------------------------------------
 NAME
    H5Aopen_idx
 PURPOSE
    Opens the n'th attribute for an object
 USAGE
    hid_t H5Aopen_idx (loc_id, idx)
        hid_t loc_id;       IN: Object that attribute is attached to
        unsigned idx;       IN: Index (0-based) attribute to open
 RETURNS
    ID of attribute on success, H5I_INVALID_HID on failure

 DESCRIPTION
        This function opens an existing attribute for access.  The attribute
    index specified is used to look up the corresponding attribute for the
    object.  The attribute ID returned from this function must be released with
    H5Aclose or resource leaks will develop.
        The location object may be either a group or a dataset, both of
    which may have any sort of attribute.
 NOTE
    Deprecated in favor of H5Aopen_by_idx
--------------------------------------------------------------------------*/
hid_t
H5Aopen_idx(hid_t loc_id, unsigned idx)
{
    void             *attr    = NULL; /* attr object from VOL connector */
    H5VL_object_t    *vol_obj = NULL; /* Object of loc_id */
    H5VL_loc_params_t loc_params;
    hid_t             ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE2("i", "iIu", loc_id, idx);

    /* Check arguments */
    if (H5I_ATTR == H5I_get_type(loc_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "location is not valid for an attribute");

    /* Set location parameters */
    loc_params.type                         = H5VL_OBJECT_BY_IDX;
    loc_params.loc_data.loc_by_idx.name     = ".";
    loc_params.loc_data.loc_by_idx.idx_type = H5_INDEX_CRT_ORDER;
    loc_params.loc_data.loc_by_idx.order    = H5_ITER_INC;
    loc_params.loc_data.loc_by_idx.n        = (hsize_t)idx;
    loc_params.loc_data.loc_by_idx.lapl_id  = H5P_LINK_ACCESS_DEFAULT;
    loc_params.obj_type                     = H5I_get_type(loc_id);

    /* Get the location object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid location identifier");

    /* Open the attribute */
    if (NULL == (attr = H5VL_attr_open(vol_obj, &loc_params, NULL, H5P_ATTRIBUTE_ACCESS_DEFAULT,
                                       H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL)))
        HGOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, H5I_INVALID_HID, "unable to open attribute");

    /* Register the attribute and get an ID for it */
    if ((ret_value = H5VL_register(H5I_ATTR, attr, vol_obj->connector, true)) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register attribute handle");

done:
    /* Clean up on failure */
    if (H5I_INVALID_HID == ret_value)
        if (attr && H5VL_attr_close(vol_obj, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HDONE_ERROR(H5E_ATTR, H5E_CLOSEERROR, H5I_INVALID_HID, "can't close attribute");

    FUNC_LEAVE_API(ret_value)
} /* H5Aopen_idx() */

/*--------------------------------------------------------------------------
 NAME
    H5Aget_num_attrs
 PURPOSE
    Determines the number of attributes attached to an object
 NOTE
    Deprecated in favor of H5Oget_info[_by_idx]
 USAGE
    int H5Aget_num_attrs (loc_id)
        hid_t loc_id;       IN: Object (dataset or group) to be queried
 RETURNS
    Number of attributes on success, negative on failure
 DESCRIPTION
        This function returns the number of attributes attached to a dataset or
    group, 'location_id'.
 NOTE
    Deprecated in favor of H5Oget_info
--------------------------------------------------------------------------*/
int
H5Aget_num_attrs(hid_t loc_id)
{
    H5VL_object_t         *vol_obj = NULL; /* Object of loc_id */
    H5VL_object_get_args_t vol_cb_args;    /* Arguments to VOL callback */
    H5VL_loc_params_t      loc_params;
    H5O_info2_t            oinfo;
    int                    ret_value = -1;

    FUNC_ENTER_API((-1))
    H5TRACE1("Is", "i", loc_id);

    loc_params.type     = H5VL_OBJECT_BY_SELF;
    loc_params.obj_type = H5I_get_type(loc_id);

    /* Get the location object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, (-1), "invalid location identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type              = H5VL_OBJECT_GET_INFO;
    vol_cb_args.args.get_info.oinfo  = &oinfo;
    vol_cb_args.args.get_info.fields = H5O_INFO_NUM_ATTRS;

    /* Get the number of attributes for the object */
    if (H5VL_object_get(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, (-1), "unable to get attribute count for object");

    H5_CHECKED_ASSIGN(ret_value, int, oinfo.num_attrs, hsize_t);

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Aget_num_attrs() */

/*--------------------------------------------------------------------------
 NAME
    H5Aiterate1
 PURPOSE
    Calls a user's function for each attribute on an object
 USAGE
    herr_t H5Aiterate1(loc_id, attr_num, op, data)
        hid_t loc_id;       IN: Object (dataset or group) to be iterated over
        unsigned *attr_num; IN/OUT: Starting (IN) & Ending (OUT) attribute number
        H5A_operator1_t op;  IN: User's function to pass each attribute to
        void *op_data;      IN/OUT: User's data to pass through to iterator operator function
 RETURNS
        Returns a negative value if something is wrong, the return value of the
    last operator if it was non-zero, or zero if all attributes were processed.

 DESCRIPTION
        This function iterates over the attributes of dataset or group
    specified with 'loc_id'.  For each attribute of the object, the
    'op_data' and some additional information (specified below) are passed
    to the 'op' function.  The iteration begins with the '*attr_number'
    object in the group and the next attribute to be processed by the operator
    is returned in '*attr_number'.
        The operation receives the ID for the group or dataset being iterated
    over ('loc_id'), the name of the current attribute about the object
    ('attr_name') and the pointer to the operator data passed in to H5Aiterate
    ('op_data').  The return values from an operator are:
        A. Zero causes the iterator to continue, returning zero when all
            attributes have been processed.
        B. Positive causes the iterator to immediately return that positive
            value, indicating short-circuit success.  The iterator can be
            restarted at the next attribute.
        C. Negative causes the iterator to immediately return that value,
            indicating failure.  The iterator can be restarted at the next
            attribute.
 NOTE
    Deprecated in favor of H5Aiterate2
--------------------------------------------------------------------------*/
herr_t
H5Aiterate1(hid_t loc_id, unsigned *attr_num /*in,out*/, H5A_operator1_t op, void *op_data)
{
    H5VL_object_t                   *vol_obj = NULL; /* Object of loc_id */
    H5VL_optional_args_t             vol_cb_args;    /* Arguments to VOL callback */
    H5VL_native_attr_optional_args_t attr_opt_args;  /* Arguments for optional operation */
    herr_t                           ret_value;      /* Return value */

    FUNC_ENTER_API(H5_ITER_ERROR)
    H5TRACE4("e", "i*IuAo*x", loc_id, attr_num, op, op_data);

    /* check arguments */
    if (H5I_ATTR == H5I_get_type(loc_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5_ITER_ERROR, "location is not valid for an attribute");

    /* Get the location object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ATTR, H5E_BADTYPE, H5_ITER_ERROR, "invalid location identifier");

    /* Set up VOL callback arguments */
    attr_opt_args.iterate_old.loc_id   = loc_id;
    attr_opt_args.iterate_old.attr_num = attr_num;
    attr_opt_args.iterate_old.op       = op;
    attr_opt_args.iterate_old.op_data  = op_data;
    vol_cb_args.op_type                = H5VL_NATIVE_ATTR_ITERATE_OLD;
    vol_cb_args.args                   = &attr_opt_args;

    /* Call attribute iteration routine */
    if ((ret_value = H5VL_attr_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL)) <
        0)
        HERROR(H5E_ATTR, H5E_BADITER, "error iterating over attributes");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Aiterate1() */
#endif /* H5_NO_DEPRECATED_SYMBOLS */
