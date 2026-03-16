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
 * Created:	H5Tdeprec.c
 *
 * Purpose:	Deprecated functions from the H5T interface.  These
 *              functions are here for compatibility purposes and may be
 *              removed in the future.  Applications should switch to the
 *              newer APIs.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Tmodule.h" /* This source code file is part of the H5T module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5ACprivate.h" /* Metadata cache                       */
#include "H5CXprivate.h" /* API Contexts                         */
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5FOprivate.h" /* File objects				*/
#include "H5Iprivate.h"  /* IDs					*/
#include "H5Ppublic.h"   /* Property Lists			*/
#include "H5Tpkg.h"      /* Datatypes				*/
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

/* Declare a free list to manage the H5VL_t struct */
H5FL_EXTERN(H5VL_t);

/* Declare a free list to manage the H5VL_object_t struct */
H5FL_EXTERN(H5VL_object_t);

#ifndef H5_NO_DEPRECATED_SYMBOLS

/*-------------------------------------------------------------------------
 * Function:	H5Tcommit1
 *
 * Purpose:	Save a transient datatype to a file and turn the type handle
 *		into a named, immutable type.
 *
 * Note:	Deprecated in favor of H5Tcommit2
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Tcommit1(hid_t loc_id, const char *name, hid_t type_id)
{
    void             *data    = NULL; /* VOL-managed datatype data */
    H5VL_object_t    *new_obj = NULL; /* VOL object that holds the datatype object and the VOL info */
    H5T_t            *dt      = NULL; /* High level datatype object that wraps the VOL object */
    H5VL_object_t    *vol_obj = NULL; /* Object of loc_id */
    H5VL_loc_params_t loc_params;
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "i*si", loc_id, name, type_id);

    /* Check arguments */
    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no name");
    if (NULL == (dt = (H5T_t *)H5I_object_verify(type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    if (H5T_is_named(dt))
        HGOTO_ERROR(H5E_ARGS, H5E_CANTSET, FAIL, "datatype is already committed");

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(loc_id) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTSET, FAIL, "can't set access property list info");

    loc_params.type     = H5VL_OBJECT_BY_SELF;
    loc_params.obj_type = H5I_get_type(loc_id);

    /* get the object from the loc_id */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid object identifier");

    /* Commit the datatype */
    if (NULL == (data = H5VL_datatype_commit(vol_obj, &loc_params, name, type_id, H5P_LINK_CREATE_DEFAULT,
                                             H5P_DATATYPE_CREATE_DEFAULT, H5P_DATATYPE_ACCESS_DEFAULT,
                                             H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to commit datatype");

    /* Set up VOL object */
    if (NULL == (new_obj = H5VL_create_object(data, vol_obj->connector)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTALLOC, FAIL, "can't create VOL object for committed datatype");

    /* Set the committed type object to the VOL connector pointer in the H5T_t struct */
    dt->vol_obj = new_obj;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Tcommit1() */

/*-------------------------------------------------------------------------
 * Function:	H5Topen1
 *
 * Purpose:	Opens a named datatype.
 *
 * Note:	Deprecated in favor of H5Topen2
 *
 * Return:	Success:	Object ID of the named datatype.
 *
 *		Failure:	H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Topen1(hid_t loc_id, const char *name)
{
    void             *dt      = NULL; /* Datatype object created by VOL connector */
    H5VL_object_t    *vol_obj = NULL; /* Object of loc_id */
    H5VL_loc_params_t loc_params;
    hid_t             ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE2("i", "i*s", loc_id, name);

    /* Check args */
    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "no name");

    loc_params.type     = H5VL_OBJECT_BY_SELF;
    loc_params.obj_type = H5I_get_type(loc_id);

    /* Get the location object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid location identifier");

    /* Open the datatype */
    if (NULL == (dt = H5VL_datatype_open(vol_obj, &loc_params, name, H5P_DATATYPE_ACCESS_DEFAULT,
                                         H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL)))
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTOPENOBJ, H5I_INVALID_HID, "unable to open named datatype");

    /* Register the type and return the ID */
    if ((ret_value = H5VL_register(H5I_DATATYPE, dt, vol_obj->connector, true)) < 0)
        HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register named datatype");

done:
    /* Cleanup on error */
    if (H5I_INVALID_HID == ret_value)
        if (dt && H5VL_datatype_close(vol_obj, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HDONE_ERROR(H5E_DATATYPE, H5E_CLOSEERROR, H5I_INVALID_HID, "unable to close datatype");

    FUNC_LEAVE_API(ret_value)
} /* end H5Topen1() */
#endif /* H5_NO_DEPRECATED_SYMBOLS */
