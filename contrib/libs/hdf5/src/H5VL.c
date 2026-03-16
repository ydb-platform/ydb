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

/*
 * Purpose:     The Virtual Object Layer as described in documentation.
 *              The purpose is to provide an abstraction on how to access the
 *              underlying HDF5 container, whether in a local file with
 *              a specific file format, or remotely on other machines, etc...
 */

/****************/
/* Module Setup */
/****************/

#include "H5VLmodule.h" /* This source code file is part of the H5VL module */

/***********/
/* Headers */
/***********/

#include "H5private.h"   /* Generic Functions                    */
#include "H5CXprivate.h" /* API Contexts                         */
#include "H5Eprivate.h"  /* Error handling                       */
#include "H5Iprivate.h"  /* IDs                                  */
#include "H5Pprivate.h"  /* Property lists                       */
#include "H5Tprivate.h"  /* Datatypes                            */
#include "H5VLpkg.h"     /* Virtual Object Layer                 */

/* VOL connectors */
#include "H5VLnative.h" /* Native VOL connector                 */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

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
 * Function:    H5VLregister_connector
 *
 * Purpose:     Registers a new VOL connector as a member of the virtual object
 *              layer class.
 *
 *              VIPL_ID is a VOL initialization property list which must be
 *              created with H5Pcreate(H5P_VOL_INITIALIZE) (or H5P_DEFAULT).
 *
 * Return:      Success:    A VOL connector ID which is good until the
 *                          library is closed or the connector is
 *                          unregistered.
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5VLregister_connector(const H5VL_class_t *cls, hid_t vipl_id)
{
    hid_t ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE2("i", "*#i", cls, vipl_id);

    /* Check VOL initialization property list */
    if (H5P_DEFAULT == vipl_id)
        vipl_id = H5P_VOL_INITIALIZE_DEFAULT;
    else if (true != H5P_isa_class(vipl_id, H5P_VOL_INITIALIZE))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not a VOL initialize property list");

    /* Register connector */
    if ((ret_value = H5VL__register_connector_by_class(cls, true, vipl_id)) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register VOL connector");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5VLregister_connector() */

/*-------------------------------------------------------------------------
 * Function:    H5VLregister_connector_by_name
 *
 * Purpose:     Registers a new VOL connector as a member of the virtual object
 *              layer class.
 *
 *              VIPL_ID is a VOL initialization property list which must be
 *              created with H5Pcreate(H5P_VOL_INITIALIZE) (or H5P_DEFAULT).
 *
 * Return:      Success:    A VOL connector ID which is good until the
 *                          library is closed or the connector is
 *                          unregistered.
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5VLregister_connector_by_name(const char *name, hid_t vipl_id)
{
    hid_t ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE2("i", "*si", name, vipl_id);

    /* Check arguments */
    if (!name)
        HGOTO_ERROR(H5E_ARGS, H5E_UNINITIALIZED, H5I_INVALID_HID, "null VOL connector name is disallowed");
    if (0 == strlen(name))
        HGOTO_ERROR(H5E_ARGS, H5E_UNINITIALIZED, H5I_INVALID_HID,
                    "zero-length VOL connector name is disallowed");

    /* Check VOL initialization property list */
    if (H5P_DEFAULT == vipl_id)
        vipl_id = H5P_VOL_INITIALIZE_DEFAULT;
    else if (true != H5P_isa_class(vipl_id, H5P_VOL_INITIALIZE))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not a VOL initialize property list");

    /* Register connector */
    if ((ret_value = H5VL__register_connector_by_name(name, true, vipl_id)) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register VOL connector");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5VLregister_connector_by_name() */

/*-------------------------------------------------------------------------
 * Function:    H5VLregister_connector_by_value
 *
 * Purpose:     Registers a new VOL connector as a member of the virtual object
 *              layer class.
 *
 *              VIPL_ID is a VOL initialization property list which must be
 *              created with H5Pcreate(H5P_VOL_INITIALIZE) (or H5P_DEFAULT).
 *
 * Return:      Success:    A VOL connector ID which is good until the
 *                          library is closed or the connector is
 *                          unregistered.
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5VLregister_connector_by_value(H5VL_class_value_t value, hid_t vipl_id)
{
    hid_t ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE2("i", "VCi", value, vipl_id);

    /* Check arguments */
    if (value < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_UNINITIALIZED, H5I_INVALID_HID,
                    "negative VOL connector value is disallowed");

    /* Check VOL initialization property list */
    if (H5P_DEFAULT == vipl_id)
        vipl_id = H5P_VOL_INITIALIZE_DEFAULT;
    else if (true != H5P_isa_class(vipl_id, H5P_VOL_INITIALIZE))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not a VOL initialize property list");

    /* Register connector */
    if ((ret_value = H5VL__register_connector_by_value(value, true, vipl_id)) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register VOL connector");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5VLregister_connector_by_value() */

/*-------------------------------------------------------------------------
 * Function:    H5VLis_connector_registered_by_name
 *
 * Purpose:     Tests whether a VOL class has been registered or not
 *              according to a supplied connector name.
 *
 * Return:      >0 if a VOL connector with that name has been registered
 *              0 if a VOL connector with that name has NOT been registered
 *              <0 on errors
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5VLis_connector_registered_by_name(const char *name)
{
    htri_t ret_value = false; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("t", "*s", name);

    /* Check if connector with this name is registered */
    if ((ret_value = H5VL__is_connector_registered_by_name(name)) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't check for VOL");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5VLis_connector_registered_by_name() */

/*-------------------------------------------------------------------------
 * Function:    H5VLis_connector_registered_by_value
 *
 * Purpose:     Tests whether a VOL class has been registered or not
 *              according to a supplied connector value (ID).
 *
 * Return:      >0 if a VOL connector with that value has been registered
 *              0 if a VOL connector with that value hasn't been registered
 *              <0 on errors
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5VLis_connector_registered_by_value(H5VL_class_value_t connector_value)
{
    htri_t ret_value = false;

    FUNC_ENTER_API(FAIL)
    H5TRACE1("t", "VC", connector_value);

    /* Check if connector with this value is registered */
    if ((ret_value = H5VL__is_connector_registered_by_value(connector_value)) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't check for VOL");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5VLis_connector_registered_by_value() */

/*-------------------------------------------------------------------------
 * Function:    H5VLget_connector_id
 *
 * Purpose:     Retrieves the VOL connector ID for a given object ID.
 *
 * Return:      A valid VOL connector ID. This ID will need to be closed
 *              using H5VLclose().
 *
 *              H5I_INVALID_HID on error.
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5VLget_connector_id(hid_t obj_id)
{
    hid_t ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE1("i", "i", obj_id);

    /* Get connector ID */
    if ((ret_value = H5VL__get_connector_id(obj_id, true)) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTGET, H5I_INVALID_HID, "can't get VOL id");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5VLget_connector_id() */

/*-------------------------------------------------------------------------
 * Function:    H5VLget_connector_id_by_name
 *
 * Purpose:     Retrieves the ID for a registered VOL connector.
 *
 * Return:      A valid VOL connector ID if a connector by that name has
 *              been registered. This ID will need to be closed using
 *              H5VLclose().
 *
 *              H5I_INVALID_HID on error or if a VOL connector of that
 *              name has not been registered.
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5VLget_connector_id_by_name(const char *name)
{
    hid_t ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE1("i", "*s", name);

    /* Get connector ID with this name */
    if ((ret_value = H5VL__get_connector_id_by_name(name, true)) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTGET, H5I_INVALID_HID, "can't get VOL id");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5VLget_connector_id_by_name() */

/*-------------------------------------------------------------------------
 * Function:    H5VLget_connector_id_by_value
 *
 * Purpose:     Retrieves the ID for a registered VOL connector.
 *
 * Return:      A valid VOL connector ID if a connector with that value has
 *              been registered. This ID will need to be closed using
 *              H5VLclose().
 *
 *              H5I_INVALID_HID on error or if a VOL connector with that
 *              value has not been registered.
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5VLget_connector_id_by_value(H5VL_class_value_t connector_value)
{
    hid_t ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE1("i", "VC", connector_value);

    /* Get connector ID with this value */
    if ((ret_value = H5VL__get_connector_id_by_value(connector_value, true)) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTGET, H5I_INVALID_HID, "can't get VOL id");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5VLget_connector_id_by_value() */

/*-------------------------------------------------------------------------
 * Function:    H5VLpeek_connector_id_by_name
 *
 * Purpose:     Retrieves the ID for a registered VOL connector.
 *
 * Return:      A valid VOL connector ID if a connector by that name has
 *              been registered. This ID is *not* owned by the caller and
 *              H5VLclose() should not be called.  Intended for use by VOL
 *              connectors to find their own ID.
 *
 *              H5I_INVALID_HID on error or if a VOL connector of that
 *              name has not been registered.
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5VLpeek_connector_id_by_name(const char *name)
{
    hid_t ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE1("i", "*s", name);

    /* Get connector ID with this name */
    if ((ret_value = H5VL__peek_connector_id_by_name(name)) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTGET, H5I_INVALID_HID, "can't get VOL id");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5VLpeek_connector_id_by_name() */

/*-------------------------------------------------------------------------
 * Function:    H5VLpeek_connector_id_by_value
 *
 * Purpose:     Retrieves the ID for a registered VOL connector.
 *
 * Return:      A valid VOL connector ID if a connector with that value
 *              has been registered. This ID is *not* owned by the caller
 *              and H5VLclose() should not be called.  Intended for use by
 *              VOL connectors to find their own ID.
 *
 *              H5I_INVALID_HID on error or if a VOL connector with that
 *              value has not been registered.
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5VLpeek_connector_id_by_value(H5VL_class_value_t value)
{
    hid_t ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE1("i", "VC", value);

    /* Get connector ID with this value */
    if ((ret_value = H5VL__peek_connector_id_by_value(value)) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTGET, H5I_INVALID_HID, "can't get VOL id");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5VLpeek_connector_id_by_value() */

/*-------------------------------------------------------------------------
 * Function:    H5VLget_connector_name
 *
 * Purpose:     Returns the connector name for the VOL associated with the
 *              object or file ID.
 *
 *              This works like other calls where the caller must provide a
 *              buffer of the appropriate size for the library to fill in.
 *              i.e., passing in a NULL pointer for NAME will return the
 *              required size of the buffer.
 *
 * Return:      Success:        The length of the connector name
 *
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
ssize_t
H5VLget_connector_name(hid_t obj_id, char *name /*out*/, size_t size)
{
    ssize_t ret_value = -1;

    FUNC_ENTER_API(FAIL)
    H5TRACE3("Zs", "ixz", obj_id, name, size);

    /* Call internal routine */
    if ((ret_value = H5VL__get_connector_name(obj_id, name, size)) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "Can't get connector name");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5VLget_connector_name() */

/*-------------------------------------------------------------------------
 * Function:    H5VLclose
 *
 * Purpose:     Closes a VOL connector ID.  This in no way affects
 *              file access property lists which have been defined to use
 *              this VOL connector or files which are already opened under with
 *              this connector.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VLclose(hid_t vol_id)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", vol_id);

    /* Check args */
    if (NULL == H5I_object_verify(vol_id, H5I_VOL))
        HGOTO_ERROR(H5E_VOL, H5E_BADTYPE, FAIL, "not a VOL connector");

    /* Decrement the ref count on the ID, possibly releasing the VOL connector */
    if (H5I_dec_app_ref(vol_id) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTDEC, FAIL, "unable to close VOL connector ID");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5VLclose() */

/*-------------------------------------------------------------------------
 * Function:    H5VLunregister_connector
 *
 * Purpose:     Removes a VOL connector ID from the library. This in no way affects
 *              file access property lists which have been defined to use
 *              this VOL connector or files which are already opened under with
 *              this connector.
 *
 *              The native VOL connector cannot be unregistered and attempts
 *              to do so are considered an error.
 *
 * Return:      Success:    Non-negative
 *
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VLunregister_connector(hid_t vol_id)
{
    hid_t  native_id = H5I_INVALID_HID;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", vol_id);

    /* Check arguments */
    if (NULL == H5I_object_verify(vol_id, H5I_VOL))
        HGOTO_ERROR(H5E_VOL, H5E_BADTYPE, FAIL, "not a VOL connector ID");

    /* For the time being, we disallow unregistering the native VOL connector */
    if (H5I_INVALID_HID == (native_id = H5VL__get_connector_id_by_name(H5VL_NATIVE_NAME, false)))
        HGOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "unable to find the native VOL connector ID");
    if (vol_id == native_id)
        HGOTO_ERROR(H5E_VOL, H5E_BADVALUE, FAIL, "unregistering the native VOL connector is not allowed");

    /* The H5VL_class_t struct will be freed by this function */
    if (H5I_dec_app_ref(vol_id) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTDEC, FAIL, "unable to unregister VOL connector");

done:
    if (native_id != H5I_INVALID_HID)
        if (H5I_dec_ref(native_id) < 0)
            HGOTO_ERROR(H5E_VOL, H5E_CANTDEC, FAIL, "unable to decrement count on native_id");

    FUNC_LEAVE_API(ret_value)
} /* end H5VLunregister_connector() */

/*---------------------------------------------------------------------------
 * Function:    H5VLcmp_connector_cls
 *
 * Purpose:     Compares two connector classes (based on their value field)
 *
 * Note:        This routine is _only_ for HDF5 VOL connector authors!  It is
 *              _not_ part of the public API for HDF5 application developers.
 *
 * Return:      Success:    Non-negative, *cmp set to a value like strcmp
 *
 *              Failure:    Negative, *cmp unset
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5VLcmp_connector_cls(int *cmp, hid_t connector_id1, hid_t connector_id2)
{
    H5VL_class_t *cls1, *cls2;         /* connectors for IDs */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "*Isii", cmp, connector_id1, connector_id2);

    /* Check args and get class pointers */
    if (NULL == (cls1 = (H5VL_class_t *)H5I_object_verify(connector_id1, H5I_VOL)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a VOL connector ID");
    if (NULL == (cls2 = (H5VL_class_t *)H5I_object_verify(connector_id2, H5I_VOL)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a VOL connector ID");

    /* Compare the two VOL connector classes */
    if (H5VL_cmp_connector_cls(cmp, cls1, cls2) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTCOMPARE, FAIL, "can't compare connector classes");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5VLcmp_connector_cls() */

/*---------------------------------------------------------------------------
 * Function:    H5VLwrap_register
 *
 * Purpose:     Wrap an internal object with a "wrap context" and register an
 *              hid_t for the resulting object.
 *
 * Note:        This routine is mainly targeted toward wrapping objects for
 *              iteration routine callbacks (i.e. the callbacks from H5Aiterate*,
 *              H5Literate* / H5Lvisit*, and H5Ovisit* ).
 *
 *              type must be a VOL-managed object class (H5I_FILE,
 *              H5I_GROUP, H5I_DATATYPE, H5I_DATASET, H5I_MAP, or H5I_ATTR).
 *
 * Return:      Success:    Non-negative hid_t for the object.
 *              Failure:    Negative (H5I_INVALID_HID)
 *
 *---------------------------------------------------------------------------
 */
hid_t
H5VLwrap_register(void *obj, H5I_type_t type)
{
    hid_t ret_value; /* Return value */

    /* Use FUNC_ENTER_API_NOINIT here, so the API context doesn't get reset */
    FUNC_ENTER_API_NOINIT
    H5TRACE2("i", "*xIt", obj, type);

    /* Check args */
    /* Use a switch here for (hopefully) better performance than a series of
     * equality checks.  We could also group these types together in H5I_type_t,
     * make some assertions here to guarantee that, then just check the range.
     */
    switch (type) {
        case H5I_FILE:
        case H5I_GROUP:
        case H5I_DATATYPE:
        case H5I_DATASET:
        case H5I_MAP:
        case H5I_ATTR:
            /* VOL-managed objects, call is valid */
            break;
        case H5I_UNINIT:
        case H5I_BADID:
        case H5I_DATASPACE:
        case H5I_VFL:
        case H5I_VOL:
        case H5I_GENPROP_CLS:
        case H5I_GENPROP_LST:
        case H5I_ERROR_CLASS:
        case H5I_ERROR_MSG:
        case H5I_ERROR_STACK:
        case H5I_SPACE_SEL_ITER:
        case H5I_EVENTSET:
        case H5I_NTYPES:
        default:
            HGOTO_ERROR(H5E_VOL, H5E_BADRANGE, H5I_INVALID_HID, "invalid type number");
    } /* end switch */
    if (NULL == obj)
        HGOTO_ERROR(H5E_VOL, H5E_BADVALUE, H5I_INVALID_HID, "obj is NULL");

    /* Wrap the object and register an ID for it */
    if ((ret_value = H5VL_wrap_register(type, obj, true)) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to wrap object");

done:
    FUNC_LEAVE_API_NOINIT(ret_value)
} /* H5VLwrap_register() */

/*---------------------------------------------------------------------------
 * Function:    H5VLobject
 *
 * Purpose:     Retrieve the object pointer associated with an hid_t for a
 *              VOL object.
 *
 * Note:        This routine is mainly targeted toward unwrapping objects for
 *              testing.
 *
 * Return:      Success:    Object pointer
 *              Failure:    NULL
 *
 *---------------------------------------------------------------------------
 */
void *
H5VLobject(hid_t id)
{
    void *ret_value; /* Return value */

    FUNC_ENTER_API(NULL)
    H5TRACE1("*x", "i", id);

    /* Retrieve the object pointer for the ID */
    if (NULL == (ret_value = H5VL_object(id)))
        HGOTO_ERROR(H5E_VOL, H5E_CANTGET, NULL, "unable to retrieve object");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5VLobject() */

/*---------------------------------------------------------------------------
 * Function:    H5VLobject_is_native
 *
 * Purpose:     Determines whether an object ID represents a native VOL
 *              connector object.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5VLobject_is_native(hid_t obj_id, hbool_t *is_native)
{
    H5VL_object_t *vol_obj   = NULL;
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "i*b", obj_id, is_native);

    if (!is_native)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "`is_native` argument is NULL");

    /* Get the location object for the ID */
    if (NULL == (vol_obj = H5VL_vol_object(obj_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid object identifier");

    if (H5VL_object_is_native(vol_obj, is_native) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't determine if object is a native connector object");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5VLobject_is_native() */

/*-------------------------------------------------------------------------
 * Function:    H5VLget_file_type
 *
 * Purpose:     Returns a copy of dtype_id with its location set to be in
 *              the file, with updated size, etc.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5VLget_file_type(void *file_obj, hid_t connector_id, hid_t dtype_id)
{
    H5T_t         *dtype;               /* unregistered type       */
    H5T_t         *file_type    = NULL; /* copied file type        */
    hid_t          file_type_id = -1;   /* copied file type id     */
    H5VL_object_t *file_vol_obj = NULL; /* VOL object for file     */
    hid_t          ret_value    = -1;   /* Return value            */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("i", "*xii", file_obj, connector_id, dtype_id);

    /* Check args */
    if (!file_obj)
        HGOTO_ERROR(H5E_ARGS, H5E_UNINITIALIZED, FAIL, "no file object supplied");
    if (NULL == (dtype = (H5T_t *)H5I_object_verify(dtype_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a data type");

    /* Create VOL object for file if necessary (force_conv will be true if and
     * only if file needs to be passed to H5T_set_loc) */
    if (H5T_GET_FORCE_CONV(dtype) &&
        (NULL == (file_vol_obj = H5VL_create_object_using_vol_id(H5I_FILE, file_obj, connector_id))))
        HGOTO_ERROR(H5E_VOL, H5E_CANTCREATE, FAIL, "can't create VOL object");

    /* Copy the datatype */
    if (NULL == (file_type = H5T_copy(dtype, H5T_COPY_TRANSIENT)))
        HGOTO_ERROR(H5E_VOL, H5E_CANTCOPY, FAIL, "unable to copy datatype");

    /* Register file type id */
    if ((file_type_id = H5I_register(H5I_DATATYPE, file_type, false)) < 0) {
        (void)H5T_close_real(file_type);
        HGOTO_ERROR(H5E_VOL, H5E_CANTREGISTER, FAIL, "unable to register file datatype");
    } /* end if */

    /* Set the location of the datatype to be in the file */
    if (H5T_set_loc(file_type, file_vol_obj, H5T_LOC_DISK) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTINIT, FAIL, "can't set datatype location");

    /* Release our reference to file_type */
    if (file_vol_obj) {
        if (H5VL_free_object(file_vol_obj) < 0)
            HGOTO_ERROR(H5E_VOL, H5E_CANTDEC, FAIL, "unable to free VOL object");
        file_vol_obj = NULL;
    } /* end if */

    /* Set return value */
    ret_value = file_type_id;

done:
    /* Cleanup on error */
    if (ret_value < 0) {
        if (file_vol_obj && H5VL_free_object(file_vol_obj) < 0)
            HDONE_ERROR(H5E_VOL, H5E_CANTDEC, FAIL, "unable to free VOL object");
        if (file_type_id >= 0 && H5I_dec_ref(file_type_id) < 0)
            HDONE_ERROR(H5E_VOL, H5E_CANTDEC, FAIL, "unable to close file datatype");
    } /* end if */

    FUNC_LEAVE_API(ret_value)
} /* end H5VLget_file_type() */

/*---------------------------------------------------------------------------
 * Function:    H5VLretrieve_lib_state
 *
 * Purpose:     Retrieves a copy of the internal state of the HDF5 library,
 *              so that it can be restored later.
 *
 * Note:        This routine is _only_ for HDF5 VOL connector authors!  It is
 *              _not_ part of the public API for HDF5 application developers.
 *
 * Return:      Success:    Non-negative, *state set
 *              Failure:    Negative, *state unset
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5VLretrieve_lib_state(void **state /*out*/)
{
    herr_t ret_value = SUCCEED; /* Return value */

    /* Must use this, to avoid modifying the API context stack in FUNC_ENTER */
    FUNC_ENTER_API_NOINIT
    H5TRACE1("e", "x", state);

    /* Check args */
    if (NULL == state)
        HGOTO_ERROR(H5E_VOL, H5E_BADVALUE, FAIL, "invalid state pointer");

    /* Retrieve the library state */
    if (H5VL_retrieve_lib_state(state) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't retrieve library state");

done:
    FUNC_LEAVE_API_NOINIT(ret_value)
} /* H5VLretrieve_lib_state() */

/*---------------------------------------------------------------------------
 * Function:    H5VLstart_lib_state
 *
 * Purpose:     Opens a new internal state for the HDF5 library.
 *
 * Note:        This routine is _only_ for HDF5 VOL connector authors!  It is
 *              _not_ part of the public API for HDF5 application developers.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5VLstart_lib_state(void)
{
    herr_t ret_value = SUCCEED; /* Return value */

    /* Must use this, to avoid modifying the API context stack in FUNC_ENTER */
    FUNC_ENTER_API_NOINIT
    H5TRACE0("e", "");

    /* Start a new library state */
    if (H5VL_start_lib_state() < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTSET, FAIL, "can't start new library state");

done:
    FUNC_LEAVE_API_NOINIT(ret_value)
} /* H5VLstart_lib_state() */

/*---------------------------------------------------------------------------
 * Function:    H5VLrestore_lib_state
 *
 * Purpose:     Restores the internal state of the HDF5 library.
 *
 * Note:        This routine is _only_ for HDF5 VOL connector authors!  It is
 *              _not_ part of the public API for HDF5 application developers.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5VLrestore_lib_state(const void *state)
{
    herr_t ret_value = SUCCEED; /* Return value */

    /* Must use this, to avoid modifying the API context stack in FUNC_ENTER */
    FUNC_ENTER_API_NOINIT
    H5TRACE1("e", "*x", state);

    /* Check args */
    if (NULL == state)
        HGOTO_ERROR(H5E_VOL, H5E_BADVALUE, FAIL, "invalid state pointer");

    /* Restore the library state */
    if (H5VL_restore_lib_state(state) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTSET, FAIL, "can't restore library state");

done:
    FUNC_LEAVE_API_NOINIT(ret_value)
} /* H5VLrestore_lib_state() */

/*---------------------------------------------------------------------------
 * Function:    H5VLfinish_lib_state
 *
 * Purpose:     Closes the internal state of the HDF5 library, undoing the
 *              affects of H5VLstart_lib_state.
 *
 * Note:        This routine is _only_ for HDF5 VOL connector authors!  It is
 *              _not_ part of the public API for HDF5 application developers.
 *
 * Note:        This routine must be called as a "pair" with
 *              H5VLstart_lib_state.  It can be called before / after /
 *              independently of H5VLfree_lib_state.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5VLfinish_lib_state(void)
{
    herr_t ret_value = SUCCEED; /* Return value */

    /* Must use this, to avoid modifying the API context stack in FUNC_ENTER */
    FUNC_ENTER_API_NOINIT
    H5TRACE0("e", "");

    /* Reset the library state */
    if (H5VL_finish_lib_state() < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTRESET, FAIL, "can't reset library state");

done:
    FUNC_LEAVE_API_NOINIT(ret_value)
} /* H5VLfinish_lib_state() */

/*---------------------------------------------------------------------------
 * Function:    H5VLfree_lib_state
 *
 * Purpose:     Free a retrieved library state.
 *
 * Note:        This routine is _only_ for HDF5 VOL connector authors!  It is
 *              _not_ part of the public API for HDF5 application developers.
 *
 * Note:        This routine must be called as a "pair" with
 *              H5VLretrieve_lib_state.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5VLfree_lib_state(void *state)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "*x", state);

    /* Check args */
    if (NULL == state)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid state pointer");

    /* Free the library state */
    if (H5VL_free_lib_state(state) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTRELEASE, FAIL, "can't free library state");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5VLfree_lib_state() */

/*---------------------------------------------------------------------------
 * Function:    H5VLquery_optional
 *
 * Purpose:     Determine if a VOL connector supports a particular optional
 *              callback operation, and a general sense of the operation's
 *              behavior.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5VLquery_optional(hid_t obj_id, H5VL_subclass_t subcls, int opt_type, uint64_t *flags /*out*/)
{
    H5VL_object_t *vol_obj   = NULL;
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "iVSIsx", obj_id, subcls, opt_type, flags);

    /* Check args */
    if (NULL == flags)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid 'flags' pointer");
    if (NULL == (vol_obj = H5VL_vol_object(obj_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid object identifier");

    /* Query the connector */
    if (H5VL_introspect_opt_query(vol_obj, subcls, opt_type, flags) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "unable to query VOL connector operation");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5VLquery_optional() */

/*---------------------------------------------------------------------------
 * Function:    H5VLregister_opt_operation
 *
 * Purpose:     Allow a VOL connector to register a new optional operation
 *              for a VOL object subclass.   The operation name must be runtime
 *              unique for each operation, preferably avoiding naming clashes
 *              by using a Uniform Type Identifier (UTI,
 *https://developer.apple.com/library/archive/documentation/FileManagement/Conceptual/understanding_utis/understand_utis_conc/understand_utis_conc.html)
 *              for each operation name.  The value returned in the 'op_val'
 *              pointer will be unique for that VOL connector to use for its
 *              operation on that subclass.
 *
 *              For example, registering a 'prefetch' operation for the
 *              caching VOL connector written at the ALCF at Argonne National
 *              Laboratory could have a UTI of: "gov.anl.alcf.cache.prefetch",
 *              and the "evict" operation for the same connector could have a
 *              UTI of: "gov.anl.alcf.cache.evict".   Registering a "suspend
 *              background threads" operation for the asynchronous VOL connector
 *              written at NERSC at Lawrence Berkeley National Laboratory could
 *              have a UTI of: "gov.lbnl.nersc.async.suspend_bkg_threads".
 *
 * Note:        The first 1024 values of each subclass's optional operations
 *              are reserved for the native VOL connector's use.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5VLregister_opt_operation(H5VL_subclass_t subcls, const char *op_name, int *op_val /*out*/)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "VS*sx", subcls, op_name, op_val);

    /* Check args */
    if (NULL == op_val)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid op_val pointer");
    if (NULL == op_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid op_name pointer");
    if ('\0' == *op_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid op_name string");
    if (!((H5VL_SUBCLS_ATTR == subcls) || (H5VL_SUBCLS_DATASET == subcls) ||
          (H5VL_SUBCLS_DATATYPE == subcls) || (H5VL_SUBCLS_FILE == subcls) || (H5VL_SUBCLS_GROUP == subcls) ||
          (H5VL_SUBCLS_OBJECT == subcls) || (H5VL_SUBCLS_LINK == subcls) || (H5VL_SUBCLS_REQUEST == subcls)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid VOL subclass type");

    /* Register the operation */
    if (H5VL__register_opt_operation(subcls, op_name, op_val) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTREGISTER, FAIL, "can't register dynamic optional operation: '%s'",
                    op_name);

done:
    FUNC_LEAVE_API(ret_value)
} /* H5VLregister_opt_operation() */

/*---------------------------------------------------------------------------
 * Function:    H5VLfind_opt_operation
 *
 * Purpose:     Look up a optional operation for a VOL object subclass, by name.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5VLfind_opt_operation(H5VL_subclass_t subcls, const char *op_name, int *op_val /*out*/)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "VS*sx", subcls, op_name, op_val);

    /* Check args */
    if (NULL == op_val)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid op_val pointer");
    if (NULL == op_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid op_name pointer");
    if ('\0' == *op_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid op_name string");
    if (!((H5VL_SUBCLS_ATTR == subcls) || (H5VL_SUBCLS_DATASET == subcls) ||
          (H5VL_SUBCLS_DATATYPE == subcls) || (H5VL_SUBCLS_FILE == subcls) || (H5VL_SUBCLS_GROUP == subcls) ||
          (H5VL_SUBCLS_OBJECT == subcls) || (H5VL_SUBCLS_LINK == subcls) || (H5VL_SUBCLS_REQUEST == subcls)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid VOL subclass type");

    /* Find the operation */
    if (H5VL__find_opt_operation(subcls, op_name, op_val) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_NOTFOUND, FAIL, "can't find dynamic optional operation: '%s'", op_name);

done:
    FUNC_LEAVE_API(ret_value)
} /* H5VLfind_opt_operation() */

/*---------------------------------------------------------------------------
 * Function:    H5VLunregister_opt_operation
 *
 * Purpose:     Unregister a optional operation for a VOL object subclass, by name.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5VLunregister_opt_operation(H5VL_subclass_t subcls, const char *op_name)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "VS*s", subcls, op_name);

    /* Check args */
    if (NULL == op_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid op_name pointer");
    if ('\0' == *op_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid op_name string");
    if (!((H5VL_SUBCLS_ATTR == subcls) || (H5VL_SUBCLS_DATASET == subcls) ||
          (H5VL_SUBCLS_DATATYPE == subcls) || (H5VL_SUBCLS_FILE == subcls) || (H5VL_SUBCLS_GROUP == subcls) ||
          (H5VL_SUBCLS_OBJECT == subcls) || (H5VL_SUBCLS_LINK == subcls) || (H5VL_SUBCLS_REQUEST == subcls)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid VOL subclass type");

    /* Unregister the operation */
    if (H5VL__unregister_opt_operation(subcls, op_name) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTREMOVE, FAIL, "can't unregister dynamic optional operation: '%s'",
                    op_name);

done:
    FUNC_LEAVE_API(ret_value)
} /* H5VLunregister_opt_operation() */
