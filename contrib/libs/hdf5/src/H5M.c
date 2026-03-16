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

#include "H5Mmodule.h" /* This source code file is part of the H5M module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5CXprivate.h" /* API Contexts                             */
#include "H5Mpkg.h"      /* Maps                                     */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5ESprivate.h" /* Event Sets                               */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5VLprivate.h" /* Virtual Object Layer                     */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Local Prototypes */
/********************/
static herr_t H5M__close_cb(H5VL_object_t *map_vol_obj, void **request);

#ifdef H5_HAVE_MAP_API
static hid_t  H5M__create_api_common(hid_t loc_id, const char *name, hid_t key_type_id, hid_t val_type_id,
                                     hid_t lcpl_id, hid_t mcpl_id, hid_t mapl_id, void **token_ptr,
                                     H5VL_object_t **_vol_obj_ptr);
static hid_t  H5M__open_api_common(hid_t loc_id, const char *name, hid_t mapl_id, void **token_ptr,
                                   H5VL_object_t **_vol_obj_ptr);
static herr_t H5M__put_api_common(hid_t map_id, hid_t key_mem_type_id, const void *key, hid_t val_mem_type_id,
                                  const void *value, hid_t dxpl_id, void **token_ptr,
                                  H5VL_object_t **_vol_obj_ptr);
static herr_t H5M__get_api_common(hid_t map_id, hid_t key_mem_type_id, const void *key, hid_t val_mem_type_id,
                                  void *value, hid_t dxpl_id, void **token_ptr, H5VL_object_t **_vol_obj_ptr);
#endif /*  H5_HAVE_MAP_API */

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Map ID class */
static const H5I_class_t H5I_MAP_CLS[1] = {{
    H5I_MAP,                  /* ID class value */
    0,                        /* Class flags */
    0,                        /* # of reserved IDs for class */
    (H5I_free_t)H5M__close_cb /* Callback routine for closing objects of this class */
}};

/*-------------------------------------------------------------------------
 * Function: H5M_init
 *
 * Purpose:  Initialize the interface from some other layer.
 *
 * Return:   Success:    non-negative
 *
 *           Failure:    negative
 *-------------------------------------------------------------------------
 */
herr_t
H5M_init(void)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Initialize the ID group for the map IDs */
    if (H5I_register_type(H5I_MAP_CLS) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTINIT, FAIL, "unable to initialize interface");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5M_init() */

/*-------------------------------------------------------------------------
 * Function: H5M_top_term_package
 *
 * Purpose:  Close the "top" of the interface, releasing IDs, etc.
 *
 * Return:   Success:    Positive if anything was done that might
 *                affect other interfaces; zero otherwise.
 *           Failure:    Negative.
 *-------------------------------------------------------------------------
 */
int
H5M_top_term_package(void)
{
    int n = 0;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    if (H5I_nmembers(H5I_MAP) > 0) {
        (void)H5I_clear_type(H5I_MAP, false, false);
        n++;
    }

    FUNC_LEAVE_NOAPI(n)
} /* end H5M_top_term_package() */

/*-------------------------------------------------------------------------
 * Function: H5M_term_package
 *
 * Purpose:  Terminate this interface.
 *
 * Note:     Finishes shutting down the interface, after
 *           H5M_top_term_package() is called
 *
 * Return:   Success:    Positive if anything was done that might
 *                affect other interfaces; zero otherwise.
 *            Failure:    Negative.
 *-------------------------------------------------------------------------
 */
int
H5M_term_package(void)
{
    int n = 0;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Sanity checks */
    assert(0 == H5I_nmembers(H5I_MAP));

    /* Destroy the dataset object id group */
    n += (H5I_dec_type_ref(H5I_MAP) > 0);

    FUNC_LEAVE_NOAPI(n)
} /* end H5M_term_package() */

/*-------------------------------------------------------------------------
 * Function:    H5M__close_cb
 *
 * Purpose:     Called when the ref count reaches zero on the map's ID
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5M__close_cb(H5VL_object_t *map_vol_obj, void **request)
{
    H5VL_optional_args_t vol_cb_args;         /* Arguments to VOL callback */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(map_vol_obj);

    /* Set up VOL callback arguments */
    vol_cb_args.op_type = H5VL_MAP_CLOSE;
    vol_cb_args.args    = NULL;

    /* Close the map */
    if (H5VL_optional(map_vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, request) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CLOSEERROR, FAIL, "unable to close map");

    /* Free the VOL object */
    if (H5VL_free_object(map_vol_obj) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTDEC, FAIL, "unable to free VOL object");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5M__close_cb() */

#ifdef H5_HAVE_MAP_API

/*-------------------------------------------------------------------------
 * Function:    H5M__create_api_common
 *
 * Purpose:     This is the common function for creating the HDF5 map.
 *
 * Return:      Success:    The object ID of the new map.
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
static hid_t
H5M__create_api_common(hid_t loc_id, const char *name, hid_t key_type_id, hid_t val_type_id, hid_t lcpl_id,
                       hid_t mcpl_id, hid_t mapl_id, void **token_ptr, H5VL_object_t **_vol_obj_ptr)
{
    void           *map         = NULL; /* New map's info */
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_optional_args_t vol_cb_args;                 /* Arguments to VOL callback */
    H5VL_map_args_t      map_args;                    /* Arguments for map operations */
    hid_t                ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (!name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "name parameter cannot be NULL");
    if (!*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "name parameter cannot be an empty string");

    /* Get link creation property list */
    if (H5P_DEFAULT == lcpl_id)
        lcpl_id = H5P_LINK_CREATE_DEFAULT;
    else if (true != H5P_isa_class(lcpl_id, H5P_LINK_CREATE))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "lcpl_id is not a link creation property list");

    /* Get map creation property list */
    if (H5P_DEFAULT == mcpl_id)
        mcpl_id = H5P_MAP_CREATE_DEFAULT;
    else if (true != H5P_isa_class(mcpl_id, H5P_MAP_CREATE))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "mcpl_id is not a map create property list ID");

    /* Set up VOL callback arguments */
    if (H5VL_setup_acc_args(loc_id, H5P_CLS_MACC, true, &mapl_id, vol_obj_ptr, &map_args.create.loc_params) <
        0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTSET, H5I_INVALID_HID, "can't set object access arguments");
    map_args.create.name        = name;
    map_args.create.lcpl_id     = lcpl_id;
    map_args.create.key_type_id = key_type_id;
    map_args.create.val_type_id = val_type_id;
    map_args.create.mcpl_id     = mcpl_id;
    map_args.create.mapl_id     = mapl_id;
    map_args.create.map         = NULL;
    vol_cb_args.op_type         = H5VL_MAP_CREATE;
    vol_cb_args.args            = &map_args;

    /* Create the map */
    if (H5VL_optional(*vol_obj_ptr, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, token_ptr) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTINIT, H5I_INVALID_HID, "unable to create map");
    map = map_args.create.map;

    /* Get an ID for the map */
    if ((ret_value = H5VL_register(H5I_MAP, map, (*vol_obj_ptr)->connector, true)) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register map handle");

done:
    /* Cleanup on failure */
    if (H5I_INVALID_HID == ret_value) {
        /* Set up VOL callback arguments */
        vol_cb_args.op_type = H5VL_MAP_CLOSE;
        vol_cb_args.args    = NULL;

        if (map && H5VL_optional(*vol_obj_ptr, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, H5I_INVALID_HID, "unable to release map");
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5M__create_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Mcreate
 *
 * Purpose:     Creates a new map object for storing key-value pairs.  The
 *              in-file datatype for keys is defined by KEY_TYPE_ID and
 *              the in-file datatype for values is defined by VAL_TYPE_ID.
 *              LOC_ID specifies the location to create the map object and
 *              NAME specifies the name of the link to the object
 *              (relative to LOC_ID).  Other options can be specified
 *              through the property lists LCPL_ID, MCPL_ID, and MAPL_ID.
 *
 * Return:      Success:    The object ID of the new map.
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Mcreate(hid_t loc_id, const char *name, hid_t key_type_id, hid_t val_type_id, hid_t lcpl_id, hid_t mcpl_id,
          hid_t mapl_id)
{
    hid_t ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE7("i", "i*siiiii", loc_id, name, key_type_id, val_type_id, lcpl_id, mcpl_id, mapl_id);

    /* Create the map synchronously */
    if ((ret_value = H5M__create_api_common(loc_id, name, key_type_id, val_type_id, lcpl_id, mcpl_id, mapl_id,
                                            NULL, NULL)) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTCREATE, H5I_INVALID_HID, "unable to create map synchronously");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Mcreate() */

/*-------------------------------------------------------------------------
 * Function:    H5Mcreate_async
 *
 * Purpose:     Asynchronous version of H5Mcreate
 *
 * Return:      Success:    The object ID of the new map.
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Mcreate_async(const char *app_file, const char *app_func, unsigned app_line, hid_t loc_id, const char *name,
                hid_t key_type_id, hid_t val_type_id, hid_t lcpl_id, hid_t mcpl_id, hid_t mapl_id,
                hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    hid_t          ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE11("i", "*s*sIui*siiiiii", app_file, app_func, app_line, loc_id, name, key_type_id, val_type_id,
              lcpl_id, mcpl_id, mapl_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token;

    /* Create the map asynchronously */
    if ((ret_value = H5M__create_api_common(loc_id, name, key_type_id, val_type_id, lcpl_id, mcpl_id, mapl_id,
                                            token_ptr, &vol_obj)) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTCREATE, H5I_INVALID_HID, "unable to create map asynchronously");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE11(__func__, "*s*sIui*siiiiii", app_file, app_func, app_line, loc_id, name, key_type_id, val_type_id, lcpl_id, mcpl_id, mapl_id, es_id)) < 0) {
            /* clang-format on */
            if (H5I_dec_app_ref_always_close(ret_value) < 0)
                HDONE_ERROR(H5E_MAP, H5E_CANTDEC, H5I_INVALID_HID, "can't decrement count on map ID");
            HGOTO_ERROR(H5E_MAP, H5E_CANTINSERT, H5I_INVALID_HID, "can't insert token into event set");
        } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Mcreate_async() */

/*-------------------------------------------------------------------------
 * Function:    H5Mcreate_anon
 *
 * Purpose:     Creates a new map object for storing key-value pairs.  The
 *              in-file datatype for keys is defined by KEY_TYPE_ID and
 *              the in-file datatype for values is defined by VAL_TYPE_ID.
 *              LOC_ID specifies the file to create the map object, but no
 *              link to the object is created.  Other options can be
 *              specified through the property lists LCPL_ID, MCPL_ID, and
 *              MAPL_ID.
 *
 *              The resulting ID should be linked into the file with
 *              H5Olink or it will be deleted when closed.
 *
 * Return:      Success:    The object ID of the new map. The map should
 *                          be linked into the group hierarchy before being closed or
 *                          it will be deleted. The dataset should be
 *                          closed when the caller is no longer interested
 *                          in it.
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Mcreate_anon(hid_t loc_id, hid_t key_type_id, hid_t val_type_id, hid_t mcpl_id, hid_t mapl_id)
{
    void                *map     = NULL;              /* map object from VOL connector */
    H5VL_object_t       *vol_obj = NULL;              /* object of loc_id */
    H5VL_optional_args_t vol_cb_args;                 /* Arguments to VOL callback */
    H5VL_map_args_t      map_args;                    /* Arguments for map operations */
    hid_t                ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE5("i", "iiiii", loc_id, key_type_id, val_type_id, mcpl_id, mapl_id);

    /* Check arguments */
    if (H5P_DEFAULT == mcpl_id)
        mcpl_id = H5P_MAP_CREATE_DEFAULT;
    else if (true != H5P_isa_class(mcpl_id, H5P_MAP_CREATE))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not map create property list ID");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&mapl_id, H5P_CLS_MACC, loc_id, true) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTSET, H5I_INVALID_HID, "can't set access property list info");

    /* get the location object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid location identifier");

    /* Set location parameters */

    /* Set up VOL callback arguments */
    map_args.create.loc_params.type     = H5VL_OBJECT_BY_SELF;
    map_args.create.loc_params.obj_type = H5I_get_type(loc_id);
    map_args.create.name                = NULL;
    map_args.create.lcpl_id             = H5P_LINK_CREATE_DEFAULT;
    map_args.create.key_type_id         = key_type_id;
    map_args.create.val_type_id         = val_type_id;
    map_args.create.mcpl_id             = mcpl_id;
    map_args.create.mapl_id             = mapl_id;
    map_args.create.map                 = NULL;
    vol_cb_args.op_type                 = H5VL_MAP_CREATE;
    vol_cb_args.args                    = &map_args;

    /* Create the map */
    if (H5VL_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTINIT, H5I_INVALID_HID, "unable to create map");
    map = map_args.create.map;

    /* Get an ID for the map */
    if ((ret_value = H5VL_register(H5I_MAP, map, vol_obj->connector, true)) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register map");

done:
    /* Cleanup on failure */
    if (H5I_INVALID_HID == ret_value) {
        /* Set up VOL callback arguments */
        vol_cb_args.op_type = H5VL_MAP_CLOSE;
        vol_cb_args.args    = NULL;

        if (map && H5VL_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, H5I_INVALID_HID, "unable to release map");
    } /* end if */

    FUNC_LEAVE_API(ret_value)
} /* end H5Mcreate_anon() */

/*------------------------------------------------------------------------
 * Function:    H5M__open_api_common
 *
 * Purpose:     This is the common function for opening the HDF5 map.
 *
 * Return:      Success:    Object ID of the map
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
static hid_t
H5M__open_api_common(hid_t loc_id, const char *name, hid_t mapl_id, void **token_ptr,
                     H5VL_object_t **_vol_obj_ptr)
{
    void           *map         = NULL; /* map object from VOL connector */
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_optional_args_t vol_cb_args;                 /* Arguments to VOL callback */
    H5VL_map_args_t      map_args;                    /* Arguments for map operations */
    hid_t                ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    if (!name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "name parameter cannot be NULL");
    if (!*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "name parameter cannot be an empty string");

    /* Set up VOL callback arguments */
    if (H5VL_setup_acc_args(loc_id, H5P_CLS_MACC, false, &mapl_id, vol_obj_ptr, &map_args.open.loc_params) <
        0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTSET, H5I_INVALID_HID, "can't set object access arguments");
    map_args.open.name    = name;
    map_args.open.mapl_id = mapl_id;
    map_args.open.map     = NULL;
    vol_cb_args.op_type   = H5VL_MAP_OPEN;
    vol_cb_args.args      = &map_args;

    /* Open the map */
    if (H5VL_optional(*vol_obj_ptr, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, token_ptr) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTOPENOBJ, H5I_INVALID_HID, "unable to open map");
    map = map_args.open.map;

    /* Register an ID for the map */
    if ((ret_value = H5VL_register(H5I_MAP, map, (*vol_obj_ptr)->connector, true)) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTREGISTER, H5I_INVALID_HID, "can't register map ID");

done:
    /* Cleanup on failure */
    if (H5I_INVALID_HID == ret_value) {
        /* Set up VOL callback arguments */
        vol_cb_args.op_type = H5VL_MAP_CLOSE;
        vol_cb_args.args    = NULL;

        if (map && H5VL_optional(*vol_obj_ptr, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, H5I_INVALID_HID, "unable to release map");
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5M__open_api_common() */

/*------------------------------------------------------------------------
 * Function:    H5Mopen
 *
 * Purpose:     Finds a map named NAME at LOC_ID, opens it, and returns
 *              its ID. The map should be close when the caller is no
 *              longer interested in it.
 *
 *              Takes a map access property list
 *
 * Return:      Success:    Object ID of the map
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Mopen(hid_t loc_id, const char *name, hid_t mapl_id)
{
    hid_t ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE3("i", "i*si", loc_id, name, mapl_id);

    /* Open the map synchronously */
    if ((ret_value = H5M__open_api_common(loc_id, name, mapl_id, NULL, NULL)) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTCREATE, H5I_INVALID_HID, "unable to open map synchronously");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Mopen() */

/*------------------------------------------------------------------------
 * Function:    H5Mopen_async
 *
 * Purpose:     Asynchronous version of H5Mopen
 *
 * Return:      Success:    Object ID of the map
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Mopen_async(const char *app_file, const char *app_func, unsigned app_line, hid_t loc_id, const char *name,
              hid_t mapl_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    hid_t          ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE7("i", "*s*sIui*sii", app_file, app_func, app_line, loc_id, name, mapl_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token;

    /* Open the map asynchronously */
    if ((ret_value = H5M__open_api_common(loc_id, name, mapl_id, token_ptr, &vol_obj)) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTCREATE, H5I_INVALID_HID, "unable to open map asynchronously");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE7(__func__, "*s*sIui*sii", app_file, app_func, app_line, loc_id, name, mapl_id, es_id)) < 0) {
            /* clang-format on */
            if (H5I_dec_app_ref_always_close(ret_value) < 0)
                HDONE_ERROR(H5E_MAP, H5E_CANTDEC, H5I_INVALID_HID, "can't decrement count on map ID");
            HGOTO_ERROR(H5E_MAP, H5E_CANTINSERT, H5I_INVALID_HID, "can't insert token into event set");
        } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Mopen_async() */

/*-------------------------------------------------------------------------
 * Function:    H5Mclose
 *
 * Purpose:     Closes access to a map and releases resources used by it.
 *              It is illegal to subsequently use that same map ID in
 *              calls to other map functions.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Mclose(hid_t map_id)
{
    herr_t ret_value = SUCCEED; /* Return value                     */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", map_id);

    /* Check args */
    if (H5I_MAP != H5I_get_type(map_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a map ID");

    /* Decrement the counter on the map.  It will be freed if the count
     * reaches zero.
     */
    if (H5I_dec_app_ref_always_close(map_id) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTDEC, FAIL, "can't decrement count on map ID");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Mclose() */

/*-------------------------------------------------------------------------
 * Function:    H5Mclose_async
 *
 * Purpose:     Asynchronous version of H5Mclose
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Mclose_async(const char *app_file, const char *app_func, unsigned app_line, hid_t map_id, hid_t es_id)
{
    void          *token     = NULL;            /* Request token for async operation            */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation */
    H5VL_object_t *vol_obj   = NULL;            /* VOL object of dset_id */
    H5VL_t        *connector = NULL;            /* VOL connector */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "*s*sIuii", app_file, app_func, app_line, map_id, es_id);

    /* Check args */
    if (H5I_MAP != H5I_get_type(map_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a map ID");

    /* Get map object's connector */
    if (NULL == (vol_obj = H5VL_vol_object(map_id)))
        HGOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "can't get VOL object for map");

    /* Prepare for possible asynchronous operation */
    if (H5ES_NONE != es_id) {
        /* Increase connector's refcount, so it doesn't get closed if closing
         * the dataset closes the file */
        connector = vol_obj->connector;
        H5VL_conn_inc_rc(connector);

        /* Point at token for operation to set up */
        token_ptr = &token;
    } /* end if */

    /* Decrement the counter on the map.  It will be freed if the count
     * reaches zero.
     */
    if (H5I_dec_app_ref_always_close_async(map_id, token_ptr) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTDEC, FAIL, "can't decrement count on dataset ID");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE5(__func__, "*s*sIuii", app_file, app_func, app_line, map_id, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_MAP, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    if (connector && H5VL_conn_dec_rc(connector) < 0)
        HDONE_ERROR(H5E_MAP, H5E_CANTDEC, FAIL, "can't decrement ref count on connector");

    FUNC_LEAVE_API(ret_value)
} /* end H5Mclose_async() */

/*-------------------------------------------------------------------------
 * Function:    H5Mget_key_type
 *
 * Purpose:     Returns a copy of the key datatype for a map.
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
H5Mget_key_type(hid_t map_id)
{
    H5VL_object_t       *vol_obj;                     /* Map structure    */
    H5VL_optional_args_t vol_cb_args;                 /* Arguments to VOL callback */
    H5VL_map_args_t      map_args;                    /* Arguments for map operations */
    hid_t                ret_value = H5I_INVALID_HID; /* Return value         */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE1("i", "i", map_id);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(map_id, H5I_MAP)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid map identifier");

    /* Set up VOL callback arguments */
    map_args.get.get_type                  = H5VL_MAP_GET_KEY_TYPE;
    map_args.get.args.get_key_type.type_id = H5I_INVALID_HID;
    vol_cb_args.op_type                    = H5VL_MAP_GET;
    vol_cb_args.args                       = &map_args;

    /* Get the key datatype */
    if (H5VL_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTGET, H5I_INVALID_HID, "unable to get key datatype");

    /* Set return value */
    ret_value = map_args.get.args.get_key_type.type_id;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Mget_key_type() */

/*-------------------------------------------------------------------------
 * Function:    H5Mget_val_type
 *
 * Purpose:     Returns a copy of the value datatype for a map.
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
H5Mget_val_type(hid_t map_id)
{
    H5VL_object_t       *vol_obj;                     /* Map structure    */
    H5VL_optional_args_t vol_cb_args;                 /* Arguments to VOL callback */
    H5VL_map_args_t      map_args;                    /* Arguments for map operations */
    hid_t                ret_value = H5I_INVALID_HID; /* Return value         */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE1("i", "i", map_id);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(map_id, H5I_MAP)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid map identifier");

    /* Set up VOL callback arguments */
    map_args.get.get_type                  = H5VL_MAP_GET_VAL_TYPE;
    map_args.get.args.get_val_type.type_id = H5I_INVALID_HID;
    vol_cb_args.op_type                    = H5VL_MAP_GET;
    vol_cb_args.args                       = &map_args;

    /* Get the value datatype */
    if (H5VL_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTGET, H5I_INVALID_HID, "unable to get value datatype");

    /* Set return value */
    ret_value = map_args.get.args.get_val_type.type_id;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Mget_val_type() */

/*-------------------------------------------------------------------------
 * Function:    H5Mget_create_plist
 *
 * Purpose:     Returns a copy of the map creation property list.
 *
 * Return:      Success:    ID for a copy of the map creation
 *                          property list.  The template should be
 *                          released by calling H5P_close().
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Mget_create_plist(hid_t map_id)
{
    H5VL_object_t       *vol_obj;                     /* Map structure    */
    H5VL_optional_args_t vol_cb_args;                 /* Arguments to VOL callback */
    H5VL_map_args_t      map_args;                    /* Arguments for map operations */
    hid_t                ret_value = H5I_INVALID_HID; /* Return value         */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE1("i", "i", map_id);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(map_id, H5I_MAP)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid map identifier");

    /* Set up VOL callback arguments */
    map_args.get.get_type              = H5VL_MAP_GET_MCPL;
    map_args.get.args.get_mcpl.mcpl_id = H5I_INVALID_HID;
    vol_cb_args.op_type                = H5VL_MAP_GET;
    vol_cb_args.args                   = &map_args;

    /* Get the map creation property list */
    if (H5VL_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTGET, H5I_INVALID_HID, "unable to get map creation properties");

    /* Set return value */
    ret_value = map_args.get.args.get_mcpl.mcpl_id;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Mget_create_plist() */

/*-------------------------------------------------------------------------
 * Function:    H5Mget_access_plist
 *
 * Purpose:     Returns a copy of the map access property list.
 *
 * Description: H5Mget_access_plist returns the map access property
 *              list identifier of the specified map.
 *
 * Return:      Success:    ID for a copy of the map access
 *                          property list.  The template should be
 *                          released by calling H5Pclose().
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Mget_access_plist(hid_t map_id)
{
    H5VL_object_t       *vol_obj;                     /* Map structure    */
    H5VL_optional_args_t vol_cb_args;                 /* Arguments to VOL callback */
    H5VL_map_args_t      map_args;                    /* Arguments for map operations */
    hid_t                ret_value = H5I_INVALID_HID; /* Return value         */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE1("i", "i", map_id);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(map_id, H5I_MAP)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid map identifier");

    /* Set up VOL callback arguments */
    map_args.get.get_type              = H5VL_MAP_GET_MAPL;
    map_args.get.args.get_mapl.mapl_id = H5I_INVALID_HID;
    vol_cb_args.op_type                = H5VL_MAP_GET;
    vol_cb_args.args                   = &map_args;

    /* Get the map access property list */
    if (H5VL_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTGET, H5I_INVALID_HID, "unable to get map access properties");

    /* Set return value */
    ret_value = map_args.get.args.get_mapl.mapl_id;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Mget_access_plist() */

/*-------------------------------------------------------------------------
 * Function:    H5Mget_count
 *
 * Purpose:     Returns the number of key-value pairs stored in the map.
 *
 * Description: H5Mget_count returns the number of key-value pairs stored
 *              in the specified map.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Mget_count(hid_t map_id, hsize_t *count /*out*/, hid_t dxpl_id)
{
    H5VL_object_t       *vol_obj;             /* Map structure    */
    H5VL_optional_args_t vol_cb_args;         /* Arguments to VOL callback */
    H5VL_map_args_t      map_args;            /* Arguments for map operations */
    herr_t               ret_value = SUCCEED; /* Return value         */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE3("e", "ixi", map_id, count, dxpl_id);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(map_id, H5I_MAP)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid map identifier");

    /* Get the default dataset transfer property list if the user didn't provide one */
    if (H5P_DEFAULT == dxpl_id)
        dxpl_id = H5P_DATASET_XFER_DEFAULT;
    else if (true != H5P_isa_class(dxpl_id, H5P_DATASET_XFER))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not xfer parms");

    /* Set up VOL callback arguments */
    map_args.get.get_type             = H5VL_MAP_GET_COUNT;
    map_args.get.args.get_count.count = 0;
    vol_cb_args.op_type               = H5VL_MAP_GET;
    vol_cb_args.args                  = &map_args;

    /* Get the number of key-value pairs stored in the map */
    if (H5VL_optional(vol_obj, &vol_cb_args, dxpl_id, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTGET, H5I_INVALID_HID, "unable to get KV pair count for map");

    /* Set value to return */
    if (count)
        *count = map_args.get.args.get_count.count;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Mget_count() */

/*-------------------------------------------------------------------------
 * Function:    H5M__put_api_common
 *
 * Purpose:     This is the common function for putting value to map.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5M__put_api_common(hid_t map_id, hid_t key_mem_type_id, const void *key, hid_t val_mem_type_id,
                    const void *value, hid_t dxpl_id, void **token_ptr, H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_optional_args_t vol_cb_args;                 /* Arguments to VOL callback */
    H5VL_map_args_t      map_args;                    /* Arguments for map operations */
    herr_t               ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (key_mem_type_id < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid key memory datatype ID");
    if (val_mem_type_id < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid value memory datatype ID");

    /* Get map pointer */
    if (NULL == (*vol_obj_ptr = (H5VL_object_t *)H5I_object_verify(map_id, H5I_MAP)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "map_id is not a map ID");

    /* Get the default dataset transfer property list if the user didn't provide one */
    if (H5P_DEFAULT == dxpl_id)
        dxpl_id = H5P_DATASET_XFER_DEFAULT;
    else if (true != H5P_isa_class(dxpl_id, H5P_DATASET_XFER))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not xfer parms");

    /* Set up VOL callback arguments */
    map_args.put.key_mem_type_id   = key_mem_type_id;
    map_args.put.key               = key;
    map_args.put.value_mem_type_id = val_mem_type_id;
    map_args.put.value             = value;
    vol_cb_args.op_type            = H5VL_MAP_PUT;
    vol_cb_args.args               = &map_args;

    /* Set the key/value pair */
    if (H5VL_optional(*vol_obj_ptr, &vol_cb_args, dxpl_id, token_ptr) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTSET, FAIL, "unable to put key/value pair");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5M__put_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Mput
 *
 * Purpose:     H5Mput adds a key-value pair to the Map specified by
 *              MAP_ID, or updates the value for the specified key if one
 *              was set previously. KEY_MEM_TYPE_ID and VAL_MEM_TYPE_ID
 *              specify the datatypes for the provided KEY and VALUE
 *              buffers, and if different from those used to create the
 *              map object, the key and value will be internally converted
 *              to the datatypes for the map object. Any further options
 *              can be specified through the property list DXPL_ID.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Mput(hid_t map_id, hid_t key_mem_type_id, const void *key, hid_t val_mem_type_id, const void *value,
       hid_t dxpl_id)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "ii*xi*xi", map_id, key_mem_type_id, key, val_mem_type_id, value, dxpl_id);

    /* Add key-value pair to the map synchronously */
    if ((ret_value = H5M__put_api_common(map_id, key_mem_type_id, key, val_mem_type_id, value, dxpl_id, NULL,
                                         NULL)) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTPUT, FAIL, "unable to put value to map synchronously");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Mput() */

/*-------------------------------------------------------------------------
 * Function:    H5Mput_async
 *
 * Purpose:     Asynchronous version of H5Mput
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Mput_async(const char *app_file, const char *app_func, unsigned app_line, hid_t map_id,
             hid_t key_mem_type_id, const void *key, hid_t val_mem_type_id, const void *value, hid_t dxpl_id,
             hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE10("e", "*s*sIuii*xi*xii", app_file, app_func, app_line, map_id, key_mem_type_id, key,
              val_mem_type_id, value, dxpl_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token;

    /* Add key-value pair to the map asynchronously */
    if ((ret_value = H5M__put_api_common(map_id, key_mem_type_id, key, val_mem_type_id, value, dxpl_id,
                                         token_ptr, &vol_obj)) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTPUT, FAIL, "unable to put value to map asynchronously");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE10(__func__, "*s*sIuii*xi*xii", app_file, app_func, app_line, map_id, key_mem_type_id, key, val_mem_type_id, value, dxpl_id, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_MAP, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Mput_async() */

/*-------------------------------------------------------------------------
 * Function:    H5M__get_api_common
 *
 * Purpose:     This is common function for getting value from the map.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5M__get_api_common(hid_t map_id, hid_t key_mem_type_id, const void *key, hid_t val_mem_type_id, void *value,
                    hid_t dxpl_id, void **token_ptr, H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_optional_args_t vol_cb_args;                 /* Arguments to VOL callback */
    H5VL_map_args_t      map_args;                    /* Arguments for map operations */
    herr_t               ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (key_mem_type_id < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid key memory datatype ID");
    if (val_mem_type_id < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid value memory datatype ID");

    /* Get map pointer */
    if (NULL == (*vol_obj_ptr = (H5VL_object_t *)H5I_object_verify(map_id, H5I_MAP)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "map_id is not a map ID");

    /* Get the default dataset transfer property list if the user didn't provide one */
    if (H5P_DEFAULT == dxpl_id)
        dxpl_id = H5P_DATASET_XFER_DEFAULT;
    else if (true != H5P_isa_class(dxpl_id, H5P_DATASET_XFER))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not xfer parms");

    /* Set up VOL callback arguments */
    map_args.get_val.key_mem_type_id   = key_mem_type_id;
    map_args.get_val.key               = key;
    map_args.get_val.value_mem_type_id = val_mem_type_id;
    map_args.get_val.value             = value;
    vol_cb_args.op_type                = H5VL_MAP_GET_VAL;
    vol_cb_args.args                   = &map_args;

    /* Get the value for the key */
    if (H5VL_optional(*vol_obj_ptr, &vol_cb_args, dxpl_id, token_ptr) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "unable to get value from map");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5M__get_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Mget
 *
 * Purpose:     H5Mget retrieves, from the Map specified by MAP_ID, the
 *              value associated with the provided key.  KEY_MEM_TYPE_ID
 *              and VAL_MEM_TYPE_ID specify the datatypes for the provided
 *              KEY and VALUE buffers. If KEY_MEM_TYPE_ID is different
 *              from that used to create the map object, the key will be
 *              internally converted to the datatype for the map object
 *              for the query, and if VAL_MEM_TYPE_ID is different from
 *              that used to create the map object, the returned value
 *              will be converted to VAL_MEM_TYPE_ID before the function
 *              returns. Any further options can be specified through the
 *              property list DXPL_ID.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Mget(hid_t map_id, hid_t key_mem_type_id, const void *key, hid_t val_mem_type_id, void *value,
       hid_t dxpl_id)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "ii*xi*xi", map_id, key_mem_type_id, key, val_mem_type_id, value, dxpl_id);

    /* Get key-value pair from the map synchronously */
    if ((ret_value = H5M__get_api_common(map_id, key_mem_type_id, key, val_mem_type_id, value, dxpl_id, NULL,
                                         NULL)) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "unable to get value from map synchronously");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Mget() */

/*-------------------------------------------------------------------------
 * Function:    H5Mget_async
 *
 * Purpose:     Asynchronous version of H5Mget
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Mget_async(const char *app_file, const char *app_func, unsigned app_line, hid_t map_id,
             hid_t key_mem_type_id, const void *key, hid_t val_mem_type_id, void *value, hid_t dxpl_id,
             hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE10("e", "*s*sIuii*xi*xii", app_file, app_func, app_line, map_id, key_mem_type_id, key,
              val_mem_type_id, value, dxpl_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token;

    /* Get key-value pair from the map asynchronously */
    if ((ret_value = H5M__get_api_common(map_id, key_mem_type_id, key, val_mem_type_id, value, dxpl_id,
                                         token_ptr, &vol_obj)) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTGET, FAIL, "unable to get value from map asynchronously");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE10(__func__, "*s*sIuii*xi*xii", app_file, app_func, app_line, map_id, key_mem_type_id, key, val_mem_type_id, value, dxpl_id, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_MAP, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Mget_async() */

/*-------------------------------------------------------------------------
 * Function:    H5Mexists
 *
 * Purpose:     H5Mexists checks if the provided key is stored in the map
 *              specified by MAP_ID. If KEY_MEM_TYPE_ID is different from
 *              that used to create the map object the key will be
 *              internally converted to the datatype for the map object
 *              for the query.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Mexists(hid_t map_id, hid_t key_mem_type_id, const void *key, hbool_t *exists, hid_t dxpl_id)
{
    H5VL_object_t       *vol_obj = NULL;
    H5VL_optional_args_t vol_cb_args;         /* Arguments to VOL callback */
    H5VL_map_args_t      map_args;            /* Arguments for map operations */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "ii*x*bi", map_id, key_mem_type_id, key, exists, dxpl_id);

    /* Check arguments */
    if (key_mem_type_id < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid key memory datatype ID");

    /* Get map pointer */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(map_id, H5I_MAP)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "map_id is not a map ID");

    /* Get the default dataset transfer property list if the user didn't provide one */
    if (H5P_DEFAULT == dxpl_id)
        dxpl_id = H5P_DATASET_XFER_DEFAULT;
    else if (true != H5P_isa_class(dxpl_id, H5P_DATASET_XFER))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not xfer parms");

    /* Set up VOL callback arguments */
    map_args.exists.key_mem_type_id = key_mem_type_id;
    map_args.exists.key             = key;
    map_args.exists.exists          = false;
    vol_cb_args.op_type             = H5VL_MAP_EXISTS;
    vol_cb_args.args                = &map_args;

    /* Check if key exists */
    if (H5VL_optional(vol_obj, &vol_cb_args, dxpl_id, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTGET, ret_value, "unable to check if key exists");

    /* Set value to return */
    if (exists)
        *exists = map_args.exists.exists;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Mexists() */

/*-------------------------------------------------------------------------
 * Function:    H5Miterate
 *
 * Purpose:     H5Miterate iterates over all key-value pairs stored in the
 *              map specified by MAP_ID, making the callback specified by
 *              OP for each. The IDX parameter is an in/out parameter that
 *              may be used to restart a previously interrupted iteration.
 *              At the start of iteration IDX should be set to 0, and to
 *              restart iteration at the same location on a subsequent
 *              call to H5Miterate, IDX should be the same value as
 *              returned by the previous call.
 *
 *              H5M_iterate_t is defined as:
 *              herr_t (*H5M_iterate_t)(hid_t map_id, const void *key,
 *                      void *ctx)
 *
 *              The KEY parameter is the buffer for the key for this
 *              iteration, converted to the datatype specified by
 *              KEY_MEM_TYPE_ID. The OP_DATA parameter is a simple pass
 *              through of the value passed to H5Miterate, which can be
 *              used to store application-defined data for iteration. A
 *              negative return value from this function will cause
 *              H5Miterate to issue an error, while a positive return
 *              value will cause H5Miterate to stop iterating and return
 *              this value without issuing an error. A return value of
 *              zero allows iteration to continue.
 *
 * Return:      Last value returned by op
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Miterate(hid_t map_id, hsize_t *idx, hid_t key_mem_type_id, H5M_iterate_t op, void *op_data, hid_t dxpl_id)
{
    H5VL_object_t       *vol_obj = NULL;
    H5VL_optional_args_t vol_cb_args;         /* Arguments to VOL callback */
    H5VL_map_args_t      map_args;            /* Arguments for map operations */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "i*hiMI*xi", map_id, idx, key_mem_type_id, op, op_data, dxpl_id);

    /* Check arguments */
    if (key_mem_type_id < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid key memory datatype ID");
    if (!op)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no operator specified");

    /* Get map pointer */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(map_id, H5I_MAP)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "map_id is not a map ID");

    /* Get the default dataset transfer property list if the user didn't provide one */
    if (H5P_DEFAULT == dxpl_id)
        dxpl_id = H5P_DATASET_XFER_DEFAULT;
    else if (true != H5P_isa_class(dxpl_id, H5P_DATASET_XFER))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not xfer parms");

    /* Set up VOL callback arguments */
    map_args.specific.specific_type                    = H5VL_MAP_ITER;
    map_args.specific.args.iterate.loc_params.type     = H5VL_OBJECT_BY_SELF;
    map_args.specific.args.iterate.loc_params.obj_type = H5I_get_type(map_id);
    map_args.specific.args.iterate.idx                 = (idx ? *idx : 0);
    map_args.specific.args.iterate.key_mem_type_id     = key_mem_type_id;
    map_args.specific.args.iterate.op                  = op;
    map_args.specific.args.iterate.op_data             = op_data;
    vol_cb_args.op_type                                = H5VL_MAP_SPECIFIC;
    vol_cb_args.args                                   = &map_args;

    /* Iterate over keys */
    if ((ret_value = H5VL_optional(vol_obj, &vol_cb_args, dxpl_id, H5_REQUEST_NULL)) < 0)
        HERROR(H5E_MAP, H5E_BADITER, "unable to iterate over keys");

    /* Set value to return */
    if (idx)
        *idx = map_args.specific.args.iterate.idx;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Miterate() */

/*-------------------------------------------------------------------------
 * Function:    H5Miterate_by_name
 *
 * Purpose:     H5Miterate_by_name iterates over all key-value pairs
 *              stored in the map specified by MAP_ID, making the callback
 *              specified by OP for each. The IDX parameter is an in/out
 *              parameter that may be used to restart a previously
 *              interrupted iteration.  At the start of iteration IDX
 *              should be set to 0, and to restart iteration at the same
 *              location on a subsequent call to H5Miterate, IDX should be
 *              the same value as returned by the previous call.
 *
 *              H5M_iterate_t is defined as:
 *              herr_t (*H5M_iterate_t)(hid_t map_id, const void *key,
 *                      void *ctx)
 *
 *              The KEY parameter is the buffer for the key for this
 *              iteration, converted to the datatype specified by
 *              KEY_MEM_TYPE_ID. The OP_DATA parameter is a simple pass
 *              through of the value passed to H5Miterate, which can be
 *              used to store application-defined data for iteration. A
 *              negative return value from this function will cause
 *              H5Miterate to issue an error, while a positive return
 *              value will cause H5Miterate to stop iterating and return
 *              this value without issuing an error. A return value of
 *              zero allows iteration to continue.
 *
 * Return:      Last value returned by op
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Miterate_by_name(hid_t loc_id, const char *map_name, hsize_t *idx, hid_t key_mem_type_id, H5M_iterate_t op,
                   void *op_data, hid_t dxpl_id, hid_t lapl_id)
{
    H5VL_object_t       *vol_obj = NULL;
    H5VL_optional_args_t vol_cb_args;         /* Arguments to VOL callback */
    H5VL_map_args_t      map_args;            /* Arguments for map operations */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE8("e", "i*s*hiMI*xii", loc_id, map_name, idx, key_mem_type_id, op, op_data, dxpl_id, lapl_id);

    /* Check arguments */
    if (!map_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map_name parameter cannot be NULL");
    if (!*map_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "map_name parameter cannot be an empty string");
    if (key_mem_type_id < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid key memory datatype ID");
    if (!op)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no operator specified");

    /* Get the location object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Get the default dataset transfer property list if the user didn't provide one */
    if (H5P_DEFAULT == dxpl_id)
        dxpl_id = H5P_DATASET_XFER_DEFAULT;
    else if (true != H5P_isa_class(dxpl_id, H5P_DATASET_XFER))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not xfer parms");

    /* Set up VOL callback arguments */
    map_args.specific.specific_type                                        = H5VL_MAP_ITER;
    map_args.specific.args.iterate.loc_params.type                         = H5VL_OBJECT_BY_NAME;
    map_args.specific.args.iterate.loc_params.obj_type                     = H5I_get_type(loc_id);
    map_args.specific.args.iterate.loc_params.loc_data.loc_by_name.name    = map_name;
    map_args.specific.args.iterate.loc_params.loc_data.loc_by_name.lapl_id = lapl_id;
    map_args.specific.args.iterate.idx                                     = (idx ? *idx : 0);
    map_args.specific.args.iterate.key_mem_type_id                         = key_mem_type_id;
    map_args.specific.args.iterate.op                                      = op;
    map_args.specific.args.iterate.op_data                                 = op_data;
    vol_cb_args.op_type                                                    = H5VL_MAP_SPECIFIC;
    vol_cb_args.args                                                       = &map_args;

    /* Iterate over keys */
    if ((ret_value = H5VL_optional(vol_obj, &vol_cb_args, dxpl_id, H5_REQUEST_NULL)) < 0)
        HERROR(H5E_MAP, H5E_BADITER, "unable to ierate over keys");

    /* Set value to return */
    if (idx)
        *idx = map_args.specific.args.iterate.idx;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Miterate_by_name() */

/*-------------------------------------------------------------------------
 * Function:    H5Mdelete
 *
 * Purpose:     H5Mdelete deletes a key-value pair from the Map
 *              specified by MAP_ID. KEY_MEM_TYPE_ID specifies the
 *              datatype for the provided key buffers, and if different
 *              from that used to create the Map object, the key will be
 *              internally converted to the datatype for the map object.
 *              Any further options can be specified through the property
 *              list DXPL_ID.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Mdelete(hid_t map_id, hid_t key_mem_type_id, const void *key, hid_t dxpl_id)
{
    H5VL_object_t       *vol_obj = NULL;
    H5VL_optional_args_t vol_cb_args;         /* Arguments to VOL callback */
    H5VL_map_args_t      map_args;            /* Arguments for map operations */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "ii*xi", map_id, key_mem_type_id, key, dxpl_id);

    /* Check arguments */
    if (key_mem_type_id < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid key memory datatype ID");

    /* Get map pointer */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(map_id, H5I_MAP)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "map_id is not a map ID");

    /* Get the default dataset transfer property list if the user didn't provide one */
    if (H5P_DEFAULT == dxpl_id)
        dxpl_id = H5P_DATASET_XFER_DEFAULT;
    else if (true != H5P_isa_class(dxpl_id, H5P_DATASET_XFER))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not xfer parms");

    /* Set up VOL callback arguments */
    map_args.specific.specific_type                = H5VL_MAP_DELETE;
    map_args.specific.args.del.loc_params.type     = H5VL_OBJECT_BY_SELF;
    map_args.specific.args.del.loc_params.obj_type = H5I_get_type(map_id);
    map_args.specific.args.del.key_mem_type_id     = key_mem_type_id;
    map_args.specific.args.del.key                 = key;
    vol_cb_args.op_type                            = H5VL_MAP_SPECIFIC;
    vol_cb_args.args                               = &map_args;

    /* Delete the key/value pair */
    if (H5VL_optional(vol_obj, &vol_cb_args, dxpl_id, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_MAP, H5E_CANTSET, FAIL, "unable to delete key/value pair");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Mdelete() */

#endif /*  H5_HAVE_MAP_API */
