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
 * Created:	H5G.c
 *
 * Purpose:	Symbol table functions.	 The functions that begin with
 *		'H5G_stab_' don't understand the naming system; they operate
 * 		on a single symbol table at a time.
 *
 *		The functions that begin with 'H5G_node_' operate on the leaf
 *		nodes of a symbol table B-tree.  They should be defined in
 *		the H5Gnode.c file.
 *
 *		The remaining functions know how to traverse the group
 *		directed graph.
 *
 * Names:	Object names are a slash-separated list of components.  If
 *		the name begins with a slash then it's absolute, otherwise
 *		it's relative ("/foo/bar" is absolute while "foo/bar" is
 *		relative).  Multiple consecutive slashes are treated as
 *		single slashes and trailing slashes are ignored.  The special
 *		case `/' is the root group.  Every file has a root group.
 *
 *		API functions that look up names take a location ID and a
 *		name.  The location ID can be a file ID or a group ID and the
 *		name can be relative or absolute.
 *
 *              +--------------+----------- +--------------------------------+
 * 		| Location ID  | Name       | Meaning                        |
 *              +--------------+------------+--------------------------------+
 * 		| File ID      | "/foo/bar" | Find 'foo' within 'bar' within |
 *		|              |            | the root group of the specified|
 *		|              |            | file.                          |
 *              +--------------+------------+--------------------------------+
 * 		| File ID      | "foo/bar"  | Find 'foo' within 'bar' within |
 *		|              |            | the root group of the specified|
 *		|              |            | file.                          |
 *              +--------------+------------+--------------------------------+
 * 		| File ID      | "/"        | The root group of the specified|
 *		|              |            | file.                          |
 *              +--------------+------------+--------------------------------+
 * 		| File ID      | "."        | The root group of the specified|
 *		|              |            | the specified file.            |
 *              +--------------+------------+--------------------------------+
 * 		| Group ID     | "/foo/bar" | Find 'foo' within 'bar' within |
 *		|              |            | the root group of the file     |
 *		|              |            | containing the specified group.|
 *              +--------------+------------+--------------------------------+
 * 		| Group ID     | "foo/bar"  | File 'foo' within 'bar' within |
 *		|              |            | the specified group.           |
 *              +--------------+------------+--------------------------------+
 * 		| Group ID     | "/"        | The root group of the file     |
 *		|              |            | containing the specified group.|
 *              +--------------+------------+--------------------------------+
 * 		| Group ID     | "."        | The specified group.           |
 *              +--------------+------------+--------------------------------+
 *
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Gmodule.h" /* This source code file is part of the H5G module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5CXprivate.h" /* API Contexts                             */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5ESprivate.h" /* Event Sets                               */
#include "H5Gpkg.h"      /* Groups                                   */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5Pprivate.h"  /* Property lists                           */
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
static hid_t  H5G__create_api_common(hid_t loc_id, const char *name, hid_t lcpl_id, hid_t gcpl_id,
                                     hid_t gapl_id, void **token_ptr, H5VL_object_t **_vol_obj_ptr);
static hid_t  H5G__open_api_common(hid_t loc_id, const char *name, hid_t gapl_id, void **token_ptr,
                                   H5VL_object_t **_vol_obj_ptr);
static herr_t H5G__get_info_api_common(hid_t loc_id, H5G_info_t *group_info /*out*/, void **token_ptr,
                                       H5VL_object_t **_vol_obj_ptr);
static herr_t H5G__get_info_by_name_api_common(hid_t loc_id, const char *name, H5G_info_t *group_info /*out*/,
                                               hid_t lapl_id, void **token_ptr, H5VL_object_t **_vol_obj_ptr);
static herr_t H5G__get_info_by_idx_api_common(hid_t loc_id, const char *group_name, H5_index_t idx_type,
                                              H5_iter_order_t order, hsize_t n,
                                              H5G_info_t *group_info /*out*/, hid_t lapl_id, void **token_ptr,
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

/*-------------------------------------------------------------------------
 * Function:    H5G__create_api_common
 *
 * Purpose:     This is the common function for creating HDF5 groups.
 *
 * Return:      Success:    A group ID
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
static hid_t
H5G__create_api_common(hid_t loc_id, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id,
                       void **token_ptr, H5VL_object_t **_vol_obj_ptr)
{
    void           *grp         = NULL; /* Structure for new group */
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
    if (H5VL_setup_acc_args(loc_id, H5P_CLS_GACC, true, &gapl_id, vol_obj_ptr, &loc_params) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, H5I_INVALID_HID, "can't set object access arguments");

    /* Check link creation property list */
    if (H5P_DEFAULT == lcpl_id)
        lcpl_id = H5P_LINK_CREATE_DEFAULT;
    else if (true != H5P_isa_class(lcpl_id, H5P_LINK_CREATE))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not a link creation property list");

    /* Check group creation property list */
    if (H5P_DEFAULT == gcpl_id)
        gcpl_id = H5P_GROUP_CREATE_DEFAULT;
    else if (true != H5P_isa_class(gcpl_id, H5P_GROUP_CREATE))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not a group creation property list");

    /* Set the LCPL for the API context */
    H5CX_set_lcpl(lcpl_id);

    /* Create the group */
    if (NULL == (grp = H5VL_group_create(*vol_obj_ptr, &loc_params, name, lcpl_id, gcpl_id, gapl_id,
                                         H5P_DATASET_XFER_DEFAULT, token_ptr)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, H5I_INVALID_HID, "unable to create group");

    /* Get an ID for the group */
    if ((ret_value = H5VL_register(H5I_GROUP, grp, (*vol_obj_ptr)->connector, true)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to get ID for group handle");

done:
    if (H5I_INVALID_HID == ret_value)
        if (grp && H5VL_group_close(*vol_obj_ptr, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, H5I_INVALID_HID, "unable to release group");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__create_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Gcreate2
 *
 * Purpose:     Creates a new group relative to LOC_ID, giving it the
 *              specified creation property list GCPL_ID and access
 *              property list GAPL_ID.  The link to the new group is
 *              created with the LCPL_ID.
 *
 * Usage:       H5Gcreate2(loc_id, char *name, lcpl_id, gcpl_id, gapl_id)
 *                  hid_t loc_id;	  IN: File or group identifier
 *                  const char *name; IN: Absolute or relative name of the new group
 *                  hid_t lcpl_id;	  IN: Property list for link creation
 *                  hid_t gcpl_id;	  IN: Property list for group creation
 *                  hid_t gapl_id;	  IN: Property list for group access
 *
 * Return:      Success:    The object ID of a new, empty group open for
 *                          writing.  Call H5Gclose() when finished with
 *                          the group.
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Gcreate2(hid_t loc_id, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id)
{
    hid_t ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE5("i", "i*siii", loc_id, name, lcpl_id, gcpl_id, gapl_id);

    /* Create the group synchronously */
    if ((ret_value = H5G__create_api_common(loc_id, name, lcpl_id, gcpl_id, gapl_id, NULL, NULL)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTCREATE, H5I_INVALID_HID, "unable to synchronously create group");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Gcreate2() */

/*-------------------------------------------------------------------------
 * Function:    H5Gcreate_async
 *
 * Purpose:     Asynchronous version of H5Gcreate
 *
 * Return:      Success:    A group ID
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Gcreate_async(const char *app_file, const char *app_func, unsigned app_line, hid_t loc_id, const char *name,
                hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    hid_t          ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE9("i", "*s*sIui*siiii", app_file, app_func, app_line, loc_id, name, lcpl_id, gcpl_id, gapl_id,
             es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Create the group asynchronously */
    if ((ret_value = H5G__create_api_common(loc_id, name, lcpl_id, gcpl_id, gapl_id, token_ptr, &vol_obj)) <
        0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTCREATE, H5I_INVALID_HID, "unable to asynchronously create group");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE9(__func__, "*s*sIui*siiii", app_file, app_func, app_line, loc_id, name, lcpl_id, gcpl_id, gapl_id, es_id)) < 0) {
            /* clang-format on */
            if (H5I_dec_app_ref_always_close(ret_value) < 0)
                HDONE_ERROR(H5E_SYM, H5E_CANTDEC, H5I_INVALID_HID, "can't decrement count on group ID");
            HGOTO_ERROR(H5E_SYM, H5E_CANTINSERT, H5I_INVALID_HID, "can't insert token into event set");
        } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Gcreate_async() */

/*-------------------------------------------------------------------------
 * Function:    H5Gcreate_anon
 *
 * Purpose:     Creates a new group relative to LOC_ID, giving it the
 *              specified creation property list GCPL_ID and access
 *              property list GAPL_ID.
 *
 *              The resulting ID should be linked into the file with
 *              H5Olink or it will be deleted when closed.
 *
 *              Given the default setting, H5Gcreate_anon() followed by
 *              H5Olink() will have the same function as H5Gcreate2().
 *
 * Usage:       H5Gcreate_anon(loc_id, char *name, gcpl_id, gapl_id)
 *                  hid_t loc_id;	  IN: File or group identifier
 *                  const char *name; IN: Absolute or relative name of the new group
 *                  hid_t gcpl_id;	  IN: Property list for group creation
 *                  hid_t gapl_id;	  IN: Property list for group access
 *
 * Example:     To create missing groups "A" and "B01" along the given path "/A/B01/grp"
 *              hid_t create_id = H5Pcreate(H5P_GROUP_CREATE);
 *              int   status = H5Pset_create_intermediate_group(create_id, true);
 *              hid_t gid = H5Gcreate_anon(file_id, "/A/B01/grp", create_id, H5P_DEFAULT);
 *
 * Return:      Success:    The object ID of a new, empty group open for
 *                          writing. Call H5Gclose() when finished with
 *                          the group.
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Gcreate_anon(hid_t loc_id, hid_t gcpl_id, hid_t gapl_id)
{
    void             *grp     = NULL;              /* Structure for new group */
    H5VL_object_t    *vol_obj = NULL;              /* Object for loc_id */
    H5VL_loc_params_t loc_params;                  /* Location parameters for object access */
    hid_t             ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE3("i", "iii", loc_id, gcpl_id, gapl_id);

    /* Check group property list */
    if (H5P_DEFAULT == gcpl_id)
        gcpl_id = H5P_GROUP_CREATE_DEFAULT;
    else if (true != H5P_isa_class(gcpl_id, H5P_GROUP_CREATE))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not group create property list");

    if (H5P_DEFAULT == gapl_id)
        gapl_id = H5P_GROUP_ACCESS_DEFAULT;
    else if (true != H5P_isa_class(gapl_id, H5P_GROUP_ACCESS))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not group access property list");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&gapl_id, H5P_CLS_GACC, loc_id, true) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, H5I_INVALID_HID, "can't set access property list info");

    /* Set location struct fields */
    loc_params.type     = H5VL_OBJECT_BY_SELF;
    loc_params.obj_type = H5I_get_type(loc_id);

    /* Get the location object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid location identifier");

    /* Create the group */
    if (NULL == (grp = H5VL_group_create(vol_obj, &loc_params, NULL, H5P_LINK_CREATE_DEFAULT, gcpl_id,
                                         gapl_id, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, H5I_INVALID_HID, "unable to create group");

    /* Get an ID for the group */
    if ((ret_value = H5VL_register(H5I_GROUP, grp, vol_obj->connector, true)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to get ID for group handle");

done:
    /* Cleanup on failure */
    if (H5I_INVALID_HID == ret_value)
        if (grp && H5VL_group_close(vol_obj, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, H5I_INVALID_HID, "unable to release group");

    FUNC_LEAVE_API(ret_value)
} /* end H5Gcreate_anon() */

/*-------------------------------------------------------------------------
 * Function:    H5G__open_api_common
 *
 * Purpose:     This is the common function for opening HDF5 groups.
 *
 * Return:      Success:    A group ID
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
static hid_t
H5G__open_api_common(hid_t loc_id, const char *name, hid_t gapl_id, void **token_ptr,
                     H5VL_object_t **_vol_obj_ptr)
{
    void           *grp         = NULL; /* Group opened */
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
    if (H5VL_setup_acc_args(loc_id, H5P_CLS_GACC, false, &gapl_id, vol_obj_ptr, &loc_params) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, H5I_INVALID_HID, "can't set object access arguments");

    if (NULL == (grp = H5VL_group_open(*vol_obj_ptr, &loc_params, name, gapl_id, H5P_DATASET_XFER_DEFAULT,
                                       token_ptr)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, H5I_INVALID_HID, "unable to open group");

    /* Register an ID for the group */
    if ((ret_value = H5VL_register(H5I_GROUP, grp, (*vol_obj_ptr)->connector, true)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register group");

done:
    if (H5I_INVALID_HID == ret_value)
        if (grp && H5VL_group_close(*vol_obj_ptr, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, H5I_INVALID_HID, "unable to release group");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5G__open_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Gopen2
 *
 * Purpose:     Opens an existing group for modification.  When finished,
 *              call H5Gclose() to close it and release resources.
 *
 *              This function allows the user the pass in a Group Access
 *              Property List, which H5Gopen1() does not.
 *
 * Return:      Success:    Object ID of the group
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Gopen2(hid_t loc_id, const char *name, hid_t gapl_id)
{
    hid_t ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE3("i", "i*si", loc_id, name, gapl_id);

    /* Open the group synchronously */
    if ((ret_value = H5G__open_api_common(loc_id, name, gapl_id, NULL, NULL)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTCREATE, H5I_INVALID_HID, "unable to synchronously open group");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Gopen2() */

/*-------------------------------------------------------------------------
 * Function:    H5Gopen_async
 *
 * Purpose:     Asynchronous version of H5Gopen2
 *
 * Return:      Success:    A group ID
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Gopen_async(const char *app_file, const char *app_func, unsigned app_line, hid_t loc_id, const char *name,
              hid_t gapl_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    hid_t          ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE7("i", "*s*sIui*sii", app_file, app_func, app_line, loc_id, name, gapl_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Open the group asynchronously */
    if ((ret_value = H5G__open_api_common(loc_id, name, gapl_id, token_ptr, &vol_obj)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTCREATE, H5I_INVALID_HID, "unable to asynchronously open group");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE7(__func__, "*s*sIui*sii", app_file, app_func, app_line, loc_id, name, gapl_id, es_id)) < 0) {
            /* clang-format on */
            if (H5I_dec_app_ref_always_close(ret_value) < 0)
                HDONE_ERROR(H5E_SYM, H5E_CANTDEC, H5I_INVALID_HID, "can't decrement count on group ID");
            HGOTO_ERROR(H5E_SYM, H5E_CANTINSERT, H5I_INVALID_HID, "can't insert token into event set");
        } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Gopen_async() */

/*-------------------------------------------------------------------------
 * Function:    H5Gget_create_plist
 *
 * Purpose:     Returns a copy of the group creation property list.
 *
 * Return:      Success:    ID for a copy of the group creation
 *                          property list.  The property list ID should be
 *                          released by calling H5Pclose().
 *
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Gget_create_plist(hid_t group_id)
{
    H5VL_object_t        *vol_obj;     /* Object for loc_id */
    H5VL_group_get_args_t vol_cb_args; /* Arguments to VOL callback */
    hid_t                 ret_value = H5I_INVALID_HID;

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE1("i", "i", group_id);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(group_id, H5I_GROUP)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not a group ID");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type               = H5VL_GROUP_GET_GCPL;
    vol_cb_args.args.get_gcpl.gcpl_id = H5I_INVALID_HID;

    /* Get the group creation property list for the group */
    if (H5VL_group_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, H5I_INVALID_HID, "can't get group's creation property list");

    /* Set the return value */
    ret_value = vol_cb_args.args.get_gcpl.gcpl_id;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Gget_create_plist() */

/*-------------------------------------------------------------------------
 * Function:    H5G__get_info_api_common
 *
 * Purpose:     This is the common function for retrieving information
 *              about a group.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__get_info_api_common(hid_t loc_id, H5G_info_t *group_info /*out*/, void **token_ptr,
                         H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_group_get_args_t vol_cb_args;                /* Arguments to VOL callback */
    H5I_type_t            id_type;                    /* Type of ID */
    herr_t                ret_value = SUCCEED;        /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    id_type = H5I_get_type(loc_id);
    if (!(H5I_GROUP == id_type || H5I_FILE == id_type))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid group (or file) ID");
    if (!group_info)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "group_info parameter cannot be NULL");

    /* Set up VOL callback & object access arguments */
    vol_cb_args.op_type = H5VL_GROUP_GET_INFO;
    if (H5VL_setup_self_args(loc_id, vol_obj_ptr, &vol_cb_args.args.get_info.loc_params) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, FAIL, "can't set object access arguments");
    vol_cb_args.args.get_info.ginfo = group_info;

    /* Retrieve group information */
    if (H5VL_group_get(*vol_obj_ptr, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, token_ptr) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "unable to get group info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5G__get_info_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Gget_info
 *
 * Purpose:     Retrieve information about a group.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Gget_info(hid_t loc_id, H5G_info_t *group_info /*out*/)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", loc_id, group_info);

    /* Retrieve group information synchronously */
    if (H5G__get_info_api_common(loc_id, group_info, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "unable to synchronously get group info");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Gget_info() */

/*-------------------------------------------------------------------------
 * Function:    H5Gget_info_async
 *
 * Purpose:     Asynchronous version of H5Gget_info
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Gget_info_async(const char *app_file, const char *app_func, unsigned app_line, hid_t loc_id,
                  H5G_info_t *group_info /*out*/, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "*s*sIuixi", app_file, app_func, app_line, loc_id, group_info, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Retrieve group information asynchronously */
    if (H5G__get_info_api_common(loc_id, group_info, token_ptr, &vol_obj) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "unable to asynchronously get group info");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                H5ARG_TRACE6(__func__, "*s*sIuixi", app_file, app_func, app_line, loc_id, group_info, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_SYM, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Gget_info_async() */

/*-------------------------------------------------------------------------
 * Function:    H5G__get_info_by_name_api_common
 *
 * Purpose:     This is the common function for retrieving information
 *              about a group.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__get_info_by_name_api_common(hid_t loc_id, const char *name, H5G_info_t *group_info /*out*/,
                                 hid_t lapl_id, void **token_ptr, H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_group_get_args_t vol_cb_args;                /* Arguments to VOL callback */
    herr_t                ret_value = SUCCEED;        /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    if (!group_info)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "group_info parameter cannot be NULL");

    /* Set up VOL callback & object access arguments */
    vol_cb_args.op_type = H5VL_GROUP_GET_INFO;
    if (H5VL_setup_name_args(loc_id, name, false, lapl_id, vol_obj_ptr,
                             &vol_cb_args.args.get_info.loc_params) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, FAIL, "can't set object access arguments");
    vol_cb_args.args.get_info.ginfo = group_info;

    /* Retrieve group information */
    if (H5VL_group_get(*vol_obj_ptr, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, token_ptr) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "unable to get group info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5G__get_info_by_name_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Gget_info_by_name
 *
 * Purpose:     Retrieve information about a group, where the group is
 *              identified by name instead of ID.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Gget_info_by_name(hid_t loc_id, const char *name, H5G_info_t *group_info /*out*/, hid_t lapl_id)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "i*sxi", loc_id, name, group_info, lapl_id);

    /* Retrieve group information synchronously */
    if (H5G__get_info_by_name_api_common(loc_id, name, group_info, lapl_id, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't synchronously retrieve group info");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Gget_info_by_name() */

/*-------------------------------------------------------------------------
 * Function:    H5Gget_info_by_name_async
 *
 * Purpose:     Asynchronous version of H5Gget_info_by_name
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Gget_info_by_name_async(const char *app_file, const char *app_func, unsigned app_line, hid_t loc_id,
                          const char *name, H5G_info_t *group_info /*out*/, hid_t lapl_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE8("e", "*s*sIui*sxii", app_file, app_func, app_line, loc_id, name, group_info, lapl_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Retrieve group information asynchronously */
    if (H5G__get_info_by_name_api_common(loc_id, name, group_info, lapl_id, token_ptr, &vol_obj) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't asynchronously retrieve group info");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE8(__func__, "*s*sIui*sxii", app_file, app_func, app_line, loc_id, name, group_info, lapl_id, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_SYM, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Gget_info_by_name_async() */

/*-------------------------------------------------------------------------
 * Function:    H5G__get_info_by_idx_api_common
 *
 * Purpose:     This is the common function for retrieving information
 *              about a group.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__get_info_by_idx_api_common(hid_t loc_id, const char *group_name, H5_index_t idx_type,
                                H5_iter_order_t order, hsize_t n, H5G_info_t *group_info /*out*/,
                                hid_t lapl_id, void **token_ptr, H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_group_get_args_t vol_cb_args;                /* Arguments to VOL callback */
    herr_t                ret_value = SUCCEED;        /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    if (!group_info)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "group_info parameter cannot be NULL");

    /* Set up VOL callback & object access arguments */
    vol_cb_args.op_type = H5VL_GROUP_GET_INFO;
    if (H5VL_setup_idx_args(loc_id, group_name, idx_type, order, n, false, lapl_id, vol_obj_ptr,
                            &vol_cb_args.args.get_info.loc_params) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, FAIL, "can't set object access arguments");
    vol_cb_args.args.get_info.ginfo = group_info;

    /* Retrieve group information */
    if (H5VL_group_get(*vol_obj_ptr, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, token_ptr) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "unable to get group info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5G__get_info_by_idx_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Gget_info_by_idx
 *
 * Purpose:     Retrieve information about a group, according to the order
 *              of an index.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Gget_info_by_idx(hid_t loc_id, const char *group_name, H5_index_t idx_type, H5_iter_order_t order,
                   hsize_t n, H5G_info_t *group_info /*out*/, hid_t lapl_id)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE7("e", "i*sIiIohxi", loc_id, group_name, idx_type, order, n, group_info, lapl_id);

    /* Retrieve group information synchronously */
    if (H5G__get_info_by_idx_api_common(loc_id, group_name, idx_type, order, n, group_info, lapl_id, NULL,
                                        NULL) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't synchronously retrieve group info");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Gget_info_by_idx() */

/*-------------------------------------------------------------------------
 * Function:    H5Gget_info_by_idx_async
 *
 * Purpose:     Asynchronous version of H5Gget_info_by_idx
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Gget_info_by_idx_async(const char *app_file, const char *app_func, unsigned app_line, hid_t loc_id,
                         const char *group_name, H5_index_t idx_type, H5_iter_order_t order, hsize_t n,
                         H5G_info_t *group_info /*out*/, hid_t lapl_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE11("e", "*s*sIui*sIiIohxii", app_file, app_func, app_line, loc_id, group_name, idx_type, order, n,
              group_info, lapl_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Retrieve group information asynchronously */
    if (H5G__get_info_by_idx_api_common(loc_id, group_name, idx_type, order, n, group_info, lapl_id,
                                        token_ptr, &vol_obj) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't asynchronously retrieve group info");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE11(__func__, "*s*sIui*sIiIohxii", app_file, app_func, app_line, loc_id, group_name, idx_type, order, n, group_info, lapl_id, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_SYM, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Gget_info_by_idx_async() */

/*-------------------------------------------------------------------------
 * Function:    H5Gclose
 *
 * Purpose:     Closes the specified group. The group ID will no longer be
 *              valid for accessing the group.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Gclose(hid_t group_id)
{
    herr_t ret_value = SUCCEED; /* Return value                     */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", group_id);

    /* Check arguments */
    if (H5I_GROUP != H5I_get_type(group_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a group ID");

    /* Decrement the counter on the group ID. It will be freed if the count
     * reaches zero.
     */
    if (H5I_dec_app_ref(group_id) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTDEC, FAIL, "decrementing group ID failed");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Gclose() */

/*-------------------------------------------------------------------------
 * Function:    H5Gclose_async
 *
 * Purpose:     Asynchronous version of H5Gclose
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Gclose_async(const char *app_file, const char *app_func, unsigned app_line, hid_t group_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    H5VL_t        *connector = NULL;            /* VOL connector */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;         /* Return value                     */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "*s*sIuii", app_file, app_func, app_line, group_id, es_id);

    /* Check arguments */
    if (H5I_GROUP != H5I_get_type(group_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a group ID");

    /* Prepare for possible asynchronous operation */
    if (H5ES_NONE != es_id) {
        /* Get group object's connector */
        if (NULL == (vol_obj = H5VL_vol_object(group_id)))
            HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't get VOL object for group");

        /* Increase connector's refcount, so it doesn't get closed if closing
         * the group closes the file */
        connector = vol_obj->connector;
        H5VL_conn_inc_rc(connector);

        /* Point at token for operation to set up */
        token_ptr = &token;
    } /* end if */

    /* Decrement the counter on the group ID. It will be freed if the count
     * reaches zero.
     */
    if (H5I_dec_app_ref_async(group_id, token_ptr) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTDEC, FAIL, "decrementing group ID failed");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE5(__func__, "*s*sIuii", app_file, app_func, app_line, group_id, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_SYM, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    if (connector && H5VL_conn_dec_rc(connector) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTDEC, FAIL, "can't decrement ref count on connector");

    FUNC_LEAVE_API(ret_value)
} /* end H5Gclose_async() */

/*-------------------------------------------------------------------------
 * Function:    H5Gflush
 *
 * Purpose:     Flushes all buffers associated with a group to disk.
 *
 * Return:      Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Gflush(hid_t group_id)
{
    H5VL_object_t             *vol_obj;             /* Object of loc_id */
    H5VL_group_specific_args_t vol_cb_args;         /* Arguments to VOL callback */
    herr_t                     ret_value = SUCCEED; /* Return value                 */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", group_id);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(group_id, H5I_GROUP)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a group ID");

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(group_id) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, FAIL, "can't set collective metadata read info");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type           = H5VL_GROUP_FLUSH;
    vol_cb_args.args.flush.grp_id = group_id;

    /* Flush group's metadata to file */
    if (H5VL_group_specific(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTFLUSH, FAIL, "unable to flush group");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Gflush */

/*-------------------------------------------------------------------------
 * Function:    H5Grefresh
 *
 * Purpose:     Refreshes all buffers associated with a group.
 *
 * Return:      Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Grefresh(hid_t group_id)
{
    H5VL_object_t             *vol_obj;             /* Object of loc_id */
    H5VL_group_specific_args_t vol_cb_args;         /* Arguments to VOL callback */
    herr_t                     ret_value = SUCCEED; /* Return value                 */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "i", group_id);

    /* Check args */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object_verify(group_id, H5I_GROUP)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a group ID");

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(group_id) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, FAIL, "can't set collective metadata read info");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type             = H5VL_GROUP_REFRESH;
    vol_cb_args.args.refresh.grp_id = group_id;

    /* Refresh group's metadata */
    if (H5VL_group_specific(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTLOAD, FAIL, "unable to refresh group");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Grefresh */
