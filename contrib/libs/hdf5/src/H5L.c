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

#include "H5Lmodule.h" /* This source code file is part of the H5L module */

/***********/
/* Headers */
/***********/
#include "H5private.h"          /* Generic Functions                        */
#include "H5CXprivate.h"        /* API Contexts                             */
#include "H5Eprivate.h"         /* Error handling                           */
#include "H5ESprivate.h"        /* Event Sets                               */
#include "H5Gprivate.h"         /* Groups                                   */
#include "H5Iprivate.h"         /* IDs                                      */
#include "H5Lpkg.h"             /* Links                                    */
#include "H5MMprivate.h"        /* Memory management                        */
#include "H5Pprivate.h"         /* Property lists                           */
#include "H5VLprivate.h"        /* Virtual Object Layer                     */
#include "H5VLnative_private.h" /* Native VOL                               */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Local Prototypes */
/********************/

static herr_t H5L__create_soft_api_common(const char *link_target, hid_t link_loc_id, const char *link_name,
                                          hid_t lcpl_id, hid_t lapl_id, void **token_ptr,
                                          H5VL_object_t **_vol_obj_ptr);
static herr_t H5L__create_hard_api_common(hid_t cur_loc_id, const char *cur_name, hid_t new_loc_id,
                                          const char *new_name, hid_t lcpl_id, hid_t lapl_id,
                                          void **token_ptr, H5VL_object_t **_vol_obj_ptr);
static herr_t H5L__delete_api_common(hid_t loc_id, const char *name, hid_t lapl_id, void **token_ptr,
                                     H5VL_object_t **_vol_obj_ptr);
static herr_t H5L__delete_by_idx_api_common(hid_t loc_id, const char *group_name, H5_index_t idx_type,
                                            H5_iter_order_t order, hsize_t n, hid_t lapl_id, void **token_ptr,
                                            H5VL_object_t **_vol_obj_ptr);
static herr_t H5L__exists_api_common(hid_t loc_id, const char *name, bool *exists, hid_t lapl_id,
                                     void **token_ptr, H5VL_object_t **_vol_obj_ptr);
static herr_t H5L__iterate_api_common(hid_t group_id, H5_index_t idx_type, H5_iter_order_t order,
                                      hsize_t *idx_p, H5L_iterate2_t op, void *op_data, void **token_ptr,
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
 * Function:    H5Lmove
 *
 * Purpose:     Renames an object within an HDF5 file and moves it to a new
 *              group.  The original name SRC is unlinked from the group graph
 *              and then inserted with the new name DST (which can specify a
 *              new path for the object) as an atomic operation. The names
 *              are interpreted relative to SRC_LOC_ID and DST_LOC_ID,
 *              which are either file IDs or group ID.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Lmove(hid_t src_loc_id, const char *src_name, hid_t dst_loc_id, const char *dst_name, hid_t lcpl_id,
        hid_t lapl_id)
{
    H5VL_object_t    *vol_obj1 = NULL; /* Object of src_id */
    H5VL_object_t    *vol_obj2 = NULL; /* Object of dst_id */
    H5VL_loc_params_t loc_params1;
    H5VL_loc_params_t loc_params2;
    H5VL_object_t     tmp_vol_obj;         /* Temporary object */
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "i*si*sii", src_loc_id, src_name, dst_loc_id, dst_name, lcpl_id, lapl_id);

    /* Check arguments */
    if (src_loc_id == H5L_SAME_LOC && dst_loc_id == H5L_SAME_LOC)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "source and destination should not both be H5L_SAME_LOC");
    if (!src_name || !*src_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no current name specified");
    if (!dst_name || !*dst_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no destination name specified");
    if (lcpl_id != H5P_DEFAULT && (true != H5P_isa_class(lcpl_id, H5P_LINK_CREATE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a link creation property list");

    /* Check the link create property list */
    if (H5P_DEFAULT == lcpl_id)
        lcpl_id = H5P_LINK_CREATE_DEFAULT;

    /* Set the LCPL for the API context */
    H5CX_set_lcpl(lcpl_id);

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&lapl_id, H5P_CLS_LACC, ((src_loc_id != H5L_SAME_LOC) ? src_loc_id : dst_loc_id), true) <
        0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTSET, FAIL, "can't set access property list info");

    /* Set location parameter for source object */
    loc_params1.type                         = H5VL_OBJECT_BY_NAME;
    loc_params1.loc_data.loc_by_name.name    = src_name;
    loc_params1.loc_data.loc_by_name.lapl_id = lapl_id;
    loc_params1.obj_type                     = H5I_get_type(src_loc_id);

    /* Set location parameter for destination object */
    loc_params2.type                         = H5VL_OBJECT_BY_NAME;
    loc_params2.loc_data.loc_by_name.name    = dst_name;
    loc_params2.loc_data.loc_by_name.lapl_id = lapl_id;
    loc_params2.obj_type                     = H5I_get_type(dst_loc_id);

    if (H5L_SAME_LOC != src_loc_id)
        /* Get the location object */
        if (NULL == (vol_obj1 = (H5VL_object_t *)H5I_object(src_loc_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");
    if (H5L_SAME_LOC != dst_loc_id)
        /* Get the location object */
        if (NULL == (vol_obj2 = (H5VL_object_t *)H5I_object(dst_loc_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Make sure that the VOL connectors are the same */
    if (vol_obj1 && vol_obj2) {
        int same_connector = 0;

        /* Check if both objects are associated with the same VOL connector */
        if (H5VL_cmp_connector_cls(&same_connector, vol_obj1->connector->cls, vol_obj2->connector->cls) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTCOMPARE, FAIL, "can't compare connector classes");
        if (same_connector)
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL,
                        "Objects are accessed through different VOL connectors and can't be linked");
    }

    /* Construct a temporary source VOL object */
    if (vol_obj1) {
        tmp_vol_obj.connector = vol_obj1->connector;
        tmp_vol_obj.data      = vol_obj1->data;
    }
    else {
        if (NULL == vol_obj2)
            HGOTO_ERROR(H5E_LINK, H5E_BADVALUE, FAIL, "NULL VOL object");

        tmp_vol_obj.connector = vol_obj2->connector;
        tmp_vol_obj.data      = NULL;
    }

    /* Move the link */
    if (H5VL_link_move(&tmp_vol_obj, &loc_params1, vol_obj2, &loc_params2, lcpl_id, lapl_id,
                       H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTMOVE, FAIL, "unable to move link");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Lmove() */

/*-------------------------------------------------------------------------
 * Function:    H5Lcopy
 *
 * Purpose:     Creates an identical copy of a link with the same creation
 *              time and target.  The new link can have a different name
 *              and be in a different location than the original.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Lcopy(hid_t src_loc_id, const char *src_name, hid_t dst_loc_id, const char *dst_name, hid_t lcpl_id,
        hid_t lapl_id)
{
    H5VL_object_t    *vol_obj1 = NULL; /* Object of src_id */
    H5VL_loc_params_t loc_params1;
    H5VL_object_t    *vol_obj2 = NULL; /* Object of dst_id */
    H5VL_loc_params_t loc_params2;
    H5VL_object_t     tmp_vol_obj;         /* Temporary object */
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "i*si*sii", src_loc_id, src_name, dst_loc_id, dst_name, lcpl_id, lapl_id);

    /* Check arguments */
    if (src_loc_id == H5L_SAME_LOC && dst_loc_id == H5L_SAME_LOC)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "source and destination should not both be H5L_SAME_LOC");
    if (!src_name || !*src_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no current name specified");
    if (!dst_name || !*dst_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no destination name specified");
    if (lcpl_id != H5P_DEFAULT && (true != H5P_isa_class(lcpl_id, H5P_LINK_CREATE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a link creation property list");

    /* Check the link create property list */
    if (H5P_DEFAULT == lcpl_id)
        lcpl_id = H5P_LINK_CREATE_DEFAULT;

    /* Set the LCPL for the API context */
    H5CX_set_lcpl(lcpl_id);

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&lapl_id, H5P_CLS_LACC, ((src_loc_id != H5L_SAME_LOC) ? src_loc_id : dst_loc_id), true) <
        0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTSET, FAIL, "can't set access property list info");

    /* Set location parameter for source object */
    loc_params1.type                         = H5VL_OBJECT_BY_NAME;
    loc_params1.loc_data.loc_by_name.name    = src_name;
    loc_params1.loc_data.loc_by_name.lapl_id = lapl_id;
    loc_params1.obj_type                     = H5I_get_type(src_loc_id);

    /* Set location parameter for destination object */
    loc_params2.type                         = H5VL_OBJECT_BY_NAME;
    loc_params2.loc_data.loc_by_name.name    = dst_name;
    loc_params2.loc_data.loc_by_name.lapl_id = lapl_id;
    loc_params2.obj_type                     = H5I_get_type(dst_loc_id);

    if (H5L_SAME_LOC != src_loc_id)
        /* Get the location object */
        if (NULL == (vol_obj1 = (H5VL_object_t *)H5I_object(src_loc_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");
    if (H5L_SAME_LOC != dst_loc_id)
        /* Get the location object */
        if (NULL == (vol_obj2 = (H5VL_object_t *)H5I_object(dst_loc_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Make sure that the VOL connectors are the same */
    if (vol_obj1 && vol_obj2) {
        int same_connector = 0;

        /* Check if both objects are associated with the same VOL connector */
        if (H5VL_cmp_connector_cls(&same_connector, vol_obj1->connector->cls, vol_obj2->connector->cls) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTCOMPARE, FAIL, "can't compare connector classes");
        if (same_connector)
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL,
                        "Objects are accessed through different VOL connectors and can't be linked");
    } /* end if */

    /* Construct a temporary source VOL object */
    if (vol_obj1) {
        tmp_vol_obj.connector = vol_obj1->connector;
        tmp_vol_obj.data      = vol_obj1->data;
    } /* end if */
    else {
        if (NULL == vol_obj2)
            HGOTO_ERROR(H5E_LINK, H5E_BADVALUE, FAIL, "NULL VOL object pointer");

        tmp_vol_obj.connector = vol_obj2->connector;
        tmp_vol_obj.data      = NULL;
    } /* end else */

    /* Copy the link */
    if (H5VL_link_copy(&tmp_vol_obj, &loc_params1, vol_obj2, &loc_params2, lcpl_id, lapl_id,
                       H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTMOVE, FAIL, "unable to copy link");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Lcopy() */

/*-------------------------------------------------------------------------
 * Function:    H5L__create_soft_api_common
 *
 * Purpose:     This is the common function for creating a soft link
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5L__create_soft_api_common(const char *link_target, hid_t link_loc_id, const char *link_name, hid_t lcpl_id,
                            hid_t lapl_id, void **token_ptr, H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_link_create_args_t vol_cb_args;              /* Arguments to VOL callback */
    H5VL_loc_params_t       loc_params;               /* Location parameters for object access */
    herr_t                  ret_value = SUCCEED;      /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (link_loc_id == H5L_SAME_LOC)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "link location id should not be H5L_SAME_LOC");
    if (!link_target)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "link_target parameter cannot be NULL");
    if (!*link_target)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "link_target parameter cannot be an empty string");
    if (lcpl_id != H5P_DEFAULT && (true != H5P_isa_class(lcpl_id, H5P_LINK_CREATE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a link creation property list");
    /* link_name is verified in H5VL_setup_name_args() */

    /* Get the link creation property list */
    if (H5P_DEFAULT == lcpl_id)
        lcpl_id = H5P_LINK_CREATE_DEFAULT;

    /* Set the LCPL for the API context */
    H5CX_set_lcpl(lcpl_id);

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&lapl_id, H5P_CLS_LACC, link_loc_id, true) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTSET, FAIL, "can't set access property list info");

    /* Set up object access arguments */
    if (H5VL_setup_name_args(link_loc_id, link_name, true, lapl_id, vol_obj_ptr, &loc_params) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTSET, FAIL, "can't set object access arguments");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type          = H5VL_LINK_CREATE_SOFT;
    vol_cb_args.args.soft.target = link_target;

    /* Create the link */
    if (H5VL_link_create(&vol_cb_args, *vol_obj_ptr, &loc_params, lcpl_id, lapl_id, H5P_DATASET_XFER_DEFAULT,
                         token_ptr) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTCREATE, FAIL, "unable to create soft link");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5L__create_soft_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Lcreate_soft
 *
 * Purpose:     Creates a soft link from LINK_NAME to LINK_TARGET.
 *
 *              LINK_TARGET can be anything and is interpreted at lookup
 *              time relative to the group which contains the final component
 *              of LINK_NAME.  For instance, if LINK_TARGET is `./foo' and
 *              LINK_NAME is `./x/y/bar' and a request is made for `./x/y/bar'
 *              then the actual object looked up is `./x/y/./foo'.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Lcreate_soft(const char *link_target, hid_t link_loc_id, const char *link_name, hid_t lcpl_id,
               hid_t lapl_id)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "*si*sii", link_target, link_loc_id, link_name, lcpl_id, lapl_id);

    /* Creates a soft link synchronously */
    if (H5L__create_soft_api_common(link_target, link_loc_id, link_name, lcpl_id, lapl_id, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTCREATE, FAIL, "unable to synchronously create soft link");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Lcreate_soft() */

/*-------------------------------------------------------------------------
 * Function:    H5Lcreate_soft_async
 *
 * Purpose:     Asynchronous version of H5Lcreate_soft
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Lcreate_soft_async(const char *app_file, const char *app_func, unsigned app_line, const char *link_target,
                     hid_t link_loc_id, const char *link_name, hid_t lcpl_id, hid_t lapl_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE9("e", "*s*sIu*si*siii", app_file, app_func, app_line, link_target, link_loc_id, link_name,
             lcpl_id, lapl_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Creates a soft link asynchronously */
    if (H5L__create_soft_api_common(link_target, link_loc_id, link_name, lcpl_id, lapl_id, token_ptr,
                                    &vol_obj) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTCREATE, FAIL, "unable to asynchronously create soft link");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE9(__func__, "*s*sIu*si*siii", app_file, app_func, app_line, link_target, link_loc_id, link_name, lcpl_id, lapl_id, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_LINK, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Lcreate_soft_async() */

/*-------------------------------------------------------------------------
 * Function:    H5L__create_hard_api_common
 *
 * Purpose:     This is the common function for creating a hard link
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5L__create_hard_api_common(hid_t cur_loc_id, const char *cur_name, hid_t link_loc_id, const char *link_name,
                            hid_t lcpl_id, hid_t lapl_id, void **token_ptr, H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *curr_vol_obj = NULL;            /* Object of cur_loc_id */
    H5VL_object_t  *link_vol_obj = NULL;            /* Object of link_loc_id */
    H5VL_object_t   tmp_vol_obj;                    /* Temporary object */
    H5VL_object_t  *tmp_vol_obj_ptr = &tmp_vol_obj; /* Ptr to temporary object */
    H5VL_object_t **tmp_vol_obj_ptr_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj_ptr); /* Ptr to ptr to temporary object */
    H5VL_link_create_args_t vol_cb_args;                  /* Arguments to VOL callback */
    H5VL_loc_params_t       link_loc_params;     /* Location parameters for link_loc_id object access */
    herr_t                  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (cur_loc_id == H5L_SAME_LOC && link_loc_id == H5L_SAME_LOC)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "source and destination should not be both H5L_SAME_LOC");
    if (!cur_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "cur_name parameter cannot be NULL");
    if (!*cur_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "cur_name parameter cannot be an empty string");
    if (!link_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "new_name parameter cannot be NULL");
    if (!*link_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "new_name parameter cannot be an empty string");
    if (lcpl_id != H5P_DEFAULT && (true != H5P_isa_class(lcpl_id, H5P_LINK_CREATE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a link creation property list");

    /* Check the link create property list */
    if (H5P_DEFAULT == lcpl_id)
        lcpl_id = H5P_LINK_CREATE_DEFAULT;

    /* Set the LCPL for the API context */
    H5CX_set_lcpl(lcpl_id);

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&lapl_id, H5P_CLS_LACC, cur_loc_id, true) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTSET, FAIL, "can't set access property list info");

    /* Set up new location struct */
    link_loc_params.type                         = H5VL_OBJECT_BY_NAME;
    link_loc_params.obj_type                     = H5I_get_type(link_loc_id);
    link_loc_params.loc_data.loc_by_name.name    = link_name;
    link_loc_params.loc_data.loc_by_name.lapl_id = lapl_id;

    if (H5L_SAME_LOC != cur_loc_id)
        /* Get the current location object */
        if (NULL == (curr_vol_obj = (H5VL_object_t *)H5VL_vol_object(cur_loc_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");
    if (H5L_SAME_LOC != link_loc_id)
        /* Get the new location object */
        if (NULL == (link_vol_obj = (H5VL_object_t *)H5VL_vol_object(link_loc_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Make sure that the VOL connectors are the same */
    if (curr_vol_obj && link_vol_obj) {
        int same_connector = 0;

        /* Check if both objects are associated with the same VOL connector */
        if (H5VL_cmp_connector_cls(&same_connector, curr_vol_obj->connector->cls,
                                   link_vol_obj->connector->cls) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTCOMPARE, FAIL, "can't compare connector classes");
        if (same_connector)
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL,
                        "Objects are accessed through different VOL connectors and can't be linked");
    } /* end if */

    /* Construct a temporary VOL object */
    if (curr_vol_obj)
        (*tmp_vol_obj_ptr_ptr)->connector = curr_vol_obj->connector;
    else {
        if (NULL == link_vol_obj)
            HGOTO_ERROR(H5E_LINK, H5E_BADVALUE, FAIL, "NULL VOL object pointer");

        (*tmp_vol_obj_ptr_ptr)->connector = link_vol_obj->connector;
    } /* end else */
    if (link_vol_obj)
        (*tmp_vol_obj_ptr_ptr)->data = link_vol_obj->data;
    else
        (*tmp_vol_obj_ptr_ptr)->data = NULL;

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                        = H5VL_LINK_CREATE_HARD;
    vol_cb_args.args.hard.curr_obj             = (curr_vol_obj ? curr_vol_obj->data : NULL);
    vol_cb_args.args.hard.curr_loc_params.type = H5VL_OBJECT_BY_NAME;
    vol_cb_args.args.hard.curr_loc_params.obj_type =
        (H5L_SAME_LOC != cur_loc_id ? H5I_get_type(cur_loc_id) : H5I_BADID);
    vol_cb_args.args.hard.curr_loc_params.loc_data.loc_by_name.name    = cur_name;
    vol_cb_args.args.hard.curr_loc_params.loc_data.loc_by_name.lapl_id = lapl_id;

    /* Create the link */
    if (H5VL_link_create(&vol_cb_args, *tmp_vol_obj_ptr_ptr, &link_loc_params, lcpl_id, lapl_id,
                         H5P_DATASET_XFER_DEFAULT, token_ptr) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTCREATE, FAIL, "unable to create hard link");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5L__create_hard_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Lcreate_hard
 *
 * Purpose:     Creates a hard link from NEW_NAME to CUR_NAME.
 *
 *              CUR_NAME must name an existing object.  CUR_NAME and
 *              NEW_NAME are interpreted relative to CUR_LOC_ID and
 *              NEW_LOC_ID, which are either file IDs or group IDs.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Lcreate_hard(hid_t cur_loc_id, const char *cur_name, hid_t new_loc_id, const char *new_name, hid_t lcpl_id,
               hid_t lapl_id)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "i*si*sii", cur_loc_id, cur_name, new_loc_id, new_name, lcpl_id, lapl_id);

    /* Creates a hard link synchronously */
    if (H5L__create_hard_api_common(cur_loc_id, cur_name, new_loc_id, new_name, lcpl_id, lapl_id, NULL,
                                    NULL) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTCREATE, FAIL, "unable to synchronously create hard link");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Lcreate_hard() */

/*-------------------------------------------------------------------------
 * Function:    H5Lcreate_hard_async
 *
 * Purpose:     Asynchronous version of H5Lcreate_hard
 *
 * Note:        The implementation for this routine is different from other
 *              _async operations, as the 'api_common' routine needs a "real"
 *              H5VL_object_t to point at, which is usually provided by the
 *              loc_id, but isn't here.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Lcreate_hard_async(const char *app_file, const char *app_func, unsigned app_line, hid_t cur_loc_id,
                     const char *cur_name, hid_t new_loc_id, const char *new_name, hid_t lcpl_id,
                     hid_t lapl_id, hid_t es_id)
{
    H5VL_object_t  vol_obj;                       /* Object for loc_id */
    H5VL_object_t *vol_obj_ptr = &vol_obj;        /* Pointer to object for loc_id */
    void          *token       = NULL;            /* Request token for async operation        */
    void         **token_ptr   = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value   = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE10("e", "*s*sIui*si*siii", app_file, app_func, app_line, cur_loc_id, cur_name, new_loc_id,
              new_name, lcpl_id, lapl_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Creates a hard link asynchronously */
    if (H5L__create_hard_api_common(cur_loc_id, cur_name, new_loc_id, new_name, lcpl_id, lapl_id, token_ptr,
                                    &vol_obj_ptr) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTCREATE, FAIL, "unable to asynchronously create hard link");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj_ptr->connector, token,
                        H5ARG_TRACE10(__func__, "*s*sIui*si*siii", app_file, app_func, app_line, cur_loc_id, cur_name, new_loc_id, new_name, lcpl_id, lapl_id, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_LINK, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Lcreate_hard_async() */

/*-------------------------------------------------------------------------
 * Function:    H5Lcreate_external
 *
 * Purpose:     Creates an external link from LINK_NAME to OBJ_NAME.
 *
 *              External links are links to objects in other HDF5 files.  They
 *              are allowed to "dangle" like soft links internal to a file.
 *              FILE_NAME is the name of the file that OBJ_NAME is contained
 *              within.  If OBJ_NAME is given as a relative path name, the
 *              path will be relative to the root group of FILE_NAME.
 *              LINK_NAME is interpreted relative to LINK_LOC_ID, which is
 *              either a file ID or a group ID.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Lcreate_external(const char *file_name, const char *obj_name, hid_t link_loc_id, const char *link_name,
                   hid_t lcpl_id, hid_t lapl_id)
{
    H5VL_object_t          *vol_obj = NULL;       /* Object of loc_id */
    H5VL_link_create_args_t vol_cb_args;          /* Arguments to VOL callback */
    H5VL_loc_params_t       loc_params;           /* Location parameters for object access */
    char                   *norm_obj_name = NULL; /* Pointer to normalized current name */
    void                   *ext_link_buf  = NULL; /* Buffer to contain external link */
    size_t                  buf_size;             /* Size of buffer to hold external link */
    size_t                  file_name_len;        /* Length of file name string */
    size_t                  norm_obj_name_len;    /* Length of normalized object name string */
    uint8_t                *p;                    /* Pointer into external link buffer */
    herr_t                  ret_value = SUCCEED;  /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "*s*si*sii", file_name, obj_name, link_loc_id, link_name, lcpl_id, lapl_id);

    /* Check arguments */
    if (!file_name || !*file_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no file name specified");
    if (!obj_name || !*obj_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no object name specified");
    if (!link_name || !*link_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no link name specified");

    /* Get the link creation property list */
    if (H5P_DEFAULT == lcpl_id)
        lcpl_id = H5P_LINK_CREATE_DEFAULT;

    /* Set the LCPL for the API context */
    H5CX_set_lcpl(lcpl_id);

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&lapl_id, H5P_CLS_LACC, link_loc_id, true) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTSET, FAIL, "can't set access property list info");

    /* Get normalized copy of the link target */
    if (NULL == (norm_obj_name = H5G_normalize(obj_name)))
        HGOTO_ERROR(H5E_LINK, H5E_BADVALUE, FAIL, "can't normalize object name");

    /* Combine the filename and link name into a single buffer to give to the UD link */
    file_name_len     = strlen(file_name) + 1;
    norm_obj_name_len = strlen(norm_obj_name) + 1;
    buf_size          = 1 + file_name_len + norm_obj_name_len;
    if (NULL == (ext_link_buf = H5MM_malloc(buf_size)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "unable to allocate udata buffer");

    /* Encode the external link information */
    p    = (uint8_t *)ext_link_buf;
    *p++ = (H5L_EXT_VERSION << 4) | H5L_EXT_FLAGS_ALL; /* External link version & flags */
    strncpy((char *)p, file_name, buf_size - 1);       /* Name of file containing external link's object */
    p += file_name_len;
    strncpy((char *)p, norm_obj_name, buf_size - (file_name_len + 1)); /* External link's object */

    loc_params.type                         = H5VL_OBJECT_BY_NAME;
    loc_params.loc_data.loc_by_name.name    = link_name;
    loc_params.loc_data.loc_by_name.lapl_id = lapl_id;
    loc_params.obj_type                     = H5I_get_type(link_loc_id);

    /* get the location object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(link_loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid object identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type          = H5VL_LINK_CREATE_UD;
    vol_cb_args.args.ud.type     = H5L_TYPE_EXTERNAL;
    vol_cb_args.args.ud.buf      = ext_link_buf;
    vol_cb_args.args.ud.buf_size = buf_size;

    /* Create an external link */
    if (H5VL_link_create(&vol_cb_args, vol_obj, &loc_params, lcpl_id, lapl_id, H5P_DATASET_XFER_DEFAULT,
                         H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTINIT, FAIL, "unable to create external link");

done:
    H5MM_xfree(ext_link_buf);
    H5MM_xfree(norm_obj_name);

    FUNC_LEAVE_API(ret_value)
} /* end H5Lcreate_external() */

/*-------------------------------------------------------------------------
 * Function:    H5Lcreate_ud
 *
 * Purpose:     Creates a user-defined link of type LINK_TYPE named LINK_NAME
 *              with user-specified data UDATA.
 *
 *              The format of the information pointed to by UDATA is
 *              defined by the user. UDATA_SIZE holds the size of this buffer.
 *
 *              LINK_NAME is interpreted relative to LINK_LOC_ID.
 *
 *              The property list specified by LCPL_ID holds properties used
 *              to create the link.
 *
 *              The link class of the new link must already be registered
 *              with the library.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Lcreate_ud(hid_t link_loc_id, const char *link_name, H5L_type_t link_type, const void *udata,
             size_t udata_size, hid_t lcpl_id, hid_t lapl_id)
{
    H5VL_object_t          *vol_obj = NULL;      /* Object of loc_id */
    H5VL_link_create_args_t vol_cb_args;         /* Arguments to VOL callback */
    H5VL_loc_params_t       loc_params;          /* Location parameters for object access */
    herr_t                  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE7("e", "i*sLl*xzii", link_loc_id, link_name, link_type, udata, udata_size, lcpl_id, lapl_id);

    /* Check arguments */
    if (!link_name || !*link_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no link name specified");
    if (link_type < H5L_TYPE_UD_MIN || link_type > H5L_TYPE_MAX)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid link class");
    if (!udata && udata_size)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "udata cannot be NULL if udata_size is non-zero");

    /* Get the link creation property list */
    if (H5P_DEFAULT == lcpl_id)
        lcpl_id = H5P_LINK_CREATE_DEFAULT;

    /* Set the LCPL for the API context */
    H5CX_set_lcpl(lcpl_id);

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&lapl_id, H5P_CLS_LACC, link_loc_id, true) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTSET, FAIL, "can't set access property list info");

    loc_params.type                         = H5VL_OBJECT_BY_NAME;
    loc_params.loc_data.loc_by_name.name    = link_name;
    loc_params.loc_data.loc_by_name.lapl_id = lapl_id;
    loc_params.obj_type                     = H5I_get_type(link_loc_id);

    /* get the location object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(link_loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type          = H5VL_LINK_CREATE_UD;
    vol_cb_args.args.ud.type     = link_type;
    vol_cb_args.args.ud.buf      = udata;
    vol_cb_args.args.ud.buf_size = udata_size;

    /* Create user-defined link */
    if (H5VL_link_create(&vol_cb_args, vol_obj, &loc_params, lcpl_id, lapl_id, H5P_DATASET_XFER_DEFAULT,
                         H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTINIT, FAIL, "unable to create link");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Lcreate_ud() */

/*-------------------------------------------------------------------------
 * Function:    H5L__delete_api_common
 *
 * Purpose:     This is the common function for deleting a link
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5L__delete_api_common(hid_t loc_id, const char *name, hid_t lapl_id, void **token_ptr,
                       H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_link_specific_args_t vol_cb_args;            /* Arguments to VOL callback */
    H5VL_loc_params_t         loc_params;             /* Location parameters for object access */
    herr_t                    ret_value = SUCCEED;    /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    /* name is verified in H5VL_setup_name_args() */

    /* Set up object access arguments */
    if (H5VL_setup_name_args(loc_id, name, true, lapl_id, vol_obj_ptr, &loc_params) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTSET, FAIL, "can't set object access arguments");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type = H5VL_LINK_DELETE;

    /* Delete link */
    if (H5VL_link_specific(*vol_obj_ptr, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, token_ptr) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTDELETE, FAIL, "unable to delete link");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5L__delete_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Ldelete
 *
 * Purpose:     Removes the specified NAME from the group graph and
 *              decrements the link count for the object to which NAME
 *              points. If the link count reaches zero then all file-space
 *              associated with the object will be reclaimed (but if the
 *              object is open, then the reclamation of the file space is
 *              delayed until all handles to the object are closed).
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Ldelete(hid_t loc_id, const char *name, hid_t lapl_id)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "i*si", loc_id, name, lapl_id);

    /* Delete a link synchronously */
    if (H5L__delete_api_common(loc_id, name, lapl_id, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTDELETE, FAIL, "unable to synchronously delete link");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Ldelete() */

/*-------------------------------------------------------------------------
 * Function:    H5Ldelete_async
 *
 * Purpose:     Asynchronous version of H5Ldelete
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Ldelete_async(const char *app_file, const char *app_func, unsigned app_line, hid_t loc_id, const char *name,
                hid_t lapl_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE7("e", "*s*sIui*sii", app_file, app_func, app_line, loc_id, name, lapl_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Delete a link asynchronously */
    if (H5L__delete_api_common(loc_id, name, lapl_id, token_ptr, &vol_obj) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTDELETE, FAIL, "unable to asynchronously delete link");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE7(__func__, "*s*sIui*sii", app_file, app_func, app_line, loc_id, name, lapl_id, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_LINK, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Ldelete_async() */

/*-------------------------------------------------------------------------
 * Function:    H5L__delete_by_idx_api_common
 *
 * Purpose:     This is the common function for deleting a link
 *		        according to the order within an index.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5L__delete_by_idx_api_common(hid_t loc_id, const char *group_name, H5_index_t idx_type,
                              H5_iter_order_t order, hsize_t n, hid_t lapl_id, void **token_ptr,
                              H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_link_specific_args_t vol_cb_args;            /* Arguments to VOL callback */
    H5VL_loc_params_t         loc_params;             /* Location parameters for object access */
    herr_t                    ret_value = SUCCEED;    /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (!group_name || !*group_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no name specified");
    if (idx_type <= H5_INDEX_UNKNOWN || idx_type >= H5_INDEX_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid index type specified");
    if (order <= H5_ITER_UNKNOWN || order >= H5_ITER_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid iteration order specified");

    /* Set up object access arguments */
    if (H5VL_setup_idx_args(loc_id, group_name, idx_type, order, n, true, lapl_id, vol_obj_ptr, &loc_params) <
        0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTSET, FAIL, "can't set object access arguments");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type = H5VL_LINK_DELETE;

    /* Delete the link */
    if (H5VL_link_specific(*vol_obj_ptr, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, token_ptr) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTDELETE, FAIL, "unable to delete link");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5L__delete_by_idx_api_common() */

/*-------------------------------------------------------------------------
 * Function:	H5Ldelete_by_idx
 *
 * Purpose:	Removes the specified link from the group graph and
 *		decrements the link count for the object to which it
 *		points, according to the order within an index.
 *
 *		If the link count reaches zero then all file-space
 *		associated with the object will be reclaimed (but if the
 *		object is open, then the reclamation of the file space is
 *		delayed until all handles to the object are closed).
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Ldelete_by_idx(hid_t loc_id, const char *group_name, H5_index_t idx_type, H5_iter_order_t order, hsize_t n,
                 hid_t lapl_id)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "i*sIiIohi", loc_id, group_name, idx_type, order, n, lapl_id);

    /* Delete a link synchronously */
    if (H5L__delete_by_idx_api_common(loc_id, group_name, idx_type, order, n, lapl_id, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTDELETE, FAIL, "unable to synchronously delete link");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Ldelete_by_idx() */

/*-------------------------------------------------------------------------
 * Function:    H5Ldelete_by_idx_async
 *
 * Purpose:     Asynchronous version of H5Ldelete_by_idx
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Ldelete_by_idx_async(const char *app_file, const char *app_func, unsigned app_line, hid_t loc_id,
                       const char *group_name, H5_index_t idx_type, H5_iter_order_t order, hsize_t n,
                       hid_t lapl_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE10("e", "*s*sIui*sIiIohii", app_file, app_func, app_line, loc_id, group_name, idx_type, order, n,
              lapl_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Delete a link asynchronously */
    if (H5L__delete_by_idx_api_common(loc_id, group_name, idx_type, order, n, lapl_id, token_ptr, &vol_obj) <
        0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTDELETE, FAIL, "unable to asynchronously delete link");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE10(__func__, "*s*sIui*sIiIohii", app_file, app_func, app_line, loc_id, group_name, idx_type, order, n, lapl_id, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_LINK, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Ldelete_by_idx_async() */

/*-------------------------------------------------------------------------
 * Function:	H5Lget_val
 *
 * Purpose:	Returns the link value of a link whose name is NAME.  For
 *              symbolic links, this is the path to which the link points,
 *              including the null terminator.  For user-defined links, it
 *              is the link buffer.
 *
 *              At most SIZE bytes are copied to the BUF result buffer.
 *
 * Return:	Success:	Non-negative with the link value in BUF.
 *
 * 		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Lget_val(hid_t loc_id, const char *name, void *buf /*out*/, size_t size, hid_t lapl_id)
{
    H5VL_object_t       *vol_obj = NULL;      /* object of loc_id */
    H5VL_link_get_args_t vol_cb_args;         /* Arguments to VOL callback */
    H5VL_loc_params_t    loc_params;          /* Location parameters for object access */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "i*sxzi", loc_id, name, buf, size, lapl_id);

    /* Check arguments */
    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no name specified");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&lapl_id, H5P_CLS_LACC, loc_id, false) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTSET, FAIL, "can't set access property list info");

    /* Set up location struct */
    loc_params.type                         = H5VL_OBJECT_BY_NAME;
    loc_params.obj_type                     = H5I_get_type(loc_id);
    loc_params.loc_data.loc_by_name.name    = name;
    loc_params.loc_data.loc_by_name.lapl_id = lapl_id;

    /* Get the VOL object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type               = H5VL_LINK_GET_VAL;
    vol_cb_args.args.get_val.buf      = buf;
    vol_cb_args.args.get_val.buf_size = size;

    /* Get the link value */
    if (H5VL_link_get(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "unable to get link value for '%s'", name);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Lget_val() */

/*-------------------------------------------------------------------------
 * Function:	H5Lget_val_by_idx
 *
 * Purpose:	Returns the link value of a link, according to the order of
 *              an index.  For symbolic links, this is the path to which the
 *              link points, including the null terminator.  For user-defined
 *              links, it is the link buffer.
 *
 *              At most SIZE bytes are copied to the BUF result buffer.
 *
 * Return:	Success:	Non-negative with the link value in BUF.
 * 		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Lget_val_by_idx(hid_t loc_id, const char *group_name, H5_index_t idx_type, H5_iter_order_t order, hsize_t n,
                  void *buf /*out*/, size_t size, hid_t lapl_id)
{
    H5VL_object_t       *vol_obj = NULL;      /* object of loc_id */
    H5VL_link_get_args_t vol_cb_args;         /* Arguments to VOL callback */
    H5VL_loc_params_t    loc_params;          /* Location parameters for object access */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE8("e", "i*sIiIohxzi", loc_id, group_name, idx_type, order, n, buf, size, lapl_id);

    /* Check arguments */
    if (!group_name || !*group_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no name specified");
    if (idx_type <= H5_INDEX_UNKNOWN || idx_type >= H5_INDEX_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid index type specified");
    if (order <= H5_ITER_UNKNOWN || order >= H5_ITER_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid iteration order specified");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&lapl_id, H5P_CLS_LACC, loc_id, false) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTSET, FAIL, "can't set access property list info");

    /* Set up location struct */
    loc_params.type                         = H5VL_OBJECT_BY_IDX;
    loc_params.loc_data.loc_by_idx.name     = group_name;
    loc_params.loc_data.loc_by_idx.idx_type = idx_type;
    loc_params.loc_data.loc_by_idx.order    = order;
    loc_params.loc_data.loc_by_idx.n        = n;
    loc_params.loc_data.loc_by_idx.lapl_id  = lapl_id;
    loc_params.obj_type                     = H5I_get_type(loc_id);

    /* Get the VOL object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type               = H5VL_LINK_GET_VAL;
    vol_cb_args.args.get_val.buf      = buf;
    vol_cb_args.args.get_val.buf_size = size;

    /* Get the link value */
    if (H5VL_link_get(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "unable to get link value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Lget_val_by_idx() */

/*--------------------------------------------------------------------------
 *  NAME
 *      H5L__exists_api_common
 *  PURPOSE
 *      Common helper routine for sync/async check if an attribute exists
 *  RETURNS
 *      Non-negative on success/Negative on failure
 *
 *--------------------------------------------------------------------------*/
static herr_t
H5L__exists_api_common(hid_t loc_id, const char *name, bool *exists, hid_t lapl_id, void **token_ptr,
                       H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj); /* Ptr to object ptr for loc_id */
    H5VL_link_specific_args_t vol_cb_args;            /* Arguments to VOL callback */
    H5VL_loc_params_t         loc_params;             /* Location parameters for object access */
    herr_t                    ret_value = SUCCEED;    /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    /* name is verified in H5VL_setup_name_args() */
    if (NULL == exists)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid pointer for link existence");

    /* Set up object access arguments */
    if (H5VL_setup_name_args(loc_id, name, false, lapl_id, vol_obj_ptr, &loc_params) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTSET, FAIL, "can't set object access arguments");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type            = H5VL_LINK_EXISTS;
    vol_cb_args.args.exists.exists = exists;

    /* Check for the existence of the link */
    if (H5VL_link_specific(*vol_obj_ptr, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, token_ptr) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "unable to get link info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5L__exists_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Lexists
 *
 * Purpose:     Checks if a link of a given name exists in a group
 *
 * Return:      Success:    true/false/FAIL
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5Lexists(hid_t loc_id, const char *name, hid_t lapl_id)
{
    bool   exists;           /* Flag to indicate if link exists */
    htri_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("t", "i*si", loc_id, name, lapl_id);

    /* Synchronously check if a link exists */
    exists = false;
    if (H5L__exists_api_common(loc_id, name, &exists, lapl_id, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "unable to synchronously check link existence");

    /* Set return value */
    ret_value = (htri_t)exists;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Lexists() */

/*--------------------------------------------------------------------------
 * Function:    H5Lexists_async
 *
 * Purpose:     Asynchronous version of H5Lexists
 *
 * Return:      Success:    true/false/FAIL
 *
 *--------------------------------------------------------------------------*/
herr_t
H5Lexists_async(const char *app_file, const char *app_func, unsigned app_line, hid_t loc_id, const char *name,
                hbool_t *exists, hid_t lapl_id, hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value = SUCCEED;         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE8("e", "*s*sIui*s*bii", app_file, app_func, app_line, loc_id, name, exists, lapl_id, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Asynchronously check if a link exists */
    if (H5L__exists_api_common(loc_id, name, exists, lapl_id, token_ptr, &vol_obj) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "unable to asynchronously check link existence");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        /* clang-format off */
                        H5ARG_TRACE8(__func__, "*s*sIui*s*bii", app_file, app_func, app_line, loc_id, name, exists, lapl_id, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_LINK, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Lexists_async() */

/*-------------------------------------------------------------------------
 * Function:    H5Lget_info2
 *
 * Purpose:     Gets metadata for a link.
 *
 * Return:      Success:    Non-negative with information in LINFO
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Lget_info2(hid_t loc_id, const char *name, H5L_info2_t *linfo /*out*/, hid_t lapl_id)
{
    H5VL_object_t       *vol_obj = NULL;      /* object of loc_id */
    H5VL_link_get_args_t vol_cb_args;         /* Arguments to VOL callback */
    H5VL_loc_params_t    loc_params;          /* Location parameters for object access */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "i*sxi", loc_id, name, linfo, lapl_id);

    /* Check arguments */
    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no name specified");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&lapl_id, H5P_CLS_LACC, loc_id, true) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTSET, FAIL, "can't set access property list info");

    /* Set up location struct */
    loc_params.type                         = H5VL_OBJECT_BY_NAME;
    loc_params.obj_type                     = H5I_get_type(loc_id);
    loc_params.loc_data.loc_by_name.name    = name;
    loc_params.loc_data.loc_by_name.lapl_id = lapl_id;

    /* Get the location object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type             = H5VL_LINK_GET_INFO;
    vol_cb_args.args.get_info.linfo = linfo;

    /* Get the link information */
    if (H5VL_link_get(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "unable to get link info");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Lget_info2() */

/*-------------------------------------------------------------------------
 * Function:    H5Lget_info_by_idx2
 *
 * Purpose:     Gets metadata for a link, according to the order within an
 *              index.
 *
 * Return:      Success:    Non-negative with information in LINFO
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Lget_info_by_idx2(hid_t loc_id, const char *group_name, H5_index_t idx_type, H5_iter_order_t order,
                    hsize_t n, H5L_info2_t *linfo /*out*/, hid_t lapl_id)
{
    H5VL_object_t       *vol_obj = NULL;      /* object of loc_id */
    H5VL_link_get_args_t vol_cb_args;         /* Arguments to VOL callback */
    H5VL_loc_params_t    loc_params;          /* Location parameters for object access */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE7("e", "i*sIiIohxi", loc_id, group_name, idx_type, order, n, linfo, lapl_id);

    /* Check arguments */
    if (!group_name || !*group_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no name specified");
    if (idx_type <= H5_INDEX_UNKNOWN || idx_type >= H5_INDEX_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid index type specified");
    if (order <= H5_ITER_UNKNOWN || order >= H5_ITER_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid iteration order specified");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&lapl_id, H5P_CLS_LACC, loc_id, false) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTSET, FAIL, "can't set access property list info");

    /* Set up location struct */
    loc_params.type                         = H5VL_OBJECT_BY_IDX;
    loc_params.loc_data.loc_by_idx.name     = group_name;
    loc_params.loc_data.loc_by_idx.idx_type = idx_type;
    loc_params.loc_data.loc_by_idx.order    = order;
    loc_params.loc_data.loc_by_idx.n        = n;
    loc_params.loc_data.loc_by_idx.lapl_id  = lapl_id;
    loc_params.obj_type                     = H5I_get_type(loc_id);

    /* Get the location object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type             = H5VL_LINK_GET_INFO;
    vol_cb_args.args.get_info.linfo = linfo;

    /* Get the link information */
    if (H5VL_link_get(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "unable to get link info");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Lget_info_by_idx2() */

/*-------------------------------------------------------------------------
 * Function:	H5Lregister
 *
 * Purpose:	Registers a class of user-defined links, or changes the
 *              behavior of an existing class.
 *
 *              The link class passed in will override any existing link
 *              class for the specified link class ID. It must at least
 *              include a H5L_class_t version (which should be
 *              H5L_LINK_CLASS_T_VERS), a link class ID, and a traversal
 *              function.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Lregister(const H5L_class_t *cls)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "*#", cls);

    /* Check args */
    if (cls == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid link class");

    /* Check H5L_class_t version number; this is where a function to convert
     * from an outdated version should be called.
     *
     * v0 of the H5L_class_t is only different in the parameters to the
     * traversal callback, which is handled in H5G_traverse_ud()
     * (in src/H5Gtraverse.c), so it's allowed to to pass through here. - QAK, 2018/02/06
     */
    if (cls->version > H5L_LINK_CLASS_T_VERS)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid H5L_class_t version number");
#ifdef H5_NO_DEPRECATED_SYMBOLS
    if (cls->version < H5L_LINK_CLASS_T_VERS)
        HGOTO_ERROR(
            H5E_ARGS, H5E_BADVALUE, FAIL,
            "deprecated H5L_class_t version number (%d) and library built without deprecated symbol support",
            cls->version);
#endif /* H5_NO_DEPRECATED_SYMBOLS */

    if (cls->id < H5L_TYPE_UD_MIN || cls->id > H5L_TYPE_MAX)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid link identification number");
    if (cls->trav_func == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no traversal function specified");

    /* Do it */
    if (H5L_register(cls) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_NOTREGISTERED, FAIL, "unable to register link type");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Lregister() */

/*-------------------------------------------------------------------------
 * Function:	H5Lunregister
 *
 * Purpose:	Unregisters a class of user-defined links, preventing them
 *              from being traversed, queried, moved, etc.
 *
 *              A link class can be re-registered using H5Lregister().
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Lunregister(H5L_type_t id)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("e", "Ll", id);

    /* Check args */
    if (id < 0 || id > H5L_TYPE_MAX)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid link type");

    /* Do it */
    if (H5L_unregister(id) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_NOTREGISTERED, FAIL, "unable to unregister link type");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Lunregister() */

/*-------------------------------------------------------------------------
 * Function:    H5Lis_registered
 *
 * Purpose:     Tests whether a user-defined link class has been registered
 *              or not.
 *
 * Return:      true if the link class has been registered
 *              false if it is unregistered
 *              FAIL on error (if the class is not a valid UD class ID)
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5Lis_registered(H5L_type_t id)
{
    bool   is_registered = false;
    htri_t ret_value     = false; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("t", "Ll", id);

    /* Check args */
    if (id < 0 || id > H5L_TYPE_MAX)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid link type id number");

    /* Is the link class already registered? */
    if (H5L_is_registered(id, &is_registered) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_BADTYPE, FAIL, "could not determine registration status of UD link type");

    ret_value = is_registered ? true : false;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Lis_registered() */

/*-------------------------------------------------------------------------
 * Function:    H5Lget_name_by_idx
 *
 * Purpose:     Gets name for a link, according to the order within an
 *              index.
 *
 *              Same pattern of behavior as H5Iget_name.
 *
 * Return:      Success:    Non-negative length of name, with information
 *                          in NAME buffer
 *
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
ssize_t
H5Lget_name_by_idx(hid_t loc_id, const char *group_name, H5_index_t idx_type, H5_iter_order_t order,
                   hsize_t n, char *name /*out*/, size_t size, hid_t lapl_id)
{
    H5VL_object_t       *vol_obj = NULL;     /* object of loc_id */
    H5VL_link_get_args_t vol_cb_args;        /* Arguments to VOL callback */
    H5VL_loc_params_t    loc_params;         /* Location parameters for object access */
    size_t               link_name_len = 0;  /* Length of the link name string */
    ssize_t              ret_value     = -1; /* Return value */

    FUNC_ENTER_API((-1))
    H5TRACE8("Zs", "i*sIiIohxzi", loc_id, group_name, idx_type, order, n, name, size, lapl_id);

    /* Check arguments */
    if (!group_name || !*group_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, (-1), "no name specified");
    if (idx_type <= H5_INDEX_UNKNOWN || idx_type >= H5_INDEX_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, (-1), "invalid index type specified");
    if (order <= H5_ITER_UNKNOWN || order >= H5_ITER_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, (-1), "invalid iteration order specified");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&lapl_id, H5P_CLS_LACC, loc_id, true) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTSET, (-1), "can't set access property list info");

    /* Set up location struct */
    loc_params.type                         = H5VL_OBJECT_BY_IDX;
    loc_params.loc_data.loc_by_idx.name     = group_name;
    loc_params.loc_data.loc_by_idx.idx_type = idx_type;
    loc_params.loc_data.loc_by_idx.order    = order;
    loc_params.loc_data.loc_by_idx.n        = n;
    loc_params.loc_data.loc_by_idx.lapl_id  = lapl_id;
    loc_params.obj_type                     = H5I_get_type(loc_id);

    /* Get the VOL object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, (-1), "invalid location identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                 = H5VL_LINK_GET_NAME;
    vol_cb_args.args.get_name.name_size = size;
    vol_cb_args.args.get_name.name      = name;
    vol_cb_args.args.get_name.name_len  = &link_name_len;

    /* Get the link information */
    if (H5VL_link_get(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTGET, (-1), "unable to get link name");

    /* Set the return value */
    ret_value = (ssize_t)link_name_len;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Lget_name_by_idx() */

/*-------------------------------------------------------------------------
 * Function:    H5L__iterate_api_common
 *
 * Purpose:     This is the common function for iterating over links
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5L__iterate_api_common(hid_t group_id, H5_index_t idx_type, H5_iter_order_t order, hsize_t *idx_p,
                        H5L_iterate2_t op, void *op_data, void **token_ptr, H5VL_object_t **_vol_obj_ptr)
{
    H5VL_object_t  *tmp_vol_obj = NULL; /* Object for loc_id */
    H5VL_object_t **vol_obj_ptr =
        (_vol_obj_ptr ? _vol_obj_ptr : &tmp_vol_obj);   /* Ptr to object ptr for loc_id */
    H5VL_link_specific_args_t vol_cb_args;              /* Arguments to VOL callback */
    H5VL_loc_params_t         loc_params;               /* Location parameters for object access */
    H5I_type_t                id_type;                  /* Type of ID */
    herr_t                    ret_value = H5_ITER_CONT; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    id_type = H5I_get_type(group_id);
    if (!(H5I_GROUP == id_type || H5I_FILE == id_type))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid argument");
    if (idx_type <= H5_INDEX_UNKNOWN || idx_type >= H5_INDEX_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid index type specified");
    if (order <= H5_ITER_UNKNOWN || order >= H5_ITER_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid iteration order specified");
    if (!op)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no operator specified");

    /* Set up object access arguments */
    if (H5VL_setup_self_args(group_id, vol_obj_ptr, &loc_params) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTSET, FAIL, "can't set object access arguments");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                = H5VL_LINK_ITER;
    vol_cb_args.args.iterate.recursive = false;
    vol_cb_args.args.iterate.idx_type  = idx_type;
    vol_cb_args.args.iterate.order     = order;
    vol_cb_args.args.iterate.idx_p     = idx_p;
    vol_cb_args.args.iterate.op        = op;
    vol_cb_args.args.iterate.op_data   = op_data;

    /* Iterate over the links */
    if ((ret_value = H5VL_link_specific(*vol_obj_ptr, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT,
                                        token_ptr)) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_BADITER, FAIL, "link iteration failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5L__iterate_api_common() */

/*-------------------------------------------------------------------------
 * Function:    H5Literate2
 *
 * Purpose:     Iterates over links in a group, with user callback routine,
 *              according to the order within an index.
 *
 *              Same pattern of behavior as H5Giterate.
 *
 * Return:      Success:    The return value of the first operator that
 *                          returns non-zero, or zero if all members were
 *                          processed with no operator returning non-zero.
 *
 *              Failure:    Negative if something goes wrong within the
 *                          library, or the negative value returned by one
 *                          of the operators.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Literate2(hid_t group_id, H5_index_t idx_type, H5_iter_order_t order, hsize_t *idx_p, H5L_iterate2_t op,
            void *op_data)
{
    herr_t ret_value; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "iIiIo*hLI*x", group_id, idx_type, order, idx_p, op, op_data);

    /* Iterate over links synchronously */
    if ((ret_value = H5L__iterate_api_common(group_id, idx_type, order, idx_p, op, op_data, NULL, NULL)) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_BADITER, FAIL, "synchronous link iteration failed");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Literate2() */

/*-------------------------------------------------------------------------
 * Function:    H5Literate_async
 *
 * Purpose:     Asynchronous version of H5Literate2
 *
 * Return:      Success:    The return value of the first operator that
 *                          returns non-zero, or zero if all members were
 *                          processed with no operator returning non-zero.
 *
 *              Failure:    Negative if something goes wrong within the
 *                          library, or the negative value returned by one
 *                          of the operators.
 *
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Literate_async(const char *app_file, const char *app_func, unsigned app_line, hid_t group_id,
                 H5_index_t idx_type, H5_iter_order_t order, hsize_t *idx_p, H5L_iterate2_t op, void *op_data,
                 hid_t es_id)
{
    H5VL_object_t *vol_obj   = NULL;            /* Object for loc_id */
    void          *token     = NULL;            /* Request token for async operation        */
    void         **token_ptr = H5_REQUEST_NULL; /* Pointer to request token for async operation        */
    herr_t         ret_value;                   /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE10("e", "*s*sIuiIiIo*hLI*xi", app_file, app_func, app_line, group_id, idx_type, order, idx_p, op,
              op_data, es_id);

    /* Set up request token pointer for asynchronous operation */
    if (H5ES_NONE != es_id)
        token_ptr = &token; /* Point at token for VOL connector to set up */

    /* Iterate over links asynchronously */
    if ((ret_value =
             H5L__iterate_api_common(group_id, idx_type, order, idx_p, op, op_data, token_ptr, &vol_obj)) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_BADITER, FAIL, "asynchronous link iteration failed");

    /* If a token was created, add the token to the event set */
    if (NULL != token)
        /* clang-format off */
        if (H5ES_insert(es_id, vol_obj->connector, token,
                        H5ARG_TRACE10(__func__, "*s*sIuiIiIo*hLI*xi", app_file, app_func, app_line, group_id, idx_type, order, idx_p, op, op_data, es_id)) < 0)
            /* clang-format on */
            HGOTO_ERROR(H5E_LINK, H5E_CANTINSERT, FAIL, "can't insert token into event set");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Literate_async() */

/*-------------------------------------------------------------------------
 * Function:    H5Literate_by_name2
 *
 * Purpose:     Iterates over links in a group, with user callback routine,
 *              according to the order within an index.
 *
 *              Same pattern of behavior as H5Giterate.
 *
 * Return:      Success:    The return value of the first operator that
 *                          returns non-zero, or zero if all members were
 *                          processed with no operator returning non-zero.
 *
 *              Failure:    Negative if something goes wrong within the
 *                          library, or the negative value returned by one
 *                          of the operators.
 *
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Literate_by_name2(hid_t loc_id, const char *group_name, H5_index_t idx_type, H5_iter_order_t order,
                    hsize_t *idx_p, H5L_iterate2_t op, void *op_data, hid_t lapl_id)
{
    H5VL_object_t            *vol_obj = NULL; /* Object of loc_id */
    H5VL_link_specific_args_t vol_cb_args;    /* Arguments to VOL callback */
    H5VL_loc_params_t         loc_params;     /* Location parameters for object access */
    herr_t                    ret_value;      /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE8("e", "i*sIiIo*hLI*xi", loc_id, group_name, idx_type, order, idx_p, op, op_data, lapl_id);

    /* Check arguments */
    if (!group_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "group_name parameter cannot be NULL");
    if (!*group_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "group_name parameter cannot be an empty string");
    if (idx_type <= H5_INDEX_UNKNOWN || idx_type >= H5_INDEX_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid index type specified");
    if (order <= H5_ITER_UNKNOWN || order >= H5_ITER_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid iteration order specified");
    if (!op)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no operator specified");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&lapl_id, H5P_CLS_LACC, loc_id, false) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTSET, FAIL, "can't set access property list info");

    /* Get the location object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set location struct fields */
    loc_params.type                         = H5VL_OBJECT_BY_NAME;
    loc_params.obj_type                     = H5I_get_type(loc_id);
    loc_params.loc_data.loc_by_name.name    = group_name;
    loc_params.loc_data.loc_by_name.lapl_id = lapl_id;

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                = H5VL_LINK_ITER;
    vol_cb_args.args.iterate.recursive = false;
    vol_cb_args.args.iterate.idx_type  = idx_type;
    vol_cb_args.args.iterate.order     = order;
    vol_cb_args.args.iterate.idx_p     = idx_p;
    vol_cb_args.args.iterate.op        = op;
    vol_cb_args.args.iterate.op_data   = op_data;

    /* Iterate over the links */
    if ((ret_value = H5VL_link_specific(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT,
                                        H5_REQUEST_NULL)) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_BADITER, FAIL, "link iteration failed");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Literate_by_name() */

/*-------------------------------------------------------------------------
 * Function:    H5Lvisit2
 *
 * Purpose:     Recursively visit all the links in a group and all
 *              the groups that are linked to from that group.  Links within
 *              each group are visited according to the order within the
 *              specified index (unless the specified index does not exist for
 *              a particular group, then the "name" index is used).
 *
 *              NOTE: Each _link_ reachable from the initial group will only be
 *              visited once.  However, because an object may be reached from
 *              more than one link, the visitation may call the application's
 *              callback with more than one link that points to a particular
 *              _object_.
 *
 * Return:      Success:    The return value of the first operator that
 *                          returns non-zero, or zero if all members were
 *                          processed with no operator returning non-zero.
 *
 *              Failure:    Negative if something goes wrong within the
 *                          library, or the negative value returned by one
 *                          of the operators.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Lvisit2(hid_t group_id, H5_index_t idx_type, H5_iter_order_t order, H5L_iterate2_t op, void *op_data)
{
    H5VL_object_t            *vol_obj = NULL; /* Object of loc_id */
    H5VL_link_specific_args_t vol_cb_args;    /* Arguments to VOL callback */
    H5VL_loc_params_t         loc_params;     /* Location parameters for object access */
    H5I_type_t                id_type;        /* Type of ID */
    herr_t                    ret_value;      /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "iIiIoLI*x", group_id, idx_type, order, op, op_data);

    /* Check args */
    id_type = H5I_get_type(group_id);
    if (!(H5I_GROUP == id_type || H5I_FILE == id_type))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid argument");
    if (idx_type <= H5_INDEX_UNKNOWN || idx_type >= H5_INDEX_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid index type specified");
    if (order <= H5_ITER_UNKNOWN || order >= H5_ITER_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid iteration order specified");
    if (!op)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no callback operator specified");

    /* Set location struct fields */
    loc_params.type     = H5VL_OBJECT_BY_SELF;
    loc_params.obj_type = H5I_get_type(group_id);

    /* Get the location object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(group_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                = H5VL_LINK_ITER;
    vol_cb_args.args.iterate.recursive = true;
    vol_cb_args.args.iterate.idx_type  = idx_type;
    vol_cb_args.args.iterate.order     = order;
    vol_cb_args.args.iterate.idx_p     = NULL;
    vol_cb_args.args.iterate.op        = op;
    vol_cb_args.args.iterate.op_data   = op_data;

    /* Iterate over the links */
    if ((ret_value = H5VL_link_specific(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT,
                                        H5_REQUEST_NULL)) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_BADITER, FAIL, "link visitation failed");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Lvisit2() */

/*-------------------------------------------------------------------------
 * Function:    H5Lvisit_by_name2
 *
 * Purpose:     Recursively visit all the links in a group and all
 *              the groups that are linked to from that group.  Links within
 *              each group are visited according to the order within the
 *              specified index (unless the specified index does not exist for
 *              a particular group, then the "name" index is used).
 *
 *              NOTE: Each _link_ reachable from the initial group will only be
 *              visited once.  However, because an object may be reached from
 *              more than one link, the visitation may call the application's
 *              callback with more than one link that points to a particular
 *              _object_.
 *
 * Return:      Success:    The return value of the first operator that
 *                          returns non-zero, or zero if all members were
 *                          processed with no operator returning non-zero.
 *
 *              Failure:    Negative if something goes wrong within the
 *                          library, or the negative value returned by one
 *                          of the operators.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Lvisit_by_name2(hid_t loc_id, const char *group_name, H5_index_t idx_type, H5_iter_order_t order,
                  H5L_iterate2_t op, void *op_data, hid_t lapl_id)
{
    H5VL_object_t            *vol_obj = NULL; /* Object of loc_id */
    H5VL_link_specific_args_t vol_cb_args;    /* Arguments to VOL callback */
    H5VL_loc_params_t         loc_params;     /* Location parameters for object access */
    herr_t                    ret_value;      /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE7("e", "i*sIiIoLI*xi", loc_id, group_name, idx_type, order, op, op_data, lapl_id);

    /* Check args */
    if (!group_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "group_name parameter cannot be NULL");
    if (!*group_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "group_name parameter cannot be an empty string");
    if (idx_type <= H5_INDEX_UNKNOWN || idx_type >= H5_INDEX_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid index type specified");
    if (order <= H5_ITER_UNKNOWN || order >= H5_ITER_N)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid iteration order specified");
    if (!op)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no callback operator specified");

    /* Verify access property list and set up collective metadata if appropriate */
    if (H5CX_set_apl(&lapl_id, H5P_CLS_LACC, loc_id, false) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTSET, FAIL, "can't set access property list info");

    /* get the location object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set location struct fields */
    loc_params.type                         = H5VL_OBJECT_BY_NAME;
    loc_params.obj_type                     = H5I_get_type(loc_id);
    loc_params.loc_data.loc_by_name.name    = group_name;
    loc_params.loc_data.loc_by_name.lapl_id = lapl_id;

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                = H5VL_LINK_ITER;
    vol_cb_args.args.iterate.recursive = true;
    vol_cb_args.args.iterate.idx_type  = idx_type;
    vol_cb_args.args.iterate.order     = order;
    vol_cb_args.args.iterate.idx_p     = NULL;
    vol_cb_args.args.iterate.op        = op;
    vol_cb_args.args.iterate.op_data   = op_data;

    /* Visit the links */
    if ((ret_value = H5VL_link_specific(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT,
                                        H5_REQUEST_NULL)) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_BADITER, FAIL, "link visitation failed");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Lvisit_by_name2() */

/*-------------------------------------------------------------------------
 * Function: H5Lunpack_elink_val
 *
 * Purpose: Given a buffer holding the "link value" from an external link,
 *              gets pointers to the information within the link value buffer.
 *
 *              External link link values contain some flags and
 *              two NULL-terminated strings, one after the other.
 *
 *              The FLAGS value will be filled in and FILENAME and
 *              OBJ_PATH will be set to pointers within ext_linkval (unless
 *              any of these values is NULL).
 *
 *              Using this function on strings that aren't external link
 *              udata buffers can result in segmentation faults.
 *
 * Return: Non-negative on success/ Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Lunpack_elink_val(const void *_ext_linkval, size_t link_size, unsigned *flags, const char **filename,
                    const char **obj_path)
{
    const uint8_t *ext_linkval = (const uint8_t *)_ext_linkval; /* Pointer to the link value */
    unsigned       lnk_version;                                 /* External link format version */
    unsigned       lnk_flags;                                   /* External link flags */
    size_t         len;                                         /* Length of the filename in the linkval*/
    herr_t         ret_value = SUCCEED;                         /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "*xz*Iu**s**s", _ext_linkval, link_size, flags, filename, obj_path);

    /* Sanity check external link buffer */
    if (ext_linkval == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not an external link linkval buffer");
    lnk_version = (*ext_linkval >> 4) & 0x0F;
    lnk_flags   = *ext_linkval & 0x0F;
    if (lnk_version > H5L_EXT_VERSION)
        HGOTO_ERROR(H5E_LINK, H5E_CANTDECODE, FAIL, "bad version number for external link");
    if (lnk_flags & (unsigned)~H5L_EXT_FLAGS_ALL)
        HGOTO_ERROR(H5E_LINK, H5E_CANTDECODE, FAIL, "bad flags for external link");
    if (link_size <= 2)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a valid external link buffer");

    /* Try to do some error checking.  If the last character in the linkval
     * (the last character of obj_path) isn't NULL, then something's wrong.
     */
    if (ext_linkval[link_size - 1] != '\0')
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "linkval buffer is not NULL-terminated");

    /* We're now guaranteed that strlen won't segfault, since the buffer has
     * at least one NULL in it.
     */
    len = strlen((const char *)ext_linkval + 1);

    /* If the first NULL we found was at the very end of the buffer, then
     * this external link value has no object name and is invalid.
     */
    if ((len + 1) >= (link_size - 1))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "linkval buffer doesn't contain an object path");

    /* If we got here then the buffer contains (at least) two strings packed
     * in the correct way.  Assume it's correct and return pointers to the
     * filename and object path.
     */
    if (filename)
        *filename = (const char *)ext_linkval + 1;
    if (obj_path)
        *obj_path = ((const char *)ext_linkval + 1) + len + 1; /* Add one for NULL terminator */

    /* Set the flags to return */
    if (flags)
        *flags = lnk_flags;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Lunpack_elink_val() */
