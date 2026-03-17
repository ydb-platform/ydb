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
 * Purpose:	Deprecated functions from the H5L interface.  These
 *              functions are here for compatibility purposes and may be
 *              removed in the future.  Applications should switch to the
 *              newer APIs.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Lmodule.h" /* This source code file is part of the H5L module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5CXprivate.h" /* API Contexts                             */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5Lpkg.h"      /* Links                                    */

#include "H5VLnative_private.h"

#ifndef H5_NO_DEPRECATED_SYMBOLS

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/* Shim data for using native H5Literate/visit callbacks with the VOL */
typedef struct H5L_shim_data_t {
    H5L_iterate1_t real_op;
    void          *real_op_data;
} H5L_shim_data_t;

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

/*-------------------------------------------------------------------------
 * Function:    H5L__iterate2_shim
 *
 * Purpose:     Shim function for translating between H5L_info2_t and
 *              H5L_info1_t structures, as used by H5Literate2/H5Lvisit2
 *              and H5Literate1/H5Lvisit1, respectively.
 *
 * Return:      Success:    H5_ITER_CONT or H5_ITER_STOP
 *              Failure:    H5_ITER_ERROR
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5L__iterate2_shim(hid_t group_id, const char *name, const H5L_info2_t *linfo2, void *op_data)
{
    H5L_shim_data_t *shim_data = (H5L_shim_data_t *)op_data;
    H5L_info1_t      linfo;
    herr_t           ret_value = H5_ITER_CONT;

    FUNC_ENTER_PACKAGE

    /* Copy the new-style members into the old-style struct */
    if (linfo2) {
        linfo.type         = linfo2->type;
        linfo.corder_valid = linfo2->corder_valid;
        linfo.corder       = linfo2->corder;
        linfo.cset         = linfo2->cset;
        if (H5L_TYPE_HARD == linfo2->type) {
            if (H5VLnative_token_to_addr(group_id, linfo2->u.token, &linfo.u.address) < 0)
                HGOTO_ERROR(H5E_LINK, H5E_CANTUNSERIALIZE, H5_ITER_ERROR,
                            "can't deserialize object token into address");
        }
        else
            linfo.u.val_size = linfo2->u.val_size;
    }

    /* Invoke the real callback */
    ret_value = shim_data->real_op(group_id, name, &linfo, shim_data->real_op_data);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L__iterate2_shim() */

/*-------------------------------------------------------------------------
 * Function:    H5Literate1
 *
 * Purpose:     Iterates over links in a group, with user callback routine,
 *              according to the order within an index.
 *
 *              Same pattern of behavior as H5Giterate.
 *
 * Note:        Deprecated in favor of H5Literate2
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
H5Literate1(hid_t group_id, H5_index_t idx_type, H5_iter_order_t order, hsize_t *idx_p, H5L_iterate1_t op,
            void *op_data)
{
    H5VL_object_t            *vol_obj = NULL; /* Object of loc_id */
    H5VL_link_specific_args_t vol_cb_args;    /* Arguments to VOL callback */
    H5VL_loc_params_t         loc_params;
    H5I_type_t                id_type; /* Type of ID */
    H5L_shim_data_t           shim_data;
    bool                      is_native_vol_obj;
    herr_t                    ret_value; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE6("e", "iIiIo*hLi*x", group_id, idx_type, order, idx_p, op, op_data);

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

    /* Get the location object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(group_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Check if the VOL object is a native VOL connector object */
    if (H5VL_object_is_native(vol_obj, &is_native_vol_obj) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't determine if VOL object is native connector object");
    if (!is_native_vol_obj)
        HGOTO_ERROR(H5E_LINK, H5E_BADVALUE, FAIL,
                    "H5Literate1 is only meant to be used with the native VOL connector");

    /* Set location struct fields */
    loc_params.type     = H5VL_OBJECT_BY_SELF;
    loc_params.obj_type = H5I_get_type(group_id);

    /* Set up shim */
    shim_data.real_op      = op;
    shim_data.real_op_data = op_data;

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                = H5VL_LINK_ITER;
    vol_cb_args.args.iterate.recursive = false;
    vol_cb_args.args.iterate.idx_type  = idx_type;
    vol_cb_args.args.iterate.order     = order;
    vol_cb_args.args.iterate.idx_p     = idx_p;
    vol_cb_args.args.iterate.op        = H5L__iterate2_shim;
    vol_cb_args.args.iterate.op_data   = &shim_data;

    /* Iterate over the links */
    if ((ret_value = H5VL_link_specific(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT,
                                        H5_REQUEST_NULL)) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_BADITER, FAIL, "link iteration failed");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Literate1() */

/*-------------------------------------------------------------------------
 * Function:    H5Literate_by_name1
 *
 * Purpose:     Iterates over links in a group, with user callback routine,
 *              according to the order within an index.
 *
 *              Same pattern of behavior as H5Giterate.
 *
 * Note:        Deprecated in favor of H5Literate_by_name2
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
H5Literate_by_name1(hid_t loc_id, const char *group_name, H5_index_t idx_type, H5_iter_order_t order,
                    hsize_t *idx_p, H5L_iterate1_t op, void *op_data, hid_t lapl_id)
{
    H5VL_object_t            *vol_obj = NULL; /* Object of loc_id */
    H5VL_link_specific_args_t vol_cb_args;    /* Arguments to VOL callback */
    H5VL_loc_params_t         loc_params;
    H5L_shim_data_t           shim_data;
    bool                      is_native_vol_obj;
    herr_t                    ret_value; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE8("e", "i*sIiIo*hLi*xi", loc_id, group_name, idx_type, order, idx_p, op, op_data, lapl_id);

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

    /* Check if the VOL object is a native VOL connector object */
    if (H5VL_object_is_native(vol_obj, &is_native_vol_obj) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't determine if VOL object is native connector object");
    if (!is_native_vol_obj)
        HGOTO_ERROR(H5E_LINK, H5E_BADVALUE, FAIL,
                    "H5Literate_by_name1 is only meant to be used with the native VOL connector");

    /* Set location struct fields */
    loc_params.type                         = H5VL_OBJECT_BY_NAME;
    loc_params.obj_type                     = H5I_get_type(loc_id);
    loc_params.loc_data.loc_by_name.name    = group_name;
    loc_params.loc_data.loc_by_name.lapl_id = lapl_id;

    /* Set up shim */
    shim_data.real_op      = op;
    shim_data.real_op_data = op_data;

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                = H5VL_LINK_ITER;
    vol_cb_args.args.iterate.recursive = false;
    vol_cb_args.args.iterate.idx_type  = idx_type;
    vol_cb_args.args.iterate.order     = order;
    vol_cb_args.args.iterate.idx_p     = idx_p;
    vol_cb_args.args.iterate.op        = H5L__iterate2_shim;
    vol_cb_args.args.iterate.op_data   = &shim_data;

    /* Iterate over the links */
    if ((ret_value = H5VL_link_specific(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT,
                                        H5_REQUEST_NULL)) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_BADITER, FAIL, "link iteration failed");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Literate_by_name1() */

/*-------------------------------------------------------------------------
 * Function:    H5Lget_info1
 *
 * Purpose:     Gets metadata for a link.
 *
 * Note:        Deprecated in favor of H5Lget_info2
 *
 * Return:      Success:    Non-negative with information in LINFO
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Lget_info1(hid_t loc_id, const char *name, H5L_info1_t *linfo /*out*/, hid_t lapl_id)
{
    H5VL_object_t       *vol_obj = NULL; /* object of loc_id */
    H5VL_link_get_args_t vol_cb_args;    /* Arguments to VOL callback */
    H5VL_loc_params_t    loc_params;
    H5L_info2_t          linfo2; /* New-style link info */
    bool                 is_native_vol_obj;
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

    /* Check if the VOL object is a native VOL connector object */
    if (H5VL_object_is_native(vol_obj, &is_native_vol_obj) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't determine if VOL object is native connector object");
    if (!is_native_vol_obj)
        HGOTO_ERROR(H5E_LINK, H5E_BADVALUE, FAIL,
                    "H5Lget_info1 is only meant to be used with the native VOL connector");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type             = H5VL_LINK_GET_INFO;
    vol_cb_args.args.get_info.linfo = &linfo2;

    /* Get the link information */
    if (H5VL_link_get(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "unable to get link info");

    /* Copy the new-style members into the old-style struct */
    if (linfo) {
        linfo->type         = linfo2.type;
        linfo->corder_valid = linfo2.corder_valid;
        linfo->corder       = linfo2.corder;
        linfo->cset         = linfo2.cset;
        if (H5L_TYPE_HARD == linfo2.type) {
            void *vol_obj_data;

            if (NULL == (vol_obj_data = H5VL_object_data(vol_obj)))
                HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't get underlying VOL object");

            if (H5VL_native_token_to_addr(vol_obj_data, loc_params.obj_type, linfo2.u.token,
                                          &linfo->u.address) < 0)
                HGOTO_ERROR(H5E_LINK, H5E_CANTUNSERIALIZE, FAIL,
                            "can't deserialize object token into address");
        } /* end if */
        else
            linfo->u.val_size = linfo2.u.val_size;
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Lget_info1() */

/*-------------------------------------------------------------------------
 * Function:    H5Lget_info_by_idx1
 *
 * Purpose:     Gets metadata for a link, according to the order within an
 *              index.
 *
 * Note:        Deprecated in favor of H5Lget_info_by_idx2
 *
 * Return:      Success:    Non-negative with information in LINFO
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Lget_info_by_idx1(hid_t loc_id, const char *group_name, H5_index_t idx_type, H5_iter_order_t order,
                    hsize_t n, H5L_info1_t *linfo /*out*/, hid_t lapl_id)
{
    H5VL_object_t       *vol_obj = NULL; /* object of loc_id */
    H5VL_link_get_args_t vol_cb_args;    /* Arguments to VOL callback */
    H5VL_loc_params_t    loc_params;
    H5L_info2_t          linfo2; /* New-style link info */
    bool                 is_native_vol_obj;
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

    /* Check if the VOL object is a native VOL connector object */
    if (H5VL_object_is_native(vol_obj, &is_native_vol_obj) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't determine if VOL object is native connector object");
    if (!is_native_vol_obj)
        HGOTO_ERROR(H5E_LINK, H5E_BADVALUE, FAIL,
                    "H5Lget_info_by_idx1 is only meant to be used with the native VOL connector");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type             = H5VL_LINK_GET_INFO;
    vol_cb_args.args.get_info.linfo = &linfo2;

    /* Get the link information */
    if (H5VL_link_get(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "unable to get link info");

    /* Copy the new-style members into the old-style struct */
    if (linfo) {
        linfo->type         = linfo2.type;
        linfo->corder_valid = linfo2.corder_valid;
        linfo->corder       = linfo2.corder;
        linfo->cset         = linfo2.cset;
        if (H5L_TYPE_HARD == linfo2.type) {
            void *vol_obj_data;

            if (NULL == (vol_obj_data = H5VL_object_data(vol_obj)))
                HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't get underlying VOL object");

            if (H5VL_native_token_to_addr(vol_obj_data, loc_params.obj_type, linfo2.u.token,
                                          &linfo->u.address) < 0)
                HGOTO_ERROR(H5E_LINK, H5E_CANTUNSERIALIZE, FAIL,
                            "can't deserialize object token into address");
        } /* end if */
        else
            linfo->u.val_size = linfo2.u.val_size;
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Lget_info_by_idx1() */

/*-------------------------------------------------------------------------
 * Function:    H5Lvisit1
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
 * Note:        Deprecated in favor of H5Lvisit2
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
H5Lvisit1(hid_t group_id, H5_index_t idx_type, H5_iter_order_t order, H5L_iterate1_t op, void *op_data)
{
    H5VL_object_t            *vol_obj = NULL; /* Object of loc_id */
    H5VL_link_specific_args_t vol_cb_args;    /* Arguments to VOL callback */
    H5VL_loc_params_t         loc_params;
    H5I_type_t                id_type; /* Type of ID */
    H5L_shim_data_t           shim_data;
    bool                      is_native_vol_obj;
    herr_t                    ret_value; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "iIiIoLi*x", group_id, idx_type, order, op, op_data);

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

    /* Check if the VOL object is a native VOL connector object */
    if (H5VL_object_is_native(vol_obj, &is_native_vol_obj) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't determine if VOL object is native connector object");
    if (!is_native_vol_obj)
        HGOTO_ERROR(H5E_LINK, H5E_BADVALUE, FAIL,
                    "H5Lvisit1 is only meant to be used with the native VOL connector");

    /* Set up shim */
    shim_data.real_op      = op;
    shim_data.real_op_data = op_data;

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                = H5VL_LINK_ITER;
    vol_cb_args.args.iterate.recursive = true;
    vol_cb_args.args.iterate.idx_type  = idx_type;
    vol_cb_args.args.iterate.order     = order;
    vol_cb_args.args.iterate.idx_p     = NULL;
    vol_cb_args.args.iterate.op        = H5L__iterate2_shim;
    vol_cb_args.args.iterate.op_data   = &shim_data;

    /* Iterate over the links */
    if ((ret_value = H5VL_link_specific(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT,
                                        H5_REQUEST_NULL)) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_BADITER, FAIL, "link visitation failed");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Lvisit1() */

/*-------------------------------------------------------------------------
 * Function:    H5Lvisit_by_name1
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
 * Note:        Deprecated in favor of H5Lvisit_by_name2
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
H5Lvisit_by_name1(hid_t loc_id, const char *group_name, H5_index_t idx_type, H5_iter_order_t order,
                  H5L_iterate1_t op, void *op_data, hid_t lapl_id)
{
    H5VL_object_t            *vol_obj = NULL; /* Object of loc_id */
    H5VL_link_specific_args_t vol_cb_args;    /* Arguments to VOL callback */
    H5VL_loc_params_t         loc_params;
    H5L_shim_data_t           shim_data;
    bool                      is_native_vol_obj;
    herr_t                    ret_value; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE7("e", "i*sIiIoLi*xi", loc_id, group_name, idx_type, order, op, op_data, lapl_id);

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

    /* Check if the VOL object is a native VOL connector object */
    if (H5VL_object_is_native(vol_obj, &is_native_vol_obj) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "can't determine if VOL object is native connector object");
    if (!is_native_vol_obj)
        HGOTO_ERROR(H5E_LINK, H5E_BADVALUE, FAIL,
                    "H5Lvisit_by_name1 is only meant to be used with the native VOL connector");

    /* Set location struct fields */
    loc_params.type                         = H5VL_OBJECT_BY_NAME;
    loc_params.obj_type                     = H5I_get_type(loc_id);
    loc_params.loc_data.loc_by_name.name    = group_name;
    loc_params.loc_data.loc_by_name.lapl_id = lapl_id;

    /* Set up shim */
    shim_data.real_op      = op;
    shim_data.real_op_data = op_data;

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                = H5VL_LINK_ITER;
    vol_cb_args.args.iterate.recursive = true;
    vol_cb_args.args.iterate.idx_type  = idx_type;
    vol_cb_args.args.iterate.order     = order;
    vol_cb_args.args.iterate.idx_p     = NULL;
    vol_cb_args.args.iterate.op        = H5L__iterate2_shim;
    vol_cb_args.args.iterate.op_data   = &shim_data;

    /* Visit the links */
    if ((ret_value = H5VL_link_specific(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT,
                                        H5_REQUEST_NULL)) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_BADITER, FAIL, "link visitation failed");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Lvisit_by_name1() */

#endif /* H5_NO_DEPRECATED_SYMBOLS */
