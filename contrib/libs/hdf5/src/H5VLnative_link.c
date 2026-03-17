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
 * Purpose:     Link callbacks for the native VOL connector
 *
 */

/****************/
/* Module Setup */
/****************/

#define H5L_FRIEND /* Suppress error about including H5Lpkg    */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5Gprivate.h"  /* Groups                                   */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5Lpkg.h"      /* Links                                    */
#include "H5Pprivate.h"  /* Property lists                           */
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
 * Function:    H5VL__native_link_create
 *
 * Purpose:     Handles the link create callback
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_link_create(H5VL_link_create_args_t *args, void *obj, const H5VL_loc_params_t *loc_params,
                         hid_t lcpl_id, hid_t H5_ATTR_UNUSED lapl_id, hid_t H5_ATTR_UNUSED dxpl_id,
                         void H5_ATTR_UNUSED **req)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    switch (args->op_type) {
        case H5VL_LINK_CREATE_HARD: {
            H5G_loc_t          cur_loc;
            H5G_loc_t          link_loc;
            void              *cur_obj    = args->args.hard.curr_obj;
            H5VL_loc_params_t *cur_params = &args->args.hard.curr_loc_params;

            if (NULL != cur_obj && H5G_loc_real(cur_obj, cur_params->obj_type, &cur_loc) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file or file object");
            if (NULL != obj && H5G_loc_real(obj, loc_params->obj_type, &link_loc) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file or file object");

            /* H5Lcreate_hard */
            if (H5VL_OBJECT_BY_NAME == cur_params->type) {
                H5G_loc_t *cur_loc_p, *link_loc_p;

                /* Set up current & new location pointers */
                cur_loc_p  = &cur_loc;
                link_loc_p = &link_loc;
                if (NULL == cur_obj)
                    cur_loc_p = link_loc_p;
                else if (NULL == obj)
                    link_loc_p = cur_loc_p;
                else if (cur_loc_p->oloc->file != link_loc_p->oloc->file)
                    HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                                "source and destination should be in the same file.");

                /* Create the link */
                if (H5L__create_hard(cur_loc_p, cur_params->loc_data.loc_by_name.name, link_loc_p,
                                     loc_params->loc_data.loc_by_name.name, lcpl_id) < 0)
                    HGOTO_ERROR(H5E_LINK, H5E_CANTINIT, FAIL, "unable to create link");
            }      /* end if */
            else { /* H5Olink */
                /* Link to the object */
                if (H5L_link(&link_loc, loc_params->loc_data.loc_by_name.name, &cur_loc, lcpl_id) < 0)
                    HGOTO_ERROR(H5E_LINK, H5E_CANTINIT, FAIL, "unable to create link");
            } /* end else */

            break;
        }

        case H5VL_LINK_CREATE_SOFT: {
            H5G_loc_t link_loc; /* Group location for new link */

            if (H5G_loc_real(obj, loc_params->obj_type, &link_loc) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file or file object");
            if (H5L__create_soft(args->args.soft.target, &link_loc, loc_params->loc_data.loc_by_name.name,
                                 lcpl_id) < 0)
                HGOTO_ERROR(H5E_LINK, H5E_CANTCREATE, FAIL, "unable to create link");

            break;
        }

        case H5VL_LINK_CREATE_UD: {
            H5G_loc_t link_loc; /* Group location for new link */

            if (H5G_loc_real(obj, loc_params->obj_type, &link_loc) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file or file object");
            if (H5L__create_ud(&link_loc, loc_params->loc_data.loc_by_name.name, args->args.ud.buf,
                               args->args.ud.buf_size, args->args.ud.type, lcpl_id) < 0)
                HGOTO_ERROR(H5E_LINK, H5E_CANTINIT, FAIL, "unable to create link");

            break;
        }

        default:
            HGOTO_ERROR(H5E_LINK, H5E_CANTINIT, FAIL, "invalid link creation call");
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_link_create() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_link_copy
 *
 * Purpose:     Handles the link copy callback
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_link_copy(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj,
                       const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t H5_ATTR_UNUSED lapl_id,
                       hid_t H5_ATTR_UNUSED dxpl_id, void H5_ATTR_UNUSED **req)
{
    H5G_loc_t src_loc, *src_loc_p;
    H5G_loc_t dst_loc, *dst_loc_p;
    herr_t    ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    if (NULL != src_obj && H5G_loc_real(src_obj, loc_params1->obj_type, &src_loc) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file or file object");
    if (NULL != dst_obj && H5G_loc_real(dst_obj, loc_params2->obj_type, &dst_loc) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file or file object");

    /* Set up src & dst location pointers */
    src_loc_p = &src_loc;
    dst_loc_p = &dst_loc;
    if (NULL == src_obj)
        src_loc_p = dst_loc_p;
    else if (NULL == dst_obj)
        dst_loc_p = src_loc_p;

    /* Copy the link */
    if (H5L__move(src_loc_p, loc_params1->loc_data.loc_by_name.name, dst_loc_p,
                  loc_params2->loc_data.loc_by_name.name, true, lcpl_id) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTCOPY, FAIL, "unable to copy link");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_link_copy() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_link_move
 *
 * Purpose:     Handles the link move callback
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_link_move(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj,
                       const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t H5_ATTR_UNUSED lapl_id,
                       hid_t H5_ATTR_UNUSED dxpl_id, void H5_ATTR_UNUSED **req)
{
    H5G_loc_t src_loc, *src_loc_p;
    H5G_loc_t dst_loc, *dst_loc_p;
    herr_t    ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    if (NULL != src_obj && H5G_loc_real(src_obj, loc_params1->obj_type, &src_loc) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file or file object");
    if (NULL != dst_obj && H5G_loc_real(dst_obj, loc_params2->obj_type, &dst_loc) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file or file object");

    /* Set up src & dst location pointers */
    src_loc_p = &src_loc;
    dst_loc_p = &dst_loc;
    if (NULL == src_obj)
        src_loc_p = dst_loc_p;
    else if (NULL == dst_obj)
        dst_loc_p = src_loc_p;

    /* Move the link */
    if (H5L__move(src_loc_p, loc_params1->loc_data.loc_by_name.name, dst_loc_p,
                  loc_params2->loc_data.loc_by_name.name, false, lcpl_id) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTMOVE, FAIL, "unable to move link");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_link_move() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_link_get
 *
 * Purpose:     Handles the link get callback
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_link_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_link_get_args_t *args,
                      hid_t H5_ATTR_UNUSED dxpl_id, void H5_ATTR_UNUSED **req)
{
    H5G_loc_t loc;
    herr_t    ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    if (H5G_loc_real(obj, loc_params->obj_type, &loc) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file or file object");

    switch (args->op_type) {
        /* H5Lget_info/H5Lget_info_by_idx */
        case H5VL_LINK_GET_INFO: {
            /* Get the link information */
            if (loc_params->type == H5VL_OBJECT_BY_NAME) {
                if (H5L_get_info(&loc, loc_params->loc_data.loc_by_name.name, args->args.get_info.linfo) < 0)
                    HGOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "unable to get link info");
            } /* end if */
            else if (loc_params->type == H5VL_OBJECT_BY_IDX) {
                if (H5L__get_info_by_idx(&loc, loc_params->loc_data.loc_by_idx.name,
                                         loc_params->loc_data.loc_by_idx.idx_type,
                                         loc_params->loc_data.loc_by_idx.order,
                                         loc_params->loc_data.loc_by_idx.n, args->args.get_info.linfo) < 0)
                    HGOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "unable to get link info");
            } /* end else-if */
            else
                HGOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "unable to get link info");

            break;
        }

        /* H5Lget_name_by_idx */
        case H5VL_LINK_GET_NAME: {
            if (H5L__get_name_by_idx(&loc, loc_params->loc_data.loc_by_idx.name,
                                     loc_params->loc_data.loc_by_idx.idx_type,
                                     loc_params->loc_data.loc_by_idx.order, loc_params->loc_data.loc_by_idx.n,
                                     args->args.get_name.name, args->args.get_name.name_size,
                                     args->args.get_name.name_len) < 0)
                HGOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "unable to get link info");

            break;
        }

        /* H5Lget_val/H5Lget_val_by_idx */
        case H5VL_LINK_GET_VAL: {
            /* Get the link information */
            if (loc_params->type == H5VL_OBJECT_BY_NAME) {
                if (H5L__get_val(&loc, loc_params->loc_data.loc_by_name.name, args->args.get_val.buf,
                                 args->args.get_val.buf_size) < 0)
                    HGOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "unable to get link value");
            }
            else if (loc_params->type == H5VL_OBJECT_BY_IDX) {
                if (H5L__get_val_by_idx(
                        &loc, loc_params->loc_data.loc_by_idx.name, loc_params->loc_data.loc_by_idx.idx_type,
                        loc_params->loc_data.loc_by_idx.order, loc_params->loc_data.loc_by_idx.n,
                        args->args.get_val.buf, args->args.get_val.buf_size) < 0)
                    HGOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "unable to get link val");
            }
            else
                HGOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "unable to get link val");

            break;
        }

        default:
            HGOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't get this type of information from link");
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_link_get() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_link_specific
 *
 * Purpose:     Handles the link specific callback
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_link_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_link_specific_args_t *args,
                           hid_t H5_ATTR_UNUSED dxpl_id, void H5_ATTR_UNUSED **req)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    switch (args->op_type) {
        case H5VL_LINK_EXISTS: {
            H5G_loc_t loc;

            if (H5G_loc_real(obj, loc_params->obj_type, &loc) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file or file object");
            if (H5L__exists(&loc, loc_params->loc_data.loc_by_name.name, args->args.exists.exists) < 0)
                HGOTO_ERROR(H5E_LINK, H5E_NOTFOUND, FAIL, "unable to specific link info");

            break;
        }

        case H5VL_LINK_ITER: {
            H5VL_link_iterate_args_t *iter_args = &args->args.iterate;
            H5G_loc_t                 loc;

            /* Get the location */
            if (H5G_loc_real(obj, loc_params->obj_type, &loc) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a location");

            /* Visit or iterate over the links */
            if (loc_params->type == H5VL_OBJECT_BY_SELF) {
                if (iter_args->recursive) {
                    /* H5Lvisit */
                    if ((ret_value = H5G_visit(&loc, ".", iter_args->idx_type, iter_args->order,
                                               iter_args->op, iter_args->op_data)) < 0)
                        HGOTO_ERROR(H5E_LINK, H5E_BADITER, FAIL, "link visitation failed");
                } /* end if */
                else {
                    /* H5Literate */
                    if ((ret_value = H5L_iterate(&loc, ".", iter_args->idx_type, iter_args->order,
                                                 iter_args->idx_p, iter_args->op, iter_args->op_data)) < 0)
                        HGOTO_ERROR(H5E_LINK, H5E_BADITER, FAIL, "error iterating over links");
                } /* end else */
            }     /* end if */
            else if (loc_params->type == H5VL_OBJECT_BY_NAME) {
                if (iter_args->recursive) {
                    /* H5Lvisit_by_name */
                    if ((ret_value =
                             H5G_visit(&loc, loc_params->loc_data.loc_by_name.name, iter_args->idx_type,
                                       iter_args->order, iter_args->op, iter_args->op_data)) < 0)
                        HGOTO_ERROR(H5E_LINK, H5E_BADITER, FAIL, "link visitation failed");
                } /* end if */
                else {
                    /* H5Literate_by_name */
                    if ((ret_value = H5L_iterate(&loc, loc_params->loc_data.loc_by_name.name,
                                                 iter_args->idx_type, iter_args->order, iter_args->idx_p,
                                                 iter_args->op, iter_args->op_data)) < 0)
                        HGOTO_ERROR(H5E_LINK, H5E_BADITER, FAIL, "error iterating over links");
                } /* end else */
            }     /* end else-if */
            else
                HGOTO_ERROR(H5E_LINK, H5E_UNSUPPORTED, FAIL, "unknown link iterate params");

            break;
        }

        case H5VL_LINK_DELETE: {
            H5G_loc_t loc;

            if (H5G_loc_real(obj, loc_params->obj_type, &loc) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file or file object");

            /* Unlink */
            if (loc_params->type == H5VL_OBJECT_BY_NAME) { /* H5Ldelete */
                if (H5L__delete(&loc, loc_params->loc_data.loc_by_name.name) < 0)
                    HGOTO_ERROR(H5E_LINK, H5E_CANTDELETE, FAIL, "unable to delete link");
            }                                                  /* end if */
            else if (loc_params->type == H5VL_OBJECT_BY_IDX) { /* H5Ldelete_by_idx */
                if (H5L__delete_by_idx(
                        &loc, loc_params->loc_data.loc_by_idx.name, loc_params->loc_data.loc_by_idx.idx_type,
                        loc_params->loc_data.loc_by_idx.order, loc_params->loc_data.loc_by_idx.n) < 0)
                    HGOTO_ERROR(H5E_LINK, H5E_CANTDELETE, FAIL, "unable to delete link");
            } /* end else-if */
            else
                HGOTO_ERROR(H5E_LINK, H5E_CANTDELETE, FAIL, "unable to delete link");
            break;
        }

        default:
            HGOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid specific operation");
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_link_specific() */
