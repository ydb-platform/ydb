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
 * Purpose:     Group callbacks for the native VOL connector
 *
 */

/****************/
/* Module Setup */
/****************/

#define H5G_FRIEND /* Suppress error about including H5Gpkg    */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5Gpkg.h"      /* Groups                                   */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5Oprivate.h"  /* Object headers                           */
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
 * Function:    H5VL__native_group_create
 *
 * Purpose:     Handles the group create callback
 *
 * Return:      Success:    group pointer
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5VL__native_group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id,
                          hid_t gcpl_id, hid_t H5_ATTR_UNUSED gapl_id, hid_t H5_ATTR_UNUSED dxpl_id,
                          void H5_ATTR_UNUSED **req)
{
    H5G_loc_t loc;        /* Location to create group     */
    H5G_t    *grp = NULL; /* New group created            */
    void     *ret_value;

    FUNC_ENTER_PACKAGE

    /* Set up the location */
    if (H5G_loc_real(obj, loc_params->obj_type, &loc) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a file or file object");

    /* if name is NULL then this is from H5Gcreate_anon */
    if (name == NULL) {
        H5G_obj_create_t gcrt_info; /* Information for group creation */

        /* Set up group creation info */
        gcrt_info.gcpl_id    = gcpl_id;
        gcrt_info.cache_type = H5G_NOTHING_CACHED;
        memset(&gcrt_info.cache, 0, sizeof(gcrt_info.cache));

        /* Create the new group & get its ID */
        if (NULL == (grp = H5G__create(loc.oloc->file, &gcrt_info)))
            HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "unable to create group");
    } /* end if */
    /* otherwise it's from H5Gcreate */
    else {
        /* Create the new group & get its ID */
        if (NULL == (grp = H5G__create_named(&loc, name, lcpl_id, gcpl_id)))
            HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, NULL, "unable to create group");
    } /* end else */

    ret_value = (void *)grp;

done:
    if (name == NULL) {
        /* Release the group's object header, if it was created */
        if (grp) {
            H5O_loc_t *oloc; /* Object location for group */

            /* Get the new group's object location */
            if (NULL == (oloc = H5G_oloc(grp)))
                HDONE_ERROR(H5E_SYM, H5E_CANTGET, NULL, "unable to get object location of group");

            /* Decrement refcount on group's object header in memory */
            if (H5O_dec_rc_by_loc(oloc) < 0)
                HDONE_ERROR(H5E_SYM, H5E_CANTDEC, NULL,
                            "unable to decrement refcount on newly created object");
        } /* end if */
    }     /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_group_create() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_group_open
 *
 * Purpose:     Handles the group open callback
 *
 * Return:      Success:    group pointer
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5VL__native_group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                        hid_t H5_ATTR_UNUSED gapl_id, hid_t H5_ATTR_UNUSED dxpl_id, void H5_ATTR_UNUSED **req)
{
    H5G_loc_t loc;        /* Location to open group   */
    H5G_t    *grp = NULL; /* New group opened         */
    void     *ret_value;

    FUNC_ENTER_PACKAGE

    /* Set up the location */
    if (H5G_loc_real(obj, loc_params->obj_type, &loc) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a file or file object");

    /* Open the group */
    if ((grp = H5G__open_name(&loc, name)) == NULL)
        HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, NULL, "unable to open group");

    ret_value = (void *)grp;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_group_open() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_group_get
 *
 * Purpose:     Handles the group get callback
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_group_get(void *obj, H5VL_group_get_args_t *args, hid_t H5_ATTR_UNUSED dxpl_id,
                       void H5_ATTR_UNUSED **req)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    switch (args->op_type) {
        /* H5Gget_create_plist */
        case H5VL_GROUP_GET_GCPL: {
            if ((args->args.get_gcpl.gcpl_id = H5G_get_create_plist((H5G_t *)obj)) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't get creation property list for group");

            break;
        }

        /* H5Gget_info */
        case H5VL_GROUP_GET_INFO: {
            H5VL_group_get_info_args_t *get_info_args = &args->args.get_info;
            H5G_loc_t                   loc;

            if (H5G_loc_real(obj, get_info_args->loc_params.obj_type, &loc) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file or file object");

            if (get_info_args->loc_params.type == H5VL_OBJECT_BY_SELF) {
                /* H5Gget_info */

                /* Retrieve the group's information */
                if (H5G__obj_info(loc.oloc, get_info_args->ginfo) < 0)
                    HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't retrieve group info");
            } /* end if */
            else if (get_info_args->loc_params.type == H5VL_OBJECT_BY_NAME) {
                /* H5Gget_info_by_name */

                /* Retrieve the group's information */
                if (H5G__get_info_by_name(&loc, get_info_args->loc_params.loc_data.loc_by_name.name,
                                          get_info_args->ginfo) < 0)
                    HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't retrieve group info");
            } /* end else-if */
            else if (get_info_args->loc_params.type == H5VL_OBJECT_BY_IDX) {
                /* H5Gget_info_by_idx */

                /* Retrieve the group's information */
                if (H5G__get_info_by_idx(&loc, get_info_args->loc_params.loc_data.loc_by_idx.name,
                                         get_info_args->loc_params.loc_data.loc_by_idx.idx_type,
                                         get_info_args->loc_params.loc_data.loc_by_idx.order,
                                         get_info_args->loc_params.loc_data.loc_by_idx.n,
                                         get_info_args->ginfo) < 0)
                    HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't retrieve group info");
            } /* end else-if */
            else
                HGOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "unknown get info parameters");
            break;
        }

        default:
            HGOTO_ERROR(H5E_VOL, H5E_CANTGET, FAIL, "can't get this type of information from group");
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_group_get() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_group_specific
 *
 * Purpose:     Handles the group specific callback
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_group_specific(void *obj, H5VL_group_specific_args_t *args, hid_t H5_ATTR_UNUSED dxpl_id,
                            void H5_ATTR_UNUSED **req)
{
    H5G_t *grp       = (H5G_t *)obj;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    switch (args->op_type) {
        /* H5Fmount */
        case H5VL_GROUP_MOUNT: {
            H5G_loc_t loc;

            if (H5G_loc_real(grp, H5I_GROUP, &loc) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a group object");

            if (H5F_mount(&loc, args->args.mount.name, args->args.mount.child_file,
                          args->args.mount.fmpl_id) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_MOUNT, FAIL, "unable to mount file");

            break;
        }

        /* H5Funmount */
        case H5VL_GROUP_UNMOUNT: {
            H5G_loc_t loc;

            if (H5G_loc_real(grp, H5I_GROUP, &loc) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a group object");

            if (H5F_unmount(&loc, args->args.unmount.name) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_UNMOUNT, FAIL, "unable to unmount file");

            break;
        }

        /* H5Gflush */
        case H5VL_GROUP_FLUSH: {
            /* Currently, H5Oflush causes H5Fclose to trigger an assertion failure in metadata cache.
             * Leave this situation for the future solution */
            if (H5F_HAS_FEATURE(grp->oloc.file, H5FD_FEAT_HAS_MPI))
                HGOTO_ERROR(H5E_SYM, H5E_UNSUPPORTED, FAIL, "H5Oflush isn't supported for parallel");

            if (H5O_flush_common(&grp->oloc, args->args.flush.grp_id) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_CANTFLUSH, FAIL, "unable to flush group");

            break;
        }

        /* H5Grefresh */
        case H5VL_GROUP_REFRESH: {
            if ((H5O_refresh_metadata(&grp->oloc, args->args.refresh.grp_id)) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_CANTLOAD, FAIL, "unable to refresh group");

            break;
        }

        default:
            HGOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid specific operation");
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_group_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_group_optional
 *
 * Purpose:     Handles the group optional callback
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_group_optional(void H5_ATTR_UNUSED *obj, H5VL_optional_args_t *args,
                            hid_t H5_ATTR_UNUSED dxpl_id, void H5_ATTR_UNUSED **req)
{
#ifndef H5_NO_DEPRECATED_SYMBOLS
    H5VL_native_group_optional_args_t *opt_args = args->args; /* Pointer to native operation's arguments */
#endif                                                        /* H5_NO_DEPRECATED_SYMBOLS */
    herr_t ret_value = SUCCEED;                               /* Return value */

    FUNC_ENTER_PACKAGE

    switch (args->op_type) {
#ifndef H5_NO_DEPRECATED_SYMBOLS
        /* H5Giterate (deprecated) */
        case H5VL_NATIVE_GROUP_ITERATE_OLD: {
            H5VL_native_group_iterate_old_t *iter_args = &opt_args->iterate_old;
            H5G_link_iterate_t               lnk_op; /* Link operator                    */
            H5G_loc_t                        grp_loc;

            /* Get the location struct for the object */
            if (H5G_loc_real(obj, iter_args->loc_params.obj_type, &grp_loc) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file or file object");

            /* Set up link iteration callback struct */
            lnk_op.op_type        = H5G_LINK_OP_OLD;
            lnk_op.op_func.op_old = iter_args->op;

            /* Call the actual iteration routine */
            if ((ret_value = H5G_iterate(&grp_loc, iter_args->loc_params.loc_data.loc_by_name.name,
                                         H5_INDEX_NAME, H5_ITER_INC, iter_args->idx, iter_args->last_obj,
                                         &lnk_op, iter_args->op_data)) < 0)
                HERROR(H5E_SYM, H5E_BADITER, "error iterating over group's links");

            break;
        }

        /* H5Gget_objinfo (deprecated) */
        case H5VL_NATIVE_GROUP_GET_OBJINFO: {
            H5VL_native_group_get_objinfo_t *goi_args = &opt_args->get_objinfo;
            H5G_loc_t                        grp_loc;

            /* Get the location struct for the object */
            if (H5G_loc_real(obj, goi_args->loc_params.obj_type, &grp_loc) < 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file or file object");

            /* Call the actual group objinfo routine */
            if (H5G__get_objinfo(&grp_loc, goi_args->loc_params.loc_data.loc_by_name.name,
                                 goi_args->follow_link, goi_args->statbuf) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "cannot stat object");

            break;
        }
#endif /* H5_NO_DEPRECATED_SYMBOLS */

        default:
            HGOTO_ERROR(H5E_VOL, H5E_UNSUPPORTED, FAIL, "invalid optional operation");
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_group_optional() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__native_group_close
 *
 * Purpose:     Handles the group close callback
 *
 * Return:      Success:    SUCCEED
 *              Failure:    FAIL (group will not be closed)
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL__native_group_close(void *grp, hid_t H5_ATTR_UNUSED dxpl_id, void H5_ATTR_UNUSED **req)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    if (H5G_close((H5G_t *)grp) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CLOSEERROR, FAIL, "can't close group");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_group_close() */
