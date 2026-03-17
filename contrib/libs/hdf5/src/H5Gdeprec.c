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
 * Created:	H5Gdeprec.c
 *
 * Purpose:	Deprecated functions from the H5G interface.  These
 *              functions are here for compatibility purposes and may be
 *              removed in the future.  Applications should switch to the
 *              newer APIs.
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
#include "H5private.h"   /* Generic Functions			*/
#include "H5ACprivate.h" /* Metadata cache			*/
#include "H5CXprivate.h" /* API Contexts                         */
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Gpkg.h"      /* Groups		  		*/
#include "H5Iprivate.h"  /* IDs			  		*/
#include "H5Lprivate.h"  /* Links                                */
#include "H5Pprivate.h"  /* Property lists                       */
#include "H5VLprivate.h" /* Virtual Object Layer                 */

#include "H5VLnative_private.h" /* Native VOL connector                     */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

#ifndef H5_NO_DEPRECATED_SYMBOLS
/* User data for path traversal routine for getting object info */
typedef struct {
    H5G_stat_t *statbuf;     /* Stat buffer about object */
    bool        follow_link; /* Whether we are following a link or not */
    H5F_t      *loc_file;    /* Pointer to the file the location is in */
} H5G_trav_goi_t;

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/
static herr_t H5G__get_objinfo_cb(H5G_loc_t *grp_loc /*in*/, const char *name, const H5O_link_t *lnk,
                                  H5G_loc_t *obj_loc, void *_udata /*in,out*/,
                                  H5G_own_loc_t *own_loc /*out*/);
#endif /* H5_NO_DEPRECATED_SYMBOLS */

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

/*-------------------------------------------------------------------------
 * Function:	H5G_map_obj_type
 *
 * Purpose:	Maps the object type to the older "group" object type
 *
 * Return:	Object type (can't fail)
 *
 *-------------------------------------------------------------------------
 */
H5G_obj_t
H5G_map_obj_type(H5O_type_t obj_type)
{
    H5G_obj_t ret_value = H5G_UNKNOWN; /* Return value */

    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Map object type to older "group" object type */
    switch (obj_type) {
        case H5O_TYPE_GROUP:
            ret_value = H5G_GROUP;
            break;

        case H5O_TYPE_DATASET:
            ret_value = H5G_DATASET;
            break;

        case H5O_TYPE_NAMED_DATATYPE:
            ret_value = H5G_TYPE;
            break;

        case H5O_TYPE_MAP:
            /* Maps not supported in native VOL connector */

        case H5O_TYPE_UNKNOWN:
        case H5O_TYPE_NTYPES:
        default:
            ret_value = H5G_UNKNOWN;
    } /* end switch */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G_map_obj_type() */

/*-------------------------------------------------------------------------
 * Function:	H5Gcreate1
 *
 * Purpose:	Creates a new group relative to LOC_ID and gives it the
 *		specified NAME.  The group is opened for write access
 *		and it's object ID is returned.
 *
 *		The SIZE_HINT parameter specifies how much file space to reserve
 *		to store the names that will appear in this group. This number
 *		must be less than or equal to UINT32_MAX. If zero is supplied
 *		for the SIZE_HINT then a default size is chosen.
 *
 * Note:	Deprecated in favor of H5Gcreate2
 *
 * Return:	Success:	The object ID of a new, empty group open for
 *				writing.  Call H5Gclose() when finished with
 *				the group.
 *
 *		Failure:	H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Gcreate1(hid_t loc_id, const char *name, size_t size_hint)
{
    void             *grp = NULL; /* New group created */
    H5VL_object_t    *vol_obj;    /* Object of loc_id */
    H5VL_loc_params_t loc_params;
    hid_t             tmp_gcpl  = H5I_INVALID_HID; /* Temporary group creation property list */
    hid_t             ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE3("i", "i*sz", loc_id, name, size_hint);

    /* Check arguments */
    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "no name given");
    if (size_hint > UINT32_MAX)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "size_hint cannot be larger than UINT32_MAX");

    /* Check if we need to create a non-standard GCPL */
    if (size_hint > 0) {
        H5O_ginfo_t     ginfo;    /* Group info property */
        H5P_genplist_t *gc_plist; /* Property list created */

        /* Get the default property list */
        if (NULL == (gc_plist = (H5P_genplist_t *)H5I_object(H5P_GROUP_CREATE_DEFAULT)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not a property list");

        /* Make a copy of the default property list */
        if ((tmp_gcpl = H5P_copy_plist(gc_plist, false)) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTGET, H5I_INVALID_HID, "unable to copy the creation property list");

        /* Get pointer to the copied property list */
        if (NULL == (gc_plist = (H5P_genplist_t *)H5I_object(tmp_gcpl)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "not a property list");

        /* Get the group info property */
        if (H5P_get(gc_plist, H5G_CRT_GROUP_INFO_NAME, &ginfo) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, H5I_INVALID_HID, "can't get group info");

        /* Set the non-default local heap size hint */
        H5_CHECKED_ASSIGN(ginfo.lheap_size_hint, uint32_t, size_hint, size_t);
        if (H5P_set(gc_plist, H5G_CRT_GROUP_INFO_NAME, &ginfo) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, H5I_INVALID_HID, "can't set group info");
    }
    else
        tmp_gcpl = H5P_GROUP_CREATE_DEFAULT;

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(loc_id) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, H5I_INVALID_HID, "can't set collective metadata read info");

    /* Set location parameters */
    loc_params.type     = H5VL_OBJECT_BY_SELF;
    loc_params.obj_type = H5I_get_type(loc_id);

    /* get the location object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid location identifier");

    /* Create the group */
    if (NULL ==
        (grp = H5VL_group_create(vol_obj, &loc_params, name, H5P_LINK_CREATE_DEFAULT, tmp_gcpl,
                                 H5P_GROUP_ACCESS_DEFAULT, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, H5I_INVALID_HID, "unable to create group");

    /* Get an ID for the group */
    if ((ret_value = H5VL_register(H5I_GROUP, grp, vol_obj->connector, true)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register group");

done:
    if (H5I_INVALID_HID != tmp_gcpl && tmp_gcpl != H5P_GROUP_CREATE_DEFAULT)
        if (H5I_dec_ref(tmp_gcpl) < 0)
            HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, H5I_INVALID_HID, "unable to release property list");

    if (H5I_INVALID_HID == ret_value)
        if (grp && H5VL_group_close(vol_obj, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, H5I_INVALID_HID, "unable to release group");

    FUNC_LEAVE_API(ret_value)
} /* end H5Gcreate1() */

/*-------------------------------------------------------------------------
 * Function:	H5Gopen1
 *
 * Purpose:	Opens an existing group for modification.  When finished,
 *		call H5Gclose() to close it and release resources.
 *
 * Note:	Deprecated in favor of H5Gopen2
 *
 * Return:	Success:	Object ID of the group.
 *
 *		Failure:	H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Gopen1(hid_t loc_id, const char *name)
{
    void             *grp     = NULL; /* Group opened */
    H5VL_object_t    *vol_obj = NULL; /* Object of loc_id */
    H5VL_loc_params_t loc_params;
    hid_t             ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_API(H5I_INVALID_HID)
    H5TRACE2("i", "i*s", loc_id, name);

    /* Check args */
    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "no name");

    /* Set location parameters */
    loc_params.type     = H5VL_OBJECT_BY_SELF;
    loc_params.obj_type = H5I_get_type(loc_id);

    /* get the location object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5I_INVALID_HID, "invalid location identifier");

    /* Open the group */
    if (NULL == (grp = H5VL_group_open(vol_obj, &loc_params, name, H5P_GROUP_ACCESS_DEFAULT,
                                       H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, H5I_INVALID_HID, "unable to open group");

    /* Get an ID for the group */
    if ((ret_value = H5VL_register(H5I_GROUP, grp, vol_obj->connector, true)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register group");

done:
    if (H5I_INVALID_HID == ret_value)
        if (grp && H5VL_group_close(vol_obj, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HDONE_ERROR(H5E_SYM, H5E_CLOSEERROR, H5I_INVALID_HID, "unable to release group");

    FUNC_LEAVE_API(ret_value)
} /* end H5Gopen1() */

/*-------------------------------------------------------------------------
 * Function:	H5Glink
 *
 * Purpose:	Creates a link between two existing objects.  The new
 *              APIs to do this are H5Lcreate_hard and H5Lcreate_soft.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Glink(hid_t cur_loc_id, H5G_link_t type, const char *cur_name, const char *new_name)
{
    H5VL_link_create_args_t vol_cb_args;         /* Arguments to VOL callback */
    herr_t                  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "iLl*s*s", cur_loc_id, type, cur_name, new_name);

    /* Check arguments */
    if (!cur_name || !*cur_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no current name specified");
    if (!new_name || !*new_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no new name specified");

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(cur_loc_id) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, FAIL, "can't set collective metadata read info");

    /* Create link */
    if (type == H5L_TYPE_HARD) {
        H5VL_object_t    *vol_obj; /* Object of loc_id */
        H5VL_loc_params_t new_loc_params;
        H5VL_object_t     tmp_vol_obj; /* Temporary object */

        /* Set up new location struct */
        new_loc_params.type                         = H5VL_OBJECT_BY_NAME;
        new_loc_params.loc_data.loc_by_name.name    = new_name;
        new_loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;

        /* Get the location object */
        if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(cur_loc_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

        /* Construct a temporary VOL object */
        tmp_vol_obj.data      = NULL;
        tmp_vol_obj.connector = vol_obj->connector;

        /* Set up VOL callback arguments */
        vol_cb_args.op_type                                                = H5VL_LINK_CREATE_HARD;
        vol_cb_args.args.hard.curr_obj                                     = vol_obj->data;
        vol_cb_args.args.hard.curr_loc_params.type                         = H5VL_OBJECT_BY_NAME;
        vol_cb_args.args.hard.curr_loc_params.obj_type                     = H5I_get_type(cur_loc_id);
        vol_cb_args.args.hard.curr_loc_params.loc_data.loc_by_name.name    = cur_name;
        vol_cb_args.args.hard.curr_loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;

        /* Create the link through the VOL */
        if (H5VL_link_create(&vol_cb_args, &tmp_vol_obj, &new_loc_params, H5P_LINK_CREATE_DEFAULT,
                             H5P_LINK_ACCESS_DEFAULT, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to create link");
    } /* end if */
    else if (type == H5L_TYPE_SOFT) {
        H5VL_object_t    *vol_obj; /* Object of loc_id */
        H5VL_loc_params_t loc_params;

        /* Set up location struct */
        loc_params.type                         = H5VL_OBJECT_BY_NAME;
        loc_params.loc_data.loc_by_name.name    = new_name;
        loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;
        loc_params.obj_type                     = H5I_get_type(cur_loc_id);

        /* get the location object */
        if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(cur_loc_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

        /* Set up VOL callback arguments */
        vol_cb_args.op_type          = H5VL_LINK_CREATE_SOFT;
        vol_cb_args.args.soft.target = cur_name;

        /* Create the link through the VOL */
        if (H5VL_link_create(&vol_cb_args, vol_obj, &loc_params, H5P_LINK_CREATE_DEFAULT,
                             H5P_LINK_ACCESS_DEFAULT, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to create link");
    } /* end else-if */
    else
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Not a valid link type");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Glink() */

/*-------------------------------------------------------------------------
 * Function:	H5Glink2
 *
 * Purpose:	Creates a link between two existing objects.  The new
 *              APIs to do this are H5Lcreate_hard and H5Lcreate_soft.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Glink2(hid_t cur_loc_id, const char *cur_name, H5G_link_t type, hid_t new_loc_id, const char *new_name)
{
    H5VL_link_create_args_t vol_cb_args;         /* Arguments to VOL callback */
    herr_t                  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "i*sLli*s", cur_loc_id, cur_name, type, new_loc_id, new_name);

    /* Check arguments */
    if (!cur_name || !*cur_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no current name specified");
    if (!new_name || !*new_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no new name specified");

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(cur_loc_id) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, FAIL, "can't set collective metadata read info");

    /* Create the appropriate kind of link */
    if (type == H5L_TYPE_HARD) {
        H5VL_object_t    *vol_obj1; /* Object of loc_id */
        H5VL_object_t    *vol_obj2; /* Object of loc_id */
        H5VL_loc_params_t new_loc_params;

        /* Set up new location struct */
        new_loc_params.type                         = H5VL_OBJECT_BY_NAME;
        new_loc_params.obj_type                     = H5I_get_type(new_loc_id);
        new_loc_params.loc_data.loc_by_name.name    = new_name;
        new_loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;

        /* Get the location objects */
        if (NULL == (vol_obj1 = (H5VL_object_t *)H5I_object(cur_loc_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");
        if (NULL == (vol_obj2 = (H5VL_object_t *)H5I_object(new_loc_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

        /* Set up VOL callback arguments */
        vol_cb_args.op_type                                                = H5VL_LINK_CREATE_HARD;
        vol_cb_args.args.hard.curr_obj                                     = vol_obj1->data;
        vol_cb_args.args.hard.curr_loc_params.type                         = H5VL_OBJECT_BY_NAME;
        vol_cb_args.args.hard.curr_loc_params.obj_type                     = H5I_get_type(cur_loc_id);
        vol_cb_args.args.hard.curr_loc_params.loc_data.loc_by_name.name    = cur_name;
        vol_cb_args.args.hard.curr_loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;

        /* Create the link through the VOL */
        if (H5VL_link_create(&vol_cb_args, vol_obj2, &new_loc_params, H5P_LINK_CREATE_DEFAULT,
                             H5P_LINK_ACCESS_DEFAULT, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to create link");
    } /* end if */
    else if (type == H5L_TYPE_SOFT) {
        H5VL_object_t    *vol_obj; /* Object of loc_id */
        H5VL_loc_params_t loc_params;

        /* Soft links only need one location, the new_loc_id, but it's possible that
         * new_loc_id is H5L_SAME_LOC */
        if (new_loc_id == H5L_SAME_LOC)
            new_loc_id = cur_loc_id;

        /* Set up location struct */
        loc_params.type                         = H5VL_OBJECT_BY_NAME;
        loc_params.loc_data.loc_by_name.name    = new_name;
        loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;
        loc_params.obj_type                     = H5I_get_type(new_loc_id);

        /* get the location object */
        if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(new_loc_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

        /* Set up VOL callback arguments */
        vol_cb_args.op_type          = H5VL_LINK_CREATE_SOFT;
        vol_cb_args.args.soft.target = cur_name;

        /* Create the link through the VOL */
        if (H5VL_link_create(&vol_cb_args, vol_obj, &loc_params, H5P_LINK_CREATE_DEFAULT,
                             H5P_LINK_ACCESS_DEFAULT, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to create link");
    } /* end else-if */
    else
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "not a valid link type");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Glink2() */

/*-------------------------------------------------------------------------
 * Function:	H5Gmove
 *
 * Purpose:	Moves and renames a link.  The new API to do this is H5Lmove.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Gmove(hid_t src_loc_id, const char *src_name, const char *dst_name)
{
    H5VL_object_t    *vol_obj; /* Object of loc_id */
    H5VL_loc_params_t loc_params1;
    H5VL_loc_params_t loc_params2;
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "i*s*s", src_loc_id, src_name, dst_name);

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(src_loc_id) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, FAIL, "can't set collective metadata read info");

    loc_params1.type                         = H5VL_OBJECT_BY_NAME;
    loc_params1.obj_type                     = H5I_get_type(src_loc_id);
    loc_params1.loc_data.loc_by_name.name    = src_name;
    loc_params1.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;

    loc_params2.type                         = H5VL_OBJECT_BY_NAME;
    loc_params2.loc_data.loc_by_name.name    = dst_name;
    loc_params2.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;

    /* get the location object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(src_loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Move the link */
    if (H5VL_link_move(vol_obj, &loc_params1, NULL, &loc_params2, H5P_LINK_CREATE_DEFAULT,
                       H5P_LINK_ACCESS_DEFAULT, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTMOVE, FAIL, "couldn't move link");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Gmove() */

/*-------------------------------------------------------------------------
 * Function:	H5Gmove2
 *
 * Purpose:	Moves and renames a link.  The new API to do this is H5Lmove.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Gmove2(hid_t src_loc_id, const char *src_name, hid_t dst_loc_id, const char *dst_name)
{
    H5VL_object_t    *vol_obj1 = NULL; /* Object of src_id */
    H5VL_loc_params_t loc_params1;
    H5VL_object_t    *vol_obj2 = NULL; /* Object of dst_id */
    H5VL_loc_params_t loc_params2;
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "i*si*s", src_loc_id, src_name, dst_loc_id, dst_name);

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(dst_loc_id) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, FAIL, "can't set collective metadata read info");

    /* Set location parameter for source object */
    loc_params1.type                         = H5VL_OBJECT_BY_NAME;
    loc_params1.loc_data.loc_by_name.name    = src_name;
    loc_params1.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;
    loc_params1.obj_type                     = H5I_get_type(src_loc_id);

    /* Set location parameter for destination object */
    loc_params2.type                         = H5VL_OBJECT_BY_NAME;
    loc_params2.loc_data.loc_by_name.name    = dst_name;
    loc_params2.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;
    loc_params2.obj_type                     = H5I_get_type(dst_loc_id);

    if (H5L_SAME_LOC != src_loc_id)
        /* get the location object */
        if (NULL == (vol_obj1 = (H5VL_object_t *)H5I_object(src_loc_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");
    if (H5L_SAME_LOC != dst_loc_id)
        /* get the location object */
        if (NULL == (vol_obj2 = (H5VL_object_t *)H5I_object(dst_loc_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Move the link */
    if (H5VL_link_move(vol_obj1, &loc_params1, vol_obj2, &loc_params2, H5P_LINK_CREATE_DEFAULT,
                       H5P_LINK_ACCESS_DEFAULT, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTMOVE, FAIL, "unable to move link");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Gmove2() */

/*-------------------------------------------------------------------------
 * Function:	H5Gunlink
 *
 * Purpose:	Removes a link.  The new API is H5Ldelete/H5Ldelete_by_idx.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Gunlink(hid_t loc_id, const char *name)
{
    H5VL_object_t            *vol_obj;     /* Object of loc_id */
    H5VL_link_specific_args_t vol_cb_args; /* Arguments to VOL callback */
    H5VL_loc_params_t         loc_params;
    herr_t                    ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "i*s", loc_id, name);

    /* Check arguments */
    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no name");

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(loc_id) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, FAIL, "can't set collective metadata read info");

    loc_params.type                         = H5VL_OBJECT_BY_NAME;
    loc_params.obj_type                     = H5I_get_type(loc_id);
    loc_params.loc_data.loc_by_name.name    = name;
    loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;

    /* Get the location object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type = H5VL_LINK_DELETE;

    /* Delete the link */
    if (H5VL_link_specific(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTDELETE, FAIL, "couldn't delete link");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Gunlink() */

/*-------------------------------------------------------------------------
 * Function:	H5Gget_linkval
 *
 * Purpose:	Retrieve's a soft link's data.  The new API is
 *              H5Lget_val/H5Lget_val_by_idx.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Gget_linkval(hid_t loc_id, const char *name, size_t size, char *buf /*out*/)
{
    H5VL_object_t       *vol_obj;     /* Object of loc_id */
    H5VL_link_get_args_t vol_cb_args; /* Arguments to VOL callback */
    H5VL_loc_params_t    loc_params;
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "i*szx", loc_id, name, size, buf);

    /* Check arguments */
    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no name specified");

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(loc_id) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, FAIL, "can't set collective metadata read info");

    /* Set up location struct */
    loc_params.type                         = H5VL_OBJECT_BY_NAME;
    loc_params.obj_type                     = H5I_get_type(loc_id);
    loc_params.loc_data.loc_by_name.name    = name;
    loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;

    /* Get the location object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type               = H5VL_LINK_GET_VAL;
    vol_cb_args.args.get_val.buf      = buf;
    vol_cb_args.args.get_val.buf_size = size;

    /* Get the link value */
    if (H5VL_link_get(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "unable to get link value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Gget_linkval() */

/*-------------------------------------------------------------------------
 * Function:	H5Gset_comment
 *
 * Purpose:     Gives the specified object a comment.  The COMMENT string
 *		should be a null terminated string.  An object can have only
 *		one comment at a time.  Passing NULL for the COMMENT argument
 *		will remove the comment property from the object.
 *
 * Note:	Deprecated in favor of H5Oset_comment/H5Oset_comment_by_name
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Gset_comment(hid_t loc_id, const char *name, const char *comment)
{
    H5VL_object_t                     *vol_obj;      /* Object of loc_id */
    H5VL_optional_args_t               vol_cb_args;  /* Arguments to VOL callback */
    H5VL_native_object_optional_args_t obj_opt_args; /* Arguments for optional operation */
    H5VL_loc_params_t                  loc_params;
    herr_t                             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "i*s*s", loc_id, name, comment);

    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no name specified");

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(loc_id) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, FAIL, "can't set collective metadata read info");

    /* Fill in location struct fields */
    loc_params.type                         = H5VL_OBJECT_BY_NAME;
    loc_params.loc_data.loc_by_name.name    = name;
    loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;
    loc_params.obj_type                     = H5I_get_type(loc_id);

    /* Get the location object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set up VOL callback arguments */
    obj_opt_args.set_comment.comment = comment;
    vol_cb_args.op_type              = H5VL_NATIVE_OBJECT_SET_COMMENT;
    vol_cb_args.args                 = &obj_opt_args;

    /* Set the comment */
    if (H5VL_object_optional(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) <
        0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, FAIL, "unable to set comment value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Gset_comment() */

/*-------------------------------------------------------------------------
 * Function:	H5Gget_comment
 *
 * Purpose:	Return at most BUFSIZE characters of the comment for the
 *		specified object.  If BUFSIZE is large enough to hold the
 *		entire comment then the comment string will be null
 *		terminated, otherwise it will not.  If the object does not
 *		have a comment value then no bytes are copied to the BUF
 *		buffer.
 *
 * Note:	Deprecated in favor of H5Oget_comment/H5Oget_comment_by_name
 *
 * Return:	Success:	Number of characters in the comment counting
 *				the null terminator.  The value returned may
 *				be larger than the BUFSIZE argument.
 *
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
int
H5Gget_comment(hid_t loc_id, const char *name, size_t bufsize, char *buf /*out*/)
{
    H5VL_object_t                     *vol_obj;      /* Object of loc_id */
    H5VL_optional_args_t               vol_cb_args;  /* Arguments to VOL callback */
    H5VL_native_object_optional_args_t obj_opt_args; /* Arguments for optional operation */
    H5VL_loc_params_t                  loc_params;
    size_t                             comment_len = 0; /* Length of comment */
    int                                ret_value;       /* Return value */

    FUNC_ENTER_API(-1)
    H5TRACE4("Is", "i*szx", loc_id, name, bufsize, buf);

    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, -1, "no name specified");
    if (bufsize > 0 && !buf)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, -1, "no buffer specified");

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(loc_id) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, -1, "can't set collective metadata read info");

    /* Fill in location struct fields */
    loc_params.type                         = H5VL_OBJECT_BY_NAME;
    loc_params.loc_data.loc_by_name.name    = name;
    loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;
    loc_params.obj_type                     = H5I_get_type(loc_id);

    /* Get the location object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, -1, "invalid location identifier");

    /* Set up VOL callback arguments */
    obj_opt_args.get_comment.buf         = buf;
    obj_opt_args.get_comment.buf_size    = bufsize;
    obj_opt_args.get_comment.comment_len = &comment_len;
    vol_cb_args.op_type                  = H5VL_NATIVE_OBJECT_GET_COMMENT;
    vol_cb_args.args                     = &obj_opt_args;

    /* Get the comment */
    if (H5VL_object_optional(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) <
        0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, -1, "unable to get comment value");

    /* Set return value */
    ret_value = (int)comment_len;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Gget_comment() */

/*-------------------------------------------------------------------------
 * Function:    H5Giterate
 *
 * Purpose:     Iterates over the entries of a group.  The LOC_ID and NAME
 *              identify the group over which to iterate and IDX indicates
 *              where to start iterating (zero means at the beginning).	 The
 *              OPERATOR is called for each member and the iteration
 *              continues until the operator returns non-zero or all members
 *              are processed. The operator is passed a group ID for the
 *              group being iterated, a member name, and OP_DATA for each
 *              member.
 *
 * NOTE:        Deprecated in favor of H5Literate
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
H5Giterate(hid_t loc_id, const char *name, int *idx_p, H5G_iterate_t op, void *op_data)
{
    H5VL_object_t                    *vol_obj;      /* Object of loc_id */
    H5VL_optional_args_t              vol_cb_args;  /* Arguments to VOL callback */
    H5VL_native_group_optional_args_t grp_opt_args; /* Arguments for optional operation */
    hsize_t                           last_obj = 0; /* Pointer to index value */
    herr_t                            ret_value;    /* Return value                     */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "i*s*IsGi*x", loc_id, name, idx_p, op, op_data);

    /* Check args */
    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no name specified");
    if (idx_p && *idx_p < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid index specified");
    if (!op)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no operator specified");

    /* Get the object pointer */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ID, H5E_BADTYPE, (-1), "invalid identifier");

    /* Set up VOL callback arguments */
    grp_opt_args.iterate_old.loc_params.type                         = H5VL_OBJECT_BY_NAME;
    grp_opt_args.iterate_old.loc_params.loc_data.loc_by_name.name    = name;
    grp_opt_args.iterate_old.loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;
    grp_opt_args.iterate_old.loc_params.obj_type                     = H5I_get_type(loc_id);
    grp_opt_args.iterate_old.idx                                     = (hsize_t)(idx_p == NULL ? 0 : *idx_p);
    grp_opt_args.iterate_old.last_obj                                = &last_obj;
    grp_opt_args.iterate_old.op                                      = op;
    grp_opt_args.iterate_old.op_data                                 = op_data;
    vol_cb_args.op_type                                              = H5VL_NATIVE_GROUP_ITERATE_OLD;
    vol_cb_args.args                                                 = &grp_opt_args;

    /* Call private iteration function, through VOL callback */
    if ((ret_value = H5VL_group_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL)) <
        0)
        HERROR(H5E_SYM, H5E_BADITER, "error iterating over group's links");

    /* Set value to return */
    if (idx_p)
        *idx_p = (int)last_obj;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Giterate() */

/*-------------------------------------------------------------------------
 * Function:	H5Gget_num_objs
 *
 * Purpose:     Returns the number of objects in the group.  It iterates
 *              all B-tree leaves and sum up total number of group members.
 *
 * Note:	Deprecated in favor of H5Gget_info
 *
 * Return:	Success:        Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Gget_num_objs(hid_t loc_id, hsize_t *num_objs /*out*/)
{
    H5VL_object_t        *vol_obj = NULL;      /* Object of loc_id */
    H5VL_group_get_args_t vol_cb_args;         /* Arguments to VOL callback */
    H5I_type_t            id_type;             /* Type of ID */
    H5G_info_t            grp_info;            /* Group information */
    herr_t                ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", loc_id, num_objs);

    /* Check args */
    id_type = H5I_get_type(loc_id);
    if (!(H5I_GROUP == id_type || H5I_FILE == id_type))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid group (or file) ID");
    if (!num_objs)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "bad pointer to # of objects");

    /* Set up VOL callback & object access arguments */
    vol_cb_args.op_type = H5VL_GROUP_GET_INFO;
    if (H5VL_setup_self_args(loc_id, &vol_obj, &vol_cb_args.args.get_info.loc_params) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, FAIL, "can't set object access arguments");
    vol_cb_args.args.get_info.ginfo = &grp_info;

    /* Retrieve the group's information */
    if (H5VL_group_get(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "unable to get group info");

    /* Set the number of objects [i.e. links] in the group */
    *num_objs = grp_info.nlinks;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Gget_num_objs() */

/*-------------------------------------------------------------------------
 * Function:	H5Gget_objinfo
 *
 * Purpose:	Returns information about an object.  If FOLLOW_LINK is
 *		non-zero then all symbolic links are followed; otherwise all
 *		links except the last component of the name are followed.
 *
 * Note:	Deprecated in favor of H5Lget_info/H5Oget_info
 *
 * Return:	Non-negative on success, with the fields of STATBUF (if
 *              non-null) initialized. Negative on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Gget_objinfo(hid_t loc_id, const char *name, hbool_t follow_link, H5G_stat_t *statbuf /*out*/)
{
    H5VL_object_t                    *vol_obj = NULL;      /* Object of loc_id */
    H5VL_optional_args_t              vol_cb_args;         /* Arguments to VOL callback */
    H5VL_native_group_optional_args_t grp_opt_args;        /* Arguments for optional operation */
    herr_t                            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "i*sbx", loc_id, name, follow_link, statbuf);

    /* Check arguments */
    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no name specified");

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(loc_id) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, FAIL, "can't set collective metadata read info");

    /* Get the location object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Set up VOL callback arguments */
    grp_opt_args.get_objinfo.loc_params.type                         = H5VL_OBJECT_BY_NAME;
    grp_opt_args.get_objinfo.loc_params.loc_data.loc_by_name.name    = name;
    grp_opt_args.get_objinfo.loc_params.loc_data.loc_by_name.lapl_id = H5P_LINK_ACCESS_DEFAULT;
    grp_opt_args.get_objinfo.loc_params.obj_type                     = H5I_get_type(loc_id);
    grp_opt_args.get_objinfo.follow_link                             = follow_link;
    grp_opt_args.get_objinfo.statbuf                                 = statbuf;
    vol_cb_args.op_type                                              = H5VL_NATIVE_GROUP_GET_OBJINFO;
    vol_cb_args.args                                                 = &grp_opt_args;

    /* Retrieve the object's information */
    if (H5VL_group_optional(vol_obj, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't get info for object: '%s'", name);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Gget_objinfo() */

/*-------------------------------------------------------------------------
 * Function:	H5G__get_objinfo_cb
 *
 * Purpose:	Callback for retrieving info about an object.  This routine
 *              gets the info
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__get_objinfo_cb(H5G_loc_t H5_ATTR_UNUSED *grp_loc /*in*/, const char *name, const H5O_link_t *lnk,
                    H5G_loc_t *obj_loc, void *_udata /*in,out*/, H5G_own_loc_t *own_loc /*out*/)
{
    H5G_trav_goi_t *udata     = (H5G_trav_goi_t *)_udata; /* User data passed in */
    herr_t          ret_value = SUCCEED;                  /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check if the name in this group resolved to a valid link */
    if (lnk == NULL && obj_loc == NULL)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "'%s' doesn't exist", name);

    /* Only modify user's buffer if it's available */
    if (udata->statbuf) {
        H5G_stat_t *statbuf = udata->statbuf; /* Convenience pointer for statbuf */

        /* Common code to retrieve the file's fileno */
        if (H5F_get_fileno((obj_loc ? obj_loc : grp_loc)->oloc->file, &statbuf->fileno[0]) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL, "unable to read fileno");

        /* Info for soft and UD links is gotten by H5L_get_info. If we have
         *      a hard link, follow it and get info on the object
         */
        if (udata->follow_link || !lnk || (lnk->type == H5L_TYPE_HARD)) {
            H5O_info2_t       dm_info;  /* Data model information */
            H5O_native_info_t nat_info; /* Native information */
            haddr_t           obj_addr; /* Address of object */

            /* Go retrieve the data model & native object information */
            /* (don't need index & heap info) */
            assert(obj_loc);
            if (H5O_get_info(obj_loc->oloc, &dm_info, H5O_INFO_BASIC | H5O_INFO_TIME) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "unable to get data model object info");
            if (H5O_get_native_info(obj_loc->oloc, &nat_info, H5O_INFO_HDR) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "unable to get native object info");

            /* Get mapped object type */
            statbuf->type = H5G_map_obj_type(dm_info.type);

            /* Get object number (i.e. address) for object */
            if (H5VL_native_token_to_addr(obj_loc->oloc->file, H5I_FILE, dm_info.token, &obj_addr) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTUNSERIALIZE, FAIL,
                            "can't deserialize object token into address");

            statbuf->objno[0] = (unsigned long)(obj_addr);
#if H5_SIZEOF_UINT64_T > H5_SIZEOF_LONG
            statbuf->objno[1] = (unsigned long)(obj_addr >> 8 * sizeof(long));
#else
            statbuf->objno[1] = 0;
#endif
            /* Get # of hard links pointing to object */
            statbuf->nlink = dm_info.rc;

            /* Get modification time for object */
            statbuf->mtime = dm_info.ctime;

            /* Retrieve the object header information */
            statbuf->ohdr.size    = nat_info.hdr.space.total;
            statbuf->ohdr.free    = nat_info.hdr.space.free;
            statbuf->ohdr.nmesgs  = nat_info.hdr.nmesgs;
            statbuf->ohdr.nchunks = nat_info.hdr.nchunks;
        } /* end if */
    }     /* end if */

done:
    /* Indicate that this callback didn't take ownership of the group *
     * location for the object */
    *own_loc = H5G_OWN_NONE;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__get_objinfo_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5G__get_objinfo
 *
 * Purpose:	Returns information about an object.
 *
 * Return:	Success:	Non-negative with info about the object
 *				returned through STATBUF if it isn't the null
 *				pointer.
 *
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__get_objinfo(const H5G_loc_t *loc, const char *name, bool follow_link, H5G_stat_t *statbuf /*out*/)
{
    H5G_trav_goi_t udata;               /* User data for callback */
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(loc);
    assert(name && *name);

    /* Reset stat buffer */
    if (statbuf)
        memset(statbuf, 0, sizeof(H5G_stat_t));

    /* Set up user data for retrieving information */
    udata.statbuf     = statbuf;
    udata.follow_link = follow_link;
    udata.loc_file    = loc->oloc->file;

    /* Traverse the group hierarchy to locate the object to get info about */
    if (H5G_traverse(loc, name,
                     (unsigned)(follow_link ? H5G_TARGET_NORMAL : (H5G_TARGET_SLINK | H5G_TARGET_UDLINK)),
                     H5G__get_objinfo_cb, &udata) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_EXISTS, FAIL, "name doesn't exist");

    /* If we're pointing at a soft or UD link, get the real link length and type */
    if (statbuf && follow_link == 0) {
        H5L_info2_t linfo; /* Link information buffer */
        herr_t      ret;

        /* Get information about link to the object. If this fails, e.g.
         * because the object is ".", just treat the object as a hard link. */
        H5E_BEGIN_TRY
        {
            ret = H5L_get_info(loc, name, &linfo);
        }
        H5E_END_TRY

        if (ret >= 0 && linfo.type != H5L_TYPE_HARD) {
            statbuf->linklen = linfo.u.val_size;
            if (linfo.type == H5L_TYPE_SOFT) {
                statbuf->type = H5G_LINK;
            }
            else {
                /* UD link. H5L_get_info checked for invalid link classes */
                assert(linfo.type >= H5L_TYPE_UD_MIN && linfo.type <= H5L_TYPE_MAX);
                statbuf->type = H5G_UDLINK;
            }
        }
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__get_objinfo() */

/*-------------------------------------------------------------------------
 * Function:	H5Gget_objname_by_idx
 *
 * Purpose:     Returns the name of objects in the group by giving index.
 *              If `name' is non-NULL then write up to `size' bytes into that
 *              buffer and always return the length of the entry name.
 *              Otherwise `size' is ignored and the function does not store the name,
 *              just returning the number of characters required to store the name.
 *              If an error occurs then the buffer pointed to by `name' (NULL or non-NULL)
 *              is unchanged and the function returns a negative value.
 *              If a zero is returned for the name's length, then there is no name
 *              associated with the ID.
 *
 * Note:	Deprecated in favor of H5Lget_name_by_idx
 *
 * Return:	Success:        Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
ssize_t
H5Gget_objname_by_idx(hid_t loc_id, hsize_t idx, char *name /*out*/, size_t size)
{
    H5VL_object_t       *vol_obj;     /* Object of loc_id */
    H5VL_link_get_args_t vol_cb_args; /* Arguments to VOL callback */
    H5VL_loc_params_t    loc_params;
    size_t               name_len = 0; /* Length of object name */
    ssize_t              ret_value;    /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("Zs", "ihxz", loc_id, idx, name, size);

    /* Set up collective metadata if appropriate */
    if (H5CX_set_loc(loc_id) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, (-1), "can't set collective metadata read info");

    /* Set up location struct */
    loc_params.type                         = H5VL_OBJECT_BY_IDX;
    loc_params.loc_data.loc_by_idx.name     = ".";
    loc_params.loc_data.loc_by_idx.idx_type = H5_INDEX_NAME;
    loc_params.loc_data.loc_by_idx.order    = H5_ITER_INC;
    loc_params.loc_data.loc_by_idx.n        = idx;
    loc_params.loc_data.loc_by_idx.lapl_id  = H5P_LINK_ACCESS_DEFAULT;
    loc_params.obj_type                     = H5I_get_type(loc_id);

    /* Get the location object */
    if (NULL == (vol_obj = (H5VL_object_t *)H5I_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, (-1), "invalid location identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                 = H5VL_LINK_GET_NAME;
    vol_cb_args.args.get_name.name_size = size;
    vol_cb_args.args.get_name.name      = name;
    vol_cb_args.args.get_name.name_len  = &name_len;

    /* Call internal function */
    if (H5VL_link_get(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, (-1), "can't get object name");

    /* Set the return value */
    ret_value = (ssize_t)name_len;

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Gget_objname_by_idx() */

/*-------------------------------------------------------------------------
 * Function:	H5Gget_objtype_by_idx
 *
 * Purpose:     Returns the type of objects in the group by giving index.
 *
 * Note:	Deprecated in favor of H5Lget_info/H5Oget_info
 *
 * Return:	Success:        H5G_GROUP(1), H5G_DATASET(2), H5G_TYPE(3)
 *		Failure:	H5G_UNKNOWN
 *
 *-------------------------------------------------------------------------
 */
H5G_obj_t
H5Gget_objtype_by_idx(hid_t loc_id, hsize_t idx)
{
    H5VL_object_t         *vol_obj;     /* Object of loc_id */
    H5VL_object_get_args_t vol_cb_args; /* Arguments to VOL callback */
    H5VL_loc_params_t      loc_params;
    H5O_info2_t            oinfo;     /* Object info (contains object type) */
    H5G_obj_t              ret_value; /* Return value */

    FUNC_ENTER_API(H5G_UNKNOWN)
    H5TRACE2("Go", "ih", loc_id, idx);

    /* Set location parameters */
    loc_params.type                         = H5VL_OBJECT_BY_IDX;
    loc_params.loc_data.loc_by_idx.name     = ".";
    loc_params.loc_data.loc_by_idx.idx_type = H5_INDEX_NAME;
    loc_params.loc_data.loc_by_idx.order    = H5_ITER_INC;
    loc_params.loc_data.loc_by_idx.n        = idx;
    loc_params.loc_data.loc_by_idx.lapl_id  = H5P_LINK_ACCESS_DEFAULT;
    loc_params.obj_type                     = H5I_get_type(loc_id);

    /* Get the location object */
    if (NULL == (vol_obj = H5VL_vol_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, H5G_UNKNOWN, "invalid location identifier");

    /* Set up VOL callback arguments */
    vol_cb_args.op_type              = H5VL_OBJECT_GET_INFO;
    vol_cb_args.args.get_info.oinfo  = &oinfo;
    vol_cb_args.args.get_info.fields = H5O_INFO_BASIC;

    /* Retrieve the object's basic information (which includes its type) */
    if (H5VL_object_get(vol_obj, &loc_params, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, H5_REQUEST_NULL) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_BADTYPE, H5G_UNKNOWN, "can't get object info");

    /* Map to group object type */
    if (H5G_UNKNOWN == (ret_value = H5G_map_obj_type(oinfo.type)))
        HGOTO_ERROR(H5E_SYM, H5E_BADTYPE, H5G_UNKNOWN, "can't determine object type");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Gget_objtype_by_idx() */
#endif /* H5_NO_DEPRECATED_SYMBOLS */
