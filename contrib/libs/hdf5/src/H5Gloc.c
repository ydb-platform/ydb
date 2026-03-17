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
 * Created:		H5Gloc.c
 *
 * Purpose:		Functions for working with group "locations"
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
#include "H5private.h"  /* Generic Functions			*/
#include "H5Aprivate.h" /* Attributes				*/
#include "H5Dprivate.h" /* Datasets				*/
#include "H5Eprivate.h" /* Error handling		  	*/
#include "H5Gpkg.h"     /* Groups		  		*/
#include "H5Iprivate.h" /* IDs			  		*/
#include "H5Lprivate.h" /* Links				*/

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/* User data for looking up an object in a group */
typedef struct {
    /* upward */
    H5G_loc_t *loc; /* Group location to set */
} H5G_loc_fnd_t;

/* User data for looking up an object in a group by index */
typedef struct {
    /* downward */
    H5_index_t      idx_type; /* Index to use */
    H5_iter_order_t order;    /* Iteration order within index */
    hsize_t         n;        /* Offset within index */

    /* upward */
    H5G_loc_t *loc; /* Group location to set */
} H5G_loc_fbi_t;

/* User data for getting an object's data model info in a group */
typedef struct {
    /* downward */
    unsigned fields; /* which fields in H5O_info2_t struct to fill in */

    /* upward */
    H5O_info2_t *oinfo; /* Object information to retrieve */
} H5G_loc_info_t;

/* User data for getting an object's native info in a group */
typedef struct {
    /* downward */
    unsigned fields; /* which fields in H5O_native_info_t struct to fill in */

    /* upward */
    H5O_native_info_t *oinfo; /* Object information to retrieve */
} H5G_loc_native_info_t;

/* User data for setting an object's comment in a group */
typedef struct {
    /* downward */
    const char *comment; /* Object comment buffer */

    /* upward */
} H5G_loc_sc_t;

/* User data for getting an object's comment in a group */
typedef struct {
    /* downward */
    char  *comment; /* Object comment buffer */
    size_t bufsize; /* Size of object comment buffer */

    /* upward */
    size_t comment_size; /* Actual size of object comment */
} H5G_loc_gc_t;

/********************/
/* Local Prototypes */
/********************/

/* Group traversal callbacks */
static herr_t H5G__loc_find_cb(H5G_loc_t *grp_loc, const char *name, const H5O_link_t *lnk,
                               H5G_loc_t *obj_loc, void *_udata, H5G_own_loc_t *own_loc);
static herr_t H5G__loc_find_by_idx_cb(H5G_loc_t *grp_loc, const char *name, const H5O_link_t *lnk,
                                      H5G_loc_t *obj_loc, void *_udata, H5G_own_loc_t *own_loc);
static herr_t H5G__loc_addr_cb(H5G_loc_t *grp_loc, const char *name, const H5O_link_t *lnk,
                               H5G_loc_t *obj_loc, void *_udata, H5G_own_loc_t *own_loc);
static herr_t H5G__loc_info_cb(H5G_loc_t *grp_loc, const char *name, const H5O_link_t *lnk,
                               H5G_loc_t *obj_loc, void *_udata, H5G_own_loc_t *own_loc);
static herr_t H5G__loc_native_info_cb(H5G_loc_t *grp_loc, const char *name, const H5O_link_t *lnk,
                                      H5G_loc_t *obj_loc, void *_udata, H5G_own_loc_t *own_loc);
static herr_t H5G__loc_set_comment_cb(H5G_loc_t *grp_loc, const char *name, const H5O_link_t *lnk,
                                      H5G_loc_t *obj_loc, void *_udata, H5G_own_loc_t *own_loc);
static herr_t H5G__loc_get_comment_cb(H5G_loc_t *grp_loc, const char *name, const H5O_link_t *lnk,
                                      H5G_loc_t *obj_loc, void *_udata, H5G_own_loc_t *own_loc);

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
 * Function:    H5G_loc_real
 *
 * Purpose:     Utility routine to get object location
 *
 * Returns:     SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_loc_real(void *obj, H5I_type_t type, H5G_loc_t *loc)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    switch (type) {
        case H5I_FILE: {
            H5F_t *f = (H5F_t *)obj;

            /* Construct a group location for root group of the file */
            if (H5G_root_loc(f, loc) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "unable to create location for file");
            break;
        }

        case H5I_GROUP: {
            H5G_t *group = (H5G_t *)obj;

            if (NULL == (loc->oloc = H5G_oloc(group)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to get object location of group");
            if (NULL == (loc->path = H5G_nameof(group)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to get path of group");
            break;
        }

        case H5I_DATATYPE: {
            H5T_t *dt = NULL;

            /* Get the actual datatype object if the VOL object is set */
            dt = H5T_get_actual_type((H5T_t *)obj);

            if (NULL == (loc->oloc = H5T_oloc(dt)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to get object location of datatype");
            if (NULL == (loc->path = H5T_nameof(dt)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to get path of datatype");
            break;
        }

        case H5I_DATASET: {
            H5D_t *dset = (H5D_t *)obj;

            if (NULL == (loc->oloc = H5D_oloc(dset)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to get object location of dataset");
            if (NULL == (loc->path = H5D_nameof(dset)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to get path of dataset");
            break;
        }

        case H5I_ATTR: {
            H5A_t *attr = (H5A_t *)obj;

            if (NULL == (loc->oloc = H5A_oloc(attr)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to get object location of attribute");
            if (NULL == (loc->path = H5A_nameof(attr)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to get path of attribute");
            break;
        }

        case H5I_DATASPACE:
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to get group location of dataspace");

        case H5I_MAP:
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "maps not supported in native VOL connector");

        case H5I_GENPROP_CLS:
        case H5I_GENPROP_LST:
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to get group location of property list");

        case H5I_ERROR_CLASS:
        case H5I_ERROR_MSG:
        case H5I_ERROR_STACK:
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                        "unable to get group location of error class, message or stack");

        case H5I_VFL:
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                        "unable to get group location of a virtual file driver (VFD)");

        case H5I_VOL:
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                        "unable to get group location of a virtual object layer (VOL) connector");

        case H5I_SPACE_SEL_ITER:
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                        "unable to get group location of a dataspace selection iterator");

        case H5I_EVENTSET:
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unable to get group location of a event set");

        case H5I_UNINIT:
        case H5I_BADID:
        case H5I_NTYPES:
        default:
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid location ID");
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G_loc_real() */

/*-------------------------------------------------------------------------
 * Function:    H5G_loc
 *
 * Purpose:     Given an object ID return a location for the object.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_loc(hid_t loc_id, H5G_loc_t *loc)
{
    void  *obj       = NULL;    /* VOL object   */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Get the object from the VOL */
    if (NULL == (obj = H5VL_object(loc_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "invalid location identifier");

    /* Fill in the struct */
    if (H5G_loc_real(obj, H5I_get_type(loc_id), loc) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "unable to fill in location struct");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G_loc() */

/*-------------------------------------------------------------------------
 * Function:	H5G_loc_copy
 *
 * Purpose:	Copy over information for a location
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_loc_copy(H5G_loc_t *dst, const H5G_loc_t *src, H5_copy_depth_t depth)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args. */
    assert(dst);
    assert(src);

    /* Copy components of the location */
    if (H5O_loc_copy(dst->oloc, src->oloc, depth) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to copy entry");
    if (H5G_name_copy(dst->path, src->path, depth) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to copy path");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G_loc_copy() */

/*-------------------------------------------------------------------------
 * Function:	H5G_loc_reset
 *
 * Purpose:	Reset information for a location
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_loc_reset(H5G_loc_t *loc)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args. */
    assert(loc);

    /* Reset components of the location */
    if (H5O_loc_reset(loc->oloc) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to reset entry");
    if (H5G_name_reset(loc->path) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to reset path");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G_loc_reset() */

/*-------------------------------------------------------------------------
 * Function:	H5G_loc_free
 *
 * Purpose:	Free information for a location
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_loc_free(H5G_loc_t *loc)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args. */
    assert(loc);

    /* Reset components of the location */
    if (H5G_name_free(loc->path) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTRELEASE, FAIL, "unable to free path");
    if (H5O_loc_free(loc->oloc) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTRELEASE, FAIL, "unable to free object header location");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G_loc_free() */

/*-------------------------------------------------------------------------
 * Function:	H5G__loc_find_cb
 *
 * Purpose:	Callback for retrieving object location for an object in a group
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__loc_find_cb(H5G_loc_t H5_ATTR_UNUSED *grp_loc /*in*/, const char *name,
                 const H5O_link_t H5_ATTR_UNUSED *lnk, H5G_loc_t *obj_loc, void *_udata /*in,out*/,
                 H5G_own_loc_t *own_loc /*out*/)
{
    H5G_loc_fnd_t *udata     = (H5G_loc_fnd_t *)_udata; /* User data passed in */
    herr_t         ret_value = SUCCEED;                 /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check if the name in this group resolved to a valid object */
    if (obj_loc == NULL)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "object '%s' doesn't exist", name);

    /* Take ownership of the object's group location */
    /* (Group traversal callbacks are responsible for either taking ownership
     *  of the group location for the object, or freeing it. - QAK)
     */
    H5G_loc_copy(udata->loc, obj_loc, H5_COPY_SHALLOW);
    *own_loc = H5G_OWN_OBJ_LOC;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__loc_find_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5G_loc_find
 *
 * Purpose:	Find a symbol from a location
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_loc_find(const H5G_loc_t *loc, const char *name, H5G_loc_t *obj_loc /*out*/)
{
    H5G_loc_fnd_t udata;               /* User data for traversal callback */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args. */
    assert(loc);
    assert(name && *name);
    assert(obj_loc);

    /* Set up user data for locating object */
    udata.loc = obj_loc;

    /* Traverse group hierarchy to locate object */
    if (H5G_traverse(loc, name, H5G_TARGET_NORMAL, H5G__loc_find_cb, &udata) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "can't find object");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G_loc_find() */

/*-------------------------------------------------------------------------
 * Function:	H5G__loc_find_by_idx_cb
 *
 * Purpose:	Callback for retrieving object location for an object in a group
 *              according to the order within an index
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__loc_find_by_idx_cb(H5G_loc_t H5_ATTR_UNUSED *grp_loc /*in*/, const char H5_ATTR_UNUSED *name,
                        const H5O_link_t H5_ATTR_UNUSED *lnk, H5G_loc_t *obj_loc, void *_udata /*in,out*/,
                        H5G_own_loc_t *own_loc /*out*/)
{
    H5G_loc_fbi_t *udata = (H5G_loc_fbi_t *)_udata; /* User data passed in */
    H5O_link_t     fnd_lnk;                         /* Link within group */
    bool           lnk_copied    = false;           /* Whether the link was copied */
    bool           obj_loc_valid = false;           /* Flag to indicate that the object location is valid */
    bool           obj_exists    = false;           /* Whether the object exists (unused) */
    herr_t         ret_value     = SUCCEED;         /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check if the name in this group resolved to a valid link */
    if (obj_loc == NULL)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "group doesn't exist");

    /* Query link */
    if (H5G_obj_lookup_by_idx(obj_loc->oloc, udata->idx_type, udata->order, udata->n, &fnd_lnk) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "link not found");
    lnk_copied = true;

    /* Build the initial object location for the link */
    if (H5G__link_to_loc(obj_loc, &fnd_lnk, udata->loc) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "cannot initialize object location");
    obj_loc_valid = true;

    /* Perform any special traversals that the link needs */
    /* (soft links, user-defined links, file mounting, etc.) */
    /* (may modify the object location) */
    if (H5G__traverse_special(obj_loc, &fnd_lnk, H5G_TARGET_NORMAL, true, udata->loc, &obj_exists) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_TRAVERSE, FAIL, "special link traversal failed");

done:
    /* Reset the link information, if we have a copy */
    if (lnk_copied)
        H5O_msg_reset(H5O_LINK_ID, &fnd_lnk);

    /* Release the object location if we failed after copying it */
    if (ret_value < 0 && obj_loc_valid)
        if (H5G_loc_free(udata->loc) < 0)
            HDONE_ERROR(H5E_SYM, H5E_CANTRELEASE, FAIL, "can't free location");

    /* Indicate that this callback didn't take ownership of the group *
     * location for the object */
    *own_loc = H5G_OWN_NONE;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__loc_find_by_idx_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5G_loc_find_by_idx
 *
 * Purpose:	Find a symbol from a location, according to the order in an index
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_loc_find_by_idx(const H5G_loc_t *loc, const char *group_name, H5_index_t idx_type, H5_iter_order_t order,
                    hsize_t n, H5G_loc_t *obj_loc /*out*/)
{
    H5G_loc_fbi_t udata;               /* User data for traversal callback */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args. */
    assert(loc);
    assert(group_name && *group_name);
    assert(obj_loc);

    /* Set up user data for locating object */
    udata.idx_type = idx_type;
    udata.order    = order;
    udata.n        = n;
    udata.loc      = obj_loc;

    /* Traverse group hierarchy to locate object */
    if (H5G_traverse(loc, group_name, H5G_TARGET_NORMAL, H5G__loc_find_by_idx_cb, &udata) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "can't find object");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G_loc_find_by_idx() */

/*-------------------------------------------------------------------------
 * Function:	H5G__loc_insert
 *
 * Purpose:	Insert an object at a location
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__loc_insert(H5G_loc_t *grp_loc, char *name, H5G_loc_t *obj_loc, H5O_type_t obj_type, const void *crt_info)
{
    H5O_link_t lnk;                 /* Link for object to insert */
    herr_t     ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args. */
    assert(grp_loc);
    assert(name && *name);
    assert(obj_loc);

    /* Create link object for the object location */
    lnk.type         = H5L_TYPE_HARD;
    lnk.cset         = H5F_DEFAULT_CSET;
    lnk.corder       = 0;     /* Will be reset if the group is tracking creation order */
    lnk.corder_valid = false; /* Indicate that the creation order isn't valid (yet) */
    lnk.name         = name;
    lnk.u.hard.addr  = obj_loc->oloc->addr;

    /* Insert new group into current group's symbol table */
    if (H5G_obj_insert(grp_loc->oloc, name, &lnk, true, obj_type, crt_info) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINSERT, FAIL, "unable to insert object");

    /* Set the name of the object location */
    if (H5G_name_set(grp_loc->path, obj_loc->path, name) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "cannot set name");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__loc_insert() */

/*-------------------------------------------------------------------------
 * Function:	H5G__loc_exists_cb
 *
 * Purpose:	Callback for checking if an object exists
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__loc_exists_cb(H5G_loc_t H5_ATTR_UNUSED *grp_loc /*in*/, const char H5_ATTR_UNUSED *name,
                   const H5O_link_t H5_ATTR_UNUSED *lnk, H5G_loc_t *obj_loc, void *_udata /*in,out*/,
                   H5G_own_loc_t *own_loc /*out*/)
{
    bool  *exists    = (bool *)_udata; /* User data passed in */
    herr_t ret_value = SUCCEED;        /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check if the name in this group resolved to a valid object */
    if (obj_loc == NULL)
        if (lnk)
            *exists = false;
        else
            HGOTO_ERROR(H5E_SYM, H5E_INTERNAL, FAIL, "no object or link info?");
    else
        *exists = true;

    /* Indicate that this callback didn't take ownership of the group *
     * location for the object */
    *own_loc = H5G_OWN_NONE;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__loc_exists_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5G_loc_exists
 *
 * Purpose:	Check if an object actually exists at a location
 *
 * Return:	Success:	true/false
 * 		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_loc_exists(const H5G_loc_t *loc, const char *name, bool *exists)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args. */
    assert(loc);
    assert(name && *name);
    assert(exists);

    /* Traverse group hierarchy to locate object */
    if (H5G_traverse(loc, name, H5G_TARGET_EXISTS, H5G__loc_exists_cb, exists) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "can't check if object exists");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G_loc_exists() */

/*-------------------------------------------------------------------------
 * Function:	H5G__loc_addr_cb
 *
 * Purpose:	Callback for retrieving the address for an object in a group
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__loc_addr_cb(H5G_loc_t H5_ATTR_UNUSED *grp_loc /*in*/, const char H5_ATTR_UNUSED *name,
                 const H5O_link_t H5_ATTR_UNUSED *lnk, H5G_loc_t *obj_loc, void *_udata /*in,out*/,
                 H5G_own_loc_t *own_loc /*out*/)
{
    haddr_t *udata     = (haddr_t *)_udata; /* User data passed in */
    herr_t   ret_value = SUCCEED;           /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check if the name in this group resolved to a valid link */
    if (obj_loc == NULL)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "name doesn't exist");

    /* Set address of object */
    *udata = obj_loc->oloc->addr;

done:
    /* Indicate that this callback didn't take ownership of the group *
     * location for the object */
    *own_loc = H5G_OWN_NONE;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__loc_addr_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5G__loc_addr
 *
 * Purpose:	Retrieve the information for an object from a group location
 *              and path to that object
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__loc_addr(const H5G_loc_t *loc, const char *name, haddr_t *addr /*out*/)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args. */
    assert(loc);
    assert(name && *name);
    assert(addr);

    /* Traverse group hierarchy to locate object */
    if (H5G_traverse(loc, name, H5G_TARGET_NORMAL, H5G__loc_addr_cb, addr) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "can't find object");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__loc_addr() */

/*-------------------------------------------------------------------------
 * Function:	H5G__loc_info_cb
 *
 * Purpose:	Callback for retrieving data model info for an object in a group
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__loc_info_cb(H5G_loc_t H5_ATTR_UNUSED *grp_loc /*in*/, const char H5_ATTR_UNUSED *name,
                 const H5O_link_t H5_ATTR_UNUSED *lnk, H5G_loc_t *obj_loc, void *_udata /*in,out*/,
                 H5G_own_loc_t *own_loc /*out*/)
{
    H5G_loc_info_t *udata     = (H5G_loc_info_t *)_udata; /* User data passed in */
    herr_t          ret_value = SUCCEED;                  /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check if the name in this group resolved to a valid link */
    if (obj_loc == NULL)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "name doesn't exist");

    /* Query object information */
    if (H5O_get_info(obj_loc->oloc, udata->oinfo, udata->fields) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't get object info");

done:
    /* Indicate that this callback didn't take ownership of the group *
     * location for the object */
    *own_loc = H5G_OWN_NONE;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__loc_info_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5G_loc_info
 *
 * Purpose:     Retrieve the data model information for an object from a group location
 *              and path to that object
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_loc_info(const H5G_loc_t *loc, const char *name, H5O_info2_t *oinfo /*out*/, unsigned fields)
{
    H5G_loc_info_t udata;               /* User data for traversal callback */
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args. */
    assert(loc);
    assert(name && *name);
    assert(oinfo);

    /* Set up user data for locating object */
    udata.fields = fields;
    udata.oinfo  = oinfo;

    /* Traverse group hierarchy to locate object */
    if (H5G_traverse(loc, name, H5G_TARGET_NORMAL, H5G__loc_info_cb, &udata) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "can't find object");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G_loc_info() */

/*-------------------------------------------------------------------------
 * Function:	H5G__loc_native_info_cb
 *
 * Purpose:	Callback for retrieving native info for an object in a group
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__loc_native_info_cb(H5G_loc_t H5_ATTR_UNUSED *grp_loc /*in*/, const char H5_ATTR_UNUSED *name,
                        const H5O_link_t H5_ATTR_UNUSED *lnk, H5G_loc_t *obj_loc, void *_udata /*in,out*/,
                        H5G_own_loc_t *own_loc /*out*/)
{
    H5G_loc_native_info_t *udata     = (H5G_loc_native_info_t *)_udata; /* User data passed in */
    herr_t                 ret_value = SUCCEED;                         /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check if the name in this group resolved to a valid link */
    if (obj_loc == NULL)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "name doesn't exist");

    /* Query object information */
    if (H5O_get_native_info(obj_loc->oloc, udata->oinfo, udata->fields) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "can't get object info");

done:
    /* Indicate that this callback didn't take ownership of the group *
     * location for the object */
    *own_loc = H5G_OWN_NONE;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__loc_native_info_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5G_loc_native_info
 *
 * Purpose:	Retrieve the native information for an object from a group location
 *              and path to that object
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_loc_native_info(const H5G_loc_t *loc, const char *name, H5O_native_info_t *oinfo /*out*/, unsigned fields)
{
    H5G_loc_native_info_t udata;               /* User data for traversal callback */
    herr_t                ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args. */
    assert(loc);
    assert(name && *name);
    assert(oinfo);

    /* Set up user data for locating object */
    udata.fields = fields;
    udata.oinfo  = oinfo;

    /* Traverse group hierarchy to locate object */
    if (H5G_traverse(loc, name, H5G_TARGET_NORMAL, H5G__loc_native_info_cb, &udata) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "can't find object");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G_loc_native_info() */

/*-------------------------------------------------------------------------
 * Function:	H5G__loc_set_comment_cb
 *
 * Purpose:	Callback for (re)setting object comment for an object in a group
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__loc_set_comment_cb(H5G_loc_t H5_ATTR_UNUSED *grp_loc /*in*/, const char H5_ATTR_UNUSED *name,
                        const H5O_link_t H5_ATTR_UNUSED *lnk, H5G_loc_t *obj_loc, void *_udata /*in,out*/,
                        H5G_own_loc_t *own_loc /*out*/)
{
    H5G_loc_sc_t *udata   = (H5G_loc_sc_t *)_udata; /* User data passed in */
    H5O_name_t    comment = {0};                    /* Object header "comment" message */
    htri_t        exists;                           /* Whether a "comment" message already exists */
    herr_t        ret_value = SUCCEED;              /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check if the name in this group resolved to a valid link */
    if (obj_loc == NULL)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "name doesn't exist");

    /* Check for existing comment message */
    if ((exists = H5O_msg_exists(obj_loc->oloc, H5O_NAME_ID)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "unable to read object header");

    /* Remove the previous comment message if any */
    if (exists)
        if (H5O_msg_remove(obj_loc->oloc, H5O_NAME_ID, 0, true) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTDELETE, FAIL,
                        "unable to delete existing comment object header message");

    /* Add the new message */
    if (udata->comment && *udata->comment) {
        if (NULL == (comment.s = strdup(udata->comment)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't copy group comment");
        if (H5O_msg_create(obj_loc->oloc, H5O_NAME_ID, 0, H5O_UPDATE_TIME, &comment) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, FAIL, "unable to set comment object header message");
    } /* end if */

done:
    free(comment.s);

    /* Indicate that this callback didn't take ownership of the group *
     * location for the object */
    *own_loc = H5G_OWN_NONE;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__loc_set_comment_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5G_loc_set_comment
 *
 * Purpose:	(Re)set the information for an object from a group location
 *              and path to that object
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_loc_set_comment(const H5G_loc_t *loc, const char *name, const char *comment)
{
    H5G_loc_sc_t udata;               /* User data for traversal callback */
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args. */
    assert(loc);
    assert(name && *name);

    /* Set up user data for locating object */
    udata.comment = comment;

    /* Traverse group hierarchy to locate object */
    if (H5G_traverse(loc, name, H5G_TARGET_NORMAL, H5G__loc_set_comment_cb, &udata) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "can't find object");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G_loc_set_comment() */

/*-------------------------------------------------------------------------
 * Function:	H5G__loc_get_comment_cb
 *
 * Purpose:	Callback for retrieving object comment for an object in a group
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__loc_get_comment_cb(H5G_loc_t H5_ATTR_UNUSED *grp_loc /*in*/, const char H5_ATTR_UNUSED *name,
                        const H5O_link_t H5_ATTR_UNUSED *lnk, H5G_loc_t *obj_loc, void *_udata /*in,out*/,
                        H5G_own_loc_t *own_loc /*out*/)
{
    H5G_loc_gc_t *udata = (H5G_loc_gc_t *)_udata; /* User data passed in */
    H5O_name_t    comment;                        /* Object header "comment" message */
    herr_t        ret_value = SUCCEED;            /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check if the name in this group resolved to a valid link */
    if (obj_loc == NULL)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "name doesn't exist");

    /* Query object comment */
    comment.s = NULL;
    if (NULL == H5O_msg_read(obj_loc->oloc, H5O_NAME_ID, &comment)) {
        if (udata->comment && udata->bufsize > 0)
            udata->comment[0] = '\0';
        udata->comment_size = 0;
    }
    else {
        if (udata->comment && udata->bufsize)
            strncpy(udata->comment, comment.s, udata->bufsize);
        udata->comment_size = strlen(comment.s);
        H5O_msg_reset(H5O_NAME_ID, &comment);
    }

done:
    /* Indicate that this callback didn't take ownership of the group *
     * location for the object.
     */
    *own_loc = H5G_OWN_NONE;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__loc_get_comment_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5G_loc_get_comment
 *
 * Purpose:	Retrieve the information for an object from a group location
 *              and path to that object
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_loc_get_comment(const H5G_loc_t *loc, const char *name, char *comment /*out*/, size_t bufsize,
                    size_t *comment_len)
{
    H5G_loc_gc_t udata;               /* User data for traversal callback */
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args. */
    assert(loc);
    assert(name && *name);

    /* Set up user data for locating object */
    udata.comment      = comment;
    udata.bufsize      = bufsize;
    udata.comment_size = 0;

    /* Traverse group hierarchy to locate object */
    if (H5G_traverse(loc, name, H5G_TARGET_NORMAL, H5G__loc_get_comment_cb, &udata) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "can't find object");

    /* Set value to return */
    if (comment_len)
        *comment_len = udata.comment_size;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G_loc_get_comment() */
