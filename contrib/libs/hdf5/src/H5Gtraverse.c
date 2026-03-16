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
 * Created:		H5Gtraverse.c
 *
 * Purpose:		Functions for traversing group hierarchy
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
#include "H5Dprivate.h"  /* Datasets                                 */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5Fprivate.h"  /* File access                              */
#include "H5Gpkg.h"      /* Groups                                   */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5Lprivate.h"  /* Links                                    */
#include "H5MMprivate.h" /* Memory management                        */
#include "H5Ppublic.h"   /* Property Lists                           */
#include "H5WBprivate.h" /* Wrapped Buffers                          */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/* User data for path traversal routine */
typedef struct {
    /* down */
    bool chk_exists; /* Flag to indicate we are checking if object exists */

    /* up */
    H5G_loc_t *obj_loc; /* Object location */
    bool       exists;  /* Indicate if object exists */
} H5G_trav_slink_t;

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/
static herr_t H5G__traverse_slink_cb(H5G_loc_t *grp_loc, const char *name, const H5O_link_t *lnk,
                                     H5G_loc_t *obj_loc, void *_udata /*in,out*/,
                                     H5G_own_loc_t *own_loc /*out*/);
static herr_t H5G__traverse_ud(const H5G_loc_t *grp_loc, const H5O_link_t *lnk, H5G_loc_t *obj_loc /*in,out*/,
                               unsigned target, bool *obj_exists);
static herr_t H5G__traverse_slink(const H5G_loc_t *grp_loc, const H5O_link_t *lnk,
                                  H5G_loc_t *obj_loc /*in,out*/, unsigned target, bool *obj_exists);
static herr_t H5G__traverse_real(const H5G_loc_t *loc, const char *name, unsigned target, H5G_traverse_t op,
                                 void *op_data);

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
 * Function:	H5G__traverse_slink_cb
 *
 * Purpose:	Callback for soft link traversal.  This routine sets the
 *              correct information for the object location.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__traverse_slink_cb(H5G_loc_t H5_ATTR_UNUSED *grp_loc, const char H5_ATTR_UNUSED *name,
                       const H5O_link_t H5_ATTR_UNUSED *lnk, H5G_loc_t *obj_loc, void *_udata /*in,out*/,
                       H5G_own_loc_t *own_loc /*out*/)
{
    H5G_trav_slink_t *udata     = (H5G_trav_slink_t *)_udata; /* User data passed in */
    herr_t            ret_value = SUCCEED;                    /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check for dangling soft link */
    if (obj_loc == NULL) {
        if (udata->chk_exists)
            udata->exists = false;
        else
            HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "component not found");
    } /* end if */
    else {
        /* Copy new location information for resolved object */
        H5O_loc_copy_deep(udata->obj_loc->oloc, obj_loc->oloc);

        /* Indicate that the object exists */
        udata->exists = true;
    } /* end else */

done:
    /* Indicate that this callback didn't take ownership of the group *
     * location for the object */
    *own_loc = H5G_OWN_NONE;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__traverse_slink_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5G__traverse_ud
 *
 * Purpose:	Callback for user-defined link traversal.  Sets up a
 *              location ID and passes it to the user traversal callback.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__traverse_ud(const H5G_loc_t *grp_loc /*in,out*/, const H5O_link_t *lnk, H5G_loc_t *obj_loc /*in,out*/,
                 unsigned target, bool *obj_exists)
{
    const H5L_class_t *link_class;     /* User-defined link class */
    hid_t              cb_return = -1; /* The ID the user-defined callback returned */
    H5G_loc_t          grp_loc_copy;
    H5G_name_t         grp_path_copy;
    H5O_loc_t          grp_oloc_copy;
    H5G_loc_t          new_loc; /* Group location for newly opened external object */
    H5G_t             *grp;
    hid_t              cur_grp   = (-1);
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(grp_loc);
    assert(lnk);
    assert(lnk->type >= H5L_TYPE_UD_MIN);
    assert(obj_loc);

    /* Get the link class for this type of link. */
    if (NULL == (link_class = H5L_find_class(lnk->type)))
        HGOTO_ERROR(H5E_SYM, H5E_NOTREGISTERED, FAIL, "unable to get UD link class");

    /* Set up location for user-defined callback.  Use a copy of our current
     * grp_loc. */
    grp_loc_copy.path = &grp_path_copy;
    grp_loc_copy.oloc = &grp_oloc_copy;
    H5G_loc_reset(&grp_loc_copy);
    if (H5G_loc_copy(&grp_loc_copy, grp_loc, H5_COPY_DEEP) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTCOPY, FAIL, "unable to copy object location");

    /* Create a group ID to pass to the user-defined callback */
    if (NULL == (grp = H5G_open(&grp_loc_copy)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to open group");
    if ((cur_grp = H5VL_wrap_register(H5I_GROUP, grp, false)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTREGISTER, FAIL, "unable to register group");

        /* User-defined callback function */
#ifndef H5_NO_DEPRECATED_SYMBOLS
    /* (Backwardly compatible with v0 H5L_class_t traversal callback) */
    if (link_class->version == H5L_LINK_CLASS_T_VERS_0)
        cb_return = (((const H5L_class_0_t *)link_class)->trav_func)(lnk->name, cur_grp, lnk->u.ud.udata,
                                                                     lnk->u.ud.size, H5CX_get_lapl());
    else
        cb_return = (link_class->trav_func)(lnk->name, cur_grp, lnk->u.ud.udata, lnk->u.ud.size,
                                            H5CX_get_lapl(), H5CX_get_dxpl());
#else  /* H5_NO_DEPRECATED_SYMBOLS */
    cb_return = (link_class->trav_func)(lnk->name, cur_grp, lnk->u.ud.udata, lnk->u.ud.size, H5CX_get_lapl(),
                                        H5CX_get_dxpl());
#endif /* H5_NO_DEPRECATED_SYMBOLS */

    /* Check for failing to locate the object */
    if (cb_return < 0) {
        /* Check if we just needed to know if the object exists */
        if (target & H5G_TARGET_EXISTS) {
            /* Clear any errors from the stack */
            H5E_clear_stack(NULL);

            /* Indicate that the object doesn't exist */
            *obj_exists = false;

            /* Get out now */
            HGOTO_DONE(SUCCEED);
        } /* end if */
        /* else, we really needed to open the object */
        else
            HGOTO_ERROR(H5E_SYM, H5E_BADID, FAIL, "traversal callback returned invalid ID");
    } /* end if */

    /* Get the object location information from the ID the user callback returned */
    if (H5G_loc(cb_return, &new_loc) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "unable to get object location from ID");

    /* Release any previous location information for the object */
    H5G_loc_free(obj_loc);

    /* Copy new object's location information */
    H5G_loc_copy(obj_loc, &new_loc, H5_COPY_DEEP);

    /* Hold the file open until we free this object header (otherwise the
     * object location will be invalidated when the file closes).
     */
    if (H5O_loc_hold_file(obj_loc->oloc) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to hold file open");

    /* We have a copy of the location and we're holding the file open.
     * Close the open ID the user passed back.
     */
    if (H5I_dec_ref(cb_return) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTRELEASE, FAIL, "unable to close ID from UD callback");
    cb_return = (hid_t)(-1);

done:
    /* Close location given to callback. */
    if (cur_grp > 0 && H5I_dec_ref(cur_grp) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTRELEASE, FAIL, "unable to close ID for current location");

    if (ret_value < 0 && cb_return > 0 && H5I_dec_ref(cb_return) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTRELEASE, FAIL, "unable to close ID from UD callback");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__traverse_ud() */

/*-------------------------------------------------------------------------
 * Function:	H5G__traverse_slink
 *
 * Purpose:	Traverses symbolic link.  The link head appears in the group
 *		whose entry is GRP_LOC and the link tail entry is OBJ_LOC.
 *
 * Return:	Success:	Non-negative, OBJ_LOC will contain information
 *				about the object to which the link points
 *
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__traverse_slink(const H5G_loc_t *grp_loc, const H5O_link_t *lnk, H5G_loc_t *obj_loc /*in,out*/,
                    unsigned target, bool *obj_exists)
{
    H5G_trav_slink_t udata;                     /* User data to pass to link traversal callback */
    H5G_name_t       tmp_obj_path;              /* Temporary copy of object's path */
    bool             tmp_obj_path_set = false;  /* Flag to indicate that tmp object path is initialized */
    H5O_loc_t        tmp_grp_oloc;              /* Temporary copy of group entry */
    H5G_name_t       tmp_grp_path;              /* Temporary copy of group's path */
    H5G_loc_t        tmp_grp_loc;               /* Temporary copy of group's location */
    bool             tmp_grp_loc_set = false;   /* Flag to indicate that tmp group location is initialized */
    herr_t           ret_value       = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(grp_loc);
    assert(lnk);
    assert(lnk->type == H5L_TYPE_SOFT);

    /* Set up temporary location */
    tmp_grp_loc.oloc = &tmp_grp_oloc;
    tmp_grp_loc.path = &tmp_grp_path;

    /* Portably initialize the temporary objects */
    H5G_loc_reset(&tmp_grp_loc);
    H5G_name_reset(&tmp_obj_path);

    /* Clone the group location, so we can track the names properly */
    /* ("tracking the names properly" means to ignore the effects of the
     *  link traversal on the object's & group's paths - QAK)
     */
    H5G_loc_copy(&tmp_grp_loc, grp_loc, H5_COPY_DEEP);
    tmp_grp_loc_set = true;

    /* Hold the object's group hier. path to restore later */
    /* (Part of "tracking the names properly") */
    H5G_name_copy(&tmp_obj_path, obj_loc->path, H5_COPY_SHALLOW);
    tmp_obj_path_set = true;

    /* Set up user data for traversal callback */
    udata.chk_exists = (target & H5G_TARGET_EXISTS) ? true : false;
    udata.exists     = false;
    udata.obj_loc    = obj_loc;

    /* Traverse the link */
    if (H5G__traverse_real(&tmp_grp_loc, lnk->u.soft.name, target, H5G__traverse_slink_cb, &udata) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "unable to follow symbolic link");

    /* Pass back information about whether the object exists */
    *obj_exists = udata.exists;

done:
    /* Restore object's group hier. path */
    if (tmp_obj_path_set) {
        H5G_name_free(obj_loc->path);
        H5G_name_copy(obj_loc->path, &tmp_obj_path, H5_COPY_SHALLOW);
    } /* end if */

    /* Release cloned copy of group location */
    if (tmp_grp_loc_set)
        H5G_loc_free(&tmp_grp_loc);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__traverse_slink() */

/*-------------------------------------------------------------------------
 * Function:	H5G__traverse_special
 *
 * Purpose:	Handle traversing special link situations
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__traverse_special(const H5G_loc_t *grp_loc, const H5O_link_t *lnk, unsigned target, bool last_comp,
                      H5G_loc_t *obj_loc, bool *obj_exists)
{
    size_t nlinks;              /* # of soft / UD links left to traverse */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(grp_loc);
    assert(lnk);
    assert(obj_loc);

    /* If we found a symbolic link then we should follow it.  But if this
     * is the last component of the name and the H5G_TARGET_SLINK bit of
     * TARGET is set then we don't follow it.
     */
    if (H5L_TYPE_SOFT == lnk->type && (0 == (target & H5G_TARGET_SLINK) || !last_comp)) {

        /* Get the # of soft / UD links left to traverse */
        if (H5CX_get_nlinks(&nlinks) < 0)
            HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "unable to retrieve # of soft / UD links to traverse");

        /* Decrement # of links and range check */
        if ((nlinks)-- <= 0)
            HGOTO_ERROR(H5E_LINK, H5E_NLINKS, FAIL, "too many links");

        /* Update the # of links in the API context */
        if (H5CX_set_nlinks(nlinks) < 0)
            HGOTO_ERROR(H5E_LINK, H5E_CANTSET, FAIL, "can't update # of soft / UD links to traverse");

        /* Traverse soft link */
        if (H5G__traverse_slink(grp_loc, lnk, obj_loc, (target & H5G_TARGET_EXISTS), obj_exists) < 0)
            HGOTO_ERROR(H5E_LINK, H5E_TRAVERSE, FAIL, "symbolic link traversal failed");
    } /* end if */

    /*
     * If we found a user-defined link then we should follow it.  But if this
     * is the last component of the name and the H5G_TARGET_UDLINK bit of
     * TARGET is set then we don't follow it.
     */
    if (lnk->type >= H5L_TYPE_UD_MIN && (0 == (target & H5G_TARGET_UDLINK) || !last_comp)) {

        /* Get the # of soft / UD links left to traverse */
        if (H5CX_get_nlinks(&nlinks) < 0)
            HGOTO_ERROR(H5E_LINK, H5E_CANTGET, FAIL, "unable to retrieve # of soft / UD links to traverse");

        /* Decrement # of links and range check */
        if ((nlinks)-- <= 0)
            HGOTO_ERROR(H5E_LINK, H5E_NLINKS, FAIL, "too many links");

        /* Update the # of links in the API context */
        if (H5CX_set_nlinks(nlinks) < 0)
            HGOTO_ERROR(H5E_LINK, H5E_CANTSET, FAIL, "can't update # of soft / UD links to traverse");

        /* Traverse user-defined link */
        if (H5G__traverse_ud(grp_loc, lnk, obj_loc, (target & H5G_TARGET_EXISTS), obj_exists) < 0)
            HGOTO_ERROR(H5E_LINK, H5E_TRAVERSE, FAIL, "user-defined link traversal failed");
    } /* end if */

    /*
     * Resolve mount points to the mounted group.  Do not do this step if
     * the H5G_TARGET_MOUNT bit of TARGET is set and this is the last
     * component of the name.
     *
     * (If this link is a hard link, try to perform mount point traversal)
     *
     * (Note that the soft and external link traversal above can change
     *  the status of the object (into a hard link), so don't use an 'else'
     *  statement here. -QAK)
     */
    if (H5_addr_defined(obj_loc->oloc->addr) && (0 == (target & H5G_TARGET_MOUNT) || !last_comp)) {
        if (H5F_traverse_mount(obj_loc->oloc /*in,out*/) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "mount point traversal failed");
    } /* end if */

    /* If the grp_loc is the only thing holding an external file open
     * and obj_loc is in the same file, obj_loc should also hold the
     * file open so that closing the grp_loc doesn't close the file.
     */
    if (grp_loc->oloc->holding_file && grp_loc->oloc->file == obj_loc->oloc->file)
        if (H5O_loc_hold_file(obj_loc->oloc) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to hold file open");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__traverse_special() */

/*-------------------------------------------------------------------------
 * Function:	H5G__traverse_real
 *
 * Purpose:	Internal version of path traversal routine
 *
 * Return:	Success:	Non-negative if name can be fully resolved.
 *
 *		Failure:	Negative if the name could not be fully
 *				resolved.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__traverse_real(const H5G_loc_t *_loc, const char *name, unsigned target, H5G_traverse_t op, void *op_data)
{
    H5G_loc_t     loc;                    /* Location of start object     */
    H5O_loc_t     grp_oloc;               /* Object loc. for current group */
    H5G_name_t    grp_path;               /* Path for current group	*/
    H5G_loc_t     grp_loc;                /* Location of group            */
    H5O_loc_t     obj_oloc;               /* Object found			*/
    H5G_name_t    obj_path;               /* Path for object found	*/
    H5G_loc_t     obj_loc;                /* Location of object           */
    size_t        nchars;                 /* component name length	*/
    H5O_link_t    lnk;                    /* Link information for object  */
    bool          link_valid    = false;  /* Flag to indicate that the link information is valid */
    bool          obj_loc_valid = false;  /* Flag to indicate that the object location is valid */
    H5G_own_loc_t own_loc = H5G_OWN_NONE; /* Enum to indicate whether callback took ownership of locations*/
    bool          group_copy = false;     /* Flag to indicate that the group entry is copied */
    char          comp_buf[1024];         /* Temporary buffer for path components */
    char         *comp;                   /* Pointer to buffer for path components */
    H5WB_t       *wb        = NULL;       /* Wrapped buffer for temporary buffer */
    bool          last_comp = false; /* Flag to indicate that a component is the last component in the name */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check parameters */
    assert(_loc);
    assert(name);
    assert(op);

    /*
     * Where does the searching start?  For absolute names it starts at the
     * root of the file; for relative names it starts at CWG.
     */
    /* Check if we need to get the root group's entry */
    if ('/' == *name) {
        H5G_t *root_grp; /* Temporary pointer to root group of file */

        /* Look up root group for starting location */
        root_grp = H5G_rootof(_loc->oloc->file);
        assert(root_grp);

        /* Set the location entry to the root group's info */
        loc.oloc = &(root_grp->oloc);
        loc.path = &(root_grp->path);
    } /* end if */
    else {
        loc.oloc = _loc->oloc;
        loc.path = _loc->path;
    } /* end else */

    /* Set up group & object locations */
    grp_loc.oloc = &grp_oloc;
    grp_loc.path = &grp_path;
    obj_loc.oloc = &obj_oloc;
    obj_loc.path = &obj_path;

#if defined(H5_USING_MEMCHECKER) || !defined(NDEBUG)
    /* Clear group location */
    if (H5G_loc_reset(&grp_loc) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to reset location");
#endif /* H5_USING_MEMCHECKER */

    /* Deep copy of the starting location to group location */
    if (H5G_loc_copy(&grp_loc, &loc, H5_COPY_DEEP) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to copy location");
    group_copy = true;

    /* Clear object location */
    if (H5G_loc_reset(&obj_loc) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to reset location");

    /* Wrap the local buffer for serialized header info */
    if (NULL == (wb = H5WB_wrap(comp_buf, sizeof(comp_buf))))
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't wrap buffer");

    /* Get a pointer to a buffer that's large enough  */
    if (NULL == (comp = (char *)H5WB_actual(wb, (strlen(name) + 1))))
        HGOTO_ERROR(H5E_SYM, H5E_NOSPACE, FAIL, "can't get actual buffer");

    /* Traverse the path */
    while ((name = H5G__component(name, &nchars)) && *name) {
        const char *s;             /* Temporary string pointer */
        bool        lookup_status; /* Status from object lookup */
        bool        obj_exists;    /* Whether the object exists */

        /*
         * Copy the component name into a null-terminated buffer so
         * we can pass it down to the other symbol table functions.
         */
        H5MM_memcpy(comp, name, nchars);
        comp[nchars] = '\0';

        /*
         * The special name `.' is a no-op.
         */
        if ('.' == comp[0] && !comp[1]) {
            name += nchars;
            continue;
        } /* end if */

        /* Check if this is the last component of the name */
        if (!((s = H5G__component(name + nchars, NULL)) && *s))
            last_comp = true;

        /* If there's valid information in the link, reset it */
        if (link_valid) {
            H5O_msg_reset(H5O_LINK_ID, &lnk);
            link_valid = false;
        } /* end if */

        /* Get information for object in current group */
        lookup_status = false;
        if (H5G__obj_lookup(grp_loc.oloc, comp, &lookup_status, &lnk /*out*/) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "can't look up component");
        obj_exists = false;

        /* If the lookup was OK, build object location and traverse special links, etc. */
        if (lookup_status) {
            /* Sanity check link and indicate it's valid */
            assert(lnk.type >= H5L_TYPE_HARD);
            assert(!strcmp(comp, lnk.name));
            link_valid = true;

            /* Build object location from the link */
            if (H5G__link_to_loc(&grp_loc, &lnk, &obj_loc) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "cannot initialize object location");
            obj_loc_valid = true;

            /* Assume object exists */
            obj_exists = true;

            /* Perform any special traversals that the link needs */
            /* (soft links, user-defined links, file mounting, etc.) */
            if (H5G__traverse_special(&grp_loc, &lnk, target, last_comp, &obj_loc, &obj_exists) < 0)
                HGOTO_ERROR(H5E_LINK, H5E_TRAVERSE, FAIL, "special link traversal failed");
        } /* end if */

        /* Check for last component in name provided */
        if (last_comp) {
            H5O_link_t *cb_lnk; /* Pointer to link info for callback */
            H5G_loc_t  *cb_loc; /* Pointer to object location for callback */

            /* Set callback parameters appropriately, based on link being found */
            if (lookup_status) {
                cb_lnk = &lnk;
                if (obj_exists)
                    cb_loc = &obj_loc;
                else
                    cb_loc = NULL;
            } /* end if */
            else {
                assert(!obj_loc_valid);
                cb_lnk = NULL;
                cb_loc = NULL;
            } /* end else */

            /* Call 'operator' routine */
            if ((op)(&grp_loc, comp, cb_lnk, cb_loc, op_data, &own_loc) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_CALLBACK, FAIL, "traversal operator failed");

            HGOTO_DONE(SUCCEED);
        } /* end if */

        /* Handle lookup failures now */
        if (!lookup_status) {
            /* If an intermediate group doesn't exist & flag is set, create the group */
            if (target & H5G_CRT_INTMD_GROUP) {
                const H5O_ginfo_t  def_ginfo = H5G_CRT_GROUP_INFO_DEF; /* Default group info settings */
                const H5O_linfo_t  def_linfo = H5G_CRT_LINK_INFO_DEF;  /* Default link info settings */
                const H5O_pline_t  def_pline = H5O_CRT_PIPELINE_DEF;   /* Default filter pipeline settings */
                H5O_ginfo_t        par_ginfo; /* Group info settings for parent group */
                H5O_linfo_t        par_linfo; /* Link info settings for parent group */
                H5O_pline_t        par_pline; /* Filter pipeline settings for parent group */
                H5O_linfo_t        tmp_linfo; /* Temporary link info settings */
                htri_t             exists;    /* Whether a group or link info message exists */
                const H5O_ginfo_t *ginfo;     /* Group info settings for new group */
                const H5O_linfo_t *linfo;     /* Link info settings for new group */
                const H5O_pline_t *pline;     /* Filter pipeline settings for new group */
                H5G_obj_create_t   gcrt_info; /* Group creation info */

                /* Check for the parent group having a group info message */
                /* (OK if not found) */
                if ((exists = H5O_msg_exists(grp_loc.oloc, H5O_GINFO_ID)) < 0)
                    HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "unable to read object header");
                if (exists) {
                    /* Get the group info for parent group */
                    if (NULL == H5O_msg_read(grp_loc.oloc, H5O_GINFO_ID, &par_ginfo))
                        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "group info message not present");

                    /* Use parent group info settings */
                    ginfo = &par_ginfo;
                } /* end if */
                else
                    /* Use default group info settings */
                    ginfo = &def_ginfo;

                /* Check for the parent group having a link info message */
                /* (OK if not found) */
                /* Get the link info for parent group */
                if ((exists = H5G__obj_get_linfo(grp_loc.oloc, &par_linfo)) < 0)
                    HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "unable to read object header");
                if (exists) {
                    /* Only keep the creation order information from the parent
                     *  group's link info
                     */
                    H5MM_memcpy(&tmp_linfo, &def_linfo, sizeof(H5O_linfo_t));
                    tmp_linfo.track_corder = par_linfo.track_corder;
                    tmp_linfo.index_corder = par_linfo.index_corder;
                    linfo                  = &tmp_linfo;
                } /* end if */
                else
                    /* Use default link info settings */
                    linfo = &def_linfo;

                /* Check for the parent group having a filter pipeline message */
                /* (OK if not found) */
                if ((exists = H5O_msg_exists(grp_loc.oloc, H5O_PLINE_ID)) < 0)
                    HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "unable to read object header");
                if (exists) {
                    /* Get the filter pipeline for parent group */
                    if (NULL == H5O_msg_read(grp_loc.oloc, H5O_PLINE_ID, &par_pline))
                        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "filter pipeline message not present");

                    /* Use parent filter pipeline settings */
                    pline = &par_pline;
                } /* end if */
                else
                    /* Use default filter pipeline settings */
                    pline = &def_pline;

                /* Create the intermediate group */
                /* XXX: Should we allow user to control the group creation params here? -QAK */
                gcrt_info.gcpl_id    = H5P_GROUP_CREATE_DEFAULT;
                gcrt_info.cache_type = H5G_NOTHING_CACHED;
                memset(&gcrt_info.cache, 0, sizeof(gcrt_info.cache));
                if (H5G__obj_create_real(grp_oloc.file, ginfo, linfo, pline, &gcrt_info,
                                         obj_loc.oloc /*out*/) < 0)
                    HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to create group entry");

                /* Insert new group into current group's symbol table */
                if (H5G__loc_insert(&grp_loc, comp, &obj_loc, H5O_TYPE_GROUP, &gcrt_info) < 0)
                    HGOTO_ERROR(H5E_SYM, H5E_CANTINSERT, FAIL, "unable to insert intermediate group");

                /* Decrement refcount on intermediate group's object header in memory */
                if (H5O_dec_rc_by_loc(obj_loc.oloc) < 0)
                    HGOTO_ERROR(H5E_SYM, H5E_CANTDEC, FAIL,
                                "unable to decrement refcount on newly created object");

                /* Close new group */
                if (H5O_close(obj_loc.oloc, NULL) < 0)
                    HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to close");

                /* If the parent group was holding the file open, the
                 * newly-created group should, as well.
                 */
                if (grp_loc.oloc->holding_file)
                    if (H5O_loc_hold_file(obj_loc.oloc) < 0)
                        HGOTO_ERROR(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to hold file open");

                /* Reset any non-default object header messages */
                H5_GCC_CLANG_DIAG_OFF("cast-qual")
                if (ginfo != &def_ginfo)
                    if (H5O_msg_reset(H5O_GINFO_ID, (void *)ginfo) < 0)
                        HGOTO_ERROR(H5E_SYM, H5E_CANTRELEASE, FAIL, "unable to reset group info message");
                if (linfo != &def_linfo)
                    if (H5O_msg_reset(H5O_LINFO_ID, (void *)linfo) < 0)
                        HGOTO_ERROR(H5E_SYM, H5E_CANTRELEASE, FAIL, "unable to reset link info message");
                if (pline != &def_pline)
                    if (H5O_msg_reset(H5O_PLINE_ID, (void *)pline) < 0)
                        HGOTO_ERROR(H5E_SYM, H5E_CANTRELEASE, FAIL, "unable to reset I/O pipeline message");
                H5_GCC_CLANG_DIAG_ON("cast-qual")
            } /* end if */
            else
                HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "component not found");
        } /* end if */

        /*
         * Advance to the next component of the path.
         */

        /* Transfer "ownership" of the object's information to the group object */
        H5G_loc_free(&grp_loc);
        H5G_loc_copy(&grp_loc, &obj_loc, H5_COPY_SHALLOW);
        H5G_loc_reset(&obj_loc);
        obj_loc_valid = false;

        /* Advance to next component in string */
        name += nchars;
    } /* end while */

    /* Call 'operator' routine */
    /* If we've fallen through to here, the name must be something like just '.'
     * and we should issue the callback on that. -QAK
     * Since we don't have a group location or a link to the object we pass in
     * NULL.
     */
    assert(group_copy);
    if ((op)(NULL, ".", NULL, &grp_loc, op_data, &own_loc) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTNEXT, FAIL, "traversal operator failed");

    /* If the callback took ownership of the object location, it actually has
     * ownership of grp_loc.  It shouldn't have tried to take ownership of
     * the "group location", which was NULL. */
    assert(!(own_loc & H5G_OWN_GRP_LOC));
    if (own_loc & H5G_OWN_OBJ_LOC)
        own_loc |= H5G_OWN_GRP_LOC;

done:
    /* If there's been an error, the callback doesn't really get ownership of
     * any location and we should close them both */
    if (ret_value < 0)
        own_loc = H5G_OWN_NONE;

    /* Free all open locations.  This also closes any open external files. */
    if (obj_loc_valid && !(own_loc & H5G_OWN_OBJ_LOC))
        H5G_loc_free(&obj_loc);
    if (group_copy && !(own_loc & H5G_OWN_GRP_LOC))
        H5G_loc_free(&grp_loc);

    /* If there's valid information in the link, reset it */
    if (link_valid)
        if (H5O_msg_reset(H5O_LINK_ID, &lnk) < 0)
            HDONE_ERROR(H5E_SYM, H5E_CANTRELEASE, FAIL, "unable to reset link message");

    /* Release temporary component buffer */
    if (wb && H5WB_unwrap(wb) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTRELEASE, FAIL, "can't release wrapped buffer");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__traverse_real() */

/*-------------------------------------------------------------------------
 * Function:	H5G_traverse
 *
 * Purpose:	Traverse a path from a location & perform an operation when
 *              the last component of the name is reached.
 *
 * Return:	Success:	Non-negative if path can be fully traversed.
 *		Failure:	Negative if the path could not be fully
 *				traversed.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_traverse(const H5G_loc_t *loc, const char *name, unsigned target, H5G_traverse_t op, void *op_data)
{
    size_t orig_nlinks;         /* Original value for # of soft / UD links able to traverse */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args */
    if (!name || !*name)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "no name given");
    if (!loc)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "no starting location");
    if (!op)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "no operation provided");

    /* Retrieve the original # of soft / UD links that are able to be traversed
     * (So that multiple calls to H5G_traverse don't incorrectly look
     *  like they've traversed too many.  Nested calls, like in H5L__move(),
     *  may need their own mechanism to set & reset the # of links to traverse)
     */
    if (H5CX_get_nlinks(&orig_nlinks) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "unable to retrieve # of soft / UD links to traverse");

    /* Set up invalid tag. This is a precautionary step only. Setting an invalid
     * tag here will ensure that no metadata accessed while doing the traversal
     * is given an improper tag, unless another one is specifically set up
     * first. This will ensure we're not accidentally tagging something we
     * shouldn't be during the traversal. Note that for best tagging assertion
     * coverage, setting H5C_DO_TAGGING_SANITY_CHECKS is advised.
     */
    H5_BEGIN_TAG(H5AC__INVALID_TAG)

    /* Go perform "real" traversal */
    if (H5G__traverse_real(loc, name, target, op, op_data) < 0)
        HGOTO_ERROR_TAG(H5E_SYM, H5E_NOTFOUND, FAIL, "internal path traversal failed");

    /* Reset tag after traversal */
    H5_END_TAG

    /* Reset the # of soft / UD links that can be traversed */
    if (H5CX_set_nlinks(orig_nlinks) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, FAIL, "can't reset # of soft / UD links to traverse");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G_traverse() */
