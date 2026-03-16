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
 * Created:		H5Gcompact.c
 *
 * Purpose:		Functions for handling compact storage.
 *
 *-------------------------------------------------------------------------
 */
#include "H5Gmodule.h" /* This source code file is part of the H5G module */

/* Packages needed by this file... */
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Gpkg.h"      /* Groups		  		*/
#include "H5MMprivate.h" /* Memory management			*/

/* Private typedefs */

/* User data for link message iteration when building link table */
typedef struct {
    H5G_link_table_t *ltable;   /* Pointer to link table to build */
    size_t            curr_lnk; /* Current link to operate on */
} H5G_iter_bt_t;

/* User data for deleting a link in the link messages */
typedef struct {
    /* downward */
    H5F_t      *file;            /* File that object header is located within */
    H5RS_str_t *grp_full_path_r; /* Full path for group of link */
    const char *name;            /* Link name to search for */
} H5G_iter_rm_t;

/* User data for link message iteration when querying link info */
typedef struct {
    /* downward */
    const char *name; /* Name to search for */

    /* upward */
    H5O_link_t *lnk;   /* Link struct to fill in */
    bool       *found; /* Pointer to flag to indicate that the object was found */
} H5G_iter_lkp_t;

/* Private macros */

/* PRIVATE PROTOTYPES */
static herr_t H5G__compact_build_table_cb(const void *_mesg, unsigned idx, void *_udata);
static herr_t H5G__compact_build_table(const H5O_loc_t *oloc, const H5O_linfo_t *linfo, H5_index_t idx_type,
                                       H5_iter_order_t order, H5G_link_table_t *ltable);
static herr_t H5G__compact_lookup_cb(const void *_mesg, unsigned H5_ATTR_UNUSED idx, void *_udata);

/*-------------------------------------------------------------------------
 * Function:    H5G__compact_build_table_cb
 *
 * Purpose:     Callback routine for searching 'link' messages for a particular
 *              name.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__compact_build_table_cb(const void *_mesg, unsigned H5_ATTR_UNUSED idx, void *_udata)
{
    const H5O_link_t *lnk       = (const H5O_link_t *)_mesg; /* Pointer to link */
    H5G_iter_bt_t    *udata     = (H5G_iter_bt_t *)_udata;   /* 'User data' passed in */
    herr_t            ret_value = H5_ITER_CONT;              /* Return value */

    FUNC_ENTER_PACKAGE

    /* check arguments */
    assert(lnk);
    assert(udata);
    assert(udata->curr_lnk < udata->ltable->nlinks);

    /* Copy link message into table */
    if (NULL == H5O_msg_copy(H5O_LINK_ID, lnk, &(udata->ltable->lnks[udata->curr_lnk])))
        HGOTO_ERROR(H5E_SYM, H5E_CANTCOPY, H5_ITER_ERROR, "can't copy link message");

    /* Increment current link entry to operate on */
    udata->curr_lnk++;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__compact_build_table_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5G__compact_build_table
 *
 * Purpose:     Builds a table containing a sorted (alphabetically) list of
 *              links for a group
 *
 * Return:	Success:        Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__compact_build_table(const H5O_loc_t *oloc, const H5O_linfo_t *linfo, H5_index_t idx_type,
                         H5_iter_order_t order, H5G_link_table_t *ltable)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(oloc);
    assert(linfo);
    assert(ltable);

    /* Set size of table */
    H5_CHECK_OVERFLOW(linfo->nlinks, hsize_t, size_t);
    ltable->nlinks = (size_t)linfo->nlinks;

    /* Allocate space for the table entries */
    if (ltable->nlinks > 0) {
        H5G_iter_bt_t       udata; /* User data for iteration callback */
        H5O_mesg_operator_t op;    /* Message operator */

        /* Allocate the link table */
        if ((ltable->lnks = (H5O_link_t *)H5MM_calloc(sizeof(H5O_link_t) * ltable->nlinks)) == NULL)
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");

        /* Set up user data for iteration */
        udata.ltable   = ltable;
        udata.curr_lnk = 0;

        /* Iterate through the link messages, adding them to the table */
        op.op_type  = H5O_MESG_OP_APP;
        op.u.app_op = H5G__compact_build_table_cb;
        if (H5O_msg_iterate(oloc, H5O_LINK_ID, &op, &udata) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "error iterating over link messages");

        /* Sort link table in correct iteration order */
        if (H5G__link_sort_table(ltable, idx_type, order) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTSORT, FAIL, "error sorting link messages");
    } /* end if */
    else
        ltable->lnks = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__compact_build_table() */

/*-------------------------------------------------------------------------
 * Function:	H5G__compact_insert
 *
 * Purpose:	Insert a new symbol into the table described by GRP_ENT in
 *		file F.	 The name of the new symbol is NAME and its symbol
 *		table entry is OBJ_ENT.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__compact_insert(const H5O_loc_t *grp_oloc, H5O_link_t *obj_lnk)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check arguments */
    assert(grp_oloc && grp_oloc->file);
    assert(obj_lnk);

    /* Insert link message into group */
    if (H5O_msg_create(grp_oloc, H5O_LINK_ID, 0, H5O_UPDATE_TIME, obj_lnk) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__compact_insert() */

/*-------------------------------------------------------------------------
 * Function:	H5G__compact_get_name_by_idx
 *
 * Purpose:     Returns the name of objects in the group by giving index.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__compact_get_name_by_idx(const H5O_loc_t *oloc, const H5O_linfo_t *linfo, H5_index_t idx_type,
                             H5_iter_order_t order, hsize_t idx, char *name, size_t name_size,
                             size_t *name_len)
{
    H5G_link_table_t ltable    = {0, NULL}; /* Link table */
    herr_t           ret_value = SUCCEED;   /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(oloc);

    /* Build table of all link messages */
    if (H5G__compact_build_table(oloc, linfo, idx_type, order, &ltable) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create link message table");

    /* Check for going out of bounds */
    if (idx >= ltable.nlinks)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "index out of bound");

    /* Get the length of the name */
    *name_len = strlen(ltable.lnks[idx].name);

    /* Copy the name into the user's buffer, if given */
    if (name) {
        strncpy(name, ltable.lnks[idx].name, MIN((*name_len + 1), name_size));
        if (*name_len >= name_size)
            name[name_size - 1] = '\0';
    } /* end if */

done:
    /* Release link table */
    if (ltable.lnks && H5G__link_release_table(&ltable) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTFREE, FAIL, "unable to release link table");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__compact_get_name_by_idx() */

/*-------------------------------------------------------------------------
 * Function:	H5G__compact_remove_common_cb
 *
 * Purpose:	Common callback routine for deleting 'link' message for a
 *              particular name.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__compact_remove_common_cb(const void *_mesg, unsigned H5_ATTR_UNUSED idx, void *_udata)
{
    const H5O_link_t *lnk       = (const H5O_link_t *)_mesg; /* Pointer to link */
    H5G_iter_rm_t    *udata     = (H5G_iter_rm_t *)_udata;   /* 'User data' passed in */
    herr_t            ret_value = H5_ITER_CONT;              /* Return value */

    FUNC_ENTER_PACKAGE

    /* check arguments */
    assert(lnk);
    assert(udata);

    /* If we've found the right link, get the object type */
    if (strcmp(lnk->name, udata->name) == 0) {
        /* Replace path names for link being removed */
        if (H5G__link_name_replace(udata->file, udata->grp_full_path_r, lnk) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTGET, H5_ITER_ERROR, "unable to get object type");

        /* Stop the iteration, we found the correct link */
        HGOTO_DONE(H5_ITER_STOP);
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__compact_remove_common_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5G__compact_remove
 *
 * Purpose:	Remove NAME from links.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__compact_remove(const H5O_loc_t *oloc, H5RS_str_t *grp_full_path_r, const char *name)
{
    H5G_iter_rm_t udata;               /* Data to pass through OH iteration */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(oloc && oloc->file);
    assert(name && *name);

    /* Initialize data to pass through object header iteration */
    udata.file            = oloc->file;
    udata.grp_full_path_r = grp_full_path_r;
    udata.name            = name;

    /* Iterate over the link messages to delete the right one */
    if (H5O_msg_remove_op(oloc, H5O_LINK_ID, H5O_FIRST, H5G__compact_remove_common_cb, &udata, true) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTDELETE, FAIL, "unable to delete link message");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__compact_remove() */

/*-------------------------------------------------------------------------
 * Function:	H5G__compact_remove_by_idx
 *
 * Purpose:	Remove link from group, according to an index order.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__compact_remove_by_idx(const H5O_loc_t *oloc, const H5O_linfo_t *linfo, H5RS_str_t *grp_full_path_r,
                           H5_index_t idx_type, H5_iter_order_t order, hsize_t n)
{
    H5G_link_table_t ltable = {0, NULL};  /* Link table */
    H5G_iter_rm_t    udata;               /* Data to pass through OH iteration */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(oloc && oloc->file);
    assert(linfo);

    /* Build table of all link messages, sorted according to desired order */
    if (H5G__compact_build_table(oloc, linfo, idx_type, order, &ltable) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create link message table");

    /* Check for going out of bounds */
    if (n >= ltable.nlinks)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "index out of bound");

    /* Initialize data to pass through object header iteration */
    udata.file            = oloc->file;
    udata.grp_full_path_r = grp_full_path_r;
    udata.name            = ltable.lnks[n].name;

    /* Iterate over the link messages to delete the right one */
    if (H5O_msg_remove_op(oloc, H5O_LINK_ID, H5O_FIRST, H5G__compact_remove_common_cb, &udata, true) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTDELETE, FAIL, "unable to delete link message");

done:
    /* Release link table */
    if (ltable.lnks && H5G__link_release_table(&ltable) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTFREE, FAIL, "unable to release link table");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__compact_remove_by_idx() */

/*-------------------------------------------------------------------------
 * Function:	H5G__compact_iterate
 *
 * Purpose:	Iterate over the links in a group
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__compact_iterate(const H5O_loc_t *oloc, const H5O_linfo_t *linfo, H5_index_t idx_type,
                     H5_iter_order_t order, hsize_t skip, hsize_t *last_lnk, H5G_lib_iterate_t op,
                     void *op_data)
{
    H5G_link_table_t ltable    = {0, NULL}; /* Link table */
    herr_t           ret_value = FAIL;      /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(oloc);
    assert(linfo);
    assert(op);

    /* Build table of all link messages */
    if (H5G__compact_build_table(oloc, linfo, idx_type, order, &ltable) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create link message table");

    /* Iterate over links in table */
    if ((ret_value = H5G__link_iterate_table(&ltable, skip, last_lnk, op, op_data)) < 0)
        HERROR(H5E_SYM, H5E_CANTNEXT, "iteration operator failed");

done:
    /* Release link table */
    if (ltable.lnks && H5G__link_release_table(&ltable) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTFREE, FAIL, "unable to release link table");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__compact_iterate() */

/*-------------------------------------------------------------------------
 * Function:    H5G__compact_lookup_cb
 *
 * Purpose:     Callback routine for searching 'link' messages for a particular
 *              name & getting object location for it
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__compact_lookup_cb(const void *_mesg, unsigned H5_ATTR_UNUSED idx, void *_udata)
{
    const H5O_link_t *lnk       = (const H5O_link_t *)_mesg; /* Pointer to link */
    H5G_iter_lkp_t   *udata     = (H5G_iter_lkp_t *)_udata;  /* 'User data' passed in */
    herr_t            ret_value = H5_ITER_CONT;              /* Return value */

    FUNC_ENTER_PACKAGE

    /* check arguments */
    assert(lnk);
    assert(udata);

    /* Check for name to get information */
    if (strcmp(lnk->name, udata->name) == 0) {
        if (udata->lnk) {
            /* Copy link information */
            if (NULL == H5O_msg_copy(H5O_LINK_ID, lnk, udata->lnk))
                HGOTO_ERROR(H5E_SYM, H5E_CANTCOPY, H5_ITER_ERROR, "can't copy link message");
        } /* end if */

        /* Indicate that the correct link was found */
        *udata->found = true;

        /* Stop iteration now */
        HGOTO_DONE(H5_ITER_STOP);
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__compact_lookup_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5G__compact_lookup
 *
 * Purpose:	Look up an object relative to a group, using link messages.
 *
 * Return:	Non-negative (true/false) on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__compact_lookup(const H5O_loc_t *oloc, const char *name, bool *found, H5O_link_t *lnk)
{
    H5G_iter_lkp_t      udata;               /* User data for iteration callback */
    H5O_mesg_operator_t op;                  /* Message operator */
    herr_t              ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check arguments */
    assert(name && *name);
    assert(found);
    assert(lnk && oloc->file);

    /* Set up user data for iteration */
    udata.name  = name;
    udata.lnk   = lnk;
    udata.found = found;

    /* Iterate through the link messages, adding them to the table */
    op.op_type  = H5O_MESG_OP_APP;
    op.u.app_op = H5G__compact_lookup_cb;
    if (H5O_msg_iterate(oloc, H5O_LINK_ID, &op, &udata) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "error iterating over link messages");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__compact_lookup() */

/*-------------------------------------------------------------------------
 * Function:	H5G__compact_lookup_by_idx
 *
 * Purpose:	Look up an object in a group using link messages,
 *              according to the order of an index
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__compact_lookup_by_idx(const H5O_loc_t *oloc, const H5O_linfo_t *linfo, H5_index_t idx_type,
                           H5_iter_order_t order, hsize_t n, H5O_link_t *lnk)
{
    H5G_link_table_t ltable    = {0, NULL}; /* Link table */
    herr_t           ret_value = SUCCEED;   /* Return value */

    FUNC_ENTER_PACKAGE

    /* check arguments */
    assert(oloc && oloc->file);
    assert(linfo);
    assert(lnk);

    /* Build table of all link messages, sorted according to desired order */
    if (H5G__compact_build_table(oloc, linfo, idx_type, order, &ltable) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "can't create link message table");

    /* Check for going out of bounds */
    if (n >= ltable.nlinks)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "index out of bound");

    /* Copy link information */
    if (NULL == H5O_msg_copy(H5O_LINK_ID, &ltable.lnks[n], lnk))
        HGOTO_ERROR(H5E_SYM, H5E_CANTCOPY, H5_ITER_ERROR, "can't copy link message");

done:
    /* Release link table */
    if (ltable.lnks && H5G__link_release_table(&ltable) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTFREE, FAIL, "unable to release link table");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__compact_lookup_by_idx() */
