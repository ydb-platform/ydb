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
 * Purpose:	Group testing functions.
 */

/****************/
/* Module Setup */
/****************/

#include "H5Gmodule.h" /* This source code file is part of the H5G module */
#define H5G_TESTING    /*suppress warning about H5G testing funcs*/

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5CXprivate.h" /* API Contexts                             */
#include "H5Dprivate.h"  /* Datasets                                 */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5Gpkg.h"      /* Groups                                   */
#include "H5HLprivate.h" /* Local Heaps                              */
#include "H5Iprivate.h"  /* IDs                                      */
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

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*--------------------------------------------------------------------------
 NAME
    H5G__is_empty_test
 PURPOSE
    Determine whether a group contains no objects
 USAGE
    htri_t H5G__is_empty_test(gid)
        hid_t gid;              IN: group to check
 RETURNS
    true/false on success, FAIL on failure
 DESCRIPTION
    Checks to see if the group has no link messages and no symbol table message
    and no "dense" link storage
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    DO NOT USE THIS FUNCTION FOR ANYTHING EXCEPT TESTING
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
htri_t
H5G__is_empty_test(hid_t gid)
{
    H5G_t *grp            = NULL;  /* Pointer to group */
    htri_t msg_exists     = false; /* Indicate that a header message is present */
    htri_t linfo_exists   = false; /* Indicate that the 'link info' message is present */
    bool   api_ctx_pushed = false; /* Whether API context pushed */
    htri_t ret_value      = true;  /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get group structure */
    if (NULL == (grp = (H5G_t *)H5VL_object_verify(gid, H5I_GROUP)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a group");

    /* Set API context */
    if (H5CX_push() < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, FAIL, "can't set API context");
    api_ctx_pushed = true;

    /* "New format" checks */

    /* Check if the group has any link messages */
    if ((msg_exists = H5O_msg_exists(&(grp->oloc), H5O_LINK_ID)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to read object header");
    if (msg_exists > 0) {
        /* Sanity check that new group format shouldn't have old messages */
        if ((msg_exists = H5O_msg_exists(&(grp->oloc), H5O_STAB_ID)) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to read object header");
        if (msg_exists > 0)
            HGOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "both symbol table and link messages found");

        HGOTO_DONE(false);
    } /* end if */

    /* Check for a link info message */
    if ((linfo_exists = H5O_msg_exists(&(grp->oloc), H5O_LINFO_ID)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to read object header");
    if (linfo_exists > 0) {
        H5O_linfo_t linfo; /* Link info message */

        /* Sanity check that new group format shouldn't have old messages */
        if ((msg_exists = H5O_msg_exists(&(grp->oloc), H5O_STAB_ID)) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to read object header");
        if (msg_exists > 0)
            HGOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "both symbol table and link info messages found");

        /* Get the link info */
        if (H5G__obj_get_linfo(&(grp->oloc), &linfo) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_BADMESG, FAIL, "can't get link info");

        /* Check for 'dense' link storage file addresses being defined */
        if (H5_addr_defined(linfo.fheap_addr))
            HGOTO_DONE(false);
        if (H5_addr_defined(linfo.name_bt2_addr))
            HGOTO_DONE(false);
        if (H5_addr_defined(linfo.corder_bt2_addr))
            HGOTO_DONE(false);

        /* Check for link count */
        if (linfo.nlinks > 0)
            HGOTO_DONE(false);
    } /* end if */

    /* "Old format" checks */

    /* Check if the group has a symbol table message */
    if ((msg_exists = H5O_msg_exists(&(grp->oloc), H5O_STAB_ID)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to read object header");
    if (msg_exists > 0) {
        H5O_stab_t stab;   /* Info about local heap & B-tree */
        hsize_t    nlinks; /* Number of links in the group */

        /* Sanity check that old group format shouldn't have new messages */
        if (linfo_exists > 0)
            HGOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "both symbol table and link info messages found");
        if ((msg_exists = H5O_msg_exists(&(grp->oloc), H5O_GINFO_ID)) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to read object header");
        if (msg_exists > 0)
            HGOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "both symbol table and group info messages found");

        /* Get the B-tree & local heap info */
        if (NULL == H5O_msg_read(&(grp->oloc), H5O_STAB_ID, &stab))
            HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "unable to read symbol table message");

        /* Get the count of links in the group */
        if (H5G__stab_count(&(grp->oloc), &nlinks) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "unable to count links");

        /* Check for link count */
        if (nlinks > 0)
            HGOTO_DONE(false);
    } /* end if */

done:
    if (api_ctx_pushed && H5CX_pop(false) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTRESET, FAIL, "can't reset API context");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5G__is_empty_test() */

/*--------------------------------------------------------------------------
 NAME
    H5G__has_links_test
 PURPOSE
    Determine whether a group contains link messages
 USAGE
    htri_t H5G__has_links_test(gid)
        hid_t gid;              IN: group to check
        unsigned *nmsgs;        OUT: # of link messages in header
 RETURNS
    true/false on success, FAIL on failure
 DESCRIPTION
    Checks to see if the group has link messages and how many.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    DO NOT USE THIS FUNCTION FOR ANYTHING EXCEPT TESTING
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
htri_t
H5G__has_links_test(hid_t gid, unsigned *nmsgs)
{
    H5G_t *grp            = NULL;  /* Pointer to group */
    htri_t msg_exists     = 0;     /* Indicate that a header message is present */
    bool   api_ctx_pushed = false; /* Whether API context pushed */
    htri_t ret_value      = true;  /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get group structure */
    if (NULL == (grp = (H5G_t *)H5VL_object_verify(gid, H5I_GROUP)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a group");

    /* Set API context */
    if (H5CX_push() < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, FAIL, "can't set API context");
    api_ctx_pushed = true;

    /* Check if the group has any link messages */
    if ((msg_exists = H5O_msg_exists(&(grp->oloc), H5O_LINK_ID)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to read object header");
    if (msg_exists == 0)
        HGOTO_DONE(false);

    /* Check if the group has a symbol table message */
    if ((msg_exists = H5O_msg_exists(&(grp->oloc), H5O_STAB_ID)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to read object header");
    if (msg_exists > 0)
        HGOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "both symbol table and link messages found");

    /* Check if we should retrieve the number of link messages */
    if (nmsgs) {
        int msg_count; /* Number of messages of a type */

        /* Check how many link messages there are */
        if ((msg_count = H5O_msg_count(&(grp->oloc), H5O_LINK_ID)) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTCOUNT, FAIL, "unable to count link messages");
        *nmsgs = (unsigned)msg_count;
    } /* end if */

done:
    if (api_ctx_pushed && H5CX_pop(false) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTRESET, FAIL, "can't reset API context");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5G__has_links_test() */

/*--------------------------------------------------------------------------
 NAME
    H5G__has_stab_test
 PURPOSE
    Determine whether a group contains a symbol table message
 USAGE
    htri_t H5G__has_stab_test(gid)
        hid_t gid;              IN: group to check
 RETURNS
    true/false on success, FAIL on failure
 DESCRIPTION
    Checks to see if the group has a symbol table message.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    DO NOT USE THIS FUNCTION FOR ANYTHING EXCEPT TESTING
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
htri_t
H5G__has_stab_test(hid_t gid)
{
    H5G_t *grp            = NULL;  /* Pointer to group */
    htri_t msg_exists     = 0;     /* Indicate that a header message is present */
    bool   api_ctx_pushed = false; /* Whether API context pushed */
    htri_t ret_value      = true;  /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get group structure */
    if (NULL == (grp = (H5G_t *)H5VL_object_verify(gid, H5I_GROUP)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a group");

    /* Set API context */
    if (H5CX_push() < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, FAIL, "can't set API context");
    api_ctx_pushed = true;

    /* Check if the group has a symbol table message */
    if ((msg_exists = H5O_msg_exists(&(grp->oloc), H5O_STAB_ID)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to read object header");
    if (msg_exists == 0)
        HGOTO_DONE(false);

    /* Check if the group has any link messages */
    if ((msg_exists = H5O_msg_exists(&(grp->oloc), H5O_LINK_ID)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to read object header");
    if (msg_exists > 0)
        HGOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "both symbol table and link messages found");

done:
    if (api_ctx_pushed && H5CX_pop(false) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTRESET, FAIL, "can't reset API context");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5G__has_stab_test() */

/*--------------------------------------------------------------------------
 NAME
    H5G__is_new_dense_test
 PURPOSE
    Determine whether a group is in the "new" format and dense
 USAGE
    htri_t H5G__is_new_dense_test(gid)
        hid_t gid;              IN: group to check
 RETURNS
    true/false on success, FAIL on failure
 DESCRIPTION
    Checks to see if the group is in the "new" format for groups (link messages/
    fractal heap+v2 B-tree) and if it is in "dense" storage form (ie. it has
    a name B-tree index).
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    DO NOT USE THIS FUNCTION FOR ANYTHING EXCEPT TESTING
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
htri_t
H5G__is_new_dense_test(hid_t gid)
{
    H5G_t *grp            = NULL;  /* Pointer to group */
    htri_t msg_exists     = 0;     /* Indicate that a header message is present */
    bool   api_ctx_pushed = false; /* Whether API context pushed */
    htri_t ret_value      = true;  /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get group structure */
    if (NULL == (grp = (H5G_t *)H5VL_object_verify(gid, H5I_GROUP)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a group");

    /* Set API context */
    if (H5CX_push() < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, FAIL, "can't set API context");
    api_ctx_pushed = true;

    /* Check if the group has a symbol table message */
    if ((msg_exists = H5O_msg_exists(&(grp->oloc), H5O_STAB_ID)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to read object header");
    if (msg_exists > 0)
        HGOTO_DONE(false);

    /* Check if the group has any link messages */
    if ((msg_exists = H5O_msg_exists(&(grp->oloc), H5O_LINK_ID)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to read object header");
    if (msg_exists > 0)
        HGOTO_DONE(false);

    /* Check if the group has link info message */
    if ((msg_exists = H5O_msg_exists(&(grp->oloc), H5O_LINFO_ID)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to read object header");
    if (msg_exists > 0) {
        H5O_linfo_t linfo; /* Link info message */

        /* Get the link info */
        if (H5G__obj_get_linfo(&(grp->oloc), &linfo) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_BADMESG, FAIL, "can't get link info");

        /* Check for 'dense' link storage file addresses being defined */
        if (!H5_addr_defined(linfo.fheap_addr))
            HGOTO_DONE(false);
        if (!H5_addr_defined(linfo.name_bt2_addr))
            HGOTO_DONE(false);
    } /* end if */

done:
    if (api_ctx_pushed && H5CX_pop(false) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTRESET, FAIL, "can't reset API context");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5G__is_new_dense_test() */

/*--------------------------------------------------------------------------
 NAME
    H5G__new_dense_info_test
 PURPOSE
    Retrieve information about the state of the new "dense" storage for groups
 USAGE
    herr_t H5G__new_dense_info_test(gid, name_count, corder_count)
        hid_t gid;              IN: group to check
        hsize_t *name_count;    OUT: Number of links in name index
        hsize_t *corder_count;  OUT: Number of links in creation order index
 RETURNS
    SUCCEED/FAIL
 DESCRIPTION
    Currently, just retrieves the number of links in each index and returns
    them.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    DO NOT USE THIS FUNCTION FOR ANYTHING EXCEPT TESTING
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5G__new_dense_info_test(hid_t gid, hsize_t *name_count, hsize_t *corder_count)
{
    H5B2_t     *bt2_name   = NULL;        /* v2 B-tree handle for name index */
    H5B2_t     *bt2_corder = NULL;        /* v2 B-tree handle for creation order index */
    H5O_linfo_t linfo;                    /* Link info message */
    H5G_t      *grp            = NULL;    /* Pointer to group */
    bool        api_ctx_pushed = false;   /* Whether API context pushed */
    herr_t      ret_value      = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get group structure */
    if (NULL == (grp = (H5G_t *)H5VL_object_verify(gid, H5I_GROUP)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a group");

    /* Set API context */
    if (H5CX_push() < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, FAIL, "can't set API context");
    api_ctx_pushed = true;

    /* Set metadata tag in API context */
    H5_BEGIN_TAG(grp->oloc.addr)

    /* Get the link info */
    if (H5G__obj_get_linfo(&(grp->oloc), &linfo) < 0)
        HGOTO_ERROR_TAG(H5E_SYM, H5E_BADMESG, FAIL, "can't get link info");

    /* Check for 'dense' link storage file addresses being defined */
    if (!H5_addr_defined(linfo.fheap_addr))
        HGOTO_DONE_TAG(FAIL);
    if (!H5_addr_defined(linfo.name_bt2_addr))
        HGOTO_DONE_TAG(FAIL);

    /* Open the name index v2 B-tree */
    if (NULL == (bt2_name = H5B2_open(grp->oloc.file, linfo.name_bt2_addr, NULL)))
        HGOTO_ERROR_TAG(H5E_SYM, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for name index");

    /* Retrieve # of records in name index */
    if (H5B2_get_nrec(bt2_name, name_count) < 0)
        HGOTO_ERROR_TAG(H5E_SYM, H5E_CANTCOUNT, FAIL, "unable to retrieve # of records from name index");

    /* Check if there is a creation order index */
    if (H5_addr_defined(linfo.corder_bt2_addr)) {
        /* Open the creation order index v2 B-tree */
        if (NULL == (bt2_corder = H5B2_open(grp->oloc.file, linfo.corder_bt2_addr, NULL)))
            HGOTO_ERROR_TAG(H5E_SYM, H5E_CANTOPENOBJ, FAIL,
                            "unable to open v2 B-tree for creation order index");

        /* Retrieve # of records in creation order index */
        if (H5B2_get_nrec(bt2_corder, corder_count) < 0)
            HGOTO_ERROR_TAG(H5E_SYM, H5E_CANTCOUNT, FAIL,
                            "unable to retrieve # of records from creation order index");
    } /* end if */
    else
        *corder_count = 0;

    /* Reset metadata tag in API context */
    H5_END_TAG

done:
    /* Release resources */
    if (bt2_name && H5B2_close(bt2_name) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTCLOSEOBJ, FAIL, "can't close v2 B-tree for name index");
    if (bt2_corder && H5B2_close(bt2_corder) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTCLOSEOBJ, FAIL, "can't close v2 B-tree for creation order index");
    if (api_ctx_pushed && H5CX_pop(false) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTRESET, FAIL, "can't reset API context");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5G__new_dense_info_test() */

/*--------------------------------------------------------------------------
 NAME
    H5G__lheap_size_test
 PURPOSE
    Determine the size of a local heap for a group
 USAGE
    herr_t H5G__lheap_size_test(gid, lheap_size)
        hid_t gid;              IN: group to check
        size_t *lheap_size;     OUT: Size of local heap
 RETURNS
    SUCCEED/FAIL
 DESCRIPTION
    Checks the size of the local heap for a group
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    DO NOT USE THIS FUNCTION FOR ANYTHING EXCEPT TESTING
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5G__lheap_size_test(hid_t gid, size_t *lheap_size)
{
    H5G_t     *grp = NULL;               /* Pointer to group */
    H5O_stab_t stab;                     /* Symbol table message	*/
    bool       api_ctx_pushed = false;   /* Whether API context pushed */
    herr_t     ret_value      = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get group structure */
    if (NULL == (grp = (H5G_t *)H5VL_object_verify(gid, H5I_GROUP)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a group");

    /* Set API context */
    if (H5CX_push() < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, FAIL, "can't set API context");
    api_ctx_pushed = true;

    /* Make certain the group has a symbol table message */
    if (NULL == H5O_msg_read(&(grp->oloc), H5O_STAB_ID, &stab))
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to read symbol table message");

    /* Check the size of the local heap for the group */
    if (H5HL_get_size(grp->oloc.file, stab.heap_addr, lheap_size) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGETSIZE, FAIL, "can't query local heap size");

done:
    if (api_ctx_pushed && H5CX_pop(false) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTRESET, FAIL, "can't reset API context");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5G__lheap_size_test() */

/*--------------------------------------------------------------------------
 NAME
    H5G__user_path_test
 PURPOSE
    Retrieve the user path for an ID
 USAGE
    herr_t H5G__user_path_test(obj_id, user_path, user_path_len)
        hid_t obj_id;           IN: ID to check
        char *user_path;        OUT: Pointer to buffer for User path
        size_t *user_path_len;  OUT: Size of user path
        unsigned *obj_hidden;   OUT: Whether object is hidden
 RETURNS
    SUCCEED/FAIL
 DESCRIPTION
    Retrieves the user path for an ID.  A zero for the length is returned in
    the case of no user path.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    DO NOT USE THIS FUNCTION FOR ANYTHING EXCEPT TESTING
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5G__user_path_test(hid_t obj_id, char *user_path, size_t *user_path_len, unsigned *obj_hidden)
{
    void             *obj_ptr;                  /* Pointer to object for ID */
    const H5G_name_t *obj_path;                 /* Pointer to group hier. path for obj */
    bool              api_ctx_pushed = false;   /* Whether API context pushed */
    herr_t            ret_value      = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(user_path_len);
    assert(obj_hidden);

    /* Get pointer to object for ID */
    if (NULL == (obj_ptr = H5VL_object(obj_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "can't get object for ID");

    /* Set API context */
    if (H5CX_push() < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTSET, FAIL, "can't set API context");
    api_ctx_pushed = true;

    /* Get the symbol table entry */
    switch (H5I_get_type(obj_id)) {
        case H5I_GROUP:
            obj_path = H5G_nameof((H5G_t *)obj_ptr);
            break;

        case H5I_DATASET:
            obj_path = H5D_nameof((H5D_t *)obj_ptr);
            break;

        case H5I_DATATYPE:
            /* Avoid non-named datatypes */
            if (!H5T_is_named((H5T_t *)obj_ptr))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a named datatype");

            obj_path = H5T_nameof((H5T_t *)obj_ptr);
            break;

        case H5I_MAP:
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "maps not supported in native VOL connector");

        case H5I_UNINIT:
        case H5I_BADID:
        case H5I_FILE:
        case H5I_DATASPACE:
        case H5I_ATTR:
        case H5I_VFL:
        case H5I_VOL:
        case H5I_GENPROP_CLS:
        case H5I_GENPROP_LST:
        case H5I_ERROR_CLASS:
        case H5I_ERROR_MSG:
        case H5I_ERROR_STACK:
        case H5I_SPACE_SEL_ITER:
        case H5I_EVENTSET:
        case H5I_NTYPES:
        default:
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "unknown data object type");
    } /* end switch */
    assert(obj_path);

    /* Retrieve a copy of the user path and put it into the buffer */
    if (obj_path->user_path_r) {
        size_t len = H5RS_len(obj_path->user_path_r);

        /* Set the user path, if given */
        if (user_path)
            strncpy(user_path, H5RS_get_str(obj_path->user_path_r), (len + 1));

        /* Set the length of the path */
        *user_path_len = len;

        /* Set the user path hidden flag */
        *obj_hidden = obj_path->obj_hidden;
    } /* end if */
    else {
        *user_path_len = 0;
        *obj_hidden    = 0;
    } /* end else */

done:
    if (api_ctx_pushed && H5CX_pop(false) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTRESET, FAIL, "can't reset API context");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5G__user_path_test() */

/*-------------------------------------------------------------------------
 * Function:    H5G__verify_cached_stab_test
 *
 * Purpose:     Check that a that the provided group entry contains a
 *              cached symbol table entry, that the entry matches that in
 *              the provided group's object header, and check that the
 *              addresses are valid.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__verify_cached_stab_test(H5O_loc_t *grp_oloc, H5G_entry_t *ent)
{
    H5O_stab_t stab;                /* Symbol table             */
    H5HL_t    *heap      = NULL;    /* Pointer to local heap    */
    herr_t     ret_value = SUCCEED; /* Return value             */

    FUNC_ENTER_PACKAGE_TAG(grp_oloc->addr)

    /* Verify that stab info is cached in ent */
    if (ent->type != H5G_CACHED_STAB)
        HGOTO_ERROR(H5E_SYM, H5E_BADTYPE, FAIL, "symbol table information is not cached");

    /* Read the symbol table message from the group */
    if (NULL == H5O_msg_read(grp_oloc, H5O_STAB_ID, &stab))
        HGOTO_ERROR(H5E_SYM, H5E_BADMESG, FAIL, "unable to read symbol table message");

    /* Verify that the cached symbol table info matches the symbol table message
     * in the object header.
     */
    if ((ent->cache.stab.btree_addr != stab.btree_addr) || (ent->cache.stab.heap_addr != stab.heap_addr))
        HGOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "cached stab info does not match object header");

    /* Verify that the btree address is valid */
    if (H5B_valid(grp_oloc->file, H5B_SNODE, stab.btree_addr) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_NOTFOUND, FAIL, "b-tree address is invalid");

    /* Verify that the heap address is valid */
    if (NULL == (heap = H5HL_protect(grp_oloc->file, stab.heap_addr, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_HEAP, H5E_NOTFOUND, FAIL, "heap address is invalid");

done:
    /* Release resources */
    if (heap && H5HL_unprotect(heap) < 0)
        HDONE_ERROR(H5E_SYM, H5E_PROTECT, FAIL, "unable to unprotect symbol table heap");

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5G__verify_cached_stab_test() */

/*-------------------------------------------------------------------------
 * Function:    H5G__verify_cached_stabs_test_cb
 *
 * Purpose:     Verify that all entries in this node contain cached symbol
 *              table information if and only if the entry refers to a
 *              group with a symbol table, and that that information is
 *              correct.
 *
 * Return:      H5_ITER_STOP/H5_ITER_CONT/H5_ITER_ERROR
 *
 *-------------------------------------------------------------------------
 */
static int
H5G__verify_cached_stabs_test_cb(H5F_t *f, const void H5_ATTR_UNUSED *_lt_key, haddr_t addr,
                                 const void H5_ATTR_UNUSED *_rt_key, void H5_ATTR_UNUSED *udata)
{
    H5G_node_t *sn = NULL;
    H5O_loc_t   targ_oloc;
    H5O_t      *targ_oh = NULL;
    htri_t      stab_exists;
    H5O_stab_t  stab;
    unsigned    i;
    int         ret_value = H5_ITER_CONT;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(f);
    assert(H5_addr_defined(addr));

    /* Load the node */
    if (NULL == (sn = (H5G_node_t *)H5AC_protect(f, H5AC_SNODE, addr, f, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTLOAD, H5_ITER_ERROR, "unable to load symbol table node");

    /* Check each target object to see if its stab message (if present) matches
     * the cached stab (if present).  If one exists, both must exist. */
    /* Initialize constant fields in target oloc */
    targ_oloc.file         = f;
    targ_oloc.holding_file = false;

    /* Iterate over entries */
    for (i = 0; i < sn->nsyms; i++) {
        /* Update oloc address */
        targ_oloc.addr = sn->entry[i].header;

        /* Load target object header */
        if (NULL == (targ_oh = H5O_protect(&targ_oloc, H5AC__READ_ONLY_FLAG, false)))
            HGOTO_ERROR(H5E_SYM, H5E_CANTPROTECT, H5_ITER_ERROR, "unable to protect target object header");

        /* Check if a symbol table message exists */
        if ((stab_exists = H5O_msg_exists_oh(targ_oh, H5O_STAB_ID)) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, H5_ITER_ERROR, "unable to check for STAB message");

        if (stab_exists) {
            /* Read symbol table message */
            if (NULL == H5O_msg_read_oh(f, targ_oh, H5O_STAB_ID, &stab))
                HGOTO_ERROR(H5E_SYM, H5E_CANTGET, H5_ITER_ERROR, "unable to read STAB message");

            /* Check if the stab matches the cached stab info */
            if (sn->entry[i].type != H5G_CACHED_STAB)
                HGOTO_ERROR(H5E_SYM, H5E_BADVALUE, H5_ITER_ERROR, "STAB message is not cached in group node");

            if ((sn->entry[i].cache.stab.btree_addr != stab.btree_addr) ||
                (sn->entry[i].cache.stab.heap_addr != stab.heap_addr))
                HGOTO_ERROR(H5E_SYM, H5E_BADVALUE, H5_ITER_ERROR,
                            "cached symbol table information is incorrect");
        }
        else if (sn->entry[i].type == H5G_CACHED_STAB)
            HGOTO_ERROR(H5E_SYM, H5E_BADVALUE, H5_ITER_ERROR, "nonexistent STAB message is cached");

        /* Unprotect target object */
        if (H5O_unprotect(&targ_oloc, targ_oh, H5AC__NO_FLAGS_SET) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTUNPROTECT, H5_ITER_ERROR, "unable to release object header");
        targ_oh = NULL;
    } /* end for */

done:
    if (sn && H5AC_unprotect(f, H5AC_SNODE, addr, sn, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_SYM, H5E_PROTECT, H5_ITER_ERROR, "unable to release object header");

    if (targ_oh) {
        assert(ret_value == H5_ITER_ERROR);
        if (H5O_unprotect(&targ_oloc, targ_oh, H5AC__NO_FLAGS_SET) < 0)
            HDONE_ERROR(H5E_SYM, H5E_CANTUNPROTECT, H5_ITER_ERROR, "unable to release object header");
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__verify_cached_stabs_test_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5G__verify_cached_stabs_test
 *
 * Purpose:     If the provided group contains a symbol table, verifies
 *              that all links in the group contain cached symbol table
 *              information if and only if the link points to a group
 *              with a symbol table, and that that information is correct.
 *              If the provided group does not contain a symbol table,
 *              does nothing.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__verify_cached_stabs_test(hid_t gid)
{
    H5G_t          *grp = NULL; /* Group */
    htri_t          stab_exists;
    H5O_stab_t      stab;                     /* Symbol table message */
    H5G_bt_common_t udata     = {NULL, NULL}; /* Dummy udata so H5B_iterate doesn't freak out */
    haddr_t         prev_tag  = HADDR_UNDEF;  /* Previous metadata tag */
    herr_t          ret_value = SUCCEED;      /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(gid >= 0);

    /* Check args */
    if (NULL == (grp = (H5G_t *)H5VL_object_verify(gid, H5I_GROUP)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a group");

    /* Set up metadata tagging */
    H5AC_tag(grp->oloc.addr, &prev_tag);

    /* Check for group having a symbol table message */
    /* Check for the group having a group info message */
    if ((stab_exists = H5O_msg_exists(&(grp->oloc), H5O_STAB_ID)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to read object header");

    /* No need to check anything if the symbol table doesn't exist */
    if (!stab_exists)
        HGOTO_DONE(SUCCEED);

    /* Read the stab */
    if (NULL == H5O_msg_read(&(grp->oloc), H5O_STAB_ID, &stab))
        HGOTO_ERROR(H5E_SYM, H5E_BADMESG, FAIL, "can't get symbol table info");

    /* Iterate over the b-tree, checking validity of cached information */
    if ((ret_value = H5B_iterate(grp->oloc.file, H5B_SNODE, stab.btree_addr, H5G__verify_cached_stabs_test_cb,
                                 &udata)) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTNEXT, FAIL, "iteration operator failed");

    /* Reset metadata tagging */
    H5AC_tag(prev_tag, NULL);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__verify_cached_stabs_test() */
