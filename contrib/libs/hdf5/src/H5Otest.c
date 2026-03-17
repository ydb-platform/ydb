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
 * Purpose:	Object header testing functions.
 */

/****************/
/* Module Setup */
/****************/

#define H5A_FRIEND     /*suppress error about including H5Apkg	  */
#include "H5Omodule.h" /* This source code file is part of the H5O module */
#define H5O_TESTING    /*suppress warning about H5O testing funcs*/

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Apkg.h"      /* Attributes	  			*/
#include "H5ACprivate.h" /* Metadata cache			*/
#include "H5CXprivate.h" /* API Contexts                         */
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Iprivate.h"  /* IDs			  		*/
#include "H5Opkg.h"      /* Object headers			*/

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
    H5O__is_attr_dense_test
 PURPOSE
    Determine whether attributes for an object are stored "densely"
 USAGE
    htri_t H5O__is_attr_dense_test(oid)
        hid_t oid;              IN: object to check
 RETURNS
    Non-negative true/false on success, negative on failure
 DESCRIPTION
    Checks to see if the object is storing attributes in the "dense" or
    "compact" form.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    DO NOT USE THIS FUNCTION FOR ANYTHING EXCEPT TESTING
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
htri_t
H5O__is_attr_dense_test(hid_t oid)
{
    H5O_t      *oh = NULL;              /* Object header */
    H5O_ainfo_t ainfo;                  /* Attribute information for object */
    H5O_loc_t  *loc;                    /* Pointer to object's location */
    bool        api_ctx_pushed = false; /* Whether API context pushed */
    htri_t      ret_value      = FAIL;  /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get object location for object */
    if (NULL == (loc = H5O_get_loc(oid)))
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "object not found");

    /* Set API context */
    if (H5CX_push() < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't set API context");
    api_ctx_pushed = true;

    /* Get the object header */
    if (NULL == (oh = H5O_protect(loc, H5AC__READ_ONLY_FLAG, false)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, FAIL, "unable to load object header");

    /* Check for attribute info stored */
    ainfo.fheap_addr = HADDR_UNDEF;
    if (oh->version > H5O_VERSION_1) {
        /* Check for (& retrieve if available) attribute info */
        if (H5A__get_ainfo(loc->file, oh, &ainfo) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't check for attribute info message");
    } /* end if */

    /* Check if dense storage is being used */
    if (H5_addr_defined(ainfo.fheap_addr)) {
        /* Check for any messages in object header */
        assert(H5O__msg_count_real(oh, H5O_MSG_ATTR) == 0);

        ret_value = true;
    } /* end if */
    else
        ret_value = false;

done:
    if (oh && H5O_unprotect(loc, oh, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, FAIL, "unable to release object header");
    if (api_ctx_pushed && H5CX_pop(false) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTRESET, FAIL, "can't reset API context");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__is_attr_dense_test() */

/*--------------------------------------------------------------------------
 NAME
    H5O__is_attr_empty_test
 PURPOSE
    Determine whether there are any attributes for an object
 USAGE
    htri_t H5O__is_attr_empty_test(oid)
        hid_t oid;              IN: object to check
 RETURNS
    Non-negative true/false on success, negative on failure
 DESCRIPTION
    Checks to see if the object is storing any attributes.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    DO NOT USE THIS FUNCTION FOR ANYTHING EXCEPT TESTING
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
htri_t
H5O__is_attr_empty_test(hid_t oid)
{
    H5O_t      *oh       = NULL;        /* Object header */
    H5B2_t     *bt2_name = NULL;        /* v2 B-tree handle for name index */
    H5O_ainfo_t ainfo;                  /* Attribute information for object */
    htri_t      ainfo_exists = false;   /* Whether the attribute info exists in the file */
    H5O_loc_t  *loc;                    /* Pointer to object's location */
    hsize_t     nattrs;                 /* Number of attributes */
    bool        api_ctx_pushed = false; /* Whether API context pushed */
    htri_t      ret_value      = FAIL;  /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get object location for object */
    if (NULL == (loc = H5O_get_loc(oid)))
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "object not found");

    /* Set API context */
    if (H5CX_push() < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't set API context");
    api_ctx_pushed = true;

    /* Get the object header */
    if (NULL == (oh = H5O_protect(loc, H5AC__READ_ONLY_FLAG, false)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, FAIL, "unable to load object header");

    /* Check for attribute info stored */
    if (oh->version > H5O_VERSION_1) {
        /* Check for (& retrieve if available) attribute info */
        if ((ainfo_exists = H5A__get_ainfo(loc->file, oh, &ainfo)) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't check for attribute info message");
    } /* end if */

    /* Retrieve the number of attribute messages in header */
    nattrs = H5O__msg_count_real(oh, H5O_MSG_ATTR);

    /* Check for later version of object header format & attribute info available */
    if (oh->version > H5O_VERSION_1) {
        if (ainfo_exists) {
            /* Check for using dense storage */
            if (H5_addr_defined(ainfo.fheap_addr)) {
                /* Check for any messages in object header */
                assert(nattrs == 0);

                /* Set metadata tag in API context */
                H5_BEGIN_TAG(loc->addr)

                /* Open the name index v2 B-tree */
                if (NULL == (bt2_name = H5B2_open(loc->file, ainfo.name_bt2_addr, NULL)))
                    HGOTO_ERROR_TAG(H5E_OHDR, H5E_CANTOPENOBJ, FAIL,
                                    "unable to open v2 B-tree for name index");

                /* Reset metadata tag in API context */
                H5_END_TAG

                /* Retrieve # of records in name index */
                if (H5B2_get_nrec(bt2_name, &nattrs) < 0)
                    HGOTO_ERROR(H5E_OHDR, H5E_CANTCOUNT, FAIL,
                                "unable to retrieve # of records from name index");
            } /* end if */

            /* Verify that attribute count in object header is correct */
            assert(nattrs == ainfo.nattrs);
        } /* end if */
        else
            assert(nattrs == 0);
    } /* end if */

    /* Set the return value */
    ret_value = (nattrs == 0) ? true : false;

done:
    /* Release resources */
    if (bt2_name && H5B2_close(bt2_name) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTCLOSEOBJ, FAIL, "can't close v2 B-tree for name index");
    if (oh && H5O_unprotect(loc, oh, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, FAIL, "unable to release object header");
    if (api_ctx_pushed && H5CX_pop(false) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTRESET, FAIL, "can't reset API context");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__is_attr_empty_test() */

/*--------------------------------------------------------------------------
 NAME
    H5O__num_attrs_test
 PURPOSE
    Determine whether there are any attributes for an object
 USAGE
    herr_t H5O__num_attrs_test(oid, nattrs)
        hid_t oid;              IN: object to check
        hsize_t *nattrs;        OUT: Number of attributes on object
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Checks the # of attributes on an object
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    DO NOT USE THIS FUNCTION FOR ANYTHING EXCEPT TESTING
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5O__num_attrs_test(hid_t oid, hsize_t *nattrs)
{
    H5O_t      *oh       = NULL;          /* Object header */
    H5B2_t     *bt2_name = NULL;          /* v2 B-tree handle for name index */
    H5O_ainfo_t ainfo;                    /* Attribute information for object */
    H5O_loc_t  *loc;                      /* Pointer to object's location */
    hsize_t     obj_nattrs;               /* Number of attributes */
    bool        api_ctx_pushed = false;   /* Whether API context pushed */
    herr_t      ret_value      = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get object location for object */
    if (NULL == (loc = H5O_get_loc(oid)))
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "object not found");

    /* Set API context */
    if (H5CX_push() < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't set API context");
    api_ctx_pushed = true;

    /* Get the object header */
    if (NULL == (oh = H5O_protect(loc, H5AC__READ_ONLY_FLAG, false)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, FAIL, "unable to load object header");

    /* Check for attribute info stored */
    ainfo.fheap_addr = HADDR_UNDEF;
    if (oh->version > H5O_VERSION_1) {
        /* Check for (& retrieve if available) attribute info */
        if (H5A__get_ainfo(loc->file, oh, &ainfo) < 0)
            HGOTO_ERROR(H5E_ATTR, H5E_CANTGET, FAIL, "can't check for attribute info message");
    } /* end if */

    /* Retrieve the number of attribute messages in header */
    obj_nattrs = H5O__msg_count_real(oh, H5O_MSG_ATTR);

    /* Check for later version of object header format */
    if (oh->version > H5O_VERSION_1) {
        /* Check for using dense storage */
        if (H5_addr_defined(ainfo.fheap_addr)) {
            /* Check for any messages in object header */
            assert(obj_nattrs == 0);

            /* Set metadata tag in API context */
            H5_BEGIN_TAG(loc->addr)

            /* Open the name index v2 B-tree */
            if (NULL == (bt2_name = H5B2_open(loc->file, ainfo.name_bt2_addr, NULL)))
                HGOTO_ERROR_TAG(H5E_OHDR, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for name index");

            /* Reset metadata tag in API context */
            H5_END_TAG

            /* Retrieve # of records in name index */
            if (H5B2_get_nrec(bt2_name, &obj_nattrs) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTCOUNT, FAIL, "unable to retrieve # of records from name index");
        } /* end if */

        /* Verify that attribute count in object header is correct */
        assert(obj_nattrs == ainfo.nattrs);
    } /* end if */

    /* Set the number of attributes */
    *nattrs = obj_nattrs;

done:
    /* Release resources */
    if (bt2_name && H5B2_close(bt2_name) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTCLOSEOBJ, FAIL, "can't close v2 B-tree for name index");
    if (oh && H5O_unprotect(loc, oh, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, FAIL, "unable to release object header");
    if (api_ctx_pushed && H5CX_pop(false) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTRESET, FAIL, "can't reset API context");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__num_attrs_test() */

/*--------------------------------------------------------------------------
 NAME
    H5O__attr_dense_info_test
 PURPOSE
    Retrieve information about the state of the "dense" storage for attributes
 USAGE
    herr_t H5O__attr_dense_info_test(oid, name_count, corder_count)
        hid_t oid;              IN: Object to check
        hsize_t *name_count;    OUT: Number of attributes in name index
        hsize_t *corder_count;  OUT: Number of attributes in creation order index
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Currently, just retrieves the number of attributes in each index and returns
    them.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    DO NOT USE THIS FUNCTION FOR ANYTHING EXCEPT TESTING
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5O__attr_dense_info_test(hid_t oid, hsize_t *name_count, hsize_t *corder_count)
{
    H5O_t      *oh         = NULL;        /* Object header */
    H5B2_t     *bt2_name   = NULL;        /* v2 B-tree handle for name index */
    H5B2_t     *bt2_corder = NULL;        /* v2 B-tree handle for creation order index */
    H5O_ainfo_t ainfo;                    /* Attribute information for object */
    H5O_loc_t  *loc;                      /* Pointer to object's location */
    bool        api_ctx_pushed = false;   /* Whether API context pushed */
    herr_t      ret_value      = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get object location for object */
    if (NULL == (loc = H5O_get_loc(oid)))
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "object not found");

    /* Set API context */
    if (H5CX_push() < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't set API context");
    api_ctx_pushed = true;

    /* Set metadata tag in API context */
    H5_BEGIN_TAG(loc->addr)

    /* Get the object header */
    if (NULL == (oh = H5O_protect(loc, H5AC__READ_ONLY_FLAG, false)))
        HGOTO_ERROR_TAG(H5E_OHDR, H5E_CANTPROTECT, FAIL, "unable to load object header");

    /* Check for attribute info stored */
    ainfo.fheap_addr = HADDR_UNDEF;
    if (oh->version > H5O_VERSION_1) {
        /* Check for (& retrieve if available) attribute info */
        if (H5A__get_ainfo(loc->file, oh, &ainfo) < 0)
            HGOTO_ERROR_TAG(H5E_ATTR, H5E_CANTGET, FAIL, "can't check for attribute info message");
    } /* end if */

    /* Check for 'dense' attribute storage file addresses being defined */
    if (!H5_addr_defined(ainfo.fheap_addr))
        HGOTO_DONE_TAG(FAIL);
    if (!H5_addr_defined(ainfo.name_bt2_addr))
        HGOTO_DONE_TAG(FAIL);

    /* Open the name index v2 B-tree */
    if (NULL == (bt2_name = H5B2_open(loc->file, ainfo.name_bt2_addr, NULL)))
        HGOTO_ERROR_TAG(H5E_OHDR, H5E_CANTOPENOBJ, FAIL, "unable to open v2 B-tree for name index");

    /* Retrieve # of records in name index */
    if (H5B2_get_nrec(bt2_name, name_count) < 0)
        HGOTO_ERROR_TAG(H5E_OHDR, H5E_CANTCOUNT, FAIL, "unable to retrieve # of records from name index");

    /* Check if there is a creation order index */
    if (H5_addr_defined(ainfo.corder_bt2_addr)) {
        /* Open the creation order index v2 B-tree */
        if (NULL == (bt2_corder = H5B2_open(loc->file, ainfo.corder_bt2_addr, NULL)))
            HGOTO_ERROR_TAG(H5E_OHDR, H5E_CANTOPENOBJ, FAIL,
                            "unable to open v2 B-tree for creation order index");

        /* Retrieve # of records in creation order index */
        if (H5B2_get_nrec(bt2_corder, corder_count) < 0)
            HGOTO_ERROR_TAG(H5E_OHDR, H5E_CANTCOUNT, FAIL,
                            "unable to retrieve # of records from creation order index");
    } /* end if */
    else
        *corder_count = 0;

    /* Reset metadata tag in API context */
    H5_END_TAG

done:
    /* Release resources */
    if (bt2_name && H5B2_close(bt2_name) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTCLOSEOBJ, FAIL, "can't close v2 B-tree for name index");
    if (bt2_corder && H5B2_close(bt2_corder) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTCLOSEOBJ, FAIL, "can't close v2 B-tree for creation order index");
    if (oh && H5O_unprotect(loc, oh, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, FAIL, "unable to release object header");
    if (api_ctx_pushed && H5CX_pop(false) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTRESET, FAIL, "can't reset API context");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__attr_dense_info_test() */

/*--------------------------------------------------------------------------
 NAME
    H5O__check_msg_marked_test
 PURPOSE
    Check if an unknown message with the "mark if unknown" flag actually gets
    marked.
 USAGE
    herr_t H5O__check_msg_marked_test(oid, flag_val)
        hid_t oid;              IN: Object to check
        bool flag_val;       IN: Desired flag value
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Locates the "unknown" message and checks that the "was unknown" flag is set
    correctly.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    DO NOT USE THIS FUNCTION FOR ANYTHING EXCEPT TESTING
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5O__check_msg_marked_test(hid_t oid, bool flag_val)
{
    H5O_t      *oh = NULL;           /* Object header */
    H5O_loc_t  *loc;                 /* Pointer to object's location */
    H5O_mesg_t *idx_msg;             /* Pointer to message */
    unsigned    idx;                 /* Index of message */
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get object location for object */
    if (NULL == (loc = H5O_get_loc(oid)))
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "object not found");

    /* Get the object header */
    if (NULL == (oh = H5O_protect(loc, H5AC__READ_ONLY_FLAG, false)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, FAIL, "unable to load object header");

    /* Locate "unknown" message  */
    for (idx = 0, idx_msg = &oh->mesg[0]; idx < oh->nmesgs; idx++, idx_msg++)
        if (idx_msg->type->id == H5O_UNKNOWN_ID) {
            /* Check for "unknown" message having the correct flags */
            if (((idx_msg->flags & H5O_MSG_FLAG_WAS_UNKNOWN) > 0) != flag_val)
                HGOTO_ERROR(H5E_OHDR, H5E_BADVALUE, FAIL,
                            "'unknown' message has incorrect 'was unknown' flag value");

            /* Break out of loop, to indicate that the "unknown" message was found */
            break;
        } /* end if */

    /* Check for not finding an "unknown" message */
    if (idx == oh->nmesgs)
        HGOTO_ERROR(H5E_OHDR, H5E_NOTFOUND, FAIL, "'unknown' message type not found");

done:
    if (oh && H5O_unprotect(loc, oh, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, FAIL, "unable to release object header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__check_msg_marked_test() */

/*--------------------------------------------------------------------------
 NAME
    H5O__expunge_chunks_test
 PURPOSE
    Expunge all the chunks for an object header from the cache.
 USAGE
    herr_t H5O_expunge_chunks_test(loc)
        H5O_loc_t *loc;         IN: Object location for object header to expunge
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Iterates over all the chunks for an object header an expunges each from the
    metadata cache.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    DO NOT USE THIS FUNCTION FOR ANYTHING EXCEPT TESTING
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5O__expunge_chunks_test(const H5O_loc_t *loc)
{
    H5O_t  *oh = NULL;           /* Object header */
    haddr_t chk_addr[16];        /* Array of chunk addresses */
    size_t  nchunks;             /* Number of chunks in object header */
    size_t  u;                   /* Local index variable */
    herr_t  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get the object header */
    if (NULL == (oh = H5O_protect(loc, H5AC__NO_FLAGS_SET, false)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, FAIL, "unable to protect object header");

    /* Safety check */
    nchunks = oh->nchunks;
    assert(0 < nchunks && nchunks < NELMTS(chk_addr));

    /* Iterate over all the chunks, saving the chunk addresses */
    for (u = 0; u < oh->nchunks; u++)
        chk_addr[u] = oh->chunk[u].addr;

    /* Release the object header */
    if (H5O_unprotect(loc, oh, H5AC__NO_FLAGS_SET) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, FAIL, "unable to unprotect object header");

    /* Iterate over all the saved chunk addresses, evicting them from the cache */
    /* (in reverse order, so that chunk #0 is unpinned) */
    for (u = nchunks - 1; u < nchunks; u--)
        if (H5AC_expunge_entry(loc->file, (u == 0 ? H5AC_OHDR : H5AC_OHDR_CHK), chk_addr[u],
                               H5AC__NO_FLAGS_SET) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTEXPUNGE, FAIL, "unable to expunge object header chunk");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__expunge_chunks_test() */

/*--------------------------------------------------------------------------
 NAME
    H5O__get_rc_test
 PURPOSE
    Retrieve the refcount for the object header
 USAGE
    herr_t H5O__get_rc_test(loc, rc)
        const H5O_loc_t *loc;   IN: Object location for object header to query
        unsigned *rc;           OUT: Pointer to refcount for object header
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Protects object header, retrieves the object header's refcount, and
    unprotects object header.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    DO NOT USE THIS FUNCTION FOR ANYTHING EXCEPT TESTING
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5O__get_rc_test(const H5O_loc_t *loc, unsigned *rc)
{
    H5O_t *oh        = NULL;    /* Object header */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(loc);
    assert(rc);

    /* Get the object header */
    if (NULL == (oh = H5O_protect(loc, H5AC__READ_ONLY_FLAG, false)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, FAIL, "unable to protect object header");

    /* Save the refcount for the object header */
    *rc = oh->nlink;

done:
    /* Release the object header */
    if (oh && H5O_unprotect(loc, oh, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, FAIL, "unable to unprotect object header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__get_rc_test() */

/*--------------------------------------------------------------------------
 NAME
    H5O__msg_get_chunkno_test
 PURPOSE
    Retrieve the chunk number for an object header message of a given type.
 USAGE
    herr_t H5O__msg_get_chunkno_test(oid, msg_type, chunk_num)
        hid_t oid;              IN: Object to check
        unsigned msg_type;      IN: Object header message type to check
        unsigned *chunk_num;    OUT: Object header chunk that the message is in
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Retrieves the chunk number for the first object header message of a given
    type found in an object's header.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    DO NOT USE THIS FUNCTION FOR ANYTHING EXCEPT TESTING
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5O__msg_get_chunkno_test(hid_t oid, unsigned msg_type, unsigned *chunk_num)
{
    H5O_t      *oh = NULL;                /* Object header */
    H5O_loc_t  *loc;                      /* Pointer to object's location */
    H5O_mesg_t *idx_msg;                  /* Pointer to message */
    unsigned    idx;                      /* Index of message */
    bool        api_ctx_pushed = false;   /* Whether API context pushed */
    herr_t      ret_value      = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get object location for object */
    if (NULL == (loc = H5O_get_loc(oid)))
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "object not found");

    /* Set API context */
    if (H5CX_push() < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't set API context");
    api_ctx_pushed = true;

    /* Get the object header */
    if (NULL == (oh = H5O_protect(loc, H5AC__READ_ONLY_FLAG, false)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, FAIL, "unable to load object header");

    /* Locate first message of given type */
    for (idx = 0, idx_msg = &oh->mesg[0]; idx < oh->nmesgs; idx++, idx_msg++)
        if (idx_msg->type->id == msg_type) {
            /* Set the chunk number for the message */
            *chunk_num = idx_msg->chunkno;

            /* Break out of loop, to indicate that the message was found */
            break;
        } /* end if */

    /* Check for not finding a message of the given type*/
    if (idx == oh->nmesgs)
        HGOTO_ERROR(H5E_OHDR, H5E_NOTFOUND, FAIL, "message of type not found");

done:
    if (oh && H5O_unprotect(loc, oh, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, FAIL, "unable to release object header");
    if (api_ctx_pushed && H5CX_pop(false) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTRESET, FAIL, "can't reset API context");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__msg_get_chunkno_test() */

/*--------------------------------------------------------------------------
 NAME
    H5O__msg_move_to_new_chunk_test
 PURPOSE
    Move a message into a new chunk
 USAGE
    herr_t H5O__msg_move_to_new_chunk_test(oid, msg_type)
        hid_t oid;              IN: Object to check
        unsigned msg_type;      IN: Object header message type to check
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Moves the first message of the given type to a new object header chunk.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    DO NOT USE THIS FUNCTION FOR ANYTHING EXCEPT TESTING
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5O__msg_move_to_new_chunk_test(hid_t oid, unsigned msg_type)
{
    H5O_t      *oh = NULL;                /* Object header */
    H5O_loc_t  *loc;                      /* Pointer to object's location */
    H5O_mesg_t *curr_msg;                 /* Pointer to current message */
    unsigned    idx;                      /* Index of message */
    bool        api_ctx_pushed = false;   /* Whether API context pushed */
    herr_t      ret_value      = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get object location for object */
    if (NULL == (loc = H5O_get_loc(oid)))
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "object not found");

    /* Set API context */
    if (H5CX_push() < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't set API context");
    api_ctx_pushed = true;

    /* Get the object header */
    if (NULL == (oh = H5O_protect(loc, H5AC__NO_FLAGS_SET, false)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, FAIL, "unable to load object header");

    /* Locate first message of given type */
    for (idx = 0, curr_msg = &oh->mesg[0]; idx < oh->nmesgs; idx++, curr_msg++)
        if (curr_msg->type->id == msg_type) {
            H5O_msg_alloc_info_t found_msg;                       /* Information about message to move */
            unsigned             msg_chunkno = curr_msg->chunkno; /* Chunk that the message is in */
            uint8_t             *end_chunk_data =
                (oh->chunk[msg_chunkno].image + oh->chunk[msg_chunkno].size) -
                (H5O_SIZEOF_CHKSUM_OH(oh) + oh->chunk[msg_chunkno].gap); /* End of message data in chunk */
            uint8_t *end_msg    = curr_msg->raw + curr_msg->raw_size;    /* End of current message */
            size_t   gap_size   = 0; /* Size of gap after current message */
            size_t   null_size  = 0; /* Size of NULL message after current message */
            unsigned null_msgno = 0; /* Index of NULL message after current message */
            size_t   total_size;     /* Total size of available space "around" current message */
            size_t   new_idx;        /* Index of new null message */

            /* Check if the message is the last one in the chunk */
            if (end_msg == end_chunk_data)
                gap_size = oh->chunk[msg_chunkno].gap;
            else {
                H5O_mesg_t *tmp_msg; /* Temp. pointer to message to operate on */
                unsigned    v;       /* Local index variable */

                /* Check for null message after this message, in same chunk */
                for (v = 0, tmp_msg = &oh->mesg[0]; v < oh->nmesgs; v++, tmp_msg++) {
                    if (tmp_msg->type->id == H5O_NULL_ID &&
                        (tmp_msg->raw - H5O_SIZEOF_MSGHDR_OH(oh)) == end_msg) {
                        null_msgno = v;
                        null_size  = (size_t)H5O_SIZEOF_MSGHDR_OH(oh) + tmp_msg->raw_size;
                        break;
                    } /* end if */

                    /* XXX: Should also check for NULL message in front of current message... */

                } /* end for */
            }     /* end else */

            /* Add up current message's total available space */
            total_size = curr_msg->raw_size + gap_size + null_size;

            /* Set up "found message" info for moving the message */
            found_msg.msgno      = (int)idx;
            found_msg.id         = curr_msg->type->id;
            found_msg.chunkno    = msg_chunkno;
            found_msg.gap_size   = gap_size;
            found_msg.null_size  = null_size;
            found_msg.total_size = total_size;
            found_msg.null_msgno = null_msgno;

            /* Allocate and initialize new chunk in the file, moving the found message */
            /* (*new_idx returned from this routine is unused here) */
            if (H5O__alloc_chunk(loc->file, oh, (curr_msg->raw_size + (size_t)H5O_SIZEOF_MSGHDR_OH(oh)),
                                 oh->nmesgs, &found_msg, &new_idx) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTALLOC, FAIL, "can't allocate new object header chunk");

            /* Break out of loop, the message was found */
            break;
        } /* end if */

done:
    if (oh && H5O_unprotect(loc, oh, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, FAIL, "unable to release object header");
    if (api_ctx_pushed && H5CX_pop(false) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTRESET, FAIL, "can't reset API context");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O__msg_move_to_new_chunk_test() */
