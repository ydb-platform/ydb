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
 * Created:             H5ACproxy_entry.c
 *
 * Purpose:             Functions and a cache client for a "proxy" cache entry.
 *			A proxy cache entry is used as a placeholder for entire
 *			data structures to attach flush dependencies, etc.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/
#include "H5ACmodule.h" /* This source code file is part of the H5AC module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                    */
#include "H5ACpkg.h"     /* Metadata cache                       */
#include "H5Eprivate.h"  /* Error handling                       */
#include "H5MFprivate.h" /* File memory management		*/

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

/* Metadata cache (H5AC) callbacks */
static herr_t H5AC__proxy_entry_image_len(const void *thing, size_t *image_len);
static herr_t H5AC__proxy_entry_serialize(const H5F_t *f, void *image_ptr, size_t len, void *thing);
static herr_t H5AC__proxy_entry_notify(H5AC_notify_action_t action, void *thing);
static herr_t H5AC__proxy_entry_free_icr(void *thing);

/*********************/
/* Package Variables */
/*********************/

/* H5AC proxy entries inherit cache-like properties from H5AC */
const H5AC_class_t H5AC_PROXY_ENTRY[1] = {{
    H5AC_PROXY_ENTRY_ID,         /* Metadata client ID */
    "Proxy entry",               /* Metadata client name (for debugging) */
    H5FD_MEM_SUPER,              /* File space memory type for client */
    0,                           /* Client class behavior flags */
    NULL,                        /* 'get_initial_load_size' callback */
    NULL,                        /* 'get_final_load_size' callback */
    NULL,                        /* 'verify_chksum' callback */
    NULL,                        /* 'deserialize' callback */
    H5AC__proxy_entry_image_len, /* 'image_len' callback */
    NULL,                        /* 'pre_serialize' callback */
    H5AC__proxy_entry_serialize, /* 'serialize' callback */
    H5AC__proxy_entry_notify,    /* 'notify' callback */
    H5AC__proxy_entry_free_icr,  /* 'free_icr' callback */
    NULL,                        /* 'fsf_size' callback */
}};

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Declare a free list to manage H5AC_proxy_entry_t objects */
H5FL_DEFINE_STATIC(H5AC_proxy_entry_t);

/*-------------------------------------------------------------------------
 * Function:    H5AC_proxy_entry_create
 *
 * Purpose:     Create a new proxy entry
 *
 * Return:	Success:	Pointer to the new proxy entry object.
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
H5AC_proxy_entry_t *
H5AC_proxy_entry_create(void)
{
    H5AC_proxy_entry_t *pentry    = NULL; /* Pointer to new proxy entry */
    H5AC_proxy_entry_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /* Allocate new proxy entry */
    if (NULL == (pentry = H5FL_CALLOC(H5AC_proxy_entry_t)))
        HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, NULL, "can't allocate proxy entry");

    /* Set non-zero fields */
    pentry->addr = HADDR_UNDEF;

    /* Set return value */
    ret_value = pentry;

done:
    /* Release resources on error */
    if (!ret_value)
        if (pentry)
            pentry = H5FL_FREE(H5AC_proxy_entry_t, pentry);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5AC_proxy_entry_create() */

/*-------------------------------------------------------------------------
 * Function:    H5AC_proxy_entry_add_parent
 *
 * Purpose:     Add a parent to a proxy entry
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5AC_proxy_entry_add_parent(H5AC_proxy_entry_t *pentry, void *_parent)
{
    H5AC_info_t *parent    = (H5AC_info_t *)_parent; /* Parent entry's cache info */
    herr_t       ret_value = SUCCEED;                /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(parent);
    assert(pentry);

    /* Add parent to the list of parents */
    if (NULL == pentry->parents)
        if (NULL == (pentry->parents = H5SL_create(H5SL_TYPE_HADDR, NULL)))
            HGOTO_ERROR(H5E_CACHE, H5E_CANTCREATE, FAIL,
                        "unable to create skip list for parents of proxy entry");

    /* Insert parent address into skip list */
    if (H5SL_insert(pentry->parents, parent, &parent->addr) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTINSERT, FAIL, "unable to insert parent into proxy's skip list");

    /* Add flush dependency on parent */
    if (pentry->nchildren > 0) {
        /* Sanity check */
        assert(H5_addr_defined(pentry->addr));

        if (H5AC_create_flush_dependency(parent, pentry) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTDEPEND, FAIL, "unable to set flush dependency on proxy entry");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5AC_proxy_entry_add_parent() */

/*-------------------------------------------------------------------------
 * Function:    H5AC_proxy_entry_remove_parent
 *
 * Purpose:     Removes a parent from a proxy entry
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5AC_proxy_entry_remove_parent(H5AC_proxy_entry_t *pentry, void *_parent)
{
    H5AC_info_t *parent = (H5AC_info_t *)_parent; /* Pointer to the parent entry */
    H5AC_info_t *rem_parent;                      /* Pointer to the removed parent entry */
    herr_t       ret_value = SUCCEED;             /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(pentry);
    assert(pentry->parents);
    assert(parent);

    /* Remove parent from skip list */
    if (NULL == (rem_parent = (H5AC_info_t *)H5SL_remove(pentry->parents, &parent->addr)))
        HGOTO_ERROR(H5E_CACHE, H5E_CANTREMOVE, FAIL, "unable to remove proxy entry parent from skip list");
    if (!H5_addr_eq(rem_parent->addr, parent->addr))
        HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "removed proxy entry parent not the same as real parent");

    /* Shut down the skip list, if this is the last parent */
    if (0 == H5SL_count(pentry->parents)) {
        /* Sanity check */
        assert(0 == pentry->nchildren);

        if (H5SL_close(pentry->parents) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CLOSEERROR, FAIL, "can't close proxy parent skip list");
        pentry->parents = NULL;
    } /* end if */

    /* Remove flush dependency between the proxy entry and a parent */
    if (pentry->nchildren > 0)
        if (H5AC_destroy_flush_dependency(parent, pentry) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTUNDEPEND, FAIL,
                        "unable to remove flush dependency on proxy entry");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5AC_proxy_entry_remove_parent() */

/*-------------------------------------------------------------------------
 * Function:	H5AC__proxy_entry_add_child_cb
 *
 * Purpose:	Callback routine for adding an entry as a flush dependency for
 *		a proxy entry.
 *
 * Return:	Success:	Non-negative on success
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static int
H5AC__proxy_entry_add_child_cb(void *_item, void H5_ATTR_UNUSED *_key, void *_udata)
{
    H5AC_info_t        *parent    = (H5AC_info_t *)_item;         /* Pointer to the parent entry */
    H5AC_proxy_entry_t *pentry    = (H5AC_proxy_entry_t *)_udata; /* Pointer to the proxy entry */
    int                 ret_value = H5_ITER_CONT;                 /* Callback return value */

    FUNC_ENTER_PACKAGE

    /* Add flush dependency on parent for proxy entry */
    if (H5AC_create_flush_dependency(parent, pentry) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTDEPEND, H5_ITER_ERROR,
                    "unable to set flush dependency for virtual entry");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5AC__proxy_entry_add_child_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5AC_proxy_entry_add_child
 *
 * Purpose:     Add a child a proxy entry
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5AC_proxy_entry_add_child(H5AC_proxy_entry_t *pentry, H5F_t *f, void *child)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(pentry);
    assert(child);

    /* Check for first child */
    if (0 == pentry->nchildren) {
        /* Get an address, if the proxy doesn't already have one */
        if (!H5_addr_defined(pentry->addr))
            if (HADDR_UNDEF == (pentry->addr = H5MF_alloc_tmp(f, 1)))
                HGOTO_ERROR(H5E_CACHE, H5E_CANTALLOC, FAIL,
                            "temporary file space allocation failed for proxy entry");

        /* Insert the proxy entry into the cache */
        if (H5AC_insert_entry(f, H5AC_PROXY_ENTRY, pentry->addr, pentry, H5AC__PIN_ENTRY_FLAG) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTINSERT, FAIL, "unable to cache proxy entry");

        /* Proxies start out clean (insertions are automatically marked dirty) */
        if (H5AC_mark_entry_clean(pentry) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTCLEAN, FAIL, "can't mark proxy entry clean");

        /* Proxies start out serialized (insertions are automatically marked unserialized) */
        if (H5AC_mark_entry_serialized(pentry) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTSERIALIZE, FAIL, "can't mark proxy entry clean");

        /* If there are currently parents, iterate over the list of parents, creating flush dependency on them
         */
        if (pentry->parents)
            if (H5SL_iterate(pentry->parents, H5AC__proxy_entry_add_child_cb, pentry) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_BADITER, FAIL, "can't visit parents");
    } /* end if */

    /* Add flush dependency on proxy entry */
    if (H5AC_create_flush_dependency(pentry, child) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTDEPEND, FAIL, "unable to set flush dependency on proxy entry");

    /* Increment count of children */
    pentry->nchildren++;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5AC_proxy_entry_add_child() */

/*-------------------------------------------------------------------------
 * Function:	H5AC__proxy_entry_remove_child_cb
 *
 * Purpose:	Callback routine for removing an entry as a flush dependency for
 *		proxy entry.
 *
 * Return:	Success:	Non-negative on success
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
static int
H5AC__proxy_entry_remove_child_cb(void *_item, void H5_ATTR_UNUSED *_key, void *_udata)
{
    H5AC_info_t        *parent    = (H5AC_info_t *)_item;         /* Pointer to the parent entry */
    H5AC_proxy_entry_t *pentry    = (H5AC_proxy_entry_t *)_udata; /* Pointer to the proxy entry */
    int                 ret_value = H5_ITER_CONT;                 /* Callback return value */

    FUNC_ENTER_PACKAGE

    /* Remove flush dependency on parent for proxy entry */
    if (H5AC_destroy_flush_dependency(parent, pentry) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTUNDEPEND, H5_ITER_ERROR,
                    "unable to remove flush dependency for proxy entry");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5AC__proxy_entry_remove_child_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5AC_proxy_entry_remove_child
 *
 * Purpose:     Remove a child a proxy entry
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5AC_proxy_entry_remove_child(H5AC_proxy_entry_t *pentry, void *child)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(pentry);
    assert(child);

    /* Remove flush dependency on proxy entry */
    if (H5AC_destroy_flush_dependency(pentry, child) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTUNDEPEND, FAIL, "unable to remove flush dependency on proxy entry");

    /* Decrement count of children */
    pentry->nchildren--;

    /* Check for last child */
    if (0 == pentry->nchildren) {
        /* Check for flush dependencies on proxy's parents */
        if (pentry->parents)
            /* Iterate over the list of parents, removing flush dependency on them */
            if (H5SL_iterate(pentry->parents, H5AC__proxy_entry_remove_child_cb, pentry) < 0)
                HGOTO_ERROR(H5E_CACHE, H5E_BADITER, FAIL, "can't visit parents");

        /* Unpin proxy */
        if (H5AC_unpin_entry(pentry) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTUNPIN, FAIL, "can't unpin proxy entry");

        /* Remove proxy entry from cache */
        if (H5AC_remove_entry(pentry) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_CANTREMOVE, FAIL, "unable to remove proxy entry");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5AC_proxy_entry_remove_child() */

/*-------------------------------------------------------------------------
 * Function:    H5AC_proxy_entry_dest
 *
 * Purpose:     Destroys a proxy entry in memory.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5AC_proxy_entry_dest(H5AC_proxy_entry_t *pentry)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    /* Sanity checks */
    assert(pentry);
    assert(NULL == pentry->parents);
    assert(0 == pentry->nchildren);
    assert(0 == pentry->ndirty_children);
    assert(0 == pentry->nunser_children);

    /* Free the proxy entry object */
    pentry = H5FL_FREE(H5AC_proxy_entry_t, pentry);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5AC_proxy_entry_dest() */

/*-------------------------------------------------------------------------
 * Function:    H5AC__proxy_entry_image_len
 *
 * Purpose:     Compute the size of the data structure on disk.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5AC__proxy_entry_image_len(const void H5_ATTR_UNUSED *thing, size_t *image_len)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(image_len);

    /* Set the image length size to 1 byte */
    *image_len = 1;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5AC__proxy_entry_image_len() */

/*-------------------------------------------------------------------------
 * Function:    H5AC__proxy_entry_serialize
 *
 * Purpose:	Serializes a data structure for writing to disk.
 *
 * Note:	Should never be invoked.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5AC__proxy_entry_serialize(const H5F_t H5_ATTR_UNUSED *f, void H5_ATTR_UNUSED *image,
                            size_t H5_ATTR_UNUSED len, void H5_ATTR_UNUSED *thing)
{
    FUNC_ENTER_PACKAGE_NOERR /* Yes, even though this pushes an error on the stack */

        /* Should never be invoked */
        assert(0 && "Invalid callback?!?");

    HERROR(H5E_CACHE, H5E_CANTSERIALIZE, "called unreachable fcn.");

    FUNC_LEAVE_NOAPI(FAIL)
} /* end H5AC__proxy_entry_serialize() */

/*-------------------------------------------------------------------------
 * Function:    H5AC__proxy_entry_notify
 *
 * Purpose:     Handle cache action notifications
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5AC__proxy_entry_notify(H5AC_notify_action_t action, void *_thing)
{
    H5AC_proxy_entry_t *pentry    = (H5AC_proxy_entry_t *)_thing;
    herr_t              ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(pentry);

    switch (action) {
        case H5AC_NOTIFY_ACTION_AFTER_INSERT:
            break;

        case H5AC_NOTIFY_ACTION_AFTER_LOAD:
#ifdef NDEBUG
            HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "invalid notify action from metadata cache");
#else  /* NDEBUG */
            assert(0 && "Invalid action?!?");
#endif /* NDEBUG */
            break;

        case H5AC_NOTIFY_ACTION_AFTER_FLUSH:
#ifdef NDEBUG
            HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "invalid notify action from metadata cache");
#else  /* NDEBUG */
            assert(0 && "Invalid action?!?");
#endif /* NDEBUG */
            break;

        case H5AC_NOTIFY_ACTION_BEFORE_EVICT:
            /* Sanity checks */
            assert(0 == pentry->ndirty_children);
            assert(0 == pentry->nunser_children);

            /* No action */
            break;

        case H5AC_NOTIFY_ACTION_ENTRY_DIRTIED:
            /* Sanity checks */
            assert(pentry->ndirty_children > 0);

            /* No action */
            break;

        case H5AC_NOTIFY_ACTION_ENTRY_CLEANED:
            /* Sanity checks */
            assert(0 == pentry->ndirty_children);

            /* No action */
            break;

        case H5AC_NOTIFY_ACTION_CHILD_DIRTIED:
            /* Increment # of dirty children */
            pentry->ndirty_children++;

            /* Check for first dirty child */
            if (1 == pentry->ndirty_children)
                if (H5AC_mark_entry_dirty(pentry) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTDIRTY, FAIL, "can't mark proxy entry dirty");
            break;

        case H5AC_NOTIFY_ACTION_CHILD_CLEANED:
            /* Sanity check */
            assert(pentry->ndirty_children > 0);

            /* Decrement # of dirty children */
            pentry->ndirty_children--;

            /* Check for last dirty child */
            if (0 == pentry->ndirty_children)
                if (H5AC_mark_entry_clean(pentry) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTCLEAN, FAIL, "can't mark proxy entry clean");
            break;

        case H5AC_NOTIFY_ACTION_CHILD_UNSERIALIZED:
            /* Increment # of unserialized children */
            pentry->nunser_children++;

            /* Check for first unserialized child */
            if (1 == pentry->nunser_children)
                if (H5AC_mark_entry_unserialized(pentry) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTUNSERIALIZE, FAIL, "can't mark proxy entry unserialized");
            break;

        case H5AC_NOTIFY_ACTION_CHILD_SERIALIZED:
            /* Sanity check */
            assert(pentry->nunser_children > 0);

            /* Decrement # of unserialized children */
            pentry->nunser_children--;

            /* Check for last unserialized child */
            if (0 == pentry->nunser_children)
                if (H5AC_mark_entry_serialized(pentry) < 0)
                    HGOTO_ERROR(H5E_CACHE, H5E_CANTSERIALIZE, FAIL, "can't mark proxy entry serialized");
            break;

        default:
#ifdef NDEBUG
            HGOTO_ERROR(H5E_CACHE, H5E_BADVALUE, FAIL, "unknown notify action from metadata cache");
#else  /* NDEBUG */
            assert(0 && "Unknown action?!?");
#endif /* NDEBUG */
    }  /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5AC__proxy_entry_notify() */

/*-------------------------------------------------------------------------
 * Function:	H5AC__proxy_entry_free_icr
 *
 * Purpose:	Destroy/release an "in core representation" of a data
 *              structure
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5AC__proxy_entry_free_icr(void *_thing)
{
    H5AC_proxy_entry_t *pentry    = (H5AC_proxy_entry_t *)_thing;
    herr_t              ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Destroy the proxy entry */
    if (H5AC_proxy_entry_dest(pentry) < 0)
        HGOTO_ERROR(H5E_CACHE, H5E_CANTFREE, FAIL, "unable to destroy proxy entry");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5AC__proxy_entry_free_icr() */
