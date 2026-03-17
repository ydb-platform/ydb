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
 * Created:		H5EAdblkpage.c
 *
 * Purpose:		Data block page routines for extensible arrays.
 *
 *-------------------------------------------------------------------------
 */

/**********************/
/* Module Declaration */
/**********************/

#include "H5EAmodule.h" /* This source code file is part of the H5EA module */

/***********************/
/* Other Packages Used */
/***********************/

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5EApkg.h"     /* Extensible Arrays			*/
#include "H5FLprivate.h" /* Free Lists                           */
#include "H5VMprivate.h" /* Vectors and arrays 			*/

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

/* Declare a free list to manage the H5EA_dblk_page_t struct */
H5FL_DEFINE_STATIC(H5EA_dblk_page_t);

/*-------------------------------------------------------------------------
 * Function:	H5EA__dblk_page_alloc
 *
 * Purpose:	Allocate extensible array data block page
 *
 * Return:	Non-NULL pointer to data block on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5EA_dblk_page_t *
H5EA__dblk_page_alloc(H5EA_hdr_t *hdr, H5EA_sblock_t *parent)
{
    H5EA_dblk_page_t *dblk_page = NULL; /* Extensible array data block page */
    H5EA_dblk_page_t *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(hdr);

    /* Allocate memory for the data block */
    if (NULL == (dblk_page = H5FL_CALLOC(H5EA_dblk_page_t)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTALLOC, NULL,
                    "memory allocation failed for extensible array data block page");

    /* Share common array information */
    if (H5EA__hdr_incr(hdr) < 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTINC, NULL, "can't increment reference count on shared array header");
    dblk_page->hdr = hdr;

    /* Set non-zero internal fields */
    dblk_page->parent = parent;

    /* Allocate buffer for elements in data block page */
    if (NULL == (dblk_page->elmts = H5EA__hdr_alloc_elmts(hdr, hdr->dblk_page_nelmts)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTALLOC, NULL,
                    "memory allocation failed for data block page element buffer");

    /* Set the return value */
    ret_value = dblk_page;

done:
    if (!ret_value)
        if (dblk_page && H5EA__dblk_page_dest(dblk_page) < 0)
            HDONE_ERROR(H5E_EARRAY, H5E_CANTFREE, NULL, "unable to destroy extensible array data block page");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__dblk_page_alloc() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__dblk_page_create
 *
 * Purpose:	Creates a new extensible array data block page in the file
 *
 * Return:	Valid file address on success/HADDR_UNDEF on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5EA__dblk_page_create(H5EA_hdr_t *hdr, H5EA_sblock_t *parent, haddr_t addr)
{
    H5EA_dblk_page_t *dblk_page = NULL;  /* Extensible array data block page */
    bool              inserted  = false; /* Whether the header was inserted into cache */
    herr_t            ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(hdr);

    /* Allocate the data block page */
    if (NULL == (dblk_page = H5EA__dblk_page_alloc(hdr, parent)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTALLOC, FAIL,
                    "memory allocation failed for extensible array data block page");

    /* Set info about data block page on disk */
    dblk_page->addr = addr;
    dblk_page->size = H5EA_DBLK_PAGE_SIZE(hdr);

    /* Clear any elements in data block page to fill value */
    if ((hdr->cparam.cls->fill)(dblk_page->elmts, (size_t)hdr->dblk_page_nelmts) < 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTSET, FAIL,
                    "can't set extensible array data block page elements to class's fill value");

    /* Cache the new extensible array data block page */
    if (H5AC_insert_entry(hdr->f, H5AC_EARRAY_DBLK_PAGE, dblk_page->addr, dblk_page, H5AC__NO_FLAGS_SET) < 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTINSERT, FAIL, "can't add extensible array data block page to cache");
    inserted = true;

    /* Add data block page as child of 'top' proxy */
    if (hdr->top_proxy) {
        if (H5AC_proxy_entry_add_child(hdr->top_proxy, hdr->f, dblk_page) < 0)
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTSET, FAIL,
                        "unable to add extensible array entry as child of array proxy");
        dblk_page->top_proxy = hdr->top_proxy;
    } /* end if */

done:
    if (ret_value < 0)
        if (dblk_page) {
            /* Remove from cache, if inserted */
            if (inserted)
                if (H5AC_remove_entry(dblk_page) < 0)
                    HDONE_ERROR(H5E_EARRAY, H5E_CANTREMOVE, FAIL,
                                "unable to remove extensible array data block page from cache");

            /* Destroy data block page */
            if (H5EA__dblk_page_dest(dblk_page) < 0)
                HDONE_ERROR(H5E_EARRAY, H5E_CANTFREE, FAIL,
                            "unable to destroy extensible array data block page");
        } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__dblk_page_create() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__dblk_page_protect
 *
 * Purpose:	Convenience wrapper around protecting extensible array data
 *              block page
 *
 * Return:	Non-NULL pointer to data block page on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5EA_dblk_page_t *
H5EA__dblk_page_protect(H5EA_hdr_t *hdr, H5EA_sblock_t *parent, haddr_t dblk_page_addr, unsigned flags)
{
    H5EA_dblk_page_t         *dblk_page = NULL; /* Extensible array data block page */
    H5EA_dblk_page_cache_ud_t udata;            /* Information needed for loading data block page */
    H5EA_dblk_page_t         *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(hdr);
    assert(H5_addr_defined(dblk_page_addr));

    /* only the H5AC__READ_ONLY_FLAG may be set */
    assert((flags & (unsigned)(~H5AC__READ_ONLY_FLAG)) == 0);

    /* Set up user data */
    udata.hdr            = hdr;
    udata.parent         = parent;
    udata.dblk_page_addr = dblk_page_addr;

    /* Protect the data block page */
    if (NULL == (dblk_page = (H5EA_dblk_page_t *)H5AC_protect(hdr->f, H5AC_EARRAY_DBLK_PAGE, dblk_page_addr,
                                                              &udata, flags)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTPROTECT, NULL,
                    "unable to protect extensible array data block page, address = %llu",
                    (unsigned long long)dblk_page_addr);

    /* Create top proxy, if it doesn't exist */
    if (hdr->top_proxy && NULL == dblk_page->top_proxy) {
        /* Add data block page as child of 'top' proxy */
        if (H5AC_proxy_entry_add_child(hdr->top_proxy, hdr->f, dblk_page) < 0)
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTSET, NULL,
                        "unable to add extensible array entry as child of array proxy");
        dblk_page->top_proxy = hdr->top_proxy;
    } /* end if */

    /* Set return value */
    ret_value = dblk_page;

done:
    /* Clean up on error */
    if (!ret_value) {
        /* Release the data block page, if it was protected */
        if (dblk_page &&
            H5AC_unprotect(hdr->f, H5AC_EARRAY_DBLK_PAGE, dblk_page->addr, dblk_page, H5AC__NO_FLAGS_SET) < 0)
            HDONE_ERROR(H5E_EARRAY, H5E_CANTUNPROTECT, NULL,
                        "unable to unprotect extensible array data block page, address = %llu",
                        (unsigned long long)dblk_page->addr);
    } /* end if */
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__dblk_page_protect() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__dblk_page_unprotect
 *
 * Purpose:	Convenience wrapper around unprotecting extensible array
 *              data block page
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5EA__dblk_page_unprotect(H5EA_dblk_page_t *dblk_page, unsigned cache_flags)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(dblk_page);

    /* Unprotect the data block page */
    if (H5AC_unprotect(dblk_page->hdr->f, H5AC_EARRAY_DBLK_PAGE, dblk_page->addr, dblk_page, cache_flags) < 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTUNPROTECT, FAIL,
                    "unable to unprotect extensible array data block page, address = %llu",
                    (unsigned long long)dblk_page->addr);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__dblk_page_unprotect() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__dblk_page_dest
 *
 * Purpose:	Destroys an extensible array data block page in memory.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5EA__dblk_page_dest(H5EA_dblk_page_t *dblk_page)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(dblk_page);
    assert(!dblk_page->has_hdr_depend);

    /* Check if header field has been initialized */
    if (dblk_page->hdr) {
        /* Check if buffer for data block page elements has been initialized */
        if (dblk_page->elmts) {
            /* Free buffer for data block page elements */
            if (H5EA__hdr_free_elmts(dblk_page->hdr, dblk_page->hdr->dblk_page_nelmts, dblk_page->elmts) < 0)
                HGOTO_ERROR(H5E_EARRAY, H5E_CANTFREE, FAIL,
                            "unable to free extensible array data block element buffer");
            dblk_page->elmts = NULL;
        } /* end if */

        /* Decrement reference count on shared info */
        if (H5EA__hdr_decr(dblk_page->hdr) < 0)
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTDEC, FAIL,
                        "can't decrement reference count on shared array header");
        dblk_page->hdr = NULL;
    } /* end if */

    /* Sanity check */
    assert(NULL == dblk_page->top_proxy);

    /* Free the data block page itself */
    dblk_page = H5FL_FREE(H5EA_dblk_page_t, dblk_page);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__dblk_page_dest() */
