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
 * Created:     H5FAdblkpage.c
 *
 * Purpose:     Data block page routines for fixed array.
 *
 *-------------------------------------------------------------------------
 */

/**********************/
/* Module Declaration */
/**********************/

#include "H5FAmodule.h" /* This source code file is part of the H5FA module */

/***********************/
/* Other Packages Used */
/***********************/

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5FApkg.h"     /* Fixed Arrays                             */
#include "H5FLprivate.h" /* Free Lists                               */

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

/* Declare a free list to manage the H5FA_dblk_page_t struct */
H5FL_DEFINE_STATIC(H5FA_dblk_page_t);

/* Declare a free list to manage the page elements */
H5FL_BLK_DEFINE(page_elmts);

/*-------------------------------------------------------------------------
 * Function:    H5FA__dblk_page_alloc
 *
 * Purpose:     Allocate fixed array data block page
 *
 * Return:      Non-NULL pointer to data block on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5FA_dblk_page_t *
H5FA__dblk_page_alloc(H5FA_hdr_t *hdr, size_t nelmts)
{
    H5FA_dblk_page_t *dblk_page = NULL; /* Fixed array data block page */
    H5FA_dblk_page_t *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(hdr);

    /* Allocate memory for the data block */
    if (NULL == (dblk_page = H5FL_CALLOC(H5FA_dblk_page_t)))
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTALLOC, NULL,
                    "memory allocation failed for fixed array data block page");

    /* Share common array information */
    if (H5FA__hdr_incr(hdr) < 0)
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTINC, NULL, "can't increment reference count on shared array header");
    dblk_page->hdr = hdr;

    /* Set non-zero internal fields */
    dblk_page->nelmts = nelmts;

    /* Allocate buffer for elements in data block page */
    if (NULL == (dblk_page->elmts = H5FL_BLK_MALLOC(page_elmts, nelmts * hdr->cparam.cls->nat_elmt_size)))
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTALLOC, NULL,
                    "memory allocation failed for data block page element buffer");

    /* Set the return value */
    ret_value = dblk_page;

done:

    if (!ret_value)
        if (dblk_page && H5FA__dblk_page_dest(dblk_page) < 0)
            HDONE_ERROR(H5E_FARRAY, H5E_CANTFREE, NULL, "unable to destroy fixed array data block page");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FA__dblk_page_alloc() */

/*-------------------------------------------------------------------------
 * Function:    H5FA__dblk_page_create
 *
 * Purpose:     Creates a new fixed array data block page in the file
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FA__dblk_page_create(H5FA_hdr_t *hdr, haddr_t addr, size_t nelmts)
{
    H5FA_dblk_page_t *dblk_page = NULL;  /* Fixed array data block page */
    bool              inserted  = false; /* Whether the header was inserted into cache */
    herr_t            ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

#ifdef H5FA_DEBUG
    fprintf(stderr, "%s: Called, addr = %" PRIuHADDR "\n", __func__, addr);
#endif /* H5FA_DEBUG */

    /* Sanity check */
    assert(hdr);

    /* Allocate the data block page */
    if (NULL == (dblk_page = H5FA__dblk_page_alloc(hdr, nelmts)))
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTALLOC, FAIL,
                    "memory allocation failed for fixed array data block page");

    /* Set info about data block page on disk */
    dblk_page->addr = addr;
    dblk_page->size = H5FA_DBLK_PAGE_SIZE(hdr, nelmts);
#ifdef H5FA_DEBUG
    fprintf(stderr, "%s: dblk_page->size = %Zu\n", __func__, dblk_page->size);
#endif /* H5FA_DEBUG */

    /* Clear any elements in data block page to fill value */
    if ((hdr->cparam.cls->fill)(dblk_page->elmts, nelmts) < 0)
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTSET, FAIL,
                    "can't set fixed array data block page elements to class's fill value");

    /* Cache the new fixed array data block page */
    if (H5AC_insert_entry(hdr->f, H5AC_FARRAY_DBLK_PAGE, dblk_page->addr, dblk_page, H5AC__NO_FLAGS_SET) < 0)
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTINSERT, FAIL, "can't add fixed array data block page to cache");
    inserted = true;

    /* Add data block page as child of 'top' proxy */
    if (hdr->top_proxy) {
        if (H5AC_proxy_entry_add_child(hdr->top_proxy, hdr->f, dblk_page) < 0)
            HGOTO_ERROR(H5E_FARRAY, H5E_CANTSET, FAIL,
                        "unable to add fixed array entry as child of array proxy");
        dblk_page->top_proxy = hdr->top_proxy;
    } /* end if */

done:
    if (ret_value < 0)
        if (dblk_page) {
            /* Remove from cache, if inserted */
            if (inserted)
                if (H5AC_remove_entry(dblk_page) < 0)
                    HDONE_ERROR(H5E_FARRAY, H5E_CANTREMOVE, FAIL,
                                "unable to remove fixed array data block page from cache");

            /* Destroy data block page */
            if (H5FA__dblk_page_dest(dblk_page) < 0)
                HDONE_ERROR(H5E_FARRAY, H5E_CANTFREE, FAIL, "unable to destroy fixed array data block page");
        } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FA__dblk_page_create() */

/*-------------------------------------------------------------------------
 * Function:    H5FA__dblk_page_protect
 *
 * Purpose:     Convenience wrapper around protecting fixed array data
 *              block page
 *
 * Return:      Non-NULL pointer to data block page on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5FA_dblk_page_t *
H5FA__dblk_page_protect(H5FA_hdr_t *hdr, haddr_t dblk_page_addr, size_t dblk_page_nelmts, unsigned flags)
{
    H5FA_dblk_page_t         *dblk_page = NULL; /* Fixed array data block page */
    H5FA_dblk_page_cache_ud_t udata;            /* Information needed for loading data block page */
    H5FA_dblk_page_t         *ret_value = NULL;

    FUNC_ENTER_PACKAGE

#ifdef H5FA_DEBUG
    fprintf(stderr, "%s: Called\n", __func__);
#endif /* H5FA_DEBUG */

    /* Sanity check */
    assert(hdr);
    assert(H5_addr_defined(dblk_page_addr));

    /* only the H5AC__READ_ONLY_FLAG is permitted */
    assert((flags & (unsigned)(~H5AC__READ_ONLY_FLAG)) == 0);

    /* Set up user data */
    udata.hdr            = hdr;
    udata.nelmts         = dblk_page_nelmts;
    udata.dblk_page_addr = dblk_page_addr;

    /* Protect the data block page */
    if (NULL == (dblk_page = (H5FA_dblk_page_t *)H5AC_protect(hdr->f, H5AC_FARRAY_DBLK_PAGE, dblk_page_addr,
                                                              &udata, flags)))
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTPROTECT, NULL,
                    "unable to protect fixed array data block page, address = %llu",
                    (unsigned long long)dblk_page_addr);

    /* Create top proxy, if it doesn't exist */
    if (hdr->top_proxy && NULL == dblk_page->top_proxy) {
        /* Add data block page as child of 'top' proxy */
        if (H5AC_proxy_entry_add_child(hdr->top_proxy, hdr->f, dblk_page) < 0)
            HGOTO_ERROR(H5E_FARRAY, H5E_CANTSET, NULL,
                        "unable to add fixed array entry as child of array proxy");
        dblk_page->top_proxy = hdr->top_proxy;
    } /* end if */

    /* Set return value */
    ret_value = dblk_page;

done:

    /* Clean up on error */
    if (!ret_value) {
        /* Release the data block page, if it was protected */
        if (dblk_page &&
            H5AC_unprotect(hdr->f, H5AC_FARRAY_DBLK_PAGE, dblk_page->addr, dblk_page, H5AC__NO_FLAGS_SET) < 0)
            HDONE_ERROR(H5E_FARRAY, H5E_CANTUNPROTECT, NULL,
                        "unable to unprotect fixed array data block page, address = %llu",
                        (unsigned long long)dblk_page->addr);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FA__dblk_page_protect() */

/*-------------------------------------------------------------------------
 * Function:    H5FA__dblk_page_unprotect
 *
 * Purpose:     Convenience wrapper around unprotecting fixed array
 *              data block page
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FA__dblk_page_unprotect(H5FA_dblk_page_t *dblk_page, unsigned cache_flags)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

#ifdef H5FA_DEBUG
    fprintf(stderr, "%s: Called\n", __func__);
#endif /* H5FA_DEBUG */

    /* Sanity check */
    assert(dblk_page);

    /* Unprotect the data block page */
    if (H5AC_unprotect(dblk_page->hdr->f, H5AC_FARRAY_DBLK_PAGE, dblk_page->addr, dblk_page, cache_flags) < 0)
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTUNPROTECT, FAIL,
                    "unable to unprotect fixed array data block page, address = %llu",
                    (unsigned long long)dblk_page->addr);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FA__dblk_page_unprotect() */

/*-------------------------------------------------------------------------
 * Function:    H5FA__dblk_page_dest
 *
 * Purpose:     Destroys a fixed array data block page in memory.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FA__dblk_page_dest(H5FA_dblk_page_t *dblk_page)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(dblk_page);

    /* Check if header field has been initialized */
    if (dblk_page->hdr) {
        /* Check if buffer for data block page elements has been initialized */
        if (dblk_page->elmts) {
            /* Free buffer for data block page elements */
            dblk_page->elmts = H5FL_BLK_FREE(page_elmts, dblk_page->elmts);
        } /* end if */

        /* Decrement reference count on shared info */
        if (H5FA__hdr_decr(dblk_page->hdr) < 0)
            HGOTO_ERROR(H5E_FARRAY, H5E_CANTDEC, FAIL,
                        "can't decrement reference count on shared array header");
        dblk_page->hdr = NULL;
    } /* end if */

    /* Sanity check */
    assert(NULL == dblk_page->top_proxy);

    /* Free the data block page itself */
    dblk_page = H5FL_FREE(H5FA_dblk_page_t, dblk_page);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FA__dblk_page_dest() */
