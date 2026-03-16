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
 * Created:     H5FAdblock.c
 *
 * Purpose:     Data block routines for fixed arrays.
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
#include "H5MFprivate.h" /* File memory management                   */

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

/* Declare a free list to manage the H5FA_dblock_t struct */
H5FL_DEFINE_STATIC(H5FA_dblock_t);

/* Declare a free list to manage the chunk elements */
H5FL_BLK_DEFINE(chunk_elmts);

/* Declare a free list to manage blocks of 'page init' bitmasks */
H5FL_BLK_DEFINE(fa_page_init);

/*-------------------------------------------------------------------------
 * Function:    H5FA__dblock_alloc
 *
 * Purpose:     Allocate fixed array data block
 *
 * Return:      Non-NULL pointer to data block on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5FA_dblock_t *
H5FA__dblock_alloc(H5FA_hdr_t *hdr)
{
    H5FA_dblock_t *dblock    = NULL; /* fixed array data block */
    H5FA_dblock_t *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(hdr);
    assert(hdr->cparam.nelmts > 0);

    /* Allocate memory for the data block */
    if (NULL == (dblock = H5FL_CALLOC(H5FA_dblock_t)))
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTALLOC, NULL, "memory allocation failed for fixed array data block");

    /* Share common array information */
    if (H5FA__hdr_incr(hdr) < 0)
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTINC, NULL, "can't increment reference count on shared array header");
    dblock->hdr = hdr;

    /* Set non-zero internal fields */
    dblock->dblk_page_nelmts = (size_t)1 << hdr->cparam.max_dblk_page_nelmts_bits;

    /* Check if this data block should be paged */
    if (hdr->cparam.nelmts > dblock->dblk_page_nelmts) {
        /* Compute number of pages */
        hsize_t npages = ((hdr->cparam.nelmts + dblock->dblk_page_nelmts) - 1) / dblock->dblk_page_nelmts;

        /* Safely assign the number of pages */
        H5_CHECKED_ASSIGN(dblock->npages, size_t, npages, hsize_t);

        /* Sanity check that we have at least 1 page */
        assert(dblock->npages > 0);

        /* Compute size of 'page init' flag array, in bytes */
        dblock->dblk_page_init_size = (dblock->npages + 7) / 8;
        assert(dblock->dblk_page_init_size > 0);

        /* Allocate space for 'page init' flags */
        if (NULL == (dblock->dblk_page_init = H5FL_BLK_CALLOC(fa_page_init, dblock->dblk_page_init_size)))
            HGOTO_ERROR(H5E_FARRAY, H5E_CANTALLOC, NULL, "memory allocation failed for page init bitmask");

        /* Compute data block page size */
        dblock->dblk_page_size = (dblock->dblk_page_nelmts * hdr->cparam.raw_elmt_size) + H5FA_SIZEOF_CHKSUM;

        /* Compute the # of elements on last page */
        if (0 == hdr->cparam.nelmts % dblock->dblk_page_nelmts)
            dblock->last_page_nelmts = dblock->dblk_page_nelmts;
        else
            dblock->last_page_nelmts = (size_t)(hdr->cparam.nelmts % dblock->dblk_page_nelmts);
    } /* end if */
    else {
        hsize_t dblk_size = hdr->cparam.nelmts * hdr->cparam.cls->nat_elmt_size;

        /* Allocate buffer for elements in data block */
        H5_CHECK_OVERFLOW(dblk_size, /* From: */ hsize_t, /* To: */ size_t);
        if (NULL == (dblock->elmts = H5FL_BLK_MALLOC(chunk_elmts, (size_t)dblk_size)))
            HGOTO_ERROR(H5E_FARRAY, H5E_CANTALLOC, NULL,
                        "memory allocation failed for data block element buffer");
    } /* end else */

    /* Set the return value */
    ret_value = dblock;

done:
    if (!ret_value)
        if (dblock && H5FA__dblock_dest(dblock) < 0)
            HDONE_ERROR(H5E_FARRAY, H5E_CANTFREE, NULL, "unable to destroy fixed array data block");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FA__dblock_alloc() */

/*-------------------------------------------------------------------------
 * Function:    H5FA__dblock_create
 *
 * Purpose:     Creates a fixed array data block in the file
 *
 * Return:      Valid file address on success/HADDR_UNDEF on failure
 *
 *-------------------------------------------------------------------------
 */
haddr_t
H5FA__dblock_create(H5FA_hdr_t *hdr, bool *hdr_dirty)
{
    H5FA_dblock_t *dblock = NULL;     /* Fixed array data block */
    haddr_t        dblock_addr;       /* Fixed array data block address */
    bool           inserted  = false; /* Whether the header was inserted into cache */
    haddr_t        ret_value = HADDR_UNDEF;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(hdr);
    assert(hdr_dirty);

    /* Allocate the data block */
    if (NULL == (dblock = H5FA__dblock_alloc(hdr)))
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTALLOC, HADDR_UNDEF,
                    "memory allocation failed for fixed array data block");

    /* Set size of data block on disk */
    hdr->stats.dblk_size = dblock->size = H5FA_DBLOCK_SIZE(dblock);

    /* Allocate space for the data block on disk */
    if (HADDR_UNDEF == (dblock_addr = H5MF_alloc(hdr->f, H5FD_MEM_FARRAY_DBLOCK, (hsize_t)dblock->size)))
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTALLOC, HADDR_UNDEF,
                    "file allocation failed for fixed array data block");
    dblock->addr = dblock_addr;

    /* Don't initialize elements if paged */
    if (!dblock->npages)
        /* Clear any elements in data block to fill value */
        if ((hdr->cparam.cls->fill)(dblock->elmts, (size_t)hdr->cparam.nelmts) < 0)
            HGOTO_ERROR(H5E_FARRAY, H5E_CANTSET, HADDR_UNDEF,
                        "can't set fixed array data block elements to class's fill value");

    /* Cache the new fixed array data block */
    if (H5AC_insert_entry(hdr->f, H5AC_FARRAY_DBLOCK, dblock_addr, dblock, H5AC__NO_FLAGS_SET) < 0)
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTINSERT, HADDR_UNDEF, "can't add fixed array data block to cache");
    inserted = true;

    /* Add data block as child of 'top' proxy */
    if (hdr->top_proxy) {
        if (H5AC_proxy_entry_add_child(hdr->top_proxy, hdr->f, dblock) < 0)
            HGOTO_ERROR(H5E_FARRAY, H5E_CANTSET, HADDR_UNDEF,
                        "unable to add fixed array entry as child of array proxy");
        dblock->top_proxy = hdr->top_proxy;
    } /* end if */

    /* Mark the header dirty (for updating statistics) */
    *hdr_dirty = true;

    /* Set address of data block to return */
    ret_value = dblock_addr;

done:

    if (!H5_addr_defined(ret_value))
        if (dblock) {
            /* Remove from cache, if inserted */
            if (inserted)
                if (H5AC_remove_entry(dblock) < 0)
                    HDONE_ERROR(H5E_FARRAY, H5E_CANTREMOVE, HADDR_UNDEF,
                                "unable to remove fixed array data block from cache");

            /* Release data block's disk space */
            if (H5_addr_defined(dblock->addr) &&
                H5MF_xfree(hdr->f, H5FD_MEM_FARRAY_DBLOCK, dblock->addr, (hsize_t)dblock->size) < 0)
                HDONE_ERROR(H5E_FARRAY, H5E_CANTFREE, HADDR_UNDEF,
                            "unable to release fixed array data block");

            /* Destroy data block */
            if (H5FA__dblock_dest(dblock) < 0)
                HDONE_ERROR(H5E_FARRAY, H5E_CANTFREE, HADDR_UNDEF,
                            "unable to destroy fixed array data block");
        } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FA__dblock_create() */

/*-------------------------------------------------------------------------
 * Function:    H5FA__dblock_protect
 *
 * Purpose:     Convenience wrapper around protecting fixed array data block
 *
 * Return:      Non-NULL pointer to data block on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5FA_dblock_t *
H5FA__dblock_protect(H5FA_hdr_t *hdr, haddr_t dblk_addr, unsigned flags)
{
    H5FA_dblock_t         *dblock = NULL; /* Fixed array data block */
    H5FA_dblock_cache_ud_t udata;         /* Information needed for loading data block */
    H5FA_dblock_t         *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(hdr);
    assert(H5_addr_defined(dblk_addr));

    /* only the H5AC__READ_ONLY_FLAG flag is permitted */
    assert((flags & (unsigned)(~H5AC__READ_ONLY_FLAG)) == 0);

    /* Set up user data */
    udata.hdr       = hdr;
    udata.dblk_addr = dblk_addr;

    /* Protect the data block */
    if (NULL ==
        (dblock = (H5FA_dblock_t *)H5AC_protect(hdr->f, H5AC_FARRAY_DBLOCK, dblk_addr, &udata, flags)))
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTPROTECT, NULL,
                    "unable to protect fixed array data block, address = %llu",
                    (unsigned long long)dblk_addr);

    /* Create top proxy, if it doesn't exist */
    if (hdr->top_proxy && NULL == dblock->top_proxy) {
        /* Add data block as child of 'top' proxy */
        if (H5AC_proxy_entry_add_child(hdr->top_proxy, hdr->f, dblock) < 0)
            HGOTO_ERROR(H5E_FARRAY, H5E_CANTSET, NULL,
                        "unable to add fixed array entry as child of array proxy");
        dblock->top_proxy = hdr->top_proxy;
    } /* end if */

    /* Set return value */
    ret_value = dblock;

done:
    /* Clean up on error */
    if (!ret_value)
        /* Release the data block, if it was protected */
        if (dblock &&
            H5AC_unprotect(hdr->f, H5AC_FARRAY_DBLOCK, dblock->addr, dblock, H5AC__NO_FLAGS_SET) < 0)
            HDONE_ERROR(H5E_FARRAY, H5E_CANTUNPROTECT, NULL,
                        "unable to unprotect fixed array data block, address = %llu",
                        (unsigned long long)dblock->addr)

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FA__dblock_protect() */

/*-------------------------------------------------------------------------
 * Function:    H5FA__dblock_unprotect
 *
 * Purpose:     Convenience wrapper around unprotecting fixed array data block
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FA__dblock_unprotect(H5FA_dblock_t *dblock, unsigned cache_flags)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(dblock);

    /* Unprotect the data block */
    if (H5AC_unprotect(dblock->hdr->f, H5AC_FARRAY_DBLOCK, dblock->addr, dblock, cache_flags) < 0)
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTUNPROTECT, FAIL,
                    "unable to unprotect fixed array data block, address = %llu",
                    (unsigned long long)dblock->addr);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FA__dblock_unprotect() */

/*-------------------------------------------------------------------------
 * Function:    H5FA__dblock_delete
 *
 * Purpose:     Delete a data block
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FA__dblock_delete(H5FA_hdr_t *hdr, haddr_t dblk_addr)
{
    H5FA_dblock_t *dblock    = NULL; /* Pointer to data block */
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(hdr);
    assert(H5_addr_defined(dblk_addr));

    /* Protect data block */
    if (NULL == (dblock = H5FA__dblock_protect(hdr, dblk_addr, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTPROTECT, FAIL,
                    "unable to protect fixed array data block, address = %llu",
                    (unsigned long long)dblk_addr);

    /* Check if data block is paged */
    if (dblock->npages) {
        haddr_t dblk_page_addr; /* Address of each data block page */
        size_t  u;              /* Local index variable */

        /* Set up initial state */
        dblk_page_addr = dblk_addr + H5FA_DBLOCK_PREFIX_SIZE(dblock);

        /* Iterate over pages in data block */
        for (u = 0; u < dblock->npages; u++) {
            /* Evict the data block page from the metadata cache */
            /* (OK to call if it doesn't exist in the cache) */
            if (H5AC_expunge_entry(hdr->f, H5AC_FARRAY_DBLK_PAGE, dblk_page_addr, H5AC__NO_FLAGS_SET) < 0)
                HGOTO_ERROR(H5E_FARRAY, H5E_CANTEXPUNGE, FAIL,
                            "unable to remove array data block page from metadata cache");

            /* Advance to next page address */
            dblk_page_addr += dblock->dblk_page_size;
        } /* end for */
    }     /* end if */

done:
    /* Finished deleting data block in metadata cache */
    if (dblock && H5FA__dblock_unprotect(dblock, H5AC__DIRTIED_FLAG | H5AC__DELETED_FLAG |
                                                     H5AC__FREE_FILE_SPACE_FLAG) < 0)
        HDONE_ERROR(H5E_FARRAY, H5E_CANTUNPROTECT, FAIL, "unable to release fixed array data block");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FA__dblock_delete() */

/*-------------------------------------------------------------------------
 * Function:    H5FA__dblock_dest
 *
 * Purpose:     Destroys a fixed array data block in memory.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FA__dblock_dest(H5FA_dblock_t *dblock)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(dblock);

    /* Check if shared header field has been initialized */
    if (dblock->hdr) {
        /* Check if we've got elements in the data block */
        if (dblock->elmts && !dblock->npages) {
            /* Free buffer for data block elements */
            assert(dblock->hdr->cparam.nelmts > 0);
            dblock->elmts = H5FL_BLK_FREE(chunk_elmts, dblock->elmts);
        } /* end if */

        /* Check if data block is paged */
        if (dblock->npages) {
            /* Free buffer for 'page init' bitmask, if there is one */
            assert(dblock->dblk_page_init_size > 0);
            if (dblock->dblk_page_init)
                dblock->dblk_page_init = H5FL_BLK_FREE(fa_page_init, dblock->dblk_page_init);
        } /* end if */

        /* Decrement reference count on shared info */
        if (H5FA__hdr_decr(dblock->hdr) < 0)
            HGOTO_ERROR(H5E_FARRAY, H5E_CANTDEC, FAIL,
                        "can't decrement reference count on shared array header");
        dblock->hdr = NULL;
    } /* end if */

    /* Sanity check */
    assert(NULL == dblock->top_proxy);

    /* Free the data block itself */
    dblock = H5FL_FREE(H5FA_dblock_t, dblock);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FA__dblock_dest() */
