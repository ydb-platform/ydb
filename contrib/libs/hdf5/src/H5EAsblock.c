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
 * Created:		H5EAsblock.c
 *
 * Purpose:		Super block routines for extensible arrays.
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
#include "H5MFprivate.h" /* File memory management		*/
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

/* Declare a free list to manage the H5EA_iblock_t struct */
H5FL_DEFINE_STATIC(H5EA_sblock_t);

/* Declare a free list to manage the haddr_t sequence information */
H5FL_SEQ_DEFINE_STATIC(haddr_t);

/* Declare a free list to manage blocks of 'page init' bitmasks */
H5FL_BLK_DEFINE(page_init);

/*-------------------------------------------------------------------------
 * Function:	H5EA__sblock_alloc
 *
 * Purpose:	Allocate extensible array super block
 *
 * Return:	Non-NULL pointer to super block on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5EA_sblock_t *
H5EA__sblock_alloc(H5EA_hdr_t *hdr, H5EA_iblock_t *parent, unsigned sblk_idx)
{
    H5EA_sblock_t *sblock    = NULL; /* Extensible array super block */
    H5EA_sblock_t *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(hdr);

    /* Allocate memory for the index block */
    if (NULL == (sblock = H5FL_CALLOC(H5EA_sblock_t)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTALLOC, NULL,
                    "memory allocation failed for extensible array super block");

    /* Share common array information */
    if (H5EA__hdr_incr(hdr) < 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTINC, NULL, "can't increment reference count on shared array header");
    sblock->hdr = hdr;

    /* Set non-zero internal fields */
    sblock->parent = parent;
    sblock->addr   = HADDR_UNDEF;

    /* Compute/cache information */
    sblock->idx    = sblk_idx;
    sblock->ndblks = hdr->sblk_info[sblk_idx].ndblks;
    assert(sblock->ndblks);
    sblock->dblk_nelmts = hdr->sblk_info[sblk_idx].dblk_nelmts;

    /* Allocate buffer for data block addresses in super block */
    if (NULL == (sblock->dblk_addrs = H5FL_SEQ_MALLOC(haddr_t, sblock->ndblks)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTALLOC, NULL,
                    "memory allocation failed for super block data block addresses");

    /* Check if # of elements in data blocks requires paging */
    if (sblock->dblk_nelmts > hdr->dblk_page_nelmts) {
        /* Compute # of pages in each data block from this super block */
        sblock->dblk_npages = sblock->dblk_nelmts / hdr->dblk_page_nelmts;

        /* Sanity check that we have at least 2 pages in data block */
        assert(sblock->dblk_npages > 1);

        /* Sanity check for integer truncation */
        assert((sblock->dblk_npages * hdr->dblk_page_nelmts) == sblock->dblk_nelmts);

        /* Compute size of buffer for each data block's 'page init' bitmask */
        sblock->dblk_page_init_size = ((sblock->dblk_npages) + 7) / 8;
        assert(sblock->dblk_page_init_size > 0);

        /* Allocate buffer for all 'page init' bitmasks in super block */
        if (NULL ==
            (sblock->page_init = H5FL_BLK_CALLOC(page_init, sblock->ndblks * sblock->dblk_page_init_size)))
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTALLOC, NULL,
                        "memory allocation failed for super block page init bitmask");

        /* Compute data block page size */
        sblock->dblk_page_size = (hdr->dblk_page_nelmts * hdr->cparam.raw_elmt_size) + H5EA_SIZEOF_CHKSUM;
    } /* end if */

    /* Set the return value */
    ret_value = sblock;

done:
    if (!ret_value)
        if (sblock && H5EA__sblock_dest(sblock) < 0)
            HDONE_ERROR(H5E_EARRAY, H5E_CANTFREE, NULL, "unable to destroy extensible array super block");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__sblock_alloc() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__sblock_create
 *
 * Purpose:	Creates a new extensible array super block in the file
 *
 * Return:	Valid file address on success/HADDR_UNDEF on failure
 *
 *-------------------------------------------------------------------------
 */
haddr_t
H5EA__sblock_create(H5EA_hdr_t *hdr, H5EA_iblock_t *parent, bool *stats_changed, unsigned sblk_idx)
{
    H5EA_sblock_t *sblock = NULL;           /* Extensible array super block */
    haddr_t        sblock_addr;             /* Extensible array super block address */
    haddr_t        tmp_addr  = HADDR_UNDEF; /* Address value to fill data block addresses with */
    bool           inserted  = false;       /* Whether the header was inserted into cache */
    haddr_t        ret_value = HADDR_UNDEF;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(hdr);
    assert(stats_changed);

    /* Allocate the super block */
    if (NULL == (sblock = H5EA__sblock_alloc(hdr, parent, sblk_idx)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTALLOC, HADDR_UNDEF,
                    "memory allocation failed for extensible array super block");

    /* Set size of super block on disk */
    sblock->size = H5EA_SBLOCK_SIZE(sblock);

    /* Set offset of block in array's address space */
    sblock->block_off = hdr->sblk_info[sblk_idx].start_idx;

    /* Allocate space for the super block on disk */
    if (HADDR_UNDEF == (sblock_addr = H5MF_alloc(hdr->f, H5FD_MEM_EARRAY_SBLOCK, (hsize_t)sblock->size)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTALLOC, HADDR_UNDEF,
                    "file allocation failed for extensible array super block");
    sblock->addr = sblock_addr;

    /* Reset data block addresses to "undefined" address value */
    H5VM_array_fill(sblock->dblk_addrs, &tmp_addr, sizeof(haddr_t), sblock->ndblks);

    /* Cache the new extensible array super block */
    if (H5AC_insert_entry(hdr->f, H5AC_EARRAY_SBLOCK, sblock_addr, sblock, H5AC__NO_FLAGS_SET) < 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTINSERT, HADDR_UNDEF,
                    "can't add extensible array super block to cache");
    inserted = true;

    /* Add super block as child of 'top' proxy */
    if (hdr->top_proxy) {
        if (H5AC_proxy_entry_add_child(hdr->top_proxy, hdr->f, sblock) < 0)
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTSET, HADDR_UNDEF,
                        "unable to add extensible array entry as child of array proxy");
        sblock->top_proxy = hdr->top_proxy;
    } /* end if */

    /* Update extensible array super block statistics */
    hdr->stats.stored.nsuper_blks++;
    hdr->stats.stored.super_blk_size += sblock->size;

    /* Mark the statistics as changed */
    *stats_changed = true;

    /* Set address of super block to return */
    ret_value = sblock_addr;

done:
    if (!H5_addr_defined(ret_value))
        if (sblock) {
            /* Remove from cache, if inserted */
            if (inserted)
                if (H5AC_remove_entry(sblock) < 0)
                    HDONE_ERROR(H5E_EARRAY, H5E_CANTREMOVE, HADDR_UNDEF,
                                "unable to remove extensible array super block from cache");

            /* Release super block's disk space */
            if (H5_addr_defined(sblock->addr) &&
                H5MF_xfree(hdr->f, H5FD_MEM_EARRAY_SBLOCK, sblock->addr, (hsize_t)sblock->size) < 0)
                HDONE_ERROR(H5E_EARRAY, H5E_CANTFREE, HADDR_UNDEF,
                            "unable to release extensible array super block");

            /* Destroy super block */
            if (H5EA__sblock_dest(sblock) < 0)
                HDONE_ERROR(H5E_EARRAY, H5E_CANTFREE, HADDR_UNDEF,
                            "unable to destroy extensible array super block");
        } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__sblock_create() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__sblock_protect
 *
 * Purpose:	Convenience wrapper around protecting extensible array super block
 *
 * Return:	Non-NULL pointer to data block on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5EA_sblock_t *
H5EA__sblock_protect(H5EA_hdr_t *hdr, H5EA_iblock_t *parent, haddr_t sblk_addr, unsigned sblk_idx,
                     unsigned flags)
{
    H5EA_sblock_t         *sblock = NULL; /* Pointer to super block */
    H5EA_sblock_cache_ud_t udata;         /* Information needed for loading super block */
    H5EA_sblock_t         *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(hdr);
    assert(H5_addr_defined(sblk_addr));

    /* only the H5AC__READ_ONLY_FLAG may be set */
    assert((flags & (unsigned)(~H5AC__READ_ONLY_FLAG)) == 0);

    /* Set up user data */
    udata.hdr       = hdr;
    udata.parent    = parent;
    udata.sblk_idx  = sblk_idx;
    udata.sblk_addr = sblk_addr;

    /* Protect the super block */
    if (NULL ==
        (sblock = (H5EA_sblock_t *)H5AC_protect(hdr->f, H5AC_EARRAY_SBLOCK, sblk_addr, &udata, flags)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTPROTECT, NULL,
                    "unable to protect extensible array super block, address = %llu",
                    (unsigned long long)sblk_addr);

    /* Create top proxy, if it doesn't exist */
    if (hdr->top_proxy && NULL == sblock->top_proxy) {
        /* Add super block as child of 'top' proxy */
        if (H5AC_proxy_entry_add_child(hdr->top_proxy, hdr->f, sblock) < 0)
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTSET, NULL,
                        "unable to add extensible array entry as child of array proxy");
        sblock->top_proxy = hdr->top_proxy;
    }

    /* Set return value */
    ret_value = sblock;

done:
    /* Clean up on error */
    if (!ret_value) {
        /* Release the super block, if it was protected */
        if (sblock &&
            H5AC_unprotect(hdr->f, H5AC_EARRAY_SBLOCK, sblock->addr, sblock, H5AC__NO_FLAGS_SET) < 0)
            HDONE_ERROR(H5E_EARRAY, H5E_CANTUNPROTECT, NULL,
                        "unable to unprotect extensible array super block, address = %llu",
                        (unsigned long long)sblock->addr);
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__sblock_protect() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__sblock_unprotect
 *
 * Purpose:	Convenience wrapper around unprotecting extensible array super block
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5EA__sblock_unprotect(H5EA_sblock_t *sblock, unsigned cache_flags)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(sblock);

    /* Unprotect the super block */
    if (H5AC_unprotect(sblock->hdr->f, H5AC_EARRAY_SBLOCK, sblock->addr, sblock, cache_flags) < 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTUNPROTECT, FAIL,
                    "unable to unprotect extensible array super block, address = %llu",
                    (unsigned long long)sblock->addr);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__sblock_unprotect() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__sblock_delete
 *
 * Purpose:	Delete a super block
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5EA__sblock_delete(H5EA_hdr_t *hdr, H5EA_iblock_t *parent, haddr_t sblk_addr, unsigned sblk_idx)
{
    H5EA_sblock_t *sblock = NULL; /* Pointer to super block */
    size_t         u;             /* Local index variable */
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(hdr);
    assert(H5_addr_defined(sblk_addr));

    /* Protect super block */
    if (NULL == (sblock = H5EA__sblock_protect(hdr, parent, sblk_addr, sblk_idx, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTPROTECT, FAIL,
                    "unable to protect extensible array super block, address = %llu",
                    (unsigned long long)sblk_addr);

    /* Iterate over data blocks */
    for (u = 0; u < sblock->ndblks; u++) {
        /* Check for data block existing */
        if (H5_addr_defined(sblock->dblk_addrs[u])) {
            /* Delete data block */
            if (H5EA__dblock_delete(hdr, sblock, sblock->dblk_addrs[u], sblock->dblk_nelmts) < 0)
                HGOTO_ERROR(H5E_EARRAY, H5E_CANTDELETE, FAIL, "unable to delete extensible array data block");
            sblock->dblk_addrs[u] = HADDR_UNDEF;
        } /* end if */
    }     /* end for */

done:
    /* Finished deleting super block in metadata cache */
    if (sblock && H5EA__sblock_unprotect(sblock, H5AC__DIRTIED_FLAG | H5AC__DELETED_FLAG |
                                                     H5AC__FREE_FILE_SPACE_FLAG) < 0)
        HDONE_ERROR(H5E_EARRAY, H5E_CANTUNPROTECT, FAIL, "unable to release extensible array super block");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__sblock_delete() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__sblock_dest
 *
 * Purpose:	Destroys an extensible array super block in memory.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5EA__sblock_dest(H5EA_sblock_t *sblock)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(sblock);
    assert(!sblock->has_hdr_depend);

    /* Check if shared header field has been initialized */
    if (sblock->hdr) {
        /* Free buffer for super block data block addresses, if there are any */
        if (sblock->dblk_addrs)
            sblock->dblk_addrs = H5FL_SEQ_FREE(haddr_t, sblock->dblk_addrs);

        /* Free buffer for super block 'page init' bitmask, if there is one */
        if (sblock->page_init) {
            assert(sblock->dblk_npages > 0);
            sblock->page_init = H5FL_BLK_FREE(page_init, sblock->page_init);
        } /* end if */

        /* Decrement reference count on shared info */
        if (H5EA__hdr_decr(sblock->hdr) < 0)
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTDEC, FAIL,
                        "can't decrement reference count on shared array header");
        sblock->hdr = NULL;
    } /* end if */

    /* Sanity check */
    assert(NULL == sblock->top_proxy);

    /* Free the super block itself */
    sblock = H5FL_FREE(H5EA_sblock_t, sblock);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__sblock_dest() */
