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
 * Created:		H5EAiblock.c
 *
 * Purpose:		Index block routines for extensible arrays.
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
H5FL_DEFINE_STATIC(H5EA_iblock_t);

/* Declare a free list to manage the index block elements */
H5FL_BLK_DEFINE_STATIC(idx_blk_elmt_buf);

/* Declare a free list to manage the haddr_t sequence information */
H5FL_SEQ_DEFINE_STATIC(haddr_t);

/*-------------------------------------------------------------------------
 * Function:	H5EA__iblock_alloc
 *
 * Purpose:	Allocate extensible array index block
 *
 * Return:	Non-NULL pointer to index block on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5EA_iblock_t *
H5EA__iblock_alloc(H5EA_hdr_t *hdr)
{
    H5EA_iblock_t *iblock    = NULL; /* Extensible array index block */
    H5EA_iblock_t *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(hdr);

    /* Allocate memory for the index block */
    if (NULL == (iblock = H5FL_CALLOC(H5EA_iblock_t)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTALLOC, NULL,
                    "memory allocation failed for extensible array index block");

    /* Share common array information */
    if (H5EA__hdr_incr(hdr) < 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTINC, NULL, "can't increment reference count on shared array header");
    iblock->hdr = hdr;

    /* Set non-zero internal fields */
    iblock->addr = HADDR_UNDEF;

    /* Compute information */
    iblock->nsblks      = H5EA_SBLK_FIRST_IDX(hdr->cparam.sup_blk_min_data_ptrs);
    iblock->ndblk_addrs = 2 * ((size_t)hdr->cparam.sup_blk_min_data_ptrs - 1);
    iblock->nsblk_addrs = hdr->nsblks - iblock->nsblks;

    /* Allocate buffer for elements in index block */
    if (hdr->cparam.idx_blk_elmts > 0)
        if (NULL ==
            (iblock->elmts = H5FL_BLK_MALLOC(
                 idx_blk_elmt_buf, (size_t)(hdr->cparam.idx_blk_elmts * hdr->cparam.cls->nat_elmt_size))))
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTALLOC, NULL,
                        "memory allocation failed for index block data element buffer");

    /* Allocate buffer for data block addresses in index block */
    if (iblock->ndblk_addrs > 0)
        if (NULL == (iblock->dblk_addrs = H5FL_SEQ_MALLOC(haddr_t, iblock->ndblk_addrs)))
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTALLOC, NULL,
                        "memory allocation failed for index block data block addresses");

    /* Allocate buffer for super block addresses in index block */
    if (iblock->nsblk_addrs > 0)
        if (NULL == (iblock->sblk_addrs = H5FL_SEQ_MALLOC(haddr_t, iblock->nsblk_addrs)))
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTALLOC, NULL,
                        "memory allocation failed for index block super block addresses");

    /* Set the return value */
    ret_value = iblock;

done:
    if (!ret_value)
        if (iblock && H5EA__iblock_dest(iblock) < 0)
            HDONE_ERROR(H5E_EARRAY, H5E_CANTFREE, NULL, "unable to destroy extensible array index block");
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__iblock_alloc() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__iblock_create
 *
 * Purpose:	Creates a new extensible array index block in the file
 *
 * Return:	Valid file address on success/HADDR_UNDEF on failure
 *
 *-------------------------------------------------------------------------
 */
haddr_t
H5EA__iblock_create(H5EA_hdr_t *hdr, bool *stats_changed)
{
    H5EA_iblock_t *iblock = NULL;     /* Extensible array index block */
    haddr_t        iblock_addr;       /* Extensible array index block address */
    bool           inserted  = false; /* Whether the header was inserted into cache */
    haddr_t        ret_value = HADDR_UNDEF;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(hdr);
    assert(stats_changed);

    /* Allocate the index block */
    if (NULL == (iblock = H5EA__iblock_alloc(hdr)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTALLOC, HADDR_UNDEF,
                    "memory allocation failed for extensible array index block");

    /* Set size of index block on disk */
    iblock->size = H5EA_IBLOCK_SIZE(iblock);

    /* Allocate space for the index block on disk */
    if (HADDR_UNDEF == (iblock_addr = H5MF_alloc(hdr->f, H5FD_MEM_EARRAY_IBLOCK, (hsize_t)iblock->size)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTALLOC, HADDR_UNDEF,
                    "file allocation failed for extensible array index block");
    iblock->addr = iblock_addr;

    /* Clear any elements in index block to fill value */
    if (hdr->cparam.idx_blk_elmts > 0) {
        /* Call the class's 'fill' callback */
        if ((hdr->cparam.cls->fill)(iblock->elmts, (size_t)hdr->cparam.idx_blk_elmts) < 0)
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTSET, HADDR_UNDEF,
                        "can't set extensible array index block elements to class's fill value");
    } /* end if */

    /* Reset any data block addresses in the index block */
    if (iblock->ndblk_addrs > 0) {
        haddr_t tmp_addr = HADDR_UNDEF; /* Address value to fill data block addresses with */

        /* Set all the data block addresses to "undefined" address value */
        H5VM_array_fill(iblock->dblk_addrs, &tmp_addr, sizeof(haddr_t), iblock->ndblk_addrs);
    } /* end if */

    /* Reset any super block addresses in the index block */
    if (iblock->nsblk_addrs > 0) {
        haddr_t tmp_addr = HADDR_UNDEF; /* Address value to fill super block addresses with */

        /* Set all the super block addresses to "undefined" address value */
        H5VM_array_fill(iblock->sblk_addrs, &tmp_addr, sizeof(haddr_t), iblock->nsblk_addrs);
    } /* end if */

    /* Cache the new extensible array index block */
    if (H5AC_insert_entry(hdr->f, H5AC_EARRAY_IBLOCK, iblock_addr, iblock, H5AC__NO_FLAGS_SET) < 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTINSERT, HADDR_UNDEF,
                    "can't add extensible array index block to cache");
    inserted = true;

    /* Add index block as child of 'top' proxy */
    if (hdr->top_proxy) {
        if (H5AC_proxy_entry_add_child(hdr->top_proxy, hdr->f, iblock) < 0)
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTSET, HADDR_UNDEF,
                        "unable to add extensible array entry as child of array proxy");
        iblock->top_proxy = hdr->top_proxy;
    } /* end if */

    /* Update extensible array index block statistics */
    assert(0 == hdr->stats.computed.nindex_blks);
    assert(0 == hdr->stats.computed.index_blk_size);
    hdr->stats.computed.nindex_blks    = 1;
    hdr->stats.computed.index_blk_size = iblock->size;

    /* Increment count of elements "realized" */
    hdr->stats.stored.nelmts += hdr->cparam.idx_blk_elmts;

    /* Mark the statistics as changed */
    *stats_changed = true;

    /* Set address of index block to return */
    ret_value = iblock_addr;

done:
    if (!H5_addr_defined(ret_value))
        if (iblock) {
            /* Remove from cache, if inserted */
            if (inserted)
                if (H5AC_remove_entry(iblock) < 0)
                    HDONE_ERROR(H5E_EARRAY, H5E_CANTREMOVE, HADDR_UNDEF,
                                "unable to remove extensible array index block from cache");

            /* Release index block's disk space */
            if (H5_addr_defined(iblock->addr) &&
                H5MF_xfree(hdr->f, H5FD_MEM_EARRAY_IBLOCK, iblock->addr, (hsize_t)iblock->size) < 0)
                HDONE_ERROR(H5E_EARRAY, H5E_CANTFREE, HADDR_UNDEF,
                            "unable to release file space for extensible array index block");

            /* Destroy index block */
            if (H5EA__iblock_dest(iblock) < 0)
                HDONE_ERROR(H5E_EARRAY, H5E_CANTFREE, HADDR_UNDEF,
                            "unable to destroy extensible array index block");
        } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__iblock_create() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__iblock_protect
 *
 * Purpose:	Convenience wrapper around protecting extensible array index block
 *
 * Return:	Non-NULL pointer to index block on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5EA_iblock_t *
H5EA__iblock_protect(H5EA_hdr_t *hdr, unsigned flags)
{
    H5EA_iblock_t *iblock    = NULL; /* Pointer to index block */
    H5EA_iblock_t *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(hdr);

    /* only the H5AC__READ_ONLY_FLAG may be set */
    assert((flags & (unsigned)(~H5AC__READ_ONLY_FLAG)) == 0);

    /* Protect the index block */
    if (NULL ==
        (iblock = (H5EA_iblock_t *)H5AC_protect(hdr->f, H5AC_EARRAY_IBLOCK, hdr->idx_blk_addr, hdr, flags)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTPROTECT, NULL,
                    "unable to protect extensible array index block, address = %llu",
                    (unsigned long long)hdr->idx_blk_addr);

    /* Create top proxy, if it doesn't exist */
    if (hdr->top_proxy && NULL == iblock->top_proxy) {
        /* Add index block as child of 'top' proxy */
        if (H5AC_proxy_entry_add_child(hdr->top_proxy, hdr->f, iblock) < 0)
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTSET, NULL,
                        "unable to add extensible array entry as child of array proxy");
        iblock->top_proxy = hdr->top_proxy;
    } /* end if */

    /* Set return value */
    ret_value = iblock;

done:
    /* Clean up on error */
    if (!ret_value) {
        /* Release the index block, if it was protected */
        if (iblock &&
            H5AC_unprotect(hdr->f, H5AC_EARRAY_IBLOCK, iblock->addr, iblock, H5AC__NO_FLAGS_SET) < 0)
            HDONE_ERROR(H5E_EARRAY, H5E_CANTUNPROTECT, NULL,
                        "unable to unprotect extensible array index block, address = %llu",
                        (unsigned long long)iblock->addr);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__iblock_protect() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__iblock_unprotect
 *
 * Purpose:	Convenience wrapper around unprotecting extensible array index block
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5EA__iblock_unprotect(H5EA_iblock_t *iblock, unsigned cache_flags)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(iblock);

    /* Unprotect the index block */
    if (H5AC_unprotect(iblock->hdr->f, H5AC_EARRAY_IBLOCK, iblock->addr, iblock, cache_flags) < 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTUNPROTECT, FAIL,
                    "unable to unprotect extensible array index block, address = %llu",
                    (unsigned long long)iblock->addr);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__iblock_unprotect() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__iblock_delete
 *
 * Purpose:	Delete index block
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5EA__iblock_delete(H5EA_hdr_t *hdr)
{
    H5EA_iblock_t *iblock    = NULL; /* Pointer to index block */
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(hdr);
    assert(H5_addr_defined(hdr->idx_blk_addr));

    /* Protect index block */
    if (NULL == (iblock = H5EA__iblock_protect(hdr, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTPROTECT, FAIL,
                    "unable to protect extensible array index block, address = %llu",
                    (unsigned long long)hdr->idx_blk_addr);

    /* Check for index block having data block pointers */
    if (iblock->ndblk_addrs > 0) {
        unsigned sblk_idx; /* Current super block index */
        unsigned dblk_idx; /* Current data block index w/in super block */
        size_t   u;        /* Local index variable */

        /* Iterate over data blocks */
        sblk_idx = dblk_idx = 0;
        for (u = 0; u < iblock->ndblk_addrs; u++) {
            /* Check for data block existing */
            if (H5_addr_defined(iblock->dblk_addrs[u])) {
                /* Delete data block */
                if (H5EA__dblock_delete(hdr, iblock, iblock->dblk_addrs[u],
                                        hdr->sblk_info[sblk_idx].dblk_nelmts) < 0)
                    HGOTO_ERROR(H5E_EARRAY, H5E_CANTDELETE, FAIL,
                                "unable to delete extensible array data block");
                iblock->dblk_addrs[u] = HADDR_UNDEF;
            } /* end if */

            /* Advance to next data block w/in super block */
            dblk_idx++;

            /* Check for moving to next super block */
            if (dblk_idx >= hdr->sblk_info[sblk_idx].ndblks) {
                sblk_idx++;
                dblk_idx = 0;
            } /* end if */
        }     /* end for */
    }         /* end if */

    /* Check for index block having data block pointers (not yet) */
    if (iblock->nsblk_addrs > 0) {
        size_t u; /* Local index variable */

        /* Iterate over super blocks */
        for (u = 0; u < iblock->nsblk_addrs; u++) {
            /* Check for data block existing */
            if (H5_addr_defined(iblock->sblk_addrs[u])) {
                /* Delete super block */
                if (H5EA__sblock_delete(hdr, iblock, iblock->sblk_addrs[u], (unsigned)(u + iblock->nsblks)) <
                    0)
                    HGOTO_ERROR(H5E_EARRAY, H5E_CANTDELETE, FAIL,
                                "unable to delete extensible array super block");
                iblock->sblk_addrs[u] = HADDR_UNDEF;
            }
        }
    }

done:
    /* Finished deleting index block in metadata cache */
    if (iblock && H5EA__iblock_unprotect(iblock, H5AC__DIRTIED_FLAG | H5AC__DELETED_FLAG |
                                                     H5AC__FREE_FILE_SPACE_FLAG) < 0)
        HDONE_ERROR(H5E_EARRAY, H5E_CANTUNPROTECT, FAIL, "unable to release extensible array index block");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__iblock_delete() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__iblock_dest
 *
 * Purpose:	Destroys an extensible array index block in memory.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5EA__iblock_dest(H5EA_iblock_t *iblock)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(iblock);

    /* Check if shared header field has been initialized */
    if (iblock->hdr) {
        /* Check if we've got elements in the index block */
        if (iblock->elmts) {
            /* Free buffer for index block elements */
            assert(iblock->hdr->cparam.idx_blk_elmts > 0);
            iblock->elmts = H5FL_BLK_FREE(idx_blk_elmt_buf, iblock->elmts);
        } /* end if */

        /* Check if we've got data block addresses in the index block */
        if (iblock->dblk_addrs) {
            /* Free buffer for index block data block addresses */
            assert(iblock->ndblk_addrs > 0);
            iblock->dblk_addrs  = H5FL_SEQ_FREE(haddr_t, iblock->dblk_addrs);
            iblock->ndblk_addrs = 0;
        } /* end if */

        /* Check if we've got super block addresses in the index block */
        if (iblock->sblk_addrs) {
            /* Free buffer for index block super block addresses */
            assert(iblock->nsblk_addrs > 0);
            iblock->sblk_addrs  = H5FL_SEQ_FREE(haddr_t, iblock->sblk_addrs);
            iblock->nsblk_addrs = 0;
        } /* end if */

        /* Decrement reference count on shared info */
        if (H5EA__hdr_decr(iblock->hdr) < 0)
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTDEC, FAIL,
                        "can't decrement reference count on shared array header");
        iblock->hdr = NULL;
    } /* end if */

    /* Sanity check */
    assert(NULL == iblock->top_proxy);

    /* Free the index block itself */
    iblock = H5FL_FREE(H5EA_iblock_t, iblock);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__iblock_dest() */
