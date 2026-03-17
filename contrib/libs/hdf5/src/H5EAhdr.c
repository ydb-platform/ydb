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
 * Created:		H5EAhdr.c
 *
 * Purpose:		Array header routines for extensible arrays.
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
#include "H5MFprivate.h" /* File memory management		*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5VMprivate.h" /* Vectors and arrays 			*/

/****************/
/* Local Macros */
/****************/

#ifndef NDEBUG
/* Max. # of bits for max. nelmts index */
#define H5EA_MAX_NELMTS_IDX_MAX 64
#endif /* NDEBUG */

/* # of elements in a data block for a particular super block */
#define H5EA_SBLK_DBLK_NELMTS(s, m) (size_t) H5_EXP2(((s) + 1) / 2) * (m)

/******************/
/* Local Typedefs */
/******************/

/* Alias for pointer to factory, for use when allocating sequences of them */
typedef H5FL_fac_head_t *H5FL_fac_head_ptr_t;

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

/* Declare a free list to manage the H5EA_hdr_t struct */
H5FL_DEFINE_STATIC(H5EA_hdr_t);

/* Declare a free list to manage the H5FL_fac_head_ptr_t sequence information */
H5FL_SEQ_DEFINE_STATIC(H5FL_fac_head_ptr_t);

/* Declare a free list to manage the H5EA_sblk_info_t sequence information */
H5FL_SEQ_DEFINE_STATIC(H5EA_sblk_info_t);

/*-------------------------------------------------------------------------
 * Function:	H5EA__hdr_alloc
 *
 * Purpose:	Allocate shared extensible array header
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
H5EA_hdr_t *
H5EA__hdr_alloc(H5F_t *f)
{
    H5EA_hdr_t *hdr       = NULL; /* Shared extensible array header */
    H5EA_hdr_t *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(f);

    /* Allocate space for the shared information */
    if (NULL == (hdr = H5FL_CALLOC(H5EA_hdr_t)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTALLOC, NULL,
                    "memory allocation failed for extensible array shared header");

    /* Set non-zero internal fields */
    hdr->addr = HADDR_UNDEF;

    /* Set the internal parameters for the array */
    hdr->f           = f;
    hdr->swmr_write  = (H5F_INTENT(f) & H5F_ACC_SWMR_WRITE) > 0;
    hdr->sizeof_addr = H5F_SIZEOF_ADDR(f);
    hdr->sizeof_size = H5F_SIZEOF_SIZE(f);

    /* Set the return value */
    ret_value = hdr;

done:
    if (!ret_value)
        if (hdr && H5EA__hdr_dest(hdr) < 0)
            HDONE_ERROR(H5E_EARRAY, H5E_CANTFREE, NULL, "unable to destroy extensible array header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__hdr_alloc() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__hdr_init
 *
 * Purpose:	Compute useful information for extensible array, based on
 *              "creation" information.
 *
 * Notes:	The equations for variables below are based on this information:
 *
 *	<sblk idx>  <# of dblks>  <size of dblks>       Range of elements in sblk
 *	==========  ============  ===============       =========================
 *	      0          1         1 * <dblk min elmts>   0 * <dblk min elmts> <->   1 * <dblk min elmts> - 1
 *	      1          1         2 * <dblk min elmts>   1 * <dblk min elmts> <->   3 * <dblk min elmts> - 1
 *	      2          2         2 * <dblk min elmts>   3 * <dblk min elmts> <->   7 * <dblk min elmts> - 1
 *	      3          2         4 * <dblk min elmts>   7 * <dblk min elmts> <->  15 * <dblk min elmts> - 1
 *	      4          4         4 * <dblk min elmts>  15 * <dblk min elmts> <->  31 * <dblk min elmts> - 1
 *	      5          4         8 * <dblk min elmts>  31 * <dblk min elmts> <->  63 * <dblk min elmts> - 1
 *	      6          8         8 * <dblk min elmts>  63 * <dblk min elmts> <-> 127 * <dblk min elmts> - 1
 *	      7          8        16 * <dblk min elmts> 127 * <dblk min elmts> <-> 255 * <dblk min elmts> - 1
 *	      .          .         . * <dblk min elmts>   . * <dblk min elmts> <->   . * <dblk min elmts> - 1
 *	      .          .         . * <dblk min elmts>   . * <dblk min elmts> <->   . * <dblk min elmts> - 1
 *	      .          .         . * <dblk min elmts>   . * <dblk min elmts> <->   . * <dblk min elmts> - 1
 *
 *	Therefore:
 *		<sblk idx>(<elmt idx>) = lg2((<elmt idx> / <dblk min elmts>) + 1)
 *		<# of dblks>(<sblk idx>) = 2 ^ (<sblk idx> / 2)
 *		<size of dblk>(<sblk idx>) = 2 ^ ((<sblk idx> + 1) / 2)
 *		<total # of sblks>(<max. # of elmts>) = 1 + (lg2(<max. # of elmts>) - lg2(<dblk min_elmts>))
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5EA__hdr_init(H5EA_hdr_t *hdr, void *ctx_udata)
{
    hsize_t start_idx;  /* First element index for each super block */
    hsize_t start_dblk; /* First data block index for each super block */
    size_t  u;          /* Local index variable */
    herr_t  ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(hdr);
    assert(hdr->cparam.max_nelmts_bits);
    assert(hdr->cparam.data_blk_min_elmts);
    assert(hdr->cparam.sup_blk_min_data_ptrs);

    /* Compute general information */
    hdr->nsblks           = 1 + (hdr->cparam.max_nelmts_bits - H5VM_log2_of2(hdr->cparam.data_blk_min_elmts));
    hdr->dblk_page_nelmts = (size_t)1 << hdr->cparam.max_dblk_page_nelmts_bits;
    hdr->arr_off_size     = (unsigned char)H5EA_SIZEOF_OFFSET_BITS(hdr->cparam.max_nelmts_bits);

    /* Allocate information for each super block */
    if (NULL == (hdr->sblk_info = H5FL_SEQ_MALLOC(H5EA_sblk_info_t, hdr->nsblks)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTALLOC, FAIL, "memory allocation failed for super block info array");

    /* Compute information about each super block */
    start_idx  = 0;
    start_dblk = 0;
    for (u = 0; u < hdr->nsblks; u++) {
        hdr->sblk_info[u].ndblks      = (size_t)H5_EXP2(u / 2);
        hdr->sblk_info[u].dblk_nelmts = H5EA_SBLK_DBLK_NELMTS(u, hdr->cparam.data_blk_min_elmts);
        hdr->sblk_info[u].start_idx   = start_idx;
        hdr->sblk_info[u].start_dblk  = start_dblk;

        /* Advance starting indices for next super block */
        start_idx += (hsize_t)hdr->sblk_info[u].ndblks * (hsize_t)hdr->sblk_info[u].dblk_nelmts;
        start_dblk += (hsize_t)hdr->sblk_info[u].ndblks;
    }

    /* Set size of header on disk (locally and in statistics) */
    hdr->stats.computed.hdr_size = hdr->size = H5EA_HEADER_SIZE_HDR(hdr);

    /* Create the callback context, if there's one */
    if (hdr->cparam.cls->crt_context) {
        if (NULL == (hdr->cb_ctx = (*hdr->cparam.cls->crt_context)(ctx_udata)))
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTCREATE, FAIL,
                        "unable to create extensible array client callback context");
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__hdr_init() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__hdr_alloc_elmts
 *
 * Purpose:	Allocate extensible array data block elements
 *
 * Return:	Non-NULL pointer to buffer for elements on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
void *
H5EA__hdr_alloc_elmts(H5EA_hdr_t *hdr, size_t nelmts)
{
    void    *elmts = NULL; /* Element buffer allocated */
    unsigned idx;          /* Index of element buffer factory in header */
    void    *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(hdr);
    assert(nelmts > 0);

    /* Compute the index of the element buffer factory */
    H5_CHECK_OVERFLOW(nelmts, /*From:*/ size_t, /*To:*/ uint32_t);
    idx = H5VM_log2_of2((uint32_t)nelmts) - H5VM_log2_of2((uint32_t)hdr->cparam.data_blk_min_elmts);

    /* Check for needing to increase size of array of factories */
    if (idx >= hdr->elmt_fac.nalloc) {
        H5FL_fac_head_t **new_fac; /* New array of element factories */
        size_t            new_nalloc =
            MAX3(1, (idx + 1), (2 * hdr->elmt_fac.nalloc)); /* New number of factories allocated */

        /* Re-allocate array of element factories */
        if (NULL == (new_fac = H5FL_SEQ_REALLOC(H5FL_fac_head_ptr_t, hdr->elmt_fac.fac, new_nalloc)))
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTALLOC, NULL,
                        "memory allocation failed for data block data element buffer factory array");

        /* Zero out new elements allocated */
        memset(new_fac + hdr->elmt_fac.nalloc, 0,
               (new_nalloc - hdr->elmt_fac.nalloc) * sizeof(H5FL_fac_head_ptr_t));

        /* Update information about element factories in header */
        hdr->elmt_fac.nalloc = new_nalloc;
        hdr->elmt_fac.fac    = new_fac;
    } /* end if */

    /* Check for un-initialized factory at index */
    if (NULL == hdr->elmt_fac.fac[idx]) {
        if (NULL == (hdr->elmt_fac.fac[idx] = H5FL_fac_init(nelmts * (size_t)hdr->cparam.cls->nat_elmt_size)))
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTINIT, NULL,
                        "can't create data block data element buffer factory");
    } /* end if */

    /* Allocate buffer for elements in index block */
    if (NULL == (elmts = H5FL_FAC_MALLOC(hdr->elmt_fac.fac[idx])))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTALLOC, NULL,
                    "memory allocation failed for data block data element buffer");

    /* Set the return value */
    ret_value = elmts;

done:
    if (!ret_value)
        if (elmts)
            elmts = H5FL_FAC_FREE(hdr->elmt_fac.fac[idx], elmts);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__hdr_alloc_elmts() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__hdr_free_elmts
 *
 * Purpose:	Free extensible array data block elements
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5EA__hdr_free_elmts(H5EA_hdr_t *hdr, size_t nelmts, void *elmts)
{
    unsigned idx; /* Index of element buffer factory in header */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(hdr);
    assert(nelmts > 0);
    assert(elmts);

    /* Compute the index of the element buffer factory */
    H5_CHECK_OVERFLOW(nelmts, /*From:*/ size_t, /*To:*/ uint32_t);
    idx = H5VM_log2_of2((uint32_t)nelmts) - H5VM_log2_of2((uint32_t)hdr->cparam.data_blk_min_elmts);

    /* Free buffer for elements in index block */
    assert(idx < hdr->elmt_fac.nalloc);
    assert(hdr->elmt_fac.fac[idx]);
    elmts = H5FL_FAC_FREE(hdr->elmt_fac.fac[idx], elmts);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5EA__hdr_free_elmts() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__hdr_create
 *
 * Purpose:	Creates a new extensible array header in the file
 *
 * Return:      Success:    Address of new header in the file
 *              Failure:    HADDR_UNDEF
 *
 *-------------------------------------------------------------------------
 */
haddr_t
H5EA__hdr_create(H5F_t *f, const H5EA_create_t *cparam, void *ctx_udata)
{
    H5EA_hdr_t *hdr       = NULL;  /* Extensible array header */
    bool        inserted  = false; /* Whether the header was inserted into cache */
    haddr_t     ret_value = HADDR_UNDEF;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(f);
    assert(cparam);

#ifndef NDEBUG
    {
        unsigned sblk_idx;         /* Super block index for first "actual" super block */
        size_t   dblk_nelmts;      /* Number of data block elements */
        size_t   dblk_page_nelmts; /* Number of elements in a data block page */

        /* Check for valid parameters */
        if (cparam->raw_elmt_size == 0)
            HGOTO_ERROR(H5E_EARRAY, H5E_BADVALUE, HADDR_UNDEF, "element size must be greater than zero");
        if (cparam->max_nelmts_bits == 0)
            HGOTO_ERROR(H5E_EARRAY, H5E_BADVALUE, HADDR_UNDEF,
                        "max. # of elements bits must be greater than zero");
        if (cparam->max_nelmts_bits > H5EA_MAX_NELMTS_IDX_MAX)
            HGOTO_ERROR(H5E_EARRAY, H5E_BADVALUE, HADDR_UNDEF, "max. # of elements bits must be <= %u",
                        (unsigned)H5EA_MAX_NELMTS_IDX_MAX);
        if (cparam->sup_blk_min_data_ptrs < 2)
            HGOTO_ERROR(H5E_EARRAY, H5E_BADVALUE, HADDR_UNDEF,
                        "min # of data block pointers in super block must be >= two");
        if (!POWER_OF_TWO(cparam->sup_blk_min_data_ptrs))
            HGOTO_ERROR(H5E_EARRAY, H5E_BADVALUE, HADDR_UNDEF,
                        "min # of data block pointers in super block must be power of two");
        if (!POWER_OF_TWO(cparam->data_blk_min_elmts))
            HGOTO_ERROR(H5E_EARRAY, H5E_BADVALUE, HADDR_UNDEF,
                        "min # of elements per data block must be power of two");
        dblk_page_nelmts = (size_t)1 << cparam->max_dblk_page_nelmts_bits;
        if (dblk_page_nelmts < cparam->idx_blk_elmts)
            HGOTO_ERROR(
                H5E_EARRAY, H5E_BADVALUE, HADDR_UNDEF,
                "# of elements per data block page must be greater than # of elements in index block");

        /* Compute the number of elements in data blocks for first actual super block */
        sblk_idx    = H5EA_SBLK_FIRST_IDX(cparam->sup_blk_min_data_ptrs);
        dblk_nelmts = H5EA_SBLK_DBLK_NELMTS(sblk_idx, cparam->data_blk_min_elmts);
        if (dblk_page_nelmts < dblk_nelmts)
            HGOTO_ERROR(H5E_EARRAY, H5E_BADVALUE, HADDR_UNDEF,
                        "max. # of elements per data block page bits must be > # of elements in "
                        "first data block from super block");

        if (cparam->max_dblk_page_nelmts_bits > cparam->max_nelmts_bits)
            HGOTO_ERROR(H5E_EARRAY, H5E_BADVALUE, HADDR_UNDEF,
                        "max. # of elements per data block page bits must be <= max. # of elements bits");
    }
#endif /* NDEBUG */

    /* Allocate space for the shared information */
    if (NULL == (hdr = H5EA__hdr_alloc(f)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTALLOC, HADDR_UNDEF,
                    "memory allocation failed for extensible array shared header");

    /* Set the internal parameters for the array */
    hdr->idx_blk_addr = HADDR_UNDEF;

    /* Set the creation parameters for the array */
    H5MM_memcpy(&hdr->cparam, cparam, sizeof(hdr->cparam));

    /* Finish initializing extensible array header */
    if (H5EA__hdr_init(hdr, ctx_udata) < 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTINIT, HADDR_UNDEF,
                    "initialization failed for extensible array header");

    /* Allocate space for the header on disk */
    if (HADDR_UNDEF == (hdr->addr = H5MF_alloc(f, H5FD_MEM_EARRAY_HDR, (hsize_t)hdr->size)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTALLOC, HADDR_UNDEF,
                    "file allocation failed for extensible array header");

    /* Create 'top' proxy for extensible array entries */
    if (hdr->swmr_write)
        if (NULL == (hdr->top_proxy = H5AC_proxy_entry_create()))
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTCREATE, HADDR_UNDEF, "can't create extensible array entry proxy");

    /* Cache the new extensible array header */
    if (H5AC_insert_entry(f, H5AC_EARRAY_HDR, hdr->addr, hdr, H5AC__NO_FLAGS_SET) < 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTINSERT, HADDR_UNDEF, "can't add extensible array header to cache");
    inserted = true;

    /* Add header as child of 'top' proxy */
    if (hdr->top_proxy)
        if (H5AC_proxy_entry_add_child(hdr->top_proxy, f, hdr) < 0)
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTSET, HADDR_UNDEF,
                        "unable to add extensible array entry as child of array proxy");

    /* Set address of array header to return */
    ret_value = hdr->addr;

done:
    if (!H5_addr_defined(ret_value))
        if (hdr) {
            /* Remove from cache, if inserted */
            if (inserted)
                if (H5AC_remove_entry(hdr) < 0)
                    HDONE_ERROR(H5E_EARRAY, H5E_CANTREMOVE, HADDR_UNDEF,
                                "unable to remove extensible array header from cache");

            /* Release header's disk space */
            if (H5_addr_defined(hdr->addr) &&
                H5MF_xfree(f, H5FD_MEM_EARRAY_HDR, hdr->addr, (hsize_t)hdr->size) < 0)
                HDONE_ERROR(H5E_EARRAY, H5E_CANTFREE, HADDR_UNDEF, "unable to free extensible array header");

            /* Destroy header */
            if (H5EA__hdr_dest(hdr) < 0)
                HDONE_ERROR(H5E_EARRAY, H5E_CANTFREE, HADDR_UNDEF,
                            "unable to destroy extensible array header");
        } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__hdr_create() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__hdr_incr
 *
 * Purpose:	Increment component reference count on shared array header
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5EA__hdr_incr(H5EA_hdr_t *hdr)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(hdr);

    /* Mark header as un-evictable when something is depending on it */
    if (hdr->rc == 0)
        if (H5AC_pin_protected_entry(hdr) < 0)
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTPIN, FAIL, "unable to pin extensible array header");

    /* Increment reference count on shared header */
    hdr->rc++;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__hdr_incr() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__hdr_decr
 *
 * Purpose:	Decrement component reference count on shared array header
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5EA__hdr_decr(H5EA_hdr_t *hdr)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(hdr);
    assert(hdr->rc);

    /* Decrement reference count on shared header */
    hdr->rc--;

    /* Mark header as evictable again when nothing depend on it */
    if (hdr->rc == 0) {
        assert(hdr->file_rc == 0);
        if (H5AC_unpin_entry(hdr) < 0)
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTUNPIN, FAIL, "unable to unpin extensible array header");
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__hdr_decr() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__hdr_fuse_incr
 *
 * Purpose:	Increment file reference count on shared array header
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5EA__hdr_fuse_incr(H5EA_hdr_t *hdr)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(hdr);

    /* Increment file reference count on shared header */
    hdr->file_rc++;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5EA__hdr_fuse_incr() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__hdr_fuse_decr
 *
 * Purpose:	Decrement file reference count on shared array header
 *
 * Return:      Success:    The reference count of the header
 *              Failure:    Can't fail
 *
 *-------------------------------------------------------------------------
 */
size_t
H5EA__hdr_fuse_decr(H5EA_hdr_t *hdr)
{
    size_t ret_value = 0;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(hdr);
    assert(hdr->file_rc);

    /* Decrement file reference count on shared header */
    hdr->file_rc--;

    /* Set return value */
    ret_value = hdr->file_rc;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__hdr_fuse_decr() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__hdr_modified
 *
 * Purpose:	Mark an extensible array as modified
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5EA__hdr_modified(H5EA_hdr_t *hdr)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(hdr);
    assert(hdr->f);

    /* Mark header as dirty in cache */
    if (H5AC_mark_entry_dirty(hdr) < 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTMARKDIRTY, FAIL, "unable to mark extensible array header as dirty");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__hdr_modified() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__hdr_protect
 *
 * Purpose:	Convenience wrapper around protecting extensible array header
 *
 * Return:	Non-NULL pointer to header on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5EA_hdr_t *
H5EA__hdr_protect(H5F_t *f, haddr_t ea_addr, void *ctx_udata, unsigned flags)
{
    H5EA_hdr_t         *hdr;   /* Extensible array header */
    H5EA_hdr_cache_ud_t udata; /* User data for cache callbacks */
    H5EA_hdr_t         *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(f);
    assert(H5_addr_defined(ea_addr));

    /* only the H5AC__READ_ONLY_FLAG may appear in flags */
    assert((flags & (unsigned)(~H5AC__READ_ONLY_FLAG)) == 0);

    /* Set up user data for cache callbacks */
    udata.f         = f;
    udata.addr      = ea_addr;
    udata.ctx_udata = ctx_udata;

    /* Protect the header */
    if (NULL == (hdr = (H5EA_hdr_t *)H5AC_protect(f, H5AC_EARRAY_HDR, ea_addr, &udata, flags)))
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTPROTECT, NULL,
                    "unable to protect extensible array header, address = %llu", (unsigned long long)ea_addr);
    hdr->f = f; /* (Must be set again here, in case the header was already in the cache -QAK) */

    /* Create top proxy, if it doesn't exist */
    if (hdr->swmr_write && NULL == hdr->top_proxy) {
        /* Create 'top' proxy for extensible array entries */
        if (NULL == (hdr->top_proxy = H5AC_proxy_entry_create()))
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTCREATE, NULL, "can't create extensible array entry proxy");

        /* Add header as child of 'top' proxy */
        if (H5AC_proxy_entry_add_child(hdr->top_proxy, f, hdr) < 0)
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTSET, NULL,
                        "unable to add extensible array entry as child of array proxy");
    } /* end if */

    /* Set return value */
    ret_value = hdr;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__hdr_protect() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__hdr_unprotect
 *
 * Purpose:	Convenience wrapper around unprotecting extensible array header
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5EA__hdr_unprotect(H5EA_hdr_t *hdr, unsigned cache_flags)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(hdr);

    /* Unprotect the header */
    if (H5AC_unprotect(hdr->f, H5AC_EARRAY_HDR, hdr->addr, hdr, cache_flags) < 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTUNPROTECT, FAIL,
                    "unable to unprotect extensible array hdr, address = %llu",
                    (unsigned long long)hdr->addr);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__hdr_unprotect() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__hdr_delete
 *
 * Purpose:	Delete an extensible array, starting with the header
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5EA__hdr_delete(H5EA_hdr_t *hdr)
{
    unsigned cache_flags = H5AC__NO_FLAGS_SET; /* Flags for unprotecting header */
    herr_t   ret_value   = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(hdr);
    assert(!hdr->file_rc);

#ifndef NDEBUG
    unsigned hdr_status = 0; /* Array header's status in the metadata cache */

    /* Check the array header's status in the metadata cache */
    if (H5AC_get_entry_status(hdr->f, hdr->addr, &hdr_status) < 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTGET, FAIL, "unable to check metadata cache status for array header");

    /* Sanity checks on array header */
    assert(hdr_status & H5AC_ES__IN_CACHE);
    assert(hdr_status & H5AC_ES__IS_PROTECTED);
#endif /* NDEBUG */

    /* Check for index block */
    if (H5_addr_defined(hdr->idx_blk_addr)) {
        /* Delete index block */
        if (H5EA__iblock_delete(hdr) < 0)
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTDELETE, FAIL, "unable to delete extensible array index block");
    } /* end if */

    /* Set flags to finish deleting header on unprotect */
    cache_flags |= H5AC__DIRTIED_FLAG | H5AC__DELETED_FLAG | H5AC__FREE_FILE_SPACE_FLAG;

done:

    /* Unprotect the header, deleting it if an error hasn't occurred */
    if (H5EA__hdr_unprotect(hdr, cache_flags) < 0)
        HGOTO_ERROR(H5E_EARRAY, H5E_CANTUNPROTECT, FAIL, "unable to release extensible array header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__hdr_delete() */

/*-------------------------------------------------------------------------
 * Function:	H5EA__hdr_dest
 *
 * Purpose:	Destroys an extensible array header in memory.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5EA__hdr_dest(H5EA_hdr_t *hdr)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(hdr);
    assert(hdr->rc == 0);

    /* Destroy the callback context */
    if (hdr->cb_ctx) {
        if ((*hdr->cparam.cls->dst_context)(hdr->cb_ctx) < 0)
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTRELEASE, FAIL,
                        "unable to destroy extensible array client callback context");
    } /* end if */
    hdr->cb_ctx = NULL;

    /* Check for data block element buffer factory info to free */
    if (hdr->elmt_fac.fac) {
        unsigned u; /* Local index variable */

        /* Sanity check */
        assert(hdr->elmt_fac.nalloc > 0);

        /* Iterate over factories, shutting them down */
        for (u = 0; u < hdr->elmt_fac.nalloc; u++) {
            /* Check if this factory has been initialized */
            if (hdr->elmt_fac.fac[u]) {
                if (H5FL_fac_term(hdr->elmt_fac.fac[u]) < 0)
                    HGOTO_ERROR(H5E_EARRAY, H5E_CANTRELEASE, FAIL,
                                "unable to destroy extensible array header factory");
                hdr->elmt_fac.fac[u] = NULL;
            } /* end if */
        }     /* end for */

        /* Free factory array */
        hdr->elmt_fac.fac = (H5FL_fac_head_t **)H5FL_SEQ_FREE(H5FL_fac_head_ptr_t, hdr->elmt_fac.fac);
    } /* end if */

    /* Free the super block info array */
    if (hdr->sblk_info)
        hdr->sblk_info = (H5EA_sblk_info_t *)H5FL_SEQ_FREE(H5EA_sblk_info_t, hdr->sblk_info);

    /* Destroy the 'top' proxy */
    if (hdr->top_proxy) {
        if (H5AC_proxy_entry_dest(hdr->top_proxy) < 0)
            HGOTO_ERROR(H5E_EARRAY, H5E_CANTRELEASE, FAIL, "unable to destroy extensible array 'top' proxy");
        hdr->top_proxy = NULL;
    } /* end if */

    /* Free the shared info itself */
    hdr = H5FL_FREE(H5EA_hdr_t, hdr);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5EA__hdr_dest() */
