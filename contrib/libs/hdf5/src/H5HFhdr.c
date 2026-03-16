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
 * Created:		H5HFhdr.c
 *
 * Purpose:		Heap header routines for fractal heaps.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5HFmodule.h" /* This source code file is part of the H5HF module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5HFpkg.h"     /* Fractal heaps			*/
#include "H5MFprivate.h" /* File memory management		*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5VMprivate.h" /* Vectors and arrays 			*/

/****************/
/* Local Macros */
/****************/

#ifndef NDEBUG
/* Limit on the size of the max. direct block size */
/* (This is limited to 32-bits currently, because I think it's unlikely to
 *      need to be larger, the 32-bit limit for H5VM_log2_of2(n), and
 *      some offsets/sizes are encoded with a maximum of 32-bits  - QAK)
 */
#define H5HF_MAX_DIRECT_SIZE_LIMIT ((hsize_t)2 * 1024 * 1024 * 1024)

/* Limit on the width of the doubling table */
/* (This is limited to 16-bits currently, because I think it's unlikely to
 *      need to be larger, and its encoded with a maximum of 16-bits  - QAK)
 */
#define H5HF_WIDTH_LIMIT (64 * 1024)
#endif /* NDEBUG */

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

/* Declare a free list to manage the H5HF_hdr_t struct */
H5FL_DEFINE_STATIC(H5HF_hdr_t);

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:	H5HF__hdr_alloc
 *
 * Purpose:	Allocate shared fractal heap header
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
H5HF_hdr_t *
H5HF__hdr_alloc(H5F_t *f)
{
    H5HF_hdr_t *hdr       = NULL; /* Shared fractal heap header */
    H5HF_hdr_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);

    /* Allocate space for the shared information */
    if (NULL == (hdr = H5FL_CALLOC(H5HF_hdr_t)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, NULL, "allocation failed for fractal heap shared header");

    /* Set the internal parameters for the heap */
    hdr->f           = f;
    hdr->sizeof_size = H5F_SIZEOF_SIZE(f);
    hdr->sizeof_addr = H5F_SIZEOF_ADDR(f);

    /* Set the return value */
    ret_value = hdr;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__hdr_alloc() */

/*-------------------------------------------------------------------------
 * Function:	H5HF_hdr_free_space
 *
 * Purpose:	Compute direct block free space, for indirect blocks of
 *              different sizes.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__hdr_compute_free_space(H5HF_hdr_t *hdr, unsigned iblock_row)
{
    hsize_t  acc_heap_size;       /* Accumumated heap space */
    hsize_t  iblock_size;         /* Size of indirect block to calculate for */
    hsize_t  acc_dblock_free;     /* Accumumated direct block free space */
    size_t   max_dblock_free;     /* Max. direct block free space */
    unsigned curr_row;            /* Current row in block */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(iblock_row >= hdr->man_dtable.max_direct_rows);

    /* Set the free space in direct blocks */
    acc_heap_size   = 0;
    acc_dblock_free = 0;
    max_dblock_free = 0;
    iblock_size     = hdr->man_dtable.row_block_size[iblock_row];
    curr_row        = 0;
    while (acc_heap_size < iblock_size) {
        acc_heap_size += hdr->man_dtable.row_block_size[curr_row] * hdr->man_dtable.cparam.width;
        acc_dblock_free += hdr->man_dtable.row_tot_dblock_free[curr_row] * hdr->man_dtable.cparam.width;
        if (hdr->man_dtable.row_max_dblock_free[curr_row] > max_dblock_free)
            max_dblock_free = hdr->man_dtable.row_max_dblock_free[curr_row];
        curr_row++;
    } /* end while */

    /* Set direct block free space values for indirect block */
    hdr->man_dtable.row_tot_dblock_free[iblock_row] = acc_dblock_free;
    hdr->man_dtable.row_max_dblock_free[iblock_row] = max_dblock_free;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__hdr_compute_free_space() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__hdr_finish_init_phase1
 *
 * Purpose:	First phase to finish initializing info in shared heap header
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__hdr_finish_init_phase1(H5HF_hdr_t *hdr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);

    /* Compute/cache some values */
    hdr->heap_off_size = (uint8_t)H5HF_SIZEOF_OFFSET_BITS(hdr->man_dtable.cparam.max_index);
    if (H5HF__dtable_init(&hdr->man_dtable) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't initialize doubling table info");

    /* Set the size of heap IDs */
    hdr->heap_len_size =
        (uint8_t)MIN(hdr->man_dtable.max_dir_blk_off_size, H5VM_limit_enc_size((uint64_t)hdr->max_man_size));

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__hdr_finish_init_phase1() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__hdr_finish_init_phase2
 *
 * Purpose:	Second phase to finish initializing info in shared heap header
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__hdr_finish_init_phase2(H5HF_hdr_t *hdr)
{
    unsigned u;                   /* Local index variable */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);

    /* Set the free space in direct blocks */
    for (u = 0; u < hdr->man_dtable.max_root_rows; u++) {
        if (u < hdr->man_dtable.max_direct_rows) {
            hdr->man_dtable.row_tot_dblock_free[u] =
                hdr->man_dtable.row_block_size[u] - H5HF_MAN_ABS_DIRECT_OVERHEAD(hdr);
            H5_CHECKED_ASSIGN(hdr->man_dtable.row_max_dblock_free[u], size_t,
                              hdr->man_dtable.row_tot_dblock_free[u], hsize_t);
        } /* end if */
        else if (H5HF__hdr_compute_free_space(hdr, u) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL,
                        "can't initialize direct block free space for indirect block");
    } /* end for */

    /* Initialize the block iterator for searching for free space */
    if (H5HF__man_iter_init(&hdr->next_block) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't initialize space search block iterator");

    /* Initialize the information for tracking 'huge' objects */
    if (H5HF__huge_init(hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't initialize info for tracking huge objects");

    /* Initialize the information for tracking 'tiny' objects */
    if (H5HF__tiny_init(hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't initialize info for tracking tiny objects");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__hdr_finish_init_phase2() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__hdr_finish_init
 *
 * Purpose:	Finish initializing info in shared heap header
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__hdr_finish_init(H5HF_hdr_t *hdr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);

    /* First phase of header final initialization */
    if (H5HF__hdr_finish_init_phase1(hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't finish phase #1 of header final initialization");

    /* Second phase of header final initialization */
    if (H5HF__hdr_finish_init_phase2(hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't finish phase #2 of header final initialization");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__hdr_finish_init() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__hdr_create
 *
 * Purpose:	Create new fractal heap header
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
haddr_t
H5HF__hdr_create(H5F_t *f, const H5HF_create_t *cparam)
{
    H5HF_hdr_t *hdr = NULL;              /* The new fractal heap header information */
    size_t      dblock_overhead;         /* Direct block's overhead */
    haddr_t     ret_value = HADDR_UNDEF; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);
    assert(cparam);

#ifndef NDEBUG
    /* Check for valid parameters */
    if (cparam->managed.width == 0)
        HGOTO_ERROR(H5E_HEAP, H5E_BADVALUE, HADDR_UNDEF, "width must be greater than zero");
    if (cparam->managed.width > H5HF_WIDTH_LIMIT)
        HGOTO_ERROR(H5E_HEAP, H5E_BADVALUE, HADDR_UNDEF, "width too large");
    if (!POWER_OF_TWO(cparam->managed.width))
        HGOTO_ERROR(H5E_HEAP, H5E_BADVALUE, HADDR_UNDEF, "width not power of two");
    if (cparam->managed.start_block_size == 0)
        HGOTO_ERROR(H5E_HEAP, H5E_BADVALUE, HADDR_UNDEF, "starting block size must be greater than zero");
    if (!POWER_OF_TWO(cparam->managed.start_block_size))
        HGOTO_ERROR(H5E_HEAP, H5E_BADVALUE, HADDR_UNDEF, "starting block size not power of two");
    if (cparam->managed.max_direct_size == 0)
        HGOTO_ERROR(H5E_HEAP, H5E_BADVALUE, HADDR_UNDEF, "max. direct block size must be greater than zero");
    if (cparam->managed.max_direct_size > H5HF_MAX_DIRECT_SIZE_LIMIT)
        HGOTO_ERROR(H5E_HEAP, H5E_BADVALUE, HADDR_UNDEF, "max. direct block size too large");
    if (!POWER_OF_TWO(cparam->managed.max_direct_size))
        HGOTO_ERROR(H5E_HEAP, H5E_BADVALUE, HADDR_UNDEF, "max. direct block size not power of two");
    if (cparam->managed.max_direct_size < cparam->max_man_size)
        HGOTO_ERROR(H5E_HEAP, H5E_BADVALUE, HADDR_UNDEF,
                    "max. direct block size not large enough to hold all managed blocks");
    if (cparam->managed.max_index == 0)
        HGOTO_ERROR(H5E_HEAP, H5E_BADVALUE, HADDR_UNDEF, "max. heap size must be greater than zero");
#endif /* NDEBUG */

    /* Allocate & basic initialization for the shared header */
    if (NULL == (hdr = H5HF__hdr_alloc(f)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, HADDR_UNDEF, "can't allocate space for shared heap info");

#ifndef NDEBUG
    if (cparam->managed.max_index > (unsigned)(8 * hdr->sizeof_size))
        HGOTO_ERROR(H5E_HEAP, H5E_BADVALUE, HADDR_UNDEF, "max. heap size too large for file");
#endif /* NDEBUG */

    /* Set the creation parameters for the heap */
    hdr->max_man_size     = cparam->max_man_size;
    hdr->checksum_dblocks = cparam->checksum_dblocks;
    H5MM_memcpy(&(hdr->man_dtable.cparam), &(cparam->managed), sizeof(H5HF_dtable_cparam_t));

    /* Set root table address to indicate that the heap is empty currently */
    hdr->man_dtable.table_addr = HADDR_UNDEF;

    /* Set free list header address to indicate that the heap is empty currently */
    hdr->fs_addr = HADDR_UNDEF;

    /* Set "huge" object tracker v2 B-tree address to indicate that there aren't any yet */
    hdr->huge_bt2_addr = HADDR_UNDEF;

    /* First phase of header final initialization */
    /* (doesn't need ID length set up) */
    if (H5HF__hdr_finish_init_phase1(hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, HADDR_UNDEF,
                    "can't finish phase #1 of header final initialization");

    /* Copy any I/O filter pipeline */
    /* (This code is not in the "finish init phase" routines because those
     *  routines are also called from the cache 'load' callback, and the filter
     *  length is already set in that case (its stored in the header on disk))
     */
    if (cparam->pline.nused > 0) {
        /* Check if the filters in the DCPL can be applied to this dataset */
        if (H5Z_can_apply_direct(&(cparam->pline)) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, HADDR_UNDEF, "I/O filters can't operate on this heap");

        /* Mark the filters as checked */
        hdr->checked_filters = true;

        /* Make the "set local" filter callbacks for this dataset */
        if (H5Z_set_local_direct(&(cparam->pline)) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, HADDR_UNDEF, "unable to set local filter parameters");

        /* Copy the I/O filter pipeline from the creation parameters to the header */
        if (NULL == H5O_msg_copy(H5O_PLINE_ID, &(cparam->pline), &(hdr->pline)))
            HGOTO_ERROR(H5E_HEAP, H5E_CANTCOPY, HADDR_UNDEF, "can't copy I/O filter pipeline");

        /* Set the version for the I/O pipeline message */
        if (H5O_pline_set_version(hdr->f, &(hdr->pline)) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTSET, HADDR_UNDEF, "can't set version of I/O filter pipeline");

        /* Compute the I/O filters' encoded size */
        if (0 == (hdr->filter_len = (unsigned)H5O_msg_raw_size(hdr->f, H5O_PLINE_ID, false, &(hdr->pline))))
            HGOTO_ERROR(H5E_HEAP, H5E_CANTGETSIZE, HADDR_UNDEF, "can't get I/O filter pipeline size");

        /* Compute size of header on disk */
        hdr->heap_size = H5HF_HEADER_SIZE(hdr) /* Base header size */
                         + hdr->sizeof_size    /* Size of size for filtered root direct block */
                         + 4                   /* Size of filter mask for filtered root direct block */
                         + hdr->filter_len;    /* Size of encoded I/O filter info */
    }                                          /* end if */
    else {
        /* Set size of header on disk */
        hdr->heap_size = H5HF_HEADER_SIZE(hdr);

        /* Mark filters as checked, for performance reasons */
        hdr->checked_filters = true;
    } /* end else */

    /* Set the length of IDs in the heap */
    /* (This code is not in the "finish init phase" routines because those
     *  routines are also called from the cache 'load' callback, and the ID
     *  length is already set in that case (its stored in the header on disk))
     */
    switch (cparam->id_len) {
        case 0: /* Set the length of heap IDs to just enough to hold the offset & length of 'normal' objects
                   in the heap */
            hdr->id_len = (unsigned)1 + hdr->heap_off_size + hdr->heap_len_size;
            break;

        case 1: /* Set the length of heap IDs to just enough to hold the information needed to directly access
                   'huge' objects in the heap */
            if (hdr->filter_len > 0)
                hdr->id_len = (unsigned)1         /* ID flags */
                              + hdr->sizeof_addr  /* Address of filtered object */
                              + hdr->sizeof_size  /* Length of filtered object */
                              + 4                 /* Filter mask for filtered object */
                              + hdr->sizeof_size; /* Size of de-filtered object in memory */
            else
                hdr->id_len = (unsigned)1         /* ID flags */
                              + hdr->sizeof_addr  /* Address of object */
                              + hdr->sizeof_size; /* Length of object */
            break;

        default: /* Use the requested size for the heap ID */
            /* Check boundaries */
            if (cparam->id_len < (1 + hdr->heap_off_size + hdr->heap_len_size))
                HGOTO_ERROR(H5E_HEAP, H5E_BADRANGE, HADDR_UNDEF,
                            "ID length not large enough to hold object IDs");
            else if (cparam->id_len > H5HF_MAX_ID_LEN)
                HGOTO_ERROR(H5E_HEAP, H5E_BADRANGE, HADDR_UNDEF,
                            "ID length too large to store tiny object lengths");

            /* Use the requested size for the heap ID */
            hdr->id_len = cparam->id_len;
            break;
    } /* end switch */

    /* Second phase of header final initialization */
    /* (needs ID and filter lengths set up) */
    if (H5HF__hdr_finish_init_phase2(hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, HADDR_UNDEF,
                    "can't finish phase #2 of header final initialization");

    /* Extra checking for possible gap between max. direct block size minus
     * overhead and "huge" object size */
    dblock_overhead = H5HF_MAN_ABS_DIRECT_OVERHEAD(hdr);
    if ((cparam->managed.max_direct_size - dblock_overhead) < cparam->max_man_size)
        HGOTO_ERROR(H5E_HEAP, H5E_BADVALUE, HADDR_UNDEF,
                    "max. direct block size not large enough to hold all managed blocks");

    /* Allocate space for the header on disk */
    if (HADDR_UNDEF == (hdr->heap_addr = H5MF_alloc(f, H5FD_MEM_FHEAP_HDR, (hsize_t)hdr->heap_size)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, HADDR_UNDEF, "file allocation failed for fractal heap header");

    /* Cache the new fractal heap header */
    if (H5AC_insert_entry(f, H5AC_FHEAP_HDR, hdr->heap_addr, hdr, H5AC__NO_FLAGS_SET) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINSERT, HADDR_UNDEF, "can't add fractal heap header to cache");

    /* Set address of heap header to return */
    ret_value = hdr->heap_addr;

done:
    if (!H5_addr_defined(ret_value) && hdr)
        if (H5HF__hdr_free(hdr) < 0)
            HDONE_ERROR(H5E_HEAP, H5E_CANTRELEASE, HADDR_UNDEF, "unable to release fractal heap header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__hdr_create() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__hdr_protect
 *
 * Purpose:	Convenience wrapper around H5AC_protect on an indirect block
 *
 * Return:	Pointer to indirect block on success, NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5HF_hdr_t *
H5HF__hdr_protect(H5F_t *f, haddr_t addr, unsigned flags)
{
    H5HF_hdr_cache_ud_t cache_udata;      /* User-data for callback */
    H5HF_hdr_t         *hdr;              /* Fractal heap header */
    H5HF_hdr_t         *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(f);
    assert(H5_addr_defined(addr));

    /* only H5AC__READ_ONLY_FLAG may appear in flags */
    assert((flags & (unsigned)(~H5AC__READ_ONLY_FLAG)) == 0);

    /* Set up userdata for protect call */
    cache_udata.f = f;

    /* Lock the heap header into memory */
    if (NULL == (hdr = (H5HF_hdr_t *)H5AC_protect(f, H5AC_FHEAP_HDR, addr, &cache_udata, flags)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, NULL, "unable to protect fractal heap header");

    /* Set the header's address */
    hdr->heap_addr = addr;

    /* Update header's file pointer */
    hdr->f = f;

    /* Set the return value */
    ret_value = hdr;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__hdr_protect() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__hdr_incr
 *
 * Purpose:	Increment component reference count on shared heap header
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__hdr_incr(H5HF_hdr_t *hdr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(hdr);

    /* Mark header as un-evictable when a block is depending on it */
    if (hdr->rc == 0)
        if (H5AC_pin_protected_entry(hdr) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTPIN, FAIL, "unable to pin fractal heap header");

    /* Increment reference count on shared header */
    hdr->rc++;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__hdr_incr() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__hdr_decr
 *
 * Purpose:	Decrement component reference count on shared heap header
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__hdr_decr(H5HF_hdr_t *hdr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(hdr);
    assert(hdr->rc);

    /* Decrement reference count on shared header */
    hdr->rc--;

    /* Mark header as evictable again when no child blocks depend on it */
    if (hdr->rc == 0) {
        assert(hdr->file_rc == 0);
        if (H5AC_unpin_entry(hdr) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTUNPIN, FAIL, "unable to unpin fractal heap header");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__hdr_decr() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__hdr_fuse_incr
 *
 * Purpose:	Increment file reference count on shared heap header
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__hdr_fuse_incr(H5HF_hdr_t *hdr)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(hdr);

    /* Increment file reference count on shared header */
    hdr->file_rc++;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HF__hdr_fuse_incr() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__hdr_fuse_decr
 *
 * Purpose:	Decrement file reference count on shared heap header
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
size_t
H5HF__hdr_fuse_decr(H5HF_hdr_t *hdr)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(hdr);
    assert(hdr->file_rc);

    /* Decrement file reference count on shared header */
    hdr->file_rc--;

    FUNC_LEAVE_NOAPI(hdr->file_rc)
} /* end H5HF__hdr_fuse_decr() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__hdr_dirty
 *
 * Purpose:	Mark heap header as dirty
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__hdr_dirty(H5HF_hdr_t *hdr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(hdr);

    /* Resize pinned header in cache if I/O filter is present. */
    if (hdr->filter_len > 0)
        if (H5AC_resize_entry(hdr, (size_t)hdr->heap_size) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTRESIZE, FAIL, "unable to resize fractal heap header");

    /* Mark header as dirty in cache */
    if (H5AC_mark_entry_dirty(hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTMARKDIRTY, FAIL, "unable to mark fractal heap header as dirty");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__hdr_dirty() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__hdr_adj_free
 *
 * Purpose:	Adjust the free space for a heap
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__hdr_adj_free(H5HF_hdr_t *hdr, ssize_t amt)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);

    /* Update heap header */
    assert(amt > 0 || hdr->total_man_free >= (hsize_t)-amt);
    hdr->total_man_free = (hsize_t)((hssize_t)hdr->total_man_free + amt);

    /* Mark heap header as modified */
    if (H5HF__hdr_dirty(hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTDIRTY, FAIL, "can't mark heap header as dirty");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__hdr_adj_free() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__hdr_adjust_heap
 *
 * Purpose:	Adjust heap space
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__hdr_adjust_heap(H5HF_hdr_t *hdr, hsize_t new_size, hssize_t extra_free)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);

    /* Set the total managed space in heap */
    hdr->man_size = new_size;

    /* Adjust the free space in direct blocks */
    assert(extra_free > 0 || hdr->total_man_free >= (hsize_t)-extra_free);
    hdr->total_man_free = (hsize_t)((hssize_t)hdr->total_man_free + extra_free);

    /* Mark heap header as modified */
    if (H5HF__hdr_dirty(hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTDIRTY, FAIL, "can't mark header as dirty");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__hdr_adjust_heap() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__hdr_inc_alloc
 *
 * Purpose:	Increase allocated size of heap
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__hdr_inc_alloc(H5HF_hdr_t *hdr, size_t alloc_size)
{
    FUNC_ENTER_PACKAGE_NOERR

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(alloc_size);

    /* Update the "allocated" size within the heap */
    hdr->man_alloc_size += alloc_size;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HF__hdr_inc_alloc() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__hdr_start_iter
 *
 * Purpose:	Start "next block" iterator at an offset/entry in the heap
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__hdr_start_iter(H5HF_hdr_t *hdr, H5HF_indirect_t *iblock, hsize_t curr_off, unsigned curr_entry)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(iblock);

    /* Set up "next block" iterator at correct location */
    if (H5HF__man_iter_start_entry(hdr, &hdr->next_block, iblock, curr_entry) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't initialize block iterator");

    /* Set the offset of the iterator in the heap */
    hdr->man_iter_off = curr_off;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__hdr_start_iter() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__hdr_reset_iter
 *
 * Purpose:	Reset "next block" iterator
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__hdr_reset_iter(H5HF_hdr_t *hdr, hsize_t curr_off)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);

    /* Reset "next block" iterator */
    if (H5HF__man_iter_reset(&hdr->next_block) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't reset block iterator");

    /* Set the offset of the iterator in the heap */
    hdr->man_iter_off = curr_off;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__hdr_reset_iter() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__hdr_skip_blocks
 *
 * Purpose:	Add skipped direct blocks to free space for heap
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__hdr_skip_blocks(H5HF_hdr_t *hdr, H5HF_indirect_t *iblock, unsigned start_entry, unsigned nentries)
{
    unsigned row, col;            /* Row & column of entry */
    hsize_t  sect_size;           /* Size of section in heap space */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(iblock);
    assert(nentries);

    /* Compute the span within the heap to skip */
    row       = start_entry / hdr->man_dtable.cparam.width;
    col       = start_entry % hdr->man_dtable.cparam.width;
    sect_size = H5HF__dtable_span_size(&hdr->man_dtable, row, col, nentries);
    assert(sect_size > 0);

    /* Advance the new block iterator */
    if (H5HF__hdr_inc_iter(hdr, sect_size, nentries) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't increase allocated heap size");

    /* Add 'indirect' section for blocks skipped in this row */
    if (H5HF__sect_indirect_add(hdr, iblock, start_entry, nentries) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL,
                    "can't create indirect section for indirect block's free space");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__hdr_skip_blocks() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__hdr_update_iter
 *
 * Purpose:	Update state of heap to account for current iterator
 *              position.
 *
 * Note:	Creates necessary indirect blocks
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__hdr_update_iter(H5HF_hdr_t *hdr, size_t min_dblock_size)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(min_dblock_size > 0);

    /* Check for creating first indirect block */
    if (hdr->man_dtable.curr_root_rows == 0) {
        if (H5HF__man_iblock_root_create(hdr, min_dblock_size) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTEXTEND, FAIL, "unable to create root indirect block");
    } /* end if */
    else {
        H5HF_indirect_t *iblock;                 /* Pointer to indirect block */
        bool             walked_up, walked_down; /* Condition variables for finding direct block location */
        unsigned         next_row;               /* Iterator's next block row */
        unsigned         next_entry;             /* Iterator's next block entry */
        unsigned         min_dblock_row;         /* Minimum row for direct block size request */

        /* Compute min. row for direct block requested */
        min_dblock_row = H5HF__dtable_size_to_row(&hdr->man_dtable, min_dblock_size);

        /* Initialize block iterator, if necessary */
        if (!H5HF__man_iter_ready(&hdr->next_block)) {
            /* Start iterator with previous offset of iterator */
            if (H5HF__man_iter_start_offset(hdr, &hdr->next_block, hdr->man_iter_off) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "unable to set block iterator location");
        } /* end if */

        /* Get information about current iterator location */
        if (H5HF__man_iter_curr(&hdr->next_block, &next_row, NULL, &next_entry, &iblock) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "unable to retrieve current block iterator location");

        /* Check for skipping over blocks in the current block */
        if (min_dblock_row > next_row && next_row < iblock->nrows) {
            unsigned min_entry;    /* Min entry for direct block requested */
            unsigned skip_entries; /* Number of entries to skip in the current block */

            /* Compute the number of entries to skip in the current block */
            min_entry = min_dblock_row * hdr->man_dtable.cparam.width;
            if (min_dblock_row >= iblock->nrows)
                skip_entries = (iblock->nrows * hdr->man_dtable.cparam.width) - next_entry;
            else
                skip_entries = min_entry - next_entry;

            /* Add skipped direct blocks to heap's free space */
            if (H5HF__hdr_skip_blocks(hdr, iblock, next_entry, skip_entries) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTDEC, FAIL, "can't add skipped blocks to heap's free space");

            /* Get information about new iterator location */
            if (H5HF__man_iter_curr(&hdr->next_block, &next_row, NULL, &next_entry, &iblock) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL,
                            "unable to retrieve current block iterator location");
        } /* end if */

        do {
            /* Reset conditions for leaving loop */
            walked_up = walked_down = false;

            /* Check for walking off end of indirect block */
            /* (walk up iterator) */
            while (next_row >= iblock->nrows) {
                /* Check for needing to expand root indirect block */
                if (iblock->parent == NULL) {
                    if (H5HF__man_iblock_root_double(hdr, min_dblock_size) < 0)
                        HGOTO_ERROR(H5E_HEAP, H5E_CANTEXTEND, FAIL, "unable to double root indirect block");
                } /* end if */
                else {
                    /* Move iterator up one level */
                    if (H5HF__man_iter_up(&hdr->next_block) < 0)
                        HGOTO_ERROR(H5E_HEAP, H5E_CANTNEXT, FAIL,
                                    "unable to advance current block iterator location");

                    /* Increment location of next block at this level */
                    if (H5HF__man_iter_next(hdr, &hdr->next_block, 1) < 0)
                        HGOTO_ERROR(H5E_HEAP, H5E_CANTINC, FAIL, "can't advance fractal heap block location");
                } /* end else */

                /* Get information about new iterator location */
                if (H5HF__man_iter_curr(&hdr->next_block, &next_row, NULL, &next_entry, &iblock) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL,
                                "unable to retrieve current block iterator location");

                /* Indicate that we walked up */
                walked_up = true;
            } /* end while */

            /* Check for walking into child indirect block */
            /* (walk down iterator) */
            if (next_row >= hdr->man_dtable.max_direct_rows) {
                unsigned child_nrows; /* Number of rows in new indirect block */

                assert(!H5_addr_defined(iblock->ents[next_entry].addr));

                /* Compute # of rows in next child indirect block to use */
                child_nrows =
                    H5HF__dtable_size_to_rows(&hdr->man_dtable, hdr->man_dtable.row_block_size[next_row]);

                /* Check for skipping over indirect blocks */
                /* (that don't have direct blocks large enough to hold direct block size requested) */
                if (hdr->man_dtable.row_block_size[child_nrows - 1] < min_dblock_size) {
                    unsigned child_rows_needed; /* Number of rows needed to hold direct block */
                    unsigned child_entry;       /* Entry of child indirect block */

                    /* Compute # of rows needed in child indirect block */
                    child_rows_needed = (H5VM_log2_of2((uint32_t)min_dblock_size) -
                                         H5VM_log2_of2((uint32_t)hdr->man_dtable.cparam.start_block_size)) +
                                        2;
                    assert(child_rows_needed > child_nrows);
                    child_entry =
                        (next_row + (child_rows_needed - child_nrows)) * hdr->man_dtable.cparam.width;
                    if (child_entry > (iblock->nrows * hdr->man_dtable.cparam.width))
                        child_entry = iblock->nrows * hdr->man_dtable.cparam.width;

                    /* Add skipped indirect blocks to heap's free space */
                    if (H5HF__hdr_skip_blocks(hdr, iblock, next_entry, (child_entry - next_entry)) < 0)
                        HGOTO_ERROR(H5E_HEAP, H5E_CANTDEC, FAIL,
                                    "can't add skipped blocks to heap's free space");
                } /* end if */
                else {
                    H5HF_indirect_t *new_iblock;      /* Pointer to new indirect block */
                    bool             did_protect;     /* Whether we protected the indirect block or not */
                    haddr_t          new_iblock_addr; /* New indirect block's address */

                    /* Allocate new indirect block */
                    if (H5HF__man_iblock_create(hdr, iblock, next_entry, child_nrows, child_nrows,
                                                &new_iblock_addr) < 0)
                        HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, FAIL,
                                    "can't allocate fractal heap indirect block");

                    /* Lock new indirect block */
                    if (NULL == (new_iblock = H5HF__man_iblock_protect(hdr, new_iblock_addr, child_nrows,
                                                                       iblock, next_entry, false,
                                                                       H5AC__NO_FLAGS_SET, &did_protect)))
                        HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL,
                                    "unable to protect fractal heap indirect block");

                    /* Move iterator down one level (pins indirect block) */
                    if (H5HF__man_iter_down(&hdr->next_block, new_iblock) < 0)
                        HGOTO_ERROR(H5E_HEAP, H5E_CANTNEXT, FAIL,
                                    "unable to advance current block iterator location");

                    /* Check for skipping over rows and add free section for skipped rows */
                    if (min_dblock_size > hdr->man_dtable.cparam.start_block_size) {
                        unsigned new_entry; /* Entry of direct block which is large enough */

                        /* Compute entry for direct block size requested */
                        new_entry = hdr->man_dtable.cparam.width * min_dblock_row;

                        /* Add skipped blocks to heap's free space */
                        if (H5HF__hdr_skip_blocks(hdr, new_iblock, 0, new_entry) < 0)
                            HGOTO_ERROR(H5E_HEAP, H5E_CANTDEC, FAIL,
                                        "can't add skipped blocks to heap's free space");
                    } /* end if */

                    /* Unprotect child indirect block */
                    if (H5HF__man_iblock_unprotect(new_iblock, H5AC__NO_FLAGS_SET, did_protect) < 0)
                        HGOTO_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL,
                                    "unable to release fractal heap indirect block");
                } /* end else */

                /* Get information about new iterator location */
                if (H5HF__man_iter_curr(&hdr->next_block, &next_row, NULL, &next_entry, &iblock) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL,
                                "unable to retrieve current block iterator location");

                /* Indicate that we walked down */
                walked_down = true;
            } /* end if */
        } while (walked_down || walked_up);
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__hdr_update_iter() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__hdr_inc_iter
 *
 * Purpose:	Advance "next block" iterator
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__hdr_inc_iter(H5HF_hdr_t *hdr, hsize_t adv_size, unsigned nentries)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(nentries);

    /* Advance the iterator for the current location within the indirect block */
    if (hdr->next_block.curr)
        if (H5HF__man_iter_next(hdr, &hdr->next_block, nentries) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTNEXT, FAIL, "unable to advance current block iterator location");

    /* Increment the offset of the iterator in the heap */
    hdr->man_iter_off += adv_size;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__hdr_inc_iter() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__hdr_reverse_iter
 *
 * Purpose:	Walk "next block" iterator backwards until the correct
 *              location to allocate next block from is found
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__hdr_reverse_iter(H5HF_hdr_t *hdr, haddr_t dblock_addr)
{
    H5HF_indirect_t *iblock;              /* Indirect block where iterator is located */
    unsigned         curr_entry;          /* Current entry for iterator */
    bool             walked_down;         /* Loop flag */
    bool             walked_up;           /* Loop flag */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);

    /* Initialize block iterator, if necessary */
    if (!H5HF__man_iter_ready(&hdr->next_block))
        /* Start iterator with previous offset of iterator */
        if (H5HF__man_iter_start_offset(hdr, &hdr->next_block, hdr->man_iter_off) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "unable to set block iterator location");

    /* Walk backwards through heap, looking for direct block to place iterator after */

    /* Get information about current iterator location */
    if (H5HF__man_iter_curr(&hdr->next_block, NULL, NULL, &curr_entry, &iblock) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "unable to retrieve current block iterator information");

    /* Move current iterator position backwards once */
    curr_entry--;

    /* Search backwards in the heap address space for direct block to latch onto */
    do {
        int tmp_entry; /* Temp. entry for iterator (use signed value to detect errors) */

        /* Reset loop flags */
        walked_down = false;
        walked_up   = false;

        /* Walk backwards through entries, until we find one that has a child */
        /* (Skip direct block that will be deleted, if we find it) */
        tmp_entry = (int)curr_entry;
        while (tmp_entry >= 0 && (H5_addr_eq(iblock->ents[tmp_entry].addr, dblock_addr) ||
                                  !H5_addr_defined(iblock->ents[tmp_entry].addr)))
            tmp_entry--;
        /* Check for no earlier blocks in this indirect block */
        if (tmp_entry < 0) {
            /* Check for parent of current indirect block */
            if (iblock->parent) {
                /* Move iterator to parent of current block */
                if (H5HF__man_iter_up(&hdr->next_block) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTNEXT, FAIL,
                                "unable to move current block iterator location up");

                /* Get information about current iterator location */
                if (H5HF__man_iter_curr(&hdr->next_block, NULL, NULL, &curr_entry, &iblock) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL,
                                "unable to retrieve current block iterator information");

                /* Move current iterator position backwards once */
                curr_entry--;

                /* Note that we walked up */
                walked_up = true;
            } /* end if */
            else {
                /* Reset iterator offset */
                hdr->man_iter_off = 0;

                /* Reset 'next block' iterator */
                if (H5HF__man_iter_reset(&hdr->next_block) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't reset block iterator");
            } /* end else */
        }     /* end if */
        else {
            unsigned row; /* Row for entry */

            curr_entry = (unsigned)tmp_entry;

            /* Check if entry is for a direct block */
            row = curr_entry / hdr->man_dtable.cparam.width;
            if (row < hdr->man_dtable.max_direct_rows) {
                /* Increment entry to empty location */
                curr_entry++;

                /* Set the current location of the iterator to next entry after the existing direct block */
                if (H5HF__man_iter_set_entry(hdr, &hdr->next_block, curr_entry) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTSET, FAIL, "unable to set current block iterator location");

                /* Update iterator offset */
                hdr->man_iter_off = iblock->block_off;
                hdr->man_iter_off += hdr->man_dtable.row_block_off[curr_entry / hdr->man_dtable.cparam.width];
                hdr->man_iter_off +=
                    hdr->man_dtable.row_block_size[curr_entry / hdr->man_dtable.cparam.width] *
                    (curr_entry % hdr->man_dtable.cparam.width);
            } /* end if */
            else {
                H5HF_indirect_t *child_iblock; /* Pointer to child indirect block */
                bool             did_protect;  /* Whether we protected the indirect block or not */
                unsigned         child_nrows;  /* # of rows in child block */

                /* Compute # of rows in next child indirect block to use */
                child_nrows =
                    H5HF__dtable_size_to_rows(&hdr->man_dtable, hdr->man_dtable.row_block_size[row]);

                /* Lock child indirect block */
                if (NULL == (child_iblock = H5HF__man_iblock_protect(hdr, iblock->ents[curr_entry].addr,
                                                                     child_nrows, iblock, curr_entry, false,
                                                                     H5AC__NO_FLAGS_SET, &did_protect)))
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL,
                                "unable to protect fractal heap indirect block");

                /* Set the current location of the iterator */
                if (H5HF__man_iter_set_entry(hdr, &hdr->next_block, curr_entry) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTSET, FAIL, "unable to set current block iterator location");

                /* Walk down into child indirect block (pins child block) */
                if (H5HF__man_iter_down(&hdr->next_block, child_iblock) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTNEXT, FAIL,
                                "unable to advance current block iterator location");

                /* Update iterator location */
                iblock     = child_iblock;
                curr_entry = (child_iblock->nrows * hdr->man_dtable.cparam.width) - 1;

                /* Unprotect child indirect block */
                if (H5HF__man_iblock_unprotect(child_iblock, H5AC__NO_FLAGS_SET, did_protect) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL,
                                "unable to release fractal heap indirect block");

                /* Note that we walked down */
                walked_down = true;
            } /* end else */
        }     /* end else */
    } while (walked_down || walked_up);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__hdr_reverse_iter() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__hdr_empty
 *
 * Purpose:	Reset heap header to 'empty heap' state
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__hdr_empty(H5HF_hdr_t *hdr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(hdr);

    /* Reset block iterator, if necessary */
    if (H5HF__man_iter_ready(&hdr->next_block))
        if (H5HF__man_iter_reset(&hdr->next_block) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't reset block iterator");

    /* Shrink managed heap size */
    hdr->man_size       = 0;
    hdr->man_alloc_size = 0;

    /* Reset root pointer information */
    hdr->man_dtable.curr_root_rows = 0;
    hdr->man_dtable.table_addr     = HADDR_UNDEF;

    /* Reset the 'next block' iterator location */
    hdr->man_iter_off = 0;

    /* Reset the free space in direct blocks */
    hdr->total_man_free = 0;

    /* Mark heap header as modified */
    if (H5HF__hdr_dirty(hdr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTDIRTY, FAIL, "can't mark header as dirty");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__hdr_empty() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__hdr_free
 *
 * Purpose:	Free shared fractal heap header
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__hdr_free(H5HF_hdr_t *hdr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);

    /* Free the block size lookup table for the doubling table */
    if (H5HF__dtable_dest(&hdr->man_dtable) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "unable to destroy fractal heap doubling table");

    /* Release any I/O pipeline filter information */
    if (hdr->pline.nused)
        if (H5O_msg_reset(H5O_PLINE_ID, &(hdr->pline)) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "unable to reset I/O pipeline message");

    /* Free the shared info itself */
    hdr = H5FL_FREE(H5HF_hdr_t, hdr);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__hdr_free() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__hdr_delete
 *
 * Purpose:	Delete a fractal heap, starting with the header
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__hdr_delete(H5HF_hdr_t *hdr)
{
    unsigned cache_flags = H5AC__NO_FLAGS_SET; /* Flags for unprotecting heap header */
    herr_t   ret_value   = SUCCEED;            /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(!hdr->file_rc);

#ifndef NDEBUG
    {
        unsigned hdr_status = 0; /* Heap header's status in the metadata cache */

        /* Check the heap header's status in the metadata cache */
        if (H5AC_get_entry_status(hdr->f, hdr->heap_addr, &hdr_status) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "unable to check metadata cache status for heap header");

        /* Sanity checks on heap header */
        assert(hdr_status & H5AC_ES__IN_CACHE);
        assert(hdr_status & H5AC_ES__IS_PROTECTED);
    }  /* end block */
#endif /* NDEBUG */

    /* Check for free space manager for heap */
    /* (must occur before attempting to delete the heap, so indirect blocks
     *  will get unpinned)
     */
    if (H5_addr_defined(hdr->fs_addr))
        /* Delete free space manager for heap */
        if (H5HF__space_delete(hdr) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "unable to release fractal heap free space manager");

    /* Check for root direct/indirect block */
    if (H5_addr_defined(hdr->man_dtable.table_addr)) {
        if (hdr->man_dtable.curr_root_rows == 0) {
            hsize_t dblock_size; /* Size of direct block on disk */

            /* Check for I/O filters on this heap */
            if (hdr->filter_len > 0) {
                /* Set the dblock's size */
                dblock_size = hdr->pline_root_direct_size;

                /* Reset the header's pipeline information */
                hdr->pline_root_direct_size        = 0;
                hdr->pline_root_direct_filter_mask = 0;
            } /* end if */
            else
                dblock_size = hdr->man_dtable.cparam.start_block_size;

            /* Delete root direct block */
            if (H5HF__man_dblock_delete(hdr->f, hdr->man_dtable.table_addr, dblock_size) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "unable to release fractal heap root direct block");
        } /* end if */
        else {
            /* Delete root indirect block */
            if (H5HF__man_iblock_delete(hdr, hdr->man_dtable.table_addr, hdr->man_dtable.curr_root_rows, NULL,
                                        0) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL,
                            "unable to release fractal heap root indirect block");
        } /* end else */
    }     /* end if */

    /* Check for 'huge' objects in heap */
    if (H5_addr_defined(hdr->huge_bt2_addr)) {
        /* Delete huge objects in heap and their tracker */
        if (H5HF__huge_delete(hdr) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL,
                        "unable to release fractal heap 'huge' objects and tracker");
    } /* end if */

    /* Indicate that the heap header should be deleted & file space freed */
    cache_flags |= H5AC__DIRTIED_FLAG | H5AC__DELETED_FLAG | H5AC__FREE_FILE_SPACE_FLAG;

done:
    /* Unprotect the header with appropriate flags */
    if (H5AC_unprotect(hdr->f, H5AC_FHEAP_HDR, hdr->heap_addr, hdr, cache_flags) < 0)
        HDONE_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to release fractal heap header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__hdr_delete() */
