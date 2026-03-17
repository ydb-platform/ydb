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
 * Created:     H5FA.c
 *
 * Purpose:     Implements a Fixed Array for storing elements
 *              of datasets with fixed dimensions.
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
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5FApkg.h"     /* Fixed Arrays				*/
#include "H5FLprivate.h" /* Free Lists                           */
#include "H5MMprivate.h" /* Memory management			*/
#include "H5VMprivate.h" /* Vector functions			*/

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
static H5FA_t *H5FA__new(H5F_t *f, haddr_t fa_addr, bool from_open, void *ctx_udata);

/*********************/
/* Package Variables */
/*********************/

/* Fixed array client ID to class mapping */

/* Remember to add client ID to H5FA_cls_id_t in H5FAprivate.h when adding a new
 * client class..
 */
const H5FA_class_t *const H5FA_client_class_g[] = {
    H5FA_CLS_CHUNK,      /* 0 - H5FA_CLS_CHUNK_ID                */
    H5FA_CLS_FILT_CHUNK, /* 1 - H5FA_CLS_FILT_CHUNK_ID           */
    H5FA_CLS_TEST,       /* ? - H5FA_CLS_TEST_ID                 */
};

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Declare a free list to manage the H5FA_t struct */
H5FL_DEFINE_STATIC(H5FA_t);

/* Declare a PQ free list to manage the element */
H5FL_BLK_DEFINE(fa_native_elmt);

/*-------------------------------------------------------------------------
 * Function:	H5FA__new
 *
 * Purpose:	Allocate and initialize a new fixed array wrapper in memory
 *
 * Return:	Pointer to farray wrapper success
 *              NULL on failure
 *
 *-------------------------------------------------------------------------
 */
static H5FA_t *
H5FA__new(H5F_t *f, haddr_t fa_addr, bool from_open, void *ctx_udata)
{
    H5FA_t     *fa        = NULL; /* Pointer to new fixed array */
    H5FA_hdr_t *hdr       = NULL; /* The fixed array header information */
    H5FA_t     *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(f);
    assert(H5_addr_defined(fa_addr));

    /* Allocate fixed array wrapper */
    if (NULL == (fa = H5FL_CALLOC(H5FA_t)))
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTALLOC, NULL, "memory allocation failed for fixed array info");

    /* Lock the array header into memory */
    if (NULL == (hdr = H5FA__hdr_protect(f, fa_addr, ctx_udata, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTPROTECT, NULL, "unable to load fixed array header");

    /* Check for pending array deletion */
    if (from_open && hdr->pending_delete)
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTOPENOBJ, NULL, "can't open fixed array pending deletion");

    /* Point fixed array wrapper at header and bump it's ref count */
    fa->hdr = hdr;
    if (H5FA__hdr_incr(fa->hdr) < 0)
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTINC, NULL, "can't increment reference count on shared array header");

    /* Increment # of files using this array header */
    if (H5FA__hdr_fuse_incr(fa->hdr) < 0)
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTINC, NULL,
                    "can't increment file reference count on shared array header");

    /* Set file pointer for this array open context */
    fa->f = f;

    /* Set the return value */
    ret_value = fa;

done:
    if (hdr && H5FA__hdr_unprotect(hdr, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_FARRAY, H5E_CANTUNPROTECT, NULL, "unable to release fixed array header");
    if (!ret_value)
        if (fa && H5FA_close(fa) < 0)
            HDONE_ERROR(H5E_FARRAY, H5E_CLOSEERROR, NULL, "unable to close fixed array");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FA__new() */

/*-------------------------------------------------------------------------
 * Function:    H5FA_create
 *
 * Purpose:     Creates a new fixed array (header) in the file.
 *
 * Return:      Pointer to fixed array wrapper on success
 *              NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5FA_t *
H5FA_create(H5F_t *f, const H5FA_create_t *cparam, void *ctx_udata)
{
    H5FA_t *fa = NULL; /* Pointer to new fixed array */
    haddr_t fa_addr;   /* Fixed array header address */
    H5FA_t *ret_value = NULL;

    FUNC_ENTER_NOAPI(NULL)

    /* Check arguments */
    assert(f);
    assert(cparam);

    /* H5FA interface sanity check */
    HDcompile_assert(H5FA_NUM_CLS_ID == NELMTS(H5FA_client_class_g));

    /* Create fixed array header */
    if (HADDR_UNDEF == (fa_addr = H5FA__hdr_create(f, cparam, ctx_udata)))
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTINIT, NULL, "can't create fixed array header");

    /* Allocate and initialize new fixed array wrapper */
    if (NULL == (fa = H5FA__new(f, fa_addr, false, ctx_udata)))
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTINIT, NULL,
                    "allocation and/or initialization failed for fixed array wrapper");

    /* Set the return value */
    ret_value = fa;

done:
    if (!ret_value)
        if (fa && H5FA_close(fa) < 0)
            HDONE_ERROR(H5E_FARRAY, H5E_CLOSEERROR, NULL, "unable to close fixed array");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FA_create() */

/*-------------------------------------------------------------------------
 * Function:    H5FA_open
 *
 * Purpose:     Opens an existing fixed array in the file.
 *
 * Return:      Pointer to array wrapper on success
 *              NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5FA_t *
H5FA_open(H5F_t *f, haddr_t fa_addr, void *ctx_udata)
{
    H5FA_t *fa        = NULL; /* Pointer to new fixed array wrapper */
    H5FA_t *ret_value = NULL;

    FUNC_ENTER_NOAPI(NULL)

    /* Check arguments */
    assert(f);
    assert(H5_addr_defined(fa_addr));

    /* Allocate and initialize new fixed array wrapper */
    if (NULL == (fa = H5FA__new(f, fa_addr, true, ctx_udata)))
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTINIT, NULL,
                    "allocation and/or initialization failed for fixed array wrapper");

    /* Set the return value */
    ret_value = fa;

done:
    if (!ret_value)
        if (fa && H5FA_close(fa) < 0)
            HDONE_ERROR(H5E_FARRAY, H5E_CLOSEERROR, NULL, "unable to close fixed array");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FA_open() */

/*-------------------------------------------------------------------------
 * Function:    H5FA_get_nelmts
 *
 * Purpose:     Query the current number of elements in array
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FA_get_nelmts(const H5FA_t *fa, hsize_t *nelmts)
{
    FUNC_ENTER_NOAPI_NOERR

    /* Check arguments */
    assert(fa);
    assert(nelmts);

    /* Retrieve the current number of elements in the fixed array */
    *nelmts = fa->hdr->stats.nelmts;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FA_get_nelmts() */

/*-------------------------------------------------------------------------
 * Function:    H5FA_get_addr
 *
 * Purpose:     Query the address of the array
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FA_get_addr(const H5FA_t *fa, haddr_t *addr)
{
    FUNC_ENTER_NOAPI_NOERR

    /* Check arguments */
    assert(fa);
    assert(fa->hdr);
    assert(addr);

    /* Retrieve the address of the fixed array's header */
    *addr = fa->hdr->addr;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FA_get_addr() */

/*-------------------------------------------------------------------------
 * Function:    H5FA_set
 *
 * Purpose:     Set an element of a fixed array
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FA_set(const H5FA_t *fa, hsize_t idx, const void *elmt)
{
    H5FA_hdr_t       *hdr       = fa->hdr;            /* Header for fixed array */
    H5FA_dblock_t    *dblock    = NULL;               /* Pointer to fixed array Data block */
    H5FA_dblk_page_t *dblk_page = NULL;               /* Pointer to fixed array Data block page */
    unsigned dblock_cache_flags = H5AC__NO_FLAGS_SET; /* Flags to unprotecting fixed array Data block */
    unsigned dblk_page_cache_flags =
        H5AC__NO_FLAGS_SET;   /* Flags to unprotecting FIxed Array Data block page */
    bool   hdr_dirty = false; /* Whether header information changed */
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments */
    assert(fa);
    assert(fa->hdr);

    /* Set the shared array header's file context for this operation */
    hdr->f = fa->f;

    /* Check if we need to create the fixed array data block */
    if (!H5_addr_defined(hdr->dblk_addr)) {
        /* Create the data block */
        hdr->dblk_addr = H5FA__dblock_create(hdr, &hdr_dirty);
        if (!H5_addr_defined(hdr->dblk_addr))
            HGOTO_ERROR(H5E_FARRAY, H5E_CANTCREATE, FAIL, "unable to create fixed array data block");
    }

    assert(idx < hdr->cparam.nelmts);

    /* Protect data block */
    if (NULL == (dblock = H5FA__dblock_protect(hdr, hdr->dblk_addr, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTPROTECT, FAIL,
                    "unable to protect fixed array data block, address = %llu",
                    (unsigned long long)hdr->dblk_addr);

    /* Check for paging data block */
    if (!dblock->npages) {
        /* Set element in data block */
        H5MM_memcpy(((uint8_t *)dblock->elmts) + (hdr->cparam.cls->nat_elmt_size * idx), elmt,
                    hdr->cparam.cls->nat_elmt_size);
        dblock_cache_flags |= H5AC__DIRTIED_FLAG;
    }                             /* end if */
    else {                        /* paging */
        size_t  page_idx;         /* Index of page within data block */
        size_t  dblk_page_nelmts; /* # of elements in a data block page */
        size_t  elmt_idx;         /* Element index within the page */
        haddr_t dblk_page_addr;   /* Address of data block page */

        /* Compute the page & element index */
        page_idx = (size_t)(idx / dblock->dblk_page_nelmts);
        elmt_idx = (size_t)(idx % dblock->dblk_page_nelmts);

        /* Get the address of the data block page */
        dblk_page_addr =
            dblock->addr + H5FA_DBLOCK_PREFIX_SIZE(dblock) + ((hsize_t)page_idx * dblock->dblk_page_size);

        /* Check for using last page, to set the number of elements on the page */
        if ((page_idx + 1) == dblock->npages)
            dblk_page_nelmts = dblock->last_page_nelmts;
        else
            dblk_page_nelmts = dblock->dblk_page_nelmts;

        /* Check if the page has been created yet */
        if (!H5VM_bit_get(dblock->dblk_page_init, page_idx)) {
            /* Create the data block page */
            if (H5FA__dblk_page_create(hdr, dblk_page_addr, dblk_page_nelmts) < 0)
                HGOTO_ERROR(H5E_FARRAY, H5E_CANTCREATE, FAIL, "unable to create data block page");

            /* Mark data block page as initialized in data block */
            H5VM_bit_set(dblock->dblk_page_init, page_idx, true);
            dblock_cache_flags |= H5AC__DIRTIED_FLAG;
        } /* end if */

        /* Protect the data block page */
        if (NULL ==
            (dblk_page = H5FA__dblk_page_protect(hdr, dblk_page_addr, dblk_page_nelmts, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_FARRAY, H5E_CANTPROTECT, FAIL,
                        "unable to protect fixed array data block page, address = %llu",
                        (unsigned long long)dblk_page_addr);

        /* Set the element in the data block page */
        H5MM_memcpy(((uint8_t *)dblk_page->elmts) + (hdr->cparam.cls->nat_elmt_size * elmt_idx), elmt,
                    hdr->cparam.cls->nat_elmt_size);
        dblk_page_cache_flags |= H5AC__DIRTIED_FLAG;
    } /* end else */

done:
    /* Check for header modified */
    if (hdr_dirty)
        if (H5FA__hdr_modified(hdr) < 0)
            HDONE_ERROR(H5E_FARRAY, H5E_CANTMARKDIRTY, FAIL, "unable to mark fixed array header as modified");

    /* Release resources */
    if (dblock && H5FA__dblock_unprotect(dblock, dblock_cache_flags) < 0)
        HDONE_ERROR(H5E_FARRAY, H5E_CANTUNPROTECT, FAIL, "unable to release fixed array data block");
    if (dblk_page && H5FA__dblk_page_unprotect(dblk_page, dblk_page_cache_flags) < 0)
        HDONE_ERROR(H5E_FARRAY, H5E_CANTUNPROTECT, FAIL, "unable to release fixed array data block page");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FA_set() */

/*-------------------------------------------------------------------------
 * Function:    H5FA_get
 *
 * Purpose:     Get an element of a fixed array
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FA_get(const H5FA_t *fa, hsize_t idx, void *elmt)
{
    H5FA_hdr_t       *hdr       = fa->hdr; /* Header for FA */
    H5FA_dblock_t    *dblock    = NULL;    /* Pointer to data block for FA */
    H5FA_dblk_page_t *dblk_page = NULL;    /* Pointer to data block page for FA */
    herr_t            ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments */
    assert(fa);
    assert(fa->hdr);

    /* Set the shared array header's file context for this operation */
    hdr->f = fa->f;

    /* Check if the fixed array data block has been allocated on disk yet */
    if (!H5_addr_defined(hdr->dblk_addr)) {
        /* Call the class's 'fill' callback */
        if ((hdr->cparam.cls->fill)(elmt, (size_t)1) < 0)
            HGOTO_ERROR(H5E_FARRAY, H5E_CANTSET, FAIL, "can't set element to class's fill value");
    } /* end if */
    else {
        /* Get the data block */
        assert(H5_addr_defined(hdr->dblk_addr));
        if (NULL == (dblock = H5FA__dblock_protect(hdr, hdr->dblk_addr, H5AC__READ_ONLY_FLAG)))
            HGOTO_ERROR(H5E_FARRAY, H5E_CANTPROTECT, FAIL,
                        "unable to protect fixed array data block, address = %llu",
                        (unsigned long long)hdr->dblk_addr);

        /* Check for paged data block */
        if (!dblock->npages)
            /* Retrieve element from data block */
            H5MM_memcpy(elmt, ((uint8_t *)dblock->elmts) + (hdr->cparam.cls->nat_elmt_size * idx),
                        hdr->cparam.cls->nat_elmt_size);
        else {               /* paging */
            size_t page_idx; /* Index of page within data block */

            /* Compute the page index */
            page_idx = (size_t)(idx / dblock->dblk_page_nelmts);

            /* Check if the page is defined yet */
            if (!H5VM_bit_get(dblock->dblk_page_init, page_idx)) {
                /* Call the class's 'fill' callback */
                if ((hdr->cparam.cls->fill)(elmt, (size_t)1) < 0)
                    HGOTO_ERROR(H5E_FARRAY, H5E_CANTSET, FAIL, "can't set element to class's fill value");

                /* We've retrieved the value, leave now */
                HGOTO_DONE(SUCCEED);
            }                             /* end if */
            else {                        /* get the page */
                size_t  dblk_page_nelmts; /* # of elements in a data block page */
                size_t  elmt_idx;         /* Element index within the page */
                haddr_t dblk_page_addr;   /* Address of data block page */

                /* Compute the element index */
                elmt_idx = (size_t)(idx % dblock->dblk_page_nelmts);

                /* Compute the address of the data block */
                dblk_page_addr = dblock->addr + H5FA_DBLOCK_PREFIX_SIZE(dblock) +
                                 ((hsize_t)page_idx * dblock->dblk_page_size);

                /* Check for using last page, to set the number of elements on the page */
                if ((page_idx + 1) == dblock->npages)
                    dblk_page_nelmts = dblock->last_page_nelmts;
                else
                    dblk_page_nelmts = dblock->dblk_page_nelmts;

                /* Protect the data block page */
                if (NULL == (dblk_page = H5FA__dblk_page_protect(hdr, dblk_page_addr, dblk_page_nelmts,
                                                                 H5AC__READ_ONLY_FLAG)))
                    HGOTO_ERROR(H5E_FARRAY, H5E_CANTPROTECT, FAIL,
                                "unable to protect fixed array data block page, address = %llu",
                                (unsigned long long)dblk_page_addr);

                /* Retrieve element from data block */
                H5MM_memcpy(elmt, ((uint8_t *)dblk_page->elmts) + (hdr->cparam.cls->nat_elmt_size * elmt_idx),
                            hdr->cparam.cls->nat_elmt_size);
            } /* end else */
        }     /* end else */
    }         /* end else */

done:
    if (dblock && H5FA__dblock_unprotect(dblock, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_FARRAY, H5E_CANTUNPROTECT, FAIL, "unable to release fixed array data block");
    if (dblk_page && H5FA__dblk_page_unprotect(dblk_page, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_FARRAY, H5E_CANTUNPROTECT, FAIL, "unable to release fixed array data block page");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FA_get() */

/*-------------------------------------------------------------------------
 * Function:    H5FA_close
 *
 * Purpose:     Close a fixed array
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FA_close(H5FA_t *fa)
{
    bool    pending_delete = false;       /* Whether the array is pending deletion */
    haddr_t fa_addr        = HADDR_UNDEF; /* Address of array (for deletion) */
    herr_t  ret_value      = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments */
    assert(fa);

    /* Close the header if it was set */
    if (fa->hdr) {
        /* Decrement file reference & check if this is the last open fixed array using the shared array header
         */
        if (0 == H5FA__hdr_fuse_decr(fa->hdr)) {
            /* Set the shared array header's file context for this operation */
            fa->hdr->f = fa->f;

            /* Shut down anything that can't be put in the header's 'flush' callback */

            /* Check for pending array deletion */
            if (fa->hdr->pending_delete) {
                /* Set local info, so array deletion can occur after decrementing the
                 *  header's ref count
                 */
                pending_delete = true;
                fa_addr        = fa->hdr->addr;
            } /* end if */
        }     /* end if */

        /* Check for pending array deletion */
        if (pending_delete) {
            H5FA_hdr_t *hdr; /* Another pointer to fixed array header */

#ifndef NDEBUG
            {
                unsigned hdr_status = 0; /* Header's status in the metadata cache */

                /* Check the header's status in the metadata cache */
                if (H5AC_get_entry_status(fa->f, fa_addr, &hdr_status) < 0)
                    HGOTO_ERROR(H5E_FARRAY, H5E_CANTGET, FAIL,
                                "unable to check metadata cache status for fixed array header");

                /* Sanity checks on header */
                assert(hdr_status & H5AC_ES__IN_CACHE);
                assert(hdr_status & H5AC_ES__IS_PINNED);
                assert(!(hdr_status & H5AC_ES__IS_PROTECTED));
            }
#endif /* NDEBUG */

            /* Lock the array header into memory */
            /* (OK to pass in NULL for callback context, since we know the header must be in the cache) */
            if (NULL == (hdr = H5FA__hdr_protect(fa->f, fa_addr, NULL, H5AC__NO_FLAGS_SET)))
                HGOTO_ERROR(H5E_FARRAY, H5E_CANTLOAD, FAIL, "unable to load fixed array header");

            /* Set the shared array header's file context for this operation */
            hdr->f = fa->f;

            /* Decrement the reference count on the array header */
            /* (don't put in H5FA_hdr_fuse_decr() as the array header may be evicted
             *  immediately -QAK)
             */
            if (H5FA__hdr_decr(fa->hdr) < 0)
                HGOTO_ERROR(H5E_FARRAY, H5E_CANTDEC, FAIL,
                            "can't decrement reference count on shared array header");

            /* Delete array, starting with header (unprotects header) */
            if (H5FA__hdr_delete(hdr) < 0)
                HGOTO_ERROR(H5E_FARRAY, H5E_CANTDELETE, FAIL, "unable to delete fixed array");
        } /* end if */
        else {
            /* Decrement the reference count on the array header */
            /* (don't put in H5FA_hdr_fuse_decr() as the array header may be evicted
             *  immediately -QAK)
             */
            if (H5FA__hdr_decr(fa->hdr) < 0)
                HGOTO_ERROR(H5E_FARRAY, H5E_CANTDEC, FAIL,
                            "can't decrement reference count on shared array header");
        } /* end else */
    }     /* end if */

    /* Release the fixed array wrapper */
    fa = H5FL_FREE(H5FA_t, fa);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FA_close() */

/*-------------------------------------------------------------------------
 * Function:    H5FA_delete
 *
 * Purpose:     Delete a fixed array
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FA_delete(H5F_t *f, haddr_t fa_addr, void *ctx_udata)
{
    H5FA_hdr_t *hdr       = NULL; /* The fixed array header information */
    herr_t      ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments */
    assert(f);
    assert(H5_addr_defined(fa_addr));

    /* Lock the array header into memory */
    if (NULL == (hdr = H5FA__hdr_protect(f, fa_addr, ctx_udata, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTPROTECT, FAIL, "unable to protect fixed array header, address = %llu",
                    (unsigned long long)fa_addr);

    /* Check for files using shared array header */
    if (hdr->file_rc)
        hdr->pending_delete = true;
    else {
        /* Set the shared array header's file context for this operation */
        hdr->f = f;

        /* Delete array now, starting with header (unprotects header) */
        if (H5FA__hdr_delete(hdr) < 0)
            HGOTO_ERROR(H5E_FARRAY, H5E_CANTDELETE, FAIL, "unable to delete fixed array");
        hdr = NULL;
    }

done:
    /* Unprotect the header if an error occurred */
    if (hdr && H5FA__hdr_unprotect(hdr, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_FARRAY, H5E_CANTUNPROTECT, FAIL, "unable to release fixed array header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FA_delete() */

/*-------------------------------------------------------------------------
 * Function:    H5FA_iterate
 *
 * Purpose:     Iterate over the elements of a fixed array
 *
 * Note:        This is not very efficient, we should be iterating directly
 *              over the fixed array's direct block [pages].
 *
 * Return:      H5_ITER_CONT/H5_ITER_ERROR
 *
 *-------------------------------------------------------------------------
 */
int
H5FA_iterate(H5FA_t *fa, H5FA_operator_t op, void *udata)
{
    uint8_t *elmt = NULL;
    hsize_t  u;
    int      ret_value = H5_ITER_CONT;

    FUNC_ENTER_NOAPI(H5_ITER_ERROR)

    /* Check arguments */
    assert(fa);
    assert(op);
    assert(udata);

    /* Allocate space for a native array element */
    if (NULL == (elmt = H5FL_BLK_MALLOC(fa_native_elmt, fa->hdr->cparam.cls->nat_elmt_size)))
        HGOTO_ERROR(H5E_FARRAY, H5E_CANTALLOC, H5_ITER_ERROR,
                    "memory allocation failed for fixed array element");

    /* Iterate over all elements in array */
    for (u = 0; u < fa->hdr->stats.nelmts && ret_value == H5_ITER_CONT; u++) {
        /* Get array element */
        if (H5FA_get(fa, u, elmt) < 0)
            HGOTO_ERROR(H5E_FARRAY, H5E_CANTGET, H5_ITER_ERROR, "unable to delete fixed array");

        /* Invoke callback */
        if ((ret_value = (*op)(u, elmt, udata)) < 0) {
            HERROR(H5E_FARRAY, H5E_BADITER, "iteration callback error");
            break;
        }
    }

done:
    if (elmt)
        elmt = H5FL_BLK_FREE(fa_native_elmt, elmt);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FA_iterate() */

/*-------------------------------------------------------------------------
 * Function:    H5FA_depend
 *
 * Purpose:     Make a child flush dependency between the fixed array
 *              and another piece of metadata in the file.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FA_depend(H5FA_t *fa, H5AC_proxy_entry_t *parent)
{
    H5FA_hdr_t *hdr       = fa->hdr; /* Header for FA */
    herr_t      ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments */
    assert(fa);
    assert(hdr);
    assert(parent);

    /*
     * Check to see if a flush dependency between the fixed array
     * and another data structure in the file has already been set up.
     * If it hasn't, do so now.
     */
    if (NULL == hdr->parent) {
        /* Sanity check */
        assert(hdr->top_proxy);

        /* Set the shared array header's file context for this operation */
        hdr->f = fa->f;

        /* Add the fixed array as a child of the parent (proxy) */
        if (H5AC_proxy_entry_add_child(parent, hdr->f, hdr->top_proxy) < 0)
            HGOTO_ERROR(H5E_FARRAY, H5E_CANTSET, FAIL, "unable to add fixed array as child of proxy");
        hdr->parent = parent;
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FA_depend() */

/*-------------------------------------------------------------------------
 * Function:    H5FA_patch_file
 *
 * Purpose:     Patch the top-level file pointer contained in fa
 *              to point to idx_info->f if they are different.
 *              This is possible because the file pointer in fa can be
 *              closed out if fa remains open.
 *
 * Return:      SUCCEED
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FA_patch_file(H5FA_t *fa, H5F_t *f)
{
    FUNC_ENTER_NOAPI_NOERR

    /* Check arguments */
    assert(fa);
    assert(f);

    if (fa->f != f || fa->hdr->f != f)
        fa->f = fa->hdr->f = f;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FA_patch_file() */
