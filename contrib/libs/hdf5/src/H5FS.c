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
 * Purpose:     Free space tracking functions.
 *
 * Note:        (Used to be in the H5HFflist.c file, prior to the date above)
 */

/****************/
/* Module Setup */
/****************/

#include "H5FSmodule.h" /* This source code file is part of the H5FS module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5ACprivate.h" /* Metadata cache                           */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5FSpkg.h"     /* File free space                          */
#include "H5MFprivate.h" /* File memory management                   */
#include "H5MMprivate.h" /* Memory management                        */

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

/* Section info routines */
static herr_t H5FS__sinfo_free_sect_cb(void *item, void *key, void *op_data);
static herr_t H5FS__sinfo_free_node_cb(void *item, void *key, void *op_data);

/*********************/
/* Package Variables */
/*********************/

/* Declare a free list to manage the H5FS_section_class_t sequence information */
H5FL_SEQ_DEFINE(H5FS_section_class_t);

/* Declare a free list to manage the H5FS_t struct */
H5FL_DEFINE(H5FS_t);

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:    H5FS_create
 *
 * Purpose:     Allocate & initialize file free space info
 *
 * Return:      Success:    Pointer to free space structure
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
H5FS_t *
H5FS_create(H5F_t *f, haddr_t *fs_addr, const H5FS_create_t *fs_create, uint16_t nclasses,
            const H5FS_section_class_t *classes[], void *cls_init_udata, hsize_t alignment, hsize_t threshold)
{
    H5FS_t *fspace    = NULL; /* New free space structure */
    H5FS_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)
#ifdef H5FS_DEBUG
    fprintf(stderr, "%s: Creating free space manager, nclasses = %Zu\n", __func__, nclasses);
#endif /* H5FS_DEBUG */

    /* Check arguments. */
    assert(fs_create->shrink_percent);
    assert(fs_create->shrink_percent < fs_create->expand_percent);
    assert(fs_create->max_sect_size);
    assert(nclasses == 0 || classes);

    /*
     * Allocate free space structure
     */
    if (NULL == (fspace = H5FS__new(f, nclasses, classes, cls_init_udata)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed for free space free list");

    /* Initialize creation information for free space manager */
    fspace->client         = fs_create->client;
    fspace->shrink_percent = fs_create->shrink_percent;
    fspace->expand_percent = fs_create->expand_percent;
    fspace->max_sect_addr  = fs_create->max_sect_addr;
    fspace->max_sect_size  = fs_create->max_sect_size;
    fspace->swmr_write     = (H5F_INTENT(f) & H5F_ACC_SWMR_WRITE) > 0;

    fspace->alignment   = alignment;
    fspace->align_thres = threshold;

    /* Check if the free space tracker is supposed to be persistent */
    if (fs_addr) {
        /* Allocate space for the free space header */
        if (HADDR_UNDEF == (fspace->addr = H5MF_alloc(f, H5FD_MEM_FSPACE_HDR, (hsize_t)fspace->hdr_size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "file allocation failed for free space header");

        /* Cache the new free space header (pinned) */
        if (H5AC_insert_entry(f, H5AC_FSPACE_HDR, fspace->addr, fspace, H5AC__PIN_ENTRY_FLAG) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTINIT, NULL, "can't add free space header to cache");

        /* Return free space header address to caller, if desired */
        *fs_addr = fspace->addr;
    } /* end if */

    /* Set the reference count to 1, since we inserted the entry in the cache pinned */
    fspace->rc = 1;

    /* Set the return value */
    ret_value = fspace;
#ifdef H5FS_DEBUG
    fprintf(stderr, "%s: fspace = %p, fspace->addr = %" PRIuHADDR "\n", __func__, (void *)fspace,
            fspace->addr);
#endif /* H5FS_DEBUG */

done:
    if (!ret_value && fspace)
        if (H5FS__hdr_dest(fspace) < 0)
            HDONE_ERROR(H5E_FSPACE, H5E_CANTFREE, NULL, "unable to destroy free space header");

#ifdef H5FS_DEBUG
    fprintf(stderr, "%s: Leaving, ret_value = %p\n", __func__, (void *)ret_value);
#endif /* H5FS_DEBUG */
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS_create() */

/*-------------------------------------------------------------------------
 * Function:    H5FS_open
 *
 * Purpose:     Open an existing file free space info structure on disk
 *
 * Return:      Success:    Pointer to free space structure
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
H5FS_t *
H5FS_open(H5F_t *f, haddr_t fs_addr, uint16_t nclasses, const H5FS_section_class_t *classes[],
          void *cls_init_udata, hsize_t alignment, hsize_t threshold)
{
    H5FS_t             *fspace = NULL;    /* New free space structure */
    H5FS_hdr_cache_ud_t cache_udata;      /* User-data for metadata cache callback */
    H5FS_t             *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)
#ifdef H5FS_DEBUG
    fprintf(stderr, "%s: Opening free space manager, fs_addr = %" PRIuHADDR ", nclasses = %Zu\n", __func__,
            fs_addr, nclasses);
#endif /* H5FS_DEBUG */

    /* Check arguments. */
    assert(H5_addr_defined(fs_addr));
    assert(nclasses);
    assert(classes);

    /* Initialize user data for protecting the free space manager */
    cache_udata.f              = f;
    cache_udata.nclasses       = nclasses;
    cache_udata.classes        = classes;
    cache_udata.cls_init_udata = cls_init_udata;
    cache_udata.addr           = fs_addr;

    /* Protect the free space header */
    if (NULL ==
        (fspace = (H5FS_t *)H5AC_protect(f, H5AC_FSPACE_HDR, fs_addr, &cache_udata, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTPROTECT, NULL, "unable to load free space header");
#ifdef H5FS_DEBUG
    fprintf(stderr, "%s: fspace->sect_addr = %" PRIuHADDR "\n", __func__, fspace->sect_addr);
    fprintf(stderr, "%s: fspace->sect_size = %" PRIuHSIZE "\n", __func__, fspace->sect_size);
    fprintf(stderr, "%s: fspace->alloc_sect_size = %" PRIuHSIZE "\n", __func__, fspace->alloc_sect_size);
    fprintf(stderr, "%s: fspace->sinfo = %p\n", __func__, (void *)fspace->sinfo);
    fprintf(stderr, "%s: fspace->rc = %u\n", __func__, fspace->rc);
#endif /* H5FS_DEBUG */

    /* Increment the reference count on the free space manager header */
    assert(fspace->rc <= 1);
    if (H5FS__incr(fspace) < 0)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTINC, NULL, "unable to increment ref. count on free space header");

    fspace->alignment   = alignment;
    fspace->align_thres = threshold;

    /* Unlock free space header */
    if (H5AC_unprotect(f, H5AC_FSPACE_HDR, fs_addr, fspace, H5AC__NO_FLAGS_SET) < 0)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTUNPROTECT, NULL, "unable to release free space header");

    /* Set return value */
    ret_value = fspace;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS_open() */

/*-------------------------------------------------------------------------
 * Function:    H5FS_delete
 *
 * Purpose:     Delete a free space manager on disk
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FS_delete(H5F_t *f, haddr_t fs_addr)
{
    H5FS_t             *fspace = NULL;       /* Free space header loaded from file */
    H5FS_hdr_cache_ud_t cache_udata;         /* User-data for metadata cache callback */
    herr_t              ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)
#ifdef H5FS_DEBUG
    fprintf(stderr, "%s: Deleting free space manager, fs_addr = %" PRIuHADDR "\n", __func__, fs_addr);
#endif /* H5FS_DEBUG */

    /* Check arguments. */
    assert(f);
    assert(H5_addr_defined(fs_addr));

    /* Initialize user data for protecting the free space manager */
    /* (no class information necessary for delete) */
    cache_udata.f              = f;
    cache_udata.nclasses       = 0;
    cache_udata.classes        = NULL;
    cache_udata.cls_init_udata = NULL;
    cache_udata.addr           = fs_addr;

#ifdef H5FS_DEBUG
    {
        unsigned fspace_status = 0; /* Free space section info's status in the metadata cache */

        /* Sanity check */
        assert(H5_addr_defined(fs_addr));

        /* Check the free space section info's status in the metadata cache */
        if (H5AC_get_entry_status(f, fs_addr, &fspace_status) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL,
                        "unable to check metadata cache status for free space section info");

        fprintf(stderr, "%s: fspace_status = %0x: ", __func__, fspace_status);
        if (fspace_status) {
            bool printed = false;

            if (fspace_status & H5AC_ES__IN_CACHE) {
                fprintf(stderr, "H5AC_ES__IN_CACHE");
                printed = true;
            } /* end if */
            if (fspace_status & H5AC_ES__IS_DIRTY) {
                fprintf(stderr, "%sH5AC_ES__IS_DIRTY", (printed ? " | " : ""));
                printed = true;
            } /* end if */
            if (fspace_status & H5AC_ES__IS_PROTECTED) {
                fprintf(stderr, "%sH5AC_ES__IS_PROTECTED", (printed ? " | " : ""));
                printed = true;
            } /* end if */
            if (fspace_status & H5AC_ES__IS_PINNED) {
                fprintf(stderr, "%sH5AC_ES__IS_PINNED", (printed ? " | " : ""));
                printed = true;
            } /* end if */
            if (fspace_status & H5AC_ES__IS_FLUSH_DEP_PARENT) {
                fprintf(stderr, "%sH5AC_ES__IS_FLUSH_DEP_PARENT", (printed ? " | " : ""));
                printed = true;
            } /* end if */
            if (fspace_status & H5AC_ES__IS_FLUSH_DEP_CHILD) {
                fprintf(stderr, "%sH5AC_ES__IS_FLUSH_DEP_CHILD", (printed ? " | " : ""));
                printed = true;
            } /* end if */
        }     /* end if */
        fprintf(stderr, "\n");
    }
#endif /* H5FS_DEBUG */

    /* Protect the free space header */
    if (NULL ==
        (fspace = (H5FS_t *)H5AC_protect(f, H5AC_FSPACE_HDR, fs_addr, &cache_udata, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTPROTECT, FAIL, "unable to protect free space header");

    /* Sanity check */
    assert(fspace->sinfo == NULL);

    /* Delete serialized section storage, if there are any */
#ifdef H5FS_DEBUG
    fprintf(stderr, "%s: fspace->sect_addr = %" PRIuHADDR "\n", __func__, fspace->sect_addr);
#endif /* H5FS_DEBUG */
    if (fspace->serial_sect_count > 0) {
        unsigned sinfo_status = 0; /* Free space section info's status in the metadata cache */

        /* Sanity check */
        assert(H5_addr_defined(fspace->sect_addr));
        assert(fspace->alloc_sect_size > 0);

        /* Check the free space section info's status in the metadata cache */
        if (H5AC_get_entry_status(f, fspace->sect_addr, &sinfo_status) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL,
                        "unable to check metadata cache status for free space section info");

        /* If the free space section info is in the cache, expunge it now */
        if (sinfo_status & H5AC_ES__IN_CACHE) {
            /* Sanity checks on direct block */
            assert(!(sinfo_status & H5AC_ES__IS_PINNED));
            assert(!(sinfo_status & H5AC_ES__IS_PROTECTED));

#ifdef H5FS_DEBUG
            fprintf(stderr, "%s: Expunging free space section info from cache\n", __func__);
#endif /* H5FS_DEBUG */
            /* Evict the free space section info from the metadata cache */
            /* (Free file space) */
            {
                unsigned cache_flags = H5AC__NO_FLAGS_SET;

                /* if the indirect block is in real file space, tell
                 * the cache to free its file space.
                 */
                if (!H5F_IS_TMP_ADDR(f, fspace->sect_addr))
                    cache_flags |= H5AC__FREE_FILE_SPACE_FLAG;

                if (H5AC_expunge_entry(f, H5AC_FSPACE_SINFO, fspace->sect_addr, cache_flags) < 0)
                    HGOTO_ERROR(H5E_HEAP, H5E_CANTREMOVE, FAIL,
                                "unable to remove free space section info from cache");
            } /* end block */

#ifdef H5FS_DEBUG
            fprintf(stderr, "%s: Done expunging free space section info from cache\n", __func__);
#endif    /* H5FS_DEBUG */
        } /* end if */
        else {
#ifdef H5FS_DEBUG
            fprintf(stderr, "%s: Deleting free space section info from file\n", __func__);
#endif /* H5FS_DEBUG */
            /* Release the space in the file */
            if (!H5F_IS_TMP_ADDR(f, fspace->sect_addr))
                if (H5MF_xfree(f, H5FD_MEM_FSPACE_SINFO, fspace->sect_addr, fspace->alloc_sect_size) < 0)
                    HGOTO_ERROR(H5E_FSPACE, H5E_CANTFREE, FAIL, "unable to release free space sections");
        } /* end else */
    }     /* end if */

done:
    if (fspace && H5AC_unprotect(f, H5AC_FSPACE_HDR, fs_addr, fspace,
                                 H5AC__DELETED_FLAG | H5AC__FREE_FILE_SPACE_FLAG) < 0)
        HDONE_ERROR(H5E_FSPACE, H5E_CANTUNPROTECT, FAIL, "unable to release free space header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS_delete() */

/*-------------------------------------------------------------------------
 * Function:    H5FS_close
 *
 * Purpose:     Destroy & deallocate free list structure, serializing sections
 *              in the bins
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FS_close(H5F_t *f, H5FS_t *fspace)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments. */
    assert(f);
    assert(fspace);
#ifdef H5FS_DEBUG
    fprintf(stderr, "%s: Entering, fspace = %p, fspace->addr = %" PRIuHADDR ", fspace->sinfo = %p\n",
            __func__, (void *)fspace, fspace->addr, (void *)fspace->sinfo);
#endif /* H5FS_DEBUG */

    /* Check if section info is valid */
    /* (i.e. the header "owns" the section info and it's not in the cache) */
    if (fspace->sinfo) {
#ifdef H5FS_DEBUG
        fprintf(stderr,
                "%s: fspace->tot_sect_count = %" PRIuHSIZE ", fspace->serial_sect_count = %" PRIuHSIZE
                ", fspace->sect_addr = %" PRIuHADDR ", fspace->rc = %u\n",
                __func__, fspace->tot_sect_count, fspace->serial_sect_count, fspace->sect_addr, fspace->rc);
        fprintf(stderr, "%s: fspace->alloc_sect_size = %" PRIuHSIZE ", fspace->sect_size = %" PRIuHSIZE "\n",
                __func__, fspace->alloc_sect_size, fspace->sect_size);
#endif /* H5FS_DEBUG */
        /* If there are sections to serialize, update them */
        /* (if the free space manager is persistent) */
        if (fspace->serial_sect_count > 0 && H5_addr_defined(fspace->addr)) {
#ifdef H5FS_DEBUG
            fprintf(stderr, "%s: Real sections to store in file\n", __func__);
#endif /* H5FS_DEBUG */
            if (fspace->sinfo->dirty) {
                /* Check if the section info is "floating" */
                if (!H5_addr_defined(fspace->sect_addr)) {
                    /* Sanity check */
                    assert(fspace->sect_size > 0);

                    /* Allocate space for the section info in file */
                    if (H5F_USE_TMP_SPACE(f)) {
                        if (HADDR_UNDEF == (fspace->sect_addr = H5MF_alloc_tmp(f, fspace->sect_size)))
                            HGOTO_ERROR(H5E_FSPACE, H5E_NOSPACE, FAIL,
                                        "file allocation failed for free space sections");
                    } /* end if */
                    else {
                        if (HADDR_UNDEF ==
                            (fspace->sect_addr = H5MF_alloc(f, H5FD_MEM_FSPACE_SINFO, fspace->sect_size)))
                            HGOTO_ERROR(H5E_FSPACE, H5E_NOSPACE, FAIL,
                                        "file allocation failed for free space sections");
                    } /* end if */
                    fspace->alloc_sect_size = (size_t)fspace->sect_size;

                    /* Mark free space header as dirty */
                    if (H5AC_mark_entry_dirty(fspace) < 0)
                        HGOTO_ERROR(H5E_FSPACE, H5E_CANTMARKDIRTY, FAIL,
                                    "unable to mark free space header as dirty");
                } /* end if */
            }     /* end if */
            else
                /* Sanity check that section info has address */
                assert(H5_addr_defined(fspace->sect_addr));

            /* Cache the free space section info */
            if (H5AC_insert_entry(f, H5AC_FSPACE_SINFO, fspace->sect_addr, fspace->sinfo,
                                  H5AC__NO_FLAGS_SET) < 0)
                HGOTO_ERROR(H5E_FSPACE, H5E_CANTINIT, FAIL, "can't add free space sections to cache");
        } /* end if */
        else {
#ifdef H5FS_DEBUG
            fprintf(stderr, "%s: NOT storing section info in file\n", __func__);
#endif /* H5FS_DEBUG */
            /* Check if space for the section info is allocated */
            if (H5_addr_defined(fspace->sect_addr)) {
                /* Sanity check */
                /* (section info should only be in the file if the header is */
                assert(H5_addr_defined(fspace->addr));

#ifdef H5FS_DEBUG
                fprintf(stderr, "%s: Section info allocated though\n", __func__);
#endif /* H5FS_DEBUG */
                /* Check if the section info is for the free space in the file */
                /* (NOTE: This is the "bootstrapping" special case for the
                 *      free space manager, to avoid freeing the space for the
                 *      section info and re-creating it as a section in the
                 *      manager. -QAK)
                 */
                if (fspace->client == H5FS_CLIENT_FILE_ID) {
                    htri_t status; /* "can absorb" status for section into */

#ifdef H5FS_DEBUG
                    fprintf(stderr, "%s: Section info is for file free space\n", __func__);
#endif /* H5FS_DEBUG */
                    /* Try to shrink the file or absorb the section info into a block aggregator */
                    if (H5F_IS_TMP_ADDR(f, fspace->sect_addr)) {
#ifdef H5FS_DEBUG
                        fprintf(stderr, "%s: Section info in temp. address space went 'go away'\n", __func__);
#endif /* H5FS_DEBUG */
                        /* Reset section info in header */
                        fspace->sect_addr       = HADDR_UNDEF;
                        fspace->alloc_sect_size = 0;

                        /* Mark free space header as dirty */
                        if (H5AC_mark_entry_dirty(fspace) < 0)
                            HGOTO_ERROR(H5E_FSPACE, H5E_CANTMARKDIRTY, FAIL,
                                        "unable to mark free space header as dirty");
                    } /* end if */
                    else {
                        if ((status = H5MF_try_shrink(f, H5FD_MEM_FSPACE_SINFO, fspace->sect_addr,
                                                      fspace->alloc_sect_size)) < 0)
                            HGOTO_ERROR(H5E_FSPACE, H5E_CANTMERGE, FAIL,
                                        "can't check for absorbing section info");
                        else if (status == false) {
                            /* Section info can't "go away", but it's free.  Allow
                             *      header to record it
                             */
#ifdef H5FS_DEBUG
                            fprintf(stderr, "%s: Section info can't 'go away', header will own it\n",
                                    __func__);
#endif                    /* H5FS_DEBUG */
                        } /* end if */
                        else {
#ifdef H5FS_DEBUG
                            fprintf(stderr, "%s: Section info went 'go away'\n", __func__);
#endif /* H5FS_DEBUG */
                            /* Reset section info in header */
                            fspace->sect_addr       = HADDR_UNDEF;
                            fspace->alloc_sect_size = 0;

                            /* Mark free space header as dirty */
                            if (H5AC_mark_entry_dirty(fspace) < 0)
                                HGOTO_ERROR(H5E_FSPACE, H5E_CANTMARKDIRTY, FAIL,
                                            "unable to mark free space header as dirty");
                        } /* end else */
                    }     /* end else */
                }         /* end if */
                else {
                    haddr_t old_sect_addr = fspace->sect_addr; /* Previous location of section info in file */
                    hsize_t old_alloc_sect_size =
                        fspace->alloc_sect_size; /* Previous size of section info in file */

#ifdef H5FS_DEBUG
                    fprintf(stderr, "%s: Section info is NOT for file free space\n", __func__);
#endif /* H5FS_DEBUG */
                    /* Reset section info in header */
                    fspace->sect_addr       = HADDR_UNDEF;
                    fspace->alloc_sect_size = 0;

                    /* Mark free space header as dirty */
                    if (H5AC_mark_entry_dirty(fspace) < 0)
                        HGOTO_ERROR(H5E_FSPACE, H5E_CANTMARKDIRTY, FAIL,
                                    "unable to mark free space header as dirty");

                    /* Free previous serialized sections disk space */
                    if (!H5F_IS_TMP_ADDR(f, old_sect_addr)) {
                        if (H5MF_xfree(f, H5FD_MEM_FSPACE_SINFO, old_sect_addr, old_alloc_sect_size) < 0)
                            HGOTO_ERROR(H5E_FSPACE, H5E_CANTFREE, FAIL, "unable to free free space sections");
                    } /* end if */
                }     /* end else */
            }         /* end if */

            /* Destroy section info */
            if (H5FS__sinfo_dest(fspace->sinfo) < 0)
                HGOTO_ERROR(H5E_FSPACE, H5E_CANTCLOSEOBJ, FAIL, "unable to destroy free space section info");
        } /* end else */

        /* Reset the header's pointer to the section info */
        fspace->sinfo = NULL;
    } /* end if */
    else {
        /* Just sanity checks... */
        if (fspace->serial_sect_count > 0)
            /* Sanity check that section info has address */
            assert(H5_addr_defined(fspace->sect_addr));
    } /* end else */

    /* Decrement the reference count on the free space manager header */
    if (H5FS__decr(fspace) < 0)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTDEC, FAIL, "unable to decrement ref. count on free space header");

done:
#ifdef H5FS_DEBUG
    fprintf(stderr, "%s: Leaving, ret_value = %d, fspace->rc = %u\n", __func__, ret_value, fspace->rc);
#endif /* H5FS_DEBUG */
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS_close() */

/*-------------------------------------------------------------------------
 * Function:	H5FS__new
 *
 * Purpose:     Create new free space manager structure
 *
 * Return:      Success:    non-NULL, pointer to new free space manager struct
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
H5FS_t *
H5FS__new(const H5F_t *f, uint16_t nclasses, const H5FS_section_class_t *classes[], void *cls_init_udata)
{
    H5FS_t *fspace = NULL;    /* Free space manager */
    size_t  u;                /* Local index variable */
    H5FS_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(nclasses == 0 || (nclasses > 0 && classes));

    /*
     * Allocate free space structure
     */
    if (NULL == (fspace = H5FL_CALLOC(H5FS_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed for free space free list");

    /* Set immutable free list parameters */
    H5_CHECKED_ASSIGN(fspace->nclasses, uint16_t, nclasses, size_t);
    if (nclasses > 0) {
        if (NULL == (fspace->sect_cls = H5FL_SEQ_MALLOC(H5FS_section_class_t, nclasses)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL,
                        "memory allocation failed for free space section class array");

        /* Initialize the section classes for this free space list */
        for (u = 0; u < nclasses; u++) {
            /* Make certain that section class type can be used as an array index into this array */
            assert(u == classes[u]->type);

            /* Copy the class information into the free space manager */
            H5MM_memcpy(&fspace->sect_cls[u], classes[u], sizeof(H5FS_section_class_t));

            /* Call the class initialization routine, if there is one */
            if (fspace->sect_cls[u].init_cls)
                if ((fspace->sect_cls[u].init_cls)(&fspace->sect_cls[u], cls_init_udata) < 0)
                    HGOTO_ERROR(H5E_RESOURCE, H5E_CANTINIT, NULL, "unable to initialize section class");

            /* Determine maximum class-specific serialization size for each section */
            if (fspace->sect_cls[u].serial_size > fspace->max_cls_serial_size)
                fspace->max_cls_serial_size = fspace->sect_cls[u].serial_size;
        } /* end for */
    }     /* end if */

    /* Initialize non-zero information for new free space manager */
    fspace->addr      = HADDR_UNDEF;
    fspace->hdr_size  = (size_t)H5FS_HEADER_SIZE(f);
    fspace->sect_addr = HADDR_UNDEF;

    /* Set return value */
    ret_value = fspace;

done:
    if (!ret_value)
        if (fspace) {
            /* Should probably call the class 'term' callback for all classes
             *  that have had their 'init' callback called... -QAK
             */
            if (fspace->sect_cls)
                fspace->sect_cls =
                    (H5FS_section_class_t *)H5FL_SEQ_FREE(H5FS_section_class_t, fspace->sect_cls);
            fspace = H5FL_FREE(H5FS_t, fspace);
        } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS__new() */

/*-------------------------------------------------------------------------
 * Function:    H5FS_size
 *
 * Purpose:     Collect meta storage info used by the free space manager
 *
 * Return:      SUCCEED (Can't fail)
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FS_size(const H5FS_t *fspace, hsize_t *meta_size)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /*
     * Check arguments.
     */
    assert(fspace);
    assert(meta_size);

    /* Get the free space size info */
    *meta_size += fspace->hdr_size + (fspace->sinfo ? fspace->sect_size : fspace->alloc_sect_size);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FS_size() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__incr
 *
 * Purpose:     Increment reference count on free space header
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FS__incr(H5FS_t *fspace)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE
#ifdef H5FS_DEBUG
    fprintf(stderr, "%s: Entering, fpace->addr = %" PRIuHADDR ", fspace->rc = %u\n", __func__, fspace->addr,
            fspace->rc);
#endif /* H5FS_DEBUG */

    /*
     * Check arguments.
     */
    assert(fspace);

    /* Check if we should pin the header in the cache */
    if (fspace->rc == 0 && H5_addr_defined(fspace->addr))
        if (H5AC_pin_protected_entry(fspace) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTPIN, FAIL, "unable to pin free space header");

    /* Increment reference count on header */
    fspace->rc++;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FS__incr() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__decr
 *
 * Purpose:     Decrement reference count on free space header
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FS__decr(H5FS_t *fspace)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE
#ifdef H5FS_DEBUG
    fprintf(stderr, "%s: Entering, fpace->addr = %" PRIuHADDR ", fspace->rc = %u\n", __func__, fspace->addr,
            fspace->rc);
#endif /* H5FS_DEBUG */

    /*
     * Check arguments.
     */
    assert(fspace);

    /* Decrement reference count on header */
    fspace->rc--;

    /* Check if we should unpin the header in the cache */
    if (fspace->rc == 0) {
        if (H5_addr_defined(fspace->addr)) {
            if (H5AC_unpin_entry(fspace) < 0)
                HGOTO_ERROR(H5E_FSPACE, H5E_CANTUNPIN, FAIL, "unable to unpin free space header");
        } /* end if */
        else {
            if (H5FS__hdr_dest(fspace) < 0)
                HGOTO_ERROR(H5E_FSPACE, H5E_CANTCLOSEOBJ, FAIL, "unable to destroy free space header");
        } /* end else */
    }     /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FS__decr() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__dirty
 *
 * Purpose:     Mark free space header as dirty
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FS__dirty(H5FS_t *fspace)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(fspace);

    /* Check if the free space manager is persistent */
    if (H5_addr_defined(fspace->addr))
        /* Mark header as dirty in cache */
        if (H5AC_mark_entry_dirty(fspace) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTMARKDIRTY, FAIL, "unable to mark free space header as dirty");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FS__dirty() */

/*-------------------------------------------------------------------------
 * Function:    H5FS_alloc_hdr()
 *
 * Purpose:     Allocate space for the free-space manager header
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FS_alloc_hdr(H5F_t *f, H5FS_t *fspace, haddr_t *fs_addr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments. */
    assert(f);
    assert(fspace);

    if (!H5_addr_defined(fspace->addr)) {
        /* Allocate space for the free space header */
        if (HADDR_UNDEF == (fspace->addr = H5MF_alloc(f, H5FD_MEM_FSPACE_HDR, (hsize_t)H5FS_HEADER_SIZE(f))))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "file allocation failed for free space header");

        /* Cache the new free space header (pinned) */
        if (H5AC_insert_entry(f, H5AC_FSPACE_HDR, fspace->addr, fspace, H5AC__PIN_ENTRY_FLAG) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTINIT, FAIL, "can't add free space header to cache");
    } /* end if */

    if (fs_addr)
        *fs_addr = fspace->addr;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS_alloc_hdr() */

/*-------------------------------------------------------------------------
 * Function:    H5FS_alloc_sect()
 *
 * Purpose:     Allocate space for the free-space manager section info header
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FS_alloc_sect(H5F_t *f, H5FS_t *fspace)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments. */
    assert(f);
    assert(fspace);

    if (!H5_addr_defined(fspace->sect_addr) && fspace->sinfo && fspace->serial_sect_count > 0) {
        if (HADDR_UNDEF == (fspace->sect_addr = H5MF_alloc(f, H5FD_MEM_FSPACE_SINFO, fspace->sect_size)))
            HGOTO_ERROR(H5E_FSPACE, H5E_NOSPACE, FAIL, "file allocation failed for section info");
        fspace->alloc_sect_size = fspace->sect_size;

        /* Mark free-space header as dirty */
        if (H5FS__dirty(fspace) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTMARKDIRTY, FAIL, "unable to mark free space header as dirty");

        /* Cache the free-space section info */
        if (H5AC_insert_entry(f, H5AC_FSPACE_SINFO, fspace->sect_addr, fspace->sinfo, H5AC__NO_FLAGS_SET) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTINIT, FAIL, "can't add free space sections to cache");

        /* Since space has been allocated for the section info and the sinfo
         * has been inserted into the cache, relinquish ownership (i.e. float)
         * the section info.
         */
        fspace->sinfo = NULL;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS_alloc_sect() */

/*-------------------------------------------------------------------------
 * Function:    H5FS_free()
 *
 * Purpose:     Free space for free-space manager header and section info header
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FS_free(H5F_t *f, H5FS_t *fspace, bool free_file_space)
{
    haddr_t  saved_addr;          /* Previous address of item */
    unsigned cache_flags;         /* Flags for unprotecting cache entries */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments. */
    assert(f);
    assert(fspace);

    cache_flags = H5AC__DELETED_FLAG | H5AC__TAKE_OWNERSHIP_FLAG;

    /* Free space for section info */
    if (H5_addr_defined(fspace->sect_addr)) {
        hsize_t  saved_size;       /* Size of previous section info */
        unsigned sinfo_status = 0; /* Section info cache status */

        /* Check whether free-space manager section info is in cache or not */
        if (H5AC_get_entry_status(f, fspace->sect_addr, &sinfo_status) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTGET, FAIL,
                        "unable to check metadata cache status for free-space section info");

        /* Load free-space manager section info */
        if (sinfo_status & H5AC_ES__IN_CACHE || !fspace->sinfo) {
            H5FS_sinfo_cache_ud_t cache_udata; /* User-data for cache callback */

            /* Protect the free space sections */
            cache_udata.f      = f;
            cache_udata.fspace = fspace;
            if (NULL == (fspace->sinfo = (H5FS_sinfo_t *)H5AC_protect(f, H5AC_FSPACE_SINFO, fspace->sect_addr,
                                                                      &cache_udata, H5AC__READ_ONLY_FLAG)))
                HGOTO_ERROR(H5E_FSPACE, H5E_CANTPROTECT, FAIL, "unable to protect free space section info");

            /* Unload and release ownership of the free-space manager section info */
            if (H5AC_unprotect(f, H5AC_FSPACE_SINFO, fspace->sect_addr, fspace->sinfo, cache_flags) < 0)
                HGOTO_ERROR(H5E_FSPACE, H5E_CANTUNPROTECT, FAIL, "unable to release free space section info");
        } /* end if */

        saved_addr = fspace->sect_addr;
        saved_size = fspace->alloc_sect_size;

        fspace->sect_addr       = HADDR_UNDEF;
        fspace->alloc_sect_size = 0;

        /* Free space for the free-space manager section info */
        if (!H5F_IS_TMP_ADDR(f, saved_addr)) {
            if (free_file_space && H5MF_xfree(f, H5FD_MEM_FSPACE_SINFO, saved_addr, saved_size) < 0)
                HGOTO_ERROR(H5E_FSPACE, H5E_CANTFREE, FAIL, "unable to release free space sections");
        } /* end if */

        /* Mark free-space manager header as dirty */
        if (H5FS__dirty(fspace) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTMARKDIRTY, FAIL, "unable to mark free space header as dirty");
    } /* end if */

    /* Free space for header */
    if (H5_addr_defined(fspace->addr)) {
        unsigned hdr_status = 0; /* Header entry status */

        /* Check whether free-space manager header is in cache or not */
        if (H5AC_get_entry_status(f, fspace->addr, &hdr_status) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTGET, FAIL,
                        "unable to check metadata cache status for free-space section info");

        if (hdr_status & H5AC_ES__IN_CACHE) {
            H5FS_hdr_cache_ud_t cache_udata; /* User-data for metadata cache callback */

            /* Protect the free-space manager header */
            /* (no class information necessary since it's in the cache) */
            cache_udata.f              = f;
            cache_udata.nclasses       = 0;
            cache_udata.classes        = NULL;
            cache_udata.cls_init_udata = NULL;
            if (NULL == (fspace = (H5FS_t *)H5AC_protect(f, H5AC_FSPACE_HDR, fspace->addr, &cache_udata,
                                                         H5AC__READ_ONLY_FLAG)))
                HGOTO_ERROR(H5E_FSPACE, H5E_CANTPROTECT, FAIL, "unable to protect free space section info");

            /* Unpin the free-space manager header */
            if (H5AC_unpin_entry(fspace) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTUNPIN, FAIL, "unable to unpin fractal heap header");

            /* Unload and release ownership of the free-space header */
            if (H5AC_unprotect(f, H5AC_FSPACE_HDR, fspace->addr, fspace, cache_flags) < 0)
                HGOTO_ERROR(H5E_FSPACE, H5E_CANTUNPROTECT, FAIL, "unable to release free space section info");
        } /* end if */

        saved_addr   = fspace->addr;
        fspace->addr = HADDR_UNDEF;

        /* Free space for the free-space manager header */
        if (free_file_space &&
            H5MF_xfree(f, H5FD_MEM_FSPACE_HDR, saved_addr, (hsize_t)H5FS_HEADER_SIZE(f)) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTFREE, FAIL, "unable to free free space header");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS_free() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__hdr_dest
 *
 * Purpose:     Destroys a free space header in memory.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FS__hdr_dest(H5FS_t *fspace)
{
    unsigned u;                   /* Local index variable */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(fspace);

    /* Terminate the section classes for this free space list */
    for (u = 0; u < fspace->nclasses; u++) {
        /* Call the class termination routine, if there is one */
        if (fspace->sect_cls[u].term_cls)
            if ((fspace->sect_cls[u].term_cls)(&fspace->sect_cls[u]) < 0)
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTRELEASE, FAIL, "unable to finalize section class");
    } /* end for */

    /* Release the memory for the free space section classes */
    if (fspace->sect_cls)
        fspace->sect_cls = (H5FS_section_class_t *)H5FL_SEQ_FREE(H5FS_section_class_t, fspace->sect_cls);

    /* Free free space info */
    fspace = H5FL_FREE(H5FS_t, fspace);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FS__hdr_dest() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__sinfo_free_sect_cb
 *
 * Purpose:     Free a size-tracking node for a bin
 *
 * Return:      SUCCEED (Can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__sinfo_free_sect_cb(void *_sect, void H5_ATTR_UNUSED *key, void *op_data)
{
    H5FS_section_info_t *sect  = (H5FS_section_info_t *)_sect;  /* Section to free */
    const H5FS_sinfo_t  *sinfo = (const H5FS_sinfo_t *)op_data; /* Free space manager for section */

    FUNC_ENTER_PACKAGE_NOERR

    assert(sect);
    assert(sinfo);

    /* Call the section's class 'free' method on the section */
    (*sinfo->fspace->sect_cls[sect->type].free)(sect);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5FS__sinfo_free_sect_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__sinfo_free_node_cb
 *
 * Purpose:     Free a size-tracking node for a bin
 *
 * Return:      SUCCEED (Can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__sinfo_free_node_cb(void *item, void H5_ATTR_UNUSED *key, void *op_data)
{
    H5FS_node_t *fspace_node = (H5FS_node_t *)item; /* Temporary pointer to free space list node */

    FUNC_ENTER_PACKAGE_NOERR

    assert(fspace_node);
    assert(op_data);

    /* Release the skip list for sections of this size */
    H5SL_destroy(fspace_node->sect_list, H5FS__sinfo_free_sect_cb, op_data);

    /* Release free space list node */
    fspace_node = H5FL_FREE(H5FS_node_t, fspace_node);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5FS__sinfo_free_node_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__sinfo_dest
 *
 * Purpose:     Destroys a free space section info in memory.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FS__sinfo_dest(H5FS_sinfo_t *sinfo)
{
    unsigned u;                   /* Local index variable */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(sinfo);
    assert(sinfo->fspace);
    assert(sinfo->bins);

    /* Clear out lists of nodes */
    for (u = 0; u < sinfo->nbins; u++)
        if (sinfo->bins[u].bin_list) {
            H5SL_destroy(sinfo->bins[u].bin_list, H5FS__sinfo_free_node_cb, sinfo);
            sinfo->bins[u].bin_list = NULL;
        } /* end if */

    /* Release bins for skip lists */
    sinfo->bins = H5FL_SEQ_FREE(H5FS_bin_t, sinfo->bins);

    /* Release skip list for merging sections */
    if (sinfo->merge_list)
        if (H5SL_close(sinfo->merge_list) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTCLOSEOBJ, FAIL, "can't destroy section merging skip list");

    /* Decrement the reference count on free space header */
    /* (make certain this is last action with section info, to allow for header
     *  disappearing immediately)
     */
    sinfo->fspace->sinfo = NULL;
    if (H5FS__decr(sinfo->fspace) < 0)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTDEC, FAIL, "unable to decrement ref. count on free space header");
    sinfo->fspace = NULL;

    /* Release free space section info */
    sinfo = H5FL_FREE(H5FS_sinfo_t, sinfo);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FS__sinfo_dest() */

herr_t
H5FS_get_sect_count(const H5FS_t *frsp, hsize_t *tot_sect_count)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check arguments. */
    assert(frsp);
    assert(tot_sect_count);

    /* Report statistics for free space */
    *tot_sect_count = frsp->serial_sect_count;

    FUNC_LEAVE_NOAPI(ret_value)
}

#ifdef H5FS_DEBUG_ASSERT

/*-------------------------------------------------------------------------
 * Function:    H5FS__assert
 *
 * Purpose:     Verify that the free space manager is mostly sane
 *
 * Return:      void
 *
 *-------------------------------------------------------------------------
 */
void
H5FS__assert(const H5FS_t *fspace)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Checks for section info, if it's available */
    if (fspace->sinfo) {
        /* Sanity check sections */
        H5FS__sect_assert(fspace);

        /* General assumptions about the section size counts */
        assert(fspace->sinfo->tot_size_count >= fspace->sinfo->serial_size_count);
        assert(fspace->sinfo->tot_size_count >= fspace->sinfo->ghost_size_count);
    } /* end if */

    /* General assumptions about the section counts */
    assert(fspace->tot_sect_count >= fspace->serial_sect_count);
    assert(fspace->tot_sect_count >= fspace->ghost_sect_count);
    assert(fspace->tot_sect_count == (fspace->serial_sect_count + fspace->ghost_sect_count));

    FUNC_LEAVE_NOAPI_VOID
} /* end H5FS__assert() */
#endif /* H5FS_DEBUG_ASSERT */
