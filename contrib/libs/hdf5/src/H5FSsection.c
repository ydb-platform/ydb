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
 */

/****************/
/* Module Setup */
/****************/

#define H5F_FRIEND /*suppress error about including H5Fpkg   */

#include "H5FSmodule.h" /* This source code file is part of the H5FS module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Fpkg.h"      /* File access                          */
#include "H5FSpkg.h"     /* File free space			*/
#include "H5MFprivate.h" /* File memory management		*/
#include "H5VMprivate.h" /* Vectors and arrays 			*/

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/* User data for skip list iterator callback for iterating over section size nodes */
typedef struct {
    H5FS_t         *fspace;  /* Free space manager info */
    H5FS_operator_t op;      /* Operator for the iteration */
    void           *op_data; /* Information to pass to the operator */
} H5FS_iter_ud_t;

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/
static herr_t H5FS__sect_increase(H5FS_t *fspace, const H5FS_section_class_t *cls, unsigned flags);
static herr_t H5FS__sect_decrease(H5FS_t *fspace, const H5FS_section_class_t *cls);
static herr_t H5FS__size_node_decr(H5FS_sinfo_t *sinfo, unsigned bin, H5FS_node_t *fspace_node,
                                   const H5FS_section_class_t *cls);
static herr_t H5FS__sect_unlink_size(H5FS_sinfo_t *sinfo, const H5FS_section_class_t *cls,
                                     H5FS_section_info_t *sect);
static herr_t H5FS__sect_unlink_rest(H5FS_t *fspace, const H5FS_section_class_t *cls,
                                     H5FS_section_info_t *sect);
static herr_t H5FS__sect_remove_real(H5FS_t *fspace, H5FS_section_info_t *sect);
static herr_t H5FS__sect_link_size(H5FS_sinfo_t *sinfo, const H5FS_section_class_t *cls,
                                   H5FS_section_info_t *sect);
static herr_t H5FS__sect_link_rest(H5FS_t *fspace, const H5FS_section_class_t *cls, H5FS_section_info_t *sect,
                                   unsigned flags);
static herr_t H5FS__sect_link(H5FS_t *fspace, H5FS_section_info_t *sect, unsigned flags);
static herr_t H5FS__sect_merge(H5FS_t *fspace, H5FS_section_info_t **sect, void *op_data);
static htri_t H5FS__sect_find_node(H5FS_t *fspace, hsize_t request, H5FS_section_info_t **node);
static herr_t H5FS__sect_serialize_size(H5FS_t *fspace);

/*********************/
/* Package Variables */
/*********************/

/* Declare a free list to manage the H5FS_node_t struct */
H5FL_DEFINE(H5FS_node_t);

/* Declare a free list to manage the H5FS_bin_t sequence information */
H5FL_SEQ_DEFINE(H5FS_bin_t);

/* Declare a free list to manage the H5FS_sinfo_t struct */
H5FL_DEFINE(H5FS_sinfo_t);

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:    H5FS__sinfo_new
 *
 * Purpose:     Create new section info structure
 *
 * Return:      Success:    non-NULL, pointer to new section info struct
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
H5FS_sinfo_t *
H5FS__sinfo_new(H5F_t *f, H5FS_t *fspace)
{
    H5FS_sinfo_t *sinfo     = NULL; /* Section information struct created */
    H5FS_sinfo_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(f);
    assert(fspace);
#ifdef H5FS_SINFO_DEBUG
    fprintf(stderr, "%s: fspace->addr = %" PRIuHADDR "\n", __func__, fspace->addr);
#endif /* H5FS_SINFO_DEBUG */

    /* Allocate the free space header */
    if (NULL == (sinfo = H5FL_CALLOC(H5FS_sinfo_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Set non-zero values */
    sinfo->nbins            = H5VM_log2_gen(fspace->max_sect_size);
    sinfo->sect_prefix_size = H5FS_SINFO_PREFIX_SIZE(f);
    sinfo->sect_off_size    = (fspace->max_sect_addr + 7) / 8;
    sinfo->sect_len_size    = H5VM_limit_enc_size((uint64_t)fspace->max_sect_size);
#ifdef H5FS_SINFO_DEBUG
    fprintf(stderr, "%s: fspace->max_sect_size = %" PRIuHSIZE "\n", __func__, fspace->max_sect_size);
    fprintf(stderr, "%s: fspace->max_sect_addr = %u\n", __func__, fspace->max_sect_addr);
    fprintf(stderr, "%s: sinfo->nbins = %u\n", __func__, sinfo->nbins);
    fprintf(stderr, "%s: sinfo->sect_off_size = %u, sinfo->sect_len_size = %u\n", __func__,
            sinfo->sect_off_size, sinfo->sect_len_size);
#endif /* H5FS_SINFO_DEBUG */

    /* Allocate space for the section size bins */
    if (NULL == (sinfo->bins = H5FL_SEQ_CALLOC(H5FS_bin_t, (size_t)sinfo->nbins)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL,
                    "memory allocation failed for free space section bin array");

    /* Increment the reference count on the free space manager header */
    if (H5FS__incr(fspace) < 0)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTINC, NULL, "unable to increment ref. count on free space header");
    sinfo->fspace = fspace;

    /* Link free space manager to section info */
    /* (for deserializing sections) */
    assert(fspace->sinfo == NULL);
    fspace->sinfo = sinfo;

    /* Set return value */
    ret_value = sinfo;

done:
    if (ret_value == NULL && sinfo) {
        /* Release bins for skip lists */
        if (sinfo->bins)
            sinfo->bins = H5FL_SEQ_FREE(H5FS_bin_t, sinfo->bins);

        /* Release free space section info */
        sinfo = H5FL_FREE(H5FS_sinfo_t, sinfo);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS__sinfo_new() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__sinfo_lock
 *
 * Purpose:     Make certain the section info for the free space manager is
 *              in memory.
 *
 *              Either uses existing section info owned by the free space
 *              header, loads section info from disk, or creates new section
 *              info
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__sinfo_lock(H5F_t *f, H5FS_t *fspace, unsigned accmode)
{
    H5FS_sinfo_cache_ud_t cache_udata;         /* User-data for cache callback */
    herr_t                ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

#ifdef H5FS_SINFO_DEBUG
    fprintf(stderr,
            "%s: Called, fspace->addr = %" PRIuHADDR ", fspace->sinfo = %p, fspace->sect_addr = %" PRIuHADDR
            "\n",
            __func__, fspace->addr, (void *)fspace->sinfo, fspace->sect_addr);
    fprintf(stderr, "%s: fspace->alloc_sect_size = %" PRIuHSIZE ", fspace->sect_size = %" PRIuHSIZE "\n",
            __func__, fspace->alloc_sect_size, fspace->sect_size);
#endif /* H5FS_SINFO_DEBUG */

    /* Check arguments. */
    assert(f);
    assert(fspace);

    /* only H5AC__READ_ONLY_FLAG may appear in accmode */
    assert((accmode & (unsigned)(~H5AC__READ_ONLY_FLAG)) == 0);

    /* If the free space header doesn't already "own" the section info, load
     *  section info or create it
     */
    if (fspace->sinfo) {
        /* Check if the section info was protected & we want a different access mode */

        /* only H5AC__READ_ONLY_FLAG may appear in fspace->sinfo_accmode */
        assert(((fspace->sinfo_accmode) & (unsigned)(~H5AC__READ_ONLY_FLAG)) == 0);

        if (fspace->sinfo_protected && accmode != fspace->sinfo_accmode) {
            /* Check if we need to switch from read-only access to read-write */
            if (0 == (accmode & (unsigned)(~H5AC__READ_ONLY_FLAG))) {
                /* Unprotect the read-only section info */
                if (H5AC_unprotect(f, H5AC_FSPACE_SINFO, fspace->sect_addr, fspace->sinfo,
                                   H5AC__NO_FLAGS_SET) < 0)
                    HGOTO_ERROR(H5E_FSPACE, H5E_CANTUNPROTECT, FAIL,
                                "unable to release free space section info");

                /* Re-protect the section info with read-write access */
                cache_udata.f      = f;
                cache_udata.fspace = fspace;
                if (NULL == (fspace->sinfo = (H5FS_sinfo_t *)H5AC_protect(
                                 f, H5AC_FSPACE_SINFO, fspace->sect_addr, &cache_udata, H5AC__NO_FLAGS_SET)))
                    HGOTO_ERROR(H5E_FSPACE, H5E_CANTPROTECT, FAIL, "unable to load free space sections");

                /* Switch the access mode we have */
                fspace->sinfo_accmode = H5AC__NO_FLAGS_SET;
            } /* end if */
        }     /* end if */
    }         /* end if */
    else {
        /* If the section address is defined, load it from the file */
        if (H5_addr_defined(fspace->sect_addr)) {
            /* Sanity check */
            assert(fspace->sinfo_protected == false);
            assert(H5_addr_defined(fspace->addr));

#ifdef H5FS_SINFO_DEBUG
            fprintf(stderr, "%s: Reading in existing sections, fspace->sect_addr = %" PRIuHADDR "\n",
                    __func__, fspace->sect_addr);
#endif /* H5FS_SINFO_DEBUG */
            /* Protect the free space sections */
            cache_udata.f      = f;
            cache_udata.fspace = fspace;
            if (NULL == (fspace->sinfo = (H5FS_sinfo_t *)H5AC_protect(f, H5AC_FSPACE_SINFO, fspace->sect_addr,
                                                                      &cache_udata, accmode)))
                HGOTO_ERROR(H5E_FSPACE, H5E_CANTPROTECT, FAIL, "unable to load free space sections");

            /* Remember that we protected the section info & the access mode */
            fspace->sinfo_protected = true;
            fspace->sinfo_accmode   = accmode;
        } /* end if */
        else {
#ifdef H5FS_SINFO_DEBUG
            fprintf(stderr, "%s: Creating new section info\n", __func__);
#endif /* H5FS_SINFO_DEBUG */
            /* Sanity check */
            assert(fspace->tot_sect_count == 0);
            assert(fspace->serial_sect_count == 0);
            assert(fspace->ghost_sect_count == 0);

            /* Allocate and initialize free space section info */
            if (NULL == (fspace->sinfo = H5FS__sinfo_new(f, fspace)))
                HGOTO_ERROR(H5E_FSPACE, H5E_CANTCREATE, FAIL, "can't create section info");

            /* Set initial size of section info to 0 */
            fspace->sect_size = fspace->alloc_sect_size = 0;
        } /* end if */
    }     /* end if */
    assert(fspace->rc == 2);

    /* Increment the section info lock count */
    fspace->sinfo_lock_count++;

done:
#ifdef H5FS_SINFO_DEBUG
    fprintf(stderr,
            "%s: Leaving, fspace->addr = %" PRIuHADDR ", fspace->sinfo = %p, fspace->sect_addr = %" PRIuHADDR
            "\n",
            __func__, fspace->addr, (void *)fspace->sinfo, fspace->sect_addr);
    fprintf(stderr, "%s: fspace->alloc_sect_size = %" PRIuHSIZE ", fspace->sect_size = %" PRIuHSIZE "\n",
            __func__, fspace->alloc_sect_size, fspace->sect_size);
#endif /* H5FS_SINFO_DEBUG */
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS__sinfo_lock() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__sinfo_unlock
 *
 * Purpose:     Release the section info, either giving ownership back to
 *              the cache or letting the free space header keep it.
 *
 *              Add the fix in this routine to resolve the potential infinite loop
 *              problem when allocating file space for the meta data of the
 *              self-referential free-space managers at file closing.
 *
 *              On file close or flushing, when the section info is modified
 *              and protected/unprotected, does not allow the section info size
 *              to shrink:
 *              --if the current allocated section info size in fspace->sect_size is
 *                larger than the previous section info size in fpsace->alloc_sect_size,
 *                 release the section info
 *              --Otherwise, set the fspace->sect_size to be the same as
 *                fpsace->alloc_sect_size.  This means fspace->sect_size may be larger
 *                than what is actually needed.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__sinfo_unlock(H5F_t *f, H5FS_t *fspace, bool modified)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE
#ifdef H5FS_SINFO_DEBUG
    fprintf(stderr,
            "%s: Called, modified = %d, fspace->addr = %" PRIuHADDR ", fspace->sect_addr = %" PRIuHADDR "\n",
            __func__, modified, fspace->addr, fspace->sect_addr);
    fprintf(stderr,
            "%s: fspace->sinfo_lock_count = %u, fspace->sinfo_modified = %d, fspace->sinfo_protected = %d\n",
            __func__, fspace->sinfo_lock_count, fspace->sinfo_modified, fspace->sinfo_protected);
    fprintf(stderr, "%s: fspace->alloc_sect_size = %" PRIuHSIZE ", fspace->sect_size = %" PRIuHSIZE "\n",
            __func__, fspace->alloc_sect_size, fspace->sect_size);
#endif /* H5FS_SINFO_DEBUG */

    /* Check arguments. */
    assert(f);
    assert(fspace);
    assert(fspace->rc == 2);
    assert(fspace->sinfo);

    /* Check if we modified any section */
    if (modified) {
        /* Check if the section info was protected with a different access mode */
        if (fspace->sinfo_protected && (0 != ((fspace->sinfo_accmode) & H5AC__READ_ONLY_FLAG)))
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTDIRTY, FAIL, "attempt to modify read-only section info");

        /* If we modified the section info, mark it dirty */
        fspace->sinfo->dirty = true;

        /* Remember that the section info was modified while locked */
        fspace->sinfo_modified = true;

        /* Assume that the modification will affect the statistics in the header
         *  and mark that dirty also
         */
        if (H5FS__dirty(fspace) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTMARKDIRTY, FAIL, "unable to mark free space header as dirty");
    } /* end if */

    /* Decrement the lock count on the section info */
    fspace->sinfo_lock_count--;

    /* Check if section info lock count dropped to zero */
    if (fspace->sinfo_lock_count == 0) {
        bool release_sinfo_space = false; /* Flag to indicate section info space in file should be released */
        bool closing_or_flushing = f->shared->closing; /* Is closing or flushing in progress */

        /* Check whether cache-flush is in progress if closing is not. */
        if (!closing_or_flushing &&
            H5AC_get_cache_flush_in_progress(f->shared->cache, &closing_or_flushing) < 0)
            HGOTO_ERROR(H5E_CACHE, H5E_SYSTEM, FAIL, "Can't get flush_in_progress");

        /* Check if we actually protected the section info */
        if (fspace->sinfo_protected) {
            unsigned cache_flags = H5AC__NO_FLAGS_SET; /* Flags for unprotecting heap */

            /* Sanity check */
            assert(H5_addr_defined(fspace->addr));

            /* Check if we've made new changes to the section info while locked */
            if (fspace->sinfo_modified) {
                /* Note that we've modified the section info */
                cache_flags |= H5AC__DIRTIED_FLAG;

                /* On file close or flushing, does not allow section info to shrink in size */
                if (closing_or_flushing) {
                    if (fspace->sect_size > fspace->alloc_sect_size)
                        cache_flags |= H5AC__DELETED_FLAG | H5AC__TAKE_OWNERSHIP_FLAG;
                    else
                        fspace->sect_size = fspace->alloc_sect_size;

                    /* Check if the section info size in the file has changed */
                }
                else if (fspace->sect_size != fspace->alloc_sect_size)
                    cache_flags |= H5AC__DELETED_FLAG | H5AC__TAKE_OWNERSHIP_FLAG;

            } /* end if */

            /* Sanity check */
            assert(H5_addr_defined(fspace->sect_addr));

            /* Unprotect section info in cache */
            /* (Possibly dirty) */
            /* (Possibly taking ownership from the cache) */
#ifdef H5FS_SINFO_DEBUG
            fprintf(stderr, "%s: Unprotecting section info, cache_flags = %u\n", __func__, cache_flags);
#endif /* H5FS_SINFO_DEBUG */
            if (H5AC_unprotect(f, H5AC_FSPACE_SINFO, fspace->sect_addr, fspace->sinfo, cache_flags) < 0)
                HGOTO_ERROR(H5E_FSPACE, H5E_CANTUNPROTECT, FAIL, "unable to release free space section info");

            /* Reset the protected flag on the section info */
            fspace->sinfo_protected = false;

            /* Check if header is taking ownership of section info */
            if ((cache_flags & H5AC__TAKE_OWNERSHIP_FLAG)) {
#ifdef H5FS_SINFO_DEBUG
                fprintf(stderr, "%s: Taking ownership of section info\n", __func__);
#endif /* H5FS_SINFO_DEBUG */
                /* Set flag to release section info space in file */
                release_sinfo_space = true;
            } /* end if */
            else {
#ifdef H5FS_SINFO_DEBUG
                fprintf(stderr, "%s: Relinquishing section info ownership\n", __func__);
#endif /* H5FS_SINFO_DEBUG */
                /* Free space header relinquished ownership of section info */
                fspace->sinfo = NULL;
            } /* end else */
        }     /* end if */
        else {
            /* Check if the section info was modified */
            if (fspace->sinfo_modified) {
                /* Check if we need to release section info in the file */
                if (H5_addr_defined(fspace->sect_addr)) {
                    /* Set flag to release section info space in file */
                    /* On file close or flushing, only need to release section info with size
                       bigger than previous section */
                    if (closing_or_flushing) {
                        if (fspace->sect_size > fspace->alloc_sect_size)
                            release_sinfo_space = true;
                        else
                            fspace->sect_size = fspace->alloc_sect_size;
                    }
                    else
                        release_sinfo_space = true;
                }
                else
                    assert(fspace->alloc_sect_size == 0);

            } /* end if */
            else {
                /* Sanity checks... */
                if (H5_addr_defined(fspace->sect_addr))
                    assert(fspace->alloc_sect_size == fspace->sect_size);
                else
                    assert(fspace->alloc_sect_size == 0);
            } /* end else */
        }     /* end else */

        /* Reset the "section info modified" flag */
        fspace->sinfo_modified = false;

        /* Check if header needs to release section info in the file */
        if (release_sinfo_space) {
            haddr_t old_sect_addr       = fspace->sect_addr; /* Previous location of section info in file */
            hsize_t old_alloc_sect_size = fspace->alloc_sect_size; /* Previous size of section info in file */

            /* Sanity check */
            assert(H5_addr_defined(fspace->addr));

            /* Reset section info in header */
            fspace->sect_addr       = HADDR_UNDEF;
            fspace->alloc_sect_size = 0;

            /* If we haven't already marked the header dirty, do so now */
            if (!modified)
                if (H5FS__dirty(fspace) < 0)
                    HGOTO_ERROR(H5E_FSPACE, H5E_CANTMARKDIRTY, FAIL,
                                "unable to mark free space header as dirty");

#ifdef H5FS_SINFO_DEBUG
            fprintf(stderr,
                    "%s: Freeing section info on disk, old_sect_addr = %" PRIuHADDR
                    ", old_alloc_sect_size = %" PRIuHSIZE "\n",
                    __func__, old_sect_addr, old_alloc_sect_size);
#endif /* H5FS_SINFO_DEBUG */
            /* Release space for section info in file */
            if (!H5F_IS_TMP_ADDR(f, old_sect_addr))
                if (H5MF_xfree(f, H5FD_MEM_FSPACE_SINFO, old_sect_addr, old_alloc_sect_size) < 0)
                    HGOTO_ERROR(H5E_FSPACE, H5E_CANTFREE, FAIL, "unable to free free space sections");
        } /* end if */
    }     /* end if */

done:
#ifdef H5FS_SINFO_DEBUG
    fprintf(stderr, "%s: Leaving, ret_value = %d\n", __func__, ret_value);
#endif /* H5FS_SINFO_DEBUG */
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS__sinfo_unlock() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__sect_serialize_size
 *
 * Purpose:     Determine serialized size of all sections in free space manager
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__sect_serialize_size(H5FS_t *fspace)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments. */
    assert(fspace);

    /* Compute the size of the buffer required to serialize all the sections */
    if (fspace->serial_sect_count > 0) {
        size_t sect_buf_size; /* Section buffer size */

        /* Serialized sections prefix */
        sect_buf_size = fspace->sinfo->sect_prefix_size;

        /* Count for each differently sized serializable section */
        sect_buf_size +=
            fspace->sinfo->serial_size_count * H5VM_limit_enc_size((uint64_t)fspace->serial_sect_count);

        /* Size for each differently sized serializable section */
        sect_buf_size += fspace->sinfo->serial_size_count * fspace->sinfo->sect_len_size;

        /* Offsets of each section in address space */
        sect_buf_size += fspace->serial_sect_count * fspace->sinfo->sect_off_size;

        /* Class of each section */
        sect_buf_size += fspace->serial_sect_count * 1 /* byte */;

        /* Extra space required to serialize each section */
        sect_buf_size += fspace->sinfo->serial_size;

        /* Update section size in header */
        fspace->sect_size = sect_buf_size;
    } /* end if */
    else
        /* Reset section size in header */
        fspace->sect_size = fspace->sinfo->sect_prefix_size;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5FS__sect_serialize_size() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__sect_increase
 *
 * Purpose:     Increase the size of the serialized free space section info
 *              on disk
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__sect_increase(H5FS_t *fspace, const H5FS_section_class_t *cls, unsigned flags)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(fspace);
    assert(fspace->sinfo);
    assert(cls);

    /* Increment total # of sections on free space list */
    fspace->tot_sect_count++;

    /* Check for serializable or 'ghost' section */
    if (cls->flags & H5FS_CLS_GHOST_OBJ) {
        /* Sanity check */
        assert(cls->serial_size == 0);

        /* Increment # of ghost sections */
        fspace->ghost_sect_count++;
    } /* end if */
    else {
        /* Increment # of serializable sections */
        fspace->serial_sect_count++;

        /* Increment amount of space required to serialize all sections */
        fspace->sinfo->serial_size += cls->serial_size;

        /* Update the free space sections' serialized size */
        /* (if we're not deserializing the sections from disk) */
        if (!(flags & H5FS_ADD_DESERIALIZING)) {
            if (H5FS__sect_serialize_size(fspace) < 0)
                HGOTO_ERROR(H5E_FSPACE, H5E_CANTCOMPUTE, FAIL,
                            "can't adjust free space section size on disk");
        } /* end if */
    }     /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS__sect_increase() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__sect_decrease
 *
 * Purpose:     Decrease the size of the serialized free space section info
 *              on disk
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__sect_decrease(H5FS_t *fspace, const H5FS_section_class_t *cls)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(fspace);
    assert(fspace->sinfo);
    assert(cls);

    /* Decrement total # of sections in free space manager */
    fspace->tot_sect_count--;

    /* Check for serializable or 'ghost' section */
    if (cls->flags & H5FS_CLS_GHOST_OBJ) {
        /* Sanity check */
        assert(cls->serial_size == 0);

        /* Decrement # of ghost sections */
        fspace->ghost_sect_count--;
    } /* end if */
    else {
        /* Decrement # of serializable sections */
        fspace->serial_sect_count--;

        /* Decrement amount of space required to serialize all sections */
        fspace->sinfo->serial_size -= cls->serial_size;

        /* Update the free space sections' serialized size */
        if (H5FS__sect_serialize_size(fspace) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTCOMPUTE, FAIL, "can't adjust free space section size on disk");
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS__sect_decrease() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__size_node_decr
 *
 * Purpose:     Decrement the number of sections of a particular size
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__size_node_decr(H5FS_sinfo_t *sinfo, unsigned bin, H5FS_node_t *fspace_node,
                     const H5FS_section_class_t *cls)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(sinfo);
    assert(fspace_node);
    assert(cls);

    /* Decrement the # of sections in this bin */
    /* (Different from the # of items in the bin's skiplist, since each node on
     *  the bin's skiplist is also a skiplist...)
     */
    sinfo->bins[bin].tot_sect_count--;

    /* Check for 'ghost' or 'serializable' section */
    if (cls->flags & H5FS_CLS_GHOST_OBJ) {
        /* Decrement node's ghost section count */
        fspace_node->ghost_count--;

        /* Decrement bin's ghost section count */
        sinfo->bins[bin].ghost_sect_count--;

        /* If the node has no more ghost sections, decrement number of ghost section sizes managed */
        if (fspace_node->ghost_count == 0)
            sinfo->ghost_size_count--;
    } /* end if */
    else {
        /* Decrement node's serializable section count */
        fspace_node->serial_count--;

        /* Decrement bin's serializable section count */
        sinfo->bins[bin].serial_sect_count--;

        /* If the node has no more serializable sections, decrement number of serializable section sizes
         * managed */
        if (fspace_node->serial_count == 0)
            sinfo->serial_size_count--;
    } /* end else */

    /* Check for no more nodes on list of that size */
    if (H5SL_count(fspace_node->sect_list) == 0) {
        H5FS_node_t *tmp_fspace_node; /* Free space list size node */

        /* Sanity checks */
        assert(fspace_node->ghost_count == 0);
        assert(fspace_node->serial_count == 0);

        /* Remove size tracking list from bin */
        tmp_fspace_node = (H5FS_node_t *)H5SL_remove(sinfo->bins[bin].bin_list, &fspace_node->sect_size);
        if (tmp_fspace_node == NULL || tmp_fspace_node != fspace_node)
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTREMOVE, FAIL, "can't remove free space node from skip list");

        /* Destroy skip list for size tracking node */
        if (H5SL_close(fspace_node->sect_list) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTCLOSEOBJ, FAIL, "can't destroy size tracking node's skip list");

        /* Release free space list node */
        fspace_node = H5FL_FREE(H5FS_node_t, fspace_node);

        /* Decrement total number of section sizes managed */
        sinfo->tot_size_count--;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS__size_node_decr() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__sect_unlink_size
 *
 * Purpose:     Remove a section node from size tracking data structures for
 *              a free space manager
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__sect_unlink_size(H5FS_sinfo_t *sinfo, const H5FS_section_class_t *cls, H5FS_section_info_t *sect)
{
    H5FS_node_t         *fspace_node;         /* Free list size node */
    H5FS_section_info_t *tmp_sect_node;       /* Temporary section node */
    unsigned             bin;                 /* Bin to put the free space section in */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(sinfo);
    assert(sinfo->bins);
    assert(sect);
    assert(cls);

    /* Determine correct bin which holds items of at least the section's size */
    bin = H5VM_log2_gen(sect->size);
    assert(bin < sinfo->nbins);
    if (sinfo->bins[bin].bin_list == NULL)
        HGOTO_ERROR(H5E_FSPACE, H5E_NOTFOUND, FAIL, "node's bin is empty?");

    /* Find space node for section's size */
    if ((fspace_node = (H5FS_node_t *)H5SL_search(sinfo->bins[bin].bin_list, &sect->size)) == NULL)
        HGOTO_ERROR(H5E_FSPACE, H5E_NOTFOUND, FAIL, "can't find section size node");

    /* Remove the section's node from the list */
    tmp_sect_node = (H5FS_section_info_t *)H5SL_remove(fspace_node->sect_list, &sect->addr);
    if (tmp_sect_node == NULL || tmp_sect_node != sect)
        HGOTO_ERROR(H5E_FSPACE, H5E_NOTFOUND, FAIL, "can't find section node on size list");

    /* Decrement # of sections in section size node */
    if (H5FS__size_node_decr(sinfo, bin, fspace_node, cls) < 0)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTREMOVE, FAIL, "can't remove free space size node from skip list");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS__sect_unlink_size() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__sect_unlink_rest
 *
 * Purpose:     Finish unlinking a section from the rest of the free space
 *              manager's data structures, after the section has been removed
 *              from the size tracking data structures
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__sect_unlink_rest(H5FS_t *fspace, const H5FS_section_class_t *cls, H5FS_section_info_t *sect)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(fspace);
    assert(fspace->sinfo);
    assert(cls);
    assert(sect);

    /* Remove node from merge list, if it was entered there */
    if (!(cls->flags & H5FS_CLS_SEPAR_OBJ)) {
        H5FS_section_info_t *tmp_sect_node; /* Temporary section node */

        tmp_sect_node = (H5FS_section_info_t *)H5SL_remove(fspace->sinfo->merge_list, &sect->addr);
        if (tmp_sect_node == NULL || tmp_sect_node != sect)
            HGOTO_ERROR(H5E_FSPACE, H5E_NOTFOUND, FAIL, "can't find section node on size list");
    } /* end if */

    /* Update section info & check if we need less room for the serialized free space sections */
    if (H5FS__sect_decrease(fspace, cls) < 0)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTINSERT, FAIL, "can't increase free space section size on disk");

    /* Decrement amount of free space managed */
    fspace->tot_space -= sect->size;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS__sect_unlink_rest() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__sect_remove_real
 *
 * Purpose:     Remove a section from the free space manager
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__sect_remove_real(H5FS_t *fspace, H5FS_section_info_t *sect)
{
    const H5FS_section_class_t *cls;                 /* Class of section */
    herr_t                      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(fspace);
    assert(fspace->sinfo);
    assert(sect);

    /* Get section's class */
    cls = &fspace->sect_cls[sect->type];

    /* Remove node from size tracked data structures */
    if (H5FS__sect_unlink_size(fspace->sinfo, cls, sect) < 0)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTFREE, FAIL,
                    "can't remove section from size tracking data structures");

    /* Update rest of free space manager data structures for node removal */
    if (H5FS__sect_unlink_rest(fspace, cls, sect) < 0)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTFREE, FAIL,
                    "can't remove section from non-size tracking data structures");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS__sect_remove_real() */

/*-------------------------------------------------------------------------
 * Function:    H5FS_sect_remove
 *
 * Purpose:     Remove a section from the free space manager
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FS_sect_remove(H5F_t *f, H5FS_t *fspace, H5FS_section_info_t *sect)
{
    bool   sinfo_valid = false;   /* Whether the section info is valid */
    herr_t ret_value   = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Check arguments. */
    assert(f);
    assert(fspace);
    assert(sect);

    /* Get a pointer to the section info */
    if (H5FS__sinfo_lock(f, fspace, H5AC__NO_FLAGS_SET) < 0)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTGET, FAIL, "can't get section info");
    sinfo_valid = true;

    /* Perform actual section removal */
    if (H5FS__sect_remove_real(fspace, sect) < 0)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTREMOVE, FAIL, "can't remove section");

done:
    /* Release the section info */
    if (sinfo_valid && H5FS__sinfo_unlock(f, fspace, true) < 0)
        HDONE_ERROR(H5E_FSPACE, H5E_CANTRELEASE, FAIL, "can't release section info");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS_sect_remove() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__sect_link_size
 *
 * Purpose:     Add a section of free space to the free list bins
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__sect_link_size(H5FS_sinfo_t *sinfo, const H5FS_section_class_t *cls, H5FS_section_info_t *sect)
{
    H5FS_node_t *fspace_node       = NULL;  /* Pointer to free space node of the correct size */
    bool         fspace_node_alloc = false; /* Whether the free space node was allocated */
    unsigned     bin;                       /* Bin to put the free space section in */
    herr_t       ret_value = SUCCEED;       /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(sinfo);
    assert(sect);
    assert(H5_addr_defined(sect->addr));
    assert(sect->size);

    /* Determine correct bin which holds items of the section's size */
    bin = H5VM_log2_gen(sect->size);
    assert(bin < sinfo->nbins);
    if (sinfo->bins[bin].bin_list == NULL) {
        if (NULL == (sinfo->bins[bin].bin_list = H5SL_create(H5SL_TYPE_HSIZE, NULL)))
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTCREATE, FAIL, "can't create skip list for free space nodes");
    } /* end if */
    else
        /* Check for node list of the correct size already */
        fspace_node = (H5FS_node_t *)H5SL_search(sinfo->bins[bin].bin_list, &sect->size);

    /* Check if we need to create a new skip list for nodes of this size */
    if (fspace_node == NULL) {
        /* Allocate new free list size node */
        if (NULL == (fspace_node = H5FL_MALLOC(H5FS_node_t)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed for free space node");
        fspace_node_alloc = true;

        /* Initialize the free list size node */
        fspace_node->sect_size    = sect->size;
        fspace_node->serial_count = fspace_node->ghost_count = 0;
        if (NULL == (fspace_node->sect_list = H5SL_create(H5SL_TYPE_HADDR, NULL)))
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTCREATE, FAIL, "can't create skip list for free space nodes");

        /* Insert new free space size node into bin's list */
        if (H5SL_insert(sinfo->bins[bin].bin_list, fspace_node, &fspace_node->sect_size) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTINSERT, FAIL, "can't insert free space node into skip list");
        fspace_node_alloc = false; /* (owned by the bin skip list now, don't need to free on error) */

        /* Increment number of section sizes */
        sinfo->tot_size_count++;
    } /* end if */

    /* Increment # of section in bin */
    /* (Different from the # of items in the bin's skiplist, since each node on
     *  the bin's skiplist is also a skiplist...)
     */
    sinfo->bins[bin].tot_sect_count++;
    if (cls->flags & H5FS_CLS_GHOST_OBJ) {
        sinfo->bins[bin].ghost_sect_count++;
        fspace_node->ghost_count++;

        /* Check for first ghost section in node */
        if (fspace_node->ghost_count == 1)
            sinfo->ghost_size_count++;
    } /* end if */
    else {
        sinfo->bins[bin].serial_sect_count++;
        fspace_node->serial_count++;

        /* Check for first serializable section in node */
        if (fspace_node->serial_count == 1)
            sinfo->serial_size_count++;
    } /* end else */

    /* Insert free space node into correct skip list */
    if (H5SL_insert(fspace_node->sect_list, sect, &sect->addr) < 0)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTINSERT, FAIL, "can't insert free space node into skip list");

done:
    if (ret_value < 0)
        if (fspace_node && fspace_node_alloc) {
            if (fspace_node->sect_list && H5SL_close(fspace_node->sect_list) < 0)
                HDONE_ERROR(H5E_FSPACE, H5E_CANTCLOSEOBJ, FAIL,
                            "can't destroy size free space node's skip list");
            fspace_node = H5FL_FREE(H5FS_node_t, fspace_node);
        } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS__sect_link_size() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__sect_link_rest
 *
 * Purpose:     Link a section into the rest of the non-size tracking
 *              free space manager data structures
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__sect_link_rest(H5FS_t *fspace, const H5FS_section_class_t *cls, H5FS_section_info_t *sect,
                     unsigned flags)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(fspace);
    assert(fspace->sinfo);
    assert(sect);

    /* Add section to the address-ordered list of sections, if allowed */
    if (!(cls->flags & H5FS_CLS_SEPAR_OBJ)) {
        if (fspace->sinfo->merge_list == NULL)
            if (NULL == (fspace->sinfo->merge_list = H5SL_create(H5SL_TYPE_HADDR, NULL)))
                HGOTO_ERROR(H5E_FSPACE, H5E_CANTCREATE, FAIL,
                            "can't create skip list for merging free space sections");
        if (H5SL_insert(fspace->sinfo->merge_list, sect, &sect->addr) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTINSERT, FAIL,
                        "can't insert free space node into merging skip list");
    } /* end if */

    /* Update section info & check if we need more room for the serialized free space sections */
    if (H5FS__sect_increase(fspace, cls, flags) < 0)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTINSERT, FAIL, "can't increase free space section size on disk");

    /* Increment amount of free space managed */
    fspace->tot_space += sect->size;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS__sect_link_rest() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__sect_link
 *
 * Purpose:     Link a section into the internal data structures
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__sect_link(H5FS_t *fspace, H5FS_section_info_t *sect, unsigned flags)
{
    const H5FS_section_class_t *cls;                 /* Class of section */
    herr_t                      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(fspace);
    assert(fspace->sinfo);
    assert(sect);

    /* Get section's class */
    cls = &fspace->sect_cls[sect->type];

    /* Add section to size tracked data structures */
    if (H5FS__sect_link_size(fspace->sinfo, cls, sect) < 0)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTINSERT, FAIL, "can't add section to size tracking data structures");

    /* Update rest of free space manager data structures for section addition */
    if (H5FS__sect_link_rest(fspace, cls, sect, flags) < 0)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTINSERT, FAIL,
                    "can't add section to non-size tracking data structures");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS__sect_link() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__sect_merge
 *
 * Purpose:     Attempt to merge a returned free space section with existing
 *              free space.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__sect_merge(H5FS_t *fspace, H5FS_section_info_t **sect, void *op_data)
{
    H5FS_section_class_t *sect_cls;            /* Section's class */
    bool                  modified;            /* Flag to indicate merge or shrink occurred */
    bool                  remove_sect = false; /* Whether a section should be removed before shrinking */
    htri_t                status;              /* Status value */
    herr_t                ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(fspace);
    assert(*sect);
    assert(H5_addr_defined((*sect)->addr));
    assert((*sect)->size);

    /* Loop until no more merging */
    if (fspace->sinfo->merge_list) {
        do {
            H5SL_node_t *less_sect_node;           /* Skip list node for section less than new section */
            H5SL_node_t *greater_sect_node = NULL; /* Skip list node for section greater than new section */
            H5FS_section_info_t  *tmp_sect;        /* Temporary free space section */
            H5FS_section_class_t *tmp_sect_cls;    /* Temporary section's class */
            bool greater_sect_node_valid = false;  /* Indicate if 'greater than' section node is valid */

            /* Reset 'modification occurred' flag */
            modified = false;

            /* Look for neighboring section before new section */
            less_sect_node = H5SL_below(fspace->sinfo->merge_list, &(*sect)->addr);

            /* Check for node before new node able to merge with new node */
            if (less_sect_node) {
                /* Check for node greater than section */
                greater_sect_node       = H5SL_next(less_sect_node);
                greater_sect_node_valid = true;

                /* Get section for 'less than' skip list node */
                tmp_sect = (H5FS_section_info_t *)H5SL_item(less_sect_node);

                /* Get classes for right & left sections */
                tmp_sect_cls = &fspace->sect_cls[tmp_sect->type];
                sect_cls     = &fspace->sect_cls[(*sect)->type];

                /* Check if sections of the left most class can merge with sections
                 *  of another class & whether the sections are the same type,
                 *  then check for 'can merge' callback
                 */
                if ((!(tmp_sect_cls->flags & H5FS_CLS_MERGE_SYM) || (tmp_sect->type == (*sect)->type)) &&
                    tmp_sect_cls->can_merge) {
                    /* Determine if the sections can merge */
                    if ((status = (*tmp_sect_cls->can_merge)(tmp_sect, *sect, op_data)) < 0)
                        HGOTO_ERROR(H5E_FSPACE, H5E_CANTMERGE, FAIL, "can't check for merging sections");
                    if (status > 0) {
                        /* Sanity check */
                        assert(tmp_sect_cls->merge);

                        /* Remove 'less than' node from data structures */
                        if (H5FS__sect_remove_real(fspace, tmp_sect) < 0)
                            HGOTO_ERROR(H5E_FSPACE, H5E_CANTRELEASE, FAIL,
                                        "can't remove section from internal data structures");

                        /* Merge the two sections together */
                        if ((*tmp_sect_cls->merge)(&tmp_sect, *sect, op_data) < 0)
                            HGOTO_ERROR(H5E_FSPACE, H5E_CANTINSERT, FAIL, "can't merge two sections");

                        /* Retarget section pointer to 'less than' node that was merged into */
                        *sect = tmp_sect;
                        if (*sect == NULL)
                            HGOTO_DONE(ret_value);

                        /* Indicate successful merge occurred */
                        modified = true;
                    } /* end if */
                }     /* end if */
            }         /* end if */

            /* Look for section after new (or merged) section, if not already determined */
            if (!greater_sect_node_valid)
                greater_sect_node = H5SL_above(fspace->sinfo->merge_list, &(*sect)->addr);

            /* Check for node after new node able to merge with new node */
            if (greater_sect_node) {
                /* Get section for 'greater than' skip list node */
                tmp_sect = (H5FS_section_info_t *)H5SL_item(greater_sect_node);

                /* Get classes for right & left sections */
                sect_cls     = &fspace->sect_cls[(*sect)->type];
                tmp_sect_cls = &fspace->sect_cls[tmp_sect->type];

                /* Check if sections of the left most class can merge with sections
                 *  of another class & whether the sections are the same type,
                 *  then check for 'can merge' callback
                 */
                if ((!(sect_cls->flags & H5FS_CLS_MERGE_SYM) || ((*sect)->type == tmp_sect->type)) &&
                    sect_cls->can_merge) {

                    /* Determine if the sections can merge */
                    if ((status = (*sect_cls->can_merge)(*sect, tmp_sect, op_data)) < 0)
                        HGOTO_ERROR(H5E_FSPACE, H5E_CANTMERGE, FAIL, "can't check for merging sections");
                    if (status > 0) {
                        /* Sanity check */
                        assert(sect_cls->merge);

                        /* Remove 'greater than' node from data structures */
                        if (H5FS__sect_remove_real(fspace, tmp_sect) < 0)
                            HGOTO_ERROR(H5E_FSPACE, H5E_CANTRELEASE, FAIL,
                                        "can't remove section from internal data structures");

                        /* Merge the two sections together */
                        if ((*sect_cls->merge)(sect, tmp_sect, op_data) < 0)
                            HGOTO_ERROR(H5E_FSPACE, H5E_CANTINSERT, FAIL, "can't merge two sections");

                        /* It's possible that the merge caused the section to be deleted (particularly in the
                         * paged allocation case) */
                        if (*sect == NULL)
                            HGOTO_DONE(ret_value);

                        /* Indicate successful merge occurred */
                        modified = true;
                    } /* end if */
                }     /* end if */
            }         /* end if */
        } while (modified);
    } /* end if */
    assert(*sect);

    /* Loop until no more shrinking */
    do {
        /* Reset 'modification occurred' flag */
        modified = false;

        /* Check for (possibly merged) section able to shrink the size of the container */
        sect_cls = &fspace->sect_cls[(*sect)->type];
        if (sect_cls->can_shrink) {
            if ((status = (*sect_cls->can_shrink)(*sect, op_data)) < 0)
                HGOTO_ERROR(H5E_FSPACE, H5E_CANTSHRINK, FAIL, "can't check for shrinking container");
            if (status > 0) {
                /* Remove SECT from free-space manager */
                /* (only possible to happen on second+ pass through loop) */
                if (remove_sect) {
                    if (H5FS__sect_remove_real(fspace, *sect) < 0)
                        HGOTO_ERROR(H5E_FSPACE, H5E_CANTRELEASE, FAIL,
                                    "can't remove section from internal data structures");
                    remove_sect = false;
                } /* end if */

                /* Shrink the container */
                /* (callback can indicate that it has discarded the section by setting *sect to NULL) */
                assert(sect_cls->shrink);
                if ((*sect_cls->shrink)(sect, op_data) < 0)
                    HGOTO_ERROR(H5E_FSPACE, H5E_CANTINSERT, FAIL, "can't shrink free space container");

                /* If this section was shrunk away, we may need to shrink another section */
                if (*sect == NULL) {
                    /* Check for sections on merge list */
                    if (fspace->sinfo->merge_list) {
                        H5SL_node_t *last_node; /* Last node in merge list */

                        /* Check for last node in the merge list */
                        if (NULL != (last_node = H5SL_last(fspace->sinfo->merge_list))) {
                            /* Get the pointer to the last section, from the last node */
                            *sect = (H5FS_section_info_t *)H5SL_item(last_node);
                            assert(*sect);

                            /* Indicate that this section needs to be removed if it causes a shrink */
                            remove_sect = true;
                        } /* end if */
                    }     /* end if */
                }         /* end if */

                /* Indicate successful merge occurred */
                modified = true;
            } /* end if */
        }     /* end if */
    } while (modified && *sect);

    /* Check for section that was shrunk away and next section not shrinking */
    if (remove_sect && (*sect != NULL))
        *sect = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS__sect_merge() */

/*-------------------------------------------------------------------------
 * Function:    H5FS_sect_add
 *
 * Purpose:     Add a section of free space to the free list
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FS_sect_add(H5F_t *f, H5FS_t *fspace, H5FS_section_info_t *sect, unsigned flags, void *op_data)
{
    H5FS_section_class_t *cls;                      /* Section's class */
    bool                  sinfo_valid    = false;   /* Whether the section info is valid */
    bool                  sinfo_modified = false;   /* Whether the section info was modified */
    herr_t                ret_value      = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

#ifdef H5FS_SINFO_DEBUG
    fprintf(stderr, "%s: *sect = {%" PRIuHADDR ", %" PRIuHSIZE ", %u, %s}\n", __func__, sect->addr,
            sect->size, sect->type,
            (sect->state == H5FS_SECT_LIVE ? "H5FS_SECT_LIVE" : "H5FS_SECT_SERIALIZED"));
#endif /* H5FS_SINFO_DEBUG */

    /* Check arguments. */
    assert(fspace);
    assert(sect);
    assert(H5_addr_defined(sect->addr));
    assert(sect->size);

    /* Get a pointer to the section info */
    if (H5FS__sinfo_lock(f, fspace, H5AC__NO_FLAGS_SET) < 0)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTGET, FAIL, "can't get section info");
    sinfo_valid = true;

    /* Call "add" section class callback, if there is one */
    cls = &fspace->sect_cls[sect->type];
    if (cls->add)
        if ((*cls->add)(&sect, &flags, op_data) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTINSERT, FAIL, "'add' section class callback failed");

    /* Check for merging returned space with existing section node */
    if (flags & H5FS_ADD_RETURNED_SPACE) {
#ifdef H5FS_SINFO_DEBUG
        fprintf(stderr, "%s: Returning space\n", __func__);
#endif /* H5FS_SINFO_DEBUG */

        /* Attempt to merge returned section with existing sections */
        if (H5FS__sect_merge(fspace, &sect, op_data) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTMERGE, FAIL, "can't merge sections");
    } /* end if */

    /* Add new (possibly merged) node to free sections data structures */
    /* (If section has been completely merged or shrunk away, 'sect' will
     *  be NULL at this point - QAK)
     */
    if (sect)
        if (H5FS__sect_link(fspace, sect, flags) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTINSERT, FAIL, "can't insert free space section into skip list");

#ifdef H5FS_SINFO_DEBUG
    fprintf(stderr, "%s: fspace->tot_space = %" PRIuHSIZE "\n", __func__, fspace->tot_space);
#endif /* H5FS_SINFO_DEBUG */
    /* Mark free space sections as changed */
    /* (if adding sections while deserializing sections, don't set the flag) */
    if (!(flags & (H5FS_ADD_DESERIALIZING | H5FS_PAGE_END_NO_ADD)))
        sinfo_modified = true;

done:
    /* Release the section info */
    if (sinfo_valid && H5FS__sinfo_unlock(f, fspace, sinfo_modified) < 0)
        HDONE_ERROR(H5E_FSPACE, H5E_CANTRELEASE, FAIL, "can't release section info");

#ifdef H5FS_DEBUG_ASSERT
    if (!(flags & (H5FS_ADD_DESERIALIZING | H5FS_ADD_SKIP_VALID)))
        H5FS__assert(fspace);
#endif /* H5FS_DEBUG_ASSERT */
#ifdef H5FS_SINFO_DEBUG
    fprintf(stderr, "%s: Leaving, ret_value = %d\n", __func__, ret_value);
#endif /* H5FS_SINFO_DEBUG */
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS_sect_add() */

/*-------------------------------------------------------------------------
 * Function:    H5FS_sect_try_extend
 *
 * Purpose:     Try to extend a block using space from a section on the free list
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5FS_sect_try_extend(H5F_t *f, H5FS_t *fspace, haddr_t addr, hsize_t size, hsize_t extra_requested,
                     unsigned flags, void *op_data)
{
    bool   sinfo_valid    = false; /* Whether the section info is valid */
    bool   sinfo_modified = false; /* Whether the section info was modified */
    htri_t ret_value      = false; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

#ifdef H5FS_SINFO_DEBUG
    fprintf(stderr, "%s: addr = %" PRIuHADDR ", size = %" PRIuHSIZE ", extra_requested = %" PRIuHSIZE "\n",
            __func__, addr, size, extra_requested);
#endif /* H5FS_SINFO_DEBUG */

    /* Check arguments. */
    assert(f);
    assert(fspace);
    assert(H5_addr_defined(addr));
    assert(size > 0);
    assert(extra_requested > 0);

    /* Check for any sections on free space list */
#ifdef H5FS_SINFO_DEBUG
    fprintf(stderr, "%s: fspace->tot_sect_count = %" PRIuHSIZE "\n", __func__, fspace->tot_sect_count);
    fprintf(stderr, "%s: fspace->serial_sect_count = %" PRIuHSIZE "\n", __func__, fspace->serial_sect_count);
    fprintf(stderr, "%s: fspace->ghost_sect_count = %" PRIuHSIZE "\n", __func__, fspace->ghost_sect_count);
#endif /* H5FS_SINFO_DEBUG */
    if (fspace->tot_sect_count > 0) {
        H5FS_section_info_t *sect; /* Temporary free space section */

        /* Get a pointer to the section info */
        if (H5FS__sinfo_lock(f, fspace, H5AC__NO_FLAGS_SET) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTGET, FAIL, "can't get section info");
        sinfo_valid = true;

        /*

        Pseudo-code for algorithm:

        _section_ = <Get pointer to section with address > _region.addr_>
        if(_section_)
            if(_section_ adjoins _region_ && _section.size_ >= _extra_requested_)
                <remove section from data structures>
                if(_section.size_ > _extra_requested_)
                    if(<can adjust _section_>)
                        <adjust _section_ by _extra_requested_>
                        <add adjusted section back to data structures>
                    else
                        <re-add UNadjusted section back to data structures>
                        <error>
                <mark free space sections as changed in metadata cache>

        */
        /* Look for a section after block to extend */
        if ((sect = (H5FS_section_info_t *)H5SL_greater(fspace->sinfo->merge_list, &addr))) {
            /* Check if this section adjoins the block and is large enough to
             *  fulfill extension request.
             *
             * (Note: we assume that the section is fully merged with any
             *  possible neighboring nodes and is not at the end of the file
             *  (or it would have been eliminated), etc)
             */
            if (sect->size >= extra_requested && (addr + size) == sect->addr) {
                H5FS_section_class_t *cls; /* Section's class */

                /* Remove section from data structures */
                if (H5FS__sect_remove_real(fspace, sect) < 0)
                    HGOTO_ERROR(H5E_FSPACE, H5E_CANTRELEASE, FAIL,
                                "can't remove section from internal data structures");

                /* Get class for section */
                cls = &fspace->sect_cls[sect->type];

                /* Check for the section needing to be adjusted and re-added */
                /* (Note: we should probably add a can_adjust/adjust callback
                 *      to the section class structure, but we don't need it
                 *      for the current usage, so I've deferred messing with
                 *      it. - QAK - 2008/01/08)
                 */
                if (sect->size > extra_requested) {
                    /* Sanity check (for now) */
                    assert(cls->flags & H5FS_CLS_ADJUST_OK);

                    /* Adjust section by amount requested */
                    sect->addr += extra_requested;
                    sect->size -= extra_requested;
                    if (cls->add)
                        if ((*cls->add)(&sect, &flags, op_data) < 0)
                            HGOTO_ERROR(H5E_FSPACE, H5E_CANTINSERT, FAIL,
                                        "'add' section class callback failed");

                    /* Re-adding the section could cause it to disappear (particularly when paging) */
                    if (sect) {
                        /* Re-add adjusted section to free sections data structures */
                        if (H5FS__sect_link(fspace, sect, 0) < 0)
                            HGOTO_ERROR(H5E_FSPACE, H5E_CANTINSERT, FAIL,
                                        "can't insert free space section into skip list");
                    } /* end if */
                }     /* end if */
                else {
                    /* Sanity check */
                    assert(sect->size == extra_requested);

                    /* Exact match, so just free section */
                    if ((*cls->free)(sect) < 0)
                        HGOTO_ERROR(H5E_FSPACE, H5E_CANTFREE, FAIL, "can't free section");
                } /* end else */

                /* Note that we modified the section info */
                sinfo_modified = true;

                /* Indicate success */
                HGOTO_DONE(true);
            } /* end if */
        }     /* end if */
    }         /* end if */

done:
    /* Release the section info */
    if (sinfo_valid && H5FS__sinfo_unlock(f, fspace, sinfo_modified) < 0)
        HDONE_ERROR(H5E_FSPACE, H5E_CANTRELEASE, FAIL, "can't release section info");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS_sect_try_extend() */

/*-------------------------------------------------------------------------
 * Function:    H5FS_sect_try_merge
 *
 * Purpose:     Try to merge/shrink a block
 *
 * Return:      true:       merged/shrunk
 *              false:      not merged/not shrunk
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5FS_sect_try_merge(H5F_t *f, H5FS_t *fspace, H5FS_section_info_t *sect, unsigned flags, void *op_data)
{
    bool    sinfo_valid    = false; /* Whether the section info is valid */
    bool    sinfo_modified = false; /* Whether the section info was modified */
    hsize_t saved_fs_size;          /* Copy of the free-space section size */
    htri_t  ret_value = false;      /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments. */
    assert(f);
    assert(fspace);
    assert(sect);
    assert(H5_addr_defined(sect->addr));
    assert(sect->size);

    /* Get a pointer to the section info */
    if (H5FS__sinfo_lock(f, fspace, H5AC__NO_FLAGS_SET) < 0)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTGET, FAIL, "can't get section info");
    sinfo_valid   = true;
    saved_fs_size = sect->size;

    /* Attempt to merge/shrink section with existing sections */
    if (H5FS__sect_merge(fspace, &sect, op_data) < 0)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTMERGE, FAIL, "can't merge sections");

    /* Check if section is shrunk and/or merged away completely */
    if (!sect) {
        sinfo_modified = true;
        HGOTO_DONE(true);
    } /* end if */
    else {
        /* Check if section is merged */
        if (sect->size != saved_fs_size) {
            if (H5FS__sect_link(fspace, sect, flags) < 0)
                HGOTO_ERROR(H5E_FSPACE, H5E_CANTINSERT, FAIL,
                            "can't insert free space section into skip list");
            sinfo_modified = true;
            HGOTO_DONE(true);
        } /* end if */
    }     /* end else */

done:
    /* Release the section info */
    if (sinfo_valid && H5FS__sinfo_unlock(f, fspace, sinfo_modified) < 0)
        HDONE_ERROR(H5E_FSPACE, H5E_CANTRELEASE, FAIL, "can't release section info");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS_sect_try_merge() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__sect_find_node
 *
 * Purpose:     Locate a section of free space (in existing free space list
 *              bins) that is large enough to fulfill request.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5FS__sect_find_node(H5FS_t *fspace, hsize_t request, H5FS_section_info_t **node)
{
    H5FS_node_t *fspace_node;       /* Free list size node */
    unsigned     bin;               /* Bin to put the free space section in */
    htri_t       ret_value = false; /* Return value */

    H5SL_node_t                *curr_size_node = NULL;
    const H5FS_section_class_t *cls; /* Class of section */
    hsize_t                     alignment;

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(fspace);
    assert(fspace->sinfo);
    assert(fspace->sinfo->bins);
    assert(request > 0);
    assert(node);

    /* Determine correct bin which holds items of at least the section's size */
    bin = H5VM_log2_gen(request);
    assert(bin < fspace->sinfo->nbins);
    alignment = fspace->alignment;
    if (!((alignment > 1) && (request >= fspace->align_thres)))
        alignment = 0; /* no alignment */

    do {
        /* Check if there's any sections in this bin */
        if (fspace->sinfo->bins[bin].bin_list) {

            if (!alignment) { /* no alignment */
                /* Find the first free space section that is large enough to fulfill request */
                /* (Since the bins use skip lists to track the sizes of the address-ordered
                 *  lists, this is actually a "best fit" algorithm)
                 */
                /* Look for large enough free space section in this bin */
                if ((fspace_node =
                         (H5FS_node_t *)H5SL_greater(fspace->sinfo->bins[bin].bin_list, &request))) {
                    /* Take first node off of the list (ie. node w/lowest address) */
                    if (NULL == (*node = (H5FS_section_info_t *)H5SL_remove_first(fspace_node->sect_list)))
                        HGOTO_ERROR(H5E_FSPACE, H5E_CANTREMOVE, FAIL,
                                    "can't remove free space node from skip list");

                    /* Get section's class */
                    cls = &fspace->sect_cls[(*node)->type];
                    /* Decrement # of sections in section size node */
                    if (H5FS__size_node_decr(fspace->sinfo, bin, fspace_node, cls) < 0)
                        HGOTO_ERROR(H5E_FSPACE, H5E_CANTREMOVE, FAIL,
                                    "can't remove free space size node from skip list");
                    if (H5FS__sect_unlink_rest(fspace, cls, *node) < 0)
                        HGOTO_ERROR(H5E_FSPACE, H5E_CANTFREE, FAIL,
                                    "can't remove section from non-size tracking data structures");
                    /* Indicate that we found a node for the request */
                    HGOTO_DONE(true);
                }  /* end if */
            }      /* end if */
            else { /* alignment is set */
                /* get the first node of a certain size in this bin */
                curr_size_node = H5SL_first(fspace->sinfo->bins[bin].bin_list);
                while (curr_size_node != NULL) {
                    H5FS_node_t *curr_fspace_node = NULL;
                    H5SL_node_t *curr_sect_node   = NULL;

                    /* Get the free space node for free space sections of the same size */
                    curr_fspace_node = (H5FS_node_t *)H5SL_item(curr_size_node);

                    /* Get the Skip list which holds  pointers to actual free list sections */
                    curr_sect_node = (H5SL_node_t *)H5SL_first(curr_fspace_node->sect_list);

                    while (curr_sect_node != NULL) {
                        H5FS_section_info_t *curr_sect = NULL;
                        hsize_t              mis_align = 0, frag_size = 0;
                        H5FS_section_info_t *split_sect = NULL;

                        /* Get section node */
                        curr_sect = (H5FS_section_info_t *)H5SL_item(curr_sect_node);

                        assert(H5_addr_defined(curr_sect->addr));
                        assert(curr_fspace_node->sect_size == curr_sect->size);

                        cls = &fspace->sect_cls[curr_sect->type];

                        assert(alignment);
                        assert(cls);

                        if ((mis_align = curr_sect->addr % alignment))
                            frag_size = alignment - mis_align;

                        if ((curr_sect->size >= (request + frag_size)) && (cls->split)) {
                            /* remove the section with aligned address */
                            if (NULL == (*node = (H5FS_section_info_t *)H5SL_remove(
                                             curr_fspace_node->sect_list, &curr_sect->addr)))
                                HGOTO_ERROR(H5E_FSPACE, H5E_CANTREMOVE, FAIL,
                                            "can't remove free space node from skip list");
                            /* Decrement # of sections in section size node */
                            if (H5FS__size_node_decr(fspace->sinfo, bin, curr_fspace_node, cls) < 0)
                                HGOTO_ERROR(H5E_FSPACE, H5E_CANTREMOVE, FAIL,
                                            "can't remove free space size node from skip list");

                            if (H5FS__sect_unlink_rest(fspace, cls, *node) < 0)
                                HGOTO_ERROR(H5E_FSPACE, H5E_CANTFREE, FAIL,
                                            "can't remove section from non-size tracking data structures");

                            /*
                             * The split() callback splits NODE into 2 sections:
                             *  split_sect is the unused fragment for aligning NODE
                             *  NODE's addr & size are updated to point to the remaining aligned section
                             * split_sect is re-added to free-space
                             */
                            if (mis_align) {
                                split_sect = cls->split(*node, frag_size);
                                if ((H5FS__sect_link(fspace, split_sect, 0) < 0))
                                    HGOTO_ERROR(H5E_FSPACE, H5E_CANTINSERT, FAIL,
                                                "can't insert free space section into skip list");
                                /* sanity check */
                                assert(split_sect->addr < (*node)->addr);
                                assert(request <= (*node)->size);
                            } /* end if */
                            /* Indicate that we found a node for the request */
                            HGOTO_DONE(true);
                        } /* end if */

                        /* Get the next section node in the list */
                        curr_sect_node = H5SL_next(curr_sect_node);
                    } /* end while of curr_sect_node */

                    /* Get the next size node in the bin */
                    curr_size_node = H5SL_next(curr_size_node);
                } /* end while of curr_size_node */
            }     /* else of alignment */
        }         /* if bin_list */
        /* Advance to next larger bin */
        bin++;
    } while (bin < fspace->sinfo->nbins);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS__sect_find_node() */

/*-------------------------------------------------------------------------
 * Function:    H5FS_sect_find
 *
 * Purpose:     Locate a section of free space (in existing free space list) that
 *              is large enough to fulfill request.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5FS_sect_find(H5F_t *f, H5FS_t *fspace, hsize_t request, H5FS_section_info_t **node)
{
    bool   sinfo_valid    = false; /* Whether the section info is valid */
    bool   sinfo_modified = false; /* Whether the section info was modified */
    htri_t ret_value      = false; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments. */
    assert(fspace);
    assert(fspace->nclasses);
    assert(request);
    assert(node);

    /* Check for any sections on free space list */
    if (fspace->tot_sect_count > 0) {
        /* Get a pointer to the section info */
        if (H5FS__sinfo_lock(f, fspace, H5AC__NO_FLAGS_SET) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTGET, FAIL, "can't get section info");
        sinfo_valid = true;

        /* Look for node in bins */
        if ((ret_value = H5FS__sect_find_node(fspace, request, node)) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTFREE, FAIL, "can't remove section from bins");

        /* Decrement # of sections on free list, if we found an object */
        if (ret_value > 0) {
            /* Note that we've modified the section info */
            sinfo_modified = true;
        } /* end if */
    }     /* end if */

done:
    /* Release the section info */
    if (sinfo_valid && H5FS__sinfo_unlock(f, fspace, sinfo_modified) < 0)
        HDONE_ERROR(H5E_FSPACE, H5E_CANTRELEASE, FAIL, "can't release section info");

#ifdef H5FS_DEBUG_ASSERT
    H5FS__assert(fspace);
#endif /* H5FS_DEBUG_ASSERT */
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS_sect_find() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__iterate_sect_cb
 *
 * Purpose:     Skip list iterator callback to iterate over free space sections
 *              of a particular size
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__iterate_sect_cb(void *_item, void H5_ATTR_UNUSED *key, void *_udata)
{
    H5FS_section_info_t *sect_info = (H5FS_section_info_t *)_item; /* Free space section to work on */
    H5FS_iter_ud_t      *udata     = (H5FS_iter_ud_t *)_udata;     /* Callback info */
    herr_t               ret_value = SUCCEED;                      /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(sect_info);
    assert(udata->fspace);
    assert(udata->op);

    /* Make callback for this section */
    if ((*udata->op)(sect_info, udata->op_data) < 0)
        HGOTO_ERROR(H5E_FSPACE, H5E_BADITER, FAIL, "iteration callback failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS__iterate_sect_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5FS__iterate_node_cb
 *
 * Purpose:     Skip list iterator callback to iterate over free space sections
 *              in a bin
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FS__iterate_node_cb(void *_item, void H5_ATTR_UNUSED *key, void *_udata)
{
    H5FS_node_t    *fspace_node = (H5FS_node_t *)_item;     /* Free space size node to work on */
    H5FS_iter_ud_t *udata       = (H5FS_iter_ud_t *)_udata; /* Callback info */
    herr_t          ret_value   = SUCCEED;                  /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(fspace_node);
    assert(udata->fspace);
    assert(udata->op);

    /* Iterate through all the sections of this size */
    assert(fspace_node->sect_list);
    if (H5SL_iterate(fspace_node->sect_list, H5FS__iterate_sect_cb, udata) < 0)
        HGOTO_ERROR(H5E_FSPACE, H5E_BADITER, FAIL, "can't iterate over section nodes");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS__iterate_node_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5FS_sect_iterate
 *
 * Purpose:     Iterate over all the sections managed
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FS_sect_iterate(H5F_t *f, H5FS_t *fspace, H5FS_operator_t op, void *op_data)
{
    H5FS_iter_ud_t udata;                 /* User data for callbacks */
    bool           sinfo_valid = false;   /* Whether the section info is valid */
    herr_t         ret_value   = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Check arguments. */
    assert(fspace);
    assert(op);

    /* Set up user data for iterator */
    udata.fspace  = fspace;
    udata.op      = op;
    udata.op_data = op_data;

    /* Iterate over sections, if there are any */
    if (fspace->tot_sect_count) {
        unsigned bin; /* Current bin we are on */

        /* Get a pointer to the section info */
        if (H5FS__sinfo_lock(f, fspace, H5AC__READ_ONLY_FLAG) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTGET, FAIL, "can't get section info");
        sinfo_valid = true;

        /* Iterate over all the bins */
        for (bin = 0; bin < fspace->sinfo->nbins; bin++) {
            /* Check if there are any sections in this bin */
            if (fspace->sinfo->bins[bin].bin_list) {
                /* Iterate over list of section size nodes for bin */
                if (H5SL_iterate(fspace->sinfo->bins[bin].bin_list, H5FS__iterate_node_cb, &udata) < 0)
                    HGOTO_ERROR(H5E_FSPACE, H5E_BADITER, FAIL, "can't iterate over section size nodes");
            } /* end if */
        }     /* end for */
    }         /* end if */

done:
    /* Release the section info */
    if (sinfo_valid && H5FS__sinfo_unlock(f, fspace, false) < 0)
        HDONE_ERROR(H5E_FSPACE, H5E_CANTRELEASE, FAIL, "can't release section info");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS_sect_iterate() */

/*-------------------------------------------------------------------------
 * Function:    H5FS_sect_stats
 *
 * Purpose:     Retrieve info about the sections managed
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FS_sect_stats(const H5FS_t *fspace, hsize_t *tot_space, hsize_t *nsects)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check arguments. */
    assert(fspace);

    /* Get the stats desired */
    if (tot_space)
        *tot_space = fspace->tot_space;
    if (nsects)
        *nsects = fspace->tot_sect_count;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5FS_sect_stats() */

/*-------------------------------------------------------------------------
 * Function:    H5FS_sect_change_class
 *
 * Purpose:     Make appropriate adjustments to internal data structures when
 *              a section changes class
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FS_sect_change_class(H5F_t *f, H5FS_t *fspace, H5FS_section_info_t *sect, uint16_t new_class)
{
    const H5FS_section_class_t *old_cls;               /* Old class of section */
    const H5FS_section_class_t *new_cls;               /* New class of section */
    unsigned                    old_class;             /* Old class ID of section */
    bool                        sinfo_valid = false;   /* Whether the section info is valid */
    herr_t                      ret_value   = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Check arguments. */
    assert(fspace);
    assert(sect);
    assert(sect->type < fspace->nclasses);
    assert(new_class < fspace->nclasses);

    /* Get a pointer to the section info */
    if (H5FS__sinfo_lock(f, fspace, H5AC__NO_FLAGS_SET) < 0)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTGET, FAIL, "can't get section info");
    sinfo_valid = true;

    /* Get class info */
    old_class = sect->type;
    old_cls   = &fspace->sect_cls[sect->type];
    new_cls   = &fspace->sect_cls[new_class];

    /* Check if the section's class change will affect the # of serializable or ghost sections */
    if ((old_cls->flags & H5FS_CLS_GHOST_OBJ) != (new_cls->flags & H5FS_CLS_GHOST_OBJ)) {
        H5FS_node_t *fspace_node; /* Free list size node */
        unsigned     bin;         /* Bin to put the free space section in */
        bool         to_ghost;    /* Flag if the section is changing to a ghost section */

        /* Determine if this section is becoming a ghost or is becoming serializable */
        if (old_cls->flags & H5FS_CLS_GHOST_OBJ)
            to_ghost = false;
        else
            to_ghost = true;

        /* Sanity check */
        assert(fspace->sinfo->bins);

        /* Determine correct bin which holds items of at least the section's size */
        bin = H5VM_log2_gen(sect->size);
        assert(bin < fspace->sinfo->nbins);
        assert(fspace->sinfo->bins[bin].bin_list);

        /* Get space node for section's size */
        fspace_node = (H5FS_node_t *)H5SL_search(fspace->sinfo->bins[bin].bin_list, &sect->size);
        assert(fspace_node);

        /* Adjust serializable/ghost counts */
        if (to_ghost) {
            /* Adjust global section count totals */
            fspace->serial_sect_count--;
            fspace->ghost_sect_count++;

            /* Adjust bin's section count totals */
            fspace->sinfo->bins[bin].serial_sect_count--;
            fspace->sinfo->bins[bin].ghost_sect_count++;

            /* Adjust section size node's section count totals */
            fspace_node->serial_count--;
            fspace_node->ghost_count++;

            /* Check if we switched a section size node's status */
            if (fspace_node->serial_count == 0)
                fspace->sinfo->serial_size_count--;
            if (fspace_node->ghost_count == 1)
                fspace->sinfo->ghost_size_count++;
        } /* end if */
        else {
            /* Adjust global section count totals */
            fspace->serial_sect_count++;
            fspace->ghost_sect_count--;

            /* Adjust bin's section count totals */
            fspace->sinfo->bins[bin].serial_sect_count++;
            fspace->sinfo->bins[bin].ghost_sect_count--;

            /* Adjust section size node's section count totals */
            fspace_node->serial_count++;
            fspace_node->ghost_count--;

            /* Check if we switched a section size node's status */
            if (fspace_node->serial_count == 1)
                fspace->sinfo->serial_size_count++;
            if (fspace_node->ghost_count == 0)
                fspace->sinfo->ghost_size_count--;
        } /* end else */
    }     /* end if */

    /* Check if the section's class change will affect the mergeable list */
    if ((old_cls->flags & H5FS_CLS_SEPAR_OBJ) != (new_cls->flags & H5FS_CLS_SEPAR_OBJ)) {
        bool to_mergable; /* Flag if the section is changing to a mergeable section */

        /* Determine if this section is becoming mergeable or is becoming separate */
        if (old_cls->flags & H5FS_CLS_SEPAR_OBJ)
            to_mergable = true;
        else
            to_mergable = false;

        /* Add or remove section from merge list, as appropriate */
        if (to_mergable) {
            if (fspace->sinfo->merge_list == NULL)
                if (NULL == (fspace->sinfo->merge_list = H5SL_create(H5SL_TYPE_HADDR, NULL)))
                    HGOTO_ERROR(H5E_FSPACE, H5E_CANTCREATE, FAIL,
                                "can't create skip list for merging free space sections");
            if (H5SL_insert(fspace->sinfo->merge_list, sect, &sect->addr) < 0)
                HGOTO_ERROR(H5E_FSPACE, H5E_CANTINSERT, FAIL,
                            "can't insert free space node into merging skip list");
        } /* end if */
        else {
            H5FS_section_info_t *tmp_sect_node; /* Temporary section node */

            tmp_sect_node = (H5FS_section_info_t *)H5SL_remove(fspace->sinfo->merge_list, &sect->addr);
            if (tmp_sect_node == NULL || tmp_sect_node != sect)
                HGOTO_ERROR(H5E_FSPACE, H5E_NOTFOUND, FAIL, "can't find section node on size list");
        } /* end else */
    }     /* end if */

    /* Change the section's class */
    sect->type = new_class;

    /* Change the serialized size of sections */
    fspace->sinfo->serial_size -= fspace->sect_cls[old_class].serial_size;
    fspace->sinfo->serial_size += fspace->sect_cls[new_class].serial_size;

    /* Update current space used for free space sections */
    if (H5FS__sect_serialize_size(fspace) < 0)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTCOMPUTE, FAIL, "can't adjust free space section size on disk");

done:
    /* Release the section info */
    if (sinfo_valid && H5FS__sinfo_unlock(f, fspace, true) < 0)
        HDONE_ERROR(H5E_FSPACE, H5E_CANTRELEASE, FAIL, "can't release section info");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS_sect_change_class() */

#ifdef H5FS_DEBUG_ASSERT

/*-------------------------------------------------------------------------
 * Function:    H5FS__sect_assert
 *
 * Purpose:     Verify that the sections managed are mostly sane
 *
 * Return:      void
 *
 *-------------------------------------------------------------------------
 */
void
H5FS__sect_assert(const H5FS_t *fspace)
{
    hsize_t separate_obj; /* The number of separate objects managed */

    FUNC_ENTER_PACKAGE_NOERR

    /* Initialize state */
    separate_obj = 0;

    /* Check for bins to work on */
    if (fspace->sinfo->bins) {
        hsize_t  acc_tot_sect_count;    /* Accumulated total section count from bins */
        hsize_t  acc_serial_sect_count; /* Accumulated serializable section count from bins */
        hsize_t  acc_ghost_sect_count;  /* Accumulated ghost section count from bins */
        size_t   acc_tot_size_count;    /* Accumulated total section size count from bins */
        size_t   acc_serial_size_count; /* Accumulated serializable section size count from bins */
        size_t   acc_ghost_size_count;  /* Accumulated ghost section size count from bins */
        unsigned u;                     /* Local index variable */

        /* Walk through all sections in bins */
        acc_tot_sect_count    = 0;
        acc_serial_sect_count = 0;
        acc_ghost_sect_count  = 0;
        acc_tot_size_count    = 0;
        acc_serial_size_count = 0;
        acc_ghost_size_count  = 0;
        for (u = 0; u < fspace->sinfo->nbins; u++) {
            acc_tot_sect_count += fspace->sinfo->bins[u].tot_sect_count;
            acc_serial_sect_count += fspace->sinfo->bins[u].serial_sect_count;
            acc_ghost_sect_count += fspace->sinfo->bins[u].ghost_sect_count;
            if (fspace->sinfo->bins[u].bin_list) {
                H5SL_node_t *curr_size_node;   /* Current section size node in skip list */
                size_t       bin_serial_count; /* # of serializable sections in this bin */
                size_t       bin_ghost_count;  /* # of ghost sections in this bin */

                acc_tot_size_count += H5SL_count(fspace->sinfo->bins[u].bin_list);

                /* Walk through the sections in this bin */
                curr_size_node   = H5SL_first(fspace->sinfo->bins[u].bin_list);
                bin_serial_count = 0;
                bin_ghost_count  = 0;
                while (curr_size_node != NULL) {
                    H5FS_node_t *fspace_node;       /* Section size node */
                    H5SL_node_t *curr_sect_node;    /* Current section node in skip list */
                    size_t       size_serial_count; /* # of serializable sections of this size */
                    size_t       size_ghost_count;  /* # of ghost sections of this size */

                    /* Get section size node */
                    fspace_node = (H5FS_node_t *)H5SL_item(curr_size_node);

                    /* Check sections on list */
                    curr_sect_node    = H5SL_first(fspace_node->sect_list);
                    size_serial_count = 0;
                    size_ghost_count  = 0;
                    while (curr_sect_node != NULL) {
                        H5FS_section_class_t *cls;  /* Class of section */
                        H5FS_section_info_t  *sect; /* Section */

                        /* Get section node & it's class */
                        sect = (H5FS_section_info_t *)H5SL_item(curr_sect_node);
                        cls  = &fspace->sect_cls[sect->type];

                        /* Sanity check section */
                        assert(H5_addr_defined(sect->addr));
                        assert(fspace_node->sect_size == sect->size);
                        if (cls->valid)
                            (*cls->valid)(cls, sect);

                        /* Add to correct count */
                        if (cls->flags & H5FS_CLS_GHOST_OBJ)
                            size_ghost_count++;
                        else
                            size_serial_count++;

                        /* Count node, if separate */
                        if (cls->flags & H5FS_CLS_SEPAR_OBJ)
                            separate_obj++;

                        /* Get the next section node in the list */
                        curr_sect_node = H5SL_next(curr_sect_node);
                    } /* end while */

                    /* Check the number of serializable & ghost sections of this size */
                    assert(fspace_node->serial_count == size_serial_count);
                    assert(fspace_node->ghost_count == size_ghost_count);

                    /* Add to global count of serializable & ghost section sizes */
                    if (fspace_node->serial_count > 0)
                        acc_serial_size_count++;
                    if (fspace_node->ghost_count > 0)
                        acc_ghost_size_count++;

                    /* Add to bin's serializable & ghost counts */
                    bin_serial_count += size_serial_count;
                    bin_ghost_count += size_ghost_count;

                    /* Get the next section size node in the list */
                    curr_size_node = H5SL_next(curr_size_node);
                } /* end while */

                /* Check the number of serializable & ghost sections in this bin */
                assert(fspace->sinfo->bins[u].tot_sect_count == (bin_serial_count + bin_ghost_count));
                assert(fspace->sinfo->bins[u].serial_sect_count == bin_serial_count);
                assert(fspace->sinfo->bins[u].ghost_sect_count == bin_ghost_count);
            } /* end if */
        }     /* end for */

        /* Check counts from bins vs. global counts */
        assert(fspace->sinfo->tot_size_count == acc_tot_size_count);
        assert(fspace->sinfo->serial_size_count == acc_serial_size_count);
        assert(fspace->sinfo->ghost_size_count == acc_ghost_size_count);
        assert(fspace->tot_sect_count == acc_tot_sect_count);
        assert(fspace->serial_sect_count == acc_serial_sect_count);
        assert(fspace->ghost_sect_count == acc_ghost_sect_count);
    } /* end if */
    else {
        /* Check counts are zero */
        assert(fspace->tot_sect_count == 0);
        assert(fspace->serial_sect_count == 0);
        assert(fspace->ghost_sect_count == 0);
    } /* end else */

    /* Make certain that the number of sections on the address list is correct */
    if (fspace->sinfo->merge_list)
        assert(fspace->tot_sect_count == (separate_obj + H5SL_count(fspace->sinfo->merge_list)));

    FUNC_LEAVE_NOAPI_VOID
} /* end H5FS__sect_assert() */
#endif /* H5FS_DEBUG_ASSERT */

/*-------------------------------------------------------------------------
 * Function:    H5FS_sect_try_shrink_eoa
 *
 * Purpose:     To shrink the last section on the merge list if the section
 *              is at EOF.
 *
 * Return:      true/false/FAIL
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5FS_sect_try_shrink_eoa(H5F_t *f, H5FS_t *fspace, void *op_data)
{
    bool   sinfo_valid     = false; /* Whether the section info is valid */
    bool   section_removed = false; /* Whether a section was removed */
    htri_t ret_value       = false; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments. */
    assert(fspace);

    if (H5FS__sinfo_lock(f, fspace, H5AC__NO_FLAGS_SET) < 0)
        HGOTO_ERROR(H5E_FSPACE, H5E_CANTGET, FAIL, "can't get section info");
    sinfo_valid = true;

    if (fspace->sinfo && fspace->sinfo->merge_list) {
        H5SL_node_t *last_node; /* Last node in merge list */

        /* Check for last node in the merge list */
        if (NULL != (last_node = H5SL_last(fspace->sinfo->merge_list))) {
            H5FS_section_info_t  *tmp_sect;     /* Temporary free space section */
            H5FS_section_class_t *tmp_sect_cls; /* Temporary section's class */

            /* Get the pointer to the last section, from the last node */
            tmp_sect = (H5FS_section_info_t *)H5SL_item(last_node);
            assert(tmp_sect);
            tmp_sect_cls = &fspace->sect_cls[tmp_sect->type];
            if (tmp_sect_cls->can_shrink) {
                /* Check if the section can be shrunk away */
                if ((ret_value = (*tmp_sect_cls->can_shrink)(tmp_sect, op_data)) < 0)
                    HGOTO_ERROR(H5E_FSPACE, H5E_CANTSHRINK, FAIL, "can't check for shrinking container");
                if (ret_value > 0) {
                    assert(tmp_sect_cls->shrink);

                    /* Remove section from free space manager */
                    if (H5FS__sect_remove_real(fspace, tmp_sect) < 0)
                        HGOTO_ERROR(H5E_FSPACE, H5E_CANTRELEASE, FAIL,
                                    "can't remove section from internal data structures");
                    section_removed = true;

                    /* Shrink away section */
                    if ((*tmp_sect_cls->shrink)(&tmp_sect, op_data) < 0)
                        HGOTO_ERROR(H5E_FSPACE, H5E_CANTINSERT, FAIL, "can't shrink free space container");
                } /* end if */
            }     /* end if */
        }         /* end if */
    }             /* end if */

done:
    /* Release the section info */
    if (sinfo_valid && H5FS__sinfo_unlock(f, fspace, section_removed) < 0)
        HDONE_ERROR(H5E_FSPACE, H5E_CANTRELEASE, FAIL, "can't release section info");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS_sect_try_shrink_eoa() */

/*-------------------------------------------------------------------------
 * Function:    H5FS_vfd_alloc_hdr_and_section_info_if_needed
 *
 * Purpose:     The purpose of this function is to allocate file space for
 *              the header and section info of the target free space manager
 *              if they are not allocated yet.
 *
 *              The previous hack in this routine to avoid the potential infinite
 *              loops by allocating file space directly from the end of the file
 *              is removed.  The allocation can now be done via the usual
 *              file space allocation call H5MF_alloc().
 *
 *              The design flaw is addressed by not allowing the size
 *              of section info to shrink.  This means, when trying to allocate
 *              section info size X via H5MF_alloc() and the section info size
 *              after H5MF_alloc() changes to Y:
 *              --if Y is larger than X, free the just allocated file space X
 *                via H5MF_xfree() and set fspace->sect_size to Y.
 *                This routine will be called again later from
 *                H5MF_settle_meta_data_fsm() to allocate section info with the
 *                larger fpsace->sect_size Y.
 *              --if Y is smaller than X, no further allocation is needed and
 *                fspace->sect_size and fspace->alloc_sect_size are set to X.
 *                This means fspace->sect_size may be larger than what is
 *                actually needed.
 *
 *              This routine also re-inserts the header and section info in the
 *              metadata cache with this allocation.
 *
 * Return:      Success:        non-negative
 *              Failure:        negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FS_vfd_alloc_hdr_and_section_info_if_needed(H5F_t *f, H5FS_t *fspace, haddr_t *fs_addr_ptr)
{
    hsize_t hdr_alloc_size;
    hsize_t sinfo_alloc_size;
    haddr_t sect_addr = HADDR_UNDEF; /* address of sinfo */
    haddr_t eoa       = HADDR_UNDEF; /* Initial EOA for the file */
    herr_t  ret_value = SUCCEED;     /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    /* Check arguments. */
    assert(f);
    assert(f->shared);
    assert(f->shared->lf);
    assert(fspace);
    assert(fs_addr_ptr);

    /* the section info should be unlocked */
    assert(fspace->sinfo_lock_count == 0);

    /* persistent free space managers must be enabled */
    assert(f->shared->fs_persist);

    /* At present, all free space strategies enable the free space managers.
     * This will probably change -- at which point this assertion should
     * be revisited.
     */
    /* Updated: Only the following two strategies enable the free-space managers */
    assert((f->shared->fs_strategy == H5F_FSPACE_STRATEGY_FSM_AGGR) ||
           (f->shared->fs_strategy == H5F_FSPACE_STRATEGY_PAGE));

    if (fspace->serial_sect_count > 0 && fspace->sinfo) {
        /* the section info is floating, so space->sinfo should be defined */

        if (!H5_addr_defined(fspace->addr)) {

            /* start by allocating file space for the header */

            /* Get the EOA for the file -- need for sanity check below */
            if (HADDR_UNDEF == (eoa = H5F_get_eoa(f, H5FD_MEM_FSPACE_HDR)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGET, FAIL, "Unable to get eoa");

            /* check for overlap into temporary allocation space */
            if (H5F_IS_TMP_ADDR(f, (eoa + fspace->sect_size)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_BADRANGE, FAIL,
                            "hdr file space alloc will overlap into 'temporary' file space");

            hdr_alloc_size = H5FS_HEADER_SIZE(f);

            if (H5F_PAGED_AGGR(f))
                assert(0 == (eoa % f->shared->fs_page_size));

            /* Allocate space for the free space header */
            if (HADDR_UNDEF == (fspace->addr = H5MF_alloc(f, H5FD_MEM_FSPACE_HDR, hdr_alloc_size)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "file allocation failed for free space header");

            /* Cache the new free space header (pinned) */
            if (H5AC_insert_entry(f, H5AC_FSPACE_HDR, fspace->addr, fspace, H5AC__PIN_ENTRY_FLAG) < 0)
                HGOTO_ERROR(H5E_FSPACE, H5E_CANTINIT, FAIL, "can't add free space header to cache");

            *fs_addr_ptr = fspace->addr;
        }

        if (!H5_addr_defined(fspace->sect_addr)) {

            /* now allocate file space for the section info */

            /* Get the EOA for the file -- need for sanity check below */
            if (HADDR_UNDEF == (eoa = H5F_get_eoa(f, H5FD_MEM_FSPACE_SINFO)))
                HGOTO_ERROR(H5E_FSPACE, H5E_CANTGET, FAIL, "Unable to get eoa");

            /* check for overlap into temporary allocation space */
            if (H5F_IS_TMP_ADDR(f, (eoa + fspace->sect_size)))
                HGOTO_ERROR(H5E_FSPACE, H5E_BADRANGE, FAIL,
                            "sinfo file space alloc will overlap into 'temporary' file space");

            sinfo_alloc_size = fspace->sect_size;

            if (H5F_PAGED_AGGR(f))
                assert(0 == (eoa % f->shared->fs_page_size));

            /* allocate space for the section info */
            if (HADDR_UNDEF == (sect_addr = H5MF_alloc(f, H5FD_MEM_FSPACE_SINFO, sinfo_alloc_size)))
                HGOTO_ERROR(H5E_FSPACE, H5E_NOSPACE, FAIL, "file allocation failed for section info");

            /* update fspace->alloc_sect_size and fspace->sect_addr to reflect
             * the allocation
             */
            if (fspace->sect_size > sinfo_alloc_size) {
                hsize_t saved_sect_size = fspace->sect_size;

                if (H5MF_xfree(f, H5FD_MEM_FSPACE_SINFO, sect_addr, sinfo_alloc_size) < 0)
                    HGOTO_ERROR(H5E_FSPACE, H5E_CANTFREE, FAIL, "unable to free free space sections");
                fspace->sect_size = saved_sect_size;
            }
            else {
                fspace->alloc_sect_size = sinfo_alloc_size;
                fspace->sect_size       = sinfo_alloc_size;
                fspace->sect_addr       = sect_addr;

                /* insert the new section info into the metadata cache.  */

                /* Question: Do we need to worry about this insertion causing an
                 * eviction from the metadata cache?  Think on this.  If so, add a
                 * flag to H5AC_insert() to force it to skip the call to make space in
                 * cache.
                 *
                 * On reflection, no.
                 *
                 * On a regular file close, any eviction will not change the
                 * the contents of the free space manager(s), as all entries
                 * should have correct file space allocated by the time this
                 * function is called.
                 *
                 * In the cache image case, the selection of entries for inclusion
                 * in the cache image will not take place until after this call.
                 * (Recall that this call is made during the metadata fsm settle
                 * routine, which is called during the serialization routine in
                 * the cache image case.  Entries are not selected for inclusion
                 * in the image until after the cache is serialized.)
                 *
                 *                                        JRM -- 11/4/16
                 */
                if (H5AC_insert_entry(f, H5AC_FSPACE_SINFO, sect_addr, fspace->sinfo, H5AC__NO_FLAGS_SET) < 0)
                    HGOTO_ERROR(H5E_FSPACE, H5E_CANTINIT, FAIL, "can't add free space sinfo to cache");

                /* We have changed the sinfo address -- Mark free space header dirty */
                if (H5AC_mark_entry_dirty(fspace) < 0)
                    HGOTO_ERROR(H5E_FSPACE, H5E_CANTMARKDIRTY, FAIL,
                                "unable to mark free space header as dirty");

                /* since space has been allocated for the section info and the sinfo
                 * has been inserted into the cache, relinquish ownership (i.e. float)
                 * the section info.
                 */
                fspace->sinfo = NULL;
            }
        }
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FS_vfd_alloc_hdr_and_section_info_if_needed() */
