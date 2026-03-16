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
 * Purpose:	Routines for aggregating free space allocations
 */

/****************/
/* Module Setup */
/****************/

#define H5F_FRIEND      /*suppress error about including H5Fpkg	  */
#include "H5MFmodule.h" /* This source code file is part of the H5MF module */

/***********/
/* Headers */
/***********/
#include "H5private.h"  /* Generic Functions			*/
#include "H5Eprivate.h" /* Error handling		  	*/
#include "H5Fpkg.h"     /* File access				*/
#include "H5MFpkg.h"    /* File memory management		*/

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/
#define EXTEND_THRESHOLD 0.10F

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/
static herr_t  H5MF__aggr_free(H5F_t *f, H5FD_mem_t type, H5F_blk_aggr_t *aggr);
static haddr_t H5MF__aggr_alloc(H5F_t *f, H5F_blk_aggr_t *aggr, H5F_blk_aggr_t *other_aggr, H5FD_mem_t type,
                                hsize_t size);
static herr_t  H5MF__aggr_reset(H5F_t *f, H5F_blk_aggr_t *aggr);
static htri_t  H5MF__aggr_can_shrink_eoa(H5F_t *f, H5FD_mem_t type, H5F_blk_aggr_t *aggr);

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:    H5MF_aggr_vfd_alloc
 *
 * Purpose:     Allocate SIZE bytes of file memory via H5MF__aggr_alloc()
 *		and return the relative address where that contiguous chunk
 *		of file memory exists.
 *		The TYPE argument describes the purpose for which the storage
 *		is being requested.
 *
 * Return:      Success:        The file address of new chunk.
 *              Failure:        HADDR_UNDEF
 *
 *-------------------------------------------------------------------------
 */
haddr_t
H5MF_aggr_vfd_alloc(H5F_t *f, H5FD_mem_t alloc_type, hsize_t size)
{
    haddr_t ret_value = HADDR_UNDEF; /* Return value */

    FUNC_ENTER_NOAPI(HADDR_UNDEF)
#ifdef H5MF_AGGR_DEBUG
    fprintf(stderr, "%s: alloc_type = %u, size = %" PRIuHSIZE "\n", __func__, (unsigned)alloc_type, size);
#endif /* H5MF_AGGR_DEBUG */

    /* check arguments */
    assert(f);
    assert(f->shared);
    assert(f->shared->lf);
    assert(size > 0);

    /* Couldn't find anything from the free space manager, go allocate some */
    if (alloc_type != H5FD_MEM_DRAW && alloc_type != H5FD_MEM_GHEAP) {
        /* Handle metadata differently from "raw" data */
        if (HADDR_UNDEF == (ret_value = H5MF__aggr_alloc(f, &(f->shared->meta_aggr), &(f->shared->sdata_aggr),
                                                         alloc_type, size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, HADDR_UNDEF, "can't allocate metadata");
    } /* end if */
    else {
        /* Allocate "raw" data: H5FD_MEM_DRAW and H5FD_MEM_GHEAP */
        if (HADDR_UNDEF == (ret_value = H5MF__aggr_alloc(f, &(f->shared->sdata_aggr), &(f->shared->meta_aggr),
                                                         H5FD_MEM_DRAW, size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, HADDR_UNDEF, "can't allocate raw data");
    } /* end else */

    /* Sanity check for overlapping into file's temporary allocation space */
    assert(H5_addr_le((ret_value + size), f->shared->tmp_addr));

done:
#ifdef H5MF_AGGR_DEBUG
    fprintf(stderr, "%s: Leaving: ret_value = %" PRIuHADDR ", size = %" PRIuHSIZE "\n", __func__, ret_value,
            size);
#endif /* H5MF_AGGR_DEBUG */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5MF_aggr_vfd_alloc() */

/*-------------------------------------------------------------------------
 * Function:    H5MF__aggr_alloc
 *
 * Purpose:     Try to allocate SIZE bytes of memory from an aggregator
 *              block if possible.
 *
 * Return:      Success:    The format address of the new file memory.
 *              Failure:    The undefined address HADDR_UNDEF
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5MF__aggr_alloc(H5F_t *f, H5F_blk_aggr_t *aggr, H5F_blk_aggr_t *other_aggr, H5FD_mem_t type, hsize_t size)
{
    haddr_t eoa_frag_addr = HADDR_UNDEF; /* Address of fragment at EOA */
    hsize_t eoa_frag_size = 0;           /* Size of fragment at EOA */
    haddr_t eoa           = HADDR_UNDEF; /* Initial EOA for the file */
    haddr_t ret_value     = HADDR_UNDEF; /* Return value */

    FUNC_ENTER_PACKAGE
#ifdef H5MF_AGGR_DEBUG
    fprintf(stderr, "%s: type = %u, size = %" PRIuHSIZE "\n", __func__, (unsigned)type, size);
#endif /* H5MF_AGGR_DEBUG */

    /* check args */
    assert(f);
    assert(aggr);
    assert(aggr->feature_flag == H5FD_FEAT_AGGREGATE_METADATA ||
           aggr->feature_flag == H5FD_FEAT_AGGREGATE_SMALLDATA);
    assert(other_aggr);
    assert(other_aggr->feature_flag == H5FD_FEAT_AGGREGATE_METADATA ||
           other_aggr->feature_flag == H5FD_FEAT_AGGREGATE_SMALLDATA);
    assert(other_aggr->feature_flag != aggr->feature_flag);
    assert(type >= H5FD_MEM_DEFAULT && type < H5FD_MEM_NTYPES);
    assert(size > 0);

    /* Get the EOA for the file */
    if (HADDR_UNDEF == (eoa = H5F_get_eoa(f, type)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGET, HADDR_UNDEF, "Unable to get eoa");

    /*
     * If the aggregation feature is enabled for this file and strategy is not H5F_FILE_SPACE_NONE,
     * allocate "generic" space and sub-allocate out of that, if possible.
     * Otherwise just allocate through H5F__alloc().
     */

    /*
     * Replace the following line with the line in #ifdef REPLACE/#endif.
     * The line in #ifdef REPLACE triggers the following problem:
     *   test/objcopy.c: test_copy_group_deep() test fails with the family driver
     *
     * When closing the destination file after H5Ocopy, the library flushes the fractal
     * heap direct block via H5HF__cache_dblock_pre_serialize().  While doing so,
     * the cache eventually adjusts/evicts ageout entries and ends up flushing out the
     * same entry that is being serialized (flush_in_progress).
     */
    if ((f->shared->feature_flags & aggr->feature_flag) &&
        f->shared->fs_strategy != H5F_FSPACE_STRATEGY_NONE &&
        (!f->shared->closing || !f->shared->fs_persist)) {
#ifdef REPLACE
        if ((f->shared->feature_flags & aggr->feature_flag) &&
            f->shared->fs_strategy != H5F_FSPACE_STRATEGY_NONE && !f->shared->closing) {
#endif
            haddr_t    aggr_frag_addr = HADDR_UNDEF; /* Address of aggregator fragment */
            hsize_t    aggr_frag_size = 0;           /* Size of aggregator fragment */
            hsize_t    alignment;                    /* Alignment of this section */
            hsize_t    aggr_mis_align = 0;           /* Misalignment of aggregator */
            H5FD_mem_t alloc_type, other_alloc_type; /* Current aggregator & 'other' aggregator types */

#ifdef H5MF_AGGR_DEBUG
            fprintf(stderr, "%s: aggr = {%" PRIuHADDR ", %" PRIuHSIZE ", %" PRIuHSIZE "}\n", __func__,
                    aggr->addr, aggr->tot_size, aggr->size);
#endif /* H5MF_AGGR_DEBUG */

            /* Turn off alignment if allocation < threshold */
            alignment = H5F_ALIGNMENT(f);
            if (!((alignment > 1) && (size >= H5F_THRESHOLD(f))))
                alignment = 0; /* no alignment */

            /* Generate fragment if aggregator is mis-aligned */
            if (alignment && H5_addr_gt(aggr->addr, 0) &&
                (aggr_mis_align = (aggr->addr + H5F_BASE_ADDR(f)) % alignment)) {
                aggr_frag_addr = aggr->addr;
                aggr_frag_size = alignment - aggr_mis_align;
            } /* end if */

            alloc_type =
                aggr->feature_flag == H5FD_FEAT_AGGREGATE_METADATA ? H5FD_MEM_DEFAULT : H5FD_MEM_DRAW;
            other_alloc_type =
                other_aggr->feature_flag == H5FD_FEAT_AGGREGATE_METADATA ? H5FD_MEM_DEFAULT : H5FD_MEM_DRAW;

            /* Check if the space requested is larger than the space left in the block */
            if ((size + aggr_frag_size) > aggr->size) {
                htri_t extended = false; /* Whether the file was extended */

                /* Check if the block asked for is too large for 'normal' aggregator block */
                if (size >= aggr->alloc_size) {
                    hsize_t ext_size = size + aggr_frag_size;

                    /* Check for overlapping into file's temporary allocation space */
                    if (H5_addr_gt((aggr->addr + aggr->size + ext_size), f->shared->tmp_addr))
                        HGOTO_ERROR(H5E_RESOURCE, H5E_BADRANGE, HADDR_UNDEF,
                                    "'normal' file space allocation request will overlap into 'temporary' "
                                    "file space");

                    if ((aggr->addr > 0) &&
                        (extended = H5F__try_extend(f, alloc_type, (aggr->addr + aggr->size), ext_size)) < 0)
                        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, HADDR_UNDEF, "can't extending space");
                    else if (extended) {
                        /* aggr->size is unchanged */
                        ret_value = aggr->addr + aggr_frag_size;
                        aggr->addr += ext_size;
                        aggr->tot_size += ext_size;
                    }
                    else {
                        /* Release "other" aggregator, if it exists, is at the end of the allocated space,
                         * has allocated more than one block and the unallocated space is greater than its
                         * allocation block size.
                         */
                        if ((other_aggr->size > 0) &&
                            (H5_addr_eq((other_aggr->addr + other_aggr->size), eoa)) &&
                            (other_aggr->tot_size > other_aggr->size) &&
                            ((other_aggr->tot_size - other_aggr->size) >= other_aggr->alloc_size)) {
                            if (H5MF__aggr_free(f, other_alloc_type, other_aggr) < 0)
                                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTFREE, HADDR_UNDEF,
                                            "can't free aggregation block");
                        } /* end if */

                        /* Allocate space from the VFD (i.e. at the end of the file) */
                        if (HADDR_UNDEF ==
                            (ret_value = H5F__alloc(f, alloc_type, size, &eoa_frag_addr, &eoa_frag_size)))
                            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, HADDR_UNDEF,
                                        "can't allocate file space");
                    } /* end else */
                }     /* end if */
                else {
                    hsize_t ext_size = aggr->alloc_size;

                    /* Allocate another block */
#ifdef H5MF_AGGR_DEBUG
                    fprintf(stderr, "%s: Allocating block\n", __func__);
#endif /* H5MF_AGGR_DEBUG */

                    if (aggr_frag_size > (ext_size - size))
                        ext_size += (aggr_frag_size - (ext_size - size));

                    /* Check for overlapping into file's temporary allocation space */
                    if (H5_addr_gt((aggr->addr + aggr->size + ext_size), f->shared->tmp_addr))
                        HGOTO_ERROR(H5E_RESOURCE, H5E_BADRANGE, HADDR_UNDEF,
                                    "'normal' file space allocation request will overlap into 'temporary' "
                                    "file space");

                    if ((aggr->addr > 0) &&
                        (extended = H5F__try_extend(f, alloc_type, (aggr->addr + aggr->size), ext_size)) < 0)
                        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, HADDR_UNDEF, "can't extending space");
                    else if (extended) {
                        aggr->addr += aggr_frag_size;
                        aggr->size += (ext_size - aggr_frag_size);
                        aggr->tot_size += ext_size;
                    } /* end else-if */
                    else {
                        haddr_t new_space; /* Address of new space allocated */

                        /* Release "other" aggregator, if it exists, is at the end of the allocated space,
                         * has allocated more than one block and the unallocated space is greater than its
                         * allocation block size.
                         */
                        if ((other_aggr->size > 0) &&
                            (H5_addr_eq((other_aggr->addr + other_aggr->size), eoa)) &&
                            (other_aggr->tot_size > other_aggr->size) &&
                            ((other_aggr->tot_size - other_aggr->size) >= other_aggr->alloc_size)) {
                            if (H5MF__aggr_free(f, other_alloc_type, other_aggr) < 0)
                                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTFREE, HADDR_UNDEF,
                                            "can't free aggregation block");
                        } /* end if */

                        /* Allocate space from the VFD (i.e. at the end of the file) */
                        if (HADDR_UNDEF == (new_space = H5F__alloc(f, alloc_type, aggr->alloc_size,
                                                                   &eoa_frag_addr, &eoa_frag_size)))
                            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, HADDR_UNDEF,
                                        "can't allocate file space");

                        /* Return the unused portion of the block to a free list */
                        if (aggr->size > 0)
                            if (H5MF_xfree(f, alloc_type, aggr->addr, aggr->size) < 0)
                                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTFREE, HADDR_UNDEF,
                                            "can't free aggregation block");

                        /* If the block is not to be aligned, fold the eoa fragment
                         * into the newly allocated aggregator, as it could have
                         * been allocated in an aligned manner if the aggregator
                         * block is larger than the threshold */
                        if (eoa_frag_size && !alignment) {
                            assert(eoa_frag_addr + eoa_frag_size == new_space);
                            aggr->addr     = eoa_frag_addr;
                            aggr->size     = aggr->alloc_size + eoa_frag_size;
                            aggr->tot_size = aggr->size;

                            /* Reset EOA fragment */
                            eoa_frag_addr = HADDR_UNDEF;
                            eoa_frag_size = 0;
                        } /* end if */
                        else {
                            /* Point the aggregator at the newly allocated block */
                            aggr->addr     = new_space;
                            aggr->size     = aggr->alloc_size;
                            aggr->tot_size = aggr->alloc_size;
                        } /* end else */
                    }     /* end else */

                    /* Allocate space out of the metadata block */
                    ret_value = aggr->addr;
                    aggr->size -= size;
                    aggr->addr += size;
                } /* end else */

                /* Freeing any possible fragment due to file allocation */
                if (eoa_frag_size)
                    if (H5MF_xfree(f, alloc_type, eoa_frag_addr, eoa_frag_size) < 0)
                        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTFREE, HADDR_UNDEF, "can't free eoa fragment");

                /* Freeing any possible fragment due to alignment in the block after extension */
                if (extended && aggr_frag_size)
                    if (H5MF_xfree(f, alloc_type, aggr_frag_addr, aggr_frag_size) < 0)
                        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTFREE, HADDR_UNDEF,
                                    "can't free aggregation fragment");
            } /* end if */
            else {
                /* Allocate space out of the block */
                ret_value = aggr->addr + aggr_frag_size;
                aggr->size -= (size + aggr_frag_size);
                aggr->addr += (size + aggr_frag_size);

                /* free any possible fragment */
                if (aggr_frag_size)
                    if (H5MF_xfree(f, alloc_type, aggr_frag_addr, aggr_frag_size) < 0)
                        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTFREE, HADDR_UNDEF,
                                    "can't free aggregation fragment");
            } /* end else */
        }     /* end if */
        else {
            /* Allocate data from the file */
            if (HADDR_UNDEF == (ret_value = H5F__alloc(f, type, size, &eoa_frag_addr, &eoa_frag_size)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, HADDR_UNDEF, "can't allocate file space");

            /* Check if fragment was generated */
            if (eoa_frag_size)
                /* Put fragment on the free list */
                if (H5MF_xfree(f, type, eoa_frag_addr, eoa_frag_size) < 0)
                    HGOTO_ERROR(H5E_RESOURCE, H5E_CANTFREE, HADDR_UNDEF, "can't free eoa fragment");
        } /* end else */

        /* Sanity check for overlapping into file's temporary allocation space */
        assert(H5_addr_le((ret_value + size), f->shared->tmp_addr));

        /* Post-condition sanity check */
        if (H5F_ALIGNMENT(f) && size >= H5F_THRESHOLD(f))
            assert(!((ret_value + H5FD_get_base_addr(f->shared->lf)) % H5F_ALIGNMENT(f)));

done:
#ifdef H5MF_AGGR_DEBUG
        fprintf(stderr, "%s: ret_value = %" PRIuHADDR "\n", __func__, ret_value);
#endif /* H5MF_AGGR_DEBUG */
        FUNC_LEAVE_NOAPI(ret_value)
    } /* end H5MF__aggr_alloc() */

    /*-------------------------------------------------------------------------
     * Function:    H5MF__aggr_try_extend
     *
     * Purpose:	Check if a block is inside an aggregator block and extend it
     *              if possible.
     *
     * Note:
     *	        When the block to be extended adjoins the aggregator--
     *		    1) When the aggregator is at end of file:
     *		       A) If the request is below the threshold, extend the block into the aggregator
     *		       B) If the request is above the threshold,
     *			    a) extend the aggregator by aggr->alloc_size or the extended amount
     *			    b) extend the block into the aggregator
     *		    2) When the aggregator is not at end of file:
     *		       Extended the block into the aggregator if it has enough space to satisfy the request
     *
     * Return:	Success:	true(1)  - Block was extended
     *                              false(0) - Block could not be extended
     * 		Failure:	FAIL
     *
     *-------------------------------------------------------------------------
     */
    htri_t H5MF__aggr_try_extend(H5F_t * f, H5F_blk_aggr_t * aggr, H5FD_mem_t type, haddr_t blk_end,
                                 hsize_t extra_requested)
    {
        htri_t ret_value = false; /* Return value */

        FUNC_ENTER_PACKAGE

        /* Check args */
        assert(f);
        assert(aggr);
        assert(aggr->feature_flag == H5FD_FEAT_AGGREGATE_METADATA ||
               aggr->feature_flag == H5FD_FEAT_AGGREGATE_SMALLDATA);

        /* Check if this aggregator is active */
        if (f->shared->feature_flags & aggr->feature_flag) {
            /*
             * If the block being tested adjoins the beginning of the aggregator
             *      block, check if the aggregator can accommodate the extension.
             */
            if (H5_addr_eq(blk_end, aggr->addr)) {
                haddr_t eoa; /* EOA for the file */

                /* Get the EOA for the file */
                if (HADDR_UNDEF == (eoa = H5F_get_eoa(f, type)))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGET, FAIL, "Unable to get eoa");

                /* If the aggregator is at the end of file: */
                if (H5_addr_eq(eoa, aggr->addr + aggr->size)) {
                    /* If extra_requested is below percentage threshold, extend block into the aggregator. */
                    if (extra_requested <= (hsize_t)(EXTEND_THRESHOLD * (float)aggr->size)) {
                        aggr->size -= extra_requested;
                        aggr->addr += extra_requested;

                        /* Indicate success */
                        HGOTO_DONE(true);
                    } /* end if */
                    /*
                     * If extra_requested is above percentage threshold:
                     * 1) "bubble" up the aggregator by aggr->alloc_size or extra_requested
                     * 2) extend the block into the aggregator
                     */
                    else {
                        hsize_t extra =
                            (extra_requested < aggr->alloc_size) ? aggr->alloc_size : extra_requested;

                        if ((ret_value = H5F__try_extend(f, type, (aggr->addr + aggr->size), extra)) < 0)
                            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTEXTEND, FAIL, "error extending file");
                        else if (ret_value == true) {
                            /* Shift the aggregator block by the extra requested */
                            /* (allocates the space for the extra_requested) */
                            aggr->addr += extra_requested;

                            /* Add extra to the aggregator's total allocated amount */
                            aggr->tot_size += extra;

                            /* Account for any space added to the aggregator */
                            /* (either 0 (if extra_requested > aggr->alloc_size) or
                             *      (aggr->alloc_size - extra_requested) -QAK
                             */
                            aggr->size += extra;
                            aggr->size -= extra_requested;
                        } /* end else-if */
                    }     /* end else */
                }         /* end if */
                else {
                    /* The aggregator is not at end of file */
                    /* Check if aggregator has enough internal space to satisfy the extension. */
                    if (aggr->size >= extra_requested) {
                        /* Extend block into aggregator */
                        aggr->size -= extra_requested;
                        aggr->addr += extra_requested;

                        /* Indicate success */
                        HGOTO_DONE(true);
                    } /* end if */
                }     /* end else */
            }         /* end if */
        }             /* end if */

done:
        FUNC_LEAVE_NOAPI(ret_value)
    } /* end H5MF__aggr_try_extend() */

    /*-------------------------------------------------------------------------
     * Function:    H5MF__aggr_can_absorb
     *
     * Purpose:	Check if a section adjoins an aggregator block and one can
     *              absorb the other.
     *
     * Return:	Success:	true(1)  - Section or aggregator can be absorbed
     *                              false(0) - Section and aggregator can not be absorbed
     * 		Failure:	FAIL
     *
     *-------------------------------------------------------------------------
     */
    htri_t H5MF__aggr_can_absorb(const H5F_t *f, const H5F_blk_aggr_t *aggr, const H5MF_free_section_t *sect,
                                 H5MF_shrink_type_t *shrink)
    {
        htri_t ret_value = false; /* Return value */

        FUNC_ENTER_PACKAGE_NOERR

        /* Check args */
        assert(f);
        assert(aggr);
        assert(aggr->feature_flag == H5FD_FEAT_AGGREGATE_METADATA ||
               aggr->feature_flag == H5FD_FEAT_AGGREGATE_SMALLDATA);
        assert(sect);
        assert(shrink);

        /* Check if this aggregator is active */
        if (f->shared->feature_flags & aggr->feature_flag) {
            /* Check if the block adjoins the beginning or end of the aggregator */
            if (H5_addr_eq((sect->sect_info.addr + sect->sect_info.size), aggr->addr) ||
                H5_addr_eq((aggr->addr + aggr->size), sect->sect_info.addr)) {
#ifdef H5MF_AGGR_DEBUG
                fprintf(stderr,
                        "%s: section {%" PRIuHADDR ", %" PRIuHSIZE "} adjoins aggr = {%" PRIuHADDR
                        ", %" PRIuHSIZE "}\n",
                        "H5MF__aggr_can_absorb", sect->sect_info.addr, sect->sect_info.size, aggr->addr,
                        aggr->size);
#endif /* H5MF_AGGR_DEBUG */
                /* Check if aggregator would get too large and should be absorbed into section */
                if ((aggr->size + sect->sect_info.size) >= aggr->alloc_size)
                    *shrink = H5MF_SHRINK_SECT_ABSORB_AGGR;
                else
                    *shrink = H5MF_SHRINK_AGGR_ABSORB_SECT;

                /* Indicate success */
                HGOTO_DONE(true);
            } /* end if */
        }     /* end if */

done:
        FUNC_LEAVE_NOAPI(ret_value)
    } /* end H5MF__aggr_can_absorb() */

    /*-------------------------------------------------------------------------
     * Function:    H5MF__aggr_absorb
     *
     * Purpose:	Absorb a free space section into an aggregator block or
     *              vice versa.
     *
     * Return:      Success:        Non-negative
     *              Failure:        Negative
     *
     *-------------------------------------------------------------------------
     */
    herr_t H5MF__aggr_absorb(const H5F_t H5_ATTR_UNUSED *f, H5F_blk_aggr_t *aggr, H5MF_free_section_t *sect,
                             bool allow_sect_absorb)
    {
        FUNC_ENTER_PACKAGE_NOERR

        /* Check args */
        assert(f);
        assert(aggr);
        assert(aggr->feature_flag == H5FD_FEAT_AGGREGATE_METADATA ||
               aggr->feature_flag == H5FD_FEAT_AGGREGATE_SMALLDATA);
        assert(f->shared->feature_flags & aggr->feature_flag);
        assert(sect);

        /* Check if aggregator would get too large and should be absorbed into section */
        if ((aggr->size + sect->sect_info.size) >= aggr->alloc_size && allow_sect_absorb) {
            /* Check if the section adjoins the beginning or end of the aggregator */
            if (H5_addr_eq((sect->sect_info.addr + sect->sect_info.size), aggr->addr)) {
#ifdef H5MF_AGGR_DEBUG
                fprintf(stderr,
                        "%s: aggr {%" PRIuHADDR ", %" PRIuHSIZE "} adjoins front of section = {%" PRIuHADDR
                        ", %" PRIuHSIZE "}\n",
                        "H5MF__aggr_absorb", aggr->addr, aggr->size, sect->sect_info.addr,
                        sect->sect_info.size);
#endif /* H5MF_AGGR_DEBUG */
                /* Absorb aggregator onto end of section */
                sect->sect_info.size += aggr->size;
            } /* end if */
            else {
                /* Sanity check */
                assert(H5_addr_eq((aggr->addr + aggr->size), sect->sect_info.addr));

#ifdef H5MF_AGGR_DEBUG
                fprintf(stderr,
                        "%s: aggr {%" PRIuHADDR ", %" PRIuHSIZE "} adjoins end of section = {%" PRIuHADDR
                        ", %" PRIuHSIZE "}\n",
                        "H5MF__aggr_absorb", aggr->addr, aggr->size, sect->sect_info.addr,
                        sect->sect_info.size);
#endif /* H5MF_AGGR_DEBUG */
                /* Absorb aggregator onto beginning of section */
                sect->sect_info.addr -= aggr->size;
                sect->sect_info.size += aggr->size;
            } /* end if */

            /* Reset aggregator */
            aggr->tot_size = 0;
            aggr->addr     = 0;
            aggr->size     = 0;
        } /* end if */
        else {
            /* Check if the section adjoins the beginning or end of the aggregator */
            if (H5_addr_eq((sect->sect_info.addr + sect->sect_info.size), aggr->addr)) {
#ifdef H5MF_AGGR_DEBUG
                fprintf(stderr,
                        "%s: section {%" PRIuHADDR ", %" PRIuHSIZE "} adjoins front of aggr = {%" PRIuHADDR
                        ", %" PRIuHSIZE "}\n",
                        "H5MF__aggr_absorb", sect->sect_info.addr, sect->sect_info.size, aggr->addr,
                        aggr->size);
#endif /* H5MF_AGGR_DEBUG */
                /* Absorb section onto front of aggregator */
                aggr->addr -= sect->sect_info.size;
                aggr->size += sect->sect_info.size;

                /* Sections absorbed onto front of aggregator count against the total
                 * amount of space aggregated together.
                 */
                aggr->tot_size -= MIN(aggr->tot_size, sect->sect_info.size);
            } /* end if */
            else {
                /* Sanity check */
                assert(H5_addr_eq((aggr->addr + aggr->size), sect->sect_info.addr));

#ifdef H5MF_AGGR_DEBUG
                fprintf(stderr,
                        "%s: section {%" PRIuHADDR ", %" PRIuHSIZE "} adjoins end of aggr = {%" PRIuHADDR
                        ", %" PRIuHSIZE "}\n",
                        "H5MF__aggr_absorb", sect->sect_info.addr, sect->sect_info.size, aggr->addr,
                        aggr->size);
#endif /* H5MF_AGGR_DEBUG */
                /* Absorb section onto end of aggregator */
                aggr->size += sect->sect_info.size;
            } /* end if */
            /* Sanity check */
            assert(!allow_sect_absorb || (aggr->size < aggr->alloc_size));
        } /* end else */

        FUNC_LEAVE_NOAPI(SUCCEED)
    } /* end H5MF__aggr_absorb() */

    /*-------------------------------------------------------------------------
     * Function:    H5MF__aggr_query
     *
     * Purpose:     Query a block aggregator's current address & size info
     *
     * Return:      Success:        Non-negative
     *              Failure:        Negative
     *
     *-------------------------------------------------------------------------
     */
    herr_t H5MF__aggr_query(const H5F_t *f, const H5F_blk_aggr_t *aggr, haddr_t *addr, hsize_t *size)
    {
        FUNC_ENTER_PACKAGE_NOERR

        /* Check args */
        assert(f);
        assert(aggr);
        assert(aggr->feature_flag == H5FD_FEAT_AGGREGATE_METADATA ||
               aggr->feature_flag == H5FD_FEAT_AGGREGATE_SMALLDATA);

        /* Check if this aggregator is active */
        if (f->shared->feature_flags & aggr->feature_flag) {
            if (addr)
                *addr = aggr->addr;
            if (size)
                *size = aggr->size;
        } /* end if */

        FUNC_LEAVE_NOAPI(SUCCEED)
    } /* end H5MF__aggr_query() */

    /*-------------------------------------------------------------------------
     * Function:    H5MF__aggr_reset
     *
     * Purpose:     Reset a block aggregator, returning any space back to file
     *
     * Return:      Success:        Non-negative
     *              Failure:        Negative
     *
     *-------------------------------------------------------------------------
     */
    static herr_t H5MF__aggr_reset(H5F_t * f, H5F_blk_aggr_t * aggr)
    {
        H5FD_mem_t alloc_type;          /* Type of file memory to work with */
        herr_t     ret_value = SUCCEED; /* Return value */

        FUNC_ENTER_PACKAGE

        /* Check args */
        assert(f);
        assert(aggr);
        assert(aggr->feature_flag == H5FD_FEAT_AGGREGATE_METADATA ||
               aggr->feature_flag == H5FD_FEAT_AGGREGATE_SMALLDATA);

        /* Set the type of memory in the file */
        alloc_type = (aggr->feature_flag == H5FD_FEAT_AGGREGATE_METADATA
                          ? H5FD_MEM_DEFAULT
                          : H5FD_MEM_DRAW); /* Type of file memory to work with */

        /* Check if this aggregator is active */
        if (f->shared->feature_flags & aggr->feature_flag) {
            haddr_t tmp_addr; /* Temporary holder for aggregator address */
            hsize_t tmp_size; /* Temporary holder for aggregator size */

            /* Retain aggregator info */
            tmp_addr = aggr->addr;
            tmp_size = aggr->size;
#ifdef H5MF_AGGR_DEBUG
            fprintf(stderr, "%s: tmp_addr = %" PRIuHADDR ", tmp_size = %" PRIuHSIZE "\n", __func__, tmp_addr,
                    tmp_size);
#endif /* H5MF_AGGR_DEBUG */

            /* Reset aggregator block information */
            aggr->tot_size = 0;
            aggr->addr     = 0;
            aggr->size     = 0;

            /* Return the unused portion of the metadata block to the file */
            if (tmp_size > 0 && (H5F_INTENT(f) & H5F_ACC_RDWR))
                if (H5MF_xfree(f, alloc_type, tmp_addr, tmp_size) < 0)
                    HGOTO_ERROR(H5E_RESOURCE, H5E_CANTFREE, FAIL, "can't release aggregator's free space");
        } /* end if */

done:
        FUNC_LEAVE_NOAPI(ret_value)
    } /* end H5MF__aggr_reset() */

    /*-------------------------------------------------------------------------
     * Function:    H5MF_free_aggrs
     *
     * Purpose:     Reset a metadata & small block aggregators, returning any space
     *		back to file
     *
     * Return:      Success:        Non-negative
     *              Failure:        Negative
     *
     *-------------------------------------------------------------------------
     */
    herr_t H5MF_free_aggrs(H5F_t * f)
    {
        H5F_blk_aggr_t *first_aggr;              /* First aggregator to reset */
        H5F_blk_aggr_t *second_aggr;             /* Second aggregator to reset */
        haddr_t         ma_addr   = HADDR_UNDEF; /* Base "metadata aggregator" address */
        hsize_t         ma_size   = 0;           /* Size of "metadata aggregator" */
        haddr_t         sda_addr  = HADDR_UNDEF; /* Base "small data aggregator" address */
        hsize_t         sda_size  = 0;           /* Size of "small data aggregator" */
        herr_t          ret_value = SUCCEED;     /* Return value */

        FUNC_ENTER_NOAPI(FAIL)

        /* Check args */
        assert(f);
        assert(f->shared);
        assert(f->shared->lf);

        /* Retrieve metadata aggregator info, if available */
        if (H5MF__aggr_query(f, &(f->shared->meta_aggr), &ma_addr, &ma_size) < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGET, FAIL, "can't query metadata aggregator stats");

        /* Retrieve 'small data' aggregator info, if available */
        if (H5MF__aggr_query(f, &(f->shared->sdata_aggr), &sda_addr, &sda_size) < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGET, FAIL, "can't query small data aggregator stats");

        /* Make certain we release the aggregator that's later in the file first */
        /* (so the file shrinks properly) */
        if (H5_addr_defined(ma_addr) && H5_addr_defined(sda_addr)) {
            if (H5_addr_lt(ma_addr, sda_addr)) {
                first_aggr  = &(f->shared->sdata_aggr);
                second_aggr = &(f->shared->meta_aggr);
            } /* end if */
            else {
                first_aggr  = &(f->shared->meta_aggr);
                second_aggr = &(f->shared->sdata_aggr);
            } /* end else */
        }     /* end if */
        else {
            first_aggr  = &(f->shared->meta_aggr);
            second_aggr = &(f->shared->sdata_aggr);
        } /* end else */

        /* Release the unused portion of the metadata and "small data" blocks back
         * to the free lists in the file.
         */
        if (H5MF__aggr_reset(f, first_aggr) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTFREE, FAIL, "can't reset metadata block");
        if (H5MF__aggr_reset(f, second_aggr) < 0)
            HGOTO_ERROR(H5E_FILE, H5E_CANTFREE, FAIL, "can't reset 'small data' block");
done:
        FUNC_LEAVE_NOAPI(ret_value)
    } /* end H5MF_free_aggrs() */

    /*-------------------------------------------------------------------------
     * Function:    H5MF__aggr_can_shrink_eoa
     *
     * Purpose:     Check if the remaining space in the aggregator is at EOA
     *
     * Return:      Success:        non-negative (true/false)
     *              Failure:        negative
     *
     *-------------------------------------------------------------------------
     */
    static htri_t H5MF__aggr_can_shrink_eoa(H5F_t * f, H5FD_mem_t type, H5F_blk_aggr_t * aggr)
    {
        haddr_t eoa       = HADDR_UNDEF; /* EOA for the file */
        htri_t  ret_value = false;       /* Return value */

        FUNC_ENTER_PACKAGE

        /* Sanity check */
        assert(f);
        assert(aggr);
        assert(aggr->feature_flag == H5FD_FEAT_AGGREGATE_METADATA ||
               aggr->feature_flag == H5FD_FEAT_AGGREGATE_SMALLDATA);

        /* Get the EOA for the file */
        if (HADDR_UNDEF == (eoa = H5F_get_eoa(f, type)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGET, FAIL, "Unable to get eoa");

        /* Check if the aggregator is at EOA */
        if (aggr->size > 0 && H5_addr_defined(aggr->addr))
            ret_value = H5_addr_eq(eoa, aggr->addr + aggr->size);

done:
        FUNC_LEAVE_NOAPI(ret_value)
    } /* H5MF__aggr_can_shrink_eoa() */

    /*-------------------------------------------------------------------------
     * Function:    H5MF__aggr_free
     *
     * Purpose:     Free the aggregator's space in the file.
     *
     * Note:        Does _not_ put the space on a free list
     *
     * Return:      Success:        Non-negative
     *              Failure:        Negative
     *
     *-------------------------------------------------------------------------
     */
    static herr_t H5MF__aggr_free(H5F_t * f, H5FD_mem_t type, H5F_blk_aggr_t * aggr)
    {
        herr_t ret_value = SUCCEED; /* Return value */

        FUNC_ENTER_PACKAGE

        /* Sanity check */
        assert(f);
        assert(f->shared->lf);
        assert(aggr);
        assert(H5_addr_defined(aggr->addr));
        assert(aggr->size > 0);
        assert(H5F_INTENT(f) & H5F_ACC_RDWR);
        assert(aggr->feature_flag == H5FD_FEAT_AGGREGATE_METADATA ||
               aggr->feature_flag == H5FD_FEAT_AGGREGATE_SMALLDATA);
        assert(f->shared->feature_flags & aggr->feature_flag);

        /* Free the remaining space at EOA in the aggregator */
        if (H5F__free(f, type, aggr->addr, aggr->size) < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTFREE, FAIL, "can't free aggregation block");

        /* Reset the aggregator */
        aggr->tot_size = 0;
        aggr->addr     = HADDR_UNDEF;
        aggr->size     = 0;

done:
        FUNC_LEAVE_NOAPI(ret_value)
    } /* H5MF__aggr_free() */

    /*-------------------------------------------------------------------------
     * Function:    H5MF__aggrs_try_shrink_eoa
     *
     * Purpose:     Check the metadata & small block aggregators to see if
     *		EOA shrink is possible; if so, shrink each aggregator
     *
     * Return:      Success:        Non-negative
     *              Failure:        Negative
     *
     *-------------------------------------------------------------------------
     */
    htri_t H5MF__aggrs_try_shrink_eoa(H5F_t * f)
    {
        htri_t ma_status;        /* Whether the metadata aggregator can shrink the EOA */
        htri_t sda_status;       /* Whether the small data aggregator can shrink the EOA */
        htri_t ret_value = FAIL; /* Return value */

        FUNC_ENTER_PACKAGE

        /* Check args */
        assert(f);
        assert(f->shared);

        if ((ma_status = H5MF__aggr_can_shrink_eoa(f, H5FD_MEM_DEFAULT, &(f->shared->meta_aggr))) < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGET, FAIL, "can't query metadata aggregator stats");
        if (ma_status > 0)
            if (H5MF__aggr_free(f, H5FD_MEM_DEFAULT, &(f->shared->meta_aggr)) < 0)
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTSHRINK, FAIL, "can't check for shrinking eoa");

        if ((sda_status = H5MF__aggr_can_shrink_eoa(f, H5FD_MEM_DRAW, &(f->shared->sdata_aggr))) < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGET, FAIL, "can't query small data aggregator stats");
        if (sda_status > 0)
            if (H5MF__aggr_free(f, H5FD_MEM_DRAW, &(f->shared->sdata_aggr)) < 0)
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTSHRINK, FAIL, "can't check for shrinking eoa");

        ret_value = (ma_status || sda_status);

done:
        FUNC_LEAVE_NOAPI(ret_value)
    } /* end H5MF__aggrs_try_shrink_eoa() */
