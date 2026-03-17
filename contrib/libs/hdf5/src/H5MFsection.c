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
 * Purpose:	Free space section callbacks for file.
 *
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

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

/* 'simple/small/large' section callbacks */
static H5FS_section_info_t *H5MF__sect_deserialize(const H5FS_section_class_t *cls, const uint8_t *buf,
                                                   haddr_t sect_addr, hsize_t sect_size, unsigned *des_flags);
static herr_t H5MF__sect_valid(const H5FS_section_class_t *cls, const H5FS_section_info_t *sect);
static H5FS_section_info_t *H5MF__sect_split(H5FS_section_info_t *sect, hsize_t frag_size);

/* 'simple' section callbacks */
static htri_t H5MF__sect_simple_can_merge(const H5FS_section_info_t *sect1, const H5FS_section_info_t *sect2,
                                          void *udata);
static herr_t H5MF__sect_simple_merge(H5FS_section_info_t **sect1, H5FS_section_info_t *sect2, void *udata);
static htri_t H5MF__sect_simple_can_shrink(const H5FS_section_info_t *_sect, void *udata);
static herr_t H5MF__sect_simple_shrink(H5FS_section_info_t **_sect, void *udata);

/* 'small' section callbacks */
static herr_t H5MF__sect_small_add(H5FS_section_info_t **_sect, unsigned *flags, void *_udata);
static htri_t H5MF__sect_small_can_merge(const H5FS_section_info_t *sect1, const H5FS_section_info_t *sect2,
                                         void *udata);
static herr_t H5MF__sect_small_merge(H5FS_section_info_t **sect1, H5FS_section_info_t *sect2, void *udata);

/* 'large' section callbacks */
static htri_t H5MF__sect_large_can_merge(const H5FS_section_info_t *sect1, const H5FS_section_info_t *sect2,
                                         void *udata);
static herr_t H5MF__sect_large_merge(H5FS_section_info_t **sect1, H5FS_section_info_t *sect2, void *udata);
static htri_t H5MF__sect_large_can_shrink(const H5FS_section_info_t *_sect, void *udata);
static herr_t H5MF__sect_large_shrink(H5FS_section_info_t **_sect, void *udata);

/*********************/
/* Package Variables */
/*********************/

/* Class info for "simple" free space sections */
const H5FS_section_class_t H5MF_FSPACE_SECT_CLS_SIMPLE[1] = {{
    /* Class variables */
    H5MF_FSPACE_SECT_SIMPLE,                 /* Section type                 */
    0,                                       /* Extra serialized size        */
    H5FS_CLS_MERGE_SYM | H5FS_CLS_ADJUST_OK, /* Class flags                  */
    NULL,                                    /* Class private info           */

    /* Class methods */
    NULL, /* Initialize section class     */
    NULL, /* Terminate section class      */

    /* Object methods */
    NULL,                         /* Add section                  */
    NULL,                         /* Serialize section            */
    H5MF__sect_deserialize,       /* Deserialize section          */
    H5MF__sect_simple_can_merge,  /* Can sections merge?          */
    H5MF__sect_simple_merge,      /* Merge sections               */
    H5MF__sect_simple_can_shrink, /* Can section shrink container?*/
    H5MF__sect_simple_shrink,     /* Shrink container w/section   */
    H5MF__sect_free,              /* Free section                 */
    H5MF__sect_valid,             /* Check validity of section    */
    H5MF__sect_split,             /* Split section node for alignment */
    NULL,                         /* Dump debugging for section   */
}};

/* Class info for "small" free space sections */
const H5FS_section_class_t H5MF_FSPACE_SECT_CLS_SMALL[1] = {{
    /* Class variables */
    H5MF_FSPACE_SECT_SMALL,                  /* Section type                 */
    0,                                       /* Extra serialized size        */
    H5FS_CLS_MERGE_SYM | H5FS_CLS_ADJUST_OK, /* Class flags                  */
    NULL,                                    /* Class private info           */

    /* Class methods */
    NULL, /* Initialize section class     */
    NULL, /* Terminate section class      */

    /* Object methods */
    H5MF__sect_small_add,       /* Add section                  */
    NULL,                       /* Serialize section            */
    H5MF__sect_deserialize,     /* Deserialize section          */
    H5MF__sect_small_can_merge, /* Can sections merge?          */
    H5MF__sect_small_merge,     /* Merge sections               */
    NULL,                       /* Can section shrink container?*/
    NULL,                       /* Shrink container w/section   */
    H5MF__sect_free,            /* Free section                 */
    H5MF__sect_valid,           /* Check validity of section    */
    H5MF__sect_split,           /* Split section node for alignment */
    NULL,                       /* Dump debugging for section   */
}};

/* Class info for "large" free space sections */
const H5FS_section_class_t H5MF_FSPACE_SECT_CLS_LARGE[1] = {{
    /* Class variables */
    H5MF_FSPACE_SECT_LARGE,                  /* Section type                 */
    0,                                       /* Extra serialized size        */
    H5FS_CLS_MERGE_SYM | H5FS_CLS_ADJUST_OK, /* Class flags                  */
    NULL,                                    /* Class private info           */

    /* Class methods */
    NULL, /* Initialize section class     */
    NULL, /* Terminate section class      */

    /* Object methods */
    NULL,                        /* Add section                  */
    NULL,                        /* Serialize section            */
    H5MF__sect_deserialize,      /* Deserialize section          */
    H5MF__sect_large_can_merge,  /* Can sections merge?          */
    H5MF__sect_large_merge,      /* Merge sections               */
    H5MF__sect_large_can_shrink, /* Can section shrink container?*/
    H5MF__sect_large_shrink,     /* Shrink container w/section   */
    H5MF__sect_free,             /* Free section                 */
    H5MF__sect_valid,            /* Check validity of section    */
    H5MF__sect_split,            /* Split section node for alignment */
    NULL,                        /* Dump debugging for section   */
}};

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Declare a free list to manage the H5MF_free_section_t struct */
H5FL_DEFINE_STATIC(H5MF_free_section_t);

/*
 * "simple/small/large" section callbacks
 */

/*-------------------------------------------------------------------------
 * Function:	H5MF__sect_new
 *
 * Purpose:	Create a new section of "ctype" and return it to the caller
 *
 * Return:	Pointer to new section on success/NULL on failure
 *
 *-------------------------------------------------------------------------
 */
H5MF_free_section_t *
H5MF__sect_new(unsigned ctype, haddr_t sect_off, hsize_t sect_size)
{
    H5MF_free_section_t *sect;             /* 'Simple' free space section to add */
    H5MF_free_section_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments.  */
    assert(sect_size);

    /* Create free space section node */
    if (NULL == (sect = H5FL_MALLOC(H5MF_free_section_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL,
                    "memory allocation failed for direct block free list section");

    /* Set the information passed in */
    sect->sect_info.addr = sect_off;
    sect->sect_info.size = sect_size;

    /* Set the section's class & state */
    sect->sect_info.type  = ctype;
    sect->sect_info.state = H5FS_SECT_LIVE;

    /* Set return value */
    ret_value = sect;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5MF__sect_new() */

/*-------------------------------------------------------------------------
 * Function:	H5MF__sect_free
 *
 * Purpose:	Free a 'simple/small/large' section node
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5MF__sect_free(H5FS_section_info_t *_sect)
{
    H5MF_free_section_t *sect = (H5MF_free_section_t *)_sect; /* File free section */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments. */
    assert(sect);

    /* Release the section */
    sect = H5FL_FREE(H5MF_free_section_t, sect);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5MF__sect_free() */

/*-------------------------------------------------------------------------
 * Function:	H5MF__sect_deserialize
 *
 * Purpose:	Deserialize a buffer into a "live" section
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static H5FS_section_info_t *
H5MF__sect_deserialize(const H5FS_section_class_t *cls, const uint8_t H5_ATTR_UNUSED *buf, haddr_t sect_addr,
                       hsize_t sect_size, unsigned H5_ATTR_UNUSED *des_flags)
{
    H5MF_free_section_t *sect;             /* New section */
    H5FS_section_info_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(cls);
    assert(H5_addr_defined(sect_addr));
    assert(sect_size);

    /* Create free space section for block */
    if (NULL == (sect = H5MF__sect_new(cls->type, sect_addr, sect_size)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't initialize free space section");

    /* Set return value */
    ret_value = (H5FS_section_info_t *)sect;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5MF__sect_deserialize() */

/*-------------------------------------------------------------------------
 * Function:	H5MF__sect_valid
 *
 * Purpose:	Check the validity of a section
 *
 * Return:	Success:	non-negative
 *          Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5MF__sect_valid(const H5FS_section_class_t H5_ATTR_UNUSED *cls, const H5FS_section_info_t
#ifdef NDEBUG
                                                                     H5_ATTR_UNUSED
#endif /* NDEBUG */
                                                                         *_sect)
{
#ifndef NDEBUG
    const H5MF_free_section_t *sect = (const H5MF_free_section_t *)_sect; /* File free section */
#endif                                                                    /* NDEBUG */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments. */
    assert(sect);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5MF__sect_valid() */

/*-------------------------------------------------------------------------
 * Function:	H5MF__sect_split
 *
 * Purpose:	Split SECT into 2 sections: fragment for alignment & the aligned section
 *          SECT's addr and size are updated to point to the aligned section
 *
 * Return:	Success:	the fragment for aligning sect
 *          Failure:	null
 *
 *-------------------------------------------------------------------------
 */
static H5FS_section_info_t *
H5MF__sect_split(H5FS_section_info_t *sect, hsize_t frag_size)
{
    H5MF_free_section_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Allocate space for new section */
    if (NULL == (ret_value = H5MF__sect_new(sect->type, sect->addr, frag_size)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, NULL, "can't initialize free space section");

    /* Set new section's info */
    sect->addr += frag_size;
    sect->size -= frag_size;

done:
    FUNC_LEAVE_NOAPI((H5FS_section_info_t *)ret_value)
} /* end H5MF__sect_split() */

/*
 * "simple" section callbacks
 */

/*-------------------------------------------------------------------------
 * Function:	H5MF__sect_simple_can_merge
 *
 * Purpose:	Can two sections of this type merge?
 *
 * Note:        Second section must be "after" first section
 *
 * Return:	Success:	non-negative (true/false)
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5MF__sect_simple_can_merge(const H5FS_section_info_t *_sect1, const H5FS_section_info_t *_sect2,
                            void H5_ATTR_UNUSED *_udata)
{
    const H5MF_free_section_t *sect1     = (const H5MF_free_section_t *)_sect1; /* File free section */
    const H5MF_free_section_t *sect2     = (const H5MF_free_section_t *)_sect2; /* File free section */
    htri_t                     ret_value = FAIL;                                /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments. */
    assert(sect1);
    assert(sect2);
    assert(sect1->sect_info.type == sect2->sect_info.type); /* Checks "MERGE_SYM" flag */
    assert(H5_addr_lt(sect1->sect_info.addr, sect2->sect_info.addr));

    /* Check if second section adjoins first section */
    ret_value = H5_addr_eq(sect1->sect_info.addr + sect1->sect_info.size, sect2->sect_info.addr);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5MF__sect_simple_can_merge() */

/*-------------------------------------------------------------------------
 * Function:	H5MF__sect_simple_merge
 *
 * Purpose:	Merge two sections of this type
 *
 * Note:        Second section always merges into first node
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5MF__sect_simple_merge(H5FS_section_info_t **_sect1, H5FS_section_info_t *_sect2,
                        void H5_ATTR_UNUSED *_udata)
{
    H5MF_free_section_t **sect1     = (H5MF_free_section_t **)_sect1; /* File free section */
    H5MF_free_section_t  *sect2     = (H5MF_free_section_t *)_sect2;  /* File free section */
    herr_t                ret_value = SUCCEED;                        /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(sect1);
    assert((*sect1)->sect_info.type == H5MF_FSPACE_SECT_SIMPLE);
    assert(sect2);
    assert(sect2->sect_info.type == H5MF_FSPACE_SECT_SIMPLE);
    assert(H5_addr_eq((*sect1)->sect_info.addr + (*sect1)->sect_info.size, sect2->sect_info.addr));

    /* Add second section's size to first section */
    (*sect1)->sect_info.size += sect2->sect_info.size;

    /* Get rid of second section */
    if (H5MF__sect_free((H5FS_section_info_t *)sect2) < 0)
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTRELEASE, FAIL, "can't free section node");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5MF__sect_simple_merge() */

/*-------------------------------------------------------------------------
 * Function:	H5MF__sect_simple_can_shrink
 *
 * Purpose:	Can this section shrink the container?
 *
 * Return:	Success:	non-negative (true/false)
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5MF__sect_simple_can_shrink(const H5FS_section_info_t *_sect, void *_udata)
{
    const H5MF_free_section_t *sect  = (const H5MF_free_section_t *)_sect; /* File free section */
    H5MF_sect_ud_t            *udata = (H5MF_sect_ud_t *)_udata;           /* User data for callback */
    haddr_t                    eoa;              /* End of address space in the file */
    haddr_t                    end;              /* End of section to extend */
    htri_t                     ret_value = FAIL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(sect);
    assert(udata);
    assert(udata->f);

    /* Retrieve the end of the file's address space */
    if (HADDR_UNDEF == (eoa = H5F_get_eoa(udata->f, udata->alloc_type)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGET, FAIL, "driver get_eoa request failed");

    /* Compute address of end of section to check */
    end = sect->sect_info.addr + sect->sect_info.size;

    /* Check if the section is exactly at the end of the allocated space in the file */
    if (H5_addr_eq(end, eoa)) {
        /* Set the shrinking type */
        udata->shrink = H5MF_SHRINK_EOA;
#ifdef H5MF_ALLOC_DEBUG_MORE
        fprintf(stderr, "%s: section {%" PRIuHADDR ", %" PRIuHSIZE "}, shrinks file, eoa = %" PRIuHADDR "\n",
                __func__, sect->sect_info.addr, sect->sect_info.size, eoa);
#endif /* H5MF_ALLOC_DEBUG_MORE */

        /* Indicate shrinking can occur */
        HGOTO_DONE(true);
    } /* end if */
    else {
        /* Shrinking can't occur if the 'eoa_shrink_only' flag is set and we're not shrinking the EOA */
        if (udata->allow_eoa_shrink_only)
            HGOTO_DONE(false);

        /* Check if this section is allowed to merge with metadata aggregation block */
        if (udata->f->shared->fs_aggr_merge[udata->alloc_type] & H5F_FS_MERGE_METADATA) {
            htri_t status; /* Status from aggregator adjoin */

            /* See if section can absorb the aggregator & vice versa */
            if ((status = H5MF__aggr_can_absorb(udata->f, &(udata->f->shared->meta_aggr), sect,
                                                &(udata->shrink))) < 0)
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTMERGE, FAIL,
                            "error merging section with aggregation block");
            else if (status > 0) {
                /* Set the aggregator to operate on */
                udata->aggr = &(udata->f->shared->meta_aggr);
#ifdef H5MF_ALLOC_DEBUG_MORE
                fprintf(stderr, "%s: section {%" PRIuHADDR ", %" PRIuHSIZE "}, adjoins metadata aggregator\n",
                        __func__, sect->sect_info.addr, sect->sect_info.size);
#endif /* H5MF_ALLOC_DEBUG_MORE */

                /* Indicate shrinking can occur */
                HGOTO_DONE(true);
            } /* end if */
        }     /* end if */

        /* Check if this section is allowed to merge with small 'raw' aggregation block */
        if (udata->f->shared->fs_aggr_merge[udata->alloc_type] & H5F_FS_MERGE_RAWDATA) {
            htri_t status; /* Status from aggregator adjoin */

            /* See if section can absorb the aggregator & vice versa */
            if ((status = H5MF__aggr_can_absorb(udata->f, &(udata->f->shared->sdata_aggr), sect,
                                                &(udata->shrink))) < 0)
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTMERGE, FAIL,
                            "error merging section with aggregation block");
            else if (status > 0) {
                /* Set the aggregator to operate on */
                udata->aggr = &(udata->f->shared->sdata_aggr);
#ifdef H5MF_ALLOC_DEBUG_MORE
                fprintf(stderr,
                        "%s: section {%" PRIuHADDR ", %" PRIuHSIZE "}, adjoins small data aggregator\n",
                        __func__, sect->sect_info.addr, sect->sect_info.size);
#endif /* H5MF_ALLOC_DEBUG_MORE */

                /* Indicate shrinking can occur */
                HGOTO_DONE(true);
            } /* end if */
        }     /* end if */
    }         /* end else */

    /* Set return value */
    ret_value = false;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5MF__sect_simple_can_shrink() */

/*-------------------------------------------------------------------------
 * Function:	H5MF__sect_simple_shrink
 *
 * Purpose:	Shrink container with section
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5MF__sect_simple_shrink(H5FS_section_info_t **_sect, void *_udata)
{
    H5MF_free_section_t **sect      = (H5MF_free_section_t **)_sect; /* File free section */
    H5MF_sect_ud_t       *udata     = (H5MF_sect_ud_t *)_udata;      /* User data for callback */
    herr_t                ret_value = SUCCEED;                       /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(sect);
    assert(udata);
    assert(udata->f);

    /* Check for shrinking file */
    if (H5MF_SHRINK_EOA == udata->shrink) {
        /* Sanity check */
        assert(H5F_INTENT(udata->f) & H5F_ACC_RDWR);

        /* Release section's space at EOA */
        if (H5F__free(udata->f, udata->alloc_type, (*sect)->sect_info.addr, (*sect)->sect_info.size) < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTFREE, FAIL, "driver free request failed");
    } /* end if */
    else {
        /* Sanity check */
        assert(udata->aggr);

        /* Absorb the section into the aggregator or vice versa */
        if (H5MF__aggr_absorb(udata->f, udata->aggr, *sect, udata->allow_sect_absorb) < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTMERGE, FAIL,
                        "can't absorb section into aggregator or vice versa");
    } /* end else */

    /* Check for freeing section */
    if (udata->shrink != H5MF_SHRINK_SECT_ABSORB_AGGR) {
        /* Free section */
        if (H5MF__sect_free((H5FS_section_info_t *)*sect) < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTRELEASE, FAIL, "can't free simple section node");

        /* Mark section as freed, for free space manager */
        *sect = NULL;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5MF__sect_simple_shrink() */

/*
 * "small" section callbacks
 */

/*-------------------------------------------------------------------------
 * Function:    H5MF__sect_small_add
 *
 * Purpose:     Perform actions on a small "meta" action before adding it to the free space manager:
 *              1) Drop the section if it is at page end and its size <= page end threshold
 *              2) Adjust section size to include page end threshold if
 *                 (section size + threshold) is at page end
 *
 * Return:      Success:        non-negative
 *              Failure:        negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5MF__sect_small_add(H5FS_section_info_t **_sect, unsigned *flags, void *_udata)
{
    H5MF_free_section_t **sect  = (H5MF_free_section_t **)_sect; /* Fractal heap free section */
    H5MF_sect_ud_t       *udata = (H5MF_sect_ud_t *)_udata;      /* User data for callback */
    haddr_t               sect_end;
    hsize_t               rem, prem;
    herr_t                ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

#ifdef H5MF_ALLOC_DEBUG_MORE
    fprintf(stderr, "%s: Entering, section {%" PRIuHADDR ", %" PRIuHSIZE "}\n", __func__,
            (*sect)->sect_info.addr, (*sect)->sect_info.size);
#endif /* H5MF_ALLOC_DEBUG_MORE */

    /* Do not adjust the section raw data or global heap data */
    if (udata->alloc_type == H5FD_MEM_DRAW || udata->alloc_type == H5FD_MEM_GHEAP)
        HGOTO_DONE(ret_value);

    sect_end = (*sect)->sect_info.addr + (*sect)->sect_info.size;
    rem      = sect_end % udata->f->shared->fs_page_size;
    prem     = udata->f->shared->fs_page_size - rem;

    /* Drop the section if it is at page end and its size is <= pgend threshold */
    if (!rem && (*sect)->sect_info.size <= H5F_PGEND_META_THRES(udata->f) &&
        (*flags & H5FS_ADD_RETURNED_SPACE)) {
        if (H5MF__sect_free((H5FS_section_info_t *)(*sect)) < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTRELEASE, FAIL, "can't free section node");
        *sect = NULL;
        *flags &= (unsigned)~H5FS_ADD_RETURNED_SPACE;
        *flags |= H5FS_PAGE_END_NO_ADD;
#ifdef H5MF_ALLOC_DEBUG_MORE
        fprintf(stderr, "%s: section is dropped\n", __func__);
#endif /* H5MF_ALLOC_DEBUG_MORE */
    }  /* end if */
    /* Adjust the section if it is not at page end but its size + prem is at page end */
    else if (prem <= H5F_PGEND_META_THRES(udata->f)) {
        (*sect)->sect_info.size += prem;
#ifdef H5MF_ALLOC_DEBUG_MORE
        fprintf(stderr, "%s: section is adjusted {%" PRIuHADDR ", %" PRIuHSIZE "}\n", __func__,
                (*sect)->sect_info.addr, (*sect)->sect_info.size);
#endif /* H5MF_ALLOC_DEBUG_MORE */
    }  /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5MF__sect_small_add() */

/*-------------------------------------------------------------------------
 * Function:	H5MF__sect_small_can_merge
 *
 * Purpose:	Can two sections of this type merge?
 *
 * Note: Second section must be "after" first section
 *       The "merged" section cannot cross page boundary.
 *
 * Return:	Success:	non-negative (true/false)
 *          Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5MF__sect_small_can_merge(const H5FS_section_info_t *_sect1, const H5FS_section_info_t *_sect2, void *_udata)
{
    const H5MF_free_section_t *sect1     = (const H5MF_free_section_t *)_sect1; /* File free section */
    const H5MF_free_section_t *sect2     = (const H5MF_free_section_t *)_sect2; /* File free section */
    H5MF_sect_ud_t            *udata     = (H5MF_sect_ud_t *)_udata;            /* User data for callback */
    htri_t                     ret_value = false;                               /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments. */
    assert(sect1);
    assert(sect2);
    assert(sect1->sect_info.type == sect2->sect_info.type); /* Checks "MERGE_SYM" flag */
    assert(H5_addr_lt(sect1->sect_info.addr, sect2->sect_info.addr));

    /* Check if second section adjoins first section */
    ret_value = H5_addr_eq(sect1->sect_info.addr + sect1->sect_info.size, sect2->sect_info.addr);
    if (ret_value > 0)
        /* If they are on different pages, couldn't merge */
        if ((sect1->sect_info.addr / udata->f->shared->fs_page_size) !=
            (((sect2->sect_info.addr + sect2->sect_info.size - 1) / udata->f->shared->fs_page_size)))
            ret_value = false;

#ifdef H5MF_ALLOC_DEBUG_MORE
    fprintf(stderr, "%s: Leaving: ret_value = %d\n", __func__, ret_value);
#endif /* H5MF_ALLOC_DEBUG_MORE */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5MF__sect_small_can_merge() */

/*-------------------------------------------------------------------------
 * Function:	H5MF__sect_small_merge
 *
 * Purpose:	Merge two sections of this type
 *
 * Note: Second section always merges into first node.
 *       If the size of the "merged" section is equal to file space page size,
 *       free the section.
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5MF__sect_small_merge(H5FS_section_info_t **_sect1, H5FS_section_info_t *_sect2, void *_udata)
{
    H5MF_free_section_t **sect1     = (H5MF_free_section_t **)_sect1; /* File free section */
    H5MF_free_section_t  *sect2     = (H5MF_free_section_t *)_sect2;  /* File free section */
    H5MF_sect_ud_t       *udata     = (H5MF_sect_ud_t *)_udata;       /* User data for callback */
    herr_t                ret_value = SUCCEED;                        /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(sect1);
    assert((*sect1)->sect_info.type == H5MF_FSPACE_SECT_SMALL);
    assert(sect2);
    assert(sect2->sect_info.type == H5MF_FSPACE_SECT_SMALL);
    assert(H5_addr_eq((*sect1)->sect_info.addr + (*sect1)->sect_info.size, sect2->sect_info.addr));

    /* Add second section's size to first section */
    (*sect1)->sect_info.size += sect2->sect_info.size;

    if ((*sect1)->sect_info.size == udata->f->shared->fs_page_size) {
        if (H5MF_xfree(udata->f, udata->alloc_type, (*sect1)->sect_info.addr, (*sect1)->sect_info.size) < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTFREE, FAIL, "can't free merged section");

        /* Need to free possible metadata page in the PB cache */
        /* This is in response to the data corruption bug from fheap.c with page buffering + page strategy */
        /* Note: Large metadata page bypasses the PB cache */
        /* Note: Update of raw data page (large or small sized) is handled by the PB cache */
        if (udata->f->shared->page_buf != NULL && udata->alloc_type != H5FD_MEM_DRAW)
            if (H5PB_remove_entry(udata->f->shared, (*sect1)->sect_info.addr) < 0)
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTFREE, FAIL, "can't free merged section");

        if (H5MF__sect_free((H5FS_section_info_t *)(*sect1)) < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTRELEASE, FAIL, "can't free section node");
        *sect1 = NULL;
    } /* end if */

    /* Get rid of second section */
    if (H5MF__sect_free((H5FS_section_info_t *)sect2) < 0)
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTRELEASE, FAIL, "can't free section node");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5MF__sect_small_merge() */

/*
 * "Large" section callbacks
 */

/*-------------------------------------------------------------------------
 * Function:	H5MF__sect_large_can_merge (same as H5MF__sect_simple_can_merge)
 *
 * Purpose:	Can two sections of this type merge?
 *
 * Note: Second section must be "after" first section
 *
 * Return:	Success:	non-negative (true/false)
 *          Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5MF__sect_large_can_merge(const H5FS_section_info_t *_sect1, const H5FS_section_info_t *_sect2,
                           void H5_ATTR_UNUSED *_udata)
{
    const H5MF_free_section_t *sect1     = (const H5MF_free_section_t *)_sect1; /* File free section */
    const H5MF_free_section_t *sect2     = (const H5MF_free_section_t *)_sect2; /* File free section */
    htri_t                     ret_value = false;                               /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments. */
    assert(sect1);
    assert(sect2);
    assert(sect1->sect_info.type == sect2->sect_info.type); /* Checks "MERGE_SYM" flag */
    assert(H5_addr_lt(sect1->sect_info.addr, sect2->sect_info.addr));

    ret_value = H5_addr_eq(sect1->sect_info.addr + sect1->sect_info.size, sect2->sect_info.addr);

#ifdef H5MF_ALLOC_DEBUG_MORE
    fprintf(stderr, "%s: Leaving: ret_value = %d\n", __func__, ret_value);
#endif /* H5MF_ALLOC_DEBUG_MORE */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5MF__sect_large_can_merge() */

/*-------------------------------------------------------------------------
 * Function:	H5MF__sect_large_merge (same as H5MF__sect_simple_merge)
 *
 * Purpose:	Merge two sections of this type
 *
 * Note: Second section always merges into first node
 *
 * Return:	Success:	non-negative
 *          Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5MF__sect_large_merge(H5FS_section_info_t **_sect1, H5FS_section_info_t *_sect2, void H5_ATTR_UNUSED *_udata)
{
    H5MF_free_section_t **sect1     = (H5MF_free_section_t **)_sect1; /* File free section */
    H5MF_free_section_t  *sect2     = (H5MF_free_section_t *)_sect2;  /* File free section */
    herr_t                ret_value = SUCCEED;                        /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(sect1);
    assert((*sect1)->sect_info.type == H5MF_FSPACE_SECT_LARGE);
    assert(sect2);
    assert(sect2->sect_info.type == H5MF_FSPACE_SECT_LARGE);
    assert(H5_addr_eq((*sect1)->sect_info.addr + (*sect1)->sect_info.size, sect2->sect_info.addr));

    /* Add second section's size to first section */
    (*sect1)->sect_info.size += sect2->sect_info.size;

    /* Get rid of second section */
    if (H5MF__sect_free((H5FS_section_info_t *)sect2) < 0)
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTRELEASE, FAIL, "can't free section node");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5MF__sect_large_merge() */

/*-------------------------------------------------------------------------
 * Function:	H5MF__sect_large_can_shrink
 *
 * Purpose:	Can this section shrink the container?
 *
 * Return:	Success:	non-negative (true/false)
 *          Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5MF__sect_large_can_shrink(const H5FS_section_info_t *_sect, void *_udata)
{
    const H5MF_free_section_t *sect  = (const H5MF_free_section_t *)_sect; /* File free section */
    H5MF_sect_ud_t            *udata = (H5MF_sect_ud_t *)_udata;           /* User data for callback */
    haddr_t                    eoa;               /* End of address space in the file */
    haddr_t                    end;               /* End of section to extend */
    htri_t                     ret_value = false; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(sect);
    assert(sect->sect_info.type == H5MF_FSPACE_SECT_LARGE);
    assert(udata);
    assert(udata->f);

    /* Retrieve the end of the file's address space */
    if (HADDR_UNDEF == (eoa = H5FD_get_eoa(udata->f->shared->lf, udata->alloc_type)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTGET, FAIL, "driver get_eoa request failed");

    /* Compute address of end of section to check */
    end = sect->sect_info.addr + sect->sect_info.size;

    /* Check if the section is exactly at the end of the allocated space in the file */
    if (H5_addr_eq(end, eoa) && sect->sect_info.size >= udata->f->shared->fs_page_size) {
        /* Set the shrinking type */
        udata->shrink = H5MF_SHRINK_EOA;
#ifdef H5MF_ALLOC_DEBUG_MORE
        fprintf(stderr, "%s: section {%" PRIuHADDR ", %" PRIuHSIZE "}, shrinks file, eoa = %" PRIuHADDR "\n",
                __func__, sect->sect_info.addr, sect->sect_info.size, eoa);
#endif /* H5MF_ALLOC_DEBUG_MORE */

        /* Indicate shrinking can occur */
        HGOTO_DONE(true);
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5MF__sect_large_can_shrink() */

/*-------------------------------------------------------------------------
 * Function:	H5MF__sect_large_shrink
 *
 * Purpose:     Shrink a large-sized section
 *
 * Return:      Success:	non-negative
 *              Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5MF__sect_large_shrink(H5FS_section_info_t **_sect, void *_udata)
{
    H5MF_free_section_t **sect      = (H5MF_free_section_t **)_sect; /* File free section */
    H5MF_sect_ud_t       *udata     = (H5MF_sect_ud_t *)_udata;      /* User data for callback */
    hsize_t               frag_size = 0;                             /* Fragment size */
    herr_t                ret_value = SUCCEED;                       /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(sect);
    assert((*sect)->sect_info.type == H5MF_FSPACE_SECT_LARGE);
    assert(udata);
    assert(udata->f);
    assert(udata->shrink == H5MF_SHRINK_EOA);
    assert(H5F_INTENT(udata->f) & H5F_ACC_RDWR);
    assert(H5F_PAGED_AGGR(udata->f));

    /* Calculate possible mis-aligned fragment */
    H5MF_EOA_MISALIGN(udata->f, (*sect)->sect_info.addr, udata->f->shared->fs_page_size, frag_size);

    /* Free full pages from EOA */
    /* Retain partial page in the free-space manager so as to keep EOA at page boundary */
    if (H5F__free(udata->f, udata->alloc_type, (*sect)->sect_info.addr + frag_size,
                  (*sect)->sect_info.size - frag_size) < 0)
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTFREE, FAIL, "driver free request failed");

    if (frag_size) /* Adjust section size for the partial page */
        (*sect)->sect_info.size = frag_size;
    else {
        /* Free section */
        if (H5MF__sect_free((H5FS_section_info_t *)*sect) < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTRELEASE, FAIL, "can't free simple section node");

        /* Mark section as freed, for free space manager */
        *sect = NULL;
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5MF__sect_large_shrink() */
