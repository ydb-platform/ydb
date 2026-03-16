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
 * Created:		H5HFspace.c
 *
 * Purpose:		Space allocation routines for fractal heaps.
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
#include "H5private.h"  /* Generic Functions			*/
#include "H5Eprivate.h" /* Error handling		  	*/
#include "H5HFpkg.h"    /* Fractal heaps			*/

/****************/
/* Local Macros */
/****************/

#define H5HF_FSPACE_SHRINK    80  /* Percent of "normal" size to shrink serialized free space size */
#define H5HF_FSPACE_EXPAND    120 /* Percent of "normal" size to expand serialized free space size */
#define H5HF_FSPACE_THRHD_DEF 1   /* Default: no alignment threshold */
#define H5HF_FSPACE_ALIGN_DEF 1   /* Default: no alignment */

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

/*-------------------------------------------------------------------------
 * Function:	H5HF__space_start
 *
 * Purpose:	"Start up" free space for heap - open existing free space
 *              structure if one exists, otherwise create a new free space
 *              structure
 *
 * Return:	Success:	non-negative
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__space_start(H5HF_hdr_t *hdr, bool may_create)
{
    const H5FS_section_class_t *classes[] = {/* Free space section classes implemented for fractal heap */
                                             H5HF_FSPACE_SECT_CLS_SINGLE, H5HF_FSPACE_SECT_CLS_FIRST_ROW,
                                             H5HF_FSPACE_SECT_CLS_NORMAL_ROW, H5HF_FSPACE_SECT_CLS_INDIRECT};
    herr_t                      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);

    /* Check for creating free space info for the heap */
    if (H5_addr_defined(hdr->fs_addr)) {
        /* Open an existing free space structure for the heap */
        if (NULL == (hdr->fspace = H5FS_open(hdr->f, hdr->fs_addr, NELMTS(classes), classes, hdr,
                                             (hsize_t)H5HF_FSPACE_THRHD_DEF, (hsize_t)H5HF_FSPACE_ALIGN_DEF)))
            HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't initialize free space info");
    } /* end if */
    else {
        /* Check if we are allowed to create the free space manager */
        if (may_create) {
            H5FS_create_t fs_create; /* Free space creation parameters */

            /* Set the free space creation parameters */
            fs_create.client         = H5FS_CLIENT_FHEAP_ID;
            fs_create.shrink_percent = H5HF_FSPACE_SHRINK;
            fs_create.expand_percent = H5HF_FSPACE_EXPAND;
            fs_create.max_sect_size  = hdr->man_dtable.cparam.max_direct_size;
            fs_create.max_sect_addr  = hdr->man_dtable.cparam.max_index;

            /* Create the free space structure for the heap */
            if (NULL ==
                (hdr->fspace = H5FS_create(hdr->f, &hdr->fs_addr, &fs_create, NELMTS(classes), classes, hdr,
                                           (hsize_t)H5HF_FSPACE_THRHD_DEF, (hsize_t)H5HF_FSPACE_ALIGN_DEF)))
                HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't initialize free space info");
            assert(H5_addr_defined(hdr->fs_addr));
        } /* end if */
    }     /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__space_start() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__space_add
 *
 * Purpose:	Add a section to the free space for the heap
 *
 * Return:	Success:	non-negative
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__space_add(H5HF_hdr_t *hdr, H5HF_free_section_t *node, unsigned flags)
{
    H5HF_sect_add_ud_t udata;               /* User data for free space manager 'add' */
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(node);

    /* Check if the free space for the heap has been initialized */
    if (!hdr->fspace)
        if (H5HF__space_start(hdr, true) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't initialize heap free space");

    /* Construct user data */
    udata.hdr = hdr;

    /* Add to the free space for the heap */
    if (H5FS_sect_add(hdr->f, hdr->fspace, (H5FS_section_info_t *)node, flags, &udata) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINSERT, FAIL, "can't add section to heap free space");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__space_add() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__space_find
 *
 * Purpose:	Attempt to find space in a fractal heap
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5HF__space_find(H5HF_hdr_t *hdr, hsize_t request, H5HF_free_section_t **node)
{
    htri_t node_found = false; /* Whether an existing free list node was found */
    htri_t ret_value  = FAIL;  /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(request);
    assert(node);

    /* Check if the free space for the heap has been initialized */
    if (!hdr->fspace)
        if (H5HF__space_start(hdr, false) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't initialize heap free space");

    /* Search for free space in the heap */
    if (hdr->fspace)
        if ((node_found = H5FS_sect_find(hdr->f, hdr->fspace, request, (H5FS_section_info_t **)node)) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, FAIL, "can't locate free space in fractal heap");

    /* Set return value */
    ret_value = node_found;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__space_find() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__space_revert_root_cb
 *
 * Purpose:	Callback routine from iterator, to reset 'parent' pointers in
 *		sections, when the heap is changing from having a root indirect
 *		block to a direct block.
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__space_revert_root_cb(H5FS_section_info_t *_sect, void H5_ATTR_UNUSED *_udata)
{
    H5HF_free_section_t *sect      = (H5HF_free_section_t *)_sect; /* Section to dump info */
    herr_t               ret_value = SUCCEED;                      /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(sect);

    /* Only modify "live" single blocks... */
    if (sect->sect_info.type == H5HF_FSPACE_SECT_SINGLE && sect->sect_info.state == H5FS_SECT_LIVE) {
        /* Release hold on previous indirect block (we must have one) */
        assert(sect->u.single.parent);
        if (H5HF__iblock_decr(sect->u.single.parent) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTDEC, FAIL,
                        "can't decrement reference count on section's indirect block");

        /* Reset parent information */
        sect->u.single.parent    = NULL;
        sect->u.single.par_entry = 0;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__space_revert_root_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__space_revert_root
 *
 * Purpose:	Reset 'parent' pointers in sections, when the heap is
 *		changing from having a root indirect block to a direct block.
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__space_revert_root(const H5HF_hdr_t *hdr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);

    /* Only need to scan the sections if the free space has been initialized */
    if (hdr->fspace)
        /* Iterate over all sections, resetting the parent pointers in 'single' sections */
        if (H5FS_sect_iterate(hdr->f, hdr->fspace, H5HF__space_revert_root_cb, NULL) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_BADITER, FAIL,
                        "can't iterate over sections to reset parent pointers");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__space_revert_root() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__space_create_root_cb
 *
 * Purpose:	Callback routine from iterator, to set 'parent' pointers in
 *		sections to newly created root indirect block, when the heap
 *		is changing from having a root direct block to an indirect block.
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__space_create_root_cb(H5FS_section_info_t *_sect, void *_udata)
{
    H5HF_free_section_t *sect        = (H5HF_free_section_t *)_sect; /* Section to dump info */
    H5HF_indirect_t     *root_iblock = (H5HF_indirect_t *)_udata;    /* User data for callback */
    herr_t               ret_value   = SUCCEED;                      /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(sect);
    assert(root_iblock);

    /* Sanity check sections */
    /* (If we are switching from a direct block for the root block of the heap, */
    /*	there should only be 'single' type sections. -QAK) */
    assert(sect->sect_info.type == H5HF_FSPACE_SECT_SINGLE);

    /* Increment ref. count on new root indirect block */
    if (H5HF__iblock_incr(root_iblock) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINC, FAIL,
                    "can't increment reference count on section's indirect block");

    /* Set parent info ("live" section must _NOT_ have a parent right now) */
    if (sect->sect_info.state == H5FS_SECT_SERIALIZED)
        sect->sect_info.state = H5FS_SECT_LIVE; /* Mark "live" now */
    else
        assert(!sect->u.single.parent);
    sect->u.single.parent    = root_iblock;
    sect->u.single.par_entry = 0;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__space_create_root_cb() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__space_create_root
 *
 * Purpose:	Set 'parent' pointers in sections to new indirect block, when
 *		the heap is changing from having a root direct block to a
 *		indirect block.
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__space_create_root(const H5HF_hdr_t *hdr, H5HF_indirect_t *root_iblock)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(root_iblock);

    /* Only need to scan the sections if the free space has been initialized */
    if (hdr->fspace)
        /* Iterate over all sections, setting the parent pointers in 'single' sections to the new indirect
         * block */
        if (H5FS_sect_iterate(hdr->f, hdr->fspace, H5HF__space_create_root_cb, root_iblock) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_BADITER, FAIL, "can't iterate over sections to set parent pointers");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__space_create_root() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__space_size
 *
 * Purpose:	Query the size of the heap's free space info on disk
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__space_size(H5HF_hdr_t *hdr, hsize_t *fs_size)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(fs_size);

    /* Check if the free space for the heap has been initialized */
    if (!hdr->fspace)
        if (H5HF__space_start(hdr, false) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't initialize heap free space");

    /* Get free space metadata size */
    if (hdr->fspace) {
        if (H5FS_size(hdr->fspace, fs_size) < 0)
            HGOTO_ERROR(H5E_FSPACE, H5E_CANTGET, FAIL, "can't retrieve FS meta storage info");
    } /* end if */
    else
        *fs_size = 0;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__space_size() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__space_remove
 *
 * Purpose:	Remove a section from the free space for the heap
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__space_remove(H5HF_hdr_t *hdr, H5HF_free_section_t *node)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(hdr->fspace);
    assert(node);

    /* Remove from the free space for the heap */
    if (H5FS_sect_remove(hdr->f, hdr->fspace, (H5FS_section_info_t *)node) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTREMOVE, FAIL, "can't remove section from heap free space");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__space_remove() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__space_close
 *
 * Purpose:	Close the free space for the heap
 *
 * Return:	Success:	non-negative
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__space_close(H5HF_hdr_t *hdr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);

    /* Check if the free space was ever opened */
    if (hdr->fspace) {
        hsize_t nsects; /* Number of sections for this heap */

        /* Retrieve the number of sections for this heap */
        if (H5FS_sect_stats(hdr->fspace, NULL, &nsects) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTCOUNT, FAIL, "can't query free space section count");

        /* Close the free space for the heap */
        if (H5FS_close(hdr->f, hdr->fspace) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTRELEASE, FAIL, "can't release free space info");
        hdr->fspace = NULL;

        /* Check if we can delete the free space manager for this heap */
        if (!nsects) {
            if (H5FS_delete(hdr->f, hdr->fs_addr) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTDELETE, FAIL, "can't delete free space info");
            hdr->fs_addr = HADDR_UNDEF;
        } /* end if */
    }     /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__space_close() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__space_delete
 *
 * Purpose:	Delete the free space manager for the heap
 *
 * Return:	Success:	non-negative
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__space_delete(H5HF_hdr_t *hdr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);

    /* Delete the free space manager */
    if (H5FS_delete(hdr->f, hdr->fs_addr) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "can't delete to free space manager");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__space_delete() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__space_sect_change_class
 *
 * Purpose:	Change a section's class
 *
 * Return:	Success:	non-negative
 *
 *		Failure:	negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__space_sect_change_class(H5HF_hdr_t *hdr, H5HF_free_section_t *sect, uint16_t new_class)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(hdr->fspace);
    assert(sect);

    /* Notify the free space manager that a section has changed class */
    if (H5FS_sect_change_class(hdr->f, hdr->fspace, (H5FS_section_info_t *)sect, new_class) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTMODIFY, FAIL, "can't modify class of free space section");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__space_sect_change_class() */
