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
 * Created:		H5HFman.c
 *
 * Purpose:		"Managed" object routines for fractal heaps.
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

/* Macro to check if we can apply all filters in the pipeline.  Use whenever
 * performing a modification operation */
#define H5HF_MAN_WRITE_CHECK_PLINE(HDR)                                                                      \
    {                                                                                                        \
        if (!((HDR)->checked_filters)) {                                                                     \
            if ((HDR)->pline.nused)                                                                          \
                if (H5Z_can_apply_direct(&((HDR)->pline)) < 0)                                               \
                    HGOTO_ERROR(H5E_ARGS, H5E_CANTINIT, FAIL, "I/O filters can't operate on this heap");     \
                                                                                                             \
            (HDR)->checked_filters = true;                                                                   \
        } /* end if */                                                                                       \
    }

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/
static herr_t H5HF__man_op_real(H5HF_hdr_t *hdr, const uint8_t *id, H5HF_operator_t op, void *op_data,
                                unsigned op_flags);

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
 * Function:	H5HF__man_insert
 *
 * Purpose:	Insert an object in a managed direct block
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_insert(H5HF_hdr_t *hdr, size_t obj_size, const void *obj, void *_id)
{
    H5HF_free_section_t *sec_node    = NULL;        /* Pointer to free space section */
    H5HF_direct_t       *dblock      = NULL;        /* Pointer to direct block to modify */
    haddr_t              dblock_addr = HADDR_UNDEF; /* Direct block address */
    size_t               dblock_size;               /* Direct block size */
    uint8_t             *id = (uint8_t *)_id;       /* Pointer to ID buffer */
    size_t               blk_off;                   /* Offset of object within block */
    htri_t               node_found;                /* Whether an existing free list node was found */
    herr_t               ret_value = SUCCEED;       /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(obj_size > 0);
    assert(obj);
    assert(id);

    /* Check pipeline */
    H5HF_MAN_WRITE_CHECK_PLINE(hdr)

    /* Look for free space */
    if ((node_found = H5HF__space_find(hdr, (hsize_t)obj_size, &sec_node)) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, FAIL, "can't locate free space in fractal heap");

    /* If we didn't find a node, go create a direct block big enough to hold the requested block */
    if (!node_found)
        /* Allocate direct block big enough to hold requested size */
        if (H5HF__man_dblock_new(hdr, obj_size, &sec_node) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTCREATE, FAIL, "can't create fractal heap direct block");

    /* Check for row section */
    if (sec_node->sect_info.type == H5HF_FSPACE_SECT_FIRST_ROW ||
        sec_node->sect_info.type == H5HF_FSPACE_SECT_NORMAL_ROW) {

        /* Allocate 'single' selection out of 'row' selection */
        if (H5HF__man_iblock_alloc_row(hdr, &sec_node) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTALLOC, FAIL, "can't break up row section");
    } /* end if */
    assert(sec_node->sect_info.type == H5HF_FSPACE_SECT_SINGLE);

    /* Check for 'single' section being serialized */
    if (sec_node->sect_info.state == H5FS_SECT_SERIALIZED)
        if (H5HF__sect_single_revive(hdr, sec_node) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't revive single free section");
    assert(sec_node->sect_info.state == H5FS_SECT_LIVE);

    /* Retrieve direct block address from section */
    if (H5HF__sect_single_dblock_info(hdr, sec_node, &dblock_addr, &dblock_size) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTGET, FAIL, "can't retrieve direct block information");

    /* Lock direct block */
    if (NULL == (dblock = H5HF__man_dblock_protect(hdr, dblock_addr, dblock_size, sec_node->u.single.parent,
                                                   sec_node->u.single.par_entry, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to load fractal heap direct block");

    /* Insert object into block */

    /* Get the offset of the object within the block */
    H5_CHECK_OVERFLOW((sec_node->sect_info.addr - dblock->block_off), hsize_t, size_t);
    blk_off = (size_t)(sec_node->sect_info.addr - dblock->block_off);

    /* Sanity checks */
    assert(sec_node->sect_info.size >= obj_size);

    /* Reduce (& possibly re-add) single section */
    if (H5HF__sect_single_reduce(hdr, sec_node, obj_size) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTSHRINK, FAIL, "can't reduce single section node");
    sec_node = NULL;

    /* Encode the object in the block */
    {
        uint8_t *p; /* Temporary pointer to obj info in block */

        /* Point to location for object */
        p = dblock->blk + blk_off;

        /* Copy the object's data into the heap */
        H5MM_memcpy(p, obj, obj_size);
        p += obj_size;

        /* Sanity check */
        assert((size_t)(p - (dblock->blk + blk_off)) == obj_size);
    } /* end block */

    /* Set the heap ID for the new object (heap offset & obj length) */
    H5HF_MAN_ID_ENCODE(id, hdr, (dblock->block_off + blk_off), obj_size);

    /* Update statistics about heap */
    hdr->man_nobjs++;

    /* Reduce space available in heap (marks header dirty) */
    if (H5HF__hdr_adj_free(hdr, -(ssize_t)obj_size) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTDEC, FAIL, "can't adjust free space for heap");

done:
    /* Release section node on error */
    if (ret_value < 0)
        if (sec_node && H5HF__sect_single_free((H5FS_section_info_t *)sec_node) < 0)
            HDONE_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "unable to release section node");

    /* Release the direct block (marked as dirty) */
    if (dblock && H5AC_unprotect(hdr->f, H5AC_FHEAP_DBLOCK, dblock_addr, dblock, H5AC__DIRTIED_FLAG) < 0)
        HDONE_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to release fractal heap direct block");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_insert() */

/*-------------------------------------------------------------------------
 * Function:    H5HF__man_get_obj_len
 *
 * Purpose:     Get the size of a managed heap object
 *
 * Return:      SUCCEED (Can't fail)
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_get_obj_len(H5HF_hdr_t *hdr, const uint8_t *id, size_t *obj_len_p)
{

    FUNC_ENTER_PACKAGE_NOERR

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(id);
    assert(obj_len_p);

    /* Skip over the flag byte */
    id++;

    /* Skip over object offset */
    id += hdr->heap_off_size;

    /* Retrieve the entry length */
    UINT64DECODE_VAR(id, *obj_len_p, hdr->heap_len_size);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5HF__man_get_obj_len() */

/*-------------------------------------------------------------------------
 * Function:    H5HF__man_get_obj_off
 *
 * Purpose:     Get the offset of a managed heap object
 *
 * Return:      SUCCEED (Can't fail)
 *
 *-------------------------------------------------------------------------
 */
void
H5HF__man_get_obj_off(const H5HF_hdr_t *hdr, const uint8_t *id, hsize_t *obj_off_p)
{
    FUNC_ENTER_PACKAGE_NOERR

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(id);
    assert(obj_off_p);

    /* Skip over the flag byte */
    id++;

    /* Skip over object offset */
    UINT64DECODE_VAR(id, *obj_off_p, hdr->heap_off_size);

    FUNC_LEAVE_NOAPI_VOID
} /* end H5HF__man_get_obj_off() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_op_real
 *
 * Purpose:	Internal routine to perform an operation on a managed heap
 *              object
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5HF__man_op_real(H5HF_hdr_t *hdr, const uint8_t *id, H5HF_operator_t op, void *op_data, unsigned op_flags)
{
    H5HF_direct_t *dblock = NULL;       /* Pointer to direct block to query */
    unsigned       dblock_access_flags; /* Access method for direct block */
                                        /* must equal either
                                         * H5AC__NO_FLAGS_SET or
                                         * H5AC__READ_ONLY_FLAG
                                         */
    haddr_t  dblock_addr = HADDR_UNDEF; /* Direct block address */
    size_t   dblock_size;               /* Direct block size */
    unsigned dblock_cache_flags = 0;    /* Flags for unprotecting direct block */
    hsize_t  obj_off;                   /* Object's offset in heap */
    size_t   obj_len;                   /* Object's length in heap */
    size_t   blk_off;                   /* Offset of object in block */
    uint8_t *p;                         /* Temporary pointer to obj info in block */
    herr_t   ret_value = SUCCEED;       /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(id);
    assert(op);

    /* Set the access mode for the direct block */
    if (op_flags & H5HF_OP_MODIFY) {
        /* Check pipeline */
        H5HF_MAN_WRITE_CHECK_PLINE(hdr)

        dblock_access_flags = H5AC__NO_FLAGS_SET;
        dblock_cache_flags  = H5AC__DIRTIED_FLAG;
    } /* end if */
    else {
        dblock_access_flags = H5AC__READ_ONLY_FLAG;
        dblock_cache_flags  = H5AC__NO_FLAGS_SET;
    } /* end else */

    /* Skip over the flag byte */
    id++;

    /* Decode the object offset within the heap & its length */
    UINT64DECODE_VAR(id, obj_off, hdr->heap_off_size);
    UINT64DECODE_VAR(id, obj_len, hdr->heap_len_size);

    /* Check for bad offset or length */
    if (obj_off == 0)
        HGOTO_ERROR(H5E_HEAP, H5E_BADRANGE, FAIL, "invalid fractal heap offset");
    if (obj_off > hdr->man_size)
        HGOTO_ERROR(H5E_HEAP, H5E_BADRANGE, FAIL, "fractal heap object offset too large");
    if (obj_len == 0)
        HGOTO_ERROR(H5E_HEAP, H5E_BADRANGE, FAIL, "invalid fractal heap object size");
    if (obj_len > hdr->man_dtable.cparam.max_direct_size)
        HGOTO_ERROR(H5E_HEAP, H5E_BADRANGE, FAIL, "fractal heap object size too large for direct block");
    if (obj_len > hdr->max_man_size)
        HGOTO_ERROR(H5E_HEAP, H5E_BADRANGE, FAIL, "fractal heap object should be standalone");

    /* Check for root direct block */
    if (hdr->man_dtable.curr_root_rows == 0) {
        /* Set direct block info */
        dblock_addr = hdr->man_dtable.table_addr;
        dblock_size = hdr->man_dtable.cparam.start_block_size;

        /* Lock direct block */
        if (NULL ==
            (dblock = H5HF__man_dblock_protect(hdr, dblock_addr, dblock_size, NULL, 0, dblock_access_flags)))
            HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to protect fractal heap direct block");
    } /* end if */
    else {
        H5HF_indirect_t *iblock;      /* Pointer to indirect block */
        bool             did_protect; /* Whether we protected the indirect block or not */
        unsigned         entry;       /* Entry of block */

        /* Look up indirect block containing direct block */
        if (H5HF__man_dblock_locate(hdr, obj_off, &iblock, &entry, &did_protect, H5AC__READ_ONLY_FLAG) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTCOMPUTE, FAIL, "can't compute row & column of section");

        /* Set direct block info */
        dblock_addr = iblock->ents[entry].addr;
        H5_CHECK_OVERFLOW((hdr->man_dtable.row_block_size[entry / hdr->man_dtable.cparam.width]), hsize_t,
                          size_t);
        dblock_size = (size_t)hdr->man_dtable.row_block_size[entry / hdr->man_dtable.cparam.width];

        /* Check for offset of invalid direct block */
        if (!H5_addr_defined(dblock_addr)) {
            /* Unlock indirect block */
            if (H5HF__man_iblock_unprotect(iblock, H5AC__NO_FLAGS_SET, did_protect) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL,
                            "unable to release fractal heap indirect block");

            HGOTO_ERROR(H5E_HEAP, H5E_BADRANGE, FAIL, "fractal heap ID not in allocated direct block");
        } /* end if */

        /* Lock direct block */
        if (NULL == (dblock = H5HF__man_dblock_protect(hdr, dblock_addr, dblock_size, iblock, entry,
                                                       dblock_access_flags))) {
            /* Unlock indirect block */
            if (H5HF__man_iblock_unprotect(iblock, H5AC__NO_FLAGS_SET, did_protect) < 0)
                HGOTO_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL,
                            "unable to release fractal heap indirect block");

            HGOTO_ERROR(H5E_HEAP, H5E_CANTPROTECT, FAIL, "unable to protect fractal heap direct block");
        } /* end if */

        /* Unlock indirect block */
        if (H5HF__man_iblock_unprotect(iblock, H5AC__NO_FLAGS_SET, did_protect) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to release fractal heap indirect block");
        iblock = NULL;
    } /* end else */

    /* Compute offset of object within block */
    assert((obj_off - dblock->block_off) < (hsize_t)dblock_size);
    blk_off = (size_t)(obj_off - dblock->block_off);

    /* Check for object's offset in the direct block prefix information */
    if (blk_off < (size_t)H5HF_MAN_ABS_DIRECT_OVERHEAD(hdr))
        HGOTO_ERROR(H5E_HEAP, H5E_BADRANGE, FAIL, "object located in prefix of direct block");

    /* Check for object's length overrunning the end of the direct block */
    if ((blk_off + obj_len) > dblock_size)
        HGOTO_ERROR(H5E_HEAP, H5E_BADRANGE, FAIL, "object overruns end of direct block");

    /* Point to location for object */
    p = dblock->blk + blk_off;

    /* Call the user's 'op' callback */
    if (op(p, obj_len, op_data) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTOPERATE, FAIL, "application's callback failed");

done:
    /* Unlock direct block */
    if (dblock && H5AC_unprotect(hdr->f, H5AC_FHEAP_DBLOCK, dblock_addr, dblock, dblock_cache_flags) < 0)
        HDONE_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to release fractal heap direct block");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_op_real() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_read
 *
 * Purpose:	Read an object from a managed heap
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_read(H5HF_hdr_t *hdr, const uint8_t *id, void *obj)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(id);
    assert(obj);

    /* Call the internal 'op' routine routine */
    if (H5HF__man_op_real(hdr, id, H5HF__op_read, obj, 0) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTOPERATE, FAIL, "unable to operate on heap object");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_read() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_write
 *
 * Purpose:	Write an object to a managed heap
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_write(H5HF_hdr_t *hdr, const uint8_t *id, const void *obj)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(id);
    assert(obj);

    /* Call the internal 'op' routine routine
     *
     * In this case, the callback operation needs to modify the obj buffer that
     * was passed in as const. We quiet the warning here because an obj pointer
     * that was originally const should *never* arrive here.
     */
    H5_GCC_CLANG_DIAG_OFF("cast-qual")
    if (H5HF__man_op_real(hdr, id, H5HF__op_write, (void *)obj, H5HF_OP_MODIFY) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTOPERATE, FAIL, "unable to operate on heap object");
    H5_GCC_CLANG_DIAG_ON("cast-qual")

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_write() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_op
 *
 * Purpose:	Operate directly on an object from a managed heap
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_op(H5HF_hdr_t *hdr, const uint8_t *id, H5HF_operator_t op, void *op_data)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(id);
    assert(op);

    /* Call the internal 'op' routine routine */
    if (H5HF__man_op_real(hdr, id, op, op_data, 0) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTOPERATE, FAIL, "unable to operate on heap object");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_op() */

/*-------------------------------------------------------------------------
 * Function:	H5HF__man_remove
 *
 * Purpose:	Remove an object from a managed heap
 *
 * Return:	SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5HF__man_remove(H5HF_hdr_t *hdr, const uint8_t *id)
{
    H5HF_free_section_t *sec_node    = NULL;  /* Pointer to free space section for block */
    H5HF_indirect_t     *iblock      = NULL;  /* Pointer to indirect block */
    bool                 did_protect = false; /* Whether we protected the indirect block or not */
    hsize_t              obj_off;             /* Object's offset in heap */
    size_t               obj_len;             /* Object's length in heap */
    size_t               dblock_size;         /* Direct block size */
    hsize_t              dblock_block_off;    /* Offset of the direct block within the heap's address space */
    unsigned             dblock_entry;        /* Entry of direct block in parent indirect block */
    size_t               blk_off;             /* Offset of object in block */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(hdr);
    assert(id);

    /* Check pipeline */
    H5HF_MAN_WRITE_CHECK_PLINE(hdr)

    /* Skip over the flag byte */
    id++;

    /* Decode the object offset within the heap & it's length */
    UINT64DECODE_VAR(id, obj_off, hdr->heap_off_size);
    UINT64DECODE_VAR(id, obj_len, hdr->heap_len_size);

    /* Check for bad offset or length */
    if (obj_off == 0)
        HGOTO_ERROR(H5E_HEAP, H5E_BADRANGE, FAIL, "invalid fractal heap offset");
    if (obj_off > hdr->man_size)
        HGOTO_ERROR(H5E_HEAP, H5E_BADRANGE, FAIL, "fractal heap object offset too large");
    if (obj_len == 0)
        HGOTO_ERROR(H5E_HEAP, H5E_BADRANGE, FAIL, "invalid fractal heap object size");
    if (obj_len > hdr->man_dtable.cparam.max_direct_size)
        HGOTO_ERROR(H5E_HEAP, H5E_BADRANGE, FAIL, "fractal heap object size too large for direct block");
    if (obj_len > hdr->max_man_size)
        HGOTO_ERROR(H5E_HEAP, H5E_BADRANGE, FAIL, "fractal heap object should be standalone");

    /* Check for root direct block */
    if (hdr->man_dtable.curr_root_rows == 0) {
        /* Set direct block info */
        dblock_size      = hdr->man_dtable.cparam.start_block_size;
        dblock_block_off = 0;
        dblock_entry     = 0;
    } /* end if */
    else {
        /* Look up indirect block containing direct block */
        if (H5HF__man_dblock_locate(hdr, obj_off, &iblock, &dblock_entry, &did_protect, H5AC__NO_FLAGS_SET) <
            0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTCOMPUTE, FAIL, "can't compute row & column of section");

        /* Check for offset of invalid direct block */
        if (!H5_addr_defined(iblock->ents[dblock_entry].addr))
            HGOTO_ERROR(H5E_HEAP, H5E_BADRANGE, FAIL, "fractal heap ID not in allocated direct block");

        /* Set direct block info */
        H5_CHECK_OVERFLOW((hdr->man_dtable.row_block_size[dblock_entry / hdr->man_dtable.cparam.width]),
                          hsize_t, size_t);
        dblock_size = (size_t)(hdr->man_dtable.row_block_size[dblock_entry / hdr->man_dtable.cparam.width]);

        /* Compute the direct block's offset in the heap's address space */
        /* (based on parent indirect block's block offset) */
        dblock_block_off = iblock->block_off;
        dblock_block_off += hdr->man_dtable.row_block_off[dblock_entry / hdr->man_dtable.cparam.width];
        dblock_block_off += hdr->man_dtable.row_block_size[dblock_entry / hdr->man_dtable.cparam.width] *
                            (dblock_entry % hdr->man_dtable.cparam.width);
    } /* end else */

    /* Compute offset of object within block */
    assert((obj_off - dblock_block_off) < (hsize_t)dblock_size);
    blk_off = (size_t)(obj_off - dblock_block_off);

    /* Check for object's offset in the direct block prefix information */
    if (blk_off < (size_t)H5HF_MAN_ABS_DIRECT_OVERHEAD(hdr))
        HGOTO_ERROR(H5E_HEAP, H5E_BADRANGE, FAIL, "object located in prefix of direct block");

    /* Check for object's length overrunning the end of the direct block */
    if ((blk_off + obj_len) > dblock_size)
        HGOTO_ERROR(H5E_HEAP, H5E_BADRANGE, FAIL, "object overruns end of direct block");

    /* Create free space section node */
    if (NULL == (sec_node = H5HF__sect_single_new(obj_off, obj_len, iblock, dblock_entry)))
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't create section for direct block's free space");

    /* Unlock indirect block */
    if (iblock) {
        if (H5HF__man_iblock_unprotect(iblock, H5AC__NO_FLAGS_SET, did_protect) < 0)
            HGOTO_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to release fractal heap indirect block");
        iblock = NULL;
    } /* end if */

    /* Increase space available in heap (marks header dirty) */
    if (H5HF__hdr_adj_free(hdr, (ssize_t)obj_len) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTDEC, FAIL, "can't adjust free space for heap");

    /* Update statistics about heap */
    hdr->man_nobjs--;

    /* Return free space to the heap's list of space */
    if (H5HF__space_add(hdr, sec_node, H5FS_ADD_RETURNED_SPACE) < 0)
        HGOTO_ERROR(H5E_HEAP, H5E_CANTINIT, FAIL, "can't add direct block free space to global list");
    sec_node = NULL;

done:
    if (ret_value < 0) {
        /* Release section node */
        if (sec_node && H5HF__sect_single_free((H5FS_section_info_t *)sec_node) < 0)
            HDONE_ERROR(H5E_HEAP, H5E_CANTFREE, FAIL, "unable to release section node");
    } /* end if */

    /* Unlock indirect block */
    if (iblock && H5HF__man_iblock_unprotect(iblock, H5AC__NO_FLAGS_SET, did_protect) < 0)
        HDONE_ERROR(H5E_HEAP, H5E_CANTUNPROTECT, FAIL, "unable to release fractal heap indirect block");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5HF__man_remove() */
