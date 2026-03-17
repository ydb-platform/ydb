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
 * Purpose: Single Chunk I/O functions.
 *          This is used when the dataset has only 1 chunk (with or without filter):
 *          cur_dims[] is equal to max_dims[] is equal to the chunk dims[]
 *          non-filter chunk record: [address of the chunk]
 *          filtered chunk record: 	[address of the chunk, chunk size, filter mask]
 */

/****************/
/* Module Setup */
/****************/

#include "H5Dmodule.h" /* This source code file is part of the H5D module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                    */
#include "H5Dpkg.h"      /* Datasets                             */
#include "H5Eprivate.h"  /* Error handling                       */
#include "H5FLprivate.h" /* Free Lists                           */
#include "H5MFprivate.h" /* File space management                */
#include "H5VMprivate.h" /* Vector functions                     */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Local Prototypes */
/********************/

/* Single Chunk Index chunking I/O ops */
static herr_t H5D__single_idx_init(const H5D_chk_idx_info_t *idx_info, const H5S_t *space,
                                   haddr_t dset_ohdr_addr);
static herr_t H5D__single_idx_create(const H5D_chk_idx_info_t *idx_info);
static herr_t H5D__single_idx_open(const H5D_chk_idx_info_t *idx_info);
static herr_t H5D__single_idx_close(const H5D_chk_idx_info_t *idx_info);
static herr_t H5D__single_idx_is_open(const H5D_chk_idx_info_t *idx_info, bool *is_open);
static bool   H5D__single_idx_is_space_alloc(const H5O_storage_chunk_t *storage);
static herr_t H5D__single_idx_insert(const H5D_chk_idx_info_t *idx_info, H5D_chunk_ud_t *udata,
                                     const H5D_t *dset);
static herr_t H5D__single_idx_get_addr(const H5D_chk_idx_info_t *idx_info, H5D_chunk_ud_t *udata);
static herr_t H5D__single_idx_load_metadata(const H5D_chk_idx_info_t *idx_info);
static int    H5D__single_idx_iterate(const H5D_chk_idx_info_t *idx_info, H5D_chunk_cb_func_t chunk_cb,
                                      void *chunk_udata);
static herr_t H5D__single_idx_remove(const H5D_chk_idx_info_t *idx_info, H5D_chunk_common_ud_t *udata);
static herr_t H5D__single_idx_delete(const H5D_chk_idx_info_t *idx_info);
static herr_t H5D__single_idx_copy_setup(const H5D_chk_idx_info_t *idx_info_src,
                                         const H5D_chk_idx_info_t *idx_info_dst);
static herr_t H5D__single_idx_size(const H5D_chk_idx_info_t *idx_info, hsize_t *size);
static herr_t H5D__single_idx_reset(H5O_storage_chunk_t *storage, bool reset_addr);
static herr_t H5D__single_idx_dump(const H5O_storage_chunk_t *storage, FILE *stream);

/*********************/
/* Package Variables */
/*********************/

/* Non Index chunk I/O ops */
const H5D_chunk_ops_t H5D_COPS_SINGLE[1] = {{
    false,                          /* Single Chunk indexing doesn't current support SWMR access */
    H5D__single_idx_init,           /* init */
    H5D__single_idx_create,         /* create */
    H5D__single_idx_open,           /* open */
    H5D__single_idx_close,          /* close */
    H5D__single_idx_is_open,        /* is_open */
    H5D__single_idx_is_space_alloc, /* is_space_alloc */
    H5D__single_idx_insert,         /* insert */
    H5D__single_idx_get_addr,       /* get_addr */
    H5D__single_idx_load_metadata,  /* load_metadata */
    NULL,                           /* resize */
    H5D__single_idx_iterate,        /* iterate */
    H5D__single_idx_remove,         /* remove */
    H5D__single_idx_delete,         /* delete */
    H5D__single_idx_copy_setup,     /* copy_setup */
    NULL,                           /* copy_shutdown */
    H5D__single_idx_size,           /* size */
    H5D__single_idx_reset,          /* reset */
    H5D__single_idx_dump,           /* dump */
    NULL                            /* destroy */
}};

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:    H5D__single_idx_init
 *
 * Purpose:     Initialize the indexing information for a dataset.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__single_idx_init(const H5D_chk_idx_info_t *idx_info, const H5S_t H5_ATTR_UNUSED *space,
                     haddr_t H5_ATTR_UNUSED dset_ohdr_addr)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);

    if (idx_info->pline->nused) {
        idx_info->layout->flags |= H5O_LAYOUT_CHUNK_SINGLE_INDEX_WITH_FILTER;

        if (!H5_addr_defined(idx_info->storage->idx_addr)) {
            idx_info->storage->u.single.nbytes      = 0;
            idx_info->storage->u.single.filter_mask = 0;
        }
    }
    else
        idx_info->layout->flags = 0;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__single_idx_init() */

/*-------------------------------------------------------------------------
 * Function:    H5D__single_idx_create
 *
 * Purpose:     Set up Single Chunk Index: filtered or non-filtered
 *
 * Return:      Non-negative on success
 *              Negative on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__single_idx_create(const H5D_chk_idx_info_t *idx_info)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);
    assert(idx_info->layout->max_nchunks == idx_info->layout->nchunks);
    assert(idx_info->layout->nchunks == 1);
    assert(!H5_addr_defined(idx_info->storage->idx_addr));

    if (idx_info->pline->nused)
        assert(idx_info->layout->flags & H5O_LAYOUT_CHUNK_SINGLE_INDEX_WITH_FILTER);
    else
        assert(!(idx_info->layout->flags & H5O_LAYOUT_CHUNK_SINGLE_INDEX_WITH_FILTER));

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__single_idx_create() */

/*-------------------------------------------------------------------------
 * Function:    H5D__single_idx_open
 *
 * Purpose:     Opens an existing "single" index. Currently a no-op.
 *
 * Return:      SUCCEED (can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__single_idx_open(const H5D_chk_idx_info_t H5_ATTR_UNUSED *idx_info)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* NO OP */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__single_idx_open() */

/*-------------------------------------------------------------------------
 * Function:    H5D__single_idx_close
 *
 * Purpose:     Closes an existing "single" index. Currently a no-op.
 *
 * Return:      SUCCEED (can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__single_idx_close(const H5D_chk_idx_info_t H5_ATTR_UNUSED *idx_info)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* NO OP */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__single_idx_close() */

/*-------------------------------------------------------------------------
 * Function:    H5D__single_idx_is_open
 *
 * Purpose:     Query if the index is opened or not
 *
 * Return:      SUCCEED (can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__single_idx_is_open(const H5D_chk_idx_info_t H5_ATTR_NDEBUG_UNUSED *idx_info, bool *is_open)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(idx_info);
    assert(idx_info->storage);
    assert(H5D_CHUNK_IDX_SINGLE == idx_info->storage->idx_type);
    assert(is_open);

    *is_open = true;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__single_idx_is_open() */

/*-------------------------------------------------------------------------
 * Function:    H5D__single_idx_is_space_alloc
 *
 * Purpose:     Query if space is allocated for the single chunk
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static bool
H5D__single_idx_is_space_alloc(const H5O_storage_chunk_t *storage)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(storage);

    FUNC_LEAVE_NOAPI((bool)H5_addr_defined(storage->idx_addr))
} /* end H5D__single_idx_is_space_alloc() */

/*-------------------------------------------------------------------------
 * Function:    H5D__single_idx_insert
 *
 * Purpose:     Allocate space for the single chunk
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__single_idx_insert(const H5D_chk_idx_info_t *idx_info, H5D_chunk_ud_t *udata, const H5D_t *dset)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);
    assert(idx_info->layout->nchunks == 1);
    assert(idx_info->layout->max_nchunks == 1);
    assert(udata);

    /* Set the address for the chunk */
    assert(H5_addr_defined(udata->chunk_block.offset));
    idx_info->storage->idx_addr = udata->chunk_block.offset;

    if (idx_info->pline->nused > 0) {
        H5_CHECKED_ASSIGN(idx_info->storage->u.single.nbytes, uint32_t, udata->chunk_block.length, hsize_t);
        idx_info->storage->u.single.filter_mask = udata->filter_mask;
    } /* end if */

    if (dset)
        if (dset->shared->dcpl_cache.fill.alloc_time != H5D_ALLOC_TIME_EARLY || idx_info->pline->nused > 0)
            /* Mark the layout dirty so that the address of the single chunk will be flushed later */
            if (H5D__mark(dset, H5D_MARK_LAYOUT) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "unable to mark layout as dirty");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__single_idx_insert() */

/*-------------------------------------------------------------------------
 * Function:    H5D__single_idx_get_addr
 *
 * Purpose:     Get the file address of a chunk.
 *              Save the retrieved information in the udata supplied.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__single_idx_get_addr(const H5D_chk_idx_info_t *idx_info, H5D_chunk_ud_t *udata)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);
    assert(idx_info->layout->nchunks == 1);
    assert(idx_info->layout->max_nchunks == 1);
    assert(udata);

    udata->chunk_block.offset = idx_info->storage->idx_addr;
    if (idx_info->layout->flags & H5O_LAYOUT_CHUNK_SINGLE_INDEX_WITH_FILTER) {
        udata->chunk_block.length = idx_info->storage->u.single.nbytes;
        udata->filter_mask        = idx_info->storage->u.single.filter_mask;
    } /* end if */
    else {
        udata->chunk_block.length = idx_info->layout->size;
        udata->filter_mask        = 0;
    } /* end else */
    if (!H5_addr_defined(udata->chunk_block.offset))
        udata->chunk_block.length = 0;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5D__single_idx_get_addr() */

/*-------------------------------------------------------------------------
 * Function:    H5D__single_idx_load_metadata
 *
 * Purpose:     Load additional chunk index metadata beyond the chunk index
 *              itself. Currently a no-op.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__single_idx_load_metadata(const H5D_chk_idx_info_t H5_ATTR_UNUSED *idx_info)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* NO OP */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5D__single_idx_load_metadata() */

/*-------------------------------------------------------------------------
 * Function:    H5D__single_idx_iterate
 *
 * Purpose:     Make callback for the single chunk
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static int
H5D__single_idx_iterate(const H5D_chk_idx_info_t *idx_info, H5D_chunk_cb_func_t chunk_cb, void *chunk_udata)
{
    H5D_chunk_rec_t chunk_rec;      /* generic chunk record  */
    int             ret_value = -1; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);
    assert(chunk_cb);
    assert(chunk_udata);
    assert(H5_addr_defined(idx_info->storage->idx_addr));

    /* Initialize generic chunk record */
    memset(&chunk_rec, 0, sizeof(chunk_rec));
    chunk_rec.chunk_addr = idx_info->storage->idx_addr;

    if (idx_info->layout->flags & H5O_LAYOUT_CHUNK_SINGLE_INDEX_WITH_FILTER) {
        chunk_rec.nbytes      = idx_info->storage->u.single.nbytes;
        chunk_rec.filter_mask = idx_info->storage->u.single.filter_mask;
    } /* end if */
    else {
        chunk_rec.nbytes      = idx_info->layout->size;
        chunk_rec.filter_mask = 0;
    } /* end else */

    /* Make "generic chunk" callback */
    if ((ret_value = (*chunk_cb)(&chunk_rec, chunk_udata)) < 0)
        HERROR(H5E_DATASET, H5E_CALLBACK, "failure in generic chunk iterator callback");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__single_idx_iterate() */

/*-------------------------------------------------------------------------
 * Function:    H5D__single_idx_remove
 *
 * Purpose:     Remove the single chunk
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__single_idx_remove(const H5D_chk_idx_info_t *idx_info, H5D_chunk_common_ud_t H5_ATTR_UNUSED *udata)
{
    hsize_t nbytes;              /* Size of all chunks */
    herr_t  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);
    assert(H5_addr_defined(idx_info->storage->idx_addr));

    if (idx_info->layout->flags & H5O_LAYOUT_CHUNK_SINGLE_INDEX_WITH_FILTER)
        nbytes = idx_info->storage->u.single.nbytes;
    else
        nbytes = idx_info->layout->size;

    if (H5MF_xfree(idx_info->f, H5FD_MEM_DRAW, idx_info->storage->idx_addr, nbytes) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTFREE, H5_ITER_ERROR, "unable to free dataset chunks");

    idx_info->storage->idx_addr = HADDR_UNDEF;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__single_idx_remove() */

/*-------------------------------------------------------------------------
 * Function:    H5D__single_idx_delete
 *
 * Purpose:     Delete raw data storage for entire dataset (i.e. the only
 *              chunk)
 *
 * Return:      Success:    Non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__single_idx_delete(const H5D_chk_idx_info_t *idx_info)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);

    if (H5_addr_defined(idx_info->storage->idx_addr))
        ret_value = H5D__single_idx_remove(idx_info, NULL);
    else
        assert(!H5_addr_defined(idx_info->storage->idx_addr));

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__single_idx_delete() */

/*-------------------------------------------------------------------------
 * Function:    H5D__single_idx_copy_setup
 *
 * Purpose:     Set up any necessary information for copying the single
 *              chunk
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__single_idx_copy_setup(const H5D_chk_idx_info_t H5_ATTR_NDEBUG_UNUSED *idx_info_src,
                           const H5D_chk_idx_info_t                       *idx_info_dst)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(idx_info_src);
    assert(idx_info_src->f);
    assert(idx_info_src->pline);
    assert(idx_info_src->layout);
    assert(idx_info_src->storage);
    assert(H5_addr_defined(idx_info_src->storage->idx_addr));

    assert(idx_info_dst);
    assert(idx_info_dst->f);
    assert(idx_info_dst->pline);
    assert(idx_info_dst->layout);
    assert(idx_info_dst->storage);

    /* Set copied metadata tag */
    H5_BEGIN_TAG(H5AC__COPIED_TAG)

    /* Set up information at the destination file */
    if (H5D__single_idx_create(idx_info_dst) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to initialize chunked storage");

    /* Reset metadata tag */
    H5_END_TAG

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__single_idx_copy_setup() */

/*-------------------------------------------------------------------------
 * Function:    H5D__single_idx_size
 *
 * Purpose:     Retrieve the amount of index storage for the chunked dataset
 *
 * Return:      Success:        Non-negative
 *              Failure:        negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__single_idx_size(const H5D_chk_idx_info_t H5_ATTR_UNUSED *idx_info, hsize_t *index_size)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(index_size);

    *index_size = 0;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__single_idx_size() */

/*-------------------------------------------------------------------------
 * Function:    H5D__single_idx_reset
 *
 * Purpose:     Reset indexing information.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__single_idx_reset(H5O_storage_chunk_t *storage, bool reset_addr)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(storage);

    /* Reset index info */
    if (reset_addr)
        storage->idx_addr = HADDR_UNDEF;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__single_idx_reset() */

/*-------------------------------------------------------------------------
 * Function:    H5D__single_idx_dump
 *
 * Purpose:     Dump the address of the single chunk
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__single_idx_dump(const H5O_storage_chunk_t *storage, FILE *stream)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(storage);
    assert(stream);

    fprintf(stream, "    Address: %" PRIuHADDR "\n", storage->idx_addr);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__single_idx_dump() */
