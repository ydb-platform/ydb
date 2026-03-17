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

/* High-level library internal header file */
#include "H5HLprivate2.h"

/* public LT prototypes			*/
#include "H5DOpublic.h"

#ifndef H5_NO_DEPRECATED_SYMBOLS

/*-------------------------------------------------------------------------
 * Function:    H5DOwrite_chunk
 *
 * Purpose:     Writes an entire chunk to the file directly.
 *
 *              The H5DOwrite_chunk() call was moved to H5Dwrite_chunk. This
 *              simple wrapper remains so that people can still link to the
 *              high-level library without changing their code.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5DOwrite_chunk(hid_t dset_id, hid_t dxpl_id, uint32_t filters, const hsize_t *offset, size_t data_size,
                const void *buf)
{
    /* Call underlying H5D function */
    if (H5Dwrite_chunk(dset_id, dxpl_id, filters, offset, data_size, buf) < 0)
        return FAIL;
    else
        return SUCCEED;

} /* end H5DOwrite_chunk() */

/*-------------------------------------------------------------------------
 * Function:    H5DOread_chunk
 *
 * Purpose:     Reads an entire chunk from the file directly.
 *
 *              The H5DOread_chunk() call was moved to H5Dread_chunk. This
 *              simple wrapper remains so that people can still link to the
 *              high-level library without changing their code.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5DOread_chunk(hid_t dset_id, hid_t dxpl_id, const hsize_t *offset, uint32_t *filters, void *buf)
{
    /* Call underlying H5D function */
    if (H5Dread_chunk(dset_id, dxpl_id, offset, filters, buf) < 0)
        return FAIL;
    else
        return SUCCEED;
} /* end H5DOread_chunk() */

#endif /* H5_NO_DEPRECATED_SYMBOLS */

/*-------------------------------------------------------------------------
 * Function:    H5DOappend()
 *
 * Purpose:     To append elements to a dataset.
 *
 *      axis:       the dataset dimension (zero-based) for the append
 *      extension:  the # of elements to append for the axis-th dimension
 *      memtype:    the datatype
 *      buf:        buffer with data for the append
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Note:
 * 	This routine is copied from the fast forward feature branch: features/hdf5_ff
 *	src/H5FF.c:H5DOappend() with the following modifications:
 * 	1) Remove and replace macro calls such as
 *		FUNC_ENTER_API, H5TRACE, HGOTO_ERROR
 * 	   accordingly because hl does not have these macros
 *	2) Replace H5I_get_type() by H5Iget_type()
 *	3) Replace H5P_isa_class() by H5Pisa_class()
 *	4) Fix a bug in the following: replace extension by size[axis]
 *		if(extension < old_size) {
 *		  ret_value = FAIL;
 *		  goto done;
 *   		}
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5DOappend(hid_t dset_id, hid_t dxpl_id, unsigned axis, size_t extension, hid_t memtype, const void *buf)
{
    hsize_t  size[H5S_MAX_RANK];  /* The new size (after extension */
    hsize_t  old_size = 0;        /* The size of the dimension to be extended */
    int      sndims;              /* Number of dimensions in dataspace (signed) */
    unsigned ndims;               /* Number of dimensions in dataspace */
    hid_t    space_id     = FAIL; /* Old file space */
    hid_t    new_space_id = FAIL; /* New file space (after extension) */
    hid_t    mem_space_id = FAIL; /* Memory space for data buffer */
    hssize_t snelmts;             /* Number of elements in selection (signed) */
    hsize_t  nelmts;              /* Number of elements in selection */
    hid_t    dapl = FAIL;         /* Dataset access property list */

    hsize_t start[H5S_MAX_RANK];  /* H5Sselect_Hyperslab: starting offset */
    hsize_t count[H5S_MAX_RANK];  /* H5Sselect_hyperslab: # of blocks to select */
    hsize_t stride[H5S_MAX_RANK]; /* H5Sselect_hyperslab: # of elements to move when selecting */
    hsize_t block[H5S_MAX_RANK];  /* H5Sselect_hyperslab: # of elements in a block */

    hsize_t        *boundary = NULL;  /* Boundary set in append flush property */
    H5D_append_cb_t append_cb;        /* Callback function set in append flush property */
    void           *udata;            /* User data set in append flush property */
    bool            hit = false;      /* Boundary is hit or not */
    hsize_t         k;                /* Local index variable */
    unsigned        u;                /* Local index variable */
    herr_t          ret_value = FAIL; /* Return value */

    /* check arguments */
    if (H5I_DATASET != H5Iget_type(dset_id))
        goto done;

    /* If the user passed in a default DXPL, sanity check it */
    if (H5P_DEFAULT != dxpl_id)
        if (true != H5Pisa_class(dxpl_id, H5P_DATASET_XFER))
            goto done;

    /* Get the dataspace of the dataset */
    if (FAIL == (space_id = H5Dget_space(dset_id)))
        goto done;

    /* Get the rank of this dataspace */
    if ((sndims = H5Sget_simple_extent_ndims(space_id)) < 0)
        goto done;
    ndims = (unsigned)sndims;

    /* Verify correct axis */
    if (axis >= ndims)
        goto done;

    /* Get the dimensions sizes of the dataspace */
    if (H5Sget_simple_extent_dims(space_id, size, NULL) < 0)
        goto done;

    /* Adjust the dimension size of the requested dimension,
     * but first record the old dimension size
     */
    old_size = size[axis];
    size[axis] += extension;
    if (size[axis] < old_size)
        goto done;

    /* Set the extent of the dataset to the new dimension */
    if (H5Dset_extent(dset_id, size) < 0)
        goto done;

    /* Get the new dataspace of the dataset */
    if (FAIL == (new_space_id = H5Dget_space(dset_id)))
        goto done;

    /* Select a hyperslab corresponding to the append operation */
    for (u = 0; u < ndims; u++) {
        start[u]  = 0;
        stride[u] = 1;
        count[u]  = size[u];
        block[u]  = 1;
        if (u == axis) {
            count[u] = extension;
            start[u] = old_size;
        } /* end if */
    }     /* end for */
    if (FAIL == H5Sselect_hyperslab(new_space_id, H5S_SELECT_SET, start, stride, count, block))
        goto done;

    /* The # of elements in the new extended dataspace */
    if ((snelmts = H5Sget_select_npoints(new_space_id)) < 0)
        goto done;
    nelmts = (hsize_t)snelmts;

    /* create a memory space */
    if (FAIL == (mem_space_id = H5Screate_simple(1, &nelmts, NULL)))
        goto done;

    /* Write the data */
    if (H5Dwrite(dset_id, memtype, mem_space_id, new_space_id, dxpl_id, buf) < 0)
        goto done;

    /* Obtain the dataset's access property list */
    if ((dapl = H5Dget_access_plist(dset_id)) < 0)
        goto done;

    /* Allocate the boundary array */
    boundary = (hsize_t *)malloc(ndims * sizeof(hsize_t));

    /* Retrieve the append flush property */
    if (H5Pget_append_flush(dapl, ndims, boundary, &append_cb, &udata) < 0)
        goto done;

    /* No boundary for this axis */
    if (boundary[axis] != 0) {

        /* Determine whether a boundary is hit or not */
        for (k = start[axis]; k < size[axis]; k++)
            if (!((k + 1) % boundary[axis])) {
                hit = true;
                break;
            }

        if (hit) { /* Hit the boundary */
            /* Invoke callback if there is one */
            if (append_cb && append_cb(dset_id, size, udata) < 0)
                goto done;

            /* Do a dataset flush */
            if (H5Dflush(dset_id) < 0)
                goto done;
        } /* end if */
    }     /* end if */

    /* Indicate success */
    ret_value = SUCCEED;

done:
    /* Close old dataspace */
    if (space_id != FAIL && H5Sclose(space_id) < 0)
        ret_value = FAIL;

    /* Close new dataspace */
    if (new_space_id != FAIL && H5Sclose(new_space_id) < 0)
        ret_value = FAIL;

    /* Close memory dataspace */
    if (mem_space_id != FAIL && H5Sclose(mem_space_id) < 0)
        ret_value = FAIL;

    /* Close the dataset access property list */
    if (dapl != FAIL && H5Pclose(dapl) < 0)
        ret_value = FAIL;

    if (boundary)
        free(boundary);

    return ret_value;
} /* H5DOappend() */
