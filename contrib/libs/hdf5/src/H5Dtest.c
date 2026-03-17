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
 * Purpose:	Dataset testing functions.
 */

/****************/
/* Module Setup */
/****************/

#include "H5Dmodule.h" /* This source code file is part of the H5D module */
#define H5D_TESTING    /*suppress warning about H5D testing funcs*/

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5Dpkg.h"      /* Datasets                                 */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5VLprivate.h" /* Virtual Object Layer                     */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Local Prototypes */
/********************/

/*********************/
/* Package Variables */
/*********************/

/*******************/
/* Local Variables */
/*******************/

/*--------------------------------------------------------------------------
 NAME
    H5D__layout_version_test
 PURPOSE
    Determine the storage layout version for a dataset's layout information
 USAGE
    herr_t H5D__layout_version_test(did, version)
        hid_t did;              IN: Dataset to query
        unsigned *version;      OUT: Pointer to location to place version info
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Checks the version of the storage layout information for a dataset.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    DO NOT USE THIS FUNCTION FOR ANYTHING EXCEPT TESTING
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5D__layout_version_test(hid_t did, unsigned *version)
{
    H5D_t *dset;                /* Pointer to dataset to query */
    herr_t ret_value = SUCCEED; /* return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    if (NULL == (dset = (H5D_t *)H5VL_object_verify(did, H5I_DATASET)))
        HGOTO_ERROR(H5E_DATASET, H5E_BADTYPE, FAIL, "not a dataset");

    if (version)
        *version = dset->shared->layout.version;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__layout_version_test() */

/*--------------------------------------------------------------------------
 NAME
    H5D__layout_contig_size_test
 PURPOSE
    Determine the size of a contiguous layout for a dataset's layout information
 USAGE
    herr_t H5D__layout_contig_size_test(did, size)
        hid_t did;              IN: Dataset to query
        hsize_t *size;          OUT: Pointer to location to place size info
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Checks the size of a contiguous dataset's storage.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    DO NOT USE THIS FUNCTION FOR ANYTHING EXCEPT TESTING
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5D__layout_contig_size_test(hid_t did, hsize_t *size)
{
    H5D_t *dset;                /* Pointer to dataset to query */
    herr_t ret_value = SUCCEED; /* return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    if (NULL == (dset = (H5D_t *)H5VL_object_verify(did, H5I_DATASET)))
        HGOTO_ERROR(H5E_DATASET, H5E_BADTYPE, FAIL, "not a dataset");

    if (size) {
        assert(dset->shared->layout.type == H5D_CONTIGUOUS);
        *size = dset->shared->layout.storage.u.contig.size;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__layout_contig_size_test() */

/*--------------------------------------------------------------------------
 NAME
    H5D__layout_compact_dirty_test
 PURPOSE
    Determine the "dirty" flag of a compact layout for a dataset's layout information
 USAGE
    herr_t H5D__layout_compact_dirty_test(did, dirty)
        hid_t did;              IN: Dataset to query
        bool *dirty;         OUT: Pointer to location to place "dirty" info
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Checks the "dirty" flag of a compact dataset.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    DO NOT USE THIS FUNCTION FOR ANYTHING EXCEPT TESTING
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5D__layout_compact_dirty_test(hid_t did, bool *dirty)
{
    H5D_t *dset;                /* Pointer to dataset to query */
    herr_t ret_value = SUCCEED; /* return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    if (NULL == (dset = (H5D_t *)H5VL_object_verify(did, H5I_DATASET)))
        HGOTO_ERROR(H5E_DATASET, H5E_BADTYPE, FAIL, "not a dataset");

    if (dirty) {
        assert(dset->shared->layout.type == H5D_COMPACT);
        *dirty = dset->shared->layout.storage.u.compact.dirty;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__layout_compact_dirty_test() */

/*--------------------------------------------------------------------------
 NAME
    H5D__layout_type_test
 PURPOSE
    Determine the storage layout type for a dataset
 USAGE
    herr_t H5D__layout_type_test(did, layout_type)
        hid_t did;              IN: Dataset to query
        H5D_layout_t *layout_type;      OUT: Pointer to location to place layout info
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Checks the layout type for a dataset.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    DO NOT USE THIS FUNCTION FOR ANYTHING EXCEPT TESTING
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5D__layout_type_test(hid_t did, H5D_layout_t *layout_type)
{
    H5D_t *dset;                /* Pointer to dataset to query */
    herr_t ret_value = SUCCEED; /* return value */

    FUNC_ENTER_PACKAGE

    assert(layout_type);

    /* Check args */
    if (NULL == (dset = (H5D_t *)H5VL_object_verify(did, H5I_DATASET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataset");

    if (layout_type)
        *layout_type = dset->shared->layout.type;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__layout_type_test() */

/*--------------------------------------------------------------------------
 NAME
    H5D__layout_idx_type_test
 PURPOSE
    Determine the storage layout index type for a dataset's layout information
 USAGE
    herr_t H5D__layout_idx_type_test(did, idx_type)
        hid_t did;              IN: Dataset to query
        H5D_chunk_index_t *idx_type;      OUT: Pointer to location to place index type info
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Checks the index type of the storage layout information for a dataset.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    DO NOT USE THIS FUNCTION FOR ANYTHING EXCEPT TESTING
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5D__layout_idx_type_test(hid_t did, H5D_chunk_index_t *idx_type)
{
    H5D_t *dset;                /* Pointer to dataset to query */
    herr_t ret_value = SUCCEED; /* return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    if (NULL == (dset = (H5D_t *)H5VL_object_verify(did, H5I_DATASET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataset");
    if (dset->shared->layout.type != H5D_CHUNKED)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "dataset is not chunked");

    if (idx_type)
        *idx_type = dset->shared->layout.u.chunk.idx_type;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__layout_idx_type_test() */

/*--------------------------------------------------------------------------
 NAME
    H5D__current_cache_size_test
 PURPOSE
    Determine current the size of the dataset's chunk cache
 USAGE
    herr_t H5D__current_cache_size_test(did, size)
        hid_t did;              IN: Dataset to query
        hsize_t *size;          OUT: Pointer to location to place size info
 RETURNS
    Non-negative on success, negative on failure
 DESCRIPTION
    Checks the size of a contiguous dataset's storage.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    DO NOT USE THIS FUNCTION FOR ANYTHING EXCEPT TESTING
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5D__current_cache_size_test(hid_t did, size_t *nbytes_used, int *nused)
{
    H5D_t *dset;                /* Pointer to dataset to query */
    herr_t ret_value = SUCCEED; /* return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    if (NULL == (dset = (H5D_t *)H5VL_object_verify(did, H5I_DATASET)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataset");

    if (nbytes_used) {
        assert(dset->shared->layout.type == H5D_CHUNKED);
        *nbytes_used = dset->shared->cache.chunk.nbytes_used;
    } /* end if */

    if (nused) {
        assert(dset->shared->layout.type == H5D_CHUNKED);
        *nused = dset->shared->cache.chunk.nused;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__current_cache_size_test() */
