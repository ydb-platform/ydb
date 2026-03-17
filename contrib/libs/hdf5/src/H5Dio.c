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

/****************/
/* Module Setup */
/****************/

#include "H5Dmodule.h" /* This source code file is part of the H5D module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5CXprivate.h" /* API Contexts                             */
#include "H5Dpkg.h"      /* Dataset functions                        */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5FLprivate.h" /* Free Lists                               */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5MMprivate.h" /* Memory management                        */
#include "H5Sprivate.h"  /* Dataspace                                */

#include "H5VLnative_private.h" /* Native VOL connector                     */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Local Prototypes */
/********************/

/* Setup/teardown routines */
static herr_t H5D__ioinfo_init(size_t count, H5D_io_op_type_t op_type, H5D_dset_io_info_t *dset_info,
                               H5D_io_info_t *io_info);
static herr_t H5D__dset_ioinfo_init(H5D_t *dset, H5D_dset_io_info_t *dset_info, H5D_storage_t *store);
static herr_t H5D__typeinfo_init(H5D_io_info_t *io_info, H5D_dset_io_info_t *dset_info, hid_t mem_type_id);
static herr_t H5D__typeinfo_init_phase2(H5D_io_info_t *io_info);
static herr_t H5D__typeinfo_init_phase3(H5D_io_info_t *io_info);
#ifdef H5_HAVE_PARALLEL
static herr_t H5D__ioinfo_adjust(H5D_io_info_t *io_info);
#endif /* H5_HAVE_PARALLEL */
static herr_t H5D__typeinfo_term(H5D_io_info_t *io_info);

/*********************/
/* Package Variables */
/*********************/

/*******************/
/* Local Variables */
/*******************/

/* Declare a free list to manage blocks of type conversion data */
H5FL_BLK_DEFINE(type_conv);

/*-------------------------------------------------------------------------
 * Function:	H5D__read
 *
 * Purpose:	Reads multiple (parts of) DATASETs into application memory BUFs.
 *              See H5Dread_multi() for complete details.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__read(size_t count, H5D_dset_io_info_t *dset_info)
{
    H5D_io_info_t io_info;                    /* Dataset I/O info  for multi dsets */
    H5S_t        *orig_mem_space_local;       /* Local buffer for orig_mem_space */
    H5S_t       **orig_mem_space = NULL;      /* If not NULL, ptr to an array of dataspaces       */
                                              /* containing the original memory spaces contained  */
                                              /* in dset_info.  This is needed in order to        */
                                              /* restore the original state of  dset_info if we   */
                                              /* replaced any mem spaces with equivalents         */
                                              /* projected to a rank equal to that of file_space. */
                                              /*                                                  */
                                              /* This field is only used if                       */
                                              /* H5S_select_shape_same() returns true when        */
                                              /* comparing at least one mem_space and data_space, */
                                              /* and the mem_space has a different rank.          */
                                              /*                                                  */
                                              /* Note that this is a temporary variable - the     */
                                              /* projected memory space is stored in dset_info,   */
                                              /* and will be freed when that structure is         */
                                              /* freed. */
    H5D_storage_t  store_local;               /* Local buffer for store */
    H5D_storage_t *store      = &store_local; /* Union of EFL and chunk pointer in file space */
    size_t         io_op_init = 0;            /* Number I/O ops that have been initialized */
    size_t         io_skipped =
        0;            /* Number I/O ops that have been skipped (due to the dataset not being allocated) */
    size_t i;         /* Local index variable */
    char   fake_char; /* Temporary variable for NULL buffer pointers */
    herr_t ret_value = SUCCEED; /* Return value	*/

    FUNC_ENTER_NOAPI(FAIL)

#ifdef H5_HAVE_PARALLEL
    /* Reset the actual io mode properties to the default values in case
     * the DXPL (if it's non-default) was previously used in a collective
     * I/O operation.
     */
    if (!H5CX_is_def_dxpl()) {
        H5CX_set_mpio_actual_chunk_opt(H5D_MPIO_NO_CHUNK_OPTIMIZATION);
        H5CX_set_mpio_actual_io_mode(H5D_MPIO_NO_COLLECTIVE);
    } /* end if */
#endif

    /* Init io_info */
    if (H5D__ioinfo_init(count, H5D_IO_OP_READ, dset_info, &io_info) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't initialize I/O info");

    /* Allocate store buffer if necessary */
    if (count > 1)
        if (NULL == (store = (H5D_storage_t *)H5MM_malloc(count * sizeof(H5D_storage_t))))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate dset storage info array buffer");

#ifdef H5_HAVE_PARALLEL
    /* Check for non-MPI-based VFD.  Only need to check first dataset since all
     * share the same file. */
    if (!(H5F_HAS_FEATURE(dset_info[0].dset->oloc.file, H5FD_FEAT_HAS_MPI))) {
        H5FD_mpio_xfer_t io_xfer_mode; /* MPI I/O transfer mode */

        /* Get I/O transfer mode */
        if (H5CX_get_io_xfer_mode(&io_xfer_mode) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get MPI-I/O transfer mode");

        /* Collective access is not permissible without a MPI based VFD */
        if (io_xfer_mode == H5FD_MPIO_COLLECTIVE)
            HGOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL, "collective access for MPI-based drivers only");
    }  /* end if */
#endif /*H5_HAVE_PARALLEL*/

    /* Iterate over all dsets and construct I/O information necessary to do I/O */
    for (i = 0; i < count; i++) {
        haddr_t prev_tag = HADDR_UNDEF;

        /* Check args */
        if (NULL == dset_info[i].dset)
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataset");
        if (NULL == dset_info[i].dset->oloc.file)
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file");

        /* Set metadata tagging with dset oheader addr */
        H5AC_tag(dset_info[i].dset->oloc.addr, &prev_tag);

        /* Set up datatype info for operation */
        if (H5D__typeinfo_init(&io_info, &(dset_info[i]), dset_info[i].mem_type_id) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to set up type info");

        /* Make certain that the number of elements in each selection is the same, and cache nelmts in
         * dset_info */
        dset_info[i].nelmts = H5S_GET_SELECT_NPOINTS(dset_info[i].mem_space);
        if (dset_info[i].nelmts != H5S_GET_SELECT_NPOINTS(dset_info[i].file_space))
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                        "src and dest dataspaces have different number of elements selected");

        /* Check for a NULL buffer */
        if (NULL == dset_info[i].buf.vp) {
            /* Check for any elements selected (which is invalid) */
            if (dset_info[i].nelmts > 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no output buffer");

            /* If the buffer is nil, and 0 element is selected, make a fake buffer. */
            dset_info[i].buf.vp = &fake_char;
        } /* end if */

        /* Make sure that both selections have their extents set */
        if (!(H5S_has_extent(dset_info[i].file_space)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file dataspace does not have extent set");
        if (!(H5S_has_extent(dset_info[i].mem_space)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "memory dataspace does not have extent set");

        /* H5S_select_shape_same() has been modified to accept topologically identical
         * selections with different rank as having the same shape (if the most
         * rapidly changing coordinates match up), but the I/O code still has
         * difficulties with the notion.
         *
         * To solve this, check if H5S_select_shape_same() returns true
         * and the ranks of the mem and file spaces are different.  If so,
         * construct a new mem space that is equivalent to the old mem space, and
         * use that instead.
         *
         * Note that in general, this requires us to touch up the memory buffer as
         * well.
         */
        if (dset_info[i].nelmts > 0 &&
            true == H5S_SELECT_SHAPE_SAME(dset_info[i].mem_space, dset_info[i].file_space) &&
            H5S_GET_EXTENT_NDIMS(dset_info[i].mem_space) != H5S_GET_EXTENT_NDIMS(dset_info[i].file_space)) {
            ptrdiff_t buf_adj = 0;

            /* Allocate original memory space buffer if necessary */
            if (!orig_mem_space) {
                if (count > 1) {
                    /* Allocate buffer */
                    if (NULL == (orig_mem_space = (H5S_t **)H5MM_calloc(count * sizeof(H5S_t *))))
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL,
                                    "couldn't allocate original memory space array buffer");
                }
                else
                    /* Use local buffer */
                    orig_mem_space = &orig_mem_space_local;
            }

            /* Save original memory space */
            orig_mem_space[i]      = dset_info[i].mem_space;
            dset_info[i].mem_space = NULL;

            /* Attempt to construct projected dataspace for memory dataspace */
            if (H5S_select_construct_projection(orig_mem_space[i], &dset_info[i].mem_space,
                                                (unsigned)H5S_GET_EXTENT_NDIMS(dset_info[i].file_space),
                                                (hsize_t)dset_info[i].type_info.dst_type_size, &buf_adj) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                            "unable to construct projected memory dataspace");
            assert(dset_info[i].mem_space);

            /* Adjust the buffer by the given amount */
            dset_info[i].buf.vp = (void *)(((uint8_t *)dset_info[i].buf.vp) + buf_adj);
        } /* end if */

        /* Set up I/O operation */
        if (H5D__dset_ioinfo_init(dset_info[i].dset, &(dset_info[i]), &(store[i])) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to set up I/O operation");

        /* Check if any filters are applied to the dataset */
        if (dset_info[i].dset->shared->dcpl_cache.pline.nused > 0)
            io_info.filtered_count++;

        /* If space hasn't been allocated and not using external storage,
         * return fill value to buffer if fill time is upon allocation, or
         * do nothing if fill time is never.  If the dataset is compact and
         * fill time is NEVER, there is no way to tell whether part of data
         * has been overwritten.  So just proceed in reading.
         */
        if (dset_info[i].nelmts > 0 && dset_info[i].dset->shared->dcpl_cache.efl.nused == 0 &&
            !(*dset_info[i].dset->shared->layout.ops->is_space_alloc)(
                &dset_info[i].dset->shared->layout.storage) &&
            !(dset_info[i].dset->shared->layout.ops->is_data_cached &&
              (*dset_info[i].dset->shared->layout.ops->is_data_cached)(dset_info[i].dset->shared))) {
            H5D_fill_value_t fill_status; /* Whether/How the fill value is defined */

            /* Retrieve dataset's fill-value properties */
            if (H5P_is_fill_value_defined(&dset_info[i].dset->shared->dcpl_cache.fill, &fill_status) < 0)
                HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't tell if fill value defined");

            /* Should be impossible, but check anyway... */
            if (fill_status == H5D_FILL_VALUE_UNDEFINED &&
                (dset_info[i].dset->shared->dcpl_cache.fill.fill_time == H5D_FILL_TIME_ALLOC ||
                 dset_info[i].dset->shared->dcpl_cache.fill.fill_time == H5D_FILL_TIME_IFSET))
                HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL,
                            "read failed: dataset doesn't exist, no data can be read");

            /* If we're never going to fill this dataset, just leave the junk in the user's buffer */
            if (dset_info[i].dset->shared->dcpl_cache.fill.fill_time != H5D_FILL_TIME_NEVER)
                /* Go fill the user's selection with the dataset's fill value */
                if (H5D__fill(dset_info[i].dset->shared->dcpl_cache.fill.buf, dset_info[i].dset->shared->type,
                              dset_info[i].buf.vp, dset_info[i].type_info.mem_type,
                              dset_info[i].mem_space) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "filling buf failed");

            /* No need to perform any more I/O for this dataset */
            dset_info[i].skip_io = true;
            io_skipped           = io_skipped + 1;
        } /* end if */
        else {
            /* Sanity check that space is allocated, if there are elements */
            if (dset_info[i].nelmts > 0)
                assert(
                    (*dset_info[i].dset->shared->layout.ops->is_space_alloc)(
                        &dset_info[i].dset->shared->layout.storage) ||
                    (dset_info[i].dset->shared->layout.ops->is_data_cached &&
                     (*dset_info[i].dset->shared->layout.ops->is_data_cached)(dset_info[i].dset->shared)) ||
                    dset_info[i].dset->shared->dcpl_cache.efl.nused > 0 ||
                    dset_info[i].dset->shared->layout.type == H5D_COMPACT);

            dset_info[i].skip_io = false;
        }

        /* Call storage method's I/O initialization routine */
        if (dset_info[i].layout_ops.io_init &&
            (dset_info[i].layout_ops.io_init)(&io_info, &(dset_info[i])) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't initialize I/O info");
        io_op_init++;

        /* Reset metadata tagging */
        H5AC_tag(prev_tag, NULL);
    } /* end of for loop */

    assert(io_op_init == count);

    /* If no datasets have I/O, we're done */
    if (io_skipped == count)
        HGOTO_DONE(SUCCEED);

    /* Perform second phase of type info initialization */
    if (H5D__typeinfo_init_phase2(&io_info) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to set up type info (second phase)");

#ifdef H5_HAVE_PARALLEL
    /* Adjust I/O info for any parallel or selection I/O */
    if (H5D__ioinfo_adjust(&io_info) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                    "unable to adjust I/O info for parallel or selection I/O");
#endif /* H5_HAVE_PARALLEL */

    /* Perform third phase of type info initialization */
    if (H5D__typeinfo_init_phase3(&io_info) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to set up type info (third phase)");

    H5CX_set_no_selection_io_cause(io_info.no_selection_io_cause);

    /* If multi dataset I/O callback is not provided, perform read IO via
     * single-dset path with looping */
    if (io_info.md_io_ops.multi_read_md) {
        /* Create sel_pieces array if any pieces are selected */
        if (io_info.piece_count > 0) {
            assert(!io_info.sel_pieces);
            assert(io_info.pieces_added == 0);

            /* Allocate sel_pieces array */
            if (NULL ==
                (io_info.sel_pieces = H5MM_malloc(io_info.piece_count * sizeof(io_info.sel_pieces[0]))))
                HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "unable to allocate array of selected pieces");
        }

        /* MDIO-specific second phase initialization */
        for (i = 0; i < count; i++) {
            /* Check for skipped I/O */
            if (dset_info[i].skip_io)
                continue;

            if (dset_info[i].layout_ops.mdio_init) {
                haddr_t prev_tag = HADDR_UNDEF;

                /* Set metadata tagging with dset oheader addr */
                H5AC_tag(dset_info[i].dset->oloc.addr, &prev_tag);

                /* Make second phase IO init call */
                if ((dset_info[i].layout_ops.mdio_init)(&io_info, &(dset_info[i])) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't populate array of selected pieces");

                /* Reset metadata tagging */
                H5AC_tag(prev_tag, NULL);
            }
        }

        /* Invoke correct "high level" I/O routine */
        if ((*io_info.md_io_ops.multi_read_md)(&io_info) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "can't read data");
    } /* end if */
    else {
        haddr_t prev_tag = HADDR_UNDEF;

        /* Allocate selection I/O parameter arrays if necessary */
        if (!H5D_LAYOUT_CB_PERFORM_IO(&io_info) && io_info.piece_count > 0) {
            if (NULL == (io_info.mem_spaces = H5MM_malloc(io_info.piece_count * sizeof(H5S_t *))))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                            "memory allocation failed for memory space list");
            if (NULL == (io_info.file_spaces = H5MM_malloc(io_info.piece_count * sizeof(H5S_t *))))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                            "memory allocation failed for file space list");
            if (NULL == (io_info.addrs = H5MM_malloc(io_info.piece_count * sizeof(haddr_t))))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                            "memory allocation failed for piece address list");
            if (NULL == (io_info.element_sizes = H5MM_malloc(io_info.piece_count * sizeof(size_t))))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                            "memory allocation failed for element size list");
            if (NULL == (io_info.rbufs = H5MM_malloc(io_info.piece_count * sizeof(void *))))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                            "memory allocation failed for read buffer list");
            if (io_info.max_tconv_type_size > 0)
                if (NULL ==
                    (io_info.sel_pieces = H5MM_malloc(io_info.piece_count * sizeof(io_info.sel_pieces[0]))))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                                "unable to allocate array of selected pieces");
        }

        /* Loop with serial & single-dset read IO path */
        for (i = 0; i < count; i++) {
            /* Check for skipped I/O */
            if (dset_info[i].skip_io)
                continue;

            /* Set metadata tagging with dset object header addr */
            H5AC_tag(dset_info[i].dset->oloc.addr, &prev_tag);

            /* Invoke correct "high level" I/O routine */
            if ((*dset_info[i].io_ops.multi_read)(&io_info, &dset_info[i]) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "can't read data");

            /* Reset metadata tagging */
            H5AC_tag(prev_tag, NULL);
        }

        /* Make final selection I/O call if the multi_read callbacks did not perform the actual I/O
         * (if using selection I/O and either multi dataset or type conversion) */
        if (!H5D_LAYOUT_CB_PERFORM_IO(&io_info)) {
            /* Check for type conversion */
            if (io_info.max_tconv_type_size > 0) {
                /* Type conversion pathway */
                if (H5D__scatgath_read_select(&io_info) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "type conversion selection read failed");
            }
            else {
                /* Call selection I/O directly */
                H5_CHECK_OVERFLOW(io_info.pieces_added, size_t, uint32_t);
                if (H5F_shared_select_read(io_info.f_sh, H5FD_MEM_DRAW, (uint32_t)io_info.pieces_added,
                                           io_info.mem_spaces, io_info.file_spaces, io_info.addrs,
                                           io_info.element_sizes, io_info.rbufs) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "selection read failed");
            }
        }

#ifdef H5_HAVE_PARALLEL
        /* Report the actual I/O mode to the application if appropriate */
        if (io_info.using_mpi_vfd) {
            H5FD_mpio_xfer_t xfer_mode; /* Parallel transfer for this request */

            /* Get the parallel I/O transfer mode */
            if (H5CX_get_io_xfer_mode(&xfer_mode) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get MPI-I/O transfer mode");

            /* Only report the collective I/O mode if we're actually performing collective I/O */
            if (xfer_mode == H5FD_MPIO_COLLECTIVE) {
                H5CX_set_mpio_actual_io_mode(io_info.actual_io_mode);

                /* If we did selection I/O, report that we used "link chunk" mode, since that's the most
                 * analogous to what selection I/O does */
                if (io_info.use_select_io == H5D_SELECTION_IO_MODE_ON)
                    H5CX_set_mpio_actual_chunk_opt(H5D_MPIO_LINK_CHUNK);
            }
        }
#endif /* H5_HAVE_PARALLEL */
    }

done:
    /* Shut down the I/O op information */
    for (i = 0; i < io_op_init; i++)
        if (dset_info[i].layout_ops.io_term &&
            (*dset_info[i].layout_ops.io_term)(&io_info, &(dset_info[i])) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTCLOSEOBJ, FAIL, "unable to shut down I/O op info");

    /* Shut down datatype info for operation */
    if (H5D__typeinfo_term(&io_info) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTCLOSEOBJ, FAIL, "unable to shut down type info");

    /* Discard projected mem spaces and restore originals */
    if (orig_mem_space) {
        for (i = 0; i < count; i++)
            if (orig_mem_space[i]) {
                if (H5S_close(dset_info[i].mem_space) < 0)
                    HDONE_ERROR(H5E_DATASET, H5E_CANTCLOSEOBJ, FAIL,
                                "unable to shut down projected memory dataspace");
                dset_info[i].mem_space = orig_mem_space[i];
            }

        /* Free orig_mem_space array if it was allocated */
        if (orig_mem_space != &orig_mem_space_local)
            H5MM_free(orig_mem_space);
    }

    /* Free global piece array */
    H5MM_xfree(io_info.sel_pieces);

    /* Free selection I/O arrays */
    H5MM_xfree(io_info.mem_spaces);
    H5MM_xfree(io_info.file_spaces);
    H5MM_xfree(io_info.addrs);
    H5MM_xfree(io_info.element_sizes);
    H5MM_xfree(io_info.rbufs);

    /* Free store array if it was allocated */
    if (store != &store_local)
        H5MM_free(store);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__read() */

/*-------------------------------------------------------------------------
 * Function:	H5D__write
 *
 * Purpose:	Writes multiple (part of) DATASETs to a file from application
 *          memory BUFs. See H5Dwrite_multi() for complete details.
 *
 *          This was referred from H5D__write for multi-dset work.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__write(size_t count, H5D_dset_io_info_t *dset_info)
{
    H5D_io_info_t io_info;                    /* Dataset I/O info for multi dsets */
    H5S_t        *orig_mem_space_local;       /* Local buffer for orig_mem_space */
    H5S_t       **orig_mem_space = NULL;      /* If not NULL, ptr to an array of dataspaces       */
                                              /* containing the original memory spaces contained  */
                                              /* in dset_info.  This is needed in order to        */
                                              /* restore the original state of  dset_info if we   */
                                              /* replaced any mem spaces with equivalents         */
                                              /* projected to a rank equal to that of file_space. */
                                              /*                                                  */
                                              /* This field is only used if                       */
                                              /* H5S_select_shape_same() returns true when        */
                                              /* comparing at least one mem_space and data_space, */
                                              /* and the mem_space has a different rank.          */
                                              /*                                                  */
                                              /* Note that this is a temporary variable - the     */
                                              /* projected memory space is stored in dset_info,   */
                                              /* and will be freed when that structure is         */
                                              /* freed. */
    H5D_storage_t  store_local;               /* Local buffer for store */
    H5D_storage_t *store      = &store_local; /* Union of EFL and chunk pointer in file space */
    size_t         io_op_init = 0;            /* Number I/O ops that have been initialized */
    size_t         i;                         /* Local index variable */
    char           fake_char;                 /* Temporary variable for NULL buffer pointers */
    herr_t         ret_value = SUCCEED;       /* Return value	*/

    FUNC_ENTER_NOAPI(FAIL)

#ifdef H5_HAVE_PARALLEL
    /* Reset the actual io mode properties to the default values in case
     * the DXPL (if it's non-default) was previously used in a collective
     * I/O operation.
     */
    if (!H5CX_is_def_dxpl()) {
        H5CX_set_mpio_actual_chunk_opt(H5D_MPIO_NO_CHUNK_OPTIMIZATION);
        H5CX_set_mpio_actual_io_mode(H5D_MPIO_NO_COLLECTIVE);
    } /* end if */
#endif

    /* Init io_info */
    if (H5D__ioinfo_init(count, H5D_IO_OP_WRITE, dset_info, &io_info) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't initialize I/O info");

    /* Allocate store buffer if necessary */
    if (count > 1)
        if (NULL == (store = (H5D_storage_t *)H5MM_malloc(count * sizeof(H5D_storage_t))))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate dset storage info array buffer");

    /* Iterate over all dsets and construct I/O information */
    for (i = 0; i < count; i++) {
        bool    should_alloc_space = false; /* Whether or not to initialize dataset's storage */
        haddr_t prev_tag           = HADDR_UNDEF;

        /* Check args */
        if (NULL == dset_info[i].dset)
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a dataset");
        if (NULL == dset_info[i].dset->oloc.file)
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file");

        /* Set metadata tagging with dset oheader addr */
        H5AC_tag(dset_info[i].dset->oloc.addr, &prev_tag);

        /* All filters in the DCPL must have encoding enabled. */
        if (!dset_info[i].dset->shared->checked_filters) {
            if (H5Z_can_apply(dset_info[i].dset->shared->dcpl_id, dset_info[i].dset->shared->type_id) < 0)
                HGOTO_ERROR(H5E_PLINE, H5E_CANAPPLY, FAIL, "can't apply filters");

            dset_info[i].dset->shared->checked_filters = true;
        } /* end if */

        /* Check if we are allowed to write to this file */
        if (0 == (H5F_INTENT(dset_info[i].dset->oloc.file) & H5F_ACC_RDWR))
            HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "no write intent on file");

        /* Set up datatype info for operation */
        if (H5D__typeinfo_init(&io_info, &(dset_info[i]), dset_info[i].mem_type_id) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to set up type info");

            /* Various MPI based checks */
#ifdef H5_HAVE_PARALLEL
        if (H5F_HAS_FEATURE(dset_info[i].dset->oloc.file, H5FD_FEAT_HAS_MPI)) {
            /* If MPI based VFD is used, no VL or region reference datatype support yet. */
            /* This is because they use the global heap in the file and we don't */
            /* support parallel access of that yet */
            if (H5T_is_vl_storage(dset_info[i].type_info.mem_type) > 0)
                HGOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL,
                            "Parallel IO does not support writing VL or region reference datatypes yet");
        } /* end if */
        else {
            H5FD_mpio_xfer_t io_xfer_mode; /* MPI I/O transfer mode */

            /* Get I/O transfer mode */
            if (H5CX_get_io_xfer_mode(&io_xfer_mode) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get MPI-I/O transfer mode");

            /* Collective access is not permissible without a MPI based VFD */
            if (io_xfer_mode == H5FD_MPIO_COLLECTIVE)
                HGOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL,
                            "collective access for MPI-based driver only");
        } /* end else */
#endif    /*H5_HAVE_PARALLEL*/

        /* Make certain that the number of elements in each selection is the same, and cache nelmts in
         * dset_info */
        dset_info[i].nelmts = H5S_GET_SELECT_NPOINTS(dset_info[i].mem_space);
        if (dset_info[i].nelmts != H5S_GET_SELECT_NPOINTS(dset_info[i].file_space))
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                        "src and dest dataspaces have different number of elements selected");

        /* Check for a NULL buffer */
        if (NULL == dset_info[i].buf.cvp) {
            /* Check for any elements selected (which is invalid) */
            if (dset_info[i].nelmts > 0)
                HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "no input buffer");

            /* If the buffer is nil, and 0 element is selected, make a fake buffer. */
            dset_info[i].buf.cvp = &fake_char;
        } /* end if */

        /* Make sure that both selections have their extents set */
        if (!(H5S_has_extent(dset_info[i].file_space)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file dataspace does not have extent set");
        if (!(H5S_has_extent(dset_info[i].mem_space)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "memory dataspace does not have extent set");

        /* H5S_select_shape_same() has been modified to accept topologically identical
         * selections with different rank as having the same shape (if the most
         * rapidly changing coordinates match up), but the I/O code still has
         * difficulties with the notion.
         *
         * To solve this, check if H5S_select_shape_same() returns true
         * and the ranks of the mem and file spaces are different.  If so,
         * construct a new mem space that is equivalent to the old mem space, and
         * use that instead.
         *
         * Note that in general, this requires us to touch up the memory buffer as
         * well.
         */
        if (dset_info[i].nelmts > 0 &&
            true == H5S_SELECT_SHAPE_SAME(dset_info[i].mem_space, dset_info[i].file_space) &&
            H5S_GET_EXTENT_NDIMS(dset_info[i].mem_space) != H5S_GET_EXTENT_NDIMS(dset_info[i].file_space)) {
            ptrdiff_t buf_adj = 0;

            /* Allocate original memory space buffer if necessary */
            if (!orig_mem_space) {
                if (count > 1) {
                    /* Allocate buffer */
                    if (NULL == (orig_mem_space = (H5S_t **)H5MM_calloc(count * sizeof(H5S_t *))))
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL,
                                    "couldn't allocate original memory space array buffer");
                }
                else
                    /* Use local buffer */
                    orig_mem_space = &orig_mem_space_local;
            }

            /* Save original memory space */
            orig_mem_space[i]      = dset_info[i].mem_space;
            dset_info[i].mem_space = NULL;

            /* Attempt to construct projected dataspace for memory dataspace */
            if (H5S_select_construct_projection(orig_mem_space[i], &dset_info[i].mem_space,
                                                (unsigned)H5S_GET_EXTENT_NDIMS(dset_info[i].file_space),
                                                dset_info[i].type_info.src_type_size, &buf_adj) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                            "unable to construct projected memory dataspace");
            assert(dset_info[i].mem_space);

            /* Adjust the buffer by the given amount */
            dset_info[i].buf.cvp = (const void *)(((const uint8_t *)dset_info[i].buf.cvp) + buf_adj);
        } /* end if */

        /* Retrieve dataset properties */
        /* <none needed currently> */

        /* Set up I/O operation */
        if (H5D__dset_ioinfo_init(dset_info[i].dset, &(dset_info[i]), &(store[i])) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to set up I/O operation");

        /* Check if any filters are applied to the dataset */
        if (dset_info[i].dset->shared->dcpl_cache.pline.nused > 0)
            io_info.filtered_count++;

        /* Allocate dataspace and initialize it if it hasn't been. */
        should_alloc_space = dset_info[i].dset->shared->dcpl_cache.efl.nused == 0 &&
                             !(*dset_info[i].dset->shared->layout.ops->is_space_alloc)(
                                 &dset_info[i].dset->shared->layout.storage);

        /*
         * If not using an MPI-based VFD, we only need to allocate
         * and initialize storage if there's a selection in the
         * dataset's dataspace. Otherwise, we always need to participate
         * in the storage allocation since this may use collective
         * operations and we will hang if we don't participate.
         */
        if (!H5F_HAS_FEATURE(dset_info[i].dset->oloc.file, H5FD_FEAT_HAS_MPI))
            should_alloc_space = should_alloc_space && (dset_info[i].nelmts > 0);

        if (should_alloc_space) {
            hssize_t file_nelmts;    /* Number of elements in file dataset's dataspace */
            bool     full_overwrite; /* Whether we are over-writing all the elements */

            /* Get the number of elements in file dataset's dataspace */
            if ((file_nelmts = H5S_GET_EXTENT_NPOINTS(dset_info[i].file_space)) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL,
                            "can't retrieve number of elements in file dataset");

            /* Always allow fill values to be written if the dataset has a VL datatype */
            if (H5T_detect_class(dset_info[i].dset->shared->type, H5T_VLEN, false))
                full_overwrite = false;
            else
                full_overwrite = (bool)((hsize_t)file_nelmts == dset_info[i].nelmts ? true : false);

            /* Allocate storage */
            if (H5D__alloc_storage(dset_info[i].dset, H5D_ALLOC_WRITE, full_overwrite, NULL) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to initialize storage");
        } /* end if */

        /* Call storage method's I/O initialization routine */
        /* Init io_info.dset_info[] and generate piece_info in skip list */
        if (dset_info[i].layout_ops.io_init &&
            (*dset_info[i].layout_ops.io_init)(&io_info, &(dset_info[i])) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't initialize I/O info");
        dset_info[i].skip_io = false;
        io_op_init++;

        /* Reset metadata tagging */
        H5AC_tag(prev_tag, NULL);
    } /* end of for loop */

    assert(io_op_init == count);

    /* Perform second phase of type info initialization */
    if (H5D__typeinfo_init_phase2(&io_info) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to set up type info (second phase)");

#ifdef H5_HAVE_PARALLEL
    /* Adjust I/O info for any parallel or selection I/O */
    if (H5D__ioinfo_adjust(&io_info) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                    "unable to adjust I/O info for parallel or selection I/O");
#endif /* H5_HAVE_PARALLEL */

    /* Perform third phase of type info initialization */
    if (H5D__typeinfo_init_phase3(&io_info) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to set up type info (third phase)");

    H5CX_set_no_selection_io_cause(io_info.no_selection_io_cause);

    /* If multi dataset I/O callback is not provided, perform write IO via
     * single-dset path with looping */
    if (io_info.md_io_ops.multi_write_md) {
        /* Create sel_pieces array if any pieces are selected */
        if (io_info.piece_count > 0) {
            assert(!io_info.sel_pieces);
            assert(io_info.pieces_added == 0);

            /* Allocate sel_pieces array */
            if (NULL ==
                (io_info.sel_pieces = H5MM_malloc(io_info.piece_count * sizeof(io_info.sel_pieces[0]))))
                HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "unable to allocate array of selected pieces");
        }

        /* MDIO-specific second phase initialization */
        for (i = 0; i < count; i++)
            if (dset_info[i].layout_ops.mdio_init) {
                haddr_t prev_tag = HADDR_UNDEF;

                /* set metadata tagging with dset oheader addr */
                H5AC_tag(dset_info[i].dset->oloc.addr, &prev_tag);

                /* Make second phase IO init call */
                if ((dset_info[i].layout_ops.mdio_init)(&io_info, &(dset_info[i])) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't populate array of selected pieces");

                /* Reset metadata tagging */
                H5AC_tag(prev_tag, NULL);
            }

        /* Invoke correct "high level" I/O routine */
        if ((*io_info.md_io_ops.multi_write_md)(&io_info) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "can't write data");
    } /* end if */
    else {
        haddr_t prev_tag = HADDR_UNDEF;

        /* Allocate selection I/O parameter arrays if necessary */
        if (!H5D_LAYOUT_CB_PERFORM_IO(&io_info) && io_info.piece_count > 0) {
            if (NULL == (io_info.mem_spaces = H5MM_malloc(io_info.piece_count * sizeof(H5S_t *))))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                            "memory allocation failed for memory space list");
            if (NULL == (io_info.file_spaces = H5MM_malloc(io_info.piece_count * sizeof(H5S_t *))))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                            "memory allocation failed for file space list");
            if (NULL == (io_info.addrs = H5MM_malloc(io_info.piece_count * sizeof(haddr_t))))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                            "memory allocation failed for piece address list");
            if (NULL == (io_info.element_sizes = H5MM_malloc(io_info.piece_count * sizeof(size_t))))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                            "memory allocation failed for element size list");
            if (NULL == (io_info.wbufs = H5MM_malloc(io_info.piece_count * sizeof(const void *))))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                            "memory allocation failed for write buffer list");
            if (io_info.max_tconv_type_size > 0)
                if (NULL ==
                    (io_info.sel_pieces = H5MM_malloc(io_info.piece_count * sizeof(io_info.sel_pieces[0]))))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                                "unable to allocate array of selected pieces");
        }

        /* Loop with serial & single-dset write IO path */
        for (i = 0; i < count; i++) {
            assert(!dset_info[i].skip_io);

            /* Set metadata tagging with dset oheader addr */
            H5AC_tag(dset_info->dset->oloc.addr, &prev_tag);

            /* Invoke correct "high level" I/O routine */
            if ((*dset_info[i].io_ops.multi_write)(&io_info, &dset_info[i]) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "can't write data");

            /* Reset metadata tagging */
            H5AC_tag(prev_tag, NULL);
        }

        /* Make final selection I/O call if the multi_write callbacks did not perform the actual I/O
         * (if using selection I/O and either multi dataset or type conversion) */
        if (!H5D_LAYOUT_CB_PERFORM_IO(&io_info)) {
            /* Check for type conversion */
            if (io_info.max_tconv_type_size > 0) {
                /* Type conversion pathway */
                if (H5D__scatgath_write_select(&io_info) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "type conversion selection write failed");
            }
            else {
                /* Call selection I/O directly */
                H5_CHECK_OVERFLOW(io_info.pieces_added, size_t, uint32_t);
                if (H5F_shared_select_write(io_info.f_sh, H5FD_MEM_DRAW, (uint32_t)io_info.pieces_added,
                                            io_info.mem_spaces, io_info.file_spaces, io_info.addrs,
                                            io_info.element_sizes, io_info.wbufs) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "selection write failed");
            }
        }

#ifdef H5_HAVE_PARALLEL
        /* Report the actual I/O mode to the application if appropriate */
        if (io_info.using_mpi_vfd) {
            H5FD_mpio_xfer_t xfer_mode; /* Parallel transfer for this request */

            /* Get the parallel I/O transfer mode */
            if (H5CX_get_io_xfer_mode(&xfer_mode) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get MPI-I/O transfer mode");

            /* Only report the collective I/O mode if we're actually performing collective I/O */
            if (xfer_mode == H5FD_MPIO_COLLECTIVE) {
                H5CX_set_mpio_actual_io_mode(io_info.actual_io_mode);

                /* If we did selection I/O, report that we used "link chunk" mode, since that's the most
                 * analogous to what selection I/O does */
                if (io_info.use_select_io == H5D_SELECTION_IO_MODE_ON)
                    H5CX_set_mpio_actual_chunk_opt(H5D_MPIO_LINK_CHUNK);
            }
        }
#endif /* H5_HAVE_PARALLEL */
    }

done:
    /* Shut down the I/O op information */
    for (i = 0; i < io_op_init; i++) {
        assert(!dset_info[i].skip_io);
        if (dset_info[i].layout_ops.io_term &&
            (*dset_info[i].layout_ops.io_term)(&io_info, &(dset_info[i])) < 0)
            HDONE_ERROR(H5E_DATASET, H5E_CANTCLOSEOBJ, FAIL, "unable to shut down I/O op info");
    }

    /* Shut down datatype info for operation */
    if (H5D__typeinfo_term(&io_info) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTCLOSEOBJ, FAIL, "unable to shut down type info");

    /* Discard projected mem spaces and restore originals */
    if (orig_mem_space) {
        for (i = 0; i < count; i++)
            if (orig_mem_space[i]) {
                if (H5S_close(dset_info[i].mem_space) < 0)
                    HDONE_ERROR(H5E_DATASET, H5E_CANTCLOSEOBJ, FAIL,
                                "unable to shut down projected memory dataspace");
                dset_info[i].mem_space = orig_mem_space[i];
            }

        /* Free orig_mem_space array if it was allocated */
        if (orig_mem_space != &orig_mem_space_local)
            H5MM_free(orig_mem_space);
    }

    /* Free global piece array */
    H5MM_xfree(io_info.sel_pieces);

    /* Free selection I/O arrays */
    H5MM_xfree(io_info.mem_spaces);
    H5MM_xfree(io_info.file_spaces);
    H5MM_xfree(io_info.addrs);
    H5MM_xfree(io_info.element_sizes);
    H5MM_xfree(io_info.wbufs);

    /* Free store array if it was allocated */
    if (store != &store_local)
        H5MM_free(store);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__write */

/*-------------------------------------------------------------------------
 * Function:	H5D__ioinfo_init
 *
 * Purpose:	General setup for H5D_io_info_t struct
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__ioinfo_init(size_t count, H5D_io_op_type_t op_type, H5D_dset_io_info_t *dset_info,
                 H5D_io_info_t *io_info)
{
    H5D_selection_io_mode_t selection_io_mode;

    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(count > 0);
    assert(dset_info);
    assert(dset_info[0].dset->oloc.file);
    assert(io_info);

    /* Zero out struct */
    memset(io_info, 0, sizeof(*io_info));

    /* Set up simple fields */
    io_info->op_type = op_type;
    io_info->f_sh    = count > 0 ? H5F_SHARED(dset_info[0].dset->oloc.file) : NULL;
    io_info->count   = count;

    /* Start without multi-dataset I/O ops. If we're not using the collective
     * I/O path then we will call the single dataset callbacks in a loop. */

    /* Use provided dset_info */
    io_info->dsets_info = dset_info;

    /* Start with selection I/O mode from property list.  If enabled, layout callback will turn it off if it
     * is not supported by the layout.  Handling of H5D_SELECTION_IO_MODE_AUTO occurs in H5D__ioinfo_adjust.
     */
    H5CX_get_selection_io_mode(&selection_io_mode);
    io_info->use_select_io = selection_io_mode;

    /* Record no selection I/O cause if it was disabled by the API */
    if (selection_io_mode == H5D_SELECTION_IO_MODE_OFF)
        io_info->no_selection_io_cause = H5D_SEL_IO_DISABLE_BY_API;

#ifdef H5_HAVE_PARALLEL

    /* Determine if the file was opened with an MPI VFD */
    if (count > 0)
        io_info->using_mpi_vfd = H5F_HAS_FEATURE(dset_info[0].dset->oloc.file, H5FD_FEAT_HAS_MPI);
#endif /* H5_HAVE_PARALLEL */

    /* Check if we could potentially use in-place type conversion */
    if (op_type == H5D_IO_OP_READ)
        /* Always on for read (modulo other restrictions that are handled in layout callbacks) */
        io_info->may_use_in_place_tconv = true;
    else
        /* Only enable in-place type conversion if we're allowed to modify the write buffer */
        H5CX_get_modify_write_buf(&io_info->may_use_in_place_tconv);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__ioinfo_init() */

/*-------------------------------------------------------------------------
 * Function:	H5D__dset_ioinfo_init
 *
 * Purpose:	Routine for determining correct I/O operations for each I/O action.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__dset_ioinfo_init(H5D_t *dset, H5D_dset_io_info_t *dset_info, H5D_storage_t *store)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(dset);
    assert(dset->oloc.file);
    assert(dset_info->type_info.tpath);

    /* Set up "normal" I/O fields */
    dset_info->dset  = dset;
    dset_info->store = store;

    /* Set I/O operations to initial values */
    dset_info->layout_ops = *dset->shared->layout.ops;

    /* Set the "high-level" I/O operations for the dataset */
    dset_info->io_ops.multi_read  = dset->shared->layout.ops->ser_read;
    dset_info->io_ops.multi_write = dset->shared->layout.ops->ser_write;

    /* Set the I/O operations for reading/writing single blocks on disk */
    if (dset_info->type_info.is_xform_noop && dset_info->type_info.is_conv_noop) {
        /*
         * If there is no data transform or type conversion then read directly
         * into the application's buffer.
         * This saves at least one mem-to-mem copy.
         */
        dset_info->io_ops.single_read  = H5D__select_read;
        dset_info->io_ops.single_write = H5D__select_write;
    } /* end if */
    else {
        /*
         * This is the general case (type conversion, usually).
         */
        dset_info->io_ops.single_read  = H5D__scatgath_read;
        dset_info->io_ops.single_write = H5D__scatgath_write;
    } /* end else */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__dset_ioinfo_init() */

/*-------------------------------------------------------------------------
 * Function:	H5D__typeinfo_init
 *
 * Purpose:	Routine for determining correct datatype information for
 *              each I/O action.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__typeinfo_init(H5D_io_info_t *io_info, H5D_dset_io_info_t *dset_info, hid_t mem_type_id)
{
    H5D_type_info_t  *type_info;
    const H5D_t      *dset;
    const H5T_t      *src_type;            /* Source datatype */
    const H5T_t      *dst_type;            /* Destination datatype */
    H5Z_data_xform_t *data_transform;      /* Data transform info */
    herr_t            ret_value = SUCCEED; /* Return value	*/

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(io_info);
    assert(dset_info);

    /* Set convenience pointers */
    type_info = &dset_info->type_info;
    dset      = dset_info->dset;
    assert(dset);

    /* Patch the top level file pointer for dt->shared->u.vlen.f if needed */
    if (H5T_patch_vlen_file(dset->shared->type, H5F_VOL_OBJ(dset->oloc.file)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTOPENOBJ, FAIL, "can't patch VL datatype file pointer");

    /* Initialize type info safely */
    memset(type_info, 0, sizeof(*type_info));

    /* Get the memory & dataset datatypes */
    if (NULL == (type_info->mem_type = (const H5T_t *)H5I_object_verify(mem_type_id, H5I_DATATYPE)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a datatype");
    type_info->dset_type = dset->shared->type;

    if (io_info->op_type == H5D_IO_OP_WRITE) {
        src_type               = type_info->mem_type;
        dst_type               = dset->shared->type;
        type_info->src_type_id = mem_type_id;
        type_info->dst_type_id = dset->shared->type_id;
    } /* end if */
    else {
        src_type               = dset->shared->type;
        dst_type               = type_info->mem_type;
        type_info->src_type_id = dset->shared->type_id;
        type_info->dst_type_id = mem_type_id;
    } /* end else */

    /* Locate the type conversion function and dataspace conversion
     * functions, and set up the element numbering information. If a data
     * type conversion is necessary then register datatype IDs. Data type
     * conversion is necessary if the user has set the `need_bkg' to a high
     * enough value in xfer_parms since turning off datatype conversion also
     * turns off background preservation.
     */
    if (NULL == (type_info->tpath = H5T_path_find(src_type, dst_type)))
        HGOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL, "unable to convert between src and dest datatype");

    /* Retrieve info from API context */
    if (H5CX_get_data_transform(&data_transform) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get data transform info");

    /* Precompute some useful information */
    type_info->src_type_size = H5T_get_size(src_type);
    type_info->dst_type_size = H5T_get_size(dst_type);
    type_info->is_conv_noop  = H5T_path_noop(type_info->tpath);
    type_info->is_xform_noop = H5Z_xform_noop(data_transform);
    if (type_info->is_xform_noop && type_info->is_conv_noop) {
        type_info->cmpd_subset = NULL;
        type_info->need_bkg    = H5T_BKG_NO;
    } /* end if */
    else {
        H5T_bkg_t bkgr_buf_type; /* Background buffer type */

        /* Get info from API context */
        if (H5CX_get_bkgr_buf_type(&bkgr_buf_type) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't retrieve background buffer type");

        /* Check if the datatypes are compound subsets of one another */
        type_info->cmpd_subset = H5T_path_compound_subset(type_info->tpath);

        /* Update io_info->max_tconv_type_size */
        io_info->max_tconv_type_size =
            MAX3(io_info->max_tconv_type_size, type_info->src_type_size, type_info->dst_type_size);

        /* Check if we need a background buffer */
        if ((io_info->op_type == H5D_IO_OP_WRITE) && H5T_detect_class(dset->shared->type, H5T_VLEN, false))
            type_info->need_bkg = H5T_BKG_YES;
        else {
            H5T_bkg_t path_bkg; /* Type conversion's background info */

            if ((path_bkg = H5T_path_bkg(type_info->tpath))) {
                /* Retrieve the bkgr buffer property */
                type_info->need_bkg = bkgr_buf_type;
                type_info->need_bkg = MAX(path_bkg, type_info->need_bkg);
            } /* end if */
            else
                type_info->need_bkg = H5T_BKG_NO; /*never needed even if app says yes*/
        }                                         /* end else */
    }                                             /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__typeinfo_init() */

/*-------------------------------------------------------------------------
 * Function:    H5D__typeinfo_init_phase2
 *
 * Purpose:     Continues initializing type info for all datasets after
 *              calculating the max type size across all datasets, and
 *              before final determination of collective/independent in
 *              H5D__ioinfo_adjust().  Currently just checks to see if
 *              selection I/O can be used with type conversion, and sets
 *              no_collective_cause flags related to selection I/O.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__typeinfo_init_phase2(H5D_io_info_t *io_info)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(io_info);

    /* If selection I/O mode is default (auto), enable it here if the VFD supports it (it will be turned off
     * later if something else conflicts), otherwise disable it */
    if (io_info->use_select_io == H5D_SELECTION_IO_MODE_DEFAULT) {
        if (H5F_has_vector_select_io(io_info->dsets_info[0].dset->oloc.file,
                                     io_info->op_type == H5D_IO_OP_WRITE))
            io_info->use_select_io = H5D_SELECTION_IO_MODE_ON;
        else {
            io_info->use_select_io = H5D_SELECTION_IO_MODE_OFF;
            io_info->no_selection_io_cause |= H5D_SEL_IO_DEFAULT_OFF;
        }
    }

    /* If we're doing type conversion and we might be doing selection I/O, check if the buffers are large
     * enough to handle the whole I/O */
    if (io_info->max_tconv_type_size && io_info->use_select_io != H5D_SELECTION_IO_MODE_OFF) {
        size_t max_temp_buf; /* Maximum temporary buffer size */
        size_t i;            /* Local index variable */

        /* Collective I/O, conversion buffer must be large enough for entire I/O (for now) */

        /* Calculate size of background buffer (tconv buf size was calculated in layout io_init callbacks)
         */
        for (i = 0; i < io_info->count; i++) {
            H5D_type_info_t *type_info = &io_info->dsets_info[i].type_info;

            /* Check for background buffer */
            if (type_info->need_bkg) {
                /* Add size of this dataset's background buffer to the global background buffer size
                 */
                io_info->bkg_buf_size += io_info->dsets_info[i].nelmts * type_info->dst_type_size;

                /* Check if we need to fill the background buffer with the destination contents */
                if (type_info->need_bkg == H5T_BKG_YES)
                    io_info->must_fill_bkg = true;
            }
        }

        /* Get max temp buffer size from API context */
        if (H5CX_get_max_temp_buf(&max_temp_buf) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't retrieve max. temp. buf size");

        /* Check if the needed type conversion or background buffer size is too big */
        if (io_info->tconv_buf_size > max_temp_buf) {
            io_info->use_select_io = H5D_SELECTION_IO_MODE_OFF;
            io_info->no_selection_io_cause |= H5D_SEL_IO_TCONV_BUF_TOO_SMALL;
            io_info->tconv_buf_size = 0;
            io_info->bkg_buf_size   = 0;
            io_info->must_fill_bkg  = false;
        }
        if (io_info->bkg_buf_size > max_temp_buf) {
            io_info->use_select_io = H5D_SELECTION_IO_MODE_OFF;
            io_info->no_selection_io_cause |= H5D_SEL_IO_BKG_BUF_TOO_SMALL;
            io_info->tconv_buf_size = 0;
            io_info->bkg_buf_size   = 0;
            io_info->must_fill_bkg  = false;
        }
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__typeinfo_init_phase2() */

#ifdef H5_HAVE_PARALLEL
/*-------------------------------------------------------------------------
 * Function:    H5D__ioinfo_adjust
 *
 * Purpose:     Adjusts operation's I/O info for any parallel I/O, also
 *              handle decision on selection I/O even in serial case
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__ioinfo_adjust(H5D_io_info_t *io_info)
{
    H5D_t *dset0;               /* only the first dset , also for single dsets case */
    herr_t ret_value = SUCCEED; /* Return value	*/

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(io_info);

    /* Check the first dset, should exist either single or multi dset cases */
    assert(io_info->dsets_info[0].dset);
    dset0 = io_info->dsets_info[0].dset;
    assert(dset0->oloc.file);

    /* Make any parallel I/O adjustments */
    if (io_info->using_mpi_vfd) {
        H5FD_mpio_xfer_t xfer_mode; /* Parallel transfer for this request */
        htri_t           opt;       /* Flag whether a selection is optimizable */

        /* Get the original state of parallel I/O transfer mode */
        if (H5CX_get_io_xfer_mode(&xfer_mode) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get MPI-I/O transfer mode");

        /* Get MPI communicator */
        if (MPI_COMM_NULL == (io_info->comm = H5F_mpi_get_comm(dset0->oloc.file)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't retrieve MPI communicator");

        /* Check if we can set direct MPI-IO read/write functions */
        if ((opt = H5D__mpio_opt_possible(io_info)) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_BADRANGE, FAIL, "invalid check for direct IO dataspace ");

        /* Check if we can use the optimized parallel I/O routines */
        if (opt == true) {
            /* Override the I/O op pointers to the MPI-specific routines, unless
             * selection I/O is to be used - in this case the file driver will
             * handle collective I/O */
            /* Check for selection/vector support in file driver? -NAF */
            if (io_info->use_select_io == H5D_SELECTION_IO_MODE_OFF) {
                io_info->md_io_ops.multi_read_md   = H5D__collective_read;
                io_info->md_io_ops.multi_write_md  = H5D__collective_write;
                io_info->md_io_ops.single_read_md  = H5D__mpio_select_read;
                io_info->md_io_ops.single_write_md = H5D__mpio_select_write;
            } /* end if */
        }     /* end if */
        else {
            /* Fail when file sync is required, since it requires collective write */
            if (io_info->op_type == H5D_IO_OP_WRITE) {
                bool mpi_file_sync_required = false;
                if (H5F_shared_get_mpi_file_sync_required(io_info->f_sh, &mpi_file_sync_required) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get MPI file_sync_required flag");

                if (mpi_file_sync_required)
                    HGOTO_ERROR(
                        H5E_DATASET, H5E_NO_INDEPENDENT, FAIL,
                        "Can't perform independent write when MPI_File_sync is required by ROMIO driver.");
            }

            /* Check if there are any filters in the pipeline. If there are,
             * we cannot break to independent I/O if this is a write operation
             * with multiple ranks involved; otherwise, there will be metadata
             * inconsistencies in the file.
             */
            if (io_info->op_type == H5D_IO_OP_WRITE) {
                size_t i;

                /* Check all datasets for filters */
                for (i = 0; i < io_info->count; i++)
                    if (io_info->dsets_info[i].dset->shared->dcpl_cache.pline.nused > 0)
                        break;

                /* If the above loop didn't complete, at least one dataset has a filter */
                if (i < io_info->count) {
                    int comm_size = 0;

                    /* Retrieve size of MPI communicator used for file */
                    if ((comm_size = H5F_shared_mpi_get_size(io_info->f_sh)) < 0)
                        HGOTO_ERROR(H5E_FILE, H5E_CANTGET, FAIL, "can't get MPI communicator size");

                    if (comm_size > 1) {
                        char local_no_coll_cause_string[512];
                        char global_no_coll_cause_string[512];

                        if (H5D__mpio_get_no_coll_cause_strings(local_no_coll_cause_string, 512,
                                                                global_no_coll_cause_string, 512) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL,
                                        "can't get reasons for breaking collective I/O");

                        HGOTO_ERROR(H5E_IO, H5E_NO_INDEPENDENT, FAIL,
                                    "Can't perform independent write with filters in pipeline.\n"
                                    "    The following caused a break from collective I/O:\n"
                                    "        Local causes: %s\n"
                                    "        Global causes: %s",
                                    local_no_coll_cause_string, global_no_coll_cause_string);
                    }
                }
            }

            /* If we won't be doing collective I/O, but the user asked for
             * collective I/O, change the request to use independent I/O
             */
            if (xfer_mode == H5FD_MPIO_COLLECTIVE) {
                /* Change the xfer_mode to independent for handling the I/O */
                if (H5CX_set_io_xfer_mode(H5FD_MPIO_INDEPENDENT) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTSET, FAIL, "can't set MPI-I/O transfer mode");
            } /* end if */
        }     /* end else */
    }         /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__ioinfo_adjust() */
#endif /* H5_HAVE_PARALLEL */

/*-------------------------------------------------------------------------
 * Function:    H5D__typeinfo_init_phase3
 *
 * Purpose:     Finishes initializing type info for all datasets after
 *              calculating the max type size across all datasets and
 *              final collective/independent determination in
 *              H5D__ioinfo_adjust().
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__typeinfo_init_phase3(H5D_io_info_t *io_info)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(io_info);

    /* Check if we need to allocate a shared type conversion buffer */
    if (io_info->max_tconv_type_size) {
        void *tconv_buf; /* Temporary conversion buffer pointer */
        void *bkgr_buf;  /* Background conversion buffer pointer */

        /* Get provided buffers from API context */
        if (H5CX_get_tconv_buf(&tconv_buf) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't retrieve temp. conversion buffer pointer");
        if (H5CX_get_bkgr_buf(&bkgr_buf) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL,
                        "can't retrieve background conversion buffer pointer");

        /* Check if we're doing selection I/O */
        if (io_info->use_select_io == H5D_SELECTION_IO_MODE_ON) {
            /* Selection I/O, conversion buffers must be large enough for entire I/O (for now) */

            /* Allocate global type conversion buffer (if any, could be none if datasets in this
             * I/O have 0 elements selected) */
            /* Allocating large buffers here will blow out all other type conversion buffers
             * on the free list.  Should we change this to a regular malloc?  Would require
             * keeping track of which version of free() to call. -NAF */
            if (io_info->tconv_buf_size > 0) {
                if (NULL == (io_info->tconv_buf = H5FL_BLK_MALLOC(type_conv, io_info->tconv_buf_size)))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                                "memory allocation failed for type conversion");
                io_info->tconv_buf_allocated = true;
            }

            /* Allocate global background buffer (if any) */
            if (io_info->bkg_buf_size > 0) {
                if (NULL == (io_info->bkg_buf = H5FL_BLK_MALLOC(type_conv, io_info->bkg_buf_size)))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                                "memory allocation failed for type conversion");
                io_info->bkg_buf_allocated = true;
            }
        }
        else {
            /* No selection I/O, only need to make sure it's big enough for one element */
            size_t max_temp_buf; /* Maximum temporary buffer size */
            size_t target_size;  /* Desired buffer size	*/
            size_t i;

            /* Make sure selection I/O is disabled (DEFAULT should have been handled by now) */
            assert(io_info->use_select_io == H5D_SELECTION_IO_MODE_OFF);

            /* Get max buffer size from API context */
            if (H5CX_get_max_temp_buf(&max_temp_buf) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't retrieve max. temp. buf size");

            /* Set up datatype conversion/background buffers */
            target_size = max_temp_buf;

            /* If the buffer is too small to hold even one element (in the dataset with the largest , try to
             * make it bigger */
            if (target_size < io_info->max_tconv_type_size) {
                bool default_buffer_info; /* Whether the buffer information are the defaults */

                /* Detect if we have all default settings for buffers */
                default_buffer_info =
                    (bool)((H5D_TEMP_BUF_SIZE == max_temp_buf) && (NULL == tconv_buf) && (NULL == bkgr_buf));

                /* Check if we are using the default buffer info */
                if (default_buffer_info)
                    /* OK to get bigger for library default settings */
                    target_size = io_info->max_tconv_type_size;
                else
                    /* Don't get bigger than the application has requested */
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "temporary buffer max size is too small");
            } /* end if */

            /* Get a temporary buffer for type conversion unless the app has already
             * supplied one through the xfer properties. Instead of allocating a
             * buffer which is the exact size, we allocate the target size. This
             * buffer is shared among all datasets in the operation. */
            if (NULL == (io_info->tconv_buf = (uint8_t *)tconv_buf)) {
                /* Allocate temporary buffer */
                if (NULL == (io_info->tconv_buf = H5FL_BLK_MALLOC(type_conv, target_size)))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                                "memory allocation failed for type conversion");
                io_info->tconv_buf_allocated = true;
            } /* end if */

            /* Iterate over datasets */
            for (i = 0; i < io_info->count; i++) {
                H5D_type_info_t *type_info = &io_info->dsets_info[i].type_info;

                /* Compute the number of elements that will fit into buffer */
                type_info->request_nelmts =
                    target_size / MAX(type_info->src_type_size, type_info->dst_type_size);

                /* Check if we need a background buffer and one hasn't been allocated yet */
                if (type_info->need_bkg && (NULL == io_info->bkg_buf) &&
                    (NULL == (io_info->bkg_buf = (uint8_t *)bkgr_buf))) {
                    /* Allocate background buffer with the same size as the type conversion buffer.  We can do
                     * this since the number of elements that fit in the type conversion buffer will never be
                     * larger than the number that could fit in a background buffer of equal size, since the
                     * tconv element size is max(src, dst) and the bkg element size is dst */
                    if (NULL == (io_info->bkg_buf = H5FL_BLK_MALLOC(type_conv, target_size)))
                        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                                    "memory allocation failed for background conversion");
                    io_info->bkg_buf_allocated = true;
                }
            }
        }
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__typeinfo_init_phase3() */

/*-------------------------------------------------------------------------
 * Function:    H5D__typeinfo_term
 *
 * Purpose:     Common logic for terminating a type info object
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__typeinfo_term(H5D_io_info_t *io_info)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check for releasing datatype conversion & background buffers */
    if (io_info->tconv_buf_allocated) {
        assert(io_info->tconv_buf);
        (void)H5FL_BLK_FREE(type_conv, io_info->tconv_buf);
    } /* end if */
    if (io_info->bkg_buf_allocated) {
        assert(io_info->bkg_buf);
        (void)H5FL_BLK_FREE(type_conv, io_info->bkg_buf);
    } /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__typeinfo_term() */
