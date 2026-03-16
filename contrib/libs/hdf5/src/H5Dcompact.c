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
 * Purpose:     Compact dataset I/O functions.  These routines are similar
 *              H5D_contig_* and H5D_chunk_*.
 */

/****************/
/* Module Setup */
/****************/

#include "H5Dmodule.h" /* This source code file is part of the H5D module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Dpkg.h"      /* Dataset functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Fprivate.h"  /* Files				*/
#include "H5FDprivate.h" /* File drivers				*/
#include "H5FLprivate.h" /* Free Lists                           */
#include "H5Iprivate.h"  /* IDs			  		*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5Oprivate.h"  /* Object headers		  	*/
#include "H5VMprivate.h" /* Vector and array functions		*/

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/* Callback info for I/O operation when file driver
 * wishes to do its own memory management
 */
typedef struct H5D_compact_iovv_memmanage_ud_t {
    H5F_shared_t *f_sh;   /* Shared file for dataset */
    void         *dstbuf; /* Pointer to buffer to be read into/written into */
    const void   *srcbuf; /* Pointer to buffer to be read from/written from */
} H5D_compact_iovv_memmanage_ud_t;

/********************/
/* Local Prototypes */
/********************/

/* Layout operation callbacks */
static herr_t  H5D__compact_construct(H5F_t *f, H5D_t *dset);
static bool    H5D__compact_is_space_alloc(const H5O_storage_t *storage);
static herr_t  H5D__compact_io_init(H5D_io_info_t *io_info, H5D_dset_io_info_t *dinfo);
static herr_t  H5D__compact_iovv_memmanage_cb(hsize_t dst_off, hsize_t src_off, size_t len, void *_udata);
static ssize_t H5D__compact_readvv(const H5D_io_info_t *io_info, const H5D_dset_io_info_t *dset_info,
                                   size_t dset_max_nseq, size_t *dset_curr_seq, size_t dset_size_arr[],
                                   hsize_t dset_offset_arr[], size_t mem_max_nseq, size_t *mem_curr_seq,
                                   size_t mem_size_arr[], hsize_t mem_offset_arr[]);
static ssize_t H5D__compact_writevv(const H5D_io_info_t *io_info, const H5D_dset_io_info_t *dset_info,
                                    size_t dset_max_nseq, size_t *dset_curr_seq, size_t dset_size_arr[],
                                    hsize_t dset_offset_arr[], size_t mem_max_nseq, size_t *mem_curr_seq,
                                    size_t mem_size_arr[], hsize_t mem_offset_arr[]);
static herr_t  H5D__compact_flush(H5D_t *dset);
static herr_t  H5D__compact_dest(H5D_t *dset);

/*********************/
/* Package Variables */
/*********************/

/* Compact storage layout I/O ops */
const H5D_layout_ops_t H5D_LOPS_COMPACT[1] = {{
    H5D__compact_construct,      /* construct */
    NULL,                        /* init */
    H5D__compact_is_space_alloc, /* is_space_alloc */
    NULL,                        /* is_data_cached */
    H5D__compact_io_init,        /* io_init */
    NULL,                        /* mdio_init */
    H5D__contig_read,            /* ser_read */
    H5D__contig_write,           /* ser_write */
    H5D__compact_readvv,         /* readvv */
    H5D__compact_writevv,        /* writevv */
    H5D__compact_flush,          /* flush */
    NULL,                        /* io_term */
    H5D__compact_dest            /* dest */
}};

/*******************/
/* Local Variables */
/*******************/

/* Declare extern the free list to manage blocks of type conversion data */
H5FL_BLK_EXTERN(type_conv);

/*-------------------------------------------------------------------------
 * Function:	H5D__compact_fill
 *
 * Purpose:	Write fill values to a compactly stored dataset.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__compact_fill(const H5D_t *dset)
{
    H5D_fill_buf_info_t fb_info;                /* Dataset's fill buffer info */
    bool                fb_info_init = false;   /* Whether the fill value buffer has been initialized */
    herr_t              ret_value    = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(dset && H5D_COMPACT == dset->shared->layout.type);
    assert(dset->shared->layout.storage.u.compact.buf);
    assert(dset->shared->type);
    assert(dset->shared->space);

    /* Initialize the fill value buffer */
    /* (use the compact dataset storage buffer as the fill value buffer) */
    if (H5D__fill_init(&fb_info, dset->shared->layout.storage.u.compact.buf, NULL, NULL, NULL, NULL,
                       &dset->shared->dcpl_cache.fill, dset->shared->type, dset->shared->type_id, (size_t)0,
                       dset->shared->layout.storage.u.compact.size) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't initialize fill buffer info");
    fb_info_init = true;

    /* Check for VL datatype & non-default fill value */
    if (fb_info.has_vlen_fill_type)
        /* Fill the buffer with VL datatype fill values */
        if (H5D__fill_refill_vl(&fb_info, fb_info.elmts_per_buf) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTCONVERT, FAIL, "can't refill fill value buffer");

done:
    /* Release the fill buffer info, if it's been initialized */
    if (fb_info_init && H5D__fill_term(&fb_info) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "Can't release fill buffer info");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__compact_fill() */

/*-------------------------------------------------------------------------
 * Function:	H5D__compact_construct
 *
 * Purpose:	Constructs new compact layout information for dataset
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__compact_construct(H5F_t *f, H5D_t *dset)
{
    hssize_t stmp_size;           /* Temporary holder for raw data size */
    hsize_t  tmp_size;            /* Temporary holder for raw data size */
    hsize_t  max_comp_data_size;  /* Max. allowed size of compact data */
    unsigned u;                   /* Local index variable */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(f);
    assert(dset);

    /* Check for invalid dataset dimensions */
    for (u = 0; u < dset->shared->ndims; u++)
        if (dset->shared->max_dims[u] > dset->shared->curr_dims[u])
            HGOTO_ERROR(H5E_DATASET, H5E_UNSUPPORTED, FAIL, "extendible compact dataset not allowed");

    /*
     * Compact dataset is stored in dataset object header message of
     * layout.
     */
    stmp_size = H5S_GET_EXTENT_NPOINTS(dset->shared->space);
    assert(stmp_size >= 0);
    tmp_size = H5T_get_size(dset->shared->type);
    assert(tmp_size > 0);
    tmp_size = tmp_size * (hsize_t)stmp_size;
    H5_CHECKED_ASSIGN(dset->shared->layout.storage.u.compact.size, size_t, tmp_size, hssize_t);

    /* Verify data size is smaller than maximum header message size
     * (64KB) minus other layout message fields.
     */
    max_comp_data_size = H5O_MESG_MAX_SIZE - H5D__layout_meta_size(f, &(dset->shared->layout), false);
    if (dset->shared->layout.storage.u.compact.size > max_comp_data_size)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                    "compact dataset size is bigger than header message maximum size");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__compact_construct() */

/*-------------------------------------------------------------------------
 * Function:	H5D__compact_is_space_alloc
 *
 * Purpose:	Query if space is allocated for layout
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static bool
H5D__compact_is_space_alloc(const H5O_storage_t H5_ATTR_UNUSED *storage)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(storage);

    /* Compact storage is currently always allocated */
    FUNC_LEAVE_NOAPI(true)
} /* end H5D__compact_is_space_alloc() */

/*-------------------------------------------------------------------------
 * Function:	H5D__compact_io_init
 *
 * Purpose:	Performs initialization before any sort of I/O on the raw data
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__compact_io_init(H5D_io_info_t *io_info, H5D_dset_io_info_t *dinfo)
{
    FUNC_ENTER_PACKAGE_NOERR

    dinfo->store->compact.buf               = dinfo->dset->shared->layout.storage.u.compact.buf;
    dinfo->store->compact.dirty             = &dinfo->dset->shared->layout.storage.u.compact.dirty;
    dinfo->layout_io_info.contig_piece_info = NULL;

    /* Disable selection I/O */
    io_info->use_select_io = H5D_SELECTION_IO_MODE_OFF;
    io_info->no_selection_io_cause |= H5D_SEL_IO_NOT_CONTIGUOUS_OR_CHUNKED_DATASET;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__compact_io_init() */

/*-------------------------------------------------------------------------
 * Function:    H5D__compact_iovv_memmanage_cb
 *
 * Purpose:     Callback operator for H5D__compact_readvv()/_writevv() to
 *              send a memory copy request to the underlying file driver.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__compact_iovv_memmanage_cb(hsize_t dst_off, hsize_t src_off, size_t len, void *_udata)
{
    H5D_compact_iovv_memmanage_ud_t *udata = (H5D_compact_iovv_memmanage_ud_t *)_udata;
    H5FD_ctl_memcpy_args_t           op_args;
    uint64_t                         op_flags;
    H5FD_t                          *file_handle = NULL;
    herr_t                           ret_value   = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Retrieve pointer to file driver structure for ctl call */
    if (H5F_shared_get_file_driver(udata->f_sh, &file_handle) < 0)
        HGOTO_ERROR(H5E_IO, H5E_CANTGET, FAIL, "can't get file handle");

    /* Setup operation flags and arguments */
    op_flags = H5FD_CTL_ROUTE_TO_TERMINAL_VFD_FLAG | H5FD_CTL_FAIL_IF_UNKNOWN_FLAG;

    op_args.dstbuf  = udata->dstbuf;
    op_args.dst_off = dst_off;
    op_args.srcbuf  = udata->srcbuf;
    op_args.src_off = src_off;
    op_args.len     = len;

    /* Make request to file driver */
    if (H5FD_ctl(file_handle, H5FD_CTL_MEM_COPY, op_flags, &op_args, NULL) < 0)
        HGOTO_ERROR(H5E_IO, H5E_FCNTL, FAIL, "VFD memcpy request failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__compact_iovv_memmanage_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5D__compact_readvv
 *
 * Purpose:     Reads some data vectors from a dataset into a buffer.
 *              The data is in compact dataset.  The address is relative
 *              to the beginning address of the dataset.  The offsets and
 *              sequence lengths are in bytes.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Notes:
 *              Offsets in the sequences must be monotonically increasing
 *
 *-------------------------------------------------------------------------
 */
static ssize_t
H5D__compact_readvv(const H5D_io_info_t *io_info, const H5D_dset_io_info_t *dset_info, size_t dset_max_nseq,
                    size_t *dset_curr_seq, size_t dset_size_arr[], hsize_t dset_offset_arr[],
                    size_t mem_max_nseq, size_t *mem_curr_seq, size_t mem_size_arr[],
                    hsize_t mem_offset_arr[])
{
    ssize_t ret_value = -1; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(io_info);
    assert(dset_info);

    /* Check if file driver wishes to do its own memory management */
    if (H5F_SHARED_HAS_FEATURE(io_info->f_sh, H5FD_FEAT_MEMMANAGE)) {
        H5D_compact_iovv_memmanage_ud_t udata;

        /* Set up udata for memory copy operation */
        udata.f_sh   = io_info->f_sh;
        udata.dstbuf = dset_info->buf.vp;
        udata.srcbuf = dset_info->store->compact.buf;

        /* Request that file driver does the memory copy */
        if ((ret_value = H5VM_opvv(mem_max_nseq, mem_curr_seq, mem_size_arr, mem_offset_arr, dset_max_nseq,
                                   dset_curr_seq, dset_size_arr, dset_offset_arr,
                                   H5D__compact_iovv_memmanage_cb, &udata)) < 0)
            HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "vectorized memcpy failed");
    }
    else {
        /* Use the vectorized memory copy routine to do actual work */
        if ((ret_value = H5VM_memcpyvv(dset_info->buf.vp, mem_max_nseq, mem_curr_seq, mem_size_arr,
                                       mem_offset_arr, dset_info->store->compact.buf, dset_max_nseq,
                                       dset_curr_seq, dset_size_arr, dset_offset_arr)) < 0)
            HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "vectorized memcpy failed");
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__compact_readvv() */

/*-------------------------------------------------------------------------
 * Function:    H5D__compact_writevv
 *
 * Purpose:     Writes some data vectors from a dataset into a buffer.
 *              The data is in compact dataset.  The address is relative
 *              to the beginning address for the file.  The offsets and
 *              sequence lengths are in bytes.  This function only copies
 *              data into the buffer in the LAYOUT struct and mark it
 *              as DIRTY.  Later in H5D_close, the data is copied into
 *              header message in memory.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 * Notes:
 *              Offsets in the sequences must be monotonically increasing
 *
 *-------------------------------------------------------------------------
 */
static ssize_t
H5D__compact_writevv(const H5D_io_info_t *io_info, const H5D_dset_io_info_t *dset_info, size_t dset_max_nseq,
                     size_t *dset_curr_seq, size_t dset_size_arr[], hsize_t dset_offset_arr[],
                     size_t mem_max_nseq, size_t *mem_curr_seq, size_t mem_size_arr[],
                     hsize_t mem_offset_arr[])
{
    ssize_t ret_value = -1; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(io_info);
    assert(dset_info);

    /* Check if file driver wishes to do its own memory management */
    if (H5F_SHARED_HAS_FEATURE(io_info->f_sh, H5FD_FEAT_MEMMANAGE)) {
        H5D_compact_iovv_memmanage_ud_t udata;

        /* Set up udata for memory copy operation */
        udata.f_sh   = io_info->f_sh;
        udata.dstbuf = dset_info->store->compact.buf;
        udata.srcbuf = dset_info->buf.cvp;

        /* Request that file driver does the memory copy */
        if ((ret_value = H5VM_opvv(dset_max_nseq, dset_curr_seq, dset_size_arr, dset_offset_arr, mem_max_nseq,
                                   mem_curr_seq, mem_size_arr, mem_offset_arr, H5D__compact_iovv_memmanage_cb,
                                   &udata)) < 0)
            HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "vectorized memcpy failed");
    }
    else {
        /* Use the vectorized memory copy routine to do actual work */
        if ((ret_value = H5VM_memcpyvv(dset_info->store->compact.buf, dset_max_nseq, dset_curr_seq,
                                       dset_size_arr, dset_offset_arr, dset_info->buf.cvp, mem_max_nseq,
                                       mem_curr_seq, mem_size_arr, mem_offset_arr)) < 0)
            HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "vectorized memcpy failed");
    }

    /* Mark the compact dataset's buffer as dirty */
    *dset_info->store->compact.dirty = true;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__compact_writevv() */

/*-------------------------------------------------------------------------
 * Function:	H5D__compact_flush
 *
 * Purpose:	Writes dirty compact data to object header
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__compact_flush(H5D_t *dset)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(dset);

    /* Check if the buffered compact information is dirty */
    if (dset->shared->layout.storage.u.compact.dirty) {
        dset->shared->layout.storage.u.compact.dirty = false;
        if (H5O_msg_write(&(dset->oloc), H5O_LAYOUT_ID, 0, H5O_UPDATE_TIME, &(dset->shared->layout)) < 0) {
            dset->shared->layout.storage.u.compact.dirty = true;
            HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "unable to update layout message");
        }
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__compact_flush() */

/*-------------------------------------------------------------------------
 * Function:	H5D__compact_dest
 *
 * Purpose:	Free the compact buffer
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__compact_dest(H5D_t *dset)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(dset);

    /* Free the buffer for the raw data for compact datasets */
    dset->shared->layout.storage.u.compact.buf = H5MM_xfree(dset->shared->layout.storage.u.compact.buf);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__compact_dest() */

/*-------------------------------------------------------------------------
 * Function:    H5D__compact_copy
 *
 * Purpose:     Copy compact storage raw data from SRC file to DST file.
 *
 * Return:      Non-negative on success, negative on failure.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__compact_copy(H5F_t *f_src, H5O_storage_compact_t *_storage_src, H5F_t *f_dst,
                  H5O_storage_compact_t *storage_dst, H5T_t *dt_src, H5O_copy_t *cpy_info)
{
    hid_t         tid_src     = -1;   /* Datatype ID for source datatype */
    hid_t         tid_dst     = -1;   /* Datatype ID for destination datatype */
    hid_t         tid_mem     = -1;   /* Datatype ID for memory datatype */
    void         *buf         = NULL; /* Buffer for copying data */
    void         *bkg         = NULL; /* Temporary buffer for copying data */
    void         *reclaim_buf = NULL; /* Buffer for reclaiming data */
    hid_t         buf_sid     = -1;   /* ID for buffer dataspace */
    H5D_shared_t *shared_fo =
        (H5D_shared_t *)cpy_info->shared_fo;           /* Pointer to the shared struct for dataset object */
    H5O_storage_compact_t *storage_src = _storage_src; /* Pointer to storage_src */
    herr_t                 ret_value   = SUCCEED;      /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(f_src);
    assert(storage_src);
    assert(f_dst);
    assert(storage_dst);
    assert(storage_dst->buf);
    assert(dt_src);

    /* If the dataset is open in the file, point to "layout" in the shared struct */
    if (shared_fo != NULL)
        storage_src = &(shared_fo->layout.storage.u.compact);

    /* Create datatype ID for src datatype, so it gets freed */
    if ((tid_src = H5I_register(H5I_DATATYPE, dt_src, false)) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTREGISTER, FAIL, "unable to register source file datatype");

    /* If there's a VLEN source datatype, do type conversion information */
    if (H5T_detect_class(dt_src, H5T_VLEN, false) > 0) {
        H5T_path_t *tpath_src_mem, *tpath_mem_dst; /* Datatype conversion paths */
        H5T_t      *dt_dst;                        /* Destination datatype */
        H5T_t      *dt_mem;                        /* Memory datatype */
        H5S_t      *buf_space;                     /* Dataspace describing buffer */
        size_t      buf_size;                      /* Size of copy buffer */
        size_t      nelmts;                        /* Number of elements in buffer */
        size_t      src_dt_size;                   /* Source datatype size */
        size_t      tmp_dt_size;                   /* Temporary datatype size */
        size_t      max_dt_size;                   /* Max atatype size */
        hsize_t     buf_dim;                       /* Dimension for buffer */

        /* create a memory copy of the variable-length datatype */
        if (NULL == (dt_mem = H5T_copy(dt_src, H5T_COPY_TRANSIENT)))
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to copy");
        if ((tid_mem = H5I_register(H5I_DATATYPE, dt_mem, false)) < 0) {
            (void)H5T_close_real(dt_mem);
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREGISTER, FAIL, "unable to register memory datatype");
        } /* end if */

        /* create variable-length datatype at the destination file */
        if (NULL == (dt_dst = H5T_copy(dt_src, H5T_COPY_TRANSIENT)))
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to copy");
        if (H5T_set_loc(dt_dst, H5F_VOL_OBJ(f_dst), H5T_LOC_DISK) < 0) {
            (void)H5T_close_real(dt_dst);
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "cannot mark datatype on disk");
        } /* end if */
        if ((tid_dst = H5I_register(H5I_DATATYPE, dt_dst, false)) < 0) {
            (void)H5T_close_real(dt_dst);
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTREGISTER, FAIL, "unable to register destination file datatype");
        } /* end if */

        /* Set up the conversion functions */
        if (NULL == (tpath_src_mem = H5T_path_find(dt_src, dt_mem)))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to convert between src and mem datatypes");
        if (NULL == (tpath_mem_dst = H5T_path_find(dt_mem, dt_dst)))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to convert between mem and dst datatypes");

        /* Determine largest datatype size */
        if (0 == (src_dt_size = H5T_get_size(dt_src)))
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to determine datatype size");
        if (0 == (tmp_dt_size = H5T_get_size(dt_mem)))
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to determine datatype size");
        max_dt_size = MAX(src_dt_size, tmp_dt_size);
        if (0 == (tmp_dt_size = H5T_get_size(dt_dst)))
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "unable to determine datatype size");
        max_dt_size = MAX(max_dt_size, tmp_dt_size);

        /* Set number of whole elements that fit in buffer */
        if (0 == (nelmts = storage_src->size / src_dt_size))
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "element size too large");

        /* Set up number of bytes to copy, and initial buffer size */
        buf_size = nelmts * max_dt_size;

        /* Create dataspace for number of elements in buffer */
        buf_dim = nelmts;

        /* Create the space and set the initial extent */
        if (NULL == (buf_space = H5S_create_simple((unsigned)1, &buf_dim, NULL)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTCREATE, FAIL, "can't create simple dataspace");

        /* Register */
        if ((buf_sid = H5I_register(H5I_DATASPACE, buf_space, false)) < 0) {
            H5S_close(buf_space);
            HGOTO_ERROR(H5E_ID, H5E_CANTREGISTER, FAIL, "unable to register dataspace ID");
        } /* end if */

        /* Allocate memory for recclaim buf */
        if (NULL == (reclaim_buf = H5FL_BLK_MALLOC(type_conv, buf_size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");

        /* Allocate memory for copying the chunk */
        if (NULL == (buf = H5FL_BLK_MALLOC(type_conv, buf_size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");

        H5MM_memcpy(buf, storage_src->buf, storage_src->size);

        /* allocate temporary bkg buff for data conversion */
        if (NULL == (bkg = H5FL_BLK_MALLOC(type_conv, buf_size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");

        /* Convert from source file to memory */
        if (H5T_convert(tpath_src_mem, tid_src, tid_mem, nelmts, (size_t)0, (size_t)0, buf, bkg) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "datatype conversion failed");

        /* Copy into another buffer, to reclaim memory later */
        H5MM_memcpy(reclaim_buf, buf, buf_size);

        /* Set background buffer to all zeros */
        memset(bkg, 0, buf_size);

        /* Convert from memory to destination file */
        if (H5T_convert(tpath_mem_dst, tid_mem, tid_dst, nelmts, (size_t)0, (size_t)0, buf, bkg) < 0)
            HGOTO_ERROR(H5E_DATATYPE, H5E_CANTINIT, FAIL, "datatype conversion failed");

        H5MM_memcpy(storage_dst->buf, buf, storage_dst->size);

        if (H5T_reclaim(tid_mem, buf_space, reclaim_buf) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_BADITER, FAIL, "unable to reclaim variable-length data");
    } /* end if */
    else if (H5T_get_class(dt_src, false) == H5T_REFERENCE) {
        if (f_src != f_dst) {
            /* Check for expanding references */
            if (cpy_info->expand_ref) {
                /* Copy objects referenced in source buffer to destination file and set destination elements
                 */
                if (H5O_copy_expand_ref(f_src, tid_src, dt_src, storage_src->buf, storage_src->size, f_dst,
                                        storage_dst->buf, cpy_info) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTCOPY, FAIL, "unable to copy reference attribute");
            } /* end if */
            else
                /* Reset value to zero */
                memset(storage_dst->buf, 0, storage_src->size);
        } /* end if */
        else
            /* Type conversion not necessary */
            H5MM_memcpy(storage_dst->buf, storage_src->buf, storage_src->size);
    } /* end if */
    else
        /* Type conversion not necessary */
        H5MM_memcpy(storage_dst->buf, storage_src->buf, storage_src->size);

    /* Mark destination buffer as dirty */
    storage_dst->dirty = true;

done:
    if (buf_sid > 0 && H5I_dec_ref(buf_sid) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "can't decrement temporary dataspace ID");
    if (tid_src > 0 && H5I_dec_ref(tid_src) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "Can't decrement temporary datatype ID");
    if (tid_dst > 0 && H5I_dec_ref(tid_dst) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "Can't decrement temporary datatype ID");
    if (tid_mem > 0 && H5I_dec_ref(tid_mem) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "Can't decrement temporary datatype ID");
    if (buf)
        buf = H5FL_BLK_FREE(type_conv, buf);
    if (reclaim_buf)
        reclaim_buf = H5FL_BLK_FREE(type_conv, reclaim_buf);
    if (bkg)
        bkg = H5FL_BLK_FREE(type_conv, bkg);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__compact_copy() */
