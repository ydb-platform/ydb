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
#include "H5private.h"   /* Generic Functions			*/
#include "H5CXprivate.h" /* API Contexts                         */
#include "H5Dpkg.h"      /* Dataset functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5FLprivate.h" /* Free Lists                           */
#include "H5Iprivate.h"  /* IDs                                  */
#include "H5MMprivate.h" /* Memory management			*/

/****************/
/* Local Macros */
/****************/

/* Macro to determine if we're using H5D__compound_opt_read() */
#define H5D__SCATGATH_USE_CMPD_OPT_READ(DSET_INFO, IN_PLACE_TCONV)                                           \
    ((DSET_INFO)->type_info.cmpd_subset && H5T_SUBSET_FALSE != (DSET_INFO)->type_info.cmpd_subset->subset && \
     !(IN_PLACE_TCONV))

/* Macro to determine if we're using H5D__compound_opt_write() */
#define H5D__SCATGATH_USE_CMPD_OPT_WRITE(DSET_INFO, IN_PLACE_TCONV)                                          \
    ((DSET_INFO)->type_info.cmpd_subset && H5T_SUBSET_DST == (DSET_INFO)->type_info.cmpd_subset->subset &&   \
     (DSET_INFO)->type_info.dst_type_size == (DSET_INFO)->type_info.cmpd_subset->copy_size &&                \
     !(IN_PLACE_TCONV))

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Local Prototypes */
/********************/
static herr_t H5D__scatter_file(const H5D_io_info_t *io_info, const H5D_dset_io_info_t *dset_info,
                                H5S_sel_iter_t *file_iter, size_t nelmts, const void *buf);
static size_t H5D__gather_file(const H5D_io_info_t *io_info, const H5D_dset_io_info_t *dset_info,
                               H5S_sel_iter_t *file_iter, size_t nelmts, void *buf);
static herr_t H5D__compound_opt_read(size_t nelmts, H5S_sel_iter_t *iter, const H5D_type_info_t *type_info,
                                     uint8_t *tconv_buf, void *user_buf /*out*/);
static herr_t H5D__compound_opt_write(size_t nelmts, const H5D_type_info_t *type_info, void *tconv_buf);

/*********************/
/* Package Variables */
/*********************/

/*******************/
/* Local Variables */
/*******************/

/* Declare extern free list to manage the H5S_sel_iter_t struct */
H5FL_EXTERN(H5S_sel_iter_t);

/* Declare extern free list to manage sequences of size_t */
H5FL_SEQ_EXTERN(size_t);

/* Declare extern free list to manage sequences of hsize_t */
H5FL_SEQ_EXTERN(hsize_t);

/*-------------------------------------------------------------------------
 * Function:	H5D__scatter_file
 *
 * Purpose:	Scatters dataset elements from the type conversion buffer BUF
 *		to the file F where the data points are arranged according to
 *		the file dataspace FILE_SPACE and stored according to
 *		LAYOUT and EFL. Each element is ELMT_SIZE bytes.
 *		The caller is requesting that NELMTS elements are copied.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__scatter_file(const H5D_io_info_t *_io_info, const H5D_dset_io_info_t *_dset_info, H5S_sel_iter_t *iter,
                  size_t nelmts, const void *_buf)
{
    H5D_io_info_t      tmp_io_info;           /* Temporary I/O info object */
    H5D_dset_io_info_t tmp_dset_info;         /* Temporary I/O info object */
    hsize_t           *off = NULL;            /* Pointer to sequence offsets */
    hsize_t            mem_off;               /* Offset in memory */
    size_t             mem_curr_seq;          /* "Current sequence" in memory */
    size_t             dset_curr_seq;         /* "Current sequence" in dataset */
    size_t            *len = NULL;            /* Array to store sequence lengths */
    size_t             orig_mem_len, mem_len; /* Length of sequence in memory */
    size_t             nseq;                  /* Number of sequences generated */
    size_t             nelem;                 /* Number of elements used in sequences */
    size_t             dxpl_vec_size;         /* Vector length from API context's DXPL */
    size_t             vec_size;              /* Vector length */
    herr_t             ret_value = SUCCEED;   /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(_io_info);
    assert(_dset_info);
    assert(_dset_info->dset);
    assert(_dset_info->store);
    assert(iter);
    assert(nelmts > 0);
    assert(_buf);

    /* Set up temporary I/O info object */
    H5MM_memcpy(&tmp_io_info, _io_info, sizeof(*_io_info));
    H5MM_memcpy(&tmp_dset_info, _dset_info, sizeof(*_dset_info));
    tmp_io_info.op_type    = H5D_IO_OP_WRITE;
    tmp_dset_info.buf.cvp  = _buf;
    tmp_io_info.dsets_info = &tmp_dset_info;

    /* Get info from API context */
    if (H5CX_get_vec_size(&dxpl_vec_size) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't retrieve I/O vector size");

    /* Allocate the vector I/O arrays */
    if (dxpl_vec_size > H5D_IO_VECTOR_SIZE)
        vec_size = dxpl_vec_size;
    else
        vec_size = H5D_IO_VECTOR_SIZE;
    if (NULL == (len = H5FL_SEQ_MALLOC(size_t, vec_size)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate I/O length vector array");
    if (NULL == (off = H5FL_SEQ_MALLOC(hsize_t, vec_size)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate I/O offset vector array");

    /* Loop until all elements are written */
    while (nelmts > 0) {
        /* Get list of sequences for selection to write */
        if (H5S_SELECT_ITER_GET_SEQ_LIST(iter, vec_size, nelmts, &nseq, &nelem, off, len) < 0)
            HGOTO_ERROR(H5E_INTERNAL, H5E_UNSUPPORTED, FAIL, "sequence length generation failed");

        /* Reset the current sequence information */
        mem_curr_seq = dset_curr_seq = 0;
        orig_mem_len = mem_len = nelem * iter->elmt_size;
        mem_off                = 0;

        /* Write sequence list out */
        if ((*tmp_dset_info.layout_ops.writevv)(&tmp_io_info, &tmp_dset_info, nseq, &dset_curr_seq, len, off,
                                                (size_t)1, &mem_curr_seq, &mem_len, &mem_off) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_WRITEERROR, FAIL, "write error");

        /* Update buffer */
        tmp_dset_info.buf.cvp = (const uint8_t *)tmp_dset_info.buf.cvp + orig_mem_len;

        /* Decrement number of elements left to process */
        nelmts -= nelem;
    } /* end while */

done:
    /* Release resources, if allocated */
    if (len)
        len = H5FL_SEQ_FREE(size_t, len);
    if (off)
        off = H5FL_SEQ_FREE(hsize_t, off);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__scatter_file() */

/*-------------------------------------------------------------------------
 * Function:	H5D__gather_file
 *
 * Purpose:	Gathers data points from file F and accumulates them in the
 *		type conversion buffer BUF.  The LAYOUT argument describes
 *		how the data is stored on disk and EFL describes how the data
 *		is organized in external files.  ELMT_SIZE is the size in
 *		bytes of a datum which this function treats as opaque.
 *		FILE_SPACE describes the dataspace of the dataset on disk
 *		and the elements that have been selected for reading (via
 *		hyperslab, etc).  This function will copy at most NELMTS
 *		elements.
 *
 * Return:	Success:	Number of elements copied.
 *		Failure:	0
 *
 *-------------------------------------------------------------------------
 */
static size_t
H5D__gather_file(const H5D_io_info_t *_io_info, const H5D_dset_io_info_t *_dset_info, H5S_sel_iter_t *iter,
                 size_t nelmts, void *_buf /*out*/)
{
    H5D_io_info_t      tmp_io_info;           /* Temporary I/O info object */
    H5D_dset_io_info_t tmp_dset_info;         /* Temporary I/O info object */
    hsize_t           *off = NULL;            /* Pointer to sequence offsets */
    hsize_t            mem_off;               /* Offset in memory */
    size_t             mem_curr_seq;          /* "Current sequence" in memory */
    size_t             dset_curr_seq;         /* "Current sequence" in dataset */
    size_t            *len = NULL;            /* Pointer to sequence lengths */
    size_t             orig_mem_len, mem_len; /* Length of sequence in memory */
    size_t             nseq;                  /* Number of sequences generated */
    size_t             nelem;                 /* Number of elements used in sequences */
    size_t             dxpl_vec_size;         /* Vector length from API context's DXPL */
    size_t             vec_size;              /* Vector length */
    size_t             ret_value = nelmts;    /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(_io_info);
    assert(_dset_info);
    assert(_dset_info->dset);
    assert(_dset_info->store);
    assert(iter);
    assert(nelmts > 0);
    assert(_buf);

    /* Set up temporary I/O info object */
    H5MM_memcpy(&tmp_io_info, _io_info, sizeof(*_io_info));
    H5MM_memcpy(&tmp_dset_info, _dset_info, sizeof(*_dset_info));
    tmp_io_info.op_type    = H5D_IO_OP_READ;
    tmp_dset_info.buf.vp   = _buf;
    tmp_io_info.dsets_info = &tmp_dset_info;

    /* Get info from API context */
    if (H5CX_get_vec_size(&dxpl_vec_size) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, 0, "can't retrieve I/O vector size");

    /* Allocate the vector I/O arrays */
    if (dxpl_vec_size > H5D_IO_VECTOR_SIZE)
        vec_size = dxpl_vec_size;
    else
        vec_size = H5D_IO_VECTOR_SIZE;
    if (NULL == (len = H5FL_SEQ_MALLOC(size_t, vec_size)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, 0, "can't allocate I/O length vector array");
    if (NULL == (off = H5FL_SEQ_MALLOC(hsize_t, vec_size)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, 0, "can't allocate I/O offset vector array");

    /* Loop until all elements are read */
    while (nelmts > 0) {
        /* Get list of sequences for selection to read */
        if (H5S_SELECT_ITER_GET_SEQ_LIST(iter, vec_size, nelmts, &nseq, &nelem, off, len) < 0)
            HGOTO_ERROR(H5E_INTERNAL, H5E_UNSUPPORTED, 0, "sequence length generation failed");

        /* Reset the current sequence information */
        mem_curr_seq = dset_curr_seq = 0;
        orig_mem_len = mem_len = nelem * iter->elmt_size;
        mem_off                = 0;

        /* Read sequence list in */
        if ((*tmp_dset_info.layout_ops.readvv)(&tmp_io_info, &tmp_dset_info, nseq, &dset_curr_seq, len, off,
                                               (size_t)1, &mem_curr_seq, &mem_len, &mem_off) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_READERROR, 0, "read error");

        /* Update buffer */
        tmp_dset_info.buf.vp = (uint8_t *)tmp_dset_info.buf.vp + orig_mem_len;

        /* Decrement number of elements left to process */
        nelmts -= nelem;
    } /* end while */

done:
    /* Release resources, if allocated */
    if (len)
        len = H5FL_SEQ_FREE(size_t, len);
    if (off)
        off = H5FL_SEQ_FREE(hsize_t, off);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__gather_file() */

/*-------------------------------------------------------------------------
 * Function:	H5D__scatter_mem
 *
 * Purpose:	Scatters NELMTS data points from the scatter buffer
 *		TSCAT_BUF to the application buffer BUF.  Each element is
 *		ELMT_SIZE bytes and they are organized in application memory
 *		according to SPACE.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__scatter_mem(const void *_tscat_buf, H5S_sel_iter_t *iter, size_t nelmts, void *_buf /*out*/)
{
    uint8_t       *buf       = (uint8_t *)_buf; /* Get local copies for address arithmetic */
    const uint8_t *tscat_buf = (const uint8_t *)_tscat_buf;
    hsize_t       *off       = NULL;    /* Pointer to sequence offsets */
    size_t        *len       = NULL;    /* Pointer to sequence lengths */
    size_t         curr_len;            /* Length of bytes left to process in sequence */
    size_t         nseq;                /* Number of sequences generated */
    size_t         curr_seq;            /* Current sequence being processed */
    size_t         nelem;               /* Number of elements used in sequences */
    size_t         dxpl_vec_size;       /* Vector length from API context's DXPL */
    size_t         vec_size;            /* Vector length */
    herr_t         ret_value = SUCCEED; /* Number of elements scattered */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(tscat_buf);
    assert(iter);
    assert(nelmts > 0);
    assert(buf);

    /* Get info from API context */
    if (H5CX_get_vec_size(&dxpl_vec_size) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't retrieve I/O vector size");

    /* Allocate the vector I/O arrays */
    if (dxpl_vec_size > H5D_IO_VECTOR_SIZE)
        vec_size = dxpl_vec_size;
    else
        vec_size = H5D_IO_VECTOR_SIZE;
    if (NULL == (len = H5FL_SEQ_MALLOC(size_t, vec_size)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate I/O length vector array");
    if (NULL == (off = H5FL_SEQ_MALLOC(hsize_t, vec_size)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate I/O offset vector array");

    /* Loop until all elements are written */
    while (nelmts > 0) {
        /* Get list of sequences for selection to write */
        if (H5S_SELECT_ITER_GET_SEQ_LIST(iter, vec_size, nelmts, &nseq, &nelem, off, len) < 0)
            HGOTO_ERROR(H5E_INTERNAL, H5E_UNSUPPORTED, 0, "sequence length generation failed");

        /* Loop, while sequences left to process */
        for (curr_seq = 0; curr_seq < nseq; curr_seq++) {
            /* Get the number of bytes in sequence */
            curr_len = len[curr_seq];

            H5MM_memcpy(buf + off[curr_seq], tscat_buf, curr_len);

            /* Advance offset in destination buffer */
            tscat_buf += curr_len;
        } /* end for */

        /* Decrement number of elements left to process */
        nelmts -= nelem;
    } /* end while */

done:
    /* Release resources, if allocated */
    if (len)
        len = H5FL_SEQ_FREE(size_t, len);
    if (off)
        off = H5FL_SEQ_FREE(hsize_t, off);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__scatter_mem() */

/*-------------------------------------------------------------------------
 * Function:	H5D__gather_mem
 *
 * Purpose:	Gathers dataset elements from application memory BUF and
 *		copies them into the gather buffer TGATH_BUF.
 *		Each element is ELMT_SIZE bytes and arranged in application
 *		memory according to SPACE.
 *		The caller is requesting that exactly NELMTS be gathered.
 *
 * Return:	Success:	Number of elements copied.
 *		Failure:	0
 *
 *-------------------------------------------------------------------------
 */
size_t
H5D__gather_mem(const void *_buf, H5S_sel_iter_t *iter, size_t nelmts, void *_tgath_buf /*out*/)
{
    const uint8_t *buf       = (const uint8_t *)_buf; /* Get local copies for address arithmetic */
    uint8_t       *tgath_buf = (uint8_t *)_tgath_buf;
    hsize_t       *off       = NULL;   /* Pointer to sequence offsets */
    size_t        *len       = NULL;   /* Pointer to sequence lengths */
    size_t         curr_len;           /* Length of bytes left to process in sequence */
    size_t         nseq;               /* Number of sequences generated */
    size_t         curr_seq;           /* Current sequence being processed */
    size_t         nelem;              /* Number of elements used in sequences */
    size_t         dxpl_vec_size;      /* Vector length from API context's DXPL */
    size_t         vec_size;           /* Vector length */
    size_t         ret_value = nelmts; /* Number of elements gathered */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(buf);
    assert(iter);
    assert(nelmts > 0);
    assert(tgath_buf);

    /* Get info from API context */
    if (H5CX_get_vec_size(&dxpl_vec_size) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, 0, "can't retrieve I/O vector size");

    /* Allocate the vector I/O arrays */
    if (dxpl_vec_size > H5D_IO_VECTOR_SIZE)
        vec_size = dxpl_vec_size;
    else
        vec_size = H5D_IO_VECTOR_SIZE;
    if (NULL == (len = H5FL_SEQ_MALLOC(size_t, vec_size)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, 0, "can't allocate I/O length vector array");
    if (NULL == (off = H5FL_SEQ_MALLOC(hsize_t, vec_size)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, 0, "can't allocate I/O offset vector array");

    /* Loop until all elements are written */
    while (nelmts > 0) {
        /* Get list of sequences for selection to write */
        if (H5S_SELECT_ITER_GET_SEQ_LIST(iter, vec_size, nelmts, &nseq, &nelem, off, len) < 0)
            HGOTO_ERROR(H5E_INTERNAL, H5E_UNSUPPORTED, 0, "sequence length generation failed");

        /* Loop, while sequences left to process */
        for (curr_seq = 0; curr_seq < nseq; curr_seq++) {
            /* Get the number of bytes in sequence */
            curr_len = len[curr_seq];

            H5MM_memcpy(tgath_buf, buf + off[curr_seq], curr_len);

            /* Advance offset in gather buffer */
            tgath_buf += curr_len;
        } /* end for */

        /* Decrement number of elements left to process */
        nelmts -= nelem;
    } /* end while */

done:
    /* Release resources, if allocated */
    if (len)
        len = H5FL_SEQ_FREE(size_t, len);
    if (off)
        off = H5FL_SEQ_FREE(hsize_t, off);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__gather_mem() */

/*-------------------------------------------------------------------------
 * Function:	H5D__scatgath_read
 *
 * Purpose:	Perform scatter/gather ead from a contiguous [piece of a] dataset.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__scatgath_read(const H5D_io_info_t *io_info, const H5D_dset_io_info_t *dset_info)
{
    void           *buf;                    /* Local pointer to application buffer */
    void           *tmp_buf;                /* Buffer to use for type conversion */
    H5S_sel_iter_t *mem_iter       = NULL;  /* Memory selection iteration info*/
    bool            mem_iter_init  = false; /* Memory selection iteration info has been initialized */
    H5S_sel_iter_t *bkg_iter       = NULL;  /* Background iteration info*/
    bool            bkg_iter_init  = false; /* Background iteration info has been initialized */
    H5S_sel_iter_t *file_iter      = NULL;  /* File selection iteration info*/
    bool            file_iter_init = false; /* File selection iteration info has been initialized */
    hsize_t         smine_start;            /* Strip mine start loc	*/
    size_t          smine_nelmts;           /* Elements per strip	*/
    bool            in_place_tconv;         /* Whether to perform in-place type_conversion */
    herr_t          ret_value = SUCCEED;    /* Return value		*/

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(io_info);
    assert(dset_info);
    assert(dset_info->mem_space);
    assert(dset_info->file_space);
    assert(dset_info->buf.vp);

    /* Set buf pointer */
    buf = dset_info->buf.vp;

    /* Check for NOOP read */
    if (dset_info->nelmts == 0)
        HGOTO_DONE(SUCCEED);

    /* Check for in-place type conversion */
    in_place_tconv = dset_info->layout_io_info.contig_piece_info &&
                     dset_info->layout_io_info.contig_piece_info->in_place_tconv;

    /* Check if we should disable in-place type conversion for performance.  Do so if we can use the optimized
     * compound read function, if this is not a selection I/O operation (so we have normal size conversion
     * buffers), and the either entire I/O operation can fit in the type conversion buffer or we need to use a
     * background buffer (and therefore could not do the I/O in one operation with in-place conversion
     * anyways). */
    if (in_place_tconv && H5D__SCATGATH_USE_CMPD_OPT_READ(dset_info, false) &&
        (io_info->use_select_io != H5D_SELECTION_IO_MODE_ON) &&
        (dset_info->type_info.need_bkg || (dset_info->nelmts <= dset_info->type_info.request_nelmts)))
        in_place_tconv = false;

    /* Allocate the iterators */
    if (NULL == (mem_iter = H5FL_MALLOC(H5S_sel_iter_t)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate memory iterator");
    if (NULL == (bkg_iter = H5FL_MALLOC(H5S_sel_iter_t)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate background iterator");
    if (NULL == (file_iter = H5FL_MALLOC(H5S_sel_iter_t)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate file iterator");

    /* Figure out the strip mine size. */
    if (H5S_select_iter_init(file_iter, dset_info->file_space, dset_info->type_info.src_type_size,
                             H5S_SEL_ITER_GET_SEQ_LIST_SORTED) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to initialize file selection information");
    file_iter_init = true; /*file selection iteration info has been initialized */
    if (H5S_select_iter_init(mem_iter, dset_info->mem_space, dset_info->type_info.dst_type_size, 0) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to initialize memory selection information");
    mem_iter_init = true; /*file selection iteration info has been initialized */
    if (H5S_select_iter_init(bkg_iter, dset_info->mem_space, dset_info->type_info.dst_type_size, 0) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to initialize background selection information");
    bkg_iter_init = true; /*file selection iteration info has been initialized */

    /* Start strip mining... */
    for (smine_start = 0; smine_start < dset_info->nelmts; smine_start += smine_nelmts) {
        size_t n; /* Elements operated on */

        assert(H5S_SELECT_ITER_NELMTS(file_iter) == (dset_info->nelmts - smine_start));

        /* Determine strip mine size. First check if we're doing in-place type conversion */
        if (in_place_tconv) {
            /* If this is not a selection I/O operation and there is a background buffer, we cannot exceed
             * request_nelmts.  It could be part of a selection I/O operation if this read is used to fill in
             * a nonexistent chunk */
            assert(!H5D__SCATGATH_USE_CMPD_OPT_READ(dset_info, in_place_tconv));
            if (dset_info->type_info.need_bkg && (io_info->use_select_io != H5D_SELECTION_IO_MODE_ON))
                smine_nelmts =
                    (size_t)MIN(dset_info->type_info.request_nelmts, (dset_info->nelmts - smine_start));
            else {
                assert(smine_start == 0);
                smine_nelmts = dset_info->nelmts;
            }

            /* Calculate buffer position in user buffer */
            tmp_buf = (uint8_t *)buf + dset_info->layout_io_info.contig_piece_info->buf_off +
                      (smine_start * dset_info->type_info.dst_type_size);
        }
        else {
            /* Do type conversion using intermediate buffer */
            tmp_buf = io_info->tconv_buf;

            /* Go figure out how many elements to read from the file */
            smine_nelmts =
                (size_t)MIN(dset_info->type_info.request_nelmts, (dset_info->nelmts - smine_start));
        }

        /*
         * Gather the data from disk into the datatype conversion
         * buffer. Also gather data from application to background buffer
         * if necessary.
         */

        /* Fill background buffer here unless we will use H5D__compound_opt_read().  Must do this before
         * the read so the read buffer doesn't get wiped out if we're using in-place type conversion */
        if ((H5T_BKG_YES == dset_info->type_info.need_bkg) &&
            !H5D__SCATGATH_USE_CMPD_OPT_READ(dset_info, in_place_tconv)) {
            n = H5D__gather_mem(buf, bkg_iter, smine_nelmts, io_info->bkg_buf /*out*/);
            if (n != smine_nelmts)
                HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "mem gather failed");
        }

        /*
         * Gather data
         */
        n = H5D__gather_file(io_info, dset_info, file_iter, smine_nelmts, tmp_buf /*out*/);
        if (n != smine_nelmts)
            HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "file gather failed");

        /* If the source and destination are compound types and subset of each other
         * and no conversion is needed, copy the data directly into user's buffer and
         * bypass the rest of steps.
         */
        if (H5D__SCATGATH_USE_CMPD_OPT_READ(dset_info, in_place_tconv)) {
            if (H5D__compound_opt_read(smine_nelmts, mem_iter, &dset_info->type_info, tmp_buf, buf /*out*/) <
                0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "datatype conversion failed");
        } /* end if */
        else {
            /*
             * Perform datatype conversion.
             */
            if (H5T_convert(dset_info->type_info.tpath, dset_info->type_info.src_type_id,
                            dset_info->type_info.dst_type_id, smine_nelmts, (size_t)0, (size_t)0, tmp_buf,
                            io_info->bkg_buf) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTCONVERT, FAIL, "datatype conversion failed");

            /* Do the data transform after the conversion (since we're using type mem_type) */
            if (!dset_info->type_info.is_xform_noop) {
                H5Z_data_xform_t *data_transform; /* Data transform info */

                /* Retrieve info from API context */
                if (H5CX_get_data_transform(&data_transform) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get data transform info");

                if (H5Z_xform_eval(data_transform, tmp_buf, smine_nelmts, dset_info->type_info.mem_type) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "Error performing data transform");
            }

            /* Scatter the data into memory if this was not an in-place conversion */
            if (!in_place_tconv)
                if (H5D__scatter_mem(tmp_buf, mem_iter, smine_nelmts, buf /*out*/) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "scatter failed");
        } /* end else */
    }     /* end for */

done:
    /* Release selection iterators */
    if (file_iter_init && H5S_SELECT_ITER_RELEASE(file_iter) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "Can't release selection iterator");
    if (file_iter)
        file_iter = H5FL_FREE(H5S_sel_iter_t, file_iter);
    if (mem_iter_init && H5S_SELECT_ITER_RELEASE(mem_iter) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "Can't release selection iterator");
    if (mem_iter)
        mem_iter = H5FL_FREE(H5S_sel_iter_t, mem_iter);
    if (bkg_iter_init && H5S_SELECT_ITER_RELEASE(bkg_iter) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "Can't release selection iterator");
    if (bkg_iter)
        bkg_iter = H5FL_FREE(H5S_sel_iter_t, bkg_iter);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__scatgath_read() */

/*-------------------------------------------------------------------------
 * Function:	H5D__scatgath_write
 *
 * Purpose:	Perform scatter/gather write to a contiguous [piece of a] dataset.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__scatgath_write(const H5D_io_info_t *io_info, const H5D_dset_io_info_t *dset_info)
{
    const void     *buf;                    /* Local pointer to application buffer */
    void           *tmp_buf;                /* Buffer to use for type conversion */
    H5S_sel_iter_t *mem_iter       = NULL;  /* Memory selection iteration info*/
    bool            mem_iter_init  = false; /* Memory selection iteration info has been initialized */
    H5S_sel_iter_t *bkg_iter       = NULL;  /* Background iteration info*/
    bool            bkg_iter_init  = false; /* Background iteration info has been initialized */
    H5S_sel_iter_t *file_iter      = NULL;  /* File selection iteration info*/
    bool            file_iter_init = false; /* File selection iteration info has been initialized */
    hsize_t         smine_start;            /* Strip mine start loc	*/
    size_t          smine_nelmts;           /* Elements per strip	*/
    bool            in_place_tconv;         /* Whether to perform in-place type_conversion */
    herr_t          ret_value = SUCCEED;    /* Return value		*/

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(io_info);
    assert(dset_info);
    assert(dset_info->mem_space);
    assert(dset_info->file_space);
    assert(dset_info->buf.cvp);

    /* Set buf pointer */
    buf = dset_info->buf.cvp;

    /* Check for NOOP write */
    if (dset_info->nelmts == 0)
        HGOTO_DONE(SUCCEED);

    /* Check for in-place type conversion */
    in_place_tconv = dset_info->layout_io_info.contig_piece_info &&
                     dset_info->layout_io_info.contig_piece_info->in_place_tconv;

    /* Check if we should disable in-place type conversion for performance.  Do so if we can use the optimized
     * compound write function, if this is not a selection I/O operation (so we have normal size conversion
     * buffers), and the either entire I/O operation can fit in the type conversion buffer or we need to use a
     * background buffer (and therefore could not do the I/O in one operation with in-place conversion
     * anyways). */
    if (in_place_tconv && H5D__SCATGATH_USE_CMPD_OPT_WRITE(dset_info, false) &&
        (io_info->use_select_io != H5D_SELECTION_IO_MODE_ON) &&
        (dset_info->type_info.need_bkg || (dset_info->nelmts <= dset_info->type_info.request_nelmts)))
        in_place_tconv = false;

    /* Allocate the iterators */
    if (NULL == (mem_iter = H5FL_MALLOC(H5S_sel_iter_t)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate memory iterator");
    if (NULL == (bkg_iter = H5FL_MALLOC(H5S_sel_iter_t)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate background iterator");
    if (NULL == (file_iter = H5FL_MALLOC(H5S_sel_iter_t)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate file iterator");

    /* Figure out the strip mine size. */
    if (H5S_select_iter_init(file_iter, dset_info->file_space, dset_info->type_info.dst_type_size,
                             H5S_SEL_ITER_GET_SEQ_LIST_SORTED) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to initialize file selection information");
    file_iter_init = true; /*file selection iteration info has been initialized */
    if (H5S_select_iter_init(mem_iter, dset_info->mem_space, dset_info->type_info.src_type_size, 0) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to initialize memory selection information");
    mem_iter_init = true; /*file selection iteration info has been initialized */
    if (H5S_select_iter_init(bkg_iter, dset_info->file_space, dset_info->type_info.dst_type_size,
                             H5S_SEL_ITER_GET_SEQ_LIST_SORTED) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "unable to initialize background selection information");
    bkg_iter_init = true; /*file selection iteration info has been initialized */

    /* Start strip mining... */
    for (smine_start = 0; smine_start < dset_info->nelmts; smine_start += smine_nelmts) {
        size_t n; /* Elements operated on */

        assert(H5S_SELECT_ITER_NELMTS(file_iter) == (dset_info->nelmts - smine_start));

        /* Determine strip mine size. First check if we're doing in-place type conversion */
        if (in_place_tconv) {
            /* If this is not a selection I/O operation and there is a background buffer, we cannot exceed
             * request_nelmts.  It could be part of a selection I/O operation if this is used to write the
             * fill value to a cached chunk that will immediately be evicted. */
            assert(!H5D__SCATGATH_USE_CMPD_OPT_WRITE(dset_info, in_place_tconv));
            if (dset_info->type_info.need_bkg && (io_info->use_select_io != H5D_SELECTION_IO_MODE_ON))
                smine_nelmts =
                    (size_t)MIN(dset_info->type_info.request_nelmts, (dset_info->nelmts - smine_start));
            else {
                assert(smine_start == 0);
                smine_nelmts = dset_info->nelmts;
            }

            /* Calculate buffer position in user buffer */
            /* Use "vp" field of union to twiddle away const.  OK because if we're doing this it means the
             * user explicitly allowed us to modify this buffer via H5Pset_modify_write_buf(). */
            tmp_buf = (uint8_t *)dset_info->buf.vp + dset_info->layout_io_info.contig_piece_info->buf_off +
                      (smine_start * dset_info->type_info.src_type_size);
        }
        else {
            /* Do type conversion using intermediate buffer */
            tmp_buf = io_info->tconv_buf;

            /* Go figure out how many elements to read from the file */
            smine_nelmts =
                (size_t)MIN(dset_info->type_info.request_nelmts, (dset_info->nelmts - smine_start));

            /*
             * Gather data from application buffer into the datatype conversion
             * buffer. Also gather data from the file into the background buffer
             * if necessary.
             */
            n = H5D__gather_mem(buf, mem_iter, smine_nelmts, tmp_buf /*out*/);
            if (n != smine_nelmts)
                HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "mem gather failed");
        }

        /* If the source and destination are compound types and the destination is
         * is a subset of the source and no conversion is needed, copy the data
         * directly from user's buffer and bypass the rest of steps.  If the source
         * is a subset of the destination, the optimization is done in conversion
         * function H5T_conv_struct_opt to protect the background data.
         */
        if (H5D__SCATGATH_USE_CMPD_OPT_WRITE(dset_info, in_place_tconv)) {
            if (H5D__compound_opt_write(smine_nelmts, &dset_info->type_info, tmp_buf) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "datatype conversion failed");

        } /* end if */
        else {
            if (H5T_BKG_YES == dset_info->type_info.need_bkg) {
                n = H5D__gather_file(io_info, dset_info, bkg_iter, smine_nelmts, io_info->bkg_buf /*out*/);
                if (n != smine_nelmts)
                    HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "file gather failed");
            } /* end if */

            /* Do the data transform before the type conversion (since
             * transforms must be done in the memory type). */
            if (!dset_info->type_info.is_xform_noop) {
                H5Z_data_xform_t *data_transform; /* Data transform info */

                /* Retrieve info from API context */
                if (H5CX_get_data_transform(&data_transform) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get data transform info");

                if (H5Z_xform_eval(data_transform, tmp_buf, smine_nelmts, dset_info->type_info.mem_type) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "Error performing data transform");
            }

            /*
             * Perform datatype conversion.
             */
            if (H5T_convert(dset_info->type_info.tpath, dset_info->type_info.src_type_id,
                            dset_info->type_info.dst_type_id, smine_nelmts, (size_t)0, (size_t)0, tmp_buf,
                            io_info->bkg_buf) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTCONVERT, FAIL, "datatype conversion failed");
        } /* end else */

        /*
         * Scatter the data out to the file.
         */
        if (H5D__scatter_file(io_info, dset_info, file_iter, smine_nelmts, tmp_buf) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "scatter failed");
    } /* end for */

done:
    /* Release selection iterators */
    if (file_iter_init && H5S_SELECT_ITER_RELEASE(file_iter) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "Can't release selection iterator");
    if (file_iter)
        file_iter = H5FL_FREE(H5S_sel_iter_t, file_iter);
    if (mem_iter_init && H5S_SELECT_ITER_RELEASE(mem_iter) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "Can't release selection iterator");
    if (mem_iter)
        mem_iter = H5FL_FREE(H5S_sel_iter_t, mem_iter);
    if (bkg_iter_init && H5S_SELECT_ITER_RELEASE(bkg_iter) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "Can't release selection iterator");
    if (bkg_iter)
        bkg_iter = H5FL_FREE(H5S_sel_iter_t, bkg_iter);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__scatgath_write() */

/*-------------------------------------------------------------------------
 * Function:	H5D__scatgath_read_select
 *
 * Purpose:	Perform scatter/gather read from a list of dataset pieces
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__scatgath_read_select(H5D_io_info_t *io_info)
{
    H5S_t         **tmp_mem_spaces   = NULL;  /* Memory spaces to use for read from disk */
    H5S_sel_iter_t *mem_iter         = NULL;  /* Memory selection iteration info */
    bool            mem_iter_init    = false; /* Memory selection iteration info has been initialized */
    void          **tmp_bufs         = NULL;  /* Buffers to use for read from disk */
    void           *tmp_bkg_buf      = NULL;  /* Temporary background buffer pointer */
    size_t          tconv_bytes_used = 0;     /* Number of bytes used so far in conversion buffer */
    size_t          bkg_bytes_used   = 0;     /* Number of bytes used so far in background buffer */
    size_t          i;                        /* Local index variable */
    herr_t          ret_value = SUCCEED;      /* Return value		*/

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(io_info);
    assert(io_info->count > 0);
    assert(io_info->mem_spaces || io_info->pieces_added == 0);
    assert(io_info->file_spaces || io_info->pieces_added == 0);
    assert(io_info->addrs || io_info->pieces_added == 0);
    assert(io_info->element_sizes || io_info->pieces_added == 0);
    assert(io_info->rbufs || io_info->pieces_added == 0);

    /* Allocate list of buffers (within the tconv buf) */
    if (NULL == (tmp_bufs = H5MM_malloc(io_info->pieces_added * sizeof(void *))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "memory allocation failed for temporary buffer list");

    /* Allocate the iterator */
    if (NULL == (mem_iter = H5FL_MALLOC(H5S_sel_iter_t)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate memory iterator");

    /* Allocate list of block memory spaces */
    /*!FIXME delay doing this until we find the first mem space that is non-contiguous or doesn't start at 0
     */
    if (NULL == (tmp_mem_spaces = H5MM_malloc(io_info->pieces_added * sizeof(H5S_t *))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                    "memory allocation failed for temporary memory space list");

    /* Build read operation to tconv buffer */
    for (i = 0; i < io_info->pieces_added; i++) {
        H5D_dset_io_info_t *dset_info = io_info->sel_pieces[i]->dset_info;

        assert(io_info->sel_pieces[i]->piece_points > 0);

        /* Check if this piece is involved in type conversion */
        if (dset_info->type_info.is_xform_noop && dset_info->type_info.is_conv_noop) {
            /* No type conversion, just copy the mem space and buffer */
            tmp_mem_spaces[i] = io_info->mem_spaces[i];
            tmp_bufs[i]       = io_info->rbufs[i];
        }
        else {
            /* Create block memory space */
            if (NULL ==
                (tmp_mem_spaces[i] = H5S_create_simple(1, &io_info->sel_pieces[i]->piece_points, NULL))) {
                memset(&tmp_mem_spaces[i], 0, (io_info->pieces_added - i) * sizeof(tmp_mem_spaces[0]));
                HGOTO_ERROR(H5E_DATASET, H5E_CANTCREATE, FAIL, "unable to create simple memory dataspace");
            }

            /* Check for in-place type conversion */
            if (io_info->sel_pieces[i]->in_place_tconv)
                /* Set buffer to point to read buffer + offset */
                tmp_bufs[i] = (uint8_t *)(io_info->rbufs[i]) + io_info->sel_pieces[i]->buf_off;
            else {
                /* Set buffer to point into type conversion buffer */
                tmp_bufs[i] = io_info->tconv_buf + tconv_bytes_used;
                tconv_bytes_used +=
                    io_info->sel_pieces[i]->piece_points *
                    MAX(dset_info->type_info.src_type_size, dset_info->type_info.dst_type_size);
                assert(tconv_bytes_used <= io_info->tconv_buf_size);
            }

            /* Fill background buffer here unless we will use H5D__compound_opt_read().  Must do this before
             * the read so the read buffer doesn't get wiped out if we're using in-place type conversion */
            if (!H5D__SCATGATH_USE_CMPD_OPT_READ(dset_info, io_info->sel_pieces[i]->in_place_tconv)) {
                /* Check for background buffer */
                if (dset_info->type_info.need_bkg) {
                    assert(io_info->bkg_buf);

                    /* Calculate background buffer position */
                    tmp_bkg_buf = io_info->bkg_buf + bkg_bytes_used;
                    bkg_bytes_used +=
                        io_info->sel_pieces[i]->piece_points * dset_info->type_info.dst_type_size;
                    assert(bkg_bytes_used <= io_info->bkg_buf_size);

                    /* Gather data from read buffer to background buffer if necessary */
                    if (H5T_BKG_YES == dset_info->type_info.need_bkg) {
                        /* Initialize memory iterator */
                        assert(!mem_iter_init);
                        if (H5S_select_iter_init(mem_iter, io_info->mem_spaces[i],
                                                 dset_info->type_info.dst_type_size, 0) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                                        "unable to initialize memory selection information");
                        mem_iter_init = true; /* Memory selection iteration info has been initialized */

                        if ((size_t)io_info->sel_pieces[i]->piece_points !=
                            H5D__gather_mem(io_info->rbufs[i], mem_iter,
                                            (size_t)io_info->sel_pieces[i]->piece_points,
                                            tmp_bkg_buf /*out*/))
                            HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "mem gather failed");

                        /* Reset selection iterator */
                        assert(mem_iter_init);
                        if (H5S_SELECT_ITER_RELEASE(mem_iter) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "Can't release selection iterator");
                        mem_iter_init = false;
                    }
                }
            }
        }
    }

    /* Read data from all pieces */
    H5_CHECK_OVERFLOW(io_info->pieces_added, size_t, uint32_t);
    if (H5F_shared_select_read(io_info->f_sh, H5FD_MEM_DRAW, (uint32_t)io_info->pieces_added, tmp_mem_spaces,
                               io_info->file_spaces, io_info->addrs, io_info->element_sizes, tmp_bufs) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "selection read failed");

    /* Reset bkg_bytes_used */
    bkg_bytes_used = 0;

    /* Perform type conversion and scatter data to memory buffers for datasets that need this */
    for (i = 0; i < io_info->pieces_added; i++) {
        H5D_dset_io_info_t *dset_info = io_info->sel_pieces[i]->dset_info;

        assert(tmp_mem_spaces[i]);

        /* Check if this piece is involved in type conversion */
        if (tmp_mem_spaces[i] != io_info->mem_spaces[i]) {
            H5_CHECK_OVERFLOW(io_info->sel_pieces[i]->piece_points, hsize_t, size_t);

            /* Initialize memory iterator */
            assert(!mem_iter_init);
            if (H5S_select_iter_init(mem_iter, io_info->mem_spaces[i], dset_info->type_info.dst_type_size,
                                     0) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                            "unable to initialize memory selection information");
            mem_iter_init = true; /* Memory selection iteration info has been initialized */

            /* If the source and destination are compound types and subset of each other
             * and no conversion is needed, copy the data directly into user's buffer and
             * bypass the rest of steps.
             */
            if (H5D__SCATGATH_USE_CMPD_OPT_READ(dset_info, io_info->sel_pieces[i]->in_place_tconv)) {
                if (H5D__compound_opt_read((size_t)io_info->sel_pieces[i]->piece_points, mem_iter,
                                           &dset_info->type_info, tmp_bufs[i], io_info->rbufs[i] /*out*/) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "datatype conversion failed");
            }
            else {
                /* Check for background buffer */
                if (dset_info->type_info.need_bkg) {
                    assert(io_info->bkg_buf);

                    /* Calculate background buffer position */
                    tmp_bkg_buf = io_info->bkg_buf + bkg_bytes_used;
                    bkg_bytes_used +=
                        io_info->sel_pieces[i]->piece_points * dset_info->type_info.dst_type_size;
                    assert(bkg_bytes_used <= io_info->bkg_buf_size);
                }

                /*
                 * Perform datatype conversion.
                 */
                if (H5T_convert(dset_info->type_info.tpath, dset_info->type_info.src_type_id,
                                dset_info->type_info.dst_type_id,
                                (size_t)io_info->sel_pieces[i]->piece_points, (size_t)0, (size_t)0,
                                tmp_bufs[i], tmp_bkg_buf) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTCONVERT, FAIL, "datatype conversion failed");

                /* Do the data transform after the conversion (since we're using type mem_type) */
                if (!dset_info->type_info.is_xform_noop) {
                    H5Z_data_xform_t *data_transform; /* Data transform info */

                    /* Retrieve info from API context */
                    if (H5CX_get_data_transform(&data_transform) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get data transform info");

                    if (H5Z_xform_eval(data_transform, tmp_bufs[i],
                                       (size_t)io_info->sel_pieces[i]->piece_points,
                                       dset_info->type_info.mem_type) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "Error performing data transform");
                }

                /* Scatter the data into memory if this was not an in-place conversion */
                if (!io_info->sel_pieces[i]->in_place_tconv)
                    if (H5D__scatter_mem(tmp_bufs[i], mem_iter, (size_t)io_info->sel_pieces[i]->piece_points,
                                         io_info->rbufs[i] /*out*/) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "scatter failed");
            }

            /* Release selection iterator */
            assert(mem_iter_init);
            if (H5S_SELECT_ITER_RELEASE(mem_iter) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "Can't release selection iterator");
            mem_iter_init = false;
        }
    }

done:
    /* Release and free selection iterator */
    if (mem_iter_init && H5S_SELECT_ITER_RELEASE(mem_iter) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "Can't release selection iterator");
    if (mem_iter)
        mem_iter = H5FL_FREE(H5S_sel_iter_t, mem_iter);

    /* Free tmp_bufs */
    H5MM_free(tmp_bufs);
    tmp_bufs = NULL;

    /* Clear and free tmp_mem_spaces */
    if (tmp_mem_spaces) {
        for (i = 0; i < io_info->pieces_added; i++)
            if (tmp_mem_spaces[i] != io_info->mem_spaces[i] && tmp_mem_spaces[i] &&
                H5S_close(tmp_mem_spaces[i]) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "Can't close dataspace");
        H5MM_free(tmp_mem_spaces);
        tmp_mem_spaces = NULL;
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__scatgath_read_select() */

/*-------------------------------------------------------------------------
 * Function:	H5D__scatgath_write_select
 *
 * Purpose:	Perform scatter/gather write to a list of dataset pieces.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__scatgath_write_select(H5D_io_info_t *io_info)
{
    H5S_t         **write_mem_spaces  = NULL;  /* Memory spaces to use for write to disk */
    size_t          spaces_added      = 0;     /* Number of spaces added to write_mem_spaces */
    H5S_sel_iter_t *mem_iter          = NULL;  /* Memory selection iteration info */
    bool            mem_iter_init     = false; /* Memory selection iteration info has been initialized */
    const void    **write_bufs        = NULL;  /* Buffers to use for write to disk */
    size_t          tconv_bytes_used  = 0;     /* Number of bytes used so far in conversion buffer */
    size_t          bkg_bytes_used    = 0;     /* Number of bytes used so far in background buffer */
    H5S_t         **bkg_mem_spaces    = NULL;  /* Array of memory spaces for read to background buffer */
    H5S_t         **bkg_file_spaces   = NULL;  /* Array of file spaces for read to background buffer */
    haddr_t        *bkg_addrs         = NULL;  /* Array of file addresses for read to background buffer */
    size_t         *bkg_element_sizes = NULL;  /* Array of element sizes for read to background buffer */
    void          **bkg_bufs   = NULL; /* Array background buffers for read of existing file contents */
    size_t          bkg_pieces = 0;    /* Number of pieces that need to read the background data from disk */
    size_t          i;                 /* Local index variable */
    herr_t          ret_value = SUCCEED; /* Return value		*/

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(io_info);
    assert(io_info->count > 0);
    assert(io_info->mem_spaces || io_info->pieces_added == 0);
    assert(io_info->file_spaces || io_info->pieces_added == 0);
    assert(io_info->addrs || io_info->pieces_added == 0);
    assert(io_info->element_sizes || io_info->pieces_added == 0);
    assert(io_info->wbufs || io_info->pieces_added == 0);

    /* Allocate list of buffers (within the tconv buf) */
    if (NULL == (write_bufs = (const void **)H5MM_malloc(io_info->pieces_added * sizeof(const void *))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "memory allocation failed for temporary buffer list");

    /* Allocate the iterator */
    if (NULL == (mem_iter = H5FL_MALLOC(H5S_sel_iter_t)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate memory iterator");

    /* Allocate list of block memory spaces */
    /*!FIXME delay doing this until we find the first mem space that is non-contiguous or doesn't start at 0
     */
    if (NULL == (write_mem_spaces = H5MM_malloc(io_info->pieces_added * sizeof(H5S_t *))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                    "memory allocation failed for temporary memory space list");

    /* Build operations to read data to background buffer and to write data */
    for (i = 0; i < io_info->pieces_added; i++) {
        H5D_dset_io_info_t *dset_info = io_info->sel_pieces[i]->dset_info;

        assert(io_info->sel_pieces[i]->piece_points > 0);

        /* Check if this piece is involved in type conversion */
        if (dset_info->type_info.is_xform_noop && dset_info->type_info.is_conv_noop) {
            /* No type conversion, just copy the mem space and buffer */
            write_mem_spaces[i] = io_info->mem_spaces[i];
            spaces_added++;
            write_bufs[i] = io_info->wbufs[i];
        }
        else {
            void *tmp_write_buf; /* To sidestep const warnings */
            void *tmp_bkg_buf = NULL;

            H5_CHECK_OVERFLOW(io_info->sel_pieces[i]->piece_points, hsize_t, size_t);

            /* Initialize memory iterator */
            assert(!mem_iter_init);
            if (H5S_select_iter_init(mem_iter, io_info->mem_spaces[i], dset_info->type_info.src_type_size,
                                     0) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL,
                            "unable to initialize memory selection information");
            mem_iter_init = true; /* Memory selection iteration info has been initialized */

            /* Create block memory space */
            if (NULL ==
                (write_mem_spaces[i] = H5S_create_simple(1, &io_info->sel_pieces[i]->piece_points, NULL)))
                HGOTO_ERROR(H5E_DATASET, H5E_CANTCREATE, FAIL, "unable to create simple memory dataspace");
            spaces_added++;

            /* Check for in-place type conversion */
            if (io_info->sel_pieces[i]->in_place_tconv) {
                H5_flexible_const_ptr_t flex_buf;

                /* Set buffer to point to write buffer + offset */
                /* Use cast to union to twiddle away const.  OK because if we're doing this it means the user
                 * explicitly allowed us to modify this buffer via H5Pset_modify_write_buf(). */
                flex_buf.cvp  = io_info->wbufs[i];
                tmp_write_buf = (uint8_t *)flex_buf.vp + io_info->sel_pieces[i]->buf_off;
            }
            else {
                /* Set buffer to point into type conversion buffer */
                tmp_write_buf = io_info->tconv_buf + tconv_bytes_used;
                tconv_bytes_used +=
                    io_info->sel_pieces[i]->piece_points *
                    MAX(dset_info->type_info.src_type_size, dset_info->type_info.dst_type_size);
                assert(tconv_bytes_used <= io_info->tconv_buf_size);

                /* Gather data from application buffer into the datatype conversion buffer */
                if ((size_t)io_info->sel_pieces[i]->piece_points !=
                    H5D__gather_mem(io_info->wbufs[i], mem_iter, (size_t)io_info->sel_pieces[i]->piece_points,
                                    tmp_write_buf /*out*/))
                    HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "mem gather failed");
            }

            /* Set buffer for writing to disk (from type conversion buffer) */
            write_bufs[i] = (const void *)tmp_write_buf;

            /* If the source and destination are compound types and the destination is a subset of
             * the source and no conversion is needed, copy the data directly into the type
             * conversion buffer and bypass the rest of steps.  If the source is a subset of the
             * destination, the optimization is done in conversion function H5T_conv_struct_opt to
             * protect the background data.
             */
            if (H5D__SCATGATH_USE_CMPD_OPT_WRITE(dset_info, io_info->sel_pieces[i]->in_place_tconv)) {
                if (H5D__compound_opt_write((size_t)io_info->sel_pieces[i]->piece_points,
                                            &dset_info->type_info, tmp_write_buf) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "datatype conversion failed");

            } /* end if */
            else {
                /* Check for background buffer */
                if (dset_info->type_info.need_bkg) {
                    assert(io_info->bkg_buf);

                    /* Calculate background buffer position */
                    tmp_bkg_buf = io_info->bkg_buf + bkg_bytes_used;
                    bkg_bytes_used +=
                        io_info->sel_pieces[i]->piece_points * dset_info->type_info.dst_type_size;
                    assert(bkg_bytes_used <= io_info->bkg_buf_size);
                }

                /* Set up background buffer read operation if necessary */
                if (H5T_BKG_YES == dset_info->type_info.need_bkg) {
                    assert(io_info->must_fill_bkg);

                    /* Allocate arrays of parameters for selection read to background buffer if necessary */
                    if (!bkg_mem_spaces) {
                        assert(!bkg_file_spaces && !bkg_addrs && !bkg_element_sizes && !bkg_bufs);
                        if (NULL == (bkg_mem_spaces = H5MM_malloc(io_info->pieces_added * sizeof(H5S_t *))))
                            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                                        "memory allocation failed for memory space list");
                        if (NULL == (bkg_file_spaces = H5MM_malloc(io_info->pieces_added * sizeof(H5S_t *))))
                            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                                        "memory allocation failed for file space list");
                        if (NULL == (bkg_addrs = H5MM_malloc(io_info->pieces_added * sizeof(haddr_t))))
                            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                                        "memory allocation failed for piece address list");
                        if (NULL == (bkg_element_sizes = H5MM_malloc(io_info->pieces_added * sizeof(size_t))))
                            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                                        "memory allocation failed for element size list");
                        if (NULL == (bkg_bufs = H5MM_malloc(io_info->pieces_added * sizeof(const void *))))
                            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                                        "memory allocation failed for write buffer list");
                    }

                    /* Use same (block) memory space, file space, address, and element size as write operation
                     */
                    assert(bkg_mem_spaces && bkg_file_spaces && bkg_addrs && bkg_element_sizes && bkg_bufs);
                    bkg_mem_spaces[bkg_pieces]    = write_mem_spaces[i];
                    bkg_file_spaces[bkg_pieces]   = io_info->file_spaces[i];
                    bkg_addrs[bkg_pieces]         = io_info->addrs[i];
                    bkg_element_sizes[bkg_pieces] = io_info->element_sizes[i];

                    /* Use previously calculated background buffer position */
                    bkg_bufs[bkg_pieces] = tmp_bkg_buf;

                    /* Add piece */
                    bkg_pieces++;
                }
                else {
                    /* Perform type conversion here to avoid second loop if no dsets use the background buffer
                     */
                    /* Do the data transform before the type conversion (since
                     * transforms must be done in the memory type). */
                    if (!dset_info->type_info.is_xform_noop) {
                        H5Z_data_xform_t *data_transform; /* Data transform info */

                        /* Retrieve info from API context */
                        if (H5CX_get_data_transform(&data_transform) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get data transform info");

                        if (H5Z_xform_eval(data_transform, tmp_write_buf,
                                           (size_t)io_info->sel_pieces[i]->piece_points,
                                           dset_info->type_info.mem_type) < 0)
                            HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "Error performing data transform");
                    }

                    /*
                     * Perform datatype conversion.
                     */
                    if (H5T_convert(dset_info->type_info.tpath, dset_info->type_info.src_type_id,
                                    dset_info->type_info.dst_type_id,
                                    (size_t)io_info->sel_pieces[i]->piece_points, (size_t)0, (size_t)0,
                                    tmp_write_buf, tmp_bkg_buf) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTCONVERT, FAIL, "datatype conversion failed");
                }
            }

            /* Release selection iterator */
            assert(mem_iter_init);
            if (H5S_SELECT_ITER_RELEASE(mem_iter) < 0)
                HGOTO_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "Can't release selection iterator");
            mem_iter_init = false;
        }
    }

    assert(spaces_added == io_info->pieces_added);

    /* Gather data to background buffer if necessary */
    if (io_info->must_fill_bkg) {
        size_t j = 0; /* Index into array of background buffers */

        /* Read data */
        H5_CHECK_OVERFLOW(bkg_pieces, size_t, uint32_t);
        if (H5F_shared_select_read(io_info->f_sh, H5FD_MEM_DRAW, (uint32_t)bkg_pieces, bkg_mem_spaces,
                                   bkg_file_spaces, bkg_addrs, bkg_element_sizes, bkg_bufs) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_READERROR, FAIL, "selection read to background buffer failed");

        /* Perform type conversion on pieces with background buffers that were just read */
        for (i = 0; i < io_info->pieces_added; i++) {
            H5D_dset_io_info_t *dset_info = io_info->sel_pieces[i]->dset_info;

            if ((H5T_BKG_YES == dset_info->type_info.need_bkg) &&
                !H5D__SCATGATH_USE_CMPD_OPT_WRITE(dset_info, io_info->sel_pieces[i]->in_place_tconv)) {
                /* Non-const write_buf[i].  Use pointer math here to avoid const warnings.  When
                 * there's a background buffer write_buf[i] always points inside the non-const tconv
                 * buf so this is OK. */
                void *tmp_write_buf =
                    (void *)((uint8_t *)io_info->tconv_buf +
                             ((const uint8_t *)write_bufs[i] - (const uint8_t *)io_info->tconv_buf));

                /* Do the data transform before the type conversion (since
                 * transforms must be done in the memory type). */
                if (!dset_info->type_info.is_xform_noop) {
                    H5Z_data_xform_t *data_transform; /* Data transform info */

                    /* Retrieve info from API context */
                    if (H5CX_get_data_transform(&data_transform) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't get data transform info");

                    if (H5Z_xform_eval(data_transform, tmp_write_buf,
                                       (size_t)io_info->sel_pieces[i]->piece_points,
                                       dset_info->type_info.mem_type) < 0)
                        HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "Error performing data transform");
                }

                /*
                 * Perform datatype conversion.
                 */
                assert(j < bkg_pieces);
                if (H5T_convert(dset_info->type_info.tpath, dset_info->type_info.src_type_id,
                                dset_info->type_info.dst_type_id,
                                (size_t)io_info->sel_pieces[i]->piece_points, (size_t)0, (size_t)0,
                                tmp_write_buf, bkg_bufs[j]) < 0)
                    HGOTO_ERROR(H5E_DATASET, H5E_CANTCONVERT, FAIL, "datatype conversion failed");

                /* Advance to next background buffer */
                j++;
            }
        }

        assert(j == bkg_pieces);
    }

    /* Write data to disk */
    H5_CHECK_OVERFLOW(io_info->pieces_added, size_t, uint32_t);
    if (H5F_shared_select_write(io_info->f_sh, H5FD_MEM_DRAW, (uint32_t)io_info->pieces_added,
                                write_mem_spaces, io_info->file_spaces, io_info->addrs,
                                io_info->element_sizes, write_bufs) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_WRITEERROR, FAIL, "selection write failed");

done:
    /* Release and free selection iterator */
    if (mem_iter_init && H5S_SELECT_ITER_RELEASE(mem_iter) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "Can't release selection iterator");
    if (mem_iter)
        mem_iter = H5FL_FREE(H5S_sel_iter_t, mem_iter);

    /* Free write_bufs */
    H5MM_free(write_bufs);
    write_bufs = NULL;

    /* Clear and free write_mem_spaces */
    if (write_mem_spaces) {
        for (i = 0; i < spaces_added; i++) {
            assert(write_mem_spaces[i]);
            if (write_mem_spaces[i] != io_info->mem_spaces[i] && H5S_close(write_mem_spaces[i]) < 0)
                HDONE_ERROR(H5E_DATASET, H5E_CLOSEERROR, FAIL, "Can't close dataspace");
        }
        H5MM_free(write_mem_spaces);
        write_mem_spaces = NULL;
    }

    /* Free background buffer parameter arrays */
    H5MM_free(bkg_mem_spaces);
    bkg_mem_spaces = NULL;
    H5MM_free(bkg_file_spaces);
    bkg_file_spaces = NULL;
    H5MM_free(bkg_addrs);
    bkg_addrs = NULL;
    H5MM_free(bkg_element_sizes);
    bkg_element_sizes = NULL;
    H5MM_free(bkg_bufs);
    bkg_bufs = NULL;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__scatgath_write_select() */

/*-------------------------------------------------------------------------
 * Function:	H5D__compound_opt_read
 *
 * Purpose:	A special optimization case when the source and
 *              destination members are a subset of each other, and
 *              the order is the same, and no conversion is needed.
 *              For example:
 *                  struct source {            struct destination {
 *                      TYPE1 A;      -->          TYPE1 A;
 *                      TYPE2 B;      -->          TYPE2 B;
 *                      TYPE3 C;      -->          TYPE3 C;
 *                  };                             TYPE4 D;
 *                                                 TYPE5 E;
 *                                             };
 *              or
 *                  struct destination {       struct source {
 *                      TYPE1 A;      <--          TYPE1 A;
 *                      TYPE2 B;      <--          TYPE2 B;
 *                      TYPE3 C;      <--          TYPE3 C;
 *                  };                             TYPE4 D;
 *                                                 TYPE5 E;
 *                                             };
 *              The optimization is simply moving data to the appropriate
 *              places in the buffer.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__compound_opt_read(size_t nelmts, H5S_sel_iter_t *iter, const H5D_type_info_t *type_info,
                       uint8_t *tconv_buf, void *user_buf /*out*/)
{
    uint8_t *ubuf = (uint8_t *)user_buf; /* Cast for pointer arithmetic	*/
    uint8_t *xdbuf;                      /* Pointer into dataset buffer */
    hsize_t *off = NULL;                 /* Pointer to sequence offsets */
    size_t  *len = NULL;                 /* Pointer to sequence lengths */
    size_t   src_stride, dst_stride, copy_size;
    size_t   dxpl_vec_size;       /* Vector length from API context's DXPL */
    size_t   vec_size;            /* Vector length */
    herr_t   ret_value = SUCCEED; /* Return value		*/

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(nelmts > 0);
    assert(iter);
    assert(type_info);
    assert(type_info->cmpd_subset);
    assert(H5T_SUBSET_SRC == type_info->cmpd_subset->subset ||
           H5T_SUBSET_DST == type_info->cmpd_subset->subset);
    assert(user_buf);

    /* Get info from API context */
    if (H5CX_get_vec_size(&dxpl_vec_size) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't retrieve I/O vector size");

    /* Allocate the vector I/O arrays */
    if (dxpl_vec_size > H5D_IO_VECTOR_SIZE)
        vec_size = dxpl_vec_size;
    else
        vec_size = H5D_IO_VECTOR_SIZE;
    if (NULL == (len = H5FL_SEQ_MALLOC(size_t, vec_size)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate I/O length vector array");
    if (NULL == (off = H5FL_SEQ_MALLOC(hsize_t, vec_size)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate I/O offset vector array");

    /* Get source & destination strides */
    src_stride = type_info->src_type_size;
    dst_stride = type_info->dst_type_size;

    /* Get the size, in bytes, to copy for each element */
    copy_size = type_info->cmpd_subset->copy_size;

    /* Loop until all elements are written */
    xdbuf = tconv_buf;
    while (nelmts > 0) {
        size_t nseq;     /* Number of sequences generated */
        size_t curr_seq; /* Current sequence being processed */
        size_t elmtno;   /* Element counter */

        /* Get list of sequences for selection to write */
        if (H5S_SELECT_ITER_GET_SEQ_LIST(iter, vec_size, nelmts, &nseq, &elmtno, off, len) < 0)
            HGOTO_ERROR(H5E_INTERNAL, H5E_UNSUPPORTED, 0, "sequence length generation failed");

        /* Loop, while sequences left to process */
        for (curr_seq = 0; curr_seq < nseq; curr_seq++) {
            size_t   curr_off;    /* Offset of bytes left to process in sequence */
            size_t   curr_len;    /* Length of bytes left to process in sequence */
            size_t   curr_nelmts; /* Number of elements to process in sequence   */
            uint8_t *xubuf;
            size_t   i; /* Local index variable */

            /* Get the number of bytes and offset in sequence */
            curr_len = len[curr_seq];
            H5_CHECK_OVERFLOW(off[curr_seq], hsize_t, size_t);
            curr_off = (size_t)off[curr_seq];

            /* Decide the number of elements and position in the buffer. */
            curr_nelmts = curr_len / dst_stride;
            xubuf       = ubuf + curr_off;

            /* Copy the data into the right place. */
            for (i = 0; i < curr_nelmts; i++) {
                memmove(xubuf, xdbuf, copy_size);

                /* Update pointers */
                xdbuf += src_stride;
                xubuf += dst_stride;
            } /* end for */
        }     /* end for */

        /* Decrement number of elements left to process */
        nelmts -= elmtno;
    } /* end while */

done:
    /* Release resources, if allocated */
    if (len)
        len = H5FL_SEQ_FREE(size_t, len);
    if (off)
        off = H5FL_SEQ_FREE(hsize_t, off);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__compound_opt_read() */

/*-------------------------------------------------------------------------
 * Function:	H5D__compound_opt_write
 *
 * Purpose:	A special optimization case when the source and
 *              destination members are a subset of each other, and
 *              the order is the same, and no conversion is needed.
 *              For example:
 *                  struct source {            struct destination {
 *                      TYPE1 A;      -->          TYPE1 A;
 *                      TYPE2 B;      -->          TYPE2 B;
 *                      TYPE3 C;      -->          TYPE3 C;
 *                  };                             TYPE4 D;
 *                                                 TYPE5 E;
 *                                             };
 *              or
 *                  struct destination {       struct source {
 *                      TYPE1 A;      <--          TYPE1 A;
 *                      TYPE2 B;      <--          TYPE2 B;
 *                      TYPE3 C;      <--          TYPE3 C;
 *                  };                             TYPE4 D;
 *                                                 TYPE5 E;
 *                                             };
 *              The optimization is simply moving data to the appropriate
 *              places in the buffer.
 *
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__compound_opt_write(size_t nelmts, const H5D_type_info_t *type_info, void *tconv_buf)
{
    uint8_t *xsbuf, *xdbuf;          /* Source & destination pointers into dataset buffer */
    size_t   src_stride, dst_stride; /* Strides through source & destination datatypes */
    size_t   i;                      /* Local index variable */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(nelmts > 0);
    assert(type_info);

    /* Initialize values for loop */
    src_stride = type_info->src_type_size;
    dst_stride = type_info->dst_type_size;

    /* Loop until all elements are written */
    xsbuf = tconv_buf;
    xdbuf = tconv_buf;
    for (i = 0; i < nelmts; i++) {
        memmove(xdbuf, xsbuf, dst_stride);

        /* Update pointers */
        xsbuf += src_stride;
        xdbuf += dst_stride;
    } /* end for */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__compound_opt_write() */
