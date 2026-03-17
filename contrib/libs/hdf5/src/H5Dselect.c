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
 * Purpose:	Dataspace I/O functions.
 */

/****************/
/* Module Setup */
/****************/

#include "H5Dmodule.h" /* This source code file is part of the H5D module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5CXprivate.h" /* API Contexts                             */
#include "H5Dpkg.h"      /* Datasets                                 */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5FLprivate.h" /* Free Lists                               */
#include "H5VMprivate.h" /* Vector Functions                         */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Local Prototypes */
/********************/

static herr_t H5D__select_io(const H5D_io_info_t *io_info, const H5D_dset_io_info_t *dset_info,
                             size_t elmt_size);

/*********************/
/* Package Variables */
/*********************/

/*******************/
/* Local Variables */
/*******************/

/* Declare a free list to manage sequences of size_t */
H5FL_SEQ_DEFINE(size_t);

/* Declare a free list to manage sequences of hsize_t */
H5FL_SEQ_DEFINE(hsize_t);

/* Declare extern free list to manage the H5S_sel_iter_t struct */
H5FL_EXTERN(H5S_sel_iter_t);

/*-------------------------------------------------------------------------
 * Function:	H5D__select_io
 *
 * Purpose:	Perform I/O directly from application memory and a file
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__select_io(const H5D_io_info_t *io_info, const H5D_dset_io_info_t *dset_info, size_t elmt_size)
{
    H5S_sel_iter_t *mem_iter       = NULL;  /* Memory selection iteration info */
    bool            mem_iter_init  = false; /* Memory selection iteration info has been initialized */
    H5S_sel_iter_t *file_iter      = NULL;  /* File selection iteration info */
    bool            file_iter_init = false; /* File selection iteration info has been initialized */
    hsize_t        *mem_off        = NULL;  /* Pointer to sequence offsets in memory */
    hsize_t        *file_off       = NULL;  /* Pointer to sequence offsets in the file */
    size_t         *mem_len        = NULL;  /* Pointer to sequence lengths in memory */
    size_t         *file_len       = NULL;  /* Pointer to sequence lengths in the file */
    size_t          curr_mem_seq;           /* Current memory sequence to operate on */
    size_t          curr_file_seq;          /* Current file sequence to operate on */
    size_t          mem_nseq;               /* Number of sequences generated in the file */
    size_t          file_nseq;              /* Number of sequences generated in memory */
    size_t          dxpl_vec_size;          /* Vector length from API context's DXPL */
    size_t          vec_size;               /* Vector length */
    ssize_t         tmp_file_len;           /* Temporary number of bytes in file sequence */
    size_t          nelmts;                 /* Number of elements to process */
    herr_t          ret_value = SUCCEED;    /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(io_info);
    assert(dset_info->dset);
    assert(dset_info->store);
    assert(dset_info->buf.vp);

    if (elmt_size == 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "invalid elmt_size of 0");

    /* Initialize nelmts */
    nelmts = dset_info->nelmts;

    /* Check for only one element in selection */
    if (nelmts == 1) {
        hsize_t single_mem_off;  /* Offset in memory */
        hsize_t single_file_off; /* Offset in the file */
        size_t  single_mem_len;  /* Length in memory */
        size_t  single_file_len; /* Length in the file */

        /* Get offset of first element in selections */
        if (H5S_SELECT_OFFSET(dset_info->file_space, &single_file_off) < 0)
            HGOTO_ERROR(H5E_INTERNAL, H5E_UNSUPPORTED, FAIL, "can't retrieve file selection offset");
        if (H5S_SELECT_OFFSET(dset_info->mem_space, &single_mem_off) < 0)
            HGOTO_ERROR(H5E_INTERNAL, H5E_UNSUPPORTED, FAIL, "can't retrieve memory selection offset");

        /* Set up necessary information for I/O operation */
        file_nseq = mem_nseq = 1;
        curr_mem_seq = curr_file_seq = 0;
        single_file_off *= elmt_size;
        single_mem_off *= elmt_size;
        single_file_len = single_mem_len = elmt_size;

        /* Perform I/O on memory and file sequences */
        if (io_info->op_type == H5D_IO_OP_READ) {
            if ((tmp_file_len = (*dset_info->layout_ops.readvv)(
                     io_info, dset_info, file_nseq, &curr_file_seq, &single_file_len, &single_file_off,
                     mem_nseq, &curr_mem_seq, &single_mem_len, &single_mem_off)) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_READERROR, FAIL, "read error");
        } /* end if */
        else {
            assert(io_info->op_type == H5D_IO_OP_WRITE);
            if ((tmp_file_len = (*dset_info->layout_ops.writevv)(
                     io_info, dset_info, file_nseq, &curr_file_seq, &single_file_len, &single_file_off,
                     mem_nseq, &curr_mem_seq, &single_mem_len, &single_mem_off)) < 0)
                HGOTO_ERROR(H5E_DATASPACE, H5E_WRITEERROR, FAIL, "write error");
        } /* end else */

        /* Decrement number of elements left to process */
        assert(((size_t)tmp_file_len % elmt_size) == 0);
    } /* end if */
    else {
        size_t mem_nelem;  /* Number of elements used in memory sequences */
        size_t file_nelem; /* Number of elements used in file sequences */

        /* Get info from API context */
        if (H5CX_get_vec_size(&dxpl_vec_size) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTGET, FAIL, "can't retrieve I/O vector size");

        /* Allocate the vector I/O arrays */
        if (dxpl_vec_size > H5D_IO_VECTOR_SIZE)
            vec_size = dxpl_vec_size;
        else
            vec_size = H5D_IO_VECTOR_SIZE;
        if (NULL == (mem_len = H5FL_SEQ_MALLOC(size_t, vec_size)))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate I/O length vector array");
        if (NULL == (mem_off = H5FL_SEQ_MALLOC(hsize_t, vec_size)))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate I/O offset vector array");
        if (NULL == (file_len = H5FL_SEQ_MALLOC(size_t, vec_size)))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate I/O length vector array");
        if (NULL == (file_off = H5FL_SEQ_MALLOC(hsize_t, vec_size)))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate I/O offset vector array");

        /* Allocate the iterators */
        if (NULL == (mem_iter = H5FL_MALLOC(H5S_sel_iter_t)))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate memory iterator");
        if (NULL == (file_iter = H5FL_MALLOC(H5S_sel_iter_t)))
            HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate file iterator");

        /* Initialize file iterator */
        if (H5S_select_iter_init(file_iter, dset_info->file_space, elmt_size,
                                 H5S_SEL_ITER_GET_SEQ_LIST_SORTED) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to initialize selection iterator");
        file_iter_init = 1; /* File selection iteration info has been initialized */

        /* Initialize memory iterator */
        if (H5S_select_iter_init(mem_iter, dset_info->mem_space, elmt_size, 0) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to initialize selection iterator");
        mem_iter_init = 1; /* Memory selection iteration info has been initialized */

        /* Initialize sequence counts */
        curr_mem_seq = curr_file_seq = 0;
        mem_nseq = file_nseq = 0;

        /* Loop, until all bytes are processed */
        while (nelmts > 0) {
            /* Check if more file sequences are needed */
            if (curr_file_seq >= file_nseq) {
                /* Get sequences for file selection */
                if (H5S_SELECT_ITER_GET_SEQ_LIST(file_iter, vec_size, nelmts, &file_nseq, &file_nelem,
                                                 file_off, file_len) < 0)
                    HGOTO_ERROR(H5E_INTERNAL, H5E_UNSUPPORTED, FAIL, "sequence length generation failed");

                /* Start at the beginning of the sequences again */
                curr_file_seq = 0;
            } /* end if */

            /* Check if more memory sequences are needed */
            if (curr_mem_seq >= mem_nseq) {
                /* Get sequences for memory selection */
                if (H5S_SELECT_ITER_GET_SEQ_LIST(mem_iter, vec_size, nelmts, &mem_nseq, &mem_nelem, mem_off,
                                                 mem_len) < 0)
                    HGOTO_ERROR(H5E_INTERNAL, H5E_UNSUPPORTED, FAIL, "sequence length generation failed");

                /* Start at the beginning of the sequences again */
                curr_mem_seq = 0;
            } /* end if */

            /* Perform I/O on memory and file sequences */
            if (io_info->op_type == H5D_IO_OP_READ) {
                if ((tmp_file_len = (*dset_info->layout_ops.readvv)(
                         io_info, dset_info, file_nseq, &curr_file_seq, file_len, file_off, mem_nseq,
                         &curr_mem_seq, mem_len, mem_off)) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_READERROR, FAIL, "read error");
            } /* end if */
            else {
                assert(io_info->op_type == H5D_IO_OP_WRITE);
                if ((tmp_file_len = (*dset_info->layout_ops.writevv)(
                         io_info, dset_info, file_nseq, &curr_file_seq, file_len, file_off, mem_nseq,
                         &curr_mem_seq, mem_len, mem_off)) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_WRITEERROR, FAIL, "write error");
            } /* end else */

            /* Decrement number of elements left to process */
            assert(((size_t)tmp_file_len % elmt_size) == 0);
            nelmts -= ((size_t)tmp_file_len / elmt_size);
        } /* end while */
    }     /* end else */

done:
    /* Release selection iterators */
    if (file_iter_init && H5S_SELECT_ITER_RELEASE(file_iter) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTRELEASE, FAIL, "unable to release selection iterator");
    if (file_iter)
        file_iter = H5FL_FREE(H5S_sel_iter_t, file_iter);
    if (mem_iter_init && H5S_SELECT_ITER_RELEASE(mem_iter) < 0)
        HDONE_ERROR(H5E_DATASET, H5E_CANTRELEASE, FAIL, "unable to release selection iterator");
    if (mem_iter)
        mem_iter = H5FL_FREE(H5S_sel_iter_t, mem_iter);

    /* Release vector arrays, if allocated */
    if (file_len)
        file_len = H5FL_SEQ_FREE(size_t, file_len);
    if (file_off)
        file_off = H5FL_SEQ_FREE(hsize_t, file_off);
    if (mem_len)
        mem_len = H5FL_SEQ_FREE(size_t, mem_len);
    if (mem_off)
        mem_off = H5FL_SEQ_FREE(hsize_t, mem_off);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__select_io() */

/*-------------------------------------------------------------------------
 * Function:    H5D_select_io_mem
 *
 * Purpose:     Perform memory copies directly between two memory buffers
 *              according to the selections in the `dst_space` and
 *              `src_space` dataspaces.
 *
 * Note:        This routine is [basically] the same as H5D__select_io,
 *              with the only difference being that the readvv/writevv
 *              calls are exchanged for H5VM_memcpyvv calls. Changes should
 *              be made to both routines.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D_select_io_mem(void *dst_buf, H5S_t *dst_space, const void *src_buf, H5S_t *src_space, size_t elmt_size,
                  size_t nelmts)
{
    H5S_sel_iter_t *dst_sel_iter      = NULL;  /* Destination dataspace iteration info */
    H5S_sel_iter_t *src_sel_iter      = NULL;  /* Source dataspace iteration info */
    bool            dst_sel_iter_init = false; /* Destination dataspace selection iterator initialized? */
    bool            src_sel_iter_init = false; /* Source dataspace selection iterator initialized? */
    hsize_t        *dst_off           = NULL;  /* Pointer to sequence offsets in destination buffer */
    hsize_t        *src_off           = NULL;  /* Pointer to sequence offsets in source buffer */
    size_t         *dst_len           = NULL;  /* Pointer to sequence lengths in destination buffer */
    size_t         *src_len           = NULL;  /* Pointer to sequence lengths in source buffer */
    size_t          curr_dst_seq;              /* Current destination buffer sequence to operate on */
    size_t          curr_src_seq;              /* Current source buffer sequence to operate on */
    size_t          dst_nseq;                  /* Number of sequences generated for destination buffer */
    size_t          src_nseq;                  /* Number of sequences generated for source buffer */
    size_t          dxpl_vec_size;             /* Vector length from API context's DXPL */
    size_t          vec_size;                  /* Vector length */
    ssize_t         bytes_copied;
    herr_t          ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    assert(dst_buf);
    assert(dst_space);
    assert(src_buf);
    assert(src_space);

    if (elmt_size == 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_BADVALUE, FAIL, "invalid elmt_size of 0");

    /* Check for only one element in selection */
    if (nelmts == 1) {
        hsize_t single_dst_off; /* Offset in dst_space */
        hsize_t single_src_off; /* Offset in src_space */
        size_t  single_dst_len; /* Length in dst_space */
        size_t  single_src_len; /* Length in src_space */

        /* Get offset of first element in selections */
        if (H5S_SELECT_OFFSET(dst_space, &single_dst_off) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't retrieve destination selection offset");
        if (H5S_SELECT_OFFSET(src_space, &single_src_off) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "can't retrieve source selection offset");

        /* Set up necessary information for I/O operation */
        dst_nseq = src_nseq = 1;
        curr_dst_seq = curr_src_seq = 0;
        single_dst_off *= elmt_size;
        single_src_off *= elmt_size;
        single_dst_len = single_src_len = elmt_size;

        /* Perform vectorized memcpy from src_buf to dst_buf */
        if ((bytes_copied =
                 H5VM_memcpyvv(dst_buf, dst_nseq, &curr_dst_seq, &single_dst_len, &single_dst_off, src_buf,
                               src_nseq, &curr_src_seq, &single_src_len, &single_src_off)) < 0)
            HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "vectorized memcpy failed");

        assert(((size_t)bytes_copied % elmt_size) == 0);
    }
    else {
        unsigned sel_iter_flags = H5S_SEL_ITER_GET_SEQ_LIST_SORTED | H5S_SEL_ITER_SHARE_WITH_DATASPACE;
        size_t   dst_nelem; /* Number of elements used in destination buffer sequences */
        size_t   src_nelem; /* Number of elements used in source buffer sequences */

        /* Get info from API context */
        if (H5CX_get_vec_size(&dxpl_vec_size) < 0)
            HGOTO_ERROR(H5E_IO, H5E_CANTGET, FAIL, "can't retrieve I/O vector size");

        /* Allocate the vector I/O arrays */
        if (dxpl_vec_size > H5D_IO_VECTOR_SIZE)
            vec_size = dxpl_vec_size;
        else
            vec_size = H5D_IO_VECTOR_SIZE;

        if (NULL == (dst_len = H5FL_SEQ_MALLOC(size_t, vec_size)))
            HGOTO_ERROR(H5E_IO, H5E_CANTALLOC, FAIL, "can't allocate I/O length vector array");
        if (NULL == (dst_off = H5FL_SEQ_MALLOC(hsize_t, vec_size)))
            HGOTO_ERROR(H5E_IO, H5E_CANTALLOC, FAIL, "can't allocate I/O offset vector array");
        if (NULL == (src_len = H5FL_SEQ_MALLOC(size_t, vec_size)))
            HGOTO_ERROR(H5E_IO, H5E_CANTALLOC, FAIL, "can't allocate I/O length vector array");
        if (NULL == (src_off = H5FL_SEQ_MALLOC(hsize_t, vec_size)))
            HGOTO_ERROR(H5E_IO, H5E_CANTALLOC, FAIL, "can't allocate I/O offset vector array");

        /* Allocate the dataspace selection iterators */
        if (NULL == (dst_sel_iter = H5FL_MALLOC(H5S_sel_iter_t)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate destination selection iterator");
        if (NULL == (src_sel_iter = H5FL_MALLOC(H5S_sel_iter_t)))
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTALLOC, FAIL, "can't allocate source selection iterator");

        /* Initialize destination selection iterator */
        if (H5S_select_iter_init(dst_sel_iter, dst_space, elmt_size, sel_iter_flags) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to initialize selection iterator");
        dst_sel_iter_init = true; /* Destination selection iteration info has been initialized */

        /* Initialize source selection iterator */
        if (H5S_select_iter_init(src_sel_iter, src_space, elmt_size, H5S_SEL_ITER_SHARE_WITH_DATASPACE) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_CANTINIT, FAIL, "unable to initialize selection iterator");
        src_sel_iter_init = true; /* Source selection iteration info has been initialized */

        /* Initialize sequence counts */
        curr_dst_seq = curr_src_seq = 0;
        dst_nseq = src_nseq = 0;

        /* Loop, until all bytes are processed */
        while (nelmts > 0) {
            /* Check if more destination buffer sequences are needed */
            if (curr_dst_seq >= dst_nseq) {
                /* Get sequences for destination selection */
                if (H5S_SELECT_ITER_GET_SEQ_LIST(dst_sel_iter, vec_size, nelmts, &dst_nseq, &dst_nelem,
                                                 dst_off, dst_len) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "sequence length generation failed");

                /* Start at the beginning of the sequences again */
                curr_dst_seq = 0;
            }

            /* Check if more source buffer sequences are needed */
            if (curr_src_seq >= src_nseq) {
                /* Get sequences for source selection */
                if (H5S_SELECT_ITER_GET_SEQ_LIST(src_sel_iter, vec_size, nelmts, &src_nseq, &src_nelem,
                                                 src_off, src_len) < 0)
                    HGOTO_ERROR(H5E_DATASPACE, H5E_CANTGET, FAIL, "sequence length generation failed");

                /* Start at the beginning of the sequences again */
                curr_src_seq = 0;
            } /* end if */

            /* Perform vectorized memcpy from src_buf to dst_buf */
            if ((bytes_copied = H5VM_memcpyvv(dst_buf, dst_nseq, &curr_dst_seq, dst_len, dst_off, src_buf,
                                              src_nseq, &curr_src_seq, src_len, src_off)) < 0)
                HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "vectorized memcpy failed");

            /* Decrement number of elements left to process */
            assert(((size_t)bytes_copied % elmt_size) == 0);
            nelmts -= ((size_t)bytes_copied / elmt_size);
        }
    }

done:
    /* Release selection iterators */
    if (src_sel_iter) {
        if (src_sel_iter_init && H5S_SELECT_ITER_RELEASE(src_sel_iter) < 0)
            HDONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release selection iterator");

        src_sel_iter = H5FL_FREE(H5S_sel_iter_t, src_sel_iter);
    }
    if (dst_sel_iter) {
        if (dst_sel_iter_init && H5S_SELECT_ITER_RELEASE(dst_sel_iter) < 0)
            HDONE_ERROR(H5E_DATASPACE, H5E_CANTRELEASE, FAIL, "unable to release selection iterator");

        dst_sel_iter = H5FL_FREE(H5S_sel_iter_t, dst_sel_iter);
    }

    /* Release vector arrays, if allocated */
    if (src_off)
        src_off = H5FL_SEQ_FREE(hsize_t, src_off);
    if (src_len)
        src_len = H5FL_SEQ_FREE(size_t, src_len);
    if (dst_off)
        dst_off = H5FL_SEQ_FREE(hsize_t, dst_off);
    if (dst_len)
        dst_len = H5FL_SEQ_FREE(size_t, dst_len);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D_select_io_mem() */

/*-------------------------------------------------------------------------
 * Function:	H5D__select_read
 *
 * Purpose:	Reads directly from file into application memory.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__select_read(const H5D_io_info_t *io_info, const H5D_dset_io_info_t *dset_info)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Call generic selection operation */
    if (H5D__select_io(io_info, dset_info, dset_info->type_info.src_type_size) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_READERROR, FAIL, "read error");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__select_read() */

/*-------------------------------------------------------------------------
 * Function:	H5D__select_write
 *
 * Purpose:	Writes directly from application memory into a file
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D__select_write(const H5D_io_info_t *io_info, const H5D_dset_io_info_t *dset_info)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Call generic selection operation */
    if (H5D__select_io(io_info, dset_info, dset_info->type_info.dst_type_size) < 0)
        HGOTO_ERROR(H5E_DATASPACE, H5E_WRITEERROR, FAIL, "write error");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__select_write() */
