/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2018 University of Houston. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_COMMON_OMPIO_CUDA_H
#define MCA_COMMON_OMPIO_CUDA_H


#define OMPIO_CUDA_PREPARE_BUF(_fh,_buf,_count,_datatype,_tbuf,_convertor,_max_data,_decoded_iov,_iov_count){ \
    opal_convertor_clone ( _fh->f_convertor, _convertor, 0);                          \
    opal_convertor_prepare_for_send ( _convertor, &(_datatype->super), _count, _buf );\
    opal_convertor_get_packed_size( _convertor, &_max_data );           \
    _tbuf = mca_common_ompio_alloc_buf (_fh, _max_data);                \
    if ( NULL == _tbuf ) {                                              \
        opal_output(1, "common_ompio: error allocating memory\n");      \
        return OMPI_ERR_OUT_OF_RESOURCE;                                \
    }                                                                   \
    _decoded_iov = (struct iovec *) malloc ( sizeof ( struct iovec ));  \
    if ( NULL == _decoded_iov ) {                                       \
        opal_output(1, "common_ompio: could not allocate memory.\n");   \
        return OMPI_ERR_OUT_OF_RESOURCE;                                \
    }                                                                   \
    _decoded_iov->iov_base = _tbuf;                                     \
    _decoded_iov->iov_len  = _max_data;                                 \
    _iov_count=1;}


void mca_common_ompio_check_gpu_buf ( ompio_file_t *fh, const void *buf, 
				      int *is_gpu, int *is_managed);
int mca_common_ompio_cuda_alloc_init ( void );
int mca_common_ompio_cuda_alloc_fini ( void );


void* mca_common_ompio_alloc_buf ( ompio_file_t *fh, size_t bufsize);
void mca_common_ompio_release_buf ( ompio_file_t *fh,  void *buf );

#endif
