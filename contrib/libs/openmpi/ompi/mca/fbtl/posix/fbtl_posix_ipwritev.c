/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2015 University of Houston. All rights reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "fbtl_posix.h"

#include <unistd.h>
#include <sys/uio.h>
#if HAVE_AIO_H
#include <aio.h>
#endif

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/mca/fbtl/fbtl.h"

ssize_t  mca_fbtl_posix_ipwritev (ompio_file_t *fh,
				 ompi_request_t *request)
{
#if defined(FBTL_POSIX_HAVE_AIO)
    mca_fbtl_posix_request_data_t *data;
    mca_ompio_request_t *req = (mca_ompio_request_t *) request;
    int i=0, ret;
    off_t start_offset, end_offset, total_length;

    data = (mca_fbtl_posix_request_data_t *) malloc ( sizeof (mca_fbtl_posix_request_data_t));
    if ( NULL == data ) {
        opal_output (1,"could not allocate memory\n");
        return 0;
    }

    data->aio_req_count = fh->f_num_of_io_entries;
    data->aio_open_reqs = fh->f_num_of_io_entries;
    data->aio_req_type  = FBTL_POSIX_WRITE;
    data->aio_req_chunks = fbtl_posix_max_aio_active_reqs;
    data->aio_total_len = 0;
    data->aio_reqs = (struct aiocb *) malloc (sizeof(struct aiocb) *
                                              fh->f_num_of_io_entries);
    if (NULL == data->aio_reqs) {
        opal_output(1, "OUT OF MEMORY\n");
        free(data);
        return 0;
    }

    data->aio_req_status = (int *) malloc (sizeof(int) * fh->f_num_of_io_entries);
    if (NULL == data->aio_req_status) {
        opal_output(1, "OUT OF MEMORY\n");
        free(data->aio_reqs);
        free(data);
        return 0;
    }
    data->aio_fh = fh;

    for ( i=0; i<fh->f_num_of_io_entries; i++ ) {
        data->aio_reqs[i].aio_offset  = (OMPI_MPI_OFFSET_TYPE)(intptr_t)
            fh->f_io_array[i].offset;
        data->aio_reqs[i].aio_buf     = fh->f_io_array[i].memory_address;
        data->aio_reqs[i].aio_nbytes  = fh->f_io_array[i].length;
        data->aio_reqs[i].aio_fildes  = fh->fd;
        data->aio_reqs[i].aio_reqprio = 0;
        data->aio_reqs[i].aio_sigevent.sigev_notify = SIGEV_NONE;
	data->aio_req_status[i]        = EINPROGRESS;
    }

    data->aio_first_active_req = 0;
    if ( data->aio_req_count > data->aio_req_chunks ) {
	data->aio_last_active_req = data->aio_req_chunks;
    }
    else {
	data->aio_last_active_req = data->aio_req_count;
    }
    
    start_offset = data->aio_reqs[data->aio_first_active_req].aio_offset;
    end_offset   = data->aio_reqs[data->aio_last_active_req-1].aio_offset + data->aio_reqs[data->aio_last_active_req-1].aio_nbytes;
    total_length = (end_offset - start_offset);
    ret = mca_fbtl_posix_lock( &data->aio_lock, data->aio_fh, F_WRLCK, start_offset, total_length, OMPIO_LOCK_ENTIRE_REGION );
    if ( 0 < ret ) {
        opal_output(1, "mca_fbtl_posix_ipwritev: error in mca_fbtl_posix_lock() error ret=%d %s", ret, strerror(errno));
        mca_fbtl_posix_unlock ( &data->aio_lock, data->aio_fh );            
        free(data->aio_reqs);
        free(data->aio_req_status);
        free(data);
        return OMPI_ERROR;
    }

    for (i=0; i < data->aio_last_active_req; i++) {
        if (-1 == aio_write(&data->aio_reqs[i])) {
            opal_output(1, "mca_fbtl_posix_ipwritev: error in aio_write():  %s", strerror(errno));
            mca_fbtl_posix_unlock ( &data->aio_lock, data->aio_fh );                    
            free(data->aio_req_status);
            free(data->aio_reqs);
            free(data);
            return OMPI_ERROR;
        }
    }

    req->req_data = data;
    req->req_progress_fn = mca_fbtl_posix_progress;
    req->req_free_fn     = mca_fbtl_posix_request_free;
#endif
    return OMPI_SUCCESS;
}
