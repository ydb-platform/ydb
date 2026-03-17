/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2015 University of Houston. All rights reserved.
 * Copyright (c) 2018      Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * These symbols are in a file by themselves to provide nice linker
 * semantics. Since linkers generally pull in symbols by object fules,
 * keeping these symbols as the only symbols in this file prevents
 * utility programs such as "ompi_info" from having to import entire
 * modules just to query their version and parameters
 */

#include "ompi_config.h"
#include "mpi.h"

#include <unistd.h>
#include <sys/uio.h>
#if HAVE_AIO_H
#include <aio.h>
#endif

int fbtl_posix_max_aio_active_reqs=2048;

#include "ompi/mca/fbtl/fbtl.h"
#include "ompi/mca/fbtl/posix/fbtl_posix.h"

/*
 * *******************************************************************
 * ************************ actions structure ************************
 * *******************************************************************
 */
static mca_fbtl_base_module_1_0_0_t posix =  {
    mca_fbtl_posix_module_init,     /* initalise after being selected */
    mca_fbtl_posix_module_finalize, /* close a module on a communicator */
    mca_fbtl_posix_preadv,          /* blocking read */
#if defined (FBTL_POSIX_HAVE_AIO)
    mca_fbtl_posix_ipreadv,         /* non-blocking read*/
#else
    NULL,                           /* non-blocking read */
#endif
    mca_fbtl_posix_pwritev,         /* blocking write */
#if defined (FBTL_POSIX_HAVE_AIO)
    mca_fbtl_posix_ipwritev,        /* non-blocking write */
    mca_fbtl_posix_progress,        /* module specific progress */
    mca_fbtl_posix_request_free     /* free module specific data items on the request */
#else
    NULL,                           /* non-blocking write */
    NULL,                           /* module specific progress */
    NULL                            /* free module specific data items on the request */
#endif
};
/*
 * *******************************************************************
 * ************************* structure ends **************************
 * *******************************************************************
 */

int mca_fbtl_posix_component_init_query(bool enable_progress_threads,
                                      bool enable_mpi_threads) {
    /* Nothing to do */

   return OMPI_SUCCESS;
}

struct mca_fbtl_base_module_1_0_0_t *
mca_fbtl_posix_component_file_query (ompio_file_t *fh, int *priority) {
   *priority = mca_fbtl_posix_priority;

   if (UFS == fh->f_fstype) {
       if (*priority < 50) {
           *priority = 50;
       }
   }

   return &posix;
}

int mca_fbtl_posix_component_file_unquery (ompio_file_t *file) {
   /* This function might be needed for some purposes later. for now it
    * does not have anything to do since there are no steps which need
    * to be undone if this module is not selected */

   return OMPI_SUCCESS;
}

int mca_fbtl_posix_module_init (ompio_file_t *file) {

#if defined (FBTL_POSIX_HAVE_AIO)
    long val = sysconf(_SC_AIO_MAX);
    if ( -1 != val ) {
	fbtl_posix_max_aio_active_reqs = (int)val;
    }
#endif
    return OMPI_SUCCESS;
}


int mca_fbtl_posix_module_finalize (ompio_file_t *file) {
    return OMPI_SUCCESS;
}

bool mca_fbtl_posix_progress ( mca_ompio_request_t *req)
{
    bool ret=false;
#if defined (FBTL_POSIX_HAVE_AIO)
    int i=0, lcount=0, ret_code=0;
    mca_fbtl_posix_request_data_t *data=(mca_fbtl_posix_request_data_t *)req->req_data;
    off_t start_offset, end_offset, total_length;

    for (i=data->aio_first_active_req; i < data->aio_last_active_req; i++ ) {
	if ( EINPROGRESS == data->aio_req_status[i] ) {
	    data->aio_req_status[i] = aio_error ( &data->aio_reqs[i]);
	    if ( 0 == data->aio_req_status[i]){
		data->aio_open_reqs--;
		lcount++;
		/* assuming right now that aio_return will return
		** the number of bytes written/read and not an error code,
		** since aio_error should have returned an error in that
		** case and not 0 ( which means request is complete)
		*/
		data->aio_total_len += aio_return (&data->aio_reqs[i]);
	    }
	    else if ( EINPROGRESS == data->aio_req_status[i]){
		/* not yet done */
		continue;
	    }
	    else {
		/* an error occured. Mark the request done, but
		   set an error code in the status */
		req->req_ompi.req_status.MPI_ERROR = OMPI_ERROR;
		req->req_ompi.req_status._ucount = data->aio_total_len;
		ret = true;
		break;
	    }
	}
	else {
	    lcount++;
	}
    }
#if 0
    printf("lcount=%d open_reqs=%d\n", lcount, data->aio_open_reqs );
#endif

    if ( (lcount == data->aio_req_chunks) && (0 != data->aio_open_reqs )) {
        /* release the lock of the previous operations */
        mca_fbtl_posix_unlock ( &data->aio_lock, data->aio_fh );
        
	/* post the next batch of operations */
	data->aio_first_active_req = data->aio_last_active_req;
	if ( (data->aio_req_count-data->aio_last_active_req) > data->aio_req_chunks ) {
	    data->aio_last_active_req += data->aio_req_chunks;
	}
	else {
	    data->aio_last_active_req = data->aio_req_count;
	}

        start_offset = data->aio_reqs[data->aio_first_active_req].aio_offset;
        end_offset   = data->aio_reqs[data->aio_last_active_req-1].aio_offset + data->aio_reqs[data->aio_last_active_req-1].aio_nbytes;
        total_length = (end_offset - start_offset);

        if ( FBTL_POSIX_READ == data->aio_req_type ) {
            ret_code = mca_fbtl_posix_lock( &data->aio_lock, data->aio_fh, F_RDLCK, start_offset, total_length, OMPIO_LOCK_ENTIRE_REGION );
        }
        else if ( FBTL_POSIX_WRITE == data->aio_req_type ) {
            ret_code = mca_fbtl_posix_lock( &data->aio_lock, data->aio_fh, F_WRLCK, start_offset, total_length, OMPIO_LOCK_ENTIRE_REGION );
        }
        if ( 0 < ret_code ) {
            opal_output(1, "mca_fbtl_posix_progress: error in mca_fbtl_posix_lock() %d", ret_code);
            /* Just in case some part of the lock actually succeeded. */
            mca_fbtl_posix_unlock ( &data->aio_lock, data->aio_fh );
            return OMPI_ERROR;
        }
        
	for ( i=data->aio_first_active_req; i< data->aio_last_active_req; i++ ) {
	    if ( FBTL_POSIX_READ == data->aio_req_type ) {
		if (-1 == aio_read(&data->aio_reqs[i])) {
		    opal_output(1, "mca_fbtl_posix_progress: error in aio_read()");
                    mca_fbtl_posix_unlock ( &data->aio_lock, data->aio_fh );
		    return OMPI_ERROR;
		}
	    }
	    else if ( FBTL_POSIX_WRITE == data->aio_req_type ) {
		if (-1 == aio_write(&data->aio_reqs[i])) {
		    opal_output(1, "mca_fbtl_posix_progress: error in aio_write()");
                    mca_fbtl_posix_unlock ( &data->aio_lock, data->aio_fh );
		    return OMPI_ERROR;
		}
	    }
	}
#if 0
	printf("posting new batch: first=%d last=%d\n", data->aio_first_active_req, data->aio_last_active_req );
#endif
    }

    if ( 0 == data->aio_open_reqs ) {
	/* all pending operations are finished for this request */
	req->req_ompi.req_status.MPI_ERROR = OMPI_SUCCESS;
	req->req_ompi.req_status._ucount = data->aio_total_len;
        mca_fbtl_posix_unlock ( &data->aio_lock, data->aio_fh );
	ret = true;
    }
#endif
    return ret;
}

void mca_fbtl_posix_request_free ( mca_ompio_request_t *req)
{
#if defined (FBTL_POSIX_HAVE_AIO)
    /* Free the fbtl specific data structures */
    mca_fbtl_posix_request_data_t *data=(mca_fbtl_posix_request_data_t *)req->req_data;
    if (NULL != data ) {
        mca_fbtl_posix_unlock ( &data->aio_lock, data->aio_fh );
	if ( NULL != data->aio_reqs ) {
	    free ( data->aio_reqs);
	}
	if ( NULL != data->aio_req_status ) {
	    free ( data->aio_req_status );
	}
	free ( data );
	req->req_data = NULL;
    }
#endif
  return;
}
