/*
 *  Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                          University Research and Technology
 *                          Corporation.  All rights reserved.
 *  Copyright (c) 2004-2016 The University of Tennessee and The University
 *                          of Tennessee Research Foundation.  All rights
 *                          reserved.
 *  Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                          University of Stuttgart.  All rights reserved.
 *  Copyright (c) 2004-2005 The Regents of the University of California.
 *                          All rights reserved.
 *  Copyright (c) 2008-2019 University of Houston. All rights reserved.
 *  Copyright (c) 2018      Research Organization for Information Science
 *                          and Technology (RIST). All rights reserved.
 *  $COPYRIGHT$
 *
 *  Additional copyrights may follow
 *
 *  $HEADER$
 */

#include "ompi_config.h"

#include "ompi/communicator/communicator.h"
#include "ompi/info/info.h"
#include "ompi/file/file.h"
#include "ompi/mca/fs/fs.h"
#include "ompi/mca/fs/base/base.h"
#include "ompi/mca/fcoll/fcoll.h"
#include "ompi/mca/fcoll/base/base.h"
#include "ompi/mca/fbtl/fbtl.h"
#include "ompi/mca/fbtl/base/base.h"

#include "common_ompio.h"
#include "common_ompio_request.h"
#include <unistd.h>
#include <math.h>

#if OPAL_CUDA_SUPPORT
#include "common_ompio_cuda.h"
#endif

/* Read and write routines are split into two interfaces.
**   The
**   mca_io_ompio_file_read/write[_at]
**
** routines are the ones registered with the ompio modules.
** The
**
** mca_common_ompio_file_read/write[_at]
**
** routesin are used e.g. from the shared file pointer modules.
** The main difference is, that the first one takes an ompi_file_t
** as a file pointer argument, while the second uses the ompio internal
** ompio_file_t structure.
*/

int mca_common_ompio_file_read (ompio_file_t *fh,
			      void *buf,
			      int count,
			      struct ompi_datatype_t *datatype,
			      ompi_status_public_t *status)
{
    int ret = OMPI_SUCCESS;

    size_t total_bytes_read = 0;       /* total bytes that have been read*/
    size_t bytes_per_cycle = 0;        /* total read in each cycle by each process*/
    int index = 0;
    int cycles = 0;

    uint32_t iov_count = 0;
    struct iovec *decoded_iov = NULL;

    size_t max_data=0, real_bytes_read=0;
    size_t spc=0;
    ssize_t ret_code=0;
    int i = 0; /* index into the decoded iovec of the buffer */
    int j = 0; /* index into the file vie iovec */

    if (fh->f_amode & MPI_MODE_WRONLY){
//      opal_output(10, "Improper use of FILE Mode, Using WRONLY for Read!\n");
        ret = MPI_ERR_ACCESS;
      return ret;
    }

    if ( 0 == count ) {
        if ( MPI_STATUS_IGNORE != status ) {
            status->_ucount = 0;
        }
        return ret;
    }


#if OPAL_CUDA_SUPPORT
    int is_gpu, is_managed;
    opal_convertor_t convertor;
    mca_common_ompio_check_gpu_buf ( fh, buf, &is_gpu, &is_managed);
    if ( is_gpu && !is_managed ) {
        char *tbuf=NULL;

        OMPIO_CUDA_PREPARE_BUF(fh,buf,count,datatype,tbuf,&convertor,max_data,decoded_iov,iov_count);        
        
   }
    else {
        mca_common_ompio_decode_datatype (fh,
                                          datatype,
                                          count,
                                          buf,
                                          &max_data,
                                          &decoded_iov,
                                          &iov_count);
    }
#else
    mca_common_ompio_decode_datatype (fh,
                                      datatype,
                                      count,
                                      buf,
                                      &max_data,
                                      &decoded_iov,
                                      &iov_count);
#endif
    if ( 0 < max_data && 0 == fh->f_iov_count  ) {
        if ( MPI_STATUS_IGNORE != status ) {
            status->_ucount = 0;
        }
        return OMPI_SUCCESS;
    }

    if ( -1 == OMPIO_MCA_GET(fh, cycle_buffer_size )) {
        bytes_per_cycle = max_data;
    }
    else {
	bytes_per_cycle = OMPIO_MCA_GET(fh, cycle_buffer_size);
    }
    cycles = ceil((double)max_data/bytes_per_cycle);
    
#if 0
	printf ("Bytes per Cycle: %d   Cycles: %d max_data:%d \n",bytes_per_cycle, cycles, max_data);
#endif

    j = fh->f_index_in_file_view;

    for (index = 0; index < cycles; index++) {

	mca_common_ompio_build_io_array ( fh,
                                          index,
                                          cycles,
                                          bytes_per_cycle,
                                          max_data,
                                          iov_count,
                                          decoded_iov,
                                          &i,
                                          &j,
                                          &total_bytes_read, 
                                          &spc);

        if (fh->f_num_of_io_entries) {
            ret_code = fh->f_fbtl->fbtl_preadv (fh);
            if ( 0<= ret_code ) {
                real_bytes_read+=(size_t)ret_code;
            }
        }

        fh->f_num_of_io_entries = 0;
        if (NULL != fh->f_io_array) {
            free (fh->f_io_array);
            fh->f_io_array = NULL;
        }
    }

#if OPAL_CUDA_SUPPORT
    if ( is_gpu && !is_managed ) {
        size_t pos=0;

        opal_convertor_unpack (&convertor, decoded_iov, &iov_count, &pos );
        opal_convertor_cleanup (&convertor);
        mca_common_ompio_release_buf (fh, decoded_iov->iov_base);
    }
#endif
    if (NULL != decoded_iov) {
        free (decoded_iov);
        decoded_iov = NULL;
    }

    if ( MPI_STATUS_IGNORE != status ) {
        status->_ucount = real_bytes_read;
    }

    return ret;
}

int mca_common_ompio_file_read_at (ompio_file_t *fh,
				 OMPI_MPI_OFFSET_TYPE offset,
				 void *buf,
				 int count,
				 struct ompi_datatype_t *datatype,
				 ompi_status_public_t * status)
{
    int ret = OMPI_SUCCESS;
    OMPI_MPI_OFFSET_TYPE prev_offset;

    mca_common_ompio_file_get_position (fh, &prev_offset );

    mca_common_ompio_set_explicit_offset (fh, offset);
    ret = mca_common_ompio_file_read (fh,
				    buf,
				    count,
				    datatype,
				    status);

    // An explicit offset file operation is not suppsed to modify
    // the internal file pointer. So reset the pointer
    // to the previous value
    mca_common_ompio_set_explicit_offset (fh, prev_offset);

    return ret;
}


int mca_common_ompio_file_iread (ompio_file_t *fh,
			       void *buf,
			       int count,
			       struct ompi_datatype_t *datatype,
			       ompi_request_t **request)
{
    int ret = OMPI_SUCCESS;
    mca_ompio_request_t *ompio_req=NULL;
    size_t spc=0;

    if (fh->f_amode & MPI_MODE_WRONLY){
//      opal_output(10, "Improper use of FILE Mode, Using WRONLY for Read!\n");
        ret = MPI_ERR_ACCESS;
      return ret;
    }

    mca_common_ompio_request_alloc ( &ompio_req, MCA_OMPIO_REQUEST_READ);

    if ( 0 == count ) {
        ompio_req->req_ompi.req_status.MPI_ERROR = OMPI_SUCCESS;
        ompio_req->req_ompi.req_status._ucount = 0;
        ompi_request_complete (&ompio_req->req_ompi, false);
        *request = (ompi_request_t *) ompio_req;
        
        return OMPI_SUCCESS;
    }

    if ( NULL != fh->f_fbtl->fbtl_ipreadv ) {
        // This fbtl has support for non-blocking operations

        size_t total_bytes_read = 0;       /* total bytes that have been read*/
        uint32_t iov_count = 0;
        struct iovec *decoded_iov = NULL;
        
        size_t max_data = 0;
        int i = 0; /* index into the decoded iovec of the buffer */
        int j = 0; /* index into the file vie iovec */
        
#if OPAL_CUDA_SUPPORT
        int is_gpu, is_managed;
        mca_common_ompio_check_gpu_buf ( fh, buf, &is_gpu, &is_managed);
        if ( is_gpu && !is_managed ) {
            char *tbuf=NULL;
            
            OMPIO_CUDA_PREPARE_BUF(fh,buf,count,datatype,tbuf,&ompio_req->req_convertor,max_data,decoded_iov,iov_count);        
            
            ompio_req->req_tbuf = tbuf;
            ompio_req->req_size = max_data;
        }
        else {
            mca_common_ompio_decode_datatype (fh,
                                              datatype,
                                              count,
                                              buf,
                                              &max_data,
                                              &decoded_iov,
                                              &iov_count);
        }
#else
        mca_common_ompio_decode_datatype (fh,
                                          datatype,
                                          count,
                                          buf,
                                          &max_data,
                                          &decoded_iov,
                                          &iov_count);
#endif
        if ( 0 < max_data && 0 == fh->f_iov_count  ) {
            ompio_req->req_ompi.req_status.MPI_ERROR = OMPI_SUCCESS;
            ompio_req->req_ompi.req_status._ucount = 0;
            ompi_request_complete (&ompio_req->req_ompi, false);
            *request = (ompi_request_t *) ompio_req;
            return OMPI_SUCCESS;
        }

        // Non-blocking operations have to occur in a single cycle
        j = fh->f_index_in_file_view;
        
        mca_common_ompio_build_io_array ( fh,
                                          0,         // index
                                          1,         // no. of cyces
                                          max_data,  // setting bytes per cycle to match data
                                          max_data,
                                          iov_count,
                                          decoded_iov,
                                          &i,
                                          &j,
                                          &total_bytes_read, 
                                          &spc);

	if (fh->f_num_of_io_entries) {
	  fh->f_fbtl->fbtl_ipreadv (fh, (ompi_request_t *) ompio_req);
	}

        mca_common_ompio_register_progress ();

	fh->f_num_of_io_entries = 0;
	if (NULL != fh->f_io_array) {
	    free (fh->f_io_array);
	    fh->f_io_array = NULL;
	}

	if (NULL != decoded_iov) {
	    free (decoded_iov);
	    decoded_iov = NULL;
	}
    }
    else {
	// This fbtl does not  support non-blocking operations
	ompi_status_public_t status;
	ret = mca_common_ompio_file_read (fh, buf, count, datatype, &status);

	ompio_req->req_ompi.req_status.MPI_ERROR = ret;
	ompio_req->req_ompi.req_status._ucount = status._ucount;
	ompi_request_complete (&ompio_req->req_ompi, false);
    }

    *request = (ompi_request_t *) ompio_req;
    return ret;
}


int mca_common_ompio_file_iread_at (ompio_file_t *fh,
				  OMPI_MPI_OFFSET_TYPE offset,
				  void *buf,
				  int count,
				  struct ompi_datatype_t *datatype,
				  ompi_request_t **request)
{
    int ret = OMPI_SUCCESS;
    OMPI_MPI_OFFSET_TYPE prev_offset;
    mca_common_ompio_file_get_position (fh, &prev_offset );

    mca_common_ompio_set_explicit_offset (fh, offset);
    ret = mca_common_ompio_file_iread (fh,
				    buf,
				    count,
				    datatype,
				    request);

    /* An explicit offset file operation is not suppsed to modify
    ** the internal file pointer. So reset the pointer
    ** to the previous value
    ** It is OK to reset the position already here, althgouth
    ** the operation might still be pending/ongoing, since
    ** the entire array of <offset, length, memaddress> have
    ** already been constructed in the file_iread operation
    */
    mca_common_ompio_set_explicit_offset (fh, prev_offset);

    return ret;
}


/* Infrastructure for collective operations  */
int mca_common_ompio_file_read_at_all (ompio_file_t *fh,
				     OMPI_MPI_OFFSET_TYPE offset,
				     void *buf,
				     int count,
				     struct ompi_datatype_t *datatype,
				     ompi_status_public_t * status)
{
    int ret = OMPI_SUCCESS;
    OMPI_MPI_OFFSET_TYPE prev_offset;
    mca_common_ompio_file_get_position (fh, &prev_offset );

    mca_common_ompio_set_explicit_offset (fh, offset);
    ret = fh->f_fcoll->fcoll_file_read_all (fh,
                                            buf,
                                            count,
                                            datatype,
                                            status);

    mca_common_ompio_set_explicit_offset (fh, prev_offset);
    return ret;
}

int mca_common_ompio_file_iread_at_all (ompio_file_t *fp,
				      OMPI_MPI_OFFSET_TYPE offset,
				      void *buf,
				      int count,
				      struct ompi_datatype_t *datatype,
				      ompi_request_t **request)
{
    int ret = OMPI_SUCCESS;
    OMPI_MPI_OFFSET_TYPE prev_offset;

    mca_common_ompio_file_get_position (fp, &prev_offset );
    mca_common_ompio_set_explicit_offset (fp, offset);

    if ( NULL != fp->f_fcoll->fcoll_file_iread_all ) {
	ret = fp->f_fcoll->fcoll_file_iread_all (fp,
						 buf,
						 count,
						 datatype,
						 request);
    }
    else {
	/* this fcoll component does not support non-blocking
	   collective I/O operations. WE fake it with
	   individual non-blocking I/O operations. */
	ret = mca_common_ompio_file_iread ( fp, buf, count, datatype, request );
    }


    mca_common_ompio_set_explicit_offset (fp, prev_offset);
    return ret;
}

int mca_common_ompio_set_explicit_offset (ompio_file_t *fh,
                                          OMPI_MPI_OFFSET_TYPE offset)
{
    int i = 0;
    int k = 0;

    if ( fh->f_view_size  > 0 ) {
	/* starting offset of the current copy of the filew view */
	fh->f_offset = (fh->f_view_extent *
			((offset*fh->f_etype_size) / fh->f_view_size)) + fh->f_disp;


	/* number of bytes used within the current copy of the file view */
	fh->f_total_bytes = (offset*fh->f_etype_size) % fh->f_view_size;
	i = fh->f_total_bytes;


	/* Initialize the block id and the starting offset of the current block
	   within the current copy of the file view to zero */
	fh->f_index_in_file_view = 0;
	fh->f_position_in_file_view = 0;

	/* determine block id that the offset is located in and
	   the starting offset of that block */
	k = fh->f_decoded_iov[fh->f_index_in_file_view].iov_len;
	while (i >= k) {
	    fh->f_position_in_file_view = k;
	    fh->f_index_in_file_view++;
	    k += fh->f_decoded_iov[fh->f_index_in_file_view].iov_len;
	}
    }

    return OMPI_SUCCESS;
}
