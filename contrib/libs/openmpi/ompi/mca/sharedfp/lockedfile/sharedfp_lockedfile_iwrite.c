/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2013-2018 University of Houston. All rights reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "ompi_config.h"
#include "sharedfp_lockedfile.h"

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/mca/sharedfp/sharedfp.h"
#include "ompi/mca/sharedfp/base/base.h"
#include "ompi/mca/common/ompio/common_ompio.h"

int mca_sharedfp_lockedfile_iwrite(ompio_file_t *fh,
                                   const void *buf,
                                   int count,
                                   ompi_datatype_t *datatype,
                                   MPI_Request * request)
{
    int ret = OMPI_SUCCESS;
    OMPI_MPI_OFFSET_TYPE offset = 0;
    long bytesRequested = 0;
    size_t numofBytes;
    struct mca_sharedfp_base_data_t *sh = NULL;

    if(fh->f_sharedfp_data==NULL){
        opal_output(ompi_sharedfp_base_framework.framework_output,
                    "sharedfp_lockedfile_iwrite: module not initialized \n");
        return OMPI_ERROR;
    }

    /*Calculate the number of bytes to write*/
    opal_datatype_type_size ( &datatype->super, &numofBytes);
    bytesRequested = count * numofBytes;
    if ( mca_sharedfp_lockedfile_verbose ) {
	opal_output(ompi_sharedfp_base_framework.framework_output,
		    "sharedfp_lockedfile_iwrite: Bytes Requested is %ld\n",bytesRequested);
    }

    /*Retrieve the shared file data struct*/
    sh = fh->f_sharedfp_data;

    /*Request the offset to write bytesRequested bytes*/
    ret = mca_sharedfp_lockedfile_request_position(sh,bytesRequested,&offset);
    offset /= fh->f_etype_size;

    if ( -1 != ret) {
	if ( mca_sharedfp_lockedfile_verbose ) {
	    opal_output(ompi_sharedfp_base_framework.framework_output,
			"sharedfp_lockedfile_iwrite: Offset received is %lld\n",offset);
	}

        /* Write to the file */
        ret = mca_common_ompio_file_iwrite_at(fh,offset,buf,count,datatype,request);
    }

    return ret;
}

int mca_sharedfp_lockedfile_write_ordered_begin(ompio_file_t *fh,
                                                const void *buf,
                                                int count,
                                                struct ompi_datatype_t *datatype)
{
    int ret = OMPI_SUCCESS;
    OMPI_MPI_OFFSET_TYPE offset = 0;
    long sendBuff = 0;
    long *buff=NULL;
    long offsetBuff;
    OMPI_MPI_OFFSET_TYPE offsetReceived = 0;
    long bytesRequested = 0;
    int recvcnt = 1, sendcnt = 1;
    size_t numofBytes;
    int rank, size, i;
    struct mca_sharedfp_base_data_t *sh = NULL;

    if(fh->f_sharedfp_data==NULL){
        opal_output(ompi_sharedfp_base_framework.framework_output,
                    "sharedfp_lockedfile_write_ordered_begin: module not initialized \n");
        return OMPI_ERROR;
    }


    if ( true == fh->f_split_coll_in_use ) {
        opal_output(0, "Only one split collective I/O operation allowed per file handle at "
                    "any given point in time!\n");
        return MPI_ERR_REQUEST;
    }

    /*Retrieve the new communicator*/
    sh = fh->f_sharedfp_data;

    /* Calculate the number of bytes to write*/
    opal_datatype_type_size ( &datatype->super, &numofBytes);
    sendBuff = count * numofBytes;

    /* Get the ranks in the communicator */
    rank = ompi_comm_rank ( fh->f_comm );
    size = ompi_comm_size ( fh->f_comm );

    if ( 0 == rank ) {
        buff = (long*) malloc (sizeof(long) * size);
        if ( NULL == buff ) {
            return OMPI_ERR_OUT_OF_RESOURCE;
	}
    }

    ret = fh->f_comm->c_coll->coll_gather ( &sendBuff, 
                                            sendcnt, 
                                            OMPI_OFFSET_DATATYPE, 
                                            buff, 
                                            recvcnt,
                                            OMPI_OFFSET_DATATYPE, 
                                            0, 
                                            fh->f_comm,
                                            fh->f_comm->c_coll->coll_gather_module );
    if ( OMPI_SUCCESS != ret ) {
	goto exit;
    }

    /* All the counts are present now in the recvBuff.
       The size of recvBuff is sizeof_newComm
     */
    if (rank == 0) {
        for ( i = 0; i < size ; i ++)  {
            bytesRequested += buff[i];
	    if ( mca_sharedfp_lockedfile_verbose ) {
		opal_output(ompi_sharedfp_base_framework.framework_output,
			    "sharedfp_lockedfile_write_ordered_begin: Bytes requested are %ld\n",bytesRequested);
	    }
        }

        /*Request the offset to write bytesRequested bytes
          only the root process needs to do the request,
          since the root process will then tell the other
          processes at what offset they should write their
          share of the data.
         */
        ret = mca_sharedfp_lockedfile_request_position(sh,bytesRequested,&offsetReceived);
        if ( OMPI_SUCCESS != ret ){
            goto exit;
        }
	if ( mca_sharedfp_lockedfile_verbose ) {
	    opal_output(ompi_sharedfp_base_framework.framework_output,
                        "sharedfp_lockedfile_write_ordered_begin: Offset received is %lld\n",offsetReceived);
	}
        buff[0] += offsetReceived;
        for (i = 1 ; i < size; i++) {
            buff[i] += buff[i-1];
        }
    }

    /* Scatter the results to the other processes*/
    ret = fh->f_comm->c_coll->coll_scatter ( buff, 
                                             sendcnt, 
                                             OMPI_OFFSET_DATATYPE,
                                             &offsetBuff, 
                                             recvcnt, 
                                             OMPI_OFFSET_DATATYPE, 
                                             0,
                                             fh->f_comm, 
                                             fh->f_comm->c_coll->coll_scatter_module );
    if ( OMPI_SUCCESS != ret ) {
	goto exit;
    }

    /*Each process now has its own individual offset*/
    offset = offsetBuff - sendBuff;
    offset /= fh->f_etype_size;

    if ( mca_sharedfp_lockedfile_verbose ) {
	opal_output(ompi_sharedfp_base_framework.framework_output,
                    "sharedfp_lockedfile_write_ordered_begin: Offset returned is %lld\n",offset);
     }

    ret = mca_common_ompio_file_iwrite_at_all ( fh, offset, buf, count, datatype, &fh->f_split_coll_req );
    fh->f_split_coll_in_use = true;

exit:
    if ( NULL != buff ) {
	free ( buff);
    }

    return ret;
}



int mca_sharedfp_lockedfile_write_ordered_end(ompio_file_t *fh,
                                              const void *buf,
                                              ompi_status_public_t *status)
{
    int ret = OMPI_SUCCESS;
    ret = ompi_request_wait ( &fh->f_split_coll_req, status );

    /* remove the flag again */
    fh->f_split_coll_in_use = false;
    return ret;
}
