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
#include "sharedfp_sm.h"

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/mca/sharedfp/sharedfp.h"
#include "ompi/mca/sharedfp/base/base.h"

int mca_sharedfp_sm_write (ompio_file_t *fh,
                           const void *buf,
                           int count,
                           struct ompi_datatype_t *datatype,
                           ompi_status_public_t *status)
{
    int ret = OMPI_SUCCESS;
    OMPI_MPI_OFFSET_TYPE offset = 0;
    long bytesRequested = 0;
    size_t numofBytes;

    if( NULL == fh->f_sharedfp_data ){
        opal_output(ompi_sharedfp_base_framework.framework_output,
                    "sharedfp_sm_write:  module not initialized\n");
        return OMPI_ERROR;
    }

    /* Calculate the number of bytes to write*/
    opal_datatype_type_size ( &datatype->super, &numofBytes);
    bytesRequested = count * numofBytes;

    /*Retrieve the shared file data struct*/

    if ( mca_sharedfp_sm_verbose ) {
        opal_output(ompi_sharedfp_base_framework.framework_output,
                    "sharedfp_sm_write: Requested is %ld\n",bytesRequested);
    }

    /*Request the offset to write bytesRequested bytes*/
    ret = mca_sharedfp_sm_request_position(fh, bytesRequested,&offset);
    offset /= fh->f_etype_size;
    if ( -1 != ret ) {
        if ( mca_sharedfp_sm_verbose ) {
            opal_output(ompi_sharedfp_base_framework.framework_output,
                        "sharedfp_sm_write: fset received is %lld\n",offset);
        }

        /* Write to the file*/
        ret = mca_common_ompio_file_write_at(fh,offset,buf,count,datatype,status);
    }

    return ret;
}

int mca_sharedfp_sm_write_ordered (ompio_file_t *fh,
                                   const void *buf,
                                   int count,
                                   struct ompi_datatype_t *datatype,
                                   ompi_status_public_t *status)
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
    int i;

    if( NULL == fh->f_sharedfp_data){
        opal_output(ompi_sharedfp_base_framework.framework_output,
                    "sharedfp_sm_write_ordered: module not initialzed \n");
        return OMPI_ERROR;
    }

    /* Calculate the number of bytes to write*/
    opal_datatype_type_size ( &datatype->super, &numofBytes);
    sendBuff = count * numofBytes;

    if ( 0 == fh->f_rank ) {
        buff = (long*)malloc(sizeof(long) * fh->f_size);
        if ( NULL == buff )
            return OMPI_ERR_OUT_OF_RESOURCE;
    }

    ret = fh->f_comm->c_coll->coll_gather ( &sendBuff, sendcnt, OMPI_OFFSET_DATATYPE,
                                            buff, recvcnt, OMPI_OFFSET_DATATYPE, 0,
                                            fh->f_comm, fh->f_comm->c_coll->coll_gather_module );
    if ( OMPI_SUCCESS != ret ) {
        goto exit;
    }

    /* All the counts are present now in the recvBuff.
    ** The size of recvBuff is sizeof_newComm
    */
    if (  0 == fh->f_rank ) {
        for (i = 0; i < fh->f_size ; i ++) {
            bytesRequested += buff[i];
            if ( mca_sharedfp_sm_verbose ) {
                opal_output(ompi_sharedfp_base_framework.framework_output,
                            "sharedfp_sm_write_ordered: Bytes requested are %ld\n",bytesRequested);
            }
        }

        /* Request the offset to write bytesRequested bytes
        ** only the root process needs to do the request,
        ** since the root process will then tell the other
        ** processes at what offset they should write their
        ** share of the data.
        */
        ret = mca_sharedfp_sm_request_position(fh,bytesRequested,&offsetReceived);
        if( OMPI_SUCCESS != ret){
            goto exit;
        }
        if ( mca_sharedfp_sm_verbose ) {
            opal_output(ompi_sharedfp_base_framework.framework_output,
                        "sharedfp_sm_write_ordered: Offset received is %lld\n",offsetReceived);
        }
        buff[0] += offsetReceived;

        for (i = 1 ; i < fh->f_size; i++) {
            buff[i] += buff[i-1];
        }
    }

    /* Scatter the results to the other processes*/
    ret = fh->f_comm->c_coll->coll_scatter ( buff, sendcnt, OMPI_OFFSET_DATATYPE,
                                             &offsetBuff, recvcnt, OMPI_OFFSET_DATATYPE, 0,
                                             fh->f_comm, fh->f_comm->c_coll->coll_scatter_module );

    if ( OMPI_SUCCESS != ret ) {
        goto exit;
    }

    /* Each process now has its own individual offset */
    offset = offsetBuff - sendBuff;
    offset /= fh->f_etype_size;

    if ( mca_sharedfp_sm_verbose ) {
        opal_output(ompi_sharedfp_base_framework.framework_output,
                    "sharedfp_sm_write_ordered: Offset returned is %lld\n",offset);
    }
    /* write to the file */
    ret = mca_common_ompio_file_write_at_all(fh,offset,buf,count,datatype,status);

exit:
    if ( NULL != buff ) {
        free ( buff );
    }

    return ret;
}
