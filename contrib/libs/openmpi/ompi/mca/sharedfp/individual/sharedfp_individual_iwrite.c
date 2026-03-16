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
#include "sharedfp_individual.h"

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/mca/sharedfp/sharedfp.h"
#include "ompi/mca/sharedfp/base/base.h"

int mca_sharedfp_individual_iwrite(ompio_file_t *fh,
                                   const void *buf,
                                   int count,
                                   ompi_datatype_t *datatype,
                                   MPI_Request * request)
{
    int ret = OMPI_SUCCESS;
    size_t numofbytes = 0;
    OMPI_MPI_OFFSET_TYPE totalbytes = 0;
    mca_sharedfp_individual_header_record *headnode = NULL;
    struct mca_sharedfp_base_data_t *sh = NULL;

    if(fh->f_sharedfp_data==NULL){
        opal_output(ompi_sharedfp_base_framework.framework_output,
                    "mca_sharedfp_individual_iwrite: module not initialized \n");
        return OMPI_ERROR;
    }
    mca_sharedfp_individual_usage_counter++;

    /* Calculate the number of bytes of data that needs to be written*/
    opal_datatype_type_size ( &datatype->super, &numofbytes);
    totalbytes = count * numofbytes;

    sh = fh->f_sharedfp_data;

    headnode = (mca_sharedfp_individual_header_record*)sh->selected_module_data;
    if ( NULL == headnode)  {
	opal_output (0, "sharedfp_individual_iwrite: headnode is NULL but file is open\n");
	return OMPI_ERROR;
    }

    /*Insert metadata record into a queue*/
    ret = mca_sharedfp_individual_insert_metadata(OMPI_FILE_WRITE_SHARED,totalbytes,sh);


    /*Write the data into individual file*/
    ret = mca_common_ompio_file_iwrite_at ( headnode->datafilehandle, headnode->datafile_offset,
                                            buf, count, datatype, request);
    if ( OMPI_SUCCESS != ret )  {
	opal_output(0,"sharedfp_individual_iwrite: Error while iwriting the datafile \n");
	return ret;
    }

    /* Update the datafileoffset */
    headnode->datafile_offset = headnode->datafile_offset + totalbytes;

    return ret;
}

int mca_sharedfp_individual_write_ordered_begin(ompio_file_t *fh,
                                                const void *buf,
                                                int count,
                                                struct ompi_datatype_t *datatype)
{
    int ret = OMPI_SUCCESS;
    int i = 0;
    size_t numofbytes = 0;
    size_t totalbytes = 0;
    OMPI_MPI_OFFSET_TYPE *offbuff=NULL;
    OMPI_MPI_OFFSET_TYPE global_offset = 0;
    OMPI_MPI_OFFSET_TYPE prev_offset = 0;
    OMPI_MPI_OFFSET_TYPE temp = 0, offset = 0;
    mca_sharedfp_individual_header_record *headnode = NULL;
    struct mca_sharedfp_base_data_t *sh = NULL;

    if(fh->f_sharedfp_data==NULL){
        opal_output(ompi_sharedfp_base_framework.framework_output,
                    "sharedfp_individual_write_ordered_begin - module not initialized\n");
        return OMPI_ERROR;
    }

    if ( true == fh->f_split_coll_in_use ) {
        opal_output(0, "Only one split collective I/O operation allowed per file handle "
                    "at any given point in time!\n");
        return MPI_ERR_REQUEST;
    }
    mca_sharedfp_individual_usage_counter++;

    /*Retrieve the sharedfp data structures*/
    sh = fh->f_sharedfp_data;

    /* Calculate the number of bytes of data that needs to be written*/
    opal_datatype_type_size ( &datatype->super, &numofbytes);
    totalbytes = count * numofbytes;

    headnode = (mca_sharedfp_individual_header_record*)sh->selected_module_data;
    if ( NULL == headnode)  {
	opal_output (0, "sharedfp_individual_write_ordered_begin: headnode is NULL but file is open\n");
	return OMPI_ERROR;
    }

    /* Data from all the metadata is combined and written to the main file */
    ret  = mca_sharedfp_individual_collaborate_data ( sh, fh );
    if ( OMPI_SUCCESS != ret)  {
	return ret;
    }

    if ( 0 == fh->f_rank )  {
	offbuff = (OMPI_MPI_OFFSET_TYPE *)malloc ( sizeof(OMPI_MPI_OFFSET_TYPE) * fh->f_size);
	if (NULL == offbuff ) {
	    return OMPI_ERR_OUT_OF_RESOURCE;
	}
    }

    /*collect the total bytes to be written*/
    ret = fh->f_comm->c_coll->coll_gather ( &totalbytes, 
                                            1, 
                                            OMPI_OFFSET_DATATYPE,
                                            offbuff, 
                                            1, 
                                            OMPI_OFFSET_DATATYPE, 
                                            0,
                                            fh->f_comm, 
                                            fh->f_comm->c_coll->coll_gather_module );

    if ( OMPI_SUCCESS != ret ) {
	opal_output(0,"sharedfp_individual_write_ordered_begin: Error in gatherring offsets \n");
        goto exit;
    }
    
    if ( 0 == fh->f_rank ) {
        prev_offset = offbuff[0];
        offbuff[0]   = sh->global_offset;

        for (i = 1; i < fh->f_size ; i++){
            temp = offbuff[i];
            offbuff[i] = offbuff[i - 1] + prev_offset;
            prev_offset = temp;
        }

        for (i = 0; i < fh->f_size; i++){
            global_offset = offbuff[fh->f_size - 1] + prev_offset;
        }
    }


    /* Scatter the results to the other processes */
    ret = fh->f_comm->c_coll->coll_scatter ( offbuff, 
                                             1, 
                                             OMPI_OFFSET_DATATYPE,
                                             &offset, 
                                             1, 
                                             OMPI_OFFSET_DATATYPE, 
                                             0,
                                             fh->f_comm, 
                                             fh->f_comm->c_coll->coll_scatter_module );
    if ( OMPI_SUCCESS != ret )  {
	opal_output(0,"sharedfp_individual_write_ordered_begin: Error in scattering offsets \n");
	goto exit;
    }

    ret = fh->f_comm->c_coll->coll_bcast ( &global_offset, 
                                           1, 
                                           OMPI_OFFSET_DATATYPE,
                                           0, 
                                           fh->f_comm, 
                                           fh->f_comm->c_coll->coll_bcast_module );
    if ( OMPI_SUCCESS != ret )  {
	opal_output(0,"sharedfp_individual_write_ordered_begin: Error while bcasting global offset \n");
	goto exit;
    }

    sh->global_offset = global_offset;

    /*use file_write_at_all to ensure the order*/
    ret = mca_common_ompio_file_iwrite_at_all(fh, offset, buf, count, datatype,
                                              &fh->f_split_coll_req);
    fh->f_split_coll_in_use = true;
    if ( OMPI_SUCCESS != ret )  {
	opal_output(0,"sharedfp_individual_write_ordered_begin: Error while writing the datafile \n");
    }

exit:
    if ( NULL != offbuff ) {
	free ( offbuff);
    }

    return ret;
}

int mca_sharedfp_individual_write_ordered_end(ompio_file_t *fh,
                                              const void *buf,
                                              ompi_status_public_t *status)
{
    int ret = OMPI_SUCCESS;
    ret = ompi_request_wait ( &fh->f_split_coll_req, status );

    /* remove the flag again */
    fh->f_split_coll_in_use = false;
    return ret;
}
