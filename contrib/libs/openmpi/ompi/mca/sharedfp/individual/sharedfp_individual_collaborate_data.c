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
 * Copyright (c) 2018      Research Organization for Information Science
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
#include "ompi/mca/common/ompio/common_ompio.h"

#include <stdlib.h>
#include <stdio.h>

int mca_sharedfp_individual_collaborate_data(struct mca_sharedfp_base_data_t *sh, ompio_file_t *ompio_fh)
{
    int ret = OMPI_SUCCESS;
    mca_sharedfp_individual_header_record *headnode = NULL;
    char *buff=NULL;
    int nodesoneachprocess = 0;
    int idx=0,i=0,j=0, l=0;
    int *ranks = NULL;
    double *timestampbuff = NULL;
    OMPI_MPI_OFFSET_TYPE *offsetbuff = NULL;
    int *countbuff = NULL;
    int *displ = NULL;
    double *ind_ts = NULL;
    long *ind_recordlength = NULL;
    OMPI_MPI_OFFSET_TYPE *local_off = NULL;
    int totalnodes = 0;
    ompi_status_public_t status;
    int recordlength=0;

    headnode = (mca_sharedfp_individual_header_record*)sh->selected_module_data;
    if ( NULL == headnode)  {
        opal_output(0, "sharedfp_individual_collaborate_data: headnode is NULL but file is open\n");
        return OMPI_ERROR;
    }

    /* Number of nodes on each process is the sum of records
     * on file and records in the linked list
     */
    nodesoneachprocess = headnode->numofrecordsonfile + headnode->numofrecords;

    if ( mca_sharedfp_individual_verbose ) {
        opal_output(ompi_sharedfp_base_framework.framework_output,
                    "Nodes of each process = %d\n",nodesoneachprocess);
    }

    countbuff = (int*)malloc(ompio_fh->f_size * sizeof(int));
    if ( NULL == countbuff  ) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    displ = (int*)malloc(sizeof(int) * ompio_fh->f_size);
    if ( NULL == displ ) {
        ret = OMPI_ERR_OUT_OF_RESOURCE;
	goto exit;
    }

    /* Each process counts the number of nodes
     * in its linked list for which global offset */
    ret =  mca_sharedfp_individual_get_timestamps_and_reclengths ( &ind_ts, &ind_recordlength,
								   &local_off, sh );
    if ( OMPI_SUCCESS != ret ) {
	goto exit;
    }

    ret = ompio_fh->f_comm->c_coll->coll_allgather ( &nodesoneachprocess, 
                                                     1, 
                                                     MPI_INT,
                                                     countbuff, 
                                                     1, 
                                                     MPI_INT, 
                                                     ompio_fh->f_comm,
                                                     ompio_fh->f_comm->c_coll->coll_allgather_module );

    if ( OMPI_SUCCESS != ret ) {
	goto exit;
    }


    if ( mca_sharedfp_individual_verbose) {
	for (i = 0; i < ompio_fh->f_size ; i++) {
            opal_output(ompi_sharedfp_base_framework.framework_output,"sharedfp_individual_collaborate_data: "
                        "Countbuff[%d] = %d\n", i, countbuff[i]);
	}
    }

    if ( 0 == nodesoneachprocess )    {
        ind_ts[0] = 0;
        ind_recordlength[0] = 0;
        local_off[0] = 0;
    }

    for(i = 0; i < ompio_fh->f_size; i++) {
        displ[i]    = totalnodes;
	if ( mca_sharedfp_individual_verbose ) {
            opal_output(ompi_sharedfp_base_framework.framework_output,
                        "sharedfp_individual_collaborate_data: displ[%d] = %d\n",i,displ[i]);
        }
        totalnodes  = totalnodes + countbuff[i];
    }

    if (totalnodes <= 0 ) {
	goto exit;
    }

    ranks = (int *) malloc ( totalnodes * sizeof(int));
    if ( NULL == ranks ) {
        ret = OMPI_ERR_OUT_OF_RESOURCE;
	goto exit;
    }
    for ( l=0, i=0; i< ompio_fh->f_size; i++ ) {
        for ( j=0; j< countbuff[i]; j++ ) {
            ranks[l++]=i;
        }
    }

    ret =  mca_sharedfp_individual_create_buff ( &timestampbuff, &offsetbuff, totalnodes, ompio_fh->f_size);
    if ( OMPI_SUCCESS != ret ) {
	goto exit;
    }

    ret = ompio_fh->f_comm->c_coll->coll_allgatherv ( ind_ts, 
                                                      countbuff[ompio_fh->f_rank], 
                                                      MPI_DOUBLE,
                                                      timestampbuff, 
                                                      countbuff, 
                                                      displ, 
                                                      MPI_DOUBLE,
                                                      ompio_fh->f_comm, 
                                                      ompio_fh->f_comm->c_coll->coll_allgatherv_module );
    if ( OMPI_SUCCESS != ret ) {
	goto exit;
    }

    ret = ompio_fh->f_comm->c_coll->coll_allgatherv ( ind_recordlength, 
                                                      countbuff[ompio_fh->f_rank], 
                                                      OMPI_OFFSET_DATATYPE,
                                                      offsetbuff, 
                                                      countbuff, 
                                                      displ, 
                                                      OMPI_OFFSET_DATATYPE,
                                                      ompio_fh->f_comm, 
                                                      ompio_fh->f_comm->c_coll->coll_allgatherv_module );
    if ( OMPI_SUCCESS != ret ) {
	goto exit;
    }

    ret =  mca_sharedfp_individual_sort_timestamps(&timestampbuff, &offsetbuff, &ranks, totalnodes);
    if ( OMPI_SUCCESS != ret ) {
	goto exit;
    }

    sh->global_offset = mca_sharedfp_individual_assign_globaloffset ( &offsetbuff, totalnodes, sh);

    recordlength = ind_recordlength[0] * 1.2;
    buff = (char * ) malloc( recordlength );
    if  ( NULL == buff ) {
	ret = OMPI_ERR_OUT_OF_RESOURCE;
	goto exit;
    }

    for (i = 0; i < nodesoneachprocess ; i++)  {
        if ( ind_recordlength[i] > recordlength ) {
            recordlength = ind_recordlength[i] * 1.2;
            buff = (char *) realloc ( buff, recordlength );
            if  ( NULL == buff ) {
                ret = OMPI_ERR_OUT_OF_RESOURCE;
                goto exit;
            }
        }

	/*Read from the local data file*/
	ret = mca_common_ompio_file_read_at ( headnode->datafilehandle,
                                              local_off[i], buff, ind_recordlength[i],
                                              MPI_BYTE, &status);
        if ( OMPI_SUCCESS != ret ) {
            goto exit;
        }

	idx =  mca_sharedfp_individual_getoffset(ind_ts[i],timestampbuff, ranks, ompio_fh->f_rank, totalnodes);

	if ( mca_sharedfp_individual_verbose ) {
            opal_output(ompi_sharedfp_base_framework.framework_output,
                        "sharedfp_individual_collaborate_data: Process %d writing %ld bytes to main file at position"
                        "%lld (%d)\n", ompio_fh->f_rank, ind_recordlength[i], offsetbuff[idx], idx);
        }

	/*Write into main data file*/
	ret = mca_common_ompio_file_write_at( ompio_fh, offsetbuff[idx], buff,
                                              ind_recordlength[i], MPI_BYTE, &status);
        if ( OMPI_SUCCESS != ret ) {
            goto exit;
        }

    }

exit:
    if ( NULL != countbuff ) {
	free ( countbuff );
    }
    if ( NULL != displ ) {
	free ( displ );
    }

    if( NULL != timestampbuff ){
        free ( timestampbuff );
    }
    if ( NULL != offsetbuff ){
        free ( offsetbuff );
    }
    if ( NULL != ind_ts ) {
	free ( ind_ts );
    }
    if ( NULL != ind_recordlength ) {
	free ( ind_recordlength );
    }
    if ( NULL != local_off ) {
	free ( local_off );
    }
    if ( NULL != buff ) {
	free ( buff );
    }
    if ( NULL != ranks ) {
        free ( ranks );
    }

    return ret;
}

/* Count the number of nodes and create and array of the timestamps*/
int  mca_sharedfp_individual_get_timestamps_and_reclengths ( double **buff, long **rec_length, 
                                                             MPI_Offset **offbuff,struct mca_sharedfp_base_data_t *sh)
{
    int num = 0, i= 0, ctr = 0;
    int ret=OMPI_SUCCESS;
    mca_sharedfp_individual_metadata_node *currnode;
    mca_sharedfp_individual_header_record *headnode;
    OMPI_MPI_OFFSET_TYPE metaoffset = 0;
    struct  mca_sharedfp_individual_record2 rec;
    MPI_Status status;

    headnode = (mca_sharedfp_individual_header_record*)(sh->selected_module_data);
    num = ( headnode->numofrecords + headnode->numofrecordsonfile);
    currnode = headnode->next;

    if ( mca_sharedfp_individual_verbose ) {
        opal_output(ompi_sharedfp_base_framework.framework_output,"Num is %d\n",num);
    }

    if ( 0 == num )   {
        *buff       = (double*) malloc ( sizeof ( double ));
        *rec_length = (long *) malloc ( sizeof ( long ));
        *offbuff    = (OMPI_MPI_OFFSET_TYPE *)malloc ( sizeof(OMPI_MPI_OFFSET_TYPE) );
	if ( NULL == *buff || NULL == *rec_length || NULL == *offbuff ) {
	    ret = OMPI_ERR_OUT_OF_RESOURCE;
	    goto exit;
	}
    }
    else {
        *buff       = (double* ) malloc(sizeof ( double) * num);
        *rec_length = (long *) malloc(sizeof ( long) * num);
        *offbuff    = (OMPI_MPI_OFFSET_TYPE *) malloc ( sizeof(OMPI_MPI_OFFSET_TYPE) * num);
	if ( NULL == *buff || NULL == *rec_length || NULL == *offbuff ) {
	    ret = OMPI_ERR_OUT_OF_RESOURCE;
	    goto exit;
	}
    }

    if ( mca_sharedfp_individual_verbose ) {
        opal_output(ompi_sharedfp_base_framework.framework_output,
                    "sharedfp_individual_get_timestamps_and_reclengths: Numofrecords on file %d\n",
                    headnode->numofrecordsonfile);
    }

    if (headnode->numofrecordsonfile >  0)  {
        metaoffset = headnode->metafile_start_offset;
        ctr = 0;
        for (i = 0; i < headnode->numofrecordsonfile ; i++)  {

            ret = mca_common_ompio_file_read_at(headnode->metadatafilehandle,metaoffset, 
                                                &rec, 32, MPI_BYTE,&status);
            if ( OMPI_SUCCESS != ret ) {
                goto exit;
            }

            *(*rec_length + ctr) = rec.recordlength;
            *(*buff + ctr) = rec.timestamp;
            *(*offbuff + ctr) = rec.localposition;

            metaoffset = metaoffset +  sizeof(struct  mca_sharedfp_individual_record2);

            if ( mca_sharedfp_individual_verbose ) {
                opal_output(ompi_sharedfp_base_framework.framework_output,
                            "sharedfp_individual_get_timestamps_and_reclengths: Ctr = %d\n",ctr);
            }
            ctr++;
        }

        headnode->numofrecordsonfile = 0;
        headnode->metafile_start_offset = metaoffset;

    }	/* End of if (headnode->numofrecordsonfile > 0) */

    /* Add the records from the linked list */
    currnode = headnode->next;
    while (currnode)  {
        if ( mca_sharedfp_individual_verbose ) {
            opal_output(ompi_sharedfp_base_framework.framework_output,"Ctr = %d\n",ctr);
        }
        /* Some error over here..need to check this code again */
        /*while(headnode->next  != NULL)*/

        *(*rec_length + ctr) = currnode->recordlength;
        *(*buff + ctr) = currnode->timestamp;
        *(*offbuff + ctr) = currnode->localposition;

        ctr = ctr + 1;

        headnode->next = currnode->next;
        if ( mca_sharedfp_individual_verbose ) {
            opal_output(ompi_sharedfp_base_framework.framework_output,
                        "sharedfp_individual_get_timestamps_and_reclengths: node deleted from the metadatalinked list\n");
        }
        free(currnode);
        currnode = headnode->next;

    }	/*End of while(currnode) loop*/


    /*Reset the numofrecords*/
    headnode->numofrecords = 0;

exit:

    return ret;
}

int  mca_sharedfp_individual_create_buff(double **ts,MPI_Offset **off,int totalnodes, int size)
{

    if ( totalnodes)  {
        *off = (OMPI_MPI_OFFSET_TYPE *) malloc ( totalnodes * sizeof(OMPI_MPI_OFFSET_TYPE));
        if ( NULL == *off ) {
	    return OMPI_ERR_OUT_OF_RESOURCE;
	}

        *ts = (double *) malloc ( totalnodes * sizeof(double) );
        if (NULL == *ts ) {
            return OMPI_ERR_OUT_OF_RESOURCE;
	}

    }

    return OMPI_SUCCESS;
}

/*Sort the timestamp buffer*/
int  mca_sharedfp_individual_sort_timestamps(double **ts, MPI_Offset **off, int **ranks, int totalnodes)
{

    int i = 0;
    int j = 0;
    int flag = 1;
    double tempts = 0.0;
    OMPI_MPI_OFFSET_TYPE tempoffset = 0;
    int temprank = 0;

    for (i= 1; (i <= totalnodes)&&(flag) ; i++)  {
        flag = 0;
        for (j = 0; j < (totalnodes - 1); j++)  {
            if ( *(*ts + j + 1) < *(*ts + j ))  {
                /*swap timestamp*/
                tempts = *(*ts + j );
                *(*ts + j) = *(*ts + j + 1);
                *(*ts + j + 1) = tempts;

                /*swap offset*/
                tempoffset = *(*off + j);
                *(*off + j) = *(*off + j + 1);
                *(*off + j + 1) = tempoffset;

                /*swap ranks*/
                temprank = *(*ranks + j);
                *(*ranks + j) = *(*ranks + j + 1);
                *(*ranks + j + 1) = temprank;

                flag = 1;
            }
        }

    }

    return OMPI_SUCCESS;
}


MPI_Offset  mca_sharedfp_individual_assign_globaloffset(MPI_Offset **offsetbuff,int totalnodes,
                                                        struct mca_sharedfp_base_data_t *sh)
{
    int i = 0;
    OMPI_MPI_OFFSET_TYPE temp = 0,prevoffset = 0;
    OMPI_MPI_OFFSET_TYPE global_offset = 0;

    for (i = 0; i < totalnodes; i++) {
        temp = *(*offsetbuff + i);

        if (i == 0) {
            *(*offsetbuff + i ) = sh->global_offset;
	}
        else {
            *(*offsetbuff + i) = *(*offsetbuff + i - 1) + prevoffset;
	}
        prevoffset = temp;
    }
    global_offset =   *(*offsetbuff + i - 1) + prevoffset;

    return global_offset;
}


int  mca_sharedfp_individual_getoffset(double timestamp, double *ts, int *ranks, int myrank, int totalnodes)
{
    int i = 0;
    int notfound = 1;


    while (notfound) {
        if (ts[i] == timestamp && ranks[i] == myrank )
            break;

        i++;

        if (i == totalnodes)  {
            notfound = 0;
        }
    }

    if (!notfound) {
        return -1;
    }

    return i;
}
