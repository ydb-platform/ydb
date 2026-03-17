/* -*- Mode: C; c-basic-offset:4 ; -*- */
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
 * Copyright (c) 2008-2016 University of Houston. All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "ompi/communicator/communicator.h"
#include "ompi/datatype/ompi_datatype.h"

#include "ompi/mca/common/ompio/common_ompio.h"


/* Print queue related function implementations */
int mca_common_ompio_initialize_print_queue( struct mca_common_ompio_print_queue  **r){

    struct mca_common_ompio_print_queue *q=NULL;
    int ret = OMPI_SUCCESS;

    q = (struct mca_common_ompio_print_queue *) malloc ( sizeof(mca_common_ompio_print_queue));
    if ( NULL == q ) {
        ret = OMPI_ERR_OUT_OF_RESOURCE;
    }
    q->first = 0;
    q->last = MCA_COMMON_OMPIO_QUEUESIZE - 1;
    q->count = 0;
    
    *r = q;
    return ret;
}

int mca_common_ompio_register_print_entry ( struct mca_common_ompio_print_queue *q,
                                            mca_common_ompio_print_entry x)
{
    if (q->count >= MCA_COMMON_OMPIO_QUEUESIZE){
        return OMPI_ERROR;
    }
    else{
        q->last = (q->last + 1) % MCA_COMMON_OMPIO_QUEUESIZE;
        q->entry[q->last] = x;
        q->count = q->count + 1;
    }

    return OMPI_SUCCESS;
}

int  mca_common_ompio_unregister_print_entry ( struct mca_common_ompio_print_queue *q,
                                               mca_common_ompio_print_entry *x)
{

    if (q->count <= 0){
        return OMPI_ERROR;
    }
    else{
        *x = q->entry[q->first];
        q->first = (q->first+1) % MCA_COMMON_OMPIO_QUEUESIZE;
        q->count = q->count - 1;
    }

    return OMPI_SUCCESS;
}

int mca_common_ompio_empty_print_queue( struct mca_common_ompio_print_queue *q)
{
    if (q->count == 0) {
        return 1;
    }
    
    return 0;
}

int mca_common_ompio_full_print_queue( struct mca_common_ompio_print_queue *q)
{
    if (q->count < MCA_COMMON_OMPIO_QUEUESIZE) {
        return 0;
    }

    return 1;
}


int mca_common_ompio_print_time_info( struct mca_common_ompio_print_queue *q,
                                      char *name,
                                      struct ompio_file_t *fh){

    int i = 0, j=0, nprocs_for_coll = 0, ret = OMPI_SUCCESS, count = 0;
    double *time_details = NULL, *final_sum = NULL;
    double *final_max = NULL, *final_min = NULL;
    double *final_time_details=NULL;

    nprocs_for_coll = q->entry[0].nprocs_for_coll;
    time_details = (double *) calloc (4,sizeof(double));
    if ( NULL == time_details){
	ret = OMPI_ERR_OUT_OF_RESOURCE;
	goto exit;

    }

    if (!fh->f_rank){

	final_min = (double *) malloc (3*sizeof(double));
	if ( NULL == final_min){
	    ret = OMPI_ERR_OUT_OF_RESOURCE;
	    goto exit;
	}

	final_max = (double *) malloc (3*sizeof(double));
	if ( NULL == final_max){
	    ret = OMPI_ERR_OUT_OF_RESOURCE;
	    goto exit;

	}

	final_sum = (double *) malloc (3*sizeof(double));
	if ( NULL == final_sum){
	    ret = OMPI_ERR_OUT_OF_RESOURCE;
	    goto exit;
	}

	final_time_details = (double *)calloc (fh->f_size, 4 * sizeof(double));
	if (NULL == final_time_details){
	    ret = OMPI_ERR_OUT_OF_RESOURCE;
	    goto exit;
	}

	count = 4 * fh->f_size;
    }

    if (q->count > 0){
	for (i=0; i < q->count; i++){
	    for (j=0;j<3;j++){
		if (!fh->f_rank){
		    final_min[j] = 100000.0;
		    final_max[j] = 0.0;
		    final_sum[j] = 0.0;
		}
		time_details[j] += q->entry[i].time[j];
	    }
	    time_details[3]  = q->entry[i].aggregator;
	}
    }

    ret = fh->f_comm->c_coll->coll_gather(time_details,
                                         4,
                                         MPI_DOUBLE,
                                         final_time_details,
                                         4,
                                         MPI_DOUBLE,
                                         0,
                                         fh->f_comm,
                                         fh->f_comm->c_coll->coll_gather_module);
    
    if ( OMPI_SUCCESS != ret ) {
    }

    if (!fh->f_rank){

	for (i=0;i<count;i+=4){
	    if (final_time_details[i+3] == 1){
		final_sum[0] += final_time_details[i];
		final_sum[1] += final_time_details[i+1];
		final_sum[2] += final_time_details[i+2];

		if ( final_time_details[i] < final_min[0])
		    final_min[0] = final_time_details[i];
		if ( final_time_details[i+1] < final_min[1])
		    final_min[1] = final_time_details[i+1];
		if ( final_time_details[i+2] < final_min[2])
		    final_min[2] = final_time_details[i+2];



		if ( final_time_details[i] > final_max[0])
		    final_max[0] = final_time_details[i];
		if ( final_time_details[i+1] > final_max[1])
		    final_max[1] = final_time_details[i+1];
		if ( final_time_details[i+2] > final_max[2])
		    final_max[2] = final_time_details[i+2];

	    }
	}

	printf ("\n# MAX-%s AVG-%s MIN-%s MAX-COMM AVG-COMM MIN-COMM",
		name, name, name);
	printf (" MAX-EXCH AVG-EXCH MIN-EXCH\n");
	printf (" %f %f %f %f %f %f %f %f %f\n\n",
		final_max[0], final_sum[0]/nprocs_for_coll, final_min[0],
		final_max[1], final_sum[1]/nprocs_for_coll, final_min[1],
		final_max[2], final_sum[2]/nprocs_for_coll, final_min[2]);

    }

 exit:
    if ( NULL != final_max){
	free(final_max);
	final_max = NULL;
    }
    if (NULL != final_min){
	free(final_min);
	final_min = NULL;
    }
    if (NULL != final_sum){
	free(final_sum);
	final_sum = NULL;
    }
    if (NULL != time_details){
	free(time_details);
	time_details = NULL;
    }

    return ret;
}

