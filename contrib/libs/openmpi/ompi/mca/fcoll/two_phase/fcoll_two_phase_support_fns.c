/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2011 University of Houston. All rights reserved.
 * Copyright (c) 2014-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "fcoll_two_phase.h"

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/mca/fcoll/fcoll.h"
#include "ompi/mca/common/ompio/common_ompio.h"
#include "ompi/mca/io/io.h"
#include "opal/mca/base/base.h"
#include "math.h"
#include "ompi/mca/pml/pml.h"
#include <unistd.h>

/*Based on ROMIO's domain partitioning implementaion
Series of functions implementations for two-phase implementation
Functions to support Domain partitioning and aggregator
selection for two_phase .
This is commom to both two_phase_read and write. */

int mca_fcoll_two_phase_domain_partition (ompio_file_t *fh,
					  OMPI_MPI_OFFSET_TYPE *start_offsets,
					  OMPI_MPI_OFFSET_TYPE *end_offsets,
					  OMPI_MPI_OFFSET_TYPE *min_st_offset_ptr,
					  OMPI_MPI_OFFSET_TYPE **fd_st_ptr,
					  OMPI_MPI_OFFSET_TYPE **fd_end_ptr,
					  int min_fd_size,
					  OMPI_MPI_OFFSET_TYPE *fd_size_ptr,
					  int striping_unit,
					  int nprocs_for_coll){

    OMPI_MPI_OFFSET_TYPE min_st_offset, max_end_offset, *fd_start=NULL, *fd_end=NULL, fd_size;
    int i;

    min_st_offset = start_offsets[0];
    max_end_offset = end_offsets[0];

    for (i=0; i< fh->f_size; i++){
	min_st_offset = OMPIO_MIN(min_st_offset, start_offsets[i]);
	max_end_offset = OMPIO_MAX(max_end_offset, end_offsets[i]);

    }

    fd_size = ((max_end_offset - min_st_offset + 1) + nprocs_for_coll - 1)/nprocs_for_coll;

    if (fd_size < min_fd_size)
	fd_size = min_fd_size;

    *fd_st_ptr = (OMPI_MPI_OFFSET_TYPE *)
	malloc(nprocs_for_coll*sizeof(OMPI_MPI_OFFSET_TYPE));

    if ( NULL == *fd_st_ptr ) {
	return OMPI_ERR_OUT_OF_RESOURCE;
    }

    *fd_end_ptr = (OMPI_MPI_OFFSET_TYPE *)
	malloc(nprocs_for_coll*sizeof(OMPI_MPI_OFFSET_TYPE));

    if ( NULL == *fd_end_ptr ) {
	return OMPI_ERR_OUT_OF_RESOURCE;
    }


    fd_start = *fd_st_ptr;
    fd_end = *fd_end_ptr;


    if (striping_unit > 0){
      /* Lock Boundary based domain partitioning */
	int rem_front, rem_back;
	OMPI_MPI_OFFSET_TYPE end_off;

	fd_start[0] = min_st_offset;
        end_off     = fd_start[0] + fd_size;
        rem_front   = end_off % striping_unit;
        rem_back    = striping_unit - rem_front;
        if (rem_front < rem_back)
		end_off -= rem_front;
        else
		end_off += rem_back;
        fd_end[0] = end_off - 1;

	/* align fd_end[i] to the nearest file lock boundary */
        for (i=1; i<nprocs_for_coll; i++) {
            fd_start[i] = fd_end[i-1] + 1;
            end_off     = min_st_offset + fd_size * (i+1);
            rem_front   = end_off % striping_unit;
            rem_back    = striping_unit - rem_front;
            if (rem_front < rem_back)
		    end_off -= rem_front;
            else
		    end_off += rem_back;
            fd_end[i] = end_off - 1;
        }
        fd_end[nprocs_for_coll-1] = max_end_offset;
    }
    else{
	fd_start[0] = min_st_offset;
        fd_end[0] = min_st_offset + fd_size - 1;

        for (i=1; i<nprocs_for_coll; i++) {
            fd_start[i] = fd_end[i-1] + 1;
            fd_end[i] = fd_start[i] + fd_size - 1;
        }

    }

    for (i=0; i<nprocs_for_coll; i++) {
	if (fd_start[i] > max_end_offset)
	    fd_start[i] = fd_end[i] = -1;
	if (fd_end[i] > max_end_offset)
	    fd_end[i] = max_end_offset;
    }

    *fd_size_ptr = fd_size;
    *min_st_offset_ptr = min_st_offset;

    return OMPI_SUCCESS;
}



int mca_fcoll_two_phase_calc_aggregator(ompio_file_t *fh,
					OMPI_MPI_OFFSET_TYPE off,
					OMPI_MPI_OFFSET_TYPE min_off,
					OMPI_MPI_OFFSET_TYPE *len,
					OMPI_MPI_OFFSET_TYPE fd_size,
					OMPI_MPI_OFFSET_TYPE *fd_start,
					OMPI_MPI_OFFSET_TYPE *fd_end,
					int striping_unit,
					int num_aggregators,
					int *aggregator_list)
{


    int rank_index, rank;
    OMPI_MPI_OFFSET_TYPE avail_bytes;

    rank_index = (int) ((off - min_off + fd_size)/ fd_size - 1);

    if (striping_unit > 0){
	rank_index = 0;
	while (off > fd_end[rank_index]) rank_index++;
    }


    if (rank_index >= num_aggregators || rank_index < 0) {
       fprintf(stderr,
	       "Error in ompi_io_ompio_calcl_aggregator():");
       fprintf(stderr,
	       "rank_index(%d) >= num_aggregators(%d)fd_size=%lld off=%lld\n",
	       rank_index,num_aggregators,fd_size,off);
       ompi_mpi_abort(&ompi_mpi_comm_world.comm, 1);
    }


    avail_bytes = fd_end[rank_index] + 1 - off;
    if (avail_bytes < *len){
	*len = avail_bytes;
    }

    rank = aggregator_list[rank_index];

    #if 0
    printf("rank : %d, rank_index : %d\n",rank, rank_index);
    #endif

    return rank;
}

int mca_fcoll_two_phase_calc_others_requests(ompio_file_t *fh,
					     int count_my_req_procs,
					     int *count_my_req_per_proc,
					     mca_common_ompio_access_array_t *my_req,
					     int *count_others_req_procs_ptr,
					     mca_common_ompio_access_array_t **others_req_ptr)
{


    int *count_others_req_per_proc=NULL, count_others_req_procs;
    int i,j, ret=OMPI_SUCCESS;
    MPI_Request *requests=NULL;
    mca_common_ompio_access_array_t *others_req=NULL;

    count_others_req_per_proc = (int *)malloc(fh->f_size*sizeof(int));

    if ( NULL == count_others_req_per_proc ) {
	return OMPI_ERR_OUT_OF_RESOURCE;
    }

    /* Change it to the ompio specific alltoall in coll module : VVN*/
    ret =  fh->f_comm->c_coll->coll_alltoall (count_my_req_per_proc,
					     1,
					     MPI_INT,
					     count_others_req_per_proc,
					     1,
					     MPI_INT,
					     fh->f_comm,
					     fh->f_comm->c_coll->coll_alltoall_module);
    if ( OMPI_SUCCESS != ret ) {
	return ret;
    }

#if 0
    for( i = 0; i< fh->f_size; i++){
	printf("my: %d, others: %d\n",count_my_req_per_proc[i],
	       count_others_req_per_proc[i]);

    }
#endif

    *others_req_ptr = (mca_common_ompio_access_array_t  *) malloc
	(fh->f_size*sizeof(mca_common_ompio_access_array_t));
    others_req = *others_req_ptr;

    count_others_req_procs = 0;
    for (i=0; i<fh->f_size; i++) {
	if (count_others_req_per_proc[i]) {
	    others_req[i].count = count_others_req_per_proc[i];
	    others_req[i].offsets = (OMPI_MPI_OFFSET_TYPE *)
		malloc(count_others_req_per_proc[i]*sizeof(OMPI_MPI_OFFSET_TYPE));
	    others_req[i].lens = (int *)
		malloc(count_others_req_per_proc[i]*sizeof(int));
	    others_req[i].mem_ptrs = (MPI_Aint *)
		malloc(count_others_req_per_proc[i]*sizeof(MPI_Aint));
	    count_others_req_procs++;
	}
	else
	    others_req[i].count = 0;
    }


    requests = (MPI_Request *)
	malloc(1+2*(count_my_req_procs+count_others_req_procs)*
	       sizeof(MPI_Request));

    if ( NULL == requests ) {
	ret = OMPI_ERR_OUT_OF_RESOURCE;
	goto exit;
    }

    j = 0;
    for (i=0; i<fh->f_size; i++){
	if (others_req[i].count){
            ret = MCA_PML_CALL(irecv(others_req[i].offsets,
				     others_req[i].count,
				     OMPI_OFFSET_DATATYPE,
				     i,
				     i+fh->f_rank,
				     fh->f_comm,
				     &requests[j]));
	    if ( OMPI_SUCCESS != ret  ) {
		goto exit;
	    }

	    j++;

	    ret = MCA_PML_CALL(irecv(others_req[i].lens,
				     others_req[i].count,
				     MPI_INT,
				     i,
				     i+fh->f_rank+1,
				     fh->f_comm,
				     &requests[j]));
	    if ( OMPI_SUCCESS != ret  ) {
		goto exit;
	    }

	    j++;
	}
    }


    for (i=0; i < fh->f_size; i++) {
	if (my_req[i].count) {
	    ret = MCA_PML_CALL(isend(my_req[i].offsets,
				     my_req[i].count,
				     OMPI_OFFSET_DATATYPE,
				     i,
				     i+fh->f_rank,
				     MCA_PML_BASE_SEND_STANDARD,
				     fh->f_comm,
				     &requests[j]));
	    if ( OMPI_SUCCESS != ret  ) {
		goto exit;
	    }

	    j++;
	    ret = MCA_PML_CALL(isend(my_req[i].lens,
				     my_req[i].count,
				     MPI_INT,
				     i,
				     i+fh->f_rank+1,
				     MCA_PML_BASE_SEND_STANDARD,
				     fh->f_comm,
				     &requests[j]));
	    if ( OMPI_SUCCESS != ret  ) {
		goto exit;
	    }

	    j++;
	}
    }

    if (j) {
	ret = ompi_request_wait_all ( j, requests, MPI_STATUSES_IGNORE );
	if ( OMPI_SUCCESS != ret  ) {
	    return ret;
	}
    }

    *count_others_req_procs_ptr = count_others_req_procs;

exit:
    if ( NULL != requests ) {
	free(requests);
    }
    if ( NULL != count_others_req_per_proc ) {
	free(count_others_req_per_proc);
    }


    return ret;
}


int mca_fcoll_two_phase_calc_my_requests (ompio_file_t *fh,
					  struct iovec *offset_len,
					  int contig_access_count,
					  OMPI_MPI_OFFSET_TYPE min_st_offset,
					  OMPI_MPI_OFFSET_TYPE *fd_start,
					  OMPI_MPI_OFFSET_TYPE *fd_end,
					  OMPI_MPI_OFFSET_TYPE fd_size,
					  int *count_my_req_procs_ptr,
					  int **count_my_req_per_proc_ptr,
					  mca_common_ompio_access_array_t **my_req_ptr,
					  size_t **buf_indices,
					  int striping_unit,
					  int num_aggregators,
					  int *aggregator_list)
{
    int ret = MPI_SUCCESS;
    int *count_my_req_per_proc, count_my_req_procs;
    size_t *buf_idx = NULL;
    int i, l, proc;
    OMPI_MPI_OFFSET_TYPE fd_len, rem_len, curr_idx, off;
    mca_common_ompio_access_array_t *my_req = NULL;


    *count_my_req_per_proc_ptr = (int*)malloc(fh->f_size*sizeof(int));

    if ( NULL == *count_my_req_per_proc_ptr ){
	return OMPI_ERR_OUT_OF_RESOURCE;
    }

    count_my_req_per_proc = *count_my_req_per_proc_ptr;

    for (i=0;i<fh->f_size;i++){
	count_my_req_per_proc[i] = 0;
    }

    buf_idx = (size_t *) malloc (fh->f_size * sizeof(size_t));

    if ( NULL == buf_idx ){
	return OMPI_ERR_OUT_OF_RESOURCE;
    }

    for (i=0; i < fh->f_size; i++) buf_idx[i] = -1;

    for (i=0;i<contig_access_count; i++){

	if (offset_len[i].iov_len==0)
	    continue;
	off = (OMPI_MPI_OFFSET_TYPE)(intptr_t)offset_len[i].iov_base;
	fd_len = (OMPI_MPI_OFFSET_TYPE)offset_len[i].iov_len;
	proc = mca_fcoll_two_phase_calc_aggregator(fh, off, min_st_offset, &fd_len, fd_size,
					     fd_start, fd_end, striping_unit, num_aggregators,aggregator_list);
	count_my_req_per_proc[proc]++;
	rem_len = offset_len[i].iov_len - fd_len;

	while (rem_len != 0) {
	    off += fd_len; /* point to first remaining byte */
	    fd_len = rem_len; /* save remaining size, pass to calc */
	    proc = mca_fcoll_two_phase_calc_aggregator(fh, off, min_st_offset, &fd_len,
						 fd_size, fd_start, fd_end, striping_unit,
						 num_aggregators, aggregator_list);

	    count_my_req_per_proc[proc]++;
	    rem_len -= fd_len; /* reduce remaining length by amount from fd */
	}

    }

/*    printf("%d: fh->f_size : %d\n", fh->f_rank,fh->f_size);*/
    *my_req_ptr =  (mca_common_ompio_access_array_t *)
	malloc (fh->f_size * sizeof(mca_common_ompio_access_array_t));
    if ( NULL == *my_req_ptr ) {
        ret = OMPI_ERR_OUT_OF_RESOURCE;
        goto err_exit;
    }
    my_req = *my_req_ptr;

    count_my_req_procs = 0;
    for (i = 0; i < fh->f_size; i++){
	if(count_my_req_per_proc[i]) {
	    my_req[i].offsets = (OMPI_MPI_OFFSET_TYPE *)
		malloc(count_my_req_per_proc[i] * sizeof(OMPI_MPI_OFFSET_TYPE));

	    if ( NULL == my_req[i].offsets ) {
	        ret =  OMPI_ERR_OUT_OF_RESOURCE;
                goto err_exit;
	    }

	    my_req[i].lens = (int *)
		malloc(count_my_req_per_proc[i] * sizeof(int));

	    if ( NULL == my_req[i].lens ) {
	        ret =  OMPI_ERR_OUT_OF_RESOURCE;
                goto err_exit;
	    }
	    count_my_req_procs++;
	}
	my_req[i].count = 0;
    }
    curr_idx = 0;
    for (i=0; i<contig_access_count; i++) {
	if ((int)offset_len[i].iov_len == 0)
	    continue;
	off = (OMPI_MPI_OFFSET_TYPE)(intptr_t)offset_len[i].iov_base;
	fd_len = (OMPI_MPI_OFFSET_TYPE)offset_len[i].iov_len;
 	proc = mca_fcoll_two_phase_calc_aggregator(fh, off, min_st_offset, &fd_len,
					     fd_size, fd_start, fd_end,
					     striping_unit, num_aggregators,
					     aggregator_list);
	if (buf_idx[proc] == (size_t) -1){
	    buf_idx[proc] = (int) curr_idx;
	}
	l = my_req[proc].count;
	curr_idx += fd_len;
	rem_len = offset_len[i].iov_len - fd_len;
	my_req[proc].offsets[l] = off;
	my_req[proc].lens[l] = (int)fd_len;
	my_req[proc].count++;

	while (rem_len != 0) {
	    off += fd_len;
	    fd_len = rem_len;
	    proc = mca_fcoll_two_phase_calc_aggregator(fh, off, min_st_offset,
						       &fd_len, fd_size, fd_start,
						       fd_end, striping_unit,
						       num_aggregators,
						       aggregator_list);

	    if (buf_idx[proc] == (size_t) -1){
		buf_idx[proc] = (int) curr_idx;
	    }

	    l = my_req[proc].count;
	    curr_idx += fd_len;
	    rem_len -= fd_len;

	    my_req[proc].offsets[l] = off;
	    my_req[proc].lens[l] = (int) fd_len;
	    my_req[proc].count++;

	}

    }

  #if 0
    for (i=0; i<fh->f_size; i++) {
	if (count_my_req_per_proc[i] > 0) {
	    fprintf(stdout, "data needed from %d (count = %d):\n", i,
		    my_req[i].count);
	    for (l=0; l < my_req[i].count; l++) {
		fprintf(stdout, " %d: off[%d] = %lld, len[%d] = %d\n", fh->f_rank, l,
		my_req[i].offsets[l], l, my_req[i].lens[l]);
	    }
	    fprintf(stdout, "%d: buf_idx[%d] = 0x%x\n", fh->f_rank, i, buf_idx[i]);
	}
    }
#endif


    *count_my_req_procs_ptr = count_my_req_procs;
    *buf_indices = buf_idx;

    return ret;
err_exit:
    if (NULL != my_req) {
        for (i = 0; i < fh->f_size; i++) {
            if (NULL != my_req[i].offsets) {
                free(my_req[i].offsets);
            }
            if (NULL != my_req[i].lens) {
                free(my_req[i].lens);
            }
        }
    }
    if (NULL != buf_idx) {
        free(buf_idx);
    }
    return ret;
}
/*Two-phase support functions ends here!*/
