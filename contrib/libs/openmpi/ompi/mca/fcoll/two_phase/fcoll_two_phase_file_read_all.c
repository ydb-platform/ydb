/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
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
 * Copyright (c) 2008-2014 University of Houston. All rights reserved.
 * Copyright (c) 2015      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2017-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
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
#include "ompi/communicator/communicator.h"
#include "ompi/mca/fcoll/fcoll.h"
#include "ompi/mca/common/ompio/common_ompio.h"
#include "ompi/mca/io/io.h"
#include "opal/mca/base/base.h"
#include "math.h"
#include "ompi/mca/pml/pml.h"
#include <unistd.h>

#define DEBUG 0

/* Two Phase implementation from ROMIO ported to OMPIO infrastructure
 * This is pretty much the same as ROMIO's two_phase and based on ROMIO's code
 * base
 */


/* Datastructure to support specifying the flat-list. */
typedef struct flat_list_node {
    MPI_Datatype type;
    int count;
    OMPI_MPI_OFFSET_TYPE *blocklens;
    OMPI_MPI_OFFSET_TYPE *indices;
    struct flat_list_node *next;
}Flatlist_node;

/* local function declarations  */
static int two_phase_read_and_exch(ompio_file_t *fh,
				   void *buf,
				   MPI_Datatype datatype,
				   mca_common_ompio_access_array_t *others_req,
				   struct iovec *offset_len,
				   int contig_access_count,
				   OMPI_MPI_OFFSET_TYPE min_st_offset,
				   OMPI_MPI_OFFSET_TYPE fd_size,
				   OMPI_MPI_OFFSET_TYPE *fd_start,
				   OMPI_MPI_OFFSET_TYPE *fd_end,
				   Flatlist_node *flat_buf,
				   size_t *buf_idx, int striping_unit,
				   int num_io_procs, int *aggregator_list);

static int  two_phase_exchange_data(ompio_file_t *fh,
				    void *buf,
				    struct iovec *offset_length,
				    int *send_size, int *start_pos,
				    int *recv_size,
				    int *count,
				    int *partial_send, int *recd_from_proc,
				    int contig_access_count,
				    OMPI_MPI_OFFSET_TYPE min_st_offset,
				    OMPI_MPI_OFFSET_TYPE fd_size,
				    OMPI_MPI_OFFSET_TYPE *fd_start,
				    OMPI_MPI_OFFSET_TYPE *fd_end,
				    Flatlist_node *flat_buf,
				    mca_common_ompio_access_array_t *others_req,
				    int iter,
				    size_t *buf_idx, MPI_Aint buftype_extent,
				    int striping_unit, int num_io_procs,
				    int *aggregator_list);


static void two_phase_fill_user_buffer(ompio_file_t *fh,
				       void *buf,
				       Flatlist_node *flat_buf,
				       char **recv_buf,
				       struct iovec *offset_length,
				       unsigned *recv_size,
				       MPI_Request *requests,
				       int *recd_from_proc,
				       int contig_access_count,
				       OMPI_MPI_OFFSET_TYPE min_st_offset,
				       OMPI_MPI_OFFSET_TYPE fd_size,
				       OMPI_MPI_OFFSET_TYPE *fd_start,
				       OMPI_MPI_OFFSET_TYPE *fd_end,
				       MPI_Aint buftype_extent,
				       int striping_unit,
				       int num_io_procs, int *aggregator_list);
#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
static int isread_aggregator(int rank,
			     int nprocs_for_coll,
			     int *aggregator_list);

#endif

#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
double read_time = 0.0, start_read_time = 0.0, end_read_time = 0.0;
double rcomm_time = 0.0, start_rcomm_time = 0.0, end_rcomm_time = 0.0;
double read_exch = 0.0, start_rexch = 0.0, end_rexch = 0.0;
#endif


int
mca_fcoll_two_phase_file_read_all (ompio_file_t *fh,
				   void *buf,
				   int count,
				   struct ompi_datatype_t *datatype,
				   ompi_status_public_t *status)
{

    int ret = OMPI_SUCCESS, i = 0, j = 0, interleave_count = 0, striping_unit = 0;
    MPI_Aint recv_buf_addr = 0;
    uint32_t iov_count = 0, ti = 0;
    struct iovec *decoded_iov = NULL, *temp_iov = NULL, *iov = NULL;
    size_t max_data = 0;
    long long_max_data = 0, long_total_bytes = 0;
    int domain_size=0, *count_my_req_per_proc=NULL, count_my_req_procs = 0;
    int count_other_req_procs;
    size_t *buf_indices=NULL;
    int *aggregator_list = NULL, local_count = 0, local_size = 0;
    int two_phase_num_io_procs=1;
    OMPI_MPI_OFFSET_TYPE start_offset = 0, end_offset = 0, fd_size = 0;
    OMPI_MPI_OFFSET_TYPE *start_offsets=NULL, *end_offsets=NULL;
    OMPI_MPI_OFFSET_TYPE *fd_start=NULL, *fd_end=NULL, min_st_offset = 0;
    Flatlist_node *flat_buf=NULL;
    mca_common_ompio_access_array_t *my_req=NULL, *others_req=NULL;
#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
    mca_common_ompio_print_entry nentry;
#endif
//    if (opal_datatype_is_predefined(&datatype->super)) {
//	fh->f_flags = fh->f_flags |  OMPIO_CONTIGUOUS_MEMORY;
//    }

    if (! (fh->f_flags & OMPIO_CONTIGUOUS_MEMORY)) {
	ret =   mca_common_ompio_decode_datatype ((struct ompio_file_t *)fh,
				                  datatype,
				                  count,
				                  buf,
				                  &max_data,
				                  &temp_iov,
				                  &iov_count);
	if (OMPI_SUCCESS != ret ){
	    goto exit;
	}

	recv_buf_addr = (size_t)(buf);
	decoded_iov  = (struct iovec *) calloc
	    (iov_count, sizeof(struct iovec));

	for (ti = 0; ti < iov_count; ti++){

	    decoded_iov[ti].iov_base = (IOVBASE_TYPE *)
		((ptrdiff_t)temp_iov[ti].iov_base - recv_buf_addr);
	    decoded_iov[ti].iov_len = temp_iov[ti].iov_len;
#if DEBUG
	    printf("d_offset[%d]: %ld, d_len[%d]: %ld\n",
		   ti, (ptrdiff_t)decoded_iov[ti].iov_base,
		   ti, decoded_iov[ti].iov_len);
#endif
	}

    }
    else{
	max_data = count * datatype->super.size;
    }

    if ( MPI_STATUS_IGNORE != status ) {
	status->_ucount = max_data;
    }

    two_phase_num_io_procs = fh->f_get_mca_parameter_value ( "num_aggregators", strlen ("num_aggregators"));
    if ( OMPI_ERR_MAX == two_phase_num_io_procs ) {
        ret = OMPI_ERROR;
        goto exit;
    }
    if (-1 == two_phase_num_io_procs ){
	ret = mca_common_ompio_set_aggregator_props ((struct ompio_file_t *)fh,
					             two_phase_num_io_procs,
					             max_data);
	if (OMPI_SUCCESS != ret){
            goto exit;
	}

	two_phase_num_io_procs = fh->f_num_aggrs;

    }

    if (two_phase_num_io_procs > fh->f_size){
        two_phase_num_io_procs = fh->f_size;
    }

    aggregator_list = (int *) calloc (two_phase_num_io_procs, sizeof(int));
    if (NULL == aggregator_list){
	ret = OMPI_ERR_OUT_OF_RESOURCE;
	goto exit;
    }

    if ( OMPI_COMM_IS_MAPBY_NODE (&ompi_mpi_comm_world.comm) ) {
        for (i =0; i< two_phase_num_io_procs; i++){
            aggregator_list[i] = i;
        }
    }
    else {
        for (i =0; i< two_phase_num_io_procs; i++){
            aggregator_list[i] = i * fh->f_size / two_phase_num_io_procs;
        }
    }        

    ret = fh->f_generate_current_file_view ((struct ompio_file_t *)fh,
					    max_data,
					    &iov,
					    &local_count);

    if (OMPI_SUCCESS != ret){
	goto exit;
    }

    long_max_data = (long) max_data;
    ret = fh->f_comm->c_coll->coll_allreduce (&long_max_data,
					     &long_total_bytes,
					     1,
					     MPI_LONG,
					     MPI_SUM,
					     fh->f_comm,
					     fh->f_comm->c_coll->coll_allreduce_module);

    if ( OMPI_SUCCESS != ret ) {
	goto exit;
    }

    if (!(fh->f_flags & OMPIO_CONTIGUOUS_MEMORY)) {

	/* This datastructre translates between OMPIO->ROMIO its a little hacky!*/
	/* But helps to re-use romio's code for handling non-contiguous file-type*/
	/*Flattened datatype for ompio is in decoded_iov it translated into
	  flatbuf*/

	flat_buf = (Flatlist_node *)calloc(1, sizeof(Flatlist_node));
	if ( NULL == flat_buf ){
	    ret = OMPI_ERR_OUT_OF_RESOURCE;
	    goto exit;
	}

	flat_buf->type = datatype;
	flat_buf->next = NULL;
	flat_buf->count = 0;
	flat_buf->indices = NULL;
	flat_buf->blocklens = NULL;

	if ( 0 < count ) {
	    local_size = OMPIO_MAX(1,iov_count/count);
	}
	else {
	    local_size = 0;
	}


	if ( 0 < local_size ) {
	    flat_buf->indices =
		(OMPI_MPI_OFFSET_TYPE *)calloc(local_size,
					       sizeof(OMPI_MPI_OFFSET_TYPE));
	    if (NULL == flat_buf->indices){
		ret = OMPI_ERR_OUT_OF_RESOURCE;
		goto exit;
	    }

	    flat_buf->blocklens =
		(OMPI_MPI_OFFSET_TYPE *)calloc(local_size,
					       sizeof(OMPI_MPI_OFFSET_TYPE));
	    if ( NULL == flat_buf->blocklens ){
		ret = OMPI_ERR_OUT_OF_RESOURCE;
		goto exit;
	    }
	}
	flat_buf->count = local_size;
        for (j = 0 ; j < local_size ; ++j) {
	    flat_buf->indices[j] = (OMPI_MPI_OFFSET_TYPE)(intptr_t)decoded_iov[j].iov_base;
	    flat_buf->blocklens[j] = decoded_iov[j].iov_len;
	}

#if DEBUG
	printf("flat_buf count: %d\n",
	       flat_buf->count);
	for(i=0;i<flat_buf->count;i++){
	    printf("%d: blocklen[%d] : %lld, indices[%d]: %lld\n",
		   fh->f_rank, i, flat_buf->blocklens[i], i ,flat_buf->indices[i]);
	}
#endif
    }

#if DEBUG
    printf("%d: total_bytes:%ld, local_count: %d\n",
	   fh->f_rank, long_total_bytes, local_count);
    for (i=0 ; i<local_count ; i++) {
	printf("%d: fcoll:two_phase:read_all:OFFSET:%ld,LENGTH:%ld\n",
	       fh->f_rank,
	       (size_t)iov[i].iov_base,
	       (size_t)iov[i].iov_len);
    }
#endif

    start_offset = (OMPI_MPI_OFFSET_TYPE)(intptr_t)iov[0].iov_base;
    if ( 0 < local_count ) {
	end_offset = (OMPI_MPI_OFFSET_TYPE)(intptr_t)iov[local_count-1].iov_base +
	    (OMPI_MPI_OFFSET_TYPE)(intptr_t)iov[local_count-1].iov_len - 1;
    }
    else {
	end_offset = 0;
    }
#if DEBUG
    printf("%d: START OFFSET:%ld, END OFFSET:%ld\n",
	   fh->f_rank,
	   (size_t)start_offset,
	   (size_t)end_offset);
#endif

    start_offsets = (OMPI_MPI_OFFSET_TYPE *)calloc
	(fh->f_size, sizeof(OMPI_MPI_OFFSET_TYPE));

    if ( NULL == start_offsets ){
	ret = OMPI_ERR_OUT_OF_RESOURCE;
	goto exit;
    }

    end_offsets = (OMPI_MPI_OFFSET_TYPE *)calloc
	(fh->f_size, sizeof(OMPI_MPI_OFFSET_TYPE));

    if (NULL == end_offsets){
	ret = OMPI_ERR_OUT_OF_RESOURCE;
	goto exit;
    }

    ret = fh->f_comm->c_coll->coll_allgather(&start_offset,
					    1,
					    OMPI_OFFSET_DATATYPE,
					    start_offsets,
					    1,
					    OMPI_OFFSET_DATATYPE,
					    fh->f_comm,
					    fh->f_comm->c_coll->coll_allgather_module);

    if ( OMPI_SUCCESS != ret ){
	goto exit;
    }

    ret = fh->f_comm->c_coll->coll_allgather(&end_offset,
					    1,
					    OMPI_OFFSET_DATATYPE,
					    end_offsets,
					    1,
					    OMPI_OFFSET_DATATYPE,
					    fh->f_comm,
					    fh->f_comm->c_coll->coll_allgather_module);


    if ( OMPI_SUCCESS != ret ){
	goto exit;
    }

#if DEBUG
    for (i=0;i<fh->f_size;i++){
	printf("%d: start[%d]:%ld,end[%d]:%ld\n",
	       fh->f_rank,i,
	       (size_t)start_offsets[i],i,
	       (size_t)end_offsets[i]);
    }
#endif

    for (i=1; i<fh->f_size; i++){
	if ((start_offsets[i] < end_offsets[i-1]) &&
	    (start_offsets[i] <= end_offsets[i])){
	    interleave_count++;
	}
    }

#if DEBUG
    printf("%d: interleave_count:%d\n",
	   fh->f_rank,interleave_count);
#endif

    ret = mca_fcoll_two_phase_domain_partition(fh,
					       start_offsets,
					       end_offsets,
					       &min_st_offset,
					       &fd_start,
					       &fd_end,
					       domain_size,
					       &fd_size,
					       striping_unit,
					       two_phase_num_io_procs);
    if (OMPI_SUCCESS != ret){
	goto exit;
    }

#if DEBUG
    for (i=0;i<two_phase_num_io_procs;i++){
	printf("fd_start[%d] : %lld, fd_end[%d] : %lld, local_count: %d\n",
	       i, fd_start[i], i, fd_end[i], local_count);
    }
#endif

    ret = mca_fcoll_two_phase_calc_my_requests (fh,
						iov,
						local_count,
						min_st_offset,
						fd_start,
						fd_end,
						fd_size,
						&count_my_req_procs,
						&count_my_req_per_proc,
						&my_req,
						&buf_indices,
						striping_unit,
						two_phase_num_io_procs,
						aggregator_list);
    if ( OMPI_SUCCESS != ret ){
	goto exit;
    }

    ret = mca_fcoll_two_phase_calc_others_requests(fh,
						   count_my_req_procs,
						   count_my_req_per_proc,
						   my_req,
						   &count_other_req_procs,
						   &others_req);
    if (OMPI_SUCCESS != ret ){
	goto exit;
    }

#if DEBUG
    printf("%d count_other_req_procs : %d\n",
	   fh->f_rank,
	   count_other_req_procs);
#endif

#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
    start_rexch = MPI_Wtime();
#endif


    ret = two_phase_read_and_exch(fh,
				  buf,
				  datatype,
				  others_req,
				  iov,
				  local_count,
				  min_st_offset,
				  fd_size,
				  fd_start,
				  fd_end,
				  flat_buf,
				  buf_indices,
				  striping_unit,
				  two_phase_num_io_procs,
				  aggregator_list);


    if (OMPI_SUCCESS != ret){
	goto exit;
    }
#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
    end_rexch = MPI_Wtime();
    read_exch += (end_rexch - start_rexch);
    nentry.time[0] = read_time;
    nentry.time[1] = rcomm_time;
    nentry.time[2] = read_exch;
    if (isread_aggregator(fh->f_rank,
			  two_phase_num_io_procs,
			  aggregator_list)){
	nentry.aggregator = 1;
    }
    else{
	nentry.aggregator = 0;
    }
    nentry.nprocs_for_coll = two_phase_num_io_procs;


    if (!mca_common_ompio_full_print_queue(fh->f_coll_read_time)){
	mca_common_ompio_register_print_entry(fh->f_coll_read_time,
                                              nentry);
    }
#endif


exit:
    if (flat_buf != NULL){
	if (flat_buf->blocklens != NULL){
	    free (flat_buf->blocklens);
	}
	if (flat_buf->indices != NULL){
	    free (flat_buf->indices);
	}
        free (flat_buf);
    }

    free (start_offsets);
    free (end_offsets);
    free (aggregator_list);
    free (fd_start);
    free (decoded_iov);
    free (buf_indices);
    free (count_my_req_per_proc);
    free (my_req);
    free (others_req);
    free (fd_end);

    return ret;
}




static int two_phase_read_and_exch(ompio_file_t *fh,
				   void *buf,
				   MPI_Datatype datatype,
				   mca_common_ompio_access_array_t *others_req,
				   struct iovec *offset_len,
				   int contig_access_count,
				   OMPI_MPI_OFFSET_TYPE min_st_offset,
				   OMPI_MPI_OFFSET_TYPE fd_size,
				   OMPI_MPI_OFFSET_TYPE *fd_start,
				   OMPI_MPI_OFFSET_TYPE *fd_end,
				   Flatlist_node *flat_buf,
				   size_t *buf_idx, int striping_unit,
				   int two_phase_num_io_procs,
				   int *aggregator_list){


    int ret=OMPI_SUCCESS, i = 0, j = 0, ntimes = 0, max_ntimes = 0;
    int m = 0;
    int *curr_offlen_ptr=NULL, *count=NULL, *send_size=NULL, *recv_size=NULL;
    int *partial_send=NULL, *start_pos=NULL, req_len=0, flag=0;
    int *recd_from_proc=NULL;
    MPI_Aint buftype_extent=0;
    size_t byte_size = 0;
    OMPI_MPI_OFFSET_TYPE st_loc=-1, end_loc=-1, off=0, done=0, for_next_iter=0;
    OMPI_MPI_OFFSET_TYPE size=0, req_off=0, real_size=0, real_off=0, len=0;
    OMPI_MPI_OFFSET_TYPE for_curr_iter=0;
    char *read_buf=NULL, *tmp_buf=NULL;
    MPI_Datatype byte = MPI_BYTE;
    int two_phase_cycle_buffer_size=0;

    opal_datatype_type_size(&byte->super,
			    &byte_size);

    for (i = 0; i < fh->f_size; i++){
	if (others_req[i].count) {
	    st_loc = others_req[i].offsets[0];
	    end_loc = others_req[i].offsets[0];
	    break;
	}
    }

    for (i=0;i<fh->f_size;i++){
	for(j=0;j< others_req[i].count; j++){
	    st_loc =
		OMPIO_MIN(st_loc, others_req[i].offsets[j]);
	    end_loc =
		OMPIO_MAX(end_loc, (others_req[i].offsets[j] +
				    others_req[i].lens[j] - 1));
	}
    }

    two_phase_cycle_buffer_size = fh->f_bytes_per_agg;
    ntimes = (int)((end_loc - st_loc + two_phase_cycle_buffer_size)/
		   two_phase_cycle_buffer_size);

    if ((st_loc == -1) && (end_loc == -1)){
	ntimes = 0;
    }

    fh->f_comm->c_coll->coll_allreduce (&ntimes,
				       &max_ntimes,
				       1,
				       MPI_INT,
				       MPI_MAX,
				       fh->f_comm,
				       fh->f_comm->c_coll->coll_allreduce_module);

    if (ntimes){
	read_buf = (char *) calloc (two_phase_cycle_buffer_size, sizeof(char));
	if ( NULL == read_buf ){
	    ret =  OMPI_ERR_OUT_OF_RESOURCE;
	    goto exit;
	}
    }

    curr_offlen_ptr = (int *)calloc (fh->f_size,
				     sizeof(int));
    if (NULL == curr_offlen_ptr){
	ret = OMPI_ERR_OUT_OF_RESOURCE;
	goto exit;
    }

    count = (int *)calloc (fh->f_size,
			   sizeof(int));
    if (NULL == count){
	ret = OMPI_ERR_OUT_OF_RESOURCE;
	goto exit;
    }

    partial_send = (int *)calloc(fh->f_size, sizeof(int));
    if ( NULL == partial_send ){
	ret = OMPI_ERR_OUT_OF_RESOURCE;
	goto exit;
    }

    send_size = (int *)malloc(fh->f_size * sizeof(int));
    if (NULL == send_size){
	ret = OMPI_ERR_OUT_OF_RESOURCE;
	goto exit;
    }

    recv_size = (int *)malloc(fh->f_size * sizeof(int));
    if (NULL == recv_size){
	ret = OMPI_ERR_OUT_OF_RESOURCE;
	goto exit;
    }

    recd_from_proc = (int *)calloc(fh->f_size,sizeof(int));
    if (NULL == recd_from_proc){
	ret = OMPI_ERR_OUT_OF_RESOURCE;
	goto exit;
    }

    start_pos = (int *) calloc(fh->f_size, sizeof(int));
    if ( NULL == start_pos ){
        ret = OMPI_ERR_OUT_OF_RESOURCE;
        goto exit;
    }

    done = 0;
    off = st_loc;
    for_curr_iter = for_next_iter = 0;

    ompi_datatype_type_extent(datatype, &buftype_extent);

    for (m=0; m<ntimes; m++) {

	size = OMPIO_MIN((unsigned)two_phase_cycle_buffer_size, end_loc-st_loc+1-done);
	real_off = off - for_curr_iter;
	real_size = size + for_curr_iter;

	for (i=0; i<fh->f_size; i++) count[i] = send_size[i] = 0;
	for_next_iter = 0;

	for (i=0; i<fh->f_size; i++) {
	    if (others_req[i].count) {
		start_pos[i] = curr_offlen_ptr[i];
		for (j=curr_offlen_ptr[i]; j<others_req[i].count;
		     j++) {
		    if (partial_send[i]) {
			/* this request may have been partially
			   satisfied in the previous iteration. */
			req_off = others_req[i].offsets[j] +
			    partial_send[i];
			req_len = others_req[i].lens[j] -
			    partial_send[i];
			partial_send[i] = 0;
			/* modify the off-len pair to reflect this change */
			others_req[i].offsets[j] = req_off;
			others_req[i].lens[j] = req_len;
		    }
		    else {
			req_off = others_req[i].offsets[j];
			req_len = others_req[i].lens[j];
		    }
		    if (req_off < real_off + real_size) {
			count[i]++;
			PMPI_Get_address(read_buf+req_off-real_off,
				     &(others_req[i].mem_ptrs[j]));

			send_size[i] += (int)(OMPIO_MIN(real_off + real_size - req_off,
							(OMPI_MPI_OFFSET_TYPE)req_len));

			if (real_off+real_size-req_off < (OMPI_MPI_OFFSET_TYPE)req_len) {
			    partial_send[i] = (int) (real_off + real_size - req_off);
			    if ((j+1 < others_req[i].count) &&
				(others_req[i].offsets[j+1] <
				 real_off+real_size)) {
				/* this is the case illustrated in the
				   figure above. */
				for_next_iter = OMPIO_MAX(for_next_iter,
							  real_off + real_size - others_req[i].offsets[j+1]);
				/* max because it must cover requests
				   from different processes */
			    }
			    break;
			}
		    }
		    else break;
		}
		curr_offlen_ptr[i] = j;
	    }
	}
	flag = 0;
	for (i=0; i<fh->f_size; i++)
	    if (count[i]) flag = 1;

	if (flag) {

#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
	    start_read_time = MPI_Wtime();
#endif

	    len = size * byte_size;
	    fh->f_io_array = (mca_common_ompio_io_array_t *)calloc
		(1,sizeof(mca_common_ompio_io_array_t));
	    if (NULL == fh->f_io_array) {
		opal_output(1, "OUT OF MEMORY\n");
                ret = OMPI_ERR_OUT_OF_RESOURCE;
                goto exit;
	    }
	    fh->f_io_array[0].offset = (IOVBASE_TYPE *)(intptr_t)off;
	    fh->f_io_array[0].length = len;
	    fh->f_io_array[0].memory_address =
		read_buf+for_curr_iter;
	    fh->f_num_of_io_entries = 1;

	    if (fh->f_num_of_io_entries){
		if ( 0 > fh->f_fbtl->fbtl_preadv (fh)) {
		    opal_output(1, "READ FAILED\n");
                    ret = OMPI_ERROR;
                    goto exit;
		}
	    }

#if 0
	    int ii;
	    printf("%d: len/4 : %lld\n",
		   fh->f_rank,
		   len/4);
	    for (ii = 0; ii < len/4 ;ii++){
		printf("%d: read_buf[%d]: %ld\n",
		       fh->f_rank,
		       ii,
		       (int *)read_buf[ii]);
	    }
#endif
	    fh->f_num_of_io_entries = 0;
	    if (NULL != fh->f_io_array) {
		free (fh->f_io_array);
		fh->f_io_array = NULL;
	    }

#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
	    end_read_time = MPI_Wtime();
	    read_time += (end_read_time - start_read_time);
#endif


	}

	for_curr_iter = for_next_iter;

	for (i=0; i< fh->f_size; i++){
	    recv_size[i]  = 0;
	}
	two_phase_exchange_data(fh, buf, offset_len,
				send_size, start_pos, recv_size, count,
				partial_send, recd_from_proc,
				contig_access_count,
				min_st_offset, fd_size, fd_start, fd_end,
				flat_buf, others_req, m, buf_idx,
				buftype_extent, striping_unit, two_phase_num_io_procs,
				aggregator_list);

	if (for_next_iter){
	    tmp_buf = (char *) calloc (for_next_iter, sizeof(char));
	    memcpy(tmp_buf,
		   read_buf+real_size-for_next_iter,
		   for_next_iter);
	    free(read_buf);
	    read_buf = (char *)malloc(for_next_iter+two_phase_cycle_buffer_size);
	    memcpy(read_buf, tmp_buf, for_next_iter);
	    free(tmp_buf);
	}

	off += size;
	done += size;
    }

    for (i=0; i<fh->f_size; i++) count[i] = send_size[i] = 0;
    for (m=ntimes; m<max_ntimes; m++)
	two_phase_exchange_data(fh, buf, offset_len, send_size,
				start_pos, recv_size, count,
				partial_send, recd_from_proc,
				contig_access_count,
				min_st_offset, fd_size, fd_start, fd_end,
				flat_buf, others_req, m, buf_idx,
				buftype_extent, striping_unit, two_phase_num_io_procs,
				aggregator_list);

exit:
    free (read_buf);
    free (curr_offlen_ptr);
    free (count);
    free (partial_send);
    free (send_size);
    free (recv_size);
    free (recd_from_proc);
    free (start_pos);

    return ret;

}

static int two_phase_exchange_data(ompio_file_t *fh,
				   void *buf, struct iovec *offset_len,
				   int *send_size, int *start_pos, int *recv_size,
				   int *count, int *partial_send,
				   int *recd_from_proc, int contig_access_count,
				   OMPI_MPI_OFFSET_TYPE min_st_offset,
				   OMPI_MPI_OFFSET_TYPE fd_size,
				   OMPI_MPI_OFFSET_TYPE *fd_start,
				   OMPI_MPI_OFFSET_TYPE *fd_end,
				   Flatlist_node *flat_buf,
				   mca_common_ompio_access_array_t *others_req,
				   int iter, size_t *buf_idx,
				   MPI_Aint buftype_extent, int striping_unit,
				   int two_phase_num_io_procs, int *aggregator_list)
{

    int i=0, j=0, k=0, tmp=0, nprocs_recv=0, nprocs_send=0;
    int ret = OMPI_SUCCESS;
    char **recv_buf = NULL;
    MPI_Request *requests=NULL;
    MPI_Datatype send_type;



#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
    start_rcomm_time = MPI_Wtime();
#endif

    ret = fh->f_comm->c_coll->coll_alltoall (send_size,
					    1,
					    MPI_INT,
					    recv_size,
					    1,
					    MPI_INT,
					    fh->f_comm,
					    fh->f_comm->c_coll->coll_alltoall_module);

    if ( OMPI_SUCCESS != ret ){
	goto exit;
    }


#if DEBUG
    for (i=0; i<fh->f_size; i++){
	printf("%d: RS[%d]: %d\n", fh->f_rank,
	       i,
	       recv_size[i]);
    }
#endif


    nprocs_recv = 0;
    for (i=0; i < fh->f_size; i++)
	if (recv_size[i]) nprocs_recv++;

    nprocs_send = 0;
    for (i=0; i< fh->f_size; i++)
	if (send_size[i]) nprocs_send++;

    requests = (MPI_Request *)
	malloc((nprocs_send+nprocs_recv+1) *  sizeof(MPI_Request));

    if (fh->f_flags & OMPIO_CONTIGUOUS_MEMORY) {
	j = 0;
	for (i=0; i < fh->f_size; i++){
	    if (recv_size[i]){
		ret = MCA_PML_CALL(irecv(((char *) buf)+ buf_idx[i],
					 recv_size[i],
					 MPI_BYTE,
					 i,
					 fh->f_rank+i+100*iter,
					 fh->f_comm,
					 requests+j));

		if ( OMPI_SUCCESS != ret ){
		    return ret;
		}
		j++;
		buf_idx[i] += recv_size[i];
	    }
	}
    }
    else{

	recv_buf = (char **) calloc (fh->f_size, sizeof(char *));
	if (NULL == recv_buf){
	    ret = OMPI_ERR_OUT_OF_RESOURCE;
	    goto exit;
	}

	for (i=0; i < fh->f_size; i++)
	    if(recv_size[i]) recv_buf[i] =
				 (char *) malloc (recv_size[i] *  sizeof(char));
	j = 0;
	for(i=0; i<fh->f_size; i++)
	    if (recv_size[i]) {
		ret = MCA_PML_CALL(irecv(recv_buf[i],
					 recv_size[i],
					 MPI_BYTE,
					 i,
					 fh->f_rank+i+100*iter,
					 fh->f_comm,
					 requests+j));
		j++;

	    }
    }



    j = 0;
    for (i = 0; i< fh->f_size; i++){
	if (send_size[i]){
	    if (partial_send[i]){
		k = start_pos[i] + count[i] - 1;
		tmp = others_req[i].lens[k];
		others_req[i].lens[k] = partial_send[i];
	    }

	    ompi_datatype_create_hindexed(count[i],
					  &(others_req[i].lens[start_pos[i]]),
					  &(others_req[i].mem_ptrs[start_pos[i]]),
					  MPI_BYTE,
					  &send_type);

	    ompi_datatype_commit(&send_type);

	    ret = MCA_PML_CALL(isend(MPI_BOTTOM,
				     1,
				     send_type,
				     i,
				     fh->f_rank+i+100*iter,
				     MCA_PML_BASE_SEND_STANDARD,
				     fh->f_comm,
				     requests+nprocs_recv+j));
	    ompi_datatype_destroy(&send_type);

	    if (partial_send[i]) others_req[i].lens[k] = tmp;
	    j++;
	}
    }


    if (nprocs_recv) {

	ret = ompi_request_wait_all(nprocs_recv,
				    requests,
				    MPI_STATUS_IGNORE);
        if (OMPI_SUCCESS != ret) {
            goto exit;
        }

	if (! (fh->f_flags & OMPIO_CONTIGUOUS_MEMORY)) {

	    two_phase_fill_user_buffer(fh, buf, flat_buf,
				       recv_buf, offset_len,
				       (unsigned *)recv_size, requests,
				       recd_from_proc, contig_access_count,
				       min_st_offset, fd_size, fd_start, fd_end,
				       buftype_extent, striping_unit, two_phase_num_io_procs,
				       aggregator_list);
	}
    }

    ret = ompi_request_wait_all(nprocs_send,
				requests+nprocs_recv,
				MPI_STATUS_IGNORE);

#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
    end_rcomm_time = MPI_Wtime();
    rcomm_time += (end_rcomm_time - start_rcomm_time);
#endif

exit:

    if (recv_buf) {
	for (i=0; i< fh->f_size; i++){
            free(recv_buf[i]);
	}

	free(recv_buf);
    }

    free(requests);

    return ret;

}


#define TWO_PHASE_BUF_INCR			\
    {						\
	while (buf_incr) {				\
	    size_in_buf = OMPIO_MIN(buf_incr, flat_buf_sz);	\
	    user_buf_idx += size_in_buf;			\
	    flat_buf_sz -= size_in_buf;				\
	    if (!flat_buf_sz) {					      \
		if (flat_buf_idx < (flat_buf->count - 1)) flat_buf_idx++; \
		else {							\
		    flat_buf_idx = 0;					\
		    n_buftypes++;					\
		}							\
		user_buf_idx = flat_buf->indices[flat_buf_idx] +	\
		(OMPI_MPI_OFFSET_TYPE)n_buftypes*(OMPI_MPI_OFFSET_TYPE)buftype_extent; \
            flat_buf_sz = flat_buf->blocklens[flat_buf_idx]; \
        } \
        buf_incr -= size_in_buf; \
    } \
}


#define TWO_PHASE_BUF_COPY \
{ \
    while (size) { \
        size_in_buf = OMPIO_MIN(size, flat_buf_sz); \
	memcpy(((char *) buf) + user_buf_idx, \
	       &(recv_buf[p][recv_buf_idx[p]]), size_in_buf); \
	recv_buf_idx[p] += size_in_buf; \
	user_buf_idx += size_in_buf; \
	flat_buf_sz -= size_in_buf; \
        if (!flat_buf_sz) { \
           if (flat_buf_idx < (flat_buf->count - 1)) flat_buf_idx++; \
            else { \
                flat_buf_idx = 0; \
                n_buftypes++; \
            } \
            user_buf_idx = flat_buf->indices[flat_buf_idx] + \
	      (OMPI_MPI_OFFSET_TYPE)n_buftypes*(OMPI_MPI_OFFSET_TYPE)buftype_extent; \
            flat_buf_sz = flat_buf->blocklens[flat_buf_idx]; \
        } \
        size -= size_in_buf; \
        buf_incr -= size_in_buf; \
    } \
    TWO_PHASE_BUF_INCR \
}



static void two_phase_fill_user_buffer(ompio_file_t *fh,
				       void *buf,
				       Flatlist_node *flat_buf,
				       char **recv_buf,
				       struct iovec *offset_length,
				       unsigned *recv_size,
				       MPI_Request *requests,
				       int *recd_from_proc,
				       int contig_access_count,
				       OMPI_MPI_OFFSET_TYPE min_st_offset,
				       OMPI_MPI_OFFSET_TYPE fd_size,
				       OMPI_MPI_OFFSET_TYPE *fd_start,
				       OMPI_MPI_OFFSET_TYPE *fd_end,
				       MPI_Aint buftype_extent,
				       int striping_unit, int two_phase_num_io_procs,
				       int *aggregator_list){

    int i = 0, p = 0, flat_buf_idx = 0;
    OMPI_MPI_OFFSET_TYPE flat_buf_sz = 0, size_in_buf = 0, buf_incr = 0, size = 0;
    int n_buftypes = 0;
    OMPI_MPI_OFFSET_TYPE off=0, len=0, rem_len=0, user_buf_idx=0;
    unsigned *curr_from_proc=NULL, *done_from_proc=NULL, *recv_buf_idx=NULL;

    curr_from_proc = (unsigned *) malloc (fh->f_size * sizeof(unsigned));
    done_from_proc = (unsigned *) malloc (fh->f_size * sizeof(unsigned));
    recv_buf_idx = (unsigned *) malloc (fh->f_size * sizeof(unsigned));

    for (i=0; i < fh->f_size; i++) {
	recv_buf_idx[i] = curr_from_proc[i] = 0;
	done_from_proc[i] = recd_from_proc[i];
    }

    flat_buf_idx = 0;
    n_buftypes = 0;

    if ( flat_buf->count > 0 ) {
	user_buf_idx = flat_buf->indices[0];
	flat_buf_sz = flat_buf->blocklens[0];
    }

    /* flat_buf_idx = current index into flattened buftype
       flat_buf_sz = size of current contiguous component in
       flattened buf */

    for (i=0; i<contig_access_count; i++) {

	off     = (OMPI_MPI_OFFSET_TYPE)(intptr_t)offset_length[i].iov_base;
	rem_len = (OMPI_MPI_OFFSET_TYPE)offset_length[i].iov_len;

	/* this request may span the file domains of more than one process */
	while (rem_len != 0) {
	    len = rem_len;
	    /* NOTE: len value is modified by ADIOI_Calc_aggregator() to be no
	     * longer than the single region that processor "p" is responsible
	     * for.
	     */
	    p = mca_fcoll_two_phase_calc_aggregator(fh,
						    off,
						    min_st_offset,
						    &len,
						    fd_size,
						    fd_start,
						    fd_end,
						    striping_unit,
						    two_phase_num_io_procs,
						    aggregator_list);

	    if (recv_buf_idx[p] < recv_size[p]) {
		if (curr_from_proc[p]+len > done_from_proc[p]) {
		    if (done_from_proc[p] > curr_from_proc[p]) {
			size = OMPIO_MIN(curr_from_proc[p] + len -
					 done_from_proc[p], recv_size[p]-recv_buf_idx[p]);
			buf_incr = done_from_proc[p] - curr_from_proc[p];
			TWO_PHASE_BUF_INCR
			    buf_incr = curr_from_proc[p]+len-done_from_proc[p];
			curr_from_proc[p] = done_from_proc[p] + size;
			TWO_PHASE_BUF_COPY
			    }
		    else {
			size = OMPIO_MIN(len,recv_size[p]-recv_buf_idx[p]);
			buf_incr = len;
			curr_from_proc[p] += (unsigned) size;
			TWO_PHASE_BUF_COPY
			    }
		}
		else {
		    curr_from_proc[p] += (unsigned) len;
		    buf_incr = len;
		    TWO_PHASE_BUF_INCR
			}
	    }
	    else {
		buf_incr = len;
		TWO_PHASE_BUF_INCR
		    }
	    off += len;
	    rem_len -= len;
	}
    }
    for (i=0; i < fh->f_size; i++)
	if (recv_size[i]) recd_from_proc[i] = curr_from_proc[i];

    free(curr_from_proc);
    free(done_from_proc);
    free(recv_buf_idx);

}

#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
int isread_aggregator(int rank,
		      int nprocs_for_coll,
		      int *aggregator_list){

    int i=0;
    for (i=0; i<nprocs_for_coll; i++){
	if (aggregator_list[i] == rank)
	    return 1;
    }
    return 0;
}
#endif
