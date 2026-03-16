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
 * Copyright (c) 2008-2018 University of Houston. All rights reserved.
 * Copyright (c) 2011-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2013 Inria.  All rights reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/topo/topo.h"
#include "ompi/mca/fcoll/base/fcoll_base_coll_array.h"
#include "opal/datatype/opal_convertor.h"
#include "opal/datatype/opal_datatype.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/info/info.h"
#include "ompi/request/request.h"

#include <math.h>
#include <unistd.h>

#include "io_ompio.h"


int ompi_io_ompio_generate_current_file_view (struct ompio_file_t *fh,
                                              size_t max_data,
                                              struct iovec **f_iov,
                                              int *iov_count)
{

    struct iovec *iov = NULL;
    size_t bytes_to_write;
    size_t sum_previous_counts = 0;
    int j, k;
    int block = 1;

   /* allocate an initial iovec, will grow if needed */
    iov = (struct iovec *) calloc
        (OMPIO_IOVEC_INITIAL_SIZE, sizeof (struct iovec));
    if (NULL == iov) {
        opal_output(1, "OUT OF MEMORY\n");
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    sum_previous_counts = fh->f_position_in_file_view;
    j = fh->f_index_in_file_view;
    bytes_to_write = max_data;
    k = 0;

    while (bytes_to_write) {
        ptrdiff_t disp;
        /* reallocate if needed */
        if (OMPIO_IOVEC_INITIAL_SIZE*block <= k) {
            block ++;
            iov = (struct iovec *)realloc
                (iov, OMPIO_IOVEC_INITIAL_SIZE *block *sizeof(struct iovec));
            if (NULL == iov) {
                opal_output(1, "OUT OF MEMORY\n");
                return OMPI_ERR_OUT_OF_RESOURCE;
            }
        }

        if (fh->f_decoded_iov[j].iov_len -
            (fh->f_total_bytes - sum_previous_counts) <= 0) {
            sum_previous_counts += fh->f_decoded_iov[j].iov_len;
            j = j + 1;
            if (j == (int)fh->f_iov_count) {
                j = 0;
                sum_previous_counts = 0;
                fh->f_offset += fh->f_view_extent;
                fh->f_position_in_file_view = sum_previous_counts;
                fh->f_index_in_file_view = j;
                fh->f_total_bytes = 0;
            }
        }

        disp = (ptrdiff_t)(fh->f_decoded_iov[j].iov_base) +
            (fh->f_total_bytes - sum_previous_counts);
        iov[k].iov_base = (IOVBASE_TYPE *)(intptr_t)(disp + fh->f_offset);

        if ((fh->f_decoded_iov[j].iov_len -
             (fh->f_total_bytes - sum_previous_counts))
            >= bytes_to_write) {
            iov[k].iov_len = bytes_to_write;
        }
        else {
            iov[k].iov_len =  fh->f_decoded_iov[j].iov_len -
                (fh->f_total_bytes - sum_previous_counts);
        }

        fh->f_total_bytes += iov[k].iov_len;
        bytes_to_write -= iov[k].iov_len;
        k = k + 1;
    }
    fh->f_position_in_file_view = sum_previous_counts;
    fh->f_index_in_file_view = j;
    *iov_count = k;
    *f_iov = iov;

    if (mca_io_ompio_record_offset_info){

	int tot_entries=0, *recvcounts=NULL, *displs=NULL;
	mca_io_ompio_offlen_array_t *per_process=NULL;
	mca_io_ompio_offlen_array_t  *all_process=NULL;
	int *sorted=NULL, *column_list=NULL, *values=NULL;
	int *row_index=NULL, i=0, l=0, m=0;
	int column_index=0, r_index=0;
	int blocklen[3] = {1, 1, 1};
	ptrdiff_t d[3], base;
	ompi_datatype_t *types[3];
	ompi_datatype_t *io_array_type=MPI_DATATYPE_NULL;
	int **adj_matrix=NULL;
	FILE *fp;


        recvcounts = (int *) malloc (fh->f_size * sizeof(int));
        if (NULL == recvcounts){
            return OMPI_ERR_OUT_OF_RESOURCE;
	}
        displs = (int *) malloc (fh->f_size * sizeof(int));
        if (NULL == displs){
            free(recvcounts);
            return OMPI_ERR_OUT_OF_RESOURCE;
	}

        fh->f_comm->c_coll->coll_gather (&k,
                                        1,
                                        MPI_INT,
                                        recvcounts,
                                        1,
					MPI_INT,
                                        OMPIO_ROOT,
                                        fh->f_comm,
                                        fh->f_comm->c_coll->coll_gather_module);

        per_process = (mca_io_ompio_offlen_array_t *)
	    malloc (k * sizeof(mca_io_ompio_offlen_array_t));
	if (NULL == per_process){
            opal_output(1,"Error while allocating per process!\n");
            free(recvcounts);
            free(displs);
            return  OMPI_ERR_OUT_OF_RESOURCE;
        }
        for (i=0;i<k;i++){
            per_process[i].offset =
                (OMPI_MPI_OFFSET_TYPE)(intptr_t)iov[i].iov_base;
            per_process[i].length =
                (MPI_Aint)iov[i].iov_len;
            per_process[i].process_id = fh->f_rank;
        }

	types[0] = &ompi_mpi_long.dt;
        types[1] = &ompi_mpi_long.dt;
        types[2] = &ompi_mpi_int.dt;

        d[0] = (ptrdiff_t)&per_process[0];
        d[1] = (ptrdiff_t)&per_process[0].length;
        d[2] = (ptrdiff_t)&per_process[0].process_id;
        base = d[0];
        for (i=0;i<3;i++){
            d[i] -= base;
        }
        ompi_datatype_create_struct (3,
                                     blocklen,
                                     d,
                                     types,
                                     &io_array_type);
        ompi_datatype_commit (&io_array_type);

	if (OMPIO_ROOT == fh->f_rank){
            tot_entries = recvcounts[0];
            displs[0] = 0;
            for(i=1;i<fh->f_size;i++){
                displs[i] = displs[i-1] + recvcounts[i-1];
                tot_entries += recvcounts[i];
            }
            all_process = (mca_io_ompio_offlen_array_t *)
                malloc (tot_entries * sizeof(mca_io_ompio_offlen_array_t));
            if (NULL == all_process){
                opal_output(1,"Error while allocating per process!\n");
                free(per_process);
                free(recvcounts);
                free(displs);
                return  OMPI_ERR_OUT_OF_RESOURCE;
            }

            sorted = (int *) malloc
                (tot_entries * sizeof(int));
            if (NULL == sorted){
                opal_output(1,"Error while allocating per process!\n");
                free(all_process);
                free(per_process);
                free(recvcounts);
                free(displs);
                return  OMPI_ERR_OUT_OF_RESOURCE;
            }

            adj_matrix = (int **) malloc (fh->f_size *
                                          sizeof(int *));
            if (NULL == adj_matrix) {
                opal_output(1,"Error while allocating per process!\n");
                free(sorted);
                free(all_process);
                free(per_process);
                free(recvcounts);
                free(displs);
                return  OMPI_ERR_OUT_OF_RESOURCE;
            }
            for (i=0;i<fh->f_size;i++){
                adj_matrix[i] = (int *) malloc (fh->f_size *
                                                sizeof (int ));
                if (NULL == adj_matrix[i]) {
                    for (j=0; j<i; j++) {
                        free(adj_matrix[j]);
                    }
                    free(adj_matrix);
                    free(sorted);
                    free(all_process);
                    free(per_process);
                    free(recvcounts);
                    free(displs);
                    return  OMPI_ERR_OUT_OF_RESOURCE;
                 }
            }

            for (i=0;i<fh->f_size;i++){
                for (j=0;j<fh->f_size;j++){
                    adj_matrix[i][j] = 0;
                }
            }
	}
	fh->f_comm->c_coll->coll_gatherv (per_process,
					 k,
					 io_array_type,
					 all_process,
					 recvcounts,
					 displs,
					 io_array_type,
					 OMPIO_ROOT,
					 fh->f_comm,
					 fh->f_comm->c_coll->coll_gatherv_module);

	ompi_datatype_destroy(&io_array_type);

	if (OMPIO_ROOT == fh->f_rank){

	    ompi_io_ompio_sort_offlen(all_process,
				      tot_entries,
				      sorted);

	    for (i=0;i<tot_entries-1;i++){
		j = all_process[sorted[i]].process_id;
		l = all_process[sorted[i+1]].process_id;
		adj_matrix[j][l] += 1;
		adj_matrix[l][j] += 1;
	    }

	    /*Compress sparse matrix based on CRS to write to file */
	    m = 0;
	    for (i=0; i<fh->f_size; i++){
		for (j=0; j<fh->f_size; j++){
		    if (adj_matrix[i][j] > 0){
			m++;
		    }
		}
	    }
	    fp = fopen("fileview_info.out", "w+");
            if ( NULL == fp ) {
                for (i=0; i<fh->f_size; i++) {
                    free(adj_matrix[i]);
                }
                free(adj_matrix);
                free(sorted);
                free(all_process);
                free(per_process);
                free(recvcounts);
                free(displs);
		return MPI_ERR_OTHER;
            }
	    fprintf(fp,"FILEVIEW\n");
	    column_list = (int *) malloc ( m * sizeof(int));
	    if (NULL == column_list){
		opal_output(1,"Error while allocating column list\n");
                fclose(fp);
                for (i=0; i<fh->f_size; i++) {
                    free(adj_matrix[i]);
                }
                free(adj_matrix);
                free(sorted);
                free(all_process);
                free(per_process);
                free(recvcounts);
                free(displs);
		return OMPI_ERR_OUT_OF_RESOURCE;
	    }
	    values = (int *) malloc ( m * sizeof(int));
	    if (NULL == values){
		opal_output(1,"Error while allocating values list\n");
                fclose(fp);
                for (i=0; i<fh->f_size; i++) {
                    free(adj_matrix[i]);
                }
                free(adj_matrix);
                free(column_list);
                free(sorted);
                free(all_process);
                free(per_process);
                free(recvcounts);
                free(displs);
		return OMPI_ERR_OUT_OF_RESOURCE;
	    }

	    row_index = (int *) malloc ((fh->f_size + 1) *
					sizeof(int));
	    if (NULL == row_index){
		opal_output(1,"Error while allocating row_index list\n");
                fclose(fp);
                for (i=0; i<fh->f_size; i++) {
                    free(adj_matrix[i]);
                }
                free(adj_matrix);
                free(values);
                free(column_list);
                free(sorted);
                free(all_process);
                free(per_process);
                free(recvcounts);
                free(displs);
		return OMPI_ERR_OUT_OF_RESOURCE;
	    }
	    fprintf(fp,"%d %d\n", m, fh->f_size+1);
	    column_index = 0;
	    r_index = 1;
	    row_index[0] = r_index;
	    for (i=0; i<fh->f_size; i++){
		for (j=0; j<fh->f_size; j++){
		    if (adj_matrix[i][j] > 0){
			values[column_index]= adj_matrix[i][j];
			column_list[column_index]= j;
			fprintf(fp,"%d ", column_list[column_index]);
			column_index++;
			r_index++;
		    }

		}
		row_index[i+1]= r_index;
	    }

	    fprintf(fp,"\n");
	    for (i=0; i<m;i++){
		fprintf(fp, "%d ", values[i]);
	    }
	    fprintf(fp, "\n");
	    for (i=0; i< (fh->f_size + 1); i++){
		fprintf(fp, "%d ", row_index[i]);
	    }
	    fprintf(fp, "\n");
	    fclose(fp);

	    if (NULL != recvcounts){
		free(recvcounts);
		recvcounts = NULL;
	    }
	    if (NULL != displs){
		free(displs);
		displs = NULL;
	    }
	    if (NULL != sorted){
		free(sorted);
		sorted = NULL;
	    }
	    if (NULL != per_process){
		free(per_process);
		per_process = NULL;
	    }
	    if (NULL != all_process){
		free(all_process);
		all_process = NULL;
	    }
	    free(column_list);
	    free(values);
	    if (NULL != row_index){
		free(row_index);
		row_index = NULL;
	    }
	    if (NULL != adj_matrix){
		for (i=0;i<fh->f_size;i++){
		    free(adj_matrix[i]);
		}
		free(adj_matrix);
		adj_matrix = NULL;
	    }
	}
    }
    return OMPI_SUCCESS;
}


int ompi_io_ompio_sort_offlen (mca_io_ompio_offlen_array_t *io_array,
                               int num_entries,
                               int *sorted){

    int i = 0;
    int j = 0;
    int left = 0;
    int right = 0;
    int largest = 0;
    int heap_size = num_entries - 1;
    int temp = 0;
    unsigned char done = 0;
    int* temp_arr = NULL;

    temp_arr = (int*)malloc(num_entries*sizeof(int));
    if (NULL == temp_arr) {
        opal_output (1, "OUT OF MEMORY\n");
        return OMPI_ERR_OUT_OF_RESOURCE;
    }
    temp_arr[0] = 0;
    for (i = 1; i < num_entries; ++i) {
        temp_arr[i] = i;
    }
    /* num_entries can be a large no. so NO RECURSION */
    for (i = num_entries/2-1 ; i>=0 ; i--) {
        done = 0;
        j = i;
        largest = j;

        while (!done) {
	    left = j*2+1;
            right = j*2+2;
            if ((left <= heap_size) &&
                (io_array[temp_arr[left]].offset > io_array[temp_arr[j]].offset)) {
                largest = left;
            }
            else {
                largest = j;
            }
            if ((right <= heap_size) &&
                (io_array[temp_arr[right]].offset >
                 io_array[temp_arr[largest]].offset)) {
                largest = right;
            }
            if (largest != j) {
                temp = temp_arr[largest];
                temp_arr[largest] = temp_arr[j];
                temp_arr[j] = temp;
                j = largest;
            }
            else {
                done = 1;
            }
        }
    }

    for (i = num_entries-1; i >=1; --i) {
        temp = temp_arr[0];
        temp_arr[0] = temp_arr[i];
        temp_arr[i] = temp;
        heap_size--;
        done = 0;
        j = 0;
        largest = j;

        while (!done) {
            left =  j*2+1;
            right = j*2+2;

            if ((left <= heap_size) &&
                (io_array[temp_arr[left]].offset >
                 io_array[temp_arr[j]].offset)) {
		largest = left;
            }
            else {
                largest = j;
            }
            if ((right <= heap_size) &&
                (io_array[temp_arr[right]].offset >
                 io_array[temp_arr[largest]].offset)) {
                largest = right;
            }
            if (largest != j) {
                temp = temp_arr[largest];
                temp_arr[largest] = temp_arr[j];
                temp_arr[j] = temp;
                j = largest;
            }
            else {
                done = 1;
            }
        }
        sorted[i] = temp_arr[i];
    }
    sorted[0] = temp_arr[0];

    if (NULL != temp_arr) {
        free(temp_arr);
        temp_arr = NULL;
    }
    return OMPI_SUCCESS;
}


int mca_io_ompio_get_mca_parameter_value ( char *mca_parameter_name, int name_length )
{
    if ( !strncmp ( mca_parameter_name, "verbose_info_parsing", name_length )) {
        return mca_io_ompio_verbose_info_parsing;
    }
    else if ( !strncmp ( mca_parameter_name, "num_aggregators", name_length )) {
        return mca_io_ompio_num_aggregators;
    }
    else if ( !strncmp ( mca_parameter_name, "bytes_per_agg", name_length )) {
        return mca_io_ompio_bytes_per_agg;
    }
    else if ( !strncmp ( mca_parameter_name, "overwrite_amode", name_length )) {
        return mca_io_ompio_overwrite_amode;
    }
    else if ( !strncmp ( mca_parameter_name, "cycle_buffer_size", name_length )) {
        return mca_io_ompio_cycle_buffer_size;
    }
    else if ( !strncmp ( mca_parameter_name, "max_aggregators_ratio", name_length )) {
        return mca_io_ompio_max_aggregators_ratio;
    }
    else if ( !strncmp ( mca_parameter_name, "aggregators_cutoff_threshold", name_length )) {
        return mca_io_ompio_aggregators_cutoff_threshold;
    }
    else if ( !strncmp ( mca_parameter_name, "grouping_option", name_length )) {
        return mca_io_ompio_grouping_option;
    }
    else if ( !strncmp ( mca_parameter_name, "coll_timing_info", name_length )) {
        return mca_io_ompio_coll_timing_info;
    }
    else {
        opal_output (1, "Error in mca_io_ompio_get_mca_parameter_value: unknown parameter name");
    }

    /* Using here OMPI_ERROR_MAX instead of OMPI_ERROR, since -1 (which is OMPI_ERROR) 
    ** is a valid value for some mca parameters, indicating that the user did not set 
    ** that parameter value 
    */
    return OMPI_ERR_MAX;
}





