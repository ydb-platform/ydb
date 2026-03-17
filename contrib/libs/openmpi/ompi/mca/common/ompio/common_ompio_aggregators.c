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
 * Copyright (c) 2008-2017 University of Houston. All rights reserved.
 * Copyright (c) 2011-2018 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2012-2013 Inria.  All rights reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
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

#include "common_ompio.h"

/*
** This file contains all the functionality related to determing the number of aggregators
** and the list of aggregators.
**
** The first group functions determines the number of aggregators based on various characteristics
** 
** 1. simple_grouping: A heuristic based on a cost model
** 2. fview_based_grouping: analysis the fileview to detect regular patterns
** 3. cart_based_grouping: uses a cartesian communicator to derive certain (probable) properties
**    of the access pattern
*/

static double cost_calc (int P, int P_agg, size_t Data_proc, size_t coll_buffer, int dim );
#define DIM1 1
#define DIM2 2

int mca_common_ompio_simple_grouping(ompio_file_t *fh,
                                     int *num_groups_out,
                                     mca_common_ompio_contg *contg_groups)
{
    int num_groups=1;

    double time=0.0, time_prev=0.0, dtime=0.0, dtime_abs=0.0, dtime_diff=0.0, dtime_prev=0.0;
    double dtime_threshold=0.0;

    /* This is the threshold for absolute improvement. It is not 
    ** exposed as an MCA parameter to avoid overwhelming users. It is 
    ** mostly relevant for smaller process counts and data volumes. 
    */
    double time_threshold=0.001; 

    int incr=1, mode=1;
    int P_a, P_a_prev;

    /* The aggregator selection algorithm is based on the formulas described
    ** in: Shweta Jha, Edgar Gabriel, 'Performance Models for Communication in
    ** Collective I/O operations', Proceedings of the 17th IEEE/ACM Symposium
    ** on Cluster, Cloud and Grid Computing, Workshop on Theoretical
    ** Approaches to Performance Evaluation, Modeling and Simulation, 2017.
    **
    ** The current implementation is based on the 1-D and 2-D models derived for the even
    ** file partitioning strategy in the paper. Note, that the formulas currently only model
    ** the communication aspect of collective I/O operations. There are two extensions in this
    ** implementation: 
    ** 
    ** 1. Since the resulting formula has an asymptotic behavior w.r.t. the
    ** no. of aggregators, this version determines the no. of aggregators to
    ** be used iteratively and stops increasing the no. of aggregators if the
    ** benefits of increasing the aggregators is below a certain threshold
    ** value relative to the last number tested. The aggresivnes of cutting of
    ** the increasie in the number of aggregators is controlled by the new mca
    ** parameter mca_io_ompio_aggregator_cutoff_threshold.  Lower values for
    ** this parameter will lead to higher number of aggregators (useful e.g
    ** for PVFS2 and GPFS file systems), while higher number will lead to
    ** lower no. of aggregators (useful for regular UNIX or NFS file systems).
    **
    ** 2. The algorithm further caps the maximum no. of aggregators used to not exceed
    ** (no. of processes / mca_io_ompio_max_aggregators_ratio), i.e. a higher value
    ** for mca_io_ompio_max_aggregators will decrease the maximum number of aggregators
    ** allowed for the given no. of processes.
    */
    dtime_threshold = (double) OMPIO_MCA_GET(fh, aggregators_cutoff_threshold) / 100.0;

    /* Determine whether to use the formula for 1-D or 2-D data decomposition. Anything
    ** that is not 1-D is assumed to be 2-D in this version
    */ 
    mode = ( fh->f_cc_size == fh->f_view_size ) ? 1 : 2;

    /* Determine the increment size when searching the optimal
    ** no. of aggregators 
    */
    if ( fh->f_size < 16 ) {
	incr = 2;
    }
    else if (fh->f_size < 128 ) {
	incr = 4;
    }
    else if ( fh->f_size < 4096 ) {
	incr = 16;
    }
    else {
	incr = 32;
    }

    P_a = 1;
    time_prev = cost_calc ( fh->f_size, P_a, fh->f_view_size, (size_t) fh->f_bytes_per_agg, mode );
    P_a_prev = P_a;
    for ( P_a = incr; P_a <= fh->f_size; P_a += incr ) {
	time = cost_calc ( fh->f_size, P_a, fh->f_view_size, (size_t) fh->f_bytes_per_agg, mode );
	dtime_abs = (time_prev - time);
	dtime = dtime_abs / time_prev;
	dtime_diff = ( P_a == incr ) ? dtime : (dtime_prev - dtime);
#ifdef OMPIO_DEBUG
	if ( 0 == fh->f_rank  ){
	    printf(" d_p = %ld P_a = %d time = %lf dtime = %lf dtime_abs =%lf dtime_diff=%lf\n", 
		   fh->f_view_size, P_a, time, dtime, dtime_abs, dtime_diff );
	}
#endif
	if ( dtime_diff < dtime_threshold ) {
	    /* The relative improvement compared to the last number
	    ** of aggregators was below a certain threshold. This is typically
	    ** the dominating factor for large data volumes and larger process
	    ** counts 
	    */
#ifdef OMPIO_DEBUG
	    if ( 0 == fh->f_rank ) {
		printf("dtime_diff below threshold\n");
	    }
#endif
	    break;
	}
	if ( dtime_abs < time_threshold ) {
	    /* The absolute improvement compared to the last number 
	    ** of aggregators was below a given threshold. This is typically
	    ** important for small data valomes and smallers process counts
	    */
#ifdef OMPIO_DEBUG
	    if ( 0 == fh->f_rank ) {
		printf("dtime_abs below threshold\n");
	    }
#endif
	    break;
	}
	time_prev = time;
	dtime_prev = dtime;
	P_a_prev = P_a;	
    }
    num_groups = P_a_prev;
#ifdef OMPIO_DEBUG
    printf(" For P=%d d_p=%ld b_c=%d threshold=%f chosen P_a = %d \n", 
	   fh->f_size, fh->f_view_size, fh->f_bytes_per_agg, dtime_threshold, P_a_prev);
#endif
    
    /* Cap the maximum number of aggregators.*/
    if ( num_groups > (fh->f_size/OMPIO_MCA_GET(fh, max_aggregators_ratio))) {
	num_groups = (fh->f_size/OMPIO_MCA_GET(fh, max_aggregators_ratio));
    }
    if ( 1 >= num_groups ) {
	num_groups = 1;
    }
    
    *num_groups_out = num_groups;
    return mca_common_ompio_forced_grouping ( fh, num_groups, contg_groups);
}

int  mca_common_ompio_forced_grouping ( ompio_file_t *fh,
                                        int num_groups,
                                        mca_common_ompio_contg *contg_groups)
{
    int group_size = fh->f_size / num_groups;
    int rest = fh->f_size % num_groups;
    int flag = OMPI_COMM_IS_MAPBY_NODE (&ompi_mpi_comm_world.comm);
    int k=0, p=0, g=0;
    int total_procs = 0; 

    for ( k=0, p=0; p<num_groups; p++ ) {
        if ( p < rest ) {
            contg_groups[p].procs_per_contg_group = group_size+1;
            total_procs +=(group_size+1);
        }
        else {
            contg_groups[p].procs_per_contg_group = group_size;
            total_procs +=group_size;
        }

        if ( flag ) {
            /* Map by node used for MPI_COMM_WORLD */
            for ( g=0; g<contg_groups[p].procs_per_contg_group; g++ ) {
                k = g*num_groups+p;
                contg_groups[p].procs_in_contg_group[g] = k;
            }
        }
        else {
            for ( g=0; g<contg_groups[p].procs_per_contg_group; g++ ) {
                contg_groups[p].procs_in_contg_group[g] = k;
                k++;
            }
        }
    }    

    return OMPI_SUCCESS;
}

int mca_common_ompio_fview_based_grouping(ompio_file_t *fh,
                     		          int *num_groups,
				          mca_common_ompio_contg *contg_groups)
{

    int k = 0;
    int p = 0;
    int g = 0;
    int ret = OMPI_SUCCESS;
    OMPI_MPI_OFFSET_TYPE start_offset_len[3] = {0};
    OMPI_MPI_OFFSET_TYPE *end_offsets = NULL;
    OMPI_MPI_OFFSET_TYPE *start_offsets_lens = NULL;

    //Store start offset,length and corresponding rank in an array
    if(NULL == fh->f_decoded_iov){
      start_offset_len[0] = 0;
      start_offset_len[1] = 0;
    }
    else{
       start_offset_len[0] = (OMPI_MPI_OFFSET_TYPE) fh->f_decoded_iov[0].iov_base;
       start_offset_len[1] = fh->f_decoded_iov[0].iov_len;
    }
    start_offset_len[2] = fh->f_rank;

    start_offsets_lens = (OMPI_MPI_OFFSET_TYPE* )malloc (3 * fh->f_size * sizeof(OMPI_MPI_OFFSET_TYPE));
    if (NULL == start_offsets_lens) {
        opal_output (1, "OUT OF MEMORY\n");
        ret = OMPI_ERR_OUT_OF_RESOURCE;
        goto exit;
    }
    end_offsets = (OMPI_MPI_OFFSET_TYPE* )malloc (fh->f_size * sizeof(OMPI_MPI_OFFSET_TYPE));
    if (NULL == end_offsets) {
        opal_output (1, "OUT OF MEMORY\n");
        ret = OMPI_ERR_OUT_OF_RESOURCE;
        goto exit;
    }
    
    //Allgather start offsets across processes in a group on aggregator
    ret = fh->f_comm->c_coll->coll_allgather (start_offset_len,
                                             3,
                                             OMPI_OFFSET_DATATYPE,
                                             start_offsets_lens,
                                             3,
                                             OMPI_OFFSET_DATATYPE,
                                             fh->f_comm,
                                             fh->f_comm->c_coll->coll_allgather_module);
    if ( OMPI_SUCCESS != ret ) {
        goto exit;
    }


    //Calculate contg chunk size and contg subgroups
    for( k = 0 ; k < fh->f_size; k++){
        end_offsets[k] = start_offsets_lens[3*k] + start_offsets_lens[3*k+1];
        contg_groups[k].contg_chunk_size = 0;
    }
    k = 0;
    while( k < fh->f_size){
        if( k == 0){
            contg_groups[p].contg_chunk_size += start_offsets_lens[3*k+1];
            contg_groups[p].procs_in_contg_group[g] = start_offsets_lens[3*k + 2];
            g++;
            contg_groups[p].procs_per_contg_group = g;
            k++;
        }
        else if( start_offsets_lens[3*k] == end_offsets[k - 1] ){
            contg_groups[p].contg_chunk_size += start_offsets_lens[3*k+1];
            contg_groups[p].procs_in_contg_group[g] = start_offsets_lens[3*k + 2];
            g++;
            contg_groups[p].procs_per_contg_group = g;
            k++;
        }
        else{
            p++;
            g = 0;
            contg_groups[p].contg_chunk_size += start_offsets_lens[3*k+1];
            contg_groups[p].procs_in_contg_group[g] = start_offsets_lens[3*k + 2];
            g++;
            contg_groups[p].procs_per_contg_group = g;
            k++;
        }
    }
    
    *num_groups = p+1;
    ret = OMPI_SUCCESS;

exit:
    if (NULL != start_offsets_lens) {
        free (start_offsets_lens);
    }
    if (NULL != end_offsets) {
        free(end_offsets);
    }
 
    return ret;
}

int mca_common_ompio_cart_based_grouping(ompio_file_t *ompio_fh, 
                                         int *num_groups,
                                         mca_common_ompio_contg *contg_groups)
{
    int k = 0;
    int g=0;
    int ret = OMPI_SUCCESS, tmp_rank = 0;
    int *coords_tmp = NULL;

    mca_io_ompio_cart_topo_components cart_topo;
    memset (&cart_topo, 0, sizeof(mca_io_ompio_cart_topo_components)); 

    ret = ompio_fh->f_comm->c_topo->topo.cart.cartdim_get(ompio_fh->f_comm, &cart_topo.ndims);
    if (OMPI_SUCCESS != ret  ) {
        goto exit;
    }

    if (cart_topo.ndims < 2 ) {
        /* We shouldn't be here, this routine only works for more than 1 dimension */
        ret = MPI_ERR_INTERN;
        goto exit;
    }

    cart_topo.dims = (int*)malloc (cart_topo.ndims * sizeof(int));
    if (NULL == cart_topo.dims) {
        opal_output (1, "OUT OF MEMORY\n");
        ret = OMPI_ERR_OUT_OF_RESOURCE;
        goto exit;
    }
    cart_topo.periods = (int*)malloc (cart_topo.ndims * sizeof(int));
    if (NULL == cart_topo.periods) {
        opal_output (1, "OUT OF MEMORY\n");
        ret = OMPI_ERR_OUT_OF_RESOURCE;
        goto exit;
    }
    cart_topo.coords = (int*)malloc (cart_topo.ndims * sizeof(int));
    if (NULL == cart_topo.coords) {
        opal_output (1, "OUT OF MEMORY\n");
        ret = OMPI_ERR_OUT_OF_RESOURCE;
        goto exit;
    }

    coords_tmp  = (int*)malloc (cart_topo.ndims * sizeof(int));
    if (NULL == coords_tmp) {
        opal_output (1, "OUT OF MEMORY\n");
        ret = OMPI_ERR_OUT_OF_RESOURCE;
        goto exit;
    }

    ret = ompio_fh->f_comm->c_topo->topo.cart.cart_get(ompio_fh->f_comm,
                                                       cart_topo.ndims,
                                                       cart_topo.dims,
                                                       cart_topo.periods,
                                                       cart_topo.coords);
    if ( OMPI_SUCCESS != ret ) {
        opal_output (1, "mca_io_ompio_cart_based_grouping: Error in cart_get \n");
        goto exit;
    }

    *num_groups = cart_topo.dims[0];  //number of rows    

    for(k = 0; k < cart_topo.dims[0]; k++){
        int done = 0;
        int index = cart_topo.ndims-1;

        memset ( coords_tmp, 0, cart_topo.ndims * sizeof(int));
        contg_groups[k].procs_per_contg_group = (ompio_fh->f_size / cart_topo.dims[0]);
        coords_tmp[0] = k;

        ret = ompio_fh->f_comm->c_topo->topo.cart.cart_rank (ompio_fh->f_comm,coords_tmp,&tmp_rank);
        if ( OMPI_SUCCESS != ret ) {
            opal_output (1, "mca_io_ompio_cart_based_grouping: Error in cart_rank\n");
            goto exit;
        }
        contg_groups[k].procs_in_contg_group[0] = tmp_rank;

        for ( g=1; g< contg_groups[k].procs_per_contg_group; g++ ) {
            done = 0;
            index = cart_topo.ndims-1;
  
            while ( ! done ) { 
                coords_tmp[index]++;
                if ( coords_tmp[index] ==cart_topo.dims[index] ) {
                    coords_tmp[index]=0;
                    index--;
                }
                else {
                    done = 1;
                }
                if ( index == 0 ) {
                    done = 1;
                }
            }

           ret = ompio_fh->f_comm->c_topo->topo.cart.cart_rank (ompio_fh->f_comm,coords_tmp,&tmp_rank);
           if ( OMPI_SUCCESS != ret ) {
             opal_output (1, "mca_io_ompio_cart_based_grouping: Error in cart_rank\n");
             goto exit;
           }
           contg_groups[k].procs_in_contg_group[g] = tmp_rank;
        }
    }


exit:
    if (NULL != cart_topo.dims) {
       free (cart_topo.dims);
       cart_topo.dims = NULL;
    }
    if (NULL != cart_topo.periods) {
       free (cart_topo.periods);
       cart_topo.periods = NULL;
    }
    if (NULL != cart_topo.coords) {
       free (cart_topo.coords);
       cart_topo.coords = NULL;
    }
    if (NULL != coords_tmp) {
       free (coords_tmp);
       coords_tmp = NULL;
    }

    return ret;
}



int mca_common_ompio_finalize_initial_grouping(ompio_file_t *fh,
		                               int num_groups,
					       mca_common_ompio_contg *contg_groups)
{

    int z = 0;
    int y = 0;

    fh->f_init_num_aggrs = num_groups;
    if (NULL != fh->f_init_aggr_list) {
        free(fh->f_init_aggr_list);
    }
    fh->f_init_aggr_list = (int*)malloc (fh->f_init_num_aggrs * sizeof(int));
    if (NULL == fh->f_init_aggr_list) {
        opal_output (1, "OUT OF MEMORY\n");
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    for( z = 0 ;z < num_groups; z++){
        for( y = 0; y < contg_groups[z].procs_per_contg_group; y++){
            if ( fh->f_rank == contg_groups[z].procs_in_contg_group[y] ) {
                fh->f_init_procs_per_group = contg_groups[z].procs_per_contg_group;
                if (NULL != fh->f_init_procs_in_group) {
                    free(fh->f_init_procs_in_group);
                }
                fh->f_init_procs_in_group = (int*)malloc (fh->f_init_procs_per_group * sizeof(int));
                if (NULL == fh->f_init_procs_in_group) {
                    opal_output (1, "OUT OF MEMORY\n");
                    return OMPI_ERR_OUT_OF_RESOURCE;
                }
                memcpy ( fh->f_init_procs_in_group, contg_groups[z].procs_in_contg_group, 
                         contg_groups[z].procs_per_contg_group * sizeof (int));
                
            }
        }
    }

    for( z = 0 ;z < num_groups; z++){
        fh->f_init_aggr_list[z] = contg_groups[z].procs_in_contg_group[0];
    }


   return OMPI_SUCCESS;
}

/*****************************************************************************************************/
/*****************************************************************************************************/
/*****************************************************************************************************/
/* 
** This function is called by the collective I/O operations to determine the final number
** of aggregators.
*/

int mca_common_ompio_set_aggregator_props (struct ompio_file_t *fh,
                                           int num_aggregators,
                                           size_t bytes_per_proc)
{
    int j;
    int ret=OMPI_SUCCESS;

    fh->f_flags |= OMPIO_AGGREGATOR_IS_SET;

    if ( (-1 == num_aggregators) && 
         ((SIMPLE        != OMPIO_MCA_GET(fh, grouping_option) &&
           NO_REFINEMENT != OMPIO_MCA_GET(fh, grouping_option) &&
           SIMPLE_PLUS   != OMPIO_MCA_GET(fh, grouping_option) ))) {
        ret = mca_common_ompio_create_groups(fh,bytes_per_proc);
    }
    else {
        fh->f_procs_per_group  = fh->f_init_procs_per_group;
        fh->f_procs_in_group   = (int*)malloc (fh->f_procs_per_group * sizeof(int));
        if (NULL == fh->f_procs_in_group) {
            opal_output (1, "OUT OF MEMORY\n");
            return OMPI_ERR_OUT_OF_RESOURCE;
        }
        for (j=0 ; j<fh->f_procs_per_group ; j++) {
            fh->f_procs_in_group[j] = fh->f_init_procs_in_group[j];
        }
        
        fh->f_num_aggrs = fh->f_init_num_aggrs;
        fh->f_aggr_list = (int*) malloc ( fh->f_num_aggrs * sizeof(int));
        if (NULL == fh->f_aggr_list ) {
            opal_output (1, "OUT OF MEMORY\n");
            return OMPI_ERR_OUT_OF_RESOURCE;
        }
        for (j=0 ; j<fh->f_num_aggrs; j++) {
            fh->f_aggr_list[j] = fh->f_init_aggr_list[j];
        }            
    }

    return ret;
}



/*****************************************************************************************************/
/*****************************************************************************************************/
/*****************************************************************************************************/
int mca_common_ompio_create_groups(ompio_file_t *fh,
		                   size_t bytes_per_proc)
{

    int is_aggregator = 0;
    int final_aggr = 0;
    int final_num_aggrs = 0;
    int ret = OMPI_SUCCESS, ompio_grouping_flag = 0;
    int *tmp_final_aggrs=NULL;
    int *decision_list = NULL;
    int i,j;

    OMPI_MPI_OFFSET_TYPE *start_offsets_lens = NULL;
    OMPI_MPI_OFFSET_TYPE *end_offsets = NULL;
    OMPI_MPI_OFFSET_TYPE bytes_per_group = 0;
    OMPI_MPI_OFFSET_TYPE *aggr_bytes_per_group = NULL;

    ret = mca_common_ompio_prepare_to_group(fh,
                                            &start_offsets_lens,
                                            &end_offsets,
                                            &aggr_bytes_per_group,
                                            &bytes_per_group,
                                            &decision_list,
                                            bytes_per_proc,
                                            &is_aggregator,
                                            &ompio_grouping_flag);
    if ( OMPI_SUCCESS != ret ) {
        opal_output (1, "mca_common_ompio_create_groups: error in mca_common_ompio_prepare_to_group\n");
        goto exit;
    }

    switch(ompio_grouping_flag){

        case OMPIO_SPLIT:
            ret = mca_common_ompio_split_initial_groups(fh,
                                                        start_offsets_lens,
                                                        end_offsets,
                                                        bytes_per_group);
        break;

        case OMPIO_MERGE:
            ret = mca_common_ompio_merge_initial_groups(fh,
                                                        aggr_bytes_per_group,
                                                        decision_list,
                                                        is_aggregator);
            break;
            
        case  OMPIO_RETAIN:

            ret = mca_common_ompio_retain_initial_groups(fh);

        break;


    }
    if ( OMPI_SUCCESS != ret ) {
        opal_output (1, "mca_common_ompio_create_groups: error in subroutine called within switch statement\n");
        goto exit;
    }
    
    //Set aggregator index

    //Calculate final number of aggregators
    if(fh->f_rank == fh->f_procs_in_group[0]){
	   final_aggr = 1;
    }
    ret = fh->f_comm->c_coll->coll_allreduce (&final_aggr,
                                             &final_num_aggrs,
                                             1,
                                             MPI_INT,
                                             MPI_SUM,
                                             fh->f_comm,
                                             fh->f_comm->c_coll->coll_allreduce_module);
    if ( OMPI_SUCCESS != ret ) {
        opal_output (1, "mca_common_ompio_create_groups: error in allreduce\n");
        goto exit;
    }

    tmp_final_aggrs =(int*) malloc ( fh->f_size *sizeof(int));
    if ( NULL == tmp_final_aggrs ) {
        opal_output(1,"mca_common_ompio_create_groups: could not allocate memory\n");
        goto exit;
    }
    ret = fh->f_comm->c_coll->coll_allgather (&final_aggr,
                                              1, 
                                              MPI_INT,
                                              tmp_final_aggrs,
                                              1,
                                              MPI_INT,
                                              fh->f_comm,
                                              fh->f_comm->c_coll->coll_allgather_module);
    if ( OMPI_SUCCESS != ret ) {
        opal_output (1, "mca_common_ompio_create_groups: error in allreduce\n");
        goto exit;
    }
    

    //Set final number of aggregators in file handle
    fh->f_num_aggrs = final_num_aggrs;
    fh->f_aggr_list = (int*) malloc (fh->f_num_aggrs * sizeof(int));
    if ( NULL == fh->f_aggr_list ) {
        opal_output(1,"mca_common_ompio_create_groups: could not allocate memory\n");
        goto exit;
    }
    
    int found;
    for ( i=0, j=0; i<fh->f_num_aggrs; i++ ) {
        found = 0; 
        do {
            if ( 1 == tmp_final_aggrs[j] ) {
                fh->f_aggr_list[i] = j;
                found=1;
            }
            j++;
        } while ( !found && j < fh->f_size);       
    }

exit:

    if (NULL != start_offsets_lens) {
        free (start_offsets_lens);
    }
    if (NULL != end_offsets) {
        free (end_offsets);
    }
    if(NULL != aggr_bytes_per_group){
        free(aggr_bytes_per_group);
    }
    if( NULL != decision_list){
        free(decision_list);
    }
    if ( NULL != tmp_final_aggrs){
        free(tmp_final_aggrs);
    }

   return ret;
}

int mca_common_ompio_merge_initial_groups(ompio_file_t *fh,
		                          OMPI_MPI_OFFSET_TYPE *aggr_bytes_per_group,
				          int *decision_list,
	                                  int is_aggregator){

    OMPI_MPI_OFFSET_TYPE sum_bytes = 0;
    MPI_Request *sendreqs = NULL;

    int start = 0;
    int end = 0;
    int i = 0;
    int j = 0;
    int r  = 0;

    int merge_pair_flag = 4;
    int first_merge_flag = 4;
    int *merge_aggrs = NULL;
    int is_new_aggregator= 0;
    int ret = OMPI_SUCCESS;

    if(is_aggregator){
        i = 0;
	sum_bytes = 0;
        //go through the decision list
	//Find the aggregators that could merge

	while(i < fh->f_init_num_aggrs){
	    while(1){
	        if( i >= fh->f_init_num_aggrs){
	            break;
	        }
	        else if((decision_list[i] == OMPIO_MERGE) &&
	                (sum_bytes <= OMPIO_MCA_GET(fh, bytes_per_agg))){
	            sum_bytes = sum_bytes + aggr_bytes_per_group[i];
	            decision_list[i] = merge_pair_flag;
	            i++;
	        }
	        else if((decision_list[i] == OMPIO_MERGE) &&
	                (sum_bytes >= OMPIO_MCA_GET(fh, bytes_per_agg))){
	           if(decision_list[i+1] == OMPIO_MERGE){
	               merge_pair_flag++;
	               decision_list[i] = merge_pair_flag;
	               sum_bytes = aggr_bytes_per_group[i];
	               i++;
	           }
	           else{
	               decision_list[i] = merge_pair_flag;
	               i++;
	           }
	        }
	        else{
	            i++;
	            if(decision_list[i] == OMPIO_MERGE)
	               merge_pair_flag++;
	            sum_bytes = 0;
	            break;
	        }
	    }
        }

        //Now go through the new edited decision list and
	//make lists of aggregators to merge and number
	//of groups to me merged.
	i = 0;
	j = 0;

	while(i < fh->f_init_num_aggrs){
	   if(decision_list[i] >= first_merge_flag){
	       start = i;
	       while((decision_list[i] >= first_merge_flag) &&
		      (i < fh->f_init_num_aggrs-1)){
	           if(decision_list[i+1] == decision_list[i]){
	               i++;
	           }
	           else{
	               break;
	           }
	           end = i;
	       }
	       merge_aggrs = (int *)malloc((end - start + 1) * sizeof(int));
	       if (NULL == merge_aggrs) {
		  opal_output (1, "OUT OF MEMORY\n");
		  return OMPI_ERR_OUT_OF_RESOURCE;
	       }
	       j = 0;
	       for( j = 0 ; j < end - start + 1; j++){
	           merge_aggrs[j] = fh->f_init_aggr_list[start+j];
	       }
               if(fh->f_rank == merge_aggrs[0])
	          is_new_aggregator = 1;

	       for( j = 0 ; j < end-start+1 ;j++){
	          if(fh->f_rank == merge_aggrs[j]){
	              ret = mca_common_ompio_merge_groups(fh, merge_aggrs,
                                                          end-start+1);
                      if ( OMPI_SUCCESS != ret ) {
                          opal_output (1, "mca_common_ompio_merge_initial_groups: error in mca_common_ompio_merge_groups\n");
                          free ( merge_aggrs );                          
                          return ret;
                      }
		  }
	       }
               if(NULL != merge_aggrs){
	           free(merge_aggrs);
                   merge_aggrs = NULL;
               }

	   }
           i++;
        }

    }//end old aggregators

    //New aggregators communicate new grouping info to the groups
    if(is_new_aggregator){
       sendreqs = (MPI_Request *)malloc ( 2 *fh->f_procs_per_group * sizeof(MPI_Request));
       if (NULL == sendreqs) {
          return OMPI_ERR_OUT_OF_RESOURCE;
       }
       //Communicate grouping info
       for( j = 0 ; j < fh->f_procs_per_group; j++){
	   if (fh->f_procs_in_group[j] == fh->f_rank ) {
	       continue;
	   }
           //new aggregator sends new procs_per_group to all its members
	   ret = MCA_PML_CALL(isend(&fh->f_procs_per_group,
                                    1,
                                    MPI_INT,
                                    fh->f_procs_in_group[j],
                                    OMPIO_PROCS_PER_GROUP_TAG,
                                    MCA_PML_BASE_SEND_STANDARD,
                                    fh->f_comm,
                                    sendreqs + r++));
           if ( OMPI_SUCCESS != ret ) {
               opal_output (1, "mca_common_ompio_merge_initial_groups: error in Isend\n");
               goto exit;
           }
	   //new aggregator sends distribution of process to all its new members
	   ret = MCA_PML_CALL(isend(fh->f_procs_in_group,
                                    fh->f_procs_per_group,
                                    MPI_INT,
                                    fh->f_procs_in_group[j],
                                    OMPIO_PROCS_IN_GROUP_TAG,
                                    MCA_PML_BASE_SEND_STANDARD,
                                    fh->f_comm,
                                    sendreqs + r++));
           if ( OMPI_SUCCESS != ret ) {
               opal_output (1, "mca_common_ompio_merge_initial_groups: error in Isend 2\n");
               goto exit;
           }
           
       }
    }
    else {
	//All non aggregators
	//All processes receive initial process distribution from aggregators
	ret = MCA_PML_CALL(recv(&fh->f_procs_per_group,
                                1,
                                MPI_INT,
                                MPI_ANY_SOURCE,
                                OMPIO_PROCS_PER_GROUP_TAG,
                                fh->f_comm,
                                MPI_STATUS_IGNORE));
        if ( OMPI_SUCCESS != ret ) {
            opal_output (1, "mca_common_ompio_merge_initial_groups: error in Recv\n");
            return ret;
        }
        
	fh->f_procs_in_group = (int*)malloc (fh->f_procs_per_group * sizeof(int));
	if (NULL == fh->f_procs_in_group) {
	    opal_output (1, "OUT OF MEMORY\n");
	    return OMPI_ERR_OUT_OF_RESOURCE;
	}

	ret = MCA_PML_CALL(recv(fh->f_procs_in_group,
                                fh->f_procs_per_group,
                                MPI_INT,
                                MPI_ANY_SOURCE,
                                OMPIO_PROCS_IN_GROUP_TAG,
                                fh->f_comm,
                                MPI_STATUS_IGNORE));
        if ( OMPI_SUCCESS != ret ) {
            opal_output (1, "mca_common_ompio_merge_initial_groups: error in Recv 2\n");
            return ret;
        }

    }
    
    if(is_new_aggregator) {
	ret = ompi_request_wait_all (r, sendreqs, MPI_STATUSES_IGNORE);
    }

exit:
    if (NULL != sendreqs) {
        free(sendreqs);
    }

    return ret;
}

int mca_common_ompio_split_initial_groups(ompio_file_t *fh,
		                          OMPI_MPI_OFFSET_TYPE *start_offsets_lens,
				          OMPI_MPI_OFFSET_TYPE *end_offsets,
				          OMPI_MPI_OFFSET_TYPE bytes_per_group){


    int size_new_group = 0;
    int size_old_group = 0;
    int size_last_group = 0;
    int size_smallest_group = 0;
    int num_groups = 0;
    int ret = OMPI_SUCCESS;
    OMPI_MPI_COUNT_TYPE bytes_per_agg_group = 0;

    OMPI_MPI_OFFSET_TYPE max_cci = 0;
    OMPI_MPI_OFFSET_TYPE min_cci = 0;

    bytes_per_agg_group = (OMPI_MPI_COUNT_TYPE)OMPIO_MCA_GET(fh, bytes_per_agg);
    // integer round up
    size_new_group = (int)(bytes_per_agg_group / bytes_per_group + (bytes_per_agg_group % bytes_per_group ? 1u : 0u));
    size_old_group = fh->f_init_procs_per_group;

    ret = mca_common_ompio_split_a_group(fh,
                                         start_offsets_lens,
                                         end_offsets,
                                         size_new_group,
                                         &max_cci,
                                         &min_cci,
                                         &num_groups,
                                         &size_smallest_group);
    if (OMPI_SUCCESS != ret ) {
        opal_output (1, "mca_common_ompio_split_initial_groups: error in mca_common_ompio_split_a_group\n");
        return ret;
    }


    switch(OMPIO_MCA_GET(fh, grouping_option)){
        case DATA_VOLUME:
            //Just use size as returned by split group
            size_last_group = size_smallest_group;
	break;

	case UNIFORM_DISTRIBUTION:
	    if(size_smallest_group <= OMPIO_UNIFORM_DIST_THRESHOLD * size_new_group){
	        //uneven split need to call split again
	        if( size_old_group % num_groups == 0 ){
	           //most even distribution possible
	           size_new_group = size_old_group / num_groups;
	           size_last_group = size_new_group;
	        }
	        else{
	            //merge the last small group with the previous group
	            size_last_group = size_new_group + size_smallest_group;
	        }
	    }
	    else{
	         //Considered uniform
	         size_last_group = size_smallest_group;
	    }
	break;

	case CONTIGUITY:

   	    while(1){
		 if((max_cci < OMPIO_CONTG_THRESHOLD) &&
		    (size_new_group < size_old_group)){

		    size_new_group = (size_new_group + size_old_group ) / 2;
  	            ret = mca_common_ompio_split_a_group(fh,
                                                         start_offsets_lens,
                                                         end_offsets,
                                                         size_new_group,
                                                         &max_cci,
                                                         &min_cci,
                                                         &num_groups,
                                                         &size_smallest_group);
                    if (OMPI_SUCCESS != ret ) {
                        opal_output (1, "mca_common_ompio_split_initial_groups: error in mca_common_ompio_split_a_group 2\n");
                        return ret;
                    }
                 }
                 else{
                     break;
                 }
            }
	    size_last_group = size_smallest_group;
	break;

	case OPTIMIZE_GROUPING:
            //This case is a combination of Data volume, contiguity and uniform distribution
	    while(1){
	         if((max_cci < OMPIO_CONTG_THRESHOLD) &&
	            (size_new_group < size_old_group)){  //can be a better condition
                 //monitor the previous iteration
		 //break if it has not changed.
	      	     size_new_group = size_new_group + size_old_group;
	      	     // integer round up
	      	     size_new_group = size_new_group / 2 + (size_new_group % 2 ? 1 : 0);
		     ret = mca_common_ompio_split_a_group(fh,
                                                          start_offsets_lens,
                                                          end_offsets,
                                                          size_new_group,
                                                          &max_cci,
                                                          &min_cci,
                                                          &num_groups,
                                                          &size_smallest_group);
                    if (OMPI_SUCCESS != ret ) {
                        opal_output (1, "mca_common_ompio_split_initial_groups: error in mca_common_ompio_split_a_group 3\n");
                        return ret;
                    }
		 }
		 else{
		     break;
		 }
	    }

	   if(size_smallest_group <= OMPIO_UNIFORM_DIST_THRESHOLD * size_new_group){
	       //uneven split need to call split again
	       if( size_old_group % num_groups == 0 ){
	           //most even distribution possible
	           size_new_group = size_old_group / num_groups;
		   size_last_group = size_new_group;
	       }
	       else{
	            //merge the last small group with the previous group
	            size_last_group = size_new_group + size_smallest_group;
	       }
	   }
	   else{
	       //Considered uniform
	       size_last_group = size_smallest_group;
	   }

	break;
    }

    ret = mca_common_ompio_finalize_split(fh, size_new_group, size_last_group);

    return ret;
}


int mca_common_ompio_retain_initial_groups(ompio_file_t *fh){

    int i = 0;

    fh->f_procs_per_group = fh->f_init_procs_per_group;
    fh->f_procs_in_group = (int*)malloc (fh->f_procs_per_group * sizeof(int));
    if (NULL == fh->f_procs_in_group) {
        opal_output (1, "OUT OF MEMORY\n");
        return OMPI_ERR_OUT_OF_RESOURCE;
    }
    for( i = 0 ; i < fh->f_procs_per_group; i++){
        fh->f_procs_in_group[i] = fh->f_init_procs_in_group[i];
    }


    return OMPI_SUCCESS;
}

int mca_common_ompio_merge_groups(ompio_file_t *fh,
		                  int *merge_aggrs,
			          int num_merge_aggrs)
{
    int i = 0;
    int *sizes_old_group;
    int ret;
    int *displs = NULL;

    sizes_old_group = (int*)malloc(num_merge_aggrs * sizeof(int));
    if (NULL == sizes_old_group) {
        opal_output (1, "OUT OF MEMORY\n");
        ret = OMPI_ERR_OUT_OF_RESOURCE;
        goto exit;
    }


    displs = (int*)malloc(num_merge_aggrs * sizeof(int));
    if (NULL == displs) {
        opal_output (1, "OUT OF MEMORY\n");
        ret = OMPI_ERR_OUT_OF_RESOURCE;
        goto exit;
    }


    //merge_aggrs[0] is considered the new aggregator
    //New aggregator collects group sizes of the groups to be merged
    ret = ompi_fcoll_base_coll_allgather_array (&fh->f_init_procs_per_group,
                                           1,
                                           MPI_INT,
                                           sizes_old_group,
                                           1,
                                           MPI_INT,
                                           0,
                                           merge_aggrs,
                                           num_merge_aggrs,
                                           fh->f_comm);
    
    if ( OMPI_SUCCESS != ret ) {
        goto exit;
    }
    fh->f_procs_per_group = 0;


    for( i = 0; i < num_merge_aggrs; i++){
        fh->f_procs_per_group = fh->f_procs_per_group + sizes_old_group[i];
    }

    displs[0] = 0;
    for(i = 1; i < num_merge_aggrs; i++){
	  displs[i] = displs[i-1] + sizes_old_group[i-1];
    }

    fh->f_procs_in_group = (int*)malloc (fh->f_procs_per_group * sizeof(int));
    if (NULL == fh->f_procs_in_group) {
        opal_output (1, "OUT OF MEMORY\n");
        ret = OMPI_ERR_OUT_OF_RESOURCE;
        goto exit;
    }

    //New aggregator also collects the grouping distribution
    //This is the actual merge
    //use allgatherv array
    ret = ompi_fcoll_base_coll_allgatherv_array (fh->f_init_procs_in_group,
                                            fh->f_init_procs_per_group,
                                            MPI_INT,
                                            fh->f_procs_in_group,
                                            sizes_old_group,
                                            displs,
                                            MPI_INT,
                                            0,
                                            merge_aggrs,
                                            num_merge_aggrs,
                                            fh->f_comm);
    
exit:
    if (NULL != displs) {
        free (displs);
    }
    if (NULL != sizes_old_group) {
        free (sizes_old_group);
    }

    return ret;

}



int mca_common_ompio_split_a_group(ompio_file_t *fh,
     		                   OMPI_MPI_OFFSET_TYPE *start_offsets_lens,
		                   OMPI_MPI_OFFSET_TYPE *end_offsets,
		                   int size_new_group,
		                   OMPI_MPI_OFFSET_TYPE *max_cci,
		                   OMPI_MPI_OFFSET_TYPE *min_cci,
		                   int *num_groups,
		                   int *size_smallest_group)
{

    OMPI_MPI_OFFSET_TYPE *cci = NULL;
    *num_groups = fh->f_init_procs_per_group / size_new_group;
    *size_smallest_group = size_new_group;
    int i = 0;
    int k = 0;
    int flag = 0; //all groups same size
    int size = 0;

    if( fh->f_init_procs_per_group % size_new_group != 0 ){
        *num_groups = *num_groups + 1;
	*size_smallest_group = fh->f_init_procs_per_group % size_new_group;
	flag = 1;
    }

    cci = (OMPI_MPI_OFFSET_TYPE*)malloc(*num_groups * sizeof( OMPI_MPI_OFFSET_TYPE ));
    if (NULL == cci) {
        opal_output(1, "OUT OF MEMORY\n");
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    //check contiguity within new groups
    size = size_new_group;
    for( i = 0; i < *num_groups; i++){
         cci[i] = start_offsets_lens[3*size_new_group*i  + 1];
         //if it is the last group check if it is the smallest group
	 if( (i == *num_groups-1) && flag == 1){
             size = *size_smallest_group;
	 }
	 for( k = 0; k < size-1; k++){
	     if( end_offsets[size_new_group* i + k] == start_offsets_lens[3*size_new_group*i + 3*(k+1)] ){
	         cci[i] += start_offsets_lens[3*size_new_group*i + 3*(k + 1) + 1];
	     }
       	 }
     }

     //get min and max cci
     *min_cci = cci[0];
     *max_cci = cci[0];
     for( i = 1 ; i < *num_groups; i++){
         if(cci[i] > *max_cci){
	     *max_cci = cci[i];
	 }
	 else if(cci[i] < *min_cci){
	     *min_cci = cci[i];
	 }
     }

     free (cci);
     return OMPI_SUCCESS;
}

int mca_common_ompio_finalize_split(ompio_file_t *fh,
                                    int size_new_group,
                                    int size_last_group)
{
   //based on new group and last group finalize f_procs_per_group and f_procs_in_group

    int i = 0;
    int j = 0;
    int k = 0;

    for( i = 0; i < fh->f_init_procs_per_group ; i++){

        if( fh->f_rank == fh->f_init_procs_in_group[i]){
             if( i >= fh->f_init_procs_per_group - size_last_group ){
	         fh->f_procs_per_group = size_last_group;
	     }
             else{
	         fh->f_procs_per_group = size_new_group;
	     }
        }
    }


    fh->f_procs_in_group = (int*)malloc (fh->f_procs_per_group * sizeof(int));
    if (NULL == fh->f_procs_in_group) {
        opal_output (1, "OUT OF MEMORY\n");
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    for( i = 0; i < fh->f_init_procs_per_group ; i++){
        if( fh->f_rank == fh->f_init_procs_in_group[i]){
            if( i >= fh->f_init_procs_per_group - size_last_group ){
	       //distribution of last group
	       for( j = 0; j < fh->f_procs_per_group; j++){
	           fh->f_procs_in_group[j] = fh->f_init_procs_in_group[fh->f_init_procs_per_group - size_last_group + j];
	       }
	    }
	    else{
	         //distribute all other groups
		 for( j = 0 ; j < fh->f_init_procs_per_group; j = j + size_new_group){
	             if(i >= j && i < j+size_new_group  ){
                         for( k = 0; k < fh->f_procs_per_group ; k++){
	                    fh->f_procs_in_group[k] = fh->f_init_procs_in_group[j+k];
			 }
		     }
		 }
	    }

        }
    }

    return OMPI_SUCCESS;
}

int mca_common_ompio_prepare_to_group(ompio_file_t *fh,
		                      OMPI_MPI_OFFSET_TYPE **start_offsets_lens,
				      OMPI_MPI_OFFSET_TYPE **end_offsets, // need it?
				      OMPI_MPI_OFFSET_TYPE **aggr_bytes_per_group,
				      OMPI_MPI_OFFSET_TYPE *bytes_per_group,
                                      int **decision_list,
		                      size_t bytes_per_proc,
				      int *is_aggregator,
				      int *ompio_grouping_flag)
{

    OMPI_MPI_OFFSET_TYPE start_offset_len[3] = {0};
    OMPI_MPI_OFFSET_TYPE *aggr_bytes_per_group_tmp = NULL;
    OMPI_MPI_OFFSET_TYPE *start_offsets_lens_tmp = NULL;
    OMPI_MPI_OFFSET_TYPE *end_offsets_tmp = NULL;
    int *decision_list_tmp = NULL;

    int i = 0;
    int j = 0;
    int k = 0;
    int merge_count = 0;
    int split_count = 0; //not req?
    int retain_as_is_count = 0; //not req?
    int ret=OMPI_SUCCESS;

    //Store start offset and length in an array //also add bytes per process
    if(NULL == fh->f_decoded_iov){
         start_offset_len[0] = 0;
         start_offset_len[1] = 0;
    }
    else{
         start_offset_len[0] = (OMPI_MPI_OFFSET_TYPE) fh->f_decoded_iov[0].iov_base;
         start_offset_len[1] = fh->f_decoded_iov[0].iov_len;
    }
    start_offset_len[2] = bytes_per_proc;
    start_offsets_lens_tmp = (OMPI_MPI_OFFSET_TYPE* )malloc (3 * fh->f_init_procs_per_group * sizeof(OMPI_MPI_OFFSET_TYPE));
    if (NULL == start_offsets_lens_tmp) {
        opal_output (1, "OUT OF MEMORY\n");
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    //Gather start offsets across processes in a group on aggregator
    ret = ompi_fcoll_base_coll_allgather_array (start_offset_len,
                                           3,
                                           OMPI_OFFSET_DATATYPE,
                                           start_offsets_lens_tmp,
                                           3,
                                           OMPI_OFFSET_DATATYPE,
                                           0,
                                           fh->f_init_procs_in_group,
                                           fh->f_init_procs_per_group,
                                           fh->f_comm);
    if ( OMPI_SUCCESS != ret ) {
        opal_output (1, "mca_common_ompio_prepare_to_group: error in ompi_fcoll_base_coll_allgather_array\n");
        goto exit;
    }
    end_offsets_tmp = (OMPI_MPI_OFFSET_TYPE* )malloc (fh->f_init_procs_per_group * sizeof(OMPI_MPI_OFFSET_TYPE));
    if (NULL == end_offsets_tmp) {
        opal_output (1, "OUT OF MEMORY\n");
        goto exit;
    }
    for( k = 0 ; k < fh->f_init_procs_per_group; k++){
        end_offsets_tmp[k] = start_offsets_lens_tmp[3*k] + start_offsets_lens_tmp[3*k+1];
    }
    //Every process has the total bytes written in its group
    for(j = 0; j < fh->f_init_procs_per_group; j++){
        *bytes_per_group = *bytes_per_group + start_offsets_lens_tmp[3*j+2];
    }

    *start_offsets_lens = &start_offsets_lens_tmp[0];
    *end_offsets = &end_offsets_tmp[0];


    for( j = 0 ; j < fh->f_init_num_aggrs ; j++){
        if(fh->f_rank == fh->f_init_aggr_list[j])
           *is_aggregator = 1;
    }
    //Decide groups going in for a merge or a split
    //Merge only if the groups are consecutive
    if(*is_aggregator == 1){
       aggr_bytes_per_group_tmp = (OMPI_MPI_OFFSET_TYPE*)malloc (fh->f_init_num_aggrs * sizeof(OMPI_MPI_OFFSET_TYPE));
       if (NULL == aggr_bytes_per_group_tmp) {
          opal_output (1, "OUT OF MEMORY\n");
          ret = OMPI_ERR_OUT_OF_RESOURCE;
          free(end_offsets_tmp);
          goto exit;
       }
    decision_list_tmp = (int* )malloc (fh->f_init_num_aggrs * sizeof(int));
    if (NULL == decision_list_tmp) {
        opal_output (1, "OUT OF MEMORY\n");
        ret = OMPI_ERR_OUT_OF_RESOURCE;
        free(end_offsets_tmp);
        if (NULL != aggr_bytes_per_group_tmp) {
            free(aggr_bytes_per_group_tmp);
        }
        goto exit;
    }
    //Communicate bytes per group between all aggregators
    ret = ompi_fcoll_base_coll_allgather_array (bytes_per_group,
                                           1,
                                           OMPI_OFFSET_DATATYPE,
                                           aggr_bytes_per_group_tmp,
                                           1,
                                           OMPI_OFFSET_DATATYPE,
                                           0,
                                           fh->f_init_aggr_list,
                                           fh->f_init_num_aggrs,
                                           fh->f_comm);
    if ( OMPI_SUCCESS != ret ) {
        opal_output (1, "mca_common_ompio_prepare_to_group: error in ompi_fcoll_base_coll_allgather_array 2\n");
        free(decision_list_tmp);
        goto exit;
    }
    
    for( i = 0; i < fh->f_init_num_aggrs; i++){
       if((size_t)(aggr_bytes_per_group_tmp[i])>
          (size_t)OMPIO_MCA_GET(fh, bytes_per_agg)){
          decision_list_tmp[i] = OMPIO_SPLIT;
          split_count++;
       }
       else if((size_t)(aggr_bytes_per_group_tmp[i])<
               (size_t)OMPIO_MCA_GET(fh, bytes_per_agg)){
            decision_list_tmp[i] = OMPIO_MERGE;
            merge_count++;
       }
       else{
	   decision_list_tmp[i] = OMPIO_RETAIN;
	   retain_as_is_count++;
	   }
    }

    *aggr_bytes_per_group = &aggr_bytes_per_group_tmp[0];
    //Go through the decision list to see if non consecutive
    //processes intend to merge, if yes retain original grouping
    for( i = 0; i < fh->f_init_num_aggrs ; i++){
        if(decision_list_tmp[i] == OMPIO_MERGE){
	    if( (i == 0) &&
	        (decision_list_tmp[i+1] != OMPIO_MERGE)){ //first group
		    decision_list_tmp[i] = OMPIO_RETAIN;
            }
	    else if( (i == fh->f_init_num_aggrs-1) &&
	             (decision_list_tmp[i-1] != OMPIO_MERGE)){

	        decision_list_tmp[i] = OMPIO_RETAIN;
	    }
	    else if(!((decision_list_tmp[i-1] == OMPIO_MERGE) ||
                      (decision_list_tmp[i+1] == OMPIO_MERGE))){

		 decision_list_tmp[i] = OMPIO_RETAIN;
	    }
        }
    }

    //Set the flag as per the decision list
    for( i = 0 ; i < fh->f_init_num_aggrs; i++){
        if((decision_list_tmp[i] == OMPIO_MERGE)&&
	   (fh->f_rank == fh->f_init_aggr_list[i]))
           *ompio_grouping_flag = OMPIO_MERGE;

       	if((decision_list_tmp[i] == OMPIO_SPLIT)&&
	   (fh->f_rank == fh->f_init_aggr_list[i]))
           *ompio_grouping_flag = OMPIO_SPLIT;

	if((decision_list_tmp[i] == OMPIO_RETAIN)&&
	   (fh->f_rank == fh->f_init_aggr_list[i]))
           *ompio_grouping_flag = OMPIO_RETAIN;
    }

    //print decision list of aggregators
    /*printf("RANK%d  : Printing decsion list   : \n",fh->f_rank);
    for( i = 0; i < fh->f_init_num_aggrs; i++){
        if(decision_list_tmp[i] == OMPIO_MERGE)
            printf("MERGE,");
        else if(decision_list_tmp[i] == OMPIO_SPLIT)
            printf("SPLIT, ");
	else if(decision_list_tmp[i] == OMPIO_RETAIN)
	    printf("RETAIN, " );
    }
    printf("\n\n");
    */
    *decision_list = &decision_list_tmp[0];
    }
    //Communicate flag to all group members
    ret = ompi_fcoll_base_coll_bcast_array (ompio_grouping_flag,
                                       1,
                                       MPI_INT,
                                       0,
                                       fh->f_init_procs_in_group,
                                       fh->f_init_procs_per_group,
                                       fh->f_comm);   

exit:
    /* Do not free aggr_bytes_per_group_tmp, 
    ** start_offsets_lens_tmp, and end_offsets_tmp
    ** here. The memory is released in the layer above.
    */


    return ret;
}

/*
** This is the actual formula of the cost function from the paper.
** One change made here is to use floating point values for
** all parameters, since the ceil() function leads to sometimes
** unexpected jumps in the execution time. Using float leads to 
** more consistent predictions for the no. of aggregators.
*/
static double cost_calc (int P, int P_a, size_t d_p, size_t b_c, int dim )
{
    float  n_as=1.0, m_s=1.0, n_s=1.0;
    float  n_ar=1.0;
    double t_send, t_recv, t_tot;

    /* LogGP parameters based on DDR InfiniBand values */
    double L=.00000184;
    double o=.00000149;
    double g=.0000119;
    double G=.00000000067;
    
    long file_domain = (P * d_p) / P_a;
    float n_r = (float)file_domain/(float) b_c;
    
    switch (dim) {
	case DIM1:
	{
	    if( d_p > b_c ){
		//printf("case 1\n");
		n_ar = 1;
		n_as = 1;
		m_s = b_c;
		n_s = (float)d_p/(float)b_c;
	    }
	    else {
		n_ar = (float)b_c/(float)d_p;
		n_as = 1;
		m_s = d_p;
		n_s = 1;
	    }
	    break;
	}	  
	case DIM2:
	{
	    int P_x, P_y, c;
	    
	    P_x = P_y = (int) sqrt(P);
	    c = (float) P_a / (float)P_x;
	    
	    n_ar = (float) P_y;
	    n_as = (float) c;
	    if ( d_p > (P_a*b_c/P )) {
		m_s = fmin(b_c / P_y, d_p);
	    }
	    else {
		m_s = fmin(d_p * P_x / P_a, d_p);
	    }
	    break;	  
	}
	default :
	    printf("stop putting random values\n");
	    break;
    } 
    
    n_s = (float) d_p / (float)(n_as * m_s);
    
    if( m_s < 33554432) {
	g = .00000108;
    }	
    t_send = n_s * (L + 2 * o + (n_as -1) * g + (m_s - 1) * n_as * G);
    t_recv=  n_r * (L + 2 * o + (n_ar -1) * g + (m_s - 1) * n_ar * G);;
    t_tot = t_send + t_recv;
    
    return t_tot;
}
    
