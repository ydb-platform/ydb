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
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * Copyright (c) 2018      Cisco Systems, Inc.  All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "fcoll_dynamic_gen2.h"

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/mca/fcoll/fcoll.h"
#include "ompi/mca/fcoll/base/fcoll_base_coll_array.h"
#include "ompi/mca/common/ompio/common_ompio.h"
#include "ompi/mca/io/io.h"
#include "math.h"
#include "ompi/mca/pml/pml.h"
#include <unistd.h>


#define DEBUG_ON 0
#define FCOLL_DYNAMIC_GEN2_SHUFFLE_TAG   123
#define INIT_LEN 10

/*Used for loading file-offsets per aggregator*/
typedef struct mca_io_ompio_local_io_array{
    OMPI_MPI_OFFSET_TYPE offset;
    MPI_Aint             length;
    int                  process_id;
}mca_io_ompio_local_io_array;

typedef struct mca_io_ompio_aggregator_data {
    int *disp_index, *sorted, *fview_count, n;
    int *max_disp_index;
    int **blocklen_per_process;
    MPI_Aint **displs_per_process, total_bytes, bytes_per_cycle, total_bytes_written;
    MPI_Comm comm;
    char *buf, *global_buf, *prev_global_buf;
    ompi_datatype_t **recvtype, **prev_recvtype;
    struct iovec *global_iov_array;
    int current_index, current_position;
    int bytes_to_write_in_cycle, bytes_remaining, procs_per_group;    
    int *procs_in_group, iov_index;
    int bytes_sent, prev_bytes_sent;
    struct iovec *decoded_iov;
    int bytes_to_write, prev_bytes_to_write;
    mca_common_ompio_io_array_t *io_array, *prev_io_array;
    int num_io_entries, prev_num_io_entries;
} mca_io_ompio_aggregator_data;


#define SWAP_REQUESTS(_r1,_r2) { \
    ompi_request_t **_t=_r1;     \
    _r1=_r2;                     \
    _r2=_t;}

#define SWAP_AGGR_POINTERS(_aggr,_num) {                        \
    int _i;                                                     \
    char *_t;                                                   \
    for (_i=0; _i<_num; _i++ ) {                                \
        _aggr[_i]->prev_io_array=_aggr[_i]->io_array;             \
        _aggr[_i]->prev_num_io_entries=_aggr[_i]->num_io_entries; \
        _aggr[_i]->prev_bytes_sent=_aggr[_i]->bytes_sent;         \
        _aggr[_i]->prev_bytes_to_write=_aggr[_i]->bytes_to_write; \
        _t=_aggr[_i]->prev_global_buf;                            \
        _aggr[_i]->prev_global_buf=_aggr[_i]->global_buf;         \
        _aggr[_i]->global_buf=_t;                                 \
        _t=(char *)_aggr[_i]->recvtype;                           \
        _aggr[_i]->recvtype=_aggr[_i]->prev_recvtype;             \
        _aggr[_i]->prev_recvtype=(ompi_datatype_t **)_t;          }                                                             \
}



static int shuffle_init ( int index, int cycles, int aggregator, int rank, 
                          mca_io_ompio_aggregator_data *data, 
                          ompi_request_t **reqs );
static int write_init (ompio_file_t *fh, int aggregator, mca_io_ompio_aggregator_data *aggr_data, int write_chunksize );

int mca_fcoll_dynamic_gen2_break_file_view ( struct iovec *decoded_iov, int iov_count, 
                                        struct iovec *local_iov_array, int local_count, 
                                        struct iovec ***broken_decoded_iovs, int **broken_iov_counts,
                                        struct iovec ***broken_iov_arrays, int **broken_counts, 
                                        MPI_Aint **broken_total_lengths,
                                        int stripe_count, int stripe_size); 


int mca_fcoll_dynamic_gen2_get_configuration (ompio_file_t *fh, int *dynamic_gen2_num_io_procs, 
                                              int **ret_aggregators);


static int local_heap_sort (mca_io_ompio_local_io_array *io_array,
			    int num_entries,
			    int *sorted);

int mca_fcoll_dynamic_gen2_split_iov_array ( ompio_file_t *fh, mca_common_ompio_io_array_t *work_array,
                                             int num_entries, int *last_array_pos, int *last_pos_in_field,
                                             int chunk_size );


int mca_fcoll_dynamic_gen2_file_write_all (ompio_file_t *fh,
                                      const void *buf,
                                      int count,
                                      struct ompi_datatype_t *datatype,
                                      ompi_status_public_t *status)
{
    int index = 0;
    int cycles = 0;
    int ret =0, l, i, j, bytes_per_cycle;
    uint32_t iov_count = 0;
    struct iovec *decoded_iov = NULL;
    struct iovec *local_iov_array=NULL;
    uint32_t total_fview_count = 0;
    int local_count = 0;
    ompi_request_t **reqs1=NULL,**reqs2=NULL;
    ompi_request_t **curr_reqs=NULL,**prev_reqs=NULL;
    mca_io_ompio_aggregator_data **aggr_data=NULL;
    
    int *displs = NULL;
    int dynamic_gen2_num_io_procs;
    size_t max_data = 0;
    MPI_Aint *total_bytes_per_process = NULL;
    
    struct iovec **broken_iov_arrays=NULL;
    struct iovec **broken_decoded_iovs=NULL;
    int *broken_counts=NULL;
    int *broken_iov_counts=NULL;
    MPI_Aint *broken_total_lengths=NULL;

    int *aggregators=NULL;
    int write_chunksize, *result_counts=NULL;
    
    
#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
    double write_time = 0.0, start_write_time = 0.0, end_write_time = 0.0;
    double comm_time = 0.0, start_comm_time = 0.0, end_comm_time = 0.0;
    double exch_write = 0.0, start_exch = 0.0, end_exch = 0.0;
    mca_common_ompio_print_entry nentry;
#endif
    
    
    /**************************************************************************
     ** 1.  In case the data is not contigous in memory, decode it into an iovec
     **************************************************************************/
    bytes_per_cycle = fh->f_bytes_per_agg;

    /* since we want to overlap 2 iterations, define the bytes_per_cycle to be half of what
       the user requested */
    bytes_per_cycle =bytes_per_cycle/2;
        
    ret =   mca_common_ompio_decode_datatype ((struct ompio_file_t *) fh,
                                              datatype,
                                              count,
                                              buf,
                                              &max_data,
                                              &decoded_iov,
                                              &iov_count);
    if (OMPI_SUCCESS != ret ){
        goto exit;
    }

    if ( MPI_STATUS_IGNORE != status ) {
	status->_ucount = max_data;
    }
    
    /* difference to the first generation of this function:
    ** dynamic_gen2_num_io_procs should be the number of io_procs per group
    ** consequently.Initially, we will have only 1 group.
    */
    if ( fh->f_stripe_count > 1 ) {
        dynamic_gen2_num_io_procs =  fh->f_stripe_count;
    }
    else {
        dynamic_gen2_num_io_procs = fh->f_get_mca_parameter_value ( "num_aggregators", strlen ("num_aggregators"));
        if ( OMPI_ERR_MAX == dynamic_gen2_num_io_procs ) {
            ret = OMPI_ERROR;
            goto exit;
        }
    }


    if ( fh->f_stripe_size == 0 ) {
        // EDGAR: just a quick heck for testing 
        fh->f_stripe_size = 65536;
    }
    if ( -1 == mca_fcoll_dynamic_gen2_write_chunksize  ) {
        write_chunksize = fh->f_stripe_size;
    }
    else {
        write_chunksize = mca_fcoll_dynamic_gen2_write_chunksize;
    }


    ret = mca_fcoll_dynamic_gen2_get_configuration (fh, &dynamic_gen2_num_io_procs, &aggregators);
    if (OMPI_SUCCESS != ret){
	goto exit;
    }
    
    aggr_data = (mca_io_ompio_aggregator_data **) malloc ( dynamic_gen2_num_io_procs * 
                                                           sizeof(mca_io_ompio_aggregator_data*));

    for ( i=0; i< dynamic_gen2_num_io_procs; i++ ) {
        // At this point we know the number of aggregators. If there is a correlation between
        // number of aggregators and number of IO nodes, we know how many aggr_data arrays we need
        // to allocate.
        aggr_data[i] = (mca_io_ompio_aggregator_data *) calloc ( 1, sizeof(mca_io_ompio_aggregator_data));
        aggr_data[i]->procs_per_group = fh->f_procs_per_group;
        aggr_data[i]->procs_in_group  = fh->f_procs_in_group;
        aggr_data[i]->comm = fh->f_comm;
        aggr_data[i]->buf  = (char *)buf;             // should not be used in the new version.
    }

    /*********************************************************************
     *** 2. Generate the local offsets/lengths array corresponding to
     ***    this write operation
     ********************************************************************/
    ret = fh->f_generate_current_file_view( (struct ompio_file_t *) fh,
					    max_data,
					    &local_iov_array,
					    &local_count);
    if (ret != OMPI_SUCCESS){
	goto exit;
    }

    /*************************************************************************
     ** 2b. Separate the local_iov_array entries based on the number of aggregators
     *************************************************************************/
    // broken_iov_arrays[0] contains broken_counts[0] entries to aggregator 0,
    // broken_iov_arrays[1] contains broken_counts[1] entries to aggregator 1, etc.
    ret = mca_fcoll_dynamic_gen2_break_file_view ( decoded_iov, iov_count, 
                                              local_iov_array, local_count, 
                                              &broken_decoded_iovs, &broken_iov_counts,
                                              &broken_iov_arrays, &broken_counts, 
                                              &broken_total_lengths,
                                              dynamic_gen2_num_io_procs,  fh->f_stripe_size); 


    /**************************************************************************
     ** 3. Determine the total amount of data to be written and no. of cycles
     **************************************************************************/
#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
    start_comm_time = MPI_Wtime();
#endif
    if ( 1 == mca_fcoll_dynamic_gen2_num_groups ) {
        ret = fh->f_comm->c_coll->coll_allreduce (MPI_IN_PLACE,
                                                  broken_total_lengths,
                                                  dynamic_gen2_num_io_procs,
                                                  MPI_LONG,
                                                  MPI_SUM,
                                                  fh->f_comm,
                                                  fh->f_comm->c_coll->coll_allreduce_module);
        if( OMPI_SUCCESS != ret){
            goto exit;
        }
    }
    else {
        total_bytes_per_process = (MPI_Aint*)malloc
            (dynamic_gen2_num_io_procs * fh->f_procs_per_group*sizeof(MPI_Aint));
        if (NULL == total_bytes_per_process) {
            opal_output (1, "OUT OF MEMORY\n");
            ret = OMPI_ERR_OUT_OF_RESOURCE;
            goto exit;
        }
    
        ret = ompi_fcoll_base_coll_allgather_array (broken_total_lengths,
                                                    dynamic_gen2_num_io_procs,
                                                    MPI_LONG,
                                                    total_bytes_per_process,
                                                    dynamic_gen2_num_io_procs,
                                                    MPI_LONG,
                                                    0,
                                                    fh->f_procs_in_group,
                                                    fh->f_procs_per_group,
                                                    fh->f_comm);
        if( OMPI_SUCCESS != ret){
            goto exit;
        }
        for ( i=0; i<dynamic_gen2_num_io_procs; i++ ) {
            broken_total_lengths[i] = 0;
            for (j=0 ; j<fh->f_procs_per_group ; j++) {
                broken_total_lengths[i] += total_bytes_per_process[j*dynamic_gen2_num_io_procs + i];
            }
        }
        if (NULL != total_bytes_per_process) {
            free (total_bytes_per_process);
            total_bytes_per_process = NULL;
        }
    }
#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
    end_comm_time = MPI_Wtime();
    comm_time += (end_comm_time - start_comm_time);
#endif

    cycles=0;
    for ( i=0; i<dynamic_gen2_num_io_procs; i++ ) {
#if DEBUG_ON
        printf("%d: Overall broken_total_lengths[%d] = %ld\n", fh->f_rank, i, broken_total_lengths[i]);
#endif
        if ( ceil((double)broken_total_lengths[i]/bytes_per_cycle) > cycles ) {
            cycles = ceil((double)broken_total_lengths[i]/bytes_per_cycle);
        }
    }
    

    result_counts = (int *) malloc ( dynamic_gen2_num_io_procs * fh->f_procs_per_group * sizeof(int) );
    if ( NULL == result_counts ) {
        ret = OMPI_ERR_OUT_OF_RESOURCE;
        goto exit;
    }

#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
    start_comm_time = MPI_Wtime();
#endif
    if ( 1 == mca_fcoll_dynamic_gen2_num_groups ) {
        ret = fh->f_comm->c_coll->coll_allgather(broken_counts,
                                                dynamic_gen2_num_io_procs,
                                                MPI_INT,
                                                result_counts,
                                                dynamic_gen2_num_io_procs,
                                                MPI_INT,
                                                fh->f_comm,
                                                fh->f_comm->c_coll->coll_allgather_module);            
    }
    else {
        ret = ompi_fcoll_base_coll_allgather_array (broken_counts,
                                               dynamic_gen2_num_io_procs,
                                               MPI_INT,
                                               result_counts,
                                               dynamic_gen2_num_io_procs,
                                               MPI_INT,
                                               0,
                                               fh->f_procs_in_group,
                                               fh->f_procs_per_group,
                                               fh->f_comm);
    }
    if( OMPI_SUCCESS != ret){
        goto exit;
    }
#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
    end_comm_time = MPI_Wtime();
    comm_time += (end_comm_time - start_comm_time);
#endif

    /*************************************************************
     *** 4. Allgather the offset/lengths array from all processes
     *************************************************************/
    for ( i=0; i< dynamic_gen2_num_io_procs; i++ ) {
        aggr_data[i]->total_bytes = broken_total_lengths[i];
        aggr_data[i]->decoded_iov = broken_decoded_iovs[i];
        aggr_data[i]->fview_count = (int *) malloc (fh->f_procs_per_group * sizeof (int));
        if (NULL == aggr_data[i]->fview_count) {
            opal_output (1, "OUT OF MEMORY\n");
            ret = OMPI_ERR_OUT_OF_RESOURCE;
            goto exit;
        }
        for ( j=0; j <fh->f_procs_per_group; j++ ) {
            aggr_data[i]->fview_count[j] = result_counts[dynamic_gen2_num_io_procs*j+i];
        }
        displs = (int*) malloc (fh->f_procs_per_group * sizeof (int));
        if (NULL == displs) {
            opal_output (1, "OUT OF MEMORY\n");
            ret = OMPI_ERR_OUT_OF_RESOURCE;
            goto exit;
        }
        
        displs[0] = 0;
        total_fview_count = aggr_data[i]->fview_count[0];
        for (j=1 ; j<fh->f_procs_per_group ; j++) {
            total_fview_count += aggr_data[i]->fview_count[j];
            displs[j] = displs[j-1] + aggr_data[i]->fview_count[j-1];
        }
        
#if DEBUG_ON
        printf("total_fview_count : %d\n", total_fview_count);
        if (aggregators[i] == fh->f_rank) {
            for (j=0 ; j<fh->f_procs_per_group ; i++) {
                printf ("%d: PROCESS: %d  ELEMENTS: %d  DISPLS: %d\n",
                        fh->f_rank,
                        j,
                        aggr_data[i]->fview_count[j],
                        displs[j]);
            }
        }
#endif
    
        /* allocate the global iovec  */
        if (0 != total_fview_count) {
            aggr_data[i]->global_iov_array = (struct iovec*) malloc (total_fview_count *
                                                                     sizeof(struct iovec));
            if (NULL == aggr_data[i]->global_iov_array){
                opal_output(1, "OUT OF MEMORY\n");
                ret = OMPI_ERR_OUT_OF_RESOURCE;
                goto exit;
            }            
        }
    
#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
        start_comm_time = MPI_Wtime();
#endif
        if ( 1 == mca_fcoll_dynamic_gen2_num_groups ) {
            ret = fh->f_comm->c_coll->coll_allgatherv (broken_iov_arrays[i],
                                                      broken_counts[i],
                                                      fh->f_iov_type,
                                                      aggr_data[i]->global_iov_array,
                                                      aggr_data[i]->fview_count,
                                                      displs,
                                                      fh->f_iov_type,
                                                      fh->f_comm,
                                                      fh->f_comm->c_coll->coll_allgatherv_module );
        }
        else {
            ret = ompi_fcoll_base_coll_allgatherv_array (broken_iov_arrays[i],
                                                    broken_counts[i],
                                                    fh->f_iov_type,
                                                    aggr_data[i]->global_iov_array,
                                                    aggr_data[i]->fview_count,
                                                    displs,
                                                    fh->f_iov_type,
                                                    aggregators[i],
                                                    fh->f_procs_in_group,
                                                    fh->f_procs_per_group,
                                                    fh->f_comm);
        }
        if (OMPI_SUCCESS != ret){
            goto exit;
        }
#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
        end_comm_time = MPI_Wtime();
        comm_time += (end_comm_time - start_comm_time);
#endif
        
        /****************************************************************************************
         *** 5. Sort the global offset/lengths list based on the offsets.
         *** The result of the sort operation is the 'sorted', an integer array,
         *** which contains the indexes of the global_iov_array based on the offset.
         *** For example, if global_iov_array[x].offset is followed by global_iov_array[y].offset
         *** in the file, and that one is followed by global_iov_array[z].offset, than
         *** sorted[0] = x, sorted[1]=y and sorted[2]=z;
         ******************************************************************************************/
        if (0 != total_fview_count) {
            aggr_data[i]->sorted = (int *)malloc (total_fview_count * sizeof(int));
            if (NULL == aggr_data[i]->sorted) {
                opal_output (1, "OUT OF MEMORY\n");
                ret = OMPI_ERR_OUT_OF_RESOURCE;
                goto exit;
            }
            ompi_fcoll_base_sort_iovec (aggr_data[i]->global_iov_array, total_fview_count, aggr_data[i]->sorted);
        }
        
        if (NULL != local_iov_array){
            free(local_iov_array);
            local_iov_array = NULL;
        }
        
        if (NULL != displs){
            free(displs);
            displs=NULL;
        }
    
    
#if DEBUG_ON
        if (aggregators[i] == fh->f_rank) {
            uint32_t tv=0;
            for (tv=0 ; tv<total_fview_count ; tv++) {
                printf("%d: OFFSET: %lld   LENGTH: %ld\n",
                       fh->f_rank,
                       aggr_data[i]->global_iov_array[aggr_data[i]->sorted[tv]].iov_base,
                       aggr_data[i]->global_iov_array[aggr_data[i]->sorted[tv]].iov_len);
            }
        }
#endif
        /*************************************************************
         *** 6. Determine the number of cycles required to execute this
         ***    operation
         *************************************************************/
        
        aggr_data[i]->bytes_per_cycle = bytes_per_cycle;
    
        if (aggregators[i] == fh->f_rank) {
            aggr_data[i]->disp_index = (int *)malloc (fh->f_procs_per_group * sizeof (int));
            if (NULL == aggr_data[i]->disp_index) {
                opal_output (1, "OUT OF MEMORY\n");
                ret = OMPI_ERR_OUT_OF_RESOURCE;
                goto exit;
            }
        
            aggr_data[i]->max_disp_index = (int *)calloc (fh->f_procs_per_group,  sizeof (int));
            if (NULL == aggr_data[i]->max_disp_index) {
                opal_output (1, "OUT OF MEMORY\n");
                ret = OMPI_ERR_OUT_OF_RESOURCE;
                goto exit;
            }

            aggr_data[i]->blocklen_per_process = (int **)calloc (fh->f_procs_per_group, sizeof (int*));
            if (NULL == aggr_data[i]->blocklen_per_process) {
                opal_output (1, "OUT OF MEMORY\n");
                ret = OMPI_ERR_OUT_OF_RESOURCE;
                goto exit;
            }
        
            aggr_data[i]->displs_per_process = (MPI_Aint **)calloc (fh->f_procs_per_group, sizeof (MPI_Aint*));
            if (NULL == aggr_data[i]->displs_per_process) {
                opal_output (1, "OUT OF MEMORY\n");
                ret = OMPI_ERR_OUT_OF_RESOURCE;
                goto exit;
            }
        
            
            aggr_data[i]->global_buf       = (char *) malloc (bytes_per_cycle);
            aggr_data[i]->prev_global_buf  = (char *) malloc (bytes_per_cycle);
            if (NULL == aggr_data[i]->global_buf || NULL == aggr_data[i]->prev_global_buf){
                opal_output(1, "OUT OF MEMORY");
                ret = OMPI_ERR_OUT_OF_RESOURCE;
                goto exit;
            }
        
            aggr_data[i]->recvtype = (ompi_datatype_t **) malloc (fh->f_procs_per_group  * 
                                                                  sizeof(ompi_datatype_t *));
            aggr_data[i]->prev_recvtype = (ompi_datatype_t **) malloc (fh->f_procs_per_group  * 
                                                                       sizeof(ompi_datatype_t *));
            if (NULL == aggr_data[i]->recvtype || NULL == aggr_data[i]->prev_recvtype) {
                opal_output (1, "OUT OF MEMORY\n");
                ret = OMPI_ERR_OUT_OF_RESOURCE;
                goto exit;
            }
            for(l=0;l<fh->f_procs_per_group;l++){
                aggr_data[i]->recvtype[l]      = MPI_DATATYPE_NULL;
                aggr_data[i]->prev_recvtype[l] = MPI_DATATYPE_NULL;
            }
        }
    
#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
        start_exch = MPI_Wtime();
#endif
    }    

    reqs1 = (ompi_request_t **)malloc ((fh->f_procs_per_group + 1 )*dynamic_gen2_num_io_procs *sizeof(ompi_request_t *));
    reqs2 = (ompi_request_t **)malloc ((fh->f_procs_per_group + 1 )*dynamic_gen2_num_io_procs *sizeof(ompi_request_t *));
    if ( NULL == reqs1 || NULL == reqs2 ) {
        opal_output (1, "OUT OF MEMORY\n");
        ret = OMPI_ERR_OUT_OF_RESOURCE;
        goto exit;
    }
    for (l=0,i=0; i < dynamic_gen2_num_io_procs; i++ ) {
        for ( j=0; j< (fh->f_procs_per_group+1); j++ ) {
            reqs1[l] = MPI_REQUEST_NULL;
            reqs2[l] = MPI_REQUEST_NULL;
            l++;
        }
    }

    curr_reqs = reqs1;
    prev_reqs = reqs2;
    
    /* Initialize communication for iteration 0 */
    if ( cycles > 0 ) {
        for ( i=0; i<dynamic_gen2_num_io_procs; i++ ) {
            ret = shuffle_init ( 0, cycles, aggregators[i], fh->f_rank, aggr_data[i], 
                                 &curr_reqs[i*(fh->f_procs_per_group + 1)] );
            if ( OMPI_SUCCESS != ret ) {
                goto exit;
            }
        }
    }


    for (index = 1; index < cycles; index++) {
        SWAP_REQUESTS(curr_reqs,prev_reqs);
        SWAP_AGGR_POINTERS(aggr_data,dynamic_gen2_num_io_procs); 

        /* Initialize communication for iteration i */
        for ( i=0; i<dynamic_gen2_num_io_procs; i++ ) {
            ret = shuffle_init ( index, cycles, aggregators[i], fh->f_rank, aggr_data[i], 
                                 &curr_reqs[i*(fh->f_procs_per_group + 1)] );
            if ( OMPI_SUCCESS != ret ) {
                goto exit;
            }
        }

        /* Finish communication for iteration i-1 */
        ret = ompi_request_wait_all ( (fh->f_procs_per_group + 1 )*dynamic_gen2_num_io_procs, 
                                      prev_reqs, MPI_STATUS_IGNORE);
        if (OMPI_SUCCESS != ret){
            goto exit;
        }

        
        /* Write data for iteration i-1 */
        for ( i=0; i<dynamic_gen2_num_io_procs; i++ ) {
#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
            start_write_time = MPI_Wtime();
#endif
            ret = write_init (fh, aggregators[i], aggr_data[i], write_chunksize );
            if (OMPI_SUCCESS != ret){
                goto exit;
            }            
#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
            end_write_time = MPI_Wtime();
            write_time += end_write_time - start_write_time;
#endif
        }
        
    } /* end  for (index = 0; index < cycles; index++) */


    /* Finish communication for iteration i = cycles-1 */
    if ( cycles > 0 ) {
        SWAP_REQUESTS(curr_reqs,prev_reqs);
        SWAP_AGGR_POINTERS(aggr_data,dynamic_gen2_num_io_procs); 
        
        ret = ompi_request_wait_all ( (fh->f_procs_per_group + 1 )*dynamic_gen2_num_io_procs, 
                                      prev_reqs, MPI_STATUS_IGNORE);
        if (OMPI_SUCCESS != ret){
            goto exit;
        }
        
        /* Write data for iteration i=cycles-1 */
        for ( i=0; i<dynamic_gen2_num_io_procs; i++ ) {
#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
            start_write_time = MPI_Wtime();
#endif
            ret = write_init (fh, aggregators[i], aggr_data[i], write_chunksize );
            if (OMPI_SUCCESS != ret){
                goto exit;
            }                    
#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
            end_write_time = MPI_Wtime();
            write_time += end_write_time - start_write_time;
#endif
        }
    }

        
#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
    end_exch = MPI_Wtime();
    exch_write += end_exch - start_exch;
    nentry.time[0] = write_time;
    nentry.time[1] = comm_time;
    nentry.time[2] = exch_write;
    nentry.aggregator = 0;
    for ( i=0; i<dynamic_gen2_num_io_procs; i++ ) {
        if (aggregators[i] == fh->f_rank)
	nentry.aggregator = 1;
    }
    nentry.nprocs_for_coll = dynamic_gen2_num_io_procs;
    if (!mca_common_ompio_full_print_queue(fh->f_coll_write_time)){
        mca_common_ompio_register_print_entry(fh->f_coll_write_time,
                                               nentry);
    }
#endif
    
    
exit :
    
    if ( NULL != aggr_data ) {
        
        for ( i=0; i< dynamic_gen2_num_io_procs; i++ ) {            
            if (aggregators[i] == fh->f_rank) {
                if (NULL != aggr_data[i]->recvtype){
                    for (j =0; j< aggr_data[i]->procs_per_group; j++) {
                        if ( MPI_DATATYPE_NULL != aggr_data[i]->recvtype[j] ) {
                            ompi_datatype_destroy(&aggr_data[i]->recvtype[j]);
                        }
                        if ( MPI_DATATYPE_NULL != aggr_data[i]->prev_recvtype[j] ) {
                            ompi_datatype_destroy(&aggr_data[i]->prev_recvtype[j]);
                        }

                    }
                    free(aggr_data[i]->recvtype);
                    free(aggr_data[i]->prev_recvtype);
                }
                
                free (aggr_data[i]->disp_index);
                free (aggr_data[i]->max_disp_index);
                free (aggr_data[i]->global_buf);
                free (aggr_data[i]->prev_global_buf);
                for(l=0;l<aggr_data[i]->procs_per_group;l++){
                    free (aggr_data[i]->blocklen_per_process[l]);
                    free (aggr_data[i]->displs_per_process[l]);
                }
                
                free (aggr_data[i]->blocklen_per_process);
                free (aggr_data[i]->displs_per_process);
            }
            free (aggr_data[i]->sorted);
            free (aggr_data[i]->global_iov_array);
            free (aggr_data[i]->fview_count);
            free (aggr_data[i]->decoded_iov);
            
            free (aggr_data[i]);
        }
        free (aggr_data);
    }
    free(local_iov_array);
    free(displs);
    free(decoded_iov);
    free(broken_counts);
    free(broken_total_lengths);
    free(broken_iov_counts);
    free(broken_decoded_iovs); // decoded_iov arrays[i] were freed as aggr_data[i]->decoded_iov;
    if ( NULL != broken_iov_arrays ) {
        for (i=0; i<dynamic_gen2_num_io_procs; i++ ) {
            free(broken_iov_arrays[i]);
        }
    }
    free(broken_iov_arrays);
    free(aggregators);
    free(fh->f_procs_in_group);
    fh->f_procs_in_group=NULL;
    fh->f_procs_per_group=0;
    free(reqs1);
    free(reqs2);
    free(result_counts);

     
    return OMPI_SUCCESS;
}


static int write_init (ompio_file_t *fh, int aggregator, mca_io_ompio_aggregator_data *aggr_data, int write_chunksize )
{
    int ret=OMPI_SUCCESS;
    int last_array_pos=0;
    int last_pos=0;
        

    if ( aggregator == fh->f_rank && aggr_data->prev_num_io_entries) {
        while ( aggr_data->prev_bytes_to_write > 0 ) {                    
            aggr_data->prev_bytes_to_write -= mca_fcoll_dynamic_gen2_split_iov_array (fh, aggr_data->prev_io_array, 
                                                                                      aggr_data->prev_num_io_entries, 
                                                                                      &last_array_pos, &last_pos,
                                                                                      write_chunksize );
            if ( 0 >  fh->f_fbtl->fbtl_pwritev (fh)) {
                free ( aggr_data->prev_io_array);
                opal_output (1, "dynamic_gen2_write_all: fbtl_pwritev failed\n");
                ret = OMPI_ERROR;
                goto exit;
            }
        }
        free ( fh->f_io_array );
        free ( aggr_data->prev_io_array);
    } 

exit:

    fh->f_io_array=NULL;
    fh->f_num_of_io_entries=0;
    
    return ret;
}

static int shuffle_init ( int index, int cycles, int aggregator, int rank, mca_io_ompio_aggregator_data *data, 
                          ompi_request_t **reqs )
{
    int bytes_sent = 0;
    int blocks=0, temp_pindex;
    int i, j, l, ret;
    int  entries_per_aggregator=0;
    mca_io_ompio_local_io_array *file_offsets_for_agg=NULL;
    int *sorted_file_offsets=NULL;
    int temp_index=0;
    MPI_Aint *memory_displacements=NULL;
    int *temp_disp_index=NULL;
    MPI_Aint global_count = 0;
    int* blocklength_proc=NULL;
    ptrdiff_t* displs_proc=NULL;

    data->num_io_entries = 0;
    data->bytes_sent = 0;
    data->io_array=NULL;
    /**********************************************************************
     ***  7a. Getting ready for next cycle: initializing and freeing buffers
     **********************************************************************/
    if (aggregator == rank) {
        
        if (NULL != data->recvtype){
            for (i =0; i< data->procs_per_group; i++) {
                if ( MPI_DATATYPE_NULL != data->recvtype[i] ) {
                    ompi_datatype_destroy(&data->recvtype[i]);
                    data->recvtype[i] = MPI_DATATYPE_NULL;
                }
            }
        }
        
        for(l=0;l<data->procs_per_group;l++){
            data->disp_index[l] =  1;

            if(data->max_disp_index[l] == 0) {
                data->blocklen_per_process[l]   = (int *)       calloc (INIT_LEN, sizeof(int));
                data->displs_per_process[l]     = (MPI_Aint *)  calloc (INIT_LEN, sizeof(MPI_Aint));
                if (NULL == data->displs_per_process[l] || NULL == data->blocklen_per_process[l]){
                    opal_output (1, "OUT OF MEMORY for displs\n");
                    ret = OMPI_ERR_OUT_OF_RESOURCE;
                    goto exit;
                }
                data->max_disp_index[l] = INIT_LEN;
            }
            else {
                memset ( data->blocklen_per_process[l], 0, data->max_disp_index[l]*sizeof(int) );
                memset ( data->displs_per_process[l], 0, data->max_disp_index[l]*sizeof(MPI_Aint) );
            }
        }
    } /* (aggregator == rank */
    
    /**************************************************************************
     ***  7b. Determine the number of bytes to be actually written in this cycle
     **************************************************************************/
    int local_cycles= ceil((double)data->total_bytes / data->bytes_per_cycle);
    if ( index  < (local_cycles -1) ) {
        data->bytes_to_write_in_cycle = data->bytes_per_cycle;
    }
    else if ( index == (local_cycles -1)) {
        data->bytes_to_write_in_cycle = data->total_bytes - data->bytes_per_cycle*index ;
    }
    else {
        data->bytes_to_write_in_cycle = 0;
    }
    data->bytes_to_write = data->bytes_to_write_in_cycle;

#if DEBUG_ON
    if (aggregator == rank) {
        printf ("****%d: CYCLE %d   Bytes %lld**********\n",
                rank,
                index,
                data->bytes_to_write_in_cycle);
    }
#endif
    /**********************************************************
     **Gather the Data from all the processes at the writers **
     *********************************************************/
    
#if DEBUG_ON
    printf("bytes_to_write_in_cycle: %ld, cycle : %d\n", data->bytes_to_write_in_cycle,
           index);
#endif
    
    /*****************************************************************
     *** 7c. Calculate how much data will be contributed in this cycle
     ***     by each process
     *****************************************************************/
    
    /* The blocklen and displs calculation only done at aggregators!*/
    while (data->bytes_to_write_in_cycle) {
        
        /* This next block identifies which process is the holder
        ** of the sorted[current_index] element;
        */
        blocks = data->fview_count[0];
        for (j=0 ; j<data->procs_per_group ; j++) {
            if (data->sorted[data->current_index] < blocks) {
                data->n = j;
                break;
            }
            else {
                blocks += data->fview_count[j+1];
            }
        }
        
        if (data->bytes_remaining) {
            /* Finish up a partially used buffer from the previous  cycle */
            
            if (data->bytes_remaining <= data->bytes_to_write_in_cycle) {
                /* The data fits completely into the block */
                if (aggregator == rank) {
                    data->blocklen_per_process[data->n][data->disp_index[data->n] - 1] = data->bytes_remaining;
                    data->displs_per_process[data->n][data->disp_index[data->n] - 1] =
                        (ptrdiff_t)data->global_iov_array[data->sorted[data->current_index]].iov_base +
                        (data->global_iov_array[data->sorted[data->current_index]].iov_len
                         - data->bytes_remaining);
                    
                    data->disp_index[data->n] += 1;

                    /* In this cases the length is consumed so allocating for
                       next displacement and blocklength*/
                    if ( data->disp_index[data->n] == data->max_disp_index[data->n] ) {
                        data->max_disp_index[data->n] *= 2;
                        data->blocklen_per_process[data->n] = (int *) realloc(
                            (void *)data->blocklen_per_process[data->n], 
                            (data->max_disp_index[data->n])*sizeof(int));
                        data->displs_per_process[data->n] = (MPI_Aint *) realloc(
                            (void *)data->displs_per_process[data->n], 
                            (data->max_disp_index[data->n])*sizeof(MPI_Aint));
                    }

                    data->blocklen_per_process[data->n][data->disp_index[data->n]] = 0;
                    data->displs_per_process[data->n][data->disp_index[data->n]] = 0;
                }
                if (data->procs_in_group[data->n] == rank) {
                    bytes_sent += data->bytes_remaining;
                }
                data->current_index ++;
                data->bytes_to_write_in_cycle -= data->bytes_remaining;
                data->bytes_remaining = 0;
            }
            else {
                /* the remaining data from the previous cycle is larger than the
                   data->bytes_to_write_in_cycle, so we have to segment again */
                if (aggregator == rank) {
                    data->blocklen_per_process[data->n][data->disp_index[data->n] - 1] = data->bytes_to_write_in_cycle;
                    data->displs_per_process[data->n][data->disp_index[data->n] - 1] =
                        (ptrdiff_t)data->global_iov_array[data->sorted[data->current_index]].iov_base +
                        (data->global_iov_array[data->sorted[data->current_index]].iov_len
                         - data->bytes_remaining);
                }
                
                if (data->procs_in_group[data->n] == rank) {
                    bytes_sent += data->bytes_to_write_in_cycle;
                }
                data->bytes_remaining -= data->bytes_to_write_in_cycle;
                data->bytes_to_write_in_cycle = 0;
                break;
            }
        }
        else {
            /* No partially used entry available, have to start a new one */
            if (data->bytes_to_write_in_cycle <
                (MPI_Aint) data->global_iov_array[data->sorted[data->current_index]].iov_len) {
                /* This entry has more data than we can sendin one cycle */
                if (aggregator == rank) {
                    data->blocklen_per_process[data->n][data->disp_index[data->n] - 1] = data->bytes_to_write_in_cycle;
                    data->displs_per_process[data->n][data->disp_index[data->n] - 1] =
                        (ptrdiff_t)data->global_iov_array[data->sorted[data->current_index]].iov_base ;
                }
                if (data->procs_in_group[data->n] == rank) {
                    bytes_sent += data->bytes_to_write_in_cycle;
                    
                }
                data->bytes_remaining = data->global_iov_array[data->sorted[data->current_index]].iov_len -
                    data->bytes_to_write_in_cycle;
                data->bytes_to_write_in_cycle = 0;
                break;
            }
            else {
                /* Next data entry is less than data->bytes_to_write_in_cycle */
                if (aggregator == rank) {
                    data->blocklen_per_process[data->n][data->disp_index[data->n] - 1] =
                        data->global_iov_array[data->sorted[data->current_index]].iov_len;
                    data->displs_per_process[data->n][data->disp_index[data->n] - 1] = (ptrdiff_t)
                        data->global_iov_array[data->sorted[data->current_index]].iov_base;
                    
                    data->disp_index[data->n] += 1;

                    /*realloc for next blocklength
                      and assign this displacement and check for next displs as
                      the total length of this entry has been consumed!*/
                    if ( data->disp_index[data->n] == data->max_disp_index[data->n] ) {
                        data->max_disp_index[data->n] *= 2;
                        data->blocklen_per_process[data->n] = (int *) realloc(
                            (void *)data->blocklen_per_process[data->n], 
                            (data->max_disp_index[data->n]*sizeof(int)));
                        data->displs_per_process[data->n] = (MPI_Aint *)realloc(
                            (void *)data->displs_per_process[data->n], 
                            (data->max_disp_index[data->n]*sizeof(MPI_Aint)));
                    }
                    data->blocklen_per_process[data->n][data->disp_index[data->n]] = 0;
                    data->displs_per_process[data->n][data->disp_index[data->n]] = 0;
                }
                if (data->procs_in_group[data->n] == rank) {
                    bytes_sent += data->global_iov_array[data->sorted[data->current_index]].iov_len;
                }
                data->bytes_to_write_in_cycle -=
                    data->global_iov_array[data->sorted[data->current_index]].iov_len;
                data->current_index ++;
            }
        }
    }
    
    
    /*************************************************************************
     *** 7d. Calculate the displacement on where to put the data and allocate
     ***     the recieve buffer (global_buf)
     *************************************************************************/
    if (aggregator == rank) {
        entries_per_aggregator=0;
        for (i=0;i<data->procs_per_group; i++){
            for (j=0;j<data->disp_index[i];j++){
                if (data->blocklen_per_process[i][j] > 0)
                    entries_per_aggregator++ ;
            }
        }
        
#if DEBUG_ON
        printf("%d: cycle: %d, bytes_sent: %d\n ",rank,index,
               bytes_sent);
        printf("%d : Entries per aggregator : %d\n",rank,entries_per_aggregator);
#endif
        
        if (entries_per_aggregator > 0){
            file_offsets_for_agg = (mca_io_ompio_local_io_array *)
                malloc(entries_per_aggregator*sizeof(mca_io_ompio_local_io_array));
            if (NULL == file_offsets_for_agg) {
                opal_output (1, "OUT OF MEMORY\n");
                ret = OMPI_ERR_OUT_OF_RESOURCE;
                goto exit;
            }
            
            sorted_file_offsets = (int *)
                malloc (entries_per_aggregator*sizeof(int));
            if (NULL == sorted_file_offsets){
                opal_output (1, "OUT OF MEMORY\n");
                ret =  OMPI_ERR_OUT_OF_RESOURCE;
                goto exit;
            }
            
            /*Moving file offsets to an IO array!*/
            temp_index = 0;
            
            for (i=0;i<data->procs_per_group; i++){
                for(j=0;j<data->disp_index[i];j++){
                    if (data->blocklen_per_process[i][j] > 0){
                        file_offsets_for_agg[temp_index].length =
                            data->blocklen_per_process[i][j];
                        file_offsets_for_agg[temp_index].process_id = i;
                        file_offsets_for_agg[temp_index].offset =
                            data->displs_per_process[i][j];
                        temp_index++;
                        
#if DEBUG_ON
                        printf("************Cycle: %d,  Aggregator: %d ***************\n",
                               index+1,rank);
                        
                        printf("%d sends blocklen[%d]: %d, disp[%d]: %ld to %d\n",
                               data->procs_in_group[i],j,
                               data->blocklen_per_process[i][j],j,
                               data->displs_per_process[i][j],
                               rank);
#endif
                    }
                }
            }
                
            /* Sort the displacements for each aggregator*/
            local_heap_sort (file_offsets_for_agg,
                             entries_per_aggregator,
                             sorted_file_offsets);
            
            /*create contiguous memory displacements
              based on blocklens on the same displs array
              and map it to this aggregator's actual
              file-displacements (this is in the io-array created above)*/
            memory_displacements = (MPI_Aint *) malloc
                (entries_per_aggregator * sizeof(MPI_Aint));
            
            memory_displacements[sorted_file_offsets[0]] = 0;
            for (i=1; i<entries_per_aggregator; i++){
                memory_displacements[sorted_file_offsets[i]] =
                    memory_displacements[sorted_file_offsets[i-1]] +
                    file_offsets_for_agg[sorted_file_offsets[i-1]].length;
            }
            
            temp_disp_index = (int *)calloc (1, data->procs_per_group * sizeof (int));
            if (NULL == temp_disp_index) {
                opal_output (1, "OUT OF MEMORY\n");
                ret = OMPI_ERR_OUT_OF_RESOURCE;
                goto exit;
            }
            
            /*Now update the displacements array  with memory offsets*/
            global_count = 0;
            for (i=0;i<entries_per_aggregator;i++){
                temp_pindex =
                    file_offsets_for_agg[sorted_file_offsets[i]].process_id;
                data->displs_per_process[temp_pindex][temp_disp_index[temp_pindex]] =
                    memory_displacements[sorted_file_offsets[i]];
                if (temp_disp_index[temp_pindex] < data->disp_index[temp_pindex])
                    temp_disp_index[temp_pindex] += 1;
                else{
                    printf("temp_disp_index[%d]: %d is greater than disp_index[%d]: %d\n",
                           temp_pindex, temp_disp_index[temp_pindex],
                           temp_pindex, data->disp_index[temp_pindex]);
                }
                global_count +=
                    file_offsets_for_agg[sorted_file_offsets[i]].length;
            }
            
            if (NULL != temp_disp_index){
                free(temp_disp_index);
                temp_disp_index = NULL;
            }
            
#if DEBUG_ON
            
            printf("************Cycle: %d,  Aggregator: %d ***************\n",
                   index+1,rank);
            for (i=0;i<data->procs_per_group; i++){
                for(j=0;j<data->disp_index[i];j++){
                    if (data->blocklen_per_process[i][j] > 0){
                        printf("%d sends blocklen[%d]: %d, disp[%d]: %ld to %d\n",
                               data->procs_in_group[i],j,
                               data->blocklen_per_process[i][j],j,
                               data->displs_per_process[i][j],
                               rank);
                        
                    }
                }
            }
            printf("************Cycle: %d,  Aggregator: %d ***************\n",
                   index+1,rank);
            for (i=0; i<entries_per_aggregator;i++){
                printf("%d: OFFSET: %lld   LENGTH: %ld, Mem-offset: %ld\n",
                       file_offsets_for_agg[sorted_file_offsets[i]].process_id,
                       file_offsets_for_agg[sorted_file_offsets[i]].offset,
                       file_offsets_for_agg[sorted_file_offsets[i]].length,
                       memory_displacements[sorted_file_offsets[i]]);
            }
            printf("%d : global_count : %ld, bytes_sent : %d\n",
                   rank,global_count, bytes_sent);
#endif
//#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
//            start_comm_time = MPI_Wtime();
//#endif
            /*************************************************************************
             *** 7e. Perform the actual communication
             *************************************************************************/
            for (i=0;i<data->procs_per_group; i++) {
                size_t datatype_size;
                reqs[i] = MPI_REQUEST_NULL;
                if ( 0 < data->disp_index[i] ) {
                    ompi_datatype_create_hindexed(data->disp_index[i],
                                                  data->blocklen_per_process[i],
                                                  data->displs_per_process[i],
                                                  MPI_BYTE,
                                                  &data->recvtype[i]);
                    ompi_datatype_commit(&data->recvtype[i]);
                    opal_datatype_type_size(&data->recvtype[i]->super, &datatype_size);
                    
                    if (datatype_size){
                        ret = MCA_PML_CALL(irecv(data->global_buf,
                                                 1,
                                                 data->recvtype[i],
                                                 data->procs_in_group[i],
                                                 FCOLL_DYNAMIC_GEN2_SHUFFLE_TAG+index,
                                                 data->comm,
                                                 &reqs[i]));
                        if (OMPI_SUCCESS != ret){
                            goto exit;
                        }
                    }
                }
            }
        }  /* end if (entries_per_aggr > 0 ) */
    }/* end if (aggregator == rank ) */

    if (bytes_sent) {
        size_t remaining      = bytes_sent;
        int block_index       = -1;
        int blocklength_size  = INIT_LEN;

        ptrdiff_t send_mem_address  = (ptrdiff_t) NULL;
        ompi_datatype_t *newType    = MPI_DATATYPE_NULL;
        blocklength_proc            = (int *)       calloc (blocklength_size, sizeof (int));
        displs_proc                 = (ptrdiff_t *) calloc (blocklength_size, sizeof (ptrdiff_t));

        if (NULL == blocklength_proc || NULL == displs_proc ) {
            opal_output (1, "OUT OF MEMORY\n");
            ret = OMPI_ERR_OUT_OF_RESOURCE;
            goto exit;
        }

        while (remaining) {
            block_index++;

            if(0 == block_index) {
                send_mem_address = (ptrdiff_t) (data->decoded_iov[data->iov_index].iov_base) +
                                                data->current_position;
            }
            else {
                // Reallocate more memory if blocklength_size is not enough
                if(0 == block_index % INIT_LEN) {
                    blocklength_size += INIT_LEN;
                    blocklength_proc = (int *)       realloc(blocklength_proc, blocklength_size * sizeof(int));
                    displs_proc      = (ptrdiff_t *) realloc(displs_proc, blocklength_size * sizeof(ptrdiff_t));
                }
                displs_proc[block_index] = (ptrdiff_t) (data->decoded_iov[data->iov_index].iov_base) +
                                                        data->current_position - send_mem_address;
            }

            if (remaining >=
                (data->decoded_iov[data->iov_index].iov_len - data->current_position)) {

                blocklength_proc[block_index] = data->decoded_iov[data->iov_index].iov_len -
                                                data->current_position;
                remaining = remaining -
                            (data->decoded_iov[data->iov_index].iov_len - data->current_position);
                data->iov_index = data->iov_index + 1;
                data->current_position = 0;
            }
            else {
                blocklength_proc[block_index] = remaining;
                data->current_position += remaining;
                remaining = 0;
            }
        }

        data->total_bytes_written += bytes_sent;
        data->bytes_sent = bytes_sent;

        if ( 0 <= block_index ) {
            ompi_datatype_create_hindexed(block_index+1,
                                          blocklength_proc,
                                          displs_proc,
                                          MPI_BYTE,
                                          &newType);
            ompi_datatype_commit(&newType);

            ret = MCA_PML_CALL(isend((char *)send_mem_address,
                                     1,
                                     newType,
                                     aggregator,
                                     FCOLL_DYNAMIC_GEN2_SHUFFLE_TAG+index,
                                     MCA_PML_BASE_SEND_STANDARD,
                                     data->comm,
                                     &reqs[data->procs_per_group]));
            if ( MPI_DATATYPE_NULL != newType ) {
                ompi_datatype_destroy(&newType);
            }
            if (OMPI_SUCCESS != ret){
                goto exit;
            }
        }
    }

    
#if DEBUG_ON
    if (aggregator == rank){
        printf("************Cycle: %d,  Aggregator: %d ***************\n",
               index+1,rank);
        for (i=0 ; i<global_count/4 ; i++)
            printf (" RECV %d \n",((int *)data->global_buf)[i]);
    }
#endif
    
//#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
//    end_comm_time = MPI_Wtime();
//    comm_time += (end_comm_time - start_comm_time);
//#endif
    /**********************************************************
     *** 7f. Create the io array, and pass it to fbtl
     *********************************************************/
    
    if (aggregator == rank && entries_per_aggregator>0) {
        
        
        data->io_array = (mca_common_ompio_io_array_t *) malloc
            (entries_per_aggregator * sizeof (mca_common_ompio_io_array_t));
        if (NULL == data->io_array) {
            opal_output(1, "OUT OF MEMORY\n");
            ret = OMPI_ERR_OUT_OF_RESOURCE;
            goto exit;
        }
        
        data->num_io_entries = 0;
        /*First entry for every aggregator*/
        data->io_array[0].offset =
            (IOVBASE_TYPE *)(intptr_t)file_offsets_for_agg[sorted_file_offsets[0]].offset;
        data->io_array[0].length =
            file_offsets_for_agg[sorted_file_offsets[0]].length;
        data->io_array[0].memory_address =
            data->global_buf+memory_displacements[sorted_file_offsets[0]];
        data->num_io_entries++;
        
        for (i=1;i<entries_per_aggregator;i++){
            /* If the enrties are contiguous merge them,
               else make a new entry */
            if (file_offsets_for_agg[sorted_file_offsets[i-1]].offset +
                file_offsets_for_agg[sorted_file_offsets[i-1]].length ==
                file_offsets_for_agg[sorted_file_offsets[i]].offset){
                data->io_array[data->num_io_entries - 1].length +=
                    file_offsets_for_agg[sorted_file_offsets[i]].length;
            }
            else {
                data->io_array[data->num_io_entries].offset =
                    (IOVBASE_TYPE *)(intptr_t)file_offsets_for_agg[sorted_file_offsets[i]].offset;
                data->io_array[data->num_io_entries].length =
                    file_offsets_for_agg[sorted_file_offsets[i]].length;
                data->io_array[data->num_io_entries].memory_address =
                    data->global_buf+memory_displacements[sorted_file_offsets[i]];
                data->num_io_entries++;
            }
            
        }
        
#if DEBUG_ON
        printf("*************************** %d\n", num_of_io_entries);
        for (i=0 ; i<num_of_io_entries ; i++) {
            printf(" ADDRESS: %p  OFFSET: %ld   LENGTH: %ld\n",
                   io_array[i].memory_address,
                   (ptrdiff_t)io_array[i].offset,
                   io_array[i].length);
        }
        
#endif
    }
        
exit:
    free(sorted_file_offsets);
    free(file_offsets_for_agg);
    free(memory_displacements);
    free(blocklength_proc);
    free(displs_proc);
    
    return OMPI_SUCCESS;
}
    
    

int mca_fcoll_dynamic_gen2_break_file_view ( struct iovec *mem_iov, int mem_count, 
                                        struct iovec *file_iov, int file_count, 
                                        struct iovec ***ret_broken_mem_iovs, int **ret_broken_mem_counts,
                                        struct iovec ***ret_broken_file_iovs, int **ret_broken_file_counts, 
                                        MPI_Aint **ret_broken_total_lengths,
                                        int stripe_count, int stripe_size)
{
    int i, j, ret=OMPI_SUCCESS;
    struct iovec **broken_mem_iovs=NULL; 
    int *broken_mem_counts=NULL;
    struct iovec **broken_file_iovs=NULL; 
    int *broken_file_counts=NULL;
    MPI_Aint *broken_total_lengths=NULL;
    int **block=NULL, **max_lengths=NULL;
    
    broken_mem_iovs  = (struct iovec **) malloc ( stripe_count * sizeof(struct iovec *)); 
    broken_file_iovs = (struct iovec **) malloc ( stripe_count * sizeof(struct iovec *)); 
    if ( NULL == broken_mem_iovs || NULL == broken_file_iovs ) {
        ret = OMPI_ERR_OUT_OF_RESOURCE;
        goto exit;
    }
    for ( i=0; i<stripe_count; i++ ) {
        broken_mem_iovs[i]  = (struct iovec*) calloc (1, sizeof(struct iovec ));
        broken_file_iovs[i] = (struct iovec*) calloc (1, sizeof(struct iovec ));
    }
    
    broken_mem_counts    = (int *) calloc ( stripe_count, sizeof(int));
    broken_file_counts   = (int *) calloc ( stripe_count, sizeof(int));
    broken_total_lengths = (MPI_Aint *) calloc ( stripe_count, sizeof(MPI_Aint));
    if ( NULL == broken_mem_counts || NULL == broken_file_counts ||
         NULL == broken_total_lengths ) {
        ret = OMPI_ERR_OUT_OF_RESOURCE;
        goto exit;
    }

    block       = (int **) calloc ( stripe_count, sizeof(int *));
    max_lengths = (int **) calloc ( stripe_count,  sizeof(int *));
    if ( NULL == block || NULL == max_lengths ) {
        ret = OMPI_ERR_OUT_OF_RESOURCE;
        goto exit;
    }
    
    for ( i=0; i<stripe_count; i++ ){
        block[i]       = (int *) malloc ( 5 * sizeof(int));
        max_lengths[i] = (int *) malloc ( 2 * sizeof(int));
        if ( NULL == block[i] || NULL == max_lengths[i]) {
            ret = OMPI_ERR_OUT_OF_RESOURCE;
            goto exit;
        }
        max_lengths[i][0] = 1;
        max_lengths[i][1] = 1;
        
        for ( j=0; j<5; j++ ) {
            block[i][j]=2;
        }
    }
    
    /* Step 1: separate the local_iov_array per aggregator */
    int owner;
    size_t rest, len, temp_len, blocklen, memlen=0;
    off_t offset, temp_offset, start_offset, memoffset=0;

    i=j=0;

    if ( 0 < mem_count ) {
        memoffset = (off_t ) mem_iov[j].iov_base;
        memlen    = mem_iov[j].iov_len;
    }
    while ( i < file_count) {
        offset = (off_t) file_iov[i].iov_base;
        len    = file_iov[i].iov_len;


#if DEBUG_ON
        printf("%d:file_iov[%d].base=%ld .len=%d\n", rank, i, 
               file_iov[i].iov_base, file_iov[i].iov_len);
#endif
        do {
            owner        = (offset / stripe_size ) % stripe_count;
            start_offset = (offset / stripe_size );
            rest         = (start_offset + 1) * stripe_size - offset;

            if ( len >= rest ) {
                blocklen    = rest;
                temp_offset = offset+rest;
                temp_len    = len - rest;
            }
            else {
                blocklen    = len;
                temp_offset = 0;
                temp_len    = 0;
            }
            
            broken_file_iovs[owner][broken_file_counts[owner]].iov_base = (void *)offset;
            broken_file_iovs[owner][broken_file_counts[owner]].iov_len  = blocklen;
#if DEBUG_ON
            printf("%d: owner=%d b_file_iovs[%d].base=%ld .len=%d \n", rank, owner, 
                   broken_file_counts[owner], 
                   broken_file_iovs[owner][broken_file_counts[owner]].iov_base, 
                   broken_file_iovs[owner][broken_file_counts[owner]].iov_len );
#endif
            do {
                if ( memlen >=  blocklen ) {
                    broken_mem_iovs[owner][broken_mem_counts[owner]].iov_base = (void *) memoffset;
                    broken_mem_iovs[owner][broken_mem_counts[owner]].iov_len  = blocklen;
                    memoffset += blocklen;
                    memlen    -= blocklen;
                    blocklen   = 0;

                    if ( 0 == memlen ) {
                        j++;
                        if ( j < mem_count ) {
                            memoffset = (off_t) mem_iov[j].iov_base;
                            memlen    = mem_iov[j].iov_len;
                        }
                        else
                            break;
                    }
                }                
                else {
                    broken_mem_iovs[owner][broken_mem_counts[owner]].iov_base = (void *) memoffset;
                    broken_mem_iovs[owner][broken_mem_counts[owner]].iov_len  = memlen;
                    blocklen -= memlen;
                    
                    j++;
                    if ( j < mem_count ) {
                        memoffset = (off_t) mem_iov[j].iov_base;
                        memlen    = mem_iov[j].iov_len;
                    }
                    else 
                        break;
                }
#if DEBUG_ON
                printf("%d: owner=%d b_mem_iovs[%d].base=%ld .len=%d\n", rank, owner,
                       broken_mem_counts[owner],
                       broken_mem_iovs[owner][broken_mem_counts[owner]].iov_base,
                       broken_mem_iovs[owner][broken_mem_counts[owner]].iov_len);
#endif

                broken_mem_counts[owner]++;
                if ( broken_mem_counts[owner] >= max_lengths[owner][0] ) {
                    broken_mem_iovs[owner] = (struct iovec*) realloc ( broken_mem_iovs[owner],
                                                                       mem_count * block[owner][0] * 
                                                                       sizeof(struct iovec ));
                    max_lengths[owner][0] = mem_count * block[owner][0];
                    block[owner][0]++;
                }

            } while ( blocklen > 0 );

            broken_file_counts[owner]++;
            if ( broken_file_counts[owner] >= max_lengths[owner][1] ) {
                broken_file_iovs[owner] = (struct iovec*) realloc ( broken_file_iovs[owner],
                                                                    file_count * block[owner][1] * 
                                                                    sizeof(struct iovec ));
                max_lengths[owner][1] = file_count * block[owner][1];
                block[owner][1]++;
            }

            offset = temp_offset;
            len    = temp_len;
        } while( temp_len > 0 );

        i++;
    } 

    
    /* Step 2: recalculating the total lengths per aggregator */
    for ( i=0; i< stripe_count; i++ ) {
        for ( j=0; j<broken_file_counts[i]; j++ ) {
            broken_total_lengths[i] += broken_file_iovs[i][j].iov_len;
        }
#if DEBUG_ON
        printf("%d: broken_total_lengths[%d] = %d\n", rank, i, broken_total_lengths[i]);
#endif
    }

    *ret_broken_mem_iovs      = broken_mem_iovs;
    *ret_broken_mem_counts    = broken_mem_counts;
    *ret_broken_file_iovs     = broken_file_iovs;
    *ret_broken_file_counts   = broken_file_counts;
    *ret_broken_total_lengths = broken_total_lengths;    

    if ( NULL != block) {
        for ( i=0; i<stripe_count; i++ ){
            free (block[i] );
        }
        free ( block);
    }
    if ( NULL != max_lengths) {
        for ( i=0; i<stripe_count; i++ ){
            free (max_lengths[i] );
        }
        free ( max_lengths);
    }

    return ret;

exit:
    free ( broken_mem_iovs);    
    free ( broken_mem_counts);
    free ( broken_file_iovs );
    free ( broken_file_counts);
    free ( broken_total_lengths);

    if ( NULL != block) {
        for ( i=0; i<stripe_count; i++ ){
            free (block[i] );
        }
        free ( block);
    }
    if ( NULL != max_lengths) {
        for ( i=0; i<stripe_count; i++ ){
            free (max_lengths[i] );
        }
        free ( max_lengths);
    }

    *ret_broken_mem_iovs      = NULL;
    *ret_broken_mem_counts    = NULL;
    *ret_broken_file_iovs     = NULL;
    *ret_broken_file_counts   = NULL;
    *ret_broken_total_lengths = NULL;

    return ret;
}


int mca_fcoll_dynamic_gen2_get_configuration (ompio_file_t *fh, int *dynamic_gen2_num_io_procs, int **ret_aggregators)
{
    int *aggregators=NULL;
    int num_io_procs = *dynamic_gen2_num_io_procs;
    int i;

    if ( num_io_procs < 1 ) {
        num_io_procs = fh->f_stripe_count;
        if ( num_io_procs < 1 ) {
            num_io_procs = 1;
        }
    }
    if ( num_io_procs > fh->f_size ) {
        num_io_procs = fh->f_size;
    }

    fh->f_procs_per_group = fh->f_size;
    fh->f_procs_in_group = (int *) malloc ( sizeof(int) * fh->f_size );
    if ( NULL == fh->f_procs_in_group) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }
    for (i=0; i<fh->f_size; i++ ) {
        fh->f_procs_in_group[i]=i;
    }


    aggregators = (int *) malloc ( num_io_procs * sizeof(int));
    if ( NULL == aggregators ) {
        // fh->procs_in_group will be freed with the fh structure. No need to do it here.
        return OMPI_ERR_OUT_OF_RESOURCE;
    }
    for ( i=0; i<num_io_procs; i++ ) {
        aggregators[i] = i * fh->f_size / num_io_procs;
    }

    *dynamic_gen2_num_io_procs = num_io_procs;
    *ret_aggregators = aggregators;

    return OMPI_SUCCESS;
}    
    

int mca_fcoll_dynamic_gen2_split_iov_array ( ompio_file_t *fh, mca_common_ompio_io_array_t *io_array, int num_entries,
                                             int *ret_array_pos, int *ret_pos,  int chunk_size )
{

    int array_pos = *ret_array_pos;
    int pos       = *ret_pos;
    size_t bytes_written = 0;
    size_t bytes_to_write = chunk_size;

    if ( 0 == array_pos && 0 == pos ) {
        fh->f_io_array = (mca_common_ompio_io_array_t *) malloc ( num_entries * sizeof(mca_common_ompio_io_array_t));
        if ( NULL == fh->f_io_array ){
            opal_output (1,"Could not allocate memory\n");
            return -1;
        }
    }
        
    int i=0;
    while (bytes_to_write > 0 ) {
        fh->f_io_array[i].memory_address = &(((char *)io_array[array_pos].memory_address)[pos]);
        fh->f_io_array[i].offset = &(((char *)io_array[array_pos].offset)[pos]);

        if ( (io_array[array_pos].length - pos ) >= bytes_to_write ) {
            fh->f_io_array[i].length = bytes_to_write;
        }
        else {
            fh->f_io_array[i].length = io_array[array_pos].length - pos;
        }

        pos           += fh->f_io_array[i].length;
        bytes_written += fh->f_io_array[i].length;
        bytes_to_write-= fh->f_io_array[i].length;
        i++;

        if ( pos == (int)io_array[array_pos].length ) {
            pos = 0;
            if ((array_pos + 1) < num_entries) {
                array_pos++;
            }
            else {
                break;
            }
        }
    }
    
    fh->f_num_of_io_entries = i;
    *ret_array_pos   = array_pos;
    *ret_pos         = pos;
    return bytes_written;
}

    
static int local_heap_sort (mca_io_ompio_local_io_array *io_array,
			    int num_entries,
			    int *sorted)
{
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

