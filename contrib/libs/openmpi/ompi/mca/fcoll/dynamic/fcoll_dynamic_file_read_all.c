/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2015 University of Houston. All rights reserved.
 * Copyright (c) 2017-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "fcoll_dynamic.h"

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

/*Used for loading file-offsets per aggregator*/
typedef struct mca_io_ompio_local_io_array{
    OMPI_MPI_OFFSET_TYPE offset;
    MPI_Aint             length;
    int                  process_id;
}mca_io_ompio_local_io_array;


static int read_heap_sort (mca_io_ompio_local_io_array *io_array,
                           int num_entries,
                           int *sorted);



int
mca_fcoll_dynamic_file_read_all (ompio_file_t *fh,
                                 void *buf,
                                 int count,
                                 struct ompi_datatype_t *datatype,
                                 ompi_status_public_t *status)
{
    MPI_Aint position = 0;
    MPI_Aint total_bytes = 0;          /* total bytes to be read */
    MPI_Aint bytes_to_read_in_cycle = 0; /* left to be read in a cycle*/
    MPI_Aint bytes_per_cycle = 0;      /* total read in each cycle by each process*/
    int index = 0, ret=OMPI_SUCCESS;
    int cycles = 0;
    int i=0, j=0, l=0;
    int n=0; /* current position in total_bytes_per_process array */
    MPI_Aint bytes_remaining = 0; /* how many bytes have been read from the current
                                     value from total_bytes_per_process */
    int *sorted_file_offsets=NULL, entries_per_aggregator=0;
    int bytes_received = 0;
    int blocks = 0;
    /* iovec structure and count of the buffer passed in */
    uint32_t iov_count = 0;
    struct iovec *decoded_iov = NULL;
    int iov_index = 0;
    size_t current_position = 0;
    struct iovec *local_iov_array=NULL, *global_iov_array=NULL;
    char *receive_buf = NULL;
    MPI_Aint *memory_displacements=NULL;
    /* global iovec at the readers that contain the iovecs created from
       file_set_view */
    uint32_t total_fview_count = 0;
    int local_count = 0;
    int *fview_count = NULL, *disp_index=NULL, *temp_disp_index=NULL;
    int current_index=0, temp_index=0;
    int **blocklen_per_process=NULL;
    MPI_Aint **displs_per_process=NULL;
    char *global_buf = NULL;
    MPI_Aint global_count = 0;
    mca_io_ompio_local_io_array *file_offsets_for_agg=NULL;

    /* array that contains the sorted indices of the global_iov */
    int *sorted = NULL;
    int *displs = NULL;
    int dynamic_num_io_procs;
    size_t max_data = 0;
    MPI_Aint *total_bytes_per_process = NULL;
    ompi_datatype_t **sendtype = NULL;
    MPI_Request *send_req=NULL, recv_req=NULL;
    int my_aggregator =-1;
    bool recvbuf_is_contiguous=false;
    size_t ftype_size;
    ptrdiff_t ftype_extent, lb;


#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
    double read_time = 0.0, start_read_time = 0.0, end_read_time = 0.0;
    double rcomm_time = 0.0, start_rcomm_time = 0.0, end_rcomm_time = 0.0;
    double read_exch = 0.0, start_rexch = 0.0, end_rexch = 0.0;
    mca_common_ompio_print_entry nentry;
#endif

    /**************************************************************************
     ** 1. In case the data is not contigous in memory, decode it into an iovec
     **************************************************************************/

    opal_datatype_type_size ( &datatype->super, &ftype_size );
    opal_datatype_get_extent ( &datatype->super, &lb, &ftype_extent );

    if ( (ftype_extent == (ptrdiff_t) ftype_size)             &&
        opal_datatype_is_contiguous_memory_layout(&datatype->super,1) &&
        0 == lb ) {
        recvbuf_is_contiguous = true;
    }


    if (! recvbuf_is_contiguous ) {
        ret = mca_common_ompio_decode_datatype ((struct ompio_file_t *)fh,
                                                datatype,
                                                count,
                                                buf,
                                                &max_data,
                                                &decoded_iov,
                                                &iov_count);
        if (OMPI_SUCCESS != ret){
            goto exit;
        }
    }
    else {
        max_data = count * datatype->super.size;
    }

    if ( MPI_STATUS_IGNORE != status ) {
        status->_ucount = max_data;
    }

    dynamic_num_io_procs = fh->f_get_mca_parameter_value ( "num_aggregators", strlen ("num_aggregators"));
    if ( OMPI_ERR_MAX == dynamic_num_io_procs ) {
        ret = OMPI_ERROR;
        goto exit;
    }
    ret = mca_common_ompio_set_aggregator_props ((struct ompio_file_t *) fh,
                                                 dynamic_num_io_procs,
                                                 max_data);
    if (OMPI_SUCCESS != ret){
        goto exit;
    }
    my_aggregator = fh->f_procs_in_group[0];

    /**************************************************************************
     ** 2. Determine the total amount of data to be written
     **************************************************************************/
    total_bytes_per_process = (MPI_Aint*)malloc(fh->f_procs_per_group*sizeof(MPI_Aint));
    if (NULL == total_bytes_per_process) {
        opal_output (1, "OUT OF MEMORY\n");
        ret = OMPI_ERR_OUT_OF_RESOURCE;
        goto exit;
    }
#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
    start_rcomm_time = MPI_Wtime();
#endif
    ret = ompi_fcoll_base_coll_allgather_array (&max_data,
                                           1,
                                           MPI_LONG,
                                           total_bytes_per_process,
                                           1,
                                           MPI_LONG,
                                           0,
                                           fh->f_procs_in_group,
                                           fh->f_procs_per_group,
                                           fh->f_comm);
    if (OMPI_SUCCESS != ret){
        goto exit;
    }
#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
    end_rcomm_time = MPI_Wtime();
    rcomm_time += end_rcomm_time - start_rcomm_time;
#endif

    for (i=0 ; i<fh->f_procs_per_group ; i++) {
        total_bytes += total_bytes_per_process[i];
    }

    if (NULL != total_bytes_per_process) {
        free (total_bytes_per_process);
        total_bytes_per_process = NULL;
    }

    /*********************************************************************
     *** 3. Generate the File offsets/lengths corresponding to this write
     ********************************************************************/
    ret = fh->f_generate_current_file_view ((struct ompio_file_t *) fh,
                                            max_data,
                                            &local_iov_array,
                                            &local_count);

    if (ret != OMPI_SUCCESS){
        goto exit;
    }

    /*************************************************************
     *** 4. Allgather the File View information at all processes
     *************************************************************/

    fview_count = (int *) malloc (fh->f_procs_per_group * sizeof (int));
    if (NULL == fview_count) {
        opal_output (1, "OUT OF MEMORY\n");
        ret = OMPI_ERR_OUT_OF_RESOURCE;
        goto exit;
    }
#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
    start_rcomm_time = MPI_Wtime();
#endif
    ret = ompi_fcoll_base_coll_allgather_array (&local_count,
                                           1,
                                           MPI_INT,
                                           fview_count,
                                           1,
                                           MPI_INT,
                                           0,
                                           fh->f_procs_in_group,
                                           fh->f_procs_per_group,
                                           fh->f_comm);
    
    if (OMPI_SUCCESS != ret){
        goto exit;
    }
#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
    end_rcomm_time = MPI_Wtime();
    rcomm_time += end_rcomm_time - start_rcomm_time;
#endif

    displs = (int*)malloc (fh->f_procs_per_group*sizeof(int));
    if (NULL == displs) {
        opal_output (1, "OUT OF MEMORY\n");
        ret = OMPI_ERR_OUT_OF_RESOURCE;
        goto exit;
    }

    displs[0] = 0;
    total_fview_count = fview_count[0];
    for (i=1 ; i<fh->f_procs_per_group ; i++) {
        total_fview_count += fview_count[i];
        displs[i] = displs[i-1] + fview_count[i-1];
    }

#if DEBUG_ON
    if (my_aggregator == fh->f_rank) {
    for (i=0 ; i<fh->f_procs_per_group ; i++) {
    printf ("%d: PROCESS: %d  ELEMENTS: %d  DISPLS: %d\n",
        fh->f_rank,
        i,
        fview_count[i],
        displs[i]);
}
}
#endif

    /* allocate the global iovec  */
    if (0 != total_fview_count) {
        global_iov_array = (struct iovec*)malloc (total_fview_count *
                                                  sizeof(struct iovec));
        if (NULL == global_iov_array) {
            opal_output (1, "OUT OF MEMORY\n");
            ret = OMPI_ERR_OUT_OF_RESOURCE;
            goto exit;
        }
    }
#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
    start_rcomm_time = MPI_Wtime();
#endif
    ret =  ompi_fcoll_base_coll_allgatherv_array (local_iov_array,
                                             local_count,
                                             fh->f_iov_type,
                                             global_iov_array,
                                             fview_count,
                                             displs,
                                             fh->f_iov_type,
                                             0,
                                             fh->f_procs_in_group,
                                             fh->f_procs_per_group,
                                             fh->f_comm);
    
    if (OMPI_SUCCESS != ret){
        goto exit;
    }
#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
    end_rcomm_time = MPI_Wtime();
    rcomm_time += end_rcomm_time - start_rcomm_time;
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
       sorted = (int *)malloc (total_fview_count * sizeof(int));
      if (NULL == sorted) {
            opal_output (1, "OUT OF MEMORY\n");
            ret = OMPI_ERR_OUT_OF_RESOURCE;
            goto exit;
        }
        ompi_fcoll_base_sort_iovec (global_iov_array, total_fview_count, sorted);
    }

    if (NULL != local_iov_array) {
        free (local_iov_array);
        local_iov_array = NULL;
    }

#if DEBUG_ON
    if (my_aggregator == fh->f_rank) {
        for (i=0 ; i<total_fview_count ; i++) {
            printf("%d: OFFSET: %p   LENGTH: %d\n",
                   fh->f_rank,
                   global_iov_array[sorted[i]].iov_base,
                   global_iov_array[sorted[i]].iov_len);
        }
    }
#endif

    /*************************************************************
     *** 6. Determine the number of cycles required to execute this
     ***    operation
     *************************************************************/
    bytes_per_cycle = fh->f_bytes_per_agg;
    cycles = ceil((double)total_bytes/bytes_per_cycle);

    if ( my_aggregator == fh->f_rank) {
      disp_index = (int *)malloc (fh->f_procs_per_group * sizeof (int));
      if (NULL == disp_index) {
            opal_output (1, "OUT OF MEMORY\n");
            ret = OMPI_ERR_OUT_OF_RESOURCE;
            goto exit;
        }

        blocklen_per_process = (int **)malloc (fh->f_procs_per_group * sizeof (int*));
        if (NULL == blocklen_per_process) {
            opal_output (1, "OUT OF MEMORY\n");
            ret = OMPI_ERR_OUT_OF_RESOURCE;
            goto exit;
        }

        displs_per_process = (MPI_Aint **)malloc (fh->f_procs_per_group * sizeof (MPI_Aint*));
        if (NULL == displs_per_process){
            opal_output (1, "OUT OF MEMORY\n");
            ret = OMPI_ERR_OUT_OF_RESOURCE;
            goto exit;
        }

        for (i=0;i<fh->f_procs_per_group;i++){
            blocklen_per_process[i] = NULL;
            displs_per_process[i] = NULL;
        }

	send_req = (MPI_Request *) malloc (fh->f_procs_per_group * sizeof(MPI_Request));
	if (NULL == send_req){
	    opal_output ( 1, "OUT OF MEMORY\n");
	    ret = OMPI_ERR_OUT_OF_RESOURCE;
	    goto exit;
	}

	global_buf = (char *) malloc (bytes_per_cycle);
	if (NULL == global_buf){
	    opal_output(1, "OUT OF MEMORY\n");
	    ret = OMPI_ERR_OUT_OF_RESOURCE;
	    goto exit;
	}

	sendtype = (ompi_datatype_t **) malloc (fh->f_procs_per_group * sizeof(ompi_datatype_t *));
	if (NULL == sendtype) {
            opal_output (1, "OUT OF MEMORY\n");
	    ret = OMPI_ERR_OUT_OF_RESOURCE;
	    goto exit;
	}

	for(l=0;l<fh->f_procs_per_group;l++){
            sendtype[l] = MPI_DATATYPE_NULL;
	}
    }




#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
    start_rexch = MPI_Wtime();
#endif
    n = 0;
    bytes_remaining = 0;
    current_index = 0;

    for (index = 0; index < cycles; index++) {
        /**********************************************************************
         ***  7a. Getting ready for next cycle: initializing and freeing buffers
	 **********************************************************************/
        if (my_aggregator == fh->f_rank) {
             if (NULL != fh->f_io_array) {
                free (fh->f_io_array);
                fh->f_io_array = NULL;
            }
            fh->f_num_of_io_entries = 0;

            if (NULL != sendtype){
                for (i =0; i< fh->f_procs_per_group; i++) {
		    if ( MPI_DATATYPE_NULL != sendtype[i] ) {
                        ompi_datatype_destroy(&sendtype[i]);
                        sendtype[i] = MPI_DATATYPE_NULL;
                    }
		}
            }

            for(l=0;l<fh->f_procs_per_group;l++){
                disp_index[l] =  1;

                if (NULL != blocklen_per_process[l]){
                    free(blocklen_per_process[l]);
                    blocklen_per_process[l] = NULL;
                }
                if (NULL != displs_per_process[l]){
                    free(displs_per_process[l]);
                    displs_per_process[l] = NULL;
                }
                blocklen_per_process[l] = (int *) calloc (1, sizeof(int));
                if (NULL == blocklen_per_process[l]) {
                    opal_output (1, "OUT OF MEMORY for blocklen\n");
                    ret = OMPI_ERR_OUT_OF_RESOURCE;
                    goto exit;
                }
                displs_per_process[l] = (MPI_Aint *) calloc (1, sizeof(MPI_Aint));
                if (NULL == displs_per_process[l]){
                    opal_output (1, "OUT OF MEMORY for displs\n");
                    ret = OMPI_ERR_OUT_OF_RESOURCE;
                    goto exit;
                }
            }

            if (NULL != sorted_file_offsets){
                free(sorted_file_offsets);
                sorted_file_offsets = NULL;
            }

            if(NULL != file_offsets_for_agg){
                free(file_offsets_for_agg);
                file_offsets_for_agg = NULL;
            }
            if (NULL != memory_displacements){
                free(memory_displacements);
                memory_displacements = NULL;
            }
        }  /* (my_aggregator == fh->f_rank */

        /**************************************************************************
         ***  7b. Determine the number of bytes to be actually read in this cycle
	 **************************************************************************/
        if (cycles-1 == index) {
            bytes_to_read_in_cycle = total_bytes - bytes_per_cycle*index;
        }
        else {
            bytes_to_read_in_cycle = bytes_per_cycle;
        }

#if DEBUG_ON
        if (my_aggregator == fh->f_rank) {
            printf ("****%d: CYCLE %d   Bytes %d**********\n",
                    fh->f_rank,
                    index,
                    bytes_to_write_in_cycle);
        }
#endif

        /*****************************************************************
         *** 7c. Calculate how much data will be contributed in this cycle
	 ***     by each process
         *****************************************************************/
        bytes_received = 0;

        while (bytes_to_read_in_cycle) {
            /* This next block identifies which process is the holder
            ** of the sorted[current_index] element;
            */
            blocks = fview_count[0];
            for (j=0 ; j<fh->f_procs_per_group ; j++) {
                if (sorted[current_index] < blocks) {
                    n = j;
                    break;
                }
                else {
                    blocks += fview_count[j+1];
                }
            }

            if (bytes_remaining) {
                /* Finish up a partially used buffer from the previous  cycle */
                if (bytes_remaining <= bytes_to_read_in_cycle) {
                    /* Data fits completely into the block */
                    if (my_aggregator == fh->f_rank) {
                        blocklen_per_process[n][disp_index[n] - 1] = bytes_remaining;
                        displs_per_process[n][disp_index[n] - 1] =
                            (ptrdiff_t)global_iov_array[sorted[current_index]].iov_base +
                            (global_iov_array[sorted[current_index]].iov_len - bytes_remaining);

                        blocklen_per_process[n] = (int *) realloc
                            ((void *)blocklen_per_process[n], (disp_index[n]+1)*sizeof(int));
                        displs_per_process[n] = (MPI_Aint *) realloc
                            ((void *)displs_per_process[n], (disp_index[n]+1)*sizeof(MPI_Aint));
                        blocklen_per_process[n][disp_index[n]] = 0;
                        displs_per_process[n][disp_index[n]] = 0;
                        disp_index[n] += 1;
                    }
                    if (fh->f_procs_in_group[n] == fh->f_rank) {
                        bytes_received += bytes_remaining;
                    }
                    current_index ++;
                    bytes_to_read_in_cycle -= bytes_remaining;
                    bytes_remaining = 0;
                    continue;
                }
                else {
                     /* the remaining data from the previous cycle is larger than the
                        bytes_to_write_in_cycle, so we have to segment again */
                    if (my_aggregator == fh->f_rank) {
                        blocklen_per_process[n][disp_index[n] - 1] = bytes_to_read_in_cycle;
                        displs_per_process[n][disp_index[n] - 1] =
                            (ptrdiff_t)global_iov_array[sorted[current_index]].iov_base +
                            (global_iov_array[sorted[current_index]].iov_len
                             - bytes_remaining);
                    }
                    if (fh->f_procs_in_group[n] == fh->f_rank) {
                        bytes_received += bytes_to_read_in_cycle;
                    }
                    bytes_remaining -= bytes_to_read_in_cycle;
                    bytes_to_read_in_cycle = 0;
                    break;
                }
            }
            else {
                /* No partially used entry available, have to start a new one */
                if (bytes_to_read_in_cycle <
                    (MPI_Aint) global_iov_array[sorted[current_index]].iov_len) {
                    /* This entry has more data than we can sendin one cycle */
                    if (my_aggregator == fh->f_rank) {
                        blocklen_per_process[n][disp_index[n] - 1] = bytes_to_read_in_cycle;
                        displs_per_process[n][disp_index[n] - 1] =
                            (ptrdiff_t)global_iov_array[sorted[current_index]].iov_base ;
                    }

                    if (fh->f_procs_in_group[n] == fh->f_rank) {
                        bytes_received += bytes_to_read_in_cycle;
                    }
                    bytes_remaining = global_iov_array[sorted[current_index]].iov_len -
                        bytes_to_read_in_cycle;
                    bytes_to_read_in_cycle = 0;
                    break;
                }
                else {
                    /* Next data entry is less than bytes_to_write_in_cycle */
                    if (my_aggregator ==  fh->f_rank) {
                        blocklen_per_process[n][disp_index[n] - 1] =
                            global_iov_array[sorted[current_index]].iov_len;
                        displs_per_process[n][disp_index[n] - 1] = (ptrdiff_t)
                            global_iov_array[sorted[current_index]].iov_base;
                        blocklen_per_process[n] =
                            (int *) realloc ((void *)blocklen_per_process[n], (disp_index[n]+1)*sizeof(int));
                        displs_per_process[n] = (MPI_Aint *)realloc
                            ((void *)displs_per_process[n], (disp_index[n]+1)*sizeof(MPI_Aint));
                        blocklen_per_process[n][disp_index[n]] = 0;
                        displs_per_process[n][disp_index[n]] = 0;
                        disp_index[n] += 1;
                    }
                    if (fh->f_procs_in_group[n] == fh->f_rank) {
                        bytes_received +=
                            global_iov_array[sorted[current_index]].iov_len;
                    }
                    bytes_to_read_in_cycle -=
                        global_iov_array[sorted[current_index]].iov_len;
                    current_index ++;
                    continue;
                }
            }
        } /* end while (bytes_to_read_in_cycle) */

        /*************************************************************************
	 *** 7d. Calculate the displacement on where to put the data and allocate
         ***     the recieve buffer (global_buf)
	 *************************************************************************/
        if (my_aggregator == fh->f_rank) {
            entries_per_aggregator=0;
            for (i=0;i<fh->f_procs_per_group; i++){
                for (j=0;j<disp_index[i];j++){
                    if (blocklen_per_process[i][j] > 0)
                        entries_per_aggregator++ ;
                }
            }
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
                global_count = 0;
                for (i=0;i<fh->f_procs_per_group; i++){
                    for(j=0;j<disp_index[i];j++){
                        if (blocklen_per_process[i][j] > 0){
                            file_offsets_for_agg[temp_index].length =
                                blocklen_per_process[i][j];
                            global_count += blocklen_per_process[i][j];
                            file_offsets_for_agg[temp_index].process_id = i;
                            file_offsets_for_agg[temp_index].offset =
                                displs_per_process[i][j];
                            temp_index++;
                        }
                    }
                }
            }
            else{
                continue;
            }

             /* Sort the displacements for each aggregator */
            read_heap_sort (file_offsets_for_agg,
                            entries_per_aggregator,
                            sorted_file_offsets);

            memory_displacements = (MPI_Aint *) malloc
                (entries_per_aggregator * sizeof(MPI_Aint));
            memory_displacements[sorted_file_offsets[0]] = 0;
            for (i=1; i<entries_per_aggregator; i++){
                memory_displacements[sorted_file_offsets[i]] =
                    memory_displacements[sorted_file_offsets[i-1]] +
                    file_offsets_for_agg[sorted_file_offsets[i-1]].length;
            }

             /**********************************************************
	     *** 7e. Create the io array, and pass it to fbtl
	     *********************************************************/
            fh->f_io_array = (mca_common_ompio_io_array_t *) malloc
                (entries_per_aggregator * sizeof (mca_common_ompio_io_array_t));
            if (NULL == fh->f_io_array) {
                opal_output(1, "OUT OF MEMORY\n");
                ret = OMPI_ERR_OUT_OF_RESOURCE;
                goto exit;
            }

            fh->f_num_of_io_entries = 0;
            fh->f_io_array[0].offset =
                (IOVBASE_TYPE *)(intptr_t)file_offsets_for_agg[sorted_file_offsets[0]].offset;
            fh->f_io_array[0].length =
                file_offsets_for_agg[sorted_file_offsets[0]].length;
            fh->f_io_array[0].memory_address =
                global_buf+memory_displacements[sorted_file_offsets[0]];
            fh->f_num_of_io_entries++;
            for (i=1;i<entries_per_aggregator;i++){
                if (file_offsets_for_agg[sorted_file_offsets[i-1]].offset +
                    file_offsets_for_agg[sorted_file_offsets[i-1]].length ==
                    file_offsets_for_agg[sorted_file_offsets[i]].offset){
                    fh->f_io_array[fh->f_num_of_io_entries - 1].length +=
                        file_offsets_for_agg[sorted_file_offsets[i]].length;
                }
                else{
                    fh->f_io_array[fh->f_num_of_io_entries].offset =
                        (IOVBASE_TYPE *)(intptr_t)file_offsets_for_agg[sorted_file_offsets[i]].offset;
                    fh->f_io_array[fh->f_num_of_io_entries].length =
                        file_offsets_for_agg[sorted_file_offsets[i]].length;
                    fh->f_io_array[fh->f_num_of_io_entries].memory_address =
                        global_buf+memory_displacements[sorted_file_offsets[i]];
                    fh->f_num_of_io_entries++;
                }
            }


#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
            start_read_time = MPI_Wtime();
#endif

            if (fh->f_num_of_io_entries) {
                if ( 0 >  fh->f_fbtl->fbtl_preadv (fh)) {
                    opal_output (1, "READ FAILED\n");
                    ret = OMPI_ERROR;
                    goto exit;
                }
            }

#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
            end_read_time = MPI_Wtime();
            read_time += end_read_time - start_read_time;
#endif
            /**********************************************************
             ******************** DONE READING ************************
             *********************************************************/

            temp_disp_index = (int *)calloc (1, fh->f_procs_per_group * sizeof (int));
            if (NULL == temp_disp_index) {
                opal_output (1, "OUT OF MEMORY\n");
                ret = OMPI_ERR_OUT_OF_RESOURCE;
                goto exit;
            }
            for (i=0; i<entries_per_aggregator; i++){
                temp_index =
                    file_offsets_for_agg[sorted_file_offsets[i]].process_id;
                displs_per_process[temp_index][temp_disp_index[temp_index]] =
                    memory_displacements[sorted_file_offsets[i]];
                if (temp_disp_index[temp_index] < disp_index[temp_index]){
                    temp_disp_index[temp_index] += 1;
                }
                else{
                    printf("temp_disp_index[%d]: %d is greater than disp_index[%d]: %d\n",
                           temp_index, temp_disp_index[temp_index],
                           temp_index, disp_index[temp_index]);
                }
            }
            if (NULL != temp_disp_index){
                free(temp_disp_index);
                temp_disp_index = NULL;
            }

#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
            start_rcomm_time = MPI_Wtime();
#endif
            for (i=0;i<fh->f_procs_per_group;i++){
                send_req[i] = MPI_REQUEST_NULL;
                if ( 0 < disp_index[i] ) {
                    ompi_datatype_create_hindexed(disp_index[i],
                                                  blocklen_per_process[i],
                                                  displs_per_process[i],
                                                  MPI_BYTE,
                                                  &sendtype[i]);
                    ompi_datatype_commit(&sendtype[i]);
                    ret = MCA_PML_CALL (isend(global_buf,
                                              1,
                                              sendtype[i],
                                              fh->f_procs_in_group[i],
                                              123,
                                              MCA_PML_BASE_SEND_STANDARD,
                                              fh->f_comm,
                                              &send_req[i]));
                    if(OMPI_SUCCESS != ret){
                        goto exit;
                    }
                }
            }
#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
            end_rcomm_time = MPI_Wtime();
            rcomm_time += end_rcomm_time - start_rcomm_time;
#endif
        }

        /**********************************************************
         *** 7f.  Scatter the Data from the readers
         *********************************************************/
        if ( recvbuf_is_contiguous ) {
            receive_buf = &((char*)buf)[position];
        }
        else if (bytes_received) {
            /* allocate a receive buffer and copy the data that needs
               to be received into it in case the data is non-contigous
               in memory */
            receive_buf = malloc (bytes_received);
            if (NULL == receive_buf) {
                opal_output (1, "OUT OF MEMORY\n");
                ret = OMPI_ERR_OUT_OF_RESOURCE;
                goto exit;
            }
        }

#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
        start_rcomm_time = MPI_Wtime();
#endif
        ret = MCA_PML_CALL(irecv(receive_buf,
                                 bytes_received,
                                 MPI_BYTE,
                                 my_aggregator,
                                 123,
                                 fh->f_comm,
                                 &recv_req));
        if (OMPI_SUCCESS != ret){
            goto exit;
        }


        if (my_aggregator == fh->f_rank){
            ret = ompi_request_wait_all (fh->f_procs_per_group,
                                         send_req,
                                         MPI_STATUS_IGNORE);
            if (OMPI_SUCCESS != ret){
                goto exit;
            }
        }

        ret = ompi_request_wait (&recv_req, MPI_STATUS_IGNORE);
        if (OMPI_SUCCESS != ret){
            goto exit;
        }
        position += bytes_received;

        /* If data is not contigous in memory, copy the data from the
           receive buffer into the buffer passed in */
        if (!recvbuf_is_contiguous ) {
            ptrdiff_t mem_address;
            size_t remaining = 0;
            size_t temp_position = 0;

            remaining = bytes_received;

            while (remaining) {
                mem_address = (ptrdiff_t)
                    (decoded_iov[iov_index].iov_base) + current_position;

                if (remaining >=
                    (decoded_iov[iov_index].iov_len - current_position)) {
                    memcpy ((IOVBASE_TYPE *) mem_address,
                            receive_buf+temp_position,
                            decoded_iov[iov_index].iov_len - current_position);
                    remaining = remaining -
                        (decoded_iov[iov_index].iov_len - current_position);
                    temp_position = temp_position +
                        (decoded_iov[iov_index].iov_len - current_position);
                    iov_index = iov_index + 1;
                    current_position = 0;
                }
                else {
                    memcpy ((IOVBASE_TYPE *) mem_address,
                            receive_buf+temp_position,
                            remaining);
                    current_position = current_position + remaining;
                    remaining = 0;
                }
            }

            if (NULL != receive_buf) {
                free (receive_buf);
                receive_buf = NULL;
            }
        }
#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
        end_rcomm_time = MPI_Wtime();
        rcomm_time += end_rcomm_time - start_rcomm_time;
#endif
    } /* end for (index=0; index < cycles; index ++) */

#if OMPIO_FCOLL_WANT_TIME_BREAKDOWN
    end_rexch = MPI_Wtime();
    read_exch += end_rexch - start_rexch;
    nentry.time[0] = read_time;
    nentry.time[1] = rcomm_time;
    nentry.time[2] = read_exch;
    if (my_aggregator == fh->f_rank)
        nentry.aggregator = 1;
    else
        nentry.aggregator = 0;
    nentry.nprocs_for_coll = dynamic_num_io_procs;
    if (!mca_common_ompio_full_print_queue(fh->f_coll_read_time)){
        mca_common_ompio_register_print_entry(fh->f_coll_read_time,
                                              nentry);
    }
#endif

exit:
    if (!recvbuf_is_contiguous) {
        if (NULL != receive_buf) {
            free (receive_buf);
            receive_buf = NULL;
        }
    }
    if (NULL != global_buf) {
        free (global_buf);
        global_buf = NULL;
    }
    if (NULL != sorted) {
        free (sorted);
        sorted = NULL;
    }
    if (NULL != global_iov_array) {
        free (global_iov_array);
        global_iov_array = NULL;
    }
    if (NULL != fview_count) {
        free (fview_count);
        fview_count = NULL;
    }
    if (NULL != decoded_iov) {
        free (decoded_iov);
        decoded_iov = NULL;
    }
    if (NULL != local_iov_array){
        free(local_iov_array);
        local_iov_array=NULL;
    }

    if (NULL != displs) {
        free (displs);
        displs = NULL;
    }
    if (my_aggregator == fh->f_rank) {

        if (NULL != sorted_file_offsets){
            free(sorted_file_offsets);
            sorted_file_offsets = NULL;
        }
        if (NULL != file_offsets_for_agg){
            free(file_offsets_for_agg);
            file_offsets_for_agg = NULL;
        }
        if (NULL != memory_displacements){
            free(memory_displacements);
            memory_displacements= NULL;
        }
        if (NULL != sendtype){
            for (i = 0; i < fh->f_procs_per_group; i++) {
                if ( MPI_DATATYPE_NULL != sendtype[i] ) {
                    ompi_datatype_destroy(&sendtype[i]);
                }
            }
            free(sendtype);
            sendtype=NULL;
        }

        if (NULL != disp_index){
            free(disp_index);
            disp_index = NULL;
        }

        if ( NULL != blocklen_per_process){
            for(l=0;l<fh->f_procs_per_group;l++){
                if (NULL != blocklen_per_process[l]){
                    free(blocklen_per_process[l]);
                    blocklen_per_process[l] = NULL;
                }
            }

            free(blocklen_per_process);
            blocklen_per_process = NULL;
        }

        if (NULL != displs_per_process){
            for (l=0; i<fh->f_procs_per_group; l++){
                if (NULL != displs_per_process[l]){
                    free(displs_per_process[l]);
                    displs_per_process[l] = NULL;
                }
            }
            free(displs_per_process);
            displs_per_process = NULL;
        }
        if ( NULL != send_req ) {
            free ( send_req );
            send_req = NULL;
        }
    }
    return ret;
}


static int read_heap_sort (mca_io_ompio_local_io_array *io_array,
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



