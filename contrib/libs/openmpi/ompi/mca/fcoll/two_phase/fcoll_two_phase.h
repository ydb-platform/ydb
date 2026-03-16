/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2016 University of Houston. All rights reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_FCOLL_TWO_PHASE_EXPORT_H
#define MCA_FCOLL_TWO_PHASE_EXPORT_H

#include "ompi_config.h"

#include "mpi.h"
#include "ompi/mca/mca.h"
#include "ompi/mca/fcoll/fcoll.h"
#include "ompi/mca/fcoll/base/base.h"
#include "ompi/mca/common/ompio/common_ompio.h"

BEGIN_C_DECLS

/* Globally exported variables */

extern int mca_fcoll_two_phase_priority;

OMPI_MODULE_DECLSPEC extern mca_fcoll_base_component_2_0_0_t mca_fcoll_two_phase_component;

/* API functions */

int mca_fcoll_two_phase_component_init_query(bool enable_progress_threads,
                                           bool enable_mpi_threads);
struct mca_fcoll_base_module_1_0_0_t *
mca_fcoll_two_phase_component_file_query (ompio_file_t *fh, int *priority);

int mca_fcoll_two_phase_component_file_unquery (ompio_file_t *file);

int mca_fcoll_two_phase_module_init (ompio_file_t *file);
int mca_fcoll_two_phase_module_finalize (ompio_file_t *file);

int mca_fcoll_two_phase_file_read_all (ompio_file_t *fh,
                                     void *buf,
                                     int count,
                                     struct ompi_datatype_t *datatype,
                                     ompi_status_public_t * status);


int mca_fcoll_two_phase_file_write_all (ompio_file_t *fh,
                                      const void *buf,
                                      int count,
                                      struct ompi_datatype_t *datatype,
                                      ompi_status_public_t * status);


int mca_fcoll_two_phase_calc_aggregator (ompio_file_t *fh,
					 OMPI_MPI_OFFSET_TYPE off,
					 OMPI_MPI_OFFSET_TYPE min_off,
					 OMPI_MPI_OFFSET_TYPE *len,
					 OMPI_MPI_OFFSET_TYPE fd_size,
					 OMPI_MPI_OFFSET_TYPE *fd_start,
					 OMPI_MPI_OFFSET_TYPE *fd_end,
					 int striping_unit,
					 int num_aggregators,
					 int *aggregator_list);

int mca_fcoll_two_phase_calc_others_requests(ompio_file_t *fh,
					     int count_my_req_procs,
					     int *count_my_req_per_proc,
					     mca_common_ompio_access_array_t *my_req,
					     int *count_others_req_procs_ptr,
					     mca_common_ompio_access_array_t **others_req_ptr);

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
					  int *aggregator_list);

int mca_fcoll_two_phase_domain_partition (ompio_file_t *fh,
					   OMPI_MPI_OFFSET_TYPE *start_offsets,
					   OMPI_MPI_OFFSET_TYPE *end_offsets,
					   OMPI_MPI_OFFSET_TYPE *min_st_offset_ptr,
					   OMPI_MPI_OFFSET_TYPE **fd_st_ptr,
					   OMPI_MPI_OFFSET_TYPE **fd_end_ptr,
					   int min_fd_size,
					   OMPI_MPI_OFFSET_TYPE *fd_size_ptr,
					   int striping_unit,
					   int nprocs_for_coll);




END_C_DECLS

#endif /* MCA_FCOLL_TWO_PHASE_EXPORT_H */
