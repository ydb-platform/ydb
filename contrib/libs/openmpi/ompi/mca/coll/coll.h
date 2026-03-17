/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2015 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2008 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2007-2008 UT-Battelle, LLC
 * Copyright (c) 2012      Oak Rigde National Laboratory. All rights reserved.
 * Copyright (c) 2013-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2014-2015 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2017 IBM Corporation.  All rights reserved.
 * Copyright (c) 2017      FUJITSU LIMITED.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * Collective Communication Interface
 *
 * Interface for implementing the collective communication interface
 * of MPI.  The MPI interface provides error checking and error
 * handler invocation, but the collective components provide all other
 * functionality.
 *
 * Component selection is done per commuicator, at Communicator
 * construction time.  mca_coll_base_comm_select() is used to
 * create the list of components available to the compoenent
 * collm_comm_query function, instantiating a module for each
 * component that i usable, and sets the module collective function pointers.
 * mca_coll_base_comm_select() then loops through the list of available
 * components (via the instantiated module), and uses the
 * module's coll_module_enable() function to enable the modules, and
 * if successful, sets the communicator collective functions to the
 * those supplied by the given module, keeping track of which module it
 * is associated with.
 *
 * The module destructors are called for each module used by the
 * communicator, at communicator desctruction time.
 *
 * This can result in up to N different components being used for a
 * single communicator, one per needed collective function.
 *
 * The interface is the same for inter- or intra-communicators, and
 * components should be able to handle either style of communicator
 * during initialization (although handling may include indicating the
 * component is not available).
 */

#ifndef OMPI_MCA_COLL_COLL_H
#define OMPI_MCA_COLL_COLL_H

#include "ompi_config.h"

#include "mpi.h"
#include "ompi/mca/mca.h"
#include "opal/mca/base/base.h"

#include "opal/mca/crs/crs.h"
#include "opal/mca/crs/base/base.h"
#include "ompi/request/request.h"

BEGIN_C_DECLS


/* ******************************************************************** */


struct ompi_communicator_t;
struct ompi_datatype_t;
struct ompi_op_t;


/* ******************************************************************** */


/**
 * Collective component initialization
 *
 * Initialize the given collective component.  This function should
 * initialize any component-level. data.  It will be called exactly
 * once during MPI_INIT.
 *
 * @note The component framework is not lazily opened, so attempts
 * should be made to minimze the amount of memory allocated during
 * this function.
 *
 * @param[in] enable_progress_threads True if the component needs to
 *                                support progress threads
 * @param[in] enable_mpi_threads  True if the component needs to
 *                                support MPI_THREAD_MULTIPLE
 *
 * @retval OMPI_SUCCESS Component successfully initialized
 * @retval OMPI_ERROR   An unspecified error occurred
 */
typedef int (*mca_coll_base_component_init_query_fn_t)
     (bool enable_progress_threads, bool enable_mpi_threads);



/**
 * Query whether a component is available for the given communicator
 *
 * Query whether the component is available for the given
 * communicator.  If the component is available, an object should be
 * allocated and returned (with refcount at 1).  The module will not
 * be used for collective operations until module_enable() is called
 * on the module, but may be destroyed (via OBJ_RELEASE) either before
 * or after module_enable() is called.  If the module needs to release
 * resources obtained during query(), it should do so in the module
 * destructor.
 *
 * A component may provide NULL to this function to indicate it does
 * not wish to run or return an error during module_enable().
 *
 * @note The communicator is available for point-to-point
 * communication, but other functionality is not available during this
 * phase of initialization.
 *
 * @param[in] comm        The communicator being created
 * @param[out] priority   Priority setting for component on
 *                        this communicator
 *
 * @returns An initialized module structure if the component can
 * provide a module with the requested functionality or NULL if the
 * component should not be used on the given communicator.
 */
typedef struct mca_coll_base_module_2_3_0_t *
  (*mca_coll_base_component_comm_query_2_0_0_fn_t)
    (struct ompi_communicator_t *comm, int *priority);


/* ******************************************************************** */


/**
 * Enable module for collective communication
 *
 * Enable the module for collective communication.  Modules are enabled
 * in order from lowest to highest priority.  At each component,
 * collective functions with priority higher than the existing
 * function are copied into the communicator's function table and the
 * module's reference count is incremented.  Replaced functions have
 * their module's reference count decremented, so a component will go
 * out of scope when it has been examined and is no longer used in any
 * collective functions.
 *
 * Because the function list is built on increasing priority, a
 * component that needs functions from a lower priority component
 * (say, a multi-cast barrier that might need a point-to-point barrier
 * for resource exhaustion issues) can keep the function pointer and
 * module pointer and increase the reference count of the module and
 * use the module during execution.
 *
 * When a module is not used for any interface functions and no
 * higher-priority module has increased its refcount, it will have
 * it's destructor triggered and the module will be destroyed.
 *
 * @note The collective component should not modify the communicator
 * during this operation.  The communicator will be updated with the
 * collective algorithm's function pointers and module (and the ref
 * count increased on the module) by the base selection functionality.
 *
 * @param[in/out] module     Module created during comm_query()
 * @param[in]     comm       Communicator being created
 */
typedef int
(*mca_coll_base_module_enable_1_1_0_fn_t)(struct mca_coll_base_module_2_3_0_t* module,
                                          struct ompi_communicator_t *comm);


/**
 * Disable module for collective communication
 *
 * Disable the module for collective communication. This callback is
 * meant to avoid unused modules referencing unused modules
 * (and hence avoid memory leaks).
 *
 * @param[in/out] module     Module disabled during mca_coll_base_comm_unselect
 * @param[in]     comm       Communicator being disabled
 */
typedef int
(*mca_coll_base_module_disable_1_2_0_fn_t)(struct mca_coll_base_module_2_3_0_t* module,
                                          struct ompi_communicator_t *comm);

/* blocking collectives */
typedef int (*mca_coll_base_module_allgather_fn_t)
  (const void *sbuf, int scount, struct ompi_datatype_t *sdtype,
   void *rbuf, int rcount, struct ompi_datatype_t *rdtype,
   struct ompi_communicator_t *comm, struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_allgatherv_fn_t)
  (const void *sbuf, int scount, struct ompi_datatype_t *sdtype,
   void * rbuf, const int *rcounts, const int *disps,  struct ompi_datatype_t *rdtype,
   struct ompi_communicator_t *comm, struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_allreduce_fn_t)
  (const void *sbuf, void *rbuf, int count, struct ompi_datatype_t *dtype,
   struct ompi_op_t *op, struct ompi_communicator_t *comm, struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_alltoall_fn_t)
  (const void *sbuf, int scount, struct ompi_datatype_t *sdtype,
   void* rbuf, int rcount, struct ompi_datatype_t *rdtype,
   struct ompi_communicator_t *comm, struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_alltoallv_fn_t)
  (const void *sbuf, const int *scounts, const int *sdisps, struct ompi_datatype_t *sdtype,
   void *rbuf, const int *rcounts, const int *rdisps, struct ompi_datatype_t *rdtype,
   struct ompi_communicator_t *comm, struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_alltoallw_fn_t)
  (const void *sbuf, const int *scounts, const int *sdisps, struct ompi_datatype_t * const *sdtypes,
   void *rbuf, const int *rcounts, const int *rdisps, struct ompi_datatype_t * const *rdtypes,
   struct ompi_communicator_t *comm, struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_barrier_fn_t)
  (struct ompi_communicator_t *comm, struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_bcast_fn_t)
  (void *buff, int count, struct ompi_datatype_t *datatype, int root,
   struct ompi_communicator_t *comm, struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_exscan_fn_t)
  (const void *sbuf, void *rbuf, int count, struct ompi_datatype_t *dtype,
   struct ompi_op_t *op, struct ompi_communicator_t *comm, struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_gather_fn_t)
  (const void *sbuf, int scount, struct ompi_datatype_t *sdtype,
   void *rbuf, int rcount, struct ompi_datatype_t *rdtype,
   int root, struct ompi_communicator_t *comm, struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_gatherv_fn_t)
  (const void *sbuf, int scount, struct ompi_datatype_t *sdtype,
   void *rbuf, const int *rcounts, const int *disps, struct ompi_datatype_t *rdtype,
   int root, struct ompi_communicator_t *comm, struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_reduce_fn_t)
  (const void *sbuf, void* rbuf, int count, struct ompi_datatype_t *dtype,
   struct ompi_op_t *op, int root, struct ompi_communicator_t *comm, struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_reduce_scatter_fn_t)
  (const void *sbuf, void *rbuf, const int *rcounts, struct ompi_datatype_t *dtype,
   struct ompi_op_t *op, struct ompi_communicator_t *comm, struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_reduce_scatter_block_fn_t)
  (const void *sbuf, void *rbuf, int rcount, struct ompi_datatype_t *dtype,
   struct ompi_op_t *op, struct ompi_communicator_t *comm, struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_scan_fn_t)
  (const void *sbuf, void *rbuf, int count, struct ompi_datatype_t *dtype,
   struct ompi_op_t *op, struct ompi_communicator_t *comm, struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_scatter_fn_t)
  (const void *sbuf, int scount, struct ompi_datatype_t *sdtype,
   void *rbuf, int rcount, struct ompi_datatype_t *rdtype,
   int root, struct ompi_communicator_t *comm, struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_scatterv_fn_t)
  (const void *sbuf, const int *scounts, const int *disps, struct ompi_datatype_t *sdtype,
   void* rbuf, int rcount, struct ompi_datatype_t *rdtype,
   int root, struct ompi_communicator_t *comm, struct mca_coll_base_module_2_3_0_t *module);

/* nonblocking collectives */
typedef int (*mca_coll_base_module_iallgather_fn_t)
  (const void *sbuf, int scount, struct ompi_datatype_t *sdtype,
   void *rbuf, int rcount, struct ompi_datatype_t *rdtype,
   struct ompi_communicator_t *comm, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_iallgatherv_fn_t)
  (const void *sbuf, int scount, struct ompi_datatype_t *sdtype,
   void * rbuf, const int *rcounts, const int *disps,  struct ompi_datatype_t *rdtype,
   struct ompi_communicator_t *comm, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_iallreduce_fn_t)
  (const void *sbuf, void *rbuf, int count, struct ompi_datatype_t *dtype,
   struct ompi_op_t *op, struct ompi_communicator_t *comm,
   ompi_request_t ** request, struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_ialltoall_fn_t)
  (const void *sbuf, int scount, struct ompi_datatype_t *sdtype,
   void* rbuf, int rcount, struct ompi_datatype_t *rdtype,
   struct ompi_communicator_t *comm, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_ialltoallv_fn_t)
  (const void *sbuf, const int *scounts, const int *sdisps, struct ompi_datatype_t *sdtype,
   void *rbuf, const int *rcounts, const int *rdisps, struct ompi_datatype_t *rdtype,
   struct ompi_communicator_t *comm, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_ialltoallw_fn_t)
  (const void *sbuf, const int *scounts, const int *sdisps, struct ompi_datatype_t * const *sdtypes,
   void *rbuf, const int *rcounts, const int *rdisps, struct ompi_datatype_t * const *rdtypes,
   struct ompi_communicator_t *comm, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_ibarrier_fn_t)
  (struct ompi_communicator_t *comm, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_ibcast_fn_t)
  (void *buff, int count, struct ompi_datatype_t *datatype, int root,
   struct ompi_communicator_t *comm, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_iexscan_fn_t)
  (const void *sbuf, void *rbuf, int count, struct ompi_datatype_t *dtype,
   struct ompi_op_t *op, struct ompi_communicator_t *comm, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_igather_fn_t)
  (const void *sbuf, int scount, struct ompi_datatype_t *sdtype,
   void *rbuf, int rcount, struct ompi_datatype_t *rdtype,
   int root, struct ompi_communicator_t *comm, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_igatherv_fn_t)
  (const void *sbuf, int scount, struct ompi_datatype_t *sdtype,
   void *rbuf, const int *rcounts, const int *disps, struct ompi_datatype_t *rdtype,
   int root, struct ompi_communicator_t *comm, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_ireduce_fn_t)
  (const void *sbuf, void* rbuf, int count, struct ompi_datatype_t *dtype,
   struct ompi_op_t *op, int root, struct ompi_communicator_t *comm, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_ireduce_scatter_fn_t)
  (const void *sbuf, void *rbuf, const int *rcounts, struct ompi_datatype_t *dtype,
   struct ompi_op_t *op, struct ompi_communicator_t *comm, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_ireduce_scatter_block_fn_t)
  (const void *sbuf, void *rbuf, int rcount, struct ompi_datatype_t *dtype,
   struct ompi_op_t *op, struct ompi_communicator_t *comm, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_iscan_fn_t)
  (const void *sbuf, void *rbuf, int count, struct ompi_datatype_t *dtype,
   struct ompi_op_t *op, struct ompi_communicator_t *comm, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_iscatter_fn_t)
  (const void *sbuf, int scount, struct ompi_datatype_t *sdtype,
   void *rbuf, int rcount, struct ompi_datatype_t *rdtype,
   int root, struct ompi_communicator_t *comm, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_iscatterv_fn_t)
  (const void *sbuf, const int *scounts, const int *disps, struct ompi_datatype_t *sdtype,
   void* rbuf, int rcount, struct ompi_datatype_t *rdtype,
   int root, struct ompi_communicator_t *comm, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);

/* persistent collectives */
typedef int (*mca_coll_base_module_allgather_init_fn_t)
  (const void *sbuf, int scount, struct ompi_datatype_t *sdtype,
   void *rbuf, int rcount, struct ompi_datatype_t *rdtype,
   struct ompi_communicator_t *comm, struct ompi_info_t *info, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_allgatherv_init_fn_t)
  (const void *sbuf, int scount, struct ompi_datatype_t *sdtype,
   void * rbuf, const int *rcounts, const int *disps,  struct ompi_datatype_t *rdtype,
   struct ompi_communicator_t *comm, struct ompi_info_t *info, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_allreduce_init_fn_t)
  (const void *sbuf, void *rbuf, int count, struct ompi_datatype_t *dtype,
   struct ompi_op_t *op, struct ompi_communicator_t *comm, struct ompi_info_t *info,
   ompi_request_t ** request, struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_alltoall_init_fn_t)
  (const void *sbuf, int scount, struct ompi_datatype_t *sdtype,
   void* rbuf, int rcount, struct ompi_datatype_t *rdtype,
   struct ompi_communicator_t *comm, struct ompi_info_t *info, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_alltoallv_init_fn_t)
  (const void *sbuf, const int *scounts, const int *sdisps, struct ompi_datatype_t *sdtype,
   void *rbuf, const int *rcounts, const int *rdisps, struct ompi_datatype_t *rdtype,
   struct ompi_communicator_t *comm, struct ompi_info_t *info, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_alltoallw_init_fn_t)
  (const void *sbuf, const int *scounts, const int *sdisps, struct ompi_datatype_t * const *sdtypes,
   void *rbuf, const int *rcounts, const int *rdisps, struct ompi_datatype_t * const *rdtypes,
   struct ompi_communicator_t *comm, struct ompi_info_t *info, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_barrier_init_fn_t)
  (struct ompi_communicator_t *comm, struct ompi_info_t *info, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_bcast_init_fn_t)
  (void *buff, int count, struct ompi_datatype_t *datatype, int root,
   struct ompi_communicator_t *comm, struct ompi_info_t *info, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_exscan_init_fn_t)
  (const void *sbuf, void *rbuf, int count, struct ompi_datatype_t *dtype,
   struct ompi_op_t *op, struct ompi_communicator_t *comm, struct ompi_info_t *info, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_gather_init_fn_t)
  (const void *sbuf, int scount, struct ompi_datatype_t *sdtype,
   void *rbuf, int rcount, struct ompi_datatype_t *rdtype,
   int root, struct ompi_communicator_t *comm, struct ompi_info_t *info, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_gatherv_init_fn_t)
  (const void *sbuf, int scount, struct ompi_datatype_t *sdtype,
   void *rbuf, const int *rcounts, const int *disps, struct ompi_datatype_t *rdtype,
   int root, struct ompi_communicator_t *comm, struct ompi_info_t *info, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_reduce_init_fn_t)
  (const void *sbuf, void* rbuf, int count, struct ompi_datatype_t *dtype,
   struct ompi_op_t *op, int root, struct ompi_communicator_t *comm, struct ompi_info_t *info, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_reduce_scatter_init_fn_t)
  (const void *sbuf, void *rbuf, const int *rcounts, struct ompi_datatype_t *dtype,
   struct ompi_op_t *op, struct ompi_communicator_t *comm, struct ompi_info_t *info, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_reduce_scatter_block_init_fn_t)
  (const void *sbuf, void *rbuf, int rcount, struct ompi_datatype_t *dtype,
   struct ompi_op_t *op, struct ompi_communicator_t *comm, struct ompi_info_t *info, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_scan_init_fn_t)
  (const void *sbuf, void *rbuf, int count, struct ompi_datatype_t *dtype,
   struct ompi_op_t *op, struct ompi_communicator_t *comm, struct ompi_info_t *info, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_scatter_init_fn_t)
  (const void *sbuf, int scount, struct ompi_datatype_t *sdtype,
   void *rbuf, int rcount, struct ompi_datatype_t *rdtype,
   int root, struct ompi_communicator_t *comm, struct ompi_info_t *info, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_scatterv_init_fn_t)
  (const void *sbuf, const int *scounts, const int *disps, struct ompi_datatype_t *sdtype,
   void* rbuf, int rcount, struct ompi_datatype_t *rdtype,
   int root, struct ompi_communicator_t *comm, struct ompi_info_t *info, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);

/*
 * The signature of the neighborhood alltoallw differs from alltoallw
 */
typedef int (*mca_coll_base_module_neighbor_alltoallw_fn_t)
  (const void *sbuf, const int *scounts, const MPI_Aint *sdisps, struct ompi_datatype_t * const *sdtypes,
   void *rbuf, const int *rcounts, const MPI_Aint *rdisps, struct ompi_datatype_t * const *rdtypes,
   struct ompi_communicator_t *comm, struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_ineighbor_alltoallw_fn_t)
  (const void *sbuf, const int *scounts, const MPI_Aint *sdisps, struct ompi_datatype_t * const *sdtypes,
   void *rbuf, const int *rcounts, const MPI_Aint *rdisps, struct ompi_datatype_t * const *rdtypes,
   struct ompi_communicator_t *comm, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);
typedef int (*mca_coll_base_module_neighbor_alltoallw_init_fn_t)
  (const void *sbuf, const int *scounts, const MPI_Aint *sdisps, struct ompi_datatype_t * const *sdtypes,
   void *rbuf, const int *rcounts, const MPI_Aint *rdisps, struct ompi_datatype_t * const *rdtypes,
   struct ompi_communicator_t *comm, struct ompi_info_t *info, ompi_request_t ** request,
   struct mca_coll_base_module_2_3_0_t *module);

/*
 * reduce_local
 * Even though this is not a collective operation, it is related to the
 * collectives. Adding to the framework allows a collective component the
 * option of intercepting it, if desired.
 */
typedef int (*mca_coll_base_module_reduce_local_fn_t)
   (const void *inbuf, void *inoutbuf, int count,
    struct ompi_datatype_t * dtype, struct ompi_op_t * op,
    struct mca_coll_base_module_2_3_0_t *module);


/**
 * Fault Tolerance Awareness function.
 *
 * Fault tolerance function -- called when a process / job state
 * change is noticed.
 *
 * @param[in] state    State change that triggered the function
 *
 * @retval OMPI_SUCCESS Component successfully selected
 * @retval OMPI_ERROR   An unspecified error occurred
 */
typedef int (*mca_coll_base_module_ft_event_fn_t) (int state);


/* ******************************************************************** */


/**
 * Collective component interface
 *
 * Component interface for the collective framework.  A public
 * instance of this structure, called
 * mca_coll_[component_name]_component, must exist in any collective
 * component.
 */
struct mca_coll_base_component_2_0_0_t {
    /** Base component description */
    mca_base_component_t collm_version;
    /** Base component data block */
    mca_base_component_data_t collm_data;

    /** Component initialization function */
    mca_coll_base_component_init_query_fn_t collm_init_query;
    /** Query whether component is useable for given communicator */
    mca_coll_base_component_comm_query_2_0_0_fn_t collm_comm_query;
};
typedef struct mca_coll_base_component_2_0_0_t mca_coll_base_component_2_0_0_t;

/** Per guidence in mca.h, use the unversioned struct name if you just
    want to always keep up with the most recent version of the
    interace. */
typedef struct mca_coll_base_component_2_0_0_t mca_coll_base_component_t;


/**
 * Collective module interface
 *
 * Module interface to the Collective framework.  Modules are
 * reference counted based on the number of functions from the module
 * used on the commuicator.  There is at most one module per component
 * on a given communicator, and there can be many component modules on
 * a given communicator.
 *
 * @note The collective framework and the
 * communicator functionality only stores a pointer to the module
 * function, so the component is free to create a structure that
 * inherits from this one for use as the module structure.
 */
struct mca_coll_base_module_2_3_0_t {
    /** Collective modules all inherit from opal_object */
    opal_object_t super;

    /** Enable function called when a collective module is (possibly)
        going to be used for the given communicator */
    mca_coll_base_module_enable_1_1_0_fn_t coll_module_enable;

    /* Collective function pointers */

    /* blocking functions */
    mca_coll_base_module_allgather_fn_t coll_allgather;
    mca_coll_base_module_allgatherv_fn_t coll_allgatherv;
    mca_coll_base_module_allreduce_fn_t coll_allreduce;
    mca_coll_base_module_alltoall_fn_t coll_alltoall;
    mca_coll_base_module_alltoallv_fn_t coll_alltoallv;
    mca_coll_base_module_alltoallw_fn_t coll_alltoallw;
    mca_coll_base_module_barrier_fn_t coll_barrier;
    mca_coll_base_module_bcast_fn_t coll_bcast;
    mca_coll_base_module_exscan_fn_t coll_exscan;
    mca_coll_base_module_gather_fn_t coll_gather;
    mca_coll_base_module_gatherv_fn_t coll_gatherv;
    mca_coll_base_module_reduce_fn_t coll_reduce;
    mca_coll_base_module_reduce_scatter_fn_t coll_reduce_scatter;
    mca_coll_base_module_reduce_scatter_block_fn_t coll_reduce_scatter_block;
    mca_coll_base_module_scan_fn_t coll_scan;
    mca_coll_base_module_scatter_fn_t coll_scatter;
    mca_coll_base_module_scatterv_fn_t coll_scatterv;

    /* nonblocking functions */
    mca_coll_base_module_iallgather_fn_t coll_iallgather;
    mca_coll_base_module_iallgatherv_fn_t coll_iallgatherv;
    mca_coll_base_module_iallreduce_fn_t coll_iallreduce;
    mca_coll_base_module_ialltoall_fn_t coll_ialltoall;
    mca_coll_base_module_ialltoallv_fn_t coll_ialltoallv;
    mca_coll_base_module_ialltoallw_fn_t coll_ialltoallw;
    mca_coll_base_module_ibarrier_fn_t coll_ibarrier;
    mca_coll_base_module_ibcast_fn_t coll_ibcast;
    mca_coll_base_module_iexscan_fn_t coll_iexscan;
    mca_coll_base_module_igather_fn_t coll_igather;
    mca_coll_base_module_igatherv_fn_t coll_igatherv;
    mca_coll_base_module_ireduce_fn_t coll_ireduce;
    mca_coll_base_module_ireduce_scatter_fn_t coll_ireduce_scatter;
    mca_coll_base_module_ireduce_scatter_block_fn_t coll_ireduce_scatter_block;
    mca_coll_base_module_iscan_fn_t coll_iscan;
    mca_coll_base_module_iscatter_fn_t coll_iscatter;
    mca_coll_base_module_iscatterv_fn_t coll_iscatterv;

    /* persistent functions */
    mca_coll_base_module_allgather_init_fn_t coll_allgather_init;
    mca_coll_base_module_allgatherv_init_fn_t coll_allgatherv_init;
    mca_coll_base_module_allreduce_init_fn_t coll_allreduce_init;
    mca_coll_base_module_alltoall_init_fn_t coll_alltoall_init;
    mca_coll_base_module_alltoallv_init_fn_t coll_alltoallv_init;
    mca_coll_base_module_alltoallw_init_fn_t coll_alltoallw_init;
    mca_coll_base_module_barrier_init_fn_t coll_barrier_init;
    mca_coll_base_module_bcast_init_fn_t coll_bcast_init;
    mca_coll_base_module_exscan_init_fn_t coll_exscan_init;
    mca_coll_base_module_gather_init_fn_t coll_gather_init;
    mca_coll_base_module_gatherv_init_fn_t coll_gatherv_init;
    mca_coll_base_module_reduce_init_fn_t coll_reduce_init;
    mca_coll_base_module_reduce_scatter_init_fn_t coll_reduce_scatter_init;
    mca_coll_base_module_reduce_scatter_block_init_fn_t coll_reduce_scatter_block_init;
    mca_coll_base_module_scan_init_fn_t coll_scan_init;
    mca_coll_base_module_scatter_init_fn_t coll_scatter_init;
    mca_coll_base_module_scatterv_init_fn_t coll_scatterv_init;

    /* neighborhood functions */
    mca_coll_base_module_allgather_fn_t coll_neighbor_allgather;
    mca_coll_base_module_allgatherv_fn_t coll_neighbor_allgatherv;
    mca_coll_base_module_alltoall_fn_t coll_neighbor_alltoall;
    mca_coll_base_module_alltoallv_fn_t coll_neighbor_alltoallv;
    mca_coll_base_module_neighbor_alltoallw_fn_t coll_neighbor_alltoallw;

    /* nonblocking neighborhood functions */
    mca_coll_base_module_iallgather_fn_t coll_ineighbor_allgather;
    mca_coll_base_module_iallgatherv_fn_t coll_ineighbor_allgatherv;
    mca_coll_base_module_ialltoall_fn_t coll_ineighbor_alltoall;
    mca_coll_base_module_ialltoallv_fn_t coll_ineighbor_alltoallv;
    mca_coll_base_module_ineighbor_alltoallw_fn_t coll_ineighbor_alltoallw;

    /* persistent neighborhood functions */
    mca_coll_base_module_allgather_init_fn_t coll_neighbor_allgather_init;
    mca_coll_base_module_allgatherv_init_fn_t coll_neighbor_allgatherv_init;
    mca_coll_base_module_alltoall_init_fn_t coll_neighbor_alltoall_init;
    mca_coll_base_module_alltoallv_init_fn_t coll_neighbor_alltoallv_init;
    mca_coll_base_module_neighbor_alltoallw_init_fn_t coll_neighbor_alltoallw_init;

    /** Fault tolerance event trigger function */
    mca_coll_base_module_ft_event_fn_t ft_event;

    /** Disable function called when a collective module will not
        be used for the given communicator */
    mca_coll_base_module_disable_1_2_0_fn_t coll_module_disable;

    mca_coll_base_module_reduce_local_fn_t coll_reduce_local;

    /** Data storage for all the algorithms defined in the base. Should
        not be used by other modules */
    struct mca_coll_base_comm_t* base_data;
};
typedef struct mca_coll_base_module_2_3_0_t mca_coll_base_module_2_3_0_t;

/** Per guidence in mca.h, use the unversioned struct name if you just
    want to always keep up with the most recent version of the
    interace. */
typedef struct mca_coll_base_module_2_3_0_t mca_coll_base_module_t;
OMPI_DECLSPEC OBJ_CLASS_DECLARATION(mca_coll_base_module_t);

/**
 * Collectives communicator cache structure
 *
 * Collectives communicator cache structure, used to find functions to
 * implement collective algorithms and their associated modules.  This
 * function may also be used internally by a module if it needs to
 * keep a large number of "backing" functions, such as the demo
 * component.
 */
struct mca_coll_base_comm_coll_t {

    /* blocking collectives */
    mca_coll_base_module_allgather_fn_t coll_allgather;
    mca_coll_base_module_2_3_0_t *coll_allgather_module;
    mca_coll_base_module_allgatherv_fn_t coll_allgatherv;
    mca_coll_base_module_2_3_0_t *coll_allgatherv_module;
    mca_coll_base_module_allreduce_fn_t coll_allreduce;
    mca_coll_base_module_2_3_0_t *coll_allreduce_module;
    mca_coll_base_module_alltoall_fn_t coll_alltoall;
    mca_coll_base_module_2_3_0_t *coll_alltoall_module;
    mca_coll_base_module_alltoallv_fn_t coll_alltoallv;
    mca_coll_base_module_2_3_0_t *coll_alltoallv_module;
    mca_coll_base_module_alltoallw_fn_t coll_alltoallw;
    mca_coll_base_module_2_3_0_t *coll_alltoallw_module;
    mca_coll_base_module_barrier_fn_t coll_barrier;
    mca_coll_base_module_2_3_0_t *coll_barrier_module;
    mca_coll_base_module_bcast_fn_t coll_bcast;
    mca_coll_base_module_2_3_0_t *coll_bcast_module;
    mca_coll_base_module_exscan_fn_t coll_exscan;
    mca_coll_base_module_2_3_0_t *coll_exscan_module;
    mca_coll_base_module_gather_fn_t coll_gather;
    mca_coll_base_module_2_3_0_t *coll_gather_module;
    mca_coll_base_module_gatherv_fn_t coll_gatherv;
    mca_coll_base_module_2_3_0_t *coll_gatherv_module;
    mca_coll_base_module_reduce_fn_t coll_reduce;
    mca_coll_base_module_2_3_0_t *coll_reduce_module;
    mca_coll_base_module_reduce_scatter_fn_t coll_reduce_scatter;
    mca_coll_base_module_2_3_0_t *coll_reduce_scatter_module;
    mca_coll_base_module_reduce_scatter_block_fn_t coll_reduce_scatter_block;
    mca_coll_base_module_2_3_0_t *coll_reduce_scatter_block_module;
    mca_coll_base_module_scan_fn_t coll_scan;
    mca_coll_base_module_2_3_0_t *coll_scan_module;
    mca_coll_base_module_scatter_fn_t coll_scatter;
    mca_coll_base_module_2_3_0_t *coll_scatter_module;
    mca_coll_base_module_scatterv_fn_t coll_scatterv;
    mca_coll_base_module_2_3_0_t *coll_scatterv_module;

    /* nonblocking collectives */
    mca_coll_base_module_iallgather_fn_t coll_iallgather;
    mca_coll_base_module_2_3_0_t *coll_iallgather_module;
    mca_coll_base_module_iallgatherv_fn_t coll_iallgatherv;
    mca_coll_base_module_2_3_0_t *coll_iallgatherv_module;
    mca_coll_base_module_iallreduce_fn_t coll_iallreduce;
    mca_coll_base_module_2_3_0_t *coll_iallreduce_module;
    mca_coll_base_module_ialltoall_fn_t coll_ialltoall;
    mca_coll_base_module_2_3_0_t *coll_ialltoall_module;
    mca_coll_base_module_ialltoallv_fn_t coll_ialltoallv;
    mca_coll_base_module_2_3_0_t *coll_ialltoallv_module;
    mca_coll_base_module_ialltoallw_fn_t coll_ialltoallw;
    mca_coll_base_module_2_3_0_t *coll_ialltoallw_module;
    mca_coll_base_module_ibarrier_fn_t coll_ibarrier;
    mca_coll_base_module_2_3_0_t *coll_ibarrier_module;
    mca_coll_base_module_ibcast_fn_t coll_ibcast;
    mca_coll_base_module_2_3_0_t *coll_ibcast_module;
    mca_coll_base_module_iexscan_fn_t coll_iexscan;
    mca_coll_base_module_2_3_0_t *coll_iexscan_module;
    mca_coll_base_module_igather_fn_t coll_igather;
    mca_coll_base_module_2_3_0_t *coll_igather_module;
    mca_coll_base_module_igatherv_fn_t coll_igatherv;
    mca_coll_base_module_2_3_0_t *coll_igatherv_module;
    mca_coll_base_module_ireduce_fn_t coll_ireduce;
    mca_coll_base_module_2_3_0_t *coll_ireduce_module;
    mca_coll_base_module_ireduce_scatter_fn_t coll_ireduce_scatter;
    mca_coll_base_module_2_3_0_t *coll_ireduce_scatter_module;
    mca_coll_base_module_ireduce_scatter_block_fn_t coll_ireduce_scatter_block;
    mca_coll_base_module_2_3_0_t *coll_ireduce_scatter_block_module;
    mca_coll_base_module_iscan_fn_t coll_iscan;
    mca_coll_base_module_2_3_0_t *coll_iscan_module;
    mca_coll_base_module_iscatter_fn_t coll_iscatter;
    mca_coll_base_module_2_3_0_t *coll_iscatter_module;
    mca_coll_base_module_iscatterv_fn_t coll_iscatterv;
    mca_coll_base_module_2_3_0_t *coll_iscatterv_module;

    /* persistent collectives */
    mca_coll_base_module_allgather_init_fn_t coll_allgather_init;
    mca_coll_base_module_2_3_0_t *coll_allgather_init_module;
    mca_coll_base_module_allgatherv_init_fn_t coll_allgatherv_init;
    mca_coll_base_module_2_3_0_t *coll_allgatherv_init_module;
    mca_coll_base_module_allreduce_init_fn_t coll_allreduce_init;
    mca_coll_base_module_2_3_0_t *coll_allreduce_init_module;
    mca_coll_base_module_alltoall_init_fn_t coll_alltoall_init;
    mca_coll_base_module_2_3_0_t *coll_alltoall_init_module;
    mca_coll_base_module_alltoallv_init_fn_t coll_alltoallv_init;
    mca_coll_base_module_2_3_0_t *coll_alltoallv_init_module;
    mca_coll_base_module_alltoallw_init_fn_t coll_alltoallw_init;
    mca_coll_base_module_2_3_0_t *coll_alltoallw_init_module;
    mca_coll_base_module_barrier_init_fn_t coll_barrier_init;
    mca_coll_base_module_2_3_0_t *coll_barrier_init_module;
    mca_coll_base_module_bcast_init_fn_t coll_bcast_init;
    mca_coll_base_module_2_3_0_t *coll_bcast_init_module;
    mca_coll_base_module_exscan_init_fn_t coll_exscan_init;
    mca_coll_base_module_2_3_0_t *coll_exscan_init_module;
    mca_coll_base_module_gather_init_fn_t coll_gather_init;
    mca_coll_base_module_2_3_0_t *coll_gather_init_module;
    mca_coll_base_module_gatherv_init_fn_t coll_gatherv_init;
    mca_coll_base_module_2_3_0_t *coll_gatherv_init_module;
    mca_coll_base_module_reduce_init_fn_t coll_reduce_init;
    mca_coll_base_module_2_3_0_t *coll_reduce_init_module;
    mca_coll_base_module_reduce_scatter_init_fn_t coll_reduce_scatter_init;
    mca_coll_base_module_2_3_0_t *coll_reduce_scatter_init_module;
    mca_coll_base_module_reduce_scatter_block_init_fn_t coll_reduce_scatter_block_init;
    mca_coll_base_module_2_3_0_t *coll_reduce_scatter_block_init_module;
    mca_coll_base_module_scan_init_fn_t coll_scan_init;
    mca_coll_base_module_2_3_0_t *coll_scan_init_module;
    mca_coll_base_module_scatter_init_fn_t coll_scatter_init;
    mca_coll_base_module_2_3_0_t *coll_scatter_init_module;
    mca_coll_base_module_scatterv_init_fn_t coll_scatterv_init;
    mca_coll_base_module_2_3_0_t *coll_scatterv_init_module;

    /* blocking neighborhood collectives */
    mca_coll_base_module_allgather_fn_t coll_neighbor_allgather;
    mca_coll_base_module_2_3_0_t *coll_neighbor_allgather_module;
    mca_coll_base_module_allgatherv_fn_t coll_neighbor_allgatherv;
    mca_coll_base_module_2_3_0_t *coll_neighbor_allgatherv_module;
    mca_coll_base_module_alltoall_fn_t coll_neighbor_alltoall;
    mca_coll_base_module_2_3_0_t *coll_neighbor_alltoall_module;
    mca_coll_base_module_alltoallv_fn_t coll_neighbor_alltoallv;
    mca_coll_base_module_2_3_0_t *coll_neighbor_alltoallv_module;
    mca_coll_base_module_neighbor_alltoallw_fn_t coll_neighbor_alltoallw;
    mca_coll_base_module_2_3_0_t *coll_neighbor_alltoallw_module;

    /* nonblocking neighborhood collectives */
    mca_coll_base_module_iallgather_fn_t coll_ineighbor_allgather;
    mca_coll_base_module_2_3_0_t *coll_ineighbor_allgather_module;
    mca_coll_base_module_iallgatherv_fn_t coll_ineighbor_allgatherv;
    mca_coll_base_module_2_3_0_t *coll_ineighbor_allgatherv_module;
    mca_coll_base_module_ialltoall_fn_t coll_ineighbor_alltoall;
    mca_coll_base_module_2_3_0_t *coll_ineighbor_alltoall_module;
    mca_coll_base_module_ialltoallv_fn_t coll_ineighbor_alltoallv;
    mca_coll_base_module_2_3_0_t *coll_ineighbor_alltoallv_module;
    mca_coll_base_module_ineighbor_alltoallw_fn_t coll_ineighbor_alltoallw;
    mca_coll_base_module_2_3_0_t *coll_ineighbor_alltoallw_module;

    /* persistent neighborhood collectives */
    mca_coll_base_module_allgather_init_fn_t coll_neighbor_allgather_init;
    mca_coll_base_module_2_3_0_t *coll_neighbor_allgather_init_module;
    mca_coll_base_module_allgatherv_init_fn_t coll_neighbor_allgatherv_init;
    mca_coll_base_module_2_3_0_t *coll_neighbor_allgatherv_init_module;
    mca_coll_base_module_alltoall_init_fn_t coll_neighbor_alltoall_init;
    mca_coll_base_module_2_3_0_t *coll_neighbor_alltoall_init_module;
    mca_coll_base_module_alltoallv_init_fn_t coll_neighbor_alltoallv_init;
    mca_coll_base_module_2_3_0_t *coll_neighbor_alltoallv_init_module;
    mca_coll_base_module_neighbor_alltoallw_init_fn_t coll_neighbor_alltoallw_init;
    mca_coll_base_module_2_3_0_t *coll_neighbor_alltoallw_init_module;

    mca_coll_base_module_reduce_local_fn_t coll_reduce_local;
    mca_coll_base_module_2_3_0_t *coll_reduce_local_module;
};
typedef struct mca_coll_base_comm_coll_t mca_coll_base_comm_coll_t;


/* ******************************************************************** */


/*
 * Macro for use in components that are of type coll
 */
#define MCA_COLL_BASE_VERSION_2_0_0 \
    OMPI_MCA_BASE_VERSION_2_1_0("coll", 2, 0, 0)


/* ******************************************************************** */


END_C_DECLS

#endif /* MCA_COLL_H */
