/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_COLL_SELF_EXPORT_H
#define MCA_COLL_SELF_EXPORT_H

#include "ompi_config.h"

#include "mpi.h"
#include "ompi/mca/mca.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/request/request.h"

BEGIN_C_DECLS

/*
 * Globally exported variable
 */

OMPI_MODULE_DECLSPEC extern const mca_coll_base_component_2_0_0_t mca_coll_self_component;
extern int ompi_coll_self_priority;

/*
 * coll API functions
 */


  /* API functions */

int mca_coll_self_init_query(bool enable_progress_threads,
                             bool enable_mpi_threads);
mca_coll_base_module_t *
mca_coll_self_comm_query(struct ompi_communicator_t *comm, int *priority);

int mca_coll_self_module_enable(mca_coll_base_module_t *module,
                                struct ompi_communicator_t *comm);

int mca_coll_self_allgather_intra(const void *sbuf, int scount,
                                  struct ompi_datatype_t *sdtype,
                                  void *rbuf, int rcount,
                                  struct ompi_datatype_t *rdtype,
                                  struct ompi_communicator_t *comm,
                                  mca_coll_base_module_t *module);
int mca_coll_self_allgatherv_intra(const void *sbuf, int scount,
                                   struct ompi_datatype_t *sdtype,
                                   void * rbuf, const int *rcounts, const int *disps,
                                   struct ompi_datatype_t *rdtype,
                                   struct ompi_communicator_t *comm,
                                   mca_coll_base_module_t *module);
int mca_coll_self_allreduce_intra(const void *sbuf, void *rbuf, int count,
                                  struct ompi_datatype_t *dtype,
                                  struct ompi_op_t *op,
                                  struct ompi_communicator_t *comm,
                                  mca_coll_base_module_t *module);
int mca_coll_self_alltoall_intra(const void *sbuf, int scount,
                                 struct ompi_datatype_t *sdtype,
                                 void* rbuf, int rcount,
                                 struct ompi_datatype_t *rdtype,
                                 struct ompi_communicator_t *comm,
                                 mca_coll_base_module_t *module);
int mca_coll_self_alltoallv_intra(const void *sbuf, const int *scounts, const int *sdisps,
                                  struct ompi_datatype_t *sdtype,
                                  void *rbuf, const int *rcounts, const int *rdisps,
                                  struct ompi_datatype_t *rdtype,
                                  struct ompi_communicator_t *comm,
                                  mca_coll_base_module_t *module);
int mca_coll_self_alltoallw_intra(const void *sbuf, const int *scounts, const int *sdisps,
                                  struct ompi_datatype_t * const *sdtypes,
                                  void *rbuf, const int *rcounts, const int *rdisps,
                                  struct ompi_datatype_t * const *rdtypes,
                                  struct ompi_communicator_t *comm,
                                  mca_coll_base_module_t *module);
int mca_coll_self_barrier_intra(struct ompi_communicator_t *comm,
                                mca_coll_base_module_t *module);
int mca_coll_self_bcast_intra(void *buff, int count,
                              struct ompi_datatype_t *datatype,
                              int root,
                              struct ompi_communicator_t *comm,
                              mca_coll_base_module_t *module);
int mca_coll_self_exscan_intra(const void *sbuf, void *rbuf, int count,
                               struct ompi_datatype_t *dtype,
                               struct ompi_op_t *op,
                               struct ompi_communicator_t *comm,
                               mca_coll_base_module_t *module);
int mca_coll_self_gather_intra(const void *sbuf, int scount,
                               struct ompi_datatype_t *sdtype, void *rbuf,
                               int rcount, struct ompi_datatype_t *rdtype,
                               int root, struct ompi_communicator_t *comm,
                               mca_coll_base_module_t *module);
int mca_coll_self_gatherv_intra(const void *sbuf, int scount,
                                struct ompi_datatype_t *sdtype, void *rbuf,
                                const int *rcounts, const int *disps,
                                struct ompi_datatype_t *rdtype, int root,
                                struct ompi_communicator_t *comm,
                                mca_coll_base_module_t *module);
int mca_coll_self_reduce_intra(const void *sbuf, void* rbuf, int count,
                               struct ompi_datatype_t *dtype,
                               struct ompi_op_t *op,
                               int root,
                               struct ompi_communicator_t *comm,
                               mca_coll_base_module_t *module);
int mca_coll_self_reduce_scatter_intra(const void *sbuf, void *rbuf,
                                       const int *rcounts,
                                       struct ompi_datatype_t *dtype,
                                       struct ompi_op_t *op,
                                       struct ompi_communicator_t *comm,
                                       mca_coll_base_module_t *module);
int mca_coll_self_scan_intra(const void *sbuf, void *rbuf, int count,
                             struct ompi_datatype_t *dtype,
                             struct ompi_op_t *op,
                             struct ompi_communicator_t *comm,
                             mca_coll_base_module_t *module);
int mca_coll_self_scatter_intra(const void *sbuf, int scount,
                                struct ompi_datatype_t *sdtype, void *rbuf,
                                int rcount, struct ompi_datatype_t *rdtype,
                                int root, struct ompi_communicator_t *comm,
                                mca_coll_base_module_t *module);
int mca_coll_self_scatterv_intra(const void *sbuf, const int *scounts, const int *disps,
                                 struct ompi_datatype_t *sdtype,
                                 void* rbuf, int rcount,
                                 struct ompi_datatype_t *rdtype, int root,
                                 struct ompi_communicator_t *comm,
                                 mca_coll_base_module_t *module);

int mca_coll_self_ft_event(int state);


struct mca_coll_self_module_t {
    mca_coll_base_module_t super;
};
typedef struct mca_coll_self_module_t mca_coll_self_module_t;
OBJ_CLASS_DECLARATION(mca_coll_self_module_t);


END_C_DECLS

#endif /* MCA_COLL_SELF_EXPORT_H */
