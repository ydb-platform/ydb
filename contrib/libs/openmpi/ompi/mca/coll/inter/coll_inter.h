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
 * Copyright (c) 2006-2007 University of Houston. All rights reserved.
 * Copyright (c) 2008      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_COLL_INTER_EXPORT_H
#define MCA_COLL_INTER_EXPORT_H

#define mca_coll_inter_crossover 1
#include "ompi_config.h"

#include "mpi.h"
#include "ompi/mca/mca.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/communicator/communicator.h"

BEGIN_C_DECLS

/*
 * Globally exported variable
 */

OMPI_MODULE_DECLSPEC extern const mca_coll_base_component_2_0_0_t mca_coll_inter_component;
extern int mca_coll_inter_priority_param;
extern int mca_coll_inter_verbose_param;


/*
 * coll API functions
 */
int mca_coll_inter_init_query(bool allow_inter_user_threads,
                              bool have_hidden_threads);
mca_coll_base_module_t *
mca_coll_inter_comm_query(struct ompi_communicator_t *comm, int *priority);

int mca_coll_inter_module_enable(mca_coll_base_module_t *module,
                                 struct ompi_communicator_t *comm);

int mca_coll_inter_allgather_inter(const void *sbuf, int scount,
				   struct ompi_datatype_t *sdtype,
				   void *rbuf, int rcount,
				   struct ompi_datatype_t *rdtype,
				   struct ompi_communicator_t *comm,
                                   mca_coll_base_module_t *module);
int mca_coll_inter_allgatherv_inter(const void *sbuf, int scount,
				    struct ompi_datatype_t *sdtype,
				    void *rbuf, const int *rcounts, const int *disps,
				    struct ompi_datatype_t *rdtype,
				    struct ompi_communicator_t *comm,
                                    mca_coll_base_module_t *module);
int mca_coll_inter_allreduce_inter(const void *sbuf, void *rbuf, int count,
				   struct ompi_datatype_t *dtype,
				   struct ompi_op_t *op,
				   struct ompi_communicator_t *comm,
                                   mca_coll_base_module_t *module);
int mca_coll_inter_bcast_inter(void *buff, int count,
			       struct ompi_datatype_t *datatype,
			       int root,
			       struct ompi_communicator_t *comm,
                               mca_coll_base_module_t *module);
int mca_coll_inter_gather_inter(const void *sbuf, int scount,
				struct ompi_datatype_t *sdtype,
				void *rbuf, int rcount,
				struct ompi_datatype_t *rdtype,
				int root,
				struct ompi_communicator_t *comm,
                                mca_coll_base_module_t *module);
int mca_coll_inter_gatherv_inter(const void *sbuf, int scount,
				 struct ompi_datatype_t *sdtype,
				 void *rbuf, const int *rcounts, const int *disps,
				 struct ompi_datatype_t *rdtype,
				 int root,
				 struct ompi_communicator_t *comm,
                                 mca_coll_base_module_t *module);
int mca_coll_inter_reduce_inter(const void *sbuf, void* rbuf, int count,
				struct ompi_datatype_t *dtype,
				struct ompi_op_t *op,
				int root,
				struct ompi_communicator_t *comm,
                                mca_coll_base_module_t *module);
int mca_coll_inter_scatter_inter(const void *sbuf, int scount,
				 struct ompi_datatype_t *sdtype, void *rbuf,
				 int rcount, struct ompi_datatype_t *rdtype,
				 int root, struct ompi_communicator_t *comm,
                                 mca_coll_base_module_t *module);
int mca_coll_inter_scatterv_inter(const void *sbuf, const int *scounts, const int *disps,
				  struct ompi_datatype_t *sdtype,
				  void* rbuf, int rcount,
				  struct ompi_datatype_t *rdtype, int root,
				  struct ompi_communicator_t *comm,
                                  mca_coll_base_module_t *module);


struct mca_coll_inter_module_t {
    mca_coll_base_module_t super;

    /* Clarifying some terminology:
     *  comm:    the input communicator, consisting of several lower level communicators.
     */
    struct ompi_communicator_t        *inter_comm; /* link back to the attached comm */
};
typedef struct mca_coll_inter_module_t mca_coll_inter_module_t;
OBJ_CLASS_DECLARATION(mca_coll_inter_module_t);


END_C_DECLS

#endif /* MCA_COLL_INTER_EXPORT_H */
