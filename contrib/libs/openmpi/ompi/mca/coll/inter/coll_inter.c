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
 * Copyright (c) 2006-2007 University of Houston. All rights reserved.
 * Copyright (c) 2008      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2013 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2017      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "coll_inter.h"

#include <stdio.h>

#include "mpi.h"
#include "ompi/communicator/communicator.h"

#include "ompi/mca/coll/coll.h"
#include "ompi/mca/coll/base/base.h"
#include "ompi/mca/coll/base/coll_base_functions.h"
#include "ompi/mca/coll/base/coll_tags.h"

#include "ompi/mca/bml/base/base.h"


#if 0
static void mca_coll_inter_dump_struct ( struct mca_coll_base_comm_t *c);

static const mca_coll_base_module_1_0_0_t inter = {

  /* Initialization / finalization functions */

  mca_coll_inter_module_init,
  mca_coll_inter_module_finalize,

  /* Collective function pointers */
  /* function pointers marked with NULL are not yet implemented
     and will use the functions provided in the basic module */
  mca_coll_inter_allgather_inter,
  mca_coll_inter_allgatherv_inter,
  mca_coll_inter_allreduce_inter,
  NULL, /* alltoall */
  NULL, /* alltoallv */
  NULL, /* alltoallw */
  NULL, /* barrier */
  mca_coll_inter_bcast_inter,
  NULL,  /* exscan */
  mca_coll_inter_gather_inter,
  mca_coll_inter_gatherv_inter,
  mca_coll_inter_reduce_inter,
  NULL,  /* reduce_scatter */
  NULL,  /* scan */
  mca_coll_inter_scatter_inter,
  mca_coll_inter_scatterv_inter
};
#endif


/*
 * Initial query function that is invoked during MPI_INIT, allowing
 * this module to indicate what level of thread support it provides.
 */
int mca_coll_inter_init_query(bool allow_inter_user_threads,
                             bool have_hidden_user_threads)
{
    /* Don't ask. All done */
    return OMPI_SUCCESS;
}


/*
 * Invoked when there's a new communicator that has been created.
 * Look at the communicator and decide which set of functions and
 * priority we want to return.
 */
mca_coll_base_module_t *
mca_coll_inter_comm_query(struct ompi_communicator_t *comm, int *priority)
{
    int size, rsize;
    mca_coll_inter_module_t *inter_module;

    /* This module only works for inter-communicators */
    if (!OMPI_COMM_IS_INTER(comm)) {
        return NULL;
    }

    /* Get the priority level attached to this module. If priority is less
     * than or equal to 0, then the module is unavailable. */
    *priority = mca_coll_inter_priority_param;
    if (0 >= mca_coll_inter_priority_param) {
	return NULL;
    }

    size = ompi_comm_size(comm);
    rsize = ompi_comm_remote_size(comm);

    if ( size < mca_coll_inter_crossover && rsize < mca_coll_inter_crossover) {
	return NULL;
    }

    inter_module = OBJ_NEW(mca_coll_inter_module_t);
    if (NULL == inter_module) {
	return NULL;
    }

    inter_module->super.coll_module_enable = mca_coll_inter_module_enable;
    inter_module->super.ft_event = NULL;

    inter_module->super.coll_allgather  = mca_coll_inter_allgather_inter;
    inter_module->super.coll_allgatherv = mca_coll_inter_allgatherv_inter;
    inter_module->super.coll_allreduce  = mca_coll_inter_allreduce_inter;
    inter_module->super.coll_alltoall   = NULL;
    inter_module->super.coll_alltoallv  = NULL;
    inter_module->super.coll_alltoallw  = NULL;
    inter_module->super.coll_barrier    = NULL;
    inter_module->super.coll_bcast      = mca_coll_inter_bcast_inter;
    inter_module->super.coll_exscan     = NULL;
    inter_module->super.coll_gather     = mca_coll_inter_gather_inter;
    inter_module->super.coll_gatherv    = mca_coll_inter_gatherv_inter;
    inter_module->super.coll_reduce     = mca_coll_inter_reduce_inter;
    inter_module->super.coll_reduce_scatter = NULL;
    inter_module->super.coll_scan       = NULL;
    inter_module->super.coll_scatter    = mca_coll_inter_scatter_inter;
    inter_module->super.coll_scatterv   = mca_coll_inter_scatterv_inter;
    inter_module->super.coll_reduce_local = mca_coll_base_reduce_local;

    return &(inter_module->super);
}


/*
 * Init module on the communicator
 */
int
mca_coll_inter_module_enable(mca_coll_base_module_t *module,
                             struct ompi_communicator_t *comm)
{
    mca_coll_inter_module_t *inter_module = (mca_coll_inter_module_t*) module;

    inter_module->inter_comm = comm;

#if 0
    if ( mca_coll_inter_verbose_param ) {
      mca_coll_inter_dump_struct (data);
    }
#endif

    return OMPI_SUCCESS;
}


#if 0
static void mca_coll_inter_dump_struct ( struct mca_coll_base_comm_t *c)
{
    int rank;

    rank = ompi_comm_rank ( c->inter_comm );

    printf("%d: Dump of inter-struct for  comm %s cid %u\n",
           rank, c->inter_comm->c_name, c->inter_comm->c_contextid);


    return;
}
#endif
