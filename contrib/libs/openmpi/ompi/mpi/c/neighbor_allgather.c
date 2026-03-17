/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2018 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010      University of Houston.  All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include <stdio.h>

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/memchecker.h"
#include "ompi/mca/topo/topo.h"
#include "ompi/mca/topo/base/base.h"
#include "ompi/runtime/ompi_spc.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Neighbor_allgather = PMPI_Neighbor_allgather
#endif
#define MPI_Neighbor_allgather PMPI_Neighbor_allgather
#endif

static const char FUNC_NAME[] = "MPI_Neighbor_allgather";


int MPI_Neighbor_allgather(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                           void *recvbuf, int recvcount, MPI_Datatype recvtype,
                           MPI_Comm comm)
{
    int err;

    SPC_RECORD(OMPI_SPC_NEIGHBOR_ALLGATHER, 1);

    MEMCHECKER(
        int rank;
        ptrdiff_t ext;

        rank = ompi_comm_rank(comm);
        ompi_datatype_type_extent(recvtype, &ext);

        memchecker_datatype(recvtype);
        memchecker_comm(comm);
        /* check whether the actual send buffer is defined. */
        if (MPI_IN_PLACE != sendbuf) {
            memchecker_datatype(sendtype);
            memchecker_call(&opal_memchecker_base_isdefined, sendbuf, sendcount, sendtype);
        }
        /* check whether the receive buffer is addressable. */
        memchecker_call(&opal_memchecker_base_isaddressable, recvbuf, recvcount, recvtype);
    );

    if (MPI_PARAM_CHECK) {

        /* Unrooted operation -- same checks for all ranks on both
           intracommunicators and intercommunicators */

        err = MPI_SUCCESS;
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (ompi_comm_invalid(comm) || OMPI_COMM_IS_INTER(comm)) {
          OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COMM, FUNC_NAME);
        } else if (! OMPI_COMM_IS_TOPO(comm)) {
          OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_TOPOLOGY, FUNC_NAME);
        } else if (MPI_DATATYPE_NULL == recvtype || NULL == recvtype) {
          err = MPI_ERR_TYPE;
        } else if (recvcount < 0) {
          err = MPI_ERR_COUNT;
        } else if (MPI_IN_PLACE == sendbuf || MPI_IN_PLACE == recvbuf) {
          return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_ARG, FUNC_NAME);
        } else {
            OMPI_CHECK_DATATYPE_FOR_SEND(err, sendtype, sendcount);
        }
        OMPI_ERRHANDLER_CHECK(err, comm, err, FUNC_NAME);

        if( OMPI_COMM_IS_CART(comm) ) {
            const mca_topo_base_comm_cart_2_2_0_t *cart = comm->c_topo->mtc.cart;
            if( 0 > cart->ndims ) {
                return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_ARG, FUNC_NAME);
            }
        }
        else if( OMPI_COMM_IS_GRAPH(comm) ) {
            int degree;
            mca_topo_base_graph_neighbors_count(comm, ompi_comm_rank(comm), &degree);
            if( 0 > degree ) {
                return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_ARG, FUNC_NAME);
            }
        }
        else if( OMPI_COMM_IS_DIST_GRAPH(comm) ) {
            const mca_topo_base_comm_dist_graph_2_2_0_t *dist_graph = comm->c_topo->mtc.dist_graph;
            int indegree  = dist_graph->indegree;
            int outdegree = dist_graph->outdegree;
            if( indegree <  0 || outdegree <  0 ) {
                return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_ARG, FUNC_NAME);
            }
        }
    }

    /* Do we need to do anything?  Everyone had to give the same send
       signature, which means that everyone must have given a
       sendcount > 0. */
    if ((0 == sendcount) || (0 == recvcount)) {
        return MPI_SUCCESS;
    }

    OPAL_CR_ENTER_LIBRARY();

    /* Invoke the coll component to perform the back-end operation */
    err = comm->c_coll->coll_neighbor_allgather(sendbuf, sendcount, sendtype,
                                               recvbuf, recvcount, recvtype, comm,
                                               comm->c_coll->coll_neighbor_allgather_module);
    OMPI_ERRHANDLER_RETURN(err, comm, err, FUNC_NAME);
}

