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
 * Copyright (c) 2010      University of Houston.  All rights reserved.
 * Copyright (c) 2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2016 Los Alamos National Security, LLC.  All rights
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
#include "ompi/mca/topo/base/base.h"
#include "ompi/memchecker.h"
#include "ompi/mca/topo/topo.h"
#include "ompi/mca/topo/base/base.h"
#include "ompi/runtime/ompi_spc.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Neighbor_allgatherv = PMPI_Neighbor_allgatherv
#endif
#define MPI_Neighbor_allgatherv PMPI_Neighbor_allgatherv
#endif

static const char FUNC_NAME[] = "MPI_Neighbor_allgatherv";


int MPI_Neighbor_allgatherv(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                            void *recvbuf, const int recvcounts[], const int displs[],
                            MPI_Datatype recvtype, MPI_Comm comm)
{
    int in_size, out_size, err;

    SPC_RECORD(OMPI_SPC_NEIGHBOR_ALLGATHERV, 1);

    MEMCHECKER(
        int rank;
        ptrdiff_t ext;

        rank = ompi_comm_rank(comm);
        mca_topo_base_neighbor_count (comm, &in_size, &out_size);
        ompi_datatype_type_extent(recvtype, &ext);

        memchecker_datatype(recvtype);
        memchecker_comm (comm);
        /* check whether the receive buffer is addressable. */
        for (int i = 0; i < in_size; ++i) {
            memchecker_call(&opal_memchecker_base_isaddressable,
                            (char *)(recvbuf)+displs[i]*ext,
                            recvcounts[i], recvtype);
        }

        /* check whether the actual send buffer is defined. */
        if (MPI_IN_PLACE != sendbuf) {
            memchecker_datatype(sendtype);
            memchecker_call(&opal_memchecker_base_isdefined, sendbuf, sendcount, sendtype);
        }
    );

    if (MPI_PARAM_CHECK) {

        /* Unrooted operation -- same checks for all ranks on both
           intracommunicators and intercommunicators */

        err = MPI_SUCCESS;
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (ompi_comm_invalid(comm) || OMPI_COMM_IS_INTER(comm)) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COMM,
                                          FUNC_NAME);
        } else if (! OMPI_COMM_IS_TOPO(comm)) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_TOPOLOGY,
                                          FUNC_NAME);
        } else if (MPI_IN_PLACE == sendbuf || MPI_IN_PLACE == recvbuf) {
            return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_ARG, FUNC_NAME);
        } else if (MPI_DATATYPE_NULL == recvtype) {
            return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_TYPE, FUNC_NAME);
        }

        OMPI_CHECK_DATATYPE_FOR_SEND(err, sendtype, sendcount);
        OMPI_ERRHANDLER_CHECK(err, comm, err, FUNC_NAME);

      /* We always define the remote group to be the same as the local
         group in the case of an intracommunicator, so it's safe to
         get the size of the remote group here for both intra- and
         intercommunicators */

        mca_topo_base_neighbor_count (comm, &in_size, &out_size);
        for (int i = 0; i < in_size; ++i) {
          if (recvcounts[i] < 0) {
            return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_COUNT, FUNC_NAME);
          }
        }

        if (NULL == displs) {
          return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_BUFFER, FUNC_NAME);
        }

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

    OPAL_CR_ENTER_LIBRARY();

    /* Invoke the coll component to perform the back-end operation */
    err = comm->c_coll->coll_neighbor_allgatherv(sendbuf, sendcount, sendtype,
                                                recvbuf, recvcounts, displs,
                                                recvtype, comm, comm->c_coll->coll_neighbor_allgatherv_module);
    OMPI_ERRHANDLER_RETURN(err, comm, err, FUNC_NAME);
}
