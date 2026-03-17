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
 * Copyright (c) 2013      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2014-2015 Research Organization for Information Science
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
#pragma weak MPI_Neighbor_alltoall = PMPI_Neighbor_alltoall
#endif
#define MPI_Neighbor_alltoall PMPI_Neighbor_alltoall
#endif

static const char FUNC_NAME[] = "MPI_Neighbor_alltoall";


int MPI_Neighbor_alltoall(const void *sendbuf, int sendcount, MPI_Datatype sendtype,
                          void *recvbuf, int recvcount, MPI_Datatype recvtype,
                          MPI_Comm comm)
{
    size_t sendtype_size, recvtype_size;
    int err;

    SPC_RECORD(OMPI_SPC_NEIGHBOR_ALLTOALL, 1);

    MEMCHECKER(
        memchecker_comm(comm);
        if (MPI_IN_PLACE != sendbuf) {
            memchecker_datatype(sendtype);
            memchecker_call(&opal_memchecker_base_isdefined, (void *)sendbuf, sendcount, sendtype);
        }
        memchecker_datatype(recvtype);
        memchecker_call(&opal_memchecker_base_isaddressable, recvbuf, recvcount, recvtype);
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
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG,
                                          FUNC_NAME);
        } else {
            OMPI_CHECK_DATATYPE_FOR_SEND(err, sendtype, sendcount);
            OMPI_ERRHANDLER_CHECK(err, comm, err, FUNC_NAME);
            OMPI_CHECK_DATATYPE_FOR_RECV(err, recvtype, recvcount);
            OMPI_ERRHANDLER_CHECK(err, comm, err, FUNC_NAME);
        }

        ompi_datatype_type_size(sendtype, &sendtype_size);
        ompi_datatype_type_size(recvtype, &recvtype_size);
        if ((sendtype_size*sendcount) != (recvtype_size*recvcount)) {
            return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_TRUNCATE, FUNC_NAME);
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

    /* Do we need to do anything? */

    ompi_datatype_type_size(sendtype, &sendtype_size);
    ompi_datatype_type_size(recvtype, &recvtype_size);
    if (((0 == sendcount) || (0 == sendtype_size)) &&
        ((0 == recvcount) || 0 == (recvtype_size))) {
        return MPI_SUCCESS;
    }

    OPAL_CR_ENTER_LIBRARY();

    /* Invoke the coll component to perform the back-end operation */
    err = comm->c_coll->coll_neighbor_alltoall(sendbuf, sendcount, sendtype, recvbuf,
                                              recvcount, recvtype, comm,
                                              comm->c_coll->coll_neighbor_alltoall_module);
    OMPI_ERRHANDLER_RETURN(err, comm, err, FUNC_NAME);
}

