/*
 * Copyright (c) 2008      The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2011-2013 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2013 Inria.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 */
#include <assert.h>
#include <stdlib.h>

#include "ompi_config.h"

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/memchecker.h"
#include "ompi/mca/topo/topo.h"
#include "ompi/mca/topo/base/base.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Dist_graph_neighbors_count = PMPI_Dist_graph_neighbors_count
#endif
#define MPI_Dist_graph_neighbors_count PMPI_Dist_graph_neighbors_count
#endif

static const char FUNC_NAME[] = "MPI_Dist_graph_neighbors_count";


int MPI_Dist_graph_neighbors_count(MPI_Comm comm, int *inneighbors,
                                   int *outneighbors, int *weighted)
{
    int err;

    MEMCHECKER(
        memchecker_comm(comm);
    );

    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (ompi_comm_invalid(comm)) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COMM,
                                          FUNC_NAME);
        } else if (NULL == inneighbors || NULL == outneighbors ||
                   NULL == weighted) {
            return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_ARG, FUNC_NAME);
        }
    }

    if (!OMPI_COMM_IS_DIST_GRAPH(comm)) {
        return OMPI_ERRHANDLER_INVOKE (comm, MPI_ERR_TOPOLOGY,
                                       FUNC_NAME);
    }
    err = comm->c_topo->topo.dist_graph.dist_graph_neighbors_count(comm, inneighbors,
                                                                   outneighbors, weighted);
    OMPI_ERRHANDLER_RETURN(err, comm, err, FUNC_NAME);
}

