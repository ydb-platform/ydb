/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2013 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012-2013 Inria.  All rights reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "ompi/mca/topo/base/base.h"
#include "ompi/communicator/communicator.h"

/*
 * function - returns the number of neighbors of a node
 *            associated with a graph topology
 *
 * @param comm communicator with graph topology (handle)
 * @param rank rank of process in group of 'comm' (integer)
 * @param nneighbors number of neighbors of specified process (integer)
 *
 * @retval MPI_SUCCESS
 */

int mca_topo_base_graph_neighbors_count (ompi_communicator_t* comm,
                                         int rank,
                                         int *nneighbors)
{
    mca_topo_base_comm_graph_2_2_0_t* graph = comm->c_topo->mtc.graph;
    *nneighbors = graph->index[rank];
    if (rank > 0) {
        *nneighbors -= graph->index[rank - 1];
    }

    return MPI_SUCCESS;
}
