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
 * function - Retrieves graph topology information associated with a
 *            communicator
 *
 * @param comm - communicator for group with graph structure (handle)
 * @param nodes - number of nodes in graph (integer)
 * @param nedges - number of edges in graph (integer)
 *
 * @retval MPI_SUCCESS
 * @retval MPI_ERR_TOPOLOGY
 * @retval MPI_ERR_COMM
 * @retval MPI_ERR_ARG
 */
int mca_topo_base_graphdims_get (ompi_communicator_t* comm,
                                 int *nodes,
                                 int *nedges)
{
    mca_topo_base_comm_graph_2_2_0_t* graph = comm->c_topo->mtc.graph;
    *nodes = ompi_comm_size(comm);
    *nedges = graph->index[*nodes -1];

    return MPI_SUCCESS;
}
