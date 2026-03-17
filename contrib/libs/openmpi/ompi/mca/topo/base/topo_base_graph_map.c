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
 * Copyright (c) 2015      Research Organization for Information Science
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
 * function - mca_topo_base_graph_map
 *
 *  @param comm input communicator (handle)
 *  @param nnodes number of graph nodes (integer)
 *  @param index integer array specifying the graph structure
 *  @param edges integer array specifying the graph structure
 *  @param newrank reordered rank of the calling process; 'MPI_UNDEFINED'
 *                  if the calling process does not belong to
 *                  graph (integer)
 *
 *  @retval MPI_SUCCESS
 *  @retval MPI_UNDEFINED
 */

int mca_topo_base_graph_map(ompi_communicator_t * comm,
                            int nnodes,
                            const int *index, const int *edges, int *newrank)
{
    int myrank;

    myrank = ompi_comm_rank(comm);
    *newrank =
        ((0 > myrank) || (myrank >= nnodes)) ? MPI_UNDEFINED : myrank;

    return OMPI_SUCCESS;
}

