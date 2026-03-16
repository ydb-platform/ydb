/*
 * Copyright (c) 2008 The Trustees of Indiana University and Indiana
 *                    University Research and Technology
 *                    Corporation.  All rights reserved.
 * Copyright (c) 2009 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 */

#include "ompi_config.h"

#include "ompi/communicator/communicator.h"
#include "ompi/info/info.h"
#include "ompi/mca/topo/base/base.h"


int mca_topo_base_dist_graph_neighbors_count(ompi_communicator_t *comm,
                                             int *inneighbors,
                                             int *outneighbors, int *weighted)
{
    mca_topo_base_comm_dist_graph_2_2_0_t* dist_graph = comm->c_topo->mtc.dist_graph;

    if (!OMPI_COMM_IS_DIST_GRAPH(comm)) {
        return OMPI_ERR_NOT_FOUND;
    }

    *inneighbors = dist_graph->indegree;
    *outneighbors = dist_graph->outdegree;
    *weighted = (int)dist_graph->weighted;

    return MPI_SUCCESS;
}
