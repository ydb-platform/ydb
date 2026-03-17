/*
 * Copyright (c) 2008      The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011-2013 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2011-2013 Inria.  All rights reserved.
 * Copyright (c) 2011-2013 Universite Bordeaux 1
 * Copyright (c) 2014-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 */

#include "ompi_config.h"

#include "ompi/communicator/communicator.h"
#include "ompi/info/info.h"
#include "ompi/mca/topo/base/base.h"


int mca_topo_base_dist_graph_neighbors(ompi_communicator_t *comm,
                                       int maxindegree,
                                       int sources[], int sourceweights[],
                                       int maxoutdegree, int destinations[],
                                       int destweights[])
{
    mca_topo_base_comm_dist_graph_2_2_0_t *dg = comm->c_topo->mtc.dist_graph;
    int i;

    if (!OMPI_COMM_IS_DIST_GRAPH(comm)) {
        return OMPI_ERR_NOT_FOUND;
    }
    if (maxindegree > dg->indegree) {
        maxindegree = dg->indegree;
    }
    if (maxoutdegree > dg->outdegree) {
        maxoutdegree = dg->outdegree;
    }

    for (i = 0; i < maxindegree; ++i) {
        sources[i] = dg->in[i];
        if (MPI_UNWEIGHTED != sourceweights && NULL != dg->inw) {
            sourceweights[i] = dg->inw[i];
        }
    }
    for (i = 0; i < maxoutdegree; ++i) {
        destinations[i] = dg->out[i];
        if (MPI_UNWEIGHTED != destweights && NULL != dg->outw) {
            destweights[i] = dg->outw[i];
        }
    }

    return MPI_SUCCESS;
}

