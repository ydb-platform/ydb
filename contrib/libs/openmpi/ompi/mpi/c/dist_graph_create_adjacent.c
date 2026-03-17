/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2008      The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2011-2018 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013-2014 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2012-2013 Inria.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * Author(s): Torsten Hoefler
 *
 */

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
#pragma weak MPI_Dist_graph_create_adjacent = PMPI_Dist_graph_create_adjacent
#endif
#define MPI_Dist_graph_create_adjacent PMPI_Dist_graph_create_adjacent
#endif

static const char FUNC_NAME[] = "MPI_Dist_graph_create_adjacent";


int MPI_Dist_graph_create_adjacent(MPI_Comm comm_old,
                                   int indegree, const int sources[],
                                   const int sourceweights[], int outdegree,
                                   const int destinations[], const int destweights[],
                                   MPI_Info info, int reorder,
                                   MPI_Comm *comm_dist_graph)
{
    mca_topo_base_module_t* topo;
    int i, comm_size, err;

    MEMCHECKER(
        memchecker_comm(comm_old);
    );

    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (ompi_comm_invalid(comm_old)) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COMM,
                                          FUNC_NAME);
        } else if (OMPI_COMM_IS_INTER(comm_old)) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COMM,
                                          FUNC_NAME);
        } else if (indegree < 0 || outdegree < 0 || NULL == comm_dist_graph) {
            return OMPI_ERRHANDLER_INVOKE(comm_old, MPI_ERR_ARG,
                                          "MPI_Dist_graph_create_adjacent negative degree");
        } else if ((indegree > 0 &&
                    (NULL == sources || NULL == sourceweights)) ||
                   (outdegree > 0 &&
                    (NULL == destinations || NULL == destweights))) {
            return OMPI_ERRHANDLER_INVOKE(comm_old, MPI_ERR_ARG, "MPI_Dist_graph_create_adjacent mismatched sources or destinations");
        }
        comm_size = ompi_comm_size(comm_old);
        for (i = 0; i < indegree; ++i) {
            if (((sources[i] < 0) && (sources[i] != MPI_PROC_NULL)) || sources[i] >= comm_size) {
                return OMPI_ERRHANDLER_INVOKE(comm_old, MPI_ERR_ARG,
                                              "MPI_Dist_graph_create_adjacent invalid sources");
            } else if (MPI_UNWEIGHTED != sourceweights && sourceweights[i] < 0) {
                return OMPI_ERRHANDLER_INVOKE(comm_old, MPI_ERR_ARG,
                                              "MPI_Dist_graph_create_adjacent invalid sourceweights");
            }
        }
        for (i = 0; i < outdegree; ++i) {
            if (((destinations[i] < 0) && (destinations[i] != MPI_PROC_NULL)) || destinations[i] >= comm_size) {
                return OMPI_ERRHANDLER_INVOKE(comm_old, MPI_ERR_ARG,
                                              "MPI_Dist_graph_create_adjacent invalid destinations");
            } else if (MPI_UNWEIGHTED != destweights && destweights[i] < 0) {
                return OMPI_ERRHANDLER_INVOKE(comm_old, MPI_ERR_ARG,
                                              "MPI_Dist_graph_create_adjacent invalid destweights");
            }
        }
    }

    /* Ensure there is a topo attached to this communicator */
    if(OMPI_SUCCESS != (err = mca_topo_base_comm_select(comm_old, NULL,
                                                        &topo, OMPI_COMM_DIST_GRAPH))) {
        return OMPI_ERRHANDLER_INVOKE(comm_old, err, FUNC_NAME);
    }

    err = topo->topo.dist_graph.dist_graph_create_adjacent(topo, comm_old, indegree,
                                                           sources, sourceweights, outdegree,
                                                           destinations, destweights, &(info->super),
                                                           reorder, comm_dist_graph);
    OMPI_ERRHANDLER_RETURN(err, comm_old, err, FUNC_NAME);
}

