/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2012-2018 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2012-2013 Inria.  All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
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
#pragma weak MPI_Dist_graph_create = PMPI_Dist_graph_create
#endif
#define MPI_Dist_graph_create PMPI_Dist_graph_create
#endif

static const char FUNC_NAME[] = "MPI_Dist_graph_create";

int MPI_Dist_graph_create(MPI_Comm comm_old, int n, const int sources[],
                          const int degrees[], const int destinations[], const int weights[],
                          MPI_Info info, int reorder, MPI_Comm * newcomm)
{
    mca_topo_base_module_t* topo;
    int i, j, index, err, comm_size;

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
        } else if (n < 0 || NULL == newcomm) {
            return OMPI_ERRHANDLER_INVOKE(comm_old, MPI_ERR_ARG, FUNC_NAME);
        } else if (n > 0 && (NULL == sources || NULL == degrees ||
                              NULL == destinations || NULL == weights)) {
            return OMPI_ERRHANDLER_INVOKE(comm_old, MPI_ERR_ARG, FUNC_NAME);
        }
        /* Ensure the arrays are full of valid-valued integers */
        comm_size = ompi_comm_size(comm_old);
        for( i = index = 0; i < n; ++i ) {
            if (((sources[i] < 0) && (sources[i] != MPI_PROC_NULL)) || sources[i] >= comm_size) {
                return OMPI_ERRHANDLER_INVOKE(comm_old, MPI_ERR_ARG,
                                              FUNC_NAME);
            } else if (degrees[i] < 0) {
                return OMPI_ERRHANDLER_INVOKE(comm_old, MPI_ERR_ARG,
                                              FUNC_NAME);
            }
            for( j = 0; j < degrees[i]; ++j ) {
                if (((destinations[index] < 0) && (destinations[index] != MPI_PROC_NULL)) || destinations[index] >= comm_size) {
                    return OMPI_ERRHANDLER_INVOKE(comm_old, MPI_ERR_ARG,
                                                  FUNC_NAME);
                } else if (MPI_UNWEIGHTED != weights && weights[index] < 0) {
                    return OMPI_ERRHANDLER_INVOKE(comm_old, MPI_ERR_ARG,
                                                  FUNC_NAME);
                }
                index++;
            }
        }
    }

    /* Ensure there is a topo attached to this communicator */
    if(OMPI_SUCCESS != (err = mca_topo_base_comm_select(comm_old, NULL,
                                                        &topo, OMPI_COMM_DIST_GRAPH))) {
        return OMPI_ERRHANDLER_INVOKE(comm_old, err, FUNC_NAME);
    }

    err = topo->topo.dist_graph.dist_graph_create(topo, comm_old, n, sources, degrees,
                                                  destinations, weights, &(info->super),
                                                  reorder, newcomm);
    OMPI_ERRHANDLER_RETURN(err, comm_old, err, FUNC_NAME);
}

