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
 * Copyright (c) 2008      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2012-2013 Los Alamos National Security, Inc.  All rights reserved.
 * Copyright (c) 2012-2013 Inria.  All rights reserved.
 * Copyright (c) 2014-2015 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_TOPO_BASE_H
#define MCA_TOPO_BASE_H

#include "ompi_config.h"

#include "opal/class/opal_list.h"
#include "ompi/mca/mca.h"
#include "ompi/mca/topo/topo.h"
#include "ompi/proc/proc.h"
#include "ompi/communicator/communicator.h"
#include "ompi/info/info.h"

/*
 * All stuff goes in here
 */

BEGIN_C_DECLS

/* Lazily initialize the framework (if it wasn't already) */
OMPI_DECLSPEC int mca_topo_base_lazy_init(void);

/*
 * MCA Framework
 */
OMPI_DECLSPEC extern mca_base_framework_t ompi_topo_base_framework;

/* Select a topo module for a particular type of topology */
OMPI_DECLSPEC int
mca_topo_base_comm_select(const ompi_communicator_t*  comm,
                          mca_topo_base_module_t*     preferred_module,
                          mca_topo_base_module_t**    selected_module,
                          uint32_t                    type);

/* Find all components that want to be considered in this job */
OMPI_DECLSPEC int
mca_topo_base_find_available(bool enable_progress_threads,
                             bool enable_mpi_threads);

/*
 * All the glue functions which we will provide to the users by
 * default. The component authors need to only write back-end
 * functions for cart_create(), graph_create(), graph_map(), and
 * cart_map() for their topology components.  But they can implement
 * these glue functions if they want.
 *
 * These glue functions
 */
OMPI_DECLSPEC int
mca_topo_base_cart_create(mca_topo_base_module_t *topo_module,
                          ompi_communicator_t* old_comm,
                          int ndims,
                          const int *dims,
                          const int *periods,
                          bool reorder,
                          ompi_communicator_t** comm_topo);

OMPI_DECLSPEC int
mca_topo_base_cart_coords(ompi_communicator_t *comm,
                          int rank,
                          int maxdims,
                          int *coords);

OMPI_DECLSPEC int
mca_topo_base_cartdim_get(ompi_communicator_t *comm,
                          int *ndims);

OMPI_DECLSPEC int
mca_topo_base_cart_get(ompi_communicator_t *comm,
                       int maxdims,
                       int *dims,
                       int *periods,
                       int *coords);

OMPI_DECLSPEC int
mca_topo_base_cart_map(ompi_communicator_t * comm,
                       int ndims,
                       const int *dims, const int *periods, int *newrank);

OMPI_DECLSPEC int
mca_topo_base_cart_rank(ompi_communicator_t *comm,
                        const int *coords,
                        int *rank);

OMPI_DECLSPEC int
mca_topo_base_cart_shift(ompi_communicator_t *comm,
                         int direction,
                         int disp,
                         int *rank_source,
                         int *rank_dest);

OMPI_DECLSPEC int
mca_topo_base_cart_sub(ompi_communicator_t *comm,
                       const int *remain_dims,
                       ompi_communicator_t **new_comm);

OMPI_DECLSPEC int
mca_topo_base_graphdims_get(ompi_communicator_t *comm,
                            int *nodes,
                            int *nedges);

OMPI_DECLSPEC int
mca_topo_base_graph_create(mca_topo_base_module_t *topo_module,
                           ompi_communicator_t* old_comm,
                           int nnodes,
                           const int *index,
                           const int *edges,
                           bool reorder,
                           ompi_communicator_t** new_comm);

OMPI_DECLSPEC int
mca_topo_base_graph_get(ompi_communicator_t *comm,
                        int maxindex,
                        int maxedges,
                        int *index,
                        int *edges);

OMPI_DECLSPEC int
mca_topo_base_graph_map(ompi_communicator_t * comm,
                        int nnodes,
                        const int *index, const int *edges, int *newrank);

OMPI_DECLSPEC int
mca_topo_base_graph_neighbors(ompi_communicator_t *comm,
                              int rank,
                              int maxneighbors,
                              int *neighbors);

OMPI_DECLSPEC int
mca_topo_base_graph_neighbors_count(ompi_communicator_t *comm,
                                    int rank,
                                    int *nneighbors);

/**
 * Efficiently distribute the information about the distributed graph as
 * submitted through the distributed graph interface.
 */
OMPI_DECLSPEC int
mca_topo_base_dist_graph_distribute(mca_topo_base_module_t* module,
                                    ompi_communicator_t *comm,
                                    int n, const int nodes[],
                                    const int degrees[], const int targets[],
                                    const int weights[],
                                    mca_topo_base_comm_dist_graph_2_2_0_t** ptopo);

OMPI_DECLSPEC int
mca_topo_base_dist_graph_create(mca_topo_base_module_t* module,
                                ompi_communicator_t *old_comm,
                                int n, const int nodes[],
                                const int degrees[], const int targets[], const int weights[],
                                opal_info_t *info, int reorder,
                                ompi_communicator_t **new_comm);

OMPI_DECLSPEC int
mca_topo_base_dist_graph_create_adjacent(mca_topo_base_module_t* module,
                                         ompi_communicator_t *old_comm,
                                         int indegree, const int sources[],
                                         const int sourceweights[], int outdegree,
                                         const int destinations[], const int destweights[],
                                         opal_info_t *info, int reorder,
                                         ompi_communicator_t **comm_dist_graph);

OMPI_DECLSPEC int
mca_topo_base_dist_graph_neighbors(ompi_communicator_t *comm,
                                   int maxindegree,
                                   int sources[], int sourceweights[],
                                   int maxoutdegree, int destinations[],
                                   int destweights[]);

OMPI_DECLSPEC int
mca_topo_base_dist_graph_neighbors_count(ompi_communicator_t *comm,
                                         int *inneighbors, int *outneighbors, int *weighted);


int mca_topo_base_neighbor_count (ompi_communicator_t *comm, int *indegree, int *outdegree);

END_C_DECLS

#endif /* MCA_BASE_TOPO_H */
