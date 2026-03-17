/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2006 The Trustees of Indiana University and Indiana
 *                    University Research and Technology
 *                    Corporation.  All rights reserved.
 * Copyright (c) 2006 The Technical University of Chemnitz. All
 *                    rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 *
 * Author(s): Torsten Hoefler <htor@cs.indiana.edu>
 *
 */

#include "nbc_internal.h"
#include "ompi/mca/topo/base/base.h"

int NBC_Comm_neighbors_count (ompi_communicator_t *comm, int *indegree, int *outdegree) {
  if (OMPI_COMM_IS_CART(comm)) {
    /* cartesian */
    /* outdegree is always 2*ndims because we need to iterate over empty buffers for MPI_PROC_NULL */
    *outdegree = *indegree = 2 * comm->c_topo->mtc.cart->ndims;
  } else if (OMPI_COMM_IS_GRAPH(comm)) {
    /* graph */
    int rank, nneighbors;

    rank = ompi_comm_rank (comm);
    mca_topo_base_graph_neighbors_count (comm, rank, &nneighbors);

    *outdegree = *indegree = nneighbors;
  } else if (OMPI_COMM_IS_DIST_GRAPH(comm)) {
    /* graph */
    *indegree = comm->c_topo->mtc.dist_graph->indegree;
    *outdegree = comm->c_topo->mtc.dist_graph->outdegree;
  } else {
    return OMPI_ERR_BAD_PARAM;
  }

  return OMPI_SUCCESS;
}

int NBC_Comm_neighbors (ompi_communicator_t *comm, int **sources, int *source_count, int **destinations, int *dest_count) {
  int res, indeg, outdeg;

  *sources = *destinations = NULL;

  res = NBC_Comm_neighbors_count(comm, &indeg, &outdeg);
  if (OMPI_SUCCESS != res) {
    return res;
  }

  *source_count = indeg;
  *dest_count = outdeg;

  if (indeg) {
    *sources = malloc (sizeof (int) * indeg);
    if (OPAL_UNLIKELY(NULL == *sources)) {
      return OMPI_ERR_OUT_OF_RESOURCE;
    }
  } else {
    *sources = NULL;
  }

  if (outdeg) {
    *destinations = malloc (sizeof (int) * outdeg);
    if (OPAL_UNLIKELY(NULL == *destinations)) {
      free (*sources);
      *sources = NULL;
      return OMPI_ERR_OUT_OF_RESOURCE;
    }
  } else {
    *destinations = NULL;
  }

  /* silence clang static analyzer warning about NULL-dereference */
  if (0 == indeg && 0 == outdeg) {
    return OMPI_SUCCESS;
  }

  if (OMPI_COMM_IS_CART(comm)) {
    /* cartesian */
    int rpeer, speer;

    /* silence clang static analyzer warning */
    assert (indeg == outdeg);

    for (int dim = 0, i = 0 ; dim < comm->c_topo->mtc.cart->ndims ; ++dim) {
      mca_topo_base_cart_shift (comm, dim, 1, &rpeer, &speer);
      sources[0][i] = destinations[0][i] = rpeer; i++;
      sources[0][i] = destinations[0][i] = speer; i++;
    }
  } else if (OMPI_COMM_IS_GRAPH(comm)) {
    /* graph */
    mca_topo_base_graph_neighbors (comm, ompi_comm_rank (comm), indeg, sources[0]);
    memcpy (destinations[0], sources[0], indeg * sizeof (int));
  } else if (OMPI_COMM_IS_DIST_GRAPH(comm)) {
    /* dist graph */
    mca_topo_base_dist_graph_neighbors (comm, indeg, sources[0], MPI_UNWEIGHTED, outdeg, destinations[0],
                                         MPI_UNWEIGHTED);
  }

  return OMPI_SUCCESS;
}
