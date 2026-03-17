/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
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
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2012-2013 Inria.  All rights reserved.
 * Copyright (c) 2014-2015 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_TOPO_H
#define MCA_TOPO_H

#include "ompi_config.h"

#include "ompi/mca/mca.h"
#include "opal/mca/base/base.h"
#include "ompi/communicator/communicator.h"

/* Forward reference to ompi_proc_t */
struct ompi_proc_t;
typedef struct mca_topo_base_module_t mca_topo_base_module_t;

/*
 * Initial component query, called during mca_topo_base_open.
 */
typedef int (*mca_topo_base_component_init_query_2_2_0_fn_t)
    (bool enable_progress_threads,
     bool enable_mpi_threads);

/*
 * Communicator query, called during cart and graph communicator
 * creation.
 */
typedef struct mca_topo_base_module_t*
(*mca_topo_base_component_comm_query_2_2_0_fn_t)
    (const ompi_communicator_t *comm, int *priority, uint32_t type);

/*
 * Structure for topo v2.1.0 components.This is chained to MCA v2.0.0
 */
typedef struct mca_topo_base_component_2_2_0_t {
    mca_base_component_t topoc_version;
    mca_base_component_data_t topoc_data;

    mca_topo_base_component_init_query_2_2_0_fn_t topoc_init_query;
    mca_topo_base_component_comm_query_2_2_0_fn_t topoc_comm_query;
} mca_topo_base_component_2_2_0_t;
typedef mca_topo_base_component_2_2_0_t mca_topo_base_component_t;

/*
 * Struct for holding graph communicator information
 */
typedef struct mca_topo_base_comm_graph_2_2_0_t {
    /* Make this structure be an object so that it has a constructor
       and destructor. */
    opal_object_t super;
    int nnodes;
    int *index;
    int *edges;
} mca_topo_base_comm_graph_2_2_0_t;
typedef mca_topo_base_comm_graph_2_2_0_t mca_topo_base_comm_graph_t;

OMPI_DECLSPEC OBJ_CLASS_DECLARATION(mca_topo_base_comm_graph_2_2_0_t);

/*
 * Struct for holding cartesian communicator information
 */
typedef struct mca_topo_base_comm_cart_2_2_0_t {
    /* Make this structure be an object so that it has a constructor
       and destructor. */
    opal_object_t super;
    int ndims;
    int *dims;
    int *periods;
    int *coords;
} mca_topo_base_comm_cart_2_2_0_t;
typedef mca_topo_base_comm_cart_2_2_0_t mca_topo_base_comm_cart_t;

OMPI_DECLSPEC OBJ_CLASS_DECLARATION(mca_topo_base_comm_cart_2_2_0_t);

/*
 * Struct for holding distributed graph information
 */
typedef struct mca_topo_base_comm_dist_graph_2_2_0_t {
    /* Make this structure be an object so that it has a constructor
       and destructor. */
    opal_object_t super;
    int *in;
    int *inw;
    int *out;
    int *outw;
    int indegree, outdegree;
    bool weighted;
} mca_topo_base_comm_dist_graph_2_2_0_t;
typedef mca_topo_base_comm_dist_graph_2_2_0_t mca_topo_base_comm_dist_graph_t;

OMPI_DECLSPEC OBJ_CLASS_DECLARATION(mca_topo_base_comm_dist_graph_2_2_0_t);

/*
 * This union must be declared (can't be anonymous in the struct where
 * it is used) because we need to be able to resolve it and find field
 * offsets in the ompi/debuggers/ stuff (i.e., so that debuggers can
 * parse/understand the individual fields on communicators).
 */
typedef union mca_topo_base_comm_cgd_union_2_2_0_t {
    mca_topo_base_comm_graph_2_2_0_t*      graph;
    mca_topo_base_comm_cart_2_2_0_t*       cart;
    mca_topo_base_comm_dist_graph_2_2_0_t* dist_graph;
} mca_topo_base_comm_cgd_union_2_2_0_t;
typedef mca_topo_base_comm_cgd_union_2_2_0_t mca_topo_base_comm_cgd_union_t;

/**
 * The logic for creating communicators with attached topologies is
 * the following. A new placeholder communicator is prepared before
 * the *_create function is called, and is initialized with minimal
 * information. As an example, this communicator is not safe neither
 * for point-to-point nor collective messages, it is provided only
 * as a placeholder. However, the original communicator where the
 * topology function was called upon is provided as well, in order to have
 * a valid medium for messaging. In return from the *_create functions,
 * a new group of processes is expected one containing all processes in
 * the local_group of the new communicator. Once this information
 * returned the new communicator will be fully initialized and activated.
 */

/*
 * Module function typedefs
 */

/* Back end for MPI_CART_COORDS */
typedef int (*mca_topo_base_module_cart_coords_fn_t)
                    (struct ompi_communicator_t *comm,
                     int rank,
                     int maxdims,
                     int *coords);

/* Back end for MPI_CART_CREATE */
typedef int (*mca_topo_base_module_cart_create_fn_t)
                    (mca_topo_base_module_t *topo_module,
                     ompi_communicator_t* old_comm,
                     int ndims,
                     const int *dims,
                     const int *periods,
                     bool reorder,
                     ompi_communicator_t** comm_topo);

/* Back end for MPI_CART_GET */
typedef int (*mca_topo_base_module_cart_get_fn_t)
                    (struct ompi_communicator_t *comm,
                     int maxdims,
                     int *dims,
                     int *periods,
                     int *coords);

/* Back end for MPI_CARTDIM_GET */
typedef int (*mca_topo_base_module_cartdim_get_fn_t)
                    (struct ompi_communicator_t *comm,
                     int *ndims);

/* Back end for MPI_CART_MAP */
typedef int (*mca_topo_base_module_cart_map_fn_t)
                    (struct ompi_communicator_t *comm,
                     int ndims,
                     const int *dims,
                     const int *periods,
                     int *newrank);

/* Back end for MPI_CART_RANK */
typedef int (*mca_topo_base_module_cart_rank_fn_t)
                    (struct ompi_communicator_t *comm,
                     const int *coords,
                     int *rank);

/* Back end for MPI_CART_SHIFT */
typedef int (*mca_topo_base_module_cart_shift_fn_t)
                    (struct ompi_communicator_t *comm,
                     int direction,
                     int disp,
                     int *rank_source,
                     int *rank_dest);

/* Back end for MPI_CART_SUB */
typedef int (*mca_topo_base_module_cart_sub_fn_t)
                    (struct ompi_communicator_t *comm,
                     const int *remain_dims,
                     struct ompi_communicator_t ** new_comm);

/* Back end for MPI_GRAPH_CREATE */
typedef int (*mca_topo_base_module_graph_create_fn_t)
                    (mca_topo_base_module_t *topo_module,
                     ompi_communicator_t* old_comm,
                     int nnodes,
                     const int *index,
                     const int *edges,
                     bool reorder,
                     ompi_communicator_t** new_comm);

/* Back end for MPI_GRAPH_GET */
typedef int (*mca_topo_base_module_graph_get_fn_t)
                    (struct ompi_communicator_t *comm,
                     int maxindex,
                     int maxedges,
                     int *index,
                     int *edges);

/* Back end for MPI_GRAPH_MAP */
typedef int (*mca_topo_base_module_graph_map_fn_t)
                    (struct ompi_communicator_t *comm,
                     int nnodes,
                     const int *index,
                     const int *edges,
                     int *newrank);

/* Back end for MPI_GRAPHDIMS_GET */
typedef int (*mca_topo_base_module_graphdims_get_fn_t)
                    (struct ompi_communicator_t *comm,
                     int *nnodes,
                     int *nnedges);

/* Back end for MPI_GRAPH_NEIGHBORS */
typedef int (*mca_topo_base_module_graph_neighbors_fn_t)
                    (struct ompi_communicator_t *comm,
                     int rank,
                     int maxneighbors,
                     int *neighbors);

/* Back end for MPI_GRAPH_NEIGHBORS_COUNT */
typedef int (*mca_topo_base_module_graph_neighbors_count_fn_t)
                    (struct ompi_communicator_t *comm,
                     int rank,
                     int *nneighbors);

/* Back end for MPI_DIST_GRAPH_CREATE */
typedef int (*mca_topo_base_module_dist_graph_create_fn_t)
                    (struct mca_topo_base_module_t* module,
                     struct ompi_communicator_t *old_comm,
                     int n, const int nodes[],
                     const int degrees[], const int targets[], const int weights[],
                     struct opal_info_t *info, int reorder,
                     struct ompi_communicator_t **new_comm);

/* Back end for MPI_DIST_GRAPH_CREATE_ADJACENT */
typedef int (*mca_topo_base_module_dist_graph_create_adjacent_fn_t)
                    (struct mca_topo_base_module_t* module,
                     ompi_communicator_t *comm_old,
                     int indegree, const int sources[],
                     const int sourceweights[],
                     int outdegree,
                     const int destinations[],
                     const int destweights[],
                     struct opal_info_t *info, int reorder,
                     ompi_communicator_t **comm_dist_graph);

/* Back end for MPI_DIST_GRAPH_NEIGHBORS */
typedef int (*mca_topo_base_module_dist_graph_neighbors_fn_t)
                    (struct ompi_communicator_t *comm,
                     int maxindegree,
                     int sources[], int sourceweights[],
                     int maxoutdegree, int destinations[],
                     int destweights[]);

/* Back end for MPI_DIST_GRAPH_NEIGHBORS_COUNT */
typedef int (*mca_topo_base_module_dist_graph_neighbors_count_fn_t)
                    (struct ompi_communicator_t *comm,
                     int *inneighbors, int *outneighbors, int *weighted);

/*
 * Topo module structure.  If a given topo module needs to cache more
 * information than what is contained in the mca_topo_base_module_t, it should
 * create its own module struct that uses mca_topo_base_module_t as a super.
 *
 * A module only needs to define two vital functions: the create and the map (or
 * equivalent). However, if no specialized functions are provided, they will be
 * automatically replaced by their default version. They will return the answers
 * based on the base information stored in the associated module extra data.
 */
typedef struct mca_topo_base_cart_module_2_2_0_t {
    mca_topo_base_module_cart_coords_fn_t cart_coords;
    mca_topo_base_module_cart_create_fn_t cart_create;
    mca_topo_base_module_cart_get_fn_t    cart_get;
    mca_topo_base_module_cartdim_get_fn_t cartdim_get;
    mca_topo_base_module_cart_map_fn_t    cart_map;
    mca_topo_base_module_cart_rank_fn_t   cart_rank;
    mca_topo_base_module_cart_shift_fn_t  cart_shift;
    mca_topo_base_module_cart_sub_fn_t    cart_sub;
} mca_topo_base_cart_module_2_2_0_t;

typedef struct mca_topo_base_graph_module_2_2_0_t {
    mca_topo_base_module_graph_create_fn_t          graph_create;
    mca_topo_base_module_graph_get_fn_t             graph_get;
    mca_topo_base_module_graph_map_fn_t             graph_map;
    mca_topo_base_module_graphdims_get_fn_t         graphdims_get;
    mca_topo_base_module_graph_neighbors_fn_t       graph_neighbors;
    mca_topo_base_module_graph_neighbors_count_fn_t graph_neighbors_count;
} mca_topo_base_graph_module_2_2_0_t;

typedef struct mca_topo_base_dist_graph_module_2_2_0_t {
    mca_topo_base_module_dist_graph_create_fn_t          dist_graph_create;
    mca_topo_base_module_dist_graph_create_adjacent_fn_t dist_graph_create_adjacent;
    mca_topo_base_module_dist_graph_neighbors_fn_t       dist_graph_neighbors;
    mca_topo_base_module_dist_graph_neighbors_count_fn_t dist_graph_neighbors_count;
} mca_topo_base_dist_graph_module_2_2_0_t;

struct mca_topo_base_module_t {
    /* Make this structure be an object so that it has a constructor
       and destructor. */
    opal_object_t              super;

    uint32_t                   type;             /* type of topology */
    bool                       reorder;          /* reordering was required */
    mca_topo_base_component_t* topo_component;   /* Component of this topo module */

    /* Cart, graph or dist graph related functions */
    union {
        mca_topo_base_cart_module_2_2_0_t cart;
        mca_topo_base_graph_module_2_2_0_t graph;
        mca_topo_base_dist_graph_module_2_2_0_t dist_graph;
    } topo;

    /* This union caches the parameters passed when the communicator
       was created.  Look in comm->c_flags to figure out whether this
       is a cartesian, graph, or dist graph communicator. */
    mca_topo_base_comm_cgd_union_t mtc;
};

OMPI_DECLSPEC OBJ_CLASS_DECLARATION(mca_topo_base_module_t);

/*
 * ******************************************************************
 * ********** Use in components that are of type topo v2.2.0 ********
 * ******************************************************************
 */
#define MCA_TOPO_BASE_VERSION_2_2_0 \
    OMPI_MCA_BASE_VERSION_2_1_0("topo", 2, 2, 0)

#endif /* MCA_TOPO_H */
