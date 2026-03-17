/*
 * Copyright (c) 2014      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2017      Amazon.com, Inc. or its affiliates.  All Rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/* Implements an adjacency-list-based weighted directed graph (digraph),
 * focused on supporting bipartite digraphs and flow-network problems.
 *
 * Note that some operations might be more efficient if this structure were
 * converted to use an adjacency matrix instead of an adjacency list.  OTOH
 * that complicates other pieces of the implementation (specifically, adding
 * and removing edges). */

#ifndef OPAL_BP_GRAPH_H
#define OPAL_BP_GRAPH_H

struct opal_bp_graph_vertex_t;
struct opal_bp_graph_edge_t;
struct opal_bp_graph_t;

typedef struct opal_bp_graph_vertex_t opal_bp_graph_vertex_t;
typedef struct opal_bp_graph_edge_t opal_bp_graph_edge_t;
typedef struct opal_bp_graph_t opal_bp_graph_t;

/**
 * callback function pointer type for cleaning up user data associated with a
 * vertex or edge */
typedef void (*opal_bp_graph_cleanup_fn_t)(void *user_data);

/**
 * create a new empty graph
 *
 * Any new vertices will have NULL user data associated.
 *
 * @param[in] v_data_cleanup_fn  cleanup function to use for vertex user data
 * @param[in] e_data_cleanup_fn  cleanup function to use for edge user data
 * @param[out] g_out             the created graph
 *
 * @returns OPAL_SUCCESS or an OMPI error code
 */
int opal_bp_graph_create(opal_bp_graph_cleanup_fn_t v_data_cleanup_fn,
			 opal_bp_graph_cleanup_fn_t e_data_cleanup_fn,
			 opal_bp_graph_t **g_out);

/**
 * free the given graph
 *
 * Any user data associated with vertices or edges in the graph will have
 * the given edge/vertex cleanup callback invoked in some arbitrary order.
 *
 * @returns OPAL_SUCCESS or an OMPI error code
 */
int opal_bp_graph_free(opal_bp_graph_t *g);

/**
 * clone (deep copy) the given graph
 *
 * Note that copy_user_data==true is not currently supported (requires the
 * addition of a copy callback for user data).
 *
 * @param[in] g               the graph to clone
 * @param[in] copy_user_data  if true, copy vertex/edge user data to the new
 *                            graph
 * @param[in] g_clone_out     the resulting cloned graph
 * @returns OPAL_SUCCESS or an OMPI error code
 */
int opal_bp_graph_clone(const opal_bp_graph_t *g,
			bool copy_user_data,
			opal_bp_graph_t **g_clone_out);

/**
 * return the number of edges for which this vertex is a destination
 *
 * @param[in] g       the graph to query
 * @param[in] vertex  the vertex id to query
 * @returns the number of edges for which this vertex is a destination
 */
int opal_bp_graph_indegree(const opal_bp_graph_t *g,
			   int vertex);

/**
 * return the number of edges for which this vertex is a source
 *
 * @param[in] g       the graph to query
 * @param[in] vertex  the vertex id to query
 * @returns the number of edges for which this vertex is a source
 */
int opal_bp_graph_outdegree(const opal_bp_graph_t *g,
			    int vertex);

/**
 * add an edge to the given graph
 *
 * @param[in] from      source vertex ID
 * @param[in] to        target vertex ID
 * @param[in] cost      cost value for this edge (lower is better)
 * @param[in] capacity  maximum flow transmissible on this edge
 * @param[in] e_data    caller data to associate with this edge, useful for
 *                      debugging or minimizing state shared across components
 *
 * @returns OPAL_SUCCESS or an OMPI error code
 */
int opal_bp_graph_add_edge(opal_bp_graph_t *g,
			   int from,
			   int to,
			   int64_t cost,
			   int capacity,
			   void *e_data);

/**
 * add a vertex to the given graph
 *
 * @param[in]  g          graph to manipulate
 * @param[in]  v_data     data to associate with the new vertex
 * @param[out] index_out  integer index of the new vertex.  May be NULL.
 *
 * @returns OPAL_SUCCESS or an OMPI error code
 */
int opal_bp_graph_add_vertex(opal_bp_graph_t *g,
			     void *v_data,
			     int *index_out);

/**
 * compute the order of a graph (number of vertices)
 *
 * @param[in] g the graph to query
 */
int opal_bp_graph_order(const opal_bp_graph_t *g);

/**
 * This function solves the "assignment problem":
 * http://en.wikipedia.org/wiki/Assignment_problem
 *
 * The goal is to find a maximum cardinality, minimum cost matching in a
 * weighted bipartite graph.  Maximum cardinality takes priority over minimum
 * cost.
 *
 * Capacities in the given graph are ignored (assumed to be 1 at the start).
 * It is also assumed that the graph only contains edges from one vertex set
 * to the other and that no edges exist in the reverse direction ("forward"
 * edges only).
 *
 * The algorithm(s) used will be deterministic.  That is, given the exact same
 * graph, two calls to this routine will result in the same matching result.
 *
 * @param[in] g                     an acyclic bipartite directed graph for
 *                                  which a matching is sought
 * @param[out] num_match_edges_out  number edges found in the matching
 * @param[out] match_edges_out      an array of (u,v) vertex pairs indicating
 *                                  which edges are in the matching
 *
 * @returns OPAL_SUCCESS or an OMPI error code
 */
int opal_bp_graph_solve_bipartite_assignment(const opal_bp_graph_t *g,
					     int *num_match_edges_out,
					     int **match_edges_out);

#endif /* OPAL_BP_GRAPH_H */
