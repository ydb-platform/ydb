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

/*
 * This file defines a number of internal structures to the BP graph
 * code which need to be exposed only for unit testing.  This file
 * should not be included in code that uses the BP graph interface.
 */

#ifndef BIPARTITE_GRAPH_INTERNAL
#define BIPARTITE_GRAPH_INTERNAL 1

struct opal_bp_graph_edge_t {
    opal_object_t super;

    opal_list_item_t outbound_li;
    opal_list_item_t inbound_li;

    /** source of this edge */
    int source;

    /** v_index of target of this edge */
    int target;

    /** cost (weight) of this edge */
    int64_t cost;

    /**
     * (flow-network) capacity of this edge.  Zero-capacity edges essentially do
     * not exist and will be ignored by most of the algorithms implemented here.
     */
    int capacity;

    /** any other information associated with this edge */
    void *e_data;
};

struct opal_bp_graph_vertex_t {
    /** index in the graph's array of vertices */
    int v_index;

    /** any other information associated with the vertex */
    void *v_data;

    /** linked list of edges for which this vertex is a source */
    opal_list_t out_edges;

    /** linked list of edges for which this vertex is a target */
    opal_list_t in_edges;
};

struct opal_bp_graph_t {
    /** number of vertices currently in this graph */
    int num_vertices;

    /** vertices in this graph (with number of set elements == num_vertices) */
    opal_pointer_array_t vertices;

    /** index of the source vertex, or -1 if not present */
    int source_idx;

    /** index of the sink vertex, or -1 if not present */
    int sink_idx;

    /** user callback to clean up the v_data */
    opal_bp_graph_cleanup_fn_t v_data_cleanup_fn;

    /** user callback to clean up the e_data */
    opal_bp_graph_cleanup_fn_t e_data_cleanup_fn;
};


#define LIST_FOREACH_CONTAINED(item, list, type, member)		\
    for (item = container_of( (list)->opal_list_sentinel.opal_list_next, type, member ); \
	 &item->member != &(list)->opal_list_sentinel;			\
	 item = container_of(						\
			     ((opal_list_item_t *) (&item->member))->opal_list_next, type, member ))

#define LIST_FOREACH_SAFE_CONTAINED(item, next, list, type, member)	\
    for (item = container_of( (list)->opal_list_sentinel.opal_list_next, type, member ), \
	     next = container_of(					\
				 ((opal_list_item_t *) (&item->member))->opal_list_next, type, member ); \
	 &item->member != &(list)->opal_list_sentinel;			\
	 item = next,							\
	     next = container_of(					\
				 ((opal_list_item_t *) (&item->member))->opal_list_next, type, member ))

#define NUM_VERTICES(g) (g->num_vertices)

#define CHECK_VERTEX_RANGE(g,v)			\
    do {					\
        if ((v) < 0 ||				\
            (v) >= NUM_VERTICES(g)) {		\
            return OPAL_ERR_BAD_PARAM;		\
        }					\
    } while (0)

/* cast away any constness of &g->vertices b/c the opal_pointer_array API is
 * not const-correct */
#define V_ID_TO_PTR(g, v_id)						\
    ((opal_bp_graph_vertex_t *)						\
     opal_pointer_array_get_item((opal_pointer_array_t *)&g->vertices, v_id))

#define FOREACH_OUT_EDGE(g,v_id,e_ptr)				\
    LIST_FOREACH_CONTAINED(e_ptr,				\
                           &(V_ID_TO_PTR(g, v_id)->out_edges),	\
                           opal_bp_graph_edge_t,		\
                           outbound_li)

#define FOREACH_IN_EDGE(g,v_id,e_ptr)				\
    LIST_FOREACH_CONTAINED(e_ptr,				\
                           &(V_ID_TO_PTR(g, v_id)->in_edges),	\
                           opal_bp_graph_edge_t,		\
                           inbound_li)


/* Iterate over (u,v) edge pairs along the given path, where path is defined
 * by the predecessor array "pred".  Stops when a -1 predecessor is
 * encountered.  Note: because it is a *predecessor* array, the traversal
 * starts at the sink and progresses towards the source. */
#define FOREACH_UV_ON_PATH(pred, source, sink, u, v)		\
    for (u = pred[sink], v = sink; u != -1; v = u, u = pred[u])


bool opal_bp_graph_bellman_ford(opal_bp_graph_t *gx,
				int source,
				int target,
				int *pred);

int opal_bp_graph_bipartite_to_flow(opal_bp_graph_t *g);

#endif
