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

#include "opal_config.h"

#include <stdlib.h>
#include <stddef.h>

#include "opal_stdint.h"
#include "opal/constants.h"
#include "opal/class/opal_list.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/util/output.h"
#include "opal/util/error.h"

#include "opal/util/bipartite_graph.h"
#include "opal/util/bipartite_graph_internal.h"

#ifndef container_of
#define container_of(ptr, type, member) ( \
        (type *)( ((char *)(ptr)) - offsetof(type,member) ))
#endif

#define GRAPH_DEBUG 0
#if GRAPH_DEBUG
#  define GRAPH_DEBUG_OUT(args) printf(args)
#else
#  define GRAPH_DEBUG_OUT(args) do {} while(0)
#endif

#define MAX_COST INT64_MAX

#ifndef MAX
#  define MAX(a,b) ((a) > (b) ? (a) : (b))
#endif

#ifndef MIN
#  define MIN(a,b) ((a) < (b) ? (a) : (b))
#endif

#define f(i,j) flow[n*i + j]

/* ensure that (a+b<=max) */
static inline void check_add64_overflow(int64_t a, int64_t b)
{
    assert(!((b > 0) && (a > (INT64_MAX - b))) &&
           !((b < 0) && (a < (INT64_MIN - b))));
}

static void edge_constructor(opal_bp_graph_edge_t *e)
{
    OBJ_CONSTRUCT(&e->outbound_li, opal_list_item_t);
    OBJ_CONSTRUCT(&e->inbound_li, opal_list_item_t);
}

static void edge_destructor(opal_bp_graph_edge_t *e)
{
    OBJ_DESTRUCT(&e->outbound_li);
    OBJ_DESTRUCT(&e->inbound_li);
}

OBJ_CLASS_DECLARATION(opal_bp_graph_edge_t);
OBJ_CLASS_INSTANCE(opal_bp_graph_edge_t, opal_object_t,
                   edge_constructor, edge_destructor);

static void dump_vec(const char *name, int *vec, int n)
    __opal_attribute_unused__;

static void dump_vec(const char *name, int *vec, int n)
{
    int i;
    fprintf(stderr, "%s={", name);
    for (i = 0; i < n; ++i) {
        fprintf(stderr, "[%d]=%2d, ", i, vec[i]);
    }
    fprintf(stderr, "}\n");
}

static void dump_vec64(const char *name, int64_t *vec, int n)
    __opal_attribute_unused__;

static void dump_vec64(const char *name, int64_t *vec, int n)
{
    int i;
    fprintf(stderr, "%s={", name);
    for (i = 0; i < n; ++i) {
        fprintf(stderr, "[%d]=%2" PRIi64 ", ", i, vec[i]);
    }
    fprintf(stderr, "}\n");
}


static void dump_flow(int *flow, int n)
    __opal_attribute_unused__;

static void dump_flow(int *flow, int n)
{
    int u, v;

    fprintf(stderr, "flow={\n");
    for (u = 0; u < n; ++u) {
        fprintf(stderr, "u=%d| ", u);
        for (v = 0; v < n; ++v) {
            fprintf(stderr, "%2d,", f(u,v));
        }
        fprintf(stderr, "\n");
    }
    fprintf(stderr, "}\n");
}


static int get_capacity(opal_bp_graph_t *g, int source, int target)
{
    opal_bp_graph_edge_t *e;

    CHECK_VERTEX_RANGE(g, source);
    CHECK_VERTEX_RANGE(g, target);

    FOREACH_OUT_EDGE(g, source, e) {
        assert(e->source == source);
        if (e->target == target) {
            return e->capacity;
        }
    }

    return 0;
}

static int
set_capacity(opal_bp_graph_t *g, int source, int target, int cap)
{
    opal_bp_graph_edge_t *e;

    CHECK_VERTEX_RANGE(g, source);
    CHECK_VERTEX_RANGE(g, target);

    FOREACH_OUT_EDGE(g, source, e) {
        assert(e->source == source);
        if (e->target == target) {
            e->capacity = cap;
            return OPAL_SUCCESS;
        }
    }

    return OPAL_ERR_NOT_FOUND;
}

static void free_vertex(opal_bp_graph_t *g,
                        opal_bp_graph_vertex_t *v)
{
    if (NULL != v) {
        if (NULL != g->v_data_cleanup_fn && NULL != v->v_data) {
            g->v_data_cleanup_fn(v->v_data);
        }
        free(v);
    }
}

int opal_bp_graph_create(opal_bp_graph_cleanup_fn_t v_data_cleanup_fn,
			 opal_bp_graph_cleanup_fn_t e_data_cleanup_fn,
			 opal_bp_graph_t **g_out)
{
    int err;
    opal_bp_graph_t *g = NULL;

    if (NULL == g_out) {
        return OPAL_ERR_BAD_PARAM;
    }
    *g_out = NULL;

    g = calloc(1, sizeof(*g));
    if (NULL == g) {
        OPAL_ERROR_LOG(OPAL_ERR_OUT_OF_RESOURCE);
        err = OPAL_ERR_OUT_OF_RESOURCE;
        goto out_free_g;
    }

    g->source_idx = -1;
    g->sink_idx = -1;

    g->v_data_cleanup_fn = v_data_cleanup_fn;
    g->e_data_cleanup_fn = e_data_cleanup_fn;

    /* now that we essentially have an empty graph, add vertices to it */
    OBJ_CONSTRUCT(&g->vertices, opal_pointer_array_t);
    err = opal_pointer_array_init(&g->vertices, 0, INT_MAX, 32);
    if (OPAL_SUCCESS != err) {
        goto out_free_g;
    }

    *g_out = g;
    return OPAL_SUCCESS;

 out_free_g:
    free(g);
    return err;
}

int opal_bp_graph_free(opal_bp_graph_t *g)
{
    int i;
    opal_bp_graph_edge_t *e, *next;
    opal_bp_graph_vertex_t *v;

    /* remove all edges from all out_edges lists */
    for (i = 0; i < NUM_VERTICES(g); ++i) {
        v = V_ID_TO_PTR(g, i);
        LIST_FOREACH_SAFE_CONTAINED(e, next, &v->out_edges,
                                    opal_bp_graph_edge_t, outbound_li) {
            opal_list_remove_item(&v->out_edges, &e->outbound_li);
            OBJ_RELEASE(e);
        }
    }
    /* now remove from all in_edges lists and free the edge */
    for (i = 0; i < NUM_VERTICES(g); ++i) {
        v = V_ID_TO_PTR(g, i);
        LIST_FOREACH_SAFE_CONTAINED(e, next, &v->in_edges,
                                    opal_bp_graph_edge_t, inbound_li) {
            opal_list_remove_item(&v->in_edges, &e->inbound_li);

            if (NULL != g->e_data_cleanup_fn && NULL != e->e_data) {
                g->e_data_cleanup_fn(e->e_data);
            }
            OBJ_RELEASE(e);
        }

        free_vertex(g, V_ID_TO_PTR(g, i));
        opal_pointer_array_set_item(&g->vertices, i, NULL);
    }
    g->num_vertices = 0;

    OBJ_DESTRUCT(&g->vertices);
    free(g);

    return OPAL_SUCCESS;
}

int opal_bp_graph_clone(const opal_bp_graph_t *g,
			bool copy_user_data,
			opal_bp_graph_t **g_clone_out)
{
    int err;
    int i;
    int index;
    opal_bp_graph_t *gx;
    opal_bp_graph_edge_t *e;

    if (NULL == g_clone_out) {
        return OPAL_ERR_BAD_PARAM;
    }
    *g_clone_out = NULL;

    if (copy_user_data) {
	opal_output(0, "[%s:%d:%s] user data copy requested but not yet supported", 
		    __FILE__, __LINE__, __func__);
        abort();
        return OPAL_ERR_FATAL;
    }

    gx = NULL;
    err = opal_bp_graph_create(NULL, NULL, &gx);
    if (OPAL_SUCCESS != err) {
        return err;
    }
    assert(NULL != gx);

    /* reconstruct all vertices */
    for (i = 0; i < NUM_VERTICES(g); ++i) {
        err = opal_bp_graph_add_vertex(gx, NULL, &index);
        if (OPAL_SUCCESS != err) {
            goto out_free_gx;
        }
        assert(index == i);
    }

    /* now reconstruct all the edges (iterate by source vertex only to avoid
     * double-adding) */
    for (i = 0; i < NUM_VERTICES(g); ++i) {
        FOREACH_OUT_EDGE(g, i, e) {
            assert(i == e->source);
            err = opal_bp_graph_add_edge(gx, e->source, e->target,
					    e->cost, e->capacity, NULL);
            if (OPAL_SUCCESS != err) {
                goto out_free_gx;
            }
        }
    }

    *g_clone_out = gx;
    return OPAL_SUCCESS;

 out_free_gx:
    /* we don't reach in and manipulate gx's state directly, so it should be
     * safe to use the standard free function */
    opal_bp_graph_free(gx);
    return err;
}

int opal_bp_graph_indegree(const opal_bp_graph_t *g,
			   int vertex)
{
    opal_bp_graph_vertex_t *v;

    v = V_ID_TO_PTR(g, vertex);
    return opal_list_get_size(&v->in_edges);
}

int opal_bp_graph_outdegree(const opal_bp_graph_t *g,
			       int vertex)
{
    opal_bp_graph_vertex_t *v;

    v = V_ID_TO_PTR(g, vertex);
    return opal_list_get_size(&v->out_edges);
}

int opal_bp_graph_add_edge(opal_bp_graph_t *g,
			   int from,
			   int to,
			   int64_t cost,
			   int capacity,
			   void *e_data)
{
    opal_bp_graph_edge_t *e;
    opal_bp_graph_vertex_t *v_from, *v_to;

    if (from < 0 || from >= NUM_VERTICES(g)) {
        return OPAL_ERR_BAD_PARAM;
    }
    if (to < 0 || to >= NUM_VERTICES(g)) {
        return OPAL_ERR_BAD_PARAM;
    }
    if (cost == MAX_COST) {
        return OPAL_ERR_BAD_PARAM;
    }
    if (capacity < 0) {
        /* negative cost is fine, but negative capacity is not currently
         * handled appropriately */
        return OPAL_ERR_BAD_PARAM;
    }
    FOREACH_OUT_EDGE(g, from, e) {
        assert(e->source == from);
        if (e->target == to) {
            return OPAL_EXISTS;
        }
    }

    /* this reference is owned by the out_edges list */
    e = OBJ_NEW(opal_bp_graph_edge_t);
    if (NULL == e) {
        OPAL_ERROR_LOG(OPAL_ERR_OUT_OF_RESOURCE);
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    e->source   = from;
    e->target   = to;
    e->cost     = cost;
    e->capacity = capacity;
    e->e_data   = e_data;

    v_from = V_ID_TO_PTR(g, from);
    opal_list_append(&v_from->out_edges, &e->outbound_li);

    OBJ_RETAIN(e); /* ref owned by in_edges list */
    v_to = V_ID_TO_PTR(g, to);
    opal_list_append(&v_to->in_edges, &e->inbound_li);

    return OPAL_SUCCESS;
}

int opal_bp_graph_add_vertex(opal_bp_graph_t *g,
			     void *v_data,
			     int *index_out)
{
    opal_bp_graph_vertex_t *v;

    v = calloc(1, sizeof(*v));
    if (NULL == v) {
        OPAL_ERROR_LOG(OPAL_ERR_OUT_OF_RESOURCE);
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    /* add to the ptr array early to simplify cleanup in the incredibly rare
     * chance that adding fails */
    v->v_index = opal_pointer_array_add(&g->vertices, v);
    if (-1 == v->v_index) {
        free(v);
        OPAL_ERROR_LOG(OPAL_ERR_OUT_OF_RESOURCE);
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    assert(v->v_index == g->num_vertices);

    ++g->num_vertices;

    v->v_data = v_data;
    OBJ_CONSTRUCT(&v->out_edges, opal_list_t);
    OBJ_CONSTRUCT(&v->in_edges, opal_list_t);

    if (NULL != index_out) {
        *index_out = v->v_index;
    }

    return OPAL_SUCCESS;
}

int opal_bp_graph_order(const opal_bp_graph_t *g)
{
    return NUM_VERTICES(g);
}

/**
 * shrink a flow matrix for old_n vertices to one works for new_n
 *
 * Takes a matrix stored in a one-dimensional array of size (old_n*old_n) and
 * "truncates" it into a dense array of size (new_n*new_n) that only contain
 * the flow values for the first new_n vertices.  E.g., it turns this array
 * (old_n=5, new_n=3):
 *
 *    1  2  3  4  5
 *    6  7  8  9 10
 *   11 12 13 14 15
 *   16 17 18 19 20
 *   21 22 23 24 25
 *
 * into this array;
 *
 *    1  2  3
 *    6  7  8
 *   11 12 13
 */
static void shrink_flow_matrix(int *flow, int old_n, int new_n)
{
    int u, v;

    assert(old_n > new_n);

    for (u = 0; u < new_n; ++u) {
        for (v = 0; v < new_n; ++v) {
            flow[new_n*u + v] = flow[old_n*u + v];
        }
    }
}

/**
 * Compute the so-called "bottleneck" capacity value for a path "pred" through
 * graph "gx".
 */
static int
bottleneck_path(
		opal_bp_graph_t *gx,
		int n,
		int *pred)
{
    int u, v;
    int min;

    min = INT_MAX;
    FOREACH_UV_ON_PATH(pred, gx->source_idx, gx->sink_idx, u, v) {
        int cap_f_uv = get_capacity(gx, u, v);
        min = MIN(min, cap_f_uv);
    }

    return min;
}


/**
 * This routine implements the Bellman-Ford shortest paths algorithm, slightly
 * specialized for our forumlation of flow networks:
 * http://en.wikipedia.org/wiki/Bellman%E2%80%93Ford_algorithm
 *
 * Specifically, it attempts to find the shortest path from "source" to
 * "target".  It returns true if such a path was found, false otherwise.  Any
 * found path is returned in "pred" as a predecessor chain (i.e., pred[sink]
 * is the start of the path and pred[pred[sink]] is its predecessor, etc.).
 *
 * The contents of "pred" are only valid if this routine returns true.
 */
bool opal_bp_graph_bellman_ford(opal_bp_graph_t *gx,
				int source,
				int target,
				int *pred)
{
    int64_t *dist;
    int i;
    int n;
    int u, v;
    bool found_target = false;

    if (NULL == gx) {
        OPAL_ERROR_LOG(OPAL_ERR_BAD_PARAM);
        return false;
    }
    if (NULL == pred) {
        OPAL_ERROR_LOG(OPAL_ERR_BAD_PARAM);
        return false;
    }
    if (source < 0 || source >= NUM_VERTICES(gx)) {
        return OPAL_ERR_BAD_PARAM;
    }
    if (target < 0 || target >= NUM_VERTICES(gx)) {
        return OPAL_ERR_BAD_PARAM;
    }

    /* initialize */
    n = opal_bp_graph_order(gx);
    dist = malloc(n * sizeof(*dist));
    if (NULL == dist) {
        OPAL_ERROR_LOG(OPAL_ERR_OUT_OF_RESOURCE);
        goto out;
    }
    for (i = 0; i < n; ++i) {
        dist[i] = MAX_COST;
        pred[i] = -1;
    }
    dist[source] = 0;

    /* relax repeatedly */
    for (i = 1; i < NUM_VERTICES(gx); ++i) {
        bool relaxed = false;
#if GRAPH_DEBUG
        dump_vec("pred", pred, NUM_VERTICES(gx));
        dump_vec64("dist", dist, NUM_VERTICES(gx));
#endif

        for (u = 0; u < NUM_VERTICES(gx); ++u) {
            opal_bp_graph_edge_t *e_ptr;

            FOREACH_OUT_EDGE(gx, u, e_ptr) {
                v = e_ptr->target;

                /* make sure to only construct paths from edges that actually have
                 * non-zero capacity */
                if (e_ptr->capacity > 0 &&
                    dist[u] != MAX_COST) { /* avoid signed overflow for "infinity" */
                    check_add64_overflow(dist[u], e_ptr->cost);
                    if ((dist[u] + e_ptr->cost) < dist[v]) {
                        dist[v] = dist[u] + e_ptr->cost;
                        pred[v] = u;
                        relaxed = true;
                    }
                }
            }
        }
        /* optimization: stop if an outer iteration did not succeed in
         * changing any dist/pred values (already at optimum) */
        if (!relaxed) {
            GRAPH_DEBUG_OUT(("relaxed==false, breaking out"));
            break;
        }
    }

    /* check for negative-cost cycles */
    for (u = 0; u < NUM_VERTICES(gx); ++u) {
        opal_bp_graph_edge_t * e_ptr;

        FOREACH_OUT_EDGE(gx, u, e_ptr) {
            v = e_ptr->target;
            if (e_ptr->capacity > 0 &&
                dist[u] != MAX_COST && /* avoid signed overflow */
                (dist[u] + e_ptr->cost) < dist[v]) {
                opal_output(0, "[%s:%d:%s] negative-weight cycle detected",
			    __FILE__, __LINE__, __func__);
                abort();
                goto out;
            }
        }
    }

    if (dist[target] != MAX_COST) {
        found_target = true;
    }

 out:
#if GRAPH_DEBUG
    dump_vec("pred", pred, NUM_VERTICES(gx));
#endif
    assert(pred[source] == -1);
    free(dist);
    GRAPH_DEBUG_OUT(("bellman_ford: found_target=%s", found_target ? "true" : "false"));
    return found_target;
}

/**
 * Transform the given connected, bipartite, acyclic digraph into a flow
 * network (i.e., add a source and a sink, with the source connected to vertex
 * set V1 and the sink connected to vertex set V2).  This also creates
 * residual edges suitable for augmenting-path algorithms.  All "source" nodes
 * in the original graph are considered to have an output of 1 and "sink"
 * nodes can take an input of 1.  The result is that "forward" edges are all
 * created with capacity=1, "backward" (residual) edges are created with
 * capacity=0.
 *
 * After this routine, all capacities are "residual capacities" ($c_f$ in the
 * literature).
 *
 * Initial flow throughout the network is assumed to be 0 at all edges.
 *
 * The graph will be left in an undefined state if an error occurs (though
 * freeing it should still be safe).
 */
int opal_bp_graph_bipartite_to_flow(opal_bp_graph_t *g)
{
    int err;
    int order;
    int u, v;
    int num_left, num_right;

    /* grab size before adding extra vertices */
    order = opal_bp_graph_order(g);

    err = opal_bp_graph_add_vertex(g, NULL, &g->source_idx);
    if (OPAL_SUCCESS != err) {
        return err;
    }
    err = opal_bp_graph_add_vertex(g, NULL, &g->sink_idx);
    if (OPAL_SUCCESS != err) {
        return err;
    }

    /* The networks we are interested in are bipartite and have edges only
     * from one partition to the other partition (none vice versa).  We
     * visualize this conventionally with all of the source vertices on the
     * left-hand side of an imaginary rendering of the graph and the target
     * vertices on the right-hand side of the rendering.  The direction
     * "forward" is considered to be moving from left to right.
     */
    num_left = 0;
    num_right = 0;
    for (u = 0; u < order; ++u) {
        int inbound = opal_bp_graph_indegree(g, u);
        int outbound = opal_bp_graph_outdegree(g, u);

        if (inbound > 0 && outbound > 0) {
            opal_output(0, "[%s:%d:%s] graph is not (unidirectionally) bipartite",
			__FILE__, __LINE__, __func__);
            abort();
        }
        else if (inbound > 0) {
            /* "right" side of the graph, create edges to the sink */
            ++num_right;
            err = opal_bp_graph_add_edge(g, u, g->sink_idx,
					    0, /* no cost */
					    /*capacity=*/1,
					    /*e_data=*/NULL);
            if (OPAL_SUCCESS != err) {
                GRAPH_DEBUG_OUT(("add_edge failed"));
                return err;
            }
        }
        else if (outbound > 0) {
            /* "left" side of the graph, create edges to the source */
            ++num_left;
            err = opal_bp_graph_add_edge(g, g->source_idx, u,
					    0, /* no cost */
					    /*capacity=*/1,
					    /*e_data=*/NULL);
            if (OPAL_SUCCESS != err) {
                GRAPH_DEBUG_OUT(("add_edge failed"));
                return err;
            }
        }
    }

    /* it doesn't make sense to extend this graph with a source and sink
     * unless */
    if (num_right == 0 || num_left == 0) {
        return OPAL_ERR_BAD_PARAM;
    }

    /* now run through and create "residual" edges as well (i.e., create edges
     * in the reverse direction with 0 initial flow and a residual capacity of
     * $c_f(u,v)=c(u,v)-f(u,v)$).  Residual edges can exist where no edges
     * exist in the original graph.
     */
    order = opal_bp_graph_order(g); /* need residuals for newly created
					  source/sink edges too */
    for (u = 0; u < order; ++u) {
        opal_bp_graph_edge_t * e_ptr;
        FOREACH_OUT_EDGE(g, u, e_ptr) {
            v = e_ptr->target;

            /* (u,v) exists, add (v,u) if not already present.  Cost is
             * negative for these edges because "giving back" flow pays us
             * back any cost already incurred. */
            err = opal_bp_graph_add_edge(g, v, u,
					    -e_ptr->cost,
					    /*capacity=*/0,
					    /*e_data=*/NULL);
            if (OPAL_SUCCESS != err && OPAL_EXISTS != err) {
                return err;
            }
        }
    }

    return OPAL_SUCCESS;
}

/**
 * Implements the "Successive Shortest Path" algorithm for computing the
 * minimum cost flow problem.  This is a generalized version of the
 * Ford-Fulkerson algorithm.  There are two major changes from F-F:
 *  1. In addition to capacities and flows, this algorithm pays attention to
 *     costs for traversing an edge.  This particular function leaves the
 *     caller's costs alone but sets its own capacities.
 *  2. Shortest paths are computed using the cost metric.
 *
 * The algorithm's sketch looks like:
 *  1    Transform network G by adding source and sink, create residual edges
 *  2    Initial flow x is zero
 *  3    while ( Gx contains a path from s to t ) do
 *  4        Find any shortest path P from s to t
 *  5        Augment current flow x along P
 *  6        update Gx
 *
 * This function mutates the given graph (adding vertices and edges, changing
 * capacties, etc.), so callers may wish to clone the graph before calling
 * this routine.
 *
 * The result is an array of (u,v) vertex pairs, where (u,v) is an edge in the
 * original graph which has non-zero flow.
 *
 * Returns OMPI error codes like OPAL_SUCCESS/OPAL_ERR_OUT_OF_RESOURCE.
 *
 * This version of the algorithm has a theoretical upper bound on its running
 * time of O(|V|^2 * |E| * f), where f is essentially the maximum flow in the
 * graph.  In our case, f=min(|V1|,|V2|), where V1 and V2 are the two
 * constituent sets of the bipartite graph.
 *
 * This algorithm's performance could probably be improved by modifying it to
 * use vertex potentials and Dijkstra's Algorithm instead of Bellman-Ford.
 * Normally vertex potentials are needed in order to use Dijkstra's safely,
 * but our graphs are constrained enough that this may not be necessary.
 * Switching to Dijkstra's implemented with a heap should yield a reduced
 * upper bound of O(|V| * |E| * f * log(|V|)).  Let's consider this a future
 * enhancement for the time being, since it's not obvious at this point that
 * the faster running time will be worth the additional implementation
 * complexity.
 */
static int min_cost_flow_ssp(opal_bp_graph_t *gx,
                             int **flow_out)
{
    int err = OPAL_SUCCESS;
    int n;
    int *pred = NULL;
    int *flow = NULL;
    int u, v;
    int c;

    GRAPH_DEBUG_OUT(("begin min_cost_flow_ssp()"));

    if (NULL == flow_out) {
        return OPAL_ERR_BAD_PARAM;
    }
    *flow_out = NULL;

    n = opal_bp_graph_order(gx);

    pred = malloc(n*sizeof(*pred));
    if (NULL == pred) {
        OPAL_ERROR_LOG(OPAL_ERR_OUT_OF_RESOURCE);
        err = OPAL_ERR_OUT_OF_RESOURCE;
        goto out_error;
    }

    /* "flow" is a 2d matrix of current flow values, all initialized to zero */
    flow = calloc(n*n, sizeof(*flow));
    if (NULL == flow) {
        OPAL_ERROR_LOG(OPAL_ERR_OUT_OF_RESOURCE);
        err = OPAL_ERR_OUT_OF_RESOURCE;
        goto out_error;
    }

    /* loop as long as paths exist from source to sink */
    while (opal_bp_graph_bellman_ford(gx, gx->source_idx, gx->sink_idx, pred)) {
        int cap_f_path;

        /* find any shortest path P from s to t (already present in pred) */
        GRAPH_DEBUG_OUT(("start outer iteration of SSP algorithm"));
#if GRAPH_DEBUG
        dump_vec("pred", pred, NUM_VERTICES(gx));
        dump_flow(flow, n);
#endif

        cap_f_path = bottleneck_path(gx, n, pred);

        /* augment current flow along P */
        FOREACH_UV_ON_PATH(pred, gx->source_idx, gx->sink_idx, u, v) {
            assert(u == pred[v]);

            f(u,v) = f(u,v) + cap_f_path; /* "forward" edge */
            f(v,u) = f(v,u) - cap_f_path; /* residual network edge */

            assert(f(u,v) == -f(v,u)); /* skew symmetry invariant */

            /* update Gx as we go along: decrease capacity by this new
             * augmenting flow */
            c = get_capacity(gx, u, v) - cap_f_path;
            assert(c >= 0);
            err = set_capacity(gx, u, v, c);
            if (OPAL_SUCCESS != err) {
                opal_output(0, "[%s:%d:%s] unable to set capacity, missing edge?",
			    __FILE__, __LINE__, __func__);
                abort();
            }

            c = get_capacity(gx, v, u) + cap_f_path;
            assert(c >= 0);
            err = set_capacity(gx, v, u, c);
            if (OPAL_SUCCESS != err) {
                opal_output(0, "[%s:%d:%s] unable to set capacity, missing edge?",
			    __FILE__, __LINE__, __func__);
                abort();
            }
        }
    }

 out:
    *flow_out = flow;
    free(pred);
    return err;

 out_error:
    free(*flow_out);
    GRAPH_DEBUG_OUT(("returning error %d", err));
    goto out;
}

int opal_bp_graph_solve_bipartite_assignment(const opal_bp_graph_t *g,
					     int *num_match_edges_out,
					     int **match_edges_out)
{
    int err;
    int i;
    int u, v;
    int n;
    int *flow = NULL;
    opal_bp_graph_t *gx = NULL;

    if (NULL == match_edges_out || NULL == num_match_edges_out) {
        return OPAL_ERR_BAD_PARAM;
    }
    *num_match_edges_out = 0;
    *match_edges_out = NULL;

    /* don't perturb the caller's data structure */
    err = opal_bp_graph_clone(g, false, &gx);
    if (OPAL_SUCCESS != err) {
        GRAPH_DEBUG_OUT(("opal_bp_graph_clone failed"));
        goto out;
    }

    /* Transform gx into a residual flow network with capacities, a source, a
     * sink, and residual edges.  We track the actual flow separately in the
     * "flow" matrix.  Initial capacity for every forward edge is 1.  Initial
     * capacity for every backward (residual) edge is 0.
     *
     * For the remainder of this routine (and the ssp routine) the capacities
     * refer to residual capacities ($c_f$) not capacities in the original
     * graph.  For convenience we adjust all residual capacities as we go
     * along rather than recomputing them from the flow and capacities in the
     * original graph.  This allows many other graph operations to have no
     * direct knowledge of the flow matrix.
     */
    err = opal_bp_graph_bipartite_to_flow(gx);
    if (OPAL_SUCCESS != err) {
        GRAPH_DEBUG_OUT(("bipartite_to_flow failed"));
        OPAL_ERROR_LOG(err);
        return err;
    }

    /* Use the SSP algorithm to compute the min-cost flow over this network.
     * Edges with non-zero flow in the result should be part of the matching.
     *
     * Note that the flow array returned is sized for gx, not for g.  Index
     * accordingly later on.
     */
    err = min_cost_flow_ssp(gx, &flow);
    if (OPAL_SUCCESS != err) {
        GRAPH_DEBUG_OUT(("min_cost_flow_ssp failed"));
        return err;
    }
    assert(NULL != flow);

    /* don't care about new edges in gx, only old edges in g */
    n = opal_bp_graph_order(g);

#if GRAPH_DEBUG
    dump_flow(flow, NUM_VERTICES(gx));
#endif
    shrink_flow_matrix(flow, opal_bp_graph_order(gx), n);
#if GRAPH_DEBUG
    dump_flow(flow, n);
#endif

    for (u = 0; u < n; ++u) {
        for (v = 0; v < n; ++v) {
            if (f(u,v) > 0) {
                ++(*num_match_edges_out);
            }
        }
    }

    if (0 == *num_match_edges_out) {
        /* avoid attempting to allocate a zero-byte buffer */
        goto out;
    }

    *match_edges_out = malloc(*num_match_edges_out * 2 * sizeof(int));
    if (NULL == *match_edges_out) {
        *num_match_edges_out = 0;
        OPAL_ERROR_LOG(OPAL_ERR_OUT_OF_RESOURCE);
        err = OPAL_ERR_OUT_OF_RESOURCE;
        goto out;
    }

    i = 0;
    for (u = 0; u < n; ++u) {
        for (v = 0; v < n; ++v) {
            /* flow exists on this edge so include this edge in the matching */
            if (f(u,v) > 0) {
                (*match_edges_out)[i++] = u;
                (*match_edges_out)[i++] = v;
            }
        }
    }

 out:
    free(flow);
    opal_bp_graph_free(gx);
    return err;
}
