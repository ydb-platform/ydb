/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2012 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2007 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Voltaire All rights reserved.
 * Copyright (c) 2016-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016 Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include "opal/class/opal_list.h"
#include "opal/constants.h"
#include "opal/class/opal_graph.h"
#include "opal/util/output.h"

static int compare_vertex_distance(const void *item1, const void *item2);

/*
 *  Graph classes
 */


static void opal_graph_vertex_construct(opal_graph_vertex_t *vertex);
static void opal_graph_vertex_destruct(opal_graph_vertex_t *vertex);

OBJ_CLASS_INSTANCE(
    opal_graph_vertex_t,
    opal_list_item_t,
    opal_graph_vertex_construct,
    opal_graph_vertex_destruct
);


static void opal_graph_edge_construct(opal_graph_edge_t *edge);
static void opal_graph_edge_destruct(opal_graph_edge_t *edge);

OBJ_CLASS_INSTANCE(
    opal_graph_edge_t,
    opal_list_item_t,
    opal_graph_edge_construct,
    opal_graph_edge_destruct
);

static void opal_graph_construct(opal_graph_t *graph);
static void opal_graph_destruct(opal_graph_t *graph);

OBJ_CLASS_INSTANCE(
    opal_graph_t,
    opal_object_t,
    opal_graph_construct,
    opal_graph_destruct
);

static void opal_adjacency_list_construct(opal_adjacency_list_t *aj_list);
static void opal_adjacency_list_destruct(opal_adjacency_list_t *aj_list);

OBJ_CLASS_INSTANCE(
    opal_adjacency_list_t,
    opal_list_item_t,
    opal_adjacency_list_construct,
    opal_adjacency_list_destruct
);



/*
 *
 *      opal_graph_vertex_t interface
 *
 */

static void opal_graph_vertex_construct(opal_graph_vertex_t *vertex)
{
    vertex->in_adj_list = NULL;
    vertex->in_graph = NULL;
    vertex->vertex_data = NULL;
    vertex->sibling = NULL;
    vertex->copy_vertex_data = NULL;
    vertex->free_vertex_data = NULL;
    vertex->alloc_vertex_data = NULL;
    vertex->compare_vertex = NULL;
    vertex->print_vertex = NULL;
}

static void opal_graph_vertex_destruct(opal_graph_vertex_t *vertex)
{
    vertex->in_adj_list = NULL;
    vertex->in_graph = NULL;
    vertex->sibling = NULL;
    vertex->copy_vertex_data = NULL;
    vertex->alloc_vertex_data = NULL;
    vertex->compare_vertex = NULL;
    if (NULL != vertex->free_vertex_data) {
        vertex->free_vertex_data(vertex->vertex_data);
    }
    vertex->vertex_data = NULL;
    vertex->print_vertex = NULL;
}


/*
 *
 *      opal_graph_edge_t interface
 *
 */

static void opal_graph_edge_construct(opal_graph_edge_t *edge)
{
    edge->end = NULL;
    edge->start = NULL;
    edge->weight = 0;
    edge->in_adj_list = NULL;
}


static void opal_graph_edge_destruct(opal_graph_edge_t *edge)
{
    edge->end = NULL;
    edge->start = NULL;
    edge->weight = 0;
    edge->in_adj_list = NULL;
}


/*
 *
 *     opal_graph_t  interface
 *
 */


static void opal_graph_construct(opal_graph_t *graph)
{
    graph->adjacency_list = OBJ_NEW(opal_list_t);
    graph->number_of_vertices = 0;
    graph->number_of_edges = 0;
}

static void opal_graph_destruct(opal_graph_t *graph)
{
    OPAL_LIST_RELEASE(graph->adjacency_list);
    graph->number_of_vertices = 0;
    graph->number_of_edges = 0;
}

/*
 *
 *     opal_adjacency_list  interface
 *
 */

static void opal_adjacency_list_construct(opal_adjacency_list_t *aj_list)
{
    aj_list->vertex = NULL;
    aj_list->edges = OBJ_NEW(opal_list_t);
}

static void opal_adjacency_list_destruct(opal_adjacency_list_t *aj_list)
{
   aj_list->vertex = NULL;
   OPAL_LIST_RELEASE(aj_list->edges);
}

/**
 * This function deletes all the edges that are connected *to* a
 * vertex.
 *
 * @param graph
 * @param vertex
 */
static void delete_all_edges_conceded_to_vertex(opal_graph_t *graph, opal_graph_vertex_t *vertex)
{
    opal_adjacency_list_t *aj_list;
    opal_graph_edge_t *edge, *next;

    /**
     * for all the adjacency list in the graph
     */
    OPAL_LIST_FOREACH(aj_list, graph->adjacency_list, opal_adjacency_list_t) {
        /**
         * for all the edges in the adjacency list
         */
        OPAL_LIST_FOREACH_SAFE(edge, next, aj_list->edges, opal_graph_edge_t) {
            /**
             * if the edge is ended in the vertex
             */
            if (edge->end == vertex) {
                /* Delete this edge  */
                opal_list_remove_item(edge->in_adj_list->edges, (opal_list_item_t*)edge);
                /* distract this edge */
                OBJ_RELEASE(edge);
            }
        }
    }
}

/**
 * This graph API adds a vertex to graph. The most common use
 * for this API is while building a graph.
 *
 * @param graph The graph that the vertex will be added to.
 * @param vertex The vertex we want to add.
 */
void opal_graph_add_vertex(opal_graph_t *graph, opal_graph_vertex_t *vertex)
{
   opal_adjacency_list_t *aj_list;

   /**
    * Find if this vertex already exists in the graph.
    */
   OPAL_LIST_FOREACH(aj_list, graph->adjacency_list, opal_adjacency_list_t) {
       if (aj_list->vertex == vertex) {
           /* If this vertex exists, dont do anything. */
           return;
       }
   }
   /* Construct a new adjacency list */
   aj_list = OBJ_NEW(opal_adjacency_list_t);
   aj_list->vertex = vertex;
   /* point the vertex to the adjacency list of the vertex (for easy searching) */
   vertex->in_adj_list = aj_list;
   /* Append the new creates adjacency list to the graph */
   opal_list_append(graph->adjacency_list, (opal_list_item_t*)aj_list);
   /* point the vertex to the graph it belongs to (mostly for debug uses)*/
   vertex->in_graph = graph;
   /* increase the number of vertices in the graph */
   graph->number_of_vertices++;
}


/**
 * This graph API adds an edge (connection between two
 * vertices) to a graph. The most common use
 * for this API is while building a graph.
 *
 * @param graph The graph that this edge will be added to.
 * @param edge The edge that we want to add.
 *
 * @return int Success or error. this API can return an error if
 *         one of the vertices is not in the graph.
 */
int opal_graph_add_edge(opal_graph_t *graph, opal_graph_edge_t *edge)
{
    opal_adjacency_list_t *aj_list, *start_aj_list= NULL;
    bool end_found = false;


    /**
     * find the vertices that this edge should connect.
     */
    OPAL_LIST_FOREACH(aj_list, graph->adjacency_list, opal_adjacency_list_t) {
        if (aj_list->vertex == edge->start) {
            start_aj_list = aj_list;
        }
        if (aj_list->vertex == edge->end) {
            end_found = true;
        }
    }
    /**
     * if one of the vertices either the start or the end is not
     * found - return an error.
     */
    if (NULL == start_aj_list || false == end_found) {
        return OPAL_ERROR;
    }
    /* point the edge to the adjacency list of the start vertex (for easy search) */
    edge->in_adj_list=start_aj_list;
    /* append the edge to the adjacency list of the start vertex */
    opal_list_append(start_aj_list->edges, (opal_list_item_t*)edge);
    /* increase the graph size */
    graph->number_of_edges++;
    return OPAL_SUCCESS;
}


/**
 * This graph API removes an edge (a connection between two
 * vertices) from the graph. The most common use of this API is
 * while destructing a graph or while removing vertices from a
 * graph. while removing vertices from a graph, we should also
 * remove the connections from and to the vertices that we are
 * removing.
 *
 * @param graph The graph that this edge will be remove from.
 * @param edge the edge that we want to remove.
 */
void opal_graph_remove_edge (opal_graph_t *graph, opal_graph_edge_t *edge)
{
    /* remove the edge from the list it belongs to */
    opal_list_remove_item(edge->in_adj_list->edges, (opal_list_item_t*)edge);
    /* decrees the number of edges in the graph */
    graph->number_of_edges--;
    /* Note that the edge is not destructed - the caller should destruct this edge. */
}

/**
 * This graph API remove a vertex from graph. The most common
 * use for this API is while distracting a graph or while
 * removing relevant vertices from a graph.
 *
 * @param graph The graph that the vertex will be remove from.
 * @param vertex The vertex we want to remove.
 */

void opal_graph_remove_vertex(opal_graph_t *graph, opal_graph_vertex_t *vertex)
{
    opal_adjacency_list_t *adj_list;

    /* do not need to remove all the edges of this vertex and destruct them as
     * they will be released in the destructor for adj_list */
    adj_list = vertex->in_adj_list;
    /**
     * remove the adjscency list of this vertex from the graph and
     * destruct it.
     */
    opal_list_remove_item(graph->adjacency_list, (opal_list_item_t*)adj_list);
    OBJ_RELEASE(adj_list);
    /**
     * delete all the edges that connected *to* the vertex.
     */
    delete_all_edges_conceded_to_vertex(graph, vertex);
    /* destruct the vertex */
    OBJ_RELEASE(vertex);
    /* decrease the number of vertices in the graph. */
    graph->number_of_vertices--;
}

/**
 * This graph API tell us if two vertices are adjacent
 *
 * @param graph The graph that the vertices belongs to.
 * @param vertex1 first vertex.
 * @param vertex2 second vertex.
 *
 * @return uint32_t the weight of the connection between the two
 *         vertices or infinity if the vertices are not
 *         connected.
 */
uint32_t opal_graph_adjacent(opal_graph_t *graph, opal_graph_vertex_t *vertex1, opal_graph_vertex_t *vertex2)
{
    opal_adjacency_list_t *adj_list;
    opal_graph_edge_t *edge;

    /**
     * Verify that the first vertex belongs to the graph.
     */
    if (graph != vertex1->in_graph) {
        OPAL_OUTPUT((0,"opal_graph_adjacent 1 Vertex1 %p not in the graph %p\n",(void *)vertex1,(void *)graph));
        return DISTANCE_INFINITY;
    }
    /**
     * Verify that the second vertex belongs to the graph.
     */
    if (graph != vertex2->in_graph) {
        OPAL_OUTPUT((0,"opal_graph_adjacent 2 Vertex2 %p not in the graph %p\n",(void *)vertex2,(void *)graph));
        return DISTANCE_INFINITY;
    }
    /**
     * If the first vertex and the second vertex are the same
     * vertex, the distance between the is 0.
     */
    if (vertex1 == vertex2) {
        return 0;
    }
    /**
     * find the second vertex in the adjacency list of the first
     * vertex.
     */
    adj_list = (opal_adjacency_list_t *) vertex1->in_adj_list;
    OPAL_LIST_FOREACH(edge, adj_list->edges, opal_graph_edge_t) {
        if (edge->end == vertex2) {
            /* if the second vertex was found in the adjacency list of the first one, return the weight */
            return edge->weight;
        }
    }
    /* if the second vertex was not found in the adjacency list of the first one, return infinity */
    return DISTANCE_INFINITY;
}

/**
 * This Graph API returns the order of the graph (number of
 * vertices)
 *
 * @param graph
 *
 * @return int
 */
int opal_graph_get_order(opal_graph_t *graph)
{
    return graph->number_of_vertices;
}

/**
 * This Graph API returns the size of the graph (number of
 * edges)
 *
 * @param graph
 *
 * @return int
 */
int opal_graph_get_size(opal_graph_t *graph)
{
    return graph->number_of_edges;
}

/**
 * This graph API finds a vertex in the graph according the
 * vertex data.
 * @param graph the graph we searching in.
 * @param vertex_data the vertex data we are searching according
 *                    to.
 *
 * @return opal_graph_vertex_t* The vertex founded or NULL.
 */
opal_graph_vertex_t *opal_graph_find_vertex(opal_graph_t *graph, void *vertex_data)
{
    opal_adjacency_list_t *aj_list;

    /**
     * Run on all the vertices of the graph
     */
    OPAL_LIST_FOREACH(aj_list, graph->adjacency_list, opal_adjacency_list_t) {
        if (NULL != aj_list->vertex->compare_vertex) {
            /* if the vertex data of a vertex is equal to the vertex data */
            if (0 == aj_list->vertex->compare_vertex(aj_list->vertex->vertex_data, vertex_data)) {
                /* return the found vertex */
                return aj_list->vertex;
            }
        }
    }
    /* if a vertex is not found, return NULL */
    return NULL;
}


/**
 * This graph API returns an array of pointers of all the
 * vertices in the graph.
 *
 *
 * @param graph
 * @param vertices_list an array of pointers of all the
 *         vertices in the graph vertices.
 *
 * @return int returning the graph order (the
 *                    number of vertices in the returned array)
 */
int opal_graph_get_graph_vertices(opal_graph_t *graph, opal_pointer_array_t *vertices_list)
{
    opal_adjacency_list_t *aj_list;

    /**
     * If the graph order is 0, return NULL.
     */
    if (0 == graph->number_of_vertices) {
        return 0;
    }
    /* Run on all the vertices of the graph */
    OPAL_LIST_FOREACH(aj_list, graph->adjacency_list, opal_adjacency_list_t) {
        /* Add the vertex to the vertices array */
        opal_pointer_array_add(vertices_list,(void *)aj_list->vertex);
    }
    /* return the vertices list */
    return graph->number_of_vertices;
}

/**
 * This graph API returns all the adjacents of a vertex and the
 * distance (weight) of those adjacents and the vertex.
 *
 * @param graph
 * @param vertex The reference vertex
 * @param adjacents An allocated pointer array of vertices and
 *                  their distance from the reference vertex.
 *                  Note that this pointer should be free after
 *                  usage by the user
 *
 * @return int the number of adjacents in the list.
 */
int opal_graph_get_adjacent_vertices(opal_graph_t *graph, opal_graph_vertex_t *vertex, opal_value_array_t *adjacents)
{
    opal_adjacency_list_t *adj_list;
    opal_graph_edge_t *edge;
    int adjacents_number;
    vertex_distance_from_t distance_from;

    /**
     * Verify that the vertex belongs to the graph.
     */
    if (graph != vertex->in_graph) {
        OPAL_OUTPUT((0,"Vertex %p not in the graph %p\n", (void *)vertex, (void *)graph));
        return 0;
    }
    /**
     * find the adjacency list that this vertex belongs to
     */
    adj_list = (opal_adjacency_list_t *) vertex->in_adj_list;
    /* find the number of adjcents of this vertex */
    adjacents_number = opal_list_get_size(adj_list->edges);
    /* Run on all the edges from this vertex */
    OPAL_LIST_FOREACH(edge, adj_list->edges, opal_graph_edge_t) {
        /* assign vertices and their weight in the adjcents list */
        distance_from.vertex = edge->end;
        distance_from.weight = edge->weight;
        opal_value_array_append_item(adjacents, &distance_from);
    }
    /* return the number of the adjacents in the list */
    return adjacents_number;
}

/**
 * This graph API finds the shortest path between two vertices.
 *
 * @param graph
 * @param vertex1 The start vertex.
 * @param vertex2 The end vertex.
 *
 * @return uint32_t the distance between the two vertices.
 */

uint32_t opal_graph_spf(opal_graph_t *graph, opal_graph_vertex_t *vertex1, opal_graph_vertex_t *vertex2)
{
    opal_value_array_t *distance_array;
    uint32_t items_in_distance_array, spf = DISTANCE_INFINITY;
    vertex_distance_from_t *vertex_distance;
    uint32_t i;

    /**
     * Verify that the first vertex belongs to the graph.
     */
    if (graph != vertex1->in_graph) {
        OPAL_OUTPUT((0,"opal_graph_spf 1 Vertex1 %p not in the graph %p\n",(void *)vertex1,(void *)graph));
        return DISTANCE_INFINITY;
    }
    /**
     * Verify that the second vertex belongs to the graph.
     */
    if (graph != vertex2->in_graph) {
        OPAL_OUTPUT((0,"opal_graph_spf 2 Vertex2 %p not in the graph %p\n",(void *)vertex2,(void *)graph));
        return DISTANCE_INFINITY;
    }
    /**
     * Run Dijkstra algorithm on the graph from the start vertex.
     */
    distance_array = OBJ_NEW(opal_value_array_t);
    opal_value_array_init(distance_array, sizeof(vertex_distance_from_t));
    opal_value_array_reserve(distance_array,50);
    items_in_distance_array = opal_graph_dijkstra(graph, vertex1, distance_array);
    /**
     * find the end vertex in the distance array that Dijkstra
     * algorithm returned.
     */
    for (i = 0; i < items_in_distance_array; i++) {
        vertex_distance = opal_value_array_get_item(distance_array, i);
        if (vertex_distance->vertex == vertex2) {
            spf = vertex_distance->weight;
            break;
        }
    }
    OBJ_RELEASE(distance_array);
    /* return the distance (weight) to the end vertex */
    return spf;
}

/**
 * Compare the distance between two vertex distance items. this
 * function is used for sorting an array of vertices distance by
 * qsort function.
 *
 * @param item1 a void pointer to vertex distance structure
 * @param item2 a void pointer to vertex distance structure
 *
 * @return int 1 - the first item weight is higher then the
 *         second item weight. 0 - the weights are equal. -1 -
 *         the second item weight is higher the the first item
 *         weight.
 */
static int compare_vertex_distance(const void *item1, const void *item2)
{
    vertex_distance_from_t *vertex_dist1, *vertex_dist2;

    /* convert the void pointers to vertex distance pointers. */
    vertex_dist1 = (vertex_distance_from_t *)item1;
    vertex_dist2 = (vertex_distance_from_t *)item2;

    /* If the first item weight is higher then the second item weight return 1*/
    if (vertex_dist1->weight > vertex_dist2->weight) {
        return 1;
    }
    /* If they are equal return 0 */
    else if (vertex_dist1->weight == vertex_dist2->weight) {
        return 0;
    }
    /* if you reached here then the second item weight is higher the the first item weight */
    return -1;
}


/**
 * This graph API returns the distance (weight) from a reference
 * vertex to all other vertices in the graph using the Dijkstra
 * algorithm
 *
 * @param graph
 * @param vertex The reference vertex.
 * @param distance_array An array of vertices and
 *         their distance from the reference vertex.
 *
 * @return uint32_t the size of the distance array
 */
uint32_t opal_graph_dijkstra(opal_graph_t *graph, opal_graph_vertex_t *vertex, opal_value_array_t *distance_array)
{
    int graph_order;
    vertex_distance_from_t *Q, *q_start, *current_vertex;
    opal_adjacency_list_t *adj_list;
    int number_of_items_in_q;
    int i;
    uint32_t weight;


    /**
     * Verify that the reference vertex belongs to the graph.
     */
    if (graph != vertex->in_graph) {
        OPAL_OUTPUT((0,"opal:graph:dijkstra: vertex %p not in the graph %p\n",(void *)vertex,(void *)graph));
        return 0;
    }
    /* get the order of the graph and allocate a working queue accordingly */
    graph_order = opal_graph_get_order(graph);
    Q = (vertex_distance_from_t *)malloc(graph_order * sizeof(vertex_distance_from_t));
    /* assign a pointer to the start of the queue */
    q_start = Q;
    /* run on all the vertices of the graph */
    i = 0;
    OPAL_LIST_FOREACH(adj_list, graph->adjacency_list, opal_adjacency_list_t) {
        /* insert the vertices pointes to the working queue */
        Q[i].vertex = adj_list->vertex;
        /**
         * assign an infinity distance to all the vertices in the queue
         * except the reference vertex which its distance should be 0.
         */
        Q[i++].weight = (adj_list->vertex == vertex) ? 0 : DISTANCE_INFINITY;
    }
    number_of_items_in_q = i;
    /* sort the working queue according the distance from the reference vertex */
    qsort(q_start, number_of_items_in_q, sizeof(vertex_distance_from_t), compare_vertex_distance);
    /* while the working queue is not empty */
    while (number_of_items_in_q > 0) {
        /* start to work with the first vertex in the working queue */
        current_vertex = q_start;
        /* remove the first vertex from the queue */
        q_start++;
        /* decrees the number of vertices in the queue */
        number_of_items_in_q--;
        /* find the distance of all other vertices in the queue from the first vertex in the queue */
        for (i = 0; i < number_of_items_in_q; i++) {
            weight = opal_graph_adjacent(graph, current_vertex->vertex, q_start[i].vertex);
            /**
             * if the distance from the first vertex in the queue to the I
             * vertex in the queue plus the distance of the first vertex in
             * the queue from the referenced vertex is smaller than the
             * distance of the I vertex from the referenced vertex, assign
             * the lower distance to the I vertex.
             */
            if (current_vertex->weight + weight < q_start[i].weight) {
                q_start[i].weight = weight + current_vertex->weight;
            }
        }
        /* sort again the working queue */
        qsort(q_start, number_of_items_in_q, sizeof(vertex_distance_from_t), compare_vertex_distance);
    }
    /* copy the working queue the the returned distance array */
    for (i = 0; i < graph_order-1; i++) {
        opal_value_array_append_item(distance_array, (void *)&(Q[i+1]));
    }
    /* free the working queue */
    free(Q);
    /* assign the distance array size. */
    return graph_order - 1;
}


/**
 * This graph API duplicates a graph. Note that this API does
 * not copy the graph but builds a new graph while coping just
 * the vertex data.
 *
 * @param dest The new created graph.
 * @param src The graph we want to duplicate.
 */
void opal_graph_duplicate(opal_graph_t **dest, opal_graph_t *src)
{
    opal_adjacency_list_t *aj_list;
    opal_graph_vertex_t *vertex;
    opal_graph_edge_t *edge, *new_edge;

    /* construct a new graph */
    *dest = OBJ_NEW(opal_graph_t);
    /* Run on all the vertices of the src graph */
    OPAL_LIST_FOREACH(aj_list, src->adjacency_list, opal_adjacency_list_t) {
        /* for each vertex in the src graph, construct a new vertex */
        vertex = OBJ_NEW(opal_graph_vertex_t);
        /* associate the new vertex to a vertex from the original graph */
        vertex->sibling = aj_list->vertex;
        /* associate the original vertex to the new constructed vertex */
        aj_list->vertex->sibling = vertex;
        /* allocate space to vertex data in the new vertex */
        if (NULL != aj_list->vertex->alloc_vertex_data) {
            vertex->vertex_data = aj_list->vertex->alloc_vertex_data();
            vertex->alloc_vertex_data = aj_list->vertex->alloc_vertex_data;
        }
        /* copy the vertex data from the original vertex  to the new vertex */
        if (NULL != aj_list->vertex->copy_vertex_data) {
            aj_list->vertex->copy_vertex_data(&(vertex->vertex_data), aj_list->vertex->vertex_data);
            vertex->copy_vertex_data = aj_list->vertex->copy_vertex_data;
        }
        /* copy all the fields of the original vertex to the new vertex. */
        vertex->free_vertex_data = aj_list->vertex->free_vertex_data;
        vertex->print_vertex = aj_list->vertex->print_vertex;
        vertex->compare_vertex = aj_list->vertex->compare_vertex;
        vertex->in_graph = *dest;
        /* add the new vertex to the new graph */
        opal_graph_add_vertex(*dest, vertex);
    }
    /**
     * Now, copy all the edges from the source graph
     */
    /* Run on all the adjscency lists in the graph */
    OPAL_LIST_FOREACH(aj_list, src->adjacency_list, opal_adjacency_list_t) {
        /* for all the edges in the adjscency list */
        OPAL_LIST_FOREACH(edge, aj_list->edges, opal_graph_edge_t) {
            /* construct new edge for the new graph */
            new_edge = OBJ_NEW(opal_graph_edge_t);
            /* copy the edge weight from the original edge */
            new_edge->weight = edge->weight;
            /* connect the new edge according to start and end associations to the vertices of the src graph */
            new_edge->start = edge->start->sibling;
            new_edge->end = edge->end->sibling;
            /* add the new edge to the new graph */
            opal_graph_add_edge(*dest, new_edge);
        }
    }
}

/**
 * This graph API prints a graph - mostly for debug uses.
 * @param graph
 */
void opal_graph_print(opal_graph_t *graph)
{
    opal_adjacency_list_t *aj_list;
    opal_graph_edge_t *edge;
    char *tmp_str1, *tmp_str2;
    bool need_free1, need_free2;

    /* print header */
    opal_output(0, "      Graph         ");
    opal_output(0, "====================");
    /* run on all the vertices of the graph */
    OPAL_LIST_FOREACH(aj_list, graph->adjacency_list, opal_adjacency_list_t) {
        /* print vertex data to temporary string*/
        if (NULL != aj_list->vertex->print_vertex) {
            need_free1 = true;
            tmp_str1 = aj_list->vertex->print_vertex(aj_list->vertex->vertex_data);
        }
        else {
            need_free1 = false;
            tmp_str1 = "";
        }
        /* print vertex */
        opal_output(0, "V(%s) Connections:",tmp_str1);
        /* run on all the edges of the vertex */
        OPAL_LIST_FOREACH(edge, aj_list->edges, opal_graph_edge_t) {
            /* print the vertex data of the vertex in the end of the edge to a temporary string */
            if (NULL != edge->end->print_vertex) {
                need_free2 = true;
                tmp_str2 = edge->end->print_vertex(edge->end->vertex_data);
            }
            else {
                need_free2 = false;
                tmp_str2 = "";
            }
            /* print the edge */
            opal_output(0, "    E(%s -> %d -> %s)",tmp_str1, edge->weight, tmp_str2);
            if (need_free2) {
                free(tmp_str2);
            }
        }
        if (need_free1) {
            free(tmp_str1);
        }
    }
}

