/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Voltaire All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 * The opal_graph interface is used to provide a generic graph infrastructure
 * to Open-MPI. The graph is represented as an adjacentcy list.
 * The graph is a list of vertices. The graph is a weighted directional graph.
 * Each vertex contains a pointer to a vertex data.
 * This pointer can point to the structure that this vertex belongs to.
 */
#ifndef OPAL_GRAPH_H
#define OPAL_GRAPH_H

#include "opal_config.h"
#include <stdio.h>
#include <stdlib.h>
#include "opal/class/opal_object.h"
#include "opal/class/opal_list.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/class/opal_value_array.h"

BEGIN_C_DECLS

    /* When two vertices are not connected, the distance between them is infinite. */
#define DISTANCE_INFINITY 0x7fffffff

    /* A class for vertex */
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_graph_vertex_t);

    /* A class for an edge (a connection between two verices) */
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_graph_edge_t);

    /* A class for an adjacency list  */
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_adjacency_list_t);

    /* A class for graph */
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_graph_t);

  /**
   * Function pointer for coping a vertex data from one vertex to
   * another
   *
   * @param dst The destination pointer of vertex_data
   * @param src The source pointer of the vertex_data
   *
   *
   */
typedef void (*opal_graph_copy_vertex_data)(void **dst, void *src);

/**
 * free vertex data.
 * @param vertex_data
 *
 * The vertex data can point to the structure that this vertex
 * belongs to.
 */
typedef void (*opal_graph_free_vertex_data)(void *vertex_data);

/**
 * Allocate vertex data.
 */
typedef void *(*opal_graph_alloc_vertex_data)(void);

/**
 * Compare two vertices data.
 *
 *@param vertex_data1
 *@param vertex_data2
 *
 *@return int The comparition results. 1- vertex_data1 is bigger
 *        then vertex_data2, 0- vertex_data1 is equal to
 *        vertex_data2 and -1- vertex_data1 is smaller the
 *        vertex_data2.
 */
typedef int  (*opal_graph_compare_vertex_data)(void *vertex_data1, void *vertex_data2);

/**
 * print a vertex data.
 *
 * @param vertex_data
 */
typedef char *(*opal_graph_print_vertex)(void *vertex_data);


/**
 * A vertex class.
 */
struct opal_graph_vertex_t {
    opal_list_item_t             super; /* A pointer to a vertex parent */
    void                         *in_graph; /* A pointer to the graph that this vertex belongs to */
    void                         *in_adj_list; /* A pointer to the adjacency that this vertex belongs to */
    void                         *vertex_data;/* A pointer to some data. this pointer can point to the struct the this*/
                                              /* vertex belongs to*/
    struct opal_graph_vertex_t   *sibling;/* A pointer to a sibling vertex. */
                                          /* if this vertex was copied this pointer will point to the source vertex */
                                          /* This pointer is for internal uses. */
    opal_graph_copy_vertex_data  copy_vertex_data; /* A function to copy vertex data */
    opal_graph_free_vertex_data  free_vertex_data; /* A function to print vertex data */
    opal_graph_alloc_vertex_data alloc_vertex_data;/* A function to allocate vertex data */
    opal_graph_compare_vertex_data compare_vertex; /* A function to compare between two vertices data */
    opal_graph_print_vertex        print_vertex;   /* A function to print vertex data */
};

/**
 * A type for vertex.
 */
typedef struct opal_graph_vertex_t opal_graph_vertex_t;

/**
 * An opal_adjacency_list_t class
 */
struct opal_adjacency_list_t {
    opal_list_item_t     super;   /* A pointer to vertex parent */
    opal_graph_vertex_t *vertex;  /* The adjacency_list is for adjacent of this vertex */
    opal_list_t         *edges;   /* An edge list for all the adjacent and their weights */
};

/**
 * A type for opal_adjacency_list_t
 */
typedef struct opal_adjacency_list_t opal_adjacency_list_t;

/**
 * An edge class. (connection between two vertices.) Since the
 * graph is a directional graph, each vertex contains a start
 * and an end pointers for the start vertex and the end vertex
 * of this edge. Since the graph is a weighted graph, the edges
 * contains a weight field.
 */
struct opal_graph_edge_t {
    opal_list_item_t         super;  /* A pointer to the edge parent */
    opal_graph_vertex_t      *start; /* The start vertex. */
    opal_graph_vertex_t      *end;   /* The end vertex */
    uint32_t                 weight; /* The weight of this edge */
    opal_adjacency_list_t    *in_adj_list; /* The adjacency list in witch this edge in.*/
                                           /* This adjacency list contains the start vertex of this edge*/
                                           /* and its for internal uses */
};

/**
 * A type for an edge
 */
typedef struct opal_graph_edge_t opal_graph_edge_t;


/**
 * A graph class.
 */
struct opal_graph_t {
    opal_object_t       super;
    opal_list_t         *adjacency_list;
    int                 number_of_edges;
    int                 number_of_vertices;
};

/**
 * A type for graph class
 */
typedef struct opal_graph_t opal_graph_t;

/**
 * This structure represent the distance (weight) of a vertex
 * from some point in the graph.
 */
struct vertex_distance_from_t {
    opal_graph_vertex_t *vertex;
    uint32_t            weight;
};

/**
 * A type for vertex distance.
 */
typedef struct vertex_distance_from_t vertex_distance_from_t;

/**
 * This graph API adds a vertex to graph. The most common use
 * for this API is while building a graph.
 *
 * @param graph The graph that the vertex will be added to.
 * @param vertex The vertex we want to add.
 */
OPAL_DECLSPEC void opal_graph_add_vertex(opal_graph_t *graph, opal_graph_vertex_t *vertex);

/**
 * This graph API remove a vertex from graph. The most common
 * use for this API is while distracting a graph or while
 * removing relevant vertices from a graph.
 *
 * @param graph The graph that the vertex will be remove from.
 * @param vertex The vertex we want to remove.
 */
OPAL_DECLSPEC void opal_graph_remove_vertex(opal_graph_t *graph, opal_graph_vertex_t *vertex);

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
OPAL_DECLSPEC int opal_graph_add_edge(opal_graph_t *graph, opal_graph_edge_t *edge);

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
OPAL_DECLSPEC void opal_graph_remove_edge (opal_graph_t *graph, opal_graph_edge_t *edge);

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
OPAL_DECLSPEC uint32_t opal_graph_adjacent(opal_graph_t *graph, opal_graph_vertex_t *vertex1, opal_graph_vertex_t *vertex2);

/**
 * This Graph API returns the order of the graph (number of
 * vertices)
 *
 * @param graph
 *
 * @return int
 */
OPAL_DECLSPEC int opal_graph_get_order(opal_graph_t *graph);

/**
 * This Graph API returns the size of the graph (number of
 * edges)
 *
 * @param graph
 *
 * @return int
 */
OPAL_DECLSPEC int opal_graph_get_size(opal_graph_t *graph);

/**
 * This graph API finds a vertex in the graph according the
 * vertex data.
 * @param graph the graph we searching in.
 * @param vertex_data the vertex data we are searching according
 *                    to.
 *
 * @return opal_graph_vertex_t* The vertex founded or NULL.
 */
OPAL_DECLSPEC opal_graph_vertex_t *opal_graph_find_vertex(opal_graph_t *graph, void *vertex_data);


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
OPAL_DECLSPEC int opal_graph_get_graph_vertices(opal_graph_t *graph, opal_pointer_array_t *vertices_list);

/**
 * This graph API returns all the adjacent of a vertex and the
 * distance (weight) of those adjacent and the vertex.
 *
 * @param graph
 * @param vertex The reference vertex
 * @param adjacent An allocated pointer array of vertices and
 *                  their distance from the reference vertex.
 *                  Note that this pointer should be free after
 *                  usage by the user
 *
 * @return int the number of adjacent in the list.
 */
OPAL_DECLSPEC int opal_graph_get_adjacent_vertices(opal_graph_t *graph, opal_graph_vertex_t *vertex, opal_value_array_t *adjacent);

/**
 * This graph API duplicates a graph. Note that this API does
 * not copy the graph but builds a new graph while coping just
 * the vertices data.
 *
 * @param dest The new created graph.
 * @param src The graph we want to duplicate.
 */
OPAL_DECLSPEC void opal_graph_duplicate(opal_graph_t **dest, opal_graph_t *src);

/**
 * This graph API finds the shortest path between two vertices.
 *
 * @param graph
 * @param vertex1 The start vertex.
 * @param vertex2 The end vertex.
 *
 * @return uint32_t the distance between the two vertices.
 */
OPAL_DECLSPEC uint32_t opal_graph_spf(opal_graph_t *graph, opal_graph_vertex_t *vertex1, opal_graph_vertex_t *vertex2);

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
OPAL_DECLSPEC uint32_t opal_graph_dijkstra(opal_graph_t *graph, opal_graph_vertex_t *vertex, opal_value_array_t *distance_array);

/**
 * This graph API prints a graph - mostly for debug uses.
 * @param graph
 */
OPAL_DECLSPEC void opal_graph_print(opal_graph_t *graph);

END_C_DECLS


#endif /* OPAL_GRAPH_H */

