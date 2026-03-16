/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015-2018 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

/** @file
 *
 *     A thread-safe interval tree derived from opal_rb_tree_t
 */

#ifndef OPAL_INTERVAL_TREE_H
#define OPAL_INTERVAL_TREE_H

#include "opal_config.h"
#include <stdlib.h>
#include "opal/constants.h"
#include "opal/class/opal_object.h"
#include "opal/class/opal_free_list.h"

BEGIN_C_DECLS
/*
 * Data structures and datatypes
 */

/**
  * red and black enum
  */
typedef enum {OPAL_INTERVAL_TREE_COLOR_RED, OPAL_INTERVAL_TREE_COLOR_BLACK} opal_interval_tree_nodecolor_t;

/**
  * node data structure
  */
struct opal_interval_tree_node_t
{
    opal_free_list_item_t super;        /**< the parent class */
    opal_interval_tree_nodecolor_t color;     /**< the node color */
    struct opal_interval_tree_node_t *parent;/**< the parent node, can be NULL */
    struct opal_interval_tree_node_t *left;  /**< the left child - can be nill */
    struct opal_interval_tree_node_t *right; /**< the right child - can be nill */
    /** edit epoch associated with this node */
    uint32_t epoch;
    /** data for this interval */
    void *data;
    /** low value of this interval */
    uint64_t low;
    /** high value of this interval */
    uint64_t high;
    /** maximum value of all intervals in tree rooted at this node */
    uint64_t max;
};
typedef struct opal_interval_tree_node_t opal_interval_tree_node_t;

/** maximum number of simultaneous readers */
#define OPAL_INTERVAL_TREE_MAX_READERS 128

/**
  * the data structure that holds all the needed information about the tree.
  */
struct opal_interval_tree_t {
    opal_object_t super;           /**< the parent class */
    /* this root pointer doesn't actually point to the root of the tree.
     * rather, it points to a sentinal node who's left branch is the real
     * root of the tree. This is done to eliminate special cases */
    opal_interval_tree_node_t root; /**< a pointer to the root of the tree */
    opal_interval_tree_node_t nill;     /**< the nill sentinal node */
    opal_free_list_t free_list;   /**< the free list to get the memory from */
    opal_list_t gc_list;          /**< list of nodes that need to be released */
    uint32_t epoch;               /**< current update epoch */
    volatile size_t tree_size;    /**< the current size of the tree */
    volatile int32_t lock;        /**< update lock */
    volatile int32_t reader_count;    /**< current highest reader slot to check */
    volatile uint32_t reader_id;  /**< next reader slot to check */
    volatile uint32_t reader_epochs[OPAL_INTERVAL_TREE_MAX_READERS];
};
typedef struct opal_interval_tree_t opal_interval_tree_t;

/** declare the tree node as a class */
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_interval_tree_node_t);
/** declare the tree as a class */
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_interval_tree_t);

/* Function pointers for map traversal function */
/**
  * this function is used for the opal_interval_tree_traverse function.
  * it is passed a pointer to the value for each node and, if it returns
  * a one, the action function is called on that node. Otherwise, the node is ignored.
  */
typedef int (*opal_interval_tree_condition_fn_t)(void *);
/**
 * this function is used for the user to perform any action on the passed
 * values. The first argument is the key and the second is the value.
 * note that this function SHOULD NOT modify the keys, as that would
 * mess up the tree.
 */
typedef int (*opal_interval_tree_action_fn_t)(uint64_t low, uint64_t high, void *data, void *ctx);

/*
 * Public function protoypes
 */

/**
  * the function creates a new tree
  *
  * @param tree a pointer to an allocated area of memory for the main
  *  tree data structure.
  * @param comp a pointer to the function to use for comaparing 2 nodes
  *
  * @retval OPAL_SUCCESS if it is successful
  * @retval OPAL_ERR_TEMP_OUT_OF_RESOURCE if unsuccessful
  */
OPAL_DECLSPEC int opal_interval_tree_init(opal_interval_tree_t * tree);


/**
  * inserts a node into the tree
  *
  * @param tree a pointer to the tree data structure
  * @param key the key for the node
  * @param value the value for the node
  *
  * @retval OPAL_SUCCESS
  * @retval OPAL_ERR_TEMP_OUT_OF_RESOURCE if unsuccessful
  */
OPAL_DECLSPEC int opal_interval_tree_insert(opal_interval_tree_t *tree, void *value, uint64_t low, uint64_t high);

/**
  * finds a value in the tree based on the passed key using passed
  * compare function
  *
  * @param tree a pointer to the tree data structure
  * @param key a pointer to the key
  * @param compare function
  *
  * @retval pointer to the value if found
  * @retval NULL if not found
  */
OPAL_DECLSPEC void *opal_interval_tree_find_overlapping (opal_interval_tree_t *tree, uint64_t low, uint64_t high);

/**
  * deletes a node based on its interval
  *
  * @param tree a pointer to the tree data structure
  * @param low low value of interval
  * @param high high value of interval
  * @param data data to match (NULL for any)
  *
  * @retval OPAL_SUCCESS if the node is found and deleted
  * @retval OPAL_ERR_NOT_FOUND if the node is not found
  *
  * This function finds and deletes an interval from the tree that exactly matches
  * the given range.
  */
OPAL_DECLSPEC int opal_interval_tree_delete(opal_interval_tree_t *tree, uint64_t low, uint64_t high, void *data);

/**
  * frees all the nodes on the tree
  *
  * @param tree a pointer to the tree data structure
  *
  * @retval OPAL_SUCCESS
  */
OPAL_DECLSPEC int opal_interval_tree_destroy(opal_interval_tree_t *tree);

/**
  * traverses the entire tree, performing the cond function on each of the
  * values and if it returns one it performs the action function on the values
  *
  * @param tree a pointer to the tree
  * @param low low value of interval
  * @param high high value of interval
  * @param partial_ok traverse nodes that parially overlap the given range
  * @param action a pointer to the action function
  * @param ctx context to pass to action function
  *
  * @retval OPAL_SUCCESS
  * @retval OPAL_ERROR if there is an error
  */
OPAL_DECLSPEC int opal_interval_tree_traverse (opal_interval_tree_t *tree, uint64_t low, uint64_t high,
                                               bool complete, opal_interval_tree_action_fn_t action, void *ctx);

/**
  * returns the size of the tree
  *
  * @param tree a pointer to the tree data structure
  *
  * @retval int the nuber of items on the tree
  */
OPAL_DECLSPEC size_t opal_interval_tree_size (opal_interval_tree_t *tree);

/**
 * Diagnostic function to get the max depth of an interval tree.
 *
 * @param[in] tree    opal interval tree pointer
 *
 * This is an expensive function that walks the entire tree to find the
 * maximum depth. For a valid interval tree this depth will always be
 * O(log(n)) where n is the number of intervals in the tree.
 */
OPAL_DECLSPEC size_t opal_interval_tree_depth (opal_interval_tree_t *tree);

/**
 * Diagnostic function that can be used to verify that an interval tree
 * is valid.
 *
 * @param[in] tree    opal interval tree pointer
 *
 * @returns true if the tree is a valid interval tree
 * @returns false otherwise
 */
OPAL_DECLSPEC bool opal_interval_tree_verify (opal_interval_tree_t *tree);

/**
 * Dump a DOT representation of the interval tree
 *
 * @param[in] tree    opal interval tree pointer
 * @param[in] path    output file path
 *
 * This function dumps the tree and includes: color, data value, interval, and sub-tree
 * min and max.
 */
OPAL_DECLSPEC int opal_interval_tree_dump (opal_interval_tree_t *tree, const char *path);

END_C_DECLS
#endif /* OPAL_INTERVAL_TREE_H */
