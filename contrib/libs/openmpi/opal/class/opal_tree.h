/*
 * Copyright (c) 2011      Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2011      Oak Ridge National Labs.  All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * The opal_tree_t interface is used to provide a generic
 * tree list container for Open MPI.  It was inspired by the opal_list_t
 * interface but instead of organizing items in a doubly-linked list
 * fashion, we order them in a finite tree structure.
 *
 * The general idea is a user creates an class instance that has two
 * components.  A tree structure component as defined by opal_tree_item_t
 * that links all the items together to form the tree.  Then there is
 * a user specific data component which the user defines what is stored at
 * each item.  When a user create a type to be used for a OBJ_CLASS_INSTANCE
 * it will contain the opal_tree_item_t followed by any user specific
 * data.  Then the opal_tree_item_t objects can be put in an
 * opal_tree_t.  Hence, you create a new type that derives from
 * opal_tree_item_t; this new type can then be used with opal_tree_t
 * containers.
 *
 * NOTE: opal_tree_item_t instances can only be on \em one tree at a
 * time.  Specifically, if you add an opal_tree_item_t to one tree,
 * and then add it to another tree (without first removing it from the
 * first tree), you will effectively be hosing the first tree.  You
 * have been warned.
 *
 * If OPAL_ENABLE_DEBUG is true, a bunch of checks occur, including
 * some spot checks for a debugging reference count in an attempt to
 * ensure that an opal_tree_item_t is only one *one* tree at a time.
 * Given the highly concurrent nature of this class, these spot checks
 * cannot guarantee that an item is only one tree at a time.
 * Specifically, since it is a desirable attribute of this class to
 * not use locks for normal operations, it is possible that two
 * threads may [erroneously] modify an opal_tree_item_t concurrently.
 *
 * The only way to guarantee that a debugging reference count is valid
 * for the duration of an operation is to lock the item_t during the
 * operation.  But this fundamentally changes the desirable attribute
 * of this class (i.e., no locks).  So all we can do is spot-check the
 * reference count in a bunch of places and check that it is still the
 * value that we think it should be.  But this doesn't mean that you
 * can run into "unlucky" cases where two threads are concurrently
 * modifying an item_t, but all the spot checks still return the
 * "right" values.  All we can do is hope that we have enough spot
 * checks to statistically drive down the possibility of the unlucky
 * cases happening.
 */

#ifndef OPAL_TREE_H
#define OPAL_TREE_H

#include "opal_config.h"
#include <stdio.h>
#include <stdlib.h>
#include "opal/class/opal_object.h"
#include "opal/dss/dss.h"

#if OPAL_ENABLE_DEBUG
/* Need atomics for debugging (reference counting) */
#include "opal/sys/atomic.h"
#include "opal/threads/mutex.h"
#endif

BEGIN_C_DECLS

/**
 * \internal
 *
 * The class for the tree container.
 */
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_tree_t);
/**
 * \internal
 *
 * Base class for items that are put in tree containers.
 */
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_tree_item_t);

/**
 * \internal
 *
 * Struct of an opal_tree_item_t
 */
typedef struct opal_tree_item_t
{
    /** Generic parent class for all Open MPI objects */
    opal_object_t super;
    /** Pointer to the tree this item belongs to */
    struct opal_tree_t *opal_tree_container;
    /* Parent info */
    /** Pointer to parent tree item */
    struct opal_tree_item_t *opal_tree_parent;
    /** Depth from the root item in tree */
    unsigned opal_tree_num_ancestors;
    /* Logical rank we are compared to other siblings */
    unsigned opal_tree_sibling_rank;
    /** Pointer to next item below same parent (or NULL if this is the
        leftmost sibling) */
    struct opal_tree_item_t *opal_tree_next_sibling;
    /** Pointer to previous item below same parent (or NULL if this is
        the rightmost sibling) */
    struct opal_tree_item_t *opal_tree_prev_sibling;

    /* Children info */
    /** Number of children */
    unsigned opal_tree_num_children;
    /** Pointer to first child item, or NULL if there are no children */
    struct opal_tree_item_t *opal_tree_first_child;
    /** Pointer to last child item, or NULL if there are no children */
    struct opal_tree_item_t *opal_tree_last_child;

#if OPAL_ENABLE_DEBUG
    /** Atomic reference count for debugging */
    int32_t opal_tree_item_refcount;
    /** The tree this item belong to */
    struct opal_tree_t* opal_tree_item_belong_to;
#endif
} opal_tree_item_t;

/**
 * Check to see if item's user specific data matches key
 *
 * @param item - The item we want to check to see if it matches key
 * @param key - The opaque key that we want the match function to check
 *              in the item's user specific data.
 *
 * @returns 0 - If item's user specific data matches key
 * @returns non-zero - If item's user specific data does not match key
 *
 * This function is implemented by the code that constructs the tree
 * and initialized the pointer by the call to opal_tree_init.
 *
 */
typedef int (*opal_tree_comp_fn_t)(opal_tree_item_t *item, void *key);

/**
  * The serialize function typedef. This function is called by the
  * opal tree serialize code to serialize a tree item's user specific
  * data of a class type.
  *
  * @params item - item to serialize the user specific data from
  * @params buffer - the opal_buffer_t to store the serialized data in.
  *
  * @returns OPAL_SUCCESS - when successfully serialized item
  */
typedef int (*opal_tree_item_serialize_fn_t)(opal_tree_item_t *item,
					     opal_buffer_t *buffer);

/**
  * The deserialize function typedef. This function is called by the
  * opal tree deserialize code to deserialize a tree item's user
  * specific data.
  *
  * @params buffer - the opal_buffer_t to deserialized data.
  * @params item - item to store the deserialized data into the user
  *                specific data
  *
  * @returns OPAL_SUCCESS - when successfully deserialized item
  */
typedef int (*opal_tree_item_deserialize_fn_t)(opal_buffer_t *buffer,
					       opal_tree_item_t **item);

/**
  * Get the 'key' associated with this item
  */
typedef void *(*opal_tree_get_key_fn_t)(opal_tree_item_t *item);

/**
 * \internal
 *
 * Struct of an opal_tree_t
 *
 */
typedef struct opal_tree_t
{
    /** Generic parent class for all Open MPI objects */
    opal_object_t       super;
    /** Guard item of the tree that child points to root */
    opal_tree_item_t    opal_tree_sentinel;
    /** Quick reference to the number of items in the tree */
    volatile size_t     opal_tree_num_items;
    /** Function to compare two items in tree */
    opal_tree_comp_fn_t comp;
    /** Function to serialize tree item data */
    opal_tree_item_serialize_fn_t serialize;
    /** Function to deserialize tree item data */
    opal_tree_item_deserialize_fn_t deserialize;
    /**< Function to deserialize tree item data */
    opal_tree_get_key_fn_t get_key;
} opal_tree_t;

/** Macros to access items in the tree */

/**
 * Get the parent of item in the tree.
 *
 * @param item A tree item.
 *
 * @returns The parent item in the tree
 *
 * This function is safe to be called with a null item pointer.
 */
static inline opal_tree_item_t *opal_tree_get_parent(opal_tree_item_t *item)
{
    return ((item) ? item->opal_tree_parent : NULL);
}

/**
 * Get the next sibling item in the tree.
 *
 * @param item A tree item.
 *
 * @returns The next sibling item in the tree
 *
 * This function is safe to be called with a null item pointer.
 */
static inline opal_tree_item_t *opal_tree_get_next_sibling(opal_tree_item_t
                                                           *item)
{
    return ((item) ? item->opal_tree_next_sibling : NULL);
}


/**
 * Get the previous sibling item in the tree.
 *
 * @param item A tree item.
 *
 * @returns The previous sibling item in the tree
 *
 * This function is safe to be called with a null item pointer.
 */
static inline opal_tree_item_t *opal_tree_get_prev_sibling(opal_tree_item_t
                                                           *item)
{
    return ((item) ? item->opal_tree_prev_sibling : NULL);
}

/**
 * Get the first child item in the tree.
 *
 * @param item A tree item.
 *
 * @returns The first child item in the tree
 *
 * This function is safe to be called with a null item pointer.
 *
 */
static inline opal_tree_item_t *opal_tree_get_first_child(opal_tree_item_t
                                                          *item)
{
    return ((item) ? item->opal_tree_first_child : NULL);
}

/**
 * Get the last child item in the tree.
 *
 * @param item A tree item.
 *
 * @returns The last child item in the tree
 *
 * This function is safe to be called with a null item pointer.
 *
 */
static inline opal_tree_item_t *opal_tree_get_last_child(opal_tree_item_t
                                                         *item)
{
    return ((item) ? item->opal_tree_last_child : NULL);
}

/**
 * Check for empty tree
 *
 * @param tree The tree container
 *
 * @returns true if tree's size is 0, false otherwise
 *
 * This is an O(1) operation.
 *
 * This is an inlined function in compilers that support inlining,
 * so it's usually a cheap operation.
 *
 */
static inline bool opal_tree_is_empty(opal_tree_t* tree)
{
#if OPAL_ENABLE_DEBUG
    /* Spot check that the tree is a non-null pointer */
    assert(NULL != tree);
#endif
    return (tree->opal_tree_sentinel.opal_tree_first_child ==
            &(tree->opal_tree_sentinel) ? true : false);
}


/**
 * Return the root item on the tree (does not remove it).
 *
 * @param tree The tree container
 *
 * @returns A pointer to the first item in the tree
 *
 * This is an O(1) operation to return the first item in the tree.
 *
 * This is an inlined function in compilers that support inlining, so
 * it's usually a cheap operation.
 *
 */
static inline opal_tree_item_t* opal_tree_get_root(opal_tree_t* tree)
{
    opal_tree_item_t* item;
#if OPAL_ENABLE_DEBUG
    assert(NULL != tree);
#endif
    item = tree->opal_tree_sentinel.opal_tree_first_child;
#if OPAL_ENABLE_DEBUG
    /* Spot check: ensure that the first item is only on one list */
    assert(1 == item->opal_tree_item_refcount);
    assert(tree == item->opal_tree_item_belong_to );
#endif
    return item;
}

/**
 * Return the number of items in a tree
 *
 * @param tree The tree container
 *
 * @returns The size of the tree (size_t)
 *
 * This is an O(1) (in non-debug mode) lookup to return the
 * size of the list.
 */
OPAL_DECLSPEC size_t opal_tree_get_size(opal_tree_t* tree);


/* Functions to manage the tree */
/**
 * Initialize tree container; must be called before using
 * the tree.
 *
 * @param tree        The tree to initialize
 * @param comp        Comparison function to attach to tree.
 * @param serialize   Serialization function to attach to tree.
 * @param deserialize De-serialization function to attach to tree.
 *
 */
OPAL_DECLSPEC void opal_tree_init(opal_tree_t *tree,
                                  opal_tree_comp_fn_t comp,
                                  opal_tree_item_serialize_fn_t serialize,
                                  opal_tree_item_deserialize_fn_t deserialize,
                                  opal_tree_get_key_fn_t get_key);

/**
 * Add new item as child to its parent item
 *
 * @param parent_item pointer to what parent the new item belongs to
 * @param new_item the item to be added as a child to parent_item
 *
 * The new_item is added at the end of the child list of the parent_item.
 */
OPAL_DECLSPEC void opal_tree_add_child(opal_tree_item_t *parent_item,
				       opal_tree_item_t *new_item);

/**
 * Remove an item and everything below from a tree.
 *
 * @param item The item at the top of subtree to remove
 *
 * @returns A pointer to the item on the list previous to the one
 * that was removed.
 *
 * This is an O(1) operation to remove an item from the tree.  The
 * item and all children below it will be removed from the tree.  This
 * means the item's siblings pointers and potentially the parents first
 * and last pointers will be updated to skip over the item.  The tree container
 * will also have its num_items adjusted to reflect the number of items
 * that were removed.  The tree item (and all children below it) that is
 * returned is now "owned" by the caller -- they are responsible for
 * OBJ_RELEASE()'ing it.
 *
 * With ENABLE_DEBUG on this routine will validate whether the item is actually
 * in the tree before doing pointer manipulation.
 */
OPAL_DECLSPEC opal_tree_item_t *opal_tree_remove_subtree(opal_tree_item_t *item);

/**
 * Remove an item, everything below inherited by parent.
 *
 * @param tree Tree from which to remove
 * @param item The item to remove
 *
 * @returns Success/Failure
 */
OPAL_DECLSPEC int opal_tree_remove_item(opal_tree_t *tree,
                                        opal_tree_item_t *item);

/**
 * Serialize tree data
 *
 * @param start_item The item of a tree to start serializing data
 * @param buffer The opal buffer that contains the serialized
 *  data stream of the tree
 *
 * @returns OPAL_SUCCESS if data has been successfully converted.
 *
 * This routine walks the tree starting at start_item until it has serialized
 * all children items of start_item and creates a bytestream of data,
 * using the opal_dss.pack routine, that can be sent over a network.
 * The format of the bytestream represents the tree parent/child relationship
 * of each item in the tree plus the data inside the tree.  This routine calls
 * the tree's serialization method to serialize the user specific data for
 * each item.
 *
 */
OPAL_DECLSPEC int opal_tree_serialize(opal_tree_item_t *start_item,
				      opal_buffer_t *buffer);

/**
 * De-serialize tree data
 *
 * @param buffer The opal buffer that is to be deserialized
 * @param start_item The item in the tree the data should be
 * deserialized into
 *
 * @returns Status of call OPAL_SUCCESS if everything worked
 *
 * This routine takes a bytestream that was created by the
 * opal_tree_serialize() function and deserializes it into the
 * tree given.  If the tree already has data in it, this routine
 * will start adding the new data as a new child of the root
 * item.  This routine calls the tree's de-serialization
 * method to deserialize the user specific data for each item.
 *
 */
OPAL_DECLSPEC int opal_tree_deserialize(opal_buffer_t *buffer,
                                        opal_tree_item_t *start_item);

/**
 * Access the 'key' associated with the item
 *
 * @param tree Source Tree
 * @param item Item to access key of
 *
 * @returns Success/Failure
 */
OPAL_DECLSPEC void * opal_tree_get_key(opal_tree_t *tree, opal_tree_item_t *item);

/**
 * Copy/Duplicate a tree (requires serialize/deserialize)
 *
 * @param from Source tree to copy 'from'
 * @param to   Destination tree to copy 'to'
 *
 * @returns Success/Failure
 */
OPAL_DECLSPEC int opal_tree_dup(opal_tree_t *from, opal_tree_t *to);

/**
 * Copy/Duplicate a subtree (requires serialize/deserialize)
 *
 * @param base Base tree
 * @param from Source tree item to copy 'from'
 *
 * @returns Tree item copy
 */
OPAL_DECLSPEC int opal_tree_copy_subtree(opal_tree_t *from_tree, opal_tree_item_t *from_item,
                                         opal_tree_t *to_tree,   opal_tree_item_t *to_parent);

/**
 * Copy/Duplicate a tree item (requires serialize/deserialize)
 *
 * @param base Base tree
 * @param from Source tree item to copy 'from'
 *
 * @returns Tree item copy
 */
OPAL_DECLSPEC opal_tree_item_t *opal_tree_dup_item(opal_tree_t *base, opal_tree_item_t *from);

/**
 * Count the number of children of this parent
 *
 * @param parent A parent node in the tree
 *
 * @returns Number of children of this parent
 */
OPAL_DECLSPEC int opal_tree_num_children(opal_tree_item_t *parent);

/**
 * Compare two trees
 *
 * @param left Tree
 * @param right Tree
 *
 * @returns 0 if identical, ow returns non-zero
 */
OPAL_DECLSPEC int opal_tree_compare(opal_tree_t *left, opal_tree_t *right);

/* Functions to search for items on tree */
/**
 * Return the next tree item that matches key provided
 *
 * @param item The item to start the find from
 * @param key the key we are wanting to match with
 *
 * @returns A pointer to the next item that in the tree (starting from item)
 * that matches the key based on a depth first search of the tree.  A null
 * pointer is returned if we've reached the end of the tree and have not
 * matched the key.
 *
 * This routine uses the tree container's comp function to determine the
 * whether there is a match between the key and each item we search in the
 * tree.  This means the actual tree type constructed determines how the
 * compare is done with the key.  In the case no compare routine is given
 * and NULL pointer is always returned for this function.
 *
 */
OPAL_DECLSPEC opal_tree_item_t *opal_tree_find_with(opal_tree_item_t *item,
                                                    void *key);
END_C_DECLS

#endif /* OPAL_TREE_H */
