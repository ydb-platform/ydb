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
 * Copyright (c) 2015-2018 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/*
 * @file
 */

#include "opal_config.h"

#include "opal/class/opal_interval_tree.h"
#include <limits.h>

/* Private functions */
static void opal_interval_tree_insert_node (opal_interval_tree_t *tree, opal_interval_tree_node_t *node);

/* tree rebalancing functions */
static void opal_interval_tree_delete_fixup (opal_interval_tree_t *tree, opal_interval_tree_node_t *node,
                                             opal_interval_tree_node_t *parent);
static void opal_interval_tree_insert_fixup (opal_interval_tree_t *tree, opal_interval_tree_node_t *x);

static opal_interval_tree_node_t *opal_interval_tree_next (opal_interval_tree_t *tree,
                                                           opal_interval_tree_node_t *node);
static opal_interval_tree_node_t * opal_interval_tree_find_node(opal_interval_tree_t *tree,
                                                                uint64_t low, uint64_t high,
                                                                bool exact, void *data);

static opal_interval_tree_node_t *left_rotate (opal_interval_tree_t *tree, opal_interval_tree_node_t *x);
static opal_interval_tree_node_t *right_rotate (opal_interval_tree_t *tree, opal_interval_tree_node_t *x);

static void inorder_destroy(opal_interval_tree_t *tree, opal_interval_tree_node_t * node);

#define max(x,y) (((x) > (y)) ? (x) : (y))

/**
 * the constructor function. creates the free list to get the nodes from
 *
 * @param object the tree that is to be used
 *
 * @retval NONE
 */
static void opal_interval_tree_construct (opal_interval_tree_t *tree)
{
    OBJ_CONSTRUCT(&tree->root, opal_interval_tree_node_t);
    OBJ_CONSTRUCT(&tree->nill, opal_interval_tree_node_t);
    OBJ_CONSTRUCT(&tree->free_list, opal_free_list_t);
    OBJ_CONSTRUCT(&tree->gc_list, opal_list_t);

    /* initialize sentinel */
    tree->nill.color = OPAL_INTERVAL_TREE_COLOR_BLACK;
    tree->nill.left = tree->nill.right = tree->nill.parent = &tree->nill;
    tree->nill.max = 0;
    tree->nill.data = NULL;

    /* initialize root sentinel */
    tree->root.color = OPAL_INTERVAL_TREE_COLOR_BLACK;
    tree->root.left = tree->root.right = tree->root.parent = &tree->nill;
    /* this simplifies inserting at the root as we only have to check the
     * low value. */
    tree->root.low = (uint64_t) -1;
    tree->root.data = NULL;

    /* set the tree size to zero */
    tree->tree_size = 0;
    tree->lock = 0;
    tree->reader_count = 0;
    tree->epoch = 0;

    /* set all reader epochs to UINT_MAX. this value is used to simplfy
     * checks against the current epoch. */
    for (int i = 0 ; i < OPAL_INTERVAL_TREE_MAX_READERS ; ++i) {
        tree->reader_epochs[i] = UINT_MAX;
    }
}

/**
 * the destructor function. Free the tree and destroys the free list.
 *
 * @param object the tree object
 */
static void opal_interval_tree_destruct (opal_interval_tree_t *tree)
{
    opal_interval_tree_destroy (tree);

    OBJ_DESTRUCT(&tree->free_list);
    OBJ_DESTRUCT(&tree->root);
    OBJ_DESTRUCT(&tree->nill);
}

/* declare the instance of the classes  */
OBJ_CLASS_INSTANCE(opal_interval_tree_node_t, opal_free_list_item_t, NULL, NULL);
OBJ_CLASS_INSTANCE(opal_interval_tree_t, opal_object_t, opal_interval_tree_construct,
                   opal_interval_tree_destruct);

typedef int32_t opal_interval_tree_token_t;

/**
 * @brief pick and return a reader slot
 */
static opal_interval_tree_token_t opal_interval_tree_reader_get_token (opal_interval_tree_t *tree)
{
    opal_interval_tree_token_t token = -1;

    if (token < 0) {
        int32_t reader_count = tree->reader_count;
        /* NTH: could have used an atomic here but all we are after is some distribution of threads
         * across the reader slots. with high thread counts i see no real performance difference
         * using atomics. */
        token = tree->reader_id++ % OPAL_INTERVAL_TREE_MAX_READERS;
        while (OPAL_UNLIKELY(reader_count <= token)) {
            if (opal_atomic_compare_exchange_strong_32 (&tree->reader_count, &reader_count, token + 1)) {
                break;
            }
        }
    }

    while (!OPAL_ATOMIC_COMPARE_EXCHANGE_STRONG_32((volatile int32_t *) &tree->reader_epochs[token],
                                                   &(int32_t) {UINT_MAX}, tree->epoch));

    return token;
}

static void opal_interval_tree_reader_return_token (opal_interval_tree_t *tree, opal_interval_tree_token_t token)
{
    tree->reader_epochs[token] = UINT_MAX;
}

/* Create the tree */
int opal_interval_tree_init (opal_interval_tree_t *tree)
{
    return opal_free_list_init (&tree->free_list, sizeof(opal_interval_tree_node_t),
                                opal_cache_line_size, OBJ_CLASS(opal_interval_tree_node_t),
                                0, opal_cache_line_size, 0, -1 , 128, NULL, 0, NULL, NULL, NULL);
}

static bool opal_interval_tree_write_trylock (opal_interval_tree_t *tree)
{
    opal_atomic_rmb ();
    return !(tree->lock || opal_atomic_swap_32 (&tree->lock, 1));
}

static void opal_interval_tree_write_lock (opal_interval_tree_t *tree)
{
    while (!opal_interval_tree_write_trylock (tree));
}

static void opal_interval_tree_write_unlock (opal_interval_tree_t *tree)
{
    opal_atomic_wmb ();
    tree->lock = 0;
}

static void opal_interval_tree_insert_fixup_helper (opal_interval_tree_t *tree, opal_interval_tree_node_t *node) {
    opal_interval_tree_node_t *y, *parent = node->parent;
    bool rotate_right = false;

    if (parent->color == OPAL_INTERVAL_TREE_COLOR_BLACK) {
        return;
    }

    if (parent == parent->parent->left) {
        y = parent->parent->right;
        rotate_right = true;
    } else {
        y = parent->parent->left;
    }

    if (y->color == OPAL_INTERVAL_TREE_COLOR_RED) {
        parent->color = OPAL_INTERVAL_TREE_COLOR_BLACK;
        y->color = OPAL_INTERVAL_TREE_COLOR_BLACK;
        parent->parent->color = OPAL_INTERVAL_TREE_COLOR_RED;
        opal_interval_tree_insert_fixup_helper (tree, parent->parent);
        return;
    }

    if (rotate_right) {
        if (node == parent->right) {
            node = left_rotate (tree, parent);
            parent = node->parent;
        }

        parent->color = OPAL_INTERVAL_TREE_COLOR_BLACK;
        parent->parent->color = OPAL_INTERVAL_TREE_COLOR_RED;
        (void) right_rotate(tree, parent->parent);
    } else {
        if (node == parent->left) {
            node = right_rotate(tree, parent);
            parent = node->parent;
        }
        parent->color = OPAL_INTERVAL_TREE_COLOR_BLACK;
        parent->parent->color = OPAL_INTERVAL_TREE_COLOR_RED;
        (void) left_rotate(tree, parent->parent);
    }

    opal_interval_tree_insert_fixup_helper (tree, node);
}

static void opal_interval_tree_insert_fixup (opal_interval_tree_t *tree, opal_interval_tree_node_t *node) {
    /* do the rotations */
    /* usually one would have to check for NULL, but because of the sentinal,
     * we don't have to   */
    opal_interval_tree_insert_fixup_helper (tree, node);

    /* after the rotations the root is black */
    tree->root.left->color = OPAL_INTERVAL_TREE_COLOR_BLACK;
}

/**
 * @brief Guts of the delete fixup
 *
 * @param[in] tree    opal interval tree
 * @param[in] node    node to fixup
 * @param[in] left    true if the node is a left child of its parent
 *
 * @returns the next node to fixup or root if done
 */
static inline opal_interval_tree_node_t *
opal_interval_tree_delete_fixup_helper (opal_interval_tree_t *tree, opal_interval_tree_node_t *node,
                                        opal_interval_tree_node_t *parent, const bool left)
{
    opal_interval_tree_node_t *w;

    /* get sibling */
    w = left ? parent->right : parent->left;
    if (w->color == OPAL_INTERVAL_TREE_COLOR_RED) {
        w->color = OPAL_INTERVAL_TREE_COLOR_BLACK;
        parent->color = OPAL_INTERVAL_TREE_COLOR_RED;
        if (left) {
            (void) left_rotate(tree, parent);
            w = parent->right;
        } else {
            (void) right_rotate(tree, parent);
            w = parent->left;
        }
    }

    if ((w->left->color == OPAL_INTERVAL_TREE_COLOR_BLACK) && (w->right->color == OPAL_INTERVAL_TREE_COLOR_BLACK)) {
        w->color = OPAL_INTERVAL_TREE_COLOR_RED;
        return parent;
    }

    if (left) {
        if (w->right->color == OPAL_INTERVAL_TREE_COLOR_BLACK) {
            w->left->color = OPAL_INTERVAL_TREE_COLOR_BLACK;
            w->color = OPAL_INTERVAL_TREE_COLOR_RED;
            (void) right_rotate(tree, w);
            w = parent->right;
        }
        w->color = parent->color;
        parent->color = OPAL_INTERVAL_TREE_COLOR_BLACK;
        w->right->color = OPAL_INTERVAL_TREE_COLOR_BLACK;
        (void) left_rotate(tree, parent);
    } else {
        if (w->left->color == OPAL_INTERVAL_TREE_COLOR_BLACK) {
            w->right->color = OPAL_INTERVAL_TREE_COLOR_BLACK;
            w->color = OPAL_INTERVAL_TREE_COLOR_RED;
            (void) left_rotate(tree, w);
            w = parent->left;
        }
        w->color = parent->color;
        parent->color = OPAL_INTERVAL_TREE_COLOR_BLACK;
        w->left->color = OPAL_INTERVAL_TREE_COLOR_BLACK;
        (void) right_rotate(tree, parent);
    }

    /* return the root */
    return tree->root.left;
 }

/* Fixup the balance of the btree after deletion    */
static void opal_interval_tree_delete_fixup (opal_interval_tree_t *tree, opal_interval_tree_node_t *node,
                                             opal_interval_tree_node_t *parent)
{
    while ((node != tree->root.left) && (node->color == OPAL_INTERVAL_TREE_COLOR_BLACK)) {
        node = opal_interval_tree_delete_fixup_helper (tree, node, parent, node == parent->left);
        parent = node->parent;
    }

    node->color = OPAL_INTERVAL_TREE_COLOR_BLACK;
    tree->nill.color = OPAL_INTERVAL_TREE_COLOR_BLACK;
}

/* traverse the garbage-collection list and return any nodes that can not have any
 * references. this function MUST be called with the writer lock held. */
static void opal_interval_tree_gc_clean (opal_interval_tree_t *tree)
{
    opal_interval_tree_node_t *node, *next;
    uint32_t oldest_epoch = UINT_MAX;

    if (0 == opal_list_get_size (&tree->gc_list)) {
        return;
    }

    for (int i = 0 ; i < tree->reader_count ; ++i) {
        oldest_epoch = (oldest_epoch < tree->reader_epochs[i]) ? oldest_epoch : tree->reader_epochs[i];
    }

    OPAL_LIST_FOREACH_SAFE(node, next, &tree->gc_list, opal_interval_tree_node_t) {
        if (node->epoch < oldest_epoch) {
            opal_list_remove_item (&tree->gc_list, &node->super.super);
            opal_free_list_return_st (&tree->free_list, &node->super);
        }
    }
}

/* This inserts a node into the tree based on the passed values. */
int opal_interval_tree_insert (opal_interval_tree_t *tree, void *value, uint64_t low, uint64_t high)
{
    opal_interval_tree_node_t * node;

    if (low > high) {
        return OPAL_ERR_BAD_PARAM;
    }

    opal_interval_tree_write_lock (tree);

    opal_interval_tree_gc_clean (tree);

    /* get the memory for a node */
    node = (opal_interval_tree_node_t *) opal_free_list_get (&tree->free_list);
    if (OPAL_UNLIKELY(NULL == node)) {
        opal_interval_tree_write_unlock (tree);
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    /* insert the data into the node */
    node->data = value;
    node->low = low;
    node->high = high;
    node->max = high;
    node->epoch = tree->epoch;

    /* insert the node into the tree */
    opal_interval_tree_insert_node (tree, node);

    opal_interval_tree_insert_fixup (tree, node);
    opal_interval_tree_write_unlock (tree);

    return OPAL_SUCCESS;
}

static opal_interval_tree_node_t *opal_interval_tree_find_interval(opal_interval_tree_t *tree, opal_interval_tree_node_t *node, uint64_t low,
                                                                   uint64_t high, bool exact, void *data)
{
    if (node == &tree->nill) {
        return NULL;
    }

    if (((exact && node->low == low && node->high == high) || (!exact && node->low <= low && node->high >= high)) &&
        (!data || node->data == data)) {
        return node;
    }

    if (low <= node->low) {
        return opal_interval_tree_find_interval (tree, node->left, low, high, exact, data);
    }

    return opal_interval_tree_find_interval (tree, node->right, low, high, exact, data);
}

/* Finds the node in the tree based on the key and returns a pointer
 * to the node. This is a bit a code duplication, but this has to be fast
 * so we go ahead with the duplication */
static opal_interval_tree_node_t *opal_interval_tree_find_node(opal_interval_tree_t *tree, uint64_t low, uint64_t high, bool exact, void *data)
{
    return opal_interval_tree_find_interval (tree, tree->root.left, low, high, exact, data);
}

void *opal_interval_tree_find_overlapping (opal_interval_tree_t *tree, uint64_t low, uint64_t high)
{
    opal_interval_tree_token_t token;
    opal_interval_tree_node_t *node;

    token = opal_interval_tree_reader_get_token (tree);
    node = opal_interval_tree_find_node (tree, low, high, true, NULL);
    opal_interval_tree_reader_return_token (tree, token);

    return node ? node->data : NULL;
}

static size_t opal_interval_tree_depth_node (opal_interval_tree_t *tree, opal_interval_tree_node_t *node)
{
    if (&tree->nill == node) {
        return 0;
    }

    return 1 + max (opal_interval_tree_depth_node (tree, node->right), opal_interval_tree_depth_node (tree, node->left));
}

size_t opal_interval_tree_depth (opal_interval_tree_t *tree)
{
    opal_interval_tree_token_t token;
    size_t depth;

    token = opal_interval_tree_reader_get_token (tree);
    depth = opal_interval_tree_depth_node (tree, &tree->root);
    opal_interval_tree_reader_return_token (tree, token);

    return depth;
}

/* update the value of a tree pointer */
static inline void rp_publish (opal_interval_tree_node_t **ptr, opal_interval_tree_node_t *node)
{
    /* ensure all writes complete before continuing */
    opal_atomic_wmb ();
    /* just set the value */
    *ptr = node;
}


static inline void rp_wait_for_readers (opal_interval_tree_t *tree)
{
    uint32_t epoch_id = ++tree->epoch;

    /* wait for all readers to see the new tree version */
    for (int i = 0 ; i < tree->reader_count ; ++i) {
        while (tree->reader_epochs[i] < epoch_id);
    }
}

/* waits for all writers to finish with the node then releases the last reference */
static inline void rp_free_wait (opal_interval_tree_t *tree, opal_interval_tree_node_t *node)
{
    rp_wait_for_readers (tree);
    /* no other threads are working on this node so go ahead and return it */
    opal_free_list_return_st (&tree->free_list, &node->super);
}

/* schedules the node for releasing */
static inline void rp_free (opal_interval_tree_t *tree, opal_interval_tree_node_t *node)
{
    opal_list_append (&tree->gc_list, &node->super.super);
}

static opal_interval_tree_node_t *opal_interval_tree_node_copy (opal_interval_tree_t *tree, opal_interval_tree_node_t *node)
{
    opal_interval_tree_node_t *copy = (opal_interval_tree_node_t *) opal_free_list_wait_st (&tree->free_list);
    size_t color_offset = offsetof(opal_interval_tree_node_t, color);
    assert (NULL != copy);
    memcpy ((unsigned char *) copy + color_offset, (unsigned char *) node + color_offset,
            sizeof (*node) - color_offset);
    return copy;
}

/* this function deletes a node that is either a left or right leaf (or both) */
static void opal_interval_tree_delete_leaf (opal_interval_tree_t *tree, opal_interval_tree_node_t *node)
{
    const opal_interval_tree_node_t *nill = &tree->nill;
    opal_interval_tree_node_t **parent_ptr, *next, *parent = node->parent;
    opal_interval_tree_nodecolor_t color = node->color;

    assert (node->left == nill || node->right == nill);

    parent_ptr = (parent->right == node) ? &parent->right : &parent->left;

    next = (node->right == nill) ? node->left : node->right;

    next->parent = node->parent;
    rp_publish (parent_ptr, next);

    rp_free (tree, node);

    if (OPAL_INTERVAL_TREE_COLOR_BLACK == color) {
        if (OPAL_INTERVAL_TREE_COLOR_RED == next->color) {
            next->color = OPAL_INTERVAL_TREE_COLOR_BLACK;
        } else {
            opal_interval_tree_delete_fixup (tree, next, parent);
        }
    }
}

static void opal_interval_tree_delete_interior (opal_interval_tree_t *tree, opal_interval_tree_node_t *node)
{
    opal_interval_tree_node_t **parent_ptr, *next, *next_copy, *parent = node->parent;
    opal_interval_tree_nodecolor_t color = node->color, next_color;

    parent_ptr = (parent->right == node) ? &parent->right : &parent->left;
    next = opal_interval_tree_next (tree, node);
    next_color = next->color;

    if (next != node->right) {
        /* case 3 */
        next_copy = opal_interval_tree_node_copy (tree, next);
        next_copy->color = node->color;
        next_copy->left = node->left;
        next_copy->left->parent = next_copy;
        next_copy->right = node->right;
        next_copy->right->parent = next_copy;
        next_copy->parent = node->parent;

        rp_publish (parent_ptr, next_copy);
        rp_free_wait (tree, node);

        opal_interval_tree_delete_leaf (tree, next);
    } else {
        /* case 2. no copies are needed */
        next->color = color;
        next->left = node->left;
        next->left->parent = next;
        next->parent = node->parent;
        rp_publish (parent_ptr, next);
        rp_free (tree, node);

	/* since we are actually "deleting" the next node the fixup needs to happen on the
	 * right child of next (by definition next was a left child) */
        if (OPAL_INTERVAL_TREE_COLOR_BLACK == next_color) {
	    if (OPAL_INTERVAL_TREE_COLOR_RED == next->right->color) {
		next->right->color = OPAL_INTERVAL_TREE_COLOR_BLACK;
            } else {
		opal_interval_tree_delete_fixup (tree, next->right, next);
	    }
	}
    }
}

/* Delete a node from the tree based on the key */
int opal_interval_tree_delete (opal_interval_tree_t *tree, uint64_t low, uint64_t high, void *data)
{
    opal_interval_tree_node_t *node;

    opal_interval_tree_write_lock (tree);
    node = opal_interval_tree_find_node (tree, low, high, true, data);
    if (NULL == node) {
        opal_interval_tree_write_unlock (tree);
        return OPAL_ERR_NOT_FOUND;
    }

    /* there are three cases that have to be handled:
     * 1) the node p is a left leaf or a right left (one of p's children is nill)
     *    in this case we can delete p and we can replace it with one of it's children
     *    or nill (if both children are nill).
     * 2) the right child of p is a left leaf (node->right->left == nill)
     *    in this case we can set node->right->left = node->left and replace node with node->right
     * 3) p is a interior node
     *    we replace node with next(node)
     */
    
    if ((node->left == &tree->nill) || (node->right == &tree->nill)) {
        /* handle case 1 */
        opal_interval_tree_delete_leaf (tree, node);
    } else {
        /* handle case 2 and 3 */
        opal_interval_tree_delete_interior (tree, node);
    }

    --tree->tree_size;

    opal_interval_tree_write_unlock (tree);

    return OPAL_SUCCESS;
}

int opal_interval_tree_destroy (opal_interval_tree_t *tree)
{
    /* Recursive inorder traversal for delete */
    inorder_destroy(tree, &tree->root);
    tree->tree_size = 0;
    return OPAL_SUCCESS;
}


/* Find the next inorder successor of a node    */
static opal_interval_tree_node_t *opal_interval_tree_next (opal_interval_tree_t *tree, opal_interval_tree_node_t *node)
{
    opal_interval_tree_node_t *p = node->right;

    if (p == &tree->nill) {
        p = node->parent;
        while (node == p->right) {
            node = p;
            p = p->parent;
        }

        if (p == &tree->root) {
            return &tree->nill;
        }

        return p;
    }

    while (p->left != &tree->nill) {
        p = p->left;
    }

    return p;
}

/* Insert an element in the normal binary search tree fashion    */
/* this function goes through the tree and finds the leaf where
 * the node will be inserted   */
static void opal_interval_tree_insert_node (opal_interval_tree_t *tree, opal_interval_tree_node_t *node)
{
    opal_interval_tree_node_t *parent = &tree->root;
    opal_interval_tree_node_t *n = parent->left; /* the real root of the tree */
    opal_interval_tree_node_t *nill = &tree->nill;

    /* set up initial values for the node */
    node->color = OPAL_INTERVAL_TREE_COLOR_RED;
    node->parent = NULL;
    node->left = nill;
    node->right = nill;

    /* find the leaf where we will insert the node */
    while (n != nill) {
        if (n->max < node->high) {
            n->max = node->high;
        }

        parent = n;
        n = ((node->low < n->low) ? n->left : n->right);
        assert (nill == n || n->parent == parent);
    }

    /* place it on either the left or the right */
    if ((node->low < parent->low)) {
        parent->left = node;
    } else {
        parent->right = node;
    }

    /* set its parent and children */
    node->parent = parent;

    ++tree->tree_size;
}

static int inorder_traversal (opal_interval_tree_t *tree, uint64_t low, uint64_t high,
			      bool partial_ok, opal_interval_tree_action_fn_t action,
			      opal_interval_tree_node_t * node, void *ctx)
{
    int rc;

    if (node == &tree->nill) {
        return OPAL_SUCCESS;
    }

    rc = inorder_traversal(tree, low, high, partial_ok, action, node->left, ctx);
    if (OPAL_SUCCESS != rc) {
        return rc;
    }

    if ((!partial_ok && (node->low <= low && node->high >= high)) ||
        (partial_ok && ((low >= node->low && low <= node->high) ||
                        (high >= node->low && high <= node->high) ||
                        (node->low >= low && node->low <= high) ||
                        (node->high >= high && node->high <= high)))) {
        rc = action (node->low, node->high, node->data, ctx);
        if (OPAL_SUCCESS != rc) {
            return rc;
        }
    }

    return inorder_traversal(tree, low, high, partial_ok, action, node->right, ctx);
}

/* Free the nodes in inorder fashion    */

static void inorder_destroy (opal_interval_tree_t *tree, opal_interval_tree_node_t *node)
{
    if (node == &tree->nill) {
        return;
    }

    inorder_destroy(tree, node->left);
    inorder_destroy(tree, node->right);

    if (node->left != &tree->nill) {
        opal_free_list_return_st (&tree->free_list, &node->left->super);
    }

    if (node->right != &tree->nill) {
        opal_free_list_return_st (&tree->free_list, &node->right->super);
    }
}

/* Try to access all the elements of the hashmap conditionally */

int opal_interval_tree_traverse (opal_interval_tree_t *tree, uint64_t low, uint64_t high,
                                 bool partial_ok, opal_interval_tree_action_fn_t action, void *ctx)
{
    opal_interval_tree_token_t token;
    int rc;

    if (action == NULL) {
        return OPAL_ERR_BAD_PARAM;
    }

    token = opal_interval_tree_reader_get_token (tree);
    rc = inorder_traversal (tree, low, high, partial_ok, action, tree->root.left, ctx);
    opal_interval_tree_reader_return_token (tree, token);
    return rc;
}

/* Left rotate the tree    */
/* basically what we want to do is to make x be the left child
 * of its right child    */
static opal_interval_tree_node_t *left_rotate (opal_interval_tree_t *tree, opal_interval_tree_node_t *x)
{
    opal_interval_tree_node_t *x_copy = x;
    opal_interval_tree_node_t *y = x->right;
    opal_interval_tree_node_t *parent = x->parent;

    /* make the left child of y's parent be x if it is not the sentinal node*/
    if (y->left != &tree->nill) {
        y->left->parent = x_copy;
    }

    /* x's parent is now y */
    x_copy->parent = y;
    x_copy->right = y->left;
    x_copy->max = max (x_copy->high, max (x_copy->left->max, x_copy->left->max));

    rp_publish (&y->left, x_copy);

    /* normlly we would have to check to see if we are at the root.
     * however, the root sentinal takes care of it for us */
    if (x == parent->left) {
        rp_publish (&parent->left, y);
    } else {
        rp_publish (&parent->right, y);
    }

    /* the old parent of x is now y's parent */
    y->parent = parent;

    return x_copy;
}


/* Right rotate the tree    */
/* basically what we want to do is to make x be the right child
 * of its left child */
static opal_interval_tree_node_t *right_rotate (opal_interval_tree_t *tree, opal_interval_tree_node_t *x)
{
    opal_interval_tree_node_t *x_copy = x;
    opal_interval_tree_node_t *y = x->left;
    opal_interval_tree_node_t *parent = x->parent;

    /* make the left child of y's parent be x if it is not the sentinal node*/
    if (y->right != &tree->nill) {
        y->right->parent = x_copy;
    }

    x_copy->left = y->right;
    x_copy->parent = y;

    rp_publish (&y->right, x_copy);

    /* the maximum value in the subtree rooted at y is now the value it
     * was at x */
    y->max = x->max;
    y->parent = parent;

    if (parent->left == x) {
        rp_publish (&parent->left, y);
    } else {
        rp_publish (&parent->right, y);
    }

    return x_copy;
}

/* returns the size of the tree */
size_t opal_interval_tree_size(opal_interval_tree_t *tree)
{
    return tree->tree_size;
}

static bool opal_interval_tree_verify_node (opal_interval_tree_t *tree, opal_interval_tree_node_t *node, int black_depth,
                                            int current_black_depth)
{
    if (node == &tree->nill) {
        return true;
    }

    if (OPAL_INTERVAL_TREE_COLOR_RED == node->color &&
        (OPAL_INTERVAL_TREE_COLOR_BLACK != node->left->color ||
         OPAL_INTERVAL_TREE_COLOR_BLACK != node->right->color)) {
        fprintf (stderr, "Red node has a red child!\n");
        return false;
    }

    if (OPAL_INTERVAL_TREE_COLOR_BLACK == node->color) {
        current_black_depth++;
    }

    if (node->left == &tree->nill && node->right == &tree->nill) {
        if (black_depth != current_black_depth) {
            fprintf (stderr, "Found leaf with unexpected black depth: %d, expected: %d\n", current_black_depth, black_depth);
            return false;
        }

        return true;
    }

    return opal_interval_tree_verify_node (tree, node->left, black_depth, current_black_depth) ||
        opal_interval_tree_verify_node (tree, node->right, black_depth, current_black_depth);
}

static int opal_interval_tree_black_depth (opal_interval_tree_t *tree, opal_interval_tree_node_t *node, int depth)
{
    if (node == &tree->nill) {
        return depth;
    }

    /* suffices to always go left */
    if (OPAL_INTERVAL_TREE_COLOR_BLACK == node->color) {
        depth++;
    }

    return opal_interval_tree_black_depth (tree, node->left, depth);
}

bool opal_interval_tree_verify (opal_interval_tree_t *tree)
{
    int black_depth;

    if (OPAL_INTERVAL_TREE_COLOR_BLACK != tree->root.left->color) {
        fprintf (stderr, "Root node of tree is NOT black!\n");
        return false;
    }

    if (OPAL_INTERVAL_TREE_COLOR_BLACK != tree->nill.color) {
        fprintf (stderr, "Leaf node color is NOT black!\n");
        return false;
    }

    black_depth = opal_interval_tree_black_depth (tree, tree->root.left, 0);

    return opal_interval_tree_verify_node (tree, tree->root.left, black_depth, 0);
}

static void opal_interval_tree_dump_node (opal_interval_tree_t *tree, opal_interval_tree_node_t *node, int black_rank, FILE *fh)
{
    const char *color = (node->color == OPAL_INTERVAL_TREE_COLOR_BLACK) ? "black" : "red";
    uintptr_t left = (uintptr_t) node->left, right = (uintptr_t) node->right;
    opal_interval_tree_node_t *nill = &tree->nill;

    if (node->color == OPAL_INTERVAL_TREE_COLOR_BLACK) {
	++black_rank;
    }

    if (nill == node) {
        return;
    }

    /* print out nill nodes if any */
    if ((uintptr_t) nill == left) {
        left = (uintptr_t) node | 0x1;
        fprintf (fh, "  Node%lx [color=black,label=nill];\n\n", left);
    } else {
        left = (uintptr_t) node->left;
    }

    if ((uintptr_t) nill == right) {
        right = (uintptr_t) node | 0x2;
        fprintf (fh, "  Node%lx [color=black,label=nill];\n\n", right);
    } else {
        right = (uintptr_t) node->right;
    }

    /* print out this node and its edges */
    fprintf (fh, "  Node%lx [color=%s,shape=box,label=\"[0x%" PRIx64 ",0x%" PRIx64 "]\\nmax=0x%" PRIx64
             "\\ndata=0x%lx\\nblack rank=%d\"];\n", (uintptr_t) node, color, node->low, node->high, node->max,
             (uintptr_t) node->data, black_rank);
    fprintf (fh, "  Node%lx -> Node%lx;\n", (uintptr_t) node, left);
    fprintf (fh, "  Node%lx -> Node%lx;\n\n", (uintptr_t) node, right);
    if (node != tree->root.left) {
        fprintf (fh, "  Node%lx -> Node%lx;\n\n", (uintptr_t) node, (uintptr_t) node->parent);
    }
    opal_interval_tree_dump_node (tree, node->left, black_rank, fh);
    opal_interval_tree_dump_node (tree, node->right, black_rank, fh);
}

int opal_interval_tree_dump (opal_interval_tree_t *tree, const char *path)
{
    FILE *fh;

    fh = fopen (path, "w");
    if (NULL == fh) {
        return OPAL_ERR_BAD_PARAM;
    }

    fprintf (fh, "digraph {\n");
    fprintf (fh, "  graph [ordering=\"out\"];");
    opal_interval_tree_dump_node (tree, tree->root.left, 0, fh);
    fprintf (fh, "}\n");

    fclose (fh);

    return OPAL_SUCCESS;
}
