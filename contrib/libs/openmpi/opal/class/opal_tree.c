/*
 * Copyright (c) 2011      Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2011      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2012      The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2013      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include "opal/class/opal_tree.h"
#include "opal/constants.h"

/*
 *  List classes
 */

static void opal_tree_item_construct(opal_tree_item_t*);
static void opal_tree_item_destruct(opal_tree_item_t*);

OBJ_CLASS_INSTANCE(
                   opal_tree_item_t,
                   opal_object_t,
                   opal_tree_item_construct,
                   opal_tree_item_destruct
                   );

static void opal_tree_construct(opal_tree_t*);
static void opal_tree_destruct(opal_tree_t*);

OBJ_CLASS_INSTANCE(
                   opal_tree_t,
                   opal_object_t,
                   opal_tree_construct,
                   opal_tree_destruct
                   );


/*
 *
 *      opal_tree_item_t interface
 *
 */

static void opal_tree_item_construct(opal_tree_item_t *item)
{
    item->opal_tree_parent = NULL;
    item->opal_tree_num_ancestors = 0;
    item->opal_tree_sibling_rank = 0xdeadbeef;
    item->opal_tree_next_sibling = item->opal_tree_prev_sibling = NULL;
    item->opal_tree_num_children = 0;
    item->opal_tree_first_child = item->opal_tree_last_child = NULL;
#if OPAL_ENABLE_DEBUG
    item->opal_tree_item_refcount = 0;
    item->opal_tree_item_belong_to = NULL;
#endif
}

static void opal_tree_item_destruct(opal_tree_item_t *item)
{
#if OPAL_ENABLE_DEBUG
    assert( 0 == item->opal_tree_item_refcount );
    assert( NULL == item->opal_tree_item_belong_to );
#endif  /* OPAL_ENABLE_DEBUG */
}


/*
 *
 *      opal_tree_t interface
 *
 */

static void opal_tree_construct(opal_tree_t *tree)
{
    OBJ_CONSTRUCT( &(tree->opal_tree_sentinel), opal_tree_item_t );

#if OPAL_ENABLE_DEBUG
    /* These refcounts should never be used in assertions because they
       should never be removed from this list, added to another list,
       etc.  So set them to sentinel values. */

    tree->opal_tree_sentinel.opal_tree_item_refcount  = 1;
    tree->opal_tree_sentinel.opal_tree_item_belong_to = tree;
#endif
    tree->opal_tree_sentinel.opal_tree_container = tree;
    tree->opal_tree_sentinel.opal_tree_parent = &tree->opal_tree_sentinel;
    tree->opal_tree_sentinel.opal_tree_num_ancestors = -1;

    tree->opal_tree_sentinel.opal_tree_next_sibling =
        &tree->opal_tree_sentinel;
    tree->opal_tree_sentinel.opal_tree_prev_sibling =
        &tree->opal_tree_sentinel;

    tree->opal_tree_sentinel.opal_tree_first_child = &tree->opal_tree_sentinel;
    tree->opal_tree_sentinel.opal_tree_last_child = &tree->opal_tree_sentinel;

    tree->opal_tree_num_items = 0;
    tree->comp = NULL;
    tree->serialize = NULL;
    tree->deserialize = NULL;
    tree->get_key = NULL;
}

/*
 * Reset all the pointers to be NULL -- do not actually destroy
 * anything.
 */
static void opal_tree_destruct(opal_tree_t *tree)
{
    opal_tree_construct(tree);
}

/*
 * initialize tree container
 */
void opal_tree_init(opal_tree_t *tree, opal_tree_comp_fn_t comp,
                    opal_tree_item_serialize_fn_t serialize,
                    opal_tree_item_deserialize_fn_t deserialize,
                    opal_tree_get_key_fn_t get_key)
{
    tree->comp = comp;
    tree->serialize = serialize;
    tree->deserialize = deserialize;
    tree->get_key = get_key;
}

/*
 * count all the descendants from our level and below
 */
static int count_descendants(opal_tree_item_t* item)
{
    int current_count = 0;

    /* loop over all siblings for descendants to count */
    while (item) {
        current_count += count_descendants(opal_tree_get_first_child(item));
        current_count++; /* count ourselves */
        item = opal_tree_get_next_sibling(item);
    }
    return(current_count);
}

/*
 * get size of tree
 */
size_t opal_tree_get_size(opal_tree_t* tree)
{
#if OPAL_ENABLE_DEBUG
    /* not sure if we really want this running in devel, as it does
     * slow things down.  Wanted for development of splice / join to
     * make sure length was reset properly
     */
    size_t check_len = 0;
    opal_tree_item_t *root;

    if (!opal_tree_is_empty(tree)) {
        /* tree has entries so count up items */
        root = opal_tree_get_root(tree);
        check_len = count_descendants(root);
    }

    if (check_len != tree->opal_tree_num_items) {
        fprintf(stderr," Error :: opal_tree_get_size - opal_tree_num_items does not match actual tree length\n");
        fflush(stderr);
        abort();
    }
#endif

    return tree->opal_tree_num_items;
}

/*
 * add item to parent's child list
 */
void opal_tree_add_child(opal_tree_item_t *parent_item,
                         opal_tree_item_t *new_item)
{
#if OPAL_ENABLE_DEBUG
    /* Spot check: ensure that this item is previously on no lists */

    assert(0 == new_item->opal_tree_item_refcount);
    assert( NULL == new_item->opal_tree_item_belong_to );
#endif

    new_item->opal_tree_parent = parent_item;
    new_item->opal_tree_num_ancestors = parent_item->opal_tree_num_ancestors+1;
    if (parent_item->opal_tree_num_children) {
        /* append item to end of children and sibling lists */
        new_item->opal_tree_prev_sibling = parent_item->opal_tree_last_child;
        parent_item->opal_tree_last_child->opal_tree_next_sibling = new_item;
    } else {
        /* no children existing on parent */
        parent_item->opal_tree_first_child = new_item;
    }
    parent_item->opal_tree_last_child = new_item;
    parent_item->opal_tree_num_children++;
    new_item->opal_tree_container = parent_item->opal_tree_container;
    new_item->opal_tree_container->opal_tree_num_items++;

#if OPAL_ENABLE_DEBUG
    /* Spot check: ensure this item is only on the list that we just
       appended it to */

    OPAL_THREAD_ADD_FETCH32( &(new_item->opal_tree_item_refcount), 1 );
    assert(1 == new_item->opal_tree_item_refcount);
    new_item->opal_tree_item_belong_to = new_item->opal_tree_container;
#endif
}

/*
 * check to see if item is in tree
 */
#if OPAL_ENABLE_DEBUG
static bool item_in_tree(opal_tree_item_t *item, opal_tree_item_t *search_item)
{
    bool result = false;
    opal_tree_item_t *first_child;

    while (!result && item) {
        /* check for item match */
        result = (item == search_item) ? true : false;
        if (!result && (first_child = opal_tree_get_first_child(item))) {
            /* search descendants for match */
            result = item_in_tree(first_child, search_item);
        }
        if (!result) {
            /* didn't find match at our node or descending so check sibling */
            item = opal_tree_get_next_sibling(item);
        }
    }
    return(result);
}
#endif  /* OPAL_ENABLE_DEBUG */

/*
 * remove item and all items below it from tree and return it to the caller
 */
opal_tree_item_t *opal_tree_remove_subtree(opal_tree_item_t *item)
{
    opal_tree_item_t *parent_item = NULL;

#if OPAL_ENABLE_DEBUG
    /* validate that item does exist on tree */
    if (NULL != item->opal_tree_container) {
        /* we point to a container, check if we can find item in tree */
        if (!item_in_tree(opal_tree_get_root(item->opal_tree_container), item)) {
            return(NULL);
        }
    } else {
        return (NULL);
    }
#endif

    parent_item = item->opal_tree_parent;

    /* remove from parent */
    /* If item is the only child, set _first_child and _last_child to
       be item's _first_child and _last_child */
    if (parent_item->opal_tree_first_child == item &&
        parent_item->opal_tree_last_child == item) {
        parent_item->opal_tree_first_child = item->opal_tree_first_child;
        parent_item->opal_tree_last_child = item->opal_tree_last_child;
    } else {
        /* Otherwise, there are multiple children of this parent.  If
           this item is the first or last child of this parent, then
           ensure that the parent gets a valid first / last child:
           - If I have children, then my first/last child
           - If I have no children, then my immediate sibling */
        if (item->opal_tree_parent->opal_tree_first_child == item) {
            if (item->opal_tree_num_children > 0) {
                parent_item->opal_tree_first_child =
                    item->opal_tree_next_sibling;
            } else {
                parent_item->opal_tree_first_child =
                    opal_tree_get_next_sibling(item);
            }
        } else if (parent_item->opal_tree_last_child == item) {
            if (item->opal_tree_num_children > 0) {
                parent_item->opal_tree_last_child =
                    item->opal_tree_last_child;
            } else {
                parent_item->opal_tree_last_child =
                    opal_tree_get_prev_sibling(item);
            }
        }
    }
    item->opal_tree_parent->opal_tree_num_children--;

    /* remove from sibling pointers */
    if (NULL != item->opal_tree_prev_sibling) {
        item->opal_tree_prev_sibling->opal_tree_next_sibling=
            item->opal_tree_next_sibling;
    }
    if (NULL != item->opal_tree_next_sibling) {
        item->opal_tree_next_sibling->opal_tree_prev_sibling=
            item->opal_tree_prev_sibling;
    }
    item->opal_tree_prev_sibling = NULL;
    item->opal_tree_next_sibling = NULL;

    /* adjust items relating to container */
    item->opal_tree_container->opal_tree_num_items -= count_descendants(item);
    item->opal_tree_container = NULL;

    return(item);
}

int opal_tree_remove_item(opal_tree_t *tree,
                          opal_tree_item_t *item)
{
    opal_tree_item_t *parent_item = NULL, *child = NULL;

    parent_item = (opal_tree_item_t*)item->opal_tree_parent;

    /*
     * Point each of my children to my parent
     */
    for(child  = opal_tree_get_first_child(item);
        child != NULL;
        child  = opal_tree_get_next_sibling(child)) {
        child->opal_tree_parent = parent_item;
        child->opal_tree_num_ancestors--;

        parent_item->opal_tree_num_children++;
    }

    /*
     * My first child points to my 'prev' sibling
     */
    child  = opal_tree_get_first_child(item);
    if( NULL != child ) {
        child->opal_tree_prev_sibling = item->opal_tree_prev_sibling;
    }
    if( NULL != item->opal_tree_prev_sibling ) {
        (item->opal_tree_prev_sibling)->opal_tree_next_sibling = child;
    }

    child  = opal_tree_get_last_child(item);
    if( NULL != child ) {
        child->opal_tree_next_sibling = item->opal_tree_next_sibling;
    }
    if( NULL != item->opal_tree_next_sibling ) {
        (item->opal_tree_next_sibling)->opal_tree_prev_sibling = child;
    }

    /*
     * Remove me from my parent.  If I was the only child, then make
     * the first child be my first child, and make the last child be
     * my last child.
     */
    if( parent_item->opal_tree_first_child == item &&
        parent_item->opal_tree_last_child == item ) {
        parent_item->opal_tree_first_child = opal_tree_get_first_child(item);
        parent_item->opal_tree_last_child = opal_tree_get_last_child(item);
    } else {
        /* There were multiple children.  If I was the first or last,
           then ensure the parent gets a valid first or last child:
           - If I have children, then my first/last
           - If I have no childen, then my immediate sibling */
        if (parent_item->opal_tree_first_child == item) {
            if (item->opal_tree_num_children > 0) {
                parent_item->opal_tree_first_child =
                    item->opal_tree_first_child;
            } else {
                parent_item->opal_tree_first_child =
                    opal_tree_get_next_sibling(item);
            }
        } else if (parent_item->opal_tree_last_child == item) {
            if (item->opal_tree_num_children > 0) {
                parent_item->opal_tree_last_child =
                    item->opal_tree_last_child;
            } else {
                parent_item->opal_tree_last_child =
                    opal_tree_get_prev_sibling(item);
            }
        }
    }
    parent_item->opal_tree_num_children--;

    return OPAL_SUCCESS;
}

/* delimeter characters that mark items in a serialized stream */
static char *start_lvl = "[";
static char *end_lvl = "]";
static char *end_stream = "E";

/*
 * add item to opal buffer that represents all items of a sub-tree from the
 * item passed in on down.  We exit out of converting tree items once we've
 * done the last child of the tree_item and we are at depth 1.
 */
static int add_tree_item2buf(opal_tree_item_t *tree_item,
                             opal_buffer_t *buf,
                             opal_tree_item_serialize_fn_t fn,
                             int depth
                             )
{
    opal_tree_item_t *first_child;
    int rc;

    do {
        /* add start delim to buffer */
        if (OPAL_SUCCESS !=
            (rc = opal_dss.pack(buf, &start_lvl, 1, OPAL_STRING))){
            return(rc);
        }
        /* add item to opal buffer from class creator */
        fn(tree_item, buf);

        if ((first_child = opal_tree_get_first_child(tree_item))) {
            /* add items for our children */
            if (OPAL_SUCCESS !=
                (rc = add_tree_item2buf(first_child, buf, fn, depth+1))){
                return(rc);
            }
            if (OPAL_SUCCESS !=
                (rc = opal_dss.pack(buf, &end_lvl, 1, OPAL_STRING))){
                return(rc);
            }
        } else {
            /* end item entry */
            if (OPAL_SUCCESS !=
                (rc = opal_dss.pack(buf, &end_lvl, 1, OPAL_STRING))){
                return(rc);
            }
        }

        /* advance to next sibling, if none we'll drop out of
         * loop and return to our parent
         */
        tree_item = opal_tree_get_next_sibling(tree_item);
    } while (tree_item && 1 < depth);

    return(OPAL_SUCCESS);
}

/*
 * serialize tree data
 */
int opal_tree_serialize(opal_tree_item_t *start_item, opal_buffer_t *buffer)
{
    int rc;

    if (OPAL_SUCCESS !=
        (rc = add_tree_item2buf(start_item, buffer,
                                start_item->opal_tree_container->serialize,
                                1))){
        return(rc);
    }
    if (OPAL_SUCCESS !=
        (rc = opal_dss.pack(buffer, &end_stream, 1, OPAL_STRING))){
        return(rc);
    }
    return(OPAL_SUCCESS);
}

static int deserialize_add_tree_item(opal_buffer_t *data,
                                     opal_tree_item_t *parent_item,
                                     opal_tree_item_deserialize_fn_t deserialize,
                                     char **curr_delim,
                                     int depth)
{
    int idx = 1, rc;
    opal_tree_item_t *new_item = NULL;
    int level = 0; /* 0 - one up 1 - curr, 2 - one down */

    if (!*curr_delim) {
        if (OPAL_SUCCESS !=
            (rc = opal_dss.unpack(data, curr_delim, &idx, OPAL_STRING))) {
            return(rc);
        }
    }
    while(*curr_delim[0] != end_stream[0]) {
        if (*curr_delim[0] == start_lvl[0]) {
            level++;
        } else {
            level--;
        }

        switch (level) {
        case 0:
            if (1 < depth) {
                /* done with this level go up one level */
                return(OPAL_SUCCESS);
            }
            break;
        case 1:
            /* add found child at this level */
            deserialize(data, &new_item);
            opal_tree_add_child(parent_item, new_item);
            break;
        case 2:
            /* need to add child one level down */
            deserialize_add_tree_item(data, new_item, deserialize, curr_delim,
                                      depth+1);
            level--;
            break;
        }
        if (OPAL_SUCCESS !=
            (rc = opal_dss.unpack(data, curr_delim, &idx, OPAL_STRING))) {
            return(rc);
        }
    }
    return(OPAL_SUCCESS);
}

/*
 * deserialize tree data
 */
int opal_tree_deserialize(opal_buffer_t *serialized_data,
                          opal_tree_item_t *start_item)
{
    char * null = NULL;
    deserialize_add_tree_item(serialized_data,
                              start_item,
                              start_item->opal_tree_container->deserialize,
                              &null,
                              1);
    return OPAL_SUCCESS;
}

void * opal_tree_get_key(opal_tree_t *tree, opal_tree_item_t *item)
{
    return tree->get_key(item);
}

int opal_tree_dup(opal_tree_t *from, opal_tree_t *to)
{
    int ret;
    opal_buffer_t *buffer = NULL;

    opal_tree_init(to,
                   from->comp,
                   from->serialize,
                   from->deserialize,
                   from->get_key);

    buffer = OBJ_NEW(opal_buffer_t);

    opal_tree_serialize(opal_tree_get_root(from), buffer);
    ret = opal_tree_deserialize(buffer, opal_tree_get_root(to));

    OBJ_RELEASE(buffer);
    return ret;
}

int opal_tree_copy_subtree(opal_tree_t *from_tree, opal_tree_item_t *from_item,
                           opal_tree_t *to_tree,   opal_tree_item_t *to_parent)
{
    int ret;
    opal_buffer_t *buffer = NULL;

    buffer = OBJ_NEW(opal_buffer_t);

    opal_tree_serialize(from_item, buffer);
    ret = opal_tree_deserialize(buffer, to_parent);

    OBJ_RELEASE(buffer);
    return ret;
}

opal_tree_item_t *opal_tree_dup_item(opal_tree_t *base, opal_tree_item_t *from)
{
    opal_buffer_t *buffer = NULL;
    opal_tree_item_t *new_item = NULL;

    buffer = OBJ_NEW(opal_buffer_t);

    opal_tree_serialize(from, buffer);

    new_item = OBJ_NEW(opal_tree_item_t);
    opal_tree_deserialize(buffer, new_item);

    OBJ_RELEASE(buffer);
    return new_item;
}

int opal_tree_num_children(opal_tree_item_t *parent)
{
    opal_tree_item_t *child = NULL;
    int i = 0;

    for(child  = opal_tree_get_first_child(parent);
        child != NULL;
        child  = opal_tree_get_next_sibling(child) ) {
        ++i;
    }

    return i;
}

static int opal_tree_compare_subtrees(opal_tree_t *tree_left, opal_tree_t *tree_right,
                                      opal_tree_item_t *left, opal_tree_item_t *right)
{
    int ret;
    opal_tree_item_t *left_child = NULL, *right_child = NULL;

    /* Basecase */
    if( NULL == left && NULL == right ) {
        return 0; /* Match */
    }

    /* Compare: Depth */
    if( NULL == left && NULL != right ) {
        return -1;
    }
    else if( NULL != left && NULL == right ) {
        return 1;
    }

    /* Compare: Keys */
    if( 0 != tree_left->comp(right, opal_tree_get_key(tree_left, left)) ) {
        return -2;
    }

    /* Compare: Number of children */
    if( opal_tree_num_children(left) != opal_tree_num_children(right) ) {
        return 2;
    }

    /* Recursively compare all children */
    for(left_child  = opal_tree_get_first_child(left),        right_child  = opal_tree_get_first_child(right);
        left_child != NULL &&                                 right_child != NULL;
        left_child  = opal_tree_get_next_sibling(left_child), right_child  = opal_tree_get_next_sibling(right_child) ) {
        /* On first difference, return the result */
        if( 0 != (ret = opal_tree_compare_subtrees(tree_left, tree_right, left_child, right_child)) ) {
            return ret;
        }
    }

    return 0;
}

int opal_tree_compare(opal_tree_t *left, opal_tree_t *right)
{
    return opal_tree_compare_subtrees(left, right, opal_tree_get_root(left), opal_tree_get_root(right));
}

/*
 * search myself, descendants and siblings for item matching key
 */
static opal_tree_item_t *find_in_descendants(opal_tree_item_t* item, void *key)
{
    opal_tree_item_t *result = NULL, *first_child;

    while (!result && item) {
        /* check for item match */
        result = (item->opal_tree_container->comp(item, key) == 0) ?
            item : NULL;
        if (!result && (first_child = opal_tree_get_first_child(item))) {
            /* search descendants for match */
            result = find_in_descendants(first_child, key);
        }
        if (!result) {
            /* didn't find match at our node or descending so check sibling */
            item = opal_tree_get_next_sibling(item);
        }
    }
    return(result);
}

/*
 * return next tree item that matches key
 */
opal_tree_item_t *opal_tree_find_with(opal_tree_item_t *item, void *key)
{
    opal_tree_item_t *curr_item = item, *result = NULL;

    if (!opal_tree_is_empty(item->opal_tree_container)) {
        /* check my descendant for a match */
        result = find_in_descendants(opal_tree_get_first_child(item), key);

        if (!result) {
            /* check my siblings for match */
            if (NULL != (curr_item = opal_tree_get_next_sibling(curr_item))) {
                result = find_in_descendants(curr_item, key);
            }
        }

        /* check my ancestors (uncles) for match */
        curr_item = item;
        while (!result && curr_item && curr_item->opal_tree_num_ancestors > 0){
            curr_item = opal_tree_get_next_sibling(item->opal_tree_parent);
            while (NULL == curr_item &&
                   item->opal_tree_parent->opal_tree_num_ancestors > 0) {
                item = item->opal_tree_parent;
                curr_item = opal_tree_get_next_sibling(item->opal_tree_parent);
            }
            if (curr_item) {
                /* search ancestors descendants for match */
                result = find_in_descendants(curr_item, key);
            }
        }
    }

    return(result);
}
