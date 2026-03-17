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
 *                         All rights reserved.5A
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2007      Voltaire. All rights reserved.
 * Copyright (c) 2010      IBM Corporation.  All rights reserved.
 * Copyright (c) 2012-2015 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include "opal/mca/mca.h"
#include "opal/util/show_help.h"
#include "opal/util/proc.h"

#include "opal/class/opal_rb_tree.h"
#include "mpool_base_tree.h"


static int num_leaks = 0;
static int max_mem_leaks = -1;
static char *leak_msg = NULL;

static int condition(void *value);
static void action(void *key, void *value);

OBJ_CLASS_INSTANCE(mca_mpool_base_tree_item_t, opal_free_list_item_t, NULL, NULL);

/*
 * use globals for the tree and the tree_item free list..
 */
opal_rb_tree_t mca_mpool_base_tree = {{0}};
opal_free_list_t mca_mpool_base_tree_item_free_list = {{{0}}};
static opal_mutex_t tree_lock;

/*
 *  simple minded compare function...
 */
int mca_mpool_base_tree_node_compare(void * key1, void * key2)
{
    if(key1 < key2)
    {
        return -1;
    }
    else if(key1 > key2)
    {
        return 1;
    }
    else
    {
        return 0;
    }
}

/*
 * initialize the rb tree
 */
int mca_mpool_base_tree_init(void) {
    int rc;
    OBJ_CONSTRUCT(&mca_mpool_base_tree, opal_rb_tree_t);
    OBJ_CONSTRUCT(&mca_mpool_base_tree_item_free_list, opal_free_list_t);
    OBJ_CONSTRUCT(&tree_lock, opal_mutex_t);
    rc = opal_free_list_init (&mca_mpool_base_tree_item_free_list,
            sizeof(mca_mpool_base_tree_item_t),
            opal_cache_line_size,
            OBJ_CLASS(mca_mpool_base_tree_item_t),
            0,opal_cache_line_size,
            0, -1 , 4, NULL, 0, NULL, NULL, NULL);
    if(OPAL_SUCCESS == rc) {
        rc = opal_rb_tree_init(&mca_mpool_base_tree, mca_mpool_base_tree_node_compare);
    }
    return rc;
}

/*
 *
 */
int mca_mpool_base_tree_fini(void)
{
    OBJ_DESTRUCT(&mca_mpool_base_tree);
    OBJ_DESTRUCT(&mca_mpool_base_tree_item_free_list);
    OBJ_DESTRUCT(&tree_lock);
    return OPAL_SUCCESS;
}

/*
 * insert an item in the rb tree
 */
int mca_mpool_base_tree_insert(mca_mpool_base_tree_item_t* item) {
    int rc;

    OPAL_THREAD_LOCK(&tree_lock);
    rc = opal_rb_tree_insert(&mca_mpool_base_tree, item->key, item);
    OPAL_THREAD_UNLOCK(&tree_lock);

    return rc;
}

/*
 * remove an item from the rb tree
 * Does not put the item back onto the free list. That
 * must be done separately by calling mca_mpool_base_tree_item_put.
 * This allows a caller to remove an item from the tree
 * before safely cleaning up the item and only then returning it
 * to the free list. If the item is returned to the free list too soon
 * race conditions can occur
 *
 */
int mca_mpool_base_tree_delete(mca_mpool_base_tree_item_t* item) {
    int rc;

    OPAL_THREAD_LOCK(&tree_lock);
    rc = opal_rb_tree_delete(&mca_mpool_base_tree, item->key);
    OPAL_THREAD_UNLOCK(&tree_lock);

    return rc;
}

/**
 *  find the item in the rb tree
 */
mca_mpool_base_tree_item_t* mca_mpool_base_tree_find(void* base) {
    mca_mpool_base_tree_item_t* item;

    OPAL_THREAD_LOCK(&tree_lock);
    item = (mca_mpool_base_tree_item_t*)opal_rb_tree_find(&mca_mpool_base_tree,
            base);
    OPAL_THREAD_UNLOCK(&tree_lock);

    return item;
}

/*
 * get a tree item from the free list
 */
mca_mpool_base_tree_item_t* mca_mpool_base_tree_item_get(void) {
    return (mca_mpool_base_tree_item_t *)
        opal_free_list_get (&mca_mpool_base_tree_item_free_list);
}

/*
 * put an item back into the free list
 */
void mca_mpool_base_tree_item_put(mca_mpool_base_tree_item_t* item) {
    opal_free_list_return (&mca_mpool_base_tree_item_free_list,
                           &item->super);
}


/*
 * Print a show_help kind of message for an items still left in the
 * tree
 */
void mca_mpool_base_tree_print(int show_up_to_mem_leaks)
{
    /* If they asked to show 0 leaks, then don't show anything.  */
    if (0 == show_up_to_mem_leaks) {
        return;
    }

    num_leaks = 0;
    max_mem_leaks = show_up_to_mem_leaks;
    opal_rb_tree_traverse(&mca_mpool_base_tree, condition, action);
    if (0 == num_leaks) {
        return;
    }

    if (num_leaks <= show_up_to_mem_leaks ||
        show_up_to_mem_leaks < 0) {
        opal_show_help("help-mpool-base.txt", "all mem leaks",
                       true, OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),
                       opal_proc_local_get()->proc_hostname,
                       getpid(), leak_msg);
    } else {
        int i = num_leaks - show_up_to_mem_leaks;
        opal_show_help("help-mpool-base.txt", "some mem leaks",
                       true, OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),
                       opal_proc_local_get()->proc_hostname,
                       getpid(), leak_msg, i,
                       (i > 1) ? "s were" : " was",
                       (i > 1) ? "are" : "is");
    }
    free(leak_msg);
    leak_msg = NULL;
}


/* Condition function for rb traversal */
static int condition(void *value)
{
    return 1;
}


/* Action function for rb traversal */
static void action(void *key, void *value)
{
    char *tmp;
    mca_mpool_base_tree_item_t *item = (mca_mpool_base_tree_item_t *) value;

    if( (++num_leaks <= max_mem_leaks) || (max_mem_leaks < 0) ) {

        /* We know that we're supposed to make the first one; check on
           successive items if we're supposed to catenate more
           notices. */
        if (NULL == leak_msg) {
            asprintf(&leak_msg, "    %lu bytes at address 0x%lx",
                     (unsigned long) item->num_bytes,
                     (unsigned long) key);
        } else {
            asprintf(&tmp, "%s\n    %lu bytes at address 0x%lx",
                     leak_msg, (unsigned long) item->num_bytes,
                     (unsigned long) key);
            free(leak_msg);
            leak_msg = tmp;
        }
    }
}
