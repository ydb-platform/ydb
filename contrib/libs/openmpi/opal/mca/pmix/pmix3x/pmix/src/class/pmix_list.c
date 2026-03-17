/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2007 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Voltaire All rights reserved.
 * Copyright (c) 2013-2015 Intel, Inc. All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>
#include "include/pmix_common.h"
#include "src/class/pmix_list.h"

/*
 *  List classes
 */

static void pmix_list_item_construct(pmix_list_item_t*);
static void pmix_list_item_destruct(pmix_list_item_t*);

PMIX_CLASS_INSTANCE(
    pmix_list_item_t,
    pmix_object_t,
    pmix_list_item_construct,
    pmix_list_item_destruct
);

static void pmix_list_construct(pmix_list_t*);
static void pmix_list_destruct(pmix_list_t*);

PMIX_CLASS_INSTANCE(
    pmix_list_t,
    pmix_object_t,
    pmix_list_construct,
    pmix_list_destruct
);


/*
 *
 *      pmix_list_link_item_t interface
 *
 */

static void pmix_list_item_construct(pmix_list_item_t *item)
{
    item->pmix_list_next = item->pmix_list_prev = NULL;
    item->item_free = 1;
#if PMIX_ENABLE_DEBUG
    item->pmix_list_item_refcount = 0;
    item->pmix_list_item_belong_to = NULL;
#endif
}

static void pmix_list_item_destruct(pmix_list_item_t *item)
{
#if PMIX_ENABLE_DEBUG
    assert( 0 == item->pmix_list_item_refcount );
    assert( NULL == item->pmix_list_item_belong_to );
#endif  /* PMIX_ENABLE_DEBUG */
}


/*
 *
 *      pmix_list_list_t interface
 *
 */

static void pmix_list_construct(pmix_list_t *list)
{
#if PMIX_ENABLE_DEBUG
    /* These refcounts should never be used in assertions because they
       should never be removed from this list, added to another list,
       etc.  So set them to sentinel values. */

    PMIX_CONSTRUCT( &(list->pmix_list_sentinel), pmix_list_item_t );
    list->pmix_list_sentinel.pmix_list_item_refcount  = 1;
    list->pmix_list_sentinel.pmix_list_item_belong_to = list;
#endif

    list->pmix_list_sentinel.pmix_list_next = &list->pmix_list_sentinel;
    list->pmix_list_sentinel.pmix_list_prev = &list->pmix_list_sentinel;
    list->pmix_list_length = 0;
}


/*
 * Reset all the pointers to be NULL -- do not actually destroy
 * anything.
 */
static void pmix_list_destruct(pmix_list_t *list)
{
    pmix_list_construct(list);
}


/*
 * Insert an item at a specific place in a list
 */
bool pmix_list_insert(pmix_list_t *list, pmix_list_item_t *item, long long idx)
{
    /* Adds item to list at index and retains item. */
    int     i;
    volatile pmix_list_item_t *ptr, *next;

    if ( idx >= (long long)list->pmix_list_length ) {
        return false;
    }

    if ( 0 == idx )
    {
        pmix_list_prepend(list, item);
    } else {
#if PMIX_ENABLE_DEBUG
        /* Spot check: ensure that this item is previously on no
           lists */

        assert(0 == item->pmix_list_item_refcount);
#endif
        /* pointer to element 0 */
        ptr = list->pmix_list_sentinel.pmix_list_next;
        for ( i = 0; i < idx-1; i++ )
            ptr = ptr->pmix_list_next;

        next = ptr->pmix_list_next;
        item->pmix_list_next = next;
        item->pmix_list_prev = ptr;
        next->pmix_list_prev = item;
        ptr->pmix_list_next = item;

#if PMIX_ENABLE_DEBUG
        /* Spot check: ensure this item is only on the list that we
           just insertted it into */

        item->pmix_list_item_refcount += 1;
        assert(1 == item->pmix_list_item_refcount);
        item->pmix_list_item_belong_to = list;
#endif
    }

    list->pmix_list_length++;
    return true;
}


static
void
pmix_list_transfer(pmix_list_item_t *pos, pmix_list_item_t *begin,
                   pmix_list_item_t *end)
{
    volatile pmix_list_item_t *tmp;

    if (pos != end) {
        /* remove [begin, end) */
        end->pmix_list_prev->pmix_list_next = pos;
        begin->pmix_list_prev->pmix_list_next = end;
        pos->pmix_list_prev->pmix_list_next = begin;

        /* splice into new position before pos */
        tmp = pos->pmix_list_prev;
        pos->pmix_list_prev = end->pmix_list_prev;
        end->pmix_list_prev = begin->pmix_list_prev;
        begin->pmix_list_prev = tmp;
#if PMIX_ENABLE_DEBUG
        {
            volatile pmix_list_item_t* item = begin;
            while( pos != item ) {
                item->pmix_list_item_belong_to = pos->pmix_list_item_belong_to;
                item = item->pmix_list_next;
                assert(NULL != item);
            }
        }
#endif  /* PMIX_ENABLE_DEBUG */
    }
}


void
pmix_list_join(pmix_list_t *thislist, pmix_list_item_t *pos,
               pmix_list_t *xlist)
{
    if (0 != pmix_list_get_size(xlist)) {
        pmix_list_transfer(pos, pmix_list_get_first(xlist),
                           pmix_list_get_end(xlist));

        /* fix the sizes */
        thislist->pmix_list_length += xlist->pmix_list_length;
        xlist->pmix_list_length = 0;
    }
}


void
pmix_list_splice(pmix_list_t *thislist, pmix_list_item_t *pos,
                 pmix_list_t *xlist, pmix_list_item_t *first,
                 pmix_list_item_t *last)
{
    size_t change = 0;
    pmix_list_item_t *tmp;

    if (first != last) {
        /* figure out how many things we are going to move (have to do
         * first, since last might be end and then we wouldn't be able
         * to run the loop)
         */
        for (tmp = first ; tmp != last ; tmp = pmix_list_get_next(tmp)) {
            change++;
        }

        pmix_list_transfer(pos, first, last);

        /* fix the sizes */
        thislist->pmix_list_length += change;
        xlist->pmix_list_length -= change;
    }
}


int pmix_list_sort(pmix_list_t* list, pmix_list_item_compare_fn_t compare)
{
    pmix_list_item_t* item;
    pmix_list_item_t** items;
    size_t i, index=0;

    if (0 == list->pmix_list_length) {
        return PMIX_SUCCESS;
    }
    items = (pmix_list_item_t**)malloc(sizeof(pmix_list_item_t*) *
                                       list->pmix_list_length);

    if (NULL == items) {
        return PMIX_ERR_OUT_OF_RESOURCE;
    }

    while(NULL != (item = pmix_list_remove_first(list))) {
        items[index++] = item;
    }

    qsort(items, index, sizeof(pmix_list_item_t*),
          (int(*)(const void*,const void*))compare);
    for (i=0; i<index; i++) {
        pmix_list_append(list,items[i]);
    }
    free(items);
    return PMIX_SUCCESS;
}
