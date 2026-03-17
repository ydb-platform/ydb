/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2014 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2007 High Performance Computing Center Stuttgart,
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

#include "opal_config.h"

#include "opal/class/opal_list.h"
#include "opal/constants.h"

/*
 *  List classes
 */

static void opal_list_item_construct(opal_list_item_t*);
static void opal_list_item_destruct(opal_list_item_t*);

OBJ_CLASS_INSTANCE(
    opal_list_item_t,
    opal_object_t,
    opal_list_item_construct,
    opal_list_item_destruct
);

static void opal_list_construct(opal_list_t*);
static void opal_list_destruct(opal_list_t*);

OBJ_CLASS_INSTANCE(
    opal_list_t,
    opal_object_t,
    opal_list_construct,
    opal_list_destruct
);


/*
 *
 *      opal_list_link_item_t interface
 *
 */

static void opal_list_item_construct(opal_list_item_t *item)
{
    item->opal_list_next = item->opal_list_prev = NULL;
    item->item_free = 1;
#if OPAL_ENABLE_DEBUG
    item->opal_list_item_refcount = 0;
    item->opal_list_item_belong_to = NULL;
#endif
}

static void opal_list_item_destruct(opal_list_item_t *item)
{
#if OPAL_ENABLE_DEBUG
    assert( 0 == item->opal_list_item_refcount );
    assert( NULL == item->opal_list_item_belong_to );
#endif  /* OPAL_ENABLE_DEBUG */
}


/*
 *
 *      opal_list_list_t interface
 *
 */

static void opal_list_construct(opal_list_t *list)
{
#if OPAL_ENABLE_DEBUG
    /* These refcounts should never be used in assertions because they
       should never be removed from this list, added to another list,
       etc.  So set them to sentinel values. */

    OBJ_CONSTRUCT( &(list->opal_list_sentinel), opal_list_item_t );
    list->opal_list_sentinel.opal_list_item_refcount  = 1;
    list->opal_list_sentinel.opal_list_item_belong_to = list;
#endif

    list->opal_list_sentinel.opal_list_next = &list->opal_list_sentinel;
    list->opal_list_sentinel.opal_list_prev = &list->opal_list_sentinel;
    list->opal_list_length = 0;
}


/*
 * Reset all the pointers to be NULL -- do not actually destroy
 * anything.
 */
static void opal_list_destruct(opal_list_t *list)
{
    opal_list_construct(list);
}


/*
 * Insert an item at a specific place in a list
 */
bool opal_list_insert(opal_list_t *list, opal_list_item_t *item, long long idx)
{
    /* Adds item to list at index and retains item. */
    int     i;
    volatile opal_list_item_t *ptr, *next;

    if ( idx >= (long long)list->opal_list_length ) {
        return false;
    }

    if ( 0 == idx )
    {
        opal_list_prepend(list, item);
    } else {
#if OPAL_ENABLE_DEBUG
        /* Spot check: ensure that this item is previously on no
           lists */

        assert(0 == item->opal_list_item_refcount);
#endif
        /* pointer to element 0 */
        ptr = list->opal_list_sentinel.opal_list_next;
        for ( i = 0; i < idx-1; i++ )
            ptr = ptr->opal_list_next;

        next = ptr->opal_list_next;
        item->opal_list_next = next;
        item->opal_list_prev = ptr;
        next->opal_list_prev = item;
        ptr->opal_list_next = item;

#if OPAL_ENABLE_DEBUG
        /* Spot check: ensure this item is only on the list that we
           just insertted it into */

        opal_atomic_add ( &(item->opal_list_item_refcount), 1 );
        assert(1 == item->opal_list_item_refcount);
        item->opal_list_item_belong_to = list;
#endif
    }

    list->opal_list_length++;
    return true;
}


static
void
opal_list_transfer(opal_list_item_t *pos, opal_list_item_t *begin,
                   opal_list_item_t *end)
{
    volatile opal_list_item_t *tmp;

    if (pos != end) {
        /* remove [begin, end) */
        end->opal_list_prev->opal_list_next = pos;
        begin->opal_list_prev->opal_list_next = end;
        pos->opal_list_prev->opal_list_next = begin;

        /* splice into new position before pos */
        tmp = pos->opal_list_prev;
        pos->opal_list_prev = end->opal_list_prev;
        end->opal_list_prev = begin->opal_list_prev;
        begin->opal_list_prev = tmp;
#if OPAL_ENABLE_DEBUG
        {
            volatile opal_list_item_t* item = begin;
            while( pos != item ) {
                item->opal_list_item_belong_to = pos->opal_list_item_belong_to;
                item = item->opal_list_next;
                assert(NULL != item);
            }
        }
#endif  /* OPAL_ENABLE_DEBUG */
    }
}


void
opal_list_join(opal_list_t *thislist, opal_list_item_t *pos,
               opal_list_t *xlist)
{
    if (0 != opal_list_get_size(xlist)) {
        opal_list_transfer(pos, opal_list_get_first(xlist),
                           opal_list_get_end(xlist));

        /* fix the sizes */
        thislist->opal_list_length += xlist->opal_list_length;
        xlist->opal_list_length = 0;
    }
}


void
opal_list_splice(opal_list_t *thislist, opal_list_item_t *pos,
                 opal_list_t *xlist, opal_list_item_t *first,
                 opal_list_item_t *last)
{
    size_t change = 0;
    opal_list_item_t *tmp;

    if (first != last) {
        /* figure out how many things we are going to move (have to do
         * first, since last might be end and then we wouldn't be able
         * to run the loop)
         */
        for (tmp = first ; tmp != last ; tmp = opal_list_get_next(tmp)) {
            change++;
        }

        opal_list_transfer(pos, first, last);

        /* fix the sizes */
        thislist->opal_list_length += change;
        xlist->opal_list_length -= change;
    }
}


int opal_list_sort(opal_list_t* list, opal_list_item_compare_fn_t compare)
{
    opal_list_item_t* item;
    opal_list_item_t** items;
    size_t i, index=0;

    if (0 == list->opal_list_length) {
        return OPAL_SUCCESS;
    }
    items = (opal_list_item_t**)malloc(sizeof(opal_list_item_t*) *
                                       list->opal_list_length);

    if (NULL == items) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    while(NULL != (item = opal_list_remove_first(list))) {
        items[index++] = item;
    }

    qsort(items, index, sizeof(opal_list_item_t*),
          (int(*)(const void*,const void*))compare);
    for (i=0; i<index; i++) {
        opal_list_append(list,items[i]);
    }
    free(items);
    return OPAL_SUCCESS;
}
