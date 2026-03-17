/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Voltaire All rights reserved.
 * Copyright (c) 2010      IBM Corporation.  All rights reserved.
 * Copyright (c) 2014-2018 Los Alamos National Security, LLC. All rights
 *                         reseved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_FIFO_H_HAS_BEEN_INCLUDED
#define OPAL_FIFO_H_HAS_BEEN_INCLUDED

#include "opal_config.h"
#include "opal/class/opal_lifo.h"

#include "opal/sys/atomic.h"
#include "opal/threads/mutex.h"

BEGIN_C_DECLS

/* Atomic First In First Out lists. If we are in a multi-threaded environment then the
 * atomicity is insured via the compare-and-swap operation, if not we simply do a read
 * and/or a write.
 *
 * There is a trick. The ghost element at the end of the list. This ghost element has
 * the next pointer pointing to itself, therefore we cannot go past the end of the list.
 * With this approach we will never have a NULL element in the list, so we never have
 * to test for the NULL.
 */
struct opal_fifo_t {
    opal_object_t super;

    /** first element on the fifo */
    volatile opal_counted_pointer_t opal_fifo_head;
    /** last element on the fifo */
    volatile opal_counted_pointer_t opal_fifo_tail;

    /** list sentinel (always points to self) */
    opal_list_item_t opal_fifo_ghost;
};

typedef struct opal_fifo_t opal_fifo_t;

OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_fifo_t);

static inline opal_list_item_t *opal_fifo_head (opal_fifo_t* fifo)
{
    return (opal_list_item_t *) fifo->opal_fifo_head.data.item;
}

static inline opal_list_item_t *opal_fifo_tail (opal_fifo_t* fifo)
{
    return (opal_list_item_t *) fifo->opal_fifo_tail.data.item;
}

/* The ghost pointer will never change. The head will change via an atomic
 * compare-and-swap. On most architectures the reading of a pointer is an
 * atomic operation so we don't have to protect it.
 */
static inline bool opal_fifo_is_empty( opal_fifo_t* fifo )
{
    return opal_fifo_head (fifo) == &fifo->opal_fifo_ghost;
}

#if OPAL_HAVE_ATOMIC_COMPARE_EXCHANGE_128

/* Add one element to the FIFO. We will return the last head of the list
 * to allow the upper level to detect if this element is the first one in the
 * list (if the list was empty before this operation).
 */
static inline opal_list_item_t *opal_fifo_push_atomic (opal_fifo_t *fifo,
                                                       opal_list_item_t *item)
{
    opal_counted_pointer_t tail = {.value = fifo->opal_fifo_tail.value};
    const opal_list_item_t * const ghost = &fifo->opal_fifo_ghost;

    item->opal_list_next = (opal_list_item_t *) ghost;

    opal_atomic_wmb ();

    do {
        if (opal_update_counted_pointer (&fifo->opal_fifo_tail, &tail, item)) {
            break;
        }
    } while (1);

    opal_atomic_wmb ();

    if (ghost == tail.data.item) {
        /* update the head */
        opal_counted_pointer_t head = {.value = fifo->opal_fifo_head.value};
        opal_update_counted_pointer (&fifo->opal_fifo_head, &head, item);
    } else {
        /* update previous item */
        tail.data.item->opal_list_next = item;
    }

    return (opal_list_item_t *) tail.data.item;
}

/* Retrieve one element from the FIFO. If we reach the ghost element then the FIFO
 * is empty so we return NULL.
 */
static inline opal_list_item_t *opal_fifo_pop_atomic (opal_fifo_t *fifo)
{
    opal_list_item_t *item, *next, *ghost = &fifo->opal_fifo_ghost;
    opal_counted_pointer_t head, tail;

    opal_read_counted_pointer (&fifo->opal_fifo_head, &head);

    do {
        tail.value = fifo->opal_fifo_tail.value;
        opal_atomic_rmb ();

        item = (opal_list_item_t *) head.data.item;
        next = (opal_list_item_t *) item->opal_list_next;

        if (ghost == tail.data.item && ghost == item) {
            return NULL;
        }

        /* the head or next pointer are in an inconsistent state. keep looping. */
        if (tail.data.item != item && ghost != tail.data.item && ghost == next) {
            opal_read_counted_pointer (&fifo->opal_fifo_head, &head);
            continue;
        }

        /* try popping the head */
        if (opal_update_counted_pointer (&fifo->opal_fifo_head, &head, next)) {
            break;
        }
    } while (1);

    opal_atomic_wmb ();

    /* check for tail and head consistency */
    if (ghost == next) {
        /* the head was just set to &fifo->opal_fifo_ghost. try to update the tail as well */
        if (!opal_update_counted_pointer (&fifo->opal_fifo_tail, &tail, ghost)) {
            /* tail was changed by a push operation. wait for the item's next pointer to be se then
             * update the head */

            /* wait for next pointer to be updated by push */
            while (ghost == item->opal_list_next) {
                opal_atomic_rmb ();
            }

            opal_atomic_rmb ();

            /* update the head with the real next value. note that no other thread
             * will be attempting to update the head until after it has been updated
             * with the next pointer. push will not see an empty list and other pop
             * operations will loop until the head is consistent. */
            fifo->opal_fifo_head.data.item = (opal_list_item_t *) item->opal_list_next;
            opal_atomic_wmb ();
        }
    }

    item->opal_list_next = NULL;

    return item;
}

#else

/* When compare-and-set 128 is not available we avoid the ABA problem by
 * using a spin-lock on the head (using the head counter). Otherwise
 * the algorithm is identical to the compare-and-set 128 version. */
static inline opal_list_item_t *opal_fifo_push_atomic (opal_fifo_t *fifo,
                                                       opal_list_item_t *item)
{
    const opal_list_item_t * const ghost = &fifo->opal_fifo_ghost;
    opal_list_item_t *tail_item;

    item->opal_list_next = (opal_list_item_t *) ghost;

    opal_atomic_wmb ();

    /* try to get the tail */
    tail_item = opal_atomic_swap_ptr (&fifo->opal_fifo_tail.data.item, item);

    opal_atomic_wmb ();

    if (ghost == tail_item) {
        /* update the head */
        fifo->opal_fifo_head.data.item = item;
    } else {
        /* update previous item */
        tail_item->opal_list_next = item;
    }

    opal_atomic_wmb ();

    return (opal_list_item_t *) tail_item;
}

/* Retrieve one element from the FIFO. If we reach the ghost element then the FIFO
 * is empty so we return NULL.
 */
static inline opal_list_item_t *opal_fifo_pop_atomic (opal_fifo_t *fifo)
{
    const opal_list_item_t * const ghost = &fifo->opal_fifo_ghost;

#if OPAL_HAVE_ATOMIC_LLSC_PTR
    register opal_list_item_t *item, *next;
    int attempt = 0, ret = 0;

    /* use load-linked store-conditional to avoid ABA issues */
    do {
        if (++attempt == 5) {
            /* deliberatly suspend this thread to allow other threads to run. this should
             * only occur during periods of contention on the lifo. */
            _opal_lifo_release_cpu ();
            attempt = 0;
        }

        opal_atomic_ll_ptr(&fifo->opal_fifo_head.data.item, item);
        if (ghost == item) {
            if (ghost == fifo->opal_fifo_tail.data.item) {
                return NULL;
            }

            /* fifo does not appear empty. wait for the fifo to be made
             * consistent by conflicting thread. */
            continue;
        }

        next = (opal_list_item_t *) item->opal_list_next;
        opal_atomic_sc_ptr(&fifo->opal_fifo_head.data.item, next, ret);
    } while (!ret);

#else
    opal_list_item_t *item, *next;

    /* protect against ABA issues by "locking" the head */
    do {
        if (!opal_atomic_swap_32 ((volatile int32_t *) &fifo->opal_fifo_head.data.counter, 1)) {
            break;
        }

        opal_atomic_wmb ();
    } while (1);

    opal_atomic_wmb();

    item = opal_fifo_head (fifo);
    if (ghost == item) {
        fifo->opal_fifo_head.data.counter = 0;
        return NULL;
    }

    next = (opal_list_item_t *) item->opal_list_next;
    fifo->opal_fifo_head.data.item = next;
#endif

    if (ghost == next) {
        void *tmp = item;

        if (!opal_atomic_compare_exchange_strong_ptr (&fifo->opal_fifo_tail.data.item, &tmp, (void *) ghost)) {
            do {
                opal_atomic_rmb ();
            } while (ghost == item->opal_list_next);

            fifo->opal_fifo_head.data.item = (opal_list_item_t *) item->opal_list_next;
        }
    }

    opal_atomic_wmb ();

    /* unlock the head */
    fifo->opal_fifo_head.data.counter = 0;

    item->opal_list_next = NULL;

    return item;
}

#endif

/* single threaded versions of push/pop */
static inline opal_list_item_t *opal_fifo_push_st (opal_fifo_t *fifo,
                                                   opal_list_item_t *item)
{
    opal_list_item_t *prev = opal_fifo_tail (fifo);

    item->opal_list_next = &fifo->opal_fifo_ghost;

    fifo->opal_fifo_tail.data.item = item;
    if (&fifo->opal_fifo_ghost == opal_fifo_head (fifo)) {
        fifo->opal_fifo_head.data.item = item;
    } else {
        prev->opal_list_next = item;
    }

    return (opal_list_item_t *) item->opal_list_next;
}

static inline opal_list_item_t *opal_fifo_pop_st (opal_fifo_t *fifo)
{
    opal_list_item_t *item = opal_fifo_head (fifo);

    if (item == &fifo->opal_fifo_ghost) {
        return NULL;
    }

    fifo->opal_fifo_head.data.item = (opal_list_item_t *) item->opal_list_next;
    if (&fifo->opal_fifo_ghost == opal_fifo_head (fifo)) {
        fifo->opal_fifo_tail.data.item = &fifo->opal_fifo_ghost;
    }

    item->opal_list_next = NULL;
    return item;
}

/* push/pop versions conditioned off opal_using_threads() */
static inline opal_list_item_t *opal_fifo_push (opal_fifo_t *fifo,
                                                opal_list_item_t *item)
{
    if (opal_using_threads ()) {
        return opal_fifo_push_atomic (fifo, item);
    }

    return opal_fifo_push_st (fifo, item);
}

static inline opal_list_item_t *opal_fifo_pop (opal_fifo_t *fifo)
{
    if (opal_using_threads ()) {
        return opal_fifo_pop_atomic (fifo);
    }

    return opal_fifo_pop_st (fifo);
}

END_C_DECLS

#endif  /* OPAL_FIFO_H_HAS_BEEN_INCLUDED */
