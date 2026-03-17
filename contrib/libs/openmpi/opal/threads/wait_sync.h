/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2016      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016      Mellanox Technologies. All rights reserved.
 * Copyright (c) 2016      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#if !defined(OPAL_THREADS_WAIT_SYNC_H)
#define OPAL_THREADS_WAIT_SYNC_H

#include "opal/sys/atomic.h"
#include "opal/threads/condition.h"
#include <pthread.h>

BEGIN_C_DECLS

typedef struct ompi_wait_sync_t {
    int32_t count;
    int32_t status;
    pthread_cond_t condition;
    pthread_mutex_t lock;
    struct ompi_wait_sync_t *next;
    struct ompi_wait_sync_t *prev;
    volatile bool signaling;
} ompi_wait_sync_t;

#define REQUEST_PENDING        (void*)0L
#define REQUEST_COMPLETED      (void*)1L

#define SYNC_WAIT(sync)                 (opal_using_threads() ? ompi_sync_wait_mt (sync) : sync_wait_st (sync))

/* The loop in release handles a race condition between the signaling
 * thread and the destruction of the condition variable. The signaling
 * member will be set to false after the final signaling thread has
 * finished operating on the sync object. This is done to avoid
 * extra atomics in the signalling function and keep it as fast
 * as possible. Note that the race window is small so spinning here
 * is more optimal than sleeping since this macro is called in
 * the critical path. */
#define WAIT_SYNC_RELEASE(sync)                       \
    if (opal_using_threads()) {                       \
        while ((sync)->signaling) {                   \
            continue;                                 \
        }                                             \
        pthread_cond_destroy(&(sync)->condition);     \
        pthread_mutex_destroy(&(sync)->lock);         \
    }

#define WAIT_SYNC_RELEASE_NOWAIT(sync)                \
    if (opal_using_threads()) {                       \
        pthread_cond_destroy(&(sync)->condition);     \
        pthread_mutex_destroy(&(sync)->lock);         \
    }


#define WAIT_SYNC_SIGNAL(sync)                        \
    if (opal_using_threads()) {                       \
        pthread_mutex_lock(&(sync->lock));            \
        pthread_cond_signal(&sync->condition);        \
        pthread_mutex_unlock(&(sync->lock));          \
        sync->signaling = false;                      \
    }

#define WAIT_SYNC_SIGNALLED(sync){                    \
        (sync)->signaling = false;                    \
}

OPAL_DECLSPEC int ompi_sync_wait_mt(ompi_wait_sync_t *sync);
static inline int sync_wait_st (ompi_wait_sync_t *sync)
{
    while (sync->count > 0) {
        opal_progress();
    }

    return sync->status;
}


#define WAIT_SYNC_INIT(sync,c)                                  \
    do {                                                        \
        (sync)->count = (c);                                    \
        (sync)->next = NULL;                                    \
        (sync)->prev = NULL;                                    \
        (sync)->status = 0;                                     \
        (sync)->signaling = (0 != (c));                         \
        if (opal_using_threads()) {                             \
            pthread_cond_init (&(sync)->condition, NULL);       \
            pthread_mutex_init (&(sync)->lock, NULL);           \
        }                                                       \
    } while(0)

/**
 * Update the status of the synchronization primitive. If an error is
 * reported the synchronization is completed and the signal
 * triggered. The status of the synchronization will be reported to
 * the waiting threads.
 */
static inline void wait_sync_update(ompi_wait_sync_t *sync, int updates, int status)
{
    if( OPAL_LIKELY(OPAL_SUCCESS == status) ) {
        if( 0 != (OPAL_THREAD_ADD_FETCH32(&sync->count, -updates)) ) {
            return;
        }
    } else {
        /* this is an error path so just use the atomic */
        sync->status = OPAL_ERROR;
        opal_atomic_wmb ();
        opal_atomic_swap_32 (&sync->count, 0);
    }
    WAIT_SYNC_SIGNAL(sync);
}

END_C_DECLS

#endif /* defined(OPAL_THREADS_WAIT_SYNC_H) */
