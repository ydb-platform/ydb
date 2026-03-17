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
 * Copyright (c) 2010      Cisco Systems, Inc. All rights reserved.
 * Copyright (c) 2010      Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017-2018 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef PMIX_THREAD_H
#define PMIX_THREAD_H 1

#include "pmix_config.h"

#include <pthread.h>
#include <signal.h>

#include "src/class/pmix_object.h"
#if PMIX_ENABLE_DEBUG
#include "src/util/output.h"
#endif

#include "mutex.h"

BEGIN_C_DECLS

typedef void *(*pmix_thread_fn_t) (pmix_object_t *);

#define PMIX_THREAD_CANCELLED   ((void*)1);

struct pmix_thread_t {
    pmix_object_t super;
    pmix_thread_fn_t t_run;
    void* t_arg;
    pthread_t t_handle;
};

typedef struct pmix_thread_t pmix_thread_t;

#if PMIX_ENABLE_DEBUG
PMIX_EXPORT extern bool pmix_debug_threads;
#endif


PMIX_EXPORT PMIX_CLASS_DECLARATION(pmix_thread_t);

#define pmix_condition_wait(a,b)    pthread_cond_wait(a, &(b)->m_lock_pthread)
typedef pthread_cond_t pmix_condition_t;
#define pmix_condition_broadcast(a) pthread_cond_broadcast(a)
#define pmix_condition_signal(a)    pthread_cond_signal(a)
#define PMIX_CONDITION_STATIC_INIT PTHREAD_COND_INITIALIZER

typedef struct {
    pmix_status_t status;
    pmix_mutex_t mutex;
    pmix_condition_t cond;
    volatile bool active;
} pmix_lock_t;

#define PMIX_CONSTRUCT_LOCK(l)                          \
    do {                                                \
        PMIX_CONSTRUCT(&(l)->mutex, pmix_mutex_t);      \
        pthread_cond_init(&(l)->cond, NULL);            \
        (l)->active = true;                             \
    } while(0)

#define PMIX_DESTRUCT_LOCK(l)               \
    do {                                    \
        PMIX_DESTRUCT(&(l)->mutex);         \
        pthread_cond_destroy(&(l)->cond);   \
    } while(0)


#if PMIX_ENABLE_DEBUG
#define PMIX_ACQUIRE_THREAD(lck)                                \
    do {                                                        \
        pmix_mutex_lock(&(lck)->mutex);                         \
        if (pmix_debug_threads) {                               \
            pmix_output(0, "Waiting for thread %s:%d",          \
                        __FILE__, __LINE__);                    \
        }                                                       \
        while ((lck)->active) {                                 \
            pmix_condition_wait(&(lck)->cond, &(lck)->mutex);   \
        }                                                       \
        if (pmix_debug_threads) {                               \
            pmix_output(0, "Thread obtained %s:%d",             \
                        __FILE__, __LINE__);                    \
        }                                                       \
        PMIX_ACQUIRE_OBJECT(lck);                               \
        (lck)->active = true;                                   \
    } while(0)
#else
#define PMIX_ACQUIRE_THREAD(lck)                                \
    do {                                                        \
        pmix_mutex_lock(&(lck)->mutex);                         \
        while ((lck)->active) {                                 \
            pmix_condition_wait(&(lck)->cond, &(lck)->mutex);   \
        }                                                       \
        PMIX_ACQUIRE_OBJECT(lck);                               \
        (lck)->active = true;                                   \
    } while(0)
#endif


#if PMIX_ENABLE_DEBUG
#define PMIX_WAIT_THREAD(lck)                                   \
    do {                                                        \
        pmix_mutex_lock(&(lck)->mutex);                         \
        if (pmix_debug_threads) {                               \
            pmix_output(0, "Waiting for thread %s:%d",          \
                        __FILE__, __LINE__);                    \
        }                                                       \
        while ((lck)->active) {                                 \
            pmix_condition_wait(&(lck)->cond, &(lck)->mutex);   \
        }                                                       \
        if (pmix_debug_threads) {                               \
            pmix_output(0, "Thread obtained %s:%d",             \
                        __FILE__, __LINE__);                    \
        }                                                       \
        PMIX_ACQUIRE_OBJECT(lck);                               \
        pmix_mutex_unlock(&(lck)->mutex);                       \
    } while(0)
#else
#define PMIX_WAIT_THREAD(lck)                                   \
    do {                                                        \
        pmix_mutex_lock(&(lck)->mutex);                         \
        while ((lck)->active) {                                 \
            pmix_condition_wait(&(lck)->cond, &(lck)->mutex);   \
        }                                                       \
        PMIX_ACQUIRE_OBJECT(lck);                               \
        pmix_mutex_unlock(&(lck)->mutex);                       \
    } while(0)
#endif


#if PMIX_ENABLE_DEBUG
#define PMIX_RELEASE_THREAD(lck)                        \
    do {                                                \
        if (pmix_debug_threads) {                       \
            pmix_output(0, "Releasing thread %s:%d",    \
                        __FILE__, __LINE__);            \
        }                                               \
        (lck)->active = false;                          \
        PMIX_POST_OBJECT(lck);                  \
        pmix_condition_broadcast(&(lck)->cond);         \
        pmix_mutex_unlock(&(lck)->mutex);               \
    } while(0)
#else
#define PMIX_RELEASE_THREAD(lck)                \
    do {                                        \
        (lck)->active = false;                  \
        PMIX_POST_OBJECT(lck);                  \
        pmix_condition_broadcast(&(lck)->cond); \
        pmix_mutex_unlock(&(lck)->mutex);       \
    } while(0)
#endif


#define PMIX_WAKEUP_THREAD(lck)                 \
    do {                                        \
        pmix_mutex_lock(&(lck)->mutex);         \
        (lck)->active = false;                  \
        PMIX_POST_OBJECT(lck);                  \
        pmix_condition_broadcast(&(lck)->cond); \
        pmix_mutex_unlock(&(lck)->mutex);       \
    } while(0)


/* provide a macro for forward-proofing the shifting
 * of objects between threads - at some point, we
 * may revamp our threading model */

/* post an object to another thread - for now, we
 * only have a memory barrier */
#define PMIX_POST_OBJECT(o)     pmix_atomic_wmb()

/* acquire an object from another thread - for now,
 * we only have a memory barrier */
#define PMIX_ACQUIRE_OBJECT(o)  pmix_atomic_rmb()


PMIX_EXPORT int  pmix_thread_start(pmix_thread_t *);
PMIX_EXPORT int  pmix_thread_join(pmix_thread_t *, void **thread_return);
PMIX_EXPORT bool pmix_thread_self_compare(pmix_thread_t*);
PMIX_EXPORT pmix_thread_t *pmix_thread_get_self(void);
PMIX_EXPORT void pmix_thread_kill(pmix_thread_t *, int sig);
PMIX_EXPORT void pmix_thread_set_main(void);

END_C_DECLS

#endif /* PMIX_THREAD_H */
