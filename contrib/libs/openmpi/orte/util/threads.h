/*
 * Copyright (c) 2017      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef ORTE_THREADS_H
#define ORTE_THREADS_H

#include "orte_config.h"

#include "opal/sys/atomic.h"
#include "opal/threads/threads.h"

/* provide macros for forward-proofing the shifting
 * of objects between threads - at some point, we
 * may revamp our threading model */

/* post an object to another thread - for now, we
 * only have a memory barrier */
#define ORTE_POST_OBJECT(o)     opal_atomic_wmb()

/* acquire an object from another thread - for now,
 * we only have a memory barrier */
#define ORTE_ACQUIRE_OBJECT(o)  opal_atomic_rmb()

#define orte_condition_wait(a,b)    pthread_cond_wait(a, &(b)->m_lock_pthread)
typedef pthread_cond_t orte_condition_t;
#define orte_condition_broadcast(a) pthread_cond_broadcast(a)
#define orte_condition_signal(a)    pthread_cond_signal(a)
#define ORTE_CONDITION_STATIC_INIT PTHREAD_COND_INITIALIZER

/* define a threadshift macro */
#define ORTE_THREADSHIFT(x, eb, f, p)                                   \
    do {                                                                \
        opal_event_set((eb), &((x)->ev), -1, OPAL_EV_WRITE, (f), (x));  \
        opal_event_set_priority(&((x)->ev), (p));                       \
        ORTE_POST_OBJECT((x));                                          \
        opal_event_active(&((x)->ev), OPAL_EV_WRITE, 1);                \
    } while(0)

typedef struct {
    opal_mutex_t mutex;
    orte_condition_t cond;
    volatile bool active;
} orte_lock_t;

#define ORTE_CONSTRUCT_LOCK(l)                          \
    do {                                                \
        OBJ_CONSTRUCT(&(l)->mutex, opal_mutex_t);       \
        pthread_cond_init(&(l)->cond, NULL);            \
        (l)->active = true;                             \
    } while(0)

#define ORTE_DESTRUCT_LOCK(l)               \
    do {                                    \
        OBJ_DESTRUCT(&(l)->mutex);          \
        pthread_cond_destroy(&(l)->cond);   \
    } while(0)


#if OPAL_ENABLE_DEBUG
#define ORTE_ACQUIRE_THREAD(lck)                                \
    do {                                                        \
        opal_mutex_lock(&(lck)->mutex);                         \
        if (opal_debug_threads) {                               \
            opal_output(0, "Waiting for thread %s:%d",          \
                        __FILE__, __LINE__);                    \
        }                                                       \
        while ((lck)->active) {                                 \
            orte_condition_wait(&(lck)->cond, &(lck)->mutex);   \
        }                                                       \
        if (opal_debug_threads) {                               \
            opal_output(0, "Thread obtained %s:%d",             \
                        __FILE__, __LINE__);                    \
        }                                                       \
        (lck)->active = true;                                   \
        OPAL_ACQUIRE_OBJECT(lck);                               \
    } while(0)
#else
#define ORTE_ACQUIRE_THREAD(lck)                                \
    do {                                                        \
        opal_mutex_lock(&(lck)->mutex);                         \
        while ((lck)->active) {                                 \
            orte_condition_wait(&(lck)->cond, &(lck)->mutex);   \
        }                                                       \
        (lck)->active = true;                                   \
        OPAL_ACQUIRE_OBJECT(lck);                               \
    } while(0)
#endif


#if OPAL_ENABLE_DEBUG
#define ORTE_WAIT_THREAD(lck)                                   \
    do {                                                        \
        opal_mutex_lock(&(lck)->mutex);                         \
        if (opal_debug_threads) {                               \
            opal_output(0, "Waiting for thread %s:%d",          \
                        __FILE__, __LINE__);                    \
        }                                                       \
        while ((lck)->active) {                                 \
            orte_condition_wait(&(lck)->cond, &(lck)->mutex);   \
        }                                                       \
        if (opal_debug_threads) {                               \
            opal_output(0, "Thread obtained %s:%d",             \
                        __FILE__, __LINE__);                    \
        }                                                       \
        OPAL_ACQUIRE_OBJECT(&lck);                              \
        opal_mutex_unlock(&(lck)->mutex);                       \
    } while(0)
#else
#define ORTE_WAIT_THREAD(lck)                                   \
    do {                                                        \
        opal_mutex_lock(&(lck)->mutex);                         \
        while ((lck)->active) {                                 \
            orte_condition_wait(&(lck)->cond, &(lck)->mutex);   \
        }                                                       \
        OPAL_ACQUIRE_OBJECT(lck);                               \
        opal_mutex_unlock(&(lck)->mutex);                       \
    } while(0)
#endif


#if OPAL_ENABLE_DEBUG
#define ORTE_RELEASE_THREAD(lck)                        \
    do {                                                \
        if (opal_debug_threads) {                       \
            opal_output(0, "Releasing thread %s:%d",    \
                        __FILE__, __LINE__);            \
        }                                               \
        (lck)->active = false;                          \
        OPAL_POST_OBJECT(lck);                          \
        orte_condition_broadcast(&(lck)->cond);         \
        opal_mutex_unlock(&(lck)->mutex);               \
    } while(0)
#else
#define ORTE_RELEASE_THREAD(lck)                \
    do {                                        \
        (lck)->active = false;                  \
        OPAL_POST_OBJECT(lck);                  \
       orte_condition_broadcast(&(lck)->cond);  \
        opal_mutex_unlock(&(lck)->mutex);       \
    } while(0)
#endif


#define ORTE_WAKEUP_THREAD(lck)                 \
    do {                                        \
        opal_mutex_lock(&(lck)->mutex);         \
        (lck)->active = false;                  \
        OPAL_POST_OBJECT(lck);                  \
        orte_condition_broadcast(&(lck)->cond); \
        opal_mutex_unlock(&(lck)->mutex);       \
    } while(0)

#endif /* ORTE_THREADS_H */
