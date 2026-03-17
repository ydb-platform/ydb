/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2007-2016 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2007      Voltaire. All rights reserved.
 * Copyright (c) 2010      Oracle and/or its affiliates.  All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef  OPAL_MUTEX_H
#define  OPAL_MUTEX_H 1

#include "opal_config.h"

#include "opal/threads/thread_usage.h"

BEGIN_C_DECLS

/**
 * @file:
 *
 * Mutual exclusion functions.
 *
 * Functions for locking of critical sections.
 */

/**
 * Opaque mutex object
 */
typedef struct opal_mutex_t opal_mutex_t;
typedef struct opal_mutex_t opal_recursive_mutex_t;

/**
 * Try to acquire a mutex.
 *
 * @param mutex         Address of the mutex.
 * @return              0 if the mutex was acquired, 1 otherwise.
 */
static inline int opal_mutex_trylock(opal_mutex_t *mutex);


/**
 * Acquire a mutex.
 *
 * @param mutex         Address of the mutex.
 */
static inline void opal_mutex_lock(opal_mutex_t *mutex);


/**
 * Release a mutex.
 *
 * @param mutex         Address of the mutex.
 */
static inline void opal_mutex_unlock(opal_mutex_t *mutex);


/**
 * Try to acquire a mutex using atomic operations.
 *
 * @param mutex         Address of the mutex.
 * @return              0 if the mutex was acquired, 1 otherwise.
 */
static inline int opal_mutex_atomic_trylock(opal_mutex_t *mutex);


/**
 * Acquire a mutex using atomic operations.
 *
 * @param mutex         Address of the mutex.
 */
static inline void opal_mutex_atomic_lock(opal_mutex_t *mutex);


/**
 * Release a mutex using atomic operations.
 *
 * @param mutex         Address of the mutex.
 */
static inline void opal_mutex_atomic_unlock(opal_mutex_t *mutex);

END_C_DECLS

#include "mutex_unix.h"

BEGIN_C_DECLS

/**
 * Lock a mutex if opal_using_threads() says that multiple threads may
 * be active in the process.
 *
 * @param mutex Pointer to a opal_mutex_t to lock.
 *
 * If there is a possibility that multiple threads are running in the
 * process (as determined by opal_using_threads()), this function will
 * block waiting to lock the mutex.
 *
 * If there is no possibility that multiple threads are running in the
 * process, return immediately.
 */
#define OPAL_THREAD_LOCK(mutex)                 \
    do {                                        \
        if (OPAL_UNLIKELY(opal_using_threads())) {      \
            opal_mutex_lock(mutex);             \
        }                                       \
    } while (0)


/**
 * Try to lock a mutex if opal_using_threads() says that multiple
 * threads may be active in the process.
 *
 * @param mutex Pointer to a opal_mutex_t to trylock
 *
 * If there is a possibility that multiple threads are running in the
 * process (as determined by opal_using_threads()), this function will
 * trylock the mutex.
 *
 * If there is no possibility that multiple threads are running in the
 * process, return immediately without modifying the mutex.
 *
 * Returns 0 if mutex was locked, non-zero otherwise.
 */
#define OPAL_THREAD_TRYLOCK(mutex)                      \
    (OPAL_UNLIKELY(opal_using_threads()) ? opal_mutex_trylock(mutex) : 0)

/**
 * Unlock a mutex if opal_using_threads() says that multiple threads
 * may be active in the process.
 *
 * @param mutex Pointer to a opal_mutex_t to unlock.
 *
 * If there is a possibility that multiple threads are running in the
 * process (as determined by opal_using_threads()), this function will
 * unlock the mutex.
 *
 * If there is no possibility that multiple threads are running in the
 * process, return immediately without modifying the mutex.
 */
#define OPAL_THREAD_UNLOCK(mutex)               \
    do {                                        \
        if (OPAL_UNLIKELY(opal_using_threads())) {      \
            opal_mutex_unlock(mutex);           \
        }                                       \
    } while (0)


/**
 * Lock a mutex if opal_using_threads() says that multiple threads may
 * be active in the process for the duration of the specified action.
 *
 * @param mutex    Pointer to a opal_mutex_t to lock.
 * @param action   A scope over which the lock is held.
 *
 * If there is a possibility that multiple threads are running in the
 * process (as determined by opal_using_threads()), this function will
 * acquire the lock before invoking the specified action and release
 * it on return.
 *
 * If there is no possibility that multiple threads are running in the
 * process, invoke the action without acquiring the lock.
 */
#define OPAL_THREAD_SCOPED_LOCK(mutex, action)  \
    do {                                        \
        if(OPAL_UNLIKELY(opal_using_threads())) {       \
            opal_mutex_lock(mutex);             \
            action;                             \
            opal_mutex_unlock(mutex);           \
        } else {                                \
            action;                             \
        }                                       \
    } while (0)

END_C_DECLS

#endif                          /* OPAL_MUTEX_H */
