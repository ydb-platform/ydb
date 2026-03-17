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
 * Copyright (c) 2017      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef  PMIX_MUTEX_H
#define  PMIX_MUTEX_H 1

#include "pmix_config.h"

#include "src/threads/thread_usage.h"

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
typedef struct pmix_mutex_t pmix_mutex_t;
typedef struct pmix_mutex_t pmix_recursive_mutex_t;

/**
 * Try to acquire a mutex.
 *
 * @param mutex         Address of the mutex.
 * @return              0 if the mutex was acquired, 1 otherwise.
 */
static inline int pmix_mutex_trylock(pmix_mutex_t *mutex);


/**
 * Acquire a mutex.
 *
 * @param mutex         Address of the mutex.
 */
static inline void pmix_mutex_lock(pmix_mutex_t *mutex);


/**
 * Release a mutex.
 *
 * @param mutex         Address of the mutex.
 */
static inline void pmix_mutex_unlock(pmix_mutex_t *mutex);


/**
 * Try to acquire a mutex using atomic operations.
 *
 * @param mutex         Address of the mutex.
 * @return              0 if the mutex was acquired, 1 otherwise.
 */
static inline int pmix_mutex_atomic_trylock(pmix_mutex_t *mutex);


/**
 * Acquire a mutex using atomic operations.
 *
 * @param mutex         Address of the mutex.
 */
static inline void pmix_mutex_atomic_lock(pmix_mutex_t *mutex);


/**
 * Release a mutex using atomic operations.
 *
 * @param mutex         Address of the mutex.
 */
static inline void pmix_mutex_atomic_unlock(pmix_mutex_t *mutex);

END_C_DECLS

#include "mutex_unix.h"

#endif                          /* PMIX_MUTEX_H */
