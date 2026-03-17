/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2016 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017-2018 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "pmix_config.h"

#include "src/threads/mutex.h"

static void pmix_mutex_construct(pmix_mutex_t *m)
{
#if PMIX_ENABLE_DEBUG
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);

    /* set type to ERRORCHECK so that we catch recursive locks */
#if PMIX_HAVE_PTHREAD_MUTEX_ERRORCHECK_NP
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK_NP);
#elif PMIX_HAVE_PTHREAD_MUTEX_ERRORCHECK
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK);
#endif /* PMIX_HAVE_PTHREAD_MUTEX_ERRORCHECK_NP */

    pthread_mutex_init(&m->m_lock_pthread, &attr);
    pthread_mutexattr_destroy(&attr);

    m->m_lock_debug = 0;
    m->m_lock_file = NULL;
    m->m_lock_line = 0;
#else

    /* Without debugging, choose the fastest available mutexes */
    pthread_mutex_init(&m->m_lock_pthread, NULL);

#endif /* PMIX_ENABLE_DEBUG */

#if PMIX_HAVE_ATOMIC_SPINLOCKS
    pmix_atomic_lock_init( &m->m_lock_atomic, PMIX_ATOMIC_LOCK_UNLOCKED );
#endif
}

static void pmix_mutex_destruct(pmix_mutex_t *m)
{
    pthread_mutex_destroy(&m->m_lock_pthread);
}

PMIX_CLASS_INSTANCE(pmix_mutex_t,
                   pmix_object_t,
                   pmix_mutex_construct,
                   pmix_mutex_destruct);

static void pmix_recursive_mutex_construct(pmix_recursive_mutex_t *m)
{
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);

#if PMIX_ENABLE_DEBUG
    m->m_lock_debug = 0;
    m->m_lock_file = NULL;
    m->m_lock_line = 0;
#endif

    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);

    pthread_mutex_init(&m->m_lock_pthread, &attr);
    pthread_mutexattr_destroy(&attr);

#if PMIX_HAVE_ATOMIC_SPINLOCKS
    pmix_atomic_lock_init( &m->m_lock_atomic, PMIX_ATOMIC_LOCK_UNLOCKED );
#endif
}

PMIX_CLASS_INSTANCE(pmix_recursive_mutex_t,
                   pmix_object_t,
                   pmix_recursive_mutex_construct,
                   pmix_mutex_destruct);
