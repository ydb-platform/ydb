/*
 * Copyright (c) 2007-2013 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2008      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017-2018 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#ifndef PMIX_THREADS_TSD_H
#define PMIX_THREADS_TSD_H

#include "pmix_config.h"

#include <pthread.h>

#include "pmix_common.h"

BEGIN_C_DECLS

/**
 * @file
 *
 * Thread Specific Datastore Interface
 *
 * Functions for providing thread-specific datastore capabilities.
 */


/**
 * Prototype for callback when tsd data is being destroyed
 */
typedef void (*pmix_tsd_destructor_t)(void *value);


typedef pthread_key_t pmix_tsd_key_t;

static inline int
pmix_tsd_key_delete(pmix_tsd_key_t key)
{
    return pthread_key_delete(key);
}

static inline int
pmix_tsd_setspecific(pmix_tsd_key_t key, void *value)
{
    return pthread_setspecific(key, value);
}

static inline int
pmix_tsd_getspecific(pmix_tsd_key_t key, void **valuep)
{
    *valuep = pthread_getspecific(key);
    return PMIX_SUCCESS;
}

/**
 * Create thread-specific data key
 *
 * Create a thread-specific data key visible to all threads in the
 * current process.  The returned key is valid in all threads,
 * although the values bound to the key by pmix_tsd_setspecific() are
 * allocated on a per-thread basis and persist for the life of the
 * calling thread.
 *
 * Upon key creation, the value NULL is associated with the new key in
 * all active threads.  When a new thread is created, the value NULL
 * is associated with all defined keys in the new thread.
 *
 * The destructor parameter may be NULL.  At thread exit, if
 * destructor is non-NULL AND the thread has a non-NULL value
 * associated with the key, the function is called with the current
 * value as its argument.
 *
 * @param key[out]       The key for accessing thread-specific data
 * @param destructor[in] Cleanup function to call when a thread exits
 *
 * @retval PMIX_SUCCESS  Success
 * @retval EAGAIN        The system lacked the necessary resource to
 *                       create another thread specific data key
 * @retval ENOMEM        Insufficient memory exists to create the key
 */
PMIX_EXPORT int pmix_tsd_key_create(pmix_tsd_key_t *key,
                                      pmix_tsd_destructor_t destructor);


/**
 * Destruct all thread-specific data keys
 *
 * Destruct all thread-specific data keys and invoke the destructor
 *
 * This should only be invoked in the main thread.
 * This is made necessary since destructors are not invoked on the
 * keys of the main thread, since there is no such thing as
 * pthread_join(main_thread)
 *
 * @retval PMIX_SUCCESS  Success
 */
PMIX_EXPORT int pmix_tsd_keys_destruct(void);

END_C_DECLS

#endif /* PMIX_MTHREADS_TSD_H */
