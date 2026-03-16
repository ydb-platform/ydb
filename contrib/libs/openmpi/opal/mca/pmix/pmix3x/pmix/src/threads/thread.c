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
 * Copyright (c) 2010      Cisco Systems, Inc. All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "pmix_config.h"

#include "src/threads/threads.h"
#include "src/threads/tsd.h"
#include "pmix_common.h"

bool pmix_debug_threads = false;

static void pmix_thread_construct(pmix_thread_t *t);

static pthread_t pmix_main_thread;

struct pmix_tsd_key_value {
    pmix_tsd_key_t key;
    pmix_tsd_destructor_t destructor;
};

static struct pmix_tsd_key_value *pmix_tsd_key_values = NULL;
static int pmix_tsd_key_values_count = 0;

PMIX_EXPORT PMIX_CLASS_INSTANCE(pmix_thread_t,
                                pmix_object_t,
                                pmix_thread_construct, NULL);


/*
 * Constructor
 */
static void pmix_thread_construct(pmix_thread_t *t)
{
    t->t_run = 0;
    t->t_handle = (pthread_t) -1;
}

int pmix_thread_start(pmix_thread_t *t)
{
    int rc;

    if (PMIX_ENABLE_DEBUG) {
        if (NULL == t->t_run || t->t_handle != (pthread_t) -1) {
            return PMIX_ERR_BAD_PARAM;
        }
    }

    rc = pthread_create(&t->t_handle, NULL, (void*(*)(void*)) t->t_run, t);

    return (rc == 0) ? PMIX_SUCCESS : PMIX_ERROR;
}


int pmix_thread_join(pmix_thread_t *t, void **thr_return)
{
    int rc = pthread_join(t->t_handle, thr_return);
    t->t_handle = (pthread_t) -1;
    return (rc == 0) ? PMIX_SUCCESS : PMIX_ERROR;
}


bool pmix_thread_self_compare(pmix_thread_t *t)
{
    return t->t_handle == pthread_self();
}


pmix_thread_t *pmix_thread_get_self(void)
{
    pmix_thread_t *t = PMIX_NEW(pmix_thread_t);
    t->t_handle = pthread_self();
    return t;
}

void pmix_thread_kill(pmix_thread_t *t, int sig)
{
    pthread_kill(t->t_handle, sig);
}

int pmix_tsd_key_create(pmix_tsd_key_t *key,
                    pmix_tsd_destructor_t destructor)
{
    int rc;
    rc = pthread_key_create(key, destructor);
    if ((0 == rc) && (pthread_self() == pmix_main_thread)) {
        pmix_tsd_key_values = (struct pmix_tsd_key_value *)realloc(pmix_tsd_key_values, (pmix_tsd_key_values_count+1) * sizeof(struct pmix_tsd_key_value));
        pmix_tsd_key_values[pmix_tsd_key_values_count].key = *key;
        pmix_tsd_key_values[pmix_tsd_key_values_count].destructor = destructor;
        pmix_tsd_key_values_count ++;
    }
    return rc;
}

int pmix_tsd_keys_destruct()
{
    int i;
    void * ptr;
    for (i=0; i<pmix_tsd_key_values_count; i++) {
        if(PMIX_SUCCESS == pmix_tsd_getspecific(pmix_tsd_key_values[i].key, &ptr)) {
            if (NULL != pmix_tsd_key_values[i].destructor) {
                pmix_tsd_key_values[i].destructor(ptr);
                pmix_tsd_setspecific(pmix_tsd_key_values[i].key, NULL);
            }
        }
    }
    if (0 < pmix_tsd_key_values_count) {
        free(pmix_tsd_key_values);
        pmix_tsd_key_values_count = 0;
    }
    return PMIX_SUCCESS;
}

void pmix_thread_set_main() {
    pmix_main_thread = pthread_self();
}
