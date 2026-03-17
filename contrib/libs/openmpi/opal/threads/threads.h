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
 * Copyright (c) 2017      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_THREAD_H
#define OPAL_THREAD_H 1

#include "opal_config.h"

#include <pthread.h>
#include <signal.h>

#include "opal/class/opal_object.h"
#if OPAL_ENABLE_DEBUG
#include "opal/util/output.h"
#endif

#include "mutex.h"
#include "condition.h"

BEGIN_C_DECLS

typedef void *(*opal_thread_fn_t) (opal_object_t *);

#define OPAL_THREAD_CANCELLED   ((void*)1);

struct opal_thread_t {
    opal_object_t super;
    opal_thread_fn_t t_run;
    void* t_arg;
    pthread_t t_handle;
};

typedef struct opal_thread_t opal_thread_t;

#if OPAL_ENABLE_DEBUG
OPAL_DECLSPEC extern bool opal_debug_threads;
#endif


OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_thread_t);

#if OPAL_ENABLE_DEBUG
#define OPAL_ACQUIRE_THREAD(lck, cnd, act)               \
    do {                                                 \
        OPAL_THREAD_LOCK((lck));                         \
        if (opal_debug_threads) {                        \
            opal_output(0, "Waiting for thread %s:%d",   \
                        __FILE__, __LINE__);             \
        }                                                \
        while (*(act)) {                                 \
            opal_condition_wait((cnd), (lck));           \
        }                                                \
        if (opal_debug_threads) {                        \
            opal_output(0, "Thread obtained %s:%d",      \
                        __FILE__, __LINE__);             \
        }                                                \
        *(act) = true;                                   \
    } while(0);
#else
#define OPAL_ACQUIRE_THREAD(lck, cnd, act)               \
    do {                                                 \
        OPAL_THREAD_LOCK((lck));                         \
        while (*(act)) {                                 \
            opal_condition_wait((cnd), (lck));           \
        }                                                \
        *(act) = true;                                   \
    } while(0);
#endif


#if OPAL_ENABLE_DEBUG
#define OPAL_RELEASE_THREAD(lck, cnd, act)              \
    do {                                                \
        if (opal_debug_threads) {                       \
            opal_output(0, "Releasing thread %s:%d",    \
                        __FILE__, __LINE__);            \
        }                                               \
        *(act) = false;                                 \
        opal_condition_broadcast((cnd));                \
        OPAL_THREAD_UNLOCK((lck));                      \
    } while(0);
#else
#define OPAL_RELEASE_THREAD(lck, cnd, act)              \
    do {                                                \
        *(act) = false;                                 \
        opal_condition_broadcast((cnd));                \
        OPAL_THREAD_UNLOCK((lck));                      \
    } while(0);
#endif


#define OPAL_WAKEUP_THREAD(cnd, act)        \
    do {                                    \
        *(act) = false;                     \
        opal_condition_broadcast((cnd));    \
    } while(0);

/* provide a macro for forward-proofing the shifting
 * of objects between libevent threads - at some point, we
 * may revamp that threading model */

/* post an object to another thread - for now, we
 * only have a memory barrier */
#define OPAL_POST_OBJECT(o)     opal_atomic_wmb()

/* acquire an object from another thread - for now,
 * we only have a memory barrier */
#define OPAL_ACQUIRE_OBJECT(o)  opal_atomic_rmb()



OPAL_DECLSPEC int  opal_thread_start(opal_thread_t *);
OPAL_DECLSPEC int  opal_thread_join(opal_thread_t *, void **thread_return);
OPAL_DECLSPEC bool opal_thread_self_compare(opal_thread_t*);
OPAL_DECLSPEC opal_thread_t *opal_thread_get_self(void);
OPAL_DECLSPEC void opal_thread_kill(opal_thread_t *, int sig);
OPAL_DECLSPEC void opal_thread_set_main(void);

END_C_DECLS

#endif /* OPAL_THREAD_H */
