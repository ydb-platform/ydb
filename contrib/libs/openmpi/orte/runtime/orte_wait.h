/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008      Institut National de Recherche en Informatique
 *                         et Automatique. All rights reserved.
 * Copyright (c) 2011      Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * Interface for waitpid / async notification of child death with the
 * libevent runtime system.
 */
#ifndef ORTE_WAIT_H
#define ORTE_WAIT_H

#include "orte_config.h"

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#include <time.h>
#if HAVE_SYS_TIME_H
#include <sys/time.h>
#endif

#include "opal/dss/dss.h"
#include "opal/util/output.h"
#include "opal/sys/atomic.h"
#include "opal/mca/event/event.h"

#include "orte/types.h"
#include "orte/mca/rml/rml_types.h"
#include "orte/runtime/orte_globals.h"
#include "orte/util/threads.h"

BEGIN_C_DECLS

/** typedef for callback function used in \c orte_wait_cb */
typedef void (*orte_wait_cbfunc_t)(int fd, short args, void* cb);

/* define a tracker */
typedef struct {
    opal_list_item_t super;
    opal_event_t ev;
    opal_event_base_t *evb;
    orte_proc_t *child;
    orte_wait_cbfunc_t cbfunc;
    void *cbdata;
} orte_wait_tracker_t;
OBJ_CLASS_DECLARATION(orte_wait_tracker_t);

/**
 * Disable / re-Enable SIGCHLD handler
 *
 * These functions have to be used after orte_wait_init was called.
 */

ORTE_DECLSPEC void orte_wait_enable(void);
ORTE_DECLSPEC void orte_wait_disable(void);

/**
 * Register a callback for process termination
 *
 * Register a callback for notification when this process causes a SIGCHLD.
 * \c waitpid() will have already been called on the process at this
 * time.
 */
ORTE_DECLSPEC void orte_wait_cb(orte_proc_t *proc, orte_wait_cbfunc_t callback,
                                opal_event_base_t *evb, void *data);

ORTE_DECLSPEC void orte_wait_cb_cancel(orte_proc_t *proc);


/* In a few places, we need to barrier until something happens
 * that changes a flag to indicate we can release - e.g., waiting
 * for a specific message to arrive. If no progress thread is running,
 * we cycle across opal_progress - however, if a progress thread
 * is active, then we need to just nanosleep to avoid cross-thread
 * confusion
 */
#define ORTE_WAIT_FOR_COMPLETION(flg)                                   \
    do {                                                                \
        opal_output_verbose(1, orte_progress_thread_debug,              \
                            "%s waiting on progress thread at %s:%d",   \
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),         \
                            __FILE__, __LINE__);                        \
        while ((flg)) {                                                 \
            /* provide a short quiet period so we                       \
             * don't hammer the cpu while waiting                       \
             */                                                         \
            struct timespec tp = {0, 100000};                           \
            nanosleep(&tp, NULL);                                       \
        }                                                               \
        ORTE_ACQUIRE_OBJECT(flg);                                       \
    }while(0);

/**
 * In a number of places within the code, we want to setup a timer
 * to detect when some procedure failed to complete. For example,
 * when we launch the daemons, we frequently have no way to directly
 * detect that a daemon failed to launch. Setting a timer allows us
 * to automatically fail out of the launch if we don't hear from a
 * daemon in some specified time window.
 *
 * Computing the amount of time to wait takes a few lines of code, but
 * this macro encapsulates those lines along with the timer event
 * definition just as a convenience. It also centralizes the
 * necessary checks to ensure that the microsecond field is always
 * less than 1M since some systems care about that, and to ensure
 * that the computed wait time doesn't exceed the desired max
 * wait
 *
 * NOTE: the callback function is responsible for releasing the timer
 * event back to the event pool!
 */
#define ORTE_DETECT_TIMEOUT(n, deltat, maxwait, cbfunc, cbd)                \
    do {                                                                    \
        orte_timer_t *tmp;                                                  \
        int timeout;                                                        \
        tmp =  OBJ_NEW(orte_timer_t);                                       \
        tmp->payload = (cbd);                                               \
        opal_event_evtimer_set(orte_event_base,                             \
                               tmp->ev, (cbfunc), tmp);                     \
        opal_event_set_priority(tmp->ev, ORTE_ERROR_PRI);                   \
        timeout = (deltat) * (n);                                           \
        if ((maxwait) > 0 && timeout > (maxwait)) {                         \
            timeout = (maxwait);                                            \
        }                                                                   \
        tmp->tv.tv_sec = timeout/1000000;                                   \
        tmp->tv.tv_usec = timeout%1000000;                                  \
        OPAL_OUTPUT_VERBOSE((1, orte_debug_output,                          \
                             "defining timeout: %ld sec %ld usec at %s:%d", \
                            (long)tmp->tv.tv_sec, (long)tmp->tv.tv_usec,    \
                            __FILE__, __LINE__));                           \
        ORTE_POST_OBJECT(tmp);                                              \
        opal_event_evtimer_add(tmp->ev, &tmp->tv);                          \
    }while(0);                                                              \


/**
 * There are places in the code where we just want to periodically
 * wakeup to do something, and then go back to sleep again. Setting
 * a timer allows us to do this
 *
 * NOTE: the callback function is responsible for releasing the timer
 * event back to the event pool when done! Otherwise, the finalize
 * function will take care of it.
 */
#define ORTE_TIMER_EVENT(sec, usec, cbfunc, pri)                                \
    do {                                                                        \
        orte_timer_t *tm;                                                       \
        tm = OBJ_NEW(orte_timer_t);                                             \
        opal_event_evtimer_set(orte_event_base,                                 \
                               tm->ev, (cbfunc), tm);                           \
        opal_event_set_priority(tm->ev, (pri));                                 \
        tm->tv.tv_sec = (sec) + (usec)/1000000;                                 \
        tm->tv.tv_usec = (usec) % 1000000;                                      \
        OPAL_OUTPUT_VERBOSE((1, orte_debug_output,                              \
                             "defining timer event: %ld sec %ld usec at %s:%d", \
                             (long)tm->tv.tv_sec, (long)tm->tv.tv_usec,         \
                             __FILE__, __LINE__));                              \
        ORTE_POST_OBJECT(tm);                                                   \
        opal_event_evtimer_add(tm->ev, &tm->tv);                                \
    }while(0);                                                                  \


/**
 * \internal
 *
 * Initialize the wait system (allocate mutexes, etc.)
 */
ORTE_DECLSPEC int orte_wait_init(void);

/**
 * \internal
 *
 * Finalize the wait system (deallocate mutexes, etc.)
 */
ORTE_DECLSPEC int orte_wait_finalize(void);

END_C_DECLS

#endif /* #ifndef ORTE_WAIT_H */
