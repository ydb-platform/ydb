/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2008 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2013 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2008      Institut National de Recherche en Informatique
 *                         et Automatique. All rights reserved.
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "orte_config.h"

#include <string.h>
#include <assert.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_QUEUE_H
#include <sys/queue.h>
#endif
#include <errno.h>
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#include <fcntl.h>
#include <stdlib.h>
#include <signal.h>
#include <stdio.h>
#include <sys/stat.h>
#ifdef HAVE_SYS_WAIT_H
#include <sys/wait.h>
#endif

#include "opal/dss/dss_types.h"
#include "opal/class/opal_object.h"
#include "opal/util/output.h"
#include "opal/class/opal_list.h"
#include "opal/mca/event/event.h"
#include "opal/threads/mutex.h"
#include "opal/threads/condition.h"
#include "opal/sys/atomic.h"

#include "orte/constants.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/util/name_fns.h"
#include "orte/util/threads.h"
#include "orte/runtime/orte_globals.h"

#include "orte/runtime/orte_wait.h"

/* Timer Object Declaration */
static void timer_const(orte_timer_t *tm)
{
    tm->ev = opal_event_alloc();
    tm->payload = NULL;
}
static void timer_dest(orte_timer_t *tm)
{
    opal_event_free(tm->ev);
}
OBJ_CLASS_INSTANCE(orte_timer_t,
                   opal_object_t,
                   timer_const,
                   timer_dest);


static void wccon(orte_wait_tracker_t *p)
{
    p->child = NULL;
    p->cbfunc = NULL;
    p->cbdata = NULL;
}
static void wcdes(orte_wait_tracker_t *p)
{
    if (NULL != p->child) {
        OBJ_RELEASE(p->child);
    }
}
OBJ_CLASS_INSTANCE(orte_wait_tracker_t,
                   opal_list_item_t,
                   wccon, wcdes);

/* Local Variables */
static opal_event_t handler;
static opal_list_t pending_cbs;

/* Local Function Prototypes */
static void wait_signal_callback(int fd, short event, void *arg);

/* Interface Functions */

void orte_wait_disable(void)
{
    opal_event_del(&handler);
}

void orte_wait_enable(void)
{
    opal_event_add(&handler, NULL);
}

int orte_wait_init(void)
{
    OBJ_CONSTRUCT(&pending_cbs, opal_list_t);

    opal_event_set(orte_event_base,
                   &handler, SIGCHLD, OPAL_EV_SIGNAL|OPAL_EV_PERSIST,
                   wait_signal_callback,
                   &handler);
    opal_event_set_priority(&handler, ORTE_SYS_PRI);

    opal_event_add(&handler, NULL);
    return ORTE_SUCCESS;
}


int orte_wait_finalize(void)
{
    opal_event_del(&handler);

    /* clear out the pending cbs */
    OPAL_LIST_DESTRUCT(&pending_cbs);

    return ORTE_SUCCESS;
}

/* this function *must* always be called from
 * within an event in the orte_event_base */
void orte_wait_cb(orte_proc_t *child, orte_wait_cbfunc_t callback,
                  opal_event_base_t *evb, void *data)
{
    orte_wait_tracker_t *t2;

    if (NULL == child || NULL == callback) {
        /* bozo protection */
        ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
        return;
    }

    /* see if this proc is still alive */
    if (!ORTE_FLAG_TEST(child, ORTE_PROC_FLAG_ALIVE)) {
        if (NULL != callback) {
            /* already heard this proc is dead, so just do the callback */
            t2 = OBJ_NEW(orte_wait_tracker_t);
            OBJ_RETAIN(child);  // protect against race conditions
            t2->child = child;
            t2->evb = evb;
            t2->cbfunc = callback;
            t2->cbdata = data;
            opal_event_set(t2->evb, &t2->ev, -1,
                           OPAL_EV_WRITE, t2->cbfunc, t2);
            opal_event_set_priority(&t2->ev, ORTE_MSG_PRI);
            opal_event_active(&t2->ev, OPAL_EV_WRITE, 1);
        }
        return;
    }

   /* we just override any existing registration */
    OPAL_LIST_FOREACH(t2, &pending_cbs, orte_wait_tracker_t) {
        if (t2->child == child) {
            t2->cbfunc = callback;
            t2->cbdata = data;
            return;
        }
    }
    /* get here if this is a new registration */
    t2 = OBJ_NEW(orte_wait_tracker_t);
    OBJ_RETAIN(child);  // protect against race conditions
    t2->child = child;
    t2->evb = evb;
    t2->cbfunc = callback;
    t2->cbdata = data;
    opal_list_append(&pending_cbs, &t2->super);
}

static void cancel_callback(int fd, short args, void *cbdata)
{
    orte_wait_tracker_t *trk = (orte_wait_tracker_t*)cbdata;
    orte_wait_tracker_t *t2;

    ORTE_ACQUIRE_OBJECT(trk);

    OPAL_LIST_FOREACH(t2, &pending_cbs, orte_wait_tracker_t) {
        if (t2->child == trk->child) {
            opal_list_remove_item(&pending_cbs, &t2->super);
            OBJ_RELEASE(t2);
            OBJ_RELEASE(trk);
            return;
        }
    }

    OBJ_RELEASE(trk);
}

void orte_wait_cb_cancel(orte_proc_t *child)
{
    orte_wait_tracker_t *trk;

    if (NULL == child) {
        /* bozo protection */
        ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
        return;
    }

    /* push this into the event library for handling */
    trk = OBJ_NEW(orte_wait_tracker_t);
    OBJ_RETAIN(child);  // protect against race conditions
    trk->child = child;
    ORTE_THREADSHIFT(trk, orte_event_base, cancel_callback, ORTE_SYS_PRI);
}


/* callback from the event library whenever a SIGCHLD is received */
static void wait_signal_callback(int fd, short event, void *arg)
{
    opal_event_t *signal = (opal_event_t*) arg;
    int status;
    pid_t pid;
    orte_wait_tracker_t *t2;

    ORTE_ACQUIRE_OBJECT(signal);

    if (SIGCHLD != OPAL_EVENT_SIGNAL(signal)) {
        return;
    }

    /* we can have multiple children leave but only get one
     * sigchild callback, so reap all the waitpids until we
     * don't get anything valid back */
    while (1) {
        pid = waitpid(-1, &status, WNOHANG);
        if (-1 == pid && EINTR == errno) {
            /* try it again */
            continue;
        }
        /* if we got garbage, then nothing we can do */
        if (pid <= 0) {
            return;
        }

        /* we are already in an event, so it is safe to access the list */
        OPAL_LIST_FOREACH(t2, &pending_cbs, orte_wait_tracker_t) {
            if (pid == t2->child->pid) {
                /* found it! */
                t2->child->exit_code = status;
                opal_list_remove_item(&pending_cbs, &t2->super);
                if (NULL != t2->cbfunc) {
                    opal_event_set(t2->evb, &t2->ev, -1,
                                   OPAL_EV_WRITE, t2->cbfunc, t2);
                    opal_event_set_priority(&t2->ev, ORTE_MSG_PRI);
                    opal_event_active(&t2->ev, OPAL_EV_WRITE, 1);
                } else {
                    OBJ_RELEASE(t2);
                }
                break;
            }
        }
    }
}
