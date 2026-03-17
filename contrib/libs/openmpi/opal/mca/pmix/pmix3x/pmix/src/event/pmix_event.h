/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
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
 * Copyright (c) 2015-2018 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef PMIX_EVENT_H
#define PMIX_EVENT_H

#include <src/include/pmix_config.h>
#include "src/include/types.h"
#include PMIX_EVENT_HEADER

#include <pmix_common.h>
#include "src/class/pmix_list.h"
#include "src/util/output.h"

 BEGIN_C_DECLS

#define PMIX_EVENT_ORDER_NONE       0x00
#define PMIX_EVENT_ORDER_FIRST      0x01
#define PMIX_EVENT_ORDER_LAST       0x02
#define PMIX_EVENT_ORDER_BEFORE     0x04
#define PMIX_EVENT_ORDER_AFTER      0x08
#define PMIX_EVENT_ORDER_PREPEND    0x10
#define PMIX_EVENT_ORDER_APPEND     0x20

/* define an internal attribute for marking that the
 * server processed an event before passing it up
 * to its host in case it comes back down - avoids
 * infinite loop */
#define PMIX_SERVER_INTERNAL_NOTIFY   "pmix.srvr.internal.notify"


/* define a struct for tracking registration ranges */
typedef struct {
    pmix_data_range_t range;
    pmix_proc_t *procs;
    size_t nprocs;
} pmix_range_trkr_t;

/* define a common struct for tracking event handlers */
typedef struct {
    pmix_list_item_t super;
    char *name;
    size_t index;
    uint8_t precedence;
    char *locator;
    pmix_proc_t source;  // who generated this event
    /* When registering for events, callers can specify
     * the range of sources from which they are willing
     * to receive notifications - e.g., for callers to
     * define different handlers for events coming from
     * the RM vs those coming from their peers. We use
     * the rng field to track these values upon registration.
     */
    pmix_range_trkr_t rng;
    /* For registration, we use the affected field to track
     * the range of procs that, if affected by the event,
     * should cause the handler to be called (subject, of
     * course, to any rng constraints).
     */
    pmix_proc_t *affected;
    size_t naffected;
    pmix_notification_fn_t evhdlr;
    void *cbobject;
    pmix_status_t *codes;
    size_t ncodes;
} pmix_event_hdlr_t;
PMIX_CLASS_DECLARATION(pmix_event_hdlr_t);

/* define an object for tracking status codes we are actively
 * registered to receive */
typedef struct {
    pmix_list_item_t super;
    pmix_status_t code;
    size_t nregs;
} pmix_active_code_t;
PMIX_CLASS_DECLARATION(pmix_active_code_t);

/* define an object for housing the different lists of events
 * we have registered so we can easily scan them in precedent
 * order when we get an event */
typedef struct {
    pmix_object_t super;
    size_t nhdlrs;
    pmix_event_hdlr_t *first;
    pmix_event_hdlr_t *last;
    pmix_list_t actives;
    pmix_list_t single_events;
    pmix_list_t multi_events;
    pmix_list_t default_events;
} pmix_events_t;
PMIX_CLASS_DECLARATION(pmix_events_t);

/* define an object for chaining event notifications thru
 * the local state machine. Each registered event handler
 * that asked to be notified for a given code is given a
 * chance to "see" the reported event, starting with
 * single-code handlers, then multi-code handlers, and
 * finally default handlers. This object provides a
 * means for us to relay the event across that chain
 */
typedef struct pmix_event_chain_t {
    pmix_list_item_t super;
    pmix_status_t status;
    pmix_event_t ev;
    bool timer_active;
    bool nondefault;
    bool endchain;
    pmix_proc_t source;
    pmix_data_range_t range;
    /* When generating events, callers can specify
     * the range of targets to receive notifications.
     */
    pmix_proc_t *targets;
    size_t ntargets;
    /* the processes that we affected by the event */
    pmix_proc_t *affected;
    size_t naffected;
    /* any info provided by the event generator */
    pmix_info_t *info;
    size_t ninfo;
    size_t nallocated;
    pmix_info_t *results;
    size_t nresults;
    pmix_event_hdlr_t *evhdlr;
    pmix_op_cbfunc_t final_cbfunc;
    void *final_cbdata;
} pmix_event_chain_t;
PMIX_CLASS_DECLARATION(pmix_event_chain_t);

/* prepare a chain for processing by cycling across provided
 * info structs and translating those supported by the event
 * system into the chain object*/
pmix_status_t pmix_prep_event_chain(pmix_event_chain_t *chain,
                                    const pmix_info_t *info, size_t ninfo,
                                    bool xfer);

/* invoke the error handler that is registered against the given
 * status, passing it the provided info on the procs that were
 * affected, plus any additional info provided by the server */
void pmix_invoke_local_event_hdlr(pmix_event_chain_t *chain);

bool pmix_notify_check_range(pmix_range_trkr_t *rng,
                             const pmix_proc_t *proc);

bool pmix_notify_check_affected(pmix_proc_t *interested, size_t ninterested,
                                pmix_proc_t *affected, size_t naffected);


/* invoke the server event notification handler */
pmix_status_t pmix_server_notify_client_of_event(pmix_status_t status,
                                                 const pmix_proc_t *source,
                                                 pmix_data_range_t range,
                                                 const pmix_info_t info[], size_t ninfo,
                                                 pmix_op_cbfunc_t cbfunc, void *cbdata);

void pmix_event_timeout_cb(int fd, short flags, void *arg);

#define PMIX_REPORT_EVENT(e, p, r, f)                                                   \
    do {                                                                                \
        pmix_event_chain_t *ch, *cp;                                                    \
        size_t n, ninfo;                                                                \
        pmix_info_t *info;                                                              \
        pmix_proc_t proc;                                                               \
                                                                                        \
        ch = NULL;                                                                      \
        /* see if we already have this event cached */                                  \
        PMIX_LIST_FOREACH(cp, &pmix_globals.cached_events, pmix_event_chain_t) {        \
            if (cp->status == (e)) {                                                    \
                ch = cp;                                                                \
                break;                                                                  \
            }                                                                           \
        }                                                                               \
        if (NULL == ch) {                                                               \
            /* nope - need to add it */                                                 \
            ch = PMIX_NEW(pmix_event_chain_t);                                          \
            ch->status = (e);                                                           \
            ch->range = (r);                                                            \
            PMIX_LOAD_PROCID(&ch->source, (p)->nptr->nspace,                            \
                             (p)->info->pname.rank);                                    \
            PMIX_PROC_CREATE(ch->affected, 1);                                          \
            ch->naffected = 1;                                                          \
            PMIX_LOAD_PROCID(ch->affected, (p)->nptr->nspace,                           \
                             (p)->info->pname.rank);                                    \
            /* if I'm a client or tool and this is my server, then we don't */          \
            /* set the targets - otherwise, we do */                                    \
            if (!PMIX_PROC_IS_SERVER(pmix_globals.mypeer) &&                            \
                !PMIX_CHECK_PROCID(&pmix_client_globals.myserver->info->pname,          \
                                  &(p)->info->pname)) {                                 \
                PMIX_PROC_CREATE(ch->targets, 1);                                       \
                ch->ntargets = 1;                                                       \
                PMIX_LOAD_PROCID(ch->targets, (p)->nptr->nspace, PMIX_RANK_WILDCARD);   \
            }                                                                           \
            /* if this is lost-connection-to-server, then we let it go to */            \
            /* the default event handler - otherwise, we don't */                       \
            if (PMIX_ERR_LOST_CONNECTION_TO_SERVER != (e) &&                            \
                PMIX_ERR_UNREACH != (e)) {                                              \
                ch->ninfo = 1;                                                          \
                ch->nallocated = 3;                                                     \
                PMIX_INFO_CREATE(ch->info, ch->nallocated);                             \
                /* mark for non-default handlers only */                                \
                PMIX_INFO_LOAD(&ch->info[0], PMIX_EVENT_NON_DEFAULT, NULL, PMIX_BOOL);  \
            } else {                                                                    \
                ch->nallocated = 2;                                                     \
                PMIX_INFO_CREATE(ch->info, ch->nallocated);                             \
            }                                                                           \
            ch->final_cbfunc = (f);                                                     \
            ch->final_cbdata = ch;                                                      \
            /* cache it */                                                              \
            pmix_list_append(&pmix_globals.cached_events, &ch->super);                  \
            ch->timer_active = true;                                                    \
            pmix_event_assign(&ch->ev, pmix_globals.evbase, -1, 0,                      \
                              pmix_event_timeout_cb, ch);                               \
            PMIX_POST_OBJECT(ch);                                                       \
            pmix_event_add(&ch->ev, &pmix_globals.event_window);                        \
        } else {                                                                        \
            /* add this peer to the array of sources */                                 \
            pmix_strncpy(proc.nspace, (p)->nptr->nspace, PMIX_MAX_NSLEN);               \
            proc.rank = (p)->info->pname.rank;                                          \
            ninfo = ch->nallocated + 1;                                                 \
            PMIX_INFO_CREATE(info, ninfo);                                              \
            /* must keep the hdlr name and return object at the end, so prepend */      \
            PMIX_INFO_LOAD(&info[0], PMIX_PROCID,                                       \
                           &proc, PMIX_PROC);                                           \
            for (n=0; n < ch->ninfo; n++) {                                             \
                PMIX_INFO_XFER(&info[n+1], &ch->info[n]);                               \
            }                                                                           \
            PMIX_INFO_FREE(ch->info, ch->nallocated);                                   \
            ch->nallocated = ninfo;                                                     \
            ch->info = info;                                                            \
            ch->ninfo = ninfo - 2;                                                      \
            /* reset the timer */                                                       \
            pmix_event_del(&ch->ev);                                                    \
            PMIX_POST_OBJECT(ch);                                                       \
            pmix_event_add(&ch->ev, &pmix_globals.event_window);                        \
        }                                                                               \
    } while(0)


 END_C_DECLS

#endif /* PMIX_EVENT_H */
