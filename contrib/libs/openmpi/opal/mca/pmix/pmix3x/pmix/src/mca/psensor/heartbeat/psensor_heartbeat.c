/*
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011-2012 Los Alamos National Security, LLC.  All rights
 *                         reserved.
  *
 * Copyright (c) 2017-2018 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>
#include <pmix_common.h>

#include <errno.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */
#ifdef HAVE_STRING_H
#include <string.h>
#endif  /* HAVE_STRING_H */
#include <stdio.h>
#include <pthread.h>
#include PMIX_EVENT_HEADER

#include "src/util/argv.h"
#include "src/util/error.h"
#include "src/util/output.h"
#include "src/util/show_help.h"
#include "src/include/pmix_globals.h"
#include "src/mca/ptl/base/base.h"

#include "src/mca/psensor/base/base.h"
#include "psensor_heartbeat.h"

/* declare the API functions */
static pmix_status_t heartbeat_start(pmix_peer_t *requestor, pmix_status_t error,
                                     const pmix_info_t *monitor,
                                     const pmix_info_t directives[], size_t ndirs);
static pmix_status_t heartbeat_stop(pmix_peer_t *requestor, char *id);

/* instantiate the module */
pmix_psensor_base_module_t pmix_psensor_heartbeat_module = {
    .start = heartbeat_start,
    .stop = heartbeat_stop
};

/* tracker object */
typedef struct {
    pmix_list_item_t super;
    pmix_peer_t *requestor;
    char *id;
    bool event_active;
    pmix_event_t ev;
    pmix_event_t cdev;
    struct timeval tv;
    uint32_t nbeats;
    uint32_t ndrops;
    uint32_t nmissed;
    pmix_status_t error;
    pmix_data_range_t range;
    pmix_info_t *info;
    size_t ninfo;
    bool stopped;
} pmix_heartbeat_trkr_t;

static void ft_constructor(pmix_heartbeat_trkr_t *ft)
{
    ft->requestor = NULL;
    ft->id = NULL;
    ft->event_active = false;
    ft->tv.tv_sec = 0;
    ft->tv.tv_usec = 0;
    ft->nbeats = 0;
    ft->ndrops = 0;
    ft->nmissed = 0;
    ft->error = PMIX_SUCCESS;
    ft->range = PMIX_RANGE_NAMESPACE;
    ft->info = NULL;
    ft->ninfo = 0;
    ft->stopped = false;
}
static void ft_destructor(pmix_heartbeat_trkr_t *ft)
{
    if (NULL != ft->requestor) {
        PMIX_RELEASE(ft->requestor);
    }
    if (NULL != ft->id) {
        free(ft->id);
    }
    if (ft->event_active) {
        pmix_event_del(&ft->ev);
    }
    if (NULL != ft->info) {
        PMIX_INFO_FREE(ft->info, ft->ninfo);
    }
}
PMIX_CLASS_INSTANCE(pmix_heartbeat_trkr_t,
                    pmix_list_item_t,
                    ft_constructor, ft_destructor);

/* define a local caddy */
typedef struct {
    pmix_object_t super;
    pmix_event_t ev;
    pmix_peer_t *requestor;
    char *id;
} heartbeat_caddy_t;
static void cd_con(heartbeat_caddy_t *p)
{
    p->requestor = NULL;
    p->id = NULL;
}
static void cd_des(heartbeat_caddy_t *p)
{
    if (NULL != (p->requestor)) {
        PMIX_RELEASE(p->requestor);
    }
    if (NULL != p->id) {
        free(p->id);
    }
}
PMIX_CLASS_INSTANCE(heartbeat_caddy_t,
                    pmix_object_t,
                    cd_con, cd_des);

typedef struct {
    pmix_object_t super;
    pmix_event_t ev;
    pmix_peer_t *peer;
} pmix_psensor_beat_t;

static void bcon(pmix_psensor_beat_t *p)
{
    p->peer = NULL;
}
static void bdes(pmix_psensor_beat_t *p)
{
    if (NULL != p->peer) {
        PMIX_RELEASE(p->peer);
    }
}
PMIX_CLASS_INSTANCE(pmix_psensor_beat_t,
                    pmix_object_t,
                    bcon, bdes);

static void check_heartbeat(int fd, short dummy, void *arg);

static void add_tracker(int sd, short flags, void *cbdata)
{
    pmix_heartbeat_trkr_t *ft = (pmix_heartbeat_trkr_t*)cbdata;

    PMIX_ACQUIRE_OBJECT(ft);

    /* add the tracker to our list */
    pmix_list_append(&mca_psensor_heartbeat_component.trackers, &ft->super);

    /* setup the timer event */
    pmix_event_evtimer_set(pmix_psensor_base.evbase, &ft->ev,
                           check_heartbeat, ft);
    pmix_event_evtimer_add(&ft->ev, &ft->tv);
    ft->event_active = true;
}

static pmix_status_t heartbeat_start(pmix_peer_t *requestor, pmix_status_t error,
                                     const pmix_info_t *monitor,
                                     const pmix_info_t directives[], size_t ndirs)
{
    pmix_heartbeat_trkr_t *ft;
    size_t n;
    pmix_ptl_posted_recv_t *rcv;

    PMIX_OUTPUT_VERBOSE((1, pmix_psensor_base_framework.framework_output,
                         "[%s:%d] checking heartbeat monitoring for requestor %s:%d",
                         pmix_globals.myid.nspace, pmix_globals.myid.rank,
                         requestor->info->pname.nspace, requestor->info->pname.rank));

    /* if they didn't ask for heartbeats, then nothing for us to do */
    if (0 != strcmp(monitor->key, PMIX_MONITOR_HEARTBEAT)) {
        return PMIX_ERR_TAKE_NEXT_OPTION;
    }

    /* setup to track this monitoring operation */
    ft = PMIX_NEW(pmix_heartbeat_trkr_t);
    PMIX_RETAIN(requestor);
    ft->requestor = requestor;
    ft->error = error;

    /* check the directives to see what they want monitored */
    for (n=0; n < ndirs; n++) {
        if (0 == strcmp(directives[n].key, PMIX_MONITOR_HEARTBEAT_TIME)) {
            ft->tv.tv_sec = directives[n].value.data.uint32;
        } else if (0 == strcmp(directives[n].key, PMIX_MONITOR_HEARTBEAT_DROPS)) {
            ft->ndrops = directives[n].value.data.uint32;
        } else if (0 == strcmp(directives[n].key, PMIX_RANGE)) {
            ft->range = directives[n].value.data.range;
        }
    }

    if (0 == ft->tv.tv_sec) {
        /* didn't specify a sample rate, or what should be sampled */
        PMIX_RELEASE(ft);
        return PMIX_ERR_BAD_PARAM;
    }

    /* if the recv hasn't been posted, so so now */
    if (!mca_psensor_heartbeat_component.recv_active) {
        /* setup to receive heartbeats */
        rcv = PMIX_NEW(pmix_ptl_posted_recv_t);
        rcv->tag = PMIX_PTL_TAG_HEARTBEAT;
        rcv->cbfunc = pmix_psensor_heartbeat_recv_beats;
        /* add it to the beginning of the list of recvs */
        pmix_list_prepend(&pmix_ptl_globals.posted_recvs, &rcv->super);
        mca_psensor_heartbeat_component.recv_active = true;
    }

    /* need to push into our event base to add this to our trackers */
    pmix_event_assign(&ft->cdev, pmix_psensor_base.evbase, -1,
                      EV_WRITE, add_tracker, ft);
    PMIX_POST_OBJECT(ft);
    pmix_event_active(&ft->cdev, EV_WRITE, 1);

    return PMIX_SUCCESS;
}

static void del_tracker(int sd, short flags, void *cbdata)
{
    heartbeat_caddy_t *cd = (heartbeat_caddy_t*)cbdata;
    pmix_heartbeat_trkr_t *ft, *ftnext;

    PMIX_ACQUIRE_OBJECT(cd);

    /* remove the tracker from our list */
    PMIX_LIST_FOREACH_SAFE(ft, ftnext, &mca_psensor_heartbeat_component.trackers, pmix_heartbeat_trkr_t) {
        if (ft->requestor != cd->requestor) {
            continue;
        }
        if (NULL == cd->id ||
            (NULL != ft->id && 0 == strcmp(ft->id, cd->id))) {
            pmix_list_remove_item(&mca_psensor_heartbeat_component.trackers, &ft->super);
            PMIX_RELEASE(ft);
        }
    }
    PMIX_RELEASE(cd);
}

static pmix_status_t heartbeat_stop(pmix_peer_t *requestor, char *id)
{
    heartbeat_caddy_t *cd;

    cd = PMIX_NEW(heartbeat_caddy_t);
    PMIX_RETAIN(requestor);
    cd->requestor = requestor;
    if (NULL != id) {
        cd->id = strdup(id);
    }

    /* need to push into our event base to remove this from our trackers */
    pmix_event_assign(&cd->ev, pmix_psensor_base.evbase, -1,
                      EV_WRITE, del_tracker, cd);
    PMIX_POST_OBJECT(cd);
    pmix_event_active(&cd->ev, EV_WRITE, 1);

    return PMIX_SUCCESS;
}

static void opcbfunc(pmix_status_t status, void *cbdata)
{
    pmix_heartbeat_trkr_t *ft = (pmix_heartbeat_trkr_t*)cbdata;

    PMIX_RELEASE(ft);  // maintain accounting
}

/* this function automatically gets periodically called
 * by the event library so we can check on the state
 * of the various procs we are monitoring
 */
static void check_heartbeat(int fd, short dummy, void *cbdata)
{
    pmix_heartbeat_trkr_t *ft = (pmix_heartbeat_trkr_t*)cbdata;
    pmix_status_t rc;
    pmix_proc_t source;

    PMIX_ACQUIRE_OBJECT(ft);

    PMIX_OUTPUT_VERBOSE((1, pmix_psensor_base_framework.framework_output,
                         "[%s:%d] sensor:check_heartbeat for proc %s:%d",
                         pmix_globals.myid.nspace, pmix_globals.myid.rank,
                        ft->requestor->info->pname.nspace, ft->requestor->info->pname.rank));

    if (0 == ft->nbeats && !ft->stopped) {
        /* no heartbeat recvd in last window */
        PMIX_OUTPUT_VERBOSE((1, pmix_psensor_base_framework.framework_output,
                             "[%s:%d] sensor:check_heartbeat failed for proc %s:%d",
                             pmix_globals.myid.nspace, pmix_globals.myid.rank,
                             ft->requestor->info->pname.nspace, ft->requestor->info->pname.rank));
        /* generate an event */
        pmix_strncpy(source.nspace, ft->requestor->info->pname.nspace, PMIX_MAX_NSLEN);
        source.rank = ft->requestor->info->pname.rank;
        /* ensure the tracker remains throughout the process */
        PMIX_RETAIN(ft);
        /* mark that the process appears stopped so we don't
         * continue to report it */
        ft->stopped = true;
        rc = PMIx_Notify_event(PMIX_MONITOR_HEARTBEAT_ALERT, &source,
                               ft->range, ft->info, ft->ninfo, opcbfunc, ft);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
        }
    } else {
        PMIX_OUTPUT_VERBOSE((1, pmix_psensor_base_framework.framework_output,
                             "[%s:%d] sensor:check_heartbeat detected %d beats for proc %s:%d",
                             pmix_globals.myid.nspace, pmix_globals.myid.rank, ft->nbeats,
                             ft->requestor->info->pname.nspace, ft->requestor->info->pname.rank));
    }
    /* reset for next period */
    ft->nbeats = 0;

    /* reset the timer */
    pmix_event_evtimer_add(&ft->ev, &ft->tv);
}

static void add_beat(int sd, short args, void *cbdata)
{
    pmix_psensor_beat_t *b = (pmix_psensor_beat_t*)cbdata;
    pmix_heartbeat_trkr_t *ft;

    PMIX_ACQUIRE_OBJECT(b);

    /* find this peer in our trackers */
    PMIX_LIST_FOREACH(ft, &mca_psensor_heartbeat_component.trackers, pmix_heartbeat_trkr_t) {
        if (ft->requestor == b->peer) {
            /* increment the beat count */
            ++ft->nbeats;
            /* ensure we know that the proc is alive */
            ft->stopped = false;
            break;
        }
    }

    PMIX_RELEASE(b);
}

void pmix_psensor_heartbeat_recv_beats(struct pmix_peer_t *peer,
                                       pmix_ptl_hdr_t *hdr,
                                       pmix_buffer_t *buf, void *cbdata)
{
    pmix_psensor_beat_t *b;

    b = PMIX_NEW(pmix_psensor_beat_t);
    PMIX_RETAIN(peer);
    b->peer = peer;

    /* shift this to our thread for processing */
    pmix_event_assign(&b->ev, pmix_psensor_base.evbase, -1,
                      EV_WRITE, add_beat, b);
    PMIX_POST_OBJECT(b);
    pmix_event_active(&b->ev, EV_WRITE, 1);
}
