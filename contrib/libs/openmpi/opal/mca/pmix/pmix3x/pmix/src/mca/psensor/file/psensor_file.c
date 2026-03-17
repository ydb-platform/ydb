/*
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2011-2012 Los Alamos National Security, LLC.
 *                         All rights reserved.
 *
 * Copyright (c) 2017-2018 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>
#include <src/include/types.h>
#include <pmix_common.h>

#include <stdio.h>
#include <stddef.h>
#include <ctype.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif
#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#ifdef HAVE_TIME_H
#include <time.h>
#endif
#include <sys/stat.h>
#include <sys/types.h>

#include "src/class/pmix_list.h"
#include "src/include/pmix_globals.h"
#include "src/util/error.h"
#include "src/util/output.h"
#include "src/util/show_help.h"

#include "src/mca/psensor/base/base.h"
#include "psensor_file.h"

/* declare the API functions */
static pmix_status_t start(pmix_peer_t *requestor, pmix_status_t error,
                           const pmix_info_t *monitor,
                           const pmix_info_t directives[], size_t ndirs);
static pmix_status_t stop(pmix_peer_t *requestor, char *id);

/* instantiate the module */
pmix_psensor_base_module_t pmix_psensor_file_module = {
    .start = start,
    .stop = stop
};

/* define a tracking object */
typedef struct {
    pmix_list_item_t super;
    pmix_peer_t *requestor;
    char *id;
    bool event_active;
    pmix_event_t ev;
    pmix_event_t cdev;
    struct timeval tv;
    int tick;
    char *file;
    bool file_size;
    bool file_access;
    bool file_mod;
    size_t last_size;
    time_t last_access;
    time_t last_mod;
    uint32_t ndrops;
    uint32_t nmisses;
    pmix_status_t error;
    pmix_data_range_t range;
    pmix_info_t *info;
    size_t ninfo;
} file_tracker_t;
static void ft_constructor(file_tracker_t *ft)
{
    ft->requestor = NULL;
    ft->id = NULL;
    ft->event_active = false;
    ft->tv.tv_sec = 0;
    ft->tv.tv_usec = 0;
    ft->tick = 0;
    ft->file_size = false;
    ft->file_access = false;
    ft->file_mod = false;
    ft->last_size = 0;
    ft->last_access = 0;
    ft->last_mod = 0;
    ft->ndrops = 0;
    ft->nmisses = 0;
    ft->error = PMIX_SUCCESS;
    ft->range = PMIX_RANGE_NAMESPACE;
    ft->info = NULL;
    ft->ninfo = 0;
}
static void ft_destructor(file_tracker_t *ft)
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
    if (NULL != ft->file) {
        free(ft->file);
    }
    if (NULL != ft->info) {
        PMIX_INFO_FREE(ft->info, ft->ninfo);
    }
}
PMIX_CLASS_INSTANCE(file_tracker_t,
                    pmix_list_item_t,
                    ft_constructor, ft_destructor);

/* define a local caddy */
typedef struct {
    pmix_object_t super;
    pmix_event_t ev;
    pmix_peer_t *requestor;
    char *id;
} file_caddy_t;
static void cd_con(file_caddy_t *p)
{
    p->requestor = NULL;
    p->id = NULL;
}
static void cd_des(file_caddy_t *p)
{
    if (NULL != (p->requestor)) {
        PMIX_RELEASE(p->requestor);
    }
    if (NULL != p->id) {
        free(p->id);
    }
}
PMIX_CLASS_INSTANCE(file_caddy_t,
                    pmix_object_t,
                    cd_con, cd_des);

static void file_sample(int sd, short args, void *cbdata);

static void add_tracker(int sd, short flags, void *cbdata)
{
    file_tracker_t *ft = (file_tracker_t*)cbdata;

    PMIX_ACQUIRE_OBJECT(fd);

    /* add the tracker to our list */
    pmix_list_append(&mca_psensor_file_component.trackers, &ft->super);

    /* setup the timer event */
    pmix_event_evtimer_set(pmix_psensor_base.evbase, &ft->ev,
                           file_sample, ft);
    pmix_event_evtimer_add(&ft->ev, &ft->tv);
    ft->event_active = true;
}

/*
 * Start monitoring of local processes
 */
static pmix_status_t start(pmix_peer_t *requestor, pmix_status_t error,
                           const pmix_info_t *monitor,
                           const pmix_info_t directives[], size_t ndirs)
{
    file_tracker_t *ft;
    size_t n;

    PMIX_OUTPUT_VERBOSE((1, pmix_psensor_base_framework.framework_output,
                         "[%s:%d] checking file monitoring for requestor %s:%d",
                         pmix_globals.myid.nspace, pmix_globals.myid.rank,
                         requestor->info->pname.nspace, requestor->info->pname.rank));

    /* if they didn't ask to monitor a file, then nothing for us to do */
    if (0 != strcmp(monitor->key, PMIX_MONITOR_FILE)) {
        return PMIX_ERR_TAKE_NEXT_OPTION;
    }

    /* setup to track this monitoring operation */
    ft = PMIX_NEW(file_tracker_t);
    PMIX_RETAIN(requestor);
    ft->requestor = requestor;
    ft->file = strdup(monitor->value.data.string);

    /* check the directives to see if what they want monitored */
    for (n=0; n < ndirs; n++) {
        if (0 == strcmp(directives[n].key, PMIX_MONITOR_FILE_SIZE)) {
            ft->file_size = PMIX_INFO_TRUE(&directives[n]);
        } else if (0 == strcmp(directives[n].key, PMIX_MONITOR_FILE_ACCESS)) {
            ft->file_access = PMIX_INFO_TRUE(&directives[n]);
        } else if (0 == strcmp(directives[n].key, PMIX_MONITOR_FILE_MODIFY)) {
            ft->file_mod = PMIX_INFO_TRUE(&directives[n]);
        } else if (0 == strcmp(directives[n].key, PMIX_MONITOR_FILE_DROPS)) {
            ft->ndrops = directives[n].value.data.uint32;
        } else if (0 == strcmp(directives[n].key, PMIX_MONITOR_FILE_CHECK_TIME)) {
            ft->tv.tv_sec = directives[n].value.data.uint32;
        } else if (0 == strcmp(directives[n].key, PMIX_RANGE)) {
            ft->range = directives[n].value.data.range;
        }
    }

    if (0 == ft->tv.tv_sec ||
        (!ft->file_size && !ft->file_access && !ft->file_mod)) {
        /* didn't specify a sample rate, or what should be sampled */
        PMIX_RELEASE(ft);
        return PMIX_ERR_BAD_PARAM;
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
    file_caddy_t *cd = (file_caddy_t*)cbdata;
    file_tracker_t *ft, *ftnext;

    PMIX_ACQUIRE_OBJECT(cd);

    /* remove the tracker from our list */
    PMIX_LIST_FOREACH_SAFE(ft, ftnext, &mca_psensor_file_component.trackers, file_tracker_t) {
        if (ft->requestor != cd->requestor) {
            continue;
        }
        if (NULL == cd->id ||
            (NULL != ft->id && 0 == strcmp(ft->id, cd->id))) {
            pmix_list_remove_item(&mca_psensor_file_component.trackers, &ft->super);
            PMIX_RELEASE(ft);
        }
    }
    PMIX_RELEASE(cd);
}

static pmix_status_t stop(pmix_peer_t *requestor, char *id)
{
    file_caddy_t *cd;

    cd = PMIX_NEW(file_caddy_t);
    PMIX_RETAIN(requestor);
    cd->requestor = requestor;
    if (NULL != id) {
        cd->id = strdup(id);
    }

    /* need to push into our event base to add this to our trackers */
    pmix_event_assign(&cd->ev, pmix_psensor_base.evbase, -1,
                      EV_WRITE, del_tracker, cd);
    PMIX_POST_OBJECT(cd);
    pmix_event_active(&cd->ev, EV_WRITE, 1);

    return PMIX_SUCCESS;
}

static void opcbfunc(pmix_status_t status, void *cbdata)
{
    file_tracker_t *ft = (file_tracker_t*)cbdata;

    PMIX_RELEASE(ft);
}

static void file_sample(int sd, short args, void *cbdata)
{
    file_tracker_t *ft = (file_tracker_t*)cbdata;
    struct stat buf;
    pmix_status_t rc;
    pmix_proc_t source;

    PMIX_ACQUIRE_OBJECT(ft);

    PMIX_OUTPUT_VERBOSE((1, pmix_psensor_base_framework.framework_output,
                         "[%s:%d] sampling file %s",
                         pmix_globals.myid.nspace, pmix_globals.myid.rank,
                         ft->file));

    /* stat the file and get its info */
    if (0 > stat(ft->file, &buf)) {
        /* cannot stat file */
        PMIX_OUTPUT_VERBOSE((1, pmix_psensor_base_framework.framework_output,
                             "[%s:%d] could not stat %s",
                             pmix_globals.myid.nspace, pmix_globals.myid.rank,
                             ft->file));
        /* re-add the timer, in case this file shows up */
        pmix_event_evtimer_add(&ft->ev, &ft->tv);
        return;
    }

    PMIX_OUTPUT_VERBOSE((1, pmix_psensor_base_framework.framework_output,
                         "[%s:%d] size %lu access %s\tmod %s",
                         pmix_globals.myid.nspace, pmix_globals.myid.rank,
                         (unsigned long)buf.st_size, ctime(&buf.st_atime), ctime(&buf.st_mtime)));

    if (ft->file_size) {
        if (buf.st_size == (int64_t)ft->last_size) {
            ft->nmisses++;
        } else {
            ft->nmisses = 0;
            ft->last_size = buf.st_size;
        }
    } else if (ft->file_access) {
        if (buf.st_atime == ft->last_access) {
            ft->nmisses++;
        } else {
            ft->nmisses = 0;
            ft->last_access = buf.st_atime;
        }
    } else if (ft->file_mod) {
        if (buf.st_mtime == ft->last_mod) {
            ft->nmisses++;
        } else {
            ft->nmisses = 0;
            ft->last_mod = buf.st_mtime;
        }
    }

    PMIX_OUTPUT_VERBOSE((1, pmix_psensor_base_framework.framework_output,
                         "[%s:%d] sampled file %s misses %d",
                         pmix_globals.myid.nspace, pmix_globals.myid.rank,
                         ft->file, ft->nmisses));

    if (ft->nmisses == ft->ndrops) {
        if (4 < pmix_output_get_verbosity(pmix_psensor_base_framework.framework_output)) {
            pmix_show_help("help-pmix-psensor-file.txt", "file-stalled", true,
                           ft->file, ft->last_size, ctime(&ft->last_access), ctime(&ft->last_mod));
        }
        /* stop monitoring this client */
        pmix_list_remove_item(&mca_psensor_file_component.trackers, &ft->super);
        /* generate an event */
        pmix_strncpy(source.nspace, ft->requestor->info->pname.nspace, PMIX_MAX_NSLEN);
        source.rank = ft->requestor->info->pname.rank;
        rc = PMIx_Notify_event(PMIX_MONITOR_FILE_ALERT, &source,
                               ft->range, ft->info, ft->ninfo, opcbfunc, ft);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
        }
        return;
    }

    /* re-add the timer */
    pmix_event_evtimer_add(&ft->ev, &ft->tv);
}
