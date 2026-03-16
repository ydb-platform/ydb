/*
 * Copyright (c) 2014-2015 Intel, Inc.  All rights reserved.
 * Copyright (c) 2015 Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"
#include "opal/constants.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "opal/class/opal_list.h"
#include "opal/mca/event/event.h"
#include "opal/threads/threads.h"
#include "opal/util/error.h"
#include "opal/util/fd.h"

#include "opal/runtime/opal_progress_threads.h"


/* create a tracking object for progress threads */
typedef struct {
    opal_list_item_t super;

    int refcount;
    char *name;

    opal_event_base_t *ev_base;

    /* This will be set to false when it is time for the progress
       thread to exit */
    volatile bool ev_active;

    /* This event will always be set on the ev_base (so that the
       ev_base is not empty!) */
    opal_event_t block;

    bool engine_constructed;
    opal_thread_t engine;
} opal_progress_tracker_t;

static void tracker_constructor(opal_progress_tracker_t *p)
{
    p->refcount = 1;  // start at one since someone created it
    p->name = NULL;
    p->ev_base = NULL;
    p->ev_active = false;
    p->engine_constructed = false;
}

static void tracker_destructor(opal_progress_tracker_t *p)
{
    opal_event_del(&p->block);

    if (NULL != p->name) {
        free(p->name);
    }
    if (NULL != p->ev_base) {
        opal_event_base_free(p->ev_base);
    }
    if (p->engine_constructed) {
        OBJ_DESTRUCT(&p->engine);
    }
}

static OBJ_CLASS_INSTANCE(opal_progress_tracker_t,
                          opal_list_item_t,
                          tracker_constructor,
                          tracker_destructor);

static bool inited = false;
static opal_list_t tracking;
static struct timeval long_timeout = {
    .tv_sec = 3600,
    .tv_usec = 0
};
static const char *shared_thread_name = "OPAL-wide async progress thread";

/*
 * If this event is fired, just restart it so that this event base
 * continues to have something to block on.
 */
static void dummy_timeout_cb(int fd, short args, void *cbdata)
{
    opal_progress_tracker_t *trk = (opal_progress_tracker_t*)cbdata;

    opal_event_add(&trk->block, &long_timeout);
}

/*
 * Main for the progress thread
 */
static void* progress_engine(opal_object_t *obj)
{
    opal_thread_t *t = (opal_thread_t*)obj;
    opal_progress_tracker_t *trk = (opal_progress_tracker_t*)t->t_arg;

    while (trk->ev_active) {
        opal_event_loop(trk->ev_base, OPAL_EVLOOP_ONCE);
    }

    return OPAL_THREAD_CANCELLED;
}

static void stop_progress_engine(opal_progress_tracker_t *trk)
{
    assert(trk->ev_active);
    trk->ev_active = false;

    /* break the event loop - this will cause the loop to exit upon
       completion of any current event */
    opal_event_base_loopbreak(trk->ev_base);

    opal_thread_join(&trk->engine, NULL);
}

static int start_progress_engine(opal_progress_tracker_t *trk)
{
    assert(!trk->ev_active);
    trk->ev_active = true;

    /* fork off a thread to progress it */
    trk->engine.t_run = progress_engine;
    trk->engine.t_arg = trk;

    int rc = opal_thread_start(&trk->engine);
    if (OPAL_SUCCESS != rc) {
        OPAL_ERROR_LOG(rc);
    }

    return rc;
}

opal_event_base_t *opal_progress_thread_init(const char *name)
{
    opal_progress_tracker_t *trk;
    int rc;

    if (!inited) {
        OBJ_CONSTRUCT(&tracking, opal_list_t);
        inited = true;
    }

    if (NULL == name) {
        name = shared_thread_name;
    }

    /* check if we already have this thread */
    OPAL_LIST_FOREACH(trk, &tracking, opal_progress_tracker_t) {
        if (0 == strcmp(name, trk->name)) {
            /* we do, so up the refcount on it */
            ++trk->refcount;
            /* return the existing base */
            return trk->ev_base;
        }
    }

    trk = OBJ_NEW(opal_progress_tracker_t);
    if (NULL == trk) {
        OPAL_ERROR_LOG(OPAL_ERR_OUT_OF_RESOURCE);
        return NULL;
    }

    trk->name = strdup(name);
    if (NULL == trk->name) {
        OPAL_ERROR_LOG(OPAL_ERR_OUT_OF_RESOURCE);
        OBJ_RELEASE(trk);
        return NULL;
    }

    if (NULL == (trk->ev_base = opal_event_base_create())) {
        OPAL_ERROR_LOG(OPAL_ERR_OUT_OF_RESOURCE);
        OBJ_RELEASE(trk);
        return NULL;
    }

    /* add an event to the new event base (if there are no events,
       opal_event_loop() will return immediately) */
    opal_event_set(trk->ev_base, &trk->block, -1, OPAL_EV_PERSIST,
                   dummy_timeout_cb, trk);
    opal_event_add(&trk->block, &long_timeout);

    /* construct the thread object */
    OBJ_CONSTRUCT(&trk->engine, opal_thread_t);
    trk->engine_constructed = true;
    if (OPAL_SUCCESS != (rc = start_progress_engine(trk))) {
        OPAL_ERROR_LOG(rc);
        OBJ_RELEASE(trk);
        return NULL;
    }
    opal_list_append(&tracking, &trk->super);

    return trk->ev_base;
}

int opal_progress_thread_finalize(const char *name)
{
    opal_progress_tracker_t *trk;

    if (!inited) {
        /* nothing we can do */
        return OPAL_ERR_NOT_FOUND;
    }

    if (NULL == name) {
        name = shared_thread_name;
    }

    /* find the specified engine */
    OPAL_LIST_FOREACH(trk, &tracking, opal_progress_tracker_t) {
        if (0 == strcmp(name, trk->name)) {
            /* decrement the refcount */
            --trk->refcount;

            /* If the refcount is still above 0, we're done here */
            if (trk->refcount > 0) {
                return OPAL_SUCCESS;
            }

            /* If the progress thread is active, stop it */
            if (trk->ev_active) {
                stop_progress_engine(trk);
            }

            opal_list_remove_item(&tracking, &trk->super);
            OBJ_RELEASE(trk);
            return OPAL_SUCCESS;
        }
    }

    return OPAL_ERR_NOT_FOUND;
}

/*
 * Stop the progress thread, but don't delete the tracker (or event base)
 */
int opal_progress_thread_pause(const char *name)
{
    opal_progress_tracker_t *trk;

    if (!inited) {
        /* nothing we can do */
        return OPAL_ERR_NOT_FOUND;
    }

    if (NULL == name) {
        name = shared_thread_name;
    }

    /* find the specified engine */
    OPAL_LIST_FOREACH(trk, &tracking, opal_progress_tracker_t) {
        if (0 == strcmp(name, trk->name)) {
            if (trk->ev_active) {
                stop_progress_engine(trk);
            }

            return OPAL_SUCCESS;
        }
    }

    return OPAL_ERR_NOT_FOUND;
}

int opal_progress_thread_resume(const char *name)
{
    opal_progress_tracker_t *trk;

    if (!inited) {
        /* nothing we can do */
        return OPAL_ERR_NOT_FOUND;
    }

    if (NULL == name) {
        name = shared_thread_name;
    }

    /* find the specified engine */
    OPAL_LIST_FOREACH(trk, &tracking, opal_progress_tracker_t) {
        if (0 == strcmp(name, trk->name)) {
            if (trk->ev_active) {
                return OPAL_ERR_RESOURCE_BUSY;
            }

            return start_progress_engine(trk);
        }
    }

    return OPAL_ERR_NOT_FOUND;
}
