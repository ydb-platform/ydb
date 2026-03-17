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
 * Copyright (c) 2006-2018 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2015-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 *
 * Copyright (c) 2018      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#ifdef HAVE_SCHED_H
#include <sched.h>
#endif

#include "opal/runtime/opal_progress.h"
#include "opal/mca/event/event.h"
#include "opal/mca/base/mca_base_var.h"
#include "opal/constants.h"
#include "opal/mca/timer/base/base.h"
#include "opal/util/output.h"
#include "opal/runtime/opal_params.h"

#define OPAL_PROGRESS_USE_TIMERS (OPAL_TIMER_CYCLE_SUPPORTED || OPAL_TIMER_USEC_SUPPORTED)
#define OPAL_PROGRESS_ONLY_USEC_NATIVE (OPAL_TIMER_USEC_NATIVE && !OPAL_TIMER_CYCLE_NATIVE)

#if OPAL_ENABLE_DEBUG
bool opal_progress_debug = false;
#endif

/*
 * default parameters
 */
static int opal_progress_event_flag = OPAL_EVLOOP_ONCE | OPAL_EVLOOP_NONBLOCK;
int opal_progress_spin_count = 10000;


/*
 * Local variables
 */
static opal_atomic_lock_t progress_lock;

/* callbacks to progress */
static volatile opal_progress_callback_t *callbacks = NULL;
static size_t callbacks_len = 0;
static size_t callbacks_size = 0;

static volatile opal_progress_callback_t *callbacks_lp = NULL;
static size_t callbacks_lp_len = 0;
static size_t callbacks_lp_size = 0;

/* do we want to call sched_yield() if nothing happened */
bool opal_progress_yield_when_idle = false;

#if OPAL_PROGRESS_USE_TIMERS
static opal_timer_t event_progress_last_time = 0;
static opal_timer_t event_progress_delta = 0;
#else
/* current count down until we tick the event library */
static int32_t event_progress_counter = 0;
/* reset value for counter when it hits 0 */
static int32_t event_progress_delta = 0;
#endif
/* users of the event library from MPI cause the tick rate to
   be every time */
static int32_t num_event_users = 0;

#if OPAL_ENABLE_DEBUG
static int debug_output = -1;
#endif

/**
 * Fake callback used for threading purpose when one thread
 * progesses callbacks while another unregister somes. The root
 * of the problem is that we allow modifications of the callback
 * array directly from the callbacks themselves. Now if
 * writing a pointer is atomic, we should not have any more
 * problems.
 */
static int fake_cb(void) { return 0; }

static int _opal_progress_unregister (opal_progress_callback_t cb, volatile opal_progress_callback_t *callback_array,
                                      size_t *callback_array_len);

/* init the progress engine - called from orte_init */
int
opal_progress_init(void)
{
    /* reentrant issues */
    opal_atomic_lock_init(&progress_lock, OPAL_ATOMIC_LOCK_UNLOCKED);

    /* set the event tick rate */
    opal_progress_set_event_poll_rate(10000);

#if OPAL_ENABLE_DEBUG
    if (opal_progress_debug) {
       debug_output = opal_output_open(NULL);
    }
#endif

    callbacks_size = callbacks_lp_size = 8;

    callbacks = malloc (callbacks_size * sizeof (callbacks[0]));
    callbacks_lp = malloc (callbacks_lp_size * sizeof (callbacks_lp[0]));

    if (NULL == callbacks || NULL == callbacks_lp) {
        free ((void *) callbacks);
        free ((void *) callbacks_lp);
        callbacks_size = callbacks_lp_size = 0;
        callbacks = callbacks_lp = NULL;
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    for (size_t i = 0 ; i < callbacks_size ; ++i) {
        callbacks[i] = fake_cb;
    }

    for (size_t i = 0 ; i < callbacks_lp_size ; ++i) {
        callbacks_lp[i] = fake_cb;
    }

    OPAL_OUTPUT((debug_output, "progress: initialized event flag to: %x",
                 opal_progress_event_flag));
    OPAL_OUTPUT((debug_output, "progress: initialized yield_when_idle to: %s",
                 opal_progress_yield_when_idle ? "true" : "false"));
    OPAL_OUTPUT((debug_output, "progress: initialized num users to: %d",
                 num_event_users));
    OPAL_OUTPUT((debug_output, "progress: initialized poll rate to: %ld",
                 (long) event_progress_delta));

    return OPAL_SUCCESS;
}


int
opal_progress_finalize(void)
{
    /* free memory associated with the callbacks */
    opal_atomic_lock(&progress_lock);

    callbacks_len = 0;
    callbacks_size = 0;
    free ((void *) callbacks);
    callbacks = NULL;

    callbacks_lp_len = 0;
    callbacks_lp_size = 0;
    free ((void *) callbacks_lp);
    callbacks_lp = NULL;

    opal_atomic_unlock(&progress_lock);

    return OPAL_SUCCESS;
}

static int opal_progress_events(void)
{
    static volatile int32_t lock = 0;
    int events = 0;

    if( opal_progress_event_flag != 0 && !OPAL_THREAD_SWAP_32(&lock, 1) ) {
#if OPAL_HAVE_WORKING_EVENTOPS
#if OPAL_PROGRESS_USE_TIMERS
#if OPAL_PROGRESS_ONLY_USEC_NATIVE
        opal_timer_t now = opal_timer_base_get_usec();
#else
        opal_timer_t now = opal_timer_base_get_cycles();
#endif  /* OPAL_PROGRESS_ONLY_USEC_NATIVE */
    /* trip the event library if we've reached our tick rate and we are
       enabled */
        if (now - event_progress_last_time > event_progress_delta ) {
                event_progress_last_time = (num_event_users > 0) ?
                    now - event_progress_delta : now;

                events += opal_event_loop(opal_sync_event_base, opal_progress_event_flag);
        }

#else /* OPAL_PROGRESS_USE_TIMERS */
    /* trip the event library if we've reached our tick rate and we are
       enabled */
        if (OPAL_THREAD_ADD_FETCH32(&event_progress_counter, -1) <= 0 ) {
                event_progress_counter =
                    (num_event_users > 0) ? 0 : event_progress_delta;
                events += opal_event_loop(opal_sync_event_base, opal_progress_event_flag);
        }
#endif /* OPAL_PROGRESS_USE_TIMERS */

#endif /* OPAL_HAVE_WORKING_EVENTOPS */
        lock = 0;
    }

    return events;
}

/*
 * Progress the event library and any functions that have registered to
 * be called.  We don't propogate errors from the progress functions,
 * so no action is taken if they return failures.  The functions are
 * expected to return the number of events progressed, to determine
 * whether or not we should call sched_yield() during MPI progress.
 * This is only losely tracked, as an error return can cause the number
 * of progressed events to appear lower than it actually is.  We don't
 * care, as the cost of that happening is far outweighed by the cost
 * of the if checks (they were resulting in bad pipe stalling behavior)
 */
void
opal_progress(void)
{
    static uint32_t num_calls = 0;
    size_t i;
    int events = 0;

    /* progress all registered callbacks */
    for (i = 0 ; i < callbacks_len ; ++i) {
        events += (callbacks[i])();
    }

    /* Run low priority callbacks and events once every 8 calls to opal_progress().
     * Even though "num_calls" can be modified by multiple threads, we do not use
     * atomic operations here, for performance reasons. In case of a race, the
     * number of calls may be inaccurate, but since it will eventually be incremented,
     * it's not a problem.
     */
    if (((num_calls++) & 0x7) == 0) {
        for (i = 0 ; i < callbacks_lp_len ; ++i) {
            events += (callbacks_lp[i])();
        }

        opal_progress_events();
    } else if (num_event_users > 0) {
        opal_progress_events();
    }

#if OPAL_HAVE_SCHED_YIELD
    if (opal_progress_yield_when_idle && events <= 0) {
        /* If there is nothing to do - yield the processor - otherwise
         * we could consume the processor for the entire time slice. If
         * the processor is oversubscribed - this will result in a best-case
         * latency equivalent to the time-slice.
         */
        sched_yield();
    }
#endif  /* defined(HAVE_SCHED_YIELD) */
}


int
opal_progress_set_event_flag(int flag)
{
    int tmp = opal_progress_event_flag;
    opal_progress_event_flag = flag;

    OPAL_OUTPUT((debug_output, "progress: set_event_flag setting to %d", flag));

    return tmp;
}


void
opal_progress_event_users_increment(void)
{
#if OPAL_ENABLE_DEBUG
    int32_t val;
    val = opal_atomic_add_fetch_32(&num_event_users, 1);

    OPAL_OUTPUT((debug_output, "progress: event_users_increment setting count to %d", val));
#else
    (void)opal_atomic_add_fetch_32(&num_event_users, 1);
#endif

#if OPAL_PROGRESS_USE_TIMERS
    /* force an update next round (we'll be past the delta) */
    event_progress_last_time -= event_progress_delta;
#else
    /* always reset the tick rate - can't hurt */
    event_progress_counter = 0;
#endif
}


void
opal_progress_event_users_decrement(void)
{
#if OPAL_ENABLE_DEBUG || ! OPAL_PROGRESS_USE_TIMERS
    int32_t val;
    val = opal_atomic_sub_fetch_32(&num_event_users, 1);

    OPAL_OUTPUT((debug_output, "progress: event_users_decrement setting count to %d", val));
#else
    (void)opal_atomic_sub_fetch_32(&num_event_users, 1);
#endif

#if !OPAL_PROGRESS_USE_TIMERS
   /* start now in delaying if it's easy */
   if (val >= 0) {
       event_progress_counter = event_progress_delta;
   }
#endif
}


bool
opal_progress_set_yield_when_idle(bool yieldopt)
{
    bool tmp = opal_progress_yield_when_idle;
    opal_progress_yield_when_idle = (yieldopt) ? 1 : 0;

    OPAL_OUTPUT((debug_output, "progress: progress_set_yield_when_idle to %s",
                                    opal_progress_yield_when_idle ? "true" : "false"));

    return tmp;
}


void
opal_progress_set_event_poll_rate(int polltime)
{
    OPAL_OUTPUT((debug_output, "progress: progress_set_event_poll_rate(%d)", polltime));

#if OPAL_PROGRESS_USE_TIMERS
    event_progress_delta = 0;
#  if OPAL_PROGRESS_ONLY_USEC_NATIVE
    event_progress_last_time = opal_timer_base_get_usec();
#  else
    event_progress_last_time = opal_timer_base_get_cycles();
#  endif
#else
    event_progress_counter = event_progress_delta = 0;
#endif

    if (polltime == 0) {
#if OPAL_PROGRESS_USE_TIMERS
        /* user specified as never tick - tick once per minute */
        event_progress_delta = 60 * 1000000;
#else
        /* user specified as never tick - don't count often */
        event_progress_delta = INT_MAX;
#endif
    } else {
#if OPAL_PROGRESS_USE_TIMERS
        event_progress_delta = polltime;
#else
        /* subtract one so that we can do post-fix subtraction
           in the inner loop and go faster */
        event_progress_delta = polltime - 1;
#endif
    }

#if OPAL_PROGRESS_USE_TIMERS && !OPAL_PROGRESS_ONLY_USEC_NATIVE
    /*  going to use cycles for counter.  Adjust specified usec into cycles */
    event_progress_delta = event_progress_delta * opal_timer_base_get_freq() / 1000000;
#endif
}

static int opal_progress_find_cb (opal_progress_callback_t cb, volatile opal_progress_callback_t *cbs,
                                     size_t cbs_len)
{
    for (size_t i = 0 ; i < cbs_len ; ++i) {
        if (cbs[i] == cb) {
            return (int) i;
        }
    }

    return OPAL_ERR_NOT_FOUND;
}

static int _opal_progress_register (opal_progress_callback_t cb, volatile opal_progress_callback_t **cbs,
                                    size_t *cbs_size, size_t *cbs_len)
{
    int ret = OPAL_SUCCESS;

    if (OPAL_ERR_NOT_FOUND != opal_progress_find_cb (cb, *cbs, *cbs_len)) {
        return OPAL_SUCCESS;
    }

    /* see if we need to allocate more space */
    if (*cbs_len + 1 > *cbs_size) {
        opal_progress_callback_t *tmp, *old;

        tmp = (opal_progress_callback_t *) malloc (sizeof (tmp[0]) * 2 * *cbs_size);
        if (tmp == NULL) {
            return OPAL_ERR_TEMP_OUT_OF_RESOURCE;
        }

        if (*cbs) {
            /* copy old callbacks */
            memcpy (tmp, (void *) *cbs, sizeof(tmp[0]) * *cbs_size);
        }

        for (size_t i = *cbs_len ; i < 2 * *cbs_size ; ++i) {
            tmp[i] = fake_cb;
        }

        opal_atomic_wmb ();

        /* swap out callback array */
        old = opal_atomic_swap_ptr (cbs, tmp);

        opal_atomic_wmb ();

        free (old);
        *cbs_size *= 2;
    }

    cbs[0][*cbs_len] = cb;
    ++*cbs_len;

    opal_atomic_wmb ();

    return ret;
}

int opal_progress_register (opal_progress_callback_t cb)
{
    int ret;

    opal_atomic_lock(&progress_lock);

    (void) _opal_progress_unregister (cb, callbacks_lp, &callbacks_lp_len);

    ret = _opal_progress_register (cb, &callbacks, &callbacks_size, &callbacks_len);

    opal_atomic_unlock(&progress_lock);

    return ret;
}

int opal_progress_register_lp (opal_progress_callback_t cb)
{
    int ret;

    opal_atomic_lock(&progress_lock);

    (void) _opal_progress_unregister (cb, callbacks, &callbacks_len);

    ret = _opal_progress_register (cb, &callbacks_lp, &callbacks_lp_size, &callbacks_lp_len);

    opal_atomic_unlock(&progress_lock);

    return ret;
}

static int _opal_progress_unregister (opal_progress_callback_t cb, volatile opal_progress_callback_t *callback_array,
                                      size_t *callback_array_len)
{
    int ret = opal_progress_find_cb (cb, callback_array, *callback_array_len);
    if (OPAL_ERR_NOT_FOUND == ret) {
        return ret;
    }

    /* If we found the function we're unregistering: If callbacks_len
       is 0, we're not goig to do anything interesting anyway, so
       skip.  If callbacks_len is 1, it will soon be 0, so no need to
       do any repacking. */
    for (size_t i = (size_t) ret ; i < *callback_array_len - 1 ; ++i) {
        /* copy callbacks atomically since another thread may be in
         * opal_progress(). */
        (void) opal_atomic_swap_ptr (callback_array + i, callback_array[i+1]);
    }

    callback_array[*callback_array_len] = fake_cb;
    --*callback_array_len;

    return OPAL_SUCCESS;
}

int opal_progress_unregister (opal_progress_callback_t cb)
{
    int ret;

    opal_atomic_lock(&progress_lock);

    ret = _opal_progress_unregister (cb, callbacks, &callbacks_len);

    if (OPAL_SUCCESS != ret) {
        /* if not in the high-priority array try to remove from the lp array.
         * a callback will never be in both. */
        ret = _opal_progress_unregister (cb, callbacks_lp, &callbacks_lp_len);
    }

    opal_atomic_unlock(&progress_lock);

    return ret;
}
