/*
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2015      Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef PMIX_PROGRESS_THREADS_H
#define PMIX_PROGRESS_THREADS_H

#include "pmix_config.h"

#include <pthread.h>
#include PMIX_EVENT_HEADER

#include "src/include/types.h"

/**
 * Initialize a progress thread name; if a progress thread is not
 * already associated with that name, start a progress thread.
 *
 * If you have general events that need to run in *a* progress thread
 * (but not necessarily a your own, dedicated progress thread), pass
 * NULL the "name" argument to the pmix_progress_thead_init() function
 * to glom on to the general PMIX-wide progress thread.
 *
 * If a name is passed that was already used in a prior call to
 * pmix_progress_thread_init(), the event base associated with that
 * already-running progress thread will be returned (i.e., no new
 * progress thread will be started).
 */
pmix_event_base_t *pmix_progress_thread_init(const char *name);

/**
 * Stop a progress thread name (reference counted).
 *
 * Once this function is invoked as many times as
 * pmix_progress_thread_init() was invoked on this name (or NULL), the
 * progress function is shut down.
 * it is destroyed.
 *
 * Will return PMIX_ERR_NOT_FOUND if the progress thread name does not
 * exist; PMIX_SUCCESS otherwise.
 */
int pmix_progress_thread_stop(const char *name);

/**
 * Finalize a progress thread name (reference counted).
 *
 * Once this function is invoked after pmix_progress_thread_stop() has been called
 * as many times as pmix_progress_thread_init() was invoked on this name (or NULL),
 * the event base associated with it is destroyed.
 *
 * Will return PMIX_ERR_NOT_FOUND if the progress thread name does not
 * exist; PMIX_SUCCESS otherwise.
 */
int pmix_progress_thread_finalize(const char *name);

/**
 * Temporarily pause the progress thread associated with this name.
 *
 * This function does not destroy the event base associated with this
 * progress thread name, but it does stop processing all events on
 * that event base until pmix_progress_thread_resume() is invoked on
 * that name.
 *
 * Will return PMIX_ERR_NOT_FOUND if the progress thread name does not
 * exist; PMIX_SUCCESS otherwise.
 */
int pmix_progress_thread_pause(const char *name);

/**
 * Restart a previously-paused progress thread associated with this
 * name.
 *
 * Will return PMIX_ERR_NOT_FOUND if the progress thread name does not
 * exist; PMIX_SUCCESS otherwise.
 */
int pmix_progress_thread_resume(const char *name);

#endif
