/*
 * Copyright (c) 2011-2018 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2013      Los Alamos National Security, LLC.  All rights reserved.
 * Copyright (c) 2015-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation.  All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * When this component is used, this file is included in the rest of
 * the code base via opal/mca/event/event.h.  As such,
 * this header represents the public interface to this static component.
 */

#ifndef MCA_OPAL_EVENT_EXTERNAL_H
#define MCA_OPAL_EVENT_EXTERNAL_H

#include "opal_config.h"

#if defined(__CYGWIN__) && defined(WIN32)
#undef WIN32
#endif

#include "event.h"
#include "event2/event.h"
#include "event2/thread.h"

typedef event_callback_fn opal_event_cbfunc_t;

BEGIN_C_DECLS

typedef struct event_base opal_event_base_t;
typedef struct event opal_event_t;

OPAL_DECLSPEC extern opal_event_base_t *opal_sync_event_base;

#define OPAL_EV_TIMEOUT EV_TIMEOUT
#define OPAL_EV_READ    EV_READ
#define OPAL_EV_WRITE   EV_WRITE
#define OPAL_EV_SIGNAL  EV_SIGNAL
/* Persistent event: won't get removed automatically when activated. */
#define OPAL_EV_PERSIST EV_PERSIST

#define OPAL_EVLOOP_ONCE     EVLOOP_ONCE        /**< Block at most once. */
#define OPAL_EVLOOP_NONBLOCK EVLOOP_NONBLOCK    /**< Do not block. */

/* Global function to create and release an event base */
OPAL_DECLSPEC opal_event_base_t* opal_event_base_create(void);

#define opal_event_base_free(x) event_base_free(x)

OPAL_DECLSPEC int opal_event_init(void);

OPAL_DECLSPEC int opal_event_finalize(void);

#define opal_event_reinit(b) event_reinit((b))

#define opal_event_base_init_common_timeout (b, t) event_base_init_common_timeout((b), (t))

#define opal_event_base_loopbreak(b) event_base_loopbreak(b)

#define opal_event_base_loopexit(b) event_base_loopexit(b, NULL)

/* Event priority APIs */
#define opal_event_base_priority_init(b, n) event_base_priority_init((b), (n))

#define opal_event_set_priority(x, n) event_priority_set((x), (n))

/* thread support APIs */
#define opal_event_use_threads() evthread_use_pthreads()

/* Basic event APIs */
#define opal_event_enable_debug_mode() event_enable_debug_mode()

#define opal_event_set(b, x, fd, fg, cb, arg) event_assign((x), (b), (fd), (fg), (event_callback_fn) (cb), (arg))

#define opal_event_assign(x, b, fd, fg, cb, arg) event_assign((x), (b), (fd), (fg), (event_callback_fn) (cb), (arg))

#define opal_event_add(ev, tv) event_add((ev), (tv))

#define opal_event_del(ev) event_del((ev))

#define opal_event_active(x, y, z) event_active((x), (y), (z))

#define opal_event_new(b, fd, fg, cb, arg) event_new((b), (fd), (fg), (event_callback_fn) (cb), (arg))

OPAL_DECLSPEC opal_event_t* opal_event_alloc(void);

#define opal_event_free(x) event_free((x))

/* Timer APIs */
#define opal_event_evtimer_new(b, cb, arg) opal_event_new((b), -1, 0, (cb), (arg))

#define opal_event_evtimer_add(x, tv) opal_event_add((x), (tv))

#define opal_event_evtimer_set(b, x, cb, arg) event_assign((x), (b), -1, 0, (event_callback_fn) (cb), (arg))

#define opal_event_evtimer_del(x) opal_event_del((x))

#define opal_event_evtimer_pending(x, tv) event_pending((x), EV_TIMEOUT, (tv))

#define opal_event_evtimer_initialized(x) event_initialized((x))

/* Signal APIs */
#define opal_event_signal_add(x, tv) event_add((x), (tv))

#define opal_event_signal_set(b, x, fd, cb, arg) event_assign((x), (b), (fd), EV_SIGNAL|EV_PERSIST, (event_callback_fn) (cb), (arg))

#define opal_event_signal_del(x) event_del((x))

#define opal_event_signal_pending(x, tv) event_pending((x), EV_SIGNAL, (tv))

#define opal_event_signal_initalized(x) event_initialized((x))

#define opal_event_get_signal(x) event_get_signal((x))

#define opal_event_loop(b, fg) event_base_loop((b), (fg))

END_C_DECLS

#endif /* MCA_OPAL_EVENT_EXTERNAL_H */
