/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      Cisco Systems, Inc.  All Rights Reserved.
 * Copyright (c) 2012-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file:
 *
 * The OpenRTE Notifier Framework
 *
 * The OpenRTE Notifier framework provides a mechanism for notifying
 * system administrators or other fault monitoring systems that a
 * problem with the underlying cluster has been detected - e.g., a
 * failed connection in a network fabric
 */

#ifndef MCA_NOTIFIER_H
#define MCA_NOTIFIER_H

/*
 * includes
 */

#include "orte_config.h"

#include <stdarg.h>
#include <limits.h>
#ifdef HAVE_SYSLOG_H
#include <syslog.h>
#endif

#include "orte/mca/mca.h"

#include "orte/constants.h"
#include "orte/types.h"

#include "orte/runtime/orte_globals.h"
#include "orte/util/threads.h"

BEGIN_C_DECLS

/* make the verbose channel visible here so everyone
 * doesn't have to include notifier/base/base.h */
ORTE_DECLSPEC extern int orte_notifier_debug_output;

/* The maximum size of any on-stack buffers used in the notifier
 * so we can try to avoid calling malloc in OUT_OF_RESOURCES conditions.
 * The code has NOT been auditied for use of malloc, so this still
 * may fail to get the "OUT_OF_RESOURCE" message out.  Oh Well.
 */
#define ORTE_NOTIFIER_MAX_BUF   512

/* Severities */
typedef enum {
#ifdef HAVE_SYSLOG_H
    ORTE_NOTIFIER_EMERG = LOG_EMERG,
    ORTE_NOTIFIER_ALERT = LOG_ALERT,
    ORTE_NOTIFIER_CRIT = LOG_CRIT,
    ORTE_NOTIFIER_ERROR = LOG_ERR,
    ORTE_NOTIFIER_WARN = LOG_WARNING,
    ORTE_NOTIFIER_NOTICE = LOG_NOTICE,
    ORTE_NOTIFIER_INFO = LOG_INFO,
    ORTE_NOTIFIER_DEBUG = LOG_DEBUG
#else
    ORTE_NOTIFIER_EMERG,
    ORTE_NOTIFIER_ALERT,
    ORTE_NOTIFIER_CRIT,
    ORTE_NOTIFIER_ERROR,
    ORTE_NOTIFIER_WARN,
    ORTE_NOTIFIER_NOTICE,
    ORTE_NOTIFIER_INFO,
    ORTE_NOTIFIER_DEBUG
#endif
} orte_notifier_severity_t;

typedef struct {
    opal_object_t super;
    opal_event_t ev;
    orte_job_t *jdata;
    orte_job_state_t state;
    orte_notifier_severity_t severity;
    int errcode;
    const char *msg;
    const char *action;
    time_t t;
} orte_notifier_request_t;
OBJ_CLASS_DECLARATION(orte_notifier_request_t);

/*
 * Component functions - all MUST be provided!
 */

/* initialize the selected module */
typedef int (*orte_notifier_base_module_init_fn_t)(void);

/* finalize the selected module */
typedef void (*orte_notifier_base_module_finalize_fn_t)(void);

/* Log an internal error - this will include the job that caused the
 * error to occur */
typedef void (*orte_notifier_base_module_log_fn_t)(orte_notifier_request_t *req);

/* Report a system event - e.g., a temperature out-of-bound */
typedef void (*orte_notifier_base_module_event_fn_t)(orte_notifier_request_t *req);

/* Report a job state */
typedef void (*orte_notifier_base_module_report_fn_t)(orte_notifier_request_t *req);


#define ORTE_NOTIFIER_INTERNAL_ERROR(j, st, s, e, m)                    \
    do {                                                                \
        orte_notifier_request_t *_n;                                    \
        opal_output_verbose(2, orte_notifier_debug_output,              \
                            "%s notifier:internal:error[%s:%d] "        \
                            "job %s error %s severity %s",              \
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),         \
                            __FILE__, __LINE__,                         \
                            ORTE_JOBID_PRINT((NULL == (j)) ?            \
                                             ORTE_JOBID_INVALID :       \
                                             (j)->jobid),               \
                            ORTE_ERROR_NAME((e)),                       \
                            orte_notifier_base_sev2str(s));             \
        _n = OBJ_NEW(orte_notifier_request_t);                          \
        _n->jdata = (j);                                                \
        _n->state = (st);                                               \
        _n->severity = (s);                                             \
        _n->errcode = (e);                                              \
        _n->msg = (m);                                                  \
        _n->t = time(NULL);                                             \
        _n->action = (NULL);                                            \
        /* add the event */                                             \
        opal_event_set(orte_notifier_base.ev_base, &(_n)->ev, -1,       \
                       OPAL_EV_WRITE, orte_notifier_base_log, (_n));    \
        opal_event_set_priority(&(_n)->ev, ORTE_ERROR_PRI);             \
        ORTE_POST_OBJECT(_n);                                           \
        opal_event_active(&(_n)->ev, OPAL_EV_WRITE, 1);                 \
    } while(0);

#define ORTE_NOTIFIER_JOB_STATE(j, st, m)                               \
    do {                                                                \
        orte_notifier_request_t *_n;                                    \
        opal_output_verbose(2, orte_notifier_debug_output,              \
                            "%s notifier[%s:%d] job %s state %s",       \
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),         \
                            __FILE__, __LINE__,                         \
                            ORTE_JOBID_PRINT((NULL == (j)) ?            \
                                             ORTE_JOBID_INVALID :       \
                                             (j)->jobid),               \
                            orte_job_state_to_str(st));                 \
        _n = OBJ_NEW(orte_notifier_request_t);                          \
        _n->jdata = (j);                                                \
        _n->state = (st);                                               \
        _n->msg = (m);                                                  \
        _n->t = time(NULL);                                             \
        _n->action = (NULL);                                            \
        /* add the event */                                             \
        opal_event_set(orte_notifier_base.ev_base, &(_n)->ev, -1,       \
                       OPAL_EV_WRITE, orte_notifier_base_report, (_n)); \
        opal_event_set_priority(&(_n)->ev, ORTE_ERROR_PRI);             \
        ORTE_POST_OBJECT(_n);                                           \
        opal_event_active(&(_n)->ev, OPAL_EV_WRITE, 1);                 \
    } while(0);

#define ORTE_NOTIFIER_SYSTEM_EVENT(s, m, a)                             \
    do {                                                                \
        orte_notifier_request_t *_n;                                    \
        opal_output_verbose(2, orte_notifier_debug_output,              \
                            "%s notifier:sys:event[%s:%d] event %s",    \
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),         \
                            __FILE__, __LINE__,                         \
                            orte_notifier_base_sev2str(s));             \
        _n = OBJ_NEW(orte_notifier_request_t);                          \
        _n->jdata = (NULL);                                             \
        _n->state = (NULL);                                             \
        _n->jdata = NULL;                                               \
        _n->msg = (m);                                                  \
        _n->t = time(NULL);                                             \
        _n->severity = (s);                                             \
        _n->action = (a);                                               \
        /* add the event */                                             \
        opal_event_set(orte_notifier_base.ev_base, &(_n)->ev, -1,       \
                       OPAL_EV_WRITE, orte_notifier_base_event, (_n));  \
        opal_event_set_priority(&(_n)->ev, ORTE_ERROR_PRI);             \
        ORTE_POST_OBJECT(_n);                                           \
        opal_event_active(&(_n)->ev, OPAL_EV_WRITE, 1);                 \
    } while(0);

/*
 * Ver 1.0
 */
typedef struct {
    orte_notifier_base_module_init_fn_t             init;
    orte_notifier_base_module_finalize_fn_t         finalize;
    orte_notifier_base_module_log_fn_t              log;
    orte_notifier_base_module_event_fn_t            event;
    orte_notifier_base_module_report_fn_t           report;
} orte_notifier_base_module_t;


/*
 * the standard component data structure
 */
typedef struct {
    mca_base_component_t base_version;
    mca_base_component_data_t base_data;
} orte_notifier_base_component_t;


/*
 * Macro for use in components that are of type notifier v1.0.0
 */
#define ORTE_NOTIFIER_BASE_VERSION_1_0_0 \
    /* notifier v1.0 is chained to MCA v2.0 */ \
    ORTE_MCA_BASE_VERSION_2_1_0("notifier", 1, 0, 0)

END_C_DECLS

#endif /* MCA_NOTIFIER_H */
