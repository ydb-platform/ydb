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
 * Copyright (c) 2007      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"

#include <string.h>
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif  /* HAVE_SYS_TIME_H */
#ifdef HAVE_SYSLOG_H
#include <syslog.h>
#endif
#include <stdarg.h>

#include "opal/util/show_help.h"

#include "orte/util/error_strings.h"
#include "orte/util/name_fns.h"

#include "orte/mca/notifier/base/base.h"
#include "notifier_syslog.h"


/* Static API's */
static int init(void);
static void finalize(void);
static void mylog(orte_notifier_request_t *req);
static void myevent(orte_notifier_request_t *req);
static void myreport(orte_notifier_request_t *req);

/* Module def */
orte_notifier_base_module_t orte_notifier_syslog_module = {
    .init = init,
    .finalize = finalize,
    .log = mylog,
    .event = myevent,
    .report = myreport
};


static int init(void)
{
    int opts;

    opts = LOG_CONS | LOG_PID;
    openlog("OpenRTE Error Report:", opts, LOG_USER);

    return ORTE_SUCCESS;
}

static void finalize(void)
{
    closelog();
}

static void mylog(orte_notifier_request_t *req)
{
    char tod[48];

    opal_output_verbose(5, orte_notifier_base_framework.framework_output,
                           "notifier:syslog:mylog function called with severity %d errcode %d and messg %s",
                           (int)req->severity, req->errcode, req->msg);
    /* If there was a message, output it */
    (void)ctime_r(&req->t, tod);
    /* trim the newline */
    tod[strlen(tod)] = '\0';

    syslog(req->severity, "[%s]%s %s: JOBID %s REPORTS ERROR %s: %s", tod,
           ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
           orte_notifier_base_sev2str(req->severity),
           ORTE_JOBID_PRINT((NULL == req->jdata) ?
                            ORTE_JOBID_INVALID : req->jdata->jobid),
           orte_job_state_to_str(req->state),
           (NULL == req->msg) ? "<N/A>" : req->msg);
}

static void myevent(orte_notifier_request_t *req)
{
    char tod[48];

    opal_output_verbose(5, orte_notifier_base_framework.framework_output,
                           "notifier:syslog:myevent function called with severity %d and messg %s",
                           (int)req->severity, req->msg);
    /* If there was a message, output it */
    (void)ctime_r(&req->t, tod);
    /* trim the newline */
    tod[strlen(tod)] = '\0';

    syslog(req->severity, "[%s]%s %s SYSTEM EVENT : %s", tod,
           ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
           orte_notifier_base_sev2str(req->severity),
           (NULL == req->msg) ? "<N/A>" : req->msg);
}

static void myreport(orte_notifier_request_t *req)
{
    char tod[48];

    opal_output_verbose(5, orte_notifier_base_framework.framework_output,
                           "notifier:syslog:myreport function called with severity %d state %s and messg %s",
                           (int)req->severity, orte_job_state_to_str(req->state),
                           req->msg);
    /* If there was a message, output it */
    (void)ctime_r(&req->t, tod);
    /* trim the newline */
    tod[strlen(tod)] = '\0';

    syslog(req->severity, "[%s]%s JOBID %s REPORTS STATE %s: %s", tod,
           ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
           ORTE_JOBID_PRINT((NULL == req->jdata) ?
                            ORTE_JOBID_INVALID : req->jdata->jobid),
           orte_job_state_to_str(req->state),
           (NULL == req->msg) ? "<N/A>" : req->msg);
}
