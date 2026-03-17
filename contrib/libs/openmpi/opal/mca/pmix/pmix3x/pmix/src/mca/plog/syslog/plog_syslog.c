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
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "pmix_config.h"
#include "pmix_common.h"

#include <string.h>
#ifdef HAVE_TIME_H
#include <time.h>
#endif
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif  /* HAVE_SYS_TIME_H */
#ifdef HAVE_SYSLOG_H
#include <syslog.h>
#endif
#include <stdarg.h>

#include "src/util/argv.h"
#include "src/util/error.h"
#include "src/util/name_fns.h"
#include "src/util/show_help.h"
#include "src/mca/bfrops/bfrops.h"
#include "src/server/pmix_server_ops.h"

#include "src/mca/plog/base/base.h"
#include "plog_syslog.h"


/* Static API's */
static pmix_status_t init(void);
static void finalize(void);
static pmix_status_t mylog(const pmix_proc_t *source,
                           const pmix_info_t data[], size_t ndata,
                           const pmix_info_t directives[], size_t ndirs,
                           pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Module def */
pmix_plog_module_t pmix_plog_syslog_module = {
    .name = "syslog",
    .init = init,
    .finalize = finalize,
    .log = mylog
};


static pmix_status_t init(void)
{
    int opts;
    char *mychannels = "lsys,gsys,syslog,local_syslog,global_syslog";

    pmix_plog_syslog_module.channels = pmix_argv_split(mychannels, ',');

    opts = LOG_CONS | LOG_PID;
    openlog("PMIx Log Report:", opts, LOG_USER);

    return PMIX_SUCCESS;
}

static void finalize(void)
{
    closelog();
    pmix_argv_free(pmix_plog_syslog_module.channels);
}

static pmix_status_t write_local(const pmix_proc_t *source,
                                 time_t timestamp,
                                 int severity, char *msg,
                                 const pmix_info_t *data, size_t ndata);

/* we only get called if we are a SERVER */
static pmix_status_t mylog(const pmix_proc_t *source,
                           const pmix_info_t data[], size_t ndata,
                           const pmix_info_t directives[], size_t ndirs,
                           pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    size_t n;
    int pri = mca_plog_syslog_component.level;
    pmix_status_t rc;
    time_t timestamp = 0;

    /* if there is no data, then we don't handle it */
    if (NULL == data || 0 == ndata) {
        return PMIX_ERR_NOT_AVAILABLE;
    }

    /* check directives */
    if (NULL != directives) {
        for (n=0; n < ndirs; n++) {
            if (0 == strncmp(directives[n].key, PMIX_LOG_SYSLOG_PRI, PMIX_MAX_KEYLEN)) {
                pri = directives[n].value.data.integer;
            } else if (0 == strncmp(directives[n].key, PMIX_LOG_TIMESTAMP, PMIX_MAX_KEYLEN)) {
                timestamp = directives[n].value.data.time;
            }
        }
    }

    /* check to see if there are any syslog entries */
    for (n=0; n < ndata; n++) {
        if (0 == strncmp(data[n].key, PMIX_LOG_SYSLOG, PMIX_MAX_KEYLEN)) {
            /* we default to using the local syslog */
            rc = write_local(source, timestamp, pri, data[n].value.data.string, data, ndata);
            if (PMIX_SUCCESS == rc) {
                /* flag that we did this one */
                PMIX_INFO_OP_COMPLETED(&data[n]);
            }
        } else if (0 == strncmp(data[n].key, PMIX_LOG_LOCAL_SYSLOG, PMIX_MAX_KEYLEN)) {
            rc = write_local(source, timestamp, pri, data[n].value.data.string, data, ndata);
            if (PMIX_SUCCESS == rc) {
                /* flag that we did this one */
                PMIX_INFO_OP_COMPLETED(&data[n]);
            }
        } else if (0 == strncmp(data[n].key, PMIX_LOG_GLOBAL_SYSLOG, PMIX_MAX_KEYLEN)) {
            /* only do this if we are a gateway server */
            if (PMIX_PROC_IS_GATEWAY(pmix_globals.mypeer)) {
                rc = write_local(source, timestamp, pri, data[n].value.data.string, data, ndata);
                if (PMIX_SUCCESS == rc) {
                    /* flag that we did this one */
                    PMIX_INFO_OP_COMPLETED(&data[n]);
                }
            }
        }
    }

    return PMIX_SUCCESS;
}

static char* sev2str(int severity)
{
    switch (severity) {
        case LOG_EMERG:
            return "EMERGENCY";
        case LOG_ALERT:
            return "ALERT";
        case LOG_CRIT:
            return "CRITICAL";
        case LOG_ERR:
            return "ERROR";
        case LOG_WARNING:
            return "WARNING";
        case LOG_NOTICE:
            return "NOTICE";
        case LOG_INFO:
            return "INFO";
        case LOG_DEBUG:
            return "DEBUG";
        default:
            return "UNKNOWN SEVERITY";
    }
}

static pmix_status_t write_local(const pmix_proc_t *source,
                                 time_t timestamp,
                                 int severity, char *msg,
                                 const pmix_info_t *data, size_t ndata)
{
    char tod[48], *datastr, *tmp, *tmp2;
    pmix_status_t rc;
    size_t n;

    pmix_output_verbose(5, pmix_plog_base_framework.framework_output,
                           "plog:syslog:mylog function called with severity %d", severity);

    if (0 < timestamp) {
        /* If there was a message, output it */
        (void)ctime_r(&timestamp, tod);
        /* trim the newline */
        tod[strlen(tod)] = '\0';
    }

    if (NULL == data) {
        syslog(severity, "%s [%s:%d]%s PROC %s:%d REPORTS: %s",
               tod, pmix_globals.myid.nspace, pmix_globals.myid.rank,
               sev2str(severity),
               source->nspace, source->rank,
               (NULL == msg) ? "<N/A>" : msg);
    } else {
        /* need to print the info from the data, starting
         * with any provided msg */
        if (NULL == msg) {
            datastr = strdup("\n");
        } else {
            if (0 > asprintf(&datastr, "%s", msg)) {
                return PMIX_ERR_NOMEM;
            }
        }
        for (n=0; n < ndata; n++) {
            PMIX_BFROPS_PRINT(rc, pmix_globals.mypeer,
                              &tmp, "\t", (pmix_info_t*)&data[n], PMIX_INFO);
            if (PMIX_SUCCESS != rc) {
                free(datastr);
                return rc;
            }
            if (0 > asprintf(&tmp2, "%s\n%s", datastr, tmp)) {
                free(datastr);
                return PMIX_ERR_NOMEM;
            }
            free(datastr);
            free(tmp);
            datastr = tmp2;
        }
        /* print out the consolidated msg */
        syslog(severity, "%s [%s:%d]%s PROC %s:%d REPORTS: %s",
               tod, pmix_globals.myid.nspace, pmix_globals.myid.rank,
               sev2str(severity), source->nspace, source->rank, datastr);
        free(datastr);
    }

    return PMIX_SUCCESS;
}
