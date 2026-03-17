/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2017      Mellanox Technologies. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"

#include <errno.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */
#include <string.h>
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#else
#ifdef HAVE_SYS_FCNTL_H
#include <sys/fcntl.h>
#endif
#endif

#include "opal/dss/dss.h"
#include "opal/mca/pmix/pmix.h"

#include "orte/mca/rml/rml.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/util/name_fns.h"
#include "orte/util/threads.h"
#include "orte/runtime/orte_globals.h"

#include "orte/mca/iof/iof.h"
#include "orte/mca/iof/base/base.h"

#include "iof_hnp.h"


void orte_iof_hnp_recv(int status, orte_process_name_t* sender,
                       opal_buffer_t* buffer, orte_rml_tag_t tag,
                       void* cbdata)
{
    orte_process_name_t origin, requestor;
    unsigned char data[ORTE_IOF_BASE_MSG_MAX];
    orte_iof_tag_t stream;
    int32_t count, numbytes;
    orte_iof_sink_t *sink, *next;
    int rc;
    bool exclusive;
    orte_iof_proc_t *proct;
    orte_ns_cmp_bitmask_t mask=ORTE_NS_CMP_ALL | ORTE_NS_CMP_WILD;

    OPAL_OUTPUT_VERBOSE((1, orte_iof_base_framework.framework_output,
                         "%s received IOF from proc %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(sender)));

    /* unpack the stream first as this may be flow control info */
    count = 1;
    if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &stream, &count, ORTE_IOF_TAG))) {
        ORTE_ERROR_LOG(rc);
        goto CLEAN_RETURN;
    }

    if (ORTE_IOF_XON & stream) {
        /* re-start the stdin read event */
        if (NULL != mca_iof_hnp_component.stdinev &&
            !orte_job_term_ordered &&
            !mca_iof_hnp_component.stdinev->active) {
            ORTE_IOF_READ_ACTIVATE(mca_iof_hnp_component.stdinev);
        }
        goto CLEAN_RETURN;
    } else if (ORTE_IOF_XOFF & stream) {
        /* stop the stdin read event */
        if (NULL != mca_iof_hnp_component.stdinev &&
            !mca_iof_hnp_component.stdinev->active) {
            opal_event_del(mca_iof_hnp_component.stdinev->ev);
            mca_iof_hnp_component.stdinev->active = false;
        }
        goto CLEAN_RETURN;
    }

    /* get name of the process whose io we are discussing */
    count = 1;
    if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &origin, &count, ORTE_NAME))) {
        ORTE_ERROR_LOG(rc);
        goto CLEAN_RETURN;
    }

    OPAL_OUTPUT_VERBOSE((1, orte_iof_base_framework.framework_output,
                         "%s received IOF cmd for source %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(&origin)));

    /* check to see if a tool has requested something */
    if (ORTE_IOF_PULL & stream) {
        /* get name of the process wishing to be the sink */
        count = 1;
        if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &requestor, &count, ORTE_NAME))) {
            ORTE_ERROR_LOG(rc);
            goto CLEAN_RETURN;
        }

        OPAL_OUTPUT_VERBOSE((1, orte_iof_base_framework.framework_output,
                             "%s received pull cmd from remote tool %s for proc %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(&requestor),
                             ORTE_NAME_PRINT(&origin)));

        if (ORTE_IOF_EXCLUSIVE & stream) {
            exclusive = true;
        } else {
            exclusive = false;
        }
        /* do we already have this process in our list? */
        OPAL_LIST_FOREACH(proct, &mca_iof_hnp_component.procs, orte_iof_proc_t) {
            if (OPAL_EQUAL == orte_util_compare_name_fields(mask, &proct->name, &origin)) {
                /* found it */
                goto PROCESS;
            }
        }
        /* if we get here, then we don't yet have this proc in our list */
        proct = OBJ_NEW(orte_iof_proc_t);
        proct->name.jobid = origin.jobid;
        proct->name.vpid = origin.vpid;
        opal_list_append(&mca_iof_hnp_component.procs, &proct->super);

      PROCESS:
        /* a tool is requesting that we send it a copy of the specified stream(s)
         * from the specified process(es), so create a sink for it
         */
        if (NULL == proct->subscribers) {
            proct->subscribers = OBJ_NEW(opal_list_t);
        }
        if (ORTE_IOF_STDOUT & stream) {
            ORTE_IOF_SINK_DEFINE(&sink, &origin, -1, ORTE_IOF_STDOUT, NULL);
            sink->daemon.jobid = requestor.jobid;
            sink->daemon.vpid = requestor.vpid;
            sink->exclusive = exclusive;
            opal_list_append(proct->subscribers, &sink->super);
        }
        if (ORTE_IOF_STDERR & stream) {
            ORTE_IOF_SINK_DEFINE(&sink, &origin, -1, ORTE_IOF_STDERR, NULL);
            sink->daemon.jobid = requestor.jobid;
            sink->daemon.vpid = requestor.vpid;
            sink->exclusive = exclusive;
            opal_list_append(proct->subscribers, &sink->super);
        }
        if (ORTE_IOF_STDDIAG & stream) {
            ORTE_IOF_SINK_DEFINE(&sink, &origin, -1, ORTE_IOF_STDDIAG, NULL);
            sink->daemon.jobid = requestor.jobid;
            sink->daemon.vpid = requestor.vpid;
            sink->exclusive = exclusive;
            opal_list_append(proct->subscribers, &sink->super);
        }
        goto CLEAN_RETURN;
    }

    if (ORTE_IOF_CLOSE & stream) {
        OPAL_OUTPUT_VERBOSE((1, orte_iof_base_framework.framework_output,
                             "%s received close cmd from remote tool %s for proc %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(sender),
                             ORTE_NAME_PRINT(&origin)));
        /* a tool is requesting that we no longer forward a copy of the
         * specified stream(s) from the specified process(es) - remove the sink
         */
        OPAL_LIST_FOREACH(proct, &mca_iof_hnp_component.procs, orte_iof_proc_t) {
            if (OPAL_EQUAL != orte_util_compare_name_fields(mask, &proct->name, &origin)) {
                continue;
            }
            OPAL_LIST_FOREACH_SAFE(sink, next, proct->subscribers, orte_iof_sink_t) {
                 /* if the target isn't set, then this sink is for another purpose - ignore it */
                if (ORTE_JOBID_INVALID == sink->daemon.jobid) {
                    continue;
                }
                /* if this sink is the designated one, then remove it from list */
                if ((stream & sink->tag) &&
                    sink->name.jobid == origin.jobid &&
                    (ORTE_VPID_WILDCARD == sink->name.vpid ||
                     ORTE_VPID_WILDCARD == origin.vpid ||
                     sink->name.vpid == origin.vpid)) {
                    /* send an ack message to the requestor - this ensures that the RML has
                     * completed sending anything to that requestor before it exits
                     */
                    orte_iof_hnp_send_data_to_endpoint(&sink->daemon, &origin, ORTE_IOF_CLOSE, NULL, 0);
                    opal_list_remove_item(proct->subscribers, &sink->super);
                    OBJ_RELEASE(sink);
                }
            }
        }
        goto CLEAN_RETURN;
    }

    /* this must have come from a daemon forwarding output - unpack the data */
    numbytes=ORTE_IOF_BASE_MSG_MAX;
    if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, data, &numbytes, OPAL_BYTE))) {
        ORTE_ERROR_LOG(rc);
        goto CLEAN_RETURN;
    }
    /* numbytes will contain the actual #bytes that were sent */

    OPAL_OUTPUT_VERBOSE((1, orte_iof_base_framework.framework_output,
                         "%s unpacked %d bytes from remote proc %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), numbytes,
                         ORTE_NAME_PRINT(&origin)));

    /* do we already have this process in our list? */
    OPAL_LIST_FOREACH(proct, &mca_iof_hnp_component.procs, orte_iof_proc_t) {
        if (OPAL_EQUAL == orte_util_compare_name_fields(mask, &proct->name, &origin)) {
            /* found it */
            goto NSTEP;
        }
    }
    /* if we get here, then we don't yet have this proc in our list */
    proct = OBJ_NEW(orte_iof_proc_t);
    proct->name.jobid = origin.jobid;
    proct->name.vpid = origin.vpid;
    opal_list_append(&mca_iof_hnp_component.procs, &proct->super);

  NSTEP:
    /* cycle through the endpoints to see if someone else wants a copy */
    exclusive = false;
    if (NULL != proct->subscribers) {
        OPAL_LIST_FOREACH(sink, proct->subscribers, orte_iof_sink_t) {
            /* if the target isn't set, then this sink is for another purpose - ignore it */
            if (ORTE_JOBID_INVALID == sink->daemon.jobid) {
                continue;
            }
            if ((stream & sink->tag) &&
                sink->name.jobid == origin.jobid &&
                (ORTE_VPID_WILDCARD == sink->name.vpid ||
                 ORTE_VPID_WILDCARD == origin.vpid ||
                 sink->name.vpid == origin.vpid)) {
                /* send the data to the tool */
                if (NULL != opal_pmix.server_iof_push) {
                    /* don't pass along zero byte blobs */
                    if (0 < numbytes) {
                        rc = opal_pmix.server_iof_push(&proct->name, stream, data, numbytes);
                        if (ORTE_SUCCESS != rc) {
                            ORTE_ERROR_LOG(rc);
                        }
                    }
                } else {
                    orte_iof_hnp_send_data_to_endpoint(&sink->daemon, &origin, stream, data, numbytes);
                }
                if (sink->exclusive) {
                    exclusive = true;
                }
            }
        }
    }
    /* if the user doesn't want a copy written to the screen, then we are done */
    if (!proct->copy) {
        return;
    }

    /* output this to our local output unless one of the sinks was exclusive */
    if (!exclusive) {
        if (ORTE_IOF_STDOUT & stream || orte_xml_output) {
            orte_iof_base_write_output(&origin, stream, data, numbytes, orte_iof_base.iof_write_stdout->wev);
        } else {
            orte_iof_base_write_output(&origin, stream, data, numbytes, orte_iof_base.iof_write_stderr->wev);
        }
    }

 CLEAN_RETURN:
    return;
}
