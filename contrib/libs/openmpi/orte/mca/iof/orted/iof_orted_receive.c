/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2014-2016 Intel Corporation.  All rights reserved.
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

#include "opal/dss/dss.h"

#include "orte/mca/rml/rml.h"
#include "orte/mca/rml/rml_types.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/util/name_fns.h"
#include "orte/runtime/orte_globals.h"

#include "orte/mca/iof/iof_types.h"
#include "orte/mca/iof/base/base.h"

#include "iof_orted.h"

static void send_cb(int status, orte_process_name_t *peer,
                    opal_buffer_t *buf, orte_rml_tag_t tag,
                    void *cbdata)
{
    /* nothing to do here - just release buffer and return */
    OBJ_RELEASE(buf);
}

void orte_iof_orted_send_xonxoff(orte_iof_tag_t tag)
{
    opal_buffer_t *buf;
    int rc;

    buf = OBJ_NEW(opal_buffer_t);

    /* pack the tag - we do this first so that flow control messages can
     * consist solely of the tag
     */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buf, &tag, 1, ORTE_IOF_TAG))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buf);
        return;
    }

    OPAL_OUTPUT_VERBOSE((1, orte_iof_base_framework.framework_output,
                         "%s sending %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         (ORTE_IOF_XON == tag) ? "xon" : "xoff"));

    /* send the buffer to the HNP */
    if (0 > (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                                  ORTE_PROC_MY_HNP, buf, ORTE_RML_TAG_IOF_HNP,
                                                  send_cb, NULL))) {
        ORTE_ERROR_LOG(rc);
    }
}

/*
 * The only messages coming to an orted are either:
 *
 * (a) stdin, which is to be copied to whichever local
 *     procs "pull'd" a copy
 *
 * (b) flow control messages
 */
void orte_iof_orted_recv(int status, orte_process_name_t* sender,
                         opal_buffer_t* buffer, orte_rml_tag_t tag,
                         void* cbdata)
{
    unsigned char data[ORTE_IOF_BASE_MSG_MAX];
    orte_iof_tag_t stream;
    int32_t count, numbytes;
    orte_process_name_t target;
    orte_iof_proc_t *proct;
    int rc;

    /* see what stream generated this data */
    count = 1;
    if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &stream, &count, ORTE_IOF_TAG))) {
        ORTE_ERROR_LOG(rc);
        return;
    }

    /* if this isn't stdin, then we have an error */
    if (ORTE_IOF_STDIN != stream) {
        ORTE_ERROR_LOG(ORTE_ERR_COMM_FAILURE);
        return;
    }

    /* unpack the intended target */
    count = 1;
    if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &target, &count, ORTE_NAME))) {
        ORTE_ERROR_LOG(rc);
        return;
    }

    /* unpack the data */
    numbytes=ORTE_IOF_BASE_MSG_MAX;
    if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, data, &numbytes, OPAL_BYTE))) {
        ORTE_ERROR_LOG(rc);
        return;
    }
    /* numbytes will contain the actual #bytes that were sent */

    OPAL_OUTPUT_VERBOSE((1, orte_iof_base_framework.framework_output,
                         "%s unpacked %d bytes for local proc %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), numbytes,
                         ORTE_NAME_PRINT(&target)));

    /* cycle through our list of procs */
    OPAL_LIST_FOREACH(proct, &mca_iof_orted_component.procs, orte_iof_proc_t) {
        /* is this intended for this jobid? */
        if (target.jobid == proct->name.jobid) {
            /* yes - is this intended for all vpids or this vpid? */
            if (ORTE_VPID_WILDCARD == target.vpid ||
                proct->name.vpid == target.vpid) {
                OPAL_OUTPUT_VERBOSE((1, orte_iof_base_framework.framework_output,
                                     "%s writing data to local proc %s",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     ORTE_NAME_PRINT(&proct->name)));
                if (NULL == proct->stdinev) {
                    continue;
                }
                /* send the bytes down the pipe - we even send 0 byte events
                 * down the pipe so it forces out any preceding data before
                 * closing the output stream
                 */
                if (ORTE_IOF_MAX_INPUT_BUFFERS < orte_iof_base_write_output(&target, stream, data, numbytes, proct->stdinev->wev)) {
                    /* getting too backed up - tell the HNP to hold off any more input if we
                     * haven't already told it
                     */
                    if (!mca_iof_orted_component.xoff) {
                        mca_iof_orted_component.xoff = true;
                        orte_iof_orted_send_xonxoff(ORTE_IOF_XOFF);
                    }
                }
            }
        }
    }
}
