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
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
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

#include "orte/mca/rml/rml.h"
#include "orte/mca/rml/rml_types.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/util/name_fns.h"
#include "orte/runtime/orte_globals.h"
#include "orte/runtime/orte_wait.h"

#include "orte/mca/iof/iof.h"
#include "orte/mca/iof/base/base.h"

#include "iof_tool.h"

static int init(void);

static int tool_push(const orte_process_name_t* dst_name, orte_iof_tag_t src_tag, int fd);

static int tool_pull(const orte_process_name_t* src_name,
                      orte_iof_tag_t src_tag,
                      int fd);

static int tool_close(const orte_process_name_t* peer,
                       orte_iof_tag_t source_tag);

static int tool_output(const orte_process_name_t* peer,
                       orte_iof_tag_t source_tag,
                       const char *msg);

static int finalize(void);

static int tool_ft_event(int state);

orte_iof_base_module_t orte_iof_tool_module = {
    .init = init,
    .push = tool_push,
    .pull = tool_pull,
    .close = tool_close,
    .output = tool_output,
    .finalize = finalize,
    .ft_event = tool_ft_event
};


static int init(void)
{
    /* post a non-blocking RML receive to get messages
     from the HNP IOF component */
    orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD,
                            ORTE_RML_TAG_IOF_PROXY,
                            ORTE_RML_PERSISTENT,
                            orte_iof_tool_recv,
                            NULL);

    mca_iof_tool_component.closed = false;

    return ORTE_SUCCESS;
}

/**
 * Push data from the specified file descriptor
 * to the indicated SINK set of peers.
 */

static int tool_push(const orte_process_name_t* dst_name, orte_iof_tag_t src_tag, int fd)
{
    /* at this time, we do not allow tools to push data into the
     * stdin of a job. This is due to potential confusion over which
     * stdin is being read/used, and the impossibility of resolving
     * potential interleaving of the data
     */

    return ORTE_ERR_NOT_SUPPORTED;
}


/*
 * Callback when non-blocking RML send completes.
 */
static void send_cb(int status, orte_process_name_t *peer,
                    opal_buffer_t *buf, orte_rml_tag_t tag,
                    void *cbdata)
{
    /* nothing to do here - just release buffer and return */
    OBJ_RELEASE(buf);
}

/**
 * Pull data from the specified set of SOURCE peers and
 * dump to the indicated file descriptor.
 */

static int tool_pull(const orte_process_name_t* src_name,
                     orte_iof_tag_t src_tag,
                     int fd)
{
    /* if we are a tool, then we need to request the HNP to please
     * forward the data from the specified process to us. Note that
     * the HNP will return an error if the specified stream of any
     * intended recipient is not open. By default, stdout/err/diag
     * are all left open. However, the user can also direct us to
     * close any or all of those streams, so the success of this call
     * will depend upon how the user executed the application
     */

    opal_buffer_t *buf;
    orte_iof_tag_t tag;
    orte_process_name_t hnp;
    int rc;

    OPAL_OUTPUT_VERBOSE((1, orte_iof_base_framework.framework_output,
                         "%s pulling output for proc %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(src_name)));

    buf = OBJ_NEW(opal_buffer_t);

    /* setup the tag to pull from HNP */
    tag = src_tag | ORTE_IOF_PULL;

    /* pack the tag - we do this first so that flow control messages can
     * consist solely of the tag
     */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buf, &tag, 1, ORTE_IOF_TAG))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buf);
        return rc;
    }
    /* pack the name of the source */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buf, src_name, 1, ORTE_NAME))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buf);
        return rc;
    }
    /* pack our name as the sink */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buf, ORTE_PROC_MY_NAME, 1, ORTE_NAME))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buf);
        return rc;
    }

    /* send the buffer to the correct HNP */
    ORTE_HNP_NAME_FROM_JOB(&hnp, src_name->jobid);
    orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                    &hnp, buf, ORTE_RML_TAG_IOF_HNP,
                                    send_cb, NULL);

    return ORTE_SUCCESS;
}


static int tool_close(const orte_process_name_t* src_name,
                      orte_iof_tag_t src_tag)
{
    /* if we are a tool, then we need to request the HNP to stop
     * forwarding data from this process/stream
     */

    opal_buffer_t *buf;
    orte_iof_tag_t tag;
    orte_process_name_t hnp;
    int rc;

    OPAL_OUTPUT_VERBOSE((1, orte_iof_base_framework.framework_output,
                         "%s closing output for proc %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(src_name)));

    buf = OBJ_NEW(opal_buffer_t);

    /* setup the tag to stop the copy */
    tag = src_tag | ORTE_IOF_CLOSE;

    /* pack the tag - we do this first so that flow control messages can
     * consist solely of the tag
     */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buf, &tag, 1, ORTE_IOF_TAG))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buf);
        return rc;
    }
    /* pack the name of the source */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buf, src_name, 1, ORTE_NAME))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buf);
        return rc;
    }

    /* flag that the close is incomplete */
    mca_iof_tool_component.closed = false;

    /* send the buffer to the correct HNP */
    ORTE_HNP_NAME_FROM_JOB(&hnp, src_name->jobid);
    orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                    &hnp, buf, ORTE_RML_TAG_IOF_HNP,
                                    send_cb, NULL);

    return ORTE_SUCCESS;
}

static int finalize(void)
{
    opal_list_item_t* item;
    orte_iof_write_output_t *output;
    orte_iof_write_event_t *wev;
    int num_written;
    bool dump;

    /* check if anything is still trying to be written out */
    wev = orte_iof_base.iof_write_stdout->wev;
    if (!opal_list_is_empty(&wev->outputs)) {
        dump = false;
        /* make one last attempt to write this out */
        while (NULL != (item = opal_list_remove_first(&wev->outputs))) {
            output = (orte_iof_write_output_t*)item;
            if (!dump) {
                num_written = write(wev->fd, output->data, output->numbytes);
                if (num_written < output->numbytes) {
                    /* don't retry - just cleanout the list and dump it */
                    dump = true;
                }
            }
            OBJ_RELEASE(output);
        }
    }
    OBJ_RELEASE(orte_iof_base.iof_write_stdout);
    if (!orte_xml_output) {
        /* we only opened stderr channel if we are NOT doing xml output */
        wev = orte_iof_base.iof_write_stderr->wev;
        if (!opal_list_is_empty(&wev->outputs)) {
            dump = false;
            /* make one last attempt to write this out */
            while (NULL != (item = opal_list_remove_first(&wev->outputs))) {
                output = (orte_iof_write_output_t*)item;
                if (!dump) {
                    num_written = write(wev->fd, output->data, output->numbytes);
                    if (num_written < output->numbytes) {
                        /* don't retry - just cleanout the list and dump it */
                        dump = true;
                    }
                }
                OBJ_RELEASE(output);
            }
        }
        OBJ_RELEASE(orte_iof_base.iof_write_stderr);
    }

    /* Cancel the RML receive */
    orte_rml.recv_cancel(ORTE_NAME_WILDCARD, ORTE_RML_TAG_IOF_PROXY);

    return ORTE_SUCCESS;
}

static int tool_output(const orte_process_name_t* peer,
                       orte_iof_tag_t source_tag,
                       const char *msg)
{
    /* output this to our local output */
    if (ORTE_IOF_STDOUT & source_tag || orte_xml_output) {
        orte_iof_base_write_output(peer, source_tag, (const unsigned char*)msg, strlen(msg), orte_iof_base.iof_write_stdout->wev);
    } else {
        orte_iof_base_write_output(peer, source_tag, (const unsigned char*)msg, strlen(msg), orte_iof_base.iof_write_stderr->wev);
    }

    return ORTE_SUCCESS;
}

/*
 * FT event
 */

static int tool_ft_event(int state)
{
    return ORTE_ERR_NOT_IMPLEMENTED;
}
