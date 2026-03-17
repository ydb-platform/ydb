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

#include "opal/dss/dss.h"

#include "orte/mca/rml/rml.h"
#include "orte/mca/rml/rml_types.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/util/name_fns.h"
#include "orte/runtime/orte_globals.h"

#include "orte/mca/iof/iof.h"
#include "orte/mca/iof/base/base.h"

#include "iof_tool.h"


void orte_iof_tool_recv(int status, orte_process_name_t* sender,
                        opal_buffer_t* buffer, orte_rml_tag_t tag,
                        void* cbdata)
{
    orte_process_name_t origin;
    unsigned char data[ORTE_IOF_BASE_MSG_MAX];
    orte_iof_tag_t stream;
    int32_t count, numbytes;
    int rc;


    /* unpack the stream first as this may be flow control info */
    count = 1;
    if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &stream, &count, ORTE_IOF_TAG))) {
        ORTE_ERROR_LOG(rc);
        goto CLEAN_RETURN;
    }

    /* if this is a CLOSE tag, then ignore the rest - this is just the
     * tail end of a handshake to indicate we have closed a stream
     */
    if (ORTE_IOF_CLOSE & stream) {
        OPAL_OUTPUT_VERBOSE((1, orte_iof_base_framework.framework_output,
                             "%s received CLOSE handshake from remote hnp %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(sender)));
        mca_iof_tool_component.closed = true;
        goto CLEAN_RETURN;
    }

    /* get name of the process whose io we are receiving */
    count = 1;
    if (ORTE_SUCCESS != (rc = opal_dss.unpack(buffer, &origin, &count, ORTE_NAME))) {
        ORTE_ERROR_LOG(rc);
        goto CLEAN_RETURN;
    }

    /* unpack the data */
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

    /* if numbytes is zero, it means that the channel was closed on the far end - for
     * now, we just ignore this condition
     */
    if (0 < numbytes) {
        /* write the output locally */
        if (ORTE_IOF_STDOUT & stream) {
            orte_iof_base_write_output(&origin, stream, data, numbytes, orte_iof_base.iof_write_stdout->wev);
        } else {
            orte_iof_base_write_output(&origin, stream, data, numbytes, orte_iof_base.iof_write_stderr->wev);
        }
    }

CLEAN_RETURN:
    return;
}
