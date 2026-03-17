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
 * Copyright (c) 2016-2017 Intel, Inc. All rights reserved.
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
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/odls/odls_types.h"
#include "orte/util/name_fns.h"
#include "orte/util/threads.h"
#include "orte/mca/state/state.h"
#include "orte/runtime/orte_globals.h"

#include "orte/mca/iof/iof.h"
#include "orte/mca/iof/base/base.h"

#include "iof_orted.h"

void orte_iof_orted_read_handler(int fd, short event, void *cbdata)
{
    orte_iof_read_event_t *rev = (orte_iof_read_event_t*)cbdata;
    unsigned char data[ORTE_IOF_BASE_MSG_MAX];
    opal_buffer_t *buf=NULL;
    int rc;
    int32_t numbytes;
    orte_iof_proc_t *proct = (orte_iof_proc_t*)rev->proc;

    ORTE_ACQUIRE_OBJECT(rev);

    /* As we may use timer events, fd can be bogus (-1)
     * use the right one here
     */
    fd = rev->fd;

    /* read up to the fragment size */
#if !defined(__WINDOWS__)
    numbytes = read(fd, data, sizeof(data));
#else
    {
        DWORD readed;
        HANDLE handle = (HANDLE)_get_osfhandle(fd);
        ReadFile(handle, data, sizeof(data), &readed, NULL);
        numbytes = (int)readed;
    }
#endif  /* !defined(__WINDOWS__) */

    if (NULL == proct) {
        /* nothing we can do */
        ORTE_ERROR_LOG(ORTE_ERR_ADDRESSEE_UNKNOWN);
        return;
    }

    OPAL_OUTPUT_VERBOSE((1, orte_iof_base_framework.framework_output,
                         "%s iof:orted:read handler read %d bytes from %s, fd %d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         numbytes, ORTE_NAME_PRINT(&proct->name), fd));

    if (numbytes <= 0) {
        if (0 > numbytes) {
            /* either we have a connection error or it was a non-blocking read */
            if (EAGAIN == errno || EINTR == errno) {
                /* non-blocking, retry */
                ORTE_IOF_READ_ACTIVATE(rev);
                return;
            }

            OPAL_OUTPUT_VERBOSE((1, orte_iof_base_framework.framework_output,
                                 "%s iof:orted:read handler %s Error on connection:%d",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_NAME_PRINT(&proct->name), fd));
        }
        /* numbytes must have been zero, so go down and close the fd etc */
        goto CLEAN_RETURN;
    }

    /* see if the user wanted the output directed to files */
    if (NULL != rev->sink) {
        /* output to the corresponding file */
        orte_iof_base_write_output(&proct->name, rev->tag, data, numbytes, rev->sink->wev);
    }
    if (!proct->copy) {
        /* re-add the event */
        ORTE_IOF_READ_ACTIVATE(rev);
        return;
    }

    /* prep the buffer */
    buf = OBJ_NEW(opal_buffer_t);

    /* pack the stream first - we do this so that flow control messages can
     * consist solely of the tag
     */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buf, &rev->tag, 1, ORTE_IOF_TAG))) {
        ORTE_ERROR_LOG(rc);
        goto CLEAN_RETURN;
    }

    /* pack name of process that gave us this data */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buf, &proct->name, 1, ORTE_NAME))) {
        ORTE_ERROR_LOG(rc);
        goto CLEAN_RETURN;
    }

    /* pack the data - only pack the #bytes we read! */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buf, &data, numbytes, OPAL_BYTE))) {
        ORTE_ERROR_LOG(rc);
        goto CLEAN_RETURN;
    }

    /* start non-blocking RML call to forward received data */
    OPAL_OUTPUT_VERBOSE((1, orte_iof_base_framework.framework_output,
                         "%s iof:orted:read handler sending %d bytes to HNP",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), numbytes));

    orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                    ORTE_PROC_MY_HNP, buf, ORTE_RML_TAG_IOF_HNP,
                                    orte_rml_send_callback, NULL);

    /* re-add the event */
    ORTE_IOF_READ_ACTIVATE(rev);

    return;

 CLEAN_RETURN:
    /* must be an error, or zero bytes were read indicating that the
     * proc terminated this IOF channel - either way, release the
     * corresponding event. This deletes the read event and closes
     * the file descriptor */
    if (rev->tag & ORTE_IOF_STDOUT) {
        if( NULL != proct->revstdout ) {
            orte_iof_base_static_dump_output(proct->revstdout);
            OBJ_RELEASE(proct->revstdout);
        }
    } else if (rev->tag & ORTE_IOF_STDERR) {
        if( NULL != proct->revstderr ) {
            orte_iof_base_static_dump_output(proct->revstderr);
            OBJ_RELEASE(proct->revstderr);
        }
#if OPAL_PMIX_V1
    } else if (rev->tag & ORTE_IOF_STDDIAG) {
        if( NULL != proct->revstddiag ) {
            orte_iof_base_static_dump_output(proct->revstddiag);
            OBJ_RELEASE(proct->revstddiag);
        }
#endif
    }
    /* check to see if they are all done */
    if (NULL == proct->revstdout &&
#if OPAL_PMIX_V1
        NULL == proct->revstddiag &&
#endif
        NULL == proct->revstderr) {
        /* this proc's iof is complete */
        ORTE_ACTIVATE_PROC_STATE(&proct->name, ORTE_PROC_STATE_IOF_COMPLETE);
    }
    if (NULL != buf) {
        OBJ_RELEASE(buf);
    }
    return;
}
