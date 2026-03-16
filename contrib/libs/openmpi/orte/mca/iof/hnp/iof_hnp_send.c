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
 * Copyright (c) 2012      Los Alamos National Security, LLC
 *                         All rights reserved
 * Copyright (c) 2014-2016 Intel, Inc.  All rights reserved.
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
#include "orte/runtime/orte_globals.h"
#include "orte/mca/grpcomm/grpcomm.h"
#include "orte/util/name_fns.h"

#include "orte/mca/iof/iof.h"
#include "orte/mca/iof/base/base.h"

#include "iof_hnp.h"

int orte_iof_hnp_send_data_to_endpoint(orte_process_name_t *host,
                                       orte_process_name_t *target,
                                       orte_iof_tag_t tag,
                                       unsigned char *data, int numbytes)
{
    opal_buffer_t *buf;
    int rc;
    orte_grpcomm_signature_t *sig;

    /* if the host is a daemon and we are in the process of aborting,
     * then ignore this request. We leave it alone if the host is not
     * a daemon because it might be a tool that wants to watch the
     * output from an abort procedure
     */
    if (ORTE_JOB_FAMILY(host->jobid) == ORTE_JOB_FAMILY(ORTE_PROC_MY_NAME->jobid)
        && orte_job_term_ordered) {
        return ORTE_SUCCESS;
    }

    buf = OBJ_NEW(opal_buffer_t);

    /* pack the tag - we do this first so that flow control messages can
     * consist solely of the tag
     */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buf, &tag, 1, ORTE_IOF_TAG))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buf);
        return rc;
    }
    /* pack the name of the target - this is either the intended
     * recipient (if the tag is stdin and we are sending to a daemon),
     * or the source (if we are sending to anyone else)
     */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buf, target, 1, ORTE_NAME))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buf);
        return rc;
    }

    /* if data is NULL, then we are done */
    if (NULL != data) {
        /* pack the data - if numbytes is zero, we will pack zero bytes */
        if (ORTE_SUCCESS != (rc = opal_dss.pack(buf, data, numbytes, OPAL_BYTE))) {
            ORTE_ERROR_LOG(rc);
            OBJ_RELEASE(buf);
            return rc;
        }
    }

    /* if the target is wildcard, then this needs to go to everyone - xcast it */
    if (ORTE_PROC_MY_NAME->jobid == host->jobid &&
        ORTE_VPID_WILDCARD == host->vpid) {
        /* xcast this to everyone - the local daemons will know how to handle it */
        sig = OBJ_NEW(orte_grpcomm_signature_t);
        sig->signature = (orte_process_name_t*)malloc(sizeof(orte_process_name_t));
        sig->signature[0].jobid = ORTE_PROC_MY_NAME->jobid;
        sig->signature[0].vpid = ORTE_VPID_WILDCARD;
        (void)orte_grpcomm.xcast(sig, ORTE_RML_TAG_IOF_PROXY, buf);
        OBJ_RELEASE(buf);
        OBJ_RELEASE(sig);
        return ORTE_SUCCESS;
    }

    /* send the buffer to the host - this is either a daemon or
     * a tool that requested IOF
     */
    if (0 > (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                                  host, buf, ORTE_RML_TAG_IOF_PROXY,
                                                  orte_rml_send_callback, NULL))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }

    return ORTE_SUCCESS;
}
