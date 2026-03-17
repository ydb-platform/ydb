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
 * Copyright (c) 2016-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file */

#include "orte_config.h"
#include "orte/constants.h"
#include "orte/types.h"

#include "opal/util/argv.h"
#include "opal/util/output.h"

#include "opal/dss/dss.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/routed/routed.h"
#include "orte/util/name_fns.h"
#include "orte/util/proc_info.h"
#include "orte/runtime/orte_globals.h"

#include "orte/mca/rml/rml.h"
#include "orte/mca/rml/base/rml_contact.h"
#include "orte/mca/rml/base/base.h"


int orte_rml_base_parse_uris(const char* uri,
                             orte_process_name_t* peer,
                             char*** uris)
{
    int rc;

    /* parse the process name */
    char* cinfo = strdup(uri);
    char* ptr = strchr(cinfo, ';');
    if(NULL == ptr) {
        ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
        free(cinfo);
        return ORTE_ERR_BAD_PARAM;
    }
    *ptr = '\0';
    ptr++;
    if (ORTE_SUCCESS != (rc = orte_util_convert_string_to_process_name(peer, cinfo))) {
        ORTE_ERROR_LOG(rc);
        free(cinfo);
        return rc;
    }

    if (NULL != uris) {
        /* parse the remainder of the string into an array of uris */
        *uris = opal_argv_split(ptr, ';');
    }
    free(cinfo);
    return ORTE_SUCCESS;
}
