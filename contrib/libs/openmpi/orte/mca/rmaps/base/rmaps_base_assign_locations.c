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
 * Copyright (c) 2011-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011-2012 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014-2017 Intel, Inc.  All rights reserved.
 * Copyright (c) 2016      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"

#include <string.h>

#include "orte/mca/mca.h"
#include "opal/util/output.h"
#include "opal/mca/base/base.h"

#include "orte/runtime/orte_globals.h"
#include "orte/util/show_help.h"
#include "orte/mca/errmgr/errmgr.h"

#include "orte/mca/rmaps/base/base.h"
#include "orte/mca/rmaps/base/rmaps_private.h"


int orte_rmaps_base_assign_locations(orte_job_t *jdata)
{
    int rc;
    orte_rmaps_base_selected_module_t *mod;

    opal_output_verbose(5, orte_rmaps_base_framework.framework_output,
                        "mca:rmaps: assigning locations for job %s",
                        ORTE_JOBID_PRINT(jdata->jobid));

    /* cycle thru the available mappers until one agrees to assign
     * locations for the job
     */
    if (1 == opal_list_get_size(&orte_rmaps_base.selected_modules)) {
        /* forced selection */
        mod = (orte_rmaps_base_selected_module_t*)opal_list_get_first(&orte_rmaps_base.selected_modules);
        jdata->map->req_mapper = strdup(mod->component->mca_component_name);
    }
    OPAL_LIST_FOREACH(mod, &orte_rmaps_base.selected_modules, orte_rmaps_base_selected_module_t) {
        if (NULL == mod->module->assign_locations) {
            continue;
        }
        if (ORTE_SUCCESS == (rc = mod->module->assign_locations(jdata))) {
            return rc;
        }
        /* mappers return "next option" if they didn't attempt to
         * process the job. anything else is a true error.
         */
        if (ORTE_ERR_TAKE_NEXT_OPTION != rc) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
    }

    /* if we get here without doing the assignments, then that's an error */
    orte_show_help("help-orte-rmaps-base.txt", "failed-assignments", true,
                   orte_process_info.nodename,
                   orte_rmaps_base_print_mapping(jdata->map->mapping));
    return ORTE_ERROR;
}
