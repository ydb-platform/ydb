/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2012 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011-2015 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2016      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "src/class/pmix_list.h"
#include "src/util/argv.h"
#include "src/util/error.h"
#include "src/util/output.h"
#include "src/util/show_help.h"
#include "src/mca/mca.h"
#include "src/mca/base/base.h"
#include "src/mca/base/pmix_mca_base_framework.h"
#include "src/mca/base/pmix_mca_base_component_repository.h"
#include "pmix_common.h"

/*
 * Local functions
 */
static int register_components(pmix_mca_base_framework_t *framework);
/**
 * Function for finding and opening either all MCA components, or the
 * one that was specifically requested via a MCA parameter.
 */
int pmix_mca_base_framework_components_register (pmix_mca_base_framework_t *framework,
                                            pmix_mca_base_register_flag_t flags)
{
    bool open_dso_components = !(flags & PMIX_MCA_BASE_REGISTER_STATIC_ONLY);
    bool ignore_requested = !!(flags & PMIX_MCA_BASE_REGISTER_ALL);
    int ret;

    /* Find and load requested components */
    ret = pmix_mca_base_component_find(NULL, framework, ignore_requested, open_dso_components);
    if (PMIX_SUCCESS != ret) {
        return ret;
    }

    /* Register all remaining components */
    return register_components(framework);
}

/*
 * Traverse the entire list of found components (a list of
 * pmix_mca_base_component_t instances).  If the requested_component_names
 * array is empty, or the name of each component in the list of found
 * components is in the requested_components_array, try to open it.
 * If it opens, add it to the components_available list.
 */
static int register_components(pmix_mca_base_framework_t *framework)
{
    int ret;
    pmix_mca_base_component_t *component;
    pmix_mca_base_component_list_item_t *cli, *next;
    int output_id = framework->framework_output;

    /* Announce */
    pmix_output_verbose (PMIX_MCA_BASE_VERBOSE_COMPONENT, output_id,
                         "pmix:mca: base: components_register: registering framework %s components",
                         framework->framework_name);

    /* Traverse the list of found components */

    PMIX_LIST_FOREACH_SAFE(cli, next, &framework->framework_components, pmix_mca_base_component_list_item_t) {
        component = (pmix_mca_base_component_t *)cli->cli_component;

        pmix_output_verbose(PMIX_MCA_BASE_VERBOSE_COMPONENT, output_id,
                            "pmix:mca: base: components_register: found loaded component %s",
                            component->pmix_mca_component_name);

        /* Call the component's MCA parameter registration function (or open if register doesn't exist) */
        if (NULL == component->pmix_mca_register_component_params) {
            pmix_output_verbose (PMIX_MCA_BASE_VERBOSE_COMPONENT, output_id,
                                 "pmix:mca: base: components_register: "
                                 "component %s has no register or open function",
                                 component->pmix_mca_component_name);
            ret = PMIX_SUCCESS;
        } else {
            ret = component->pmix_mca_register_component_params();
        }

        if (PMIX_SUCCESS != ret) {
            if (PMIX_ERR_NOT_AVAILABLE != ret) {
                /* If the component returns PMIX_ERR_NOT_AVAILABLE,
                   it's a cue to "silently ignore me" -- it's not a
                   failure, it's just a way for the component to say
                   "nope!".

                   Otherwise, however, display an error.  We may end
                   up displaying this twice, but it may go to separate
                   streams.  So better to be redundant than to not
                   display the error in the stream where it was
                   expected. */

                if (pmix_mca_base_component_show_load_errors) {
                    pmix_output_verbose (PMIX_MCA_BASE_VERBOSE_ERROR, output_id,
                                         "pmix:mca: base: components_register: component %s "
                                         "/ %s register function failed",
                                         component->pmix_mca_type_name,
                                         component->pmix_mca_component_name);
                }

                pmix_output_verbose (PMIX_MCA_BASE_VERBOSE_COMPONENT, output_id,
                                     "pmix:mca: base: components_register: "
                                     "component %s register function failed",
                                     component->pmix_mca_component_name);
            }

            pmix_list_remove_item (&framework->framework_components, &cli->super);

            /* Release this list item */
            PMIX_RELEASE(cli);
            continue;
        }

        if (NULL != component->pmix_mca_register_component_params) {
            pmix_output_verbose (PMIX_MCA_BASE_VERBOSE_COMPONENT, output_id, "pmix:mca: base: components_register: "
                                 "component %s register function successful",
                                 component->pmix_mca_component_name);
        }

        /* Register this component's version */
        pmix_mca_base_component_var_register (component, "major_version", NULL, PMIX_MCA_BASE_VAR_TYPE_INT, NULL,
                                         0, PMIX_MCA_BASE_VAR_FLAG_DEFAULT_ONLY | PMIX_MCA_BASE_VAR_FLAG_INTERNAL,
                                         PMIX_INFO_LVL_9, PMIX_MCA_BASE_VAR_SCOPE_CONSTANT,
                                         &component->pmix_mca_component_major_version);
        pmix_mca_base_component_var_register (component, "minor_version", NULL, PMIX_MCA_BASE_VAR_TYPE_INT, NULL,
                                         0, PMIX_MCA_BASE_VAR_FLAG_DEFAULT_ONLY | PMIX_MCA_BASE_VAR_FLAG_INTERNAL,
                                         PMIX_INFO_LVL_9, PMIX_MCA_BASE_VAR_SCOPE_CONSTANT,
                                         &component->pmix_mca_component_minor_version);
        pmix_mca_base_component_var_register (component, "release_version", NULL, PMIX_MCA_BASE_VAR_TYPE_INT, NULL,
                                         0, PMIX_MCA_BASE_VAR_FLAG_DEFAULT_ONLY | PMIX_MCA_BASE_VAR_FLAG_INTERNAL,
                                         PMIX_INFO_LVL_9, PMIX_MCA_BASE_VAR_SCOPE_CONSTANT,
                                         &component->pmix_mca_component_release_version);
    }

    /* All done */

    return PMIX_SUCCESS;
}
