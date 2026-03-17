/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2006 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2013-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>

#include "src/class/pmix_list.h"
#include "src/util/output.h"
#include "src/mca/mca.h"
#include "src/mca/base/base.h"
#include "src/mca/base/pmix_mca_base_component_repository.h"
#include "pmix_common.h"

void pmix_mca_base_component_unload (const pmix_mca_base_component_t *component, int output_id)
{
    int ret;

    /* Unload */
    pmix_output_verbose (PMIX_MCA_BASE_VERBOSE_COMPONENT, output_id,
                         "mca: base: close: unloading component %s",
                         component->pmix_mca_component_name);

    ret = pmix_mca_base_var_group_find (component->pmix_mca_project_name, component->pmix_mca_type_name,
                                   component->pmix_mca_component_name);
    if (0 <= ret) {
        pmix_mca_base_var_group_deregister (ret);
    }

    pmix_mca_base_component_repository_release (component);
}

void pmix_mca_base_component_close (const pmix_mca_base_component_t *component, int output_id)
{
    /* Close */
    if (NULL != component->pmix_mca_close_component) {
        component->pmix_mca_close_component();
        pmix_output_verbose (PMIX_MCA_BASE_VERBOSE_COMPONENT, output_id,
                             "mca: base: close: component %s closed",
                             component->pmix_mca_component_name);
    }

    pmix_mca_base_component_unload (component, output_id);
}

int pmix_mca_base_framework_components_close (pmix_mca_base_framework_t *framework,
                                              const pmix_mca_base_component_t *skip)
{
    return pmix_mca_base_components_close (framework->framework_output,
                                           &framework->framework_components,
                                           skip);
}

int pmix_mca_base_components_close(int output_id, pmix_list_t *components,
                                   const pmix_mca_base_component_t *skip)
{
    pmix_mca_base_component_list_item_t *cli, *next;

    /* Close and unload all components in the available list, except the
       "skip" item.  This is handy to close out all non-selected
       components.  It's easier to simply remove the entire list and
       then simply re-add the skip entry when done. */

    PMIX_LIST_FOREACH_SAFE(cli, next, components, pmix_mca_base_component_list_item_t) {
        if (skip == cli->cli_component) {
            continue;
        }

        pmix_mca_base_component_close (cli->cli_component, output_id);
        pmix_list_remove_item (components, &cli->super);

        PMIX_RELEASE(cli);
    }

    /* All done */
    return PMIX_SUCCESS;
}
