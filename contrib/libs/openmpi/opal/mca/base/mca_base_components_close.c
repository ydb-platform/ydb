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
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include "opal/class/opal_list.h"
#include "opal/util/output.h"
#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/mca/base/mca_base_component_repository.h"
#include "opal/constants.h"

void mca_base_component_unload (const mca_base_component_t *component, int output_id)
{
    int ret;

    /* Unload */
    opal_output_verbose (MCA_BASE_VERBOSE_COMPONENT, output_id,
                         "mca: base: close: unloading component %s",
                         component->mca_component_name);

    ret = mca_base_var_group_find (component->mca_project_name, component->mca_type_name,
                                   component->mca_component_name);
    if (0 <= ret) {
        mca_base_var_group_deregister (ret);
    }

    mca_base_component_repository_release (component);
}

void mca_base_component_close (const mca_base_component_t *component, int output_id)
{
    /* Close */
    if (NULL != component->mca_close_component) {
        component->mca_close_component();
        opal_output_verbose (MCA_BASE_VERBOSE_COMPONENT, output_id,
                             "mca: base: close: component %s closed",
                             component->mca_component_name);
    }

    mca_base_component_unload (component, output_id);
}

int mca_base_framework_components_close (mca_base_framework_t *framework,
                                         const mca_base_component_t *skip)
{
    return mca_base_components_close (framework->framework_output,
                                      &framework->framework_components,
                                      skip);
}

int mca_base_components_close(int output_id, opal_list_t *components,
                              const mca_base_component_t *skip)
{
    mca_base_component_list_item_t *cli, *next;

    /* Close and unload all components in the available list, except the
       "skip" item.  This is handy to close out all non-selected
       components.  It's easier to simply remove the entire list and
       then simply re-add the skip entry when done. */

    OPAL_LIST_FOREACH_SAFE(cli, next, components, mca_base_component_list_item_t) {
        if (skip == cli->cli_component) {
            continue;
        }

        mca_base_component_close (cli->cli_component, output_id);
        opal_list_remove_item (components, &cli->super);

        OBJ_RELEASE(cli);
    }

    /* All done */
    return OPAL_SUCCESS;
}
