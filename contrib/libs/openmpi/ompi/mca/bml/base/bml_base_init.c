/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2007 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "ompi/mca/bml/base/base.h"
#include "opal/mca/base/base.h"

#include "ompi/mca/mca.h"
#include "opal/runtime/opal.h"

mca_bml_base_module_t mca_bml = {
    NULL,                    /* bml_component */
    NULL,                    /* bml_add_procs */
    NULL,                    /* bml_del_procs */
    NULL,                    /* bml_add_btl */
    NULL,                    /* bml_del_btl */
    NULL,                    /* bml_del_proc_btl */
    NULL,                    /* bml_register */
    NULL,                    /* bml_register_error */
    NULL,                    /* bml_finalize*/
    NULL                     /* FT event */
};
mca_bml_base_component_t mca_bml_component = {{0}};

static bool init_called = false;

bool
mca_bml_base_inited(void)
{
    return init_called;
}

int mca_bml_base_init( bool enable_progress_threads,
                       bool enable_mpi_threads) {
    mca_bml_base_component_t *component = NULL, *best_component = NULL;
    mca_bml_base_module_t *module = NULL, *best_module = NULL;
    int priority = 0, best_priority = -1;
    mca_base_component_list_item_t *cli = NULL;

    if (init_called) {
        return OPAL_SUCCESS;
    }

    init_called = true;

    OPAL_LIST_FOREACH(cli, &ompi_bml_base_framework.framework_components, mca_base_component_list_item_t) {
        component = (mca_bml_base_component_t*) cli->cli_component;
        if(NULL == component->bml_init) {
            opal_output_verbose( 10, ompi_bml_base_framework.framework_output,
                                 "select: no init function; ignoring component %s",
                                 component->bml_version.mca_component_name );
            continue;
        }
        module = component->bml_init(&priority,
                                     enable_progress_threads,
                                     enable_mpi_threads);

        if(NULL == module) {
            continue;
        }
        if(priority > best_priority) {
            best_priority = priority;
            best_component = component;
            best_module = module;
        }

    }
    if(NULL == best_module) {
        return OMPI_SUCCESS;
    }

    mca_bml_component = *best_component;
    mca_bml = *best_module;
    return mca_base_framework_components_close(&ompi_bml_base_framework,
                                               (mca_base_component_t*) best_component);
}
