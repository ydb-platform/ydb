/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2012-2014 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014-2015 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2014      Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include "opal/util/argv.h"
#include "opal/util/output.h"
#include "opal/util/show_help.h"
#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/mca/base/mca_base_component_repository.h"
#include "opal/runtime/opal.h"
#include "opal/mca/btl/btl.h"
#include "opal/mca/btl/base/btl_base_error.h"
#include "opal/mca/btl/base/base.h"

OBJ_CLASS_INSTANCE( mca_btl_base_selected_module_t,
                    opal_list_item_t,
                    NULL,
                    NULL );

/**
 * Function for weeding out btl components that don't want to run.
 *
 * Call the init function on all available components to find out if
 * they want to run.  Select all components that don't fail.  Failing
 * components will be closed and unloaded.  The selected modules will
 * be returned to the caller in a opal_list_t.
 */
int mca_btl_base_select(bool enable_progress_threads,
                        bool enable_mpi_threads)
{
    int i, num_btls;
    mca_base_component_list_item_t *cli, *next;
    mca_btl_base_component_t *component;
    mca_btl_base_module_t **modules;
    mca_btl_base_selected_module_t *sm;

    char** include = opal_argv_split(mca_btl_base_include, ',');
    char** exclude = opal_argv_split(mca_btl_base_exclude, ',');

    /* Traverse the list of opened modules; call their init
       functions. */

    OPAL_LIST_FOREACH_SAFE(cli, next, &opal_btl_base_framework.framework_components, mca_base_component_list_item_t) {
        component = (mca_btl_base_component_t *) cli->cli_component;

        /* if there is an include list - item must be in the list to be included */
        if ( NULL != include ) {
            char** argv = include;
            bool found = false;
            while(argv && *argv) {
                if(strcmp(component->btl_version.mca_component_name,*argv) == 0) {
                    found = true;
                    break;
                }
                argv++;
            }
            if(found == false) {
                continue;
            }

            /* otherwise - check the exclude list to see if this item has been specifically excluded */
        } else if ( NULL != exclude ) {
            char** argv = exclude;
            bool found = false;
            while(argv && *argv) {
                if(strcmp(component->btl_version.mca_component_name,*argv) == 0) {
                    found = true;
                    break;
                }
                argv++;
            }
            if(found == true) {
                continue;
            }
        }

        opal_output_verbose(10, opal_btl_base_framework.framework_output,
                            "select: initializing %s component %s",
                            component->btl_version.mca_type_name,
                            component->btl_version.mca_component_name);
        if (NULL == component->btl_init) {
            opal_output_verbose(10, opal_btl_base_framework.framework_output,
                                "select: no init function; ignoring component %s",
                                component->btl_version.mca_component_name);
        } else {
            modules = component->btl_init(&num_btls, enable_progress_threads,
                                          enable_mpi_threads);

            /* If the component didn't initialize, remove it from the opened
               list and remove it from the component repository */

            if (NULL == modules) {
                opal_output_verbose(10, opal_btl_base_framework.framework_output,
                                    "select: init of component %s returned failure",
                                    component->btl_version.mca_component_name);

                opal_list_remove_item(&opal_btl_base_framework.framework_components, &cli->super);
                OBJ_RELEASE(cli);
                mca_base_component_close((mca_base_component_t *) component,
                                         opal_btl_base_framework.framework_output);
            }

            /* Otherwise, if it initialized properly, save it. */

            else {
                opal_output_verbose(10, opal_btl_base_framework.framework_output,
                                    "select: init of component %s returned success",
                                    component->btl_version.mca_component_name);

                for (i = 0; i < num_btls; ++i) {
                    /* If modules[i] is NULL, it's a developer error */
                    if (NULL == modules[i]) {
                        BTL_ERROR(("BTL module init of %s returned a NULL -- this should never happen, and is a developer error.  Contact the Open MPI developers.",
                                   component->btl_version.mca_component_name));
                        exit(1);
                    }
                    sm = OBJ_NEW(mca_btl_base_selected_module_t);
                    if (NULL == sm) {
                        if (NULL != include) {
                            opal_argv_free(include);
                        }
                        if (NULL != exclude) {
                            opal_argv_free(exclude);
                        }
                        return OPAL_ERR_OUT_OF_RESOURCE;
                    }
                    sm->btl_component = component;
                    sm->btl_module = modules[i];
                    opal_list_append(&mca_btl_base_modules_initialized,
                                     (opal_list_item_t*) sm);
                }
                free(modules);
            }
        }
    }

    if (NULL != include) {
        opal_argv_free(include);
    }
    if (NULL != exclude) {
        opal_argv_free(exclude);
    }

    /* Finished querying all components.  Check for the bozo case. */

    if (0 == opal_list_get_size(&mca_btl_base_modules_initialized)) {
        opal_show_help("help-mca-base.txt", "find-available:none found",
                       true,
                       "btl",
                       opal_process_info.nodename,
                       "btl");
        return OPAL_ERROR;
    }
    return OPAL_SUCCESS;
}
