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
 * Copyright (c) 2008-2009 Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mpi.h"

#include "opal/class/opal_list.h"
#include "opal/util/output.h"
#include "ompi/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/mca/base/mca_base_component_repository.h"


#include "ompi/constants.h"
#include "ompi/mca/op/op.h"
#include "ompi/mca/op/base/base.h"

/*
 * Private functions
 */
static int init_query(const mca_base_component_t * ls,
                      bool enable_progress_threads,
                      bool enable_mpi_threads);
static int init_query_1_0_0(const mca_base_component_t * ls,
                            bool enable_progress_threads,
                            bool enable_mpi_threads);

/*
 * Scan down the list of successfully opened components and query each
 * of them (the opened list will be one or more components.  If the
 * user requested a specific set of components, they will be the only
 * components in the opened list).  Create and populate the available
 * list of all components who indicate that they want to be considered
 * for selection.  Close all components who do not want to be
 * considered for selection.  Finally, destroy the "opened" list,
 * because the only the "available" list is relevant now.
 */
int ompi_op_base_find_available(bool enable_progress_threads,
                                bool enable_mpi_threads)
{
    mca_base_component_list_item_t *cli, *next;

    /* The list of components that we should check has already been
       established in ompi_op_base_open. */

    OPAL_LIST_FOREACH_SAFE(cli, next, &ompi_op_base_framework.framework_components, mca_base_component_list_item_t) {
        const mca_base_component_t *component = cli->cli_component;

        /* Call a subroutine to do the work, because the component may
           represent different versions of the op MCA. */

        if (OMPI_SUCCESS != init_query(component,
                                       enable_progress_threads,
                                       enable_mpi_threads)) {

            /* If the component doesn't want to run, then close it. */
            opal_list_remove_item(&ompi_op_base_framework.framework_components, &cli->super);
            mca_base_component_close(component, ompi_op_base_framework.framework_output);
            OBJ_RELEASE(cli);
        }
    }

    /* It is not an error if there are no components available; we'll
       just fall back to the base functions. */

    return OMPI_SUCCESS;
}


/*
 * Query a component, see if it wants to run at all.  If it does, save
 * some information.  If it doesn't, close it.
 */
static int init_query(const mca_base_component_t * c,
                      bool enable_progress_threads, bool enable_mpi_threads)
{
    int ret;

    opal_output_verbose(10, ompi_op_base_framework.framework_output,
                        "op:find_available: querying op component %s",
                        c->mca_component_name);

    /* This component has already been successfully opened.  So now
       query it. */

    if (1 == c->mca_type_major_version &&
        0 == c->mca_type_minor_version &&
        0 == c->mca_type_release_version) {
        ret = init_query_1_0_0(c, enable_progress_threads,
                               enable_mpi_threads);
    } else {
        /* Unrecognized op API version */

        opal_output_verbose(10, ompi_op_base_framework.framework_output,
                            "op:find_available: unrecognized op API version (%d.%d.%d, ignored)",
                            c->mca_type_major_version,
                            c->mca_type_minor_version,
                            c->mca_type_release_version);
        return OMPI_ERROR;
    }

    /* Query done -- look at the return value to see what happened */

    if (OMPI_SUCCESS != ret) {
        opal_output_verbose(10, ompi_op_base_framework.framework_output,
                            "op:find_available: op component %s is not available",
                            c->mca_component_name);
    } else {
        opal_output_verbose(10, ompi_op_base_framework.framework_output,
                            "op:find_available: op component %s is available",
                            c->mca_component_name);
    }

    /* All done */

    return ret;
}


/*
 * Query a specific component, op v2.0.0
 */
static int init_query_1_0_0(const mca_base_component_t * component,
                            bool enable_progress_threads,
                            bool enable_mpi_threads)
{
    ompi_op_base_component_1_0_0_t *op =
        (ompi_op_base_component_1_0_0_t *) component;

    return op->opc_init_query(enable_progress_threads,
                              enable_mpi_threads);
}
