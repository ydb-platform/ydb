/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2014 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
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
#include "opal/util/show_help.h"
#include "ompi/constants.h"
#include "opal/class/opal_list.h"
#include "opal/util/output.h"
#include "ompi/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/mca/base/mca_base_component_repository.h"

#include "ompi/mca/rte/rte.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/mca/coll/base/base.h"

/*
 * Private functions
 */
static int init_query(const mca_base_component_t * ls,
                      bool enable_progress_threads,
                      bool enable_mpi_threads);

/*
 * Scan down the list of successfully opened components and query each of
 * them (the opened list will be one or more components.  If the user
 * requested a specific component, it will be the only component in the
 * opened list).  Create and populate the available list of all
 * components who indicate that they want to be considered for selection.
 * Close all components who do not want to be considered for selection,
 * and destroy the opened list.
 *
 * Also find the basic component while we're doing all of this, and save
 * it in a global variable so that we can find it easily later (e.g.,
 * during scope selection).
 */
int mca_coll_base_find_available(bool enable_progress_threads,
                                 bool enable_mpi_threads)
{
    mca_base_component_list_item_t *cli, *next;
    const mca_base_component_t *component;

    /* The list of components that we should check has already been
       established in mca_coll_base_open. */

    OPAL_LIST_FOREACH_SAFE(cli, next, &ompi_coll_base_framework.framework_components, mca_base_component_list_item_t) {
        component = cli->cli_component;

        /* Call a subroutine to do the work, because the component may
           represent different versions of the coll MCA. */

        if (OMPI_SUCCESS != init_query(component, enable_progress_threads,
                                       enable_mpi_threads)) {
            /* If the component doesn't want to run, then close it.
               Now close it out and release it from the DSO repository (if it's there). */
            opal_list_remove_item(&ompi_coll_base_framework.framework_components, &cli->super);
            mca_base_component_close(component, ompi_coll_base_framework.framework_output);
            OBJ_RELEASE(cli);
        }
    }

    /* If we have no collective components available, it's an error.
       Thanks for playing! */

    if (opal_list_get_size(&ompi_coll_base_framework.framework_components) == 0) {
        opal_output_verbose(10, ompi_coll_base_framework.framework_output,
                            "coll:find_available: no coll components available!");
        opal_show_help("help-mca-base.txt", "find-available:not-valid", true,
                       "coll");
        return OMPI_ERROR;
    }

    /* All done */

    return OMPI_SUCCESS;
}


/*
 * Query a specific component, coll v2.0.0
 */
static inline int
init_query_2_0_0(const mca_base_component_t * component,
                 bool enable_progress_threads,
                 bool enable_mpi_threads)
{
    mca_coll_base_component_2_0_0_t *coll =
        (mca_coll_base_component_2_0_0_t *) component;

    return coll->collm_init_query(enable_progress_threads,
                                  enable_mpi_threads);
}
/*
 * Query a component, see if it wants to run at all.  If it does, save
 * some information.  If it doesn't, close it.
 */
static int init_query(const mca_base_component_t * component,
                      bool enable_progress_threads, bool enable_mpi_threads)
{
    int ret;

    opal_output_verbose(10, ompi_coll_base_framework.framework_output,
                        "coll:find_available: querying coll component %s",
                        component->mca_component_name);

    /* This component has already been successfully opened.  So now
       query it. */

    if (2 == component->mca_type_major_version &&
        0 == component->mca_type_minor_version &&
        0 == component->mca_type_release_version) {
        ret = init_query_2_0_0(component, enable_progress_threads,
                               enable_mpi_threads);
    } else {
        /* Unrecognized coll API version */

        opal_output_verbose(10, ompi_coll_base_framework.framework_output,
                            "coll:find_available: unrecognized coll API version (%d.%d.%d, ignored)",
                            component->mca_type_major_version,
                            component->mca_type_minor_version,
                            component->mca_type_release_version);
        return OMPI_ERROR;
    }

    /* Query done -- look at the return value to see what happened */
    opal_output_verbose(10, ompi_coll_base_framework.framework_output,
                        "coll:find_available: coll component %s is %savailable",
                        component->mca_component_name,
                        (OMPI_SUCCESS == ret) ? "": "not ");

    return ret;
}

