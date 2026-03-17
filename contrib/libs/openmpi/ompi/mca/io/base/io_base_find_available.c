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
 * Copyright (c) 2008      Sun Microsystems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <stdio.h>
#include <stdlib.h>

#include "mpi.h"
#include "ompi/constants.h"
#include "opal/util/output.h"
#include "ompi/mca/mca.h"
#include "opal/mca/base/base.h"
#include "ompi/mca/io/io.h"
#include "ompi/mca/io/base/base.h"
#include "ompi/mca/io/base/io_base_request.h"



/*
 * Private functions
 */
static int init_query(const mca_base_component_t *ls,
                      bool enable_progress_threads,
                      bool enable_mpi_threads);
static int init_query_2_0_0(const mca_base_component_t *ls,
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
 * It is *not* an error if there are no io components available.
 * Appropriate run-time MPI exceptions will be invoked during
 * MPI_FILE_OPEN and MPI_FILE_DELETE.
 */
int mca_io_base_find_available(bool enable_progress_threads,
                               bool enable_mpi_threads)
{
    mca_base_component_list_item_t *cli, *next;

    /* The list of components that we should check has already been
       established in mca_io_base_open. */

    OPAL_LIST_FOREACH_SAFE(cli, next, &ompi_io_base_framework.framework_components, mca_base_component_list_item_t) {
        const mca_base_component_t *component = cli->cli_component;

        /* Call a subroutine to do the work, because the component may
           represent different versions of the io MCA. */

        if (OMPI_SUCCESS != init_query(component,
                                       enable_progress_threads,
                                       enable_mpi_threads)) {

            /* If the component doesn't want to run, then close it.
               It's already had its close() method invoked; now close
               it out of the DSO repository (if it's there). */
            opal_list_remove_item(&ompi_io_base_framework.framework_components, &cli->super);
            mca_base_component_close(component, ompi_io_base_framework.framework_output);
            OBJ_RELEASE(cli);
        }
    }

    /* All done */

    return OMPI_SUCCESS;
}


/*
 * Query a component, see if it wants to run at all.  If it does, save
 * some information.  If it doesn't, close it.
 */
static int init_query(const mca_base_component_t *m,
                      bool enable_progress_threads,
                      bool enable_mpi_threads)
{
    int ret;

    opal_output_verbose(10, ompi_io_base_framework.framework_output,
                        "io:find_available: querying io component %s",
                        m->mca_component_name);

    /* This component has already been successfully opened.  So now
       query it. */

    if (2 == m->mca_type_major_version &&
        0 == m->mca_type_minor_version &&
        0 == m->mca_type_release_version) {
        ret = init_query_2_0_0(m, enable_progress_threads,
                               enable_mpi_threads);
    } else {
        /* Unrecognized io API version */

        opal_output_verbose(10, ompi_io_base_framework.framework_output,
                            "io:find_available: unrecognized io API version (%d.%d.%d)",
                            m->mca_type_major_version,
                            m->mca_type_minor_version,
                            m->mca_type_release_version);
        /* JMS show_help */
        return OMPI_ERROR;
    }

    /* Query done -- look at the return value to see what happened */

    if (OMPI_SUCCESS != ret) {
        opal_output_verbose(10, ompi_io_base_framework.framework_output,
                            "io:find_available: io component %s is not available",
                            m->mca_component_name);
    } else {
        opal_output_verbose(10, ompi_io_base_framework.framework_output,
                            "io:find_available: io component %s is available",
                            m->mca_component_name);
    }

    /* All done */

    return ret;
}


/*
 * Query a specific component, io v2.0.0
 */
static int init_query_2_0_0(const mca_base_component_t *component,
                            bool enable_progress_threads,
                            bool enable_mpi_threads)
{
    mca_io_base_component_2_0_0_t *io =
	(mca_io_base_component_2_0_0_t *) component;

    return io->io_init_query(enable_progress_threads,
                             enable_mpi_threads);
}
