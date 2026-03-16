/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2011 University of Houston. All rights reserved.
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
#include "ompi/mca/mca.h"
#include "opal/mca/base/base.h"
#include "ompi/mca/fs/fs.h"
#include "ompi/mca/fs/base/base.h"
#include "opal/util/output.h"

static int init_query(const mca_base_component_t *m,
                      bool enable_progress_threads,
                      bool enable_mpi_threads);
static int init_query_2_0_0(const mca_base_component_t *component,
                            bool enable_progress_threads,
                            bool enable_mpi_threads);

int mca_fs_base_find_available(bool enable_progress_threads,
                               bool enable_mpi_threads)
{
    mca_base_component_list_item_t *cli, *next;

    /* The list of components which we should check is already present
       in mca_fs_base_components_opened, which was established in
       mca_fs_base_open */

    OPAL_LIST_FOREACH_SAFE(cli, next, &ompi_fs_base_framework.framework_components, mca_base_component_list_item_t) {
        /* Now for this entry, we have to determine the thread level. Call
           a subroutine to do the job for us */

        if (OMPI_SUCCESS != init_query(cli->cli_component,
                                       enable_progress_threads,
                                       enable_mpi_threads)) {
            /* The component does not want to run, so close it. Its close()
               has already been invoked. Close it out of the DSO repository
               (if it is there in the repository) */
            opal_list_remove_item (&ompi_fs_base_framework.framework_components, &cli->super);
            mca_base_component_close(cli->cli_component, ompi_fs_base_framework.framework_output);
            OBJ_RELEASE(cli);
        }
    }

    /* There should atleast be one fs component which was available */
    if (0 == opal_list_get_size(&ompi_fs_base_framework.framework_components)) {
         opal_output_verbose (10, ompi_fs_base_framework.framework_output,
                              "fs:find_available: no fs components available!");
         return OMPI_ERROR;
     }

     /* All done */
     return OMPI_SUCCESS;
}


static int init_query(const mca_base_component_t *m,
                      bool enable_progress_threads,
                      bool enable_mpi_threads)
{
    int ret;

    opal_output_verbose(10, ompi_fs_base_framework.framework_output,
                        "fs:find_available: querying fs component %s",
                        m->mca_component_name);

    /* This component has been successfully opened, now try to query it */
    if (2 == m->mca_type_major_version &&
        0 == m->mca_type_minor_version &&
        0 == m->mca_type_release_version) {
        ret = init_query_2_0_0(m, enable_progress_threads,
                               enable_mpi_threads);
    } else {
        /* unrecognised API version */
        opal_output_verbose(10, ompi_fs_base_framework.framework_output,
                            "fs:find_available:unrecognised fs API version (%d.%d.%d)",
                            m->mca_type_major_version,
                            m->mca_type_minor_version,
                            m->mca_type_release_version);
        return OMPI_ERROR;
    }

    /* Query done -- look at return value to see what happened */
    if (OMPI_SUCCESS != ret) {
        opal_output_verbose(10, ompi_fs_base_framework.framework_output,
                            "fs:find_available fs component %s is not available",
                            m->mca_component_name);
    } else {
        opal_output_verbose(10, ompi_fs_base_framework.framework_output,
                            "fs:find_avalable: fs component %s is available",
                            m->mca_component_name);

    }
    /* All done */
    return ret;
}


static int init_query_2_0_0(const mca_base_component_t *component,
                            bool enable_progress_threads,
                            bool enable_mpi_threads)
{
    mca_fs_base_component_2_0_0_t *fs =
        (mca_fs_base_component_2_0_0_t *) component;

    return fs->fsm_init_query(enable_progress_threads,
                              enable_mpi_threads);
}
