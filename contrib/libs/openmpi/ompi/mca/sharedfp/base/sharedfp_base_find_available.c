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
#include "opal/class/opal_list.h"
#include "opal/util/output.h"
#include "ompi/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/mca/base/mca_base_component_repository.h"
#include "ompi/mca/sharedfp/sharedfp.h"
#include "ompi/mca/sharedfp/base/base.h"

static int init_query(const mca_base_component_t *m,
                      mca_base_component_list_item_t *entry,
                      bool enable_progress_threads,
                      bool enable_mpi_threads);
static int init_query_2_0_0(const mca_base_component_t *component,
                            mca_base_component_list_item_t *entry,
                            bool enable_progress_threads,
                            bool enable_mpi_threads);

int mca_sharedfp_base_find_available(bool enable_progress_threads,
                                     bool enable_mpi_threads)
{
    opal_list_item_t *item, *next;
    mca_base_component_list_item_t *cli;

    /* The list of components which we should check is already present
       in ompi_sharedfp_base_framework.framework_components, which was established in
       mca_sharedfp_base_open */

    item = opal_list_get_first(&ompi_sharedfp_base_framework.framework_components);
    while (item != opal_list_get_end(&ompi_sharedfp_base_framework.framework_components)) {
        next = opal_list_get_next(item);
         cli = (mca_base_component_list_item_t*)item;

         /* Now for this entry, we have to determine the thread level. Call
            a subroutine to do the job for us */

         if (OMPI_SUCCESS != init_query(cli->cli_component, cli,
                                        enable_progress_threads,
                                        enable_mpi_threads)) {
             /* The component does not want to run, so close it. Its close()
                has already been invoked. Close it out of the DSO repository
                (if it is there in the repository) */
             mca_base_component_repository_release(cli->cli_component);
             OBJ_RELEASE(item);
         }
         item = next;
     }

     /* There should at least be one sharedfp component which was available */
    if (0 == opal_list_get_size(&ompi_sharedfp_base_framework.framework_components)) {
         opal_output_verbose (10, ompi_sharedfp_base_framework.framework_output,
                              "sharedfp:find_available: no sharedfp components available!");
         return OMPI_ERROR;
     }

     /* All done */
     return OMPI_SUCCESS;
}


static int init_query(const mca_base_component_t *m,
                      mca_base_component_list_item_t *entry,
                      bool enable_progress_threads,
                      bool enable_mpi_threads)
{
    int ret;

    opal_output_verbose(10, ompi_sharedfp_base_framework.framework_output,
                        "sharedfp:find_available: querying sharedfp component %s",
                        m->mca_component_name);

    /* This component has been successfully opened, now try to query it */
    if (2 == m->mca_type_major_version &&
        0 == m->mca_type_minor_version &&
        0 == m->mca_type_release_version) {
        ret = init_query_2_0_0(m, entry, enable_progress_threads,
                               enable_mpi_threads);
    } else {
        /* unrecognised API version */
        opal_output_verbose(10, ompi_sharedfp_base_framework.framework_output,
                            "sharedfp:find_available:unrecognised sharedfp API version (%d.%d.%d)",
                            m->mca_type_major_version,
                            m->mca_type_minor_version,
                            m->mca_type_release_version);
        return OMPI_ERROR;
    }

    /* Query done -- look at return value to see what happened */
    if (OMPI_SUCCESS != ret) {
        opal_output_verbose(10, ompi_sharedfp_base_framework.framework_output,
                            "sharedfp:find_available sharedfp component %s is not available",
                            m->mca_component_name);
        if (NULL != m->mca_close_component) {
            m->mca_close_component();
        }
    } else {
        opal_output_verbose(10, ompi_sharedfp_base_framework.framework_output,
                            "sharedfp:find_avalable: sharedfp component %s is available",
                            m->mca_component_name);

    }
    /* All done */
    return ret;
}


static int init_query_2_0_0(const mca_base_component_t *component,
                            mca_base_component_list_item_t *entry,
                            bool enable_progress_threads,
                            bool enable_mpi_threads)
{
    mca_sharedfp_base_component_2_0_0_t *sharedfp =
        (mca_sharedfp_base_component_2_0_0_t *) component;

    return sharedfp->sharedfpm_init_query(enable_progress_threads,
                              enable_mpi_threads);
}
