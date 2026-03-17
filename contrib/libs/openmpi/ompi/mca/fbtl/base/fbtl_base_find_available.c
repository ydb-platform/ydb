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
 * Copyright (c) 2015      Research Organization for Information Science
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

#include "mpi.h"
#include "ompi/constants.h"
#include "opal/class/opal_list.h"
#include "opal/util/output.h"
#include "ompi/mca/mca.h"
#include "opal/mca/base/base.h"
#include "ompi/mca/fbtl/fbtl.h"
#include "ompi/mca/fbtl/base/base.h"

static int init_query(const mca_base_component_t *m,
                      bool enable_progress_threads,
                      bool enable_mpi_threads);
static int init_query_2_0_0(const mca_base_component_t *component,
                            bool enable_progress_threads,
                            bool enable_mpi_threads);

int mca_fbtl_base_find_available(bool enable_progress_threads,
                               bool enable_mpi_threads)
{
    mca_base_component_list_item_t *cli, *next;

    /* The list of components which we should check is already present
       in mca_fbtl_base_components_opened, which was established in
       mca_fbtl_base_open */

    OPAL_LIST_FOREACH_SAFE(cli, next, &ompi_fbtl_base_framework.framework_components, mca_base_component_list_item_t) {
         /* Now for this entry, we have to determine the thread level. Call
            a subroutine to do the job for us */

         if (OMPI_SUCCESS != init_query(cli->cli_component,
                                        enable_progress_threads,
                                        enable_mpi_threads)) {
             mca_base_component_close(cli->cli_component, ompi_fbtl_base_framework.framework_output);
             opal_list_remove_item(&ompi_fbtl_base_framework.framework_components, &cli->super);
             OBJ_RELEASE(cli);
         }
     }

    /* There should atleast be one fbtl component which was available */
    if (0 == opal_list_get_size(&ompi_fbtl_base_framework.framework_components)) {
         opal_output_verbose (10, ompi_fbtl_base_framework.framework_output,
                              "fbtl:find_available: no fbtl components available!");
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

    opal_output_verbose(10, ompi_fbtl_base_framework.framework_output,
                        "fbtl:find_available: querying fbtl component %s",
                        m->mca_component_name);

    /* This component has been successfully opened, now try to query it */
    if (2 == m->mca_type_major_version &&
        0 == m->mca_type_minor_version &&
        0 == m->mca_type_release_version) {
        ret = init_query_2_0_0(m, enable_progress_threads,
                               enable_mpi_threads);
    } else {
        /* unrecognised API version */
        opal_output_verbose(10, ompi_fbtl_base_framework.framework_output,
                            "fbtl:find_available:unrecognised fbtl API version (%d.%d.%d)",
                            m->mca_type_major_version,
                            m->mca_type_minor_version,
                            m->mca_type_release_version);
        return OMPI_ERROR;
    }

    /* Query done -- look at return value to see what happened */
    if (OMPI_SUCCESS != ret) {
        opal_output_verbose(10, ompi_fbtl_base_framework.framework_output,
                            "fbtl:find_available fbtl component %s is not available",
                            m->mca_component_name);
    } else {
        opal_output_verbose(10, ompi_fbtl_base_framework.framework_output,
                            "fbtl:find_avalable: fbtl component %s is available",
                            m->mca_component_name);

    }
    /* All done */
    return ret;
}


static int init_query_2_0_0(const mca_base_component_t *component,
                            bool enable_progress_threads,
                            bool enable_mpi_threads)
{
    mca_fbtl_base_component_2_0_0_t *fbtl =
        (mca_fbtl_base_component_2_0_0_t *) component;

    return fbtl->fbtlm_init_query(enable_progress_threads,
                                  enable_mpi_threads);
}
