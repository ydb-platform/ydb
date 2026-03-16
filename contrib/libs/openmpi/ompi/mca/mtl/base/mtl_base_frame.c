/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015-2018 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "ompi/mca/mca.h"
#include "opal/util/output.h"
#include "opal/mca/base/base.h"


#include "ompi/constants.h"
#include "ompi/mca/mtl/mtl.h"
#include "ompi/mca/mtl/base/base.h"

/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */

#include "ompi/mca/mtl/base/static-components.h"

mca_mtl_base_component_t *ompi_mtl_base_selected_component = NULL;
mca_mtl_base_module_t *ompi_mtl = NULL;

/*
 * Function for selecting one component from all those that are
 * available.
 *
 * For now, we take the first component that says it can run.  Might
 * need to reexamine this at a later time.
 */
int
ompi_mtl_base_select (bool enable_progress_threads,
                      bool enable_mpi_threads,
                      int *priority)
{
    int ret = OMPI_ERR_NOT_FOUND;
    mca_mtl_base_component_t *best_component = NULL;
    mca_mtl_base_module_t *best_module = NULL;
    int best_priority;

    /*
     * Select the best component
     */
    if( OPAL_SUCCESS != mca_base_select("mtl", ompi_mtl_base_framework.framework_output,
                                        &ompi_mtl_base_framework.framework_components,
                                        (mca_base_module_t **) &best_module,
                                        (mca_base_component_t **) &best_component,
                                        &best_priority) ) {
        /* notify caller that no available component found */
        return ret;
    }

    opal_output_verbose( 10, ompi_mtl_base_framework.framework_output,
                         "select: initializing %s component %s",
                         best_component->mtl_version.mca_type_name,
                         best_component->mtl_version.mca_component_name );

    if (NULL == best_component->mtl_init(enable_progress_threads,
                                          enable_mpi_threads)) {
        opal_output_verbose( 10, ompi_mtl_base_framework.framework_output,
                             "select: init returned failure for component %s",
                             best_component->mtl_version.mca_component_name );
    } else {
        opal_output_verbose( 10, ompi_mtl_base_framework.framework_output,
                             "select: init returned success");
        ompi_mtl_base_selected_component = best_component;
        ompi_mtl = best_module;
        *priority = best_priority;
        ret = OMPI_SUCCESS;
    }

    /* All done */
    if (NULL == ompi_mtl) {
        opal_output_verbose( 10, ompi_mtl_base_framework.framework_output,
                             "select: no component selected");
    } else {
        opal_output_verbose( 10, ompi_mtl_base_framework.framework_output,
                             "select: component %s selected",
                             ompi_mtl_base_selected_component->
                             mtl_version.mca_component_name );
    }
    return ret;
}


static int
ompi_mtl_base_close(void)
{
    /* NTH: Should we be freeing the mtl module here? */
    ompi_mtl = NULL;
    ompi_mtl_base_selected_component = NULL;

    /* Close all remaining available modules (may be one if this is a
       OMPI RTE program, or [possibly] multiple if this is ompi_info) */
    return mca_base_framework_components_close(&ompi_mtl_base_framework, NULL);
}

MCA_BASE_FRAMEWORK_DECLARE(ompi, mtl, NULL, NULL, NULL, ompi_mtl_base_close,
                           mca_mtl_base_static_components, 0);
