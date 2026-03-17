/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
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
#include "ompi/constants.h"

#include "ompi/mca/mca.h"
#include "opal/util/output.h"
#include "opal/mca/base/base.h"


#include "ompi/mca/osc/osc.h"
#include "ompi/mca/osc/base/base.h"


/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */

#include "ompi/mca/osc/base/static-components.h"

int
ompi_osc_base_find_available(bool enable_progress_threads,
                            bool enable_mpi_threads)
{
    mca_base_component_list_item_t *cli, *next;

    OPAL_LIST_FOREACH_SAFE(cli, next, &ompi_osc_base_framework.framework_components, mca_base_component_list_item_t) {
        int ret;
        ompi_osc_base_component_t *component = (ompi_osc_base_component_t*) cli->cli_component;

        /* see if this component is ready to run... */
        ret = component->osc_init(enable_progress_threads, enable_mpi_threads);
        if (OMPI_SUCCESS != ret) {
            /* not available. close the component */
            opal_list_remove_item(&ompi_osc_base_framework.framework_components, &cli->super);
            mca_base_component_close((mca_base_component_t *)component,
                                     ompi_osc_base_framework.framework_output);
            OBJ_RELEASE(cli);
        }
    }
    return OMPI_SUCCESS;
}

int
ompi_osc_base_finalize(void)
{
    opal_list_item_t* item;

    /* Finalize all available modules */
    while (NULL !=
           (item = opal_list_remove_first(&ompi_osc_base_framework.framework_components))) {
        ompi_osc_base_component_t *component = (ompi_osc_base_component_t*)
            ((mca_base_component_list_item_t*) item)->cli_component;
        component->osc_finalize();
        OBJ_RELEASE(item);
    }
    return OMPI_SUCCESS;
}

MCA_BASE_FRAMEWORK_DECLARE(ompi, osc, "One-sided communication", NULL, NULL, NULL,
                           mca_osc_base_static_components, 0);
