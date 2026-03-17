/*
 * Copyright (c) 2004-2007 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2012-2013 Los Alamos National Security, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <string.h>

#include "base.h"
#include "ompi/mca/mca.h"
#include "opal/mca/base/base.h"
#include "ompi/mca/pml/v/pml_v_output.h"

mca_vprotocol_base_module_t mca_vprotocol = {0};
mca_vprotocol_base_component_t mca_vprotocol_component = {
    {MCA_VPROTOCOL_BASE_VERSION_0_0_0} /* Initialized with invalid version */
};

typedef struct opened_component_t {
    opal_list_item_t super;
    mca_vprotocol_base_component_t *om_component;
} opened_component_t;


/*
 * Function for selecting one component from all those that are
 * available.
 *
 * Call the init function on all available components and get their
 * priorities.  Select the component with the highest priority.  All
 * other components will be closed and unloaded.  The selected component
 * will have all of its function pointers saved and returned to the
 * caller.
 */
int mca_vprotocol_base_select(bool enable_progress_threads,
                              bool enable_mpi_threads)
{
    int priority = 0, best_priority = -1;
    opal_list_item_t *item = NULL;
    mca_base_component_list_item_t *cli = NULL;
    mca_vprotocol_base_component_t *component = NULL, *best_component = NULL;
    mca_vprotocol_base_module_t *module = NULL, *best_module = NULL;
    opal_list_t opened;
    opened_component_t *om = NULL;

    /* Traverse the list of available components; call their init
        functions. */
    OBJ_CONSTRUCT(&opened, opal_list_t);
    OPAL_LIST_FOREACH(cli, &ompi_vprotocol_base_framework.framework_components, mca_base_component_list_item_t)
    {
        component = (mca_vprotocol_base_component_t *) cli->cli_component;

        if (NULL == mca_vprotocol_base_include_list) {
            continue;
        }

        V_OUTPUT_VERBOSE(500, "vprotocol select: initializing %s component %s", component->pmlm_version.mca_type_name, component->pmlm_version.mca_component_name);
        if(strcmp(component->pmlm_version.mca_component_name,
                  mca_vprotocol_base_include_list)) {
            V_OUTPUT_VERBOSE(500, "This component is not in the include list: skipping %s", component->pmlm_version.mca_component_name);
            continue;
        }
        if(NULL == component->pmlm_init) {
            V_OUTPUT_VERBOSE(2, "vprotocol select: no init function; ignoring component %s", component->pmlm_version.mca_component_name);
            continue;
        }
        module = component->pmlm_init(&priority, enable_progress_threads, enable_mpi_threads);
        if (NULL == module) {
            V_OUTPUT_VERBOSE(2, "vprotocol select: init returned failure for component %s", component->pmlm_version.mca_component_name);
            continue;
        }
        V_OUTPUT_VERBOSE(500, "vprotocol select: component %s init returned priority %d", component->pmlm_version.mca_component_name, priority);
        if (priority > best_priority)
        {
            best_priority = priority;
            best_component = component;
            best_module = module;
        }

        om = (opened_component_t *) malloc(sizeof(opened_component_t));
        if (NULL == om) return OMPI_ERR_OUT_OF_RESOURCE;
        OBJ_CONSTRUCT(om, opal_list_item_t);
        om->om_component = component;
        opal_list_append(&opened, (opal_list_item_t*) om);
    }

    /* Finished querying all components.  Check for the bozo case. */
    if (NULL == best_component) {
        V_OUTPUT_VERBOSE(2, "vprotocol select: no protocol has returned a positive priority, fault tolerance is OFF");
    }
    else
    {
        /* Save the winner */
        mca_vprotocol_component = *best_component;
        mca_vprotocol = *best_module;
    }

    /* Finalize all non-selected components */
    for (item = opal_list_remove_first(&opened);
         NULL != item;
         item = opal_list_remove_first(&opened))
    {
        om = (opened_component_t *) item;
        if (om->om_component != best_component) {
            /* Finalize */
            V_OUTPUT_VERBOSE(500, "vprotocol select: component %s not selected / finalized", om->om_component->pmlm_version.mca_component_name);
            if (NULL != om->om_component->pmlm_finalize) {
                /* Blatently ignore the return code (what would we do to
                recover, anyway?  This component is going away, so errors
                don't matter anymore) */
                om->om_component->pmlm_finalize();
            }
        }
        OBJ_DESTRUCT(om);
        free(om);
    }

    mca_base_components_close(mca_pml_v.output,
                              &ompi_vprotocol_base_framework.framework_components,
                              (mca_base_component_t *) best_component);

    /* All done */
    if(best_component != NULL)
    {
        V_OUTPUT_VERBOSE(500, "vprotocol select: component %s selected", mca_vprotocol_component.pmlm_version.mca_component_name);
        return OMPI_SUCCESS;
    }
    else
        return OMPI_ERR_NOT_FOUND;
}
