/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
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

#include "ompi/mca/crcp/crcp.h"
#include "ompi/mca/crcp/base/base.h"


static ompi_crcp_base_component_t none_component = {
    /* Handle the general mca_component_t struct containing
     *  meta information about the component itself
     */
    {
        OMPI_CRCP_BASE_VERSION_2_0_0,

        /* Component name and version */
        "none",
        OMPI_MAJOR_VERSION,
        OMPI_MINOR_VERSION,
        OMPI_RELEASE_VERSION,

        /* Component open and close functions */
        ompi_crcp_base_none_open,
        ompi_crcp_base_none_close,
        ompi_crcp_base_none_query
    },
    {
        /* Component is checkpointable */
        MCA_BASE_METADATA_PARAM_CHECKPOINT
    },

    /* Verbosity level */
    0,
    /* opal_output handler */
    -1,
    /* Default priority */
    1
};

static ompi_crcp_base_module_t none_module = {
    /** Initialization Function */
    ompi_crcp_base_module_init,
    /** Finalization Function */
    ompi_crcp_base_module_finalize,

    /** Quiesce interface */
    ompi_crcp_base_none_quiesce_start,
    ompi_crcp_base_none_quiesce_end,

    /** PML Wrapper */
    ompi_crcp_base_none_pml_enable,

    ompi_crcp_base_none_pml_add_comm,
    ompi_crcp_base_none_pml_del_comm,

    ompi_crcp_base_none_pml_add_procs,
    ompi_crcp_base_none_pml_del_procs,

    ompi_crcp_base_none_pml_progress,

    ompi_crcp_base_none_pml_iprobe,
    ompi_crcp_base_none_pml_probe,

    ompi_crcp_base_none_pml_isend_init,
    ompi_crcp_base_none_pml_isend,
    ompi_crcp_base_none_pml_send,

    ompi_crcp_base_none_pml_irecv_init,
    ompi_crcp_base_none_pml_irecv,
    ompi_crcp_base_none_pml_recv,

    ompi_crcp_base_none_pml_dump,
    ompi_crcp_base_none_pml_start,
    ompi_crcp_base_none_pml_ft_event,

    /** Request Wrapper */
    ompi_crcp_base_none_request_complete,

    /** BTL Wrapper */
    ompi_crcp_base_none_btl_add_procs,
    ompi_crcp_base_none_btl_del_procs,

    ompi_crcp_base_none_btl_register,
    ompi_crcp_base_none_btl_finalize,

    ompi_crcp_base_none_btl_alloc,
    ompi_crcp_base_none_btl_free,

    ompi_crcp_base_none_btl_prepare_src,
    ompi_crcp_base_none_btl_prepare_dst,

    ompi_crcp_base_none_btl_send,
    ompi_crcp_base_none_btl_put,
    ompi_crcp_base_none_btl_get,

    ompi_crcp_base_none_btl_dump,
    ompi_crcp_base_none_btl_ft_event
};

int ompi_crcp_base_select(void)
{
    int ret;
    ompi_crcp_base_component_t *best_component = NULL;
    ompi_crcp_base_module_t *best_module = NULL;
    const char *include_list = NULL;
    const char **selection_value;
    int var_id;

    /*
     * Register the framework MCA param and look up include list
     */
    var_id = mca_base_var_find("ompi", "crcp", NULL, NULL);

    /* NTH: The old parameter code here set the selection to none if no file value
       or environment value was set. This effectively means include_list is never NULL. */
    selection_value = NULL;
    (void) mca_base_var_get_value(var_id, &selection_value, NULL, NULL);
    if (NULL == selection_value || NULL == selection_value[0]) {
        (void) mca_base_var_set_value(var_id, "none", 5, MCA_BASE_VAR_SOURCE_DEFAULT, NULL);
        include_list = "none";
    } else {
        include_list = selection_value[0];
    }

    if(0 == strncmp(include_list, "none", strlen("none")) ){
        opal_output_verbose(10, ompi_crcp_base_framework.framework_output,
                            "crcp:select: Using %s component",
                            include_list);
        best_component = &none_component;
        best_module    = &none_module;
        /* JJH: Todo: Check if none is in the list */
        /* Close all components since none will be used */
        mca_base_components_close(ompi_crcp_base_framework.framework_output,
                                  &ompi_crcp_base_framework.framework_components,
                                  NULL);
    } else

    /*
     * Select the best component
     */
    if( OPAL_SUCCESS != mca_base_select("crcp", ompi_crcp_base_framework.framework_output,
                                        &ompi_crcp_base_framework.framework_components,
                                        (mca_base_module_t **) &best_module,
                                        (mca_base_component_t **) &best_component, NULL) ) {
        /* This will only happen if no component was selected */
        return OMPI_ERROR;
    }

    /* Save the winner */
    ompi_crcp_base_selected_component = *best_component;
    ompi_crcp = *best_module;

    /* Initialize the winner */
    if (OPAL_SUCCESS != (ret = ompi_crcp.crcp_init()) ) {
        return ret;
    }

    return OMPI_SUCCESS;
}
