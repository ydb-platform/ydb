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
 * Copyright (c) 2007      Evergrid, Inc. All rights reserved.
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include "opal/mca/base/base.h"
#include "opal/mca/crs/base/base.h"

#include "opal/mca/crs/base/static-components.h"

/*
 * Globals
 */
opal_crs_base_module_t opal_crs = {
    NULL, /* crs_init               */
    NULL, /* crs_finalize           */
    NULL, /* crs_checkpoint         */
    NULL, /* crs_restart_cmd        */
    NULL, /* crs_disable_checkpoint */
    NULL, /* crs_enable_checkpoint  */
    NULL, /* crs_prelaunch          */
    NULL  /* crs_reg_thread         */
};

opal_crs_base_component_t opal_crs_base_selected_component = {{0}};

extern bool opal_crs_base_do_not_select;
static int opal_crs_base_register(mca_base_register_flag_t flags);

/* Use default select */
MCA_BASE_FRAMEWORK_DECLARE(opal, crs, "Checkpoint and Restart Service (CRS)",
                           opal_crs_base_register, opal_crs_base_open,
                           opal_crs_base_close, mca_crs_base_static_components, 0);

static int opal_crs_base_register(mca_base_register_flag_t flags)
{
    int ret;
    /*
     * Note: If we are a tool, then we will manually run the selection routine
     *       for the checkpointer.  The tool will set the MCA parameter
     *       'crs_base_do_not_select' before opal_init and then reset it after to
     *       disable the selection logic.
     *       This is useful for opal_restart because it reads the metadata file
     *       that indicates the checkpointer to be used after calling opal_init.
     *       Therefore it would need to select a specific module, but it doesn't
     *       know which one until later. It will set the MCA parameter 'crs'
     *       before calling select.
     */
    ret = mca_base_framework_var_register(&opal_crs_base_framework, "do_not_select",
                                          "Do not do the selection of the CRS component",
                                          MCA_BASE_VAR_TYPE_BOOL, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE |
                                          MCA_BASE_VAR_FLAG_INTERNAL, OPAL_INFO_LVL_9,
                                          MCA_BASE_VAR_SCOPE_ALL_EQ, &opal_crs_base_do_not_select);

    return (0 > ret) ? ret : OPAL_SUCCESS;
}

/**
 * Function for finding and opening either all MCA components,
 * or the one that was specifically requested via a MCA parameter.
 */
int opal_crs_base_open(mca_base_open_flag_t flags)
{
    if(!opal_cr_is_enabled) {
        opal_output_verbose(10, opal_crs_base_framework.framework_output,
                            "crs:open: FT is not enabled, skipping!");
        return OPAL_SUCCESS;
    }

    return mca_base_framework_components_open(&opal_crs_base_framework, flags);
}
