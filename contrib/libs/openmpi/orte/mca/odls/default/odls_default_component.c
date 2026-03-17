/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2017      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * These symbols are in a file by themselves to provide nice linker
 * semantics.  Since linkers generally pull in symbols by object
 * files, keeping these symbols as the only symbols in this file
 * prevents utility programs such as "ompi_info" from having to import
 * entire components just to query their version and parameters.
 */

#include "orte_config.h"
#include "orte/constants.h"

#include <stdlib.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <ctype.h>

#include "orte/mca/mca.h"
#include "opal/mca/base/base.h"

#include "orte/mca/odls/odls.h"
#include "orte/mca/odls/base/odls_private.h"
#include "orte/mca/odls/default/odls_default.h"

/*
 * Instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */

orte_odls_base_component_t mca_odls_default_component = {
    /* First, the mca_component_t struct containing meta information
    about the component itself */
    .version = {
        ORTE_ODLS_BASE_VERSION_2_0_0,
        /* Component name and version */
        .mca_component_name = "default",
        MCA_BASE_MAKE_VERSION(component, ORTE_MAJOR_VERSION, ORTE_MINOR_VERSION,
                              ORTE_RELEASE_VERSION),

        /* Component open and close functions */
        .mca_open_component = orte_odls_default_component_open,
        .mca_close_component = orte_odls_default_component_close,
        .mca_query_component = orte_odls_default_component_query,
    },
    .base_data = {
        /* The component is checkpoint ready */
        MCA_BASE_METADATA_PARAM_CHECKPOINT
    },
};



int orte_odls_default_component_open(void)
{
    return ORTE_SUCCESS;
}

int orte_odls_default_component_query(mca_base_module_t **module, int *priority)
{
    /* the base open/select logic protects us against operation when
     * we are NOT in a daemon, so we don't have to check that here
     */

    /* we have built some logic into the configure.m4 file that checks
     * to see if we have "fork" support and only builds this component
     * if we do. Hence, we only get here if we CAN build - in which
     * case, we definitely should be considered for selection
     */
    *priority = 10; /* let others override us - we are the default */
    *module = (mca_base_module_t *) &orte_odls_default_module;
    return ORTE_SUCCESS;
}


int orte_odls_default_component_close(void)
{
    return ORTE_SUCCESS;
}
