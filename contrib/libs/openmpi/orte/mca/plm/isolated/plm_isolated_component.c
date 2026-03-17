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
 * Copyright (c) 2007-2015 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2008-2009 Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2010      Oracle and/or its affiliates.  All rights
 *                         reserved.
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011      IBM Corporation.  All rights reserved.
 * Copyright (c) 2014      Intel, Inc.  All rights reserved.
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

#include "orte/runtime/orte_globals.h"

#include "orte/mca/plm/plm.h"
#include "orte/mca/plm/isolated/plm_isolated.h"

/*
 * Public string showing the plm ompi_isolated component version number
 */
const char *mca_plm_isolated_component_version_string =
  "Open MPI isolated plm MCA component version " ORTE_VERSION;


static int isolated_component_open(void);
static int isolated_component_query(mca_base_module_t **module, int *priority);
static int isolated_component_close(void);

/*
 * Instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */

orte_plm_base_component_t mca_plm_isolated_component = {
    .base_version = {
        ORTE_PLM_BASE_VERSION_2_0_0,

        /* Component name and version */
        .mca_component_name = "isolated",
        MCA_BASE_MAKE_VERSION(component, ORTE_MAJOR_VERSION, ORTE_MINOR_VERSION,
                              ORTE_RELEASE_VERSION),

        /* Component open and close functions */
        .mca_open_component = isolated_component_open,
        .mca_close_component = isolated_component_close,
        .mca_query_component = isolated_component_query,
    },
    .base_data = {
        /* The component is checkpoint ready */
        MCA_BASE_METADATA_PARAM_CHECKPOINT
    },
};

static int isolated_component_open(void)
{
    return ORTE_SUCCESS;
}


static int isolated_component_query(mca_base_module_t **module, int *priority)
{
    /* make ourselves available at a very low priority */
    if (ORTE_PROC_IS_HNP) {
        *priority = 0;
        *module = (mca_base_module_t *) &orte_plm_isolated_module;
        return ORTE_SUCCESS;
    }
    *module = NULL;
    return ORTE_ERROR;
}


static int isolated_component_close(void)
{
    return ORTE_SUCCESS;
}

