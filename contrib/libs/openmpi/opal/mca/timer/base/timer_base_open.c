/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2014 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2014 Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "opal_config.h"

#include "opal/constants.h"
#include "opal/mca/timer/base/base.h"

bool mca_timer_base_monotonic = true;

/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */
#include "opal/mca/timer/base/static-components.h"

static int mca_timer_base_register(mca_base_register_flag_t flags)
{
    (void) mca_base_var_register("opal", "timer", "require", "monotonic",
                                 "Node-level monotonic timer required (default yes)",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                 OPAL_INFO_LVL_5,
                                 MCA_BASE_VAR_SCOPE_LOCAL,
                                 &mca_timer_base_monotonic);

    return OPAL_SUCCESS;
}

/*
 * Globals
 */
/* Use default register/open/close functions */
MCA_BASE_FRAMEWORK_DECLARE(opal, timer, "OPAL OS timer", mca_timer_base_register, NULL, NULL,
                           mca_timer_base_static_components, 0);
