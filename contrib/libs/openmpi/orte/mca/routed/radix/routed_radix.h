/*
 * Copyright (c) 2007      Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2016      Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_ROUTED_RADIX_H
#define MCA_ROUTED_RADIX_H

#include "orte_config.h"

#include "orte/mca/routed/routed.h"

BEGIN_C_DECLS

typedef struct {
    orte_routed_component_t super;
    int radix;
} orte_routed_radix_component_t;
ORTE_MODULE_DECLSPEC extern orte_routed_radix_component_t mca_routed_radix_component;

extern orte_routed_module_t orte_routed_radix_module;

END_C_DECLS

#endif
