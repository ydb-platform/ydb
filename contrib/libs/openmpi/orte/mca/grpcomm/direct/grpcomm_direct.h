/* -*- C -*-
 *
 * Copyright (c) 2011      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014      Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */
#ifndef GRPCOMM_DIRECT_H
#define GRPCOMM_DIRECT_H

#include "orte_config.h"


#include "orte/mca/grpcomm/grpcomm.h"

BEGIN_C_DECLS

/*
 * Grpcomm interfaces
 */

ORTE_MODULE_DECLSPEC extern orte_grpcomm_base_component_t mca_grpcomm_direct_component;
extern orte_grpcomm_base_module_t orte_grpcomm_direct_module;

END_C_DECLS

#endif
