/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC.  All rights reserved.
 * Copyright (c) 2017      Cisco Systems, Inc.  All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * Resource Mapping
 */
#ifndef ORTE_RMAPS_MINDIST_H
#define ORTE_RMAPS_MINDIST_H

#include "orte_config.h"

#include "opal/mca/hwloc/hwloc-internal.h"
#include "opal/class/opal_list.h"

#include "orte/mca/rmaps/rmaps.h"

BEGIN_C_DECLS

ORTE_MODULE_DECLSPEC extern orte_rmaps_base_component_t mca_rmaps_mindist_component;
extern orte_rmaps_base_module_t orte_rmaps_mindist_module;

END_C_DECLS

#endif
