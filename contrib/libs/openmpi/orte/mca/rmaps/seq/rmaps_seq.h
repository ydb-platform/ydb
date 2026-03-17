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
#ifndef ORTE_RMAPS_SEQ_H
#define ORTE_RMAPS_SEQ_H

#include "orte_config.h"
#include "orte/mca/rmaps/rmaps.h"

BEGIN_C_DECLS

/**
 * RMGR Component
 */

ORTE_MODULE_DECLSPEC extern orte_rmaps_base_component_t mca_rmaps_seq_component;
extern orte_rmaps_base_module_t orte_rmaps_seq_module;


END_C_DECLS

#endif
