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
 * Copyright (c) 2008      Voltaire. All rights reserved
 * Copyright (c) 2011      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014      Intel, Inc. All rights reserved.
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


#ifndef ORTE_RMAPS_RF_H
#define ORTE_RMAPS_RF_H

#include "orte_config.h"

#include "opal/class/opal_object.h"

#include "orte/mca/rmaps/rmaps.h"

BEGIN_C_DECLS

int orte_rmaps_rank_file_lex_destroy (void);

struct orte_rmaps_rf_component_t {
    orte_rmaps_base_component_t super;
    char *slot_list;
    bool physical;
};
typedef struct orte_rmaps_rf_component_t orte_rmaps_rf_component_t;

ORTE_MODULE_DECLSPEC extern orte_rmaps_rf_component_t mca_rmaps_rank_file_component;
extern orte_rmaps_base_module_t orte_rmaps_rank_file_module;


typedef struct cpu_socket_t cpu_socket_t;

struct orte_rmaps_rank_file_map_t {
    opal_object_t super;
    char* node_name;
    char slot_list[64];
};
typedef struct orte_rmaps_rank_file_map_t orte_rmaps_rank_file_map_t;

ORTE_DECLSPEC OBJ_CLASS_DECLARATION(orte_rmaps_rank_file_map_t);

END_C_DECLS

#endif
