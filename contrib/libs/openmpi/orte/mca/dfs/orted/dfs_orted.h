/*
 * Copyright (c) 2012      Los Alamos National Security, LLC.
 *                         All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 */

#ifndef MCA_dfs_orted_EXPORT_H
#define MCA_dfs_orted_EXPORT_H

#include "orte_config.h"

#include "orte/mca/dfs/dfs.h"

BEGIN_C_DECLS

/*
 * Local Component structures
 */

ORTE_MODULE_DECLSPEC extern orte_dfs_base_component_t mca_dfs_orted_component;

ORTE_DECLSPEC extern orte_dfs_base_module_t orte_dfs_orted_module;

extern int orte_dfs_orted_num_worker_threads;

END_C_DECLS

#endif /* MCA_dfs_orted_EXPORT_H */
