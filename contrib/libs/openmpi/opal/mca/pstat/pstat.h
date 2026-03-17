/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2008 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
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
 * pstat (process statistics) framework component interface.
 *
 * Intent
 *
 * To support the ompi-top utility.
 *
 */

#ifndef OPAL_MCA_PSTAT_H
#define OPAL_MCA_PSTAT_H

#include "opal_config.h"

#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/dss/dss_types.h"

BEGIN_C_DECLS

/**
 * Module initialization function.  Should return OPAL_SUCCESS.
 */
typedef int (*opal_pstat_base_module_init_fn_t)(void);

typedef int (*opal_pstat_base_module_query_fn_t)(pid_t pid,
                                                 opal_pstats_t *stats,
                                                 opal_node_stats_t *nstats);

typedef int (*opal_pstat_base_module_fini_fn_t)(void);

/**
 * Structure for pstat components.
 */
struct opal_pstat_base_component_2_0_0_t {
    /** MCA base component */
    mca_base_component_t base_version;
    /** MCA base data */
    mca_base_component_data_t base_data;
};

/**
 * Convenience typedef
 */
typedef struct opal_pstat_base_component_2_0_0_t opal_pstat_base_component_2_0_0_t;
typedef struct opal_pstat_base_component_2_0_0_t opal_pstat_base_component_t;

/**
 * Structure for pstat modules
 */
struct opal_pstat_base_module_1_0_0_t {
    opal_pstat_base_module_init_fn_t    init;
    opal_pstat_base_module_query_fn_t   query;
    opal_pstat_base_module_fini_fn_t    finalize;
};

/**
 * Convenience typedef
 */
typedef struct opal_pstat_base_module_1_0_0_t opal_pstat_base_module_1_0_0_t;
typedef struct opal_pstat_base_module_1_0_0_t opal_pstat_base_module_t;


/**
 * Macro for use in components that are of type pstat
 */
#define OPAL_PSTAT_BASE_VERSION_2_0_0 \
    OPAL_MCA_BASE_VERSION_2_1_0("pstat", 2, 0, 0)

/* Global structure for accessing pstat functions */
OPAL_DECLSPEC extern opal_pstat_base_module_t opal_pstat;

END_C_DECLS

#endif /* OPAL_MCA_PSTAT_H */
