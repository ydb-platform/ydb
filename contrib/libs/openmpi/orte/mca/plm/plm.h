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
 * Copyright (c) 2011-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file:
 *
 * The Process Lifecycle Management (PLM) subsystem serves as the central
 * switchyard for all process management activities, including
 * resource allocation, process mapping, process launch, and process
 * monitoring.
 */

#ifndef ORTE_PLM_H
#define ORTE_PLM_H

/*
 * includes
 */

#include "orte_config.h"
#include "orte/types.h"

#include "orte/mca/mca.h"
#include "opal/dss/dss_types.h"
#include "opal/class/opal_pointer_array.h"

#include "orte/runtime/orte_globals.h"

#include "plm_types.h"

BEGIN_C_DECLS

/*
 * Component functions - all MUST be provided
 */

/*
 * allow the selected module to initialize
 */
typedef int (*orte_plm_base_module_init_fn_t)(void);

/*
 * Spawn a job - this is a non-blocking function!
 */
typedef int (*orte_plm_base_module_spawn_fn_t)(orte_job_t *jdata);

/*
 * Remote spawn - spawn called by a daemon to launch a process on its own
 */
typedef int (*orte_plm_base_module_remote_spawn_fn_t)(void);

/*
 * Entry point to set the HNP name
 */
typedef int (*orte_plm_base_module_set_hnp_name_fn_t)(void);

/**
    * Cleanup resources held by module.
 */

typedef int (*orte_plm_base_module_finalize_fn_t)(void);

/**
 * Terminate any processes launched for the respective jobid by
 * this component.
 */
typedef int (*orte_plm_base_module_terminate_job_fn_t)(orte_jobid_t);

/**
 * Terminate the daemons
 */
typedef int (*orte_plm_base_module_terminate_orteds_fn_t)(void);

/**
 * Terminate an array of specific procs
 */
typedef int (*orte_plm_base_module_terminate_procs_fn_t)(opal_pointer_array_t *procs);

    /**
 * Signal any processes launched for the respective jobid by
 * this component.
 */
typedef int (*orte_plm_base_module_signal_job_fn_t)(orte_jobid_t, int32_t);

/**
 * plm module version 1.0.0
 */
struct orte_plm_base_module_1_0_0_t {
    orte_plm_base_module_init_fn_t               init;
    orte_plm_base_module_set_hnp_name_fn_t       set_hnp_name;
    orte_plm_base_module_spawn_fn_t              spawn;
    orte_plm_base_module_remote_spawn_fn_t       remote_spawn;
    orte_plm_base_module_terminate_job_fn_t      terminate_job;
    orte_plm_base_module_terminate_orteds_fn_t   terminate_orteds;
    orte_plm_base_module_terminate_procs_fn_t    terminate_procs;
    orte_plm_base_module_signal_job_fn_t         signal_job;
    orte_plm_base_module_finalize_fn_t           finalize;
};

/** shorten orte_plm_base_module_1_0_0_t declaration */
typedef struct orte_plm_base_module_1_0_0_t orte_plm_base_module_1_0_0_t;
/** shorten orte_plm_base_module_t declaration */
typedef struct orte_plm_base_module_1_0_0_t orte_plm_base_module_t;


/**
 * plm component
 */
struct orte_plm_base_component_2_0_0_t {
    /** component version */
    mca_base_component_t base_version;
    /** component data */
    mca_base_component_data_t base_data;
};
/** Convenience typedef */
typedef struct orte_plm_base_component_2_0_0_t orte_plm_base_component_2_0_0_t;
/** Convenience typedef */
typedef orte_plm_base_component_2_0_0_t orte_plm_base_component_t;


/**
 * Macro for use in modules that are of type plm
 */
#define ORTE_PLM_BASE_VERSION_2_0_0 \
    ORTE_MCA_BASE_VERSION_2_1_0("plm", 2, 0, 0)

/* Global structure for accessing PLM functions */
ORTE_DECLSPEC extern orte_plm_base_module_t orte_plm;  /* holds selected module's function pointers */

END_C_DECLS

#endif
